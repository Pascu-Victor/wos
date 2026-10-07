#include "rdma_operations.hpp"

#include <cerrno>
#include <cstring>
#include <utility>

namespace ker::net::wki::rdma_operations {
namespace {

class TryGuard final {
   public:
    explicit TryGuard(std::atomic_flag& flag) : flag_(flag), held_(!flag.test_and_set(std::memory_order_acquire)) {}
    ~TryGuard() {
        if (held_) flag_.clear(std::memory_order_release);
    }
    explicit operator bool() const { return held_; }
    TryGuard(const TryGuard&) = delete;
    auto operator=(const TryGuard&) -> TryGuard& = delete;

   private:
    std::atomic_flag& flag_;
    bool held_;
};

template <size_t Size>
auto nonzero(const std::array<uint8_t, Size>& value) -> bool {
    uint8_t bits = 0;
    for (auto byte : value) bits |= byte;
    return bits != 0;
}

auto valid_binding(const rdma_registry::Binding& value) -> bool {
    return value.owner_node != 0 && value.owner_node != UINT16_MAX && value.peer_node != 0 && value.peer_node != UINT16_MAX &&
           value.owner_node != value.peer_node && nonzero(value.session_id) && value.secure_incarnation != 0 &&
           value.transport_cookie != 0 && value.transport_generation != 0 && value.channel_cookie != 0 && value.channel_generation != 0 &&
           value.channel_id != 0 && rdma_protocol::transport_bit(value.transport) != 0 && rdma_protocol::service_bit(value.service) != 0;
}

auto same_binding(const rdma_registry::Binding& lhs, const rdma_registry::Binding& rhs) -> bool {
    return lhs.owner_node == rhs.owner_node && lhs.peer_node == rhs.peer_node && lhs.session_id == rhs.session_id &&
           lhs.secure_incarnation == rhs.secure_incarnation && lhs.transport_cookie == rhs.transport_cookie &&
           lhs.transport_generation == rhs.transport_generation && lhs.channel_cookie == rhs.channel_cookie &&
           lhs.channel_generation == rhs.channel_generation && lhs.channel_id == rhs.channel_id && lhs.transport == rhs.transport &&
           lhs.service == rhs.service;
}

auto incoming(Role role) -> bool { return role == Role::RECEIVE_WRITE || role == Role::SERVE_READ || role == Role::RECEIVE_NOTIFY; }

auto valid_spec(const Spec& spec) -> bool {
    if (!valid_binding(spec.binding) || spec.operation_id == 0 || spec.receive_ticket == 0 || spec.deadline_us == 0 ||
        spec.requested > rdma_protocol::MAX_TRANSFER_BYTES) {
        return false;
    }
    rdma_protocol::Opcode opcode;
    switch (spec.role) {
        case Role::RECEIVE_WRITE:
        case Role::SEND_WRITE:
            opcode = rdma_protocol::Opcode::WRITE;
            break;
        case Role::SERVE_READ:
        case Role::RECEIVE_READ:
            opcode = rdma_protocol::Opcode::READ;
            break;
        case Role::RECEIVE_NOTIFY:
            opcode = rdma_protocol::Opcode::NOTIFY;
            break;
        default:
            return false;
    }
    rdma_protocol::Header header{};
    header.transport = static_cast<uint8_t>(spec.binding.transport);
    header.service = static_cast<uint8_t>(spec.binding.service);
    header.opcode = static_cast<uint8_t>(opcode);
    header.src_node = spec.binding.peer_node;
    header.dst_node = spec.binding.owner_node;
    header.channel_id = spec.binding.channel_id;
    header.flags = spec.flags;
    header.session_id = spec.binding.session_id;
    header.region_generation = spec.grant.generation;
    header.capability = spec.grant.capability;
    header.region_offset = spec.offset;
    header.operation_id = spec.operation_id;
    header.binding_generation = spec.grant.binding_generation;
    header.receive_ticket = spec.receive_ticket;
    header.counter = 1;
    header.total_length = spec.requested;
    header.payload_length = opcode == rdma_protocol::Opcode::WRITE
                                ? (spec.requested < rdma_protocol::FRAGMENT_BYTES ? spec.requested : rdma_protocol::FRAGMENT_BYTES)
                                : 0;
    header.rkey = spec.grant.rkey;
    return rdma_protocol::validate_grant(header, spec.grant) == rdma_protocol::Drop::NONE;
}

auto add_count(uint32_t lhs, uint32_t rhs) -> uint32_t { return rhs > UINT32_MAX - lhs ? UINT32_MAX : lhs + rhs; }

}  // namespace

Handle::~Handle() { reset(); }
Handle::Handle(Handle&& other) noexcept { *this = std::move(other); }
auto Handle::operator=(Handle&& other) noexcept -> Handle& {
    if (this == &other) return *this;
    reset();
    owner_ = std::exchange(other.owner_, nullptr);
    slot_ = std::exchange(other.slot_, OPERATION_SLOTS);
    generation_ = std::exchange(other.generation_, 0);
    return *this;
}
void Handle::reset() {
    if (owner_ != nullptr) owner_->release(*this);
    owner_ = nullptr;
    slot_ = OPERATION_SLOTS;
    generation_ = 0;
}

Work::~Work() { reset(); }
Work::Work(Work&& other) noexcept { *this = std::move(other); }
auto Work::operator=(Work&& other) noexcept -> Work& {
    if (this == &other) return *this;
    reset();
    owner_ = std::exchange(other.owner_, nullptr);
    slot_ = std::exchange(other.slot_, OPERATION_SLOTS);
    generation_ = std::exchange(other.generation_, 0);
    kind_ = std::exchange(other.kind_, WorkKind::NONE);
    spec_ = other.spec_;
    staging_ = std::exchange(other.staging_, {});
    backing_ = std::exchange(other.backing_, {});
    auth_crypto::secure_erase(&other.spec_, sizeof(other.spec_));
    return *this;
}
auto Work::prepare_buffer() -> std::span<uint8_t> {
    if (owner_ == nullptr || (kind_ != WorkKind::PREPARE && !(kind_ == WorkKind::EFFECT && spec_.role == Role::SERVE_READ))) return {};
    return {const_cast<uint8_t*>(staging_.data()), staging_.size()};
}
void Work::reset() {
    if (owner_ != nullptr) owner_->release(*this);
    owner_ = nullptr;
    slot_ = OPERATION_SLOTS;
    generation_ = 0;
    kind_ = WorkKind::NONE;
    staging_ = {};
    backing_ = {};
    auth_crypto::secure_erase(&spec_, sizeof(spec_));
}

EraseWork::~EraseWork() {
    // The moved lease releases independently. ERASING remains quarantined:
    // destruction cannot certify that the owner's large wipe happened.
}

Operations::~Operations() {
    // The owner must drain external handles/work first and retain scratch.
    // Destruction never frees or substitutes for task-only scratch erasure.
    for (auto& slot : slots_) {
        auth_crypto::secure_erase(&slot.spec, sizeof(slot.spec));
        auth_crypto::secure_erase(&slot.coverage, sizeof(slot.coverage));
        auth_crypto::secure_erase(&slot.result, sizeof(slot.result));
    }
}

auto Operations::initialize(std::span<uint8_t> scratch) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (initialized_once_) return rdma_protocol::Drop::RETIRED;
    if (scratch.data() == nullptr || scratch.size() != SCRATCH_BYTES) return rdma_protocol::Drop::LENGTH;
    if (scratch.size() > UINTPTR_MAX - reinterpret_cast<uintptr_t>(scratch.data())) return rdma_protocol::Drop::RANGE;
    scratch_ = scratch;
    initialized_ = true;
    initialized_once_ = true;
    return rdma_protocol::Drop::NONE;
}

auto Operations::prepare(const Spec& spec, rdma_registry::Lease&& lease, Handle& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_ || stopping_) return rdma_protocol::Drop::RETIRED;
    if (output) return rdma_protocol::Drop::OPERATION;
    if (!valid_spec(spec)) return rdma_protocol::Drop::OPERATION;
    if (incoming(spec.role)) {
        auto const DESCRIPTOR = lease.descriptor();
        if (!lease || lease.memory().backing != nullptr || std::memcmp(&DESCRIPTOR, &spec.grant, sizeof(DESCRIPTOR)) != 0) {
            return rdma_protocol::Drop::GRANT;
        }
    } else if (lease) {
        return rdma_protocol::Drop::GRANT;
    }
    size_t available = OPERATION_SLOTS;
    for (size_t index = 0; index < slots_.size(); ++index) {
        auto& slot = slots_[index];
        settle_abandoned_locked(slot);
        if (slot.state == State::FREE) {
            if (available == OPERATION_SLOTS) available = index;
        } else if (same_binding(slot.spec.binding, spec.binding) &&
                   (slot.spec.operation_id == spec.operation_id || slot.spec.receive_ticket == spec.receive_ticket)) {
            return rdma_protocol::Drop::OPERATION;
        }
    }
    if (available == OPERATION_SLOTS) return rdma_protocol::Drop::PRESSURE;
    uint64_t const GENERATION = rdma_protocol::reserve(next_generation_);
    if (GENERATION == 0) return rdma_protocol::Drop::COUNTER;
    auto& slot = slots_[available];
    slot.spec = spec;
    slot.generation = GENERATION;
    slot.lease = std::move(lease);
    slot.coverage = {};
    if (spec.role == Role::RECEIVE_WRITE) static_cast<void>(rdma_protocol::coverage_begin(slot.coverage, spec.requested));
    slot.state = spec.role == Role::SEND_WRITE ? State::PREPARING : spec.role == Role::RECEIVE_READ ? State::READY : State::ARMED;
    slot.handles.store(1, std::memory_order_relaxed);
    output.owner_ = this;
    output.slot_ = available;
    output.generation_ = GENERATION;
    return rdma_protocol::Drop::NONE;
}

auto Operations::find(const Handle& handle) -> Slot* {
    if (handle.owner_ != this || handle.slot_ >= OPERATION_SLOTS) return nullptr;
    auto& slot = slots_[handle.slot_];
    return slot.state != State::FREE && slot.generation == handle.generation_ ? &slot : nullptr;
}
auto Operations::find(const Work& work) -> Slot* {
    if (work.owner_ != this || work.slot_ >= OPERATION_SLOTS) return nullptr;
    auto& slot = slots_[work.slot_];
    return slot.state != State::FREE && slot.generation == work.generation_ ? &slot : nullptr;
}

void Operations::publish_work(size_t index, WorkKind kind, Work& output) {
    auto& slot = slots_[index];
    slot.work.fetch_add(1, std::memory_order_relaxed);
    output.owner_ = this;
    output.slot_ = index;
    output.generation_ = slot.generation;
    output.kind_ = kind;
    output.spec_ = slot.spec;
    uint32_t const SIZE = kind == WorkKind::RESULT                                         ? slot.result.bytes
                          : kind == WorkKind::SEND && slot.spec.role == Role::RECEIVE_READ ? 0
                                                                                           : slot.spec.requested;
    output.staging_ = {scratch_.data() + index * rdma_protocol::MAX_TRANSFER_BYTES, SIZE};
    output.backing_ = kind == WorkKind::EFFECT ? slot.lease.memory() : rdma_registry::ReleasedRegion{};
}

auto Operations::arm(const Handle& handle, uint64_t consumer_generation) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->closing) return rdma_protocol::Drop::RETIRED;
    if (consumer_generation == 0 || slot->consumer_generation != 0 ||
        (slot->state != State::ARMED && slot->state != State::PREPARING && slot->state != State::READY)) {
        return rdma_protocol::Drop::OPERATION;
    }
    slot->consumer_generation = consumer_generation;
    return rdma_protocol::Drop::NONE;
}

auto Operations::begin_prepare(const Handle& handle, Work& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (output) return rdma_protocol::Drop::OPERATION;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->closing) return rdma_protocol::Drop::RETIRED;
    if (slot->spec.role != Role::SEND_WRITE || slot->state != State::PREPARING) return rdma_protocol::Drop::OPERATION;
    if (slot->work.load(std::memory_order_acquire) != 0) return rdma_protocol::Drop::PRESSURE;
    publish_work(handle.slot_, WorkKind::PREPARE, output);
    return rdma_protocol::Drop::NONE;
}

auto Operations::seal(Work& work) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    auto* slot = find(work);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    if (slot->closing) return rdma_protocol::Drop::RETIRED;
    if (work.kind_ != WorkKind::PREPARE || slot->state != State::PREPARING) return rdma_protocol::Drop::OPERATION;
    slot->state = State::READY;
    work.kind_ = WorkKind::RESULT;
    work.reset();
    return rdma_protocol::Drop::NONE;
}

auto Operations::begin_send(const Handle& handle, Work& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (output) return rdma_protocol::Drop::OPERATION;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->closing) return rdma_protocol::Drop::RETIRED;
    if (slot->state != State::READY || (slot->spec.role != Role::SEND_WRITE && slot->spec.role != Role::RECEIVE_READ)) {
        return rdma_protocol::Drop::OPERATION;
    }
    if (slot->work.load(std::memory_order_acquire) != 0) return rdma_protocol::Drop::PRESSURE;
    slot->state = State::WAIT_END;
    slot->send_attempted = true;
    publish_work(handle.slot_, WorkKind::SEND, output);
    return rdma_protocol::Drop::NONE;
}

auto Operations::finish_send(Work& work, int32_t status) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    auto* slot = find(work);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    if (work.kind_ != WorkKind::SEND || status > 0 || (slot->state != State::WAIT_END && slot->state != State::TERMINAL)) {
        return rdma_protocol::Drop::OPERATION;
    }
    if (status < 0) close_locked(*slot, status);
    work.kind_ = WorkKind::RESULT;
    work.reset();
    return rdma_protocol::Drop::NONE;
}

auto Operations::admit(const rdma_registry::Binding& current, const rdma_protocol::Header& header, std::span<const uint8_t> plaintext)
    -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_ || stopping_) return rdma_protocol::Drop::RETIRED;
    if (plaintext.size() != header.payload_length || (plaintext.size() != 0 && plaintext.data() == nullptr)) {
        return rdma_protocol::Drop::LENGTH;
    }
    auto const VALID = rdma_protocol::validate_header(header, rdma_protocol::HEADER_BYTES + plaintext.size());
    if (VALID != rdma_protocol::Drop::NONE) return VALID;
    if (!valid_binding(current) || header.src_node != current.peer_node || header.dst_node != current.owner_node) {
        return rdma_protocol::Drop::IDENTITY;
    }
    size_t index = OPERATION_SLOTS;
    for (size_t candidate = 0; candidate < slots_.size(); ++candidate) {
        const auto& slot = slots_[candidate];
        if (slot.state != State::FREE && slot.spec.operation_id == header.operation_id &&
            slot.spec.receive_ticket == header.receive_ticket && same_binding(slot.spec.binding, current)) {
            index = candidate;
            break;
        }
    }
    if (index == OPERATION_SLOTS) return rdma_protocol::Drop::OPERATION;
    auto& slot = slots_[index];
    settle_abandoned_locked(slot);
    if (slot.closing) return rdma_protocol::Drop::RETIRED;
    auto const GRANT = rdma_protocol::validate_grant(header, slot.spec.grant);
    if (GRANT != rdma_protocol::Drop::NONE) return GRANT;
    if (header.region_offset != slot.spec.offset) return rdma_protocol::Drop::RANGE;
    auto const OPCODE = static_cast<rdma_protocol::Opcode>(header.opcode);
    if (incoming(slot.spec.role)) {
        if (slot.consumer_generation == 0) return rdma_protocol::Drop::OPERATION;
        if (slot.state != State::ARMED && slot.state != State::RECEIVING) return rdma_protocol::Drop::OPERATION;
        if (header.flags != slot.spec.flags || header.total_length != slot.spec.requested) return rdma_protocol::Drop::OPERATION;
        if (slot.spec.role == Role::RECEIVE_WRITE) {
            if (OPCODE != rdma_protocol::Opcode::WRITE) return rdma_protocol::Drop::OPERATION;
            auto coverage = slot.coverage;
            auto const ACCEPTED = rdma_protocol::coverage_admit(coverage, header.fragment_offset, header.payload_length);
            if (ACCEPTED != rdma_protocol::Drop::NONE) return ACCEPTED;
            std::memcpy(scratch_.data() + index * rdma_protocol::MAX_TRANSFER_BYTES + header.fragment_offset, plaintext.data(),
                        plaintext.size());
            slot.coverage = coverage;
            slot.state = rdma_protocol::coverage_complete(slot.coverage) ? State::READY : State::RECEIVING;
        } else {
            if ((slot.spec.role == Role::SERVE_READ && OPCODE != rdma_protocol::Opcode::READ) ||
                (slot.spec.role == Role::RECEIVE_NOTIFY && OPCODE != rdma_protocol::Opcode::NOTIFY)) {
                return rdma_protocol::Drop::OPERATION;
            }
            slot.state = State::READY;
        }
        return rdma_protocol::Drop::NONE;
    }
    if (slot.state != State::WAIT_END || header.flags != rdma_protocol::FLAG_RESPONSE) return rdma_protocol::Drop::OPERATION;
    if (slot.spec.role == Role::SEND_WRITE) {
        if (OPCODE != rdma_protocol::Opcode::WRITE_END || header.total_length != slot.spec.requested) return rdma_protocol::Drop::OPERATION;
        slot.result = {.status = header.status,
                       .bytes = header.status == 0 ? slot.spec.requested : 0,
                       .outcome = header.status == 0 ? Outcome::EXACT : Outcome::UNCERTAIN};
        slot.state = State::TERMINAL;
        slot.closing = true;
        return rdma_protocol::Drop::NONE;
    }
    if (slot.spec.role != Role::RECEIVE_READ || (OPCODE != rdma_protocol::Opcode::READ_DATA && OPCODE != rdma_protocol::Opcode::READ_END) ||
        header.total_length > slot.spec.requested) {
        return rdma_protocol::Drop::OPERATION;
    }
    if (slot.returned_length_set && slot.coverage.length != header.total_length) return rdma_protocol::Drop::OPERATION;
    auto coverage = slot.coverage;
    if (!slot.returned_length_set) static_cast<void>(rdma_protocol::coverage_begin(coverage, header.total_length));
    if (OPCODE == rdma_protocol::Opcode::READ_DATA) {
        auto const ACCEPTED = rdma_protocol::coverage_admit(coverage, header.fragment_offset, header.payload_length);
        if (ACCEPTED != rdma_protocol::Drop::NONE) return ACCEPTED;
        std::memcpy(scratch_.data() + index * rdma_protocol::MAX_TRANSFER_BYTES + header.fragment_offset, plaintext.data(),
                    plaintext.size());
    } else {
        if (slot.end_seen) return rdma_protocol::Drop::DUPLICATE;
        slot.end_seen = true;
        slot.result.status = header.status;
    }
    slot.coverage = coverage;
    slot.returned_length_set = true;
    if (slot.end_seen && rdma_protocol::coverage_complete(slot.coverage)) {
        slot.result.bytes = slot.coverage.length;
        slot.result.outcome = Outcome::EXACT;
        slot.state = State::TERMINAL;
        slot.closing = true;
    }
    return rdma_protocol::Drop::NONE;
}

auto Operations::take_effect(const Handle& handle, Work& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (output) return rdma_protocol::Drop::OPERATION;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->closing) return rdma_protocol::Drop::RETIRED;
    if (!incoming(slot->spec.role) || slot->consumer_generation == 0 || slot->state != State::READY) return rdma_protocol::Drop::OPERATION;
    if (slot->work.load(std::memory_order_acquire) != 0) return rdma_protocol::Drop::PRESSURE;
    auto const CLAIM = slot->lease.claim();
    if (CLAIM != rdma_protocol::Drop::NONE) return CLAIM;
    slot->state = State::CLAIMED;
    publish_work(handle.slot_, WorkKind::EFFECT, output);
    return rdma_protocol::Drop::NONE;
}

auto Operations::finish_effect(Work& work, const Result& result) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    auto* slot = find(work);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    if (work.kind_ != WorkKind::EFFECT || slot->state != State::CLAIMED || result.status > 0 || result.bytes > slot->spec.requested ||
        (result.outcome != Outcome::NO_EFFECT && result.outcome != Outcome::EXACT && result.outcome != Outcome::UNCERTAIN)) {
        return rdma_protocol::Drop::OPERATION;
    }
    slot->result = result;
    slot->closing = true;
    slot->state = State::TERMINAL;
    work.kind_ = WorkKind::RESULT;
    work.reset();
    return rdma_protocol::Drop::NONE;
}

auto Operations::observe(const Handle& handle, Result& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->state != State::TERMINAL || slot->work.load(std::memory_order_acquire) != 0) return rdma_protocol::Drop::PRESSURE;
    output = slot->result;
    return rdma_protocol::Drop::NONE;
}

auto Operations::take_result(const Handle& handle, Work& output, Result& result) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (output) return rdma_protocol::Drop::OPERATION;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    if (slot->state != State::TERMINAL || slot->work.load(std::memory_order_acquire) != 0) return rdma_protocol::Drop::PRESSURE;
    publish_work(handle.slot_, WorkKind::RESULT, output);
    result = slot->result;
    return rdma_protocol::Drop::NONE;
}

void Operations::close_locked(Slot& slot, int32_t status) {
    if (slot.state == State::FREE || slot.state == State::ERASING) return;
    slot.closing = true;
    if (slot.state == State::TERMINAL || slot.state == State::CLAIMED) return;
    slot.result = {.status = status,
                   .bytes = 0,
                   .outcome = slot.spec.role == Role::SEND_WRITE && slot.send_attempted ? Outcome::UNCERTAIN : Outcome::NO_EFFECT};
    slot.state = State::TERMINAL;
}

void Operations::settle_abandoned_locked(Slot& slot) {
    if (slot.work.load(std::memory_order_acquire) != 0 || !slot.abandoned.exchange(false, std::memory_order_acq_rel)) return;
    if (slot.state == State::CLAIMED) {
        slot.closing = true;
        slot.result = {.status = -EIO, .bytes = 0, .outcome = Outcome::UNCERTAIN};
        slot.state = State::TERMINAL;
    } else {
        close_locked(slot, -EIO);
    }
}

auto Operations::cancel(const Handle& handle, int32_t status) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (status >= 0) return rdma_protocol::Drop::OPERATION;
    auto* slot = find(handle);
    if (slot == nullptr) return rdma_protocol::Drop::RETIRED;
    settle_abandoned_locked(*slot);
    close_locked(*slot, status);
    return rdma_protocol::Drop::NONE;
}

auto Operations::close_binding(const rdma_registry::Binding& binding, int32_t status) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!valid_binding(binding) || status >= 0) return rdma_protocol::Drop::OPERATION;
    for (auto& slot : slots_) {
        settle_abandoned_locked(slot);
        if (slot.state != State::FREE && same_binding(binding, slot.spec.binding)) close_locked(slot, status);
    }
    return rdma_protocol::Drop::NONE;
}

auto Operations::expire(uint64_t now_us, int32_t status) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (status >= 0) return rdma_protocol::Drop::OPERATION;
    for (auto& slot : slots_) {
        settle_abandoned_locked(slot);
        if (slot.state != State::FREE && slot.spec.deadline_us <= now_us) close_locked(slot, status);
    }
    return rdma_protocol::Drop::NONE;
}

void Operations::release(Handle& handle) {
    if (handle.owner_ == this && handle.slot_ < OPERATION_SLOTS) slots_[handle.slot_].handles.fetch_sub(1, std::memory_order_release);
}
void Operations::release(Work& work) {
    if (work.owner_ != this || work.slot_ >= OPERATION_SLOTS) return;
    auto& slot = slots_[work.slot_];
    if (work.kind_ == WorkKind::PREPARE || work.kind_ == WorkKind::SEND || work.kind_ == WorkKind::EFFECT) {
        slot.abandoned.store(true, std::memory_order_release);
    }
    slot.work.fetch_sub(1, std::memory_order_release);
}

auto Operations::begin_erase(EraseWork& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (output) return rdma_protocol::Drop::OPERATION;
    for (size_t index = 0; index < slots_.size(); ++index) {
        auto& slot = slots_[index];
        settle_abandoned_locked(slot);
        if (slot.state != State::TERMINAL || !slot.closing || slot.handles.load(std::memory_order_acquire) != 0 ||
            slot.work.load(std::memory_order_acquire) != 0) {
            continue;
        }
        slot.state = State::ERASING;
        slot.work.store(1, std::memory_order_relaxed);
        output.owner_ = this;
        output.slot_ = index;
        output.generation_ = slot.generation;
        output.staging_ = scratch_.subspan(index * rdma_protocol::MAX_TRANSFER_BYTES, rdma_protocol::MAX_TRANSFER_BYTES);
        output.lease_ = std::move(slot.lease);
        return rdma_protocol::Drop::NONE;
    }
    return rdma_protocol::Drop::PRESSURE;
}

auto Operations::finish_erase(EraseWork& work) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (work.owner_ != this || work.slot_ >= OPERATION_SLOTS) return rdma_protocol::Drop::RETIRED;
    auto& slot = slots_[work.slot_];
    if (slot.state != State::ERASING || slot.generation != work.generation_) return rdma_protocol::Drop::RETIRED;
    work.lease_.reset();
    auth_crypto::secure_erase(&slot.spec, sizeof(slot.spec));
    auth_crypto::secure_erase(&slot.coverage, sizeof(slot.coverage));
    auth_crypto::secure_erase(&slot.result, sizeof(slot.result));
    slot.closing = false;
    slot.end_seen = false;
    slot.returned_length_set = false;
    slot.send_attempted = false;
    slot.generation = 0;
    slot.consumer_generation = 0;
    slot.abandoned.store(false, std::memory_order_relaxed);
    slot.work.store(0, std::memory_order_relaxed);
    slot.state = State::FREE;
    work.owner_ = nullptr;
    work.slot_ = OPERATION_SLOTS;
    work.generation_ = 0;
    work.staging_ = {};
    return rdma_protocol::Drop::NONE;
}

auto Operations::statistics(Statistics& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    Statistics candidate{};
    for (auto& slot : slots_) {
        settle_abandoned_locked(slot);
        if (slot.state == State::FREE) continue;
        ++candidate.live;
        if (slot.consumer_generation != 0) ++candidate.armed;
        if (slot.state == State::CLAIMED) ++candidate.claimed;
        if (slot.state == State::TERMINAL) ++candidate.terminal;
        if (slot.state == State::ERASING) ++candidate.erasing;
        candidate.handles = add_count(candidate.handles, slot.handles.load(std::memory_order_acquire));
        candidate.work = add_count(candidate.work, slot.work.load(std::memory_order_acquire));
    }
    output = candidate;
    return rdma_protocol::Drop::NONE;
}

auto Operations::shutdown() -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_) return initialized_once_ ? rdma_protocol::Drop::NONE : rdma_protocol::Drop::RETIRED;
    stopping_ = true;
    bool live = false;
    for (auto& slot : slots_) {
        settle_abandoned_locked(slot);
        if (slot.state != State::FREE) {
            close_locked(slot, -EIO);
            live = true;
        }
    }
    if (live) return rdma_protocol::Drop::PRESSURE;
    scratch_ = {};
    initialized_ = false;
    return rdma_protocol::Drop::NONE;
}

}  // namespace ker::net::wki::rdma_operations
