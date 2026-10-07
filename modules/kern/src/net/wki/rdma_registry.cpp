#include "rdma_registry.hpp"

#include <utility>

namespace ker::net::wki::rdma_registry {

namespace {

class TryGuard final {
   public:
    explicit TryGuard(std::atomic_flag& flag) : flag_(flag), held_(!flag.test_and_set(std::memory_order_acquire)) {}
    ~TryGuard() {
        if (held_) flag_.clear(std::memory_order_release);
    }
    TryGuard(const TryGuard&) = delete;
    auto operator=(const TryGuard&) -> TryGuard& = delete;
    explicit operator bool() const { return held_; }

   private:
    std::atomic_flag& flag_;
    bool held_;
};

template <size_t Size>
auto nonzero(const std::array<uint8_t, Size>& value) -> bool {
    uint8_t combined = 0;
    for (auto byte : value) combined |= byte;
    return combined != 0;
}

auto valid_node(uint16_t node) -> bool { return node != 0 && node != UINT16_MAX; }

auto valid_binding(const Binding& binding) -> bool {
    return valid_node(binding.owner_node) && valid_node(binding.peer_node) && binding.owner_node != binding.peer_node &&
           nonzero(binding.session_id) && binding.secure_incarnation != 0 && binding.transport_cookie != 0 &&
           binding.transport_generation != 0 && binding.channel_cookie != 0 && binding.channel_generation != 0 && binding.channel_id != 0 &&
           rdma_protocol::transport_bit(binding.transport) != 0 && rdma_protocol::service_bit(binding.service) != 0;
}

auto same_binding(const Binding& lhs, const Binding& rhs) -> bool {
    return lhs.owner_node == rhs.owner_node && lhs.peer_node == rhs.peer_node && lhs.session_id == rhs.session_id &&
           lhs.secure_incarnation == rhs.secure_incarnation && lhs.transport_cookie == rhs.transport_cookie &&
           lhs.transport_generation == rhs.transport_generation && lhs.channel_cookie == rhs.channel_cookie &&
           lhs.channel_generation == rhs.channel_generation && lhs.channel_id == rhs.channel_id && lhs.transport == rhs.transport &&
           lhs.service == rhs.service;
}

template <class Slots>
auto find_slot(const Slots& slots, uint32_t rkey, uint64_t generation) -> size_t {
    for (size_t index = 0; index < slots.size(); ++index) {
        const auto& slot = slots[index];
        if (slot.active && slot.descriptor.rkey == rkey && slot.descriptor.generation == generation) return index;
    }
    return slots.size();
}

auto add_count(uint32_t lhs, uint32_t rhs) -> uint32_t { return rhs > UINT32_MAX - lhs ? UINT32_MAX : lhs + rhs; }

}  // namespace

Lease::~Lease() { reset(); }

Lease::Lease(Lease&& other) noexcept { *this = std::move(other); }

auto Lease::operator=(Lease&& other) noexcept -> Lease& {
    if (this == &other) return *this;
    reset();
    registry_ = std::exchange(other.registry_, nullptr);
    slot_ = std::exchange(other.slot_, REGION_SLOTS);
    generation_ = std::exchange(other.generation_, 0);
    claimed_ = std::exchange(other.claimed_, false);
    descriptor_ = other.descriptor_;
    auth_crypto::secure_erase(&other.descriptor_, sizeof(other.descriptor_));
    return *this;
}

auto Lease::claim() -> rdma_protocol::Drop { return registry_ != nullptr ? registry_->claim(*this) : rdma_protocol::Drop::RETIRED; }

auto Lease::memory() const -> ReleasedRegion { return registry_ != nullptr ? registry_->memory(*this) : ReleasedRegion{}; }

void Lease::reset() {
    if (registry_ != nullptr) registry_->release(*this);
    registry_ = nullptr;
    slot_ = REGION_SLOTS;
    generation_ = 0;
    claimed_ = false;
    auth_crypto::secure_erase(&descriptor_, sizeof(descriptor_));
}

Registry::~Registry() {
    // The owner must drain leases before destroying this object. Registry
    // destruction never frees caller-owned backing or substitutes for reclaim.
    auth_crypto::secure_erase(capability_root_.data(), capability_root_.size());
    for (auto& slot : slots_) {
        auth_crypto::secure_erase(&slot.binding, sizeof(slot.binding));
        auth_crypto::secure_erase(&slot.descriptor, sizeof(slot.descriptor));
    }
}

auto Registry::initialize(uint16_t owner, const rdma_crypto::Key& root) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (initialized_once_) return rdma_protocol::Drop::RETIRED;
    if (!valid_node(owner)) return rdma_protocol::Drop::IDENTITY;
    if (!nonzero(root)) return rdma_protocol::Drop::GRANT;
    capability_root_ = root;
    owner_ = owner;
    initialized_ = true;
    initialized_once_ = true;
    return rdma_protocol::Drop::NONE;
}

auto Registry::register_region(const Registration& registration, rdma_protocol::RegionDescriptor& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_ || stopping_) return rdma_protocol::Drop::RETIRED;
    if (!valid_binding(registration.binding) || registration.binding.owner_node != owner_) return rdma_protocol::Drop::IDENTITY;
    if (registration.backing == nullptr || registration.size == 0 || registration.binding_generation == 0) {
        return rdma_protocol::Drop::GRANT;
    }
    if (registration.permissions == 0 || (registration.permissions & ~rdma_protocol::PERMISSIONS) != 0) {
        return rdma_protocol::Drop::PERMISSION;
    }
    if (registration.size > UINTPTR_MAX - reinterpret_cast<uintptr_t>(registration.backing)) return rdma_protocol::Drop::RANGE;
    if (registration.size > REGISTERED_BYTES - bytes_) return rdma_protocol::Drop::PRESSURE;
    size_t available = REGION_SLOTS;
    for (size_t index = 0; index < slots_.size(); ++index) {
        if (!slots_[index].active) {
            available = index;
            break;
        }
    }
    if (available == REGION_SLOTS) return rdma_protocol::Drop::PRESSURE;
    if (next_rkey_ == 0 || next_rkey_ == UINT32_MAX || next_generation_ == 0 || next_generation_ == UINT64_MAX) {
        if (next_rkey_ == UINT32_MAX) next_rkey_ = 0;
        if (next_generation_ == UINT64_MAX) next_generation_ = 0;
        return rdma_protocol::Drop::COUNTER;
    }

    rdma_protocol::RegionDescriptor candidate{};
    candidate.rkey = next_rkey_;
    candidate.size = registration.size;
    candidate.generation = next_generation_;
    candidate.binding_generation = registration.binding_generation;
    candidate.session_id = registration.binding.session_id;
    candidate.channel_id = registration.binding.channel_id;
    candidate.transport = static_cast<uint8_t>(registration.binding.transport);
    candidate.service = static_cast<uint8_t>(registration.binding.service);
    candidate.permissions = registration.permissions;
    if (!rdma_protocol::derive_capability(capability_root_, owner_, registration.binding.peer_node,
                                          registration.binding.transport_generation, candidate, candidate.capability)) {
        return rdma_protocol::Drop::GRANT;
    }

    auto& slot = slots_[available];
    slot.binding = registration.binding;
    slot.descriptor = candidate;
    slot.backing = registration.backing;
    slot.closing = false;
    slot.active = true;
    bytes_ += registration.size;
    ++next_rkey_;
    ++next_generation_;
    output = candidate;
    auth_crypto::secure_erase(&candidate, sizeof(candidate));
    return rdma_protocol::Drop::NONE;
}

auto Registry::admit(const Binding& expected, const rdma_protocol::Header& header, Lease& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_ || stopping_) return rdma_protocol::Drop::RETIRED;
    if (output) return rdma_protocol::Drop::OPERATION;
    rdma_protocol::Drop const HEADER = rdma_protocol::validate_header(header, rdma_protocol::HEADER_BYTES + header.payload_length);
    if (HEADER != rdma_protocol::Drop::NONE) return HEADER;
    auto const OPCODE = static_cast<rdma_protocol::Opcode>(header.opcode);
    if (OPCODE != rdma_protocol::Opcode::READ && OPCODE != rdma_protocol::Opcode::WRITE && OPCODE != rdma_protocol::Opcode::NOTIFY) {
        return rdma_protocol::Drop::OPERATION;
    }
    if (!valid_binding(expected) || expected.owner_node != owner_ || header.src_node != expected.peer_node ||
        header.dst_node != expected.owner_node) {
        return rdma_protocol::Drop::IDENTITY;
    }
    size_t const INDEX = find_slot(slots_, header.rkey, header.region_generation);
    if (INDEX == REGION_SLOTS) return rdma_protocol::Drop::GRANT;
    auto& slot = slots_[INDEX];
    if (slot.closing) return rdma_protocol::Drop::RETIRED;
    if (!same_binding(expected, slot.binding)) return rdma_protocol::Drop::IDENTITY;
    rdma_protocol::Drop const GRANT = rdma_protocol::validate_grant(header, slot.descriptor);
    if (GRANT != rdma_protocol::Drop::NONE) return GRANT;
    if (slot.references.load(std::memory_order_relaxed) == UINT32_MAX) return rdma_protocol::Drop::PRESSURE;
    slot.references.fetch_add(1, std::memory_order_relaxed);
    output.registry_ = this;
    output.slot_ = INDEX;
    output.generation_ = slot.descriptor.generation;
    output.descriptor_ = slot.descriptor;
    return rdma_protocol::Drop::NONE;
}

auto Registry::claim(Lease& lease) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (lease.registry_ != this || lease.slot_ >= REGION_SLOTS) return rdma_protocol::Drop::RETIRED;
    auto& slot = slots_[lease.slot_];
    if (lease.claimed_) return rdma_protocol::Drop::NONE;
    if (!initialized_ || stopping_ || !slot.active || slot.closing || slot.descriptor.generation != lease.generation_) {
        return rdma_protocol::Drop::RETIRED;
    }
    if (slot.claims.load(std::memory_order_relaxed) == UINT32_MAX) return rdma_protocol::Drop::PRESSURE;
    slot.claims.fetch_add(1, std::memory_order_relaxed);
    lease.claimed_ = true;
    return rdma_protocol::Drop::NONE;
}

auto Registry::memory(const Lease& lease) const -> ReleasedRegion {
    if (lease.registry_ != this || !lease.claimed_ || lease.slot_ >= REGION_SLOTS) return {};
    // The lease reference pins this immutable backing even after close. Never
    // read mutable active/closing metadata outside the registry guard.
    return {.backing = slots_[lease.slot_].backing, .size = lease.descriptor_.size};
}

void Registry::release(Lease& lease) {
    if (lease.registry_ != this || lease.slot_ >= REGION_SLOTS) return;
    auto& slot = slots_[lease.slot_];
    if (lease.claimed_) slot.claims.fetch_sub(1, std::memory_order_release);
    // The final reference release orders every claimed memory effect before
    // the reclaiming owner's acquire load and subsequent wipe/free.
    slot.references.fetch_sub(1, std::memory_order_release);
}

auto Registry::revoke(uint32_t rkey, uint64_t generation) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    size_t const INDEX = find_slot(slots_, rkey, generation);
    if (INDEX == REGION_SLOTS) return rdma_protocol::Drop::GRANT;
    slots_[INDEX].closing = true;
    return rdma_protocol::Drop::NONE;
}

auto Registry::close_peer(uint16_t peer, const rdma_protocol::SessionId& session) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!valid_node(peer) || !nonzero(session)) return rdma_protocol::Drop::IDENTITY;
    for (auto& slot : slots_) {
        if (slot.active && slot.binding.peer_node == peer && slot.binding.session_id == session) slot.closing = true;
    }
    return rdma_protocol::Drop::NONE;
}

auto Registry::close_transport(uintptr_t cookie, uint64_t generation) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (cookie == 0 || generation == 0) return rdma_protocol::Drop::IDENTITY;
    for (auto& slot : slots_) {
        if (slot.active && slot.binding.transport_cookie == cookie && slot.binding.transport_generation == generation) slot.closing = true;
    }
    return rdma_protocol::Drop::NONE;
}

auto Registry::reclaim(uint32_t rkey, uint64_t generation, ReleasedRegion& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    size_t const INDEX = find_slot(slots_, rkey, generation);
    if (INDEX == REGION_SLOTS) return rdma_protocol::Drop::GRANT;
    auto& slot = slots_[INDEX];
    slot.closing = true;
    if (slot.references.load(std::memory_order_acquire) != 0 || slot.claims.load(std::memory_order_acquire) != 0) {
        return rdma_protocol::Drop::PRESSURE;
    }
    output = {.backing = slot.backing, .size = slot.descriptor.size};
    bytes_ -= slot.descriptor.size;
    slot.backing = nullptr;
    slot.active = false;
    slot.closing = false;
    auth_crypto::secure_erase(&slot.binding, sizeof(slot.binding));
    auth_crypto::secure_erase(&slot.descriptor, sizeof(slot.descriptor));
    return rdma_protocol::Drop::NONE;
}

auto Registry::statistics(Statistics& output) -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    Statistics candidate{.bytes = bytes_};
    for (const auto& slot : slots_) {
        if (!slot.active) continue;
        ++candidate.live;
        if (slot.closing) ++candidate.closing;
        candidate.references = add_count(candidate.references, slot.references.load(std::memory_order_acquire));
        candidate.claims = add_count(candidate.claims, slot.claims.load(std::memory_order_acquire));
    }
    output = candidate;
    return rdma_protocol::Drop::NONE;
}

auto Registry::shutdown() -> rdma_protocol::Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return rdma_protocol::Drop::PRESSURE;
    if (!initialized_) return initialized_once_ ? rdma_protocol::Drop::NONE : rdma_protocol::Drop::RETIRED;
    stopping_ = true;
    bool live = false;
    for (auto& slot : slots_) {
        if (!slot.active) continue;
        slot.closing = true;
        live = true;
    }
    if (live) return rdma_protocol::Drop::PRESSURE;
    auth_crypto::secure_erase(capability_root_.data(), capability_root_.size());
    owner_ = 0;
    initialized_ = false;
    return rdma_protocol::Drop::NONE;
}

}  // namespace ker::net::wki::rdma_registry
