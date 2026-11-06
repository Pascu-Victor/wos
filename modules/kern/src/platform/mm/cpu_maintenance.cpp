#include "cpu_maintenance.hpp"

#include <atomic>
#include <utility>

#include "cpu_maintenance_controller.hpp"
#include "cpu_maintenance_platform.hpp"

namespace ker::mod::mm::cpu_maintenance {
namespace detail {
// Stack-only dispatch; never placed in a Slot, Ticket or asynchronous handler.
class ControllerCall {
    const Controller* controller_{};
    uint64_t saved_flags_{};
    bool boot_{};

   public:
    ControllerCall() = default;
    explicit ControllerCall(const Controller& controller)
        : controller_(&controller), boot_(controller.origin_ == Controller::Origin::boot) {
        if (boot_) saved_flags_ = platform::save_disable_interrupts();
    }
    ~ControllerCall() {
        if (boot_) platform::restore_interrupts(saved_flags_);
    }
    ControllerCall(const ControllerCall&) = delete;
    auto operator=(const ControllerCall&) -> ControllerCall& = delete;
    [[nodiscard]] auto status() const -> Status {
        if (controller_ != nullptr) return controller_->context_status();
        return platform::controller_context_ready() ? Status::ready : Status::invalid_context;
    }
    [[nodiscard]] auto ready() const -> bool { return status() == Status::ready; }
    [[nodiscard]] auto private_request() const -> bool { return controller_ != nullptr; }
    void yield() const {
        if (boot_)
            platform::spin_pause();
        else
            platform::controller_yield();
    }
};
namespace {
constexpr uint64_t CLOSED = uint64_t{1} << 63;
constexpr uint64_t MAX_READERS = CLOSED - 1;
constexpr uint64_t REQUEST_ID_LIMIT = UINT64_MAX;
constexpr size_t WAIT_STEPS = 1'000'000;
constexpr size_t YIELD_STEPS = 1024;
constexpr unsigned RETRIES = 8;
constexpr uint64_t RETRY_NS = 1'000'000;
using CacheStatus = mmio_cache::Status;

class InterruptGuard {
    uint64_t flags_{platform::save_disable_interrupts()};

   public:
    InterruptGuard() = default;
    ~InterruptGuard() { platform::restore_interrupts(flags_); }
    InterruptGuard(const InterruptGuard&) = delete;
    auto operator=(const InterruptGuard&) -> InterruptGuard& = delete;
};

struct Round {
    std::array<std::atomic<uint64_t>, CPU_CAPACITY> claimed{};
    std::array<std::atomic<uint64_t>, CPU_CAPACITY> completed{};
    std::array<std::atomic<CacheStatus>, CPU_CAPACITY> status{};
    std::array<std::atomic<uint32_t>, CPU_CAPACITY> apic{};
    std::array<std::atomic<uint64_t>, CPU_CAPACITY> cr3{};
};

// Only overloads consuming genuine private-issued owners can select a policy.
// The immutable payload survives every published timeout and handler tail.
class QualifiedRange {
    mmio_cache::QualifiedUcRange uc_{};
    mmio_cache::QualifiedWcRange wc_{};

   public:
    [[nodiscard]] auto empty() const noexcept -> bool { return uc_.empty() && wc_.empty(); }
    [[nodiscard]] auto valid() const noexcept -> bool { return uc_.empty() != wc_.empty(); }
    void adopt(mmio_cache::QualifiedUcRange&& range) noexcept { uc_ = std::move(range); }
    void adopt(mmio_cache::QualifiedWcRange&& range) noexcept { wc_ = std::move(range); }
    void clear() noexcept {
        uc_ = mmio_cache::QualifiedUcRange{};
        wc_ = mmio_cache::QualifiedWcRange{};
    }
    [[nodiscard]] auto physical() const noexcept -> uint64_t { return wc_.empty() ? uc_.physical() : wc_.physical(); }
    [[nodiscard]] auto bytes() const noexcept -> uint64_t { return wc_.empty() ? uc_.bytes() : wc_.bytes(); }
    [[nodiscard]] auto incarnation() const noexcept -> uint64_t { return wc_.empty() ? uc_.incarnation() : wc_.incarnation(); }
    [[nodiscard]] auto pat_index() const noexcept -> uint8_t {
        return wc_.empty() ? mmio_cache::QualifiedUcRange::PAT_INDEX : mmio_cache::QualifiedWcRange::PAT_INDEX;
    }
    [[nodiscard]] auto revalidate() const -> CacheStatus {
        if (!valid()) return CacheStatus::invalid_configuration;
        return wc_.empty() ? mmio_cache::revalidate_current_cpu(uc_) : mmio_cache::revalidate_current_cpu(wc_);
    }
};

struct Slot {
    std::atomic<bool> used{};
    std::atomic<uint64_t> gate{CLOSED};
    std::atomic<Phase> phase{Phase::empty};
    std::atomic<bool> timed_out{};
    std::atomic<bool> abandoned{};
    std::atomic<CacheStatus> error{CacheStatus::ready};
    uint64_t id{};
    Kind kind{Kind::translations};
    bool private_request{};
    QualifiedRange range{};
    uint64_t targets{};
    std::array<uint32_t, CPU_CAPACITY> target_apic{};
    std::array<Round, 2> rounds{};
    std::array<std::atomic<uint64_t>, 2> defer{};
    std::array<std::atomic<uint64_t>, 2> withheld{};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free && std::atomic<Phase>::is_always_lock_free &&
              std::atomic<CacheStatus>::is_always_lock_free && std::atomic<uint8_t>::is_always_lock_free);

auto round_index(Phase phase) -> size_t { return phase == Phase::caches ? 1 : 0; }
auto retry_deadline(uint64_t now) -> uint64_t { return now > UINT64_MAX - RETRY_NS ? UINT64_MAX : now + RETRY_NS; }

auto all_completed(const Slot& slot, size_t round) -> bool {
    for (size_t i = 0; i < CPU_CAPACITY; ++i) {
        if ((slot.targets & (uint64_t{1} << i)) != 0 && slot.rounds[round].completed[i].load(std::memory_order_acquire) != slot.id)
            return false;
    }
    return true;
}

class Reader {
    Slot* slot_{};

   public:
    explicit Reader(Slot& slot) {
        uint64_t value = slot.gate.load(std::memory_order_acquire);
        if ((value & CLOSED) != 0 || value >= MAX_READERS) return;
        // Join precedes every ID/payload load; a pre-CAS scanner owns no old ID.
        if (!slot.gate.compare_exchange_strong(value, value + 1, std::memory_order_acq_rel, std::memory_order_acquire)) return;
        slot_ = &slot;
    }
    ~Reader() {
        // This release is the reader's final slot access, including ACK tails.
        if (slot_ != nullptr) slot_->gate.fetch_sub(1, std::memory_order_release);
    }
    Reader(const Reader&) = delete;
    auto operator=(const Reader&) -> Reader& = delete;
    [[nodiscard]] explicit operator bool() const { return slot_ != nullptr; }
};
}  // namespace

class Runtime {
    std::array<Slot, POOL_CAPACITY> slots_{};
    std::atomic<uint64_t> next_id_{1};
    std::atomic<uint8_t> initialization_{};
    std::atomic<uint64_t> expected_{};
    std::atomic<uint64_t> ready_{};
    std::atomic<uint64_t> protection_ready_{};
    std::atomic<CacheStatus> protection_error_{CacheStatus::ready};
    std::array<std::atomic<uint8_t>, CPU_CAPACITY> ready_state_{};
    std::array<std::atomic<uint32_t>, CPU_CAPACITY> apics_{};
    std::atomic<uint8_t> vector_{};
    std::atomic<Status> configuration_error_{Status::ready};
    std::atomic<CacheStatus> service_error_{CacheStatus::ready};
    std::atomic<uint64_t> active_{};

    void service_failure(CacheStatus status) {
        CacheStatus empty = CacheStatus::ready;
        (void)service_error_.compare_exchange_strong(empty, status, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    auto find(const Ticket& ticket) -> Slot* {
        if (ticket.empty() || ticket.slot_ >= slots_.size()) return nullptr;
        auto& slot = slots_[ticket.slot_];
        return slot.used.load(std::memory_order_acquire) && slot.id == ticket.id_ ? &slot : nullptr;
    }

    auto failed(const Slot& slot) const -> bool {
        if (service_error_.load(std::memory_order_acquire) != CacheStatus::ready ||
            slot.error.load(std::memory_order_acquire) != CacheStatus::ready)
            return true;
        for (const auto& round : slot.rounds) {
            for (size_t i = 0; i < CPU_CAPACITY; ++i) {
                auto const status = round.status[i].load(std::memory_order_acquire);
                if (status != CacheStatus::pending && status != CacheStatus::ready) return true;
            }
        }
        return false;
    }

    void clear(Slot& slot, Ticket& ticket) {
        slot.range.clear();
        slot.id = 0;
        slot.targets = 0;
        slot.phase.store(Phase::empty, std::memory_order_release);
        slot.used.store(false, std::memory_order_release);
        ticket.id_ = 0;
        ticket.slot_ = POOL_CAPACITY;
    }

    auto reclaimable(const Slot& slot) const -> bool {
        return slot.phase.load(std::memory_order_acquire) == Phase::complete && slot.gate.load(std::memory_order_acquire) == CLOSED &&
               all_completed(slot, 0) && (slot.kind == Kind::translations || all_completed(slot, 1)) && !failed(slot);
    }

    void finish(Slot& slot) {
        if (failed(slot)) return;
        auto phase = slot.phase.load(std::memory_order_acquire);
        if (phase != Phase::closing) {
            slot.phase.store(Phase::closing, std::memory_order_release);
            slot.gate.fetch_or(CLOSED, std::memory_order_acq_rel);
        }
        if (slot.gate.load(std::memory_order_acquire) != CLOSED || !all_completed(slot, 0) ||
            (slot.kind == Kind::translations_then_cache && !all_completed(slot, 1)) || failed(slot))
            return;
        slot.phase.store(Phase::complete, std::memory_order_release);
        active_.fetch_sub(1, std::memory_order_acq_rel);
    }

    void send(const Slot& slot, Phase phase) {
        size_t const round = round_index(phase);
        uint8_t const vector = vector_.load(std::memory_order_acquire);
        for (size_t i = 0; i < CPU_CAPACITY; ++i) {
            if ((slot.targets & (uint64_t{1} << i)) != 0 && slot.rounds[round].completed[i].load(std::memory_order_acquire) != slot.id)
                platform::send_ipi(slot.target_apic[i], vector);
        }
    }

    void service_slot(Slot& slot, size_t logical, uint32_t apic) {
        Reader reader(slot);
        if (!reader) return;
        uint64_t const id = slot.id;
        auto const phase = slot.phase.load(std::memory_order_acquire);
        uint64_t const bit = uint64_t{1} << logical;
        if ((phase != Phase::translations && phase != Phase::caches) || (slot.targets & bit) == 0 ||
            service_error_.load(std::memory_order_acquire) != CacheStatus::ready)
            return;
        size_t const index = round_index(phase);
#ifdef WOS_SELFTEST
        if ((slot.defer[index].load(std::memory_order_acquire) & bit) != 0) {
            slot.withheld[index].fetch_or(bit, std::memory_order_release);
            return;
        }
#endif
        if (phase == Phase::caches && !all_completed(slot, 0)) {
            slot.error.store(CacheStatus::inconsistent_policy, std::memory_order_release);
            return;
        }
        auto& row = slot.rounds[index];
        uint64_t empty = 0;
        if (!row.claimed[logical].compare_exchange_strong(empty, id, std::memory_order_acq_rel, std::memory_order_acquire)) return;
        row.apic[logical].store(apic, std::memory_order_relaxed);
        auto const result = slot.range.revalidate();
        if (result != CacheStatus::ready) {
            row.status[logical].store(result, std::memory_order_release);
            return;
        }
        if (slot.private_request) {
            auto const protection = platform::private_protection_status();
            if (protection != CacheStatus::ready) {
                row.status[logical].store(protection, std::memory_order_release);
                CacheStatus empty_error = CacheStatus::ready;
                (void)protection_error_.compare_exchange_strong(empty_error, protection, std::memory_order_acq_rel,
                                                                std::memory_order_acquire);
                return;
            }
        }
        uint64_t const cr3 = platform::execute_local(phase);
        row.cr3[logical].store(cr3, std::memory_order_relaxed);
        row.status[logical].store(CacheStatus::ready, std::memory_order_relaxed);
        row.completed[logical].store(id, std::memory_order_release);
        // Reader destruction below is the final slot access after this ACK.
    }

    auto snapshot(const Ticket& ticket, Status override_status = Status::ready) -> Report {
        Report value{};
        auto* slot = find(ticket);
        if (slot == nullptr) return value;
        auto const before = slot->phase.load(std::memory_order_acquire);
        value.phase = before;
        value.id = slot->id;
        value.kind = slot->kind;
        value.physical = slot->range.physical();
        value.bytes = slot->range.bytes();
        value.cohort_incarnation = slot->range.incarnation();
        value.pat_index = slot->range.pat_index();
        value.targets = slot->targets;
        value.timed_out_ever = slot->timed_out.load(std::memory_order_acquire);
        value.abandoned = slot->abandoned.load(std::memory_order_acquire);
        value.translation_withheld = slot->withheld[0].load(std::memory_order_acquire);
        value.cache_withheld = slot->withheld[1].load(std::memory_order_acquire);
        value.service_error = service_error_.load(std::memory_order_acquire);
        auto const local_error = slot->error.load(std::memory_order_acquire);
        if (value.service_error == CacheStatus::ready && local_error != CacheStatus::ready) value.service_error = local_error;
        for (size_t i = 0; i < CPU_CAPACITY; ++i) {
            if ((value.targets & (uint64_t{1} << i)) == 0) continue;
            auto& cpu = value.cpus[i];
            cpu.target_apic = slot->target_apic[i];
            cpu.translation_completed = slot->rounds[0].completed[i].load(std::memory_order_acquire);
            cpu.cache_completed = slot->rounds[1].completed[i].load(std::memory_order_acquire);
            cpu.translation_claimed = slot->rounds[0].claimed[i].load(std::memory_order_acquire);
            cpu.cache_claimed = slot->rounds[1].claimed[i].load(std::memory_order_acquire);
            cpu.translation_status = slot->rounds[0].status[i].load(std::memory_order_acquire);
            cpu.cache_status = slot->rounds[1].status[i].load(std::memory_order_acquire);
            cpu.translation_apic = slot->rounds[0].apic[i].load(std::memory_order_relaxed);
            cpu.cache_apic = slot->rounds[1].apic[i].load(std::memory_order_relaxed);
            cpu.translation_cr3 = slot->rounds[0].cr3[i].load(std::memory_order_relaxed);
            cpu.cache_cr3 = slot->rounds[1].cr3[i].load(std::memory_order_relaxed);
            uint64_t const bit = uint64_t{1} << i;
            if (cpu.translation_claimed == value.id) value.translation_claimed |= bit;
            if (cpu.translation_completed == value.id) value.translation_completed |= bit;
            if (cpu.cache_claimed == value.id) value.cache_claimed |= bit;
            if (cpu.cache_completed == value.id) value.cache_completed |= bit;
            if ((cpu.translation_status != CacheStatus::pending && cpu.translation_status != CacheStatus::ready) ||
                (cpu.cache_status != CacheStatus::pending && cpu.cache_status != CacheStatus::ready))
                value.failed |= bit;
        }
        uint64_t const gate = slot->gate.load(std::memory_order_acquire);
        value.readers = gate & ~CLOSED;
        value.admission_closed = (gate & CLOSED) != 0;
        if (slot->phase.load(std::memory_order_acquire) != before)
            value.status = Status::busy;
        else if (value.service_error != CacheStatus::ready || value.failed != 0)
            value.status = Status::failed_retained;
        else if (before == Phase::complete && value.admission_closed && value.readers == 0 &&
                 value.translation_completed == value.targets &&
                 (value.kind == Kind::translations || value.cache_completed == value.targets))
            value.status = Status::complete;
        else if (before == Phase::prepared)
            value.status = Status::ready;
        else
            value.status = Status::pending;
        if (override_status != Status::ready && value.status != Status::complete) value.status = override_status;
        return value;
    }

   public:
    auto initialize(uint8_t vector) -> Status {
        size_t const count = platform::configured_cpu_count();
        if (count == 0 || count > CPU_CAPACITY || vector < 32) {
            configuration_error_.store(Status::invalid_configuration, std::memory_order_release);
            return Status::invalid_configuration;
        }
        uint64_t const mask = count == CPU_CAPACITY ? UINT64_MAX : (uint64_t{1} << count) - 1;
        uint8_t empty = 0;
        if (!initialization_.compare_exchange_strong(empty, 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (empty == 1) return Status::busy;
            if (expected_.load(std::memory_order_acquire) != mask || vector_.load(std::memory_order_acquire) != vector)
                configuration_error_.store(Status::invalid_configuration, std::memory_order_release);
            return configuration_error_.load(std::memory_order_acquire);
        }
        expected_.store(mask, std::memory_order_relaxed);
        vector_.store(vector, std::memory_order_relaxed);
        initialization_.store(2, std::memory_order_release);
        return configuration_error_.load(std::memory_order_acquire);
    }

    auto ready_cpu() -> Status {
        InterruptGuard guard;
        auto const configured = service_observation();
        if (configured.status == Status::unconfigured || configured.status == Status::invalid_configuration ||
            configured.status == Status::busy || configured.status == Status::failed_retained)
            return configured.status;
        size_t logical = CPU_CAPACITY;
        uint32_t apic = UINT32_MAX;
        if (platform::current_cpu(logical, apic) != CacheStatus::ready || logical >= CPU_CAPACITY ||
            (configured.expected_mask & (uint64_t{1} << logical)) == 0 || !platform::current_root_is_kernel())
            return Status::not_ready;
        mmio_cache::CpuObservation observed{};
        if (!mmio_cache::cpu_observation(logical, observed) || observed.status != CacheStatus::ready || observed.logical != logical ||
            observed.apic != apic)
            return Status::not_ready;
        auto const protection = platform::private_protection_status();
        if (protection == CacheStatus::ready)
            protection_ready_.fetch_or(uint64_t{1} << logical, std::memory_order_release);
        else {
            protection_ready_.fetch_and(~(uint64_t{1} << logical), std::memory_order_release);
            CacheStatus empty_error = CacheStatus::ready;
            (void)protection_error_.compare_exchange_strong(empty_error, protection, std::memory_order_acq_rel, std::memory_order_acquire);
        }
        uint8_t empty = 0;
        if (!ready_state_[logical].compare_exchange_strong(empty, 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (empty == 1) return Status::busy;
            if (apics_[logical].load(std::memory_order_acquire) == apic) return Status::ready;
            configuration_error_.store(Status::invalid_configuration, std::memory_order_release);
            return Status::invalid_configuration;
        }
        apics_[logical].store(apic, std::memory_order_relaxed);
        ready_state_[logical].store(2, std::memory_order_release);
        ready_.fetch_or(uint64_t{1} << logical, std::memory_order_release);
        return Status::ready;
    }

    auto service_observation() const -> ServiceObservation {
        ServiceObservation value{};
        auto const state = initialization_.load(std::memory_order_acquire);
        value.expected_mask = expected_.load(std::memory_order_acquire);
        value.ready_mask = ready_.load(std::memory_order_acquire);
        value.vector = vector_.load(std::memory_order_acquire);
        value.service_error = service_error_.load(std::memory_order_acquire);
        value.status = configuration_error_.load(std::memory_order_acquire);
        if (value.status != Status::ready) return value;
        if (state == 0)
            value.status = Status::unconfigured;
        else if (state == 1)
            value.status = Status::busy;
        else if (value.service_error != CacheStatus::ready)
            value.status = Status::failed_retained;
        else if (value.ready_mask != value.expected_mask)
            value.status = Status::not_ready;
        return value;
    }

    auto protection_observation() const -> ProtectionObservation {
        return {protection_ready_.load(std::memory_order_acquire), protection_error_.load(std::memory_order_acquire)};
    }

    auto private_readiness(uint64_t targets, const ControllerCall& call) const -> Status {
        if (!call.private_request()) return Status::ready;
        auto const protection = protection_observation();
        if (protection.error != CacheStatus::ready) return Status::failed_retained;
        return protection.ready_mask == targets ? Status::ready : Status::not_ready;
    }

    auto await_ready(uint64_t deadline, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        uint64_t now = 0;
        if (!platform::clock_now_ns(now)) return Status::clock_unavailable;
        for (size_t step = 0; step < WAIT_STEPS; ++step) {
            if (!call.ready()) return Status::invalid_context;
            auto const state = service_observation();
            if (state.status != Status::ready && state.status != Status::not_ready && state.status != Status::busy) return state.status;
            auto const cohort = mmio_cache::cohort_observation();
            if (cohort.expected_mask != state.expected_mask || cohort.invalidated_mask != 0) return Status::failed_retained;
            if (cohort.status != CacheStatus::ready && cohort.status != CacheStatus::pending) return Status::failed_retained;
            auto const protection = private_readiness(state.expected_mask, call);
            if (protection == Status::failed_retained) return protection;
            if (state.status == Status::ready && cohort.status == CacheStatus::ready && cohort.terminal_mask == state.expected_mask &&
                cohort.ready_mask == state.expected_mask && protection == Status::ready)
                return Status::ready;
            if (!platform::clock_now_ns(now)) return Status::clock_unavailable;
            if (now >= deadline) return Status::not_ready;
            service();
            if ((step + 1) % YIELD_STEPS == 0)
                call.yield();
            else
                platform::spin_pause();
        }
        return Status::not_ready;
    }

    template <typename Range>
    auto prepare(Range&& range, Kind kind, Ticket& output, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        if (!output.empty()) return Status::output_not_empty;
        if (kind != Kind::translations && kind != Kind::translations_then_cache) return Status::invalid_kind;
        if (range.empty()) return Status::empty_range;
        auto const state = service_observation();
        if (state.status != Status::ready) return state.status;
        auto const protection = private_readiness(state.expected_mask, call);
        if (protection != Status::ready) return protection;
        auto const cohort = mmio_cache::cohort_observation();
        if (cohort.status != CacheStatus::ready || !cohort.sealed || cohort.incarnation != range.incarnation() ||
            cohort.expected_mask != state.expected_mask)
            return Status::not_ready;
        size_t chosen = POOL_CAPACITY;
        for (size_t i = 0; i < slots_.size(); ++i) {
            bool empty = false;
            if (slots_[i].used.compare_exchange_strong(empty, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
                chosen = i;
                break;
            }
        }
        if (chosen == POOL_CAPACITY) return Status::capacity;
        auto& slot = slots_[chosen];
        if (!slot.range.empty()) {
            slot.used.store(false, std::memory_order_release);
            return Status::failed_retained;
        }
        uint64_t id = next_id_.load(std::memory_order_acquire);
        if (id == 0 || id >= REQUEST_ID_LIMIT) {
            slot.used.store(false, std::memory_order_release);
            return Status::generation_exhausted;
        }
        if (!next_id_.compare_exchange_strong(id, id + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            slot.used.store(false, std::memory_order_release);
            return Status::busy;
        }
        slot.id = id;
        slot.kind = kind;
        slot.private_request = call.private_request();
        slot.targets = state.expected_mask;
        slot.timed_out.store(false, std::memory_order_relaxed);
        slot.abandoned.store(false, std::memory_order_relaxed);
        slot.error.store(CacheStatus::ready, std::memory_order_relaxed);
        for (size_t i = 0; i < CPU_CAPACITY; ++i) {
            slot.target_apic[i] = apics_[i].load(std::memory_order_acquire);
            for (auto& round : slot.rounds) {
                round.claimed[i].store(0, std::memory_order_relaxed);
                round.completed[i].store(0, std::memory_order_relaxed);
                round.status[i].store(CacheStatus::pending, std::memory_order_relaxed);
                round.apic[i].store(UINT32_MAX, std::memory_order_relaxed);
                round.cr3[i].store(0, std::memory_order_relaxed);
            }
        }
        for (size_t i = 0; i < 2; ++i) {
            slot.defer[i].store(0, std::memory_order_relaxed);
            slot.withheld[i].store(0, std::memory_order_relaxed);
        }
        slot.range.adopt(std::move(range));
        slot.phase.store(Phase::prepared, std::memory_order_release);
        output = Ticket(chosen, id);
        return Status::ready;
    }

    auto publish(Ticket& ticket, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        auto* slot = find(ticket);
        if (slot == nullptr) return Status::invalid_ticket;
        if (slot->phase.load(std::memory_order_acquire) != Phase::prepared) return Status::invalid_ticket;
        if (!slot->range.valid()) return Status::not_ready;
        auto const state = service_observation();
        if (state.status != Status::ready) return state.status;
        auto const protection = private_readiness(state.expected_mask, call);
        if (protection != Status::ready) return protection;
        auto const cohort = mmio_cache::cohort_observation();
        if (cohort.status != CacheStatus::ready || !cohort.sealed || cohort.incarnation != slot->range.incarnation() ||
            cohort.expected_mask != state.expected_mask)
            return Status::not_ready;
        uint64_t now = 0;
        if (!platform::clock_now_ns(now)) return Status::clock_unavailable;
        slot->phase.store(Phase::translations, std::memory_order_release);
        active_.fetch_add(1, std::memory_order_acq_rel);
        slot->gate.store(0, std::memory_order_release);
        send(*slot, Phase::translations);
        return Status::pending;
    }

    void service() noexcept {
        if (active_.load(std::memory_order_acquire) == 0 || service_error_.load(std::memory_order_acquire) != CacheStatus::ready) return;
        InterruptGuard guard;
        size_t logical = CPU_CAPACITY;
        uint32_t apic = UINT32_MAX;
        auto status = platform::current_cpu(logical, apic);
        uint64_t const expected = expected_.load(std::memory_order_acquire);
        if (status == CacheStatus::ready && (logical >= CPU_CAPACITY || (expected & (uint64_t{1} << logical)) == 0 ||
                                             (ready_.load(std::memory_order_acquire) & (uint64_t{1} << logical)) == 0 ||
                                             apics_[logical].load(std::memory_order_acquire) != apic))
            status = CacheStatus::wrong_cpu;
        if (status != CacheStatus::ready) {
            if (active_.load(std::memory_order_acquire) != 0) service_failure(status);
            return;
        }
        for (auto& slot : slots_) service_slot(slot, logical, apic);
    }

    auto observe(const Ticket& ticket, const ControllerCall& call) -> Report {
        if (!call.ready()) {
            Report value{};
            value.status = Status::invalid_context;
            return value;
        }
        return snapshot(ticket);
    }

    auto wait(Ticket& ticket, uint64_t deadline, const ControllerCall& call) -> Report {
        if (!call.ready()) return observe(ticket, call);
        auto* slot = find(ticket);
        if (slot == nullptr) return {};
        auto phase = slot->phase.load(std::memory_order_acquire);
        if (phase == Phase::prepared || phase == Phase::complete) return snapshot(ticket);
        std::array<unsigned, 2> retries{};
        uint64_t now = 0;
        if (!platform::clock_now_ns(now)) return snapshot(ticket, Status::clock_unavailable);
        std::array<uint64_t, 2> next_retry{retry_deadline(now), retry_deadline(now)};
        for (size_t step = 0; step < WAIT_STEPS; ++step) {
            if (!call.ready()) return snapshot(ticket, Status::invalid_context);
            if (failed(*slot)) return snapshot(ticket, Status::failed_retained);
            if (!platform::clock_now_ns(now)) return snapshot(ticket, Status::clock_unavailable);
            if (now >= deadline) {
                slot->timed_out.store(true, std::memory_order_release);
                return snapshot(ticket, Status::timeout_retained);
            }
            service();
            if (failed(*slot)) return snapshot(ticket, Status::failed_retained);
            if (!call.ready()) return snapshot(ticket, Status::invalid_context);
            if (!platform::clock_now_ns(now)) return snapshot(ticket, Status::clock_unavailable);
            if (now >= deadline) {
                slot->timed_out.store(true, std::memory_order_release);
                return snapshot(ticket, Status::timeout_retained);
            }
            phase = slot->phase.load(std::memory_order_acquire);
            if (phase == Phase::translations && all_completed(*slot, 0)) {
                if (slot->kind == Kind::translations_then_cache) {
                    slot->phase.store(Phase::caches, std::memory_order_release);
                    send(*slot, Phase::caches);
                    phase = Phase::caches;
                } else
                    finish(*slot);
            }
            if (phase == Phase::caches && all_completed(*slot, 1)) finish(*slot);
            if (slot->phase.load(std::memory_order_acquire) == Phase::closing) finish(*slot);
            if (slot->phase.load(std::memory_order_acquire) == Phase::complete) return snapshot(ticket);
            phase = slot->phase.load(std::memory_order_acquire);
            if (phase == Phase::translations || phase == Phase::caches) {
                size_t const index = round_index(phase);
                if (retries[index] < RETRIES && now >= next_retry[index]) {
                    send(*slot, phase);
                    ++retries[index];
                    next_retry[index] = retry_deadline(now);
                }
            }
            if ((step + 1) % YIELD_STEPS == 0)
                call.yield();
            else
                platform::spin_pause();
        }
        slot->timed_out.store(true, std::memory_order_release);
        return snapshot(ticket, Status::timeout_retained);
    }

    auto release(Ticket& ticket, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        auto* slot = find(ticket);
        if (slot == nullptr) return Status::invalid_ticket;
        if (!reclaimable(*slot)) return Status::busy;
        clear(*slot, ticket);
        return Status::complete;
    }

    void abandon(Ticket& ticket) noexcept {
        auto* slot = find(ticket);
        if (slot == nullptr) {
            ticket.id_ = 0;
            ticket.slot_ = POOL_CAPACITY;
            return;
        }
        if (slot->phase.load(std::memory_order_acquire) == Phase::prepared || reclaimable(*slot))
            clear(*slot, ticket);
        else {
            slot->abandoned.store(true, std::memory_order_release);
            ticket.id_ = 0;
            ticket.slot_ = POOL_CAPACITY;
        }
    }

    auto stats() const -> PoolStats {
        PoolStats value{};
        for (const auto& slot : slots_) {
            if (!slot.used.load(std::memory_order_acquire)) continue;
            auto const phase = slot.phase.load(std::memory_order_acquire);
            if (phase == Phase::prepared || phase == Phase::empty)
                ++value.prepared;
            else if (slot.abandoned.load(std::memory_order_acquire) || failed(slot))
                ++value.retained;
            else if (phase == Phase::complete)
                ++value.complete;
            else if (slot.timed_out.load(std::memory_order_acquire))
                ++value.retained;
            else
                ++value.published;
        }
        return value;
    }

#ifdef WOS_SELFTEST
    auto defer(Ticket& ticket, Phase phase, size_t logical, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        auto* slot = find(ticket);
        if (slot == nullptr) return Status::invalid_ticket;
        if (slot->phase.load(std::memory_order_acquire) != Phase::prepared || (phase != Phase::translations && phase != Phase::caches) ||
            logical >= CPU_CAPACITY || (slot->targets & (uint64_t{1} << logical)) == 0 ||
            (phase == Phase::caches && slot->kind != Kind::translations_then_cache))
            return Status::invalid_configuration;
        slot->defer[round_index(phase)].fetch_or(uint64_t{1} << logical, std::memory_order_release);
        return Status::ready;
    }
    auto resume(Ticket& ticket, Phase phase, const ControllerCall& call) -> Status {
        if (!call.ready()) return Status::invalid_context;
        auto* slot = find(ticket);
        if (slot == nullptr) return Status::invalid_ticket;
        if (phase != Phase::translations && phase != Phase::caches) return Status::invalid_configuration;
        slot->defer[round_index(phase)].store(0, std::memory_order_release);
        return Status::ready;
    }
#endif
};

namespace {
constinit Runtime runtime{};
}  // namespace
}  // namespace detail

Ticket::Ticket(size_t slot, uint64_t id) : id_(id), slot_(slot) {}
Ticket::~Ticket() { detail::runtime.abandon(*this); }
Ticket::Ticket(Ticket&& other) noexcept : id_(other.id_), slot_(other.slot_) {
    other.id_ = 0;
    other.slot_ = POOL_CAPACITY;
}
auto Ticket::operator=(Ticket&& other) noexcept -> Ticket& {
    if (this == &other) return *this;
    detail::runtime.abandon(*this);
    id_ = other.id_;
    slot_ = other.slot_;
    other.id_ = 0;
    other.slot_ = POOL_CAPACITY;
    return *this;
}

auto prepare(mmio_cache::QualifiedUcRange&& range, Kind kind, Ticket& output) -> Status {
    detail::ControllerCall call;
    return detail::runtime.prepare(std::move(range), kind, output, call);
}
auto prepare(mmio_cache::QualifiedWcRange&& range, Kind kind, Ticket& output) -> Status {
    detail::ControllerCall call;
    return detail::runtime.prepare(std::move(range), kind, output, call);
}
auto publish(Ticket& ticket) -> Status {
    detail::ControllerCall call;
    return detail::runtime.publish(ticket, call);
}
auto observe(const Ticket& ticket) -> Report {
    detail::ControllerCall call;
    return detail::runtime.observe(ticket, call);
}
auto wait_until(Ticket& ticket, uint64_t deadline) -> Report {
    detail::ControllerCall call;
    return detail::runtime.wait(ticket, deadline, call);
}
auto release_complete(Ticket& ticket) -> Status {
    detail::ControllerCall call;
    return detail::runtime.release(ticket, call);
}
auto service_observation() -> ServiceObservation { return detail::runtime.service_observation(); }
auto pool_stats() -> PoolStats { return detail::runtime.stats(); }
auto status_name(Status status) -> const char* {
    static constexpr std::array names{"ready",
                                      "pending",
                                      "busy",
                                      "complete",
                                      "timeout_retained",
                                      "failed_retained",
                                      "unconfigured",
                                      "invalid_configuration",
                                      "invalid_context",
                                      "invalid_ticket",
                                      "invalid_kind",
                                      "empty_range",
                                      "output_not_empty",
                                      "not_ready",
                                      "capacity",
                                      "generation_exhausted",
                                      "clock_unavailable"};
    auto const index = static_cast<size_t>(status);
    return index < names.size() ? names[index] : "invalid_status";
}
namespace detail {
auto protection_observation() -> ProtectionObservation { return runtime.protection_observation(); }
auto await_ready(const Controller& controller, uint64_t deadline) -> Status {
    ControllerCall call(controller);
    return runtime.await_ready(deadline, call);
}
auto prepare(const Controller& controller, mmio_cache::QualifiedUcRange&& range, Kind kind, Ticket& output) -> Status {
    ControllerCall call(controller);
    return runtime.prepare(std::move(range), kind, output, call);
}
auto prepare(const Controller& controller, mmio_cache::QualifiedWcRange&& range, Kind kind, Ticket& output) -> Status {
    ControllerCall call(controller);
    return runtime.prepare(std::move(range), kind, output, call);
}
auto publish(const Controller& controller, Ticket& ticket) -> Status {
    ControllerCall call(controller);
    return runtime.publish(ticket, call);
}
auto observe(const Controller& controller, const Ticket& ticket) -> Report {
    ControllerCall call(controller);
    return runtime.observe(ticket, call);
}
auto wait_until(const Controller& controller, Ticket& ticket, uint64_t deadline) -> Report {
    ControllerCall call(controller);
    return runtime.wait(ticket, deadline, call);
}
auto release_complete(const Controller& controller, Ticket& ticket) -> Status {
    ControllerCall call(controller);
    return runtime.release(ticket, call);
}
#ifdef WOS_SELFTEST
auto diagnostic_defer(const Controller& controller, Ticket& ticket, Phase phase, size_t logical) -> Status {
    ControllerCall call(controller);
    return runtime.defer(ticket, phase, logical, call);
}
auto diagnostic_resume(const Controller& controller, Ticket& ticket, Phase phase) -> Status {
    ControllerCall call(controller);
    return runtime.resume(ticket, phase, call);
}
#endif

auto initialize_boot(uint8_t vector) -> Status { return runtime.initialize(vector); }
auto note_current_cpu_ready() -> Status { return runtime.ready_cpu(); }
void service_current_cpu() noexcept { runtime.service(); }
}  // namespace detail
#ifdef WOS_SELFTEST
auto diagnostic_defer(Ticket& ticket, Phase phase, size_t logical) -> Status {
    detail::ControllerCall call;
    return detail::runtime.defer(ticket, phase, logical, call);
}
auto diagnostic_resume(Ticket& ticket, Phase phase) -> Status {
    detail::ControllerCall call;
    return detail::runtime.resume(ticket, phase, call);
}
#endif
}  // namespace ker::mod::mm::cpu_maintenance
