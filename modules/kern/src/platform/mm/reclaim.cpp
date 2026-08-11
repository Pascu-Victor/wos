#include "reclaim.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>

#include "reclaim_policy.hpp"

namespace ker::mod::mm::reclaim {
namespace {

constexpr uintptr_t OWNER_WITHOUT_TASK = 1;
constexpr uint32_t BACKGROUND_RECLAIM_MAX_PASSES = 64;
constexpr uint32_t EXPLICIT_RECLAIM_MAX_PASSES = 1024;
constexpr uint32_t EXPLICIT_NO_PROGRESS_PASS_LIMIT = 64;
constexpr size_t EXPLICIT_SHRINKER_NAME_CAPACITY = 32;
constexpr uint64_t ALLOCATION_PRESSURE_SAMPLE_PAGES = 256;
constexpr uint8_t IMMEDIATE_PRESSURE_SAMPLE_ORDER = 4;
constexpr uint32_t KNOWN_CAPABILITIES = RECLAIM_MAY_BLOCK | RECLAIM_MAY_IO | RECLAIM_MAY_ALLOCATE;

// Keep early-MM coordinator state visibly zero-initialized rather than relying
// on implicit atomic default construction.
// NOLINTBEGIN(readability-redundant-member-init)
struct AtomicGlobalStats {
    std::atomic<uint64_t> direct_attempts{};
    std::atomic<uint64_t> background_attempts{};
    std::atomic<uint64_t> explicit_attempts{};
    std::atomic<uint64_t> explicit_requests{};
    std::atomic<uint64_t> explicit_completions{};
    std::atomic<uint64_t> explicit_rejections{};
    std::atomic<uint64_t> callbacks{};
    std::atomic<uint64_t> scanned_units{};
    std::atomic<uint64_t> reclaimed_units{};
    std::atomic<uint64_t> scanned_pages{};
    std::atomic<uint64_t> scanned_bytes{};
    std::atomic<uint64_t> scanned_objects{};
    std::atomic<uint64_t> reported_pages{};
    std::atomic<uint64_t> reported_bytes{};
    std::atomic<uint64_t> reported_objects{};
    std::atomic<uint64_t> reclaimed_pages{};
    std::atomic<uint64_t> no_progress{};
    std::atomic<uint64_t> failures{};
    std::atomic<uint64_t> recursion_avoided{};
    std::atomic<uint64_t> lease_contention{};
    std::atomic<uint64_t> unsafe_context_skips{};
    std::atomic<uint64_t> capability_skips{};
    std::atomic<uint64_t> cooldown_skips{};
    std::atomic<uint64_t> latency_us{};
    std::atomic<uint64_t> latency_max_us{};
    std::atomic<uint64_t> low_transitions{};
    std::atomic<uint64_t> critical_transitions{};
    std::atomic<uint64_t> recovered_transitions{};
    std::atomic<uint64_t> worker_wakeups{};
    std::atomic<uint64_t> worker_no_progress{};
    std::atomic<PressureLevel> current_pressure{PressureLevel::NONE};
};

struct AtomicShrinkerStats {
    std::atomic<uint64_t> count_calls{};
    std::atomic<uint64_t> scan_calls{};
    std::atomic<uint64_t> scanned_units{};
    std::atomic<uint64_t> reclaimed_units{};
    std::atomic<uint64_t> reclaimed_pages{};
    std::atomic<uint64_t> no_progress{};
    std::atomic<uint64_t> failures{};
    std::atomic<uint64_t> context_skips{};
    std::atomic<uint64_t> cooldown_skips{};
    std::atomic<uint64_t> latency_us{};
    std::atomic<uint64_t> latency_max_us{};
};

struct RegistryEntry {
    Shrinker shrinker{};
    AtomicShrinkerStats stats{};
    ReclaimCount last_count{};  // Protected by Coordinator::registry_lock.
    std::atomic<uint64_t> last_selected{};
    uint64_t cooldown_until_round{};  // Serialized by the scan lease.
    uint32_t no_progress_streak{};    // Serialized by the scan lease.
};

struct Coordinator {
    sys::Spinlock registry_lock{};
    std::array<RegistryEntry, MAX_SHRINKERS> entries{};
    size_t entry_count{};
    std::atomic<uintptr_t> scan_owner{};
    std::atomic<uint64_t> scan_round{};
    std::atomic<uint64_t> selection_clock{};
    AtomicGlobalStats stats{};
};
// NOLINTEND(readability-redundant-member-init)

struct Candidate {
    size_t slot{};
    Shrinker shrinker{};
    uint64_t last_selected{};
};

struct ExplicitJob {
    ReclaimRequest request{};
    std::array<char, EXPLICIT_SHRINKER_NAME_CAPACITY> shrinker_name{};
    uint64_t sequence{};
};

struct ExplicitJobSlot {
    sys::Spinlock lock;
    ExplicitJob job{};
    bool pending{};
};

Coordinator coordinator;
std::atomic<bool> initialized{};
std::atomic<bool> worker_started{};
std::atomic<sched::task::Task*> worker_task{};
std::atomic<bool> background_pending{};
std::atomic<uint8_t> background_order{};
std::atomic<uint64_t> background_target_pages{};
std::atomic<uint64_t> allocation_pages_since_pressure_sample{};
ExplicitJobSlot explicit_job_slot{};

void add_counter(std::atomic<uint64_t>& counter, uint64_t delta = 1) {
    uint64_t value = counter.load(std::memory_order_relaxed);
    for (;;) {
        uint64_t const UPDATED = saturating_add(value, delta);
        if (counter.compare_exchange_weak(value, UPDATED, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return;
        }
    }
}

void update_max(std::atomic<uint64_t>& value, uint64_t candidate) {
    uint64_t current = value.load(std::memory_order_relaxed);
    while (current < candidate && !value.compare_exchange_weak(current, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void update_max(std::atomic<uint8_t>& value, uint8_t candidate) {
    uint8_t current = value.load(std::memory_order_relaxed);
    while (current < candidate && !value.compare_exchange_weak(current, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void record_latency(std::atomic<uint64_t>& total, std::atomic<uint64_t>& maximum, uint64_t elapsed_us) {
    add_counter(total, elapsed_us);
    update_max(maximum, elapsed_us);
}

void record_scanned_units(AtomicGlobalStats& stats, ReclaimUnit unit, uint64_t amount) {
    switch (unit) {
        case ReclaimUnit::PAGES:
            add_counter(stats.scanned_pages, amount);
            break;
        case ReclaimUnit::BYTES:
            add_counter(stats.scanned_bytes, amount);
            break;
        case ReclaimUnit::OBJECTS:
            add_counter(stats.scanned_objects, amount);
            break;
    }
}

void record_reported_units(AtomicGlobalStats& stats, ReclaimUnit unit, uint64_t amount) {
    switch (unit) {
        case ReclaimUnit::PAGES:
            add_counter(stats.reported_pages, amount);
            break;
        case ReclaimUnit::BYTES:
            add_counter(stats.reported_bytes, amount);
            break;
        case ReclaimUnit::OBJECTS:
            add_counter(stats.reported_objects, amount);
            break;
    }
}

[[nodiscard]] auto valid_unit(ReclaimUnit unit) -> bool {
    return unit == ReclaimUnit::PAGES || unit == ReclaimUnit::BYTES || unit == ReclaimUnit::OBJECTS;
}

[[nodiscard]] auto valid_priority(ReclaimPriority priority) -> bool {
    return priority == ReclaimPriority::LOW || priority == ReclaimPriority::NORMAL || priority == ReclaimPriority::CRITICAL;
}

[[nodiscard]] auto valid_context(ReclaimContext context) -> bool {
    return context == ReclaimContext::DIRECT || context == ReclaimContext::BACKGROUND || context == ReclaimContext::EXPLICIT;
}

[[nodiscard]] auto valid_shrinker(const Shrinker& shrinker) -> bool {
    return shrinker.name != nullptr && shrinker.name[0] != '\0' && valid_unit(shrinker.unit) && valid_unit(shrinker.scan_unit) &&
           valid_priority(shrinker.min_priority) && (shrinker.capabilities & ~KNOWN_CAPABILITIES) == 0 && shrinker.max_batch_units != 0 &&
           shrinker.max_scan_units != 0 && shrinker.count != nullptr && shrinker.scan != nullptr;
}

[[nodiscard]] auto same_shrinker(const Shrinker& lhs, const Shrinker& rhs) -> bool {
    return std::string_view(lhs.name) == std::string_view(rhs.name) && lhs.unit == rhs.unit && lhs.scan_unit == rhs.scan_unit &&
           lhs.rank == rhs.rank && lhs.min_priority == rhs.min_priority && lhs.capabilities == rhs.capabilities &&
           lhs.max_batch_units == rhs.max_batch_units && lhs.max_scan_units == rhs.max_scan_units && lhs.count == rhs.count &&
           lhs.scan == rhs.scan && lhs.opaque == rhs.opaque;
}

[[nodiscard]] auto register_on(Coordinator& state, const Shrinker& shrinker) -> bool {
    if (!valid_shrinker(shrinker)) {
        return false;
    }

    uint64_t const FLAGS = state.registry_lock.lock_irqsave();
    for (size_t i = 0; i < state.entry_count; ++i) {
        if (std::string_view(state.entries.at(i).shrinker.name) == std::string_view(shrinker.name)) {
            bool const IDEMPOTENT = same_shrinker(state.entries.at(i).shrinker, shrinker);
            state.registry_lock.unlock_irqrestore(FLAGS);
            return IDEMPOTENT;
        }
    }
    if (state.entry_count == state.entries.size()) {
        state.registry_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    state.entries.at(state.entry_count).shrinker = shrinker;
    ++state.entry_count;
    state.registry_lock.unlock_irqrestore(FLAGS);
    return true;
}

[[nodiscard]] auto candidate_before(const Candidate& lhs, const Candidate& rhs) -> bool {
    if (lhs.shrinker.rank != rhs.shrinker.rank) {
        return lhs.shrinker.rank < rhs.shrinker.rank;
    }
    if (lhs.last_selected != rhs.last_selected) {
        return lhs.last_selected < rhs.last_selected;
    }
    return lhs.slot < rhs.slot;
}

void sort_candidates(std::array<Candidate, MAX_SHRINKERS>& candidates, size_t count) {
    // Explicit insertion sort keeps this bounded path mechanically allocation-free.
    for (size_t i = 1; i < count; ++i) {
        Candidate const CURRENT = candidates.at(i);
        size_t position = i;
        while (position != 0 && candidate_before(CURRENT, candidates.at(position - 1))) {
            candidates.at(position) = candidates.at(position - 1);
            --position;
        }
        candidates.at(position) = CURRENT;
    }
}

[[nodiscard]] auto snapshot_candidates(Coordinator& state, std::array<Candidate, MAX_SHRINKERS>& out, std::string_view only_name)
    -> size_t {
    size_t count = 0;
    uint64_t const FLAGS = state.registry_lock.lock_irqsave();
    for (size_t slot = 0; slot < state.entry_count; ++slot) {
        RegistryEntry& entry = state.entries.at(slot);
        if (!only_name.empty() && std::string_view(entry.shrinker.name) != only_name) {
            continue;
        }
        out.at(count++) = Candidate{
            .slot = slot,
            .shrinker = entry.shrinker,
            .last_selected = entry.last_selected.load(std::memory_order_relaxed),
        };
    }
    state.registry_lock.unlock_irqrestore(FLAGS);
    sort_candidates(out, count);
    return count;
}

[[nodiscard]] auto capabilities_allowed(const Shrinker& shrinker, const ReclaimRequest& request) -> bool {
    if ((shrinker.capabilities & RECLAIM_MAY_BLOCK) != 0 && !request.may_block) {
        return false;
    }
    if ((shrinker.capabilities & RECLAIM_MAY_IO) != 0 && !request.may_io) {
        return false;
    }
    return (shrinker.capabilities & RECLAIM_MAY_ALLOCATE) == 0 || request.may_allocate;
}

void note_attempt(AtomicGlobalStats& stats, ReclaimContext context) {
    switch (context) {
        case ReclaimContext::DIRECT:
            add_counter(stats.direct_attempts);
            break;
        case ReclaimContext::BACKGROUND:
            add_counter(stats.background_attempts);
            break;
        case ReclaimContext::EXPLICIT:
            add_counter(stats.explicit_attempts);
            break;
    }
}

[[nodiscard]] auto owner_token() -> uintptr_t {
    if (!sched::can_query_current_task()) {
        return OWNER_WITHOUT_TASK;
    }
    auto* const TASK = sched::get_current_task();
    return TASK == nullptr ? OWNER_WITHOUT_TASK : reinterpret_cast<uintptr_t>(TASK);
}

struct LeaseGuard {
    Coordinator& state;
    explicit LeaseGuard(Coordinator& coordinator_state) : state(coordinator_state) {}
    ~LeaseGuard() { state.scan_owner.store(0, std::memory_order_release); }

    LeaseGuard(const LeaseGuard&) = delete;
    auto operator=(const LeaseGuard&) -> LeaseGuard& = delete;
};

void store_last_count(Coordinator& state, size_t slot, const ReclaimCount& count) {
    uint64_t const FLAGS = state.registry_lock.lock_irqsave();
    state.entries.at(slot).last_count = count;
    state.registry_lock.unlock_irqrestore(FLAGS);
}

void note_entry_no_progress(RegistryEntry& entry, uint64_t round) {
    add_counter(entry.stats.no_progress);
    if (entry.no_progress_streak != std::numeric_limits<uint32_t>::max()) {
        ++entry.no_progress_streak;
    }
    entry.cooldown_until_round = saturating_add(round, backoff_rounds(entry.no_progress_streak));
}

void note_entry_progress(RegistryEntry& entry) {
    entry.no_progress_streak = 0;
    entry.cooldown_until_round = 0;
}

void finish_run(Coordinator& state, ReclaimRunResult& result, uint64_t started_us, bool scanned) {
    uint64_t const FINISHED_US = time::get_us();
    result.elapsed_us = FINISHED_US >= started_us ? FINISHED_US - started_us : 0;
    result.made_progress = result.reclaimed_units != 0 || result.reclaimed_pages != 0;
    record_latency(state.stats.latency_us, state.stats.latency_max_us, result.elapsed_us);
    if (scanned && !result.made_progress) {
        add_counter(state.stats.no_progress);
    }
}

[[nodiscard]] auto run_on(Coordinator& state, const ReclaimRequest& request, std::string_view only_name, uintptr_t token,
                          bool enforce_runtime_context, bool sample_physical_pages) -> ReclaimRunResult {
    ReclaimRunResult result{};
    uint64_t const STARTED_US = time::get_us();
    note_attempt(state.stats, request.context);

    if (!valid_context(request.context) || !valid_priority(request.priority) ||
        (enforce_runtime_context && !phys::can_wait_for_reclaim())) {
        result.unsafe_context = true;
        add_counter(state.stats.unsafe_context_skips);
        finish_run(state, result, STARTED_US, false);
        return result;
    }

    uintptr_t expected = 0;
    if (!state.scan_owner.compare_exchange_strong(expected, token, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (expected == token) {
            result.recursion_avoided = true;
            add_counter(state.stats.recursion_avoided);
        } else {
            result.lease_contended = true;
            add_counter(state.stats.lease_contention);
        }
        finish_run(state, result, STARTED_US, false);
        return result;
    }
    LeaseGuard const LEASE{state};

    uint64_t const ROUND = state.scan_round.fetch_add(1, std::memory_order_relaxed) + 1;
    std::array<Candidate, MAX_SHRINKERS> candidates{};
    size_t const CANDIDATE_COUNT = snapshot_candidates(state, candidates, only_name);
    bool attempted_scan = false;

    for (size_t candidate_index = 0; candidate_index < CANDIDATE_COUNT; ++candidate_index) {
        Candidate const& candidate = candidates.at(candidate_index);
        RegistryEntry& entry = state.entries.at(candidate.slot);
        Shrinker const& shrinker = candidate.shrinker;

        if (request.deadline_us != 0 && time::get_us() >= request.deadline_us) {
            break;
        }
        if (static_cast<uint8_t>(request.priority) < static_cast<uint8_t>(shrinker.min_priority)) {
            continue;
        }
        if (request.priority != ReclaimPriority::CRITICAL && ROUND < entry.cooldown_until_round) {
            add_counter(entry.stats.cooldown_skips);
            add_counter(state.stats.cooldown_skips);
            continue;
        }
        if (!capabilities_allowed(shrinker, request)) {
            add_counter(entry.stats.context_skips);
            add_counter(state.stats.capability_skips);
            continue;
        }

        entry.last_selected.store(state.selection_clock.fetch_add(1, std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        uint64_t const CALLBACK_STARTED_US = time::get_us();
        add_counter(entry.stats.count_calls);
        ReclaimCount const COUNT = shrinker.count(shrinker.opaque, request);
        store_last_count(state, candidate.slot, COUNT);

        if (COUNT.reclaimable == 0) {
            note_entry_no_progress(entry, ROUND);
            uint64_t const CALLBACK_FINISHED_US = time::get_us();
            uint64_t const ELAPSED_US = CALLBACK_FINISHED_US >= CALLBACK_STARTED_US ? CALLBACK_FINISHED_US - CALLBACK_STARTED_US : 0;
            record_latency(entry.stats.latency_us, entry.stats.latency_max_us, ELAPSED_US);
            continue;
        }
        if (request.deadline_us != 0 && time::get_us() >= request.deadline_us) {
            uint64_t const CALLBACK_FINISHED_US = time::get_us();
            uint64_t const ELAPSED_US = CALLBACK_FINISHED_US >= CALLBACK_STARTED_US ? CALLBACK_FINISHED_US - CALLBACK_STARTED_US : 0;
            record_latency(entry.stats.latency_us, entry.stats.latency_max_us, ELAPSED_US);
            break;
        }

        ReclaimRequest callback_request = request;
        callback_request.target_pages = std::max<uint64_t>(request.target_pages, 1);
        callback_request.budget_units = request.budget_units != 0
                                            ? std::min(request.budget_units, shrinker.max_batch_units)
                                            : callback_budget(shrinker.unit, callback_request.target_pages, shrinker.max_batch_units);
        callback_request.budget_units = std::min(callback_request.budget_units, COUNT.reclaimable);
        callback_request.scan_budget_units =
            request.scan_budget_units != 0 ? std::min(request.scan_budget_units, shrinker.max_scan_units) : shrinker.max_scan_units;

        uint64_t const FREE_BEFORE = sample_physical_pages ? phys::get_free_mem_pages() : 0;
        add_counter(entry.stats.scan_calls);
        add_counter(state.stats.callbacks);
        ++result.callbacks;
        attempted_scan = true;
        ReclaimScanResult const CALLBACK_RESULT = shrinker.scan(shrinker.opaque, callback_request);
        uint64_t const FREE_AFTER = sample_physical_pages ? phys::get_free_mem_pages() : FREE_BEFORE;

        bool const BUDGET_VIOLATION =
            CALLBACK_RESULT.scanned > callback_request.scan_budget_units || CALLBACK_RESULT.reclaimed > callback_request.budget_units;
        uint64_t const SCANNED = std::min(CALLBACK_RESULT.scanned, callback_request.scan_budget_units);
        uint64_t const RECLAIMED = std::min(CALLBACK_RESULT.reclaimed, callback_request.budget_units);
        uint64_t const RECLAIMED_PAGES = FREE_AFTER > FREE_BEFORE ? FREE_AFTER - FREE_BEFORE : 0;
        bool const CALLBACK_FAILED = CALLBACK_RESULT.failed || BUDGET_VIOLATION;

        result.scanned_units = saturating_add(result.scanned_units, SCANNED);
        result.reclaimed_units = saturating_add(result.reclaimed_units, RECLAIMED);
        result.reclaimed_pages = saturating_add(result.reclaimed_pages, RECLAIMED_PAGES);
        result.has_more |= CALLBACK_RESULT.has_more;
        add_counter(entry.stats.scanned_units, SCANNED);
        add_counter(entry.stats.reclaimed_units, RECLAIMED);
        add_counter(entry.stats.reclaimed_pages, RECLAIMED_PAGES);
        add_counter(state.stats.scanned_units, SCANNED);
        add_counter(state.stats.reclaimed_units, RECLAIMED);
        record_scanned_units(state.stats, shrinker.scan_unit, SCANNED);
        record_reported_units(state.stats, shrinker.unit, RECLAIMED);
        add_counter(state.stats.reclaimed_pages, RECLAIMED_PAGES);
        if (CALLBACK_FAILED) {
            add_counter(entry.stats.failures);
            add_counter(state.stats.failures);
        }

        if (RECLAIMED != 0 || RECLAIMED_PAGES != 0) {
            note_entry_progress(entry);
        } else {
            note_entry_no_progress(entry, ROUND);
        }

        uint64_t const CALLBACK_FINISHED_US = time::get_us();
        uint64_t const ELAPSED_US = CALLBACK_FINISHED_US >= CALLBACK_STARTED_US ? CALLBACK_FINISHED_US - CALLBACK_STARTED_US : 0;
        record_latency(entry.stats.latency_us, entry.stats.latency_max_us, ELAPSED_US);

        if (sample_physical_pages && result.reclaimed_pages >= std::max<uint64_t>(request.target_pages, 1)) {
            break;
        }
    }

    finish_run(state, result, STARTED_US, attempted_scan || CANDIDATE_COUNT != 0);
    return result;
}

[[nodiscard]] auto load_pressure(uint8_t requested_order, ZoneWatermarkSnapshot* snapshots, size_t& count) -> PressureLevel {
    count = snapshot_zone_watermarks(snapshots, MAX_WATERMARK_ZONES, requested_order);
    uint64_t free_pages = 0;
    uint64_t critical_pages = 0;
    uint64_t low_pages = 0;
    bool has_satisfiable_zone = false;
    bool has_requested_order_block = false;
    uint64_t const ORDER_PAGES = order_pages(requested_order);
    for (size_t i = 0; i < count; ++i) {
        ZoneWatermarkSnapshot const& snapshot = snapshots[i];
        if (snapshot.total_pages < ORDER_PAGES) {
            continue;
        }
        has_satisfiable_zone = true;
        free_pages = saturating_add(free_pages, snapshot.free_pages);
        critical_pages = saturating_add(critical_pages, snapshot.critical_pages);
        low_pages = saturating_add(low_pages, snapshot.low_pages);
        has_requested_order_block |=
            snapshot.largest_free_order >= 0 && static_cast<uint8_t>(snapshot.largest_free_order) >= requested_order;
    }
    if (!has_satisfiable_zone) {
        return PressureLevel::UNSATISFIABLE;
    }
    if (!has_requested_order_block || free_pages < ORDER_PAGES || free_pages <= critical_pages) {
        return PressureLevel::CRITICAL;
    }
    return free_pages <= low_pages ? PressureLevel::LOW : PressureLevel::NONE;
}

void observe_pressure(PressureLevel pressure) {
    PressureLevel const PREVIOUS = coordinator.stats.current_pressure.exchange(pressure, std::memory_order_acq_rel);
    if (PREVIOUS == pressure) {
        return;
    }
    switch (pressure) {
        case PressureLevel::NONE:
            if (PREVIOUS != PressureLevel::NONE) {
                add_counter(coordinator.stats.recovered_transitions);
            }
            break;
        case PressureLevel::LOW:
            add_counter(coordinator.stats.low_transitions);
            break;
        case PressureLevel::CRITICAL:
        case PressureLevel::UNSATISFIABLE:
            add_counter(coordinator.stats.critical_transitions);
            break;
    }
}

[[nodiscard]] auto pages_below_high_watermark(uint8_t requested_order) -> uint64_t {
    std::array<ZoneWatermarkSnapshot, MAX_WATERMARK_ZONES> snapshots{};
    size_t const COUNT = snapshot_zone_watermarks(snapshots.data(), snapshots.size(), requested_order);
    uint64_t free_pages = 0;
    uint64_t high_pages = 0;
    bool has_satisfiable_zone = false;
    bool has_requested_order_block = false;
    uint64_t const ORDER_PAGES = order_pages(requested_order);
    for (size_t i = 0; i < COUNT; ++i) {
        ZoneWatermarkSnapshot const& snapshot = snapshots.at(i);
        if (snapshot.total_pages < ORDER_PAGES) {
            continue;
        }
        has_satisfiable_zone = true;
        free_pages = saturating_add(free_pages, snapshot.free_pages);
        high_pages = saturating_add(high_pages, snapshot.high_pages);
        has_requested_order_block |=
            snapshot.largest_free_order >= 0 && static_cast<uint8_t>(snapshot.largest_free_order) >= requested_order;
    }
    if (!has_satisfiable_zone) {
        return ORDER_PAGES;
    }
    uint64_t missing = high_pages > free_pages ? high_pages - free_pages : 0;
    if (!has_requested_order_block) {
        missing = std::max(missing, ORDER_PAGES);
    }
    return missing;
}

[[nodiscard]] auto take_explicit_job(ExplicitJob& out) -> bool {
    uint64_t const FLAGS = explicit_job_slot.lock.lock_irqsave();
    if (!explicit_job_slot.pending) {
        explicit_job_slot.lock.unlock_irqrestore(FLAGS);
        return false;
    }
    out = explicit_job_slot.job;
    explicit_job_slot.pending = false;
    explicit_job_slot.lock.unlock_irqrestore(FLAGS);
    return true;
}

[[nodiscard]] auto explicit_job_pending() -> bool {
    uint64_t const FLAGS = explicit_job_slot.lock.lock_irqsave();
    bool const PENDING = explicit_job_slot.pending;
    explicit_job_slot.lock.unlock_irqrestore(FLAGS);
    return PENDING;
}

void run_explicit_job(const ExplicitJob& job) {
    ReclaimRequest request = job.request;
    uint64_t remaining_units = request.budget_units;
    uint64_t remaining_pages = std::max<uint64_t>(request.target_pages, 1);
    bool const HAS_UNIT_TARGET = remaining_units != 0;
    uint32_t consecutive_no_progress = 0;

    for (uint32_t pass = 0; pass < EXPLICIT_RECLAIM_MAX_PASSES; ++pass) {
        if (request.deadline_us != 0 && time::get_us() >= request.deadline_us) {
            break;
        }
        request.budget_units = remaining_units;
        request.target_pages = remaining_pages;
        ReclaimRunResult const RESULT = run(request, std::string_view(job.shrinker_name.data()));
        if (RESULT.lease_contended) {
            sched::kern_yield();
            continue;
        }
        if (RESULT.unsafe_context || RESULT.recursion_avoided) {
            break;
        }

        remaining_units = RESULT.reclaimed_units >= remaining_units ? 0 : remaining_units - RESULT.reclaimed_units;
        remaining_pages = RESULT.reclaimed_pages >= remaining_pages ? 0 : remaining_pages - RESULT.reclaimed_pages;
        if ((HAS_UNIT_TARGET && remaining_units == 0) || (!HAS_UNIT_TARGET && remaining_pages == 0)) {
            break;
        }
        if (RESULT.made_progress) {
            consecutive_no_progress = 0;
        } else if (!RESULT.has_more || ++consecutive_no_progress >= EXPLICIT_NO_PROGRESS_PASS_LIMIT) {
            break;
        }
        sched::kern_yield();
    }

    coordinator.stats.explicit_completions.store(job.sequence, std::memory_order_release);
}

[[noreturn]] void reclaim_worker_main() {
    for (;;) {
        ExplicitJob explicit_job{};
        if (take_explicit_job(explicit_job)) {
            run_explicit_job(explicit_job);
            continue;
        }
        if (!background_pending.exchange(false, std::memory_order_acq_rel)) {
            sched::kern_block();
            continue;
        }

        uint8_t const REQUESTED_ORDER = background_order.exchange(0, std::memory_order_acq_rel);
        uint64_t target_pages = std::max<uint64_t>(background_target_pages.exchange(0, std::memory_order_acq_rel), 1);
        bool made_physical_progress = false;
        uint32_t pass = 0;
        for (; pass < BACKGROUND_RECLAIM_MAX_PASSES && !above_high_watermark(REQUESTED_ORDER); ++pass) {
            PressureLevel const PRESSURE = pressure_for_order(REQUESTED_ORDER);
            target_pages = std::max(target_pages, pages_below_high_watermark(REQUESTED_ORDER));
            ReclaimRequest const REQUEST{
                .context = ReclaimContext::BACKGROUND,
                .priority = pressure_priority(PRESSURE),
                .requested_order = REQUESTED_ORDER,
                .target_pages = target_pages,
                .may_block = true,
                .may_io = true,
                .may_allocate = true,
            };
            ReclaimRunResult const RESULT = run(REQUEST);
            if (RESULT.reclaimed_pages == 0) {
                add_counter(coordinator.stats.worker_no_progress);
                made_physical_progress = false;
                break;
            }
            made_physical_progress = true;
            sched::kern_yield();
        }

        if (pass == BACKGROUND_RECLAIM_MAX_PASSES && made_physical_progress && !above_high_watermark(REQUESTED_ORDER)) {
            request_background(REQUESTED_ORDER, target_pages);
            sched::kern_yield();
        }
    }
}

#ifdef WOS_SELFTEST
struct FakeShrinkerState {
    Coordinator* nested_coordinator{};
    uintptr_t nested_token{};
    uint64_t reclaimable{8};
    uint64_t scanned{1};
    uint64_t reclaimed{1};
    uint64_t count_calls{};
    uint64_t scan_calls{};
    uint64_t seen_budget{};
    uint64_t seen_scan_budget{};
    bool recurse{};
    bool over_report{};
    ReclaimRunResult nested_result{};
};

auto fake_count(void* opaque, const ReclaimRequest&) -> ReclaimCount {
    auto& state = *static_cast<FakeShrinkerState*>(opaque);
    ++state.count_calls;
    return {.reclaimable = state.reclaimable};
}

auto fake_scan(void* opaque, const ReclaimRequest& request) -> ReclaimScanResult {
    auto& state = *static_cast<FakeShrinkerState*>(opaque);
    ++state.scan_calls;
    state.seen_budget = request.budget_units;
    state.seen_scan_budget = request.scan_budget_units;
    if (state.recurse) {
        state.nested_result = run_on(*state.nested_coordinator, request, {}, state.nested_token, false, false);
    }
    if (state.over_report) {
        return {
            .scanned = std::numeric_limits<uint64_t>::max(),
            .reclaimed = std::numeric_limits<uint64_t>::max(),
        };
    }
    return {.scanned = state.scanned, .reclaimed = state.reclaimed, .has_more = true};
}

[[nodiscard]] auto fake_descriptor(const char* name, FakeShrinkerState& state) -> Shrinker {
    return {
        .name = name,
        .unit = ReclaimUnit::PAGES,
        .scan_unit = ReclaimUnit::OBJECTS,
        .rank = 1,
        .min_priority = ReclaimPriority::LOW,
        .capabilities = RECLAIM_NONE,
        .max_batch_units = 64,
        .max_scan_units = 64,
        .count = fake_count,
        .scan = fake_scan,
        .opaque = &state,
    };
}
#endif

}  // namespace

void init() {
    bool expected = false;
    if (initialized.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        static_cast<void>(pressure_for_order(0));
    }
}

auto register_shrinker(const Shrinker& shrinker) -> bool { return register_on(coordinator, shrinker); }

auto run(const ReclaimRequest& request, std::string_view only_name) -> ReclaimRunResult {
    return run_on(coordinator, request, only_name, owner_token(), true, true);
}

auto request_explicit(const ReclaimRequest& request, std::string_view only_name) -> bool {
    if (request.context != ReclaimContext::EXPLICIT || !valid_priority(request.priority) || only_name.empty() ||
        only_name.size() >= EXPLICIT_SHRINKER_NAME_CAPACITY) {
        add_counter(coordinator.stats.explicit_rejections);
        return false;
    }

    uint64_t const FLAGS = explicit_job_slot.lock.lock_irqsave();
    if (explicit_job_slot.pending) {
        explicit_job_slot.lock.unlock_irqrestore(FLAGS);
        add_counter(coordinator.stats.explicit_rejections);
        return false;
    }

    ExplicitJob& job = explicit_job_slot.job;
    job = {};
    job.request = request;
    std::ranges::copy(only_name, job.shrinker_name.begin());
    uint64_t const SEQUENCE = saturating_add(coordinator.stats.explicit_requests.load(std::memory_order_relaxed), 1);
    coordinator.stats.explicit_requests.store(SEQUENCE, std::memory_order_relaxed);
    job.sequence = SEQUENCE;
    explicit_job_slot.pending = true;
    explicit_job_slot.lock.unlock_irqrestore(FLAGS);

    add_counter(coordinator.stats.worker_wakeups);
    auto* const TASK = worker_task.load(std::memory_order_acquire);
    if (TASK != nullptr) {
        sched::wake_task_from_event(TASK);
    }
    return true;
}

auto reclaim_for_allocation(uint8_t requested_order, uint64_t target_pages) -> ReclaimRunResult {
    PressureLevel const PRESSURE = pressure_for_order(requested_order);
    if (PRESSURE == PressureLevel::NONE) {
        return {};
    }
    ReclaimRequest const REQUEST{
        .context = ReclaimContext::DIRECT,
        .priority = pressure_priority(PRESSURE),
        .requested_order = requested_order,
        .target_pages = std::max<uint64_t>(target_pages, 1),
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    ReclaimRunResult result = run(REQUEST);
    if (pressure_for_order(requested_order) != PressureLevel::NONE) {
        request_background(requested_order, target_pages);
    }
    return result;
}

void request_background(uint8_t requested_order, uint64_t target_pages) {
    update_max(background_order, requested_order);
    update_max(background_target_pages, std::max<uint64_t>(target_pages, 1));
    bool const WAS_PENDING = background_pending.exchange(true, std::memory_order_acq_rel);
    if (!WAS_PENDING) {
        add_counter(coordinator.stats.worker_wakeups);
    }
    auto* const TASK = worker_task.load(std::memory_order_acquire);
    if (TASK != nullptr) {
        sched::wake_task_from_event(TASK);
    }
}

void note_allocation(uint8_t requested_order, uint64_t allocated_pages) {
    uint64_t const ALLOCATED_PAGES = std::max<uint64_t>(allocated_pages, 1);
    if (requested_order < IMMEDIATE_PRESSURE_SAMPLE_ORDER) {
        uint64_t const PREVIOUS = allocation_pages_since_pressure_sample.fetch_add(ALLOCATED_PAGES, std::memory_order_relaxed);
        uint64_t const PAGES_IN_INTERVAL = PREVIOUS % ALLOCATION_PRESSURE_SAMPLE_PAGES;
        if (ALLOCATED_PAGES < ALLOCATION_PRESSURE_SAMPLE_PAGES && PAGES_IN_INTERVAL < ALLOCATION_PRESSURE_SAMPLE_PAGES - ALLOCATED_PAGES) {
            return;
        }
    }
    if (pressure_for_order(requested_order) != PressureLevel::NONE) {
        request_background(requested_order, ALLOCATED_PAGES);
    }
}

void start_worker() {
    bool expected = false;
    if (!worker_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    auto* const TASK = sched::task::Task::create_kernel_thread("mm_reclaim", reclaim_worker_main);
    if (TASK == nullptr) {
        worker_started.store(false, std::memory_order_release);
        return;
    }
    worker_task.store(TASK, std::memory_order_release);
    static_cast<void>(sched::post_task_balanced(TASK));
    if (background_pending.load(std::memory_order_acquire) || explicit_job_pending()) {
        sched::wake_task_from_event(TASK);
    }
}

auto pressure_for_order(uint8_t requested_order) -> PressureLevel {
    std::array<ZoneWatermarkSnapshot, MAX_WATERMARK_ZONES> snapshots{};
    size_t count = 0;
    PressureLevel const PRESSURE = load_pressure(requested_order, snapshots.data(), count);
    if (requested_order == 0) {
        observe_pressure(PRESSURE);
    }
    return PRESSURE;
}

auto above_high_watermark(uint8_t requested_order) -> bool {
    std::array<ZoneWatermarkSnapshot, MAX_WATERMARK_ZONES> snapshots{};
    size_t const COUNT = snapshot_zone_watermarks(snapshots.data(), snapshots.size(), requested_order);
    uint64_t free_pages = 0;
    uint64_t high_pages = 0;
    bool has_satisfiable_zone = false;
    bool has_requested_order_block = false;
    uint64_t const ORDER_PAGES = order_pages(requested_order);
    for (size_t i = 0; i < COUNT; ++i) {
        ZoneWatermarkSnapshot const& snapshot = snapshots.at(i);
        if (snapshot.total_pages < ORDER_PAGES) {
            continue;
        }
        has_satisfiable_zone = true;
        free_pages = saturating_add(free_pages, snapshot.free_pages);
        high_pages = saturating_add(high_pages, snapshot.high_pages);
        has_requested_order_block |=
            snapshot.largest_free_order >= 0 && static_cast<uint8_t>(snapshot.largest_free_order) >= requested_order;
    }
    return has_satisfiable_zone && has_requested_order_block && free_pages >= high_pages;
}

auto snapshot_zone_watermarks(ZoneWatermarkSnapshot* out, size_t capacity, uint8_t requested_order) -> size_t {
    if (out == nullptr || capacity == 0) {
        return 0;
    }

    std::array<phys::ReclaimZoneSnapshot, MAX_WATERMARK_ZONES> zones{};
    size_t const COUNT = phys::snapshot_reclaim_zones(zones.data(), zones.size());
    uint64_t managed_pages = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        managed_pages = saturating_add(managed_pages, zones.at(i).total_pages);
    }

    size_t const OUTPUT_COUNT = std::min(COUNT, capacity);
    for (size_t i = 0; i < OUTPUT_COUNT; ++i) {
        phys::ReclaimZoneSnapshot const& zone = zones.at(i);
        Watermarks const WATERMARKS = zone_watermarks(zone.total_pages, managed_pages, requested_order);
        out[i] = ZoneWatermarkSnapshot{
            .zone = zone.zone,
            .total_pages = zone.total_pages,
            .free_pages = zone.free_pages,
            .critical_pages = WATERMARKS.critical_pages,
            .low_pages = WATERMARKS.low_pages,
            .high_pages = WATERMARKS.high_pages,
            .largest_free_order = zone.largest_free_order,
            .requested_order = requested_order,
            .level = classify_zone_pressure(zone.free_pages, zone.total_pages, zone.largest_free_order, requested_order, WATERMARKS),
        };
    }
    return OUTPUT_COUNT;
}

void get_global_stats(GlobalStatsSnapshot& out) {
    out = {
        .direct_attempts = coordinator.stats.direct_attempts.load(std::memory_order_relaxed),
        .background_attempts = coordinator.stats.background_attempts.load(std::memory_order_relaxed),
        .explicit_attempts = coordinator.stats.explicit_attempts.load(std::memory_order_relaxed),
        .explicit_requests = coordinator.stats.explicit_requests.load(std::memory_order_relaxed),
        .explicit_completions = coordinator.stats.explicit_completions.load(std::memory_order_acquire),
        .explicit_rejections = coordinator.stats.explicit_rejections.load(std::memory_order_relaxed),
        .callbacks = coordinator.stats.callbacks.load(std::memory_order_relaxed),
        .scanned_units = coordinator.stats.scanned_units.load(std::memory_order_relaxed),
        .reclaimed_units = coordinator.stats.reclaimed_units.load(std::memory_order_relaxed),
        .scanned_pages = coordinator.stats.scanned_pages.load(std::memory_order_relaxed),
        .scanned_bytes = coordinator.stats.scanned_bytes.load(std::memory_order_relaxed),
        .scanned_objects = coordinator.stats.scanned_objects.load(std::memory_order_relaxed),
        .reported_pages = coordinator.stats.reported_pages.load(std::memory_order_relaxed),
        .reported_bytes = coordinator.stats.reported_bytes.load(std::memory_order_relaxed),
        .reported_objects = coordinator.stats.reported_objects.load(std::memory_order_relaxed),
        .reclaimed_pages = coordinator.stats.reclaimed_pages.load(std::memory_order_relaxed),
        .no_progress = coordinator.stats.no_progress.load(std::memory_order_relaxed),
        .failures = coordinator.stats.failures.load(std::memory_order_relaxed),
        .recursion_avoided = coordinator.stats.recursion_avoided.load(std::memory_order_relaxed),
        .lease_contention = coordinator.stats.lease_contention.load(std::memory_order_relaxed),
        .unsafe_context_skips = coordinator.stats.unsafe_context_skips.load(std::memory_order_relaxed),
        .capability_skips = coordinator.stats.capability_skips.load(std::memory_order_relaxed),
        .cooldown_skips = coordinator.stats.cooldown_skips.load(std::memory_order_relaxed),
        .latency_us = coordinator.stats.latency_us.load(std::memory_order_relaxed),
        .latency_max_us = coordinator.stats.latency_max_us.load(std::memory_order_relaxed),
        .low_transitions = coordinator.stats.low_transitions.load(std::memory_order_relaxed),
        .critical_transitions = coordinator.stats.critical_transitions.load(std::memory_order_relaxed),
        .recovered_transitions = coordinator.stats.recovered_transitions.load(std::memory_order_relaxed),
        .worker_wakeups = coordinator.stats.worker_wakeups.load(std::memory_order_relaxed),
        .worker_no_progress = coordinator.stats.worker_no_progress.load(std::memory_order_relaxed),
        .current_pressure = coordinator.stats.current_pressure.load(std::memory_order_relaxed),
    };
}

auto snapshot_shrinker_stats(ShrinkerStatsSnapshot* out, size_t capacity) -> size_t {
    if (out == nullptr || capacity == 0) {
        return 0;
    }

    uint64_t const FLAGS = coordinator.registry_lock.lock_irqsave();
    size_t const COUNT = std::min(coordinator.entry_count, capacity);
    for (size_t i = 0; i < COUNT; ++i) {
        RegistryEntry const& entry = coordinator.entries.at(i);
        out[i] = {
            .name = entry.shrinker.name,
            .unit = entry.shrinker.unit,
            .scan_unit = entry.shrinker.scan_unit,
            .rank = entry.shrinker.rank,
            .min_priority = entry.shrinker.min_priority,
            .capabilities = entry.shrinker.capabilities,
            .count_calls = entry.stats.count_calls.load(std::memory_order_relaxed),
            .scan_calls = entry.stats.scan_calls.load(std::memory_order_relaxed),
            .scanned_units = entry.stats.scanned_units.load(std::memory_order_relaxed),
            .reclaimed_units = entry.stats.reclaimed_units.load(std::memory_order_relaxed),
            .reclaimed_pages = entry.stats.reclaimed_pages.load(std::memory_order_relaxed),
            .no_progress = entry.stats.no_progress.load(std::memory_order_relaxed),
            .failures = entry.stats.failures.load(std::memory_order_relaxed),
            .context_skips = entry.stats.context_skips.load(std::memory_order_relaxed),
            .cooldown_skips = entry.stats.cooldown_skips.load(std::memory_order_relaxed),
            .latency_us = entry.stats.latency_us.load(std::memory_order_relaxed),
            .latency_max_us = entry.stats.latency_max_us.load(std::memory_order_relaxed),
            .last_count = entry.last_count,
        };
    }
    coordinator.registry_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

auto unit_name(ReclaimUnit unit) -> const char* {
    switch (unit) {
        case ReclaimUnit::PAGES:
            return "pages";
        case ReclaimUnit::BYTES:
            return "bytes";
        case ReclaimUnit::OBJECTS:
            return "objects";
    }
    return "unknown";
}

auto priority_name(ReclaimPriority priority) -> const char* {
    switch (priority) {
        case ReclaimPriority::LOW:
            return "low";
        case ReclaimPriority::NORMAL:
            return "normal";
        case ReclaimPriority::CRITICAL:
            return "critical";
    }
    return "unknown";
}

auto pressure_name(PressureLevel level) -> const char* {
    switch (level) {
        case PressureLevel::NONE:
            return "none";
        case PressureLevel::LOW:
            return "low";
        case PressureLevel::CRITICAL:
            return "critical";
        case PressureLevel::UNSATISFIABLE:
            return "unsatisfiable";
    }
    return "unknown";
}

#ifdef WOS_SELFTEST
auto selftest_priority_and_progress() -> bool {
    Coordinator state{};
    FakeShrinkerState eligible{};
    FakeShrinkerState critical_only{};
    Shrinker first = fake_descriptor("selftest_priority_eligible", eligible);
    first.rank = 5;
    Shrinker second = fake_descriptor("selftest_priority_critical", critical_only);
    second.rank = 1;
    second.min_priority = ReclaimPriority::CRITICAL;
    if (!register_on(state, first) || !register_on(state, second)) {
        return false;
    }

    ReclaimRequest const REQUEST{
        .context = ReclaimContext::EXPLICIT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 8,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    auto const RESULT = run_on(state, REQUEST, {}, reinterpret_cast<uintptr_t>(&state), false, false);
    return RESULT.has_more && RESULT.made_progress && RESULT.callbacks == 1 && RESULT.reclaimed_units == eligible.reclaimed &&
           eligible.scan_calls == 1 && critical_only.scan_calls == 0;
}

auto selftest_no_progress_backoff() -> bool {
    Coordinator state{};
    FakeShrinkerState fake{};
    fake.reclaimed = 0;
    if (!register_on(state, fake_descriptor("selftest_backoff", fake))) {
        return false;
    }
    ReclaimRequest const REQUEST{
        .context = ReclaimContext::EXPLICIT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 1,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    uintptr_t const TOKEN = reinterpret_cast<uintptr_t>(&state);
    auto const FIRST = run_on(state, REQUEST, {}, TOKEN, false, false);
    auto const SECOND = run_on(state, REQUEST, {}, TOKEN, false, false);
    auto const THIRD = run_on(state, REQUEST, {}, TOKEN, false, false);
    return !FIRST.made_progress && SECOND.callbacks == 0 && THIRD.callbacks == 1 && fake.scan_calls == 2 &&
           state.stats.cooldown_skips.load(std::memory_order_relaxed) == 1;
}

auto selftest_recursion_guard() -> bool {
    Coordinator state{};
    FakeShrinkerState fake{};
    fake.nested_coordinator = &state;
    fake.nested_token = reinterpret_cast<uintptr_t>(&state);
    fake.recurse = true;
    if (!register_on(state, fake_descriptor("selftest_recursion", fake))) {
        return false;
    }
    ReclaimRequest const REQUEST{
        .context = ReclaimContext::EXPLICIT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 1,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    auto const OUTER = run_on(state, REQUEST, {}, fake.nested_token, false, false);
    return OUTER.made_progress && fake.scan_calls == 1 && fake.nested_result.recursion_avoided && !fake.nested_result.lease_contended &&
           state.stats.recursion_avoided.load(std::memory_order_relaxed) == 1;
}

auto selftest_context_filtering() -> bool {
    Coordinator state{};
    std::array<FakeShrinkerState, 4> fakes{};
    std::array<Shrinker, 4> shrinkers{
        fake_descriptor("selftest_context_safe", fakes.at(0)),
        fake_descriptor("selftest_context_block", fakes.at(1)),
        fake_descriptor("selftest_context_io", fakes.at(2)),
        fake_descriptor("selftest_context_allocate", fakes.at(3)),
    };
    shrinkers.at(1).capabilities = RECLAIM_MAY_BLOCK;
    shrinkers.at(2).capabilities = RECLAIM_MAY_IO;
    shrinkers.at(3).capabilities = RECLAIM_MAY_ALLOCATE;
    for (auto const& shrinker : shrinkers) {
        if (!register_on(state, shrinker)) {
            return false;
        }
    }
    ReclaimRequest const REQUEST{
        .context = ReclaimContext::DIRECT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 8,
    };
    auto const RESULT = run_on(state, REQUEST, {}, reinterpret_cast<uintptr_t>(&state), false, false);
    return RESULT.callbacks == 1 && fakes.at(0).scan_calls == 1 && fakes.at(1).scan_calls == 0 && fakes.at(2).scan_calls == 0 &&
           fakes.at(3).scan_calls == 0 && state.stats.capability_skips.load(std::memory_order_relaxed) == 3;
}

auto selftest_budget_bounds() -> bool {
    Coordinator explicit_state{};
    FakeShrinkerState explicit_fake{};
    explicit_fake.reclaimable = std::numeric_limits<uint64_t>::max();
    explicit_fake.over_report = true;
    Shrinker explicit_shrinker = fake_descriptor("selftest_budget_explicit", explicit_fake);
    explicit_shrinker.unit = ReclaimUnit::BYTES;
    explicit_shrinker.scan_unit = ReclaimUnit::OBJECTS;
    explicit_shrinker.max_batch_units = 64;
    explicit_shrinker.max_scan_units = 7;
    if (!register_on(explicit_state, explicit_shrinker)) {
        return false;
    }
    ReclaimRequest const EXPLICIT_REQUEST{
        .context = ReclaimContext::EXPLICIT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 2,
        .budget_units = 20,
        .scan_budget_units = 3,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    auto const EXPLICIT_RESULT = run_on(explicit_state, EXPLICIT_REQUEST, {}, reinterpret_cast<uintptr_t>(&explicit_state), false, false);
    bool const EXPLICIT_OK = explicit_fake.seen_budget == 20 && explicit_fake.seen_scan_budget == 3 &&
                             EXPLICIT_RESULT.reclaimed_units == 20 && EXPLICIT_RESULT.scanned_units == 3 &&
                             explicit_state.entries.at(0).stats.failures.load(std::memory_order_relaxed) == 1;

    Coordinator derived_state{};
    FakeShrinkerState derived_fake{};
    derived_fake.reclaimable = std::numeric_limits<uint64_t>::max();
    Shrinker derived_shrinker = fake_descriptor("selftest_budget_derived", derived_fake);
    derived_shrinker.unit = ReclaimUnit::BYTES;
    derived_shrinker.max_batch_units = 10'000;
    derived_shrinker.max_scan_units = 9;
    if (!register_on(derived_state, derived_shrinker)) {
        return false;
    }
    ReclaimRequest const DERIVED_REQUEST{
        .context = ReclaimContext::EXPLICIT,
        .priority = ReclaimPriority::NORMAL,
        .target_pages = 2,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    static_cast<void>(run_on(derived_state, DERIVED_REQUEST, {}, reinterpret_cast<uintptr_t>(&derived_state), false, false));
    return EXPLICIT_OK && derived_fake.seen_budget == 8192 && derived_fake.seen_scan_budget == 9;
}
#endif

}  // namespace ker::mod::mm::reclaim
