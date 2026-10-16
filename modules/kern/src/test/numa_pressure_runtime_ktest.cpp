#ifdef WOS_SELFTEST
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_root_reserve.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/mm/virt.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/compartment_lifecycle_runtime.hpp>
#include <test/numa_pressure_runtime_ktest.hpp>
#include <type_traits>
#include <utility>

namespace ker::test {
namespace {
namespace mm = mod::mm;
namespace numa = mm::numa;
namespace phys = mm::phys;
namespace reclaim = mm::reclaim;
namespace virt = mm::virt;
namespace reserve = numa::root_reserve;
namespace sched = mod::sched;
namespace task = sched::task;
namespace topology = mod::acpi::numa;
using log = mod::dbg::logger<"numa-pressure-test">;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t MAX_HELD = 1024 * 1024;  // 4GiB, actual selected usable is also a bound.
constexpr uint64_t PHASE_US = 30'000'000;
constexpr uint64_t WAIT_US = 5'000'000;
constexpr uint64_t WHOLE_US = 180'000'000;
constexpr size_t FRAME_WORDS = 32768;
constexpr uint64_t FRAME_BITS = FRAME_WORDS * 64;  // Relative window <=8GiB, not a hold allowance.
constexpr size_t SEEDS = 4;
constexpr unsigned WARM_PASSES = 1;
static_assert(WARM_PASSES <= 3);
constexpr uint64_t PRIVATE_VA = 0x6B000000;
constexpr auto OWNER = mm::PhysicalPageOwner::SELFTEST;
constexpr size_t OWNER_INDEX = static_cast<size_t>(OWNER);
struct FrameOwnership {
    uint64_t eligible[FRAME_WORDS]{};
    uint64_t owned[FRAME_WORDS]{};
    uint64_t first{}, end{}, eligible_pages{};
    bool ready{};
};
enum class SeedBoundary : uint8_t { BeforeAllocate, AfterAllocate, AfterFree };
constexpr size_t SEED_OBSERVATIONS = 2 * SEEDS * 3;
struct SeedFacts {
    uint64_t saved_pa{}, owned_refcount{};
    uint32_t span_pages{};
    numa::NodeId actual_node{}, physical_node{};
    uint8_t allocation_error{};
    bool allocation_returned{}, allocation_present{}, address_valid{}, refcount_valid{};
};
// Instrumented scheduled-fixture locals. Each slot owns one independent ref
// until the final cache publication; diagnostic facts are copied while owned.
struct PreparedSeeds {
    numa::OwnedSpan spans[2 * SEEDS]{};
    SeedFacts facts[2 * SEEDS]{};
};
struct SeedObservation {
    uint64_t started_us{}, finished_us{}, cpu_before{}, cpu_after{};
    SeedFacts allocation{};
    uint16_t ordinal{}, index{};
    numa::NodeId requested_node{};
    SeedBoundary boundary{};
    bool owned_at_capture{}, released_to_page_free{}, nodes_valid{}, node_complete{};
    mm::NodeCounterError node_status{};
    uint64_t selected_cached{}, selected_buddy{}, selected_inclusive{};
    uint64_t foreign_cached{}, foreign_buddy{}, foreign_inclusive{};
    phys::PageCacheStatsSnapshot cache{};
    bool worker_valid{}, selected_job_valid{}, foreign_job_valid{};
    reclaim::SelftestWorkerObservation worker{};
    reclaim::NodeReclaimStatsSnapshot selected_job{}, foreign_job{};
    uint64_t global_background_attempts{}, global_callbacks{}, global_worker_wakeups{};
};
struct SeedDiagnostics {
    SeedObservation records[SEED_OBSERVATIONS]{};
    phys::NodeCounterSnapshot nodes{};
    reclaim::GlobalStatsSnapshot global{};
    size_t count{};
    bool overflow{}, predicate_observed{}, predicate_valid{};
    uint64_t predicate_selected_cached{}, predicate_foreign_cached{};
};
static_assert(SEED_OBSERVATIONS == 24);
static_assert(std::is_trivially_copyable_v<SeedDiagnostics>);
static_assert(sizeof(SeedDiagnostics) <= 32 * 1024, "bounded seed diagnostic storage and scratch");
struct Storage {
    SeedDiagnostics seed_diagnostics{};
    FrameOwnership frames{};
    void* batch[128]{};
    void* selected_seeds[SEEDS]{};
    void* foreign_seeds[SEEDS]{};
    phys::NodeCounterSnapshot sample{}, baseline{}, exhausted{}, final{};
    phys::NodeCounterWorkspace work{};
    phys::PageCacheStatsSnapshot cache_info{};
    phys::PhysicalBalanceSnapshot owner_before{}, owner_after{}, root_owner_before{}, root_owner_after{};
    virt::UserRootControlStats root_control_before{}, root_control_after{};
    reserve::Snapshot reserve_before{}, reserve_after{};
    reclaim::NodeReclaimStatsSnapshot stats_before{}, stats_after{};
    reclaim::SelftestWorkerObservation worker{};
    sched::ReclaimWaitObservation waiting{};
    size_t bulk_allocations{}, order0_allocations{};
    uint64_t held{}, released{}, whole_deadline{}, assertions{}, wake_cases{};
    uint64_t tail_pages{}, held_highwater{}, bulk_highwater{}, order0_highwater{}, tail_pages_highwater{};
    uint64_t limit_held{}, limit_bulk{}, limit_order0{}, limit_tail_pages{}, limit_next_pages{};
    uint64_t limit_buddy{}, limit_cached{}, limit_usable{};
    uint32_t limit_reasons{};
    bool limit_captured{}, limit_sample_valid{};
    uint64_t worker_pid{}, worker_refs{}, first_failure_us{}, completion_observed_us{}, retry_us{};
    numa::NodeId selected{}, foreign{};
    uint64_t cpu{};
    const char* phase{};
    const char* failure{};
    bool cleanup_valid{};
};
constinit Storage state{};
static_assert(sizeof(Storage) + sizeof(std::atomic<bool>) <= 1024 * 1024, "complete fixture BSS bound");
constinit std::atomic<bool> started{};

template <class T>
[[gnu::noinline]] void prepare(T& object) {
    static_assert(std::is_trivially_copyable_v<T>);
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&object);
    for (size_t i = 0; i < sizeof(object); ++i) bytes[i] = 0;
}
auto u(uint64_t value) -> unsigned long long { return static_cast<unsigned long long>(value); }
auto check(bool condition, const char* text) -> bool {
    ++state.assertions;
    if (!condition && state.failure == nullptr) state.failure = text;
    return condition;
}
auto time_left(uint64_t deadline) -> bool {
    uint64_t const NOW = mod::time::get_us();
    return check(NOW < deadline && NOW < state.whole_deadline, "finite phase/whole deadline");
}
auto sample(phys::NodeCounterSnapshot& output) -> bool {
    if (!check(phys::snapshot_node_counters(output, state.work) == mm::NodeCounterError::NONE && output.complete,
               "complete actual node counter snapshot"))
        return false;
    uint64_t usable = 0, free = 0, cached = 0, buddy = 0;
    for (size_t node = 0; node < output.node_count; ++node) {
        auto const& row = output.nodes[node];
        if (!check(row.free_inclusive == row.cached + row.buddy_free, "per-node buddy/cache identity")) return false;
        usable += row.usable;
        free += row.free_inclusive;
        cached += row.cached;
        buddy += row.buddy_free;
    }
    return check(usable == output.global.usable && free == output.global.free_inclusive && cached == output.global.cached &&
                     buddy == output.global.buddy_free && free == cached + buddy,
                 "same-lock global/node counter closure");
}
auto observe_worker() -> bool {
    return check(reclaim::selftest_observe_worker(state.worker) && state.worker.exact_daemon && state.worker.pid == state.worker_pid,
                 "exact boot-held worker identity");
}
auto wait_idle(task::Task& worker) -> bool {
    uint64_t const DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), WAIT_US);
    for (;;) {
        if (!time_left(DEADLINE) || !observe_worker()) return false;
        auto const STATUS = sched::scheduler_selftest_observe_reclaim_waiter(worker, state.waiting);
        if (!check(STATUS != sched::ReclaimWaitObservationStatus::INCOMPLETE, "complete bounded real wait-list observation")) return false;
        if (STATUS == sched::ReclaimWaitObservationStatus::FOUND_WAITING && !state.waiting.wakeup_pending &&
            !state.worker.background_pending && !state.worker.explicit_pending && state.worker.pending_nodes == 0) {
            if (!check(reclaim::snapshot_node_stats(state.selected, state.stats_after) && !state.stats_after.pending &&
                           !state.stats_after.inflight,
                       "selected job is settled before next observation"))
                return false;
            return check(state.waiting.pid == state.worker_pid && !state.waiting.current && !state.waiting.handoff,
                         "observed exact PID untimed WAITING before notification");
        }
        sched::kern_sleep_us(1000);
    }
}
auto wait_completed(uint64_t queued, uint64_t completed, bool recovery) -> bool {
    uint64_t const DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), WAIT_US);
    for (;;) {
        if (!time_left(DEADLINE)) return false;
        if (!check(reclaim::snapshot_node_stats(state.selected, state.stats_after), "actual selected worker statistics")) return false;
        auto const& row = state.stats_after;
        if (row.queued > queued && row.completed > completed && !row.pending && !row.inflight) {
            if (recovery &&
                !check(row.cache_scanned > state.stats_before.cache_scanned && row.cache_released > state.stats_before.cache_released &&
                           row.sampled_buddy_pages > state.stats_before.sampled_buddy_pages,
                       "real cache scan/release and selected buddy progress"))
                return false;
            state.completion_observed_us = mod::time::get_us();
            return true;
        }
        sched::kern_sleep_us(1000);
    }
}
auto same_reserves() -> bool {
    reserve::snapshot(state.reserve_after);
    auto const& a = state.reserve_before;
    auto const& b = state.reserve_after;
    if (!check(a.published && a.healthy && b.published && b.healthy && a.node_count == b.node_count && a.members == b.members &&
                   a.sequence == b.sequence,
               "healthy unchanged reserve membership"))
        return false;
    for (size_t node = 0; node < a.node_count; ++node) {
        auto const& x = a.nodes[node];
        auto const& y = b.nodes[node];
        if (!check(x.capacity == y.capacity && x.available == y.available && x.loaned == y.loaned && x.transfers == y.transfers &&
                       x.shortfall == y.shortfall,
                   "all origin reserve states unchanged"))
            return false;
    }
    return true;
}
// Capture only at scheduled fill/take refusal boundaries, before prefix RAII.
// No generic check() hook: sampling must never enter a guarded/locked context.
void capture_limit(uint32_t reasons, uint32_t next_pages) {
    if (state.limit_captured) return;
    state.limit_captured = true;
    state.limit_reasons = reasons;
    state.limit_next_pages = next_pages;
    state.limit_held = state.held;
    state.limit_bulk = state.bulk_allocations;
    state.limit_order0 = state.order0_allocations;
    state.limit_tail_pages = state.tail_pages;
    auto const STATUS = phys::snapshot_node_counters(state.sample, state.work);
    state.limit_sample_valid = STATUS == mm::NodeCounterError::NONE && state.sample.complete && state.selected < state.sample.node_count;
    if (state.limit_sample_valid) {
        auto const& row = state.sample.nodes[state.selected];
        state.limit_buddy = row.buddy_free;
        state.limit_cached = row.cached;
        state.limit_usable = row.usable;
    }
}

// Consume the successful snapshot's canonical views immediately: later node
// samples overwrite state.work. Only immutable allocator geometry is read.
auto prepare_frame_geometry(const topology::Topology& machine) -> bool {
    auto& frames = state.frames;
    auto const& baseline = state.baseline;
    size_t const COUNT = baseline.allocator_count;
    uint64_t const HHDM = mm::addr::get_hhdm_offset();
    if (!check(!frames.ready && baseline.complete && baseline.error == mm::NodeCounterError::NONE && COUNT != 0 &&
                   COUNT <= mm::node_counters::MAX_ALLOCATORS && machine.node_count != 0 && machine.node_count <= topology::MAX_NODES &&
                   machine.node_count == baseline.node_count && state.selected < machine.node_count && machine.memory_range_count != 0 &&
                   machine.memory_range_count <= topology::MAX_MEMORY_RANGES,
               "complete canonical geometry prerequisite"))
        return false;
    for (size_t i = 0; i < FRAME_WORDS; ++i)
        if (!check(frames.eligible[i] == 0 && frames.owned[i] == 0, "empty ownership masks before geometry")) return false;
    uint64_t previous_end = 0;
    for (size_t r = 0; r < machine.memory_range_count; ++r) {
        auto const& range = machine.memory[r];
        if (!check(range.base % PAGE == 0 && range.length != 0 && range.length % PAGE == 0 && range.base <= UINT64_MAX - range.length &&
                       range.node < machine.node_count && (r == 0 || range.base >= previous_end),
                   "sorted disjoint immutable topology geometry"))
            return false;
        previous_end = range.base + range.length;
    }
    uint64_t first = UINT64_MAX, end = 0, selected_pages = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& view = state.work.views[i];
        auto const* allocator = view.allocator;
        if (!check(allocator != nullptr && view.physical_base % PAGE == 0 && view.physical_base <= UINT64_MAX - HHDM &&
                       allocator->base == view.physical_base + HHDM && allocator->total_pages != 0 &&
                       allocator->metadata_pages <= allocator->total_pages &&
                       allocator->usable_pages == allocator->total_pages - allocator->metadata_pages,
                   "exact canonical allocator base and immutable usable geometry"))
            return false;
        uint64_t const BYTES = static_cast<uint64_t>(allocator->total_pages) * PAGE;
        if (!check(view.physical_base <= UINT64_MAX - BYTES && allocator->base <= UINT64_MAX - BYTES,
                   "checked complete physical and HHDM allocator extent"))
            return false;
        uint64_t const END = view.physical_base + BYTES;
        for (size_t j = 0; j < i; ++j) {
            auto const& other = state.work.views[j];
            uint64_t const OTHER_END = other.physical_base + static_cast<uint64_t>(other.allocator->total_pages) * PAGE;
            if (!check(other.allocator != allocator && (END <= other.physical_base || OTHER_END <= view.physical_base),
                       "canonical distinct allocators never overlap"))
                return false;
        }
        uint64_t const USABLE = view.physical_base + static_cast<uint64_t>(allocator->metadata_pages) * PAGE;
        uint64_t covered = 0;
        for (size_t r = 0; r < machine.memory_range_count; ++r) {
            auto const& range = machine.memory[r];
            uint64_t const LOW = USABLE > range.base ? USABLE : range.base;
            uint64_t const RANGE_END = range.base + range.length;
            uint64_t const HIGH = END < RANGE_END ? END : RANGE_END;
            if (LOW >= HIGH) continue;
            covered += (HIGH - LOW) / PAGE;
            if (range.node != state.selected) continue;
            if (LOW < first) first = LOW;
            if (HIGH > end) end = HIGH;
            selected_pages += (HIGH - LOW) / PAGE;
        }
        if (!check(covered == allocator->usable_pages, "complete allocator usable topology coverage")) return false;
    }
    if (!check(first != UINT64_MAX && end > first && end - first <= FRAME_BITS * PAGE && selected_pages != 0 &&
                   selected_pages == baseline.nodes[state.selected].usable && selected_pages <= MAX_HELD,
               "complete selected relative window and exact cheap usable denominator"))
        return false;
    // All geometry validates before the first eligibility write. The canonical
    // allocator views are consumed completely here; no view survives a sample.
    uint64_t marked = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& view = state.work.views[i];
        uint64_t const USABLE = view.physical_base + static_cast<uint64_t>(view.allocator->metadata_pages) * PAGE;
        uint64_t const END = view.physical_base + static_cast<uint64_t>(view.allocator->total_pages) * PAGE;
        for (size_t r = 0; r < machine.memory_range_count; ++r) {
            auto const& range = machine.memory[r];
            if (range.node != state.selected) continue;
            uint64_t const LOW = USABLE > range.base ? USABLE : range.base;
            uint64_t const RANGE_END = range.base + range.length;
            uint64_t const HIGH = END < RANGE_END ? END : RANGE_END;
            for (uint64_t physical = LOW; physical < HIGH; physical += PAGE) {
                uint64_t const INDEX = (physical - first) / PAGE;
                uint64_t const BIT = uint64_t{1} << (INDEX % 64);
                if (!check(INDEX < FRAME_BITS && (frames.eligible[INDEX / 64] & BIT) == 0, "unique eligible selected usable frame"))
                    return false;
                frames.eligible[INDEX / 64] |= BIT;
                ++marked;
            }
        }
    }
    if (!check(marked == selected_pages, "complete exact eligible mask count")) return false;
    frames.first = first;
    frames.end = end;
    frames.eligible_pages = marked;
    frames.ready = true;
    return true;
}
auto frame_span_index(uint64_t physical, uint32_t pages, uint64_t& first) -> bool {
    auto const& frames = state.frames;
    uint64_t const BYTES = static_cast<uint64_t>(pages) * PAGE;
    if (!frames.ready || (pages != 1 && pages != 512) || physical % PAGE != 0 || physical < frames.first || physical > UINT64_MAX - BYTES ||
        physical + BYTES > frames.end)
        return false;
    uint64_t const INDEX = (physical - frames.first) / PAGE;
    if (INDEX >= FRAME_BITS || pages > FRAME_BITS - INDEX) return false;
    first = INDEX;
    return true;
}
struct Holdings {
    Holdings() = default;
    Holdings(const Holdings&) = delete;
    ~Holdings() { clear(); }
    void clear() {
        size_t pending = 0;
        auto flush = [&]() {
            if (pending == 0) return;
            auto const FREED = phys::page_ref_dec_batch({state.batch, pending});
            state.cleanup_valid &= FREED.refs_decremented == pending && FREED.pages_freed == pending;
            state.held -= pending;
            state.released += pending;
            pending = 0;  // Never retry refs already submitted, even on a stats mismatch.
        };
        for (size_t word = 0; word < FRAME_WORDS; ++word) {
            while (state.frames.owned[word] != 0) {
                unsigned const BIT = static_cast<unsigned>(__builtin_ctzll(state.frames.owned[word]));
                uint64_t const INDEX = word * 64 + BIT;
                uint64_t const PHYSICAL = state.frames.first + INDEX * PAGE;
                state.batch[pending++] = mm::addr::get_virt_pointer(PHYSICAL);
                state.frames.owned[word] &= ~(uint64_t{1} << BIT);  // Bit authority transfers to pending batch.
                if (pending == 128) flush();
            }
        }
        flush();
        state.cleanup_valid &= state.held == 0;
        state.bulk_allocations = state.order0_allocations = 0;
        state.tail_pages = 0;
    }
    auto take(numa::AllocationResult& result, uint32_t pages) -> bool {
        if (!check((pages == 1 || pages == 512) && result.span.data() != nullptr && result.span.pages() == pages,
                   "actual complete supported span shape before transfer")) {
            capture_limit(8, pages);
            return false;
        }
        auto const ADDRESS = reinterpret_cast<uintptr_t>(result.span.data());
        uint64_t const BYTES = static_cast<uint64_t>(pages) * PAGE;
        uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(ADDRESS));
        uint64_t first = 0;
        if (!check(ADDRESS % PAGE == 0 && ADDRESS <= UINTPTR_MAX - BYTES && ADDRESS >= mm::addr::get_hhdm_offset() &&
                       reinterpret_cast<uint64_t>(mm::addr::get_virt_pointer(PHYSICAL)) == ADDRESS &&
                       frame_span_index(PHYSICAL, pages, first),
                   "checked exact relative physical span before transfer")) {
            capture_limit(16, pages);
            return false;
        }
        if (!check(result.actual == state.selected, "actual selected node before ownership transfer")) {
            capture_limit(32, pages);
            return false;
        }
        if (!check(state.held <= MAX_HELD - pages && pages <= state.baseline.nodes[state.selected].usable &&
                       state.held <= state.baseline.nodes[state.selected].usable - pages,
                   "unchanged physical page bound before transfer")) {
            capture_limit(4, pages);
            return false;
        }
        for (uint64_t index = first; index < first + pages; ++index) {
            uint64_t const BIT = uint64_t{1} << (index % 64);
            if (!check((state.frames.eligible[index / 64] & BIT) != 0 && (state.frames.owned[index / 64] & BIT) == 0,
                       "entire candidate eligible and unowned before transfer")) {
                capture_limit(64, pages);
                return false;
            }
        }
        // Sole-ref ownership changes only after the ENTIRE span validates.
        for (uint64_t index = first; index < first + pages; ++index) state.frames.owned[index / 64] |= uint64_t{1} << (index % 64);
        (void)result.span.release();
        state.held += pages;
        if (pages == 512)
            ++state.bulk_allocations;
        else {
            ++state.order0_allocations;
            ++state.tail_pages;
        }
        if (state.held > state.held_highwater) state.held_highwater = state.held;
        if (state.bulk_allocations > state.bulk_highwater) state.bulk_highwater = state.bulk_allocations;
        if (state.order0_allocations > state.order0_highwater) state.order0_highwater = state.order0_allocations;
        if (state.tail_pages > state.tail_pages_highwater) state.tail_pages_highwater = state.tail_pages;
        return true;
    }
};
struct PrivateRoot {
    task::Task* subject{};
    mm::paging::PageTable* root{};
    virt::UserPagePin pin{};
    bool reader{};
    void* unbound_frame{};
    PrivateRoot() = default;
    PrivateRoot(const PrivateRoot&) = delete;
    ~PrivateRoot() { clear(); }
    auto init() -> bool {
        subject = new (std::nothrow) task::Task{};
        if (!check(subject != nullptr, "private root subject allocation")) return false;
        root = virt::create_pagemap_may_fail();
        if (!check(root != nullptr, "private root allocation")) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = root;
        unbound_frame = phys::page_alloc_may_fail(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, PAGE, "numa-pressure-pin");
        if (!check(unbound_frame != nullptr, "private root mapped frame")) return false;
        std::memset(unbound_frame, 0x5a, PAGE);
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(unbound_frame)));
        virt::map_page(root, PRIVATE_VA, PHYSICAL, mm::paging::page_types::USER);
        unbound_frame = nullptr;  // A returned map_page transfers the ref to its PTE.
        if (!check(virt::translate(root, PRIVATE_VA) == PHYSICAL, "actual private mapping owns exact frame")) return false;
        return true;
    }
    auto acquire() -> bool {
        mm::paging::PageTable* actual = nullptr;
        reader = subject->try_acquire_usercopy_pagemap(actual);
        if (!check(reader && actual == root, "real Task root-reader lease")) return false;
        return check(virt::pin_user_page_for_task(subject, root, PRIVATE_VA, false, false, pin), "actual frame/root pin");
    }
    void release() {
        if (pin.hhdm_page != nullptr) virt::unpin_user_page(pin);
        if (reader) {
            subject->release_usercopy_pagemap();
            reader = false;
        }
    }
    void clear() {
        release();
        if (root != nullptr) {
            state.cleanup_valid &= subject->usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0 &&
                                   virt::selftest_user_pagemap_reader_count(root) == 0;
            auto* detached = subject->detach_pagemap_after_usercopy_quiescence();
            if (detached != root) {
                state.cleanup_valid = false;
                return;
            }
            virt::destroy_user_space(root, 0, "numa-pressure", "private-fixture-return");
            virt::release_pagemap(root);
            root = nullptr;
        }
        if (unbound_frame != nullptr) {
            state.cleanup_valid &= phys::page_ref_dec(unbound_frame) == 0;
            unbound_frame = nullptr;
        }
        delete subject;
        subject = nullptr;
    }
};
struct IrqOff {
    bool enabled{sched::interrupts_enabled()};
    IrqOff() { asm volatile("cli" ::: "memory"); }
    ~IrqOff() {
        if (enabled) asm volatile("sti" ::: "memory");
    }
};
auto timed_notify(uint64_t& elapsed) -> reclaim::NodeReclaimStatus {
    auto const START = mod::time::get_us();
    auto const RESULT = reclaim::notify_order0_node_failure(state.selected);
    elapsed = mod::time::get_us() - START;
    return RESULT;
}
auto one_wake(task::Task& worker, PrivateRoot& private_root, unsigned kind) -> bool {
    if (!wait_idle(worker)) return false;
    state.stats_before = state.stats_after;
    uint32_t const DEPTH = sched::preempt_count();
    bool const IF = sched::interrupts_enabled();
    reclaim::NodeReclaimStatus result = reclaim::NodeReclaimStatus::NOT_READY;
    uint64_t elapsed = 0;
    if (kind == 0)
        result = timed_notify(elapsed);
    else if (kind == 1) {
        IrqOff irq;
        result = timed_notify(elapsed);
    } else if (kind == 2) {
        sched::PreemptGuard guard;
        result = timed_notify(elapsed);
    } else if (kind == 3) {
        syscall::vmem::SharedVmemPublicationGuard guard;
        result = timed_notify(elapsed);
    } else {
        if (!private_root.acquire()) return false;
        {
            syscall::vmem::SharedVmemPublicationGuard pub;
            sched::PreemptGuard guard;
            IrqOff irq;
            result = timed_notify(elapsed);
        }
        private_root.release();
    }
    if (!check(result == reclaim::NodeReclaimStatus::NONE && sched::preempt_count() == DEPTH && sched::interrupts_enabled() == IF,
               "real notification accepted and exact IF/preempt state restored"))
        return false;
    if (!wait_completed(state.stats_before.queued, state.stats_before.completed, false)) return false;
    ++state.wake_cases;
    log::info(
        "NUMA_PRESSURE WAKE kind=%u pid=%llu observed_wait_cpu=%llu elapsed_us=%llu queued=%llu completed=%llu separate_observations=1",
        kind, u(state.worker_pid), u(state.waiting.cpu), u(elapsed), u(state.stats_after.queued), u(state.stats_after.completed));
    return true;
}
auto drain(uint64_t mask) -> bool {
    phys::NodeCacheDrainCursor cursor{};
    for (size_t step = 0; step < 64; ++step) {
        auto const RESULT = phys::drain_page_caches_for_nodes(mask, 128, 256, cursor);
        if (!check(!RESULT.failed, "actual cache drain remains valid")) return false;
        if (RESULT.scan_complete) return true;
    }
    return check(false, "bounded physical cache drain completes");
}
auto fill(Holdings& holdings, bool measured) -> bool {
    auto const CONTEXT = numa::AllocationContext{.policy = {.nodes = uint64_t{1} << state.selected, .mode = numa::Mode::BIND},
                                                 .local_node = state.selected,
                                                 .virtual_page_index = 0};
    uint64_t const DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), PHASE_US);
    for (;;) {
        if (!time_left(DEADLINE)) return false;
        if (!check(state.held + 512 <= MAX_HELD, "512 unchanged physical prefix bound")) {
            capture_limit(4U, 512);
            return false;
        }
        if (!sample(state.sample) ||
            !check(state.sample.nodes[state.foreign].buddy_free >= 65536, "foreign buddy safety floor during hold"))
            return false;
        auto result = numa::try_allocate_pages(CONTEXT, 512, 512 * PAGE, OWNER, phys::PageInitialization::FULL_OVERWRITE);
        if (result.span.empty()) {
            if (!check(result.error == numa::AllocationError::EXHAUSTED, "real512 attempt fails only by exhaustion")) return false;
            break;
        }
        if (!holdings.take(result, 512)) return false;
    }
    for (;;) {
        if (!time_left(DEADLINE)) return false;
        if (!check(state.held < MAX_HELD, "order0 physical prefix capacity")) {
            capture_limit(4, 1);
            return false;
        }
        if (!sample(state.sample)) return false;
        if (!check(state.sample.nodes[state.foreign].buddy_free >= 65536, "foreign buddy safety floor during tail")) return false;
        auto const BEFORE_BUDDY = state.sample.nodes[state.selected].buddy_free;
        auto const BEFORE_CACHED = state.sample.nodes[state.selected].cached;
        if (measured && BEFORE_BUDDY == 0) state.exhausted = state.sample;
        auto result = numa::try_allocate_pages(CONTEXT, 1, PAGE, OWNER, phys::PageInitialization::FULL_OVERWRITE);
        if (!result.span.empty()) {
            if (!holdings.take(result, 1)) return false;
            continue;
        }
        state.first_failure_us = mod::time::get_us();
        if (!check(result.error == numa::AllocationError::EXHAUSTED && result.actual == numa::INVALID_NODE,
                   "actual empty BIND EXHAUSTED before any separate retry"))
            return false;
        return !measured || check(BEFORE_BUDDY == 0 && BEFORE_CACHED > 0, "actual selected buddy exhausted while real cache holds frames");
    }
}
// Addressable fixed BSS outputs; each API returns after its own locks. These
// are separate observations, not one cross-subsystem instant or a reservation.
void observe_seed(SeedBoundary boundary, numa::NodeId requested, size_t index, const SeedFacts& allocation) {
    auto& diagnostics = state.seed_diagnostics;
    if (diagnostics.count == SEED_OBSERVATIONS) {
        diagnostics.overflow = true;
        return;
    }
    auto& row = diagnostics.records[diagnostics.count];
    row.ordinal = static_cast<uint16_t>(diagnostics.count++);
    row.boundary = boundary;
    row.index = static_cast<uint16_t>(index);
    row.requested_node = requested;
    row.allocation = allocation;
    row.owned_at_capture = boundary == SeedBoundary::AfterAllocate && allocation.allocation_present;
    row.released_to_page_free = boundary == SeedBoundary::AfterFree;
    if (!row.owned_at_capture) {
        row.allocation.refcount_valid = false;
        row.allocation.owned_refcount = 0;
    }
    row.started_us = mod::time::get_us();
    row.cpu_before = mod::cpu::current_cpu();
    row.node_status = phys::snapshot_node_counters(diagnostics.nodes, state.work);
    row.node_complete = diagnostics.nodes.complete;
    row.nodes_valid = row.node_status == mm::NodeCounterError::NONE && row.node_complete &&
                      diagnostics.nodes.node_count <= mm::PAGE_ALLOCATOR_MAX_NODES && state.selected < diagnostics.nodes.node_count &&
                      state.foreign < diagnostics.nodes.node_count;
    if (row.nodes_valid) {
        auto const& selected = diagnostics.nodes.nodes[state.selected];
        auto const& foreign = diagnostics.nodes.nodes[state.foreign];
        row.selected_cached = selected.cached;
        row.selected_buddy = selected.buddy_free;
        row.selected_inclusive = selected.free_inclusive;
        row.foreign_cached = foreign.cached;
        row.foreign_buddy = foreign.buddy_free;
        row.foreign_inclusive = foreign.free_inclusive;
    }
    phys::get_page_cache_stats_snapshot(row.cache);
    row.worker_valid = reclaim::selftest_observe_worker(row.worker);
    row.selected_job_valid = reclaim::snapshot_node_stats(state.selected, row.selected_job);
    row.foreign_job_valid = reclaim::snapshot_node_stats(state.foreign, row.foreign_job);
    reclaim::get_global_stats(diagnostics.global);
    row.global_background_attempts = diagnostics.global.background_attempts;
    row.global_callbacks = diagnostics.global.callbacks;
    row.global_worker_wakeups = diagnostics.global.worker_wakeups;
    row.finished_us = mod::time::get_us();
    row.cpu_after = mod::cpu::current_cpu();
}
// Called only while the returned OwnedSpan still owns every saved frame.
auto seed_allocation_facts(const numa::AllocationResult& result) -> SeedFacts {
    SeedFacts facts{};
    facts.physical_node = numa::INVALID_NODE;
    facts.allocation_returned = true;
    facts.allocation_error = static_cast<uint8_t>(result.error);
    facts.actual_node = result.actual;
    facts.span_pages = result.span.pages();
    facts.allocation_present = !result.span.empty();
    if (facts.allocation_present) {
        facts.saved_pa = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(result.span.data())));
        facts.address_valid = true;
        facts.physical_node = numa::physical_node(facts.saved_pa);
        facts.owned_refcount = phys::page_ref_get(result.span.data());
        facts.refcount_valid = true;
    }
    return facts;
}
void save_seed_predicate(bool valid) {
    auto& diagnostics = state.seed_diagnostics;
    diagnostics.predicate_observed = true;
    diagnostics.predicate_valid = valid;
    if (valid) {
        diagnostics.predicate_selected_cached = state.sample.nodes[state.selected].cached;
        diagnostics.predicate_foreign_cached = state.sample.nodes[state.foreign].cached;
    }
}
// Failure-only, after run() and the shutdown hold have unwound. PA is a
// historic scalar after free; no released frame is inspected by this logger.
void report_seed_diagnostics() {
    auto const& diagnostics = state.seed_diagnostics;
    for (size_t i = 0; i < diagnostics.count; ++i) {
        auto const& row = diagnostics.records[i];
        auto const& allocation = row.allocation;
        unsigned const VALID = (row.nodes_valid ? 1U : 0U) | (row.worker_valid ? 2U : 0U) | (row.selected_job_valid ? 4U : 0U) |
                               (row.foreign_job_valid ? 8U : 0U) | (allocation.address_valid ? 16U : 0U) |
                               (allocation.refcount_valid ? 32U : 0U) | (row.owned_at_capture ? 64U : 0U) |
                               (row.released_to_page_free ? 128U : 0U) | (allocation.allocation_returned ? 256U : 0U) |
                               (allocation.allocation_present ? 512U : 0U) | (row.node_complete ? 1024U : 0U);
        unsigned const WORKER = (row.worker.background_pending ? 1U : 0U) | (row.worker.explicit_pending ? 2U : 0U);
        unsigned const SELECTED = static_cast<unsigned>(row.selected_job.status) | (row.selected_job.pending ? 256U : 0U) |
                                  (row.selected_job.inflight ? 512U : 0U);
        unsigned const FOREIGN =
            static_cast<unsigned>(row.foreign_job.status) | (row.foreign_job.pending ? 256U : 0U) | (row.foreign_job.inflight ? 512U : 0U);
        log::error(
            "seed_diag_v1 i=%u b=%u j=%u n=%u/%u/%u e=%u p=%u v=%x ns=%u t=%llx/%llx c=%llx/%llx pa=%llx r=%llx "
            "scb=%llx/%llx fcb=%llx/%llx",
            static_cast<unsigned>(row.ordinal), static_cast<unsigned>(row.boundary), static_cast<unsigned>(row.index),
            static_cast<unsigned>(row.requested_node), static_cast<unsigned>(allocation.actual_node),
            static_cast<unsigned>(allocation.physical_node), static_cast<unsigned>(allocation.allocation_error), allocation.span_pages,
            VALID, static_cast<unsigned>(row.node_status), u(row.started_us), u(row.finished_us), u(row.cpu_before), u(row.cpu_after),
            u(allocation.saved_pa), u(allocation.owned_refcount), u(row.selected_cached), u(row.selected_buddy), u(row.foreign_cached),
            u(row.foreign_buddy));
        log::error("seed_diag_v1 activity i=%u cache=%llx/%llx/%llx/%llx/%llx w=%x/%llx sj=%x/%llx fj=%x/%llx g=%llx/%llx",
                   static_cast<unsigned>(row.ordinal), u(row.cache.cached_pages), u(row.cache.alloc_hits), u(row.cache.free_hits),
                   u(row.cache.refill_pages), u(row.cache.drain_pages), WORKER, u(row.worker.pending_nodes), SELECTED,
                   u(row.selected_job.completed), FOREIGN, u(row.foreign_job.completed), u(row.global_background_attempts),
                   u(row.global_worker_wakeups));
    }
    log::error("seed_diag_v1 predicate observed=%u valid=%u selected_cached=%llu foreign_cached=%llu records=%llu overflow=%u",
               diagnostics.predicate_observed ? 1U : 0U, diagnostics.predicate_valid ? 1U : 0U, u(diagnostics.predicate_selected_cached),
               u(diagnostics.predicate_foreign_cached), u(diagnostics.count), diagnostics.overflow ? 1U : 0U);
}
// Failure-only copied observations. The owner samples have DISTINCT bases;
// none is synchronized with the seed-publication/final cheap node samples.
// Read no released frame, current owner object, allocator metadata or provider.
void report_closure_diagnostics() {
    bool const REACHED = state.phase != nullptr && std::strcmp(state.phase, "physical-owner-closure") == 0;
    bool const NODES_VALID = REACHED && state.baseline.complete && state.final.complete &&
                             state.baseline.error == mm::NodeCounterError::NONE && state.final.error == mm::NodeCounterError::NONE &&
                             state.baseline.node_count == 2 && state.final.node_count == 2;
    log::error(
        "closure_diag_v1 reached=%u nodes_valid=%u complete=%u/%u error=%u/%u nodes=%u/%u "
        "node_bases=seed_publication/pressure_final owner_bases=pre_seed/pre_private_root/pressure_final_root_live",
        REACHED ? 1U : 0U, NODES_VALID ? 1U : 0U, state.baseline.complete ? 1U : 0U, state.final.complete ? 1U : 0U,
        static_cast<unsigned>(state.baseline.error), static_cast<unsigned>(state.final.error),
        static_cast<unsigned>(state.baseline.node_count), static_cast<unsigned>(state.final.node_count));
    if (!NODES_VALID) return;
    auto magnitude = [](uint64_t a, uint64_t b) { return b >= a ? b - a : a - b; };
    auto sign = [](uint64_t a, uint64_t b) { return b >= a ? '+' : '-'; };
    if (NODES_VALID) {
        for (size_t i = 0; i < 3; ++i) {
            auto const& a = i < 2 ? state.baseline.nodes[i] : state.baseline.global;
            auto const& b = i < 2 ? state.final.nodes[i] : state.final.global;
            log::error(
                "closure_diag_v1 kind=%s node=%u usable=%llu/%llu free=%llu/%llu d_free=%c%llu "
                "cached=%llu/%llu buddy=%llu/%llu",
                i < 2 ? "node" : "global", static_cast<unsigned>(i), u(a.usable), u(b.usable), u(a.free_inclusive), u(b.free_inclusive),
                sign(a.free_inclusive, b.free_inclusive), u(magnitude(a.free_inclusive, b.free_inclusive)), u(a.cached), u(b.cached),
                u(a.buddy_free), u(b.buddy_free));
        }
    }
    const char* const labels[]{"pre_seed", "pre_private_root", "pressure_final_root_live"};
    const phys::PhysicalBalanceSnapshot* const owners[]{&state.owner_before, &state.root_owner_before, &state.owner_after};
    for (size_t i = 0; i < 3; ++i) {
        auto const& row = *owners[i];
        log::error(
            "closure_diag_v1 physical_base=%s managed=%llu free=%llu metadata=%llu descriptor=%llu "
            "cache_reserve=%llu/%llu owner=%llu identity=%llu mismatch=%llu untracked=%llu zones=%llu",
            labels[i], u(row.managed_pages), u(row.free_pages), u(row.allocator_metadata_pages), u(row.zone_descriptor_pages),
            u(row.per_cpu_page_cache_reserve_pages), u(row.per_cpu_page_cache_reserve_objects), u(row.owner_pages), u(row.identity_pages),
            u(row.identity_mismatch_pages), u(row.untracked_unreclaimable_pages), u(row.zone_count));
    }
    for (size_t i = 0; i < mm::PHYSICAL_PAGE_OWNER_COUNT; ++i) {
        auto const& a = state.owner_before.owners[i];
        auto const& r = state.root_owner_before.owners[i];
        auto const& b = state.owner_after.owners[i];
        if (a.pages == r.pages && r.pages == b.pages && a.objects == r.objects && r.objects == b.objects) continue;
        log::error(
            "closure_diag_v1 owner=%u pages=%llu/%llu/%llu d_seed=%c%llu d_root=%c%llu "
            "objects=%llu/%llu/%llu d_seed=%c%llu d_root=%c%llu",
            static_cast<unsigned>(i), u(a.pages), u(r.pages), u(b.pages), sign(a.pages, b.pages), u(magnitude(a.pages, b.pages)),
            sign(r.pages, b.pages), u(magnitude(r.pages, b.pages)), u(a.objects), u(r.objects), u(b.objects), sign(a.objects, b.objects),
            u(magnitude(a.objects, b.objects)), sign(r.objects, b.objects), u(magnitude(r.objects, b.objects)));
    }
}
auto prepare_seed_node(numa::NodeId node, PreparedSeeds& seeds, size_t offset) -> bool {
    auto const CONTEXT = numa::AllocationContext{.policy = {.nodes = uint64_t{1} << node, .mode = numa::Mode::BIND}, .local_node = node};
    for (size_t i = 0; i < SEEDS; ++i) {
        observe_seed(SeedBoundary::BeforeAllocate, node, i,
                     SeedFacts{.actual_node = numa::INVALID_NODE, .physical_node = numa::INVALID_NODE});
        auto result = numa::try_allocate_pages(CONTEXT, 1, PAGE, OWNER, phys::PageInitialization::FULL_OVERWRITE);
        auto const FACTS = seed_allocation_facts(result);
        observe_seed(SeedBoundary::AfterAllocate, node, i, FACTS);
        if (!check(result.error == numa::AllocationError::NONE && result.actual == node && phys::page_ref_get(result.span.data()) == 1,
                   "real sole selected cache seed"))
            return false;
        seeds.facts[offset + i] = FACTS;
        seeds.spans[offset + i] = std::move(result.span);
    }
    return true;
}
void release_seed_node(numa::NodeId node, PreparedSeeds& seeds, size_t offset, void** addresses) {
    // The same eight sole ref1 owners transfer directly to the real cache-aware
    // page_free API. No allocation or released-frame inspection occurs here.
    for (size_t i = 0; i < SEEDS; ++i) {
        addresses[i] = seeds.spans[offset + i].release();
        phys::page_free(addresses[i]);
        observe_seed(SeedBoundary::AfterFree, node, i, seeds.facts[offset + i]);
    }
}
auto prepare_cache_seeds(task::Task& worker, PreparedSeeds& seeds) -> bool {
    state.phase = "warm-whole-selected-domain";
    if (!sample(state.baseline)) return false;
    for (unsigned pass = 0; pass < WARM_PASSES; ++pass) {
        Holdings warm;
        if (!fill(warm, false)) return false;
    }
    if (!check(state.cleanup_valid && state.held == 0, "full warm successful prefix returned")) return false;
    if (!wait_idle(worker) || !drain((uint64_t{1} << state.selected) | (uint64_t{1} << state.foreign))) return false;
    if (!sample(state.baseline)) return false;
    if (!check(state.baseline.nodes[state.foreign].buddy_free >= 65536, "foreign floor after actual warmup")) return false;
    // Both baselines precede all seed claims. The independent outer root
    // baseline is captured only after all preparation has actually completed.
    phys::get_physical_balance_snapshot(state.owner_before);
    if (!check(state.owner_before.identity_mismatch_pages == 0, "warm physical baseline is internally closed")) return false;
    reserve::snapshot(state.reserve_before);
    return prepare_seed_node(state.selected, seeds, 0) && prepare_seed_node(state.foreign, seeds, SEEDS);
}
auto publish_cache_seeds(task::Task& worker, PreparedSeeds& seeds) -> bool {
    // Preparation and measured-root setup can leave non-seed cache refill
    // residue. The real drain cannot consume the eight still-allocated refs.
    if (!wait_idle(worker) || !drain((uint64_t{1} << state.selected) | (uint64_t{1} << state.foreign))) return false;
    if (!sample(state.sample) || !check(state.sample.nodes[state.selected].cached == 0 && state.sample.nodes[state.foreign].cached == 0,
                                        "observed empty selected and foreign caches before seed publication"))
        return false;
    release_seed_node(state.selected, seeds, 0, state.selected_seeds);
    release_seed_node(state.foreign, seeds, SEEDS, state.foreign_seeds);
    auto const SEED_SAMPLE_VALID = sample(state.sample);
    save_seed_predicate(SEED_SAMPLE_VALID);
    if (!SEED_SAMPLE_VALID ||
        !check(state.sample.nodes[state.selected].cached == SEEDS && state.sample.nodes[state.foreign].cached == SEEDS,
               "observed exact real cache seed counts"))
        return false;
    // This exact successful predicate sample includes the now-free seeds and
    // all real preparation cost; it is never adjusted or independently resampled.
    state.baseline = state.sample;
    return true;
}
auto pressure(task::Task& worker, PreparedSeeds& seeds) -> bool {
    if (!publish_cache_seeds(worker, seeds)) return false;
    if (!wait_idle(worker)) return false;
    state.stats_before = state.stats_after;
    state.phase = "measured-real-exhaustion";
    {
        Holdings measured;
        if (!fill(measured, true)) return false;
        if (!wait_completed(state.stats_before.queued, state.stats_before.completed, true) || !sample(state.sample)) return false;
        if (!check(state.sample.nodes[state.selected].buddy_free > 0 && state.sample.nodes[state.foreign].cached == SEEDS,
                   "selected buddy recovery while foreign cache unchanged") ||
            !same_reserves())
            return false;
        auto const CONTEXT = numa::AllocationContext{.policy = {.nodes = uint64_t{1} << state.selected, .mode = numa::Mode::BIND},
                                                     .local_node = state.selected};
        auto retry = numa::try_allocate_pages(CONTEXT, 1, PAGE, OWNER, phys::PageInitialization::FULL_OVERWRITE);
        state.retry_us = mod::time::get_us();
        if (!check(
                retry.error == numa::AllocationError::NONE && retry.actual == state.selected && phys::page_ref_get(retry.span.data()) == 1,
                "distinct later actual BIND allocation succeeds"))
            return false;
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(retry.span.data())));
        if (!check(numa::physical_node(PHYSICAL) == state.selected, "retry actual physical node")) return false;
        bool known = false;
        for (auto* page : state.selected_seeds) known |= page == retry.span.data();
        if (!check(known && state.exhausted.nodes[state.selected].cached == SEEDS, "retry is a measured selected cache-release frame"))
            return false;
        std::memset(retry.span.data(), 0x6d, PAGE);
        log::info(
            "NUMA_PRESSURE RECOVERY node=%u held=%llu fail_us=%llu completion_observed_us=%llu retry_us=%llu cache_released=%llu "
            "buddy_progress=%llu foreign_cached=%llu",
            static_cast<unsigned>(state.selected), u(state.held), u(state.first_failure_us), u(state.completion_observed_us),
            u(state.retry_us), u(state.stats_after.cache_released - state.stats_before.cache_released),
            u(state.stats_after.sampled_buddy_pages - state.stats_before.sampled_buddy_pages), u(state.sample.nodes[state.foreign].cached));
    }
    state.phase = "physical-owner-closure";
    if (!wait_idle(worker) || !drain((uint64_t{1} << state.selected) | (uint64_t{1} << state.foreign))) return false;
    phys::get_physical_balance_snapshot(state.owner_after);
    if (!sample(state.final) || !same_reserves()) return false;
    if (!check(
            state.held == 0 && state.bulk_allocations == 0 && state.order0_allocations == 0 && state.tail_pages == 0 && state.cleanup_valid,
            "all exact held refs returned"))
        return false;
    if (!check(state.owner_after.owners[OWNER_INDEX].pages == state.owner_before.owners[OWNER_INDEX].pages &&
                   state.owner_after.owners[OWNER_INDEX].objects == state.owner_before.owners[OWNER_INDEX].objects &&
                   state.owner_after.identity_mismatch_pages == 0,
               "actual SELFTEST owner baseline restored"))
        return false;
    for (size_t node = 0; node < state.baseline.node_count; ++node)
        if (!check(state.final.nodes[node].free_inclusive == state.baseline.nodes[node].free_inclusive,
                   "actual per-node free-inclusive baseline restored"))
            return false;
    auto const CLOSED =
        check(state.final.global.free_inclusive == state.baseline.global.free_inclusive, "actual global free-inclusive baseline restored");
    return CLOSED;
}
auto run() -> bool {
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (!check(current != nullptr && current->type == task::TaskType::DAEMON && sched::interrupts_enabled() && sched::preempt_count() == 0,
               "genuine ordinary scheduled DAEMON"))
        return false;
    state.cpu = mod::cpu::current_cpu();
    auto const* machine = topology::boot_topology();
    if (!check(machine != nullptr && machine->node_count == 2 && machine->cpu_count == 8 && state.cpu < machine->cpu_count,
               "actual two-domain eight-CPU pressure geometry"))
        return false;
    state.foreign = machine->cpu_nodes[state.cpu];
    state.selected = state.foreign == 0 ? 1 : 0;
    phys::get_page_cache_stats_snapshot(state.cache_info);
    if (!check(state.foreign < machine->node_count && machine->nodes[state.selected].usable_bytes != 0 && state.cache_info.enabled != 0,
               "foreign CPU/selected RAM/cache real fixture"))
        return false;
    uint64_t const BOOT_DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), 45'000'000);
    for (;;) {
        auto const SETTLED = lifecycle60::ntp_boot_settlement();
        if (!check(SETTLED != lifecycle60::GateResult::REFUSE, "real NTP boot settlement valid")) return false;
        if (SETTLED == lifecycle60::GateResult::ACCEPT) break;
        if (!time_left(BOOT_DEADLINE)) return false;
        sched::kern_sleep_us(1000);
    }
    if (!sample(state.baseline) ||
        !check(state.baseline.nodes[state.selected].usable <= MAX_HELD && state.baseline.nodes[state.foreign].buddy_free >= 65536,
               "bounded selected hold and healthy foreign node"))
        return false;
    // IMMEDIATE consumption of this exact sample's canonical workspace views.
    state.phase = "frame-geometry-prerequisite";
    if (!prepare_frame_geometry(*machine)) return false;
    if (!check(reclaim::selftest_observe_worker(state.worker) && state.worker.exact_daemon && state.worker.refs == 2,
               "ready boot-held worker"))
        return false;
    state.worker_pid = state.worker.pid;
    auto* worker = sched::find_task_by_pid_safe(state.worker_pid);
    if (!check(worker != nullptr, "actual retained worker PID lookup")) return false;
    struct Release {
        task::Task* p;
        ~Release() { p->release(); }
    } release{worker};
    state.worker_refs = worker->ref_count.load(std::memory_order_acquire);
    if (!check(state.worker_refs == 3, "exact boot2 plus diagnostic observer1")) return false;
    {
        PrivateRoot warm_root;
        if (!warm_root.init() || !warm_root.acquire()) return false;
    }
    struct CacheCleanup {
        bool active{true};
        ~CacheCleanup() {
            if (active) state.cleanup_valid &= drain((uint64_t{1} << state.selected) | (uint64_t{1} << state.foreign));
        }
    } cache_cleanup;
    PreparedSeeds seeds;
    if (!prepare_cache_seeds(*worker, seeds)) return false;
    phys::get_physical_balance_snapshot(state.root_owner_before);
    virt::get_user_root_control_stats_snapshot(state.root_control_before);
    PrivateRoot root;
    if (!root.init()) return false;
    state.phase = "healthy-wake-contexts";
    for (unsigned kind = 0; kind < 5; ++kind)
        if (!one_wake(*worker, root, kind)) return false;
    if (!pressure(*worker, seeds)) return false;
    root.clear();
    phys::get_physical_balance_snapshot(state.root_owner_after);
    virt::get_user_root_control_stats_snapshot(state.root_control_after);
    constexpr auto PRIVATE = static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING);
    if (!check(state.root_owner_after.owners[PRIVATE].pages == state.root_owner_before.owners[PRIVATE].pages &&
                   state.root_owner_after.owners[PRIVATE].objects == state.root_owner_before.owners[PRIVATE].objects &&
                   state.root_control_after.active_roots == state.root_control_before.active_roots &&
                   state.root_control_after.control_pages == state.root_control_before.control_pages,
               "actual private mapping owner/root baseline restored"))
        return false;
    for (auto owner : {mm::PhysicalPageOwner::PAGE_TABLE, mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE}) {
        auto const index = static_cast<size_t>(owner);
        if (!check(state.root_owner_after.owners[index].pages == state.root_owner_before.owners[index].pages &&
                       state.root_owner_after.owners[index].objects == state.root_owner_before.owners[index].objects,
                   "actual root page-table owner baseline restored"))
            return false;
    }
    auto const CLOSED = time_left(state.whole_deadline) &&
                        check(state.cleanup_valid && worker->ref_count.load(std::memory_order_acquire) == state.worker_refs,
                              "root/readers returned and exact worker observer holds preserved");
    if (CLOSED) cache_cleanup.active = false;
    return CLOSED;
}
void entry() {
    prepare(state);
    state.phase = "start";
    state.selected = state.foreign = numa::INVALID_NODE;
    state.cleanup_valid = true;
    state.whole_deadline = sched::saturating_deadline_us(mod::time::get_us(), WHOLE_US);
    bool success = false;
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        success = run();
    }
    if (!success) {
        report_seed_diagnostics();
        report_closure_diagnostics();
        log::error(
            "NUMA_PRESSURE FAIL phase=%s reason=%s held=%llu cleanup=%u checks=%llu limit_captured=%u limit_bits=%u "
            "next_pages=%llu held_before_cleanup=%llu bulk_allocations=%llu order0_allocations=%llu tail_pages=%llu "
            "held_highwater=%llu bulk_highwater=%llu order0_highwater=%llu tail_pages_highwater=%llu "
            "window_first=%llx window_end=%llx page_capacity=%llu sample_valid=%u buddy=%llu cached=%llu usable=%llu",
            state.phase, state.failure != nullptr ? state.failure : "unknown", u(state.held), state.cleanup_valid ? 1U : 0U,
            u(state.assertions), state.limit_captured ? 1U : 0U, state.limit_reasons, u(state.limit_next_pages), u(state.limit_held),
            u(state.limit_bulk), u(state.limit_order0), u(state.limit_tail_pages), u(state.held_highwater), u(state.bulk_highwater),
            u(state.order0_highwater), u(state.tail_pages_highwater), u(state.frames.first), u(state.frames.end), u(MAX_HELD),
            state.limit_sample_valid ? 1U : 0U, u(state.limit_buddy), u(state.limit_cached), u(state.limit_usable));
        mod::dbg::panic_handler("scheduled NUMA pressure fixture failed");
    }
    log::info(
        "NUMA_PRESSURE PASS wakes=%llu selected=%u cpu_node=%u released=%llu held=0 checks=%llu separate_wait_observations=1 public_cpl3=0",
        u(state.wake_cases), static_cast<unsigned>(state.selected), static_cast<unsigned>(state.foreign), u(state.released),
        u(state.assertions));
    mod::power::selftest_shutdown();
}
}  // namespace

auto start_numa_pressure_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* machine = topology::boot_topology();
    if (machine == nullptr || machine->node_count != 2 || machine->cpu_count != 8) {
        started.store(false, std::memory_order_release);
        return false;
    }
    uint64_t selected_cpu = machine->cpu_count;
    for (uint64_t cpu = 0; cpu < machine->cpu_count; ++cpu)
        if (machine->cpu_nodes[cpu] < 2 && machine->nodes[machine->cpu_nodes[cpu] == 0 ? 1 : 0].usable_bytes != 0) {
            selected_cpu = cpu;
            break;
        }
    if (selected_cpu == machine->cpu_count) {
        started.store(false, std::memory_order_release);
        return false;
    }
    auto* worker = task::Task::create_kernel_thread("numa_pressure_runtime_selftest", entry);
    if (worker == nullptr) {
        started.store(false, std::memory_order_release);
        return false;
    }
    if (!sched::post_task_pinned_cpu(selected_cpu, worker)) {
        task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
#endif
