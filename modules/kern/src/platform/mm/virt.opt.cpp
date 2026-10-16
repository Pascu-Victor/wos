#include "virt.hpp"

#include <platform/mm/managed_huge.hpp>

#ifdef WOS_SELFTEST
#include <platform/mm/lifecycle_census.hpp>
#endif

#include <abi/callnums/vmem.h>
#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <platform/acpi/apic/apic.hpp>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_root_reserve.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <span>
#include <string_view>
#include <util/smallvec.hpp>
#include <utility>

#include "platform/sched/task.hpp"
#include "platform/sys/spinlock.hpp"
#include "platform/sys/syscall_preparation.hpp"

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "platform/asm/cpu.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/coredump.hpp"
#include "platform/loader/runtime_images.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/reclaim.hpp"
#include "platform/mm/reclaim_policy.hpp"
#include "platform/mm/swap.hpp"
#include "platform/sched/threading.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "util/hcf.hpp"
#include "vfs/fs/tmpfs.hpp"
#include "vfs/vfs.hpp"

#ifdef WOS_SELFTEST
#include "platform/sys/usercopy.hpp"
#endif

extern "C" {
extern char __kernel_image_start[];                   // NOLINT(readability-identifier-naming)
extern char __kernel_text_start[];                    // NOLINT(readability-identifier-naming)
extern char __kernel_text_end[];                      // NOLINT(readability-identifier-naming)
extern char __kernel_rodata_start[];                  // NOLINT(readability-identifier-naming)
extern char __kernel_rodata_end[];                    // NOLINT(readability-identifier-naming)
extern char __kernel_data_start[];                    // NOLINT(readability-identifier-naming)
extern char __kernel_data_end[];                      // NOLINT(readability-identifier-naming)
extern char __kernel_bss_start[];                     // NOLINT(readability-identifier-naming)
extern char __kernel_bss_end[];                       // NOLINT(readability-identifier-naming)
extern char __kernel_emergency_stack_region_start[];  // NOLINT(readability-identifier-naming)
extern char __kernel_emergency_stack_region_end[];    // NOLINT(readability-identifier-naming)
extern char __stack_chk_emergency_guard_low[];        // NOLINT(readability-identifier-naming)
extern char __stack_chk_emergency_stack[];            // NOLINT(readability-identifier-naming)
extern char __stack_chk_emergency_stack_end[];        // NOLINT(readability-identifier-naming)
extern char __stack_chk_emergency_guard_high_end[];   // NOLINT(readability-identifier-naming)
extern char __kernel_end[];                           // NOLINT(readability-identifier-naming)
}

namespace ker::mod::mm::virt {

namespace {
using log = ker::mod::dbg::logger<"virt">;

constexpr uint64_t LARGE_PAGE_2M_BYTES = paging::PAGE_SIZE * paging::PAGE_TABLE_ENTRIES;
constexpr uint64_t LARGE_PAGE_1G_BYTES = LARGE_PAGE_2M_BYTES * paging::PAGE_TABLE_ENTRIES;
constexpr uint64_t PTE_FRAME_MASK = 0x000FFFFFFFFFF000ULL;

// All objects passed from this excluded TU into the instrumented allocator
// start their lifetime only after their exact storage is addressable. In
// particular, default construction and hidden sret writes can be checked at O0.
struct DemandContext {
    numa::AllocationContext allocation{};
    DemandOrigin origin{DemandOrigin::SoftwareEnsure};
    uint64_t cpu{};
    bool hardware_user{};
    bool local_has_ram{};
    bool foreign_target{};
#ifdef WOS_SELFTEST
    uint64_t observer_cookie{};
#endif
};
static_assert(alignof(DemandContext) >= 8 && sizeof(DemandContext) % 8 == 0);
static_assert(alignof(numa::AllocationResult) >= 8 && sizeof(numa::AllocationResult) % 8 == 0);

class DemandContextStorage {
   public:
    DemandContextStorage() {
#ifdef WOS_KASAN
        if (kasan::is_enabled()) kasan::unpoison_range(storage_, sizeof(storage_));
#endif
        value_ = ::new (static_cast<void*>(storage_)) DemandContext{};
    }
    ~DemandContextStorage() { value_->~DemandContext(); }
    DemandContextStorage(const DemandContextStorage&) = delete;
    auto operator=(const DemandContextStorage&) -> DemandContextStorage& = delete;
    auto get() -> DemandContext& { return *value_; }

   private:
    alignas(DemandContext) uint8_t storage_[sizeof(DemandContext)];
    DemandContext* value_;
};

class DemandResultStorage {
   public:
    DemandResultStorage() {
#ifdef WOS_KASAN
        if (kasan::is_enabled()) kasan::unpoison_range(storage_, sizeof(storage_));
#endif
        value_ = ::new (static_cast<void*>(storage_)) numa::AllocationResult{};
    }
    ~DemandResultStorage() { value_->~AllocationResult(); }
    DemandResultStorage(const DemandResultStorage&) = delete;
    auto operator=(const DemandResultStorage&) -> DemandResultStorage& = delete;
    auto get() -> numa::AllocationResult& { return *value_; }

   private:
    alignas(numa::AllocationResult) uint8_t storage_[sizeof(numa::AllocationResult)];
    numa::AllocationResult* value_;
};

struct DemandCounters {
    std::atomic<uint64_t> candidates{}, allocation_failures{}, discarded{}, promotions{}, installed{};
    std::atomic<uint64_t> anonymous{}, zero_cow{}, private_cow{}, software{}, hardware_user{}, hardware_kernel{};
    std::atomic<uint64_t> eligible{}, local{}, locality_misses{}, fallbacks{};
    std::atomic<bool> overflow{};
};
DemandCounters demand_counters{};
void demand_increment(std::atomic<uint64_t>& counter) {
    uint64_t value = counter.load(std::memory_order_relaxed);
    while (value != UINT64_MAX) {
        if (counter.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) return;
    }
    demand_counters.overflow.store(true, std::memory_order_relaxed);
}

auto classify_private_demand(DemandOrigin origin, bool hardware_user, DemandBacking backing, const numa::Policy& policy, bool local_has_ram)
    -> DemandExclusion {
    if (origin == DemandOrigin::SoftwareEnsure) return DemandExclusion::Software;
    if (!hardware_user) return DemandExclusion::KernelFault;
    if (backing == DemandBacking::PrivateCowCopy) return DemandExclusion::PrivateCopy;
    if (policy.mode != numa::Mode::LOCAL || policy.nodes != 0) return DemandExclusion::ExplicitPolicy;
    return local_has_ram ? DemandExclusion::Eligible : DemandExclusion::NoLocalRam;
}

#ifdef WOS_SELFTEST
struct DemandObserverPlan {
    std::atomic<uint64_t> word{};
    std::atomic<sched::task::Task*> subject{};
    std::atomic<PageTable*> root{};
    std::atomic<uint64_t> root_serial{};
    std::atomic<uint64_t> address{};
    std::atomic<uint64_t> cpu{};
    std::atomic<SelftestDemandObservation*> output{};
    std::atomic<SelftestDemandAction> action{};
    std::atomic<numa::NodeMask> replacement_nodes{};
    std::atomic<numa::Mode> replacement_mode{};
};
DemandObserverPlan demand_observer{};

auto demand_observation(const DemandContext& context) -> SelftestDemandObservation* {
    if (context.observer_cookie == 0 || demand_observer.word.load(std::memory_order_acquire) != context.observer_cookie ||
        cpu::current_cpu() != context.cpu)
        return nullptr;
    return demand_observer.output.load(std::memory_order_relaxed);
}
#endif

auto capture_demand_context(sched::task::Task* target, uint64_t address, DemandOrigin origin, bool hardware_user, DemandContext& output)
    -> bool {
    if (target == nullptr || target->pagemap == nullptr || address >= 0x0000800000000000ULL) return false;
    {
        sched::PreemptGuard const PREEMPT;
        output.allocation.policy = target->memory_policy_snapshot();
        output.cpu = cpu::current_cpu();
        auto const* topology = acpi::numa::boot_topology();
        if (topology == nullptr || output.cpu >= topology->cpu_count || output.cpu >= topology->cpu_nodes.size()) return false;
        output.allocation.local_node = topology->cpu_nodes[output.cpu];
        if (output.allocation.local_node >= topology->node_count) return false;
        output.local_has_ram = topology->nodes[output.allocation.local_node].usable_bytes != 0;
    }  // Preserve the allocator's real can_wait_for_reclaim predicate.
    output.allocation.virtual_page_index = address / paging::PAGE_SIZE;
    output.origin = origin;
    output.hardware_user = hardware_user;
    output.foreign_target = sched::can_query_current_task() && sched::get_current_task() != target;
#ifdef WOS_SELFTEST
    uint64_t const WORD = demand_observer.word.load(std::memory_order_acquire);
    uint64_t observer_root_serial = 0;
    if ((WORD & 3U) == 1U && demand_observer.subject.load(std::memory_order_relaxed) == target &&
        demand_observer.root.load(std::memory_order_relaxed) == target->pagemap &&
        capture_user_root_incarnation(target->pagemap, observer_root_serial) &&
        demand_observer.root_serial.load(std::memory_order_relaxed) == observer_root_serial &&
        demand_observer.address.load(std::memory_order_relaxed) == (address & ~(paging::PAGE_SIZE - 1)) &&
        demand_observer.cpu.load(std::memory_order_relaxed) == output.cpu) {
        output.observer_cookie = WORD;
        auto* observation = demand_observer.output.load(std::memory_order_relaxed);
        observation->captured_policy = output.allocation.policy;
        observation->local_node = output.allocation.local_node;
        observation->origin = origin;
        observation->foreign_target = output.foreign_target;
        ++observation->captures;
        if (demand_observer.action.load(std::memory_order_relaxed) == SelftestDemandAction::ChangePolicyAfterCapture &&
            !observation->action_consumed) {
            observation->action_consumed =
                target->selftest_set_memory_policy({demand_observer.replacement_nodes.load(std::memory_order_relaxed),
                                                    demand_observer.replacement_mode.load(std::memory_order_relaxed)});
        }
    }
#endif
    return true;
}

void record_demand_candidate(const DemandContext& context, bool failed) {
    demand_increment(demand_counters.candidates);
    if (failed) demand_increment(demand_counters.allocation_failures);
#ifdef WOS_SELFTEST
    if (auto* observation = demand_observation(context); observation != nullptr) {
        ++observation->candidates;
        if (failed) ++observation->allocation_failures;
    }
#else
    (void)context;
#endif
}

// Call only after the final lock interval, with frozen classification values.
void record_demand_result(const DemandContext& context, DemandBacking backing, numa::NodeId requested, numa::NodeId actual, bool installed,
                          bool promoted, bool observed) {
    if (promoted)
        demand_increment(demand_counters.promotions);
    else if (!installed)
        demand_increment(demand_counters.discarded);
    else {
        demand_increment(demand_counters.installed);
        if (actual != requested) demand_increment(demand_counters.fallbacks);
        demand_increment(backing == DemandBacking::Anonymous ? demand_counters.anonymous
                         : backing == DemandBacking::ZeroCow ? demand_counters.zero_cow
                                                             : demand_counters.private_cow);
        demand_increment(context.origin == DemandOrigin::SoftwareEnsure ? demand_counters.software
                         : context.hardware_user                        ? demand_counters.hardware_user
                                                                        : demand_counters.hardware_kernel);
        if (classify_private_demand(context.origin, context.hardware_user, backing, context.allocation.policy, context.local_has_ram) ==
            DemandExclusion::Eligible) {
            demand_increment(demand_counters.eligible);
            demand_increment(actual == context.allocation.local_node ? demand_counters.local : demand_counters.locality_misses);
        }
    }
#ifdef WOS_SELFTEST
    if (observed) {
        auto* observation = demand_observer.output.load(std::memory_order_relaxed);
        observation->backing = backing;
        observation->requested = requested;
        observation->actual = actual;
        observation->used_fallback = installed && actual != requested;
        observation->exclusion =
            classify_private_demand(context.origin, context.hardware_user, backing, context.allocation.policy, context.local_has_ram);
        if (promoted)
            ++observation->promotions;
        else if (installed)
            ++observation->installed;
        else
            ++observation->discarded;
    }
#else
    (void)requested;
    (void)observed;
#endif
}

auto demand_is_observed(const DemandContext& context) -> bool {
#ifdef WOS_SELFTEST
    return demand_observation(context) != nullptr;
#else
    (void)context;
    return false;
#endif
}

sys::Spinlock cow_pte_lock;
// Serializes leaf publication/removal with usercopy frame pin acquisition.
// Never hold this across allocation, fault resolution, memcpy, or shootdown.
#ifdef WOS_SELFTEST
ker::mod::mm::lifecycle_census::Epoch selftest_root_epoch{};
ker::mod::mm::lifecycle_census::OwnerLock user_mapping_pin_lock{selftest_root_epoch};
#else
sys::Spinlock user_mapping_pin_lock;
#endif
file_mapping::FileMappingIndex file_mapping_index;
constinit huge::RegistryCore managed_huge_registry{};

// Exact root associations use the same lock as pinned mapping snapshots. Only
// copied serials escape; queued futexes never retain a control or root reader.
constexpr size_t USER_ROOT_CONTROL_BUCKET_COUNT = 256;
constexpr size_t USER_ROOT_CONTROLS_PER_PAGE = 96;
struct UserRootControlChunk;
struct UserRootControl {
    PageTable* root{};
    uint64_t serial{};
    UserRootControl* hash_next{};
    UserRootControl* free_next{};
    UserRootControlChunk* chunk{};
};
struct UserRootControlChunk {
    UserRootControlChunk* available_next{};
    UserRootControl* free_list{};
    size_t active_count{};
    std::array<UserRootControl, USER_ROOT_CONTROLS_PER_PAGE> controls{};
};
static_assert(sizeof(UserRootControlChunk) <= paging::PAGE_SIZE);
static_assert((USER_ROOT_CONTROL_BUCKET_COUNT & (USER_ROOT_CONTROL_BUCKET_COUNT - 1)) == 0);
std::array<UserRootControl*, USER_ROOT_CONTROL_BUCKET_COUNT> user_root_control_buckets{};
UserRootControlChunk* user_root_control_available{};
uint64_t user_root_control_last_serial{};
uint64_t user_root_control_active_roots{};
uint64_t user_root_control_pages{};

enum class AnonymousSwapRuntimeState : uint8_t {
    PAGEOUT,
    SWAPPED,
    PAGEIN,
    FAILED,
};

struct AnonymousSwapObject {
    swap::SwapSlot slot{};
    uint32_t mapping_refs{};  // Serialized by cow_pte_lock.
};

struct AnonymousSwapMapping {
    PageTable* pagemap{};
    vaddr_t vaddr{};
    AnonymousSwapRuntimeState state{AnonymousSwapRuntimeState::PAGEOUT};
    AnonymousSwapObject* object{};
    void* frame{};  // Extra pageout pin or unpublished pagein destination.
    uint64_t original_raw{};
    uint64_t frozen_raw{};
    uint64_t generation{};
    int error{};
    bool pagein_queued{};
    bool cancelled{};
    // The first fault holds the pagemap read-side lifetime gate from queue
    // publication through worker completion.  Teardown/fork therefore joins
    // queued work as well as I/O already in progress.
    size_t pagein_access_gate{static_cast<size_t>(-1)};
    sched::task::Task* waiters{};
    AnonymousSwapMapping* next{};
};

struct AnonymousSwapAtomicStats {
    std::atomic<uint64_t> resident_candidates{};
    std::atomic<uint64_t> swapped_pages{};
    std::atomic<uint64_t> pageout_inflight{};
    std::atomic<uint64_t> pagein_inflight{};
    std::atomic<uint64_t> pageout_attempts{};
    std::atomic<uint64_t> pageout_successes{};
    std::atomic<uint64_t> pageout_failures{};
    std::atomic<uint64_t> pagein_attempts{};
    std::atomic<uint64_t> pagein_successes{};
    std::atomic<uint64_t> pagein_failures{};
    std::atomic<uint64_t> pagein_corruption{};
    std::atomic<uint64_t> pageout_latency_us{};
    std::atomic<uint64_t> pageout_latency_max_us{};
    std::atomic<uint64_t> pagein_latency_us{};
    std::atomic<uint64_t> pagein_latency_max_us{};
};

constexpr size_t ANONYMOUS_PAGEIN_QUEUE_CAPACITY = 256;
constexpr size_t ANONYMOUS_RECLAIM_MAX_RANGES = 64;
sys::Spinlock anonymous_pagein_queue_lock;
std::array<AnonymousSwapMapping*, ANONYMOUS_PAGEIN_QUEUE_CAPACITY> anonymous_pagein_queue{};
size_t anonymous_pagein_queue_head{};
size_t anonymous_pagein_queue_tail{};
size_t anonymous_pagein_queue_count{};
AnonymousSwapMapping* anonymous_swap_mappings{};  // Serialized by cow_pte_lock.
AnonymousSwapAtomicStats anonymous_swap_stats{};
std::atomic<uint64_t> anonymous_swap_generation{1};
std::atomic<uint64_t> anonymous_reclaim_cursor{};
std::atomic<bool> anonymous_swap_shrinker_registered{};
std::atomic<bool> anonymous_swap_worker_started{};
std::atomic<sched::task::Task*> anonymous_swap_worker_task{};

auto find_user_root_control_locked(PageTable* root) -> UserRootControl*;
auto pte_raw(const paging::PageTableEntry& entry) -> uint64_t;
auto pte_from_raw(uint64_t raw) -> paging::PageTableEntry;
auto is_reserved_leaf(const PageTableEntry& entry) -> bool;
auto leaf_entry(PageTable* root, vaddr_t vaddr) -> PageTableEntry*;
void owned_frame_track_fresh_normal_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void owned_frame_untrack_leaf(PageTable* pagemap, vaddr_t vaddr, const PageTableEntry& entry);
void flush_pagemap_after_update(PageTable* pagemap, vaddr_t vaddr, bool reload_cr3);

// Page-table roots are shared by user threads and CLONE_VM tasks, so a Task's
// local close flag alone cannot protect the tree from last-publisher teardown.
// A striped read/exclusive gate keeps the bounded usercopy hot path allocation
// free. Safe faulting callers may wait for an unrelated colliding teardown;
// mapped-only IRQ/scheduler callers fail closed instead.
constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT = 4096;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE = static_cast<size_t>(-1);
static_assert((USER_PAGEMAP_ACCESS_GATE_COUNT & (USER_PAGEMAP_ACCESS_GATE_COUNT - 1U)) == 0);
struct alignas(64) UserPagemapAccessGate {
    std::atomic<uint32_t> readers{0};
    std::atomic<bool> teardown{false};
};
std::array<UserPagemapAccessGate, USER_PAGEMAP_ACCESS_GATE_COUNT> user_pagemap_access_gates{};

[[nodiscard]] auto user_pagemap_access_gate_index(PageTable* pagemap) -> size_t {
    return (reinterpret_cast<uintptr_t>(pagemap) >> 12U) & (USER_PAGEMAP_ACCESS_GATE_COUNT - 1U);
}

void service_user_pagemap_gate_wait(uint64_t& service_spins) {
    // A pinned reader may be resolving COW and waiting for our TLB ack while
    // fork drains readers. With IF masked neither the reader nor exclusive
    // waiter can rely on an IPI interrupt to break that dependency cycle.
    ++service_spins;
    if ((service_spins & 0xFFU) == 0U && !sched::interrupts_enabled()) {
        service_pending_tlb_shootdowns();
    }
}

[[nodiscard]] auto acquire_user_pagemap_access(PageTable* pagemap, bool wait_for_teardown, size_t& gate_index) -> bool {
    gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (pagemap == nullptr) {
        return false;
    }

    size_t const INDEX = user_pagemap_access_gate_index(pagemap);
    auto& gate = user_pagemap_access_gates.at(INDEX);
    uint64_t service_spins = 0;
    for (;;) {
        while (gate.teardown.load(std::memory_order_acquire)) {
            if (!wait_for_teardown) {
                return false;
            }
            service_user_pagemap_gate_wait(service_spins);
            asm volatile("pause" ::: "memory");
        }

        uint32_t readers = gate.readers.load(std::memory_order_acquire);
        while (readers != UINT32_MAX) {
            if (gate.readers.compare_exchange_weak(readers, readers + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        if (readers == UINT32_MAX) {
            return false;
        }

        if (!gate.teardown.load(std::memory_order_acquire)) {
            gate_index = INDEX;
            return true;
        }
        gate.readers.fetch_sub(1, std::memory_order_acq_rel);
        if (!wait_for_teardown) {
            return false;
        }
    }
}

void release_user_pagemap_access(size_t gate_index) {
    if (gate_index != USER_PAGEMAP_ACCESS_GATE_NONE) {
        user_pagemap_access_gates.at(gate_index).readers.fetch_sub(1, std::memory_order_acq_rel);
    }
}

[[nodiscard]] auto begin_user_pagemap_exclusive(PageTable* pagemap) -> size_t {
    size_t const INDEX = user_pagemap_access_gate_index(pagemap);
    auto& gate = user_pagemap_access_gates.at(INDEX);
    bool expected = false;
    uint64_t service_spins = 0;
    while (!gate.teardown.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        expected = false;
        service_user_pagemap_gate_wait(service_spins);
        asm volatile("pause" ::: "memory");
    }
    while (gate.readers.load(std::memory_order_acquire) != 0) {
        service_user_pagemap_gate_wait(service_spins);
        asm volatile("pause" ::: "memory");
    }
    return INDEX;
}

[[nodiscard]] auto try_begin_user_pagemap_exclusive(PageTable* pagemap) -> size_t {
    size_t const INDEX = user_pagemap_access_gate_index(pagemap);
    auto& gate = user_pagemap_access_gates.at(INDEX);
    bool expected = false;
    if (!gate.teardown.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire))
        return USER_PAGEMAP_ACCESS_GATE_NONE;
    if (gate.readers.load(std::memory_order_acquire) != 0) {
        // Only release the exclusive bit acquired by this exact attempt. The
        // single GC worker must remain free to advance a colliding held walk.
        gate.teardown.store(false, std::memory_order_release);
        return USER_PAGEMAP_ACCESS_GATE_NONE;
    }
    return INDEX;
}

void end_user_pagemap_exclusive(size_t gate_index) {
    user_pagemap_access_gates.at(gate_index).teardown.store(false, std::memory_order_release);
}

class UserPagemapExclusiveScope {
   public:
    explicit UserPagemapExclusiveScope(PageTable* pagemap) : gate_index(begin_user_pagemap_exclusive(pagemap)) {}
    ~UserPagemapExclusiveScope() { finish(); }
    void finish() {
        if (gate_index == USER_PAGEMAP_ACCESS_GATE_NONE) return;
        end_user_pagemap_exclusive(gate_index);
        gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    }

    UserPagemapExclusiveScope(const UserPagemapExclusiveScope&) = delete;
    UserPagemapExclusiveScope(UserPagemapExclusiveScope&&) = delete;
    auto operator=(const UserPagemapExclusiveScope&) -> UserPagemapExclusiveScope& = delete;
    auto operator=(UserPagemapExclusiveScope&&) -> UserPagemapExclusiveScope& = delete;

   private:
    size_t gate_index;
};

void invalidate_local_tlb_if_current(PageTable* pagemap, vaddr_t vaddr, bool reload_cr3);

// Buffer-cache snapshots need contiguous virtual bytes, not contiguous
// physical frames. Keep this arena within one kernel-half PML4 slot and back
// it with order-0 pages. Freed mappings form a reusable warm pool; pressure
// paths purge that pool only from IRQ-enabled contexts. Runtime page-table
// allocation is forbidden: init pre-creates every path the active prefix uses.
constexpr vaddr_t KERNEL_VMAP_BASE = 0xffffa00000000000ULL;
constexpr uint64_t KERNEL_VMAP_MAX_BYTES = uint64_t{16} * 1024 * 1024 * 1024;
constexpr size_t KERNEL_VMAP_MAX_PAGES = KERNEL_VMAP_MAX_BYTES / paging::PAGE_SIZE;
constexpr size_t KERNEL_VMAP_BITMAP_WORDS = (KERNEL_VMAP_MAX_PAGES + 63) / 64;
constexpr uint64_t KERNEL_VMAP_MIN_BYTES = uint64_t{256} * 1024 * 1024;
constexpr size_t KERNEL_VMAP_WARM_POOL_MAX_PAGES = (size_t{256} * 1024 * 1024) / paging::PAGE_SIZE;

sys::Spinlock kernel_vmap_lock;
std::array<uint64_t, KERNEL_VMAP_BITMAP_WORDS> kernel_vmap_bitmap{};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<uint64_t, KERNEL_VMAP_BITMAP_WORDS> kernel_vmap_pending_bitmap{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> kernel_vmap_draining{false};                                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t kernel_vmap_active_pages = 0;                                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t kernel_vmap_next_page = 0;                                             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t kernel_vmap_pending_first_page = KERNEL_VMAP_MAX_PAGES;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t kernel_vmap_pending_pages = 0;                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool kernel_vmap_initialized = false;                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// The arena is fixed and fully mapped before task/AP publication; runtime
// allocation only changes pool ownership. Its address is deliberately outside
// KASan's reserved shadow interval so a real stack-guard fault cannot be
// mistaken for a demand fault on shadow memory.
constexpr vaddr_t GUARDED_KERNEL_STACK_ARENA_BASE = ker::mod::mm::GUARDED_KERNEL_STACK_ARENA_BASE;
constexpr uint64_t GUARDED_KERNEL_STACK_STRIDE = ker::mod::mm::KERNEL_STACK_SLOT_STRIDE;
static_assert(ker::mod::mm::KERNEL_STACK_GUARD_SIZE == paging::PAGE_SIZE);
#ifdef WOS_KASAN
static_assert(GUARDED_KERNEL_STACK_ARENA_BASE >= kasan::SHADOW_REGION_END);
#endif
size_t guarded_kernel_stack_capacity{};   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool guarded_kernel_stack_initialized{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto kernel_vmap_bit_is_set(size_t page) -> bool { return (kernel_vmap_bitmap.at(page / 64) & (uint64_t{1} << (page % 64))) != 0; }

void kernel_vmap_bit_set(size_t page) { kernel_vmap_bitmap.at(page / 64) |= uint64_t{1} << (page % 64); }

void kernel_vmap_bit_clear(size_t page) { kernel_vmap_bitmap.at(page / 64) &= ~(uint64_t{1} << (page % 64)); }

auto kernel_vmap_pending_bit_is_set(size_t page) -> bool {
    return (kernel_vmap_pending_bitmap.at(page / 64) & (uint64_t{1} << (page % 64))) != 0;
}

void kernel_vmap_pending_bit_set(size_t page) { kernel_vmap_pending_bitmap.at(page / 64) |= uint64_t{1} << (page % 64); }

void kernel_vmap_pending_bit_clear(size_t page) { kernel_vmap_pending_bitmap.at(page / 64) &= ~(uint64_t{1} << (page % 64)); }

auto find_kernel_vmap_span_locked(size_t begin, size_t end, size_t page_count, size_t& first_page) -> bool {
    size_t run = 0;
    for (size_t page = begin; page < end; ++page) {
        if (kernel_vmap_bit_is_set(page)) {
            run = 0;
            continue;
        }
        ++run;
        if (run == page_count) {
            first_page = page + 1 - page_count;
            return true;
        }
    }
    return false;
}

auto reserve_kernel_vmap_span(size_t page_count, size_t& first_page) -> bool {
    if (!kernel_vmap_initialized || page_count == 0 || page_count > kernel_vmap_active_pages) {
        return false;
    }

    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    bool found = find_kernel_vmap_span_locked(kernel_vmap_next_page, kernel_vmap_active_pages, page_count, first_page);
    if (!found && kernel_vmap_next_page != 0) {
        found = find_kernel_vmap_span_locked(0, kernel_vmap_next_page, page_count, first_page);
    }
    if (found) {
        for (size_t page = first_page; page < first_page + page_count; ++page) {
            if (kernel_vmap_pending_bit_is_set(page)) {
                --kernel_vmap_pending_pages;
            }
            kernel_vmap_bit_set(page);
            kernel_vmap_pending_bit_clear(page);
        }
        if (kernel_vmap_pending_first_page >= first_page && kernel_vmap_pending_first_page < first_page + page_count) {
            while (kernel_vmap_pending_first_page < kernel_vmap_active_pages &&
                   !kernel_vmap_pending_bit_is_set(kernel_vmap_pending_first_page)) {
                ++kernel_vmap_pending_first_page;
            }
        }
        kernel_vmap_next_page = first_page + page_count;
        if (kernel_vmap_next_page >= kernel_vmap_active_pages) {
            kernel_vmap_next_page = 0;
        }
    }
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return found;
}

auto release_kernel_vmap_span(size_t first_page, size_t page_count, bool require_pending = false) -> bool {
    if (page_count == 0 || first_page >= kernel_vmap_active_pages || page_count > kernel_vmap_active_pages - first_page) {
        return false;
    }

    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    for (size_t page = first_page; page < first_page + page_count; ++page) {
        if (!kernel_vmap_bit_is_set(page) || (require_pending && !kernel_vmap_pending_bit_is_set(page))) {
            kernel_vmap_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (size_t page = first_page; page < first_page + page_count; ++page) {
        if (kernel_vmap_pending_bit_is_set(page)) {
            --kernel_vmap_pending_pages;
        }
        kernel_vmap_bit_clear(page);
        kernel_vmap_pending_bit_clear(page);
    }
    if (kernel_vmap_pending_first_page >= first_page && kernel_vmap_pending_first_page < first_page + page_count) {
        kernel_vmap_pending_first_page = first_page + page_count;
        while (kernel_vmap_pending_first_page < kernel_vmap_active_pages &&
               !kernel_vmap_pending_bit_is_set(kernel_vmap_pending_first_page)) {
            ++kernel_vmap_pending_first_page;
        }
    }
    kernel_vmap_next_page = std::min(first_page, kernel_vmap_next_page);
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto queue_kernel_vmap_free(size_t first_page, size_t page_count) -> bool {
    if (page_count == 0 || first_page >= kernel_vmap_active_pages || page_count > kernel_vmap_active_pages - first_page) {
        return false;
    }

    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    for (size_t page = first_page; page < first_page + page_count; ++page) {
        if (!kernel_vmap_bit_is_set(page) || kernel_vmap_pending_bit_is_set(page)) {
            kernel_vmap_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (size_t page = first_page; page < first_page + page_count; ++page) {
        kernel_vmap_pending_bit_set(page);
        kernel_vmap_bit_clear(page);
    }
    kernel_vmap_pending_pages += page_count;
    kernel_vmap_pending_first_page = std::min(kernel_vmap_pending_first_page, first_page);
    kernel_vmap_next_page = std::min(kernel_vmap_next_page, first_page);
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto find_pending_kernel_vmap_run(size_t max_pages, size_t& first_page, size_t& page_count) -> bool {
    if (max_pages == 0) {
        return false;
    }
    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    size_t page = kernel_vmap_pending_first_page;
    while (page < kernel_vmap_active_pages && !kernel_vmap_pending_bit_is_set(page)) {
        ++page;
    }
    if (page >= kernel_vmap_active_pages) {
        kernel_vmap_pending_first_page = kernel_vmap_active_pages;
        kernel_vmap_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    first_page = page;
    while (page < kernel_vmap_active_pages && kernel_vmap_pending_bit_is_set(page) && page - first_page < max_pages) {
        kernel_vmap_bit_set(page);
        ++page;
    }
    page_count = page - first_page;
    kernel_vmap_pending_first_page = page;
    while (kernel_vmap_pending_first_page < kernel_vmap_active_pages && !kernel_vmap_pending_bit_is_set(kernel_vmap_pending_first_page)) {
        ++kernel_vmap_pending_first_page;
    }
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto kernel_vmap_has_pending_frees() -> bool {
    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    bool const PENDING = kernel_vmap_pending_first_page < kernel_vmap_active_pages;
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return PENDING;
}

auto kernel_vmap_warm_pool_over_limit() -> bool {
    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    bool const OVER_LIMIT = kernel_vmap_pending_pages >= KERNEL_VMAP_WARM_POOL_MAX_PAGES;
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return OVER_LIMIT;
}

auto perf_clamp_u32(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

auto perf_clamp_i32(uint64_t value) -> int32_t {
    return value > static_cast<uint64_t>(INT32_MAX) ? INT32_MAX : static_cast<int32_t>(value);
}

auto cow_ref_category(uint32_t refcount) -> uint16_t {
    if (refcount <= 1) {
        return 1;
    }
    if (refcount <= 4) {
        return 2;
    }
    if (refcount <= 16) {
        return 3;
    }
    return 4;
}

auto lazy_user_vmem_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = paging::PAGE_PRESENT | paging::PAGE_USER;
    if ((prot & 0x2ULL) != 0) {
        flags |= paging::PAGE_WRITE;
    }
    if ((prot & 0x4ULL) == 0) {
        flags |= paging::PAGE_NX;
    }
    return flags;
}

auto is_asan_shadow_address(uint64_t vaddr) -> bool {
    constexpr uint64_t ASAN_LOW_SHADOW_BEG = 0x000000007fff8000ULL;
    constexpr uint64_t ASAN_HIGH_SHADOW_END = 0x000010007fff7fffULL;
    return vaddr >= ASAN_LOW_SHADOW_BEG && vaddr <= ASAN_HIGH_SHADOW_END;
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ') {
            ++cursor;
        }

        const char* start = cursor;
        while (*cursor != '\0' && *cursor != ' ') {
            ++cursor;
        }

        if (std::cmp_equal(cursor - start, TOKEN_LEN) && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto lazy_vmem_fault_diag_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.lazy_diag");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (ENABLED) {
            log::info("lazy vmem fault diagnostics enabled by cmdline");
        }
        return ENABLED;
    }

    return expected != 0;
}

auto fork_eager_copy_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.fork_eager_copy");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (ENABLED) {
            log::info("fork writable private mappings will be eagerly copied by cmdline");
        }
        return ENABLED;
    }

    return expected != 0;
}

void log_sibling_lazy_vmem_fault_state(sched::task::Task* task, uint64_t page_vaddr) {
    if (task == nullptr || task->pagemap == nullptr) {
        return;
    }

    constexpr size_t MAX_LOGGED_SIBLINGS = 8;
    auto* const PAGEMAP = task->pagemap;
    size_t logged = 0;

    uint32_t const TASK_COUNT = sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT && logged < MAX_LOGGED_SIBLINGS; ++i) {
        auto* candidate = sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }

        if (candidate == task || candidate->pagemap != PAGEMAP || candidate->type == sched::task::TaskType::DAEMON) {
            candidate->release();
            continue;
        }

        sched::task::LazyVmemRange matching_range{};
        bool found = false;
        size_t range_count = 0;
        uint64_t const IRQF = candidate->lazy_vmem_lock.lock_irqsave();
        range_count = candidate->lazy_vmem_ranges.size();
        for (const auto& range : candidate->lazy_vmem_ranges) {
            if (page_vaddr >= range.start && page_vaddr < range.end) {
                matching_range = range;
                found = true;
                break;
            }
        }
        candidate->lazy_vmem_lock.unlock_irqrestore(IRQF);

        if (found) {
            log::warn(" lazy sibling hit: pid=%lu name=%s ranges=%llu range=[0x%llx, 0x%llx) prot=0x%llx flags=0x%llx", candidate->pid,
                      candidate->name != nullptr ? candidate->name : "?", static_cast<unsigned long long>(range_count),
                      static_cast<unsigned long long>(matching_range.start), static_cast<unsigned long long>(matching_range.end),
                      static_cast<unsigned long long>(matching_range.prot), static_cast<unsigned long long>(matching_range.flags));
            ++logged;
        }

        candidate->release();
    }
}

void log_lazy_vmem_fault_state(sched::task::Task* task, uint64_t page_vaddr, const paging::PageFault& fault, const char* reason,
                               uint64_t fault_rip = 0, uint64_t fault_rsp = 0) {
    constexpr size_t MAX_LOGGED_RANGES = 24;
    if (task == nullptr || (!is_asan_shadow_address(page_vaddr) && !lazy_vmem_fault_diag_enabled())) {
        return;
    }

    std::array<sched::task::LazyVmemRange, MAX_LOGGED_RANGES> ranges{};
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    size_t const RANGE_COUNT = task->lazy_vmem_ranges.size();
    size_t const LOG_COUNT = RANGE_COUNT < MAX_LOGGED_RANGES ? RANGE_COUNT : MAX_LOGGED_RANGES;
    for (size_t i = 0; i < LOG_COUNT; ++i) {
        ranges.at(i) = task->lazy_vmem_ranges.at(i);
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);

    log::warn("lazy vmem %s: pid=%lu name=%s page=0x%llx rip=0x%llx rsp=0x%llx err_present=%u err_write=%u ranges=%llu", reason, task->pid,
              task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(page_vaddr),
              static_cast<unsigned long long>(fault_rip), static_cast<unsigned long long>(fault_rsp), static_cast<unsigned>(fault.present),
              static_cast<unsigned>(fault.writable), static_cast<unsigned long long>(RANGE_COUNT));

    for (size_t i = 0; i < LOG_COUNT; ++i) {
        auto const& range = ranges.at(i);
        bool const CONTAINS = page_vaddr >= range.start && page_vaddr < range.end;
        log::warn(" lazy[%llu]: [0x%llx, 0x%llx) prot=0x%llx flags=0x%llx contains=%u", static_cast<unsigned long long>(i),
                  static_cast<unsigned long long>(range.start), static_cast<unsigned long long>(range.end),
                  static_cast<unsigned long long>(range.prot), static_cast<unsigned long long>(range.flags),
                  static_cast<unsigned>(CONTAINS));
    }

    if (lazy_vmem_fault_diag_enabled()) {
        log_sibling_lazy_vmem_fault_state(task, page_vaddr);
    }
}

auto anonymous_lazy_range_allows_fault(sched::task::Task* task, uint64_t page_vaddr, const paging::PageFault& fault, uint64_t& prot)
    -> bool {
    if (task == nullptr) {
        return false;
    }

    bool allowed = false;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (range.kind != sched::task::LazyVmemKind::ANONYMOUS || page_vaddr < range.start || page_vaddr >= range.end) {
            continue;
        }
        if (range.prot != 0 && (((range.prot & 0x2ULL) != 0) || fault.writable == 0U)) {
            prot = range.prot;
            allowed = true;
        }
        break;
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return allowed;
}

auto lazy_file_range_matches(const sched::task::LazyVmemRange& lhs, const sched::task::LazyVmemRange& rhs, uint64_t page_vaddr) -> bool {
    return sched::task::lazy_vmem_is_file(lhs) && lhs.kind == rhs.kind && lhs.start == rhs.start && lhs.end == rhs.end &&
           lhs.prot == rhs.prot && lhs.flags == rhs.flags &&
           (sched::task::lazy_vmem_has_lifetime(lhs) ? lhs.lifetime == rhs.lifetime : lhs.file == rhs.file) &&
           lhs.file_offset == rhs.file_offset && lhs.file_dev == rhs.file_dev && lhs.file_ino == rhs.file_ino && page_vaddr >= lhs.start &&
           page_vaddr < lhs.end;
}

auto file_binding_matches_range(const file_mapping::FileMappingBinding& binding, const sched::task::LazyVmemRange& range) -> bool {
    if (binding.lifetime() != range.lifetime || range.start < binding.start() || range.end > binding.end() || range.start >= range.end)
        return false;
    uint64_t const DELTA = range.start - binding.start();
    return binding.file_offset() <= UINT64_MAX - DELTA && range.file_offset == binding.file_offset() + DELTA;
}

auto demand_range_policy_valid(const sched::task::LazyVmemRange& range) -> bool {
    if (!sched::task::lazy_vmem_is_tmpfs_shared(range) || range.lifetime == nullptr || !range.lifetime->is_demand_shared() ||
        range.flags != ker::abi::vmem::MAP_SHARED)
        return false;
    bool const WRITABLE = range.lifetime->shared_writable();
    auto const* backing = range.lifetime->demand_read_shared_backing();
    uint64_t const MAXIMUM = ker::abi::vmem::PROT_READ | (WRITABLE ? ker::abi::vmem::PROT_WRITE : 0);
    return (range.kind == sched::task::LazyVmemKind::TMPFS_WRITE_SHARED) == WRITABLE && backing != nullptr &&
           backing->writable() == WRITABLE && (range.prot & ~MAXIMUM) == 0;
}

auto prepare_captured_tmpfs_read_shared_page(sched::task::Task& task, const sched::task::LazyVmemRange& range, uint64_t address,
                                             uint64_t root_incarnation, file_mapping::FileMappingBinding& binding) -> bool;

#ifdef WOS_ASYNC_BLOCK_IO
enum class LazyFileFaultDisposition : uint8_t { NOT_TRACKED, RESOLVED, PARK, FAILED };

struct LazyFileFaultWork {
    sched::task::Task* task{};
    PageTable* pagemap{};
    sched::task::LazyVmemRange range{};
    paging::PageFault fault{};
    uint64_t vaddr{};
    uint64_t generation{};
    uint64_t sequence{};
    size_t access_gate{USER_PAGEMAP_ACCESS_GATE_NONE};
    bool issued{};
    uint64_t root_incarnation{};                         // Public demand work never owns/dereferences an old root.
    file_mapping::FileMappingBinding* demand_binding{};  // One exact SNAPSHOT, no root lease.
};
constexpr size_t LAZY_FILE_FAULT_CAPACITY = 64;
std::array<LazyFileFaultWork, LAZY_FILE_FAULT_CAPACITY> lazy_file_fault_work;
sys::Spinlock lazy_file_fault_lock;
sched::task::Task* lazy_file_fault_worker{};  // Retained while published.
uint64_t lazy_file_fault_next_sequence{1};
bool lazy_file_fault_admission{};
bool lazy_file_fault_worker_starting{};

void wake_lazy_file_fault_worker() {
    uint64_t const FLAGS = lazy_file_fault_lock.lock_irqsave();
    auto* worker = lazy_file_fault_worker;
    if (worker != nullptr && !worker->try_acquire()) worker = nullptr;
    lazy_file_fault_lock.unlock_irqrestore(FLAGS);
    if (worker != nullptr) {
        sched::wake_task_from_event(worker);
        worker->release();
    }
}

auto probe_lazy_file_fault(sched::task::Task* task, uint64_t vaddr, const paging::PageFault& fault, bool user_frame, uint64_t& generation)
    -> LazyFileFaultDisposition {
    if (task == nullptr || task->pagemap == nullptr) return LazyFileFaultDisposition::NOT_TRACKED;
    uint64_t const PAGE_VADDR = page_align_down(vaddr);
    sched::task::LazyVmemRange snapshot;
    bool found = false;
    uint64_t flags = task->lazy_vmem_lock.lock_irqsave();
    for (auto const& range : task->lazy_vmem_ranges) {
        if (PAGE_VADDR >= range.start && PAGE_VADDR < range.end && sched::task::lazy_vmem_is_file(range)) {
            snapshot = range;
            found = true;
            break;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(flags);
    if (!found) return LazyFileFaultDisposition::NOT_TRACKED;
    // The existing exception-wait frame classifier accepts a user PROCESS
    // fault. A kernel PROCESS fault is not a timer frame and cannot be parked
    // through that protocol. Ordinary software-prefault usercopy is unchanged.
    if (!user_frame || task->type != sched::task::TaskType::PROCESS || task->preempt_disable_depth != 0 ||
        (sched::task::lazy_vmem_has_lifetime(snapshot) ? snapshot.lifetime == nullptr : snapshot.file == nullptr) || snapshot.prot == 0 ||
        (fault.writable != 0U && (snapshot.prot & 2U) == 0) || (fault.fetch != 0U && (snapshot.prot & 4U) == 0))
        return LazyFileFaultDisposition::FAILED;
    if (!task->try_acquire()) return LazyFileFaultDisposition::FAILED;
    PageTable* pagemap = nullptr;
    if (!task->try_acquire_usercopy_pagemap(pagemap)) {
        task->release();
        return LazyFileFaultDisposition::FAILED;
    }
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(pagemap, false, gate) && !sched::task::lazy_vmem_is_tmpfs_shared(snapshot)) {
        task->release_usercopy_pagemap();
        task->release();
        return LazyFileFaultDisposition::FAILED;
    }
    auto release_leases = [&] {
        release_user_pagemap_access(gate);
        task->release_usercopy_pagemap();
        task->release();
    };
    bool mapped = false;
    if (gate != USER_PAGEMAP_ACCESS_GATE_NONE) {
        uint64_t const PTE_FLAGS = cow_pte_lock.lock_irqsave();
        mapped = task->pagemap == pagemap && translate(pagemap, PAGE_VADDR) != PADDR_INVALID;
        cow_pte_lock.unlock_irqrestore(PTE_FLAGS);
    }
    if (mapped) {
        release_leases();
        return LazyFileFaultDisposition::RESOLVED;
    }

    flags = lazy_file_fault_lock.lock_irqsave();
    uint64_t const PREVIOUS = task->lazy_file_fault_generation;
    uint64_t const COMPLETED = task->lazy_file_fault_completion_generation.load(std::memory_order_acquire);
    if (PREVIOUS != 0 && COMPLETED != PREVIOUS) {
        generation = PREVIOUS;
        bool const SAME = task->lazy_file_fault_vaddr == PAGE_VADDR;
        lazy_file_fault_lock.unlock_irqrestore(flags);
        release_leases();
        return SAME ? LazyFileFaultDisposition::PARK : LazyFileFaultDisposition::FAILED;
    }
    if ((PREVIOUS != 0 && task->lazy_file_fault_vaddr == PAGE_VADDR && task->lazy_file_fault_status != 0) || !lazy_file_fault_admission ||
        lazy_file_fault_worker == nullptr || lazy_file_fault_worker_starting || PREVIOUS == UINT64_MAX ||
        lazy_file_fault_next_sequence == UINT64_MAX) {
        lazy_file_fault_lock.unlock_irqrestore(flags);
        release_leases();
        return LazyFileFaultDisposition::FAILED;
    }
    size_t index = lazy_file_fault_work.size();
    for (size_t i = 0; i < lazy_file_fault_work.size(); ++i) {
        if (lazy_file_fault_work[i].task == nullptr) {
            index = i;
            break;
        }
    }
    if (index == lazy_file_fault_work.size()) {
        lazy_file_fault_lock.unlock_irqrestore(flags);
        release_leases();
        return LazyFileFaultDisposition::FAILED;
    }
    // The pool -> range order occurs only here. Revalidate and retain the File
    // once publication is guaranteed, so rejection can never run a last File
    // put/close from the hardware fault. retain_file is an atomic increment.
    uint64_t const RANGE_FLAGS = task->lazy_vmem_lock.lock_irqsave();
    bool current = false;
    bool family_owned = false;
    file_mapping::FileMappingBinding* demand_binding = nullptr;
    uint64_t root_incarnation = 0;
    for (auto const& range : task->lazy_vmem_ranges) {
        if (task->pagemap == pagemap && lazy_file_range_matches(range, snapshot, PAGE_VADDR)) {
            snapshot = range;
            if (sched::task::lazy_vmem_has_lifetime(snapshot)) {
                current = family_owned =
                    snapshot.lifetime != nullptr && snapshot.lifetime->try_acquire(file_mapping::FamilyOwner::SNAPSHOT);
                if (current && sched::task::lazy_vmem_is_tmpfs_shared(snapshot)) {
                    uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
                    auto* control = find_user_root_control_locked(pagemap);
                    current = control != nullptr &&
                              file_mapping_index.retain_for_pin_locked(pagemap, control->serial, PAGE_VADDR, demand_binding) ==
                                  file_mapping::FileMappingPinResult::ACQUIRED;
                    if (current) {
                        root_incarnation = control->serial;
                        current = file_binding_matches_range(*demand_binding, snapshot) &&
                                  demand_binding->reclassify(file_mapping::BindingOwner::PIN, file_mapping::BindingOwner::SNAPSHOT);
                    }
                    user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
                }
            } else {
                ker::vfs::vfs_retain_file(snapshot.file);
                current = true;
            }
            break;
        }
    }
    if (current) {
        generation = PREVIOUS + 1;
        if (sched::task::lazy_vmem_is_tmpfs_shared(snapshot)) {
            // Make the work runnable only after both teardown-blocking leases
            // have gone. The retained Task/family/binding are root-independent.
            release_user_pagemap_access(gate);
            gate = USER_PAGEMAP_ACCESS_GATE_NONE;
            task->release_usercopy_pagemap();
            pagemap = nullptr;
        }
        lazy_file_fault_work[index] = {
            task, pagemap, snapshot,         fault,         PAGE_VADDR, generation, lazy_file_fault_next_sequence++,
            gate, false,   root_incarnation, demand_binding};
        task->lazy_file_fault_vaddr = PAGE_VADDR;
        task->lazy_file_fault_status = 0;
        task->lazy_file_fault_generation = generation;
    }
    task->lazy_vmem_lock.unlock_irqrestore(RANGE_FLAGS);
    lazy_file_fault_lock.unlock_irqrestore(flags);
    if (!current) {
        release_user_pagemap_access(gate);
        task->release_usercopy_pagemap();
        if (demand_binding != nullptr) demand_binding->release(file_mapping::BindingOwner::PIN);
        if (family_owned) snapshot.lifetime->release(file_mapping::FamilyOwner::SNAPSHOT);
        task->release();
        return LazyFileFaultDisposition::FAILED;
    }
    wake_lazy_file_fault_worker();
    return LazyFileFaultDisposition::PARK;
}

auto take_lazy_file_fault(LazyFileFaultWork& work, size_t& index) -> bool {
    uint64_t const FLAGS = lazy_file_fault_lock.lock_irqsave();
    index = lazy_file_fault_work.size();
    for (size_t i = 0; i < lazy_file_fault_work.size(); ++i) {
        auto const& candidate = lazy_file_fault_work[i];
        if (candidate.task != nullptr && !candidate.issued &&
            (index == lazy_file_fault_work.size() || candidate.sequence < lazy_file_fault_work[index].sequence))
            index = i;
    }
    if (index != lazy_file_fault_work.size()) {
        lazy_file_fault_work[index].issued = true;
        work = lazy_file_fault_work[index];
    }
    lazy_file_fault_lock.unlock_irqrestore(FLAGS);
    return index != lazy_file_fault_work.size();
}

void finish_lazy_file_fault(const LazyFileFaultWork& work, size_t index, int status) {
    auto* task = work.task;
    if (work.access_gate != USER_PAGEMAP_ACCESS_GATE_NONE) {
        release_user_pagemap_access(work.access_gate);
        task->release_usercopy_pagemap();
    }
    // Backend close may wait for MM readers to drain. Keep the work's File and
    // Task ownership, but release finished MM leases before entering close.
    if (work.demand_binding != nullptr) work.demand_binding->release(file_mapping::BindingOwner::SNAPSHOT);
    if (sched::task::lazy_vmem_has_lifetime(work.range))
        work.range.lifetime->release(file_mapping::FamilyOwner::SNAPSHOT);
    else
        ker::vfs::vfs_put_file(work.range.file);
    uint64_t const FLAGS = lazy_file_fault_lock.lock_irqsave();
    task->lazy_file_fault_status = status;
    lazy_file_fault_work[index] = {};
    task->lazy_file_fault_completion_generation.store(work.generation, std::memory_order_release);
    lazy_file_fault_lock.unlock_irqrestore(FLAGS);
    sched::wake_task_from_event(task);
    task->release();
}

void lazy_file_fault_worker_loop() {
    auto* current = sched::get_current_task();
    for (;;) {
        auto const HOLD = sched::hold_kernel_shutdown();
        uint64_t flags = lazy_file_fault_lock.lock_irqsave();
        if (sched::kernel_threads_shutdown_requested()) lazy_file_fault_admission = false;
        lazy_file_fault_lock.unlock_irqrestore(flags);
        LazyFileFaultWork work;
        size_t index = lazy_file_fault_work.size();
        if (take_lazy_file_fault(work, index)) {
#ifdef WOS_KASAN
            if (kasan::is_enabled()) kasan::unpoison_range(&work, sizeof(work));
#endif
            bool const DEMAND = sched::task::lazy_vmem_is_tmpfs_shared(work.range);
            bool const LIVE = work.task->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
                              (DEMAND || work.task->pagemap == work.pagemap);
            bool const OK =
                LIVE && (DEMAND ? work.demand_binding != nullptr &&
                                      prepare_captured_tmpfs_read_shared_page(*work.task, work.range, work.vaddr, work.root_incarnation,
                                                                              *work.demand_binding)
                                : ker::syscall::vmem::materialize_lazy_file_page(work.task, work.range, work.vaddr, work.fault));
            finish_lazy_file_fault(work, index, OK ? 0 : -EFAULT);
            continue;
        }
        flags = lazy_file_fault_lock.lock_irqsave();
        bool stop = sched::kernel_threads_shutdown_requested();
        bool pending = false;
        for (auto const& candidate : lazy_file_fault_work) pending |= candidate.task != nullptr;
        if (stop) lazy_file_fault_admission = false;
        bool const DRAIN = stop && pending;
        stop &= !pending;
        if (stop) {
            lazy_file_fault_worker = nullptr;
        }
        lazy_file_fault_lock.unlock_irqrestore(flags);
        if (stop) {
            current->release();
            return;
        }
        if (DRAIN) continue;
        sched::kern_block();
    }
}

void lazy_file_fault_worker_entry() {
    lazy_file_fault_worker_loop();
    sched::exit_current_kernel_thread();
}

void start_lazy_file_fault_worker() {
    auto const HOLD = sched::hold_kernel_shutdown();
    uint64_t flags = lazy_file_fault_lock.lock_irqsave();
    if (lazy_file_fault_worker != nullptr || lazy_file_fault_worker_starting || sched::kernel_threads_shutdown_requested()) {
        lazy_file_fault_lock.unlock_irqrestore(flags);
        return;
    }
    lazy_file_fault_worker_starting = true;
    lazy_file_fault_lock.unlock_irqrestore(flags);
    auto* worker = sched::task::Task::create_kernel_thread("file_fault", lazy_file_fault_worker_entry);
    if (worker == nullptr) {
        flags = lazy_file_fault_lock.lock_irqsave();
        lazy_file_fault_worker_starting = false;
        lazy_file_fault_lock.unlock_irqrestore(flags);
        return;
    }
    if (!worker->try_acquire_lifetime_ref()) ker::mod::dbg::panic_handler("file fault: invalid fresh worker lifetime");
    flags = lazy_file_fault_lock.lock_irqsave();
    lazy_file_fault_worker = worker;
    lazy_file_fault_lock.unlock_irqrestore(flags);
    if (!sched::post_task_balanced(worker)) {
        flags = lazy_file_fault_lock.lock_irqsave();
        lazy_file_fault_worker = nullptr;
        lazy_file_fault_worker_starting = false;
        lazy_file_fault_lock.unlock_irqrestore(flags);
        worker->release();
        sched::task::destroy_unpublished_user_thread(worker);
        return;
    }
    flags = lazy_file_fault_lock.lock_irqsave();
    lazy_file_fault_admission = lazy_file_fault_worker == worker && !sched::kernel_threads_shutdown_requested();
    lazy_file_fault_worker_starting = false;
    lazy_file_fault_lock.unlock_irqrestore(flags);
    wake_lazy_file_fault_worker();
}
#endif

auto find_anonymous_swap_mapping_locked(PageTable* pagemap, vaddr_t vaddr) -> AnonymousSwapMapping*;

auto handle_lazy_vmem_fault(sched::task::Task* task, uint64_t vaddr, const paging::PageFault& fault, const DemandContext& demand,
                            uint64_t fault_rip = 0, uint64_t fault_rsp = 0, bool allow_file_io = true) -> bool {
    if (task == nullptr || task->pagemap == nullptr) {
        return false;
    }

    uint64_t const PAGE_VADDR = vaddr & ~(paging::PAGE_SIZE - 1);
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (PAGE_VADDR < range.start || PAGE_VADDR >= range.end) {
            continue;
        }
        if (range.prot == 0 || (((range.prot & 0x2ULL) == 0) && fault.writable != 0U)) {
            task->lazy_vmem_lock.unlock_irqrestore(IRQF);
            log_lazy_vmem_fault_state(task, PAGE_VADDR, fault, "deny", fault_rip, fault_rsp);
            return false;
        }

        if (sched::task::lazy_vmem_is_file(range)) {
            // Public demand I/O has its own prepare-before-every-root-lease
            // route. Generic fault-in can already be beneath an outer pin.
            if (sched::task::lazy_vmem_is_tmpfs_shared(range)) {
                task->lazy_vmem_lock.unlock_irqrestore(IRQF);
                return false;
            }
            // A range can become file-backed after the hardware fault's
            // deferred probe missed it. Never fall back to synchronous file
            // I/O from that exception context.
            if (!allow_file_io) {
                task->lazy_vmem_lock.unlock_irqrestore(IRQF);
                return false;
            }
            auto file_range = range;
            if (file_range.kind == sched::task::LazyVmemKind::NATIVE_FILE_BACKED) {
                if (file_range.lifetime == nullptr || !file_range.lifetime->try_acquire(file_mapping::FamilyOwner::SNAPSHOT)) {
                    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
                    return false;
                }
            } else if (file_range.file != nullptr) {
                ker::vfs::vfs_retain_file(file_range.file);
            }
            task->lazy_vmem_lock.unlock_irqrestore(IRQF);
#ifdef WOS_KASAN
            if (kasan::is_enabled()) {
                // This translation unit is intentionally excluded from KASAN
                // instrumentation, so its stack slots can retain stale shadow
                // poison.  The materializer is instrumented and must receive
                // an addressable snapshot.
                kasan::unpoison_range(&file_range, sizeof(file_range));
            }
#endif
            bool const OK = ker::syscall::vmem::materialize_lazy_file_page(task, file_range, PAGE_VADDR, fault);
            if (file_range.kind == sched::task::LazyVmemKind::NATIVE_FILE_BACKED) {
                file_range.lifetime->release(file_mapping::FamilyOwner::SNAPSHOT);
            } else if (file_range.file != nullptr) {
                ker::vfs::vfs_put_file(file_range.file);
            }
            return OK;
        }

        task->lazy_vmem_lock.unlock_irqrestore(IRQF);

        // The output storage is addressable before construction/callee sret.
        // Lazy backing remains one plain fallible attempt, even on UMA.
        DemandResultStorage storage;
        auto& allocation = storage.get();
        numa::try_allocate_pages_into(allocation, demand.allocation, 1, paging::PAGE_SIZE, PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                      phys::PageInitialization::FULL_OVERWRITE, numa::ReclaimMode::NEVER, "lazy-vmem");
        void* const PAGE = allocation.span.data();
        record_demand_candidate(demand, PAGE == nullptr);
        if (PAGE == nullptr) {
            log::error("lazy vmem fault: OOM pid=%lu vaddr=0x%llx", task->pid, static_cast<unsigned long long>(PAGE_VADDR));
            return false;
        }
        std::memset(PAGE, 0, paging::PAGE_SIZE);
        auto const PADDR = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(PAGE)));

        // Threads have independent lazy-range locks even when they share a
        // pagemap. Serialize the absent-PTE check and install with COW updates
        // so two simultaneous faults cannot expose different pages at the
        // same virtual address. Keep the established PTE -> lazy-range lock
        // order and revalidate metadata after allocating outside both locks.
        auto const REQUESTED = allocation.requested;
        auto const ACTUAL = allocation.actual;
        uint64_t current_prot = 0;
        bool allowed = false;
        bool already_mapped = false;
        bool observed = false;
        {
            sched::PreemptGuard const PREEMPT;
            observed = demand_is_observed(demand);
            uint64_t const PTE_IRQF = cow_pte_lock.lock_irqsave();
            // A non-present tracked swap leaf still owns saved bytes. Generic
            // software fault-in must not replace it with fresh zero backing.
            allowed = find_anonymous_swap_mapping_locked(task->pagemap, PAGE_VADDR) == nullptr &&
                      anonymous_lazy_range_allows_fault(task, PAGE_VADDR, fault, current_prot);
            already_mapped = translate(task->pagemap, PAGE_VADDR) != PADDR_INVALID;
            if (allowed && !already_mapped) {
                map_page(task->pagemap, PAGE_VADDR, PADDR, lazy_user_vmem_flags(current_prot));
            }
            cow_pte_lock.unlock_irqrestore(PTE_IRQF);
        }
        bool const ALLOWED = allowed;
        bool const ALREADY_MAPPED = already_mapped;
        if (ALLOWED && !ALREADY_MAPPED)
            (void)allocation.span.release();
        else
            allocation.span.reset();
        record_demand_result(demand, DemandBacking::Anonymous, REQUESTED, ACTUAL, ALLOWED && !ALREADY_MAPPED, false, observed);
        if (!ALLOWED) {
            log_lazy_vmem_fault_state(task, PAGE_VADDR, fault, "stale", fault_rip, fault_rsp);
            return false;
        }
        if (ALREADY_MAPPED) {
            invalidate_local_tlb_if_current(task->pagemap, PAGE_VADDR, false);
        }
        return true;
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);

    log_lazy_vmem_fault_state(task, PAGE_VADDR, fault, "miss", fault_rip, fault_rsp);
    return false;
}

// Caller holds task->lazy_vmem_lock through the corresponding leaf update.
auto writable_anonymous_range_contains_locked(sched::task::Task* task, uint64_t vaddr) -> bool {
    if (task == nullptr) {
        return false;
    }

    constexpr uint64_t PROT_WRITE = 0x2ULL;
    constexpr uint64_t MAP_ANONYMOUS = 0x20ULL;
    uint64_t const PAGE_VADDR = vaddr & ~(paging::PAGE_SIZE - 1);
    bool contains = false;
    for (const auto& range : task->lazy_vmem_ranges) {
        if (PAGE_VADDR >= range.start && PAGE_VADDR < range.end && (range.prot & PROT_WRITE) != 0U && (range.flags & MAP_ANONYMOUS) != 0U) {
            contains = true;
            break;
        }
    }
    return contains;
}

void record_cow_perf_event(sched::task::Task* task, perf::WkiPerfLocalVmemOp op, uint64_t vaddr, uint32_t refcount, uint64_t started_us) {
    if (task == nullptr || !perf::is_wki_recording_enabled()) {
        return;
    }

    uint64_t const NOW_US = time::get_us();
    uint32_t const ELAPSED_US = perf_clamp_u32(NOW_US >= started_us ? NOW_US - started_us : 0);
    perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), task->pid, perf::WkiPerfScope::LOCAL_VMEM, static_cast<uint8_t>(op),
                           perf::WkiPerfPhase::END, 1, cow_ref_category(refcount), perf::next_wki_trace_correlation(),
                           perf_clamp_i32(refcount), ELAPSED_US, vaddr);
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_VMEM, static_cast<uint8_t>(op), 0, 0, perf_clamp_i32(refcount), ELAPSED_US, true, 0,
                             paging::PAGE_SIZE);
}

void alloc_cow_destination_page(bool full_overwrite, const DemandContext& demand, numa::AllocationResult& output) {
    numa::try_allocate_pages_into(output, demand.allocation, 1, paging::PAGE_SIZE, PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                  full_overwrite ? phys::PageInitialization::FULL_OVERWRITE : phys::PageInitialization::ZERO,
                                  numa::ReclaimMode::LEGACY_UMA, full_overwrite ? "cow_copy" : "cow_zero");
    void* const PAGE = output.span.data();
    if (PAGE != nullptr && !full_overwrite) std::memset(PAGE, 0, paging::PAGE_SIZE);
}

void anonymous_swap_add(std::atomic<uint64_t>& counter, uint64_t amount = 1) {
    uint64_t value = counter.load(std::memory_order_relaxed);
    while (value != UINT64_MAX) {
        uint64_t const UPDATED = amount > UINT64_MAX - value ? UINT64_MAX : value + amount;
        if (counter.compare_exchange_weak(value, UPDATED, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return;
        }
    }
}

void anonymous_swap_update_max(std::atomic<uint64_t>& counter, uint64_t candidate) {
    uint64_t value = counter.load(std::memory_order_relaxed);
    while (value < candidate && !counter.compare_exchange_weak(value, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

auto anonymous_swap_enabled() -> bool {
    static std::atomic<int> enabled{-1};
    int cached = enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }
    bool const VALUE = cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.anon_swap=on");
    int expected = -1;
    if (enabled.compare_exchange_strong(expected, VALUE ? 1 : 0, std::memory_order_acq_rel, std::memory_order_acquire) && VALUE) {
        log::info("private anonymous swap enabled by cmdline");
    }
    return VALUE;
}

auto next_anonymous_swap_generation() -> uint64_t {
    uint64_t generation = anonymous_swap_generation.fetch_add(1, std::memory_order_relaxed);
    if (generation == 0 || generation == UINT64_MAX) {
        generation = anonymous_swap_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return generation;
}

auto find_anonymous_swap_mapping_locked(PageTable* pagemap, vaddr_t vaddr) -> AnonymousSwapMapping* {
    for (auto* mapping = anonymous_swap_mappings; mapping != nullptr; mapping = mapping->next) {
        if (mapping->pagemap == pagemap && mapping->vaddr == vaddr) {
            return mapping;
        }
    }
    return nullptr;
}

void insert_anonymous_swap_mapping_locked(AnonymousSwapMapping* mapping) {
    mapping->next = anonymous_swap_mappings;
    anonymous_swap_mappings = mapping;
}

void remove_anonymous_swap_mapping_locked(AnonymousSwapMapping* mapping) {
    AnonymousSwapMapping** link = &anonymous_swap_mappings;
    while (*link != nullptr) {
        if (*link == mapping) {
            *link = mapping->next;
            mapping->next = nullptr;
            return;
        }
        link = &(*link)->next;
    }
}

auto anonymous_swap_marker_raw(uint64_t resident_raw) -> uint64_t {
    constexpr uint64_t PRESERVED = paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_COW | paging::PAGE_NX;
    return (resident_raw & PRESERVED) | paging::PAGE_RESERVED;
}

auto anonymous_pagein_enqueue(AnonymousSwapMapping* mapping) -> bool {
    uint64_t const FLAGS = anonymous_pagein_queue_lock.lock_irqsave();
    if (anonymous_pagein_queue_count == anonymous_pagein_queue.size()) {
        anonymous_pagein_queue_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    anonymous_pagein_queue.at(anonymous_pagein_queue_tail) = mapping;
    anonymous_pagein_queue_tail = (anonymous_pagein_queue_tail + 1) % anonymous_pagein_queue.size();
    ++anonymous_pagein_queue_count;
    anonymous_pagein_queue_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto anonymous_pagein_dequeue() -> AnonymousSwapMapping* {
    uint64_t const FLAGS = anonymous_pagein_queue_lock.lock_irqsave();
    if (anonymous_pagein_queue_count == 0) {
        anonymous_pagein_queue_lock.unlock_irqrestore(FLAGS);
        return nullptr;
    }
    auto* mapping = anonymous_pagein_queue.at(anonymous_pagein_queue_head);
    anonymous_pagein_queue.at(anonymous_pagein_queue_head) = nullptr;
    anonymous_pagein_queue_head = (anonymous_pagein_queue_head + 1) % anonymous_pagein_queue.size();
    --anonymous_pagein_queue_count;
    anonymous_pagein_queue_lock.unlock_irqrestore(FLAGS);
    return mapping;
}

void release_anonymous_swap_object(AnonymousSwapObject* object) {
    if (object == nullptr) {
        return;
    }
    static_cast<void>(swap::free_slot(object->slot));
    delete object;
}

auto detach_anonymous_swap_object_locked(AnonymousSwapMapping* mapping) -> AnonymousSwapObject* {
    if (mapping == nullptr || mapping->object == nullptr) {
        return nullptr;
    }
    auto* object = mapping->object;
    mapping->object = nullptr;
    if (object->mapping_refs != 0) {
        --object->mapping_refs;
    }
    return object->mapping_refs == 0 ? object : nullptr;
}

auto detach_anonymous_swap_waiters_locked(AnonymousSwapMapping* mapping) -> sched::task::Task* {
    auto* waiters = mapping != nullptr ? mapping->waiters : nullptr;
    if (mapping != nullptr) {
        mapping->waiters = nullptr;
    }
    for (auto* task = waiters; task != nullptr; task = task->anonymous_swap_wait_next) {
        task->anonymous_swap_wait_mapping = nullptr;
        task->anonymous_swap_completion_generation.store(task->anonymous_swap_wait_generation, std::memory_order_release);
    }
    return waiters;
}

void wake_anonymous_swap_waiters(sched::task::Task* waiters) {
    while (waiters != nullptr) {
        auto* task = waiters;
        waiters = task->anonymous_swap_wait_next;
        task->anonymous_swap_wait_next = nullptr;
        sched::wake_task_from_event(task);
        task->release();
    }
}

enum class AnonymousSwapFaultDisposition : uint8_t {
    NOT_TRACKED,
    RESOLVED,
    PARK,
    FAILED,
};

auto probe_anonymous_swap_fault_locked(sched::task::Task* task, vaddr_t vaddr, const paging::PageFault& fault, uint64_t& wait_generation)
    -> AnonymousSwapFaultDisposition {
    auto* mapping = find_anonymous_swap_mapping_locked(task->pagemap, vaddr);
    if (mapping == nullptr) {
        return AnonymousSwapFaultDisposition::NOT_TRACKED;
    }
    if (mapping->state == AnonymousSwapRuntimeState::FAILED || mapping->cancelled || mapping->error < 0) {
        return AnonymousSwapFaultDisposition::FAILED;
    }
    if (mapping->state == AnonymousSwapRuntimeState::PAGEOUT) {
        return AnonymousSwapFaultDisposition::RESOLVED;
    }
    bool const WRITABLE = (mapping->original_raw & (paging::PAGE_WRITE | paging::PAGE_COW)) != 0U;
    if ((mapping->original_raw & paging::PAGE_USER) == 0U || (fault.writable != 0U && !WRITABLE) ||
        (fault.fetch != 0U && (mapping->original_raw & paging::PAGE_NX) != 0U)) {
        return AnonymousSwapFaultDisposition::FAILED;
    }

    if (!mapping->pagein_queued) {
        size_t access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
        if (!acquire_user_pagemap_access(mapping->pagemap, false, access_gate)) {
            return AnonymousSwapFaultDisposition::FAILED;
        }
        mapping->generation = next_anonymous_swap_generation();
        mapping->pagein_queued = true;
        mapping->pagein_access_gate = access_gate;
        if (!anonymous_pagein_enqueue(mapping)) {
            mapping->pagein_queued = false;
            mapping->pagein_access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
            release_user_pagemap_access(access_gate);
            mapping->error = -EAGAIN;
            return AnonymousSwapFaultDisposition::FAILED;
        }
        anonymous_swap_add(anonymous_swap_stats.pagein_attempts);
        auto* const WORKER = anonymous_swap_worker_task.load(std::memory_order_acquire);
        if (WORKER != nullptr) {
            sched::wake_task_from_event(WORKER);
        }
    }

    if (task->anonymous_swap_wait_mapping != nullptr || !task->try_acquire()) {
        return AnonymousSwapFaultDisposition::FAILED;
    }
    wait_generation = mapping->generation;
    task->anonymous_swap_wait_mapping = mapping;
    task->anonymous_swap_wait_generation = wait_generation;
    task->anonymous_swap_completion_generation.store(0, std::memory_order_relaxed);
    task->anonymous_swap_wait_next = mapping->waiters;
    mapping->waiters = task;
    return AnonymousSwapFaultDisposition::PARK;
}

auto probe_anonymous_swap_fault(sched::task::Task* task, vaddr_t vaddr, const paging::PageFault& fault, uint64_t& wait_generation)
    -> AnonymousSwapFaultDisposition {
    if (task == nullptr || task->pagemap == nullptr) {
        return AnonymousSwapFaultDisposition::NOT_TRACKED;
    }
    uint64_t const PAGE_VADDR = page_align_down(vaddr);
    uint64_t const FLAGS = cow_pte_lock.lock_irqsave();
    auto const RESULT = probe_anonymous_swap_fault_locked(task, PAGE_VADDR, fault, wait_generation);
    cow_pte_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

// Every window entry owns its access gate, mapping/frame and slot I/O until
// finish. Storage is permanent and bounded; no batch lives on the kernel stack.
struct AnonymousPageinOperation {
    AnonymousSwapMapping* mapping{};
    void* page{};
    uint64_t started_us{};
    uint64_t generation{};
    size_t access_gate{USER_PAGEMAP_ACCESS_GATE_NONE};
    bool should_read{};
    int result{};
    swap::SlotIo io;
};
#ifdef WOS_ASYNC_BLOCK_IO
constexpr size_t ANONYMOUS_PAGEIN_WINDOW = 8;
#else
constexpr size_t ANONYMOUS_PAGEIN_WINDOW = 1;
#endif
std::array<AnonymousPageinOperation, ANONYMOUS_PAGEIN_WINDOW> anonymous_pagein_operations;

void begin_anonymous_pagein(AnonymousSwapMapping* mapping, AnonymousPageinOperation& operation) {
    operation.mapping = mapping;
    operation.page = nullptr;
    operation.generation = 0;
    operation.should_read = false;
    operation.started_us = time::get_us();
    operation.access_gate = mapping->pagein_access_gate;
    size_t const ACCESS_GATE = operation.access_gate;
    bool const ACCESS = ACCESS_GATE != USER_PAGEMAP_ACCESS_GATE_NONE;
    auto& page = operation.page;
    auto& generation = operation.generation;
    auto& should_read = operation.should_read;
    swap::SwapSlot slot{};
    if (ACCESS) {
        page = phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, "anon-swapin");
    }
    {
        uint64_t const FLAGS = cow_pte_lock.lock_irqsave();
        mapping->pagein_access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
        if (ACCESS && page != nullptr && mapping->pagein_queued && !mapping->cancelled &&
            mapping->state == AnonymousSwapRuntimeState::SWAPPED && mapping->object != nullptr) {
            mapping->state = AnonymousSwapRuntimeState::PAGEIN;
            mapping->frame = page;
            slot = mapping->object->slot;
            generation = mapping->generation;
            should_read = true;
            anonymous_swap_stats.swapped_pages.fetch_sub(1, std::memory_order_relaxed);
            anonymous_swap_add(anonymous_swap_stats.pagein_inflight);
        }
        cow_pte_lock.unlock_irqrestore(FLAGS);
    }

    operation.result = page == nullptr ? -ENOMEM : -ECANCELED;
    if (should_read) {
        operation.result = swap::submit_slot_io(operation.io, slot, page, false);
    }
}

void finish_anonymous_pagein(AnonymousPageinOperation& operation) {
    auto* mapping = operation.mapping;
    void* page = operation.page;
    uint64_t const STARTED_US = operation.started_us;
    size_t const ACCESS_GATE = operation.access_gate;
    bool const ACCESS = ACCESS_GATE != USER_PAGEMAP_ACCESS_GATE_NONE;
    uint64_t const generation = operation.generation;
    bool const should_read = operation.should_read;
    // The worker stops preparation at backpressure and finishes older entries
    // first. Their explicit release makes capacity available for this retry.
    while (!operation.io.active && operation.result == -EAGAIN) {
        operation.result = swap::submit_slot_io(operation.io, operation.io.slot, page, false);
        if (operation.result == -EAGAIN) ker::dev::block_queue_wait_for_capacity(operation.io.device);
    }
    int result = operation.io.active ? swap::finish_slot_io(operation.io) : operation.result;
    AnonymousSwapObject* object_to_release = nullptr;
    sched::task::Task* waiters = nullptr;
    bool installed = false;
    bool delete_mapping_after = false;
    {
        uint64_t const FLAGS = cow_pte_lock.lock_irqsave();
        if (should_read && mapping->state == AnonymousSwapRuntimeState::PAGEIN && mapping->generation == generation &&
            mapping->frame == page) {
            PageTableEntry* entry = leaf_entry(mapping->pagemap, mapping->vaddr);
            bool const CURRENT = entry != nullptr && entry->present == 0 && is_reserved_leaf(*entry);
            if (result == 0 && CURRENT && !mapping->cancelled) {
                uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
                uint64_t const PADDR = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(page)));
                uint64_t const RAW = (mapping->original_raw & ~PTE_FRAME_MASK) | (PADDR & PTE_FRAME_MASK) | paging::PAGE_PRESENT;
                *entry = pte_from_raw(RAW);
                owned_frame_track_fresh_normal_mapping(mapping->pagemap, mapping->vaddr, PADDR, RAW);
                user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
                mapping->frame = nullptr;
                mapping->pagein_queued = false;
                object_to_release = detach_anonymous_swap_object_locked(mapping);
                waiters = detach_anonymous_swap_waiters_locked(mapping);
                remove_anonymous_swap_mapping_locked(mapping);
                delete_mapping_after = true;
                installed = true;
                anonymous_swap_add(anonymous_swap_stats.pagein_successes);
            } else if (mapping->cancelled || !CURRENT) {
                mapping->frame = nullptr;
                mapping->pagein_queued = false;
                object_to_release = detach_anonymous_swap_object_locked(mapping);
                waiters = detach_anonymous_swap_waiters_locked(mapping);
                remove_anonymous_swap_mapping_locked(mapping);
                delete_mapping_after = true;
            } else {
                mapping->frame = nullptr;
                mapping->pagein_queued = false;
                object_to_release = detach_anonymous_swap_object_locked(mapping);
                waiters = detach_anonymous_swap_waiters_locked(mapping);
                mapping->state = AnonymousSwapRuntimeState::FAILED;
                mapping->error = result < 0 ? result : -EIO;
                anonymous_swap_add(anonymous_swap_stats.pagein_failures);
                if (result == -EILSEQ) {
                    anonymous_swap_add(anonymous_swap_stats.pagein_corruption);
                }
            }
            anonymous_swap_stats.pagein_inflight.fetch_sub(1, std::memory_order_relaxed);
        } else {
            mapping->pagein_queued = false;
            waiters = detach_anonymous_swap_waiters_locked(mapping);
            if (mapping->cancelled) {
                object_to_release = detach_anonymous_swap_object_locked(mapping);
                remove_anonymous_swap_mapping_locked(mapping);
                delete_mapping_after = true;
            } else if (mapping->state == AnonymousSwapRuntimeState::SWAPPED) {
                object_to_release = detach_anonymous_swap_object_locked(mapping);
                mapping->state = AnonymousSwapRuntimeState::FAILED;
                mapping->error = result < 0 ? result : -EIO;
                anonymous_swap_stats.swapped_pages.fetch_sub(1, std::memory_order_relaxed);
                anonymous_swap_add(anonymous_swap_stats.pagein_failures);
            }
        }
        cow_pte_lock.unlock_irqrestore(FLAGS);
    }

    if (installed) {
        flush_pagemap_after_update(mapping->pagemap, mapping->vaddr, false);
    }
    if (page != nullptr && !installed) {
        std::memset(page, 0, paging::PAGE_SIZE);
        phys::page_ref_dec(page);
    }
    release_anonymous_swap_object(object_to_release);
    wake_anonymous_swap_waiters(waiters);
    if (ACCESS) {
        release_user_pagemap_access(ACCESS_GATE);
    }
    uint64_t const FINISHED_US = time::get_us();
    uint64_t const ELAPSED = FINISHED_US >= STARTED_US ? FINISHED_US - STARTED_US : 0;
    anonymous_swap_add(anonymous_swap_stats.pagein_latency_us, ELAPSED);
    anonymous_swap_update_max(anonymous_swap_stats.pagein_latency_max_us, ELAPSED);
    if (delete_mapping_after) {
        delete mapping;
    }
}

// Migration and BSP selftests retain the one-page bridge; only the dedicated
// worker owns the permanent window above.
void service_anonymous_pagein(AnonymousSwapMapping* mapping) {
    if (mapping == nullptr) return;
    auto const HOLD = sched::hold_kernel_shutdown();
    AnonymousPageinOperation operation;
    begin_anonymous_pagein(mapping, operation);
    finish_anonymous_pagein(operation);
}

[[noreturn]] void anonymous_pagein_worker_main() {
    for (;;) {
        bool empty = true;
        {
            auto const HOLD = sched::hold_kernel_shutdown();
            size_t count = 0;
            while (count < anonymous_pagein_operations.size()) {
                auto* mapping = anonymous_pagein_dequeue();
                if (mapping == nullptr) break;
                if (count == 0) {
                    begin_anonymous_pagein(mapping, anonymous_pagein_operations[count]);
                } else {
                    // Earlier entries retain the queue's owner/byte budget.
                    // Allocation must not reclaim through I/O on that queue
                    // before this batch can join and release those owners.
                    reclaim::NoIoScope const NO_IO;
                    begin_anonymous_pagein(mapping, anonymous_pagein_operations[count]);
                }
                bool const FULL = anonymous_pagein_operations[count].result == -EAGAIN;
                ++count;
                if (FULL) break;
            }
            empty = count == 0;
            for (size_t i = 0; i < count; ++i) finish_anonymous_pagein(anonymous_pagein_operations[i]);
        }
        if (empty) sched::kern_block();
    }
}

struct AnonymousPageoutCandidate {
    PageTable* pagemap{};
    vaddr_t vaddr{};
    void* frame{};
    uint64_t raw{};
    uint64_t prot{};
    size_t access_gate{USER_PAGEMAP_ACCESS_GATE_NONE};
};

enum class AnonymousCandidateDisposition : uint8_t {
    NONE,
    AGED,
    SELECTED,
};

auto snapshot_anonymous_candidate(sched::task::Task* task, uint64_t cursor, AnonymousPageoutCandidate& out)
    -> AnonymousCandidateDisposition {
    if (task == nullptr) {
        return AnonymousCandidateDisposition::NONE;
    }
    PageTable* const PAGEMAP = task->pagemap;
    size_t access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (PAGEMAP == nullptr || !acquire_user_pagemap_access(PAGEMAP, false, access_gate) || task->pagemap != PAGEMAP) {
        release_user_pagemap_access(access_gate);
        return AnonymousCandidateDisposition::NONE;
    }

    std::array<sched::task::LazyVmemRange, ANONYMOUS_RECLAIM_MAX_RANGES> ranges{};
    size_t range_count = 0;
    uint64_t const RANGE_FLAGS = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        constexpr uint64_t MAP_SHARED = 0x01;
        if (range_count == ranges.size()) {
            break;
        }
        if (range.kind == sched::task::LazyVmemKind::ANONYMOUS && range.start < range.end && (range.flags & MAP_SHARED) == 0) {
            ranges.at(range_count++) = range;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(RANGE_FLAGS);
    if (range_count == 0) {
        release_user_pagemap_access(access_gate);
        return AnonymousCandidateDisposition::NONE;
    }

    auto const& range = ranges.at(static_cast<size_t>(cursor % range_count));
    uint64_t const PAGE_COUNT = (range.end - range.start) / paging::PAGE_SIZE;
    if (PAGE_COUNT == 0) {
        release_user_pagemap_access(access_gate);
        return AnonymousCandidateDisposition::NONE;
    }
    vaddr_t const VADDR = range.start + (((cursor / range_count) % PAGE_COUNT) * paging::PAGE_SIZE);

    bool aged = false;
    bool selected = false;
    uint64_t const PTE_FLAGS = cow_pte_lock.lock_irqsave();
    uint64_t const LAZY_FLAGS = task->lazy_vmem_lock.lock_irqsave();
    bool range_current = false;
    uint64_t prot = 0;
    for (const auto& candidate : task->lazy_vmem_ranges) {
        if (candidate.kind == sched::task::LazyVmemKind::ANONYMOUS && VADDR >= candidate.start && VADDR < candidate.end &&
            (candidate.flags & 0x01ULL) == 0) {
            range_current = true;
            prot = candidate.prot;
            break;
        }
    }
    uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry* entry = range_current ? leaf_entry(PAGEMAP, VADDR) : nullptr;
    if (entry != nullptr && entry->present != 0 && entry->user != 0 && (pte_raw(*entry) & paging::PAGE_SHARED) == 0U &&
        find_anonymous_swap_mapping_locked(PAGEMAP, VADDR) == nullptr) {
        if (entry->accessed != 0) {
            entry->accessed = 0;
            aged = true;
        } else {
            uint64_t const PHYS = static_cast<uint64_t>(entry->frame) << paging::PAGE_SHIFT;
            void* const FRAME = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS));
            phys::PageLookupHint lookup{};
            if (phys::page_ref_get(FRAME, &lookup) == 1) {
                phys::page_ref_inc(FRAME, &lookup);
                out = {
                    .pagemap = PAGEMAP, .vaddr = VADDR, .frame = FRAME, .raw = pte_raw(*entry), .prot = prot, .access_gate = access_gate};
                selected = true;
            }
        }
    }
    user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
    task->lazy_vmem_lock.unlock_irqrestore(LAZY_FLAGS);
    cow_pte_lock.unlock_irqrestore(PTE_FLAGS);

    if (aged) {
        flush_pagemap_after_update(PAGEMAP, VADDR, false);
    }
    if (!selected) {
        release_user_pagemap_access(access_gate);
    }
    if (selected) {
        return AnonymousCandidateDisposition::SELECTED;
    }
    return aged ? AnonymousCandidateDisposition::AGED : AnonymousCandidateDisposition::NONE;
}

auto pageout_anonymous_candidate(AnonymousPageoutCandidate& candidate) -> bool {
    anonymous_swap_add(anonymous_swap_stats.pageout_attempts);
    auto* object = new AnonymousSwapObject{};
    auto* mapping = new AnonymousSwapMapping{};
    if (object == nullptr || mapping == nullptr) {
        delete object;
        delete mapping;
        phys::page_ref_dec(candidate.frame);
        release_user_pagemap_access(candidate.access_gate);
        anonymous_swap_add(anonymous_swap_stats.pageout_failures);
        return false;
    }
    int result = swap::allocate_slot(&object->slot);
    if (result < 0) {
        delete object;
        delete mapping;
        phys::page_ref_dec(candidate.frame);
        release_user_pagemap_access(candidate.access_gate);
        anonymous_swap_add(anonymous_swap_stats.pageout_failures);
        return false;
    }
    object->mapping_refs = 1;
    mapping->pagemap = candidate.pagemap;
    mapping->vaddr = candidate.vaddr;
    mapping->state = AnonymousSwapRuntimeState::PAGEOUT;
    mapping->object = object;
    mapping->frame = candidate.frame;
    mapping->original_raw = candidate.raw;
    mapping->generation = next_anonymous_swap_generation();

    bool published = false;
    bool froze_writable_mapping = false;
    {
        uint64_t const FLAGS = cow_pte_lock.lock_irqsave();
        uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
        PageTableEntry* entry = leaf_entry(candidate.pagemap, candidate.vaddr);
        if (entry != nullptr && entry->present != 0 &&
            (static_cast<uint64_t>(entry->frame) << paging::PAGE_SHIFT) ==
                reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(candidate.frame))) &&
            find_anonymous_swap_mapping_locked(candidate.pagemap, candidate.vaddr) == nullptr) {
            mapping->original_raw = pte_raw(*entry);
            mapping->frozen_raw = mapping->original_raw;
            if ((mapping->frozen_raw & paging::PAGE_WRITE) != 0U) {
                mapping->frozen_raw &= ~paging::PAGE_WRITE;
                mapping->frozen_raw |= paging::PAGE_COW;
                *entry = pte_from_raw(mapping->frozen_raw);
                froze_writable_mapping = true;
            }
            insert_anonymous_swap_mapping_locked(mapping);
            published = true;
            anonymous_swap_add(anonymous_swap_stats.pageout_inflight);
        }
        user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
        cow_pte_lock.unlock_irqrestore(FLAGS);
    }
    if (!published) {
        release_anonymous_swap_object(object);
        delete mapping;
        phys::page_ref_dec(candidate.frame);
        release_user_pagemap_access(candidate.access_gate);
        anonymous_swap_add(anonymous_swap_stats.pageout_failures);
        return false;
    }

    if (froze_writable_mapping) {
        flush_pagemap_after_update(candidate.pagemap, candidate.vaddr, false);
    }

    uint64_t const STARTED_US = time::get_us();
    result = swap::write_slot(object->slot, candidate.frame);
    bool committed = false;
    bool restored = false;
    AnonymousSwapObject* object_to_release = nullptr;
    {
        uint64_t const FLAGS = cow_pte_lock.lock_irqsave();
        uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
        PageTableEntry* entry = leaf_entry(candidate.pagemap, candidate.vaddr);
        bool const SAME_FRAME = entry != nullptr && entry->present != 0 && pte_raw(*entry) == mapping->frozen_raw &&
                                (static_cast<uint64_t>(entry->frame) << paging::PAGE_SHIFT) ==
                                    reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(candidate.frame)));
        if (result == 0 && SAME_FRAME && !mapping->cancelled) {
            owned_frame_untrack_leaf(candidate.pagemap, candidate.vaddr, *entry);
            *entry = pte_from_raw(anonymous_swap_marker_raw(mapping->original_raw));
            mapping->state = AnonymousSwapRuntimeState::SWAPPED;
            mapping->frame = nullptr;
            committed = true;
            anonymous_swap_add(anonymous_swap_stats.swapped_pages);
            anonymous_swap_add(anonymous_swap_stats.pageout_successes);
        } else {
            if (result < 0 && SAME_FRAME && !mapping->cancelled && mapping->frozen_raw != mapping->original_raw) {
                *entry = pte_from_raw(mapping->original_raw);
                restored = true;
            }
            remove_anonymous_swap_mapping_locked(mapping);
            object_to_release = detach_anonymous_swap_object_locked(mapping);
            mapping->frame = nullptr;
            anonymous_swap_add(anonymous_swap_stats.pageout_failures);
        }
        anonymous_swap_stats.pageout_inflight.fetch_sub(1, std::memory_order_relaxed);
        user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
        cow_pte_lock.unlock_irqrestore(FLAGS);
    }

    if (committed) {
        flush_pagemap_after_update(candidate.pagemap, candidate.vaddr, false);
        // PTE mapping ownership and the pageout pin are distinct references.
        phys::page_ref_dec(candidate.frame);
        phys::page_ref_dec(candidate.frame);
    } else {
        if (restored) {
            flush_pagemap_after_update(candidate.pagemap, candidate.vaddr, false);
        }
        phys::page_ref_dec(candidate.frame);
        release_anonymous_swap_object(object_to_release);
        delete mapping;
    }
    release_user_pagemap_access(candidate.access_gate);
    uint64_t const FINISHED_US = time::get_us();
    uint64_t const ELAPSED = FINISHED_US >= STARTED_US ? FINISHED_US - STARTED_US : 0;
    anonymous_swap_add(anonymous_swap_stats.pageout_latency_us, ELAPSED);
    anonymous_swap_update_max(anonymous_swap_stats.pageout_latency_max_us, ELAPSED);
    return committed;
}

auto anonymous_swap_reclaim_count(void* /*context*/, const reclaim::ReclaimRequest& /*request*/) -> reclaim::ReclaimCount {
    if (!anonymous_swap_enabled()) {
        return {};
    }
    swap::SwapStats stats{};
    swap::get_stats(&stats);
    uint64_t const FREE_SLOTS = stats.free_bytes / paging::PAGE_SIZE;
    uint64_t const TASKS = sched::get_active_task_count();
    uint64_t const RECLAIMABLE = std::min<uint64_t>(FREE_SLOTS, TASKS * 32);
    uint64_t const UNRECLAIMABLE = TASKS != 0 && FREE_SLOTS == 0 ? TASKS : 0U;
    return {.reclaimable = RECLAIMABLE, .unreclaimable = UNRECLAIMABLE};
}

auto anonymous_swap_reclaim_scan(void* /*context*/, const reclaim::ReclaimRequest& request) -> reclaim::ReclaimScanResult {
    uint64_t const PAGE_BUDGET = std::min(request.budget_units, request.target_pages);
    uint64_t const SCAN_BUDGET = request.scan_budget_units;
    reclaim::ReclaimScanResult result{};
    uint32_t const TASK_COUNT = sched::get_active_task_count();
    if (PAGE_BUDGET == 0 || SCAN_BUDGET == 0 || TASK_COUNT == 0 || !anonymous_swap_enabled()) {
        return result;
    }

    uint64_t const START = anonymous_reclaim_cursor.fetch_add(SCAN_BUDGET, std::memory_order_relaxed);
    while (result.scanned < SCAN_BUDGET && result.reclaimed < PAGE_BUDGET) {
        uint64_t const CURSOR = START + result.scanned;
        auto* task = sched::get_active_task_at_safe(static_cast<uint32_t>(CURSOR % TASK_COUNT));
        ++result.scanned;
        if (task == nullptr) {
            continue;
        }
        AnonymousPageoutCandidate candidate{};
        auto disposition = snapshot_anonymous_candidate(task, CURSOR / TASK_COUNT, candidate);
        if (disposition == AnonymousCandidateDisposition::AGED) {
            // Give the owner a scheduling opportunity after the TLB flush. A
            // hot page sets Accessed again and receives another chance; a cold
            // page remains clear and can be selected on this bounded revisit.
            sched::kern_yield();
            disposition = snapshot_anonymous_candidate(task, CURSOR / TASK_COUNT, candidate);
        }
        task->release();
        if (disposition == AnonymousCandidateDisposition::SELECTED) {
            anonymous_swap_add(anonymous_swap_stats.resident_candidates);
            if (pageout_anonymous_candidate(candidate)) {
                ++result.reclaimed;
            }
        }
    }
    result.has_more = swap::swap_available();
    return result;
}

auto migrate_anonymous_swap_area(void* /*context*/, uint32_t area_id) -> int {
    for (;;) {
        AnonymousSwapMapping* target = nullptr;
        PageTable* target_pagemap = nullptr;
        vaddr_t target_vaddr = 0;
        size_t access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
        bool wait_for_inflight = false;
        {
            uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
            for (auto* mapping = anonymous_swap_mappings; mapping != nullptr; mapping = mapping->next) {
                if (mapping->object == nullptr || mapping->object->slot.area != area_id) {
                    continue;
                }
                if (mapping->state != AnonymousSwapRuntimeState::SWAPPED || mapping->pagein_queued) {
                    wait_for_inflight = true;
                    break;
                }
                if (!acquire_user_pagemap_access(mapping->pagemap, false, access_gate)) {
                    wait_for_inflight = true;
                    break;
                }
                mapping->generation = next_anonymous_swap_generation();
                mapping->pagein_queued = true;
                mapping->pagein_access_gate = access_gate;
                anonymous_swap_add(anonymous_swap_stats.pagein_attempts);
                target = mapping;
                target_pagemap = mapping->pagemap;
                target_vaddr = mapping->vaddr;
                break;
            }
            cow_pte_lock.unlock_irqrestore(COW_FLAGS);
        }

        if (target != nullptr) {
            service_anonymous_pagein(target);
            int error = 0;
            uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
            auto* remaining = find_anonymous_swap_mapping_locked(target_pagemap, target_vaddr);
            if (remaining != nullptr && remaining->state == AnonymousSwapRuntimeState::FAILED) {
                error = remaining->error < 0 ? remaining->error : -EIO;
            }
            cow_pte_lock.unlock_irqrestore(COW_FLAGS);
            if (error < 0) {
                return error;
            }
            continue;
        }
        if (!wait_for_inflight) {
            return 0;
        }
        sched::kern_yield();
    }
}

auto clone_anonymous_swap_mappings(PageTable* src, PageTable* dst) -> bool {
    uint64_t last_vaddr = 0;
    bool have_last = false;
    for (;;) {
        auto* child = new AnonymousSwapMapping{};
        if (child == nullptr) {
            return false;
        }

        bool found = false;
        {
            uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
            AnonymousSwapMapping* source = nullptr;
            for (auto* candidate = anonymous_swap_mappings; candidate != nullptr; candidate = candidate->next) {
                if (candidate->pagemap != src || (have_last && candidate->vaddr <= last_vaddr)) {
                    continue;
                }
                if (source == nullptr || candidate->vaddr < source->vaddr) {
                    source = candidate;
                }
            }
            if (source != nullptr &&
                (source->state == AnonymousSwapRuntimeState::SWAPPED || source->state == AnonymousSwapRuntimeState::FAILED) &&
                !source->pagein_queued) {
                uint64_t raw = source->original_raw;
                if ((raw & paging::PAGE_WRITE) != 0U || (raw & paging::PAGE_COW) != 0U) {
                    raw &= ~paging::PAGE_WRITE;
                    raw |= paging::PAGE_COW;
                    source->original_raw = raw;
                    if (auto* parent_entry = leaf_entry(src, source->vaddr);
                        parent_entry != nullptr && parent_entry->present == 0 && is_reserved_leaf(*parent_entry)) {
                        *parent_entry = pte_from_raw(anonymous_swap_marker_raw(raw));
                    }
                }

                *child = *source;
                child->pagemap = dst;
                child->waiters = nullptr;
                child->next = nullptr;
                child->pagein_queued = false;
                child->pagein_access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
                child->cancelled = false;
                if (child->object != nullptr) {
                    ++child->object->mapping_refs;
                }
                if (auto* child_entry = leaf_entry(dst, child->vaddr);
                    child_entry != nullptr && child_entry->present == 0 && is_reserved_leaf(*child_entry)) {
                    *child_entry = pte_from_raw(anonymous_swap_marker_raw(raw));
                }
                insert_anonymous_swap_mapping_locked(child);
                if (child->state == AnonymousSwapRuntimeState::SWAPPED) {
                    anonymous_swap_add(anonymous_swap_stats.swapped_pages);
                }
                last_vaddr = source->vaddr;
                have_last = true;
                found = true;
            }
            cow_pte_lock.unlock_irqrestore(COW_FLAGS);
        }

        if (!found) {
            delete child;
            return true;
        }
    }
}

void purge_anonymous_swap_mappings(PageTable* pagemap) {
    for (;;) {
        AnonymousSwapMapping* victim = nullptr;
        AnonymousSwapObject* object = nullptr;
        {
            uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
            for (auto* mapping = anonymous_swap_mappings; mapping != nullptr; mapping = mapping->next) {
                if (mapping->pagemap == pagemap) {
                    victim = mapping;
                    break;
                }
            }
            if (victim != nullptr) {
                // Pagemap exclusive ownership proves pageout/pagein has
                // relinquished its read-side gate before teardown gets here.
                if (victim->state == AnonymousSwapRuntimeState::SWAPPED) {
                    anonymous_swap_stats.swapped_pages.fetch_sub(1, std::memory_order_relaxed);
                }
                remove_anonymous_swap_mapping_locked(victim);
                object = detach_anonymous_swap_object_locked(victim);
            }
            cow_pte_lock.unlock_irqrestore(COW_FLAGS);
        }
        if (victim == nullptr) {
            return;
        }
        release_anonymous_swap_object(object);
        delete victim;
    }
}

paging::PageTable* kernel_pagemap;
limine_memmap_response* memmap_response;
limine_executable_file_response* kernel_file_response;
limine_executable_address_response* kernel_address_response;
bool kernel_nx_supported{};

#ifdef WOS_SELFTEST
enum class ExpectedPageFaultState : uint8_t {
    IDLE,
    CONFIGURING,
    ARMED,
    HANDLED,
};

struct ExpectedKernelPageFault {
    std::atomic<ExpectedPageFaultState> state{ExpectedPageFaultState::IDLE};
    vaddr_t fault_address{};
    vaddr_t fault_rip{};
    vaddr_t recovery_rip{};
    uint64_t expected_error{};
    uint64_t error_mask{};
};

ExpectedKernelPageFault expected_kernel_page_fault{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void selftest_require_mutable_pagemap_root(PageTable const* root, const char* phase, vaddr_t vaddr = 0);
#endif

constexpr size_t PAGE_TABLE_POOL_CAPACITY = 64;

struct PageTableReleaseOrigin {
    void* caller = nullptr;
    uint64_t owner_pid = 0;
};

struct PageTablePool {
    mod::sys::Spinlock lock;
    std::array<PageTable*, PAGE_TABLE_POOL_CAPACITY> pages{};
    // Each record lives exactly as long as its resident pool entry.
    std::array<PageTableReleaseOrigin, PAGE_TABLE_POOL_CAPACITY> origins{};
    size_t count = 0;
};

PageTablePool page_table_pool{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_table_pool_alloc_hits{0};    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_table_pool_alloc_misses{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_table_pool_releases{0};      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_table_pool_rejects{0};       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr size_t OWNED_FRAME_TABLE_CAPACITY = 131072;
constexpr size_t OWNED_FRAME_SHARD_COUNT = 64;
constexpr size_t OWNED_FRAME_PROBE_LIMIT = 16;
constexpr bool OWNED_FRAME_TRACKING_ENABLED = false;
static_assert((OWNED_FRAME_TABLE_CAPACITY & (OWNED_FRAME_TABLE_CAPACITY - 1)) == 0);
static_assert((OWNED_FRAME_SHARD_COUNT & (OWNED_FRAME_SHARD_COUNT - 1)) == 0);
static_assert(OWNED_FRAME_TABLE_CAPACITY % OWNED_FRAME_SHARD_COUNT == 0);
constexpr size_t OWNED_FRAME_SHARD_SIZE = OWNED_FRAME_TABLE_CAPACITY / OWNED_FRAME_SHARD_COUNT;
static_assert((OWNED_FRAME_SHARD_SIZE & (OWNED_FRAME_SHARD_SIZE - 1)) == 0);

struct OwnedFrameEntry {
    uint64_t phys_addr = 0;
    PageTable* pagemap = nullptr;
    uint64_t vaddr = 0;
};

struct OwnedFrameTable {
    std::array<mod::sys::Spinlock, OWNED_FRAME_SHARD_COUNT> locks{};
    std::array<OwnedFrameEntry, OWNED_FRAME_TABLE_CAPACITY> entries{};
    std::atomic<uint64_t> tracked{0};
};

struct OwnedFrameStatsAtomic {
    std::atomic<uint64_t> track_attempts{0};
    std::atomic<uint64_t> track_added{0};
    std::atomic<uint64_t> track_replaced{0};
    std::atomic<uint64_t> track_skipped{0};
    std::atomic<uint64_t> track_conflicts{0};
    std::atomic<uint64_t> track_probe_failures{0};
    std::atomic<uint64_t> untrack_attempts{0};
    std::atomic<uint64_t> untrack_removed{0};
    std::atomic<uint64_t> untrack_missed{0};
    std::atomic<uint64_t> purge_calls{0};
    std::atomic<uint64_t> purge_removed{0};
};

OwnedFrameTable owned_frame_table{};        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
OwnedFrameStatsAtomic owned_frame_stats{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto is_current_pagemap(PageTable* pagemap) -> bool {
    if (pagemap == nullptr) {
        return false;
    }

    auto const PAGEMAP_PHYS = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(pagemap)));
    return PAGEMAP_PHYS == rdcr3();
}

constexpr size_t TLB_SHOOTDOWN_MAX_CPUS = desc::gdt::MAX_CPUS;

struct TlbShootdownRequest {
    std::atomic<uint64_t> generation{0};
    std::atomic<PageTable*> pagemap{nullptr};
    std::atomic<vaddr_t> vaddr{0};
    std::atomic<bool> reload_cr3{false};
    std::atomic<bool> shared_kernel{false};
    std::atomic<uint32_t> pending{0};
    std::array<std::atomic<uint8_t>, TLB_SHOOTDOWN_MAX_CPUS> targets{};
    std::array<std::atomic<uint64_t>, TLB_SHOOTDOWN_MAX_CPUS> observed{};
    std::array<std::atomic<uint64_t>, TLB_SHOOTDOWN_MAX_CPUS> completed{};
};

std::array<TlbShootdownRequest, TLB_SHOOTDOWN_MAX_CPUS>
    tlb_shootdown_requests{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<std::atomic<uint8_t>, TLB_SHOOTDOWN_MAX_CPUS>
    tlb_shootdown_cpu_online{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<std::atomic<PageTable*>, TLB_SHOOTDOWN_MAX_CPUS>
    tlb_active_pagemaps{};                                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> tlb_shootdown_init_attempted{false};    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> tlb_shootdown_late_acks{0};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint32_t> tlb_shootdown_gate_owner{0};        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> tlb_shootdown_gate_contentions{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint8_t tlb_shootdown_vector = 0;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr uint32_t TLB_SHOOTDOWN_GATE_FREE = 0;
constexpr uint64_t TLB_SHOOTDOWN_RETRY_US = 1'000;
constexpr uint64_t TLB_SHOOTDOWN_WAKE_RESCUE_US = 10'000;

struct TlbShootdownWaitSnapshot {
    uint64_t missing_mask_low{};
    uint32_t missing_count{};
    uint32_t unclaimed_count{};
    uint32_t first_missing{UINT32_MAX};
    uint32_t pending{};
};

struct TlbShootdownGateGuard {
    sched::task::Task* preempt_owner{};
    uint64_t cpu_no{UINT64_MAX};
    uint64_t interrupt_flags{};
    bool held{};
};

auto bounded_core_count() -> uint64_t {
    if (!smt::has_cpu_data()) {
        return 1;
    }
    return std::min<uint64_t>(smt::get_core_count(), TLB_SHOOTDOWN_MAX_CPUS);
}

auto cpu_slot_valid(uint64_t cpu_no) -> bool { return cpu_no < TLB_SHOOTDOWN_MAX_CPUS; }

auto user_pagemap_can_need_remote_shootdown(PageTable* pagemap) -> bool { return pagemap != nullptr && pagemap != kernel_pagemap; }

void note_active_pagemap(PageTable* pagemap) {
    if (!smt::has_cpu_data()) {
        return;
    }

    uint64_t const CPU_NO = cpu::current_cpu();
    if (!cpu_slot_valid(CPU_NO)) {
        return;
    }

    tlb_active_pagemaps[static_cast<size_t>(CPU_NO)].store(pagemap, std::memory_order_release);
}

auto cpu_may_have_active_pagemap(uint64_t cpu_no, PageTable* pagemap) -> bool {
    if (!cpu_slot_valid(cpu_no) || tlb_shootdown_cpu_online[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == 0U) {
        return false;
    }

    PageTable* const ACTIVE = tlb_active_pagemaps[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire);
    return ACTIVE == nullptr || ACTIVE == pagemap;
}

auto snapshot_tlb_shootdown_wait(TlbShootdownRequest& request, uint64_t core_count, uint64_t generation) -> TlbShootdownWaitSnapshot {
    TlbShootdownWaitSnapshot snapshot{};
    snapshot.pending = request.pending.load(std::memory_order_acquire);
    for (uint64_t cpu_no = 0; cpu_no < core_count; ++cpu_no) {
        if (request.targets[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == 0U) {
            continue;
        }

        if (request.observed[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == generation) {
            if (request.completed[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == generation) {
                continue;
            }
        } else {
            ++snapshot.unclaimed_count;
        }

        if (request.completed[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == generation) {
            continue;
        }

        if (cpu_no < 64) {
            snapshot.missing_mask_low |= 1ULL << cpu_no;
        }
        if (snapshot.first_missing == UINT32_MAX) {
            snapshot.first_missing = static_cast<uint32_t>(cpu_no);
        }
        ++snapshot.missing_count;
    }
    return snapshot;
}

void invalidate_local_tlb_if_current(PageTable* pagemap, vaddr_t vaddr, bool reload_cr3) {
    if (!is_current_pagemap(pagemap)) {
        return;
    }
    if (reload_cr3) {
        wrcr3(rdcr3());
        return;
    }
    invlpg(vaddr);
}

void service_tlb_shootdown_requests_for_cpu(uint64_t cpu_no) {
    if (!cpu_slot_valid(cpu_no) || tlb_shootdown_cpu_online[cpu_no].load(std::memory_order_acquire) == 0U) {
        return;
    }

    uint64_t const CORE_COUNT = bounded_core_count();
    for (uint64_t origin = 0; origin < CORE_COUNT; ++origin) {
        if (origin == cpu_no) {
            continue;
        }

        auto& request = tlb_shootdown_requests[static_cast<size_t>(origin)];
        uint64_t const GENERATION = request.generation.load(std::memory_order_acquire);
        if (GENERATION == 0) {
            continue;
        }
        if (request.targets[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == 0U ||
            request.generation.load(std::memory_order_acquire) != GENERATION) {
            continue;
        }

        auto& observed = request.observed[static_cast<size_t>(cpu_no)];
        uint64_t seen = observed.load(std::memory_order_acquire);
        if (seen == GENERATION) {
            continue;
        }
        if (!observed.compare_exchange_strong(seen, GENERATION, std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }

        bool const SHARED_KERNEL = request.shared_kernel.load(std::memory_order_acquire);
        PageTable* const TARGET_PAGEMAP = request.pagemap.load(std::memory_order_acquire);
        vaddr_t const TARGET_VADDR = request.vaddr.load(std::memory_order_acquire);
        bool const RELOAD_CR3 = request.reload_cr3.load(std::memory_order_acquire);
        if (request.generation.load(std::memory_order_acquire) != GENERATION) {
            continue;
        }
        if (SHARED_KERNEL) {
            // Kernel-half page tables are shared by every task pagemap. A full
            // local reload invalidates stale leaves regardless of which task
            // CR3 happens to be active on this CPU.
            wrcr3(rdcr3());
        } else if (TARGET_PAGEMAP != nullptr) {
            invalidate_local_tlb_if_current(TARGET_PAGEMAP, TARGET_VADDR, RELOAD_CR3);
        }

        uint32_t pending = request.pending.load(std::memory_order_acquire);
        while (pending != 0U) {
            if (request.pending.compare_exchange_weak(pending, pending - 1U, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        if (pending == 0U) {
            tlb_shootdown_late_acks.fetch_add(1, std::memory_order_relaxed);
        }
        request.completed[static_cast<size_t>(cpu_no)].store(GENERATION, std::memory_order_release);
    }
}

auto acquire_tlb_shootdown_gate() -> TlbShootdownGateGuard {
    uint64_t spins = 0;
    bool counted_contention = false;

    for (;;) {
        uint64_t interrupt_flags = 0;
        asm volatile("pushfq; popq %0; cli" : "=r"(interrupt_flags)::"memory");
        auto* const PREEMPT_OWNER = sched::preempt_disable_token_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
        uint64_t const CPU_NO = cpu::current_cpu();
        if (!cpu_slot_valid(CPU_NO)) {
            sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
            if ((interrupt_flags & cpu::GATE_IF_MASK) != 0U) {
                asm volatile("sti" ::: "memory");
            }
            return {};
        }

        auto const OWNER_ID = static_cast<uint32_t>(CPU_NO + 1U);
        uint32_t expected = TLB_SHOOTDOWN_GATE_FREE;
        if (tlb_shootdown_gate_owner.compare_exchange_weak(expected, OWNER_ID, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return TlbShootdownGateGuard{
                .preempt_owner = PREEMPT_OWNER,
                .cpu_no = CPU_NO,
                .interrupt_flags = interrupt_flags,
                .held = true,
            };
        }

        service_tlb_shootdown_requests_for_cpu(CPU_NO);
        sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
        if ((interrupt_flags & cpu::GATE_IF_MASK) != 0U) {
            asm volatile("sti" ::: "memory");
        }

        if (!counted_contention) {
            tlb_shootdown_gate_contentions.fetch_add(1, std::memory_order_relaxed);
            counted_contention = true;
        }

        ++spins;
        if ((spins & 0xFFFU) == 0U) {
            if (sched::preempt_count() == 0 && sched::interrupts_enabled()) {
                sched::kern_yield();
            }
        }
        asm volatile("pause" ::: "memory");
    }
}

void release_tlb_shootdown_gate(TlbShootdownGateGuard& guard) {
    if (guard.held) {
        tlb_shootdown_gate_owner.store(TLB_SHOOTDOWN_GATE_FREE, std::memory_order_release);
        guard.held = false;
    }
    sched::preempt_enable_token_at(guard.preempt_owner, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    if ((guard.interrupt_flags & cpu::GATE_IF_MASK) != 0U) {
        asm volatile("sti" ::: "memory");
    }
}

void send_tlb_shootdown_ipi(uint64_t cpu_no) {
    if (tlb_shootdown_vector == 0 || !smt::has_cpu_data() || cpu_no >= smt::get_core_count()) {
        return;
    }

    apic::IPIConfig const IPI{
        .vector = tlb_shootdown_vector,
        .delivery_mode = apic::IPIDeliveryMode::FIXED,
        .destination_mode = apic::IPIDestinationMode::PHYSICAL,
        .level = apic::IPILevel::ASSERT,
        .trigger_mode = apic::IPITriggerMode::EDGE,
        .destination_shorthand = apic::IPIDestinationShorthand::NONE,
    };

    apic::send_ipi(IPI, smt::get_cpu(cpu_no).lapic_id);
}

void retry_tlb_shootdown_wait(TlbShootdownRequest& request, uint64_t core_count, uint64_t generation, bool wake_rescue) {
    TlbShootdownWaitSnapshot const SNAPSHOT = snapshot_tlb_shootdown_wait(request, core_count, generation);
    if (SNAPSHOT.missing_count == 0) {
        return;
    }

    for (uint64_t target_cpu = 0; target_cpu < core_count; ++target_cpu) {
        if (request.targets[static_cast<size_t>(target_cpu)].load(std::memory_order_acquire) == 0U) {
            continue;
        }
        if (request.completed[static_cast<size_t>(target_cpu)].load(std::memory_order_acquire) == generation) {
            continue;
        }
        send_tlb_shootdown_ipi(target_cpu);
        if (wake_rescue) {
            sched::wake_cpu(target_cpu, sched::WakeCpuMode::FORCE);
        }
    }
}

void wait_for_tlb_shootdown_completion(TlbShootdownRequest& request, uint64_t origin_cpu, uint64_t generation, uint64_t core_count) {
    uint64_t const START_US = time::get_us();
    uint64_t next_retry_us = START_US + TLB_SHOOTDOWN_RETRY_US;
    uint64_t next_wake_rescue_us = START_US + TLB_SHOOTDOWN_WAKE_RESCUE_US;
    uint64_t spins = 0;

    for (;;) {
        if (request.pending.load(std::memory_order_acquire) == 0U &&
            snapshot_tlb_shootdown_wait(request, core_count, generation).missing_count == 0U) {
            break;
        }

        service_tlb_shootdown_requests_for_cpu(origin_cpu);
        ++spins;
        if ((spins & 0xFFFU) == 0) {
            TlbShootdownWaitSnapshot const SNAPSHOT = snapshot_tlb_shootdown_wait(request, core_count, generation);
            if (SNAPSHOT.missing_count == 0U) {
                if (SNAPSHOT.pending != 0U) {
                    request.pending.store(0U, std::memory_order_release);
                }
                break;
            }

            uint64_t const NOW_US = time::get_us();
            if (NOW_US >= next_retry_us) {
                bool const WAKE_RESCUE = NOW_US >= next_wake_rescue_us;
                retry_tlb_shootdown_wait(request, core_count, generation, WAKE_RESCUE);
                next_retry_us = NOW_US + TLB_SHOOTDOWN_RETRY_US;
                if (WAKE_RESCUE) {
                    next_wake_rescue_us = NOW_US + TLB_SHOOTDOWN_WAKE_RESCUE_US;
                }
            }
        }
        asm volatile("pause" ::: "memory");
    }
}

void shootdown_remote_user_pagemap(PageTable* pagemap, vaddr_t vaddr, bool reload_cr3) {
    if (!user_pagemap_can_need_remote_shootdown(pagemap) || tlb_shootdown_vector == 0 || !smt::has_cpu_data()) {
        return;
    }

    auto gate = acquire_tlb_shootdown_gate();
    if (!gate.held) {
        return;
    }

    uint64_t const ORIGIN_CPU = gate.cpu_no;
    uint64_t const CORE_COUNT = bounded_core_count();
    auto& request = tlb_shootdown_requests[static_cast<size_t>(ORIGIN_CPU)];
    uint64_t next_generation = request.generation.exchange(0, std::memory_order_acq_rel) + 1;
    if (next_generation == 0) {
        next_generation = 1;
    }

    uint32_t pending = 0;
    for (uint64_t target_cpu = 0; target_cpu < CORE_COUNT; ++target_cpu) {
        bool const TARGET = target_cpu != ORIGIN_CPU && cpu_may_have_active_pagemap(target_cpu, pagemap);
        request.targets[static_cast<size_t>(target_cpu)].store(TARGET ? 1U : 0U, std::memory_order_release);
        if (TARGET) {
            ++pending;
        }
    }

    if (pending == 0) {
        request.pending.store(0, std::memory_order_relaxed);
        request.generation.store(next_generation, std::memory_order_release);
        release_tlb_shootdown_gate(gate);
        return;
    }

    request.pagemap.store(pagemap, std::memory_order_relaxed);
    request.vaddr.store(vaddr, std::memory_order_relaxed);
    request.reload_cr3.store(reload_cr3, std::memory_order_relaxed);
    request.shared_kernel.store(false, std::memory_order_relaxed);
    request.pending.store(pending, std::memory_order_release);
    request.generation.store(next_generation, std::memory_order_release);

    for (uint64_t target_cpu = 0; target_cpu < CORE_COUNT; ++target_cpu) {
        if (request.targets[static_cast<size_t>(target_cpu)].load(std::memory_order_relaxed) != 0U) {
            send_tlb_shootdown_ipi(target_cpu);
        }
    }

    wait_for_tlb_shootdown_completion(request, ORIGIN_CPU, next_generation, CORE_COUNT);
    request.pagemap.store(nullptr, std::memory_order_release);
    release_tlb_shootdown_gate(gate);
}

void shootdown_shared_kernel_mappings() {
    // Before the shootdown vector/SMP state exists, only the BSP can have used
    // these runtime mappings. Reload its current CR3 and return.
    wrcr3(rdcr3());
    if (tlb_shootdown_vector == 0 || !smt::has_cpu_data()) {
        return;
    }

    auto gate = acquire_tlb_shootdown_gate();
    if (!gate.held) {
        return;
    }

    uint64_t const ORIGIN_CPU = gate.cpu_no;
    uint64_t const CORE_COUNT = bounded_core_count();
    auto& request = tlb_shootdown_requests[static_cast<size_t>(ORIGIN_CPU)];
    uint64_t next_generation = request.generation.exchange(0, std::memory_order_acq_rel) + 1;
    if (next_generation == 0) {
        next_generation = 1;
    }

    uint32_t pending = 0;
    for (uint64_t target_cpu = 0; target_cpu < CORE_COUNT; ++target_cpu) {
        bool const TARGET =
            target_cpu != ORIGIN_CPU && tlb_shootdown_cpu_online[static_cast<size_t>(target_cpu)].load(std::memory_order_acquire) != 0U;
        request.targets[static_cast<size_t>(target_cpu)].store(TARGET ? 1U : 0U, std::memory_order_release);
        if (TARGET) {
            ++pending;
        }
    }

    if (pending == 0) {
        request.pending.store(0, std::memory_order_relaxed);
        request.generation.store(next_generation, std::memory_order_release);
        release_tlb_shootdown_gate(gate);
        return;
    }

    request.pagemap.store(kernel_pagemap, std::memory_order_relaxed);
    request.vaddr.store(0, std::memory_order_relaxed);
    request.reload_cr3.store(true, std::memory_order_relaxed);
    request.shared_kernel.store(true, std::memory_order_relaxed);
    request.pending.store(pending, std::memory_order_release);
    request.generation.store(next_generation, std::memory_order_release);

    for (uint64_t target_cpu = 0; target_cpu < CORE_COUNT; ++target_cpu) {
        if (request.targets[static_cast<size_t>(target_cpu)].load(std::memory_order_relaxed) != 0U) {
            send_tlb_shootdown_ipi(target_cpu);
        }
    }

    wait_for_tlb_shootdown_completion(request, ORIGIN_CPU, next_generation, CORE_COUNT);
    request.shared_kernel.store(false, std::memory_order_release);
    request.pagemap.store(nullptr, std::memory_order_release);
    release_tlb_shootdown_gate(gate);
}

void flush_pagemap_after_update(PageTable* pagemap, vaddr_t vaddr, bool reload_cr3) {
    invalidate_local_tlb_if_current(pagemap, vaddr, reload_cr3);
    shootdown_remote_user_pagemap(pagemap, vaddr, reload_cr3);
}

void tlb_shootdown_handler([[maybe_unused]] cpu::GPRegs gpr, [[maybe_unused]] gates::InterruptFrame frame) {
    service_tlb_shootdown_requests_for_cpu(cpu::current_cpu());
}

void zero_page_table_for_pool(PageTable* table) {
    uint64_t saved_cr3 = 0;
    if (kernel_pagemap != nullptr) {
        auto const KERNEL_CR3 = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(kernel_pagemap)));
        uint64_t const CURRENT_CR3 = rdcr3();
        if (CURRENT_CR3 != KERNEL_CR3) {
            saved_cr3 = CURRENT_CR3;
            wrcr3(KERNEL_CR3);
        }
    }

    std::memset(table, 0, sizeof(PageTable));

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }
}

auto try_alloc_page_table_from_pool() -> PageTable* {
    auto& pool = page_table_pool;
    uint64_t const FLAGS = pool.lock.lock_irqsave();
    if (pool.count == 0) {
        pool.lock.unlock_irqrestore(FLAGS);
        page_table_pool_alloc_misses.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    --pool.count;
    auto* table = pool.pages[pool.count];
    pool.pages[pool.count] = nullptr;
    pool.origins[pool.count] = {};
    pool.lock.unlock_irqrestore(FLAGS);

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(table, "page-table-pool-alloc");
#endif

    if (!phys::page_reassign_owner(table, PhysicalPageOwner::PAGE_TABLE)) {
        log::critical("page-table pool could not restore active ownership page=%p", table);
        hcf();
    }
    if (phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1U) {
        log::critical("page-table pool handed out corrupt page table page=%p", table);
        hcf();
    }

    page_table_pool_alloc_hits.fetch_add(1, std::memory_order_relaxed);
    return table;
}

auto try_release_page_table_to_pool(PageTable* table, PageTableReleaseOrigin origin) -> bool {
    if (table == nullptr) {
        return true;
    }

    if (phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1U) {
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    auto& pool = page_table_pool;
    uint64_t const FLAGS = pool.lock.lock_irqsave();
    for (size_t i = 0; i < pool.count; ++i) {
        if (pool.pages.at(i) == table) {
            auto const PREVIOUS = pool.origins.at(i);
            pool.lock.unlock_irqrestore(FLAGS);
            page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
            log::critical("duplicate page-table pool release page=%p previous_pid=%lu previous_caller=%p owner_pid=%lu caller=%p", table,
                          PREVIOUS.owner_pid, PREVIOUS.caller, origin.owner_pid, origin.caller);
            return true;
        }
    }
    if (pool.count >= pool.pages.size()) {
        pool.lock.unlock_irqrestore(FLAGS);
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    pool.lock.unlock_irqrestore(FLAGS);

    zero_page_table_for_pool(table);

    if (!phys::page_reassign_owner(table, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE)) {
        log::critical("page-table pool could not assign reserve ownership page=%p", table);
        hcf();
    }

    uint64_t const INSERT_FLAGS = pool.lock.lock_irqsave();
    for (size_t i = 0; i < pool.count; ++i) {
        if (pool.pages.at(i) == table) {
            auto const PREVIOUS = pool.origins.at(i);
            pool.lock.unlock_irqrestore(INSERT_FLAGS);
            page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
            log::critical("raced duplicate page-table pool release page=%p previous_pid=%lu previous_caller=%p owner_pid=%lu caller=%p",
                          table, PREVIOUS.owner_pid, PREVIOUS.caller, origin.owner_pid, origin.caller);
            return true;
        }
    }
    if (pool.count >= pool.pages.size()) {
        pool.lock.unlock_irqrestore(INSERT_FLAGS);
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        if (!phys::page_reassign_owner(table, PhysicalPageOwner::PAGE_TABLE)) {
            log::critical("page-table pool could not restore rejected ownership page=%p", table);
            hcf();
        }
        return false;
    }
    pool.pages[pool.count] = table;
    pool.origins[pool.count] = origin;
    ++pool.count;
    pool.lock.unlock_irqrestore(INSERT_FLAGS);
    page_table_pool_releases.fetch_add(1, std::memory_order_relaxed);
    return true;
}

auto alloc_zeroed_page_table(bool may_fail = false) -> PageTable* {
    auto* pooled_table = try_alloc_page_table_from_pool();
    if (pooled_table != nullptr) {
        return pooled_table;
    }

    auto* table = static_cast<PageTable*>(
        may_fail ? phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner::PAGE_TABLE, "page_table")
                 : phys::page_alloc_full_overwrite_page_with_reclaim(PhysicalPageOwner::PAGE_TABLE, "page_table"));
    if (table == nullptr) {
        return nullptr;
    }

    // Clear the private HHDM page before kind tagging or hierarchy publication,
    // keeping the current pagemap instead of switching CR3 solely to zero it.
    std::memset(table, 0, paging::PAGE_SIZE);
    (void)phys::page_mark_kind(table, PageKind::PAGE_TABLE);
    return table;
}

auto user_root_control_bucket(PageTable* root) -> size_t {
    uint64_t const PAGE = reinterpret_cast<uint64_t>(root) >> paging::PAGE_SHIFT;
    return static_cast<size_t>((PAGE ^ (PAGE >> 16U) ^ (PAGE >> 32U)) & (USER_ROOT_CONTROL_BUCKET_COUNT - 1));
}

auto find_user_root_control_locked(PageTable* root) -> UserRootControl* {
    for (auto* control = user_root_control_buckets[user_root_control_bucket(root)]; control != nullptr; control = control->hash_next) {
        if (control->root == root) return control;
    }
    return nullptr;
}

auto next_user_root_serial(uint64_t& last_serial, uint64_t& serial) -> bool {
    if (last_serial == UINT64_MAX) return false;
    serial = ++last_serial;
    return true;
}

enum class UserRootAttachResult : uint8_t {
    ATTACHED,
    NEED_CHUNK,
    SERIAL_EXHAUSTED,
    DUPLICATE,
};

auto attach_user_root_control_locked(PageTable* root, UserRootControlChunk*& spare) -> UserRootAttachResult {
    if (find_user_root_control_locked(root) != nullptr) return UserRootAttachResult::DUPLICATE;
    if (user_root_control_last_serial == UINT64_MAX) return UserRootAttachResult::SERIAL_EXHAUSTED;
    if (user_root_control_available == nullptr) {
        if (spare == nullptr) return UserRootAttachResult::NEED_CHUNK;
        user_root_control_available = spare;
        spare = nullptr;  // Ownership transfers only with the locked admission.
        ++user_root_control_pages;
    }
    auto* chunk = user_root_control_available;
    auto* control = chunk->free_list;
    chunk->free_list = control->free_next;
    ++chunk->active_count;
    if (chunk->free_list == nullptr) {
        user_root_control_available = chunk->available_next;
        chunk->available_next = nullptr;
    }
    uint64_t serial = 0;
    static_cast<void>(next_user_root_serial(user_root_control_last_serial, serial));
    control->root = root;
    control->serial = serial;
    control->free_next = nullptr;
    auto& bucket = user_root_control_buckets[user_root_control_bucket(root)];
    control->hash_next = bucket;
    bucket = control;
    ++user_root_control_active_roots;
    return UserRootAttachResult::ATTACHED;
}

auto allocate_user_root_control_chunk() -> UserRootControlChunk* {
    // This raw fallible allocator does not directly reclaim/yield, but can
    // pressure-wake a worker. It must remain outside every mapping/bucket lock.
    void* page = phys::page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner::USER_ROOT_CONTROL, "user_root_control");
    if (page == nullptr) return nullptr;
    std::memset(page, 0, paging::PAGE_SIZE);
    auto* chunk = new (page) UserRootControlChunk{};
    for (auto& control : chunk->controls) {
        control.chunk = chunk;
        control.free_next = chunk->free_list;
        chunk->free_list = &control;
    }
    return chunk;
}

struct RootReserveLocality {
    uint64_t cpu{UINT64_MAX};
    numa::NodeId node{numa::INVALID_NODE};
};

auto capture_root_reserve_locality() -> RootReserveLocality {
    RootReserveLocality result;
    {
        sched::PreemptGuard const PREEMPT;
        auto const* topology = acpi::numa::boot_topology();
        result.cpu = cpu::get_current_cpu_id_safe();
        if (topology != nullptr && result.cpu < topology->cpu_count && result.cpu < topology->cpu_nodes.size()) {
            auto const NODE = topology->cpu_nodes[static_cast<size_t>(result.cpu)];
            if (NODE < topology->node_count) result.node = NODE;
        }
    }  // Never carry preemption exclusion into reclaim, allocation or TLB work.
    return result;
}

auto return_root_to_emergency_reserve(PageTable* root) -> bool {
    auto const RETURNED = numa::root_reserve::try_return(root, zero_page_table_for_pool);
    if (RETURNED == numa::root_reserve::ReturnResult::CORRUPT) dbg::panic_handler("corrupt emergency root return");
    return RETURNED == numa::root_reserve::ReturnResult::RETURNED;
}

void release_unregistered_user_root(PageTable* root) {
    // The factory owns this zeroed, unpublished root; it has never returned a
    // pointer or registered a control. No reader/exclusive gate can be needed.
    if (return_root_to_emergency_reserve(root)) return;
    if (!try_release_page_table_to_pool(root, {__builtin_return_address(0), 0})) phys::page_free(root);
}

auto create_user_root_common(bool may_fail
#ifdef WOS_SELFTEST
                             ,
                             const SelftestRootFactoryOptions* options = nullptr, SelftestRootFactoryOutput* output = nullptr
#endif
                             ) -> PageTable* {
#ifdef WOS_SELFTEST
    ker::mod::mm::lifecycle_census::Operation selftest_operation(selftest_root_epoch);
#endif
    auto const LOCALITY = capture_root_reserve_locality();
    auto requested = LOCALITY.node;
    bool inject_ordinary_failure = false;
#ifdef WOS_SELFTEST
    if (output != nullptr) {
        output->captured_cpu = LOCALITY.cpu;
        output->captured_local = LOCALITY.node;
    }
    if (options != nullptr) {
        inject_ordinary_failure = options->force_ordinary_failure;
        if (options->use_node_override) {
            auto const* topology = acpi::numa::boot_topology();
            if (output != nullptr) output->override_used = true;
            if (topology == nullptr || options->node_override >= topology->node_count) {
                if (output != nullptr) output->reserve_error = numa::root_reserve::Error::INVALID_NODE;
                return nullptr;
            }
            requested = options->node_override;
        }
    }
    if (output != nullptr) {
        output->requested = requested;
        output->ordinary_failure_injected = inject_ordinary_failure;
        output->ordinary_allocator_called = !inject_ordinary_failure;
    }
#endif
    // Ordinary global pool/fallible PMM is always first. The scoped diagnostic
    // branch skips that actual call without consuming or draining the old pool.
    auto* root = inject_ordinary_failure ? nullptr : alloc_zeroed_page_table(true);
    bool from_reserve = false;
    if (root == nullptr && requested != numa::INVALID_NODE) {
        auto const RESULT = numa::root_reserve::try_take(requested);
#ifdef WOS_SELFTEST
        if (output != nullptr) {
            output->reserve_attempted = true;
            output->reserve_error = RESULT.error;
        }
#endif
        if (RESULT.error == numa::root_reserve::Error::CORRUPT || RESULT.error == numa::root_reserve::Error::INVALID_NODE ||
            (RESULT.error == numa::root_reserve::Error::NONE) != (RESULT.table != nullptr))
            dbg::panic_handler("corrupt emergency root take");
        root = RESULT.table;
        from_reserve = root != nullptr;
    }
    if (root == nullptr && !may_fail) root = alloc_zeroed_page_table(false);
    if (root == nullptr) return nullptr;
#ifndef WOS_SELFTEST
    static_cast<void>(from_reserve);
#endif
    UserRootControlChunk* spare = nullptr;
    for (;;) {
        uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
        auto const RESULT = attach_user_root_control_locked(root, spare);
        user_mapping_pin_lock.unlock_irqrestore(FLAGS);
        if (RESULT == UserRootAttachResult::ATTACHED) {
            if (spare != nullptr) phys::page_free(spare);  // Another creator supplied capacity.
#ifdef WOS_SELFTEST
            if (output != nullptr) output->control_registered = true;
#endif
            return root;
        }
        if (RESULT == UserRootAttachResult::DUPLICATE) {
            // A duplicate contradicts the root allocator's private ownership.
            // Never recycle a page whose existing control may own a live root.
            log::critical("duplicate user-root admission root=%p", root);
            hcf();
        }
        if (RESULT == UserRootAttachResult::SERIAL_EXHAUSTED) {
            if (spare != nullptr) phys::page_free(spare);
            release_unregistered_user_root(root);
#ifdef WOS_SELFTEST
            if (output != nullptr) output->rollback_returned = from_reserve;
#endif
            return nullptr;
        }
#ifdef WOS_SELFTEST
        if (output != nullptr) output->need_chunk_seen = true;
        if (options != nullptr && options->fail_needed_control_chunk) {
            if (output != nullptr) output->control_failure_injected = true;
            release_unregistered_user_root(root);
            if (output != nullptr) output->rollback_returned = from_reserve;
            return nullptr;
        }
#endif
        spare = allocate_user_root_control_chunk();
        if (spare == nullptr) {
            release_unregistered_user_root(root);
#ifdef WOS_SELFTEST
            if (output != nullptr) output->rollback_returned = from_reserve;
#endif
            return nullptr;
        }
    }
}

auto retire_user_root_control_locked(PageTable* root, UserRootControlChunk*& empty) -> bool {
    auto** link = &user_root_control_buckets[user_root_control_bucket(root)];
    while (*link != nullptr && (*link)->root != root) link = &(*link)->hash_next;
    if (*link == nullptr || empty != nullptr) return false;
    auto* control = *link;
    auto* chunk = control->chunk;
    if (control->serial == 0 || chunk == nullptr || chunk->active_count == 0 || chunk->active_count > USER_ROOT_CONTROLS_PER_PAGE ||
        user_root_control_active_roots == 0 || user_root_control_pages == 0) {
        return false;
    }
    bool const WAS_FULL = chunk->free_list == nullptr;
    if (WAS_FULL != (chunk->active_count == USER_ROOT_CONTROLS_PER_PAGE)) return false;
    auto** available_link = &user_root_control_available;
    if (!WAS_FULL) {
        while (*available_link != nullptr && *available_link != chunk) available_link = &(*available_link)->available_next;
        if (*available_link == nullptr) return false;
    }
    *link = control->hash_next;
    control->root = nullptr;
    control->serial = 0;
    control->hash_next = nullptr;
    control->free_next = chunk->free_list;
    chunk->free_list = control;
    --chunk->active_count;
    --user_root_control_active_roots;
    if (chunk->active_count == 0) {
        *available_link = chunk->available_next;
        chunk->available_next = nullptr;
        --user_root_control_pages;
        empty = chunk;  // No bucket/free-list/creator can still name this page.
    } else if (WAS_FULL) {
        chunk->available_next = user_root_control_available;
        user_root_control_available = chunk;
    }
    return true;
}

auto pte_raw(const paging::PageTableEntry& e) -> uint64_t {
    uint64_t val = 0;
    std::memcpy(&val, &e, sizeof(val));
    return val;
}

auto pte_from_raw(uint64_t raw) -> paging::PageTableEntry {
    paging::PageTableEntry e{};
    std::memcpy(&e, &raw, sizeof(e));
    return e;
}

void owned_frame_untrack_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr);

auto owned_frame_hash(uint64_t phys_addr) -> size_t {
    uint64_t page = phys_addr >> paging::PAGE_SHIFT;
    page ^= page >> 33U;
    page *= 0xff51afd7ed558ccdULL;
    page ^= page >> 33U;
    return static_cast<size_t>(page);
}

auto owned_frame_shard_index(size_t hash) -> size_t { return hash & (OWNED_FRAME_SHARD_COUNT - 1); }

auto owned_frame_probe_index(size_t shard, size_t hash, size_t probe) -> size_t {
    size_t const SHARD_BASE = shard * OWNED_FRAME_SHARD_SIZE;
    size_t const START = (hash >> 6U) & (OWNED_FRAME_SHARD_SIZE - 1);
    return SHARD_BASE + ((START + probe) & (OWNED_FRAME_SHARD_SIZE - 1));
}

auto owned_frame_is_private_candidate(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr, uint64_t flags) -> bool {
    if (pagemap == nullptr || pagemap == kernel_pagemap || paddr == 0) {
        return false;
    }
    if (vaddr >= 0x0000800000000000ULL) {
        return false;
    }
    if ((flags & paging::PAGE_PRESENT) == 0U || (flags & paging::PAGE_USER) == 0U) {
        return false;
    }
    if ((flags & (paging::PAGE_COW | paging::PAGE_SHARED)) != 0U) {
        return false;
    }
    return true;
}

void owned_frame_insert_private_mapping(PageTable* pagemap, vaddr_t vaddr, uint64_t phys_addr) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)phys_addr;
        return;
    }

    auto& table = owned_frame_table;
    size_t const HASH = owned_frame_hash(phys_addr);
    size_t const SHARD = owned_frame_shard_index(HASH);
    auto& lock = table.locks[SHARD];
    uint64_t const IRQ_FLAGS = lock.lock_irqsave();
    OwnedFrameEntry* first_empty = nullptr;
    for (size_t probe = 0; probe < OWNED_FRAME_PROBE_LIMIT; ++probe) {
        auto& entry = table.entries[owned_frame_probe_index(SHARD, HASH, probe)];
        if (entry.pagemap == nullptr) {
            if (first_empty == nullptr) {
                first_empty = &entry;
            }
            continue;
        }
        if (entry.phys_addr != phys_addr) {
            continue;
        }

        uint64_t const PAGE_VADDR = vaddr & ~(paging::PAGE_SIZE - 1);
        if (entry.pagemap == pagemap && entry.vaddr == PAGE_VADDR) {
            owned_frame_stats.track_replaced.fetch_add(1, std::memory_order_relaxed);
            lock.unlock_irqrestore(IRQ_FLAGS);
            return;
        }

        entry = {};
        table.tracked.fetch_sub(1, std::memory_order_relaxed);
        owned_frame_stats.track_conflicts.fetch_add(1, std::memory_order_relaxed);
        lock.unlock_irqrestore(IRQ_FLAGS);
        return;
    }

    if (first_empty != nullptr) {
        first_empty->phys_addr = phys_addr;
        first_empty->pagemap = pagemap;
        first_empty->vaddr = vaddr & ~(paging::PAGE_SIZE - 1);
        table.tracked.fetch_add(1, std::memory_order_relaxed);
        owned_frame_stats.track_added.fetch_add(1, std::memory_order_relaxed);
        lock.unlock_irqrestore(IRQ_FLAGS);
        return;
    }

    owned_frame_stats.track_probe_failures.fetch_add(1, std::memory_order_relaxed);
    lock.unlock_irqrestore(IRQ_FLAGS);
}

void owned_frame_track_private_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr, uint64_t flags) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)paddr;
        (void)flags;
        return;
    }

    uint64_t const PHYS_ADDR = paddr & ~(paging::PAGE_SIZE - 1);
    if (!owned_frame_is_private_candidate(pagemap, vaddr, PHYS_ADDR, flags)) {
        return;
    }

    owned_frame_stats.track_attempts.fetch_add(1, std::memory_order_relaxed);
    auto* page = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS_ADDR));
    phys::PageLookupHint lookup{};
    if (phys::page_kind_get(page, &lookup) != PageKind::NORMAL || phys::page_ref_get(page, &lookup) != 1U) {
        owned_frame_untrack_mapping(pagemap, vaddr, PHYS_ADDR);
        owned_frame_stats.track_skipped.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    owned_frame_insert_private_mapping(pagemap, vaddr, PHYS_ADDR);
}

void owned_frame_track_fresh_normal_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr, uint64_t flags) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)paddr;
        (void)flags;
        return;
    }

    uint64_t const PHYS_ADDR = paddr & ~(paging::PAGE_SIZE - 1);
    if (!owned_frame_is_private_candidate(pagemap, vaddr, PHYS_ADDR, flags)) {
        return;
    }

    owned_frame_stats.track_attempts.fetch_add(1, std::memory_order_relaxed);
    owned_frame_insert_private_mapping(pagemap, vaddr, PHYS_ADDR);
}

void owned_frame_untrack_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)paddr;
        return;
    }

    if (pagemap == nullptr || paddr == 0) {
        return;
    }

    owned_frame_stats.untrack_attempts.fetch_add(1, std::memory_order_relaxed);

    uint64_t const PHYS_ADDR = paddr & ~(paging::PAGE_SIZE - 1);
    uint64_t const PAGE_VADDR = vaddr & ~(paging::PAGE_SIZE - 1);
    auto& table = owned_frame_table;
    size_t const HASH = owned_frame_hash(PHYS_ADDR);
    size_t const SHARD = owned_frame_shard_index(HASH);
    auto& lock = table.locks[SHARD];
    uint64_t const IRQ_FLAGS = lock.lock_irqsave();
    for (size_t probe = 0; probe < OWNED_FRAME_PROBE_LIMIT; ++probe) {
        auto& entry = table.entries[owned_frame_probe_index(SHARD, HASH, probe)];
        if (entry.pagemap == nullptr) {
            continue;
        }
        if (entry.phys_addr != PHYS_ADDR) {
            continue;
        }
        if (entry.pagemap == pagemap && entry.vaddr == PAGE_VADDR) {
            entry = {};
            table.tracked.fetch_sub(1, std::memory_order_relaxed);
            owned_frame_stats.untrack_removed.fetch_add(1, std::memory_order_relaxed);
            lock.unlock_irqrestore(IRQ_FLAGS);
            return;
        }

        entry = {};
        table.tracked.fetch_sub(1, std::memory_order_relaxed);
        owned_frame_stats.track_conflicts.fetch_add(1, std::memory_order_relaxed);
        lock.unlock_irqrestore(IRQ_FLAGS);
        return;
    }

    owned_frame_stats.untrack_missed.fetch_add(1, std::memory_order_relaxed);
    lock.unlock_irqrestore(IRQ_FLAGS);
}

void owned_frame_untrack_leaf(PageTable* pagemap, vaddr_t vaddr, const PageTableEntry& entry) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)entry;
        return;
    }

    if (entry.frame == 0) {
        return;
    }
    owned_frame_untrack_mapping(pagemap, vaddr, static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT);
}

void owned_frame_refresh_leaf(PageTable* pagemap, vaddr_t vaddr, const PageTableEntry& entry) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        (void)vaddr;
        (void)entry;
        return;
    }

    if (entry.present == 0 || entry.frame == 0) {
        owned_frame_untrack_leaf(pagemap, vaddr, entry);
        return;
    }

    uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
    uint64_t const RAW = pte_raw(entry);
    if (owned_frame_is_private_candidate(pagemap, vaddr, PHYS_ADDR, RAW)) {
        owned_frame_track_private_mapping(pagemap, vaddr, PHYS_ADDR, RAW);
        return;
    }
    owned_frame_untrack_mapping(pagemap, vaddr, PHYS_ADDR);
}

void owned_frame_purge_pagemap(PageTable* pagemap) {
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)pagemap;
        return;
    }

    if (pagemap == nullptr) {
        return;
    }

    owned_frame_stats.purge_calls.fetch_add(1, std::memory_order_relaxed);

    uint64_t removed = 0;
    auto& table = owned_frame_table;
    for (size_t shard = 0; shard < OWNED_FRAME_SHARD_COUNT; ++shard) {
        auto& lock = table.locks[shard];
        uint64_t const IRQ_FLAGS = lock.lock_irqsave();
        size_t const SHARD_BASE = shard * OWNED_FRAME_SHARD_SIZE;
        for (size_t i = 0; i < OWNED_FRAME_SHARD_SIZE; ++i) {
            auto& entry = table.entries[SHARD_BASE + i];
            if (entry.pagemap != pagemap) {
                continue;
            }
            entry = {};
            ++removed;
        }
        lock.unlock_irqrestore(IRQ_FLAGS);
    }
    table.tracked.fetch_sub(removed, std::memory_order_relaxed);
    owned_frame_stats.purge_removed.fetch_add(removed, std::memory_order_relaxed);
}

auto index_of(const uint64_t VADDR, const int OFFSET) -> uint64_t { return VADDR >> (12 + (9 * (OFFSET - 1))) & 0x1FF; }

auto entry_at(PageTable* table, size_t index) -> PageTableEntry& { return table->entries[index]; }

auto entry_at(const PageTable* table, size_t index) -> const PageTableEntry& { return table->entries[index]; }

auto table_flags_for_leaf(const uint64_t FLAGS) -> uint64_t {
    uint64_t table_flags = paging::PAGE_PRESENT | paging::PAGE_WRITE;
    if ((FLAGS & paging::PAGE_USER) != 0U) {
        table_flags |= paging::PAGE_USER;
    }
    if ((FLAGS & paging::PAGE_PWT) != 0U) {
        table_flags |= paging::PAGE_PWT;
    }
    if ((FLAGS & paging::PAGE_PCD) != 0U) {
        table_flags |= paging::PAGE_PCD;
    }
    return table_flags;
}

auto is_aligned_to(uint64_t value, uint64_t alignment) -> bool { return (value & (alignment - 1)) == 0; }

auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted = nullptr) -> paging::PageTable*;

auto is_reserved_leaf(const PageTableEntry& entry) -> bool {
    uint64_t const RAW = pte_raw(entry);
    return entry.present == 0 && (RAW & paging::PAGE_RESERVED) != 0U;
}

auto leaf_entry(PageTable* root, vaddr_t vaddr) -> PageTableEntry* {
    if (root == nullptr) {
        return nullptr;
    }

    PageTable* table = root;
    for (int i = 4; i > 1; i--) {
        PageTableEntry& entry = entry_at(table, index_of(vaddr, i));
        if (!entry.present || entry.pagesize != 0) {
            return nullptr;  // A PS data frame is never a next-level table.
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    return &entry_at(table, index_of(vaddr, 1));
}

// BEGIN managed user-leaf scalar core. Actual table alignment, not packed
// PageTableEntry's declared alignment, is the atomic representation contract.
class MutableUserEntrySlot {
   public:
    [[gnu::always_inline]] static auto from(PageTable* table, size_t index, MutableUserEntrySlot& empty) noexcept -> bool {
        auto const base = reinterpret_cast<uintptr_t>(table);
        if (table == nullptr || (base & (paging::PAGE_SIZE - 1)) != 0 || index >= paging::PAGE_TABLE_ENTRIES || empty.word_ != nullptr)
            return false;
        auto const address = base + index * sizeof(uint64_t);
        if ((address & (alignof(uint64_t) - 1)) != 0) return false;
        empty.word_ = reinterpret_cast<uint64_t*>(address);
        return true;
    }
    [[gnu::always_inline]] auto load() const noexcept -> uint64_t { return __atomic_load_n(word_, __ATOMIC_ACQUIRE); }
    [[gnu::always_inline]] auto exchange(uint64_t& expected, uint64_t desired) const noexcept -> bool {
        return __atomic_compare_exchange_n(word_, &expected, desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    }
    [[gnu::always_inline]] void mark_dirty() const noexcept { (void)__atomic_fetch_or(word_, uint64_t{1} << 6, __ATOMIC_ACQ_REL); }

   private:
    uint64_t* word_{};
};
static_assert(sizeof(PageTableEntry) == 8 && offsetof(PageTable, entries) == 0);
enum class UserLeafKind : uint8_t { Absent, Pml1, ManagedPml2, Unsupported };
struct UserLeafResolution {
    UserLeafKind kind{};
    PageTable* slot_table{};
    uint16_t slot_index{};
    uint8_t level{};
    uint64_t raw{}, physical_4k{};
    huge::RootKey root{};
    huge::MappedLeaf managed{};
};
// Pin lock held; the caller owns actual root lifetime. No allocation, physical
// scan, topology access, or borrowed slot survives the enclosing pin section.
auto resolve_user_leaf_locked(PageTable* root, vaddr_t address, bool writable, UserLeafResolution& output) noexcept -> bool {
    if (root == nullptr || address >= (uint64_t{1} << 47)) return false;
    auto const* control = find_user_root_control_locked(root);
    huge::RootKey const key{reinterpret_cast<uint64_t>(root), control != nullptr ? control->serial : 0};
    PageTable* table = root;
    for (int level = 4; level >= 1; --level) {
        auto const index = index_of(address, level);
        MutableUserEntrySlot slot;
        if (!MutableUserEntrySlot::from(table, index, slot)) return false;
        uint64_t const raw = slot.load();
        if ((raw & paging::PAGE_PRESENT) == 0 || (raw & paging::PAGE_USER) == 0 ||
            (writable && ((raw & paging::PAGE_WRITE) == 0 || (raw & paging::PAGE_COW) != 0)))
            return false;
        UserLeafResolution result{};
        result.slot_table = table;
        result.slot_index = static_cast<uint16_t>(index);
        result.level = static_cast<uint8_t>(level);
        result.raw = raw;
        result.root = key;
        if (level > 1 && (raw & (uint64_t{1} << 7)) != 0) {
            if (level != 2 ||
                managed_huge_registry.lookup_mapped(key, address & ~(huge::LEAF_BYTES - 1), raw, result.managed) != huge::Error::None) {
                result.kind = UserLeafKind::Unsupported;
                output = result;
                return false;
            }
            result.kind = UserLeafKind::ManagedPml2;
            result.physical_4k = result.managed.key.pa_2m + (address & (huge::LEAF_BYTES - 1) & ~(paging::PAGE_SIZE - 1));
            output = result;
            return true;
        }
        if (level == 1) {
            if (writable && paging::shared_write_restricted(raw)) return false;
            result.kind = UserLeafKind::Pml1;
            result.physical_4k = raw & PTE_FRAME_MASK;
            output = result;
            return true;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(raw & PTE_FRAME_MASK));
    }
    return false;
}
// END managed user-leaf scalar core.

// BEGIN private managed publication/split. All actual owners are constructed
// by the instrumented caller. This excluded TU keeps only scalar views.
class HugePublicationViewStorage {
   public:
    HugePublicationViewStorage() {
#ifdef WOS_KASAN
        if (kasan::is_enabled()) kasan::unpoison_range(bytes_, sizeof(bytes_));
#endif
        view_ = ::new (static_cast<void*>(bytes_)) huge::detail::SpanPublicationView{};
    }
    auto get() -> huge::detail::SpanPublicationView& { return *view_; }

   private:
    alignas(huge::detail::SpanPublicationView) uint8_t bytes_[sizeof(huge::detail::SpanPublicationView)];
    huge::detail::SpanPublicationView* view_;
};
class ManagedRootReader {
   public:
    explicit ManagedRootReader(PageTable* root) { (void)acquire_user_pagemap_access(root, true, index_); }
    ~ManagedRootReader() { release_user_pagemap_access(index_); }
    auto valid() const -> bool { return index_ != USER_PAGEMAP_ACCESS_GATE_NONE; }
    ManagedRootReader(const ManagedRootReader&) = delete;
    auto operator=(const ManagedRootReader&) -> ManagedRootReader& = delete;

   private:
    size_t index_{USER_PAGEMAP_ACCESS_GATE_NONE};
};
enum class Pml2Walk : uint8_t { Absent, Slot, Unsupported };
// No ancestor promotion, allocation, root-reader acquisition or ownership scan.
auto managed_pml2_slot_locked(PageTable* root, uint64_t va, MutableUserEntrySlot& output) -> Pml2Walk {
    if (root == nullptr || va >= (uint64_t{1} << 47)) return Pml2Walk::Unsupported;
    auto* table = root;
    for (int level = 4; level > 2; --level) {
        MutableUserEntrySlot slot;
        if (!MutableUserEntrySlot::from(table, index_of(va, level), slot)) return Pml2Walk::Unsupported;
        uint64_t const raw = slot.load();
        if ((raw & paging::PAGE_PRESENT) == 0) return raw == 0 ? Pml2Walk::Absent : Pml2Walk::Unsupported;
        if ((raw & (uint64_t{1} << 7)) != 0) return Pml2Walk::Unsupported;
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(raw & PTE_FRAME_MASK));
    }
    return MutableUserEntrySlot::from(table, index_of(va, 2), output) ? Pml2Walk::Slot : Pml2Walk::Unsupported;
}
auto managed_root_matches_locked(huge::RootKey key) -> bool {
    auto const* control = find_user_root_control_locked(reinterpret_cast<PageTable*>(key.address_bits));
    return control != nullptr && key.incarnation != 0 && control->serial == key.incarnation;
}
auto managed_replace_ad_locked(const MutableUserEntrySlot& slot, uint64_t expected, uint64_t desired) -> bool {
    constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
    auto observed = slot.load();
    for (;;) {
        if ((observed & ~AD) != (expected & ~AD)) return false;
        uint64_t const replacement = (desired & ~AD) | (observed & AD);
        if (slot.exchange(observed, replacement)) return true;
    }
}
auto managed_table_owner_valid(PageTable* table) -> bool {
    return table != nullptr && (reinterpret_cast<uint64_t>(table) & (paging::PAGE_SIZE - 1)) == 0 &&
           phys::page_kind_get(table) == PageKind::PAGE_TABLE && phys::page_ref_get(table) == 1;
}
auto managed_plan_owners_valid(const huge::SplitPlanStorage& storage) -> bool {
    if (storage.table_count == 0 || storage.table_count > huge::MAX_PLAN_TABLES) return false;
    for (uint16_t i = 0; i < storage.table_count; ++i) {
        if (!managed_table_owner_valid(storage.tables[i]) || (storage.table_physical[i] & ~PTE_FRAME_MASK) != 0 ||
            storage.table_physical[i] != reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(storage.tables[i]))))
            return false;
        for (uint16_t j = 0; j < i; ++j)
            if (storage.tables[i] == storage.tables[j]) return false;
    }
    return true;
}
// Existing PUB -> COW -> pin, all outputs already addressable. An exclusive
// unpublished owner supplies preparation lifetime, not the copied RootKey.
auto survey_managed_split_locked(huge::RootKey key, uint64_t start, uint64_t end, huge::SplitPlanStorage* storage, uint16_t& count)
    -> huge::Error {
    if (!managed_root_matches_locked(key)) return huge::Error::Again;
    auto* root = reinterpret_cast<PageTable*>(key.address_bits);
    count = 0;
    for (uint64_t va = start; va < end; va += huge::LEAF_BYTES) {
        MutableUserEntrySlot slot;
        auto const walk = managed_pml2_slot_locked(root, va, slot);
        if (walk == Pml2Walk::Absent) continue;
        if (walk != Pml2Walk::Slot) return huge::Error::Unsupported;
        uint64_t const raw = slot.load();
        if ((raw & paging::PAGE_PRESENT) == 0 || (raw & (uint64_t{1} << 7)) == 0) continue;
        huge::MappedLeaf leaf{};
        if (managed_huge_registry.lookup_mapped(key, va, raw, leaf) != huge::Error::None) return huge::Error::Unsupported;
        if (count == huge::MAX_ROOT_LEAVES) return huge::Error::Capacity;
        if (storage != nullptr) {
            if (count >= storage->table_count) return huge::Error::Again;
            storage->descriptors[count] = {leaf.key, leaf.handle, leaf.semantic_word};
        }
        ++count;
    }
    return huge::Error::None;
}
// Heap/table allocation and instrumented preparation happen outside every
// publication/root-reader/MM guard. At most 2 complete EAGAIN retries.
[[maybe_unused]] auto prepare_managed_split_plan(PageTable* root, uint64_t start, uint64_t end, huge::PreparedTables& empty
#ifdef WOS_SELFTEST
                                                 ,
                                                 uint16_t fail_before_table = UINT16_MAX
#endif
                                                 ) -> huge::Error {
    if (!empty.empty() || !huge::RegistryCore::valid_va(start) || end <= start || end > (uint64_t{1} << 47) ||
        (end & (huge::LEAF_BYTES - 1)) != 0)
        return huge::Error::Invalid;
    if ((end - start) / huge::LEAF_BYTES > huge::MAX_ROOT_LEAVES) return huge::Error::Unsupported;
    huge::RootKey key{};
    if (auto const error = huge::detail::root_key(root, key); error != huge::Error::None) return error;
    for (unsigned attempt = 0; attempt < 3; ++attempt) {
        uint16_t required = 0;
        huge::Error error;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            uint64_t const cow = cow_pte_lock.lock_irqsave();
            uint64_t const pin = user_mapping_pin_lock.lock_irqsave();
            error = survey_managed_split_locked(key, start, end, nullptr, required);
            user_mapping_pin_lock.unlock_irqrestore(pin);
            cow_pte_lock.unlock_irqrestore(cow);
        }
        if (error != huge::Error::None) {
            if (error == huge::Error::Again) continue;
            return error;
        }
        if (required == 0) return huge::Error::None;
        auto const prepared =
#ifdef WOS_SELFTEST
            huge::detail::PreparationAccess::prepare_tables_for_test(empty, required, fail_before_table);
#else
            empty.prepare(required);
#endif
        if (prepared != huge::Error::None) return prepared;
        auto* storage = huge::detail::PreparationAccess::plan_storage(empty);
        if (storage == nullptr || !managed_plan_owners_valid(*storage)) {
            empty.reset();
            return huge::Error::Invalid;
        }
        uint16_t actual = 0;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            uint64_t const cow = cow_pte_lock.lock_irqsave();
            uint64_t const pin = user_mapping_pin_lock.lock_irqsave();
            error = survey_managed_split_locked(key, start, end, storage, actual);
            user_mapping_pin_lock.unlock_irqrestore(pin);
            cow_pte_lock.unlock_irqrestore(cow);
        }
        if (error == huge::Error::None) {
            if (actual == 0) {
                empty.reset();
                return huge::Error::None;
            }
            storage->root = key;
            storage->start = start;
            storage->end = end;
            storage->descriptor_count = actual;
            storage->purpose = huge::PlanPurpose::Split;
            storage->surveyed = true;
            // Private tables are filled before incoming locks. All original
            // WB anonymous P/W/U/NX semantics survive; A/D are conservative.
            constexpr uint64_t PERMISSIONS = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_NX;
            constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
            for (uint16_t i = 0; i < actual; ++i) {
                auto const& descriptor = storage->descriptors[i];
                auto* words = reinterpret_cast<uint64_t*>(storage->tables[i]);
                for (uint64_t page = 0; page < huge::LEAF_PAGES; ++page)
                    words[page] = (descriptor.key.pa_2m + page * paging::PAGE_SIZE) | (descriptor.semantic_word & PERMISSIONS) | AD;
            }
            return huge::Error::None;
        }
        empty.reset();
        if (error != huge::Error::Again) return error;
    }
    return huge::Error::Again;
}
// PUB is actual typed caller authority. The temporary root reader is compatible
// with already held stable pins; it excludes retirement, not representation.
[[maybe_unused]] auto commit_managed_split_plan(PageTable* root, huge::PreparedTables& plan,
                                                const ker::syscall::vmem::SharedVmemPublicationGuard&) -> huge::Error {
    auto* storage = huge::detail::PreparationAccess::plan_storage(plan);
    if (storage == nullptr || storage->purpose != huge::PlanPurpose::Split || !storage->surveyed ||
        storage->root.address_bits != reinterpret_cast<uint64_t>(root) || storage->descriptor_count == 0 ||
        storage->descriptor_count > storage->table_count || !huge::RegistryCore::valid_va(storage->start) ||
        storage->end <= storage->start || storage->end > (uint64_t{1} << 47) || (storage->end & (huge::LEAF_BYTES - 1)) != 0 ||
        (storage->end - storage->start) / huge::LEAF_BYTES > huge::MAX_ROOT_LEAVES || !managed_plan_owners_valid(*storage))
        return huge::Error::Invalid;
    ManagedRootReader reader(root);
    if (!reader.valid()) return huge::Error::Again;
    uint64_t const cow = cow_pte_lock.lock_irqsave();
    uint64_t const pin = user_mapping_pin_lock.lock_irqsave();
    uint16_t actual_count = 0;
    bool valid = survey_managed_split_locked(storage->root, storage->start, storage->end, nullptr, actual_count) == huge::Error::None &&
                 actual_count == storage->descriptor_count;
    // Validate the WHOLE plan before the first actual PTE/index mutation.
    for (uint16_t i = 0; valid && i < storage->descriptor_count; ++i) {
        auto const& d = storage->descriptors[i];
        MutableUserEntrySlot slot;
        valid = huge::RegistryCore::same_root(d.key.root, storage->root) && d.key.va_2m >= storage->start && d.key.va_2m < storage->end &&
                managed_pml2_slot_locked(root, d.key.va_2m, slot) == Pml2Walk::Slot &&
                managed_huge_registry.match_mapped(d.handle, d.key, slot.load()) &&
                (slot.load() & ~uint64_t{0x60}) == (d.semantic_word & ~uint64_t{0x60});
        if (i != 0 && storage->descriptors[i - 1].key.va_2m >= d.key.va_2m) valid = false;
    }
    if (!valid) {
        user_mapping_pin_lock.unlock_irqrestore(pin);
        cow_pte_lock.unlock_irqrestore(cow);
        return huge::Error::Again;
    }
    for (uint16_t i = 0; i < storage->descriptor_count; ++i) {
        auto const& d = storage->descriptors[i];
        MutableUserEntrySlot slot;
        if (managed_pml2_slot_locked(root, d.key.va_2m, slot) != Pml2Walk::Slot ||
            managed_huge_registry.begin_split(d.handle, d.key) != huge::Error::None)
            __builtin_trap();
        uint64_t const pt = storage->table_physical[i] | paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER;
        if (!managed_replace_ad_locked(slot, d.semantic_word, pt)) __builtin_trap();
    }
    user_mapping_pin_lock.unlock_irqrestore(pin);
    cow_pte_lock.unlock_irqrestore(cow);
    // No return/callback/wait between publication and local ownership transfer.
    uint64_t const adopted = storage->descriptor_count == 64 ? UINT64_MAX : (uint64_t{1} << storage->descriptor_count) - 1;
    huge::detail::PreparationAccess::tables_transferred_after_publish(plan, adopted);
    flush_pagemap_after_update(root, 0, true);
    // Only this lexical post-real-flush completion returns managed capacity.
    uint64_t const finish_cow = cow_pte_lock.lock_irqsave();
    uint64_t const finish_pin = user_mapping_pin_lock.lock_irqsave();
    if (!managed_root_matches_locked(storage->root)) __builtin_trap();
    for (uint16_t i = 0; i < storage->descriptor_count; ++i) {
        auto const& d = storage->descriptors[i];
        MutableUserEntrySlot current;
        constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
        uint64_t const expected_pt = storage->table_physical[i] | paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER;
        if (managed_pml2_slot_locked(root, d.key.va_2m, current) != Pml2Walk::Slot || (current.load() & ~AD) != expected_pt ||
            managed_huge_registry.complete_split_after_ack(d.handle) != huge::Error::None)
            __builtin_trap();
        for (uint64_t page = 0; page < huge::LEAF_PAGES; ++page)
            owned_frame_track_fresh_normal_mapping(root, d.key.va_2m + page * paging::PAGE_SIZE, d.key.pa_2m + page * paging::PAGE_SIZE,
                                                   d.semantic_word & ~(uint64_t{1} << 7));
    }
    user_mapping_pin_lock.unlock_irqrestore(finish_pin);
    cow_pte_lock.unlock_irqrestore(finish_cow);
    storage->surveyed = false;
    return huge::Error::None;
}
// An already owned cleanup table is filled/bound without allocation before
// the fixture may leave a published PS behind on ANY later failure path.
auto bind_preallocated_cleanup_split(PageTable* root, uint64_t va, huge::PreparedTables& cleanup) -> huge::Error {
    auto* storage = huge::detail::PreparationAccess::plan_storage(cleanup);
    if (storage == nullptr || storage->table_count != 1 || storage->purpose != huge::PlanPurpose::Unbound ||
        !managed_plan_owners_valid(*storage))
        return huge::Error::Invalid;
    huge::RootKey key{};
    if (auto const error = huge::detail::root_key(root, key); error != huge::Error::None) return error;
    huge::MappedLeaf leaf{};
    {
        uint64_t const cow = cow_pte_lock.lock_irqsave();
        uint64_t const pin = user_mapping_pin_lock.lock_irqsave();
        MutableUserEntrySlot slot;
        bool const valid = managed_root_matches_locked(key) && managed_pml2_slot_locked(root, va, slot) == Pml2Walk::Slot &&
                           managed_huge_registry.lookup_mapped(key, va, slot.load(), leaf) == huge::Error::None;
        user_mapping_pin_lock.unlock_irqrestore(pin);
        cow_pte_lock.unlock_irqrestore(cow);
        if (!valid) return huge::Error::Again;
    }
    storage->root = key;
    storage->start = va;
    storage->end = va + huge::LEAF_BYTES;
    storage->descriptor_count = 1;
    storage->descriptors[0] = {leaf.key, leaf.handle, leaf.semantic_word};
    storage->purpose = huge::PlanPurpose::Split;
    storage->surveyed = true;
    auto* words = reinterpret_cast<uint64_t*>(storage->tables[0]);
    for (uint64_t page = 0; page < huge::LEAF_PAGES; ++page)
        words[page] = (leaf.key.pa_2m + page * paging::PAGE_SIZE) |
                      (leaf.semantic_word & (paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_NX)) |
                      (uint64_t{1} << 5) | (uint64_t{1} << 6);
    return huge::Error::None;
}
// Span and both table owners are instrumented-caller constructed/addressable.
// Cleanup is validated before publication, then bound infallibly immediately
// after the actual mapping becomes owned. No public syscall reaches this path.
[[maybe_unused]] auto publish_managed_2m_prepared(PageTable* root, uint64_t va, uint64_t flags, huge::PreparedSpan& span,
                                                  huge::PreparedTables& parents, huge::PreparedTables& cleanup) -> huge::Error {
    constexpr uint64_t PERMISSIONS = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_NX;
    if (!huge::RegistryCore::valid_va(va) || (flags & paging::PAGE_PRESENT) == 0 || (flags & ~PERMISSIONS) != 0)
        return huge::Error::Invalid;
    auto* parent_storage = huge::detail::PreparationAccess::plan_storage(parents);
    auto* cleanup_storage = huge::detail::PreparationAccess::plan_storage(cleanup);
    if (parent_storage == nullptr || cleanup_storage == nullptr || parent_storage == cleanup_storage || parent_storage->table_count != 2 ||
        cleanup_storage->table_count != 1 || parent_storage->purpose != huge::PlanPurpose::Unbound ||
        cleanup_storage->purpose != huge::PlanPurpose::Unbound || !managed_plan_owners_valid(*parent_storage) ||
        !managed_plan_owners_valid(*cleanup_storage))
        return huge::Error::Invalid;
    for (uint16_t i = 0; i < parent_storage->table_count; ++i) {
        if (parent_storage->tables[i] == cleanup_storage->tables[0]) return huge::Error::Invalid;
        auto const* words = reinterpret_cast<const uint64_t*>(parent_storage->tables[i]);
        for (size_t word = 0; word < paging::PAGE_TABLE_ENTRIES; ++word)
            if (words[word] != 0) return huge::Error::Invalid;
    }
    HugePublicationViewStorage view_storage;
    auto& view = view_storage.get();
    if (!huge::detail::PreparationAccess::span_view(span, view)) return huge::Error::Invalid;
    // Reject malformed physical encoding BEFORE any PTE/registry mutation.
    constexpr uint64_t HUGE_FRAME = PTE_FRAME_MASK & ~(huge::LEAF_BYTES - 1);
    if ((view.physical_2m & ~HUGE_FRAME) != 0) return huge::Error::Invalid;
    huge::RootKey key{};
    if (auto const error = huge::detail::root_key(root, key); error != huge::Error::None) return error;
    huge::LeafKey const leaf{key, va, view.physical_2m};
    uint64_t const ps = view.physical_2m | flags | (uint64_t{1} << 7);
    uint64_t adopted = 0;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        ManagedRootReader reader(root);
        if (!reader.valid()) return huge::Error::Again;
        uint64_t const cow = cow_pte_lock.lock_irqsave();
        uint64_t const pin = user_mapping_pin_lock.lock_irqsave();
        PageTable* table = root;
        MutableUserEntrySlot slots[3];
        uint64_t expected[3]{};
        bool valid = managed_root_matches_locked(key) && managed_huge_registry.validate_preparing(view.handle, leaf, view.actual);
        uint16_t next = 0;
        for (int level = 4; valid && level >= 2; --level) {
            auto const i = static_cast<size_t>(4 - level);
            valid = MutableUserEntrySlot::from(table, index_of(va, level), slots[i]);
            if (!valid) break;
            expected[i] = slots[i].load();
            if (level == 2) {
                valid = expected[i] == 0;
                break;
            }
            if (expected[i] == 0) {
                if (next >= parent_storage->table_count) {
                    valid = false;
                    break;
                }
                table = parent_storage->tables[next++];
            } else if ((expected[i] & paging::PAGE_PRESENT) == 0 || (expected[i] & (uint64_t{1} << 7)) != 0) {
                valid = false;
            } else {
                table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(expected[i] & PTE_FRAME_MASK));
            }
        }
        if (!valid) {
            user_mapping_pin_lock.unlock_irqrestore(pin);
            cow_pte_lock.unlock_irqrestore(cow);
            return huge::Error::Again;
        }
        // Preflight above includes every destination. Private children first,
        // then their parents; only complete validated links become reachable.
        if (!managed_replace_ad_locked(slots[2], 0, ps)) __builtin_trap();
        uint16_t used = next;
        for (int i = 1; i >= 0; --i) {
            uint64_t desired = expected[i];
            if (desired == 0) {
                --used;
                desired = parent_storage->table_physical[used] | paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER;
                adopted |= uint64_t{1} << used;
            } else {
                desired |= paging::PAGE_WRITE | paging::PAGE_USER;
                if ((flags & paging::PAGE_NX) == 0) desired &= ~paging::PAGE_NX;
            }
            if (!managed_replace_ad_locked(slots[i], expected[i], desired)) __builtin_trap();
        }
        if (managed_huge_registry.publish_mapped(view.handle, leaf, ps) != huge::Error::None) __builtin_trap();
        user_mapping_pin_lock.unlock_irqrestore(pin);
        cow_pte_lock.unlock_irqrestore(cow);
        huge::detail::PreparationAccess::span_transferred_after_publish(span);
        huge::detail::PreparationAccess::tables_transferred_after_publish(parents, adopted);
        // A full real flush also covers any ancestor promotion.
        flush_pagemap_after_update(root, 0, true);
        // Binding uses only the already owned mandatory cleanup PML1. It does
        // no allocation and cannot fail after the preflight/proven publication.
        if (bind_preallocated_cleanup_split(root, va, cleanup) != huge::Error::None) __builtin_trap();
    }
    parents.reset();
    return huge::Error::None;
}
// END private managed publication/split.

auto present_mapping_entry(PageTable* root, vaddr_t vaddr) -> const PageTableEntry* {
    if (root == nullptr) {
        return nullptr;
    }

    PageTable const* table = root;
    for (int level = 4; level > 1; --level) {
        PageTableEntry const& entry = entry_at(table, index_of(vaddr, level));
        if (entry.present == 0) {
            return nullptr;
        }
        if (level < 4 && entry.pagesize != 0) {
            return &entry;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    PageTableEntry const& entry = entry_at(table, index_of(vaddr, 1));
    return entry.present != 0 ? &entry : nullptr;
}

auto user_copy_writable_entry(const PageTableEntry& entry) -> bool {
    uint64_t const RAW = pte_raw(entry);
    return entry.present != 0 && entry.writable != 0 && entry.user != 0 && (RAW & paging::PAGE_COW) == 0;
}

auto user_page_mapped_now_impl(PageTable* pagemap, vaddr_t vaddr) -> bool {
    if (pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    PageTable const* table = pagemap;
    for (int level = 4; level > 1; --level) {
        PageTableEntry const& entry = entry_at(table, index_of(vaddr, level));
        if (entry.present == 0 || entry.user == 0) {
            return false;
        }
        if (level < 4 && entry.pagesize != 0) {
            return true;
        }
        table = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    PageTableEntry const& entry = entry_at(table, index_of(vaddr, 1));
    return entry.present != 0 && entry.user != 0;
}

auto user_page_writable_now_impl(PageTable* pagemap, vaddr_t vaddr) -> bool {
    if (pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    PageTable const* table = pagemap;
    for (int level = 4; level > 1; --level) {
        PageTableEntry const& entry = entry_at(table, index_of(vaddr, level));
        if (!user_copy_writable_entry(entry)) {
            return false;
        }
        if (level < 4 && entry.pagesize != 0) {
            return true;
        }
        table = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    PageTableEntry const& entry = entry_at(table, index_of(vaddr, 1));
    return user_copy_writable_entry(entry);
}

auto snapshot_user_page_mapping_locked(PageTable* pagemap, vaddr_t vaddr, bool require_writable, paddr_t& physical_page,
                                       bool* dirty = nullptr, uint64_t* leaf_flags = nullptr) -> bool {
    physical_page = PADDR_INVALID;
    if (dirty != nullptr) *dirty = false;
    UserLeafResolution leaf{};
    if (!resolve_user_leaf_locked(pagemap, vaddr, require_writable, leaf)) return false;
    physical_page = leaf.physical_4k;
    if (dirty != nullptr) *dirty = (leaf.raw & (uint64_t{1} << 6)) != 0;
    if (leaf_flags != nullptr) *leaf_flags = leaf.raw;
    return true;
}

void drop_present_leaf_ref(const PageTableEntry& entry) {
    if (entry.present == 0 || entry.frame == 0) {
        return;
    }

    uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
    void* virt_ptr = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS_ADDR));
    phys::page_ref_dec(virt_ptr);
}

auto promote_user_write_path(PageTableEntry& pml4e, PageTableEntry& pml3e, PageTableEntry& pml2e) -> bool {
    bool changed = false;
    if (!pml4e.writable) {
        pml4e.writable = 1;
        changed = true;
    }
    if (!pml3e.writable) {
        pml3e.writable = 1;
        changed = true;
    }
    if (!pml2e.writable) {
        pml2e.writable = 1;
        changed = true;
    }
    return changed;
}

enum class UserWriteFaultStatus : uint8_t {
    HANDLED,
    NEED_LAZY_BACKING,
    NOT_WRITABLE,
};

#ifdef WOS_SELFTEST
// Low two bits: idle, ready, consumed, preparing. The retained generation
// prevents a delayed mismatched hook from consuming a later scope.
std::atomic<uint64_t> selftest_cow_plan_word{};
struct SelftestCowPlan {
    std::atomic<sched::task::Task*> subject{};
    std::atomic<PageTable*> root{};
    std::atomic<vaddr_t> address{};
    std::atomic<paddr_t> frame{};
    std::atomic<uint64_t> cpu{UINT64_MAX};
    std::atomic<SelftestCowAction> action{};
    std::atomic<SelftestCowObservation*> observation{};
};
SelftestCowPlan selftest_cow_plan{};

auto selftest_consume_cow_action(sched::task::Task* subject, PageTable* root, vaddr_t address, paddr_t frame, bool before_allocation,
                                 void* destination) -> bool {
    uint64_t word = selftest_cow_plan_word.load(std::memory_order_acquire);
    if ((word & 3U) != 1U || selftest_cow_plan.cpu.load(std::memory_order_relaxed) != cpu::current_cpu() ||
        selftest_cow_plan.subject.load(std::memory_order_relaxed) != subject ||
        selftest_cow_plan.root.load(std::memory_order_relaxed) != root ||
        selftest_cow_plan.address.load(std::memory_order_relaxed) != address ||
        selftest_cow_plan.frame.load(std::memory_order_relaxed) != frame) {
        return false;
    }
    SelftestCowAction const ACTION = selftest_cow_plan.action.load(std::memory_order_relaxed);
    if (before_allocation != (ACTION == SelftestCowAction::FAIL_DESTINATION_ALLOCATION)) {
        return false;
    }
    auto* observation = selftest_cow_plan.observation.load(std::memory_order_relaxed);
    if (observation == nullptr ||
        !selftest_cow_plan_word.compare_exchange_strong(word, (word & ~3ULL) | 2U, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return false;
    }
    // Only the synchronous caller's CPU may consume its borrowed observation.
    // Other resolver calls never access it, including during disarm/rearm.
    observation->consumed = 1;
    observation->destination = destination;
    observation->action_outside_spinlocks = true;
    if (!before_allocation) {
        uint64_t const FLAGS = ACTION == SelftestCowAction::DOWNGRADE_READ_BEFORE_COMMIT ? paging::page_types::USER_READONLY
                                                                                         : paging::PAGE_PRESENT | paging::PAGE_NX;
        unify_page_flags(root, address, FLAGS);
    }
    return true;
}
#endif

auto resolve_user_write_mapping(sched::task::Task* task, vaddr_t vaddr, const DemandContext& demand) -> UserWriteFaultStatus {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return UserWriteFaultStatus::NOT_WRITABLE;
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(task->pagemap, "resolve-user-write-mapping", vaddr);
#endif

    PageTable* const PAGEMAP = task->pagemap;
    uint64_t const VADDR = vaddr;
    uint64_t const IDX4 = index_of(VADDR, 4);
    uint64_t const IDX3 = index_of(VADDR, 3);
    uint64_t const IDX2 = index_of(VADDR, 2);
    uint64_t const IDX1 = index_of(VADDR, 1);

    void* old_virt = nullptr;
    paddr_t old_phys = 0;
    phys::PageLookupHint cow_lookup{};
    uint32_t refcount = 0;
    bool old_is_zero_page = false;
    bool old_mapping_refcounted = true;
    perf::WkiPerfLocalVmemOp cow_op = perf::WkiPerfLocalVmemOp::COW_COPY;
    uint64_t cow_started_us = 0;
    bool initial_path_promoted = false;

    {
        // Hand CPU stability to the existing PTE lock's preemption hold;
        // no extra guard spans the later shootdown/perf or allocator call.
        sched::preempt_disable();
        bool const OBSERVED = demand_is_observed(demand);
        uint64_t const LOCK_FLAGS = cow_pte_lock.lock_irqsave();
        sched::preempt_enable();

        PageTableEntry& pml4e = entry_at(PAGEMAP, IDX4);
        if (!pml4e.present) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        auto* pml3 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml4e.frame << paging::PAGE_SHIFT));
        PageTableEntry& pml3e = entry_at(pml3, IDX3);
        if (!pml3e.present || pml3e.pagesize != 0) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return pml3e.present ? UserWriteFaultStatus::NOT_WRITABLE : UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        auto* pml2 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml3e.frame << paging::PAGE_SHIFT));
        PageTableEntry& pml2e = entry_at(pml2, IDX2);
        if (pml2e.present && pml2e.pagesize != 0) {
            uint64_t const pin_flags = user_mapping_pin_lock.lock_irqsave();
            UserLeafResolution managed{};
            bool const writable = resolve_user_leaf_locked(PAGEMAP, VADDR, true, managed);
            user_mapping_pin_lock.unlock_irqrestore(pin_flags);
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return writable ? UserWriteFaultStatus::HANDLED : UserWriteFaultStatus::NOT_WRITABLE;
        }
        if (!pml2e.present) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        auto* pml1 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml2e.frame << paging::PAGE_SHIFT));
        PageTableEntry& pte = entry_at(pml1, IDX1);
        if (!pte.present) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        uint64_t raw = pte_raw(pte);
        bool const CHECK_ANON_RANGE =
            (raw & (paging::PAGE_COW | paging::PAGE_WRITE | paging::PAGE_SHARED)) == 0U && (raw & paging::PAGE_USER) != 0U;
        uint64_t const LAZY_LOCK_FLAGS = CHECK_ANON_RANGE ? task->lazy_vmem_lock.lock_irqsave() : 0;
        bool const WRITABLE_ANON_RANGE = CHECK_ANON_RANGE && writable_anonymous_range_contains_locked(task, VADDR);
        uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
        auto unlock_initial = [&] {
            user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
            if (CHECK_ANON_RANGE) {
                task->lazy_vmem_lock.unlock_irqrestore(LAZY_LOCK_FLAGS);
            }
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
        };
        raw = pte_raw(pte);
        if ((raw & paging::PAGE_USER) == 0U || paging::shared_write_restricted(raw)) {
            unlock_initial();
            return UserWriteFaultStatus::NOT_WRITABLE;
        }
        bool const SYNTHETIC_ANON_COW = (raw & (paging::PAGE_COW | paging::PAGE_WRITE | paging::PAGE_SHARED)) == 0U && WRITABLE_ANON_RANGE;
        if (SYNTHETIC_ANON_COW) {
            owned_frame_untrack_leaf(PAGEMAP, VADDR, pte);
            raw |= paging::PAGE_COW;
            pte = pte_from_raw(raw);
        }

        if ((raw & paging::PAGE_COW) == 0U) {
            if ((raw & paging::PAGE_WRITE) == 0U || (raw & paging::PAGE_USER) == 0U) {
                unlock_initial();
                return UserWriteFaultStatus::NOT_WRITABLE;
            }

            bool const PATH_PROMOTED = promote_user_write_path(pml4e, pml3e, pml2e);
            unlock_initial();
            flush_pagemap_after_update(PAGEMAP, VADDR, PATH_PROMOTED);
            return UserWriteFaultStatus::HANDLED;
        }

        cow_started_us = time::get_us();
        initial_path_promoted = promote_user_write_path(pml4e, pml3e, pml2e);

        old_phys = pte.frame << paging::PAGE_SHIFT;
        old_virt = reinterpret_cast<void*>(addr::get_virt_pointer(old_phys));
        refcount = phys::page_ref_get(old_virt, &cow_lookup);
        old_is_zero_page = perf::is_local_vmem_zero_page(old_virt);
        old_mapping_refcounted = !old_is_zero_page || refcount > 1U;
        if (old_is_zero_page) {
            cow_op = perf::WkiPerfLocalVmemOp::COW_ZERO;
        } else if (refcount <= 1) {
            cow_op = perf::WkiPerfLocalVmemOp::COW_PROMOTE;
        }

        if (refcount <= 1 && !old_is_zero_page) {
            raw &= ~paging::PAGE_COW;
            raw |= paging::PAGE_WRITE;
            pte = pte_from_raw(raw);
            owned_frame_track_private_mapping(PAGEMAP, VADDR, old_phys, raw);
            unlock_initial();
            flush_pagemap_after_update(PAGEMAP, VADDR, initial_path_promoted);
            record_cow_perf_event(task, cow_op, VADDR, refcount, cow_started_us);
            record_demand_result(demand, DemandBacking::PrivateCowCopy, numa::INVALID_NODE, numa::INVALID_NODE, false, true, OBSERVED);
            return UserWriteFaultStatus::HANDLED;
        }

        // Pin the old frame while the PTE lock still proves this mapping's
        // PTE reference exists. Allocation and memcpy below intentionally run
        // outside the lock; the commit path rechecks the same frame under lock.
        phys::page_ref_inc(old_virt, &cow_lookup);
        unlock_initial();
    }

    bool const DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE = !old_is_zero_page;
    DemandResultStorage storage;
    auto& allocation = storage.get();
    void* new_page = nullptr;
#ifdef WOS_SELFTEST
    if (!selftest_consume_cow_action(task, PAGEMAP, VADDR, old_phys, true, nullptr))
#endif
    {
        alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE, demand, allocation);
        new_page = allocation.span.data();
    }
    record_demand_candidate(demand, new_page == nullptr);
    if (new_page == nullptr) {
        phys::page_ref_dec(old_virt, &cow_lookup);
        log::error("COW fault: OOM allocating new page for vaddr 0x%x", VADDR);
        return UserWriteFaultStatus::NOT_WRITABLE;
    }

    if (DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE) {
        std::memcpy(new_page, old_virt, paging::PAGE_SIZE);
    }

#ifdef WOS_SELFTEST
    (void)selftest_consume_cow_action(task, PAGEMAP, VADDR, old_phys, false, new_page);
#endif

    auto const REQUESTED = allocation.requested;
    auto const ACTUAL = allocation.actual;
    bool installed_private_page = false;
    bool final_path_promoted = false;
    bool observed = false;
    auto const NEW_PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(new_page)));
    {
        sched::PreemptGuard const PREEMPT;
        observed = demand_is_observed(demand);
        uint64_t const LOCK_FLAGS = cow_pte_lock.lock_irqsave();
        uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
        PageTableEntry& pml4e = entry_at(PAGEMAP, IDX4);
        if (pml4e.present) {
            auto* pml3 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml4e.frame << paging::PAGE_SHIFT));
            PageTableEntry& pml3e = entry_at(pml3, IDX3);
            if (pml3e.present && pml3e.pagesize == 0) {
                auto* pml2 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml3e.frame << paging::PAGE_SHIFT));
                PageTableEntry& pml2e = entry_at(pml2, IDX2);
                if (pml2e.present && pml2e.pagesize == 0) {
                    auto* pml1 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml2e.frame << paging::PAGE_SHIFT));
                    PageTableEntry& pte = entry_at(pml1, IDX1);
                    uint64_t raw_now = pte.present ? pte_raw(pte) : 0;
                    paddr_t const CURRENT_PHYS = pte.present ? (pte.frame << paging::PAGE_SHIFT) : 0;
                    if ((raw_now & (paging::PAGE_USER | paging::PAGE_COW)) == (paging::PAGE_USER | paging::PAGE_COW) &&
                        !paging::shared_write_restricted(raw_now) && CURRENT_PHYS == old_phys) {
                        final_path_promoted = promote_user_write_path(pml4e, pml3e, pml2e);
                        raw_now &= ~paging::PAGE_COW;
                        raw_now |= paging::PAGE_WRITE;
                        raw_now &= ~(0xFFFFFFFFFFULL << 12);
                        raw_now |= (NEW_PHYS & ~0xFFFULL);
                        pte = pte_from_raw(raw_now);
                        owned_frame_track_fresh_normal_mapping(PAGEMAP, VADDR, NEW_PHYS, raw_now);
                        installed_private_page = true;
                    }
                }
            }
        }
        user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
        cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
    }

    if (installed_private_page)
        (void)allocation.span.release();
    else
        allocation.span.reset();
    record_demand_result(demand, old_is_zero_page ? DemandBacking::ZeroCow : DemandBacking::PrivateCowCopy, REQUESTED, ACTUAL,
                         installed_private_page, false, observed);
    if (!installed_private_page) {
        phys::page_ref_dec(old_virt, &cow_lookup);
        flush_pagemap_after_update(PAGEMAP, VADDR, initial_path_promoted);
        record_cow_perf_event(task, cow_op, VADDR, refcount, cow_started_us);
        return UserWriteFaultStatus::HANDLED;
    }

    flush_pagemap_after_update(PAGEMAP, VADDR, initial_path_promoted || final_path_promoted);
    phys::page_ref_dec(old_virt, &cow_lookup);
    if (old_mapping_refcounted) {
        phys::page_ref_dec(old_virt, &cow_lookup);
    } else {
        log::warn("COW zero page mapping had no mapping ref to drop: pid=%lu name=%s vaddr=0x%llx ref=%u", task->pid,
                  task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(VADDR), refcount);
    }
    record_cow_perf_event(task, cow_op, VADDR, refcount, cow_started_us);
    return UserWriteFaultStatus::HANDLED;
}

auto ensure_user_page_writable_with_context(sched::task::Task* task, vaddr_t vaddr, const DemandContext& demand) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        switch (resolve_user_write_mapping(task, vaddr, demand)) {
            case UserWriteFaultStatus::HANDLED:
                return true;
            case UserWriteFaultStatus::NOT_WRITABLE:
                return false;
            case UserWriteFaultStatus::NEED_LAZY_BACKING:
                break;
        }

        paging::PageFault const WRITE_FAULT = paging::create_page_fault(1ULL << paging::error_flags::WRITE, true);
        if (!handle_lazy_vmem_fault(task, vaddr, WRITE_FAULT, demand)) {
            return false;
        }
    }

    return false;
}

auto ensure_user_page_mapped_with_context(sched::task::Task* task, vaddr_t vaddr, const DemandContext& demand) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        if (translate(task->pagemap, vaddr) != PADDR_INVALID) {
            return true;
        }

        paging::PageFault const READ_FAULT = paging::create_page_fault(0, true);
        if (!handle_lazy_vmem_fault(task, vaddr, READ_FAULT, demand)) {
            return false;
        }
    }

    return translate(task->pagemap, vaddr) != PADDR_INVALID;
}
auto ensure_user_page_writable_for_task(sched::task::Task* task, vaddr_t vaddr) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) return false;
    DemandContextStorage storage;
    auto& demand = storage.get();
    return capture_demand_context(task, vaddr, DemandOrigin::SoftwareEnsure, false, demand) &&
           ensure_user_page_writable_with_context(task, vaddr, demand);
}

auto ensure_user_page_mapped_for_task(sched::task::Task* task, vaddr_t vaddr) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) return false;
    DemandContextStorage storage;
    auto& demand = storage.get();
    return capture_demand_context(task, vaddr, DemandOrigin::SoftwareEnsure, false, demand) &&
           ensure_user_page_mapped_with_context(task, vaddr, demand);
}

}  // namespace

void get_private_demand_snapshot(PrivateDemandSnapshot& output) noexcept {
    output.candidates = demand_counters.candidates.load(std::memory_order_relaxed);
    output.allocation_failures = demand_counters.allocation_failures.load(std::memory_order_relaxed);
    output.discarded = demand_counters.discarded.load(std::memory_order_relaxed);
    output.promotions = demand_counters.promotions.load(std::memory_order_relaxed);
    output.installed = demand_counters.installed.load(std::memory_order_relaxed);
    output.anonymous = demand_counters.anonymous.load(std::memory_order_relaxed);
    output.zero_cow = demand_counters.zero_cow.load(std::memory_order_relaxed);
    output.private_cow = demand_counters.private_cow.load(std::memory_order_relaxed);
    output.software = demand_counters.software.load(std::memory_order_relaxed);
    output.hardware_user = demand_counters.hardware_user.load(std::memory_order_relaxed);
    output.hardware_kernel = demand_counters.hardware_kernel.load(std::memory_order_relaxed);
    output.eligible = demand_counters.eligible.load(std::memory_order_relaxed);
    output.local = demand_counters.local.load(std::memory_order_relaxed);
    output.locality_misses = demand_counters.locality_misses.load(std::memory_order_relaxed);
    output.fallbacks = demand_counters.fallbacks.load(std::memory_order_relaxed);
    output.overflow = demand_counters.overflow.load(std::memory_order_relaxed);
}

void service_pending_tlb_shootdowns() {
    if (tlb_shootdown_vector == 0 || !smt::has_cpu_data()) {
        return;
    }
    service_tlb_shootdown_requests_for_cpu(cpu::get_current_cpu_id_safe());
}

constexpr size_t KERNEL_PML4_START = 256;
constexpr size_t KERNEL_PML4_END = 512;

namespace {

struct DestroyUserSpaceStatsAtomic {
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> collect_frames_us_total{0};
    std::atomic<uint64_t> collect_frames_us_max{0};
    std::atomic<uint64_t> free_data_us_total{0};
    std::atomic<uint64_t> free_data_us_max{0};
    std::atomic<uint64_t> free_pt_us_total{0};
    std::atomic<uint64_t> free_pt_us_max{0};
    std::atomic<uint64_t> tlb_flush_us_total{0};
    std::atomic<uint64_t> tlb_flush_us_max{0};
    std::atomic<uint64_t> data_leaf_entries_visited{0};
    std::atomic<uint64_t> data_pages_ref_decremented{0};
    std::atomic<uint64_t> data_pages_freed{0};
    std::atomic<uint64_t> page_table_pages_ref_decremented{0};
    std::atomic<uint64_t> page_table_pages_freed{0};
    std::atomic<uint64_t> skipped_huge_pages{0};
    std::atomic<uint64_t> skipped_unknown_frames{0};
    std::atomic<uint64_t> skipped_slab_alloc_frames{0};
    std::atomic<uint64_t> skipped_medium_alloc_frames{0};
    std::atomic<uint64_t> skipped_kmalloc_large_alloc_frames{0};
    std::atomic<uint64_t> skipped_page_table_aliases{0};
    std::atomic<uint64_t> skipped_corrupt_entries{0};
    std::atomic<uint64_t> magic_unknown_probe_reads{0};
    std::atomic<uint64_t> magic_unknown_slab_hits{0};
    std::atomic<uint64_t> magic_unknown_medium_hits{0};
    std::atomic<uint64_t> magic_unknown_kmalloc_large_hits{0};
};

struct DestroyUserSpaceCallStats {
    uint64_t collect_frames_us = 0;
    uint64_t free_data_us = 0;
    uint64_t free_pt_us = 0;
    uint64_t tlb_flush_us = 0;
    uint64_t data_leaf_entries_visited = 0;
    uint64_t data_pages_ref_decremented = 0;
    uint64_t data_pages_freed = 0;
    uint64_t page_table_pages_ref_decremented = 0;
    uint64_t page_table_pages_freed = 0;
    uint64_t skipped_huge_pages = 0;
    uint64_t skipped_unknown_frames = 0;
    uint64_t skipped_slab_alloc_frames = 0;
    uint64_t skipped_medium_alloc_frames = 0;
    uint64_t skipped_kmalloc_large_alloc_frames = 0;
    uint64_t skipped_page_table_aliases = 0;
    uint64_t skipped_corrupt_entries = 0;
    uint64_t magic_unknown_probe_reads = 0;
    uint64_t magic_unknown_slab_hits = 0;
    uint64_t magic_unknown_medium_hits = 0;
    uint64_t magic_unknown_kmalloc_large_hits = 0;
};

enum class DestroyUserSpacePhase : uint8_t {
    COLLECT_FRAMES = 0,
    FREE_DATA = 1,
    FREE_PAGE_TABLES = 2,
    TLB_FLUSH = 3,
    DONE = 4,
};

struct DestroyWalkFrame {
    PageTable* table = nullptr;
    uint64_t vaddr_base = 0;
    uint64_t phys_addr = 0;
    uint16_t next_index = 0;
    int8_t level = 0;
};

std::array<DestroyUserSpaceStatsAtomic, desc::gdt::MAX_CPUS>
    g_destroy_user_space_stats{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

inline void update_relaxed_max(std::atomic<uint64_t>& slot, uint64_t value) {
    uint64_t current = slot.load(std::memory_order_relaxed);
    while (value > current && !slot.compare_exchange_weak(current, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

inline auto elapsed_us_since(uint64_t start_us, uint64_t end_us) -> uint64_t { return end_us >= start_us ? end_us - start_us : 0; }

void note_destroy_refdec_batch_stats(DestroyUserSpaceCallStats* stats, phys::PageRefBatchStats const& batch_stats) {
    if (stats == nullptr) {
        return;
    }

    stats->data_pages_ref_decremented += batch_stats.refs_decremented;
    stats->data_pages_freed += batch_stats.pages_freed;
}

void note_destroy_page_table_refdec_batch_stats(DestroyUserSpaceCallStats* stats, phys::PageRefBatchStats const& batch_stats) {
    if (stats == nullptr) {
        return;
    }

    stats->page_table_pages_ref_decremented += batch_stats.refs_decremented;
    stats->page_table_pages_freed += batch_stats.pages_freed;
}

void note_destroy_huge_skip(DestroyUserSpaceCallStats* stats) {
    if (stats != nullptr) {
        stats->skipped_huge_pages++;
    }
}

void note_destroy_page_table_alias_skip(DestroyUserSpaceCallStats* stats) {
    if (stats != nullptr) {
        stats->skipped_page_table_aliases++;
    }
}

void note_destroy_corrupt_skip(DestroyUserSpaceCallStats* stats) {
    if (stats != nullptr) {
        stats->skipped_corrupt_entries++;
    }
}

void note_destroy_kind_skip(DestroyUserSpaceCallStats* stats, PageKind kind) {
    if (stats == nullptr) {
        return;
    }

    switch (kind) {
        case PageKind::UNKNOWN:
            stats->skipped_unknown_frames++;
            break;
        case PageKind::SLAB:
            stats->skipped_slab_alloc_frames++;
            break;
        case PageKind::MEDIUM:
            stats->skipped_medium_alloc_frames++;
            break;
        case PageKind::KMALLOC_LARGE:
            stats->skipped_kmalloc_large_alloc_frames++;
            break;
        case PageKind::FREE:
        case PageKind::RESERVED:
        case PageKind::NORMAL:
        case PageKind::PAGE_TABLE:
        default:
            stats->skipped_corrupt_entries++;
            break;
    }
}

#ifdef WOS_MM_RECLAIM_MAGIC_PROBES
void note_destroy_magic_unknown_probe(DestroyUserSpaceCallStats* stats) {
    if (stats != nullptr) {
        stats->magic_unknown_probe_reads++;
    }
}

void note_destroy_magic_unknown_hit(DestroyUserSpaceCallStats* stats, PageKind kind) {
    if (stats == nullptr) {
        return;
    }

    switch (kind) {
        case PageKind::SLAB:
            stats->magic_unknown_slab_hits++;
            break;
        case PageKind::MEDIUM:
            stats->magic_unknown_medium_hits++;
            break;
        case PageKind::KMALLOC_LARGE:
            stats->magic_unknown_kmalloc_large_hits++;
            break;
        case PageKind::UNKNOWN:
        case PageKind::FREE:
        case PageKind::RESERVED:
        case PageKind::NORMAL:
        case PageKind::PAGE_TABLE:
        default:
            break;
    }
}
#endif

void note_destroy_user_space_stats(DestroyUserSpaceCallStats const& sample) {
    uint64_t cpu_no = 0;
    if (smt::has_cpu_data()) {
        cpu_no = cpu::current_cpu();
    }
    if (cpu_no >= g_destroy_user_space_stats.size()) {
        return;
    }

    auto& stats = g_destroy_user_space_stats[static_cast<size_t>(cpu_no)];
    stats.calls.fetch_add(1, std::memory_order_relaxed);
    stats.collect_frames_us_total.fetch_add(sample.collect_frames_us, std::memory_order_relaxed);
    update_relaxed_max(stats.collect_frames_us_max, sample.collect_frames_us);
    stats.free_data_us_total.fetch_add(sample.free_data_us, std::memory_order_relaxed);
    update_relaxed_max(stats.free_data_us_max, sample.free_data_us);
    stats.free_pt_us_total.fetch_add(sample.free_pt_us, std::memory_order_relaxed);
    update_relaxed_max(stats.free_pt_us_max, sample.free_pt_us);
    stats.tlb_flush_us_total.fetch_add(sample.tlb_flush_us, std::memory_order_relaxed);
    update_relaxed_max(stats.tlb_flush_us_max, sample.tlb_flush_us);
    stats.data_leaf_entries_visited.fetch_add(sample.data_leaf_entries_visited, std::memory_order_relaxed);
    stats.data_pages_ref_decremented.fetch_add(sample.data_pages_ref_decremented, std::memory_order_relaxed);
    stats.data_pages_freed.fetch_add(sample.data_pages_freed, std::memory_order_relaxed);
    stats.page_table_pages_ref_decremented.fetch_add(sample.page_table_pages_ref_decremented, std::memory_order_relaxed);
    stats.page_table_pages_freed.fetch_add(sample.page_table_pages_freed, std::memory_order_relaxed);
    stats.skipped_huge_pages.fetch_add(sample.skipped_huge_pages, std::memory_order_relaxed);
    stats.skipped_unknown_frames.fetch_add(sample.skipped_unknown_frames, std::memory_order_relaxed);
    stats.skipped_slab_alloc_frames.fetch_add(sample.skipped_slab_alloc_frames, std::memory_order_relaxed);
    stats.skipped_medium_alloc_frames.fetch_add(sample.skipped_medium_alloc_frames, std::memory_order_relaxed);
    stats.skipped_kmalloc_large_alloc_frames.fetch_add(sample.skipped_kmalloc_large_alloc_frames, std::memory_order_relaxed);
    stats.skipped_page_table_aliases.fetch_add(sample.skipped_page_table_aliases, std::memory_order_relaxed);
    stats.skipped_corrupt_entries.fetch_add(sample.skipped_corrupt_entries, std::memory_order_relaxed);
    stats.magic_unknown_probe_reads.fetch_add(sample.magic_unknown_probe_reads, std::memory_order_relaxed);
    stats.magic_unknown_slab_hits.fetch_add(sample.magic_unknown_slab_hits, std::memory_order_relaxed);
    stats.magic_unknown_medium_hits.fetch_add(sample.magic_unknown_medium_hits, std::memory_order_relaxed);
    stats.magic_unknown_kmalloc_large_hits.fetch_add(sample.magic_unknown_kmalloc_large_hits, std::memory_order_relaxed);
}

void note_destroy_user_space_phase_time(DestroyUserSpaceCallStats& sample, DestroyUserSpacePhase phase, uint64_t elapsed_us) {
    switch (phase) {
        case DestroyUserSpacePhase::COLLECT_FRAMES:
            sample.collect_frames_us += elapsed_us;
            break;
        case DestroyUserSpacePhase::FREE_DATA:
            sample.free_data_us += elapsed_us;
            break;
        case DestroyUserSpacePhase::FREE_PAGE_TABLES:
            sample.free_pt_us += elapsed_us;
            break;
        case DestroyUserSpacePhase::TLB_FLUSH:
            sample.tlb_flush_us += elapsed_us;
            break;
        case DestroyUserSpacePhase::DONE:
            break;
    }
}

}  // namespace

namespace {
void refresh_kernel_mappings(PageTable* page_table) {
    if (page_table == nullptr || kernel_pagemap == nullptr || page_table == kernel_pagemap) {
        return;
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "refresh-kernel-mappings");
#endif

    for (size_t i = KERNEL_PML4_START; i < KERNEL_PML4_END; i++) {
        PageTableEntry const& kernel_entry = entry_at(kernel_pagemap, i);
        PageTableEntry& task_entry = entry_at(page_table, i);
        if (pte_raw(task_entry) != pte_raw(kernel_entry)) {
            task_entry = kernel_entry;
        }
    }
}

auto range_valid(Range range) -> bool { return range.start < range.end; }

auto memmap_page_range(const limine_memmap_entry* entry, Range& range) -> bool {
    range = {};
    if (entry == nullptr || entry->length < paging::PAGE_SIZE) {
        return false;
    }

    uint64_t const PAGE_COUNT = entry->length / paging::PAGE_SIZE;
    uint64_t const BYTES = PAGE_COUNT * paging::PAGE_SIZE;
    if (entry->base > UINT64_MAX - BYTES) {
        return false;
    }

    range = Range{.start = entry->base, .end = entry->base + BYTES};
    return range_valid(range);
}

auto direct_map_flags_for_entry(uint64_t type) -> uint64_t {
    bool const READONLY =
        type == LIMINE_MEMMAP_RESERVED || type == LIMINE_MEMMAP_BAD_MEMORY || type == LIMINE_MEMMAP_EXECUTABLE_AND_MODULES;
    uint64_t flags = READONLY ? paging::page_types::READONLY : paging::page_types::KERNEL;
    if (kernel_nx_supported) {
        flags |= paging::PAGE_NX;
    }
    return flags;
}

struct BootDirectMapStats {
    size_t pages{};
    size_t small_4k{};
    size_t large_2m{};
};

struct MappingPermissions {
    bool present{};
    bool writable{};
    bool executable{};
};

auto mapping_permissions(PageTable* root, vaddr_t vaddr) -> MappingPermissions {
    MappingPermissions result{.present = true, .writable = true, .executable = true};
    PageTable* table = root;
    for (int level = 4; level >= 1; --level) {
        if (table == nullptr) {
            return {};
        }
        PageTableEntry const& entry = entry_at(table, index_of(vaddr, level));
        if (entry.present == 0) {
            return {};
        }
        result.writable = result.writable && entry.writable != 0;
        result.executable = result.executable && entry.no_execute == 0;
        if (level == 1 || (entry.pagesize != 0 && (level == 3 || level == 2))) {
            return result;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }
    return {};
}

auto audit_mapping_range(vaddr_t start, vaddr_t end, bool writable, bool executable) -> bool {
    if (kernel_pagemap == nullptr || start >= end || (start & (paging::PAGE_SIZE - 1)) != 0U || (end & (paging::PAGE_SIZE - 1)) != 0U) {
        return false;
    }
    for (vaddr_t page = start; page < end; page += paging::PAGE_SIZE) {
        MappingPermissions const PERMISSIONS = mapping_permissions(kernel_pagemap, page);
        if (!PERMISSIONS.present || PERMISSIONS.writable != writable || PERMISSIONS.executable != executable) {
            return false;
        }
    }
    return true;
}

auto map_2m_page(PageTable* page_table, vaddr_t vaddr, paddr_t paddr, uint64_t flags) -> bool {
    if (page_table == nullptr || flags == 0 || !is_aligned_to(vaddr, LARGE_PAGE_2M_BYTES) || !is_aligned_to(paddr, LARGE_PAGE_2M_BYTES)) {
        return false;
    }

    PageTable* pml3 = advance_page_table(page_table, index_of(vaddr, 4), table_flags_for_leaf(flags));
    PageTable* pml2 = advance_page_table(pml3, index_of(vaddr, 3), table_flags_for_leaf(flags));
    PageTableEntry& entry = entry_at(pml2, index_of(vaddr, 2));
    if (entry.present != 0 && entry.pagesize == 0) {
        return false;
    }

    entry = paging::create_page_table_entry(paddr, flags);
    entry.pagesize = 1;
    return true;
}

// Boot HHDM leaves are permanent borrowed RAM translations. This bounded
// read-only recognizer neither grants user-leaf authority nor demotes a leaf.
auto boot_direct_map_allows_2m(uint64_t type) noexcept -> bool { return type == LIMINE_MEMMAP_USABLE; }

auto kernel_hhdm_2m_flags_match(uint64_t pml4_raw, uint64_t pml3_raw, uint64_t leaf_raw, uint64_t flags) noexcept -> bool {
    constexpr uint64_t PS = uint64_t{1} << 7;
    constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
    constexpr uint64_t REQUEST = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_PWT | paging::PAGE_PCD | paging::PAGE_NX;
    constexpr uint64_t ANCESTOR = PTE_FRAME_MASK | REQUEST | (uint64_t{1} << 5);
    constexpr uint64_t LEAF = (PTE_FRAME_MASK & ~(LARGE_PAGE_2M_BYTES - 1)) | REQUEST | PS | AD;
    if ((flags & paging::PAGE_PRESENT) == 0 || (flags & ~REQUEST) != 0 || (pml4_raw & paging::PAGE_PRESENT) == 0 ||
        (pml3_raw & paging::PAGE_PRESENT) == 0 || (pml4_raw & ~ANCESTOR) != 0 || (pml3_raw & ~ANCESTOR) != 0 ||
        (leaf_raw & (paging::PAGE_PRESENT | PS)) != (paging::PAGE_PRESENT | PS) || (leaf_raw & ~LEAF) != 0 ||
        (leaf_raw & REQUEST) != flags) {
        return false;
    }
    // A restrictive parent must not hide leaf permissions that a later parent
    // promotion would expose. Raw leaf permissions above must match as well.
    if ((flags & paging::PAGE_WRITE) != 0 && ((pml4_raw & pml3_raw & paging::PAGE_WRITE) == 0)) return false;
    if ((flags & paging::PAGE_NX) == 0 && ((pml4_raw | pml3_raw) & paging::PAGE_NX) != 0) return false;
    return true;
}

auto kernel_hhdm_entry_raw(PageTable const* table, size_t index) noexcept -> uint64_t {
    // Actual allocated page alignment, not packed PageTable's type alignment.
    auto const base = reinterpret_cast<uintptr_t>(table);
    if (table == nullptr || (base & (paging::PAGE_SIZE - 1)) != 0 || index >= paging::PAGE_TABLE_ENTRIES) return 0;
    auto const* word = reinterpret_cast<uint64_t const*>(base + index * sizeof(uint64_t));
    return __atomic_load_n(word, __ATOMIC_ACQUIRE);
}

auto kernel_hhdm_2m_mapping_matches(PageTable* root, vaddr_t address, paddr_t physical, uint64_t flags) noexcept -> bool {
    uint64_t const HHDM = addr::get_hhdm_offset();
    if (root == nullptr || root != kernel_pagemap || address < 0xffff800000000000ULL || HHDM < 0xffff800000000000ULL ||
        (address & (paging::PAGE_SIZE - 1)) != 0 || (physical & ~PTE_FRAME_MASK) != 0 || physical > UINT64_MAX - HHDM ||
        address != HHDM + physical)
        return false;

    // map_2m_page is the only kernel PS publisher; after boot these ancestors
    // and leaves remain permanent. Read under the existing leaf-pin lock. No
    // allocation, callbacks, addressability work or shootdown occurs here.
    uint64_t const LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    constexpr uint64_t ANCESTOR_ALLOWED = PTE_FRAME_MASK | paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_PWT | paging::PAGE_PCD |
                                          paging::PAGE_NX | (uint64_t{1} << 5);
    uint64_t const PML4 = kernel_hhdm_entry_raw(root, index_of(address, 4));
    bool result = false;
    if ((PML4 & paging::PAGE_PRESENT) != 0 && (PML4 & ~ANCESTOR_ALLOWED) == 0) {
        auto const* pml3 = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(PML4 & PTE_FRAME_MASK));
        uint64_t const PML3 = kernel_hhdm_entry_raw(pml3, index_of(address, 3));
        // This check rejects 1GiB PS before its physical frame is dereferenced.
        if ((PML3 & paging::PAGE_PRESENT) != 0 && (PML3 & ~ANCESTOR_ALLOWED) == 0) {
            auto const* pml2 = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(PML3 & PTE_FRAME_MASK));
            uint64_t const LEAF = kernel_hhdm_entry_raw(pml2, index_of(address, 2));
            uint64_t const BASE = LEAF & PTE_FRAME_MASK & ~(LARGE_PAGE_2M_BYTES - 1);
            result = kernel_hhdm_2m_flags_match(PML4, PML3, LEAF, flags) && BASE + (address & (LARGE_PAGE_2M_BYTES - 1)) == physical;
        }
    }
    user_mapping_pin_lock.unlock_irqrestore(LOCK_FLAGS);
    return result;
}

auto map_boot_direct_range(PageTable* page_table, Range range, uint64_t flags, bool allow_large) -> BootDirectMapStats {
    BootDirectMapStats stats{};
    if (!range_valid(range)) {
        return stats;
    }

    PageMapBatch batch{};
    init_page_map_batch(&batch, page_table, flags);
    bool const HHDM_SUPPORTS_2M = is_aligned_to(addr::get_hhdm_offset(), LARGE_PAGE_2M_BYTES);
    for (uint64_t phys = range.start; phys < range.end; phys += paging::PAGE_SIZE) {
        auto const VADDR = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(phys));
        if (allow_large && HHDM_SUPPORTS_2M && is_aligned_to(phys, LARGE_PAGE_2M_BYTES) && range.end - phys >= LARGE_PAGE_2M_BYTES &&
            map_2m_page(page_table, VADDR, phys, flags)) {
            stats.pages += paging::PAGE_TABLE_ENTRIES;
            stats.large_2m++;
            phys += LARGE_PAGE_2M_BYTES - paging::PAGE_SIZE;
            continue;
        }

        map_page_batched(&batch, VADDR, phys, flags);
        stats.pages++;
        stats.small_4k++;
    }
    flush_page_map_batch(&batch);
    if (stats.large_2m > 0) {
        wrcr3(rdcr3());
    }
    return stats;
}
}  // namespace

void init_tlb_shootdown() {
    bool expected = false;
    if (!tlb_shootdown_init_attempted.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    tlb_shootdown_vector = gates::allocate_vector();
    if (tlb_shootdown_vector != 0) {
        gates::set_interrupt_handler(tlb_shootdown_vector, tlb_shootdown_handler);
        dbg::log("Registered TLB shootdown IPI handler at vector 0x%x", tlb_shootdown_vector);
    } else {
        if (smt::get_core_count() > 1) dbg::panic_handler("SMP requires a TLB shootdown vector");
        dbg::log("WARNING: No free interrupt vector for TLB shootdown IPI");
    }
}

bool active_pagemap_is(PageTable* pagemap) { return is_current_pagemap(pagemap); }

bool user_page_mapped_now(PageTable* pagemap, vaddr_t vaddr) { return user_page_mapped_now_impl(pagemap, vaddr); }

bool user_page_writable_now(PageTable* pagemap, vaddr_t vaddr) { return user_page_writable_now_impl(pagemap, vaddr); }

namespace {

auto try_pin_current_user_page_mapping(PageTable* pagemap, vaddr_t vaddr, bool require_writable, size_t held_access_gate_index,
                                       UserPagePin& out) -> bool {
    paddr_t physical_page = PADDR_INVALID;
    paddr_t confirmed_page = PADDR_INVALID;
    void* hhdm_page = nullptr;
    phys::PageLookupHint lookup_hint{};
    bool ref_acquired = false;
    bool pinned = false;
    bool dirty = false;
    file_mapping::FileMappingBinding* binding = nullptr;
    uint64_t root_incarnation = 0;

    uint64_t const LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(pagemap);
    if (control != nullptr) root_incarnation = control->serial;
    if (snapshot_user_page_mapping_locked(pagemap, vaddr, require_writable, physical_page)) {
        hhdm_page = reinterpret_cast<void*>(addr::get_virt_pointer(physical_page));
        ref_acquired = phys::page_ref_try_inc(hhdm_page, &lookup_hint);
        pinned = ref_acquired && snapshot_user_page_mapping_locked(pagemap, vaddr, require_writable, confirmed_page, &dirty) &&
                 confirmed_page == physical_page;
        if (pinned) {
            auto const RESULT = file_mapping_index.retain_for_pin_locked(pagemap, root_incarnation, vaddr, binding);
            pinned = RESULT != file_mapping::FileMappingPinResult::REFUSE;
        }
    }
    user_mapping_pin_lock.unlock_irqrestore(LOCK_FLAGS);

    if (!pinned) {
        if (ref_acquired) {
            phys::page_ref_dec(hhdm_page, &lookup_hint);
        }
        return false;
    }

    out = {
        .pagemap = pagemap,
        .user_page = page_align_down(vaddr),
        .physical_page = physical_page,
        .hhdm_page = hhdm_page,
        .lookup_hint = lookup_hint,
        .access_gate_index = held_access_gate_index,
        .require_writable = require_writable,
        .dirty = dirty,
        .file_binding = binding,
        .root_incarnation = root_incarnation,
    };
    return true;
}

auto pin_user_page_common(sched::task::Task* task, PageTable* pagemap, vaddr_t vaddr, bool require_writable, bool fault_in,
                          bool wait_for_teardown, UserPagePin& out
#ifdef WOS_SELFTEST
                          ,
                          UserPagePinPathObservation* observation = nullptr
#endif
                          ) -> bool {
    out = {};
    size_t access_gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(pagemap, wait_for_teardown, access_gate_index)) {
        return false;
    }

    if (task != nullptr && require_writable && fault_in && task->pagemap == pagemap) {
#ifdef WOS_SELFTEST
        if (observation != nullptr) {
            ++observation->early_attempts;
        }
#endif
        if (try_pin_current_user_page_mapping(pagemap, vaddr, require_writable, access_gate_index, out)) {
#ifdef WOS_SELFTEST
            if (observation != nullptr) {
                ++observation->early_successes;
            }
#endif
            return true;
        }
    }

#ifdef WOS_SELFTEST
    if (observation != nullptr && task != nullptr && task->pagemap == pagemap && fault_in && require_writable) {
        ++observation->writable_resolution_calls;
    }
#endif
    bool const READY = task == nullptr ||
                       (task->pagemap == pagemap &&
                        (!fault_in || (require_writable ? ensure_user_page_writable(task, vaddr) : ensure_user_page_mapped(task, vaddr))));
    if (!READY || (task != nullptr && task->pagemap != pagemap)) {
        release_user_pagemap_access(access_gate_index);
        return false;
    }

    if (!try_pin_current_user_page_mapping(pagemap, vaddr, require_writable, access_gate_index, out)) {
        release_user_pagemap_access(access_gate_index);
        return false;
    }
    return true;
}

}  // namespace

auto pin_user_page(PageTable* pagemap, vaddr_t vaddr, bool require_writable, UserPagePin& out, bool wait_for_teardown) -> bool {
    return pin_user_page_common(nullptr, pagemap, vaddr, require_writable, false, wait_for_teardown, out);
}

auto pin_user_page_for_task(sched::task::Task* task, PageTable* expected_pagemap, vaddr_t vaddr, bool require_writable, bool fault_in,
                            UserPagePin& out) -> bool {
    if (task == nullptr) {
        out = {};
        return false;
    }
    return pin_user_page_common(task, expected_pagemap, vaddr, require_writable, fault_in, fault_in, out);
}

auto user_page_pin_still_mapped(const UserPagePin& pin) -> bool {
    if (pin.pagemap == nullptr || pin.hhdm_page == nullptr || pin.physical_page == PADDR_INVALID ||
        pin.access_gate_index == USER_PAGEMAP_ACCESS_GATE_NONE) {
        return false;
    }

    paddr_t current_page = PADDR_INVALID;
    uint64_t const LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(pin.pagemap);
    uint64_t const SERIAL = control != nullptr ? control->serial : 0;
    bool const MATCHES = snapshot_user_page_mapping_locked(pin.pagemap, pin.user_page, pin.require_writable, current_page) &&
                         current_page == pin.physical_page && SERIAL == pin.root_incarnation &&
                         file_mapping_index.matches_pin_locked(pin.pagemap, SERIAL, pin.user_page, pin.file_binding);
    user_mapping_pin_lock.unlock_irqrestore(LOCK_FLAGS);
    return MATCHES;
}

auto capture_user_futex_identity(const UserPagePin& pin, vaddr_t word, UserFutexIdentity& out) -> bool {
    if (out.root_incarnation != 0 || pin.pagemap == nullptr || pin.hhdm_page == nullptr || pin.physical_page == PADDR_INVALID ||
        pin.access_gate_index == USER_PAGEMAP_ACCESS_GATE_NONE || page_align_down(word) != pin.user_page ||
        (word % alignof(uint32_t)) != 0 || (word - pin.user_page) > paging::PAGE_SIZE - sizeof(uint32_t)) {
        return false;
    }
    paddr_t current_page = PADDR_INVALID;
    uint64_t leaf_flags = 0;
    UserFutexIdentity captured{};
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(pin.pagemap);
    bool const MATCHES = control != nullptr && control->serial != 0 && control->serial == pin.root_incarnation &&
                         file_mapping_index.matches_pin_locked(pin.pagemap, control->serial, word, pin.file_binding) &&
                         snapshot_user_page_mapping_locked(pin.pagemap, word, pin.require_writable, current_page, nullptr, &leaf_flags) &&
                         current_page == pin.physical_page;
    if (MATCHES) {
        captured = {.root_incarnation = control->serial,
                    .virtual_word = word,
                    .physical_word = current_page + (word - pin.user_page),
                    .shared = (leaf_flags & paging::PAGE_SHARED) != 0};
    }
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    if (MATCHES) out = captured;
    return MATCHES;
}

auto user_futex_identity_still_mapped(const UserPagePin& pin, const UserFutexIdentity& identity) -> bool {
    if (identity.root_incarnation == 0) return false;
    UserFutexIdentity current{};
    return capture_user_futex_identity(pin, identity.virtual_word, current) && current.root_incarnation == identity.root_incarnation &&
           current.virtual_word == identity.virtual_word && current.physical_word == identity.physical_word &&
           current.shared == identity.shared;
}

auto user_page_pin_commit_write(const UserPagePin& pin) -> bool {
    if (pin.pagemap == nullptr || pin.hhdm_page == nullptr || pin.physical_page == PADDR_INVALID || !pin.require_writable ||
        pin.access_gate_index == USER_PAGEMAP_ACCESS_GATE_NONE)
        return false;
    uint64_t const flags = user_mapping_pin_lock.lock_irqsave();
    auto const* control = find_user_root_control_locked(pin.pagemap);
    uint64_t const serial = control != nullptr ? control->serial : 0;
    UserLeafResolution leaf{};
    bool const matches = serial == pin.root_incarnation &&
                         file_mapping_index.matches_pin_locked(pin.pagemap, serial, pin.user_page, pin.file_binding) &&
                         resolve_user_leaf_locked(pin.pagemap, pin.user_page, true, leaf) && leaf.physical_4k == pin.physical_page;
    if (matches) {
        MutableUserEntrySlot current;
        if (!MutableUserEntrySlot::from(leaf.slot_table, leaf.slot_index, current)) __builtin_trap();
        current.mark_dirty();  // CURRENT PS or PML1 slot, never a saved PDE.
    }
    user_mapping_pin_lock.unlock_irqrestore(flags);
    return matches;
}

void unpin_user_page(UserPagePin& pin) {
    void* const PAGE = pin.hhdm_page;
    phys::PageLookupHint lookup_hint = pin.lookup_hint;
    size_t const ACCESS_GATE_INDEX = pin.access_gate_index;
    auto* binding = pin.file_binding;
    pin = {};
    if (PAGE != nullptr) {
        phys::page_ref_dec(PAGE, &lookup_hint);
    }
    release_user_pagemap_access(ACCESS_GATE_INDEX);
    if (binding != nullptr) binding->release(file_mapping::BindingOwner::PIN);
}

#ifdef WOS_SELFTEST
auto selftest_classify_private_demand(DemandOrigin origin, bool hardware_user, DemandBacking backing, numa::Policy policy,
                                      bool local_has_ram) noexcept -> DemandExclusion {
    return classify_private_demand(origin, hardware_user, backing, policy, local_has_ram);
}

auto selftest_observe_private_demand(sched::task::Task& subject, uint64_t address, bool writable, SelftestDemandObservation& output,
                                     SelftestDemandAction action, numa::Policy replacement) -> bool {
#ifdef WOS_KASAN
    if (kasan::is_enabled()) kasan::unpoison_range(&output, sizeof(output));
#endif
    output = {};
    output.disarmed = true;
    auto const* topology = acpi::numa::boot_topology();
    if (action == SelftestDemandAction::ChangePolicyAfterCapture &&
        (topology == nullptr || !numa::valid_policy(replacement, topology->node_count)))
        return false;
    if (address >= 0x0000800000000000ULL || subject.type != sched::task::TaskType::PROCESS || subject.is_thread || subject.pid != 0 ||
        subject.thread != nullptr || subject.state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE ||
        subject.scheduler_published.load(std::memory_order_acquire) || subject.shares_user_pagemap.load(std::memory_order_acquire) ||
        !subject.try_acquire())
        return false;
    PageTable* root = nullptr;
    if (!subject.try_acquire_usercopy_pagemap(root)) {
        subject.release();
        return false;
    }
    uint64_t serial = 0;
    bool const VALID =
        root != nullptr && root == subject.pagemap && root != kernel_pagemap && !is_current_pagemap(root) &&
        !selftest_kernel_page_table_frame(root) && capture_user_root_incarnation(root, serial) &&
        (!sched::can_query_current_task() || sched::get_current_task() != &subject) &&
        (action == SelftestDemandAction::None || action == SelftestDemandAction::ChangePolicyAfterCapture ||
         (writable && (action == SelftestDemandAction::FailCowAllocation || action == SelftestDemandAction::DowngradeCowRead ||
                       action == SelftestDemandAction::DowngradeCowNone)));
    uint64_t word = demand_observer.word.load(std::memory_order_acquire);
    bool result = false;
    if (VALID && word <= UINT64_MAX - 7 && (word & 3U) == 0U &&
        demand_observer.word.compare_exchange_strong(word, word + 3, std::memory_order_acq_rel)) {
        uint64_t const READY = word + 5;
        demand_observer.subject.store(&subject, std::memory_order_relaxed);
        demand_observer.root.store(root, std::memory_order_relaxed);
        demand_observer.root_serial.store(serial, std::memory_order_relaxed);
        demand_observer.address.store(address & ~(paging::PAGE_SIZE - 1), std::memory_order_relaxed);
        demand_observer.cpu.store(cpu::current_cpu(), std::memory_order_relaxed);
        demand_observer.output.store(&output, std::memory_order_relaxed);
        demand_observer.action.store(action, std::memory_order_relaxed);
        demand_observer.replacement_nodes.store(replacement.nodes, std::memory_order_relaxed);
        demand_observer.replacement_mode.store(replacement.mode, std::memory_order_relaxed);
        output.admitted = true;
        output.disarmed = false;
        output.cookie = READY;
        demand_observer.word.store(READY, std::memory_order_release);
        if (action == SelftestDemandAction::FailCowAllocation || action == SelftestDemandAction::DowngradeCowRead ||
            action == SelftestDemandAction::DowngradeCowNone) {
            alignas(SelftestCowObservation) uint8_t bytes[sizeof(SelftestCowObservation)];
            uint8_t value = 0;
#ifdef WOS_KASAN
            if (kasan::is_enabled()) {
                kasan::unpoison_range(bytes, sizeof(bytes));
                kasan::unpoison_range(&value, sizeof(value));
            }
#endif
            auto* cow = ::new (static_cast<void*>(bytes)) SelftestCowObservation{};
            SelftestCowAction const COW_ACTION =
                action == SelftestDemandAction::FailCowAllocation  ? SelftestCowAction::FAIL_DESTINATION_ALLOCATION
                : action == SelftestDemandAction::DowngradeCowRead ? SelftestCowAction::DOWNGRADE_READ_BEFORE_COMMIT
                                                                   : SelftestCowAction::DOWNGRADE_NONE_BEFORE_COMMIT;
            result = selftest_copy_to_task_cow_action(subject, address, &value, sizeof(value), COW_ACTION, *cow);
            output.action_consumed = cow->consumed == 1;
            cow->~SelftestCowObservation();
        } else {
            result = writable ? ensure_user_page_writable_for_task(&subject, address) : ensure_user_page_mapped_for_task(&subject, address);
        }
        demand_observer.word.store(word + 7, std::memory_order_release);
        demand_observer.output.store(nullptr, std::memory_order_relaxed);
        demand_observer.subject.store(nullptr, std::memory_order_relaxed);
        demand_observer.root.store(nullptr, std::memory_order_relaxed);
        demand_observer.word.store(word + 4, std::memory_order_release);
        output.disarmed = true;
    }
    subject.release_usercopy_pagemap();
    subject.release();
    return result;
}

auto selftest_copy_to_task_cow_action(sched::task::Task& subject, uint64_t address, const void* source, size_t size,
                                      SelftestCowAction action, SelftestCowObservation& observation) -> bool {
    observation = {};
    observation.disarmed = true;
    if (source == nullptr || size == 0 || address >= sys::usercopy::USER_ADDR_LIMIT ||
        size > paging::PAGE_SIZE - (address & (paging::PAGE_SIZE - 1)) ||
        (action != SelftestCowAction::FAIL_DESTINATION_ALLOCATION && action != SelftestCowAction::DOWNGRADE_READ_BEFORE_COMMIT &&
         action != SelftestCowAction::DOWNGRADE_NONE_BEFORE_COMMIT) ||
        subject.state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE || subject.type != sched::task::TaskType::PROCESS ||
        subject.is_thread || subject.pid != 0 || subject.thread != nullptr || subject.scheduler_published.load(std::memory_order_acquire) ||
        subject.shares_user_pagemap.load(std::memory_order_acquire) || !subject.try_acquire()) {
        return false;
    }
    PageTable* root = nullptr;
    if (!subject.try_acquire_usercopy_pagemap(root)) {
        subject.release();
        return false;
    }
    bool valid =
        root != nullptr && root == subject.pagemap && root != kernel_pagemap && !is_current_pagemap(root) &&
        !selftest_kernel_page_table_frame(root) && subject.state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
        subject.type == sched::task::TaskType::PROCESS && !subject.is_thread && subject.pid == 0 && subject.thread == nullptr &&
        !subject.scheduler_published.load(std::memory_order_acquire) && !subject.shares_user_pagemap.load(std::memory_order_acquire) &&
        (!sched::can_query_current_task() || sched::get_current_task() != &subject);
    paddr_t physical_page = PADDR_INVALID;
    if (valid) {
        uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
        valid = snapshot_user_page_mapping_locked(root, address, false, physical_page);
        if (valid) {
            auto* entry = leaf_entry(root, address);
            uint64_t const RAW = pte_raw(*entry);
            void* const FRAME = addr::get_virt_pointer(physical_page);
            valid = (RAW & (paging::PAGE_USER | paging::PAGE_COW | paging::PAGE_SHARED)) == (paging::PAGE_USER | paging::PAGE_COW) &&
                    !paging::shared_write_restricted(RAW) && phys::page_ref_get(FRAME) > 1 && !perf::is_local_vmem_zero_page(FRAME);
        }
        user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
    }
    bool copied = false;
    uint64_t word = selftest_cow_plan_word.load(std::memory_order_acquire);
    uint64_t const PREPARING = ((word & ~3ULL) + 4U) | 3U;
    if (valid && (word & 3U) == 0U &&
        selftest_cow_plan_word.compare_exchange_strong(word, PREPARING, std::memory_order_acq_rel, std::memory_order_acquire)) {
        selftest_cow_plan.subject.store(&subject, std::memory_order_relaxed);
        selftest_cow_plan.root.store(root, std::memory_order_relaxed);
        selftest_cow_plan.address.store(address, std::memory_order_relaxed);
        selftest_cow_plan.frame.store(physical_page, std::memory_order_relaxed);
        selftest_cow_plan.cpu.store(cpu::current_cpu(), std::memory_order_relaxed);
        selftest_cow_plan.action.store(action, std::memory_order_relaxed);
        selftest_cow_plan.observation.store(&observation, std::memory_order_relaxed);
        observation.admitted = true;
        observation.disarmed = false;
        selftest_cow_plan_word.store((PREPARING & ~3ULL) | 1U, std::memory_order_release);
        copied = sys::usercopy::copy_to_task(subject, address, source, size);
        selftest_cow_plan_word.store(PREPARING, std::memory_order_release);
        selftest_cow_plan.subject.store(nullptr, std::memory_order_relaxed);
        selftest_cow_plan.root.store(nullptr, std::memory_order_relaxed);
        selftest_cow_plan.address.store(0, std::memory_order_relaxed);
        selftest_cow_plan.frame.store(0, std::memory_order_relaxed);
        selftest_cow_plan.cpu.store(UINT64_MAX, std::memory_order_relaxed);
        selftest_cow_plan.action.store(SelftestCowAction::FAIL_DESTINATION_ALLOCATION, std::memory_order_relaxed);
        selftest_cow_plan.observation.store(nullptr, std::memory_order_relaxed);
        selftest_cow_plan_word.store(PREPARING & ~3ULL, std::memory_order_release);
        observation.disarmed = true;
    }
    subject.release_usercopy_pagemap();
    subject.release();
    return copied;
}

namespace {
// Only a caller-proven exclusive never-active fixture may use these delegates.
// Task refs keep storage; the actual root reader/PUB still protect mutation.
class SelftestManagedAccess {
   public:
    explicit SelftestManagedAccess(sched::task::Task& subject) : subject_(subject) {
        if (subject.state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE ||
            subject.exit_in_progress.load(std::memory_order_acquire) || subject.gc_queued.load(std::memory_order_acquire) ||
            subject.type != sched::task::TaskType::PROCESS || subject.is_thread || subject.pid != 0 || subject.thread != nullptr ||
            subject.scheduler_published.load(std::memory_order_acquire) || subject.shares_user_pagemap.load(std::memory_order_acquire) ||
            (sched::can_query_current_task() && sched::get_current_task() == &subject) || !subject.try_acquire())
            return;
        retained_ = true;
        if (!subject.try_acquire_usercopy_pagemap(root_)) return;
        accessed_ = true;
        uint64_t serial = 0;
        valid_ = root_ != nullptr && root_ == subject.pagemap && root_ != kernel_pagemap && !is_current_pagemap(root_) &&
                 !selftest_kernel_page_table_frame(root_) && capture_user_root_incarnation(root_, serial);
    }
    ~SelftestManagedAccess() {
        if (accessed_) subject_.release_usercopy_pagemap();
        if (retained_) subject_.release();
    }
    auto root() const -> PageTable* { return valid_ ? root_ : nullptr; }
    SelftestManagedAccess(const SelftestManagedAccess&) = delete;
    auto operator=(const SelftestManagedAccess&) -> SelftestManagedAccess& = delete;

   private:
    sched::task::Task& subject_;
    PageTable* root_{};
    bool retained_{}, accessed_{}, valid_{};
};
}  // namespace
auto selftest_publish_managed_huge(sched::task::Task& subject, uint64_t va, uint64_t flags, huge::PreparedSpan& span,
                                   huge::PreparedTables& parents, huge::PreparedTables& cleanup, SelftestManagedHugeReport& output)
    -> huge::Error {
    SelftestManagedAccess access(subject);
    if (access.root() == nullptr) return huge::Error::Invalid;
    auto const error = publish_managed_2m_prepared(access.root(), va, flags, span, parents, cleanup);
    if (error == huge::Error::None) {
        ++output.published;
        output.actual_full_flush_called = true;
    }
    return error;
}
auto selftest_prepare_managed_split(sched::task::Task& subject, uint64_t start, uint64_t end, huge::PreparedTables& plan,
                                    const SelftestManagedHugeOptions& options) -> huge::Error {
    SelftestManagedAccess access(subject);
    if (access.root() == nullptr) return huge::Error::Invalid;
    return prepare_managed_split_plan(access.root(), start, end, plan, options.fail_before_table);
}
auto selftest_commit_managed_split(sched::task::Task& subject, huge::PreparedTables& plan, SelftestManagedHugeReport& output)
    -> huge::Error {
    SelftestManagedAccess access(subject);
    if (access.root() == nullptr) return huge::Error::Invalid;
    huge::Error error;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        error = commit_managed_split_plan(access.root(), plan, publication);
    }
    if (error == huge::Error::None) {
        plan.reset();
        ++output.split;
        output.actual_full_flush_called = true;
    }
    return error;
}
auto selftest_split_managed_huge(sched::task::Task& subject, uint64_t va, huge::PreparedTables& cleanup, SelftestManagedHugeReport& output)
    -> huge::Error {
    auto* storage = huge::detail::PreparationAccess::plan_storage(cleanup);
    if (storage == nullptr || storage->start != va || storage->end != va + huge::LEAF_BYTES) return huge::Error::Invalid;
    return selftest_commit_managed_split(subject, cleanup, output);
}

bool selftest_user_pagemap_exclusive_rejects_mapped_pin(PageTable* pagemap) {
    if (pagemap == nullptr) {
        return false;
    }
    UserPagemapExclusiveScope exclusive(pagemap);
    UserPagePin pin{};
    return !pin_user_page(pagemap, 0, false, pin, false) && pin.pagemap == nullptr && pin.hhdm_page == nullptr;
}

auto selftest_pin_user_page_for_task_observed(sched::task::Task* task, PageTable* expected_pagemap, vaddr_t vaddr, bool require_writable,
                                              bool fault_in, UserPagePin& out, UserPagePinPathObservation& observation) -> bool {
    observation = {};
    if (task == nullptr) {
        out = {};
        return false;
    }
    return pin_user_page_common(task, expected_pagemap, vaddr, require_writable, fault_in, fault_in, out, &observation);
}

auto selftest_user_pagemap_reader_count(PageTable* pagemap) -> uint32_t {
    if (pagemap == nullptr) {
        return UINT32_MAX;
    }
    return user_pagemap_access_gates.at(user_pagemap_access_gate_index(pagemap)).readers.load(std::memory_order_acquire);
}

auto selftest_exchange_unpublished_user_entry(sched::task::Task& task, PageTable* expected_pagemap, vaddr_t vaddr, unsigned level,
                                              uint64_t expected_raw, uint64_t replacement_raw) -> bool {
    if (task.state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE || task.pid != 0 ||
        task.scheduler_published.load(std::memory_order_acquire) || task.thread != nullptr || expected_pagemap == nullptr ||
        task.pagemap != expected_pagemap || expected_pagemap == kernel_pagemap || is_current_pagemap(expected_pagemap) ||
        vaddr >= 0x0000800000000000ULL || level < 1 || level > 4) {
        return false;
    }
    uint64_t allowed = paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_COW;
    if (level == 2 || level == 3) {
        allowed |= 1ULL << 7U;
    }
    if (level == 1) {
        allowed |= paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED;
    }
    if (((expected_raw ^ replacement_raw) & ~allowed) != 0U) {
        return false;
    }

    uint64_t const LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTable* table = expected_pagemap;
    bool valid_path = true;
    for (unsigned current = 4; current > level; --current) {
        PageTableEntry& entry = entry_at(table, index_of(vaddr, static_cast<int>(current)));
        if (entry.present == 0 || entry.user == 0 || (current < 4 && entry.pagesize != 0)) {
            valid_path = false;
            break;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }
    bool exchanged = false;
    if (valid_path) {
        PageTableEntry& entry = entry_at(table, index_of(vaddr, static_cast<int>(level)));
        if (pte_raw(entry) == expected_raw) {
            entry = pte_from_raw(replacement_raw);
            exchanged = true;
        }
    }
    user_mapping_pin_lock.unlock_irqrestore(LOCK_FLAGS);
    return exchanged;
}
#endif

void note_tlb_shootdown_cpu_online() {
    uint64_t const CPU_NO = cpu::current_cpu();
    if (!cpu_slot_valid(CPU_NO)) {
        return;
    }
    if (tlb_shootdown_vector == 0 && (smt::get_core_count() > 1 || CPU_NO != 0))
        dbg::panic_handler("CPU cannot enter SMP scheduling without TLB shootdown");
    tlb_active_pagemaps[static_cast<size_t>(CPU_NO)].store(kernel_pagemap, std::memory_order_release);
    tlb_shootdown_cpu_online[static_cast<size_t>(CPU_NO)].store(1U, std::memory_order_release);
}

void init(limine_memmap_response* memmap_response_param, limine_executable_file_response* kernel_file_response_param,
          limine_executable_address_response* kernel_address_response_param) {
    memmap_response = memmap_response_param;
    kernel_file_response = kernel_file_response_param;
    kernel_address_response = kernel_address_response_param;
}

void switch_to_kernel_pagemap() { wrcr3(reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<paddr_t>(kernel_pagemap)))); }

auto get_kernel_pagemap() -> PageTable* { return kernel_pagemap; }

auto create_pagemap() -> PageTable* { return create_user_root_common(false); }
auto create_pagemap_may_fail() -> PageTable* { return create_user_root_common(true); }

#ifdef WOS_SELFTEST
auto selftest_create_pagemap(const SelftestRootFactoryOptions& options, SelftestRootFactoryOutput& output) -> PageTable* {
    output = {};
    return create_user_root_common(true, &options, &output);
}
#endif

namespace {
// Requires this exact root's existing exclusive lease and sole allocator owner.
// Caller provenance is passed through so the ordinary wrapper keeps its trace.
void release_pagemap_exclusive_owned(PageTable* pagemap, uint64_t owner_pid, void* caller) {
#ifdef WOS_SELFTEST
    ker::mod::mm::lifecycle_census::Operation selftest_operation(selftest_root_epoch);
#endif

#ifdef WOS_SELFTEST
    // Root pages are recycled directly into the page-table pool, so they do
    // not necessarily pass through page_free() or page_ref_dec(). Catch a
    // stale/corrupt root pointer before zero_page_table_for_pool() can erase a
    // live child of the permanent kernel hierarchy.
    if (selftest_kernel_page_table_frame(pagemap)) {
        dbg::emergency_log("illegal kernel page-table root release page=0x%lx caller=0x%lx\n", reinterpret_cast<uint64_t>(pagemap),
                           reinterpret_cast<uint64_t>(caller));
        ker::mod::dbg::panic_handler("attempted to recycle a permanent kernel page table as a user root");
    }
#endif

    // Validate the original sole allocator owner before irrevocably detaching
    // its control. A copied serial is not authority to release a reused root.
    if (phys::page_kind_get(pagemap) != PageKind::PAGE_TABLE || phys::page_ref_get(pagemap) != 1U) {
        log::critical("invalid final user-root owner root=%p owner_pid=%lu", pagemap, owner_pid);
        hcf();
    }
    UserRootControlChunk* empty = nullptr;
    uint64_t const CONTROL_FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto const* huge_control = find_user_root_control_locked(pagemap);
    bool const HUGE_EMPTY = huge_control != nullptr &&
                            !managed_huge_registry.root_has_published_owners({reinterpret_cast<uint64_t>(pagemap), huge_control->serial});
    bool const RETIRED = HUGE_EMPTY && file_mapping_index.root_empty_locked(pagemap) && retire_user_root_control_locked(pagemap, empty);
    user_mapping_pin_lock.unlock_irqrestore(CONTROL_FLAGS);
    if (!RETIRED) {
        log::critical("unregistered or corrupt user-root retirement root=%p owner_pid=%lu", pagemap, owner_pid);
        hcf();
    }
    if (empty != nullptr) phys::page_free(empty);

    owned_frame_purge_pagemap(pagemap);

    if (return_root_to_emergency_reserve(pagemap)) return;
    if (try_release_page_table_to_pool(pagemap, {caller, owner_pid})) {
        return;
    }

    phys::page_free(pagemap);
}
}  // namespace

void release_pagemap(PageTable* pagemap, uint64_t owner_pid) {
    if (pagemap == nullptr) {
        return;
    }

    if (pagemap == kernel_pagemap || is_current_pagemap(pagemap)) {
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    UserPagemapExclusiveScope teardown_scope(pagemap);
    release_pagemap_exclusive_owned(pagemap, owner_pid, __builtin_return_address(0));
}

void get_user_root_control_stats_snapshot(UserRootControlStats& out) {
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    out = {.active_roots = user_root_control_active_roots,
           .control_pages = user_root_control_pages,
           .last_serial = user_root_control_last_serial};
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
}

#ifdef WOS_SELFTEST
auto selftest_root_census() -> SelftestRootCensus {
    auto const selftest_before = selftest_root_epoch.snapshot();
    SelftestRootCensus out{};
    uint64_t const FLAGS = user_mapping_pin_lock.observe_lock_irqsave();
    out.roots = {.active_roots = user_root_control_active_roots,
                 .control_pages = user_root_control_pages,
                 .last_serial = user_root_control_last_serial};
    out.complete = file_mapping_index.selftest_count_all_locked(out.indexed_bindings);
    user_mapping_pin_lock.observe_unlock_irqrestore(FLAGS);
    out.stamp = selftest_root_epoch.snapshot();
    out.complete = out.complete && ker::mod::mm::lifecycle_census::unchanged(selftest_before, out.stamp);
    return out;
}
#endif

auto unpublished_user_root_empty(PageTable* root) -> bool {
    if (root == nullptr || root == kernel_pagemap || is_current_pagemap(root)) return false;
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(root, false, gate)) return false;
    bool empty = false;
    if (phys::page_kind_get(root) == PageKind::PAGE_TABLE && phys::page_ref_get(root) == 1U) {
        uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
        auto* control = find_user_root_control_locked(root);
        empty = control != nullptr && control->serial != 0 && file_mapping_index.root_empty_locked(root);
        for (size_t i = 0; empty && i < 256; ++i) empty = pte_raw(entry_at(root, i)) == 0;
        user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    }
    release_user_pagemap_access(gate);
    return empty;
}

auto prepare_unpublished_file_mapping_tables(PageTable* root, uint64_t start, uint64_t length, uint64_t owner_pid) -> bool {
    constexpr uint64_t USER_END = 0x0000800000000000ULL;
    constexpr uint64_t MAX_PAGES = 64;
    constexpr size_t MAX_TABLES = 6;
    constexpr uint64_t TABLE_FLAGS = paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE;
    if (root == nullptr || root == kernel_pagemap || is_current_pagemap(root) || !sched::interrupts_enabled() ||
        sched::preempt_count() != 0 || start < paging::PAGE_SIZE || start >= USER_END || length == 0 ||
        length > MAX_PAGES * paging::PAGE_SIZE || length > USER_END - start || ((start | length) & (paging::PAGE_SIZE - 1)) != 0)
        return false;

    // A <=64-page interval needs at most two tables at each upper level,
    // including an interval crossing a PML4 boundary. Allocate before taking
    // a root reader: reclaim may need to progress a colliding root teardown.
    std::array<PageTable*, MAX_TABLES> spares{};
    bool prepared = true;
    for (auto& spare : spares) {
        spare = alloc_zeroed_page_table(true);
        if (spare == nullptr) {
            prepared = false;
            break;
        }
    }

    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (prepared) prepared = acquire_user_pagemap_access(root, false, gate);
    if (prepared) {
        prepared = phys::page_kind_get(root) == PageKind::PAGE_TABLE && phys::page_ref_get(root) == 1U;
        if (prepared) {
            uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
            auto* control = find_user_root_control_locked(root);
            prepared = control != nullptr && control->serial != 0;
            size_t used = 0;
            for (uint64_t address = start; prepared && address < start + length; address += paging::PAGE_SIZE) {
                auto* table = root;
                for (int level = 4; prepared && level > 1; --level) {
                    auto& entry = entry_at(table, index_of(address, level));
                    if (!entry.present) {
                        if (pte_raw(entry) != 0 || used == spares.size()) {
                            prepared = false;
                            break;
                        }
                        auto* spare = std::exchange(spares.at(used++), nullptr);
                        auto const PHYSICAL = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(spare)));
                        // Transfer this sole table owner to the root. A later
                        // refusal preserves the prefix for ordinary teardown.
                        entry = paging::create_page_table_entry(PHYSICAL, TABLE_FLAGS);
                        table = spare;
                    } else {
                        if (entry.pagesize || !entry.user || !entry.writable || entry.no_execute || entry.frame == 0) {
                            prepared = false;
                            break;
                        }
                        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
                        if (phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1U) {
                            prepared = false;
                            break;
                        }
                    }
                }
                if (prepared && pte_raw(entry_at(table, index_of(address, 1))) != 0) prepared = false;
            }
            user_mapping_pin_lock.unlock_irqrestore(FLAGS);
        }
    }
    release_user_pagemap_access(gate);

    // Only private unused spares remain here. No pool operation, zeroing,
    // physical free or reclaim runs under the root reader or MM pin lock.
    for (auto* spare : spares) {
        if (spare != nullptr && !try_release_page_table_to_pool(spare, {__builtin_return_address(0), owner_pid})) phys::page_free(spare);
    }
    return prepared;
}

auto attach_unpublished_file_binding(file_mapping::FileMappingBinding*& prepared, PageTable* root, uint64_t start, uint64_t end,
                                     uint64_t file_offset) -> bool {
    if (prepared == nullptr || root == nullptr) return false;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(root);
    bool const ATTACHED = control != nullptr && prepared->configure_unpublished(root, control->serial, start, end, file_offset) &&
                          file_mapping_index.attach_locked(prepared, control->serial);
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    return ATTACHED;
}

auto snapshot_file_mapping_bindings(PageTable* root, file_mapping::FileMappingBinding** output, size_t capacity, size_t& owned_count)
    -> bool {
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(root, false, gate)) return false;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(root);
    bool const CAPTURED =
        control != nullptr && file_mapping_index.snapshot_root_locked(root, control->serial, output, capacity, owned_count);
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    release_user_pagemap_access(gate);
    return CAPTURED;
}

FileMappingInstallerLease::~FileMappingInstallerLease() {
    release_user_pagemap_access(access_gate_);
    if (binding_ != nullptr) binding_->release(file_mapping::BindingOwner::INSTALLER);
}

auto FileMappingInstallerLease::acquire(sched::task::Task& task, const sched::task::LazyVmemRange& range, uint64_t address) -> bool {
    if (binding_ != nullptr || access_gate_ != USER_PAGEMAP_ACCESS_GATE_NONE || !sched::task::lazy_vmem_has_lifetime(range)) return false;
    auto* root = task.pagemap;
    if (!acquire_user_pagemap_access(root, false, access_gate_)) return false;
    uint64_t const RANGE_FLAGS = task.lazy_vmem_lock.lock_irqsave();
    bool current = false;
    for (auto const& candidate : task.lazy_vmem_ranges) {
        if (task.pagemap != root || !lazy_file_range_matches(candidate, range, address)) continue;
        uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
        auto* control = find_user_root_control_locked(root);
        auto const RESULT = control != nullptr ? file_mapping_index.retain_for_pin_locked(root, control->serial, address, binding_)
                                               : file_mapping::FileMappingPinResult::REFUSE;
        if (RESULT == file_mapping::FileMappingPinResult::ACQUIRED) {
            current = file_binding_matches_range(*binding_, candidate) &&
                      binding_->reclassify(file_mapping::BindingOwner::PIN, file_mapping::BindingOwner::INSTALLER);
        }
        user_mapping_pin_lock.unlock_irqrestore(FLAGS);
        break;
    }
    task.lazy_vmem_lock.unlock_irqrestore(RANGE_FLAGS);
    if (!current) {
        release_user_pagemap_access(access_gate_);
        access_gate_ = USER_PAGEMAP_ACCESS_GATE_NONE;
        if (binding_ != nullptr) binding_->release(file_mapping::BindingOwner::PIN);
        binding_ = nullptr;
    }
    return current;
}

namespace {

// Always nested inside a short PreemptGuard. Never carried across preparation,
// backend I/O or a scheduling boundary, including Task's local root lease.
class DemandRootAccess {
   public:
    explicit DemandRootAccess(sched::task::Task& task) : task_(task) {
        local_ = task_.try_acquire_usercopy_pagemap(root_);
        if (local_ && !acquire_user_pagemap_access(root_, false, gate_)) root_ = nullptr;
    }
    ~DemandRootAccess() {
        release_user_pagemap_access(gate_);
        if (local_) task_.release_usercopy_pagemap();
    }
    [[nodiscard]] auto root() const -> PageTable* { return root_; }
    [[nodiscard]] auto reader_refused() const -> bool { return local_ && gate_ == USER_PAGEMAP_ACCESS_GATE_NONE; }
    DemandRootAccess(const DemandRootAccess&) = delete;
    auto operator=(const DemandRootAccess&) -> DemandRootAccess& = delete;

   private:
    sched::task::Task& task_;
    PageTable* root_{};
    size_t gate_{USER_PAGEMAP_ACCESS_GATE_NONE};
    bool local_{};
};

struct DemandPageSnapshot {
    sched::task::LazyVmemRange range{};
    file_mapping::FileMappingLifetime* family{};
    file_mapping::FileMappingBinding* binding{};
    file_mapping::BindingOwner binding_role{file_mapping::BindingOwner::PIN};
    uint64_t incarnation{};
    bool mapped{};
    bool retry{};
    ~DemandPageSnapshot() {
        if (binding != nullptr) binding->release(binding_role);
        if (family != nullptr) family->release(file_mapping::FamilyOwner::SNAPSHOT);
    }
};

auto capture_demand_page(sched::task::Task& task, uint64_t address, bool writable, DemandPageSnapshot& out) -> PublicReadPrepareResult {
    sched::PreemptGuard const PREEMPT;
    DemandRootAccess access(task);
    auto* root = access.root();
    if (root == nullptr) {
        out.retry = access.reader_refused();
        return PublicReadPrepareResult::REFUSED;
    }
    auto result = PublicReadPrepareResult::NOT_TRACKED;
    uint64_t const RANGE_FLAGS = task.lazy_vmem_lock.lock_irqsave();
    for (auto const& range : task.lazy_vmem_ranges) {
        if (address < range.start || address >= range.end) continue;
        if (!sched::task::lazy_vmem_is_tmpfs_shared(range)) break;
        result = PublicReadPrepareResult::REFUSED;
        if (!demand_range_policy_valid(range) || range.prot == 0 || (writable && (range.prot & ker::abi::vmem::PROT_WRITE) == 0)) break;
        uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
        auto* control = find_user_root_control_locked(root);
        file_mapping::FileMappingBinding* binding = nullptr;
        if (control != nullptr && file_mapping_index.retain_for_pin_locked(root, control->serial, address, binding) ==
                                      file_mapping::FileMappingPinResult::ACQUIRED) {
            out.binding = binding;
            if (file_binding_matches_range(*binding, range) &&
                binding->reclassify(file_mapping::BindingOwner::PIN, file_mapping::BindingOwner::SNAPSHOT)) {
                out.binding_role = file_mapping::BindingOwner::SNAPSHOT;
                if (range.lifetime->try_acquire(file_mapping::FamilyOwner::SNAPSHOT)) {
                    out.family = range.lifetime;
                    out.range = range;
                    out.incarnation = control->serial;
                    paddr_t physical = PADDR_INVALID;
                    out.mapped = snapshot_user_page_mapping_locked(root, address, writable, physical);
                    result = PublicReadPrepareResult::READY;
                }
            }
        }
        user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
        break;
    }
    task.lazy_vmem_lock.unlock_irqrestore(RANGE_FLAGS);
    return result;
}

struct DemandTableSpares {
    std::array<PageTable*, 3> pages{};
    ~DemandTableSpares() {
        for (auto* table : pages) {
            if (table != nullptr && !try_release_page_table_to_pool(table, {__builtin_return_address(0), 0})) phys::page_free(table);
        }
    }
    auto prepare() -> bool {
        for (auto*& table : pages) {
            table = alloc_zeroed_page_table(true);
            if (table == nullptr) return false;
        }
        return true;
    }
};

// cow+pin held. An absent literal-zero path is valid/cold without spares. Every
// adopted table already owns its frame; no allocation/promotion occurs here.
auto demand_leaf_locked(PageTable* root, uint64_t address, DemandTableSpares* spares, bool& valid, bool& changed) -> PageTableEntry* {
    valid = false;
    auto* table = root;
    size_t used = 0;
    for (int level = 4; level > 1; --level) {
        auto& entry = entry_at(table, index_of(address, level));
        if (!entry.present) {
            if (pte_raw(entry) != 0) return nullptr;
            if (spares == nullptr) {
                valid = true;
                return nullptr;
            }
            if (used == spares->pages.size()) return nullptr;
            auto* spare = std::exchange(spares->pages.at(used++), nullptr);
            if (spare == nullptr) return nullptr;
            auto const physical = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(spare)));
            entry = paging::create_page_table_entry(physical, paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE);
            table = spare;
            changed = true;
        } else {
            if (entry.pagesize || !entry.user || entry.frame == 0) return nullptr;
            table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
            if (phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1U) return nullptr;
        }
    }
    valid = true;
    return &entry_at(table, index_of(address, 1));
}

auto prepare_captured_tmpfs_read_shared_page(sched::task::Task& task, const sched::task::LazyVmemRange& range, uint64_t address,
                                             uint64_t root_incarnation, file_mapping::FileMappingBinding& binding) -> bool {
    if (!sched::interrupts_enabled() || sched::preempt_count() != 0 || root_incarnation == 0 || !demand_range_policy_valid(range) ||
        range.prot == 0 || !file_binding_matches_range(binding, range) || address < range.start || address >= range.end ||
        range.file_offset > UINT64_MAX - (address - range.start))
        return false;
    auto const* backing = range.lifetime->demand_read_shared_backing();
    if (backing == nullptr || range.file_size != backing->eof()) return false;

    DemandTableSpares spares;
    if (!spares.prepare()) return false;
    ker::vfs::tmpfs::TmpfsPageLoan loan;
#ifdef WOS_KASAN
    if (kasan::is_enabled()) kasan::unpoison_range(&loan, sizeof(loan));
#endif
    if (backing->prepare_page(range.file_offset + (address - range.start), loan) != 0) return false;
    auto const PHYSICAL = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(loan.frame())));
    bool mapped = false;
    bool installer_owned = false;
    bool retry = false;
    do {
        retry = false;
        {
            sched::PreemptGuard const PREEMPT;
            DemandRootAccess access(task);
            auto* root = access.root();
            retry = access.reader_refused();
            if (root != nullptr && task.pagemap == root && binding.root_incarnation() == root_incarnation) {
                bool changed = false;
                uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
                uint64_t const RANGE_FLAGS = task.lazy_vmem_lock.lock_irqsave();
                for (auto const& candidate : task.lazy_vmem_ranges) {
                    if (task.pagemap != root || !lazy_file_range_matches(candidate, range, address)) continue;
                    uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
                    auto* control = find_user_root_control_locked(root);
                    if (control != nullptr && control->serial == root_incarnation &&
                        file_mapping_index.matches_pin_locked(root, root_incarnation, address, &binding) &&
                        find_anonymous_swap_mapping_locked(root, address) == nullptr &&
                        binding.try_acquire(file_mapping::BindingOwner::INSTALLER)) {
                        installer_owned = true;
                        bool valid = false;
                        auto* leaf = demand_leaf_locked(root, address, &spares, valid, changed);
                        if (valid && leaf != nullptr) {
                            uint64_t const RAW = pte_raw(*leaf);
                            uint64_t const FLAGS = paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_SHARED | paging::PAGE_NX |
                                                   (range.lifetime->shared_writable() ? 0 : paging::PAGE_WRITE_RESTRICTED) |
                                                   ((range.prot & ker::abi::vmem::PROT_WRITE) != 0 ? paging::PAGE_WRITE : 0);
                            if (RAW == 0) {
                                *leaf = paging::create_page_table_entry(PHYSICAL, FLAGS);
                                (void)loan.release_to_pte();
                                mapped = changed = true;
                            } else if ((RAW & (paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_SHARED | paging::PAGE_WRITE |
                                               paging::PAGE_COW | paging::PAGE_WRITE_RESTRICTED | paging::PAGE_NX)) == FLAGS &&
                                       (leaf->frame << paging::PAGE_SHIFT) == PHYSICAL) {
                                mapped = true;
                            }
                        }
                    }
                    user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
                    break;
                }
                task.lazy_vmem_lock.unlock_irqrestore(RANGE_FLAGS);
                cow_pte_lock.unlock_irqrestore(COW_FLAGS);
                if (changed) flush_pagemap_after_update(root, address, true);
            }
        }
        if (retry) sched::kern_yield();
    } while (retry);
    if (installer_owned) binding.release(file_mapping::BindingOwner::INSTALLER);
    if (mapped && (range.prot & ker::abi::vmem::PROT_WRITE) != 0 && backing->sync_metadata() != 0)
        dbg::panic_handler("public TMPFS fault metadata lost committed context");
    // Loan and unused spare owners retire only after installer/local/root leases.
    return mapped;
}

auto current_process_root(sched::task::Task& task) -> bool {
    return sched::can_query_current_task() && sched::get_current_task() == &task && task.type == sched::task::TaskType::PROCESS &&
           task.state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE && task.pagemap != nullptr &&
           task.pagemap != kernel_pagemap;
}

}  // namespace

auto prepare_tmpfs_read_shared_page(sched::task::Task& task, uint64_t address, bool require_writable) -> PublicReadPrepareResult {
    if (address >= 0x0000800000000000ULL || !task.try_acquire()) return PublicReadPrepareResult::REFUSED;
    bool demand = false, allowed = false;
    uint64_t const FLAGS = task.lazy_vmem_lock.lock_irqsave();
    for (auto const& range : task.lazy_vmem_ranges) {
        if (address < range.start || address >= range.end) continue;
        demand = sched::task::lazy_vmem_is_tmpfs_shared(range);
        allowed =
            demand_range_policy_valid(range) && range.prot != 0 && (!require_writable || (range.prot & ker::abi::vmem::PROT_WRITE) != 0);
        break;
    }
    task.lazy_vmem_lock.unlock_irqrestore(FLAGS);
    if (!demand || !allowed) {
        task.release();
        return demand ? PublicReadPrepareResult::REFUSED : PublicReadPrepareResult::NOT_TRACKED;
    }
    PublicReadPrepareResult result;
    {
        std::optional<sys::SyscallPreparationScope> preparation;
        if (!sched::interrupts_enabled()) {
            if (!sched::can_query_current_task() || sched::get_current_task() == nullptr) {
                task.release();
                return PublicReadPrepareResult::REFUSED;
            }
            preparation.emplace(*sched::get_current_task());
            if (!preparation->ready()) {
                task.release();
                return PublicReadPrepareResult::REFUSED;
            }
        }
        DemandPageSnapshot snapshot;
        uint64_t const PAGE = page_align_down(address);
        do {
            snapshot.retry = false;
            result = capture_demand_page(task, PAGE, require_writable, snapshot);
            if (snapshot.retry) sched::kern_yield();
        } while (snapshot.retry);
        if (result == PublicReadPrepareResult::READY && !snapshot.mapped &&
            !prepare_captured_tmpfs_read_shared_page(task, snapshot.range, PAGE, snapshot.incarnation, *snapshot.binding))
            result = PublicReadPrepareResult::REFUSED;
    }
    task.release();
    return result;
}

auto capture_user_root_incarnation(PageTable* root, uint64_t& empty) noexcept -> bool {
    if (root == nullptr || empty != 0) return false;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(root);
    if (control != nullptr) empty = control->serial;
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    return empty != 0;
}

auto current_user_root_incarnation(sched::task::Task& task) -> uint64_t {
    if (!current_process_root(task)) return 0;
    sched::PreemptGuard const PREEMPT;
    DemandRootAccess access(task);
    uint64_t serial = 0;
    if (access.root() != nullptr) (void)capture_user_root_incarnation(access.root(), serial);
    return serial;
}

auto attach_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding*& prepared, uint64_t start, uint64_t end,
                                 uint64_t file_offset, uint64_t expected_serial) -> bool {
    if (!current_process_root(task) || prepared == nullptr || expected_serial == 0) return false;
    sched::PreemptGuard const PREEMPT;
    DemandRootAccess access(task);
    auto* root = access.root();
    if (root == nullptr) return false;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(root);
    bool const attached = control != nullptr && control->serial == expected_serial &&
                          prepared->configure_unpublished(root, expected_serial, start, end, file_offset) &&
                          file_mapping_index.attach_locked(prepared, expected_serial);
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    return attached;
}

auto query_current_file_binding(sched::task::Task& task, uint64_t start, uint64_t end, file_mapping::FileMappingBinding*& owned,
                                bool& exact) -> bool {
    if (!current_process_root(task) || owned != nullptr) return false;
    sched::PreemptGuard const PREEMPT;
    DemandRootAccess access(task);
    auto* root = access.root();
    if (root == nullptr) return false;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(root);
    bool const captured = control != nullptr && file_mapping_index.overlap_locked(root, control->serial, start, end, owned, exact);
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    return captured;
}

namespace {

auto demand_binding_matches_locked(PageTable* root, file_mapping::FileMappingBinding& binding, uint64_t address) -> bool {
    auto* control = find_user_root_control_locked(root);
    return control != nullptr && binding.root() == root && control->serial == binding.root_incarnation() &&
           binding.lifetime()->is_demand_shared() && file_mapping_index.matches_pin_locked(root, control->serial, address, &binding);
}

auto demand_leaf_valid_locked(PageTable* root, uint64_t address, file_mapping::FileMappingBinding& binding, PageTableEntry*& leaf) -> bool {
    if (find_anonymous_swap_mapping_locked(root, address) != nullptr) return false;
    bool valid = false, changed = false;
    leaf = demand_leaf_locked(root, address, nullptr, valid, changed);
    if (!valid || leaf == nullptr) return valid;
    uint64_t const RAW = pte_raw(*leaf);
    constexpr uint64_t MASK = paging::PAGE_PRESENT | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED | paging::PAGE_NX |
                              paging::PAGE_WRITE | paging::PAGE_COW;
    bool const WRITABLE = binding.lifetime()->shared_writable();
    uint64_t const REQUIRED = paging::PAGE_PRESENT | paging::PAGE_SHARED | paging::PAGE_NX | (WRITABLE ? 0 : paging::PAGE_WRITE_RESTRICTED);
    uint64_t const ALLOWED = WRITABLE ? paging::PAGE_WRITE : 0;
    return RAW == 0 || ((RAW & (MASK & ~ALLOWED)) == REQUIRED && leaf->frame != 0);
}

// A complete nonmutating pass precedes the first PTE effect. The current
// executing publisher plus caller's publication guard keep this root alive
// between slices; this is intentionally not an arbitrary-Task/root API.
auto preflight_current_demand_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding) -> bool {
    if (!current_process_root(task) || task.pagemap != binding.root() || binding.start() >= binding.end()) return false;
    for (uint64_t cursor = binding.start(); cursor < binding.end();) {
        uint64_t const END = cursor + std::min<uint64_t>(binding.end() - cursor, 64 * paging::PAGE_SIZE);
        bool valid = false;
        {
            sched::PreemptGuard const PREEMPT;
            DemandRootAccess access(task);
            auto* root = access.root();
            if (root == nullptr) return false;
            uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
            uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
            valid = demand_binding_matches_locked(root, binding, cursor);
            for (uint64_t page = cursor; valid && page < END; page += paging::PAGE_SIZE) {
                PageTableEntry* leaf = nullptr;
                valid = demand_leaf_valid_locked(root, page, binding, leaf);
            }
            user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
            cow_pte_lock.unlock_irqrestore(COW_FLAGS);
        }
        if (!valid) return false;
        cursor = END;
    }
    return true;
}

auto mutate_current_demand_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding, bool remove, uint64_t prot,
                                   file_mapping::FileMappingTlbRetirement& retirement) -> bool {
    if (!retirement.empty() || !current_process_root(task) || task.pagemap != binding.root() ||
        (!remove && (prot & ~(ker::abi::vmem::PROT_READ | (binding.lifetime()->shared_writable() ? ker::abi::vmem::PROT_WRITE : 0))) != 0))
        dbg::panic_handler("public TMPFS mutation entered without validated current authority");
    for (uint64_t cursor = binding.start(); cursor < binding.end();) {
        uint64_t const END = cursor + std::min<uint64_t>(binding.end() - cursor, 64 * paging::PAGE_SIZE);
        std::array<PageTableEntry, 64> old{};
        size_t count = 0;
        bool acquired = false;
        {
            sched::PreemptGuard const PREEMPT;
            DemandRootAccess access(task);
            auto* root = access.root();
            if (root != nullptr) {
                acquired = true;
                uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
                uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
                if (!current_process_root(task) || !demand_binding_matches_locked(root, binding, cursor))
                    dbg::panic_handler("public TMPFS mutation lost current root/binding authority");
                for (uint64_t page = cursor; page < END; page += paging::PAGE_SIZE) {
                    PageTableEntry* leaf = nullptr;
                    if (!demand_leaf_valid_locked(root, page, binding, leaf))
                        dbg::panic_handler("public TMPFS leaf changed after withdrawal preflight");
                    if (leaf == nullptr || !leaf->present) continue;
                    if (remove) {
                        old.at(count++) = *leaf;
                        owned_frame_untrack_leaf(root, page, *leaf);
                        *leaf = {};
                    } else {
                        // Hardware can update A/D concurrently. Preserve those
                        // bits with the actual atomic permission operation.
                        auto* raw = reinterpret_cast<uint64_t*>(leaf);
                        uint64_t observed = __atomic_load_n(raw, __ATOMIC_ACQUIRE);
                        uint64_t desired = 0;
                        do {
                            desired = (observed & ~(paging::PAGE_USER | paging::PAGE_WRITE)) | (prot != 0 ? paging::PAGE_USER : 0) |
                                      ((prot & ker::abi::vmem::PROT_WRITE) != 0 ? paging::PAGE_WRITE : 0);
                        } while (!__atomic_compare_exchange_n(raw, &observed, desired, true, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));
                    }
                }
                user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
                cow_pte_lock.unlock_irqrestore(COW_FLAGS);
                flush_pagemap_after_update(root, 0, true);
            }
        }
        if (!acquired) {
            // Same-root fork requires publication, and current execution is a
            // live publisher, so only an unrelated hashed exclusive can refuse.
            // No reader survives this yield; never expose partial EAGAIN.
            sched::kern_yield();
            continue;
        }
        for (size_t i = 0; i < count; ++i) drop_present_leaf_ref(old.at(i));
        cursor = END;
    }
    if (remove) {
        bool detached = false;
        while (!detached) {
            {
                sched::PreemptGuard const PREEMPT;
                DemandRootAccess access(task);
                auto* root = access.root();
                if (root != nullptr) {
                    uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
                    if (!current_process_root(task) || !demand_binding_matches_locked(root, binding, binding.start()) ||
                        !file_mapping_index.detach_locked(binding, retirement))
                        dbg::panic_handler("public TMPFS final index retirement lost authority");
                    user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
                    flush_pagemap_after_update(root, 0, true);
                    detached = true;
                }
            }
            if (!detached) sched::kern_yield();
        }
    }
    return true;
}

}  // namespace

auto preflight_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding) -> bool {
    return preflight_current_demand_binding(task, binding);
}

auto retire_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding,
                                 file_mapping::FileMappingTlbRetirement& after_tlb) -> bool {
    return mutate_current_demand_binding(task, binding, true, 0, after_tlb);
}

auto protect_current_file_binding(sched::task::Task& task, file_mapping::FileMappingBinding& binding, uint64_t prot) -> bool {
    file_mapping::FileMappingTlbRetirement empty;
    return mutate_current_demand_binding(task, binding, false, prot, empty);
}

#ifdef WOS_SELFTEST
auto selftest_user_root_incarnation(PageTable* owned_root) -> uint64_t {
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(owned_root, false, gate)) return 0;
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(owned_root);
    uint64_t const SERIAL = control != nullptr ? control->serial : 0;
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    release_user_pagemap_access(gate);
    return SERIAL;
}
#endif

void get_page_table_pool_stats_snapshot(PageTablePoolStatsSnapshot& out) {
    uint64_t const FLAGS = page_table_pool.lock.lock_irqsave();
    uint64_t const CACHED_PAGES = page_table_pool.count;
    page_table_pool.lock.unlock_irqrestore(FLAGS);

    out = {
        .capacity = PAGE_TABLE_POOL_CAPACITY,
        .cached_pages = CACHED_PAGES,
        .alloc_hits = page_table_pool_alloc_hits.load(std::memory_order_relaxed),
        .alloc_misses = page_table_pool_alloc_misses.load(std::memory_order_relaxed),
        .releases = page_table_pool_releases.load(std::memory_order_relaxed),
        .rejects = page_table_pool_rejects.load(std::memory_order_relaxed),
    };
}

void get_owned_frame_stats_snapshot(OwnedFrameStatsSnapshot& out) {
    out = {
        .capacity = OWNED_FRAME_TRACKING_ENABLED ? OWNED_FRAME_TABLE_CAPACITY : 0,
        .entries = owned_frame_table.tracked.load(std::memory_order_relaxed),
        .track_attempts = owned_frame_stats.track_attempts.load(std::memory_order_relaxed),
        .track_added = owned_frame_stats.track_added.load(std::memory_order_relaxed),
        .track_replaced = owned_frame_stats.track_replaced.load(std::memory_order_relaxed),
        .track_skipped = owned_frame_stats.track_skipped.load(std::memory_order_relaxed),
        .track_conflicts = owned_frame_stats.track_conflicts.load(std::memory_order_relaxed),
        .track_probe_failures = owned_frame_stats.track_probe_failures.load(std::memory_order_relaxed),
        .untrack_attempts = owned_frame_stats.untrack_attempts.load(std::memory_order_relaxed),
        .untrack_removed = owned_frame_stats.untrack_removed.load(std::memory_order_relaxed),
        .untrack_missed = owned_frame_stats.untrack_missed.load(std::memory_order_relaxed),
        .purge_calls = owned_frame_stats.purge_calls.load(std::memory_order_relaxed),
        .purge_removed = owned_frame_stats.purge_removed.load(std::memory_order_relaxed),
    };
}

void copy_kernel_mappings_to(PageTable* pagemap) { refresh_kernel_mappings(pagemap); }

void copy_kernel_mappings(sched::task::Task* t) {
    if (t == nullptr) {
        return;
    }
    refresh_kernel_mappings(t->pagemap);
}

void switch_pagemap(sched::task::Task* t) {
    if (t->pagemap == nullptr) {
        [[unlikely]]
        if (t->name != nullptr) {
            log::critical("task %s has no pagemap", t->name);
        } else {
            log::critical("task has no pagemap; halting");
        }
        hcf();
    }

    refresh_kernel_mappings(t->pagemap);

    auto phys_pagemap = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(t->pagemap)));
#ifdef VERBOSE_PAGEMAP_SWITCH
    log::debug("switchPagemap: task=%s pid=%d virt=0x%x phys=0x%x", (t->name != nullptr) ? t->name : "unknown", t->pid,
               static_cast<unsigned int>(reinterpret_cast<uintptr_t>(t->pagemap)), static_cast<unsigned int>(phys_pagemap));
#endif
    // Always reload CR3 on task switches.
    //
    // Comparing only the physical PML4 frame is unsafe because page-table
    // pages are freed and later reused. If a new address space reuses the same
    // physical PML4 frame as an old one, skipping the CR3 write leaves stale
    // TLB entries from the previous owner alive and can produce impossible
    // user faults on pages whose current PTEs are present and writable.
    note_active_pagemap(nullptr);
    wrcr3(phys_pagemap);
    note_active_pagemap(t->pagemap);
}

auto pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& gpr) -> bool {
    PageFault const PAGEFAULT = paging::create_page_fault(frame.err_code, true);

#ifdef WOS_KASAN
    // KASan shadow-region demand paging.  Shadow pages are zeroed (accessible)
    // on first access; KASan poisons them selectively afterwards.
    // This must run before the COW path to avoid mistaking a shadow fault for
    // a kernel panic.
    if (kasan::handle_shadow_fault(control_register)) {
        return true;
    }
#endif

#ifdef WOS_SELFTEST
    if (expected_kernel_page_fault.state.load(std::memory_order_acquire) == ExpectedPageFaultState::ARMED &&
        control_register == expected_kernel_page_fault.fault_address && frame.rip == expected_kernel_page_fault.fault_rip &&
        (frame.err_code & expected_kernel_page_fault.error_mask) == expected_kernel_page_fault.expected_error) {
        frame.rip = expected_kernel_page_fault.recovery_rip;
        expected_kernel_page_fault.state.store(ExpectedPageFaultState::HANDLED, std::memory_order_release);
        return true;
    }
#endif

    // Lazy user-stack backing. This handles non-present faults in the
    // reserved process stack range, including kernel-mode faults caused by
    // syscall copy paths writing into a not-yet-backed user stack page.
    if (PAGEFAULT.present == 0U && control_register < 0x0000800000000000ULL) {
        auto* current_task = sched::get_current_task();
        uint64_t wait_generation = 0;
        auto const SWAP_FAULT = probe_anonymous_swap_fault(current_task, control_register, PAGEFAULT, wait_generation);
        if (SWAP_FAULT == AnonymousSwapFaultDisposition::PARK) {
            while (current_task->anonymous_swap_completion_generation.load(std::memory_order_acquire) != wait_generation) {
                auto const WAIT = sched::place_task_in_wait_queue_if_pending(gpr, frame, current_task->anonymous_swap_completion_generation,
                                                                             wait_generation);
                if (WAIT == sched::ExceptionWaitResult::PARKED) {
                    return true;
                }
            }
            return translate(current_task->pagemap, page_align_down(control_register)) != PADDR_INVALID;
        }
        if (SWAP_FAULT == AnonymousSwapFaultDisposition::RESOLVED) {
            return true;
        }
        if (SWAP_FAULT == AnonymousSwapFaultDisposition::FAILED) {
            return false;
        }
#ifdef WOS_ASYNC_BLOCK_IO
        auto const FILE_FAULT = probe_lazy_file_fault(current_task, control_register, PAGEFAULT, (frame.cs & 3U) == 3U, wait_generation);
        if (FILE_FAULT == LazyFileFaultDisposition::PARK) {
            while (current_task->lazy_file_fault_completion_generation.load(std::memory_order_acquire) != wait_generation) {
                auto const WAIT = sched::place_task_in_wait_queue_if_pending(
                    gpr, frame, current_task->lazy_file_fault_completion_generation, wait_generation);
                if (WAIT == sched::ExceptionWaitResult::PARKED) return true;
            }
            return current_task->lazy_file_fault_status == 0 &&
                   translate(current_task->pagemap, page_align_down(control_register)) != PADDR_INVALID;
        }
        if (FILE_FAULT == LazyFileFaultDisposition::RESOLVED) return true;
        if (FILE_FAULT == LazyFileFaultDisposition::FAILED) return false;
#endif
#ifdef WOS_ASYNC_BLOCK_IO
        constexpr bool ALLOW_FILE_IO = false;
#else
        constexpr bool ALLOW_FILE_IO = true;
#endif
        if (current_task != nullptr && current_task->pagemap != nullptr) {
            DemandContextStorage demand_storage;
            auto& demand = demand_storage.get();
            if (capture_demand_context(current_task, control_register, DemandOrigin::HardwareFault, (frame.cs & 3U) == 3U, demand) &&
                handle_lazy_vmem_fault(current_task, control_register, PAGEFAULT, demand, frame.rip, frame.rsp, ALLOW_FILE_IO))
                return true;
        }
        if (current_task != nullptr && current_task->pagemap != nullptr && current_task->thread != nullptr &&
            sched::threading::handle_lazy_stack_fault(current_task->thread, current_task->pagemap, control_register, frame.rsp,
                                                      current_task)) {
            return true;
        }
    }

    // COW handling for write faults to user-space COW pages.
    // This covers both user-mode writes AND kernel-mode writes to user pages
    // (e.g. syscall read() copying data into a user buffer via memcpy).
    // Guard: only handle addresses in the user-space half of the canonical
    // address space to ensure we never touch kernel page-table entries.
    if ((PAGEFAULT.writable != 0U) && (control_register < 0x0000800000000000ULL)) {
        auto* current_task = sched::get_current_task();
        if (current_task == nullptr || current_task->pagemap == nullptr) {
            log::error("COW fault: no current task or pagemap");
            hcf();
            return false;
        }

        DemandContextStorage demand_storage;
        auto& demand = demand_storage.get();
        if (capture_demand_context(current_task, control_register, DemandOrigin::HardwareFault, (frame.cs & 3U) == 3U, demand) &&
            resolve_user_write_mapping(current_task, control_register, demand) == UserWriteFaultStatus::HANDLED) {
            return true;
        }
    }

    if (control_register < 0x0000800000000000ULL) {
        auto* current_task = sched::get_current_task();
        paddr_t const PHYS = current_task != nullptr && current_task->pagemap != nullptr
                                 ? translate(current_task->pagemap, control_register)
                                 : PADDR_INVALID;
        log::warn(
            "user page fault: pid=%llu name=%s fault=0x%llx rip=0x%llx rsp=0x%llx cs=0x%llx err=0x%llx present=%u write=%u user=%u "
            "phys=0x%llx",
            static_cast<unsigned long long>(current_task != nullptr ? current_task->pid : 0),
            current_task != nullptr && current_task->name != nullptr ? current_task->name : "?",
            static_cast<unsigned long long>(control_register), static_cast<unsigned long long>(frame.rip),
            static_cast<unsigned long long>(frame.rsp), static_cast<unsigned long long>(frame.cs),
            static_cast<unsigned long long>(frame.err_code), PAGEFAULT.present != 0U ? 1U : 0U, PAGEFAULT.writable != 0U ? 1U : 0U,
            PAGEFAULT.user != 0U ? 1U : 0U, static_cast<unsigned long long>(PHYS));
    }

    // Not a COW fault - let the caller handle it (userspace crash / kernel panic).
    return false;
}

#ifdef WOS_SELFTEST
auto selftest_arm_expected_kernel_page_fault(vaddr_t fault_address, vaddr_t fault_rip, vaddr_t recovery_rip, uint64_t expected_error,
                                             uint64_t error_mask) -> bool {
    ExpectedPageFaultState expected = ExpectedPageFaultState::IDLE;
    if (!expected_kernel_page_fault.state.compare_exchange_strong(expected, ExpectedPageFaultState::CONFIGURING, std::memory_order_acq_rel,
                                                                  std::memory_order_acquire)) {
        return false;
    }
    expected_kernel_page_fault.fault_address = fault_address;
    expected_kernel_page_fault.fault_rip = fault_rip;
    expected_kernel_page_fault.recovery_rip = recovery_rip;
    expected_kernel_page_fault.expected_error = expected_error;
    expected_kernel_page_fault.error_mask = error_mask;
    expected_kernel_page_fault.state.store(ExpectedPageFaultState::ARMED, std::memory_order_release);
    return true;
}

auto selftest_finish_expected_kernel_page_fault() -> bool {
    ExpectedPageFaultState const STATE = expected_kernel_page_fault.state.exchange(ExpectedPageFaultState::IDLE, std::memory_order_acq_rel);
    return STATE == ExpectedPageFaultState::HANDLED;
}
#endif

paddr_t translate(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        log::critical("translate: no page table");
        hcf();
    }

    PageTable const* table = page_table;
    for (int level = 4; level > 1; level--) {
        const auto& entry = entry_at(table, index_of(vaddr, level));
        if (!entry.present) {
            return PADDR_INVALID;
        }
        uint64_t const PHYS = entry.frame << paging::PAGE_SHIFT;
        if (level == 3 && entry.pagesize != 0) {
            return PHYS + (vaddr & (LARGE_PAGE_1G_BYTES - 1));
        }
        if (level == 2 && entry.pagesize != 0) {
            return PHYS + (vaddr & (LARGE_PAGE_2M_BYTES - 1));
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(PHYS));
    }

    const auto& pte = entry_at(table, index_of(vaddr, 1));
    if (!pte.present) {
        return PADDR_INVALID;
    }
    uint64_t const PHYS = (pte.frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1));
    return PHYS;  // Return physical address only
}

#ifdef WOS_SELFTEST
bool direct_map_selftest_ready = false;

namespace {

constexpr size_t SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY = 1U << 15U;
static_assert((SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY & (SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY - 1U)) == 0U);

std::array<std::atomic<paddr_t>, SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY>
    selftest_kernel_page_table_registry{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

__attribute__((no_sanitize("address"))) auto selftest_kernel_page_table_hash(paddr_t phys) -> size_t {
    uint64_t page = phys >> paging::PAGE_SHIFT;
    page ^= page >> 33U;
    page *= 0xff51afd7ed558ccdULL;
    page ^= page >> 33U;
    return static_cast<size_t>(page) & (SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY - 1U);
}

__attribute__((no_sanitize("address"))) void selftest_register_kernel_page_table(PageTable const* table) {
    if (table == nullptr) {
        return;
    }

    paddr_t const PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(table))) & ~(paging::PAGE_SIZE - 1);
    if (PHYS == 0) {
        ker::mod::dbg::panic_handler("kernel page-table registry rejected physical page zero");
    }

    size_t slot = selftest_kernel_page_table_hash(PHYS);
    for (size_t probe = 0; probe < selftest_kernel_page_table_registry.size(); ++probe) {
        auto& candidate = selftest_kernel_page_table_registry.at(slot);
        paddr_t observed = candidate.load(std::memory_order_acquire);
        if (observed == PHYS) {
            return;
        }
        if (observed == 0 && candidate.compare_exchange_strong(observed, PHYS, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
        slot = (slot + 1U) & (SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY - 1U);
    }

    ker::mod::dbg::panic_handler("kernel page-table selftest registry exhausted");
}

__attribute__((no_sanitize("address"))) void selftest_register_kernel_page_table_tree(PageTable const* table, int level, bool root) {
    if (table == nullptr || level < 1) {
        return;
    }
    selftest_register_kernel_page_table(table);
    if (level == 1) {
        return;
    }

    size_t const FIRST_ENTRY = root ? KERNEL_PML4_START : 0;
    size_t const LAST_ENTRY = root ? KERNEL_PML4_END : paging::PAGE_TABLE_ENTRIES;
    for (size_t index = FIRST_ENTRY; index < LAST_ENTRY; ++index) {
        PageTableEntry const& entry = table->entries.at(index);
        if (entry.present == 0 || entry.pagesize != 0) {
            continue;
        }
        auto* const CHILD = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
        selftest_register_kernel_page_table_tree(CHILD, level - 1, false);
    }
}

__attribute__((no_sanitize("address"))) void selftest_register_kernel_page_table_path(vaddr_t vaddr) {
    PageTable const* table = kernel_pagemap;
    if (table == nullptr) {
        return;
    }

    for (int level = 4; level > 1; --level) {
        selftest_register_kernel_page_table(table);
        PageTableEntry const& entry = table->entries.at(index_of(vaddr, level));
        if (entry.present == 0 || entry.pagesize != 0) {
            return;
        }
        table = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }
    selftest_register_kernel_page_table(table);
}

__attribute__((no_sanitize("address"))) void selftest_register_kernel_page_tables() {
    selftest_register_kernel_page_table_tree(kernel_pagemap, 4, true);
}

}  // namespace

__attribute__((no_sanitize("address"))) bool selftest_kernel_page_table_frame(const void* ptr) {
    if (!direct_map_selftest_ready || ptr == nullptr) {
        return false;
    }

    vaddr_t const VADDR = reinterpret_cast<vaddr_t>(ptr);
    uint64_t const HHDM_OFFSET = addr::get_hhdm_offset();
    if (VADDR < HHDM_OFFSET) {
        return false;
    }
    paddr_t const PHYS = (VADDR - HHDM_OFFSET) & ~(paging::PAGE_SIZE - 1);
    if (PHYS == 0) {
        return false;
    }

    size_t slot = selftest_kernel_page_table_hash(PHYS);
    for (size_t probe = 0; probe < selftest_kernel_page_table_registry.size(); ++probe) {
        paddr_t const OBSERVED = selftest_kernel_page_table_registry.at(slot).load(std::memory_order_acquire);
        if (OBSERVED == PHYS) {
            return true;
        }
        if (OBSERVED == 0) {
            return false;
        }
        slot = (slot + 1U) & (SELFTEST_KERNEL_PAGE_TABLE_REGISTRY_CAPACITY - 1U);
    }
    return false;
}

__attribute__((no_sanitize("address"))) auto selftest_kernel_page_table_count() -> uint64_t {
    uint64_t count = 0;
    for (const auto& frame : selftest_kernel_page_table_registry) {
        if (frame.load(std::memory_order_acquire) != 0) ++count;
    }
    return count;
}

namespace {

__attribute__((no_sanitize("address"))) void selftest_require_mutable_pagemap_root(PageTable const* root, const char* phase,
                                                                                   vaddr_t vaddr) {
    if (root == nullptr || root == kernel_pagemap || !selftest_kernel_page_table_frame(root)) {
        return;
    }

    dbg::emergency_log("permanent kernel page-table used as mutable pagemap root phase=%s root=0x%lx vaddr=0x%lx caller=0x%lx\n",
                       phase != nullptr ? phase : "?", reinterpret_cast<uint64_t>(root), vaddr,
                       reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    ker::mod::dbg::panic_handler("permanent kernel page table used as a non-kernel pagemap root");
}

}  // namespace

__attribute__((no_sanitize("address"))) bool selftest_direct_map_contains(const void* ptr) {
    if (!direct_map_selftest_ready) {
        return true;
    }
    if (ptr == nullptr || kernel_pagemap == nullptr) {
        return false;
    }

    vaddr_t const vaddr = reinterpret_cast<vaddr_t>(ptr);
    uint64_t const hhdm_offset = addr::get_hhdm_offset();
    if (vaddr < hhdm_offset) {
        return false;
    }
    paddr_t const expected = (vaddr - hhdm_offset) & ~(paging::PAGE_SIZE - 1);

    std::array<uint64_t, 4> raw{};
    std::array<paddr_t, 3> table_phys{};
    PageTable const* table = kernel_pagemap;
    size_t failed_level = 0;
    paddr_t mapped = PADDR_INVALID;
    for (int level = 4; level > 1; --level) {
        size_t const index = (vaddr >> (12 + (9 * (level - 1)))) & 0x1FF;
        PageTableEntry const& entry = table->entries[index];
        std::memcpy(&raw.at(static_cast<size_t>(4 - level)), &entry, sizeof(uint64_t));
        if (entry.present == 0) {
            failed_level = static_cast<size_t>(level);
            break;
        }

        paddr_t const phys = entry.frame << paging::PAGE_SHIFT;
        if (level == 3 && entry.pagesize != 0) {
            mapped = phys + (vaddr & (LARGE_PAGE_1G_BYTES - 1));
            break;
        }
        if (level == 2 && entry.pagesize != 0) {
            mapped = phys + (vaddr & (LARGE_PAGE_2M_BYTES - 1));
            break;
        }
        table_phys.at(static_cast<size_t>(4 - level)) = phys;
        table = reinterpret_cast<PageTable const*>(addr::get_virt_pointer(phys));
    }

    if (mapped == PADDR_INVALID && failed_level == 0) {
        size_t const index = (vaddr >> paging::PAGE_SHIFT) & 0x1FF;
        PageTableEntry const& entry = table->entries[index];
        std::memcpy(&raw.at(3), &entry, sizeof(uint64_t));
        if (entry.present != 0) {
            mapped = (entry.frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1));
        } else {
            failed_level = 1;
        }
    }

    if ((mapped & ~(paging::PAGE_SIZE - 1)) == expected) {
        return true;
    }

    dbg::emergency_log(
        "direct-map pointer audit failed vaddr=0x%lx expected=0x%lx mapped=0x%lx failed_level=%lu "
        "raw4=0x%lx raw3=0x%lx raw2=0x%lx raw1=0x%lx table3=0x%lx table2=0x%lx table1=0x%lx\n",
        vaddr, expected, mapped, static_cast<uint64_t>(failed_level), raw.at(0), raw.at(1), raw.at(2), raw.at(3), table_phys.at(0),
        table_phys.at(1), table_phys.at(2));
    for (size_t i = 0; i < table_phys.size(); ++i) {
        if (table_phys.at(i) == 0) {
            continue;
        }
        auto* const table_page = reinterpret_cast<void*>(addr::get_virt_pointer(table_phys.at(i)));
        dbg::emergency_log("direct-map table level=%lu page=0x%lx kind=%lu ref=%lu\n", static_cast<uint64_t>(3 - i),
                           reinterpret_cast<uint64_t>(table_page), static_cast<uint64_t>(phys::page_kind_get(table_page)),
                           static_cast<uint64_t>(phys::page_ref_get(table_page)));
    }
    return false;
}

namespace {

constexpr size_t SELFTEST_DIRECT_MAP_EDGE_PAGES = 8;

__attribute__((no_sanitize("address"))) void selftest_require_direct_map_phys(paddr_t phys, const char* phase) {
    auto const* ptr = reinterpret_cast<const void*>(addr::get_virt_pointer(phys));
    if (selftest_direct_map_contains(ptr)) {
        return;
    }

    dbg::emergency_log("boot direct-map boundary audit failed phase=%s phys=0x%lx\n", phase != nullptr ? phase : "?", phys);
    ker::mod::dbg::panic_handler("boot direct-map boundary audit found a missing identity mapping");
}

__attribute__((no_sanitize("address"))) void selftest_audit_boot_direct_map_boundaries(const char* phase) {
    if (memmap_response == nullptr) {
        ker::mod::dbg::panic_handler("boot direct-map boundary audit has no memory map");
    }

    size_t checked = 0;
    for (size_t entry_index = 0; entry_index < memmap_response->entry_count; ++entry_index) {
        Range range{};
        if (!memmap_page_range(memmap_response->entries[entry_index], range)) {
            continue;
        }

        uint64_t const PAGE_COUNT = (range.end - range.start) / paging::PAGE_SIZE;
        uint64_t const EDGE_PAGES = std::min<uint64_t>(PAGE_COUNT, SELFTEST_DIRECT_MAP_EDGE_PAGES);
        for (uint64_t page = 0; page < EDGE_PAGES; ++page) {
            selftest_require_direct_map_phys(range.start + (page * paging::PAGE_SIZE), phase);
            ++checked;
        }

        uint64_t const LAST_EDGE_PAGE = PAGE_COUNT > EDGE_PAGES ? PAGE_COUNT - EDGE_PAGES : EDGE_PAGES;
        for (uint64_t page = LAST_EDGE_PAGE; page < PAGE_COUNT; ++page) {
            selftest_require_direct_map_phys(range.start + (page * paging::PAGE_SIZE), phase);
            ++checked;
        }

        uint64_t boundary = range.start;
        uint64_t const MISALIGNMENT = boundary % LARGE_PAGE_2M_BYTES;
        if (MISALIGNMENT != 0) {
            uint64_t const ADVANCE = LARGE_PAGE_2M_BYTES - MISALIGNMENT;
            if (boundary > UINT64_MAX - ADVANCE) {
                continue;
            }
            boundary += ADVANCE;
        }

        while (boundary < range.end) {
            if (boundary > range.start) {
                selftest_require_direct_map_phys(boundary - paging::PAGE_SIZE, phase);
                ++checked;
            }
            selftest_require_direct_map_phys(boundary, phase);
            ++checked;
            if (boundary > UINT64_MAX - LARGE_PAGE_2M_BYTES) {
                break;
            }
            boundary += LARGE_PAGE_2M_BYTES;
        }
    }

    log::info("boot direct-map boundary audit passed: phase=%s samples=%zu", phase != nullptr ? phase : "?", checked);
}

}  // namespace

#endif

auto install_lazy_file_page_if_current(sched::task::Task* task, const sched::task::LazyVmemRange& range, vaddr_t page_vaddr,
                                       paddr_t page_paddr, uint64_t page_flags, file_mapping::FileMappingBinding* expected_binding)
    -> LazyFilePageInstallResult {
    if (task == nullptr || task->pagemap == nullptr) {
        return LazyFilePageInstallResult::STALE_RANGE;
    }

    LazyFilePageInstallResult result = LazyFilePageInstallResult::STALE_RANGE;
    uint64_t const PTE_IRQF = cow_pte_lock.lock_irqsave();
    uint64_t const RANGE_IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& candidate : task->lazy_vmem_ranges) {
        if (!lazy_file_range_matches(candidate, range, page_vaddr)) {
            continue;
        }
        // Demand canonical frames are published only by the prepared-spare
        // consumer. This legacy mapper may allocate/promote upper tables.
        if (sched::task::lazy_vmem_is_tmpfs_shared(candidate)) break;
        if (sched::task::lazy_vmem_has_lifetime(candidate)) {
            uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
            auto* control = find_user_root_control_locked(task->pagemap);
            bool const CURRENT = expected_binding != nullptr && control != nullptr &&
                                 file_binding_matches_range(*expected_binding, candidate) &&
                                 file_mapping_index.matches_pin_locked(task->pagemap, control->serial, page_vaddr, expected_binding);
            user_mapping_pin_lock.unlock_irqrestore(FLAGS);
            if (!CURRENT) break;
            if (candidate.lifetime->backing_mode() == file_mapping::FileMappingBackingMode::TMPFS_READ_SHARED ||
                candidate.lifetime->is_demand_shared()) {
                constexpr uint64_t REQUIRED =
                    paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED | paging::PAGE_NX;
                if (candidate.flags != ker::abi::vmem::MAP_SHARED || candidate.prot != ker::abi::vmem::PROT_READ || page_flags != REQUIRED)
                    break;
            }
        } else if (expected_binding != nullptr) {
            break;
        }
        if (translate(task->pagemap, page_vaddr) != PADDR_INVALID) {
            result = LazyFilePageInstallResult::ALREADY_MAPPED;
        } else {
            map_page(task->pagemap, page_vaddr, page_paddr, page_flags);
            result = LazyFilePageInstallResult::MAPPED;
        }
        break;
    }
    task->lazy_vmem_lock.unlock_irqrestore(RANGE_IRQF);
    cow_pte_lock.unlock_irqrestore(PTE_IRQF);

    if (result == LazyFilePageInstallResult::ALREADY_MAPPED) {
        invalidate_local_tlb_if_current(task->pagemap, page_vaddr, false);
    }
    return result;
}

bool ensure_user_page_writable(sched::task::Task* task, vaddr_t vaddr) { return ensure_user_page_writable_for_task(task, vaddr); }

bool ensure_user_page_mapped(sched::task::Task* task, vaddr_t vaddr) { return ensure_user_page_mapped_for_task(task, vaddr); }

#ifdef WOS_SELFTEST
bool selftest_anonymous_swap_pageout(PageTable* pagemap, vaddr_t vaddr) {
    vaddr = page_align_down(vaddr);
    AnonymousPageoutCandidate candidate{};
    if (pagemap == nullptr || !acquire_user_pagemap_access(pagemap, false, candidate.access_gate)) {
        return false;
    }
    candidate.pagemap = pagemap;
    candidate.vaddr = vaddr;

    uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
    uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry* entry = leaf_entry(pagemap, vaddr);
    if (entry != nullptr && entry->present != 0 && entry->user != 0 && (pte_raw(*entry) & paging::PAGE_SHARED) == 0U &&
        find_anonymous_swap_mapping_locked(pagemap, vaddr) == nullptr) {
        uint64_t const PHYS = static_cast<uint64_t>(entry->frame) << paging::PAGE_SHIFT;
        candidate.frame = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS));
        candidate.raw = pte_raw(*entry);
        phys::page_ref_inc(candidate.frame);
    }
    user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
    cow_pte_lock.unlock_irqrestore(COW_FLAGS);
    if (candidate.frame == nullptr) {
        release_user_pagemap_access(candidate.access_gate);
        return false;
    }
    return pageout_anonymous_candidate(candidate);
}

bool selftest_anonymous_swap_pagein(PageTable* pagemap, vaddr_t vaddr) {
    vaddr = page_align_down(vaddr);
    AnonymousSwapMapping* mapping = nullptr;
    size_t access_gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
    mapping = find_anonymous_swap_mapping_locked(pagemap, vaddr);
    if (mapping != nullptr && mapping->state == AnonymousSwapRuntimeState::SWAPPED && !mapping->pagein_queued &&
        acquire_user_pagemap_access(pagemap, false, access_gate)) {
        mapping->generation = next_anonymous_swap_generation();
        mapping->pagein_queued = true;
        mapping->pagein_access_gate = access_gate;
        anonymous_swap_add(anonymous_swap_stats.pagein_attempts);
    } else {
        mapping = nullptr;
    }
    cow_pte_lock.unlock_irqrestore(COW_FLAGS);
    if (mapping == nullptr) {
        release_user_pagemap_access(access_gate);
        return false;
    }
    service_anonymous_pagein(mapping);
    return translate(pagemap, vaddr) != PADDR_INVALID;
}

bool selftest_anonymous_swap_is_swapped(PageTable* pagemap, vaddr_t vaddr) {
    uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
    auto* mapping = find_anonymous_swap_mapping_locked(pagemap, page_align_down(vaddr));
    bool const SWAPPED = mapping != nullptr && mapping->state == AnonymousSwapRuntimeState::SWAPPED;
    cow_pte_lock.unlock_irqrestore(COW_FLAGS);
    return SWAPPED;
}
#endif

namespace {
template <bool StopAtFirst = false>
auto count_virtual_bytes_in_table(PageTable* table, unsigned level, uint64_t base, uint64_t start, uint64_t end) -> uint64_t {
    unsigned const SHIFT = paging::PAGE_SHIFT + 9 * (level - 1);
    uint64_t const STRIDE = uint64_t{1} << SHIFT;
    uint64_t bytes = 0;
    size_t const FIRST = static_cast<size_t>((start - base) >> SHIFT);
    size_t const LAST = static_cast<size_t>((end - 1 - base) >> SHIFT);
    if (level == 1) {
        // Public bounds and recursive table clips are page aligned. Count
        // occupied leaves before scaling once to their exact byte total.
        uint64_t occupied_pages = 0;
        for (size_t i = FIRST; i <= LAST; ++i) {
            uint64_t const RAW = pte_raw(entry_at(table, i));
            bool const OCCUPIED = (RAW & (paging::PAGE_PRESENT | paging::PAGE_RESERVED)) != 0;
            if constexpr (StopAtFirst) {
                if (OCCUPIED) {
                    return paging::PAGE_SIZE;
                }
            } else {
                occupied_pages += OCCUPIED;
            }
        }
        return occupied_pages * paging::PAGE_SIZE;
    }
    for (size_t i = FIRST; i <= LAST; ++i) {
        uint64_t const RAW = pte_raw(entry_at(table, i));
        uint64_t const ENTRY_START = base + i * STRIDE;
        uint64_t const LOW = std::max(start, ENTRY_START);
        uint64_t const HIGH = std::min(end, ENTRY_START + STRIDE);
        bool const PRESENT = (RAW & paging::PAGE_PRESENT) != 0;
        if (PRESENT && level < 4 && (RAW & (uint64_t{1} << 7)) != 0) {
            // Lower-half supervisor leaves can be resident PROT_NONE. They
            // still occupy user VA; kernel mappings live in the upper half.
            if (PRESENT || (RAW & paging::PAGE_RESERVED) != 0) {
                bytes += HIGH - LOW;
            }
        } else if (PRESENT) {
            auto* child = reinterpret_cast<PageTable*>(addr::get_virt_pointer(RAW & 0x000ffffffffff000ULL));
            bytes += count_virtual_bytes_in_table<StopAtFirst>(child, level - 1, ENTRY_START, LOW, HIGH);
        }
        if constexpr (StopAtFirst) {
            if (bytes != 0) {
                return bytes;
            }
        }
    }
    return bytes;
}
}  // namespace

auto validate_user_pagemap_access(PageTable* page_table) -> bool {
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(page_table, false, gate)) {
        return false;
    }
    release_user_pagemap_access(gate);
    return true;
}

auto count_user_virtual_bytes(PageTable* page_table, uint64_t start, uint64_t end, uint64_t& bytes) -> bool {
    bytes = 0;
    if (page_table == nullptr || start > end || end > 0x0000800000000000ULL || ((start | end) & (paging::PAGE_SIZE - 1)) != 0) {
        return false;
    }
    if (start == end) {
        return true;
    }
    // The bounded walk cannot block; keep its root pin out of non-unwinding
    // fatal syscall handoffs. Timer accounting remains active.
    sched::PreemptGuard guard;
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(page_table, false, gate)) {
        return false;
    }
    bytes = count_virtual_bytes_in_table(page_table, 4, 0, start, end);
    release_user_pagemap_access(gate);
    return true;
}

auto is_user_virtual_range_free(PageTable* page_table, uint64_t start, uint64_t end) -> bool {
    if (page_table == nullptr || start >= end || end > 0x0000800000000000ULL || ((start | end) & (paging::PAGE_SIZE - 1)) != 0) {
        return false;
    }
    // As with counting, callers serialize publication or own a private root.
    // The pin only keeps fork/teardown from reclaiming tables during the walk.
    sched::PreemptGuard guard;
    size_t gate = USER_PAGEMAP_ACCESS_GATE_NONE;
    if (!acquire_user_pagemap_access(page_table, false, gate)) {
        return false;
    }
    bool const FREE = count_virtual_bytes_in_table<true>(page_table, 4, 0, start, end) == 0;
    release_user_pagemap_access(gate);
    return FREE;
}

auto collect_user_memory_stats(PageTable* page_table) -> UserMemoryStats {
    UserMemoryStats stats{};
    if (page_table == nullptr) {
        return stats;
    }

    stats.page_table_pages = 1;  // Count the user address space root.
    constexpr size_t USER_PML4_ENTRIES = 256;
    constexpr uint64_t PAGES_PER_2M = 512;
    constexpr uint64_t PAGES_PER_1G = 512 * PAGES_PER_2M;
    phys::PageLookupHint ref_lookup{};

    auto count_present_leaf = [&](const PageTableEntry& entry, uint64_t page_count) {
        if (entry.user == 0) {
            return;
        }

        stats.virtual_pages += page_count;
        stats.resident_pages += page_count;

        uint64_t const RAW = pte_raw(entry);
        bool shared = (RAW & paging::PAGE_SHARED) != 0U;
        if (!shared && entry.frame != 0) {
            uint64_t const PHYS = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
            auto* virt_page = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS));
            shared = phys::page_ref_get(virt_page, &ref_lookup) > 1;
        }
        if (shared) {
            stats.shared_pages += page_count;
        }
    };

    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = entry_at(page_table, i4);
        if (!pml4e.present) {
            continue;
        }

        stats.page_table_pages++;
        auto* pml3 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(pml4e.frame) << paging::PAGE_SHIFT));
        for (const auto& pml3e : pml3->entries) {
            if (!pml3e.present) {
                continue;
            }
            if (pml3e.pagesize) {
                count_present_leaf(pml3e, PAGES_PER_1G);
                continue;
            }

            stats.page_table_pages++;
            auto* pml2 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(pml3e.frame) << paging::PAGE_SHIFT));
            for (const auto& pml2e : pml2->entries) {
                if (!pml2e.present) {
                    continue;
                }
                if (pml2e.pagesize) {
                    count_present_leaf(pml2e, PAGES_PER_2M);
                    continue;
                }

                stats.page_table_pages++;
                auto* pml1 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(pml2e.frame) << paging::PAGE_SHIFT));
                for (const auto& pte : pml1->entries) {
                    if (pte.present) {
                        count_present_leaf(pte, 1);
                        continue;
                    }
                    if (is_reserved_leaf(pte)) {
                        stats.virtual_pages++;
                    }
                }
            }
        }
    }

    // Reserved PTEs contribute to virtual_pages above; the side table
    // distinguishes demand-zero reservations from evicted anonymous pages.
    uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
    for (auto* mapping = anonymous_swap_mappings; mapping != nullptr; mapping = mapping->next) {
        if (mapping->pagemap != page_table) {
            continue;
        }
        if (mapping->state == AnonymousSwapRuntimeState::SWAPPED) {
            ++stats.swapped_pages;
        } else if (mapping->state == AnonymousSwapRuntimeState::PAGEOUT || mapping->state == AnonymousSwapRuntimeState::PAGEIN) {
            ++stats.swap_inflight_pages;
        }
    }
    cow_pte_lock.unlock_irqrestore(COW_FLAGS);

    return stats;
}

void get_anonymous_swap_stats_snapshot(AnonymousSwapStatsSnapshot& out) {
    out = {
        .resident_candidates = anonymous_swap_stats.resident_candidates.load(std::memory_order_relaxed),
        .swapped_pages = anonymous_swap_stats.swapped_pages.load(std::memory_order_relaxed),
        .pageout_inflight = anonymous_swap_stats.pageout_inflight.load(std::memory_order_relaxed),
        .pagein_inflight = anonymous_swap_stats.pagein_inflight.load(std::memory_order_relaxed),
        .pageout_attempts = anonymous_swap_stats.pageout_attempts.load(std::memory_order_relaxed),
        .pageout_successes = anonymous_swap_stats.pageout_successes.load(std::memory_order_relaxed),
        .pageout_failures = anonymous_swap_stats.pageout_failures.load(std::memory_order_relaxed),
        .pagein_attempts = anonymous_swap_stats.pagein_attempts.load(std::memory_order_relaxed),
        .pagein_successes = anonymous_swap_stats.pagein_successes.load(std::memory_order_relaxed),
        .pagein_failures = anonymous_swap_stats.pagein_failures.load(std::memory_order_relaxed),
        .pagein_corruption = anonymous_swap_stats.pagein_corruption.load(std::memory_order_relaxed),
        .pageout_latency_us = anonymous_swap_stats.pageout_latency_us.load(std::memory_order_relaxed),
        .pageout_latency_max_us = anonymous_swap_stats.pageout_latency_max_us.load(std::memory_order_relaxed),
        .pagein_latency_us = anonymous_swap_stats.pagein_latency_us.load(std::memory_order_relaxed),
        .pagein_latency_max_us = anonymous_swap_stats.pagein_latency_max_us.load(std::memory_order_relaxed),
    };
}

void register_anonymous_swap_shrinker() {
    bool expected = false;
    if (!anonymous_swap_shrinker_registered.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    swap::SwapConsumer const CONSUMER{
        .name = "anonymous_vmem",
        .context = nullptr,
        .migrate_area = migrate_anonymous_swap_area,
    };
    if (!swap::register_consumer(CONSUMER)) {
        anonymous_swap_shrinker_registered.store(false, std::memory_order_release);
        log::error("failed to register anonymous swapoff consumer");
        return;
    }
    reclaim::Shrinker const SHRINKER{
        .name = "anonymous_swap",
        .unit = reclaim::ReclaimUnit::PAGES,
        .scan_unit = reclaim::ReclaimUnit::PAGES,
        .rank = 6,
        .min_priority = reclaim::ReclaimPriority::LOW,
        .capabilities = reclaim::RECLAIM_MAY_BLOCK | reclaim::RECLAIM_MAY_IO | reclaim::RECLAIM_MAY_ALLOCATE,
        .max_batch_units = 32,
        .max_scan_units = 4096,
        .count = anonymous_swap_reclaim_count,
        .scan = anonymous_swap_reclaim_scan,
    };
    if (!reclaim::register_shrinker(SHRINKER)) {
        anonymous_swap_shrinker_registered.store(false, std::memory_order_release);
    }
}

void start_anonymous_swap_worker() {
#ifdef WOS_ASYNC_BLOCK_IO
    // SMT calls this after scheduler startup even when swap is disabled. Cold
    // file mappings need their own task-context fault service in every ON boot.
    start_lazy_file_fault_worker();
#endif
    if (!anonymous_swap_enabled()) {
        return;
    }
    bool expected = false;
    if (!anonymous_swap_worker_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    auto* worker = sched::task::Task::create_kernel_thread("anon_swapin", anonymous_pagein_worker_main);
    if (worker == nullptr) {
        anonymous_swap_worker_started.store(false, std::memory_order_release);
        return;
    }
    anonymous_swap_worker_task.store(worker, std::memory_order_release);
    (void)sched::post_task_for_cpu(0, worker);
}

void init_pagemap() {
    // PAGE_NX is reserved until EFER.NXE is active. Configure the BSP before
    // constructing or loading any page table that contains NX leaves.
    kernel_nx_supported = cpu::enable_nx();
    cpu::enable_write_protect();
    cpu::SupervisorProtectionState const SUPERVISOR = cpu::enable_supervisor_protections();
    if ((SUPERVISOR.smep_supported && !SUPERVISOR.smep_enabled) || (SUPERVISOR.smap_supported && !SUPERVISOR.smap_enabled)) {
        log::critical("CPU supervisor protection policy failed: SMEP supported=%u enabled=%u SMAP supported=%u enabled=%u",
                      SUPERVISOR.smep_supported ? 1U : 0U, SUPERVISOR.smep_enabled ? 1U : 0U, SUPERVISOR.smap_supported ? 1U : 0U,
                      SUPERVISOR.smap_enabled ? 1U : 0U);
        hcf();
    }
    cpu::publish_supervisor_protection_policy(SUPERVISOR);
    log::info("CPU supervisor protection policy: SMEP=%s SMAP=%s usercopy=%s", SUPERVISOR.smep_enabled ? "enabled" : "unsupported",
              SUPERVISOR.smap_enabled ? "enabled" : "unsupported",
              SUPERVISOR.smap_enabled ? "AC-gated pinned-page window" : "software pinned-page fallback");
    cpu::enable_pae();
    cpu::enable_pse();
    kernel_pagemap = alloc_zeroed_page_table();
    if (kernel_pagemap == nullptr) {
        // PANIC!
        log::critical("init: failed to allocate kernel pagemap in init_pagemap");
        hcf();
    }
    log::info("kernel pagemap allocated at %x", kernel_pagemap);
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        const char* type_str = [&]() -> const char* {
            switch (entry->type) {
                case LIMINE_MEMMAP_USABLE:
                    return "USABLE";
                case LIMINE_MEMMAP_RESERVED:
                    return "RESERVED";
                case LIMINE_MEMMAP_ACPI_RECLAIMABLE:
                    return "ACPI_RECLAIMABLE";
                case LIMINE_MEMMAP_ACPI_NVS:
                    return "ACPI_NVS";
                case LIMINE_MEMMAP_BAD_MEMORY:
                    return "BAD_MEMORY";
                case LIMINE_MEMMAP_BOOTLOADER_RECLAIMABLE:
                    return "BOOTLOADER_RECLAIMABLE";
                case LIMINE_MEMMAP_EXECUTABLE_AND_MODULES:
                    return "KERNEL_AND_MODULES";
                case LIMINE_MEMMAP_FRAMEBUFFER:
                    return "FRAMEBUFFER";
                default:
                    return "UNKNOWN";
            }
        }();
        log::debug("memory map entry %zu: %lx - %lx (%s)", i, entry->base, entry->base + entry->length, type_str);
    }

    BootDirectMapStats direct_map_stats{};
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        Range entry_range{};
        if (!memmap_page_range(entry, entry_range)) {
            continue;
        }

        BootDirectMapStats const ENTRY_STATS = map_boot_direct_range(kernel_pagemap, entry_range, direct_map_flags_for_entry(entry->type),
                                                                     boot_direct_map_allows_2m(entry->type));
        direct_map_stats.pages += ENTRY_STATS.pages;
        direct_map_stats.small_4k += ENTRY_STATS.small_4k;
        direct_map_stats.large_2m += ENTRY_STATS.large_2m;
    }
    log::info("mapped total of %zu pages from memory map (%zu x 2M, %zu x 4K)", direct_map_stats.pages, direct_map_stats.large_2m,
              direct_map_stats.small_4k);

    if (kernel_file_response == nullptr || kernel_file_response->executable_file == nullptr || kernel_address_response == nullptr) {
        log::critical("init_pagemap: missing kernel executable metadata");
        hcf();
    }

    uint64_t const KERNEL_VIRT_BASE = kernel_address_response->virtual_base;
    auto const KERNEL_LINK_BASE = reinterpret_cast<uint64_t>(__kernel_image_start);
    auto const KERNEL_IMAGE_END = reinterpret_cast<uint64_t>(__kernel_end);
    if (KERNEL_IMAGE_END < KERNEL_VIRT_BASE || KERNEL_LINK_BASE != KERNEL_VIRT_BASE) {
        log::critical("init_pagemap: invalid kernel image bounds load=%p link=%p end=%p", KERNEL_VIRT_BASE, KERNEL_LINK_BASE,
                      KERNEL_IMAGE_END);
        hcf();
    }
#ifdef WOS_SELFTEST
    // The diagnostic kernel publishes its bootloader-selected base only on the
    // private serial test channel so repeated-boot KASLR sampling is exact.
    log::info("selftest kernel relocation: link_base=0x0 load_base=%p slide=%p", KERNEL_VIRT_BASE, KERNEL_VIRT_BASE);
#endif

    uint64_t const KERNEL_IMAGE_SIZE = KERNEL_IMAGE_END - KERNEL_VIRT_BASE;
    auto map_kernel_range = [&](vaddr_t start, vaddr_t end, uint64_t flags) {
        if (start >= end || start < KERNEL_VIRT_BASE || end > KERNEL_IMAGE_END || ((start | end) & (paging::PAGE_SIZE - 1)) != 0U) {
            log::critical("init_pagemap: invalid section range start=%p end=%p flags=%lx", start, end, flags);
            hcf();
        }
        PageMapBatch batch{};
        init_page_map_batch(&batch, kernel_pagemap, flags);
        for (vaddr_t vaddr = start; vaddr < end; vaddr += paging::PAGE_SIZE) {
            paddr_t const paddr = kernel_address_response->physical_base + (vaddr - KERNEL_VIRT_BASE);
            map_page_batched(&batch, vaddr, paddr, flags);
        }
        flush_page_map_batch(&batch);
    };

    uint64_t const NON_EXEC = kernel_nx_supported ? paging::PAGE_NX : 0;
    vaddr_t const TEXT_START = page_align_down(reinterpret_cast<vaddr_t>(__kernel_text_start));
    vaddr_t const TEXT_END = page_align_up(reinterpret_cast<vaddr_t>(__kernel_text_end));
    vaddr_t const RODATA_START = reinterpret_cast<vaddr_t>(__kernel_rodata_start);
    vaddr_t const RODATA_END = page_align_up(reinterpret_cast<vaddr_t>(__kernel_rodata_end));
    vaddr_t const DATA_START = reinterpret_cast<vaddr_t>(__kernel_data_start);
    vaddr_t const DATA_END = reinterpret_cast<vaddr_t>(__kernel_data_end);
    vaddr_t const EMERGENCY_REGION_START = reinterpret_cast<vaddr_t>(__kernel_emergency_stack_region_start);
    vaddr_t const EMERGENCY_GUARD_LOW = reinterpret_cast<vaddr_t>(__stack_chk_emergency_guard_low);
    vaddr_t const EMERGENCY_STACK_START = reinterpret_cast<vaddr_t>(__stack_chk_emergency_stack);
    vaddr_t const EMERGENCY_STACK_END = reinterpret_cast<vaddr_t>(__stack_chk_emergency_stack_end);
    vaddr_t const EMERGENCY_GUARD_HIGH_END = reinterpret_cast<vaddr_t>(__stack_chk_emergency_guard_high_end);
    vaddr_t const EMERGENCY_REGION_END = reinterpret_cast<vaddr_t>(__kernel_emergency_stack_region_end);
    if (KERNEL_IMAGE_SIZE == 0 || TEXT_START != KERNEL_VIRT_BASE || TEXT_END != RODATA_START || RODATA_END != DATA_START ||
        DATA_END != KERNEL_IMAGE_END || EMERGENCY_REGION_START != EMERGENCY_GUARD_LOW ||
        EMERGENCY_STACK_START - EMERGENCY_GUARD_LOW != paging::PAGE_SIZE ||
        EMERGENCY_STACK_END - EMERGENCY_STACK_START != 4 * paging::PAGE_SIZE ||
        EMERGENCY_GUARD_HIGH_END - EMERGENCY_STACK_END != paging::PAGE_SIZE || EMERGENCY_REGION_END != EMERGENCY_GUARD_HIGH_END ||
        EMERGENCY_REGION_START < DATA_START || EMERGENCY_REGION_END > DATA_END) {
        log::critical("init_pagemap: non-contiguous kernel sections image=[%p,%p) text=[%p,%p) rodata=[%p,%p) data=[%p,%p)",
                      KERNEL_VIRT_BASE, KERNEL_IMAGE_END, TEXT_START, TEXT_END, RODATA_START, RODATA_END, DATA_START, DATA_END);
        hcf();
    }
    map_kernel_range(TEXT_START, TEXT_END, paging::page_types::READONLY);
    map_kernel_range(RODATA_START, RODATA_END, paging::page_types::READONLY | NON_EXEC);
    map_kernel_range(DATA_START, EMERGENCY_GUARD_LOW, kernel_data_page_flags());
    map_kernel_range(EMERGENCY_STACK_START, EMERGENCY_STACK_END, kernel_data_page_flags());
    map_kernel_range(EMERGENCY_GUARD_HIGH_END, DATA_END, kernel_data_page_flags());

    // Raw QEMU RSP has no WOS image-catalog packet. Publish one immutable,
    // NX descriptor at a stable diagnostic VA so WOSDBG can discover the
    // randomized image base and authenticate the exact local kernel first.
    auto* const DEBUG_METADATA_PAGE = phys::page_alloc(PhysicalPageOwner::DEBUGGER, paging::PAGE_SIZE, "kernel_debug_metadata");
    if (DEBUG_METADATA_PAGE == nullptr) {
        log::critical("init_pagemap: failed to allocate kernel debug metadata");
        hcf();
    }
    std::memset(DEBUG_METADATA_PAGE, 0, paging::PAGE_SIZE);
    auto* const DEBUG_METADATA = static_cast<wos::abi::KernelDebugMetadata*>(DEBUG_METADATA_PAGE);
    if (!loader::runtime::kernel_debug_metadata(*DEBUG_METADATA)) {
        log::critical("init_pagemap: failed to construct kernel debug metadata");
        hcf();
    }
    auto const DEBUG_METADATA_PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(DEBUG_METADATA_PAGE)));
    map_page(kernel_pagemap, wos::abi::KERNEL_DEBUG_METADATA_ADDRESS, DEBUG_METADATA_PHYS, paging::page_types::READONLY | NON_EXEC);

    // CRITICAL: Ensure all page table pages are mapped in the HHDM.
    // Page tables allocated by advancePageTable() might be from physical addresses
    // above 4GB that haven't been mapped yet in our kernel pagemap.
    // We must iterate until no new mappings are needed, because mapping a page
    // table page might allocate NEW page table pages that also need mapping.
    size_t pt_pages_mappped = 0;
    size_t iteration = 0;
    bool made_progress = true;

    while (made_progress) {
        made_progress = false;
        iteration++;
        size_t mapped_this_round = 0;
        for (size_t pml4i = KERNEL_PML4_START; pml4i < KERNEL_PML4_END; pml4i++) {  // Only kernel half
            PageTableEntry const& pml4e = entry_at(kernel_pagemap, pml4i);
            if (!pml4e.present) {
                continue;
            }

            paddr_t const PML3_PHYS = pml4e.frame << paging::PAGE_SHIFT;
            auto pml3_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML3_PHYS));
            if (!is_page_mapped(kernel_pagemap, pml3_virt)) {
                map_page(kernel_pagemap, pml3_virt, PML3_PHYS, kernel_data_page_flags());
                pt_pages_mappped++;
                mapped_this_round++;
                made_progress = true;
            }

            auto* pml3 = reinterpret_cast<PageTable*>(pml3_virt);
            for (auto& pml3e : pml3->entries) {
                if (!pml3e.present) {
                    continue;
                }
                if (pml3e.pagesize != 0) {
                    continue;
                }

                paddr_t const PML2_PHYS = pml3e.frame << paging::PAGE_SHIFT;
                auto pml2_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML2_PHYS));
                if (!is_page_mapped(kernel_pagemap, pml2_virt)) {
                    map_page(kernel_pagemap, pml2_virt, PML2_PHYS, kernel_data_page_flags());
                    pt_pages_mappped++;
                    mapped_this_round++;
                    made_progress = true;
                }

                auto* pml2 = reinterpret_cast<PageTable*>(pml2_virt);
                for (auto& pml2e : pml2->entries) {
                    if (!pml2e.present) {
                        continue;
                    }
                    if (pml2e.pagesize != 0) {
                        continue;
                    }

                    paddr_t const PML1_PHYS = pml2e.frame << paging::PAGE_SHIFT;

                    auto pml1_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML1_PHYS));
                    if (!is_page_mapped(kernel_pagemap, pml1_virt)) {
                        map_page(kernel_pagemap, pml1_virt, PML1_PHYS, kernel_data_page_flags());
                        pt_pages_mappped++;
                        mapped_this_round++;
                        made_progress = true;
                    }
                }
            }
        }

        if (mapped_this_round > 0) {
            log::debug("page table fixup iteration %d: mapped %d pages", iteration, mapped_this_round);
        }
    }
    log::info("total page table pages mapped into HHDM: %d (in %d iterations)", pt_pages_mappped, iteration);

    // CRITICAL: Clear PML4[0] to ensure null pointer dereferences cause page faults!
    // Limine may leave identity mappings in the lower half which mask null pointer bugs.
    entry_at(kernel_pagemap, 0) = PageTableEntry{};
    log::info("cleared PML4[0] - null derefs will now fault");

    switch_to_kernel_pagemap();
    KernelProtectionAudit const AUDIT = audit_kernel_protections();
    bool const BASELINE_VALID = AUDIT.write_protect_active && AUDIT.layout_valid && AUDIT.text_readonly && AUDIT.text_executable &&
                                AUDIT.rodata_readonly && AUDIT.data_writable && AUDIT.emergency_stack_guarded &&
                                AUDIT.direct_alias_readonly && AUDIT.debug_metadata_readonly;
    if (!BASELINE_VALID || (kernel_nx_supported && !AUDIT.enforced())) {
        log::critical(
            "kernel protection audit failed: nx_supported=%u nx_active=%u wp=%u layout=%u text_ro=%u text_x=%u rodata_ro=%u rodata_nx=%u "
            "data_rw=%u data_nx=%u emergency_stack_guarded=%u alias_ro=%u alias_nx=%u debug_ro=%u debug_nx=%u",
            AUDIT.nx_supported ? 1U : 0U, AUDIT.nx_active ? 1U : 0U, AUDIT.write_protect_active ? 1U : 0U, AUDIT.layout_valid ? 1U : 0U,
            AUDIT.text_readonly ? 1U : 0U, AUDIT.text_executable ? 1U : 0U, AUDIT.rodata_readonly ? 1U : 0U,
            AUDIT.rodata_non_executable ? 1U : 0U, AUDIT.data_writable ? 1U : 0U, AUDIT.data_non_executable ? 1U : 0U,
            AUDIT.emergency_stack_guarded ? 1U : 0U, AUDIT.direct_alias_readonly ? 1U : 0U, AUDIT.direct_alias_non_executable ? 1U : 0U,
            AUDIT.debug_metadata_readonly ? 1U : 0U, AUDIT.debug_metadata_non_executable ? 1U : 0U);
        hcf();
    }
    if (!kernel_nx_supported) {
        log::warn("CPU lacks NX support; kernel write protection is active but non-executable data cannot be enforced");
    } else {
        log::info(
            "kernel W^X active: text=RO+X rodata=RO+NX data=RW+NX emergency-stack=guarded direct-map-alias=RO+NX "
            "debug-metadata=RO+NX");
    }
#ifdef WOS_SELFTEST
    selftest_register_kernel_page_tables();
    direct_map_selftest_ready = true;
    selftest_audit_boot_direct_map_boundaries("post-cr3-switch");
#endif
}

auto audit_kernel_protections() -> KernelProtectionAudit {
    KernelProtectionAudit audit{};
    audit.nx_supported = kernel_nx_supported;
    audit.nx_active = cpu::nx_enabled();
    audit.write_protect_active = cpu::write_protect_enabled();
    if (kernel_pagemap == nullptr || kernel_address_response == nullptr) {
        return audit;
    }

    vaddr_t const IMAGE_START = reinterpret_cast<vaddr_t>(__kernel_image_start);
    vaddr_t const TEXT_START = page_align_down(reinterpret_cast<vaddr_t>(__kernel_text_start));
    vaddr_t const TEXT_END = page_align_up(reinterpret_cast<vaddr_t>(__kernel_text_end));
    vaddr_t const RODATA_START = reinterpret_cast<vaddr_t>(__kernel_rodata_start);
    vaddr_t const RODATA_END = page_align_up(reinterpret_cast<vaddr_t>(__kernel_rodata_end));
    vaddr_t const DATA_START = reinterpret_cast<vaddr_t>(__kernel_data_start);
    vaddr_t const DATA_END = reinterpret_cast<vaddr_t>(__kernel_data_end);
    vaddr_t const BSS_START = reinterpret_cast<vaddr_t>(__kernel_bss_start);
    vaddr_t const BSS_END = reinterpret_cast<vaddr_t>(__kernel_bss_end);
    vaddr_t const EMERGENCY_GUARD_LOW = reinterpret_cast<vaddr_t>(__stack_chk_emergency_guard_low);
    vaddr_t const EMERGENCY_STACK_START = reinterpret_cast<vaddr_t>(__stack_chk_emergency_stack);
    vaddr_t const EMERGENCY_STACK_END = reinterpret_cast<vaddr_t>(__stack_chk_emergency_stack_end);
    vaddr_t const EMERGENCY_GUARD_HIGH_END = reinterpret_cast<vaddr_t>(__stack_chk_emergency_guard_high_end);

    audit.layout_valid =
        IMAGE_START == kernel_address_response->virtual_base && IMAGE_START == TEXT_START && TEXT_START < TEXT_END &&
        TEXT_END == RODATA_START && RODATA_START < RODATA_END && RODATA_END == DATA_START && DATA_START < DATA_END &&
        BSS_START >= DATA_START && BSS_START <= EMERGENCY_GUARD_LOW && EMERGENCY_STACK_START - EMERGENCY_GUARD_LOW == paging::PAGE_SIZE &&
        EMERGENCY_STACK_END - EMERGENCY_STACK_START == 4 * paging::PAGE_SIZE &&
        EMERGENCY_GUARD_HIGH_END - EMERGENCY_STACK_END == paging::PAGE_SIZE && EMERGENCY_GUARD_HIGH_END <= BSS_END && BSS_END <= DATA_END;
    if (!audit.layout_valid) {
        return audit;
    }

    audit.text_readonly = audit_mapping_range(TEXT_START, TEXT_END, false, true);
    audit.text_executable = audit.text_readonly;
    audit.rodata_readonly = audit_mapping_range(RODATA_START, RODATA_END, false, !kernel_nx_supported);
    audit.rodata_non_executable = kernel_nx_supported && audit.rodata_readonly;
    audit.data_writable = audit_mapping_range(DATA_START, EMERGENCY_GUARD_LOW, true, !kernel_nx_supported) &&
                          audit_mapping_range(EMERGENCY_STACK_START, EMERGENCY_STACK_END, true, !kernel_nx_supported) &&
                          audit_mapping_range(EMERGENCY_GUARD_HIGH_END, DATA_END, true, !kernel_nx_supported);
    audit.data_non_executable = kernel_nx_supported && audit.data_writable;
    audit.emergency_stack_guarded = !mapping_permissions(kernel_pagemap, EMERGENCY_GUARD_LOW).present &&
                                    !mapping_permissions(kernel_pagemap, EMERGENCY_STACK_END).present;

    uint64_t const IMAGE_BYTES = DATA_END - IMAGE_START;
    vaddr_t const ALIAS_START = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(kernel_address_response->physical_base));
    vaddr_t const ALIAS_END = ALIAS_START + IMAGE_BYTES;
    audit.direct_alias_readonly = ALIAS_END >= ALIAS_START && audit_mapping_range(ALIAS_START, ALIAS_END, false, !kernel_nx_supported);
    audit.direct_alias_non_executable = kernel_nx_supported && audit.direct_alias_readonly;
    audit.debug_metadata_readonly = audit_mapping_range(
        wos::abi::KERNEL_DEBUG_METADATA_ADDRESS, wos::abi::KERNEL_DEBUG_METADATA_ADDRESS + paging::PAGE_SIZE, false, !kernel_nx_supported);
    audit.debug_metadata_non_executable = kernel_nx_supported && audit.debug_metadata_readonly;
    return audit;
}

auto kernel_data_page_flags() -> uint64_t { return paging::page_types::KERNEL | (kernel_nx_supported ? paging::PAGE_NX : 0); }

auto kernel_mmio_page_flags() -> uint64_t { return paging::page_types::MMIO | (kernel_nx_supported ? paging::PAGE_NX : 0); }

namespace {

// advance page table by level pages
auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted) -> paging::PageTable* {
    PageTableEntry entry = entry_at(page_table, level);
    if (entry.present) {
        if (entry.pagesize != 0) {
            dbg::panic_handler("4K page-table mutator encountered an unsplit large leaf");
            __builtin_trap();
        }
        bool changed = false;
        if (((flags & paging::PAGE_WRITE) != 0U) && !entry.writable) {
            entry.writable = 1;
            changed = true;
        }
        if (((flags & paging::PAGE_USER) != 0U) && !entry.user) {
            entry.user = 1;
            changed = true;
        }

        // Only clear NX bit if we are mapping executable code.
        // Never set NX bit on intermediate entries as it affects all children.
        if (((flags & paging::PAGE_NX) == 0U) && entry.no_execute) {
            entry.no_execute = 0;
            auto* raw_entry = reinterpret_cast<uint64_t*>(&entry);
            constexpr uint64_t NX_MASK = ~(1ULL << 63);
            *raw_entry &= NX_MASK;  // Clear NX bit
            changed = true;
        }

        if (changed) {
            entry_at(page_table, level) = entry;
            if (promoted != nullptr) {
                *promoted = true;
            }
            // Force full TLB flush
            wrcr3(rdcr3());
        }

        return reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    auto* page_table_virt = alloc_zeroed_page_table();
    if (page_table_virt == nullptr) {
        // PANIC!
        log::critical("init: failed to allocate kernel page table in advance_page_table");
        hcf();
    }

    auto page_phys = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(page_table_virt)));

    entry_at(page_table, level) = paging::create_page_table_entry(page_phys, flags);
    return page_table_virt;
}

}  // namespace

/*
 * Map a page in the page table
 *
 * @param pageTable - the page table to map the page in
 * @param vaddr - the virtual address to map the page to
 * @param paddr - the physical address to map the page to
 * @param flags - the flags to set for the page
 */
void map_page(PageTable* page_table, const vaddr_t VADDR, const paddr_t PADDR, const uint64_t FLAGS) {
    if ((page_table == nullptr) || FLAGS == 0) {
        // PANIC!
        log::critical("init: failed to map page in map_page args=<vaddr: %p, paddr: %p, flags: %x>", VADDR, PADDR, FLAGS);
        hcf();
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "map-page", VADDR);
#endif

    if (kernel_hhdm_2m_mapping_matches(page_table, VADDR, PADDR, FLAGS)) return;

    // PageTable* table = pageTable;
    // for (int i = 4; i > 1; i--) {
    //     table = advancePageTable(table, index_of(vaddr, i), flags);
    // }
    paging::PageTable* pml3 = nullptr;
    paging::PageTable* pml2 = nullptr;
    paging::PageTable* pml1 = nullptr;
    bool path_promoted = false;
    pml3 = advance_page_table(page_table, index_of(VADDR, 4), FLAGS, &path_promoted);
    pml2 = advance_page_table(pml3, index_of(VADDR, 3), FLAGS, &path_promoted);
    pml1 = advance_page_table(pml2, index_of(VADDR, 2), FLAGS, &path_promoted);

    PageTableEntry old_entry{};
    bool replaced_translation = false;
    bool replaced_present_frame = false;
    uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry& entry = entry_at(pml1, index_of(VADDR, 1));
    old_entry = entry;
    replaced_translation = old_entry.present != 0 || is_reserved_leaf(old_entry);
    replaced_present_frame = old_entry.present != 0 && (static_cast<paddr_t>(old_entry.frame) << paging::PAGE_SHIFT) != PADDR;
    owned_frame_untrack_leaf(page_table, VADDR, old_entry);
    entry = paging::create_page_table_entry(PADDR, FLAGS);
    owned_frame_track_private_mapping(page_table, VADDR, PADDR, FLAGS);
    user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);

    invalidate_local_tlb_if_current(page_table, VADDR, path_promoted);
    if (path_promoted || replaced_translation) {
        shootdown_remote_user_pagemap(page_table, VADDR, path_promoted);
    }
    // A present PTE owns one reference to its frame. Publish and invalidate the
    // replacement before dropping that reference so no CPU can retain a stale
    // translation to a frame that has already returned to the allocator.
    if (replaced_present_frame) {
        drop_present_leaf_ref(old_entry);
    }
}

void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, const uint64_t FLAGS) {
    if (batch == nullptr || page_table == nullptr || FLAGS == 0) {
        log::critical("init_page_map_batch: invalid args=<batch: %p, pagemap: %p, flags: %x>", batch, page_table, FLAGS);
        hcf();
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "init-page-map-batch");
#endif

    batch->root = page_table;
    batch->pml3 = nullptr;
    batch->pml2 = nullptr;
    batch->pml1 = nullptr;
    batch->cached_idx4 = UINT64_MAX;
    batch->cached_idx3 = UINT64_MAX;
    batch->cached_idx2 = UINT64_MAX;
    batch->table_flags = table_flags_for_leaf(FLAGS);
    batch->dirty = false;
}

void map_page_batched(PageMapBatch* batch, const vaddr_t VADDR, const paddr_t PADDR, const uint64_t FLAGS) {
    if (batch == nullptr || batch->root == nullptr || FLAGS == 0) {
        log::critical("map_page_batched: invalid args=<batch: %p, vaddr: %p, paddr: %p, flags: %x>", batch, VADDR, PADDR, FLAGS);
        hcf();
    }

    if (kernel_hhdm_2m_mapping_matches(batch->root, VADDR, PADDR, FLAGS)) return;

    uint64_t const IDX4 = index_of(VADDR, 4);
    uint64_t const IDX3 = index_of(VADDR, 3);
    uint64_t const IDX2 = index_of(VADDR, 2);
    bool path_promoted = false;

    if (IDX4 != batch->cached_idx4 || batch->pml3 == nullptr) {
        batch->pml3 = advance_page_table(batch->root, IDX4, batch->table_flags, &path_promoted);
        batch->cached_idx4 = IDX4;
        batch->cached_idx3 = UINT64_MAX;
        batch->cached_idx2 = UINT64_MAX;
    }
    if (IDX3 != batch->cached_idx3 || batch->pml2 == nullptr) {
        batch->pml2 = advance_page_table(batch->pml3, IDX3, batch->table_flags, &path_promoted);
        batch->cached_idx3 = IDX3;
        batch->cached_idx2 = UINT64_MAX;
    }
    if (IDX2 != batch->cached_idx2 || batch->pml1 == nullptr) {
        batch->pml1 = advance_page_table(batch->pml2, IDX2, batch->table_flags, &path_promoted);
        batch->cached_idx2 = IDX2;
    }

    PageTableEntry old_entry{};
    bool replaced_present_frame = false;
    uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry& entry = entry_at(batch->pml1, index_of(VADDR, 1));
    old_entry = entry;
    replaced_present_frame = old_entry.present != 0 && (static_cast<paddr_t>(old_entry.frame) << paging::PAGE_SHIFT) != PADDR;
    owned_frame_untrack_leaf(batch->root, VADDR, entry);
    entry = paging::create_page_table_entry(PADDR, FLAGS);
    owned_frame_track_private_mapping(batch->root, VADDR, PADDR, FLAGS);
    user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
    batch->dirty = true;
    if (replaced_present_frame) {
        // Batched callers normally install absent leaves. A replacement is
        // still legal, but its old mapping reference cannot be released until
        // the batched TLB invalidation has completed.
        flush_page_map_batch(batch);
        drop_present_leaf_ref(old_entry);
    }
}

void flush_page_map_batch(PageMapBatch* batch) {
    if (batch != nullptr && batch->dirty) {
        flush_pagemap_after_update(batch->root, 0, true);
        batch->dirty = false;
    }
}

void map_same_page_range(PageTable* page_table, const vaddr_t VADDR, const paddr_t PADDR, const uint64_t PAGE_COUNT, const uint64_t FLAGS) {
    if ((page_table == nullptr) || FLAGS == 0) {
        log::critical("map_same_page_range: invalid args=<vaddr: %p, paddr: %p, pages: %llu, flags: %x>", VADDR, PADDR,
                      static_cast<unsigned long long>(PAGE_COUNT), FLAGS);
        hcf();
    }
    if (PAGE_COUNT == 0) {
        return;
    }

    uint64_t const LAST_PAGE = PAGE_COUNT - 1;
    if (LAST_PAGE > UINT64_MAX / paging::PAGE_SIZE || VADDR > UINT64_MAX - (LAST_PAGE * paging::PAGE_SIZE)) {
        log::critical("map_same_page_range: address overflow args=<vaddr: %p, pages: %llu>", VADDR,
                      static_cast<unsigned long long>(PAGE_COUNT));
        hcf();
    }

    PageMapBatch batch{};
    init_page_map_batch(&batch, page_table, FLAGS);
    for (uint64_t i = 0; i < PAGE_COUNT; i++) {
        vaddr_t const CURRENT_VADDR = VADDR + (i * paging::PAGE_SIZE);
        map_page_batched(&batch, CURRENT_VADDR, PADDR, FLAGS);
    }
    flush_page_map_batch(&batch);
}

void reserve_page_range(PageTable* page_table, const vaddr_t VADDR, const uint64_t PAGE_COUNT) {
    if (page_table == nullptr) {
        log::critical("reserve_page_range: invalid args=<pagemap: %p, vaddr: %p, pages: %llu>", page_table, VADDR,
                      static_cast<unsigned long long>(PAGE_COUNT));
        hcf();
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "reserve-page-range", VADDR);
#endif

    if (PAGE_COUNT == 0) {
        return;
    }

    uint64_t const LAST_PAGE = PAGE_COUNT - 1;
    if (LAST_PAGE > UINT64_MAX / paging::PAGE_SIZE || VADDR > UINT64_MAX - (LAST_PAGE * paging::PAGE_SIZE)) {
        log::critical("reserve_page_range: address overflow args=<vaddr: %p, pages: %llu>", VADDR,
                      static_cast<unsigned long long>(PAGE_COUNT));
        hcf();
    }

    uint64_t constexpr TABLE_FLAGS = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER;
    std::array<PageTableEntry, paging::PAGE_TABLE_ENTRIES> retired_present{};
    uint64_t page_index = 0;
    while (page_index < PAGE_COUNT) {
        vaddr_t const BATCH_VADDR = VADDR + (page_index * paging::PAGE_SIZE);
        bool path_promoted = false;
        PageTable* pml3 = advance_page_table(page_table, index_of(BATCH_VADDR, 4), TABLE_FLAGS, &path_promoted);
        PageTable* pml2 = advance_page_table(pml3, index_of(BATCH_VADDR, 3), TABLE_FLAGS, &path_promoted);
        PageTable* pml1 = advance_page_table(pml2, index_of(BATCH_VADDR, 2), TABLE_FLAGS, &path_promoted);
        size_t const FIRST_LEAF = index_of(BATCH_VADDR, 1);
        uint64_t const BATCH_PAGES = std::min<uint64_t>(PAGE_COUNT - page_index, paging::PAGE_TABLE_ENTRIES - FIRST_LEAF);
        size_t retired_count = 0;
        bool replaced_translation = false;
        uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
        for (uint64_t batch_page = 0; batch_page < BATCH_PAGES; ++batch_page) {
            vaddr_t const CURRENT_VADDR = BATCH_VADDR + (batch_page * paging::PAGE_SIZE);
            PageTableEntry& entry = entry_at(pml1, FIRST_LEAF + batch_page);
            uint64_t const OLD_RAW = pte_raw(entry);
            if (OLD_RAW == paging::PAGE_RESERVED) {
                continue;
            }

            PageTableEntry const OLD_ENTRY = entry;
            if (OLD_RAW != 0) {
                replaced_translation = true;
                owned_frame_untrack_leaf(page_table, CURRENT_VADDR, OLD_ENTRY);
            }
            if (OLD_ENTRY.present != 0 && OLD_ENTRY.frame != 0) {
                retired_present.at(retired_count++) = OLD_ENTRY;
            }
            entry = pte_from_raw(paging::PAGE_RESERVED);
        }
        user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);

        // Installing a non-present reservation into an empty leaf cannot
        // leave a stale translation. Fresh exec stacks contain thousands of
        // these leaves, so cache each leaf-table walk and hold the pin lock for
        // one bounded batch. Existing metadata/translations and promoted table
        // permissions receive one full flush before any old frame is released.
        if (path_promoted || replaced_translation) {
            flush_pagemap_after_update(page_table, BATCH_VADDR, true);
        }
        for (size_t i = 0; i < retired_count; ++i) {
            drop_present_leaf_ref(retired_present.at(i));
        }
        page_index += BATCH_PAGES;
    }
}

auto is_page_mapped(PageTable* page_table, vaddr_t vaddr) -> bool {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in is_page_mapped");
        hcf();
    }

    return present_mapping_entry(page_table, vaddr) != nullptr;
}

auto is_page_reserved(PageTable* page_table, vaddr_t vaddr) -> bool {
    if (page_table == nullptr) {
        log::critical("init: failed to get page table in is_page_reserved");
        hcf();
    }

    const PageTableEntry* entry = leaf_entry(page_table, vaddr);
    return entry != nullptr && is_reserved_leaf(*entry);
}

auto is_page_mapped_or_reserved(PageTable* page_table, vaddr_t vaddr) -> bool {
    if (page_table == nullptr) {
        log::critical("init: failed to get page table in is_page_mapped_or_reserved");
        hcf();
    }

    const PageTableEntry* entry = leaf_entry(page_table, vaddr);
    return entry != nullptr && (entry->present != 0 || is_reserved_leaf(*entry));
}

auto is_page_write_restricted(PageTable* page_table, vaddr_t vaddr) -> bool {
    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto const* entry = present_mapping_entry(page_table, vaddr);
    bool const RESTRICTED = entry != nullptr && paging::shared_write_restricted(pte_raw(*entry));
    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
    return RESTRICTED;
}

void unify_page_flags(PageTable* page_table, vaddr_t vaddr, uint64_t flags) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unify_page_flags");
        hcf();
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "unify-page-flags", vaddr);
#endif

    PageTable* table = page_table;
    bool path_promoted = false;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), flags, &path_promoted);
    }

    // Get the current page table entry by computing the index first.
    uint64_t const IDX = index_of(vaddr, 1);
    uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry& entry = entry_at(table, IDX);

    if (entry.present == 0) {
        user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
        // Page doesn't exist, nothing to modify
        if (path_promoted) {
            flush_pagemap_after_update(page_table, vaddr, true);
        }
        return;
    }

    auto* raw_entry = reinterpret_cast<uint64_t*>(&entry);
    uint64_t const OLD_RAW = *raw_entry;
    bool const PRIVATE_WRITE = (flags & (paging::PAGE_USER | paging::PAGE_WRITE)) == (paging::PAGE_USER | paging::PAGE_WRITE) &&
                               (OLD_RAW & paging::PAGE_SHARED) == 0U;
    bool const IS_COW = PRIVATE_WRITE && ((OLD_RAW & paging::PAGE_COW) != 0U ||
                                          phys::page_ref_get(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT)) > 1U);
    // COW authorizes an admitted private write. READ/NONE revoke it; granting
    // WRITE to a cached or fork-aliased frame defers the write to the resolver.
    *raw_entry = IS_COW ? (OLD_RAW | paging::PAGE_COW) : (OLD_RAW & ~paging::PAGE_COW);

    // Update page table entry flags
    entry.present = (flags & paging::PAGE_PRESENT) != 0U ? 1 : 0;
    // COW pages must stay read-only even when mprotect(PROT_WRITE) runs; the
    // write fault is what creates the private writable page.
    entry.writable = ((flags & paging::PAGE_WRITE) != 0U && !IS_COW && !paging::shared_write_restricted(OLD_RAW)) ? 1 : 0;
    entry.user = (flags & paging::PAGE_USER) != 0U ? 1 : 0;
    entry.no_execute = (flags & paging::PAGE_NX) != 0U ? 1 : 0;

    constexpr uint64_t NX_BIT_POSITION = 63ULL;
    if ((flags & paging::PAGE_NX) != 0U) {
        *raw_entry |= (1ULL << NX_BIT_POSITION);  // Set NX bit
    } else {
        *raw_entry &= ~(1ULL << NX_BIT_POSITION);  // Clear NX bit
    }

    owned_frame_refresh_leaf(page_table, vaddr, entry);
    uint64_t const NEW_RAW = *raw_entry;
    user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);

    if (NEW_RAW != OLD_RAW || path_promoted) {
        flush_pagemap_after_update(page_table, vaddr, path_promoted);
    }

#ifdef ELF_DEBUG
    if (vaddr >= 0x501000 && vaddr < 0x580000) {
        log::debug("unify_page_flags: vaddr=0x%x, flags=0x%x, entry_after=0x%x, nx=%d present=%d", vaddr, flags, NEW_RAW,
                   static_cast<int>((NEW_RAW >> NX_BIT_POSITION) & 1U), static_cast<int>((NEW_RAW & paging::PAGE_PRESENT) != 0U));
    }
#endif
}

void unmap_page(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unmap_page");
        hcf();
    }

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(page_table, "unmap-page", vaddr);
#endif

    vaddr = page_align_down(vaddr);
    AnonymousSwapObject* object_to_release = nullptr;
    AnonymousSwapMapping* mapping_to_delete = nullptr;
    uint64_t const COW_LOCK_FLAGS = cow_pte_lock.lock_irqsave();
    auto* swap_mapping = find_anonymous_swap_mapping_locked(page_table, vaddr);
    if (swap_mapping != nullptr) {
        swap_mapping->cancelled = true;
        if (swap_mapping->state == AnonymousSwapRuntimeState::SWAPPED && !swap_mapping->pagein_queued) {
            remove_anonymous_swap_mapping_locked(swap_mapping);
            object_to_release = detach_anonymous_swap_object_locked(swap_mapping);
            mapping_to_delete = swap_mapping;
            anonymous_swap_stats.swapped_pages.fetch_sub(1, std::memory_order_relaxed);
        } else if (swap_mapping->state == AnonymousSwapRuntimeState::FAILED) {
            remove_anonymous_swap_mapping_locked(swap_mapping);
            object_to_release = detach_anonymous_swap_object_locked(swap_mapping);
            mapping_to_delete = swap_mapping;
        }
    }

    uint64_t const PIN_LOCK_FLAGS = user_mapping_pin_lock.lock_irqsave();
    PageTableEntry* entry = leaf_entry(page_table, vaddr);
    if (entry == nullptr) {
        user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
        cow_pte_lock.unlock_irqrestore(COW_LOCK_FLAGS);
        release_anonymous_swap_object(object_to_release);
        delete mapping_to_delete;
        return;
    }

    PageTableEntry const OLD_ENTRY = *entry;
    if (OLD_ENTRY.present == 0 && !is_reserved_leaf(OLD_ENTRY)) {
        user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
        cow_pte_lock.unlock_irqrestore(COW_LOCK_FLAGS);
        release_anonymous_swap_object(object_to_release);
        delete mapping_to_delete;
        return;
    }
    owned_frame_untrack_leaf(page_table, vaddr, OLD_ENTRY);
    *entry = paging::purge_page_table_entry();
    user_mapping_pin_lock.unlock_irqrestore(PIN_LOCK_FLAGS);
    cow_pte_lock.unlock_irqrestore(COW_LOCK_FLAGS);
    flush_pagemap_after_update(page_table, vaddr, false);
    drop_present_leaf_ref(OLD_ENTRY);
    release_anonymous_swap_object(object_to_release);
    delete mapping_to_delete;
}

auto protect_anonymous_swap_page(PageTable* page_table, vaddr_t vaddr, uint64_t flags) -> bool {
    if (page_table == nullptr) {
        return false;
    }
    vaddr = page_align_down(vaddr);
    uint64_t const COW_FLAGS = cow_pte_lock.lock_irqsave();
    auto* mapping = find_anonymous_swap_mapping_locked(page_table, vaddr);
    if (mapping == nullptr || mapping->state == AnonymousSwapRuntimeState::PAGEOUT) {
        cow_pte_lock.unlock_irqrestore(COW_FLAGS);
        return false;
    }

    uint64_t updated = flags & ~(PTE_FRAME_MASK | paging::PAGE_COW);
    if ((flags & (paging::PAGE_USER | paging::PAGE_WRITE)) == (paging::PAGE_USER | paging::PAGE_WRITE) &&
        (mapping->original_raw & paging::PAGE_COW) != 0U) {
        updated |= paging::PAGE_COW;
        updated &= ~paging::PAGE_WRITE;
    }
    mapping->original_raw = updated;
    PageTableEntry* entry = leaf_entry(page_table, vaddr);
    if (entry != nullptr && entry->present == 0 && is_reserved_leaf(*entry)) {
        uint64_t const PIN_FLAGS = user_mapping_pin_lock.lock_irqsave();
        *entry = pte_from_raw(anonymous_swap_marker_raw(updated));
        user_mapping_pin_lock.unlock_irqrestore(PIN_FLAGS);
    }
    cow_pte_lock.unlock_irqrestore(COW_FLAGS);
    return true;
}

namespace {

auto release_kernel_vmap_mappings(vaddr_t start, size_t page_count) -> bool {
    if (kernel_pagemap == nullptr || page_count == 0) {
        return false;
    }

    // Validate the complete range before mutating it so a bad size or double
    // free cannot leave a partially torn-down allocation.
    for (size_t page = 0; page < page_count; ++page) {
        vaddr_t const VADDR = start + (page * paging::PAGE_SIZE);
        PageTableEntry* entry = leaf_entry(kernel_pagemap, VADDR);
        if (entry == nullptr || entry->present == 0 || entry->frame == 0) {
            return false;
        }
    }

    // Buffer-cache vmap allocations are at most 2 MiB, but keep teardown
    // bounded for any future caller. PTEs are cleared before the global flush;
    // frames remain referenced until every CPU has discarded stale leaves.
    constexpr size_t RELEASE_BATCH_PAGES = 512;
    std::array<void*, RELEASE_BATCH_PAGES> frames{};
    size_t released = 0;
    while (released < page_count) {
        size_t const BATCH = std::min(RELEASE_BATCH_PAGES, page_count - released);
        for (size_t page = 0; page < BATCH; ++page) {
            vaddr_t const VADDR = start + ((released + page) * paging::PAGE_SIZE);
            PageTableEntry* entry = leaf_entry(kernel_pagemap, VADDR);
            uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry->frame) << paging::PAGE_SHIFT;
            frames.at(page) = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS_ADDR));
            *entry = paging::purge_page_table_entry();
        }

        shootdown_shared_kernel_mappings();
        for (size_t page = 0; page < BATCH; ++page) {
            phys::page_ref_dec(frames.at(page));
            frames.at(page) = nullptr;
        }
        released += BATCH;
    }
    return true;
}

auto kernel_vmap_context_can_drain() -> bool {
    if (tlb_shootdown_vector == 0 || !smt::has_cpu_data()) {
        return true;
    }
    return sched::has_run_queues() && sched::preempt_count() == 0 && sched::interrupts_enabled();
}

auto kernel_vmap_reclaim_count(void* /*opaque*/, const reclaim::ReclaimRequest& /*request*/) -> reclaim::ReclaimCount {
    return {.reclaimable = kernel_vmap_pending_free_pages()};
}

auto kernel_vmap_reclaim_scan(void* /*opaque*/, const reclaim::ReclaimRequest& request) -> reclaim::ReclaimScanResult {
    uint64_t budget_pages = request.budget_units;
    if (request.scan_budget_units != 0) {
        budget_pages = std::min(budget_pages, request.scan_budget_units);
    }
    size_t const BUDGET = budget_pages > SIZE_MAX ? SIZE_MAX : static_cast<size_t>(budget_pages);
    auto const STATS = drain_kernel_vmap_frees_bounded(BUDGET);
    return {
        .scanned = STATS.scanned_pages,
        .reclaimed = STATS.reclaimed_pages,
        .has_more = STATS.has_more,
        .failed = STATS.failed,
    };
}

}  // namespace

void init_kernel_vmap() {
    if (kernel_vmap_initialized) {
        return;
    }
    if (kernel_pagemap == nullptr) {
        log::critical("kernel vmap init requires the kernel pagemap");
        hcf();
    }

    uint64_t const TOTAL_MEM = phys::get_total_mem_bytes();
    uint64_t arena_bytes = std::clamp<uint64_t>(TOTAL_MEM / 2, KERNEL_VMAP_MIN_BYTES, KERNEL_VMAP_MAX_BYTES);
    arena_bytes = (arena_bytes + LARGE_PAGE_2M_BYTES - 1) & ~(LARGE_PAGE_2M_BYTES - 1);
    kernel_vmap_active_pages = static_cast<size_t>(arena_bytes / paging::PAGE_SIZE);
    kernel_vmap_next_page = 0;
    kernel_vmap_pending_first_page = kernel_vmap_active_pages;
    kernel_vmap_pending_pages = 0;
    kernel_vmap_bitmap.fill(0);
    kernel_vmap_pending_bitmap.fill(0);
    kernel_vmap_draining.store(false, std::memory_order_relaxed);

    // Pre-create the complete hierarchy down through PML1. Runtime map_page()
    // calls then only publish independent leaf PTEs and cannot recurse into
    // physical reclaim to allocate a page table.
    uint64_t const FLAGS = kernel_data_page_flags();
    for (uint64_t offset = 0; offset < arena_bytes; offset += LARGE_PAGE_2M_BYTES) {
        vaddr_t const VADDR = KERNEL_VMAP_BASE + offset;
        PageTable* pml3 = advance_page_table(kernel_pagemap, index_of(VADDR, 4), FLAGS);
        PageTable* pml2 = advance_page_table(pml3, index_of(VADDR, 3), FLAGS);
        static_cast<void>(advance_page_table(pml2, index_of(VADDR, 2), FLAGS));
    }

    kernel_vmap_initialized = true;
    constexpr reclaim::Shrinker SHRINKER{
        .name = "kernel_vmap",
        .unit = reclaim::ReclaimUnit::PAGES,
        .scan_unit = reclaim::ReclaimUnit::PAGES,
        .rank = 0,
        .min_priority = reclaim::ReclaimPriority::LOW,
        .capabilities = reclaim::RECLAIM_MAY_BLOCK,
        .max_batch_units = 512,
        .max_scan_units = 512,
        .count = kernel_vmap_reclaim_count,
        .scan = kernel_vmap_reclaim_scan,
    };
    if (!reclaim::register_shrinker(SHRINKER)) {
        log::critical("failed to register kernel-vmap reclaim shrinker");
    }
#ifdef WOS_SELFTEST
    selftest_register_kernel_page_tables();
    selftest_audit_boot_direct_map_boundaries("post-kernel-vmap-init");
#endif
    log::info("prepared kernel vmap arena: base=%p bytes=%llu pages=%zu", KERNEL_VMAP_BASE, static_cast<unsigned long long>(arena_bytes),
              kernel_vmap_active_pages);
}

auto init_guarded_kernel_stack_pool(void* backing, size_t capacity) -> bool {
    if (guarded_kernel_stack_initialized || kernel_pagemap == nullptr || backing == nullptr || capacity == 0 ||
        capacity > UINT64_MAX / GUARDED_KERNEL_STACK_STRIDE) {
        return false;
    }

    uint64_t const ARENA_BYTES = static_cast<uint64_t>(capacity) * GUARDED_KERNEL_STACK_STRIDE;
    if (capacity > ker::mod::mm::KERNEL_STACK_POOL_CAPACITY || GUARDED_KERNEL_STACK_ARENA_BASE > UINT64_MAX - ARENA_BYTES ||
        GUARDED_KERNEL_STACK_ARENA_BASE + ARENA_BYTES > ker::mod::mm::GUARDED_KERNEL_STACK_ARENA_END) {
        return false;
    }

    uint64_t const FLAGS = kernel_data_page_flags();
    uint64_t const HHDM_BACKING = reinterpret_cast<uint64_t>(backing);
    PageMapBatch batch{};
    init_page_map_batch(&batch, kernel_pagemap, FLAGS);
    for (size_t slot = 0; slot < capacity; ++slot) {
        vaddr_t const STACK_BASE =
            GUARDED_KERNEL_STACK_ARENA_BASE + (static_cast<uint64_t>(slot) * GUARDED_KERNEL_STACK_STRIDE) + paging::PAGE_SIZE;
        uint64_t const BACKING_OFFSET = static_cast<uint64_t>(slot) * ker::mod::mm::KERNEL_STACK_SIZE;
        for (uint64_t offset = 0; offset < ker::mod::mm::KERNEL_STACK_SIZE; offset += paging::PAGE_SIZE) {
            auto const PADDR = reinterpret_cast<paddr_t>(addr::get_phys_pointer(HHDM_BACKING + BACKING_OFFSET + offset));
            map_page_batched(&batch, STACK_BASE + offset, PADDR, FLAGS);
        }
    }
    flush_page_map_batch(&batch);
    guarded_kernel_stack_capacity = capacity;
    guarded_kernel_stack_initialized = true;

#ifdef WOS_SELFTEST
    selftest_register_kernel_page_tables();
#endif
    log::info("prepared guarded kernel stacks: base=%p slots=%zu stack_bytes=%zu guard_bytes=%zu", GUARDED_KERNEL_STACK_ARENA_BASE,
              capacity, ker::mod::mm::KERNEL_STACK_SIZE, paging::PAGE_SIZE);
    return true;
}

auto guarded_kernel_stack_slot(size_t index) -> void* {
    if (!guarded_kernel_stack_initialized || index >= guarded_kernel_stack_capacity) {
        return nullptr;
    }
    vaddr_t const BASE = GUARDED_KERNEL_STACK_ARENA_BASE + (static_cast<uint64_t>(index) * GUARDED_KERNEL_STACK_STRIDE) + paging::PAGE_SIZE;
    return reinterpret_cast<void*>(BASE);
}

auto audit_guarded_kernel_stack(const void* stack_base) -> bool {
    if (!guarded_kernel_stack_initialized || stack_base == nullptr) {
        return false;
    }
    auto const BASE = reinterpret_cast<vaddr_t>(stack_base);
    if (BASE < GUARDED_KERNEL_STACK_ARENA_BASE + paging::PAGE_SIZE) {
        return false;
    }
    uint64_t const OFFSET = BASE - (GUARDED_KERNEL_STACK_ARENA_BASE + paging::PAGE_SIZE);
    if ((OFFSET % GUARDED_KERNEL_STACK_STRIDE) != 0U || OFFSET / GUARDED_KERNEL_STACK_STRIDE >= guarded_kernel_stack_capacity) {
        return false;
    }
    MappingPermissions const LOWER = mapping_permissions(kernel_pagemap, BASE - paging::PAGE_SIZE);
    MappingPermissions const UPPER = mapping_permissions(kernel_pagemap, BASE + ker::mod::mm::KERNEL_STACK_SIZE);
    return !LOWER.present && !UPPER.present &&
           audit_mapping_range(BASE, BASE + ker::mod::mm::KERNEL_STACK_SIZE, true, !kernel_nx_supported);
}

auto kernel_vmap_pending_free_pages() -> size_t {
    if (!kernel_vmap_initialized) {
        return 0;
    }
    uint64_t const FLAGS = kernel_vmap_lock.lock_irqsave();
    size_t const PENDING = kernel_vmap_pending_pages;
    kernel_vmap_lock.unlock_irqrestore(FLAGS);
    return PENDING;
}

auto drain_kernel_vmap_frees_bounded(size_t max_pages) -> KernelVmapReclaimStats {
    KernelVmapReclaimStats stats{};
    if (!kernel_vmap_initialized || max_pages == 0) {
        stats.has_more = kernel_vmap_pending_free_pages() != 0;
        return stats;
    }
    if (!kernel_vmap_context_can_drain()) {
        stats.has_more = kernel_vmap_pending_free_pages() != 0;
        stats.unsafe_context = stats.has_more;
        return stats;
    }

    bool expected = false;
    if (!kernel_vmap_draining.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        stats.has_more = kernel_vmap_pending_free_pages() != 0;
        return stats;
    }

    while (stats.reclaimed_pages < max_pages) {
        size_t first_page = 0;
        size_t page_count = 0;
        size_t const REMAINING = max_pages - stats.reclaimed_pages;
        if (!find_pending_kernel_vmap_run(REMAINING, first_page, page_count)) {
            break;
        }
        stats.scanned_pages += page_count;
        vaddr_t const START = KERNEL_VMAP_BASE + (first_page * paging::PAGE_SIZE);
        if (!release_kernel_vmap_mappings(START, page_count)) {
            log::critical("failed to release deferred kernel vmap run: ptr=%p pages=%zu", START, page_count);
            stats.failed = true;
            break;
        }
        if (!release_kernel_vmap_span(first_page, page_count, true)) {
            log::critical("kernel vmap pending bitmap mismatch: ptr=%p pages=%zu", START, page_count);
            stats.failed = true;
            break;
        }
        stats.reclaimed_pages += page_count;
    }

    kernel_vmap_draining.store(false, std::memory_order_release);
    stats.has_more = kernel_vmap_has_pending_frees();
    return stats;
}

void drain_kernel_vmap_frees() {
    while (kernel_vmap_context_can_drain()) {
        auto const STATS = drain_kernel_vmap_frees_bounded(SIZE_MAX);
        if (STATS.failed || !STATS.has_more || STATS.reclaimed_pages == 0) {
            return;
        }
    }
}

void drain_kernel_vmap_frees_if_over_limit() {
    if (kernel_vmap_warm_pool_over_limit()) {
        drain_kernel_vmap_frees();
    }
}

auto kernel_vmap_contains(const void* ptr) -> bool {
    if (!kernel_vmap_initialized || ptr == nullptr) {
        return false;
    }
    auto const ADDRESS = reinterpret_cast<vaddr_t>(ptr);
    uint64_t const ARENA_BYTES = static_cast<uint64_t>(kernel_vmap_active_pages) * paging::PAGE_SIZE;
    return ADDRESS >= KERNEL_VMAP_BASE && ADDRESS - KERNEL_VMAP_BASE < ARENA_BYTES;
}

auto kernel_vmap_alloc(PhysicalPageOwner owner, uint64_t size, std::string_view name) -> void* {
    if (!kernel_vmap_initialized || size == 0 || size > UINT64_MAX - (paging::PAGE_SIZE - 1)) {
        return nullptr;
    }

    uint64_t const ROUNDED_SIZE = (size + paging::PAGE_SIZE - 1) & ~(paging::PAGE_SIZE - 1);
    auto const PAGE_COUNT = static_cast<size_t>(ROUNDED_SIZE / paging::PAGE_SIZE);
    size_t first_page = 0;
    if (!reserve_kernel_vmap_span(PAGE_COUNT, first_page)) {
        return nullptr;
    }

    vaddr_t const START = KERNEL_VMAP_BASE + (first_page * paging::PAGE_SIZE);
    size_t mapped_pages = 0;
    for (; mapped_pages < PAGE_COUNT; ++mapped_pages) {
        vaddr_t const VADDR = START + (mapped_pages * paging::PAGE_SIZE);
        PageTableEntry* entry = leaf_entry(kernel_pagemap, VADDR);
        if (entry != nullptr && entry->present != 0 && entry->frame != 0) {
            continue;
        }
        void* frame = phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(owner, name);
        if (frame == nullptr) {
            break;
        }
        auto const PADDR = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(frame)));
        map_page(kernel_pagemap, VADDR, PADDR, kernel_data_page_flags());
    }

    if (mapped_pages != PAGE_COUNT) {
        if (mapped_pages != 0) {
            static_cast<void>(queue_kernel_vmap_free(first_page, mapped_pages));
        }
        if (mapped_pages < PAGE_COUNT) {
            static_cast<void>(release_kernel_vmap_span(first_page + mapped_pages, PAGE_COUNT - mapped_pages));
        }
        drain_kernel_vmap_frees();
        return nullptr;
    }
    return reinterpret_cast<void*>(START);
}

void kernel_vmap_free(void* ptr, uint64_t size) {
    if (ptr == nullptr || !kernel_vmap_initialized || size == 0 || size > UINT64_MAX - (paging::PAGE_SIZE - 1)) {
        return;
    }

    auto const START = reinterpret_cast<vaddr_t>(ptr);
    uint64_t const ARENA_BYTES = static_cast<uint64_t>(kernel_vmap_active_pages) * paging::PAGE_SIZE;
    uint64_t const ROUNDED_SIZE = (size + paging::PAGE_SIZE - 1) & ~(paging::PAGE_SIZE - 1);
    if ((START & (paging::PAGE_SIZE - 1)) != 0 || START < KERNEL_VMAP_BASE || START - KERNEL_VMAP_BASE >= ARENA_BYTES ||
        ROUNDED_SIZE > ARENA_BYTES - (START - KERNEL_VMAP_BASE)) {
        log::critical("rejected invalid kernel vmap free: ptr=%p size=%llu", ptr, static_cast<unsigned long long>(size));
        return;
    }

    auto const FIRST_PAGE = static_cast<size_t>((START - KERNEL_VMAP_BASE) / paging::PAGE_SIZE);
    auto const PAGE_COUNT = static_cast<size_t>(ROUNDED_SIZE / paging::PAGE_SIZE);
    if (!queue_kernel_vmap_free(FIRST_PAGE, PAGE_COUNT)) {
        log::critical("rejected duplicate or unmapped kernel vmap free: ptr=%p size=%llu", ptr, static_cast<unsigned long long>(size));
        return;
    }

    // A small warm pool avoids global shootdowns in hot alloc/free cycles, but
    // it must not retain the multi-gigabyte high-water mark of parallel native
    // builds. drain_kernel_vmap_frees() is a no-op in IRQ-disabled or
    // non-preemptible contexts, preserving the deferred-free lock invariant.
    drain_kernel_vmap_frees_if_over_limit();
}

void map_range(PageTable* page_table, Range range, uint64_t flags, uint64_t offset) {
    auto [start, end] = range;
    if (((start % paging::PAGE_SIZE) != 0U) || ((end % paging::PAGE_SIZE) != 0U) || start >= end) {
        // PANIC!
        log::critical("init: failed to map range");
        hcf();
    }

    while (start != end) {
        map_page(page_table, start + offset, start, flags);
        start += paging::PAGE_SIZE;
    }
}

void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags) {
    map_page(kernel_pagemap, vaddr, paddr, flags);
#ifdef WOS_SELFTEST
    selftest_register_kernel_page_table_path(vaddr);
#endif
}

void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset) {
    map_range(kernel_pagemap, range, flags, offset);
#ifdef WOS_SELFTEST
    selftest_register_kernel_page_tables();
#endif
}

void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    // no offset assume hhdm
    map_range(kernel_pagemap, range, flags, addr::get_hhdm_offset());
#ifdef WOS_SELFTEST
    selftest_register_kernel_page_tables();
#endif
}

namespace {

// x86-64 canonical address check: bits[63:47] must all be the same.
// For kernel HHDM addresses (bit 47 set) the upper 17 bits are all 1s.
constexpr int CANONICAL_SIGN_BIT = 47;
constexpr uint64_t CANONICAL_KERNEL_UPPER = 0x1ffffULL;

struct FrameProbeCache {
    phys::PageLookupHint lookup;
};

auto cached_allocator_contains_phys(FrameProbeCache* cache, uint64_t phys_addr) -> bool {
    if (cache == nullptr || cache->lookup.allocator == nullptr) {
        return false;
    }

    auto* const ALLOC = cache->lookup.allocator;
    auto const ALLOC_START_PHYS = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(ALLOC->base)));
    uint64_t const ALLOC_BYTES = static_cast<uint64_t>(ALLOC->total_pages) * paging::PAGE_SIZE;
    uint64_t const ALLOC_END_PHYS = ALLOC_START_PHYS + ALLOC_BYTES;
    return ALLOC_END_PHYS >= ALLOC_START_PHYS && phys_addr >= ALLOC_START_PHYS && phys_addr < ALLOC_END_PHYS;
}

auto phys_is_in_ram(uint64_t phys_addr, FrameProbeCache* cache = nullptr) -> bool {
    if (cached_allocator_contains_phys(cache, phys_addr)) {
        return true;
    }

    for (auto* zone = phys::get_zones(); zone != nullptr; zone = zone->next) {
        auto zone_start_phys = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(zone->start)));
        if (phys_addr >= zone_start_phys && phys_addr < zone_start_phys + zone->len) {
            if (cache != nullptr && zone->allocator != nullptr) {
                cache->lookup.allocator = zone->allocator;
            }
            return true;
        }
    }
    return false;
}

auto phys_to_hhdm_checked(uint64_t phys_addr, FrameProbeCache* cache = nullptr) -> void* {
    const uint64_t PAGE_BASE = phys_addr & ~(paging::PAGE_SIZE - 1);
    if (!phys_is_in_ram(PAGE_BASE, cache)) {
        return nullptr;
    }

    const uint64_t VIRT_RAW = PAGE_BASE + addr::get_hhdm_offset();
    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return nullptr;
    }

    if (phys::page_ref_get(reinterpret_cast<void*>(VIRT_RAW), cache != nullptr ? &cache->lookup : nullptr) == 0) {
        return nullptr;
    }

    return reinterpret_cast<void*>(VIRT_RAW);
}

#ifdef WOS_SELFTEST
__attribute__((no_sanitize("address"))) void selftest_require_user_page_table(PageTable const* root, PageTable const* table, int level,
                                                                              const char* phase, uint64_t owner_pid, const char* reason) {
    if (!selftest_kernel_page_table_frame(table)) {
        return;
    }

    dbg::emergency_log(
        "user page-table walk reached permanent kernel frame phase=%s root=0x%lx table=0x%lx level=%lu pid=%lu reason=%s caller=0x%lx\n",
        phase != nullptr ? phase : "?", reinterpret_cast<uint64_t>(root), reinterpret_cast<uint64_t>(table), static_cast<uint64_t>(level),
        owner_pid, reason != nullptr ? reason : "?", reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    ker::mod::dbg::panic_handler("user address-space teardown entered the permanent kernel page-table hierarchy");
}
#endif

#ifdef WOS_MM_RECLAIM_MAGIC_PROBES
auto phys_to_hhdm_for_live_probe(uint64_t phys_addr, PageKind kind, FrameProbeCache* cache = nullptr) -> void* {
    const uint64_t PAGE_BASE = phys_addr & ~(paging::PAGE_SIZE - 1);
    if (!phys_is_in_ram(PAGE_BASE, cache)) {
        return nullptr;
    }

    const uint64_t VIRT_RAW = PAGE_BASE + addr::get_hhdm_offset();
    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return nullptr;
    }

    PageKind const KIND = decode_page_kind(static_cast<uint8_t>(kind));
    if (page_kind_has_known_live_payload(KIND)) {
        return reinterpret_cast<void*>(VIRT_RAW);
    }
    if (KIND != PageKind::UNKNOWN) {
        return nullptr;
    }

    if (phys::page_ref_get(reinterpret_cast<void*>(VIRT_RAW), cache != nullptr ? &cache->lookup : nullptr) == 0) {
        return nullptr;
    }
    return reinterpret_cast<void*>(VIRT_RAW);
}
#endif

auto frame_page_kind(uint64_t phys_addr, FrameProbeCache* cache = nullptr) -> PageKind {
    uint64_t const PAGE_BASE = phys_addr & ~(paging::PAGE_SIZE - 1);
    if (!phys_is_in_ram(PAGE_BASE, cache)) {
        return PageKind::UNKNOWN;
    }

    uint64_t const VIRT_RAW = PAGE_BASE + addr::get_hhdm_offset();
    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return PageKind::UNKNOWN;
    }

    return phys::page_kind_get(reinterpret_cast<void*>(VIRT_RAW), cache != nullptr ? &cache->lookup : nullptr);
}

auto page_table_tree_contains_frame(PageTable* root, uint64_t target_phys, FrameProbeCache* cache = nullptr) -> bool {
    if (root == nullptr) {
        return false;
    }

    target_phys &= ~(paging::PAGE_SIZE - 1);
    if (reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(root))) == target_phys) {
        return true;
    }

    constexpr size_t USER_PML4_ENTRIES = 256;
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = entry_at(root, i4);
        if (!pml4e.present) {
            continue;
        }

        const uint64_t PML3_PHYS = static_cast<uint64_t>(pml4e.frame) << paging::PAGE_SHIFT;
        if (PML3_PHYS == target_phys) {
            return true;
        }

        auto* pml3 = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PML3_PHYS, cache));
        if (pml3 == nullptr) {
            continue;
        }
        for (auto pml3e : pml3->entries) {
            if (!pml3e.present || pml3e.pagesize) {
                continue;
            }

            const uint64_t PML2_PHYS = static_cast<uint64_t>(pml3e.frame) << paging::PAGE_SHIFT;
            if (PML2_PHYS == target_phys) {
                return true;
            }

            auto* pml2 = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PML2_PHYS, cache));
            if (pml2 == nullptr) {
                continue;
            }
            for (auto pml2e : pml2->entries) {
                if (!pml2e.present || pml2e.pagesize) {
                    continue;
                }

                const uint64_t PML1_PHYS = static_cast<uint64_t>(pml2e.frame) << paging::PAGE_SHIFT;
                if (PML1_PHYS == target_phys) {
                    return true;
                }
            }
        }
    }

    return false;
}

enum class LiveAllocatorFrameKind : uint8_t {
    NONE = 0,
    SLAB,
    MEDIUM,
    KMALLOC_LARGE,
};

auto live_allocator_kind_to_page_kind(LiveAllocatorFrameKind kind) -> PageKind {
    switch (kind) {
        case LiveAllocatorFrameKind::SLAB:
            return PageKind::SLAB;
        case LiveAllocatorFrameKind::MEDIUM:
            return PageKind::MEDIUM;
        case LiveAllocatorFrameKind::KMALLOC_LARGE:
            return PageKind::KMALLOC_LARGE;
        case LiveAllocatorFrameKind::NONE:
        default:
            return PageKind::UNKNOWN;
    }
}

auto classify_live_allocator_frame(uint64_t phys_addr, PageKind kind, DestroyUserSpaceCallStats* stats, FrameProbeCache* cache = nullptr)
    -> LiveAllocatorFrameKind {
    if (kind == PageKind::MEDIUM) {
        return LiveAllocatorFrameKind::MEDIUM;
    }
    if (kind == PageKind::SLAB) {
        return LiveAllocatorFrameKind::SLAB;
    }
    if (kind == PageKind::KMALLOC_LARGE) {
        return LiveAllocatorFrameKind::KMALLOC_LARGE;
    }
    if (kind != PageKind::UNKNOWN) {
        return LiveAllocatorFrameKind::NONE;
    }

#ifndef WOS_MM_RECLAIM_MAGIC_PROBES
    (void)phys_addr;
    (void)stats;
    (void)cache;
    return LiveAllocatorFrameKind::NONE;
#else
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
    constexpr uint32_t SLAB_MAGIC = 0x8CBEEFC8;
    constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;

    auto* page_ptr = phys_to_hhdm_for_live_probe(phys_addr, kind, cache);
    if (page_ptr == nullptr) {
        return LiveAllocatorFrameKind::NONE;
    }

    note_destroy_magic_unknown_probe(stats);
    const auto PAGE_BASE = reinterpret_cast<uint64_t>(page_ptr);
    LiveAllocatorFrameKind fallback = LiveAllocatorFrameKind::NONE;
    if (*reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == MEDIUM_ALLOC_MAGIC) {
        fallback = LiveAllocatorFrameKind::MEDIUM;
    } else if (*reinterpret_cast<const uint32_t*>(page_ptr) == SLAB_MAGIC) {
        fallback = LiveAllocatorFrameKind::SLAB;
    } else if (*reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == LARGE_ALLOC_MAGIC) {
        fallback = LiveAllocatorFrameKind::KMALLOC_LARGE;
    }
    if (fallback != LiveAllocatorFrameKind::NONE) {
        note_destroy_magic_unknown_hit(stats, live_allocator_kind_to_page_kind(fallback));
    }
    return fallback;
#endif
}

struct PageTableFrameSet {
    util::SmallVec<uint64_t, 64> frames;
    bool complete{true};
    bool allow_growth{true};  // RAW keeps its original allocation/fallback path.

    auto add(uint64_t phys_addr) -> bool {
        phys_addr &= ~(paging::PAGE_SIZE - 1);
        if (phys_addr == 0 || frames.contains(phys_addr)) {
            return true;
        }
        if (!allow_growth && frames.size() >= 64) {
            complete = false;
            return false;  // Never reach an allocating SmallVec growth.
        }
        if (!frames.push_back(phys_addr)) {
            complete = false;
            return false;
        }
        return true;
    }

    auto contains(PageTable* root, uint64_t phys_addr, FrameProbeCache* cache = nullptr) const -> bool {
        phys_addr &= ~(paging::PAGE_SIZE - 1);
        if (frames.contains(phys_addr)) {
            return true;
        }
        return !complete && page_table_tree_contains_frame(root, phys_addr, cache);
    }
};

}  // namespace

constexpr size_t DESTROY_REFDEC_BATCH_CAP = 64;

struct DestroyUserSpaceBudgetState {
    struct RefdecBatch {
        std::array<void*, DESTROY_REFDEC_BATCH_CAP> pages{};
        phys::PageLookupHint lookup{};
        size_t count = 0;
        PageTable* root = nullptr;  // Original owner/gate outlives every queued ref and ACK.
    };

    PageTable* pagemap = nullptr;
    uint64_t owner_pid = 0;
    const char* owner_name = nullptr;
    const char* reason = nullptr;
    DestroyUserSpacePhase phase = DestroyUserSpacePhase::DONE;
    PageTableFrameSet page_table_frames{};
    FrameProbeCache frame_probe_cache{};
    std::array<DestroyWalkFrame, 4> stack{};
    RefdecBatch refdec_batch{};
    RefdecBatch page_table_refdec_batch{};
    size_t stack_size = 0;
    size_t access_gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    file_mapping::FileMappingTlbRetirement file_retirement{};
};

namespace {

using DestroyRefdecBatch = DestroyUserSpaceBudgetState::RefdecBatch;

enum class DestroyRefdecBatchKind : uint8_t {
    DATA,
    PAGE_TABLE,
};

void note_destroy_refdec_batch_stats(DestroyUserSpaceCallStats* stats, phys::PageRefBatchStats const& batch_stats,
                                     DestroyRefdecBatchKind kind) {
    switch (kind) {
        case DestroyRefdecBatchKind::DATA:
            note_destroy_refdec_batch_stats(stats, batch_stats);
            break;
        case DestroyRefdecBatchKind::PAGE_TABLE:
            note_destroy_page_table_refdec_batch_stats(stats, batch_stats);
            break;
    }
}

void destroy_refdec_batch_flush(DestroyRefdecBatch& batch, DestroyUserSpaceCallStats* stats, DestroyRefdecBatchKind kind) {
    if (batch.count == 0) {
        return;
    }

    if (batch.root == nullptr) dbg::panic_handler("teardown refdec batch lost retained root");
    // Every queued owner is already detached. Keep the original root/gate held
    // through the existing full-root local/remote completion protocol.
    flush_pagemap_after_update(batch.root, 0, true);
    phys::PageRefBatchStats const BATCH_STATS =
        phys::page_ref_dec_batch(std::span<void* const>{batch.pages.data(), batch.count}, &batch.lookup);
    note_destroy_refdec_batch_stats(stats, BATCH_STATS, kind);
    batch.count = 0;
}

void destroy_refdec_batch_queue(DestroyRefdecBatch& batch, void* page, DestroyUserSpaceCallStats* stats, DestroyRefdecBatchKind kind) {
    if (batch.count >= batch.pages.size()) {
        destroy_refdec_batch_flush(batch, stats, kind);
    }
    batch.pages[batch.count++] = page;
    if (batch.count >= batch.pages.size()) {
        destroy_refdec_batch_flush(batch, stats, kind);
    }
}

void destroy_state_reset_walk(DestroyUserSpaceBudgetState& state, DestroyUserSpacePhase next_phase) {
    state.phase = next_phase;
    state.stack_size = 0;
    if (next_phase == DestroyUserSpacePhase::DONE || next_phase == DestroyUserSpacePhase::TLB_FLUSH || state.pagemap == nullptr) {
        return;
    }

    state.stack[0] = DestroyWalkFrame{
        .table = state.pagemap,
        .vaddr_base = 0,
        .phys_addr = 0,
        .next_index = 0,
        .level = 4,
    };
    state.stack_size = 1;
}

auto destroy_state_push(DestroyUserSpaceBudgetState& state, PageTable* table, int level, uint64_t vaddr_base, uint64_t phys_addr) -> bool {
    if (state.stack_size >= state.stack.size()) {
        return false;
    }
    state.stack[state.stack_size++] = DestroyWalkFrame{
        .table = table,
        .vaddr_base = vaddr_base,
        .phys_addr = phys_addr,
        .next_index = 0,
        .level = static_cast<int8_t>(level),
    };
    return true;
}

auto destroy_state_top(DestroyUserSpaceBudgetState& state) -> DestroyWalkFrame& { return state.stack[state.stack_size - 1]; }

void destroy_state_pop(DestroyUserSpaceBudgetState& state) {
    if (state.stack_size > 0) {
        state.stack_size--;
    }
}

void destroy_state_flush_refdec_batch(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats* stats) {
    destroy_refdec_batch_flush(state.refdec_batch, stats, DestroyRefdecBatchKind::DATA);
}

void destroy_state_flush_page_table_refdec_batch(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats* stats) {
    destroy_refdec_batch_flush(state.page_table_refdec_batch, stats, DestroyRefdecBatchKind::PAGE_TABLE);
}

void destroy_state_queue_refdec(DestroyUserSpaceBudgetState& state, void* page, DestroyUserSpaceCallStats& stats) {
    destroy_refdec_batch_queue(state.refdec_batch, page, &stats, DestroyRefdecBatchKind::DATA);
}

void destroy_state_queue_page_table_refdec(DestroyUserSpaceBudgetState& state, void* page, DestroyUserSpaceCallStats& stats) {
    destroy_refdec_batch_queue(state.page_table_refdec_batch, page, &stats, DestroyRefdecBatchKind::PAGE_TABLE);
}

void collect_page_table_frames(PageTable* table, int level, PageTableFrameSet& frames, DestroyUserSpaceCallStats* stats,
                               FrameProbeCache* frame_probe_cache = nullptr, PageTable* root = nullptr, uint64_t owner_pid = 0,
                               const char* reason = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

#ifdef WOS_SELFTEST
    selftest_require_user_page_table(root != nullptr ? root : table, table, level, "collect", owner_pid, reason);
#endif

    frames.add(reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(table))));

    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;
    for (size_t i = 0; i < MAX_ENTRY; ++i) {
        const auto& entry = entry_at(table, i);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0 || level <= 1) {
            continue;
        }
        if (entry.pagesize != 0) {
            note_destroy_huge_skip(stats);
            continue;
        }
        PageKind const KIND = frame_page_kind(PHYS_ADDR, frame_probe_cache);
        LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, stats, frame_probe_cache);
        if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
            note_destroy_kind_skip(stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
            continue;
        }
        if (KIND != PageKind::PAGE_TABLE) {
            note_destroy_kind_skip(stats, KIND);
            continue;
        }

        frames.add(PHYS_ADDR);
        auto* next_level = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PHYS_ADDR, frame_probe_cache));
        if (next_level != nullptr) {
            collect_page_table_frames(next_level, level - 1, frames, stats, frame_probe_cache, root != nullptr ? root : table, owner_pid,
                                      reason);
        }
    }
}

auto advance_collect_frames_budgeted(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats& stats) -> bool {
    while (state.stack_size > 0) {
        auto& frame = destroy_state_top(state);
#ifdef WOS_SELFTEST
        selftest_require_user_page_table(state.pagemap, frame.table, frame.level, "budget-collect", state.owner_pid, state.reason);
#endif
        size_t const MAX_ENTRY = frame.level == 4 ? 256 : 512;
        if (frame.next_index >= MAX_ENTRY) {
            destroy_state_pop(state);
            continue;
        }

        size_t const INDEX = frame.next_index++;
        auto const& entry = entry_at(frame.table, INDEX);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0 || frame.level <= 1) {
            return true;
        }
        if (entry.pagesize != 0) {
            note_destroy_huge_skip(&stats);
            return true;
        }
        PageKind const KIND = frame_page_kind(PHYS_ADDR, &state.frame_probe_cache);
        LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, &stats, &state.frame_probe_cache);
        if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
            note_destroy_kind_skip(&stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
            return true;
        }
        if (KIND != PageKind::PAGE_TABLE) {
            note_destroy_kind_skip(&stats, KIND);
            return true;
        }

        bool const ADDED = state.page_table_frames.add(PHYS_ADDR);
        if (!ADDED && !state.page_table_frames.allow_growth)
            dbg::panic_handler("native root table census exceeded inline capacity before mutation");
        auto* next_level = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PHYS_ADDR, &state.frame_probe_cache));
        if (next_level != nullptr) {
            (void)destroy_state_push(state, next_level, frame.level - 1, 0, PHYS_ADDR);
        }
        return true;
    }

    if (!state.page_table_frames.allow_growth && !state.page_table_frames.complete)
        dbg::panic_handler("native incomplete root table census before free-data");
    destroy_state_reset_walk(state, DestroyUserSpacePhase::FREE_DATA);
    return false;
}

auto advance_free_data_budgeted(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats& stats) -> bool {
    while (state.stack_size > 0) {
        auto& frame = destroy_state_top(state);
#ifdef WOS_SELFTEST
        selftest_require_user_page_table(state.pagemap, frame.table, frame.level, "budget-free-data", state.owner_pid, state.reason);
#endif
        size_t const MAX_ENTRY = frame.level == 4 ? 256 : 512;
        if (frame.next_index >= MAX_ENTRY) {
            destroy_state_pop(state);
            continue;
        }

        size_t const INDEX = frame.next_index++;
        auto& entry = entry_at(frame.table, INDEX);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        int const LEVEL_SHIFT = 12 + (9 * (frame.level - 1));
        uint64_t const ENTRY_VADDR = frame.vaddr_base | (static_cast<uint64_t>(INDEX) << LEVEL_SHIFT);

        if (frame.level > 1) {
            if (entry.pagesize != 0) {
                note_destroy_huge_skip(&stats);
                entry = paging::purge_page_table_entry();
                return true;
            }
            PageKind const KIND = frame_page_kind(PHYS_ADDR, &state.frame_probe_cache);
            LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, &stats, &state.frame_probe_cache);
            if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
                note_destroy_kind_skip(&stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
                entry = paging::purge_page_table_entry();
                return true;
            }
            if (KIND != PageKind::PAGE_TABLE) {
                note_destroy_kind_skip(&stats, KIND);
                entry = paging::purge_page_table_entry();
                return true;
            }

            uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
            if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                note_destroy_corrupt_skip(&stats);
                log::warn(
                    "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx "
                    "virt=0x%llx corrupt non-canonical next-level pointer",
                    state.owner_pid, state.owner_name != nullptr ? state.owner_name : "?", state.reason != nullptr ? state.reason : "?",
                    static_cast<void*>(state.pagemap), frame.level, INDEX, static_cast<unsigned long long>(ENTRY_VADDR),
                    static_cast<unsigned long long>(entry.frame), static_cast<unsigned long long>(PHYS_ADDR),
                    static_cast<unsigned long long>(VIRT_RAW));
                entry = paging::purge_page_table_entry();
                return true;
            }

            auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
            (void)destroy_state_push(state, next_level, frame.level - 1, ENTRY_VADDR, PHYS_ADDR);
            return true;
        }

        stats.data_leaf_entries_visited++;
        PageKind const KIND = frame_page_kind(PHYS_ADDR, &state.frame_probe_cache);
        bool const IS_TREE_PAGE_TABLE_FRAME = state.page_table_frames.contains(state.pagemap, PHYS_ADDR, &state.frame_probe_cache);
        LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, &stats, &state.frame_probe_cache);
        bool const IS_NORMAL_DATA = KIND == PageKind::NORMAL;
        if (!IS_TREE_PAGE_TABLE_FRAME && IS_NORMAL_DATA) {
            uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
            if ((VIRT_RAW >> CANONICAL_SIGN_BIT) == CANONICAL_KERNEL_UPPER) {
                void* page_virt = reinterpret_cast<void*>(VIRT_RAW);
                entry = paging::purge_page_table_entry();
                destroy_state_queue_refdec(state, page_virt, stats);
            } else {
                note_destroy_corrupt_skip(&stats);
                log::warn(
                    "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx "
                    "virt=0x%llx non-canonical leaf frame; skipping refDec",
                    state.owner_pid, state.owner_name != nullptr ? state.owner_name : "?", state.reason != nullptr ? state.reason : "?",
                    static_cast<void*>(state.pagemap), frame.level, INDEX, static_cast<unsigned long long>(ENTRY_VADDR),
                    static_cast<unsigned long long>(entry.frame), static_cast<unsigned long long>(PHYS_ADDR),
                    static_cast<unsigned long long>(VIRT_RAW));
            }
        } else if (IS_TREE_PAGE_TABLE_FRAME) {
            note_destroy_page_table_alias_skip(&stats);
        } else if (LIVE_ALLOC_KIND == LiveAllocatorFrameKind::SLAB) {
            note_destroy_kind_skip(&stats, PageKind::SLAB);
            log::warn(
                "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx "
                "phys=0x%llx hits live slab; skipping refDec",
                state.owner_pid, state.owner_name != nullptr ? state.owner_name : "?", state.reason != nullptr ? state.reason : "?",
                static_cast<void*>(state.pagemap), frame.level, INDEX, static_cast<unsigned long long>(ENTRY_VADDR),
                static_cast<unsigned long long>(entry.frame), static_cast<unsigned long long>(PHYS_ADDR));
            debug_log_user_phys_mappings(PHYS_ADDR, state.reason, state.owner_pid, state.owner_name, true);
        } else if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
            note_destroy_kind_skip(&stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
        } else if (!IS_TREE_PAGE_TABLE_FRAME) {
            note_destroy_kind_skip(&stats, KIND);
        }
        entry = paging::purge_page_table_entry();
        return true;
    }

    destroy_state_flush_refdec_batch(state, &stats);
    destroy_state_reset_walk(state, DestroyUserSpacePhase::FREE_PAGE_TABLES);
    return false;
}

auto advance_free_page_tables_budgeted(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats& stats) -> bool {
    while (state.stack_size > 0) {
        auto& frame = destroy_state_top(state);
#ifdef WOS_SELFTEST
        selftest_require_user_page_table(state.pagemap, frame.table, frame.level, "budget-free-tables", state.owner_pid, state.reason);
#endif
        size_t const MAX_ENTRY = frame.level == 4 ? 256 : 512;
        if (frame.next_index >= MAX_ENTRY) {
            PageTable* table = frame.table;
            uint64_t const PHYS_ADDR = frame.phys_addr;
            int const LEVEL = static_cast<int>(static_cast<uint8_t>(frame.level));
            destroy_state_pop(state);
            if (LEVEL < 4 && PHYS_ADDR != 0) {
                destroy_state_queue_page_table_refdec(state, table, stats);
                return true;
            }
            continue;
        }

        size_t const INDEX = frame.next_index++;
        auto& entry = entry_at(frame.table, INDEX);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        int const LEVEL_SHIFT = 12 + (9 * (frame.level - 1));
        uint64_t const ENTRY_VADDR = frame.vaddr_base | (static_cast<uint64_t>(INDEX) << LEVEL_SHIFT);

        PageKind const KIND = frame.level > 1 ? frame_page_kind(PHYS_ADDR, &state.frame_probe_cache) : PageKind::UNKNOWN;
        LiveAllocatorFrameKind const LIVE_ALLOC_KIND =
            frame.level > 1 ? classify_live_allocator_frame(PHYS_ADDR, KIND, &stats, &state.frame_probe_cache)
                            : LiveAllocatorFrameKind::NONE;
        if (frame.level > 1 && entry.pagesize == 0 && LIVE_ALLOC_KIND == LiveAllocatorFrameKind::NONE && KIND == PageKind::PAGE_TABLE) {
            uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
            if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                note_destroy_corrupt_skip(&stats);
                log::warn(
                    "corrupt PTE in free_page_table_pages: level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx virt=0x%llx, clearing",
                    frame.level, INDEX, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                    static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                entry = paging::purge_page_table_entry();
                return true;
            }

            auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
            entry = paging::purge_page_table_entry();
            (void)destroy_state_push(state, next_level, frame.level - 1, ENTRY_VADDR, PHYS_ADDR);
            return true;
        }

        if (frame.level > 1 && entry.pagesize != 0) {
            note_destroy_huge_skip(&stats);
        } else if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
            note_destroy_kind_skip(&stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
        } else if (frame.level > 1 && KIND != PageKind::PAGE_TABLE) {
            note_destroy_kind_skip(&stats, KIND);
        }
        entry = paging::purge_page_table_entry();
        return true;
    }

    destroy_state_flush_page_table_refdec_batch(state, &stats);
    state.phase = DestroyUserSpacePhase::TLB_FLUSH;
    return false;
}

constexpr bool ENABLE_WATCHED_MMAP_LOGS = false;
constexpr uint64_t WATCH_MMAP_VADDR = 0x00001000007da000ULL;

auto is_watched_mmap_vaddr(uint64_t vaddr) -> bool { return ENABLE_WATCHED_MMAP_LOGS && vaddr == WATCH_MMAP_VADDR; }

auto collect_user_vaddrs_for_phys(PageTable* table, int level, uint64_t vaddr_base, uint64_t target_phys, uint64_t* out_vaddrs,
                                  size_t max_hits) -> size_t {
    if (table == nullptr || level < 1 || max_hits == 0) {
        return 0;
    }

    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));
    size_t hits = 0;

    for (size_t i = 0; i < MAX_ENTRY && hits < max_hits; ++i) {
        const auto& entry = entry_at(table, i);
        if (!entry.present) {
            continue;
        }

        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);
        const uint64_t ENTRY_PHYS = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (ENTRY_PHYS == 0) {
            continue;
        }

        if (level == 1) {
            if ((ENTRY_PHYS & ~(paging::PAGE_SIZE - 1)) == target_phys) {
                out_vaddrs[hits++] = ENTRY_VADDR;
            }
            continue;
        }

        if (entry.pagesize != 0) {
            continue;
        }

        auto* next_level = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(ENTRY_PHYS));
        if (next_level == nullptr) {
            continue;
        }

        hits += collect_user_vaddrs_for_phys(next_level, level - 1, ENTRY_VADDR, target_phys, out_vaddrs + hits, max_hits - hits);
    }

    return hits;
}

auto mark_phys_rmap_reported(uint64_t target_phys) -> bool {
    static mod::sys::Spinlock lock;
    static std::array<uint64_t, 64> reported_phys = {};

    target_phys &= ~(paging::PAGE_SIZE - 1);
    const uint64_t FLAGS = lock.lock_irqsave();
    for (uint64_t const PHYS : reported_phys) {
        if (PHYS == target_phys) {
            lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (auto& phys : reported_phys) {
        if (phys == 0) {
            phys = target_phys;
            lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    reported_phys.at(target_phys % reported_phys.size()) = target_phys;
    lock.unlock_irqrestore(FLAGS);
    return true;
}

auto log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid, const char* owner_name, bool log_when_empty)
    -> bool {
    if (!mark_phys_rmap_reported(target_phys)) {
        return false;
    }

    constexpr size_t MAX_HITS_PER_TASK = 4;
    constexpr size_t MAX_LOGGED_TASKS = 12;
    uint32_t const ACTIVE_COUNT = sched::get_active_task_count();
    size_t logged_tasks = 0;
    size_t total_hits = 0;

    log::warn("phys-rmap: scanning tasks for phys=0x%llx trigger=%s pid=%lu name=%s", static_cast<unsigned long long>(target_phys),
              trigger != nullptr ? trigger : "?", owner_pid, owner_name != nullptr ? owner_name : "?");

    for (uint32_t i = 0; i < ACTIVE_COUNT && logged_tasks < MAX_LOGGED_TASKS; ++i) {
        auto* task = sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }

        if (task->pagemap != nullptr && task->type != sched::task::TaskType::DAEMON) {
            std::array<uint64_t, MAX_HITS_PER_TASK> hits{};
            const size_t FOUND =
                collect_user_vaddrs_for_phys(task->pagemap, 4, 0, target_phys & ~(paging::PAGE_SIZE - 1), hits.data(), hits.size());
            if (FOUND > 0) {
                total_hits += FOUND;
                ++logged_tasks;
                log::warn("phys-rmap: phys=0x%llx pid=%lu name=%s pagemap=%p hit0=0x%llx hit1=0x%llx hit2=0x%llx hit3=0x%llx",
                          static_cast<unsigned long long>(target_phys), task->pid, task->name != nullptr ? task->name : "?",
                          static_cast<void*>(task->pagemap), static_cast<unsigned long long>(hits.at(0)),
                          static_cast<unsigned long long>(hits.at(1)), static_cast<unsigned long long>(hits.at(2)),
                          static_cast<unsigned long long>(hits.at(3)));
            }
        }

        task->release();
    }

    for (uint64_t cpu_no = 0; cpu_no < smt::get_core_count() && logged_tasks < MAX_LOGGED_TASKS; ++cpu_no) {
        const size_t DEAD_COUNT = sched::get_dead_task_count(cpu_no);
        for (size_t idx = 0; idx < DEAD_COUNT && logged_tasks < MAX_LOGGED_TASKS; ++idx) {
            auto* task = sched::get_dead_task_at_safe(cpu_no, idx);
            if (task == nullptr) {
                continue;
            }

            if (task->pagemap != nullptr && task->type != sched::task::TaskType::DAEMON) {
                std::array<uint64_t, MAX_HITS_PER_TASK> hits{};
                const size_t FOUND =
                    collect_user_vaddrs_for_phys(task->pagemap, 4, 0, target_phys & ~(paging::PAGE_SIZE - 1), hits.data(), hits.size());
                if (FOUND > 0) {
                    total_hits += FOUND;
                    ++logged_tasks;
                    log::warn(
                        "phys-rmap: phys=0x%llx dead_cpu=%llu pid=%lu name=%s pagemap=%p hit0=0x%llx hit1=0x%llx hit2=0x%llx hit3=0x%llx",
                        static_cast<unsigned long long>(target_phys), static_cast<unsigned long long>(cpu_no), task->pid,
                        task->name != nullptr ? task->name : "?", static_cast<void*>(task->pagemap),
                        static_cast<unsigned long long>(hits.at(0)), static_cast<unsigned long long>(hits.at(1)),
                        static_cast<unsigned long long>(hits.at(2)), static_cast<unsigned long long>(hits.at(3)));
                }
            }

            task->release();
        }
    }

    if (logged_tasks == 0) {
        if (log_when_empty) {
            log::warn("phys-rmap: phys=0x%llx has no other active or dead user mappings", static_cast<unsigned long long>(target_phys));
        }
        return false;
    }

    log::warn("phys-rmap: phys=0x%llx logged_tasks=%zu total_hits=%zu", static_cast<unsigned long long>(target_phys), logged_tasks,
              total_hits);
    return true;
}

}  // namespace

bool debug_log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid, const char* owner_name,
                                  bool log_when_empty) {
    return log_user_phys_mappings(target_phys, trigger, owner_pid, owner_name, log_when_empty);
}

namespace {

void free_user_data_pages(PageTable* table, int level, PageTable* root, const PageTableFrameSet& page_table_frames,
                          DestroyRefdecBatch& refdec_batch, DestroyUserSpaceCallStats* stats, uint64_t vaddr_base = 0,
                          uint64_t owner_pid = 0, const char* owner_name = nullptr, const char* reason = nullptr,
                          FrameProbeCache* frame_probe_cache = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

#ifdef WOS_SELFTEST
    selftest_require_user_page_table(root, table, level, "free-data", owner_pid, reason);
#endif

    // For user space, only process entries 0-255 at PML4 level
    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;

    // Number of VA bits contributed by this level's index field
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));

    for (size_t i = 0; i < MAX_ENTRY; i++) {
        auto& entry = entry_at(table, i);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        // Reconstruct virtual address for diagnostic output.
        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);

        if (level > 1) {
            // Check for huge pages (2MB at level 2, 1GB at level 3)
            if (entry.pagesize != 0) {
                // Preserve existing behavior: huge user mappings are not currently reclaimed here.
                note_destroy_huge_skip(stats);
            } else {
                PageKind const KIND = frame_page_kind(PHYS_ADDR, frame_probe_cache);
                LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, stats, frame_probe_cache);
                if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
                    note_destroy_kind_skip(stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
                    entry = paging::purge_page_table_entry();
                } else if (KIND != PageKind::PAGE_TABLE) {
                    note_destroy_kind_skip(stats, KIND);
                    entry = paging::purge_page_table_entry();
                } else {
                    uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
                    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                        note_destroy_corrupt_skip(stats);
                        log::warn(
                            "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx "
                            "phys=0x%llx "
                            "virt=0x%llx "
                            "corrupt non-canonical next-level pointer",
                            owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root),
                            level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                            static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                        entry = paging::purge_page_table_entry();
                        continue;
                    }
                    auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
                    free_user_data_pages(next_level, level - 1, root, page_table_frames, refdec_batch, stats, ENTRY_VADDR, owner_pid,
                                         owner_name, reason, frame_probe_cache);
                }
            }
        } else {
            // Level 1 (PML1) - entries point to actual data pages
            if (stats != nullptr) {
                stats->data_leaf_entries_visited++;
            }
            //
            // Some corrupted/recursive user mappings can point back at this
            // address space's own page-table frames. Freeing such a frame as
            // data lets another CPU reuse it before the parent page-table
            // cleanup reaches the same frame, which turns into a page_free()
            // of a live kmalloc page. Keep page-table frames owned by the
            // page-table cleanup phase below.
            PageKind const KIND = frame_page_kind(PHYS_ADDR, frame_probe_cache);
            bool const IS_TREE_PAGE_TABLE_FRAME = page_table_frames.contains(root, PHYS_ADDR, frame_probe_cache);
            LiveAllocatorFrameKind const LIVE_ALLOC_KIND = classify_live_allocator_frame(PHYS_ADDR, KIND, stats, frame_probe_cache);
            bool const IS_NORMAL_DATA = KIND == PageKind::NORMAL;
            if (!IS_TREE_PAGE_TABLE_FRAME && IS_NORMAL_DATA) {
                uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
                if ((VIRT_RAW >> CANONICAL_SIGN_BIT) == CANONICAL_KERNEL_UPPER) {
                    void* page_virt = reinterpret_cast<void*>(VIRT_RAW);
                    entry = paging::purge_page_table_entry();
                    destroy_refdec_batch_queue(refdec_batch, page_virt, stats, DestroyRefdecBatchKind::DATA);
                } else {
                    note_destroy_corrupt_skip(stats);
                    log::warn(
                        "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx "
                        "virt=0x%llx "
                        "non-canonical leaf frame; skipping refDec",
                        owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root),
                        level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                        static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                }
            } else if (IS_TREE_PAGE_TABLE_FRAME) {
                note_destroy_page_table_alias_skip(stats);
            } else if (LIVE_ALLOC_KIND == LiveAllocatorFrameKind::SLAB) {
                note_destroy_kind_skip(stats, PageKind::SLAB);
                log::warn(
                    "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx "
                    "phys=0x%llx hits live slab; skipping refDec",
                    owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root), level,
                    i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                    static_cast<unsigned long long>(PHYS_ADDR));
                log_user_phys_mappings(PHYS_ADDR, reason, owner_pid, owner_name, true);
            } else if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
                note_destroy_kind_skip(stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
            } else if (!IS_TREE_PAGE_TABLE_FRAME) {
                note_destroy_kind_skip(stats, KIND);
            }
            entry = paging::purge_page_table_entry();
        }
    }
}

// Helper to free a page table level recursively after user data pages are gone.
// level: 4=PML4, 3=PML3, 2=PML2, 1=PML1
// vaddr_base: accumulated virtual address bits from outer levels (for diagnostics)
void free_page_table_pages(PageTable* table, int level, DestroyRefdecBatch& page_table_refdec_batch, DestroyUserSpaceCallStats* stats,
                           uint64_t vaddr_base = 0, FrameProbeCache* frame_probe_cache = nullptr, PageTable* root = nullptr,
                           uint64_t owner_pid = 0, const char* reason = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

#ifdef WOS_SELFTEST
    selftest_require_user_page_table(root != nullptr ? root : table, table, level, "free-tables", owner_pid, reason);
#endif

    // For user space, only process entries 0-255 at PML4 level
    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;

    // Number of VA bits contributed by this level's index field
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));

    for (size_t i = 0; i < MAX_ENTRY; i++) {
        auto& entry = entry_at(table, i);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        // Reconstruct virtual address for diagnostic output.
        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);

        PageKind const KIND = level > 1 ? frame_page_kind(PHYS_ADDR, frame_probe_cache) : PageKind::UNKNOWN;
        LiveAllocatorFrameKind const LIVE_ALLOC_KIND =
            level > 1 ? classify_live_allocator_frame(PHYS_ADDR, KIND, stats, frame_probe_cache) : LiveAllocatorFrameKind::NONE;
        if (level > 1 && entry.pagesize == 0 && LIVE_ALLOC_KIND == LiveAllocatorFrameKind::NONE && KIND == PageKind::PAGE_TABLE) {
            uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
            if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                note_destroy_corrupt_skip(stats);
                log::warn(
                    "corrupt PTE in free_page_table_pages: level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx virt=0x%llx, "
                    "clearing",
                    level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                    static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                entry = paging::purge_page_table_entry();
                continue;
            }
            auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
            entry = paging::purge_page_table_entry();
            free_page_table_pages(next_level, level - 1, page_table_refdec_batch, stats, ENTRY_VADDR, frame_probe_cache,
                                  root != nullptr ? root : table, owner_pid, reason);
            destroy_refdec_batch_queue(page_table_refdec_batch, next_level, stats, DestroyRefdecBatchKind::PAGE_TABLE);
        } else if (level > 1 && entry.pagesize != 0) {
            note_destroy_huge_skip(stats);
        } else if (LIVE_ALLOC_KIND != LiveAllocatorFrameKind::NONE) {
            note_destroy_kind_skip(stats, live_allocator_kind_to_page_kind(LIVE_ALLOC_KIND));
        } else if (level > 1 && KIND != PageKind::PAGE_TABLE) {
            note_destroy_kind_skip(stats, KIND);
        }

        entry = paging::purge_page_table_entry();
    }
}

}  // namespace

auto prepare_destroy_user_space_budget_state() -> DestroyUserSpaceBudgetState* { return new (std::nothrow) DestroyUserSpaceBudgetState{}; }

auto begin_prepared_destroy_user_space(DestroyUserSpaceBudgetState& state, PageTable* pagemap, uint64_t owner_pid, const char* owner_name,
                                       const char* reason, bool wait_for_gate) -> bool {
    if (pagemap == nullptr || state.pagemap != nullptr || state.access_gate_index != USER_PAGEMAP_ACCESS_GATE_NONE) return false;
    size_t const GATE = wait_for_gate ? begin_user_pagemap_exclusive(pagemap) : try_begin_user_pagemap_exclusive(pagemap);
    if (GATE == USER_PAGEMAP_ACCESS_GATE_NONE) return false;
    state.access_gate_index = GATE;
    state.page_table_frames.allow_growth = wait_for_gate;
    purge_anonymous_swap_mappings(pagemap);
    owned_frame_purge_pagemap(pagemap);
    state.pagemap = pagemap;
    state.refdec_batch.root = pagemap;
    state.page_table_refdec_batch.root = pagemap;
    state.owner_pid = owner_pid;
    state.owner_name = owner_name;
    state.reason = reason;
    state.phase = DestroyUserSpacePhase::COLLECT_FRAMES;
    bool const ROOT_ADDED =
        state.page_table_frames.add(reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(pagemap))));
    if (!ROOT_ADDED && !state.page_table_frames.allow_growth)
        dbg::panic_handler("native root seed exceeded inline table census before mutation");
    destroy_state_reset_walk(state, DestroyUserSpacePhase::COLLECT_FRAMES);
    return true;
}

auto create_destroy_user_space_budget_state(PageTable* pagemap, uint64_t owner_pid, const char* owner_name, const char* reason)
    -> DestroyUserSpaceBudgetState* {
    if (pagemap == nullptr) return nullptr;
    auto* state = prepare_destroy_user_space_budget_state();
    if (state == nullptr) return nullptr;
    if (!begin_prepared_destroy_user_space(*state, pagemap, owner_pid, owner_name, reason)) {
        delete state;
        return nullptr;
    }
    return state;
}

auto destroy_user_space_budgeted(DestroyUserSpaceBudgetState* state, uint32_t max_steps) -> bool {
    if (state == nullptr) {
        return true;
    }
    if (state->phase == DestroyUserSpacePhase::DONE) {
        return true;
    }
    if (max_steps == 0) {
        return false;
    }

    DestroyUserSpaceCallStats stats{};
    DestroyUserSpacePhase phase = state->phase;
    uint64_t phase_started_us = time::get_us();
    uint32_t steps = 0;

    while (steps < max_steps && state->phase != DestroyUserSpacePhase::DONE) {
        if (state->phase != phase) {
            uint64_t const NOW_US = time::get_us();
            note_destroy_user_space_phase_time(stats, phase, elapsed_us_since(phase_started_us, NOW_US));
            phase = state->phase;
            phase_started_us = NOW_US;
        }

        bool consumed_step = false;
        switch (state->phase) {
            case DestroyUserSpacePhase::COLLECT_FRAMES:
                consumed_step = advance_collect_frames_budgeted(*state, stats);
                break;
            case DestroyUserSpacePhase::FREE_DATA:
                consumed_step = advance_free_data_budgeted(*state, stats);
                break;
            case DestroyUserSpacePhase::FREE_PAGE_TABLES:
                consumed_step = advance_free_page_tables_budgeted(*state, stats);
                break;
            case DestroyUserSpacePhase::TLB_FLUSH:
                flush_pagemap_after_update(state->pagemap, 0, true);
                {
                    uint64_t const FLAGS = user_mapping_pin_lock.lock_irqsave();
                    auto* control = find_user_root_control_locked(state->pagemap);
                    bool const DETACHED =
                        control == nullptr ? file_mapping_index.root_empty_locked(state->pagemap)
                                           : file_mapping_index.detach_root_locked(state->pagemap, control->serial, state->file_retirement);
                    user_mapping_pin_lock.unlock_irqrestore(FLAGS);
                    if (!DETACHED) dbg::panic_handler("native binding root retirement mismatch");
                }
                state->phase = DestroyUserSpacePhase::DONE;
                consumed_step = true;
                break;
            case DestroyUserSpacePhase::DONE:
                break;
        }

        if (consumed_step) {
            steps++;
        }
    }

    destroy_state_flush_refdec_batch(*state, &stats);
    destroy_state_flush_page_table_refdec_batch(*state, &stats);
    note_destroy_user_space_phase_time(stats, phase, elapsed_us_since(phase_started_us, time::get_us()));
    note_destroy_user_space_stats(stats);
    return state->phase == DestroyUserSpacePhase::DONE;
}

void destroy_user_space_budget_state_destroy(DestroyUserSpaceBudgetState* state) {
    if (state == nullptr) {
        return;
    }
    destroy_state_flush_refdec_batch(*state, nullptr);
    destroy_state_flush_page_table_refdec_batch(*state, nullptr);
    if (state->access_gate_index != USER_PAGEMAP_ACCESS_GATE_NONE) end_user_pagemap_exclusive(state->access_gate_index);
    state->access_gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    state->file_retirement.release_after_completed_tlb();
    delete state;
}

auto finish_destroy_user_space_and_release_pagemap(DestroyUserSpaceBudgetState* state) -> bool {
    if (state == nullptr || state->phase != DestroyUserSpacePhase::DONE || state->pagemap == nullptr || state->pagemap == kernel_pagemap ||
        is_current_pagemap(state->pagemap) || state->access_gate_index == USER_PAGEMAP_ACCESS_GATE_NONE ||
        state->access_gate_index != user_pagemap_access_gate_index(state->pagemap) ||
        !user_pagemap_access_gates.at(state->access_gate_index).teardown.load(std::memory_order_acquire))
        return false;
    destroy_state_flush_refdec_batch(*state, nullptr);
    destroy_state_flush_page_table_refdec_batch(*state, nullptr);
    // DONE already completed the actual TLB ACK and detached the exact binding
    // batch. Keep the original exclusive lease through control/pool retirement.
    release_pagemap_exclusive_owned(state->pagemap, state->owner_pid, __builtin_return_address(0));
    state->pagemap = nullptr;
    end_user_pagemap_exclusive(state->access_gate_index);
    state->access_gate_index = USER_PAGEMAP_ACCESS_GATE_NONE;
    state->file_retirement.release_after_completed_tlb();
    delete state;
    return true;
}

void destroy_user_space(PageTable* pagemap, uint64_t owner_pid, const char* owner_name, const char* reason) {
    if (pagemap == nullptr) {
        return;
    }
    UserPagemapExclusiveScope teardown_scope(pagemap);
    purge_anonymous_swap_mappings(pagemap);
    owned_frame_purge_pagemap(pagemap);
    DestroyUserSpaceCallStats stats{};
#ifdef ELF_DEBUG
    if (owner_pid != 0) {
        log::trace("destroyUserSpace: begin pid=%lu name=%s reason=%s pagemap=%p", owner_pid, owner_name != nullptr ? owner_name : "?",
                   reason != nullptr ? reason : "?", static_cast<void*>(pagemap));
    }
#endif
    // Free all user-space mappings (PML4 entries 0-255). Keep data-page
    // reclamation separate from page-table-page reclamation so a user PTE that
    // aliases a page-table frame cannot free that frame before the page-table
    // walk reaches it.
    PageTableFrameSet page_table_frames{};
    FrameProbeCache frame_probe_cache{};
    uint64_t phase_start_us = time::get_us();
    collect_page_table_frames(pagemap, 4, page_table_frames, &stats, &frame_probe_cache, pagemap, owner_pid, reason);
    stats.collect_frames_us = elapsed_us_since(phase_start_us, time::get_us());

    phase_start_us = time::get_us();
    DestroyRefdecBatch refdec_batch{.root = pagemap};
    free_user_data_pages(pagemap, 4, pagemap, page_table_frames, refdec_batch, &stats, 0, owner_pid, owner_name, reason,
                         &frame_probe_cache);
    destroy_refdec_batch_flush(refdec_batch, &stats, DestroyRefdecBatchKind::DATA);
    stats.free_data_us = elapsed_us_since(phase_start_us, time::get_us());

    phase_start_us = time::get_us();
    DestroyRefdecBatch page_table_refdec_batch{.root = pagemap};
    free_page_table_pages(pagemap, 4, page_table_refdec_batch, &stats, 0, &frame_probe_cache, pagemap, owner_pid, reason);
    destroy_refdec_batch_flush(page_table_refdec_batch, &stats, DestroyRefdecBatchKind::PAGE_TABLE);
    stats.free_pt_us = elapsed_us_since(phase_start_us, time::get_us());

    // Invalidate TLB for this address space
    phase_start_us = time::get_us();
    flush_pagemap_after_update(pagemap, 0, true);
    stats.tlb_flush_us = elapsed_us_since(phase_start_us, time::get_us());
    file_mapping::FileMappingTlbRetirement file_retirement;
    uint64_t const FILE_FLAGS = user_mapping_pin_lock.lock_irqsave();
    auto* control = find_user_root_control_locked(pagemap);
    bool const FILE_DETACHED = control == nullptr ? file_mapping_index.root_empty_locked(pagemap)
                                                  : file_mapping_index.detach_root_locked(pagemap, control->serial, file_retirement);
    user_mapping_pin_lock.unlock_irqrestore(FILE_FLAGS);
    if (!FILE_DETACHED) dbg::panic_handler("native binding root retirement mismatch");
    teardown_scope.finish();
    file_retirement.release_after_completed_tlb();
    note_destroy_user_space_stats(stats);
#ifdef ELF_DEBUG
    if (owner_pid != 0) {
        log::trace("destroyUserSpace: end pid=%lu reason=%s pagemap=%p", owner_pid, reason != nullptr ? reason : "?",
                   static_cast<void*>(pagemap));
    }
#endif
}

auto get_destroy_user_space_stats(uint64_t cpu_no) -> DestroyUserSpaceStats {
    if (cpu_no >= g_destroy_user_space_stats.size()) {
        return {};
    }

    auto const& stats = g_destroy_user_space_stats[static_cast<size_t>(cpu_no)];
    return {
        .calls = stats.calls.load(std::memory_order_relaxed),
        .collect_frames_us_total = stats.collect_frames_us_total.load(std::memory_order_relaxed),
        .collect_frames_us_max = stats.collect_frames_us_max.load(std::memory_order_relaxed),
        .free_data_us_total = stats.free_data_us_total.load(std::memory_order_relaxed),
        .free_data_us_max = stats.free_data_us_max.load(std::memory_order_relaxed),
        .free_pt_us_total = stats.free_pt_us_total.load(std::memory_order_relaxed),
        .free_pt_us_max = stats.free_pt_us_max.load(std::memory_order_relaxed),
        .tlb_flush_us_total = stats.tlb_flush_us_total.load(std::memory_order_relaxed),
        .tlb_flush_us_max = stats.tlb_flush_us_max.load(std::memory_order_relaxed),
        .data_leaf_entries_visited = stats.data_leaf_entries_visited.load(std::memory_order_relaxed),
        .data_pages_ref_decremented = stats.data_pages_ref_decremented.load(std::memory_order_relaxed),
        .data_pages_freed = stats.data_pages_freed.load(std::memory_order_relaxed),
        .page_table_pages_ref_decremented = stats.page_table_pages_ref_decremented.load(std::memory_order_relaxed),
        .page_table_pages_freed = stats.page_table_pages_freed.load(std::memory_order_relaxed),
        .skipped_huge_pages = stats.skipped_huge_pages.load(std::memory_order_relaxed),
        .skipped_unknown_frames = stats.skipped_unknown_frames.load(std::memory_order_relaxed),
        .skipped_slab_alloc_frames = stats.skipped_slab_alloc_frames.load(std::memory_order_relaxed),
        .skipped_medium_alloc_frames = stats.skipped_medium_alloc_frames.load(std::memory_order_relaxed),
        .skipped_kmalloc_large_alloc_frames = stats.skipped_kmalloc_large_alloc_frames.load(std::memory_order_relaxed),
        .skipped_page_table_aliases = stats.skipped_page_table_aliases.load(std::memory_order_relaxed),
        .skipped_corrupt_entries = stats.skipped_corrupt_entries.load(std::memory_order_relaxed),
        .magic_unknown_probe_reads = stats.magic_unknown_probe_reads.load(std::memory_order_relaxed),
        .magic_unknown_slab_hits = stats.magic_unknown_slab_hits.load(std::memory_order_relaxed),
        .magic_unknown_medium_hits = stats.magic_unknown_medium_hits.load(std::memory_order_relaxed),
        .magic_unknown_kmalloc_large_hits = stats.magic_unknown_kmalloc_large_hits.load(std::memory_order_relaxed),
    };
}

auto deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst) -> bool {
    // Walk the user half (PML4[0..255]) of src.
    // For each present PML1 entry pointing to a data page:
    //   1. Mark the src PTE as read-only + COW
    //   2. Create the same PTE in dst (read-only + COW)
    //   3. Increment the physical page's refcount
    // Page table pages (PML3/PML2/PML1) are freshly allocated for dst.

#ifdef WOS_SELFTEST
    selftest_require_mutable_pagemap_root(src, "fork-cow-source");
    selftest_require_mutable_pagemap_root(dst, "fork-cow-destination");
#endif

    // Fork mutates writable source leaves into COW mappings. Drain every
    // stable usercopy pin on this shared root first and prevent new pins until
    // the source PTE changes and final TLB shootdown are complete. Otherwise a
    // copyout pinned as writable before fork could modify the child's newly
    // shared frame after publication.
    UserPagemapExclusiveScope source_usercopy_scope(src);

    constexpr size_t USER_PML4_ENTRIES = 256;
    bool const EAGER_COPY_WRITABLE_PRIVATE = fork_eager_copy_enabled();
    phys::PageLookupHint ref_lookup{};
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; i4++) {
        auto& src_pml4e = entry_at(src, i4);
        auto& dst_pml4e = entry_at(dst, i4);
        if (!src_pml4e.present) {
            continue;
        }

        paddr_t const SRC_PML3_PHYS = src_pml4e.frame << paging::PAGE_SHIFT;
        auto* src_pml3 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML3_PHYS));

        // Allocate a new PML3 for dst
        auto* dst_pml3 = alloc_zeroed_page_table();
        if (dst_pml3 == nullptr) {
            return false;
        }

        // Set PML4 entry in dst (copy flags from src)
        dst_pml4e = src_pml4e;
        dst_pml4e.frame = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml3))) >> paging::PAGE_SHIFT;

        for (size_t i3 = 0; i3 < 512; i3++) {
            auto& src_pml3e = entry_at(src_pml3, i3);
            auto& dst_pml3e = entry_at(dst_pml3, i3);
            if (!src_pml3e.present) {
                continue;
            }

            paddr_t const SRC_PML2_PHYS = src_pml3e.frame << paging::PAGE_SHIFT;
            auto* src_pml2 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML2_PHYS));

            auto* dst_pml2 = alloc_zeroed_page_table();
            if (dst_pml2 == nullptr) {
                return false;
            }

            dst_pml3e = src_pml3e;
            dst_pml3e.frame = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml2))) >> paging::PAGE_SHIFT;
            constexpr size_t PML2_ENTRY_NUMBER = 512;
            for (size_t i2 = 0; i2 < PML2_ENTRY_NUMBER; i2++) {
                auto& src_pml2e = entry_at(src_pml2, i2);
                auto& dst_pml2e = entry_at(dst_pml2, i2);
                if (!src_pml2e.present) {
                    continue;
                }
                if (src_pml2e.pagesize) {
                    // 2MB huge page: copy as 512 independent 4KB pages into a new PML1.
                    // Cannot use pageAllocHuge because huge_page_base may not be 2MB-aligned;
                    // an unaligned PA in a PS=1 PML2 entry is invalid and causes a GPF.
                    // Each sub-page must be an independent pageAlloc so free_user_data_pages
                    // can page_ref_dec each PTE individually — a multi-page buddy block has only
                    // one head page and the CONT pages cannot be freed individually.
                    paddr_t const SRC_PHYS = src_pml2e.frame << paging::PAGE_SHIFT;
                    auto* src_virt = reinterpret_cast<uint8_t*>(addr::get_virt_pointer(SRC_PHYS));

                    auto* dst_pml1 = alloc_zeroed_page_table();
                    if (dst_pml1 == nullptr) {
                        return false;
                    }

                    // Derive per-PTE flags from the source PML2 huge-page entry:
                    // clear PS bit (bit 7) and frame bits; preserve present/write/user/NX.
                    constexpr uint64_t PS_BIT = (1ULL << 7);
                    constexpr uint64_t FRAME_MASK = 0x000FFFFFFFFFF000ULL;
                    uint64_t const PDE_RAW_VAL = pte_raw(src_pml2e);
                    uint64_t const PTE_FLAGS = PDE_RAW_VAL & ~(FRAME_MASK | PS_BIT);

                    for (size_t i1 = 0; i1 < PML2_ENTRY_NUMBER; i1++) {
                        auto* sub = phys::page_alloc_with_reclaim_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, paging::PAGE_SIZE,
                                                                           "cow_huge_sub");
                        if (sub == nullptr) {
                            // OOM: free already-allocated sub-pages and pml1
                            for (size_t j = 0; j < i1; j++) {
                                paddr_t const SUB_PA = static_cast<paddr_t>(entry_at(dst_pml1, j).frame) << paging::PAGE_SHIFT;
                                uint64_t const SUB_VADDR = (static_cast<uint64_t>(i4) << 39) | (static_cast<uint64_t>(i3) << 30) |
                                                           (static_cast<uint64_t>(i2) << 21) | (static_cast<uint64_t>(j) << 12);
                                owned_frame_untrack_mapping(dst, SUB_VADDR, SUB_PA);
                                phys::page_free(reinterpret_cast<void*>(addr::get_virt_pointer(SUB_PA)));
                            }
                            phys::page_free(dst_pml1);
                            return false;
                        }
                        std::memcpy(sub, src_virt + (i1 * paging::PAGE_SIZE), paging::PAGE_SIZE);
                        auto const SUB_PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(sub)));
                        entry_at(dst_pml1, i1) = pte_from_raw(PTE_FLAGS | SUB_PHYS);
                        uint64_t const SUB_VADDR = (static_cast<uint64_t>(i4) << 39) | (static_cast<uint64_t>(i3) << 30) |
                                                   (static_cast<uint64_t>(i2) << 21) | (static_cast<uint64_t>(i1) << 12);
                        owned_frame_track_fresh_normal_mapping(dst, SUB_VADDR, SUB_PHYS, PTE_FLAGS | paging::PAGE_PRESENT);
                    }

                    dst_pml2e = src_pml2e;
                    dst_pml2e.pagesize = 0;
                    dst_pml2e.frame =
                        reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml1))) >> paging::PAGE_SHIFT;
                    continue;
                }

                paddr_t const SRC_PML1_PHYS = src_pml2e.frame << paging::PAGE_SHIFT;
                auto* src_pml1 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML1_PHYS));

                auto* dst_pml1 = alloc_zeroed_page_table();
                if (dst_pml1 == nullptr) {
                    return false;
                }

                dst_pml2e = src_pml2e;
                dst_pml2e.frame =
                    reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml1))) >> paging::PAGE_SHIFT;
                constexpr size_t PML1_ENTRY_NUMBER = 512;
                for (size_t i1 = 0; i1 < PML1_ENTRY_NUMBER; i1++) {
                    auto& src_pml1e = entry_at(src_pml1, i1);
                    auto& dst_pml1e = entry_at(dst_pml1, i1);
                    uint64_t raw = pte_raw(src_pml1e);
                    if (!src_pml1e.present) {
                        if ((raw & paging::PAGE_RESERVED) != 0U) {
                            dst_pml1e = src_pml1e;
                        }
                        continue;
                    }

                    const uint64_t VADDR = (static_cast<uint64_t>(i4) << 39) | (static_cast<uint64_t>(i3) << 30) |
                                           (static_cast<uint64_t>(i2) << 21) | (static_cast<uint64_t>(i1) << 12);
                    const bool WAS_WRITABLE = (raw & paging::PAGE_WRITE) != 0U;
                    const bool IS_COW = (raw & paging::PAGE_COW) != 0U;
                    const bool IS_SHARED = (raw & paging::PAGE_SHARED) != 0U;
                    const bool IS_WRITABLE_PRIVATE = !IS_SHARED && (WAS_WRITABLE || IS_COW);

                    paddr_t const DATA_PHYS = src_pml1e.frame << paging::PAGE_SHIFT;
                    void* data_virt = reinterpret_cast<void*>(addr::get_virt_pointer(DATA_PHYS));

                    if (EAGER_COPY_WRITABLE_PRIVATE && IS_WRITABLE_PRIVATE) {
                        auto* child_page = phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(
                            PhysicalPageOwner::USER_PRIVATE_MAPPING, "fork_copy");
                        if (child_page == nullptr) {
                            return false;
                        }
                        std::memcpy(child_page, data_virt, paging::PAGE_SIZE);
                        auto const CHILD_PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(child_page)));
                        uint64_t child_raw = raw;
                        child_raw &= ~paging::PAGE_COW;
                        child_raw |= paging::PAGE_WRITE;
                        dst_pml1e = pte_from_raw((child_raw & ~PTE_FRAME_MASK) | (CHILD_PHYS & PTE_FRAME_MASK));
                        owned_frame_track_fresh_normal_mapping(dst, VADDR, CHILD_PHYS, pte_raw(dst_pml1e));
                        continue;
                    }

                    // Acquire the child's actual owner before publishing its
                    // leaf (including readonly canonical shared mappings).
                    // Refusal must not leave an unowned PTE in fork rollback.
                    if (!phys::page_ref_try_inc(data_virt, &ref_lookup)) {
                        flush_pagemap_after_update(src, 0, true);
                        return false;
                    }

                    // Only writable mappings need COW. Shared read-only mappings
                    // (text/debug metadata) can stay read-only in both address
                    // spaces and simply share the backing page.
                    if (WAS_WRITABLE && !IS_SHARED) {
                        raw &= ~paging::PAGE_WRITE;
                        raw |= paging::PAGE_COW;
                        src_pml1e = pte_from_raw(raw);
                        dst_pml1e = pte_from_raw(raw);
                    } else {
                        dst_pml1e = src_pml1e;
                    }

                    owned_frame_untrack_mapping(src, VADDR, DATA_PHYS);
                    if (is_watched_mmap_vaddr(VADDR)) {
                        log::warn("watch mmap-cow: src=%p dst=%p vaddr=0x%llx phys=0x%llx writable=%u cow=%u ref=%llu",
                                  static_cast<void*>(src), static_cast<void*>(dst), static_cast<unsigned long long>(VADDR),
                                  static_cast<unsigned long long>(DATA_PHYS), WAS_WRITABLE ? 1U : 0U,
                                  (raw & paging::PAGE_COW) != 0U ? 1U : 0U,
                                  static_cast<unsigned long long>(phys::page_ref_get(data_virt, &ref_lookup)));
                    }
                }
            }
        }
    }

    // The source lifetime gate also joins queued/in-flight swap work. Clone
    // stable side-table leaves only after the ordinary page-table walk has
    // created the child's reserved marker entries.
    if (!clone_anonymous_swap_mappings(src, dst)) {
        return false;
    }

    // Flush TLB for the source (parent) since we modified its PTEs.
    flush_pagemap_after_update(src, 0, true);
    return true;
}

}  // namespace ker::mod::mm::virt

// Managed-huge foundation adapters. No PTE publisher or user opt-in exists.
namespace ker::mod::mm::huge::detail {
auto root_key(paging::PageTable* root, RootKey& empty) noexcept -> Error {
    if (root == nullptr || root == virt::kernel_pagemap || empty.address_bits != 0 || empty.incarnation != 0) return Error::Invalid;
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    auto const* control = virt::find_user_root_control_locked(root);
    Error const result = control != nullptr && control->serial != 0 ? Error::None : Error::Stale;
    if (result == Error::None) empty = {reinterpret_cast<uint64_t>(root), control->serial};
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
auto root_matches(RootKey key) noexcept -> bool {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    auto const* control = virt::find_user_root_control_locked(reinterpret_cast<paging::PageTable*>(key.address_bits));
    bool const matches = control != nullptr && control->serial != 0 && control->serial == key.incarnation;
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return matches;
}
auto initialize_caps(const CapsInput& input) noexcept -> Error {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    Error const result = virt::managed_huge_registry.initialize(input);
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
auto reserve_leaf(RootKey key, uint64_t va, uint16_t node, LeafHandle& empty) noexcept -> Error {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    auto const* control = virt::find_user_root_control_locked(reinterpret_cast<paging::PageTable*>(key.address_bits));
    Error const result = control != nullptr && control->serial != 0 && control->serial == key.incarnation
                             ? virt::managed_huge_registry.reserve_leaf(key, va, node, empty)
                             : Error::Stale;
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
auto cancel_leaf(LeafHandle handle) noexcept -> Error {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    Error const result = virt::managed_huge_registry.cancel_preparing(handle);
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
auto reserve_plan(uint16_t pages, PlanHandle& empty) noexcept -> Error {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    Error const result = virt::managed_huge_registry.reserve_table_plan(pages, empty);
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
auto release_plan(PlanHandle handle) noexcept -> Error {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    Error const result = virt::managed_huge_registry.release_table_plan(handle);
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    return result;
}
void snapshot(Snapshot& output) noexcept {
    uint64_t const flags = virt::user_mapping_pin_lock.lock_irqsave();
    virt::managed_huge_registry.snapshot(output);
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
}
auto allocate_table() noexcept -> paging::PageTable* { return virt::alloc_zeroed_page_table(true); }
void release_table(paging::PageTable* table) noexcept {
    if (table != nullptr && !virt::try_release_page_table_to_pool(table, {__builtin_return_address(0), 0})) phys::page_free(table);
}
}  // namespace ker::mod::mm::huge::detail
