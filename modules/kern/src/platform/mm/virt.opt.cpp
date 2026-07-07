#include "virt.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <platform/acpi/apic/apic.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <span>
#include <string_view>
#include <util/smallvec.hpp>
#include <utility>

#include "platform/sched/task.hpp"
#include "platform/sys/spinlock.hpp"

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "platform/asm/cpu.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/coredump.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sched/threading.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "util/hcf.hpp"
#include "vfs/vfs.hpp"

extern "C" char __kernel_end[];  // NOLINT(readability-identifier-naming)

namespace ker::mod::mm::virt {

namespace {
using log = ker::mod::dbg::logger<"virt">;

constexpr uint64_t LARGE_PAGE_2M_BYTES = paging::PAGE_SIZE * paging::PAGE_TABLE_ENTRIES;
constexpr uint64_t LARGE_PAGE_1G_BYTES = LARGE_PAGE_2M_BYTES * paging::PAGE_TABLE_ENTRIES;
constexpr uint64_t PTE_FRAME_MASK = 0x000FFFFFFFFFF000ULL;

sys::Spinlock cow_pte_lock;

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

auto handle_lazy_vmem_fault(sched::task::Task* task, uint64_t vaddr, const paging::PageFault& fault, uint64_t fault_rip = 0,
                            uint64_t fault_rsp = 0) -> bool {
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

        if (range.kind == sched::task::LazyVmemKind::FILE_BACKED) {
            auto file_range = range;
            if (file_range.file != nullptr) {
                ker::vfs::vfs_retain_file(file_range.file);
            }
            task->lazy_vmem_lock.unlock_irqrestore(IRQF);
            bool const OK = ker::syscall::vmem::materialize_lazy_file_page(task, file_range, PAGE_VADDR, fault);
            if (file_range.file != nullptr) {
                ker::vfs::vfs_put_file(file_range.file);
            }
            return OK;
        }

        void* const PAGE = phys::page_alloc(paging::PAGE_SIZE, "lazy-vmem");
        if (PAGE == nullptr) {
            task->lazy_vmem_lock.unlock_irqrestore(IRQF);
            log::error("lazy vmem fault: OOM pid=%lu vaddr=0x%llx", task->pid, static_cast<unsigned long long>(PAGE_VADDR));
            return false;
        }
        std::memset(PAGE, 0, paging::PAGE_SIZE);
        auto const PADDR = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(PAGE)));
        map_page(task->pagemap, PAGE_VADDR, PADDR, lazy_user_vmem_flags(range.prot));
        task->lazy_vmem_lock.unlock_irqrestore(IRQF);
        return true;
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);

    log_lazy_vmem_fault_state(task, PAGE_VADDR, fault, "miss", fault_rip, fault_rsp);
    return false;
}

auto writable_anonymous_range_contains(sched::task::Task* task, uint64_t vaddr) -> bool {
    if (task == nullptr) {
        return false;
    }

    constexpr uint64_t PROT_WRITE = 0x2ULL;
    constexpr uint64_t MAP_ANONYMOUS = 0x20ULL;
    uint64_t const PAGE_VADDR = vaddr & ~(paging::PAGE_SIZE - 1);
    bool contains = false;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (PAGE_VADDR >= range.start && PAGE_VADDR < range.end && (range.prot & PROT_WRITE) != 0U && (range.flags & MAP_ANONYMOUS) != 0U) {
            contains = true;
            break;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
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

auto alloc_cow_destination_page(bool full_overwrite) -> void* {
    void* const PAGE = full_overwrite ? phys::page_alloc_full_overwrite_page_with_reclaim("cow_copy")
                                      : phys::page_alloc_with_reclaim(paging::PAGE_SIZE, "cow_zero");
    if (PAGE != nullptr && !full_overwrite) {
        std::memset(PAGE, 0, paging::PAGE_SIZE);
    }
    return PAGE;
}

paging::PageTable* kernel_pagemap;
limine_memmap_response* memmap_response;
limine_executable_file_response* kernel_file_response;
limine_executable_address_response* kernel_address_response;

constexpr size_t PAGE_TABLE_POOL_CAPACITY = 64;

struct PageTablePool {
    mod::sys::Spinlock lock;
    std::array<PageTable*, PAGE_TABLE_POOL_CAPACITY> pages{};
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
constexpr uint64_t TLB_SHOOTDOWN_LOG_FIRST_US = 50'000;
constexpr uint64_t TLB_SHOOTDOWN_LOG_REPEAT_US = 1'000'000;

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
        if (request.targets[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire) == 0U) {
            continue;
        }

        uint64_t const GENERATION = request.generation.load(std::memory_order_acquire);
        if (GENERATION == 0) {
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

        PageTable* const TARGET_PAGEMAP = request.pagemap.load(std::memory_order_acquire);
        if (TARGET_PAGEMAP != nullptr) {
            invalidate_local_tlb_if_current(TARGET_PAGEMAP, request.vaddr.load(std::memory_order_acquire),
                                            request.reload_cr3.load(std::memory_order_acquire));
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
    uint64_t const START_US = time::get_us();
    uint64_t next_log_us = START_US + TLB_SHOOTDOWN_LOG_FIRST_US;
    uint64_t spins = 0;
    bool counted_contention = false;

    for (;;) {
        auto* const PREEMPT_OWNER = sched::preempt_disable_token_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
        uint64_t const CPU_NO = cpu::current_cpu();
        if (!cpu_slot_valid(CPU_NO)) {
            sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
            return {};
        }

        auto const OWNER_ID = static_cast<uint32_t>(CPU_NO + 1U);
        uint32_t expected = TLB_SHOOTDOWN_GATE_FREE;
        if (tlb_shootdown_gate_owner.compare_exchange_weak(expected, OWNER_ID, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return TlbShootdownGateGuard{.preempt_owner = PREEMPT_OWNER, .cpu_no = CPU_NO, .held = true};
        }

        service_tlb_shootdown_requests_for_cpu(CPU_NO);
        sched::preempt_enable_token_at(PREEMPT_OWNER, reinterpret_cast<uint64_t>(__builtin_return_address(0)));

        if (!counted_contention) {
            tlb_shootdown_gate_contentions.fetch_add(1, std::memory_order_relaxed);
            counted_contention = true;
        }

        ++spins;
        if ((spins & 0xFFFU) == 0U) {
            uint64_t const NOW_US = time::get_us();
            if (NOW_US >= next_log_us) {
                uint32_t const OWNER = tlb_shootdown_gate_owner.load(std::memory_order_acquire);
                log::warn("tlb shootdown gate wait: cpu=%llu owner=%u elapsed_us=%llu spins=%llu contentions=%llu",
                          static_cast<unsigned long long>(CPU_NO), OWNER, static_cast<unsigned long long>(NOW_US - START_US),
                          static_cast<unsigned long long>(spins),
                          static_cast<unsigned long long>(tlb_shootdown_gate_contentions.load(std::memory_order_relaxed)));
                next_log_us = NOW_US + TLB_SHOOTDOWN_LOG_REPEAT_US;
            }
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

void log_tlb_shootdown_wait(TlbShootdownRequest& request, uint64_t origin_cpu, uint64_t core_count, uint64_t generation,
                            uint64_t elapsed_us, uint64_t retries) {
    TlbShootdownWaitSnapshot const SNAPSHOT = snapshot_tlb_shootdown_wait(request, core_count, generation);
    auto* current = sched::get_current_task();
    log::warn(
        "tlb shootdown wait: origin_cpu=%llu pid=%llu name=%s generation=%llu pending=%u missing=%u unclaimed=%u first_missing=%u "
        "missing_low=0x%llx pagemap=%p vaddr=0x%llx reload=%u elapsed_us=%llu retries=%llu late_acks=%llu",
        static_cast<unsigned long long>(origin_cpu), static_cast<unsigned long long>(current != nullptr ? current->pid : 0),
        current != nullptr && current->name != nullptr ? current->name : "?", static_cast<unsigned long long>(generation), SNAPSHOT.pending,
        SNAPSHOT.missing_count, SNAPSHOT.unclaimed_count, SNAPSHOT.first_missing,
        static_cast<unsigned long long>(SNAPSHOT.missing_mask_low), request.pagemap.load(std::memory_order_acquire),
        static_cast<unsigned long long>(request.vaddr.load(std::memory_order_acquire)),
        request.reload_cr3.load(std::memory_order_acquire) ? 1U : 0U, static_cast<unsigned long long>(elapsed_us),
        static_cast<unsigned long long>(retries), static_cast<unsigned long long>(tlb_shootdown_late_acks.load(std::memory_order_relaxed)));
}

void wait_for_tlb_shootdown_completion(TlbShootdownRequest& request, uint64_t origin_cpu, uint64_t generation, uint64_t core_count) {
    uint64_t const START_US = time::get_us();
    uint64_t next_retry_us = START_US + TLB_SHOOTDOWN_RETRY_US;
    uint64_t next_wake_rescue_us = START_US + TLB_SHOOTDOWN_WAKE_RESCUE_US;
    uint64_t next_log_us = START_US + TLB_SHOOTDOWN_LOG_FIRST_US;
    uint64_t retries = 0;
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
                    log_tlb_shootdown_wait(request, origin_cpu, core_count, generation, time::get_us() - START_US, retries);
                    request.pending.store(0U, std::memory_order_release);
                }
                break;
            }

            uint64_t const NOW_US = time::get_us();
            if (NOW_US >= next_retry_us) {
                bool const WAKE_RESCUE = NOW_US >= next_wake_rescue_us;
                retry_tlb_shootdown_wait(request, core_count, generation, WAKE_RESCUE);
                ++retries;
                next_retry_us = NOW_US + TLB_SHOOTDOWN_RETRY_US;
                if (WAKE_RESCUE) {
                    next_wake_rescue_us = NOW_US + TLB_SHOOTDOWN_WAKE_RESCUE_US;
                }
            }
            if (NOW_US >= next_log_us) {
                log_tlb_shootdown_wait(request, origin_cpu, core_count, generation, NOW_US - START_US, retries);
                next_log_us = NOW_US + TLB_SHOOTDOWN_LOG_REPEAT_US;
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
    uint32_t pending = 0;
    for (uint64_t target_cpu = 0; target_cpu < CORE_COUNT; ++target_cpu) {
        bool const TARGET = target_cpu != ORIGIN_CPU && cpu_may_have_active_pagemap(target_cpu, pagemap);
        request.targets[static_cast<size_t>(target_cpu)].store(TARGET ? 1U : 0U, std::memory_order_relaxed);
        if (TARGET) {
            ++pending;
        }
    }

    if (pending == 0) {
        release_tlb_shootdown_gate(gate);
        return;
    }

    uint64_t next_generation = request.generation.load(std::memory_order_relaxed) + 1;
    if (next_generation == 0) {
        next_generation = 1;
    }

    request.pagemap.store(pagemap, std::memory_order_relaxed);
    request.vaddr.store(vaddr, std::memory_order_relaxed);
    request.reload_cr3.store(reload_cr3, std::memory_order_relaxed);
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
    pool.lock.unlock_irqrestore(FLAGS);

    if (phys::page_kind_get(table) != PageKind::PAGE_TABLE || phys::page_ref_get(table) != 1U) {
        log::critical("page-table pool handed out corrupt page table page=%p", table);
        hcf();
    }

    page_table_pool_alloc_hits.fetch_add(1, std::memory_order_relaxed);
    return table;
}

auto try_release_page_table_to_pool(PageTable* table) -> bool {
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
            pool.lock.unlock_irqrestore(FLAGS);
            page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
            log::critical("duplicate page-table pool release page=%p", table);
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

    uint64_t const INSERT_FLAGS = pool.lock.lock_irqsave();
    for (size_t i = 0; i < pool.count; ++i) {
        if (pool.pages.at(i) == table) {
            pool.lock.unlock_irqrestore(INSERT_FLAGS);
            page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
            log::critical("raced duplicate page-table pool release page=%p", table);
            return true;
        }
    }
    if (pool.count >= pool.pages.size()) {
        pool.lock.unlock_irqrestore(INSERT_FLAGS);
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    pool.pages[pool.count] = table;
    ++pool.count;
    pool.lock.unlock_irqrestore(INSERT_FLAGS);
    page_table_pool_releases.fetch_add(1, std::memory_order_relaxed);
    return true;
}

auto alloc_zeroed_page_table() -> PageTable* {
    auto* pooled_table = try_alloc_page_table_from_pool();
    if (pooled_table != nullptr) {
        return pooled_table;
    }

    auto* table = static_cast<PageTable*>(phys::page_alloc_with_reclaim(paging::PAGE_SIZE, "page_table"));
    if (table == nullptr) {
        return nullptr;
    }

    // page_alloc zeroes every returned PAGE_SIZE page; keep page-table ownership
    // tagging centralized before any reclaim code can observe the page.
    (void)phys::page_mark_kind(table, PageKind::PAGE_TABLE);
    return table;
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
        owned_frame_stats.track_skipped.fetch_add(1, std::memory_order_relaxed);
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
    uint64_t const PHYS_ADDR = paddr & ~(paging::PAGE_SIZE - 1);
    if (!owned_frame_is_private_candidate(pagemap, vaddr, PHYS_ADDR, flags)) {
        return;
    }

    owned_frame_stats.track_attempts.fetch_add(1, std::memory_order_relaxed);
    owned_frame_insert_private_mapping(pagemap, vaddr, PHYS_ADDR);
}

void owned_frame_untrack_mapping(PageTable* pagemap, vaddr_t vaddr, paddr_t paddr) {
    if (pagemap == nullptr || paddr == 0) {
        return;
    }

    owned_frame_stats.untrack_attempts.fetch_add(1, std::memory_order_relaxed);
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        (void)vaddr;
        owned_frame_stats.untrack_missed.fetch_add(1, std::memory_order_relaxed);
        return;
    }

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
    if (entry.frame == 0) {
        return;
    }
    owned_frame_untrack_mapping(pagemap, vaddr, static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT);
}

void owned_frame_refresh_leaf(PageTable* pagemap, vaddr_t vaddr, const PageTableEntry& entry) {
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
    if (pagemap == nullptr) {
        return;
    }

    owned_frame_stats.purge_calls.fetch_add(1, std::memory_order_relaxed);
    if constexpr (!OWNED_FRAME_TRACKING_ENABLED) {
        return;
    }

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
        if (!entry.present) {
            return nullptr;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    return &entry_at(table, index_of(vaddr, 1));
}

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

auto resolve_user_write_mapping(sched::task::Task* task, vaddr_t vaddr) -> UserWriteFaultStatus {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return UserWriteFaultStatus::NOT_WRITABLE;
    }

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
        uint64_t const LOCK_FLAGS = cow_pte_lock.lock_irqsave();

        PageTableEntry& pml4e = entry_at(PAGEMAP, IDX4);
        if (!pml4e.present) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        auto* pml3 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml4e.frame << paging::PAGE_SHIFT));
        PageTableEntry& pml3e = entry_at(pml3, IDX3);
        if (!pml3e.present || pml3e.pagesize != 0) {
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            return UserWriteFaultStatus::NEED_LAZY_BACKING;
        }

        auto* pml2 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(pml3e.frame << paging::PAGE_SHIFT));
        PageTableEntry& pml2e = entry_at(pml2, IDX2);
        if (!pml2e.present || pml2e.pagesize != 0) {
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
        bool const SYNTHETIC_ANON_COW = (raw & (paging::PAGE_COW | paging::PAGE_WRITE)) == 0U && (raw & paging::PAGE_USER) != 0U &&
                                        writable_anonymous_range_contains(task, VADDR);
        if (SYNTHETIC_ANON_COW) {
            owned_frame_untrack_leaf(PAGEMAP, VADDR, pte);
            raw |= paging::PAGE_COW;
            pte = pte_from_raw(raw);
        }

        if ((raw & paging::PAGE_COW) == 0U) {
            if ((raw & paging::PAGE_WRITE) == 0U || (raw & paging::PAGE_USER) == 0U) {
                cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
                return UserWriteFaultStatus::NOT_WRITABLE;
            }

            bool const PATH_PROMOTED = promote_user_write_path(pml4e, pml3e, pml2e);
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
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
            cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
            flush_pagemap_after_update(PAGEMAP, VADDR, initial_path_promoted);
            record_cow_perf_event(task, cow_op, VADDR, refcount, cow_started_us);
            return UserWriteFaultStatus::HANDLED;
        }

        // Pin the old frame while the PTE lock still proves this mapping's
        // PTE reference exists. Allocation and memcpy below intentionally run
        // outside the lock; the commit path rechecks the same frame under lock.
        phys::page_ref_inc(old_virt, &cow_lookup);
        cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
    }

    bool const DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE = !old_is_zero_page;
    void* new_page = alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE);
    if (new_page == nullptr) {
        phys::page_ref_dec(old_virt, &cow_lookup);
        log::error("COW fault: OOM allocating new page for vaddr 0x%x", VADDR);
        hcf();
        return UserWriteFaultStatus::NOT_WRITABLE;
    }

    if (DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE) {
        std::memcpy(new_page, old_virt, paging::PAGE_SIZE);
    }

    bool installed_private_page = false;
    bool final_path_promoted = false;
    {
        uint64_t const LOCK_FLAGS = cow_pte_lock.lock_irqsave();
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
                    if ((raw_now & paging::PAGE_COW) != 0U && CURRENT_PHYS == old_phys) {
                        final_path_promoted = promote_user_write_path(pml4e, pml3e, pml2e);
                        auto new_phys = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(new_page)));
                        raw_now &= ~paging::PAGE_COW;
                        raw_now |= paging::PAGE_WRITE;
                        raw_now &= ~(0xFFFFFFFFFFULL << 12);
                        raw_now |= (new_phys & ~0xFFFULL);
                        pte = pte_from_raw(raw_now);
                        owned_frame_track_fresh_normal_mapping(PAGEMAP, VADDR, new_phys, raw_now);
                        installed_private_page = true;
                    }
                }
            }
        }
        cow_pte_lock.unlock_irqrestore(LOCK_FLAGS);
    }

    if (!installed_private_page) {
        phys::page_ref_dec(new_page);
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

auto ensure_user_page_writable_for_task(sched::task::Task* task, vaddr_t vaddr) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        switch (resolve_user_write_mapping(task, vaddr)) {
            case UserWriteFaultStatus::HANDLED:
                return true;
            case UserWriteFaultStatus::NOT_WRITABLE:
                return false;
            case UserWriteFaultStatus::NEED_LAZY_BACKING:
                break;
        }

        paging::PageFault const WRITE_FAULT = paging::create_page_fault(1ULL << paging::error_flags::WRITE, true);
        if (!handle_lazy_vmem_fault(task, vaddr, WRITE_FAULT)) {
            return false;
        }
    }

    return false;
}

auto ensure_user_page_mapped_for_task(sched::task::Task* task, vaddr_t vaddr) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return false;
    }

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        if (translate(task->pagemap, vaddr) != PADDR_INVALID) {
            return true;
        }

        paging::PageFault const READ_FAULT = paging::create_page_fault(0, true);
        if (!handle_lazy_vmem_fault(task, vaddr, READ_FAULT)) {
            return false;
        }
    }

    return translate(task->pagemap, vaddr) != PADDR_INVALID;
}
}  // namespace

void service_pending_tlb_shootdowns() {
    if (tlb_shootdown_vector == 0 || !smt::has_cpu_data()) {
        return;
    }
    service_tlb_shootdown_requests_for_cpu(cpu::current_cpu());
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
    return READONLY ? paging::page_types::READONLY : paging::page_types::KERNEL;
}

struct BootDirectMapStats {
    size_t pages{};
    size_t small_4k{};
    size_t large_2m{};
};

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

auto map_boot_direct_range(PageTable* page_table, Range range, uint64_t flags) -> BootDirectMapStats {
    BootDirectMapStats stats{};
    if (!range_valid(range)) {
        return stats;
    }

    PageMapBatch batch{};
    init_page_map_batch(&batch, page_table, flags);
    bool const HHDM_SUPPORTS_2M = is_aligned_to(addr::get_hhdm_offset(), LARGE_PAGE_2M_BYTES);
    for (uint64_t phys = range.start; phys < range.end; phys += paging::PAGE_SIZE) {
        auto const VADDR = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(phys));
        if (HHDM_SUPPORTS_2M && is_aligned_to(phys, LARGE_PAGE_2M_BYTES) && range.end - phys >= LARGE_PAGE_2M_BYTES &&
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
        dbg::log("WARNING: No free interrupt vector for TLB shootdown IPI");
    }
}

void note_tlb_shootdown_cpu_online() {
    uint64_t const CPU_NO = cpu::current_cpu();
    if (!cpu_slot_valid(CPU_NO)) {
        return;
    }
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

auto create_pagemap() -> PageTable* { return alloc_zeroed_page_table(); }

void release_pagemap(PageTable* pagemap) {
    if (pagemap == nullptr) {
        return;
    }

    if (pagemap == kernel_pagemap || is_current_pagemap(pagemap)) {
        page_table_pool_rejects.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    owned_frame_purge_pagemap(pagemap);

    if (try_release_page_table_to_pool(pagemap)) {
        return;
    }

    phys::page_free(pagemap);
}

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

auto pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& /*gpr*/) -> bool {
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

    // Lazy user-stack backing. This handles non-present faults in the
    // reserved process stack range, including kernel-mode faults caused by
    // syscall copy paths writing into a not-yet-backed user stack page.
    if (PAGEFAULT.present == 0U && control_register < 0x0000800000000000ULL) {
        auto* current_task = sched::get_current_task();
        if (handle_lazy_vmem_fault(current_task, control_register, PAGEFAULT, frame.rip, frame.rsp)) {
            return true;
        }
        if (current_task != nullptr && current_task->pagemap != nullptr && current_task->thread != nullptr &&
            sched::threading::handle_lazy_stack_fault(current_task->thread, current_task->pagemap, control_register, frame.rsp)) {
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

        if (resolve_user_write_mapping(current_task, control_register) == UserWriteFaultStatus::HANDLED) {
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

bool ensure_user_page_writable(sched::task::Task* task, vaddr_t vaddr) { return ensure_user_page_writable_for_task(task, vaddr); }

bool ensure_user_page_mapped(sched::task::Task* task, vaddr_t vaddr) { return ensure_user_page_mapped_for_task(task, vaddr); }

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

    return stats;
}

void init_pagemap() {
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
        log::debug("memory map entry %d: %x - %x (%s)", i, entry->base, entry->base + entry->length, type_str);
    }

    BootDirectMapStats direct_map_stats{};
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        Range entry_range{};
        if (!memmap_page_range(entry, entry_range)) {
            continue;
        }

        BootDirectMapStats const ENTRY_STATS = map_boot_direct_range(kernel_pagemap, entry_range, direct_map_flags_for_entry(entry->type));
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
    auto const KERNEL_IMAGE_END = reinterpret_cast<uint64_t>(__kernel_end);
    if (KERNEL_IMAGE_END < KERNEL_VIRT_BASE) {
        log::critical("init_pagemap: invalid kernel image bounds base=%p end=%p", KERNEL_VIRT_BASE, KERNEL_IMAGE_END);
        hcf();
    }

    uint64_t const KERNEL_IMAGE_SIZE = KERNEL_IMAGE_END - KERNEL_VIRT_BASE;
    uint64_t const KERNEL_PAGE_COUNT = page_align_up(KERNEL_IMAGE_SIZE) / paging::PAGE_SIZE;
    if (KERNEL_PAGE_COUNT > 0) {
        PageMapBatch kernel_batch{};
        init_page_map_batch(&kernel_batch, kernel_pagemap, paging::page_types::KERNEL);
        for (uint64_t i = 0; i < KERNEL_PAGE_COUNT; i++) {
            vaddr_t const VADDR = kernel_address_response->virtual_base + (i * paging::PAGE_SIZE);
            paddr_t const PADDR = kernel_address_response->physical_base + (i * paging::PAGE_SIZE);
            map_page_batched(&kernel_batch, VADDR, PADDR, paging::page_types::KERNEL);
        }
        flush_page_map_batch(&kernel_batch);
    }

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
                map_page(kernel_pagemap, pml3_virt, PML3_PHYS, paging::page_types::KERNEL);
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
                    map_page(kernel_pagemap, pml2_virt, PML2_PHYS, paging::page_types::KERNEL);
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
                        map_page(kernel_pagemap, pml1_virt, PML1_PHYS, paging::page_types::KERNEL);
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
}

namespace {

// advance page table by level pages
auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted) -> paging::PageTable* {
    PageTableEntry entry = entry_at(page_table, level);
    if (entry.present) {
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

    PageTableEntry& entry = entry_at(pml1, index_of(VADDR, 1));
    PageTableEntry const OLD_ENTRY = entry;
    bool const REPLACED_TRANSLATION = OLD_ENTRY.present != 0 || is_reserved_leaf(OLD_ENTRY);
    owned_frame_untrack_leaf(page_table, VADDR, OLD_ENTRY);
    entry = paging::create_page_table_entry(PADDR, FLAGS);
    owned_frame_track_private_mapping(page_table, VADDR, PADDR, FLAGS);

    invalidate_local_tlb_if_current(page_table, VADDR, path_promoted);
    if (path_promoted || REPLACED_TRANSLATION) {
        shootdown_remote_user_pagemap(page_table, VADDR, path_promoted);
    }
}

void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, const uint64_t FLAGS) {
    if (batch == nullptr || page_table == nullptr || FLAGS == 0) {
        log::critical("init_page_map_batch: invalid args=<batch: %p, pagemap: %p, flags: %x>", batch, page_table, FLAGS);
        hcf();
    }

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

    PageTableEntry& entry = entry_at(batch->pml1, index_of(VADDR, 1));
    owned_frame_untrack_leaf(batch->root, VADDR, entry);
    entry = paging::create_page_table_entry(PADDR, FLAGS);
    owned_frame_track_private_mapping(batch->root, VADDR, PADDR, FLAGS);
    batch->dirty = true;
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
    bool changed = false;
    bool path_promoted = false;
    for (uint64_t i = 0; i < PAGE_COUNT; i++) {
        vaddr_t const CURRENT_VADDR = VADDR + (i * paging::PAGE_SIZE);
        PageTable* pml3 = advance_page_table(page_table, index_of(CURRENT_VADDR, 4), TABLE_FLAGS, &path_promoted);
        PageTable* pml2 = advance_page_table(pml3, index_of(CURRENT_VADDR, 3), TABLE_FLAGS, &path_promoted);
        PageTable* pml1 = advance_page_table(pml2, index_of(CURRENT_VADDR, 2), TABLE_FLAGS, &path_promoted);

        PageTableEntry& entry = entry_at(pml1, index_of(CURRENT_VADDR, 1));
        uint64_t const OLD_RAW = pte_raw(entry);
        if (OLD_RAW == paging::PAGE_RESERVED) {
            continue;
        }

        PageTableEntry const OLD_ENTRY = entry;
        owned_frame_untrack_leaf(page_table, CURRENT_VADDR, OLD_ENTRY);
        entry = pte_from_raw(paging::PAGE_RESERVED);
        changed = true;
        flush_pagemap_after_update(page_table, CURRENT_VADDR, path_promoted);
        path_promoted = false;
        drop_present_leaf_ref(OLD_ENTRY);
    }

    if (!changed && path_promoted) {
        flush_pagemap_after_update(page_table, VADDR, true);
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

void unify_page_flags(PageTable* page_table, vaddr_t vaddr, uint64_t flags) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unify_page_flags");
        hcf();
    }

    PageTable* table = page_table;
    bool path_promoted = false;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), flags, &path_promoted);
    }

    // Get the current page table entry by computing the index first
    uint64_t const IDX = index_of(vaddr, 1);
    PageTableEntry& entry = entry_at(table, IDX);

    if (entry.present == 0) {
        // Page doesn't exist, nothing to modify
        if (path_promoted) {
            flush_pagemap_after_update(page_table, vaddr, true);
        }
        return;
    }

    auto* raw_entry = reinterpret_cast<uint64_t*>(&entry);
    uint64_t const OLD_RAW = *raw_entry;
    bool const IS_COW = (OLD_RAW & paging::PAGE_COW) != 0U;

    // Update page table entry flags
    entry.present = (flags & paging::PAGE_PRESENT) != 0U ? 1 : 0;
    // COW pages must stay read-only even when mprotect(PROT_WRITE) runs; the
    // write fault is what creates the private writable page.
    entry.writable = ((flags & paging::PAGE_WRITE) != 0U && !IS_COW) ? 1 : 0;
    entry.user = (flags & paging::PAGE_USER) != 0U ? 1 : 0;
    entry.no_execute = (flags & paging::PAGE_NX) != 0U ? 1 : 0;

    constexpr uint64_t NX_BIT_POSITION = 63ULL;
    if ((flags & paging::PAGE_NX) != 0U) {
        *raw_entry |= (1ULL << NX_BIT_POSITION);  // Set NX bit
    } else {
        *raw_entry &= ~(1ULL << NX_BIT_POSITION);  // Clear NX bit
    }

    owned_frame_refresh_leaf(page_table, vaddr, entry);

    if (*raw_entry != OLD_RAW || path_promoted) {
        flush_pagemap_after_update(page_table, vaddr, path_promoted);
    }

#ifdef ELF_DEBUG
    if (vaddr >= 0x501000 && vaddr < 0x580000) {
        log::debug("unify_page_flags: vaddr=0x%x, flags=0x%x, entry_after=0x%x, nx=%d present=%d", vaddr, flags, *raw_entry,
                   static_cast<int>((*raw_entry >> NX_BIT_POSITION) & 1U), static_cast<int>(entry.present));
    }
#endif
}

void unmap_page(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unmap_page");
        hcf();
    }

    PageTableEntry* entry = leaf_entry(page_table, vaddr);
    if (entry == nullptr) {
        return;
    }

    PageTableEntry const OLD_ENTRY = *entry;
    if (OLD_ENTRY.present == 0 && !is_reserved_leaf(OLD_ENTRY)) {
        return;
    }
    owned_frame_untrack_leaf(page_table, vaddr, OLD_ENTRY);
    *entry = paging::purge_page_table_entry();
    flush_pagemap_after_update(page_table, vaddr, false);
    drop_present_leaf_ref(OLD_ENTRY);
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

void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags) { map_page(kernel_pagemap, vaddr, paddr, flags); }

void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset) { map_range(kernel_pagemap, range, flags, offset); }

void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    // no offset assume hhdm
    map_range(kernel_pagemap, range, flags, addr::get_hhdm_offset());
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

    auto add(uint64_t phys_addr) -> bool {
        phys_addr &= ~(paging::PAGE_SIZE - 1);
        if (phys_addr == 0 || frames.contains(phys_addr)) {
            return true;
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
                               FrameProbeCache* frame_probe_cache = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

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
            collect_page_table_frames(next_level, level - 1, frames, stats, frame_probe_cache);
        }
    }
}

auto advance_collect_frames_budgeted(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats& stats) -> bool {
    while (state.stack_size > 0) {
        auto& frame = destroy_state_top(state);
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

        state.page_table_frames.add(PHYS_ADDR);
        auto* next_level = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PHYS_ADDR, &state.frame_probe_cache));
        if (next_level != nullptr) {
            (void)destroy_state_push(state, next_level, frame.level - 1, 0, PHYS_ADDR);
        }
        return true;
    }

    destroy_state_reset_walk(state, DestroyUserSpacePhase::FREE_DATA);
    return false;
}

auto advance_free_data_budgeted(DestroyUserSpaceBudgetState& state, DestroyUserSpaceCallStats& stats) -> bool {
    while (state.stack_size > 0) {
        auto& frame = destroy_state_top(state);
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
                           uint64_t vaddr_base = 0, FrameProbeCache* frame_probe_cache = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

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
            free_page_table_pages(next_level, level - 1, page_table_refdec_batch, stats, ENTRY_VADDR, frame_probe_cache);
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

auto create_destroy_user_space_budget_state(PageTable* pagemap, uint64_t owner_pid, const char* owner_name, const char* reason)
    -> DestroyUserSpaceBudgetState* {
    if (pagemap == nullptr) {
        return nullptr;
    }

    auto* state = new DestroyUserSpaceBudgetState{};
    if (state == nullptr) {
        return nullptr;
    }

    owned_frame_purge_pagemap(pagemap);
    state->pagemap = pagemap;
    state->owner_pid = owner_pid;
    state->owner_name = owner_name;
    state->reason = reason;
    state->phase = DestroyUserSpacePhase::COLLECT_FRAMES;
    state->page_table_frames.add(reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(pagemap))));
    destroy_state_reset_walk(*state, DestroyUserSpacePhase::COLLECT_FRAMES);
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
    delete state;
}

void destroy_user_space(PageTable* pagemap, uint64_t owner_pid, const char* owner_name, const char* reason) {
    if (pagemap == nullptr) {
        return;
    }
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
    collect_page_table_frames(pagemap, 4, page_table_frames, &stats, &frame_probe_cache);
    stats.collect_frames_us = elapsed_us_since(phase_start_us, time::get_us());

    phase_start_us = time::get_us();
    DestroyRefdecBatch refdec_batch{};
    free_user_data_pages(pagemap, 4, pagemap, page_table_frames, refdec_batch, &stats, 0, owner_pid, owner_name, reason,
                         &frame_probe_cache);
    destroy_refdec_batch_flush(refdec_batch, &stats, DestroyRefdecBatchKind::DATA);
    stats.free_data_us = elapsed_us_since(phase_start_us, time::get_us());

    phase_start_us = time::get_us();
    DestroyRefdecBatch page_table_refdec_batch{};
    free_page_table_pages(pagemap, 4, page_table_refdec_batch, &stats, 0, &frame_probe_cache);
    destroy_refdec_batch_flush(page_table_refdec_batch, &stats, DestroyRefdecBatchKind::PAGE_TABLE);
    stats.free_pt_us = elapsed_us_since(phase_start_us, time::get_us());

    // Invalidate TLB for this address space
    phase_start_us = time::get_us();
    flush_pagemap_after_update(pagemap, 0, true);
    stats.tlb_flush_us = elapsed_us_since(phase_start_us, time::get_us());
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
                        auto* sub = phys::page_alloc(paging::PAGE_SIZE, "cow_huge_sub");
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
                        auto* child_page = phys::page_alloc_full_overwrite_page_with_reclaim("fork_copy");
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

                    // Increment refcount on the shared data page
                    owned_frame_untrack_mapping(src, VADDR, DATA_PHYS);
                    phys::page_ref_inc(data_virt, &ref_lookup);
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

    // Flush TLB for the source (parent) since we modified its PTEs.
    flush_pagemap_after_update(src, 0, true);
    return true;
}

}  // namespace ker::mod::mm::virt
