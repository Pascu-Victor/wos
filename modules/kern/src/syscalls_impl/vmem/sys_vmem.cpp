#include "sys_vmem.hpp"

#include <abi/callnums/vmem.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/usercopy.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vmem {
using log = ker::mod::dbg::logger<"vmem">;

// Linux x86_64 user space address range
// User space: 0x0000000000000000 - 0x00007FFFFFFFFFFF (128TB)
// Kernel space: 0xFFFF800000000000 - 0xFFFFFFFFFFFFFFFF
constexpr uint64_t USER_SPACE_START = 0x0000000000400000ULL;  // Start after first 4MB (for NULL protection and low mem)
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;    // Linux canonical address limit
constexpr uint64_t MMAP_START = 0x0000200000000000ULL;        // mmap base above ASAN shadow, below ELF debug info
constexpr uint64_t MREMAP_MAYMOVE = 1;
constexpr size_t SWAP_PATH_MAX = 512;

namespace {
// Get the current task
inline auto get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

ker::mod::sys::Mutex g_shared_vmem_publication_lock;

constexpr bool ENABLE_WATCHED_MMAP_LOGS = false;
constexpr uint64_t WATCH_MMAP_VADDR = 0x00001000007da000ULL;

inline auto is_watched_mmap_vaddr(uint64_t vaddr) -> bool { return ENABLE_WATCHED_MMAP_LOGS && vaddr == WATCH_MMAP_VADDR; }

auto clamp_perf_u16(uint64_t value) -> uint16_t { return value > UINT16_MAX ? UINT16_MAX : static_cast<uint16_t>(value); }

auto clamp_perf_u32(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

auto align_user_vmem_size(uint64_t size, uint64_t* aligned_size) -> int {
    constexpr uint64_t PAGE_MASK = ker::mod::mm::paging::PAGE_SIZE - 1;
    if (size == 0 || aligned_size == nullptr || size > UINT64_MAX - PAGE_MASK) {
        return -ker::abi::vmem::VMEM_EINVAL;
    }
    uint64_t const ALIGNED = page_align_up(size);
    if (ALIGNED == 0) {
        return -ker::abi::vmem::VMEM_EINVAL;
    }
    *aligned_size = ALIGNED;
    return 0;
}

auto file_mapping_offset_range_valid(uint64_t offset, uint64_t size) -> bool {
    constexpr uint64_t MAX_OFFSET = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    return size != 0 && offset <= MAX_OFFSET && size - 1 <= MAX_OFFSET - offset;
}

auto vmem_latency_since(uint64_t started_us) -> uint32_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    return clamp_perf_u32(NOW_US >= started_us ? NOW_US - started_us : 0);
}

auto perf_status_from_vmem_result(uint64_t result) -> int32_t {
    if (result != 0 && result <= USER_SPACE_END) {
        return 0;
    }
    return static_cast<int32_t>(result);
}

void record_local_vmem_event(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalVmemOp op, ker::mod::perf::WkiPerfPhase phase,
                             uint64_t pages, uint16_t detail, int32_t status, uint32_t latency_us, uint64_t addr, uint64_t bytes,
                             bool has_latency) {
    if (task == nullptr || !ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), task->pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_VMEM, static_cast<uint8_t>(op), phase, clamp_perf_u16(pages),
                                     detail, ker::mod::perf::next_wki_trace_correlation(), status, latency_us, addr);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_VMEM, static_cast<uint8_t>(op), 0, 0, status, latency_us,
                                       has_latency, 0, bytes);
}

void record_local_vmem_cache_event(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalVmemOp op, uint64_t file_offset,
                                   uint32_t latency_us, bool has_latency) {
    record_local_vmem_event(task, op, ker::mod::perf::WkiPerfPhase::POINT, 1, 0, 0, latency_us, file_offset,
                            ker::mod::mm::paging::PAGE_SIZE, has_latency);
}

std::atomic<void*> g_anon_zero_page{nullptr};

struct FileMmapPageKey {
    uint64_t exec_content_id{};
    ker::vfs::dev_t dev{};
    ker::vfs::ino_t ino{};
    uint64_t size{};
    int64_t mtime_sec{};
    int64_t mtime_nsec{};
    int64_t ctime_sec{};
    int64_t ctime_nsec{};
    uint64_t offset{};
};

struct FileMmapPageCacheEntry {
    FileMmapPageKey key{};
    void* page = nullptr;
    uint64_t last_used = 0;
};

struct FileMmapPageCacheSet {
    std::array<FileMmapPageCacheEntry, 4> ways{};
};

// Shared file mappings retain policy even when their backend has no writeback.
struct FileMmapRange {
    ker::mod::mm::paging::PageTable* pagemap = nullptr;
    uint64_t start = 0;
    uint64_t length = 0;
    uint64_t file_offset = 0;
    ker::vfs::File* file = nullptr;
    bool writeback = false;
};

struct FileMmapSyncRange {
    uint64_t mapping_start = 0;
    uint64_t sync_start = 0;
    uint64_t sync_end = 0;
    uint64_t file_offset = 0;
    ker::vfs::File* file = nullptr;
    bool skip_writeback = false;
};

enum class FileMmapCacheInsertResult : uint8_t {
    INSERTED,
    DUPLICATE,
    EVICTED,
};

constexpr size_t FILE_MMAP_CACHE_SET_COUNT = 32768;
constexpr size_t FILE_MMAP_CACHE_WAYS = 4;
constexpr size_t FILE_MMAP_CACHE_PAGES = FILE_MMAP_CACHE_SET_COUNT * FILE_MMAP_CACHE_WAYS;
constexpr size_t FILE_MMAP_CACHE_LOCK_COUNT = 256;
static_assert((FILE_MMAP_CACHE_SET_COUNT & (FILE_MMAP_CACHE_SET_COUNT - 1)) == 0);
static_assert((FILE_MMAP_CACHE_LOCK_COUNT & (FILE_MMAP_CACHE_LOCK_COUNT - 1)) == 0);
static_assert(FILE_MMAP_CACHE_LOCK_COUNT <= FILE_MMAP_CACHE_SET_COUNT);

// Faults for unrelated executable and source pages must not bounce one global
// mutex or timestamp cache line across every CPU. Each set maps to exactly one
// cache-line-isolated lock and clock, so entry ownership remains unchanged.
struct alignas(64) FileMmapCacheLock {
    ker::mod::sys::Mutex mutex;
    uint64_t clock = 0;
};

static_assert(sizeof(FileMmapCacheLock) % 64 == 0);
std::array<FileMmapPageCacheSet, FILE_MMAP_CACHE_SET_COUNT> g_file_mmap_cache{};
std::array<FileMmapCacheLock, FILE_MMAP_CACHE_LOCK_COUNT> g_file_mmap_cache_locks{};
std::atomic<uint64_t> g_file_mmap_cache_pages{0};
std::atomic<uint64_t> g_file_mmap_cache_scan_cursor{0};
std::atomic<bool> g_file_mmap_cache_shrinker_registered{false};
ker::util::SmallVec<FileMmapRange, 32> g_file_mmap_ranges;
ker::mod::sys::Mutex g_file_mmap_ranges_lock;

auto file_mmap_key_equal(const FileMmapPageKey& lhs, const FileMmapPageKey& rhs) -> bool {
    return lhs.exec_content_id == rhs.exec_content_id && lhs.dev == rhs.dev && lhs.ino == rhs.ino && lhs.size == rhs.size &&
           lhs.mtime_sec == rhs.mtime_sec && lhs.mtime_nsec == rhs.mtime_nsec && lhs.ctime_sec == rhs.ctime_sec &&
           lhs.ctime_nsec == rhs.ctime_nsec && lhs.offset == rhs.offset;
}

auto file_mmap_key_hash(const FileMmapPageKey& key) -> uint64_t {
    uint64_t hash = key.exec_content_id ^ key.dev ^ (key.ino << 7U) ^ (key.ino >> 3U) ^ key.size ^ key.offset;
    hash ^= static_cast<uint64_t>(key.mtime_sec) + 0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.mtime_nsec) + 0xbf58476d1ce4e5b9ULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.ctime_sec) + 0x94d049bb133111ebULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.ctime_nsec) + 0x2545f4914f6cdd1dULL + (hash << 6U) + (hash >> 2U);
    hash ^= hash >> 33U;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33U;
    return hash;
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

auto file_mmap_page_cache_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = !cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.no_file_mmap_cache");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        log::info("file mmap page cache %s", ENABLED ? "enabled" : "disabled by cmdline");
        return ENABLED;
    }

    return expected != 0;
}

auto lazy_file_mmap_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = !cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.no_lazy_file_mmap");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        log::info("lazy file mmap %s", ENABLED ? "enabled" : "disabled by cmdline");
        return ENABLED;
    }

    return expected != 0;
}

auto lazy_anon_mmap_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = !cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.no_lazy_anon");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        log::info("lazy anonymous mmap %s", ENABLED ? "enabled" : "disabled by cmdline");
        return ENABLED;
    }

    return expected != 0;
}

auto anon_zero_cow_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int cached = s_enabled.load(std::memory_order_acquire);
    if (cached >= 0) {
        return cached != 0;
    }

    bool const ENABLED = !cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.no_anon_zero_cow");
    int const VALUE = ENABLED ? 1 : 0;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        log::info("anonymous zero-page COW %s", ENABLED ? "enabled" : "disabled by cmdline");
        return ENABLED;
    }

    return expected != 0;
}

auto file_mmap_page_key(const ker::vfs::Stat& st, uint64_t offset) -> FileMmapPageKey {
    return FileMmapPageKey{.dev = st.st_dev,
                           .ino = st.st_ino,
                           .size = st.st_size > 0 ? static_cast<uint64_t>(st.st_size) : 0,
                           .mtime_sec = st.st_mtim.tv_sec,
                           .mtime_nsec = st.st_mtim.tv_nsec,
                           .ctime_sec = st.st_ctim.tv_sec,
                           .ctime_nsec = st.st_ctim.tv_nsec,
                           .offset = offset};
}

auto file_mmap_can_share(const ker::vfs::Stat& st, uint64_t prot) -> bool {
    if (!file_mmap_page_cache_enabled()) {
        return false;
    }
    return (prot & ker::abi::vmem::PROT_WRITE) == 0 && (st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG && st.st_ino != 0;
}

auto is_prot_none(uint64_t prot) -> bool { return prot == ker::abi::vmem::PROT_NONE; }

auto ranges_overlap(uint64_t start, uint64_t end, uint64_t other_start, uint64_t other_end) -> bool {
    return start < other_end && other_start < end;
}

struct KernelManagedLayoutOverlapQuery {
    ker::mod::mm::paging::PageTable* pagemap = nullptr;
    uint64_t start = 0;
    uint64_t size = 0;
};

auto task_has_overlapping_kernel_managed_layout(ker::mod::sched::task::Task* candidate, void* opaque) -> bool {
    auto* query = static_cast<KernelManagedLayoutOverlapQuery*>(opaque);
    return candidate != nullptr && query != nullptr && candidate->pagemap == query->pagemap &&
           ker::mod::sched::threading::range_overlaps_initial_layout(candidate->thread, query->start, query->size);
}

// The caller holds SharedVmemPublicationGuard, which keeps the pagemap/thread
// ownership set stable while the scheduler registry is queried.
auto overlaps_kernel_managed_user_layout_locked(ker::mod::sched::task::Task* task, uint64_t start, uint64_t size) -> bool {
    if (task == nullptr || task->pagemap == nullptr || size == 0) {
        return false;
    }

    if (ker::mod::sched::threading::range_overlaps_initial_layout(task->thread, start, size)) {
        return true;
    }

    KernelManagedLayoutOverlapQuery query{.pagemap = task->pagemap, .start = start, .size = size};
    auto* owner = ker::mod::sched::find_active_task_lifetime_ref_if(task_has_overlapping_kernel_managed_layout, &query);
    if (owner == nullptr) {
        return false;
    }
    owner->release();
    return true;
}

auto checked_range_end(uint64_t start, uint64_t length, uint64_t* end_out) -> bool {
    if (end_out == nullptr || length == 0 || start > UINT64_MAX - length) {
        return false;
    }
    *end_out = start + length;
    return true;
}

auto file_mmap_range_end(const FileMmapRange& range) -> uint64_t { return range.start + range.length; }

struct OccupiedVmemRange {
    uint64_t start = 0;
    uint64_t end = 0;
    bool found = false;
};

using LazyVmemRange = ker::mod::sched::task::LazyVmemRange;
using LazyVmemKind = ker::mod::sched::task::LazyVmemKind;
using LazyVmemRangeVec = ker::mod::sched::task::LazyVmemRangeVec;

auto slice_lazy_vmem_range(const LazyVmemRange& range, uint64_t start, uint64_t end, LazyVmemRange& out) -> bool {
    if (start < range.start || start >= end || end > range.end) {
        return false;
    }
    auto sliced = range;
    if (range.kind == LazyVmemKind::FILE_BACKED) {
        uint64_t const DELTA = start - range.start;
        if (DELTA > UINT64_MAX - range.file_offset) {
            return false;
        }
        sliced.file_offset += DELTA;
    }
    sliced.start = start;
    sliced.end = end;
    out = sliced;
    return true;
}

auto push_lazy_vmem_range(LazyVmemRangeVec& ranges, const LazyVmemRange& range) -> bool {
    if (range.start >= range.end) {
        return true;
    }
    return ranges.push_back(range);
}

auto lazy_vmem_range_has_file(const LazyVmemRange& range) -> bool {
    return range.kind == LazyVmemKind::FILE_BACKED && range.file != nullptr;
}

void retain_lazy_vmem_range_file(const LazyVmemRange& range) {
    if (lazy_vmem_range_has_file(range)) {
        ker::vfs::vfs_retain_file(range.file);
    }
}

void release_lazy_vmem_range_file(const LazyVmemRange& range) {
    if (lazy_vmem_range_has_file(range)) {
        ker::vfs::vfs_put_file(range.file);
    }
}

void release_lazy_vmem_range_files(const LazyVmemRangeVec& ranges) {
    for (const auto& range : ranges) {
        release_lazy_vmem_range_file(range);
    }
}

auto push_lazy_vmem_range_copy(LazyVmemRangeVec& ranges, const LazyVmemRange& range) -> bool {
    size_t const BEFORE = ranges.size();
    if (!push_lazy_vmem_range(ranges, range)) {
        return false;
    }
    if (ranges.size() != BEFORE) {
        retain_lazy_vmem_range_file(ranges.at(BEFORE));
    }
    return true;
}

auto pte_raw(const ker::mod::mm::paging::PageTableEntry& entry) -> uint64_t {
    uint64_t raw = 0;
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}

auto is_reserved_leaf(const ker::mod::mm::paging::PageTableEntry& entry) -> bool {
    return entry.present == 0 && (pte_raw(entry) & ker::mod::mm::paging::PAGE_RESERVED) != 0U;
}

auto table_from_entry(const ker::mod::mm::paging::PageTableEntry& entry) -> ker::mod::mm::paging::PageTable* {
    return reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << ker::mod::mm::paging::PAGE_SHIFT));
}

auto page_table_index(uint64_t vaddr, uint64_t shift) -> size_t { return static_cast<size_t>((vaddr >> shift) & 0x1FFU); }

auto next_table_boundary(uint64_t vaddr, uint64_t shift) -> uint64_t {
    uint64_t const MASK = (1ULL << shift) - 1;
    return (vaddr | MASK) + 1;
}

auto first_pagemap_occupied_range(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t end) -> OccupiedVmemRange {
    if (pagemap == nullptr || start >= end) {
        return {};
    }

    constexpr uint64_t PML4_SHIFT = 39;
    constexpr uint64_t PML3_SHIFT = 30;
    constexpr uint64_t PML2_SHIFT = 21;
    constexpr uint64_t PML1_SHIFT = 12;

    uint64_t addr = page_align_down(start);
    while (addr < end) {
        uint64_t const PML4_END = std::min(next_table_boundary(addr, PML4_SHIFT), end);
        auto const& pml4e = pagemap->entries[page_table_index(addr, PML4_SHIFT)];
        if (pml4e.present == 0) {
            addr = PML4_END;
            continue;
        }

        auto* pml3 = table_from_entry(pml4e);
        while (addr < PML4_END) {
            uint64_t const PML3_END = std::min(next_table_boundary(addr, PML3_SHIFT), PML4_END);
            auto const& pml3e = pml3->entries[page_table_index(addr, PML3_SHIFT)];
            if (pml3e.present == 0) {
                addr = PML3_END;
                continue;
            }
            if (pml3e.pagesize != 0) {
                return {.start = addr, .end = PML3_END, .found = true};
            }

            auto* pml2 = table_from_entry(pml3e);
            while (addr < PML3_END) {
                uint64_t const PML2_END = std::min(next_table_boundary(addr, PML2_SHIFT), PML3_END);
                auto const& pml2e = pml2->entries[page_table_index(addr, PML2_SHIFT)];
                if (pml2e.present == 0) {
                    addr = PML2_END;
                    continue;
                }
                if (pml2e.pagesize != 0) {
                    return {.start = addr, .end = PML2_END, .found = true};
                }

                auto* pml1 = table_from_entry(pml2e);
                while (addr < PML2_END) {
                    uint64_t const PML1_END = std::min(next_table_boundary(addr, PML1_SHIFT), PML2_END);
                    auto const& pte = pml1->entries[page_table_index(addr, PML1_SHIFT)];
                    if (pte.present != 0 || is_reserved_leaf(pte)) {
                        return {.start = addr, .end = PML1_END, .found = true};
                    }
                    addr = PML1_END;
                }
            }
        }
    }

    return {};
}

auto first_lazy_vmem_overlap(ker::mod::sched::task::Task* task, uint64_t start, uint64_t end) -> OccupiedVmemRange {
    if (task == nullptr || start >= end) {
        return {};
    }

    OccupiedVmemRange first{};
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (!ranges_overlap(start, end, range.start, range.end)) {
            continue;
        }

        OccupiedVmemRange const OVERLAP{
            .start = std::max(start, range.start),
            .end = std::min(end, range.end),
            .found = true,
        };
        if (!first.found || OVERLAP.start < first.start) {
            first = OVERLAP;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return first;
}

auto first_occupied_range(ker::mod::sched::task::Task* task, uint64_t start, uint64_t end) -> OccupiedVmemRange {
    if (task == nullptr || task->pagemap == nullptr || start >= end) {
        return {};
    }

    auto first = first_pagemap_occupied_range(task->pagemap, start, end);
    auto const LAZY = first_lazy_vmem_overlap(task, start, end);
    if (LAZY.found && (!first.found || LAZY.start < first.start)) {
        first = LAZY;
    }
    return first;
}

auto remove_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return true;
    }

    uint64_t const END = vaddr + size;
    LazyVmemRangeVec rewritten;
    LazyVmemRangeVec ranges_to_release;
    bool ok = true;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (!ranges_overlap(vaddr, END, range.start, range.end)) {
            ok = push_lazy_vmem_range_copy(rewritten, range);
            if (!ok) {
                break;
            }
            continue;
        }

        if (range.start < vaddr) {
            LazyVmemRange left;
            ok = slice_lazy_vmem_range(range, range.start, vaddr, left) && push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }
        if (END < range.end) {
            LazyVmemRange right;
            ok = slice_lazy_vmem_range(range, END, range.end, right) && push_lazy_vmem_range_copy(rewritten, right);
            if (!ok) {
                break;
            }
        }
    }
    if (ok) {
        ranges_to_release = std::move(task->lazy_vmem_ranges);
        task->lazy_vmem_ranges = std::move(rewritten);
    } else {
        ranges_to_release = std::move(rewritten);
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    release_lazy_vmem_range_files(ranges_to_release);
    return ok;
}

auto add_lazy_vmem_range_entry(ker::mod::sched::task::Task* task, const LazyVmemRange& new_range) -> bool {
    if (task == nullptr || new_range.start >= new_range.end) {
        return false;
    }

    LazyVmemRangeVec rewritten;
    LazyVmemRangeVec ranges_to_release;
    bool ok = true;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    // Shared-VM updates can still find an exiting ACTIVE executor. Serialize
    // this check with range retirement so a sibling cannot repopulate its
    // lazy-file references after usercopy has quiesced.
    if (task->usercopy_pagemap_closing.load(std::memory_order_acquire)) {
        task->lazy_vmem_lock.unlock_irqrestore(IRQF);
        return true;
    }
    for (const auto& range : task->lazy_vmem_ranges) {
        if (!ranges_overlap(new_range.start, new_range.end, range.start, range.end)) {
            ok = push_lazy_vmem_range_copy(rewritten, range);
            if (!ok) {
                break;
            }
            continue;
        }

        if (range.start < new_range.start) {
            LazyVmemRange left;
            ok = slice_lazy_vmem_range(range, range.start, new_range.start, left) && push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }
        if (new_range.end < range.end) {
            LazyVmemRange right;
            ok = slice_lazy_vmem_range(range, new_range.end, range.end, right) && push_lazy_vmem_range_copy(rewritten, right);
            if (!ok) {
                break;
            }
        }
    }
    if (ok) {
        ok = push_lazy_vmem_range_copy(rewritten, new_range);
    }
    if (ok) {
        ranges_to_release = std::move(task->lazy_vmem_ranges);
        task->lazy_vmem_ranges = std::move(rewritten);
    } else {
        ranges_to_release = std::move(rewritten);
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    release_lazy_vmem_range_files(ranges_to_release);
    return ok;
}

auto add_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return false;
    }

    LazyVmemRange const RANGE{.start = vaddr, .end = vaddr + size, .prot = prot, .flags = flags};
    return add_lazy_vmem_range_entry(task, RANGE);
}

auto add_lazy_file_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags,
                              ker::vfs::File* file, uint64_t file_offset, const ker::vfs::Stat& st) -> bool {
    if (task == nullptr || file == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return false;
    }

    LazyVmemRange const RANGE{.start = vaddr,
                              .end = vaddr + size,
                              .prot = prot,
                              .flags = flags,
                              .kind = LazyVmemKind::FILE_BACKED,
                              .file = file,
                              .file_offset = file_offset,
                              .file_dev = st.st_dev,
                              .file_ino = st.st_ino,
                              .file_size = st.st_size > 0 ? static_cast<uint64_t>(st.st_size) : 0,
                              .file_mtime_sec = st.st_mtim.tv_sec,
                              .file_mtime_nsec = st.st_mtim.tv_nsec,
                              .file_ctime_sec = st.st_ctim.tv_sec,
                              .file_ctime_nsec = st.st_ctim.tv_nsec};
    return add_lazy_vmem_range_entry(task, RANGE);
}

auto protect_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return true;
    }

    uint64_t const END = vaddr + size;
    LazyVmemRangeVec rewritten;
    LazyVmemRangeVec ranges_to_release;
    bool ok = true;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (!ranges_overlap(vaddr, END, range.start, range.end)) {
            ok = push_lazy_vmem_range_copy(rewritten, range);
            if (!ok) {
                break;
            }
            continue;
        }

        if (range.start < vaddr) {
            LazyVmemRange left;
            ok = slice_lazy_vmem_range(range, range.start, vaddr, left) && push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }

        LazyVmemRange middle;
        if (!slice_lazy_vmem_range(range, std::max(range.start, vaddr), std::min(range.end, END), middle)) {
            ok = false;
            break;
        }
        middle.prot = prot;
        ok = push_lazy_vmem_range_copy(rewritten, middle);
        if (!ok) {
            break;
        }

        if (END < range.end) {
            LazyVmemRange right;
            ok = slice_lazy_vmem_range(range, END, range.end, right) && push_lazy_vmem_range_copy(rewritten, right);
            if (!ok) {
                break;
            }
        }
    }
    if (ok) {
        ranges_to_release = std::move(task->lazy_vmem_ranges);
        task->lazy_vmem_ranges = std::move(rewritten);
    } else {
        ranges_to_release = std::move(rewritten);
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    release_lazy_vmem_range_files(ranges_to_release);
    return ok;
}

template <typename Fn>
auto update_shared_vmem_ranges_locked(ker::mod::sched::task::Task* task, Fn update) -> bool {
    if (task == nullptr || task->pagemap == nullptr) {
        return update(task);
    }
    if (!task->shares_user_pagemap.load(std::memory_order_acquire)) {
        return update(task);
    }

    auto* const PAGEMAP = task->pagemap;
    auto const MATCH_SIBLING = [](ker::mod::sched::task::Task* candidate, void* context) -> bool {
        auto* const PAGEMAP = static_cast<ker::mod::mm::paging::PageTable*>(context);
        return candidate != nullptr && candidate->pagemap == PAGEMAP && candidate->type != ker::mod::sched::task::TaskType::DAEMON;
    };

    constexpr size_t INLINE_TASKS = 32;
    std::array<ker::mod::sched::task::Task*, INLINE_TASKS> inline_tasks{};
    ker::mod::sched::task::Task** tasks = inline_tasks.data();
    ker::mod::sched::task::Task** heap_tasks = nullptr;
    size_t capacity = inline_tasks.size();
    size_t task_count = 0;

    // Snapshot the registry under one lock acquisition so removal cannot swap
    // an unvisited sibling behind an index. Shared-VM publication is serialized
    // by the caller; the retry only handles a generic capacity race.
    for (;;) {
        task_count = ker::mod::sched::snapshot_active_task_lifetime_refs_if(MATCH_SIBLING, PAGEMAP, tasks, capacity);
        if (task_count <= capacity) {
            break;
        }
        auto** larger = new (std::nothrow) ker::mod::sched::task::Task*[task_count];
        if (larger == nullptr) {
            delete[] heap_tasks;
            return false;
        }
        delete[] heap_tasks;
        heap_tasks = larger;
        tasks = heap_tasks;
        capacity = task_count;
    }

    bool touched_current = false;
    bool ok = true;
    for (size_t i = 0; i < task_count; ++i) {
        auto* candidate = tasks[i];
        touched_current = touched_current || candidate == task;
        ok = update(candidate) && ok;
        candidate->release();
    }
    delete[] heap_tasks;

    if (!touched_current) {
        ok = update(task) && ok;
    }
    return ok;
}

auto remove_shared_vmem_range_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) -> bool {
    return update_shared_vmem_ranges_locked(
        task, [vaddr, size](ker::mod::sched::task::Task* candidate) { return remove_lazy_vmem_range(candidate, vaddr, size); });
}

auto add_shared_vmem_range_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags) -> bool {
    bool const ADDED = update_shared_vmem_ranges_locked(task, [vaddr, size, prot, flags](ker::mod::sched::task::Task* candidate) {
        return add_lazy_vmem_range(candidate, vaddr, size, prot, flags);
    });
    if (!ADDED) {
        (void)update_shared_vmem_ranges_locked(
            task, [vaddr, size](ker::mod::sched::task::Task* candidate) { return remove_lazy_vmem_range(candidate, vaddr, size); });
    }
    return ADDED;
}

auto add_shared_lazy_file_vmem_range_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags,
                                            ker::vfs::File* file, uint64_t file_offset, const ker::vfs::Stat& st) -> bool {
    bool const ADDED =
        update_shared_vmem_ranges_locked(task, [vaddr, size, prot, flags, file, file_offset, &st](ker::mod::sched::task::Task* candidate) {
            return add_lazy_file_vmem_range(candidate, vaddr, size, prot, flags, file, file_offset, st);
        });
    if (!ADDED) {
        (void)update_shared_vmem_ranges_locked(
            task, [vaddr, size](ker::mod::sched::task::Task* candidate) { return remove_lazy_vmem_range(candidate, vaddr, size); });
    }
    return ADDED;
}

auto protect_shared_vmem_range_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    return update_shared_vmem_ranges_locked(task, [vaddr, size, prot](ker::mod::sched::task::Task* candidate) {
        return protect_lazy_vmem_range(candidate, vaddr, size, prot);
    });
}

auto file_mmap_cache_lookup(const FileMmapPageKey& key) -> void* {
    size_t const SET_INDEX = file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1);
    size_t const LOCK_INDEX = SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1);
    auto& cache_lock = g_file_mmap_cache_locks.at(LOCK_INDEX);
    auto& set = g_file_mmap_cache.at(SET_INDEX);

    cache_lock.mutex.lock();
    uint64_t const USE_STAMP = ++cache_lock.clock;
    for (auto& entry : set.ways) {
        if (entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            entry.last_used = USE_STAMP;
            ker::mod::mm::phys::page_ref_inc(entry.page);
            void* page = entry.page;
            cache_lock.mutex.unlock();
            return page;
        }
    }
    cache_lock.mutex.unlock();

    return nullptr;
}

void release_file_mmap_cache_page(void* page) {
    if (page == nullptr) {
        return;
    }
    if (ker::mod::mm::phys::page_ref_get(page) > 1 &&
        !ker::mod::mm::phys::page_reassign_owner(page, ker::mod::mm::PhysicalPageOwner::USER_FILE_MAPPING)) {
        ker::mod::dbg::panic_handler("file mmap cache could not transfer evicted page ownership");
    }
    ker::mod::mm::phys::page_ref_dec(page);
}

auto file_mmap_cache_insert_or_discard(const FileMmapPageKey& key, void* new_page, void** page_for_mapping) -> FileMmapCacheInsertResult {
    size_t const SET_INDEX = file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1);
    size_t const LOCK_INDEX = SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1);
    auto& cache_lock = g_file_mmap_cache_locks.at(LOCK_INDEX);
    auto& set = g_file_mmap_cache.at(SET_INDEX);

    cache_lock.mutex.lock();
    uint64_t const USE_STAMP = ++cache_lock.clock;
    for (auto& entry : set.ways) {
        if (entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            entry.last_used = USE_STAMP;
            ker::mod::mm::phys::page_ref_inc(entry.page);
            *page_for_mapping = entry.page;
            cache_lock.mutex.unlock();
            ker::mod::mm::phys::page_ref_dec(new_page);
            return FileMmapCacheInsertResult::DUPLICATE;
        }
    }

    auto* victim = &set.ways.front();
    for (auto& entry : set.ways) {
        if (entry.page == nullptr) {
            victim = &entry;
            break;
        }
        if (entry.last_used < victim->last_used) {
            victim = &entry;
        }
    }

    void* evicted = victim->page;
    victim->key = key;
    victim->page = new_page;
    victim->last_used = USE_STAMP;
    ker::mod::mm::phys::page_ref_inc(new_page);
    *page_for_mapping = new_page;
    if (evicted == nullptr) {
        g_file_mmap_cache_pages.fetch_add(1, std::memory_order_relaxed);
    }
    cache_lock.mutex.unlock();

    if (evicted != nullptr) {
        release_file_mmap_cache_page(evicted);
        return FileMmapCacheInsertResult::EVICTED;
    }

    return FileMmapCacheInsertResult::INSERTED;
}

auto read_file_mapping_exact(ker::vfs::File* file, void* dst, size_t count, uint64_t file_offset) -> bool {
    if (file == nullptr || dst == nullptr) {
        return false;
    }

    auto* out = static_cast<uint8_t*>(dst);
    size_t done = 0;
    while (done < count) {
        if (file_offset > UINT64_MAX - done) {
            return false;
        }
        uint64_t const CURRENT_OFFSET = file_offset + done;
        if (CURRENT_OFFSET > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            return false;
        }

        ssize_t const READ_RET = ker::vfs::vfs_pread_file_direct(file, out + done, count - done, static_cast<off_t>(CURRENT_OFFSET));
        if (READ_RET < 0) {
            return false;
        }
        if (READ_RET == 0) {
            log::warn("file mmap short read: path=%s off=%llu got=%zu want=%zu", file->vfs_path != nullptr ? file->vfs_path : "<pathless>",
                      static_cast<unsigned long long>(file_offset), done, count);
            return false;
        }

        done += std::min<size_t>(static_cast<size_t>(READ_RET), count - done);
    }

    return true;
}

auto file_mmap_cached_page_for_file(ker::vfs::File* file, const ker::vfs::Stat& st, uint64_t file_offset, void** page_out) -> bool {
    if (page_out == nullptr) {
        return false;
    }
    if (file == nullptr) {
        return false;
    }

    // Clock reads can require HPET MMIO. Only sample cache diagnostics when
    // recording is active; the recorder otherwise discards these events.
    bool const TRACE_CACHE = ker::mod::perf::is_wki_recording_enabled();
    auto* task = TRACE_CACHE ? get_current_task() : nullptr;
    FileMmapPageKey key = file_mmap_page_key(st, file_offset);
    if (file->exec_read_view != nullptr) key.exec_content_id = file->exec_read_view->content_id;
    FileMmapPageKey const KEY = key;
    uint64_t const LOOKUP_STARTED_US = TRACE_CACHE ? ker::mod::time::get_us() : 0;
    if (void* const CACHED_PAGE = file_mmap_cache_lookup(KEY); CACHED_PAGE != nullptr) {
        *page_out = CACHED_PAGE;
        if (TRACE_CACHE) {
            record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_HIT, file_offset,
                                          vmem_latency_since(LOOKUP_STARTED_US), true);
        }
        return true;
    }
    if (TRACE_CACHE) {
        record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_MISS, file_offset,
                                      vmem_latency_since(LOOKUP_STARTED_US), true);
    }

    uint64_t const FILE_SIZE = KEY.size;
    bool const FULL_PAGE_READ =
        file_offset < FILE_SIZE && FILE_SIZE - file_offset >= static_cast<uint64_t>(ker::mod::mm::paging::PAGE_SIZE);
    void* const NEW_PAGE = FULL_PAGE_READ
                               ? ker::mod::mm::phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(
                                     ker::mod::mm::PhysicalPageOwner::USER_FILE_CACHE, "vmem-file-cache")
                               : ker::mod::mm::phys::page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_FILE_CACHE,
                                                                                      ker::mod::mm::paging::PAGE_SIZE, "vmem-file-cache");
    if (NEW_PAGE == nullptr) {
        return false;
    }
    if (!FULL_PAGE_READ) {
        std::memset(NEW_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);
    }

    uint64_t const FILL_STARTED_US = TRACE_CACHE ? ker::mod::time::get_us() : 0;
    if (file_offset < FILE_SIZE) {
        size_t const READ_SIZE = static_cast<size_t>(std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - file_offset));
        if (file_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            ker::mod::mm::phys::page_ref_dec(NEW_PAGE);
            return false;
        }
        if (!read_file_mapping_exact(file, NEW_PAGE, READ_SIZE, file_offset)) {
            ker::mod::mm::phys::page_ref_dec(NEW_PAGE);
            return false;
        }
    }

    FileMmapCacheInsertResult const INSERT_RESULT = file_mmap_cache_insert_or_discard(KEY, NEW_PAGE, page_out);
    if (TRACE_CACHE) {
        uint32_t const FILL_US = vmem_latency_since(FILL_STARTED_US);
        if (INSERT_RESULT == FileMmapCacheInsertResult::DUPLICATE) {
            record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_HIT, file_offset, FILL_US, true);
        } else {
            record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_FILL, file_offset, FILL_US, true);
            if (INSERT_RESULT == FileMmapCacheInsertResult::EVICTED) {
                record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_EVICT, file_offset, 0, false);
            }
        }
    }
    return true;
}

auto map_file_mmap_cached_page(ker::mod::mm::virt::PageMapBatch* batch, ker::vfs::File* file, const ker::vfs::Stat& st, uint64_t address,
                               uint64_t offset, uint64_t flags, uint64_t& physical) -> bool {
    void* page = nullptr;
    if (!file_mmap_cached_page_for_file(file, st, offset, &page)) {
        return false;
    }
    auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    ker::mod::mm::virt::map_page_batched(batch, address, PADDR, flags);
    physical = PADDR;
    return true;
}

auto collect_file_mmap_sync_ranges(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length,
                                   ker::util::SmallVec<FileMmapSyncRange, 8>& out) -> int {
    uint64_t end = 0;
    if (pagemap == nullptr || !checked_range_end(start, length, &end)) {
        return -EINVAL;
    }

    g_file_mmap_ranges_lock.lock();
    for (const auto& range : g_file_mmap_ranges) {
        if (range.pagemap != pagemap || range.file == nullptr || range.length == 0 || !range.writeback) {
            continue;
        }

        uint64_t const RANGE_END = file_mmap_range_end(range);
        if (!ranges_overlap(start, end, range.start, RANGE_END)) {
            continue;
        }

        FileMmapSyncRange sync_range{
            .mapping_start = range.start,
            .sync_start = std::max(start, range.start),
            .sync_end = std::min(end, RANGE_END),
            .file_offset = range.file_offset,
            .file = range.file,
        };
        ker::vfs::vfs_retain_file(sync_range.file);
        if (!out.push_back(sync_range)) {
            ker::vfs::vfs_put_file(sync_range.file);
            g_file_mmap_ranges_lock.unlock();
            return -ENOMEM;
        }
    }
    g_file_mmap_ranges_lock.unlock();
    return 0;
}

auto authorize_file_mapping_writeback(ker::vfs::File* file, size_t count, uint64_t offset) -> int {
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        return -ENOSYS;
    }

    if (count == 0) return 0;
    constexpr uint64_t MAX_OFFSET = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    // Validate the whole range before a short first write can commit a prefix.
    if (offset > MAX_OFFSET || count - 1 > MAX_OFFSET - offset) return -EOVERFLOW;

    int const ACCESS_RET = ker::mod::security::authorize_file_description_access(static_cast<unsigned>(file->open_flags & 3), true);
    if (ACCESS_RET != 0) return ACCESS_RET;
    return ker::vfs::file_mount_read_only(*file) ? -EROFS : 0;
}

auto write_file_mapping_bytes(ker::vfs::File* file, const uint8_t* data, size_t count, uint64_t offset) -> int {
    int const ADMISSION_RET = authorize_file_mapping_writeback(file, count, offset);
    if (ADMISSION_RET != 0) return ADMISSION_RET;

    size_t written = 0;
    while (written < count) {
        uint64_t const CURRENT_OFFSET = offset + written;
        ssize_t const RET = ker::vfs::vfs_pwrite_file(file, data + written, count - written, static_cast<off_t>(CURRENT_OFFSET));
        if (RET < 0) {
            return static_cast<int>(RET);
        }
        if (RET == 0) {
            return -EIO;
        }
        written += std::min<size_t>(static_cast<size_t>(RET), count - written);
    }

    return 0;
}

void release_file_mmap_sync_ranges(ker::util::SmallVec<FileMmapSyncRange, 8>& ranges, size_t first = 0) {
    for (size_t i = first; i < ranges.size(); ++i) {
        ker::vfs::vfs_put_file(ranges.at(i).file);
    }
}

enum class FileMmapPageSnapshot : uint8_t {
    SKIP,
    READY,
    FAULT,
};

auto snapshot_file_mmap_page(ker::mod::mm::paging::PageTable* pagemap, ker::mod::sched::task::Task* live_task, uint64_t page_start,
                             uint64_t page_delta, size_t size, std::array<uint8_t, ker::mod::mm::paging::PAGE_SIZE>& snapshot)
    -> FileMmapPageSnapshot {
    if (live_task != nullptr) {
        ker::mod::sys::usercopy::StableUserPage pin;
        if (!ker::mod::sys::usercopy::pin_task_user_page(*live_task, page_start, false, false, pin) || !pin.was_dirty()) {
            return FileMmapPageSnapshot::SKIP;
        }
        // Zero bytes probes dirty state without copying, with the same pin
        // lifetime and mapping revalidation as an actual data snapshot.
        if (size != 0) {
            ker::mod::sys::usercopy::UserAccessGuard const ACCESS{};
            std::memcpy(snapshot.data(), static_cast<const uint8_t*>(pin.kernel_address()) + page_delta, size);
        }
        return pin.still_mapped() ? FileMmapPageSnapshot::READY : FileMmapPageSnapshot::FAULT;
    }

    // A null live_task is allowed only for an unpublished or usercopy-quiesced
    // pagemap. The frame reference still keeps the snapshot storage alive while
    // the leaf is checked, copied, and revalidated.
    ker::mod::mm::virt::UserPagePin pin{};
    if (!ker::mod::mm::virt::pin_user_page(pagemap, page_start, false, pin, true)) {
        return FileMmapPageSnapshot::SKIP;
    }
    if (!pin.dirty) {
        ker::mod::mm::virt::unpin_user_page(pin);
        return FileMmapPageSnapshot::SKIP;
    }
    if (size != 0) {
        ker::mod::sys::usercopy::UserAccessGuard const ACCESS{};
        std::memcpy(snapshot.data(), static_cast<const uint8_t*>(pin.hhdm_page) + page_delta, size);
    }
    bool const STILL_MAPPED = ker::mod::mm::virt::user_page_pin_still_mapped(pin);
    ker::mod::mm::virt::unpin_user_page(pin);
    return STILL_MAPPED ? FileMmapPageSnapshot::READY : FileMmapPageSnapshot::FAULT;
}

// All caller-visible mappings are stable under shared VM publication, or the
// root is unpublished/quiesced. File grants and effective view policy are
// immutable. Validate complete intervals before any backend write. A rejected
// range with no dirty work is a no-op and remains excluded from this operation,
// even if a concurrent data writer dirties it after the probe.
auto preflight_file_mmap_sync_ranges(ker::mod::mm::paging::PageTable* pagemap, ker::mod::sched::task::Task* live_task,
                                     ker::util::SmallVec<FileMmapSyncRange, 8>& ranges) -> int {
    std::array<uint8_t, ker::mod::mm::paging::PAGE_SIZE> unused_snapshot{};
    for (auto& range : ranges) {
        if (range.sync_start < range.mapping_start || range.sync_end <= range.sync_start) return -EINVAL;
        uint64_t const FILE_DELTA = range.sync_start - range.mapping_start;
        int const ADMISSION_RET = FILE_DELTA > UINT64_MAX - range.file_offset
                                      ? -EOVERFLOW
                                      : authorize_file_mapping_writeback(range.file, static_cast<size_t>(range.sync_end - range.sync_start),
                                                                         range.file_offset + FILE_DELTA);
        if (ADMISSION_RET == 0) continue;

        for (uint64_t page = page_align_down(range.sync_start); page < range.sync_end;) {
            auto const STATE = snapshot_file_mmap_page(pagemap, live_task, page, 0, 0, unused_snapshot);
            if (STATE == FileMmapPageSnapshot::FAULT) return -EFAULT;
            if (STATE == FileMmapPageSnapshot::READY) return ADMISSION_RET;
            if (page > UINT64_MAX - ker::mod::mm::paging::PAGE_SIZE) return -EOVERFLOW;
            page += ker::mod::mm::paging::PAGE_SIZE;
        }
        range.skip_writeback = true;
    }
    return 0;
}

auto sync_file_mmap_range_impl(ker::mod::mm::paging::PageTable* pagemap, ker::mod::sched::task::Task* live_task, uint64_t start,
                               uint64_t length) -> int {
    ker::util::SmallVec<FileMmapSyncRange, 8> ranges;
    int const COLLECT_RET = collect_file_mmap_sync_ranges(pagemap, start, length, ranges);
    if (COLLECT_RET < 0) {
        release_file_mmap_sync_ranges(ranges);
        return COLLECT_RET;
    }

    int const PREFLIGHT_RET = preflight_file_mmap_sync_ranges(pagemap, live_task, ranges);
    if (PREFLIGHT_RET != 0) {
        release_file_mmap_sync_ranges(ranges);
        return PREFLIGHT_RET;
    }

    int result = 0;
    for (size_t range_index = 0; range_index < ranges.size(); ++range_index) {
        const auto& range = ranges.at(range_index);
        if (range.skip_writeback) {
            ker::vfs::vfs_put_file(range.file);
            continue;
        }
        uint64_t page_vaddr = page_align_down(range.sync_start);
        while (page_vaddr < range.sync_end) {
            uint64_t const PAGE_END = page_vaddr + ker::mod::mm::paging::PAGE_SIZE;
            uint64_t const PAGE_START = page_vaddr;
            uint64_t const WRITE_START = std::max(PAGE_START, range.sync_start);
            uint64_t const WRITE_END = std::min(PAGE_END, range.sync_end);
            std::array<uint8_t, ker::mod::mm::paging::PAGE_SIZE> snapshot{};
            bool write_snapshot = false;
            if (WRITE_START < WRITE_END) {
                uint64_t const PAGE_DELTA = WRITE_START - PAGE_START;
                size_t const WRITE_SIZE = static_cast<size_t>(WRITE_END - WRITE_START);
                auto const SNAPSHOT_RESULT = snapshot_file_mmap_page(pagemap, live_task, PAGE_START, PAGE_DELTA, WRITE_SIZE, snapshot);
                if (SNAPSHOT_RESULT == FileMmapPageSnapshot::FAULT) {
                    result = -EFAULT;
                    break;
                }
                write_snapshot = SNAPSHOT_RESULT == FileMmapPageSnapshot::READY;
            }

            if (write_snapshot) {
                uint64_t const FILE_DELTA = WRITE_START - range.mapping_start;
                uint64_t const FILE_OFFSET = range.file_offset + FILE_DELTA;
                if (FILE_OFFSET < range.file_offset) {
                    result = -EOVERFLOW;
                    break;
                }
                result = write_file_mapping_bytes(range.file, snapshot.data(), static_cast<size_t>(WRITE_END - WRITE_START), FILE_OFFSET);
                if (result < 0) {
                    break;
                }
            }

            if (PAGE_END <= page_vaddr) {
                result = -EOVERFLOW;
                break;
            }
            page_vaddr = PAGE_END;
        }

        ker::vfs::vfs_put_file(range.file);
        if (result < 0) {
            release_file_mmap_sync_ranges(ranges, range_index + 1);
            return result;
        }
    }

    return 0;
}

auto sync_file_mmap_range(ker::mod::sched::task::Task& task, uint64_t start, uint64_t length) -> int {
    return sync_file_mmap_range_impl(task.pagemap, &task, start, length);
}

auto sync_file_mmap_range_quiesced(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length) -> int {
    return sync_file_mmap_range_impl(pagemap, nullptr, start, length);
}

// The caller lends an independently retained File. Only successful registry
// publication acquires a new reference; no final close can run under its lock.
auto register_file_mmap_range(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length, uint64_t file_offset,
                              ker::vfs::File* file, bool writeback) -> int {
    if (pagemap == nullptr || file == nullptr || length == 0 || start > UINT64_MAX - length) return -EINVAL;
    FileMmapRange const RANGE{
        .pagemap = pagemap,
        .start = start,
        .length = length,
        .file_offset = file_offset,
        .file = file,
        .writeback = writeback,
    };
    g_file_mmap_ranges_lock.lock();
    bool const OK = g_file_mmap_ranges.push_back(RANGE);
    if (OK) ker::vfs::vfs_retain_file(file);
    g_file_mmap_ranges_lock.unlock();
    return OK ? 0 : -ENOMEM;
}

// Caller holds SharedVmemPublicationGuard. Registry ownership pins every File
// while this allocation-free preflight reads its immutable admitted policy.
auto authorize_shared_file_protection_locked(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length, uint64_t prot)
    -> int {
    if ((prot & ker::abi::vmem::PROT_WRITE) == 0) return 0;
    g_file_mmap_ranges_lock.lock();
    int result = 0;
    for (const auto& range : g_file_mmap_ranges) {
        if (range.pagemap != pagemap || !ranges_overlap(start, start + length, range.start, file_mmap_range_end(range))) continue;
        auto* file = range.file;
        if (file == nullptr || ker::mod::security::authorize_file_mapping(static_cast<unsigned>(file->open_flags & 3), true, true,
                                                                          ker::vfs::file_mount_read_only(*file)) != 0) {
            result = -EACCES;
            break;
        }
    }
    g_file_mmap_ranges_lock.unlock();
    return result;
}

auto unregister_one_file_mmap_overlap(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t end, bool* removed_any) -> int {
    if (removed_any != nullptr) {
        *removed_any = false;
    }

    g_file_mmap_ranges_lock.lock();
    for (size_t i = 0; i < g_file_mmap_ranges.size(); ++i) {
        auto& range = g_file_mmap_ranges.at(i);
        if (range.pagemap != pagemap || range.length == 0) {
            continue;
        }

        uint64_t const RANGE_END = file_mmap_range_end(range);
        if (!ranges_overlap(start, end, range.start, RANGE_END)) {
            continue;
        }

        uint64_t const OVERLAP_START = std::max(start, range.start);
        uint64_t const OVERLAP_END = std::min(end, RANGE_END);
        if (OVERLAP_START <= range.start && OVERLAP_END >= RANGE_END) {
            auto* file = range.file;
            (void)g_file_mmap_ranges.remove_at(i);
            g_file_mmap_ranges_lock.unlock();
            ker::vfs::vfs_put_file(file);
            if (removed_any != nullptr) {
                *removed_any = true;
            }
            return 0;
        }

        if (OVERLAP_START <= range.start) {
            uint64_t const DELTA = OVERLAP_END - range.start;
            range.start = OVERLAP_END;
            range.length = RANGE_END - OVERLAP_END;
            range.file_offset += DELTA;
            g_file_mmap_ranges_lock.unlock();
            if (removed_any != nullptr) {
                *removed_any = true;
            }
            return 0;
        }

        if (OVERLAP_END >= RANGE_END) {
            range.length = OVERLAP_START - range.start;
            g_file_mmap_ranges_lock.unlock();
            if (removed_any != nullptr) {
                *removed_any = true;
            }
            return 0;
        }

        FileMmapRange right = range;
        right.start = OVERLAP_END;
        right.length = RANGE_END - OVERLAP_END;
        right.file_offset = range.file_offset + (OVERLAP_END - range.start);
        ker::vfs::vfs_retain_file(right.file);
        if (!g_file_mmap_ranges.push_back(right)) {
            ker::vfs::vfs_put_file(right.file);
            g_file_mmap_ranges_lock.unlock();
            return -ENOMEM;
        }
        range.length = OVERLAP_START - range.start;
        g_file_mmap_ranges_lock.unlock();
        if (removed_any != nullptr) {
            *removed_any = true;
        }
        return 0;
    }

    g_file_mmap_ranges_lock.unlock();
    return 0;
}

auto unregister_file_mmap_range(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length) -> int {
    uint64_t end = 0;
    if (pagemap == nullptr || !checked_range_end(start, length, &end)) {
        return -EINVAL;
    }

    for (;;) {
        bool removed_any = false;
        int const RET = unregister_one_file_mmap_overlap(pagemap, start, end, &removed_any);
        if (RET < 0 || !removed_any) {
            return RET;
        }
    }
}

auto get_anon_zero_page() -> void* {
    void* const EXISTING = g_anon_zero_page.load(std::memory_order_acquire);
    if (EXISTING != nullptr) {
        return EXISTING;
    }

    void* const CANDIDATE = ker::mod::mm::phys::page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::ANON_ZERO_PAGE_RESERVE,
                                                                                 ker::mod::mm::paging::PAGE_SIZE, "vmem-zero");
    if (CANDIDATE == nullptr) {
        return nullptr;
    }
    std::memset(CANDIDATE, 0, ker::mod::mm::paging::PAGE_SIZE);

    void* expected = nullptr;
    if (g_anon_zero_page.compare_exchange_strong(expected, CANDIDATE, std::memory_order_release, std::memory_order_acquire)) {
        ker::mod::perf::register_local_vmem_zero_page(CANDIDATE);
        return CANDIDATE;
    }

    ker::mod::mm::phys::page_ref_dec(CANDIDATE);
    ker::mod::perf::register_local_vmem_zero_page(expected);
    return expected;
}

auto mmap_range_is_free(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) -> bool {
    if (task == nullptr || task->pagemap == nullptr || vaddr < USER_SPACE_START || size == 0) {
        return false;
    }
    if (size > USER_SPACE_END - USER_SPACE_START || vaddr > USER_SPACE_END - size) {
        return false;
    }

    return !first_occupied_range(task, vaddr, vaddr + size).found;
}

auto find_free_range_from(ker::mod::sched::task::Task* task, uint64_t size, uint64_t start) -> uint64_t {
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return 0;
    }

    uint64_t current_addr = page_align_up(start);
    while (current_addr <= USER_SPACE_END - size) {
        auto const OCCUPIED = first_occupied_range(task, current_addr, current_addr + size);
        if (!OCCUPIED.found) {
            return current_addr;
        }

        uint64_t const NEXT_ADDR = page_align_up(OCCUPIED.end);
        if (NEXT_ADDR <= current_addr) {
            return 0;
        }
        current_addr = NEXT_ADDR;
    }

    return 0;
}

void advance_mmap_cursor(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {
    if (task == nullptr || size == 0 || size > USER_SPACE_END - USER_SPACE_START || vaddr > USER_SPACE_END - size) {
        return;
    }

    uint64_t const END = page_align_up(vaddr + size);
    if (END < MMAP_START) {
        return;
    }

    uint64_t cursor = task->mmap_next.load(std::memory_order_relaxed);
    while (END > cursor && !task->mmap_next.compare_exchange_weak(cursor, END, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void advance_shared_mmap_cursor_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {
    (void)update_shared_vmem_ranges_locked(task, [vaddr, size](ker::mod::sched::task::Task* candidate) {
        advance_mmap_cursor(candidate, vaddr, size);
        return true;
    });
}

// Find a free virtual address range of the given size
// Uses a simple linear search through allocated regions
auto find_free_range(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint) -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr) {
        return 0;
    }

    // Align size to page boundary
    size = page_align_up(size);
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return 0;
    }

    // If hint is provided and valid, try to use it
    if (hint >= USER_SPACE_START && hint <= USER_SPACE_END - size) {
        if (mmap_range_is_free(task, hint, size)) {
            return hint;
        }
    }

    uint64_t cursor = task->mmap_next.load(std::memory_order_relaxed);
    if (cursor < MMAP_START || cursor > USER_SPACE_END - size) {
        cursor = MMAP_START;
    }

    if (uint64_t const FOUND = find_free_range_from(task, size, cursor); FOUND != 0) {
        return FOUND;
    }
    if (cursor != MMAP_START) {
        return find_free_range_from(task, size, MMAP_START);
    }

    return 0;  // No free range found
}

auto reserve_free_mmap_range_locked(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint, uint64_t& out_vaddr) -> uint64_t {
    out_vaddr = 0;

    uint64_t const VADDR = find_free_range(task, size, hint);
    if (VADDR == 0) {
        return ker::abi::vmem::VMEM_ENOMEM;
    }

    // Reserve the chosen range before returning it to the mapper. This closes
    // the check-then-map race between sibling threads that share a pagemap.
    bool const RESERVED = update_shared_vmem_ranges_locked(
        task, [VADDR, size](ker::mod::sched::task::Task* candidate) { return add_lazy_vmem_range(candidate, VADDR, size, 0, 0); });
    if (!RESERVED) {
        (void)update_shared_vmem_ranges_locked(
            task, [VADDR, size](ker::mod::sched::task::Task* candidate) { return remove_lazy_vmem_range(candidate, VADDR, size); });
        return ker::abi::vmem::VMEM_ENOMEM;
    }

    out_vaddr = VADDR;
    return 0;
}

void release_mmap_reservation_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, bool reserved) {
    if (reserved) {
        (void)remove_shared_vmem_range_locked(task, vaddr, size);
    }
}

// Convert protection flags to page table flags
auto prot_to_page_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = ker::mod::mm::paging::PAGE_PRESENT;

    // A present supervisor-only leaf is WOS's reversible PROT_NONE encoding.
    // Leaving PAGE_USER set would allow reads despite a zero protection mask.
    if (prot != 0) {
        flags |= ker::mod::mm::paging::PAGE_USER;
    }

    if ((prot & ker::abi::vmem::PROT_WRITE) != 0) {
        flags |= ker::mod::mm::paging::PAGE_WRITE;
    }

    // If not executable, set NX bit
    if ((prot & ker::abi::vmem::PROT_EXEC) == 0) {
        flags |= ker::mod::mm::paging::PAGE_NX;
    }

    return flags;
}

auto file_mapping_page_flags(uint64_t prot, uint64_t mmap_flags) -> uint64_t {
    uint64_t flags = prot_to_page_flags(prot);
    if ((mmap_flags & ker::abi::vmem::MAP_SHARED) != 0) {
        flags |= ker::mod::mm::paging::PAGE_SHARED;
    }
    return flags;
}

auto materialize_lazy_file_page_impl(ker::mod::sched::task::Task* task, const LazyVmemRange& range, uint64_t page_vaddr,
                                     const ker::mod::mm::paging::PageFault& fault) -> bool {
    if (task == nullptr || task->pagemap == nullptr || range.kind != LazyVmemKind::FILE_BACKED || range.file == nullptr) {
        return false;
    }
    if (page_vaddr < range.start || page_vaddr >= range.end || (page_vaddr & (ker::mod::mm::paging::PAGE_SIZE - 1)) != 0) {
        return false;
    }
    if (range.prot == 0 || (((range.prot & ker::abi::vmem::PROT_WRITE) == 0) && fault.writable != 0U)) {
        return false;
    }

    uint64_t const DELTA = page_vaddr - range.start;
    if (range.file_offset > UINT64_MAX - DELTA) {
        return false;
    }
    uint64_t const FILE_OFFSET = range.file_offset + DELTA;

    ker::vfs::Stat st{};
    st.st_dev = range.file_dev;
    st.st_ino = range.file_ino;
    st.st_size = static_cast<off_t>(range.file_size);
    st.st_mtim.tv_sec = range.file_mtime_sec;
    st.st_mtim.tv_nsec = range.file_mtime_nsec;
    st.st_ctim.tv_sec = range.file_ctime_sec;
    st.st_ctim.tv_nsec = range.file_ctime_nsec;
    st.st_mode = ker::vfs::S_IFREG;

    void* page = nullptr;
    bool const USE_SHARED_CACHE = (range.prot & ker::abi::vmem::PROT_WRITE) == 0 && file_mmap_can_share(st, range.prot);
    if (USE_SHARED_CACHE) {
        if (!file_mmap_cached_page_for_file(range.file, st, FILE_OFFSET, &page)) {
            return false;
        }
    } else {
        page = ker::mod::mm::phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_FILE_MAPPING,
                                                                                        "vmem-file-lazy");
        if (page == nullptr) {
            return false;
        }
        std::memset(page, 0, ker::mod::mm::paging::PAGE_SIZE);
        if (FILE_OFFSET < range.file_size) {
            size_t const READ_SIZE =
                static_cast<size_t>(std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, range.file_size - FILE_OFFSET));
            if (FILE_OFFSET > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                ker::mod::mm::phys::page_ref_dec(page);
                return false;
            }
            if (!read_file_mapping_exact(range.file, page, READ_SIZE, FILE_OFFSET)) {
                ker::mod::mm::phys::page_ref_dec(page);
                return false;
            }
        }
    }

    auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    auto const INSTALL_RESULT = ker::mod::mm::virt::install_lazy_file_page_if_current(task, range, page_vaddr, PADDR,
                                                                                      file_mapping_page_flags(range.prot, range.flags));
    if (INSTALL_RESULT != ker::mod::mm::virt::LazyFilePageInstallResult::MAPPED) {
        ker::mod::mm::phys::page_ref_dec(page);
        return INSTALL_RESULT == ker::mod::mm::virt::LazyFilePageInstallResult::ALREADY_MAPPED;
    }
    return true;
}

auto anon_zero_page_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = prot_to_page_flags(prot);

    // A shared zero page stays read-only; only an admitted writable mapping
    // may resolve its first write by allocating a private page.
    flags &= ~ker::mod::mm::paging::PAGE_WRITE;
    if ((prot & ker::abi::vmem::PROT_WRITE) != 0U) {
        flags |= ker::mod::mm::paging::PAGE_COW;
    }
    return flags;
}

auto materialize_reserved_page(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t prot) -> bool {
    if (task == nullptr || task->pagemap == nullptr) {
        return false;
    }
    if (is_prot_none(prot)) {
        return true;
    }

    if ((prot & ker::abi::vmem::PROT_WRITE) != 0) {
        void* const ZERO_PAGE = get_anon_zero_page();
        if (ZERO_PAGE == nullptr) {
            return false;
        }
        ker::mod::mm::phys::page_ref_inc(ZERO_PAGE);
        auto const ZERO_PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(ZERO_PAGE)));
        ker::mod::mm::virt::map_page(task->pagemap, vaddr, ZERO_PADDR, anon_zero_page_flags(prot));
        return true;
    }

    void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                                                                 ker::mod::mm::paging::PAGE_SIZE, "vmem-reserve");
    if (PHYS_PAGE == nullptr) {
        return false;
    }
    std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

    auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));
    ker::mod::mm::virt::map_page(task->pagemap, vaddr, PADDR, prot_to_page_flags(prot));
    return true;
}

void rollback_mapped_pages(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t mapped_pages);

void release_fixed_mmap_range_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {
    if (task == nullptr || task->pagemap == nullptr || size == 0) {
        return;
    }

    int const SYNC_RET = sync_file_mmap_range(*task, vaddr, size);
    if (SYNC_RET < 0) {
        log::warn("MAP_FIXED replacement sync failed: pid=%lu vaddr=0x%llx size=0x%llx ret=%d", task->pid,
                  static_cast<unsigned long long>(vaddr), static_cast<unsigned long long>(size), SYNC_RET);
    }
    int const UNREGISTER_RET = unregister_file_mmap_range(task->pagemap, vaddr, size);
    if (UNREGISTER_RET < 0) {
        log::warn("MAP_FIXED replacement unregister failed: pid=%lu vaddr=0x%llx size=0x%llx ret=%d", task->pid,
                  static_cast<unsigned long long>(vaddr), static_cast<unsigned long long>(size), UNREGISTER_RET);
    }

    (void)remove_shared_vmem_range_locked(task, vaddr, size);

    uint64_t const END = vaddr + size;
    uint64_t cursor = vaddr;
    while (cursor < END) {
        auto const OCCUPIED = first_pagemap_occupied_range(task->pagemap, cursor, END);
        if (!OCCUPIED.found) {
            return;
        }

        for (uint64_t current_vaddr = OCCUPIED.start; current_vaddr < OCCUPIED.end; current_vaddr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::is_page_mapped_or_reserved(task->pagemap, current_vaddr)) {
                ker::mod::mm::virt::unmap_page(task->pagemap, current_vaddr);
            }
        }

        uint64_t const NEXT = page_align_up(OCCUPIED.end);
        if (NEXT <= cursor) {
            return;
        }
        cursor = NEXT;
    }
}

auto private_anon_allocate_locked(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t hint,
                                  uint64_t flags) -> uint64_t {
    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    uint64_t mapped_pages = 0;

    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                                                                     ker::mod::mm::paging::PAGE_SIZE, "vmem-anon");
        if (PHYS_PAGE == nullptr) {
            log::error("out of physical memory after mapping %llu/%llu anon pages", static_cast<unsigned long long>(mapped_pages),
                       static_cast<unsigned long long>(NUM_PAGES));
            rollback_mapped_pages(task, vaddr, mapped_pages);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

        auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, PADDR, PAGE_FLAGS);
        mapped_pages++;

        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=anon vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(PADDR), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot));
        }
    }

    if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags)) {
        rollback_mapped_pages(task, vaddr, mapped_pages);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    advance_shared_mmap_cursor_locked(task, vaddr, size);
    return vaddr;
}

void rollback_mapped_pages(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t mapped_pages) {
    if (task == nullptr || task->pagemap == nullptr) {
        return;
    }

    for (uint64_t i = 0; i < mapped_pages; i++) {
        uint64_t const CURRENT_VADDR = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR)) {
            ker::mod::mm::virt::unmap_page(task->pagemap, CURRENT_VADDR);
        }
    }
}

// Allocate anonymous memory
auto anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags) -> uint64_t {
    // Get current task
    auto* task = get_current_task();
    if (task == nullptr) {
        ker::mod::dbg::error("vmem: no current task");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        ker::mod::dbg::error("vmem: task has no pagemap");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate pagemap pointer is in valid HHDM range (not kernel static range)
    auto pm_addr = reinterpret_cast<uintptr_t>(task->pagemap);
    if (pm_addr < 0xffff800000000000ULL || pm_addr >= 0xffff900000000000ULL) {
        log::error("task PID %x has corrupted pagemap ptr 0x%x", task->pid, pm_addr);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    uint64_t const PERF_STARTED_US = ker::mod::time::get_us();

    // Validate size
    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check for size overflow
    if (size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    // Align size to page boundary
    size = page_align_up(size);
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    // Keep address selection/reservation, PTE installation, shared lazy-range
    // metadata, cursor publication, and every rollback in one fork-visible
    // transaction.
    SharedVmemPublicationGuard publication_guard;
    ker::mod::sched::resource::AddressSpaceObservation observation(task);

    // Find a free virtual address range
    uint64_t vaddr = 0;
    bool const IS_FIXED = ((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0;

    if (IS_FIXED) {
        // MAP_FIXED: use exact address
        if (hint < USER_SPACE_START || hint > USER_SPACE_END - size) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (overlaps_kernel_managed_user_layout_locked(task, hint, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
        if (!observation.admit_mapping(task->pagemap, size, hint, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    } else {
        if (!observation.admit_mapping(task->pagemap, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        uint64_t const RESERVE_RET = reserve_free_mmap_range_locked(task, size, hint, vaddr);
        if (RESERVE_RET != 0) {
            if (RESERVE_RET == ker::abi::vmem::VMEM_ENOMEM) {
                log::warn("no free range found for size %x", size);
            }
            return -RESERVE_RET;
        }
    }

    bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    if (IS_FIXED) {
        release_fixed_mmap_range_locked(task, vaddr, size);
    }

    if (is_prot_none(prot) || (flags & ker::abi::vmem::MAP_NORESERVE) != 0) {
        if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags)) {
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        advance_shared_mmap_cursor_locked(task, vaddr, size);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 1, 0,
                                vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
        observation.commit_mapping();
        return vaddr;
    }

    if ((prot & ker::abi::vmem::PROT_WRITE) == 0) {
        uint64_t const RESULT = private_anon_allocate_locked(task, vaddr, size, prot, hint, flags);
        int const RESULT_STATUS = perf_status_from_vmem_result(RESULT);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0,
                                RESULT_STATUS, vmem_latency_since(PERF_STARTED_US), RESULT_STATUS == 0 ? RESULT : hint, size, true);
        if (RESULT_STATUS != 0) {
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        }
        return RESULT;
    }

    if (lazy_anon_mmap_enabled() && (flags & ker::abi::vmem::MAP_POPULATE) == 0) {
        if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags)) {
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        advance_shared_mmap_cursor_locked(task, vaddr, size);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 3, 0,
                                vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
        observation.commit_mapping();
        return vaddr;
    }

    if (!anon_zero_cow_enabled()) {
        uint64_t const RESULT = private_anon_allocate_locked(task, vaddr, size, prot, hint, flags);
        int const RESULT_STATUS = perf_status_from_vmem_result(RESULT);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 2,
                                RESULT_STATUS, vmem_latency_since(PERF_STARTED_US), RESULT_STATUS == 0 ? RESULT : hint, size, true);
        if (RESULT_STATUS != 0) {
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        }
        return RESULT;
    }

    void* const ZERO_PAGE = get_anon_zero_page();
    if (ZERO_PAGE == nullptr) {
        log::error("out of physical memory allocating anon zero page");
        release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    // Anonymous mappings are initially backed by a shared read-only zero page.
    // Writes fault through the existing COW path and allocate private pages only
    // for memory that user space actually touches.
    auto const PAGE_FLAGS = anon_zero_page_flags(prot);
    auto const ZERO_PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(ZERO_PAGE)));
    if (NUM_PAGES > std::numeric_limits<uint32_t>::max()) {
        release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    ker::mod::mm::phys::page_ref_add(ZERO_PAGE, NUM_PAGES);
    ker::mod::mm::virt::map_same_page_range(task->pagemap, vaddr, ZERO_PADDR, NUM_PAGES, PAGE_FLAGS);

    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);

        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=anon-zero vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(ZERO_PADDR), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot));
        }
    }

    if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags)) {
        rollback_mapped_pages(task, vaddr, NUM_PAGES);
        release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    advance_shared_mmap_cursor_locked(task, vaddr, size);
    uint32_t const ELAPSED_US = vmem_latency_since(PERF_STARTED_US);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ZERO_PAGE_MAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            ELAPSED_US, vaddr, size, true);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            ELAPSED_US, vaddr, size, true);
    return vaddr;
}

// Free anonymous memory
auto anon_free(uint64_t addr, uint64_t size) -> uint64_t {
    // Get current task
    auto* task = get_current_task();
    if (task == nullptr) {
        log::error("vmem: no current task for free");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        log::error("vmem: task has no pagemap for free");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate address
    if (addr == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    if (addr < USER_SPACE_START || addr >= USER_SPACE_END) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check alignment
    if (addr % ker::mod::mm::paging::PAGE_SIZE != 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    int const SIZE_RET = align_user_vmem_size(size, &size);
    if (SIZE_RET < 0) {
        return static_cast<uint64_t>(SIZE_RET);
    }

    // Check bounds
    if (addr + size > USER_SPACE_END || addr + size < addr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Fork must observe either the complete mapping or the complete removal,
    // including file-range and lazy-range metadata.
    SharedVmemPublicationGuard publication_guard;
    ker::mod::sched::resource::AddressSpaceObservation observation(task);
    if (overlaps_kernel_managed_user_layout_locked(task, addr, size)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    int const SYNC_RET = sync_file_mmap_range(*task, addr, size);
    if (SYNC_RET < 0) {
        return static_cast<uint64_t>(SYNC_RET);
    }

    int const UNREGISTER_RET = unregister_file_mmap_range(task->pagemap, addr, size);
    if (UNREGISTER_RET < 0) {
        return static_cast<uint64_t>(UNREGISTER_RET);
    }

    // Unmap pages
    uint64_t const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    if (!remove_shared_vmem_range_locked(task, addr, size)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        uint64_t const CURRENT_VADDR = addr + (i * ker::mod::mm::paging::PAGE_SIZE);

        bool const IS_MAPPED = ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR);
        bool const IS_RESERVED = !IS_MAPPED && ker::mod::mm::virt::is_page_reserved(task->pagemap, CURRENT_VADDR);
        if (IS_MAPPED || IS_RESERVED) {
            if (is_watched_mmap_vaddr(CURRENT_VADDR)) {
                auto const MAPPED_PHYS =
                    IS_MAPPED ? ker::mod::sys::usercopy::mapped_physical_address(*task, CURRENT_VADDR) : ker::mod::mm::virt::PADDR_INVALID;
                uint64_t const PHYS = MAPPED_PHYS != ker::mod::mm::virt::PADDR_INVALID ? MAPPED_PHYS : 0;
                log::warn("watch mmap-unmap: pid=%lu name=%s pagemap=%p vaddr=0x%llx phys=0x%llx size=0x%llx", task->pid, task->name,
                          static_cast<void*>(task->pagemap), static_cast<unsigned long long>(CURRENT_VADDR),
                          static_cast<unsigned long long>(PHYS), static_cast<unsigned long long>(size));
            }
            // Unmap the page (this also frees the physical page)
            ker::mod::mm::virt::unmap_page(task->pagemap, CURRENT_VADDR);
        }
    }
#ifdef VMEM_DEBUG
    log::debug("freed %x bytes at %p", size, addr);
#endif
    return 0;  // Success
}

// Allocate file-backed memory with one admitted open description.
auto file_allocate(ker::mod::sched::task::Task* task, uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset)
    -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    uint64_t const PERF_STARTED_US = ker::mod::time::get_us();

    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Declared before publication_guard so even rollback's last File close
    // happens after shared VM publication unlocks. FD reuse cannot retarget us.
    auto release_file = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release_file)> retained_file(ker::vfs::vfs_get_file_retain(task, fd), release_file);
    auto* file = retained_file.get();
    ker::vfs::Stat st{};
    int const STAT_RET = ker::vfs::vfs_fstat_file(file, &st);
    if (STAT_RET < 0) {
        log::warn("file mmap fstat failed: fd=%d ret=%d hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx off=0x%llx", fd, STAT_RET,
                  static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size), static_cast<unsigned long long>(flags),
                  static_cast<unsigned long long>(prot), static_cast<unsigned long long>(offset));
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    int const POLICY_RET =
        ker::mod::security::authorize_file_mapping(static_cast<unsigned>(file->open_flags & 3), (flags & ker::abi::vmem::MAP_SHARED) != 0,
                                                   (prot & ker::abi::vmem::PROT_WRITE) != 0, ker::vfs::file_mount_read_only(*file));
    if (POLICY_RET != 0) return static_cast<uint64_t>(POLICY_RET);

    uint64_t const REQUESTED_SIZE = size;
    size = page_align_up(size);
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    if (!file_mapping_offset_range_valid(offset, size)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    // Keep reservation, page-table construction, lazy/file metadata, and
    // rollback indivisible with respect to fork's address-space snapshot.
    SharedVmemPublicationGuard publication_guard;
    ker::mod::sched::resource::AddressSpaceObservation observation(task);

    uint64_t vaddr = 0;
    bool const IS_FIXED = ((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0;
    if (IS_FIXED) {
        if (hint < USER_SPACE_START || hint > USER_SPACE_END - size) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (overlaps_kernel_managed_user_layout_locked(task, hint, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
        if (!observation.admit_mapping(task->pagemap, size, hint, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    } else {
        if (!observation.admit_mapping(task->pagemap, size)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        uint64_t const RESERVE_RET = reserve_free_mmap_range_locked(task, size, hint, vaddr);
        if (RESERVE_RET != 0) {
            return -RESERVE_RET;
        }
    }

    bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;
    auto const PAGE_FLAGS = file_mapping_page_flags(prot, flags);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    bool const TRACK_FILE_MAPPING = (flags & ker::abi::vmem::MAP_SHARED) != 0;
    bool const WRITEBACK = (st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG;
    if (IS_FIXED) {
        release_fixed_mmap_range_locked(task, vaddr, size);
    }

    if ((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG && lazy_file_mmap_enabled()) {
        if (!add_shared_lazy_file_vmem_range_locked(task, vaddr, size, prot, flags, file, offset, st)) {
            (void)remove_shared_vmem_range_locked(task, vaddr, size);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }

        if (TRACK_FILE_MAPPING) {
            int const REGISTER_RET = register_file_mmap_range(task->pagemap, vaddr, REQUESTED_SIZE, offset, file, WRITEBACK);
            if (REGISTER_RET < 0) {
                (void)remove_shared_vmem_range_locked(task, vaddr, size);
                return static_cast<uint64_t>(REGISTER_RET);
            }
        }

        advance_shared_mmap_cursor_locked(task, vaddr, size);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                                vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
        observation.commit_mapping();
        return vaddr;
    }

    if (file_mmap_can_share(st, prot)) {
        bool cache_ok = true;
        uint64_t mapped_pages = 0;
        ker::mod::mm::virt::PageMapBatch map_batch{};
        ker::mod::mm::virt::init_page_map_batch(&map_batch, task->pagemap, PAGE_FLAGS);
        for (uint64_t i = 0; i < NUM_PAGES; i++) {
            if (i > (std::numeric_limits<uint64_t>::max() - offset) / ker::mod::mm::paging::PAGE_SIZE) {
                cache_ok = false;
                break;
            }

            uint64_t const CURRENT_VADDR = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
            uint64_t const FILE_OFFSET = offset + (i * ker::mod::mm::paging::PAGE_SIZE);
            uint64_t physical = 0;
            if (!map_file_mmap_cached_page(&map_batch, file, st, CURRENT_VADDR, FILE_OFFSET, PAGE_FLAGS, physical)) {
                cache_ok = false;
                break;
            }

            uint64_t const PADDR = physical;
            mapped_pages++;

            if (is_watched_mmap_vaddr(CURRENT_VADDR)) {
                log::warn(
                    "watch mmap-map: pid=%lu name=%s pagemap=%p kind=file-cache vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx "
                    "flags=0x%llx prot=0x%llx fd=%d off=0x%llx",
                    task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(CURRENT_VADDR),
                    static_cast<unsigned long long>(PADDR), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                    static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot), fd,
                    static_cast<unsigned long long>(FILE_OFFSET));
            }
        }

        if (cache_ok) {
            ker::mod::mm::virt::flush_page_map_batch(&map_batch);
            if (TRACK_FILE_MAPPING) {
                int const REGISTER_RET = register_file_mmap_range(task->pagemap, vaddr, REQUESTED_SIZE, offset, file, WRITEBACK);
                if (REGISTER_RET < 0) {
                    rollback_mapped_pages(task, vaddr, mapped_pages);
                    release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
                    return static_cast<uint64_t>(REGISTER_RET);
                }
            }
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            advance_shared_mmap_cursor_locked(task, vaddr, size);
            record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                                    vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
            return vaddr;
        }

        ker::mod::mm::virt::flush_page_map_batch(&map_batch);
        rollback_mapped_pages(task, vaddr, mapped_pages);
    }

    // Read file content into the backing pages. File-backed mmap must not
    // mutate the descriptor offset, and positional reads avoid remote VFS
    // sequential read-ahead while materializing ELF segments.
    auto const FILE_SIZE = static_cast<uint64_t>(st.st_size);
    uint64_t mapped_pages = 0;
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_FILE_MAPPING,
                                                                                     ker::mod::mm::paging::PAGE_SIZE, "vmem-file");
        if (PHYS_PAGE == nullptr) {
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

        if (i > (std::numeric_limits<uint64_t>::max() - offset) / ker::mod::mm::paging::PAGE_SIZE) {
            ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
        }

        uint64_t const FILE_OFFSET = offset + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (FILE_OFFSET < FILE_SIZE) {
            uint64_t const READ_SIZE = std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - FILE_OFFSET);
            if (FILE_OFFSET > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
                release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
            }

            if (!read_file_mapping_exact(file, PHYS_PAGE, static_cast<size_t>(READ_SIZE), FILE_OFFSET)) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
                release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }
        }

        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, paddr, PAGE_FLAGS);
        mapped_pages++;
        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=file vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx fd=%d off=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(paddr), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot), fd,
                static_cast<unsigned long long>(FILE_OFFSET));
        }
    }

    if (TRACK_FILE_MAPPING && REQUESTED_SIZE > 0) {
        int const REGISTER_RET = register_file_mmap_range(task->pagemap, vaddr, REQUESTED_SIZE, offset, file, WRITEBACK);
        if (REGISTER_RET < 0) {
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(REGISTER_RET);
        }
    }

    release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION);
    advance_shared_mmap_cursor_locked(task, vaddr, size);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
    return vaddr;
}

}  // anonymous namespace

#ifdef WOS_SELFTEST
namespace {
class SelftestImmutableFileHold {
   public:
    explicit SelftestImmutableFileHold(ker::vfs::File& file) : file_(&file) { ker::vfs::vfs_retain_file(file_); }
    ~SelftestImmutableFileHold() { ker::vfs::vfs_put_file(file_); }
    SelftestImmutableFileHold(const SelftestImmutableFileHold&) = delete;
    auto operator=(const SelftestImmutableFileHold&) -> SelftestImmutableFileHold& = delete;

   private:
    ker::vfs::File* file_;
};

auto selftest_immutable_file_cache_key(ker::vfs::File& file, uint64_t offset, ker::vfs::Stat& stat, FileMmapPageKey& key) -> bool {
    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    if (file.exec_read_view == nullptr || file.exec_read_view->content_id == 0 || (file.open_flags & 3) != 0 || file.fops == nullptr ||
        file.fops->vfs_read == nullptr || (offset & (PAGE - 1)) != 0 || offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
        ker::vfs::vfs_fstat_file(&file, &stat) != 0 || stat.st_size < static_cast<off_t>(PAGE) ||
        !file_mmap_can_share(stat, ker::abi::vmem::PROT_READ)) {
        return false;
    }
    key = file_mmap_page_key(stat, offset);
    key.exec_content_id = file.exec_read_view->content_id;
    return true;
}

auto selftest_file_cache_key_has_capacity(const FileMmapPageKey& key) -> bool {
    size_t const SET_INDEX = file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1);
    auto& cache_lock = g_file_mmap_cache_locks.at(SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1));
    auto& set = g_file_mmap_cache.at(SET_INDEX);
    cache_lock.mutex.lock();
    bool available = false;
    for (const auto& entry : set.ways) {
        available = available || entry.page == nullptr || file_mmap_key_equal(entry.key, key);
    }
    cache_lock.mutex.unlock();
    return available;
}

class SelftestFileMapSubjectHold {
   public:
    ~SelftestFileMapSubjectHold() {
        if (root_ != nullptr) subject_->release_usercopy_pagemap();
        if (subject_ != nullptr) subject_->release();
    }
    SelftestFileMapSubjectHold() = default;
    SelftestFileMapSubjectHold(const SelftestFileMapSubjectHold&) = delete;
    auto operator=(const SelftestFileMapSubjectHold&) -> SelftestFileMapSubjectHold& = delete;
    auto acquire(ker::mod::sched::task::Task& subject) -> bool {
        namespace task = ker::mod::sched::task;
        namespace virt = ker::mod::mm::virt;
        if (subject.type != task::TaskType::PROCESS || subject.is_thread || subject.thread != nullptr || subject.pid != 0 ||
            subject.scheduler_published.load(std::memory_order_acquire) || subject.shares_user_pagemap.load(std::memory_order_acquire) ||
            !subject.try_acquire()) {
            return false;
        }
        subject_ = &subject;
        if (!subject.try_acquire_usercopy_pagemap(root_)) return false;
        return subject.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && subject.type == task::TaskType::PROCESS &&
               !subject.is_thread && subject.thread == nullptr && subject.pid == 0 && subject.pagemap == root_ &&
               !subject.scheduler_published.load(std::memory_order_acquire) &&
               !subject.shares_user_pagemap.load(std::memory_order_acquire) && root_ != virt::get_kernel_pagemap() &&
               !virt::selftest_kernel_page_table_frame(root_) && !virt::active_pagemap_is(root_) &&
               (!ker::mod::sched::can_query_current_task() || ker::mod::sched::get_current_task() != &subject);
    }
    auto root() const -> ker::mod::mm::paging::PageTable* { return root_; }

   private:
    ker::mod::sched::task::Task* subject_{};
    ker::mod::mm::paging::PageTable* root_{};
};
}  // namespace

auto selftest_observe_file_mmap_cache(ker::vfs::File& file, uint64_t offset, SelftestFileCacheObservation& observation) -> bool {
    if (observation.retained_frame != nullptr) return false;
    SelftestImmutableFileHold hold(file);
    ker::vfs::Stat stat{};
    FileMmapPageKey key{};
    if (!selftest_immutable_file_cache_key(file, offset, stat, key)) return false;
    size_t const SET_INDEX = file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1);
    auto& cache_lock = g_file_mmap_cache_locks.at(SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1));
    auto& set = g_file_mmap_cache.at(SET_INDEX);
    SelftestFileCacheObservation result{};
    cache_lock.mutex.lock();
    for (const auto& entry : set.ways) {
        result.vacant_way = result.vacant_way || entry.page == nullptr;
        if (result.retained_frame == nullptr && entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            ker::mod::mm::phys::page_ref_inc(entry.page);
            result.retained_frame = entry.page;
        }
    }
    cache_lock.mutex.unlock();
    observation = result;
    return true;
}

auto selftest_map_cached_private_file_page(ker::mod::sched::task::Task& subject, ker::vfs::File& file, uint64_t address, uint64_t offset)
    -> bool {
    namespace virt = ker::mod::mm::virt;
    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    if ((address & (PAGE - 1)) != 0 || address < USER_SPACE_START || address > USER_SPACE_END - (PAGE - 1)) return false;
    SelftestFileMapSubjectHold subject_hold;
    if (!subject_hold.acquire(subject)) return false;
    SelftestImmutableFileHold file_hold(file);
    ker::vfs::Stat stat{};
    FileMmapPageKey key{};
    if (!selftest_immutable_file_cache_key(file, offset, stat, key) || offset > static_cast<uint64_t>(stat.st_size) - PAGE ||
        !virt::is_user_virtual_range_free(subject_hold.root(), address, address + PAGE) || !selftest_file_cache_key_has_capacity(key)) {
        return false;
    }
    // Capacity is not reserved across IO: the native fixture is serialized
    // before SMP and never publishes this root. No wider topology claim.
    uint64_t const FLAGS = file_mapping_page_flags(ker::abi::vmem::PROT_READ, ker::abi::vmem::MAP_PRIVATE);
    virt::PageMapBatch batch{};
    virt::init_page_map_batch(&batch, subject_hold.root(), FLAGS);
    uint64_t physical = 0;
    bool const mapped = map_file_mmap_cached_page(&batch, &file, stat, address, offset, FLAGS, physical);
    virt::flush_page_map_batch(&batch);
    return mapped;
}

auto selftest_evict_file_mmap_cache_exact(ker::vfs::File& file, uint64_t offset, void* expected_frame) -> bool {
    if (expected_frame == nullptr) return false;
    SelftestImmutableFileHold hold(file);
    ker::vfs::Stat stat{};
    FileMmapPageKey key{};
    if (!selftest_immutable_file_cache_key(file, offset, stat, key)) return false;
    size_t const SET_INDEX = file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1);
    auto& cache_lock = g_file_mmap_cache_locks.at(SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1));
    auto& set = g_file_mmap_cache.at(SET_INDEX);
    void* removed = nullptr;
    cache_lock.mutex.lock();
    for (auto& entry : set.ways) {
        if (entry.page == expected_frame && file_mmap_key_equal(entry.key, key)) {
            removed = entry.page;
            entry = {};
            g_file_mmap_cache_pages.fetch_sub(1, std::memory_order_relaxed);
            break;
        }
    }
    cache_lock.mutex.unlock();
    if (removed == nullptr) return false;
    release_file_mmap_cache_page(removed);
    return true;
}

auto selftest_file_mapping_admission(ker::mod::mm::paging::PageTable* pagemap) -> bool {
    namespace vfs = ker::vfs;
    namespace vm = ker::abi::vmem;
    namespace virt = ker::mod::mm::virt;
    namespace phys = ker::mod::mm::phys;
    namespace paging = ker::mod::mm::paging;
    constexpr uint64_t BASE = USER_SPACE_START;
    constexpr uint64_t SIZE = paging::PAGE_SIZE;
    struct State {
        unsigned reads{};
        unsigned closes{};
        bool close_unlocked = true;
        ker::mod::sched::task::Task* replace_task{};
        vfs::File* replacement{};
        int fd{};
        bool replaced{};
    };
    vfs::FileOperations operations{};
    operations.vfs_read = [](vfs::File* file, void* out, size_t count, size_t) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.reads;
        if (state.replace_task != nullptr && !state.replaced) {
            state.replaced = true;
            vfs::vfs_release_fd(state.replace_task, state.fd);
            vfs::vfs_put_file(file);
            state.replaced = vfs::vfs_alloc_fd(state.replace_task, state.replacement) == state.fd;
        }
        std::memset(out, 0x41, count);
        return static_cast<ssize_t>(count);
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        bool const UNLOCKED = g_shared_vmem_publication_lock.try_lock();
        if (UNLOCKED) g_shared_vmem_publication_lock.unlock();
        state.close_unlocked = state.close_unlocked && UNLOCKED;
        return 0;
    };
    auto make_file = [&](State& state, unsigned mode, bool readonly, bool regular) -> vfs::File* {
        auto* file = new (std::nothrow) vfs::File{};
        if (file == nullptr) return nullptr;
        file->fs_type = vfs::FSType::SOCKET;
        file->fops = &operations;
        file->private_data = &state;
        file->open_flags = static_cast<int>(mode);
        file->mount_policy.read_only = readonly;
        file->pos = 17;
        file->refcount.store(1);
        file->exec_read_view.reset(new (std::nothrow) vfs::ExecReadView{});
        if (file->exec_read_view == nullptr) {
            vfs::vfs_put_file(file);
            return nullptr;
        }
        file->exec_read_view->content_id = vfs::ExecContentState::allocate_identity();
        file->exec_read_view->metadata.st_mode = regular ? vfs::S_IFREG : vfs::S_IFSOCK;
        file->exec_read_view->metadata.st_size = static_cast<off_t>(SIZE);
        return file;
    };
    for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
        for (bool readonly : {false, true}) {
            for (bool shared : {false, true}) {
                for (uint64_t prot : {vm::PROT_NONE, vm::PROT_READ, vm::PROT_READ | vm::PROT_WRITE}) {
                    ker::mod::sched::task::Task task;
                    task.pagemap = pagemap;
                    State state;
                    auto* file = make_file(state, mode, readonly, true);
                    if (file == nullptr) return false;
                    int const FD = vfs::vfs_alloc_fd(&task, file);
                    if (FD < 0) {
                        vfs::vfs_put_file(file);
                        return false;
                    }
                    auto* marker = phys::page_alloc(ker::mod::mm::PhysicalPageOwner::SELFTEST, SIZE, "mmap-policy-marker");
                    if (marker == nullptr) {
                        vfs::vfs_release_fd(&task, FD);
                        vfs::vfs_put_file(file);
                        return false;
                    }
                    std::memset(marker, 0x5a, SIZE);
                    auto const MARKER_PHYS =
                        reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(marker)));
                    virt::map_page(pagemap, BASE, MARKER_PHYS, paging::page_types::USER);
                    uint64_t const CURSOR = task.mmap_next.load(std::memory_order_relaxed);
                    bool const DENIED = mode == 1 || mode == vfs::FILE_ACCESS_SEARCH_ONLY ||
                                        (shared && (prot & vm::PROT_WRITE) != 0 && (mode != 2 || readonly));
                    uint64_t const RESULT =
                        file_allocate(&task, BASE, SIZE - 1, prot, vm::MAP_FIXED | (shared ? vm::MAP_SHARED : vm::MAP_PRIVATE), FD, 0);
                    bool ok = RESULT == (DENIED ? static_cast<uint64_t>(-EACCES) : BASE) && file->pos == 17;
                    if (DENIED) {
                        ok = ok && virt::translate(pagemap, BASE) == MARKER_PHYS && static_cast<uint8_t*>(marker)[0] == 0x5a &&
                             file->refcount.load() == 1 && state.reads == 0 && state.closes == 0 && task.lazy_vmem_ranges.empty() &&
                             task.mmap_next.load(std::memory_order_relaxed) == CURSOR;
                    } else if (shared) {
                        // The descriptor can disappear or be reused without changing
                        // the File selected by stat, faulting, or mprotect admission.
                        State replacement_state;
                        auto* replacement = make_file(replacement_state, 2, false, false);
                        if (replacement == nullptr) ok = false;
                        vfs::vfs_release_fd(&task, FD);
                        vfs::vfs_put_file(file);
                        int const NEW_FD = replacement == nullptr ? -1 : vfs::vfs_alloc_fd(&task, replacement);
                        ok = ok && NEW_FD == FD && state.closes == 0;
                        {
                            SharedVmemPublicationGuard guard;
                            ok = ok && authorize_shared_file_protection_locked(pagemap, BASE, SIZE, vm::PROT_WRITE) ==
                                           (mode != 2 || readonly ? -EACCES : 0);
                            ok = ok && authorize_shared_file_protection_locked(pagemap, BASE, SIZE, vm::PROT_READ) == 0;
                            for (const auto& range : task.lazy_vmem_ranges) {
                                ok = ok && range.file == file && range.prot == prot;
                                if (prot != vm::PROT_NONE) {
                                    paging::PageFault fault{};
                                    ok = materialize_lazy_file_page_impl(&task, range, BASE, fault) && ok;
                                }
                            }
                        }
                        if (prot != vm::PROT_NONE) {
                            auto const PADDR = virt::translate(pagemap, BASE);
                            ok = ok && PADDR != virt::PADDR_INVALID;
                            if (PADDR != virt::PADDR_INVALID) {
                                auto* data = reinterpret_cast<uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PADDR));
                                ok = ok && data[0] == 0x41 && state.reads != 0 && replacement_state.reads == 0;
                            }
                        }
                        remove_lazy_vmem_range(&task, BASE, SIZE);
                        unregister_file_mmap_range(pagemap, BASE, SIZE);
                        rollback_mapped_pages(&task, BASE, 1);
                        if (NEW_FD >= 0) vfs::vfs_release_fd(&task, NEW_FD);
                        if (replacement != nullptr) vfs::vfs_put_file(replacement);
                        if (!ok || state.closes != 1 || !state.close_unlocked) return false;
                        continue;
                    }
                    remove_lazy_vmem_range(&task, BASE, SIZE);
                    unregister_file_mmap_range(pagemap, BASE, SIZE);
                    rollback_mapped_pages(&task, BASE, 1);
                    vfs::vfs_release_fd(&task, FD);
                    vfs::vfs_put_file(file);
                    if (!ok || state.closes != 1 || !state.close_unlocked) return false;
                }
            }
        }
    }
    // Non-regular files force eager materialization. Replace the FD from its
    // read callback; MAP_PRIVATE has no registry reference to mask final close.
    for (bool shared : {false, true}) {
        ker::mod::sched::task::Task task;
        task.pagemap = pagemap;
        State state, replacement_state;
        auto* file = make_file(state, 0, true, false);
        auto* replacement = make_file(replacement_state, 2, false, false);
        if (file == nullptr || replacement == nullptr) {
            vfs::vfs_put_file(file);
            vfs::vfs_put_file(replacement);
            return false;
        }
        int const FD = vfs::vfs_alloc_fd(&task, file);
        if (FD < 0) {
            vfs::vfs_put_file(file);
            vfs::vfs_put_file(replacement);
            return false;
        }
        state.replace_task = &task;
        state.replacement = replacement;
        state.fd = FD;
        uint64_t const RESULT =
            file_allocate(&task, BASE, SIZE, vm::PROT_READ, vm::MAP_FIXED | (shared ? vm::MAP_SHARED : vm::MAP_PRIVATE), FD, 0);
        bool ok = RESULT == BASE && state.replaced && state.reads == 1 && replacement_state.reads == 0 && state.close_unlocked;
        {
            SharedVmemPublicationGuard guard;
            ok = ok && authorize_shared_file_protection_locked(pagemap, BASE, SIZE, vm::PROT_WRITE) == (shared ? -EACCES : 0);
            ker::util::SmallVec<FileMmapSyncRange, 8> sync;
            ok = ok && collect_file_mmap_sync_ranges(pagemap, BASE, SIZE, sync) == 0 && sync.empty();
        }
        unregister_file_mmap_range(pagemap, BASE, SIZE);
        remove_lazy_vmem_range(&task, BASE, SIZE);
        rollback_mapped_pages(&task, BASE, 1);
        auto* remaining = vfs::vfs_get_file(&task, FD);
        vfs::vfs_release_fd(&task, FD);
        vfs::vfs_put_file(remaining);
        if (!state.replaced) vfs::vfs_put_file(replacement);
        if (!ok || state.closes != 1 || !state.close_unlocked) return false;
    }
    // Clone and split keep the original policy, including a request that
    // starts in an allowed range and ends in a denied, unmaterialized page.
    State denied_state, allowed_state;
    auto* denied = make_file(denied_state, 0, true, true);
    auto* allowed = make_file(allowed_state, 2, false, true);
    if (denied == nullptr || allowed == nullptr) {
        vfs::vfs_put_file(denied);
        vfs::vfs_put_file(allowed);
        return false;
    }
    paging::PageTable clone{};
    bool ok = true;
    {
        SharedVmemPublicationGuard guard;
        ok = register_file_mmap_range(pagemap, BASE, SIZE, 0, allowed, true) == 0 && ok;
        ok = register_file_mmap_range(pagemap, BASE + SIZE, 3 * SIZE - 1, 0, denied, true) == 0 && ok;
        ok = ok && authorize_shared_file_protection_locked(pagemap, BASE, 4 * SIZE, vm::PROT_WRITE) == -EACCES;
        ok = clone_file_mmap_ranges_for_pagemap_locked(pagemap, &clone) && ok;
        ok = unregister_file_mmap_range(&clone, BASE + 2 * SIZE, SIZE) == 0 && ok;
        ok = ok && authorize_shared_file_protection_locked(&clone, BASE + 3 * SIZE, SIZE, vm::PROT_WRITE) == -EACCES &&
             authorize_shared_file_protection_locked(&clone, BASE, SIZE, vm::PROT_WRITE) == 0 &&
             authorize_shared_file_protection_locked(&clone, BASE + 2 * SIZE, SIZE, vm::PROT_WRITE) == 0;
        ok = unregister_file_mmap_range(pagemap, BASE, 4 * SIZE) == 0 && ok;
        ok = unregister_file_mmap_range(&clone, BASE, 4 * SIZE) == 0 && ok;
    }
    ok = ok && denied->refcount.load() == 1 && allowed->refcount.load() == 1 && denied_state.reads == 0 && allowed_state.reads == 0;
    vfs::vfs_put_file(denied);
    vfs::vfs_put_file(allowed);
    return ok && denied_state.closes == 1 && allowed_state.closes == 1 && denied_state.close_unlocked && allowed_state.close_unlocked;
}

auto selftest_lazy_file_projection(ker::mod::mm::paging::PageTable* pagemap) -> bool {
    namespace vfs = ker::vfs;
    namespace vm = ker::abi::vmem;
    namespace mm = ker::mod::mm;
    constexpr uint64_t BASE = 0x400000;
    constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
    constexpr uint64_t SIZE = 6 * PAGE;
    constexpr uint64_t OFFSET = 2 * PAGE;
    constexpr uint64_t FILE_SIZE = 12 * PAGE;
    struct State {
        unsigned seed{};
        unsigned reads{};
        unsigned closes{};
        bool close_unlocked = true;
    };
    struct CachedPage {
        vfs::File* file{};
        uint64_t offset{};
        void* frame{};
    };
    auto const BYTE = [](unsigned seed, uint64_t offset) {
        return static_cast<uint8_t>(seed + 13 * (offset / PAGE) + (offset % PAGE) % 251);
    };
    vfs::FileOperations operations{};
    operations.vfs_read = [](vfs::File* file, void* output, size_t count, size_t offset) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.reads;
        if (offset >= FILE_SIZE) return 0;
        size_t const N = std::min<size_t>(count, FILE_SIZE - offset);
        auto* bytes = static_cast<uint8_t*>(output);
        for (size_t i = 0; i < N; ++i) {
            uint64_t const POS = offset + i;
            bytes[i] = static_cast<uint8_t>(state.seed + 13 * (POS / PAGE) + (POS % PAGE) % 251);
        }
        return static_cast<ssize_t>(N);
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        bool const UNLOCKED = g_shared_vmem_publication_lock.try_lock();
        if (UNLOCKED) g_shared_vmem_publication_lock.unlock();
        state.close_unlocked = state.close_unlocked && UNLOCKED;
        return 0;
    };
    auto const MAKE_FILE = [&](State& state, int open_flags) -> vfs::File* {
        auto* file = new (std::nothrow) vfs::File{};
        if (file == nullptr) return nullptr;
        file->fs_type = vfs::FSType::SOCKET;
        file->fops = &operations;
        file->private_data = &state;
        file->pos = 19;
        file->open_flags = open_flags;
        file->refcount.store(1);
        file->exec_read_view.reset(new (std::nothrow) vfs::ExecReadView{});
        if (file->exec_read_view == nullptr) {
            vfs::vfs_put_file(file);
            return nullptr;
        }
        uint64_t const ID = vfs::ExecContentState::allocate_identity();
        if (ID == 0) {
            vfs::vfs_put_file(file);
            return nullptr;
        }
        file->exec_read_view->content_id = ID;
        auto& stat = file->exec_read_view->metadata;
        stat.st_dev = 0x50524f4a;
        stat.st_ino = ID;
        stat.st_mode = vfs::S_IFREG | 0600;
        stat.st_size = FILE_SIZE;
        return file;
    };
    // Real VFS Files, MM materialization/PTEs and frame refs, with a bounded
    // synthetic read backend. No native filesystem or shared-coherence claim.
    for (unsigned mode = 0; mode < 9; ++mode) {
        State first{.seed = 0x21}, second{.seed = 0xa3};
        // The immutable cache observer requires a read-only description;
        // private writable mappings do not require a writable description.
        auto* file = MAKE_FILE(first, mode == 6 || mode == 7 ? 0 : 2);
        auto* replacement = MAKE_FILE(second, 2);
        if (file == nullptr || replacement == nullptr) {
            vfs::vfs_put_file(file);
            vfs::vfs_put_file(replacement);
            return false;
        }
        ker::mod::sched::task::Task task;
        task.pagemap = pagemap;
        std::array<CachedPage, 8> cached{};
        size_t cached_count = 0;
        bool ok = true;
        const char* failure_stage = nullptr;
        uint64_t failure_address = 0;
        auto const RECORD_FAILURE = [&](const char* stage, uint64_t address = 0) {
            if (!ok && failure_stage == nullptr) {
                failure_stage = stage;
                failure_address = address;
            }
        };
        bool const PREFIX = mode == 0 || mode == 2 || mode == 4 || mode == 6;
        uint64_t const CUT_START = PREFIX ? BASE : BASE + 2 * PAGE;
        uint64_t const CUT_SIZE = mode == 0 ? PAGE : 2 * PAGE;
        uint64_t const CUT_END = CUT_START + CUT_SIZE;
        {
            SharedVmemPublicationGuard guard;
            ok = add_lazy_file_vmem_range(&task, BASE, SIZE, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_PRIVATE, file, OFFSET,
                                          file->exec_read_view->metadata);
            RECORD_FAILURE("initial-range");
            if (mode <= 1) {
                ok = remove_lazy_vmem_range(&task, CUT_START, CUT_SIZE) && ok;
            } else if (mode <= 3) {
                ok = add_lazy_file_vmem_range(&task, CUT_START, CUT_SIZE, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_PRIVATE, replacement,
                                              PAGE, replacement->exec_read_view->metadata) &&
                     ok;
            } else if (mode <= 5) {
                ok = add_lazy_vmem_range(&task, CUT_START, CUT_SIZE, vm::PROT_READ | vm::PROT_WRITE, vm::MAP_PRIVATE | vm::MAP_ANONYMOUS) &&
                     ok;
            } else {
                ok = protect_lazy_vmem_range(&task, CUT_START, CUT_SIZE, mode == 8 ? vm::PROT_NONE : vm::PROT_READ) && ok;
                if (mode == 8) {
                    ok = protect_lazy_vmem_range(&task, CUT_START, CUT_SIZE, vm::PROT_READ | vm::PROT_WRITE) && ok;
                }
            }
            RECORD_FAILURE("rewrite");
            ok = ok && first.reads == 0 && second.reads == 0;
            RECORD_FAILURE("cold-read-count");
            for (uint64_t address = BASE; ok && address < BASE + SIZE; address += PAGE) {
                bool const CUT = address >= CUT_START && address < CUT_END;
                if (CUT && mode <= 1) {
                    ok = !first_lazy_vmem_overlap(&task, address, address + PAGE).found;
                    RECORD_FAILURE("removed-range", address);
                    continue;
                }
                if (CUT && (mode == 4 || mode == 5)) {
                    bool found = false;
                    for (const auto& range : task.lazy_vmem_ranges) {
                        if (address >= range.start && address < range.end) {
                            found = range.kind == LazyVmemKind::ANONYMOUS && range.file == nullptr;
                        }
                    }
                    ok = found;
                    RECORD_FAILURE("anonymous-range", address);
                    continue;
                }
                bool const REPLACED = CUT && (mode == 2 || mode == 3);
                auto* expected_file = REPLACED ? replacement : file;
                unsigned const SEED = REPLACED ? second.seed : first.seed;
                uint64_t const EXPECTED_OFFSET = REPLACED ? PAGE + address - CUT_START : OFFSET + address - BASE;
                bool found = false;
                for (const auto& range : task.lazy_vmem_ranges) {
                    if (address < range.start || address >= range.end) continue;
                    found = true;
                    ok = range.file == expected_file && range.file_offset + address - range.start == EXPECTED_OFFSET;
                    RECORD_FAILURE("file-projection", address);
                    bool const CACHED = (range.prot & vm::PROT_WRITE) == 0;
                    if (CACHED) {
                        SelftestFileCacheObservation before{};
                        ok = selftest_observe_file_mmap_cache(*expected_file, EXPECTED_OFFSET, before) && ok;
                        if (before.retained_frame != nullptr) mm::phys::page_ref_dec(before.retained_frame);
                        ok = before.retained_frame == nullptr && before.vacant_way && ok;
                        RECORD_FAILURE("cache-before", address);
                    }
                    mm::paging::PageFault fault{};
                    if (ok) ok = materialize_lazy_file_page_impl(&task, range, address, fault);
                    RECORD_FAILURE("materialize", address);
                    if (CACHED) {
                        SelftestFileCacheObservation after{};
                        bool const OBSERVED = selftest_observe_file_mmap_cache(*expected_file, EXPECTED_OFFSET, after);
                        if (after.retained_frame != nullptr) {
                            cached.at(cached_count++) = {expected_file, EXPECTED_OFFSET, after.retained_frame};
                        }
                        ok = OBSERVED && after.retained_frame != nullptr && ok;
                        RECORD_FAILURE("cache-after", address);
                    }
                    auto const PHYSICAL = mm::virt::translate(pagemap, address);
                    if (PHYSICAL == mm::virt::PADDR_INVALID) {
                        ok = false;
                        RECORD_FAILURE("pte", address);
                        break;
                    }
                    auto const* bytes = reinterpret_cast<const uint8_t*>(mm::addr::get_virt_pointer(PHYSICAL));
                    for (size_t i = 0; i < PAGE; ++i) ok = BYTE(SEED, EXPECTED_OFFSET + i) == bytes[i] && ok;
                    RECORD_FAILURE("cold-bytes", address);
                    break;
                }
                ok = found && ok;
                RECORD_FAILURE("missing-range", address);
            }
            ok = remove_lazy_vmem_range(&task, BASE, SIZE) && ok;
            RECORD_FAILURE("remove-cleanup");
            rollback_mapped_pages(&task, BASE, SIZE / PAGE);
        }
        for (size_t i = 0; i < cached_count; ++i) {
            auto const& page = cached.at(i);
            ok = selftest_evict_file_mmap_cache_exact(*page.file, page.offset, page.frame) && ok;
            mm::phys::page_ref_dec(page.frame);
            ok = mm::phys::page_ref_get(page.frame) == 0 && ok;
            RECORD_FAILURE("cache-cleanup", BASE + page.offset - OFFSET);
        }
        ok = task.lazy_vmem_ranges.empty() && file->refcount.load() == 1 && replacement->refcount.load() == 1 && file->pos == 19 &&
             replacement->pos == 19 && ok;
        RECORD_FAILURE("file-references");
        vfs::vfs_put_file(file);
        vfs::vfs_put_file(replacement);
        if (!ok || first.closes != 1 || second.closes != 1 || !first.close_unlocked || !second.close_unlocked) {
            log::warn("lazy file projection selftest failed: mode=%u stage=%s address=0x%llx", mode,
                      failure_stage == nullptr ? "close" : failure_stage, static_cast<unsigned long long>(failure_address));
            return false;
        }
    }
    // Invalid unsigned offsets fail before reservations, lazy rows or PTEs.
    State state{.seed = 0x21};
    auto* file = MAKE_FILE(state, 2);
    if (file == nullptr) return false;
    ker::mod::sched::task::Task task;
    task.pagemap = pagemap;
    int const FD = vfs::vfs_alloc_fd(&task, file);
    if (FD < 0) {
        vfs::vfs_put_file(file);
        return false;
    }
    uint64_t const CURSOR = task.mmap_next.load();
    bool ok = true;
    constexpr uint64_t MAX_OFFSET = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    for (uint64_t offset : {UINT64_MAX, MAX_OFFSET + 1, MAX_OFFSET - PAGE + 2}) {
        for (uint64_t flags : {vm::MAP_PRIVATE, vm::MAP_SHARED}) {
            ok = file_allocate(&task, BASE, PAGE, vm::PROT_READ | vm::PROT_WRITE, flags | vm::MAP_FIXED, FD, offset) ==
                     static_cast<uint64_t>(-vm::VMEM_EOVERFLOW) &&
                 ok;
            ok = file->refcount.load() == 1 && file->pos == 19 && task.lazy_vmem_ranges.empty() && task.mmap_next.load() == CURSOR &&
                 mm::virt::translate(pagemap, BASE) == mm::virt::PADDR_INVALID && state.reads == 0 && state.closes == 0 && ok;
            // A broken admission guard must still retire this fixture's exact
            // range before its stack-backed File callback state goes away.
            {
                SharedVmemPublicationGuard guard;
                ok = remove_lazy_vmem_range(&task, BASE, PAGE) && ok;
                ok = unregister_file_mmap_range(pagemap, BASE, PAGE) == 0 && ok;
                rollback_mapped_pages(&task, BASE, 1);
            }
        }
    }
    vfs::vfs_release_fd(&task, FD);
    vfs::vfs_put_file(file);
    return ok && state.closes == 1 && state.close_unlocked;
}

auto selftest_file_mmap_sync_preflight(ker::mod::mm::paging::PageTable* pagemap) -> bool {
    namespace vfs = ker::vfs;
    namespace virt = ker::mod::mm::virt;
    namespace phys = ker::mod::mm::phys;
    namespace paging = ker::mod::mm::paging;
    constexpr uint64_t BASE = USER_SPACE_START;
    constexpr uint64_t PAGE = paging::PAGE_SIZE;
    constexpr size_t COUNT = 16;
    constexpr uint64_t MAX_OFFSET = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    struct State {
        std::array<uint8_t, COUNT> bytes{};
        unsigned writes{};
        unsigned closes{};
        int backend_error{};
        bool callback_safe = true;
        bool dirty_later{};
        ker::mod::sched::task::Task* task{};
        void* first_page{};
        void* second_page{};
    };
    vfs::FileOperations operations{};
    operations.vfs_write = [](vfs::File* file, const void* bytes, size_t count, size_t offset) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.writes;
        bool const REGISTRY_UNLOCKED = g_file_mmap_ranges_lock.try_lock();
        if (REGISTRY_UNLOCKED) g_file_mmap_ranges_lock.unlock();
        state.callback_safe = state.callback_safe && REGISTRY_UNLOCKED && state.task->usercopy_pagemap_accesses.load() == 0 &&
                              phys::page_ref_get(state.first_page) == 1 && phys::page_ref_get(state.second_page) == 1 &&
                              file->refcount.load() == 3;
        if (state.dirty_later) {
            state.dirty_later = false;
            virt::UserPagePin pin{};
            bool const PINNED = virt::pin_user_page(state.task->pagemap, BASE + PAGE, true, pin);
            state.callback_safe = PINNED && state.callback_safe;
            if (PINNED) {
                state.callback_safe = virt::user_page_pin_commit_write(pin) && state.callback_safe;
                virt::unpin_user_page(pin);
            }
        }
        if (state.backend_error != 0) return state.backend_error;
        size_t const SHORT_COUNT = std::min(count, size_t{7});
        if (offset > state.bytes.size() || SHORT_COUNT > state.bytes.size() - offset) return -EFBIG;
        std::memcpy(state.bytes.data() + offset, bytes, SHORT_COUNT);
        return static_cast<ssize_t>(SHORT_COUNT);
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        bool const REGISTRY_UNLOCKED = g_file_mmap_ranges_lock.try_lock();
        if (REGISTRY_UNLOCKED) g_file_mmap_ranges_lock.unlock();
        bool const PUBLICATION_UNLOCKED = g_shared_vmem_publication_lock.try_lock();
        if (PUBLICATION_UNLOCKED) g_shared_vmem_publication_lock.unlock();
        state.callback_safe = state.callback_safe && REGISTRY_UNLOCKED && PUBLICATION_UNLOCKED;
        return 0;
    };
    auto run = [&](unsigned mode, bool readonly, bool dirty, uint64_t offset, uint64_t length, int expected, bool quiesced, bool teardown,
                   bool dirty_later, int backend_error) -> bool {
        ker::mod::sched::task::Task task;
        task.pagemap = pagemap;
        State first{.dirty_later = dirty_later, .task = &task};
        State second{.backend_error = backend_error, .task = &task};
        auto* first_file = new (std::nothrow) vfs::File{};
        auto* second_file = new (std::nothrow) vfs::File{};
        if (first_file == nullptr || second_file == nullptr) {
            delete first_file;
            delete second_file;
            return false;
        }
        for (auto* file : {first_file, second_file}) {
            file->fs_type = vfs::FSType::SOCKET;
            file->fops = &operations;
            file->refcount.store(1);
            file->open_flags = 2;
            file->pos = 19;
        }
        first_file->private_data = &first;
        second_file->private_data = &second;
        second_file->open_flags = static_cast<int>(mode);
        second_file->mount_policy.read_only = readonly;
        first.first_page = phys::page_alloc(ker::mod::mm::PhysicalPageOwner::SELFTEST, PAGE, "msync-first");
        first.second_page = phys::page_alloc(ker::mod::mm::PhysicalPageOwner::SELFTEST, PAGE, "msync-second");
        second.first_page = first.first_page;
        second.second_page = first.second_page;
        if (first.first_page == nullptr || first.second_page == nullptr) {
            if (first.first_page != nullptr) phys::page_free(first.first_page);
            if (first.second_page != nullptr) phys::page_free(first.second_page);
            vfs::vfs_put_file(first_file);
            vfs::vfs_put_file(second_file);
            return false;
        }
        auto phys_address = [](void* page) -> uint64_t {
            return reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        };
        std::memset(first.first_page, 0x41, PAGE);
        std::memset(first.second_page, 0x42, PAGE);
        virt::map_page(pagemap, BASE, phys_address(first.first_page), paging::page_types::USER);
        virt::map_page(pagemap, BASE + PAGE, phys_address(first.second_page), paging::page_types::USER);
        bool ok = true;
        auto mark_dirty = [&](uint64_t address) {
            virt::UserPagePin pin{};
            if (!virt::pin_user_page(pagemap, address, true, pin)) {
                ok = false;
                return;
            }
            ok = virt::user_page_pin_commit_write(pin) && ok;
            virt::unpin_user_page(pin);
        };
        mark_dirty(BASE);
        if (dirty) mark_dirty(BASE + PAGE);
        int result = 0;
        {
            SharedVmemPublicationGuard guard;
            ok = register_file_mmap_range(pagemap, BASE, COUNT, 0, first_file, true) == 0 && ok;
            ok = register_file_mmap_range(pagemap, BASE + PAGE, length, offset, second_file, true) == 0 && ok;
            if (!teardown) result = sync_file_mmap_range_impl(pagemap, quiesced ? nullptr : &task, BASE, PAGE + length);
        }
        if (teardown) {
            release_file_mmap_ranges_for_pagemap(pagemap);
            result = expected;  // The teardown API logs admission failures and returns void.
        }
        bool const PREFLIGHT_DENIAL = expected != 0 && backend_error == 0;
        ok = ok && result == expected && first_file->pos == 19 && second_file->pos == 19 && first.callback_safe && second.callback_safe &&
             first_file->refcount.load() == (teardown ? 1 : 2) && second_file->refcount.load() == (teardown ? 1 : 2);
        if (PREFLIGHT_DENIAL) {
            ok = ok && first.writes == 0 && second.writes == 0 && first.bytes == std::array<uint8_t, COUNT>{} &&
                 second.bytes == std::array<uint8_t, COUNT>{};
        } else {
            std::array<uint8_t, COUNT> expected_first{};
            expected_first.fill(0x41);
            ok = ok && first.writes == 3 && first.bytes == expected_first;
            if (backend_error != 0)
                ok = ok && second.writes == 1 && second.bytes == std::array<uint8_t, COUNT>{};
            else if (dirty) {
                std::array<uint8_t, COUNT> expected_second{};
                expected_second.fill(0x42);
                ok = ok && second.writes == 3 && second.bytes == expected_second;
            } else
                ok = ok && second.writes == 0 && second.bytes == std::array<uint8_t, COUNT>{};
        }
        if (dirty_later) {
            // A newly dirty invalid interval is excluded from this completed
            // operation. A subsequent call rejects before repeating any write.
            unsigned const WRITES = first.writes;
            SharedVmemPublicationGuard guard;
            ok = ok && sync_file_mmap_range_impl(pagemap, quiesced ? nullptr : &task, BASE, PAGE + length) == -EOVERFLOW &&
                 first.writes == WRITES && second.writes == 0;
        }
        for (uint64_t address : {BASE, BASE + PAGE}) {
            virt::UserPagePin pin{};
            bool const PINNED = virt::pin_user_page(pagemap, address, false, pin);
            ok = PINNED && ok;
            if (PINNED) {
                ok = ok && pin.dirty == (address == BASE || dirty || dirty_later);
                virt::unpin_user_page(pin);
            }
        }
        ok = ok && static_cast<uint8_t*>(first.first_page)[0] == 0x41 && static_cast<uint8_t*>(first.second_page)[0] == 0x42 &&
             phys::page_ref_get(first.first_page) == 1 && phys::page_ref_get(first.second_page) == 1;
        unregister_file_mmap_range(pagemap, BASE, PAGE + length);
        rollback_mapped_pages(&task, BASE, 2);
        ok = ok && first_file->refcount.load() == 1 && second_file->refcount.load() == 1;
        vfs::vfs_put_file(first_file);
        vfs::vfs_put_file(second_file);
        return ok && first.closes == 1 && second.closes == 1 && first.callback_safe && second.callback_safe;
    };
    for (bool quiesced : {false, true}) {
        for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
            for (bool readonly : {false, true}) {
                for (bool dirty : {false, true}) {
                    int const EXPECTED = !dirty ? 0 : mode == 0 || mode == vfs::FILE_ACCESS_SEARCH_ONLY ? -EBADF : readonly ? -EROFS : 0;
                    if (!run(mode, readonly, dirty, 0, COUNT, EXPECTED, quiesced, false, false, 0)) return false;
                }
            }
        }
        if (!run(2, false, true, MAX_OFFSET - 7, COUNT, -EOVERFLOW, quiesced, false, false, 0) ||
            !run(2, false, true, UINT64_MAX, COUNT, -EOVERFLOW, quiesced, false, false, 0) ||
            !run(2, false, true, MAX_OFFSET - PAGE + 1, PAGE + COUNT, -EOVERFLOW, quiesced, false, false, 0) ||
            !run(2, false, false, MAX_OFFSET - 7, COUNT, 0, quiesced, false, true, 0) ||
            !run(2, false, true, 0, COUNT, -EIO, quiesced, false, false, -EIO))
            return false;
    }
    return run(2, true, true, 0, COUNT, -EROFS, true, true, false, 0);
}

auto selftest_file_writeback_policy() -> bool {
    namespace vfs = ker::vfs;
    struct State {
        std::array<uint8_t, 8> bytes{};
        unsigned writes{};
        unsigned fail_call{};
    };
    vfs::FileOperations operations{};
    operations.vfs_write = [](vfs::File* file, const void* data, size_t count, size_t offset) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.writes;
        if (state.writes == state.fail_call) return -EIO;
        size_t const SHORT_COUNT = std::min(count, size_t{2});
        if (offset > state.bytes.size() || SHORT_COUNT > state.bytes.size() - offset) return -EFBIG;
        std::memcpy(state.bytes.data() + offset, data, SHORT_COUNT);
        return static_cast<ssize_t>(SHORT_COUNT);
    };
    constexpr std::array<uint8_t, 4> DATA{'k', 'e', 'e', 'p'};
    constexpr uint64_t MAX_OFFSET = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
        for (bool readonly : {false, true}) {
            State state;
            vfs::File file;
            file.fs_type = vfs::FSType::SOCKET;
            file.fops = &operations;
            file.private_data = &state;
            file.open_flags = static_cast<int>(mode);
            file.mount_policy.read_only = readonly;
            file.refcount.store(1, std::memory_order_relaxed);
            file.pos = 9;
            int const EXPECTED = mode == 0 || mode == vfs::FILE_ACCESS_SEARCH_ONLY ? -EBADF : readonly ? -EROFS : 0;
            if (write_file_mapping_bytes(&file, DATA.data(), DATA.size(), 1) != EXPECTED || file.pos != 9 || file.refcount.load() != 1)
                return false;
            if (EXPECTED != 0) {
                if (state.writes != 0 || state.bytes != std::array<uint8_t, 8>{}) return false;
            } else if (state.writes != 2 || std::memcmp(state.bytes.data() + 1, DATA.data(), DATA.size()) != 0)
                return false;
            auto const BEFORE = state;
            if (write_file_mapping_bytes(&file, DATA.data(), DATA.size(), MAX_OFFSET - 1) != -EOVERFLOW ||
                write_file_mapping_bytes(&file, DATA.data(), 1, UINT64_MAX) != -EOVERFLOW ||
                write_file_mapping_bytes(&file, DATA.data(), 0, 0) != 0 || state.writes != BEFORE.writes || state.bytes != BEFORE.bytes)
                return false;
        }
    }
    State state{.fail_call = 2};
    vfs::File file;
    file.fs_type = vfs::FSType::SOCKET;
    file.fops = &operations;
    file.private_data = &state;
    file.open_flags = 2;
    if (write_file_mapping_bytes(&file, DATA.data(), DATA.size(), 1) != -EIO || state.writes != 2 || state.bytes[1] != DATA[0] ||
        state.bytes[2] != DATA[1] || state.bytes[3] != 0 || file.pos != 0)
        return false;
    return write_file_mapping_bytes(nullptr, DATA.data(), DATA.size(), 0) == -ENOSYS;
}
#endif

SharedVmemPublicationGuard::SharedVmemPublicationGuard() { g_shared_vmem_publication_lock.lock(); }

SharedVmemPublicationGuard::~SharedVmemPublicationGuard() { g_shared_vmem_publication_lock.unlock(); }

auto materialize_lazy_file_page(ker::mod::sched::task::Task* task, const ker::mod::sched::task::LazyVmemRange& range, uint64_t page_vaddr,
                                const ker::mod::mm::paging::PageFault& fault) -> bool {
    return materialize_lazy_file_page_impl(task, range, page_vaddr, fault);
}

auto file_mmap_cache_stats() -> FileMmapCacheStats {
    FileMmapCacheStats stats{
        .pages = g_file_mmap_cache_pages.load(std::memory_order_relaxed),
        .bytes = 0,
        .capacity_pages = FILE_MMAP_CACHE_PAGES,
    };
    stats.bytes = stats.pages * ker::mod::mm::paging::PAGE_SIZE;
    return stats;
}

auto file_mmap_cache_reclaim(size_t max_pages) -> size_t {
    if (max_pages == 0) {
        return 0;
    }

    constexpr size_t RELEASE_BATCH = 128;
    std::array<void*, RELEASE_BATCH> release_pages{};
    size_t reclaimed = 0;

    for (size_t lock_index = 0; lock_index < FILE_MMAP_CACHE_LOCK_COUNT && reclaimed < max_pages; ++lock_index) {
        auto& cache_lock = g_file_mmap_cache_locks.at(lock_index);
        bool more = true;
        while (more && reclaimed < max_pages) {
            size_t release_count = 0;
            more = false;
            cache_lock.mutex.lock();
            for (size_t set_index = lock_index; set_index < FILE_MMAP_CACHE_SET_COUNT; set_index += FILE_MMAP_CACHE_LOCK_COUNT) {
                for (auto& entry : g_file_mmap_cache.at(set_index).ways) {
                    if (entry.page == nullptr) {
                        continue;
                    }
                    if (release_count == release_pages.size() || reclaimed + release_count == max_pages) {
                        more = true;
                        break;
                    }
                    release_pages.at(release_count++) = entry.page;
                    entry = {};
                }
                if (more) {
                    break;
                }
            }
            cache_lock.mutex.unlock();

            if (release_count != 0) {
                g_file_mmap_cache_pages.fetch_sub(release_count, std::memory_order_relaxed);
            }

            for (size_t i = 0; i < release_count; ++i) {
                release_file_mmap_cache_page(release_pages.at(i));
                release_pages.at(i) = nullptr;
            }
            reclaimed += release_count;
        }
    }

    return reclaimed;
}

namespace {
struct FileMmapBoundedReclaimResult {
    size_t reclaimed{};
    size_t scanned{};
};

auto file_mmap_cache_reclaim_bounded(size_t max_pages, size_t scan_budget) -> FileMmapBoundedReclaimResult {
    FileMmapBoundedReclaimResult result{};
    if (max_pages == 0 || scan_budget == 0) {
        return result;
    }

    size_t const MAX_SCAN = std::min(scan_budget, FILE_MMAP_CACHE_PAGES);
    uint64_t const START = g_file_mmap_cache_scan_cursor.fetch_add(MAX_SCAN, std::memory_order_relaxed);
    while (result.scanned < MAX_SCAN && result.reclaimed < max_pages) {
        size_t const SLOT = static_cast<size_t>((START + result.scanned) % FILE_MMAP_CACHE_PAGES);
        size_t const SET_INDEX = SLOT / FILE_MMAP_CACHE_WAYS;
        size_t const WAY_INDEX = SLOT % FILE_MMAP_CACHE_WAYS;
        size_t const LOCK_INDEX = SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1);
        auto& cache_lock = g_file_mmap_cache_locks.at(LOCK_INDEX);
        void* page = nullptr;

        cache_lock.mutex.lock();
        auto& entry = g_file_mmap_cache.at(SET_INDEX).ways.at(WAY_INDEX);
        result.scanned++;
        if (entry.page != nullptr) {
            page = entry.page;
            entry = {};
            g_file_mmap_cache_pages.fetch_sub(1, std::memory_order_relaxed);
        }
        cache_lock.mutex.unlock();

        if (page != nullptr) {
            release_file_mmap_cache_page(page);
            result.reclaimed++;
        }
    }
    return result;
}

auto file_mmap_cache_reclaim_count(void*, const ker::mod::mm::reclaim::ReclaimRequest&) -> ker::mod::mm::reclaim::ReclaimCount {
    return {.reclaimable = g_file_mmap_cache_pages.load(std::memory_order_relaxed)};
}

auto file_mmap_cache_reclaim_scan(void*, const ker::mod::mm::reclaim::ReclaimRequest& request) -> ker::mod::mm::reclaim::ReclaimScanResult {
    size_t const PAGE_BUDGET = static_cast<size_t>(std::min<uint64_t>(request.budget_units, SIZE_MAX));
    size_t const SCAN_BUDGET = static_cast<size_t>(std::min<uint64_t>(request.scan_budget_units, SIZE_MAX));
    FileMmapBoundedReclaimResult const RESULT = file_mmap_cache_reclaim_bounded(PAGE_BUDGET, SCAN_BUDGET);
    return {
        .scanned = RESULT.scanned,
        .reclaimed = RESULT.reclaimed,
        .has_more = g_file_mmap_cache_pages.load(std::memory_order_relaxed) != 0,
    };
}
}  // namespace

void file_mmap_cache_register_shrinker() {
    bool expected = false;
    if (!g_file_mmap_cache_shrinker_registered.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                                       std::memory_order_acquire)) {
        return;
    }
    ker::mod::mm::reclaim::Shrinker const SHRINKER{
        .name = "file_mmap_cache",
        .unit = ker::mod::mm::reclaim::ReclaimUnit::PAGES,
        .scan_unit = ker::mod::mm::reclaim::ReclaimUnit::PAGES,
        .rank = 3,
        .min_priority = ker::mod::mm::reclaim::ReclaimPriority::LOW,
        .capabilities = ker::mod::mm::reclaim::RECLAIM_MAY_BLOCK,
        .max_batch_units = 128,
        .max_scan_units = 4096,
        .count = file_mmap_cache_reclaim_count,
        .scan = file_mmap_cache_reclaim_scan,
    };
    if (!ker::mod::mm::reclaim::register_shrinker(SHRINKER)) {
        g_file_mmap_cache_shrinker_registered.store(false, std::memory_order_release);
    }
}

auto clone_file_mmap_ranges_for_pagemap_locked(ker::mod::mm::paging::PageTable* src, ker::mod::mm::paging::PageTable* dst) -> bool {
    if (src == nullptr || dst == nullptr || src == dst) {
        return true;
    }

    bool ok = true;
    g_file_mmap_ranges_lock.lock();
    size_t const INITIAL_COUNT = g_file_mmap_ranges.size();
    for (size_t i = 0; i < INITIAL_COUNT; ++i) {
        auto const& range = g_file_mmap_ranges.at(i);
        if (range.pagemap != src || range.file == nullptr) {
            continue;
        }

        FileMmapRange clone = range;
        clone.pagemap = dst;
        ker::vfs::vfs_retain_file(clone.file);
        if (!g_file_mmap_ranges.push_back(clone)) {
            ker::vfs::vfs_put_file(clone.file);
            ok = false;
            break;
        }
    }
    g_file_mmap_ranges_lock.unlock();

    return ok;
}

void release_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* pagemap) {
    if (pagemap == nullptr) {
        return;
    }

    // Teardown is one writeback operation: a later denied range must not be
    // discovered only after an earlier registry row has already been flushed.
    int const SYNC_RET = sync_file_mmap_range_quiesced(pagemap, 0, USER_SPACE_END + 1);
    if (SYNC_RET < 0) {
        log::warn("file mmap release sync failed: pagemap=%p ret=%d", static_cast<void*>(pagemap), SYNC_RET);
    }

    for (;;) {
        FileMmapRange range{};
        bool found = false;
        g_file_mmap_ranges_lock.lock();
        for (const auto& candidate : g_file_mmap_ranges) {
            if (candidate.pagemap == pagemap) {
                range = candidate;
                found = true;
                break;
            }
        }
        g_file_mmap_ranges_lock.unlock();

        if (!found) {
            return;
        }

        int const UNREGISTER_RET = unregister_file_mmap_range(pagemap, range.start, range.length);
        if (UNREGISTER_RET < 0) {
            log::warn("file mmap release unregister failed: pagemap=%p start=0x%llx length=0x%llx ret=%d", static_cast<void*>(pagemap),
                      static_cast<unsigned long long>(range.start), static_cast<unsigned long long>(range.length), UNREGISTER_RET);
            return;
        }
    }
}

auto anon_mremap(uint64_t old_addr, uint64_t old_size, uint64_t new_size, uint64_t flags) -> uint64_t {
    auto* task = get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }
    if (old_addr == 0 || old_size == 0 || new_size == 0 || (old_addr % ker::mod::mm::paging::PAGE_SIZE) != 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    uint64_t OLD_SIZE = 0;
    uint64_t NEW_SIZE = 0;
    if (align_user_vmem_size(old_size, &OLD_SIZE) < 0 || align_user_vmem_size(new_size, &NEW_SIZE) < 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }
    if (old_addr + OLD_SIZE > USER_SPACE_END || old_addr + OLD_SIZE < old_addr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    if (NEW_SIZE == OLD_SIZE) {
        return old_addr;
    }
    if (NEW_SIZE < OLD_SIZE) {
        uint64_t const TAIL_ADDR = old_addr + NEW_SIZE;
        uint64_t const TAIL_SIZE = OLD_SIZE - NEW_SIZE;
        auto const RESULT = anon_free(TAIL_ADDR, TAIL_SIZE);
        return static_cast<int64_t>(RESULT) < 0 ? RESULT : old_addr;
    }
    if ((flags & MREMAP_MAYMOVE) == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    auto const NEW_ADDR_RESULT = anon_allocate(0, NEW_SIZE, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE,
                                               ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_ANONYMOUS);
    if (static_cast<int64_t>(NEW_ADDR_RESULT) < 0) {
        return NEW_ADDR_RESULT;
    }

    uint64_t const NEW_ADDR = NEW_ADDR_RESULT;
    uint64_t const COPY_SIZE = OLD_SIZE < NEW_SIZE ? OLD_SIZE : NEW_SIZE;
    for (uint64_t off = 0; off < COPY_SIZE; off += ker::mod::mm::paging::PAGE_SIZE) {
        uint64_t const SRC_VA = old_addr + off;
        uint64_t const DST_VA = NEW_ADDR + off;

        // Complete both pins before cleanup can reacquire VM publication.
        uint64_t const COPY_RESULT = [&]() -> uint64_t {
            ker::mod::sys::usercopy::StableUserPage src_page;
            if (!ker::mod::sys::usercopy::pin_task_user_page(*task, SRC_VA, false, true, src_page)) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }
            if (!ker::mod::mm::virt::ensure_user_page_writable(task, DST_VA)) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
            }

            ker::mod::sys::usercopy::StableUserPage dst_page;
            if (!ker::mod::sys::usercopy::pin_task_user_page(*task, DST_VA, true, false, dst_page)) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }

            {
                ker::mod::sys::usercopy::UserAccessGuard const ACCESS{};
                std::memcpy(dst_page.kernel_address(), src_page.kernel_address(), ker::mod::mm::paging::PAGE_SIZE);
            }
            bool const SOURCE_STILL_MAPPED = src_page.still_mapped();
            bool const DESTINATION_COMMITTED = dst_page.commit_write();
            if (!SOURCE_STILL_MAPPED || !DESTINATION_COMMITTED) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }
            return 0;
        }();
        if (static_cast<int64_t>(COPY_RESULT) < 0) {
            anon_free(NEW_ADDR, NEW_SIZE);
            return COPY_RESULT;
        }
    }

    auto const FREE_RESULT = anon_free(old_addr, OLD_SIZE);
    if (static_cast<int64_t>(FREE_RESULT) < 0) {
        (void)anon_free(NEW_ADDR, NEW_SIZE);
        return FREE_RESULT;
    }
    return NEW_ADDR;
}

auto mmap_msync(uint64_t addr, uint64_t size, uint64_t /*flags*/) -> uint64_t {
    auto* task = get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }
    if (addr == 0 || size == 0 || (addr % ker::mod::mm::paging::PAGE_SIZE) != 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    if (align_user_vmem_size(size, &size) < 0 || addr > USER_SPACE_END - size) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Keep the collected File identity bound to its pages until snapshots and
    // writes finish, matching munmap and MAP_FIXED's existing serialization.
    SharedVmemPublicationGuard publication_guard;
    int const RET = sync_file_mmap_range(*task, addr, size);
    return RET < 0 ? static_cast<uint64_t>(RET) : 0;
}

// Main syscall handler
auto swap_control_for_subject(ker::mod::sched::task::Task* task, uint64_t op, uint64_t address, uint64_t flags) -> uint64_t {
    auto const OP = static_cast<ker::abi::vmem::ops>(op);
    if (OP != ker::abi::vmem::ops::SWAPON && OP != ker::abi::vmem::ops::SWAPOFF) return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    if (task == nullptr) return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    int const SCOPE = ker::mod::security::compartment::host_namespace_admission(task);
    if (SCOPE != 0) return static_cast<uint64_t>(SCOPE);
    std::array<char, SWAP_PATH_MAX> path{};
    if (!ker::mod::sys::usercopy::copy_cstring_from_task_strict(*task, address, path.data(), path.size()))
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    return static_cast<uint64_t>(OP == ker::abi::vmem::ops::SWAPON ? ker::mod::mm::swap::swapon_path(path.data(), static_cast<int>(flags))
                                                                   : ker::mod::mm::swap::swapoff_path(path.data()));
}

auto sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t {
    switch (static_cast<ker::abi::vmem::ops>(op)) {
        case ker::abi::vmem::ops::ANON_ALLOCATE: {
            // a1: hint address
            // a2: size
            // a3: protection flags
            // a4: mapping flags
            return anon_allocate(a1, a2, a3, a4);
        }

        case ker::abi::vmem::ops::ANON_FREE: {
            // a1: address
            // a2: size
            return anon_free(a1, a2);
        }

        case ker::abi::vmem::ops::PROTECT: {
            // a1: address
            // a2: size
            // a3: new protection flags
            auto* task = get_current_task();
            if (task == nullptr || task->pagemap == nullptr) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }

            uint64_t const ADDR = a1;
            uint64_t size = a2;
            uint64_t const PROT = a3;

            if (ADDR % ker::mod::mm::paging::PAGE_SIZE != 0) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            int const SIZE_RET = align_user_vmem_size(size, &size);
            if (SIZE_RET < 0) {
                return static_cast<uint64_t>(SIZE_RET);
            }

            if (ADDR + size > USER_SPACE_END || ADDR + size < ADDR) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            SharedVmemPublicationGuard publication_guard;
            if (overlaps_kernel_managed_user_layout_locked(task, ADDR, size)) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            int const FILE_POLICY_RET = authorize_shared_file_protection_locked(task->pagemap, ADDR, size, PROT);
            if (FILE_POLICY_RET != 0) return static_cast<uint64_t>(FILE_POLICY_RET);

            // Validate the entire request before either lazy-range metadata or
            // leaf permissions change. SHM publication uses this same guard.
            if ((PROT & ker::abi::vmem::PROT_WRITE) != 0) {
                uint64_t const END = ADDR + size;
                uint64_t cursor = ADDR;
                while (cursor < END) {
                    auto const OCCUPIED = first_pagemap_occupied_range(task->pagemap, cursor, END);
                    if (!OCCUPIED.found) {
                        break;
                    }
                    for (uint64_t page = OCCUPIED.start; page < OCCUPIED.end; page += ker::mod::mm::paging::PAGE_SIZE) {
                        if (ker::mod::mm::virt::is_page_write_restricted(task->pagemap, page)) {
                            return static_cast<uint64_t>(-EACCES);
                        }
                    }
                    uint64_t const NEXT = page_align_up(OCCUPIED.end);
                    if (NEXT <= cursor) {
                        break;
                    }
                    cursor = NEXT;
                }
            }

            if (!protect_shared_vmem_range_locked(task, ADDR, size, PROT)) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
            }

            auto page_flags = prot_to_page_flags(PROT);
            uint64_t const END = ADDR + size;
            uint64_t cursor = ADDR;
            while (cursor < END) {
                auto const OCCUPIED = first_pagemap_occupied_range(task->pagemap, cursor, END);
                if (!OCCUPIED.found) {
                    break;
                }

                for (uint64_t current_vaddr = OCCUPIED.start; current_vaddr < OCCUPIED.end;
                     current_vaddr += ker::mod::mm::paging::PAGE_SIZE) {
                    if (ker::mod::mm::virt::protect_anonymous_swap_page(task->pagemap, current_vaddr, page_flags)) {
                        continue;
                    }
                    if (ker::mod::mm::virt::is_page_mapped(task->pagemap, current_vaddr)) {
                        ker::mod::mm::virt::unify_page_flags(task->pagemap, current_vaddr, page_flags);
                    } else if (ker::mod::mm::virt::is_page_reserved(task->pagemap, current_vaddr) &&
                               !materialize_reserved_page(task, current_vaddr, PROT)) {
                        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
                    }
                }

                uint64_t const NEXT = page_align_up(OCCUPIED.end);
                if (NEXT <= cursor) {
                    break;
                }
                cursor = NEXT;
            }

            return 0;
        }

        case ker::abi::vmem::ops::MREMAP: {
            // a1: old address
            // a2: old size
            // a3: new size
            // a4: flags
            return anon_mremap(a1, a2, a3, a4);
        }

        case ker::abi::vmem::ops::MSYNC: {
            // a1: address
            // a2: size
            // a3: flags
            return mmap_msync(a1, a2, a3);
        }

        case ker::abi::vmem::ops::SWAPON:
        case ker::abi::vmem::ops::SWAPOFF:
            return swap_control_for_subject(get_current_task(), op, a1, a2);

        default:
            log::warn("invalid operation %llu", op);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }
}

auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t {
    if ((flags & ker::abi::vmem::MAP_ANONYMOUS) != 0) {
        uint64_t const RESULT = anon_allocate(hint, size, prot, flags);
        if (static_cast<int64_t>(RESULT) < 0) {
            log::warn("anon mmap failed: ret=%ld hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx",
                      static_cast<long>(static_cast<int64_t>(RESULT)), static_cast<unsigned long long>(hint),
                      static_cast<unsigned long long>(size), static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot));
        }
        return RESULT;
    }

    if (fd > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        log::warn("file mmap failed: fd out of range fd=0x%llx hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx off=0x%llx",
                  static_cast<unsigned long long>(fd), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                  static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot), static_cast<unsigned long long>(offset));
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    uint64_t const RESULT = file_allocate(get_current_task(), hint, size, prot, flags, static_cast<int>(fd), offset);
    if (static_cast<int64_t>(RESULT) < 0) {
        log::warn("file mmap failed: ret=%ld fd=%llu hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx off=0x%llx",
                  static_cast<long>(static_cast<int64_t>(RESULT)), static_cast<unsigned long long>(fd),
                  static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size), static_cast<unsigned long long>(flags),
                  static_cast<unsigned long long>(prot), static_cast<unsigned long long>(offset));
    }
    return RESULT;
}

}  // namespace ker::syscall::vmem
