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
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/file.hpp>
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

namespace {
// Get the current task
inline auto get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

ker::mod::sys::Mutex g_mmap_reserve_lock;

constexpr bool ENABLE_WATCHED_MMAP_LOGS = false;
constexpr uint64_t WATCH_MMAP_VADDR = 0x00001000007da000ULL;

inline auto is_watched_mmap_vaddr(uint64_t vaddr) -> bool { return ENABLE_WATCHED_MMAP_LOGS && vaddr == WATCH_MMAP_VADDR; }

auto clamp_perf_u16(uint64_t value) -> uint16_t { return value > UINT16_MAX ? UINT16_MAX : static_cast<uint16_t>(value); }

auto clamp_perf_u32(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

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

struct FileMmapRange {
    ker::mod::mm::paging::PageTable* pagemap = nullptr;
    uint64_t start = 0;
    uint64_t length = 0;
    uint64_t file_offset = 0;
    ker::vfs::File* file = nullptr;
};

struct FileMmapSyncRange {
    uint64_t mapping_start = 0;
    uint64_t sync_start = 0;
    uint64_t sync_end = 0;
    uint64_t file_offset = 0;
    ker::vfs::File* file = nullptr;
};

enum class FileMmapCacheInsertResult : uint8_t {
    INSERTED,
    DUPLICATE,
    EVICTED,
};

constexpr size_t FILE_MMAP_CACHE_SET_COUNT = 2048;
constexpr size_t FILE_MMAP_CACHE_WAYS = 4;
constexpr size_t FILE_MMAP_CACHE_PAGES = FILE_MMAP_CACHE_SET_COUNT * FILE_MMAP_CACHE_WAYS;
static_assert((FILE_MMAP_CACHE_SET_COUNT & (FILE_MMAP_CACHE_SET_COUNT - 1)) == 0);
std::array<FileMmapPageCacheSet, FILE_MMAP_CACHE_SET_COUNT> g_file_mmap_cache{};
ker::mod::sys::Mutex g_file_mmap_cache_lock;
std::atomic<uint64_t> g_file_mmap_cache_clock{0};
ker::util::SmallVec<FileMmapRange, 32> g_file_mmap_ranges;
ker::mod::sys::Mutex g_file_mmap_ranges_lock;

auto file_mmap_key_equal(const FileMmapPageKey& lhs, const FileMmapPageKey& rhs) -> bool {
    return lhs.dev == rhs.dev && lhs.ino == rhs.ino && lhs.size == rhs.size && lhs.mtime_sec == rhs.mtime_sec &&
           lhs.mtime_nsec == rhs.mtime_nsec && lhs.ctime_sec == rhs.ctime_sec && lhs.ctime_nsec == rhs.ctime_nsec &&
           lhs.offset == rhs.offset;
}

auto file_mmap_key_hash(const FileMmapPageKey& key) -> uint64_t {
    uint64_t hash = key.dev ^ (key.ino << 7U) ^ (key.ino >> 3U) ^ key.size ^ key.offset;
    hash ^= static_cast<uint64_t>(key.mtime_sec) + 0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.mtime_nsec) + 0xbf58476d1ce4e5b9ULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.ctime_sec) + 0x94d049bb133111ebULL + (hash << 6U) + (hash >> 2U);
    hash ^= static_cast<uint64_t>(key.ctime_nsec) + 0x2545f4914f6cdd1dULL + (hash << 6U) + (hash >> 2U);
    hash ^= hash >> 33U;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33U;
    return hash;
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
    return (prot & ker::abi::vmem::PROT_WRITE) == 0 && (st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG && st.st_ino != 0;
}

auto file_mmap_should_track(const ker::vfs::Stat& st, uint64_t flags) -> bool {
    return (flags & ker::abi::vmem::MAP_SHARED) != 0 && (st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG;
}

auto is_prot_none(uint64_t prot) -> bool { return prot == ker::abi::vmem::PROT_NONE; }

auto ranges_overlap(uint64_t start, uint64_t end, uint64_t other_start, uint64_t other_end) -> bool {
    return start < other_end && other_start < end;
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
            auto left = range;
            left.end = vaddr;
            ok = push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }
        if (END < range.end) {
            auto right = range;
            right.start = END;
            ok = push_lazy_vmem_range_copy(rewritten, right);
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
    for (const auto& range : task->lazy_vmem_ranges) {
        if (!ranges_overlap(new_range.start, new_range.end, range.start, range.end)) {
            ok = push_lazy_vmem_range_copy(rewritten, range);
            if (!ok) {
                break;
            }
            continue;
        }

        if (range.start < new_range.start) {
            auto left = range;
            left.end = new_range.start;
            ok = push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }
        if (new_range.end < range.end) {
            auto right = range;
            right.start = new_range.end;
            ok = push_lazy_vmem_range_copy(rewritten, right);
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
            auto left = range;
            left.end = vaddr;
            ok = push_lazy_vmem_range_copy(rewritten, left);
            if (!ok) {
                break;
            }
        }

        auto middle = range;
        middle.start = std::max(range.start, vaddr);
        middle.end = std::min(range.end, END);
        middle.prot = prot;
        ok = push_lazy_vmem_range_copy(rewritten, middle);
        if (!ok) {
            break;
        }

        if (END < range.end) {
            auto right = range;
            right.start = END;
            ok = push_lazy_vmem_range_copy(rewritten, right);
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
auto update_shared_vmem_ranges(ker::mod::sched::task::Task* task, Fn update) -> bool {
    if (task == nullptr || task->pagemap == nullptr) {
        return update(task);
    }

    auto* const PAGEMAP = task->pagemap;
    bool touched_current = false;
    bool ok = true;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }

        if (candidate->pagemap == PAGEMAP && candidate->type != ker::mod::sched::task::TaskType::DAEMON) {
            touched_current = touched_current || candidate == task;
            ok = update(candidate) && ok;
        }

        candidate->release();
    }

    if (!touched_current) {
        ok = update(task) && ok;
    }
    return ok;
}

auto remove_shared_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) -> bool {
    return update_shared_vmem_ranges(
        task, [vaddr, size](ker::mod::sched::task::Task* candidate) { return remove_lazy_vmem_range(candidate, vaddr, size); });
}

auto add_shared_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags) -> bool {
    return update_shared_vmem_ranges(task, [vaddr, size, prot, flags](ker::mod::sched::task::Task* candidate) {
        return add_lazy_vmem_range(candidate, vaddr, size, prot, flags);
    });
}

auto add_shared_lazy_file_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags,
                                     ker::vfs::File* file, uint64_t file_offset, const ker::vfs::Stat& st) -> bool {
    return update_shared_vmem_ranges(task, [vaddr, size, prot, flags, file, file_offset, &st](ker::mod::sched::task::Task* candidate) {
        return add_lazy_file_vmem_range(candidate, vaddr, size, prot, flags, file, file_offset, st);
    });
}

auto protect_shared_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    return update_shared_vmem_ranges(task, [vaddr, size, prot](ker::mod::sched::task::Task* candidate) {
        return protect_lazy_vmem_range(candidate, vaddr, size, prot);
    });
}

auto file_mmap_cache_lookup(const FileMmapPageKey& key) -> void* {
    uint64_t const USE_STAMP = g_file_mmap_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    auto& set = g_file_mmap_cache.at(file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1));

    g_file_mmap_cache_lock.lock();
    for (auto& entry : set.ways) {
        if (entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            entry.last_used = USE_STAMP;
            ker::mod::mm::phys::page_ref_inc(entry.page);
            void* page = entry.page;
            g_file_mmap_cache_lock.unlock();
            return page;
        }
    }
    g_file_mmap_cache_lock.unlock();

    return nullptr;
}

auto file_mmap_cache_insert_or_discard(const FileMmapPageKey& key, void* new_page, void** page_for_mapping) -> FileMmapCacheInsertResult {
    uint64_t const USE_STAMP = g_file_mmap_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    auto& set = g_file_mmap_cache.at(file_mmap_key_hash(key) & (FILE_MMAP_CACHE_SET_COUNT - 1));

    g_file_mmap_cache_lock.lock();
    for (auto& entry : set.ways) {
        if (entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            entry.last_used = USE_STAMP;
            ker::mod::mm::phys::page_ref_inc(entry.page);
            *page_for_mapping = entry.page;
            g_file_mmap_cache_lock.unlock();
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
    g_file_mmap_cache_lock.unlock();

    if (evicted != nullptr) {
        ker::mod::mm::phys::page_ref_dec(evicted);
        return FileMmapCacheInsertResult::EVICTED;
    }

    return FileMmapCacheInsertResult::INSERTED;
}

auto file_mmap_cached_page_for_file(ker::vfs::File* file, const ker::vfs::Stat& st, uint64_t file_offset, void** page_out) -> bool {
    if (page_out == nullptr) {
        return false;
    }
    if (file == nullptr) {
        return false;
    }

    auto* task = get_current_task();
    FileMmapPageKey const KEY = file_mmap_page_key(st, file_offset);
    uint64_t const LOOKUP_STARTED_US = ker::mod::time::get_us();
    if (void* const CACHED_PAGE = file_mmap_cache_lookup(KEY); CACHED_PAGE != nullptr) {
        *page_out = CACHED_PAGE;
        record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_HIT, file_offset,
                                      vmem_latency_since(LOOKUP_STARTED_US), true);
        return true;
    }
    record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_MISS, file_offset,
                                  vmem_latency_since(LOOKUP_STARTED_US), true);

    void* const NEW_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-file-cache");
    if (NEW_PAGE == nullptr) {
        return false;
    }
    std::memset(NEW_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

    uint64_t const FILL_STARTED_US = ker::mod::time::get_us();
    uint64_t const FILE_SIZE = KEY.size;
    if (file_offset < FILE_SIZE) {
        size_t const READ_SIZE = static_cast<size_t>(std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - file_offset));
        if (file_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            ker::mod::mm::phys::page_ref_dec(NEW_PAGE);
            return false;
        }
        ssize_t const READ_RET = ker::vfs::vfs_pread_file_direct(file, NEW_PAGE, READ_SIZE, static_cast<off_t>(file_offset));
        if (READ_RET < 0) {
            ker::mod::mm::phys::page_ref_dec(NEW_PAGE);
            return false;
        }
    }

    FileMmapCacheInsertResult const INSERT_RESULT = file_mmap_cache_insert_or_discard(KEY, NEW_PAGE, page_out);
    uint32_t const FILL_US = vmem_latency_since(FILL_STARTED_US);
    if (INSERT_RESULT == FileMmapCacheInsertResult::DUPLICATE) {
        record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_HIT, file_offset, FILL_US, true);
    } else {
        record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_FILL, file_offset, FILL_US, true);
        if (INSERT_RESULT == FileMmapCacheInsertResult::EVICTED) {
            record_local_vmem_cache_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_CACHE_EVICT, file_offset, 0, false);
        }
    }
    return true;
}

auto file_mmap_cached_page(int fd, const ker::vfs::Stat& st, uint64_t file_offset, void** page_out) -> bool {
    auto* task = get_current_task();
    auto* file = ker::vfs::vfs_get_file_retain(task, fd);
    if (file == nullptr) {
        return false;
    }
    bool const OK = file_mmap_cached_page_for_file(file, st, file_offset, page_out);
    ker::vfs::vfs_put_file(file);
    return OK;
}

auto present_leaf_entry(ker::mod::mm::paging::PageTable* pagemap, uint64_t vaddr) -> ker::mod::mm::paging::PageTableEntry* {
    if (pagemap == nullptr) {
        return nullptr;
    }

    auto* table = pagemap;
    for (int level = 4; level > 1; --level) {
        auto& entry = table->entries[page_table_index(vaddr, 12 + (9 * (level - 1)))];
        if (entry.present == 0 || entry.pagesize != 0) {
            return nullptr;
        }
        table = table_from_entry(entry);
    }

    auto& entry = table->entries[page_table_index(vaddr, ker::mod::mm::paging::PAGE_SHIFT)];
    return entry.present != 0 ? &entry : nullptr;
}

auto collect_file_mmap_sync_ranges(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length,
                                   ker::util::SmallVec<FileMmapSyncRange, 8>& out) -> int {
    uint64_t end = 0;
    if (pagemap == nullptr || !checked_range_end(start, length, &end)) {
        return -EINVAL;
    }

    g_file_mmap_ranges_lock.lock();
    for (const auto& range : g_file_mmap_ranges) {
        if (range.pagemap != pagemap || range.file == nullptr || range.length == 0) {
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

auto write_file_mapping_bytes(ker::vfs::File* file, const uint8_t* data, size_t count, uint64_t offset) -> int {
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        return -ENOSYS;
    }

    size_t written = 0;
    while (written < count) {
        uint64_t const CURRENT_OFFSET = offset + written;
        if (CURRENT_OFFSET < offset) {
            return -EOVERFLOW;
        }

        ssize_t const RET = file->fops->vfs_write(file, data + written, count - written, static_cast<size_t>(CURRENT_OFFSET));
        if (RET < 0) {
            return static_cast<int>(RET);
        }
        if (RET == 0) {
            return -EIO;
        }
        written += std::min<size_t>(static_cast<size_t>(RET), count - written);
    }

    ker::vfs::vfs_cache_notify_file_changed(file);
    return 0;
}

void release_file_mmap_sync_ranges(ker::util::SmallVec<FileMmapSyncRange, 8>& ranges, size_t first = 0) {
    for (size_t i = first; i < ranges.size(); ++i) {
        ker::vfs::vfs_put_file(ranges.at(i).file);
    }
}

auto sync_file_mmap_range(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length) -> int {
    ker::util::SmallVec<FileMmapSyncRange, 8> ranges;
    int const COLLECT_RET = collect_file_mmap_sync_ranges(pagemap, start, length, ranges);
    if (COLLECT_RET < 0) {
        release_file_mmap_sync_ranges(ranges);
        return COLLECT_RET;
    }

    int result = 0;
    for (size_t range_index = 0; range_index < ranges.size(); ++range_index) {
        const auto& range = ranges.at(range_index);
        uint64_t page_vaddr = page_align_down(range.sync_start);
        while (page_vaddr < range.sync_end) {
            uint64_t const PAGE_END = page_vaddr + ker::mod::mm::paging::PAGE_SIZE;
            auto* entry = present_leaf_entry(pagemap, page_vaddr);
            if (entry != nullptr && entry->dirty != 0) {
                uint64_t const PAGE_START = page_vaddr;
                uint64_t const WRITE_START = std::max(PAGE_START, range.sync_start);
                uint64_t const WRITE_END = std::min(PAGE_END, range.sync_end);
                if (WRITE_START < WRITE_END) {
                    uint64_t const PHYS_PAGE = static_cast<uint64_t>(entry->frame) << ker::mod::mm::paging::PAGE_SHIFT;
                    const auto* const PAGE = reinterpret_cast<const uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS_PAGE));
                    uint64_t const PAGE_DELTA = WRITE_START - PAGE_START;
                    uint64_t const FILE_DELTA = WRITE_START - range.mapping_start;
                    uint64_t const FILE_OFFSET = range.file_offset + FILE_DELTA;
                    if (FILE_OFFSET < range.file_offset) {
                        result = -EOVERFLOW;
                        break;
                    }
                    result =
                        write_file_mapping_bytes(range.file, PAGE + PAGE_DELTA, static_cast<size_t>(WRITE_END - WRITE_START), FILE_OFFSET);
                    if (result < 0) {
                        break;
                    }
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

auto register_file_mmap_range(ker::mod::mm::paging::PageTable* pagemap, uint64_t start, uint64_t length, uint64_t file_offset,
                              ker::vfs::File* file) -> bool {
    if (pagemap == nullptr || file == nullptr || length == 0 || start > UINT64_MAX - length) {
        return false;
    }

    FileMmapRange const RANGE{
        .pagemap = pagemap,
        .start = start,
        .length = length,
        .file_offset = file_offset,
        .file = file,
    };

    g_file_mmap_ranges_lock.lock();
    bool const OK = g_file_mmap_ranges.push_back(RANGE);
    g_file_mmap_ranges_lock.unlock();
    return OK;
}

auto register_file_mmap_range_from_fd(ker::mod::sched::task::Task* task, int fd, uint64_t start, uint64_t length, uint64_t file_offset)
    -> int {
    if (task == nullptr || task->pagemap == nullptr) {
        return -ESRCH;
    }

    auto* file = ker::vfs::vfs_get_file_retain(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    if (!register_file_mmap_range(task->pagemap, start, length, file_offset, file)) {
        ker::vfs::vfs_put_file(file);
        return -ENOMEM;
    }
    return 0;
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

    void* const CANDIDATE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-zero");
    if (CANDIDATE == nullptr) {
        return nullptr;
    }

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

void advance_shared_mmap_cursor(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {
    (void)update_shared_vmem_ranges(task, [vaddr, size](ker::mod::sched::task::Task* candidate) {
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

auto reserve_free_mmap_range(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint, uint64_t& out_vaddr) -> uint64_t {
    out_vaddr = 0;

    ker::mod::sys::MutexGuard guard(g_mmap_reserve_lock);
    uint64_t const VADDR = find_free_range(task, size, hint);
    if (VADDR == 0) {
        return ker::abi::vmem::VMEM_ENOMEM;
    }

    // Reserve the chosen range before returning it to the mapper. This closes
    // the check-then-map race between sibling threads that share a pagemap.
    if (!add_shared_vmem_range(task, VADDR, size, 0, 0)) {
        return ker::abi::vmem::VMEM_ENOMEM;
    }

    out_vaddr = VADDR;
    return 0;
}

void release_mmap_reservation(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, bool reserved) {
    if (reserved) {
        (void)remove_shared_vmem_range(task, vaddr, size);
    }
}

// Convert protection flags to page table flags
auto prot_to_page_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = ker::mod::mm::paging::PAGE_PRESENT | ker::mod::mm::paging::PAGE_USER;

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

auto lazy_file_range_matches(const LazyVmemRange& lhs, const LazyVmemRange& rhs, uint64_t page_vaddr) -> bool {
    return lhs.kind == LazyVmemKind::FILE_BACKED && lhs.start == rhs.start && lhs.end == rhs.end && lhs.prot == rhs.prot &&
           lhs.flags == rhs.flags && lhs.file == rhs.file && lhs.file_offset == rhs.file_offset && lhs.file_dev == rhs.file_dev &&
           lhs.file_ino == rhs.file_ino && page_vaddr >= lhs.start && page_vaddr < lhs.end;
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
        page = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-file-lazy");
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
            ssize_t const READ_RET = ker::vfs::vfs_pread_file_direct(range.file, page, READ_SIZE, static_cast<off_t>(FILE_OFFSET));
            if (READ_RET < 0) {
                ker::mod::mm::phys::page_ref_dec(page);
                return false;
            }
        }
    }

    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    bool still_current = false;
    for (const auto& candidate : task->lazy_vmem_ranges) {
        if (lazy_file_range_matches(candidate, range, page_vaddr)) {
            still_current = true;
            break;
        }
    }
    if (!still_current) {
        task->lazy_vmem_lock.unlock_irqrestore(IRQF);
        ker::mod::mm::phys::page_ref_dec(page);
        return false;
    }

    if (ker::mod::mm::virt::translate(task->pagemap, page_vaddr) != ker::mod::mm::virt::PADDR_INVALID) {
        task->lazy_vmem_lock.unlock_irqrestore(IRQF);
        ker::mod::mm::phys::page_ref_dec(page);
        return true;
    }

    auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    ker::mod::mm::virt::map_page(task->pagemap, page_vaddr, PADDR, file_mapping_page_flags(range.prot, range.flags));
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return true;
}

auto anon_zero_page_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = prot_to_page_flags(prot);

    // A shared zero page must stay read-only. PAGE_COW makes the first write
    // fault allocate a private page, preserving normal anonymous mmap behavior.
    flags &= ~ker::mod::mm::paging::PAGE_WRITE;
    flags |= ker::mod::mm::paging::PAGE_COW;
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

    void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-reserve");
    if (PHYS_PAGE == nullptr) {
        return false;
    }

    auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));
    ker::mod::mm::virt::map_page(task->pagemap, vaddr, PADDR, prot_to_page_flags(prot));
    return true;
}

void rollback_mapped_pages(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t mapped_pages);

void release_fixed_mmap_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) {
    if (task == nullptr || task->pagemap == nullptr || size == 0) {
        return;
    }

    int const SYNC_RET = sync_file_mmap_range(task->pagemap, vaddr, size);
    if (SYNC_RET < 0) {
        log::warn("MAP_FIXED replacement sync failed: pid=%lu vaddr=0x%llx size=0x%llx ret=%d", task->pid,
                  static_cast<unsigned long long>(vaddr), static_cast<unsigned long long>(size), SYNC_RET);
    }
    int const UNREGISTER_RET = unregister_file_mmap_range(task->pagemap, vaddr, size);
    if (UNREGISTER_RET < 0) {
        log::warn("MAP_FIXED replacement unregister failed: pid=%lu vaddr=0x%llx size=0x%llx ret=%d", task->pid,
                  static_cast<unsigned long long>(vaddr), static_cast<unsigned long long>(size), UNREGISTER_RET);
    }

    (void)remove_shared_vmem_range(task, vaddr, size);

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

auto private_anon_allocate(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t hint, uint64_t flags)
    -> uint64_t {
    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    uint64_t mapped_pages = 0;

    if (!add_shared_vmem_range(task, vaddr, size, prot, flags)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-anon");
        if (PHYS_PAGE == nullptr) {
            log::error("out of physical memory after mapping %llu/%llu anon pages", static_cast<unsigned long long>(mapped_pages),
                       static_cast<unsigned long long>(NUM_PAGES));
            rollback_mapped_pages(task, vaddr, mapped_pages);
            (void)remove_shared_vmem_range(task, vaddr, size);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }

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

    advance_shared_mmap_cursor(task, vaddr, size);
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
    if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
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
        vaddr = hint;
    } else {
        uint64_t const RESERVE_RET = reserve_free_mmap_range(task, size, hint, vaddr);
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
        release_fixed_mmap_range(task, vaddr, size);
    }

    if (is_prot_none(prot) || (flags & ker::abi::vmem::MAP_NORESERVE) != 0) {
        if (!add_shared_vmem_range(task, vaddr, size, prot, flags)) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        advance_shared_mmap_cursor(task, vaddr, size);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 1, 0,
                                vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
        return vaddr;
    }

    if ((prot & ker::abi::vmem::PROT_WRITE) == 0) {
        uint64_t const RESULT = private_anon_allocate(task, vaddr, size, prot, hint, flags);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::ANON_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0,
                                perf_status_from_vmem_result(RESULT), vmem_latency_since(PERF_STARTED_US),
                                perf_status_from_vmem_result(RESULT) == 0 ? RESULT : hint, size, true);
        return RESULT;
    }

    void* const ZERO_PAGE = get_anon_zero_page();
    if (ZERO_PAGE == nullptr) {
        log::error("out of physical memory allocating anon zero page");
        release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    // Anonymous mappings are initially backed by a shared read-only zero page.
    // Writes fault through the existing COW path and allocate private pages only
    // for memory that user space actually touches.
    auto const PAGE_FLAGS = anon_zero_page_flags(prot);
    auto const ZERO_PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(ZERO_PAGE)));
    if (NUM_PAGES > std::numeric_limits<uint32_t>::max()) {
        release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    if (!add_shared_vmem_range(task, vaddr, size, prot, flags)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
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

    advance_shared_mmap_cursor(task, vaddr, size);
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

    // Validate size
    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check alignment
    if (addr % ker::mod::mm::paging::PAGE_SIZE != 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Align size to page boundary
    size = page_align_up(size);

    // Check bounds
    if (addr + size > USER_SPACE_END || addr + size < addr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    int const SYNC_RET = sync_file_mmap_range(task->pagemap, addr, size);
    if (SYNC_RET < 0) {
        return static_cast<uint64_t>(SYNC_RET);
    }

    int const UNREGISTER_RET = unregister_file_mmap_range(task->pagemap, addr, size);
    if (UNREGISTER_RET < 0) {
        return static_cast<uint64_t>(UNREGISTER_RET);
    }

    // Unmap pages
    uint64_t const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    if (!remove_shared_vmem_range(task, addr, size)) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        uint64_t const CURRENT_VADDR = addr + (i * ker::mod::mm::paging::PAGE_SIZE);

        bool const IS_MAPPED = ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR);
        bool const IS_RESERVED = !IS_MAPPED && ker::mod::mm::virt::is_page_reserved(task->pagemap, CURRENT_VADDR);
        if (IS_MAPPED || IS_RESERVED) {
            if (is_watched_mmap_vaddr(CURRENT_VADDR)) {
                const auto PHYS = IS_MAPPED ? ker::mod::mm::virt::translate(task->pagemap, CURRENT_VADDR) : 0;
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

// Allocate file-backed (MAP_PRIVATE) memory
auto file_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset) -> uint64_t {
    auto* task = get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    uint64_t const PERF_STARTED_US = ker::mod::time::get_us();

    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    ker::vfs::Stat st{};
    int const STAT_RET = ker::vfs::vfs_fstat(fd, &st);
    if (STAT_RET < 0) {
        log::warn("file mmap fstat failed: fd=%d ret=%d hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx off=0x%llx", fd, STAT_RET,
                  static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size), static_cast<unsigned long long>(flags),
                  static_cast<unsigned long long>(prot), static_cast<unsigned long long>(offset));
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    uint64_t const REQUESTED_SIZE = size;
    size = page_align_up(size);
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    uint64_t vaddr = 0;
    bool const IS_FIXED = ((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0;
    if (IS_FIXED) {
        if (hint < USER_SPACE_START || hint > USER_SPACE_END - size) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        uint64_t const RESERVE_RET = reserve_free_mmap_range(task, size, hint, vaddr);
        if (RESERVE_RET != 0) {
            return -RESERVE_RET;
        }
    }

    bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;
    auto const PAGE_FLAGS = file_mapping_page_flags(prot, flags);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    bool const TRACK_FILE_MAPPING = file_mmap_should_track(st, flags);
    if (IS_FIXED) {
        release_fixed_mmap_range(task, vaddr, size);
    }

    if ((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG) {
        auto* file = ker::vfs::vfs_get_file_retain(task, fd);
        if (file == nullptr) {
            release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }

        if (!add_shared_lazy_file_vmem_range(task, vaddr, size, prot, flags, file, offset, st)) {
            ker::vfs::vfs_put_file(file);
            (void)remove_shared_vmem_range(task, vaddr, size);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }

        if (TRACK_FILE_MAPPING) {
            int const REGISTER_RET = register_file_mmap_range_from_fd(task, fd, vaddr, REQUESTED_SIZE, offset);
            if (REGISTER_RET < 0) {
                (void)remove_shared_vmem_range(task, vaddr, size);
                ker::vfs::vfs_put_file(file);
                return static_cast<uint64_t>(REGISTER_RET);
            }
        }

        ker::vfs::vfs_put_file(file);
        advance_shared_mmap_cursor(task, vaddr, size);
        record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                                vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
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
            void* page = nullptr;
            if (!file_mmap_cached_page(fd, st, FILE_OFFSET, &page)) {
                cache_ok = false;
                break;
            }

            auto const PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
            ker::mod::mm::virt::map_page_batched(&map_batch, CURRENT_VADDR, PADDR, PAGE_FLAGS);
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
                int const REGISTER_RET = register_file_mmap_range_from_fd(task, fd, vaddr, REQUESTED_SIZE, offset);
                if (REGISTER_RET < 0) {
                    rollback_mapped_pages(task, vaddr, mapped_pages);
                    release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
                    return static_cast<uint64_t>(REGISTER_RET);
                }
            }
            release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            advance_shared_mmap_cursor(task, vaddr, size);
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
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-file");
        if (PHYS_PAGE == nullptr) {
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

        if (i > (std::numeric_limits<uint64_t>::max() - offset) / ker::mod::mm::paging::PAGE_SIZE) {
            ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
        }

        uint64_t const FILE_OFFSET = offset + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (FILE_OFFSET < FILE_SIZE) {
            uint64_t const READ_SIZE = std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - FILE_OFFSET);
            if (FILE_OFFSET > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
                release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
            }

            ssize_t const READ_RET = ker::vfs::vfs_pread(fd, PHYS_PAGE, static_cast<size_t>(READ_SIZE), static_cast<off_t>(FILE_OFFSET));
            if (READ_RET < 0) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
                release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
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
        int const REGISTER_RET = register_file_mmap_range_from_fd(task, fd, vaddr, REQUESTED_SIZE, offset);
        if (REGISTER_RET < 0) {
            rollback_mapped_pages(task, vaddr, mapped_pages);
            release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
            return static_cast<uint64_t>(REGISTER_RET);
        }
    }

    release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
    advance_shared_mmap_cursor(task, vaddr, size);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
    return vaddr;
}

}  // anonymous namespace

auto materialize_lazy_file_page(ker::mod::sched::task::Task* task, const ker::mod::sched::task::LazyVmemRange& range, uint64_t page_vaddr,
                                const ker::mod::mm::paging::PageFault& fault) -> bool {
    return materialize_lazy_file_page_impl(task, range, page_vaddr, fault);
}

auto file_mmap_cache_stats() -> FileMmapCacheStats {
    FileMmapCacheStats stats{};
    stats.capacity_pages = FILE_MMAP_CACHE_PAGES;

    g_file_mmap_cache_lock.lock();
    for (const auto& set : g_file_mmap_cache) {
        for (const auto& entry : set.ways) {
            if (entry.page != nullptr) {
                stats.pages++;
            }
        }
    }
    g_file_mmap_cache_lock.unlock();

    stats.bytes = stats.pages * ker::mod::mm::paging::PAGE_SIZE;
    return stats;
}

auto clone_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* src, ker::mod::mm::paging::PageTable* dst) -> bool {
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

    if (!ok) {
        release_file_mmap_ranges_for_pagemap(dst);
    }
    return ok;
}

void release_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* pagemap) {
    if (pagemap == nullptr) {
        return;
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

        int const SYNC_RET = sync_file_mmap_range(pagemap, range.start, range.length);
        if (SYNC_RET < 0) {
            log::warn("file mmap release sync failed: pagemap=%p start=0x%llx length=0x%llx ret=%d", static_cast<void*>(pagemap),
                      static_cast<unsigned long long>(range.start), static_cast<unsigned long long>(range.length), SYNC_RET);
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

    uint64_t const OLD_SIZE = page_align_up(old_size);
    uint64_t const NEW_SIZE = page_align_up(new_size);
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
        uint64_t const SRC_PA = ker::mod::mm::virt::translate(task->pagemap, SRC_VA);
        if (SRC_PA == ker::mod::mm::virt::PADDR_INVALID) {
            continue;
        }
        if (ker::mod::mm::virt::is_page_reserved(task->pagemap, DST_VA) &&
            !materialize_reserved_page(task, DST_VA, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE)) {
            anon_free(NEW_ADDR, NEW_SIZE);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        uint64_t const DST_PA = ker::mod::mm::virt::translate(task->pagemap, DST_VA);
        if (DST_PA == ker::mod::mm::virt::PADDR_INVALID) {
            anon_free(NEW_ADDR, NEW_SIZE);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
        }

        auto* src = reinterpret_cast<const void*>(ker::mod::mm::addr::get_virt_pointer(SRC_PA));
        auto* dst = reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(DST_PA));
        std::memcpy(dst, src, ker::mod::mm::paging::PAGE_SIZE);
    }

    auto const FREE_RESULT = anon_free(old_addr, OLD_SIZE);
    if (static_cast<int64_t>(FREE_RESULT) < 0) {
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

    size = page_align_up(size);
    if (size == 0 || addr > USER_SPACE_END - size) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    int const RET = sync_file_mmap_range(task->pagemap, addr, size);
    return RET < 0 ? static_cast<uint64_t>(RET) : 0;
}

// Main syscall handler
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

            if (size == 0) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }
            if (ADDR % ker::mod::mm::paging::PAGE_SIZE != 0) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            size = page_align_up(size);

            if (ADDR + size > USER_SPACE_END || ADDR + size < ADDR) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            if (!protect_shared_vmem_range(task, ADDR, size, PROT)) {
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
            return static_cast<uint64_t>(ker::mod::mm::swap::swapon_path(reinterpret_cast<const char*>(a1), static_cast<int>(a2)));

        case ker::abi::vmem::ops::SWAPOFF:
            return static_cast<uint64_t>(ker::mod::mm::swap::swapoff_path(reinterpret_cast<const char*>(a1)));

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

    uint64_t const RESULT = file_allocate(hint, size, prot, flags, static_cast<int>(fd), offset);
    if (static_cast<int64_t>(RESULT) < 0) {
        log::warn("file mmap failed: ret=%ld fd=%llu hint=0x%llx size=0x%llx flags=0x%llx prot=0x%llx off=0x%llx",
                  static_cast<long>(static_cast<int64_t>(RESULT)), static_cast<unsigned long long>(fd),
                  static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size), static_cast<unsigned long long>(flags),
                  static_cast<unsigned long long>(prot), static_cast<unsigned long long>(offset));
    }
    return RESULT;
}

}  // namespace ker::syscall::vmem
