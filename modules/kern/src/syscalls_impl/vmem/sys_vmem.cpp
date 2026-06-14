#include "sys_vmem.hpp"

#include <abi/callnums/vmem.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
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
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <utility>
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

enum class FileMmapCacheInsertResult : uint8_t {
    INSERTED,
    DUPLICATE,
    EVICTED,
};

constexpr size_t FILE_MMAP_CACHE_PAGES = 512;
std::array<FileMmapPageCacheEntry, FILE_MMAP_CACHE_PAGES> g_file_mmap_cache{};
ker::mod::sys::Mutex g_file_mmap_cache_lock;
std::atomic<uint64_t> g_file_mmap_cache_clock{0};

auto file_mmap_key_equal(const FileMmapPageKey& lhs, const FileMmapPageKey& rhs) -> bool {
    return lhs.dev == rhs.dev && lhs.ino == rhs.ino && lhs.size == rhs.size && lhs.mtime_sec == rhs.mtime_sec &&
           lhs.mtime_nsec == rhs.mtime_nsec && lhs.ctime_sec == rhs.ctime_sec && lhs.ctime_nsec == rhs.ctime_nsec &&
           lhs.offset == rhs.offset;
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

auto is_prot_none(uint64_t prot) -> bool { return prot == ker::abi::vmem::PROT_NONE; }

auto ranges_overlap(uint64_t start, uint64_t end, uint64_t other_start, uint64_t other_end) -> bool {
    return start < other_end && other_start < end;
}

struct OccupiedVmemRange {
    uint64_t start = 0;
    uint64_t end = 0;
    bool found = false;
};

using LazyVmemRange = ker::mod::sched::task::LazyVmemRange;
using LazyVmemRangeVec = ker::mod::sched::task::LazyVmemRangeVec;

auto push_lazy_vmem_range(LazyVmemRangeVec& ranges, const LazyVmemRange& range) -> bool {
    if (range.start >= range.end) {
        return true;
    }
    return ranges.push_back(range);
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

auto remove_lazy_vmem_range_locked(ker::mod::sched::task::Task& task, uint64_t vaddr, uint64_t size) -> bool {
    uint64_t const END = vaddr + size;
    LazyVmemRangeVec rewritten;
    for (const auto& range : task.lazy_vmem_ranges) {
        if (!ranges_overlap(vaddr, END, range.start, range.end)) {
            if (!push_lazy_vmem_range(rewritten, range)) {
                return false;
            }
            continue;
        }

        if (range.start < vaddr) {
            auto left = range;
            left.end = vaddr;
            if (!push_lazy_vmem_range(rewritten, left)) {
                return false;
            }
        }
        if (END < range.end) {
            auto right = range;
            right.start = END;
            if (!push_lazy_vmem_range(rewritten, right)) {
                return false;
            }
        }
    }
    task.lazy_vmem_ranges = std::move(rewritten);
    return true;
}

auto protect_lazy_vmem_range_locked(ker::mod::sched::task::Task& task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    uint64_t const END = vaddr + size;
    LazyVmemRangeVec rewritten;
    for (const auto& range : task.lazy_vmem_ranges) {
        if (!ranges_overlap(vaddr, END, range.start, range.end)) {
            if (!push_lazy_vmem_range(rewritten, range)) {
                return false;
            }
            continue;
        }

        if (range.start < vaddr) {
            auto left = range;
            left.end = vaddr;
            if (!push_lazy_vmem_range(rewritten, left)) {
                return false;
            }
        }

        auto middle = range;
        middle.start = std::max(range.start, vaddr);
        middle.end = std::min(range.end, END);
        middle.prot = prot;
        if (!push_lazy_vmem_range(rewritten, middle)) {
            return false;
        }

        if (END < range.end) {
            auto right = range;
            right.start = END;
            if (!push_lazy_vmem_range(rewritten, right)) {
                return false;
            }
        }
    }
    task.lazy_vmem_ranges = std::move(rewritten);
    return true;
}

auto remove_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return true;
    }

    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    bool const OK = remove_lazy_vmem_range_locked(*task, vaddr, size);
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return OK;
}

auto add_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t flags) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return false;
    }

    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    bool ok = remove_lazy_vmem_range_locked(*task, vaddr, size);
    if (ok) {
        ker::mod::sched::task::LazyVmemRange const RANGE{.start = vaddr, .end = vaddr + size, .prot = prot, .flags = flags};
        ok = task->lazy_vmem_ranges.push_back(RANGE);
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return ok;
}

auto protect_lazy_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    if (task == nullptr || size == 0 || vaddr > UINT64_MAX - size) {
        return true;
    }

    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    bool const OK = protect_lazy_vmem_range_locked(*task, vaddr, size, prot);
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return OK;
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

auto protect_shared_vmem_range(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot) -> bool {
    return update_shared_vmem_ranges(task, [vaddr, size, prot](ker::mod::sched::task::Task* candidate) {
        return protect_lazy_vmem_range(candidate, vaddr, size, prot);
    });
}

auto file_mmap_cache_lookup(const FileMmapPageKey& key) -> void* {
    uint64_t const USE_STAMP = g_file_mmap_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;

    g_file_mmap_cache_lock.lock();
    for (auto& entry : g_file_mmap_cache) {
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

    g_file_mmap_cache_lock.lock();
    for (auto& entry : g_file_mmap_cache) {
        if (entry.page != nullptr && file_mmap_key_equal(entry.key, key)) {
            entry.last_used = USE_STAMP;
            ker::mod::mm::phys::page_ref_inc(entry.page);
            *page_for_mapping = entry.page;
            g_file_mmap_cache_lock.unlock();
            ker::mod::mm::phys::page_ref_dec(new_page);
            return FileMmapCacheInsertResult::DUPLICATE;
        }
    }

    auto* victim = &g_file_mmap_cache.front();
    for (auto& entry : g_file_mmap_cache) {
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

auto file_mmap_cached_page(int fd, const ker::vfs::Stat& st, uint64_t file_offset, void** page_out) -> bool {
    if (page_out == nullptr) {
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

    uint64_t const FILL_STARTED_US = ker::mod::time::get_us();
    uint64_t const FILE_SIZE = KEY.size;
    if (file_offset < FILE_SIZE) {
        size_t const READ_SIZE = static_cast<size_t>(std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - file_offset));
        ssize_t const READ_RET = ker::vfs::vfs_pread(fd, NEW_PAGE, READ_SIZE, static_cast<off_t>(file_offset));
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
    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    if (IS_FIXED) {
        release_fixed_mmap_range(task, vaddr, size);
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

    release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION);
    advance_shared_mmap_cursor(task, vaddr, size);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
    return vaddr;
}

}  // anonymous namespace

auto file_mmap_cache_stats() -> FileMmapCacheStats {
    FileMmapCacheStats stats{};
    stats.capacity_pages = FILE_MMAP_CACHE_PAGES;

    g_file_mmap_cache_lock.lock();
    for (const auto& entry : g_file_mmap_cache) {
        if (entry.page != nullptr) {
            stats.pages++;
        }
    }
    g_file_mmap_cache_lock.unlock();

    stats.bytes = stats.pages * ker::mod::mm::paging::PAGE_SIZE;
    return stats;
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
