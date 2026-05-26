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
#include <platform/sys/mutex.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vmem {
using log = ker::mod::dbg::logger<"vmem">;

// Linux x86_64 user space address range
// User space: 0x0000000000000000 - 0x00007FFFFFFFFFFF (128TB)
// Kernel space: 0xFFFF800000000000 - 0xFFFFFFFFFFFFFFFF
constexpr uint64_t USER_SPACE_START = 0x0000000000400000ULL;  // Start after first 4MB (for NULL protection and low mem)
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;    // Linux canonical address limit
constexpr uint64_t MMAP_START = 0x0000100000000000ULL;        // mmap base - avoid collision with ELF debug info at 0x700000000000

namespace {
// Get the current task
inline auto get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

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

    for (uint64_t addr = vaddr; addr < vaddr + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
        if (ker::mod::mm::virt::is_page_mapped_or_reserved(task->pagemap, addr)) {
            return false;
        }
    }

    return true;
}

auto find_free_range_from(ker::mod::sched::task::Task* task, uint64_t size, uint64_t start) -> uint64_t {
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return 0;
    }

    uint64_t current_addr = page_align_up(start);
    while (current_addr <= USER_SPACE_END - size) {
        if (mmap_range_is_free(task, current_addr, size)) {
            return current_addr;
        }

        bool skipped_mapped_page = false;
        for (uint64_t addr = current_addr; addr < current_addr + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::is_page_mapped_or_reserved(task->pagemap, addr)) {
                current_addr = page_align_up(addr + ker::mod::mm::paging::PAGE_SIZE);
                skipped_mapped_page = true;
                break;
            }
        }
        if (!skipped_mapped_page) {
            current_addr += ker::mod::mm::paging::PAGE_SIZE;
        }
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

auto private_anon_allocate(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t size, uint64_t prot, uint64_t hint, uint64_t flags)
    -> uint64_t {
    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    uint64_t mapped_pages = 0;

    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-anon");
        if (PHYS_PAGE == nullptr) {
            log::error("out of physical memory after mapping %llu/%llu anon pages", static_cast<unsigned long long>(mapped_pages),
                       static_cast<unsigned long long>(NUM_PAGES));
            rollback_mapped_pages(task, vaddr, mapped_pages);
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

    advance_mmap_cursor(task, vaddr, size);
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

    if (((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0) {
        // MAP_FIXED: use exact address
        if (hint < USER_SPACE_START || hint > USER_SPACE_END - size) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        // Find a suitable range
        vaddr = find_free_range(task, size, hint);
        if (vaddr == 0) {
            log::warn("no free range found for size %x", size);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    }

    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;

    if (is_prot_none(prot)) {
        ker::mod::mm::virt::reserve_page_range(task->pagemap, vaddr, NUM_PAGES);
        advance_mmap_cursor(task, vaddr, size);
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
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    // Anonymous mappings are initially backed by a shared read-only zero page.
    // Writes fault through the existing COW path and allocate private pages only
    // for memory that user space actually touches.
    auto const PAGE_FLAGS = anon_zero_page_flags(prot);
    auto const ZERO_PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(ZERO_PAGE)));
    if (NUM_PAGES > std::numeric_limits<uint32_t>::max()) {
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

    advance_mmap_cursor(task, vaddr, size);
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
    if (ker::vfs::vfs_fstat(fd, &st) < 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    size = page_align_up(size);
    if (size == 0 || size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    uint64_t vaddr = 0;
    if (((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0) {
        if (hint < USER_SPACE_START || hint > USER_SPACE_END - size) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        vaddr = find_free_range(task, size, hint);
        if (vaddr == 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    }

    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;

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
            advance_mmap_cursor(task, vaddr, size);
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
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
        std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE);

        if (i > (std::numeric_limits<uint64_t>::max() - offset) / ker::mod::mm::paging::PAGE_SIZE) {
            ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
            rollback_mapped_pages(task, vaddr, mapped_pages);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
        }

        uint64_t const FILE_OFFSET = offset + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (FILE_OFFSET < FILE_SIZE) {
            uint64_t const READ_SIZE = std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, FILE_SIZE - FILE_OFFSET);
            if (FILE_OFFSET > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
            }

            ssize_t const READ_RET = ker::vfs::vfs_pread(fd, PHYS_PAGE, static_cast<size_t>(READ_SIZE), static_cast<off_t>(FILE_OFFSET));
            if (READ_RET < 0) {
                ker::mod::mm::phys::page_ref_dec(PHYS_PAGE);
                rollback_mapped_pages(task, vaddr, mapped_pages);
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

    advance_mmap_cursor(task, vaddr, size);
    record_local_vmem_event(task, ker::mod::perf::WkiPerfLocalVmemOp::FILE_MMAP, ker::mod::perf::WkiPerfPhase::END, NUM_PAGES, 0, 0,
                            vmem_latency_since(PERF_STARTED_US), vaddr, size, true);
    return vaddr;
}

}  // anonymous namespace

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

            auto page_flags = prot_to_page_flags(PROT);
            uint64_t const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;

            for (uint64_t i = 0; i < NUM_PAGES; i++) {
                uint64_t const CURRENT_VADDR = ADDR + (i * ker::mod::mm::paging::PAGE_SIZE);
                if (ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR)) {
                    ker::mod::mm::virt::unify_page_flags(task->pagemap, CURRENT_VADDR, page_flags);
                } else if (ker::mod::mm::virt::is_page_reserved(task->pagemap, CURRENT_VADDR)) {
                    if (!materialize_reserved_page(task, CURRENT_VADDR, PROT)) {
                        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
                    }
                }
            }

            return 0;
        }

        default:
            log::warn("invalid operation %llu", op);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }
}

auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t {
    if ((flags & ker::abi::vmem::MAP_ANONYMOUS) != 0) {
        return anon_allocate(hint, size, prot, flags);
    }

    if (fd > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    return file_allocate(hint, size, prot, flags, static_cast<int>(fd), offset);
}

}  // namespace ker::syscall::vmem
