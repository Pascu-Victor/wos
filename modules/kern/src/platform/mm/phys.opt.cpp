#include "phys.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>
#include <sanitizer/kasan.hpp>
#include <string_view>

#ifdef WOS_PHYS_LOCK_DEBUG
#include <platform/acpi/apic/apic.hpp>
#endif

#include "minimalist_malloc/mini_malloc.hpp"
#include "page_alloc.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace {
// Forward declaration - we'll get kernel pagemap physical address once during init
uint64_t kernel_cr3 = 0;
}  // anonymous namespace

namespace ker::mod::mm::phys {

namespace {
using log = ker::mod::dbg::logger<"phys">;

// Per-CPU page cache for reducing lock contention
struct PerCpuPageCache {
    static constexpr size_t CACHE_SIZE = 16;  // Pages per CPU cache
    std::array<void*, CACHE_SIZE> pages{};
    size_t count{};
    sys::Spinlock lock;  // Fine-grained per-CPU lock
};

__attribute__((section(".data"))) paging::PageZone* zones = nullptr;
__attribute__((section(".data"))) paging::PageZone* huge_page_zone = nullptr;  // Dedicated zone for huge allocations

// Per-CPU caches (initialized in init())
PerCpuPageCache* per_cpu_caches = nullptr;
size_t num_cpus = 0;
std::atomic<bool> per_cpu_ready{false};  // Set after per-CPU structures are initialized

// The cache stores only bare 4 KiB pages, but page_free() does not receive the
// allocation size. Caching the base of a multi-page allocation leaves the buddy
// metadata tagged with the original larger order and can later free/reuse pages
// that are still mapped elsewhere. Keep the cache disabled until frees carry
// enough size/order information to prove an allocation is order-0.
constexpr bool USE_PER_CPU_PAGE_CACHE = false;

struct TrackedSpinlock {
    std::atomic<bool> locked{false};
#ifdef WOS_PHYS_LOCK_DEBUG
    volatile uint64_t holder_cr3 = 0;
    volatile uint64_t holder_cpu = 0;
    volatile uint64_t holder_rip = 0;
#endif

    auto lock_irq() -> uint64_t {
        // Save RFLAGS and disable interrupts before acquiring
        // NOLINTNEXTLINE(misc-const-correctness)
        uint64_t flags = 0;
        asm volatile("pushfq; popq %0" : "=r"(flags));
        asm volatile("cli");

        while (locked.exchange(true, std::memory_order_acquire)) {
            while (locked.load(std::memory_order_relaxed)) {
                asm volatile("pause");
            }
        }
#ifdef WOS_PHYS_LOCK_DEBUG
        // Record who holds the lock
        holder_cr3 = rdcr3();
        holder_rip = reinterpret_cast<uint64_t>(__builtin_return_address(0));
        holder_cpu = per_cpu_ready.load(std::memory_order_acquire) ? cpu::current_cpu() : apic::get_apic_id();
#endif
        return flags;
    }

    void unlock_irq(uint64_t flags) {
#ifdef WOS_PHYS_LOCK_DEBUG
        holder_cr3 = 0;
        holder_cpu = 0;
        holder_rip = 0;
#endif
        locked.store(false, std::memory_order_release);

        // Restore interrupt state
        if ((flags & cpu::GATE_IF_MASK) != 0) {
            asm volatile("sti");
        }
    }
};

TrackedSpinlock memlock;  // Global lock for zone list and large allocations

// Per-CPU cache deferred initialization info
uint64_t per_cpu_caches_phys_base = 0;
uint64_t per_cpu_caches_size = 0;

// Statistics counters
uint64_t main_heap_size = 0;

// Huge page zone deferred initialization info
uint64_t huge_page_base = 0;
uint64_t huge_page_size = 0;

// Allocation tracking counters (now atomic for multi-CPU safety)
std::atomic<uint64_t> total_allocated_bytes{0};
std::atomic<uint64_t> total_freed_bytes{0};
std::atomic<uint64_t> alloc_count{0};
std::atomic<uint64_t> free_count{0};

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
// Per-caller page allocation histogram. Indexed by return address so OOM
// output shows which subsystem consumed memory.
struct CallerStat {
    uint64_t caller;  // return address (0 = empty)
    uint64_t pages;   // cumulative pages allocated from this site
};
constexpr size_t CALLER_STAT_SLOTS = 64;
__attribute__((section(".bss"))) std::array<CallerStat, CALLER_STAT_SLOTS> caller_stats{};
std::atomic<bool> caller_stat_lock{false};
std::atomic<bool> caller_stats_runtime_enabled{true};
std::atomic<uint64_t> caller_stats_generation_value{1};
#endif

struct PhysRange {
    uint64_t base = 0;
    uint64_t end = 0;
};

constexpr size_t MAX_SANITIZED_RANGES = 128;

auto checked_range_end(uint64_t base, uint64_t length) -> uint64_t {
    uint64_t const END = base + length;
    return END < base ? ~0ULL : END;
}

auto page_range_from_usable(uint64_t base, uint64_t length) -> PhysRange {
    uint64_t const END = checked_range_end(base, length);
    return {.base = page_align_up(base), .end = page_align_down(END)};
}

auto page_range_from_reserved(uint64_t base, uint64_t length) -> PhysRange {
    uint64_t const END = checked_range_end(base, length);
    return {.base = page_align_down(base), .end = page_align_up(END)};
}

auto range_valid(PhysRange range) -> bool { return range.base < range.end; }

auto range_overlaps(PhysRange a, PhysRange b) -> bool { return a.base < b.end && b.base < a.end; }

void append_range(std::array<PhysRange, MAX_SANITIZED_RANGES>& ranges, size_t& count, PhysRange range) {
    if (!range_valid(range) || count >= ranges.size()) {
        return;
    }
    ranges.at(count++) = range;
}

void remove_range_at(std::array<PhysRange, MAX_SANITIZED_RANGES>& ranges, size_t& count, size_t index) {
    for (size_t i = index + 1; i < count; ++i) {
        ranges.at(i - 1) = ranges.at(i);
    }
    --count;
}

void subtract_range(std::array<PhysRange, MAX_SANITIZED_RANGES>& ranges, size_t& count, PhysRange cut) {
    if (!range_valid(cut)) {
        return;
    }

    for (size_t i = 0; i < count;) {
        PhysRange const CUR = ranges.at(i);
        if (!range_overlaps(CUR, cut)) {
            ++i;
            continue;
        }

        if (cut.base <= CUR.base && cut.end >= CUR.end) {
            remove_range_at(ranges, count, i);
            continue;
        }

        if (cut.base <= CUR.base) {
            ranges.at(i).base = cut.end;
            ++i;
            continue;
        }

        if (cut.end >= CUR.end) {
            ranges.at(i).end = cut.base;
            ++i;
            continue;
        }

        ranges.at(i).end = cut.base;
        append_range(ranges, count, {.base = cut.end, .end = CUR.end});
        ++i;
    }
}

void subtract_ranges(std::array<PhysRange, MAX_SANITIZED_RANGES>& ranges, size_t& count,
                     const std::array<PhysRange, MAX_SANITIZED_RANGES>& cuts, size_t cut_count) {
    for (size_t i = 0; i < cut_count; ++i) {
        subtract_range(ranges, count, cuts.at(i));
    }
}

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
void record_page_alloc_caller(void* caller_addr, uint64_t num_pages) {
    if (caller_addr == nullptr) {
        return;
    }
    if (!caller_stats_runtime_enabled.load(std::memory_order_relaxed)) {
        return;
    }
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    auto caller = reinterpret_cast<uint64_t>(caller_addr);
    size_t const START = (caller >> 3) % CALLER_STAT_SLOTS;
    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        size_t const IDX = (START + i) % CALLER_STAT_SLOTS;
        if (caller_stats.at(IDX).caller == caller) {
            caller_stats.at(IDX).pages += num_pages;
            caller_stat_lock.store(false, std::memory_order_release);
            return;
        }
        if (caller_stats.at(IDX).caller == 0) {
            caller_stats.at(IDX).caller = caller;
            caller_stats.at(IDX).pages = num_pages;
            caller_stat_lock.store(false, std::memory_order_release);
            return;
        }
    }
    // Table full - silently drop; 64 slots covers all known call sites.
    caller_stat_lock.store(false, std::memory_order_release);
}
#endif

}  // namespace

namespace {
constexpr uint8_t FLAG_KIND_MASK = 0xC0U;
constexpr uint8_t FLAG_ORDER_MASK = 0x1FU;

[[maybe_unused]] void copy_cumulative_page_caller_stats_sorted(CallerPageStat* out, size_t max_rows, size_t& total_rows) {
    total_rows = 0;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    std::array<CallerStat, CALLER_STAT_SLOTS> snapshot{};
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        snapshot.at(i) = caller_stats.at(i);
    }
    caller_stat_lock.store(false, std::memory_order_release);

    for (size_t i = 0; i < CALLER_STAT_SLOTS - 1; i++) {
        size_t max_idx = i;
        for (size_t j = i + 1; j < CALLER_STAT_SLOTS; j++) {
            if (snapshot.at(j).pages > snapshot.at(max_idx).pages) {
                max_idx = j;
            }
        }
        if (max_idx != i) {
            CallerStat const TMP = snapshot.at(i);
            snapshot.at(i) = snapshot.at(max_idx);
            snapshot.at(max_idx) = TMP;
        }
    }

    for (const auto& row : snapshot) {
        if (row.caller == 0 || row.pages == 0) {
            continue;
        }
        if (out != nullptr && total_rows < max_rows) {
            out[total_rows] = CallerPageStat{.caller = row.caller, .pages = row.pages};
        }
        total_rows++;
    }
#else
    (void)out;
    (void)max_rows;
#endif
}

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
void add_live_page_caller_stat(std::array<CallerPageStat, 128>& rows, size_t& row_count, uint64_t caller, uint64_t pages) {
    if (pages == 0) {
        return;
    }
    for (size_t i = 0; i < row_count; ++i) {
        if (rows.at(i).caller == caller) {
            rows.at(i).pages += pages;
            return;
        }
    }
    if (row_count < rows.size()) {
        rows.at(row_count++) = CallerPageStat{.caller = caller, .pages = pages};
    }
}

void copy_live_page_caller_stats_sorted(CallerPageStat* out, size_t max_rows, size_t& total_rows) {
    std::array<CallerPageStat, 128> rows{};
    size_t row_count = 0;

    uint64_t const FLAGS = memlock.lock_irq();
    auto scan_allocator = [&](const PageAllocator* alloc) {
        if (alloc == nullptr || alloc->page_callers == nullptr || alloc->page_flags == nullptr) {
            return;
        }
        uint32_t page = 0;
        while (page < alloc->total_pages) {
            uint8_t const PAGE_FLAGS = alloc->page_flags[page];
            if ((PAGE_FLAGS & FLAG_KIND_MASK) != PageAllocator::FLAG_ALLOC_HEAD) {
                page++;
                continue;
            }
            int const ORDER = PAGE_FLAGS & FLAG_ORDER_MASK;
            uint64_t const PAGES = (ORDER <= PageAllocator::MAX_ORDER) ? (1ULL << ORDER) : 1ULL;
            uint64_t const CALLER = alloc->page_callers[page];
            add_live_page_caller_stat(rows, row_count, CALLER, PAGES);
            uint64_t const NEXT_PAGE = static_cast<uint64_t>(page) + PAGES;
            page = NEXT_PAGE > alloc->total_pages ? alloc->total_pages : static_cast<uint32_t>(NEXT_PAGE);
        }
    };
    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        scan_allocator(zone->allocator);
    }
    if (huge_page_zone != nullptr) {
        scan_allocator(huge_page_zone->allocator);
    }
    memlock.unlock_irq(FLAGS);

    for (size_t i = 0; i < row_count; ++i) {
        size_t max_idx = i;
        for (size_t j = i + 1; j < row_count; ++j) {
            if (rows.at(j).pages > rows.at(max_idx).pages) {
                max_idx = j;
            }
        }
        if (max_idx != i) {
            CallerPageStat const TMP = rows.at(i);
            rows.at(i) = rows.at(max_idx);
            rows.at(max_idx) = TMP;
        }
    }

    total_rows = row_count;
    if (out == nullptr) {
        return;
    }
    size_t const COPY_ROWS = std::min(max_rows, row_count);
    for (size_t i = 0; i < COPY_ROWS; ++i) {
        out[i] = rows.at(i);
    }
}
#endif
}  // namespace

void dump_alloc_stats() {
    io::serial::write("Physical alloc stats: allocated=");
    io::serial::write_hex(total_allocated_bytes.load());
    io::serial::write(" freed=");
    io::serial::write_hex(total_freed_bytes.load());
    io::serial::write(" delta=");
    io::serial::write_hex(total_allocated_bytes.load() - total_freed_bytes.load());
    io::serial::write(" allocCount=");
    io::serial::write_hex(alloc_count.load());
    io::serial::write(" freeCount=");
    io::serial::write_hex(free_count.load());
    io::serial::write("\n");
}

auto get_free_mem_bytes() -> uint64_t { return main_heap_size - (total_allocated_bytes.load() - total_freed_bytes.load()); }

auto get_total_mem_bytes() -> uint64_t { return main_heap_size; }

void get_alloc_stats_snapshot(AllocStatsSnapshot& out) {
    uint64_t const ALLOCATED = total_allocated_bytes.load(std::memory_order_relaxed);
    uint64_t const FREED = total_freed_bytes.load(std::memory_order_relaxed);
    out = AllocStatsSnapshot{.total_allocated_bytes = ALLOCATED,
                             .total_freed_bytes = FREED,
                             .live_allocated_bytes = ALLOCATED >= FREED ? ALLOCATED - FREED : 0,
                             .alloc_count = alloc_count.load(std::memory_order_relaxed),
                             .free_count = free_count.load(std::memory_order_relaxed),
                             .total_mem_bytes = get_total_mem_bytes(),
                             .free_mem_bytes = get_free_mem_bytes()};
}

auto snapshot_zones(ZoneSnapshot* out, size_t max_rows) -> size_t {
    if (out == nullptr || max_rows == 0) {
        return 0;
    }

    static_assert(MEMACC_BUDDY_ORDER_COUNT == static_cast<size_t>(PageAllocator::MAX_ORDER + 1));

    size_t rows = 0;
    uint64_t const FLAGS = memlock.lock_irq();
    for (paging::PageZone const* zone = zones; zone != nullptr && rows < max_rows; zone = zone->next) {
        ZoneSnapshot snap{};
        snap.zone_num = zone->zone_num;
        snap.start = zone->start;
        snap.len = zone->len;
        snap.page_count = zone->page_count;
        snap.name = zone->name;

        if (zone->allocator != nullptr) {
            snap.has_allocator = true;
            const auto* alloc = zone->allocator;
            snap.total_pages = alloc->total_pages;
            snap.usable_pages = alloc->usable_pages;
            snap.free_pages = alloc->free_count;
            snap.metadata_pages = alloc->metadata_pages;

            const auto* expected_flags = reinterpret_cast<const uint8_t*>(alloc->base + sizeof(PageAllocator));
            snap.invalid_allocator = alloc->base == 0 || (alloc->base & (paging::PAGE_SIZE - 1)) != 0 ||
                                     alloc->page_flags != expected_flags || alloc->total_pages == 0 ||
                                     alloc->metadata_pages >= alloc->total_pages;
            if (!snap.invalid_allocator) {
                for (uint32_t i = 0; i < alloc->total_pages; i++) {
                    uint8_t const FLAG = alloc->page_flags[i];
                    if ((FLAG & FLAG_KIND_MASK) != PageAllocator::FLAG_FREE_HEAD) {
                        continue;
                    }
                    uint8_t const ORD = FLAG & FLAG_ORDER_MASK;
                    if (ORD > PageAllocator::MAX_ORDER) {
                        snap.bad_order = true;
                        continue;
                    }
                    snap.buddy_order_counts.at(ORD) += 1;
                    snap.scanned_free_pages += (1U << ORD);
                }
                snap.free_count_mismatch = snap.scanned_free_pages != alloc->free_count;
            }
        }

        out[rows++] = snap;
    }
    memlock.unlock_irq(FLAGS);
    return rows;
}

auto page_caller_stats_available() -> bool {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    return true;
#else
    return false;
#endif
}

auto page_caller_stats_enabled() -> bool {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    return caller_stats_runtime_enabled.load(std::memory_order_acquire);
#else
    return false;
#endif
}

void page_caller_stats_set_enabled(bool enabled) {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    caller_stats_runtime_enabled.store(enabled, std::memory_order_release);
#else
    (void)enabled;
#endif
}

void page_caller_stats_reset() {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    for (auto& row : caller_stats) {
        row = {};
    }
    caller_stats_generation_value.fetch_add(1, std::memory_order_acq_rel);
    caller_stat_lock.store(false, std::memory_order_release);
#endif
}

auto page_caller_stats_generation() -> uint64_t {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    return caller_stats_generation_value.load(std::memory_order_acquire);
#else
    return 0;
#endif
}

auto page_caller_stats_default_enabled() -> bool {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    return true;
#else
    return false;
#endif
}

auto snapshot_page_caller_stats(CallerPageStat* out, size_t max_rows, size_t& total_rows) -> bool {
    total_rows = 0;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    copy_live_page_caller_stats_sorted(out, max_rows, total_rows);
    return true;
#else
    (void)out;
    (void)max_rows;
    return false;
#endif
}

void dump_caller_page_stats() {
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    io::serial::write("Physical page alloc by caller: disabled (build with WOS_PHYS_ALLOC_CALLER_STATS=ON)\n");
#else
    io::serial::write("Physical page alloc by caller (cumulative, sorted by pages desc):\n");

    // Copy table under lock so we get a stable snapshot
    constexpr uint64_t BYTES_PER_KB = 1024;
    std::array<CallerStat, CALLER_STAT_SLOTS> snapshot{};
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        snapshot.at(i) = caller_stats.at(i);
    }
    caller_stat_lock.store(false, std::memory_order_release);

    // Selection-sort descending by pages (64 entries - fine for OOM context)
    for (size_t i = 0; i < CALLER_STAT_SLOTS - 1; i++) {
        size_t max_idx = i;
        for (size_t j = i + 1; j < CALLER_STAT_SLOTS; j++) {
            if (snapshot.at(j).pages > snapshot.at(max_idx).pages) {
                max_idx = j;
            }
        }
        if (max_idx != i) {
            CallerStat const TMP = snapshot.at(i);
            snapshot.at(i) = snapshot.at(max_idx);
            snapshot.at(max_idx) = TMP;
        }
    }

    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        if (snapshot.at(i).caller == 0 || snapshot.at(i).pages == 0) {
            break;
        }
        io::serial::write("  0x");
        io::serial::write_hex(snapshot.at(i).caller);
        io::serial::write(": ");
        io::serial::write(snapshot.at(i).pages);
        io::serial::write(" pages (");
        io::serial::write(snapshot.at(i).pages * paging::PAGE_SIZE / BYTES_PER_KB);
        io::serial::write(" KB)\n");
    }
#endif
}

auto get_huge_page_zone() -> paging::PageZone* { return huge_page_zone; }
auto get_zones() -> paging::PageZone* { return zones; }

namespace {
// Forward declaration
auto find_free_block(uint64_t size, uint64_t caller = 0) -> void*;

auto buddy_accounting_size(uint64_t size) -> uint64_t {
    if (size == 0) {
        return 0;
    }

    uint64_t pages = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
    uint64_t rounded_pages = 1;
    while (rounded_pages < pages && rounded_pages < (uint64_t{1} << PageAllocator::MAX_ORDER)) {
        rounded_pages <<= 1;
    }
    return rounded_pages * paging::PAGE_SIZE;
}

auto init_page_zone(uint64_t base, uint64_t len, uint64_t zone_num) -> paging::PageZone* {
    auto* zone = reinterpret_cast<paging::PageZone*>(base);

    base = page_align_up(base + sizeof(paging::PageZone));
    len -= paging::PAGE_SIZE;

    zone->name = "Physical Memory";
    zone->zone_num = zone_num;

    // Initialise the page allocator; metadata is embedded at the start of the region.
    auto* allocator = reinterpret_cast<PageAllocator*>(base);
    allocator->init(base, len);
    zone->allocator = allocator;
    zone->start = base;
    zone->len = static_cast<uint64_t>(allocator->total_pages) * paging::PAGE_SIZE;
    zone->page_count = allocator->get_usable_pages();

    return zone;
}

auto init_huge_page_zone(uint64_t base, uint64_t len) -> paging::PageZone* {
    // Allocate zone structure from regular memory (which is already mapped)
    // Don't use the huge page region itself for metadata since it's not mapped yet
    auto* zone = static_cast<paging::PageZone*>(find_free_block(paging::PAGE_SIZE));
    if (zone == nullptr) {
        return nullptr;  // OOM
    }

    // Allocate PageAllocator structure from regular memory too
    auto* allocator = reinterpret_cast<PageAllocator*>(find_free_block(paging::PAGE_SIZE));
    if (allocator == nullptr) {
        return nullptr;  // OOM
    }

    // Convert huge page region base to virtual address for initialization
    auto virt_base = reinterpret_cast<uint64_t>(addr::get_virt_pointer(base));

    zone->name = "Huge Pages";
    zone->zone_num = paging::PageZone::ZONE_HUGE_PAGES;  // Special zone number for huge pages

    // Initialize allocator with the huge page region (all virtual addresses now)
    allocator->init(virt_base, len);
    zone->allocator = allocator;
    zone->start = virt_base;
    zone->len = static_cast<uint64_t>(allocator->total_pages) * paging::PAGE_SIZE;
    zone->page_count = allocator->get_usable_pages();
    zone->next = nullptr;

    return zone;
}

auto find_free_block(uint64_t size, uint64_t caller) -> void* {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < size) {
            continue;
        }

        void* const BLOCK = zone->allocator->alloc(size, caller);
        if (BLOCK == nullptr) {
            [[unlikely]] continue;
        }
        return BLOCK;
    }

    return nullptr;
}

auto find_free_block_huge(uint64_t size, uint64_t caller = 0) -> void* {
    if (huge_page_zone == nullptr || huge_page_zone->len < size) {
        return nullptr;
    }

    return huge_page_zone->allocator->alloc(size, caller);
}

}  // namespace

void init(limine_memmap_response* memmap_response) {
    if (memmap_response == nullptr) {
        // TODO: logging
        hcf();
    }
    limine_memmap_response const MEMMAP = *memmap_response;
    std::array<PhysRange, MAX_SANITIZED_RANGES> reserved_ranges{};
    size_t reserved_range_count = 0;
    std::array<PhysRange, MAX_SANITIZED_RANGES> accepted_ranges{};
    size_t accepted_range_count = 0;

    for (size_t i = 0; i < MEMMAP.entry_count; i++) {
        if (MEMMAP.entries[i]->type == LIMINE_MEMMAP_USABLE || MEMMAP.entries[i]->length == 0) {
            continue;
        }
        append_range(reserved_ranges, reserved_range_count, page_range_from_reserved(MEMMAP.entries[i]->base, MEMMAP.entries[i]->length));
    }

    // Initialize per-CPU caches
    num_cpus = smt::get_core_count();
    if (num_cpus == 0) {
        num_cpus = 1;  // Fallback to single CPU
    }

    // Reserve per-CPU caches region (will be mapped and initialized after virt::initPagemap)
    // We'll allocate these from the first usable memory region
    per_cpu_caches_size = page_align_up(sizeof(PerCpuPageCache) * num_cpus);
    for (size_t i = 0; i < MEMMAP.entry_count; i++) {
        if (MEMMAP.entries[i]->type == LIMINE_MEMMAP_USABLE && MEMMAP.entries[i]->length >= per_cpu_caches_size + paging::PAGE_SIZE) {
            // Save the physical address for later mapping
            per_cpu_caches_phys_base = MEMMAP.entries[i]->base;
            append_range(reserved_ranges, reserved_range_count,
                         {.base = per_cpu_caches_phys_base, .end = per_cpu_caches_phys_base + per_cpu_caches_size});
            break;
        }
    }

    if (per_cpu_caches_phys_base == 0) {
        hcf();  // Can't allocate per-CPU caches
    }

    paging::PageZone* zones_tail = nullptr;
    size_t zone_num = 0;

    for (size_t i = 0; i < MEMMAP.entry_count; i++) {
        if (MEMMAP.entries[i]->type != LIMINE_MEMMAP_USABLE || MEMMAP.entries[i]->length <= paging::PAGE_SIZE) {
            continue;
        }

        std::array<PhysRange, MAX_SANITIZED_RANGES> usable_ranges{};
        size_t usable_range_count = 0;
        append_range(usable_ranges, usable_range_count, page_range_from_usable(MEMMAP.entries[i]->base, MEMMAP.entries[i]->length));
        subtract_ranges(usable_ranges, usable_range_count, reserved_ranges, reserved_range_count);
        subtract_ranges(usable_ranges, usable_range_count, accepted_ranges, accepted_range_count);

        for (size_t range_i = 0; range_i < usable_range_count; ++range_i) {
            PhysRange const RANGE = usable_ranges.at(range_i);
            if (RANGE.end - RANGE.base <= paging::PAGE_SIZE) {
                continue;
            }

            main_heap_size += RANGE.end - RANGE.base;

            paging::PageZone* zone =
                init_page_zone(reinterpret_cast<uint64_t>(addr::get_virt_pointer(RANGE.base)), RANGE.end - RANGE.base, zone_num++);

            if (zones_tail == nullptr) {
                zones = zone;  // set the head
            } else {
                zones_tail->next = zone;
            }
            zones_tail = zone;
            append_range(accepted_ranges, accepted_range_count, RANGE);
        }
    }

    if (zones_tail == nullptr) {
        hcf();  // no usable memory???
    }

    zones_tail->next = nullptr;
}

void set_kernel_cr3(uint64_t cr3) {
    kernel_cr3 = cr3;

#ifdef WOS_PHYS_LOCK_DEBUG
    // Re-initialize the tracked memlock after pagemap switch so that
    // stale CR3/RIP values from boot-time Limine pagemaps are cleared.
    memlock.locked.store(false, std::memory_order_release);
    memlock.holder_cr3 = 0;
    memlock.holder_cpu = 0;
    memlock.holder_rip = 0;
#else
    memlock.locked.store(false, std::memory_order_release);
#endif
}

void init_huge_page_zone_deferred() {
    // Map and initialize per-CPU caches first
    if (per_cpu_caches_size > 0 && per_cpu_caches_phys_base != 0) {
        log::info("mapping per-CPU caches: base=0x%016x size=0x%016x", per_cpu_caches_phys_base, per_cpu_caches_size);

        for (uint64_t offset = 0; offset < per_cpu_caches_size; offset += paging::PAGE_SIZE) {
            uint64_t const PHYS = per_cpu_caches_phys_base + offset;
            auto virt = reinterpret_cast<uint64_t>(addr::get_virt_pointer(PHYS));
            virt::map_to_kernel_page_table(virt, PHYS, paging::page_types::KERNEL);
        }

        log::info("per-CPU caches mapped, initializing structures");

        // Now we can safely access the memory
        void* cache_memory = addr::get_virt_pointer(per_cpu_caches_phys_base);
        per_cpu_caches = static_cast<PerCpuPageCache*>(cache_memory);
        for (size_t i = 0; i < num_cpus; i++) {
            new (&per_cpu_caches[i]) PerCpuPageCache();  // Placement new
        }

        log::info("per-CPU caches initialized for %zu CPUs", num_cpus);
    }

    // Initialize the huge page zone after virt::initPagemap() has set up the kernel page map
    // This ensures the zone metadata is in mapped memory
    if (huge_page_size > 0 && huge_page_base != 0) {
        log::info("mapping huge page region: base=0x%016x size=0x%016x", huge_page_base, huge_page_size);

        for (uint64_t offset = 0; offset < huge_page_size; offset += paging::PAGE_SIZE) {
            uint64_t const PHYS = huge_page_base + offset;
            auto virt = reinterpret_cast<uint64_t>(addr::get_virt_pointer(PHYS));
            virt::map_to_kernel_page_table(virt, PHYS, paging::page_types::KERNEL);
        }

        log::info("huge page region mapped, initializing zone");

        uint64_t const FLAGS = memlock.lock_irq();
        // Pass physical addresses - initHugePageZone will convert to virtual
        huge_page_zone = init_huge_page_zone(huge_page_base, huge_page_size);
        memlock.unlock_irq(FLAGS);

        if (huge_page_zone != nullptr) {
            log::info("huge page zone initialized: start=0x%016x len=0x%016x pages=%zu", huge_page_zone->start, huge_page_zone->len,
                      huge_page_zone->page_count);
        } else {
            log::error("failed to initialize huge page zone");
        }
    }
}

void enable_per_cpu_allocations() {
    per_cpu_ready.store(true, std::memory_order_release);
    cpu::notify_per_cpu_ready();
}

auto page_alloc(uint64_t size, std::string_view name) -> void* {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    void* caller_addr = __builtin_return_address(0);
    uint64_t const NUM_PAGES = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
#endif

    // Try per-CPU cache first for single-page allocations
    if (USE_PER_CPU_PAGE_CACHE && size == paging::PAGE_SIZE && per_cpu_caches != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
        uint64_t const CPU_ID = cpu::get_current_cpu_id_safe();
        if (CPU_ID < num_cpus) {
            PerCpuPageCache& cache = per_cpu_caches[CPU_ID];
            cache.lock.lock();

            if (cache.count > 0) {
                // Fast path: pop from cache
                void* page = cache.pages.at(--cache.count);
                cache.lock.unlock();

                total_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
                alloc_count.fetch_add(1, std::memory_order_relaxed);

                // Double-alloc sentinel: if the page still holds a live slab header
                // it was freed to this cache while still in the slab chain.
                constexpr uint32_t SLAB_MAGIC_CACHE = 0x8CBEEFC8;
                if (*reinterpret_cast<const volatile uint32_t*>(page) == SLAB_MAGIC_CACHE) {
                    log::critical("DETECT: pageAlloc (cache) returning live slab page - double-alloc trap! virt=%p", page);
                    hcf();
                }

                // Zero the page
                uint64_t saved_cr3 = 0;
                if (kernel_cr3 != 0) {
                    uint64_t const CURRENT_CR3 = rdcr3();
                    if (CURRENT_CR3 != kernel_cr3) {
                        saved_cr3 = CURRENT_CR3;
                        wrcr3(kernel_cr3);
                    }
                }
                std::memset(page, 0, size);
#ifdef WOS_KASAN
                if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
                    kasan::unpoison_range(page, size);
                }
#endif
                if (saved_cr3 != 0) {
                    wrcr3(saved_cr3);
                }

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
                record_page_alloc_caller(caller_addr, NUM_PAGES);
#endif
                return page;
            }
            cache.lock.unlock();
        }
    }

    // Slow path: allocate from zones
    uint64_t const FLAGS = memlock.lock_irq();
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLER_TAG = caller_stats_runtime_enabled.load(std::memory_order_relaxed) ? reinterpret_cast<uint64_t>(caller_addr) : 0;
#else
    uint64_t const CALLER_TAG = 0;
#endif
    void* block = find_free_block(size, CALLER_TAG);
    memlock.unlock_irq(FLAGS);

    if (block == nullptr) {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        void* const OOM_CALLER_ADDR = caller_addr;
#else
        void* const OOM_CALLER_ADDR = __builtin_return_address(0);
#endif
        // OOM condition - dump allocation info for debugging
        io::serial::write("OOM: pageAlloc failed for size ");
        io::serial::write_hex(size);
        io::serial::write(" bytes\n");
        io::serial::write("Allocation site: 0x");
        io::serial::write_hex(reinterpret_cast<uint64_t>(OOM_CALLER_ADDR));
        io::serial::write(" (");
        io::serial::write(name.data(), name.size());
        io::serial::write(")\n");
        dump_page_allocations_oom();
        return nullptr;
    }

    total_allocated_bytes.fetch_add(buddy_accounting_size(size), std::memory_order_relaxed);
    alloc_count.fetch_add(1, std::memory_order_relaxed);

    // Validate the returned address is in a reasonable HHDM range
    auto block_addr = reinterpret_cast<uint64_t>(block);
    constexpr uint64_t HHDM_BASE = 0xffff800000000000ULL;
    constexpr uint64_t HHDM_END = 0xffff808000000000ULL;  // ~512GB max physical
    if (block_addr < HHDM_BASE || block_addr >= HHDM_END) {
        io::serial::write("FATAL: pageAlloc returned invalid HHDM addr: ");
        io::serial::write_hex(block_addr);
        io::serial::write("\n");
        hcf();
    }

    // Zero outside the lock - the block is exclusively ours now
    // Double-alloc sentinel (buddy path): if the page still holds a live slab
    // header it was freed to the buddy while still referenced by the slab chain.
    constexpr uint32_t SLAB_MAGIC_BUDDY = 0x8CBEEFC8;
    if (*reinterpret_cast<const volatile uint32_t*>(block) == SLAB_MAGIC_BUDDY) {
        log::critical("DETECT: pageAlloc (buddy) returning live slab page - double-alloc trap! virt=%p", block);
        hcf();
    }

    // Zero outside the lock - the block is exclusively ours now
    uint64_t saved_cr3 = 0;
    if (kernel_cr3 != 0) {
        uint64_t const CURRENT_CR3 = rdcr3();
        if (CURRENT_CR3 != kernel_cr3) {
            saved_cr3 = CURRENT_CR3;
            wrcr3(kernel_cr3);
        }
    }

    std::memset(block, 0, size);
#ifdef WOS_KASAN
    if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
        kasan::unpoison_range(block, size);
    }
#endif

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    record_page_alloc_caller(caller_addr, NUM_PAGES);
#endif
    return block;
}

auto page_alloc_huge(uint64_t size) -> void* {
    if (huge_page_zone == nullptr) {
        return nullptr;
    }

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    void* caller_addr = __builtin_return_address(0);
    uint64_t const NUM_PAGES = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
#endif

    // Allocate from dedicated huge page zone
    uint64_t const FLAGS = memlock.lock_irq();
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLER_TAG = caller_stats_runtime_enabled.load(std::memory_order_relaxed) ? reinterpret_cast<uint64_t>(caller_addr) : 0;
#else
    uint64_t const CALLER_TAG = 0;
#endif
    void* block = find_free_block_huge(size, CALLER_TAG);
    memlock.unlock_irq(FLAGS);

    if (block == nullptr) {
        return nullptr;
    }

    total_allocated_bytes.fetch_add(buddy_accounting_size(size), std::memory_order_relaxed);
    alloc_count.fetch_add(1, std::memory_order_relaxed);

    // Zero outside the lock - the block is exclusively ours now
    uint64_t saved_cr3 = 0;
    if (kernel_cr3 != 0) {
        uint64_t const CURRENT_CR3 = rdcr3();
        if (CURRENT_CR3 != kernel_cr3) {
            saved_cr3 = CURRENT_CR3;
            wrcr3(kernel_cr3);
        }
    }

    std::memset(block, 0, size);
#ifdef WOS_KASAN
    if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
        kasan::unpoison_range(block, size);
    }
#endif

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    record_page_alloc_caller(caller_addr, NUM_PAGES);
#endif
    return block;
}

void page_free(void* page) {
    // Try to return single pages to per-CPU cache
    if (USE_PER_CPU_PAGE_CACHE && per_cpu_caches != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
        uint64_t const CPU_ID = cpu::get_current_cpu_id_safe();
        if (CPU_ID < num_cpus) {
            PerCpuPageCache& cache = per_cpu_caches[CPU_ID];
            cache.lock.lock();

            if (cache.count < PerCpuPageCache::CACHE_SIZE) {
                // Fast path: push to cache
                cache.pages.at(cache.count++) = page;
                cache.lock.unlock();

                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
                return;
            }
            cache.lock.unlock();
        }
    }

    // Slow path: return to zone
    uint64_t const FLAGS = memlock.lock_irq();

    // Try huge page zone first
    if (huge_page_zone != nullptr) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR >= huge_page_zone->start && PAGE_ADDR < huge_page_zone->start + huge_page_zone->len) {
            if (huge_page_zone->allocator != nullptr) {
                uint64_t const FREED_BYTES = huge_page_zone->allocator->free(page);
                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(FREED_BYTES, std::memory_order_relaxed);
            }
            memlock.unlock_irq(FLAGS);
            return;
        }
    }

    // Check regular zones
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR < zone->start || PAGE_ADDR >= zone->start + zone->len) {
            continue;
        }

        if (zone->allocator != nullptr) {
            uint64_t const FREED_BYTES = zone->allocator->free(page);
            free_count.fetch_add(1, std::memory_order_relaxed);
            total_freed_bytes.fetch_add(FREED_BYTES, std::memory_order_relaxed);
        }
        break;
    }

    memlock.unlock_irq(FLAGS);
}

auto page_split_to_order0(void* page) -> bool {
    if (page == nullptr) {
        return false;
    }

    uint64_t const FLAGS = memlock.lock_irq();

    if (huge_page_zone != nullptr) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR >= huge_page_zone->start && PAGE_ADDR < huge_page_zone->start + huge_page_zone->len) {
            bool const OK = huge_page_zone->allocator != nullptr && huge_page_zone->allocator->split_allocated_block_to_order0(page);
            memlock.unlock_irq(FLAGS);
            return OK;
        }
    }

    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR < zone->start || PAGE_ADDR >= zone->start + zone->len) {
            continue;
        }

        bool const OK = zone->allocator != nullptr && zone->allocator->split_allocated_block_to_order0(page);
        memlock.unlock_irq(FLAGS);
        return OK;
    }

    memlock.unlock_irq(FLAGS);
    return false;
}

// --- Frame reference counting helpers ---

namespace {
// Find the PageAllocator and page index for a given HHDM pointer.
// Returns true if found, populating allocator and pageIdx.
auto find_allocator_for_page(void* page, PageAllocator*& out_alloc, uint32_t& out_idx) -> bool {
    auto addr = reinterpret_cast<uint64_t>(page);
    // Check regular zones first
    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        if (addr >= zone->start && addr < zone->start + zone->len && zone->allocator != nullptr) {
            out_alloc = zone->allocator;
            out_idx = static_cast<uint32_t>((addr - zone->allocator->base) / paging::PAGE_SIZE);
            if (out_idx < out_alloc->total_pages) {
                return true;
            }
        }
    }
    // Check huge page zone
    if (huge_page_zone != nullptr && addr >= huge_page_zone->start && addr < huge_page_zone->start + huge_page_zone->len &&
        huge_page_zone->allocator != nullptr) {
        out_alloc = huge_page_zone->allocator;
        out_idx = static_cast<uint32_t>((addr - huge_page_zone->allocator->base) / paging::PAGE_SIZE);
        if (out_idx < out_alloc->total_pages) {
            return true;
        }
    }
    return false;
}
}  // namespace

void page_ref_inc(void* page) {
    if (page == nullptr) {
        return;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t const FLAGS = memlock.lock_irq();
    if (find_allocator_for_page(page, alloc, idx)) {
        alloc->page_refcounts[idx]++;
    }
    memlock.unlock_irq(FLAGS);
}

void page_ref_add(void* page, uint64_t refs) {
    if (page == nullptr || refs == 0) {
        return;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t const FLAGS = memlock.lock_irq();
    if (find_allocator_for_page(page, alloc, idx)) {
        if (refs > UINT32_MAX - alloc->page_refcounts[idx]) {
            memlock.unlock_irq(FLAGS);
            log::critical("page_ref_add overflow for page %p refs=%llu", page, static_cast<unsigned long long>(refs));
            hcf();
        }
        alloc->page_refcounts[idx] += static_cast<uint32_t>(refs);
    }
    memlock.unlock_irq(FLAGS);
}

auto page_ref_dec(void* page) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t const FLAGS = memlock.lock_irq();
    if (find_allocator_for_page(page, alloc, idx)) {
        uint32_t new_ref = alloc->page_refcounts[idx];
        if (new_ref > 0) {
            new_ref = --alloc->page_refcounts[idx];
            if (new_ref == 0) {  // UAF sentinel: a live slab page must never reach refcount 0 via page_ref_dec.
                // If the first 4 bytes match the slab magic the page is still in the slab
                // chain - some corrupt PTE is being incorrectly treated as a user data page.
                constexpr uint32_t SLAB_MAGIC = 0x8CBEEFC8;
                if (*reinterpret_cast<const volatile uint32_t*>(page) == SLAB_MAGIC) {
                    memlock.unlock_irq(FLAGS);
                    log::critical("DETECT: page_ref_dec freeing live slab page - UAF trap! virt=%p", page);
                    hcf();
                }

                // Refcount reached zero - free the page
                uint64_t const FREED_BYTES = alloc->free(page);
                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(FREED_BYTES, std::memory_order_relaxed);
            }
        }
        // If new_ref was already 0 (page already freed), do NOT call alloc->free again.
        memlock.unlock_irq(FLAGS);
        return new_ref;
    }
    memlock.unlock_irq(FLAGS);
    return 0;
}

auto page_ref_get(void* page) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t const FLAGS = memlock.lock_irq();
    uint32_t ref = 0;
    if (find_allocator_for_page(page, alloc, idx)) {
        ref = alloc->page_refcounts[idx];
    }
    memlock.unlock_irq(FLAGS);
    return ref;
}

void dump_mini_malloc_stats() { mini_malloc::mini_dump_stats(); }

void dump_kmalloc_tracked_allocs() { ker::mod::mm::dyn::kmalloc::dump_tracked_allocations(); }

}  // namespace ker::mod::mm::phys
