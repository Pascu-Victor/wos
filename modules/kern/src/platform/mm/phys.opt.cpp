#include "phys.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>
#include <sanitizer/kasan.hpp>
#include <span>
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
#include "platform/sched/scheduler.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace {
// Forward declaration - we'll get kernel pagemap physical address once during init
uint64_t kernel_cr3 = 0;
}  // anonymous namespace

namespace ker::mod::mm::phys {

namespace {
using log = ker::mod::dbg::logger<"phys">;
namespace emergency_serial = ker::mod::dbg::emergency_serial_unlocked;
void dump_page_alloc_oom_header(uint64_t size, void* caller_addr, std::string_view name);
void write_hex_field(const char* label, uint64_t value);

// Per-CPU page cache for reducing lock contention
struct CachedOrder0Page {
    PageAllocator* allocator = nullptr;
    uint32_t page_idx = 0;
};

struct PerCpuPageCache {
    static constexpr size_t CACHE_SIZE = 16;  // Pages per CPU cache
    static constexpr size_t REFILL_BATCH = 8;
    static constexpr size_t DRAIN_BATCH = 8;
    std::array<CachedOrder0Page, CACHE_SIZE> pages{};
    size_t count{};
    sys::Spinlock lock;  // Fine-grained per-CPU lock
};

__attribute__((section(".data"))) paging::PageZone* zones = nullptr;
__attribute__((section(".data"))) paging::PageZone* huge_page_zone = nullptr;  // Dedicated zone for huge allocations

struct ZoneLookupEntry {
    uint64_t start = 0;
    uint64_t end = 0;
    paging::PageZone* zone = nullptr;
};

constexpr size_t MAX_ZONE_LOOKUP_ENTRIES = 128;
std::array<ZoneLookupEntry, MAX_ZONE_LOOKUP_ENTRIES> regular_zone_lookup{};
size_t regular_zone_lookup_count = 0;
bool regular_zone_lookup_ready = false;

// Per-CPU caches (initialized in init())
PerCpuPageCache* per_cpu_caches = nullptr;
size_t num_cpus = 0;
std::atomic<bool> per_cpu_ready{false};  // Set after per-CPU structures are initialized

// The cache is enabled only for pages whose owning allocator proves they are
// independent order-0 pages. Cached pages have a distinct allocator flag and are
// not linked in buddy lists, so coalescing cannot consume a CPU-local entry.
constexpr bool USE_PER_CPU_PAGE_CACHE = true;

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

TrackedSpinlock memlock;  // Lifecycle lock for zone-list and huge-zone initialization

// Per-CPU cache deferred initialization info
uint64_t per_cpu_caches_phys_base = 0;
uint64_t per_cpu_caches_size = 0;

// Statistics counters
uint64_t main_heap_size = 0;

// Huge page zone deferred initialization info
uint64_t huge_page_base = 0;
uint64_t huge_page_size = 0;

// Allocation tracking counters (now atomic for multi-CPU safety).
// Byte counters are cumulative, buddy-rounded physical allocator bytes.
// Operation counters count logical allocation/release events, not pages and
// not current buddy free-list blocks.
std::atomic<uint64_t> total_allocated_bytes{0};
std::atomic<uint64_t> total_freed_bytes{0};
std::atomic<uint64_t> allocation_operation_count{0};
std::atomic<uint64_t> free_operation_count{0};

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

auto accounted_pages(uint64_t bytes) -> uint64_t { return bytes / paging::PAGE_SIZE; }

std::atomic<uint64_t> page_ref_inc_ops{0};                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_inc_cas_retries{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_add_ops{0};                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_add_refs{0};                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_add_cas_retries{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_dec_ops{0};                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_dec_cas_retries{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_dec_zero_candidates{0};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_dec_zero_pages_freed{0};        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_dec_zero_validation_failed{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_batch_calls{0};                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_batch_pages{0};                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_batch_zero_candidates{0};       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_batch_free_runs{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_ref_batch_pages_freed{0};           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_alloc_hits{0};                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_alloc_misses{0};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_refill_calls{0};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_refill_pages{0};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_free_hits{0};                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_free_misses{0};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_drain_calls{0};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_drain_pages{0};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> page_cache_stale_entries{0};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto total_cached_order0_pages_snapshot() -> uint64_t {
    uint64_t total = 0;
    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->allocator == nullptr) {
            continue;
        }
        uint64_t const FLAGS = zone->allocator->lock_irq();
        total += zone->allocator->cached_order0_count;
        zone->allocator->unlock_irq(FLAGS);
    }
    return total;
}

auto live_allocated_bytes_from_counters(uint64_t allocated, uint64_t freed) -> uint64_t {
    return allocated >= freed ? allocated - freed : 0;
}

auto free_mem_bytes_from_live(uint64_t live_allocated_bytes) -> uint64_t {
    return main_heap_size >= live_allocated_bytes ? main_heap_size - live_allocated_bytes : 0;
}

void note_physical_alloc(uint64_t accounted_bytes) {
    total_allocated_bytes.fetch_add(accounted_bytes, std::memory_order_relaxed);
    allocation_operation_count.fetch_add(1, std::memory_order_relaxed);
}

void note_physical_free(uint64_t freed_bytes, uint64_t release_operations = 1) {
    if (freed_bytes == 0 || release_operations == 0) {
        return;
    }
    total_freed_bytes.fetch_add(freed_bytes, std::memory_order_relaxed);
    free_operation_count.fetch_add(release_operations, std::memory_order_relaxed);
}

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

paging::PageZone* scan_regular_zone_for_addr(uint64_t addr) {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        uint64_t const END = zone->start + zone->len;
        if (END < zone->start || addr < zone->start || addr >= END) {
            continue;
        }
        return zone;
    }
    return nullptr;
}

void rebuild_regular_zone_lookup_index() {
    regular_zone_lookup_ready = false;
    regular_zone_lookup_count = 0;
    for (auto& entry : regular_zone_lookup) {
        entry = {};
    }

    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        uint64_t const END = zone->start + zone->len;
        if (zone->allocator == nullptr || zone->len == 0 || END <= zone->start) {
            continue;
        }
        if (regular_zone_lookup_count >= regular_zone_lookup.size()) {
            regular_zone_lookup_count = 0;
            return;
        }
        regular_zone_lookup[regular_zone_lookup_count++] = ZoneLookupEntry{
            .start = zone->start,
            .end = END,
            .zone = zone,
        };
    }

    std::sort(regular_zone_lookup.begin(), regular_zone_lookup.begin() + static_cast<ptrdiff_t>(regular_zone_lookup_count),
              [](ZoneLookupEntry const& lhs, ZoneLookupEntry const& rhs) { return lhs.start < rhs.start; });
    regular_zone_lookup_ready = regular_zone_lookup_count != 0;
}

paging::PageZone* regular_zone_for_addr(uint64_t addr) {
    if (!regular_zone_lookup_ready) {
        return scan_regular_zone_for_addr(addr);
    }

    size_t low = 0;
    size_t high = regular_zone_lookup_count;
    while (low < high) {
        size_t const MID = low + ((high - low) / 2);
        ZoneLookupEntry const& entry = regular_zone_lookup[MID];
        if (addr < entry.start) {
            high = MID;
            continue;
        }
        if (addr >= entry.end) {
            low = MID + 1;
            continue;
        }
        return entry.zone;
    }
    return nullptr;
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

    auto scan_allocator = [&](PageAllocator* alloc) {
        if (alloc == nullptr || alloc->page_callers == nullptr || alloc->page_flags == nullptr) {
            return;
        }

        uint64_t const FLAGS = alloc->lock_irq();
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
        alloc->unlock_irq(FLAGS);
    };
    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        scan_allocator(zone->allocator);
    }
    if (huge_page_zone != nullptr) {
        scan_allocator(huge_page_zone->allocator);
    }

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
    uint64_t const ALLOCATED = total_allocated_bytes.load(std::memory_order_relaxed);
    uint64_t const FREED = total_freed_bytes.load(std::memory_order_relaxed);
    uint64_t const LIVE = live_allocated_bytes_from_counters(ALLOCATED, FREED);
    emergency_serial::write("Physical alloc stats:");
    write_hex_field(" allocated=", ALLOCATED);
    write_hex_field(" freed=", FREED);
    write_hex_field(" live=", LIVE);
    write_hex_field(" allocOps=", allocation_operation_count.load(std::memory_order_relaxed));
    write_hex_field(" freeOps=", free_operation_count.load(std::memory_order_relaxed));
    write_hex_field(" allocPages=", accounted_pages(ALLOCATED));
    write_hex_field(" freePages=", accounted_pages(FREED));
    emergency_serial::write("\n");
}

auto get_free_mem_bytes() -> uint64_t {
    uint64_t const ALLOCATED = total_allocated_bytes.load(std::memory_order_relaxed);
    uint64_t const FREED = total_freed_bytes.load(std::memory_order_relaxed);
    return free_mem_bytes_from_live(live_allocated_bytes_from_counters(ALLOCATED, FREED));
}

auto get_free_mem_pages() -> uint64_t { return accounted_pages(get_free_mem_bytes()); }

auto get_total_mem_bytes() -> uint64_t { return main_heap_size; }

void get_alloc_stats_snapshot(AllocStatsSnapshot& out) {
    uint64_t const ALLOCATED = total_allocated_bytes.load(std::memory_order_relaxed);
    uint64_t const FREED = total_freed_bytes.load(std::memory_order_relaxed);
    uint64_t const LIVE = live_allocated_bytes_from_counters(ALLOCATED, FREED);
    uint64_t const FREE_BYTES = free_mem_bytes_from_live(LIVE);
    out = AllocStatsSnapshot{.total_allocated_bytes = ALLOCATED,
                             .total_freed_bytes = FREED,
                             .live_allocated_bytes = LIVE,
                             .alloc_count = allocation_operation_count.load(std::memory_order_relaxed),
                             .free_count = free_operation_count.load(std::memory_order_relaxed),
                             .total_allocated_pages = accounted_pages(ALLOCATED),
                             .total_freed_pages = accounted_pages(FREED),
                             .live_allocated_pages = accounted_pages(LIVE),
                             .current_free_pages = accounted_pages(FREE_BYTES),
                             .total_mem_bytes = get_total_mem_bytes(),
                             .free_mem_bytes = FREE_BYTES};
}

auto snapshot_zones(ZoneSnapshot* out, size_t max_rows) -> size_t {
    if (out == nullptr || max_rows == 0) {
        return 0;
    }

    static_assert(MEMACC_BUDDY_ORDER_COUNT == static_cast<size_t>(PageAllocator::MAX_ORDER + 1));

    size_t rows = 0;
    for (paging::PageZone const* zone = zones; zone != nullptr && rows < max_rows; zone = zone->next) {
        ZoneSnapshot snap{};
        snap.zone_num = zone->zone_num;
        snap.start = zone->start;
        snap.len = zone->len;
        snap.page_count = zone->page_count;
        snap.name = zone->name;

        if (zone->allocator != nullptr) {
            snap.has_allocator = true;
            auto* alloc = zone->allocator;
            uint64_t const FLAGS = alloc->lock_irq();
            snap.total_pages = alloc->total_pages;
            snap.usable_pages = alloc->usable_pages;
            snap.free_pages = alloc->free_count;
            snap.metadata_pages = alloc->metadata_pages;
            snap.cached_order0_pages = alloc->cached_order0_count;

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
                snap.scanned_free_pages += alloc->cached_order0_count;
                snap.free_count_mismatch = snap.scanned_free_pages != alloc->free_count;
            }
            alloc->unlock_irq(FLAGS);
        }

        out[rows++] = snap;
    }
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
    emergency_serial::write("Physical page alloc by caller: disabled (build with WOS_PHYS_ALLOC_CALLER_STATS=ON)\n");
#else
    emergency_serial::write("Physical page alloc by caller (cumulative, sorted by pages desc):\n");

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
        emergency_serial::write("  0x");
        emergency_serial::write_hex(snapshot.at(i).caller);
        emergency_serial::write(": pages=0x");
        emergency_serial::write_hex(snapshot.at(i).pages);
        emergency_serial::write(" kb=0x");
        emergency_serial::write_hex(snapshot.at(i).pages * paging::PAGE_SIZE / BYTES_PER_KB);
        emergency_serial::write("\n");
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
        return paging::PAGE_SIZE;
    }

    uint64_t pages = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
    uint64_t rounded_pages = 1;
    while (rounded_pages < pages && rounded_pages < (uint64_t{1} << PageAllocator::MAX_ORDER)) {
        rounded_pages <<= 1;
    }
    return rounded_pages * paging::PAGE_SIZE;
}

auto page_alloc_order_for_size(uint64_t size, int& out_order, uint64_t& out_pages) -> bool {
    if (size == 0) {
        out_order = 0;
        out_pages = 0;
        return true;
    }

    uint64_t const PAGES = (size / paging::PAGE_SIZE) + ((size % paging::PAGE_SIZE) != 0 ? 1 : 0);
    uint64_t rounded_pages = 1;
    int order = 0;
    while (rounded_pages < PAGES) {
        if (order >= PageAllocator::MAX_ORDER) {
            return false;
        }
        rounded_pages <<= 1;
        order++;
    }

    out_order = order;
    out_pages = rounded_pages;
    return true;
}

auto allocator_has_free_block_for_order(const PageAllocator* allocator, int order) -> bool {
    if (allocator == nullptr || order < 0 || order > PageAllocator::MAX_ORDER) {
        return false;
    }

    for (int idx = order; idx <= PageAllocator::MAX_ORDER; ++idx) {
        if (allocator->free_list[static_cast<size_t>(idx)] != nullptr) {
            return true;
        }
    }
    return false;
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
        if (zone->len < size || zone->allocator == nullptr) {
            continue;
        }

        uint64_t const FLAGS = zone->allocator->lock_irq();
        void* const BLOCK = zone->allocator->alloc(size, caller);
        zone->allocator->unlock_irq(FLAGS);
        if (BLOCK == nullptr) {
            [[unlikely]] continue;
        }
        return BLOCK;
    }

    return nullptr;
}

auto find_free_order0_block(uint64_t caller) -> void* {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < paging::PAGE_SIZE || zone->allocator == nullptr) {
            continue;
        }

        uint64_t const FLAGS = zone->allocator->lock_irq();
        void* const BLOCK = zone->allocator->alloc_order0(caller);
        zone->allocator->unlock_irq(FLAGS);
        if (BLOCK == nullptr) {
            [[unlikely]] continue;
        }
        return BLOCK;
    }

    return nullptr;
}

auto find_free_block_huge(uint64_t size, uint64_t caller = 0) -> void* {
    if (huge_page_zone == nullptr || huge_page_zone->len < size || huge_page_zone->allocator == nullptr) {
        return nullptr;
    }

    uint64_t const FLAGS = huge_page_zone->allocator->lock_irq();
    void* const BLOCK = huge_page_zone->allocator->alloc(size, caller);
    huge_page_zone->allocator->unlock_irq(FLAGS);
    return BLOCK;
}

}  // namespace

auto page_alloc_can_satisfy(uint64_t size, uint64_t reserve_bytes) -> bool {
    int order = 0;
    uint64_t requested_pages = 0;
    if (!page_alloc_order_for_size(size, order, requested_pages)) {
        return false;
    }
    if (requested_pages == 0) {
        return true;
    }

    uint64_t const RESERVE_PAGES = (reserve_bytes / paging::PAGE_SIZE) + ((reserve_bytes % paging::PAGE_SIZE) != 0 ? 1 : 0);

    uint64_t total_free_pages = 0;
    bool has_block = false;

    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->allocator == nullptr) {
            continue;
        }
        uint64_t const FLAGS = zone->allocator->lock_irq();
        total_free_pages += zone->allocator->get_free_pages();
        if (allocator_has_free_block_for_order(zone->allocator, order)) {
            has_block = true;
        }
        zone->allocator->unlock_irq(FLAGS);
    }

    if (!has_block || total_free_pages < requested_pages) {
        return false;
    }
    return total_free_pages - requested_pages >= RESERVE_PAGES;
}

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
    rebuild_regular_zone_lookup_index();
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

namespace {

enum class ReturnedPageZeroing : uint8_t {
    ZERO,
    FULL_OVERWRITE,
};

void prepare_allocated_block(void* block, uint64_t size, ReturnedPageZeroing zeroing) {
    uint64_t saved_cr3 = 0;
    if (zeroing == ReturnedPageZeroing::ZERO && kernel_cr3 != 0) {
        uint64_t const CURRENT_CR3 = rdcr3();
        if (CURRENT_CR3 != kernel_cr3) {
            saved_cr3 = CURRENT_CR3;
            wrcr3(kernel_cr3);
        }
    }

    if (zeroing == ReturnedPageZeroing::ZERO) {
        std::memset(block, 0, size);
    }
#ifdef WOS_KASAN
    if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
        kasan::unpoison_range(block, size);
    }
#endif

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }
}

void drain_per_cpu_cache_locked(PerCpuPageCache& cache, size_t max_pages) {
    size_t drained = 0;
    while (cache.count > 0 && drained < max_pages) {
        CachedOrder0Page const ENTRY = cache.pages[--cache.count];
        cache.pages[cache.count] = {};
        if (ENTRY.allocator == nullptr) {
            page_cache_stale_entries.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        uint64_t const FLAGS = ENTRY.allocator->lock_irq();
        uint64_t const RELEASED_BYTES = ENTRY.allocator->release_cached_order0_at(ENTRY.page_idx);
        ENTRY.allocator->unlock_irq(FLAGS);
        if (RELEASED_BYTES == paging::PAGE_SIZE) {
            drained++;
            continue;
        }
        page_cache_stale_entries.fetch_add(1, std::memory_order_relaxed);
    }

    if (drained != 0) {
        page_cache_drain_calls.fetch_add(1, std::memory_order_relaxed);
        page_cache_drain_pages.fetch_add(drained, std::memory_order_relaxed);
    }
}

void refill_per_cpu_cache_locked(PerCpuPageCache& cache) {
    if (cache.count >= cache.pages.size()) {
        return;
    }

    size_t const TARGET = std::min(cache.pages.size(), cache.count + PerCpuPageCache::REFILL_BATCH);
    size_t refilled = 0;
    page_cache_refill_calls.fetch_add(1, std::memory_order_relaxed);
    for (paging::PageZone* zone = zones; zone != nullptr && cache.count < TARGET; zone = zone->next) {
        if (zone->allocator == nullptr) {
            continue;
        }

        uint64_t const FLAGS = zone->allocator->lock_irq();
        while (cache.count < TARGET) {
            uint32_t page_idx = 0;
            void* const PAGE = zone->allocator->claim_free_order0_for_cache(page_idx);
            if (PAGE == nullptr) {
                break;
            }
            cache.pages[cache.count++] = CachedOrder0Page{
                .allocator = zone->allocator,
                .page_idx = page_idx,
            };
            refilled++;
        }
        zone->allocator->unlock_irq(FLAGS);
    }

    if (refilled != 0) {
        page_cache_refill_pages.fetch_add(refilled, std::memory_order_relaxed);
    }
}

auto try_alloc_from_per_cpu_cache(uint64_t caller_tag, ReturnedPageZeroing zeroing, void* caller_addr) -> void* {
    if (!USE_PER_CPU_PAGE_CACHE || per_cpu_caches == nullptr || !per_cpu_ready.load(std::memory_order_acquire)) {
        return nullptr;
    }

    uint64_t const CPU_ID = cpu::get_current_cpu_id_safe();
    if (CPU_ID >= num_cpus) {
        return nullptr;
    }

    PerCpuPageCache& cache = per_cpu_caches[CPU_ID];
    uint64_t const CACHE_FLAGS = cache.lock.lock_irqsave();
    if (cache.count == 0) {
        refill_per_cpu_cache_locked(cache);
    }

    void* page = nullptr;
    if (cache.count > 0) {
        CachedOrder0Page const ENTRY = cache.pages[--cache.count];
        cache.pages[cache.count] = {};
        if (ENTRY.allocator != nullptr) {
            uint64_t const FLAGS = ENTRY.allocator->lock_irq();
            page = ENTRY.allocator->alloc_cached_order0_at(ENTRY.page_idx, caller_tag);
            ENTRY.allocator->unlock_irq(FLAGS);
        }
        if (page == nullptr) {
            page_cache_stale_entries.fetch_add(1, std::memory_order_relaxed);
        }
    }
    cache.lock.unlock_irqrestore(CACHE_FLAGS);

    if (page == nullptr) {
        page_cache_alloc_misses.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    page_cache_alloc_hits.fetch_add(1, std::memory_order_relaxed);
    note_physical_alloc(paging::PAGE_SIZE);

    constexpr uint32_t SLAB_MAGIC_CACHE = 0x8CBEEFC8;
    if (*reinterpret_cast<const volatile uint32_t*>(page) == SLAB_MAGIC_CACHE) {
        log::critical("DETECT: pageAlloc (cache) returning live slab page - double-alloc trap! virt=%p", page);
        hcf();
    }

    prepare_allocated_block(page, paging::PAGE_SIZE, zeroing);

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    record_page_alloc_caller(caller_addr, 1);
#else
    (void)caller_addr;
#endif
    return page;
}

auto try_cache_freed_order0_page(PageAllocator* allocator, void* page) -> bool {
    if (!USE_PER_CPU_PAGE_CACHE || allocator == nullptr || page == nullptr || per_cpu_caches == nullptr ||
        !per_cpu_ready.load(std::memory_order_acquire)) {
        return false;
    }

    uint64_t const CPU_ID = cpu::get_current_cpu_id_safe();
    if (CPU_ID >= num_cpus) {
        return false;
    }

    PerCpuPageCache& cache = per_cpu_caches[CPU_ID];
    uint64_t const CACHE_FLAGS = cache.lock.lock_irqsave();
    if (cache.count >= cache.pages.size()) {
        drain_per_cpu_cache_locked(cache, PerCpuPageCache::DRAIN_BATCH);
    }

    bool cached = false;
    if (cache.count < cache.pages.size()) {
        uint32_t page_idx = 0;
        uint64_t const FLAGS = allocator->lock_irq();
        cached = allocator->cache_allocated_order0(page, page_idx);
        allocator->unlock_irq(FLAGS);
        if (cached) {
            cache.pages[cache.count++] = CachedOrder0Page{
                .allocator = allocator,
                .page_idx = page_idx,
            };
        }
    }
    cache.lock.unlock_irqrestore(CACHE_FLAGS);

    if (!cached) {
        page_cache_free_misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    page_cache_free_hits.fetch_add(1, std::memory_order_relaxed);
    note_physical_free(paging::PAGE_SIZE);
    return true;
}

auto page_alloc_impl(uint64_t size, std::string_view name, ReturnedPageZeroing zeroing, void* caller_addr, bool log_oom) -> void* {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const NUM_PAGES = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
#endif
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLER_TAG = caller_stats_runtime_enabled.load(std::memory_order_relaxed) ? reinterpret_cast<uint64_t>(caller_addr) : 0;
#else
    uint64_t const CALLER_TAG = 0;
#endif

    // Try per-CPU cache first for single-page allocations. Cached entries carry
    // their allocator/index proof and are transitioned back to allocated state
    // under the owning allocator lock before the page is exposed.
    if (size == paging::PAGE_SIZE) {
        void* const CACHED_PAGE = try_alloc_from_per_cpu_cache(CALLER_TAG, zeroing, caller_addr);
        if (CACHED_PAGE != nullptr) {
            return CACHED_PAGE;
        }
    }

    // Slow path: allocate from zones
    void* block = nullptr;
    if (size == paging::PAGE_SIZE) {
        block = find_free_order0_block(CALLER_TAG);
    }
    if (block == nullptr) {
        block = find_free_block(size, CALLER_TAG);
    }

    if (block == nullptr) {
        if (!log_oom) {
            return nullptr;
        }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        void* const OOM_CALLER_ADDR = caller_addr;
#else
        void* const OOM_CALLER_ADDR = caller_addr;
#endif
        // OOM condition - dump allocation info for debugging. Keep this path
        // allocation-free and serial-lock-free so the real allocator dump can
        // run even if the journal or serial lock is wedged.
        dump_page_alloc_oom_header(size, OOM_CALLER_ADDR, name);
        dump_page_allocations_oom();
        return nullptr;
    }

    note_physical_alloc(buddy_accounting_size(size));

    // Validate the returned address is in a reasonable HHDM range
    auto block_addr = reinterpret_cast<uint64_t>(block);
    constexpr uint64_t HHDM_BASE = 0xffff800000000000ULL;
    constexpr uint64_t HHDM_END = 0xffff808000000000ULL;  // ~512GB max physical
    if (block_addr < HHDM_BASE || block_addr >= HHDM_END) {
        dbg::emergency_log("FATAL: pageAlloc returned invalid HHDM addr: 0x%lx\n", block_addr);
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

    prepare_allocated_block(block, size, zeroing);

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    record_page_alloc_caller(caller_addr, NUM_PAGES);
#endif
    return block;
}

auto can_wait_for_reclaim() -> bool {
    // Reclaim wait loops eventually enter kern_yield(), which is only safe from
    // a normal task context. Exception handlers such as the COW page-fault path
    // arrive with IF cleared; yielding there can hand the CPU to the timer
    // interrupt and later attempt an iretq back into an in-flight fault frame.
    return sched::has_run_queues() && sched::preempt_count() == 0 && sched::interrupts_enabled();
}

auto page_alloc_with_reclaim_impl(uint64_t size, std::string_view name, ReturnedPageZeroing zeroing, void* caller_addr,
                                  uint32_t retry_count) -> void* {
    for (uint32_t attempt = 0; attempt < retry_count; ++attempt) {
        void* const PAGE = page_alloc_impl(size, name, zeroing, caller_addr, false);
        if (PAGE != nullptr) {
            return PAGE;
        }
        if (!can_wait_for_reclaim()) {
            break;
        }
        uint32_t const RECLAIMED = sched::reclaim_memory_pressure();
        if (RECLAIMED == 0) {
            sched::request_gc_memory_pressure();
            sched::kern_yield_impl(reinterpret_cast<uint64_t>(caller_addr));
        }
    }

    return page_alloc_impl(size, name, zeroing, caller_addr, true);
}

void dump_page_alloc_oom_header(uint64_t size, void* caller_addr, std::string_view name) {
    emergency_serial::write("OOM: pageAlloc failed for size 0x");
    emergency_serial::write_hex(size);
    emergency_serial::write(" bytes\nAllocation site: 0x");
    emergency_serial::write_hex(reinterpret_cast<uint64_t>(caller_addr));
    emergency_serial::write(" (");
    emergency_serial::write(name.data(), name.size());
    emergency_serial::write(")\n");
}

void write_hex_field(const char* label, uint64_t value) {
    emergency_serial::write(label);
    emergency_serial::write("0x");
    emergency_serial::write_hex(value);
}

}  // namespace

auto page_alloc(uint64_t size, std::string_view name) -> void* {
    return page_alloc_impl(size, name, ReturnedPageZeroing::ZERO, __builtin_return_address(0), true);
}

auto page_alloc_full_overwrite_page(std::string_view name) -> void* {
    return page_alloc_impl(paging::PAGE_SIZE, name, ReturnedPageZeroing::FULL_OVERWRITE, __builtin_return_address(0), true);
}

auto page_alloc_may_fail(uint64_t size, std::string_view name) -> void* {
    return page_alloc_impl(size, name, ReturnedPageZeroing::ZERO, __builtin_return_address(0), false);
}

auto page_alloc_full_overwrite_page_may_fail(std::string_view name) -> void* {
    return page_alloc_impl(paging::PAGE_SIZE, name, ReturnedPageZeroing::FULL_OVERWRITE, __builtin_return_address(0), false);
}

auto page_alloc_with_reclaim(uint64_t size, std::string_view name, uint32_t retry_count) -> void* {
    return page_alloc_with_reclaim_impl(size, name, ReturnedPageZeroing::ZERO, __builtin_return_address(0), retry_count);
}

auto page_alloc_full_overwrite_page_with_reclaim(std::string_view name, uint32_t retry_count) -> void* {
    return page_alloc_with_reclaim_impl(paging::PAGE_SIZE, name, ReturnedPageZeroing::FULL_OVERWRITE, __builtin_return_address(0),
                                        retry_count);
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
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLER_TAG = caller_stats_runtime_enabled.load(std::memory_order_relaxed) ? reinterpret_cast<uint64_t>(caller_addr) : 0;
#else
    uint64_t const CALLER_TAG = 0;
#endif
    void* block = find_free_block_huge(size, CALLER_TAG);

    if (block == nullptr) {
        return nullptr;
    }

    note_physical_alloc(buddy_accounting_size(size));

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
    // Try huge page zone first
    if (huge_page_zone != nullptr) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR >= huge_page_zone->start && PAGE_ADDR < huge_page_zone->start + huge_page_zone->len) {
            if (huge_page_zone->allocator != nullptr) {
                uint64_t const FLAGS = huge_page_zone->allocator->lock_irq();
                uint64_t const FREED_BYTES = huge_page_zone->allocator->free(page);
                huge_page_zone->allocator->unlock_irq(FLAGS);
                note_physical_free(FREED_BYTES);
            }
            return;
        }
    }

    auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
    paging::PageZone* zone = regular_zone_for_addr(PAGE_ADDR);
    if (zone == nullptr || zone->allocator == nullptr) {
        return;
    }

    if (try_cache_freed_order0_page(zone->allocator, page)) {
        return;
    }

    uint64_t const FLAGS = zone->allocator->lock_irq();
    uint64_t const FREED_BYTES = zone->allocator->free(page);
    zone->allocator->unlock_irq(FLAGS);
    note_physical_free(FREED_BYTES);
}

auto page_split_to_order0(void* page) -> bool {
    if (page == nullptr) {
        return false;
    }

    if (huge_page_zone != nullptr) {
        auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
        if (PAGE_ADDR >= huge_page_zone->start && PAGE_ADDR < huge_page_zone->start + huge_page_zone->len) {
            uint64_t const FLAGS = huge_page_zone->allocator != nullptr ? huge_page_zone->allocator->lock_irq() : 0;
            bool const OK = huge_page_zone->allocator != nullptr && huge_page_zone->allocator->split_allocated_block_to_order0(page);
            if (huge_page_zone->allocator != nullptr) {
                huge_page_zone->allocator->unlock_irq(FLAGS);
            }
            return OK;
        }
    }

    auto const PAGE_ADDR = reinterpret_cast<uint64_t>(page);
    paging::PageZone* zone = regular_zone_for_addr(PAGE_ADDR);
    if (zone == nullptr || zone->allocator == nullptr) {
        return false;
    }

    uint64_t const FLAGS = zone->allocator->lock_irq();
    bool const OK = zone->allocator->split_allocated_block_to_order0(page);
    zone->allocator->unlock_irq(FLAGS);
    return OK;
}

// --- Frame reference counting helpers ---

namespace {
// Find the PageAllocator and page index for a given HHDM pointer.
// Returns true if found, populating allocator and pageIdx.
auto allocator_owns_page(PageAllocator* alloc, uint64_t addr, uint32_t& out_idx) -> bool {
    if (alloc == nullptr) {
        return false;
    }

    uint64_t const ALLOC_START = alloc->base;
    uint64_t const ALLOC_BYTES = static_cast<uint64_t>(alloc->total_pages) * paging::PAGE_SIZE;
    uint64_t const ALLOC_END = ALLOC_START + ALLOC_BYTES;
    if (ALLOC_END < ALLOC_START || addr < ALLOC_START || addr >= ALLOC_END) {
        return false;
    }

    out_idx = static_cast<uint32_t>((addr - ALLOC_START) / paging::PAGE_SIZE);
    return out_idx < alloc->total_pages;
}

auto allocator_is_huge_zone(PageAllocator const* alloc) -> bool { return huge_page_zone != nullptr && huge_page_zone->allocator == alloc; }

auto allocator_owns_page_as(PageAllocator* alloc, PageLookupOwner owner, uint64_t addr, uint32_t& out_idx) -> bool {
    if (owner == PageLookupOwner::NONE || alloc == nullptr) {
        return false;
    }
    if (owner == PageLookupOwner::HUGE_ZONE) {
        if (!allocator_is_huge_zone(alloc)) {
            return false;
        }
        return addr >= huge_page_zone->start && addr < huge_page_zone->start + huge_page_zone->len &&
               allocator_owns_page(alloc, addr, out_idx);
    }
    if (allocator_is_huge_zone(alloc)) {
        return false;
    }
    return allocator_owns_page(alloc, addr, out_idx);
}

auto find_allocator_for_page_owned(void* page, PageAllocator*& out_alloc, uint32_t& out_idx, PageLookupOwner& out_owner) -> bool {
    auto addr = reinterpret_cast<uint64_t>(page);
    // Check regular zones first.
    paging::PageZone* zone = regular_zone_for_addr(addr);
    if (zone != nullptr && allocator_owns_page(zone->allocator, addr, out_idx)) {
        out_alloc = zone->allocator;
        out_owner = PageLookupOwner::REGULAR_ZONE;
        return true;
    }

    // Check huge page zone
    if (huge_page_zone != nullptr && addr >= huge_page_zone->start && addr < huge_page_zone->start + huge_page_zone->len &&
        allocator_owns_page(huge_page_zone->allocator, addr, out_idx)) {
        out_alloc = huge_page_zone->allocator;
        out_owner = PageLookupOwner::HUGE_ZONE;
        return true;
    }
    out_owner = PageLookupOwner::NONE;
    return false;
}

auto find_allocator_for_page(void* page, PageAllocator*& out_alloc, uint32_t& out_idx) -> bool {
    PageLookupOwner owner = PageLookupOwner::NONE;
    return find_allocator_for_page_owned(page, out_alloc, out_idx, owner);
}

auto find_allocator_for_page_with_hint(void* page, PageLookupHint& hint, PageAllocator*& out_alloc, uint32_t& out_idx) -> bool {
    auto const ADDR = reinterpret_cast<uint64_t>(page);
    if (allocator_owns_page_as(hint.allocator, hint.owner, ADDR, out_idx)) {
        out_alloc = hint.allocator;
        return true;
    }

    PageLookupOwner owner = PageLookupOwner::NONE;
    if (!find_allocator_for_page_owned(page, out_alloc, out_idx, owner)) {
        return false;
    }
    hint.allocator = out_alloc;
    hint.owner = owner;
    return true;
}

auto find_allocator_for_page_cached(void* page, PageLookupHint* hint, PageAllocator*& out_alloc, uint32_t& out_idx) -> bool {
    if (hint == nullptr) {
        return find_allocator_for_page(page, out_alloc, out_idx);
    }

    return find_allocator_for_page_with_hint(page, *hint, out_alloc, out_idx);
}
}  // namespace

auto page_mark_kind(void* page, PageKind kind) -> bool {
    if (page == nullptr) {
        return false;
    }

    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page(page, alloc, idx) || alloc == nullptr) {
        return false;
    }

    uint64_t const FLAGS = alloc->lock_irq();
    bool const OK = alloc->mark_allocated_block_kind(page, kind);
    alloc->unlock_irq(FLAGS);
    return OK;
}

auto page_kind_get(void* page) -> PageKind { return page_kind_get(page, nullptr); }

auto page_kind_get(void* page, PageLookupHint* hint) -> PageKind {
    if (page == nullptr) {
        return PageKind::UNKNOWN;
    }

    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page_cached(page, hint, alloc, idx) || alloc == nullptr) {
        return PageKind::UNKNOWN;
    }

    return alloc->kind_of(page);
}

void page_ref_inc(void* page) { page_ref_inc(page, nullptr); }

void page_ref_inc(void* page, PageLookupHint* hint) {
    if (page == nullptr) {
        return;
    }

    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page_cached(page, hint, alloc, idx)) {
        return;
    }

    page_ref_inc_ops.fetch_add(1, std::memory_order_relaxed);
    auto& refcount = alloc->page_refcounts[idx];
    uint32_t old_ref = refcount.load(std::memory_order_acquire);
    for (;;) {
        if (old_ref == 0) {
            log::critical("page_ref_inc on zero-ref page %p", page);
            hcf();
        }
        if (old_ref == UINT32_MAX) {
            log::critical("page_ref_inc overflow for page %p", page);
            hcf();
        }
        if (refcount.compare_exchange_weak(old_ref, old_ref + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
        page_ref_inc_cas_retries.fetch_add(1, std::memory_order_relaxed);
    }
}

void page_ref_add(void* page, uint64_t refs) { page_ref_add(page, refs, nullptr); }

void page_ref_add(void* page, uint64_t refs, PageLookupHint* hint) {
    if (page == nullptr || refs == 0) {
        return;
    }
    if (refs > UINT32_MAX) {
        log::critical("page_ref_add overflow for page %p refs=%llu", page, static_cast<unsigned long long>(refs));
        hcf();
    }

    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page_cached(page, hint, alloc, idx)) {
        return;
    }

    page_ref_add_ops.fetch_add(1, std::memory_order_relaxed);
    page_ref_add_refs.fetch_add(refs, std::memory_order_relaxed);
    auto& refcount = alloc->page_refcounts[idx];
    uint32_t old_ref = refcount.load(std::memory_order_acquire);
    auto const ADD_REFS = static_cast<uint32_t>(refs);
    for (;;) {
        if (old_ref == 0) {
            log::critical("page_ref_add on zero-ref page %p refs=%llu", page, static_cast<unsigned long long>(refs));
            hcf();
        }
        if (ADD_REFS > UINT32_MAX - old_ref) {
            log::critical("page_ref_add overflow for page %p refs=%llu", page, static_cast<unsigned long long>(refs));
            hcf();
        }
        if (refcount.compare_exchange_weak(old_ref, old_ref + ADD_REFS, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
        page_ref_add_cas_retries.fetch_add(1, std::memory_order_relaxed);
    }
}

namespace {

struct ZeroRefPage {
    void* page = nullptr;
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
};

auto page_ref_dec_atomic(void* page, uint32_t& new_ref, ZeroRefPage& zero_ref_page, PageLookupHint& hint) -> bool {
    zero_ref_page = {};
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page_with_hint(page, hint, alloc, idx)) {
        new_ref = 0;
        return false;
    }

    page_ref_dec_ops.fetch_add(1, std::memory_order_relaxed);
    auto& refcount = alloc->page_refcounts[idx];
    uint32_t old_ref = refcount.load(std::memory_order_acquire);
    while (old_ref > 0) {
        uint32_t const NEXT_REF = old_ref - 1;
        if (refcount.compare_exchange_weak(old_ref, NEXT_REF, std::memory_order_acq_rel, std::memory_order_acquire)) {
            new_ref = NEXT_REF;
            if (NEXT_REF == 0) {
                page_ref_dec_zero_candidates.fetch_add(1, std::memory_order_relaxed);
                zero_ref_page = ZeroRefPage{
                    .page = page,
                    .alloc = alloc,
                    .idx = idx,
                };
            }
            return true;
        }
        page_ref_dec_cas_retries.fetch_add(1, std::memory_order_relaxed);
    }

    new_ref = 0;
    return false;
}

auto zero_ref_page_kind(ZeroRefPage const& zero_ref_page) -> PageKind {
    if (zero_ref_page.alloc == nullptr || zero_ref_page.idx >= zero_ref_page.alloc->total_pages) {
        return PageKind::UNKNOWN;
    }
    return decode_page_kind(zero_ref_page.alloc->page_kinds[zero_ref_page.idx].load(std::memory_order_acquire));
}

void verify_zero_ref_page_is_releasable(ZeroRefPage const& zero_ref_page, const char* op_name) {
    constexpr uint32_t SLAB_MAGIC = 0x8CBEEFC8;
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
    constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;
    PageKind const KIND = zero_ref_page_kind(zero_ref_page);
    if (KIND == PageKind::SLAB) {
        log::critical("DETECT: %s freeing live slab page - UAF trap! virt=%p", op_name, zero_ref_page.page);
        hcf();
    }
    if (KIND == PageKind::MEDIUM) {
        log::critical("DETECT: %s freeing live kmalloc medium page - UAF trap! virt=%p", op_name, zero_ref_page.page);
        hcf();
    }
    if (KIND == PageKind::KMALLOC_LARGE) {
        log::critical("DETECT: %s freeing live kmalloc large page - UAF trap! virt=%p", op_name, zero_ref_page.page);
        hcf();
    }
    if (KIND != PageKind::UNKNOWN) {
        return;
    }

    void* const PAGE = zero_ref_page.page;
    if (*reinterpret_cast<const volatile uint32_t*>(PAGE) == SLAB_MAGIC) {
        log::critical("DETECT: %s freeing live slab page - UAF trap! virt=%p", op_name, PAGE);
        hcf();
    }
    uint64_t const ALLOC_MAGIC = *reinterpret_cast<const volatile uint64_t*>(reinterpret_cast<uint64_t>(PAGE) + 16);
    if (ALLOC_MAGIC == MEDIUM_ALLOC_MAGIC) {
        log::critical("DETECT: %s freeing live kmalloc medium page - UAF trap! virt=%p", op_name, PAGE);
        hcf();
    }
    if (ALLOC_MAGIC == LARGE_ALLOC_MAGIC) {
        log::critical("DETECT: %s freeing live kmalloc large page - UAF trap! virt=%p", op_name, PAGE);
        hcf();
    }
}

auto zero_ref_page_is_valid_locked(ZeroRefPage const& zero_ref_page) -> bool {
    if (zero_ref_page.page == nullptr || zero_ref_page.alloc == nullptr || zero_ref_page.idx >= zero_ref_page.alloc->total_pages) {
        return false;
    }

    auto* const EXPECTED_PAGE =
        reinterpret_cast<void*>(zero_ref_page.alloc->base + (static_cast<uint64_t>(zero_ref_page.idx) * paging::PAGE_SIZE));
    if (EXPECTED_PAGE != zero_ref_page.page) {
        return false;
    }

    if (zero_ref_page.alloc->page_refcounts[zero_ref_page.idx].load(std::memory_order_acquire) != 0) {
        return false;
    }

    return true;
}

auto free_zero_ref_page_locked(ZeroRefPage const& zero_ref_page, uint64_t& freed_bytes) -> bool {
    if (!zero_ref_page_is_valid_locked(zero_ref_page)) {
        freed_bytes = 0;
        return false;
    }

    // Refcount teardown frees only independently freeable order-0 leaves.
    // Multi-page buddy allocations must be split before any PTE can expose
    // their pages to leaf-by-leaf reclaim.
    freed_bytes = zero_ref_page.alloc->free_order0_at(zero_ref_page.idx);
    return freed_bytes != 0;
}

auto count_contiguous_zero_ref_run_locked(std::span<ZeroRefPage const> pages, size_t start) -> size_t {
    if (start >= pages.size() || !zero_ref_page_is_valid_locked(pages[start])) {
        return 0;
    }

    PageAllocator* const ALLOC = pages[start].alloc;
    uint32_t expected_idx = pages[start].idx;
    size_t count = 0;
    while (start + count < pages.size()) {
        ZeroRefPage const& page = pages[start + count];
        if (page.alloc != ALLOC || page.idx != expected_idx || !zero_ref_page_is_valid_locked(page)) {
            break;
        }
        expected_idx++;
        count++;
    }
    return count;
}

void note_zero_ref_pages_freed(uint64_t freed_bytes, uint64_t release_operations, PageRefBatchStats& stats, bool batch_mode) {
    if (freed_bytes == 0) {
        return;
    }

    uint64_t const PAGES_FREED = freed_bytes / paging::PAGE_SIZE;
    note_physical_free(freed_bytes, release_operations);
    stats.pages_freed += PAGES_FREED;
    page_ref_dec_zero_pages_freed.fetch_add(PAGES_FREED, std::memory_order_relaxed);
    if (batch_mode) {
        page_ref_batch_free_runs.fetch_add(1, std::memory_order_relaxed);
        page_ref_batch_pages_freed.fetch_add(PAGES_FREED, std::memory_order_relaxed);
    }
}

void free_zero_ref_pages(std::span<ZeroRefPage const> pages, PageRefBatchStats& stats, bool batch_mode) {
    if (pages.empty()) {
        return;
    }

    size_t i = 0;
    while (i < pages.size()) {
        PageAllocator* const ALLOC = pages[i].alloc;
        if (ALLOC == nullptr) {
            i++;
            continue;
        }

        size_t group_end = i + 1;
        while (group_end < pages.size() && pages[group_end].alloc == ALLOC) {
            group_end++;
        }

        uint64_t const FLAGS = ALLOC->lock_irq();
        size_t group_i = i;
        while (group_i < group_end) {
            size_t const RUN_COUNT = count_contiguous_zero_ref_run_locked(pages, group_i);
            if (RUN_COUNT == 0) {
                page_ref_dec_zero_validation_failed.fetch_add(1, std::memory_order_relaxed);
                group_i++;
                continue;
            }

            ZeroRefPage const& first_page = pages[group_i];
            uint64_t const FREED_BYTES = first_page.alloc->free_order0_range_at(first_page.idx, static_cast<uint32_t>(RUN_COUNT));
            if (FREED_BYTES != 0) {
                note_zero_ref_pages_freed(FREED_BYTES, RUN_COUNT, stats, batch_mode);
                group_i += RUN_COUNT;
                continue;
            }

            for (size_t j = 0; j < RUN_COUNT; ++j) {
                uint64_t freed_bytes = 0;
                if (free_zero_ref_page_locked(pages[group_i + j], freed_bytes)) {
                    note_zero_ref_pages_freed(freed_bytes, 1, stats, batch_mode);
                } else {
                    page_ref_dec_zero_validation_failed.fetch_add(1, std::memory_order_relaxed);
                }
            }
            group_i += RUN_COUNT;
        }
        ALLOC->unlock_irq(FLAGS);
        i = group_end;
    }
}

}  // namespace

auto page_ref_dec(void* page) -> uint32_t { return page_ref_dec(page, nullptr); }

auto page_ref_dec(void* page, PageLookupHint* hint) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }

    uint32_t new_ref = 0;
    ZeroRefPage zero_ref_page{};
    PageLookupHint lookup_hint = hint != nullptr ? *hint : PageLookupHint{};
    bool const DEC_OK = page_ref_dec_atomic(page, new_ref, zero_ref_page, lookup_hint);
    if (hint != nullptr) {
        *hint = lookup_hint;
    }
    if (!DEC_OK) {
        return new_ref;
    }
    if (zero_ref_page.page == nullptr) {
        return new_ref;
    }

    verify_zero_ref_page_is_releasable(zero_ref_page, "page_ref_dec");
    PageRefBatchStats stats{};
    free_zero_ref_pages(std::span<ZeroRefPage const>{&zero_ref_page, 1}, stats, false);
    return new_ref;
}

auto page_ref_dec_batch(std::span<void* const> pages) -> PageRefBatchStats { return page_ref_dec_batch(pages, nullptr); }

auto page_ref_dec_batch(std::span<void* const> pages, PageLookupHint* hint) -> PageRefBatchStats {
    PageRefBatchStats stats{};
    page_ref_batch_calls.fetch_add(1, std::memory_order_relaxed);
    page_ref_batch_pages.fetch_add(pages.size(), std::memory_order_relaxed);
    if (pages.empty()) {
        return stats;
    }

    constexpr size_t ZERO_REF_BATCH_CAP = 128;
    std::array<ZeroRefPage, ZERO_REF_BATCH_CAP> zero_ref_pages{};
    size_t zero_ref_count = 0;

    auto flush_zero_ref_pages = [&]() {
        std::sort(zero_ref_pages.begin(), zero_ref_pages.begin() + static_cast<ptrdiff_t>(zero_ref_count),
                  [](ZeroRefPage const& lhs, ZeroRefPage const& rhs) {
                      auto const LHS_ALLOC = reinterpret_cast<uintptr_t>(lhs.alloc);
                      auto const RHS_ALLOC = reinterpret_cast<uintptr_t>(rhs.alloc);
                      if (LHS_ALLOC != RHS_ALLOC) {
                          return LHS_ALLOC < RHS_ALLOC;
                      }
                      return lhs.idx < rhs.idx;
                  });
        free_zero_ref_pages(std::span<ZeroRefPage const>{zero_ref_pages.data(), zero_ref_count}, stats, true);
        zero_ref_count = 0;
    };

    PageLookupHint lookup_hint = hint != nullptr ? *hint : PageLookupHint{};
    for (void* page : pages) {
        if (page == nullptr) {
            continue;
        }

        uint32_t new_ref = 0;
        ZeroRefPage zero_ref_page{};
        if (!page_ref_dec_atomic(page, new_ref, zero_ref_page, lookup_hint)) {
            continue;
        }

        stats.refs_decremented++;
        if (zero_ref_page.page == nullptr) {
            continue;
        }
        page_ref_batch_zero_candidates.fetch_add(1, std::memory_order_relaxed);
        verify_zero_ref_page_is_releasable(zero_ref_page, "page_ref_dec_batch");
        zero_ref_pages[zero_ref_count++] = zero_ref_page;
        if (zero_ref_count >= zero_ref_pages.size()) {
            flush_zero_ref_pages();
        }
    }
    flush_zero_ref_pages();
    if (hint != nullptr) {
        *hint = lookup_hint;
    }
    return stats;
}

void get_page_ref_stats_snapshot(PageRefStatsSnapshot& out) {
    out = PageRefStatsSnapshot{
        .inc_ops = page_ref_inc_ops.load(std::memory_order_relaxed),
        .inc_cas_retries = page_ref_inc_cas_retries.load(std::memory_order_relaxed),
        .add_ops = page_ref_add_ops.load(std::memory_order_relaxed),
        .add_refs = page_ref_add_refs.load(std::memory_order_relaxed),
        .add_cas_retries = page_ref_add_cas_retries.load(std::memory_order_relaxed),
        .dec_ops = page_ref_dec_ops.load(std::memory_order_relaxed),
        .dec_cas_retries = page_ref_dec_cas_retries.load(std::memory_order_relaxed),
        .dec_zero_candidates = page_ref_dec_zero_candidates.load(std::memory_order_relaxed),
        .dec_zero_pages_freed = page_ref_dec_zero_pages_freed.load(std::memory_order_relaxed),
        .dec_zero_validation_failed = page_ref_dec_zero_validation_failed.load(std::memory_order_relaxed),
        .batch_calls = page_ref_batch_calls.load(std::memory_order_relaxed),
        .batch_pages = page_ref_batch_pages.load(std::memory_order_relaxed),
        .batch_zero_candidates = page_ref_batch_zero_candidates.load(std::memory_order_relaxed),
        .batch_free_runs = page_ref_batch_free_runs.load(std::memory_order_relaxed),
        .batch_pages_freed = page_ref_batch_pages_freed.load(std::memory_order_relaxed),
    };
}

void get_page_cache_stats_snapshot(PageCacheStatsSnapshot& out) {
    out = PageCacheStatsSnapshot{
        .enabled = USE_PER_CPU_PAGE_CACHE ? 1U : 0U,
        .capacity = per_cpu_caches != nullptr ? static_cast<uint64_t>(num_cpus * PerCpuPageCache::CACHE_SIZE) : 0U,
        .cached_pages = total_cached_order0_pages_snapshot(),
        .alloc_hits = page_cache_alloc_hits.load(std::memory_order_relaxed),
        .alloc_misses = page_cache_alloc_misses.load(std::memory_order_relaxed),
        .refill_calls = page_cache_refill_calls.load(std::memory_order_relaxed),
        .refill_pages = page_cache_refill_pages.load(std::memory_order_relaxed),
        .free_hits = page_cache_free_hits.load(std::memory_order_relaxed),
        .free_misses = page_cache_free_misses.load(std::memory_order_relaxed),
        .drain_calls = page_cache_drain_calls.load(std::memory_order_relaxed),
        .drain_pages = page_cache_drain_pages.load(std::memory_order_relaxed),
        .stale_entries = page_cache_stale_entries.load(std::memory_order_relaxed),
    };
}

auto page_ref_get(void* page) -> uint32_t { return page_ref_get(page, nullptr); }

auto page_ref_get(void* page, PageLookupHint* hint) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }

    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    if (!find_allocator_for_page_cached(page, hint, alloc, idx)) {
        return 0;
    }
    return alloc->page_refcounts[idx].load(std::memory_order_acquire);
}

void dump_mini_malloc_stats() { mini_malloc::mini_dump_stats(); }

void dump_kmalloc_tracked_allocs() { ker::mod::mm::dyn::kmalloc::dump_tracked_allocations(); }

}  // namespace ker::mod::mm::phys
