#pragma once

#include <extern/limine.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/hcf.hpp>

namespace ker::mod::mm::phys {
constexpr size_t MEMACC_BUDDY_ORDER_COUNT = 21;

struct AllocStatsSnapshot {
    uint64_t total_allocated_bytes;
    uint64_t total_freed_bytes;
    uint64_t live_allocated_bytes;
    uint64_t alloc_count;
    uint64_t free_count;
    uint64_t total_mem_bytes;
    uint64_t free_mem_bytes;
};

struct ZoneSnapshot {
    uint64_t zone_num;
    uint64_t start;
    uint64_t len;
    uint64_t page_count;
    uint64_t total_pages;
    uint64_t usable_pages;
    uint64_t free_pages;
    uint64_t metadata_pages;
    uint64_t scanned_free_pages;
    std::array<uint32_t, MEMACC_BUDDY_ORDER_COUNT> buddy_order_counts;
    const char* name;
    bool has_allocator;
    bool invalid_allocator;
    bool bad_order;
    bool free_count_mismatch;
};

struct CallerPageStat {
    uint64_t caller;
    uint64_t pages;
};

void init(limine_memmap_response* memmap_response);
void set_kernel_cr3(uint64_t cr3);    // Call after initPagemap to set kernel CR3 for safe memset
void init_huge_page_zone_deferred();  // Call after initPagemap to initialize huge page zone
void enable_per_cpu_allocations();    // Call after cpuParamInit to enable per-CPU page caches
auto page_alloc(uint64_t size = ker::mod::mm::paging::PAGE_SIZE, std::string_view name = "anonymous") -> void*;
auto page_alloc_huge(uint64_t size) -> void*;  // Try the optional huge page zone, if enabled.
void page_free(void* page);
auto page_split_to_order0(void* page) -> bool;
auto page_alloc_can_satisfy(uint64_t size, uint64_t reserve_bytes = 0) -> bool;

// --- Frame reference counting (for COW fork) ---
// Increment the refcount for a physical page (HHDM pointer).
// Pages start at refcount 1 after pageAlloc().
void page_ref_inc(void* page);
void page_ref_add(void* page, uint64_t refs);
// Decrement the refcount. When it reaches 0 the page is freed.
// Returns the new refcount (0 = freed).
uint32_t page_ref_dec(void* page);
// Get current refcount for a physical page. Returns 0 for unknown/free pages.
uint32_t page_ref_get(void* page);

// Get the head of the memory zones list (for OOM diagnostics)
auto get_zones() -> paging::PageZone*;
auto get_huge_page_zone() -> paging::PageZone*;

// Dump page allocation status when OOM - uses NO dynamic allocations
// Call this when out of memory to get diagnostic information
void dump_page_allocations_oom();

// Additional OOM helpers
void dump_mini_malloc_stats();
void dump_kmalloc_tracked_allocs();
void dump_alloc_stats();        // Dump allocation/free counters for debugging
void dump_caller_page_stats();  // Dump per-call-site histogram when WOS_PHYS_ALLOC_CALLER_STATS is enabled
void enable_stack_overlap_check();

auto get_free_mem_bytes() -> uint64_t;
auto get_total_mem_bytes() -> uint64_t;

void get_alloc_stats_snapshot(AllocStatsSnapshot& out);
auto snapshot_zones(ZoneSnapshot* out, size_t max_rows) -> size_t;

auto page_caller_stats_available() -> bool;
auto page_caller_stats_enabled() -> bool;
void page_caller_stats_set_enabled(bool enabled);
void page_caller_stats_reset();
auto page_caller_stats_generation() -> uint64_t;
auto page_caller_stats_default_enabled() -> bool;
auto snapshot_page_caller_stats(CallerPageStat* out, size_t max_rows, size_t& total_rows) -> bool;

template <typename T>
inline static void page_free(T* page) {
    page_free(static_cast<void*>(page));
}
}  // namespace ker::mod::mm::phys
