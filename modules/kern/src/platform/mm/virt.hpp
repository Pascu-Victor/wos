#pragma once

#include <extern/limine.h>

#include <cstdint>
#include <platform/acpi/acpi.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/task.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"

namespace ker::mod::mm::virt {
using namespace ker::mod::mm::addr;
using namespace ker::mod::mm::paging;

struct Range {
    uint64_t start;
    uint64_t end;
};

struct PageMapBatch {
    PageTable* root{};
    PageTable* pml3{};
    PageTable* pml2{};
    PageTable* pml1{};
    uint64_t cached_idx4 = UINT64_MAX;
    uint64_t cached_idx3 = UINT64_MAX;
    uint64_t cached_idx2 = UINT64_MAX;
    uint64_t table_flags{};
    bool dirty{};
};

struct UserMemoryStats {
    uint64_t virtual_pages;
    uint64_t resident_pages;
    uint64_t shared_pages;
    uint64_t page_table_pages;
};

struct DestroyUserSpaceStats {
    uint64_t calls;
    uint64_t collect_frames_us_total;
    uint64_t collect_frames_us_max;
    uint64_t free_data_us_total;
    uint64_t free_data_us_max;
    uint64_t free_pt_us_total;
    uint64_t free_pt_us_max;
    uint64_t tlb_flush_us_total;
    uint64_t tlb_flush_us_max;
    uint64_t data_leaf_entries_visited;
    uint64_t data_pages_ref_decremented;
    uint64_t data_pages_freed;
    uint64_t page_table_pages_ref_decremented;
    uint64_t page_table_pages_freed;
    uint64_t skipped_huge_pages;
    uint64_t skipped_unknown_frames;
    uint64_t skipped_slab_alloc_frames;
    uint64_t skipped_medium_alloc_frames;
    uint64_t skipped_kmalloc_large_alloc_frames;
    uint64_t skipped_page_table_aliases;
    uint64_t skipped_corrupt_entries;
    uint64_t magic_unknown_probe_reads;
    uint64_t magic_unknown_slab_hits;
    uint64_t magic_unknown_medium_hits;
    uint64_t magic_unknown_kmalloc_large_hits;
};

struct PageTablePoolStatsSnapshot {
    uint64_t capacity;
    uint64_t cached_pages;
    uint64_t alloc_hits;
    uint64_t alloc_misses;
    uint64_t releases;
    uint64_t rejects;
};

struct OwnedFrameStatsSnapshot {
    uint64_t capacity;
    uint64_t entries;
    uint64_t track_attempts;
    uint64_t track_added;
    uint64_t track_replaced;
    uint64_t track_skipped;
    uint64_t track_conflicts;
    uint64_t track_probe_failures;
    uint64_t untrack_attempts;
    uint64_t untrack_removed;
    uint64_t untrack_missed;
    uint64_t purge_calls;
    uint64_t purge_removed;
};

struct DestroyUserSpaceBudgetState;

void init(limine_memmap_response* memmap_response, limine_executable_file_response* kernel_file_response,
          limine_executable_address_response* kernel_address_response);

static inline auto get_kernel_page_table() -> PageTable* { return reinterpret_cast<PageTable*>(rdcr3()); }

void map_page(PageTable* page_table, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void init_page_map_batch(PageMapBatch* batch, PageTable* page_table, uint64_t flags);
void map_page_batched(PageMapBatch* batch, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void flush_page_map_batch(PageMapBatch* batch);
void map_same_page_range(PageTable* page_table, vaddr_t vaddr, paddr_t paddr, uint64_t page_count, uint64_t flags);
void reserve_page_range(PageTable* page_table, vaddr_t vaddr, uint64_t page_count);
bool is_page_mapped(PageTable* page_table, vaddr_t vaddr);
bool is_page_reserved(PageTable* page_table, vaddr_t vaddr);
bool is_page_mapped_or_reserved(PageTable* page_table, vaddr_t vaddr);
void unify_page_flags(PageTable* page_table, vaddr_t vaddr, uint64_t flags);
void map_range(PageTable* page_table, Range range, uint64_t flags, uint64_t offset = 0);
void unmap_page(PageTable* page_table, vaddr_t vaddr);

void switch_to_kernel_pagemap();
PageTable* get_kernel_pagemap();
void init_tlb_shootdown();
void note_tlb_shootdown_cpu_online();
void service_pending_tlb_shootdowns();
void init_pagemap();
PageTable* create_pagemap();
void release_pagemap(PageTable* pagemap);
void get_page_table_pool_stats_snapshot(PageTablePoolStatsSnapshot& out);
void get_owned_frame_stats_snapshot(OwnedFrameStatsSnapshot& out);
void copy_kernel_mappings(sched::task::Task* t);
void switch_pagemap(sched::task::Task* t);
bool pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& gpr);
void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset);
// assume hhdm as offset
void map_range_to_kernel_page_table(Range range, uint64_t flags);
[[nodiscard]] bool active_pagemap_is(PageTable* pagemap);
[[nodiscard]] bool user_page_mapped_now(PageTable* pagemap, vaddr_t vaddr);
[[nodiscard]] bool user_page_writable_now(PageTable* pagemap, vaddr_t vaddr);
// Sentinel returned by translate() when the virtual address is not mapped.
// Using (paddr_t)-1 so physical address 0 remains valid.
static constexpr paddr_t PADDR_INVALID = static_cast<paddr_t>(-1);

paddr_t translate(PageTable* page_table, vaddr_t vaddr);
bool ensure_user_page_writable(sched::task::Task* task, vaddr_t vaddr);
bool ensure_user_page_mapped(sched::task::Task* task, vaddr_t vaddr);
auto collect_user_memory_stats(PageTable* page_table) -> UserMemoryStats;
auto get_destroy_user_space_stats(uint64_t cpu_no) -> DestroyUserSpaceStats;
auto create_destroy_user_space_budget_state(PageTable* pagemap, uint64_t owner_pid = 0, const char* owner_name = nullptr,
                                            const char* reason = nullptr) -> DestroyUserSpaceBudgetState*;
auto destroy_user_space_budgeted(DestroyUserSpaceBudgetState* state, uint32_t max_steps) -> bool;
void destroy_user_space_budget_state_destroy(DestroyUserSpaceBudgetState* state);

// Free all user-space pages and page tables in a pagemap
// Only frees the lower half (user space), keeps kernel mappings intact
// After calling this, the root pagemap itself should be released with release_pagemap().
void destroy_user_space(PageTable* pagemap, uint64_t owner_pid = 0, const char* owner_name = nullptr, const char* reason = nullptr);

// Deep-copy user-space page tables from src to dst using COW.
// Both src and dst PML1 entries are marked read-only + COW bit.
// Physical data page refcounts are incremented.
// Page table structures (PML3/PML2/PML1) are freshly allocated for dst.
// Returns true on success, false on OOM.
bool deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst);

// Debug helper: log active/dead user mappings that still reference a physical page.
// Intended for targeted fault-path diagnostics only.
bool debug_log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid = 0, const char* owner_name = nullptr,
                                  bool log_when_empty = true);
}  // namespace ker::mod::mm::virt
