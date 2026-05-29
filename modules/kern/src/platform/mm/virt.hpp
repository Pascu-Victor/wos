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
void init_pagemap();
PageTable* create_pagemap();
void copy_kernel_mappings(sched::task::Task* t);
void switch_pagemap(sched::task::Task* t);
bool pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& gpr);
void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset);
// assume hhdm as offset
void map_range_to_kernel_page_table(Range range, uint64_t flags);
// Sentinel returned by translate() when the virtual address is not mapped.
// Using (paddr_t)-1 so physical address 0 remains valid.
static constexpr paddr_t PADDR_INVALID = static_cast<paddr_t>(-1);

paddr_t translate(PageTable* page_table, vaddr_t vaddr);
auto collect_user_memory_stats(PageTable* page_table) -> UserMemoryStats;

// Free all user-space pages and page tables in a pagemap
// Only frees the lower half (user space), keeps kernel mappings intact
// After calling this, the pagemap itself should be freed with phys::page_free
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
