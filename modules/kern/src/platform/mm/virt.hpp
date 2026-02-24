#pragma once

#include <limine.h>

#include <platform/acpi/acpi.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/task.hpp>

#include "platform/interrupt/gates.hpp"

namespace ker::mod::mm::virt {
using namespace ker::mod::mm::addr;
using namespace ker::mod::mm::paging;

struct Range {
    uint64_t start;
    uint64_t end;
};

void init(limine_memmap_response* memmapResponse, limine_kernel_file_response* kernelFileResponse,
          limine_kernel_address_response* kernelAddressResponse);

static inline auto getKernelPageTable() -> PageTable* { return (PageTable*)rdcr3(); }

void mapPage(PageTable* pageTable, vaddr_t vaddr, paddr_t paddr, uint64_t flags);
bool isPageMapped(PageTable* pageTable, vaddr_t vaddr);
void unifyPageFlags(PageTable* pageTable, vaddr_t vaddr, uint64_t flags);
void mapRange(PageTable* pageTable, Range range, uint64_t flags, uint64_t offset = 0);
void unmapPage(PageTable* pageTable, vaddr_t vaddr);

void switchToKernelPagemap(void);
PageTable* getKernelPagemap(void);
void initPagemap(void);
PageTable* createPagemap(void);
void copyKernelMappings(sched::task::Task* t);
void switchPagemap(sched::task::Task* t);
bool pagefault_handler(uint64_t control_register, gates::interruptFrame& frame, ker::mod::cpu::GPRegs& gpr);
void mapToKernelPageTable(vaddr_t vaddr, paddr_t paddr, uint64_t flags);
void mapRangeToKernelPageTable(Range range, uint64_t flags, uint64_t offset);
// assume hhdm as offset
void mapRangeToKernelPageTable(Range range, uint64_t flags);
paddr_t translate(PageTable* pageTable, vaddr_t vaddr);

// Free all user-space pages and page tables in a pagemap
// Only frees the lower half (user space), keeps kernel mappings intact
// After calling this, the pagemap itself should be freed with phys::pageFree
void destroyUserSpace(PageTable* pagemap);

// Deep-copy user-space page tables from src to dst using COW.
// Both src and dst PML1 entries are marked read-only + COW bit.
// Physical data page refcounts are incremented.
// Page table structures (PML3/PML2/PML1) are freshly allocated for dst.
// Returns true on success, false on OOM.
bool deepCopyUserPagemapCOW(PageTable* src, PageTable* dst);
}  // namespace ker::mod::mm::virt
