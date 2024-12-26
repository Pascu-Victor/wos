#pragma once

#include <limine.h>

#include <platform/acpi/acpi.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::mm::virt {
using namespace ker::mod::mm::addr;
using namespace ker::mod::mm::paging;

struct Range {
    uint64_t start;
    uint64_t end;
};

void init(limine_memmap_response* memmapResponse, limine_kernel_file_response* kernelFileResponse,
          limine_kernel_address_response* kernelAddressResponse);

static inline PageTable* getKernelPageTable() { return (PageTable*)rdcr3(); }

void mapPage(PageTable* pageTable, vaddr_t vaddr, paddr_t paddr, int flags);
bool isPageMapped(PageTable* pageTable, vaddr_t vaddr);
void unifyPageFlags(PageTable* pageTable, vaddr_t vaddr, uint64_t flags);
void mapRange(PageTable* pageTable, Range range, int flags, uint64_t offset = 0);
void unmapPage(PageTable* pageTable, vaddr_t vaddr);

void switchToKernelPagemap(void);
void initPagemap(void);
PageTable* createPagemap(void);
void copyKernelMappings(sched::task::Task* t);
void switchPagemap(sched::task::Task* t);
void pagefaultHandler(uint64_t controlRegister, int errCode);
void mapToKernelPageTable(vaddr_t vaddr, paddr_t paddr, int flags);
void mapRangeToKernelPageTable(Range range, int flags, uint64_t offset);
// assume hhdm as offset
void mapRangeToKernelPageTable(Range range, int flags);
paddr_t translate(PageTable* pageTable, vaddr_t vaddr);
}  // namespace ker::mod::mm::virt
