#pragma once

#include <limine.h>
#include <mod/mm/addr.hpp>
#include <mod/acpi/acpi.hpp>
#include <mod/mm/phys.hpp>
#include <mod/mm/paging.hpp>
#include <mod/sched/task.hpp>
#include <mod/asm/tlb.hpp>

namespace ker::mod::mm::virt {
    using namespace ker::mod::mm::addr;
    using namespace ker::mod::mm::paging;

    struct Range
    {
        uint64_t start;
        uint64_t end;
    };
    
    void init(
        limine_memmap_response *memmapResponse,
        limine_kernel_file_response* kernelFileResponse,
        limine_kernel_address_response* kernelAddressResponse
    );

    void mapPage(PageTable *pageTable, vaddr_t vaddr, paddr_t paddr, int flags);
    void mapRange(PageTable *pageTable, Range range, int flags, uint64_t offset = 0);
    void unmapPage(PageTable *pageTable, vaddr_t vaddr);

    void switchToKernelPagemap(void);
    PageTable* createPagemap(void);
    void copyKernelMappings(sched::task::Task t);
    void switchPagemap(sched::task::Task t);
    void pagefaultHandler(uint64_t controlRegister, int errCode);
    paddr_t translate(PageTable* pageTable, vaddr_t vaddr);
}