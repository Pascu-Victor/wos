#include "virt.hpp"

#include <platform/dbg/dbg.hpp>
namespace ker::mod::mm::virt {
static paging::PageTable* kernelPagemap;

static limine_memmap_response* memmapResponse;
static limine_kernel_file_response* kernelFileResponse;
static limine_kernel_address_response* kernelAddressResponse;

void init(limine_memmap_response* _memmapResponse, limine_kernel_file_response* _kernelFileResponse,
          limine_kernel_address_response* _kernelAddressResponse) {
    memmapResponse = _memmapResponse;
    kernelFileResponse = _kernelFileResponse;
    kernelAddressResponse = _kernelAddressResponse;
}

void switchToKernelPagemap() { wrcr3((uint64_t)addr::getPhysPointer((paddr_t)kernelPagemap)); }

PageTable* createPagemap() { return (PageTable*)phys::pageAlloc(); }

void copyKernelMappings(sched::task::Task* t) {
    for (size_t i = 256; i < 512; i++) {
        t->pagemap->entries[i] = kernelPagemap->entries[i];
    }
}

void switchPagemap(sched::task::Task* t) {
    if (!t->pagemap) {
        // PANIC!
        hcf();
    }

    wrcr3((uint64_t)addr::getPhysPointer((paddr_t)t->pagemap));
    // wrcr3((uint64_t)addr::getPhysPointer((paddr_t)t->pagemap));
}

void pagefaultHandler(uint64_t controlRegister, int errCode) {
    PageFault pagefault = paging::createPageFault(errCode, true);

    // if (pagefault.present) {
    //     // PANIC!
    //     hcf();
    // }

    mapPage((mm::paging::PageTable*)mm::addr::getVirtPointer((uint64_t)getKernelPageTable()), controlRegister, controlRegister,
            pagefault.flags);
}

static inline uint64_t index_of(const uint64_t vaddr, const int offset) { return vaddr >> (12 + 9 * (offset - 1)) & 0x1FF; }

paddr_t translate(PageTable* pageTable, vaddr_t vaddr) {
    if (!pageTable) {
        // PANIC!
        hcf();
    }

    PageTable* table = pageTable;
    for (int i = 4; i > 1; i--) {
        table = (PageTable*)addr::getVirtPointer(table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT);
        if (!table) {
            return 0;
        }
    }

    return (paddr_t)addr::getVirtPointer((table->entries[index_of(vaddr, 1)].frame << paging::PAGE_SHIFT) +
                                         (vaddr & (paging::PAGE_SIZE - 1)));
}

void initPagemap() {
    cpu::enablePAE();
    cpu::enablePSE();
    kernelPagemap = (PageTable*)phys::pageAlloc();
    if (kernelPagemap == nullptr) {
        // PANIC!
        hcf();
    }
    dbg::log("Kernel pagemap allocated at %x\n", kernelPagemap);
    for (size_t i = 0; i < memmapResponse->entry_count; i++) {
        char typeBuf[32];
        switch (memmapResponse->entries[i]->type) {
            case LIMINE_MEMMAP_USABLE:
                std::strncpy(typeBuf, "USABLE", 32);
                break;
            case LIMINE_MEMMAP_RESERVED:
                std::strncpy(typeBuf, "RESERVED", 32);
                break;
            case LIMINE_MEMMAP_ACPI_RECLAIMABLE:
                std::strncpy(typeBuf, "ACPI_RECLAIMABLE", 32);
                break;
            case LIMINE_MEMMAP_ACPI_NVS:
                std::strncpy(typeBuf, "ACPI_NVS", 32);
                break;
            case LIMINE_MEMMAP_BAD_MEMORY:
                std::strncpy(typeBuf, "BAD_MEMORY", 32);
                break;
            case LIMINE_MEMMAP_BOOTLOADER_RECLAIMABLE:
                std::strncpy(typeBuf, "BOOTLOADER_RECLAIMABLE", 32);
                break;
            case LIMINE_MEMMAP_KERNEL_AND_MODULES:
                std::strncpy(typeBuf, "KERNEL_AND_MODULES", 32);
                break;
            case LIMINE_MEMMAP_FRAMEBUFFER:
                std::strncpy(typeBuf, "FRAMEBUFFER", 32);
                break;
            default:
                std::strncpy(typeBuf, "UNKNOWN", 32);
                break;
        }
        dbg::log("Memory map entry %d: %x - %x (%s)", i, memmapResponse->entries[i]->base,
                 memmapResponse->entries[i]->base + memmapResponse->entries[i]->length, typeBuf);
        memset(typeBuf, 0, 32);
    }

    for (size_t i = 0; i < memmapResponse->entry_count; i++) {
        auto entry = memmapResponse->entries[i];
        for (size_t j = 0; j < entry->length / paging::PAGE_SIZE; j++) {
            paddr_t vaddr = (paddr_t)addr::getVirtPointer(entry->base + j * paging::PAGE_SIZE);
            mapPage(kernelPagemap,
                    vaddr,                                                      // virtual address
                    entry->base + j * paging::PAGE_SIZE,                        // physical address
                    entry->type == LIMINE_MEMMAP_RESERVED                       // reserved memory
                            || entry->type == LIMINE_MEMMAP_BAD_MEMORY          // bad memory
                            || entry->type == LIMINE_MEMMAP_KERNEL_AND_MODULES  // kernel and modules
                        ? paging::pageTypes::READONLY                           // becomes read-only
                        : paging::pageTypes::KERNEL                             // otherwise kernel memory
            );
        }
    }

    for (size_t i = 0; i <= kernelFileResponse->kernel_file->size; i++) {
        vaddr_t vaddr = kernelAddressResponse->virtual_base + i * paging::PAGE_SIZE;
        mapPage(kernelPagemap,
                vaddr,                                                         // virtual address
                kernelAddressResponse->physical_base + i * paging::PAGE_SIZE,  // physical address
                paging::pageTypes::KERNEL                                      // kernel memory
        );
    }

    switchToKernelPagemap();
}

// advance page table by level pages
static paging::PageTable* advancePageTable(paging::PageTable* pageTable, size_t level, uint64_t flags) {
    PageTableEntry entry = pageTable->entries[level];
    if (entry.present) {
        if (flags & paging::PAGE_WRITE && !entry.writable) {
            entry.writable = 1;
            pageTable->entries[level] = entry;
        }
        if (flags & paging::PAGE_USER && !entry.user) {
            entry.user = 1;
            pageTable->entries[level] = entry;
        }

        return (PageTable*)addr::getVirtPointer(entry.frame << paging::PAGE_SHIFT);
    }

    paddr_t addr = (paddr_t)addr::getPhysPointer((vaddr_t)phys::pageAlloc());
    if (addr == 0) {
        // PANIC!
        hcf();
    }

    pageTable->entries[level] = paging::createPageTableEntry(addr, flags);
    return (PageTable*)addr::getVirtPointer(addr);
}
/*
 * Map a page in the page table
 *
 * @param pageTable - the page table to map the page in
 * @param vaddr - the virtual address to map the page to
 * @param paddr - the physical address to map the page to
 * @param flags - the flags to set for the page
 */
void mapPage(PageTable* pml4, vaddr_t vaddr, paddr_t paddr, int flags) {
    if (!pml4 || !flags) {
        // PANIC!
        hcf();
    }

    // PageTable* table = pageTable;
    // for (int i = 4; i > 1; i--) {
    //     table = advancePageTable(table, index_of(vaddr, i), flags);
    // }
    paging::PageTable *pml3, *pml2, *pml1 = nullptr;
    pml3 = advancePageTable(pml4, index_of(vaddr, 4), flags);
    pml2 = advancePageTable(pml3, index_of(vaddr, 3), flags);
    pml1 = advancePageTable(pml2, index_of(vaddr, 2), flags);

    pml1->entries[index_of(vaddr, 1)] = paging::createPageTableEntry(paddr, flags);

    invlpg(vaddr);
}

void unmapPage(PageTable* pageTable, vaddr_t vaddr) {
    if (!pageTable) {
        // PANIC!
        hcf();
    }

    PageTable* table = pageTable;
    for (int i = 4; i > 1; i--) {
        table = advancePageTable(table, index_of(vaddr, i), 0);
    }

    uint64_t addr = table->entries[index_of(vaddr, 1)].frame;
    table->entries[index_of(vaddr, 1)] = paging::purgePageTableEntry();

    invlpg(vaddr);
    phys::pageFree<>((void*)addr);
}

void mapRange(PageTable* pageTable, Range range, int flags, uint64_t offset) {
    auto [start, end] = range;
    if (start % paging::PAGE_SIZE || end % paging::PAGE_SIZE || start >= end) {
        // PANIC!
        hcf();
    }

    while (start != end) {
        mapPage(pageTable, start + offset, start, flags);
        start += paging::PAGE_SIZE;
    }
}

void mapToKernelPageTable(vaddr_t vaddr, paddr_t paddr, int flags) { mapPage(kernelPagemap, vaddr, paddr, flags); }

void mapRangeToKernelPageTable(Range range, int flags, uint64_t offset) { mapRange(kernelPagemap, range, flags, offset); }

void mapRangeToKernelPageTable(Range range, int flags) {
    // no offset assume hhdm
    mapRange(kernelPagemap, range, flags, addr::getHHDMOffset());
}

}  // namespace ker::mod::mm::virt
