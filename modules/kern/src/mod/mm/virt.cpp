#include "virt.hpp"

namespace ker::mod::mm::virt {
    static paging::PageTable* kernelPagemap;
    
    static limine_memmap_response* memmapResponse;
    static limine_kernel_file_response* kernelFileResponse;
    static limine_kernel_address_response* kernelAddressResponse;

    void init(
        limine_memmap_response *_memmapResponse,
        limine_kernel_file_response* _kernelFileResponse,
        limine_kernel_address_response* _kernelAddressResponse
    ) {
        memmapResponse = _memmapResponse;
        kernelFileResponse = _kernelFileResponse;
        kernelAddressResponse = _kernelAddressResponse;
    }

    void switchToKernelPagemap() {
        wrcr3((uint64_t)addr::getVirtAddr((paddr_t)kernelPagemap));
    }

    PageTable* createPagemap() {
        return phys::pageAlloc<PageTable>();
    }

    void copyKernelMappings(sched::task::Task t) {
        for(size_t i = 256; i < 512; i++) {
            t.pagemap->entries[i] = kernelPagemap->entries[i];
        }
    }

    void switchPagemap(sched::task::Task t) {
        if(!t.pagemap) {
            //PANIC!
            hcf();
        }

        wrcr3((uint64_t)addr::getVirtAddr((paddr_t)t.pagemap));
    }

    void pagefaultHandler(uint64_t controlRegister, int errCode) {
        PageFault pagefault = paging::createPageFault(errCode, true);

        if(pagefault.present) {
            //PANIC!
            hcf();
        }

        mapPage(
            (PageTable*)rdcr3(),
            controlRegister,
            controlRegister,
            pagefault.flags
        );
    }

    static inline uint64_t index_of(const uint64_t vaddr,const int offset)
    {
        return vaddr >> (12 + 9 * (offset - 1)) & 0x1FF;
    }

    paddr_t translate(PageTable* pageTable, vaddr_t vaddr) {
        if(!pageTable) {
            //PANIC!
            hcf();
        }

        PageTable* table = pageTable;
        for(int i = 4; i > 1; i--) {
            table = (PageTable*)addr::getPhysAddr(table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT);
            if(!table) {
                return 0;
            }
        }

        return (paddr_t)addr::getPhysAddr((table->entries[index_of(vaddr, 1)].frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1)));
    }

    void initPagemap() {
        kernelPagemap = phys::pageAlloc<paging::PageTable>();
        if (kernelPagemap == nullptr) {
            //PANIC!
            hcf();
        }


        for(size_t i = 0; i < memmapResponse->entry_count; i++) {
            auto entry = memmapResponse->entries[i];
            if (entry->type != LIMINE_MEMMAP_USABLE) {
                continue;
            }

            for(size_t j = 0; j < entry->length / paging::PAGE_SIZE; j++) {
                mapPage(
                    kernelPagemap,
                    (vaddr_t)addr::getPhysAddr(entry->base + j * paging::PAGE_SIZE),
                    entry->base + j * paging::PAGE_SIZE,
                    entry->type == LIMINE_MEMMAP_RESERVED ||
                            entry->type == LIMINE_MEMMAP_BAD_MEMORY || 
                            entry->type == LIMINE_MEMMAP_KERNEL_AND_MODULES
                            ? paging::pageTypes::READONLY : paging::pageTypes::KERNEL
                );
            }
        }

        for(size_t i = 0; i < kernelFileResponse->kernel_file->size; i++) {
            mapPage(
                kernelPagemap,
                kernelAddressResponse->virtual_base + i * paging::PAGE_SIZE,
                kernelAddressResponse->physical_base + i * paging::PAGE_SIZE,
                paging::pageTypes::KERNEL
            );
        }

        switchToKernelPagemap();
    }

    // advance page table by level pages
    static paging::PageTable* advancePageTable(paging::PageTable* pageTable, size_t level, uint64_t flags) {
        PageTableEntry entry = pageTable->entries[level];
        if(entry.present) {
            if(flags & paging::PAGE_WRITE  && !entry.writable) {
                entry.writable = 1;
                pageTable->entries[level] = entry;
            }
            if(flags & paging::PAGE_USER  && !entry.user) {
                entry.user = 1;
                pageTable->entries[level] = entry;
            }

            return (PageTable*)addr::getPhysAddr(entry.frame << paging::PAGE_SHIFT);
        }

        uint64_t addr = (uint64_t)addr::getVirtAddr((uint64_t)phys::pageAlloc<PageTable>());
        if(addr == 0) {
            //PANIC!
            hcf();
        }
        
        pageTable->entries[level] = paging::createPageTableEntry(addr, flags);
        return (PageTable*)addr::getPhysAddr(addr);
    }

    void mapPage(PageTable *pageTable, vaddr_t vaddr, paddr_t paddr, int flags) {

        if(!pageTable || !flags){
            //PANIC!
            hcf();
        }

        PageTable* table = pageTable;
        for(int i = 4; i > 1; i--) {
            table = advancePageTable(table, index_of(vaddr, i), flags);
        }

        table->entries[index_of(vaddr, 1)] = paging::createPageTableEntry(paddr, flags);

        invlpg(vaddr);
    }

    void unmapPage(PageTable *pageTable, vaddr_t vaddr) {
        if(!pageTable){
            //PANIC!
            hcf();
        }

        PageTable* table = pageTable;
        for(int i = 4; i > 1; i--) {
            table = advancePageTable(table, index_of(vaddr, i), 0);
        }

        uint64_t addr = table->entries[index_of(vaddr, 1)].frame;
        table->entries[index_of(vaddr, 1)] = paging::purgePageTableEntry();

        invlpg(vaddr);
        phys::pageFree<>(addr::getVirtAddr(addr));
    }

    void mapRange(PageTable *pageTable, Range range, int flags, uint64_t offset) {
        auto [start, end] = range;
        if(start % paging::PAGE_SIZE || end % paging::PAGE_SIZE || start >= end) {
            //PANIC!
            hcf();
        }

        while (start!=end)
        {
            mapPage(pageTable, start + offset, start, flags);
            start += paging::PAGE_SIZE;
        }
    }

}