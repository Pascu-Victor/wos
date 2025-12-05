#include "virt.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>

#include "platform/mm/paging.hpp"
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

PageTable* getKernelPagemap() { return kernelPagemap; }

PageTable* createPagemap() {
    auto* pageTable = (PageTable*)phys::pageAlloc();
    if (pageTable) {
        memset(pageTable, 0, paging::PAGE_SIZE);
    }
    return pageTable;
}

void copyKernelMappings(sched::task::Task* t) {
    for (size_t i = 256; i < 512; i++) {
        t->pagemap->entries[i] = kernelPagemap->entries[i];
    }
}

void switchPagemap(sched::task::Task* t) {
    if (t->pagemap == nullptr) {
        [[unlikely]]
        if (t->name) {
            dbg::log("Task %s has no pagemap\n", t->name);
        } else {
            dbg::log("Task has no pagemap\n Halting.");
        }
        hcf();
    }

    auto phys_pagemap = (uint64_t)addr::getPhysPointer((vaddr_t)t->pagemap);
#ifdef VERBOSE_PAGEMAP_SWITCH
    dbg::log("switchPagemap: task=%s pid=%d virt=0x%x phys=0x%x", (t->name != nullptr) ? t->name : "unknown", t->pid,
             (unsigned int)(uintptr_t)t->pagemap, (unsigned int)phys_pagemap);
#endif
    wrcr3(phys_pagemap);
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
        dbg::log("translate: no page table\n");
        hcf();
    }

    PageTable* table = pageTable;
    for (int i = 4; i > 1; i--) {
        uint64_t phys = table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT;
        table = (PageTable*)(phys + 0xffff800000000000ULL);  // Use HHDM for page table walk
        if ((uint64_t)table == 0xffff800000000000ULL) {
            return 0;
        }
    }

    uint64_t phys = (table->entries[index_of(vaddr, 1)].frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1));
    return phys;  // Return physical address only
}
void initPagemap() {
    cpu::enablePAE();
    cpu::enablePSE();
    kernelPagemap = (PageTable*)phys::pageAlloc();
    if (kernelPagemap == nullptr) {
        // PANIC!
        dbg::log("init: failed to allocate kernel pagemap\n function: initPagemap\n");
        hcf();
    }
    dbg::log("Kernel pagemap allocated at %x\n", kernelPagemap);
    for (size_t i = 0; i < memmapResponse->entry_count; i++) {
        char typeBuf[32];
        switch (memmapResponse->entries[i]->type) {
            case LIMINE_MEMMAP_USABLE: {
                constexpr char usableStr[] = "USABLE";
                std::strncpy(typeBuf, usableStr, sizeof(usableStr));
                break;
            }
            case LIMINE_MEMMAP_RESERVED: {
                constexpr char reservedStr[] = "RESERVED";
                std::strncpy(typeBuf, reservedStr, sizeof(reservedStr));
                break;
            }
            case LIMINE_MEMMAP_ACPI_RECLAIMABLE: {
                constexpr char acpiReclaimableStr[] = "ACPI_RECLAIMABLE";
                std::strncpy(typeBuf, acpiReclaimableStr, sizeof(acpiReclaimableStr));
                break;
            }
            case LIMINE_MEMMAP_ACPI_NVS: {
                constexpr char acpiNvsStr[] = "ACPI_NVS";
                std::strncpy(typeBuf, acpiNvsStr, sizeof(acpiNvsStr));
                break;
            }
            case LIMINE_MEMMAP_BAD_MEMORY: {
                constexpr char badMemoryStr[] = "BAD_MEMORY";
                std::strncpy(typeBuf, badMemoryStr, sizeof(badMemoryStr));
                break;
            }
            case LIMINE_MEMMAP_BOOTLOADER_RECLAIMABLE: {
                constexpr char bootloaderReclaimableStr[] = "BOOTLOADER_RECLAIMABLE";
                std::strncpy(typeBuf, bootloaderReclaimableStr, sizeof(bootloaderReclaimableStr));
                break;
            }
            case LIMINE_MEMMAP_KERNEL_AND_MODULES: {
                constexpr char kernelAndModulesStr[] = "KERNEL_AND_MODULES";
                std::strncpy(typeBuf, kernelAndModulesStr, sizeof(kernelAndModulesStr));
                break;
            }
            case LIMINE_MEMMAP_FRAMEBUFFER: {
                constexpr char framebufferStr[] = "FRAMEBUFFER";
                std::strncpy(typeBuf, framebufferStr, sizeof(framebufferStr));
                break;
            }
            default: {
                constexpr char unknownStr[] = "UNKNOWN";
                std::strncpy(typeBuf, unknownStr, sizeof(unknownStr));
                break;
            }
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

        bool desired_nx = (flags & paging::PAGE_NX) != 0;
        if (entry.noExecute != desired_nx) {
            entry.noExecute = desired_nx ? 1 : 0;
            auto* raw_entry = (uint64_t*)&entry;
            if (!desired_nx) {
                *raw_entry &= ~(1ULL << 63);  // Clear NX bit
            } else {
                *raw_entry |= (1ULL << 63);  // Set NX bit
            }
            pageTable->entries[level] = entry;
            // Force full TLB flush
            wrcr3(rdcr3());
        }

        return (PageTable*)addr::getVirtPointer(entry.frame << paging::PAGE_SHIFT);
    }

    void* pageVirt = phys::pageAlloc();
    if (pageVirt == nullptr) {
        // PANIC!
        dbg::log("init: failed to allocate kernel page table\n function: advancePageTable\n");
        hcf();
    }

    paddr_t pagePhys = (paddr_t)addr::getPhysPointer((vaddr_t)pageVirt);

    memset(pageVirt, 0, paging::PAGE_SIZE);

    pageTable->entries[level] = paging::createPageTableEntry(pagePhys, flags);
    return (PageTable*)pageVirt;
}
/*
 * Map a page in the page table
 *
 * @param pageTable - the page table to map the page in
 * @param vaddr - the virtual address to map the page to
 * @param paddr - the physical address to map the page to
 * @param flags - the flags to set for the page
 */
void mapPage(PageTable* pml4, const vaddr_t vaddr, const paddr_t paddr, const uint64_t flags) {
    if (!pml4 || flags == 0) {
        // PANIC!
        dbg::log("init: failed to map page\n function: mapPage\n args: <vaddr: %p, paddr: %p, flags: %x>", vaddr, paddr, flags);
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

bool isPageMapped(PageTable* pageTable, vaddr_t vaddr) {
    if (!pageTable) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: isPageMapped\n");
        hcf();
    }

    PageTable* table = pageTable;
    for (int i = 4; i > 1; i--) {
        table = (PageTable*)addr::getVirtPointer(table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT);
        if (!addr::getPhysPointer((vaddr_t)table)) {
            return false;
        }
    }

    return table->entries[index_of(vaddr, 1)].present;
}

void unifyPageFlags(PageTable* pageTable, vaddr_t vaddr, uint64_t flags) {
    if (pageTable == nullptr) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: unifyPageFlags\n");
        hcf();
    }

    PageTable* table = pageTable;
    for (int i = 4; i > 1; i--) {
        table = advancePageTable(table, index_of(vaddr, i), flags);
    }

    // Get the current page table entry by computing the index first
    uint64_t idx = index_of(vaddr, 1);
    PageTableEntry& entry = table->entries[idx];

    if (entry.present == 0) {
        // Page doesn't exist, nothing to modify
        return;
    }

    // Update page table entry flags
    entry.present = (flags & paging::PAGE_PRESENT) != 0U ? 1 : 0;
    entry.writable = (flags & paging::PAGE_WRITE) != 0U ? 1 : 0;
    entry.user = (flags & paging::PAGE_USER) != 0U ? 1 : 0;
    entry.noExecute = (flags & paging::PAGE_NX) != 0U ? 1 : 0;

    auto* raw_entry = (uint64_t*)&entry;
    constexpr uint64_t NX_BIT_POSITION = 63ULL;
    if ((flags & paging::PAGE_NX) != 0U) {
        *raw_entry |= (1ULL << NX_BIT_POSITION);  // Set NX bit
    } else {
        *raw_entry &= ~(1ULL << NX_BIT_POSITION);  // Clear NX bit
    }

#ifdef ELF_DEBUG
    if (vaddr >= 0x501000 && vaddr < 0x580000) {
        mod::dbg::log("unifyPageFlags: vaddr=0x%x, flags=0x%llx, entry_after=0x%llx, nx=%d present=%d", vaddr, flags, *raw_entry,
                      static_cast<int>((*raw_entry >> NX_BIT_POSITION) & 1U), static_cast<int>(entry.present));
    }
#endif
}

void unmapPage(PageTable* pageTable, vaddr_t vaddr) {
    if (!pageTable) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: unmapPage\n");
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

void mapRange(PageTable* pageTable, Range range, uint64_t flags, uint64_t offset) {
    auto [start, end] = range;
    if (start % paging::PAGE_SIZE || end % paging::PAGE_SIZE || start >= end) {
        // PANIC!
        dbg::log("init: failed to map range\n");
        hcf();
    }

    while (start != end) {
        mapPage(pageTable, start + offset, start, flags);
        start += paging::PAGE_SIZE;
    }
}

void mapToKernelPageTable(vaddr_t vaddr, paddr_t paddr, uint64_t flags) { mapPage(kernelPagemap, vaddr, paddr, flags); }

void mapRangeToKernelPageTable(Range range, uint64_t flags, uint64_t offset) { mapRange(kernelPagemap, range, flags, offset); }

void mapRangeToKernelPageTable(Range range, uint64_t flags) {
    // no offset assume hhdm
    mapRange(kernelPagemap, range, flags, addr::getHHDMOffset());
}

// Helper to free a page table level recursively
// level: 4=PML4, 3=PML3, 2=PML2, 1=PML1
static void freePageTableLevel(PageTable* table, int level) {
    if (table == nullptr || level < 1) {
        return;
    }

    // For user space, only process entries 0-255 at PML4 level
    // Entries 256-511 are kernel space and must not be touched
    size_t maxEntry = (level == 4) ? 256 : 512;

    for (size_t i = 0; i < maxEntry; i++) {
        if (table->entries[i].present == 0) {
            continue;
        }

        uint64_t physAddr = static_cast<uint64_t>(table->entries[i].frame) << paging::PAGE_SHIFT;
        if (physAddr == 0) {
            continue;
        }

        if (level > 1) {
            // This entry points to another page table - recurse
            // Check for huge pages (2MB at level 2, 1GB at level 3)
            if (table->entries[i].pagesize != 0) {
                // Huge page - the frame is the actual data, not a page table
                // Don't recurse, just clear the entry
                // Note: We don't free huge pages here as they may be specially allocated
            } else {
                PageTable* nextLevel = reinterpret_cast<PageTable*>(addr::getVirtPointer(physAddr));
                freePageTableLevel(nextLevel, level - 1);
                // Free the page table page itself
                phys::pageFree(nextLevel);
            }
        } else {
            // Level 1 (PML1) - entries point to actual data pages
            void* pageVirt = reinterpret_cast<void*>(addr::getVirtPointer(physAddr));
            phys::pageFree(pageVirt);
        }

        // Clear the entry
        table->entries[i] = paging::purgePageTableEntry();
    }
}

void destroyUserSpace(PageTable* pagemap) {
    if (pagemap == nullptr) {
        return;
    }

    // Free all user-space mappings (PML4 entries 0-255)
    // This recursively frees all page tables and data pages
    freePageTableLevel(pagemap, 4);

    // Invalidate TLB for this address space
    // Note: We should already be on a different pagemap when calling this
    wrcr3(rdcr3());
}

}  // namespace ker::mod::mm::virt
