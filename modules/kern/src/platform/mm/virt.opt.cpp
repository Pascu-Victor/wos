#include "virt.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>

#include "platform/dbg/coredump.hpp"
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

// Helper: get the raw uint64_t value of a PTE
static inline uint64_t pteRaw(const paging::PageTableEntry& e) {
    uint64_t val;
    __builtin_memcpy(&val, &e, sizeof(val));
    return val;
}

// Helper: reconstruct a PTE from raw uint64_t with modified flags
static inline paging::PageTableEntry pteFromRaw(uint64_t raw) {
    paging::PageTableEntry e;
    __builtin_memcpy(&e, &raw, sizeof(e));
    return e;
}

bool pagefault_handler(uint64_t control_register, gates::interruptFrame& frame, ker::mod::cpu::GPRegs& gpr) {
    PageFault pagefault = paging::createPageFault(frame.errCode, true);

    // COW handling for user-space write faults
    if ((pagefault.user != 0U) && (pagefault.writable != 0U)) {
        // Walk page tables to find the faulting PTE
        auto* current_task = sched::get_current_task();
        if (current_task == nullptr || current_task->pagemap == nullptr) {
            dbg::log("COW fault: no current task or pagemap");
            hcf();
            return false;
        }

        paging::PageTable* pml4 = current_task->pagemap;
        uint64_t vaddr = control_register;

        // Walk the 4 levels to find the PML1 entry
        uint64_t idx4 = (vaddr >> 39) & 0x1FF;
        uint64_t idx3 = (vaddr >> 30) & 0x1FF;
        uint64_t idx2 = (vaddr >> 21) & 0x1FF;
        uint64_t idx1 = (vaddr >> 12) & 0x1FF;

        if (!pml4->entries[idx4].present) {
            goto not_cow;
        }
        {
            auto* pml3 = (paging::PageTable*)addr::getVirtPointer(pml4->entries[idx4].frame << paging::PAGE_SHIFT);
            if (!pml3->entries[idx3].present) {
                goto not_cow;
            }
            auto* pml2 = (paging::PageTable*)addr::getVirtPointer(pml3->entries[idx3].frame << paging::PAGE_SHIFT);
            if (!pml2->entries[idx2].present) {
                goto not_cow;
            }
            auto* pml1 = (paging::PageTable*)addr::getVirtPointer(pml2->entries[idx2].frame << paging::PAGE_SHIFT);

            paging::PageTableEntry& pte = pml1->entries[idx1];
            if (!pte.present) {
                goto not_cow;
            }

            uint64_t raw = pteRaw(pte);
            if ((raw & paging::PAGE_COW) == 0U) {
                goto not_cow;
            }

            // This is a COW page — handle it
            paddr_t oldPhys = pte.frame << paging::PAGE_SHIFT;
            void* oldVirt = reinterpret_cast<void*>(addr::getVirtPointer(oldPhys));

            uint32_t refcount = phys::pageRefGet(oldVirt);

            if (refcount <= 1) {
                // We're the sole owner — just make it writable and clear COW
                raw &= ~paging::PAGE_COW;
                raw |= paging::PAGE_WRITE;
                pte = pteFromRaw(raw);
                invlpg(vaddr);
                return true;
            }

            // Multiple owners — allocate a new page and copy
            void* newPage = phys::pageAlloc(paging::PAGE_SIZE);
            if (newPage == nullptr) {
                dbg::log("COW fault: OOM allocating new page for vaddr 0x%x", vaddr);
                hcf();
                return false;
            }

            memcpy(newPage, oldVirt, paging::PAGE_SIZE);

            // Decrement refcount on old page (may free it if we were last user elsewhere)
            phys::pageRefDec(oldVirt);

            // Map new page as writable, clear COW
            auto new_phys = (paddr_t)addr::getPhysPointer((vaddr_t)newPage);
            raw &= ~paging::PAGE_COW;
            raw |= paging::PAGE_WRITE;
            // Replace frame bits
            raw &= ~(0xFFFFFFFFFFULL << 12);  // clear frame field
            raw |= (new_phys & ~0xFFFULL);    // set new frame
            pte = pteFromRaw(raw);
            invlpg(vaddr);
            return true;
        }
    }

not_cow:
    // Not a COW fault — let the caller handle it (userspace crash / kernel panic).
    return false;
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

    size_t totalPagesMapped = 0;
    for (size_t i = 0; i < memmapResponse->entry_count; i++) {
        auto entry = memmapResponse->entries[i];
        size_t numPages = entry->length / paging::PAGE_SIZE;

        // Log high-memory entry specifically
        if (entry->base >= 0x100000000ULL) {
            io::serial::write("Mapping entry ");
            io::serial::writeHex(i);
            io::serial::write(": phys ");
            io::serial::writeHex(entry->base);
            io::serial::write(" - ");
            io::serial::writeHex(entry->base + entry->length);
            io::serial::write(" (");
            io::serial::writeHex(numPages);
            io::serial::write(" pages)\n");
        }

        for (size_t j = 0; j < numPages; j++) {
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
            totalPagesMapped++;
        }

        if (entry->base >= 0x100000000ULL) {
            io::serial::write("  Done mapping entry ");
            io::serial::writeHex(i);
            io::serial::write("\n");
        }
    }
    io::serial::write("Total pages mapped: ");
    io::serial::writeHex(totalPagesMapped);
    io::serial::write("\n");

    for (size_t i = 0; i <= kernelFileResponse->kernel_file->size; i++) {
        vaddr_t vaddr = kernelAddressResponse->virtual_base + i * paging::PAGE_SIZE;
        mapPage(kernelPagemap,
                vaddr,                                                         // virtual address
                kernelAddressResponse->physical_base + i * paging::PAGE_SIZE,  // physical address
                paging::pageTypes::KERNEL                                      // kernel memory
        );
    }

    // CRITICAL: Ensure all page table pages are mapped in the HHDM.
    // Page tables allocated by advancePageTable() might be from physical addresses
    // above 4GB that haven't been mapped yet in our kernel pagemap.
    // We must iterate until no new mappings are needed, because mapping a page
    // table page might allocate NEW page table pages that also need mapping.
    size_t ptPagesMappped = 0;
    size_t iteration = 0;
    bool madeProgress = true;

    while (madeProgress) {
        madeProgress = false;
        iteration++;
        size_t mappedThisRound = 0;

        for (size_t pml4i = 256; pml4i < 512; pml4i++) {  // Only kernel half
            PageTableEntry& pml4e = kernelPagemap->entries[pml4i];
            if (!pml4e.present) continue;

            paddr_t pml3Phys = pml4e.frame << paging::PAGE_SHIFT;
            vaddr_t pml3Virt = (vaddr_t)addr::getVirtPointer(pml3Phys);
            if (!isPageMapped(kernelPagemap, pml3Virt)) {
                mapPage(kernelPagemap, pml3Virt, pml3Phys, paging::pageTypes::KERNEL);
                ptPagesMappped++;
                mappedThisRound++;
                madeProgress = true;
            }

            auto* pml3 = (PageTable*)pml3Virt;
            for (size_t pml3i = 0; pml3i < 512; pml3i++) {
                PageTableEntry& pml3e = pml3->entries[pml3i];
                if (!pml3e.present) continue;

                paddr_t pml2Phys = pml3e.frame << paging::PAGE_SHIFT;
                vaddr_t pml2Virt = (vaddr_t)addr::getVirtPointer(pml2Phys);
                if (!isPageMapped(kernelPagemap, pml2Virt)) {
                    mapPage(kernelPagemap, pml2Virt, pml2Phys, paging::pageTypes::KERNEL);
                    ptPagesMappped++;
                    mappedThisRound++;
                    madeProgress = true;
                }

                auto* pml2 = (PageTable*)pml2Virt;
                for (size_t pml2i = 0; pml2i < 512; pml2i++) {
                    PageTableEntry& pml2e = pml2->entries[pml2i];
                    if (!pml2e.present) continue;

                    paddr_t pml1Phys = pml2e.frame << paging::PAGE_SHIFT;
                    vaddr_t pml1Virt = (vaddr_t)addr::getVirtPointer(pml1Phys);
                    if (!isPageMapped(kernelPagemap, pml1Virt)) {
                        mapPage(kernelPagemap, pml1Virt, pml1Phys, paging::pageTypes::KERNEL);
                        ptPagesMappped++;
                        mappedThisRound++;
                        madeProgress = true;
                    }
                }
            }
        }

        if (mappedThisRound > 0) {
            dbg::log("Page table fixup iteration %d: mapped %d pages", iteration, mappedThisRound);
        }
    }
    dbg::log("Total page table pages mapped into HHDM: %d (in %d iterations)", ptPagesMappped, iteration);

    // CRITICAL: Clear PML4[0] to ensure null pointer dereferences cause page faults!
    // Limine may leave identity mappings in the lower half which mask null pointer bugs.
    kernelPagemap->entries[0] = PageTableEntry{};
    dbg::log("Cleared PML4[0] - null derefs will now fault");

    switchToKernelPagemap();
}

// advance page table by level pages
static paging::PageTable* advancePageTable(paging::PageTable* pageTable, size_t level, uint64_t flags) {
    PageTableEntry entry = pageTable->entries[level];
    if (entry.present) {
        bool changed = false;
        if (flags & paging::PAGE_WRITE && !entry.writable) {
            entry.writable = 1;
            changed = true;
        }
        if (flags & paging::PAGE_USER && !entry.user) {
            entry.user = 1;
            changed = true;
        }

        // Only clear NX bit if we are mapping executable code.
        // Never set NX bit on intermediate entries as it affects all children.
        if (!(flags & paging::PAGE_NX) && entry.noExecute) {
            entry.noExecute = 0;
            auto* raw_entry = (uint64_t*)&entry;
            *raw_entry &= ~(1ULL << 63);  // Clear NX bit
            changed = true;
        }

        if (changed) {
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
        // Check if the entry is present before following the pointer
        if (!table->entries[index_of(vaddr, i)].present) {
            return false;
        }
        table = (PageTable*)addr::getVirtPointer(table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT);
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

    invlpg(vaddr);

#ifdef ELF_DEBUG
    if (vaddr >= 0x501000 && vaddr < 0x580000) {
        mod::dbg::log("unifyPageFlags: vaddr=0x%x, flags=0x%x, entry_after=0x%x, nx=%d present=%d", vaddr, flags, *raw_entry,
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

    uint64_t frame = table->entries[index_of(vaddr, 1)].frame;
    table->entries[index_of(vaddr, 1)] = paging::purgePageTableEntry();

    invlpg(vaddr);

    // Convert frame number to physical address, then to HHDM pointer for refcount-aware free
    if (frame != 0) {
        uint64_t physAddr = frame << paging::PAGE_SHIFT;
        void* virtPtr = reinterpret_cast<void*>(addr::getVirtPointer(physAddr));
        phys::pageRefDec(virtPtr);
    }
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
                // Free the page table page itself (page tables are never COW-shared)
                phys::pageFree(nextLevel);
            }
        } else {
            // Level 1 (PML1) - entries point to actual data pages
            // Use refcount-aware free: COW-shared pages won't be freed until all users unmap
            void* pageVirt = reinterpret_cast<void*>(addr::getVirtPointer(physAddr));
            phys::pageRefDec(pageVirt);
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

bool deepCopyUserPagemapCOW(PageTable* src, PageTable* dst) {
    // Walk the user half (PML4[0..255]) of src.
    // For each present PML1 entry pointing to a data page:
    //   1. Mark the src PTE as read-only + COW
    //   2. Create the same PTE in dst (read-only + COW)
    //   3. Increment the physical page's refcount
    // Page table pages (PML3/PML2/PML1) are freshly allocated for dst.

    for (size_t i4 = 0; i4 < 256; i4++) {
        if (!src->entries[i4].present) continue;

        paddr_t srcPml3Phys = src->entries[i4].frame << paging::PAGE_SHIFT;
        auto* srcPml3 = (PageTable*)addr::getVirtPointer(srcPml3Phys);

        // Allocate a new PML3 for dst
        auto* dstPml3 = (PageTable*)phys::pageAlloc();
        if (!dstPml3) return false;
        memset(dstPml3, 0, paging::PAGE_SIZE);

        // Set PML4 entry in dst (copy flags from src)
        dst->entries[i4] = src->entries[i4];
        dst->entries[i4].frame = (paddr_t)addr::getPhysPointer((vaddr_t)dstPml3) >> paging::PAGE_SHIFT;

        for (size_t i3 = 0; i3 < 512; i3++) {
            if (!srcPml3->entries[i3].present) continue;

            paddr_t srcPml2Phys = srcPml3->entries[i3].frame << paging::PAGE_SHIFT;
            auto* srcPml2 = (PageTable*)addr::getVirtPointer(srcPml2Phys);

            auto* dstPml2 = (PageTable*)phys::pageAlloc();
            if (!dstPml2) return false;
            memset(dstPml2, 0, paging::PAGE_SIZE);

            dstPml3->entries[i3] = srcPml3->entries[i3];
            dstPml3->entries[i3].frame = (paddr_t)addr::getPhysPointer((vaddr_t)dstPml2) >> paging::PAGE_SHIFT;

            for (size_t i2 = 0; i2 < 512; i2++) {
                if (!srcPml2->entries[i2].present) continue;
                if (srcPml2->entries[i2].pagesize) {
                    // 2MB huge page — just copy the entry (no COW for huge pages)
                    dstPml2->entries[i2] = srcPml2->entries[i2];
                    continue;
                }

                paddr_t srcPml1Phys = srcPml2->entries[i2].frame << paging::PAGE_SHIFT;
                auto* srcPml1 = (PageTable*)addr::getVirtPointer(srcPml1Phys);

                auto* dstPml1 = (PageTable*)phys::pageAlloc();
                if (!dstPml1) return false;
                memset(dstPml1, 0, paging::PAGE_SIZE);

                dstPml2->entries[i2] = srcPml2->entries[i2];
                dstPml2->entries[i2].frame = (paddr_t)addr::getPhysPointer((vaddr_t)dstPml1) >> paging::PAGE_SHIFT;

                for (size_t i1 = 0; i1 < 512; i1++) {
                    if (!srcPml1->entries[i1].present) continue;

                    uint64_t raw = pteRaw(srcPml1->entries[i1]);

                    // Mark as read-only + COW in BOTH src and dst
                    raw &= ~paging::PAGE_WRITE;
                    raw |= paging::PAGE_COW;
                    srcPml1->entries[i1] = pteFromRaw(raw);
                    dstPml1->entries[i1] = pteFromRaw(raw);

                    // Increment refcount on the shared data page
                    paddr_t dataPhys = srcPml1->entries[i1].frame << paging::PAGE_SHIFT;
                    void* dataVirt = reinterpret_cast<void*>(addr::getVirtPointer(dataPhys));
                    phys::pageRefInc(dataVirt);
                }
            }
        }
    }

    // Flush TLB for the source (parent) since we modified its PTEs
    wrcr3(rdcr3());
    return true;
}

}  // namespace ker::mod::mm::virt
