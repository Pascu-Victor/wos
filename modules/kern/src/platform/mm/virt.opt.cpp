#include "virt.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <string_view>

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "mod/io/serial/serial.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/coredump.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "util/hcf.hpp"
namespace ker::mod::mm::virt {
static paging::PageTable* kernel_pagemap;

static limine_memmap_response* memmap_response;
static limine_executable_file_response* kernel_file_response;
static limine_executable_address_response* kernel_address_response;

constexpr size_t KERNEL_PML4_START = 256;
constexpr size_t KERNEL_PML4_END = 512;

void init(limine_memmap_response* memmap_response_param, limine_executable_file_response* kernel_file_response_param,
          limine_executable_address_response* kernel_address_response_param) {
    memmap_response = memmap_response_param;
    kernel_file_response = kernel_file_response_param;
    kernel_address_response = kernel_address_response_param;
}

void switchToKernelPagemap() { wrcr3((uint64_t)addr::get_phys_pointer((paddr_t)kernel_pagemap)); }

auto getKernelPagemap() -> PageTable* { return kernel_pagemap; }

auto createPagemap() -> PageTable* {
    auto* page_table = (PageTable*)phys::pageAlloc();
    if (page_table != nullptr) {
        memset(page_table, 0, paging::PAGE_SIZE);
    }
    return page_table;
}

void copyKernelMappings(sched::task::Task* t) {
    for (size_t i = KERNEL_PML4_START; i < KERNEL_PML4_END; i++) {
        t->pagemap->entries[i] = kernel_pagemap->entries[i];
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

    auto phys_pagemap = (uint64_t)addr::get_phys_pointer((vaddr_t)t->pagemap);
#ifdef VERBOSE_PAGEMAP_SWITCH
    dbg::log("switchPagemap: task=%s pid=%d virt=0x%x phys=0x%x", (t->name != nullptr) ? t->name : "unknown", t->pid,
             (unsigned int)(uintptr_t)t->pagemap, (unsigned int)phys_pagemap);
#endif
    // Skip CR3 write if the pagemap hasn't changed (e.g. switching between threads
    // of the same process). Writing CR3 flushes the entire TLB which is very
    // expensive for large working sets.
    if (rdcr3() != phys_pagemap) {
        wrcr3(phys_pagemap);
    }
}

// Helper: get the raw uint64_t value of a PTE
static inline auto pte_raw(const paging::PageTableEntry& e) -> uint64_t {
    uint64_t val = 0;
    __builtin_memcpy(&val, &e, sizeof(val));
    return val;
}

// Helper: reconstruct a PTE from raw uint64_t with modified flags
static inline auto pte_from_raw(uint64_t raw) -> paging::PageTableEntry {
    paging::PageTableEntry e{};
    __builtin_memcpy(&e, &raw, sizeof(e));
    return e;
}

auto pagefault_handler(uint64_t control_register, gates::interruptFrame& frame, ker::mod::cpu::GPRegs& gpr) -> bool {
    PageFault pagefault = paging::createPageFault(frame.errCode, true);

#ifdef WOS_KASAN
    // KASan shadow-region demand paging.  Shadow pages are zeroed (accessible)
    // on first access; KASan poisons them selectively afterwards.
    // This must run before the COW path to avoid mistaking a shadow fault for
    // a kernel panic.
    if (kasan::handle_shadow_fault(control_register, "shadow fault handler in page fault", frame, gpr)) {
        return true;
    }
#endif

    // COW handling for write faults to user-space COW pages.
    // This covers both user-mode writes AND kernel-mode writes to user pages
    // (e.g. syscall read() copying data into a user buffer via memcpy).
    // Guard: only handle addresses in the user-space half of the canonical
    // address space to ensure we never touch kernel page-table entries.
    if ((pagefault.writable != 0U) && (control_register < 0x0000800000000000ULL)) {
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
        const uint64_t IDX4 = (vaddr >> 39) & 0x1FF;
        const uint64_t IDX3 = (vaddr >> 30) & 0x1FF;
        const uint64_t IDX2 = (vaddr >> 21) & 0x1FF;
        const uint64_t IDX1 = (vaddr >> 12) & 0x1FF;

        if (!pml4->entries[IDX4].present) {
            goto not_cow;
        }
        {
            auto* pml3 = (paging::PageTable*)addr::get_virt_pointer(pml4->entries[IDX4].frame << paging::PAGE_SHIFT);
            if (!pml3->entries[IDX3].present) {
                goto not_cow;
            }
            auto* pml2 = (paging::PageTable*)addr::get_virt_pointer(pml3->entries[IDX3].frame << paging::PAGE_SHIFT);
            if (!pml2->entries[IDX2].present) {
                goto not_cow;
            }
            auto* pml1 = (paging::PageTable*)addr::get_virt_pointer(pml2->entries[IDX2].frame << paging::PAGE_SHIFT);

            paging::PageTableEntry& pte = pml1->entries[IDX1];
            if (!pte.present) {
                goto not_cow;
            }

            uint64_t raw = pte_raw(pte);
            if ((raw & paging::PAGE_COW) == 0U) {
                goto not_cow;
            }

            // This is a COW page - handle it
            paddr_t old_phys = pte.frame << paging::PAGE_SHIFT;
            void* old_virt = reinterpret_cast<void*>(addr::get_virt_pointer(old_phys));

            uint32_t refcount = phys::pageRefGet(old_virt);

            if (refcount <= 1) {
                // We're the sole owner - just make it writable and clear COW
                raw &= ~paging::PAGE_COW;
                raw |= paging::PAGE_WRITE;
                pte = pte_from_raw(raw);
                invlpg(vaddr);
                return true;
            }

            // Pin old_virt so it cannot be freed by a concurrent COW handler on
            // another CPU between our refcount read and the memcpy below.  Without
            // this, a racing handler could decrement the refcount to 0, freeing
            // and zeroing the page, so our memcpy would copy zeros instead of the
            // real content (causing e.g. RELR relocations to produce garbage VAs).
            phys::pageRefInc(old_virt);

            // Multiple owners - allocate a new page and copy
            void* new_page = phys::pageAlloc(paging::PAGE_SIZE);
            if (new_page == nullptr) {
                phys::pageRefDec(old_virt);  // release pin
                dbg::log("COW fault: OOM allocating new page for vaddr 0x%x", vaddr);
                hcf();
                return false;
            }

            memcpy(new_page, old_virt, paging::PAGE_SIZE);  // safe: old page is pinned

            // Re-read the PTE: if PAGE_COW is already gone, another CPU handled
            // this fault while we were copying.  Discard our copy; the other
            // handler's mapping is already live.
            uint64_t raw_now = pte_raw(pte);
            if ((raw_now & paging::PAGE_COW) == 0U) {
                phys::pageRefDec(new_page);  // discard unused allocation
                phys::pageRefDec(old_virt);  // release pin only (PTE ref transferred by the other handler)
                return true;
            }

            // Map new page as writable, clear COW
            auto new_phys = (paddr_t)addr::get_phys_pointer((vaddr_t)new_page);
            raw &= ~paging::PAGE_COW;
            raw |= paging::PAGE_WRITE;
            // Replace frame bits
            raw &= ~(0xFFFFFFFFFFULL << 12);  // clear frame field
            raw |= (new_phys & ~0xFFFULL);    // set new frame
            pte = pte_from_raw(raw);
            invlpg(vaddr);

            // Release pin and our PTE reference to old_virt (two decrements).
            phys::pageRefDec(old_virt);  // release pin (paired with pageRefInc above)
            phys::pageRefDec(old_virt);  // release PTE reference (old_virt no longer mapped here)
            return true;
        }
    }

not_cow:
    // Not a COW fault - let the caller handle it (userspace crash / kernel panic).
    return false;
}

static inline auto index_of(const uint64_t VADDR, const int OFFSET) -> uint64_t { return VADDR >> (12 + (9 * (OFFSET - 1))) & 0x1FF; }

paddr_t translate(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        dbg::log("translate: no page table\n");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        const auto& entry = table->entries[index_of(vaddr, i)];
        if (!entry.present) {
            return PADDR_INVALID;
        }
        uint64_t phys = entry.frame << paging::PAGE_SHIFT;
        table = (PageTable*)(phys + 0xffff800000000000ULL);  // Use HHDM for page table walk
    }

    const auto& pte = table->entries[index_of(vaddr, 1)];
    if (!pte.present) {
        return PADDR_INVALID;
    }
    uint64_t phys = (pte.frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1));
    return phys;  // Return physical address only
}
void initPagemap() {
    cpu::enablePAE();
    cpu::enablePSE();
    kernel_pagemap = (PageTable*)phys::pageAlloc();
    if (kernel_pagemap == nullptr) {
        // PANIC!
        dbg::log("init: failed to allocate kernel pagemap\n function: initPagemap\n");
        hcf();
    }
    dbg::log("Kernel pagemap allocated at %x\n", kernel_pagemap);
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        const char* type_str = [&]() -> const char* {
            switch (entry->type) {
                case LIMINE_MEMMAP_USABLE:
                    return "USABLE";
                case LIMINE_MEMMAP_RESERVED:
                    return "RESERVED";
                case LIMINE_MEMMAP_ACPI_RECLAIMABLE:
                    return "ACPI_RECLAIMABLE";
                case LIMINE_MEMMAP_ACPI_NVS:
                    return "ACPI_NVS";
                case LIMINE_MEMMAP_BAD_MEMORY:
                    return "BAD_MEMORY";
                case LIMINE_MEMMAP_BOOTLOADER_RECLAIMABLE:
                    return "BOOTLOADER_RECLAIMABLE";
                case LIMINE_MEMMAP_EXECUTABLE_AND_MODULES:
                    return "KERNEL_AND_MODULES";
                case LIMINE_MEMMAP_FRAMEBUFFER:
                    return "FRAMEBUFFER";
                default:
                    return "UNKNOWN";
            }
        }();
        dbg::log("Memory map entry %d: %x - %x (%s)", i, entry->base, entry->base + entry->length, type_str);
    }

    size_t total_pages_mapped = 0;
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        size_t num_pages = entry->length / paging::PAGE_SIZE;

        for (size_t j = 0; j < num_pages; j++) {
            auto vaddr = (paddr_t)addr::get_virt_pointer(entry->base + (j * paging::PAGE_SIZE));
            mapPage(kernel_pagemap,
                    vaddr,                                                          // virtual address
                    entry->base + (j * paging::PAGE_SIZE),                          // physical address
                    entry->type == LIMINE_MEMMAP_RESERVED                           // reserved memory
                            || entry->type == LIMINE_MEMMAP_BAD_MEMORY              // bad memory
                            || entry->type == LIMINE_MEMMAP_EXECUTABLE_AND_MODULES  // kernel and modules
                        ? paging::pageTypes::READONLY                               // becomes read-only
                        : paging::pageTypes::KERNEL                                 // otherwise kernel memory
            );
            total_pages_mapped++;
        }
    }
    io::serial::write("Total pages mapped: ");
    io::serial::writeHex(total_pages_mapped);
    io::serial::write("\n");

    for (size_t i = 0; i <= kernel_file_response->executable_file->size; i++) {
        vaddr_t vaddr = kernel_address_response->virtual_base + (i * paging::PAGE_SIZE);
        mapPage(kernel_pagemap,
                vaddr,                                                             // virtual address
                kernel_address_response->physical_base + (i * paging::PAGE_SIZE),  // physical address
                paging::pageTypes::KERNEL                                          // kernel memory
        );
    }

    // CRITICAL: Ensure all page table pages are mapped in the HHDM.
    // Page tables allocated by advancePageTable() might be from physical addresses
    // above 4GB that haven't been mapped yet in our kernel pagemap.
    // We must iterate until no new mappings are needed, because mapping a page
    // table page might allocate NEW page table pages that also need mapping.
    size_t pt_pages_mappped = 0;
    size_t iteration = 0;
    bool made_progress = true;

    while (made_progress) {
        made_progress = false;
        iteration++;
        size_t mapped_this_round = 0;
        for (size_t pml4i = KERNEL_PML4_START; pml4i < KERNEL_PML4_END; pml4i++) {  // Only kernel half
            PageTableEntry& pml4e = kernel_pagemap->entries[pml4i];
            if (!pml4e.present) {
                continue;
            }

            paddr_t pml3_phys = pml4e.frame << paging::PAGE_SHIFT;
            auto pml3_virt = (vaddr_t)addr::get_virt_pointer(pml3_phys);
            if (!isPageMapped(kernel_pagemap, pml3_virt)) {
                mapPage(kernel_pagemap, pml3_virt, pml3_phys, paging::pageTypes::KERNEL);
                pt_pages_mappped++;
                mapped_this_round++;
                made_progress = true;
            }

            auto* pml3 = (PageTable*)pml3_virt;
            for (auto& pml3e : pml3->entries) {
                if (!pml3e.present) {
                    continue;
                }

                paddr_t pml2_phys = pml3e.frame << paging::PAGE_SHIFT;
                auto pml2_virt = (vaddr_t)addr::get_virt_pointer(pml2_phys);
                if (!isPageMapped(kernel_pagemap, pml2_virt)) {
                    mapPage(kernel_pagemap, pml2_virt, pml2_phys, paging::pageTypes::KERNEL);
                    pt_pages_mappped++;
                    mapped_this_round++;
                    made_progress = true;
                }

                auto* pml2 = (PageTable*)pml2_virt;
                for (auto& pml2e : pml2->entries) {
                    if (!pml2e.present) {
                        continue;
                    }

                    paddr_t pml1_phys = pml2e.frame << paging::PAGE_SHIFT;

                    auto pml1_virt = (vaddr_t)addr::get_virt_pointer(pml1_phys);
                    if (!isPageMapped(kernel_pagemap, pml1_virt)) {
                        mapPage(kernel_pagemap, pml1_virt, pml1_phys, paging::pageTypes::KERNEL);
                        pt_pages_mappped++;
                        mapped_this_round++;
                        made_progress = true;
                    }
                }
            }
        }

        if (mapped_this_round > 0) {
            dbg::log("Page table fixup iteration %d: mapped %d pages", iteration, mapped_this_round);
        }
    }
    dbg::log("Total page table pages mapped into HHDM: %d (in %d iterations)", pt_pages_mappped, iteration);

    // CRITICAL: Clear PML4[0] to ensure null pointer dereferences cause page faults!
    // Limine may leave identity mappings in the lower half which mask null pointer bugs.
    kernel_pagemap->entries[0] = PageTableEntry{};
    dbg::log("Cleared PML4[0] - null derefs will now fault");

    switchToKernelPagemap();
}

// advance page table by level pages
static auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags) -> paging::PageTable* {
    PageTableEntry entry = page_table->entries[level];
    if (entry.present) {
        bool changed = false;
        if (((flags & paging::PAGE_WRITE) != 0U) && !entry.writable) {
            entry.writable = 1;
            changed = true;
        }
        if (((flags & paging::PAGE_USER) != 0U) && !entry.user) {
            entry.user = 1;
            changed = true;
        }

        // Only clear NX bit if we are mapping executable code.
        // Never set NX bit on intermediate entries as it affects all children.
        if (((flags & paging::PAGE_NX) == 0U) && entry.noExecute) {
            entry.noExecute = 0;
            auto* raw_entry = (uint64_t*)&entry;
            constexpr uint64_t NX_MASK = ~(1ULL << 63);
            *raw_entry &= NX_MASK;  // Clear NX bit
            changed = true;
        }

        if (changed) {
            page_table->entries[level] = entry;
            // Force full TLB flush
            wrcr3(rdcr3());
        }

        return (PageTable*)addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT);
    }

    void* page_virt = phys::pageAlloc();
    if (page_virt == nullptr) {
        // PANIC!
        dbg::log("init: failed to allocate kernel page table\n function: advancePageTable\n");
        hcf();
    }

    auto page_phys = (paddr_t)addr::get_phys_pointer((vaddr_t)page_virt);

    memset(page_virt, 0, paging::PAGE_SIZE);

    page_table->entries[level] = paging::createPageTableEntry(page_phys, flags);
    return (PageTable*)page_virt;
}
/*
 * Map a page in the page table
 *
 * @param pageTable - the page table to map the page in
 * @param vaddr - the virtual address to map the page to
 * @param paddr - the physical address to map the page to
 * @param flags - the flags to set for the page
 */
void mapPage(PageTable* pml4, const vaddr_t VADDR, const paddr_t PADDR, const uint64_t FLAGS) {
    if ((pml4 == nullptr) || FLAGS == 0) {
        // PANIC!
        dbg::log("init: failed to map page\n function: mapPage\n args: <vaddr: %p, paddr: %p, flags: %x>", VADDR, PADDR, FLAGS);
        hcf();
    }

    // PageTable* table = pageTable;
    // for (int i = 4; i > 1; i--) {
    //     table = advancePageTable(table, index_of(vaddr, i), flags);
    // }
    paging::PageTable* pml3 = nullptr;
    paging::PageTable* pml2 = nullptr;
    paging::PageTable* pml1 = nullptr;
    pml3 = advance_page_table(pml4, index_of(VADDR, 4), FLAGS);
    pml2 = advance_page_table(pml3, index_of(VADDR, 3), FLAGS);
    pml1 = advance_page_table(pml2, index_of(VADDR, 2), FLAGS);

    pml1->entries[index_of(VADDR, 1)] = paging::createPageTableEntry(PADDR, FLAGS);

    invlpg(VADDR);
}

auto isPageMapped(PageTable* page_table, vaddr_t vaddr) -> bool {
    if (page_table == nullptr) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: isPageMapped\n");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        // Check if the entry is present before following the pointer
        if (!table->entries[index_of(vaddr, i)].present) {
            return false;
        }
        table = (PageTable*)addr::get_virt_pointer(table->entries[index_of(vaddr, i)].frame << paging::PAGE_SHIFT);
    }

    return table->entries[index_of(vaddr, 1)].present;
}

void unifyPageFlags(PageTable* page_table, vaddr_t vaddr, uint64_t flags) {
    if (page_table == nullptr) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: unifyPageFlags\n");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), flags);
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

void unmapPage(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        // PANIC!
        dbg::log("init: failed to get page table\n function: unmapPage\n");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), 0);
    }

    uint64_t frame = table->entries[index_of(vaddr, 1)].frame;
    table->entries[index_of(vaddr, 1)] = paging::purgePageTableEntry();

    invlpg(vaddr);

    // Convert frame number to physical address, then to HHDM pointer for refcount-aware free
    if (frame != 0) {
        uint64_t phys_addr = frame << paging::PAGE_SHIFT;
        void* virt_ptr = reinterpret_cast<void*>(addr::get_virt_pointer(phys_addr));
        phys::pageRefDec(virt_ptr);
    }
}

void mapRange(PageTable* page_table, Range range, uint64_t flags, uint64_t offset) {
    auto [start, end] = range;
    if (((start % paging::PAGE_SIZE) != 0U) || ((end % paging::PAGE_SIZE) != 0U) || start >= end) {
        // PANIC!
        dbg::log("init: failed to map range\n");
        hcf();
    }

    while (start != end) {
        mapPage(page_table, start + offset, start, flags);
        start += paging::PAGE_SIZE;
    }
}

void mapToKernelPageTable(vaddr_t vaddr, paddr_t paddr, uint64_t flags) { mapPage(kernel_pagemap, vaddr, paddr, flags); }

void mapRangeToKernelPageTable(Range range, uint64_t flags, uint64_t offset) { mapRange(kernel_pagemap, range, flags, offset); }

void mapRangeToKernelPageTable(Range range, uint64_t flags) {
    // no offset assume hhdm
    mapRange(kernel_pagemap, range, flags, addr::get_hhdm_offset());
}

// Helper to free a page table level recursively
// level: 4=PML4, 3=PML3, 2=PML2, 1=PML1
static void free_page_table_level(PageTable* table, int level) {
    if (table == nullptr || level < 1) {
        return;
    }

    // For user space, only process entries 0-255 at PML4 level
    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;

    for (size_t i = 0; i < MAX_ENTRY; i++) {
        if (table->entries[i].present == 0) {
            continue;
        }

        uint64_t phys_addr = static_cast<uint64_t>(table->entries[i].frame) << paging::PAGE_SHIFT;
        if (phys_addr == 0) {
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
                auto* next_level = reinterpret_cast<PageTable*>(addr::get_virt_pointer(phys_addr));
                free_page_table_level(next_level, level - 1);
                // Free the page table page itself (page tables are never COW-shared)
                phys::pageFree(next_level);
            }
        } else {
            // Level 1 (PML1) - entries point to actual data pages
            // Use refcount-aware free: COW-shared pages won't be freed until all users unmap
            void* page_virt = reinterpret_cast<void*>(addr::get_virt_pointer(phys_addr));
            phys::pageRefDec(page_virt);
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
    free_page_table_level(pagemap, 4);

    // Invalidate TLB for this address space
    // Note: We should already be on a different pagemap when calling this
    wrcr3(rdcr3());
}

auto deepCopyUserPagemapCOW(PageTable* src, PageTable* dst) -> bool {
    // Walk the user half (PML4[0..255]) of src.
    // For each present PML1 entry pointing to a data page:
    //   1. Mark the src PTE as read-only + COW
    //   2. Create the same PTE in dst (read-only + COW)
    //   3. Increment the physical page's refcount
    // Page table pages (PML3/PML2/PML1) are freshly allocated for dst.

    constexpr size_t USER_PML4_ENTRIES = 256;
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; i4++) {
        if (!src->entries[i4].present) {
            continue;
        }

        paddr_t src_pml3_phys = src->entries[i4].frame << paging::PAGE_SHIFT;
        auto* src_pml3 = (PageTable*)addr::get_virt_pointer(src_pml3_phys);

        // Allocate a new PML3 for dst
        auto* dst_pml3 = (PageTable*)phys::pageAlloc();
        if (dst_pml3 == nullptr) {
            return false;
        }
        memset(dst_pml3, 0, paging::PAGE_SIZE);

        // Set PML4 entry in dst (copy flags from src)
        dst->entries[i4] = src->entries[i4];
        dst->entries[i4].frame = (paddr_t)addr::get_phys_pointer((vaddr_t)dst_pml3) >> paging::PAGE_SHIFT;

        for (size_t i3 = 0; i3 < 512; i3++) {
            if (!src_pml3->entries[i3].present) {
                continue;
            }

            paddr_t src_pml2_phys = src_pml3->entries[i3].frame << paging::PAGE_SHIFT;
            auto* src_pml2 = (PageTable*)addr::get_virt_pointer(src_pml2_phys);

            auto* dst_pml2 = (PageTable*)phys::pageAlloc();
            if (dst_pml2 == nullptr) {
                return false;
            }
            memset(dst_pml2, 0, paging::PAGE_SIZE);

            dst_pml3->entries[i3] = src_pml3->entries[i3];
            dst_pml3->entries[i3].frame = (paddr_t)addr::get_phys_pointer((vaddr_t)dst_pml2) >> paging::PAGE_SHIFT;
            constexpr size_t PML2_ENTRY_NUMBER = 512;
            for (size_t i2 = 0; i2 < PML2_ENTRY_NUMBER; i2++) {
                if (!src_pml2->entries[i2].present) {
                    continue;
                }
                if (src_pml2->entries[i2].pagesize) {
                    // 2MB huge page - just copy the entry (no COW for huge pages)
                    dst_pml2->entries[i2] = src_pml2->entries[i2];
                    continue;
                }

                paddr_t src_pml1_phys = src_pml2->entries[i2].frame << paging::PAGE_SHIFT;
                auto* src_pml1 = (PageTable*)addr::get_virt_pointer(src_pml1_phys);

                auto* dst_pml1 = (PageTable*)phys::pageAlloc();
                if (dst_pml1 == nullptr) {
                    return false;
                }
                memset(dst_pml1, 0, paging::PAGE_SIZE);

                dst_pml2->entries[i2] = src_pml2->entries[i2];
                dst_pml2->entries[i2].frame = (paddr_t)addr::get_phys_pointer((vaddr_t)dst_pml1) >> paging::PAGE_SHIFT;
                constexpr size_t PML1_ENTRY_NUMBER = 512;
                for (size_t i1 = 0; i1 < PML1_ENTRY_NUMBER; i1++) {
                    if (!src_pml1->entries[i1].present) {
                        continue;
                    }

                    uint64_t raw = pte_raw(src_pml1->entries[i1]);

                    // Mark as read-only + COW in BOTH src and dst
                    raw &= ~paging::PAGE_WRITE;
                    raw |= paging::PAGE_COW;
                    src_pml1->entries[i1] = pte_from_raw(raw);
                    dst_pml1->entries[i1] = pte_from_raw(raw);

                    // Increment refcount on the shared data page
                    paddr_t data_phys = src_pml1->entries[i1].frame << paging::PAGE_SHIFT;
                    void* data_virt = reinterpret_cast<void*>(addr::get_virt_pointer(data_phys));
                    phys::pageRefInc(data_virt);
                }
            }
        }
    }

    // Flush TLB for the source (parent) since we modified its PTEs
    wrcr3(rdcr3());
    return true;
}

}  // namespace ker::mod::mm::virt
