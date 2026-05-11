#include "virt.hpp"

#include <extern/limine.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>

#include "platform/sched/task.hpp"
#include "platform/sys/spinlock.hpp"

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "platform/asm/cpu.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/coredump.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "util/hcf.hpp"
namespace ker::mod::mm::virt {

namespace {
using log = ker::mod::dbg::logger<"virt">;

paging::PageTable* kernel_pagemap;
limine_memmap_response* memmap_response;
limine_executable_file_response* kernel_file_response;
limine_executable_address_response* kernel_address_response;

auto pte_raw(const paging::PageTableEntry& e) -> uint64_t {
    uint64_t val = 0;
    std::memcpy(&val, &e, sizeof(val));
    return val;
}

auto pte_from_raw(uint64_t raw) -> paging::PageTableEntry {
    paging::PageTableEntry e{};
    std::memcpy(&e, &raw, sizeof(e));
    return e;
}

auto index_of(const uint64_t VADDR, const int OFFSET) -> uint64_t { return VADDR >> (12 + (9 * (OFFSET - 1))) & 0x1FF; }

auto entry_at(PageTable* table, size_t index) -> PageTableEntry& { return table->entries.at(index); }

auto entry_at(const PageTable* table, size_t index) -> const PageTableEntry& { return table->entries.at(index); }
}  // namespace

constexpr size_t KERNEL_PML4_START = 256;
constexpr size_t KERNEL_PML4_END = 512;

void init(limine_memmap_response* memmap_response_param, limine_executable_file_response* kernel_file_response_param,
          limine_executable_address_response* kernel_address_response_param) {
    memmap_response = memmap_response_param;
    kernel_file_response = kernel_file_response_param;
    kernel_address_response = kernel_address_response_param;
}

void switch_to_kernel_pagemap() { wrcr3(reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<paddr_t>(kernel_pagemap)))); }

auto get_kernel_pagemap() -> PageTable* { return kernel_pagemap; }

auto create_pagemap() -> PageTable* {
    auto* page_table = static_cast<PageTable*>(phys::page_alloc());
    if (page_table != nullptr) {
        std::memset(page_table, 0, paging::PAGE_SIZE);
    }
    return page_table;
}

void copy_kernel_mappings(sched::task::Task* t) {
    for (size_t i = KERNEL_PML4_START; i < KERNEL_PML4_END; i++) {
        entry_at(t->pagemap, i) = entry_at(kernel_pagemap, i);
    }
}

void switch_pagemap(sched::task::Task* t) {
    if (t->pagemap == nullptr) {
        [[unlikely]]
        if (t->name != nullptr) {
            log::critical("task %s has no pagemap", t->name);
        } else {
            log::critical("task has no pagemap; halting");
        }
        hcf();
    }

    auto phys_pagemap = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(t->pagemap)));
#ifdef VERBOSE_PAGEMAP_SWITCH
    log::debug("switchPagemap: task=%s pid=%d virt=0x%x phys=0x%x", (t->name != nullptr) ? t->name : "unknown", t->pid,
               static_cast<unsigned int>(reinterpret_cast<uintptr_t>(t->pagemap)), static_cast<unsigned int>(phys_pagemap));
#endif
    // Always reload CR3 on task switches.
    //
    // Comparing only the physical PML4 frame is unsafe because page-table
    // pages are freed and later reused. If a new address space reuses the same
    // physical PML4 frame as an old one, skipping the CR3 write leaves stale
    // TLB entries from the previous owner alive and can produce impossible
    // user faults on pages whose current PTEs are present and writable.
    wrcr3(phys_pagemap);
}

auto pagefault_handler(uint64_t control_register, gates::InterruptFrame& frame, ker::mod::cpu::GPRegs& gpr) -> bool {
    PageFault const PAGEFAULT = paging::create_page_fault(frame.err_code, true);

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
    if ((PAGEFAULT.writable != 0U) && (control_register < 0x0000800000000000ULL)) {
        // Walk page tables to find the faulting PTE
        auto* current_task = sched::get_current_task();
        if (current_task == nullptr || current_task->pagemap == nullptr) {
            log::error("COW fault: no current task or pagemap");
            hcf();
            return false;
        }

        paging::PageTable const* pml4 = current_task->pagemap;
        uint64_t const VADDR = control_register;

        // Walk the 4 levels to find the PML1 entry
        const uint64_t IDX4 = (VADDR >> 39) & 0x1FF;
        const uint64_t IDX3 = (VADDR >> 30) & 0x1FF;
        const uint64_t IDX2 = (VADDR >> 21) & 0x1FF;
        const uint64_t IDX1 = (VADDR >> 12) & 0x1FF;

        if (!entry_at(pml4, IDX4).present) {
            goto not_cow;
        }
        {
            auto* pml3 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(entry_at(pml4, IDX4).frame << paging::PAGE_SHIFT));
            if (!entry_at(pml3, IDX3).present) {
                goto not_cow;
            }
            auto* pml2 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(entry_at(pml3, IDX3).frame << paging::PAGE_SHIFT));
            if (!entry_at(pml2, IDX2).present) {
                goto not_cow;
            }
            auto* pml1 = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(entry_at(pml2, IDX2).frame << paging::PAGE_SHIFT));

            paging::PageTableEntry& pte = entry_at(pml1, IDX1);
            if (!pte.present) {
                goto not_cow;
            }

            uint64_t raw = pte_raw(pte);
            if ((raw & paging::PAGE_COW) == 0U) {
                goto not_cow;
            }

            // This is a COW page - handle it
            paddr_t const OLD_PHYS = pte.frame << paging::PAGE_SHIFT;
            void* old_virt = reinterpret_cast<void*>(addr::get_virt_pointer(OLD_PHYS));

            uint32_t const REFCOUNT = phys::page_ref_get(old_virt);

            if (REFCOUNT <= 1) {
                // We're the sole owner - just make it writable and clear COW
                raw &= ~paging::PAGE_COW;
                raw |= paging::PAGE_WRITE;
                pte = pte_from_raw(raw);
                invlpg(VADDR);
                return true;
            }

            // Pin old_virt so it cannot be freed by a concurrent COW handler on
            // another CPU between our refcount read and the memcpy below.  Without
            // this, a racing handler could decrement the refcount to 0, freeing
            // and zeroing the page, so our memcpy would copy zeros instead of the
            // real content (causing e.g. RELR relocations to produce garbage VAs).
            phys::page_ref_inc(old_virt);

            // Multiple owners - allocate a new page and copy
            void* new_page = phys::page_alloc(paging::PAGE_SIZE);
            if (new_page == nullptr) {
                phys::page_ref_dec(old_virt);  // release pin
                log::error("COW fault: OOM allocating new page for vaddr 0x%x", VADDR);
                hcf();
                return false;
            }

            std::memcpy(new_page, old_virt, paging::PAGE_SIZE);  // safe: old page is pinned

            // Re-read the PTE: if PAGE_COW is already gone, another CPU handled
            // this fault while we were copying.  Discard our copy; the other
            // handler's mapping is already live.
            uint64_t const RAW_NOW = pte_raw(pte);
            if ((RAW_NOW & paging::PAGE_COW) == 0U) {
                phys::page_ref_dec(new_page);  // discard unused allocation
                phys::page_ref_dec(old_virt);  // release pin only (PTE ref transferred by the other handler)
                return true;
            }

            // Map new page as writable, clear COW
            auto new_phys = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(new_page)));
            raw &= ~paging::PAGE_COW;
            raw |= paging::PAGE_WRITE;
            // Replace frame bits
            raw &= ~(0xFFFFFFFFFFULL << 12);  // clear frame field
            raw |= (new_phys & ~0xFFFULL);    // set new frame
            pte = pte_from_raw(raw);
            invlpg(VADDR);

            // Release pin and our PTE reference to old_virt (two decrements).
            phys::page_ref_dec(old_virt);  // release pin (paired with pageRefInc above)
            phys::page_ref_dec(old_virt);  // release PTE reference (old_virt no longer mapped here)
            return true;
        }
    }

not_cow:
    // Not a COW fault - let the caller handle it (userspace crash / kernel panic).
    return false;
}

paddr_t translate(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        log::critical("translate: no page table");
        hcf();
    }

    PageTable const* table = page_table;
    for (int i = 4; i > 1; i--) {
        const auto& entry = entry_at(table, index_of(vaddr, i));
        if (!entry.present) {
            return PADDR_INVALID;
        }
        uint64_t const PHYS = entry.frame << paging::PAGE_SHIFT;
        table = reinterpret_cast<PageTable*>(PHYS + 0xffff800000000000ULL);  // Use HHDM for page table walk
    }

    const auto& pte = entry_at(table, index_of(vaddr, 1));
    if (!pte.present) {
        return PADDR_INVALID;
    }
    uint64_t const PHYS = (pte.frame << paging::PAGE_SHIFT) + (vaddr & (paging::PAGE_SIZE - 1));
    return PHYS;  // Return physical address only
}
void init_pagemap() {
    cpu::enable_pae();
    cpu::enable_pse();
    kernel_pagemap = static_cast<PageTable*>(phys::page_alloc());
    if (kernel_pagemap == nullptr) {
        // PANIC!
        log::critical("init: failed to allocate kernel pagemap in init_pagemap");
        hcf();
    }
    log::info("kernel pagemap allocated at %x", kernel_pagemap);
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
        log::debug("memory map entry %d: %x - %x (%s)", i, entry->base, entry->base + entry->length, type_str);
    }

    size_t total_pages_mapped = 0;
    for (size_t i = 0; i < memmap_response->entry_count; i++) {
        auto* entry = memmap_response->entries[i];
        size_t const NUM_PAGES = entry->length / paging::PAGE_SIZE;

        for (size_t j = 0; j < NUM_PAGES; j++) {
            auto vaddr = reinterpret_cast<paddr_t>(addr::get_virt_pointer(entry->base + (j * paging::PAGE_SIZE)));
            map_page(kernel_pagemap,
                     vaddr,                                                          // virtual address
                     entry->base + (j * paging::PAGE_SIZE),                          // physical address
                     entry->type == LIMINE_MEMMAP_RESERVED                           // reserved memory
                             || entry->type == LIMINE_MEMMAP_BAD_MEMORY              // bad memory
                             || entry->type == LIMINE_MEMMAP_EXECUTABLE_AND_MODULES  // kernel and modules
                         ? paging::page_types::READONLY                              // becomes read-only
                         : paging::page_types::KERNEL                                // otherwise kernel memory
            );
            total_pages_mapped++;
        }
    }
    log::info("mapped total of %zu pages from memory map", total_pages_mapped);

    for (size_t i = 0; i <= kernel_file_response->executable_file->size; i++) {
        vaddr_t const VADDR = kernel_address_response->virtual_base + (i * paging::PAGE_SIZE);
        map_page(kernel_pagemap,
                 VADDR,                                                             // virtual address
                 kernel_address_response->physical_base + (i * paging::PAGE_SIZE),  // physical address
                 paging::page_types::KERNEL                                         // kernel memory
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
            PageTableEntry const& pml4e = entry_at(kernel_pagemap, pml4i);
            if (!pml4e.present) {
                continue;
            }

            paddr_t const PML3_PHYS = pml4e.frame << paging::PAGE_SHIFT;
            auto pml3_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML3_PHYS));
            if (!is_page_mapped(kernel_pagemap, pml3_virt)) {
                map_page(kernel_pagemap, pml3_virt, PML3_PHYS, paging::page_types::KERNEL);
                pt_pages_mappped++;
                mapped_this_round++;
                made_progress = true;
            }

            auto* pml3 = reinterpret_cast<PageTable*>(pml3_virt);
            for (auto& pml3e : pml3->entries) {
                if (!pml3e.present) {
                    continue;
                }

                paddr_t const PML2_PHYS = pml3e.frame << paging::PAGE_SHIFT;
                auto pml2_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML2_PHYS));
                if (!is_page_mapped(kernel_pagemap, pml2_virt)) {
                    map_page(kernel_pagemap, pml2_virt, PML2_PHYS, paging::page_types::KERNEL);
                    pt_pages_mappped++;
                    mapped_this_round++;
                    made_progress = true;
                }

                auto* pml2 = reinterpret_cast<PageTable*>(pml2_virt);
                for (auto& pml2e : pml2->entries) {
                    if (!pml2e.present) {
                        continue;
                    }

                    paddr_t const PML1_PHYS = pml2e.frame << paging::PAGE_SHIFT;

                    auto pml1_virt = reinterpret_cast<vaddr_t>(addr::get_virt_pointer(PML1_PHYS));
                    if (!is_page_mapped(kernel_pagemap, pml1_virt)) {
                        map_page(kernel_pagemap, pml1_virt, PML1_PHYS, paging::page_types::KERNEL);
                        pt_pages_mappped++;
                        mapped_this_round++;
                        made_progress = true;
                    }
                }
            }
        }

        if (mapped_this_round > 0) {
            log::debug("page table fixup iteration %d: mapped %d pages", iteration, mapped_this_round);
        }
    }
    log::info("total page table pages mapped into HHDM: %d (in %d iterations)", pt_pages_mappped, iteration);

    // CRITICAL: Clear PML4[0] to ensure null pointer dereferences cause page faults!
    // Limine may leave identity mappings in the lower half which mask null pointer bugs.
    entry_at(kernel_pagemap, 0) = PageTableEntry{};
    log::info("cleared PML4[0] - null derefs will now fault");

    switch_to_kernel_pagemap();
}

namespace {

// advance page table by level pages
auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags) -> paging::PageTable* {
    PageTableEntry entry = entry_at(page_table, level);
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
        if (((flags & paging::PAGE_NX) == 0U) && entry.no_execute) {
            entry.no_execute = 0;
            auto* raw_entry = reinterpret_cast<uint64_t*>(&entry);
            constexpr uint64_t NX_MASK = ~(1ULL << 63);
            *raw_entry &= NX_MASK;  // Clear NX bit
            changed = true;
        }

        if (changed) {
            entry_at(page_table, level) = entry;
            // Force full TLB flush
            wrcr3(rdcr3());
        }

        return reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    void* page_virt = phys::page_alloc();
    if (page_virt == nullptr) {
        // PANIC!
        log::critical("init: failed to allocate kernel page table in advance_page_table");
        hcf();
    }

    auto page_phys = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(page_virt)));

    std::memset(page_virt, 0, paging::PAGE_SIZE);

    entry_at(page_table, level) = paging::create_page_table_entry(page_phys, flags);
    return static_cast<PageTable*>(page_virt);
}

}  // namespace

/*
 * Map a page in the page table
 *
 * @param pageTable - the page table to map the page in
 * @param vaddr - the virtual address to map the page to
 * @param paddr - the physical address to map the page to
 * @param flags - the flags to set for the page
 */
void map_page(PageTable* page_table, const vaddr_t VADDR, const paddr_t PADDR, const uint64_t FLAGS) {
    if ((page_table == nullptr) || FLAGS == 0) {
        // PANIC!
        log::critical("init: failed to map page in map_page args=<vaddr: %p, paddr: %p, flags: %x>", VADDR, PADDR, FLAGS);
        hcf();
    }

    // PageTable* table = pageTable;
    // for (int i = 4; i > 1; i--) {
    //     table = advancePageTable(table, index_of(vaddr, i), flags);
    // }
    paging::PageTable* pml3 = nullptr;
    paging::PageTable* pml2 = nullptr;
    paging::PageTable* pml1 = nullptr;
    pml3 = advance_page_table(page_table, index_of(VADDR, 4), FLAGS);
    pml2 = advance_page_table(pml3, index_of(VADDR, 3), FLAGS);
    pml1 = advance_page_table(pml2, index_of(VADDR, 2), FLAGS);

    entry_at(pml1, index_of(VADDR, 1)) = paging::create_page_table_entry(PADDR, FLAGS);

    invlpg(VADDR);
}

auto is_page_mapped(PageTable* page_table, vaddr_t vaddr) -> bool {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in is_page_mapped");
        hcf();
    }

    PageTable const* table = page_table;
    for (int i = 4; i > 1; i--) {
        // Check if the entry is present before following the pointer
        const auto& entry = entry_at(table, index_of(vaddr, i));
        if (!entry.present) {
            return false;
        }
        table = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }

    return entry_at(table, index_of(vaddr, 1)).present;
}

void unify_page_flags(PageTable* page_table, vaddr_t vaddr, uint64_t flags) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unify_page_flags");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), flags);
    }

    // Get the current page table entry by computing the index first
    uint64_t const IDX = index_of(vaddr, 1);
    PageTableEntry& entry = entry_at(table, IDX);

    if (entry.present == 0) {
        // Page doesn't exist, nothing to modify
        return;
    }

    // Update page table entry flags
    entry.present = (flags & paging::PAGE_PRESENT) != 0U ? 1 : 0;
    entry.writable = (flags & paging::PAGE_WRITE) != 0U ? 1 : 0;
    entry.user = (flags & paging::PAGE_USER) != 0U ? 1 : 0;
    entry.no_execute = (flags & paging::PAGE_NX) != 0U ? 1 : 0;

    auto* raw_entry = reinterpret_cast<uint64_t*>(&entry);
    constexpr uint64_t NX_BIT_POSITION = 63ULL;
    if ((flags & paging::PAGE_NX) != 0U) {
        *raw_entry |= (1ULL << NX_BIT_POSITION);  // Set NX bit
    } else {
        *raw_entry &= ~(1ULL << NX_BIT_POSITION);  // Clear NX bit
    }

    invlpg(vaddr);

#ifdef ELF_DEBUG
    if (vaddr >= 0x501000 && vaddr < 0x580000) {
        log::debug("unify_page_flags: vaddr=0x%x, flags=0x%x, entry_after=0x%x, nx=%d present=%d", vaddr, flags, *raw_entry,
                   static_cast<int>((*raw_entry >> NX_BIT_POSITION) & 1U), static_cast<int>(entry.present));
    }
#endif
}

void unmap_page(PageTable* page_table, vaddr_t vaddr) {
    if (page_table == nullptr) {
        // PANIC!
        log::critical("init: failed to get page table in unmap_page");
        hcf();
    }

    PageTable* table = page_table;
    for (int i = 4; i > 1; i--) {
        table = advance_page_table(table, index_of(vaddr, i), 0);
    }

    uint64_t const FRAME = entry_at(table, index_of(vaddr, 1)).frame;
    entry_at(table, index_of(vaddr, 1)) = paging::purge_page_table_entry();

    invlpg(vaddr);

    // Convert frame number to physical address, then to HHDM pointer for refcount-aware free
    if (FRAME != 0) {
        uint64_t const PHYS_ADDR = FRAME << paging::PAGE_SHIFT;
        void* virt_ptr = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS_ADDR));
        phys::page_ref_dec(virt_ptr);
    }
}

void map_range(PageTable* page_table, Range range, uint64_t flags, uint64_t offset) {
    auto [start, end] = range;
    if (((start % paging::PAGE_SIZE) != 0U) || ((end % paging::PAGE_SIZE) != 0U) || start >= end) {
        // PANIC!
        log::critical("init: failed to map range");
        hcf();
    }

    while (start != end) {
        map_page(page_table, start + offset, start, flags);
        start += paging::PAGE_SIZE;
    }
}

void map_to_kernel_page_table(vaddr_t vaddr, paddr_t paddr, uint64_t flags) { map_page(kernel_pagemap, vaddr, paddr, flags); }

void map_range_to_kernel_page_table(Range range, uint64_t flags, uint64_t offset) { map_range(kernel_pagemap, range, flags, offset); }

void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    // no offset assume hhdm
    map_range(kernel_pagemap, range, flags, addr::get_hhdm_offset());
}

namespace {

// x86-64 canonical address check: bits[63:47] must all be the same.
// For kernel HHDM addresses (bit 47 set) the upper 17 bits are all 1s.
constexpr int CANONICAL_SIGN_BIT = 47;
constexpr uint64_t CANONICAL_KERNEL_UPPER = 0x1ffffULL;

auto phys_is_in_ram(uint64_t phys_addr) -> bool {
    for (auto* zone = phys::get_zones(); zone != nullptr; zone = zone->next) {
        auto zone_start_phys = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(zone->start)));
        if (phys_addr >= zone_start_phys && phys_addr < zone_start_phys + zone->len) {
            return true;
        }
    }
    return false;
}

auto phys_to_hhdm_checked(uint64_t phys_addr) -> void* {
    const uint64_t PAGE_BASE = phys_addr & ~(paging::PAGE_SIZE - 1);
    if (!phys_is_in_ram(PAGE_BASE)) {
        return nullptr;
    }

    const uint64_t VIRT_RAW = PAGE_BASE + addr::get_hhdm_offset();
    if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
        return nullptr;
    }

    if (phys::page_ref_get(reinterpret_cast<void*>(VIRT_RAW)) == 0) {
        return nullptr;
    }

    return reinterpret_cast<void*>(VIRT_RAW);
}

auto page_table_tree_contains_frame(PageTable* root, uint64_t target_phys) -> bool {
    if (root == nullptr) {
        return false;
    }

    target_phys &= ~(paging::PAGE_SIZE - 1);
    if (reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(root))) == target_phys) {
        return true;
    }

    constexpr size_t USER_PML4_ENTRIES = 256;
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = entry_at(root, i4);
        if (!pml4e.present) {
            continue;
        }

        const uint64_t PML3_PHYS = static_cast<uint64_t>(pml4e.frame) << paging::PAGE_SHIFT;
        if (PML3_PHYS == target_phys) {
            return true;
        }

        auto* pml3 = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PML3_PHYS));
        if (pml3 == nullptr) {
            continue;
        }
        for (auto pml3e : pml3->entries) {
            if (!pml3e.present || pml3e.pagesize) {
                continue;
            }

            const uint64_t PML2_PHYS = static_cast<uint64_t>(pml3e.frame) << paging::PAGE_SHIFT;
            if (PML2_PHYS == target_phys) {
                return true;
            }

            auto* pml2 = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(PML2_PHYS));
            if (pml2 == nullptr) {
                continue;
            }
            for (auto pml2e : pml2->entries) {
                if (!pml2e.present || pml2e.pagesize) {
                    continue;
                }

                const uint64_t PML1_PHYS = static_cast<uint64_t>(pml2e.frame) << paging::PAGE_SHIFT;
                if (PML1_PHYS == target_phys) {
                    return true;
                }
            }
        }
    }

    return false;
}

auto frame_is_live_medium_alloc(uint64_t phys_addr) -> bool {
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
    auto* page_ptr = phys_to_hhdm_checked(phys_addr);
    if (page_ptr == nullptr) {
        return false;
    }
    const auto PAGE_BASE = reinterpret_cast<uint64_t>(page_ptr);
    return *reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == MEDIUM_ALLOC_MAGIC;
}

constexpr bool ENABLE_WATCHED_MMAP_LOGS = false;
constexpr uint64_t WATCH_MMAP_VADDR = 0x00001000007da000ULL;

auto is_watched_mmap_vaddr(uint64_t vaddr) -> bool { return ENABLE_WATCHED_MMAP_LOGS && vaddr == WATCH_MMAP_VADDR; }

auto collect_user_vaddrs_for_phys(PageTable* table, int level, uint64_t vaddr_base, uint64_t target_phys, uint64_t* out_vaddrs,
                                  size_t max_hits) -> size_t {
    if (table == nullptr || level < 1 || max_hits == 0) {
        return 0;
    }

    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));
    size_t hits = 0;

    for (size_t i = 0; i < MAX_ENTRY && hits < max_hits; ++i) {
        const auto& entry = entry_at(table, i);
        if (!entry.present) {
            continue;
        }

        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);
        const uint64_t ENTRY_PHYS = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (ENTRY_PHYS == 0) {
            continue;
        }

        if (level == 1) {
            if ((ENTRY_PHYS & ~(paging::PAGE_SIZE - 1)) == target_phys) {
                out_vaddrs[hits++] = ENTRY_VADDR;
            }
            continue;
        }

        if (entry.pagesize != 0) {
            continue;
        }

        auto* next_level = reinterpret_cast<PageTable*>(phys_to_hhdm_checked(ENTRY_PHYS));
        if (next_level == nullptr) {
            continue;
        }

        hits += collect_user_vaddrs_for_phys(next_level, level - 1, ENTRY_VADDR, target_phys, out_vaddrs + hits, max_hits - hits);
    }

    return hits;
}

auto mark_phys_rmap_reported(uint64_t target_phys) -> bool {
    static mod::sys::Spinlock lock;
    static std::array<uint64_t, 64> reported_phys = {};

    target_phys &= ~(paging::PAGE_SIZE - 1);
    const uint64_t FLAGS = lock.lock_irqsave();
    for (uint64_t const PHYS : reported_phys) {
        if (PHYS == target_phys) {
            lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (auto& phys : reported_phys) {
        if (phys == 0) {
            phys = target_phys;
            lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    reported_phys.at(target_phys % reported_phys.size()) = target_phys;
    lock.unlock_irqrestore(FLAGS);
    return true;
}

auto log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid, const char* owner_name, bool log_when_empty)
    -> bool {
    if (!mark_phys_rmap_reported(target_phys)) {
        return false;
    }

    constexpr size_t MAX_HITS_PER_TASK = 4;
    constexpr size_t MAX_LOGGED_TASKS = 12;
    uint32_t const ACTIVE_COUNT = sched::get_active_task_count();
    size_t logged_tasks = 0;
    size_t total_hits = 0;

    log::warn("phys-rmap: scanning tasks for phys=0x%llx trigger=%s pid=%lu name=%s", static_cast<unsigned long long>(target_phys),
              trigger != nullptr ? trigger : "?", owner_pid, owner_name != nullptr ? owner_name : "?");

    for (uint32_t i = 0; i < ACTIVE_COUNT && logged_tasks < MAX_LOGGED_TASKS; ++i) {
        auto* task = sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }

        if (task->pagemap != nullptr && task->type != sched::task::TaskType::DAEMON) {
            std::array<uint64_t, MAX_HITS_PER_TASK> hits{};
            const size_t FOUND =
                collect_user_vaddrs_for_phys(task->pagemap, 4, 0, target_phys & ~(paging::PAGE_SIZE - 1), hits.data(), hits.size());
            if (FOUND > 0) {
                total_hits += FOUND;
                ++logged_tasks;
                log::warn("phys-rmap: phys=0x%llx pid=%lu name=%s pagemap=%p hit0=0x%llx hit1=0x%llx hit2=0x%llx hit3=0x%llx",
                          static_cast<unsigned long long>(target_phys), task->pid, task->name != nullptr ? task->name : "?",
                          static_cast<void*>(task->pagemap), static_cast<unsigned long long>(hits.at(0)),
                          static_cast<unsigned long long>(hits.at(1)), static_cast<unsigned long long>(hits.at(2)),
                          static_cast<unsigned long long>(hits.at(3)));
            }
        }

        task->release();
    }

    for (uint64_t cpu_no = 0; cpu_no < smt::get_core_count() && logged_tasks < MAX_LOGGED_TASKS; ++cpu_no) {
        const size_t DEAD_COUNT = sched::get_dead_task_count(cpu_no);
        for (size_t idx = 0; idx < DEAD_COUNT && logged_tasks < MAX_LOGGED_TASKS; ++idx) {
            auto* task = sched::get_dead_task_at_safe(cpu_no, idx);
            if (task == nullptr) {
                continue;
            }

            if (task->pagemap != nullptr && task->type != sched::task::TaskType::DAEMON) {
                std::array<uint64_t, MAX_HITS_PER_TASK> hits{};
                const size_t FOUND =
                    collect_user_vaddrs_for_phys(task->pagemap, 4, 0, target_phys & ~(paging::PAGE_SIZE - 1), hits.data(), hits.size());
                if (FOUND > 0) {
                    total_hits += FOUND;
                    ++logged_tasks;
                    log::warn(
                        "phys-rmap: phys=0x%llx dead_cpu=%llu pid=%lu name=%s pagemap=%p hit0=0x%llx hit1=0x%llx hit2=0x%llx hit3=0x%llx",
                        static_cast<unsigned long long>(target_phys), static_cast<unsigned long long>(cpu_no), task->pid,
                        task->name != nullptr ? task->name : "?", static_cast<void*>(task->pagemap),
                        static_cast<unsigned long long>(hits.at(0)), static_cast<unsigned long long>(hits.at(1)),
                        static_cast<unsigned long long>(hits.at(2)), static_cast<unsigned long long>(hits.at(3)));
                }
            }

            task->release();
        }
    }

    if (logged_tasks == 0) {
        if (log_when_empty) {
            log::warn("phys-rmap: phys=0x%llx has no other active or dead user mappings", static_cast<unsigned long long>(target_phys));
        }
        return false;
    }

    log::warn("phys-rmap: phys=0x%llx logged_tasks=%zu total_hits=%zu", static_cast<unsigned long long>(target_phys), logged_tasks,
              total_hits);
    return true;
}

}  // namespace

bool debug_log_user_phys_mappings(uint64_t target_phys, const char* trigger, uint64_t owner_pid, const char* owner_name,
                                  bool log_when_empty) {
    return log_user_phys_mappings(target_phys, trigger, owner_pid, owner_name, log_when_empty);
}

namespace {

void free_user_data_pages(PageTable* table, int level, PageTable* root, uint64_t vaddr_base = 0, uint64_t owner_pid = 0,
                          const char* owner_name = nullptr, const char* reason = nullptr) {
    if (table == nullptr || level < 1) {
        return;
    }

    // For user space, only process entries 0-255 at PML4 level
    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;

    // Number of VA bits contributed by this level's index field
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));

    for (size_t i = 0; i < MAX_ENTRY; i++) {
        auto& entry = entry_at(table, i);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        // Reconstruct virtual address for diagnostic output.
        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);

        if (level > 1) {
            // Check for huge pages (2MB at level 2, 1GB at level 3)
            if (entry.pagesize != 0) {
                // Preserve existing behavior: huge user mappings are not currently reclaimed here.
            } else if (frame_is_live_medium_alloc(PHYS_ADDR)) {
                entry = paging::purge_page_table_entry();
            } else {
                uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
                if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                    log::warn(
                        "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx "
                        "virt=0x%llx "
                        "corrupt non-canonical next-level pointer",
                        owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root),
                        level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                        static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                    entry = paging::purge_page_table_entry();
                    continue;
                }
                auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
                free_user_data_pages(next_level, level - 1, root, ENTRY_VADDR, owner_pid, owner_name, reason);
            }
        } else {
            // Level 1 (PML1) - entries point to actual data pages
            //
            // Some corrupted/recursive user mappings can point back at this
            // address space's own page-table frames. Freeing such a frame as
            // data lets another CPU reuse it before the parent page-table
            // cleanup reaches the same frame, which turns into a page_free()
            // of a live kmalloc page. Keep page-table frames owned by the
            // page-table cleanup phase below.
            if (!page_table_tree_contains_frame(root, PHYS_ADDR) && !frame_is_live_medium_alloc(PHYS_ADDR)) {
                uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
                if ((VIRT_RAW >> CANONICAL_SIGN_BIT) == CANONICAL_KERNEL_UPPER) {
                    void* page_virt = reinterpret_cast<void*>(VIRT_RAW);
                    // Root-cause sentinel: a corrupt PTE whose frame happens to land on a live
                    // slab page is the chain that causes the slab UAF crash.  Detect and skip
                    // the refDec here so the slab page is never freed out from under the chain.
                    constexpr uint32_t SLAB_MAGIC = 0x8CBEEFC8;
                    if (*reinterpret_cast<const uint32_t*>(VIRT_RAW) == SLAB_MAGIC) {
                        log::warn(
                            "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx "
                            "phys=0x%llx hits live slab; "
                            "skipping refDec",
                            owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root),
                            level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                            static_cast<unsigned long long>(PHYS_ADDR));
                        log_user_phys_mappings(PHYS_ADDR, reason, owner_pid, owner_name, true);
                        entry = paging::purge_page_table_entry();
                        continue;
                    }
                    phys::page_ref_dec(page_virt);
                } else {
                    log::warn(
                        "free_user_data_pages: pid=%lu name=%s reason=%s pagemap=%p level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx "
                        "virt=0x%llx "
                        "non-canonical leaf frame; skipping refDec",
                        owner_pid, owner_name != nullptr ? owner_name : "?", reason != nullptr ? reason : "?", static_cast<void*>(root),
                        level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                        static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                }
            }
            entry = paging::purge_page_table_entry();
        }
    }
}

// Helper to free a page table level recursively after user data pages are gone.
// level: 4=PML4, 3=PML3, 2=PML2, 1=PML1
// vaddr_base: accumulated virtual address bits from outer levels (for diagnostics)
void free_page_table_pages(PageTable* table, int level, uint64_t vaddr_base = 0) {
    if (table == nullptr || level < 1) {
        return;
    }

    // For user space, only process entries 0-255 at PML4 level
    const size_t MAX_ENTRY = (level == 4) ? 256 : 512;

    // Number of VA bits contributed by this level's index field
    const int LEVEL_SHIFT = 12 + (9 * (level - 1));

    for (size_t i = 0; i < MAX_ENTRY; i++) {
        auto& entry = entry_at(table, i);
        if (entry.present == 0) {
            continue;
        }

        uint64_t const PHYS_ADDR = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        if (PHYS_ADDR == 0) {
            continue;
        }

        // Reconstruct virtual address for diagnostic output.
        const uint64_t ENTRY_VADDR = vaddr_base | (static_cast<uint64_t>(i) << LEVEL_SHIFT);

        if (level > 1 && entry.pagesize == 0 && !frame_is_live_medium_alloc(PHYS_ADDR)) {
            uint64_t const VIRT_RAW = PHYS_ADDR + addr::get_hhdm_offset();
            if ((VIRT_RAW >> CANONICAL_SIGN_BIT) != CANONICAL_KERNEL_UPPER) {
                log::warn(
                    "corrupt PTE in free_page_table_pages: level=%d i=%zu vaddr=0x%llx frame=0x%llx phys=0x%llx virt=0x%llx, "
                    "clearing",
                    level, i, static_cast<unsigned long long>(ENTRY_VADDR), static_cast<unsigned long long>(entry.frame),
                    static_cast<unsigned long long>(PHYS_ADDR), static_cast<unsigned long long>(VIRT_RAW));
                entry = paging::purge_page_table_entry();
                continue;
            }
            auto* next_level = reinterpret_cast<PageTable*>(VIRT_RAW);
            free_page_table_pages(next_level, level - 1, ENTRY_VADDR);
            phys::page_ref_dec(next_level);
        }

        entry = paging::purge_page_table_entry();
    }
}

}  // namespace

void destroy_user_space(PageTable* pagemap, uint64_t owner_pid, const char* owner_name, const char* reason) {
    if (pagemap == nullptr) {
        return;
    }
#ifdef ELF_DEBUG
    if (owner_pid != 0) {
        log::trace("destroyUserSpace: begin pid=%lu name=%s reason=%s pagemap=%p", owner_pid, owner_name != nullptr ? owner_name : "?",
                   reason != nullptr ? reason : "?", static_cast<void*>(pagemap));
    }
#endif
    // Free all user-space mappings (PML4 entries 0-255). Keep data-page
    // reclamation separate from page-table-page reclamation so a user PTE that
    // aliases a page-table frame cannot free that frame before the page-table
    // walk reaches it.
    free_user_data_pages(pagemap, 4, pagemap, 0, owner_pid, owner_name, reason);
    free_page_table_pages(pagemap, 4);

    // Invalidate TLB for this address space
    wrcr3(rdcr3());
#ifdef ELF_DEBUG
    if (owner_pid != 0) {
        log::trace("destroyUserSpace: end pid=%lu reason=%s pagemap=%p", owner_pid, reason != nullptr ? reason : "?",
                   static_cast<void*>(pagemap));
    }
#endif
}

auto deep_copy_user_pagemap_cow(PageTable* src, PageTable* dst) -> bool {
    // Walk the user half (PML4[0..255]) of src.
    // For each present PML1 entry pointing to a data page:
    //   1. Mark the src PTE as read-only + COW
    //   2. Create the same PTE in dst (read-only + COW)
    //   3. Increment the physical page's refcount
    // Page table pages (PML3/PML2/PML1) are freshly allocated for dst.

    constexpr size_t USER_PML4_ENTRIES = 256;
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; i4++) {
        auto& src_pml4e = entry_at(src, i4);
        auto& dst_pml4e = entry_at(dst, i4);
        if (!src_pml4e.present) {
            continue;
        }

        paddr_t const SRC_PML3_PHYS = src_pml4e.frame << paging::PAGE_SHIFT;
        auto* src_pml3 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML3_PHYS));

        // Allocate a new PML3 for dst
        auto* dst_pml3 = static_cast<PageTable*>(phys::page_alloc());
        if (dst_pml3 == nullptr) {
            return false;
        }
        std::memset(dst_pml3, 0, paging::PAGE_SIZE);

        // Set PML4 entry in dst (copy flags from src)
        dst_pml4e = src_pml4e;
        dst_pml4e.frame = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml3))) >> paging::PAGE_SHIFT;

        for (size_t i3 = 0; i3 < 512; i3++) {
            auto& src_pml3e = entry_at(src_pml3, i3);
            auto& dst_pml3e = entry_at(dst_pml3, i3);
            if (!src_pml3e.present) {
                continue;
            }

            paddr_t const SRC_PML2_PHYS = src_pml3e.frame << paging::PAGE_SHIFT;
            auto* src_pml2 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML2_PHYS));

            auto* dst_pml2 = static_cast<PageTable*>(phys::page_alloc());
            if (dst_pml2 == nullptr) {
                return false;
            }
            std::memset(dst_pml2, 0, paging::PAGE_SIZE);

            dst_pml3e = src_pml3e;
            dst_pml3e.frame = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml2))) >> paging::PAGE_SHIFT;
            constexpr size_t PML2_ENTRY_NUMBER = 512;
            for (size_t i2 = 0; i2 < PML2_ENTRY_NUMBER; i2++) {
                auto& src_pml2e = entry_at(src_pml2, i2);
                auto& dst_pml2e = entry_at(dst_pml2, i2);
                if (!src_pml2e.present) {
                    continue;
                }
                if (src_pml2e.pagesize) {
                    // 2MB huge page: copy as 512 independent 4KB pages into a new PML1.
                    // Cannot use pageAllocHuge because huge_page_base may not be 2MB-aligned;
                    // an unaligned PA in a PS=1 PML2 entry is invalid and causes a GPF.
                    // Each sub-page must be an independent pageAlloc so free_user_data_pages
                    // can page_ref_dec each PTE individually — a multi-page buddy block has only
                    // one head page and the CONT pages cannot be freed individually.
                    paddr_t const SRC_PHYS = src_pml2e.frame << paging::PAGE_SHIFT;
                    auto* src_virt = reinterpret_cast<uint8_t*>(addr::get_virt_pointer(SRC_PHYS));

                    auto* dst_pml1 = static_cast<PageTable*>(phys::page_alloc());
                    if (dst_pml1 == nullptr) {
                        return false;
                    }
                    std::memset(dst_pml1, 0, paging::PAGE_SIZE);

                    // Derive per-PTE flags from the source PML2 huge-page entry:
                    // clear PS bit (bit 7) and frame bits; preserve present/write/user/NX.
                    constexpr uint64_t PS_BIT = (1ULL << 7);
                    constexpr uint64_t FRAME_MASK = 0x000FFFFFFFFFF000ULL;
                    uint64_t const PDE_RAW_VAL = pte_raw(src_pml2e);
                    uint64_t const PTE_FLAGS = PDE_RAW_VAL & ~(FRAME_MASK | PS_BIT);

                    for (size_t i1 = 0; i1 < PML2_ENTRY_NUMBER; i1++) {
                        auto* sub = phys::page_alloc(paging::PAGE_SIZE, "cow_huge_sub");
                        if (sub == nullptr) {
                            // OOM: free already-allocated sub-pages and pml1
                            for (size_t j = 0; j < i1; j++) {
                                paddr_t const SUB_PA = static_cast<paddr_t>(entry_at(dst_pml1, j).frame) << paging::PAGE_SHIFT;
                                phys::page_free(reinterpret_cast<void*>(addr::get_virt_pointer(SUB_PA)));
                            }
                            phys::page_free(dst_pml1);
                            return false;
                        }
                        std::memcpy(sub, src_virt + (i1 * paging::PAGE_SIZE), paging::PAGE_SIZE);
                        auto const SUB_PHYS = reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(sub)));
                        entry_at(dst_pml1, i1) = pte_from_raw(PTE_FLAGS | SUB_PHYS);
                    }

                    dst_pml2e = src_pml2e;
                    dst_pml2e.pagesize = 0;
                    dst_pml2e.frame =
                        reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml1))) >> paging::PAGE_SHIFT;
                    continue;
                }

                paddr_t const SRC_PML1_PHYS = src_pml2e.frame << paging::PAGE_SHIFT;
                auto* src_pml1 = reinterpret_cast<PageTable*>(addr::get_virt_pointer(SRC_PML1_PHYS));

                auto* dst_pml1 = static_cast<PageTable*>(phys::page_alloc());
                if (dst_pml1 == nullptr) {
                    return false;
                }
                std::memset(dst_pml1, 0, paging::PAGE_SIZE);

                dst_pml2e = src_pml2e;
                dst_pml2e.frame =
                    reinterpret_cast<paddr_t>(addr::get_phys_pointer(reinterpret_cast<vaddr_t>(dst_pml1))) >> paging::PAGE_SHIFT;
                constexpr size_t PML1_ENTRY_NUMBER = 512;
                for (size_t i1 = 0; i1 < PML1_ENTRY_NUMBER; i1++) {
                    auto& src_pml1e = entry_at(src_pml1, i1);
                    auto& dst_pml1e = entry_at(dst_pml1, i1);
                    if (!src_pml1e.present) {
                        continue;
                    }

                    const uint64_t VADDR = (static_cast<uint64_t>(i4) << 39) | (static_cast<uint64_t>(i3) << 30) |
                                           (static_cast<uint64_t>(i2) << 21) | (static_cast<uint64_t>(i1) << 12);
                    uint64_t raw = pte_raw(src_pml1e);
                    const bool WAS_WRITABLE = (raw & paging::PAGE_WRITE) != 0U;
                    const bool IS_SHARED = (raw & paging::PAGE_SHARED) != 0U;

                    // Only writable mappings need COW. Shared read-only mappings
                    // (text/debug metadata) can stay read-only in both address
                    // spaces and simply share the backing page.
                    if (WAS_WRITABLE && !IS_SHARED) {
                        raw &= ~paging::PAGE_WRITE;
                        raw |= paging::PAGE_COW;
                        src_pml1e = pte_from_raw(raw);
                        dst_pml1e = pte_from_raw(raw);
                    } else {
                        dst_pml1e = src_pml1e;
                    }

                    // Increment refcount on the shared data page
                    paddr_t const DATA_PHYS = src_pml1e.frame << paging::PAGE_SHIFT;
                    void* data_virt = reinterpret_cast<void*>(addr::get_virt_pointer(DATA_PHYS));
                    phys::page_ref_inc(data_virt);
                    if (is_watched_mmap_vaddr(VADDR)) {
                        log::warn("watch mmap-cow: src=%p dst=%p vaddr=0x%llx phys=0x%llx writable=%u cow=%u ref=%llu",
                                  static_cast<void*>(src), static_cast<void*>(dst), static_cast<unsigned long long>(VADDR),
                                  static_cast<unsigned long long>(DATA_PHYS), WAS_WRITABLE ? 1U : 0U,
                                  (raw & paging::PAGE_COW) != 0U ? 1U : 0U, static_cast<unsigned long long>(phys::page_ref_get(data_virt)));
                    }
                }
            }
        }
    }

    // Flush TLB for the source (parent) since we modified its PTEs
    wrcr3(rdcr3());
    return true;
}

}  // namespace ker::mod::mm::virt
