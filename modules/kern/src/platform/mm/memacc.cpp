#include "memacc.hpp"

#include <cstring>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>

namespace ker::mod::mm::memacc {
namespace {
constexpr size_t USER_PML4_ENTRIES = 256;
constexpr uint64_t PAGES_PER_1G = 262144;
constexpr uint64_t PAGES_PER_2M = 512;
constexpr uint64_t PML4_SHIFT = 39;
constexpr uint64_t PML3_SHIFT = 30;
constexpr uint64_t PML2_SHIFT = 21;
constexpr uint64_t PML1_SHIFT = 12;

constexpr uint64_t CODE_REGION_START = 0x400000ULL;
constexpr uint64_t CODE_REGION_END = 0x10000000ULL;
constexpr uint64_t HEAP_REGION_START = 0x10000000ULL;
constexpr uint64_t HEAP_REGION_END = 0x100000000000ULL;
constexpr uint64_t MMAP_REGION_START = 0x100000000000ULL;
constexpr uint64_t MMAP_REGION_END = 0x700000000000ULL;
constexpr uint64_t STACK_REGION_START = 0x7F0000000000ULL;
constexpr uint64_t STACK_REGION_END = 0x800000000000ULL;

auto pte_raw(const paging::PageTableEntry& e) -> uint64_t {
    uint64_t raw = 0;
    std::memcpy(&raw, &e, sizeof(raw));
    return raw;
}

auto table_from_entry(const paging::PageTableEntry& entry) -> paging::PageTable* {
    return reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT));
}

void add_region(UserMemoryBreakdown& stats, uint64_t vaddr, uint64_t page_count) {
    if (vaddr >= MMAP_REGION_START && vaddr < MMAP_REGION_END) {
        stats.mmap_pages += page_count;
    } else if (vaddr >= STACK_REGION_START && vaddr < STACK_REGION_END) {
        stats.stack_pages += page_count;
    } else if (vaddr >= CODE_REGION_START && vaddr < CODE_REGION_END) {
        stats.code_pages += page_count;
    } else if (vaddr >= HEAP_REGION_START && vaddr < HEAP_REGION_END) {
        stats.heap_pages += page_count;
    } else {
        stats.other_pages += page_count;
    }
}

void add_permissions(UserMemoryBreakdown& stats, const paging::PageTableEntry& entry, uint64_t page_count) {
    if (entry.writable != 0) {
        stats.rw_pages += page_count;
    } else if (entry.no_execute == 0) {
        stats.rx_pages += page_count;
    } else {
        stats.ro_pages += page_count;
    }
}

void add_present_leaf(UserMemoryBreakdown& stats, const paging::PageTableEntry& entry, uint64_t vaddr, uint64_t page_count) {
    if (entry.user == 0) {
        return;
    }

    stats.virtual_pages += page_count;
    stats.resident_pages += page_count;
    add_region(stats, vaddr, page_count);
    add_permissions(stats, entry, page_count);

    uint64_t const RAW = pte_raw(entry);
    bool shared = (RAW & paging::PAGE_SHARED) != 0U;
    if (!shared && entry.frame != 0) {
        uint64_t const PHYS = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        auto* virt_page = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS));
        shared = phys::page_ref_get(virt_page) > 1;
    }
    if (shared) {
        stats.shared_pages += page_count;
    }
}

auto is_reserved_leaf(const paging::PageTableEntry& entry) -> bool {
    uint64_t const RAW = pte_raw(entry);
    return entry.present == 0 && (RAW & paging::PAGE_RESERVED) != 0U;
}
}  // namespace

auto collect_user_memory_breakdown(paging::PageTable* page_table) -> UserMemoryBreakdown {
    UserMemoryBreakdown stats{};
    if (page_table == nullptr) {
        return stats;
    }

    stats.page_table_pages = 1;
    for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
        const auto& pml4e = page_table->entries.at(i4);
        if (!pml4e.present) {
            continue;
        }

        stats.page_table_pages++;
        auto* pml3 = table_from_entry(pml4e);
        for (size_t i3 = 0; i3 < pml3->entries.size(); ++i3) {
            const auto& pml3e = pml3->entries.at(i3);
            if (!pml3e.present) {
                continue;
            }
            uint64_t const VADDR_1G = (static_cast<uint64_t>(i4) << PML4_SHIFT) | (static_cast<uint64_t>(i3) << PML3_SHIFT);
            if (pml3e.pagesize != 0) {
                add_present_leaf(stats, pml3e, VADDR_1G, PAGES_PER_1G);
                continue;
            }

            stats.page_table_pages++;
            auto* pml2 = table_from_entry(pml3e);
            for (size_t i2 = 0; i2 < pml2->entries.size(); ++i2) {
                const auto& pml2e = pml2->entries.at(i2);
                if (!pml2e.present) {
                    continue;
                }
                uint64_t const VADDR_2M = VADDR_1G | (static_cast<uint64_t>(i2) << PML2_SHIFT);
                if (pml2e.pagesize != 0) {
                    add_present_leaf(stats, pml2e, VADDR_2M, PAGES_PER_2M);
                    continue;
                }

                stats.page_table_pages++;
                auto* pml1 = table_from_entry(pml2e);
                for (size_t i1 = 0; i1 < pml1->entries.size(); ++i1) {
                    const auto& pte = pml1->entries.at(i1);
                    uint64_t const VADDR = VADDR_2M | (static_cast<uint64_t>(i1) << PML1_SHIFT);
                    if (pte.present != 0) {
                        add_present_leaf(stats, pte, VADDR, 1);
                    } else if (is_reserved_leaf(pte)) {
                        stats.virtual_pages++;
                    }
                }
            }
        }
    }

    return stats;
}

}  // namespace ker::mod::mm::memacc
