#include "memacc.hpp"

#include <cstdint>
#include <cstring>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/user_layout.hpp>

namespace ker::mod::mm::memacc {
namespace {
constexpr size_t USER_PML4_ENTRIES = 256;
constexpr uint64_t PAGES_PER_1G = 262144;
constexpr uint64_t PAGES_PER_2M = 512;
constexpr uint64_t PML4_SHIFT = 39;
constexpr uint64_t PML3_SHIFT = 30;
constexpr uint64_t PML2_SHIFT = 21;
constexpr uint64_t PML1_SHIFT = 12;

auto pte_raw(const paging::PageTableEntry& e) -> uint64_t {
    uint64_t raw = 0;
    std::memcpy(&raw, &e, sizeof(raw));
    return raw;
}

auto table_from_entry(const paging::PageTableEntry& entry) -> paging::PageTable* {
    return reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT));
}

enum class UserRegion : uint8_t {
    CODE,
    HEAP,
    MMAP,
    STACK,
    LOW_ADDRESS,
    HIGH_RUNTIME,
};

auto valid_image_range(const UserImageRange& range) -> bool {
    constexpr uint64_t PAGE_MASK = paging::PAGE_SIZE - 1;
    return range.start < range.end && (range.start & PAGE_MASK) == 0 && (range.end & PAGE_MASK) == 0;
}

auto in_range(uint64_t address, const UserImageRange& range) -> bool {
    return valid_image_range(range) && address >= range.start && address < range.end;
}

auto in_window(uint64_t address, const user_layout::AddressWindow& window) -> bool {
    return address >= window.begin && address < window.end;
}

auto classify_region(uint64_t address, const UserMemoryLayout& layout) -> UserRegion {
    for (size_t i = 0; i < layout.image_count && i < layout.images.size(); ++i) {
        if (in_range(address, layout.images.at(i))) {
            return UserRegion::CODE;
        }
    }
    if (in_window(address, user_layout::MMAP_WINDOW)) {
        return UserRegion::MMAP;
    }
    if (in_window(address, user_layout::THREAD_WINDOW)) {
        return UserRegion::STACK;
    }
    if (address >= user_layout::MAIN_IMAGE_WINDOW.begin && address < user_layout::MMAP_WINDOW.begin) {
        return UserRegion::HEAP;
    }
    if (address < user_layout::MAIN_IMAGE_WINDOW.begin) {
        return UserRegion::LOW_ADDRESS;
    }
    return UserRegion::HIGH_RUNTIME;
}

void consider_boundary(uint64_t current, uint64_t boundary, uint64_t& next) {
    if (boundary > current && boundary < next) {
        next = boundary;
    }
}

auto next_region_boundary(uint64_t current, uint64_t end, const UserMemoryLayout& layout) -> uint64_t {
    uint64_t next = end;
    for (size_t i = 0; i < layout.image_count && i < layout.images.size(); ++i) {
        auto const& image = layout.images.at(i);
        if (valid_image_range(image)) {
            consider_boundary(current, image.start, next);
            consider_boundary(current, image.end, next);
        }
    }
    consider_boundary(current, user_layout::MAIN_IMAGE_WINDOW.begin, next);
    consider_boundary(current, user_layout::MMAP_WINDOW.begin, next);
    consider_boundary(current, user_layout::MMAP_WINDOW.end, next);
    consider_boundary(current, user_layout::THREAD_WINDOW.begin, next);
    consider_boundary(current, user_layout::THREAD_WINDOW.end, next);
    return next;
}

void add_region_pages(UserMemoryBreakdown& stats, UserRegion region, uint64_t page_count) {
    switch (region) {
        case UserRegion::CODE:
            stats.code_pages += page_count;
            break;
        case UserRegion::HEAP:
            stats.heap_pages += page_count;
            break;
        case UserRegion::MMAP:
            stats.mmap_pages += page_count;
            break;
        case UserRegion::STACK:
            stats.stack_pages += page_count;
            break;
        case UserRegion::LOW_ADDRESS:
            stats.low_address_pages += page_count;
            break;
        case UserRegion::HIGH_RUNTIME:
            stats.high_runtime_pages += page_count;
            break;
    }
}

void add_region(UserMemoryBreakdown& stats, uint64_t vaddr, uint64_t page_count, const UserMemoryLayout& layout) {
    if (page_count == 0 || page_count > (UINT64_MAX - vaddr) / paging::PAGE_SIZE) {
        return;
    }
    uint64_t const END = vaddr + (page_count * paging::PAGE_SIZE);
    uint64_t current = vaddr;
    while (current < END) {
        uint64_t const NEXT = next_region_boundary(current, END, layout);
        add_region_pages(stats, classify_region(current, layout), (NEXT - current) / paging::PAGE_SIZE);
        current = NEXT;
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

void add_present_leaf(UserMemoryBreakdown& stats, const paging::PageTableEntry& entry, uint64_t vaddr, uint64_t page_count,
                      const UserMemoryLayout& layout, phys::PageLookupHint* lookup) {
    if (entry.user == 0) {
        return;
    }

    stats.virtual_pages += page_count;
    stats.resident_pages += page_count;
    add_region(stats, vaddr, page_count, layout);
    add_permissions(stats, entry, page_count);

    uint64_t const RAW = pte_raw(entry);
    bool shared = (RAW & paging::PAGE_SHARED) != 0U;
    if (!shared && entry.frame != 0) {
        uint64_t const PHYS = static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT;
        auto* virt_page = reinterpret_cast<void*>(addr::get_virt_pointer(PHYS));
        shared = phys::page_ref_get(virt_page, lookup) > 1;
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

auto collect_user_memory_breakdown(paging::PageTable* page_table, const UserMemoryLayout& layout) -> UserMemoryBreakdown {
    UserMemoryBreakdown stats{};
    if (page_table == nullptr) {
        return stats;
    }

    phys::PageLookupHint ref_lookup{};
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
                add_present_leaf(stats, pml3e, VADDR_1G, PAGES_PER_1G, layout, &ref_lookup);
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
                    add_present_leaf(stats, pml2e, VADDR_2M, PAGES_PER_2M, layout, &ref_lookup);
                    continue;
                }

                stats.page_table_pages++;
                auto* pml1 = table_from_entry(pml2e);
                for (size_t i1 = 0; i1 < pml1->entries.size(); ++i1) {
                    const auto& pte = pml1->entries.at(i1);
                    uint64_t const VADDR = VADDR_2M | (static_cast<uint64_t>(i1) << PML1_SHIFT);
                    if (pte.present != 0) {
                        add_present_leaf(stats, pte, VADDR, 1, layout, &ref_lookup);
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
