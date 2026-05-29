#include "page_alloc.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/paging.hpp>

namespace ker::mod::mm {

namespace {

// ============================================================================
// Helpers
// ============================================================================

// Smallest order k such that (1 << k) pages >= the requested byte count.
auto size_to_order(uint64_t size_bytes) -> int {
    uint64_t const PAGES = (size_bytes + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
    if (PAGES <= 1) {
        return 0;
    }
    // __builtin_clzll(0) is undefined, but pages >= 2 here.
    int bits = 64 - __builtin_clzll(PAGES - 1);
    bits = std::min(bits, PageAllocator::MAX_ORDER);
    return bits;
}

// Convert a page index to the HHDM pointer inside the zone.
auto page_to_ptr(uint64_t base, uint32_t page_idx) -> void* {
    return reinterpret_cast<void*>(base + (static_cast<uint64_t>(page_idx) * paging::PAGE_SIZE));
}

// Convert an HHDM pointer to a page index relative to the zone base.
auto ptr_to_page(uint64_t base, void* ptr) -> uint32_t {
    return static_cast<uint32_t>((reinterpret_cast<uint64_t>(ptr) - base) / paging::PAGE_SIZE);
}

auto ptr_is_page_aligned(const void* ptr) -> bool { return (reinterpret_cast<uint64_t>(ptr) & (paging::PAGE_SIZE - 1)) == 0; }

auto ptr_in_zone(const PageAllocator* alloc, const void* ptr) -> bool {
    if (ptr == nullptr || !ptr_is_page_aligned(ptr)) {
        return false;
    }

    const auto ADDR = reinterpret_cast<uint64_t>(ptr);
    const uint64_t END = alloc->base + (static_cast<uint64_t>(alloc->total_pages) * paging::PAGE_SIZE);
    return ADDR >= alloc->base && ADDR < END;
}

auto free_head_is_valid(const PageAllocator* alloc, PageAllocator::FreeBlock* block, int order, uint32_t& page_idx) -> bool {
    if (!ptr_in_zone(alloc, block)) {
        return false;
    }

    page_idx = ptr_to_page(alloc->base, block);
    const uint32_t BLOCK_SIZE = 1U << order;
    if (page_idx >= alloc->total_pages || page_idx + BLOCK_SIZE > alloc->total_pages) {
        return false;
    }

    return alloc->page_flags[page_idx] == (PageAllocator::FLAG_FREE_HEAD | static_cast<uint8_t>(order));
}

void drop_corrupt_free_list_head(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) {
    ker::mod::io::serial::write("page_alloc: dropping corrupt free-list head order=");
    ker::mod::io::serial::write(static_cast<uint64_t>(order));
    ker::mod::io::serial::write(" ptr=0x");
    ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(block));
    ker::mod::io::serial::write(" zone_base=0x");
    ker::mod::io::serial::write_hex(alloc->base);
    ker::mod::io::serial::write("\n");
    alloc->free_list.at(static_cast<size_t>(order)) = nullptr;
}

// Remove a specific FreeBlock from a singly-linked list.
// Returns true if found and removed.
auto list_remove(PageAllocator* alloc, int order, PageAllocator::FreeBlock*& head, PageAllocator::FreeBlock* target) -> bool {
    PageAllocator::FreeBlock** prev = &head;
    PageAllocator::FreeBlock* cur = head;
    while (cur != nullptr) {
        uint32_t cur_idx = 0;
        if (!free_head_is_valid(alloc, cur, order, cur_idx)) {
            ker::mod::io::serial::write("page_alloc: corrupt free-list node while removing order=");
            ker::mod::io::serial::write(static_cast<uint64_t>(order));
            ker::mod::io::serial::write(" ptr=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(cur));
            ker::mod::io::serial::write("\n");
            *prev = nullptr;
            return false;
        }
        if (cur == target) {
            *prev = cur->next;
            return true;
        }
        prev = &cur->next;
        cur = cur->next;
    }
    return false;
}

}  // namespace

// ============================================================================
// init
// ============================================================================

void PageAllocator::init(uint64_t zone_base, uint64_t size_bytes) {
    base = zone_base;
    total_pages = static_cast<uint32_t>(size_bytes / paging::PAGE_SIZE);

    // --- lay out metadata at the beginning of the zone ---

    // PageAllocator struct occupies bytes [0, sizeof(*this)).
    // page_flags array immediately after, then page_refcounts array.
    uint64_t const FLAGS_OFFSET = sizeof(PageAllocator);
    page_flags = reinterpret_cast<uint8_t*>(zone_base + FLAGS_OFFSET);

    uint64_t refcounts_offset = FLAGS_OFFSET + static_cast<uint64_t>(total_pages);
    // Align refcounts to 4-byte boundary for uint32_t access
    refcounts_offset = (refcounts_offset + 3) & ~3ULL;
    page_refcounts = reinterpret_cast<uint32_t*>(zone_base + refcounts_offset);

    uint64_t meta_bytes = refcounts_offset + (static_cast<uint64_t>(total_pages) * sizeof(uint32_t));
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLERS_OFFSET = (meta_bytes + 7) & ~7ULL;
    page_callers = reinterpret_cast<uint64_t*>(zone_base + CALLERS_OFFSET);
    meta_bytes = CALLERS_OFFSET + (static_cast<uint64_t>(total_pages) * sizeof(uint64_t));
#endif
    metadata_pages = static_cast<uint32_t>((meta_bytes + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE);

    if (metadata_pages >= total_pages) {
        // Zone too small to hold any usable pages.
        usable_pages = 0;
        free_count = 0;
        for (auto*& list_head : free_list) {
            list_head = nullptr;
        }
        std::memset(page_flags, FLAG_RESERVED, total_pages);
        std::memset(page_refcounts, 0, total_pages * sizeof(uint32_t));
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        std::memset(page_callers, 0, total_pages * sizeof(uint64_t));
#endif
        return;
    }

    usable_pages = total_pages - metadata_pages;
    free_count = 0;

    // Zero the free lists.
    for (auto*& list_head : free_list) {
        list_head = nullptr;
    }

    // Mark metadata pages as reserved, all others as allocated-continuation
    // (prevents false buddy matches during the decomposition loop below).
    std::memset(page_flags, FLAG_RESERVED, metadata_pages);
    std::memset(page_flags + metadata_pages, FLAG_ALLOC_CONT, total_pages - metadata_pages);
    // Zero all refcounts (free pages have refcount 0)
    std::memset(page_refcounts, 0, total_pages * sizeof(uint32_t));
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    std::memset(page_callers, 0, total_pages * sizeof(uint64_t));
#endif

    // --- decompose usable range into largest aligned power-of-2 blocks ---

    uint32_t page = metadata_pages;
    while (page < total_pages) {
        // Find largest order where the page is naturally aligned and
        // the block fits within the zone.
        int order = 0;
        while (order < MAX_ORDER) {
            uint32_t const BLOCK_SIZE = 1U << (order + 1);
            if ((page & (BLOCK_SIZE - 1)) != 0) {
                break;  // not aligned
            }
            if (page + BLOCK_SIZE > total_pages) {
                break;  // doesn't fit
            }
            order++;
        }

        uint32_t const BLOCK_SIZE = 1U << order;

        // Mark head page as free-head, interior pages as free-interior.
        page_flags[page] = FLAG_FREE_HEAD | static_cast<uint8_t>(order);
        for (uint32_t i = 1; i < BLOCK_SIZE; i++) {
            page_flags[page + i] = FLAG_FREE_INTERIOR;
        }

        // Prepend to the order's free list (link through the page itself).
        auto* block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, page));
        block->next = free_list.at(static_cast<size_t>(order));
        free_list.at(static_cast<size_t>(order)) = block;

        free_count += BLOCK_SIZE;
        page += BLOCK_SIZE;
    }
}

// ============================================================================
// alloc
// ============================================================================

void* PageAllocator::alloc(uint64_t size_bytes, uint64_t caller) {
    int const ORDER = size_to_order(size_bytes);

    // Walk up to find the smallest available block >= requested order.
    int k = ORDER;
    uint32_t page_idx = 0;
    FreeBlock* block = nullptr;
    while (k <= MAX_ORDER) {
        block = free_list.at(static_cast<size_t>(k));
        if (block == nullptr) {
            k++;
            continue;
        }

        if (!free_head_is_valid(this, block, k, page_idx)) {
            drop_corrupt_free_list_head(this, k, block);
            k++;
            continue;
        }

        // Pop head of free_list[k].
        free_list.at(static_cast<size_t>(k)) = block->next;
        break;
    }

    if (k > MAX_ORDER) {
        return nullptr;  // OOM
    }

    // Split down: put the upper buddy of each split into the free list.
    while (k > ORDER) {
        k--;
        uint32_t const BUDDY_SIZE = 1U << k;
        uint32_t const BUDDY_IDX = page_idx + BUDDY_SIZE;

        // Upper buddy becomes a free head at order k.
        page_flags[BUDDY_IDX] = FLAG_FREE_HEAD | static_cast<uint8_t>(k);
        for (uint32_t i = 1; i < BUDDY_SIZE; i++) {
            page_flags[BUDDY_IDX + i] = FLAG_FREE_INTERIOR;
        }

        auto* buddy_block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, BUDDY_IDX));
        buddy_block->next = free_list.at(static_cast<size_t>(k));
        free_list.at(static_cast<size_t>(k)) = buddy_block;
    }

    // Mark the allocated block.
    uint32_t const BLOCK_SIZE = 1U << ORDER;
    page_flags[page_idx] = FLAG_ALLOC_HEAD | static_cast<uint8_t>(ORDER);
    for (uint32_t i = 1; i < BLOCK_SIZE; i++) {
        page_flags[page_idx + i] = FLAG_ALLOC_CONT;
    }

    // Set refcount to 1 for all pages in the block
    for (uint32_t i = 0; i < BLOCK_SIZE; i++) {
        page_refcounts[page_idx + i] = 1;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        page_callers[page_idx + i] = caller;
#endif
    }

    free_count -= BLOCK_SIZE;
    return reinterpret_cast<void*>(base + (static_cast<uint64_t>(page_idx) * paging::PAGE_SIZE));
}

// ============================================================================
// free
// ============================================================================

uint64_t PageAllocator::free(void* ptr) {
    if (ptr == nullptr) {
        return 0;
    }
    if (!ptr_in_zone(this, ptr)) {
        ker::mod::io::serial::write("page_alloc: rejecting free outside zone ptr=0x");
        ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(ptr));
        ker::mod::io::serial::write(" zone_base=0x");
        ker::mod::io::serial::write_hex(base);
        ker::mod::io::serial::write("\n");
        return 0;
    }

    uint32_t page_idx = ptr_to_page(base, ptr);
    if (page_idx >= total_pages) {
        return 0;
    }

    uint8_t const FLAGS = page_flags[page_idx];

    // Must be an allocated head page.
    if ((FLAGS & 0xC0) != 0x80) {
        return 0;  // not FLAG_ALLOC_HEAD
    }

    // Guard: detect page_free on a live kmalloc medium allocation.
    // The MediumAllocationHeader layout is: next(8) size(8) magic(8) pad(8).
    // Magic lives at offset +16 from the page base.
    {
        constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
        const auto PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        if (*reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("BUG: page_free on live medium alloc page=0x");
            ker::mod::io::serial::write_hex(PAGE_BASE);
            ker::mod::io::serial::write(" caller_ptr=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(ptr));
            ker::mod::io::serial::write("\n");
            ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
        }
    }

    int const ORDER = FLAGS & 0x1F;
    if (ORDER > MAX_ORDER) {
        ker::mod::io::serial::write("page_alloc: rejecting free with invalid order=");
        ker::mod::io::serial::write(static_cast<uint64_t>(ORDER));
        ker::mod::io::serial::write(" ptr=0x");
        ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(ptr));
        ker::mod::io::serial::write("\n");
        return 0;
    }

    uint32_t const BLOCK_SIZE = 1U << ORDER;
    uint64_t const FREED_BYTES = static_cast<uint64_t>(BLOCK_SIZE) * paging::PAGE_SIZE;

    // Clear flags for the entire allocation.
    for (uint32_t i = 0; i < BLOCK_SIZE; i++) {
        page_flags[page_idx + i] = FLAG_FREE_INTERIOR;
        page_refcounts[page_idx + i] = 0;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        page_callers[page_idx + i] = 0;
#endif
    }

    free_count += BLOCK_SIZE;

    // Coalesce with buddies.
    int k = ORDER;
    while (k < MAX_ORDER) {
        uint32_t const BUDDY_IDX = page_idx ^ (1U << k);

        // Buddy must be within the zone and be a free head at exactly order k.
        if (BUDDY_IDX >= total_pages) {
            break;
        }
        if (page_flags[BUDDY_IDX] != (FLAG_FREE_HEAD | static_cast<uint8_t>(k))) {
            break;
        }

        // Remove buddy from its free list.
        auto* buddy_block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, BUDDY_IDX));
        if (!list_remove(this, k, free_list.at(static_cast<size_t>(k)), buddy_block)) {
            break;
        }

        // Clear buddy's head flag (it becomes an interior page of the merged block).
        page_flags[BUDDY_IDX] = FLAG_FREE_INTERIOR;

        // The merged block starts at the lower-aligned address.
        page_idx = std::min(BUDDY_IDX, page_idx);

        k++;
    }

    // Mark the (possibly merged) block as a free head.
    page_flags[page_idx] = FLAG_FREE_HEAD | static_cast<uint8_t>(k);

    // Prepend to the free list.
    auto* free_block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, page_idx));
    free_block->next = free_list.at(static_cast<size_t>(k));
    free_list.at(static_cast<size_t>(k)) = free_block;
    return FREED_BYTES;
}

auto PageAllocator::split_allocated_block_to_order0(void* ptr) const -> bool {
    if (ptr == nullptr || !ptr_in_zone(this, ptr)) {
        return false;
    }

    uint32_t const PAGE_IDX = ptr_to_page(base, ptr);
    if (PAGE_IDX >= total_pages) {
        return false;
    }

    uint8_t const FLAGS = page_flags[PAGE_IDX];
    if ((FLAGS & 0xC0) != FLAG_ALLOC_HEAD) {
        return false;
    }

    int const ORDER = FLAGS & 0x1F;
    if (ORDER == 0) {
        return true;
    }

    uint32_t const BLOCK_SIZE = 1U << ORDER;
    if (PAGE_IDX + BLOCK_SIZE > total_pages) {
        return false;
    }

#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t const CALLER = page_callers[PAGE_IDX];
#endif
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        page_flags[PAGE_IDX + i] = FLAG_ALLOC_HEAD;
        if (page_refcounts[PAGE_IDX + i] == 0) {
            page_refcounts[PAGE_IDX + i] = 1;
        }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        page_callers[PAGE_IDX + i] = CALLER;
#endif
    }

    return true;
}

}  // namespace ker::mod::mm
