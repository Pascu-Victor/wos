#include "page_alloc.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

auto largest_order_for_aligned_run(uint32_t page_idx, uint32_t page_count) -> int {
    int order = 0;
    while (order < PageAllocator::MAX_ORDER) {
        uint32_t const NEXT_BLOCK_SIZE = 1U << (order + 1);
        if (NEXT_BLOCK_SIZE > page_count) {
            break;
        }
        if ((page_idx & (NEXT_BLOCK_SIZE - 1)) != 0) {
            break;
        }
        order++;
    }
    return order;
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

auto page_kind_from_byte(uint8_t value) -> PageKind {
    switch (static_cast<PageKind>(value)) {
        case PageKind::FREE:
        case PageKind::RESERVED:
        case PageKind::NORMAL:
        case PageKind::PAGE_TABLE:
        case PageKind::SLAB:
        case PageKind::MEDIUM:
            return static_cast<PageKind>(value);
        case PageKind::UNKNOWN:
        default:
            return PageKind::UNKNOWN;
    }
}

void store_page_kind(std::atomic<uint8_t>& slot, PageKind kind) { slot.store(static_cast<uint8_t>(kind), std::memory_order_release); }

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
    dbg::emergency_log("page_alloc: dropping corrupt free-list head order=%lu ptr=0x%lx zone_base=0x%lx\n", static_cast<uint64_t>(order),
                       reinterpret_cast<uint64_t>(block), alloc->base);
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
            dbg::emergency_log("page_alloc: corrupt free-list node while removing order=%lu ptr=0x%lx\n", static_cast<uint64_t>(order),
                               reinterpret_cast<uint64_t>(cur));
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

auto page_has_live_medium_alloc_magic(PageAllocator* alloc, uint32_t page_idx) -> bool {
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
    const auto PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(alloc->base, page_idx));
    return *reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == MEDIUM_ALLOC_MAGIC;
}

auto free_allocated_block(PageAllocator* alloc, uint32_t page_idx, int order) -> uint64_t {
    if (order < 0 || order > PageAllocator::MAX_ORDER) {
        return 0;
    }

    uint32_t const BLOCK_SIZE = 1U << order;
    if (page_idx >= alloc->total_pages || page_idx + BLOCK_SIZE > alloc->total_pages) {
        return 0;
    }

    uint64_t const FREED_BYTES = static_cast<uint64_t>(BLOCK_SIZE) * paging::PAGE_SIZE;

    // Clear flags for the entire allocation.
    for (uint32_t i = 0; i < BLOCK_SIZE; i++) {
        alloc->page_flags[page_idx + i] = PageAllocator::FLAG_FREE_INTERIOR;
        store_page_kind(alloc->page_kinds[page_idx + i], PageKind::FREE);
        alloc->page_refcounts[page_idx + i].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        alloc->page_callers[page_idx + i] = 0;
#endif
    }

    alloc->free_count += BLOCK_SIZE;

    // Coalesce with buddies.
    int k = order;
    while (k < PageAllocator::MAX_ORDER) {
        uint32_t const BUDDY_IDX = page_idx ^ (1U << k);

        // Buddy must be within the zone and be a free head at exactly order k.
        if (BUDDY_IDX >= alloc->total_pages) {
            break;
        }
        if (alloc->page_flags[BUDDY_IDX] != (PageAllocator::FLAG_FREE_HEAD | static_cast<uint8_t>(k))) {
            break;
        }

        // Remove buddy from its free list.
        auto* buddy_block = reinterpret_cast<PageAllocator::FreeBlock*>(page_to_ptr(alloc->base, BUDDY_IDX));
        if (!list_remove(alloc, k, alloc->free_list.at(static_cast<size_t>(k)), buddy_block)) {
            break;
        }

        // Clear buddy's head flag (it becomes an interior page of the merged block).
        alloc->page_flags[BUDDY_IDX] = PageAllocator::FLAG_FREE_INTERIOR;

        // The merged block starts at the lower-aligned address.
        page_idx = std::min(BUDDY_IDX, page_idx);

        k++;
    }

    // Mark the (possibly merged) block as a free head.
    alloc->page_flags[page_idx] = PageAllocator::FLAG_FREE_HEAD | static_cast<uint8_t>(k);

    // Prepend to the free list.
    auto* free_block = reinterpret_cast<PageAllocator::FreeBlock*>(page_to_ptr(alloc->base, page_idx));
    free_block->next = alloc->free_list.at(static_cast<size_t>(k));
    alloc->free_list.at(static_cast<size_t>(k)) = free_block;
    return FREED_BYTES;
}

}  // namespace

// ============================================================================
// init
// ============================================================================

auto PageAllocator::lock_irq() -> uint64_t {
    uint64_t flags = 0;
    asm volatile("pushfq; popq %0" : "=r"(flags));
    asm volatile("cli");

    while (lock_held.exchange(true, std::memory_order_acquire)) {
        while (lock_held.load(std::memory_order_relaxed)) {
            asm volatile("pause");
        }
    }

    return flags;
}

void PageAllocator::unlock_irq(uint64_t flags) {
    lock_held.store(false, std::memory_order_release);
    constexpr uint64_t RFLAGS_INTERRUPT_ENABLE = 0x200;
    if ((flags & RFLAGS_INTERRUPT_ENABLE) != 0) {
        asm volatile("sti");
    }
}

void PageAllocator::init(uint64_t zone_base, uint64_t size_bytes) {
    lock_held.store(false, std::memory_order_release);
    base = zone_base;
    total_pages = static_cast<uint32_t>(size_bytes / paging::PAGE_SIZE);

    // --- lay out metadata at the beginning of the zone ---

    // PageAllocator struct occupies bytes [0, sizeof(*this)).
    // page_flags array immediately after, then page_kinds and page_refcounts.
    uint64_t const FLAGS_OFFSET = sizeof(PageAllocator);
    page_flags = reinterpret_cast<uint8_t*>(zone_base + FLAGS_OFFSET);

    uint64_t const KINDS_OFFSET = FLAGS_OFFSET + static_cast<uint64_t>(total_pages);
    page_kinds = reinterpret_cast<std::atomic<uint8_t>*>(zone_base + KINDS_OFFSET);

    uint64_t refcounts_offset = KINDS_OFFSET + (static_cast<uint64_t>(total_pages) * sizeof(std::atomic<uint8_t>));
    // Align refcounts to 4-byte boundary for uint32_t access
    refcounts_offset = (refcounts_offset + 3) & ~3ULL;
    page_refcounts = reinterpret_cast<std::atomic<uint32_t>*>(zone_base + refcounts_offset);

    uint64_t meta_bytes = refcounts_offset + (static_cast<uint64_t>(total_pages) * sizeof(std::atomic<uint32_t>));
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
        for (uint32_t i = 0; i < total_pages; ++i) {
            store_page_kind(page_kinds[i], PageKind::RESERVED);
        }
        for (uint32_t i = 0; i < total_pages; ++i) {
            page_refcounts[i].store(0, std::memory_order_relaxed);
        }
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
    for (uint32_t i = 0; i < metadata_pages; ++i) {
        store_page_kind(page_kinds[i], PageKind::RESERVED);
    }
    for (uint32_t i = metadata_pages; i < total_pages; ++i) {
        store_page_kind(page_kinds[i], PageKind::FREE);
    }
    // Zero all refcounts (free pages have refcount 0)
    for (uint32_t i = 0; i < total_pages; ++i) {
        page_refcounts[i].store(0, std::memory_order_relaxed);
    }
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
        store_page_kind(page_kinds[page_idx + i], PageKind::NORMAL);
        page_refcounts[page_idx + i].store(1, std::memory_order_release);
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
        dbg::emergency_log("page_alloc: rejecting free outside zone ptr=0x%lx zone_base=0x%lx\n", reinterpret_cast<uint64_t>(ptr), base);
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
    if (page_has_live_medium_alloc_magic(this, page_idx)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_ptr=0x%lx\n", PAGE_BASE, reinterpret_cast<uint64_t>(ptr));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
    }

    int const ORDER = FLAGS & 0x1F;
    if (ORDER > MAX_ORDER) {
        dbg::emergency_log("page_alloc: rejecting free with invalid order=%lu ptr=0x%lx\n", static_cast<uint64_t>(ORDER),
                           reinterpret_cast<uint64_t>(ptr));
        return 0;
    }

    return free_allocated_block(this, page_idx, ORDER);
}

uint64_t PageAllocator::free_order0_at(uint32_t page_idx) {
    if (page_idx >= total_pages) {
        return 0;
    }
    if (page_flags[page_idx] != FLAG_ALLOC_HEAD) {
        return 0;
    }
    if (page_has_live_medium_alloc_magic(this, page_idx)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE, static_cast<uint64_t>(page_idx));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
    }
    return free_allocated_block(this, page_idx, 0);
}

uint64_t PageAllocator::free_order0_range_at(uint32_t page_idx, uint32_t page_count) {
    if (page_count == 0 || page_idx >= total_pages || page_count > total_pages - page_idx) {
        return 0;
    }

    for (uint32_t i = 0; i < page_count; ++i) {
        uint32_t const CUR_IDX = page_idx + i;
        if (page_flags[CUR_IDX] != FLAG_ALLOC_HEAD) {
            return 0;
        }
        if (page_refcounts[CUR_IDX].load(std::memory_order_acquire) != 0) {
            return 0;
        }
        if (page_has_live_medium_alloc_magic(this, CUR_IDX)) {
            auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, CUR_IDX));
            dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE,
                               static_cast<uint64_t>(CUR_IDX));
            ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
        }
    }

    uint64_t freed_bytes = 0;
    uint32_t cursor = page_idx;
    uint32_t remaining = page_count;
    while (remaining > 0) {
        int const ORDER = largest_order_for_aligned_run(cursor, remaining);
        uint32_t const BLOCK_SIZE = 1U << ORDER;
        freed_bytes += free_allocated_block(this, cursor, ORDER);
        cursor += BLOCK_SIZE;
        remaining -= BLOCK_SIZE;
    }
    return freed_bytes;
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
    PageKind const KIND = page_kind_from_byte(page_kinds[PAGE_IDX].load(std::memory_order_acquire));
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        page_flags[PAGE_IDX + i] = FLAG_ALLOC_HEAD;
        store_page_kind(page_kinds[PAGE_IDX + i], KIND);
        if (page_refcounts[PAGE_IDX + i].load(std::memory_order_acquire) == 0) {
            page_refcounts[PAGE_IDX + i].store(1, std::memory_order_release);
        }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        page_callers[PAGE_IDX + i] = CALLER;
#endif
    }

    return true;
}

auto PageAllocator::mark_allocated_block_kind(void* ptr, PageKind kind) const -> bool {
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
    if (ORDER > MAX_ORDER) {
        return false;
    }

    uint32_t const BLOCK_SIZE = 1U << ORDER;
    if (PAGE_IDX + BLOCK_SIZE > total_pages) {
        return false;
    }

    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        store_page_kind(page_kinds[PAGE_IDX + i], kind);
    }
    return true;
}

auto PageAllocator::kind_of(void* ptr) const -> PageKind {
    if (ptr == nullptr || !ptr_in_zone(this, ptr)) {
        return PageKind::UNKNOWN;
    }

    uint32_t const PAGE_IDX = ptr_to_page(base, ptr);
    if (PAGE_IDX >= total_pages) {
        return PageKind::UNKNOWN;
    }

    return page_kind_from_byte(page_kinds[PAGE_IDX].load(std::memory_order_acquire));
}

}  // namespace ker::mod::mm
