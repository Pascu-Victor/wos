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

static_assert(sizeof(std::atomic<uint8_t>) == sizeof(uint8_t), "PageKind table fill assumes byte-sized atomics");
static_assert(sizeof(std::atomic<uint32_t>) == sizeof(uint32_t), "Page refcount table clear assumes uint32-sized atomics");

// Smallest order k such that (1 << k) pages >= the requested byte count.
auto size_to_order(uint64_t size_bytes, int& out_order) -> bool {
    uint64_t const PAGES = (size_bytes / paging::PAGE_SIZE) + ((size_bytes % paging::PAGE_SIZE) != 0 ? 1 : 0);
    if (PAGES <= 1) {
        out_order = 0;
        return true;
    }
    uint64_t const MAX_PAGES = uint64_t{1} << PageAllocator::MAX_ORDER;
    if (PAGES > MAX_PAGES) {
        return false;
    }
    // __builtin_clzll(0) is undefined, but pages >= 2 here.
    out_order = 64 - __builtin_clzll(PAGES - 1);
    return true;
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

void store_page_kind(std::atomic<uint8_t>& slot, PageKind kind) {
    slot.store(static_cast<uint8_t>(decode_page_kind(static_cast<uint8_t>(kind))), std::memory_order_release);
}

auto page_kind_name(PageKind kind) -> const char* {
    switch (kind) {
        case PageKind::UNKNOWN:
            return "UNKNOWN";
        case PageKind::FREE:
            return "FREE";
        case PageKind::RESERVED:
            return "RESERVED";
        case PageKind::NORMAL:
            return "NORMAL";
        case PageKind::PAGE_TABLE:
            return "PAGE_TABLE";
        case PageKind::SLAB:
            return "SLAB";
        case PageKind::MEDIUM:
            return "MEDIUM";
        case PageKind::KMALLOC_LARGE:
            return "KMALLOC_LARGE";
        default:
            return "INVALID";
    }
}

void fill_page_kinds(std::atomic<uint8_t>* kinds, uint32_t count, PageKind kind) {
    if (count == 0) {
        return;
    }
    std::memset(kinds, static_cast<int>(static_cast<uint8_t>(decode_page_kind(static_cast<uint8_t>(kind)))),
                static_cast<size_t>(count) * sizeof(std::atomic<uint8_t>));
}

void zero_page_refcounts(std::atomic<uint32_t>* refcounts, uint32_t count) {
    if (count == 0) {
        return;
    }
    std::memset(refcounts, 0, static_cast<size_t>(count) * sizeof(std::atomic<uint32_t>));
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

auto free_list_head(PageAllocator* alloc, int order) -> PageAllocator::FreeBlock*& { return alloc->free_list[static_cast<size_t>(order)]; }

void link_free_block_unchecked(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) {
    auto& head = free_list_head(alloc, order);
    block->prev = nullptr;
    block->next = head;
    if (head != nullptr) {
        head->prev = block;
    }
    head = block;
}

auto remove_free_block(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) -> bool {
    uint32_t block_idx = 0;
    if (!free_head_is_valid(alloc, block, order, block_idx)) {
        dbg::emergency_log("page_alloc: corrupt free-list target order=%lu ptr=0x%lx\n", static_cast<uint64_t>(order),
                           reinterpret_cast<uint64_t>(block));
        return false;
    }

    auto& head = free_list_head(alloc, order);
    auto* const PREV = block->prev;
    auto* const NEXT = block->next;

    if (PREV == nullptr) {
        if (head != block) {
            dbg::emergency_log("page_alloc: free-list target missing prev order=%lu ptr=0x%lx head=0x%lx\n", static_cast<uint64_t>(order),
                               reinterpret_cast<uint64_t>(block), reinterpret_cast<uint64_t>(head));
            return false;
        }
    } else {
        uint32_t prev_idx = 0;
        if (!free_head_is_valid(alloc, PREV, order, prev_idx) || PREV->next != block) {
            dbg::emergency_log("page_alloc: corrupt free-list prev order=%lu ptr=0x%lx prev=0x%lx\n", static_cast<uint64_t>(order),
                               reinterpret_cast<uint64_t>(block), reinterpret_cast<uint64_t>(PREV));
            return false;
        }
    }

    if (NEXT != nullptr) {
        uint32_t next_idx = 0;
        if (!free_head_is_valid(alloc, NEXT, order, next_idx) || NEXT->prev != block) {
            dbg::emergency_log("page_alloc: corrupt free-list next order=%lu ptr=0x%lx next=0x%lx\n", static_cast<uint64_t>(order),
                               reinterpret_cast<uint64_t>(block), reinterpret_cast<uint64_t>(NEXT));
            return false;
        }
    }

    if (PREV == nullptr) {
        head = NEXT;
    } else {
        PREV->next = NEXT;
    }
    if (NEXT != nullptr) {
        NEXT->prev = PREV;
    }
    block->prev = nullptr;
    block->next = nullptr;
    return true;
}

struct FreeBlockPageIssue {
    uint32_t free_head_idx;
    uint32_t bad_page_idx;
};

[[noreturn]] void panic_non_reusable_free_block_page(PageAllocator* alloc, FreeBlockPageIssue issue, int order, const char* reason) {
    PageKind const KIND = decode_page_kind(alloc->page_kinds[issue.bad_page_idx].load(std::memory_order_acquire));
    uint32_t const REFCOUNT = alloc->page_refcounts[issue.bad_page_idx].load(std::memory_order_acquire);
    dbg::emergency_log(
        "page_alloc: free block overlaps non-reusable page reason=%s zone_base=0x%lx block_idx=%lu order=%lu page_idx=%lu "
        "page=0x%lx flags=0x%lx kind=%s ref=%lu\n",
        reason != nullptr ? reason : "?", alloc->base, static_cast<uint64_t>(issue.free_head_idx), static_cast<uint64_t>(order),
        static_cast<uint64_t>(issue.bad_page_idx), reinterpret_cast<uint64_t>(page_to_ptr(alloc->base, issue.bad_page_idx)),
        static_cast<uint64_t>(alloc->page_flags[issue.bad_page_idx]), page_kind_name(KIND), static_cast<uint64_t>(REFCOUNT));
    ker::mod::dbg::panic_handler("page_alloc free block overlaps live page");
    __builtin_unreachable();
}

auto free_block_pages_are_reusable(PageAllocator* alloc, uint32_t page_idx, int order, const char* reason) -> bool {
    if (order < 0 || order > PageAllocator::MAX_ORDER) {
        return false;
    }

    uint32_t const BLOCK_SIZE = 1U << order;
    if (page_idx >= alloc->total_pages || page_idx + BLOCK_SIZE > alloc->total_pages) {
        return false;
    }

    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        uint32_t const CUR_IDX = page_idx + i;
        uint8_t const EXPECTED_FLAGS =
            i == 0 ? static_cast<uint8_t>(PageAllocator::FLAG_FREE_HEAD | static_cast<uint8_t>(order)) : PageAllocator::FLAG_FREE_INTERIOR;
        if (alloc->page_flags[CUR_IDX] != EXPECTED_FLAGS) {
            panic_non_reusable_free_block_page(alloc, {.free_head_idx = page_idx, .bad_page_idx = CUR_IDX}, order, reason);
        }
        if (decode_page_kind(alloc->page_kinds[CUR_IDX].load(std::memory_order_acquire)) != PageKind::FREE) {
            panic_non_reusable_free_block_page(alloc, {.free_head_idx = page_idx, .bad_page_idx = CUR_IDX}, order, reason);
        }
        if (alloc->page_refcounts[CUR_IDX].load(std::memory_order_acquire) != 0) {
            panic_non_reusable_free_block_page(alloc, {.free_head_idx = page_idx, .bad_page_idx = CUR_IDX}, order, reason);
        }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        if (alloc->page_callers[CUR_IDX] != 0) {
            panic_non_reusable_free_block_page(alloc, {.free_head_idx = page_idx, .bad_page_idx = CUR_IDX}, order, reason);
        }
#endif
    }

    return true;
}

void rebuild_free_lists_from_flags(PageAllocator* alloc, const char* reason) {
    dbg::emergency_log("page_alloc: rebuilding free lists reason=%s zone_base=0x%lx\n", reason != nullptr ? reason : "?", alloc->base);

    for (auto*& list_head : alloc->free_list) {
        list_head = nullptr;
    }

    uint64_t linked_free_pages = 0;
    uint64_t cached_order0_pages = 0;
    uint32_t page_idx = 0;
    while (page_idx < alloc->total_pages) {
        uint8_t const FLAGS = alloc->page_flags[page_idx];
        if (FLAGS == PageAllocator::FLAG_CACHED_ORDER0) {
            cached_order0_pages++;
            page_idx++;
            continue;
        }
        if ((FLAGS & 0xC0) != PageAllocator::FLAG_FREE_HEAD) {
            page_idx++;
            continue;
        }

        int const ORDER = FLAGS & 0x1F;
        if (ORDER < 0 || ORDER > PageAllocator::MAX_ORDER) {
            dbg::emergency_log("page_alloc: rebuild skipping invalid free head order=%lu page=%lu\n", static_cast<uint64_t>(ORDER),
                               static_cast<uint64_t>(page_idx));
            alloc->page_flags[page_idx] = PageAllocator::FLAG_FREE_INTERIOR;
            page_idx++;
            continue;
        }

        uint32_t const BLOCK_SIZE = 1U << ORDER;
        if (page_idx + BLOCK_SIZE > alloc->total_pages) {
            dbg::emergency_log("page_alloc: rebuild skipping out-of-range free head order=%lu page=%lu total=%lu\n",
                               static_cast<uint64_t>(ORDER), static_cast<uint64_t>(page_idx), static_cast<uint64_t>(alloc->total_pages));
            alloc->page_flags[page_idx] = PageAllocator::FLAG_FREE_INTERIOR;
            page_idx++;
            continue;
        }
        if (!free_block_pages_are_reusable(alloc, page_idx, ORDER, "rebuild free head")) {
            page_idx++;
            continue;
        }

        auto* block = reinterpret_cast<PageAllocator::FreeBlock*>(page_to_ptr(alloc->base, page_idx));
        link_free_block_unchecked(alloc, ORDER, block);
        linked_free_pages += BLOCK_SIZE;
        page_idx += BLOCK_SIZE;
    }

    uint64_t const TOTAL_FREE_PAGES = linked_free_pages + cached_order0_pages;
    alloc->cached_order0_count = cached_order0_pages > alloc->total_pages ? alloc->total_pages : static_cast<uint32_t>(cached_order0_pages);
    alloc->free_count = TOTAL_FREE_PAGES > alloc->total_pages ? alloc->total_pages : static_cast<uint32_t>(TOTAL_FREE_PAGES);
}

auto free_block_is_reachable(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) -> bool {
    uint32_t visited = 0;
    for (auto* cur = free_list_head(alloc, order); cur != nullptr && visited <= alloc->total_pages; cur = cur->next) {
        uint32_t cur_idx = 0;
        if (!free_head_is_valid(alloc, cur, order, cur_idx)) {
            return false;
        }
        if (cur == block) {
            return true;
        }
        visited++;
    }
    return false;
}

void insert_free_block(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) {
    auto& head = free_list_head(alloc, order);
    if (head != nullptr) {
        uint32_t head_idx = 0;
        if (!free_head_is_valid(alloc, head, order, head_idx)) {
            rebuild_free_lists_from_flags(alloc, "insert corrupt head");
            if (free_block_is_reachable(alloc, order, block)) {
                return;
            }
        }
    }

    link_free_block_unchecked(alloc, order, block);
}

auto remove_free_block_with_repair(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block, const char* reason) -> bool {
    if (remove_free_block(alloc, order, block)) {
        return true;
    }
    rebuild_free_lists_from_flags(alloc, reason);
    uint32_t rebuilt_idx = 0;
    return free_head_is_valid(alloc, block, order, rebuilt_idx) && remove_free_block(alloc, order, block);
}

auto page_has_live_tracked_alloc_magic(PageAllocator* alloc, uint32_t page_idx, PageKind tracked_kind, uint64_t magic) -> bool {
    PageKind const KIND = decode_page_kind(alloc->page_kinds[page_idx].load(std::memory_order_acquire));
    if (KIND != tracked_kind && KIND != PageKind::UNKNOWN) {
        return false;
    }

    const auto PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(alloc->base, page_idx));
    return *reinterpret_cast<const uint64_t*>(PAGE_BASE + 16) == magic;
}

auto page_has_live_medium_alloc_magic(PageAllocator* alloc, uint32_t page_idx) -> bool {
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
    return page_has_live_tracked_alloc_magic(alloc, page_idx, PageKind::MEDIUM, MEDIUM_ALLOC_MAGIC);
}

auto page_has_live_large_alloc_magic(PageAllocator* alloc, uint32_t page_idx) -> bool {
    constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;
    return page_has_live_tracked_alloc_magic(alloc, page_idx, PageKind::KMALLOC_LARGE, LARGE_ALLOC_MAGIC);
}

auto page_has_live_slab_kind(PageAllocator* alloc, uint32_t page_idx) -> bool {
    return decode_page_kind(alloc->page_kinds[page_idx].load(std::memory_order_acquire)) == PageKind::SLAB;
}

void report_live_slab_page_free(PageAllocator* alloc, uint32_t page_idx, const char* caller) {
    auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(alloc->base, page_idx));
    dbg::emergency_log("BUG: %s on live slab page=0x%lx idx=%lu\n", caller != nullptr ? caller : "page_free", PAGE_BASE,
                       static_cast<uint64_t>(page_idx));
}

auto order0_free_head_is_reusable(PageAllocator* alloc, PageAllocator::FreeBlock* block, uint32_t& page_idx) -> bool {
    if (!free_head_is_valid(alloc, block, 0, page_idx)) {
        return false;
    }

    if (decode_page_kind(alloc->page_kinds[page_idx].load(std::memory_order_acquire)) != PageKind::FREE) {
        return false;
    }
    if (alloc->page_refcounts[page_idx].load(std::memory_order_acquire) != 0) {
        return false;
    }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    if (alloc->page_callers[page_idx] != 0) {
        return false;
    }
#endif
    return true;
}

auto allocated_block_has_direct_free_refs(PageAllocator* alloc, uint32_t page_idx, int order) -> bool {
    uint32_t const BLOCK_SIZE = 1U << order;
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        if (alloc->page_refcounts[page_idx + i].load(std::memory_order_acquire) != 1) {
            return false;
        }
    }
    return true;
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
        if (!remove_free_block_with_repair(alloc, k, buddy_block, "free coalesce unlink")) {
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
    insert_free_block(alloc, k, free_block);
    alloc->free_count += BLOCK_SIZE;
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
        cached_order0_count = 0;
        for (auto*& list_head : free_list) {
            list_head = nullptr;
        }
        std::memset(page_flags, FLAG_RESERVED, total_pages);
        fill_page_kinds(page_kinds, total_pages, PageKind::RESERVED);
        zero_page_refcounts(page_refcounts, total_pages);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        std::memset(page_callers, 0, total_pages * sizeof(uint64_t));
#endif
        return;
    }

    usable_pages = total_pages - metadata_pages;
    free_count = 0;
    cached_order0_count = 0;

    // Zero the free lists.
    for (auto*& list_head : free_list) {
        list_head = nullptr;
    }

    // Mark metadata pages as reserved and pre-clear all usable pages as free
    // interiors. The decomposition loop only needs to stamp block heads.
    std::memset(page_flags, FLAG_RESERVED, metadata_pages);
    std::memset(page_flags + metadata_pages, FLAG_FREE_INTERIOR, total_pages - metadata_pages);
    fill_page_kinds(page_kinds, metadata_pages, PageKind::RESERVED);
    fill_page_kinds(page_kinds + metadata_pages, total_pages - metadata_pages, PageKind::FREE);
    zero_page_refcounts(page_refcounts, total_pages);
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

        // Mark the head; interior pages were pre-cleared above.
        page_flags[page] = FLAG_FREE_HEAD | static_cast<uint8_t>(order);

        // Prepend to the order's free list (link through the page itself).
        auto* block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, page));
        insert_free_block(this, order, block);

        free_count += BLOCK_SIZE;
        page += BLOCK_SIZE;
    }
}

// ============================================================================
// alloc
// ============================================================================

void* PageAllocator::alloc(uint64_t size_bytes, uint64_t caller) {
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    (void)caller;
#endif
    int order = 0;
    if (!size_to_order(size_bytes, order)) {
        return nullptr;
    }

    // Walk up to find the smallest available block >= requested order.
    int k = order;
    uint32_t page_idx = 0;
    FreeBlock* block = nullptr;
    bool repaired_free_lists = false;
    while (k <= MAX_ORDER) {
        block = free_list[static_cast<size_t>(k)];
        if (block == nullptr) {
            k++;
            continue;
        }

        if (!free_head_is_valid(this, block, k, page_idx)) {
            if (repaired_free_lists) {
                k++;
                continue;
            }
            rebuild_free_lists_from_flags(this, "alloc corrupt head");
            repaired_free_lists = true;
            k = order;
            continue;
        }

        // Pop head of free_list[k].
        if (!remove_free_block_with_repair(this, k, block, "alloc unlink")) {
            if (repaired_free_lists) {
                k++;
                block = nullptr;
                continue;
            }
            repaired_free_lists = true;
            k = order;
            block = nullptr;
            continue;
        }
        break;
    }

    if (k > MAX_ORDER) {
        return nullptr;  // OOM
    }

    if (!free_block_pages_are_reusable(this, page_idx, k, "alloc candidate")) {
        return nullptr;
    }

    // Split down: put the upper buddy of each split into the free list.
    while (k > order) {
        k--;
        uint32_t const BUDDY_SIZE = 1U << k;
        uint32_t const BUDDY_IDX = page_idx + BUDDY_SIZE;

        // Upper buddy becomes a free head at order k.
        page_flags[BUDDY_IDX] = FLAG_FREE_HEAD | static_cast<uint8_t>(k);
        for (uint32_t i = 1; i < BUDDY_SIZE; i++) {
            page_flags[BUDDY_IDX + i] = FLAG_FREE_INTERIOR;
        }

        auto* buddy_block = reinterpret_cast<FreeBlock*>(page_to_ptr(base, BUDDY_IDX));
        insert_free_block(this, k, buddy_block);
    }

    // Mark the allocated block.
    uint32_t const BLOCK_SIZE = 1U << order;
    page_flags[page_idx] = FLAG_ALLOC_HEAD | static_cast<uint8_t>(order);
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

void* PageAllocator::alloc_order0(uint64_t caller) {
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    (void)caller;
#endif

    auto* block = free_list[0];
    if (block == nullptr) {
        return nullptr;
    }

    uint32_t page_idx = 0;
    if (!order0_free_head_is_reusable(this, block, page_idx)) {
        rebuild_free_lists_from_flags(this, "alloc_order0 corrupt head");
        block = free_list[0];
        if (block == nullptr || !order0_free_head_is_reusable(this, block, page_idx)) {
            return nullptr;
        }
    }

    if (!remove_free_block_with_repair(this, 0, block, "alloc_order0 unlink")) {
        return nullptr;
    }

    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    store_page_kind(page_kinds[page_idx], PageKind::NORMAL);
    page_refcounts[page_idx].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = caller;
#endif
    free_count -= 1;
    return page_to_ptr(base, page_idx);
}

void* PageAllocator::claim_free_order0_for_cache(uint32_t& out_page_idx) {
    out_page_idx = 0;
    auto* block = free_list[0];
    if (block == nullptr) {
        return nullptr;
    }

    uint32_t page_idx = 0;
    if (!order0_free_head_is_reusable(this, block, page_idx)) {
        rebuild_free_lists_from_flags(this, "cache claim corrupt head");
        block = free_list[0];
        if (block == nullptr || !order0_free_head_is_reusable(this, block, page_idx)) {
            return nullptr;
        }
    }

    if (!remove_free_block_with_repair(this, 0, block, "cache claim unlink")) {
        return nullptr;
    }

    page_flags[page_idx] = FLAG_CACHED_ORDER0;
    store_page_kind(page_kinds[page_idx], PageKind::FREE);
    page_refcounts[page_idx].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = 0;
#endif
    cached_order0_count += 1;
    out_page_idx = page_idx;
    return page_to_ptr(base, page_idx);
}

auto PageAllocator::cache_allocated_order0(void* ptr, uint32_t& out_page_idx) -> bool {
    out_page_idx = 0;
    if (ptr == nullptr || !ptr_in_zone(this, ptr)) {
        return false;
    }

    uint32_t const PAGE_IDX = ptr_to_page(base, ptr);
    if (PAGE_IDX >= total_pages || page_flags[PAGE_IDX] != FLAG_ALLOC_HEAD) {
        return false;
    }
    if (page_refcounts[PAGE_IDX].load(std::memory_order_acquire) != 1) {
        return false;
    }
    if (page_has_live_slab_kind(this, PAGE_IDX)) {
        report_live_slab_page_free(this, PAGE_IDX, "page cache free");
        return false;
    }
    if (page_has_live_medium_alloc_magic(this, PAGE_IDX)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, PAGE_IDX));
        dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_ptr=0x%lx\n", PAGE_BASE, reinterpret_cast<uint64_t>(ptr));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
    }
    if (page_has_live_large_alloc_magic(this, PAGE_IDX)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, PAGE_IDX));
        dbg::emergency_log("BUG: page_free on live large alloc page=0x%lx caller_ptr=0x%lx\n", PAGE_BASE, reinterpret_cast<uint64_t>(ptr));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc large alloc");
    }

    page_flags[PAGE_IDX] = FLAG_CACHED_ORDER0;
    store_page_kind(page_kinds[PAGE_IDX], PageKind::FREE);
    page_refcounts[PAGE_IDX].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[PAGE_IDX] = 0;
#endif
    free_count += 1;
    cached_order0_count += 1;
    out_page_idx = PAGE_IDX;
    return true;
}

auto PageAllocator::alloc_cached_order0_at(uint32_t page_idx, uint64_t caller) -> void* {
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    (void)caller;
#endif
    if (page_idx >= total_pages || page_flags[page_idx] != FLAG_CACHED_ORDER0) {
        return nullptr;
    }
    if (decode_page_kind(page_kinds[page_idx].load(std::memory_order_acquire)) != PageKind::FREE) {
        return nullptr;
    }
    if (page_refcounts[page_idx].load(std::memory_order_acquire) != 0) {
        return nullptr;
    }
    if (free_count == 0 || cached_order0_count == 0) {
        return nullptr;
    }

    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    store_page_kind(page_kinds[page_idx], PageKind::NORMAL);
    page_refcounts[page_idx].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = caller;
#endif
    free_count -= 1;
    cached_order0_count -= 1;
    return page_to_ptr(base, page_idx);
}

auto PageAllocator::release_cached_order0_at(uint32_t page_idx) -> uint64_t {
    if (page_idx >= total_pages || page_flags[page_idx] != FLAG_CACHED_ORDER0) {
        return 0;
    }
    if (decode_page_kind(page_kinds[page_idx].load(std::memory_order_acquire)) != PageKind::FREE) {
        return 0;
    }
    if (page_refcounts[page_idx].load(std::memory_order_acquire) != 0) {
        return 0;
    }
    if (free_count == 0 || cached_order0_count == 0) {
        return 0;
    }

    cached_order0_count -= 1;
    free_count -= 1;
    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    return free_allocated_block(this, page_idx, 0);
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
    if (page_has_live_large_alloc_magic(this, page_idx)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        dbg::emergency_log("BUG: page_free on live large alloc page=0x%lx caller_ptr=0x%lx\n", PAGE_BASE, reinterpret_cast<uint64_t>(ptr));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc large alloc");
    }

    int const ORDER = FLAGS & 0x1F;
    if (ORDER > MAX_ORDER) {
        dbg::emergency_log("page_alloc: rejecting free with invalid order=%lu ptr=0x%lx\n", static_cast<uint64_t>(ORDER),
                           reinterpret_cast<uint64_t>(ptr));
        return 0;
    }
    for (uint32_t i = 0; i < (1U << ORDER); ++i) {
        if (page_has_live_slab_kind(this, page_idx + i)) {
            report_live_slab_page_free(this, page_idx + i, "page_free");
            return 0;
        }
    }
    if (!allocated_block_has_direct_free_refs(this, page_idx, ORDER)) {
        dbg::emergency_log("page_alloc: rejecting direct free of refcounted block order=%lu ptr=0x%lx\n", static_cast<uint64_t>(ORDER),
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
    if (page_has_live_slab_kind(this, page_idx)) {
        report_live_slab_page_free(this, page_idx, "page_free_order0");
        return 0;
    }
    if (page_has_live_medium_alloc_magic(this, page_idx)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE, static_cast<uint64_t>(page_idx));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
    }
    if (page_has_live_large_alloc_magic(this, page_idx)) {
        auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, page_idx));
        dbg::emergency_log("BUG: page_free on live large alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE, static_cast<uint64_t>(page_idx));
        ker::mod::dbg::panic_handler("page_free called on live kmalloc large alloc");
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
        if (page_has_live_slab_kind(this, CUR_IDX)) {
            report_live_slab_page_free(this, CUR_IDX, "page_free_order0_range");
            return 0;
        }
        if (page_has_live_medium_alloc_magic(this, CUR_IDX)) {
            auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, CUR_IDX));
            dbg::emergency_log("BUG: page_free on live medium alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE,
                               static_cast<uint64_t>(CUR_IDX));
            ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
        }
        if (page_has_live_large_alloc_magic(this, CUR_IDX)) {
            auto const PAGE_BASE = reinterpret_cast<uint64_t>(page_to_ptr(base, CUR_IDX));
            dbg::emergency_log("BUG: page_free on live large alloc page=0x%lx caller_idx=%lu\n", PAGE_BASE, static_cast<uint64_t>(CUR_IDX));
            ker::mod::dbg::panic_handler("page_free called on live kmalloc large alloc");
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
    PageKind const KIND = decode_page_kind(page_kinds[PAGE_IDX].load(std::memory_order_acquire));
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

    return decode_page_kind(page_kinds[PAGE_IDX].load(std::memory_order_acquire));
}

}  // namespace ker::mod::mm
