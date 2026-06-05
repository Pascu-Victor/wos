#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace ker::mod::mm {

enum class PageKind : uint8_t {
    UNKNOWN = 0,
    FREE = 1,
    RESERVED = 2,
    NORMAL = 3,
    PAGE_TABLE = 4,
    SLAB = 5,
    MEDIUM = 6,
};

// Linux-style free-list buddy page allocator.
// Manages a contiguous physical memory zone. Metadata is embedded at the
// beginning of the zone (this struct + side tables), consuming a small fixed
// fraction of the zone.
//
// All allocations are 4KB-page-aligned and sized in powers-of-two pages.
// Free uses per-page flags to recover the allocation order, so callers do
// not need to pass the size.

struct PageAllocator {
    struct FreeBlock {
        FreeBlock* next;
    };

    // 2^MAX_ORDER pages = 4 GiB max contiguous allocation
    static constexpr int MAX_ORDER = 20;

    // Per-page flag byte encoding
    //   bits 7-6  meaning
    //   -------  -------
    //     00     interior of a free block (non-head page)
    //     01     free block head   (bits 4-0 = order)
    //     10     allocated head    (bits 4-0 = order)
    //     11     allocated continuation / reserved
    static constexpr uint8_t FLAG_FREE_INTERIOR = 0x00;
    static constexpr uint8_t FLAG_FREE_HEAD = 0x40;   // | order
    static constexpr uint8_t FLAG_ALLOC_HEAD = 0x80;  // | order
    static constexpr uint8_t FLAG_ALLOC_CONT = 0xC0;
    static constexpr uint8_t FLAG_RESERVED = 0xFF;

    std::array<FreeBlock*, MAX_ORDER + 1> free_list{};  // one singly-linked list per order
    uint8_t* page_flags = nullptr;                      // 1 byte per page
    std::atomic<uint8_t>* page_kinds = nullptr;         // PageKind per page
    std::atomic<uint32_t>* page_refcounts = nullptr;    // 1 refcount per page (for COW fork)
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t* page_callers = nullptr;  // allocation return address for each live page
#endif
    uint64_t base = 0;            // HHDM start of the managed region
    uint32_t total_pages = 0;     // total pages in the region (incl. metadata)
    uint32_t usable_pages = 0;    // pages available for allocation
    uint32_t free_count = 0;      // current free page count
    uint32_t metadata_pages = 0;  // pages consumed by metadata

    // Initialise this allocator over the zone starting at `zoneBase`
    // (HHDM address) with `sizeBytes` total bytes.  Metadata is placed at
    // the beginning; the rest becomes allocatable.
    void init(uint64_t zone_base, uint64_t size_bytes);

    // Allocate >= sizeBytes of contiguous physical pages (rounded up to the
    // next power-of-two page count).  Returns an HHDM pointer or nullptr on
    // failure.
    void* alloc(uint64_t size_bytes, uint64_t caller = 0);

    // Free a previous allocation.  The allocation order is recovered from the
    // per-page flags - callers do not need to supply the size. Returns the
    // number of bytes released, or 0 if the pointer was not a live allocation.
    uint64_t free(void* ptr);

    // Fast path for callers that already resolved allocator/index under the
    // global physical allocator lock. Only frees a live order-0 page.
    uint64_t free_order0_at(uint32_t page_idx);

    // Re-tag a contiguous allocated block as a run of independently freeable
    // order-0 pages while preserving the existing per-page refcounts.
    // Use this when a multi-page allocation will be mapped/freed as separate
    // 4 KiB leaves.
    auto split_allocated_block_to_order0(void* ptr) const -> bool;

    // Mark/query the kind metadata for a live allocation. Marking applies to
    // every page in the allocation recovered from the buddy head flag.
    auto mark_allocated_block_kind(void* ptr, PageKind kind) const -> bool;
    [[nodiscard]] auto kind_of(void* ptr) const -> PageKind;

    [[nodiscard]] __attribute__((no_sanitize("address"))) uint32_t get_free_pages() const { return free_count; }
    [[nodiscard]] __attribute__((no_sanitize("address"))) uint32_t get_usable_pages() const { return usable_pages; }
};

}  // namespace ker::mod::mm
