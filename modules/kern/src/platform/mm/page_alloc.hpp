#pragma once

#include <cstdint>

namespace ker::mod::mm {

// Linux-style free-list buddy page allocator.
// Manages a contiguous physical memory zone. Metadata is embedded at the
// beginning of the zone (pageFlags array + this struct), consuming a small
// fixed fraction (~0.024%) of the zone.
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
    static constexpr uint8_t FLAG_FREE_HEAD     = 0x40;   // | order
    static constexpr uint8_t FLAG_ALLOC_HEAD    = 0x80;   // | order
    static constexpr uint8_t FLAG_ALLOC_CONT    = 0xC0;
    static constexpr uint8_t FLAG_RESERVED      = 0xFF;

    FreeBlock* freeList[MAX_ORDER + 1];   // one singly-linked list per order
    uint8_t*   pageFlags;                 // 1 byte per page
    uint64_t   base;                      // HHDM start of the managed region
    uint32_t   totalPages;                // total pages in the region (incl. metadata)
    uint32_t   usablePages;               // pages available for allocation
    uint32_t   freeCount;                 // current free page count
    uint32_t   metadataPages;             // pages consumed by metadata

    // Initialise this allocator over the zone starting at `zoneBase`
    // (HHDM address) with `sizeBytes` total bytes.  Metadata is placed at
    // the beginning; the rest becomes allocatable.
    void init(uint64_t zoneBase, uint64_t sizeBytes);

    // Allocate >= sizeBytes of contiguous physical pages (rounded up to the
    // next power-of-two page count).  Returns an HHDM pointer or nullptr on
    // failure.
    void* alloc(uint64_t sizeBytes);

    // Free a previous allocation.  The allocation order is recovered from the
    // per-page flags — callers do not need to supply the size.
    void free(void* ptr);

    uint32_t getFreePages()   const { return freeCount; }
    uint32_t getUsablePages() const { return usablePages; }
};

}  // namespace ker::mod::mm
