#include "page_alloc.hpp"

#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/paging.hpp>

namespace ker::mod::mm {

// ============================================================================
// Helpers
// ============================================================================

// Smallest order k such that (1 << k) pages >= the requested byte count.
static inline int sizeToOrder(uint64_t sizeBytes) {
    uint64_t pages = (sizeBytes + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;
    if (pages <= 1) return 0;
    // __builtin_clzll(0) is undefined, but pages >= 2 here.
    int bits = 64 - __builtin_clzll(pages - 1);
    if (bits > PageAllocator::MAX_ORDER) bits = PageAllocator::MAX_ORDER;
    return bits;
}

// Convert a page index to the HHDM pointer inside the zone.
static inline void* pageToPtr(uint64_t base, uint32_t pageIdx) {
    return reinterpret_cast<void*>(base + (uint64_t)pageIdx * paging::PAGE_SIZE);
}

// Convert an HHDM pointer to a page index relative to the zone base.
static inline uint32_t ptrToPage(uint64_t base, void* ptr) { return (uint32_t)(((uint64_t)ptr - base) / paging::PAGE_SIZE); }

static inline bool ptrIsPageAligned(const void* ptr) { return (((uint64_t)ptr) & (paging::PAGE_SIZE - 1)) == 0; }

static bool ptrInZone(const PageAllocator* alloc, const void* ptr) {
    if (ptr == nullptr || !ptrIsPageAligned(ptr)) return false;

    const uint64_t addr = reinterpret_cast<uint64_t>(ptr);
    const uint64_t end = alloc->base + (uint64_t)alloc->totalPages * paging::PAGE_SIZE;
    return addr >= alloc->base && addr < end;
}

static bool freeHeadIsValid(const PageAllocator* alloc, PageAllocator::FreeBlock* block, int order, uint32_t& pageIdx) {
    if (!ptrInZone(alloc, block)) return false;

    pageIdx = ptrToPage(alloc->base, block);
    const uint32_t blockSize = 1u << order;
    if (pageIdx >= alloc->totalPages || pageIdx + blockSize > alloc->totalPages) return false;

    return alloc->pageFlags[pageIdx] == (PageAllocator::FLAG_FREE_HEAD | (uint8_t)order);
}

static void dropCorruptFreeListHead(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block) {
    ker::mod::io::serial::write("page_alloc: dropping corrupt free-list head order=");
    ker::mod::io::serial::write((uint64_t)order);
    ker::mod::io::serial::write(" ptr=0x");
    ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(block));
    ker::mod::io::serial::write(" zone_base=0x");
    ker::mod::io::serial::write_hex(alloc->base);
    ker::mod::io::serial::write("\n");
    alloc->freeList[order] = nullptr;
}

// Remove a specific FreeBlock from a singly-linked list.
// Returns true if found and removed.
static bool listRemove(PageAllocator* alloc, int order, PageAllocator::FreeBlock*& head, PageAllocator::FreeBlock* target) {
    PageAllocator::FreeBlock** prev = &head;
    PageAllocator::FreeBlock* cur = head;
    while (cur != nullptr) {
        uint32_t curIdx = 0;
        if (!freeHeadIsValid(alloc, cur, order, curIdx)) {
            ker::mod::io::serial::write("page_alloc: corrupt free-list node while removing order=");
            ker::mod::io::serial::write((uint64_t)order);
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

// ============================================================================
// init
// ============================================================================

void PageAllocator::init(uint64_t zoneBase, uint64_t sizeBytes) {
    base = zoneBase;
    totalPages = (uint32_t)(sizeBytes / paging::PAGE_SIZE);

    // --- lay out metadata at the beginning of the zone ---

    // PageAllocator struct occupies bytes [0, sizeof(*this)).
    // pageFlags array immediately after, then pageRefcounts array.
    uint64_t flagsOffset = sizeof(PageAllocator);
    pageFlags = reinterpret_cast<uint8_t*>(zoneBase + flagsOffset);

    uint64_t refcountsOffset = flagsOffset + (uint64_t)totalPages;
    // Align refcounts to 4-byte boundary for uint32_t access
    refcountsOffset = (refcountsOffset + 3) & ~3ULL;
    pageRefcounts = reinterpret_cast<uint32_t*>(zoneBase + refcountsOffset);

    uint64_t metaBytes = refcountsOffset + (uint64_t)totalPages * sizeof(uint32_t);
    metadataPages = (uint32_t)((metaBytes + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE);

    if (metadataPages >= totalPages) {
        // Zone too small to hold any usable pages.
        usablePages = 0;
        freeCount = 0;
        for (int i = 0; i <= MAX_ORDER; i++) freeList[i] = nullptr;
        memset(pageFlags, FLAG_RESERVED, totalPages);
        memset(pageRefcounts, 0, totalPages * sizeof(uint32_t));
        return;
    }

    usablePages = totalPages - metadataPages;
    freeCount = 0;

    // Zero the free lists.
    for (int i = 0; i <= MAX_ORDER; i++) freeList[i] = nullptr;

    // Mark metadata pages as reserved, all others as allocated-continuation
    // (prevents false buddy matches during the decomposition loop below).
    memset(pageFlags, FLAG_RESERVED, metadataPages);
    memset(pageFlags + metadataPages, FLAG_ALLOC_CONT, totalPages - metadataPages);
    // Zero all refcounts (free pages have refcount 0)
    memset(pageRefcounts, 0, totalPages * sizeof(uint32_t));

    // --- decompose usable range into largest aligned power-of-2 blocks ---

    uint32_t page = metadataPages;
    while (page < totalPages) {
        // Find largest order where the page is naturally aligned and
        // the block fits within the zone.
        int order = 0;
        while (order < MAX_ORDER) {
            uint32_t blockSize = 1u << (order + 1);
            if ((page & (blockSize - 1)) != 0) break;  // not aligned
            if (page + blockSize > totalPages) break;  // doesn't fit
            order++;
        }

        uint32_t blockSize = 1u << order;

        // Mark head page as free-head, interior pages as free-interior.
        pageFlags[page] = FLAG_FREE_HEAD | (uint8_t)order;
        for (uint32_t i = 1; i < blockSize; i++) {
            pageFlags[page + i] = FLAG_FREE_INTERIOR;
        }

        // Prepend to the order's free list (link through the page itself).
        auto* block = reinterpret_cast<FreeBlock*>(pageToPtr(base, page));
        block->next = freeList[order];
        freeList[order] = block;

        freeCount += blockSize;
        page += blockSize;
    }
}

// ============================================================================
// alloc
// ============================================================================

void* PageAllocator::alloc(uint64_t sizeBytes) {
    int order = sizeToOrder(sizeBytes);

    // Walk up to find the smallest available block >= requested order.
    int k = order;
    uint32_t pageIdx = 0;
    FreeBlock* block = nullptr;
    while (k <= MAX_ORDER) {
        block = freeList[k];
        if (block == nullptr) {
            k++;
            continue;
        }

        if (!freeHeadIsValid(this, block, k, pageIdx)) {
            dropCorruptFreeListHead(this, k, block);
            k++;
            continue;
        }

        // Pop head of freeList[k].
        freeList[k] = block->next;
        break;
    }

    if (k > MAX_ORDER) return nullptr;  // OOM

    // Split down: put the upper buddy of each split into the free list.
    while (k > order) {
        k--;
        uint32_t buddySize = 1u << k;
        uint32_t buddyIdx = pageIdx + buddySize;

        // Upper buddy becomes a free head at order k.
        pageFlags[buddyIdx] = FLAG_FREE_HEAD | (uint8_t)k;
        for (uint32_t i = 1; i < buddySize; i++) {
            pageFlags[buddyIdx + i] = FLAG_FREE_INTERIOR;
        }

        auto* buddyBlock = reinterpret_cast<FreeBlock*>(pageToPtr(base, buddyIdx));
        buddyBlock->next = freeList[k];
        freeList[k] = buddyBlock;
    }

    // Mark the allocated block.
    uint32_t blockSize = 1u << order;
    pageFlags[pageIdx] = FLAG_ALLOC_HEAD | (uint8_t)order;
    for (uint32_t i = 1; i < blockSize; i++) {
        pageFlags[pageIdx + i] = FLAG_ALLOC_CONT;
    }

    // Set refcount to 1 for all pages in the block
    for (uint32_t i = 0; i < blockSize; i++) {
        pageRefcounts[pageIdx + i] = 1;
    }

    freeCount -= blockSize;
    return reinterpret_cast<void*>(base + (uint64_t)pageIdx * paging::PAGE_SIZE);
}

// ============================================================================
// free
// ============================================================================

void PageAllocator::free(void* ptr) {
    if (ptr == nullptr) return;
    if (!ptrInZone(this, ptr)) {
        ker::mod::io::serial::write("page_alloc: rejecting free outside zone ptr=0x");
        ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(ptr));
        ker::mod::io::serial::write(" zone_base=0x");
        ker::mod::io::serial::write_hex(base);
        ker::mod::io::serial::write("\n");
        return;
    }

    uint32_t pageIdx = ptrToPage(base, ptr);
    if (pageIdx >= totalPages) return;

    uint8_t flags = pageFlags[pageIdx];

    // Must be an allocated head page.
    if ((flags & 0xC0) != 0x80) return;  // not FLAG_ALLOC_HEAD

    // Guard: detect page_free on a live kmalloc medium allocation.
    // The MediumAllocationHeader layout is: next(8) size(8) magic(8) pad(8).
    // Magic lives at offset +16 from the page base.
    {
        constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;
        const auto page_base = reinterpret_cast<uint64_t>(pageToPtr(base, pageIdx));
        if (*reinterpret_cast<const uint64_t*>(page_base + 16) == MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("BUG: page_free on live medium alloc page=0x");
            ker::mod::io::serial::write_hex(page_base);
            ker::mod::io::serial::write(" caller_ptr=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(ptr));
            ker::mod::io::serial::write("\n");
            ker::mod::dbg::panic_handler("page_free called on live kmalloc medium alloc");
        }
    }

    int order = flags & 0x1F;
    uint32_t blockSize = 1u << order;

    // Clear flags for the entire allocation.
    for (uint32_t i = 0; i < blockSize; i++) {
        pageFlags[pageIdx + i] = FLAG_FREE_INTERIOR;
        pageRefcounts[pageIdx + i] = 0;
    }

    freeCount += blockSize;

    // Coalesce with buddies.
    int k = order;
    while (k < MAX_ORDER) {
        uint32_t buddyIdx = pageIdx ^ (1u << k);

        // Buddy must be within the zone and be a free head at exactly order k.
        if (buddyIdx >= totalPages) break;
        if (pageFlags[buddyIdx] != (FLAG_FREE_HEAD | (uint8_t)k)) break;

        // Remove buddy from its free list.
        auto* buddyBlock = reinterpret_cast<FreeBlock*>(pageToPtr(base, buddyIdx));
        if (!listRemove(this, k, freeList[k], buddyBlock)) break;

        // Clear buddy's head flag (it becomes an interior page of the merged block).
        pageFlags[buddyIdx] = FLAG_FREE_INTERIOR;

        // The merged block starts at the lower-aligned address.
        if (buddyIdx < pageIdx) pageIdx = buddyIdx;

        k++;
    }

    // Mark the (possibly merged) block as a free head.
    pageFlags[pageIdx] = FLAG_FREE_HEAD | (uint8_t)k;

    // Prepend to the free list.
    auto* freeBlock = reinterpret_cast<FreeBlock*>(pageToPtr(base, pageIdx));
    freeBlock->next = freeList[k];
    freeList[k] = freeBlock;
}

auto PageAllocator::splitAllocatedBlockToOrder0(void* ptr) -> bool {
    if (ptr == nullptr || !ptrInZone(this, ptr)) return false;

    uint32_t pageIdx = ptrToPage(base, ptr);
    if (pageIdx >= totalPages) return false;

    uint8_t flags = pageFlags[pageIdx];
    if ((flags & 0xC0) != FLAG_ALLOC_HEAD) return false;

    int order = flags & 0x1F;
    if (order == 0) return true;

    uint32_t blockSize = 1u << order;
    if (pageIdx + blockSize > totalPages) return false;

    for (uint32_t i = 0; i < blockSize; ++i) {
        pageFlags[pageIdx + i] = FLAG_ALLOC_HEAD;
        if (pageRefcounts[pageIdx + i] == 0) {
            pageRefcounts[pageIdx + i] = 1;
        }
    }

    return true;
}

}  // namespace ker::mod::mm
