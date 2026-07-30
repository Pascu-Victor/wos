#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ker::mod::mm {

enum class PhysicalPageOwner : uint8_t {
    INVALID = 0,
    USER_PRIVATE_MAPPING,
    USER_FILE_MAPPING,
    USER_FILE_CACHE,
    USER_EXECUTABLE_MAPPING,
    USER_THREAD_STACK,
    USER_THREAD_TLS,
    USER_SHARED_MEMORY,
    ANON_ZERO_PAGE_RESERVE,
    PAGE_TABLE,
    PAGE_TABLE_POOL_RESERVE,
    PHYSICAL_ALLOCATOR_METADATA,
    KERNEL_STACK,
    KERNEL_STACK_POOL,
    KMALLOC_SLAB,
    KMALLOC_MEDIUM,
    KMALLOC_LARGE,
    KMALLOC_DEBUG,
    BUFFER_CACHE_DATA,
    BUFFER_CACHE_METADATA_RESERVE,
    BUFFER_CACHE_METADATA,
    TMPFS_DATA,
    XFS_INODE_METADATA_RESERVE,
    XFS_INODE_METADATA,
    XFS_TRANSACTION_METADATA_RESERVE,
    XFS_TRANSACTION_METADATA,
    WKI_ZONE,
    NETWORK_PACKET_RESERVE,
    NETWORK_PACKET,
    DEVICE_AHCI,
    DEVICE_VIRTIO,
    DEVICE_E1000,
    DEVICE_USB,
    KASAN_SHADOW,
    KCOV_BUFFER,
    DEBUGGER,
    SELFTEST,
    COUNT,
};

inline constexpr size_t PHYSICAL_PAGE_OWNER_COUNT = static_cast<size_t>(PhysicalPageOwner::COUNT);

[[nodiscard]] constexpr auto physical_page_owner_is_valid(PhysicalPageOwner owner) -> bool {
    return owner > PhysicalPageOwner::INVALID && owner < PhysicalPageOwner::COUNT;
}

enum class PageKind : uint8_t {
    UNKNOWN = 0,
    FREE = 1,
    RESERVED = 2,
    NORMAL = 3,
    PAGE_TABLE = 4,
    SLAB = 5,
    MEDIUM = 6,
    KMALLOC_LARGE = 7,
};

[[nodiscard]] constexpr auto decode_page_kind(uint8_t value) -> PageKind {
    switch (static_cast<PageKind>(value)) {
        case PageKind::UNKNOWN:
        case PageKind::FREE:
        case PageKind::RESERVED:
        case PageKind::NORMAL:
        case PageKind::PAGE_TABLE:
        case PageKind::SLAB:
        case PageKind::MEDIUM:
        case PageKind::KMALLOC_LARGE:
            return static_cast<PageKind>(value);
        default:
            return PageKind::UNKNOWN;
    }
}

[[nodiscard]] constexpr auto page_kind_has_known_live_payload(PageKind kind) -> bool {
    switch (kind) {
        case PageKind::NORMAL:
        case PageKind::PAGE_TABLE:
        case PageKind::SLAB:
        case PageKind::MEDIUM:
        case PageKind::KMALLOC_LARGE:
            return true;
        case PageKind::UNKNOWN:
        case PageKind::FREE:
        case PageKind::RESERVED:
        default:
            return false;
    }
}

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
        FreeBlock* prev;
        FreeBlock* next;
    };

    // 2^MAX_ORDER pages = 4 GiB max contiguous allocation
    static constexpr int MAX_ORDER = 20;

    // Per-page flag byte encoding
    //   bits 7-6  meaning
    //   -------  -------
    //     00     interior of a free block (non-head page)
    //     00+0x20 cached order-0 free page (not linked in buddy lists)
    //     01     free block head   (bits 4-0 = order)
    //     10     allocated head    (bits 4-0 = order)
    //     11     allocated continuation / reserved
    static constexpr uint8_t FLAG_FREE_INTERIOR = 0x00;
    static constexpr uint8_t FLAG_CACHED_ORDER0 = 0x20;
    static constexpr uint8_t FLAG_FREE_HEAD = 0x40;   // | order
    static constexpr uint8_t FLAG_ALLOC_HEAD = 0x80;  // | order
    static constexpr uint8_t FLAG_ALLOC_CONT = 0xC0;
    static constexpr uint8_t FLAG_RESERVED = 0xFF;

    std::array<FreeBlock*, MAX_ORDER + 1> free_list{};  // one doubly-linked list per order
    std::atomic<bool> lock_held{false};                 // protects free_list/page_flags/link mutations
    uint8_t* page_flags = nullptr;                      // 1 byte per page
    std::atomic<uint8_t>* page_kinds = nullptr;         // PageKind per page
    uint8_t* page_owners = nullptr;                     // PhysicalPageOwner per page
    std::atomic<uint32_t>* page_refcounts = nullptr;    // 1 refcount per page (for COW fork)
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    uint64_t* page_callers = nullptr;  // allocation return address for each live page
#endif
    uint64_t base = 0;          // HHDM start of the managed region
    uint32_t total_pages = 0;   // total pages in the region (incl. metadata)
    uint32_t usable_pages = 0;  // pages available for allocation
    uint32_t free_count = 0;    // current free page count
    uint32_t cached_order0_count = 0;
    uint32_t metadata_pages = 0;  // pages consumed by metadata
    std::array<uint64_t, PHYSICAL_PAGE_OWNER_COUNT> owner_pages{};
    std::array<uint64_t, PHYSICAL_PAGE_OWNER_COUNT> owner_objects{};

    // Initialise this allocator over the zone starting at `zoneBase`
    // (HHDM address) with `sizeBytes` total bytes.  Metadata is placed at
    // the beginning; the rest becomes allocatable.
    void init(uint64_t zone_base, uint64_t size_bytes);

    auto lock_irq() -> uint64_t;
    void unlock_irq(uint64_t flags);

    // Allocate >= sizeBytes of contiguous physical pages (rounded up to the
    // next power-of-two page count).  Returns an HHDM pointer or nullptr on
    // failure.
    void* alloc(uint64_t size_bytes, PhysicalPageOwner owner, uint64_t caller = 0);

    // Allocate exactly one physical page from an already-split order-0 free
    // list head. The caller must hold this allocator's lock. Returns nullptr
    // when no proven order-0 page is immediately available.
    void* alloc_order0(PhysicalPageOwner owner, uint64_t caller = 0);

    // Per-CPU order-0 cache transitions. Callers must hold this allocator's
    // lock. Cached pages are free capacity, but are deliberately not linked
    // into buddy lists, so coalescing cannot consume a CPU-local cache entry.
    auto claim_free_order0_for_cache(uint32_t& out_page_idx) -> void*;
    auto cache_allocated_order0(void* ptr, uint32_t& out_page_idx) -> bool;
    auto alloc_cached_order0_at(uint32_t page_idx, PhysicalPageOwner owner, uint64_t caller = 0) -> void*;
    auto release_cached_order0_at(uint32_t page_idx) -> uint64_t;

    // Free a previous allocation.  The allocation order is recovered from the
    // per-page flags - callers do not need to supply the size. Returns the
    // number of bytes released, or 0 if the pointer was not a live allocation.
    uint64_t free(void* ptr);

    // Fast path for callers that already resolved allocator/index and hold
    // the owning PageAllocator lock. Only frees a live order-0 page.
    uint64_t free_order0_at(uint32_t page_idx);

    // Batch fast path for callers that already hold the owning PageAllocator
    // lock. Frees a contiguous run of live zero-ref order-0 pages.
    uint64_t free_order0_range_at(uint32_t page_idx, uint32_t page_count);

    // Re-tag a contiguous allocated block as a run of independently freeable
    // order-0 pages while preserving the existing per-page kind/refcount/caller
    // metadata. Call this before exposing a multi-page allocation through PTEs
    // that teardown will later reclaim as separate 4 KiB leaves.
    auto split_allocated_block_to_order0(void* ptr) -> bool;

    // Mark/query the kind metadata for a live allocation. Marking applies to
    // every page in the allocation recovered from the buddy head flag.
    auto mark_allocated_block_kind(void* ptr, PageKind kind) const -> bool;
    auto reassign_allocated_block_owner(void* ptr, PhysicalPageOwner owner) -> bool;
    [[nodiscard]] auto kind_of(void* ptr) const -> PageKind;

    [[nodiscard]] __attribute__((no_sanitize("address"))) uint32_t get_free_pages() const { return free_count; }
    [[nodiscard]] __attribute__((no_sanitize("address"))) uint32_t get_usable_pages() const { return usable_pages; }
};

}  // namespace ker::mod::mm
