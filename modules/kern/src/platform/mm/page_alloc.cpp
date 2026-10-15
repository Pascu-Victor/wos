#include "page_alloc.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/acpi/numa_topology.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/tlb_shootdown.hpp>

#ifdef WOS_SELFTEST
namespace ker::mod::mm::virt {
bool selftest_direct_map_contains(const void* ptr);
}
#endif

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

void store_page_owner(uint8_t& slot, PhysicalPageOwner owner) { slot = static_cast<uint8_t>(owner); }

auto load_page_owner(const uint8_t& slot) -> PhysicalPageOwner {
    auto const OWNER = static_cast<PhysicalPageOwner>(slot);
    return physical_page_owner_is_valid(OWNER) ? OWNER : PhysicalPageOwner::INVALID;
}

auto owner_index(PhysicalPageOwner owner) -> size_t { return static_cast<size_t>(owner); }

void account_owner_alloc(PageAllocator* alloc, PhysicalPageOwner owner, uint64_t pages, uint64_t objects = 1) {
    if (!physical_page_owner_is_valid(owner)) {
        ker::mod::dbg::panic_handler("physical allocation requires a concrete owner");
    }
    alloc->owner_pages.at(owner_index(owner)) += pages;
    alloc->owner_objects.at(owner_index(owner)) += objects;
}

void account_owner_free(PageAllocator* alloc, PhysicalPageOwner owner, uint64_t pages, uint64_t objects = 1) {
    if (!physical_page_owner_is_valid(owner) || alloc->owner_pages.at(owner_index(owner)) < pages ||
        alloc->owner_objects.at(owner_index(owner)) < objects) {
        ker::mod::dbg::panic_handler("physical owner accounting underflow");
    }
    alloc->owner_pages.at(owner_index(owner)) -= pages;
    alloc->owner_objects.at(owner_index(owner)) -= objects;
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

// Raw counter helpers stay in this allocator TU's existing diagnostic profile.
// Counter arrays are raw; the owner view and compiler clear provider are audited separately.
struct NodeDelta {
    uint32_t pages[PAGE_ALLOCATOR_MAX_NODES];  // Only mask-selected slots initialized/read.
    uint64_t mask;
};

// Existing owner totals retain their std::array layout. Obtain the standard
// contiguous view, then use built-in indexing. The existing kernel std/header
// sanitizer ignorelist and generated provider proof cover this one accessor;
// no pointer reinterpretation or new diagnostic exclusion is needed.
auto owner_page_totals(const PageAllocator* alloc) noexcept -> const uint64_t* { return alloc->owner_pages.data(); }

auto valid_node_ranges(const acpi::numa::MemoryRange* ranges, uint16_t range_count, uint16_t node_count) noexcept -> NodeCounterError {
    if (ranges == nullptr || range_count == 0 || node_count == 0) return NodeCounterError::INVALID_INPUT;
    if (range_count > PAGE_ALLOCATOR_MAX_NODE_RANGES || node_count > PAGE_ALLOCATOR_MAX_NODES) return NodeCounterError::CAPACITY;
    uint64_t previous_end = 0;
    for (uint16_t i = 0; i < range_count; ++i) {
        auto const& range = ranges[i];
        if (range.length == 0 || ((range.base | range.length) & (paging::PAGE_SIZE - 1)) != 0 || range.length > UINT64_MAX - range.base ||
            range.node >= node_count || (i != 0 && range.base < previous_end))
            return NodeCounterError::INVALID_INPUT;
        previous_end = range.base + range.length;
    }
    return NodeCounterError::NONE;
}

auto node_span_pages(uint64_t physical_base, uint32_t first, uint32_t count, const acpi::numa::MemoryRange* ranges, uint16_t range_count,
                     uint16_t node_count, NodeDelta& delta) noexcept -> bool {
    delta.mask = 0;
    if (count == 0) return true;
    uint64_t const OFFSET = static_cast<uint64_t>(first) * paging::PAGE_SIZE;
    uint64_t const BYTES = static_cast<uint64_t>(count) * paging::PAGE_SIZE;
    if (OFFSET > UINT64_MAX - physical_base || BYTES > UINT64_MAX - physical_base - OFFSET) return false;
    uint64_t cursor = physical_base + OFFSET;
    uint64_t const END = cursor + BYTES;
    size_t low = 0, high = range_count;
    while (low < high) {
        size_t const MID = low + (high - low) / 2;
        if (ranges[MID].base + ranges[MID].length <= cursor)
            low = MID + 1;
        else
            high = MID;
    }
    while (cursor < END) {
        if (low >= range_count) return false;
        auto const& range = ranges[low];
        if (range.base > cursor || range.node >= node_count || range.node >= PAGE_ALLOCATOR_MAX_NODES) return false;
        uint64_t const RANGE_END = range.base + range.length;
        if (RANGE_END <= cursor) return false;
        uint64_t const NEXT = RANGE_END < END ? RANGE_END : END;
        uint64_t const PAGES = (NEXT - cursor) / paging::PAGE_SIZE;
        uint64_t const BIT = uint64_t{1} << range.node;
        if ((delta.mask & BIT) == 0) {
            delta.pages[range.node] = 0;
            delta.mask |= BIT;
        }
        if (PAGES > UINT32_MAX - delta.pages[range.node]) return false;
        delta.pages[range.node] += static_cast<uint32_t>(PAGES);
        cursor = NEXT;
        ++low;
    }
    return true;
}

auto node_binding_sane(const PageAllocator* alloc) noexcept -> bool {
    auto const& store = alloc->node_counters;
    if (store.node_count == 0) return store.ranges == nullptr && store.range_count == 0 && store.reserved == 0;
    uint64_t const BYTES = static_cast<uint64_t>(alloc->total_pages) * paging::PAGE_SIZE;
    return store.node_count <= PAGE_ALLOCATOR_MAX_NODES && store.ranges != nullptr && store.range_count != 0 &&
           store.range_count <= PAGE_ALLOCATOR_MAX_NODE_RANGES && store.reserved == 0 &&
           (store.physical_base & (paging::PAGE_SIZE - 1)) == 0 && BYTES <= UINT64_MAX - store.physical_base;
}

auto prepare_node_delta(const PageAllocator* alloc, uint32_t first, uint32_t count, int free_sign, int cached_sign,
                        NodeDelta& delta) noexcept -> bool {
    delta.mask = 0;
    auto const& store = alloc->node_counters;
    if (store.node_count == 0) return true;  // Exact legacy bootstrap before topology.
    if (store.node_count > PAGE_ALLOCATOR_MAX_NODES || store.range_count == 0 || store.range_count > PAGE_ALLOCATOR_MAX_NODE_RANGES ||
        store.ranges == nullptr || store.reserved != 0 || first > alloc->total_pages || count > alloc->total_pages - first)
        return false;
    int64_t const FREE = static_cast<int64_t>(alloc->free_count) + static_cast<int64_t>(free_sign) * count;
    int64_t const CACHED = static_cast<int64_t>(alloc->cached_order0_count) + static_cast<int64_t>(cached_sign) * count;
    if (FREE < 0 || CACHED < 0 || CACHED > FREE || FREE > alloc->usable_pages ||
        !node_span_pages(store.physical_base, first, count, store.ranges, store.range_count, store.node_count, delta))
        return false;
    uint64_t mask = delta.mask;
    while (mask != 0) {
        unsigned const NODE = static_cast<unsigned>(__builtin_ctzll(mask));
        mask &= mask - 1;
        int64_t const NODE_FREE = static_cast<int64_t>(store.free_inclusive[NODE]) + static_cast<int64_t>(free_sign) * delta.pages[NODE];
        int64_t const NODE_CACHED = static_cast<int64_t>(store.cached[NODE]) + static_cast<int64_t>(cached_sign) * delta.pages[NODE];
        if (NODE_FREE < 0 || NODE_CACHED < 0 || NODE_CACHED > NODE_FREE || NODE_FREE > store.usable[NODE]) return false;
    }
    return true;
}

void apply_node_delta(PageAllocator* alloc, const NodeDelta& delta, int free_sign, int cached_sign) noexcept {
    uint64_t mask = delta.mask;
    while (mask != 0) {
        unsigned const NODE = static_cast<unsigned>(__builtin_ctzll(mask));
        mask &= mask - 1;
        alloc->node_counters.free_inclusive[NODE] = static_cast<uint32_t>(static_cast<int64_t>(alloc->node_counters.free_inclusive[NODE]) +
                                                                          static_cast<int64_t>(free_sign) * delta.pages[NODE]);
        alloc->node_counters.cached[NODE] = static_cast<uint32_t>(static_cast<int64_t>(alloc->node_counters.cached[NODE]) +
                                                                  static_cast<int64_t>(cached_sign) * delta.pages[NODE]);
    }
}

void clear_node_seed(NodeCounterSeed& seed) noexcept {
    for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node) seed.usable[node] = seed.free_inclusive[node] = seed.cached[node] = 0;
}

auto prepare_node_seed(const PageAllocator* alloc, uint64_t physical_base, const acpi::numa::MemoryRange* ranges, uint16_t range_count,
                       uint16_t node_count, NodeCounterSeed& seed) noexcept -> NodeCounterError {
    auto const VIEW = valid_node_ranges(ranges, range_count, node_count);
    if (VIEW != NodeCounterError::NONE) return VIEW;
    uint64_t const BYTES = static_cast<uint64_t>(alloc->total_pages) * paging::PAGE_SIZE;
    if (alloc->total_pages == 0 || ((physical_base | alloc->base) & (paging::PAGE_SIZE - 1)) != 0 || BYTES > UINT64_MAX - physical_base ||
        BYTES > UINT64_MAX - alloc->base || alloc->metadata_pages > alloc->total_pages ||
        alloc->usable_pages != alloc->total_pages - alloc->metadata_pages || alloc->page_flags == nullptr || alloc->page_kinds == nullptr ||
        alloc->page_owners == nullptr || alloc->page_refcounts == nullptr)
        return NodeCounterError::INVALID_INPUT;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    if (alloc->page_callers == nullptr) return NodeCounterError::INVALID_INPUT;
#endif
    uint64_t owned = 0;
    auto const* owner_pages = owner_page_totals(alloc);
    for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        if (owner_pages[owner] > alloc->usable_pages - owned) return NodeCounterError::COUNTERS;
        owned += owner_pages[owner];
    }
    if (alloc->free_count > alloc->usable_pages || owned + alloc->free_count != alloc->usable_pages) return NodeCounterError::COUNTERS;
    NodeDelta delta;
    if (!node_span_pages(physical_base, 0, alloc->total_pages, ranges, range_count, node_count, delta)) return NodeCounterError::COVERAGE;
    clear_node_seed(seed);
    if (!node_span_pages(physical_base, alloc->metadata_pages, alloc->usable_pages, ranges, range_count, node_count, delta))
        return NodeCounterError::COVERAGE;
    uint64_t mask = delta.mask;
    while (mask != 0) {
        unsigned const NODE = static_cast<unsigned>(__builtin_ctzll(mask));
        mask &= mask - 1;
        seed.usable[NODE] = delta.pages[NODE];
    }
    for (uint32_t i = 0; i < alloc->metadata_pages; ++i)
        if (alloc->page_flags[i] != PageAllocator::FLAG_RESERVED ||
            alloc->page_kinds[i].load(std::memory_order_relaxed) != static_cast<uint8_t>(PageKind::RESERVED) ||
            alloc->page_owners[i] != 0 || alloc->page_refcounts[i].load(std::memory_order_relaxed) != 0)
            return NodeCounterError::METADATA;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    for (uint32_t i = 0; i < alloc->metadata_pages; ++i)
        if (alloc->page_callers[i] != 0) return NodeCounterError::METADATA;
#endif
    uint64_t total_free = 0, total_cached = 0;
    for (uint32_t first = alloc->metadata_pages; first < alloc->total_pages;) {
        uint8_t const FLAGS = alloc->page_flags[first];
        bool const CACHED = FLAGS == PageAllocator::FLAG_CACHED_ORDER0;
        bool const FREE = (FLAGS & 0xC0) == PageAllocator::FLAG_FREE_HEAD;
        bool const ALLOCATED = (FLAGS & 0xC0) == PageAllocator::FLAG_ALLOC_HEAD;
        unsigned const ORDER = CACHED ? 0 : FLAGS & 0x1F;
        if ((!CACHED && !FREE && !ALLOCATED) || ORDER > PageAllocator::MAX_ORDER ||
            (!CACHED && FLAGS != ((FREE ? PageAllocator::FLAG_FREE_HEAD : PageAllocator::FLAG_ALLOC_HEAD) | ORDER)))
            return NodeCounterError::METADATA;
        uint32_t const COUNT = 1U << ORDER;
        if (COUNT > alloc->total_pages - first || (first & (COUNT - 1)) != 0) return NodeCounterError::METADATA;
        for (uint32_t j = 0; j < COUNT; ++j) {
            if (j != 0 && alloc->page_flags[first + j] != (FREE ? PageAllocator::FLAG_FREE_INTERIOR : PageAllocator::FLAG_ALLOC_CONT))
                return NodeCounterError::METADATA;
            if (FREE || CACHED) {
                if (alloc->page_kinds[first + j].load(std::memory_order_relaxed) != static_cast<uint8_t>(PageKind::FREE) ||
                    alloc->page_owners[first + j] != 0 || alloc->page_refcounts[first + j].load(std::memory_order_relaxed) != 0)
                    return NodeCounterError::METADATA;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
                if (alloc->page_callers[first + j] != 0) return NodeCounterError::METADATA;
#endif
            }
        }
        if (FREE || CACHED) {
            if (!node_span_pages(physical_base, first, COUNT, ranges, range_count, node_count, delta)) return NodeCounterError::COVERAGE;
            mask = delta.mask;
            while (mask != 0) {
                unsigned const NODE = static_cast<unsigned>(__builtin_ctzll(mask));
                mask &= mask - 1;
                seed.free_inclusive[NODE] += delta.pages[NODE];
                if (CACHED) seed.cached[NODE] += delta.pages[NODE];
            }
            total_free += COUNT;
            if (CACHED) total_cached += COUNT;
        }
        first += COUNT;
    }
    if (total_free != alloc->free_count || total_cached != alloc->cached_order0_count || total_cached > total_free)
        return NodeCounterError::COUNTERS;
    return NodeCounterError::NONE;
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
#ifdef WOS_SELFTEST
    if (!virt::selftest_direct_map_contains(block)) {
        ker::mod::dbg::panic_handler("buddy free-list node lost its HHDM translation");
    }
#endif
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
#ifdef WOS_SELFTEST
    if (!virt::selftest_direct_map_contains(block)) {
        ker::mod::dbg::panic_handler("buddy free-list node lost its HHDM translation");
    }
#endif
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
        if (load_page_owner(alloc->page_owners[CUR_IDX]) != PhysicalPageOwner::INVALID) {
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
    if (alloc->node_counters.node_count != 0) {
        NodeCounterSeed seed;
        auto const ERROR = prepare_node_seed(alloc, alloc->node_counters.physical_base, alloc->node_counters.ranges,
                                             alloc->node_counters.range_count, alloc->node_counters.node_count, seed);
        if (ERROR != NodeCounterError::NONE) dbg::panic_handler("node counter repair rejected metadata");
        for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node) {
            if (seed.usable[node] != alloc->node_counters.usable[node]) dbg::panic_handler("node counter capacity changed during repair");
            alloc->node_counters.free_inclusive[node] = seed.free_inclusive[node];
            alloc->node_counters.cached[node] = seed.cached[node];
        }
    }
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
    if (load_page_owner(alloc->page_owners[page_idx]) != PhysicalPageOwner::INVALID) {
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

auto free_allocated_block(PageAllocator* alloc, uint32_t page_idx, int order, bool release_owner = true) -> uint64_t {
    if (order < 0 || order > PageAllocator::MAX_ORDER) {
        return 0;
    }

    uint32_t const BLOCK_SIZE = 1U << order;
    if (page_idx >= alloc->total_pages || page_idx + BLOCK_SIZE > alloc->total_pages) {
        return 0;
    }

    NodeDelta node_delta;
    if (!prepare_node_delta(alloc, page_idx, BLOCK_SIZE, 1, 0, node_delta)) dbg::panic_handler("node counter free contradiction");
    uint64_t const FREED_BYTES = static_cast<uint64_t>(BLOCK_SIZE) * paging::PAGE_SIZE;
    if (release_owner) {
        PhysicalPageOwner const OWNER = load_page_owner(alloc->page_owners[page_idx]);
        uint64_t object_count = 0;
        for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
            if (load_page_owner(alloc->page_owners[page_idx + i]) != OWNER) {
                ker::mod::dbg::panic_handler("physical allocation spans multiple owners");
            }
            if ((alloc->page_flags[page_idx + i] & 0xC0) == PageAllocator::FLAG_ALLOC_HEAD) {
                object_count++;
            }
        }
        account_owner_free(alloc, OWNER, BLOCK_SIZE, object_count);
    }

    // Clear flags for the entire allocation.
    for (uint32_t i = 0; i < BLOCK_SIZE; i++) {
        alloc->page_flags[page_idx + i] = PageAllocator::FLAG_FREE_INTERIOR;
        store_page_kind(alloc->page_kinds[page_idx + i], PageKind::FREE);
        store_page_owner(alloc->page_owners[page_idx + i], PhysicalPageOwner::INVALID);
        alloc->page_refcounts[page_idx + i].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        alloc->page_callers[page_idx + i] = 0;
#endif
    }

    // Coalesce without recounting an incompletely represented merged span.
    bool repair_needed = false;
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
        if (!remove_free_block(alloc, k, buddy_block)) {
            repair_needed = true;
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

    // Credit only the original allocation before any possible recount. The
    // completed head now covers every previously unlinked buddy as well.
    alloc->free_count += BLOCK_SIZE;
    apply_node_delta(alloc, node_delta, 1, 0);
    if (repair_needed) {
        rebuild_free_lists_from_flags(alloc, "free coalesce unlink");
    } else {
        auto* free_block = reinterpret_cast<PageAllocator::FreeBlock*>(page_to_ptr(alloc->base, page_idx));
        insert_free_block(alloc, k, free_block);
    }
    return FREED_BYTES;
}

}  // namespace

// ============================================================================
// init
// ============================================================================

auto PageAllocator::prepare_node_counters_locked(uint64_t physical_base, const acpi::numa::MemoryRange* ranges, uint16_t range_count,
                                                 uint16_t node_count, NodeCounterSeed& scratch) const noexcept -> NodeCounterError {
    if (node_counters.node_count != 0) return NodeCounterError::ALREADY_BOUND;
    if (!node_binding_sane(this)) return NodeCounterError::INVALID_INPUT;
    return prepare_node_seed(this, physical_base, ranges, range_count, node_count, scratch);
}

void PageAllocator::commit_node_counters_locked(uint64_t physical_base, const acpi::numa::MemoryRange* ranges, uint16_t range_count,
                                                uint16_t node_count, const NodeCounterSeed& prepared) noexcept {
    for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node) {
        node_counters.usable[node] = prepared.usable[node];
        node_counters.free_inclusive[node] = prepared.free_inclusive[node];
        node_counters.cached[node] = prepared.cached[node];
    }
    node_counters.physical_base = physical_base;
    node_counters.ranges = ranges;
    node_counters.range_count = range_count;
    node_counters.reserved = 0;
    node_counters.node_count = node_count;
}

namespace {
auto validate_node_store(const PageAllocator* alloc) noexcept -> NodeCounterError {
    if (!node_binding_sane(alloc)) return NodeCounterError::INVALID_INPUT;
    auto const& store = alloc->node_counters;
    if (store.node_count == 0) return NodeCounterError::NOT_READY;
    uint64_t usable = 0, free = 0, cached = 0;
    for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node) {
        if (store.cached[node] > store.free_inclusive[node] || store.free_inclusive[node] > store.usable[node] ||
            (node >= store.node_count && (store.usable[node] != 0 || store.free_inclusive[node] != 0 || store.cached[node] != 0)))
            return NodeCounterError::COUNTERS;
        usable += store.usable[node];
        free += store.free_inclusive[node];
        cached += store.cached[node];
    }
    if (usable != alloc->usable_pages || free != alloc->free_count || cached != alloc->cached_order0_count)
        return NodeCounterError::COUNTERS;
    uint64_t owned = 0;
    auto const* owner_pages = owner_page_totals(alloc);
    for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        if (owner_pages[owner] > alloc->usable_pages - owned) return NodeCounterError::COUNTERS;
        owned += owner_pages[owner];
    }
    return owned + free == usable ? NodeCounterError::NONE : NodeCounterError::COUNTERS;
}
}  // namespace

auto PageAllocator::node_counts_locked(uint16_t node, NodeCounts32& out) const noexcept -> NodeCounterError {
    auto const ERROR = validate_node_store(this);
    if (ERROR != NodeCounterError::NONE) return ERROR;
    if (node >= node_counters.node_count) return NodeCounterError::INVALID_INPUT;
    out.usable = node_counters.usable[node];
    out.free_inclusive = node_counters.free_inclusive[node];
    out.cached = node_counters.cached[node];
    out.buddy_free = out.free_inclusive - out.cached;
    return NodeCounterError::NONE;
}

namespace node_counters {
namespace {
auto validate_views(const AllocatorView* views, size_t count) noexcept -> NodeCounterError {
    if (views == nullptr || count == 0) return NodeCounterError::INVALID_INPUT;
    if (count > MAX_ALLOCATORS) return NodeCounterError::CAPACITY;
    for (size_t i = 0; i < count; ++i) {
        if (views[i].allocator == nullptr || views[i].allocator->total_pages == 0 ||
            (views[i].physical_base & (paging::PAGE_SIZE - 1)) != 0)
            return NodeCounterError::INVALID_INPUT;
        uint64_t const BYTES = static_cast<uint64_t>(views[i].allocator->total_pages) * paging::PAGE_SIZE;
        if (BYTES > UINT64_MAX - views[i].physical_base) return NodeCounterError::OVERFLOW;
        for (size_t j = 0; j < i; ++j) {
            uint64_t const OTHER_END = views[j].physical_base + static_cast<uint64_t>(views[j].allocator->total_pages) * paging::PAGE_SIZE;
            if (views[i].allocator == views[j].allocator ||
                (views[i].physical_base < OTHER_END && views[j].physical_base < views[i].physical_base + BYTES))
                return NodeCounterError::INVALID_INPUT;
        }
    }
    return NodeCounterError::NONE;
}
void clear_snapshot(Snapshot& out, NodeCounterError error) noexcept {
    out.complete = false;
    out.error = error;
    out.node_count = out.allocator_count = out.reserved = 0;
    out.global.usable = out.global.free_inclusive = out.global.cached = out.global.buddy_free = 0;
    for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node)
        out.nodes[node].usable = out.nodes[node].free_inclusive = out.nodes[node].cached = out.nodes[node].buddy_free = 0;
}
}  // namespace

auto initialize(const AllocatorView* views, size_t count, const acpi::numa::MemoryRange* ranges, uint16_t range_count, uint16_t node_count,
                BootstrapWorkspace& scratch) noexcept -> NodeCounterError {
    auto error = validate_views(views, count);
    if (error != NodeCounterError::NONE) return error;
    error = valid_node_ranges(ranges, range_count, node_count);
    if (error != NodeCounterError::NONE) return error;
    for (size_t i = 0; i < count; ++i) scratch.locks.lock_flags[i] = views[i].allocator->lock_irq();
    for (size_t i = 0; i < count; ++i) {
        error = views[i].allocator->prepare_node_counters_locked(views[i].physical_base, ranges, range_count, node_count, scratch.seeds[i]);
        if (error != NodeCounterError::NONE) break;
    }
    if (error == NodeCounterError::NONE)
        for (size_t i = 0; i < count; ++i)
            views[i].allocator->commit_node_counters_locked(views[i].physical_base, ranges, range_count, node_count, scratch.seeds[i]);
    for (size_t i = count; i != 0; --i) views[i - 1].allocator->unlock_irq(scratch.locks.lock_flags[i - 1]);
    return error;
}

auto snapshot(const AllocatorView* views, size_t count, const acpi::numa::MemoryRange* ranges, uint16_t range_count, uint16_t node_count,
              Snapshot& out, Workspace& scratch) noexcept -> NodeCounterError {
    clear_snapshot(out, NodeCounterError::NOT_READY);
    auto error = validate_views(views, count);
    if (error == NodeCounterError::NONE) error = valid_node_ranges(ranges, range_count, node_count);
    if (error != NodeCounterError::NONE) {
        out.error = error;
        return error;
    }
    for (size_t i = 0; i < count; ++i) scratch.lock_flags[i] = views[i].allocator->lock_irq();
    for (size_t i = 0; i < count; ++i) {
        auto const* alloc = views[i].allocator;
        error = validate_node_store(alloc);
        if (error != NodeCounterError::NONE) break;
        auto const& store = alloc->node_counters;
        if (store.physical_base != views[i].physical_base || store.ranges != ranges || store.range_count != range_count ||
            store.node_count != node_count) {
            error = NodeCounterError::INVALID_INPUT;
            break;
        }
        out.global.usable += alloc->usable_pages;
        out.global.free_inclusive += alloc->free_count;
        out.global.cached += alloc->cached_order0_count;
        out.global.buddy_free += alloc->free_count - alloc->cached_order0_count;
        for (size_t node = 0; node < node_count; ++node) {
            out.nodes[node].usable += store.usable[node];
            out.nodes[node].free_inclusive += store.free_inclusive[node];
            out.nodes[node].cached += store.cached[node];
            out.nodes[node].buddy_free += store.free_inclusive[node] - store.cached[node];
        }
    }
    for (size_t i = count; i != 0; --i) views[i - 1].allocator->unlock_irq(scratch.lock_flags[i - 1]);
    if (error != NodeCounterError::NONE) {
        clear_snapshot(out, error);
        return error;
    }
    out.node_count = node_count;
    out.allocator_count = static_cast<uint16_t>(count);
    out.error = NodeCounterError::NONE;
    out.complete = true;
    return NodeCounterError::NONE;
}
}  // namespace node_counters

auto PageAllocator::lock_irq() -> uint64_t {
    uint64_t flags = 0;
#ifndef WOS_HOST_TEST
    asm volatile("pushfq; popq %0" : "=r"(flags));
    asm volatile("cli");
#endif

#ifndef WOS_HOST_TEST
    uint32_t service_spins = 0;
#endif
    while (lock_held.exchange(true, std::memory_order_acquire)) {
        for (;;) {
#ifndef WOS_HOST_TEST
            ++service_spins;
            if ((service_spins & 0xFFU) == 0U) {
                virt::service_pending_tlb_shootdowns();
            }
#endif
            asm volatile("pause" ::: "memory");
            if (!lock_held.load(std::memory_order_relaxed)) {
                break;
            }
        }
    }

    return flags;
}

void PageAllocator::unlock_irq(uint64_t flags) {
    lock_held.store(false, std::memory_order_release);
#ifndef WOS_HOST_TEST
    constexpr uint64_t RFLAGS_INTERRUPT_ENABLE = 0x200;
    if ((flags & RFLAGS_INTERRUPT_ENABLE) != 0) {
        asm volatile("sti");
    }
#else
    (void)flags;
#endif
}

void PageAllocator::init(uint64_t zone_base, uint64_t size_bytes) {
    lock_held.store(false, std::memory_order_release);
    base = zone_base;
    for (size_t node = 0; node < PAGE_ALLOCATOR_MAX_NODES; ++node)
        node_counters.usable[node] = node_counters.free_inclusive[node] = node_counters.cached[node] = 0;
    node_counters.physical_base = 0;
    node_counters.ranges = nullptr;
    node_counters.range_count = node_counters.node_count = 0;
    node_counters.reserved = 0;
    total_pages = static_cast<uint32_t>(size_bytes / paging::PAGE_SIZE);

    // --- lay out metadata at the beginning of the zone ---

    // PageAllocator struct occupies bytes [0, sizeof(*this)).
    // page_flags array immediately after, then page_kinds, page owners and
    // page_refcounts.
    uint64_t const FLAGS_OFFSET = sizeof(PageAllocator);
    page_flags = reinterpret_cast<uint8_t*>(zone_base + FLAGS_OFFSET);

    uint64_t const KINDS_OFFSET = FLAGS_OFFSET + static_cast<uint64_t>(total_pages);
    page_kinds = reinterpret_cast<std::atomic<uint8_t>*>(zone_base + KINDS_OFFSET);

    uint64_t const OWNERS_OFFSET = KINDS_OFFSET + (static_cast<uint64_t>(total_pages) * sizeof(std::atomic<uint8_t>));
    page_owners = reinterpret_cast<uint8_t*>(zone_base + OWNERS_OFFSET);

    uint64_t refcounts_offset = OWNERS_OFFSET + static_cast<uint64_t>(total_pages);
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
    owner_pages.fill(0);
    owner_objects.fill(0);

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
        std::memset(page_owners, static_cast<int>(PhysicalPageOwner::INVALID), total_pages);
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
    std::memset(page_owners, static_cast<int>(PhysicalPageOwner::INVALID), total_pages);
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

void* PageAllocator::alloc(uint64_t size_bytes, PhysicalPageOwner owner, uint64_t caller) {
    if (!node_binding_sane(this)) return nullptr;
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
    NodeDelta node_delta;
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

        if (!prepare_node_delta(this, page_idx, 1U << order, -1, 0, node_delta)) return nullptr;

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
        store_page_owner(page_owners[page_idx + i], owner);
        page_refcounts[page_idx + i].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        page_callers[page_idx + i] = caller;
#endif
    }

    account_owner_alloc(this, owner, BLOCK_SIZE);
    free_count -= BLOCK_SIZE;
    apply_node_delta(this, node_delta, -1, 0);
    return reinterpret_cast<void*>(base + (static_cast<uint64_t>(page_idx) * paging::PAGE_SIZE));
}

auto PageAllocator::alloc_order0_span_in_range(uint64_t physical_base, uint64_t range_start, uint64_t range_end, uint32_t pages,
                                               uint64_t alignment_bytes, PhysicalPageOwner owner, uint64_t caller) -> void* {
    if (!node_binding_sane(this)) return nullptr;
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    (void)caller;
#endif
    constexpr uint64_t PAGE_MASK = paging::PAGE_SIZE - 1;
    if (node_counters.node_count != 0 && physical_base != node_counters.physical_base) return nullptr;
    uint64_t const ZONE_BYTES = static_cast<uint64_t>(total_pages) * paging::PAGE_SIZE;
    if (!physical_page_owner_is_valid(owner) || pages == 0 || pages > (1U << MAX_ORDER) || pages > total_pages ||
        alignment_bytes < paging::PAGE_SIZE || (alignment_bytes & (alignment_bytes - 1)) != 0 || base == 0 ||
        ((base | physical_base) & PAGE_MASK) != 0 || ZONE_BYTES > UINT64_MAX - physical_base || ZONE_BYTES > UINT64_MAX - base ||
        range_start >= range_end || page_flags == nullptr || page_kinds == nullptr || page_owners == nullptr || page_refcounts == nullptr)
        return nullptr;

    uint64_t const LOW = std::max(physical_base, range_start);
    uint64_t const HIGH = std::min(physical_base + ZONE_BYTES, range_end);
    uint64_t const BYTES = static_cast<uint64_t>(pages) * paging::PAGE_SIZE;
    if (LOW >= HIGH || BYTES > HIGH - LOW || free_count < pages || free_count > total_pages || cached_order0_count > free_count)
        return nullptr;
    size_t const OWNER_INDEX = owner_index(owner);
    if (owner_pages[OWNER_INDEX] > UINT64_MAX - pages || owner_objects[OWNER_INDEX] > UINT64_MAX - pages) return nullptr;

    int minimum_order = 0;
    while ((1U << minimum_order) < pages) ++minimum_order;
    for (int order = minimum_order; order <= MAX_ORDER; ++order) {
        FreeBlock* previous = nullptr;
        uint32_t visited = 0;
        for (auto* block = free_list[static_cast<size_t>(order)]; block != nullptr; block = block->next) {
            uint32_t block_index = 0;
            uint32_t const BLOCK_PAGES = 1U << order;
            if (visited++ >= total_pages || !free_head_is_valid(this, block, order, block_index) ||
                BLOCK_PAGES > total_pages - block_index || (block_index & (BLOCK_PAGES - 1)) != 0 || block->prev != previous)
                return nullptr;
            previous = block;

            uint64_t const BLOCK_START = physical_base + static_cast<uint64_t>(block_index) * paging::PAGE_SIZE;
            uint64_t const BLOCK_END = BLOCK_START + static_cast<uint64_t>(BLOCK_PAGES) * paging::PAGE_SIZE;
            uint64_t const WINDOW_LOW = std::max(LOW, BLOCK_START);
            uint64_t const WINDOW_END = std::min(HIGH, BLOCK_END);
            uint64_t const ALIGN_MASK = alignment_bytes - 1;
            if (WINDOW_LOW >= WINDOW_END || WINDOW_LOW > UINT64_MAX - ALIGN_MASK) continue;
            uint64_t const START = (WINDOW_LOW + ALIGN_MASK) & ~ALIGN_MASK;
            if (START >= WINDOW_END || BYTES > WINDOW_END - START) continue;

            // Carving never needs a block as large as the original. Validate
            // every possible destination head before unlinking; the unchecked
            // insertion below cannot trigger a list repair after mutation.
            for (int destination = 0; destination < order; ++destination) {
                auto* head = free_list[static_cast<size_t>(destination)];
                uint32_t head_index = 0;
                uint32_t const HEAD_PAGES = 1U << destination;
                if (head != nullptr && (!free_head_is_valid(this, head, destination, head_index) || HEAD_PAGES > total_pages - head_index ||
                                        (head_index & (HEAD_PAGES - 1)) != 0 || head->prev != nullptr))
                    return nullptr;
            }
            if (!free_block_pages_are_reusable(this, block_index, order, "interval span candidate")) return nullptr;
            uint32_t const FIRST = static_cast<uint32_t>((START - physical_base) / paging::PAGE_SIZE);
            NodeDelta node_delta;
            if (!prepare_node_delta(this, FIRST, pages, -1, 0, node_delta)) return nullptr;
            if (!remove_free_block(this, order, block)) return nullptr;

            uint32_t const END = FIRST + pages;
            for (uint32_t i = block_index; i < block_index + BLOCK_PAGES; ++i) page_flags[i] = FLAG_FREE_INTERIOR;
            auto return_free_run = [this](uint32_t first, uint32_t count) {
                while (count != 0) {
                    int const ORDER = largest_order_for_aligned_run(first, count);
                    uint32_t const BLOCK_SIZE = 1U << ORDER;
                    page_flags[first] = FLAG_FREE_HEAD | static_cast<uint8_t>(ORDER);
                    link_free_block_unchecked(this, ORDER, static_cast<FreeBlock*>(page_to_ptr(base, first)));
                    first += BLOCK_SIZE;
                    count -= BLOCK_SIZE;
                }
            };
            return_free_run(block_index, FIRST - block_index);
            return_free_run(END, block_index + BLOCK_PAGES - END);
            for (uint32_t i = FIRST; i < END; ++i) {
                page_flags[i] = FLAG_ALLOC_HEAD;
                store_page_kind(page_kinds[i], PageKind::NORMAL);
                store_page_owner(page_owners[i], owner);
                page_refcounts[i].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
                page_callers[i] = caller;
#endif
            }
            account_owner_alloc(this, owner, pages, pages);
            free_count -= pages;
            apply_node_delta(this, node_delta, -1, 0);
            return page_to_ptr(base, FIRST);
        }
    }
    return nullptr;
}

void* PageAllocator::alloc_order0(PhysicalPageOwner owner, uint64_t caller) {
    if (!node_binding_sane(this)) return nullptr;
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

    NodeDelta node_delta;
    if (!prepare_node_delta(this, page_idx, 1, -1, 0, node_delta)) return nullptr;

    if (!remove_free_block_with_repair(this, 0, block, "alloc_order0 unlink")) {
        return nullptr;
    }

    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    store_page_kind(page_kinds[page_idx], PageKind::NORMAL);
    store_page_owner(page_owners[page_idx], owner);
    page_refcounts[page_idx].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = caller;
#endif
    account_owner_alloc(this, owner, 1);
    free_count -= 1;
    apply_node_delta(this, node_delta, -1, 0);
    return page_to_ptr(base, page_idx);
}

void* PageAllocator::claim_free_order0_for_cache(uint32_t& out_page_idx) {
    if (!node_binding_sane(this)) return nullptr;
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

    NodeDelta node_delta;
    if (!prepare_node_delta(this, page_idx, 1, 0, 1, node_delta)) return nullptr;

    if (!remove_free_block_with_repair(this, 0, block, "cache claim unlink")) {
        return nullptr;
    }

    page_flags[page_idx] = FLAG_CACHED_ORDER0;
    store_page_kind(page_kinds[page_idx], PageKind::FREE);
    store_page_owner(page_owners[page_idx], PhysicalPageOwner::INVALID);
    page_refcounts[page_idx].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = 0;
#endif
    cached_order0_count += 1;
    apply_node_delta(this, node_delta, 0, 1);
    out_page_idx = page_idx;
    return page_to_ptr(base, page_idx);
}

auto PageAllocator::cache_allocated_order0(void* ptr, uint32_t& out_page_idx) -> bool {
    if (!node_binding_sane(this)) return false;
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

    NodeDelta node_delta;
    if (!prepare_node_delta(this, PAGE_IDX, 1, 1, 1, node_delta)) return false;

    PhysicalPageOwner const OWNER = load_page_owner(page_owners[PAGE_IDX]);
    account_owner_free(this, OWNER, 1);
    page_flags[PAGE_IDX] = FLAG_CACHED_ORDER0;
    store_page_kind(page_kinds[PAGE_IDX], PageKind::FREE);
    store_page_owner(page_owners[PAGE_IDX], PhysicalPageOwner::INVALID);
    page_refcounts[PAGE_IDX].store(0, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[PAGE_IDX] = 0;
#endif
    free_count += 1;
    cached_order0_count += 1;
    apply_node_delta(this, node_delta, 1, 1);
    out_page_idx = PAGE_IDX;
    return true;
}

auto PageAllocator::alloc_cached_order0_at(uint32_t page_idx, PhysicalPageOwner owner, uint64_t caller) -> void* {
    if (!node_binding_sane(this)) return nullptr;
#ifndef WOS_PHYS_ALLOC_CALLER_STATS
    (void)caller;
#endif
    if (page_idx >= total_pages || page_flags[page_idx] != FLAG_CACHED_ORDER0) {
        return nullptr;
    }
    if (decode_page_kind(page_kinds[page_idx].load(std::memory_order_acquire)) != PageKind::FREE) {
        return nullptr;
    }
    if (load_page_owner(page_owners[page_idx]) != PhysicalPageOwner::INVALID) {
        return nullptr;
    }
    if (page_refcounts[page_idx].load(std::memory_order_acquire) != 0) {
        return nullptr;
    }
    if (free_count == 0 || cached_order0_count == 0) {
        return nullptr;
    }

    NodeDelta node_delta;
    if (!prepare_node_delta(this, page_idx, 1, -1, -1, node_delta)) return nullptr;

    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    store_page_kind(page_kinds[page_idx], PageKind::NORMAL);
    store_page_owner(page_owners[page_idx], owner);
    page_refcounts[page_idx].store(1, std::memory_order_release);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    page_callers[page_idx] = caller;
#endif
    account_owner_alloc(this, owner, 1);
    free_count -= 1;
    cached_order0_count -= 1;
    apply_node_delta(this, node_delta, -1, -1);
    return page_to_ptr(base, page_idx);
}

auto PageAllocator::release_cached_order0_at(uint32_t page_idx) -> uint64_t {
    if (!node_binding_sane(this)) return 0;
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

    NodeDelta node_delta;
    if (!prepare_node_delta(this, page_idx, 1, -1, -1, node_delta)) return 0;

    cached_order0_count -= 1;
    free_count -= 1;
    apply_node_delta(this, node_delta, -1, -1);
    page_flags[page_idx] = FLAG_ALLOC_HEAD;
    // cache_allocated_order0() already removed the former allocation owner
    // and counted this page as free. Draining only changes its free-list
    // representation; it must not decrement an owner a second time.
    return free_allocated_block(this, page_idx, 0, false);
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
        PhysicalPageOwner const OWNER = load_page_owner(page_owners[cursor]);
        uint32_t same_owner_pages = 1;
        while (same_owner_pages < remaining && load_page_owner(page_owners[cursor + same_owner_pages]) == OWNER) {
            same_owner_pages++;
        }
        // Refcount batches may combine adjacent order-0 allocations whose
        // mapping/cache ownership differs. Preserve the batching optimization,
        // but never collapse distinct owner rows into one accounting release.
        int const ORDER = largest_order_for_aligned_run(cursor, same_owner_pages);
        uint32_t const BLOCK_SIZE = 1U << ORDER;
        freed_bytes += free_allocated_block(this, cursor, ORDER);
        cursor += BLOCK_SIZE;
        remaining -= BLOCK_SIZE;
    }
    return freed_bytes;
}

auto PageAllocator::split_allocated_block_to_order0(void* ptr) -> bool {
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
    PhysicalPageOwner const OWNER = load_page_owner(page_owners[PAGE_IDX]);
    if (!physical_page_owner_is_valid(OWNER)) {
        return false;
    }
    owner_objects.at(owner_index(OWNER)) += BLOCK_SIZE - 1;
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        page_flags[PAGE_IDX + i] = FLAG_ALLOC_HEAD;
        store_page_kind(page_kinds[PAGE_IDX + i], KIND);
        store_page_owner(page_owners[PAGE_IDX + i], OWNER);
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

auto PageAllocator::reassign_allocated_block_owner(void* ptr, PhysicalPageOwner owner) -> bool {
    if (ptr == nullptr || !ptr_in_zone(this, ptr) || !physical_page_owner_is_valid(owner)) {
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

    PhysicalPageOwner const OLD_OWNER = load_page_owner(page_owners[PAGE_IDX]);
    uint64_t object_count = 0;
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        if (load_page_owner(page_owners[PAGE_IDX + i]) != OLD_OWNER) {
            return false;
        }
        if ((page_flags[PAGE_IDX + i] & 0xC0) == FLAG_ALLOC_HEAD) {
            object_count++;
        }
    }
    if (OLD_OWNER == owner) {
        return true;
    }
    account_owner_free(this, OLD_OWNER, BLOCK_SIZE, object_count);
    account_owner_alloc(this, owner, BLOCK_SIZE, object_count);
    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        store_page_owner(page_owners[PAGE_IDX + i], owner);
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
