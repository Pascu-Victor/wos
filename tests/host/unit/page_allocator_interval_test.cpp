#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <vector>

namespace {
namespace mm = ker::mod::mm;
constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
constexpr uint64_t LARGE = 512 * PAGE;
constexpr size_t ZONE_BYTES = 8 * 1024 * 1024;
constexpr auto OWNER = mm::PhysicalPageOwner::USER_PRIVATE_MAPPING;
constexpr size_t OWNER_INDEX = static_cast<size_t>(OWNER);

struct FreeStorage {
    void operator()(void* pointer) const { std::free(pointer); }
};

class AllocatorLock {
   public:
    explicit AllocatorLock(mm::PageAllocator& allocator) : allocator_(allocator), flags_(allocator.lock_irq()) {}
    ~AllocatorLock() { allocator_.unlock_irq(flags_); }
    AllocatorLock(const AllocatorLock&) = delete;
    auto operator=(const AllocatorLock&) -> AllocatorLock& = delete;

   private:
    mm::PageAllocator& allocator_;
    uint64_t flags_;
};

class PageAllocatorInterval : public ::testing::Test {
   protected:
    std::unique_ptr<void, FreeStorage> storage;
    mm::PageAllocator* allocator{};
    uint64_t physical_base{0x40003000ULL};

    void SetUp() override {
        void* allocation = std::aligned_alloc(LARGE, ZONE_BYTES + LARGE);
        ASSERT_NE(allocation, nullptr);
        storage.reset(allocation);
        std::memset(allocation, 0, ZONE_BYTES + LARGE);
        auto* mapped_base = static_cast<std::byte*>(allocation) + PAGE;
        allocator = ::new (mapped_base) mm::PageAllocator{};
        allocator->init(reinterpret_cast<uint64_t>(mapped_base), ZONE_BYTES);
        ASSERT_GT(allocator->usable_pages, 512U);
        check_invariants();
    }

    auto index(const void* pointer) const -> uint32_t {
        return static_cast<uint32_t>((reinterpret_cast<uint64_t>(pointer) - allocator->base) / PAGE);
    }
    auto address(uint32_t page) const -> uint64_t { return physical_base + static_cast<uint64_t>(page) * PAGE; }
    auto pointer(uint32_t page) const -> std::byte* { return reinterpret_cast<std::byte*>(allocator->base) + page * PAGE; }
    auto zone_end() const -> uint64_t { return address(allocator->total_pages); }
    auto allocate(uint64_t start, uint64_t end, uint32_t pages, uint64_t alignment = PAGE, uint64_t caller = 0) -> void* {
        AllocatorLock const LOCK(*allocator);
        return allocator->alloc_order0_span_in_range(physical_base, start, end, pages, alignment, OWNER, caller);
    }
    auto snapshot() const -> std::vector<std::byte> {
        // This fixture has one thread. Capture the complete arena, including
        // free-list payload links, after its actual lock has been released.
        std::vector<std::byte> result(ZONE_BYTES);
        std::memcpy(result.data(), reinterpret_cast<void*>(allocator->base), result.size());
        return result;
    }
    void expect_snapshot(const std::vector<std::byte>& before) const {
        EXPECT_EQ(std::memcmp(before.data(), reinterpret_cast<void*>(allocator->base), before.size()), 0);
    }
    auto free_block(int minimum_order, uint32_t& page, uint32_t& count) const -> bool {
        for (int order = minimum_order; order <= mm::PageAllocator::MAX_ORDER; ++order) {
            auto* head = allocator->free_list[static_cast<size_t>(order)];
            if (head != nullptr) {
                page = index(head);
                count = 1U << order;
                return true;
            }
        }
        return false;
    }
    void expect_span(void* span, uint32_t pages, uint64_t caller = 0) const {
        ASSERT_NE(span, nullptr);
        uint32_t const FIRST = index(span);
        for (uint32_t i = FIRST; i < FIRST + pages; ++i) {
            EXPECT_EQ(allocator->page_flags[i], mm::PageAllocator::FLAG_ALLOC_HEAD);
            EXPECT_EQ(allocator->kind_of(pointer(i)), mm::PageKind::NORMAL);
            EXPECT_EQ(allocator->page_owners[i], static_cast<uint8_t>(OWNER));
            EXPECT_EQ(allocator->page_refcounts[i].load(), 1U);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
            EXPECT_EQ(allocator->page_callers[i], caller);
#else
            (void)caller;
#endif
        }
    }
    void release_span(void* span, uint32_t pages) {
        AllocatorLock const LOCK(*allocator);
        for (uint32_t i = pages; i != 0; --i) EXPECT_EQ(allocator->free(static_cast<std::byte*>(span) + (i - 1) * PAGE), PAGE);
    }
    void check_invariants() const {
        std::vector<uint8_t> free_pages(allocator->total_pages);
        uint64_t linked = 0;
        for (int order = 0; order <= mm::PageAllocator::MAX_ORDER; ++order) {
            auto* previous = static_cast<mm::PageAllocator::FreeBlock*>(nullptr);
            size_t visited = 0;
            for (auto* block = allocator->free_list[static_cast<size_t>(order)]; block != nullptr; block = block->next) {
                ASSERT_LT(visited++, allocator->total_pages);
                ASSERT_GE(reinterpret_cast<uint64_t>(block), allocator->base);
                uint32_t const FIRST = index(block);
                uint32_t const COUNT = 1U << order;
                ASSERT_LE(static_cast<uint64_t>(FIRST) + COUNT, allocator->total_pages);
                EXPECT_EQ(FIRST % COUNT, 0U);
                EXPECT_EQ(block->prev, previous);
                EXPECT_EQ(allocator->page_flags[FIRST], mm::PageAllocator::FLAG_FREE_HEAD | order);
                for (uint32_t i = FIRST; i < FIRST + COUNT; ++i) {
                    EXPECT_EQ(free_pages[i]++, 0U);
                    if (i != FIRST) EXPECT_EQ(allocator->page_flags[i], mm::PageAllocator::FLAG_FREE_INTERIOR);
                    EXPECT_EQ(allocator->kind_of(pointer(i)), mm::PageKind::FREE);
                    EXPECT_EQ(allocator->page_owners[i], 0U);
                    EXPECT_EQ(allocator->page_refcounts[i].load(), 0U);
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
                    EXPECT_EQ(allocator->page_callers[i], 0U);
#endif
                }
                linked += COUNT;
                previous = block;
            }
        }
        std::array<uint64_t, mm::PHYSICAL_PAGE_OWNER_COUNT> pages{}, objects{};
        uint64_t cached = 0;
        uint64_t live = 0;
        for (uint32_t i = 0; i < allocator->total_pages; ++i) {
            uint8_t const FLAGS = allocator->page_flags[i];
            if (i < allocator->metadata_pages) {
                EXPECT_EQ(FLAGS, mm::PageAllocator::FLAG_RESERVED);
            } else if (FLAGS == mm::PageAllocator::FLAG_CACHED_ORDER0) {
                ++cached;
                EXPECT_EQ(free_pages[i], 0U);
                EXPECT_EQ(allocator->page_refcounts[i].load(), 0U);
                EXPECT_EQ(allocator->kind_of(pointer(i)), mm::PageKind::FREE);
                EXPECT_EQ(allocator->page_owners[i], 0U);
            } else if ((FLAGS & 0xC0) == mm::PageAllocator::FLAG_ALLOC_HEAD || FLAGS == mm::PageAllocator::FLAG_ALLOC_CONT) {
                ++live;
                auto const TAG = static_cast<mm::PhysicalPageOwner>(allocator->page_owners[i]);
                ASSERT_TRUE(mm::physical_page_owner_is_valid(TAG));
                ++pages[static_cast<size_t>(TAG)];
                if ((FLAGS & 0xC0) == mm::PageAllocator::FLAG_ALLOC_HEAD) ++objects[static_cast<size_t>(TAG)];
                EXPECT_GT(allocator->page_refcounts[i].load(), 0U);
                EXPECT_EQ(free_pages[i], 0U);
            } else {
                EXPECT_EQ(free_pages[i], 1U);
            }
        }
        EXPECT_EQ(allocator->owner_pages, pages);
        EXPECT_EQ(allocator->owner_objects, objects);
        EXPECT_EQ(allocator->cached_order0_count, cached);
        EXPECT_EQ(allocator->free_count, linked + cached);
        EXPECT_EQ(allocator->metadata_pages + live + linked + cached, allocator->total_pages);
    }
};

TEST_F(PageAllocatorInterval, AbsolutePhysicalAlignmentIsIndependentOfVirtualBuddyBase) {
    EXPECT_EQ(allocator->base % LARGE, PAGE);
    EXPECT_EQ(physical_base % LARGE, 3 * PAGE);
    uint32_t const BEFORE = allocator->free_count;
    auto* span = allocate(physical_base, zone_end(), 512, LARGE, 0x12345678);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(address(index(span)) % LARGE, 0U);
    EXPECT_NE(reinterpret_cast<uint64_t>(span) % LARGE, 0U);
    expect_span(span, 512, 0x12345678);
    EXPECT_EQ(allocator->free_count, BEFORE - 512);
    EXPECT_EQ(allocator->owner_pages[OWNER_INDEX], 512U);
    EXPECT_EQ(allocator->owner_objects[OWNER_INDEX], 512U);
    check_invariants();
    release_span(span, 512);
    EXPECT_EQ(allocator->free_count, BEFORE);
    check_invariants();
}

TEST_F(PageAllocatorInterval, ArbitraryPageCountCarvesBothSidesAndDoesNotZeroData) {
    uint32_t block = 0, count = 0;
    ASSERT_TRUE(free_block(5, block, count));
    uint32_t const FIRST = block + 3;
    uint32_t const BEFORE = allocator->free_count;
    pointer(FIRST)[128] = std::byte{0x5A};
    auto* span = allocate(address(FIRST), address(FIRST + 5), 5);
    ASSERT_EQ(span, pointer(FIRST));
    expect_span(span, 5);
    EXPECT_EQ(pointer(FIRST)[128], std::byte{0x5A});
    EXPECT_EQ(allocator->owner_objects[OWNER_INDEX], 5U);
    check_invariants();
    auto* prefix = allocate(address(block), address(FIRST), 2);
    auto* prefix_tail = allocate(address(block + 2), address(FIRST), 1);
    auto* suffix = allocate(address(FIRST + 5), address(block + count), 1);
    ASSERT_NE(prefix, nullptr);
    ASSERT_NE(prefix_tail, nullptr);
    ASSERT_NE(suffix, nullptr);
    release_span(prefix, 2);
    release_span(prefix_tail, 1);
    release_span(suffix, 1);
    release_span(span, 5);
    EXPECT_EQ(allocator->free_count, BEFORE);
    check_invariants();
}

TEST_F(PageAllocatorInterval, NonPageAlignedIntervalRoundsOnlyTheCandidate) {
    uint32_t block = 0, count = 0;
    ASSERT_TRUE(free_block(4, block, count));
    auto* span = allocate(address(block) + 1, address(block + 7) - 1, 3);
    ASSERT_EQ(span, pointer(block + 1));
    expect_span(span, 3);
    release_span(span, 3);
    check_invariants();
}

TEST_F(PageAllocatorInterval, DomainBoundaryRestrictsTheCompleteSpan) {
    uint32_t block = 0, count = 0;
    ASSERT_TRUE(free_block(5, block, count));
    uint64_t const BOUNDARY = address(block + 8);
    auto* left = allocate(address(block + 1), BOUNDARY, 7);
    ASSERT_EQ(left, pointer(block + 1));
    auto const BEFORE = snapshot();
    EXPECT_EQ(allocate(address(block + 1), BOUNDARY, 8), nullptr);
    expect_snapshot(BEFORE);
    auto* right = allocate(BOUNDARY, address(block + 16), 8);
    ASSERT_EQ(right, pointer(block + 8));
    expect_span(left, 7);
    expect_span(right, 8);
    release_span(left, 7);
    release_span(right, 8);
    check_invariants();
}

TEST_F(PageAllocatorInterval, InvalidRequestsAndMissingCandidateLeaveEveryByteUnchanged) {
    struct Request {
        uint64_t physical, start, end;
        uint32_t pages;
        uint64_t alignment;
        mm::PhysicalPageOwner owner;
    };
    uint64_t const END = zone_end();
    std::array const REQUESTS{
        Request{physical_base, physical_base, END, 0, PAGE, OWNER},
        Request{physical_base, physical_base, END, (1U << mm::PageAllocator::MAX_ORDER) + 1, PAGE, OWNER},
        Request{physical_base, physical_base, END, allocator->total_pages + 1, PAGE, OWNER},
        Request{physical_base, physical_base, END, 1, 0, OWNER},
        Request{physical_base, physical_base, END, 1, PAGE / 2, OWNER},
        Request{physical_base, physical_base, END, 1, 3 * PAGE, OWNER},
        Request{physical_base + 1, physical_base, END, 1, PAGE, OWNER},
        Request{physical_base, END, END, 1, PAGE, OWNER},
        Request{physical_base, END, physical_base, 1, PAGE, OWNER},
        Request{physical_base, 0, physical_base, 1, PAGE, OWNER},
        Request{physical_base, END, END + PAGE, 1, PAGE, OWNER},
        Request{physical_base, physical_base, physical_base + PAGE - 1, 1, PAGE, OWNER},
        Request{physical_base, physical_base, END, 1, uint64_t{1} << 63, OWNER},
        Request{UINT64_MAX - PAGE + 1, 0, UINT64_MAX, 1, PAGE, OWNER},
        Request{UINT64_MAX - ZONE_BYTES - PAGE + 1, 0, UINT64_MAX, 1, uint64_t{1} << 63, OWNER},
        Request{physical_base, physical_base, END, 1, PAGE, mm::PhysicalPageOwner::INVALID},
        Request{physical_base, physical_base, END, 1, PAGE, mm::PhysicalPageOwner::COUNT},
    };
    auto const BEFORE = snapshot();
    for (auto const& request : REQUESTS) {
        {
            AllocatorLock const LOCK(*allocator);
            EXPECT_EQ(allocator->alloc_order0_span_in_range(request.physical, request.start, request.end, request.pages, request.alignment,
                                                            request.owner),
                      nullptr);
        }
        expect_snapshot(BEFORE);
    }
    check_invariants();
}

TEST_F(PageAllocatorInterval, InvalidVirtualExtentAndAccountingOverflowRefuseBeforeMutation) {
    uint64_t const BASE = allocator->base;
    allocator->base = UINT64_MAX - PAGE + 1;
    {
        AllocatorLock const LOCK(*allocator);
        EXPECT_EQ(allocator->alloc_order0_span_in_range(physical_base, physical_base, zone_end(), 1, PAGE, OWNER), nullptr);
    }
    allocator->base = BASE;
    for (auto* counter : {&allocator->owner_pages[OWNER_INDEX], &allocator->owner_objects[OWNER_INDEX]}) {
        *counter = UINT64_MAX;
        auto const BEFORE = snapshot();
        EXPECT_EQ(allocate(physical_base, zone_end(), 1), nullptr);
        expect_snapshot(BEFORE);
        *counter = 0;
    }
    check_invariants();
}

TEST_F(PageAllocatorInterval, ExistingCacheAndUnsplitAllocationArePreserved) {
    void* cached = nullptr;
    void* live = nullptr;
    uint32_t cached_index = 0;
    {
        AllocatorLock const LOCK(*allocator);
        cached = allocator->alloc(PAGE, mm::PhysicalPageOwner::SELFTEST);
        ASSERT_NE(cached, nullptr);
        ASSERT_TRUE(allocator->cache_allocated_order0(cached, cached_index));
        live = allocator->alloc(2 * PAGE, mm::PhysicalPageOwner::USER_FILE_CACHE);
    }
    ASSERT_NE(live, nullptr);
    auto const BEFORE = snapshot();
    EXPECT_EQ(allocate(address(cached_index), address(cached_index + 1), 1), nullptr);
    expect_snapshot(BEFORE);
    EXPECT_EQ(allocate(address(index(live)), address(index(live) + 2), 1), nullptr);
    expect_snapshot(BEFORE);
    auto* span = allocate(physical_base, zone_end(), 512, LARGE);
    ASSERT_NE(span, nullptr);
    expect_span(span, 512);
    EXPECT_EQ(allocator->page_flags[cached_index], mm::PageAllocator::FLAG_CACHED_ORDER0);
    EXPECT_EQ(allocator->cached_order0_count, 1U);
    EXPECT_EQ(allocator->page_flags[index(live)], mm::PageAllocator::FLAG_ALLOC_HEAD | 1);
    EXPECT_EQ(allocator->page_flags[index(live) + 1], mm::PageAllocator::FLAG_ALLOC_CONT);
    check_invariants();
    release_span(span, 512);
    {
        AllocatorLock const LOCK(*allocator);
        EXPECT_EQ(allocator->free(live), 2 * PAGE);
        EXPECT_EQ(allocator->release_cached_order0_at(cached_index), PAGE);
    }
    check_invariants();
}

TEST_F(PageAllocatorInterval, SubpageRefPreventsDirectFreeWithoutHoldingItsNeighbours) {
    auto* span = allocate(physical_base, zone_end(), 7);
    ASSERT_NE(span, nullptr);
    uint32_t const FIRST = index(span);
    // The Host seam changes the actual refcount atomics to represent a pin.
    // Production ref-inc/pin acquisition and TLB retirement are not compiled
    // by this allocator target; its direct-free/ref0 gates are real code.
    EXPECT_EQ(allocator->page_refcounts[FIRST + 3].fetch_add(1), 1U);
    {
        AllocatorLock const LOCK(*allocator);
        EXPECT_EQ(allocator->free(pointer(FIRST + 3)), 0U);
        EXPECT_EQ(allocator->free(pointer(FIRST + 2)), PAGE);
    }
    EXPECT_EQ(allocator->page_refcounts[FIRST + 3].load(), 2U);
    EXPECT_EQ(allocator->page_refcounts[FIRST + 4].load(), 1U);
    EXPECT_EQ(allocator->owner_pages[OWNER_INDEX], 6U);
    EXPECT_EQ(allocator->owner_objects[OWNER_INDEX], 6U);
    EXPECT_EQ(allocator->page_refcounts[FIRST + 3].fetch_sub(1), 2U);
    {
        AllocatorLock const LOCK(*allocator);
        for (uint32_t i = 0; i < 7; ++i) {
            if (i == 2) continue;
            EXPECT_EQ(allocator->page_refcounts[FIRST + i].fetch_sub(1), 1U);
            EXPECT_EQ(allocator->free_order0_at(FIRST + i), PAGE);
        }
    }
    EXPECT_EQ(allocator->owner_pages[OWNER_INDEX], 0U);
    EXPECT_EQ(allocator->owner_objects[OWNER_INDEX], 0U);
    check_invariants();
}

TEST_F(PageAllocatorInterval, RepeatedCarveFreePreservesAllAccounting) {
    uint32_t const BEFORE = allocator->free_count;
    for (uint32_t cycle = 0; cycle < 1000; ++cycle) {
        uint32_t const PAGES = (cycle % 17) + 1;
        auto* span = allocate(physical_base, zone_end(), PAGES, 16 * PAGE);
        ASSERT_NE(span, nullptr);
        EXPECT_EQ(address(index(span)) % (16 * PAGE), 0U);
        EXPECT_EQ(allocator->owner_pages[OWNER_INDEX], PAGES);
        EXPECT_EQ(allocator->owner_objects[OWNER_INDEX], PAGES);
        release_span(span, PAGES);
        ASSERT_EQ(allocator->free_count, BEFORE);
        ASSERT_EQ(allocator->owner_pages[OWNER_INDEX], 0U);
        ASSERT_EQ(allocator->owner_objects[OWNER_INDEX], 0U);
    }
    check_invariants();
}

TEST(PageAllocatorIntervalInput, UninitializedAllocatorRefusesWithoutDereference) {
    mm::PageAllocator allocator;
    AllocatorLock const LOCK(allocator);
    EXPECT_EQ(allocator.alloc_order0_span_in_range(0, 0, LARGE, 1, PAGE, OWNER), nullptr);
}

}  // namespace
