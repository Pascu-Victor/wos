#include <gtest/gtest.h>
#include <sys/mman.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_topology.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <vector>

// Actual PageAllocator and bounded batch core are linked. Only the existing
// WOS_HOST_TEST IRQ and panic/logging primitives are Host seams. Independent
// per-frame metadata scans below are test oracles, never production queries.
namespace {
namespace mm = ker::mod::mm;
namespace nc = mm::node_counters;
namespace topo = ker::mod::acpi::numa;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr auto OWNER = mm::PhysicalPageOwner::SELFTEST;
constexpr auto NONE = mm::NodeCounterError::NONE;
struct FreeStorage {
    void operator()(void* p) const { std::free(p); }
};
struct Lock {
    mm::PageAllocator& a;
    uint64_t flags;
    explicit Lock(mm::PageAllocator& value) : a(value), flags(a.lock_irq()) {}
    ~Lock() { a.unlock_irq(flags); }
};
struct Arena {
    std::unique_ptr<void, FreeStorage> storage;
    std::unique_ptr<mm::PageAllocator> separate;
    mm::PageAllocator* a{};
    uint64_t physical;
    uint32_t pages;
    explicit Arena(uint64_t base = 0x40003000ULL, uint32_t count = 2048, bool detached_object = false) : physical(base), pages(count) {
        auto* raw = std::aligned_alloc(PAGE, static_cast<size_t>(count + 1) * PAGE);
        if (!raw) std::abort();
        storage.reset(raw);
        std::memset(raw, 0, static_cast<size_t>(count + 1) * PAGE);
        auto* mapped = static_cast<std::byte*>(raw) + PAGE;
        if (detached_object) {
            separate = std::make_unique<mm::PageAllocator>();
            a = separate.get();
        } else
            a = ::new (mapped) mm::PageAllocator{};
        a->init(reinterpret_cast<uint64_t>(mapped), static_cast<uint64_t>(count) * PAGE);
    }
    auto ptr(uint32_t page) const -> void* { return reinterpret_cast<void*>(a->base + static_cast<uint64_t>(page) * PAGE); }
    auto index(void* p) const -> uint32_t { return static_cast<uint32_t>((reinterpret_cast<uint64_t>(p) - a->base) / PAGE); }
    auto phys(uint32_t page) const -> uint64_t { return physical + static_cast<uint64_t>(page) * PAGE; }
    auto image() const -> std::vector<std::byte> {
        std::vector<std::byte> out(static_cast<size_t>(pages) * PAGE);
        std::memcpy(out.data(), reinterpret_cast<const void*>(a->base), out.size());
        return out;
    }
    auto span(uint32_t first, uint32_t count) -> void* {
        Lock lock(*a);
        return a->alloc_order0_span_in_range(physical, phys(first), phys(first + count), count, PAGE, OWNER, 0xC19A);
    }
    void free(uint32_t first) {
        Lock lock(*a);
        EXPECT_EQ(a->free(ptr(first)), PAGE);
    }
};
class NodeCounters : public ::testing::Test {
   protected:
    Arena arena;
    topo::MemoryRange ranges[3];
    std::unique_ptr<nc::BootstrapWorkspace> boot = std::make_unique<nc::BootstrapWorkspace>();
    std::unique_ptr<nc::Workspace> work = std::make_unique<nc::Workspace>();
    nc::Snapshot output;
    void SetUp() override {
        ranges[0] = {arena.physical, 1025 * PAGE, 0, 0};
        ranges[1] = {arena.phys(1025), 498 * PAGE, 1, 0};
        ranges[2] = {arena.phys(1523), (arena.pages - 1523) * PAGE, 0, 0};
        work->views[0] = {arena.a, arena.physical};
    }
    auto bind(uint16_t nodes = 3) -> mm::NodeCounterError { return nc::initialize(work->views, 1, ranges, 3, nodes, *boot); }
    auto node(uint32_t i) const -> uint16_t {
        for (auto const& range : ranges)
            if (arena.phys(i) >= range.base && arena.phys(i) - range.base < range.length) return range.node;
        std::abort();
    }
    void check() {
        ASSERT_EQ(nc::snapshot(work->views, 1, ranges, 3, arena.a->node_counters.node_count, output, *work), NONE);
        ASSERT_TRUE(output.complete);
        uint64_t usable[64]{}, free[64]{}, cached[64]{};
        for (uint32_t i = arena.a->metadata_pages; i < arena.pages;) {
            auto const FLAG = arena.a->page_flags[i];
            if (FLAG == mm::PageAllocator::FLAG_CACHED_ORDER0) {
                ++free[node(i)];
                ++cached[node(i)];
                ++i;
            } else if ((FLAG & 0xC0) == mm::PageAllocator::FLAG_FREE_HEAD) {
                uint32_t count = 1U << (FLAG & 31U);
                ASSERT_LE(count, arena.pages - i);
                for (uint32_t j = 0; j < count; ++j) ++free[node(i + j)];
                i += count;
            } else {
                ++i;
            }
        }
        for (uint32_t i = arena.a->metadata_pages; i < arena.pages; ++i) ++usable[node(i)];
        for (uint16_t n = 0; n < 64; ++n) {
            EXPECT_EQ(output.nodes[n].usable, usable[n]);
            EXPECT_EQ(output.nodes[n].free_inclusive, free[n]);
            EXPECT_EQ(output.nodes[n].cached, cached[n]);
            EXPECT_EQ(output.nodes[n].buddy_free, free[n] - cached[n]);
        }
        EXPECT_EQ(output.global.usable, arena.a->usable_pages);
        EXPECT_EQ(output.global.free_inclusive, arena.a->free_count);
        EXPECT_EQ(output.global.cached, arena.a->cached_order0_count);
        EXPECT_EQ(output.global.buddy_free, arena.a->free_count - arena.a->cached_order0_count);
        EXPECT_FALSE(arena.a->lock_held.load());
    }
};

TEST_F(NodeCounters, SeedIncludesActualEarlierAllocatedAndCachedState) {
    ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
    {
        Lock lock(*arena.a);
        uint32_t idx;
        ASSERT_TRUE(arena.a->cache_allocated_order0(arena.ptr(1027), idx));
        EXPECT_EQ(idx, 1027U);
    }
    ASSERT_EQ(bind(), NONE);
    check();
    EXPECT_NE(reinterpret_cast<uint64_t>(arena.ptr(0)), arena.physical);
    EXPECT_EQ(arena.a->node_counters.ranges, ranges);
    for (uint32_t i = 1024; i < 1032; ++i) {
        if (i == 1027) {
            Lock lock(*arena.a);
            EXPECT_EQ(arena.a->release_cached_order0_at(i), PAGE);
        } else
            arena.free(i);
        check();
    }
}

TEST_F(NodeCounters, OriginalFreedRangeSurvivesCrossNodeLowerAndUpperCoalescing) {
    ASSERT_EQ(bind(), NONE);
    for (bool lower_first : {false, true}) {
        ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
        check();
        arena.free(lower_first ? 1024 : 1025);
        check();
        arena.free(lower_first ? 1025 : 1024);
        check();
        for (uint32_t i = 1032; i > 1026;) {
            arena.free(--i);
            check();
        }
    }
}

TEST_F(NodeCounters, RoundedAllocationSplitRetagAndNonlastReferenceDoNotInventFreePages) {
    ASSERT_EQ(bind(), NONE);
    check();
    void* p;
    {
        Lock lock(*arena.a);
        p = arena.a->alloc(3 * PAGE, OWNER, 0x1234);
        ASSERT_NE(p, nullptr);
    }
    check();
    auto const FIRST = arena.index(p);
    auto const FREE = output.global.free_inclusive;
    {
        Lock lock(*arena.a);
        ASSERT_TRUE(arena.a->mark_allocated_block_kind(p, mm::PageKind::PAGE_TABLE));
        ASSERT_TRUE(arena.a->reassign_allocated_block_owner(p, mm::PhysicalPageOwner::PAGE_TABLE));
        ASSERT_TRUE(arena.a->split_allocated_block_to_order0(p));
    }
    check();
    EXPECT_EQ(output.global.free_inclusive, FREE);
    arena.a->page_refcounts[FIRST].store(2);
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->free(p), 0U);
    }
    check();
    EXPECT_EQ(output.global.free_inclusive, FREE);
    arena.a->page_refcounts[FIRST].store(1);
    for (uint32_t i = 0; i < 4; ++i) {
        arena.free(FIRST + i);
        check();
    }
}

TEST_F(NodeCounters, AllFourCacheTransitionsHaveDistinctInclusiveAndBuddyDeltas) {
    ASSERT_EQ(bind(), NONE);
    ASSERT_EQ(arena.span(1024, 4), arena.ptr(1024));
    arena.free(1024);
    check();
    auto const BEFORE = output.global;
    uint32_t idx;
    {
        Lock lock(*arena.a);
        ASSERT_NE(arena.a->claim_free_order0_for_cache(idx), nullptr);
    }
    check();
    EXPECT_EQ(output.global.free_inclusive, BEFORE.free_inclusive);
    EXPECT_EQ(output.global.cached, BEFORE.cached + 1);
    EXPECT_EQ(output.global.buddy_free + 1, BEFORE.buddy_free);
    {
        Lock lock(*arena.a);
        ASSERT_NE(arena.a->alloc_cached_order0_at(idx, OWNER, 0x99), nullptr);
    }
    check();
    EXPECT_EQ(output.global.free_inclusive + 1, BEFORE.free_inclusive);
    {
        Lock lock(*arena.a);
        uint32_t cached;
        ASSERT_TRUE(arena.a->cache_allocated_order0(arena.ptr(idx), cached));
        EXPECT_EQ(cached, idx);
    }
    check();
    EXPECT_EQ(output.global.free_inclusive, BEFORE.free_inclusive);
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->release_cached_order0_at(idx), PAGE);
    }
    check();
    EXPECT_EQ(output.global.cached, BEFORE.cached);
    EXPECT_EQ(output.global.buddy_free, BEFORE.buddy_free);
    for (uint32_t i = 1025; i < 1028; ++i) arena.free(i);
    check();
}

TEST_F(NodeCounters, FinalInsertionRepairRecountsCompletedFlagsAndNodeTotalsExactlyOnce) {
    ASSERT_EQ(bind(), NONE);
    ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
    {
        Lock lock(*arena.a);
        arena.a->free_list[0] = static_cast<mm::PageAllocator::FreeBlock*>(arena.ptr(1025));
    }
    arena.free(1024);
    check();
    for (uint32_t i = 1025; i < 1032; ++i) arena.free(i);
    check();
}

TEST_F(NodeCounters, LaterOrderOneRepairRecountsCompletedFlagsAndNodeTotalsExactlyOnce) {
    ASSERT_EQ(bind(), NONE);
    ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
    for (uint32_t i = 1032; i > 1025;) arena.free(--i);
    {
        Lock lock(*arena.a);
        static_cast<mm::PageAllocator::FreeBlock*>(arena.ptr(1026))->prev = static_cast<mm::PageAllocator::FreeBlock*>(arena.ptr(1024));
    }
    arena.free(1024);
    check();
}

TEST_F(NodeCounters, LaterOrderTwoRepairRecountsCompletedFlagsAndNodeTotalsExactlyOnce) {
    ASSERT_EQ(bind(), NONE);
    ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
    for (uint32_t i = 1032; i > 1025;) arena.free(--i);
    {
        Lock lock(*arena.a);
        static_cast<mm::PageAllocator::FreeBlock*>(arena.ptr(1028))->prev = static_cast<mm::PageAllocator::FreeBlock*>(arena.ptr(1024));
    }
    arena.free(1024);
    check();
}

TEST_F(NodeCounters, InvalidBindingsAndQueriesLeaveArenaAndOutputUnchanged) {
    auto const BEFORE = arena.image();
    for (unsigned scenario = 0; scenario < 9; ++scenario) {
        SCOPED_TRACE(scenario);
        auto saved = ranges[1];
        uint16_t nodes = 3, count = 3;
        if (scenario == 0) nodes = 0;
        if (scenario == 1) nodes = 65;
        if (scenario == 2) count = 513;
        if (scenario == 3) ranges[1].base += PAGE;
        if (scenario == 4) ranges[1].base -= PAGE;
        if (scenario == 5) ranges[1].node = nodes;
        if (scenario == 6) ranges[1].length = UINT64_MAX;
        if (scenario == 7) ranges[1].base += 1;
        if (scenario == 8) count = 0;
        EXPECT_NE(nc::initialize(work->views, 1, ranges, count, nodes, *boot), NONE);
        EXPECT_EQ(arena.image(), BEFORE);
        EXPECT_FALSE(arena.a->lock_held.load());
        ranges[1] = saved;
    }
    ASSERT_EQ(bind(), NONE);
    check();
    auto const BOUND = arena.image();
    EXPECT_EQ(bind(), mm::NodeCounterError::ALREADY_BOUND);
    EXPECT_EQ(arena.image(), BOUND);
    {
        Lock lock(*arena.a);
        mm::NodeCounts32 row{1, 2, 3, 4};
        EXPECT_EQ(arena.a->node_counts_locked(3, row), mm::NodeCounterError::INVALID_INPUT);
        EXPECT_EQ(row.usable, 1U);
        EXPECT_EQ(row.free_inclusive, 2U);
        EXPECT_EQ(row.cached, 3U);
        EXPECT_EQ(row.buddy_free, 4U);
    }
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->alloc_order0_span_in_range(arena.physical + PAGE, arena.physical, arena.phys(arena.pages), 1, PAGE, OWNER),
                  nullptr);
    }
    EXPECT_EQ(arena.image(), BOUND);
}

TEST_F(NodeCounters, ExactOrderZeroAndCounterRefusalsPreserveState) {
    mm::NodeCounts32 row{1, 2, 3, 4};
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->node_counts_locked(0, row), mm::NodeCounterError::NOT_READY);
    }
    EXPECT_EQ(row.usable, 1U);
    ASSERT_EQ(bind(), NONE);
    ASSERT_EQ(arena.span(1024, 8), arena.ptr(1024));
    arena.free(1024);
    check();
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->alloc_order0(OWNER), arena.ptr(1024));
    }
    check();
    arena.free(1024);
    check();
    uint32_t saved = arena.a->node_counters.free_inclusive[0];
    arena.a->node_counters.free_inclusive[0] = 0;
    auto const CORRUPT = arena.image();
    {
        Lock lock(*arena.a);
        EXPECT_EQ(arena.a->alloc_order0_span_in_range(arena.physical, arena.phys(1024), arena.phys(1025), 1, PAGE, OWNER), nullptr);
        EXPECT_EQ(arena.a->node_counts_locked(0, row), mm::NodeCounterError::COUNTERS);
    }
    EXPECT_EQ(arena.image(), CORRUPT);
    EXPECT_EQ(row.usable, 1U);
    EXPECT_EQ(nc::snapshot(work->views, 1, ranges, 3, 3, output, *work), mm::NodeCounterError::COUNTERS);
    EXPECT_FALSE(output.complete);
    EXPECT_EQ(output.global.usable, 0U);
    arena.a->node_counters.free_inclusive[0] = saved;
    ASSERT_EQ(arena.span(1024, 1), arena.ptr(1024));
    for (uint32_t i = 1024; i < 1032; ++i) arena.free(i);
    check();
}

TEST_F(NodeCounters, InvalidCachedMetadataCannotPublishAnyCounterBinding) {
    ASSERT_EQ(arena.span(1024, 1), arena.ptr(1024));
    {
        Lock lock(*arena.a);
        uint32_t cached;
        ASSERT_TRUE(arena.a->cache_allocated_order0(arena.ptr(1024), cached));
    }
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        if (scenario == 0) arena.a->page_kinds[1024].store(static_cast<uint8_t>(mm::PageKind::NORMAL));
        if (scenario == 1) arena.a->page_refcounts[1024].store(1);
        if (scenario == 2) arena.a->page_owners[1024] = static_cast<uint8_t>(OWNER);
        auto const BEFORE = arena.image();
        EXPECT_EQ(bind(), mm::NodeCounterError::METADATA);
        EXPECT_EQ(arena.image(), BEFORE);
        EXPECT_EQ(arena.a->node_counters.node_count, 0U);
        EXPECT_FALSE(arena.a->lock_held.load());
        arena.a->page_kinds[1024].store(static_cast<uint8_t>(mm::PageKind::FREE));
        arena.a->page_refcounts[1024].store(0);
        arena.a->page_owners[1024] = 0;
    }
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    arena.a->page_callers[1024] = 1;
    auto const BEFORE = arena.image();
    EXPECT_EQ(bind(), mm::NodeCounterError::METADATA);
    EXPECT_EQ(arena.image(), BEFORE);
    arena.a->page_callers[1024] = 0;
#endif
    ASSERT_EQ(bind(), NONE);
    check();
}

TEST_F(NodeCounters, OneThousandActualAllocationCacheFreeStepsMatchIndependentScan) {
    ASSERT_EQ(bind(), NONE);
    check();
    auto const BASELINE = output.global;
    uint8_t ledger[16]{};
    uint32_t state = 0xC19A;
    for (unsigned step = 0; step < 1000; ++step) {
        state = state * 1664525U + 1013904223U;
        uint32_t slot = (state >> 8) % 16;
        uint32_t i = 1024 + slot;
        if (ledger[slot] == 0) {
            ASSERT_EQ(arena.span(i, 1), arena.ptr(i));
            ledger[slot] = 1;
        } else if (ledger[slot] == 1) {
            if (state & 1) {
                Lock lock(*arena.a);
                uint32_t cached;
                ASSERT_TRUE(arena.a->cache_allocated_order0(arena.ptr(i), cached));
                ledger[slot] = 2;
            } else {
                arena.free(i);
                ledger[slot] = 0;
            }
        } else {
            Lock lock(*arena.a);
            if (state & 1) {
                ASSERT_EQ(arena.a->alloc_cached_order0_at(i, OWNER), arena.ptr(i));
                ledger[slot] = 1;
            } else {
                ASSERT_EQ(arena.a->release_cached_order0_at(i), PAGE);
                ledger[slot] = 0;
            }
        }
        check();
    }
    for (uint32_t slot = 0; slot < 16; ++slot) {
        if (ledger[slot] == 1) arena.free(1024 + slot);
        if (ledger[slot] == 2) {
            Lock lock(*arena.a);
            EXPECT_EQ(arena.a->release_cached_order0_at(1024 + slot), PAGE);
        }
    }
    check();
    EXPECT_EQ(output.global.usable, BASELINE.usable);
    EXPECT_EQ(output.global.free_inclusive, BASELINE.free_inclusive);
    EXPECT_EQ(output.global.cached, BASELINE.cached);
    EXPECT_EQ(output.global.buddy_free, BASELINE.buddy_free);
}

TEST_F(NodeCounters, CheapSnapshotAndQueryNeverReadGuardedMetadataTables) {
    ASSERT_EQ(bind(), NONE);
    check();
    void* guard = mmap(nullptr, PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(guard, MAP_FAILED);
    auto* flags = arena.a->page_flags;
    auto* kinds = arena.a->page_kinds;
    auto* owners = arena.a->page_owners;
    auto* refs = arena.a->page_refcounts;
    arena.a->page_flags = static_cast<uint8_t*>(guard);
    arena.a->page_kinds = static_cast<std::atomic<uint8_t>*>(guard);
    arena.a->page_owners = static_cast<uint8_t*>(guard);
    arena.a->page_refcounts = static_cast<std::atomic<uint32_t>*>(guard);
    EXPECT_EQ(nc::snapshot(work->views, 1, ranges, 3, 3, output, *work), NONE);
    EXPECT_TRUE(output.complete);
    {
        Lock lock(*arena.a);
        mm::NodeCounts32 row;
        EXPECT_EQ(arena.a->node_counts_locked(0, row), NONE);
        EXPECT_EQ(row.usable, output.nodes[0].usable);
    }
    arena.a->page_flags = flags;
    arena.a->page_kinds = kinds;
    arena.a->page_owners = owners;
    arena.a->page_refcounts = refs;
    EXPECT_EQ(munmap(guard, PAGE), 0);
    check();
}

TEST(NodeCounterBatch, LaterPrepareFailureCannotPublishEarlierAllocator) {
    Arena first(0x10000000, 128), second(0x20000000, 128, true);
    topo::MemoryRange ranges[]{{first.physical, first.pages * PAGE, 0, 0}, {second.physical, second.pages * PAGE, 1, 0}};
    nc::AllocatorView views[]{{first.a, first.physical}, {second.a, second.physical}};
    auto scratch = std::make_unique<nc::BootstrapWorkspace>();
    auto work = std::make_unique<nc::Workspace>();
    nc::Snapshot out;
    second.a->page_flags[0] = mm::PageAllocator::FLAG_ALLOC_CONT;
    auto const A = first.image(), B = second.image();
    EXPECT_EQ(nc::initialize(views, 2, ranges, 2, 2, *scratch), mm::NodeCounterError::METADATA);
    EXPECT_EQ(first.image(), A);
    EXPECT_EQ(second.image(), B);
    EXPECT_EQ(first.a->node_counters.node_count, 0U);
    EXPECT_EQ(second.a->node_counters.node_count, 0U);
    EXPECT_FALSE(first.a->lock_held.load());
    EXPECT_FALSE(second.a->lock_held.load());
    second.a->page_flags[0] = mm::PageAllocator::FLAG_RESERVED;
    ASSERT_EQ(nc::initialize(views, 2, ranges, 2, 2, *scratch), NONE);
    ASSERT_EQ(nc::snapshot(views, 2, ranges, 2, 2, out, *work), NONE);
    EXPECT_EQ(out.global.usable, first.a->usable_pages + second.a->usable_pages);
    EXPECT_EQ(out.node_count, 2U);
    EXPECT_EQ(out.nodes[0].usable, first.a->usable_pages);
    EXPECT_EQ(out.nodes[1].usable, second.a->usable_pages);
    views[1] = views[0];
    EXPECT_EQ(nc::snapshot(views, 2, ranges, 2, 2, out, *work), mm::NodeCounterError::INVALID_INPUT);
    EXPECT_FALSE(out.complete);
    EXPECT_EQ(out.global.usable, 0U);
}

TEST(NodeCounterBatch, All64NodesAnd512IntervalsStayWithinFixedCapacity) {
    Arena arena;
    std::array<topo::MemoryRange, 512> ranges;
    for (uint16_t i = 0; i < 512; ++i) ranges[i] = {arena.phys(i * 4), 4 * PAGE, static_cast<uint16_t>(i % 64), 0};
    nc::AllocatorView view{arena.a, arena.physical};
    auto boot = std::make_unique<nc::BootstrapWorkspace>();
    auto work = std::make_unique<nc::Workspace>();
    nc::Snapshot out;
    ASSERT_EQ(nc::initialize(&view, 1, ranges.data(), 512, 64, *boot), NONE);
    ASSERT_EQ(nc::snapshot(&view, 1, ranges.data(), 512, 64, out, *work), NONE);
    EXPECT_EQ(out.node_count, 64U);
    EXPECT_EQ(out.nodes[63].usable, 32U);
    {
        Lock lock(*arena.a);
        void* p = arena.a->alloc_order0_span_in_range(arena.physical, arena.phys(252), arena.phys(256), 1, PAGE, OWNER);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(arena.a->node_counters.free_inclusive[63], 31U);
        EXPECT_EQ(arena.a->free(p), PAGE);
    }
    ASSERT_EQ(nc::snapshot(&view, 1, ranges.data(), 512, 64, out, *work), NONE);
    EXPECT_EQ(out.nodes[63].free_inclusive, 32U);
    EXPECT_EQ(nc::snapshot(&view, 130, ranges.data(), 512, 64, out, *work), mm::NodeCounterError::CAPACITY);
}

TEST(NodeCounterBatch, CompiledObjectAndMetadataSizeGuardBothProfilesIncludingZeroUsable) {
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    EXPECT_EQ(sizeof(mm::PageAllocator), 1696U);
    constexpr uint64_t OLD_SIZE = 904;
#else
    EXPECT_EQ(sizeof(mm::PageAllocator), 1688U);
    constexpr uint64_t OLD_SIZE = 896;
#endif
    EXPECT_EQ(sizeof(mm::NodeCounterStore), 792U);
    EXPECT_LE(sizeof(mm::PageAllocator), PAGE);
    for (uint32_t pages : {1U, 2U, 64U, 160U, 161U, 212U, 213U, 343U, 344U, 456U, 457U, 1024U}) {
        Arena arena(0x40000000, pages);
        auto bytes = (sizeof(mm::PageAllocator) + 3ULL * pages + 3) & ~3ULL;
        auto old_bytes = (OLD_SIZE + 3ULL * pages + 3) & ~3ULL;
        bytes += 4ULL * pages;
        old_bytes += 4ULL * pages;
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        bytes = ((bytes + 7) & ~7ULL) + 8ULL * pages;
        old_bytes = ((old_bytes + 7) & ~7ULL) + 8ULL * pages;
#endif
        auto expected = (bytes + PAGE - 1) / PAGE;
        EXPECT_EQ(arena.a->metadata_pages, expected);
        EXPECT_LE(expected - (old_bytes + PAGE - 1) / PAGE, 1U);
        topo::MemoryRange range{arena.physical, pages * PAGE, 0, 0};
        nc::AllocatorView view{arena.a, arena.physical};
        auto boot = std::make_unique<nc::BootstrapWorkspace>();
        ASSERT_EQ(nc::initialize(&view, 1, &range, 1, 1, *boot), NONE);
        EXPECT_EQ(arena.a->node_counters.usable[0], arena.a->usable_pages);
    }
}
}  // namespace
