#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <new>
#include <platform/mm/numa_balance_scan.hpp>
#include <span>
#include <tuple>
#include <vector>

#include "numa_balance_reference_scan.hpp"

namespace {
namespace mm = ker::mod::mm;
namespace nb = mm::numa::balance;
namespace topology = ker::mod::acpi::numa;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr size_t BYTES = 8 * 1024 * 1024;
constexpr auto OWNER = mm::PhysicalPageOwner::USER_PRIVATE_MAPPING;
constexpr size_t OWN = static_cast<size_t>(OWNER);

struct FreeStorage {
    void operator()(void* pointer) const { std::free(pointer); }
};

struct Arena {
    std::unique_ptr<void, FreeStorage> storage{};
    mm::PageAllocator* allocator{};
    size_t bytes{};
    uint64_t physical{};

    explicit Arena(uint64_t physical_base = 0x40003000ULL, size_t length = BYTES) : bytes(length), physical(physical_base) {
        // Deliberately different Host/physical alignment. The real allocator
        // remains embedded in its real backing; no allocator metadata model.
        constexpr size_t ALIGNMENT = 2 * 1024 * 1024;
        size_t const STORAGE_BYTES = (length + PAGE + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
        storage.reset(std::aligned_alloc(ALIGNMENT, STORAGE_BYTES));
        if (storage == nullptr) std::abort();
        auto* base = static_cast<std::byte*>(storage.get()) + PAGE;
        std::memset(base, 0, length);
        allocator = ::new (base) mm::PageAllocator{};
        allocator->init(reinterpret_cast<uint64_t>(base), length);
    }
    auto pointer(uint32_t index) const -> void* { return reinterpret_cast<void*>(allocator->base + static_cast<uint64_t>(index) * PAGE); }
    auto index(void* pointer) const -> uint32_t {
        return static_cast<uint32_t>((reinterpret_cast<uint64_t>(pointer) - allocator->base) / PAGE);
    }
    auto end() const -> uint64_t { return physical + static_cast<uint64_t>(allocator->total_pages) * PAGE; }
    auto view(bool descriptor = true) const -> nb::ZoneView { return {allocator, physical, 0, descriptor}; }
    auto copy() const -> std::vector<std::byte> {
        std::vector<std::byte> result(bytes);
        std::memcpy(result.data(), reinterpret_cast<void*>(allocator->base), bytes);
        return result;
    }
};

class Lock {
   public:
    explicit Lock(mm::PageAllocator& allocator) : allocator_(allocator), flags_(allocator.lock_irq()) {}
    ~Lock() { allocator_.unlock_irq(flags_); }

   private:
    mm::PageAllocator& allocator_;
    uint64_t flags_;
};

class NumaBalance : public ::testing::Test {
   protected:
    Arena arena;
    std::unique_ptr<topology::Topology> domains = std::make_unique<topology::Topology>();
    std::unique_ptr<nb::Snapshot> output = std::make_unique<nb::Snapshot>();
    std::unique_ptr<nb::Workspace> workspace = std::make_unique<nb::Workspace>();

    std::unique_ptr<nb::Snapshot> reference_output = std::make_unique<nb::Snapshot>();
    std::unique_ptr<nb::Workspace> reference_workspace = std::make_unique<nb::Workspace>();

    static void same_counts(const nb::Counts& a, const nb::Counts& b) {
        auto fields = [](auto const& c) {
            return std::tie(c.managed_pages, c.free_pages, c.cached_free_pages, c.allocator_metadata_pages, c.zone_descriptor_pages,
                            c.owner_pages, c.kind_pages, c.sampled_zero_ref_owned_pages, c.sampled_single_ref_owned_pages,
                            c.sampled_shared_ref_owned_pages);
        };
        EXPECT_EQ(fields(a), fields(b));
        for (size_t i = 0; i < mm::PHYSICAL_PAGE_OWNER_COUNT; ++i) {
            EXPECT_EQ(a.owners[i].pages, b.owners[i].pages);
            EXPECT_EQ(a.owners[i].objects, b.owners[i].objects);
        }
    }
    void same_snapshot() {
        auto const& a = *output;
        auto const& b = *reference_output;
        EXPECT_EQ(std::tie(a.complete, a.provenance, a.node_count, a.zone_count, a.scanned_frames),
                  std::tie(b.complete, b.provenance, b.node_count, b.zone_count, b.scanned_frames));
        EXPECT_EQ(std::tie(a.result.error, a.result.zone_index, a.result.physical, a.result.flags, a.result.kind, a.result.owner),
                  std::tie(b.result.error, b.result.zone_index, b.result.physical, b.result.flags, b.result.kind, b.result.owner));
        auto global = [](auto const& c) {
            return std::tie(c.managed_pages, c.free_pages, c.allocator_metadata_pages, c.zone_descriptor_pages,
                            c.per_cpu_page_cache_reserve_pages, c.per_cpu_page_cache_reserve_objects, c.owner_pages, c.identity_pages,
                            c.identity_mismatch_pages, c.untracked_unreclaimable_pages, c.zone_count);
        };
        EXPECT_EQ(global(a.global), global(b.global));
        for (size_t i = 0; i < mm::PHYSICAL_PAGE_OWNER_COUNT; ++i) {
            EXPECT_EQ(a.global.owners[i].pages, b.global.owners[i].pages);
            EXPECT_EQ(a.global.owners[i].objects, b.global.owners[i].objects);
        }
        for (size_t i = 0; i < mm::phys::PHYSICAL_RESERVE_KIND_COUNT; ++i) {
            EXPECT_EQ(a.global.firmware_reserves[i].pages, b.global.firmware_reserves[i].pages);
            EXPECT_EQ(a.global.firmware_reserves[i].objects, b.global.firmware_reserves[i].objects);
        }
        same_counts(a.scanned, b.scanned);
        for (size_t i = 0; i < a.nodes.size(); ++i) same_counts(a.nodes[i], b.nodes[i]);
    }

    void SetUp() override { ranges({{arena.physical - PAGE, arena.end() - arena.physical + PAGE, 0, 0}}); }

    void ranges(std::initializer_list<topology::MemoryRange> input, uint16_t nodes = 3) {
        std::memset(domains.get(), 0, sizeof(*domains));
        domains->node_count = nodes;
        domains->memory_range_count = static_cast<uint16_t>(input.size());
        domains->provenance = topology::Provenance::SRAT_SLIT;
        size_t index = 0;
        for (auto const& range : input) {
            domains->memory[index++] = range;
            domains->nodes[range.node].usable_bytes += range.length;
        }
    }

    auto scan(std::span<const nb::ZoneView> views, uint64_t limit = UINT64_MAX) -> nb::Result {
        auto const old_prepared = nb::reference::prepare_scan(*domains, views, *reference_output, *reference_workspace, limit);
        auto result = nb::detail::prepare_scan(*domains, views, *output, *workspace, limit);
        EXPECT_EQ(result.valid(), old_prepared.valid());
        same_snapshot();
        if (!result.valid()) return result;
        // Both actual scanners see the SAME stable real PageAllocator state
        // under the production all-lock interval. Compare typed fields, not padding.
        for (size_t i = 0; i < workspace->zone_count; ++i) workspace->lock_flags[i] = workspace->zones[i].allocator->lock_irq();
        auto const old_result = nb::reference::scan_locked(*domains, *reference_output, *reference_workspace);
        result = nb::detail::scan_locked(*domains, *output, *workspace);
        EXPECT_EQ(result.valid(), old_result.valid());
        EXPECT_FALSE(output->complete);
        for (size_t i = workspace->zone_count; i != 0; --i) workspace->zones[i - 1].allocator->unlock_irq(workspace->lock_flags[i - 1]);
        (void)nb::reference::finish_unlocked(*reference_output, *reference_workspace);
        result = nb::detail::finish_unlocked(*output, *workspace);
        same_snapshot();
        return result;
    }

    auto scan(uint64_t limit = UINT64_MAX) -> nb::Result {
        auto const view = arena.view();
        return scan(std::span<const nb::ZoneView>{&view, 1}, limit);
    }

    auto allocate(uint64_t bytes, mm::PhysicalPageOwner owner = OWNER) -> void* {
        Lock const LOCK(*arena.allocator);
        return arena.allocator->alloc(bytes, owner);
    }
    void release(void* pointer, uint64_t expected) {
        Lock const LOCK(*arena.allocator);
        EXPECT_EQ(arena.allocator->free(pointer), expected);
    }
    void expect_exact() {
        ASSERT_TRUE(scan().valid());
        EXPECT_TRUE(output->complete);
        EXPECT_EQ(output->global.identity_mismatch_pages, 0U);
        EXPECT_EQ(output->global.managed_pages, arena.allocator->total_pages + 1U);
        EXPECT_EQ(output->global.zone_descriptor_pages, 1U);
        EXPECT_EQ(output->global.free_pages, arena.allocator->free_count);
        EXPECT_EQ(output->global.owners[OWN].pages, arena.allocator->owner_pages[OWN]);
    }
};

TEST_F(NumaBalance, RealAllocatorAllocationCacheTransferSplitAndReturn) {
    expect_exact();
    uint64_t const FREE = output->global.free_pages;
    void* block = allocate(3 * PAGE);
    void* cached = allocate(PAGE);
    ASSERT_NE(block, nullptr);
    ASSERT_NE(cached, nullptr);
    uint32_t cached_index = 0;
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_TRUE(arena.allocator->cache_allocated_order0(cached, cached_index));
    }
    expect_exact();
    EXPECT_EQ(output->scanned.cached_free_pages, 1U);
    EXPECT_EQ(output->global.owners[OWN].pages, 4U);
    EXPECT_EQ(output->global.owners[OWN].objects, 1U);
    EXPECT_EQ(output->global.free_pages, FREE - 4);
    constexpr auto SECOND = mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE;
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_EQ(arena.allocator->alloc_cached_order0_at(cached_index, SECOND), cached);
        ASSERT_TRUE(arena.allocator->reassign_allocated_block_owner(block, SECOND));
        ASSERT_TRUE(arena.allocator->split_allocated_block_to_order0(block));
    }
    expect_exact();
    size_t const second = static_cast<size_t>(SECOND);
    EXPECT_EQ(output->global.owners[OWN].pages, 0U);
    EXPECT_EQ(output->global.owners[second].pages, 5U);
    EXPECT_EQ(output->global.owners[second].objects, 5U);
    EXPECT_EQ(output->scanned.cached_free_pages, 0U);
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_TRUE(arena.allocator->cache_allocated_order0(cached, cached_index));
        EXPECT_EQ(arena.allocator->release_cached_order0_at(cached_index), PAGE);
    }
    for (uint32_t i = 0; i < 4; ++i) release(static_cast<std::byte*>(block) + i * PAGE, PAGE);
    expect_exact();
    EXPECT_EQ(output->global.free_pages, FREE);
    EXPECT_EQ(output->global.owner_pages, 0U);
}

TEST_F(NumaBalance, CrossDomainHeadsAndIndependentSpanAreCountedPhysically) {
    void* block = allocate(4 * PAGE);
    ASSERT_NE(block, nullptr);
    uint64_t const BOUNDARY = arena.physical + (arena.index(block) + 2U) * PAGE;
    uint64_t const START = arena.physical - PAGE;
    ranges({{START, BOUNDARY - START, 0, 0}, {BOUNDARY, arena.end() - BOUNDARY, 1, 0}});
    expect_exact();
    EXPECT_EQ(output->nodes[0].owners[OWN].pages, 2U);
    EXPECT_EQ(output->nodes[1].owners[OWN].pages, 2U);
    EXPECT_EQ(output->nodes[0].owners[OWN].objects, 1U);
    EXPECT_EQ(output->nodes[1].owners[OWN].objects, 0U);
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_TRUE(arena.allocator->split_allocated_block_to_order0(block));
    }
    expect_exact();
    EXPECT_EQ(output->nodes[0].owners[OWN].objects, 2U);
    EXPECT_EQ(output->nodes[1].owners[OWN].objects, 2U);
    for (uint32_t i = 0; i < 4; ++i) release(static_cast<std::byte*>(block) + i * PAGE, PAGE);
    void* span = nullptr;
    {
        Lock const LOCK(*arena.allocator);
        span = arena.allocator->alloc_order0_span_in_range(arena.physical, START, arena.end(), 5, PAGE, OWNER);
    }
    ASSERT_NE(span, nullptr);
    expect_exact();
    EXPECT_EQ(output->global.owners[OWN].pages, 5U);
    EXPECT_EQ(output->global.owners[OWN].objects, 5U);
    for (uint32_t i = 0; i < 5; ++i) release(static_cast<std::byte*>(span) + i * PAGE, PAGE);
}

TEST_F(NumaBalance, DescriptorHasOwnAffinityAndMemorylessNodesAreZero) {
    ranges({{arena.physical - PAGE, PAGE, 0, 0}, {arena.physical, arena.end() - arena.physical, 1, 0}});
    expect_exact();
    EXPECT_EQ(output->nodes[0].managed_pages, 1U);
    EXPECT_EQ(output->nodes[0].zone_descriptor_pages, 1U);
    EXPECT_EQ(output->nodes[0].free_pages, 0U);
    EXPECT_EQ(output->nodes[1].allocator_metadata_pages, arena.allocator->metadata_pages);
    EXPECT_EQ(output->nodes[1].zone_descriptor_pages, 0U);
    EXPECT_EQ(output->nodes[2].managed_pages, 0U);
    EXPECT_TRUE(nb::detail::identity(output->nodes[2]));
}

TEST_F(NumaBalance, OptionalHugeViewHasNoSyntheticDescriptorAndViewsAreSorted) {
    Arena later(arena.end() + 3 * PAGE);
    ranges({{arena.physical - PAGE, arena.end() - arena.physical + PAGE, 0, 0}, {later.physical, later.end() - later.physical, 1, 0}});
    std::array<nb::ZoneView, 2> views{later.view(false), arena.view()};
    auto const before = arena.copy();
    auto const before_later = later.copy();
    ASSERT_TRUE(scan(views).valid());
    EXPECT_TRUE(output->complete);
    EXPECT_EQ(output->global.zone_descriptor_pages, 1U);
    EXPECT_EQ(output->nodes[1].zone_descriptor_pages, 0U);
    EXPECT_EQ(output->global.managed_pages, arena.allocator->total_pages + later.allocator->total_pages + 1U);
    EXPECT_EQ(std::memcmp(before.data(), reinterpret_cast<void*>(arena.allocator->base), arena.bytes), 0);
    EXPECT_EQ(std::memcmp(before_later.data(), reinterpret_cast<void*>(later.allocator->base), later.bytes), 0);
    EXPECT_FALSE(arena.allocator->lock_held.load());
    EXPECT_FALSE(later.allocator->lock_held.load());
}

TEST_F(NumaBalance, SharedAndZeroRefSamplesDoNotMultiplyOrDiscardOwners) {
    void* block = allocate(2 * PAGE);
    ASSERT_NE(block, nullptr);
    uint32_t const INDEX = arena.index(block);
    // Scoped Host seam: actual atomic side-table transitions, not hardware
    // pins or phys::page_ref_dec. Restore before actual allocator free.
    arena.allocator->page_refcounts[INDEX].store(2);
    arena.allocator->page_refcounts[INDEX + 1].store(0);
    expect_exact();
    EXPECT_EQ(output->global.owners[OWN].pages, 2U);
    EXPECT_EQ(output->global.owners[OWN].objects, 1U);
    EXPECT_EQ(output->scanned.sampled_shared_ref_owned_pages, 1U);
    EXPECT_EQ(output->scanned.sampled_zero_ref_owned_pages, 1U);
    arena.allocator->page_refcounts[INDEX].store(1);
    arena.allocator->page_refcounts[INDEX + 1].store(1);
    release(block, 2 * PAGE);
}

TEST_F(NumaBalance, RealKindTagsAreTelemetryAndInvalidRawTagsRefuse) {
    void* block = allocate(PAGE);
    ASSERT_NE(block, nullptr);
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_TRUE(arena.allocator->mark_allocated_block_kind(block, mm::PageKind::PAGE_TABLE));
    }
    expect_exact();
    EXPECT_EQ(output->scanned.kind_pages[static_cast<size_t>(mm::PageKind::PAGE_TABLE)], 1U);
    auto const INDEX = arena.index(block);
    arena.allocator->page_kinds[INDEX].store(255);
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    EXPECT_FALSE(output->complete);
    arena.allocator->page_kinds[INDEX].store(static_cast<uint8_t>(mm::PageKind::UNKNOWN));
    expect_exact();
    EXPECT_EQ(output->scanned.kind_pages[static_cast<size_t>(mm::PageKind::UNKNOWN)], 1U);
    release(block, PAGE);
}

TEST_F(NumaBalance, BudgetRefusesBeforeLockAndPreparationSupportsWorkspaceInput) {
    auto const original = arena.copy();
    arena.allocator->lock_held.store(true);
    EXPECT_EQ(scan(arena.allocator->total_pages).error, nb::Error::FRAME_LIMIT);
    EXPECT_TRUE(arena.allocator->lock_held.load());
    arena.allocator->lock_held.store(false);
    EXPECT_FALSE(output->complete);
    EXPECT_EQ(output->scanned_frames, 0U);
    EXPECT_EQ(std::memcmp(original.data(), reinterpret_cast<void*>(arena.allocator->base), arena.bytes), 0);
    workspace->zones[0] = arena.view();
    ASSERT_TRUE(nb::detail::prepare_scan(*domains, std::span<const nb::ZoneView>{workspace->zones.data(), 1}, *output, *workspace,
                                         arena.allocator->total_pages + 1U)
                    .valid());
    auto const FLAGS = arena.allocator->lock_irq();
    auto const SCANNED = nb::detail::scan_locked(*domains, *output, *workspace);
    arena.allocator->unlock_irq(FLAGS);
    ASSERT_TRUE(SCANNED.valid());
    EXPECT_TRUE(nb::detail::finish_unlocked(*output, *workspace).valid());
    EXPECT_TRUE(output->complete);
}

TEST_F(NumaBalance, MalformedAllocatedFlagsOwnersAndContinuationsRefuseWithoutMutation) {
    void* block = allocate(4 * PAGE);
    ASSERT_NE(block, nullptr);
    uint32_t const INDEX = arena.index(block);
    auto const pristine = arena.copy();
    auto bad_flag = [&](uint32_t page, uint8_t value) {
        uint8_t const OLD = arena.allocator->page_flags[page];
        arena.allocator->page_flags[page] = value;
        auto const corrupted = arena.copy();
        EXPECT_EQ(scan().error, nb::Error::METADATA);
        EXPECT_FALSE(output->complete);
        EXPECT_EQ(std::memcmp(corrupted.data(), reinterpret_cast<void*>(arena.allocator->base), arena.bytes), 0);
        arena.allocator->page_flags[page] = OLD;
    };
    bad_flag(INDEX, mm::PageAllocator::FLAG_ALLOC_CONT);
    bad_flag(INDEX, mm::PageAllocator::FLAG_ALLOC_HEAD | 21U);
    bad_flag(INDEX, mm::PageAllocator::FLAG_ALLOC_HEAD | 20U);
    bad_flag(INDEX, mm::PageAllocator::FLAG_ALLOC_HEAD | 0x20U);
    bad_flag(INDEX + 1, mm::PageAllocator::FLAG_ALLOC_HEAD);
    bad_flag(INDEX, mm::PageAllocator::FLAG_RESERVED);
    uint8_t const OLD_OWNER = arena.allocator->page_owners[INDEX];
    arena.allocator->page_owners[INDEX] = 0;
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_owners[INDEX] = 255;
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_owners[INDEX] = OLD_OWNER;
    arena.allocator->page_owners[INDEX + 1] = static_cast<uint8_t>(mm::PhysicalPageOwner::TMPFS_DATA);
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_owners[INDEX + 1] = OLD_OWNER;
    EXPECT_EQ(std::memcmp(pristine.data(), reinterpret_cast<void*>(arena.allocator->base), arena.bytes), 0);
    expect_exact();
    release(block, 4 * PAGE);
}

TEST_F(NumaBalance, ActualCounterCacheAndReservedMetadataCorruptionAreDetected) {
    auto check_counter = [&](auto& counter) {
        ++counter;
        EXPECT_EQ(scan().error, nb::Error::COUNTERS);
        EXPECT_FALSE(output->complete);
        --counter;
    };
    check_counter(arena.allocator->free_count);
    check_counter(arena.allocator->cached_order0_count);
    check_counter(arena.allocator->owner_objects[OWN]);
    check_counter(arena.allocator->owner_pages[OWN]);
    arena.allocator->page_owners[0] = static_cast<uint8_t>(OWNER);
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_owners[0] = 0;
    arena.allocator->page_kinds[0].store(static_cast<uint8_t>(mm::PageKind::FREE));
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_kinds[0].store(static_cast<uint8_t>(mm::PageKind::RESERVED));
    uint32_t cached_index = 0;
    void* cached = allocate(PAGE);
    ASSERT_NE(cached, nullptr);
    {
        Lock const LOCK(*arena.allocator);
        ASSERT_TRUE(arena.allocator->cache_allocated_order0(cached, cached_index));
    }
    arena.allocator->page_owners[cached_index] = static_cast<uint8_t>(OWNER);
    EXPECT_EQ(scan().error, nb::Error::METADATA);
    arena.allocator->page_owners[cached_index] = 0;
    expect_exact();
    {
        Lock const LOCK(*arena.allocator);
        EXPECT_EQ(arena.allocator->release_cached_order0_at(cached_index), PAGE);
    }
}

TEST_F(NumaBalance, CoverageDuplicatesOverflowAndCapacityFailClosed) {
    std::array<nb::ZoneView, 2> duplicate{arena.view(), arena.view(false)};
    EXPECT_EQ(scan(duplicate).error, nb::Error::OVERLAP);
    auto view = arena.view();
    view.physical_base = UINT64_MAX - PAGE + 1;
    EXPECT_EQ(scan(std::span<const nb::ZoneView>{&view, 1}).error, nb::Error::ZONE);
    view = arena.view();
    view.physical_base += 1;
    EXPECT_EQ(scan(std::span<const nb::ZoneView>{&view, 1}).error, nb::Error::ZONE);
    auto* const OLD_FLAGS = arena.allocator->page_flags;
    arena.allocator->page_flags = reinterpret_cast<uint8_t*>(arena.allocator->base + arena.bytes);
    EXPECT_EQ(scan().error, nb::Error::ZONE);
    arena.allocator->page_flags = OLD_FLAGS;
    std::array<nb::ZoneView, nb::MAX_ZONES + 1> too_many{};
    EXPECT_EQ(scan(too_many).error, nb::Error::CAPACITY);
    ranges({{arena.physical, arena.end() - arena.physical, 0, 0}});
    EXPECT_EQ(scan().error, nb::Error::COVERAGE);  // Descriptor is missing.
    ranges({{arena.physical - PAGE, arena.end() - arena.physical + 2 * PAGE, 0, 0}});
    EXPECT_EQ(scan().error, nb::Error::COVERAGE);  // Extra topology frame.
    ranges({{arena.physical - PAGE, 2 * PAGE, 0, 0}, {arena.physical, arena.end() - arena.physical, 1, 0}});
    EXPECT_EQ(scan().error, nb::Error::TOPOLOGY);  // Overlap.
    EXPECT_FALSE(output->complete);
}

TEST_F(NumaBalance, WhollyReservedRealAllocatorAndMissingLockAreHandled) {
    Arena tiny(0x80003000, PAGE);
    ASSERT_EQ(tiny.allocator->metadata_pages, tiny.allocator->total_pages);
    ranges({{tiny.physical - PAGE, 2 * PAGE, 0, 0}});
    auto const view = tiny.view();
    ASSERT_TRUE(scan(std::span<const nb::ZoneView>{&view, 1}).valid());
    EXPECT_EQ(output->global.free_pages, 0U);
    EXPECT_EQ(output->global.allocator_metadata_pages, 1U);
    ASSERT_TRUE(nb::detail::prepare_scan(*domains, std::span<const nb::ZoneView>{&view, 1}, *output, *workspace, 2).valid());
    EXPECT_EQ(nb::detail::scan_locked(*domains, *output, *workspace).error, nb::Error::INVALID_INPUT);
    EXPECT_FALSE(nb::detail::finish_unlocked(*output, *workspace).valid());
    EXPECT_FALSE(output->complete);
}

TEST_F(NumaBalance, EveryCorruptContinuationKeepsTheExactOldFailurePrefixAcrossNodes) {
    void* block = allocate(32 * PAGE);
    ASSERT_NE(block, nullptr);
    uint32_t const INDEX = arena.index(block);
    uint64_t const CUT = arena.physical + static_cast<uint64_t>(INDEX + 11) * PAGE;
    ranges({{arena.physical - PAGE, CUT - arena.physical + PAGE, 0, 0}, {CUT, arena.end() - CUT, 1, 0}});
    for (uint32_t page = 0; page < 32; ++page) {
        uint8_t const FLAG = arena.allocator->page_flags[INDEX + page];
        arena.allocator->page_flags[INDEX + page] = mm::PageAllocator::FLAG_RESERVED;
        EXPECT_EQ(scan().error, nb::Error::METADATA);
        EXPECT_EQ(output->result.physical, arena.physical + static_cast<uint64_t>(INDEX + page) * PAGE);
        arena.allocator->page_flags[INDEX + page] = FLAG;
    }
    for (uint32_t page = 0; page < 32; ++page) arena.allocator->page_refcounts[INDEX + page].store(page % 3);
    expect_exact();
    EXPECT_EQ(output->scanned.sampled_zero_ref_owned_pages, 11U);
    EXPECT_EQ(output->scanned.sampled_single_ref_owned_pages, 11U);
    EXPECT_EQ(output->scanned.sampled_shared_ref_owned_pages, 10U);
    for (uint32_t page = 0; page < 32; ++page) arena.allocator->page_refcounts[INDEX + page].store(1);
    release(block, 32 * PAGE);
}

TEST_F(NumaBalance, CheckedBulkArithmeticRefusesOverflow) {
    nb::Counts counts{};
    counts.managed_pages = UINT64_MAX - 7;
    EXPECT_FALSE(nb::detail::count_run(counts, nb::detail::Category::FREE, static_cast<uint8_t>(mm::PageKind::FREE), 0, 8, 0, 0, 0, 0));
    EXPECT_EQ(counts.managed_pages, UINT64_MAX - 7);
    counts = {};
    counts.owners[OWN].objects = UINT64_MAX;
    EXPECT_FALSE(nb::detail::count_run(counts, nb::detail::Category::OWNED, static_cast<uint8_t>(mm::PageKind::NORMAL),
                                       static_cast<uint8_t>(OWNER), 512, 1, 0, 512, 0));
}

}  // namespace
