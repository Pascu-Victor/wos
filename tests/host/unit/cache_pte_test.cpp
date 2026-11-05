#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <platform/mm/cache_bits.hpp>
#include <platform/mm/paging.hpp>

namespace {
namespace cache = ker::mod::mm::cache;
namespace paging = ker::mod::mm::paging;

constexpr std::array<uint64_t, 8> CACHE_FLAGS{0x00, 0x08, 0x10, 0x18, 0x80, 0x88, 0x90, 0x98};
constexpr uint64_t PHYSICAL = 0x000123456789a000ULL;
constexpr uint64_t FRAME_MASK = 0x000ffffffffff000ULL;

static_assert(cache::leaf_selector(0x89, 1) == 5);
static_assert(cache::leaf_selector(0x1089, 2) == 5);
static_assert(cache::selector_flags_4k(5) == 0x88);
static_assert(!cache::leaf_selector(0x89, 4));
static_assert(!cache::selector_flags_4k(8));
static_assert(paging::table_flags_for_leaf(UINT64_MAX) == 7);
static_assert(paging::table_flags_for_leaf(0) == 3);
static_assert(paging::table_flags_for_leaf(paging::PAGE_RESERVED | paging::PAGE_USER) == 7);
static_assert(cache::framebuffer_page_extent(0x1003, 1)->start == 0x1000);
static_assert(cache::framebuffer_page_extent(0x1003, 1)->end == 0x2000);
static_assert(cache::framebuffer_page_extent(0x1fff, 2)->end == 0x3000);
static_assert(cache::framebuffer_page_extent(UINT64_MAX - 4096, 1)->end == UINT64_MAX - 4095);
static_assert(!cache::framebuffer_page_extent(UINT64_MAX - 4095, 1));
static_assert(!cache::framebuffer_page_extent(UINT64_MAX, 1));
static_assert(!cache::framebuffer_page_extent(0x1000, 0));

auto raw(paging::PageTableEntry entry) -> uint64_t { return std::bit_cast<uint64_t>(entry); }

TEST(CachePte, FramebufferExtentPreservesAlignedEndpoints) {
    for (auto const input : std::array<cache::PageExtent, 3>{{{0, 0x1000}, {0x1000, 0x3000}, {PHYSICAL, PHYSICAL + 0x2000}}}) {
        auto const extent = cache::framebuffer_page_extent(input.start, input.end - input.start);
        ASSERT_TRUE(extent.has_value());
        EXPECT_EQ(extent->start, input.start);
        EXPECT_EQ(extent->end, input.end);
    }
}

TEST(CachePte, FramebufferExtentIncludesSubpageAndPartialTailBytes) {
    struct Case {
        uint64_t base, length, start, end;
    };
    constexpr std::array<Case, 6> CASES{{{0, 1, 0, 0x1000},
                                         {0x1003, 1, 0x1000, 0x2000},
                                         {0x1000, 0x1001, 0x1000, 0x3000},
                                         {0x1001, 0x1fff, 0x1000, 0x3000},
                                         {0x1fff, 1, 0x1000, 0x2000},
                                         {0x1fff, 2, 0x1000, 0x3000}}};
    for (auto const input : CASES) {
        auto const extent = cache::framebuffer_page_extent(input.base, input.length);
        ASSERT_TRUE(extent.has_value());
        EXPECT_EQ(extent->start, input.start);
        EXPECT_EQ(extent->end, input.end);
    }
}

TEST(CachePte, FramebufferExtentRejectsEmptyAdditionOverflowAndRoundingOverflow) {
    for (uint64_t base : std::array<uint64_t, 4>{0, 1, PHYSICAL, UINT64_MAX}) EXPECT_FALSE(cache::framebuffer_page_extent(base, 0));
    EXPECT_FALSE(cache::framebuffer_page_extent(UINT64_MAX, 1));
    EXPECT_FALSE(cache::framebuffer_page_extent(UINT64_MAX - 1, 2));
    EXPECT_FALSE(cache::framebuffer_page_extent(PHYSICAL, UINT64_MAX));
    // These byte endpoints fit, but their covering page endpoint would wrap.
    EXPECT_FALSE(cache::framebuffer_page_extent(UINT64_MAX - 4095, 1));
    EXPECT_FALSE(cache::framebuffer_page_extent(UINT64_MAX - 1, 1));
    EXPECT_FALSE(cache::framebuffer_page_extent(0, UINT64_MAX));
}

TEST(CachePte, FramebufferExtentAcceptsTheLastRepresentableRoundedPage) {
    constexpr uint64_t LAST_END = UINT64_MAX - 4095;
    auto const whole = cache::framebuffer_page_extent(LAST_END - 4096, 4096);
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(whole->start, LAST_END - 4096);
    EXPECT_EQ(whole->end, LAST_END);
    auto const tail = cache::framebuffer_page_extent(LAST_END - 1, 1);
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->start, whole->start);
    EXPECT_EQ(tail->end, whole->end);
    auto const large = cache::framebuffer_page_extent(0, LAST_END);
    ASSERT_TRUE(large.has_value());
    EXPECT_EQ(large->start, 0U);
    EXPECT_EQ(large->end, LAST_END);
}

TEST(CachePte, AllEightSelectorsUseOnlyTheThreeLeafCacheBits) {
    for (uint8_t selector = 0; selector < CACHE_FLAGS.size(); ++selector) {
        auto const flags = cache::selector_flags_4k(selector);
        ASSERT_TRUE(flags.has_value());
        EXPECT_EQ(*flags, CACHE_FLAGS.at(selector));
        EXPECT_EQ(cache::leaf_selector(PHYSICAL | paging::PAGE_PRESENT | *flags, 1), selector);
    }
}

TEST(CachePte, OutOfRangeSelectorsRefuseInsteadOfTruncating) {
    for (unsigned selector = 8; selector <= std::numeric_limits<uint8_t>::max(); ++selector)
        EXPECT_FALSE(cache::selector_flags_4k(static_cast<uint8_t>(selector)));
}

TEST(CachePte, RequiresPresentAndAnActualLeafLevel) {
    for (unsigned level : {0U, 1U, 2U, 3U, 4U, std::numeric_limits<unsigned>::max()}) {
        EXPECT_FALSE(cache::leaf_selector(0, level));
        EXPECT_FALSE(cache::leaf_selector(UINT64_MAX & ~paging::PAGE_PRESENT, level));
    }
    for (unsigned level : {0U, 4U, std::numeric_limits<unsigned>::max()}) EXPECT_FALSE(cache::leaf_selector(UINT64_MAX, level));
    EXPECT_FALSE(cache::leaf_selector(paging::PAGE_PRESENT | PHYSICAL, 2));
    EXPECT_FALSE(cache::leaf_selector(paging::PAGE_PRESENT | PHYSICAL, 3));
}

TEST(CachePte, LargeLeafPatUsesBitTwelveAndConvertsToBitSeven) {
    for (unsigned level : {2U, 3U}) {
        for (uint8_t selector = 0; selector < CACHE_FLAGS.size(); ++selector) {
            uint64_t const large_flags = (CACHE_FLAGS.at(selector) & 0x18) | 0x80 | (selector >= 4 ? 0x1000 : 0);
            uint64_t const frame = level == 2 ? 0x0001234567800000ULL : 0x0001234540000000ULL;
            auto const decoded = cache::leaf_selector(frame | large_flags | paging::PAGE_PRESENT, level);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(*decoded, selector);
            auto const converted = cache::selector_flags_4k(*decoded);
            ASSERT_TRUE(converted.has_value());
            EXPECT_EQ(*converted, CACHE_FLAGS.at(selector));
            EXPECT_EQ(*converted & 0x1000, 0U);
        }
    }
    // Large PS bit 7 is required even for selector 0; it is not PAT there.
    EXPECT_EQ(cache::leaf_selector(0x81, 2), 0);
    EXPECT_EQ(cache::leaf_selector(0x81, 3), 0);
    EXPECT_EQ(cache::leaf_selector(0x81, 1), 4);
}

TEST(CachePte, FourKilobyteFrameBitTwelveNeverChangesTheSelector) {
    for (uint64_t frame : std::array<uint64_t, 5>{0, 0x1000, 0x1ff000, 0x3ffff000, FRAME_MASK}) {
        EXPECT_EQ(cache::leaf_selector(frame | 1, 1), 0);
        EXPECT_EQ(cache::leaf_selector(frame | 0x89, 1), 5);
    }
    // Permission, A/D, software and high fields are unrelated to selection.
    EXPECT_EQ(cache::leaf_selector(UINT64_MAX & ~uint64_t{0x98}, 1), 0);
    EXPECT_EQ(cache::leaf_selector(UINT64_MAX, 1), 7);
}

TEST(CachePte, ActualLeafConstructorPreservesEveryCacheSelectorAndFrame) {
    constexpr uint64_t POLICY = paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_NX | paging::PAGE_SHARED;
    for (uint8_t selector = 0; selector < CACHE_FLAGS.size(); ++selector) {
        auto const entry = paging::create_page_table_leaf_4k(PHYSICAL, POLICY | CACHE_FLAGS.at(selector));
        EXPECT_EQ(raw(entry), PHYSICAL | POLICY | CACHE_FLAGS.at(selector));
        EXPECT_EQ(raw(entry) & FRAME_MASK, PHYSICAL);
        EXPECT_EQ(cache::leaf_selector(raw(entry), 1), selector);
    }
    auto const index5 = paging::create_page_table_leaf_4k(PHYSICAL, POLICY | 0x88);
    EXPECT_EQ(raw(index5) & 0x98, 0x88U);  // Logical index 5, independent of PAT contents.
}

TEST(CachePte, LegacyConstructorNeverCopiesPatOrLargePageRole) {
    constexpr uint64_t POLICY = paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_NX;
    for (uint64_t flags : CACHE_FLAGS) {
        auto const entry = paging::create_page_table_entry(PHYSICAL, POLICY | flags);
        EXPECT_EQ(static_cast<unsigned>(entry.pagesize), 0U);
        EXPECT_EQ(raw(entry), PHYSICAL | POLICY | (flags & 0x18));
    }
}

TEST(CachePte, ActualConstructorsClearUnsupportedFieldsRatherThanCopyingRawFlags) {
    auto const legacy = paging::create_page_table_entry(PHYSICAL, UINT64_MAX);
    auto const leaf = paging::create_page_table_leaf_4k(PHYSICAL, UINT64_MAX);
    EXPECT_EQ(static_cast<uint64_t>(legacy.reserved), 0U);
    EXPECT_EQ(static_cast<uint64_t>(leaf.reserved), 0U);
    EXPECT_EQ(static_cast<unsigned>(legacy.accessed), 0U);
    EXPECT_EQ(static_cast<unsigned>(leaf.dirty), 0U);
    EXPECT_EQ(static_cast<unsigned>(leaf.global), 0U);
    EXPECT_EQ(raw(legacy) & FRAME_MASK, PHYSICAL);
    EXPECT_EQ(raw(leaf) & FRAME_MASK, PHYSICAL);
    EXPECT_EQ(raw(leaf) ^ raw(legacy), paging::PAGE_PAT_4K);
}

TEST(CachePte, LeafPreservesLegacyPermissionsAndCowEncoding) {
    for (uint64_t permission = 0; permission < 8; ++permission) {
        for (uint64_t policy : std::array<uint64_t, 5>{0, paging::PAGE_NX, paging::PAGE_COW, paging::PAGE_SHARED,
                                                       paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED}) {
            uint64_t const flags = permission | policy | 0x88;
            auto const legacy = paging::create_page_table_entry(PHYSICAL, flags);
            auto const leaf = paging::create_page_table_leaf_4k(PHYSICAL, flags);
            EXPECT_EQ(raw(leaf) & ~paging::PAGE_PAT_4K, raw(legacy));
        }
    }
    auto const cow = paging::create_page_table_leaf_4k(PHYSICAL, paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_COW | 0x88);
    EXPECT_EQ(static_cast<unsigned>(cow.writable), 0U);
    EXPECT_NE(raw(cow) & paging::PAGE_COW, 0U);
    auto const restricted = paging::create_page_table_leaf_4k(PHYSICAL, paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE |
                                                                            paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED | 0x88);
    EXPECT_EQ(static_cast<unsigned>(restricted.writable), 0U);
    EXPECT_NE(raw(restricted) & paging::PAGE_WRITE_RESTRICTED, 0U);
}

TEST(CachePte, AncestorsStayPresentAndWritableIncludingReservedLeaves) {
    for (uint64_t permissions = 0; permissions < 8; ++permissions) {
        uint64_t const hostile_leaf_flags = permissions | (UINT64_MAX & ~uint64_t{7});
        auto const flags = paging::table_flags_for_leaf(hostile_leaf_flags);
        uint64_t const expected = paging::PAGE_PRESENT | paging::PAGE_WRITE | (permissions & paging::PAGE_USER);
        EXPECT_EQ(flags, expected);
        auto const entry = paging::create_page_table_entry(PHYSICAL, flags);
        EXPECT_EQ(raw(entry), PHYSICAL | expected);
    }
    EXPECT_EQ(paging::table_flags_for_leaf(0), paging::PAGE_PRESENT | paging::PAGE_WRITE);
    EXPECT_EQ(paging::table_flags_for_leaf(paging::PAGE_RESERVED), paging::PAGE_PRESENT | paging::PAGE_WRITE);
    EXPECT_EQ(paging::table_flags_for_leaf(paging::PAGE_RESERVED | paging::PAGE_USER),
              paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER);
}

TEST(CachePte, PurgeClearsTheCompleteEntry) { EXPECT_EQ(raw(paging::purge_page_table_entry()), 0U); }

}  // namespace
