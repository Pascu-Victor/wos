#include <gtest/gtest.h>

#include <array>
#include <dev/iommu/reserved.hpp>
#include <vector>

namespace {
namespace r = ker::dev::iommu::reserved;
using r::Kind;
using r::Memory;
TEST(ReservedOwnership, FullyReservedAndNvsRegionsAreAcceptedWithoutMutation) {
    std::array<Memory, 3> memory{
        {{0x1000, 0x1000, Kind::firmware_reserved}, {0x2000, 0x1000, Kind::acpi_nvs}, {0x3000, 0x1000, Kind::protected_memory}}};
    auto result = r::qualify(0x1000, 0x2fff, 48, memory, {});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.bytes, 8192);
    EXPECT_EQ(memory[0].base, 0x1000);
    EXPECT_EQ(memory[1].bytes, 0x1000);
    EXPECT_EQ(memory[2].kind, Kind::protected_memory);
}
TEST(ReservedOwnership, UnsortedOverlappingCoverageCanSpanDescriptorBoundaries) {
    std::array<Memory, 4> memory{{{0x2800, 0x1800, Kind::acpi_nvs},
                                  {0x1800, 0x1400, Kind::firmware_reserved},
                                  {0x1000, 0x1000, Kind::firmware_reserved},
                                  {0, 0, Kind::protected_memory}}};
    auto result = r::qualify(0x1000, 0x3fff, 48, memory, {});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.bytes, 3 * 4096);
}
TEST(ReservedOwnership, OneByteHoleAndUnlistedSpaceAreRefused) {
    std::array<Memory, 2> memory{{{0x1000, 0x1000, Kind::firmware_reserved}, {0x2001, 0xfff, Kind::firmware_reserved}}};
    EXPECT_EQ(r::qualify(0x1000, 0x2fff, 48, memory, {}).error, r::Error::gap);
    EXPECT_EQ(r::qualify(0x4000, 0x4fff, 48, memory, {}).error, r::Error::gap);
}
TEST(ReservedOwnership, ConflictingProtectedDescriptorWinsEvenWithReservedCoverage) {
    std::array<Memory, 2> memory{{{0x1000, 0x2000, Kind::firmware_reserved}, {0x1fff, 1, Kind::protected_memory}}};
    auto result = r::qualify(0x1000, 0x2fff, 48, memory, {});
    EXPECT_EQ(result.error, r::Error::protected_memory);
    EXPECT_EQ(result.bytes, 0);
    memory[1].kind = static_cast<Kind>(255);
    EXPECT_EQ(r::qualify(0x1000, 0x2fff, 48, memory, {}).error, r::Error::protected_memory);
}
TEST(ReservedOwnership, MmioConflictsUseExactHalfOpenOverlapAndCheckOverflow) {
    std::array<Memory, 1> memory{{{0x1000, 0x2000, Kind::firmware_reserved}}};
    for (auto conflict : {r::Exclusion{0xfff, 2}, r::Exclusion{0x1000, 1}, r::Exclusion{0x2fff, 1}}) {
        std::array exclusions{conflict};
        EXPECT_EQ(r::qualify(0x1000, 0x2fff, 48, memory, exclusions).error, r::Error::mmio);
    }
    std::array<r::Exclusion, 2> outside{{{0, 0x1000}, {0x3000, 4096}}};
    EXPECT_TRUE(r::qualify(0x1000, 0x2fff, 48, memory, outside).valid());
    outside[0] = {UINT64_MAX, 2};
    EXPECT_EQ(r::qualify(0x1000, 0x2fff, 48, memory, outside).error, r::Error::memory_map);
}
TEST(ReservedOwnership, InvalidRangesWidthsAndDescriptorOverflowFailClosed) {
    std::array<Memory, 1> memory{{{0x1000, 0x2000, Kind::firmware_reserved}}};
    for (auto pair : {std::array<uint64_t, 2>{0, 0xfff}, {0x1001, 0x1fff}, {0x1000, 0x2000}, {0x3000, 0x2fff}, {0x1000, UINT64_MAX}})
        EXPECT_EQ(r::qualify(pair[0], pair[1], 48, memory, {}).error, r::Error::range);
    for (uint8_t width : {0, 11, 53, 255}) EXPECT_EQ(r::qualify(0x1000, 0x1fff, width, memory, {}).error, r::Error::width);
    EXPECT_EQ(r::qualify(0x1000, 0x1fff, 12, memory, {}).error, r::Error::width);
    memory[0] = {UINT64_MAX - 1, 3, Kind::firmware_reserved};
    EXPECT_EQ(r::qualify(0x1000, 0x1fff, 48, memory, {}).error, r::Error::memory_map);
}
TEST(ReservedOwnership, LargeRangeValidationDoesNotWalkEveryPage) {
    uint64_t const END = uint64_t{1} << 48;
    std::array<Memory, 1> memory{{{4096, END - 4096, Kind::firmware_reserved}}};
    auto result = r::qualify(4096, END - 1, 48, memory, {});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.bytes, END - 4096);
}
TEST(ReservedOwnership, InputLimitsAndWorstOrderCoverageAreBounded) {
    std::vector<Memory> memory;
    for (size_t i = r::MAX_MEMORY_REGIONS; i > 0; --i) memory.push_back({i * 4096, 4096, Kind::firmware_reserved});
    EXPECT_TRUE(r::qualify(4096, (memory.size() + 1) * 4096 - 1, 48, memory, {}).valid());
    memory.push_back({});
    EXPECT_EQ(r::qualify(4096, 8191, 48, memory, {}).error, r::Error::limit);
    EXPECT_EQ(r::qualify(4096, 8191, 48, {}, {}).error, r::Error::limit);
    memory.resize(1);
    std::vector<r::Exclusion> exclusions(r::MAX_EXCLUSIONS + 1);
    EXPECT_EQ(r::qualify(4096, 8191, 48, memory, exclusions).error, r::Error::limit);
}
struct PlanFixture {
    ker::dev::iommu::scopes::Result ownership{.device_count = 3, .region_count = 3};
    ker::dev::iommu::groups::Result groups{.error = ker::dev::iommu::groups::Error::none, .devices = 3, .count = 2};
    std::array<uint8_t, 1> widths{48};
    std::array<Memory, 1> memory{{{0x1000, 0x10000, Kind::firmware_reserved}}};
    std::array<r::Mapping, 8> output{};
    PlanFixture() {
        groups.owners[0] = 0;
        groups.owners[1] = 0;
        groups.owners[2] = 2;
        for (auto& d : ownership.devices) d.unit = 0;
        ownership.regions[0] = {0x1000, 0x2fff};
        ownership.regions[1] = {0x2000, 0x3fff};
        ownership.regions[2] = {0x5000, 0x5fff};
        ownership.devices[0].reserved = 1;
        ownership.devices[1].reserved = 2;
        ownership.devices[2].reserved = 5;
    }
    auto run() -> r::Plan { return r::plan(ownership, groups, widths, memory, {}, output); }
};
TEST(ReservedPlan, SharedGroupUnionsScopesAndNormalizesOverlapsPerRoot) {
    PlanFixture f;
    auto result = f.run();
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 3);
    EXPECT_EQ(result.pages, 6);
    EXPECT_EQ(f.output[0].group, 0);
    EXPECT_EQ(f.output[0].base, 0x1000);
    EXPECT_EQ(f.output[0].bytes, 0x3000);
    EXPECT_EQ(f.output[1].group, 2);
    EXPECT_EQ(f.output[1].base, 0x1000);
    EXPECT_EQ(f.output[1].bytes, 0x2000);
    EXPECT_EQ(f.output[2].group, 2);
    EXPECT_EQ(f.output[2].base, 0x5000);
    EXPECT_EQ(f.output[2].bytes, 0x1000);
    EXPECT_TRUE(r::overlaps(std::span{f.output}.first(result.count), 0, 0x3fff, 1));
    EXPECT_FALSE(r::overlaps(std::span{f.output}.first(result.count), 0, 0x4000, 1));
    EXPECT_TRUE(r::overlaps(std::span{f.output}.first(result.count), 2, 0x5000, 1));
    EXPECT_FALSE(r::overlaps(std::span{f.output}.first(result.count), 0, 0x5000, 1));
    EXPECT_TRUE(r::overlaps_any(std::span{f.output}.first(result.count), 0x5000, 1));
    EXPECT_TRUE(r::overlaps({}, 0, UINT64_MAX, 2));
}
TEST(ReservedPlan, AdjacentRangesMergeAndUnreferencedRegionsDoNotMap) {
    PlanFixture f;
    f.ownership.regions[1] = {0x3000, 0x3fff};
    f.ownership.devices[2].reserved = 0;
    f.ownership.regions[2] = {0x100000, 0x100fff};
    auto result = f.run();
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 1);
    EXPECT_EQ(result.pages, 3);
}
TEST(ReservedPlan, UnsafeLaterRegionClearsAllPartialPublication) {
    PlanFixture f;
    f.ownership.regions[2] = {0x100000, 0x100fff};
    auto result = f.run();
    EXPECT_EQ(result.error, r::Error::gap);
    EXPECT_EQ(result.count, 0);
    EXPECT_EQ(result.pages, 0);
    for (auto const& m : f.output) {
        EXPECT_EQ(m.bytes, 0);
        EXPECT_EQ(m.group, ker::dev::iommu::groups::NONE);
    }
}
TEST(ReservedPlan, CapacityAndPerGroupBudgetAreExplicitFailures) {
    PlanFixture f;
    EXPECT_EQ(r::plan(f.ownership, f.groups, f.widths, f.memory, {}, std::span{f.output}.first(1)).error, r::Error::limit);
    f.memory[0] = {0x1000, r::MAX_GROUP_BYTES + 4096, Kind::firmware_reserved};
    f.ownership.regions[0] = {0x1000, r::MAX_GROUP_BYTES + 0x1fff};
    EXPECT_EQ(f.run().error, r::Error::limit);
}
TEST(ReservedPlan, CorruptGroupScopeAndUnitReferencesFailClosed) {
    for (unsigned mode = 0; mode < 5; ++mode) {
        PlanFixture f;
        if (mode == 0) f.groups.owners[1] = 2;
        if (mode == 1) f.ownership.devices[1].unit = 1;
        if (mode == 2) f.ownership.devices[0].reserved |= uint64_t{1} << 63U;
        if (mode == 3) f.ownership.region_count = 65;
        if (mode == 4) f.groups.devices = 64;
        EXPECT_EQ(f.run().error, r::Error::ownership);
    }
    PlanFixture f;
    f.widths[0] = 13;
    EXPECT_EQ(f.run().error, r::Error::width);
}
}  // namespace
