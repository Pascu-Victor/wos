#include <gtest/gtest.h>

#include <dev/iommu/scopes.hpp>
#include <vector>

namespace scopes = ker::dev::iommu::scopes;
namespace {
using Bytes = std::vector<uint8_t>;
void put(Bytes& bytes, size_t offset, uint64_t value, size_t count) {
    for (size_t i = 0; i < count; ++i) bytes.at(offset + i) = static_cast<uint8_t>(value >> (i * 8));
}
auto scope(uint8_t type, uint8_t bus, std::initializer_list<uint8_t> path) -> Bytes {
    Bytes bytes{type, static_cast<uint8_t>(6 + path.size()), 0, 0, 0, bus};
    bytes.insert(bytes.end(), path);
    return bytes;
}
struct Table {
    Bytes bytes = Bytes(48);
    size_t units = 0;
    Table() {
        bytes[0] = 'D';
        bytes[1] = 'M';
        bytes[2] = 'A';
        bytes[3] = 'R';
        bytes[8] = 1;
        bytes[36] = 47;
    }
    void unit(bool all, Bytes const& scopes = {}, uint16_t segment = 0) {
        size_t const START = bytes.size();
        bytes.resize(START + 16);
        put(bytes, START + 2, 16 + scopes.size(), 2);
        bytes[START + 4] = all;
        put(bytes, START + 6, segment, 2);
        put(bytes, START + 8, 0xfed90000 + 4096 * units++, 8);
        bytes.insert(bytes.end(), scopes.begin(), scopes.end());
    }
    void region(Bytes const& scopes, uint64_t base = 0x100000, uint64_t end = 0x100fff) {
        size_t const START = bytes.size();
        bytes.resize(START + 24);
        put(bytes, START, 1, 2);
        put(bytes, START + 2, 24 + scopes.size(), 2);
        put(bytes, START + 8, base, 8);
        put(bytes, START + 16, end, 8);
        bytes.insert(bytes.end(), scopes.begin(), scopes.end());
    }
    auto qualify(std::span<const scopes::Device> devices, bool complete = true) -> scopes::Result {
        put(bytes, 4, bytes.size(), 4);
        bytes[9] = 0;
        uint8_t sum = 0;
        for (uint8_t byte : bytes) sum = static_cast<uint8_t>(sum + byte);
        bytes[9] = static_cast<uint8_t>(0U - sum);
        return scopes::qualify(bytes, devices, complete);
    }
};
auto topology() -> std::array<scopes::Device, 5> {
    return {{{.slot = 1},
             {.slot = 2, .header = 1, .secondary = 3, .subordinate = 7},
             {.bus = 3, .slot = 4, .header = 1, .parent = 1, .secondary = 7, .subordinate = 7},
             {.bus = 7, .slot = 5, .parent = 2},
             {.slot = 6}}};
}
void expect_failure(scopes::Result const& result, scopes::Error error) {
    EXPECT_EQ(result.error, error);
    EXPECT_EQ(result.device_count, 0U);
    EXPECT_EQ(result.region_count, 0U);
    for (auto const& binding : result.devices) {
        EXPECT_EQ(binding.unit, scopes::NONE);
        EXPECT_EQ(binding.reserved, 0U);
    }
}
}  // namespace

TEST(IommuScopes, ExplicitSubtreeIncludesBridgeAndOverridesSegmentDefault) {
    Table table;
    table.unit(false, scope(2, 0, {2, 0}));
    table.unit(true);
    auto const RESULT = table.qualify(topology());
    ASSERT_TRUE(RESULT.valid());
    ASSERT_EQ(RESULT.device_count, 5U);
    EXPECT_EQ(RESULT.devices[0].unit, 1);
    EXPECT_EQ(RESULT.devices[4].unit, 1);
    for (size_t i = 1; i <= 3; ++i) EXPECT_EQ(RESULT.devices[i].unit, 0);
}

TEST(IommuScopes, ResolvesMultiHopEndpointAndCollectsOnlyItsReservedRanges) {
    Table table;
    auto const PATH = scope(1, 0, {2, 0, 4, 0, 5, 0});
    table.unit(false, PATH);
    table.unit(true);
    table.region(PATH);
    table.region(scope(2, 0, {2, 0}), 0x200000, 0x201fff);
    auto const RESULT = table.qualify(topology());
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.devices[3].unit, 0);
    EXPECT_EQ(RESULT.devices[3].reserved, 3U);
    EXPECT_EQ(RESULT.devices[2].reserved, 2U);
    EXPECT_EQ(RESULT.devices[1].reserved, 2U);
    EXPECT_EQ(RESULT.devices[0].reserved, 0U);
    EXPECT_EQ(RESULT.devices[4].reserved, 0U);
    EXPECT_EQ(RESULT.region_count, 2U);
    EXPECT_EQ(RESULT.regions[1].end, 0x201fffU);
}

TEST(IommuScopes, RejectsOverlappingOwnershipWithoutPublishingPartialBindings) {
    Table table;
    table.unit(false, scope(2, 0, {2, 0}));
    table.unit(false, scope(1, 7, {5, 0}));
    table.unit(true);
    expect_failure(table.qualify(topology()), scopes::Error::ambiguous);
    Table duplicate;
    auto paths = scope(1, 0, {1, 0});
    auto const AGAIN = paths;
    paths.insert(paths.end(), AGAIN.begin(), AGAIN.end());
    duplicate.unit(false, paths);
    duplicate.unit(true);
    expect_failure(duplicate.qualify(topology()), scopes::Error::ambiguous);
}

TEST(IommuScopes, DistinguishesAbsentSlotsFromInvalidPathsAndTypes) {
    Table absent;
    absent.unit(false, scope(1, 0, {31, 0}));
    absent.unit(true);
    auto const RESULT = absent.qualify(topology());
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.absent_scopes, 1U);
    for (auto const& bad : {scope(1, 0, {1, 0, 5, 0}), scope(2, 0, {1, 0}), scope(1, 0, {2, 0})}) {
        Table table;
        table.unit(false, bad);
        table.unit(true);
        expect_failure(table.qualify(topology()), scopes::Error::path);
    }
}

TEST(IommuScopes, RejectsUnownedIncompleteAndUnsupportedInventories) {
    Table table;
    table.unit(false, scope(1, 0, {1, 0}));
    expect_failure(table.qualify(topology()), scopes::Error::unowned);
    expect_failure(table.qualify(topology(), false), scopes::Error::incomplete);
    Table other_segment;
    other_segment.unit(true, {}, 1);
    expect_failure(other_segment.qualify(topology()), scopes::Error::unsupported);
    Table unknown;
    unknown.unit(true);
    unknown.bytes.insert(unknown.bytes.end(), {9, 0, 4, 0});
    expect_failure(unknown.qualify(topology()), scopes::Error::unsupported);
    std::array<scopes::Device, 65> too_many{};
    expect_failure(table.qualify(too_many), scopes::Error::limit);
}

TEST(IommuScopes, RejectsCyclesDuplicateBdfsAndConflictingBridgeRanges) {
    Table table;
    table.unit(true);
    auto nodes = topology();
    nodes[1].parent = 2;
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[4] = nodes[0];
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[3].parent = 1;
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[2].subordinate = 8;
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[4] = {.slot = 6, .header = 1, .secondary = 4, .subordinate = 5};
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[4] = {.bus = 5, .slot = 6};
    expect_failure(table.qualify(nodes), scopes::Error::topology);
    nodes = topology();
    nodes[0].slot = 32;
    expect_failure(table.qualify(nodes), scopes::Error::topology);
}

TEST(IommuScopes, BoundsReservedRegionMaskAndRejectsMalformedTable) {
    Table table;
    table.unit(true);
    for (size_t i = 0; i < 64; ++i) table.region(scope(1, 0, {1, 0}));
    auto const RESULT = table.qualify(topology());
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.devices[0].reserved, UINT64_MAX);
    table.region(scope(1, 0, {1, 0}));
    expect_failure(table.qualify(topology()), scopes::Error::limit);
    table.bytes[0] = 'X';
    expect_failure(table.qualify(topology()), scopes::Error::table);
}

TEST(IommuScopes, ResolvesMaximumDepthAndDoesNotConfuseSiblingFunctions) {
    std::array<scopes::Device, scopes::MAX_DEVICES> nodes{};
    Bytes path{1, static_cast<uint8_t>(6 + 2 * nodes.size()), 0, 0, 0, 0};
    for (size_t i = 0; i < nodes.size(); ++i) {
        bool const BRIDGE = i + 1 < nodes.size();
        nodes[i] = {.bus = static_cast<uint8_t>(i),
                    .slot = 1,
                    .header = static_cast<uint8_t>(BRIDGE),
                    .parent = i == 0 ? scopes::NONE : static_cast<uint16_t>(i - 1),
                    .secondary = BRIDGE ? static_cast<uint8_t>(i + 1) : uint8_t{0},
                    .subordinate = BRIDGE ? uint8_t{63} : uint8_t{0}};
        path.insert(path.end(), {1, 0});
    }
    Table deep;
    deep.unit(false, path);
    deep.unit(true);
    auto const RESULT = deep.qualify(nodes);
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.devices[63].unit, 0);
    EXPECT_EQ(RESULT.devices[62].unit, 1);

    std::array<scopes::Device, 2> functions{{{.slot = 1}, {.slot = 1, .function = 1}}};
    Table sibling;
    sibling.unit(false, scope(1, 0, {1, 1}));
    sibling.unit(true);
    auto const SIBLINGS = sibling.qualify(functions);
    ASSERT_TRUE(SIBLINGS.valid());
    EXPECT_EQ(SIBLINGS.devices[0].unit, 1);
    EXPECT_EQ(SIBLINGS.devices[1].unit, 0);
}
