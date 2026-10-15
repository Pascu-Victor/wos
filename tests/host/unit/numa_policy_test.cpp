#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <platform/mm/numa_policy.hpp>

namespace numa = ker::mod::mm::numa;
namespace topology = ker::mod::acpi::numa;
namespace {

constexpr auto bit(numa::NodeId node) -> numa::NodeMask { return numa::NodeMask{1} << node; }

class NumaPolicy : public testing::Test {
   protected:
    topology::Topology machine{};

    NumaPolicy() {
        machine.node_count = 6;
        for (numa::NodeId node = 0; node < machine.node_count; ++node) {
            machine.nodes[node].proximity_id = 100 + node * 10;
            machine.nodes[node].usable_bytes = 4096;
            for (numa::NodeId other = 0; other < machine.node_count; ++other) machine.distances[node][other] = node == other ? 10 : 20;
        }
        // Local node2 sees asymmetric distances, a tie and an unreachable255.
        machine.distances[2] = {30, 20, 10, 20, 255, 11};
        machine.distances[4] = {11, 12, 13, 14, 10, 15};
    }

    template <size_t N>
    void expect_order(const numa::Selection& selected, const std::array<numa::NodeId, N>& expected) {
        ASSERT_TRUE(selected.valid());
        EXPECT_EQ(selected.error, numa::SelectionError::NONE);
        ASSERT_EQ(selected.count, N);
        ASSERT_NE(N, 0U);
        EXPECT_EQ(selected.requested, expected[0]);
        for (size_t i = 0; i < N; ++i) EXPECT_EQ(selected.nodes[i], expected[i]) << "at index " << i;
    }
};

TEST(NumaPolicyValidation, ValidatesAllFourModesWithoutTruncatingHighNodeBits) {
    for (size_t count : {1U, 2U, 63U, 64U}) {
        EXPECT_TRUE(numa::valid_policy({0, numa::Mode::LOCAL}, count));
        EXPECT_FALSE(numa::valid_policy({1, numa::Mode::LOCAL}, count));
        EXPECT_TRUE(numa::valid_policy({1, numa::Mode::PREFERRED}, count));
        EXPECT_FALSE(numa::valid_policy({0, numa::Mode::PREFERRED}, count));
        EXPECT_FALSE(numa::valid_policy({0, numa::Mode::BIND}, count));
        EXPECT_FALSE(numa::valid_policy({0, numa::Mode::INTERLEAVE}, count));
        auto const LAST = bit(static_cast<numa::NodeId>(count - 1));
        EXPECT_TRUE(numa::valid_policy({LAST, numa::Mode::PREFERRED}, count));
        EXPECT_TRUE(numa::valid_policy({LAST, numa::Mode::BIND}, count));
        EXPECT_TRUE(numa::valid_policy({LAST, numa::Mode::INTERLEAVE}, count));
    }
    EXPECT_FALSE(numa::valid_policy({3, numa::Mode::PREFERRED}, 2));
    EXPECT_FALSE(numa::valid_policy({bit(63), numa::Mode::PREFERRED}, 63));
    EXPECT_FALSE(numa::valid_policy({2, numa::Mode::BIND}, 1));
    EXPECT_FALSE(numa::valid_policy({UINT64_MAX, numa::Mode::INTERLEAVE}, 63));
    EXPECT_TRUE(numa::valid_policy({UINT64_MAX, numa::Mode::BIND}, 64));
    EXPECT_TRUE(numa::valid_policy({UINT64_MAX, numa::Mode::INTERLEAVE}, 64));
}

TEST(NumaPolicyValidation, RejectsInvalidNodeCountsAndUnknownModesBeforeAnyShift) {
    for (auto mode : {numa::Mode::LOCAL, numa::Mode::PREFERRED, numa::Mode::BIND, numa::Mode::INTERLEAVE}) {
        numa::Policy const POLICY{mode == numa::Mode::LOCAL ? 0U : 1U, mode};
        for (size_t count : {size_t{0}, size_t{65}, SIZE_MAX}) EXPECT_FALSE(numa::valid_policy(POLICY, count));
    }
    for (uint8_t mode : {4, 127, 255}) {
        EXPECT_FALSE(numa::valid_policy({0, static_cast<numa::Mode>(mode)}, 1));
        EXPECT_FALSE(numa::valid_policy({1, static_cast<numa::Mode>(mode)}, 64));
    }
}

TEST_F(NumaPolicy, ReportsInvalidPolicyAndInvalidLocalNodeWithoutAPlan) {
    auto const BAD_POLICY = numa::select_nodes({bit(6), numa::Mode::BIND}, machine, 2, 0);
    EXPECT_FALSE(BAD_POLICY.valid());
    EXPECT_EQ(BAD_POLICY.error, numa::SelectionError::INVALID_POLICY);
    EXPECT_EQ(BAD_POLICY.count, 0);
    EXPECT_EQ(BAD_POLICY.requested, numa::INVALID_NODE);
    for (auto mode : {numa::Mode::LOCAL, numa::Mode::PREFERRED, numa::Mode::BIND, numa::Mode::INTERLEAVE}) {
        numa::Policy const POLICY{mode == numa::Mode::LOCAL ? 0U : 1U, mode};
        for (auto local : {machine.node_count, numa::INVALID_NODE}) {
            auto const SELECTED = numa::select_nodes(POLICY, machine, local, UINT64_MAX);
            EXPECT_FALSE(SELECTED.valid());
            EXPECT_EQ(SELECTED.error, numa::SelectionError::INVALID_LOCAL_NODE);
            EXPECT_EQ(SELECTED.count, 0);
            EXPECT_EQ(SELECTED.requested, numa::INVALID_NODE);
        }
    }
    for (uint16_t count : {0, 65}) {
        machine.node_count = count;
        EXPECT_EQ(numa::select_nodes({}, machine, 0, 0).error, numa::SelectionError::INVALID_POLICY);
    }
}

TEST_F(NumaPolicy, LocalStartsAtTouchingCpuNodeThenUsesOriginDistancesAndDenseIdTies) {
    expect_order(numa::select_nodes({}, machine, 2, 0), std::array<numa::NodeId, 6>{2, 5, 1, 3, 0, 4});
    expect_order(numa::select_nodes({}, machine, 4, UINT64_MAX), std::array<numa::NodeId, 6>{4, 0, 1, 2, 3, 5});
}

TEST_F(NumaPolicy, PreferredKeepsMemorylessRequestedNodeVisibleThenUsesTouchingOrigin) {
    machine.nodes[4].usable_bytes = 0;
    expect_order(numa::select_nodes({bit(4), numa::Mode::PREFERRED}, machine, 2, 0), std::array<numa::NodeId, 6>{4, 2, 5, 1, 3, 0});
    // LOCAL also preserves the real local node when it has no usable RAM.
    machine.nodes[2].usable_bytes = 0;
    expect_order(numa::select_nodes({}, machine, 2, 0), std::array<numa::NodeId, 6>{2, 5, 1, 3, 0, 4});
}

TEST_F(NumaPolicy, BindSelectsOnlyItsStrictSubsetInDistanceOrder) {
    constexpr auto MASK = bit(0) | bit(3) | bit(4) | bit(5);
    auto const SELECTED = numa::select_nodes({MASK, numa::Mode::BIND}, machine, 2, 0);
    expect_order(SELECTED, std::array<numa::NodeId, 4>{5, 3, 0, 4});
    for (size_t i = 0; i < SELECTED.count; ++i) EXPECT_NE(bit(SELECTED.nodes[i]) & MASK, 0U);
    expect_order(numa::select_nodes({bit(1) | bit(2) | bit(3), numa::Mode::BIND}, machine, 2, 0), std::array<numa::NodeId, 3>{2, 1, 3});
    machine.nodes[4].usable_bytes = 0;
    expect_order(numa::select_nodes({bit(4), numa::Mode::BIND}, machine, 2, UINT64_MAX), std::array<numa::NodeId, 1>{4});
}

TEST_F(NumaPolicy, InterleaveRotatesSortedSparseMembersIncludingMaximumPageIndex) {
    constexpr auto MASK = bit(0) | bit(2) | bit(5);
    numa::Policy const POLICY{MASK, numa::Mode::INTERLEAVE};
    expect_order(numa::select_nodes(POLICY, machine, 2, 0), std::array<numa::NodeId, 3>{0, 2, 5});
    expect_order(numa::select_nodes(POLICY, machine, 2, 1), std::array<numa::NodeId, 3>{2, 5, 0});
    expect_order(numa::select_nodes(POLICY, machine, 2, 2), std::array<numa::NodeId, 3>{5, 0, 2});
    expect_order(numa::select_nodes(POLICY, machine, 2, 3), std::array<numa::NodeId, 3>{0, 2, 5});
    expect_order(numa::select_nodes(POLICY, machine, 2, UINT64_MAX), std::array<numa::NodeId, 3>{0, 2, 5});
    expect_order(numa::select_nodes(POLICY, machine, 2, UINT64_MAX - 1), std::array<numa::NodeId, 3>{5, 0, 2});
}

TEST_F(NumaPolicy, ThousandInterleaveIndicesDistributePredictablyWithoutMutableState) {
    numa::Policy const POLICY{bit(0) | bit(2) | bit(5), numa::Mode::INTERLEAVE};
    std::array<uint64_t, 6> counts{};
    constexpr std::array<numa::NodeId, 3> CYCLE{0, 2, 5};
    for (uint64_t index = 0; index < 1000; ++index) {
        auto const SELECTED = numa::select_nodes(POLICY, machine, static_cast<numa::NodeId>(index % 6), index);
        ASSERT_TRUE(SELECTED.valid());
        EXPECT_EQ(SELECTED.count, 3);
        EXPECT_EQ(SELECTED.requested, CYCLE[index % CYCLE.size()]);
        ++counts.at(SELECTED.requested);
    }
    EXPECT_EQ(counts, (std::array<uint64_t, 6>{334, 0, 333, 0, 0, 333}));
    // Reversed calls after prior selections produce the same plan.
    expect_order(numa::select_nodes(POLICY, machine, 5, 2), std::array<numa::NodeId, 3>{5, 0, 2});
    expect_order(numa::select_nodes(POLICY, machine, 0, 0), std::array<numa::NodeId, 3>{0, 2, 5});
}

TEST_F(NumaPolicy, SupportsAll64NodesAndSingletonHighBitWithoutShiftTruncation) {
    machine.node_count = 64;
    for (numa::NodeId node = 0; node < machine.node_count; ++node) machine.distances[0][node] = node == 0 ? 10 : 20;
    auto const SELECTED = numa::select_nodes({UINT64_MAX, numa::Mode::INTERLEAVE}, machine, 0, UINT64_MAX);
    ASSERT_TRUE(SELECTED.valid());
    ASSERT_EQ(SELECTED.count, 64);
    EXPECT_EQ(SELECTED.requested, 63);
    EXPECT_EQ(SELECTED.nodes[0], 63);
    for (numa::NodeId position = 1; position < SELECTED.count; ++position) EXPECT_EQ(SELECTED.nodes[position], position - 1);
    expect_order(numa::select_nodes({bit(63), numa::Mode::BIND}, machine, 0, UINT64_MAX), std::array<numa::NodeId, 1>{63});
    expect_order(numa::select_nodes({bit(63), numa::Mode::INTERLEAVE}, machine, 0, UINT64_MAX), std::array<numa::NodeId, 1>{63});
    auto const PREFERRED = numa::select_nodes({bit(63), numa::Mode::PREFERRED}, machine, 0, 0);
    ASSERT_TRUE(PREFERRED.valid());
    ASSERT_EQ(PREFERRED.count, 64);
    EXPECT_EQ(PREFERRED.requested, 63);
    for (numa::NodeId position = 1; position < PREFERRED.count; ++position) EXPECT_EQ(PREFERRED.nodes[position], position - 1);
}

TEST_F(NumaPolicy, UmaSelectionAndInputValuesRemainUnmodified) {
    machine.node_count = 1;
    auto const BEFORE = machine;
    for (auto mode : {numa::Mode::LOCAL, numa::Mode::PREFERRED, numa::Mode::BIND, numa::Mode::INTERLEAVE}) {
        numa::Policy const POLICY{mode == numa::Mode::LOCAL ? 0U : 1U, mode};
        auto const SAVED = POLICY;
        expect_order(numa::select_nodes(POLICY, machine, 0, UINT64_MAX), std::array<numa::NodeId, 1>{0});
        EXPECT_EQ(POLICY, SAVED);
        EXPECT_EQ(machine, BEFORE);
    }
}

}  // namespace
