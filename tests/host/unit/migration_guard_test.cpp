#include "platform/sched/migration_guard.hpp"

#include <gtest/gtest.h>

namespace {
using ker::mod::sched::decode_migration_guard_state;
using ker::mod::sched::encode_migration_guard_state;
using ker::mod::sched::MIGRATION_CPU_INVALID;
using ker::mod::sched::migration_disable_transition;
using ker::mod::sched::migration_enable_transition;
using ker::mod::sched::migration_guard_disabled;
using ker::mod::sched::migration_guard_target_cpu;
using ker::mod::sched::MigrationGuardState;
using ker::mod::sched::MigrationGuardTransitionError;

TEST(MigrationGuard, NestedTransitionsPreserveOwnerUntilFinalEnable) {
    MigrationGuardState state{};
    state.owner_cpu = MIGRATION_CPU_INVALID;

    auto transition = migration_disable_transition(state, 3);
    ASSERT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    EXPECT_TRUE(transition.outermost);
    EXPECT_EQ(transition.state.depth, 1U);
    EXPECT_EQ(transition.state.owner_cpu, 3U);

    transition = migration_disable_transition(transition.state, 3);
    ASSERT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    EXPECT_FALSE(transition.outermost);
    EXPECT_EQ(transition.state.depth, 2U);
    EXPECT_EQ(transition.state.owner_cpu, 3U);

    transition = migration_enable_transition(transition.state, 3);
    ASSERT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    EXPECT_FALSE(transition.outermost);
    EXPECT_EQ(transition.state.depth, 1U);
    EXPECT_EQ(transition.state.owner_cpu, 3U);

    transition = migration_enable_transition(transition.state, 3);
    ASSERT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    EXPECT_TRUE(transition.outermost);
    EXPECT_EQ(transition.state.depth, 0U);
    EXPECT_EQ(transition.state.owner_cpu, MIGRATION_CPU_INVALID);
}

TEST(MigrationGuard, RejectsUnbalancedWrongCpuAndOverflowTransitions) {
    MigrationGuardState const ENABLE_WITHOUT_DISABLE{.depth = 0, .owner_cpu = MIGRATION_CPU_INVALID};
    EXPECT_EQ(migration_enable_transition(ENABLE_WITHOUT_DISABLE, 0).error, MigrationGuardTransitionError::ENABLE_WITHOUT_DISABLE);

    MigrationGuardState const HELD{.depth = 1, .owner_cpu = 2};
    EXPECT_EQ(migration_disable_transition(HELD, 1).error, MigrationGuardTransitionError::WRONG_CPU);
    EXPECT_EQ(migration_enable_transition(HELD, 1).error, MigrationGuardTransitionError::WRONG_CPU);

    MigrationGuardState const FULL{.depth = UINT32_MAX, .owner_cpu = 2};
    EXPECT_EQ(migration_disable_transition(FULL, 2).error, MigrationGuardTransitionError::DEPTH_OVERFLOW);
    EXPECT_EQ(migration_disable_transition(ENABLE_WITHOUT_DISABLE, UINT64_MAX).error, MigrationGuardTransitionError::INVALID_CPU);
}

TEST(MigrationGuard, PackedStateAndPlacementAreSingleSnapshotDecisions) {
    MigrationGuardState const HELD{.depth = 7, .owner_cpu = 4};
    EXPECT_EQ(decode_migration_guard_state(encode_migration_guard_state(HELD)).depth, HELD.depth);
    EXPECT_EQ(decode_migration_guard_state(encode_migration_guard_state(HELD)).owner_cpu, HELD.owner_cpu);
    EXPECT_TRUE(migration_guard_disabled(HELD));
    EXPECT_EQ(migration_guard_target_cpu(HELD, 1, 8), 4U);
    EXPECT_EQ(migration_guard_target_cpu(HELD, 1, 4), UINT64_MAX);

    MigrationGuardState const ENABLED{.depth = 0, .owner_cpu = MIGRATION_CPU_INVALID};
    EXPECT_FALSE(migration_guard_disabled(ENABLED));
    EXPECT_EQ(migration_guard_target_cpu(ENABLED, 6, 8), 6U);
}
}  // namespace
