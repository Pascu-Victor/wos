#include <gtest/gtest.h>

#include <platform/sched/scheduler_transition_model.hpp>

namespace {

using ker::mod::sched::model::Scenario;
using ker::mod::sched::model::ScenarioResult;

void expect_scenario(Scenario scenario) {
    ScenarioResult const result = ker::mod::sched::model::run_scenario(scenario);
    EXPECT_TRUE(result.passed);
    EXPECT_EQ(result.violations, 0U);
    EXPECT_GT(result.steps, 0U);
}

TEST(SchedulerTransitionModel, WakeBeforeAndAfterPark) {
    auto const before = ker::mod::sched::model::run_scenario(Scenario::EVENT_BEFORE_PARK);
    auto const after = ker::mod::sched::model::run_scenario(Scenario::EVENT_AFTER_PARK);
    EXPECT_TRUE(before.passed);
    EXPECT_TRUE(after.passed);
    EXPECT_EQ(before.violations | after.violations, 0U);
    EXPECT_GT(before.rejected, 0U);
}

TEST(SchedulerTransitionModel, DuplicateRescheduleIsSingleLeader) {
    auto const result = ker::mod::sched::model::run_scenario(Scenario::DUPLICATE_RESCHEDULE);
    expect_scenario(Scenario::DUPLICATE_RESCHEDULE);
    EXPECT_GT(result.rejected, 0U);
}

TEST(SchedulerTransitionModel, ReservedHandoffWakeRequeuesOutgoing) { expect_scenario(Scenario::HANDOFF_WAKE); }

TEST(SchedulerTransitionModel, LocalAndRemoteMigration) {
    auto const local = ker::mod::sched::model::run_scenario(Scenario::LOCAL_MIGRATION);
    auto const remote = ker::mod::sched::model::run_scenario(Scenario::TWO_CPU_MIGRATION);
    EXPECT_TRUE(local.passed);
    EXPECT_TRUE(remote.passed);
    EXPECT_EQ(local.violations | remote.violations, 0U);
    EXPECT_EQ(local.ipis_sent, 0U);
    EXPECT_GT(remote.ipis_sent, 0U);
    EXPECT_EQ(remote.ipis_sent, remote.ipis_delivered);
}

TEST(SchedulerTransitionModel, PinAndDomainRejectMigrationWithoutCorruption) {
    auto const pin = ker::mod::sched::model::run_scenario(Scenario::PIN_REJECTION);
    auto const domain = ker::mod::sched::model::run_scenario(Scenario::DOMAIN_REJECTION);
    EXPECT_TRUE(pin.passed);
    EXPECT_TRUE(domain.passed);
    EXPECT_EQ(pin.violations | domain.violations, 0U);
    EXPECT_GT(pin.rejected, 0U);
    EXPECT_GT(domain.rejected, 0U);
}

TEST(SchedulerTransitionModel, PreemptPendingIsServicedAtOutermostEnable) { expect_scenario(Scenario::PREEMPT_PENDING); }

TEST(SchedulerTransitionModel, ExitDeadEpochReferenceAndGcSequence) { expect_scenario(Scenario::EXIT_GC); }

TEST(SchedulerTransitionModel, BoundedExplorationIsDeterministicallyReplayable) {
    auto const first = ker::mod::sched::model::explore_bounded();
    ASSERT_TRUE(first.passed);
    EXPECT_EQ(first.violations, 0U);
    EXPECT_GT(first.states, 0U);
    EXPECT_LE(first.states, ker::mod::sched::model::MAX_EXPLORATION_STATES);
    EXPECT_GT(first.traces, 0U);
    EXPECT_LE(first.traces, ker::mod::sched::model::MAX_EXPLORATION_TRACES);
    EXPECT_EQ(first.max_depth, ker::mod::sched::model::EXPLORATION_DEPTH);
    EXPECT_NE(first.replay_seed, 0U);

    auto const replay = ker::mod::sched::model::explore_bounded(first.replay_seed);
    EXPECT_EQ(replay.passed, first.passed);
    EXPECT_EQ(replay.states, first.states);
    EXPECT_EQ(replay.traces, first.traces);
    EXPECT_EQ(replay.max_depth, first.max_depth);
    EXPECT_EQ(replay.replay_seed, first.replay_seed);
    EXPECT_EQ(replay.violations, first.violations);
}

TEST(SchedulerTransitionModel, EveryDeclaredInvariantHasANegativeWitness) {
    constexpr uint64_t DETECTED = ker::mod::sched::model::negative_invariant_coverage();
    EXPECT_EQ(DETECTED & ker::mod::sched::model::REQUIRED_INVARIANTS, ker::mod::sched::model::REQUIRED_INVARIANTS);
    EXPECT_TRUE(ker::mod::sched::model::negative_invariant_detection());
}

}  // namespace
