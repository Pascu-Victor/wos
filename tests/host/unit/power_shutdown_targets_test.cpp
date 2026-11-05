#include <gtest/gtest.h>

#include <platform/sched/process_signal_targets.hpp>

namespace {
using ker::mod::sched::fresh_admission_allowed;
using ker::mod::sched::ProcessSignalTarget;
using ker::mod::sched::visible_process_signal_target;

constexpr ProcessSignalTarget PROCESS{.pid = 42, .process_pid = 42, .active = true, .process = true, .visible = true};

TEST(PowerShutdownTargets, FreshPublicationSealsConsumersBeforeProviders) {
    for (bool shutdown : {false, true}) {
        for (bool workers_stopping : {false, true}) {
            EXPECT_TRUE(fresh_admission_allowed(false, false, shutdown, workers_stopping));
            EXPECT_EQ(fresh_admission_allowed(true, false, shutdown, workers_stopping), !shutdown);
            EXPECT_EQ(fresh_admission_allowed(false, true, shutdown, workers_stopping), !workers_stopping);
            EXPECT_EQ(fresh_admission_allowed(true, true, shutdown, workers_stopping), !shutdown && !workers_stopping);
        }
    }
    // PREPARING closes fresh consumers but must retain provider startup.
    EXPECT_FALSE(fresh_admission_allowed(true, false, true, false));
    EXPECT_TRUE(fresh_admission_allowed(false, true, true, false));
}

TEST(PowerShutdownTargets, RequiresAnActiveVisibleProcess) {
    for (unsigned fields = 0; fields < 16; ++fields) {
        auto target = PROCESS;
        target.active = (fields & 1U) != 0;
        target.exiting = (fields & 2U) != 0;
        target.process = (fields & 4U) != 0;
        target.visible = (fields & 8U) != 0;
        EXPECT_EQ(visible_process_signal_target(target, 9, 9), target.active && target.process && target.visible);
        EXPECT_EQ(visible_process_signal_target(target, 9, 9, true), (target.active || target.exiting) && target.process);
    }
    EXPECT_FALSE(visible_process_signal_target({}, 9, 9, true));
}

TEST(PowerShutdownTargets, InitIsExcludedByDefaultAndIncludedOnlyByOptIn) {
    auto init = PROCESS;
    init.pid = init.process_pid = 1;
    EXPECT_FALSE(visible_process_signal_target(init, 9, 9));
    EXPECT_FALSE(visible_process_signal_target(init, 9, 9, false));
    EXPECT_TRUE(visible_process_signal_target(init, 9, 9, true));
    EXPECT_TRUE(visible_process_signal_target(PROCESS, 9, 9));
}

TEST(PowerShutdownTargets, InitOptInStillRespectsCurrentTaskAndProcessExclusions) {
    EXPECT_FALSE(visible_process_signal_target(PROCESS, 42, 9));
    EXPECT_FALSE(visible_process_signal_target(PROCESS, 42, 9, true));
    EXPECT_FALSE(visible_process_signal_target(PROCESS, 9, 42));
    EXPECT_FALSE(visible_process_signal_target(PROCESS, 9, 42, true));
    auto init = PROCESS;
    init.pid = init.process_pid = 1;
    EXPECT_FALSE(visible_process_signal_target(init, 1, 9, true));
    EXPECT_FALSE(visible_process_signal_target(init, 9, 1, true));
}

TEST(PowerShutdownTargets, TerminalModeIncludesInvisibleThreadsAndProxiesButNotExcludedOwners) {
    auto target = PROCESS;
    target.process_pid = 9;
    EXPECT_FALSE(visible_process_signal_target(target, 8, 9, true));
    target.process_pid = 42;
    target.visible = false;
    EXPECT_FALSE(visible_process_signal_target(target, 9, 9));
    EXPECT_TRUE(visible_process_signal_target(target, 9, 9, true));
    target.process = false;
    EXPECT_FALSE(visible_process_signal_target(target, 9, 9, true));
}

TEST(PowerShutdownTargets, TerminalModeKeepsActiveAndExitingCleanupUntilDead) {
    // exit_in_progress deliberately is not an eligibility field: descriptor,
    // native-owner and lazy-file cleanup can still block while ACTIVE.
    auto target = PROCESS;
    EXPECT_TRUE(visible_process_signal_target(target, 9, 9, true));
    target.active = false;
    target.exiting = true;
    target.visible = false;
    EXPECT_FALSE(visible_process_signal_target(target, 9, 9));
    EXPECT_TRUE(visible_process_signal_target(target, 9, 9, true));
    target.exiting = false;
    EXPECT_FALSE(visible_process_signal_target(target, 9, 9, true));
}
}  // namespace
