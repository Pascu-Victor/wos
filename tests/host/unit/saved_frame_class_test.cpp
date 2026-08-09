#include <gtest/gtest.h>

#include <platform/sched/frame_class.hpp>

namespace {
using ker::mod::sched::task::classify_saved_frame;
using ker::mod::sched::task::idle_loop_resume_ip_is_valid;
using ker::mod::sched::task::saved_frame_restore_policy;
using ker::mod::sched::task::SavedFrameClass;
using ker::mod::sched::task::SavedFrameClassificationInput;
using ker::mod::sched::task::SavedFrameOrigin;
using ker::mod::sched::task::SavedFrameOwner;
using ker::mod::sched::task::SavedFrameRestoreKind;
using ker::mod::sched::task::SavedFrameSelectors;
using ker::mod::sched::task::select_timer_return_frame_class;
using ker::mod::sched::task::select_user_fpu_restore_frame_class;

constexpr auto valid_input(SavedFrameOwner owner, SavedFrameOrigin origin, SavedFrameSelectors selectors) -> SavedFrameClassificationInput {
    return {
        .owner = owner,
        .origin = origin,
        .selectors = selectors,
        .instruction_pointer_valid = true,
        .stack_pointer_valid = true,
        .flags_valid = true,
    };
}

TEST(SavedFrameClass, DistinguishesEverySupportedFrameKind) {
    auto user = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::SYNTHETIC_USER_RETURN, SavedFrameSelectors::USER);
    EXPECT_EQ(classify_saved_frame(user), SavedFrameClass::USER_RETURN);

    auto timer_user = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::USER);
    timer_user.timer_interrupt = true;
    EXPECT_EQ(classify_saved_frame(timer_user), SavedFrameClass::USER_RETURN);

    auto voluntary = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::KERNEL);
    voluntary.timer_interrupt = true;
    voluntary.voluntary_process = true;
    EXPECT_EQ(classify_saved_frame(voluntary), SavedFrameClass::VOLUNTARY_PARKED_KERNEL);

    auto timer_process = voluntary;
    timer_process.voluntary_process = false;
    EXPECT_EQ(classify_saved_frame(timer_process), SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);

    auto daemon = valid_input(SavedFrameOwner::DAEMON, SavedFrameOrigin::DAEMON_START, SavedFrameSelectors::KERNEL);
    EXPECT_EQ(classify_saved_frame(daemon), SavedFrameClass::DAEMON_KERNEL);

    daemon.origin = SavedFrameOrigin::INTERRUPT;
    daemon.timer_interrupt = true;
    EXPECT_EQ(classify_saved_frame(daemon), SavedFrameClass::DAEMON_KERNEL);
}

TEST(SavedFrameClass, RejectsInvalidShapeProvenanceAndOwnerCombinations) {
    auto input = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::KERNEL);
    EXPECT_EQ(classify_saved_frame(input), SavedFrameClass::INVALID);

    input.timer_interrupt = true;
    input.selectors = SavedFrameSelectors::INVALID;
    EXPECT_EQ(classify_saved_frame(input), SavedFrameClass::INVALID);

    input.selectors = SavedFrameSelectors::KERNEL;
    input.instruction_pointer_valid = false;
    EXPECT_EQ(classify_saved_frame(input), SavedFrameClass::INVALID);
    input.instruction_pointer_valid = true;
    input.stack_pointer_valid = false;
    EXPECT_EQ(classify_saved_frame(input), SavedFrameClass::INVALID);
    input.stack_pointer_valid = true;
    input.flags_valid = false;
    EXPECT_EQ(classify_saved_frame(input), SavedFrameClass::INVALID);

    auto idle = valid_input(SavedFrameOwner::IDLE, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::KERNEL);
    idle.timer_interrupt = true;
    EXPECT_EQ(classify_saved_frame(idle), SavedFrameClass::INVALID);

    auto daemon_user = valid_input(SavedFrameOwner::DAEMON, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::USER);
    EXPECT_EQ(classify_saved_frame(daemon_user), SavedFrameClass::INVALID);
}

TEST(SavedFrameClass, IdleTimerResumeAcceptsEveryInstructionInLoopRange) {
    constexpr uint64_t LOOP_BEGIN = 0x1000;
    constexpr uint64_t LOOP_END = 0x1004;

    EXPECT_FALSE(idle_loop_resume_ip_is_valid(LOOP_BEGIN - 1, LOOP_BEGIN, LOOP_END));
    EXPECT_TRUE(idle_loop_resume_ip_is_valid(LOOP_BEGIN, LOOP_BEGIN, LOOP_END));
    EXPECT_TRUE(idle_loop_resume_ip_is_valid(LOOP_BEGIN + 1, LOOP_BEGIN, LOOP_END));
    EXPECT_TRUE(idle_loop_resume_ip_is_valid(LOOP_END - 1, LOOP_BEGIN, LOOP_END));
    EXPECT_FALSE(idle_loop_resume_ip_is_valid(LOOP_END, LOOP_BEGIN, LOOP_END));
    EXPECT_FALSE(idle_loop_resume_ip_is_valid(LOOP_BEGIN, LOOP_BEGIN, LOOP_BEGIN));
}

TEST(SavedFrameClass, TimerReturnUsesLiveClassOnlyWhenTheInterruptedTaskKeepsRunning) {
    EXPECT_EQ(select_timer_return_frame_class(true, SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL, SavedFrameClass::USER_RETURN),
              SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);
    EXPECT_EQ(select_timer_return_frame_class(false, SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL, SavedFrameClass::USER_RETURN),
              SavedFrameClass::USER_RETURN);
}

TEST(SavedFrameClass, UserFpuRestoreUsesMatchingValidatedTimerProvenance) {
    EXPECT_EQ(select_user_fpu_restore_frame_class(true, SavedFrameClass::USER_RETURN, SavedFrameClass::VOLUNTARY_PARKED_KERNEL),
              SavedFrameClass::USER_RETURN);
    EXPECT_EQ(select_user_fpu_restore_frame_class(false, SavedFrameClass::USER_RETURN, SavedFrameClass::VOLUNTARY_PARKED_KERNEL),
              SavedFrameClass::VOLUNTARY_PARKED_KERNEL);
    EXPECT_EQ(select_user_fpu_restore_frame_class(false, SavedFrameClass::INVALID, SavedFrameClass::USER_RETURN),
              SavedFrameClass::USER_RETURN);
}

TEST(SavedFrameClass, RestorePolicySeparatesUserAndSameCplKernelReturns) {
    auto const USER = saved_frame_restore_policy(SavedFrameClass::USER_RETURN);
    EXPECT_EQ(USER.kind, SavedFrameRestoreKind::USER_IRET);
    EXPECT_TRUE(USER.validate_as_user);
    EXPECT_TRUE(USER.deliver_signals);
    EXPECT_TRUE(USER.restore_user_fpu);

    for (auto const FRAME_CLASS :
         {SavedFrameClass::VOLUNTARY_PARKED_KERNEL, SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL, SavedFrameClass::DAEMON_KERNEL}) {
        auto const KERNEL = saved_frame_restore_policy(FRAME_CLASS);
        EXPECT_EQ(KERNEL.kind, SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET);
        EXPECT_FALSE(KERNEL.validate_as_user);
        EXPECT_FALSE(KERNEL.deliver_signals);
        EXPECT_FALSE(KERNEL.restore_user_fpu);
    }

    auto const INVALID = saved_frame_restore_policy(SavedFrameClass::INVALID);
    EXPECT_EQ(INVALID.kind, SavedFrameRestoreKind::REJECT);
    EXPECT_FALSE(INVALID.validate_as_user);
    EXPECT_FALSE(INVALID.deliver_signals);
    EXPECT_FALSE(INVALID.restore_user_fpu);
}
}  // namespace
