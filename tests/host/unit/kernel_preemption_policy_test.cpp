#include <gtest/gtest.h>

#include <limits>
#include <platform/sched/migration_policy.hpp>
#include <platform/sched/preemption_diagnostics.hpp>
#include <platform/sched/preemption_policy.hpp>

namespace {
using ker::mod::sched::CrossCpuMigrationEligibilityInput;
using ker::mod::sched::decode_migration_diagnostic;
using ker::mod::sched::decode_preemption_diagnostic;
using ker::mod::sched::encode_migration_diagnostic;
using ker::mod::sched::encode_preemption_diagnostic;
using ker::mod::sched::evaluate_cross_cpu_migration;
using ker::mod::sched::evaluate_kernel_preemption;
using ker::mod::sched::handoff_commit_allowed;
using ker::mod::sched::handoff_stack_collision;
using ker::mod::sched::KernelPreemptionBlockReason;
using ker::mod::sched::KernelPreemptionBootPolicyInput;
using ker::mod::sched::KernelPreemptionEligibilityInput;
using ker::mod::sched::migration_rejection_reason_name;
using ker::mod::sched::MigrationDiagnostic;
using ker::mod::sched::MigrationRejectionReason;
using ker::mod::sched::preempt_disable_transition;
using ker::mod::sched::preempt_enable_transition;
using ker::mod::sched::preempt_pending_action;
using ker::mod::sched::PreemptGuardTransitionError;
using ker::mod::sched::PreemptionDiagnostic;
using ker::mod::sched::PreemptPendingAction;
using ker::mod::sched::resolve_kernel_preemption_boot_policy;
using ker::mod::sched::task::SavedFrameClass;

auto ordinary_kernel_input() -> KernelPreemptionEligibilityInput {
    return {
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .ordinary_process_kernel_enabled = true,
        .same_cpu_migration_guarded = true,
    };
}

auto valid_cross_cpu_timer_input() -> CrossCpuMigrationEligibilityInput {
    return {
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .source_owner_valid = true,
        .active = true,
        .resources_valid = true,
        .frame_valid = true,
        .cpu_pin_allows_target = true,
        .domain_allows_target = true,
    };
}

TEST(KernelPreemptionPolicy, ExplicitRollbackWinsEveryBootPolicyCombination) {
    EXPECT_FALSE(resolve_kernel_preemption_boot_policy({}));
    EXPECT_TRUE(resolve_kernel_preemption_boot_policy({.default_enabled = true}));
    EXPECT_TRUE(resolve_kernel_preemption_boot_policy({.force_on = true}));
    EXPECT_FALSE(resolve_kernel_preemption_boot_policy({.default_enabled = true, .force_off = true}));
    EXPECT_FALSE(resolve_kernel_preemption_boot_policy({.default_enabled = true, .force_on = true, .force_off = true}));
}

TEST(KernelPreemptionPolicy, OrdinaryKernelFrameRequiresPolicyAndSameCpuGuard) {
    auto input = ordinary_kernel_input();
    input.ordinary_process_kernel_enabled = false;
    auto decision = evaluate_kernel_preemption(input);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_FALSE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::POLICY_DISABLED);

    input = ordinary_kernel_input();
    input.same_cpu_migration_guarded = false;
    decision = evaluate_kernel_preemption(input);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::MIGRATION_UNSTABLE);

    decision = evaluate_kernel_preemption(ordinary_kernel_input());
    EXPECT_TRUE(decision.kernel_frame);
    EXPECT_TRUE(decision.restorable);
    EXPECT_TRUE(decision.can_switch);
    EXPECT_FALSE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::NONE);
}

TEST(KernelPreemptionPolicy, TemporaryUnsafeRegionsRecordPendingWork) {
    auto input = ordinary_kernel_input();
    input.preempt_disable_depth = 2;
    auto decision = evaluate_kernel_preemption(input);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_TRUE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::PREEMPT_DISABLED);

    for (int conflict = 0; conflict < 3; ++conflict) {
        input = ordinary_kernel_input();
        input.scheduler_transition_active = conflict == 0;
        input.deferred_task_switch = conflict == 1;
        input.wants_block = conflict == 2;
        decision = evaluate_kernel_preemption(input);
        EXPECT_FALSE(decision.can_switch);
        EXPECT_TRUE(decision.record_pending);
        EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::RETURN_TRANSITION);
    }
}

TEST(KernelPreemptionPolicy, ExistingUserVoluntaryAndDaemonSemanticsRemainSeparate) {
    auto user = ordinary_kernel_input();
    user.frame_class = SavedFrameClass::USER_RETURN;
    auto decision = evaluate_kernel_preemption(user);
    EXPECT_FALSE(decision.kernel_frame);
    EXPECT_TRUE(decision.restorable);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::NOT_KERNEL_FRAME);

    auto voluntary = ordinary_kernel_input();
    voluntary.frame_class = SavedFrameClass::VOLUNTARY_PARKED_KERNEL;
    voluntary.ordinary_process_kernel_enabled = false;
    voluntary.deferred_task_switch = true;
    voluntary.wants_block = true;
    decision = evaluate_kernel_preemption(voluntary);
    EXPECT_TRUE(decision.can_switch);
    EXPECT_FALSE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::NONE);

    auto daemon = ordinary_kernel_input();
    daemon.frame_class = SavedFrameClass::DAEMON_KERNEL;
    daemon.ordinary_process_kernel_enabled = false;
    decision = evaluate_kernel_preemption(daemon);
    EXPECT_TRUE(decision.can_switch);

    auto invalid = ordinary_kernel_input();
    invalid.frame_class = SavedFrameClass::INVALID;
    decision = evaluate_kernel_preemption(invalid);
    EXPECT_FALSE(decision.restorable);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::INVALID_FRAME);
}

TEST(KernelPreemptionPolicy, UserReturnCannotBypassProtectedOrTransitionState) {
    auto user = ordinary_kernel_input();
    user.frame_class = SavedFrameClass::USER_RETURN;
    user.preempt_disable_depth = 1;
    auto decision = evaluate_kernel_preemption(user);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_TRUE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::PREEMPT_DISABLED);

    user.preempt_disable_depth = 0;
    user.scheduler_transition_active = true;
    decision = evaluate_kernel_preemption(user);
    EXPECT_FALSE(decision.can_switch);
    EXPECT_TRUE(decision.record_pending);
    EXPECT_EQ(decision.reason, KernelPreemptionBlockReason::RETURN_TRANSITION);
}

TEST(KernelPreemptionPolicy, GuardTransitionsRejectOverflowAndUnderflow) {
    auto transition = preempt_disable_transition(0);
    EXPECT_EQ(transition.depth, 1U);
    EXPECT_TRUE(transition.outermost);
    EXPECT_EQ(transition.error, PreemptGuardTransitionError::NONE);

    transition = preempt_disable_transition(transition.depth);
    EXPECT_EQ(transition.depth, 2U);
    EXPECT_FALSE(transition.outermost);

    transition = preempt_enable_transition(transition.depth);
    EXPECT_EQ(transition.depth, 1U);
    EXPECT_FALSE(transition.outermost);
    transition = preempt_enable_transition(transition.depth);
    EXPECT_EQ(transition.depth, 0U);
    EXPECT_TRUE(transition.outermost);

    EXPECT_EQ(preempt_enable_transition(0).error, PreemptGuardTransitionError::UNDERFLOW);
    EXPECT_EQ(preempt_disable_transition(std::numeric_limits<uint32_t>::max()).error, PreemptGuardTransitionError::OVERFLOW);
}

TEST(KernelPreemptionPolicy, PendingWorkSurvivesReturnTransitionAndServicesAtSafeBoundary) {
    EXPECT_EQ(preempt_pending_action(2, true, false), PreemptPendingAction::NONE);
    EXPECT_EQ(preempt_pending_action(0, false, false), PreemptPendingAction::NONE);
    EXPECT_EQ(preempt_pending_action(0, true, true), PreemptPendingAction::PRESERVE);
    EXPECT_EQ(preempt_pending_action(0, true, false), PreemptPendingAction::SERVICE);
}

TEST(KernelPreemptionPolicy, OwnershipCommitRejectsDisabledOutgoingTask) {
    EXPECT_TRUE(handoff_commit_allowed(0, true));
    EXPECT_TRUE(handoff_commit_allowed(3, false));
    EXPECT_FALSE(handoff_commit_allowed(1, true));
}

TEST(KernelPreemptionPolicy, DifferentTasksCannotShareKernelStack) {
    constexpr uint64_t STACK_A = 0xffff800000010000ULL;
    constexpr uint64_t STACK_B = 0xffff800000020000ULL;

    EXPECT_FALSE(handoff_stack_collision(STACK_A, STACK_A, false));
    EXPECT_FALSE(handoff_stack_collision(0, STACK_A, true));
    EXPECT_FALSE(handoff_stack_collision(STACK_A, STACK_B, true));
    EXPECT_TRUE(handoff_stack_collision(STACK_A, STACK_A, true));
}

TEST(KernelPreemptionPolicy, PackedDiagnosticsRoundTripWithoutAllocation) {
    constexpr PreemptionDiagnostic PREEMPTION{
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .reason = KernelPreemptionBlockReason::PREEMPT_DISABLED,
        .source_cpu = 3,
        .target_cpu = 5,
    };
    constexpr auto PREEMPTION_DECODED = decode_preemption_diagnostic(encode_preemption_diagnostic(PREEMPTION));
    static_assert(PREEMPTION_DECODED.frame_class == SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);
    static_assert(PREEMPTION_DECODED.reason == KernelPreemptionBlockReason::PREEMPT_DISABLED);
    static_assert(PREEMPTION_DECODED.source_cpu == 3);
    static_assert(PREEMPTION_DECODED.target_cpu == 5);

    constexpr MigrationDiagnostic MIGRATION{
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .reason = MigrationRejectionReason::MIGRATION_DISABLED,
        .source_cpu = 7,
        .target_cpu = 1,
    };
    constexpr auto MIGRATION_DECODED = decode_migration_diagnostic(encode_migration_diagnostic(MIGRATION));
    static_assert(MIGRATION_DECODED.frame_class == SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);
    static_assert(MIGRATION_DECODED.reason == MigrationRejectionReason::MIGRATION_DISABLED);
    static_assert(MIGRATION_DECODED.source_cpu == 7);
    static_assert(MIGRATION_DECODED.target_cpu == 1);
    EXPECT_STREQ(migration_rejection_reason_name(MigrationRejectionReason::INVALID_FRAME), "invalid-frame");
}

TEST(CrossCpuMigrationPolicy, EveryValidatedRestoreClassCanMigrate) {
    for (auto const FRAME_CLASS : {SavedFrameClass::USER_RETURN, SavedFrameClass::VOLUNTARY_PARKED_KERNEL,
                                   SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL, SavedFrameClass::DAEMON_KERNEL}) {
        auto input = valid_cross_cpu_timer_input();
        input.frame_class = FRAME_CLASS;
        auto const DECISION = evaluate_cross_cpu_migration(input);
        EXPECT_TRUE(DECISION.can_migrate);
        EXPECT_EQ(DECISION.reason, MigrationRejectionReason::NONE);
    }

    auto input = valid_cross_cpu_timer_input();
    input.frame_class = SavedFrameClass::INVALID;
    auto const DECISION = evaluate_cross_cpu_migration(input);
    EXPECT_FALSE(DECISION.can_migrate);
    EXPECT_EQ(DECISION.reason, MigrationRejectionReason::INVALID_FRAME);
}

TEST(CrossCpuMigrationPolicy, RejectsEveryOwnershipAndTransitionHazard) {
    auto expect_rejection = [](CrossCpuMigrationEligibilityInput input, MigrationRejectionReason expected) {
        auto const DECISION = evaluate_cross_cpu_migration(input);
        EXPECT_FALSE(DECISION.can_migrate);
        EXPECT_EQ(DECISION.reason, expected);
    };

    auto input = valid_cross_cpu_timer_input();
    input.source_owner_valid = false;
    expect_rejection(input, MigrationRejectionReason::INVALID_OWNER);

    input = valid_cross_cpu_timer_input();
    input.active = false;
    expect_rejection(input, MigrationRejectionReason::NOT_RUNNABLE);
    input = valid_cross_cpu_timer_input();
    input.resources_valid = false;
    expect_rejection(input, MigrationRejectionReason::NOT_RUNNABLE);
    input = valid_cross_cpu_timer_input();
    input.migration_disabled = true;
    expect_rejection(input, MigrationRejectionReason::MIGRATION_DISABLED);
    input = valid_cross_cpu_timer_input();
    input.preempt_disabled = true;
    expect_rejection(input, MigrationRejectionReason::PREEMPT_DISABLED);
    input = valid_cross_cpu_timer_input();
    input.return_transition = true;
    expect_rejection(input, MigrationRejectionReason::RETURN_TRANSITION);
    input = valid_cross_cpu_timer_input();
    input.cpu_pin_allows_target = false;
    expect_rejection(input, MigrationRejectionReason::CPU_PINNED);
    input = valid_cross_cpu_timer_input();
    input.domain_allows_target = false;
    expect_rejection(input, MigrationRejectionReason::DOMAIN_RESTRICTED);
    input = valid_cross_cpu_timer_input();
    input.wki_owned = true;
    expect_rejection(input, MigrationRejectionReason::WKI_OWNED);
    input = valid_cross_cpu_timer_input();
    input.frame_valid = false;
    expect_rejection(input, MigrationRejectionReason::INVALID_FRAME);
}
}  // namespace
