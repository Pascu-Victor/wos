#include <cstdint>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/migration_policy.hpp>
#include <platform/sched/preemption_diagnostics.hpp>
#include <platform/sched/preemption_policy.hpp>
#include <platform/sched/run_heap.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/scheduler_transition_model.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <test/ktest.hpp>

// ---------------------------------------------------------------------------
// Pure EEVDF math tests - no real tasks, no scheduler calls.
// ---------------------------------------------------------------------------

// kNiceToWeight values (nice=0 -> 1024, nice=5 -> 335, nice=-5 -> 3121).
// Hardcoded to match the table in scheduler.cpp.
static constexpr uint32_t WEIGHT_NICE_0 = 1024;
static constexpr uint32_t WEIGHT_NICE_P5 = 335;   // nice=+5 (lower prio)
static constexpr uint32_t WEIGHT_NICE_N5 = 3121;  // nice=-5 (higher prio)

namespace ker::mod::sched {
auto scheduler_selftest_transition_validator_detects_corruption() -> bool;
auto scheduler_selftest_handoff_preserves_runnable_event_token() -> bool;
auto scheduler_selftest_exception_wait_token_closes_prepark_race() -> bool;
auto scheduler_selftest_reserved_wake_precedes_handoff_commit() -> bool;
auto scheduler_selftest_concurrent_reschedule_requests_are_serialized() -> bool;
auto scheduler_selftest_runtime_delta_saturates() -> bool;
auto scheduler_selftest_migration_policy_preserves_hot_process_migration() -> bool;
auto scheduler_selftest_heap_scan_removal_repairs_stale_index() -> bool;
auto scheduler_selftest_load_balance_nudge_needs_process_backlog() -> bool;
auto scheduler_selftest_idle_steal_scan_expands_for_large_backlog() -> bool;
auto scheduler_selftest_effectively_idle_current_accepts_rebalance_probe() -> bool;
auto scheduler_selftest_loadavg_wait_channel_policy() -> bool;
auto scheduler_selftest_deferred_yield_requires_sched_yield_channel() -> bool;
auto scheduler_selftest_lazy_executable_resume_range_policy() -> bool;
}  // namespace ker::mod::sched

KTEST(Sched, VruntimeOrdering) {
    // vruntime delta = elapsed_ns * 1024 / weight
    // Lower weight -> larger delta -> vruntime accumulates faster.
    constexpr uint64_t ELAPSED_NS = 1'000'000ULL;  // 1 ms

    uint64_t const DV_NICE0 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_0;
    uint64_t const DV_NICE_P5 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_P5;
    uint64_t const DV_NICE_N5 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_N5;

    // Higher nice -> lower weight -> faster vruntime accumulation
    KEXPECT_TRUE(DV_NICE_P5 > DV_NICE0);
    KEXPECT_TRUE(DV_NICE0 > DV_NICE_N5);
    KEXPECT_TRUE(DV_NICE_N5 > 0ULL);
}

KTEST(Sched, DeadlineComputation) {
    // vdeadline = vruntime + (sliceNs * 1024) / weight
    // A task with lower weight gets a larger deadline increment,
    // so for equal vruntimes it is considered less urgent.
    constexpr uint64_t SLICE_NS = 4'000'000ULL;  // 4 ms
    constexpr int64_t VRUNTIME = 1'000'000LL;

    int64_t const DL_W0 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_0);
    int64_t const DL_WP5 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_P5);
    int64_t const DL_WN5 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_N5);

    // Lower weight (higher nice) -> larger deadline -> less urgent
    KEXPECT_TRUE(DL_WP5 > DL_W0);
    KEXPECT_TRUE(DL_W0 > DL_WN5);
}

KTEST(Sched, SaturatingDeadlineUs) {
    using ker::mod::sched::saturating_deadline_us;

    KEXPECT_EQ(saturating_deadline_us(10, 5), 15ULL);
    KEXPECT_EQ(saturating_deadline_us(10, 0), 10ULL);
    KEXPECT_EQ(saturating_deadline_us(UINT64_MAX - 5ULL, 5), UINT64_MAX);
    KEXPECT_EQ(saturating_deadline_us(UINT64_MAX - 5ULL, 6), UINT64_MAX);
    KEXPECT_EQ(saturating_deadline_us(UINT64_MAX, 1), UINT64_MAX);
}

KTEST(SchedulerFrameClass, CoversEverySavedFrameKind) {
    using ker::mod::sched::task::classify_saved_frame;
    using ker::mod::sched::task::SavedFrameClass;
    using ker::mod::sched::task::SavedFrameClassificationInput;
    using ker::mod::sched::task::SavedFrameOrigin;
    using ker::mod::sched::task::SavedFrameOwner;
    using ker::mod::sched::task::SavedFrameSelectors;

    auto valid_input = [](SavedFrameOwner owner, SavedFrameOrigin origin, SavedFrameSelectors selectors) {
        return SavedFrameClassificationInput{
            .owner = owner,
            .origin = origin,
            .selectors = selectors,
            .instruction_pointer_valid = true,
            .stack_pointer_valid = true,
            .flags_valid = true,
        };
    };

    auto user = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::SYNTHETIC_USER_RETURN, SavedFrameSelectors::USER);
    KEXPECT_EQ(classify_saved_frame(user), SavedFrameClass::USER_RETURN);

    auto voluntary = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::KERNEL);
    voluntary.timer_interrupt = true;
    voluntary.voluntary_process = true;
    KEXPECT_EQ(classify_saved_frame(voluntary), SavedFrameClass::VOLUNTARY_PARKED_KERNEL);

    voluntary.voluntary_process = false;
    KEXPECT_EQ(classify_saved_frame(voluntary), SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);

    auto daemon = valid_input(SavedFrameOwner::DAEMON, SavedFrameOrigin::DAEMON_START, SavedFrameSelectors::KERNEL);
    KEXPECT_EQ(classify_saved_frame(daemon), SavedFrameClass::DAEMON_KERNEL);

    auto invalid = valid_input(SavedFrameOwner::PROCESS, SavedFrameOrigin::INTERRUPT, SavedFrameSelectors::KERNEL);
    KEXPECT_EQ(classify_saved_frame(invalid), SavedFrameClass::INVALID);
    invalid.timer_interrupt = true;
    invalid.stack_pointer_valid = false;
    KEXPECT_EQ(classify_saved_frame(invalid), SavedFrameClass::INVALID);
}

KTEST(SchedulerFrameClass, RestorePolicyKeepsKernelFramesOffUserReturnPaths) {
    using ker::mod::sched::task::saved_frame_restore_policy;
    using ker::mod::sched::task::SavedFrameClass;
    using ker::mod::sched::task::SavedFrameRestoreKind;

    auto const USER = saved_frame_restore_policy(SavedFrameClass::USER_RETURN);
    KEXPECT_EQ(USER.kind, SavedFrameRestoreKind::USER_IRET);
    KEXPECT_TRUE(USER.validate_as_user);
    KEXPECT_TRUE(USER.deliver_signals);
    KEXPECT_TRUE(USER.restore_user_fpu);

    auto const KERNEL = saved_frame_restore_policy(SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL);
    KEXPECT_EQ(KERNEL.kind, SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET);
    KEXPECT_FALSE(KERNEL.validate_as_user);
    KEXPECT_FALSE(KERNEL.deliver_signals);
    KEXPECT_FALSE(KERNEL.restore_user_fpu);

    KEXPECT_EQ(saved_frame_restore_policy(SavedFrameClass::INVALID).kind, SavedFrameRestoreKind::REJECT);
}

KTEST(SchedulerPreemption, OrdinaryKernelEligibilityAndRollback) {
    using ker::mod::sched::decode_migration_diagnostic;
    using ker::mod::sched::encode_migration_diagnostic;
    using ker::mod::sched::evaluate_kernel_preemption;
    using ker::mod::sched::handoff_commit_allowed;
    using ker::mod::sched::handoff_stack_collision;
    using ker::mod::sched::KernelPreemptionBlockReason;
    using ker::mod::sched::KernelPreemptionEligibilityInput;
    using ker::mod::sched::MigrationDiagnostic;
    using ker::mod::sched::MigrationRejectionReason;
    using ker::mod::sched::preempt_disable_transition;
    using ker::mod::sched::preempt_enable_transition;
    using ker::mod::sched::preempt_pending_action;
    using ker::mod::sched::PreemptGuardTransitionError;
    using ker::mod::sched::PreemptPendingAction;
    using ker::mod::sched::resolve_kernel_preemption_boot_policy;
    using ker::mod::sched::task::SavedFrameClass;

    KEXPECT_TRUE(resolve_kernel_preemption_boot_policy({.default_enabled = true}));
    KEXPECT_TRUE(resolve_kernel_preemption_boot_policy({.force_on = true}));
    KEXPECT_FALSE(resolve_kernel_preemption_boot_policy({.default_enabled = true, .force_on = true, .force_off = true}));

    KernelPreemptionEligibilityInput input{
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .ordinary_process_kernel_enabled = true,
        .same_cpu_migration_guarded = true,
    };
    auto decision = evaluate_kernel_preemption(input);
    KEXPECT_TRUE(decision.can_switch);
    KEXPECT_EQ(decision.reason, KernelPreemptionBlockReason::NONE);

    input.preempt_disable_depth = 1;
    decision = evaluate_kernel_preemption(input);
    KEXPECT_FALSE(decision.can_switch);
    KEXPECT_TRUE(decision.record_pending);
    KEXPECT_EQ(decision.reason, KernelPreemptionBlockReason::PREEMPT_DISABLED);

    input.preempt_disable_depth = 0;
    input.scheduler_transition_active = true;
    decision = evaluate_kernel_preemption(input);
    KEXPECT_FALSE(decision.can_switch);
    KEXPECT_TRUE(decision.record_pending);
    KEXPECT_EQ(decision.reason, KernelPreemptionBlockReason::RETURN_TRANSITION);

    input.frame_class = SavedFrameClass::VOLUNTARY_PARKED_KERNEL;
    input.scheduler_transition_active = false;
    input.deferred_task_switch = true;
    input.wants_block = true;
    decision = evaluate_kernel_preemption(input);
    KEXPECT_TRUE(decision.can_switch);
    KEXPECT_FALSE(decision.record_pending);
    KEXPECT_EQ(decision.reason, KernelPreemptionBlockReason::NONE);

    input.frame_class = SavedFrameClass::USER_RETURN;
    input.deferred_task_switch = false;
    input.wants_block = false;
    input.preempt_disable_depth = 1;
    decision = evaluate_kernel_preemption(input);
    KEXPECT_FALSE(decision.can_switch);
    KEXPECT_TRUE(decision.record_pending);
    KEXPECT_EQ(decision.reason, KernelPreemptionBlockReason::PREEMPT_DISABLED);
    KEXPECT_FALSE(handoff_commit_allowed(1, true));
    KEXPECT_FALSE(handoff_stack_collision(0x1000, 0x1000, false));
    KEXPECT_FALSE(handoff_stack_collision(0, 0x1000, true));
    KEXPECT_FALSE(handoff_stack_collision(0x1000, 0x2000, true));
    KEXPECT_TRUE(handoff_stack_collision(0x1000, 0x1000, true));
    KEXPECT_TRUE(handoff_commit_allowed(1, false));

    auto transition = preempt_disable_transition(0);
    KEXPECT_TRUE(transition.outermost);
    transition = preempt_disable_transition(transition.depth);
    KEXPECT_EQ(transition.depth, 2U);
    transition = preempt_enable_transition(transition.depth);
    KEXPECT_FALSE(transition.outermost);
    transition = preempt_enable_transition(transition.depth);
    KEXPECT_TRUE(transition.outermost);
    KEXPECT_EQ(preempt_enable_transition(0).error, PreemptGuardTransitionError::UNDERFLOW);
    KEXPECT_EQ(preempt_pending_action(0, true, true), PreemptPendingAction::PRESERVE);
    KEXPECT_EQ(preempt_pending_action(0, true, false), PreemptPendingAction::SERVICE);

    constexpr MigrationDiagnostic MIGRATION_DIAGNOSTIC{
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .reason = MigrationRejectionReason::MIGRATION_DISABLED,
        .source_cpu = 2,
        .target_cpu = 3,
    };
    constexpr auto DECODED_MIGRATION_DIAGNOSTIC = decode_migration_diagnostic(encode_migration_diagnostic(MIGRATION_DIAGNOSTIC));
    KEXPECT_EQ(DECODED_MIGRATION_DIAGNOSTIC.reason, MigrationRejectionReason::MIGRATION_DISABLED);
    KEXPECT_EQ(DECODED_MIGRATION_DIAGNOSTIC.source_cpu, 2U);
    KEXPECT_EQ(DECODED_MIGRATION_DIAGNOSTIC.target_cpu, 3U);
}

KTEST(SchedulerMigration, CrossCpuTimerFrameRequiresEveryInvariant) {
    using ker::mod::sched::CrossCpuMigrationEligibilityInput;
    using ker::mod::sched::evaluate_cross_cpu_migration;
    using ker::mod::sched::MigrationRejectionReason;
    using ker::mod::sched::task::SavedFrameClass;

    CrossCpuMigrationEligibilityInput input{
        .frame_class = SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .source_owner_valid = true,
        .active = true,
        .resources_valid = true,
        .frame_valid = true,
        .cpu_pin_allows_target = true,
        .domain_allows_target = true,
    };
    auto decision = evaluate_cross_cpu_migration(input);
    KEXPECT_TRUE(decision.can_migrate);
    KEXPECT_EQ(decision.reason, MigrationRejectionReason::NONE);

    input.migration_disabled = true;
    decision = evaluate_cross_cpu_migration(input);
    KEXPECT_FALSE(decision.can_migrate);
    KEXPECT_EQ(decision.reason, MigrationRejectionReason::MIGRATION_DISABLED);

    input.migration_disabled = false;
    input.return_transition = true;
    decision = evaluate_cross_cpu_migration(input);
    KEXPECT_FALSE(decision.can_migrate);
    KEXPECT_EQ(decision.reason, MigrationRejectionReason::RETURN_TRANSITION);

    input.return_transition = false;
    input.frame_valid = false;
    decision = evaluate_cross_cpu_migration(input);
    KEXPECT_FALSE(decision.can_migrate);
    KEXPECT_EQ(decision.reason, MigrationRejectionReason::INVALID_FRAME);
}

KTEST(SchedulerMigrationGuard, NestedOwnerAndPlacementContract) {
    using ker::mod::sched::decode_migration_guard_state;
    using ker::mod::sched::encode_migration_guard_state;
    using ker::mod::sched::MIGRATION_CPU_INVALID;
    using ker::mod::sched::migration_disable_transition;
    using ker::mod::sched::migration_enable_transition;
    using ker::mod::sched::migration_guard_target_cpu;
    using ker::mod::sched::MigrationGuardState;
    using ker::mod::sched::MigrationGuardTransitionError;

    MigrationGuardState state{.depth = 0, .owner_cpu = MIGRATION_CPU_INVALID};
    auto transition = migration_disable_transition(state, 2);
    KEXPECT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    KEXPECT_TRUE(transition.outermost);
    KEXPECT_EQ(transition.state.depth, 1U);
    KEXPECT_EQ(migration_guard_target_cpu(transition.state, 5, 8), 2ULL);

    transition = migration_disable_transition(transition.state, 2);
    KEXPECT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    KEXPECT_FALSE(transition.outermost);
    KEXPECT_EQ(transition.state.depth, 2U);
    KEXPECT_EQ(decode_migration_guard_state(encode_migration_guard_state(transition.state)).owner_cpu, 2U);

    KEXPECT_EQ(migration_enable_transition(transition.state, 1).error, MigrationGuardTransitionError::WRONG_CPU);
    transition = migration_enable_transition(transition.state, 2);
    KEXPECT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    transition = migration_enable_transition(transition.state, 2);
    KEXPECT_EQ(transition.error, MigrationGuardTransitionError::NONE);
    KEXPECT_TRUE(transition.outermost);
    KEXPECT_EQ(transition.state.owner_cpu, MIGRATION_CPU_INVALID);
}

// The real CLI -> APIC arm -> STI/HLT boundary cannot run during boot KTEST,
// before the scheduler is started. Exercise its cancellation state machine
// separately; host source tests enforce the hardware-operation ordering.
static ker::mod::sched::task::Task g_scheduler_wait_cancel_task;  // NOLINT

KTEST(Sched, SchedulerWaitCancellationState) {
    using ker::mod::sched::scheduler_wait_should_cancel;
    using ker::mod::sched::SchedulerHaltWaitKind;

    auto& task = g_scheduler_wait_cancel_task;
    task.wakeup_pending.store(false, std::memory_order_relaxed);
    task.process_exit_requested.store(false, std::memory_order_relaxed);
    task.wants_block = true;
    task.wake_at_us = 0;

    KEXPECT_FALSE(scheduler_wait_should_cancel(&task, SchedulerHaltWaitKind::YIELD));
    KEXPECT_FALSE(scheduler_wait_should_cancel(&task, SchedulerHaltWaitKind::BLOCK));

    task.wants_block = false;
    KEXPECT_TRUE(scheduler_wait_should_cancel(&task, SchedulerHaltWaitKind::BLOCK));

    task.wants_block = true;
    task.wakeup_pending.store(true, std::memory_order_release);
    KEXPECT_TRUE(scheduler_wait_should_cancel(&task, SchedulerHaltWaitKind::YIELD));
    KEXPECT_FALSE(task.wakeup_pending.load(std::memory_order_acquire));

    task.process_exit_requested.store(true, std::memory_order_release);
    KEXPECT_TRUE(scheduler_wait_should_cancel(&task, SchedulerHaltWaitKind::PROCESS_PARK));
    task.process_exit_requested.store(false, std::memory_order_relaxed);
}

KTEST(SchedulerWake, RemoteExecProxyPreservesDeferredSwitch) {
    using ker::mod::sched::event_wake_should_cancel_deferred_switch;
    using ker::mod::sched::EventWakeDeferredSwitch;

    KEXPECT_FALSE(event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch::PRESERVE, false));
    KEXPECT_TRUE(event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch::CANCEL, false));
    KEXPECT_FALSE(event_wake_should_cancel_deferred_switch(EventWakeDeferredSwitch::CANCEL, true));
}

KTEST(SchedulerHandoff, RunnableEventTokenSurvivesCommit) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_handoff_preserves_runnable_event_token());
}

KTEST(SchedulerHandoff, ExceptionWaitTokenClosesPreparkRace) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_exception_wait_token_closes_prepark_race());
}

KTEST(SchedulerHandoff, ReservedWakePrecedesCommit) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_reserved_wake_precedes_handoff_commit());
}

KTEST(SchedulerWake, ConcurrentRescheduleRequestsAreSerialized) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_concurrent_reschedule_requests_are_serialized());
}

KTEST(SchedulerRuntime, RuntimeDeltaSaturates) { KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_runtime_delta_saturates()); }

KTEST(SchedulerMigration, PreservesHotProcessMigrationPolicy) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_migration_policy_preserves_hot_process_migration());
}

KTEST(SchedulerMigration, HeapScanRemovalRepairsStaleIndex) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_heap_scan_removal_repairs_stale_index());
}

KTEST(SchedulerMigration, LoadBalanceNudgeNeedsProcessBacklog) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_load_balance_nudge_needs_process_backlog());
}

KTEST(SchedulerMigration, IdleStealScanExpandsForLargeBacklog) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_idle_steal_scan_expands_for_large_backlog());
}

KTEST(SchedulerMigration, EffectivelyIdleCurrentAcceptsRebalanceProbe) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_effectively_idle_current_accepts_rebalance_probe());
}

KTEST(SchedulerMetrics, LoadAverageWaitChannelPolicy) { KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_loadavg_wait_channel_policy()); }

KTEST(SchedulerDeferredSwitch, YieldBitRequiresSchedYieldChannel) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_deferred_yield_requires_sched_yield_channel());
}

KTEST(SchedulerResume, LazyExecutableFileRangeIsValid) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_lazy_executable_resume_range_policy());
}

// ---------------------------------------------------------------------------
// Deterministic scheduler transition model.
//
// KTEST parks secondary CPUs, so live SMP interleavings are not available in
// this environment.  The shared freestanding model explores the same bounded
// fake-CPU transitions as the host test and checks invariants after every step.
// ---------------------------------------------------------------------------

KTEST(SchedulerTransitionModel, EventBeforeParkWake) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::EVENT_BEFORE_PARK);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, EventAfterParkWake) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::EVENT_AFTER_PARK);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, DuplicateReschedule) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::DUPLICATE_RESCHEDULE);
    KEXPECT_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
    KEXPECT_TRUE(RESULT.rejected > 0U);
}

KTEST(SchedulerTransitionModel, HandoffWake) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::HANDOFF_WAKE);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, LocalMigration) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::LOCAL_MIGRATION);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, TwoCpuMigration) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::TWO_CPU_MIGRATION);
    KEXPECT_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
    KEXPECT_TRUE(RESULT.ipis_sent > 0U);
    KEXPECT_EQ(RESULT.ipis_sent, RESULT.ipis_delivered);
}

KTEST(SchedulerTransitionModel, PinRejection) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::PIN_REJECTION);
    KEXPECT_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
    KEXPECT_TRUE(RESULT.rejected > 0U);
}

KTEST(SchedulerTransitionModel, DomainRejection) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::DOMAIN_REJECTION);
    KEXPECT_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
    KEXPECT_TRUE(RESULT.rejected > 0U);
}

KTEST(SchedulerTransitionModel, PreemptPendingAndService) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::PREEMPT_PENDING);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, ExitDeadAndGc) {
    auto const RESULT = ker::mod::sched::model::run_scenario(ker::mod::sched::model::Scenario::EXIT_GC);
    KREQUIRE_TRUE(RESULT.passed);
    KEXPECT_EQ(RESULT.violations, 0ULL);
    KEXPECT_TRUE(RESULT.steps > 0U);
}

KTEST(SchedulerTransitionModel, BoundedExplorationIsReplayable) {
    auto const FIRST = ker::mod::sched::model::explore_bounded();
    KEXPECT_TRUE(FIRST.passed);
    KEXPECT_EQ(FIRST.violations, 0ULL);
    KEXPECT_TRUE(FIRST.states > 0U);
    KEXPECT_TRUE(FIRST.traces > 0U);
    KEXPECT_TRUE(FIRST.max_depth > 0U);
    KEXPECT_NE(FIRST.replay_seed, 0ULL);

    auto const REPLAY = ker::mod::sched::model::explore_bounded(FIRST.replay_seed);
    KEXPECT_TRUE(REPLAY.passed);
    KEXPECT_EQ(REPLAY.violations, FIRST.violations);
    KEXPECT_EQ(REPLAY.states, FIRST.states);
    KEXPECT_EQ(REPLAY.traces, FIRST.traces);
    KEXPECT_EQ(REPLAY.max_depth, FIRST.max_depth);
    KEXPECT_EQ(REPLAY.replay_seed, FIRST.replay_seed);
}

KTEST(SchedulerTransitionModel, NegativeInvariantChecksAreDetected) {
    constexpr uint64_t COVERAGE = ker::mod::sched::model::negative_invariant_coverage();
    KEXPECT_EQ(COVERAGE & ker::mod::sched::model::REQUIRED_INVARIANTS, ker::mod::sched::model::REQUIRED_INVARIANTS);
    KEXPECT_TRUE(ker::mod::sched::model::negative_invariant_detection());
}

KTEST(SchedulerTransitionValidator, DetectsAndClearsCorruption) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_transition_validator_detects_corruption());
}

KTEST(ContextSwitch, RepairsStaleProcessSyscallResume) {
    KEXPECT_TRUE(ker::mod::sys::context_switch::context_switch_selftest_repair_stale_process_syscall_resume());
}

// ---------------------------------------------------------------------------
// RunHeap ordering test - push tasks with known deadlines, verify min-order.
// ---------------------------------------------------------------------------

// File-scope to avoid __cxa_guard_acquire (unavailable in freestanding kernel).
// Task embeds a 512-byte FxState; keeping these off the stack is also safer.
static constexpr int HEAP_TEST_N = 5;
static ker::mod::sched::task::Task g_heap_tasks[HEAP_TEST_N];  // NOLINT

KTEST(Sched, RunHeapOrder) {
    using namespace ker::mod::sched;

    constexpr int N = HEAP_TEST_N;
    auto* tasks = static_cast<task::Task*>(g_heap_tasks);

    // Reset relevant fields; static zero-init leaves heapIndex=0 which
    // RunHeap treats as "already in heap", so force -1 explicitly.
    constexpr int64_t DEADLINES[N] = {500, 100, 300, 200, 400};
    for (int i = 0; i < N; ++i) {
        tasks[i].heap_index = -1;
        tasks[i].vdeadline = DEADLINES[i];
        tasks[i].vruntime = 0;
    }

    RunHeap heap{};
    heap.init();

    for (int i = 0; i < N; ++i) {
        KEXPECT_TRUE(heap.insert(&tasks[i]));
    }
    KEXPECT_EQ(static_cast<int>(heap.size), N);

    // Pop in deadline order; each successive peekMin must be >= the previous.
    int64_t prev_dl = INT64_MIN;
    for (int i = 0; i < N; ++i) {
        task::Task* t = heap.peek_min();
        KREQUIRE_NE(t, nullptr);
        KEXPECT_TRUE(t->vdeadline >= prev_dl);
        prev_dl = t->vdeadline;
        KEXPECT_TRUE(heap.remove(t));
    }
    KEXPECT_EQ(static_cast<int>(heap.size), 0);
}
