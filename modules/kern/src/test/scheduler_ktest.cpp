#include <cstdint>
#include <platform/sched/run_heap.hpp>
#include <platform/sched/scheduler.hpp>
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
auto scheduler_selftest_handoff_preserves_runnable_event_token() -> bool;
auto scheduler_selftest_stalled_waitpid_claim_recovery_is_leased() -> bool;
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

KTEST(SchedulerWaitpid, StalledCompletionClaimRecoveryIsLeased) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_stalled_waitpid_claim_recovery_is_leased());
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
