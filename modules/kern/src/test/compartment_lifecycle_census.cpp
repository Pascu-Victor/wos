#include <test/compartment_lifecycle_census.hpp>

#ifdef WOS_SELFTEST
#include <atomic>
#include <limits>
#include <platform/sched/compartment_lifecycle.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/mount_namespace.hpp>

namespace ker::test::lifecycle60 {
namespace {
namespace sched = mod::sched;
namespace lifetime = mod::mm::file_mapping;
struct CensusState {
    // All fixed storage exists before the measured baseline; none owns refs.
    OwnerCensus a{}, b{}, host{}, provisional{}, driver{};
    CensusGateReport report{};
    CensusScalars provisional_scalars{};
    uint64_t host_pid{}, driver_pid{};
    bool poisoned{};
};
CensusState state{};

auto outcome(GateResult result, CensusReason reason) noexcept -> GateResult {
    auto& report = state.report;
    report.result = result;
    report.reason = reason;
    auto* counter = result == GateResult::ACCEPT ? &report.accepts : result == GateResult::WAIT ? &report.waits : &report.refuses;
    if (*counter == std::numeric_limits<uint64_t>::max()) {
        state.poisoned = true;
        report.result = GateResult::REFUSE;
        report.reason = CensusReason::REPORT_EXHAUSTED;
        return GateResult::REFUSE;
    }
    ++*counter;
    if (result == GateResult::REFUSE) state.poisoned = true;
    return result;
}

auto begin(const CensusLease& lease, Boundary boundary, uint64_t index) noexcept -> bool {
    auto& report = state.report;
    report.boundary = boundary;
    report.index = index;
    report.cookie = lease.cookie();
    report.owner_pair_stable = report.scalar_pair_stable = false;
    if (state.poisoned) return false;
    if (report.calls == std::numeric_limits<uint64_t>::max()) {
        (void)outcome(GateResult::REFUSE, CensusReason::REPORT_EXHAUSTED);
        return false;
    }
    ++report.calls;
    auto* actor = lease.subject();
    if (!lease || actor == nullptr || lease.cookie() == 0) {
        (void)outcome(GateResult::REFUSE, CensusReason::INVALID_PERMISSION);
        return false;
    }
    if (actor->pid == 0 || actor->type != sched::task::TaskType::PROCESS || actor->is_thread ||
        actor->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        (void)outcome(GateResult::REFUSE, CensusReason::WRONG_ACTOR);
        return false;
    }
    return true;
}

void collect_scalars(const CensusLease& lease, CensusScalars& out) noexcept {
    auto& actor = *lease.subject();
    out.actor_pid = actor.pid;
    out.actor_references = actor.ref_count.load(std::memory_order_acquire);
    out.actor_thread_owners = actor.resource_thread_owners.load(std::memory_order_acquire);
    out.actor_cpu_threads = actor.resource_cpu_threads.load(std::memory_order_acquire);
    out.actor_shares_root = actor.shares_user_pagemap.load(std::memory_order_acquire);
    out.core_count = mod::smt::get_core_count();
    out.active_registry = sched::get_active_task_count();
    out.scoped_pid_registry = sched::get_scoped_pid_task_count();
    out.active_threads = sched::threading::get_active_thread_count();
    out.pid_namespaces = sched::pid_ns::live_namespace_count();
    out.mount_namespaces = vfs::mount_ns::live_namespace_count();
    out.ipc_namespaces = sched::ipc::live_namespace_count();
    auto const compartment = sched::compartment::status();
    out.compartment_pending = compartment.pending;
    out.compartment_completed = compartment.completed;
    out.compartment_exit_requests = compartment.exit_requests;
    out.native_retirements_pending = sched::task::native_unpublished_retirements_pending();
    auto const trace = sched::get_scheduler_trace_stats(0);
    out.deferred_queued = trace.gc_deferred_queued;
    out.deferred_completed = trace.gc_deferred_completed;
    out.deferred_depth = trace.gc_deferred_depth;
    // Constant bound; no linked-list walk when a dead list is empty. One
    // explicit prefix sample corroborates zero, not the total dead population.
    for (size_t cpu = 0; cpu < CENSUS_CPUS; ++cpu) {
        auto& row = out.cpu[cpu];
        row = {};
        if (cpu >= out.core_count) continue;
        auto const queue = sched::get_run_queue_stats(cpu);
        row.runnable_heap = queue.active_task_count;
        row.dead = queue.expired_task_count;
        row.waiting = queue.wait_queue_count;
        row.timers = queue.timer_queue_depth;
        uint64_t pid = 0;
        uint32_t references = 0;
        row.sampled_dead = sched::get_expired_task_refcounts(cpu, &pid, &references, 1);
        row.first_dead_pid = pid;
        row.first_dead_references = references;
    }
    out.executor = lifetime::LifetimeRetirementExecutor::snapshot();
    mod::mm::phys::get_alloc_stats_snapshot(out.physical);
}

auto same_executor(const lifetime::LifetimeRetirementSnapshot& a, const lifetime::LifetimeRetirementSnapshot& b) noexcept -> bool {
    return a.initialized == b.initialized && a.available == b.available && a.admission_open == b.admission_open &&
           a.stop_requested == b.stop_requested && a.loop_finished == b.loop_finished && a.outstanding == b.outstanding &&
           a.reserved == b.reserved && a.ready == b.ready && a.running == b.running && a.completed == b.completed &&
           a.cancelled == b.cancelled && a.close_errors == b.close_errors && a.protocol_errors == b.protocol_errors &&
           a.first_close_result == b.first_close_result && a.completed_storage_slots == b.completed_storage_slots &&
           a.observer_handles == b.observer_handles && a.exhausted_slots == b.exhausted_slots;
}

auto same_scalars(const CensusScalars& a, const CensusScalars& b) noexcept -> bool {
    // Run/wait/timer population is descriptive, not a total active-Task count.
    // It may fluctuate across the two samples; dead prefixes must agree and
    // be zero below. Physical pool activity is also deliberately descriptive.
    for (size_t i = 0; i < CENSUS_CPUS; ++i)
        if (a.cpu[i].dead != b.cpu[i].dead || a.cpu[i].sampled_dead != b.cpu[i].sampled_dead ||
            a.cpu[i].first_dead_pid != b.cpu[i].first_dead_pid || a.cpu[i].first_dead_references != b.cpu[i].first_dead_references)
            return false;
    return a.actor_pid == b.actor_pid && a.actor_references == b.actor_references && a.actor_thread_owners == b.actor_thread_owners &&
           a.actor_cpu_threads == b.actor_cpu_threads && a.actor_shares_root == b.actor_shares_root && a.core_count == b.core_count &&
           a.active_registry == b.active_registry && a.scoped_pid_registry == b.scoped_pid_registry &&
           a.active_threads == b.active_threads && a.pid_namespaces == b.pid_namespaces && a.mount_namespaces == b.mount_namespaces &&
           a.ipc_namespaces == b.ipc_namespaces && a.compartment_pending == b.compartment_pending &&
           a.compartment_completed == b.compartment_completed && a.compartment_exit_requests == b.compartment_exit_requests &&
           a.native_retirements_pending == b.native_retirements_pending && a.deferred_queued == b.deferred_queued &&
           a.deferred_completed == b.deferred_completed && a.deferred_depth == b.deferred_depth && same_executor(a.executor, b.executor);
}

auto valid_counters(const OwnerCensus& value) noexcept -> bool {
    // Owner Stamp.valid also encodes a concurrent sample change, so it is a
    // WAIT via complete()/unchanged_complete(), never proof of sticky failure.
    // Storage.valid, in contrast, is explicitly sticky arithmetic corruption.
    return value.storage_before.valid && value.storage_after.valid;
}

auto collect_pair(const CensusLease& lease) noexcept -> GateResult {
    // No short circuit: preserve both actual observations, even on churn.
    bool const first = collect_into(lease, state.a);
    collect_scalars(lease, state.report.before);
    collect_scalars(lease, state.report.after);
    bool const second = collect_into(lease, state.b);
    if (!valid_counters(state.a) || !valid_counters(state.b)) return outcome(GateResult::REFUSE, CensusReason::COUNTER_INVALID);
    state.report.owner_pair_stable = first && second && unchanged_complete(state.a, state.b);
    if (!state.report.owner_pair_stable) return outcome(GateResult::WAIT, CensusReason::OWNER_CHANGED);
    state.report.scalar_pair_stable = same_scalars(state.report.before, state.report.after);
    if (!state.report.scalar_pair_stable) return outcome(GateResult::WAIT, CensusReason::SCALAR_CHANGED);
    return GateResult::ACCEPT;
}

auto no_shared_private_debt(const OwnerCensus& value) noexcept -> bool {
    return complete(value) && value.mounts.initializing_private == 0 && value.mounts.published_private == 0 &&
           value.mounts.retiring_private == 0 && value.mounts.preparation_storage == 0 && value.mounts.preparing_operations == 0 &&
           value.services.remote_creations == 0 && value.population.reservations == 0 && value.population.executor_pending == 0 &&
           value.ipc.private_segments == 0 && value.ipc.private_pages == 0 && value.ipc.publishing_attachments == 0 &&
           value.descriptors.reserved == 0 && value.descriptors.executor_pending == 0 && value.descriptors.recovery_presence == 0;
}

auto no_driver_child_debt(const OwnerCensus& value) noexcept -> bool {
    auto const& child = value.driver_children;
    // A driver has its legitimate parent link throughout; do not zero that.
    return child.children == 0 && child.publishing_children == 0 && child.live_children == 0 && child.zombie_children == 0 &&
           child.tracees == 0 && child.events == 0 && child.claimed_events == 0 && child.waiters == 0 && child.notified_waiters == 0 &&
           !child.tracer_link && !child.own_wait_registration && !child.own_claim;
}

auto driver_fds_settled(const vfs::SelftestFdCensus& value) noexcept -> bool {
    if (!value.complete || value.installed != 9 || value.distinct_files != 9 || value.reserved != 0 || value.executor_pending != 0 ||
        value.recovery_presence != 0)
        return false;
    uint64_t found = 0;
    for (auto const& slot : value.slots) {
        if (slot.reserved) return false;
        if (slot.file_identity == 0) {
            if (slot.file_references != 0 || slot.multiplicity != 0) return false;
            continue;
        }
        if (!slot.cloexec || slot.file_references != 1 || slot.multiplicity != 1) return false;
        ++found;
    }
    return found == 9;
}

auto providers_settled(const CensusScalars& value) noexcept -> bool {
    if (value.core_count != CENSUS_CPUS || value.pid_namespaces != 0 || value.mount_namespaces != 0 || value.ipc_namespaces != 0 ||
        value.compartment_pending != 0 || value.native_retirements_pending || value.deferred_depth != 0 ||
        value.deferred_queued != value.deferred_completed)
        return false;
    for (auto const& cpu : value.cpu)
        if (cpu.dead != 0 || cpu.sampled_dead != 0) return false;
    return true;
}

auto check_settled(bool driver) noexcept -> GateResult {
    auto const& scalar = state.report.after;
    auto const& executor = scalar.executor;
    if (scalar.actor_references == 0 || scalar.actor_references == std::numeric_limits<uint32_t>::max())
        return outcome(GateResult::REFUSE, CensusReason::WRONG_ACTOR);
    if (!executor.initialized || !executor.available || !executor.admission_open || executor.stop_requested || executor.loop_finished)
        return outcome(GateResult::REFUSE, CensusReason::EXECUTOR_UNAVAILABLE);
    if (executor.close_errors != 0 || executor.protocol_errors != 0 || executor.first_close_result != 0 || executor.exhausted_slots != 0)
        return outcome(GateResult::REFUSE, CensusReason::EXECUTOR_ERROR);
    if (executor.outstanding != 0 || executor.reserved != 0 || executor.ready != 0 || executor.running != 0 ||
        executor.completed_storage_slots != 0 || executor.observer_handles != 0 || !providers_settled(scalar))
        return outcome(GateResult::WAIT, CensusReason::PROVIDER_DEBT);
    if (!no_shared_private_debt(state.b)) return outcome(GateResult::WAIT, CensusReason::PRIVATE_DEBT);
    if (driver) {
        if (scalar.actor_thread_owners != 0 || scalar.actor_cpu_threads != 0 || scalar.actor_shares_root)
            return outcome(GateResult::REFUSE, CensusReason::WRONG_ACTOR);
        if (!no_driver_child_debt(state.b)) return outcome(GateResult::WAIT, CensusReason::DRIVER_CHILD_DEBT);
        if (!driver_fds_settled(state.b.descriptors)) return outcome(GateResult::WAIT, CensusReason::DRIVER_FD_DEBT);
    }
    return GateResult::ACCEPT;
}

auto same_scalar_baseline(const CensusScalars& a, const CensusScalars& b) noexcept -> bool {
    // Both operands independently passed their complete drained pair. Do not
    // compare monotonic operation totals, run/wait placement, or physical pools.
    return a.actor_pid == b.actor_pid && a.actor_references == b.actor_references && a.actor_thread_owners == b.actor_thread_owners &&
           a.actor_cpu_threads == b.actor_cpu_threads && a.actor_shares_root == b.actor_shares_root && a.core_count == b.core_count &&
           a.active_registry == b.active_registry && a.scoped_pid_registry == b.scoped_pid_registry && a.active_threads == b.active_threads;
}

auto matches_baseline(const OwnerCensus& owners, const CensusScalars& scalar) noexcept -> bool {
    return same_baseline_debt(owners, state.b) && same_scalar_baseline(scalar, state.report.after);
}
}  // namespace

auto census_gate_report() noexcept -> const CensusGateReport& { return state.report; }
auto census_owner_report() noexcept -> const OwnerCensus& { return state.b; }

auto validate_host_baseline(const CensusLease& lease) noexcept -> GateResult {
    if (!begin(lease, Boundary::BEFORE_DRIVER, 0)) return GateResult::REFUSE;
    if (state.report.provisional_driver_valid || state.report.driver_baseline_valid)
        return outcome(GateResult::REFUSE, CensusReason::WRONG_BOUNDARY);
    if (collect_pair(lease) != GateResult::ACCEPT) return state.report.result;
    if (check_settled(false) != GateResult::ACCEPT) return state.report.result;
    if (state.report.host_baseline_valid) {
        if (!matches_baseline(state.host, state.report.host_baseline)) return outcome(GateResult::WAIT, CensusReason::BASELINE_MISMATCH);
    } else {
        state.host = state.b;
        state.report.host_baseline = state.report.after;
        state.host_pid = lease.subject()->pid;
        state.report.host_baseline_valid = true;
    }
    return outcome(GateResult::ACCEPT, CensusReason::NONE);
}

auto validate_quiescent_census(const CensusLease& lease, const AttemptView& view, Boundary boundary, bool establish_baseline) noexcept
    -> GateResult {
    if (!begin(lease, boundary, view.index)) return GateResult::REFUSE;
    if (!state.report.host_baseline_valid) return outcome(GateResult::REFUSE, CensusReason::HOST_BASELINE_MISSING);
    if ((boundary != Boundary::BEFORE_CREATE && boundary != Boundary::AFTER_FAILURE && boundary != Boundary::AFTER_WAIT) ||
        view.index >= ATTEMPTS || view.cookie == 0 || view.cookie != lease.cookie() || view.warmup != (view.index < WARMUPS) ||
        establish_baseline != (boundary == Boundary::BEFORE_CREATE && view.index == WARMUPS))
        return outcome(GateResult::REFUSE, CensusReason::WRONG_BOUNDARY);
    if (lease.subject()->pid == state.host_pid || (state.driver_pid != 0 && lease.subject()->pid != state.driver_pid))
        return outcome(GateResult::REFUSE, CensusReason::WRONG_ACTOR);
    if ((boundary == Boundary::AFTER_FAILURE || boundary == Boundary::AFTER_WAIT) && !view.task_debt_empty)
        return outcome(GateResult::WAIT, CensusReason::TASK_DEBT);
    if (!view.task_debt_empty) return outcome(GateResult::REFUSE, CensusReason::TASK_DEBT);
    if (collect_pair(lease) != GateResult::ACCEPT) return state.report.result;
    if (check_settled(true) != GateResult::ACCEPT) return state.report.result;
    if (!state.report.provisional_driver_valid) {
        if (boundary != Boundary::BEFORE_CREATE || view.index != 0)
            return outcome(GateResult::REFUSE, CensusReason::DRIVER_BASELINE_MISSING);
        state.provisional = state.b;
        state.provisional_scalars = state.report.after;
        state.driver_pid = lease.subject()->pid;
        state.report.provisional_driver_valid = true;
    }
    if (!matches_baseline(state.provisional, state.provisional_scalars)) return outcome(GateResult::WAIT, CensusReason::BASELINE_MISMATCH);
    if (establish_baseline) {
        if (state.report.driver_baseline_valid) return outcome(GateResult::REFUSE, CensusReason::WRONG_BOUNDARY);
        state.driver = state.b;
        state.report.driver_baseline = state.report.after;
        state.report.driver_baseline_valid = true;
    }
    if (view.index >= WARMUPS) {
        if (!state.report.driver_baseline_valid) return outcome(GateResult::REFUSE, CensusReason::DRIVER_BASELINE_MISSING);
        if (!matches_baseline(state.driver, state.report.driver_baseline))
            return outcome(GateResult::WAIT, CensusReason::BASELINE_MISMATCH);
    }
    return outcome(GateResult::ACCEPT, CensusReason::NONE);
}

auto validate_final_census(const CensusLease& lease, const CampaignSummary& summary) noexcept -> GateResult {
    if (!begin(lease, Boundary::AFTER_DRIVER, ATTEMPTS)) return GateResult::REFUSE;
    if (!state.report.host_baseline_valid || !state.report.driver_baseline_valid)
        return outcome(GateResult::REFUSE, CensusReason::HOST_BASELINE_MISSING);
    if (lease.subject()->pid != state.host_pid) return outcome(GateResult::REFUSE, CensusReason::WRONG_ACTOR);
    if (summary.warmups != WARMUPS || summary.faults != FAULTS || summary.successes != SUCCESSES || summary.consumed_faults != FAULTS ||
        summary.actual_wait9 != WARMUPS + SUCCESSES || summary.last_cookie != ATTEMPTS || !summary.original_init_reaped_driver)
        return outcome(GateResult::REFUSE, CensusReason::SUMMARY_MISMATCH);
    if (!summary.all_attempt_task_debts_empty) return outcome(GateResult::WAIT, CensusReason::TASK_DEBT);
    if (collect_pair(lease) != GateResult::ACCEPT) return state.report.result;
    if (check_settled(false) != GateResult::ACCEPT) return state.report.result;
    if (!matches_baseline(state.host, state.report.host_baseline)) return outcome(GateResult::WAIT, CensusReason::BASELINE_MISMATCH);
    return outcome(GateResult::ACCEPT, CensusReason::NONE);
}
}  // namespace ker::test::lifecycle60
#endif
