#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TASK_HPP = ROOT / "modules/kern/src/platform/sched/task.hpp"
WAITPID_CPP = ROOT / "modules/kern/src/syscalls_impl/process/waitpid.cpp"
WAITPID_HPP = ROOT / "modules/kern/src/syscalls_impl/process/waitpid.hpp"
CHILD_EVENTS_CPP = ROOT / "modules/kern/src/syscalls_impl/process/child_events.cpp"
CHILD_EVENTS_HPP = ROOT / "modules/kern/src/syscalls_impl/process/child_events.hpp"
EXIT_CPP = ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp"
PROCESS_CPP = ROOT / "modules/kern/src/syscalls_impl/process/process.cpp"
EXEC_CPP = ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp"
SCHEDULER_CPP = ROOT / "modules/kern/src/platform/sched/scheduler.cpp"
SIGNAL_CPP = ROOT / "modules/kern/src/platform/sys/signal.cpp"
PTRACE_CPP = ROOT / "modules/kern/src/platform/debug/ptrace.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules/kern/src/net/wki/remote_compute.cpp"
CHILD_EVENTS_KTEST = ROOT / "modules/kern/src/test/child_events_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def forbid_tokens(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    starts: set[int] = set()
    for needle in [
        f"auto {name}(",
        f"inline auto {name}(",
        f"void {name}(",
        f"inline void {name}(",
        f"[[nodiscard]] auto {name}(",
        f"[[noreturn]] void {name}(",
        f'extern "C" auto {name}(',
        f'extern "C" void {name}(',
    ]:
        candidate = source.find(needle)
        while candidate >= 0:
            starts.add(candidate)
            candidate = source.find(needle, candidate + 1)
    for start in sorted(starts):
        brace = source.find("{", start)
        semicolon = source.find(";", start)
        if brace >= 0 and (semicolon < 0 or brace < semicolon):
            end = find_matching_brace(source, brace)
            return source[brace + 1 : end]
    fail(f"{name} function not found")


def require_order(source: str, before: str, after: str, context: str) -> None:
    before_pos = source.find(before)
    after_pos = source.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_task_owns_explicit_topology_events_and_waiters() -> None:
    task = TASK_HPP.read_text()
    require_tokens(
        task,
        [
            "enum class ChildMembershipState",
            "PUBLISHING",
            "LIVE",
            "ZOMBIE",
            "enum class ChildEventKind",
            "PTRACE_STOP",
            "JOB_CONTROL_STOP",
            "CONTINUED",
            "struct ChildEvent",
            "ChildEventClaim* claimed_by{}",
            "ChildEventClaim child_claim{this}",
            "ChildEventClaim(const ChildEventClaim&) = delete",
            "ChildEventClaim(ChildEventClaim&&) = delete",
            "uint64_t claim_cookie{}",
            "uint64_t process_group{}",
            "uint64_t user_time_us{}",
            "uint64_t system_time_us{}",
            "struct ChildWaitRegistration",
            "std::atomic<ChildMembershipState> child_membership_state",
            "Task* child_parent{}",
            "Task* child_list_head{}",
            "ChildEvent* child_event_head{}",
            "ChildWaitRegistration child_wait_registration{}",
            "ChildEvent child_parent_exit_event{}",
            "ChildEvent child_tracer_exit_event{}",
            "ChildEvent child_ptrace_stop_event{}",
            "ChildEvent child_job_stop_event{}",
            "ChildEvent child_continued_event{}",
            "Task* child_tracer{}",
            "Task* child_tracee_head{}",
        ],
        "Task-owned process lifecycle layout",
    )
    forbid_tokens(
        task,
        [
            "waiting_for_pid",
            "waitpid_publish_pending",
            "waitpid_completion_claimed",
            "waitpid_last_repair_us",
            "waitpid_claim_observed_us",
            "awaitee_on_exit",
            "exit_waiters_lock",
            "waited_on",
        ],
        "legacy scheduler-owned waitpid Task state",
    )


def test_lifecycle_lock_owns_snapshot_queue_and_claims() -> None:
    source = CHILD_EVENTS_CPP.read_text()
    header = CHILD_EVENTS_HPP.read_text()
    require_tokens(
        source,
        [
            "Spinlock g_lifecycle_lock",
            "std::atomic<uint64_t> g_event_sequence{1}",
            "std::atomic<uint64_t> g_claim_cookie{1}",
            "queue_insert_locked(owner, event)",
            "event.process_group = JOB.internal.pgid",
            "event.group_origin = JOB.internal.pgid",
            "*local_group = JOB.visible.pgid",
            "event.user_time_us = sched_task::task_rusage_user_time_us(subject)",
            "event.system_time_us = sched_task::task_rusage_system_time_us(subject)",
            "event.status = status",
            "EVENT->claimed_by = &claim",
            "EVENT->claim_cookie = g_claim_cookie.fetch_add",
            "clear_event_claim_locked(*EVENT)",
            "wake_no_child_waiters(owner)",
        ],
        "serialized child event snapshots and claims",
    )
    require_tokens(
        header,
        [
            "enum class ProbeResult",
            "CLAIMED",
            "WOULD_BLOCK",
            "NO_CHILD",
            "claim_or_register",
            "release_claim",
            "commit_claim",
        ],
        "child-event claim API",
    )

    enqueue = function_body(source, "enqueue_event_locked")
    forbid_tokens(enqueue, ["new ", "delete ", "kmalloc", "wake_task", "copy_value_to_task", "dbg::"], "locked event enqueue")
    claim = function_body(source, "claim_locked")
    require_order(claim, "claim.event != nullptr", "first_matching_event_locked", "busy records preserve their existing claim")
    forbid_tokens(claim, ["new ", "delete ", "get_current_task", "find_task_by_pid", "wake_task"], "executor-independent claim")
    require_tokens(function_body(source, "claim_or_register"), ["claim_locked(owner, waiter.child_claim"], "shared local claim path")
    clear = function_body(source, "clear_event_claim_locked")
    require_order(clear, "CLAIMANT->event = nullptr", "drop_lifetime_ref(WAITER)", "clear record before releasing its local waiter")
    commit = function_body(source, "commit_claim")
    require_order(commit, "clear_event_claim_locked(*EVENT)", "remove_event_locked(*EVENT)", "claim retirement before event removal")
    require_order(source, "g_lifecycle_lock.unlock_irqrestore(FLAGS);", "deliver_wake(wake);", "wakes must happen outside lifecycle lock")


def test_publication_and_reparenting_are_explicit_transactions() -> None:
    source = CHILD_EVENTS_CPP.read_text()
    begin = function_body(source, "begin_publication")
    commit = function_body(source, "commit_publication")
    abort = function_body(source, "abort_publication")
    prepare_exit = function_body(source, "prepare_process_exit")
    require_tokens(
        begin,
        [
            "child.child_publication_established = true",
            "child.child_parent = PARENT",
            "child_list_insert_locked(*PARENT, child)",
            "ChildMembershipState::PUBLISHING",
        ],
        "two-phase child publication begin",
    )
    require_tokens(commit, ["ChildMembershipState::LIVE", "child.child_publication_established"], "child publication commit")
    require_tokens(
        abort,
        [
            "child_list_remove_locked(*parent, child)",
            "ChildMembershipState::UNLINKED",
            "wake_no_child_waiters(*parent)",
        ],
        "publication rollback and waiter notification",
    )
    require_tokens(
        prepare_exit,
        [
            "cancel_wait(process)",
            "process.child_list_head",
            "transfer_parent_events_locked(process, init, *child)",
            "unlink_tracer_locked(*tracee, true)",
        ],
        "explicit child/event reparenting",
    )

    process = PROCESS_CPP.read_text()
    exec_source = EXEC_CPP.read_text()
    for source_text, context in [(process, "fork/spawn"), (exec_source, "exec creation")]:
        require_tokens(source_text, ["child_events::begin_publication", "child_events::commit_publication"], context)
        require_tokens(source_text, ["child_events::abort_publication"], f"{context} rollback")


def test_waitpid_is_a_current_context_claim_usercopy_commit_loop() -> None:
    source = WAITPID_CPP.read_text()
    header = WAITPID_HPP.read_text()
    selector = function_body(CHILD_EVENTS_CPP.read_text(), "public_wait_selector")
    syscall = function_body(source, "wos_proc_waitpid")
    copy = function_body(source, "copy_outputs")
    lifecycle = CHILD_EVENTS_CPP.read_text()
    require_tokens(function_body(lifecycle, "selector_matches"), ["origin == selector.group_origin"], "group origin boundary")
    require_tokens(function_body(lifecycle, "claim_locked"), ["claimed.group_origin = EVENT->group_origin"], "immutable claimed origin")
    require_tokens(lifecycle, ["registration.selector = selector", "event->process_group, event->group_origin"], "retained group selection")
    for operation in ["release_claim", "commit_claim"]:
        body = function_body(lifecycle, operation)
        require_tokens(body, ["ChildEvent* const EVENT = claim.event"], "live claim storage")
        forbid_tokens(body, ["EVENT = claimed.node"], "borrowed claim pointer dereference")
        require_order(body, "claimed.node", "EVENT->queued", "current identity before event access")

    require_tokens(
        selector,
        [
            "pid > 0",
            "pid == -1",
            "SELECT_ANY",
            "pid == 0",
            "selector_for_current_process_group(owner)",
            "std::numeric_limits<int64_t>::min()",
            "selector_for_process_group(owner, GROUP)",
        ],
        "POSIX waitpid selectors",
    )
    require_tokens(
        syscall,
        [
            "events::acquire_process_owner(*waiter)",
            "events::OPTION_NOHANG",
            "events::OPTION_NOWAIT",
            "events::claim_or_register(*OWNER, *waiter, selector, options, !NOHANG, claimed, events::View::PUBLIC)",
            "events::ProbeResult::CLAIMED",
            "events::release_claim(*waiter, claimed)",
            "events::commit_claim(*OWNER, *waiter, claimed, NOWAIT)",
            "events::ProbeResult::NO_CHILD",
            "waiter->has_interrupting_signal_pending()",
            'preemptible_syscall_park("waitpid", sched_task::WaitChannelKind::WAITPID)',
        ],
        "current-context waitpid event loop",
    )
    require_order(syscall, "copy_outputs(*waiter, status, rusage_vaddr, claimed)", "events::commit_claim", "usercopy before consume")
    release_pos = syscall.find("events::release_claim(*waiter, claimed)")
    fault_pos = syscall.find("return finish_wait(static_cast<uint64_t>(-EFAULT))", release_pos)
    if release_pos < 0 or fault_pos < release_pos:
        fail("EFAULT must release the claim and keep the event retryable")
    forbid_tokens(
        syscall,
        [
            "find_task_by_pid",
            "find_active_task",
            "find_dead_task",
            "get_active_task",
            "get_dead_task",
            "deferred_task_switch = true",
        ],
        "waitpid global scanning/deferred completion",
    )
    require_tokens(
        copy,
        [
            "KernRusage rusage{}",
            "claimed.user_time_us / 1000000ULL",
            "claimed.system_time_us / 1000000ULL",
            "copy_value_to_task(waiter, rusage_vaddr, rusage)",
        ],
        "waitpid status/rusage copy",
    )
    require_tokens(header, ["static_assert(sizeof(KernRusage) == 144)"], "waitpid rusage ABI layout")


def test_exit_signal_and_ptrace_are_event_producers() -> None:
    exit_source = EXIT_CPP.read_text()
    exit_body = function_body(exit_source, "wos_proc_exit_with_wait_status")
    require_tokens(
        exit_body,
        [
            "child_events::prepare_process_exit(*current_task)",
            "ker::mod::sched::finalize_current_task_cpu_accounting()",
            "current_task->has_exited = true",
            "current_task->exit_notify_ready.store(true, std::memory_order_release)",
            "child_events::publish_exit(*current_task)",
            "release_exiting_user_address_space(current_task)",
        ],
        "local process exit event publication",
    )
    require_order(exit_body, "ker::mod::sched::finalize_current_task_cpu_accounting()", "child_events::publish_exit", "accounting before exit snapshot")
    require_order(exit_body, "child_events::publish_exit", "release_exiting_user_address_space", "exit event before heavy cleanup")
    require_order(exit_body, "close_thread_cpu_admission", "request_thread_group_exit", "close admission before sibling scan")
    require_order(exit_body, "resource_cpu_threads.load", "TaskState::EXITING", "drain CPU contributions while leader schedulable")
    require_order(exit_body, "shm_cleanup_for_task(current_task)", 'asm volatile("cli"', "sleeping SHM cleanup before final interrupt boundary")
    require_order(exit_body, "quiesce_usercopy_pagemap()", "release_file_mmap_ranges_for_pagemap", "quiesce user access before mapped-file writeback")
    require_order(exit_body, "release_file_mmap_ranges_for_pagemap", 'asm volatile("cli"', "sleeping file writeback before final interrupt boundary")
    require_order(exit_body, 'asm volatile("cli"', "TaskState::EXITING", "final exit notifications cannot abandon wake leadership")
    if "resource_thread_owners.load" in exit_body:
        fail("process exit must not wait for thread metadata GC")
    thread_source = (ROOT / "modules/kern/src/syscalls_impl/multiproc/threadControl.cpp").read_text()
    thread_exit = function_body(thread_source, "wos_thread_exit_current")
    require_order(thread_exit, "process_charge.release()", "finalize_current_task_cpu_accounting()", "retire UID charge before final CPU slice")
    require_order(thread_exit, "finalize_current_task_cpu_accounting()", "resource_owner.finish_cpu()", "freeze CPU before retiring contribution")
    require_order(thread_exit, "resource_owner.finish_cpu()", "exit_notify_ready.store", "thread accounting before exit readiness")


    forbid_tokens(exit_source, ["complete_exit_wait", "notify_parent_after_exit_ready", "awaitee_on_exit"], "producer-side wait completion")

    signal = SIGNAL_CPP.read_text()
    require_tokens(
        signal,
        [
            "child_events::publish_job_control_stop",
            "child_events::publish_continued",
            "jobctl_stop_publish_deferred",
        ],
        "job-control child event producers",
    )
    ptrace = PTRACE_CPP.read_text()
    require_tokens(
        ptrace,
        [
            "child_events::attach_tracer",
            "child_events::publish_ptrace_stop",
            "child_events::acknowledge_ptrace_stop",
            "child_events::detach_tracer",
            "child_events::claim_or_register",
            "child_events::commit_claim",
        ],
        "ptrace lifecycle event integration",
    )


def test_scheduler_has_only_generic_wait_support() -> None:
    scheduler = SCHEDULER_CPP.read_text()
    forbid_tokens(
        scheduler,
        [
            "complete_waitpid",
            "waitpid_repair_due",
            "WAITPID_REPAIR_FALLBACK",
            "WAITPID_COMPLETION_CLAIM_LEASE",
            "orphaned_waitpid",
            "waiting_for_pid",
            "waitpid_publish_pending",
            "waitpid_completion_claimed",
            "awaitee_on_exit",
            "try_mark_task_waited_on",
        ],
        "scheduler waitpid ownership",
    )
    require_tokens(
        scheduler,
        [
            "wake_task_from_event",
            "preemptible_syscall_park",
            "child_events::is_waitable_zombie(*cur)",
            "zombie_resources_reclaiming.compare_exchange_strong",
        ],
        "generic park/wake and explicit zombie GC seam",
    )
    gc = function_body(scheduler, "detach_next_reclaimable_task_locked")
    require_order(
        gc,
        "child_events::is_waitable_zombie(*cur)",
        "uint32_t const RC = cur->ref_count.load",
        "lifecycle references must not pin zombie heavy resources",
    )


def test_remote_proxy_uses_the_same_immutable_exit_event() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    finalize = function_body(source, "finalize_proxy_task")
    prepare = function_body(source, "prepare_proxy_finalization")
    finish = function_body(source, "finish_proxy_exit")
    worker = function_body(source, "run_one_proxy_exit")
    require_tokens(finalize, ["prepare_proxy_finalization", "enqueue_proxy_exit"], "proxy retirement dispatch")
    require_tokens(
        prepare,
        [
            "write_proxy_output(proxy, output_data, output_len, task_id)",
            "cleanup_proxy_resources(proxy)",
            "enqueue(*proxy)",
        ],
        "remote proxy completion before deferred retirement",
    )
    require_order(prepare, "cleanup_proxy_resources(proxy)", "enqueue(*proxy)", "proxy cleanup before worker handoff")
    require_order(finish, "child_events::prepare_process_exit(*proxy)", "proxy->has_exited = true", "child ownership retired before exit")
    require_order(finish, "exit_notify_ready.store", "publish(*proxy)", "immutable completion before child event")
    require_tokens(worker, ["finish_proxy_exit", "child_events::publish_exit(task)", "insert_into_dead_list(&task)"], "worker event publication")
    require_order(worker, "child_events::publish_exit(task)", "insert_into_dead_list(&task)", "proxy exit event before dead-list publication")
    forbid_tokens(finalize + prepare + finish + worker, ["waiting_for_pid", "waitpid_publish_pending", "try_complete_proxy_wait", "wake_proxy_waiters"], "WKI direct wait completion")


def test_deterministic_lifecycle_ktests_cover_required_interleavings() -> None:
    registration = CHILD_EVENTS_KTEST.read_text()
    implementation = CHILD_EVENTS_CPP.read_text()
    tests = [
        "IndependentClaimsPreserveLocalWaiters",
        "IndependentClaimsRetireBeforeSubjectStorage",
        "CanceledClaimsNeverDereferenceRetiredStorage",
        "StaleClaimCannotReleaseReissuedEvent",
        "PublishAndRegistrationOrdering",
        "ClaimRetryNowaitAndSingleWinner",
        "WaiterHandoffAndSubjectSerialization",
        "GroupChangeWakesInvalidatedSelector",
        "SelectorsAndPublicationRace",
        "PtraceExitPreservesParentEvent",
        "DualObserverZombieLifetime",
        "StopContinueOrderAndBackpressure",
        "ParentEventTransfer",
        "ParentExitDiscardsWithoutReaper",
    ]
    require_tokens(registration, tests, "child lifecycle KTEST registration")
    require_tokens(
        implementation,
        [
            "REREGISTERED",
            "release_claim(first_waiter, first)",
            "SINGLE_WINNER",
            "SUBJECT_SERIALIZED",
            "LOSER_WOKEN_FOR_NO_CHILD",
            "SELECTOR_INVALIDATED",
            "REAPED_BEFORE_CREATOR_COMMIT",
            "TRACER_CONSUMED",
            "RETAINED_FOR_TRACER",
            "STOP_BACKPRESSURED",
            "TRANSFERRED",
        ],
        "deterministic lifecycle race coverage",
    )


def test_remote_wait_owns_values_and_copies_before_commit() -> None:
    source = WAITPID_CPP.read_text()
    remote = function_body(source, "wait_remote")
    require_order(remote, "copy_outputs(waiter", "wki_child_wait_commit(waiter", "remote wait usercopy before commit")
    require_tokens(remote, ["wki_child_wait_close(waiter, handle)", "remote_wait_interrupted(waiter, owner, committing)", "result.error != -EAGAIN",
                            "preemptible_syscall_park"], "remote wait cleanup and interruption")
    forbid_tokens(remote, ["Claim claim", ".node->", "try_claim", "import_reaped_cpu_times"], "syscall owns only copied values")
    gate = function_body(source, "remote_wait_interrupted")
    require_tokens(gate, ["signal_pending_bits() & KILL_MASK", "pending_cpu_signals.load", "process_exit_requested.load",
                          "return !committing", "has_interrupting_signal_pending", "resource_exec_quiescence_epoch"],
                   "irrevocable termination can detach an outstanding commit")
    require_order(gate, "process_exit_requested.load", "return !committing", "termination bypasses catchable-signal deferral")
    forbid_tokens(gate, ["signal_pending_store", "fetch_and", "exchange(", "wos_proc_exit"], "termination gate preserves return-path markers")
    require_order(remote, "remote_wait_interrupted(waiter, owner, committing)", "preemptible_syscall_park", "termination unwinds before parking")
    compute = REMOTE_COMPUTE_CPP.read_text()
    worker = function_body(compute, "run_one_native_child_wait")
    require_order(worker, "s_compute_lock.unlock();\n        if (CANCEL)", "child_events::cancel_claim", "claim cleanup outside compute")
    advance = function_body(compute, "advance_native_child")
    require_order(advance, "summary.pids.ambiguous", "schedule_native_child_claim(record, ChildWaitCommand::BEGIN)", "collision before claim")
    require_tokens(advance, ["-EOVERFLOW", "OPTION_NOWAIT", "ChildEventAudience::PARENT", "import_live_reaped_cpu_times"],
                   "collision refusal and exact parent accounting")
    prepare = function_body(EXEC_CPP.read_text(), "prepare_remote_exec_owner")
    require_order(prepare, "plan.admission.quiesce(owner)", "wki_child_wait_quiesce(owner)", "native executors stop before wait drain")
    require_order(prepare, "wki_child_wait_quiesce(owner)", "wki_exec_signal_snapshot", "wait drain before exec snapshot")


def main() -> None:
    test_remote_wait_owns_values_and_copies_before_commit()
    test_task_owns_explicit_topology_events_and_waiters()
    test_lifecycle_lock_owns_snapshot_queue_and_claims()
    test_publication_and_reparenting_are_explicit_transactions()
    test_waitpid_is_a_current_context_claim_usercopy_commit_loop()
    test_exit_signal_and_ptrace_are_event_producers()
    test_scheduler_has_only_generic_wait_support()
    test_remote_proxy_uses_the_same_immutable_exit_event()
    test_deterministic_lifecycle_ktests_cover_required_interleavings()
    print("process child registry/event queue source invariants hold")


if __name__ == "__main__":
    main()
