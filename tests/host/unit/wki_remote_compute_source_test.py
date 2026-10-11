#!/usr/bin/env python3

import re
from pathlib import Path

from exec_source_test import function_body as multiline_function_body


ROOT = Path(__file__).resolve().parents[3]
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"
REMOTE_COMPUTE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.hpp"
REMOTE_COMPUTE_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_remote_compute_ktest.cpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
CHANNEL_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "channel.cpp"
PEER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "peer.cpp"
SMT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "smt" / "smt.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
PROCESS_EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
PROCESS_EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def test_peer_cleanup_marks_all_targeted_submits_terminal_failure() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "fail_submitted_tasks_for_peer")

    if "t.exit_status = -1;" not in body:
        fail("fenced peer cleanup must set failure exit_status for every active submitted task")
    require_order(
        body,
        "t.exit_status = -1;",
        "if (t.response_pending.load(std::memory_order_acquire))",
        "cleanup failure status before response cleanup",
    )
    require_order(
        body,
        "t.exit_status = -1;",
        "if (t.complete_pending.load(std::memory_order_acquire))",
        "cleanup failure status before complete cleanup",
    )
    require_order(body, "t.exit_status = -1;", "t.active = false;", "cleanup failure status before inactive")


def test_proxy_wait_completion_respects_waitpid_publish_fence() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    prepare = function_body(source, "prepare_proxy_finalization")
    finish = function_body(source, "finish_proxy_exit")
    worker = function_body(source, "run_one_proxy_exit")
    finalize_body = prepare + finish + worker
    require_order(prepare, "cleanup_proxy_resources(proxy)", "enqueue(*proxy)", "stable resources before deferred ownership")
    require_order(finish, "child_events::prepare_process_exit(*proxy)", "proxy->has_exited = true", "child retirement before exit visibility")
    require_order(worker, "s_proxy_exit_lock.unlock_irqrestore", "finish_proxy_exit", "no queue lock across blocking retirement")
    loop = function_body(source, "compute_rx_worker_loop")
    require_order(loop, "run_one_proxy_exit()", "PeerLifecycleLease lifecycle", "retirement outside peer lease")
    require_order(loop, "run_one_child_query_send()", "PeerLifecycleLease lifecycle", "query sends outside incoming peer lease")
    require_order(loop, "run_one_child_claim_send()", "PeerLifecycleLease lifecycle", "claim sends outside incoming peer lease")
    require_tokens(loop, ["if (NATIVE_WORK || QUERY_WORK || CLAIM_WORK)"], "retry query and claim continuations before parking")
    require_tokens(" ".join(loop.split()), ["if (!RETIRED && !SIGNALED && !CLAIM_RETIRED && !QUERY_RETIRED) ker::mod::sched::kern_block()"], "drain retained work before parking")
    require_tokens(loop, ["run_one_child_signal()"], "deferred child notification progress before RX lease")
    require_order(
        finalize_body,
        "cleanup_proxy_resources(proxy)",
        "ker::syscall::process::child_events::publish_exit(task)",
        "proxy resources must be stable before the immutable exit event",
    )
    require_order(
        finalize_body,
        "ker::syscall::process::child_events::publish_exit(task)",
        "ker::mod::sched::insert_into_dead_list(&task)",
        "proxy exit event must precede dead-list insertion",
    )
    require_tokens(
        finalize_body,
        [
            "proxy->has_exited = true",
            "proxy->exit_notify_ready.store(true, std::memory_order_release)",
            'panic_handler("WKI remote compute: failed to publish proxy child exit event")',
        ],
        "remote proxy exit publication",
    )

    spawn_body = function_body(source, "wki_try_remote_spawn")
    require_tokens(
        spawn_body,
        [
            "child_events::abort_publication(*task)",
            "child_events::commit_publication(*task)",
        ],
        "remote proxy transactional membership",
    )

    for forbidden in [
        "proxy_waiter_context_can_be_completed",
        "try_complete_proxy_wait",
        "wake_proxy_waiters",
        "waitpid_publish_pending",
        "waiting_for_pid",
        "awaitee_on_exit",
    ]:
        if forbidden in source:
            fail(f"WKI must not complete waitpid outside the process child-event queue: {forbidden}")

    if "wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence" not in source:
        fail("remote-compute KTEST selftest must cover proxy child-event publication")
    for snippet in [
        "REGISTERED_BEFORE_PUBLISH",
        "SNAPSHOT_PRESERVED",
        "RETRY_IS_SAME_EVENT",
        "REJECTED_STALE_SPECIFIC_WAIT",
        "parent.child_user_time_us == 351",
        "parent.child_system_time_us == 588",
        "merge_proxy_resource_completion(proxy, BASELINE, completion)",
    ]:
        if snippet not in source:
            fail(f"remote-compute KTEST selftest is missing child-event coverage: {snippet}")
    if "REJECTED_STALE_SPECIFIC_WAIT" not in source:
        fail("remote-compute KTEST selftest must cover stale specific waiters")


def test_task_wait_consumes_completed_submitted_row() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "wki_task_wait")

    require_order(
        body,
        "SubmittedTask* task = find_submitted_task_any(task_id);",
        "if (!task->active)",
        "wki_task_wait must inspect retained inactive completion rows before publishing a waiter",
    )
    require_order(
        body,
        "if (!task->active)",
        "publish_task_complete_waiter_locked(task, wait)",
        "wki_task_wait must not publish complete_wait_entry for already-completed rows",
    )
    inactive_block = body[body.find("if (!task->active)") : body.find("// V2 I-4")]
    for snippet in [
        "COMPLETED_EXIT_STATUS = task->exit_status",
        "consume_submitted_task_result_locked(task)",
        "delete[] discarded_output",
        "*exit_status = COMPLETED_EXIT_STATUS",
        "return 0",
    ]:
        if snippet not in inactive_block:
            fail(f"wki_task_wait inactive-row fast path must return retained completion status: {snippet}")
    if "SubmittedTask* task = find_submitted_task(task_id);" in body:
        fail("wki_task_wait must not use the active-only submitted-task lookup")
    if "wki_remote_compute_selftest_task_wait_consumes_completed_row" not in source:
        fail("remote-compute KTEST selftest must cover waiting after TASK_COMPLETE made the row inactive")
    require_order(
        inactive_block,
        "COMPLETED_EXIT_STATUS = task->exit_status",
        "consume_submitted_task_result_locked(task)",
        "wki_task_wait must copy terminal status before recycling its one-shot row",
    )
    require_tokens(
        body,
        [
            "!task->result_handle_owned",
            "task->response_consumer_wait_entry != nullptr",
            "!task->active && task->complete_consumer_wait_entry != nullptr",
        ],
        "exclusive one-shot result ownership",
    )
    consume_body = function_body(source, "consume_submitted_task_result_locked")
    require_tokens(
        consume_body,
        [
            "task->pending_proxy_output = nullptr",
            "task->reset_local_task_ref()",
            "task->result_handle_owned = false",
            "request_submitted_task_reclaim_locked(task)",
        ],
        "direct result proxy/output detachment",
    )
    selftest = function_body(source, "wki_remote_compute_selftest_task_wait_consumes_completed_row")
    require_tokens(
        selftest,
        [
            "task.complete_consumer_wait_entry = &in_flight_consumer",
            "BLOCKED_WAIT_RC = wki_task_wait",
            "CONSUMER_PIN_PRESERVED",
            "stored->complete_consumer_wait_entry = nullptr",
            "SECOND_WAIT_RC = wki_task_wait",
        ],
        "one-shot in-flight consumer regression",
    )


def test_task_wait_completion_slot_is_single_owner() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    publish_body = function_body(source, "publish_task_complete_waiter_locked")
    require_tokens(
        publish_body,
        [
            "task->complete_pending.load(std::memory_order_acquire)",
            "return false",
            "task->complete_wait_entry = &wait",
            "task->complete_consumer_wait_entry = &wait",
            "task->complete_pending.store(true, std::memory_order_release)",
        ],
        "task wait publish helper",
    )

    clear_body = function_body(source, "clear_task_complete_waiter_after_wait_locked")
    require_tokens(
        clear_body,
        [
            "WAIT_SLOT_OWNED = task->complete_wait_entry == &wait",
            "WAIT_CONSUMER_OWNED = task->complete_consumer_wait_entry == &wait",
            "if (WAIT_SLOT_OWNED)",
            "task->complete_wait_entry = nullptr",
            "if (WAIT_CONSUMER_OWNED)",
            "task->complete_consumer_wait_entry = nullptr",
            "wait_result == WKI_ERR_TIMEOUT && WAIT_SLOT_OWNED",
            "task->complete_pending.store(false, std::memory_order_release)",
        ],
        "task wait timeout cleanup helper",
    )

    wait_body = function_body(source, "wki_task_wait")
    require_order(
        wait_body,
        "if (!publish_task_complete_waiter_locked(task, wait))",
        "wki_wait_for_op(&wait, timeout_us)",
        "wki_task_wait must publish before waiting",
    )
    require_order(
        wait_body,
        "clear_task_complete_waiter_after_wait_locked(task, wait, WAIT_RC, completed_exit_status)",
        "if (WAIT_RC == WKI_ERR_TIMEOUT)",
        "wki_task_wait must perform ownership cleanup before timeout return",
    )
    if "task->complete_wait_entry = nullptr;" in wait_body:
        fail("wki_task_wait must clear complete_wait_entry through the ownership helper")
    if "task->complete_pending.store(false" in wait_body:
        fail("wki_task_wait must clear complete_pending only through the ownership helper")

    token = "wki_remote_compute_selftest_task_wait_timeout_preserves_successor"
    require_tokens(source, [f"auto {token}() -> bool"], "task wait timeout successor selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "task wait timeout successor selftest declaration")
    require_tokens(
        ktest,
        [
            "TaskWaitTimeoutPreservesSuccessor",
            token,
        ],
        "task wait timeout successor KTEST coverage",
    )


def test_remote_load_procfs_uses_locked_snapshot() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    procfs = PROCFS_CPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    if "wki_remote_node_load(" in header + source + procfs:
        fail("remote load callers must not expose or consume raw RemoteNodeLoad pointers")

    require_tokens(
        header,
        ["auto wki_remote_node_load_snapshot(uint16_t node_id, RemoteNodeLoad* out) -> bool;"],
        "remote load snapshot declaration",
    )
    snapshot_body = function_body(source, "wki_remote_node_load_snapshot")
    require_tokens(
        snapshot_body,
        [
            "s_compute_lock.lock()",
            "RemoteNodeLoad const* load = find_remote_load(node_id)",
            "*out = *load",
            "s_compute_lock.unlock()",
            "return FOUND",
        ],
        "remote load snapshot locking",
    )
    require_order(snapshot_body, "s_compute_lock.lock()", "*out = *load", "snapshot copy under compute lock")
    require_order(snapshot_body, "*out = *load", "s_compute_lock.unlock()", "snapshot unlock after copy")

    peers_body = function_body(procfs, "generate_wki_peers")
    require_tokens(
        peers_body,
        [
            "struct PeerProcSnapshot",
            "std::array<PeerProcSnapshot, ker::net::wki::WKI_MAX_PEERS> peer_rows{}",
            "ker::net::wki::g_wki.peer_lock.unlock_irqrestore(FLAGS)",
            "wki_remote_node_load_snapshot(row.node_id, &load)",
        ],
        "procfs WKI peer load snapshots",
    )
    require_order(
        peers_body,
        "ker::net::wki::g_wki.peer_lock.unlock_irqrestore(FLAGS)",
        "wki_remote_node_load_snapshot(row.node_id, &load)",
        "procfs must not query remote load while holding peer lock",
    )

    token = "wki_remote_compute_selftest_load_snapshot_survives_cleanup"
    require_tokens(source, [f"auto {token}() -> bool"], "remote load snapshot selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "remote load snapshot selftest declaration")
    require_tokens(ktest, ["LoadSnapshotSurvivesCleanup", token], "remote load snapshot KTEST coverage")


def test_preferred_placement_accounts_for_inflight_submissions() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    preferred = function_body(source, "wki_preferred_remote_node")
    require_tokens(
        preferred,
        [
            "preferred_remote_placement_score(rl, active_submitted_tasks_for_node_locked(rl.node_id))",
            "SELECTED->placement_reservations++",
            "return SELECTED->node_id",
        ],
        "preferred placement must reserve score capacity before returning",
    )

    release = function_body(source, "wki_release_preferred_remote_node")
    require_tokens(
        release,
        [
            "s_compute_lock.lock()",
            "find_remote_load(node_id)",
            "LOAD->placement_reservations--",
            "s_compute_lock.unlock()",
        ],
        "placement reservation release must be compute-lock serialized",
    )

    spawn = function_body(source, "wki_try_remote_spawn")
    require_order(
        spawn,
        "best_node = wki_preferred_remote_node()",
        "preferred_reservation.adopt(best_node)",
        "remote-preferred selection must transfer reservation ownership to the scope guard",
    )

    token = "wki_remote_compute_selftest_placement_score_accounts_for_inflight"
    require_tokens(source, [f"auto {token}() -> bool"], "placement score selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "placement score selftest declaration")
    require_tokens(ktest, ["PlacementScoreAccountsForInflight", token], "placement score KTEST coverage")


def test_balanced_placement_scores_all_healthy_systems() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    balanced = function_body(source, "wki_balanced_node")
    require_tokens(
        balanced,
        [
            "g_balanced_local_sample_us != g_cached_local_load_update_us",
            "g_balanced_local_reservations = 0",
            "balanced_memory_eligible(local_free_mem)",
            "!balanced_memory_eligible(rl.free_mem_pages)",
            "uint64_t const LOCAL_PRESSURE = g_balanced_local_reservations",
            "uint64_t const KNOWN_ACTIVE = active_submitted_tasks_for_node_locked(rl.node_id)",
            "std::max<uint64_t>(KNOWN_ACTIVE, rl.balanced_assignments)",
            "peer->state != PeerState::CONNECTED",
            "balanced_candidate_is_starved",
            "candidate.capacity",
            "eligible_capacity",
            "balanced_last_assignment_seq",
            "g_balanced_local_last_assignment_seq",
            "g_balanced_local_reservations",
            "LOAD->balanced_assignments",
        ],
        "balanced placement must score assignment debt and bound eligible-system starvation",
    )
    score = function_body(source, "balanced_inflight_pressure")
    require_tokens(
        score,
        [
            "WKI_BALANCED_QUEUE_HEADROOM_NUMERATOR",
            "WKI_BALANCED_QUEUE_HEADROOM_DENOMINATOR",
            "+ WKI_BALANCED_QUEUE_HEADROOM_DENOMINATOR - 1",
            "inflight == 0 ? 0 : 1 + ((inflight * 1000ULL) / QUEUE_CAPACITY)",
        ],
        "balanced placement must retain bounded runnable headroom for blocking tasks",
    )

    spawn = function_body(source, "wki_try_remote_spawn")
    require_tokens(
        spawn,
        [
            "WKI_TARGET_FLAG_BALANCED",
            "} else if (BALANCED_PLACEMENT || AUTOMATIC_PLACEMENT)",
            "BalancedPlacement const PLACEMENT = wki_balanced_node(LOCAL_LOAD, LOCAL_CPUS, LOCAL_FREE_MEM)",
            "BALANCED_PLACEMENT ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL",
            'AUTOMATIC_PLACEMENT ? "automatic-no-healthy-system" : "balanced-no-healthy-system"',
            'AUTOMATIC_PLACEMENT ? "automatic-local" : "balanced-local"',
        ],
        "system-wide balanced task policy selection",
    )
    balanced_branch = spawn[spawn.index("} else if (BALANCED_PLACEMENT || AUTOMATIC_PLACEMENT)") :]
    require_order(
        balanced_branch,
        "BalancedPlacement const PLACEMENT = wki_balanced_node(LOCAL_LOAD, LOCAL_CPUS, LOCAL_FREE_MEM)",
        "if (!PLACEMENT.found)",
        "balanced placement must reject an empty healthy candidate set",
    )
    require_order(
        balanced_branch,
        "if (!PLACEMENT.found)",
        "balanced_assignment.adopt(best_node)",
        "balanced remote selection must transfer its sample-window assignment",
    )
    require_order(
        spawn,
        "if (tid == 0)",
        "balanced_assignment.commit()",
        "balanced assignment debt may commit only after successful submission",
    )
    load_report = function_body(source, "handle_load_report")
    require_tokens(
        load_report,
        ["rl->last_update_us = wki_now_us()", "rl->balanced_assignments = 0"],
        "a fresh remote load sample must retire prior balanced assignment debt",
    )

    token = "wki_remote_compute_selftest_balanced_score_accounts_for_capacity"
    require_tokens(source, [f"auto {token}() -> bool"], "balanced placement score selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "balanced placement score selftest declaration")
    require_tokens(ktest, ["BalancedScoreAccountsForCapacity", token], "balanced placement score KTEST coverage")

    policy_token = "wki_remote_compute_selftest_automatic_policy_is_workload_agnostic"
    require_tokens(source, [f"auto {policy_token}() -> bool"], "automatic placement policy selftest implementation")
    require_tokens(header, [f"auto {policy_token}() -> bool;"], "automatic placement policy selftest declaration")
    require_tokens(ktest, ["AutomaticPolicyIsWorkloadAgnostic", policy_token], "automatic placement policy KTEST coverage")


def test_placement_controllers_run_before_payload_distribution() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    spawn = function_body(source, "wki_try_remote_spawn")
    require_tokens(
        source,
        [
            "auto is_placement_control_helper(const char* name) -> bool",
            '"wkictl", "locally", "remotely", "anywhere", "homeward", "on", "forward", "wosid"',
        ],
        "placement controller classification",
    )
    require_tokens(
        spawn,
        [
            "is_placement_control_helper(task->name)",
            "is_placement_control_helper(EXE_PATH)",
            'log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "placement-control-helper")',
        ],
        "placement controller local execution",
    )
    require_order(
        spawn,
        "if (AUTOMATIC_PLACEMENT)",
        'log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "placement-control-helper")',
        "only automatic placement may pin placement controllers",
    )
    require_order(
        spawn,
        'log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "placement-control-helper")',
        "if (REMOTE_RECEIVER && !EXPLICIT_TARGET && !PREFER_REMOTE && !BALANCED_PLACEMENT)",
        "controller classification must precede receiver forwarding policy",
    )


def test_submitted_task_slots_are_indexed_and_reclaimed() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    require_tokens(
        source,
        [
            "constexpr size_t WKI_SUBMITTED_TASK_INDEX_BUCKETS = 1024",
            "struct SubmittedTaskSlot",
            "SubmittedTaskSlot* s_submitted_task_head = nullptr",
            "std::array<SubmittedTaskSlot*, WKI_SUBMITTED_TASK_INDEX_BUCKETS>",
            "SubmittedTaskSlot* s_submitted_task_free = nullptr",
            "size_t s_submitted_task_count = 0",
            "auto find_submitted_task_slot_locked(uint32_t task_id) -> SubmittedTaskSlot*",
            "auto publish_submitted_task_locked(SubmittedTask&& task, int* error_out = nullptr, bool fail_allocation = false) -> SubmittedTask*",
            "auto allocate_submitted_task_id_locked() -> uint32_t",
        ],
        "stable indexed submitted-task slots",
    )
    if "g_next_task_id++" in source:
        fail("submitted task IDs must not use raw wrapping increment")

    lookup_body = function_body(source, "find_submitted_task_slot_locked")
    require_tokens(
        lookup_body,
        [
            "s_submitted_task_index.at(submitted_task_bucket(task_id))",
            "slot = slot->id_next",
            "slot->occupied && slot->task.task_id == task_id",
        ],
        "allocation-free submitted-task lookup",
    )
    allocator_body = function_body(source, "allocate_submitted_task_id_locked")
    require_tokens(
        allocator_body,
        [
            "s_submitted_task_count >= UINT32_MAX",
            "return 0",
            "CANDIDATE == UINT32_MAX ? 1U : CANDIDATE + 1U",
            "CANDIDATE != 0",
            "find_submitted_task_slot_locked(CANDIDATE) == nullptr",
        ],
        "nonzero collision-safe submitted-task IDs",
    )

    eligibility = function_body(source, "submitted_task_can_reclaim_locked")
    require_tokens(
        eligibility,
        [
            "task.reclaim_requested",
            "!task.active",
            "task.response_consumer_wait_entry == nullptr",
            "task.complete_consumer_wait_entry == nullptr",
            "!task.result_handle_owned",
            "task.result_owner_task == nullptr",
            "task.local_task == nullptr",
            "task.pending_proxy_output == nullptr",
            "task.handoff_pin_count == 0",
        ],
        "consumer-pinned submitted-task reclamation",
    )
    if "ipc_fd_count" in eligibility or "cleanup_submitted_ipc_exports" in function_body(source, "reclaim_submitted_task_if_safe_locked"):
        fail("historical IPC metadata must not pin rows or move IPC teardown into slot reclamation")

    submit_sections = {
        "wki_task_submit_inline": source[
            source.index("auto wki_task_submit_inline(") : source.index("auto wki_task_submit_vfs_ref(")
        ],
        "wki_task_submit_vfs_ref": source[
            source.index("auto wki_task_submit_vfs_ref(") : source.index("auto wki_task_wait(")
        ],
    }
    for submit_name, submit_body in submit_sections.items():
        require_tokens(
            submit_body,
            [
                "allocate_submitted_task_id_locked()",
                "st.result_handle_owned = true",
                "st.result_owner_task = ker::mod::sched::get_current_task()",
                "publish_submitted_task_locked(std::move(st), &publication_error)",
                "task_ptr->response_consumer_wait_entry = &wait",
                "discarded_output = consume_submitted_task_result_locked(task_ptr)",
                "delete[] discarded_output",
            ],
            f"{submit_name} indexed publication and terminal retirement",
        )
        if submit_body.count("consume_submitted_task_result_locked(task_ptr)") < 3:
            fail(f"{submit_name} must detach captured output on every terminal submit-failure path")

    require_tokens(
        function_body(source, "wki_try_remote_spawn"),
        [
            "st->result_handle_owned = false",
            "st->result_owner_task = nullptr",
            "st->complete_pending.store(false, std::memory_order_release)",
            "request_submitted_task_reclaim(tid)",
        ],
        "proxy handle transfer and early-completion retirement",
    )
    require_tokens(
        function_body(source, "wki_proxy_task_blocked"),
        [
            "submitted->complete_pending.store(false, std::memory_order_release)",
            "request_submitted_task_reclaim(TASK_ID)",
        ],
        "blocked proxy completion retirement",
    )

    require_tokens(
        header,
        [
            "WkiWaitEntry* response_consumer_wait_entry = nullptr",
            "WkiWaitEntry* complete_consumer_wait_entry = nullptr",
            "bool result_handle_owned = false",
            "ker::mod::sched::task::Task* result_owner_task = nullptr",
            "bool reclaim_requested = false",
            "std::array<WkiIpcTaskFdHandoff, 16> ipc_fd_handoff",
            "uint16_t handoff_pin_count = 0",
            "wki_remote_compute_selftest_task_exit_releases_handoff_pins",
            "wki_remote_compute_selftest_submitted_slots_reclaim_safely",
            "wki_remote_compute_selftest_task_id_wrap_is_safe",
        ],
        "submitted-task ownership state and selftests",
    )
    require_tokens(
        ktest,
        [
            "SubmittedSlotsReclaimSafely",
            "TaskIdWrapIsSafe",
            "TaskExitReleasesHandoffPins",
            "wki_remote_compute_selftest_task_exit_releases_handoff_pins",
            "wki_remote_compute_selftest_submitted_slots_reclaim_safely",
            "wki_remote_compute_selftest_task_id_wrap_is_safe",
        ],
        "submitted-slot KTEST registration",
    )


def test_task_exit_retires_remote_compute_wait_owners() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    wki_source = WKI_CPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()
    thread_control = THREAD_CONTROL_CPP.read_text()
    process_exit = PROCESS_EXIT_CPP.read_text()
    process_exec = PROCESS_EXEC_CPP.read_text()
    task_source = TASK_CPP.read_text()
    task_header = TASK_HPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    scheduler_header = SCHEDULER_HPP.read_text()
    signal_source = SIGNAL_CPP.read_text()

    cleanup = function_body(source, "wki_remote_compute_cleanup_for_task")
    require_tokens(
        cleanup,
        [
            "submitted.result_owner_task != exiting_task",
            "remember_waiter(submitted.response_wait_entry)",
            "remember_waiter(submitted.response_consumer_wait_entry)",
            "remember_waiter(submitted.complete_wait_entry)",
            "remember_waiter(submitted.complete_consumer_wait_entry)",
            "submitted.response_wait_entry = nullptr",
            "submitted.response_consumer_wait_entry = nullptr",
            "submitted.response_pending.store(false",
            "submitted.complete_wait_entry = nullptr",
            "submitted.complete_consumer_wait_entry = nullptr",
            "submitted.complete_pending.store(false",
            "submitted_ipc_cleanup_snapshot_locked(&submitted)",
            "submitted.ipc_fd_count = 0",
            "take_submitted_ipc_handoff_locked(&submitted)",
            "submitted.active = false",
            "consume_submitted_task_result_locked(&submitted)",
            "wki_claim_op(waiter)",
            "wki_finish_claimed_op(waiter, WKI_ERR_PEER_FENCED)",
            "wki_quiesce_claimed_op(waiter)",
            "send_task_cancel_request",
            "cleanup_submitted_ipc_exports(ipc_cleanup)",
            "release_submitted_ipc_handoff(handoff_cleanup)",
            "delete[] discarded_output",
        ],
        "task-exit remote-compute ownership retirement",
    )
    unlock_pos = cleanup.find("s_compute_lock.unlock()")
    for unlocked_action in [
        "wki_claim_op(waiter)",
        "send_task_cancel_request",
        "cleanup_submitted_ipc_exports(ipc_cleanup)",
        "release_submitted_ipc_handoff(handoff_cleanup)",
        "delete[] discarded_output",
    ]:
        if unlock_pos < 0 or cleanup.find(unlocked_action) <= unlock_pos:
            fail(f"task-exit cleanup must perform {unlocked_action} after releasing s_compute_lock")

    wait_cleanup = function_body(wki_source, "wki_wait_cleanup_for_task")
    require_order(
        wait_cleanup,
        "s_wait_lock.unlock()",
        "wki_remote_compute_cleanup_for_task(task)",
        "generic wait cleanup must release s_wait_lock before remote-compute ownership cleanup",
    )
    require_order(
        wait_cleanup,
        "wki_remote_compute_cleanup_for_task(task)",
        "wki_remote_vfs_cleanup_for_task(task->pid)",
        "task-exit WKI subsystem cleanup order",
    )

    task_wait = function_body(source, "wki_task_wait")
    require_tokens(
        task_wait,
        [
            "CURRENT_TASK = ker::mod::sched::get_current_task()",
            "task->result_owner_task != nullptr && task->result_owner_task != CURRENT_TASK",
            "task->result_owner_task = CURRENT_TASK",
        ],
        "owner-bound one-shot task wait",
    )
    require_tokens(
        header,
        [
            "ker::mod::sched::task::Task* result_owner_task = nullptr",
            "void wki_remote_compute_cleanup_for_task(ker::mod::sched::task::Task* task)",
            "wki_remote_compute_selftest_task_exit_retires_wait_owners",
        ],
        "remote-compute task-exit ownership API",
    )
    require_tokens(
        function_body(source, "wki_remote_compute_selftest_task_exit_retires_wait_owners"),
        [
            "ExitCase::RESPONSE",
            "ExitCase::COMPLETE",
            "ExitCase::DONE_UNLINKED",
            "wki_wait_cleanup_for_task(&exiting_task)",
            "PUBLISHED_WAITER_RETIRED = !wki_claim_op(&wait)",
            "ROW_RETIRED",
        ],
        "remote-compute task-exit regression cases",
    )
    require_tokens(
        ktest,
        [
            "TaskExitRetiresWaitOwners",
            "wki_remote_compute_selftest_task_exit_retires_wait_owners",
        ],
        "remote-compute task-exit KTEST registration",
    )
    thread_exit = function_body(thread_control, "wos_thread_exit_current")
    require_order(
        thread_exit,
        "release_thread_fd_refs(task)",
        "wki_remote_compute_cleanup_for_task(task)",
        "thread exit must close inherited refs before retiring compute ownership",
    )
    require_order(
        thread_exit,
        "wki_remote_compute_cleanup_for_task(task)",
        "destroy_unpublished_process(mod::sched::task::take_unpublished_process(task))",
        "thread exit must detach WKI subject pointers before destroying an unpublished child",
    )
    require_order(
        thread_exit,
        "destroy_unpublished_process(mod::sched::task::take_unpublished_process(task))",
        "wki_wait_cleanup_for_task(task)",
        "thread exit must clean waits created by child teardown",
    )
    require_order(
        thread_exit,
        "wki_wait_cleanup_for_task(task)",
        "task->state.store(mod::sched::task::TaskState::DEAD",
        "thread exit must retire WKI stack ownership before DEAD publication",
    )
    cleanup_pos = thread_exit.find("wki_wait_cleanup_for_task(task)")
    final_handoff_pos = thread_exit.rfind("jump_to_next_task_no_save()")
    if cleanup_pos < 0 or final_handoff_pos < 0 or cleanup_pos >= final_handoff_pos:
        fail("thread exit must retire WKI stack ownership before its final stack handoff")

    process_exit_body = function_body(process_exit, "wos_proc_exit_with_wait_status")
    require_order(
        process_exit_body,
        "wki_remote_compute_cleanup_for_task(current_task)",
        "destroy_unpublished_process(sched_task::take_unpublished_process(current_task))",
        "process exit must detach WKI subject pointers before destroying an unpublished child",
    )
    require_order(
        process_exit_body,
        "destroy_unpublished_process(sched_task::take_unpublished_process(current_task))",
        "wki_wait_cleanup_for_task(current_task)",
        "process exit must clean waits created by child teardown",
    )
    require_order(
        process_exit_body,
        "wki_wait_cleanup_for_task(current_task)",
        "transition_state(ker::mod::sched::task::TaskState::ACTIVE",
        "blocking WKI cleanup must finish while process exit remains ACTIVE",
    )

    require_tokens(
        task_header,
        [
            "std::atomic<Task*> owned_unpublished_process{nullptr}",
            "claim_unpublished_process(Task* owner, Task* child)",
            "release_unpublished_process(Task* owner, Task* child)",
            "take_unpublished_process(Task* owner)",
            "destroy_unpublished_process(Task* task)",
            "destroy_owned_unpublished_process(Task* owner, Task* child)",
            "complete_unpublished_process_construction(Task* task, int* interpreter_error",
            "std::atomic<bool> unpublished_teardown_in_progress{false}",
        ],
        "persistent unpublished-process owner API",
    )
    require_tokens(
        function_body(task_source, "claim_unpublished_process"),
        ["child->try_acquire_lifetime_ref()", "compare_exchange_strong", "child->release()"],
        "unpublished-process owner reference claim",
    )
    take_unpublished = function_body(task_source, "take_unpublished_process")
    require_tokens(
        take_unpublished,
        ["owned_unpublished_process.exchange(nullptr, std::memory_order_acq_rel)"],
        "unpublished-process owner reference transfer",
    )
    if "->release()" in take_unpublished:
        fail("taking an unpublished child must transfer, not drop, the owner-slot reference")

    destroy_unpublished = function_body(task_source, "destroy_unpublished_process")
    require_tokens(
        destroy_unpublished,
        [
            "unpublished_process_is_exclusively_owned(task)",
            "teardown_unpublished_process_resources(task)",
            "task->release()",
            "delete task",
        ],
        "transferred unpublished-process teardown",
    )
    teardown_resources = function_body(task_source, "teardown_unpublished_process_resources")
    require_tokens(
        teardown_resources,
        [
            "loader::debug::unregister_process(task->pid)",
            "task->fd_table.remove(fd)",
            "ker::vfs::vfs_put_file(file)",
            "delete[] task->elf_buffer",
            "threading::destroy_thread(task->thread)",
            "auto* pagemap = task->detach_pagemap_after_usercopy_quiescence()",
            "release_file_mmap_ranges_for_pagemap(pagemap)",
            "mm::virt::destroy_user_space",
            "mm::virt::release_pagemap",
            "mm::phys::page_free",
        ],
        "complete unpublished-process teardown",
    )
    require_order(
        teardown_resources,
        "task->detach_pagemap_after_usercopy_quiescence()",
        "release_file_mmap_ranges_for_pagemap(pagemap)",
        "unpublished teardown must quiesce stable usercopy leases before releasing file mappings",
    )
    require_order(
        teardown_resources,
        "release_file_mmap_ranges_for_pagemap(pagemap)",
        "mm::virt::destroy_user_space(pagemap",
        "unpublished teardown must release file mappings before destroying the detached pagemap",
    )
    require_order(
        teardown_resources,
        "mm::virt::destroy_user_space(pagemap",
        "mm::virt::release_pagemap(pagemap)",
        "unpublished teardown must destroy the detached address space before dropping its root reference",
    )
    exclusive_check = function_body(task_source, "unpublished_process_is_exclusively_owned")
    require_tokens(
        exclusive_check,
        [
            "task->scheduler_published.load(std::memory_order_acquire)",
            "task->state.load(std::memory_order_acquire) != TaskState::ACTIVE",
            "task->gc_queued.load(std::memory_order_acquire)",
            "task->ref_count.load(std::memory_order_acquire) != 2",
            "vfs_count_exclusive_unpublished_self_refs(*task, self_refs)",
            "task->ref_count.load(std::memory_order_acquire) != 2 + self_refs",
        ],
        "unpublished-process exclusive lifetime validation",
    )
    for racy_scheduler_field in ["sched_queue", "heap_index", "sched_next"]:
        if racy_scheduler_field in exclusive_check:
            fail(f"unpublished teardown must not inspect unlocked scheduler field {racy_scheduler_field}")
    require_order(exclusive_check, "vfs_count_exclusive_unpublished_self_refs(*task, self_refs)",
                  "task->ref_count.load(std::memory_order_acquire) != 2 + self_refs",
                  "self-reference exception must prove exclusive graph before accepting its Task references")
    vfs_core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    self_proof = function_body(vfs_core, "vfs_count_exclusive_unpublished_self_refs")
    require_tokens(self_proof, ["task.fd_table.reserved_count() == 0", "captured.root.get()", "captured.cwd.get()",
                               "task.exec_image_file != captured.image", "captured.find_file_entry(range.file)",
                               "lazy_owned == captured.files[i].lazy_observed",
                               "entry.file->refcount.load(std::memory_order_acquire)",
                               "entry.view->refs.load(std::memory_order_acquire)", "2 * ALIASES",
                               "task.ref_count.load(std::memory_order_acquire) != 2 + self_refs", "out = self_refs"],
                   "bounded constructor graph and original-source proof")
    proof_code = re.sub(r"//[^\n]*|/\*[\s\S]*?\*/", "", self_proof)
    for prohibited in ("new ", "procfs_view_policy(", "find_task_by_", "vfs_open", "vfs_put_file("):
        if prohibited in proof_code:
            fail(f"unpublished ownership proof must not perform {prohibited}")
    require_tokens(
        task_header,
        ["std::atomic<bool> scheduler_published{false}"],
        "monotonic scheduler-publication marker",
    )
    # Queue-tag/publication ordering is owned by the locked transition
    # primitive; the legacy wrapper only adds diagnostics after failure.
    runnable_post = function_body(scheduler_source, "transition_make_runnable_locked")
    require_order(
        runnable_post[runnable_post.find("rq->runnable_heap.insert(t)") :],
        "t->sched_queue = task::Task::sched_queue::RUNNABLE",
        "t->scheduler_published.store(true, std::memory_order_release)",
        "runnable publication must set the monotonic teardown guard under its runqueue lock",
    )
    waiting_post = function_body(scheduler_source, "post_task_waiting")
    require_order(
        waiting_post,
        "published = register_fresh_task_visibility(task)",
        "task->scheduler_published.store(true, std::memory_order_release)",
        "waiting publication must set the monotonic teardown guard before releasing its runqueue lock",
    )
    production_scheduler_source = scheduler_source.split("#ifdef WOS_SELFTEST", maxsplit=1)[0]
    if "scheduler_published.store(false" in production_scheduler_source or "scheduler_published.store(false" in task_source:
        fail("scheduler publication marker must never be cleared")
    owned_destroy = function_body(task_source, "destroy_owned_unpublished_process")
    require_order(
        owned_destroy,
        "unpublished_teardown_in_progress.compare_exchange_strong",
        "teardown_unpublished_process_resources(child)",
        "fatal-exit deferral must publish before blocking child teardown",
    )
    require_order(
        owned_destroy,
        "teardown_unpublished_process_resources(child)",
        "owned_unpublished_process.exchange(nullptr",
        "owner slot must remain discoverable throughout blocking child teardown",
    )
    delete_child_pos = owned_destroy.find("delete child")
    final_deferral_clear_pos = owned_destroy.rfind("unpublished_teardown_in_progress.store(false, std::memory_order_release)")
    if delete_child_pos < 0 or final_deferral_clear_pos <= delete_child_pos:
        fail("fatal exit may resume only after child destruction")

    constructor_start = task_source.find("Task::Task(")
    constructor_end = task_source.find("Task* Task::create_kernel_thread", constructor_start)
    if constructor_start < 0 or constructor_end < 0:
        fail("missing Task PROCESS constructor interval")
    constructor = task_source[constructor_start:constructor_end]
    if "read_boot_file_fully(" in constructor:
        fail("Task constructor must not perform blocking PT_INTERP VFS I/O")
    require_tokens(
        constructor,
        ["this->pending_interp_path.data()", "std::memcpy", "INTERP_LEN + 1"],
        "Task constructor PT_INTERP deferral",
    )
    complete_construction = function_body(task_source, "complete_unpublished_process_construction")
    require_tokens(
        complete_construction,
        [
            "task->pending_interp_path.front() == '\\0'",
            "read_boot_file_fully(*task, task->pending_interp_path.data(), &interp_buf, &interp_size, interpreter_error)",
            "loader::elf::load_elf",
            "task->context.frame.rip = INTERP_RESULT.entry_point",
            "task->interp_base = INTERP_RESULT.load_base",
        ],
        "post-construction PT_INTERP stage",
    )

    exec_body = function_body(process_exec, "wos_proc_exec_impl")
    require_order(
        exec_body,
        "new_task->elf_buffer = elf_buffer",
        "claim_unpublished_process(parent_task, new_task)",
        "exec must transfer ELF ownership before publishing fatal-exit recovery",
    )
    require_order(
        exec_body,
        "claim_unpublished_process(parent_task, new_task)",
        "complete_unpublished_process_construction(new_task",
        "exec must claim fatal-exit ownership before PT_INTERP VFS I/O",
    )
    require_order(
        exec_body,
        "complete_unpublished_process_construction(new_task",
        "wki_try_remote_spawn(new_task, REMOTE_SPAWN)",
        "exec must finish PT_INTERP before remote submission",
    )
    require_tokens(
        exec_body,
        ["destroy_owned_unpublished_process(parent_task, new_task)"],
        "exec failure teardown must retain persistent child ownership",
    )
    local_post_pos = exec_body.find("post_task_balanced(new_task)")
    final_owner_release_pos = exec_body.rfind("release_unpublished_process(parent_task, new_task)")
    if local_post_pos < 0 or final_owner_release_pos <= local_post_pos:
        fail("local publication must precede the final owner-slot release")

    require_tokens(
        SMT_CPP.read_text(),
        ["complete_unpublished_process_construction(new_task"],
        "pre-scheduler init PT_INTERP completion",
    )
    require_tokens(
        function_body(signal_source, "exit_current_on_pending_fatal_default_signal"),
        ["unpublished_teardown_in_progress.load(std::memory_order_acquire)"],
        "fatal-signal handoff deferral during unpublished teardown",
    )
    requested_exit = function_body(process_exit, "take_requested_process_exit")
    require_tokens(function_body(process_exit, "exit_current_if_process_exit_requested"),
                   ["take_requested_process_exit"], "early exit uses the shared guarded request consumer")
    require_order(
        requested_exit,
        "unpublished_teardown_in_progress.load(std::memory_order_acquire)",
        "process_exit_requested.load(std::memory_order_acquire)",
        "group-exit request must remain pending during unpublished teardown",
    )

    reschedule = function_body(scheduler_source, "reschedule_task_for_cpu_once")
    for forbidden in ["is_waitpid_wait_channel", "waiting_for_pid", "complete_waitpid", "waitpid_repair"]:
        if forbidden in reschedule:
            fail(f"generic reschedule path must not own waitpid repair state: {forbidden}")
    lock_wrapper = function_body(scheduler_source, "with_event_wake_runqueue_lock")
    require_tokens(lock_wrapper, ["run_queues->with_lock_void(cpu_no", "std::forward<Func>(func)(rq)"],
                   "wake diagnostic callback retains the existing runqueue lock")
    final_lock_start = reschedule.rfind("with_event_wake_runqueue_lock(cpu_no, diagnostics")
    if final_lock_start < 0:
        fail("reschedule final runqueue lock callback is missing")
    final_lock_end = reschedule.find("if (!published_runnable)", final_lock_start)
    final_publication = reschedule[final_lock_start:final_lock_end]
    require_tokens(
        final_publication,
        [
            "task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE",
            "task->gc_queued.load(std::memory_order_acquire)",
            "task->wki_proxy_task_id != 0",
            "published_runnable = publish_runnable_task_locked",
        ],
        "locked reschedule anti-resurrection gate",
    )
    require_order(
        final_publication,
        "task->wki_proxy_task_id != 0",
        "published_runnable = publish_runnable_task_locked",
        "lifecycle/proxy revalidation must precede runnable publication",
    )
    shutdown_handoff = function_body(scheduler_source, "maybe_exit_current_kernel_thread_for_shutdown")
    require_order(
        shutdown_handoff,
        "task->owned_unpublished_process.load(std::memory_order_acquire) != nullptr",
        "task->preempt_pending = true",
        "daemon shutdown must defer while its unpublished child and stack locals are owned",
    )

    spawn = function_body(source, "wki_try_remote_spawn")
    require_order(
        spawn,
        "task->set_wki_proxy_task_id(tid)",
        "post_task_waiting(task)",
        "proxy identity must be visible before scheduler publication",
    )
    require_order(
        spawn,
        "task->wki_proxy_task = true",
        "post_task_waiting(task)",
        "proxy marker must be visible before scheduler publication",
    )
    require_order(
        spawn,
        "task->set_wait_channel(\"wki_execve_proxy\"",
        "post_task_waiting(task)",
        "fresh proxy wait identity must be visible before scheduler publication",
    )
    require_order(
        spawn,
        "post_task_waiting(task)",
        "delete[] task->elf_buffer",
        "proxy publication must succeed before local ELF ownership is released",
    )
    require_tokens(
        spawn,
        [
            "task->clear_wait_channel()",
            "task->wki_proxy_task = false",
            "task->set_wki_proxy_task_id(0)",
            "abandon_submitted_task_after_proxy_publish_failure(tid)",
            '"proxy-publication-failed"',
            "(STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL",
        ],
        "proxy publication failure retirement",
    )
    require_tokens(
        function_body(source, "abandon_submitted_task_after_proxy_publish_failure"),
        [
            "submitted_ipc_cleanup_snapshot_locked(submitted)",
            "consume_submitted_task_result_locked(submitted)",
            "send_task_cancel_request",
            "cleanup_submitted_ipc_exports(ipc_cleanup)",
        ],
        "failed proxy publication cleanup",
    )
    require_tokens(
        scheduler_header,
        [
            "enum class RemotePlacementResult : uint8_t",
            "LOCAL",
            "REMOTE",
            "FAILED",
            "RemotePlacementResult (*wki_try_remote_placement_fn)",
        ],
        "tri-state scheduler remote-placement contract",
    )
    require_tokens(
        function_body(scheduler_source, "post_task_balanced"),
        [
            "RESULT == RemotePlacementResult::REMOTE",
            "RESULT == RemotePlacementResult::FAILED",
            'log_rejected_task_publication("remote placement failed"',
        ],
        "scheduler must not locally execute terminal remote-placement failures",
    )
    require_tokens(
        function_body(source, "try_remote_placement"),
        [
            "case WkiRemoteSpawnResult::REMOTE",
            "case WkiRemoteSpawnResult::FAILED",
            "case WkiRemoteSpawnResult::LOCAL",
        ],
        "WKI-to-scheduler placement result mapping",
    )


def test_submit_send_failure_keeps_stack_waiter_exit_discoverable() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    function_bounds = [
        ("wki_task_submit_inline", "auto wki_task_submit_vfs_ref"),
        ("wki_task_submit_vfs_ref", "auto wki_task_wait"),
    ]
    for function_name, next_function in function_bounds:
        body_start = source.find(f"auto {function_name}(")
        body_end = source.find(next_function, body_start + 1)
        if body_start < 0 or body_end < 0:
            fail(f"missing function interval for {function_name}")
        body = source[body_start:body_end]
        failure_start = body.find("if (SEND_RET != WKI_OK)")
        failure_end = body.find("int const WAIT_RC", failure_start)
        if failure_start < 0 or failure_end < 0:
            fail(f"{function_name}: missing submit-send failure interval")
        failure = body[failure_start:failure_end]

        require_order(
            failure,
            "task_ptr->response_pending.store(false, std::memory_order_release)",
            "finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET)",
            f"{function_name} send failure must make the row terminal before quiescence",
        )
        require_order(
            failure,
            "finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET)",
            "task_ptr->response_consumer_wait_entry = nullptr",
            f"{function_name} send failure must retain the task-visible consumer through quiescence",
        )
        require_order(
            failure,
            "task_ptr->response_consumer_wait_entry = nullptr",
            "consume_submitted_task_result_locked(task_ptr)",
            f"{function_name} send failure must not drop result ownership before waiter retirement",
        )


def test_task_submit_envelopes_use_bounded_stack_storage() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    submitters = {
        "wki_task_submit_inline": (
            "auto wki_task_submit_vfs_ref(",
            "if (TOTAL > UINT16_MAX)",
        ),
        "wki_task_submit_vfs_ref": (
            "auto wki_task_wait(",
            "if (TOTAL > UINT16_MAX)",
        ),
    }
    for function_name, (next_function, size_guard) in submitters.items():
        body_start = source.index(f"auto {function_name}(")
        body = source[body_start : source.index(next_function, body_start + 1)]
        require_tokens(
            body,
            [
                size_guard,
                "auto const MSG_LEN = static_cast<uint16_t>(TOTAL)",
                "ipc_fd_count != 0 && ipc_fd_map == nullptr",
                "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> small_buf __attribute__((uninitialized));",
                "TaskSubmitBuffer large_buf(local_task",
                "uint8_t* const BUF = large_buf.data() != nullptr ? large_buf.data() : small_buf.data()",
                "reinterpret_cast<TaskSubmitPayload*>(BUF)",
                "uint8_t* cursor = BUF + sizeof(TaskSubmitPayload)",
                "send_task_submit_payload(target_node, SUBMIT_CHANNEL, TASK_ID, BUF, MSG_LEN)",
            ],
            f"{function_name} bounded submit envelope",
        )
        require_order(
            body,
            size_guard,
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> small_buf",
            f"{function_name} size check before stack use",
        )
        require_order(
            body,
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> small_buf",
            "send_task_submit_payload(target_node, SUBMIT_CHANNEL, TASK_ID, BUF, MSG_LEN)",
            f"{function_name} build before send",
        )
        for forbidden in ["delete[] large_buf", "delete[] BUF"]:
            if forbidden in body:
                fail(f"{function_name} must use RAII for its large TASK_SUBMIT envelope: found {forbidden}")
        if re.search(r"std::array<uint8_t,\s*WKI_ETH_MAX_PAYLOAD>\s+small_buf\s*(?:\{\}|=\s*\{\})", body):
            fail(f"{function_name} must not explicitly initialize the unused stack-envelope tail")

    fragment_sender = function_body(source, "send_task_submit_payload")
    require_tokens(
        fragment_sender,
        [
            "if (payload_len <= WKI_ETH_MAX_PAYLOAD)",
            "MsgType::TASK_SUBMIT, payload, payload_len",
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> frame{}",
            "TaskSubmitFragmentPayload",
            "WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX",
            "MsgType::TASK_SUBMIT_FRAGMENT",
        ],
        "large TASK_SUBMIT fragmentation with unchanged one-frame fast path",
    )

    require_tokens(
        source,
        [
            "static_assert(WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16)",
        ],
        "TASK_SUBMIT stack headroom and pre-narrowing context bounds",
    )
    require_tokens(
        function_body(source, "checked_submit_args_len"),
        [
            "static_cast<uint32_t>(argv_bytes)",
            "static_cast<uint32_t>(envp_bytes)",
            "static_cast<uint32_t>(cwd_len)",
            "if (TOTAL > UINT16_MAX)",
            "*args_len_out = static_cast<uint16_t>(TOTAL)",
        ],
        "checked submit argument length",
    )
    require_tokens(
        function_body(source, "build_submit_context_info"),
        ["checked_submit_args_len(argv_bytes, envp_bytes, info->cwd_len, &info->args_len)"],
        "submit context validates combined arguments before narrowing",
    )
    selftest_token = "wki_remote_compute_selftest_submit_context_lengths_are_checked"
    require_tokens(source, [f"auto {selftest_token}() -> bool"], "submit context length selftest implementation")
    require_tokens(header, [f"auto {selftest_token}() -> bool;"], "submit context length selftest declaration")
    require_tokens(
        ktest,
        ["SubmitContextLengthsAreCheckedBeforeNarrowing", selftest_token],
        "submit context length KTEST coverage",
    )

    wki = WKI_CPP.read_text()
    send_impl = function_body(wki, "wki_send_impl")
    require_order(
        send_impl,
        "copy_wki_payload_segments(frame + WKI_HEADER_SIZE, payload, payload_len, payload_tail, payload_tail_len)",
        "memcpy(rt_data, frame, frame_len)",
        "TASK_SUBMIT borrow copied into frame before retransmit ownership",
    )
    require_order(
        send_impl,
        "memcpy(rt_data, frame, frame_len)",
        "return WKI_OK",
        "TASK_SUBMIT borrow retained synchronously before send returns",
    )
    require_tokens(
        function_body(wki, "wki_send_on_channel_generation"),
        ["return wki_send_impl("],
        "generation-qualified sender synchronous delegation",
    )


def test_completion_queue_allocation_and_retry_preserve_ownership() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    queue = function_body(source, "queue_pending_completion_locked")
    require_tokens(queue, ["new (std::nothrow) PendingTaskCompletion(info)", "if (completion == nullptr) return false;"], "checked completion allocation")
    require_order(queue, "if (completion == nullptr)", "append_pending_completion_locked(completion)", "no failed publication")
    append = function_body(source, "append_pending_completion_locked")
    if "new " in append:
        raise AssertionError("completion retry must reuse its node without allocation")
    collector = function_body(source, "wki_remote_compute_check_completions")
    require_tokens(collector, ["if (!queue_pending_completion_locked(PendingTaskCompletion", "std::unique_ptr<PendingTaskCompletion> pending(pop_pending_completion_locked())", "append_pending_completion_locked(pending.release())"], "completion ownership")
    failure = collector[collector.index("if (!queue_pending_completion_locked"):collector.index("release_running_root_locked(rt)")]
    require_tokens(failure, ["continue;"], "allocation failure retains running ownership")
    for forbidden in ["rt.task->release()", "release_output_capture(rt.output)", "rt.output = nullptr", "rt.active = false"]:
        if forbidden in failure:
            raise AssertionError("failed completion allocation consumed receiver ownership")
    require_order(collector, "queue_pending_completion_locked", "release_running_root_locked(rt)", "durable completion before monitor release")
    require_tokens(function_body(source, "release_running_root_locked"), ["rt.task->release()", "rt.root_completion_queued = true", "rt.active = rt.service_accounting != nullptr"], "independent report lifetime")
    retry = collector[collector.index("append_pending_completion_locked(pending.release())"):]
    log = retry[retry.index("TASK_COMPLETE send deferred:"):retry.index("continue;", retry.index("TASK_COMPLETE send deferred:"))]
    if "info." in log:
        raise AssertionError("retry log reads node after ownership transfer")


def test_task_completion_uses_bounded_stack_storage() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()

    completion = function_body(source, "wki_remote_compute_check_completions")
    require_tokens(
        source,
        ["static_assert(WKI_COMPUTE_RX_PAYLOAD_MAX <= WKI_ETH_MAX_PAYLOAD);"],
        "bounded TASK_COMPLETE frame",
    )
    require_tokens(
        completion,
        [
            "info.output->freeze()",
            "static_cast<uint16_t>(OUTPUT.size())",
            "std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> buf __attribute__((uninitialized));",
            "reinterpret_cast<TaskCompletePayload*>(buf.data())",
            "complete->task_id = info.task_id",
            "complete->exit_status = info.exit_status",
            "complete->output_len = OUT_LEN",
            "complete->reserved = RESOURCE_LEN",
            "memcpy(buf.data() + sizeof(TaskCompletePayload), OUTPUT.data(), OUT_LEN)",
            "MsgType::TASK_COMPLETE, buf.data(), MSG_LEN",
        ],
        "stack-backed TASK_COMPLETE frame",
    )
    require_order(completion, "complete->reserved = RESOURCE_LEN", "memcpy(buf.data()", "completion header before output")
    require_order(completion, "memcpy(buf.data()", "MsgType::TASK_COMPLETE, buf.data(), MSG_LEN", "completion build before send")
    require_order(
        completion,
        "MsgType::TASK_COMPLETE, buf.data(), MSG_LEN",
        "if (SEND_RESULT != WKI_OK)",
        "completion send before requeue",
    )
    for forbidden in ["new (std::nothrow) uint8_t[msg_len]", "delete[] buf"]:
        if forbidden in completion:
            fail(f"TASK_COMPLETE must not heap-own its bounded envelope: found {forbidden}")


def test_proxy_waiting_publication_is_transactional() -> None:
    scheduler_source = SCHEDULER_CPP.read_text()
    scheduler_header = SCHEDULER_HPP.read_text()

    registration = function_body(scheduler_source, "register_fresh_task_visibility")
    require_tokens(
        registration,
        [
            "global_task_registry_lock.lock_irqsave()",
            "active_task_count >= MAX_ACTIVE_TASKS",
            "task->active_registry_index != ::ker::mod::sched::task::Task::ACTIVE_REGISTRY_INDEX_INVALID",
            'panic_handler("scheduler: fresh task already present in active registry")',
            "entry.pid == task->pid",
            "entry.task == task",
            'panic_handler("scheduler: fresh task already present in PID registry")',
            "pid_index == MAX_PIDS",
            "pid_slot(pid_index).task = task",
            "pid_slot(pid_index).pid = task->pid",
            "uint32_t const ACTIVE_INDEX = active_task_count++",
            "active_task_slot(ACTIVE_INDEX) = task",
            "task->active_registry_index = ACTIVE_INDEX",
            "global_task_registry_lock.unlock_irqrestore(FLAGS)",
        ],
        "atomic fresh-task registry publication",
    )
    require_order(
        registration,
        "entry.pid == task->pid",
        "pid_slot(pid_index).task = task",
        "fresh-task PID collision check before registry commit",
    )
    require_order(
        registration,
        "task->active_registry_index != ::ker::mod::sched::task::Task::ACTIVE_REGISTRY_INDEX_INVALID",
        "active_task_slot(ACTIVE_INDEX) = task",
        "fresh-task active collision check before registry commit",
    )
    commit = registration[registration.find("pid_slot(pid_index).task = task") :]
    require_order(commit, "pid_slot(pid_index).task = task", "pid_slot(pid_index).pid = task->pid", "fresh PID entry commit")
    require_order(
        commit,
        "pid_slot(pid_index).pid = task->pid",
        "active_task_slot(ACTIVE_INDEX) = task",
        "PID commit before active-list commit under one lock",
    )
    require_order(
        commit,
        "active_task_count++",
        "global_task_registry_lock.unlock_irqrestore(FLAGS)",
        "both fresh-task registries committed before visibility unlock",
    )

    publication = function_body(scheduler_source, "post_task_waiting")
    require_tokens(
        publication,
        [
            "task->sched_queue != task::Task::sched_queue::NONE",
            "task->heap_index >= 0",
            "task->sched_next != nullptr",
            "task == get_current_task()",
            "transition_make_waiting_locked(rq, task, TransitionPhase::PREPUBLICATION)",
            "published = register_fresh_task_visibility(task)",
            "if (!transition_detach_wait_locked(rq, task))",
            "transition_mark_detached_locked(rq, task, TransitionPhase::PREPUBLICATION)",
            "return published",
        ],
        "transactional fresh WAITING publication",
    )
    require_order(
        publication,
        "transition_make_waiting_locked(rq, task, TransitionPhase::PREPUBLICATION)",
        "published = register_fresh_task_visibility(task)",
        "wait-list park before global task visibility",
    )
    require_order(
        publication,
        "published = register_fresh_task_visibility(task)",
        "if (!transition_detach_wait_locked(rq, task))",
        "failed registration before wait-list rollback",
    )
    if "pid_table_insert(task)" in publication or "active_list_insert(task)" in publication:
        fail("fresh WAITING publication must not expose PID and active registries separately")

    require_tokens(
        scheduler_header,
        [
            "Transactionally publish an exclusively owned, non-current task",
            "no registry/runqueue membership",
            "false guarantees no PID/active or runqueue membership remains",
        ],
        "fresh WAITING scheduler API contract",
    )


def test_load_report_uses_cpu_accounting_and_shared_local_cache() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    procfs = PROCFS_CPP.read_text()

    require_tokens(
        source,
        [
            "auto wki_local_node_load_pct() -> uint16_t",
            "auto compute_local_runnable_load_fallback() -> uint16_t",
            "auto snapshot_total_time_us(ker::mod::sched::CpuAccountingSnapshot const& snapshot) -> uint64_t",
            "auto snapshot_busy_time_us(ker::mod::sched::CpuAccountingSnapshot const& snapshot) -> uint64_t",
            "auto snapshot_delta_pct_milli(",
        ],
        "local load accounting helpers",
    )

    body = function_body(source, "wki_load_report_send")
    require_tokens(
        body,
        [
            "ker::mod::sched::get_cpu_accounting_snapshot(c)",
            "snapshot_delta_pct_milli(current_accounting.at(c), previous)",
            "report.avg_load_pct = compute_local_runnable_load_fallback()",
            "g_cached_local_load_pct = report.avg_load_pct",
            "g_cached_local_load_update_us = NOW",
        ],
        "load report accounting-based utilization",
    )
    require_order(
        body,
        "g_cached_local_load_pct = report.avg_load_pct",
        "memcpy(buf.data(), &report, sizeof(LoadReportPayload))",
        "local load cache must update before payload serialization",
    )

    peers_body = function_body(procfs, "generate_wki_peers")
    require_tokens(
        peers_body,
        [
            "ker::net::wki::wki_local_node_load_pct()",
        ],
        "procfs local peer row should reuse the shared local load metric",
    )


def test_receiver_path_localization_bounds_suffix_scan() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "localize_receiver_logical_path")
    for snippet in [
        "PATH_LEN = std::strlen(path)",
        "PATH_END = path + PATH_LEN",
        "while (suffix < PATH_END && *suffix == '/')",
    ]:
        if snippet not in body:
            fail(f"localize_receiver_logical_path must bound the slash-skip scan: {snippet}")


def test_vfs_ref_loader_rejects_null_or_empty_path_before_vfs_use() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "load_elf_from_vfs_path")
    guard = "if (path == nullptr || path[0] == '\\0')"
    for later in [
        "localize_receiver_logical_path(path",
        "fallback_to_local_path_for_disconnected_wki_host(resolved_path",
        "ker::vfs::vfs_open_read_file_as(*CURRENT_TASK",
        "std::strncpy(pending.path.data(), cache_path.data()",
    ]:
        require_order(body, guard, later, "VFS_REF loader must reject null/empty path before VFS/cache use")


def test_vfs_ref_loader_deadlines_are_saturating() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "load_elf_from_vfs_path")

    for snippet in [
        "#include <net/wki/timer_math.hpp>",
        "uint64_t inflight_deadline_us = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US)",
        "uint64_t retry_deadline_us = wki_future_deadline_us(RETRY_WINDOW_START_US, WKI_VFS_LOAD_RETRY_WINDOW_US)",
        "wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US)",
    ]:
        if snippet not in source:
            fail(f"VFS_REF loader deadline helper use is missing: {snippet}")

    forbidden = [
        "wki_now_us() + WKI_TASK_SUBMIT_VFS_TIMEOUT_US",
        "RETRY_WINDOW_START_US + WKI_VFS_LOAD_RETRY_WINDOW_US",
        "wki_now_us() + WKI_VFS_LOAD_RETRY_BACKOFF_US",
    ]
    present = [token for token in forbidden if token in body]
    if present:
        fail("VFS_REF loader must not use wrapping deadline arithmetic: " + ", ".join(present))

    require_order(
        body,
        "s_compute_lock.unlock();",
        "if (wki_now_us() >= inflight_deadline_us || !request_is_current())",
        "inflight wait timeout must be checked after dropping compute lock",
    )
    # Open and fstat now share one retained-file retry branch; short reads keep the other backoff.
    if body.count("sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US)") < 2:
        fail("VFS_REF loader must keep retry backoff sleeps on every retry path")


def test_shared_elf_cache_preserves_inflight_load_markers() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    gc_body = function_body(source, "gc_shared_elf_cache_locked")
    require_tokens(
        gc_body,
        [
            "!it->loading && it->refcount == 0",
            "it->refcount != 0 || it->loading",
        ],
        "shared ELF cache in-flight GC exclusion",
    )

    loader_body = function_body(source, "load_elf_from_vfs_path")
    require_tokens(
        loader_body,
        [
            "std::array<char, 512> cache_path = {}",
            "std::strncpy(cache_path.data(), resolved_path, cache_path.size() - 1)",
            "find_shared_elf_cache_entry_locked(session, cache_path.data(), cache_key)",
            "find_shared_elf_cache_locked(session, cache_path.data(), cache_key)",
            "pending.session = session",
            "pending.load_token = next_nonzero_token(s_next_shared_elf_cache_load_token)",
            "std::strncpy(pending.path.data(), cache_path.data(), pending.path.size() - 1)",
            "shared_elf_freshness_matches(cache_key, retry_stat)",
            "shared_elf_freshness_matches(cache_key, post_read_stat)",
        ],
        "shared ELF cache immutable single-flight key",
    )
    require_order(
        loader_body,
        "std::strncpy(cache_path.data(), resolved_path, cache_path.size() - 1)",
        "auto fail_inflight_load = [&]()",
        "cache key snapshot before failure cleanup",
    )
    cache_snapshot = loader_body.find("std::strncpy(cache_path.data(), resolved_path, cache_path.size() - 1)")
    if "resolved_path = fallback_local_path.data()" in loader_body[cache_snapshot:]:
        fail("VFS_REF retry must not publish fallback bytes under the original cache identity")

    require_tokens(
        loader_body,
        [
            "// marker or launch them uncached for the retired request.",
            "if (is_loader)",
            "result.reject_reason = TaskRejectReason::OVERLOADED",
        ],
        "retired loader rejects instead of launching uncached bytes",
    )

    cleanup_body = function_body(source, "wki_remote_compute_cleanup_for_peer")
    require_tokens(
        cleanup_body,
        [
            "if (entry.loading)",
            "entry.loading = false",
            "entry.load_status = -1",
            "entry.valid = false",
        ],
        "peer cleanup cancels in-flight shared ELF markers",
    )
    require_order(cleanup_body, "entry.loading = false", "entry.valid = false", "cancel loader before cache invalidation")


def test_remote_stdio_capture_is_write_only_and_non_tty() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    capture_isatty = function_body(source, "capture_isatty")
    if "return false;" not in capture_isatty:
        fail("remote stdout/stderr capture must not report tty semantics")

    body = function_body(source, "install_remote_stdio")
    require_tokens(body, ["file->open_flags = fd == 0 ? 0 : 1", "file->fs_type = ker::vfs::FSType::TMPFS",
                          "file->fops = fd == 0 ? &g_stdin_null_fops : &g_capture_fops",
                          "if ((stdio_mask & (1U << fd)) == 0) continue", "task.fd_table.remove(previous)"],
                   "inherited stdio presence, access modes and rollback")
    require_order(body, "new (std::nothrow)", "task.fd_table_lock.lock_irqsave()", "allocate before FD lock")
    require_order(body, "task.fd_table.insert(", "ker::vfs::fd_usage_changed_locked(task)", "observe only committed inherited FDs")


def test_large_vfs_ref_exec_keeps_a_file_backed_source() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    loader_body = function_body(source, "load_elf_from_vfs_path")
    require_tokens(
        loader_body,
        [
            "std::cmp_greater_equal(statbuf.st_size, WKI_FILE_BACKED_ELF_MIN_SIZE)",
            "result.file = source.release()",
            "ker::syscall::process::supports_file_backed_process(source.get()",
            "result.file_stat = statbuf",
            "return result;",
            "buf = new uint8_t[file_size]",
        ],
        "large VFS_REF retained-file source",
    )
    require_order(
        loader_body,
        "std::cmp_greater_equal(statbuf.st_size, WKI_FILE_BACKED_ELF_MIN_SIZE)",
        "buf = new uint8_t[file_size]",
        "large VFS_REF must return a retained file before the legacy full-buffer allocation",
    )

    submit_body = function_body(source, "handle_task_submit_work")
    require_tokens(
        submit_body,
        [
            "vfs_result.buffer == nullptr && vfs_result.file == nullptr",
            "elf_file = vfs_result.file",
            "exec_elf_file(elf_file, binary_len, elf_file_stat, submitted_resources, mapped_credentials, stdio_mask)",
        ],
        "large VFS_REF file-backed execution",
    )


def test_submitted_vfs_policy_is_active_during_elf_construction() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()
    body = function_body(source, "handle_task_submit_work")

    require_tokens(
        source,
        [
            "saved_vfs_rules = std::move(task->wki_vfs_rules)",
            "submitted_policy_valid = deserialize_task_vfs_rules(task, policy_data, policy_len)",
            "submitted_task->wki_vfs_rules = std::move(task->wki_vfs_rules)",
            "task->wki_vfs_rules = std::move(saved_vfs_rules)",
        ],
        "submitted VFS policy scope",
    )
    require_order(body, "policy_cursor =", "ScopedSubmitVfsIdentity submit_vfs_identity", "policy cursor before scoped install")
    require_order(
        body,
        "if (reject_reason != TaskRejectReason::ACCEPTED || !submit_vfs_identity.policy_valid())",
        "ExecResult const EXEC =",
        "submitted policy validation before ELF construction",
    )
    require_order(
        body,
        "ExecResult const EXEC =",
        "submit_vfs_identity.transfer_vfs_rules_to(new_task)",
        "validated policy transfer after ELF construction",
    )
    if "deserialize_task_vfs_rules(new_task" in body:
        fail("submitted VFS policy must not be deferred until after Task construction")

    token = "wki_remote_compute_selftest_submit_policy_scope_restores_worker"
    require_tokens(source, [f"auto {token}() -> bool"], "submitted policy scope selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "submitted policy scope selftest declaration")
    require_tokens(ktest, ["SubmitPolicyScopeRestoresWorker", token], "submitted policy scope KTEST coverage")


def test_controller_descendants_default_to_local_placement() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "handle_task_submit_work")

    require_order(
        body,
        "apply_submitted_task_identity(",
        "new_task->wki_target_hostname.front() = '\\0'",
        "receiver placement must override inherited submitter targeting",
    )
    require_tokens(
        body,
        [
            "new_task->wki_target_hostname.front() = '\\0'",
            "new_task->wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL",
            "new_task->wki_skip_legacy_placement = true",
        ],
        "receiver-created task local placement",
    )

    spawn_body = function_body(source, "wki_try_remote_spawn")
    explicit_start = spawn_body.find("if (EXPLICIT_TARGET) {")
    peer_state_check = spawn_body.find("auto* peer = wki_peer_find(node_id)", explicit_start)
    if explicit_start < 0 or peer_state_check < 0:
        fail("explicit-target spawn branch is missing")
    explicit_body = spawn_body[explicit_start:peer_state_check]
    require_tokens(
        explicit_body,
        [
            "const char* const TARGET_HOSTNAME = TARGET",
            "uint16_t node_id = wki_peer_find_by_hostname(TARGET_HOSTNAME)",
            'constexpr std::string_view WOS_DOMAIN_SUFFIX = ".wos"',
            "node_id = wki_peer_find_by_hostname(short_target.data())",
            "if (node_id == g_wki.my_node_id)",
            'log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "explicit-local-target")',
            "return finish_placement(WkiRemoteSpawnResult::LOCAL)",
        ],
        "exact-first .wos target fallback and launcher-self local placement",
    )
    require_order(
        explicit_body,
        "uint16_t node_id = wki_peer_find_by_hostname(TARGET_HOSTNAME)",
        'constexpr std::string_view WOS_DOMAIN_SUFFIX = ".wos"',
        "exact hostname lookup before .wos fallback",
    )
    require_order(
        explicit_body,
        "node_id = wki_peer_find_by_hostname(short_target.data())",
        "if (node_id == g_wki.my_node_id)",
        "self target check after .wos fallback",
    )


def test_submission_errors_and_ipc_suffix_framing() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    prepare = function_body(source, "prepare_submitted_resources")
    require_tokens(prepare, ["submit_context + submit_context_len - IPC_BYTES", "ipc_data + index * sizeof(entry)",
                             "fallback_mask &=", "(*stdio_mask & BIT) == 0"], "IPC suffix excludes optional VFS policy")
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        require_tokens(body, ["*error_out = 0", "*error_out = error", "task_transport_errno(SEND_RET)",
                              "task_transport_errno(WAIT_RC)", "task_rejection_errno(accept_status)"], "specific submit errno")
        require_order(body, "cleanup_submit_ipc()", "fail_submit(task_transport_errno(SEND_RET))",
                      "release ownership before reporting send failure")
    placement = function_body(source, "wki_try_remote_spawn")
    require_tokens(placement, ["&placement_error", "*spec.error_out =", "CAN_RETRY_INLINE", "placement_error = -EAGAIN"],
                   "preserve placement error and admission failure")
    exec_source = PROCESS_EXEC_CPP.read_text()
    for name in ["wos_proc_exec_impl", "wos_proc_execve_impl"]:
        body = multiline_function_body(exec_source, name)
        tokens = ([".error_out = &remote_error", "return static_cast<uint64_t>(remote_error)"] if name == "wos_proc_exec_impl" else
                  ["try_process_remote_exec(*task", "exec_stat, EXEC_NOSUID, remote_error", "static_cast<uint64_t>(remote_error)"])
        require_tokens(body, tokens, "raw syscall preserves negative errno")


def test_resource_contract_preserves_baseline_and_completion_ownership() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        require_tokens(" ".join(body.split()), ["wki_peer_capability_negotiated(target_node, WKI_CAP_RESOURCE_LIMITS)",
                              "snapshot_task_resources(local_task, *resources, transfer != nullptr && transfer->cutover != nullptr)", "WKI_CHILD_CPU_RESOURCE_ID", "st.resource_baseline = resources",
                              "copy_submit_identity_data(cursor, local_task, resources, SUBMIT_CREDENTIALS, submit_filesystem.root, FULL_CREDENTIALS, exec_signals, ROUTED_HOME, FILESYSTEM)"],
                       "negotiated submission uses one retained resource baseline")
        require_order(body, "snapshot_task_resources(", "s_compute_lock.lock()", "snapshot outside compute lock")
    snapshot = function_body(source, "snapshot_task_resources")
    require_order(snapshot, "resource_lock.unlock_irqrestore", "child_events::read_reaped_cpu_times", "no resource lock across lifecycle observation")
    receiver = function_body(source, "handle_task_submit_work")
    require_order(receiver, "prepare_submitted_resources(", "child_events::import_reaped_cpu_times(*new_task", "validated observations before receiver installation")
    complete = function_body(source, "handle_task_complete")
    require_order(complete, "resource_contract::decode(", "s_compute_lock.lock()", "aligned decode before compute lock")
    require_order(complete, "resource_contract::valid_completion(", "take_local_task_ref()", "validate baseline before ownership transfer")
    require_tokens(complete, ["task->resource_completion = resource_completion", "task->active = false"], "retain terminal resource record")
    finalize = (function_body(source, "prepare_proxy_finalization") + function_body(source, "finish_proxy_exit")
                + function_body(source, "run_one_proxy_exit"))
    require_order(finalize, "transition_state(", "merge_proxy_resource_completion(", "one winning finalizer merges usage")
    service_source = (ROOT / "modules/kern/src/platform/sched/service_resource.cpp").read_text()
    guard_release = re.search(r"ServiceResourceMerge::~ServiceResourceMerge\(\)\s*\{([^}]*)\}", service_source)
    if guard_release is None:
        fail("missing service observation guard destructor")
    require_order(guard_release.group(1), "resource_lock.unlock_irqrestore(", "service_lock.unlock_irqrestore(", "service observation guard releases locks in reverse order")
    require_order(finalize, "merge_proxy_resource_completion(", "child_events::publish_exit(", "resource accounting before child event")
    merge = function_body(source, "merge_proxy_resource_completion")
    require_tokens(merge, ["if (!merged) return false;", "completion.cpu_user_time_us - baseline.cpu_user_time_us",
                           "completion.cpu_system_time_us - baseline.cpu_system_time_us", "completion.reaped_user_time_us",
                           "completion.reaped_system_time_us"], "validated exact remote CPU modes and child deltas")
    require_order(merge, "ServiceResourceMerge observation_guard(proxy)", "resource_contract::merge_completion(", "service snapshots serialized with task merge")
    require_order(merge, "resource_contract::merge_completion(", "observation_guard.exclude_reported_denials()", "only successful task merge can exclude report-counted denials")
    require_tokens(merge, ["if (merged && SERVICE_REPORTS)", "if (!SERVICE_REPORTS)"], "service report flag selects one accounting path")
    collector = function_body(source, "wki_remote_compute_check_completions")
    require_tokens(collector, ["resources->reaped_user_time_us = task->child_user_time_us",
                              "resources->reaped_system_time_us = task->child_system_time_us"], "stable reaped CPU completion deltas")
    require_tokens(collector, ["running_remote_task_accounting_ready(rt, *task)", "resource_contract::snapshot(task->resources)",
                              "info.resources.has_value()", "complete->reserved = RESOURCE_LEN"], "stable receiver completion snapshot")
    readiness = function_body(source, "running_remote_task_accounting_ready")
    require_tokens(readiness, ["exit_notify_ready.load(std::memory_order_acquire)", "task.has_exited",
                               "resource_cpu_threads.load(std::memory_order_acquire) == 0",
                               "resource_deferred_observations.load(std::memory_order_acquire) == 0"],
                   "completed CPU and deferred core observations before snapshot")
    if "resource_thread_owners.load" in collector + readiness:
        fail("completion must not depend on thread metadata GC")
    require_order(finalize, "cleanup_proxy_resources(proxy)", "proxy->exit_notify_ready.store", "proxy cleanup before receiver completion")
    require_order(finalize, "proxy->process_charge.release()", "proxy->exit_notify_ready.store", "proxy UID release before receiver completion")
    for name in ["wki_try_remote_spawn", "wki_proxy_task_blocked"]:
        body = function_body(source, name)
        require_tokens(body, ["resource_baseline =", "resource_completion =", "resource_completion ? &*resource_completion : nullptr"],
                       "early completion retains both resource records")


def test_resource_rejection_preserves_admission_observations() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    destroy = function_body(source, "destroy_unpublished_remote_process")
    require_order(destroy, "*failure_observations = task->resources", "destroy_owned_unpublished_process(",
                  "retain admission observations before unpublished child destruction")
    require_order(destroy, "resource_lock.unlock_irqrestore(", "destroy_owned_unpublished_process(",
                  "teardown callbacks outside resource lock")
    submit = function_body(source, "handle_task_submit_work")
    for call in re.findall(r"destroy_unpublished_remote_process\([^;]+;", submit):
        if "&submitted_resources" not in call:
            fail(f"receiver rejection teardown loses observations: {call}")
    require_tokens(submit, ["resource_observations_valid", "resource_contract::snapshot(submitted_resources)",
                            "discard_unpublished_running_task(current_task, worker_task_ref, rt, &submitted_resources)"],
                   "validated contract observations survive every rejection teardown")
    file_exec = function_body((ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text(), "create_file_backed_process_task")
    require_order(file_exec, "*failure_observations = new_task->resources", "destroy_owned_unpublished_process(",
                  "file-backed admission observation output precedes destruction")
    reject = function_body(source, "handle_task_reject")
    require_order(reject, "resource_contract::decode(", "s_compute_lock.lock()", "decode outside compute lock")
    require_order(reject, "resource_contract::valid_rejection(", "claim_and_clear_waiter_locked(",
                  "invalid rejection cannot consume the response waiter")
    require_order(reject, "claim_and_clear_waiter_locked(", "resource_contract::merge_rejection(",
                  "one response claimant merges admission observations")
    require_order(reject, "resource_contract::merge_rejection(", "response_pending.store(false",
                  "observations precede response publication")


def test_receiver_child_owner_spans_interpreter_output_and_publication() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    submit_body = function_body(source, "handle_task_submit_work")
    require_order(submit_body, "resource_submission_admission(", "prepare_submitted_resources(", "receiver requires negotiated contract before decode or fetch")
    require_order(submit_body, "prepare_submitted_resources(", "load_elf_from_vfs_path(",
                  "receiver resource validation before executable fetch")
    prepare_body = function_body(source, "prepare_submitted_resources")
    require_order(prepare_body, "IDENTITY_END > submit_context_len - IPC_BYTES", "resource_contract::decode(",
                  "resource suffix bounds before typed decode")
    require_tokens(prepare_body, ["resource_contract::restore(", "resource_contract::cpu_exhausted(record)",
                                  "TaskRejectReason::RESOURCE_INVALID", "TaskRejectReason::RESOURCE_CPU"],
                   "receiver policy and CPU admission")
    placement = function_body(source, "wki_try_remote_spawn")
    require_order(placement, "if (tid == 0)", "close_proxy_fd_table(task, true)", "CLOEXEC close only after submit acceptance")
    require_order(placement, "close_proxy_fd_table(task, true)", "bool const NEEDS_PROXY_PUBLICATION",
                  "potentially blocking CLOEXEC cleanup before proxy identity/publication")
    exec_body = function_body(source, "exec_elf_buffer")
    require_order(
        exec_body,
        "new_task->elf_buffer = elf_buffer",
        "claim_unpublished_process(OWNER, new_task)",
        "receiver must attach its ELF before publishing fatal-exit recovery ownership",
    )
    require_order(
        exec_body,
        "claim_unpublished_process(OWNER, new_task)",
        "complete_unpublished_process_construction(new_task",
        "receiver must publish child ownership before blocking PT_INTERP loading",
    )
    require_order(
        exec_body,
        "complete_unpublished_process_construction(new_task",
        "finish_remote_exec_task(new_task, stdio_mask, resources)",
        "receiver must finish the owned interpreter stage before output setup",
    )
    require_tokens(
        exec_body,
        [
            "!new_task->initialize_process_image(elf_buffer, binary_len)",
            'panic_handler("WKI remote compute: cannot publish receiver child recovery ownership")',
            'destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-construction", &resources)',
            'destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-interpreter", &resources)',
        ],
        "receiver construction failure ownership",
    )
    if "release_unpublished_process(" in exec_body:
        fail("exec_elf_buffer must return with the receiver child owner slot intact")
    require_order(exec_body, "ProcessReservation process_reservation(resources, credentials.user().real)",
                  'kernel_stack_alloc("wki_exec_kstack")', "receiver NPROC admission before kernel stack")
    require_order(exec_body, "new_task->resources = resources", "initialize_process_image(elf_buffer, binary_len)",
                  "receiver policy before image/stack allocation")
    require_order(exec_body, "process_reservation.attach(*new_task)", "initialize_process_image(elf_buffer, binary_len)",
                  "receiver population charge before image/stack allocation")

    boot_read = function_body(TASK_CPP.read_text(), "read_boot_file_fully")
    require_order(
        boot_read,
        "ker::vfs::vfs_open_read_file_as(path_context, CREDENTIALS, path,",
        "boot_file_cache_lookup_copy(cache_key.data(), freshness",
        "interpreter cache bytes require successful read admission",
    )
    require_order(
        boot_read,
        "ker::vfs::vfs_fstat_file(file, &freshness)",
        "boot_file_cache_lookup_copy(cache_key.data(), freshness",
        "interpreter cache freshness comes from the admitted open object",
    )
    if "preopen_freshness" in boot_read or boot_read.count("boot_file_cache_lookup_copy(") != 1:
        fail("interpreter cache must not bypass open authorization")

    work_body = function_body(source, "handle_task_submit_work")
    require_order(
        work_body,
        "ExecResult const EXEC =",
        "publish_running_task_locked(rt)",
        "receiver child ownership must cover output and prepublication monitor setup",
    )
    require_order(
        work_body,
        "bool const POSTED = post_admitted_remote_process(",
        "release_unpublished_process(current_task, new_task)",
        "receiver may release child recovery ownership only after scheduler publication",
    )
    require_tokens(
        work_body,
        [
            "POSTED && !ker::mod::sched::task::release_unpublished_process(current_task, new_task)",
            "destroy_unpublished_remote_process(current_task, new_task,",
        ],
        "receiver publication and failure ownership split",
    )


def test_receiver_vfs_ref_submit_uses_bounded_worker_pool() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()
    wki = WKI_CPP.read_text()
    wki_header = WKI_HPP.read_text()
    channel = CHANNEL_CPP.read_text()
    peer = PEER_CPP.read_text()
    smt = SMT_CPP.read_text()

    require_tokens(
        source,
        [
            "constexpr size_t WKI_COMPUTE_SUBMIT_WORKER_MAX = 8",
            "constexpr size_t WKI_COMPUTE_SUBMIT_QUEUE_MAX = 64",
            "std::array<PendingTaskSubmit, WKI_COMPUTE_SUBMIT_QUEUE_MAX>",
            "std::array<CancelledTaskSubmit, WKI_COMPUTE_SUBMIT_CANCEL_MAX>",
            "std::array<ker::mod::sched::task::Task*, WKI_COMPUTE_SUBMIT_WORKER_MAX>",
            "std::atomic<size_t> s_compute_submit_task_count{0}",
            "compute_submit_worker_target(ker::mod::smt::get_core_count())",
        ],
        "bounded compute-submit pool",
    )

    start_body = function_body(source, "wki_remote_compute_start_submit_thread")
    require_tokens(
        start_body,
        [
            "for (size_t i = 0; i < TARGET_WORKERS; ++i)",
            "Task::create_kernel_thread(WORKER_NAMES.at(i), wki_compute_submit_thread)",
            "post_task_balanced(task)",
            "destroy_unpublished_compute_submit_worker(task)",
            "s_compute_submit_tasks.at(started_workers++) = task",
            "s_compute_submit_task_count.store(started_workers, std::memory_order_release)",
        ],
        "compute-submit pool startup",
    )
    require_order(
        start_body,
        "post_task_balanced(task)",
        "s_compute_submit_tasks.at(started_workers++) = task",
        "compute-submit worker publication after scheduler post",
    )
    destroy_body = function_body(source, "destroy_unpublished_compute_submit_worker")
    require_tokens(
        destroy_body,
        [
            "delete reinterpret_cast<ker::mod::cpu::PerCpu*>(task->context.syscall_scratch_area)",
            "delete[] task->name",
            "phys::page_free(",
            "delete task",
        ],
        "failed compute-submit worker cleanup",
    )

    notify_body = function_body(source, "wki_remote_compute_notify_pending_submit")
    require_tokens(
        notify_body,
        [
            "s_compute_submit_task_count.load(std::memory_order_acquire)",
            "for (size_t i = 0; i < WORKER_COUNT; ++i)",
            "wake_task_from_event(s_compute_submit_tasks.at(i))",
        ],
        "compute-submit wake-all",
    )

    require_tokens(
        source,
        [
            "constexpr size_t WKI_COMPUTE_RX_WORKER_MAX = 4",
            "WKI_COMPUTE_RX_QUEUE_MAX = WKI_CREDITS_RESOURCE * WKI_COMPUTE_RX_WORKER_MAX",
            "WKI_COMPUTE_RX_PAYLOAD_MAX = std::max(sizeof(TaskCompletePayload) + WKI_TASK_MAX_OUTPUT, WKI_ETH_MAX_PAYLOAD)",
            "std::array<PendingComputeRx, WKI_COMPUTE_RX_QUEUE_MAX>",
            "std::array<ComputeRxShard, WKI_COMPUTE_RX_WORKER_MAX>",
            "std::array<bool, WKI_COMPUTE_RX_QUEUE_MAX>",
            "uint8_t type = 0",
        ],
        "bounded zero-initialized compute RX pool",
    )
    admission_body = function_body(source, "wki_remote_compute_admit_rx")
    require_tokens(admission_body, ["queue_compute_rx_work(type, hdr, payload, copy_len, rx_channel, rx_channel_generation)"],
                   "classified compute RX forwards to the shared bounded copy")
    admission_body += function_body(source, "queue_compute_rx_work")
    require_tokens(
        admission_body,
        [
            "classify_compute_rx_payload(type, payload, payload_len, &copy_len)",
            "s_compute_rx_task_count.load(std::memory_order_acquire)",
            "s_compute_rx_slot_used.at(CANDIDATE) = true",
            "std::memcpy(slot.payload.data(), payload, copy_len)",
            "shard.slot_indices.at(shard.tail)",
            "return WkiComputeRxAdmission::RETRY",
            "return WkiComputeRxAdmission::DEFERRED",
        ],
        "preallocated compute RX admission",
    )
    if "new " in admission_body or "kern_" in admission_body or "s_compute_lock" in admission_body:
        fail("compute RX admission must not allocate, wake/block, or invert channel->compute lock order")

    rx_start_body = function_body(source, "wki_remote_compute_start_rx_threads")
    require_tokens(
        rx_start_body,
        [
            "compute_rx_worker_target(ker::mod::smt::get_core_count())",
            "Task::create_kernel_thread(WORKER_NAMES.at(i), WORKER_ENTRIES.at(i))",
            "post_task_balanced(task)",
            "s_compute_rx_tasks.at(i) = task",
            "s_compute_rx_task_count.store(started_workers, std::memory_order_release)",
        ],
        "compute RX worker startup",
    )
    require_tokens(
        function_body(source, "wki_remote_compute_notify_deferred_rx"),
        ["compute_rx_worker_index(src_node, WORKER_COUNT)", "wake_task_from_event(s_compute_rx_tasks.at(WORKER_INDEX))"],
        "race-safe compute RX worker wake",
    )
    require_tokens(
        function_body(source, "compute_rx_worker_loop"),
        [
            "dequeue_compute_rx(worker_index, &slot_index)",
            "PeerLifecycleLease lifecycle",
            "compute_rx_identity_matches(work, *peer, g_wki.capabilities)",
            "compute_submit_channel_token_matches(",
            "detail::handle_task_complete(",
            "detail::handle_task_cancel(",
            "release_compute_rx_slot(slot_index)",
        ],
        "task-context compute RX dispatch",
    )
    require_tokens(smt, ["wki_remote_compute_start_rx_threads()"], "compute RX startup after scheduler initialization")

    handler_body = function_body(source, "handle_task_submit")
    require_tokens(handler_body, ["queue_admitted_task_submit(hdr, payload, payload_len, SESSION)"], "captured session passed to queue")
    handler_body += function_body(source, "queue_admitted_task_submit")
    require_tokens(
        handler_body,
        [
            "capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation, IDENTITY)",
            "wki_send_on_channel_generation(hdr->src_node, SESSION.rx_channel, SESSION.rx_channel_generation",
            "s_compute_submit_task_count.load(std::memory_order_acquire) == 0",
            "new (std::nothrow) uint8_t[payload_len]",
            "std::memcpy(copy, payload, payload_len)",
            "s_pending_task_submit_count < WKI_COMPUTE_SUBMIT_QUEUE_MAX",
            "g_pending_task_submits.at(s_pending_task_submit_tail) = pending",
            "s_pending_task_submit_count++",
            "mode == TaskDeliveryMode::INLINE ? WKI_TASK_SUBMIT_INLINE_RECEIVER_TIMEOUT_US",
            "pending.deadline_us = wki_future_deadline_us(pending.queued_at_us, RESPONSE_TIMEOUT_US)",
            "pending.session = SESSION",
            "delete[] copy",
            "reject_submit(TaskRejectReason::OVERLOADED)",
            "wki_remote_compute_notify_pending_submit()",
        ],
        "all-mode TASK_SUBMIT deferral",
    )
    if "handle_task_submit_work(" in handler_body:
        fail("TASK_SUBMIT RX handler must defer INLINE as well as VFS/RESOURCE construction")
    if "push_back(" in handler_body:
        fail("TASK_SUBMIT RX handler must not allocate queue storage under its spinlock")
    require_order(
        handler_body,
        "g_pending_task_submits.at(s_pending_task_submit_tail) = pending",
        "wki_remote_compute_notify_pending_submit()",
        "TASK_SUBMIT publish before worker wake",
    )
    overflow_start = handler_body.find("if (!queued)")
    overflow_end = handler_body.find("wki_remote_compute_notify_pending_submit()", overflow_start)
    overflow_block = handler_body[overflow_start:overflow_end]
    require_order(
        overflow_block,
        "delete[] copy",
        "reject_submit(TaskRejectReason::OVERLOADED)",
        "TASK_SUBMIT queue overflow payload release before rejection",
    )

    drain_body = function_body(source, "drain_pending_task_submits")
    require_order(
        drain_body,
        "PendingTaskSubmit pending = g_pending_task_submits.at(s_pending_task_submit_head)",
        "handle_task_submit_work(",
        "dequeue before task construction",
    )
    dequeue_start = drain_body.find("PendingTaskSubmit pending = g_pending_task_submits.at(s_pending_task_submit_head)")
    dequeue_end = drain_body.find("handle_task_submit_work(", dequeue_start)
    dequeue_block = drain_body[dequeue_start:dequeue_end]
    require_order(dequeue_block, "s_pending_task_submit_count--", "s_pending_submit_lock.unlock()", "dequeue count under queue lock")
    require_order(drain_body, "handle_task_submit_work(", "delete[] pending.payload", "payload release after task construction")
    require_tokens(
        source,
        [
            "auto submit_deadline_expired = [&]() -> bool",
            "deadline_us != 0 && wki_now_us() >= deadline_us",
            "reject_reason == TaskRejectReason::ACCEPTED && submit_deadline_expired()",
            "handle_task_submit_work(pending.src_node, pending.payload, pending.payload_len, pending.deadline_us, pending.session)",
        ],
        "queued submit deadline and session enforcement",
    )
    work_body = function_body(source, "handle_task_submit_work")
    require_tokens(
        work_body,
        [
            "compute_submit_admission_is_current_locked(session)",
            "take_cancelled_task_submit_locked(session, submit->task_id)",
            "wki_send_on_channel_generation(src_node, session.rx_channel, session.rx_channel_generation",
            "Task submit expired before post",
            "perf_compute_reject_status(TaskRejectReason::OVERLOADED)",
            "rt.published = false",
            "rt.accept_pending = false",
            "rt.submit_session_epoch = session.epoch",
            "publish_running_task_locked(rt)",
            "bool const POSTED = post_admitted_remote_process(",
            "published_row->published = true",
            "published_row->accept_pending = send_accept",
            "worker_task_ref = new_task",
            "worker_task_ref->release()",
        ],
        "final submit session, cancellation, publication, and accept fence",
    )
    publication_block = work_body[work_body.find("publish_running_task_locked(rt)") :]
    require_order(
        publication_block,
        "publish_running_task_locked(rt)",
        "s_compute_lock.unlock()",
        "running row must be published before releasing the compute lock",
    )
    require_order(
        publication_block,
        "s_compute_lock.unlock()",
        "bool const POSTED = post_admitted_remote_process(",
        "scheduler publication must not run under the compute spinlock",
    )
    successful_publication = work_body[
        work_body.find("published_row->published = true") :
    ]
    require_order(
        successful_publication,
        "task_process_owner_pid(new_task)",
        "worker_task_ref->release()",
        "worker task reference must cover post-publication cancellation",
    )
    require_order(
        successful_publication,
        "new_task->has_exited",
        "worker_task_ref->release()",
        "worker task reference must cover post-publication liveness access",
    )

    destroy_process_body = function_body(source, "destroy_unpublished_remote_process")
    require_order(
        destroy_process_body,
        "destroy_owned_unpublished_process(owner, task)",
        "release_output_capture(output)",
        "remote output may be released only after child fd teardown is complete",
    )
    require_tokens(
        destroy_process_body,
        [
            'dbg::log("[WKI] Retiring unpublished remote process',
            'panic_handler("WKI remote compute: lost unpublished-process teardown ownership")',
        ],
        "receiver child teardown ownership enforcement",
    )
    for reason in [
        "wki-submit-parse",
        "wki-submit-stack",
        "wki-submit-expired",
        "wki-submit-cancel",
        "wki-submit-session",
        "wki-submit-ref",
    ]:
        require_tokens(
            work_body,
            ["destroy_unpublished_remote_process(current_task, new_task,"],
            f"{reason} rejection uses complete unpublished-process cleanup",
        )
        if reason not in work_body:
            fail(f"{reason} rejection must use complete unpublished-process cleanup")

    refused_post = function_body(source, "discard_unposted_remote_process")
    require_tokens(refused_post, ['destroy_unpublished_remote_process(owner, child, output, "wki-submit-post"'],
                   "post refusal uses complete owner-linked cleanup")

    cleanup_body = function_body(source, "wki_remote_compute_cleanup_for_peer")
    require_tokens(
        cleanup_body,
        [
            "fail_submitted_tasks_for_peer(node_id, RemoteComputeCleanupScope::ALL)",
            "terminate_running_remote_tasks_for_peer(node_id, RemoteComputeCleanupScope::ALL)",
            "entry.loading = false",
            "entry.load_status = -1",
        ],
        "peer cleanup retains live output capture and cancels cache loads",
    )
    retired_cleanup_body = function_body(source, "wki_remote_compute_cleanup_retired_for_peer")
    require_tokens(
        retired_cleanup_body,
        [
            "fail_submitted_tasks_for_peer(node_id, RemoteComputeCleanupScope::RETIRED)",
            "terminate_running_remote_tasks_for_peer(node_id, RemoteComputeCleanupScope::RETIRED)",
            "compute_submit_session_is_current_locked(SESSION)",
            "compact_pending_task_completions_locked()",
        ],
        "ordinary epoch reset cleans only retired compute generations",
    )
    terminate_body = function_body(source, "terminate_running_remote_tasks_for_peer")
    require_tokens(
        terminate_body,
        [
            "running.discard_completion = true",
            "running.termination_requested = true",
            "queue_signal_for_process_tasks(OWNER_PID, WKI_SIGKILL_NUM)",
        ],
        "peer cleanup terminates receiver-side processes outside the compute lock",
    )
    completion_body = function_body(source, "wki_remote_compute_check_completions")
    require_tokens(
        completion_body,
        [
            "if (rt.discard_completion)",
            "retry_pending_task_accepts()",
            "running_remote_task_completion_eligible(rt)",
            "wki_send_on_channel_generation(",
            "info.submitter_node, info.submit_rx_channel, info.submit_rx_channel_generation, MsgType::TASK_COMPLETE",
            "append_pending_completion_locked(pending.release())",
            "release_output_capture(rt.output)",
            "release_running_root_locked(rt)",
        ],
        "retired peer output capture release after task exit",
    )

    cancel_body = function_body(source, "handle_task_cancel")
    require_tokens(
        cancel_body,
        [
            "capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation, IDENTITY)",
            "find_running_task(cancel->task_id, SESSION)",
            "record_cancelled_task_submit_locked(SESSION, cancel->task_id, SIGNUM)",
            "rt->accept_pending = false",
            "rt->discard_completion = true",
        ],
        "session-scoped queued/in-flight TASK_CANCEL",
    )

    for handler in ["handle_task_accept", "handle_task_reject", "handle_task_complete"]:
        body = function_body(source, handler)
        require_tokens(
            body,
            [
                "task->target_node != hdr->src_node",
                "task->submit_channel != rx_channel",
                "task->submit_channel_generation != rx_channel_generation",
            ],
            f"{handler} exact submitter channel identity",
        )

    submit_inline = source[
        source.find("auto wki_task_submit_inline") : source.find("// Submitter Side - VFS_REF Submit")
    ]
    require_tokens(
        submit_inline,
        [
            "uint64_t const TOTAL =",
            "if (TOTAL > UINT16_MAX)",
            "capture_exec_submit_channel(target_node, SUBMIT_CHANNEL)",
            "send_task_submit_payload(target_node, SUBMIT_CHANNEL, TASK_ID, BUF, MSG_LEN)",
        ],
        "INLINE sender overflow and channel-generation guards",
    )
    submit_vfs = source[
        source.find("auto wki_task_submit_vfs_ref") : source.find("// Submitter Side - Wait for Completion")
    ]
    require_tokens(
        submit_vfs,
        [
            "size_t const PATH_LEN = std::strlen(vfs_path)",
            "PATH_LEN >= 512",
            "PATH_LEN > UINT16_MAX",
            "capture_exec_submit_channel(target_node, SUBMIT_CHANNEL)",
        ],
        "VFS_REF sender path and channel-generation guards",
    )

    require_tokens(
        wki,
        [
            "detail::handle_task_submit(hdr, payload, payload_len, rx_channel, rx_channel_generation)",
            "detail::handle_task_accept(hdr, payload, payload_len, rx_channel, rx_channel_generation)",
            "detail::handle_task_reject(hdr, payload, payload_len, rx_channel, rx_channel_generation)",
            "detail::handle_task_complete(hdr, payload, payload_len, rx_channel, rx_channel_generation)",
            "detail::handle_task_cancel(hdr, payload, payload_len, rx_channel, rx_channel_generation)",
        ],
        "compute dispatch channel-generation propagation",
    )
    require_tokens(
        peer,
        [
            "class PeerLifecycleGuard",
            "lifecycle_state.compare_exchange_strong",
            "PeerLifecycleGuard lifecycle",
            "lifecycle.try_acquire(peer)",
            "lifecycle.acquire(peer)",
            "wki_remote_compute_retire_submit_session(peer_node)",
            "compute_reset_cleanup_pending.store(true, std::memory_order_release)",
            "wki_remote_compute_cleanup_retired_for_peer(peer.node_id)",
            "wki_remote_compute_cleanup_for_peer(fenced_id)",
            "wki_peer_lifecycle_release(claimed_peer)",
        ],
        "disconnect/reconnect cleanup admission gate",
    )
    require_tokens(
        wki,
        [
            "class PeerLifecycleTryLease",
            "message_uses_rx_peer_lifecycle(msg, payload, PAYLOAD_LEN)",
            "peer_lifecycle.try_acquire(hdr->src_node, SHARED_RX_LIFECYCLE)",
            "message_uses_rx_peer_lifecycle(RO_MSG, ch->reorder_head->data, ch->reorder_head->len)",
            "!peer_lifecycle.owns(hdr->src_node, RO_SHARED_RX_LIFECYCLE)",
        ],
        "TASK message admission serialized with peer cleanup",
    )
    require_tokens(
        function_body(wki, "message_allows_shared_rx_peer_lifecycle"),
        ["channel_id == WKI_CHAN_IPC_DATA", "dev_op_uses_ipc_lifecycle(type, payload, payload_len)"],
        "shared lifecycle admission is restricted to ordered IPC data",
    )
    lifecycle_body = function_body(wki, "message_uses_compute_lifecycle")
    require_tokens(
        lifecycle_body,
        ["MsgType::TASK_SUBMIT", "MsgType::TASK_ACCEPT", "MsgType::TASK_REJECT"],
        "inline TASK handlers use peer lifecycle gate",
    )
    if any(message in lifecycle_body for message in ["MsgType::TASK_COMPLETE", "MsgType::TASK_CANCEL", "MsgType::TASK_SERVICE_REPORT"]):
        fail("deferred COMPLETE/CANCEL admission must not stall behind a task-context lifecycle holder")
    require_order(
        function_body(wki, "wki_rx"),
        "message_uses_rx_peer_lifecycle(RO_MSG, ch->reorder_head->data, ch->reorder_head->len)",
        "WkiReorderEntry* ro = ch->reorder_head",
        "reordered TASK admission must precede consuming the buffered frame",
    )
    rx_body = function_body(wki, "wki_rx")
    require_tokens(
        rx_body,
        [
            "ch->channel_id == WKI_CHAN_RESOURCE && message_uses_deferred_compute_rx(msg)",
            "wki_remote_compute_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)",
            "admission == WkiComputeRxAdmission::RETRY",
            "admission == WkiComputeRxAdmission::DEFERRED",
            "wki_remote_compute_notify_deferred_rx(hdr->src_node)",
            "ch->channel_id == WKI_CHAN_RESOURCE && message_uses_deferred_compute_rx(RO_MSG)",
            "ro_admission == WkiComputeRxAdmission::RETRY",
            "ro_admission == WkiComputeRxAdmission::DEFERRED",
            "wki_remote_compute_notify_deferred_rx(RO_HDR.src_node)",
        ],
        "reliable compute RX admission results",
    )
    direct_compute_admission = rx_body[rx_body.index("wki_remote_compute_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)") :]
    require_order(
        direct_compute_admission,
        "wki_remote_compute_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)",
        "ch->rx_seq++",
        "direct compute admission before reliable consumption",
    )
    require_order(
        rx_body,
        "ro_admission = wki_remote_compute_admit_rx(",
        "WkiReorderEntry* ro = ch->reorder_head",
        "reordered compute admission before unlink",
    )
    require_tokens(
        wki_header,
        [
            "constexpr uint32_t WKI_ACK_NONE = UINT32_MAX",
            "uint32_t rx_ack_pending = WKI_ACK_NONE",
        ],
        "reliable receive no-ACK sentinel state",
    )
    require_tokens(
        channel,
        ["ch->rx_seq = 0", "ch->rx_ack_pending = WKI_ACK_NONE"],
        "channel reset clears receive sequence and cumulative ACK state",
    )
    require_tokens(wki, ["ch->rx_ack_pending = WKI_ACK_NONE"], "fresh channels start without a cumulative ACK")
    if "rx_seq = hdr->seq_num" in rx_body or "Resyncing channel seq" in rx_body:
        fail("a first future packet must not skip an undispatched reliable sequence")
    require_order(rx_body, "if (hdr->seq_num == ch->rx_seq)", "else if (seq_after(hdr->seq_num, ch->rx_seq))", "RX sequence classification")

    complete_body = function_body(source, "handle_task_complete")
    blocked_body = function_body(source, "wki_proxy_task_blocked")
    require_tokens(
        complete_body,
        [
            "task->pending_proxy_output = pending_output",
            "task->pending_proxy_output_len = OUTPUT_LEN",
            "task->active = false",
        ],
        "early completion transfers output ownership before publication",
    )
    require_order(
        complete_body,
        "task->pending_proxy_output = pending_output",
        "task->active = false",
        "early output ownership before inactive publication",
    )
    require_tokens(
        blocked_body,
        [
            "completed_output = submitted->pending_proxy_output",
            "submitted->pending_proxy_output = nullptr",
            "finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len",
            "delete[] completed_output",
        ],
        "proxy-block handoff consumes owned early output",
    )
    spawn_body = function_body(source, "wki_try_remote_spawn")
    require_tokens(
        spawn_body,
        [
            "if (proxy_ready && !st->active)",
            "completed_output = st->pending_proxy_output",
            "st->pending_proxy_output = nullptr",
            "finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, tid,",
            "delete[] completed_output",
        ],
        "spawn-side immediate completion consumes owned early output",
    )
    require_order(
        spawn_body,
        "st->proxy_ready = proxy_ready",
        "if (proxy_ready && !st->active)",
        "spawn-side completion may finalize only an already parked proxy",
    )

    token = "wki_remote_compute_selftest_submit_worker_count_is_bounded"
    require_tokens(source, [f"auto {token}() -> bool"], "submit-worker count selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "submit-worker count selftest declaration")
    require_tokens(ktest, ["SubmitWorkerCountIsBounded", token], "submit-worker count KTEST coverage")

    rx_token = "wki_remote_compute_selftest_rx_admission_is_bounded"
    require_tokens(source, [f"auto {rx_token}() -> bool"], "compute RX admission selftest implementation")
    require_tokens(header, [f"auto {rx_token}() -> bool;"], "compute RX admission selftest declaration")
    require_tokens(ktest, ["RxAdmissionIsBounded", rx_token], "compute RX admission KTEST coverage")

    fairness_body = function_body(source, "collect_pending_task_accept_attempts_locked")
    require_tokens(
        fairness_body,
        [
            "s_pending_task_accept_cursor % ROW_COUNT",
            "visited < ROW_COUNT && attempt_count < attempts.size()",
            "s_pending_task_accept_cursor = index",
        ],
        "pending TASK_ACCEPT retry round robin",
    )
    require_tokens(
        function_body(source, "retry_pending_task_accepts"),
        ["collect_pending_task_accept_attempts_locked(attempts)"],
        "pending TASK_ACCEPT retry uses round robin",
    )
    fairness_token = "wki_remote_compute_selftest_accept_retry_is_fair"
    require_tokens(source, [f"auto {fairness_token}() -> bool"], "accept retry fairness selftest implementation")
    require_tokens(header, [f"auto {fairness_token}() -> bool;"], "accept retry fairness selftest declaration")
    require_tokens(ktest, ["AcceptRetryIsFair", fairness_token], "accept retry fairness KTEST coverage")

    cancel_token = "wki_remote_compute_selftest_submit_cancel_is_session_scoped"
    require_tokens(source, [f"auto {cancel_token}() -> bool"], "session-scoped cancel selftest implementation")
    require_tokens(header, [f"auto {cancel_token}() -> bool;"], "session-scoped cancel selftest declaration")
    require_tokens(ktest, ["SubmitCancelIsSessionScoped", cancel_token], "session-scoped cancel KTEST coverage")


def test_remote_proxy_signals_never_make_the_local_frame_runnable() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    scheduler = SCHEDULER_CPP.read_text()

    forward_body = function_body(source, "wki_proxy_task_terminal_signal")
    require_tokens(forward_body, ["ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id",
        "if (TASK_ID == 0) return std::nullopt", "submitted->local_task != &target", "return -ENOTSUP",
        "WKI_TERMINAL_SIGNAL_RESOURCE_ID", "MsgType::TASK_TERMINAL_SIGNAL", "concrete_signal_origin(expected.origin)",
        "concrete_signal_origin(expected.group_origin)", "wki_send_on_channel_generation"], "terminal event route ownership")
    if "wki_task_cancel" in forward_body:
        fail("terminal events must not use lifecycle cancellation")
    for name in ["signal_terminal_process_group", "revoke_terminal_attachment"]:
        body = function_body(scheduler, name)
        require_tokens(body, ["signal_terminal_source_as"], "source terminal delivery participates in cutover admission")
        assert "signal_generate_pending_mask" not in body
    producer = function_body((ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text(), "terminal_source_signal")
    require_tokens(producer, ["PendingSignalTarget admission", "admission.admit_local(target)",
        "if (ADMISSION.remote)", "wki_proxy_task_terminal_signal", "value_or(-ESRCH)",
        "if (!ADMISSION.local_lease)", "same_session(expected, ADMISSION.job)", "same_group(expected, ADMISSION.job)",
        "expected.terminal_generation != ADMISSION.job.terminal_generation", "generate_group_signal"], "source terminal admission")
    require_order(producer, "admission.admit_local(target)", "if (ADMISSION.remote)", "admit before routing")
    require_order(producer, "if (ADMISSION.remote)", "generate_group_signal", "remote refusal cannot fall through")
    assert "SharedVmemPublicationGuard" not in producer
    receiver = function_body(source, "deliver_terminal_signal")
    require_tokens(receiver, ["capture_compute_submit_session_locked", "find_running_task", "root_completion_queued",
        "running->task->try_acquire()", "signal_terminal_as", "resolve_session_origin"], "exact accepted terminal target")
    if "find_task_by_pid" in receiver:
        fail("terminal events must not substitute a receiver PID lookup")
    local = function_body((ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text(), "signal_terminal_as")
    require_tokens(local, ["SharedVmemPublicationGuard", "admission.admit_local(target)",
        "same_session(expected, ADMISSION.job)", "same_group(expected, ADMISSION.job)",
        "generate_group_signal", "PendingSignalTarget retained(target)"], "receiver terminal membership and cutover admission")
    require_order(local, "same_session(expected, ADMISSION.job)", "generate(target)", "membership before terminal effects")

    reschedule_body = function_body(scheduler, "reschedule_task_for_cpu_once")
    require_order(
        reschedule_body,
        "if (task_is_wki_migration_owned(task))",
        "// Remove from whatever queue the task is in.",
        "proxy park guard must precede every runqueue removal/publication",
    )
    require_tokens(
        function_body(scheduler, "task_is_wki_migration_owned"),
        ["task->wki_proxy_task_id != 0", "task->wki_proxy_task", "WaitChannelKind::WKI_EXECVE_PROXY"],
        "central scheduler migration policy must recognize every WKI proxy representation",
    )
    require_tokens(
        function_body(scheduler, "event_wake_can_rebalance_process"),
        ["task->wki_proxy_task_id == 0", "WaitChannelKind::WKI_EXECVE_PROXY"],
        "event wakes must exclude both fresh and execve proxy representations",
    )


def test_submitted_proxy_rows_hold_task_lifetime_until_finalization() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()

    require_tokens(
        header,
        [
            "A non-null slot owns exactly one Task lifetime reference",
            "~SubmittedTask()",
            "reset_local_task_ref()",
            "o.local_task = nullptr",
            "task->try_acquire_lifetime_ref()",
            "auto take_local_task_ref()",
            "task->release()",
        ],
        "SubmittedTask local proxy lifetime ownership",
    )
    if re.search(r"(?:\.|->)local_task\s*=(?!=)", source):
        fail("remote-compute code must mutate SubmittedTask::local_task only through its ownership helpers")

    require_order(
        function_body(source, "consume_submitted_task_result_locked"),
        "task->reset_local_task_ref()",
        "request_submitted_task_reclaim_locked(task)",
        "ordinary result cleanup must drop the proxy task ref before recycling",
    )

    complete_body = function_body(source, "handle_task_complete")
    require_order(
        complete_body,
        "task->take_local_task_ref()",
        "finalize_proxy_task(proxy",
        "TASK_COMPLETE must transfer the row ref before out-of-lock finalization",
    )
    require_order(
        complete_body,
        "finalize_proxy_task(proxy",
        "proxy->release()",
        "TASK_COMPLETE must retain the proxy through finalization and logging",
    )

    peer_cleanup = function_body(source, "fail_submitted_tasks_for_peer")
    require_order(
        peer_cleanup,
        "t.take_local_task_ref()",
        "finalize_proxy_task(proxy",
        "peer cleanup must transfer each proxy ref into its finalizer batch",
    )
    require_order(
        peer_cleanup,
        'ker::mod::dbg::log("[WKI] Proxy task cleanup',
        "proxy->release()",
        "peer cleanup must keep each proxy alive through its last diagnostic access",
    )

    blocked = function_body(source, "wki_proxy_task_blocked")
    require_order(
        blocked,
        "submitted->take_local_task_ref()",
        "finalize_proxy_task(completed_proxy_ref",
        "early completion must transfer the row ref to the blocking task finalizer",
    )
    require_order(
        blocked,
        "finalize_proxy_task(completed_proxy_ref",
        "completed_proxy_ref->release()",
        "early completion must release only after finalization and reclamation request",
    )

    exit_cleanup = function_body(source, "wki_remote_compute_cleanup_for_task")
    require_tokens(
        exit_cleanup,
        [
            "submitted.result_owner_task != exiting_task && submitted.local_task != exiting_task",
            "consume_submitted_task_result_locked(&submitted)",
        ],
        "task exit must retire both direct handles and published proxy rows",
    )

    require_tokens(
        function_body(source, "wki_remote_compute_selftest_submitted_slots_reclaim_safely"),
        [
            "proxy.ref_count.load(std::memory_order_acquire) == 2",
            "proxy.ref_count.load(std::memory_order_acquire) == 1",
            "proxy_lifetime_ref_held",
            "proxy_lifetime_ref_released",
        ],
        "submitted-slot KTEST must observe the exact row-owned proxy reference",
    )


def test_remote_task_exit_wakes_exact_completion_monitor() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    exit_source = PROCESS_EXIT_CPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    api = "wki_remote_compute_notify_task_exit_ready"
    require_tokens(header, [f"void {api}(ker::mod::sched::task::Task* task);"], "exit-ready notification API")

    exit_body = function_body(exit_source, "wos_proc_exit_with_wait_status")
    require_order(
        exit_body,
        "current_task->exit_notify_ready.store(true, std::memory_order_release)",
        f"ker::net::wki::{api}(current_task)",
        "remote completion wake must observe the released exit publication",
    )
    if "wki_remote_compute_check_completions" in exit_body:
        fail("process exit must only notify the WKI timer, never send TASK_COMPLETE inline")

    notify_body = function_body(source, api)
    require_tokens(
        notify_body,
        [
            "task->wki_remote_pid == 0",
            "task->exit_notify_ready.load(std::memory_order_acquire)",
            "running_remote_task_matches_exit_ready(running, task)",
            "s_compute_lock.lock()",
            "s_compute_lock.unlock()",
            "wki_timer_notify()",
        ],
        "bounded exact exit-ready notification",
    )
    require_order(notify_body, "s_compute_lock.unlock()", "wki_timer_notify()", "timer wake must not nest the compute lock")
    for forbidden in ["new ", "wki_send", "wki_remote_compute_check_completions()"]:
        if forbidden in notify_body:
            fail(f"exit-ready notification must not allocate, send, or scan completions inline: {forbidden}")

    submit_body = function_body(source, "handle_task_submit_work")
    notify_pos = submit_body.find(f"{api}(new_task)")
    final_worker_release_pos = submit_body.rfind("worker_task_ref->release()")
    if notify_pos < 0 or final_worker_release_pos <= notify_pos:
        fail("post-accept exit recheck must retain the receiver child")
    require_tokens(
        source,
        [
            "running_remote_task_completion_eligible(rt)",
            "wki_remote_compute_selftest_exit_ready_completion_wake_is_exact",
        ],
        "completion wake race coverage",
    )
    require_tokens(
        ktest,
        [
            "ExitReadyCompletionWakeIsExact",
            "wki_remote_compute_selftest_exit_ready_completion_wake_is_exact",
        ],
        "exit-ready completion KTEST registration",
    )


def test_receiver_tracking_failure_unwinds_before_scheduler_publication() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    require_tokens(source, ["using RunningRemoteTasks = ker::util::SmallVec<RunningRemoteTask, 16>"], "checked receiver registry")
    compact = function_body(source, "compact_running_remote_tasks_locked")
    require_tokens(compact, ["size_t retained = 0", "g_running_remote_tasks[retained] = g_running_remote_tasks[i]", "pop_back()"],
                   "stable linear compaction")
    if "remove_at" in compact:
        fail("receiver compaction cannot shift the full tail once per retired row")
    publish = function_body(source, "publish_running_task_locked")
    require_tokens(publish, ["return !fail_allocation && g_running_remote_tasks.push_back(task)"], "propagate checked growth failure")
    start = source.index("if (!publish_running_task_locked(rt))")
    end = source.index("post_task_balanced(new_task)", start)
    failure = source[start:end]
    require_order(failure, "s_compute_lock.unlock()", "discard_unpublished_running_task(", "teardown outside compute lock")
    require_tokens(failure, ["TaskRejectReason::NO_MEM", "send_submit_response(MsgType::TASK_REJECT, reject)", "return;"],
                   "reject allocation failure before scheduler publication")
    discard = function_body(source, "discard_unpublished_running_task")
    require_order(discard, "worker_ref->release()", "destroy_unpublished_remote_process(", "drop worker ref before exclusive teardown")
    require_order(discard, "child->release()", "destroy_unpublished_remote_process(", "drop unpublished monitor ref before exclusive teardown")


def test_receiver_rejection_releases_peer_lease_before_vfs_teardown() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "handle_task_submit_work")
    branches = [
        ("if (!FINAL_SESSION_CURRENT || FINAL_CANCEL_SIGNAL != 0 || FINAL_DEADLINE_EXPIRED)",
         "destroy_unpublished_remote_process("),
        ("if (!publish_running_task_locked(rt))", "discard_unpublished_running_task("),
    ]
    for marker, teardown in branches:
        start = body.index(marker)
        branch = body[start:body.index("return;", start)]
        require_order(branch, "s_compute_lock.unlock()", "publication_lease.release()", marker)
        require_order(branch, "publication_lease.release()", teardown, marker)
    post_failure = function_body(source, "discard_unposted_remote_process")
    require_order(post_failure, "running.active = false", "s_compute_lock.unlock()",
                  "retire private monitor under compute lock")
    require_order(post_failure, "s_compute_lock.unlock()", "publication_lease.release()",
                  "drop compute lock before peer lease")
    require_order(post_failure, "publication_lease.release()", "delete completion",
                  "drop peer lease before output receipt teardown")
    require_order(post_failure, "completion_ref->release()", "destroy_unpublished_remote_process(",
                  "release extra references before owner-linked teardown")
    require_tokens(body, ["discard_unposted_remote_process(current_task, new_task, worker_task_ref, completion_task_ref"],
                   "production refusal uses the qualified cleanup helper")
    final_gate = function_body(source, "post_admitted_remote_process")
    require_tokens(final_gate, ["lease.owns(session.node_id)", "compute_submit_admission_is_current_locked(session)",
                               "take_cancelled_task_submit_locked(session, task_id)", "wki_now_us() >= deadline_us",
                               "return CURRENT && CANCEL == 0 && !EXPIRED && post()"],
                   "final admission guards actual scheduler posting")
    require_order(final_gate, "s_compute_lock.unlock()", "&& post()", "scheduler posting outside compute lock")
    require_tokens(body, ["post_admitted_remote_process(session, submit->task_id, deadline_us, publication_lease, handoff_ready"],
                   "both ordinary spawn and exec use final admission")
    # The normal path must retain the publication guard. Every earlier release
    # belongs to a returning failure branch or the explicit READY/COMMIT wait.
    before_post = body[:body.index("bool const POSTED =")]
    assert before_post.count("publication_lease.release()") == 3
    commit = before_post[before_post.index("ComputePublishStage::COMMIT"):]
    require_order(commit, "ComputePublishStage::COMMIT", "publication_lease.acquire(src_node)",
                  "reacquire peer gate after the remote cutover wait")


def test_submit_wire_buffers_are_cancelled_with_executor() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    begin = source.index("class TaskSubmitBuffer {")
    end = source.index("// Storage", begin)
    buffer = source[begin:end]
    require_order(buffer, "executor->pending_wki_submit_buffers = this", "new (std::nothrow) uint8_t[size]", "track allocation before it can be abandoned")
    require_order(buffer, "bytes = nullptr", "delete[] owned", "clear before deallocation")
    require_tokens(buffer, ["if (size == 0) return", "if (bytes == nullptr) cancel()", "previous->next = next", "next->previous = previous"],
                   "zero, failed and nested buffer cleanup")
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        require_order(body, "TaskSubmitBuffer large_buf(local_task", "send_task_submit_payload(", "own frame through send/wait")
        require_tokens(body, ["MSG_LEN > small_buf.size() ? MSG_LEN : 0", "large_buf.data() == nullptr"], "small frame stays on stack and heap failure reports")
        if "std::unique_ptr<uint8_t[]> large_buf" in body:
            fail("fatal stack abandonment must not rely on a local unique_ptr")
    cleanup = function_body(source, "wki_remote_compute_cleanup_for_task")
    require_order(cleanup, "pending_wki_submit_buffers->cancel()", "s_compute_lock.lock()", "free abandoned frames outside compute lock")


def test_publication_reports_checked_allocation_failure() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    publish = function_body(source, "publish_submitted_task_locked")
    require_order(publish, "new (std::nothrow) SubmittedTaskSlot{}", "slot->task = std::move(task)", "allocate before consuming unpublished input")
    require_order(publish, "*error_out = -ENOMEM", "slot->task = std::move(task)", "failed allocation preserves input")
    require_tokens(publish, ["*error_out = -EINVAL", "s_submitted_task_tail->storage_next = slot", "++s_submitted_task_storage_count"],
                   "stable checked slot growth")
    if "std::deque<SubmittedTaskSlot>" in source or "g_submitted_tasks.emplace_back()" in source:
        fail("submission slot storage must expose allocation failure")
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        begin = body.index("if (published == nullptr)")
        end = body.index("remember_submitted_ipc_handoff(*published", begin)
        failure = body[begin:end]
        require_order(failure, "st.reset_local_task_ref()", "cleanup_submit_ipc()", "release unpublished Task ref before blocking cleanup")
        require_tokens(failure, ["fail_submit(publication_error)"], "report publication errno")


def test_accepted_remote_spawn_commits_ipc_fd_ownership() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "wki_try_remote_spawn")
    require_tokens(
        body,
        [
            "std::array<WkiIpcTaskFdHandoff, 16> ipc_fd_handoff",
            "wki_ipc_export_task_fds(task, best_node, IPC_CHANNELS, ipc_fd_map.data(), &ipc_fd_count, ipc_fd_handoff.data(), &stdio_mask)",
            "ipc_guard.cancel()",
            "ipc_fd_count, ipc_fd_handoff.data()",
        ],
        "remote IPC fd ownership handoff",
    )
    fallback = body.find("if (vfs_ref_submit_attempted)")
    reexport = body.find("wki_ipc_export_task_fds", fallback)
    release = body.find("ipc_guard.cancel()", fallback)
    if fallback < 0 or release < fallback or reexport < release:
        fail("VFS_REF fallback must release old identity pins before re-export")
    failed_start = body.find("if (tid == 0) {")
    failed_end = body.find("balanced_assignment.commit()", failed_start)
    if failed_start < 0 or failed_end < 0:
        fail("remote submission failure block is missing")
    failed = body[failed_start:failed_end]
    require_tokens(failed, ["ipc_guard.cancel()"], "failed remote submission cleanup")
    if "wki_ipc_commit_task_fd_handoff" in body:
        fail("remote spawn must not keep accepted handoff pins on its interruptible caller stack")

    submit_sections = {
        "inline": source[source.index("auto wki_task_submit_inline(") : source.index("auto wki_task_submit_vfs_ref(")],
        "vfs_ref": source[source.index("auto wki_task_submit_vfs_ref(") : source.index("auto wki_task_wait(")],
    }
    for mode, submit in submit_sections.items():
        require_tokens(
            submit,
            [
                "remember_submitted_ipc_handoff(*published, ipc_fd_handoff, ipc_fd_count)",
                "publish_submitted_task_locked(std::move(st), &publication_error)",
                "wait_for_exec_submit(wait",
                "take_submitted_ipc_handoff_locked(task_ptr)",
                "wki_ipc_commit_task_fd_handoff(local_task, handoff_cleanup.handoff.data(), handoff_cleanup.count)",
                "restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard)",
            ],
            f"durable {mode} IPC fd handoff",
        )
        require_order(
            submit,
            "remember_submitted_ipc_handoff(*published, ipc_fd_handoff, ipc_fd_count)",
            "wait_for_exec_submit(wait",
            f"{mode} identity pins must move to the submitted row before the interruptible wait",
        )

    cleanup = function_body(source, "wki_remote_compute_cleanup_for_task")
    require_order(
        cleanup,
        "take_submitted_ipc_handoff_locked(&submitted)",
        "release_submitted_ipc_handoff(handoff_cleanup)",
        "fatal task cleanup must release pins taken from durable storage",
    )
    selftest = function_body(source, "wki_remote_compute_selftest_task_exit_releases_handoff_pins")
    require_tokens(
        selftest,
        [
            "remember_submitted_ipc_handoff(submitted, handoff.data()",
            "wki_remote_compute_cleanup_for_task(&exiting_task)",
            "pinned_file.refcount.load(std::memory_order_acquire) == 1",
            "ROW_RETIRED",
        ],
        "interrupted submit handoff-pin KTEST",
    )



def test_service_report_transport_retains_only_reporting_state() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    spawn = function_body(source, "wki_try_remote_spawn")
    require_order(spawn, "service_remote_admission(*task)", "wki_ipc_export_task_fds", "finite service policy before FD side effects")
    require_order(spawn, "wki_peer_capability_negotiated(best_node, WKI_CAP_RESOURCE_LIMITS)", "wki_ipc_export_task_fds", "resource negotiation before exports")
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        require_order(body, "return fail_submit(-ESRCH)", "snapshot_task_resources", "direct API requires policy owner")
        require_order(body, "return fail_submit(-ENOTSUP)", "snapshot_task_resources", "direct API requires negotiated policy")
    accept = function_body(source, "handle_task_accept")
    if "cancel_service_report" in accept:
        fail("TASK_ACCEPT must retain the report destination")
    require_tokens(function_body(source, "handle_task_reject"), ["cancel_service_report(false)"], "confirmed rejection ends reporting without false incompleteness")
    require_tokens(function_body(source, "submitted_task_can_reclaim_locked"), ["!task.service_report_pending"], "report owns row independently of result")
    report = function_body(source, "handle_task_service_report")
    require_order(report, "service_contract::decode", "s_compute_lock.lock()", "wire framing before locking")
    require_tokens(report, ["find_submitted_task_any", "task->target_node == hdr->src_node", "task->submit_channel == rx_channel", "task->submit_channel_generation == rx_channel_generation", "task->service_report.apply", "ServiceReportResult::APPLIED", "reclaim_submitted_task_if_safe_locked"], "session-scoped final report")
    cancel = function_body(source, "handle_task_cancel")
    require_order(cancel, "if (rt->root_completion_queued)", "find_task_by_pid_safe", "no PID reuse after root completion")
    sender = function_body(source, "send_service_reports")
    require_tokens(sender, ["rt.service_report_inflight", "rt.service_report_sequence", "OBSERVATION.pending_remote_children == 0", "MsgType::TASK_SERVICE_REPORT", "RESULT == WKI_OK", "find_running_task(report.task_id, session)"], "bounded report retry with session revalidation")
    require_tokens(function_body(source, "wki_remote_compute_selftest_service_report_transport"), ["handle_task_accept", "handle_task_reject", "handle_task_service_report", "fail_submitted_tasks_for_peer", "release_running_root_locked"], "real handler and lifetime regression coverage")

def test_complete_credentials_bind_format_to_admitted_session() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    capture = multiline_function_body(source, "capture_compute_submit_session_locked")
    require_tokens(capture, ["return peer_session->current", ".identity = identity"], "immutable admitted capability selection")
    for name in ["handle_task_submit", "handle_task_cancel"]:
        body = function_body(source, name)
        require_order(body, "capture_compute_peer_identity(hdr->src_node)", "s_compute_lock.lock()", "capabilities copied under RX lease before compute lock")
    receiver = function_body(source, "handle_task_submit_work")
    require_order(receiver, "session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS", "prepare_submitted_resources", "admitted format before parsing")
    require_order(receiver, "prepare_submitted_identity", "load_elf_from_vfs_path", "complete identity mapped before source read")
    if "wki_peer_capability_negotiated" in receiver:
        fail("deferred receiver must not select credential format from a reusable peer slot")
    sender = multiline_function_body(source, "copy_submit_identity_data")
    require_tokens(sender, ["fill_task_identity_context(task, &identity, credentials, JOB, root)", "credential_record::encode(credentials)", "dest + WKI_TASK_RESOURCE_IDENTITY_SIZE"], "same snapshot and stable resource offset")
    spawn = function_body(source, "wki_try_remote_spawn")
    require_order(spawn, "WKI_CAP_COMPUTE_CREDENTIALS", "wki_ipc_export_task_fds", "legacy representability before exports")


def test_capture_file_and_monitor_ownership_are_independent() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    append = multiline_function_body(source, "TaskOutputCapture::append")
    require_order(append, "lock.lock_irqsave()", "if (!frozen)", "append/freeze serialization")
    require_order(append, "std::min(bytes.size(), data.size() - len)", "std::memcpy", "bounded capture append")
    require_order(append, "len =", "lock.unlock_irqrestore", "publish capture bytes and length together")
    freeze = multiline_function_body(source, "TaskOutputCapture::freeze")
    require_order(freeze, "lock.lock_irqsave()", "frozen = true", "freeze under capture lock")
    require_order(freeze, "frozen = true", "lock.unlock_irqrestore", "seal snapshot before exposing its view")
    install = multiline_function_body(source, "install_remote_stdio")
    require_order(install, "task.fd_table.insert", "capture->retain()", "no File ownership before all inserts succeed")
    require_order(install, "capture->retain()", "files[fd].release()", "File pins capture before publication")
    close = function_body(source, "capture_close")
    require_order(close, "file->private_data = nullptr", "release_output_capture(capture)", "detach before last capture release")
    collector = function_body(source, "wki_remote_compute_check_completions")
    require_order(collector, "if (!completed)", "rt.output->freeze()", "freeze only after root exit/accounting readiness")
    require_order(collector, "rt.output->freeze()", "queue_pending_completion_locked", "immutable completion payload")
    for forbidden in ("delete output;", "delete rt.output;", "delete info.output;", "delete completion.output;", "output->len", "output->data"):
        if forbidden in source:
            fail("capture ownership or mutable storage bypass: " + forbidden)


def test_user_signal_owns_reply_and_target_lifetimes() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    sender = multiline_function_body(source, "wki_proxy_task_signal_as")
    require_order(sender, "signal_version != WKI_TASK_SIGNAL_VERSION", "s_next_user_signal", "support before request publication")
    require_tokens(sender, ["credential_record::encode(caller)", ".consumer_wait = &wait", "MsgType::TASK_SIGNAL_REQ", "wki_wait_for_op", "pending->result"], "bounded owned signal request")
    if "wki_task_cancel(" in sender:
        fail("user signal must not fall back to lifecycle cancel")
    receiver = function_body(source, "authorize_user_signal")
    require_order(receiver, "credential_record::decode", "auth_policy::map_compute_identity", "decode before mapped authorization")
    require_tokens(receiver, ["find_running_task(request.task_id, SESSION)", "!running->root_completion_queued", "running->task->try_acquire()", "resolve_session_origin", "signal_as(*target, caller, job, request.signum)"], "exact receiver target and mapped caller")
    if "find_task_by_pid" in receiver:
        fail("user signal must not reuse a numeric target PID")
    worker = function_body(source, "compute_rx_worker_loop")
    signal_branch = worker[worker.index("else if (WORK_TYPE == MsgType::TASK_SIGNAL_REQ ||"):worker.index("else if (WORK_TYPE == MsgType::GROUP_RECEIVER_REQ ||")]
    require_order(signal_branch, "work.type = static_cast<uint8_t>(MsgType::TASK_SIGNAL_RESP)", "wki_send_on_channel_generation(work.hdr.src_node", "persist signal decision before its send attempt")
    require_tokens(worker, ["requeue_compute_rx_slot", "if (WORK_TYPE == MsgType::TASK_SIGNAL_REQ)"], "retry reply without repeating decision")
    cleanup = function_body(source, "retire_user_signals_for_task")
    require_order(cleanup, "slot.response_wait = nullptr", "wki_quiesce_claimed_op", "unpublish response before quiescing")
    require_order(cleanup, "wki_quiesce_claimed_op", "*found = {}", "consumer slot lives until final claimant access")
    cleanup = function_body(source, "wki_remote_compute_cleanup_for_task")
    require_order(cleanup, "retire_user_signals_for_task", "cleanup_signal_targets", "retire operation before releasing target ownership")


def test_group_signal_selects_receiver_members_and_owns_replies() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    sender = multiline_function_body(source, "group_route_rpc")
    require_order(sender, "wki_resource_generation_snapshot", "s_next_user_signal", "support before group request publication")
    require_tokens(sender, [".consumer_wait = &wait", ".route_operation = !RECEIVERS", ".receiver_operation = RECEIVERS", "MsgType::GROUP_RECEIVER_REQ", "MsgType::GROUP_ROUTE_REQ", "wki_wait_for_op", "expected_boot"], "owned route operation")
    wrapper = multiline_function_body(source, "wki_group_signal_as")
    require_tokens(wrapper, ["with_local_signal_targets", "group_signal_with_local_targets"], "fixed fatal-cleanup-owned local cohort")
    driver = multiline_function_body(source, "group_signal_with_local_targets")
    require_tokens(driver, ["g_wki.peers", "GroupRouteOperation::ENUMERATE", "GroupRouteOperation::QUERY", "candidates.resolve", "GroupRouteOperation::DELIVER", "request.ceiling = ceiling", "SELF_HOME.serial", "deferred_target", "credential_record::encode(caller)"], "one home incarnation per logical process with original caller")
    require_tokens(driver, ["local_home(*target)", "targets.insert", "request.home = target.home"], "local and home routes enter one owned selection")
    selection = source[source.index("class GroupRouteTargets"):source.index("auto group_signal_with_local_targets")]
    require_tokens(selection, ["selected.home.serial == target.home.serial", "selected.home.origin.node == target.home.origin.node", "selected.home.origin.boot == target.home.origin.boot", "std::nothrow", "-ENOMEM"], "deduplicate full immutable home with checked storage")
    require_tokens(wrapper, ["GroupRouteTargets targets", "INPUT.targets", "targets.clear()"], "route storage belongs to fatal callback cleanup")
    require_order(driver, "int const ENUM_ERROR = collect()", "auto follow =", "complete collection before query or delivery")
    if driver.count("collect()") != 1 or driver.count("for (auto const& target : targets.view())") != 2:
        fail("group resolution and delivery must share a single collected route set")
    require_order(driver, "for (auto* target : local_targets)", "for (size_t i = 0; i < count; ++i)", "local discovery before remote home enumeration")
    if "g_submitted_tasks" in driver or "wki_proxy_task_forward_signal" in driver:
        fail("group discovery must not depend on surviving per-task proxies")
    mapper = multiline_function_body(source, "map_group_caller")
    require_order(mapper, "credential_record::decode", "auth_policy::map_compute_identity", "route credentials decode before mapping")
    require_tokens(mapper, ["capture_compute_submit_session_locked", "SESSION.epoch == 0"], "mapped caller retains exact receive session")
    receiver = function_body(source, "authorize_group_route")
    require_order(receiver, "map_group_caller", "execute_group_route", "map original caller before receiver admission")
    discovery = function_body(source, "authorize_group_receivers")
    require_order(discovery, "map_group_caller", "collect_group_receivers", "current discovery maps original caller")
    require_order(driver, "GroupReceiverResponse page", "GroupRouteResponse page", "current remote receivers precede historical anchors")
    require_tokens(driver, ["WKI_GROUP_RECEIVER_RESOURCE_ID", "CURRENT.home", "CURRENT.pid"], "negotiated full-home current receiver routes")
    collector = function_body(source, "collect_group_receivers")
    require_tokens(collector, ["enumerate_signal_receivers", "append_running_signal_receiver"], "registry and running-import views share pagination")
    running = multiline_function_body(source, "append_running_signal_receiver")
    require_tokens(running, ["!running.published && !running.group_signal_ready", "signal_admission_identity().remote", "TaskType::PROCESS", "running.task->is_thread", "append_signal_receiver"], "committed current canonical imports only")
    receiver_response = function_body(source, "handle_group_receiver_response")
    require_tokens(receiver_response, ["!slot.receiver_operation", "slot.channel != rx_channel", "slot.generation != rx_channel_generation", "slot.source_boot != response.boot", "entry.serial > previous", "entry.home.serial != 0", "claim_and_clear_waiter_locked"], "bounded typed current receiver response binding")
    executor = function_body(source, "execute_group_route")
    require_tokens(executor, ["signal_home_matches", "group_signal_ready", "GroupRouteResult::RETRY", "GroupRouteResult::REDIRECT", "signal_group_target_as"], "exact incarnation and effect-free routing")
    if "group_route_rpc" in executor:
        fail("receiver workers cannot wait for another receiver")
    response = function_body(source, "handle_group_route_response")
    require_tokens(response, ["!slot.route_operation", "slot.generation != rx_channel_generation", "slot.channel != rx_channel", "claim_and_clear_waiter_locked", "slot.source_boot"], "typed route response binding")
    receiver = function_body(source, "authorize_group_signal")
    require_order(receiver, "credential_record::decode", "auth_policy::map_compute_identity", "group credentials decode before mapping")
    require_order(receiver, "auth_policy::map_compute_identity", "signal_group_local_as", "map group caller before effects")
    require_tokens(receiver, ["capture_compute_submit_session_locked", "SESSION.epoch == 0", "resolve_session_origin", "query_group_local"], "exact group session and origin")
    worker = function_body(source, "compute_rx_worker_loop")
    require_order(worker, "work.type = static_cast<uint8_t>(MsgType::GROUP_SIGNAL_RESP)", "MsgType::GROUP_SIGNAL_RESP, work.payload.data()", "cache group result before reply enqueue")
    response = function_body(source, "handle_group_signal_response")
    require_tokens(response, ["!slot.group_operation", "slot.group_query", "slot.group_own_allowed", "slot.generation != rx_channel_generation", "slot.channel != rx_channel", "claim_and_clear_waiter_locked", "slot.source_boot"], "typed group response binding")


def test_exec_signal_context_is_validated_and_installed_before_publication() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    capture = function_body(source, "snapshot_exec_signals")
    require_tokens(capture, ["signal_handlers_snapshot", "signal_mask_bits", "signal_delivery_pending_bits", "WKI_EXEC_UNBLOCKABLE_SIGNALS"], "exec signal value capture")
    if "signal_take_pending_mask" in capture or "signal_pending_store" in capture:
        fail("exec preparation must not consume old-image pending signals before acceptance")
    encode = multiline_function_body(source, "copy_submit_identity_data")
    require_order(encode, "&ORIGINS, sizeof(ORIGINS)", "snapshot_exec_signals", "signal suffix follows existing origins")
    decode = multiline_function_body(source, "prepare_submitted_resources")
    require_tokens(decode, ["WKI_EXEC_SIGNALS_VERSION", "valid_exec_signals(signals)", "*exec_signals = signals"], "bounded signal context validation")
    require_order(decode, "valid_exec_signals(signals)", "*exec_signals = signals", "validate before publishing decoded signal state")
    receiver = multiline_function_body(source, "handle_task_submit_work")
    require_order(receiver, "prepare_submitted_resources", "install_exec_signals", "validate before installing signal values")
    require_order(receiver, "apply_submitted_task_identity", "install_exec_signals", "map credentials before signal installation")
    require_order(receiver, "install_exec_signals", "post_task_balanced", "install signal values before receiver can run")
    install = function_body(source, "install_exec_signals")
    require_tokens(install, ["handlers{}", "initialize_signal_handlers", "signal_mask_store(state.blocked)", "signal_pending_store(state.pending)"], "unpublished signal initialization")
    if "wake_task" in install or "signal_generate_pending_mask" in install:
        fail("imported pending bits must not replay signal generation or wake unpublished tasks")


def test_accepted_signal_route_is_independent_of_scheduler_readiness() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    accept = multiline_function_body(source, "handle_task_accept")
    require_order(accept, "claim_and_clear_waiter_locked", "publish_accepted_signal_route", "only claimed acceptance owns signal routing")
    require_order(accept, "publish_accepted_signal_route", "wki_finish_claimed_op", "publish routing before executor cleanup resumes")
    require_tokens(accept, ["task->target_node != hdr->src_node", "task->submit_channel != rx_channel", "task->submit_channel_generation != rx_channel_generation"], "exact accepted route session")
    complete = multiline_function_body(source, "handle_task_complete")
    require_order(complete, "if (response_waiter != nullptr)", "publish_accepted_signal_route", "implicit acceptance also owns routing")
    sender = multiline_function_body(source, "wki_proxy_task_signal_as")
    require_tokens(sender, ["ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id", "submitted->local_task != &target", "submitted->signal_version != WKI_TASK_SIGNAL_VERSION"], "accepted receiver signal lookup")
    task = TASK_CPP.read_text()
    publish = multiline_function_body(task, "Task::publish_accepted_signal_route")
    require_tokens(publish, ["path_metadata_lock.lock_irqsave", "m_accepted_signal_task_id", "wki_remote_pid = remote_pid"], "atomic accepted route identity")
    if "wki_proxy_task_id =" in publish or "wki_proxy_task =" in publish or "wki_proxy_task_blocked" in publish:
        fail("signal routing must not advance scheduler proxy readiness")
    admission = multiline_function_body(task, "Task::signal_admission_identity")
    require_order(admission, "path_metadata_lock.lock_irqsave", "credentials()", "route and credentials share capture")
    require_order(admission, "job_identity()", "path_metadata_lock.unlock_irqrestore", "route and group share capture")
    clear = multiline_function_body(task, "Task::set_wki_proxy_task_id")
    if "m_accepted_signal_task_id" in clear:
        fail("clearing scheduler proxy state must not revive old local signal authority")


def test_filesystem_preparation_remains_discoverable_during_reentrant_close():
    source = REMOTE_COMPUTE_CPP.read_text()
    owner = source.split("class TaskFilesystemPreparation {", 1)[1].split("// Storage", 1)[0]
    cancel = function_body(owner, "cancel")
    require_order(cancel, "std::exchange(staging_.at(index), nullptr)", "vfs_put_file(file)", "one consumed staging File")
    require_order(cancel, "vfs_put_file(file)", "executor_->pending_wki_filesystems = next_", "remaining staging stays discoverable")
    require_tokens(owner, ["std::array<ker::vfs::File*, 2>", "grant.submission_id != transaction_", "target_boot_",
                           "std::unique_ptr<char[]> heap_cwd_", "Task::FilesystemContext filesystem_"], "bounded complete preparation")
    require_tokens(REMOTE_COMPUTE_KTEST.read_text(), ["PreparationOwnsTextTransactionAndReentrantCleanup",
                    "ker::net::wki::wki_remote_compute_selftest_filesystem_preparation()"], "production preparation fixture must run")


def test_filesystem_wire_fragment_and_full_validation_agree():
    source = REMOTE_COMPUTE_CPP.read_text()
    queue = function_body(source, "queue_admitted_task_submit")
    require_order(queue, "valid_submit_filesystem_identity(*submit)", "new (std::nothrow) uint8_t[payload_len]", "full prefix before allocation")
    require_tokens(source, ["submit.task_id != fragment.task_id || !valid_submit_filesystem_identity(submit)",
                           "if (resource_reject == TaskRejectReason::ACCEPTED && HAS_FILESYSTEM) *filesystem_output = filesystem_candidate"],
                   "fragment header agreement and staged filesystem decode")
    require_tokens(REMOTE_COMPUTE_KTEST.read_text(), ["WireBindsTransactionGrantsAndCompleteIdentity",
                    "ker::net::wki::wki_remote_compute_selftest_filesystem_wire()"], "filesystem wire fixture must run")


def test_exec_transition_preserves_original_fetch_authority():
    source = REMOTE_COMPUTE_CPP.read_text()
    receiver = function_body(source, "handle_task_submit_work")
    order = ["decode_submit_credential_transition(*submit", "prepare_submitted_resources(submit", "prepare_submitted_identity(submitted_identity",
             "prepare_submitted_exec_credentials(submitted_credentials", "ScopedSubmitVfsIdentity submit_vfs_identity",
             "load_elf_from_vfs_path(*current_task", "apply_submitted_task_identity(new_task",
             "publish_exec_credentials(*new_task", "RunningRemoteTask rt"]
    positions = [receiver.index(token) for token in order]
    assert positions == sorted(positions), "exec mapping and original read authority must precede replacement publication"
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        assert "transfer->exec_credentials == nullptr" in body
        assert "WKI_EXEC_HANDOFF_RESOURCE_ID" in body
        assert "filesystem_preparation.credentials()" in body
        assert "cursor + IPC_DATA_LEN, transfer->exec_credentials" in body
        require_order(body, "select_submit_read_credentials", "TaskFilesystemPreparation filesystem_preparation",
                      "validate original/final spawn identity before filesystem preparation")
        assert "st.exec_handoff = transfer != nullptr && transfer->cutover != nullptr" in body
        assert "WKI_TASK_SUBMIT_SPAWN_TRANSITION" in body
    wait = function_body(source, "wait_for_exec_submit")
    require_order(wait, "transfer->cutover == nullptr", "TaskExecCommit commit",
                  "ordinary spawn cannot enter old-process cutover")
    select = function_body(source, "select_submit_read_credentials")
    assert "transfer->image_credentials == nullptr" in select
    assert "prepare_spawn_read_credentials(*transfer->image_credentials" in select
    check = function_body(source, "prepare_spawn_read_credentials")
    require_order(check, "prepared != final", "output = original", "failed validation leaves read identity untouched")
    rollback = wait[wait.index("if (cutover_error != 0)"):wait.index("unsigned attempts")]
    assert rollback.index("finish_or_wait_for_cancelled_waiter") < rollback.index("row->response_consumer_wait_entry = nullptr")
    assert "row->exec_cutover = false" in rollback and "return WKI_ERR_BUSY" in rollback
    assert "selftest_exec_credential_transition()" in function_body(source, "wki_remote_compute_selftest_full_credential_context")
    native = function_body(source, "wki_remote_compute_selftest_exec_cutover_retirement")
    require_tokens(native, ["wait_for_exec_submit(initial", "fail_submitted_tasks_for_peer", "publish_exec_credentials",
                            "response_consumer_wait_entry", "source.ref_count.load() == 1"],
                   "native cutover retirement fixture exercises actual waits, cleanup and credential publication")
    require_tokens(REMOTE_COMPUTE_KTEST.read_text(), ["ExecCutoverRetirementPreservesCredentialAndWaitOwnership",
                   "wki_remote_compute_selftest_exec_cutover_retirement()"], "native cutover fixture is registered")


def test_secure_exec_capability_binds_the_actual_submission_generation():
    source = REMOTE_COMPUTE_CPP.read_text()
    capture = function_body(source, "capture_exec_submit_channel")
    order = ["PeerLifecycleLease", "lifecycle.acquire(node)", "WKI_EXEC_HANDOFF_RESOURCE_ID", "capture_submitter_channel(node)", "output = CHANNEL"]
    positions = [capture.index(token) for token in order]
    assert positions == sorted(positions)
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_function_body(source, name)
        order = ["capture_exec_submit_channel(target_node, SUBMIT_CHANNEL)", "filesystem_admission.channel.generation != SUBMIT_CHANNEL.generation",
                 "wki_ipc_exports_match_submit", "allocate_submitted_task_id_locked", "send_task_submit_payload"]
        positions = [body.index(token) for token in order]
        assert positions == sorted(positions)
        assert "return fail_submit(EXEC_CHANNEL_ADMISSION)" in body


def test_exec_candidate_stays_private_until_cutover() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    spawn = function_body(source, "wki_try_remote_spawn")
    for field, fallback in [("path", "exe_path.data()"), ("bytes.data()", "elf_buffer"),
                            ("bytes.size()", "elf_buffer_size"), ("complete", "elf_buffer_complete"),
                            ("target", "wki_target_hostname.data()"), ("target_flags", "wki_target_flags")]:
        assert f"CANDIDATE->{field} : task->{fallback}" in spawn
    assert "if (CANDIDATE == nullptr) task->wki_skip_legacy_placement = true" in spawn
    cleanup = spawn[spawn.index("if (CANDIDATE == nullptr) {"):]
    cleanup = cleanup[:cleanup.index("if (task->exec_image_file")]
    assert "delete[] task->elf_buffer" in cleanup and "release_cached_elf_buffer(task->elf_buffer)" in cleanup
    assert "wki_task_submit_inline(best_node, BINARY, static_cast<uint32_t>(BINARY_SIZE)" in spawn
    assert "const char* local_path = EXE_PATH" in spawn
    reject = function_body(source, "handle_task_reject")
    assert "if (task->local_task != nullptr && !task->exec_handoff)" in reject
    native = function_body(source, "wki_remote_compute_selftest_exec_handoff_ownership")
    assert "detail::handle_task_reject" in native and "remote_pid == (exec ? 7 : 0)" in native


def main() -> None:
    test_exec_candidate_stays_private_until_cutover()
    test_secure_exec_capability_binds_the_actual_submission_generation()
    test_exec_transition_preserves_original_fetch_authority()
    test_filesystem_wire_fragment_and_full_validation_agree()
    test_filesystem_preparation_remains_discoverable_during_reentrant_close()
    test_accepted_signal_route_is_independent_of_scheduler_readiness()
    test_exec_signal_context_is_validated_and_installed_before_publication()
    test_group_signal_selects_receiver_members_and_owns_replies()
    test_capture_file_and_monitor_ownership_are_independent()
    test_complete_credentials_bind_format_to_admitted_session()
    test_peer_cleanup_marks_all_targeted_submits_terminal_failure()
    test_proxy_wait_completion_respects_waitpid_publish_fence()
    test_task_wait_consumes_completed_submitted_row()
    test_task_wait_completion_slot_is_single_owner()
    test_submitted_task_slots_are_indexed_and_reclaimed()
    test_task_exit_retires_remote_compute_wait_owners()
    test_submit_send_failure_keeps_stack_waiter_exit_discoverable()
    test_task_submit_envelopes_use_bounded_stack_storage()
    test_completion_queue_allocation_and_retry_preserve_ownership()
    test_task_completion_uses_bounded_stack_storage()
    test_proxy_waiting_publication_is_transactional()
    test_remote_load_procfs_uses_locked_snapshot()
    test_preferred_placement_accounts_for_inflight_submissions()
    test_balanced_placement_scores_all_healthy_systems()
    test_placement_controllers_run_before_payload_distribution()
    test_load_report_uses_cpu_accounting_and_shared_local_cache()
    test_receiver_path_localization_bounds_suffix_scan()
    test_vfs_ref_loader_rejects_null_or_empty_path_before_vfs_use()
    test_vfs_ref_loader_deadlines_are_saturating()
    test_shared_elf_cache_preserves_inflight_load_markers()
    test_remote_stdio_capture_is_write_only_and_non_tty()
    test_large_vfs_ref_exec_keeps_a_file_backed_source()
    test_submitted_vfs_policy_is_active_during_elf_construction()
    test_controller_descendants_default_to_local_placement()
    test_submission_errors_and_ipc_suffix_framing()
    test_resource_contract_preserves_baseline_and_completion_ownership()
    test_receiver_child_owner_spans_interpreter_output_and_publication()
    test_resource_rejection_preserves_admission_observations()
    test_receiver_vfs_ref_submit_uses_bounded_worker_pool()
    test_remote_proxy_signals_never_make_the_local_frame_runnable()
    test_submitted_proxy_rows_hold_task_lifetime_until_finalization()
    test_remote_task_exit_wakes_exact_completion_monitor()
    test_accepted_remote_spawn_commits_ipc_fd_ownership()
    test_publication_reports_checked_allocation_failure()
    test_submit_wire_buffers_are_cancelled_with_executor()
    test_receiver_tracking_failure_unwinds_before_scheduler_publication()
    test_receiver_rejection_releases_peer_lease_before_vfs_teardown()
    test_service_report_transport_retains_only_reporting_state()
    test_user_signal_owns_reply_and_target_lifetimes()
    print("WKI remote compute source invariants hold")


if __name__ == "__main__":
    main()
