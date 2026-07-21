#!/usr/bin/env python3

import re
from pathlib import Path


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
    predicate = function_body(source, "proxy_waiter_context_can_be_completed")
    for snippet in [
        "!waiter->deferred_task_switch",
        "!waiter->waitpid_publish_pending.load(std::memory_order_acquire)",
    ]:
        if snippet not in predicate:
            fail(f"proxy waiter completion predicate must preserve waitpid publish fence: {snippet}")

    completion = function_body(source, "try_complete_proxy_wait")
    for snippet in [
        "proxy_matches_waiter(waiter, proxy)",
        "proxy_waiter_context_can_be_completed(waiter)",
        "ker::mod::sched::task::task_try_claim_waitpid_completion(*waiter)",
        "ker::mod::sched::task::task_release_waitpid_completion_claim(*waiter)",
        "ker::mod::sched::task::task_try_mark_waited_on(*proxy)",
        "write_proxy_wait_status(waiter, wait_status)",
        "waiter->waitpid_publish_pending.store(false, std::memory_order_release)",
        "ker::mod::sched::task::task_clear_waitpid_block_state(*waiter)",
    ]:
        if snippet not in completion:
            fail(f"proxy wait completion must use the fenced single-result helper: {snippet}")

    match_body = function_body(source, "proxy_matches_waiter")
    for snippet in [
        "proxy->parent_pid != waiter->pid",
        "waiter->waiting_for_pid == WAIT_ANY_CHILD || waiter->waiting_for_pid == proxy->pid",
    ]:
        if snippet not in match_body:
            fail(f"proxy wait completion must reject stale specific waiters: {snippet}")

    wake_body = function_body(source, "wake_proxy_waiters")
    if "try_complete_proxy_wait(waiting_task, proxy, WAIT_STATUS)" not in wake_body:
        fail("explicit proxy waiters must complete through the publish-fenced helper")

    finalize_body = function_body(source, "finalize_proxy_task")
    if "parent->waiting_for_pid == WAIT_ANY_CHILD && try_complete_proxy_wait(parent, proxy, WAIT_STATUS)" not in finalize_body:
        fail("parent wait-any proxy completion must complete through the publish-fenced helper")
    if "parent->waiting_for_pid == WAIT_ANY_CHILD && !parent->deferred_task_switch" in finalize_body:
        fail("parent wait-any proxy completion must not bypass waitpid_publish_pending")

    if "wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence" not in source:
        fail("remote-compute KTEST selftest must cover proxy wait publish fencing")
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


def test_submitted_task_slots_are_indexed_and_reclaimed() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    header = REMOTE_COMPUTE_HPP.read_text()
    ktest = REMOTE_COMPUTE_KTEST.read_text()

    require_tokens(
        source,
        [
            "constexpr size_t WKI_SUBMITTED_TASK_INDEX_BUCKETS = 1024",
            "struct SubmittedTaskSlot",
            "std::deque<SubmittedTaskSlot> g_submitted_tasks",
            "std::array<SubmittedTaskSlot*, WKI_SUBMITTED_TASK_INDEX_BUCKETS>",
            "SubmittedTaskSlot* s_submitted_task_free = nullptr",
            "size_t s_submitted_task_count = 0",
            "auto find_submitted_task_slot_locked(uint32_t task_id) -> SubmittedTaskSlot*",
            "auto publish_submitted_task_locked(SubmittedTask&& task) -> SubmittedTask*",
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
                "publish_submitted_task_locked(std::move(st))",
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
            "submitted.active = false",
            "consume_submitted_task_result_locked(&submitted)",
            "wki_claim_op(waiter)",
            "wki_finish_claimed_op(waiter, WKI_ERR_PEER_FENCED)",
            "wki_quiesce_claimed_op(waiter)",
            "send_task_cancel_request",
            "cleanup_submitted_ipc_exports(ipc_cleanup)",
            "delete[] discarded_output",
        ],
        "task-exit remote-compute ownership retirement",
    )
    unlock_pos = cleanup.find("s_compute_lock.unlock()")
    for unlocked_action in [
        "wki_claim_op(waiter)",
        "send_task_cancel_request",
        "cleanup_submitted_ipc_exports(ipc_cleanup)",
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
            "complete_unpublished_process_construction(Task* task)",
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
            "release_file_mmap_ranges_for_pagemap(task->pagemap)",
            "mm::virt::destroy_user_space",
            "mm::virt::release_pagemap",
            "mm::phys::page_free",
        ],
        "complete unpublished-process teardown",
    )
    exclusive_check = function_body(task_source, "unpublished_process_is_exclusively_owned")
    require_tokens(
        exclusive_check,
        [
            "task->scheduler_published.load(std::memory_order_acquire)",
            "task->state.load(std::memory_order_acquire) != TaskState::ACTIVE",
            "task->gc_queued.load(std::memory_order_acquire)",
            "task->ref_count.load(std::memory_order_acquire) != 2",
        ],
        "unpublished-process exclusive lifetime validation",
    )
    for racy_scheduler_field in ["sched_queue", "heap_index", "sched_next"]:
        if racy_scheduler_field in exclusive_check:
            fail(f"unpublished teardown must not inspect unlocked scheduler field {racy_scheduler_field}")
    require_tokens(
        task_header,
        ["std::atomic<bool> scheduler_published{false}"],
        "monotonic scheduler-publication marker",
    )
    runnable_post = function_body(scheduler_source, "publish_runnable_task_locked")
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
            "read_boot_file_fully(task->pending_interp_path.data(), &interp_buf)",
            "loader::elf::load_elf",
            "task->context.frame.rip = INTERP_RESULT.entry_point",
            "task->interp_base = INTERP_BASE",
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
        "complete_unpublished_process_construction(new_task)",
        "exec must claim fatal-exit ownership before PT_INTERP VFS I/O",
    )
    require_order(
        exec_body,
        "complete_unpublished_process_construction(new_task)",
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
        ["complete_unpublished_process_construction(new_task)"],
        "pre-scheduler init PT_INTERP completion",
    )
    require_tokens(
        function_body(signal_source, "exit_current_on_pending_fatal_default_signal"),
        ["unpublished_teardown_in_progress.load(std::memory_order_acquire)"],
        "fatal-signal handoff deferral during unpublished teardown",
    )
    requested_exit = function_body(process_exit, "exit_current_if_process_exit_requested")
    require_order(
        requested_exit,
        "unpublished_teardown_in_progress.load(std::memory_order_acquire)",
        "process_exit_requested.load(std::memory_order_acquire)",
        "group-exit request must remain pending during unpublished teardown",
    )

    reschedule = function_body(scheduler_source, "reschedule_task_for_cpu_once")
    waitpid_repair_start = reschedule.find("if (is_waitpid_wait_channel")
    final_insert_start = reschedule.find("// Insert into target CPU's heap", waitpid_repair_start)
    waitpid_repair = reschedule[waitpid_repair_start:final_insert_start]
    require_order(
        waitpid_repair,
        "task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE",
        "wait_list_push_locked(rq, task)",
        "waitpid repair must revalidate lifecycle under the owner runqueue lock",
    )
    final_lock_start = reschedule.rfind("run_queues->with_lock_void(cpu_no")
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
        "task->wki_proxy_task_id = tid",
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
            "task->wki_proxy_task_id = 0",
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
            "if (TOTAL > WKI_ETH_MAX_PAYLOAD)",
            "auto msg_len = static_cast<uint16_t>(TOTAL)",
        ),
        "wki_task_submit_vfs_ref": (
            "auto wki_task_wait(",
            "if (total > WKI_ETH_MAX_PAYLOAD)",
            "auto msg_len = static_cast<uint16_t>(total)",
        ),
    }
    for function_name, (next_function, size_guard, length_assignment) in submitters.items():
        body_start = source.index(f"auto {function_name}(")
        body = source[body_start : source.index(next_function, body_start + 1)]
        require_tokens(
            body,
            [
                size_guard,
                length_assignment,
                "ipc_fd_count != 0 && ipc_fd_map == nullptr",
                "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> buf __attribute__((uninitialized));",
                "reinterpret_cast<TaskSubmitPayload*>(buf.data())",
                "uint8_t* cursor = buf.data() + sizeof(TaskSubmitPayload)",
                "MsgType::TASK_SUBMIT, buf.data(),",
            ],
            f"{function_name} bounded stack envelope",
        )
        require_order(
            body,
            size_guard,
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> buf",
            f"{function_name} size check before stack use",
        )
        require_order(
            body,
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> buf",
            "MsgType::TASK_SUBMIT, buf.data(),",
            f"{function_name} build before send",
        )
        for forbidden in ["new (std::nothrow) uint8_t[msg_len]", "delete[] buf"]:
            if forbidden in body:
                fail(f"{function_name} must not heap-own its bounded TASK_SUBMIT envelope: found {forbidden}")
        if re.search(r"std::array<uint8_t,\s*WKI_ETH_MAX_PAYLOAD>\s+buf\s*(?:\{\}|=\s*\{\})", body):
            fail(f"{function_name} must not explicitly initialize the unused stack-envelope tail")
        if re.search(r"MsgType::TASK_SUBMIT,\s*buf\.data\(\),\s*msg_len\)", body) is None:
            fail(f"{function_name} must send exactly the initialized msg_len prefix")

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
        "memcpy(rt_data, frame, FRAME_LEN)",
        "TASK_SUBMIT borrow copied into frame before retransmit ownership",
    )
    require_order(
        send_impl,
        "memcpy(rt_data, frame, FRAME_LEN)",
        "return WKI_OK",
        "TASK_SUBMIT borrow retained synchronously before send returns",
    )
    require_tokens(
        function_body(wki, "wki_send_on_channel_generation"),
        ["return wki_send_impl("],
        "generation-qualified sender synchronous delegation",
    )


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
            "std::min<uint16_t>(info.output->len, WKI_TASK_MAX_OUTPUT)",
            "std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> buf __attribute__((uninitialized));",
            "reinterpret_cast<TaskCompletePayload*>(buf.data())",
            "complete->task_id = info.task_id",
            "complete->exit_status = info.exit_status",
            "complete->output_len = OUT_LEN",
            "complete->reserved = 0",
            "memcpy(buf.data() + sizeof(TaskCompletePayload), info.output->data.data(), OUT_LEN)",
            "MsgType::TASK_COMPLETE, buf.data(), MSG_LEN",
        ],
        "stack-backed TASK_COMPLETE frame",
    )
    require_order(completion, "complete->reserved = 0", "memcpy(buf.data()", "completion header before output")
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
            "EXISTING == task",
            "EXISTING->pid == task->pid",
            'panic_handler("scheduler: fresh task already present in active registry")',
            "entry.pid == task->pid",
            "entry.task == task",
            'panic_handler("scheduler: fresh task already present in PID registry")',
            "pid_index == MAX_PIDS",
            "pid_slot(pid_index).task = task",
            "pid_slot(pid_index).pid = task->pid",
            "active_task_slot(active_task_count) = task",
            "active_task_count++",
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
        "EXISTING->pid == task->pid",
        "active_task_slot(active_task_count) = task",
        "fresh-task active collision check before registry commit",
    )
    commit = registration[registration.find("pid_slot(pid_index).task = task") :]
    require_order(commit, "pid_slot(pid_index).task = task", "pid_slot(pid_index).pid = task->pid", "fresh PID entry commit")
    require_order(
        commit,
        "pid_slot(pid_index).pid = task->pid",
        "active_task_slot(active_task_count) = task",
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
            "wait_list_push_locked(rq, task)",
            "published = register_fresh_task_visibility(task)",
            "if (!wait_list_remove_locked(rq, task))",
            "task->sched_queue = task::Task::sched_queue::NONE",
            "return published",
        ],
        "transactional fresh WAITING publication",
    )
    require_order(
        publication,
        "wait_list_push_locked(rq, task)",
        "published = register_fresh_task_visibility(task)",
        "wait-list park before global task visibility",
    )
    require_order(
        publication,
        "published = register_fresh_task_visibility(task)",
        "if (!wait_list_remove_locked(rq, task))",
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
        "ker::vfs::vfs_stat(resolved_path",
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
    if body.count("sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US)") < 3:
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

    exec_body = function_body(source, "finish_remote_exec_task")
    for snippet in [
        "stdin_file->open_flags = 0;",
        "capture_file->open_flags = 1;",
        "(void)new_task->fd_table.insert(fd_idx, capture_file)",
    ]:
        if snippet not in exec_body:
            fail(f"remote stdio capture must preserve VFS access modes: {snippet}")


def test_large_vfs_ref_exec_keeps_a_file_backed_source() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    loader_body = function_body(source, "load_elf_from_vfs_path")
    require_tokens(
        loader_body,
        [
            "std::cmp_greater_equal(statbuf.st_size, WKI_FILE_BACKED_ELF_MIN_SIZE)",
            "result.file = ker::vfs::vfs_get_file_retain(CURRENT_TASK, FD)",
            "ker::syscall::process::supports_file_backed_process(result.file",
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
            "elf_file != nullptr ? exec_elf_file(elf_file, binary_len, elf_file_stat)",
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
        "if (!submit_vfs_identity.policy_valid())",
        "ExecResult const EXEC = elf_file != nullptr",
        "submitted policy validation before ELF construction",
    )
    require_order(
        body,
        "ExecResult const EXEC = elf_file != nullptr",
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
        "apply_submitted_task_identity(new_task, submitted_identity)",
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
            "const char* const TARGET_HOSTNAME = task->wki_target_hostname.data()",
            "uint16_t node_id = wki_peer_find_by_hostname(TARGET_HOSTNAME)",
            'constexpr std::string_view WOS_DOMAIN_SUFFIX = ".wos"',
            "node_id = wki_peer_find_by_hostname(short_target.data())",
            "if (node_id == g_wki.my_node_id)",
            'log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "explicit-local-target")',
            "return WkiRemoteSpawnResult::LOCAL",
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


def test_receiver_child_owner_spans_interpreter_output_and_publication() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
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
        "complete_unpublished_process_construction(new_task)",
        "receiver must publish child ownership before blocking PT_INTERP loading",
    )
    require_order(
        exec_body,
        "complete_unpublished_process_construction(new_task)",
        "finish_remote_exec_task(new_task)",
        "receiver must finish the owned interpreter stage before output setup",
    )
    require_tokens(
        exec_body,
        [
            "new_task->thread == nullptr || new_task->pagemap == nullptr || new_task->entry == 0",
            'panic_handler("WKI remote compute: cannot publish receiver child recovery ownership")',
            'destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-construction")',
            'destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-interpreter")',
        ],
        "receiver construction failure ownership",
    )
    if "release_unpublished_process(" in exec_body:
        fail("exec_elf_buffer must return with the receiver child owner slot intact")

    work_body = function_body(source, "handle_task_submit_work")
    require_order(
        work_body,
        "ExecResult const EXEC = elf_file != nullptr",
        "g_running_remote_tasks.push_back(rt)",
        "receiver child ownership must cover output and prepublication monitor setup",
    )
    require_order(
        work_body,
        "bool const POSTED = ker::mod::sched::post_task_balanced(new_task)",
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
            "constexpr size_t WKI_COMPUTE_SUBMIT_WORKER_MAX = 4",
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
            "kern_wake(s_compute_submit_tasks.at(i))",
        ],
        "compute-submit wake-all",
    )

    require_tokens(
        source,
        [
            "constexpr size_t WKI_COMPUTE_RX_WORKER_MAX = 4",
            "WKI_COMPUTE_RX_QUEUE_MAX = WKI_CREDITS_RESOURCE * WKI_COMPUTE_RX_WORKER_MAX",
            "WKI_COMPUTE_RX_PAYLOAD_MAX = sizeof(TaskCompletePayload) + WKI_TASK_MAX_OUTPUT",
            "std::array<PendingComputeRx, WKI_COMPUTE_RX_QUEUE_MAX>",
            "std::array<ComputeRxShard, WKI_COMPUTE_RX_WORKER_MAX>",
            "std::array<bool, WKI_COMPUTE_RX_QUEUE_MAX>",
            "uint8_t type = 0",
        ],
        "bounded zero-initialized compute RX pool",
    )
    admission_body = function_body(source, "wki_remote_compute_admit_rx")
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
            "peer->state == PeerState::CONNECTED",
            "compute_submit_channel_token_matches(",
            "detail::handle_task_complete(",
            "detail::handle_task_cancel(",
            "release_compute_rx_slot(slot_index)",
        ],
        "task-context compute RX dispatch",
    )
    require_tokens(smt, ["wki_remote_compute_start_rx_threads()"], "compute RX startup after scheduler initialization")

    handler_body = function_body(source, "handle_task_submit")
    require_tokens(
        handler_body,
        [
            "capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation)",
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
            "compute_submit_session_is_current_locked(session)",
            "take_cancelled_task_submit_locked(session, submit->task_id)",
            "wki_send_on_channel_generation(src_node, session.rx_channel, session.rx_channel_generation",
            "Task submit expired before post",
            "perf_compute_reject_status(TaskRejectReason::OVERLOADED)",
            "rt.published = false",
            "rt.accept_pending = false",
            "rt.submit_session_epoch = session.epoch",
            "g_running_remote_tasks.push_back(rt)",
            "bool const POSTED = ker::mod::sched::post_task_balanced(new_task)",
            "published_row->published = true",
            "published_row->accept_pending = send_accept",
            "worker_task_ref = new_task",
            "worker_task_ref->release()",
        ],
        "final submit session, cancellation, publication, and accept fence",
    )
    publication_block = work_body[work_body.find("g_running_remote_tasks.push_back(rt)") :]
    require_order(
        publication_block,
        "g_running_remote_tasks.push_back(rt)",
        "s_compute_lock.unlock()",
        "running row must be published before releasing the compute lock",
    )
    require_order(
        publication_block,
        "s_compute_lock.unlock()",
        "bool const POSTED = ker::mod::sched::post_task_balanced(new_task)",
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
        "delete output",
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
        "wki-submit-post",
    ]:
        require_tokens(
            work_body,
            ["destroy_unpublished_remote_process(current_task, new_task,"],
            f"{reason} rejection uses complete unpublished-process cleanup",
        )
        if reason not in work_body:
            fail(f"{reason} rejection must use complete unpublished-process cleanup")

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
            "g_pending_task_completions.push_back(info)",
            "delete rt.output",
            "rt.task->release()",
        ],
        "retired peer output capture release after task exit",
    )

    cancel_body = function_body(source, "handle_task_cancel")
    require_tokens(
        cancel_body,
        [
            "capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation)",
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
            "if (TOTAL > WKI_ETH_MAX_PAYLOAD)",
            "capture_submitter_channel(target_node)",
            "wki_send_on_channel_generation(target_node, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation",
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
            "capture_submitter_channel(target_node)",
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
    if "MsgType::TASK_COMPLETE" in lifecycle_body or "MsgType::TASK_CANCEL" in lifecycle_body:
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
    require_order(
        rx_body,
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
            "bool rx_baseline_initialized = false",
        ],
        "reliable receive retry and no-ACK sentinel state",
    )
    require_tokens(
        channel,
        ["ch->rx_baseline_initialized = false", "ch->rx_ack_pending = WKI_ACK_NONE"],
        "channel reset clears receive baseline and cumulative ACK state",
    )
    require_tokens(wki, ["ch->rx_ack_pending = WKI_ACK_NONE"], "fresh channels start without a cumulative ACK")
    require_order(
        rx_body,
        "ch->rx_baseline_initialized = true",
        "wki_remote_compute_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)",
        "first refused sequence pins receive baseline",
    )

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
            "finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, tid)",
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

    forward_body = function_body(source, "wki_proxy_task_forward_signal")
    require_tokens(
        forward_body,
        [
            "signum <= 0",
            "ker::mod::sched::task::Task::MAX_SIGNALS",
            "return wki_task_cancel(PROXY_TID, signum)",
        ],
        "all valid proxy signals must be owned by the remote task",
    )
    if "signum != WKI_SIGINT_NUM" in forward_body:
        fail("proxy signal forwarding must not fall back to local execution for non-INT/KILL/TERM signals")

    reschedule_body = function_body(scheduler, "reschedule_task_for_cpu_once")
    require_order(
        reschedule_body,
        "if (task->wki_proxy_task_id != 0)",
        "// Remove from whatever queue the task is in.",
        "proxy park guard must precede every runqueue removal/publication",
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


def main() -> None:
    test_peer_cleanup_marks_all_targeted_submits_terminal_failure()
    test_proxy_wait_completion_respects_waitpid_publish_fence()
    test_task_wait_consumes_completed_submitted_row()
    test_task_wait_completion_slot_is_single_owner()
    test_submitted_task_slots_are_indexed_and_reclaimed()
    test_task_exit_retires_remote_compute_wait_owners()
    test_submit_send_failure_keeps_stack_waiter_exit_discoverable()
    test_task_submit_envelopes_use_bounded_stack_storage()
    test_task_completion_uses_bounded_stack_storage()
    test_proxy_waiting_publication_is_transactional()
    test_remote_load_procfs_uses_locked_snapshot()
    test_preferred_placement_accounts_for_inflight_submissions()
    test_load_report_uses_cpu_accounting_and_shared_local_cache()
    test_receiver_path_localization_bounds_suffix_scan()
    test_vfs_ref_loader_rejects_null_or_empty_path_before_vfs_use()
    test_vfs_ref_loader_deadlines_are_saturating()
    test_shared_elf_cache_preserves_inflight_load_markers()
    test_remote_stdio_capture_is_write_only_and_non_tty()
    test_large_vfs_ref_exec_keeps_a_file_backed_source()
    test_submitted_vfs_policy_is_active_during_elf_construction()
    test_controller_descendants_default_to_local_placement()
    test_receiver_child_owner_spans_interpreter_output_and_publication()
    test_receiver_vfs_ref_submit_uses_bounded_worker_pool()
    test_remote_proxy_signals_never_make_the_local_frame_runnable()
    test_submitted_proxy_rows_hold_task_lifetime_until_finalization()
    test_remote_task_exit_wakes_exact_completion_monitor()
    print("WKI remote compute source invariants hold")


if __name__ == "__main__":
    main()
