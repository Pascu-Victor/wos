#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"
REMOTE_COMPUTE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.hpp"
REMOTE_COMPUTE_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_remote_compute_ktest.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"


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
    body = function_body(source, "wki_remote_compute_cleanup_for_peer")

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
        "*exit_status = COMPLETED_EXIT_STATUS",
        "return 0",
    ]:
        if snippet not in inactive_block:
            fail(f"wki_task_wait inactive-row fast path must return retained completion status: {snippet}")
    if "SubmittedTask* task = find_submitted_task(task_id);" in body:
        fail("wki_task_wait must not use the active-only submitted-task lookup")
    if "wki_remote_compute_selftest_task_wait_consumes_completed_row" not in source:
        fail("remote-compute KTEST selftest must cover waiting after TASK_COMPLETE made the row inactive")


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
            "task->complete_pending.store(true, std::memory_order_release)",
        ],
        "task wait publish helper",
    )

    clear_body = function_body(source, "clear_task_complete_waiter_after_wait_locked")
    require_tokens(
        clear_body,
        [
            "WAIT_SLOT_OWNED = task->complete_wait_entry == &wait",
            "if (WAIT_SLOT_OWNED)",
            "task->complete_wait_entry = nullptr",
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
        "std::strncpy(pending.path.data(), resolved_path",
    ]:
        require_order(body, guard, later, "VFS_REF loader must reject null/empty path before VFS/cache use")


def test_vfs_ref_loader_deadlines_are_saturating() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    body = function_body(source, "load_elf_from_vfs_path")

    for snippet in [
        "#include <net/wki/timer_math.hpp>",
        "uint64_t const INFLIGHT_DEADLINE_US = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US)",
        "uint64_t const RETRY_DEADLINE_US = wki_future_deadline_us(RETRY_WINDOW_START_US, WKI_VFS_LOAD_RETRY_WINDOW_US)",
        "uint64_t const WAIT_UNTIL_US = wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US)",
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
        "if (wki_now_us() >= INFLIGHT_DEADLINE_US)",
        "inflight wait timeout must be checked after dropping compute lock",
    )
    if body.count("sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US)") < 3:
        fail("VFS_REF loader must keep retry backoff sleeps on every retry path")


def test_remote_stdio_capture_is_write_only_and_non_tty() -> None:
    source = REMOTE_COMPUTE_CPP.read_text()
    capture_isatty = function_body(source, "capture_isatty")
    if "return false;" not in capture_isatty:
        fail("remote stdout/stderr capture must not report tty semantics")

    exec_body = function_body(source, "exec_elf_buffer")
    for snippet in [
        "stdin_file->open_flags = 0;",
        "capture_file->open_flags = 1;",
        "(void)new_task->fd_table.insert(fd_idx, capture_file)",
    ]:
        if snippet not in exec_body:
            fail(f"remote stdio capture must preserve VFS access modes: {snippet}")


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
        "exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared)",
        "submitted policy validation before ELF construction",
    )
    require_order(
        body,
        "exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared)",
        "submit_vfs_identity.transfer_vfs_rules_to(new_task)",
        "validated policy transfer after ELF construction",
    )
    if "deserialize_task_vfs_rules(new_task" in body:
        fail("submitted VFS policy must not be deferred until after Task construction")

    token = "wki_remote_compute_selftest_submit_policy_scope_restores_worker"
    require_tokens(source, [f"auto {token}() -> bool"], "submitted policy scope selftest implementation")
    require_tokens(header, [f"auto {token}() -> bool;"], "submitted policy scope selftest declaration")
    require_tokens(ktest, ["SubmitPolicyScopeRestoresWorker", token], "submitted policy scope KTEST coverage")


def main() -> None:
    test_peer_cleanup_marks_all_targeted_submits_terminal_failure()
    test_proxy_wait_completion_respects_waitpid_publish_fence()
    test_task_wait_consumes_completed_submitted_row()
    test_task_wait_completion_slot_is_single_owner()
    test_remote_load_procfs_uses_locked_snapshot()
    test_load_report_uses_cpu_accounting_and_shared_local_cache()
    test_receiver_path_localization_bounds_suffix_scan()
    test_vfs_ref_loader_rejects_null_or_empty_path_before_vfs_use()
    test_vfs_ref_loader_deadlines_are_saturating()
    test_remote_stdio_capture_is_write_only_and_non_tty()
    test_submitted_vfs_policy_is_active_during_elf_construction()
    print("WKI remote compute source invariants hold")


if __name__ == "__main__":
    main()
