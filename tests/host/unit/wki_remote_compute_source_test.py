#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"


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
        "proxy_waiter_context_can_be_completed(waiter)",
        "ker::mod::sched::task::task_try_mark_waited_on(*proxy)",
        "write_proxy_wait_status(waiter, wait_status)",
        "clear_proxy_wait_result_state(waiter)",
        "waiter->waiting_for_pid = 0",
    ]:
        if snippet not in completion:
            fail(f"proxy wait completion must use the fenced single-result helper: {snippet}")

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
        "task->complete_wait_entry = &wait;",
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


def main() -> None:
    test_peer_cleanup_marks_all_targeted_submits_terminal_failure()
    test_proxy_wait_completion_respects_waitpid_publish_fence()
    test_task_wait_consumes_completed_submitted_row()
    test_receiver_path_localization_bounds_suffix_scan()
    test_vfs_ref_loader_rejects_null_or_empty_path_before_vfs_use()
    test_vfs_ref_loader_deadlines_are_saturating()
    print("WKI remote compute source invariants hold")


if __name__ == "__main__":
    main()
