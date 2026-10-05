#!/usr/bin/env python3
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[3]
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
CHILD_EVENTS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "child_events.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def require(source: str, snippet: str, label: str) -> None:
    if snippet not in source:
        fail(f"{label} is missing: {snippet}")


def function_body(source: str, name: str) -> str:
    marker = source.find(name)
    if marker < 0:
        fail(f"function {name} not found")
    start = source.find("{", marker)
    if start < 0:
        fail(f"function {name} has no body")
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    fail(f"function {name} body is unterminated")
    raise AssertionError("unreachable")


def test_task_signal_fields_are_atomic() -> None:
    task_hpp = TASK_HPP.read_text()
    for snippet in [
        "std::atomic<uint64_t> sig_pending{0};",
        "std::atomic<uint64_t> sig_mask{0};",
        "std::atomic<uint64_t> sig_mask_seq{0};",
        "sig_pending.fetch_or(mask, order)",
        "sig_pending.fetch_and(~mask, order)",
        "sig_mask.fetch_or(mask, order)",
        "sig_mask_seq.fetch_add(1, order) + 1",
        "return pending & ~signal_mask_bits(order);",
    ]:
        require(task_hpp, snippet, "Task atomic signal API")


def test_kernel_cpp_uses_signal_helpers() -> None:
    offenders: list[str] = []
    for path in (ROOT / "modules" / "kern" / "src").rglob("*.cpp"):
        source = path.read_text()
        for match in re.finditer(r"->sig_(?:pending|mask|mask_seq)\b", source):
            line_start = source.rfind("\n", 0, match.start()) + 1
            line_end = source.find("\n", match.start())
            line = source[line_start : line_end if line_end >= 0 else len(source)]
            if "options->sig_mask" in line:
                continue
            line = source.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(ROOT)}:{line}: {match.group(0)}")
    if offenders:
        fail("kernel C++ files must use Task signal helpers:\n" + "\n".join(offenders))


def test_signal_producers_use_atomic_fetch_or_helpers() -> None:
    process_cpp = PROCESS_CPP.read_text()
    exit_cpp = EXIT_CPP.read_text()
    child_events_cpp = CHILD_EVENTS_CPP.read_text()
    scheduler_cpp = SCHEDULER_CPP.read_text()
    remote_compute_cpp = REMOTE_COMPUTE_CPP.read_text()

    require(
        function_body(process_cpp[process_cpp.index("auto signal_local_as("):], "signal_local_as"),
        "target.signal_generate_pending_mask(1ULL << (signum - 1));",
        "kill pending publication",
    )
    require(
        function_body(child_events_cpp, "deliver_wake"),
        "signal_child_event(*wake.signal_owner, ker::net::wki::WKI_CHILD_SIGNAL_HOPS);",
        "child-event SIGCHLD publication",
    )
    require(
        function_body(process_cpp, "child_event_signal_local"),
        "target.signal_add_pending_mask(uint64_t{1} << 16);",
        "admitted child-event pending publication",
    )
    require(
        function_body(exit_cpp, "publish_process_exit_request"),
        "task->signal_add_pending_mask(SIGKILL_MASK);",
        "thread-group exit signal publication",
    )
    require(function_body(scheduler_cpp, "auto signal_visible_processes_except("), "signal_generate_pending_mask(MASK)",
            "shutdown signal publication")
    for name in ["auto signal_terminal_process_group(", "void revoke_terminal_attachment("]:
        require(function_body(scheduler_cpp, name), "signal_terminal_source_as", "terminal source admission")
    require(function_body(process_cpp, "auto terminal_source_signal("), "generate_group_signal", "admitted terminal generation")
    if scheduler_cpp.count("signal_add_pending_mask(1ULL << (14 - 1))") < 2:
        fail("scheduler SIGALRM timer paths must publish pending bits with atomic helpers")
    require(
        function_body(remote_compute_cpp, "run_one_proxy_exit"),
        "child_events::publish_exit(task)",
        "remote compute routes SIGCHLD through the child-event producer",
    )
    if "WKI_SIGCHLD_NUM" in remote_compute_cpp:
        fail("remote compute must not publish a second out-of-band SIGCHLD")


def test_signal_consumers_use_atomic_clear_helpers() -> None:
    signal_cpp = SIGNAL_CPP.read_text()
    selector = function_body(signal_cpp, "pending_signals_for_user_delivery")
    require(selector, "task->signal_pending_bits()", "STOP atomic snapshot")
    require(selector, "task->signal_deliverable_bits()", "ordinary atomic snapshot")
    for name in [
        "check_pending_signals",
        "check_pending_signals_interrupt",
        "check_pending_signals_handoff",
        "check_pending_signals_deferred",
    ]:
        body = function_body(signal_cpp, name)
        snapshot = "task->signal_deliverable_bits()" if name == "check_pending_signals" else "pending_signals_for_user_delivery(task)"
        require(body, snapshot, f"{name} deliverable snapshot")
        require(body, "task->claim_signal_for_delivery(1ULL << IDX)", f"{name} reserves pending delivery")
        require(body, "if (!claim.commit())", f"{name} commits only a surviving occurrence")


def test_dispositions_are_shared_snapshots_and_faults_precede_publication() -> None:
    task = TASK_HPP.read_text()
    signal = SIGNAL_CPP.read_text()
    process = PROCESS_CPP.read_text()
    for path in (ROOT / "modules" / "kern" / "src").rglob("*.cpp"):
        if "->sig_handlers" in path.read_text():
            fail(f"raw task-local disposition access: {path}")
    require(task, "SignalHandlerTable m_signal_handlers{};", "private disposition store")
    body = function_body(task, "auto signal_handler_snapshot")
    require(body, "credential_owner()", "retained process disposition owner")
    require(body, "lock_irqsave()", "IRQ-safe disposition snapshot")
    require(body, "unlock_irqrestore(IRQF)", "snapshot releases store lock")
    if signal.count("auto const HANDLER = task->signal_handler_snapshot(IDX);") != 6:
        fail("every delivery path must retain one immutable handler value")
    body = function_body(process, "auto wos_proc_sigaction")
    ordered = ["copy_value_from_task", "MutexGuard action_guard", "signal_handler_snapshot",
               "copy_value_to_task", "publish_signal_handler"]
    positions = [body.find(token) for token in ordered]
    if min(positions) < 0 or positions != sorted(positions):
        fail("sigaction must serialize old action and publish only after successful output")
    exec_source = (PROCESS_CPP.parent / "exec.cpp").read_text()
    require(exec_source, "MutexGuard action_guard(task->signal_action_mutex())", "exec transaction serialization")
    require(exec_source, "task->reset_signal_state_for_exec();", "exec process signal state reset")
    reset = function_body(task, "void reset_signal_state_for_exec()")
    require(reset, "handler.handler == IGNORED_HANDLER ? IGNORED_HANDLER : 0", "exec preserves ignored dispositions")
    for token in ["sigaltstack_sp = 0", "sigaltstack_size = 0", "sigaltstack_flags = 2",
                  "sigsuspend_active = false", "do_sigreturn = false", "in_signal_handler = false"]:
        require(reset, token, "exec drops old-image signal state")
    exec_body = function_body(exec_source[exec_source.rindex("auto finish_native_exec_image("):], "auto finish_native_exec_image(")
    commit = exec_body[exec_body.index("auto& COMMIT_STAGE"):]
    if not (commit.index("commit_native_exec_image(task, *PREPARED)") < commit.index("if (COMMIT_RESULT < 0)") <
            commit.index("retire_exec_siblings(*task)") < commit.rindex("MutexGuard action_guard") <
            commit.index("reset_signal_state_for_exec()") < commit.index("Exec admission and owned storage are gone")):
        fail("exec signal reset must serialize after successful commit and sibling retirement, before admission reopens")
    for token in ["signal_pending_store(", "signal_mask_store("]:
        if token in reset or token in commit.replace("task->signal_mask_store(handoff->signal_mask);", ""):
            fail("exec commit must preserve pending signals and the blocked mask")
    transfer = function_body(exec_source, "void transfer_native_worker_signals")
    require(transfer, "owner.signal_add_pending_mask(worker.signal_take_pending_mask", "worker exec transfers pending bits atomically")
    if not (transfer.index("local_signal_admissions_drained()") < transfer.index("owner.signal_mask_store(mask)") <
            transfer.index("worker.signal_take_pending_mask")):
        fail("worker signal admissions must drain before transfer")
    if not (commit.index("if (handoff != nullptr)", commit.index("mark_activated()")) <
            commit.index("transfer_native_worker_signals(*task, *handoff->worker, handoff->signal_mask") < commit.index("acknowledge_worker_exec(*task, *handoff, 0)") <
            commit.index("retire_exec_siblings(*task)")):
        fail("worker signals must transfer before acknowledgement and synthetic sibling retirement")


def test_default_stop_claim_and_marker_share_cancellation_lock() -> None:
    source = SIGNAL_CPP.read_text()
    controller = (TASK_HPP.parent / "job_control.cpp").read_text()
    body = function_body(controller, "auto begin_stop")
    tokens = ["resource_lock.lock_irqsave()", "signal_claim_matches(*claim, MASK)",
              "claim->commit()", "consume_default_stop(*pending_subject", "jobctl_barrier.request_stop"]
    positions = [body.find(token) for token in tokens]
    if min(positions) < 0 or positions != sorted(positions):
        fail("default stop must consume matching claim/marker under generation's resource lock")
    require(body, "subject.retired_signal_owner() != owner", "shared claim requires retired owner")
    action = function_body(source, "auto apply_job_control_stop")
    require(action, "begin_stop(*task, signo, claim)", "default action passes reservation provenance")
    for name in ("check_pending_signals", "check_pending_signals_interrupt", "check_pending_signals_deferred"):
        body = function_body(source, name)
        default = body[body.index("if (HANDLER.handler == WOS_SIG_DFL)"):]
        if default.find("&claim)") < 0 or default.find("&claim)") > default.find("consume_signal_for_delivery"):
            fail(f"{name} consumes default stop before resolving its claim provenance")
    handoff = function_body(source, "auto handle_handoff_non_user_signal_action")
    require(handoff, "apply_job_control_stop(task, signo, false, &claim)", "handoff default stop provenance")
    if "claim.commit()" in handoff:
        fail("handoff may not defer stop consumption until after publishing its action")


def test_fatal_sigkill_rechecks_late_exit_request() -> None:
    source = EXIT_CPP.read_text()
    dispatch = function_body(source, "wos_proc_exit_signal")
    require(dispatch, "execute_process_exit(prepare_signal_exit(", "fatal dispatch uses late request selection")
    select = function_body(source, "prepare_signal_exit")
    require(select, "if (signo == 9)", "only internal kill wake overrides signal status")
    require(select, "take_requested_process_exit(task, disposition)", "request checked after signal selection")
    consume = function_body(source, "take_requested_process_exit")
    require(consume, ".thread_only = task->is_thread", "sibling request remains thread-only")
    require(consume, "process_exit_requested.load(std::memory_order_acquire)", "acquire published request")
    execute = function_body(source, "execute_process_exit")
    if execute.index("wos_thread_exit_current()") > execute.index("wos_proc_exit_with_wait_status"):
        fail("thread-only retirement must precede group exit")


def main() -> None:
    test_fatal_sigkill_rechecks_late_exit_request()
    test_default_stop_claim_and_marker_share_cancellation_lock()
    test_dispositions_are_shared_snapshots_and_faults_precede_publication()
    test_task_signal_fields_are_atomic()
    test_kernel_cpp_uses_signal_helpers()
    test_signal_producers_use_atomic_fetch_or_helpers()
    test_signal_consumers_use_atomic_clear_helpers()
    print("signal atomic source invariants hold")


if __name__ == "__main__":
    main()
