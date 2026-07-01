#!/usr/bin/env python3
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[3]
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
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
        "return signal_pending_bits(order) & ~signal_mask_bits(order);",
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
    scheduler_cpp = SCHEDULER_CPP.read_text()
    remote_compute_cpp = REMOTE_COMPUTE_CPP.read_text()

    require(
        function_body(process_cpp, "wos_proc_kill"),
        "target->signal_add_pending_mask(1ULL << (sig - 1));",
        "kill pending publication",
    )
    require(
        function_body(exit_cpp, "notify_parent_after_exit_ready"),
        "parent->signal_add_pending_mask(SIGCHLD_MASK);",
        "exit SIGCHLD publication",
    )
    require(
        function_body(exit_cpp, "publish_process_exit_request"),
        "task->signal_add_pending_mask(SIGKILL_MASK);",
        "thread-group exit signal publication",
    )
    if scheduler_cpp.count("signal_add_pending_mask(MASK)") < 3:
        fail("scheduler process-group signal paths must publish pending bits with atomic helpers")
    if scheduler_cpp.count("signal_add_pending_mask(1ULL << (14 - 1))") < 2:
        fail("scheduler SIGALRM timer paths must publish pending bits with atomic helpers")
    require(
        remote_compute_cpp,
        "parent->signal_add_pending_mask(1ULL << (WKI_SIGCHLD_NUM - 1));",
        "remote compute SIGCHLD publication",
    )


def test_signal_consumers_use_atomic_clear_helpers() -> None:
    signal_cpp = SIGNAL_CPP.read_text()
    for name in [
        "check_pending_signals",
        "check_pending_signals_interrupt",
        "check_pending_signals_handoff",
        "check_pending_signals_deferred",
    ]:
        body = function_body(signal_cpp, name)
        require(body, "task->signal_deliverable_bits()", f"{name} deliverable snapshot")
        require(body, "task->signal_clear_pending_mask(1ULL << IDX)", f"{name} pending clear")


def main() -> None:
    test_task_signal_fields_are_atomic()
    test_kernel_cpp_uses_signal_helpers()
    test_signal_producers_use_atomic_fetch_or_helpers()
    test_signal_consumers_use_atomic_clear_helpers()
    print("signal atomic source invariants hold")


if __name__ == "__main__":
    main()
