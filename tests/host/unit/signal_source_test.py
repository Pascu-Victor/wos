#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:\b(?:auto|void)|extern\s+\"C\"\s+auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_syscall_signal_delivery_updates_live_gs_scratch() -> None:
    source = SIGNAL_CPP.read_text()
    body = function_body(source, "check_pending_signals")

    require_tokens(
        source,
        [
            "auto live_syscall_user_rsp() -> uint64_t",
            "auto live_syscall_return_rip() -> uint64_t",
            "auto live_syscall_return_flags() -> uint64_t",
            "void write_live_syscall_return(uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags)",
            "void write_task_syscall_return(Task& task, uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags)",
            "auto user_signal_target_valid",
        ],
        "signal syscall scratch helpers",
    )
    require_tokens(
        body,
        [
            "uint64_t const USER_RSP = live_syscall_user_rsp();",
            "uint64_t const USER_RIP = live_syscall_return_rip();",
            "uint64_t const USER_RFLAGS = live_syscall_return_flags();",
            "write_task_syscall_return(*task, frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
            "write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
            "write_task_syscall_return(*task, FRAME_ADDR, handler.handler, USER_RFLAGS);",
            "write_live_syscall_return(FRAME_ADDR, handler.handler, USER_RFLAGS);",
        ],
        "syscall signal delivery must keep live GS scratch in sync",
    )
    require_order(
        body,
        "stack_write(stack_base, STACK_OFF_RCX, handler.handler)",
        "write_live_syscall_return(FRAME_ADDR, handler.handler, USER_RFLAGS);",
        "SYSRET target write must be paired with live GS scratch update",
    )
    require_order(
        body,
        "write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
        "return 1;",
        "sigreturn must restore live GS scratch before the iret return",
    )


def test_signal_targets_are_user_canonical_on_all_delivery_paths() -> None:
    signal_source = SIGNAL_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()

    require_tokens(
        signal_source,
        [
            "if (!user_signal_target_valid(handler))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "direct syscall/interrupt signal target validation",
    )
    require_tokens(
        scheduler_source,
        [
            "inline auto user_signal_target_valid(task::Task::SigHandler const& handler) -> bool",
            "if (is_user_signal_handler(handler) && !user_signal_target_valid(handler))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "deferred signal target validation",
    )


if __name__ == "__main__":
    test_syscall_signal_delivery_updates_live_gs_scratch()
    test_signal_targets_are_user_canonical_on_all_delivery_paths()
    print("signal syscall return invariants hold")
