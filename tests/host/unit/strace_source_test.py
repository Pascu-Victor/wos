#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
STRACE_CPP = ROOT / "modules" / "strace" / "src" / "main.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_strace_startup_wait_is_deadline_bounded() -> None:
    source = STRACE_CPP.read_text()
    require_tokens(
        source,
        [
            "STRACE_STARTUP_WAIT_RETRIES",
            "STRACE_STARTUP_WAIT_POLL_US",
            "wait_for_trace_startup_stop",
            "reap_tracee_after_startup_failure",
            "detach_tracee_after_startup_failure",
        ],
        "strace startup timeout surface",
    )

    startup_body = function_body(source, "wait_for_trace_startup_stop")
    require_tokens(
        startup_body,
        [
            "waitpid(pid, &status, WUNTRACED | WNOHANG)",
            "WIFSTOPPED(status)",
            "errno == EINTR",
            "usleep(STRACE_STARTUP_WAIT_POLL_US)",
            "timed out waiting for pid",
        ],
        "strace bounded startup wait helper",
    )

    attach_body = function_body(source, "attach_and_trace")
    require_tokens(
        attach_body,
        [
            "wait_for_trace_startup_stop(static_cast<pid_t>(pid), status)",
            "detach_tracee_after_startup_failure(pid)",
        ],
        "strace attach startup cleanup",
    )
    if "waitpid(static_cast<pid_t>(pid), &status, WUNTRACED)" in attach_body:
        fail("strace attach startup must not use a blocking waitpid")

    command_body = function_body(source, "trace_command")
    require_tokens(
        command_body,
        [
            "wait_for_trace_startup_stop(CHILD, status)",
            "reap_tracee_after_startup_failure(CHILD)",
        ],
        "strace command startup cleanup",
    )
    if "waitpid(CHILD, &status, WUNTRACED)" in command_body:
        fail("strace command startup must not use a blocking waitpid")

    reap_body = function_body(source, "reap_tracee_after_startup_failure")
    require_tokens(
        reap_body,
        [
            "kill(pid, SIGKILL)",
            "waitpid(pid, &status, WNOHANG)",
            "WAITED == pid || (WAITED < 0 && errno != EINTR)",
        ],
        "strace child startup cleanup",
    )

    detach_body = function_body(source, "detach_tracee_after_startup_failure")
    require_tokens(
        detach_body,
        ["ptrace_call(ker::abi::ptrace::request::DETACH, pid, 0, 0)"],
        "strace attach startup cleanup",
    )


def test_strace_trace_loop_uses_ptrace_syscall_wait() -> None:
    source = STRACE_CPP.read_text()
    trace_body = function_body(source, "trace_loop")
    require_tokens(
        trace_body,
        [
            "ker::abi::ptrace::StopInfo stop{}",
            "syscall_wait(pid, stop)",
            'std::println(stderr, "strace: PTRACE_SYSCALL_WAIT failed")',
            "reap_follow_helpers(state, true)",
            "close_trace_output(output)",
            "return 1",
        ],
        "strace steady ptrace wait",
    )
    if "waitpid(" in trace_body or "WNOHANG" in trace_body:
        fail("strace steady trace loop should use ptrace SYSCALL_WAIT, not raw waitpid polling")


def main() -> None:
    test_strace_startup_wait_is_deadline_bounded()
    test_strace_trace_loop_uses_ptrace_syscall_wait()
    print("strace startup waits are deadline bounded")


if __name__ == "__main__":
    main()
