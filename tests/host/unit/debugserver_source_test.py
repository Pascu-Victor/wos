#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEBUGSERVER_CPP = ROOT / "modules" / "debugserver" / "src" / "main.cpp"


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


def test_debugserver_launch_startup_wait_is_deadline_bounded() -> None:
    source = DEBUGSERVER_CPP.read_text()
    require_tokens(
        source,
        [
            "DEBUGSERVER_STARTUP_WAIT_RETRIES",
            "DEBUGSERVER_STARTUP_WAIT_POLL_US",
            "terminate_launched_tracee_after_startup_failure",
        ],
        "debugserver launch timeout surface",
    )

    wait_body = function_body(source, "wait_for_traced_stop")
    if "waitpid(static_cast<pid_t>(pid), &status, options)" in wait_body:
        fail("debugserver launch wait must not use an unbounded waitpid")
    require_tokens(
        wait_body,
        [
            "waitpid(static_cast<pid_t>(pid), &status, options | WNOHANG)",
            "WIFSTOPPED(status)",
            "errno == EINTR",
            "usleep(DEBUGSERVER_STARTUP_WAIT_POLL_US)",
            "timed out waiting for launched process",
        ],
        "debugserver bounded launch wait",
    )

    cleanup_body = function_body(source, "terminate_launched_tracee_after_startup_failure")
    require_tokens(
        cleanup_body,
        [
            "ptrace_call(ker::abi::ptrace::request::KILL, pid, 0, 0)",
            "waitpid(static_cast<pid_t>(pid), &status, WNOHANG)",
            "WAITED == static_cast<pid_t>(pid) || (WAITED < 0 && errno != EINTR)",
        ],
        "debugserver launch cleanup",
    )

    launch_body = function_body(source, "launch_process")
    require_tokens(
        launch_body,
        [
            "terminate_launched_tracee_after_startup_failure(pid)",
            "wait_for_traced_stop(pid, WUNTRACED, status)",
            "run_to_exec_start(pid)",
        ],
        "debugserver launch cleanup plumbing",
    )

    wait_event_body = function_body(source, "wait_for_debug_event")
    if "WNOHANG" in wait_event_body:
        fail("active debug-event waits should remain blocking; only launch startup waits are deadline bounded")


def test_debugserver_packet_io_is_deadline_bounded() -> None:
    source = DEBUGSERVER_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <fcntl.h>",
            "#include <poll.h>",
            "DEBUGSERVER_PACKET_IO_TIMEOUT_MS",
            "DEBUGSERVER_EVENT_POLL_MS",
            "set_nonblocking_for_timeout",
            "wait_fd_ready_until",
            "read_exact_timeout",
            "write_all_timeout",
        ],
        "debugserver packet timeout surface",
    )

    raw_reads = re.findall(r"\bread\s*\(", source)
    if len(raw_reads) != 1:
        fail(f"debugserver raw read() calls must be isolated to read_exact_timeout, saw {len(raw_reads)}")
    raw_writes = re.findall(r"\bwrite\s*\(", source)
    if len(raw_writes) != 1:
        fail(f"debugserver raw write() calls must be isolated to write_all_timeout, saw {len(raw_writes)}")

    wait_body = function_body(source, "wait_fd_ready_until")
    require_tokens(
        wait_body,
        [
            "remaining_ms_until(deadline_ms, fallback_timeout_ms)",
            "poll(&pfd, 1, TIMEOUT_MS)",
            "errno == EINTR",
            "errno = ETIMEDOUT",
        ],
        "debugserver packet wait helper",
    )

    write_body = function_body(source, "write_all_timeout")
    require_tokens(
        write_body,
        [
            "set_nonblocking_for_timeout(fd, old_flags)",
            "wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms)",
            "write(fd, data.data() + done, data.size() - done)",
            "retryable_io_result(BYTES_WRITTEN)",
            "restore_fd_flags(fd, old_flags)",
        ],
        "debugserver packet write helper",
    )

    read_body = function_body(source, "read_exact_timeout")
    require_tokens(
        read_body,
        [
            "wait_fd_ready_until(fd, POLLIN, deadline_ms, timeout_ms)",
            "read(fd, out + done, len - done)",
            "retryable_io_result(BYTES_READ)",
            "errno = ECONNRESET",
        ],
        "debugserver packet read helper",
    )

    recv_body = function_body(source, "recv_packet")
    require_tokens(
        recv_body,
        [
            "int64_t const DEADLINE_MS = deadline_after_ms(DEBUGSERVER_PACKET_IO_TIMEOUT_MS)",
            "read_byte_timeout(session.fd, c, DEADLINE_MS, DEBUGSERVER_PACKET_IO_TIMEOUT_MS)",
            "read_exact_timeout(session.fd, csum.data(), csum.size(), DEADLINE_MS, DEBUGSERVER_PACKET_IO_TIMEOUT_MS)",
        ],
        "debugserver packet receive framing",
    )
    if "read(session.fd" in recv_body:
        fail("recv_packet must not use raw blocking read()")

    send_body = function_body(source, "send_packet")
    require_tokens(
        send_body,
        ["return write_all(session.fd, framed)"],
        "debugserver packet send wrapper",
    )

    loop_body = function_body(source, "continue_event_loop")
    require_tokens(loop_body, ["poll(&pfd, 1, DEBUGSERVER_EVENT_POLL_MS)"], "debugserver event-loop poll cadence")


def main() -> None:
    test_debugserver_launch_startup_wait_is_deadline_bounded()
    test_debugserver_packet_io_is_deadline_bounded()
    print("debugserver launch startup waits are deadline bounded")


if __name__ == "__main__":
    main()
