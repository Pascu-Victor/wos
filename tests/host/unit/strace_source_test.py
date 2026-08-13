#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
STRACE_SOURCE_DIR = ROOT / "modules" / "strace" / "src"


def read_strace_sources() -> str:
    return "\n".join(path.read_text() for path in sorted(STRACE_SOURCE_DIR.glob("*.cpp")))


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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        pos = source.find(token, cursor)
        if pos < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = pos + len(token)


def test_strace_startup_wait_is_deadline_bounded() -> None:
    source = read_strace_sources()
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


def test_strace_proxy_startup_cleanup_reaps_tracee() -> None:
    source = read_strace_sources()
    command_body = function_body(source, "trace_command")
    require_order(
        command_body,
        [
            "if (target_is_proxy(static_cast<uint64_t>(CHILD)))",
            'std::println(stderr, "strace: tracee became a WKI proxy before syscall tracing could start")',
            "reap_tracee_after_startup_failure(CHILD)",
            "return 1",
        ],
        "strace command proxy startup cleanup",
    )


def test_strace_tracee_is_pinned_local_before_startup_stop() -> None:
    source = read_strace_sources()
    launch_body = function_body(source, "launch_tracee")
    require_order(
        launch_body,
        [
            "ptrace_call(ker::abi::ptrace::request::TRACEME, 0, 0, 0)",
            "ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT)",
            "kill(getpid(), SIGSTOP)",
            "execvp(argv[0], argv)",
        ],
        "strace command tracee local pin before startup stop",
    )


def test_strace_trace_loop_uses_ptrace_syscall_wait() -> None:
    source = read_strace_sources()
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


def test_strace_structured_option_propagates_to_remote_helpers() -> None:
    source = read_strace_sources()
    require_tokens(
        source,
        [
            'ARG == "--structured"',
            "options.structured = true",
            'static std::array<char, sizeof("--structured")>',
            "if (options.structured)",
            "helper_argv.push_back(structured_arg())",
            "append_trace_options(helper_argv, options",
            "--wos-remote-command",
            "--wos-remote-attach",
        ],
        "strace structured remote option propagation",
    )


def test_strace_structured_events_are_bounded_typed_and_atomic() -> None:
    source = read_strace_sources()
    cmake = (ROOT / "modules" / "strace" / "CMakeLists.txt").read_text()
    require_tokens(
        source,
        [
            'make_envelope("strace", 1, "strace.syscall"',
            '"strace.signal"',
            '"strace.process.fork"',
            '"strace.process.exec"',
            '"strace.process.termination"',
            '"node_id"',
            '"observer.monotonic"',
            '"syscall_id"',
            '"syscall_sequence"',
            '"display_utf8"',
            '"display_bytes_hex"',
            "CANDIDATE + WIDTH > RAW_SIZE",
            "next_syscall_sequence++",
            "MAX_STRUCTURED_RECORD_BYTES = 4096",
            "write(FD, line.data(), line.size())",
            "written < 0 && errno == EINTR",
            "output.write_failed = true",
        ],
        "strace structured event and write contract",
    )
    require_tokens(
        function_body(source, "trace_loop"),
        [
            "emit_structured_syscall",
            "emit_structured_signal",
            "emit_structured_fork",
            "emit_structured_exec",
            "emit_structured_termination",
            "options.structured",
        ],
        "strace structured trace-loop seams",
    )
    require_tokens(
        function_body(source, "emit_deferred_syscall_without_exit_stop"),
        ["emit_structured_syscall(output, pid, tid, pending, std::nullopt, OBSERVED_AT, DISPLAY, false, true)"],
        "strace deferred syscall pairing metadata",
    )
    require_tokens(cmake, ["src/structured_output.cpp", "wos::telemetry"], "strace shared telemetry codec linkage")


def test_strace_names_process_priority_syscalls() -> None:
    source = read_strace_sources()
    require_tokens(
        source,
        [
            "case ker::abi::process::procmgmt_ops::SETPRIORITY:",
            'return "setpriority";',
            "case ker::abi::process::procmgmt_ops::GETPRIORITY:",
            'return "getpriority";',
        ],
        "strace process priority syscall names",
    )


def test_strace_names_spawn_syscall() -> None:
    source = read_strace_sources()
    require_tokens(
        source,
        [
            "case ker::abi::process::procmgmt_ops::SPAWN:",
            'return "spawn";',
            "proc_op == ker::abi::process::procmgmt_ops::SPAWN",
        ],
        "strace process spawn syscall name",
    )


def test_strace_names_init_control_syscalls() -> None:
    source = read_strace_sources()
    for operation, name in [
        ("INIT_CONTROL_SUBMIT", "init_control_submit"),
        ("INIT_CONTROL_RECEIVE", "init_control_receive"),
        ("INIT_STATUS_PUBLISH", "init_status_publish"),
        ("INIT_STATUS_READ", "init_status_read"),
    ]:
        require_tokens(
            source,
            [f"case ker::abi::process::procmgmt_ops::{operation}:", f'return "{name}";'],
            "strace init-control syscall names",
        )


def main() -> None:
    test_strace_startup_wait_is_deadline_bounded()
    test_strace_proxy_startup_cleanup_reaps_tracee()
    test_strace_tracee_is_pinned_local_before_startup_stop()
    test_strace_trace_loop_uses_ptrace_syscall_wait()
    test_strace_structured_option_propagates_to_remote_helpers()
    test_strace_structured_events_are_bounded_typed_and_atomic()
    test_strace_names_process_priority_syscalls()
    test_strace_names_spawn_syscall()
    test_strace_names_init_control_syscalls()
    print("strace startup waits are deadline bounded")


if __name__ == "__main__":
    main()
