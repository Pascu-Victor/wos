#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TESTPROG_MAIN_CPP = ROOT / "modules" / "testprog" / "src" / "main.cpp"
NETBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "netbench.cpp"
COWBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "cowbench.cpp"
USERLAND_SUITE = ROOT / "configs" / "drive" / "srv" / "wos_userland_suite.sh"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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
        fail(f"{context}: expected {first} before {second}")


def shell_function_body(source: str, name: str) -> str:
    lines = source.splitlines(keepends=True)
    start_line = None
    for index, line in enumerate(lines):
        if re.match(rf"^{name}\(\)\s*\{{\s*$", line):
            start_line = index
            break
    if start_line is None:
        fail(f"missing shell function {name}")

    depth = 1
    body: list[str] = []
    for line in lines[start_line + 1 :]:
        stripped = line.strip()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\(\)\s*\{\s*$", stripped):
            depth += 1
        if stripped == "}":
            depth -= 1
            if depth == 0:
                return "".join(body)
        body.append(line)
    if depth != 0:
        fail(f"unterminated shell function {name}")
    return "".join(body)


def test_ping_receive_is_deadline_bounded() -> None:
    source = TESTPROG_MAIN_CPP.read_text()
    require_tokens(source, ["#include <fcntl.h>", "#include <poll.h>", "#include <time.h>"], "testprog ping includes")

    ping_body = function_body(source, "ping")
    receive_body = function_body(source, "receive_ping_reply_timeout")

    if "recvfrom(" in ping_body:
        fail("ping must call the bounded receive helper instead of raw recvfrom")
    if "MAX_RETRIES" in source or "received != -11" in source:
        fail("ping must not use the old EAGAIN spin retry loop")

    require_tokens(
        ping_body,
        ["receive_ping_reply_timeout(SOCK, recv_buf, from_addr, PING_REPLY_TIMEOUT_MS)"],
        "ping bounded receive call",
    )
    require_tokens(
        receive_body,
        [
            "set_nonblocking_for_timeout(sock, old_flags)",
            "wait_fd_ready_until(sock, POLLIN, DEADLINE_MS, timeout_ms)",
            "recvfrom(",
            "retryable_socket_result(RECEIVED)",
            "restore_fd_flags(sock, old_flags)",
        ],
        "ping receive timeout helper",
    )


def test_netbench_io_is_deadline_bounded() -> None:
    source = NETBENCH_CPP.read_text()
    require_tokens(
        source,
        ["DEFAULT_NETBENCH_TIMEOUT_MS", "--timeout-ms", "parse_timeout_ms", "#include <fcntl.h>", "#include <poll.h>"],
        "netbench timeout surface",
    )
    if re.search(r"\brecv_all\s*\(", source) is not None:
        fail("netbench must not call an unbounded recv_all helper")
    if re.search(r"\bsend_all\s*\(", source) is not None:
        fail("netbench must not call an unbounded send_all helper")

    monotonic_now_body = function_body(source, "monotonic_now_ms")
    require_tokens(
        monotonic_now_body,
        [
            "ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC",
            "int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC",
            "if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC)",
            "return INT64_MAX",
        ],
        "netbench monotonic clock conversion",
    )

    deadline_after_body = function_body(source, "deadline_after_ms")
    require_tokens(
        deadline_after_body,
        [
            "if (timeout_ms <= 0)",
            "auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms)",
            "if (INT64_MAX - NOW_MS < TIMEOUT_MS)",
            "return INT64_MAX",
            "return NOW_MS + TIMEOUT_MS",
        ],
        "netbench deadline addition",
    )
    if "return NOW_MS + timeout_ms" in deadline_after_body:
        fail("netbench deadline_after_ms must not use wrapping timeout addition")

    remaining_body = function_body(source, "remaining_ms_until")
    require_tokens(
        remaining_body,
        [
            "if (deadline_ms <= NOW_MS)",
            "errno = ETIMEDOUT",
            "int64_t const REMAINING_MS = deadline_ms - NOW_MS",
            "REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS)",
        ],
        "netbench remaining timeout math",
    )
    if "int64_t const REMAINING_MS = deadline_ms - NOW_MS;\n    if (REMAINING_MS <= 0)" in remaining_body:
        fail("netbench remaining_ms_until must compare before subtracting")

    for name, event in [
        ("recv_all_timeout", "POLLIN"),
        ("send_all_timeout", "POLLOUT"),
        ("accept_timeout", "POLLIN"),
        ("connect_timeout", "POLLOUT"),
    ]:
        body = function_body(source, name)
        require_tokens(
            body,
            [
                "set_nonblocking_for_timeout(fd, old_flags)",
                f"wait_fd_ready_until(fd, {event}, DEADLINE_MS, timeout_ms)",
                "restore_fd_flags(fd, old_flags)",
            ],
            f"{name} deadline helper",
        )

    server_body = function_body(source, "run_server")
    require_tokens(
        server_body,
        [
            "parse_timeout_ms(argv[++i], &options.timeout_ms)",
            "accept_timeout(SERVER_FD, options.timeout_ms)",
            "recv_all_timeout(CLIENT_FD, &header, sizeof(header), options.timeout_ms)",
            "handle_pingpong_server(CLIENT_FD, header, options.timeout_ms)",
            "handle_stream_server(CLIENT_FD, header, options.timeout_ms)",
        ],
        "netbench server timeout plumbing",
    )

    client_body = function_body(source, "run_client")
    require_tokens(
        client_body,
        [
            "parse_timeout_ms(argv[++i], &options.timeout_ms)",
            "connect_to_host(options.host, options.port, options.timeout_ms)",
            "send_all_timeout(FD, &header, sizeof(header), options.timeout_ms)",
            "recv_all_timeout(FD, &ack, sizeof(ack), options.timeout_ms)",
        ],
        "netbench client timeout plumbing",
    )


def test_userland_suite_passes_netbench_timeout() -> None:
    source = USERLAND_SUITE.read_text()
    require_tokens(source, ["NETBENCH_TIMEOUT_MS=\"${WOS_SUITE_NETBENCH_TIMEOUT_MS:-30000}\""], "suite netbench timeout default")
    if source.count("--timeout-ms \"$NETBENCH_TIMEOUT_MS\"") < 3:
        fail("wos-userland-suite must pass --timeout-ms to the netbench server and both client modes")


def test_userland_suite_cases_are_watchdog_bounded() -> None:
    source = USERLAND_SUITE.read_text()
    run_case = shell_function_body(source, "run_case")
    require_tokens(
        source,
        [
            "DEFAULT_CASE_TIMEOUT_SECONDS=900",
            "CASE_TIMEOUT_SECONDS=\"${WOS_SUITE_CASE_TIMEOUT_SECONDS:-$DEFAULT_CASE_TIMEOUT_SECONDS}\"",
            "TIMEOUT_KILL_GRACE_SECONDS=\"${WOS_SUITE_TIMEOUT_KILL_GRACE_SECONDS:-5}\"",
        ],
        "suite watchdog timeout configuration",
    )
    require_tokens(
        run_case,
        [
            "timeout_marker=\"$WORK_DIR/$name.timeout\"",
            "rm -f \"$timeout_marker\"",
            "if [ \"$CASE_TIMEOUT_SECONDS\" -gt 0 ]",
            "\"$@\" > \"$log\" 2>&1 &",
            "case_pid=\"$!\"",
            "cleanup_watchdog()",
            "trap cleanup_watchdog TERM INT HUP",
            "sleep \"$CASE_TIMEOUT_SECONDS\" &",
            "kill -TERM \"-$case_pid\"",
            "kill \"$case_pid\"",
            "printf 'timeout after %ss\\n' \"$CASE_TIMEOUT_SECONDS\" > \"$timeout_marker\"",
            "sleep \"$TIMEOUT_KILL_GRACE_SECONDS\" &",
            "kill -KILL \"-$case_pid\"",
            "kill -KILL \"$case_pid\"",
            "watchdog_pid=\"$!\"",
            "wait \"$case_pid\"",
            "kill -TERM \"-$watchdog_pid\"",
            "kill \"$watchdog_pid\"",
            "wait \"$watchdog_pid\" 2>/dev/null || true",
            "timed_out=1",
            "rc=124",
            "printf '\\nTIMEOUT %s after %ss\\n' \"$name\" \"$CASE_TIMEOUT_SECONDS\" >> \"$log\"",
            "record_summary \"$name\" \"FAIL\" \"timeout=${CASE_TIMEOUT_SECONDS}s ${duration}s\"",
        ],
        "suite run_case watchdog path",
    )
    require_order(run_case, "sleep \"$CASE_TIMEOUT_SECONDS\" &", "kill -TERM \"-$case_pid\"", "suite timeout TERM order")
    require_order(run_case, "kill -TERM \"-$case_pid\"", "printf 'timeout after %ss\\n'", "suite timeout marker order")
    require_order(run_case, "printf 'timeout after %ss\\n'", "sleep \"$TIMEOUT_KILL_GRACE_SECONDS\" &", "suite timeout grace order")
    require_order(run_case, "sleep \"$TIMEOUT_KILL_GRACE_SECONDS\" &", "kill -KILL \"-$case_pid\"", "suite timeout KILL order")
    require_order(run_case, "wait \"$case_pid\"", "kill -TERM \"-$watchdog_pid\"", "suite watchdog cleanup order")
    require_order(run_case, "if [ -f \"$timeout_marker\" ]", "rc=124", "suite timeout rc order")
    require_order(run_case, "elif [ \"$timed_out\" -eq 1 ]", "record_summary \"$name\" \"FAIL\"", "suite timeout accounting order")


def test_cowbench_child_wait_is_deadline_bounded() -> None:
    source = COWBENCH_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <signal.h>",
            "DEFAULT_CHILD_TIMEOUT_MS",
            "CHILD_WAIT_POLL_US",
            "--child-timeout-ms",
            "clock_gettime(CLOCK_MONOTONIC, &ts)",
        ],
        "cowbench timeout surface",
    )
    if "waitpid(pid, &status, 0)" in source:
        fail("fork-cow must not use a blocking waitpid for child completion")

    parse_body = function_body(source, "parse_options")
    require_tokens(
        parse_body,
        [
            'std::strcmp(argv[i], "--child-timeout-ms")',
            "parse_u32(argv[++i], options.child_timeout_ms, false)",
        ],
        "cowbench timeout option parsing",
    )

    wait_body = function_body(source, "wait_for_child")
    require_tokens(
        wait_body,
        [
            "monotonic_ns() - START_NS <= TIMEOUT_NS",
            "waitpid(pid, &status, WNOHANG)",
            "usleep(CHILD_WAIT_POLL_US)",
            "reap_child_after_timeout(pid)",
        ],
        "cowbench wait_for_child timeout",
    )

    reap_body = function_body(source, "reap_child_after_timeout")
    require_tokens(
        reap_body,
        [
            "kill(pid, SIGKILL)",
            "waitpid(pid, &reap_status, WNOHANG)",
            "usleep(CHILD_WAIT_POLL_US)",
        ],
        "cowbench timeout reap",
    )

    stress_body = function_body(source, "run_stress")
    require_tokens(stress_body, ["wait_for_child(PID, options.child_timeout_ms)"], "cowbench stress wait timeout plumbing")


def main() -> None:
    test_ping_receive_is_deadline_bounded()
    test_netbench_io_is_deadline_bounded()
    test_userland_suite_passes_netbench_timeout()
    test_userland_suite_cases_are_watchdog_bounded()
    test_cowbench_child_wait_is_deadline_bounded()
    print("testprog ping, netbench socket waits, userland suite cases, and cowbench child waits are deadline bounded")


if __name__ == "__main__":
    main()
