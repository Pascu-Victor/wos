#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TESTD_MAIN = ROOT / "modules" / "testd" / "src" / "main.cpp"
PASS_LABEL_RE = re.compile(r'\bTESTD_(?:PASS|CHECK)\(\s*"((?:[^"\\]|\\.)*)"')
TEST_BODY_RE = re.compile(
    r"^TESTD_RUN\((?P<name>\w+)\)\s*\{(?P<body>.*?)^TESTD_RUN_END\((?P=name)\)",
    flags=re.MULTILINE | re.DOTALL,
)


def fail(message: str) -> None:
    raise AssertionError(message)


def strip_cxx_comments(source: str) -> str:
    out: list[str] = []
    index = 0
    in_string = False
    in_char = False
    escaped = False
    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""

        if in_string or in_char:
            out.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif in_string and char == '"':
                in_string = False
            elif in_char and char == "'":
                in_char = False
            index += 1
            continue

        if char == '"':
            in_string = True
            out.append(char)
            index += 1
            continue
        if char == "'":
            in_char = True
            out.append(char)
            index += 1
            continue
        if char == "/" and next_char == "/":
            index += 2
            while index < len(source) and source[index] != "\n":
                index += 1
            continue
        if char == "/" and next_char == "*":
            index += 2
            while index < len(source):
                if source[index] == "\n":
                    out.append("\n")
                    index += 1
                    continue
                if source[index] == "*" and index + 1 < len(source) and source[index + 1] == "/":
                    index += 2
                    break
                index += 1
            continue

        out.append(char)
        index += 1
    return "".join(out)


def mask_cxx_literals_preserve_offsets(source: str) -> str:
    out: list[str] = []
    index = 0
    in_string = False
    in_char = False
    escaped = False

    def push_masked(char: str) -> None:
        out.append("\n" if char == "\n" else " ")

    def is_digit_separator_quote(offset: int) -> bool:
        if offset == 0 or offset + 1 >= len(source):
            return False
        previous = source[offset - 1]
        following = source[offset + 1]
        if not (previous.isalnum() or previous == "_"):
            return False
        if not (following.isalnum() or following == "_"):
            return False

        token_start = offset - 1
        while token_start >= 0 and (source[token_start].isalnum() or source[token_start] == "_"):
            token_start -= 1
        token_before_quote = source[token_start + 1 : offset]
        if token_before_quote in {"L", "U", "u", "u8"}:
            return False
        return any(char.isdigit() for char in token_before_quote)

    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""

        if in_string or in_char:
            push_masked(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif in_string and char == '"':
                in_string = False
            elif in_char and char == "'":
                in_char = False
            index += 1
            continue

        if char == "R" and next_char == '"':
            out.extend("  ")
            index += 2
            delimiter_start = index
            while index < len(source) and source[index] != "(":
                push_masked(source[index])
                index += 1
            delimiter = source[delimiter_start:index]
            if index >= len(source):
                break
            push_masked(source[index])
            index += 1
            terminator = ")" + delimiter + '"'
            while index < len(source):
                if source.startswith(terminator, index):
                    out.extend(" " for _ in terminator)
                    index += len(terminator)
                    break
                push_masked(source[index])
                index += 1
            continue
        if char == '"':
            in_string = True
            push_masked(char)
            index += 1
            continue
        if char == "'" and not is_digit_separator_quote(index):
            in_char = True
            push_masked(char)
            index += 1
            continue

        out.append(char)
        index += 1

    return "".join(out)


def macro_body(source: str, macro_name: str) -> str:
    lines = source.splitlines()
    for index, line in enumerate(lines):
        if line.startswith(f"#define {macro_name}("):
            body_lines: list[str] = []
            current = line
            while True:
                continued = current.rstrip().endswith("\\")
                body_lines.append(current.rstrip().removesuffix("\\").strip())
                if not continued:
                    break
                index += 1
                if index >= len(lines):
                    fail(f"{macro_name} macro is unterminated")
                current = lines[index]
            return "\n".join(body_lines)
    fail(f"{macro_name} macro not found")


def parse_test_registry(source: str) -> tuple[list[str], list[str], list[str]]:
    test_runs = re.findall(r"^TESTD_RUN\((\w+)\)", source, flags=re.MULTILINE)
    test_ends = re.findall(r"^TESTD_RUN_END\((\w+)\)", source, flags=re.MULTILINE)
    body = macro_body(source, "TESTD_TESTS")
    listed = re.findall(r"\bX\((\w+)\)", body)
    return test_runs, test_ends, listed


def parse_test_bodies(source: str) -> dict[str, str]:
    bodies: dict[str, str] = {}
    for match in TEST_BODY_RE.finditer(source):
        name = match.group("name")
        if name in bodies:
            fail(f"duplicate TESTD test body: {name}")
        bodies[name] = match.group("body")
    return bodies


def parse_pass_labels_by_test(source: str) -> dict[str, list[str]]:
    return {name: PASS_LABEL_RE.findall(body) for name, body in parse_test_bodies(source).items()}


def require_unique(items: list[str], name: str) -> None:
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
            continue
        seen.add(item)
    if duplicates:
        fail(f"duplicate {name}: {', '.join(sorted(duplicates))}")


def require_ordered_equal(left: list[str], right: list[str], left_name: str, right_name: str) -> None:
    require_unique(left, left_name)
    require_unique(right, right_name)
    require_sets_equal(set(left), set(right), left_name, right_name)
    if left == right:
        return

    for index, (left_item, right_item) in enumerate(zip(left, right)):
        if left_item != right_item:
            fail(f"{left_name}/{right_name} order mismatch at index {index}: {left_item} != {right_item}")
    fail(f"{left_name}/{right_name} order length mismatch: {len(left)} != {len(right)}")


def parse_remote_modes(source: str) -> tuple[set[str], set[str]]:
    requested = set(re.findall(r"spawn_remote_(?:helper(?:_arg)?|wait_helper)\(\"([^\"]+)\"", source))
    requested.update(re.findall(r"mode_buf\s*=\s*std::to_array\(\"([^\"]+)\"\)", source))
    handled = set(re.findall(r"std::strcmp\(mode,\s*\"([^\"]+)\"\)\s*==\s*0", source))
    return requested, handled


def function_body(source: str, name: str) -> str:
    start_candidates = [
        source.find(f"auto {name}("),
        source.find(f"void {name}("),
    ]
    start = min((candidate for candidate in start_candidates if candidate >= 0), default=-1)
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")

    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1:index]
    fail(f"{name} function body is unterminated")


def braced_block_after(source: str, token: str) -> str:
    start = source.find(token)
    if start < 0:
        fail(f"block token not found: {token}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"block token has no body: {token}")

    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1:index]
    fail(f"block token body is unterminated: {token}")


def remote_helper_mode_body(source: str, mode: str) -> str:
    return braced_block_after(source, f'std::strcmp(mode, "{mode}") == 0')


def require_deadline_safe_wait_helpers(source: str) -> None:
    monotonic_now_body = function_body(source, "monotonic_now_ms")
    for snippet in [
        "ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC",
        "int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC",
        "if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC)",
        "return INT64_MAX",
    ]:
        if snippet not in monotonic_now_body:
            fail(f"monotonic_now_ms is missing saturating conversion snippet: {snippet}")

    deadline_after_body = function_body(source, "deadline_after_ms")
    for snippet in [
        "if (timeout_ms <= 0)",
        "auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms)",
        "if (INT64_MAX - NOW_MS < TIMEOUT_MS)",
        "return INT64_MAX",
        "return NOW_MS + TIMEOUT_MS",
    ]:
        if snippet not in deadline_after_body:
            fail(f"deadline_after_ms is missing saturating addition snippet: {snippet}")
    if "return NOW_MS + timeout_ms" in deadline_after_body:
        fail("deadline_after_ms must not use wrapping timeout addition")

    remaining_body = function_body(source, "remaining_ms_until")
    for snippet in [
        "if (deadline_ms <= NOW_MS)",
        "errno = ETIMEDOUT",
        "int64_t const REMAINING_MS = deadline_ms - NOW_MS",
        "REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS)",
    ]:
        if snippet not in remaining_body:
            fail(f"remaining_ms_until is missing overflow-safe remaining-time snippet: {snippet}")
    if "int64_t const REMAINING_MS = deadline_ms - NOW_MS;\n    if (REMAINING_MS <= 0)" in remaining_body:
        fail("remaining_ms_until must compare before subtracting to avoid signed overflow")

    wait_fd_ready_body = function_body(source, "wait_fd_ready")
    if "poll(&pfd, 1, timeout_ms)" not in wait_fd_ready_body:
        fail("wait_fd_ready no longer performs the raw bounded poll expected by timeout helpers")
    if "errno == EINTR" in wait_fd_ready_body or "continue;" in wait_fd_ready_body:
        fail("wait_fd_ready must not retry EINTR with the full timeout; wait_fd_ready_until owns deadline recomputation")

    wait_fd_ready_until_body = function_body(source, "wait_fd_ready_until")
    required_snippets = [
        "if (deadline_ms < 0)",
        "return wait_fd_ready(fd, events, fallback_timeout_ms)",
        "remaining_ms_until(deadline_ms, fallback_timeout_ms)",
        "wait_fd_ready(fd, events, TIMEOUT_MS)",
        "READY < 0 && errno == EINTR",
        "continue;",
    ]
    for snippet in required_snippets:
        if snippet not in wait_fd_ready_until_body:
            fail(f"wait_fd_ready_until is missing deadline-aware EINTR handling snippet: {snippet}")


def require_tcp_loopback_is_deadline_bounded(source: str) -> None:
    send_all_body = function_body(source, "send_all_timeout")
    for snippet in [
        "set_nonblocking_for_timeout(fd, old_flags)",
        "wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms)",
        "restore_fd_flags(fd, old_flags)",
    ]:
        if snippet not in send_all_body:
            fail(f"send_all_timeout is missing bounded send snippet: {snippet}")

    accept_body = function_body(source, "accept_timeout")
    for snippet in [
        "set_nonblocking_for_timeout(fd, old_flags)",
        "wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms)",
        "accept(fd, addr, addrlen)",
        "restore_fd_flags(fd, old_flags)",
    ]:
        if snippet not in accept_body:
            fail(f"accept_timeout is missing bounded accept snippet: {snippet}")

    connect_body = function_body(source, "connect_timeout")
    for snippet in [
        "set_nonblocking_for_timeout(fd, old_flags)",
        "connect(fd, addr, addrlen)",
        "wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms)",
        "errno == EISCONN",
        "restore_fd_flags(fd, old_flags)",
    ]:
        if snippet not in connect_body:
            fail(f"connect_timeout is missing bounded connect snippet: {snippet}")

    tcp_server_body = function_body(source, "tcp_echo_server")
    for snippet in [
        "accept_timeout(SRV, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS)",
        "recv_once_timeout(CLI, buf.data(), buf.size(), 0, REMOTE_IPC_TIMEOUT_MS)",
        "send_all_timeout(CLI, buf.data(), static_cast<size_t>(n), 0, REMOTE_IPC_TIMEOUT_MS)",
    ]:
        if snippet not in tcp_server_body:
            fail(f"tcp_echo_server is missing bounded I/O snippet: {snippet}")

    tcp_test_body = parse_test_bodies(source)["test_tcp_loopback"]
    for snippet in [
        "read_expected_bytes_timeout(ready_pipe[0], &byte, 1, REMOTE_IPC_TIMEOUT_MS)",
        "waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS)",
        "connect_timeout(CLI, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS)",
        "send_all_timeout(CLI, send_buf.data(), DATA_SIZE, 0, REMOTE_IPC_TIMEOUT_MS)",
        "recv_expected_bytes_timeout(CLI, recv_buf.data(), DATA_SIZE, REMOTE_IPC_TIMEOUT_MS)",
        "waitpid_timeout(SRV_PID, &srv_status, REMOTE_IPC_TIMEOUT_MS)",
    ]:
        if snippet not in tcp_test_body:
            fail(f"test_tcp_loopback is missing bounded operation snippet: {snippet}")


def require_socket_setup_tests_are_deadline_bounded(source: str) -> None:
    bodies = parse_test_bodies(source)

    refused_body = bodies["test_tcp_nonblocking_connect_refused"]
    refused_snippet = "connect_timeout(FD, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS)"
    if refused_snippet not in refused_body:
        fail(f"test_tcp_nonblocking_connect_refused is missing bounded connect snippet: {refused_snippet}")
    if "connect(FD, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr))" in refused_body:
        fail("test_tcp_nonblocking_connect_refused still uses raw connect")

    remote_socket_requirements = {
        "test_remote_ipc_socket_child_write": [
            "connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS)",
            "accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS)",
        ],
        "test_remote_ipc_socket_control_ops": [
            "connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS)",
            "accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS)",
        ],
    }
    for test_name, snippets in remote_socket_requirements.items():
        body = bodies[test_name]
        for snippet in snippets:
            if snippet not in body:
                fail(f"{test_name} is missing bounded socket setup snippet: {snippet}")
        if "connect(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr))" in body:
            fail(f"{test_name} still uses raw connect")
        if "accept(LISTEN_FD, nullptr, nullptr)" in body:
            fail(f"{test_name} still uses raw accept")


def require_remote_ipc_wait_handshake(source: str) -> None:
    spawn_body = function_body(source, "spawn_remote_wait_helper")
    for snippet in [
        "std::array<char, 16> ready_fd_str{}",
        "testd_format_to_array(ready_fd_str, \"%d\", ready_fd)",
        "close(close_ready_fd)",
        "ready_fd_str.data()",
    ]:
        if snippet not in spawn_body:
            fail(f"spawn_remote_wait_helper is missing ready-fd snippet: {snippet}")

    wait_ready_body = function_body(source, "wait_remote_waiter_ready")
    for snippet in [
        "read_expected_bytes_timeout(ready_fd, &byte, 1, REMOTE_IPC_TIMEOUT_MS)",
        "byte == RH_WAIT_READY_BYTE",
    ]:
        if snippet not in wait_ready_body:
            fail(f"wait_remote_waiter_ready is missing bounded ready read snippet: {snippet}")

    signal_ready_body = function_body(source, "signal_remote_wait_ready")
    for snippet in [
        "char const BYTE = RH_WAIT_READY_BYTE",
        "write(ready_fd, &BYTE, 1)",
        "close(ready_fd)",
    ]:
        if snippet not in signal_ready_body:
            fail(f"signal_remote_wait_ready is missing ready signal snippet: {snippet}")

    handshaked_tests = {
        "test_remote_ipc_poll_wait_pipe_readable": "poll-wait",
        "test_remote_ipc_poll_wait_pipe_hup": "poll-wait-hup",
        "test_remote_ipc_poll_pipe_read_then_hup": "poll-drain-hup",
        "test_remote_ipc_epoll_wait_pipe_readable": "epoll-wait",
        "test_remote_ipc_epoll_wait_pipe_hup": "epoll-wait-hup",
        "test_remote_ipc_epoll_pipe_read_then_hup": "epoll-drain-hup",
    }
    bodies = parse_test_bodies(source)
    for test_name, mode in handshaked_tests.items():
        body = bodies[test_name]
        if "usleep(200000)" in body:
            fail(f"{test_name} still uses a fixed sleep instead of a ready-pipe handshake")
        for snippet in [
            "std::array<int, 2> ready_pipe",
            f'spawn_remote_wait_helper("{mode}"',
            "wait_remote_waiter_ready(ready_pipe[0])",
        ]:
            if snippet not in body:
                fail(f"{test_name} is missing ready-pipe handshake snippet: {snippet}")

    preclosed_body = bodies["test_remote_ipc_poll_pipe_preclosed_hup"]
    if "spawn_remote_wait_helper" in preclosed_body or "ready_pipe" in preclosed_body:
        fail("preclosed poll HUP test must stay intentionally un-handshaked")
    if 'std::to_array("poll-preclosed-hup")' not in preclosed_body:
        fail("preclosed poll HUP test must use the dedicated preclosed helper mode")

    epoll_preclosed_body = bodies["test_remote_ipc_epoll_pipe_preclosed_hup"]
    if "spawn_remote_wait_helper" in epoll_preclosed_body or "ready_pipe" in epoll_preclosed_body:
        fail("preclosed epoll HUP test must stay intentionally un-handshaked")
    if 'std::to_array("epoll-preclosed-hup")' not in epoll_preclosed_body:
        fail("preclosed epoll HUP test must use the dedicated preclosed helper mode")

    for mode in ["poll-wait", "poll-wait-hup", "poll-drain-hup"]:
        body = remote_helper_mode_body(source, mode)
        for snippet in [
            "int const READY_FD = parse_int_arg(argv[4])",
            "poll(&preflight, 1, 0)",
            "signal_remote_wait_ready(READY_FD)",
        ]:
            if snippet not in body:
                fail(f"{mode} helper is missing poll ready-handshake snippet: {snippet}")

    for mode in ["epoll-wait", "epoll-wait-hup", "epoll-drain-hup"]:
        body = remote_helper_mode_body(source, mode)
        for snippet in [
            "int const READY_FD = parse_int_arg(argv[4])",
            "epoll_wait(EPFD, &preflight, 1, 0)",
            "signal_remote_wait_ready(READY_FD)",
        ]:
            if snippet not in body:
                fail(f"{mode} helper is missing epoll ready-handshake snippet: {snippet}")

    epoll_preclosed_helper = remote_helper_mode_body(source, "epoll-preclosed-hup")
    for snippet in [
        "int const EPFD = epoll_create1(0)",
        "epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev)",
        "int const RC = epoll_wait(EPFD, &out, 1, 1000)",
        "(out.events & EPOLLHUP) == 0",
        "ssize_t const NR = read(FD, &byte, sizeof(byte))",
        "return (NR == 0) ? 0 : 1",
    ]:
        if snippet not in epoll_preclosed_helper:
            fail(f"epoll-preclosed-hup helper is missing bounded HUP/EOF snippet: {snippet}")


def require_remote_ipc_epoll_ctl_add_is_bounded(source: str) -> None:
    body = parse_test_bodies(source)["test_remote_ipc_epoll_ctl_add"]
    for snippet in [
        "waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)",
        "int const READY = epoll_wait(EPFD, &ev, 1, 1000)",
    ]:
        if snippet not in body:
            fail(f"test_remote_ipc_epoll_ctl_add is missing bounded wait snippet: {snippet}")

    forbidden = [
        "waitpid(PID, &status, 0)",
        "waitpid(PID, nullptr, 0)",
        "epoll_wait(EPFD, &ev, 1, -1)",
    ]
    for snippet in forbidden:
        if snippet in body:
            fail(f"test_remote_ipc_epoll_ctl_add still uses an unbounded wait: {snippet}")


def require_non_waitpid_wake_tests_bound_child_waits(source: str) -> None:
    bodies = parse_test_bodies(source)
    bounded_tests = [
        "test_pipe_blocking_read_wake",
        "test_pipe_lost_wake_race_many",
        "test_poll_pipe_timeout_and_wake",
        "test_poll_pipe_hup_on_writer_close",
        "test_epoll_pipe_timeout_and_wake",
        "test_epoll_pipe_hup_on_writer_close",
        "test_pty_blocking_read_wake",
        "test_fork_pipe_byte",
        "test_fork_pipe_communication",
    ]
    for test_name in bounded_tests:
        body = bodies[test_name]
        if "waitpid(PID, &status, 0)" in body:
            fail(f"{test_name} still uses an unbounded child wait")
        if "waitpid(PID, nullptr, 0)" in body:
            fail(f"{test_name} still uses an unbounded child wait without status")
        if "waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)" not in body:
            fail(f"{test_name} is missing bounded child wait cleanup")


def require_process_waitpid_tests_are_deadline_bounded(source: str) -> None:
    wait_any_body = function_body(source, "waitpid_any_timeout")
    for snippet in [
        "waitpid(-1, status, WNOHANG)",
        "RET > 0",
        "errno = ETIMEDOUT",
        "return -1",
    ]:
        if snippet not in wait_any_body:
            fail(f"waitpid_any_timeout is missing bounded wait-any snippet: {snippet}")

    bodies = parse_test_bodies(source)
    specific_wait_tests = {
        "test_fork_exit": "waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)",
        "test_waitpid_exit_before_park_race": "waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)",
        "test_fork_multiple": "waitpid_timeout(pids[i], &status, REMOTE_IPC_TIMEOUT_MS)",
    }
    any_wait_tests = {
        "test_waitpid_any_exit_before_park_race": "waitpid_any_timeout(&status, REMOTE_IPC_TIMEOUT_MS)",
        "test_waitpid_any_multi_child_drain": "waitpid_any_timeout(&status, REMOTE_IPC_TIMEOUT_MS)",
    }
    for test_name, snippet in specific_wait_tests.items():
        body = bodies[test_name]
        if snippet not in body:
            fail(f"{test_name} must use bounded specific-child wait: {snippet}")
    for test_name, snippet in any_wait_tests.items():
        body = bodies[test_name]
        if snippet not in body:
            fail(f"{test_name} must use bounded wait-any helper: {snippet}")

    forbidden = [
        "waitpid(PID, &status, 0)",
        "waitpid(pids[i], &status, 0)",
        "waitpid(-1, &status, 0)",
    ]
    for test_name in [*specific_wait_tests.keys(), *any_wait_tests.keys()]:
        body = bodies[test_name]
        for snippet in forbidden:
            if snippet in body:
                fail(f"{test_name} still uses unbounded waitpid: {snippet}")


def require_non_waitpid_wake_tests_bound_parent_reads(source: str) -> None:
    bodies = parse_test_bodies(source)
    basic_reads = {
        "test_vfs_dup": "read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS)",
        "test_vfs_dup2": "read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS)",
        "test_pipe_basic": "read_expected_bytes_timeout(fds[0], rbuf.data(), MSG.size(), REMOTE_IPC_TIMEOUT_MS)",
        "test_pipe_eof_on_writer_close": "read_expected_bytes_timeout(fds[0], buf.data(), 1, REMOTE_IPC_TIMEOUT_MS)",
    }
    for test_name, snippet in basic_reads.items():
        body = bodies[test_name]
        if snippet not in body:
            fail(f"{test_name} is missing bounded pipe read snippet: {snippet}")
        if "read(fds[0], buf.data(), buf.size())" in body or "read(fds[0], rbuf.data(), rbuf.size())" in body:
            fail(f"{test_name} still has a raw pipe read")

    blocking_body = bodies["test_pipe_blocking_read_wake"]
    for snippet in [
        "ssize_t const NR = read(fds[0], &got, 1)",
        "_exit((NR == 1 && got == BYTE) ? 0 : 1)",
        "ssize_t const NW = write(fds[1], &BYTE, 1)",
        "waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)",
    ]:
        if snippet not in blocking_body:
            fail(f"test_pipe_blocking_read_wake is missing killable blocking-read snippet: {snippet}")

    bounded_reads = {
        "test_pipe_lost_wake_race_many": "read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_poll_pipe_timeout_and_wake": "read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_poll_pipe_hup_on_writer_close": "read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_epoll_pipe_timeout_and_wake": "read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_epoll_pipe_hup_on_writer_close": "read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_fork_pipe_byte": "read_expected_bytes_timeout(fds[0], &b, 1, REMOTE_IPC_TIMEOUT_MS)",
        "test_fork_pipe_communication": "read_expected_bytes_timeout(fds[0], buf.data(), MSG.size(), REMOTE_IPC_TIMEOUT_MS)",
    }
    forbidden_reads = [
        "read(fds[0], &got, 1)",
        "read(fds[0], &b, 1)",
        "read(fds[0], buf.data(), buf.size())",
    ]
    for test_name, snippet in bounded_reads.items():
        body = bodies[test_name]
        if snippet not in body:
            fail(f"{test_name} is missing bounded parent read snippet: {snippet}")
        for forbidden in forbidden_reads:
            if forbidden in body:
                fail(f"{test_name} still has a raw parent pipe read: {forbidden}")


def require_wki_policy_syscall_tests(source: str) -> None:
    bodies = parse_test_bodies(source)
    target_body = bodies["test_wki_target_policy_syscalls"]
    target_snippets = [
        "ker::process::setwkitarget(nullptr, 0, 0)",
        "ker::process::getwkitarget(hostname.data(), hostname.size(), &flags)",
        "ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT",
        "ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_STRICT",
        "rc != -ENAMETOOLONG",
        "ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_REMOTE",
        "rc != -EINVAL",
        "wki_target_rejects_hostname_local",
        "wki_target_rejects_oversized_hostname",
    ]
    for snippet in target_snippets:
        if snippet not in target_body:
            fail(f"test_wki_target_policy_syscalls is missing snippet: {snippet}")

    vfs_body = bodies["test_wki_vfs_rule_syscalls"]
    vfs_snippets = [
        "ker::abi::vfs::wki_rule_clear_vfs()",
        "ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route)",
        "rc != -ENOENT",
        "ker::abi::vfs::wki_rule_add_vfs(\"/tmp/testd-wki\", WKI_VFS_ROUTE_HOST)",
        "route != WKI_VFS_ROUTE_HOST",
        "ker::abi::vfs::wki_rule_add_vfs(\"/tmp/testd-wki\", WKI_VFS_ROUTE_LOCAL)",
        "route != WKI_VFS_ROUTE_LOCAL",
        "wki_rule_get_vfs(0, small_prefix.data(), small_prefix.size(), nullptr)",
        "rc != -ERANGE",
        "wki_rule_add_vfs(\"/tmp/testd-wki-bad\", 42)",
        "rc != -EINVAL",
        "wki_vfs_clear_final",
    ]
    for snippet in vfs_snippets:
        if snippet not in vfs_body:
            fail(f"test_wki_vfs_rule_syscalls is missing snippet: {snippet}")


def require_journal_device_test(source: str) -> None:
    bodies = parse_test_bodies(source)
    body = bodies["test_journal_device_userspace_record"]
    snippets = [
        'open("/dev/journal", O_RDWR)',
        'testd_format_to_array(token, "testd-journal-%d", getpid())',
        "write(FD, token.data(), TOKEN_LEN)",
        "std::array<ker::abi::sys_log::JournalRecord, JOURNAL_SCAN_BATCH>",
        "read(FD, records.data(), records.size() * sizeof(ker::abi::sys_log::JournalRecord))",
        "N % static_cast<ssize_t>(sizeof(ker::abi::sys_log::JournalRecord))",
        'constexpr std::string_view USERSPACE_MODULE = "userspace"',
        "rec.module[pos] == USERSPACE_MODULE.at(pos)",
        "rec.module[USERSPACE_MODULE.size()] == '\\0'",
        "message_matches = rec.message_len == TOKEN_LEN",
        "rec.message[pos] == token.at(pos)",
        "rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC",
        "rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION",
        "rec.header_size != sizeof(ker::abi::sys_log::JournalRecord) - ker::abi::sys_log::JOURNAL_MESSAGE_MAX",
        "rec.level != static_cast<uint8_t>(ker::abi::sys_log::sys_log_level::INFO)",
        'TESTD_PASS("journal_device_write")',
        'TESTD_PASS("journal_device_userspace_record")',
    ]
    for snippet in snippets:
        if snippet not in body:
            fail(f"test_journal_device_userspace_record is missing snippet: {snippet}")


def require_compiled_manifest_rejects_empty_tests(source: str) -> None:
    k_tests_body = braced_block_after(source, "constexpr auto K_TESTS = std::array")
    for snippet in [
        "static_assert(fn##_pass_count > 0",
        "TESTD tests must execute at least one TESTD_PASS or TESTD_CHECK path",
        "return TestSpec{fn, fn##_pass_count}",
    ]:
        if snippet not in k_tests_body:
            fail(f"compiled TESTD registry is missing zero-check guard snippet: {snippet}")


RAW_IO_TEST_ALLOWLIST = {
    "test_epoll_pipe_timeout_and_wake",
    "test_file_write_read",
    "test_fork_pipe_byte",
    "test_fork_pipe_communication",
    "test_journal_device_userspace_record",
    "test_mmap_file",
    "test_pipe_basic",
    "test_pipe_blocking_read_wake",
    "test_pipe_lost_wake_race_many",
    "test_poll_pipe_timeout_and_wake",
    "test_pty_blocking_read_wake",
    "test_remote_ipc_epoll_ctl_add",
    "test_remote_ipc_epoll_pipe_read_then_hup",
    "test_remote_ipc_epoll_wait_pipe_readable",
    "test_remote_ipc_pipe_parent_write",
    "test_remote_ipc_poll_pipe_read_then_hup",
    "test_remote_ipc_poll_wait_pipe_readable",
    "test_truncate",
    "test_vfs_dup",
    "test_vfs_dup2",
    "test_vfs_lseek",
    "test_vfs_open_write_read_close",
    "test_vfs_stat",
    "test_vfs_unlink_rename",
}


def split_call_args(args: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    for index, char in enumerate(args):
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(args[start:index].strip())
            start = index + 1
    parts.append(args[start:].strip())
    return parts


def find_calls(source: str, names: set[str]) -> list[tuple[str, str]]:
    pattern = re.compile(r"\b(" + "|".join(sorted(names)) + r")\s*\(")
    calls: list[tuple[str, str]] = []
    for match in pattern.finditer(source):
        depth = 1
        pos = match.end()
        while pos < len(source) and depth > 0:
            if source[pos] == "(":
                depth += 1
            elif source[pos] == ")":
                depth -= 1
            pos += 1
        if depth == 0:
            calls.append((match.group(1), source[match.end() : pos - 1]))
    return calls


def require_testd_run_raw_blocking_calls_are_allowlisted(source: str) -> None:
    problems: list[str] = []
    for test_name, body in parse_test_bodies(source).items():
        code_body = mask_cxx_literals_preserve_offsets(body)
        for call, args in find_calls(code_body, {"accept", "connect", "epoll_wait", "poll", "read", "waitpid", "write"}):
            split_args = split_call_args(args)
            if call in {"accept", "connect"}:
                problems.append(f"{test_name}: raw {call} must use the timeout helper")
            elif call == "waitpid" and "WNOHANG" not in args:
                problems.append(f"{test_name}: raw waitpid must be WNOHANG or use waitpid_timeout")
            elif call == "poll":
                timeout = split_args[2] if len(split_args) >= 3 else ""
                if timeout == "" or timeout.startswith("-"):
                    problems.append(f"{test_name}: raw poll must use a finite timeout")
            elif call == "epoll_wait":
                timeout = split_args[3] if len(split_args) >= 4 else ""
                if timeout == "" or timeout.startswith("-"):
                    problems.append(f"{test_name}: raw epoll_wait must use a finite timeout")
            elif call in {"read", "write"} and test_name not in RAW_IO_TEST_ALLOWLIST:
                problems.append(f"{test_name}: raw {call} must be added to the TESTD I/O allowlist")
    if problems:
        fail("TESTD_RUN raw blocking call guard failed: " + "; ".join(problems))


def require_raw_blocking_call_guard_rejects_synthetic_unbounded_tests() -> None:
    synthetic = """
TESTD_RUN(test_bad_waitpid)
{
    waitpid(PID, &status, 0);
    TESTD_PASS("bad.waitpid");
}
TESTD_RUN_END(test_bad_waitpid)

TESTD_RUN(test_bad_poll)
{
    poll(&pfd, 1, -1);
    epoll_wait(EPFD, &ev, 1, -1);
    TESTD_PASS("bad.poll");
}
TESTD_RUN_END(test_bad_poll)

TESTD_RUN(test_bad_io)
{
    read(fd, buf, len);
    write(fd, buf, len);
    char const* ignored = "waitpid(PID, &status, 0)";
    TESTD_PASS("bad.io");
}
TESTD_RUN_END(test_bad_io)
""".lstrip()
    try:
        require_testd_run_raw_blocking_calls_are_allowlisted(synthetic)
    except AssertionError as exc:
        message = str(exc)
        for token in ["raw waitpid", "raw poll", "raw epoll_wait", "raw read", "raw write"]:
            if token not in message:
                fail(f"synthetic TESTD blocking-call guard did not report {token}")
        return
    fail("synthetic TESTD blocking-call guard accepted unbounded raw calls")


def require_sets_equal(left: set[str], right: set[str], left_name: str, right_name: str) -> None:
    missing = sorted(left - right)
    extra = sorted(right - left)
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing from {right_name}: {', '.join(missing)}")
        if extra:
            parts.append(f"extra in {right_name}: {', '.join(extra)}")
        fail(f"{left_name}/{right_name} mismatch: {'; '.join(parts)}")


def main() -> None:
    source = strip_cxx_comments(TESTD_MAIN.read_text())
    require_raw_blocking_call_guard_rejects_synthetic_unbounded_tests()

    test_runs, test_ends, listed = parse_test_registry(source)
    require_ordered_equal(test_runs, test_ends, "TESTD_RUN", "TESTD_RUN_END")
    require_unique(listed, "TESTD_TESTS")
    test_run_set = set(test_runs)
    require_sets_equal(test_run_set, set(listed), "TESTD_RUN", "TESTD_TESTS")

    pass_labels_by_test = parse_pass_labels_by_test(source)
    require_sets_equal(test_run_set, set(pass_labels_by_test), "TESTD_RUN", "paired TESTD bodies")
    tests_without_passes = sorted(name for name, labels in pass_labels_by_test.items() if not labels)
    if tests_without_passes:
        fail(f"TESTD tests without PASS/CHECK labels: {', '.join(tests_without_passes)}")
    pass_labels = [label for labels in pass_labels_by_test.values() for label in labels]
    require_unique(pass_labels, "TESTD PASS/CHECK labels")
    source_level_pass_labels = PASS_LABEL_RE.findall(source)
    if len(source_level_pass_labels) != len(pass_labels):
        fail("TESTD PASS/CHECK labels exist outside paired TESTD_RUN/TESTD_RUN_END bodies")

    requested_modes, handled_modes = parse_remote_modes(source)
    require_sets_equal(requested_modes, handled_modes, "remote helper requests", "remote helper handlers")
    require_deadline_safe_wait_helpers(source)
    require_tcp_loopback_is_deadline_bounded(source)
    require_socket_setup_tests_are_deadline_bounded(source)
    require_remote_ipc_wait_handshake(source)
    require_remote_ipc_epoll_ctl_add_is_bounded(source)
    require_non_waitpid_wake_tests_bound_child_waits(source)
    require_process_waitpid_tests_are_deadline_bounded(source)
    require_non_waitpid_wake_tests_bound_parent_reads(source)
    require_wki_policy_syscall_tests(source)
    require_journal_device_test(source)
    require_compiled_manifest_rejects_empty_tests(source)
    require_testd_run_raw_blocking_calls_are_allowlisted(source)

    if "g_pass != total_tests()" not in source:
        fail("TESTD main does not fail closed on pass-count accounting mismatch")

    print(
        f"{len(test_runs)} TESTD tests, {len(pass_labels)} PASS labels, "
        f"and {len(requested_modes)} remote helper modes are fully registered"
    )


if __name__ == "__main__":
    main()
