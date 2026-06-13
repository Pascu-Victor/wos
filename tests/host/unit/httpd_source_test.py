#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HTTPD_CPP = ROOT / "modules" / "httpd" / "src" / "main.cpp"


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


def test_httpd_client_io_is_deadline_bounded() -> None:
    source = HTTPD_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <poll.h>",
            "#include <time.h>",
            "CLIENT_IO_TIMEOUT_MS",
            "CLIENT_DRAIN_TIMEOUT_MS",
            "set_nonblocking_for_timeout",
            "wait_fd_ready_until",
        ],
        "httpd timeout surface",
    )
    if "MAX_SEND_RETRIES" in source or "sched_yield" in source:
        fail("httpd send path must not use retry-count/yield spin loops")

    monotonic_now_body = function_body(source, "monotonic_now_ms")
    require_tokens(
        monotonic_now_body,
        [
            "ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC",
            "int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC",
            "if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC)",
            "return INT64_MAX",
        ],
        "httpd monotonic clock conversion",
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
        "httpd deadline addition",
    )
    if "return NOW_MS + timeout_ms" in deadline_after_body:
        fail("httpd deadline_after_ms must not use wrapping timeout addition")

    remaining_body = function_body(source, "remaining_ms_until")
    require_tokens(
        remaining_body,
        [
            "if (deadline_ms <= NOW_MS)",
            "errno = ETIMEDOUT",
            "int64_t const REMAINING_MS = deadline_ms - NOW_MS",
            "REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS)",
        ],
        "httpd remaining timeout math",
    )
    if "int64_t const REMAINING_MS = deadline_ms - NOW_MS;\n    if (REMAINING_MS <= 0)" in remaining_body:
        fail("httpd remaining_ms_until must compare before subtracting")

    raw_sends = re.findall(r"\bsend\s*\(", source)
    if len(raw_sends) != 1:
        fail(f"httpd raw send() calls must be isolated to send_all_timeout, saw {len(raw_sends)}")
    raw_recvs = re.findall(r"\brecv\s*\(", source)
    if len(raw_recvs) != 2:
        fail(f"httpd raw recv() calls must be isolated to timeout helpers/request reader, saw {len(raw_recvs)}")

    send_body = function_body(source, "send_all_timeout")
    require_tokens(
        send_body,
        [
            "set_nonblocking_for_timeout(fd, old_flags)",
            "wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms)",
            "send(fd, ptr, remaining, 0)",
            "retryable_socket_result(SENT)",
            "restore_fd_flags(fd, old_flags)",
        ],
        "httpd send_all_timeout",
    )

    drain_body = function_body(source, "drain_client_input_timeout")
    require_tokens(
        drain_body,
        [
            "set_nonblocking_for_timeout(fd, old_flags)",
            "wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms)",
            "recv(fd, drain.data(), drain.size(), 0)",
            "restore_fd_flags(fd, old_flags)",
        ],
        "httpd drain_client_input_timeout",
    )


def test_httpd_request_path_uses_deadline_helpers() -> None:
    source = HTTPD_CPP.read_text()
    main_body = function_body(source, "main")
    require_tokens(
        main_body,
        [
            "std::string request;",
            "read_request_timeout(client_fd, request, CLIENT_IO_TIMEOUT_MS)",
            "std::string_view const REQUEST(request.data(), request.size())",
            "drain_client_input_timeout(client_fd, CLIENT_DRAIN_TIMEOUT_MS)",
        ],
        "httpd main client receive/drain",
    )
    if "recv_once_timeout(client_fd, buffer.data()" in main_body or "std::string_view const REQUEST(buffer.data()" in main_body:
        fail("httpd main must not dispatch a one-shot recv buffer")

    serve_file_body = function_body(source, "serve_file")
    handle_request_body = function_body(source, "handle_request")
    if "send(" in serve_file_body or "send(" in handle_request_body:
        fail("httpd response paths must use send_all/send_response, not raw send()")


def test_httpd_request_framing_is_complete_and_bounded() -> None:
    source = HTTPD_CPP.read_text()
    require_tokens(
        source,
        [
            "MAX_REQUEST_BYTES",
            "MAX_CONTENT_LENGTH",
            "HEADER_TERMINATOR",
            "parse_content_length",
            "read_request_timeout",
        ],
        "httpd request framing surface",
    )

    parse_length_body = function_body(source, "parse_content_length")
    require_tokens(
        parse_length_body,
        [
            "content_length = 0",
            "ascii_iequals(trim_optional_whitespace(LINE.substr(0, COLON)), \"Content-Length\")",
            "parse_content_length_value(LINE.substr(COLON + 1), parsed)",
            "if (found && parsed != content_length)",
            "errno = EINVAL",
        ],
        "httpd Content-Length parser",
    )

    parse_value_body = function_body(source, "parse_content_length_value")
    require_tokens(
        parse_value_body,
        [
            "value = trim_optional_whitespace(value)",
            "if (CH < '0' || CH > '9')",
            "auto const DIGIT = static_cast<size_t>(CH - '0')",
            "parsed > (MAX_CONTENT_LENGTH - DIGIT) / 10",
            "content_length = parsed",
        ],
        "httpd Content-Length value parser",
    )

    read_body = function_body(source, "read_request_timeout")
    require_tokens(
        read_body,
        [
            "set_nonblocking_for_timeout(fd, old_flags)",
            "request.reserve(REQUEST_BUFFER_SIZE)",
            "request.find(HEADER_TERMINATOR)",
            "parse_content_length(request.substr(0, HEADER_END), content_length)",
            "EXPECTED_SIZE",
            "request.resize(EXPECTED_SIZE)",
            "errno = EMSGSIZE",
            "wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms)",
            "recv(fd, buffer.data(), CHUNK_LEN, 0)",
            "request.append(buffer.data(), static_cast<size_t>(RECEIVED))",
            "restore_fd_flags(fd, old_flags)",
        ],
        "httpd read_request_timeout",
    )

    parse_body = function_body(source, "parse_request_body")
    require_tokens(
        parse_body,
        [
            "request.find(HEADER_TERMINATOR)",
            "HEADER_TERMINATOR.size()",
        ],
        "httpd request body parser",
    )


def test_httpd_mount_child_wait_is_deadline_bounded() -> None:
    source = HTTPD_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <signal.h>",
            "#include <sys/wait.h>",
            "MOUNT_CHILD_TIMEOUT_MS",
            "MOUNT_CHILD_REAP_RETRIES",
            "wait_for_child_timeout",
            "reap_child_after_timeout",
        ],
        "httpd mount child wait surface",
    )

    reap_body = function_body(source, "reap_child_after_timeout")
    require_tokens(
        reap_body,
        [
            "if (pid <= 0)",
            "ker::process::kill(pid, SIGKILL)",
            "ker::process::waitpid(pid, &reap_status, WNOHANG, nullptr)",
            "usleep(CHILD_WAIT_POLL_US)",
        ],
        "httpd reap_child_after_timeout",
    )

    wait_body = function_body(source, "wait_for_child_timeout")
    require_tokens(
        wait_body,
        [
            "if (pid <= 0)",
            "*status = -EINVAL",
            "ker::process::waitpid(pid, status, WNOHANG, nullptr)",
            "child_wait_timed_out(DEADLINE_MS, waited_us, timeout_ms)",
            "reap_child_after_timeout(pid)",
            "*status = -ETIMEDOUT",
        ],
        "httpd wait_for_child_timeout",
    )
    if "ker::process::waitpid(pid, status, 0, nullptr)" in wait_body:
        fail("httpd wait_for_child_timeout must not use a blocking waitpid")

    handle_request_body = function_body(source, "handle_request")
    require_tokens(
        handle_request_body,
        [
            "wait_for_child_timeout(static_cast<int64_t>(exec_res), &exit_code, MOUNT_CHILD_TIMEOUT_MS)",
            "MOUNT_COMPLETED && exit_code == 0",
        ],
        "httpd /api/mount bounded wait",
    )
    forbidden = "ker::process::waitpid(static_cast<int64_t>(exec_res), &exit_code, 0, nullptr)"
    if forbidden in handle_request_body:
        fail("httpd /api/mount must not block indefinitely on mount child")


def test_httpd_mount_spawn_failure_and_fd_inheritance_are_guarded() -> None:
    source = HTTPD_CPP.read_text()
    cloexec_body = function_body(source, "set_fd_cloexec_best_effort")
    require_tokens(
        cloexec_body,
        [
            "fcntl(fd, F_GETFD, 0)",
            "fcntl(fd, F_SETFD, FLAGS | FD_CLOEXEC)",
        ],
        "httpd fd close-on-exec helper",
    )

    handle_request_body = function_body(source, "handle_request")
    require_tokens(
        handle_request_body,
        [
            "auto exec_res = ker::process::exec",
            "if (exec_res == 0)",
        ],
        "httpd /api/mount exec failure handling",
    )
    if "exec_res < 0" in handle_request_body:
        fail("WOS exec returns 0 on failure; httpd must not check exec_res < 0")

    main_body = function_body(source, "main")
    require_tokens(
        main_body,
        [
            "set_fd_cloexec_best_effort(server_fd)",
            "set_fd_cloexec_best_effort(client_fd)",
        ],
        "httpd listener/client fd inheritance guards",
    )


def main() -> None:
    test_httpd_client_io_is_deadline_bounded()
    test_httpd_request_path_uses_deadline_helpers()
    test_httpd_request_framing_is_complete_and_bounded()
    test_httpd_mount_child_wait_is_deadline_bounded()
    test_httpd_mount_spawn_failure_and_fd_inheritance_are_guarded()
    print("httpd client socket and mount child waits are deadline bounded")


if __name__ == "__main__":
    main()
