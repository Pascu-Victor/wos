#!/usr/bin/env python3

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TESTPROG_MAIN_CPP = ROOT / "modules" / "testprog" / "src" / "main.cpp"
NETBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "netbench.cpp"
PERFBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "perfbench.cpp"
COWBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "cowbench.cpp"
FSBENCH_CPP = ROOT / "modules" / "testprog" / "src" / "fsbench.cpp"
MANDELBENCH_WKI_CPP = ROOT / "modules" / "testprog" / "src" / "mandelbench" / "mandelbench_wki.cpp"
WOS_SHOWCASE_BENCH = ROOT / "configs" / "rootfs" / "root" / "wos-showcase" / "30-bench-wki.sh"
WOS_SHOWCASE_COMMON = ROOT / "configs" / "rootfs" / "root" / "wos-showcase" / "showcase-common.sh"
WOS_METADATA_BENCH = ROOT / "configs" / "rootfs" / "root" / "wos-showcase" / "metadata_bench.py"
USERLAND_SUITE = ROOT / "configs" / "drive" / "srv" / "wos_userland_suite.sh"
RUN_USERLAND_SUITE = ROOT / "scripts" / "bench" / "run_wos_userland_suite.sh"
WOS_SSH = ROOT / "scripts" / "remote" / "wos_ssh.sh"


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

    monotonic_now_body = function_body(source, "monotonic_now_ms")
    require_tokens(
        monotonic_now_body,
        [
            "ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC",
            "int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC",
            "if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC)",
            "return INT64_MAX",
        ],
        "ping monotonic clock conversion",
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
        "ping deadline addition",
    )
    if "return NOW_MS + timeout_ms" in deadline_after_body:
        fail("ping deadline_after_ms must not use wrapping timeout addition")

    remaining_body = function_body(source, "remaining_ms_until")
    require_tokens(
        remaining_body,
        [
            "if (deadline_ms <= NOW_MS)",
            "errno = ETIMEDOUT",
            "int64_t const REMAINING_MS = deadline_ms - NOW_MS",
            "REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS)",
        ],
        "ping remaining timeout math",
    )
    if "int64_t const REMAINING_MS = deadline_ms - NOW_MS;\n    if (REMAINING_MS <= 0)" in remaining_body:
        fail("ping remaining_ms_until must compare before subtracting")

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
    run_netbench_one = shell_function_body(source, "run_netbench_one")
    case_netbench_loopback = shell_function_body(source, "case_netbench_loopback")
    abort_netbench_case = shell_function_body(source, "abort_netbench_case")
    require_tokens(
        source,
        [
            "NETBENCH_TIMEOUT_MS=\"${WOS_SUITE_NETBENCH_TIMEOUT_MS:-30000}\"",
            "SUITE_REVISION=\"case-watchdog-v5\"",
            "SUITE_REVISION=%s",
            "NETBENCH_CASE_TIMEOUT_SECONDS=\"${WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS:-120}\"",
            "invalid WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS=%s; using 120",
            "NETBENCH_CASE_TIMEOUT_SECONDS=%s",
            "run_case_with_timeout netbench_loopback \"$NETBENCH_CASE_TIMEOUT_SECONDS\" case_netbench_loopback",
        ],
        "suite netbench timeout default",
    )
    if source.count("--timeout-ms \"$NETBENCH_TIMEOUT_MS\"") < 3:
        fail("wos-userland-suite must pass --timeout-ms to the netbench server and both client modes")
    require_tokens(
        run_netbench_one,
        [
            "client_log=\"$WORK_DIR/netbench-$mode-client.log\"",
            "server_status=\"$WORK_DIR/netbench-$mode-server.status\"",
            "client_status=\"$WORK_DIR/netbench-$mode-client.status\"",
            "NETBENCH_CHILD_PIDS=\"$server_pid $client_pid\"",
            "while [ ! -f \"$client_status\" ] || [ ! -f \"$server_status\" ]",
            "cleanup_netbench_children",
            "if [ \"$timed_out\" -eq 1 ]; then",
            "netbench %s client log retained at %s",
            "netbench %s server log retained at %s",
            "print_netbench_log \"netbench $mode client log\" \"$client_log\"",
            "print_netbench_log \"netbench $mode server log\" \"$server_log\"",
            "netbench %s timed out after %ss",
        ],
        "suite netbench bounded child handling",
    )
    cleanup_netbench_children = shell_function_body(source, "cleanup_netbench_children")
    require_tokens(
        cleanup_netbench_children,
        [
            "netbench_stop_pid \"$pid\"",
            "sleep \"$TIMEOUT_KILL_GRACE_SECONDS\"",
            "netbench_kill_pid \"$pid\"",
            "netbench_wait_pid \"$pid\"",
            "NETBENCH_CHILD_PIDS=",
        ],
        "suite netbench cleanup waits killed children",
    )
    require_order(
        cleanup_netbench_children,
        "netbench_kill_pid \"$pid\"",
        "netbench_wait_pid \"$pid\"",
        "netbench cleanup waits after kill",
    )
    require_order(
        run_netbench_one,
        "if [ \"$timed_out\" -eq 1 ]; then",
        "print_netbench_log \"netbench $mode client log\" \"$client_log\"",
        "suite netbench timeout skips child log replay",
    )
    if "cat \"$server_log\"" in run_netbench_one or "cat \"$client_log\"" in run_netbench_one:
        fail("wos-userland-suite must not cat possibly empty netbench logs directly")
    require_tokens(
        run_netbench_one,
        [
            "server_rc_now=\"$(netbench_status \"$server_status\")\"\n            if [ \"$server_rc_now\" != \"0\" ]; then\n                cleanup_netbench_children",
            "client_rc_now=\"$(netbench_status \"$client_status\")\"\n            if [ \"$client_rc_now\" != \"0\" ]; then\n                cleanup_netbench_children",
        ],
        "suite netbench asymmetric failure cleanup",
    )
    require_tokens(
        case_netbench_loopback,
        [
            "trap abort_netbench_case TERM INT HUP",
            "if run_netbench_one pingpong; then",
            "run_netbench_one stream || rc=1",
            "else",
            "rc=1",
            "cleanup_netbench_children",
            "trap - TERM INT HUP",
        ],
        "suite netbench cleanup trap",
    )
    require_tokens(
        abort_netbench_case,
        [
            "cleanup_netbench_children",
            "trap - TERM INT HUP",
            "exit 124",
        ],
        "suite netbench abort trap",
    )


def test_userland_suite_cases_are_watchdog_bounded() -> None:
    source = USERLAND_SUITE.read_text()
    run_case = shell_function_body(source, "run_case")
    run_case_with_timeout = shell_function_body(source, "run_case_with_timeout")
    status_file_value = shell_function_body(source, "status_file_value")
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
            "case_status=\"$WORK_DIR/$name.status\"",
            "rm -f \"$timeout_marker\" \"$case_status\"",
            "if [ \"$CASE_TIMEOUT_SECONDS\" -gt 0 ]",
            "printf '%s\\n' \"$?\" > \"$case_status\"",
            ") > \"$log\" 2>&1 &",
            "case_pid=\"$!\"",
            "trap 'exit 0' TERM INT HUP",
            "watchdog_elapsed=0",
            "while [ \"$watchdog_elapsed\" -lt \"$CASE_TIMEOUT_SECONDS\" ]",
            "watchdog_elapsed=$((watchdog_elapsed + 1))",
            "kill -TERM \"-$case_pid\"",
            "kill \"$case_pid\"",
            "if [ ! -f \"$case_status\" ]",
            "printf 'timeout after %ss\\n' \"$CASE_TIMEOUT_SECONDS\" > \"$timeout_marker\"",
            "grace_elapsed=0",
            "while [ \"$grace_elapsed\" -lt \"$TIMEOUT_KILL_GRACE_SECONDS\" ]",
            "grace_elapsed=$((grace_elapsed + 1))",
            "kill -KILL \"-$case_pid\"",
            "kill -KILL \"$case_pid\"",
            "watchdog_pid=\"$!\"",
            "while [ ! -f \"$case_status\" ]",
            "if [ -f \"$timeout_marker\" ]",
            "rc=\"$(status_file_value \"$case_status\")\"",
            "wait \"$case_pid\" 2>/dev/null || true",
            "wait \"$watchdog_pid\" 2>/dev/null || true",
            "if [ ! -f \"$case_status\" ]; then\n            wait \"$case_pid\" 2>/dev/null || true\n        fi",
            "timed_out=1",
            "rc=124",
            "printf '\\nTIMEOUT %s after %ss\\n' \"$name\" \"$CASE_TIMEOUT_SECONDS\" >> \"$log\"",
            "case log retained at %s after timeout",
            "cat_log_file \"$log\"",
            "record_summary \"$name\" \"FAIL\" \"timeout=${CASE_TIMEOUT_SECONDS}s ${duration}s\"",
        ],
        "suite run_case watchdog path",
    )
    if "cat \"$log\"" in run_case:
        fail("wos-userland-suite must not cat possibly empty case logs directly")
    require_tokens(
        status_file_value,
        [
            "read -r status_value < \"$status_file\" || status_value=124",
            "\"\"|*[!0-9]*)",
            "printf '124\\n'",
            "printf '%s\\n' \"$status_value\"",
        ],
        "suite status file sanitizing",
    )
    if "sleep \"$CASE_TIMEOUT_SECONDS\"" in run_case:
        fail("suite watchdog must not rely on a full-timeout sleeper that has to be killed after passing cases")
    if "kill -TERM \"-$watchdog_pid\"" in run_case or "kill \"$watchdog_pid\"" in run_case:
        fail("suite watchdog must self-exit from the case status file instead of requiring signal cleanup")
    require_order(run_case, "while [ \"$watchdog_elapsed\" -lt \"$CASE_TIMEOUT_SECONDS\" ]", "kill -TERM \"-$case_pid\"", "suite timeout TERM order")
    require_order(run_case, "kill -TERM \"-$case_pid\"", "printf 'timeout after %ss\\n'", "suite timeout marker order")
    require_order(run_case, "printf 'timeout after %ss\\n'", "while [ \"$grace_elapsed\" -lt \"$TIMEOUT_KILL_GRACE_SECONDS\" ]", "suite timeout grace order")
    require_order(run_case, "while [ \"$grace_elapsed\" -lt \"$TIMEOUT_KILL_GRACE_SECONDS\" ]", "kill -KILL \"-$case_pid\"", "suite timeout KILL order")
    require_order(run_case, "while [ ! -f \"$case_status\" ]", "wait \"$watchdog_pid\" 2>/dev/null || true", "suite watchdog wait order")
    require_order(run_case, "if [ -f \"$timeout_marker\" ]", "printf '\\nTIMEOUT %s after %ss\\n'", "suite timeout marker accounting order")
    require_order(run_case, "if [ \"$timed_out\" -eq 1 ]", "cat_log_file \"$log\"", "suite skips timeout log replay")
    require_order(run_case, "elif [ \"$timed_out\" -eq 1 ]", "record_summary \"$name\" \"FAIL\"", "suite timeout accounting order")
    require_tokens(
        run_case_with_timeout,
        [
            "old_case_timeout=\"$CASE_TIMEOUT_SECONDS\"",
            "CASE_TIMEOUT_SECONDS=\"$timeout_seconds\"",
            "run_case \"$name\" \"$@\"",
            "CASE_TIMEOUT_SECONDS=\"$old_case_timeout\"",
        ],
        "suite per-case timeout override",
    )


def test_userland_suite_can_request_guest_shutdown() -> None:
    source = USERLAND_SUITE.read_text()
    wrapper = RUN_USERLAND_SUITE.read_text()
    shutdown_body = shell_function_body(source, "request_suite_shutdown")
    require_tokens(
        source,
        [
            "SUITE_SHUTDOWN=\"${WOS_SUITE_SHUTDOWN:-0}\"",
            "shutdown|poweroff)",
            "SUITE_SHUTDOWN=poweroff",
            "suite_rc=0",
            "request_suite_shutdown",
            "exit \"$suite_rc\"",
        ],
        "suite shutdown request configuration",
    )
    require_tokens(
        shutdown_body,
        [
            "poweroff)",
            "shutdown_cmd=/sbin/poweroff",
            "halt)",
            "shutdown_cmd=/sbin/halt",
            "reboot)",
            "shutdown_cmd=/sbin/reboot",
            "REQUESTED_SHUTDOWN=%s",
            "\"$shutdown_cmd\" -f",
        ],
        "suite shutdown command dispatch",
    )
    result_dir_pos = source.rfind("printf 'RESULT_DIR=%s\\n' \"$ARTIFACT_ROOT\"")
    shutdown_call_pos = source.rfind("request_suite_shutdown")
    exit_pos = source.rfind("exit \"$suite_rc\"")
    if result_dir_pos < 0 or shutdown_call_pos < 0 or exit_pos < 0:
        fail("suite shutdown tail markers not found")
    if result_dir_pos >= shutdown_call_pos:
        fail("suite must print artifact path before requesting shutdown")
    if shutdown_call_pos >= exit_pos:
        fail("suite must preserve result status after shutdown request")
    require_tokens(
        wrapper,
        [
            "--shutdown [poweroff|halt|reboot]",
            "--sync-rootfs|--no-sync",
            "--probe-timeout SECONDS",
            "--timeout SECONDS",
            "DEFAULT_NETBENCH_CASE_TIMEOUT_SECONDS=120",
            "DEFAULT_PROBE_TIMEOUT=30",
            "DEFAULT_REMOTE_TIMEOUT=7200",
            "SHUTDOWN=\"\"",
            "SYNC_ROOTFS=\"auto\"",
            "CLUSTER_CONFIG=\"configs/cluster.json\"",
            "SYNC_TIMEOUT=\"300\"",
            "PROBE_TIMEOUT=\"$DEFAULT_PROBE_TIMEOUT\"",
            "REMOTE_TIMEOUT=\"$DEFAULT_REMOTE_TIMEOUT\"",
            "parse_nonnegative_seconds()",
            "run_with_timeout()",
            "timeout --kill-after=5s \"${seconds}s\" \"$@\"",
            "has_env_assignment()",
            "host_is_vm_alias()",
            "should_sync_rootfs()",
            "sync_suite_script()",
            "local_suite_revision()",
            "verify_remote_suite_revision()",
            "suite_requested_shutdown_success()",
            "grep '^SUITE_REVISION='",
            "run_with_timeout \"$PROBE_TIMEOUT\"",
            "Remote $REMOTE_SCRIPT is stale or missing SUITE_REVISION",
            "WOS_SUITE_SHUTDOWN=$SHUTDOWN",
            "WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS=$DEFAULT_NETBENCH_CASE_TIMEOUT_SECONDS",
            "shutdown|poweroff)",
            "--sync-timeout \"$SYNC_TIMEOUT\"",
            "PROBE_TIMEOUT=\"$(parse_nonnegative_seconds \"--probe-timeout\" \"$2\")\"",
            "REMOTE_TIMEOUT=\"$(parse_nonnegative_seconds \"--timeout\" \"$2\")\"",
            "--filter configs/drive/srv/wos_userland_suite.sh",
            "if should_sync_rootfs; then",
            "sync_suite_script",
            "verify_remote_suite_revision",
            "run_with_timeout \"$REMOTE_TIMEOUT\"",
            "\"${REMOTE_SCRIPTS}/wos_ssh.sh\" \"$HOST\" \"$REMOTE_CMD\" 2>&1",
            "| tee \"$OUTPUT\"",
            "STATUS=\"${PIPESTATUS[0]}\"",
            "TIMEOUT after ${REMOTE_TIMEOUT}s",
            "if [[ \"$STATUS\" -ne 0 && \"$STATUS\" -ne 124 ]] && suite_requested_shutdown_success; then",
            "STATUS=0",
            "awk -F= '/^RESULT_DIR=/ { value = $2 } END { print value }' \"$OUTPUT\"",
        ],
        "host userland suite wrapper shutdown flag",
    )
    if "sh -lc \"$REMOTE_CMD\"" in wrapper:
        fail("host userland suite wrapper must pass the quoted remote command as one SSH command string")
    if "RAW_RESULT=" in wrapper:
        fail("host userland suite wrapper must stream remote output instead of buffering the SSH result")


def test_wos_ssh_connect_path_is_bounded() -> None:
    source = WOS_SSH.read_text()
    require_tokens(
        source,
        [
            "SSH_CONNECT_TIMEOUT=\"${WOS_SSH_CONNECT_TIMEOUT:-10}\"",
            "SSH_SERVER_ALIVE_INTERVAL=\"${WOS_SSH_SERVER_ALIVE_INTERVAL:-15}\"",
            "SSH_SERVER_ALIVE_COUNT_MAX=\"${WOS_SSH_SERVER_ALIVE_COUNT_MAX:-4}\"",
            "-o ConnectTimeout=\"$SSH_CONNECT_TIMEOUT\"",
            "-o ConnectionAttempts=1",
            "-o ServerAliveInterval=\"$SSH_SERVER_ALIVE_INTERVAL\"",
            "-o ServerAliveCountMax=\"$SSH_SERVER_ALIVE_COUNT_MAX\"",
            "-o BatchMode=yes",
        ],
        "wos ssh connection timeout",
    )


def test_perfbench_context_switch_counter_is_atomic() -> None:
    source = PERFBENCH_CPP.read_text()
    require_tokens(
        source,
        [
            "auto test_context_switch() -> bool",
            "std::atomic<int>* counter",
            "std::atomic<int> counter{0}",
            "counter.store(0, std::memory_order_release)",
        ],
        "perfbench context switch counter storage",
    )

    pingpong_body = function_body(source, "pingpong_thread")
    require_tokens(
        pingpong_body,
        [
            "a->counter->load(std::memory_order_acquire) < a->target",
            "auto const OBSERVED = a->counter->load(std::memory_order_relaxed)",
            "a->counter->store(OBSERVED + 1, std::memory_order_release)",
        ],
        "perfbench context switch counter synchronization",
    )

    forbidden = [
        "volatile int* counter",
        "volatile int counter = 0",
        "while (*a->counter < a->target)",
        "*a->counter = *a->counter + 1",
    ]
    present = [snippet for snippet in forbidden if snippet in source]
    if present:
        fail(f"perfbench context switch counter still uses racy volatile state: {present[0]}")

    context_body = function_body(source, "test_context_switch")
    require_tokens(
        context_body,
        [
            "int const MTX_RESULT = mtx_init(&mtx, MTX_PLAIN)",
            "if (MTX_RESULT != THRD_SUCCESS)",
            "int const CREATE_RESULT_0 = thrd_create(&t0, pingpong_thread, &a0)",
            "if (CREATE_RESULT_0 != THRD_SUCCESS)",
            "int const CREATE_RESULT_1 = thrd_create(&t1, pingpong_thread, &a1)",
            "if (CREATE_RESULT_1 != THRD_SUCCESS)",
            "int const JOIN_RESULT_0 = thrd_join(t0, nullptr)",
            "int const JOIN_RESULT_1 = thrd_join(t1, nullptr)",
            "if (JOIN_RESULT_0 != THRD_SUCCESS || JOIN_RESULT_1 != THRD_SUCCESS)",
            "mtx_destroy(&mtx)",
            "return true;",
        ],
        "perfbench context switch thread lifecycle checks",
    )
    unchecked_calls = {
        "thrd_create(&t0, pingpong_thread, &a0);",
        "thrd_create(&t1, pingpong_thread, &a1);",
        "thrd_join(t0, nullptr);",
        "thrd_join(t1, nullptr);",
    }
    for line in context_body.splitlines():
        if line.strip() in unchecked_calls:
            fail(f"perfbench context switch must check lifecycle call result: {line.strip()}")

    run_body = function_body(source, "run_perf")
    require_tokens(
        run_body,
        ["if (!test_context_switch())", "return 1;"],
        "perfbench context switch failure propagation",
    )


def test_perfbench_parallel_workers_cleanup_before_failure_return() -> None:
    source = PERFBENCH_CPP.read_text()
    worker_body = function_body(source, "par_eff_thread")
    release_body = function_body(source, "release_parallel_workers")
    join_body = function_body(source, "join_parallel_workers")
    cleanup_body = function_body(source, "cleanup_parallel_workers")
    parallel_body = function_body(source, "test_parallel_efficiency")

    forbidden = ["#include <sys/futex.h>", "ker::futex::", "futex_wait(", "futex_wake("]
    present = [snippet for snippet in forbidden if snippet in source]
    if present:
        fail(f"perfbench parallel_eff must not depend on direct userspace futex calls: {present[0]}")

    require_tokens(
        worker_body,
        [
            "while (a->go->load(std::memory_order_acquire) == 0)",
            "timespec const REQ{.tv_sec = 0, .tv_nsec = COUNTER_WAIT_SLEEP_NS}",
            "nanosleep(&REQ, nullptr)",
        ],
        "perfbench parallel worker libc-neutral start wait",
    )
    require_tokens(
        release_body,
        ["go.store(1, std::memory_order_release)"],
        "perfbench parallel worker release",
    )
    require_tokens(
        join_body,
        ["thrd_join(thread, nullptr)", "thread = nullptr"],
        "perfbench parallel worker join cleanup",
    )
    require_tokens(
        cleanup_body,
        ["release_parallel_workers(go)", "join_parallel_workers(threads)"],
        "perfbench parallel worker cleanup helper",
    )

    for context, marker, cleanup in [
        ("create failure", "if (create_failed)", "cleanup_parallel_workers(go, threads);"),
        ("ready timeout", "if (!wait_for_counter(ready_count, n, PARALLEL_READY_TIMEOUT_NS))", "cleanup_parallel_workers(go, threads);"),
        ("done timeout", "if (!wait_for_counter(done_count, n, PARALLEL_DONE_TIMEOUT_NS))", "join_parallel_workers(threads);"),
    ]:
        start = parallel_body.find(marker)
        if start < 0:
            fail(f"perfbench parallel_eff missing {context} branch")
        ret = parallel_body.find("return false;", start)
        if ret < 0:
            fail(f"perfbench parallel_eff {context} branch no longer returns false")
        branch = parallel_body[start:ret]
        if cleanup not in branch:
            fail(f"perfbench parallel_eff {context} branch must cleanup workers before returning")

    require_order(
        parallel_body,
        "release_parallel_workers(go);",
        "if (!wait_for_counter(done_count, n, PARALLEL_DONE_TIMEOUT_NS))",
        "perfbench parallel_eff releases workers before waiting for completion",
    )


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


def test_vfsbench_metadata_timing_excludes_setup_and_cleanup() -> None:
    source = FSBENCH_CPP.read_text()
    main_source = TESTPROG_MAIN_CPP.read_text()
    require_tokens(
        main_source,
        [
            'std::strcmp(command, "vfsbench-create") == 0',
            'std::strcmp(command, "vfsbench-rename") == 0',
            'std::strcmp(command, "vfsbench-metadata-worker") == 0',
        ],
        "testprog metadata benchmark dispatch",
    )
    require_tokens(
        source,
        [
            "clock_gettime(CLOCK_MONOTONIC, &ts)",
            '"benchmark":"wos_vfsbench_create"',
            '"benchmark":"wos_vfsbench_rename"',
            "O_CREAT | O_EXCL | O_WRONLY",
            "PARSED > std::numeric_limits<uint32_t>::max()",
            "enum class MetadataPathAction",
            "MetadataPathAction::VERIFY_ABSENT",
            "MetadataPathAction::VERIFY_EMPTY",
            "metadata-worker-ready-v1",
            "metadata-worker-done-v1 {}",
        ],
        "VFS metadata benchmark",
    )

    create_body = function_body(source, "run_create")
    require_order(create_body, "iteration_paths(", "MetadataPathAction::CLEANUP", "create path setup")
    require_order(create_body, "MetadataPathAction::CLEANUP", "uint64_t const STARTED_NS", "create stale cleanup")
    require_order(create_body, "uint64_t const STARTED_NS", "MetadataPathAction::CREATE", "create timing start")
    require_order(
        create_body,
        "uint64_t const ELAPSED_NS",
        "MetadataPathAction::VERIFY_EMPTY",
        "create verification timing boundary",
    )
    create_timed = create_body[
        create_body.index("uint64_t const STARTED_NS") : create_body.index("uint64_t const ELAPSED_NS")
    ]
    if "CLEANUP" in create_timed or "VERIFY" in create_timed:
        fail("VFS create timing must exclude verification and cleanup syscalls")

    rename_body = function_body(source, "run_rename")
    require_order(rename_body, "MetadataPathAction::CREATE", "uint64_t const STARTED_NS", "rename setup")
    require_order(rename_body, "uint64_t const STARTED_NS", "MetadataPathAction::RENAME", "rename timing start")
    require_order(
        rename_body,
        "uint64_t const ELAPSED_NS",
        "MetadataPathAction::VERIFY_EMPTY",
        "rename verification timing boundary",
    )
    rename_timed = rename_body[
        rename_body.index("uint64_t const STARTED_NS") : rename_body.index("uint64_t const ELAPSED_NS")
    ]
    if "CREATE" in rename_timed or "CLEANUP" in rename_timed or "VERIFY" in rename_timed:
        fail("VFS rename timing must exclude fixture setup, verification, and cleanup syscalls")

    range_body = function_body(source, "execute_metadata_path_range")
    require_tokens(
        range_body,
        [
            "PATH_COUNT / pool.worker_count",
            "PATH_COUNT % pool.worker_count",
            "std::min(static_cast<size_t>(worker_index), EXTRA)",
            "for (size_t index = BEGIN; index < BEGIN + COUNT; ++index)",
            "apply_metadata_path_action(pool, index)",
        ],
        "deterministic disjoint metadata path partition",
    )
    require_tokens(
        range_body,
        [
            "ker::abi::vfs::METADATA_BATCH_MAX_ITEMS",
            "ker::abi::vfs::metadata_batch_header const HEADER",
            "ker::abi::vfs::metadata_batch(&HEADER, entries.data(), results.data())",
            "BATCH_STATUS == -EOPNOTSUPP && chunk_begin == BEGIN",
            "metadata_batch_result_succeeded",
        ],
        "vfsbench uniform metadata batching",
    )
    require_order(range_body, "ker::abi::vfs::metadata_batch(&HEADER", "BATCH_STATUS == -EOPNOTSUPP", "batch before safe fallback")
    require_order(range_body, "BATCH_STATUS == -EOPNOTSUPP", "apply_metadata_path_action", "fallback only after pre-send unsupported")
    require_order(range_body, "apply_metadata_path_action", "if (BATCH_STATUS != 0)", "no scalar replay for other batch failures")
    if range_body.count("apply_metadata_path_action") != 1:
        fail("vfsbench metadata worker may contain only one scalar fallback path")
    initialize_body = function_body(source, "initialize_metadata_path_workers")
    require_tokens(
        initialize_body,
        [
            "worker_count - 1",
            "arg.worker_index = static_cast<uint32_t>(created + 1)",
            "thrd_create(&pool.threads.at(created), metadata_path_worker_main, &arg)",
            "while (pool.started != BACKGROUND_COUNT)",
            "pool.initialized = true",
        ],
        "persistent metadata path pool startup",
    )
    action_body = function_body(source, "run_metadata_path_action")
    require_tokens(
        action_body,
        [
            "pool.worker_count > primary_paths.size()",
            "++pool.generation",
            "execute_metadata_path_range(pool, 0)",
            "while (pool.completed != pool.threads.size())",
            "success = arg.success && success",
        ],
        "caller-participating fixed metadata pool",
    )

    worker_body = function_body(source, "run_metadata_worker")
    require_tokens(
        worker_body,
        [
            "parse_metadata_worker_options(argc, argv, &options)",
            "initialize_metadata_path_workers(workers, options.workers)",
            'std::println("metadata-worker-ready-v1")',
            "std::fflush(stdout)",
            "std::fgets(command.data(), static_cast<int>(command.size()), stdin)",
            'std::strcmp(command.data(), "create") == 0',
            'std::strcmp(command.data(), "rename") == 0',
            "run_metadata_worker_operation(command.data(), path, options.iterations, workers)",
            "stop_metadata_path_workers(workers)",
            "std::ferror(stdin)",
        ],
        "persistent VFS metadata worker",
    )
    require_order(
        worker_body,
        "initialize_metadata_path_workers(workers, options.workers)",
        'std::println("metadata-worker-ready-v1")',
        "persistent metadata pool starts before readiness",
    )
    require_order(
        worker_body,
        'std::println("metadata-worker-ready-v1")',
        "std::fgets(command.data()",
        "persistent metadata worker readiness precedes phase input",
    )
    operation_body = function_body(source, "run_metadata_worker_operation")
    require_tokens(
        operation_body,
        [
            "MetadataOptions const OPTIONS{.path = path, .iterations = iterations, .workers = workers.worker_count}",
            "run_create(OPTIONS, workers)",
            "run_rename(OPTIONS, workers)",
            'std::println("metadata-worker-done-v1 {}", operation)',
            "std::fflush(stdout)",
        ],
        "persistent metadata worker exact operation reuse",
    )

    showcase = WOS_SHOWCASE_BENCH.read_text()
    require_tokens(
        showcase,
        [
            "showcase_scale_value metadata_iterations",
            "WOS_SHOWCASE_METADATA_TOTAL_ITERATIONS",
            'metadata_hosts="${WOS_SHOWCASE_HOSTS:-$(hostname)}"',
            'metadata_launcher="$(hostname)"',
            'showcase_cmd locally /usr/bin/python3 "$DIR/metadata_bench.py"',
            "--operation create",
            "--operation rename",
            '--hosts "$metadata_hosts"',
            '--total-work-units "$metadata_iterations"',
        ],
        "WKI metadata showcase",
    )
    if showcase.count(
        'showcase_cmd locally /usr/bin/python3 "$DIR/metadata_bench.py"'
    ) != 1:
        fail("WKI metadata showcase must share one ready worker pool across phases")
    require_tokens(
        WOS_SHOWCASE_COMMON.read_text(),
        ["quick:metadata_iterations", "full:metadata_iterations", "stress:metadata_iterations"],
        "WKI metadata showcase scales",
    )

    metadata_helper = WOS_METADATA_BENCH.read_text()
    require_tokens(
        metadata_helper,
        [
            "time.monotonic_ns()",
            "subprocess.Popen(",
            '"on",',
            '"forward",',
            '"+/tmp",',
            'LOCAL_RUNTIME_PATHS = ("/usr", "/bin", "/lib", "/lib64", "/libexec", "/share")',
            '*(f"-{path}" for path in LOCAL_RUNTIME_PATHS)',
            'READY_RECORD = "metadata-worker-ready-v1"',
            'DONE_RECORD_PREFIX = "metadata-worker-done-v1"',
            "MAX_TOTAL_WORKERS = 32",
            "exec /usr/bin/testprog vfsbench-metadata-worker",
            '--create-path "$1" --rename-path "$2" --iterations "$3" --workers "$4"',
            "stdin=subprocess.PIPE",
            "wait_for_ready_workers(",
            "for operation in config.operations:",
            "elapsed_by_operation[operation] = run_phase(",
            'print(json.dumps(payload, separators=(",", ":"), sort_keys=True))',
            '"spawner_host": worker_evidence.spawner_host',
            '"remote_pid": worker_evidence.remote_pid',
            '"workers": job.workers',
            '"placement": "local-baseline" if len(jobs) == 1 else "strict-on"',
            '"wki_route": "host-path"',
            '"total_work_units": config.total_work_units',
            '"total_workers": sum(job.workers for job in jobs)',
            "total_workers = min(MAX_TOTAL_WORKERS, config.total_work_units)",
            "worker_quotient, worker_remainder = divmod(total_workers, host_count)",
            "if not 1 <= workers <= work_units",
            "GRACEFUL_SHUTDOWN_FRACTION = 0.1",
            "GRACEFUL_SHUTDOWN_MAX_SECONDS = 10.0",
            "def graceful_shutdown_seconds(timeout_seconds: float)",
            "shutdown_seconds = graceful_shutdown_seconds(timeout_seconds)",
            "stop_worker_pool(runtimes, config.timeout_seconds, signal_guard)",
            "assert_worker_shell_execs_worker(Path(directory))",
            "expected_worker_splits",
            "reported_workers=job.workers + 1",
            "shutdown_sleep_seconds=1.2",
        ],
        "fixed-total WKI metadata coordinator",
    )
    execute_body = metadata_helper[
        metadata_helper.index("def execute_benchmarks(") : metadata_helper.index(
            "def emit_worker_diagnostics("
        )
    ]
    require_order(
        execute_body,
        "wait_for_ready_workers(",
        "for operation in config.operations:",
        "metadata workers ready before either timed phase",
    )
    phase_body = metadata_helper[
        metadata_helper.index("def run_phase(") : metadata_helper.index(
            "def stop_worker_pool("
        )
    ]
    require_order(
        phase_body,
        "started_ns = time.monotonic_ns()",
        "runtime.process.stdin.write(trigger)",
        "metadata phase timer covers every trigger",
    )
    require_order(
        phase_body,
        "completed.add(runtime.job.index)",
        "elapsed_ns = time.monotonic_ns() - started_ns",
        "metadata phase timer ends after every worker completion",
    )
    if "worker_evidence.elapsed_seconds" in metadata_helper:
        fail("metadata coordinator must report its phase timer, not maximum inner time")
    if "worker_child" in metadata_helper or "/usr/bin/testprog \"$benchmark\"" in metadata_helper:
        fail("metadata ready shell must exec one persistent testprog worker")
    result = subprocess.run(
        [sys.executable, str(WOS_METADATA_BENCH), "--self-test"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0 or "metadata_bench self-test: PASS" not in result.stdout:
        fail(
            "metadata benchmark self-test failed: "
            f"rc={result.returncode} stdout={result.stdout!r} stderr={result.stderr!r}"
        )


def test_mandelbench_worker_waits_use_monotonic_elapsed_time() -> None:
    source = MANDELBENCH_WKI_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <time.h>",
            "clock_gettime(CLOCK_MONOTONIC, &ts)",
            "ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC",
            "if (SEC > (std::numeric_limits<uint64_t>::max() - NSEC_US) / USEC_PER_SEC)",
            "return std::numeric_limits<uint64_t>::max()",
            "auto elapsed_us(uint64_t start_us, uint64_t end_us) -> uint64_t",
            "if (end_us <= start_us)",
        ],
        "mandelbench monotonic timestamp conversion",
    )
    if "gettimeofday(" in source or "#include <sys/time.h>" in source:
        fail("mandelbench worker wait deadlines must not use wall-clock gettimeofday")

    header_body = function_body(source, "complete_worker_headers")
    output_body = function_body(source, "complete_workers_and_outputs")
    streaming_body = function_body(source, "complete_worker_payloads_streaming")
    for body, context in [
        (header_body, "mandelbench header wait"),
        (output_body, "mandelbench output wait"),
        (streaming_body, "mandelbench streaming wait"),
    ]:
        if "NOW_US -" in body:
            fail(f"{context} must compare elapsed_us() instead of subtracting unsigned timestamps directly")

    require_tokens(
        header_body,
        [
            "elapsed_us(launch.header_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US",
            "elapsed_us(launch.header_last_progress_us, NOW_US) > PIPE_READ_IDLE_TIMEOUT_US",
        ],
        "mandelbench header wait elapsed checks",
    )
    require_tokens(
        output_body,
        [
            "elapsed_us(launch.read_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US",
            "elapsed_us(START_US, NOW_US) > WORKER_WAIT_TIMEOUT_US",
            "elapsed_us(launch.read_last_progress_us, NOW_US) > PIPE_PAYLOAD_IDLE_TIMEOUT_US",
        ],
        "mandelbench output wait elapsed checks",
    )
    require_tokens(
        streaming_body,
        [
            "elapsed_us(launch.header_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US",
            "elapsed_us(launch.read_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US",
            "elapsed_us(launch.header_last_progress_us, NOW_US) > PIPE_READ_IDLE_TIMEOUT_US",
            "elapsed_us(launch.read_last_progress_us, NOW_US) > PIPE_PAYLOAD_IDLE_TIMEOUT_US",
        ],
        "mandelbench streaming wait elapsed checks",
    )


def test_mandelbench_auto_nodes_request_remote_wki_placement() -> None:
    source = MANDELBENCH_WKI_CPP.read_text()
    target_body = function_body(source, "set_child_target")
    require_tokens(
        target_body,
        [
            "ker::process::WKI_TARGET_FLAG_STRICT | ker::process::WKI_TARGET_FLAG_NOINHERIT",
            "if (target_node.empty())",
            "INHERIT_FLAGS | ker::process::WKI_TARGET_FLAG_REMOTE",
            "ker::process::setwkitarget(nullptr, 0, FLAGS)",
            "failed to auto-target worker",
            "ker::process::setwkitarget(target_node.c_str(), static_cast<uint64_t>(target_node.size()), FLAGS)",
        ],
        "mandelbench auto-node WKI target selection",
    )
    if re.search(r"if\s*\(\s*target_node\.empty\(\)\s*\)\s*\{\s*return true;", target_body) is not None:
        fail("mandelbench auto-node workers must request remote WKI placement, not silently run locally")


def test_mandelbench_coalesces_remote_workers_and_keeps_local_compute_local() -> None:
    source = MANDELBENCH_WKI_CPP.read_text()
    require_tokens(
        source,
        [
            "constexpr uint64_t PIPE_PAYLOAD_IDLE_TIMEOUT_US = 300'000'000;",
            "constexpr unsigned char WORKER_CONTROL_RELEASE_PAYLOAD = 2;",
            "constexpr int MANDELBENCH_ROW_BAND_ROWS = 4;",
            "MANDELBENCH_SAVE_IMAGES",
            "MANDELBENCH_PAYLOAD_RELEASE",
            "return value == nullptr || value[0] == '\\0' || value[0] != '0';",
            "MANDELBENCH_PROFILE_WORKER_LIMIT",
            '#include <sys/vfs.h>',
            '#include <atomic>',
            'auto get_testprog_path() -> const char* { return "/usr/bin/testprog"; }',
            "auto generate_dynamic_chunk_rows(void* param) -> int",
            "fetch_add(1, std::memory_order_relaxed)",
        ],
        "mandelbench remote worker slot settings",
    )
    if "MANDELBENCH_THREADS_PER_WORKER_SLOT" in source or "find_or_create_launch" in source:
        fail("mandelbench must not fan requested threads out into one WKI pipe per worker slot")
    if "find_or_create_local_launch" in source:
        fail("mandelbench local slots must use child workers, not coordinator-local thread coalescing")

    payload_release_config_body = function_body(source, "mandelbench_payload_release_enabled")
    require_tokens(
        payload_release_config_body,
        [
            "MANDELBENCH_PAYLOAD_RELEASE",
            "return value != nullptr && value[0] != '\\0' && value[0] != '0';",
        ],
        "mandelbench payload release opt-in",
    )

    coordinator_body = function_body(source, "mandelbench_wki")
    require_tokens(
        coordinator_body,
        [
            "std::vector<WorkerLaunch> remote_launches;",
            "std::vector<WorkerLaunch> local_launches;",
            "remote_launches.reserve(static_cast<size_t>(TARGET_NODES.empty() ? ACTIVE_WORKERS : TARGET_NODES.size()));",
            "std::vector<WorkerSlot> worker_slots;",
            "start_row += MANDELBENCH_ROW_BAND_ROWS",
            "band_index % ACTIVE_WORKERS",
            "int const THREAD_COUNT = count_slots_for_target(worker_slots, slot.target_node, slot.local);",
            "auto launch_it = find_launch_for_output_slot(remote_launches, slot.worker_id);",
            "make_worker_launch(slot.worker_id, {}, 1, false)",
            "auto launch_it = find_launch_for_target(remote_launches, slot.target_node);",
            "make_worker_launch(slot.worker_id, slot.target_node, THREAD_COUNT, false)",
            "auto launch_it = find_launch_for_target(local_launches, slot.target_node);",
            "make_worker_launch(slot.worker_id, slot.target_node, THREAD_COUNT, true)",
            "bool const SAVE_IMAGES = mandelbench_save_images_enabled();",
            "bool const PAYLOAD_RELEASE = mandelbench_payload_release_enabled();",
            "LocalComputeTask local_compute_task{",
            "PAYLOAD_RELEASE, image.data(), ROW_SIZE)",
            "print_slowest_worker_profiles(remote_launches, repeat_index, START_US);",
            "if (SAVE_IMAGES)",
            "save_skipped=1",
            "install_worker_vfs_policy(launch.output_slot, launch.target_node);",
            "close_standard_fds_for_worker_child();",
            "if (!set_child_target(launch.target_node, launch.output_slot))",
        ],
        "mandelbench coalesced remote planner",
    )
    if "worker_launches.reserve(static_cast<size_t>(ACTIVE_WORKERS));" in coordinator_body:
        fail("mandelbench must not launch one child pipe per active worker slot")
    require_order(coordinator_body, "progress(DEVICE_NAME", "if (SAVE_IMAGES)", "mandelbench progress before optional save")
    require_order(
        coordinator_body,
        "install_worker_vfs_policy(launch.output_slot, launch.target_node);",
        "close_standard_fds_for_worker_child();",
        "mandelbench child VFS policy before stdio cleanup",
    )
    require_order(
        coordinator_body,
        "close_standard_fds_for_worker_child();",
        "!set_child_target(launch.target_node, launch.output_slot)",
        "mandelbench child fd cleanup before remote placement",
    )

    vfs_policy_body = function_body(source, "install_worker_vfs_policy")
    require_tokens(
        vfs_policy_body,
        [
            "ker::abi::vfs::wki_rule_add_vfs(path, ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)",
            'add_local_rule("/usr", "worker runtime");',
            'add_local_rule("/bin", "worker runtime fallback");',
            'add_local_rule("/lib", "worker runtime");',
            'add_local_rule("/lib64", "worker runtime");',
            'add_local_rule("/usr/bin/testprog", "worker executable");',
            'add_local_rule("/bin/testprog", "worker executable fallback");',
        ],
        "mandelbench worker VFS policy",
    )

    close_stdio_body = function_body(source, "close_standard_fds_for_worker_child")
    require_tokens(
        close_stdio_body,
        ["close(STDIN_FILENO);", "close(STDOUT_FILENO);", "close(STDERR_FILENO);"],
        "mandelbench worker child stdio cleanup",
    )

    worker_body = function_body(source, "mandelbench_worker")
    require_tokens(
        worker_body,
        [
            "mandelbench_profile_enabled() && fd_is_open(STDERR_FILENO)",
            "if (thread_count == 1)",
            "(void)generate_rows(&worker_thread.arg);",
            "(void)generate_dynamic_chunk_rows(&worker_thread.band_arg);",
            "std::atomic<int> next_chunk{0};",
            "std::atomic<int> grouped_rows_done{0};",
            "auto thread_func = GROUPED_CHUNKS ? generate_dynamic_chunk_rows : generate_rows;",
            "rows_done = grouped_rows_done.load(std::memory_order_relaxed);",
            "bool const PAYLOAD_RELEASE = mandelbench_payload_release_enabled();",
            'PAYLOAD_RELEASE && !wait_for_control_byte(CONTROL_FD, id, "payload release")',
        ],
        "mandelbench dynamic chunk worker compute",
    )
    if "generate_chunk_pool_rows" in source:
        fail("mandelbench grouped workers must use the current finite dynamic chunk scheduler")
    require_order(worker_body, "compute begin", "header write begin", "mandelbench worker computes before output header")
    require_order(worker_body, "header write begin", "payload write begin", "mandelbench worker output header before payload")

    streaming_body = function_body(source, "complete_worker_payloads_streaming")
    require_tokens(
        streaming_body,
        [
            "if (payload_release_enabled)",
            "release_worker_payload(launch, repeat_index)",
            "launch.payload_released = true;",
        ],
        "mandelbench payload release gate",
    )

    local_compute_body = function_body(source, "compute_local_launches")
    require_tokens(
        local_compute_body,
        [
            "std::vector<std::vector<size_t>> chunk_offsets;",
            "std::vector<std::atomic<int>> next_chunks(launches.size());",
            "std::vector<std::atomic<int>> rows_done_by_launch(launches.size());",
            "for (const auto& launch : launches)",
            "int const THREADS = std::clamp(launch.thread_count, 1, static_cast<int>(launch.chunks.size()));",
            "thrd_create(&compute_thread.thread, generate_dynamic_chunk_rows, &compute_thread.arg)",
            "for (size_t i = 0; i < created_threads; ++i)",
            "thrd_join(compute_thread.thread, nullptr)",
            "rows_done_by_launch.at(launch_index).load(std::memory_order_relaxed)",
        ],
        "mandelbench local compute dynamic chunk threads",
    )
    require_order(
        local_compute_body,
        "for (const auto& launch : launches)",
        "for (size_t i = 0; i < created_threads; ++i)",
        "mandelbench local compute must start all local chunk threads before joining any",
    )
    create_loop_start = local_compute_body.find("for (const auto& launch : launches)")
    join_loop_start = local_compute_body.find("for (size_t i = 0; i < created_threads; ++i)")
    if create_loop_start < 0 or join_loop_start < 0:
        fail("mandelbench local compute loops not found")
    if "thrd_join(" in local_compute_body[create_loop_start:join_loop_start]:
        fail("mandelbench local compute must not join one local chunk before starting the next")


def test_mandelbench_worker_payload_rle_is_bounded_and_exact() -> None:
    source = MANDELBENCH_WKI_CPP.read_text()
    require_tokens(
        source,
        [
            "constexpr uint16_t WORKER_OUTPUT_VERSION = 2;",
            "constexpr uint16_t WORKER_OUTPUT_FLAG_PAYLOAD_RLE = 1U << 0U;",
            "constexpr size_t WORKER_OUTPUT_RGBA_BYTES = 4;",
            "sizeof(uint16_t) + WORKER_OUTPUT_RGBA_BYTES",
        ],
        "mandelbench RLE protocol",
    )

    rle_config_body = function_body(source, "mandelbench_rle_enabled")
    require_tokens(
        rle_config_body,
        [
            'std::getenv("MANDELBENCH_RLE")',
            "return value == nullptr || value[0] == '\\0' || value[0] != '0';",
        ],
        "mandelbench default-on RLE configuration",
    )

    worst_case_body = function_body(source, "worker_rgba_rle_worst_case_size")
    require_tokens(
        worst_case_body,
        [
            "(raw_size % WORKER_OUTPUT_RGBA_BYTES) != 0",
            "PIXEL_COUNT > std::numeric_limits<size_t>::max() / WORKER_OUTPUT_RLE_RECORD_SIZE",
            "encoded_size = PIXEL_COUNT * WORKER_OUTPUT_RLE_RECORD_SIZE;",
        ],
        "mandelbench RLE worst-case allocation bound",
    )

    encode_body = function_body(source, "encode_worker_rgba_rle")
    require_tokens(
        encode_body,
        [
            "encoded.clear();",
            "run_length < std::numeric_limits<uint16_t>::max()",
            "static_cast<size_t>(run_length) < PIXEL_COUNT - pixel_index",
            "WORKER_OUTPUT_RGBA_BYTES) == 0",
            "RECORD_OFFSET > std::numeric_limits<size_t>::max() - WORKER_OUTPUT_RLE_RECORD_SIZE",
            "std::memcpy(encoded.data() + RECORD_OFFSET, &run_length, sizeof(run_length));",
            "std::memcpy(encoded.data() + RECORD_OFFSET + sizeof(run_length), color, WORKER_OUTPUT_RGBA_BYTES);",
        ],
        "mandelbench RLE encoder bounds",
    )

    header_body = function_body(source, "validate_worker_output_header")
    require_tokens(
        header_body,
        [
            "FLAGS == WORKER_OUTPUT_FLAG_PAYLOAD",
            "static_cast<size_t>(PAYLOAD_SIZE) == launch.expected_payload_size",
            "FLAGS == WORKER_OUTPUT_FLAG_PAYLOAD_RLE",
            "PAYLOAD_SIZE != 0",
            "static_cast<size_t>(PAYLOAD_SIZE) % WORKER_OUTPUT_RLE_RECORD_SIZE",
            "static_cast<size_t>(PAYLOAD_SIZE) < launch.expected_payload_size",
            "static_cast<size_t>(PAYLOAD_SIZE) <= launch.read_buffer.size()",
            "(!RAW_PAYLOAD && !RLE_PAYLOAD)",
            "launch.payload_flags = FLAGS;",
            "launch.read_target = static_cast<size_t>(PAYLOAD_SIZE);",
        ],
        "mandelbench RLE header validation",
    )

    fill_body = function_body(source, "fill_rgba_pixels")
    require_tokens(
        fill_body,
        [
            "size_t filled_pixels = 1;",
            "std::min(filled_pixels, pixel_count - filled_pixels)",
            "filled_pixels += COPY_PIXELS;",
        ],
        "mandelbench bounded RGBA run fill",
    )

    decode_body = function_body(source, "decode_worker_rgba_rle")
    require_tokens(
        decode_body,
        [
            "launch.payload_flags != WORKER_OUTPUT_FLAG_PAYLOAD_RLE",
            "launch.read_offset != launch.read_target",
            "launch.read_target > launch.read_buffer.size()",
            "(launch.expected_payload_size % WORKER_OUTPUT_RGBA_BYTES) != 0",
            "if (RUN_PIXELS == 0)",
            "RUN_PIXELS > EXPECTED_PIXELS - decoded_pixels",
            "while (remaining_run_pixels > 0)",
            "chunk_index >= launch.chunks.size()",
            "COPY_PIXELS = std::min(remaining_run_pixels, AVAILABLE_PIXELS)",
            "fill_rgba_pixels(image + IMAGE_OFFSET, COPY_PIXELS, COLOR);",
            "decoded_pixels != EXPECTED_PIXELS",
            "chunk_index != launch.chunks.size()",
        ],
        "mandelbench exact RLE decoder",
    )

    scatter_body = function_body(source, "scatter_worker_output")
    require_tokens(
        scatter_body,
        [
            "if (launch.output_scattered)",
            "launch.payload_flags == WORKER_OUTPUT_FLAG_PAYLOAD_RLE",
            "decode_worker_rgba_rle(launch, image, row_size)",
            "launch.payload_flags != WORKER_OUTPUT_FLAG_PAYLOAD",
            "launch.read_target != launch.expected_payload_size",
            "BYTES > launch.read_target - payload_offset",
            "payload_offset != launch.expected_payload_size",
            "launch.output_scattered = true;",
        ],
        "mandelbench idempotent raw and RLE scatter dispatch",
    )
    scatter_all_body = function_body(source, "scatter_worker_outputs")
    require_tokens(
        scatter_all_body,
        ["for (auto& launch : launches)", "scatter_worker_output(launch, image, row_size)"],
        "mandelbench final scatter validation",
    )

    worker_body = function_body(source, "mandelbench_worker")
    require_tokens(
        worker_body,
        [
            "bool const RLE_ENABLED = output_fd >= 0 && mandelbench_rle_enabled();",
            "worker_rgba_rle_worst_case_size(image.size(), rle_worst_case_size)",
            "rle_payload.reserve(rle_worst_case_size);",
            "std::span<const unsigned char> payload(image.data(), image.size());",
            "encode_worker_rgba_rle(image, rle_payload)",
            "if (rle_payload.size() < image.size())",
            "payload_flags = WORKER_OUTPUT_FLAG_PAYLOAD_RLE;",
            "make_worker_output_header(id, payload.size(), payload_flags, header)",
            "write_all(FD, payload, &write_fail_ret, &write_fail_errno)",
            "raw_bytes={} wire_bytes={}",
            "encode_ms={:.3f}",
        ],
        "mandelbench stream-only RLE worker",
    )
    require_order(worker_body, "rle_payload.reserve", "for (int repeat_offset", "mandelbench RLE reserve before timed repeats")
    require_order(worker_body, "COMPUTE_END_US = now_us();", "ENCODE_START_US = now_us();", "mandelbench compute before RLE encode")
    require_order(worker_body, "ENCODE_END_US = now_us();", "make_worker_output_header", "mandelbench encode before output header")
    require_order(worker_body, "make_worker_output_header", "write_all(FD, payload", "mandelbench header before selected payload")

    coordinator_body = function_body(source, "mandelbench_wki")
    require_order(
        coordinator_body,
        "scatter_worker_outputs(remote_launches, image.data(), ROW_SIZE)",
        "AFTER_MERGE_US = now_us();",
        "mandelbench RLE decode remains in timed interval",
    )


def test_mandelbench_overlaps_local_compute_with_remote_payload_drain() -> None:
    source = MANDELBENCH_WKI_CPP.read_text()
    execute_body = function_body(source, "execute_local_compute_task")
    thread_body = function_body(source, "run_local_compute_task")
    join_body = function_body(source, "join_local_compute_task")
    coordinator_body = function_body(source, "mandelbench_wki")

    require_tokens(
        source,
        [
            "struct LocalComputeTask",
            "std::span<const WorkerLaunch> launches;",
            "std::atomic<bool> finished{false};",
        ],
        "mandelbench local compute supervisor task",
    )
    require_tokens(
        execute_body,
        [
            "bool const OK = compute_local_launches(task.launches",
            "task.ok = OK;",
            "task.end_us = now_us();",
            "task.finished.store(true, std::memory_order_release);",
            "return OK;",
        ],
        "mandelbench local compute supervisor execution",
    )
    require_order(execute_body, "task.ok = OK;", "task.end_us = now_us();", "mandelbench supervisor result before completion time")
    require_order(
        execute_body,
        "task.end_us = now_us();",
        "task.finished.store(true, std::memory_order_release);",
        "mandelbench supervisor publishes completion after all task writes",
    )
    finished_token = "task.finished.store(true, std::memory_order_release);"
    finished_store = execute_body.find(finished_token)
    if "task." in execute_body[finished_store + len(finished_token) :]:
        fail("mandelbench supervisor must not access its stack task after publishing completion")
    require_tokens(
        thread_body,
        [
            "execute_local_compute_task(*task)",
            "EXIT_SUCCESS",
            "EXIT_FAILURE",
        ],
        "mandelbench local compute supervisor entry",
    )
    require_tokens(
        join_body,
        [
            "thrd_join(thread, &result)",
            "task.finished.load(std::memory_order_acquire)",
            "thrd_yield();",
        ],
        "mandelbench local compute supervisor join",
    )
    require_tokens(
        coordinator_body,
        [
            "bool const OVERLAP_LOCAL_AND_REMOTE = !local_launches.empty() && !remote_launches.empty();",
            "thrd_create(&local_compute_thread, run_local_compute_task, &local_compute_task)",
            "using synchronous fallback",
            "execute_local_compute_task(local_compute_task)",
            "PAYLOAD_RELEASE, image.data(), ROW_SIZE)",
            "join_local_compute_task(local_compute_thread, local_compute_task)",
            "if (!local_compute_ok || !WORKER_RESULT.ok)",
            "scatter_worker_outputs(remote_launches, image.data(), ROW_SIZE)",
        ],
        "mandelbench mixed-node compute and payload overlap",
    )
    require_order(
        coordinator_body,
        "thrd_create(&local_compute_thread, run_local_compute_task, &local_compute_task)",
        "complete_worker_payloads_streaming(",
        "mandelbench starts local compute before draining remote payloads",
    )
    require_order(
        coordinator_body,
        "complete_worker_payloads_streaming(",
        "join_local_compute_task(local_compute_thread, local_compute_task)",
        "mandelbench keeps remote child polling on the coordinator before joining local compute",
    )
    streaming_body = function_body(source, "complete_worker_payloads_streaming")
    require_tokens(
        streaming_body,
        [
            "if (READ_WAS_PENDING && !read_is_pending(launch))",
            "launch.read_ok && !scatter_worker_output(launch, image, row_size)",
            "concurrent local writes target",
            "disjoint bytes in the final image",
        ],
        "mandelbench per-worker result scatter overlap",
    )
    require_order(
        streaming_body,
        "if (READ_WAS_PENDING && !read_is_pending(launch))",
        "scatter_worker_output(launch, image, row_size)",
        "mandelbench scatters only after a complete worker read",
    )
    if "complete_worker_payloads_streaming" in thread_body:
        fail("mandelbench remote waitpid/payload draining must stay on the original fork-parent task")


def main() -> None:
    test_ping_receive_is_deadline_bounded()
    test_netbench_io_is_deadline_bounded()
    test_userland_suite_passes_netbench_timeout()
    test_userland_suite_cases_are_watchdog_bounded()
    test_userland_suite_can_request_guest_shutdown()
    test_wos_ssh_connect_path_is_bounded()
    test_perfbench_context_switch_counter_is_atomic()
    test_perfbench_parallel_workers_cleanup_before_failure_return()
    test_cowbench_child_wait_is_deadline_bounded()
    test_vfsbench_metadata_timing_excludes_setup_and_cleanup()
    test_mandelbench_worker_waits_use_monotonic_elapsed_time()
    test_mandelbench_auto_nodes_request_remote_wki_placement()
    test_mandelbench_coalesces_remote_workers_and_keeps_local_compute_local()
    test_mandelbench_worker_payload_rle_is_bounded_and_exact()
    test_mandelbench_overlaps_local_compute_with_remote_payload_drain()
    print("testprog ping, netbench, suite, perfbench, cowbench, and mandelbench waits are deadline bounded")


if __name__ == "__main__":
    main()
