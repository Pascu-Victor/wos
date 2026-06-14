#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INIT_CPP = ROOT / "modules" / "init" / "src" / "init.cpp"
SERVICES_CPP = ROOT / "modules" / "init" / "src" / "services.cpp"
NETWORK_CPP = ROOT / "modules" / "init" / "src" / "network.cpp"


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


def test_dropbear_keygen_wait_is_deadline_bounded() -> None:
    source = SERVICES_CPP.read_text()
    require_tokens(
        source,
        [
            "DROPBEAR_KEYGEN_TIMEOUT_MS",
            "CHILD_WAIT_POLL_US",
            "wait_for_child_timeout",
            "reap_child_after_timeout",
            "ker::process::kill(pid, SIGKILL)",
        ],
        "init service timeout surface",
    )
    if "ker::process::waitpid(static_cast<int64_t>(KEYGEN_PID), &exit_code, 0, nullptr)" in source:
        fail("dropbear key generation must not block init with waitpid(..., 0)")

    wait_body = function_body(source, "wait_for_child_timeout")
    require_tokens(
        wait_body,
        [
            "ker::process::waitpid(pid, status, WNOHANG, nullptr)",
            "START_NS != 0 && monotonic_ns() - START_NS <= TIMEOUT_NS",
            "START_NS == 0 && waited_us <= static_cast<uint64_t>(timeout_ms) * 1000ULL",
            "usleep(CHILD_WAIT_POLL_US)",
            "reap_child_after_timeout(pid)",
        ],
        "init child wait timeout helper",
    )

    reap_body = function_body(source, "reap_child_after_timeout")
    require_tokens(
        reap_body,
        [
            "ker::process::kill(pid, SIGKILL)",
            "ker::process::waitpid(pid, &reap_status, WNOHANG, nullptr)",
            "usleep(CHILD_WAIT_POLL_US)",
        ],
        "init child timeout reap helper",
    )

    dropbear_body = function_body(source, "start_dropbear")
    require_tokens(
        dropbear_body,
        [
            "wait_for_child_timeout(static_cast<int64_t>(KEYGEN_PID), &exit_code, DROPBEAR_KEYGEN_TIMEOUT_MS)",
            "dropbearkey did not exit within %ums; continuing boot",
        ],
        "dropbear keygen timeout plumbing",
    )


def test_network_startup_poll_is_deadline_bounded() -> None:
    source = NETWORK_CPP.read_text()
    require_tokens(
        source,
        [
            "POLL_FAILURE_TIMEOUT_SECS",
            "NETD_KILL_REAP_RETRIES",
            "terminate_netd_after_startup_timeout",
            "ker::process::kill(netd_pid, SIGKILL)",
        ],
        "init network timeout surface",
    )
    start_body = function_body(source, "start_network")
    require_tokens(
        start_body,
        [
            "ELAPSED_SECS >= POLL_FAILURE_TIMEOUT_SECS",
            "eth0 not configured after %ld seconds; failing network startup",
            "eth0 did not receive a non-zero IPv4 address before the startup timeout",
            "terminate_netd_after_startup_timeout(static_cast<int64_t>(NETD_PID))",
            "close(POLL_SOCK)",
            "return false",
        ],
        "init network startup timeout path",
    )

    timeout_body = function_body(source, "terminate_netd_after_startup_timeout")
    require_tokens(
        timeout_body,
        [
            "ker::process::kill(netd_pid, SIGKILL)",
            "ker::process::waitpid(netd_pid, &status, WNOHANG, nullptr)",
            "REAPED == netd_pid || (REAPED < 0 && REAPED != -EINTR)",
            "nanosleep(&POLL_SLEEP, nullptr)",
        ],
        "init network netd timeout reap",
    )


def test_pivot_root_retries_transient_busy_mount_refs() -> None:
    source = INIT_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <cerrno>",
            "PIVOT_ROOT_MAX_ATTEMPTS",
            "PIVOT_ROOT_RETRY_MS",
            "pivot_root_with_busy_retry",
            "ret = ker::abi::vfs::pivot_root_vfs(ROOTFS_MOUNTPOINT, OLD_ROOT_MOUNTPOINT)",
            "if (ret != -EBUSY)",
            "init_log::warn(\"init[%llu]: pivot_root busy (attempt %d/%d), retrying\"",
            "sleep_ms(PIVOT_ROOT_RETRY_MS)",
            "int const PIVOT_RET = pivot_root_with_busy_retry(CPUNO)",
            "pivot_root failed (ret=%d), continuing with initramfs root",
        ],
        "init pivot_root transient busy retry",
    )


def main() -> None:
    test_pivot_root_retries_transient_busy_mount_refs()
    test_dropbear_keygen_wait_is_deadline_bounded()
    test_network_startup_poll_is_deadline_bounded()
    print("init dropbear key generation and network readiness waits are deadline bounded")


if __name__ == "__main__":
    main()
