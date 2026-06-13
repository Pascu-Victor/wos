#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTABLE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remotable.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_deferred_retry_deadlines_are_saturating() -> None:
    source = REMOTABLE_CPP.read_text()
    if "#include <net/wki/timer_math.hpp>" not in source:
        fail("remotable retry scheduling must use the shared WKI timer helpers")

    net_requeue = function_body(source, "requeue_net_attach")
    require_order(
        net_requeue,
        [
            "pending.retry_count++",
            "delay_us = std::min(delay_us, NET_AUTO_ATTACH_RETRY_MAX_US)",
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us)",
            "queue_net_attach_locked",
        ],
        "NET auto-attach retry backoff",
    )

    local_ipv4_defer = function_body(source, "defer_net_attach_for_local_ipv4")
    require_order(
        local_ipv4_defer,
        [
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US)",
            "queue_net_attach_locked",
        ],
        "NET auto-attach local IPv4 defer",
    )

    mount_process = function_body(source, "wki_remotable_process_pending_mounts")
    require_order(
        mount_process,
        [
            "pending.retry_count++",
            "delay_us = std::min(delay_us, VFS_AUTO_MOUNT_RETRY_MAX_US)",
            "pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us)",
            "g_pending_vfs_mounts.push_back(pending)",
        ],
        "VFS auto-mount retry backoff",
    )

    forbidden = [
        "pending.next_attempt_us = wki_now_us() + delay_us",
        "pending.next_attempt_us = wki_now_us() + NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("remotable retry scheduling must not use wrapping deadline arithmetic: " + ", ".join(present))


def main() -> None:
    test_deferred_retry_deadlines_are_saturating()
    print("WKI remotable source invariants hold")


if __name__ == "__main__":
    main()
