#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TRANSPORT_ROCE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_roce.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:int|void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_roce_wait_deadlines_are_saturating() -> None:
    source = TRANSPORT_ROCE_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "wki_future_deadline_us(wki_now_us(), ROCE_RDMA_READ_TIMEOUT_US)",
            "wki_future_deadline_us(wki_now_us(), timeout_us)",
        ],
        "RoCE deadline helper use",
    )

    forbidden = [
        "wki_now_us() + ROCE_RDMA_READ_TIMEOUT_US",
        "wki_now_us() + timeout_us",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("RoCE waits must not use wrapping deadline arithmetic: " + ", ".join(present))


def test_temporary_read_completion_wait_is_deadline_bounded() -> None:
    body = function_body(TRANSPORT_ROCE_CPP.read_text(), "wait_for_temporary_region_completion")
    require_order(
        body,
        [
            "uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), ROCE_RDMA_READ_TIMEOUT_US)",
            "while (region_is_registered(rkey))",
            "if (wki_now_us() >= DEADLINE)",
            "return region_unregister(rkey) ? WKI_ERR_TIMEOUT : WKI_OK",
            "wki_spin_yield()",
        ],
        "temporary RoCE read completion wait",
    )


def test_registered_region_waits_use_shared_deadline_helper() -> None:
    source = TRANSPORT_ROCE_CPP.read_text()
    wait_received = function_body(source, "wki_roce_region_wait_received")
    wait_tagged = function_body(source, "wki_roce_region_wait_tagged_write")

    for name, body, completion_check in [
        ("wki_roce_region_wait_received", wait_received, "region_received_at_least(rkey, len)"),
        ("wki_roce_region_wait_tagged_write", wait_tagged, "region_tagged_write_received_at_least(rkey, cookie, len)"),
    ]:
        require_order(
            body,
            [
                "uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), timeout_us)",
                "while (wki_now_us() < DEADLINE)",
                completion_check,
                "napi_poll_all_pending()",
                "backlog_drain_all_pending_inline()",
            ],
            name,
        )


def main() -> None:
    test_roce_wait_deadlines_are_saturating()
    test_temporary_read_completion_wait_is_deadline_bounded()
    test_registered_region_waits_use_shared_deadline_helper()
    print("WKI RoCE source invariants hold")


if __name__ == "__main__":
    main()
