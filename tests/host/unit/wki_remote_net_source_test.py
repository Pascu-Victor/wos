#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_NET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.cpp"
REMOTE_NET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.hpp"
REMOTE_NET_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_remote_net_ktest.cpp"


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


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def braced_block_after(source: str, token: str) -> str:
    start = source.find(token)
    if start < 0:
        fail(f"missing block token {token!r}")

    brace = source.find("{", start)
    if brace < 0:
        fail(f"missing block after {token!r}")

    depth = 1
    pos = brace + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated block after {token!r}")
    return source[brace + 1 : pos - 1]


def test_net_proxy_op_slot_wait_is_bounded() -> None:
    source = REMOTE_NET_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "constexpr uint64_t NET_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_OP_TIMEOUT_US;",
            "auto acquire_net_op_slot_locked(ProxyNetState* state, uint64_t start_us) -> int",
        ],
        "remote net slot timeout scaffolding",
    )

    acquire_body = function_body(source, "acquire_net_op_slot_locked")
    require_tokens(
        acquire_body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, NET_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->lock.lock()",
            "state->retiring.load(std::memory_order_acquire)",
            "return WKI_ERR_PEER_FENCED",
            "net_proxy_op_slot_busy(state) && net_stats_poll_owns_slot(state)",
            "clear_net_op_state_locked(state, WKI_ERR_TIMEOUT)",
            "if (!net_proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "continue",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US)",
        ],
        "remote net slot acquisition",
    )
    require_order(acquire_body, "state->lock.lock()", "return WKI_OK", "slot lock success path")
    require_order(acquire_body, "state->lock.unlock()", "return WKI_ERR_PEER_FENCED", "fence path unlock")
    require_order(
        acquire_body,
        "state->lock.unlock();\n        if (!net_proxy_op_slot_busy(state))",
        "if (wki_now_us() >= DEADLINE_US)",
        "slot timeout path unlock",
    )

    prepare_body = function_body(source, "prepare_net_op_wait")
    require_order(
        prepare_body,
        "int const SLOT_RET = acquire_net_op_slot_locked(state, wki_now_us())",
        "uint16_t const COOKIE = allocate_net_op_cookie_locked(state)",
        "prepare wait allocates cookie only after slot acquisition",
    )
    require_order(prepare_body, "if (SLOT_RET != WKI_OK)", "return SLOT_RET", "prepare wait propagates slot timeout")
    require_order(
        prepare_body,
        "uint16_t const COOKIE = allocate_net_op_cookie_locked(state)",
        "state->op_wait_entry = &wait",
        "prepare wait publishes only after cookie allocation",
    )
    if "ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US)" in prepare_body:
        fail("prepare_net_op_wait must not contain its own unbounded contention sleep loop")


def test_cancel_op_waiter_preserves_successor_slot() -> None:
    source = REMOTE_NET_CPP.read_text()
    body = function_body(source, "cancel_op_waiter")
    owner_block = braced_block_after(body, "if (state->op_wait_entry == &wait)")

    require_tokens(
        owner_block,
        [
            "state->op_wait_entry = nullptr",
            "clear_net_op_state_locked(state, result)",
        ],
        "remote net cancel owned slot cleanup",
    )
    if body.count("clear_net_op_state_locked(state, result)") != 1:
        fail("cancel_op_waiter must clear op state only when the canceled waiter still owns the slot")
    require_order(
        body,
        "if (state->op_wait_entry == &wait)",
        "claimed = wki_claim_op(&wait)",
        "cancel still completes the canceled waiter after ownership check",
    )


def test_cancel_op_waiter_has_ktest_coverage() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()
    ktest = REMOTE_NET_KTEST.read_text()

    require_tokens(
        source,
        ["auto wki_remote_net_selftest_cancel_preserves_successor_op() -> bool"],
        "remote net cancel selftest implementation",
    )
    require_tokens(
        header,
        ["auto wki_remote_net_selftest_cancel_preserves_successor_op() -> bool;"],
        "remote net cancel selftest declaration",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteNetOpCancel, PreservesSuccessorSlot)",
            "wki_remote_net_selftest_cancel_preserves_successor_op()",
        ],
        "remote net cancel KTEST coverage",
    )


def main() -> None:
    test_net_proxy_op_slot_wait_is_bounded()
    test_cancel_op_waiter_preserves_successor_slot()
    test_cancel_op_waiter_has_ktest_coverage()
    print("WKI remote net source invariants hold")


if __name__ == "__main__":
    main()
