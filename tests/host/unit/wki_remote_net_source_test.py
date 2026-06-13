#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_NET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.cpp"


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


def main() -> None:
    test_net_proxy_op_slot_wait_is_bounded()
    print("WKI remote net source invariants hold")


if __name__ == "__main__":
    main()
