#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
REMOTE_NET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z_:<>]+)?\s*\{{", source)
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


def test_remote_net_stats_poll_is_scheduled() -> None:
    source = WKI_CPP.read_text()
    deferred_body = function_body(source, "process_deferred_blocking_work")
    timer_body = function_body(source, "wki_timer_tick")

    if "wki_remote_net_poll_stats();" not in deferred_body:
        fail("process_deferred_blocking_work() must schedule remote NET stats polling")
    if "process_deferred_blocking_work();" not in timer_body:
        fail("wki_timer_tick() must run deferred WKI maintenance work")


def test_deferred_work_reentrancy_guard_prevents_recursive_blocking_work() -> None:
    source = WKI_CPP.read_text()
    require_tokens(
        source,
        [
            "std::atomic<bool> s_timer_deferred_running{false}",
            "std::atomic<mod::sched::task::Task*> s_timer_deferred_task{nullptr}",
        ],
        "WKI timer deferred guard globals",
    )

    try_enter_body = braced_block_after(source, "bool try_enter()")
    require_order(
        try_enter_body,
        [
            "bool expected = false",
            "s_timer_deferred_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)",
            "s_timer_deferred_task.store(mod::sched::get_current_task(), std::memory_order_release)",
        ],
        "DeferredGuard try_enter publish order",
    )

    destructor_body = braced_block_after(source, "~DeferredGuard()")
    require_order(
        destructor_body,
        [
            "if (entered)",
            "s_timer_deferred_task.store(nullptr, std::memory_order_release)",
            "s_timer_deferred_running.store(false, std::memory_order_release)",
        ],
        "DeferredGuard destructor release order",
    )

    waiter_body = function_body(source, "is_timer_deferred_waiter")
    require_tokens(
        waiter_body,
        [
            "task != nullptr",
            "s_timer_deferred_task.load(std::memory_order_acquire) == task",
        ],
        "timer deferred waiter identity check",
    )

    deferred_body = function_body(source, "process_deferred_blocking_work")
    require_order(
        deferred_body,
        [
            "DeferredGuard deferred",
            "if (!deferred.try_enter())",
            "return",
            "wki_dev_server_process_pending_zones()",
            "wki_remotable_process_pending_mounts()",
            "wki_remotable_process_pending_net_attaches()",
        ],
        "deferred blocking work guarded entry",
    )


def test_deferred_waiter_uses_spin_yield_instead_of_self_blocking() -> None:
    wait_body = function_body(WKI_CPP.read_text(), "wki_wait_for_op")
    require_order(
        wait_body,
        [
            "auto* waiter_task = entry->task.load(std::memory_order_acquire)",
            "waiter_task->type == mod::sched::task::TaskType::DAEMON && !is_timer_deferred_waiter(waiter_task)",
            "mod::sched::kern_block()",
            "if (is_timer_deferred_waiter(waiter_task))",
            "wki_spin_yield()",
            "mod::sched::kern_yield()",
        ],
        "deferred WKI waiter avoids recursive self-block",
    )


def test_remote_net_stats_poll_has_cadence_guard() -> None:
    source = REMOTE_NET_CPP.read_text()
    poll_body = function_body(source, "wki_remote_net_poll_stats")

    required_tokens = [
        "wki_remote_net_stats_poll_due",
        "g_last_net_stats_poll_us",
        "g_last_net_stats_poll_us = NOW_US;",
        "OP_NET_GET_STATS",
    ]
    missing = [token for token in required_tokens if token not in poll_body]
    if missing:
        fail("wki_remote_net_poll_stats() is missing expected cadence/request tokens: " + ", ".join(missing))


def main() -> None:
    test_remote_net_stats_poll_is_scheduled()
    test_deferred_work_reentrancy_guard_prevents_recursive_blocking_work()
    test_deferred_waiter_uses_spin_yield_instead_of_self_blocking()
    test_remote_net_stats_poll_has_cadence_guard()
    print("WKI timer source invariants hold")


if __name__ == "__main__":
    main()
