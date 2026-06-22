#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NETPOLL_CPP = ROOT / "modules" / "kern" / "src" / "net" / "netpoll.cpp"
BACKLOG_CPP = ROOT / "modules" / "kern" / "src" / "net" / "backlog.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:\[\[noreturn\]\]\s*)?(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_napi_worker_and_scheduler_lost_wake_guards() -> None:
    source = NETPOLL_CPP.read_text()

    worker_body = function_body(source, "napi_worker_loop")
    require_order(
        worker_body,
        [
            "!napi->has_work.load(std::memory_order_acquire)",
            "ker::mod::sched::kern_block()",
            "napi->has_work.store(false, std::memory_order_release)",
            "napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED",
            "napi->has_work.store(true, std::memory_order_release)",
        ],
        "NAPI worker clear/recheck lost-wake guard",
    )
    require_tokens(
        worker_body,
        [
            "NAPI_WORKER_MAX_FULL_BUDGET_POLLS",
            "ker::mod::sched::kern_yield()",
        ],
        "NAPI worker livelock yield guard",
    )

    schedule_body = function_body(source, "napi_schedule")
    require_tokens(
        schedule_body,
        [
            "compare_exchange_strong(expected, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)",
            "napi->has_work.store(true, std::memory_order_release)",
            "ker::mod::sched::kern_wake(napi->worker)",
            "napi->worker->cpu == ker::mod::cpu::current_cpu()",
            "ker::mod::sys::context_switch::request_reschedule()",
            "ker::mod::sched::wake_cpu(napi->worker->cpu)",
        ],
        "NAPI scheduler wake/reschedule guard",
    )


def test_napi_inline_poll_rearms_on_races_and_budget_exhaustion() -> None:
    body = function_body(NETPOLL_CPP.read_text(), "napi_poll_struct_inline_budget")

    require_tokens(
        body,
        [
            "compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)",
            "napi->state.compare_exchange_strong(exp, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)",
            "napi->has_work.store(true, std::memory_order_release)",
            "ker::mod::sched::wake_cpu(napi->worker->cpu)",
        ],
        "NAPI inline budget exhaustion rearm",
    )
    require_order(
        body,
        [
            "napi->has_work.store(false, std::memory_order_release)",
            "napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED",
            "napi->has_work.store(true, std::memory_order_release)",
        ],
        "NAPI inline poll clear/recheck guard",
    )


def test_backlog_handler_and_enqueue_lost_wake_guards() -> None:
    source = BACKLOG_CPP.read_text()
    header = (ROOT / "modules" / "kern" / "src" / "net" / "backlog.hpp").read_text()

    require_tokens(
        header,
        [
            "std::atomic<bool> consumer_active{false}",
        ],
        "backlog per-queue consumer guard",
    )
    require_tokens(
        source,
        [
            "try_acquire_backlog_consumer",
            "q.consumer_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)",
            "release_backlog_consumer",
            "q.consumer_active.store(false, std::memory_order_release)",
        ],
        "backlog consumer guard helpers",
    )

    handler_body = function_body(source, "backlog_handler_loop")
    require_order(
        handler_body,
        [
            "if (!try_acquire_backlog_consumer(q))",
            "q.head.exchange(nullptr, std::memory_order_acquire)",
            "release_backlog_consumer(q)",
            "q.handler_active.store(false, std::memory_order_seq_cst)",
            "q.head.load(std::memory_order_acquire) != nullptr",
            "q.handler_active.store(true, std::memory_order_relaxed)",
            "continue;",
            "ker::mod::sched::kern_block()",
            "q.handler_active.store(true, std::memory_order_relaxed)",
        ],
        "backlog handler clear/recheck lost-wake guard",
    )
    require_order(
        handler_body,
        [
            "q.depth.fetch_sub(packet_list_count(batch), std::memory_order_relaxed)",
            "process_backlog_batch(batch)",
            "release_backlog_consumer(q)",
        ],
        "backlog handler releases consumer after batch",
    )

    enqueue_body = function_body(source, "backlog_enqueue")
    require_tokens(
        enqueue_body,
        [
            "q.head.compare_exchange_weak(old_head, pkt, std::memory_order_release, std::memory_order_relaxed)",
            "ker::mod::sched::kern_wake(q.handler)",
            "q.handler->cpu == ker::mod::cpu::current_cpu()",
            "ker::mod::sys::context_switch::request_reschedule()",
            "ker::mod::sched::wake_cpu(q.handler->cpu)",
        ],
        "backlog enqueue wake/reschedule guard",
    )

    inline_body = function_body(source, "backlog_drain_all_pending_inline")
    require_tokens(
        inline_body,
        [
            "ready.load(std::memory_order_acquire)",
            "if (!try_acquire_backlog_consumer(q))",
            "q.head.exchange(nullptr, std::memory_order_acquire)",
            "release_backlog_consumer(q)",
            "q.depth.fetch_sub(static_cast<uint64_t>(queue_drained), std::memory_order_relaxed)",
            "process_backlog_batch(batch)",
        ],
        "backlog inline drain acquire ownership guard",
    )
    require_order(
        inline_body,
        [
            "q.depth.fetch_sub(static_cast<uint64_t>(queue_drained), std::memory_order_relaxed)",
            "process_backlog_batch(batch)",
            "release_backlog_consumer(q)",
        ],
        "backlog inline drain releases consumer after batch",
    )


def main() -> None:
    test_napi_worker_and_scheduler_lost_wake_guards()
    test_napi_inline_poll_rearms_on_races_and_budget_exhaustion()
    test_backlog_handler_and_enqueue_lost_wake_guards()
    print("netpoll/backlog lost-wake guards are source covered")


if __name__ == "__main__":
    main()
