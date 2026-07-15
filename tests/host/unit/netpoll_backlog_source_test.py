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

    has_work_body = function_body(source, "napi_worker_has_work")
    require_order(
        has_work_body,
        [
            "napi->has_work.load(std::memory_order_acquire)",
            "napi->state.load(std::memory_order_acquire) != NapiState::SCHEDULED",
            "napi->has_work.store(true, std::memory_order_release)",
        ],
        "NAPI worker state/work reconciliation",
    )
    worker_body = function_body(source, "napi_worker_loop")
    require_order(
        worker_body,
        [
            "!napi_worker_has_work(napi)",
            "ker::mod::sched::kern_sleep_us(NAPI_WORKER_IDLE_SLEEP_US)",
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

    poke_body = function_body(source, "poke_napi_worker_cpu")
    require_tokens(
        poke_body,
        [
            "worker->cpu",
            "ker::mod::cpu::current_cpu()",
            "ker::mod::sys::context_switch::request_reschedule()",
            "ker::mod::sched::wake_cpu(WORKER_CPU,",
        ],
        "NAPI same-CPU worker reschedule poke",
    )

    wake_body = function_body(source, "wake_napi_worker")
    require_tokens(
        wake_body,
        [
            "ker::mod::sched::kern_wake(worker)",
            "poke_napi_worker_cpu(worker)",
        ],
        "NAPI worker wake helper",
    )

    schedule_body = function_body(source, "napi_schedule")
    require_tokens(
        schedule_body,
        [
            "compare_exchange_strong(expected, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)",
            "napi->has_work.store(true, std::memory_order_release)",
            "wake_napi_worker(napi)",
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
            "wake_napi_worker(napi)",
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
            "prepare_backlog_batch",
            "q.depth.fetch_sub(static_cast<uint64_t>(queue_drained), std::memory_order_relaxed)",
            "split_backlog_batch(reverse_packet_list(batch), normal_head, wki_head)",
            "try_acquire_backlog_consumer",
            "q.consumer_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)",
            "wake_backlog_handler",
            "q.handler == nullptr",
            "release_backlog_consumer",
            "q.consumer_active.store(false, std::memory_order_release)",
            "q.head.load(std::memory_order_acquire) != nullptr",
            "wake_backlog_handler(q, ker::mod::sched::WakeCpuMode::FORCE)",
        ],
        "backlog consumer guard and batch preparation helpers",
    )
    wake_body = function_body(source, "wake_backlog_handler")
    require_tokens(
        wake_body,
        [
            "ker::mod::sched::wake_task_from_event(q.handler, ker::mod::sched::EventWakeDeferredSwitch::PRESERVE)",
            "q.handler->cpu == ker::mod::cpu::current_cpu()",
            "ker::mod::sys::context_switch::request_reschedule()",
            "ker::mod::sched::wake_cpu(q.handler->cpu,",
            "cpu_wake_mode",
        ],
        "backlog handler wake helper",
    )
    if "kern_wake(q.handler)" in wake_body:
        fail("backlog handler wake helper: generic wake can lose an event-before-park token")
    require_order(
        wake_body,
        [
            "ker::mod::sched::wake_task_from_event(q.handler, ker::mod::sched::EventWakeDeferredSwitch::PRESERVE)",
            "q.handler->cpu == ker::mod::cpu::current_cpu()",
            "ker::mod::sys::context_switch::request_reschedule()",
            "ker::mod::sched::wake_cpu(q.handler->cpu,",
        ],
        "backlog event token precedes local/remote CPU pokes",
    )

    handler_body = function_body(source, "backlog_handler_loop")
    require_order(
        handler_body,
        [
            "if (!try_acquire_backlog_consumer(q))",
            "ker::mod::sched::kern_sleep_us(BACKLOG_CONSUMER_CONTENTION_SLEEP_US)",
            "continue;",
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
            "int const QUEUE_DRAINED = prepare_backlog_batch(batch, q, normal_head, wki_head)",
            "process_packet_list(normal_head, true)",
            "release_backlog_consumer(q)",
            "process_packet_list(wki_head, true)",
        ],
        "backlog handler releases consumer before WKI post-processing",
    )

    push_body = function_body(source, "push_backlog_packet")
    require_order(
        push_body,
        [
            "q.depth.fetch_add(1, std::memory_order_relaxed)",
            "q.head.compare_exchange_weak(old_head, pkt, std::memory_order_release, std::memory_order_relaxed)",
            "return old_head == nullptr",
        ],
        "backlog MPSC publish returns the successful-CAS empty transition",
    )
    wake_mode_body = function_body(source, "backlog_wake_mode_for_enqueue")
    require_order(
        wake_mode_body,
        [
            "queue_was_empty",
            "ker::mod::sched::WakeCpuMode::FORCE",
            "ker::mod::sched::WakeCpuMode::COALESCE",
        ],
        "backlog enqueue wake mode follows the successful-CAS transition",
    )
    enqueue_body = function_body(source, "backlog_enqueue")
    require_order(
        enqueue_body,
        [
            "bool const QUEUE_WAS_EMPTY = push_backlog_packet(q, pkt)",
            "wake_backlog_handler(q, backlog_wake_mode_for_enqueue(QUEUE_WAS_EMPTY))",
        ],
        "backlog enqueue coalesces only the remote CPU poke for an existing batch",
    )

    inline_body = function_body(source, "drain_backlog_queue_inline")
    require_tokens(
        inline_body,
        [
            "if (!try_acquire_backlog_consumer(q))",
            "q.head.exchange(nullptr, std::memory_order_acquire)",
            "release_backlog_consumer(q)",
            "prepare_backlog_batch(batch, q, normal_head, wki_head)",
            "process_packet_list(normal_head, cooperative)",
            "process_packet_list(wki_head, cooperative)",
        ],
        "backlog inline drain acquire ownership guard",
    )
    require_order(
        inline_body,
        [
            "int const QUEUE_DRAINED = prepare_backlog_batch(batch, q, normal_head, wki_head)",
            "process_packet_list(normal_head, cooperative)",
            "release_backlog_consumer(q)",
            "process_packet_list(wki_head, cooperative)",
        ],
        "backlog inline drain releases consumer before WKI post-processing",
    )

    drain_all_body = function_body(source, "backlog_drain_all_pending_inline")
    require_order(
        drain_all_body,
        [
            "ready.load(std::memory_order_acquire)",
            "drained += drain_backlog_queue_inline(queues[cpu_idx], false)",
        ],
        "backlog rescue drains each queue through the guarded inline consumer",
    )


def main() -> None:
    test_napi_worker_and_scheduler_lost_wake_guards()
    test_napi_inline_poll_rearms_on_races_and_budget_exhaustion()
    test_backlog_handler_and_enqueue_lost_wake_guards()
    print("netpoll/backlog lost-wake guards are source covered")


if __name__ == "__main__":
    main()
