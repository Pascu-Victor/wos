#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SCHEDULER_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "scheduler_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_event_wake_cancel_clears_current_task_wakeup_token() -> None:
    source = SCHEDULER_CPP.read_text()
    wake_body = function_body(source, "wake_task_from_event_on_cpu")
    reschedule_body = function_body(source, "reschedule_task_for_cpu_impl")
    wrapper_body = function_body(source, "reschedule_task_for_cpu")

    require_tokens(
        source,
        [
            "enum class CurrentTaskWakeupPending",
            "RECORD",
            "CLEAR",
            "reschedule_task_for_cpu_impl(uint64_t cpu_no, task::Task* task, CurrentTaskWakeupPending current_task_wakeup_pending)",
        ],
        "scheduler current-task wake token mode",
    )
    require_tokens(
        wake_body,
        [
            "bool const CANCEL_DEFERRED_SWITCH = event_wake_cancels_deferred_switch(deferred_switch)",
            "task->deferred_task_switch = false",
            "CANCEL_DEFERRED_SWITCH ? CurrentTaskWakeupPending::CLEAR : CurrentTaskWakeupPending::RECORD",
        ],
        "event wake cancellation mode",
    )
    require_order(
        wake_body,
        "task->deferred_task_switch = false",
        "reschedule_task_for_cpu_impl(cpu, task",
        "deferred switch must be cleared before the reschedule decision",
    )
    require_tokens(
        reschedule_body,
        [
            "if (current_task_wakeup_pending == CurrentTaskWakeupPending::RECORD)",
            "task->wakeup_pending.store(true, std::memory_order_release)",
            "task->wakeup_pending.store(false, std::memory_order_release)",
            "task->wants_block = false",
            "task->wake_at_us = 0",
        ],
        "reschedule current-task wake token handling",
    )
    require_order(
        reschedule_body,
        "task->wakeup_pending.store(false, std::memory_order_release)",
        "task->wants_block = false",
        "cancelled current-task wake must discard token before clearing block metadata",
    )
    require_tokens(
        wrapper_body,
        ["reschedule_task_for_cpu_impl(cpu_no, task, CurrentTaskWakeupPending::RECORD)"],
        "public reschedule wrapper preserves wake token behavior",
    )


def test_runtime_accounting_deltas_are_saturating() -> None:
    source = SCHEDULER_CPP.read_text()
    ktest_source = SCHEDULER_KTEST.read_text()

    require_tokens(
        source,
        [
            "auto runtime_delta_ns_from_us(uint64_t delta_us) -> uint64_t",
            "if (delta_us > UINT64_MAX / 1000ULL)",
            "auto vruntime_delta_from_runtime_ns(uint64_t delta_ns, uint32_t sched_weight) -> int64_t",
            "auto accumulate_slice_used_ns(uint32_t used_ns, uint64_t delta_ns, uint32_t slice_ns) -> uint32_t",
            "add_weighted_vruntime_delta(rq, VRUNTIME_DELTA, current_task->sched_weight)",
            "current_task->slice_used_ns = accumulate_slice_used_ns(current_task->slice_used_ns, DELTA_NS, current_task->slice_ns)",
            "scheduler_selftest_runtime_delta_saturates",
        ],
        "scheduler runtime saturation helpers",
    )
    forbidden = [
        "static_cast<int64_t>(chargeable_us) * 1000",
        "static_cast<uint32_t>(DELTA_NS)",
        "DELTA_NS * 1024",
        "VRUNTIME_DELTA * static_cast<int64_t>(current_task->sched_weight)",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail(f"scheduler runtime accounting still has wrapping/narrowing expression: {present[0]}")

    require_tokens(
        ktest_source,
        [
            "KTEST(SchedulerRuntime, RuntimeDeltaSaturates)",
            "scheduler_selftest_runtime_delta_saturates",
        ],
        "scheduler runtime KTEST",
    )


def main() -> None:
    test_event_wake_cancel_clears_current_task_wakeup_token()
    test_runtime_accounting_deltas_are_saturating()
    print("scheduler wake-token and runtime accounting invariants hold")


if __name__ == "__main__":
    main()
