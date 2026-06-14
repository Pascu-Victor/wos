#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
THREADING_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.cpp"
THREADING_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
SCHEDULER_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "scheduler_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|Thread\*)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_event_wake_cancel_preserves_current_task_wakeup_token() -> None:
    source = SCHEDULER_CPP.read_text()
    wake_body = function_body(source, "wake_task_from_event_on_cpu")
    reschedule_body = function_body(source, "reschedule_task_for_cpu")

    require_tokens(
        wake_body,
        [
            "bool const CANCEL_DEFERRED_SWITCH = event_wake_cancels_deferred_switch(deferred_switch)",
            "task->deferred_task_switch = false",
            "reschedule_task_for_cpu(cpu, task)",
        ],
        "event wake cancellation mode",
    )
    require_order(
        wake_body,
        "task->deferred_task_switch = false",
        "reschedule_task_for_cpu(cpu, task)",
        "deferred switch must be cleared before the reschedule decision",
    )
    if "CurrentTaskWakeupPending" in source:
        fail("current-task event wakes must not select a mode that clears wakeup_pending")

    current_task_branch = reschedule_body[reschedule_body.find("if (is_current_on_some_cpu)") :]
    current_task_branch = current_task_branch[: current_task_branch.find("uint64_t const NOW_US")]
    require_tokens(
        current_task_branch,
        [
            "task->wakeup_pending.store(true, std::memory_order_release)",
        ],
        "reschedule current-task wake token handling",
    )
    if "task->wakeup_pending.store(false" in current_task_branch:
        fail("current-task event wakes must preserve wakeup_pending, even when deferred switch is cancelled")


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


def test_user_thread_tcbs_publish_nonzero_tid_before_user_execution() -> None:
    threading_source = THREADING_CPP.read_text()
    threading_header = THREADING_HPP.read_text()
    task_source = TASK_CPP.read_text()
    exec_source = EXEC_CPP.read_text()
    thread_control_source = THREAD_CONTROL_CPP.read_text()

    require_tokens(
        threading_header,
        ["create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table, uint64_t initial_tid"],
        "threading create_thread TID contract",
    )

    create_thread_body = function_body(threading_source, "create_thread")
    require_tokens(
        create_thread_body,
        [
            "tcb_i32[6] = static_cast<uint32_t>(initial_tid);",
            "used by mlibc futex locks",
        ],
        "initial user TCB tid publish",
    )
    if "tcb_i32[6] = 0" in create_thread_body:
        fail("initial user TCB tid must not be left as zero; mlibc futex locks use zero as unlocked")

    require_tokens(
        task_source,
        [
            "static uint64_t next_pid = 1",
            "this->pid = sched::task::get_next_pid();",
            "threading::create_thread(ker::mod::mm::USER_STACK_SIZE, ACTUAL_TLS_INFO.tls_size, this->pagemap, this->pid",
        ],
        "fresh process TCB tid publish",
    )
    require_order(
        task_source,
        "this->pid = sched::task::get_next_pid();",
        "threading::create_thread(ker::mod::mm::USER_STACK_SIZE, ACTUAL_TLS_INFO.tls_size, this->pagemap, this->pid",
        "fresh process must allocate a PID before building the initial TCB",
    )

    require_tokens(
        exec_source,
        [
            "mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, TLS_INFO.tls_size, new_pagemap, task->pid",
        ],
        "execve replacement TCB tid publish",
    )

    require_order(
        thread_control_source,
        "publish_thread_tid_to_tcb(parent, tcb_va, t->pid)",
        "mod::sched::post_task_for_cpu(TARGET_CPU, t)",
        "secondary pthread TCB tid must be published before scheduling",
    )


def main() -> None:
    test_event_wake_cancel_preserves_current_task_wakeup_token()
    test_runtime_accounting_deltas_are_saturating()
    test_user_thread_tcbs_publish_nonzero_tid_before_user_execution()
    print("scheduler wake-token and runtime accounting invariants hold")


if __name__ == "__main__":
    main()
