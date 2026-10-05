#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROCESS_TIMER_CPP = ROOT / "modules/kern/src/platform/ktime/process_timer.cpp"
PROCESS_TIMER_HPP = ROOT / "modules/kern/src/platform/ktime/process_timer.hpp"
SCHEDULER_CPP = ROOT / "modules/kern/src/platform/sched/scheduler.cpp"


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        raise AssertionError(f"{context}: missing {token}")


def main() -> None:
    source = PROCESS_TIMER_CPP.read_text()
    header = PROCESS_TIMER_HPP.read_text()
    scheduler = SCHEDULER_CPP.read_text()

    if "POLL_US" in source:
        raise AssertionError("process timer worker still uses a fixed polling cadence")

    for token in [
        "real_queue.next_deadline(deadline)",
        "sched::kern_sleep_us(WAIT.delay_us)",
        "sched::kern_block()",
        "deferred_wake_requested.store(true, std::memory_order_release)",
        "deferred_wake_requested.exchange(false, std::memory_order_acq_rel)",
        "sched::wake_task_from_event(worker, sched::EventWakeDeferredSwitch::PRESERVE)",
    ]:
        require(source, token, "event-driven process timer worker")

    require(header, "void service_deferred_wake();", "process timer deferred-wake API")
    require(
        scheduler,
        "time::process_timer::service_deferred_wake();",
        "scheduler runqueue-safe process timer wake",
    )

    print("process timer event-driven wake path is source covered")


if __name__ == "__main__":
    main()
