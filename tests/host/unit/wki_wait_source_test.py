#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_cleanup_publishes_only_when_it_claims_waiter() -> None:
    cleanup_body = function_body(WKI_CPP.read_text(), "wki_wait_cleanup_for_task")
    require_tokens(
        cleanup_body,
        [
            "uint8_t expected = WkiWaitEntry::PENDING",
            "cur->state.compare_exchange_strong(expected, WkiWaitEntry::CLAIMED",
            "std::memory_order_acq_rel",
            "std::memory_order_acquire",
            "cur->task.store(nullptr, std::memory_order_release)",
            "publish_claimed_wait_entry(cur, WKI_ERR_PEER_FENCED)",
            "else if (expected == WkiWaitEntry::CLAIMED)",
            "claimed_waiter = cur",
            "s_wait_lock.unlock()",
            "while (!wait_done(claimed_waiter))",
            'asm volatile("pause" ::: "memory")',
        ],
        "WKI task cleanup claim/publish and in-flight claimant quiescence",
    )
    if "cur->state.store(WkiWaitEntry::DONE" in cleanup_body:
        fail("wki_wait_cleanup_for_task must not force DONE after losing the waiter claim")
    if "cur->result =" in cleanup_body:
        fail("wki_wait_cleanup_for_task must publish results only through publish_claimed_wait_entry")


def test_done_is_the_final_stack_waiter_access() -> None:
    finish_body = function_body(WKI_CPP.read_text(), "wki_finish_claimed_op")
    task_load = finish_body.find("entry->task.load(std::memory_order_acquire)")
    task_retain = finish_body.find("waiter_task->try_acquire()")
    done_publish = finish_body.find("publish_claimed_wait_entry(entry, result)")
    task_wake = finish_body.find("mod::sched::kern_wake(waiter_task)")
    task_release = finish_body.find("waiter_task->release()")
    if min(task_load, task_retain, done_publish, task_wake, task_release) < 0:
        fail("wki_finish_claimed_op must retain and release its scheduler task around DONE")
    if not task_load < task_retain < done_publish < task_wake < task_release:
        fail("wki_finish_claimed_op must pin task, publish DONE, then wake and release")
    if "entry->" in finish_body[done_publish + len("publish_claimed_wait_entry(entry, result)") :]:
        fail("wki_finish_claimed_op must not dereference a stack waiter after publishing DONE")


def test_ktest_covers_completed_claimed_cleanup() -> None:
    source = WKI_WAIT_KTEST.read_text()
    require_tokens(
        source,
        [
            "TaskCleanupPreservesCompletedClaimedWaiter",
            "wki_claim_op(&wait)",
            "wki_finish_claimed_op(&wait, 77)",
            "wki_wait_cleanup_for_task(&task)",
            "KEXPECT_NULL(wait.task.load(std::memory_order_acquire))",
            "WkiWaitEntry::DONE",
        ],
        "WKI wait cleanup completed-claim KTEST",
    )


def main() -> None:
    test_cleanup_publishes_only_when_it_claims_waiter()
    test_done_is_the_final_stack_waiter_access()
    test_ktest_covers_completed_claimed_cleanup()
    print("WKI wait cleanup source invariants hold")


if __name__ == "__main__":
    main()
