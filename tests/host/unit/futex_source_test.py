#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
FUTEX_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "futex" / "futex.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    candidates = [
        source.find(f"auto {name}("),
        source.find(f"int64_t {name}("),
        source.find(f"void {name}("),
        source.find(f"extern \"C\" void {name}("),
        source.find(f"[[nodiscard]] auto {name}("),
    ]
    start = min((candidate for candidate in candidates if candidate >= 0), default=-1)
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def braced_block_after(source: str, token: str) -> tuple[str, int]:
    start = source.find(token)
    if start < 0:
        fail(f"block token not found: {token}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"block token has no body: {token}")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end], end + 1


def require_futex_table_init_is_serialized(source: str) -> None:
    for snippet in [
        "std::atomic<bool> futex_table_initialized{false}",
        "ker::mod::sys::Spinlock futex_init_lock",
        "futex_table_initialized.load(std::memory_order_acquire)",
        "futex_init_lock.lock_irqsave()",
        "futex_table.init(256)",
        "futex_table_initialized.store(initialized, std::memory_order_release)",
        "futex_init_lock.unlock_irqrestore(FLAGS)",
    ]:
        if snippet not in source:
            fail(f"futex lazy init is missing serialized init snippet: {snippet}")

    ensure_body = function_body(source, "ensure_futex_table")
    if "-> bool" not in source[source.find("ensure_futex_table") - 64 : source.find("ensure_futex_table") + 64]:
        fail("ensure_futex_table must return bool so callers can fail closed on init OOM")
    if "futex_table_initialized = true" in ensure_body:
        fail("ensure_futex_table must not publish successful initialization with a plain bool write")

    for name in ["futex_wait", "futex_wake", "futex_wake_by_phys"]:
        body = function_body(source, name)
        if "if (!ensure_futex_table())" not in body or "return -ENOMEM;" not in body:
            fail(f"{name} does not fail closed when futex table initialization fails")


def require_futex_user_word_alignment_is_validated(source: str) -> None:
    for snippet in [
        "auto futex_addr_is_aligned(const void* addr) -> bool",
        "reinterpret_cast<uintptr_t>(addr) % alignof(int)",
        "futex_selftest_addr_alignment_guard",
    ]:
        if snippet not in source:
            fail(f"futex alignment guard is missing snippet: {snippet}")

    for name in ["futex_wait", "futex_wake"]:
        body = function_body(source, name)
        guard = body.find("if (!futex_addr_is_aligned(addr))")
        translate = body.find("mod::mm::virt::translate")
        init = body.find("if (!ensure_futex_table())")
        if guard < 0:
            fail(f"{name} must reject unaligned futex words")
        if translate >= 0 and guard > translate:
            fail(f"{name} must reject unaligned futex words before address translation")
        if init >= 0 and guard > init:
            fail(f"{name} must reject unaligned futex words before futex table initialization")
        if "return -EINVAL;" not in body[guard : guard + 120]:
            fail(f"{name} unaligned futex guard must return -EINVAL")


def require_futex_wake_counts_only_claimed_waiters(source: str) -> None:
    claim_body = function_body(source, "claim_task_waiter")
    for snippet in [
        "task == nullptr || waiter == nullptr",
        "void* expected_waiter = waiter",
        "task->futex_waiter.compare_exchange_strong(expected_waiter, nullptr, std::memory_order_acq_rel, std::memory_order_acquire)",
    ]:
        if snippet not in claim_body:
            fail(f"claim_task_waiter is missing stale-wake fence snippet: {snippet}")

    for name in ["futex_wake", "futex_wake_by_phys"]:
        body = function_body(source, name)
        claim = body.find("if (!claim_task_waiter(waiter_task, waiter))")
        wake = body.find("wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu", claim)
        count = body.find("woken_count++", claim)
        release = body.find("waiter_task->release()", claim)
        if claim < 0:
            fail(f"{name} must claim task->futex_waiter before waking")
        if wake < 0 or count < 0:
            fail(f"{name} must wake/count only after claiming task->futex_waiter")
        if not (claim < wake < release and claim < count < release):
            fail(f"{name} must keep wake/count inside the successful waiter claim branch")
        stale_branch = body[claim:wake]
        if "own_waiter = false" not in stale_branch:
            fail(f"{name} must mark stale waiters as not owned by the wake path")

    if "futex_selftest_stale_wake_does_not_claim_waiter" not in source:
        fail("KTEST helper must cover stale futex wake ownership")


def require_deferred_switch_futex_cleanup_outside_runqueue_lock(source: str) -> None:
    body = function_body(source, "deferred_task_switch")
    initial_lock_pos = body.find("&futex_abort_cleanup_task")
    if initial_lock_pos < 0:
        fail("deferred_task_switch runqueue lambda no longer captures the futex cleanup slot")
    locked_body, locked_end = braced_block_after(body, "&futex_abort_cleanup_task")
    if "futex_wait_cleanup_for_task" in locked_body:
        fail("deferred_task_switch must not call futex cleanup while the runqueue lock is held")
    if "futex_abort_cleanup_task = current_task;" not in locked_body:
        fail("deferred_task_switch no longer records aborted futex cleanup under the runqueue lock")
    before_lock = body[:initial_lock_pos]
    if "task::Task* futex_abort_cleanup_task = nullptr;" not in before_lock:
        fail("deferred_task_switch is missing the post-lock futex cleanup slot")
    after_lock = body[locked_end:]
    cleanup_pos = after_lock.find("futex_wait_cleanup_for_task(futex_abort_cleanup_task)")
    if cleanup_pos < 0:
        fail("deferred_task_switch does not perform aborted futex cleanup after dropping the runqueue lock")
    proxy_pos = after_lock.find("if (notify_wki_proxy_blocked)")
    if proxy_pos >= 0 and cleanup_pos > proxy_pos:
        fail("aborted futex cleanup must run before proxy-blocked follow-up scheduling")


def require_timer_futex_cleanup_outside_runqueue_lock(source: str) -> None:
    body = function_body(source, "process_tasks")
    locked_body, locked_end = braced_block_after(body, "run_queues->this_cpu_locked_void([NOW_US")
    if "futex_wait_cleanup_for_task" in locked_body:
        fail("process_tasks must not call futex timeout cleanup while the runqueue lock is held")
    if "pending_wake_slot(futex_timeout_cleanup, futex_timeout_cleanup_count++) = w;" not in locked_body:
        fail("process_tasks no longer records futex timeout cleanup while classifying timed-out waiters")
    after_lock = body[locked_end:]
    if "futex_wait_cleanup_for_task(pending_wake_slot(futex_timeout_cleanup, i))" not in after_lock:
        fail("process_tasks does not clean timed-out futex waiters after dropping the runqueue lock")


def main() -> None:
    futex_source = FUTEX_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    require_futex_table_init_is_serialized(futex_source)
    require_futex_user_word_alignment_is_validated(futex_source)
    require_futex_wake_counts_only_claimed_waiters(futex_source)
    require_deferred_switch_futex_cleanup_outside_runqueue_lock(scheduler_source)
    require_timer_futex_cleanup_outside_runqueue_lock(scheduler_source)
    print("futex source lock-order invariants hold")


if __name__ == "__main__":
    main()
