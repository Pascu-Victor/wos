#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
WAITPID_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "waitpid.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SYSCALL_ASM = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "syscall.asm"


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
        source.find(f"void {name}("),
        source.find(f"extern \"C\" void {name}("),
    ]
    start = min((candidate for candidate in candidates if candidate >= 0), default=-1)
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def braced_block_after(source: str, token: str) -> str:
    start = source.find(token)
    if start < 0:
        fail(f"block token not found: {token}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"block token has no body: {token}")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def require_task_has_publish_fence(task_hpp: str, task_cpp: str) -> None:
    if "std::atomic<bool> waitpid_publish_pending{false}" not in task_hpp:
        fail("Task is missing waitpid_publish_pending atomic fence")
    if task_cpp.count("waitpid_publish_pending.store(false, std::memory_order_relaxed)") < 2:
        fail("Task initialization paths must clear waitpid_publish_pending")


def require_waited_on_is_atomic_claim(task_hpp: str, task_cpp: str, waitpid_cpp: str, exit_cpp: str, scheduler_cpp: str) -> None:
    header_required = [
        "std::atomic<bool> waited_on{false}",
        "task_waited_on",
        "task_clear_waited_on",
        "task_try_mark_waited_on",
        "compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)",
    ]
    for snippet in header_required:
        if snippet not in task_hpp:
            fail(f"Task waited_on state must be an atomic single-reaper claim: {snippet}")
    if task_cpp.count("task_clear_waited_on(") < 3:
        fail("Task initialization/selftest paths must clear waited_on through the helper")

    combined = "\n".join([waitpid_cpp, exit_cpp, scheduler_cpp])
    if "waited_on = true" in combined or "waited_on = false" in combined:
        fail("process reaping paths must not write waited_on directly")
    for snippet in [
        "sched_task::task_try_mark_waited_on(*target_task)",
        "sched_task::task_try_mark_waited_on(*child)",
        "task::task_try_mark_waited_on(*target)",
        "task::task_try_mark_waited_on(*child)",
    ]:
        if snippet not in combined:
            fail(f"waitpid/deferred reaping paths must claim waited_on before completion: {snippet}")


def require_wait_any_publish_recheck(waitpid_cpp: str) -> None:
    body = function_body(waitpid_cpp, "wos_proc_waitpid")
    wait_any = braced_block_after(body, "if (pid <= 0)")
    first_publish = wait_any.find("waitpid_publish_pending.store(true, std::memory_order_release)")
    first_wait_target = wait_any.find("current_task->waiting_for_pid = WAIT_ANY_CHILD")
    first_scan = wait_any.find("claim_exited_child(current_task)")
    if first_publish < 0 or first_wait_target < 0 or first_scan < 0:
        fail("wait-any path is missing publish fence, wait target, or exited-child scan")
    if not (first_publish < first_wait_target < first_scan):
        fail("wait-any path must publish the fence and WAIT_ANY target before the first exit scan")

    deferred = wait_any.find("current_task->deferred_task_switch = true")
    second_scan = wait_any.find("claim_exited_child(current_task)", first_scan + 1)
    if deferred < 0 or second_scan < 0:
        fail("wait-any path is missing the post-publication exit recheck")
    if deferred > second_scan:
        fail("wait-any post-publication recheck must run after deferred_task_switch is set")
    if wait_any.count("clear_waitpid_publish_pending(current_task)") < 4:
        fail("wait-any immediate-return paths must clear the publish fence")


def require_specific_wait_publish_recheck(waitpid_cpp: str) -> None:
    body = function_body(waitpid_cpp, "wos_proc_waitpid")
    publish = body.find("waitpid_publish_pending.store(true, std::memory_order_release)", body.find("if ((options & WOS_WNOHANG) != 0)"))
    target = body.find("current_task->waiting_for_pid = pid")
    if publish < 0 or target < 0 or publish > target:
        fail("specific wait path must publish the fence before exposing waiting_for_pid")

    trace_wait = braced_block_after(body, "if (TRACE_WAIT)")
    for snippet in [
        "current_task->deferred_task_switch = true",
        "if (is_waitable_exit(target_task))",
        "clear_waitpid_publish_pending(current_task)",
        "current_task->deferred_task_switch = false",
    ]:
        if snippet not in trace_wait:
            fail(f"TRACE_WAIT path is missing post-publication recheck snippet: {snippet}")


def require_deferred_switch_clears_after_context_save(scheduler_cpp: str) -> None:
    body = function_body(scheduler_cpp, "deferred_task_switch")
    context_save = body.find('validate_user_resume_target(current_task, "deferred-save-current")')
    clear = body.find("waitpid_publish_pending.store(false, std::memory_order_release)")
    race_check = body.find("// Race check: for blocking waits")
    if context_save < 0 or clear < 0 or race_check < 0:
        fail("deferred_task_switch is missing context-save, publish-clear, or waitpid race-check markers")
    if not (context_save < clear < race_check):
        fail("deferred_task_switch must clear waitpid_publish_pending after context save and before waitpid recheck")


def require_syscall_epilogue_honors_deferred_before_signals(syscall_asm: str) -> None:
    save_ret = syscall_asm.find("mov [rsp+0x78], rax")
    deferred_check = syscall_asm.find("WOS_DEFERRED_TASK_SWITCH_OFFSET", save_ret)
    signal_check = syscall_asm.find("call check_pending_signals", save_ret)
    if save_ret < 0 or deferred_check < 0 or signal_check < 0:
        fail("syscall epilogue is missing return save, deferred switch check, or signal check")
    if deferred_check > signal_check:
        fail("syscall epilogue must honor deferred_task_switch before pending signal delivery")


def require_interrupted_waitpid_cleans_stale_wait_state(task_hpp: str, task_cpp: str, scheduler_cpp: str, exit_cpp: str) -> None:
    for snippet in [
        "task_clear_waitpid_block_state",
        "task.waiting_for_pid = 0",
        "task.wait_status_user_addr = 0",
        "task.wait_rusage_user_addr = 0",
        "task.wait_resume_rip_user_addr = 0",
        "task.wait_resume_rsp_user_addr = 0",
        "task.clear_wait_channel()",
    ]:
        if snippet not in task_hpp:
            fail(f"Task waitpid abort helper must clear stale wait state: {snippet}")
    if "task_selftest_waitpid_block_state_clear_resets_fields" not in task_cpp:
        fail("KTEST helper must cover waitpid abort state clearing")

    body = function_body(scheduler_cpp, "deferred_task_switch")
    abort_helper = function_body(scheduler_cpp, "interrupt_waitpid_block_for_signal")
    unlink_helper = function_body(scheduler_cpp, "unlink_specific_waitpid_waiter")
    signal_abort = braced_block_after(body, "if (current_task->has_interrupting_signal_pending() && !WAITPID_STOP_READY)")
    for snippet in [
        "interrupt_waitpid_block_for_signal(current_task)",
    ]:
        if snippet not in signal_abort:
            fail(f"deferred signal abort must call waitpid cleanup helper: {snippet}")
    for snippet in [
        "task->context.regs.rax = static_cast<uint64_t>(-EINTR)",
        "unlink_specific_waitpid_waiter(task)",
        "task::task_clear_waitpid_block_state(*task)",
    ]:
        if snippet not in abort_helper:
            fail(f"waitpid signal abort helper must clean stale waitpid state: {snippet}")
    if "target->awaitee_on_exit.remove(waiter->pid)" not in unlink_helper:
        fail("specific waitpid signal abort must unlink the task from the child's exit waiter list")
    order_signal = body.find("if (current_task->has_interrupting_signal_pending() && !WAITPID_STOP_READY)")
    deferred_clear = body.find("current_task->deferred_task_switch = false")
    if order_signal < 0 or deferred_clear < 0 or deferred_clear < order_signal:
        fail("deferred_task_switch must keep deferred_task_switch true until signal-abort cleanup is complete")

    if "waiter_context_can_be_completed(waiting_task) && waiter_matches_child(waiting_task, current_task)" not in exit_cpp:
        fail("exit waiter-list completion must revalidate the waiter still waits for this child before claiming waited_on")


def require_exit_completion_respects_publish_fence(exit_cpp: str) -> None:
    helper = function_body(exit_cpp, "waiter_context_can_be_completed")
    for snippet in [
        "waiter->deferred_task_switch",
        "!waiter->waitpid_publish_pending.load(std::memory_order_acquire)",
    ]:
        if snippet not in helper:
            fail(f"waiter completion helper is missing publish-fence snippet: {snippet}")
    for token in [
        "waiter_matches_child(parent, child) && waiter_context_can_be_completed(parent)",
        "TRACER_IS_PARENT && !sched_task::task_waited_on(*child) && waiter_context_can_be_completed(tracer)",
        "if (waiter_context_can_be_completed(waiting_task) && waiter_matches_child(waiting_task, current_task))",
    ]:
        if token not in exit_cpp:
            fail(f"exit notification direct completion is not guarded by publish fence: {token}")
    complete_body = function_body(exit_cpp, "complete_exit_wait")
    for snippet in [
        "sched_task::task_try_mark_waited_on(*child)",
        "return false",
        "return true",
    ]:
        if snippet not in complete_body:
            fail(f"complete_exit_wait must claim waited_on before writing waiter context: {snippet}")
    if "completed_wait = sched_task::task_try_mark_waited_on(*current_task)" not in exit_cpp:
        fail("awaitee_on_exit path must claim waited_on only when direct completion is safe")
    if "current_task->waited_on" in exit_cpp:
        fail("awaitee_on_exit path must not mark waited_on for deferred waiters")


def require_exit_waiter_notify_drains_all_batches(exit_cpp: str) -> None:
    for snippet in [
        "EXIT_WAITER_NOTIFY_BATCH",
        "using ExitWaiterBatch = std::array<uint64_t, EXIT_WAITER_NOTIFY_BATCH>",
        "drain_exit_waiters_for_notify",
        "while (count < waiting_pids.size() && !exiting->awaitee_on_exit.empty())",
        "exiting->awaitee_on_exit.pop_back()",
        "for (;;) {\n        ExitWaiterBatch waiting_pids{};",
        "size_t const WAITER_COUNT = drain_exit_waiters_for_notify(current_task, waiting_pids)",
        "if (WAITER_COUNT == 0)",
        "process_selftest_exit_waiter_notify_drains_over_batch",
    ]:
        if snippet not in exit_cpp:
            fail(f"exit waitpid notifier must drain waiter batches without a fixed cap: {snippet}")

    if "WAITER_COUNT && i < WAITING_PIDS_CAP" in exit_cpp:
        fail("exit waitpid notifier must not stop after one fixed-size snapshot")


def main() -> None:
    task_hpp = TASK_HPP.read_text()
    task_cpp = TASK_CPP.read_text()
    exit_cpp = EXIT_CPP.read_text()
    scheduler_cpp = SCHEDULER_CPP.read_text()
    require_task_has_publish_fence(task_hpp, task_cpp)
    waitpid_cpp = WAITPID_CPP.read_text()
    syscall_asm = SYSCALL_ASM.read_text()
    require_waited_on_is_atomic_claim(task_hpp, task_cpp, waitpid_cpp, exit_cpp, scheduler_cpp)
    require_wait_any_publish_recheck(waitpid_cpp)
    require_specific_wait_publish_recheck(waitpid_cpp)
    require_deferred_switch_clears_after_context_save(scheduler_cpp)
    require_syscall_epilogue_honors_deferred_before_signals(syscall_asm)
    require_interrupted_waitpid_cleans_stale_wait_state(task_hpp, task_cpp, scheduler_cpp, exit_cpp)
    require_exit_completion_respects_publish_fence(exit_cpp)
    require_exit_waiter_notify_drains_all_batches(exit_cpp)
    print("waitpid publish/exit notification source invariants hold")


if __name__ == "__main__":
    main()
