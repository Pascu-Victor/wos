#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
WAITPID_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "waitpid.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"
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
    starts: set[int] = set()
    for needle in [
        f"auto {name}(",
        f"inline auto {name}(",
        f"void {name}(",
        f"inline void {name}(",
        f"[[noreturn]] void {name}(",
        f"extern \"C\" void {name}(",
    ]:
        candidate = source.find(needle)
        while candidate >= 0:
            starts.add(candidate)
            candidate = source.find(needle, candidate + 1)
    for start in sorted(starts):
        close = source.find(")", start)
        brace = source.find("{", close)
        semicolon = source.find(";", close)
        if close >= 0 and brace >= 0 and (semicolon < 0 or brace < semicolon):
            end = find_matching_brace(source, brace)
            return source[brace + 1 : end]
    fail(f"{name} function not found")


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
    if "std::atomic<bool> waitpid_completion_claimed{false}" not in task_hpp:
        fail("Task is missing waitpid completion single-winner atomic")
    if "uint64_t waitpid_last_repair_us{}" not in task_hpp:
        fail("Task is missing waitpid fallback repair backoff timestamp")
    if "std::atomic<uint64_t> waitpid_claim_observed_us{0}" not in task_hpp:
        fail("Task is missing waitpid completion-claim lease timestamp")
    for snippet in [
        "task_try_claim_waitpid_completion",
        "task_release_waitpid_completion_claim",
        "compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)",
        "task.waitpid_last_repair_us = 0",
        "task.waitpid_claim_observed_us.store(0, std::memory_order_release)",
        "task.waitpid_completion_claimed.store(false, std::memory_order_release)",
    ]:
        if snippet not in task_hpp:
            fail(f"Task waitpid completion claim helper is missing snippet: {snippet}")
    if task_cpp.count("waitpid_publish_pending.store(false, std::memory_order_relaxed)") < 2:
        fail("Task initialization paths must clear waitpid_publish_pending")
    if task_cpp.count("waitpid_completion_claimed.store(false, std::memory_order_relaxed)") < 2:
        fail("Task initialization paths must clear waitpid_completion_claimed")


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
    required_claim_sites = {
        "waitpid syscall direct target": (waitpid_cpp, "sched_task::task_try_mark_waited_on(*target_task)"),
        "waitpid syscall child scan": (waitpid_cpp, "sched_task::task_try_mark_waited_on(*child)"),
        "exit direct completion": (exit_cpp, "sched_task::task_try_mark_waited_on(*child)"),
        "scheduler deferred completion": (scheduler_cpp, "task::task_try_mark_waited_on(*child)"),
    }
    for label, (source, snippet) in required_claim_sites.items():
        if snippet not in source:
            fail(f"{label} must claim waited_on before completion: {snippet}")


def require_wait_any_publish_recheck(waitpid_cpp: str) -> None:
    body = function_body(waitpid_cpp, "wos_proc_waitpid")
    wait_any = braced_block_after(body, "if (pid <= 0)")
    cleanup = function_body(waitpid_cpp, "clear_waitpid_syscall_state")
    for snippet in [
        "clear_waitpid_publish_pending(task)",
        "task->deferred_task_switch = false",
        "sched_task::task_clear_waitpid_block_state(*task)",
    ]:
        if snippet not in cleanup:
            fail(f"waitpid syscall cleanup helper must clear blocking state and claim: {snippet}")
    if "clear_waitpid_syscall_state(waiter)" not in function_body(waitpid_cpp, "consume_claimed_exit"):
        fail("direct waitpid child consumption must use the central syscall cleanup helper")

    wnohang_branch = wait_any.find("if ((options & WOS_WNOHANG) != 0)")
    first_publish = wait_any.find("waitpid_publish_pending.store(true, std::memory_order_release)")
    if wnohang_branch < 0 or first_publish < 0 or wnohang_branch > first_publish:
        fail("wait-any WNOHANG path must run before publishing blocking wait state")

    wnohang = braced_block_after(wait_any, "if ((options & WOS_WNOHANG) != 0)")
    for snippet in [
        "claim_exited_child(current_task, WAIT_SELECTOR)",
        "consume_stopped_child(current_task, status, options, WAIT_SELECTOR)",
        "has_unwaited_child(current_task, WAIT_SELECTOR)",
        "return 0",
    ]:
        if snippet not in wnohang:
            fail(f"wait-any WNOHANG path is missing nonblocking scan snippet: {snippet}")
    for forbidden in [
        "waitpid_publish_pending.store(true, std::memory_order_release)",
        "current_task->waiting_for_pid = WAIT_SELECTOR",
        "current_task->deferred_task_switch = true",
    ]:
        if forbidden in wnohang:
            fail(f"wait-any WNOHANG path must not publish a wait target: {forbidden}")

    first_wait_target = wait_any.find("current_task->waiting_for_pid = WAIT_SELECTOR", first_publish)
    first_scan = wait_any.find("claim_exited_child(current_task, WAIT_SELECTOR)", first_publish)
    if first_publish < 0 or first_wait_target < 0 or first_scan < 0:
        fail("wait-any path is missing publish fence, wait target, or exited-child scan")
    if not (first_publish < first_wait_target < first_scan):
        fail("wait-any path must publish the fence and WAIT_ANY target before the first exit scan")

    deferred = wait_any.find("current_task->deferred_task_switch = true")
    second_scan = wait_any.find("claim_exited_child(current_task, WAIT_SELECTOR)", first_scan + 1)
    if deferred < 0 or second_scan < 0:
        fail("wait-any path is missing the post-publication exit recheck")
    if deferred > second_scan:
        fail("wait-any post-publication recheck must run after deferred_task_switch is set")
    second_no_child = wait_any.find("if (!has_unwaited_child(current_task, WAIT_SELECTOR))", second_scan)
    fallthrough_block = wait_any.find("return 0", second_scan)
    if second_no_child < 0 or fallthrough_block < 0 or not (second_scan < second_no_child < fallthrough_block):
        fail("wait-any deferred path must recheck for terminal ECHILD before blocking")
    post_deferred_no_child = braced_block_after(wait_any[second_no_child:], "if (!has_unwaited_child(current_task, WAIT_SELECTOR))")
    for snippet in [
        "clear_waitpid_syscall_state(current_task)",
        "return static_cast<uint64_t>(-ECHILD)",
    ]:
        if snippet not in post_deferred_no_child:
            fail(f"wait-any terminal race recheck must clean state and return ECHILD: {snippet}")
    if wait_any.count("clear_waitpid_syscall_state(current_task)") < 4:
        fail("wait-any immediate-return paths must use central cleanup")


def require_specific_wait_publish_recheck(waitpid_cpp: str) -> None:
    body = function_body(waitpid_cpp, "wos_proc_waitpid")
    publish = body.find("waitpid_publish_pending.store(true, std::memory_order_release)", body.find("if ((options & WOS_WNOHANG) != 0)"))
    target = body.find("current_task->waiting_for_pid = TARGET_PID")
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


def require_waitpid_process_group_selectors(waitpid_cpp: str, exit_cpp: str, scheduler_cpp: str, task_hpp: str) -> None:
    for snippet in [
        "uint64_t waiting_for_pid{};            // Encoded waitpid selector",
    ]:
        if snippet not in task_hpp:
            fail(f"Task waitpid selector comment is missing: {snippet}")

    for snippet in [
        "constexpr uint64_t WAIT_PROCESS_GROUP_SELECTOR = 1ULL << 63U",
        "wait_selector_for_process_group(effective_process_group_id(*current_task))",
        "if (pid == std::numeric_limits<int64_t>::min())",
        "current_task->waiting_for_pid = WAIT_SELECTOR",
        "claim_exited_child(current_task, WAIT_SELECTOR)",
        "consume_stopped_child(current_task, status, options, WAIT_SELECTOR)",
        "has_unwaited_child(current_task, WAIT_SELECTOR)",
        "wait_selector_matches_child(selector, child)",
    ]:
        if snippet not in waitpid_cpp:
            fail(f"waitpid syscall must use selector-aware pid<=0 handling: {snippet}")

    for source, label in [
        (scheduler_cpp, "scheduler"),
        (exit_cpp, "exit notification"),
    ]:
        for snippet in [
            "constexpr uint64_t WAIT_PROCESS_GROUP_SELECTOR = 1ULL << 63U",
            "wait_selector_is_process_group",
            "wait_selector_matches_child",
        ]:
            if snippet not in source:
                fail(f"{label} must understand process-group wait selectors: {snippet}")

    for snippet in [
        "if (WAIT_SELECTOR == 0 || wait_selector_is_specific_pid(WAIT_SELECTOR))",
        "waiter->waiting_for_pid != WAIT_SELECTOR",
        "if (!wait_selector_is_specific_pid(waiter->waiting_for_pid))",
    ]:
        if snippet not in scheduler_cpp:
            fail(f"scheduler waitpid completion must route group selectors through any-child scans: {snippet}")

    if "return waiter->waiting_for_pid == WAIT_ANY_CHILD || waiter->waiting_for_pid == child->pid" in scheduler_cpp + exit_cpp:
        fail("waitpid completion must not collapse selector matching back to WAIT_ANY_OR_EXACT_PID")


def require_deferred_switch_clears_after_context_save(scheduler_cpp: str) -> None:
    body = function_body(scheduler_cpp, "deferred_task_switch")
    context_save = body.find('validate_user_resume_target(current_task, "deferred-save-current")')
    clear = body.find("waitpid_publish_pending.store(false, std::memory_order_release)")
    race_check = body.find("complete_or_preserve_waitpid_block(current_task)")
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
        "task.waitpid_last_repair_us = 0",
        "task.clear_wait_channel()",
        "task.waitpid_claim_observed_us.store(0, std::memory_order_release)",
        "task.waitpid_completion_claimed.store(false, std::memory_order_release)",
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
        "waiter->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING",
        "waiter->wait_channel_is(ker::mod::sched::task::WaitChannelKind::WAITPID)",
        "!waiter->waitpid_publish_pending.load(std::memory_order_acquire)",
    ]:
        if snippet not in helper:
            fail(f"waiter completion helper is missing publish-fence snippet: {snippet}")
    if "if (!waiter->waitpid_publish_pending.load(std::memory_order_acquire))" in helper:
        fail("exit completion must not directly complete a waiter before it reaches the wait queue")
    for token in [
        "waiter_matches_child(parent, child) && waiter_context_can_be_completed(parent)",
        "parent->wait_channel_is(ker::mod::sched::task::WaitChannelKind::WAITPID)",
        "TRACER_IS_PARENT && !sched_task::task_waited_on(*child) && waiter_context_can_be_completed(tracer)",
        "if (waiter_context_can_be_completed(waiting_task) && waiter_matches_child(waiting_task, current_task) &&\n                    complete_exit_wait(waiting_task, current_task, \"exit-specific\"))",
    ]:
        if token not in exit_cpp:
            fail(f"exit notification direct completion is not guarded by publish fence: {token}")
    complete_body = function_body(exit_cpp, "complete_exit_wait")
    for snippet in [
        "sched_task::task_try_claim_waitpid_completion(*waiter)",
        "!waiter_matches_child(waiter, child) || !waiter_context_can_be_completed(waiter)",
        "sched_task::task_release_waitpid_completion_claim(*waiter)",
        "sched_task::task_try_mark_waited_on(*child)",
        "waiter->waitpid_publish_pending.store(false, std::memory_order_release)",
        "waiter->deferred_task_switch = false",
        "waiter->set_voluntary_blocked(false)",
        "waiter->wants_block = false",
        "sched_task::task_clear_waitpid_block_state(*waiter)",
        "return false",
        "return true",
    ]:
        if snippet not in complete_body:
            fail(f"complete_exit_wait must claim waited_on before writing waiter context: {snippet}")
    if "complete_exit_wait(waiting_task, current_task, \"exit-specific\")" not in exit_cpp:
        fail("awaitee_on_exit path must use the common waitpid completion helper")
    if "current_task->waited_on" in exit_cpp:
        fail("awaitee_on_exit path must not mark waited_on for deferred waiters")


def require_scheduler_waitpid_completion_claims_waiter(task_hpp: str, waitpid_cpp: str, scheduler_cpp: str) -> None:
    for snippet in [
        "waitpid_completion_claimed.store(false, std::memory_order_release)",
    ]:
        if snippet not in waitpid_cpp:
            fail(f"blocking waitpid setup must reset completion claim: {snippet}")

    for name in [
        "complete_waitpid_exit_for_scheduler",
        "complete_registered_waitpid_exit_for_scheduler",
        "complete_waitpid_ptrace_stop_for_scheduler",
        "complete_waitpid_job_stop_if_waitable",
    ]:
        body = function_body(scheduler_cpp, name)
        if "task::task_try_claim_waitpid_completion(*waiter)" not in body:
            fail(f"{name} must claim waiter completion before publishing a waitpid result")
        if "task::task_release_waitpid_completion_claim(*waiter)" not in body:
            fail(f"{name} must release the waiter completion claim on failed revalidation")

    any_body = function_body(scheduler_cpp, "complete_waitpid_any_for_scheduler")
    for snippet in [
        "task::task_try_claim_waitpid_completion(*waiter)",
        "wait_selector_is_specific_pid(WAIT_SELECTOR)",
        "waiter->waiting_for_pid != WAIT_SELECTOR",
        "task::task_release_waitpid_completion_claim(*waiter)",
        "waiter->context.regs.rax = static_cast<uint64_t>(-ECHILD)",
    ]:
        if snippet not in any_body:
            fail(f"wait-any ECHILD completion must be claim-protected: {snippet}")

    specific_body = function_body(scheduler_cpp, "complete_waitpid_specific_for_scheduler")
    for snippet in [
        "task::task_try_claim_waitpid_completion(*waiter)",
        "!wait_selector_is_specific_pid(WAIT_TARGET)",
        "task::task_release_waitpid_completion_claim(*waiter)",
        "waiter->context.regs.rax = static_cast<uint64_t>(-ECHILD)",
    ]:
        if snippet not in specific_body:
            fail(f"specific waitpid ECHILD completion must be claim-protected: {snippet}")

    preserve_body = function_body(scheduler_cpp, "complete_or_preserve_waitpid_block")
    for snippet in [
        "waiter->waitpid_completion_claimed.load(std::memory_order_acquire)",
        "return waiter->waiting_for_pid == 0;",
    ]:
        if snippet not in preserve_body:
            fail(f"waitpid preserve path must not publish a still-blocked waiter while another completion owns it: {snippet}")


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


def require_exit_notify_ready_is_before_address_cleanup(waitpid_cpp: str, scheduler_cpp: str, exit_cpp: str) -> None:
    for source, helper in [
        (waitpid_cpp, "is_waitable_exit"),
        (scheduler_cpp, "waitpid_child_is_waitable_exit"),
    ]:
        body = function_body(source, helper)
        for snippet in [
            "exit_notify_ready.load(std::memory_order_acquire)",
            "TaskState::DEAD",
        ]:
            if snippet not in body:
                fail(f"{helper} must treat exit-notified children and dead children as waitable: {snippet}")

    exit_body = function_body(exit_cpp, "wos_proc_exit_with_wait_status")
    fd_close = exit_body.find("while (!current_task->fd_table.empty())")
    publish = exit_body.find("current_task->has_exited = true")
    notify_ready = exit_body.find("current_task->exit_notify_ready.store(true, std::memory_order_release)", publish)
    notify_parent = exit_body.find("notify_parent_after_exit_ready(current_task)", notify_ready)
    address_cleanup = exit_body.find("release_exiting_user_address_space(current_task)", notify_ready)
    accounting = exit_body.find("ker::mod::sched::finish_syscall_accounting()", fd_close)
    if min(fd_close, accounting, publish, notify_ready, notify_parent, address_cleanup) < 0:
        fail("exit path is missing FD cleanup, accounting, waitability publication, notification, or address cleanup markers")
    if not (fd_close < accounting < publish < notify_ready < notify_parent < address_cleanup):
        fail("exit must publish waitability after FD cleanup/accounting and before address-space cleanup")


def require_waitpid_uses_stable_active_scans(scheduler_hpp: str, scheduler_cpp: str, waitpid_cpp: str) -> None:
    for snippet in [
        "using ActiveTaskPredicate = bool (*)(task::Task* task, void* context)",
        "find_active_task_lifetime_ref_if(ActiveTaskPredicate predicate, void* context)",
        "using DeadTaskPredicate = bool (*)(task::Task* task, void* context)",
        "find_dead_task_lifetime_ref_if(DeadTaskPredicate predicate, void* context)",
    ]:
        if snippet not in scheduler_hpp:
            fail(f"scheduler must expose stable active-task scan API for waitpid: {snippet}")

    for snippet in [
        "constexpr uint32_t MAX_ACTIVE_TASKS = 65536",
        "auto active_list_insert(task::Task* t) -> bool",
        "log_rejected_task_publication(\"active registry full\", cpu_no, task)",
        "pid_table_remove(task->pid)",
        "find_active_task_lifetime_ref_if(ActiveTaskPredicate predicate, void* context)",
        "find_dead_task_lifetime_ref_if(DeadTaskPredicate predicate, void* context)",
        "cur->try_acquire_lifetime_ref()",
    ]:
        if snippet not in scheduler_cpp:
            fail(f"scheduler must not silently publish PID-only tasks: {snippet}")

    for snippet in [
        "find_active_task_lifetime_ref_if(active_waitable_unwaited_child, &ctx)",
        "find_active_task_lifetime_ref_if(active_waitable_specific_child, &ctx)",
        "find_active_task_lifetime_ref_if(active_unwaited_child, &ctx)",
        "find_dead_task_lifetime_ref_if(active_waitable_unwaited_child, &ctx)",
        "find_dead_task_lifetime_ref_if(active_waitable_specific_child, &ctx)",
        "find_dead_task_lifetime_ref_if(active_unwaited_child, &ctx)",
        "return {.task = child, .release_ref = true}",
    ]:
        if snippet not in waitpid_cpp:
            fail(f"waitpid syscall active scans must use lifetime-ref stable scans: {snippet}")

    scheduler_required = [
        "find_active_task_lifetime_ref_if(active_waitpid_waitable_exit_child, waiter)",
        "find_active_task_lifetime_ref_if(active_waitpid_unwaited_child, waiter)",
        "find_active_task_lifetime_ref_if(active_waitpid_job_stop_child, waiter)",
        "find_dead_task_lifetime_ref_if(active_waitpid_waitable_exit_child, waiter)",
        "find_dead_task_lifetime_ref_if(active_waitpid_unwaited_child, waiter)",
        "find_dead_task_lifetime_ref_if(dead_waitpid_specific_target, waiter)",
        "child->try_acquire_lifetime_ref()",
        "entry.task->try_acquire_lifetime_ref()",
    ]
    for snippet in scheduler_required:
        if snippet not in scheduler_cpp:
            fail(f"scheduler waitpid repair must use stable active scans: {snippet}")

    waitpid_forbidden = [
        "get_dead_task_count(cpu_no)",
        "get_dead_task_at_safe(cpu_no, i)",
    ]
    for snippet in waitpid_forbidden:
        if snippet in waitpid_cpp:
            fail(f"waitpid syscall must not index-walk dead lists: {snippet}")


def main() -> None:
    task_hpp = TASK_HPP.read_text()
    task_cpp = TASK_CPP.read_text()
    exit_cpp = EXIT_CPP.read_text()
    scheduler_cpp = SCHEDULER_CPP.read_text()
    scheduler_hpp = SCHEDULER_HPP.read_text()
    require_task_has_publish_fence(task_hpp, task_cpp)
    waitpid_cpp = WAITPID_CPP.read_text()
    syscall_asm = SYSCALL_ASM.read_text()
    require_waited_on_is_atomic_claim(task_hpp, task_cpp, waitpid_cpp, exit_cpp, scheduler_cpp)
    require_wait_any_publish_recheck(waitpid_cpp)
    require_specific_wait_publish_recheck(waitpid_cpp)
    require_waitpid_process_group_selectors(waitpid_cpp, exit_cpp, scheduler_cpp, task_hpp)
    require_deferred_switch_clears_after_context_save(scheduler_cpp)
    require_syscall_epilogue_honors_deferred_before_signals(syscall_asm)
    require_interrupted_waitpid_cleans_stale_wait_state(task_hpp, task_cpp, scheduler_cpp, exit_cpp)
    require_exit_completion_respects_publish_fence(exit_cpp)
    require_exit_waiter_notify_drains_all_batches(exit_cpp)
    require_exit_notify_ready_is_before_address_cleanup(waitpid_cpp, scheduler_cpp, exit_cpp)
    require_scheduler_waitpid_completion_claims_waiter(task_hpp, waitpid_cpp, scheduler_cpp)
    require_waitpid_uses_stable_active_scans(scheduler_hpp, scheduler_cpp, waitpid_cpp)
    print("waitpid publish/exit notification source invariants hold")


if __name__ == "__main__":
    main()
