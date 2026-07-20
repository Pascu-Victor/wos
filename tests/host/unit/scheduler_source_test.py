#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
CONTEXT_SWITCH_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.cpp"
CONTEXT_SWITCH_ASM = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.asm"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
PROCFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.hpp"
GDT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.cpp"
GDT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.hpp"
THREADING_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.cpp"
THREADING_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
MM_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "mm.hpp"
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
SCHEDULER_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "scheduler_ktest.cpp"
NET_BACKLOG_CPP = ROOT / "modules" / "kern" / "src" / "net" / "backlog.cpp"
NETPOLL_CPP = ROOT / "modules" / "kern" / "src" / "net" / "netpoll.cpp"
WKI_REMOTE_IPC_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:\[\[nodiscard\]\]\s+)?(?:inline\s+)?(?:auto|bool|void|Thread\*|uint32_t)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_scheduler_wait_check_arm_halt_is_irq_atomic() -> None:
    source = SCHEDULER_HPP.read_text()
    cancel_body = function_body(source, "scheduler_wait_should_cancel")
    halt_body = function_body(source, "scheduler_wait_halt_once")
    halt_primitive_body = function_body(source, "halt_once_preserving_interrupt_state")

    require_order(
        cancel_body,
        "task->wakeup_pending.exchange(false, std::memory_order_acquire)",
        "wait_kind != SchedulerHaltWaitKind::YIELD",
        "scheduler wait cancellation must consume event wakes before checking completed waits",
    )
    require_order(
        cancel_body,
        "wait_kind != SchedulerHaltWaitKind::YIELD",
        "wait_kind == SchedulerHaltWaitKind::PROCESS_PARK",
        "scheduler wait cancellation must check completed waits before signals",
    )
    require_tokens(
        cancel_body,
        [
            "!task->wants_block && task->wake_at_us == 0",
            "task->has_interrupting_signal_pending()",
        ],
        "scheduler wait cancellation conditions",
    )
    require_order(
        halt_body,
        'asm volatile("cli" ::: "memory")',
        "scheduler_wait_should_cancel(task, wait_kind)",
        "scheduler halt wait must mask interrupts before its final cancellation check",
    )
    require_order(
        halt_body,
        "scheduler_wait_should_cancel(task, wait_kind)",
        "request_local_timer_recheck()",
        "scheduler halt wait must check cancellation before its timer arm",
    )
    require_order(
        halt_body,
        "request_local_timer_recheck()",
        "halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED)",
        "scheduler halt wait must keep the timer arm adjacent to STI/HLT",
    )
    require_tokens(
        halt_primitive_body,
        [
            'asm volatile("sti\\n\\thlt" ::: "memory")',
            'asm volatile("sti\\n\\thlt\\n\\tcli" ::: "memory")',
        ],
        "scheduler halt primitive must preserve the x86 STI interrupt shadow",
    )
    cancel_pos = halt_body.find("scheduler_wait_should_cancel(task, wait_kind)")
    arm_pos = halt_body.find("request_local_timer_recheck()", cancel_pos)
    halt_pos = halt_body.find("halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED)", arm_pos)
    cancel_to_arm = halt_body[cancel_pos:arm_pos]
    arm_to_halt = halt_body[arm_pos:halt_pos]
    if "restore_interrupts_after_scheduler_wait" in cancel_to_arm:
        fail("scheduler wait cancellation must return with IF masked until caller cleanup")
    for forbidden in ["return", 'asm volatile("sti"', "restore_interrupts_after_scheduler_wait"]:
        if forbidden in arm_to_halt:
            fail(f"scheduler wait timer arm must remain adjacent to STI/HLT: found {forbidden}")

    wait_callers = {
        "kern_yield_impl": "SchedulerHaltWaitKind::YIELD",
        "kern_block_impl": "SchedulerHaltWaitKind::BLOCK",
        "kern_sleep_us_impl": "SchedulerHaltWaitKind::BLOCK",
        "preemptible_syscall_park_impl": "SchedulerHaltWaitKind::PROCESS_PARK",
    }
    for function_name, wait_kind in wait_callers.items():
        body = function_body(source, function_name)
        require_tokens(
            body,
            [
                "pause_syscall_accounting()",
                f"scheduler_wait_halt_once(task, {wait_kind}, INTERRUPTS_WERE_ENABLED)",
                "restore_interrupts_after_scheduler_wait(INTERRUPTS_WERE_ENABLED)",
            ],
            f"{function_name} atomic scheduler wait",
        )
        if "request_local_timer_recheck()" in body:
            fail(f"{function_name} must arm its timer only inside scheduler_wait_halt_once")
        if "halt_once_preserving_interrupt_state(" in body:
            fail(f"{function_name} must halt only inside scheduler_wait_halt_once")
        require_order(
            body,
            "pause_syscall_accounting()",
            f"scheduler_wait_halt_once(task, {wait_kind}, INTERRUPTS_WERE_ENABLED)",
            f"{function_name} must pause syscall accounting before it can halt",
        )
        require_order(
            body,
            "task->clear_wait_channel()",
            "restore_interrupts_after_scheduler_wait(INTERRUPTS_WERE_ENABLED)",
            f"{function_name} must clear wait metadata before restoring IF after cancellation",
        )

    park_body = function_body(source, "preemptible_syscall_park_impl")
    require_order(
        park_body,
        "restore_interrupts_after_scheduler_wait(INTERRUPTS_WERE_ENABLED)",
        "restore_process_syscall_park_interrupts(INTERRUPTS_WERE_ENABLED, task)",
        "process park must retain its syscall-specific IF policy after generic wait cleanup",
    )


def test_event_wake_cancel_preserves_current_task_wakeup_token() -> None:
    source = SCHEDULER_CPP.read_text()
    wake_body = function_body(source, "wake_task_from_event_on_cpu")
    reserved_wake_body = function_body(source, "publish_reserved_task_wakeup_locked")
    reschedule_body = function_body(source, "reschedule_task_for_cpu_once")

    require_tokens(
        wake_body,
        [
            "bool const CANCEL_DEFERRED_SWITCH = event_wake_cancels_deferred_switch(deferred_switch)",
            "task->deferred_task_switch = false",
            "task->wakeup_pending.store(true, std::memory_order_release)",
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
    require_order(
        wake_body,
        "task->wakeup_pending.store(true, std::memory_order_release)",
        "reschedule_task_for_cpu(cpu, task)",
        "event wakes must publish a wake token before the reschedule decision",
    )
    if "CurrentTaskWakeupPending" in source:
        fail("current-task event wakes must not select a mode that clears wakeup_pending")

    current_task_branch = reschedule_body[reschedule_body.find("if (is_current_on_some_cpu)") :]
    current_task_branch = current_task_branch[: current_task_branch.find("if (is_waitpid_wait_channel")]
    require_tokens(
        current_task_branch,
        [
            "reserved_wake.was_blocking",
            "wake_cpu(current_cpu_of_task, WakeCpuMode::FORCE)",
        ],
        "reschedule current-task wake token handling",
    )
    require_tokens(
        reserved_wake_body,
        ["task->wakeup_pending.store(true, std::memory_order_release)"],
        "reschedule must publish the current-task wake token while ownership is locked",
    )
    if "task->wakeup_pending.store(false" in current_task_branch:
        fail("current-task event wakes must preserve wakeup_pending, even when deferred switch is cancelled")


def test_concurrent_reschedule_requests_have_one_queue_transition_leader() -> None:
    source = SCHEDULER_CPP.read_text()
    task_header = TASK_HPP.read_text()
    ktest_source = SCHEDULER_KTEST.read_text()
    queue_body = function_body(source, "queue_reschedule_request")
    consume_body = function_body(source, "consume_reschedule_request")
    retain_body = function_body(source, "retain_or_reacquire_reschedule_leader")
    once_body = function_body(source, "reschedule_task_for_cpu_once")
    wrapper_body = function_body(source, "reschedule_task_for_cpu")

    require_tokens(
        task_header,
        [
            "std::atomic<uint64_t> reschedule_requested_cpu{0};",
            "std::atomic<bool> reschedule_pending{false};",
            "std::atomic<bool> reschedule_in_progress{false};",
        ],
        "per-task reschedule serialization state",
    )
    require_order(
        queue_body,
        "task->reschedule_requested_cpu.store(cpu_no, std::memory_order_relaxed)",
        "task->reschedule_pending.exchange(true, std::memory_order_acq_rel)",
        "reschedule target must be written before the pending request is published",
    )
    require_order(
        queue_body,
        "task->reschedule_pending.exchange(true, std::memory_order_acq_rel)",
        "task->reschedule_in_progress.exchange(true, std::memory_order_acq_rel)",
        "reschedule work must be published before attempting to become its leader",
    )
    require_order(
        consume_body,
        "task->reschedule_pending.exchange(false, std::memory_order_acq_rel)",
        "task->reschedule_requested_cpu.load(std::memory_order_acquire)",
        "the leader must consume pending work before sampling its placement hint",
    )
    require_tokens(
        consume_body,
        [
            "if (!task->reschedule_pending.exchange(false, std::memory_order_acq_rel))",
            "return false",
            "cpu_no = task->reschedule_requested_cpu.load(std::memory_order_acquire)",
            "return true",
        ],
        "late leader election must not dispatch an already-consumed request",
    )
    require_tokens(
        retain_body,
        [
            "while (true)",
            "if (task->reschedule_pending.load(std::memory_order_acquire))",
            "task->reschedule_in_progress.exchange(false, std::memory_order_acq_rel)",
            "if (!task->reschedule_pending.load(std::memory_order_acquire))",
            "task->reschedule_in_progress.exchange(true, std::memory_order_acq_rel)",
        ],
        "reschedule leader relinquish handshake",
    )
    require_order(
        retain_body,
        "task->reschedule_in_progress.exchange(false, std::memory_order_acq_rel)",
        "if (!task->reschedule_pending.load(std::memory_order_acquire))",
        "the leader must acquire losing producers before its final pending recheck",
    )
    require_order(
        retain_body,
        "if (!task->reschedule_pending.load(std::memory_order_acquire))",
        "task->reschedule_in_progress.exchange(true, std::memory_order_acq_rel)",
        "the old leader may reacquire only after observing work published across release",
    )
    reacquire_pos = retain_body.find("task->reschedule_in_progress.exchange(true, std::memory_order_acq_rel)")
    if reacquire_pos < 0 or "return true" in retain_body[reacquire_pos:]:
        fail("reacquired leadership must recheck pending work instead of dispatching an ABA-stale request")
    require_tokens(
        wrapper_body,
        [
            "task == nullptr || run_queues == nullptr",
            "if (!queue_reschedule_request(task, cpu_no))",
            "if (consume_reschedule_request(task, requested_cpu))",
            "reschedule_task_for_cpu_once(requested_cpu, task)",
            "retain_or_reacquire_reschedule_leader(task)",
        ],
        "serialized reschedule wrapper",
    )
    require_order(
        wrapper_body,
        "queue_reschedule_request(task, cpu_no)",
        "consume_reschedule_request(task, requested_cpu)",
        "the winning producer must enter the leader drain loop",
    )
    require_order(
        wrapper_body,
        "if (consume_reschedule_request(task, requested_cpu))",
        "reschedule_task_for_cpu_once(requested_cpu, task)",
        "each coalesced request must precede its queue transition",
    )
    for forbidden in ["with_lock_void", "wait_list_", "runnable_heap", "publish_runnable_task_locked"]:
        if forbidden in wrapper_body:
            fail(f"serialized reschedule wrapper must delegate queue mutation to its leader: found {forbidden}")
    if source.count("reschedule_task_for_cpu_once(") != 2:
        fail("all reschedule queue transitions must pass through the single-leader public wrapper")
    require_tokens(
        once_body,
        ['publish_runnable_task_locked(rq, task, "reschedule")'],
        "reschedule leader queue transition",
    )
    require_tokens(
        source,
        [
            "scheduler_selftest_concurrent_reschedule_requests_are_serialized",
            "ABA_EMPTY_REQUEST_REFUSED",
            "ABA_EMPTY_REACQUIRE_RELEASED",
        ],
        "concurrent reschedule kernel selftest implementation",
    )
    require_tokens(
        ktest_source,
        [
            "KTEST(SchedulerWake, ConcurrentRescheduleRequestsAreSerialized)",
            "scheduler_selftest_concurrent_reschedule_requests_are_serialized",
        ],
        "concurrent reschedule KTEST wiring",
    )


def test_event_wake_rebalances_normal_process_waiters() -> None:
    source = SCHEDULER_CPP.read_text()
    target_body = function_body(source, "event_wake_target_cpu")
    can_balance_body = function_body(source, "event_wake_can_rebalance_process")
    rebalance_body = function_body(source, "event_wake_rebalance_target_cpu")

    require_tokens(
        can_balance_body,
        [
            "task->type != task::TaskType::PROCESS",
            "task->cpu_pinned || task->domain_hard",
            "task->thread == nullptr || task->pagemap == nullptr",
            "task->sched_queue != task::Task::sched_queue::WAITING",
            "task->wait_channel_is(task::WaitChannelKind::WKI_EXECVE_PROXY)",
        ],
        "event wake rebalance eligibility must avoid pinned or invalid process migration",
    )
    if "!task->has_run" in can_balance_body:
        fail("event wake rebalance must allow cold process placement when the process has a runnable context")
    require_tokens(
        rebalance_body,
        [
            "get_least_loaded_cpu_for_task(task)",
            "cached_effective_load_for_cpu(preferred_cpu, task::TaskType::PROCESS)",
            "cached_effective_load_for_cpu(BEST_CPU, task::TaskType::PROCESS)",
            "EVENT_WAKE_REBALANCE_LOAD_GAP",
            "PREFERRED_LOAD > BEST_LOAD",
            "PREFERRED_LOAD - BEST_LOAD > EVENT_WAKE_REBALANCE_LOAD_GAP",
        ],
        "event wake rebalance must use cached process load with a migration gap",
    )
    require_tokens(
        target_body,
        [
            "event_wake_prefers_waker_cpu(task->cpu_pinned, WAITING, VOLUNTARY_BLOCK)",
            "return event_wake_rebalance_target_cpu(task, waker_cpu);",
            "return task->cpu;",
        ],
        "event wake target policy must remain centralized",
    )
    if "try_steal_from_peers" in rebalance_body:
        fail("event wake rebalance must not invoke peer stealing")


def test_idle_steal_can_migrate_normal_process_work() -> None:
    source = SCHEDULER_CPP.read_text()
    mask_body = function_body(source, "task_can_run_on_cpu")
    kernel_steal_body = function_body(source, "kernel_context_is_migratable")
    process_steal_body = function_body(source, "process_task_can_idle_steal")
    scan_limit_body = function_body(source, "steal_candidate_scan_limit")
    probe_body = function_body(source, "idle_rebalance_probe_needed_for_idle")
    steal_body = function_body(source, "try_steal_from_peers")

    require_tokens(
        mask_body,
        [
            "cpu_no >= core_count",
            "task->domain_mask & ALL_MASK",
            "allowed_mask == 0",
            "return cpu_mask_contains(allowed_mask, cpu_no);",
        ],
        "steal target CPU domain-mask helper",
    )
    require_tokens(
        steal_body,
        [
            "victim_rq_raw->cached_current_load_process.load(std::memory_order_relaxed)",
            "victim_rq_raw->cached_load_process.load(std::memory_order_relaxed) + VICTIM_CURRENT_LOAD",
            "VICTIM_HEAP_SIZE == 0 || (VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2)",
            "current_task_load_for_incoming(victim_rq->current_task, task::TaskType::PROCESS)",
            "task_can_run_on_cpu(t, stealing_cpu, N)",
            "t->cpu_pinned || t->domain_hard",
            "t->type != task::TaskType::PROCESS && !t->has_run",
            "process_task_can_idle_steal(t)",
            "uint32_t const SCAN = steal_candidate_scan_limit(victim_rq->runnable_heap.size)",
            'publish_runnable_task_locked(our_rq, stolen, "work-steal")',
        ],
        "idle steal process eligibility",
    )
    require_tokens(
        kernel_steal_body,
        [
            "task->is_voluntary_blocked()",
            "task->context.frame.cs == desc::gdt::GDT_KERN_CS",
            "task->context.frame.ss == desc::gdt::GDT_KERN_DS",
            "is_kernel_text_pointer(task->context.frame.rip)",
            "task_kernel_stack_contains(task, task->context.frame.rsp)",
            '(task->context.frame.flags & 0x2ULL) != 0',
        ],
        "idle steal voluntary-block kernel context validation",
    )
    require_tokens(
        process_steal_body,
        [
            "task->type != task::TaskType::PROCESS",
            "task->thread == nullptr || task->pagemap == nullptr || task->wki_proxy_task_id != 0",
            "task->preempt_disable_depth != 0 || task->deferred_task_switch || task->wants_block",
            "task->is_voluntary_blocked()",
            "return kernel_context_is_migratable(task);",
            "return user_context_is_canonical(task);",
        ],
        "idle steal process resume context eligibility",
    )
    require_tokens(
        scan_limit_body,
        [
            "constexpr uint32_t BASE_SCAN_LIMIT = 16",
            "constexpr uint32_t BACKLOG_SCAN_LIMIT = 64",
            "heap_size <= BASE_SCAN_LIMIT",
            "std::min<uint32_t>(heap_size, BACKLOG_SCAN_LIMIT)",
        ],
        "idle steal scan limit must expand for large process backlogs but stay bounded",
    )
    if "std::min<uint32_t>(scan, 16)" in steal_body:
        fail("idle steal must not use a fixed 16-entry victim scan that can hide stealable backlog")
    require_tokens(
        probe_body,
        [
            "victim_rq->cached_current_load_process.load(std::memory_order_relaxed)",
            "victim_rq->cached_load_process.load(std::memory_order_relaxed) + VICTIM_CURRENT_LOAD",
            "VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2",
            "VICTIM_LOAD >= MIN_STEAL_LOAD",
        ],
        "idle rebalance probe must notice peer current-plus-runnable backlog",
    )
    if "Never steal PROCESS tasks" in steal_body or "Only DAEMON tasks" in steal_body:
        fail("idle steal must not blanket-ban normal PROCESS migration")
    if re.search(r"if\s*\(\s*t->type\s*==\s*task::TaskType::PROCESS\s*\)\s*\{\s*continue;", steal_body):
        fail("idle steal must not skip every PROCESS task")
    if re.search(r"if\s*\(\s*!\s*t->has_run\s*\)\s*\{\s*continue;", steal_body):
        fail("idle steal must allow cold process placement when the process has a runnable context")
    if re.search(r"if\s*\(\s*task->is_voluntary_blocked\(\)\s*\)\s*\{\s*return\s+false;", process_steal_body):
        fail("idle steal must not blanket-ban voluntary-blocked process migration")


def test_busy_cpus_nudge_idle_peers_to_steal_process_backlog() -> None:
    source = SCHEDULER_CPP.read_text()
    backlog_body = function_body(source, "runqueue_has_stealable_process_backlog")
    nudge_body = function_body(source, "maybe_nudge_idle_cpu_for_load_balance")
    wake_handler_body = function_body(source, "scheduler_wake_handler")
    timer_body = function_body(source, "process_tasks")
    ktest_source = SCHEDULER_KTEST.read_text()

    require_tokens(
        backlog_body,
        [
            "uint32_t const HEAP_SIZE = rq->runnable_heap.size",
            "uint32_t const CURRENT_LOAD = rq->cached_current_load_process.load(std::memory_order_relaxed)",
            "CURRENT_LOAD == 0 && HEAP_SIZE < 2",
            "rq->cached_load_process.load(std::memory_order_relaxed) + CURRENT_LOAD",
            "rq->daemon_load_penalty.load(std::memory_order_relaxed)",
            "return LOAD >= MIN_STEAL_LOAD;",
        ],
        "busy-side load-balance nudge must require stealable process backlog",
    )
    require_tokens(
        nudge_body,
        [
            "runqueue_has_stealable_process_backlog(busy_rq)",
            "smt::find_group_for_cpu(busy_cpu)",
            "runqueue_accepts_rebalance_probe(idle_rq)",
            "idle_rebalance_probe_needed_for_idle(TARGET_CPU)",
            "busy_rq->load_balance_pushes.fetch_add(1, std::memory_order_relaxed)",
            "wake_cpu(TARGET_CPU, WakeCpuMode::COALESCE)",
        ],
        "busy-side load-balance nudge must wake idle-like peers that can run the existing steal path",
    )
    require_tokens(
        wake_handler_body,
        [
            "bool const HAS_LOCAL_WORK = rq->runnable_heap.size > 0",
            "bool const SHOULD_PROBE_IDLE_STEAL",
            "runqueue_accepts_rebalance_probe(rq)",
            "idle_rebalance_probe_needed_for_idle(cpu::current_cpu())",
            "if (HAS_LOCAL_WORK)",
            "rq->resched_timer_pending.store(true, std::memory_order_release)",
            "apic::one_shot_timer(1)",
        ],
        "scheduler wake IPI must arm idle-like steal probes, not only local runqueue work",
    )
    require_tokens(
        timer_body,
        [
            "maybe_nudge_idle_cpu_for_load_balance(cpu::current_cpu(), rq);",
            "maybe_steal_for_effectively_idle_current_locked(rq, current_task);",
        ],
        "timer path must periodically nudge idle peers and let effectively idle current tasks steal",
    )
    require_tokens(
        source,
        [
            "scheduler_selftest_load_balance_nudge_needs_process_backlog",
            "scheduler_selftest_idle_steal_scan_expands_for_large_backlog",
            "scheduler_selftest_effectively_idle_current_accepts_rebalance_probe",
            "CURRENT_ONLY_REJECTED",
            "CURRENT_PLUS_QUEUED_ACCEPTED",
            "SOFT_EXCLUSIVE_REJECTED",
            "BACKLOG_EXPANDS",
            "BACKLOG_BOUNDED",
            "VOLUNTARY_EMPTY_ACCEPTED",
            "VOLUNTARY_CURRENT_ONLY_ACCEPTED",
            "REAL_LOCAL_WORK_REJECTED",
        ],
        "busy-side load-balance nudge kernel selftest implementation",
    )
    require_tokens(
        ktest_source,
        [
            "scheduler_selftest_load_balance_nudge_needs_process_backlog",
            "scheduler_selftest_idle_steal_scan_expands_for_large_backlog",
            "scheduler_selftest_effectively_idle_current_accepts_rebalance_probe",
            "KTEST(SchedulerMigration, LoadBalanceNudgeNeedsProcessBacklog)",
            "KTEST(SchedulerMigration, IdleStealScanExpandsForLargeBacklog)",
            "KTEST(SchedulerMigration, EffectivelyIdleCurrentAcceptsRebalanceProbe)",
        ],
        "busy-side load-balance nudge kernel selftest wiring",
    )


def test_wait_channel_policy_uses_typed_kinds() -> None:
    source = SCHEDULER_CPP.read_text()
    task_header = TASK_HPP.read_text()
    low_latency_body = function_body(source, "is_low_latency_handoff_wait_channel")
    futex_body = function_body(source, "is_futex_wait_channel")

    require_tokens(
        task_header,
        [
            "enum class WaitChannelKind : uint8_t",
            "LOCAL_PIPE",
            "LOCAL_PTY",
            "WAITPID",
            "FUTEX",
            "SIGSUSPEND",
            "WKI_EXECVE_PROXY",
            "PTRACE",
            "const char* wait_channel = nullptr;",
            "WaitChannelKind wait_channel_kind{WaitChannelKind::NONE};",
            "void set_wait_channel(const char* channel, WaitChannelKind kind = WaitChannelKind::GENERIC)",
            "void clear_wait_channel()",
            "auto wait_channel_is(WaitChannelKind kind) const -> bool",
        ],
        "typed wait-channel task state",
    )
    require_tokens(
        low_latency_body,
        [
            "wait_channel == task::WaitChannelKind::LOCAL_PIPE",
            "wait_channel == task::WaitChannelKind::LOCAL_PTY",
            "wait_channel == task::WaitChannelKind::WAITPID",
        ],
        "low-latency handoff wait-channel policy",
    )
    require_tokens(
        futex_body,
        [
            "wait_channel == task::WaitChannelKind::FUTEX",
        ],
        "futex wait-channel policy",
    )
    forbidden = [
        "std::strcmp",
        "strcmp",
        "is_local_pipe_wait_channel",
        "is_local_pty_wait_channel",
    ]
    for snippet in forbidden:
        if snippet in low_latency_body or snippet in futex_body:
            fail(f"scheduler wait-channel policy still uses string classification: {snippet}")


def test_higher_priority_wakeup_bypasses_only_priority_holdoff_guards() -> None:
    source = SCHEDULER_CPP.read_text()
    net_latency_sources = "\n".join([NET_BACKLOG_CPP.read_text(), NETPOLL_CPP.read_text(), WKI_REMOTE_IPC_CPP.read_text()])
    helper_body = function_body(source, "is_higher_priority_contender")

    require_tokens(
        helper_body,
        [
            "current == nullptr",
            "contender == nullptr",
            "current->type == task::TaskType::PROCESS",
            "contender->sched_nice < current->sched_nice",
            "contender->type == task::TaskType::PROCESS || contender->type == task::TaskType::DAEMON",
            "current->type == task::TaskType::DAEMON && contender->type == task::TaskType::DAEMON",
        ],
        "higher-priority contender helper",
    )
    require_tokens(
        source,
        [
            "bool const HIGHER_PRIORITY_PREEMPT = is_higher_priority_contender(current_task, next);",
            "next->vdeadline >= current_task->vdeadline && !HIGHER_PRIORITY_PREEMPT",
            "next->just_woke && !HIGHER_PRIORITY_PREEMPT && RUN_DURATION_US < SCHED_MIN_GRANULARITY_US",
            "!HIGHER_PRIORITY_PREEMPT &&",
            "bool const LOWER_PRIORITY_PROCESS_PREEMPT",
            "bool const LOWER_WEIGHT_PREEMPT",
            "bool const BURSTY_PROCESS_WAKEUP_PREEMPT",
        ],
        "higher-priority wakeup preemption guard",
    )
    require_tokens(
        net_latency_sources,
        [
            "NET_LATENCY_DAEMON_NICE = -5",
            "WKI_LATENCY_DAEMON_NICE = -5",
            "set_task_nice(task, NET_LATENCY_DAEMON_NICE)",
            "set_task_nice(task, WKI_LATENCY_DAEMON_NICE)",
        ],
        "latency-sensitive DAEMON nice configuration",
    )


def test_real_idle_tasks_never_enter_dead_gc() -> None:
    source = SCHEDULER_CPP.read_text()
    idle_guard_body = function_body(source, "is_gc_protected_idle_task")
    idle_restore_body = function_body(source, "restore_gc_protected_idle_task")
    dead_list_body = function_body(source, "insert_into_dead_list")

    require_tokens(
        idle_guard_body,
        [
            "task->type == task::TaskType::IDLE",
            "task->pid == 0",
        ],
        "real idle task GC guard",
    )

    require_tokens(
        idle_restore_body,
        [
            "bool const NEEDS_RESTORE",
            "task->state.store(task::TaskState::ACTIVE, std::memory_order_release)",
            "task->death_epoch.store(0, std::memory_order_release)",
            "task->gc_queued.store(false, std::memory_order_release)",
            "task->sched_queue = task::Task::sched_queue::NONE",
            "return true",
        ],
        "real idle task liveness repair",
    )
    require_tokens(
        source,
        ["auto restore_gc_protected_idle_task(task::Task* task) -> bool"],
        "real idle task liveness repair helper",
    )

    require_tokens(
        dead_list_body,
        [
            "is_gc_protected_idle_task(task)",
            "if (restore_gc_protected_idle_task(task))",
            "repaired idle task after dead GC enqueue attempt",
            "task->gc_queued.compare_exchange_strong",
            "rq->dead_list.push(task)",
        ],
        "dead-list idle task protection",
    )
    require_order(
        dead_list_body,
        "is_gc_protected_idle_task(task)",
        "if (restore_gc_protected_idle_task(task))",
        "real idle task liveness must be restored before reporting a repaired idle task",
    )
    require_order(
        dead_list_body,
        "if (restore_gc_protected_idle_task(task))",
        "task->gc_queued.compare_exchange_strong",
        "real idle task must be rejected before it can be GC queued",
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
            'mm::phys::page_alloc_with_reclaim_may_fail(mm::paging::PAGE_SIZE, "thread_tls_page")',
            "free_mapped_user_range(page_table, TLS_VIRT_ADDR, TLS_VIRT_ADDR + offset);",
            "auto const INITIAL_TID = static_cast<uint32_t>(initial_tid);",
            "write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x18, INITIAL_TID)",
            "thread->tls_phys_ptr = 0;",
        ],
        "fragmentation-safe initial user TLS and TCB construction",
    )
    forbidden = [
        'mm::phys::page_alloc(ALIGNED_TOTAL_SIZE, "thread_tls")',
        "mm::phys::page_split_to_order0(tls)",
    ]
    present = [token for token in forbidden if token in create_thread_body]
    if present:
        fail(f"initial user TLS still requires a contiguous physical run: {present[0]}")

    require_tokens(
        task_source,
        [
            "static std::atomic<uint64_t> next_pid{1}",
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


def test_pid_allocation_and_publication_reject_duplicates() -> None:
    task_source = TASK_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    next_pid_body = function_body(task_source, "get_next_pid")
    pid_insert_body = function_body(scheduler_source, "pid_table_insert")
    active_insert_body = function_body(scheduler_source, "active_list_insert")

    require_tokens(
        next_pid_body,
        [
            "static std::atomic<uint64_t> next_pid{1}",
            "uint64_t pid = 0",
            "while (pid == 0)",
            "pid = next_pid.fetch_add(1, std::memory_order_relaxed)",
            "return pid",
        ],
        "PID allocation must be atomic and skip PID 0 on wrap",
    )
    if "static uint64_t next_pid" in next_pid_body or "return next_pid++" in next_pid_body:
        fail("PID allocation must not use a plain static increment")

    require_order(
        pid_insert_body,
        "if (entry.pid == 0)",
        "if (entry.pid == t->pid)",
        "PID table insert must probe empty slots before duplicate detection",
    )
    require_tokens(
        pid_insert_body,
        [
            "bool const SAME_TASK = entry.task == t",
            "return SAME_TASK",
        ],
        "PID table duplicate insert must be idempotent only for the same task",
    )
    duplicate_pid_branch = pid_insert_body[pid_insert_body.find("if (entry.pid == t->pid)") :]
    if "entry.task = t;" in duplicate_pid_branch:
        fail("PID table insert must not replace an existing task for a duplicate PID")

    require_tokens(
        active_insert_body,
        [
            "if (existing == t)",
            "return true",
            "if (existing != nullptr && existing->pid == t->pid)",
            "return false",
        ],
        "active task list must reject a different task with a duplicate PID",
    )
    if "active_task_slot(i) = t;" in active_insert_body:
        fail("active task list must not replace an existing task with the same PID")


def test_default_user_stack_reservation_covers_native_toolchain_links() -> None:
    mm_header = MM_HPP.read_text()
    match = re.search(r"USER_STACK_SIZE\s*=\s*(0x[0-9A-Fa-f]+|\d+)", mm_header)
    if match is None:
        fail("missing USER_STACK_SIZE definition")
    stack_size = int(match.group(1), 0)
    if stack_size < 64 * 1024 * 1024:
        fail("default user stack must reserve at least 64MiB for native WOS toolchain links")

    threading_source = THREADING_CPP.read_text()
    create_thread_body = function_body(threading_source, "create_thread")
    require_tokens(
        create_thread_body,
        [
            "thread->stack_phys_ptr = 0;",
            "ensure_stack_backing(thread, page_table, STACK_TOP - INITIAL_STACK_BYTES, STACK_TOP)",
        ],
        "larger user stacks must remain lazily backed",
    )
    require_tokens(
        threading_source,
        ["bool handle_lazy_stack_fault(Thread* thread, mm::paging::PageTable* page_table, uint64_t fault_addr, uint64_t rsp)"],
        "larger user stacks must still rely on lazy fault backing",
    )


def test_execve_publishes_new_context_before_old_image_teardown() -> None:
    source = EXEC_CPP.read_text()
    body = function_body(source, "wos_proc_execve_impl")

    require_tokens(
        body,
        [
            "auto* old_pagemap_to_destroy = old_pagemap;",
            "auto* old_thread_to_destroy = old_thread;",
            "task->pagemap = new_pagemap;",
            "task->thread = new_thread;",
            "mm::virt::release_pagemap(old_pagemap_to_destroy);",
            "mm::virt::release_pagemap(new_pagemap);",
        ],
        "execve replacement context publication",
    )
    require_order(
        body,
        "task->pagemap = new_pagemap;",
        "ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(old_pagemap_to_destroy);",
        "execve must stop publishing the old pagemap before old-image teardown can block",
    )
    require_order(
        body,
        "task->thread = new_thread;",
        "mod::sched::threading::destroy_thread(old_thread_to_destroy);",
        "execve must stop publishing the old thread before destroying it",
    )

    forbidden = [
        "mm::phys::page_free(old_pagemap)",
        "mm::phys::page_free(new_pagemap)",
        "mm::virt::switch_to_kernel_pagemap();\n        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(old_pagemap",
    ]
    present = [token for token in forbidden if token in body]
    if present:
        fail(f"execve can republish or directly free a live pagemap during image replacement: {present[0]}")


def test_pagemap_sibling_check_includes_dead_publishers() -> None:
    source = SCHEDULER_CPP.read_text()
    body = function_body(source, "task_has_live_pagemap_sibling")

    require_tokens(
        body,
        [
            "auto* const PAGEMAP = subject->pagemap;",
            "global_task_registry_lock.lock_irqsave()",
            "other == nullptr || other == subject || other->pagemap != PAGEMAP",
            "return true;",
            "run_queues->with_lock(cpu_no",
            "rq->dead_list.head",
            "cur != subject && cur->pagemap == PAGEMAP",
        ],
        "pagemap sibling check must include all published task metadata",
    )
    if "TaskState::DEAD" in body:
        fail("exit-time pagemap sibling check must not ignore dead tasks that still publish the pagemap")


def test_process_syscall_reschedules_defer_to_syscall_exit() -> None:
    source = CONTEXT_SWITCH_CPP.read_text()
    helper_body = function_body(source, "defer_process_reschedule_to_syscall_exit")
    request_body = function_body(source, "request_reschedule")

    require_tokens(
        helper_body,
        [
            "power::shutdown_in_progress()",
            "task->type != sched::task::TaskType::PROCESS",
            "task->deferred_task_switch",
            "task->is_voluntary_blocked()",
            "task->wants_block",
            "stack_belongs_to_task(task, rsp)",
            "task->yield_switch = true;",
            "task->deferred_task_switch = true;",
            "task->clear_wait_channel();",
        ],
        "process syscall local reschedule deferral guard",
    )
    if 'task->set_wait_channel("local_reschedule");' in helper_body:
        fail("process syscall local reschedule yields must not leave a wait-channel marker")
    require_order(
        request_body,
        "defer_process_reschedule_to_syscall_exit()",
        "apic::one_shot_timer(1)",
        "process syscall reschedules must defer before arming the one-tick timer",
    )
    require_order(
        helper_body,
        "task->is_voluntary_blocked()",
        "task->yield_switch = true;",
        "voluntary hlt paths must not be converted to syscall-exit yields",
    )
    require_order(
        helper_body,
        "task->deferred_task_switch = true;",
        "task->clear_wait_channel();",
        "deferred syscall-exit yields must clear stale wait metadata after marking the switch",
    )


def test_scheduler_timer_disarm_clears_pending_reschedule_token() -> None:
    source = SCHEDULER_CPP.read_text()
    interrupt_body = function_body(source, "note_scheduler_timer_interrupt")
    disarm_body = function_body(source, "note_scheduler_timer_disarm")
    wake_body = function_body(source, "wake_cpu")
    request_body = function_body(source, "request_local_reschedule")

    require_tokens(
        interrupt_body,
        [
            "rq->scheduler_timer_interrupts.fetch_add(1, std::memory_order_relaxed);",
            "rq->resched_timer_pending.store(false, std::memory_order_release);",
        ],
        "timer interrupt pending-token consumption",
    )
    require_tokens(
        disarm_body,
        [
            "rq->resched_timer_pending.store(false, std::memory_order_release);",
            "rq->scheduler_timer_disarms.fetch_add(1, std::memory_order_relaxed);",
        ],
        "timer disarm pending-token clearing",
    )
    require_order(
        disarm_body,
        "rq->resched_timer_pending.store(false, std::memory_order_release);",
        "rq->scheduler_timer_disarms.fetch_add(1, std::memory_order_relaxed);",
        "timer disarm must clear stale pending state before publishing the disarm counter",
    )
    require_tokens(
        wake_body,
        [
            "rq->resched_timer_pending.exchange(true, std::memory_order_acq_rel)",
            "rq->wake_ipis_coalesced.fetch_add(1, std::memory_order_relaxed);",
        ],
        "remote wake coalescing depends on non-stale pending token",
    )
    require_tokens(
        request_body,
        [
            "rq->resched_timer_pending.exchange(true, std::memory_order_acq_rel)",
            "rq->resched_timer_pending.store(false, std::memory_order_release);",
        ],
        "local wake coalescing depends on non-stale pending token",
    )


def test_idle_timer_arms_when_idle_runqueue_has_runnable_work() -> None:
    source = SCHEDULER_CPP.read_text()
    idle_timer_body = function_body(source, "arm_idle_timer_locked")
    probe_body = function_body(source, "idle_rebalance_probe_needed_for_idle")
    timer_decision_body = function_body(source, "get_scheduler_timer_decision_for_this_cpu")

    require_tokens(
        idle_timer_body,
        [
            "if (rq->runnable_heap.size != 0)",
            "rq->idle_timer_arms.fetch_add(1, std::memory_order_relaxed);",
            "apic::one_shot_timer(1);",
            "return;",
            "uint64_t const DEADLINE_US = rq->next_wait_deadline_us;",
        ],
        "idle timer must not disarm with runnable work queued",
    )
    require_order(
        idle_timer_body,
        "if (rq->runnable_heap.size != 0)",
        "uint64_t const DEADLINE_US = rq->next_wait_deadline_us;",
        "idle timer must check queued runnable work before wait deadlines",
    )
    require_tokens(
        timer_decision_body,
        [
            "if (rq->runnable_heap.size != 0 && CURRENT_IS_IDLE)",
            "SchedulerTimerArmReason::IDLE_WORK",
            "return FIXED_QUANTUM;",
        ],
        "normal scheduler timer path must also keep idle CPUs with queued work ticking",
    )
    require_tokens(
        source,
        ["constexpr uint64_t IDLE_REBALANCE_PROBE_US = 2'000;"],
        "idle rebalance probe interval",
    )
    require_tokens(
        idle_timer_body,
        [
            "idle_rebalance_probe_needed_for_idle(cpu::current_cpu())",
            "idle_timer_ticks_for_delta_us(IDLE_REBALANCE_PROBE_US)",
        ],
        "idle timer should periodically probe peer runqueues when work stealing is plausible",
    )
    require_tokens(
        probe_body,
        [
            "VICTIM_HEAP_SIZE == 0",
            "VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2",
            "victim_dom != nullptr && victim_dom->hard",
        ],
        "idle rebalance probe must preserve hard-domain and leave-work-behind constraints",
    )


def test_scheduler_cpu_dump_is_cmdline_gated_and_reports_reschedule_state() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    scheduler_header = SCHEDULER_HPP.read_text()
    cmdline_body = function_body(context_source, "cmdline_has_token")
    enabled_body = function_body(context_source, "scheduler_cpu_dump_enabled")
    timer_body = function_body(context_source, "wos_sched_timer")
    state_body = function_body(scheduler_source, "get_scheduler_cpu_state")
    dump_body = function_body(scheduler_source, "log_scheduler_cpu_state")
    dump_all_body = function_body(scheduler_source, "dump_scheduler_cpu_states")

    require_tokens(
        context_source,
        [
            "#include <platform/init/limine_requests.hpp>",
            "std::atomic<int> sched_cpu_dump_enabled{-1};",
        ],
        "scheduler CPU dump cmdline cache",
    )
    require_tokens(
        cmdline_body,
        [
            "while (*cursor == ' ' || *cursor == '\\t' || *cursor == '\\n')",
            "std::memcmp(START, token, TOKEN_LEN) == 0",
        ],
        "scheduler CPU dump exact cmdline token parser",
    )
    require_tokens(
        enabled_body,
        [
            "sched_cpu_dump_enabled.load(std::memory_order_acquire)",
            'cmdline_has_token(ker::init::get_kernel_cmdline(), "sched.cpu_dump")',
            "sched_cpu_dump_enabled.store(ENABLED ? 1 : 0, std::memory_order_release);",
        ],
        "scheduler CPU dump opt-in gate",
    )
    require_tokens(
        timer_body,
        [
            "scheduler_cpu_dump_enabled()",
            "cpu::current_cpu() == 0",
            "TICKS % SCHED_CPU_DUMP_PERIOD_TICKS",
            "sched::dump_scheduler_cpu_states();",
        ],
        "scheduler CPU dump timer hook",
    )
    require_tokens(
        scheduler_header,
        [
            "bool resched_timer_pending;",
            "uint64_t scheduler_timer_interrupts;",
            "uint64_t scheduler_timer_arms;",
            "uint64_t scheduler_timer_disarms;",
            "uint64_t wake_ipis_sent;",
            "uint64_t wake_ipis_coalesced;",
            "uint64_t local_reschedule_requests;",
            "uint64_t local_reschedule_timer_pokes;",
            "uint64_t last_tick_us;",
            "uint64_t next_wait_deadline_us;",
        ],
        "scheduler CPU state diagnostic fields",
    )
    require_tokens(
        state_body,
        [
            "state.resched_timer_pending = rq->resched_timer_pending.load(std::memory_order_acquire);",
            "state.scheduler_timer_interrupts = rq->scheduler_timer_interrupts.load(std::memory_order_relaxed);",
            "state.scheduler_timer_arms = rq->scheduler_timer_arms.load(std::memory_order_relaxed);",
            "state.scheduler_timer_disarms = rq->scheduler_timer_disarms.load(std::memory_order_relaxed);",
            "state.wake_ipis_sent = rq->wake_ipis_sent.load(std::memory_order_relaxed);",
            "state.wake_ipis_coalesced = rq->wake_ipis_coalesced.load(std::memory_order_relaxed);",
            "state.local_reschedule_requests = rq->local_reschedule_requests.load(std::memory_order_relaxed);",
            "state.local_reschedule_timer_pokes = rq->local_reschedule_timer_pokes.load(std::memory_order_relaxed);",
            "state.last_tick_us = rq->last_tick_us;",
            "state.next_wait_deadline_us = rq->next_wait_deadline_us;",
        ],
        "scheduler CPU state runqueue diagnostic snapshot",
    )
    require_tokens(
        dump_body,
        [
            "preempt=%u/%u",
            "preempt_max_us=%lu",
            "pending=%u",
            "timer=%lu/%lu/%lu",
            "wake=%lu/%lu",
            "local=%lu/%lu",
            "last_tick=%lu",
            "wait_deadline=%lu",
        ],
        "scheduler CPU dump diagnostic fields",
    )
    require_tokens(
        dump_all_body,
        ["get_scheduler_cpu_state(cpu_no)", "log_scheduler_cpu_state(state)"],
        "scheduler CPU dump snapshot/format delegation",
    )


def test_placement_load_counts_current_task() -> None:
    source = SCHEDULER_CPP.read_text()
    header = SCHEDULER_HPP.read_text()
    current_load_body = function_body(source, "current_task_load_for_incoming")
    update_current_body = function_body(source, "update_current_load_cache")
    publish_body = function_body(source, "publish_current_task")
    publish_runnable_body = function_body(source, "publish_runnable_task_locked")
    cached_body = function_body(source, "cached_effective_load_for_cpu")

    require_tokens(
        header,
        [
            "std::atomic<uint32_t> cached_current_load_default;",
            "std::atomic<uint32_t> cached_current_load_process;",
            "cached_current_load_default(0)",
            "cached_current_load_process(0)",
        ],
        "runqueue current-task placement load cache",
    )
    require_tokens(
        current_load_body,
        [
            "task == nullptr || task->type == task::TaskType::IDLE",
            "task->is_voluntary_blocked()",
            "task->wants_block",
            "task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE",
            "return task_load_for_incoming(task, incoming_type);",
        ],
        "current-task placement load filter",
    )
    require_tokens(
        update_current_body,
        [
            "rq->runnable_heap.contains(task)",
            "rq->cached_current_load_default.store(0, std::memory_order_relaxed)",
            "rq->cached_current_load_process.store(0, std::memory_order_relaxed)",
            "current_task_load_for_incoming(task, task::TaskType::DAEMON)",
            "current_task_load_for_incoming(task, task::TaskType::PROCESS)",
        ],
        "current-task placement cache must avoid double-counting heap-contained current tasks",
    )
    require_tokens(
        publish_body,
        [
            "rq->current_task = task;",
            "update_current_load_cache(rq, task);",
        ],
        "current-task publication cache update",
    )
    if len(re.findall(r"\brq->current_task\s*=(?!=)", source)) != 1:
        fail("current_task assignments must go through publish_current_task so placement load stays current")
    require_tokens(
        publish_runnable_body,
        [
            "rq->current_task == t",
            "update_current_load_cache(rq, t);",
        ],
        "runnable publication must refresh current-task placement cache",
    )
    require_tokens(
        cached_body,
        [
            "rq->cached_current_load_process.load(std::memory_order_relaxed)",
            "rq->cached_current_load_default.load(std::memory_order_relaxed)",
            "rq->placement_reservations.load(std::memory_order_relaxed) * FULL_LOAD",
        ],
        "cached placement load must include running current task",
    )


def test_cpu_accounting_snapshot_projects_live_current_runtime() -> None:
    source = SCHEDULER_CPP.read_text()
    add_body = function_body(source, "add_runtime_delta_to_cpu_snapshot")
    kernel_mode_body = function_body(source, "task_in_kernel_mode_for_cpu_snapshot")
    snapshot_body = function_body(source, "cpu_accounting_snapshot_locked")
    exported_body = function_body(source, "get_cpu_accounting_snapshot")

    require_tokens(
        add_body,
        [
            "snapshot.idle_us += delta_us;",
            "snapshot.system_us += delta_us;",
            "snapshot.nice_us += delta_us;",
            "snapshot.user_us += delta_us;",
        ],
        "live CPU accounting projection buckets",
    )
    require_tokens(
        kernel_mode_body,
        [
            "task->type != task::TaskType::PROCESS",
            "task->is_voluntary_blocked()",
            "task->context.frame.cs != desc::gdt::GDT_USER_CS",
        ],
        "live CPU accounting user/kernel classification",
    )
    require_tokens(
        snapshot_body,
        [
            "rq->cpu_user_us.load(std::memory_order_relaxed)",
            "now_us <= rq->last_tick_us",
            "auto const* current = rq->current_task;",
            "now_us - rq->last_tick_us",
            "!current->is_voluntary_blocked()",
            "add_runtime_delta_to_cpu_snapshot(snapshot, current, LIVE_DELTA_US, task_in_kernel_mode_for_cpu_snapshot(current), CHARGE_RUNNING_TIME);",
        ],
        "live CPU accounting snapshot projection",
    )
    require_tokens(
        exported_body,
        [
            "uint64_t const NOW_US = time::get_us();",
            "cpu_accounting_snapshot_locked(rq, NOW_US)",
        ],
        "exported CPU accounting snapshot must include unticked current runtime",
    )


def test_loadavg_does_not_count_interruptible_wait_channels() -> None:
    source = SCHEDULER_CPP.read_text()
    sched_header = SCHEDULER_HPP.read_text()
    ktest_source = SCHEDULER_KTEST.read_text()
    runnable_body = function_body(source, "loadavg_task_is_runnable")
    blocked_body = function_body(source, "loadavg_wait_channel_counts_as_uninterruptible")
    snapshot_body = function_body(source, "get_load_average_snapshot")

    require_tokens(
        sched_header,
        [
            "uint32_t active_for_load_tasks;",
            "uint32_t runnable_queue_tasks;",
            "uint32_t runnable_voluntary_tasks;",
            "uint32_t waiting_tasks;",
            "uint32_t dead_or_exiting_tasks;",
            "uint32_t ptrace_stopped_tasks;",
        ],
        "loadavg diagnostic snapshot fields",
    )
    require_tokens(
        runnable_body,
        [
            "task->sched_queue == task::Task::sched_queue::RUNNABLE",
            "!task->is_voluntary_blocked()",
        ],
        "loadavg runnable classifier",
    )
    require_tokens(
        blocked_body,
        [
            "task->sched_queue != task::Task::sched_queue::WAITING",
            "task->wait_channel == nullptr",
            "task->ptrace_stopped",
            "task->wait_channel_kind == task::WaitChannelKind::GENERIC",
            'std::strcmp(task->wait_channel, "dirty_bcache") == 0',
        ],
        "loadavg blocked classifier must not count every named wait channel",
    )
    if "task->sched_queue == task::Task::sched_queue::WAITING && task->wait_channel != nullptr" in snapshot_body:
        fail("loadavg must not count every named WAITING task as uninterruptible load")
    require_tokens(
        snapshot_body,
        [
            "loadavg_task_is_runnable(task)",
            "loadavg_wait_channel_counts_as_uninterruptible(task)",
            "if (IS_RUNNABLE || IS_UNINTERRUPTIBLE)",
            "snapshot.active_for_load_tasks = active_for_load;",
            "snapshot.runnable_queue_tasks = runnable_queue;",
            "snapshot.runnable_voluntary_tasks = runnable_voluntary;",
            "snapshot.waiting_tasks = waiting;",
            "snapshot.dead_or_exiting_tasks = dead_or_exiting;",
            "snapshot.ptrace_stopped_tasks = ptrace_stopped;",
        ],
        "loadavg snapshot must use explicit classifiers",
    )
    require_tokens(
        source,
        [
            "scheduler_selftest_loadavg_wait_channel_policy",
            "VOLUNTARY_RUNNABLE_IGNORED",
            "WAITPID_IGNORED",
            "PIPE_IGNORED",
            "FUTEX_IGNORED",
            "DIRTY_BCACHE_COUNTS",
        ],
        "loadavg wait-channel kernel selftest implementation",
    )
    require_tokens(
        ktest_source,
        [
            "scheduler_selftest_loadavg_wait_channel_policy",
            "KTEST(SchedulerMetrics, LoadAverageWaitChannelPolicy)",
        ],
        "loadavg wait-channel kernel selftest wiring",
    )


def test_procfs_exposes_scheduler_cpu_state_snapshot() -> None:
    source = PROCFS_CPP.read_text()
    header = PROCFS_HPP.read_text()
    body = function_body(source, "generate_kcpustate")

    require_tokens(
        header,
        [
            "KCPUSTATE_FILE",
            "/proc/kcpustate -> per-CPU scheduler current/runqueue state",
        ],
        "procfs scheduler CPU state node",
    )
    require_tokens(
        source,
        [
            "pfd->node.type == ProcNodeType::KCPUSTATE_FILE",
            "case ProcNodeType::KCPUSTATE_FILE:",
            "generate_kcpustate(pfd->content, MAX_KPERF_BUF)",
            'strcmp(path, "kcpustate")',
        ],
        "procfs scheduler CPU state wiring",
    )
    require_tokens(
        body,
        [
            "ker::mod::sched::get_load_average_snapshot()",
            "loadavg_state",
            '"load1_milli"',
            '"active"',
            '"runnable"',
            '"uninterruptible"',
            '"runq"',
            '"runq_voluntary"',
            '"waiting"',
            '"dead"',
            '"ptrace_stopped"',
            '"total"',
            "ker::mod::sched::get_scheduler_cpu_state(c)",
            "cpu_state",
            '"idle"',
            '"runq"',
            '"waitq"',
            '"cur_pid"',
            '"cur_name"',
            '"cur_type"',
            '"cur_vblk"',
            '"cur_wblk"',
            '"cur_pinned"',
            '"preempt_depth"',
            '"preempt_pending"',
            '"preempt_max_us"',
        ],
        "procfs scheduler CPU state fields",
    )


def test_procfs_status_exposes_waitpid_exit_debug_state() -> None:
    source = PROCFS_CPP.read_text()
    body = function_body(source, "generate_status")

    require_tokens(
        body,
        [
            "WaitpidCompletionClaimed",
            "task->waitpid_completion_claimed.load(std::memory_order_acquire)",
            "WaitpidLastRepairUs",
            "task->waitpid_last_repair_us",
            "ExitInProgress",
            "task->exit_in_progress ? \"1\" : \"0\"",
            "ExitNotifyReady",
            "task->exit_notify_ready.load(std::memory_order_acquire)",
            "WaitedOn",
            "task->waited_on.load(std::memory_order_acquire)",
            "WakeupPending",
            "task->wakeup_pending.load(std::memory_order_acquire)",
        ],
        "procfs waitpid/exit diagnostic state",
    )


def test_handoff_waitpid_wake_queues_out_of_lock_repair() -> None:
    source = SCHEDULER_CPP.read_text()
    requeue_body = function_body(source, "requeue_woken_outgoing_task_locked")
    commit_body = function_body(source, "commit_handoff_task_at_return_boundary")
    reserved_wake_body = function_body(source, "publish_reserved_task_wakeup_locked")
    reschedule_body = function_body(source, "reschedule_task_for_cpu_once")
    ktest_source = SCHEDULER_KTEST.read_text()

    require_tokens(
        source,
        ["task::Task*& waitpid_repair_task"],
        "handoff waitpid wake repair parameter",
    )
    require_tokens(
        requeue_body,
        [
            "outgoing->wakeup_pending.exchange(false, std::memory_order_acquire)",
            "is_waitpid_wait_channel(WAIT_CHANNEL) && outgoing->waiting_for_pid != 0",
            "outgoing->try_acquire()",
            "waitpid_repair_task = outgoing",
        ],
        "handoff waitpid wake must preserve a repair reference",
    )
    require_order(
        requeue_body,
        "outgoing->sched_queue != task::Task::sched_queue::WAITING",
        "outgoing->wakeup_pending.exchange(false, std::memory_order_acquire)",
        "runnable handoff tasks must retain event-before-park wake tokens",
    )
    require_tokens(
        commit_body,
        [
            "task::Task* waitpid_repair_task = nullptr;",
            "requeue_woken_outgoing_task_locked(rq, outgoing, NOW_US, waitpid_repair_task);",
            "if (waitpid_repair_task != nullptr)",
            "reschedule_task_for_cpu(target_cpu, waitpid_repair_task);",
            "waitpid_repair_task->release();",
        ],
        "handoff waitpid wake repair must run after runqueue unlock",
    )
    require_order(
        commit_body,
        "run_queues->this_cpu_locked_void",
        "if (waitpid_repair_task != nullptr)",
        "waitpid handoff repair must run outside the runqueue lock callback",
    )
    require_tokens(
        reserved_wake_body,
        [
            "task->wakeup_pending.store(true, std::memory_order_release)",
            "task->wants_block = false",
            "task->wake_at_us = 0",
        ],
        "reserved-task wake publication under the owner runqueue lock",
    )
    require_order(
        reserved_wake_body,
        "task->wants_block = false",
        "task->wakeup_pending.store(true, std::memory_order_release)",
        "reserved wakes must clear blocking intent before publishing the handoff token",
    )
    require_order(
        reserved_wake_body,
        "task->wake_at_us = 0",
        "task->wakeup_pending.store(true, std::memory_order_release)",
        "reserved wakes must clear their stale deadline before publishing the handoff token",
    )
    reserved_checks = list(re.finditer(r"if \(runqueue_task_is_reserved_locked\(rq, task\)\)", reschedule_body))
    if len(reserved_checks) != 2:
        fail("reschedule must have exactly fast-owner and fallback reserved-task checks")
    for check in reserved_checks:
        branch = reschedule_body[check.end() : reschedule_body.find("return;", check.end())]
        require_tokens(
            branch,
            ["reserved_wake = publish_reserved_task_wakeup_locked(rq, task)"],
            "reserved-task wake must be published before releasing the owner runqueue lock",
        )
    current_branch = reschedule_body[reschedule_body.find("if (is_current_on_some_cpu)") :]
    current_branch = current_branch[: current_branch.find("if (is_waitpid_wait_channel")]
    if "task->wakeup_pending.store(true" in current_branch:
        fail("reserved-task wake token must not be delayed until after the owner runqueue lock is released")
    require_tokens(
        source,
        [
            "scheduler_selftest_handoff_preserves_runnable_event_token",
            "scheduler_selftest_reserved_wake_precedes_handoff_commit",
        ],
        "handoff event-token kernel selftest implementation",
    )
    require_tokens(
        ktest_source,
        [
            "KTEST(SchedulerHandoff, RunnableEventTokenSurvivesCommit)",
            "scheduler_selftest_handoff_preserves_runnable_event_token",
            "KTEST(SchedulerHandoff, ReservedWakePrecedesCommit)",
            "scheduler_selftest_reserved_wake_precedes_handoff_commit",
        ],
        "handoff event-token KTEST wiring",
    )


def test_timer_waitpid_repair_rechecks_stranded_waiters_without_sigchld() -> None:
    source = SCHEDULER_CPP.read_text()
    repair_due_body = function_body(source, "waitpid_repair_due")
    wait_deadline_body = function_body(source, "task_wait_deadline_us")
    timer_body = function_body(source, "process_tasks")
    reschedule_body = function_body(source, "reschedule_task_for_cpu_once")

    require_tokens(
        source,
        ["WAITPID_REPAIR_FALLBACK_MIN_US = 50'000ULL"],
        "waitpid fallback repair threshold",
    )
    require_tokens(
        repair_due_body,
        [
            "waiter->wait_channel_is(task::WaitChannelKind::WAITPID)",
            "waiter->waiting_for_pid == 0",
            "bool const SIGCHLD_PENDING = (waiter->signal_pending_bits() & SIGCHLD_MASK) != 0;",
            "if (SIGCHLD_PENDING)",
            "waiter->waitpid_last_repair_us != 0 ? waiter->waitpid_last_repair_us : waiter->last_sleep_start_us",
            "now_us - LAST_REPAIR_US >= WAITPID_REPAIR_FALLBACK_MIN_US",
        ],
        "waitpid fallback repair predicate",
    )
    require_tokens(
        wait_deadline_body,
        [
            "t->wait_channel_is(task::WaitChannelKind::WAITPID)",
            "t->waiting_for_pid != 0",
            "t->waitpid_last_repair_us != 0 ? t->waitpid_last_repair_us : t->last_sleep_start_us",
            "LAST_REPAIR_US + WAITPID_REPAIR_FALLBACK_MIN_US",
            "min_nonzero_deadline(min_nonzero_deadline(t->wake_at_us, t->itimer_real_expire_us), waitpid_repair_deadline_us)",
        ],
        "waitpid repair must arm wait-list deadline without timed wake",
    )
    require_tokens(
        timer_body,
        [
            "uint64_t const WAIT_SCAN_NOW_US = time::get_us();",
            "while (t != nullptr)",
            "TIMED_SCAN && wake_count < PENDING_WAKE_LIMIT",
            "waitpid_repair_due(t, WAIT_SCAN_NOW_US) && t->try_acquire()",
            "t->waitpid_last_repair_us = WAIT_SCAN_NOW_US;",
            "if (complete_or_preserve_waitpid_block(waiter))",
            "reschedule_task_for_cpu(target_cpu, waiter);",
            "waiter->release();",
        ],
        "timer waitpid fallback repair flow",
    )
    require_order(
        timer_body,
        "t->waitpid_last_repair_us = WAIT_SCAN_NOW_US;",
        "recompute_wait_deadline_locked(rq);",
        "unresolved waitpid repairs must update their backoff before deadline recompute",
    )
    require_order(
        timer_body,
        "if (complete_or_preserve_waitpid_block(waiter))",
        "reschedule_task_for_cpu(target_cpu, waiter);",
        "waitpid fallback repair must only wake after the wait resolves",
    )
    repair_completion_body = timer_body[timer_body.find("for (uint32_t i = 0; i < waitpid_repair_count; ++i)") :]
    if "waiter->waitpid_last_repair_us = WAIT_SCAN_NOW_US;" in repair_completion_body:
        fail("waitpid repair backoff must be stamped while holding the runqueue lock")

    waitpid_requeue_start = reschedule_body.find(
        "if (is_waitpid_wait_channel(task->wait_channel_kind) && task->waiting_for_pid != 0 && !complete_or_preserve_waitpid_block(task))"
    )
    waitpid_requeue_end = reschedule_body.find("// Insert into target CPU", waitpid_requeue_start)
    if waitpid_requeue_start < 0 or waitpid_requeue_end < 0:
        fail("waitpid preserve requeue branch not found")
    waitpid_requeue_body = reschedule_body[waitpid_requeue_start:waitpid_requeue_end]
    require_tokens(
        waitpid_requeue_body,
        [
            "if (task->last_sleep_start_us == 0)",
            "task->last_sleep_start_us = time::get_us();",
            "wait_list_push_locked(rq, task);",
        ],
        "waitpid preserve requeue must keep fallback repair armed",
    )
    require_order(
        waitpid_requeue_body,
        "task->last_sleep_start_us = time::get_us();",
        "wait_list_push_locked(rq, task);",
        "waitpid requeue must restore the sleep timestamp before publishing to the wait list",
    )


def test_context_switch_updates_tss_rsp0_to_task_stack() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    gdt_header = GDT_HPP.read_text()
    gdt_source = GDT_CPP.read_text()
    switch_body = function_body(context_source, "switch_to")
    deferred_body = function_body(scheduler_source, "deferred_task_switch")
    start_body = function_body(scheduler_source, "start_scheduler")
    set_rsp0_body = function_body(gdt_source, "set_rsp0")

    require_tokens(
        gdt_header,
        ["void set_rsp0(const uint64_t* stack_pointer, uint64_t cpu_id);"],
        "TSS RSP0 update API declaration",
    )
    require_tokens(
        set_rsp0_body,
        [
            "if (cpu_id >= MAX_CPUS || stack_pointer == nullptr)",
            "per_cpu_gdt.at(cpu_id).tss_data.rsp[0] =",
            "reinterpret_cast<uint64_t>(stack_pointer);",
        ],
        "TSS RSP0 update helper",
    )
    require_tokens(
        switch_body,
        [
            "if (!valid_kernel_stack(next_task->context.syscall_kernel_stack))",
            "uint64_t const REAL_CPU_ID = cpu::current_cpu();",
            "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        ],
        "context switch TSS RSP0 update",
    )
    require_order(
        switch_body,
        "if (!valid_kernel_stack(next_task->context.syscall_kernel_stack))",
        "// === POINT OF NO RETURN ===",
        "context switch must validate the task kernel stack before commit",
    )
    require_order(
        switch_body,
        "uint64_t const REAL_CPU_ID = cpu::current_cpu();",
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "context switch must update the current CPU TSS RSP0 after choosing the CPU id",
    )
    require_tokens(
        deferred_body,
        [
            "uint64_t const REAL_CPU_ID = cpu::current_cpu();",
            "sys::context_switch::install_task_cpu_bases(next_task, REAL_CPU_ID);",
            "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
            "mm::virt::switch_pagemap(next_task);",
        ],
        "deferred context switch TSS RSP0 update",
    )
    require_order(
        deferred_body,
        "sys::context_switch::install_task_cpu_bases(next_task, REAL_CPU_ID);",
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "deferred context switch must update TSS RSP0 after installing task CPU bases",
    )
    require_order(
        deferred_body,
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "mm::virt::switch_pagemap(next_task);",
        "deferred context switch must update TSS RSP0 before user faults can enter the new pagemap",
    )
    require_tokens(
        start_body,
        [
            "uint64_t const REAL_CPU_ID = cpu::current_cpu();",
            "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(first_task->context.syscall_kernel_stack), REAL_CPU_ID);",
            "mm::virt::switch_pagemap(first_task);",
            "sys::context_switch::restore_debug_registers_for_task(first_task);",
            "if (ALREADY_RAN)",
        ],
        "scheduler first-task CPU state install",
    )
    require_order(
        start_body,
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(first_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "mm::virt::switch_pagemap(first_task);",
        "start_scheduler must update TSS RSP0 before entering the task pagemap",
    )
    require_order(
        start_body,
        "sys::context_switch::restore_debug_registers_for_task(first_task);",
        "if (ALREADY_RAN)",
        "already-run first-task resume must restore or clear debug registers before return",
    )


def test_deferred_user_switch_commits_after_stack_handoff() -> None:
    asm_source = CONTEXT_SWITCH_ASM.read_text()
    marker = "wos_deferred_task_switch_return:"
    start = asm_source.find(marker)
    if start < 0:
        fail("missing deferred task switch return assembly")
    end = asm_source.find("%macro", start)
    if end < 0:
        end = len(asm_source)
    deferred_body = asm_source[start:end]

    require_tokens(
        deferred_body,
        [
            "call wos_validate_deferred_return_frame",
            "build_user_return_from_ptrs_late_commit",
            "build_kernel_return_from_ptrs",
        ],
        "deferred switch return handoff path",
    )
    if "build_user_return_from_ptrs\n" in deferred_body:
        fail("deferred user switch must not publish current_task before switching to the target task stack")
    require_order(
        deferred_body,
        "call wos_validate_deferred_return_frame",
        "build_user_return_from_ptrs_late_commit",
        "deferred switch must validate the frame before the late user handoff",
    )


def test_deferred_first_run_daemon_starts_on_kernel_thread_stack() -> None:
    source = SCHEDULER_CPP.read_text()
    deferred_body = function_body(source, "deferred_task_switch")

    require_tokens(
        deferred_body,
        [
            "bool const FIRST_RUN_DAEMON = next_task->type == task::TaskType::DAEMON && !next_task->has_run;",
            'prepare_first_run_daemon(next_task, "deferred-switch");',
            "next_task->has_run = true;",
            "sys::context_switch::restore_debug_registers_for_task(next_task);",
            "arm_local_timer_after_deferred_switch(next_task);",
            "if (FIRST_RUN_DAEMON)",
            "wos_start_kernel_thread(next_task->context.frame.rsp, next_task->kthread_entry);",
            "wos_deferred_task_switch_return(&next_task->context.regs, &next_task->context.frame);",
        ],
        "deferred first-run daemon handoff",
    )
    require_order(
        deferred_body,
        "bool const FIRST_RUN_DAEMON = next_task->type == task::TaskType::DAEMON && !next_task->has_run;",
        'prepare_first_run_daemon(next_task, "deferred-switch");',
        "deferred switch must classify first-run daemons before prepare_first_run_daemon can normalize their frame",
    )
    require_order(
        deferred_body,
        'prepare_first_run_daemon(next_task, "deferred-switch");',
        "next_task->has_run = true;",
        "deferred switch must preserve first-run daemon setup before publishing has_run",
    )
    require_order(
        deferred_body,
        "arm_local_timer_after_deferred_switch(next_task);",
        "if (FIRST_RUN_DAEMON)",
        "first-run daemon start must happen after CPU state and local timer setup",
    )
    require_order(
        deferred_body,
        "if (FIRST_RUN_DAEMON)",
        "wos_deferred_task_switch_return(&next_task->context.regs, &next_task->context.frame);",
        "first-run daemons must bypass the generic same-CPL iretq resume path",
    )


def test_switch_picker_evicts_unpublishable_runnable_tasks() -> None:
    source = SCHEDULER_CPP.read_text()
    switch_pick_body = function_body(source, "pick_best_eligible_for_switch_locked")
    deferred_body = function_body(source, "deferred_task_switch")
    exit_candidate_body = function_body(source, "valid_exit_switch_candidate")

    require_tokens(
        switch_pick_body,
        [
            "while (rq->runnable_heap.size != 0)",
            "is_publishable_runnable_task(next) && !next->has_exited",
            "remove_from_heap_by_scan_locked(rq, next)",
            "next->sched_queue = task::Task::sched_queue::NONE",
        ],
        "switch picker must evict stale unpublishable heap entries before handoff",
    )
    require_tokens(
        deferred_body,
        [
            "task::Task* next = pick_best_eligible_for_switch_locked(rq, AVG, current_task)",
            "if (next == nullptr)",
            "reserve_handoff_task_locked(rq, next, time::get_us())",
        ],
        "deferred switch must use validated picker before reserving handoff",
    )
    require_order(
        deferred_body,
        "task::Task* next = pick_best_eligible_for_switch_locked(rq, AVG, current_task)",
        "reserve_handoff_task_locked(rq, next, time::get_us())",
        "deferred switch must validate picked tasks before handoff reservation",
    )
    require_tokens(
        exit_candidate_body,
        ["is_publishable_runnable_task(candidate) && !candidate->has_exited"],
        "exit switch candidate validity must use normal runnable publication contract",
    )


def test_voluntary_blocked_current_cannot_hide_runnable_peer() -> None:
    source = SCHEDULER_CPP.read_text()
    exclude_body = function_body(source, "pick_best_eligible_excluding_locked")
    switch_pick_body = function_body(source, "pick_best_eligible_for_switch_locked")

    require_tokens(
        exclude_body,
        [
            "for (uint32_t idx = 0; idx < rq->runnable_heap.size; ++idx)",
            "task::Task* candidate = run_heap_entry(rq->runnable_heap, idx)",
            "candidate == excluded",
            "int64_t const LAG = avg_vruntime - candidate->vruntime",
            "return best_eligible != nullptr ? best_eligible : best_any",
        ],
        "voluntary-block alternate runnable selection",
    )
    require_tokens(
        switch_pick_body,
        [
            "task::Task* next = rq->runnable_heap.pick_best_eligible(avg_vruntime)",
            "next == current_task",
            "current_task->is_voluntary_blocked()",
            "rq->runnable_heap.size > 1",
            "pick_best_eligible_excluding_locked(rq, avg_vruntime, current_task)",
        ],
        "voluntary-block current-task switch selection",
    )
    timer_body = source[source.find("// ---- Running task path: update EEVDF bookkeeping, maybe preempt ----") :]
    require_tokens(
        timer_body,
        [
            "int64_t const PICK_AVG = compute_avg_vruntime(rq)",
            "auto* next = DAEMON_PREEMPT_INFLATE ? pick_best_daemon_eligible_for_switch_locked",
            ": pick_best_eligible_for_switch_locked(rq, PICK_AVG, current_task)",
            "if (next == nullptr || next == current_task)",
            "perf_lag_out = PICK_AVG - current_task->vruntime",
        ],
        "timer path must not let a voluntary-blocked current task hide runnable peers",
    )


def test_runnable_publication_requires_successful_heap_insert() -> None:
    source = SCHEDULER_CPP.read_text()
    task_header = TASK_HPP.read_text()
    publish_body = function_body(source, "publish_runnable_task_locked")
    post_body = function_body(source, "post_task_for_cpu_impl")
    post_waiting_body = function_body(source, "post_task_waiting")

    require_tokens(
        task_header,
        ["std::atomic<bool> scheduler_published{false}"],
        "monotonic scheduler publication marker",
    )

    require_tokens(
        publish_body,
        [
            "rq->runnable_heap.contains(t)",
            "repair_stale_wait_membership_locked(rq, t)",
            "t->heap_index >= 0",
            'dbg::panic_handler("scheduler: runnable publish refused with stale heap index")',
            "if (!rq->runnable_heap.insert(t))",
            'dbg::panic_handler("scheduler: runnable heap full")',
            "add_to_sums(rq, t);",
            "t->sched_queue = task::Task::sched_queue::RUNNABLE;",
        ],
        "runnable publication helper",
    )
    require_order(
        publish_body,
        "if (!rq->runnable_heap.insert(t))",
        "add_to_sums(rq, t);",
        "runnable publication must account only after a successful insert",
    )
    require_order(
        publish_body,
        "add_to_sums(rq, t);",
        "t->sched_queue = task::Task::sched_queue::RUNNABLE;",
        "runnable state must be published after heap accounting",
    )
    runnable_state_pos = publish_body.rfind("t->sched_queue = task::Task::sched_queue::RUNNABLE;")
    fresh_marker_pos = publish_body.find("t->scheduler_published.store(true, std::memory_order_release)", runnable_state_pos)
    if runnable_state_pos < 0 or fresh_marker_pos <= runnable_state_pos:
        fail("fresh runnable marker must publish after heap accounting and before releasing the runqueue lock")
    if source.count("runnable_heap.insert(") != 1:
        fail("runqueue insertion must go through publish_runnable_task_locked")
    require_tokens(
        source,
        [
            'publish_runnable_task_locked(rq, outgoing, "requeue-woken-outgoing")',
            'publish_runnable_task_locked(our_rq, stolen, "work-steal")',
            'publish_runnable_task_locked(rq, task, "post-task")',
            'publish_runnable_task_locked(target_rq, task, "move-target")',
            'publish_runnable_task_locked(owner_rq, task, "move-rollback")',
            'publish_runnable_task_locked(rq, task, "kernel-thread-shutdown")',
            'publish_runnable_task_locked(rq, w, "timer-wake")',
            'publish_runnable_task_locked(rq, current_task, "proxy-block-wake")',
            'publish_runnable_task_locked(rq, task, "reschedule")',
        ],
        "runnable publication call sites",
    )
    require_tokens(
        post_body,
        [
            "bool posted = false;",
            "posted = publish_runnable_task_locked(rq, task, \"post-task\")",
            "if (!posted)",
            "active_list_remove(task->pid)",
            "pid_table_remove(task->pid)",
            'log_rejected_task_publication("runnable heap insert failed", cpu_no, task)',
            "return false;",
        ],
        "new task publication failure rollback",
    )
    require_order(
        post_waiting_body,
        "published = register_fresh_task_visibility(task)",
        "task->scheduler_published.store(true, std::memory_order_release)",
        "fresh waiting publication marker",
    )


def test_active_task_snapshot_holds_one_registry_lock() -> None:
    source = SCHEDULER_CPP.read_text()
    body = function_body(source, "snapshot_active_task_lifetime_refs_if")

    require_tokens(
        body,
        [
            "global_task_registry_lock.lock_irqsave()",
            "if (matching > capacity || (matching != 0 && out == nullptr))",
            "if (retained == capacity)",
            "out[retained_i]->release()",
            "candidate->try_acquire_lifetime_ref()",
            "out[retained++] = candidate",
            "global_task_registry_lock.unlock_irqrestore(FLAGS)",
        ],
        "active task lifetime-ref snapshot",
    )
    require_order(
        body,
        "if (matching > capacity || (matching != 0 && out == nullptr))",
        "candidate->try_acquire_lifetime_ref()",
        "snapshot must reject insufficient capacity before retaining tasks",
    )
    for forbidden in ("new ", "kmalloc", "log::", "dbg::"):
        if forbidden in body:
            fail(f"active task snapshot must not allocate or log under the registry lock: {forbidden}")


def main() -> None:
    test_scheduler_wait_check_arm_halt_is_irq_atomic()
    test_event_wake_cancel_preserves_current_task_wakeup_token()
    test_concurrent_reschedule_requests_have_one_queue_transition_leader()
    test_event_wake_rebalances_normal_process_waiters()
    test_idle_steal_can_migrate_normal_process_work()
    test_busy_cpus_nudge_idle_peers_to_steal_process_backlog()
    test_wait_channel_policy_uses_typed_kinds()
    test_higher_priority_wakeup_bypasses_only_priority_holdoff_guards()
    test_runtime_accounting_deltas_are_saturating()
    test_user_thread_tcbs_publish_nonzero_tid_before_user_execution()
    test_pid_allocation_and_publication_reject_duplicates()
    test_default_user_stack_reservation_covers_native_toolchain_links()
    test_execve_publishes_new_context_before_old_image_teardown()
    test_pagemap_sibling_check_includes_dead_publishers()
    test_process_syscall_reschedules_defer_to_syscall_exit()
    test_scheduler_timer_disarm_clears_pending_reschedule_token()
    test_idle_timer_arms_when_idle_runqueue_has_runnable_work()
    test_scheduler_cpu_dump_is_cmdline_gated_and_reports_reschedule_state()
    test_placement_load_counts_current_task()
    test_cpu_accounting_snapshot_projects_live_current_runtime()
    test_loadavg_does_not_count_interruptible_wait_channels()
    test_procfs_exposes_scheduler_cpu_state_snapshot()
    test_procfs_status_exposes_waitpid_exit_debug_state()
    test_handoff_waitpid_wake_queues_out_of_lock_repair()
    test_timer_waitpid_repair_rechecks_stranded_waiters_without_sigchld()
    test_context_switch_updates_tss_rsp0_to_task_stack()
    test_deferred_user_switch_commits_after_stack_handoff()
    test_deferred_first_run_daemon_starts_on_kernel_thread_stack()
    test_switch_picker_evicts_unpublishable_runnable_tasks()
    test_voluntary_blocked_current_cannot_hide_runnable_peer()
    test_runnable_publication_requires_successful_heap_insert()
    test_active_task_snapshot_holds_one_registry_lock()
    print("scheduler wake-token and runtime accounting invariants hold")


if __name__ == "__main__":
    main()
