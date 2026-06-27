#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
CONTEXT_SWITCH_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.cpp"
CONTEXT_SWITCH_ASM = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.asm"
GDT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.cpp"
GDT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.hpp"
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
        rf"\b(?:auto|bool|void|Thread\*)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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
            "auto restore_gc_protected_idle_task(task::Task* task) -> bool",
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


def test_context_switch_updates_tss_rsp0_to_task_stack() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    gdt_header = GDT_HPP.read_text()
    gdt_source = GDT_CPP.read_text()
    switch_body = function_body(context_source, "switch_to")
    deferred_body = function_body(scheduler_source, "deferred_task_switch")
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


def main() -> None:
    test_event_wake_cancel_preserves_current_task_wakeup_token()
    test_wait_channel_policy_uses_typed_kinds()
    test_runtime_accounting_deltas_are_saturating()
    test_user_thread_tcbs_publish_nonzero_tid_before_user_execution()
    test_process_syscall_reschedules_defer_to_syscall_exit()
    test_context_switch_updates_tss_rsp0_to_task_stack()
    test_deferred_user_switch_commits_after_stack_handoff()
    print("scheduler wake-token and runtime accounting invariants hold")


if __name__ == "__main__":
    main()
