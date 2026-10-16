#!/usr/bin/env python3

"""Source-shape checks for scheduler architecture boundaries.

Scheduler transition semantics are exercised by compiled host-model tests and
freestanding KTESTs.  This file deliberately inspects source only where the
x86 instruction/stack ordering is itself the contract.  A final, separately
labelled section retains process/procfs source contracts that are outside the
Goal 04 scheduler-transition work.
"""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
CONTEXT_SWITCH_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.cpp"
CONTEXT_SWITCH_ASM = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.asm"
GDT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.cpp"
GDT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gdt.hpp"
THREADING_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.cpp"
THREADING_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.hpp"
MM_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "mm.hpp"
EXEC_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exec.cpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def compact_cpp(source: str) -> str:
    # Ignore source layout, preserving all bytes inside C++ string/char literals.
    return re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')|\s+', lambda match: match.group(1) or "", source)


def function_body(source: str, name: str, signature_contains: str = "") -> str:
    matches = re.finditer(
        rf"(?:\[\[[^\]]+\]\]\s+)*(?:inline\s+)?(?:auto|bool|void|Task\*|Thread\*|uint32_t)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    match = next((candidate for candidate in matches
                  if compact_cpp(signature_contains) in compact_cpp(candidate.group(0))), None)
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


def asm_region(source: str, start_marker: str, end_marker: str) -> str:
    start = source.find(start_marker)
    if start < 0:
        fail(f"missing assembly boundary {start_marker}")
    end = source.find(end_marker, start + len(start_marker))
    if end < 0:
        fail(f"missing assembly boundary {end_marker}")
    return source[start:end]


def asm_macro(source: str, name: str) -> str:
    return asm_region(source, f"%macro {name}", "%endmacro")


def require_tokens(source: str, tokens: list[str], context: str, compact: bool = False) -> None:
    if compact:
        source, tokens = compact_cpp(source), [compact_cpp(token) for token in tokens]
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str, compact: bool = False) -> None:
    if compact:
        source, first, second = compact_cpp(source), compact_cpp(first), compact_cpp(second)
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first} before {second}")


# ---------------------------------------------------------------------------
# Irreducible x86 scheduler/return architecture boundaries.
# ---------------------------------------------------------------------------


def test_scheduler_wait_check_arm_halt_is_irq_atomic() -> None:
    source = SCHEDULER_HPP.read_text()
    halt_body = function_body(source, "scheduler_wait_halt_once")
    halt_primitive_body = function_body(source, "halt_once_preserving_interrupt_state")

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
        "the final cancellation check must precede the local APIC timer recheck",
    )
    require_order(
        halt_body,
        "request_local_timer_recheck()",
        "halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED)",
        "the local APIC timer recheck must remain adjacent to STI/HLT",
    )
    require_tokens(
        halt_primitive_body,
        [
            'asm volatile("sti\\n\\thlt" ::: "memory")',
            'asm volatile("sti\\n\\thlt\\n\\tcli" ::: "memory")',
        ],
        "x86 STI interrupt-shadow halt primitive",
    )

    arm_pos = halt_body.find("request_local_timer_recheck()")
    halt_pos = halt_body.find("halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED)", arm_pos)
    arm_to_halt = halt_body[arm_pos:halt_pos]
    for forbidden in ["return", 'asm volatile("sti"', "restore_interrupts_after_scheduler_wait"]:
        if forbidden in arm_to_halt:
            fail(f"scheduler timer recheck must remain adjacent to STI/HLT: found {forbidden}")


def test_classified_restore_policy_owns_validation_and_return_boundaries() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    scheduler_source = SCHEDULER_CPP.read_text()
    asm_source = CONTEXT_SWITCH_ASM.read_text()

    validator_body = function_body(context_source, "validate_classified_frame_for_restore")
    require_tokens(
        validator_body,
        [
            "SavedFrameRestoreKind::REJECT",
            "!classified_frame_is_valid(task, frame, frame_class)",
            "SavedFrameRestoreKind::USER_IRET",
            "validate_user_frame(frame, task, path)",
            "SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET",
            "validate_kernel_frame(frame, task, path)",
            "hcf()",
        ],
        "classified user/same-CPL return validator",
    )

    switch_body = function_body(context_source, "switch_to")
    require_order(
        switch_body,
        'validate_handoff_stack_ownership(sched::get_current_task(), next_task, "switchTo-prepare")',
        'validate_saved_frame_for_restore(next_task, next_task->context.frame, "switchTo-prepare")',
        "incoming stack ownership must be validated before its classified frame",
    )
    require_order(
        switch_body,
        'validate_saved_frame_for_restore(next_task, next_task->context.frame, "switchTo-prepare")',
        "// === POINT OF NO RETURN ===",
        "classified validation must precede context-switch architectural state changes",
    )
    require_order(
        switch_body,
        "install_task_cpu_bases(next_task, REAL_CPU_ID)",
        "mm::virt::switch_pagemap(next_task)",
        "GS/FS and TSS state must be installed before the incoming CR3",
    )
    require_order(
        switch_body,
        "frame.err_code = next_task->context.frame.err_code;",
        'validate_saved_frame_for_restore(next_task, frame, "switchTo-final")',
        "the copied architectural frame must receive final validation before return",
    )

    deferred_body = function_body(scheduler_source, "deferred_task_switch")
    require_order(
        deferred_body,
        'validate_handoff_stack_ownership(current_task, next_task, "deferred-switch")',
        "install_task_cpu_bases(next_task, REAL_CPU_ID)",
        "deferred restore must prove incoming stack ownership before CPU-local state changes",
    )
    require_order(
        deferred_body,
        'validate_saved_frame_for_restore(next_task, next_task->context.frame, "deferred-prepare")',
        "install_task_cpu_bases(next_task, REAL_CPU_ID)",
        "deferred classified validation must precede GS/FS/TSS installation",
    )
    require_order(
        deferred_body,
        "install_task_cpu_bases(next_task, REAL_CPU_ID)",
        "mm::virt::switch_pagemap(next_task)",
        "deferred GS/FS/TSS installation must precede the incoming CR3",
    )

    require_tokens(
        context_source,
        [
            'validate_classified_frame_for_restore(return_task, *frame_ptr, RETURN_FRAME_CLASS, "timer-return")',
            'validate_saved_frame_for_restore(return_task, *frame_ptr, "exit-return")',
            "select_user_fpu_restore_frame_class(",
            "TIMER_AUTHORIZATION_PRESENT && !TIMER_RETURN_MATCHES_TASK",
            "!POLICY.restore_user_fpu",
        ],
        "final timer/exit/FPU classified return gates",
    )
    timer_body = function_body(context_source, "wos_sched_timer")
    require_order(
        timer_body,
        'validate_classified_frame_for_restore(return_task, *frame_ptr, RETURN_FRAME_CLASS, "timer-return")',
        "set_timer_fpu_return_authorization(return_task, RETURN_FRAME_CLASS)",
        "timer FPU authorization must be published only after final classified validation",
    )
    fpu_restore_body = function_body(context_source, "wos_restore_return_task_fpu")
    require_order(
        fpu_restore_body,
        "consume_timer_fpu_return_authorization()",
        "select_user_fpu_restore_frame_class(",
        "the final FPU hook must consume one-shot timer provenance before selecting its frame class",
    )

    exit_switch = asm_region(asm_source, "global jump_to_next_task_no_save", "global wos_deferred_task_switch_return")
    require_order(
        exit_switch,
        "cli",
        "call wos_discard_abandoned_return_fpu_state",
        "abandoned FPU return state must be discarded with interrupts masked",
    )
    require_order(
        exit_switch,
        "call wos_discard_abandoned_return_fpu_state",
        "call wos_jump_to_next_task_no_save",
        "abandoned FPU return state must be discarded before successor selection",
    )

    for macro_name in ("build_kernel_return_from_stack", "build_kernel_return_from_ptrs"):
        macro_body = asm_macro(asm_source, macro_name)
        require_tokens(
            macro_body,
            ["IRETQ_KERNEL_FRAME_OFFSET", "call wos_commit_handoff_task", "add rsp, INTERRUPT_FRAME_SIZE", "iretq"],
            f"{macro_name} same-CPL iret contract",
        )
        require_order(
            macro_body,
            "mov rsp, r10",
            "call wos_commit_handoff_task",
            f"{macro_name} must leave the outgoing stack before publishing handoff ownership",
        )
        require_order(
            macro_body,
            "call wos_commit_handoff_task",
            "iretq",
            f"{macro_name} must commit only in its final iret tail",
        )
        if "wos_restore_return_task_fpu" in macro_body or "swapgs" in macro_body:
            fail(f"{macro_name} must not execute user FPU or GS return work")

    user_macro = asm_macro(asm_source, "build_user_return_from_ptrs_late_commit")
    require_order(
        user_macro,
        "call wos_user_handoff_stack_top",
        "mov rsp, r10",
        "user handoff must select and enter the target kernel stack before commit",
    )
    require_order(
        user_macro,
        "mov rsp, r10",
        "call wos_commit_handoff_task",
        "user handoff must switch stacks before publishing current_task",
    )
    require_order(
        user_macro,
        "call wos_commit_handoff_task",
        "call wos_restore_return_task_fpu",
        "user FPU restoration must follow handoff publication",
    )
    require_order(
        user_macro,
        "call wos_restore_return_task_fpu",
        "swapgs",
        "user FPU state must be restored before the GS/iret return tail",
    )
    require_order(user_macro, "swapgs", "iretq", "swapgs must precede the user iretq")


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
        "TSS RSP0 API",
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
    require_order(
        switch_body,
        "if (!valid_kernel_stack(next_task->context.syscall_kernel_stack))",
        "// === POINT OF NO RETURN ===",
        "switch_to must validate the incoming kernel stack before commit",
    )
    require_order(
        switch_body,
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "mm::virt::switch_pagemap(next_task)",
        "switch_to must install TSS RSP0 before the incoming CR3",
    )
    require_order(
        deferred_body,
        "sys::context_switch::install_task_cpu_bases(next_task, REAL_CPU_ID);",
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "deferred switch must install task CPU bases before TSS RSP0",
    )
    require_order(
        deferred_body,
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "mm::virt::switch_pagemap(next_task)",
        "deferred switch must install TSS RSP0 before the incoming CR3",
    )
    require_order(
        start_body,
        "desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(first_task->context.syscall_kernel_stack), REAL_CPU_ID);",
        "mm::virt::switch_pagemap(first_task)",
        "start_scheduler must install TSS RSP0 before the first-task CR3",
    )


def test_deferred_user_switch_commits_after_stack_handoff() -> None:
    asm_source = CONTEXT_SWITCH_ASM.read_text()
    start = asm_source.find("wos_deferred_task_switch_return:")
    if start < 0:
        fail("missing deferred task-switch return assembly boundary")
    deferred_return = asm_source[start:]
    require_tokens(
        deferred_return,
        [
            "cli",
            "call wos_validate_deferred_return_frame",
            "build_user_return_from_ptrs_late_commit",
            "build_kernel_return_from_ptrs",
        ],
        "deferred switch classified return boundary",
    )
    require_order(
        deferred_return,
        "call wos_validate_deferred_return_frame",
        "build_user_return_from_ptrs_late_commit",
        "deferred user frame validation must precede the late-commit stack handoff",
    )
    if "build_user_return_from_ptrs\n" in deferred_return:
        fail("deferred user switch must not use the early-commit user return macro")


def test_deferred_first_run_daemon_starts_on_kernel_thread_stack() -> None:
    scheduler_source = SCHEDULER_CPP.read_text()
    asm_source = CONTEXT_SWITCH_ASM.read_text()
    deferred_body = function_body(scheduler_source, "deferred_task_switch")
    start_asm = asm_region(asm_source, "global wos_start_kernel_thread", "global wos_enterIdleStack")

    require_tokens(
        deferred_body,
        [
            "if (FIRST_RUN_DAEMON)",
            "wos_start_kernel_thread(next_task->context.frame.rsp, next_task->kthread_entry);",
            "wos_deferred_task_switch_return(&next_task->context.regs, &next_task->context.frame);",
        ],
        "first-run daemon stack-transfer dispatch",
    )
    require_order(
        deferred_body,
        "if (FIRST_RUN_DAEMON)",
        "wos_deferred_task_switch_return(&next_task->context.regs, &next_task->context.frame);",
        "first-run daemons must bypass the generic same-CPL iret path",
    )
    require_order(
        start_asm,
        "call wos_validate_kernel_thread_start",
        "mov rsp, rdi",
        "kernel-thread start must validate its target before changing stacks",
    )
    require_order(
        start_asm,
        "mov rsp, rdi",
        "call wos_commit_handoff_task",
        "kernel-thread start must enter its target stack before publishing handoff ownership",
    )
    require_order(
        start_asm,
        "call wos_commit_handoff_task",
        "sti",
        "kernel-thread handoff must commit before enabling interrupts",
    )
    require_order(
        start_asm,
        "sti",
        "jmp wos_kernel_thread_trampoline",
        "kernel-thread start must enable interrupts only in the final trampoline tail",
    )


def test_migration_guard_release_follows_cli_at_nonreturning_handoff() -> None:
    source = SCHEDULER_CPP.read_text()
    deferred_body = function_body(source, "deferred_task_switch")
    require_order(
        deferred_body,
        'asm volatile("cli" ::: "memory")',
        "migration_guard.release()",
        "non-returning deferred handoff must mask interrupts before releasing CPU ownership",
    )
    cli_pos = deferred_body.find('asm volatile("cli" ::: "memory")')
    release_pos = deferred_body.find("migration_guard.release()", cli_pos)
    cli_to_release = deferred_body[cli_pos:release_pos]
    if 'asm volatile("sti"' in cli_to_release:
        fail("non-returning deferred handoff must not re-enable interrupts before migration-guard release")


# ---------------------------------------------------------------------------
# Out-of-goal source contracts.
#
# These checks cover process/thread ABI, exec image publication, and child
# diagnostics.  They are intentionally retained here until their owning
# subsystem test files absorb them; none asserts scheduler transition behavior.
# ---------------------------------------------------------------------------


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
    create_thread_body = function_body(threading_source, "create_thread", "const mm::numa::AllocationContext& allocation_context")
    map_zeroed_body = function_body(threading_source, "map_zeroed_user_range")
    require_tokens(
        create_thread_body,
        [
            'map_zeroed_user_range(page_table, TLS_VIRT_ADDR, aligned_tls_size, "thread_tls_page", allocation_context, allocation_error)',
            "free_mapped_user_range(page_table, layout_base, layout_base + layout_size)",
            "auto const INITIAL_TID = static_cast<uint32_t>(initial_tid);",
            "write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x18, INITIAL_TID)",
            "thread->tls_phys_ptr = 0;",
        ],
        "fragmentation-safe initial user TLS and TCB construction", compact=True,
    )
    require_tokens(
        map_zeroed_body,
        [
            "mm::numa::try_allocate_pages(page_context, 1, mm::paging::PAGE_SIZE, mm::PhysicalPageOwner::USER_THREAD_TLS, mm::phys::PageInitialization::FULL_OVERWRITE, mm::numa::ReclaimMode::LEGACY_UMA, allocation_name)",
            "std::memset(page, 0, mm::paging::PAGE_SIZE)",
            "mm::paging::page_types::USER | mm::paging::PAGE_NX",
        ],
        "page-granular NX TLS and SafeStack construction", compact=True,
    )
    require_order(map_zeroed_body, "std::memset(page, 0, mm::paging::PAGE_SIZE)", "mm::virt::map_page",
                  "TLS and SafeStack backing must be cleared before user PTE publication")
    for forbidden in [
        'mm::phys::page_alloc(mm::PhysicalPageOwner::USER_THREAD_TLS, ALIGNED_TOTAL_SIZE, "thread_tls")',
        "mm::phys::page_split_to_order0(tls)",
    ]:
        if forbidden in create_thread_body:
            fail(f"initial user TLS still requires a contiguous physical run: {forbidden}")

    require_order(
        task_source,
        "this->pid = sched::task::get_next_pid();",
        "namespace_process_pid(*this), actual_tls_info, this, options.allocation_context,",
        "fresh process must allocate a PID before building the initial TCB", compact=True,
    )
    require_tokens(
        exec_source,
        [
            "loader::elf::inspect_tls(elf_buffer, static_cast<size_t>(FILE_SIZE), tls_info)",
            "mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, tls_info.tls_size",
            "PROCESS_OWNER->namespace_tid(), tls_info",
        ],
        "execve replacement TCB tid publication", compact=True,
    )
    require_order(
        thread_control_source,
        "mod::sched::threading::publish_tid_to_tcb(*parent, tcb_va, NEW_TID, false)",
        "mod::sched::post_task_round_robin_inherited(t, inherited_placement, PLACEMENT_SEED)",
        "secondary pthread TCB tid must be published before scheduling",
    )
    require_order(
        thread_control_source,
        "uint64_t const NEW_TID = t->namespace_tid();",
        "mod::sched::threading::publish_tid_to_tcb(*parent, tcb_va, NEW_TID, false)",
        "TCB and creation result must use the same prepublication local TID",
    )


def test_cpu_limit_charge_does_not_double_count_syscalls() -> None:
    source = SCHEDULER_CPP.read_text()
    charge = function_body(source, "charge_syscall_time_until")
    require_tokens(charge, ["resource::charge_cpu(task, ELAPSED_US)"], "syscall CPU policy charge")
    runtime = function_body(source, "account_task_runtime_delta")
    require_order(runtime, "consume_precharged_syscall_time(task, delta_us)", "resource::charge_cpu(task, UNCHARGED_US, user_us)",
                  "timer policy charge must subtract already charged syscall time")
    commit = function_body(source, "commit_handoff_task_at_return_boundary")
    require_order(commit, "transition_commit_handoff_locked(rq, task)",
                  "sys::signal::exit_current_on_cpu_limit_user_handoff()",
                  "CPU hard handoff expiry must follow own-stack ownership commit")
    require_tokens(commit, ["if (task != outgoing)"], "CPU handoff gate must only use incoming saved context")
    thread = function_body(TASK_CPP.read_text(), "Task::create_user_thread")
    require_order(thread, "resource::bind_thread_owner(*t, *parent)", "return t;", "thread must bind policy before publication")


def test_idle_picker_prunes_invalid_candidates() -> None:
    source = SCHEDULER_CPP.read_text()
    idle = source.split("// ---- Idle path:", 1)[1].split("// ---- Running task path:", 1)[0]
    require_tokens(idle, ["pick_best_eligible_for_switch_locked(rq, AVG, nullptr)"],
                   "idle pick must remove invalid candidates with normal transition bookkeeping")


def test_default_user_stack_reservation_covers_native_toolchain_links() -> None:
    mm_header = MM_HPP.read_text()
    match = re.search(r"USER_STACK_SIZE\s*=\s*(0x[0-9A-Fa-f]+|\d+)", mm_header)
    if match is None:
        fail("missing USER_STACK_SIZE definition")
    if int(match.group(1), 0) < 64 * 1024 * 1024:
        fail("default user stack must reserve at least 64MiB for native WOS toolchain links")

    threading_source = THREADING_CPP.read_text()
    create_thread_body = function_body(threading_source, "create_thread", "const mm::numa::AllocationContext& allocation_context")
    require_tokens(
        create_thread_body,
        [
            "thread->stack_phys_ptr = 0;",
            "ensure_stack_backing(thread, page_table, STACK_TOP - INITIAL_STACK_BYTES, STACK_TOP, policy_owner, allocation_context, allocation_error)",
        ],
        "large user stacks must remain lazily backed", compact=True,
    )
    require_tokens(
        threading_source,
        ["bool handle_lazy_stack_fault(Thread* thread, mm::paging::PageTable* page_table, uint64_t fault_addr, uint64_t rsp,",
         "task::Task* policy_owner)"],
        "large user stacks must retain lazy fault backing", compact=True,
    )


def test_execve_publishes_new_context_before_old_image_teardown() -> None:
    source = EXEC_CPP.read_text()
    body = function_body(source, "finish_native_exec_image")
    prepare = function_body(source, "wos_proc_execve_impl")
    commit = function_body(source, "commit_native_exec_image")
    require_tokens(commit, [
        "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
        "retired.pagemap_has_other_publishers = mod::sched::task_has_live_pagemap_sibling(task);",
        "retired.thread = task->thread;",
        "retired.pagemap = task->replace_pagemap_after_usercopy_quiescence(image.pagemap);",
        "task->thread = image.thread;",
    ], "execve replacement-context publication")
    for forbidden in ["destroy_user_space(", "release_pagemap(", "destroy_thread("]:
        if forbidden in commit:
            fail("image commit must leave retired storage cleanup to the unlocked caller")
    require_tokens(prepare, ["mm::virt::release_pagemap(new_pagemap, task->pid);"], "private preparation abort")
    require_tokens(body, ["PREPARED->mark_activated();"],
                   "execve replacement activation and preparation abort")
    cleanup = function_body(source, "discard_retired")
    require_tokens(cleanup, ["ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(m_retired.pagemap);",
                            "ker::mod::mm::virt::release_pagemap(m_retired.pagemap, m_origin_pid);",
                            "ker::mod::sched::threading::destroy_thread(m_retired.thread);"], "owned old-image cleanup")
    if not (body.index("commit_native_exec_image(task, *PREPARED)") < body.index("PREPARED->mark_activated();") <
            body.rindex("cleanup_exec_preparation_with_status(*task, 0)")):
        fail("old-image reclamation must follow publication and replacement activation")
    if "RetiredExecImage retired;" in body:
        fail("old-image storage must remain recoverable outside the syscall stack")


def test_procfs_status_exposes_child_lifecycle_debug_state() -> None:
    body = function_body(PROCFS_CPP.read_text(), "generate_status")
    require_tokens(
        body,
        [
            "child_events::diagnostics(*task, local_view)",
            "WaitingForPid",
            "WaitpidCompletionClaimed",
            "WaitpidLastRepairUs",
            "ChildEventCount",
            "ChildWaiterCount",
            "ChildMembershipState",
            "ExitInProgress",
            "ExitNotifyReady",
            "WaitedOn",
            "WakeupPending",
        ],
        "procfs child-lifecycle diagnostic state",
    )


def test_scheduler_does_not_own_waitpid_repair_engine() -> None:
    source = SCHEDULER_CPP.read_text()
    forbidden = [
        "complete_waitpid",
        "waitpid_repair_due",
        "WAITPID_REPAIR_FALLBACK",
        "WAITPID_COMPLETION_CLAIM_LEASE",
        "recover_stalled_waitpid_completion_claim",
        "orphaned_waitpid",
        "waiting_for_pid",
        "waitpid_publish_pending",
        "waitpid_completion_claimed",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail(f"scheduler must not own waitpid completion/repair state: {', '.join(present)}")


def test_only_active_syscalls_can_defer_rescheduling_to_syscall_exit() -> None:
    body = function_body(CONTEXT_SWITCH_CPP.read_text(), "defer_process_reschedule_to_syscall_exit")
    require_order(body, "task->syscall_account_start_us == 0", "task->deferred_task_switch = true",
                  "timer and completed-return callbacks cannot create an unserviceable syscall deferral")
    require_order(body, "stack_belongs_to_task(task, rsp)", "task->deferred_task_switch = true",
                  "syscall deferral still requires the current task's stack")


def test_process_cpu_observation_preserves_lifetime() -> None:
    resource = (ROOT / "modules/kern/src/platform/sched/resource.cpp").read_text()
    snapshot = function_body(resource, "copy_cpu_observation")
    require_tokens(snapshot, ["subject.resource_owner.get()", "process->resources.cpu_user_time_us", "process->resources.cpu_system_time_us"], "persistent CPU owner")
    require_order(snapshot, "resource_lock.unlock_irqrestore", "read_reaped_cpu_times", "no nested resource/lifecycle locks")
    for forbidden in ["find_task_by_pid", "new ", "copy_value_to_task"]:
        if forbidden in snapshot:
            fail("CPU snapshot must borrow existing owner without allocation or usercopy")
    time = (ROOT / "modules/kern/src/syscalls_impl/time/time.cpp").read_text()
    require_tokens(time, ["copy_cpu_observation(*task, observed)", "process_cpu_times(*task, process_times)", "task_cpu_time_us(*task)"], "process and thread clock scope")
    if "get_active_task" in time:
        fail("process clock cannot discard reclaimed-thread CPU history")
    proc = function_body(PROCFS_CPP.read_text(), "collect_proc_task_stats")
    require_tokens(proc, ["copy_cpu_observation(task, observed)", "stats.child_user_time_us = observed.child_user_time_us"], "procfs process CPU snapshot")
    if "candidate->user_time_us" in proc or "candidate->system_time_us" in proc:
        fail("live registry scan must not supply process lifetime CPU")


def test_job_control_stops_gate_all_runnable_selection() -> None:
    source = SCHEDULER_CPP.read_text()
    park = function_body(source, "park_job_control_stopped_task_locked")
    require_tokens(park, ["jobctl_stopped.load(std::memory_order_acquire)", "transition_detach_runnable_locked",
                         "transition_detach_all_wait_locked", "transition_make_waiting_locked"], "job-control waiting admission")
    if "publish_job_control_stop" in park or "resume_job_control_for_pending_signal" in park:
        fail("runqueue lock cannot enclose child lifecycle publication")
    for name, candidate in (("pick_best_eligible_for_switch_locked", "next"), ("pick_exit_switch_candidate_locked", "candidate")):
        body = function_body(source, name)
        require_tokens(body, [f"park_job_control_stopped_task_locked(rq, {candidate})"], "stopped candidate rejection")
        if body.index("park_job_control_stopped_task_locked") > body.index(f"return {candidate};"):
            fail("stopped candidate must be parked before selection returns it")
    if source.count("runnable_heap.pick_best_eligible(") != 2:
        fail("raw heap selection must be confined to the two stopped-task-filtering pickers")
    require_tokens(function_body(source, "process_tasks"), ["park_job_control_stopped_task_locked(rq, current_task)"],
                   "timer must deschedule an already-current stopped task")


def test_gc_pagemap_scan_counts_publishers_not_execution_eligibility() -> None:
    source = SCHEDULER_CPP.read_text()
    gc_scan = function_body(source, "gc_task_has_pagemap_sibling_locked")
    require_tokens(gc_scan, ["task_has_registered_pagemap_sibling(cur)"], "GC registered root ownership")
    for forbidden in ["get_active_task_at_safe", "try_acquire(", "TaskState::DEAD", "TaskState::EXITING"]:
        if forbidden in gc_scan:
            fail("GC may not hide an existing root publisher based on execution eligibility")
    scan = function_body(source, "task_has_registered_pagemap_sibling")
    require_order(scan, "global_task_registry_lock.lock_irqsave()", "active_task_slot(i)", "publisher scan holds registry lifetime lock")
    cleanup = function_body(source, "cleanup_waitable_zombie_resources")
    require_order(cleanup, "detach_pagemap_after_usercopy_quiescence()", "if (detached.should_free_pagemap)",
                  "non-owning zombie must stop publishing before last owner frees shared root")


def main() -> None:
    test_job_control_stops_gate_all_runnable_selection()
    test_process_cpu_observation_preserves_lifetime()
    test_gc_pagemap_scan_counts_publishers_not_execution_eligibility()
    architecture_tests = (
        test_only_active_syscalls_can_defer_rescheduling_to_syscall_exit,
        test_cpu_limit_charge_does_not_double_count_syscalls,
        test_idle_picker_prunes_invalid_candidates,
        test_scheduler_wait_check_arm_halt_is_irq_atomic,
        test_classified_restore_policy_owns_validation_and_return_boundaries,
        test_context_switch_updates_tss_rsp0_to_task_stack,
        test_deferred_user_switch_commits_after_stack_handoff,
        test_deferred_first_run_daemon_starts_on_kernel_thread_stack,
        test_migration_guard_release_follows_cli_at_nonreturning_handoff,
    )
    out_of_goal_tests = (
        test_user_thread_tcbs_publish_nonzero_tid_before_user_execution,
        test_default_user_stack_reservation_covers_native_toolchain_links,
        test_execve_publishes_new_context_before_old_image_teardown,
        test_procfs_status_exposes_child_lifecycle_debug_state,
        test_scheduler_does_not_own_waitpid_repair_engine,
    )
    for test in architecture_tests + out_of_goal_tests:
        test()
    print("scheduler architecture boundaries and out-of-goal source contracts hold")


if __name__ == "__main__":
    main()
