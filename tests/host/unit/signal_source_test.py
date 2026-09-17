#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
SYSCALL_ASM = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "syscall.asm"
GATES_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gates.cpp"
CONTEXT_SWITCH_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.cpp"
COREDUMP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "dbg" / "coredump.cpp"
PROCESS_CALLNUMS = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "process.h"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
WOS_PROCESS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "process.h"
WOS_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
WOS_MLIBC_CALLNUMS = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "callnums" / "process.h"
WOS_SYSDEPS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "mlibc" / "sysdeps.hpp"
STRACE_SRC_DIR = ROOT / "modules" / "strace" / "src"


def fail(message: str) -> None:
    raise AssertionError(message)


def read_strace_source() -> str:
    paths = [*sorted(STRACE_SRC_DIR.glob("*.cpp")), *sorted(STRACE_SRC_DIR.glob("*.hpp"))]
    return "\n".join(path.read_text() for path in paths)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:\b(?:auto|void)|extern\s+\"C\"\s+(?:auto|void))\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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
        fail(f"{context}: expected {first!r} before {second!r}")


def test_syscall_signal_delivery_updates_live_gs_scratch() -> None:
    source = SIGNAL_CPP.read_text()
    body = function_body(source, "check_pending_signals")

    require_tokens(
        source,
        [
            "auto live_syscall_user_rsp() -> uint64_t",
            "auto live_syscall_return_rip() -> uint64_t",
            "auto live_syscall_return_flags() -> uint64_t",
            "void write_live_syscall_return(uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags)",
            "void write_task_syscall_return(Task& task, uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags)",
            "auto user_signal_target_valid",
        ],
        "signal syscall scratch helpers",
    )
    require_tokens(
        body,
        [
            "uint64_t const USER_RSP = live_syscall_user_rsp();",
            "uint64_t const USER_RIP = live_syscall_return_rip();",
            "uint64_t const USER_RFLAGS = live_syscall_return_flags();",
            "write_task_syscall_return(*task, frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
            "write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
            "write_task_syscall_return(*task, FRAME_ADDR, HANDLER.handler, USER_RFLAGS);",
            "write_live_syscall_return(FRAME_ADDR, HANDLER.handler, USER_RFLAGS);",
        ],
        "syscall signal delivery must keep live GS scratch in sync",
    )
    require_order(
        body,
        "stack_write(stack_base, STACK_OFF_RCX, HANDLER.handler)",
        "write_live_syscall_return(FRAME_ADDR, HANDLER.handler, USER_RFLAGS);",
        "SYSRET target write must be paired with live GS scratch update",
    )
    require_order(
        body,
        "write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
        "return 1;",
        "sigreturn must restore live GS scratch before the iret return",
    )


def test_signal_iret_preserves_restored_user_r11() -> None:
    source = SYSCALL_ASM.read_text()

    require_order(
        source,
        "jne .signal_iret_return",
        "mov [rsp + 32], r10",
        "signal IRET must branch before the SYSRET-only r11 flags update",
    )
    require_tokens(
        source,
        [
            ".signal_iret_return:",
            "mov r11, [rdi + 32]",
        ],
        "signal IRET must restore r11 from the saved GPRegs block",
    )


def test_coredump_file_is_private_and_exclusively_created() -> None:
    source = COREDUMP_CPP.read_text()

    require_tokens(
        source,
        [
            "constexpr int O_WRONLY = 01;",
            "auto open_output_for_task(",
            "ker::vfs::vfs_openat(writer, ker::vfs::AT_FDCWD, path,",
            "O_WRONLY | ker::vfs::O_CREAT | ker::vfs::O_EXCL | ker::vfs::O_NOFOLLOW",
            "int const FD = open_output_for_task(ker::mod::sched::get_current_task(), path.data());",
        ],
        "coredump output open contract",
    )
    output = function_body(source, "open_output_for_task")
    require_tokens(output, ["0600"], "new coredump private creation mode")
    writer = function_body(source, "perform_coredump")
    if "vfs_unlink(" in writer or "O_TRUNC" in writer:
        fail("coredump output must not truncate or unlink a reused pathname")
    require_tokens(writer, ["private partial output retained"], "failed private output ownership")


def test_coredump_worker_releases_stolen_elf_buffer() -> None:
    source = COREDUMP_CPP.read_text()

    require_tokens(
        source,
        [
            "if (req.elf_buffer_shared) {",
            "ker::net::wki::wki_remote_compute_release_elf_buffer(req.elf_buffer);",
            "} else {",
            "delete[] req.elf_buffer;",
            "req.elf_buffer = nullptr;",
            "req.elf_buffer_size = 0;",
            "req.elf_buffer_shared = false;",
        ],
        "coredump worker ELF buffer ownership",
    )


def test_coredump_ring_has_explicit_slot_ownership_and_no_sequence_holes() -> None:
    source = COREDUMP_CPP.read_text()
    producer = function_body(source, "try_write_for_task")
    consumer = function_body(source, "coredump_task_fn")

    require_tokens(
        source,
        [
            "enum class CoreDumpSlotState : uint8_t",
            "EMPTY,",
            "WRITING,",
            "READY,",
            "std::atomic<CoreDumpSlotState> state{CoreDumpSlotState::EMPTY};",
            "uint32_t g_consume_cursor{0};",
        ],
        "coredump ring slot ownership",
    )
    require_tokens(
        producer,
        [
            "for (uint32_t offset = 0; offset < RING_SIZE; ++offset)",
            "candidate.state.compare_exchange_strong(expected, CoreDumpSlotState::WRITING",
            "slot->state.store(CoreDumpSlotState::EMPTY, std::memory_order_release);",
            "slot->state.store(CoreDumpSlotState::READY, std::memory_order_release);",
        ],
        "coredump producer bounded claim and publication",
    )
    require_tokens(
        consumer,
        [
            "for (uint32_t offset = 0; offset < RING_SIZE; ++offset)",
            "slot.state.load(std::memory_order_acquire) != CoreDumpSlotState::READY",
            "slot.state.store(CoreDumpSlotState::EMPTY, std::memory_order_release);",
            "g_consume_cursor = (IDX + 1) % RING_SIZE;",
        ],
        "coredump consumer scans around dropped requests",
    )
    if ".ready" in source or "g_read_seq" in source:
        fail("coredump ring must not use a racy ready bit or sequential read sequence")


def test_signal_targets_are_user_canonical_on_all_delivery_paths() -> None:
    signal_source = SIGNAL_CPP.read_text()

    require_tokens(
        signal_source,
        [
            "if (!user_signal_target_valid(HANDLER))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "direct syscall/interrupt signal target validation",
    )
    require_tokens(
        signal_source,
        [
            "auto user_signal_target_valid(const ker::mod::sched::task::Task::SigHandler& handler) -> bool",
            "if (delivery == DeferredSignalDelivery::USER_HANDLERS_ONLY && !is_user_signal_handler(HANDLER))",
            "if (is_user_signal_handler(HANDLER) && !user_signal_target_valid(HANDLER))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "deferred signal target validation",
    )


def test_interrupt_signal_delivery_is_gated_by_user_return_frame() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    signal_source = SIGNAL_CPP.read_text()
    helper_body = function_body(context_source, "check_pending_signals_for_return")
    timer_body = function_body(context_source, "wos_sched_timer")
    exit_body = function_body(context_source, "wos_jump_to_next_task_no_save")

    require_tokens(
        context_source,
        [
            "inline void check_pending_signals_for_return(cpu::GPRegs& gpr, gates::InterruptFrame& frame)",
            "saved_frame_class_is_valid(return_task, frame)",
        ],
        "context-switch signal return helper",
    )
    require_tokens(
        helper_body,
        [
            "auto* return_task = sched::get_return_task();",
            "saved_frame_restore_policy(return_task->context.saved_frame_class)",
            "!POLICY.deliver_signals",
            "!saved_frame_class_is_valid(return_task, frame)",
            "sys::signal::check_pending_signals_interrupt(gpr, frame);",
            "sys::signal::check_pending_signals_handoff(return_task, gpr, frame);",
        ],
        "return-frame signal gate",
    )
    require_order(
        helper_body,
        "auto* return_task = sched::get_return_task();",
        "sys::signal::check_pending_signals_interrupt(gpr, frame);",
        "the durable return-frame class must be checked before signal delivery",
    )
    require_tokens(
        signal_source,
        [
            "auto interrupt_frame_is_user_return(sched::task::Task* task, const gates::InterruptFrame& frame) -> bool",
            "context_switch::classify_saved_frame(task, frame, sched::task::SavedFrameOrigin::INTERRUPT",
            "saved_frame_restore_policy(FRAME_CLASS).deliver_signals",
            "!sched::task::saved_frame_restore_policy(task->context.saved_frame_class).deliver_signals",
            "!context_switch::saved_frame_class_is_valid(task, task->context.frame)",
        ],
        "signal delivery classified user-return gates",
    )
    require_tokens(
        timer_body,
        [
            "sched::process_tasks(*gpr_ptr, *frame_ptr);",
            "check_pending_signals_for_return(*gpr_ptr, *frame_ptr);",
            'validate_classified_frame_for_restore(return_task, *frame_ptr, RETURN_FRAME_CLASS, "timer-return");',
        ],
        "timer return signal gate",
    )
    require_tokens(
        exit_body,
        [
            "sched::jump_to_next_task(*gpr_ptr, *frame_ptr);",
            "check_pending_signals_for_return(*gpr_ptr, *frame_ptr);",
            'validate_saved_frame_for_restore(return_task, *frame_ptr, "exit-return");',
        ],
        "exit return signal gate",
    )
    for body, context in [(timer_body, "timer"), (exit_body, "exit")]:
        if "check_pending_signals_interrupt(*gpr_ptr, *frame_ptr)" in body:
            fail(f"{context} return path must not call interrupt signal delivery before the frame-mode gate")


def test_synchronous_user_exceptions_can_reach_installed_signal_handlers_before_coredump() -> None:
    signal_source = SIGNAL_CPP.read_text()
    gates_source = GATES_CPP.read_text()
    helper_body = function_body(signal_source, "deliver_synchronous_signal_interrupt")
    exception_body = function_body(gates_source, "exception_handler")

    require_tokens(
        signal_source,
        [
            "auto deliver_synchronous_signal_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame, int signo) -> bool",
        ],
        "synchronous exception signal helper",
    )
    require_tokens(
        helper_body,
        [
            "if (!is_user_signal_handler(HANDLER) || (task->signal_mask_bits() & (1ULL << IDX)) != 0)",
            "if (!user_signal_target_valid(HANDLER))",
            "sigframe.saved_rip = frame.rip;",
            "sigframe.saved_rsp = frame.rsp;",
            "sigframe.saved_retval = gpr.rax;",
            "frame.rip = HANDLER.handler;",
            "frame.rsp = FRAME_ADDR;",
            "task->in_signal_handler = true;",
            "return true;",
        ],
        "synchronous exception helper must build the existing interrupt signal frame",
    )
    if "sig_pending" in helper_body:
        fail("synchronous exception helper must deliver the requested signal directly, not the first pending signal")
    require_tokens(
        exception_body,
        [
            "if (ker::mod::debug::ptrace::report_user_exception_stop(gpr, frame, FATAL_SIGNAL, FATAL_ADDRESS, frame.int_num))",
            "if (ker::mod::sys::signal::deliver_synchronous_signal_interrupt(gpr, frame, static_cast<int>(FATAL_SIGNAL)))",
            "ker::mod::dbg::coredump::try_write_for_task(current_task_for_dump, gpr, frame, cr2, cr3, apic::get_apic_id());",
        ],
        "user exception path must try handled signal delivery before coredump",
    )
    require_order(
        exception_body,
        "ker::mod::debug::ptrace::report_user_exception_stop",
        "ker::mod::sys::signal::deliver_synchronous_signal_interrupt",
        "ptrace must observe the exception before direct signal delivery",
    )
    require_order(
        exception_body,
        "ker::mod::sys::signal::deliver_synchronous_signal_interrupt",
        "ker::mod::dbg::coredump::try_write_for_task",
        "handled synchronous exceptions must not be coredumped first",
    )


def test_interrupt_and_scheduler_signal_paths_use_mapped_only_usercopy() -> None:
    source = SIGNAL_CPP.read_text()

    require_tokens(
        source,
        [
            "auto write_signal_frame_mapped(",
            "usercopy::copy_to_task_mapped(task, user_addr, src, size)",
            "void sync_task_signal_mask_cache_mapped(sched::task::Task* task)",
        ],
        "mapped-only signal usercopy helpers",
    )

    restore = function_body(source, "restore_deferred_sigreturn")
    require_tokens(
        restore,
        ["read_signal_frame_mapped(*task, FRAME_START, frame)", "sync_task_signal_mask_cache_mapped(task)"],
        "deferred sigreturn must not fault user pages in scheduler context",
    )
    if "read_signal_frame(*task" in restore or "sync_task_signal_mask_cache(task)" in restore:
        fail("deferred sigreturn contains faulting usercopy")

    for name in [
        "check_pending_signals_interrupt",
        "deliver_synchronous_signal_interrupt",
        "check_pending_signals_handoff",
        "check_pending_signals_deferred",
    ]:
        body = function_body(source, name)
        require_tokens(
            body,
            ["write_signal_frame_mapped(*task", "sync_task_signal_mask_cache_mapped(task)"],
            f"{name} mapped-only usercopy",
        )
        if "write_signal_frame(*task" in body or "sync_task_signal_mask_cache(task)" in body:
            fail(f"{name} contains faulting usercopy")

    interrupt = function_body(source, "check_pending_signals_interrupt")
    frame_copy = interrupt.find("if (!write_signal_frame_mapped(*task, FRAME_ADDR, sigframe))")
    frame_commit = interrupt.find("if (!claim.commit())", frame_copy)
    if frame_copy < 0 or frame_commit < frame_copy:
        fail("an unmapped asynchronous signal frame must remain pending")


def test_sigpending_is_wired_through_wos_sysdeps() -> None:
    process_callnums = PROCESS_CALLNUMS.read_text()
    process_source = PROCESS_CPP.read_text()
    wos_mlibc_callnums = WOS_MLIBC_CALLNUMS.read_text()
    wos_sysdeps_header = WOS_SYSDEPS_H.read_text()
    wos_process_header = WOS_PROCESS_H.read_text()
    wos_sysdeps = WOS_SYSDEPS_CPP.read_text()
    strace_source = read_strace_source()

    require_tokens(
        process_callnums,
        [
            "SIGPENDING,     // 41",
        ],
        "process syscall ABI exposes appended SIGPENDING op",
    )
    require_tokens(
        process_source,
        [
            "auto wos_proc_sigpending(uint64_t set_ptr) -> uint64_t",
            "std::array<uint64_t, 16> set{};",
            "set.at(0) = task->signal_delivery_pending_bits() & task->signal_mask_bits();",
            "bool const COPIED = ker::mod::sys::usercopy::copy_to_task(*task, set_ptr, set.data(), set.size() * sizeof(set.at(0)));",
            "case abi::process::procmgmt_ops::SIGPENDING:",
            "return wos_proc_sigpending(a2);",
        ],
        "kernel sigpending syscall",
    )
    require_tokens(
        wos_mlibc_callnums,
        [
            "SIGPENDING,    // 41",
        ],
        "mlibc syscall ABI copy exposes appended SIGPENDING op",
    )
    require_tokens(
        wos_sysdeps_header,
        [
            "Sigpending,",
        ],
        "wos mlibc sysdep tag list advertises sigpending",
    )
    require_tokens(
        wos_process_header,
        [
            "inline int64_t sigpending(void *set)",
            "(uint64_t)abi::process::procmgmt_ops::SIGPENDING",
        ],
        "wos mlibc sigpending syscall wrapper",
    )
    require_tokens(
        wos_sysdeps,
        [
            "int Sysdeps<Sigpending>::operator()(sigset_t *set)",
            "ker::process::sigpending((void *)set)",
        ],
        "wos mlibc sigpending sysdep",
    )
    require_tokens(
        strace_source,
        [
            "case ker::abi::process::procmgmt_ops::SIGPENDING:",
            'return "sigpending";',
        ],
        "strace process syscall name table",
    )


def test_cpu_exhaustion_waits_for_safe_user_return() -> None:
    source = SIGNAL_CPP.read_text()
    helper = function_body(source, "deliver_cpu_limits_at_user_return")
    require_tokens(helper, ["task != sched::get_current_task()", "pending_cpu_signals.exchange(0",
                            "wos_proc_exit_signal(WOS_SIGKILL)"], "CPU delivery owns the current user return")
    require_tokens(helper, ["pending_cpu_signals.fetch_and(~SOFT_MASK", "& SOFT_MASK"],
                   "incoming user handoff must transfer soft events without consuming hard expiry")
    committed = function_body(source, "exit_current_on_cpu_limit_user_handoff")
    require_tokens(committed, ["task_owns_current_kernel_stack(task)", ".deliver_signals",
                               "saved_frame_class_is_valid(task, task->context.frame)"],
                   "hard handoff expiry requires own stack and a validated user return")
    if "in_signal_handler" in committed:
        fail("CPU hard expiry must not defer for an active signal handler")
    unsafe = function_body(source, "exit_current_on_pending_fatal_default_signal")
    require_tokens(unsafe, [".deliver_signals", "saved_frame_class_is_valid(task, task->context.frame)"],
                   "fatal handoff must preserve scoped resources on suspended kernel stacks")
    for operation in ("exit_current_if_process_exit_requested()", "task->consume_signal_for_delivery"):
        if unsafe.index("saved_frame_class_is_valid") > unsafe.index(operation):
            fail("fatal handoff must validate user return before consuming signals or exiting")
    if not unsafe.index("saved_frame_class_is_valid") < unsafe.index("UserReturnFpuAuthorization return_fpu") < unsafe.index("resume_job_control_for_pending_signal"):
        fail("blocking handoff work must retain its already validated outer user-return FPU authorization")
    deferred = function_body(source, "check_pending_signals_deferred")
    if deferred.index("saved_frame_class_is_valid") > deferred.index("exit_current_if_process_exit_requested()"):
        fail("deferred process exit also requires a validated user return")
    if "pending_cpu_signals" in unsafe or "deliver_cpu_limits_at_user_return" in unsafe:
        fail("kernel-stack fatal handoff must not consume deferred CPU events")
    for name in ("check_pending_signals_interrupt", "check_pending_signals_handoff"):
        body = function_body(source, name)
        gate = body.index("if (!interrupt_frame_is_user_return(task, frame))")
        delivery = body.index("deliver_cpu_limits_at_user_return(task)")
        handler = body.index("pending_signals_for_user_delivery(task)")
        if not gate < delivery < handler:
            fail(f"{name} must gate CPU delivery on user return and enforce hard expiry during handlers")
    charge_source = (ROOT / "modules/kern/src/platform/sched/resource.cpp").read_text()
    charge = function_body(charge_source, "charge_cpu")
    if "signal_add_pending_mask" in charge or "find_task" in charge:
        fail("IRQ CPU charging must buffer signals without registry lookup or early fatal delivery")


def test_sigkill_preempts_active_handler_only_at_safe_return() -> None:
    source = SIGNAL_CPP.read_text()
    mask = function_body(source, "sanitized_signal_mask")
    require_tokens(mask, ["WOS_SIGKILL - 1", "WOS_SIGSTOP - 1", "mask & ~"], "uncatchable mask bits excluded")
    for name in ("restore_deferred_sigreturn", "check_pending_signals"):
        require_tokens(function_body(source, name), ["signal_mask_store(sanitized_signal_mask(frame.saved_mask))"],
                       "user-edited signal return mask cannot block KILL/STOP")
    if "signal_add_mask_bits(HANDLER.mask)" in source:
        fail("handler mask publication must exclude KILL/STOP on every delivery path")
    if source.count("signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask))") != 5:
        fail("all five handler delivery paths must sanitize their mask")
    helper = function_body(source, "handle_pending_sigkill_at_user_return")
    require_tokens(helper, ["signal_pending_bits()", "WOS_SIGKILL - 1", "task != sched::get_current_task()",
                            "!task_owns_current_kernel_stack(task)",
                            "wos_proc_exit_signal(WOS_SIGKILL)"], "uncatchable KILL with owned-stack delivery")
    if "signal_clear_pending_mask" in helper:
        fail("KILL must remain visible to ptrace parking until exit_in_progress protects cleanup")
    if "in_signal_handler" in helper or "signal_deliverable_bits" in helper:
        fail("active handlers and mask state must not hide uncatchable SIGKILL")
    if helper.index("task_owns_current_kernel_stack") > helper.index("wos_proc_exit_signal"):
        fail("incoming handoff must leave KILL pending until current-stack ownership")
    for name in ("check_pending_signals", "check_pending_signals_interrupt", "check_pending_signals_handoff",
                 "check_pending_signals_deferred", "exit_current_on_pending_fatal_default_signal"):
        body = function_body(source, name)
        delivery = body.index("handle_pending_sigkill_at_user_return(task)")
        deferral = "task->in_signal_handler" if name in ("check_pending_signals", "exit_current_on_pending_fatal_default_signal") else "pending_signals_for_user_delivery(task)"
        if delivery > body.index(deferral):
            fail(f"{name} must prioritize KILL before handler deferral")
        if name == "check_pending_signals":
            if delivery > body.index("if (task->do_sigreturn)"):
                fail("KILL must not be delayed by restoring a signal frame")
        else:
            gate = "interrupt_frame_is_user_return" if name in ("check_pending_signals_interrupt", "check_pending_signals_handoff") else "saved_frame_class_is_valid"
            if delivery < body.index(gate):
                fail(f"{name} must retain the safe user-return boundary")


def test_sigstop_preempts_active_handler_without_nesting() -> None:
    source = SIGNAL_CPP.read_text()
    stop_action = function_body(source, "apply_job_control_stop")
    if "deferred_task_switch = true" in stop_action:
        fail("late user-return stop must not create an unserviceable syscall transition")
    require_tokens(stop_action, ["debug_stop_task(task)", "park_current_job_control_stop(*task)"],
                   "stop uses timer reschedule or owned syscall parking")
    selector = function_body(source, "pending_signals_for_user_delivery")
    require_tokens(selector, ["signal_pending_bits() & STOP_MASK", "WOS_SIGSTOP - 1", "return STOP_MASK",
                              "task->in_signal_handler ? 0 : task->signal_deliverable_bits()"],
                   "STOP priority without changing ordinary handler deferral")
    if selector.index("return STOP_MASK") > selector.index("task->in_signal_handler"):
        fail("STOP must bypass active handler deferral")
    syscall = function_body(source, "check_pending_signals")
    stop = syscall.index("apply_job_control_stop(task, WOS_SIGSTOP, true)")
    if not syscall.index("handle_pending_sigkill_at_user_return(task)") < stop < syscall.index("if (task->do_sigreturn)"):
        fail("syscall STOP must follow KILL and precede sigreturn")
    require_tokens(syscall[stop:syscall.index("if (task->do_sigreturn)")],
                   ["exit_current_if_process_exit_requested()", "handle_pending_sigkill_at_user_return(task)"],
                   "KILL after a parked stop must precede user return")
    for name in ("check_pending_signals_interrupt", "check_pending_signals_handoff", "check_pending_signals_deferred"):
        body = function_body(source, name)
        require_tokens(body, ["pending_signals_for_user_delivery(task)"], "validated STOP selection")
        if "park_current_job_control_stop" in body or "WOS_SIGSTOP, true" in body:
            fail(f"{name} must not block on a return-preparation stack")
    deferred = function_body(source, "check_pending_signals_deferred")
    if deferred.index("delivery == DeferredSignalDelivery::USER_HANDLERS_ONLY") > deferred.index("apply_job_control_stop"):
        fail("incoming deferred preparation must retain default-action restriction")


if __name__ == "__main__":
    test_sigstop_preempts_active_handler_without_nesting()
    test_sigkill_preempts_active_handler_only_at_safe_return()
    test_cpu_exhaustion_waits_for_safe_user_return()
    test_syscall_signal_delivery_updates_live_gs_scratch()
    test_signal_iret_preserves_restored_user_r11()
    test_coredump_file_is_private_and_exclusively_created()
    test_coredump_worker_releases_stolen_elf_buffer()
    test_coredump_ring_has_explicit_slot_ownership_and_no_sequence_holes()
    test_signal_targets_are_user_canonical_on_all_delivery_paths()
    test_interrupt_signal_delivery_is_gated_by_user_return_frame()
    test_synchronous_user_exceptions_can_reach_installed_signal_handlers_before_coredump()
    test_interrupt_and_scheduler_signal_paths_use_mapped_only_usercopy()
    test_sigpending_is_wired_through_wos_sysdeps()
    print("signal syscall return invariants hold")
