#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
GATES_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gates.cpp"
CONTEXT_SWITCH_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "context_switch.cpp"
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
            "write_task_syscall_return(*task, FRAME_ADDR, handler.handler, USER_RFLAGS);",
            "write_live_syscall_return(FRAME_ADDR, handler.handler, USER_RFLAGS);",
        ],
        "syscall signal delivery must keep live GS scratch in sync",
    )
    require_order(
        body,
        "stack_write(stack_base, STACK_OFF_RCX, handler.handler)",
        "write_live_syscall_return(FRAME_ADDR, handler.handler, USER_RFLAGS);",
        "SYSRET target write must be paired with live GS scratch update",
    )
    require_order(
        body,
        "write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);",
        "return 1;",
        "sigreturn must restore live GS scratch before the iret return",
    )


def test_signal_targets_are_user_canonical_on_all_delivery_paths() -> None:
    signal_source = SIGNAL_CPP.read_text()

    require_tokens(
        signal_source,
        [
            "if (!user_signal_target_valid(handler))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "direct syscall/interrupt signal target validation",
    )
    require_tokens(
        signal_source,
        [
            "auto user_signal_target_valid(const ker::mod::sched::task::Task::SigHandler& handler) -> bool",
            "if (delivery == DeferredSignalDelivery::USER_HANDLERS_ONLY && !is_user_signal_handler(handler))",
            "if (is_user_signal_handler(handler) && !user_signal_target_valid(handler))",
            "ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);",
        ],
        "deferred signal target validation",
    )


def test_interrupt_signal_delivery_is_gated_by_user_return_frame() -> None:
    context_source = CONTEXT_SWITCH_CPP.read_text()
    helper_body = function_body(context_source, "check_pending_signals_for_return")
    timer_body = function_body(context_source, "wos_sched_timer")
    exit_body = function_body(context_source, "wos_jump_to_next_task_no_save")

    require_tokens(
        context_source,
        [
            "inline auto is_user_return_frame(const gates::InterruptFrame& frame) -> bool",
            "inline void check_pending_signals_for_return(cpu::GPRegs& gpr, gates::InterruptFrame& frame)",
        ],
        "context-switch signal return helper",
    )
    require_tokens(
        helper_body,
        [
            "if (!is_user_return_frame(frame))",
            "auto* return_task = sched::get_return_task();",
            "sys::signal::check_pending_signals_interrupt(gpr, frame);",
            "sys::signal::check_pending_signals_handoff(return_task, gpr, frame);",
        ],
        "return-frame signal gate",
    )
    require_order(
        helper_body,
        "if (!is_user_return_frame(frame))",
        "auto* return_task = sched::get_return_task();",
        "kernel return frames must be rejected before querying scheduler current/return task state",
    )
    require_tokens(
        timer_body,
        [
            "sched::process_tasks(*gpr_ptr, *frame_ptr);",
            "check_pending_signals_for_return(*gpr_ptr, *frame_ptr);",
            'validate_kernel_frame(*frame_ptr, return_task, "timer-return");',
        ],
        "timer return signal gate",
    )
    require_tokens(
        exit_body,
        [
            "sched::jump_to_next_task(*gpr_ptr, *frame_ptr);",
            "check_pending_signals_for_return(*gpr_ptr, *frame_ptr);",
            'validate_kernel_frame(*frame_ptr, return_task, "exit-return");',
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
            "if (!is_user_signal_handler(handler) || (task->sig_mask & (1ULL << IDX)) != 0)",
            "if (!user_signal_target_valid(handler))",
            "sigframe.saved_rip = frame.rip;",
            "sigframe.saved_rsp = frame.rsp;",
            "sigframe.saved_retval = gpr.rax;",
            "frame.rip = handler.handler;",
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
            "std::memset(set, 0, 1024 / 8);",
            "set[0] = task->sig_pending & task->sig_mask;",
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


if __name__ == "__main__":
    test_syscall_signal_delivery_updates_live_gs_scratch()
    test_signal_targets_are_user_canonical_on_all_delivery_paths()
    test_interrupt_signal_delivery_is_gated_by_user_return_frame()
    test_synchronous_user_exceptions_can_reach_installed_signal_handlers_before_coredump()
    test_sigpending_is_wired_through_wos_sysdeps()
    print("signal syscall return invariants hold")
