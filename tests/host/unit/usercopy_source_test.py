#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
USERCOPY_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "usercopy.hpp"
USERCOPY_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "usercopy.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
WAITPID_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "waitpid.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
FUTEX_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "futex" / "futex.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SIGNAL_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "signal.cpp"
PTRACE_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "debug" / "ptrace.cpp"
REMOTE_COMPUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_compute.cpp"


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
        source.find(f"inline auto {name}("),
        source.find(f"inline void {name}("),
        source.find(f"int64_t {name}("),
    ]
    start = min((candidate for candidate in candidates if candidate >= 0), default=-1)
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def require_common_usercopy_helper(header: str, source: str) -> None:
    for snippet in [
        "namespace ker::mod::sys::usercopy",
        "USER_ADDR_LIMIT = 0x0000800000000000ULL",
        "copy_from_task",
        "copy_to_task",
        "copy_to_task_mapped",
        "copy_cstring_from_task",
        "copy_value_from_task",
        "copy_value_to_task",
        "copy_value_to_task_mapped",
    ]:
        if snippet not in header:
            fail(f"usercopy header missing public helper: {snippet}")

    for snippet in [
        "__builtin_add_overflow",
        "mm::virt::ensure_user_page_writable(&task, CUR)",
        "mm::virt::translate(task.pagemap, CUR)",
        "mm::addr::get_virt_pointer(PHYS)",
        "copy_to_task_common(task, user_addr, src, size, true)",
        "copy_to_task_common(task, user_addr, src, size, false)",
    ]:
        if snippet not in source:
            fail(f"usercopy implementation missing safety snippet: {snippet}")


def require_process_syscalls_use_usercopy(source: str) -> None:
    for name in [
        "wos_proc_uname",
        "wos_proc_sigaction",
        "wos_proc_sigaltstack",
        "wos_proc_sigprocmask",
        "wos_proc_sigpending",
        "wos_proc_sigsuspend",
        "wos_proc_clone_vm",
        "wos_proc_prctl",
        "wos_proc_arch_prctl",
        "wos_proc_setwkitarget",
        "wos_proc_getwkitarget",
    ]:
        body = function_body(source, name)
        if "usercopy::" not in body:
            fail(f"{name} must use platform/sys/usercopy for user pointers")

    process_body = function_body(source, "process")
    for snippet in [
        "copy_value_to_task(*task, a2, task->uid)",
        "copy_value_to_task(*task, a3, task->euid)",
        "copy_value_to_task(*task, a4, task->suid)",
        "copy_value_to_task(*task, a2, task->gid)",
        "copy_value_to_task(*task, a3, task->egid)",
        "copy_value_to_task(*task, a4, task->sgid)",
        "copy_to_task(*task, a2, name, LEN + 1)",
        "copy_from_task(*task, a2, name.data(), len)",
    ]:
        if snippet not in process_body:
            fail(f"process syscall dispatch still lacks usercopy snippet: {snippet}")


def require_waitpid_outputs_are_preflighted(source: str) -> None:
    for snippet in [
        "waitpid_outputs_writable",
        "usercopy::ensure_writable(task, reinterpret_cast<uint64_t>(status), sizeof(*status))",
        "usercopy::ensure_writable(task, rusage_vaddr, sizeof(KernRusage))",
        "write_status_to_user",
        "write_rusage_to_user",
    ]:
        if snippet not in source:
            fail(f"waitpid output handling missing snippet: {snippet}")

    body = function_body(source, "wos_proc_waitpid")
    preflight = body.find("waitpid_outputs_writable(*current_task, status, rusage_vaddr)")
    first_claim = body.find("claim_exited_child")
    if preflight < 0 or first_claim < 0 or preflight > first_claim:
        fail("waitpid must validate output pointers before claiming/consuming children")

    if "*status =" in source or "fill_rusage(PHYS" in source or "get_virt_pointer(rusage" in source:
        fail("waitpid must not write status/rusage through raw user pointers or physical aliases")


def require_deferred_waiters_use_usercopy() -> None:
    files = {
        "exit": EXIT_CPP.read_text(),
        "scheduler": SCHEDULER_CPP.read_text(),
        "signal": SIGNAL_CPP.read_text(),
        "ptrace": PTRACE_CPP.read_text(),
        "remote_compute": REMOTE_COMPUTE_CPP.read_text(),
    }
    for label, source in files.items():
        if "wait_status_user_addr" in source and "usercopy::copy_value_to_task" not in source:
            fail(f"{label} wait-status completion must use usercopy")
        forbidden = ["STATUS_PHYS", "RUSAGE_PHYS", "get_virt_pointer(STATUS_PHYS)", "get_virt_pointer(RUSAGE_PHYS)"]
        for snippet in forbidden:
            if snippet in source:
                fail(f"{label} still writes waitpid output through physical alias: {snippet}")

    for source, name in [
        (EXIT_CPP.read_text(), "complete_exit_wait"),
        (SCHEDULER_CPP.read_text(), "complete_waitpid_exit_for_scheduler"),
        (SCHEDULER_CPP.read_text(), "complete_registered_waitpid_exit_for_scheduler"),
        (SCHEDULER_CPP.read_text(), "complete_waitpid_ptrace_stop_for_scheduler"),
        (SIGNAL_CPP.read_text(), "complete_waitpid_stop_waiter"),
        (PTRACE_CPP.read_text(), "complete_trace_wait"),
        (REMOTE_COMPUTE_CPP.read_text(), "try_complete_proxy_wait"),
    ]:
        body = function_body(source, name)
        if "static_cast<uint64_t>(-EFAULT)" not in body:
            fail(f"{name} must report EFAULT when deferred output copy fails")


def require_futex_timeout_and_address_usercopy(source: str) -> None:
    timeout_body = function_body(source, "relative_timeout_us")
    signature_start = source.find("auto relative_timeout_us(")
    signature_end = source.find("{", signature_start)
    if signature_start < 0 or signature_end < 0 or "mod::sched::task::Task& task" not in source[signature_start:signature_end]:
        fail("futex timeout helper must accept the current task for usercopy")
    for snippet in [
        "mod::sys::usercopy::copy_value_from_task(task, reinterpret_cast<uint64_t>(timeout), ts)",
        "return -EFAULT;",
    ]:
        if snippet not in timeout_body:
            fail(f"futex timeout must be copied from user memory; missing {snippet}")

    for name in ["futex_wait", "futex_wake"]:
        body = function_body(source, name)
        guard = body.find("mod::sys::usercopy::range_valid(user_vaddr, sizeof(int))")
        translate = body.find("mod::mm::virt::translate(current_task->pagemap, user_vaddr)")
        if guard < 0 or translate < 0 or guard > translate:
            fail(f"{name} must reject non-user futex addresses before translation")


def main() -> None:
    require_common_usercopy_helper(USERCOPY_HPP.read_text(), USERCOPY_CPP.read_text())
    require_process_syscalls_use_usercopy(PROCESS_CPP.read_text())
    require_waitpid_outputs_are_preflighted(WAITPID_CPP.read_text())
    require_deferred_waiters_use_usercopy()
    require_futex_timeout_and_address_usercopy(FUTEX_CPP.read_text())


if __name__ == "__main__":
    main()
