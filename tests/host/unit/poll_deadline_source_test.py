#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EPOLL_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "epoll.cpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    start = source.find(f"auto {name}(")
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function body not found")
    depth = 0
    for pos in range(brace, len(source)):
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : pos]
    fail(f"{name} function body is unterminated")


def body_after_marker(source: str, marker: str) -> str:
    start = source.find(marker)
    if start < 0:
        fail(f"marker not found: {marker}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"function body not found after marker: {marker}")
    depth = 0
    for pos in range(brace, len(source)):
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : pos]
    fail(f"function body after marker is unterminated: {marker}")


def require_order(source: str, *snippets: str) -> None:
    cursor = -1
    for snippet in snippets:
        pos = source.find(snippet, cursor + 1)
        if pos < 0:
            fail(f"missing ordered snippet: {snippet}")
        cursor = pos


def require_poll_deadline_is_saturating(path: Path) -> None:
    source = path.read_text()
    timeout_body = function_body(source, "poll_timeout_us_from_ms")
    deadline_body = function_body(source, "poll_deadline_after_ms")
    begin_body = function_body(source, "begin_poll_timeout")

    require_order(
        timeout_body,
        "auto const TIMEOUT_MS = static_cast<uint64_t>(timeout_ms)",
        "if (TIMEOUT_MS > UINT64_MAX / USEC_PER_MSEC)",
        "return UINT64_MAX",
        "return TIMEOUT_MS * USEC_PER_MSEC",
    )
    require_order(
        deadline_body,
        "uint64_t const TIMEOUT_US = poll_timeout_us_from_ms(timeout_ms)",
        "uint64_t const NOW_US = ker::mod::time::get_us()",
        "if (UINT64_MAX - NOW_US < TIMEOUT_US)",
        "return UINT64_MAX",
        "return NOW_US + TIMEOUT_US",
    )
    require_order(
        begin_body,
        "if (task == nullptr || timeout_ms <= 0)",
        "if (task->poll_wait_deadline_us == 0)",
        "task->poll_wait_deadline_us = poll_deadline_after_ms(timeout_ms)",
    )

    forbidden = [
        "ker::mod::time::get_us() + (static_cast<uint64_t>(timeout_ms) * USEC_PER_MSEC)",
        "ker::mod::time::get_us() + static_cast<uint64_t>(timeout_ms) * USEC_PER_MSEC",
    ]
    present = [snippet for snippet in forbidden if snippet in source]
    if present:
        fail(f"{path.relative_to(ROOT)} still uses wrapping poll deadline arithmetic: {present[0]}")


def require_poll_waits_are_signal_interruptible() -> None:
    epoll_source = EPOLL_CPP.read_text()
    epoll_body = function_body(epoll_source, "epoll_pwait")
    if "task->sig_pending & ~task->sig_mask" in epoll_body:
        fail("epoll_pwait bypasses signal disposition checks")
    require_order(
        epoll_body,
        "if (task->has_interrupting_signal_pending())",
        "bool can_block = (inst->count > 0)",
        "if (task->has_interrupting_signal_pending())",
        'ker::mod::sched::preemptible_syscall_park("epoll_wait", poll_wait_kind, DEADLINE_US)',
    )

    poll_source = SYS_NET_CPP.read_text()
    poll_body = function_body(poll_source, "run_poll_wait")
    require_order(
        poll_body,
        "if (current_task_has_deliverable_signal())",
        "bool const CAN_BLOCK = register_poll_waiters",
        "if (current_task_has_deliverable_signal())",
        "ker::mod::sched::preemptible_syscall_park(wait_channel, poll_wait_kind, DEADLINE_US)",
    )


def require_select_is_implemented_on_poll_core() -> None:
    source = SYS_NET_CPP.read_text()
    select_start = source.find("case ker::abi::net::ops::SELECT:")
    poll_start = source.find("case ker::abi::net::ops::POLL:")
    default_start = source.find("\n        default:", poll_start)
    if select_start < 0:
        fail("select syscall case not found")
    if poll_start < 0 or default_start < 0:
        fail("poll syscall case not found")
    if not select_start < poll_start < default_start:
        fail("select and poll syscall cases are not in the expected switch order")

    select_case = body_after_marker(source, "case ker::abi::net::ops::SELECT:")
    require_order(
        select_case,
        "if (a1 > WOS_FD_SETSIZE)",
        "run_select(static_cast<size_t>(a1)",
        "reinterpret_cast<const KSelectTimeval*>(a5)",
    )

    select_timeout_body = function_body(source, "select_timeout_ms")
    require_order(
        select_timeout_body,
        "timeout_ms = -1",
        "if (timeout == nullptr)",
        "if (timeout->tv_sec < 0 || timeout->tv_usec < 0 || timeout->tv_usec >= SELECT_USEC_PER_SEC)",
        "timeout_ms = SELECT_TIMEOUT_MAX_MS",
    )

    select_body = function_body(source, "run_select")
    require_order(
        select_body,
        "select_timeout_ms(timeout, timeout_ms)",
        "new (std::nothrow) KPollFd[watched_fds]{}",
        'run_poll_wait(poll_fds, watched_fds, timeout_ms, "select")',
        "WOS_POLLNVAL",
        "return -EBADF",
        "select_fd_zero(readfds)",
        "select_fd_set_bit(FD, readfds)",
        "select_fd_set_bit(FD, writefds)",
        "select_fd_set_bit(FD, exceptfds)",
    )

    poll_case = body_after_marker(source, "case ker::abi::net::ops::POLL:")
    require_order(
        poll_case,
        "auto* fds = reinterpret_cast<KPollFd*>(a1)",
        'run_poll_wait(fds, nfds, timeout, "poll")',
    )


def require_preemptible_parking_rechecks_signals() -> None:
    scheduler_source = SCHEDULER_HPP.read_text()
    park_body = body_after_marker(
        scheduler_source,
        "inline void preemptible_syscall_park_impl(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us",
    )
    require_order(
        park_body,
        "task->set_wait_channel(wait_channel, wait_kind)",
        "if (task->wakeup_pending.exchange(false, std::memory_order_acquire))",
        "if (task->has_interrupting_signal_pending())",
        "request_local_timer_recheck()",
    )
    signal_check = park_body.find("if (task->has_interrupting_signal_pending())")
    recheck = park_body.find("request_local_timer_recheck()", signal_check)
    if signal_check < 0 or recheck < 0:
        fail("preemptible park signal recheck not found")
    signal_block = park_body[signal_check:recheck]
    for snippet in [
        "task->wake_at_us = 0",
        "task->wants_block = false",
        "task->set_voluntary_blocked(false)",
        "task->clear_wait_channel()",
        "return",
    ]:
        if snippet not in signal_block:
            fail(f"preemptible park signal recheck does not clear wait state: {snippet}")


def require_sigchld_wakes_interruptible_waits() -> None:
    exit_source = EXIT_CPP.read_text()
    notify_body = body_after_marker(exit_source, "void notify_parent_after_exit_ready")
    if "parent->sig_pending & ~parent->sig_mask & SIGCHLD_MASK" in notify_body:
        fail("SIGCHLD notification bypasses signal disposition checks")
    require_order(
        notify_body,
        "parent->sig_pending |= SIGCHLD_MASK",
        "bool wake_parent = false",
        "bool signal_wake_parent = false",
        "task_can_be_interrupted_by_signal(parent)",
        "parent->has_interrupting_signal_pending()",
        "if (wake_parent)",
        "reschedule_on_task_cpu(parent)",
        "else if (signal_wake_parent)",
        "ker::mod::sched::wake_task_for_signal(parent)",
    )


def main() -> None:
    require_poll_deadline_is_saturating(EPOLL_CPP)
    require_poll_deadline_is_saturating(SYS_NET_CPP)
    require_poll_waits_are_signal_interruptible()
    require_select_is_implemented_on_poll_core()
    require_preemptible_parking_rechecks_signals()
    require_sigchld_wakes_interruptible_waits()
    print("poll and epoll waits use saturating deadlines and signal-safe parking")


if __name__ == "__main__":
    main()
