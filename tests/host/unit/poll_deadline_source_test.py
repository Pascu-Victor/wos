#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EPOLL_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "epoll.cpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
EXIT_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "exit.cpp"
CHILD_EVENTS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "child_events.cpp"
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
        "int const CAN_BLOCK = register_poll_waiters",
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
        "std::array<uint8_t, WOS_FD_SET_BYTES> readfds{}",
        "ker::mod::sys::usercopy::ensure_writable(*task, user_addr, set.size())",
        "ker::mod::sys::usercopy::copy_from_task(*task, user_addr, set.data(), set.size())",
        "ker::mod::sys::usercopy::copy_value_from_task(*task, a5, timeout)",
        "run_select(static_cast<size_t>(a1)",
        "a5 != 0 ? &timeout : nullptr",
        "ker::mod::sys::usercopy::copy_to_task(*task, a2, readfds.data(), readfds.size())",
    )
    if "reinterpret_cast<const KSelectTimeval*>(a5)" in select_case:
        fail("select passes a raw userspace timeout into the poll core")

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
        "size_t const FDS_BYTES = NFDS * sizeof(KPollFd)",
        "ker::mod::sys::usercopy::ensure_writable(*task, a1, FDS_BYTES)",
        "new (std::nothrow) KPollFd[NFDS]",
        "ker::mod::sys::usercopy::copy_from_task(*task, a1, fds, FDS_BYTES)",
        'run_poll_wait(fds, NFDS, timeout, "poll")',
        "ker::mod::sys::usercopy::copy_to_task(*task, a1, fds, FDS_BYTES)",
        "delete[] fds",
    )
    if "reinterpret_cast<KPollFd*>(a1)" in poll_case:
        fail("poll passes a raw userspace array into the poll core")


def require_preemptible_parking_rechecks_signals() -> None:
    scheduler_source = SCHEDULER_HPP.read_text()
    cancel_body = body_after_marker(scheduler_source, "inline auto scheduler_wait_should_cancel")
    park_body = body_after_marker(
        scheduler_source,
        "inline void preemptible_syscall_park_impl(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us",
    )
    require_order(
        cancel_body,
        "if (task->wakeup_pending.exchange(false, std::memory_order_acquire))",
        "wait_kind != SchedulerHaltWaitKind::YIELD",
        "task->has_interrupting_signal_pending()",
    )
    require_order(
        park_body,
        "task->set_wait_channel(wait_channel, wait_kind)",
        "scheduler_wait_halt_once(task, SchedulerHaltWaitKind::PROCESS_PARK, INTERRUPTS_WERE_ENABLED)",
        "task->wake_at_us = 0",
        "task->wants_block = false",
        "task->set_voluntary_blocked(false)",
        "task->clear_wait_channel()",
        "restore_interrupts_after_scheduler_wait(INTERRUPTS_WERE_ENABLED)",
    )
    if "request_local_timer_recheck()" in park_body:
        fail("preemptible park must arm its timer only inside scheduler_wait_halt_once")


def require_sigchld_wakes_interruptible_waits() -> None:
    notify_body = body_after_marker(CHILD_EVENTS_CPP.read_text(), "void deliver_wake")
    require_order(
        notify_body,
        "wake.signal_owner->signal_add_pending_mask(1ULL << (SIGCHLD_NUMBER - 1U))",
        "ker::mod::sched::wake_task_from_event(wake.signal_owner)",
    )
    if "g_lifecycle_lock" in notify_body:
        fail("SIGCHLD event delivery must run after dropping the lifecycle lock")


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
