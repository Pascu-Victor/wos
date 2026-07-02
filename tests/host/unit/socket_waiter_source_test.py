#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOCKET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "socket.hpp"
SOCKET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "socket.cpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
EPOLL_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "epoll.cpp"
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"
TCP_TIMER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp"
UDP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "udp.cpp"
RAW_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "raw.cpp"


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
    match = re.search(
        rf"\b(?:auto|bool|int|ssize_t|void)\s+(?:[A-Za-z0-9_:]+::)?{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&*]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")
    end = find_matching_brace(source, match.end() - 1)
    return source[match.end() : end]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_socket_waiter_list_core() -> None:
    header = SOCKET_HPP.read_text()
    require_tokens(
        header,
        [
            "struct SocketWaiter",
            "uint64_t pid = 0;",
            "SocketWaiter* next = nullptr;",
            "SocketWaiter* waiters = nullptr;",
            "auto socket_register_waiter(Socket* sock, uint64_t pid) -> bool;",
            "auto socket_defer_wait(Socket* sock, const char* wait_channel = \"sock_wait\") -> bool;",
            "void socket_wake_waiters(Socket* sock);",
        ],
        "socket waiter declarations",
    )

    source = SOCKET_CPP.read_text()
    register_body = function_body(source, "socket_register_waiter")
    require_order(
        register_body,
        [
            "socket_has_waiter_locked(sock, pid)",
            "auto* waiter = new (std::nothrow) SocketWaiter",
            "socket_has_waiter_locked(sock, pid)",
            "waiter->next = sock->waiters;",
            "sock->waiters = waiter;",
            "sock->owner_pid = pid;",
        ],
        "socket waiter registration",
    )

    defer_body = function_body(source, "socket_defer_wait")
    require_order(
        defer_body,
        [
            "auto* current_task = ker::mod::sched::get_current_task();",
            "if (!socket_register_waiter(sock, current_task->pid))",
            "current_task->set_wait_channel(wait_channel);",
            "return true;",
        ],
        "socket deferred wait registration",
    )

    wake_body = function_body(source, "socket_wake_waiters")
    if "owner_pid" in wake_body:
        fail("socket_wake_waiters must not wake only the latest owner_pid")
    require_order(
        wake_body,
        [
            "auto* waiters = socket_detach_waiters(sock);",
            "while (waiters != nullptr)",
            "wake_task_by_pid_from_event(waiters->pid)",
            "delete waiters;",
        ],
        "socket waiter wake list drain",
    )


def test_poll_and_epoll_register_socket_waiters() -> None:
    for path, label in ((SYS_NET_CPP, "poll"), (EPOLL_CPP, "epoll")):
        body = function_body(path.read_text(), "register_poll_waiter")
        if "sock->owner_pid = pid" in body:
            fail(f"{label} must not overwrite a single socket owner pid for readiness waits")
        require_order(
            body,
            [
                "file->fs_type",
                "FSType::SOCKET",
                "ker::net::socket_register_waiter(sock, pid) ? 1 : -ENOMEM",
            ],
            f"{label} socket waiter registration",
        )

    poll_waiters = function_body(SYS_NET_CPP.read_text(), "register_poll_waiters")
    require_tokens(
        poll_waiters,
        [
            "int const REGISTERED",
            "if (REGISTERED < 0)",
            "return REGISTERED;",
            "return can_block ? 1 : 0;",
        ],
        "poll waiter error propagation",
    )

    epoll_wait = function_body(EPOLL_CPP.read_text(), "epoll_pwait")
    require_tokens(
        epoll_wait,
        [
            "int const REGISTERED",
            "if (REGISTERED < 0)",
            "return REGISTERED;",
        ],
        "epoll waiter error propagation",
    )


def test_tcp_wakes_waiter_list() -> None:
    tcp_input = TCP_INPUT_CPP.read_text()
    wake_body = function_body(tcp_input, "wake_socket")
    forbidden = ["wake_task_by_pid_from_event", "find_task_by_pid_safe", "wake_task_from_event"]
    present = [token for token in forbidden if token in wake_body]
    if present:
        fail(f"TCP input wake_socket still wakes a single task directly: {present[0]}")
    require_tokens(wake_body, ["socket_wake_waiters(sock);"], "TCP input socket wake")

    timer_body = function_body(TCP_TIMER_CPP.read_text(), "wake_socket_timer")
    require_tokens(timer_body, ["socket_wake_waiters(sock);"], "TCP timer socket wake")
    if "owner_pid" in timer_body or "wake_task_by_pid_from_event" in timer_body:
        fail("TCP timer wake must use the socket waiter list")


def test_blocking_protocol_waits_handle_waiter_allocation_failure() -> None:
    tcp = TCP_CPP.read_text()
    helper = function_body(tcp, "defer_socket_wait")
    require_tokens(helper, ["return socket_defer_wait(sock, \"tcp_wait\");"], "TCP wait helper")
    for name in ("tcp_accept", "tcp_connect", "tcp_send", "tcp_recv"):
        body = function_body(tcp, name)
        if "socket_defer_wait" in body or "defer_socket_wait" in body:
            require_tokens(body, ["return -ENOMEM;"], f"{name} failed waiter allocation path")

    udp_recvfrom = function_body(UDP_CPP.read_text(), "udp_recvfrom")
    require_order(
        udp_recvfrom,
        [
            "if (!socket_call_nonblock(sock, flags))",
            "if (!socket_defer_wait(sock, \"udp_wait\"))",
            "return -ENOMEM;",
            "return -EAGAIN;",
        ],
        "UDP blocking receive waiter allocation failure",
    )

    raw_recvfrom = function_body(RAW_CPP.read_text(), "raw_recvfrom")
    require_order(
        raw_recvfrom,
        [
            "if (!socket_call_nonblock(sock, flags))",
            "if (!socket_defer_wait(sock, \"raw_wait\"))",
            "return -ENOMEM;",
            "return -EAGAIN;",
        ],
        "raw blocking receive waiter allocation failure",
    )


def test_blocking_socket_send_progress_runs_network_checkpoint() -> None:
    source = SYS_NET_CPP.read_text()
    helper = function_body(source, "checkpoint_blocking_socket_send_progress")
    require_order(
        helper,
        [
            "if (result <= 0 || socket_call_effective_nonblock(file, sock, call_flags))",
            "drain_network_rx_work();",
        ],
        "blocking socket send progress checkpoint",
    )
    if "ker::mod::sched::kern_yield();" in helper:
        fail("successful blocking socket send progress must not force a syscall-path kern_yield")

    fops_write = function_body(source, "socket_fops_write")
    require_order(
        fops_write,
        [
            "run_socket_call<ssize_t>",
            "checkpoint_blocking_socket_send_progress(f, sock, 0, RESULT);",
            "return RESULT;",
        ],
        "socket VFS write progress checkpoint",
    )

    require_tokens(
        source,
        [
            "checkpoint_blocking_socket_send_progress(handle.file, sock, static_cast<int>(a4), CLAMPED);",
        ],
        "socket send syscall progress checkpoint",
    )
    if source.count("checkpoint_blocking_socket_send_progress(handle.file, sock, static_cast<int>(a4), CLAMPED);") < 2:
        fail("SEND and SENDTO must both run the blocking socket send progress checkpoint")


def main() -> None:
    test_socket_waiter_list_core()
    test_poll_and_epoll_register_socket_waiters()
    test_tcp_wakes_waiter_list()
    test_blocking_protocol_waits_handle_waiter_allocation_failure()
    test_blocking_socket_send_progress_runs_network_checkpoint()
    print("socket waiter source invariants hold")


if __name__ == "__main__":
    main()
