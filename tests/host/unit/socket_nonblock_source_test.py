#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
SOCKET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "socket.hpp"
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
UDP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "udp.cpp"
RAW_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "raw.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_syscall_layer_keeps_nonblock_call_local() -> None:
    source = SYS_NET_CPP.read_text()
    if "SocketNonblockGuard" in source:
        fail("socket syscall layer must not use an RAII guard that mutates sock->nonblock")
    if re.search(r"sock->nonblock\s*=", source) is not None:
        fail("socket syscall layer must not assign temporary effective nonblock state to sock->nonblock")

    helper = function_body(source, "socket_call_effective_nonblock")
    require_order(
        helper,
        [
            "file != nullptr && (file->open_flags & WOS_O_NONBLOCK) != 0",
            "call_flags & ker::net::SOCKET_MSG_DONTWAIT",
            "sock != nullptr && sock->nonblock",
            "return FILE_NONBLOCK || MSG_NONBLOCK || SOCKET_NONBLOCK",
        ],
        "effective nonblocking helper",
    )

    runner = function_body(source, "run_socket_call")
    require_order(
        runner,
        [
            "bool const EFFECTIVE_NONBLOCK = socket_call_effective_nonblock(file, sock, call_flags)",
            "int const EFFECTIVE_FLAGS = EFFECTIVE_NONBLOCK ? (call_flags | ker::net::SOCKET_MSG_DONTWAIT) : call_flags",
            "T const RESULT = fn(EFFECTIVE_FLAGS)",
            "return fn(EFFECTIVE_FLAGS)",
            "T const AFTER_DRAIN = fn(EFFECTIVE_FLAGS)",
        ],
        "run_socket_call effective flags",
    )
    if "fn()" in runner:
        fail("run_socket_call must pass effective flags to every protocol callback")

    require_order(
        source,
        [
            "run_socket_call<int>(handle.file, sock, 0, [&](int flags)",
            "sock->proto_ops->connect(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), flags)",
            "run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&](int flags)",
            "sock->proto_ops->send(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), flags)",
            "run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&](int flags)",
            "sock->proto_ops->recv(sock, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), flags)",
        ],
        "socket syscall dispatch propagates effective flags",
    )


def test_protocol_ops_accept_effective_nonblock_flags() -> None:
    header = SOCKET_HPP.read_text()
    if "constexpr int SOCKET_MSG_DONTWAIT = 0x0040" not in header:
        fail("socket.hpp must define the MSG_DONTWAIT bit used by protocol-local nonblocking checks")
    require_order(
        header,
        [
            "int (*connect)(Socket*, const void*, size_t, int)",
            "inline auto socket_call_nonblock(const Socket* sock, int flags) -> bool",
            "return (sock != nullptr && sock->nonblock) || (flags & SOCKET_MSG_DONTWAIT) != 0",
        ],
        "socket protocol nonblock helper",
    )


def test_tcp_uses_call_local_nonblock_flags() -> None:
    source = TCP_CPP.read_text()
    for name in ("tcp_connect", "tcp_send", "tcp_recv"):
        body = function_body(source, name)
        if "bool const NONBLOCKING = socket_call_nonblock(sock, flags)" not in body:
            fail(f"{name} must derive nonblocking behavior from call-local flags")
        if "sock->nonblock" in body:
            fail(f"{name} must not read sock->nonblock directly after deriving call-local flags")

    connect = function_body(source, "tcp_connect")
    require_order(
        connect,
        [
            "bool const NONBLOCKING = socket_call_nonblock(sock, flags)",
            "if (cb->state == TcpState::SYN_SENT)",
            "if (NONBLOCKING)",
            "tcp_send_segment(cb, TCP_SYN, nullptr, 0)",
            "if (NONBLOCKING)",
        ],
        "TCP connect nonblocking branches",
    )


def test_udp_and_raw_receive_use_call_local_nonblock_flags() -> None:
    udp = UDP_CPP.read_text()
    udp_recv = function_body(udp, "udp_recv")
    if "return udp_recvfrom(sock, buf, len, flags, nullptr, nullptr)" not in udp_recv:
        fail("UDP recv must preserve MSG_DONTWAIT when sharing recvfrom")

    udp_recvfrom = function_body(udp, "udp_recvfrom")
    require_order(
        udp_recvfrom,
        [
            "sock->rcvbuf.available() < sizeof(UdpRecvRecord)",
            "if (!socket_call_nonblock(sock, flags))",
            "socket_defer_wait(sock, \"udp_wait\")",
        ],
        "UDP recvfrom call-local nonblock wait",
    )

    raw_recvfrom = function_body(RAW_CPP.read_text(), "raw_recvfrom")
    require_order(
        raw_recvfrom,
        [
            "sock->rcvbuf.available() < sizeof(RawRecvRecord)",
            "if (!socket_call_nonblock(sock, flags))",
            "socket_defer_wait(sock, \"raw_wait\")",
        ],
        "raw recvfrom call-local nonblock wait",
    )


def main() -> None:
    test_syscall_layer_keeps_nonblock_call_local()
    test_protocol_ops_accept_effective_nonblock_flags()
    test_tcp_uses_call_local_nonblock_flags()
    test_udp_and_raw_receive_use_call_local_nonblock_flags()
    print("socket call-local nonblocking source invariants hold")


if __name__ == "__main__":
    main()
