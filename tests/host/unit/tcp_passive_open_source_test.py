#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_HPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.hpp"
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|bool|int|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def case_body(source: str, start_token: str, end_token: str, context: str) -> str:
    start = source.find(start_token)
    if start < 0:
        fail(f"{context}: missing start token {start_token}")
    end = source.find(end_token, start + len(start_token))
    if end < 0:
        fail(f"{context}: missing end token {end_token}")
    return source[start:end]


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def test_unaccepted_child_helpers_are_public_and_destroy_socket_owner() -> None:
    header = TCP_HPP.read_text()
    source = TCP_CPP.read_text()
    destroy = function_body(source, "tcp_destroy_unaccepted_child")
    drain = function_body(source, "tcp_drain_accept_queue")

    require(header, "void tcp_destroy_unaccepted_child(Socket* child)", "TCP passive child helper declaration")
    require(header, "void tcp_drain_accept_queue(Socket* listener)", "TCP accept-queue drain declaration")

    require_order(
        destroy,
        [
            "child->accept_next = nullptr",
            "child->state = SocketState::CLOSED",
            "auto* cb = static_cast<TcpCB*>(child->proto_data)",
            "cb->state = TcpState::CLOSED",
            "tcp_free_cb(cb)",
            "child->proto_data = nullptr",
            "cb->socket = nullptr",
            "tcp_cb_release(cb)",
            "socket_destroy(child)",
        ],
        "unaccepted child destruction",
    )

    require_order(
        drain,
        [
            "listener->lock.lock_irqsave()",
            "Socket* child = listener->aq_head",
            "listener->aq_head = child->accept_next",
            "listener->aq_count--",
            "child->accept_next = nullptr",
            "listener->lock.unlock_irqrestore(FLAGS)",
            "tcp_destroy_unaccepted_child(child)",
        ],
        "accept queue drain",
    )


def test_passive_syn_failure_destroys_child() -> None:
    source = TCP_INPUT_CPP.read_text()
    listen_syn = function_body(source, "handle_listen_syn")

    require_order(
        listen_syn,
        [
            "listen_sock->lock.lock_irqsave()",
            "listen_sock->state == SocketState::LISTENING",
            "std::cmp_less(AQ_COUNT, BACKLOG)",
            "Socket* child = socket_create",
            "tcp_insert_cb(child_cb)",
            "if (!tcp_send_segment(child_cb, TCP_SYN | TCP_ACK, nullptr, 0))",
            "tcp_destroy_unaccepted_child(child)",
        ],
        "passive SYN child setup failure",
    )


def test_final_ack_drops_children_that_are_not_enqueued() -> None:
    process = function_body(TCP_INPUT_CPP.read_text(), "tcp_process_segment")
    syn_received = case_body(process, "case TcpState::SYN_RECEIVED:", "case TcpState::ESTABLISHED:", "SYN_RECEIVED TCP state")

    require_order(
        syn_received,
        [
            "bool child_enqueued = false",
            "Socket* listener_sock_to_wake = nullptr",
            "lsock->state == SocketState::LISTENING && std::cmp_less(lsock->aq_count, lsock->backlog)",
            "child_sock->accept_next = nullptr",
            "lsock->aq_count++",
            "child_enqueued = true",
            "listener_sock_to_wake = lsock",
            "if (child_enqueued)",
            "wake_socket(listener_sock_to_wake)",
            "else",
            "tcp_destroy_unaccepted_child(child_sock)",
            "tcp_cb_release(cb)",
            "return",
        ],
        "final ACK accept queue ownership",
    )


def test_listener_close_marks_socket_closed_and_drains_children() -> None:
    close_op = function_body(TCP_CPP.read_text(), "tcp_close_op")

    require_order(
        close_op,
        [
            "bool const WAS_LISTENER = (cb->state == TcpState::LISTEN)",
            "if (WAS_LISTENER)",
            "sock->lock.lock_irqsave()",
            "sock->state = SocketState::CLOSED",
            "sock->lock.unlock_irqrestore(SOCK_FLAGS)",
            "tcp_remove_listener(cb)",
            "tcp_drain_accept_queue(sock)",
        ],
        "listener close drains passive children",
    )


def main() -> None:
    test_unaccepted_child_helpers_are_public_and_destroy_socket_owner()
    test_passive_syn_failure_destroys_child()
    test_final_ack_drops_children_that_are_not_enqueued()
    test_listener_close_marks_socket_closed_and_drains_children()
    print("TCP passive-open source invariants hold")


if __name__ == "__main__":
    main()
