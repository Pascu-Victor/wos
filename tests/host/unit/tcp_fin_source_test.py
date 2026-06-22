#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"


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


def test_fin_never_skips_unqueued_payload() -> None:
    source = TCP_INPUT_CPP.read_text()
    if "SEG_SEQ + static_cast<uint32_t>(payload_len) + 1" in source:
        fail("TCP FIN handling must not advance rcv_nxt past payload that may not have been queued")

    helper = function_body(source, "consume_fin_if_in_sequence")
    require_order(
        helper,
        [
            "uint32_t const FIN_SEQ = tcp_seq_end(seg_seq, payload_len)",
            "if (FIN_SEQ != cb->rcv_nxt)",
            "return false",
            "cb->rcv_nxt = FIN_SEQ + 1",
            "return true",
        ],
        "FIN sequence guard",
    )


def test_established_queues_payload_before_fin() -> None:
    source = TCP_INPUT_CPP.read_text()
    process = function_body(source, "tcp_process_segment")
    established = case_body(process, "case TcpState::ESTABLISHED:", "case TcpState::CLOSE_WAIT:", "ESTABLISHED TCP state")
    require_order(
        established,
        [
            "if (payload_len > 0)",
            "receive_segment_payload(cb, cb->socket, payload, payload_len, SEG_SEQ)",
            "if ((flags & TCP_FIN) != 0)",
            "consume_fin_if_in_sequence(cb, SEG_SEQ, payload_len)",
            "cb->state = TcpState::CLOSE_WAIT",
            "build_deferred_ack()",
        ],
        "ESTABLISHED payload-before-FIN handling",
    )


def test_overlapping_retransmit_queues_new_suffix() -> None:
    source = TCP_INPUT_CPP.read_text()
    helper = function_body(source, "receive_segment_payload")
    require_order(
        helper,
        [
            "uint32_t const SEG_END = tcp_seq_end(seg_seq, payload_len)",
            "if (!tcp_seq_after(SEG_END, cb->rcv_nxt))",
            "return result",
            "if (tcp_seq_before(seg_seq, cb->rcv_nxt))",
            "auto const TRIM = static_cast<size_t>(cb->rcv_nxt - seg_seq)",
            "payload += TRIM",
            "payload_len -= TRIM",
            "seg_seq = cb->rcv_nxt",
            "if (seg_seq == cb->rcv_nxt)",
            "queue_in_order_payload(cb, sock, payload, payload_len)",
            "drain_out_of_order_payload(cb, sock)",
        ],
        "overlapping retransmit trim",
    )


def test_orphaned_close_discards_contiguous_payload_before_fin() -> None:
    source = TCP_INPUT_CPP.read_text()
    helper = function_body(source, "discard_orphaned_payload")
    process = function_body(source, "tcp_process_segment")
    fin_wait_1 = case_body(process, "case TcpState::FIN_WAIT_1:", "case TcpState::FIN_WAIT_2:", "FIN_WAIT_1 TCP state")
    fin_wait_2 = case_body(process, "case TcpState::FIN_WAIT_2:", "case TcpState::CLOSING:", "FIN_WAIT_2 TCP state")

    require_order(
        helper,
        [
            "uint32_t const SEG_END = tcp_seq_end(seg_seq, payload_len)",
            "if (!tcp_seq_after(SEG_END, cb->rcv_nxt))",
            "return false",
            "if (tcp_seq_before(seg_seq, cb->rcv_nxt))",
            "payload_len -= TRIM",
            "seg_seq = cb->rcv_nxt",
            "if (seg_seq != cb->rcv_nxt)",
            "return false",
            "cb->rcv_nxt += static_cast<uint32_t>(payload_len)",
            "return true",
        ],
        "orphaned payload discard",
    )
    require_order(
        fin_wait_1,
        [
            "if (cb->socket != nullptr)",
            "receive_segment_payload(cb, cb->socket, payload, payload_len, SEG_SEQ)",
            "else",
            "discard_orphaned_payload(cb, payload_len, SEG_SEQ)",
            "if ((flags & TCP_FIN) != 0 && cb->state == TcpState::FIN_WAIT_1)",
            "consume_fin_if_in_sequence(cb, SEG_SEQ, payload_len)",
        ],
        "FIN_WAIT_1 orphaned payload before FIN",
    )
    require_order(
        fin_wait_2,
        [
            "if (cb->socket != nullptr)",
            "receive_segment_payload(cb, cb->socket, payload, payload_len, SEG_SEQ)",
            "else",
            "discard_orphaned_payload(cb, payload_len, SEG_SEQ)",
            "if ((flags & TCP_FIN) != 0)",
            "consume_fin_if_in_sequence(cb, SEG_SEQ, payload_len)",
        ],
        "FIN_WAIT_2 orphaned payload before FIN",
    )


def test_closing_states_ack_retransmitted_fin() -> None:
    source = TCP_INPUT_CPP.read_text()
    process = function_body(source, "tcp_process_segment")
    fin_wait_1 = case_body(process, "case TcpState::FIN_WAIT_1:", "case TcpState::FIN_WAIT_2:", "FIN_WAIT_1 TCP state")
    closing = case_body(process, "case TcpState::CLOSING:", "case TcpState::LAST_ACK:", "CLOSING TCP state")
    last_ack = case_body(process, "case TcpState::LAST_ACK:", "case TcpState::TIME_WAIT:", "LAST_ACK TCP state")

    require_order(
        fin_wait_1,
        [
            "bool our_fin_acked = false",
            "our_fin_acked = true",
            "if ((flags & TCP_FIN) != 0 && cb->state == TcpState::FIN_WAIT_1)",
            "consume_fin_if_in_sequence(cb, SEG_SEQ, payload_len)",
            "else if (our_fin_acked)",
            "cb->state = TcpState::FIN_WAIT_2",
            "build_deferred_ack()",
        ],
        "FIN_WAIT_1 unconsumed FIN ACK",
    )
    require_order(
        closing,
        [
            "if ((flags & TCP_FIN) != 0)",
            "build_deferred_ack()",
            "if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt)",
        ],
        "CLOSING duplicate FIN ACK",
    )
    require_order(
        last_ack,
        [
            "if ((flags & TCP_FIN) != 0)",
            "build_deferred_ack()",
            "if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt)",
            "PacketBuffer* closing_ack = deferred_ack",
            "if (closing_ack != nullptr)",
            "ipv4_tx(closing_ack, CLOSING_ACK_LOCAL, CLOSING_ACK_REMOTE, IPPROTO_TCP, TCP_IPV4_TTL)",
        ],
        "LAST_ACK duplicate FIN ACK",
    )


def main() -> None:
    test_fin_never_skips_unqueued_payload()
    test_established_queues_payload_before_fin()
    test_overlapping_retransmit_queues_new_suffix()
    test_orphaned_close_discards_contiguous_payload_before_fin()
    test_closing_states_ack_retransmitted_fin()
    print("TCP FIN receive source invariants hold")


if __name__ == "__main__":
    main()
