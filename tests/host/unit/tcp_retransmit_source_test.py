#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_HPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.hpp"
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"
TCP_OUTPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_output.cpp"
TCP_TIMER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp"


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


def test_syn_ack_retires_syn_retransmit_before_established() -> None:
    process = function_body(TCP_INPUT_CPP.read_text(), "tcp_process_segment")
    syn_sent = case_body(process, "case TcpState::SYN_SENT:", "case TcpState::SYN_RECEIVED:", "SYN_SENT TCP state")
    require_order(
        syn_sent,
        [
            "cb->snd_una = seg_ack",
            "retire_acked_retransmits_locked(cb, seg_ack, tcp_now_ms(), true)",
            "parse_syn_options(cb, hdr, true)",
            "cb->state = TcpState::ESTABLISHED",
        ],
        "active-open SYN-ACK retransmit retirement",
    )


def test_ack_cleanup_helper_resets_empty_deadline() -> None:
    helper = function_body(TCP_INPUT_CPP.read_text(), "retire_acked_retransmits_locked")
    require_order(
        helper,
        [
            "while (cb->retransmit_head != nullptr)",
            "if (tcp_seq_after(ENTRY_END, seg_ack))",
            "break",
            "update_rto_from_sample_locked(cb, now_ms - entry->send_time_ms)",
            "cb->retransmit_head = entry->next",
            "if (cb->retransmit_head != nullptr)",
            "cb->retransmit_deadline = tcp_deadline_after_ms(now_ms, cb->rto_ms)",
            "else",
            "cb->retransmit_deadline = 0",
        ],
        "ACKed retransmit cleanup helper",
    )


def test_rto_floor_is_not_aggressive() -> None:
    header = TCP_HPP.read_text()
    input_source = TCP_INPUT_CPP.read_text()
    timer_source = TCP_TIMER_CPP.read_text()
    for token in [
        "constexpr uint64_t TCP_RTO_MIN_MS = 200",
        "constexpr uint64_t TCP_RTO_INITIAL_MS = 1000",
        "constexpr uint64_t TCP_RTO_MAX_MS = 60000",
        "uint64_t rto_ms = TCP_RTO_INITIAL_MS",
    ]:
        if token not in header:
            fail(f"tcp.hpp missing RTO invariant {token!r}")
    if "constexpr uint64_t TCP_RTO_INITIAL_MS = TCP_RTO_MIN_MS" in header:
        fail("initial TCP RTO must not inherit the established-flow 200ms floor")
    if "std::max<uint64_t>(cb->rto_ms, TCP_RTO_MIN_MS)" not in input_source:
        fail("RTT estimator must clamp to TCP_RTO_MIN_MS")
    if "std::min<uint64_t>(rcb->rto_ms, TCP_RTO_MAX_MS)" not in timer_source:
        fail("timer backoff must clamp to TCP_RTO_MAX_MS")
    if "50ULL" in input_source:
        fail("TCP RTO floor regressed to 50ms")


def test_receiver_sack_advertises_and_reports_out_of_order_block() -> None:
    header = TCP_HPP.read_text()
    input_source = TCP_INPUT_CPP.read_text()
    output_source = TCP_OUTPUT_CPP.read_text()

    for token in [
        "constexpr uint8_t TCP_OPTION_SACK_PERMITTED = 4",
        "constexpr uint8_t TCP_OPTION_SACK = 5",
        "bool sack_permitted = false",
    ]:
        if token not in header:
            fail(f"tcp.hpp missing SACK invariant {token!r}")

    parse_syn = function_body(input_source, "parse_syn_options")
    require_order(
        parse_syn,
        [
            "opts[i] == TCP_OPTION_SACK_PERMITTED",
            "OPT_LEN == TCP_OPTION_SACK_PERMITTED_LEN",
            "cb->sack_permitted = true",
        ],
        "SACK permitted SYN option parser",
    )

    send_segment = function_body(output_source, "tcp_send_segment")
    require_order(
        send_segment,
        [
            "options.at(4) = TCP_OPTION_SACK_PERMITTED",
            "options.at(5) = TCP_OPTION_SACK_PERMITTED_LEN",
            "opts_len = TCP_SYN_OPTIONS_LEN",
        ],
        "SACK permitted SYN option emission",
    )

    sack_helper = function_body(output_source, "write_sack_option_locked")
    require_order(
        sack_helper,
        [
            "!cb->sack_permitted",
            "uint32_t left_edge = 0",
            "if (cb->ooo_head != nullptr)",
            "left_edge = seg->seq",
            "right_edge = seg->seq + static_cast<uint32_t>(seg->len)",
            "} else if (cb->ooo_fin_pending)",
            "left_edge = cb->ooo_fin_seq",
            "right_edge = cb->ooo_fin_seq + 1",
            "return 0",
            "uint32_t const LEFT_EDGE = htonl(left_edge)",
            "uint32_t const RIGHT_EDGE = htonl(right_edge)",
            "options[2] = TCP_OPTION_SACK",
            "options[3] = TCP_OPTION_SACK_1_BLOCK_LEN",
            "std::memcpy(options + 4, &LEFT_EDGE, sizeof(LEFT_EDGE))",
            "std::memcpy(options + 8, &RIGHT_EDGE, sizeof(RIGHT_EDGE))",
        ],
        "single-block SACK ACK helper",
    )

    build_ack = function_body(output_source, "tcp_build_ack")
    require_order(
        build_ack,
        [
            "write_sack_option_locked(cb, options.data(), options.size())",
            "size_t const HDR_LEN = sizeof(TcpHeader) + OPTS_LEN",
            "std::memcpy(payload + sizeof(TcpHeader), options.data(), OPTS_LEN)",
            "hdr->data_offset = static_cast<uint8_t>((HDR_LEN / 4) << 4)",
            "pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len)",
        ],
        "SACK option included in ACK checksum",
    )


def test_tcp_poll_reports_all_recv_eof_states() -> None:
    source = TCP_CPP.read_text()
    eof_helper = function_body(source, "tcp_read_eof_state")
    poll_check = function_body(source, "tcp_poll_check_op")

    for token in [
        "case TcpState::CLOSE_WAIT:",
        "case TcpState::CLOSED:",
        "case TcpState::TIME_WAIT:",
        "case TcpState::CLOSING:",
        "case TcpState::LAST_ACK:",
    ]:
        if token not in eof_helper:
            fail(f"TCP poll EOF helper missing {token}")

    require_order(
        poll_check,
        [
            "return POLLERR | POLLHUP",
            "bool const READ_EOF = tcp_read_eof_state(STATE)",
            "bool const CONNECT_FAILED = STATE == TcpState::CLOSED && sock->state == SocketState::CONNECTING",
            "sock->rcvbuf.available() > 0 || READ_EOF",
            "ready |= POLLRDHUP",
            "ready |= POLLOUT",
            "ready |= POLLERR",
            "ready |= POLLHUP",
        ],
        "TCP poll EOF/error/hup readiness",
    )


def main() -> None:
    test_syn_ack_retires_syn_retransmit_before_established()
    test_ack_cleanup_helper_resets_empty_deadline()
    test_rto_floor_is_not_aggressive()
    test_receiver_sack_advertises_and_reports_out_of_order_block()
    test_tcp_poll_reports_all_recv_eof_states()
    print("TCP retransmit source invariants hold")


if __name__ == "__main__":
    main()
