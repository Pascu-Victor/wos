#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_FILES = [
    TCP_CPP,
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp",
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_output.cpp",
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp",
]


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void|bool)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def require_recv_window_ack_is_locked(problems: list[str]) -> None:
    source = TCP_CPP.read_text()
    recv = function_body(source, "tcp_recv")
    helper = function_body(source, "maybe_send_recv_window_update")

    if "tcp_send_ack(cb)" in recv:
        problems.append("tcp_recv must not build receive-window ACKs without cb->lock")

    require_order(
        recv,
        [
            "sock->rcvbuf.read(buf, len)",
            "if (N > 0)",
            "maybe_send_recv_window_update(cb, sock)",
        ],
        "tcp_recv window update handoff",
    )
    require_order(
        helper,
        [
            "cb->lock.lock_irqsave()",
            "cb->rcv_wnd = tcp_receive_window_space(cb, sock)",
            "tcp_build_ack(cb, &ack_local, &ack_remote)",
            "cb->lock.unlock_irqrestore(FLAGS)",
            "ipv4_tx(ack_pkt, ack_local, ack_remote, 6, 64)",
            "cb->lock.lock_irqsave()",
            "cb->ack_pending = true",
            "tcp_timer_arm(cb)",
        ],
        "locked recv-window ACK snapshot",
    )


def require_timer_scan_covers_full_list(problems: list[str]) -> None:
    timer_source = (ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp").read_text()
    timer_tick = function_body(timer_source, "tcp_timer_tick")

    if "while (*pp != nullptr && batch_count < MAX_BATCH)" in timer_tick:
        problems.append("tcp_timer_tick must scan the whole timer list even when the work batch is full")

    require_order(
        timer_tick,
        [
            "while (*pp != nullptr)",
            "bool needs_work = false",
            "if (needs_work && batch_count < MAX_BATCH)",
            "next_earliest = std::min(next_earliest, cb->retransmit_deadline)",
            "timer_earliest.store(next_earliest, std::memory_order_relaxed)",
        ],
        "full TCP timer list scan with bounded work batch",
    )


def main() -> None:
    forbidden = [
        "tcp_now_ms() +",
        "now_ms +",
        "NOW +",
    ]
    problems: list[str] = []
    for path in TCP_FILES:
        source = path.read_text()
        for snippet in forbidden:
            if snippet in source:
                problems.append(f"{path.relative_to(ROOT)} uses {snippet}")
        if path.name != "tcp_timer.cpp" and "tcp_deadline_after_ms(" not in source:
            problems.append(f"{path.relative_to(ROOT)} no longer uses tcp_deadline_after_ms")

    timer_source = (ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp").read_text()
    for token in [
        "tcp_deadline_after_ms(tcp_now_ms(), 60000)",
        "tcp_deadline_after_ms(tcp_now_ms(), 10)",
        "tcp_deadline_after_ms(now_ms, 60000)",
        "tcp_deadline_after_ms(now_ms, 10)",
        "tcp_deadline_after_ms(now_ms, rcb->keepalive_intvl_ms)",
        "tcp_deadline_after_ms(now_ms, rcb->rto_ms)",
    ]:
        if token not in timer_source:
            problems.append(f"tcp_timer.cpp missing saturating deadline use: {token}")

    require_recv_window_ack_is_locked(problems)
    require_timer_scan_covers_full_list(problems)

    if problems:
        fail("; ".join(problems))
    print("TCP timer/window source invariants hold")


if __name__ == "__main__":
    main()
