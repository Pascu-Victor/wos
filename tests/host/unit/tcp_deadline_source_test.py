#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_FILES = [
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp",
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp",
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_output.cpp",
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_timer.cpp",
]


def fail(message: str) -> None:
    raise AssertionError(message)


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

    if problems:
        fail("; ".join(problems))
    print("TCP timer deadlines use saturating arithmetic")


if __name__ == "__main__":
    main()
