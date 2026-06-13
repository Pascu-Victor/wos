#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EPOLL_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "epoll.cpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"


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


def main() -> None:
    require_poll_deadline_is_saturating(EPOLL_CPP)
    require_poll_deadline_is_saturating(SYS_NET_CPP)
    print("poll and epoll timeout deadlines use saturating arithmetic")


if __name__ == "__main__":
    main()
