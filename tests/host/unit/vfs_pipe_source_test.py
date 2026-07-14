#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def lambda_body(source: str, name: str) -> str:
    marker = f"static auto {name} = [] ("
    start = source.find(marker)
    if start < 0:
        marker = f"static auto {name} = []("
        start = source.find(marker)
    if start < 0:
        fail(f"missing lambda {name}")
    body_start = source.find("{", start + len(marker))
    if body_start < 0:
        fail(f"missing body for lambda {name}")

    depth = 1
    pos = body_start + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated lambda {name}")
    return source[body_start + 1 : pos - 1]


def function_body(source: str, name: str) -> str:
    marker = ""
    start = -1
    for return_type in ("auto", "void"):
        marker = f"{return_type} {name}("
        start = source.find(marker)
        if start >= 0:
            break
    if start < 0:
        fail(f"missing function {name}")
    body_start = source.find("{", start + len(marker))
    if body_start < 0:
        fail(f"missing body for function {name}")

    depth = 1
    pos = body_start + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[body_start + 1 : pos - 1]


def require_order(body: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = body.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def test_local_pipe_bounces_are_producer_initialized() -> None:
    source = CORE_CPP.read_text()
    declaration = (
        "std::array<char, PIPE_COPY_CHUNK> bounce "
        "__attribute__((uninitialized));"
    )
    if source.count(declaration) != 2:
        fail("local pipe read and write must each use uninitialized bounce scratch")

    read_body = lambda_body(source, "pipe_read")
    require_order(
        read_body,
        [
            declaration,
            "size_t const TO_READ = std::min({count, st->count, bounce.size()})",
            "pipe_copy_from_ring_locked(st, bounce.data(), TO_READ)",
            "st->lock.unlock_irqrestore(IRQF)",
            "pipe_copy_to_caller(buf, bounce.data(), TO_READ)",
        ],
        "local pipe read bounce",
    )

    write_body = lambda_body(source, "pipe_write")
    require_order(
        write_body,
        [
            declaration,
            "size_t const TO_STAGE = std::min({count, AVAIL, bounce.size()})",
            "pipe_copy_from_caller(buf, bounce.data(), TO_STAGE)",
            "return finish(-EFAULT)",
            "size_t const TO_WRITE = std::min(TO_STAGE, st->capacity - st->count)",
            "pipe_copy_to_ring_locked(st, bounce.data(), TO_WRITE)",
        ],
        "local pipe write bounce",
    )

    caller_copy = function_body(source, "pipe_copy_from_caller")
    require_order(
        caller_copy,
        [
            "copy_from_task(*task, USER_ADDR, dst, size)",
            "std::memcpy(dst, src, size)",
            "return true",
        ],
        "local pipe caller copy",
    )

    ring_copy = function_body(source, "pipe_copy_from_ring_locked")
    require_order(
        ring_copy,
        [
            "size_t const FIRST = std::min(count, st->capacity - st->tail)",
            "std::memcpy(dst, st->buf + st->tail, FIRST)",
            "if (FIRST < count)",
            "std::memcpy(dst + FIRST, st->buf, count - FIRST)",
        ],
        "local pipe ring copy",
    )

    if "std::array<char, PIPE_COPY_CHUNK> bounce{}" in source:
        fail("local pipe bounce scratch must not be cleared before its producer")


def main() -> None:
    test_local_pipe_bounces_are_producer_initialized()
    print("VFS pipe source invariants hold")


if __name__ == "__main__":
    main()
