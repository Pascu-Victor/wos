#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PTY_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "pty.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:auto|void|int|ssize_t)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_detached_pty_device_is_not_logged_as_pointer_corruption() -> None:
    source = PTY_CPP.read_text()
    body = function_body(source, "pair_from_device")

    require_tokens(
        body,
        [
            "auto* pair = static_cast<PtyPair*>(device->private_data);",
            "if (pair == nullptr)",
            "return nullptr;",
            "if (!is_valid_kernel_pointer(pair))",
            '"pty_%s: invalid pair pointer %p from device %p"',
        ],
        "PTY pair lookup should distinguish detach from corruption",
    )
    require_order(body, "if (pair == nullptr)", "if (!is_valid_kernel_pointer(pair))", "null detach before corruption warning")


def test_pty_pair_detaches_only_after_both_sides_close() -> None:
    source = PTY_CPP.read_text()
    master_close = function_body(source, "master_close")
    slave_close = function_body(source, "slave_close")

    require_tokens(
        master_close,
        [
            "if (pair->master_opened > 0)",
            "pair->master_opened--;",
            "if (pair->master_opened <= 0 && pair->slave_opened <= 0 && !pair->freeing)",
            "pty_detach_devices(pair);",
        ],
        "PTY master close must keep pair attached while any endpoint remains open",
    )
    require_tokens(
        slave_close,
        [
            "if (pair->slave_opened > 0)",
            "pair->slave_opened--;",
            "if (pair->master_opened <= 0 && pair->slave_opened <= 0 && !pair->freeing)",
            "pty_detach_devices(pair);",
        ],
        "PTY slave close must keep pair attached while any endpoint remains open",
    )

    if "if (pair->slave_opened <= 0 && !pair->freeing)" in master_close:
        fail("PTY master close must not detach solely because the slave side is closed")
    if "if (pair->master_opened <= 0 && !pair->freeing)" in slave_close:
        fail("PTY slave close must not detach solely because the master side is closed")


def main() -> None:
    test_detached_pty_device_is_not_logged_as_pointer_corruption()
    test_pty_pair_detaches_only_after_both_sides_close()
    print("PTY source invariants hold")


if __name__ == "__main__":
    main()
