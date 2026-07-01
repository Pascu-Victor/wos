#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SMT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "smt" / "smt.cpp"


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
    match = re.search(rf"\bvoid\s+{name}\([^)]*\)\s*\{{", source)
    if match is None:
        fail(f"missing function {name}")
    end = find_matching_brace(source, match.end() - 1)
    return source[match.end() : end]


def require_order(source: str, snippets: list[str], context: str) -> None:
    cursor = 0
    for snippet in snippets:
        found = source.find(snippet, cursor)
        if found < 0:
            fail(f"{context}: missing ordered snippet {snippet}")
        cursor = found + len(snippet)


def test_init_stdio_preserves_vfs_access_modes() -> None:
    body = function_body(SMT_CPP.read_text(), "create_init_tasks")
    for snippet in [
        "constexpr int STDIN_OPEN_FLAGS = 0;",
        "constexpr int STDOUT_OPEN_FLAGS = 1;",
        'ker::vfs::devfs::devfs_open_path("/dev/console", STDIN_OPEN_FLAGS, 0)',
        'ker::vfs::devfs::devfs_open_path("/dev/console", STDOUT_OPEN_FLAGS, 0)',
    ]:
        if snippet not in body:
            fail(f"init stdio setup must preserve VFS access modes: {snippet}")

    require_order(
        body,
        [
            'ker::vfs::devfs::devfs_open_path("/dev/console", STDIN_OPEN_FLAGS, 0)',
            "(void)new_task->fd_table.insert(0, console_stdin);",
        ],
        "init stdin open mode must be selected before fd publication",
    )
    require_order(
        body,
        [
            'ker::vfs::devfs::devfs_open_path("/dev/console", STDOUT_OPEN_FLAGS, 0)',
            "(void)new_task->fd_table.insert(1, console_stdout);",
        ],
        "init stdout open mode must be selected before fd publication",
    )
    require_order(
        body,
        [
            'ker::vfs::devfs::devfs_open_path("/dev/console", STDOUT_OPEN_FLAGS, 0)',
            "(void)new_task->fd_table.insert(2, console_stderr);",
        ],
        "init stderr open mode must be selected before fd publication",
    )


if __name__ == "__main__":
    test_init_stdio_preserves_vfs_access_modes()
    print("init stdio access-mode invariants hold")
