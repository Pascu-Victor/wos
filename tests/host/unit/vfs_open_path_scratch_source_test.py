#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def forbid(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


def test_open_path_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()

    open_body = function_body(core, "vfs_open")
    require_order(
        open_body,
        [
            "std::array<char, MAX_PATH_LEN> raw_path __attribute__((uninitialized));",
            "if (path.size() >= MAX_PATH_LEN)",
            "std::memcpy(raw_path.data(), path.data(), path.size())",
            "raw_path[path.size()] = '\\0'",
            "path_requires_directory(raw_path.data(), path.size())",
            "std::array<char, MAX_PATH_LEN> path_buffer __attribute__((uninitialized));",
            "int const FAST_RET",
            "vfs_open_absolute_common_local_fast_path(task, raw_path.data(), path_buffer, &fast_path_requires_directory,",
            "if (FAST_RET == 0)",
            "vfs_open_resolved_for_task(task, raw_path.data(), path_buffer",
            "if (FAST_RET < 0)",
            "int const RESOLVE_RET",
            "resolve_dirfd_task_path_raw(task, AT_FDCWD, raw_path.data(), path_buffer.data(), path_buffer.size(), !OPEN_LOCAL,",
            "resolve_task_path_raw_impl(raw_path.data(), path_buffer.data(), MAX_PATH_LEN, !OPEN_LOCAL, &path_buffer_len,",
            "if (RESOLVE_RET < 0)",
            "return -ENOENT",
            "vfs_open_resolved_for_task(task, raw_path.data(), path_buffer",
        ],
        "vfs_open scratch producer and consumer ordering",
    )
    forbid(
        open_body,
        [
            "std::array<char, MAX_PATH_LEN> raw_path;",
            "std::array<char, MAX_PATH_LEN> raw_path{};",
            "std::array<char, MAX_PATH_LEN> path_buffer;",
            "std::array<char, MAX_PATH_LEN> path_buffer{};",
            "raw_path.fill(",
            "path_buffer.fill(",
            "std::memset(raw_path.data()",
            "std::memset(path_buffer.data()",
        ],
        "vfs_open redundant scratch initialization",
    )

    openat_body = function_body(core, "vfs_openat")
    require_order(
        openat_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved __attribute__((uninitialized));",
            "int const FAST_RET",
            "vfs_open_absolute_common_local_fast_path(task, pathname, resolved, &path_requires_directory,",
            "if (FAST_RET == 0)",
            "vfs_open_resolved_for_task(task, pathname, resolved",
            "if (FAST_RET < 0)",
            "int const RESOLVE_RET",
            "resolve_dirfd_task_path_raw(task, dirfd, pathname, resolved.data(), resolved.size(), !OPEN_LOCAL, &path_requires_directory,",
            "if (RESOLVE_RET < 0)",
            "return RESOLVE_RET",
            "vfs_open_resolved_for_task(task, pathname, resolved",
        ],
        "vfs_openat scratch producer and consumer ordering",
    )
    forbid(
        openat_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved;",
            "std::array<char, MAX_PATH_LEN> resolved{};",
            "resolved.fill(",
            "std::memset(resolved.data()",
        ],
        "vfs_openat redundant scratch initialization",
    )

    resolved_open_body = function_body(core, "vfs_open_resolved_for_task")
    require_order(
        resolved_open_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved __attribute__((uninitialized));",
            "int const RESOLVE_RET = resolve_symlinks(path_buffer.data(), resolved.data(), resolved.size(),",
            "if (RESOLVE_RET < 0)",
            "return RESOLVE_RET",
            "path_text_equal(path_buffer.data(), path_buffer_len, resolved.data(), resolved_len)",
        ],
        "open symlink scratch producer and consumer ordering",
    )
    forbid(
        resolved_open_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved;",
            "std::array<char, MAX_PATH_LEN> resolved{};",
            "resolved.fill(",
            "std::memset(resolved.data()",
        ],
        "open symlink redundant scratch initialization",
    )

    resolve_symlinks_body = function_body(core, "resolve_symlinks")
    require_order(
        resolve_symlinks_body,
        [
            "if (known_path_len != UNKNOWN_PATH_LEN)",
            "std::memcpy(resolved_buf, path, known_path_len)",
            "while (path[path_len] != '\\0' && path_len < bufsize - 1)",
            "resolved_buf[path_len] = path[path_len]",
            "resolved_buf[path_len] = '\\0'",
            "if (apply_task_policy)",
        ],
        "resolve_symlinks initial string construction",
    )


if __name__ == "__main__":
    test_open_path_scratch_is_initialized_by_its_producers()
    print("VFS open path scratch invariants hold")
