#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
VFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "vfs.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\bauto\s+{re.escape(name)}\([^)]*\)\s*->\s*[A-Za-z0-9_:<>*]+(?:\s+const)?\s*\{{", source)
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


def require(body: str, token: str, context: str) -> None:
    if token not in body:
        fail(f"{context}: missing {token!r}")


def require_absent(body: str, token: str, context: str) -> None:
    if token in body:
        fail(f"{context}: unexpected {token!r}")


def main() -> None:
    source = CORE_CPP.read_text()
    header = VFS_HPP.read_text()

    require(
        header,
        "auto vfs_write_file(File* file, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;",
        "vfs.hpp",
    )

    write_file_body = function_body(source, "vfs_write_file")
    require(write_file_body, "TMPFS_APPEND", "vfs_write_file")
    require(write_file_body, "XFS_APPEND", "vfs_write_file")
    require(write_file_body, "cache_notify_file_data_changed_impl(f);", "vfs_write_file")

    write_body = function_body(source, "vfs_write")
    require(write_body, "vfs_get_file_retain(t, fd)", "vfs_write")
    require(write_body, "ssize_t const RESULT = vfs_write_file(f, buf, count, actual_size);", "vfs_write")
    require(write_body, "vfs_put_file(f);", "vfs_write")

    sendfile_body = function_body(source, "vfs_sendfile")
    require(sendfile_body, "vfs_sendfile_to_pipe(outfile, infile, &source_offset, count)", "vfs_sendfile")
    require(sendfile_body, "vfs_pread_file(infile, buffer, TO_READ, source_offset)", "vfs_sendfile")
    require(sendfile_body, "vfs_write_file(outfile, buffer + chunk_offset, CHUNK_SIZE - chunk_offset, &bytes_written)", "vfs_sendfile")
    require_absent(sendfile_body, "vfs_pread(infd, buffer", "vfs_sendfile")
    require_absent(sendfile_body, "vfs_write(outfd, buffer", "vfs_sendfile")

    print("VFS sendfile retained-file source invariants hold")


if __name__ == "__main__":
    main()
