#!/usr/bin/env python3
"""Source invariants for XFS close/fsync durability boundaries."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_VFS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_close_source_test: {message}")


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\bauto\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    source = XFS_VFS.read_text()
    close_body = function_body(source, "xfs_vfs_close")
    fsync_body = function_body(source, "xfs_fsync")

    if "sync_inode_data_buffers" in close_body:
        fail("close must not force data-buffer durability; use fsync for that")

    require_order(
        close_body,
        [
            "MutexGuard guard(xfd->inode->io_lock)",
            "xfs_commit_dirty_inode(xfd->mount, xfd->inode)",
            "xfs_inode_release(xfd->inode)",
        ],
        "close must commit dirty inode metadata before releasing the cached inode",
    )

    require_order(
        fsync_body,
        [
            "sync_inode_data_buffers(xfd->mount, xfd->inode, xfd->inode->size)",
            "xfs_commit_dirty_inode(xfd->mount, xfd->inode)",
        ],
        "fsync must keep data-before-inode durability",
    )


if __name__ == "__main__":
    main()
