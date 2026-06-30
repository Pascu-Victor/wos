#!/usr/bin/env python3

"""Source invariants for XFS extent free and buffer-cache ownership."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_INODE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_inode.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


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


def test_extent_free_drains_dirty_buffers_before_reuse() -> None:
    source = XFS_INODE.read_text()
    body = function_body(source, "free_inode_data_extent")

    require_order(
        body,
        [
            "uint64_t const DEV_BLOCK = inode_fsblock_to_dev_block(mount, startblock);",
            "size_t const DEV_COUNT = inode_fsb_to_dev_count(mount, blockcount);",
            "sync_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);",
            "xfs_free_extent(mount, tp, agno, agbno, static_cast<xfs_extlen_t>(SPAN));",
            "discard_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);",
        ],
        "extent free must drain dirty buffers before freeing and discard cache after freeing",
    )


def main() -> None:
    test_extent_free_drains_dirty_buffers_before_reuse()


if __name__ == "__main__":
    main()
