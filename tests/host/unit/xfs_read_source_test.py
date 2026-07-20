#!/usr/bin/env python3

"""Source invariants for XFS regular-file reads."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_VFS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token!r}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    source = XFS_VFS.read_text()
    read_overlay_start = source.find("// Cache the complete request span")
    read_overlay_end = source.find("// Partial or unaligned", read_overlay_start)
    if read_overlay_start < 0 or read_overlay_end < 0:
        fail("could not isolate full-block read handling")
    read_overlay_body = source[read_overlay_start:read_overlay_end]

    require(
        read_overlay_body,
        "bread_multi(ctx->device, DEV_BLOCK, DEV_COUNT, BufferReadClass::FILE_DATA)",
        "full-block reads must populate and reuse the file-data buffer cache",
    )
    require(
        read_overlay_body,
        "std::memcpy(dst + total_read, bp->data, chunk)",
        "full-block reads must copy from the referenced cache span",
    )
    require_order(
        read_overlay_body,
        [
            "BufHead* bp = bread_multi(ctx->device, DEV_BLOCK, DEV_COUNT, BufferReadClass::FILE_DATA);",
            "if (bp == nullptr)",
            "return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);",
            "std::memcpy(dst + total_read, bp->data, chunk);",
            "brelse(bp);",
        ],
        "cached full-block reads must retain the span through the copy and release it afterwards",
    )


if __name__ == "__main__":
    main()
