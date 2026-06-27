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
    read_overlay_start = source.find("// Full-block read. Large reads use direct I/O")
    read_overlay_end = source.find("// Partial or unaligned", read_overlay_start)
    if read_overlay_start < 0 or read_overlay_end < 0:
        fail("could not isolate full-block read handling")
    read_overlay_body = source[read_overlay_start:read_overlay_end]

    require(read_overlay_body, "auto read_cached_blocks = [&]() -> bool", "full-block reads need a buffered fallback")
    require(
        read_overlay_body,
        "copy_cached_bdev_range_if_complete(ctx->device, dev_block, dev_count, dst + total_read)",
        "full-block reads must reuse complete cached ranges before direct I/O",
    )
    require(
        read_overlay_body,
        "copy_dirty_bdev_range(ctx->device, dev_block, dev_count, direct_dst)",
        "successful direct reads must overlay dirty buffered writes",
    )
    require(
        read_overlay_body,
        "falling back to buffered blocks",
        "failed direct reads must leave a diagnostic before buffered fallback",
    )
    require_order(
        read_overlay_body,
        [
            "int const RC = xfs_block_read_with_retry(ctx->device, dev_block, dev_count, direct_dst)",
            "if (RC != 0)",
            "bool const OK = read_cached_blocks();",
            "OK ? 0 : RC, OK ? chunk : 0",
            "if (!OK)",
            "return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);",
            "} else {",
            "copy_dirty_bdev_range(ctx->device, dev_block, dev_count, direct_dst)",
        ],
        "direct-read failures must try buffered blocks before reporting EIO while preserving the direct success overlay",
    )


if __name__ == "__main__":
    main()
