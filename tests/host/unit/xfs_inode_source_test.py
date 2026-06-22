#!/usr/bin/env python3
"""Source invariants for XFS inode reads."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INODE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_inode.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_inode_source_test: {message}")


def main() -> None:
    source = INODE.read_text()

    marked_free = 'if (ALLOCATED == 0) {\n        mod::dbg::logger<"xfs">::debug("xfs_inode_read: inode %lu is marked free"'
    lookup_failed = "if (ALLOCATED < 0) {"
    rate_limited = "alloc_lookup_failure_count.fetch_add(1, std::memory_order_relaxed) + 1"
    warning_interval = "ALLOC_LOOKUP_WARN_INTERVAL"
    lookup_warn = 'mod::dbg::logger<"xfs">::warn(\n                "xfs_inode_read: allocation lookup failed'
    direct_validation = "validating dinode directly"
    disk_read = "// Not in cache - read from disk"

    for needle, description in (
        (marked_free, "hard failure only for known-free inodes"),
        (lookup_failed, "allocation lookup warning branch"),
        (rate_limited, "rate-limited allocation lookup counter"),
        (warning_interval, "allocation lookup warning interval"),
        (lookup_warn, "rate-limited allocation lookup warning"),
        (direct_validation, "direct dinode validation warning"),
        (disk_read, "disk read after allocation check"),
    ):
        if needle not in source:
            fail(f"missing {description}")

    if source.find(lookup_failed) > source.find(disk_read):
        fail("allocation lookup warning must continue into disk dinode validation")


if __name__ == "__main__":
    main()
