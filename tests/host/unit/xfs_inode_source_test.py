#!/usr/bin/env python3
"""Source invariants for XFS inode reads."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INODE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_inode.cpp"
BMAP = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_bmap.cpp"
ALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_alloc.cpp"
FORMAT = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_format.hpp"
VFS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_inode_source_test: {message}")


def main() -> None:
    source = INODE.read_text()
    bmap_source = BMAP.read_text()
    alloc_source = ALLOC.read_text()
    format_source = FORMAT.read_text()
    vfs_source = VFS.read_text()

    marked_free = 'mod::dbg::logger<"xfs">::debug("xfs_inode_read: inode %lu is marked free"'
    lookup_failed = "if (ALLOCATED < 0) {"
    rate_limited = "alloc_lookup_failure_count.fetch_add(1, std::memory_order_relaxed) + 1"
    warning_interval = "ALLOC_LOOKUP_WARN_INTERVAL"
    lookup_warn = '"xfs_inode_read: allocation lookup failed'
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

    for needle, description in (
        ("constexpr uint64_t XFS_DIFLAG2_NREXT64 = (1ULL << 4);", "per-inode NREXT64 flag"),
        ("bool const LARGE_EXTENT_COUNTS = (ip->flags2 & XFS_DIFLAG2_NREXT64) != 0;", "per-inode extent counter selection"),
        ("if (LARGE_EXTENT_COUNTS && !xfs_has_nrext64(mount))", "NREXT64 feature validation"),
        ("dip->di_anextents = Be16{0};", "NREXT64 padding clear"),
        ("dip->di_aformat = static_cast<int8_t>(XFS_DINODE_FMT_EXTENTS);", "absent attribute fork format"),
    ):
        haystack = format_source if "constexpr" in needle else source
        if needle not in haystack:
            fail(f"missing {description}")

    if "if (xfs_has_nrext64(mount))" in source:
        fail("inode counter union must not be selected from the superblock feature alone")
    if "ip->flags2 |= XFS_DIFLAG2_BIGTIME;" not in vfs_source or "ip->flags2 |= XFS_DIFLAG2_NREXT64;" not in vfs_source:
        fail("new inodes must inherit enabled per-inode format flags")

    if source.find(lookup_failed) > source.find(disk_read):
        fail("allocation lookup warning must continue into disk dinode validation")

    for needle, description in (
        ("validate_inode_extent_records(", "inode extent preflight helper"),
        ("xfs_validate_allocated_extent(mount, agno, agbno", "non-mutating allocator validation before free"),
        ("uint32_t const LIST_CAPACITY = ip->nextents + 1;", "btree extent overrun detection capacity"),
        ("std::cmp_not_equal(rc, ip->nextents)", "btree extent count mismatch check"),
        ("free_inode_extent_records(ip, tp, extents", "data free after validation"),
        ("inactivation_rc = inactivate_unlinked_inode(ip);", "inactivation return is preserved"),
        ("if (inactivation_rc != 0 && ip->nlink == 0)", "failed inactivation keeps inode cached"),
        ("ip->inactivation_started = false;", "failed inactivation can be retried"),
        ("ip->io_lock.lock();", "final inactivation takes inode I/O mutex"),
        ("ip->io_lock.unlock();", "final inactivation releases inode I/O mutex"),
    ):
        if needle not in source:
            fail(f"missing {description}")

    validate_pos = source.find("validate_inode_extent_records(ip, extents")
    free_pos = source.find("free_inode_extent_records(ip, tp, extents")
    if validate_pos < 0 or free_pos < 0 or validate_pos > free_pos:
        fail("btree data extents must be validated before being freed")

    for needle, description in (
        ("auto xfs_validate_allocated_extent(", "allocator validation helper definition"),
        ("extent overlaps previous free record", "previous free-space overlap diagnostic"),
        ("extent overlaps next free record", "next free-space overlap diagnostic"),
    ):
        if needle not in alloc_source:
            fail(f"missing {description}")

    free_extent = alloc_source[
        alloc_source.find("auto xfs_free_extent(") : alloc_source.find("// ============================================================================\n// AGFL")
    ]
    validation_pos = free_extent.find("xfs_validate_allocated_extent(mount, agno, agbno, len)")
    refill_pos = free_extent.find("if (pag->agf_flcount < XFS_AGFL_MIN)")
    neighbor_lookup_pos = free_extent.find("XfsBtreeCursor<XfsBnobtTraits> prev_cur;")
    if min(validation_pos, refill_pos, neighbor_lookup_pos) < 0 or not validation_pos < refill_pos < neighbor_lookup_pos:
        fail("xfs_free_extent must validate before AGFL refill and reopen coalescing cursors after refill")

    bmap_loop_start = bmap_source.find("while (count < max_extents)")
    bmap_loop = bmap_source[bmap_loop_start : bmap_source.find("return static_cast<int>(count);", bmap_loop_start)]
    if "if (rc == -ENOENT)" not in bmap_loop or "return rc;" not in bmap_loop:
        fail("btree extent listing must return traversal errors instead of a partial count")


if __name__ == "__main__":
    main()
