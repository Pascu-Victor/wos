#!/usr/bin/env python3
"""Source invariants for XFS inode reads."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INODE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_inode.cpp"
BMAP = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_bmap.cpp"
ALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_alloc.cpp"
IALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_ialloc.cpp"
BTREE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_btree.cpp"
DIR2 = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp"
ATTR = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_attr.cpp"
FORMAT = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_format.hpp"
VFS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp"
MOUNT = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_mount.cpp"
LOG = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_log.cpp"
TRANS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_trans.cpp"
TRANS_HPP = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_trans.hpp"
CREATE_ROOTFS = ROOT / "scripts/build/create_mountfs_disk.sh"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_inode_source_test: {message}")


def main() -> None:
    source = INODE.read_text()
    bmap_source = BMAP.read_text()
    alloc_source = ALLOC.read_text()
    ialloc_source = IALLOC.read_text()
    btree_source = BTREE.read_text()
    dir2_source = DIR2.read_text()
    attr_source = ATTR.read_text()
    format_source = FORMAT.read_text()
    vfs_source = VFS.read_text()
    mount_source = MOUNT.read_text()
    log_source = LOG.read_text()
    trans_source = TRANS.read_text()
    trans_header = TRANS_HPP.read_text()
    create_rootfs = CREATE_ROOTFS.read_text()

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
        ("xfs_alloc_ensure_freelist_headroom(mount, tp, agno)", "AGFL headroom before extent free"),
        ("uint32_t const LIST_CAPACITY = ip->nextents + 1;", "btree extent overrun detection capacity"),
        ("std::cmp_not_equal(rc, ip->nextents)", "btree extent count mismatch check"),
        ("free_inode_extent_records(ip, tp, extents", "data free after validation"),
        ("inactivation_rc = inactivate_unlinked_inode(ip);", "inactivation return is preserved"),
        ("if (inactivation_rc != 0 && ip->nlink == 0)", "failed inactivation keeps inode cached"),
        ("ip->inactivation_started = false;", "failed inactivation can be retried"),
        ("ip->io_lock.lock();", "final inactivation takes inode I/O mutex"),
        ("ip->io_lock.unlock();", "final inactivation releases inode I/O mutex"),
        ("xfs_trans_capture_inode(tp, ip);", "inactivation rollback snapshot"),
        ("rc = xfs_inode_truncate_data(ip, tp);", "inactivation clears data fork"),
        ("ip->mode = 0;", "free dinode mode clear"),
        ("xfs_trans_log_inode(tp, ip);", "free dinode core logging"),
    ):
        if needle not in source:
            fail(f"missing {description}")

    inactivate_start = source.find("auto inactivate_unlinked_inode(")
    inactivate_end = source.find("auto count_cached_mount_inodes(", inactivate_start)
    inactivate = source[inactivate_start:inactivate_end]
    cursor = 0
    for token in (
        "xfs_trans_capture_inode(tp, ip);",
        "xfs_inode_truncate_data(ip, tp);",
        "ip->mode = 0;",
        "xfs_trans_log_inode(tp, ip);",
        "xfs_ifree(ip->mount, tp, ip->ino);",
        "xfs_trans_commit(tp);",
    ):
        pos = inactivate.find(token, cursor)
        if pos < 0:
            fail(f"inactivation lifecycle ordering missing {token!r}")
        cursor = pos + len(token)

    for needle, description in (
        ("uint16_t mode{};", "inode undo mode snapshot field"),
        ("undo->mode = ip->mode;", "inode undo mode capture"),
        ("undo->ip->mode = undo->mode;", "inode undo mode restore"),
    ):
        haystack = trans_header if needle == "uint16_t mode{};" else trans_source
        if needle not in haystack:
            fail(f"missing {description}")

    for needle, description in (
        ("constexpr uint32_t SUPPORTED_RO_COMPAT = XFS_SB_FEAT_RO_COMPAT_FINOBT;", "write-compatible XFS feature allowlist"),
        ("refusing read-write mount with unsupported ro-compat features", "unsafe XFS feature rejection"),
        ("auto xfs_sync_superblock_counters(", "lazy superblock counter sync"),
        ("dsb->sb_icount = Be64::from_cpu(inode_count);", "superblock inode count update"),
        ("dsb->sb_ifree = Be64::from_cpu(free_inode_count);", "superblock free-inode update"),
        ("dsb->sb_fdblocks = Be64::from_cpu(free_block_count);", "superblock free-block update"),
    ):
        if needle not in mount_source:
            fail(f"missing {description}")

    supported_start = mount_source.find("constexpr uint32_t SUPPORTED_INCOMPAT")
    supported_end = mount_source.find("uint32_t const UNSUPPORTED", supported_start)
    if "XFS_SB_FEAT_INCOMPAT_PARENT" in mount_source[supported_start:supported_end]:
        fail("read-write feature allowlist must not advertise unmaintained parent pointers")
    if "xfs_sync_superblock_counters(ctx)" not in vfs_source:
        fail("mount sync must persist lazy superblock counters after AG metadata")
    for option in ("rmapbt=0", "reflink=0", "inobtcount=0", "parent=0"):
        if option not in create_rootfs:
            fail(f"rootfs mkfs must disable unsupported feature {option}")

    for haystack, needle, description in (
        (mount_source, "dsb->sb_lsn = Be64{};", "clean superblock LSN"),
        (source, "dip->di_lsn = Be64{};", "clean dinode LSN"),
        (ialloc_source, "agi->agi_lsn = Be64{};", "clean AGI LSN"),
        (alloc_source, "agf->agf_lsn = Be64{};", "clean AGF LSN"),
        (alloc_source, "agfl->agfl_lsn = Be64{};", "clean AGFL LSN"),
        (btree_source, "hdr->bb_lsn = Be64{};", "clean short/long btree LSN"),
        (bmap_source, "hdr->bb_lsn = Be64{};", "clean bmap btree LSN"),
        (dir2_source, "hdr->info.lsn = Be64{};", "clean directory DA LSN"),
        (dir2_source, "hdr->hdr.lsn = Be64{};", "clean directory data/free LSN"),
        (attr_source, "hdr->info.lsn = Be64{};", "clean attribute leaf LSN"),
    ):
        if needle not in haystack:
            fail(f"missing {description}")
    for needle, description in (
        ("home_metadata_clean = xfs_sync_mount(ctx) == 0;", "clean-home log discard gate"),
        ("xfs_log_unmount(ctx, home_metadata_clean);", "clean-home state passed to log teardown"),
    ):
        if needle not in mount_source:
            fail(f"missing {description}")
    for needle, description in (
        ("if (home_metadata_clean && FLUSH_RC == 0)", "log clearing only after successful home sync and final flush"),
        ("auto xfs_log_clear_clean(", "clean log erasure helper"),
        ("__builtin_memset(bh->data, 0, bh->size);", "complete clean log block erasure"),
        ("int const RC = bwrite(bh);", "synchronous clean log erasure"),
    ):
        if needle not in log_source:
            fail(f"missing {description}")

    mkdir_start = vfs_source.find("auto xfs_mkdir_path(")
    mkdir_end = vfs_source.find("// Rmdir", mkdir_start)
    mkdir_source = vfs_source[mkdir_start:mkdir_end]
    mkdir_cursor = 0
    for token in (
        "if (parent_ip->nlink == UINT32_MAX)",
        "xfs_dir_addname(parent_ip, dirname, dirname_len, NEW_INO, XFS_DIR3_FT_DIR, tp, true);",
        "parent_ip->nlink++;",
        "xfs_trans_log_inode(tp, parent_ip);",
        "xfs_trans_commit(tp);",
    ):
        pos = mkdir_source.find(token, mkdir_cursor)
        if pos < 0:
            fail(f"mkdir parent-link lifecycle ordering missing {token!r}")
        mkdir_cursor = pos + len(token)

    rmdir_start = vfs_source.find("auto xfs_rmdir_path(")
    rmdir_end = vfs_source.find("// Link", rmdir_start)
    rmdir_source = vfs_source[rmdir_start:rmdir_end]
    rmdir_cursor = 0
    for token in (
        "if (parent_ip->nlink <= 2)",
        "xfs_dir_removename(parent_ip, name, namelen, tp);",
        "parent_ip->nlink--;",
        "xfs_trans_log_inode(tp, parent_ip);",
        "dir_ip->nlink = 0;",
        "xfs_trans_commit(tp);",
    ):
        pos = rmdir_source.find(token, rmdir_cursor)
        if pos < 0:
            fail(f"rmdir parent-link lifecycle ordering missing {token!r}")
        rmdir_cursor = pos + len(token)

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
    refill_pos = free_extent.find("if (pag->agf_flcount < agfl_reserve_blocks(mount))")
    neighbor_lookup_pos = free_extent.find("XfsBtreeCursor<XfsBnobtTraits> prev_cur;")
    if min(validation_pos, refill_pos, neighbor_lookup_pos) < 0 or not validation_pos < refill_pos < neighbor_lookup_pos:
        fail("xfs_free_extent must validate before AGFL refill and reopen coalescing cursors after refill")

    for allocator_name in ("alloc_ag_by_hint", "alloc_ag_by_size"):
        allocator_start = alloc_source.find(f"auto {allocator_name}(")
        allocator_end = alloc_source.find("\nauto ", allocator_start + 1)
        allocator = alloc_source[allocator_start:allocator_end]
        headroom_pos = allocator.find("xfs_alloc_ensure_freelist_headroom(mount, tp, agno)")
        cursor_pos = allocator.find("XfsBtreeCursor<")
        if min(allocator_start, headroom_pos, cursor_pos) < 0 or headroom_pos > cursor_pos:
            fail(f"{allocator_name} must reserve AGFL headroom before opening free-space cursors")

    bmap_loop_start = bmap_source.find("while (count < max_extents)")
    bmap_loop = bmap_source[bmap_loop_start : bmap_source.find("return static_cast<int>(count);", bmap_loop_start)]
    if "if (rc == -ENOENT)" not in bmap_loop or "return rc;" not in bmap_loop:
        fail("btree extent listing must return traversal errors instead of a partial count")

    bmbt_return_start = bmap_source.find("auto bmbt_return_block(")
    bmbt_return = bmap_source[bmbt_return_start : bmap_source.find("auto bmdr_key_addr(", bmbt_return_start)]
    headroom_pos = bmbt_return.find("xfs_alloc_ensure_freelist_headroom(mount, tp, AGNO)")
    put_pos = bmbt_return.find("xfs_alloc_put_freelist(mount, tp, AGNO, AGBNO)")
    if min(headroom_pos, put_pos) < 0 or headroom_pos > put_pos:
        fail("BMBT retirement must reserve AGFL headroom before returning a block")


if __name__ == "__main__":
    main()
