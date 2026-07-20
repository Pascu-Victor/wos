#!/usr/bin/env python3

"""Source invariants for XFS buffered regular-file writes."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_VFS = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        position = source.find(token, cursor)
        if position < 0:
            fail(f"{context}: missing or out-of-order {token}")
        cursor = position + len(token)


def main() -> None:
    source = XFS_VFS.read_text()
    write_start = source.find("auto xfs_vfs_write_locked(")
    write_end = source.find("auto xfs_vfs_write(File* f", write_start)
    if write_start < 0 or write_end < 0:
        fail("could not isolate xfs_vfs_write_locked body")
    write_body = source[write_start:write_end]

    buffered_start = write_body.find("auto buffered_write = ")
    buffered_end = write_body.find("auto write_extent_data = ", buffered_start)
    if buffered_start < 0 or buffered_end < 0:
        fail("could not isolate buffered_write body")
    buffered_body = write_body[buffered_start:buffered_end]

    read_overlay_start = source.find("// Cache the complete request span")
    read_overlay_end = source.find("// Partial or unaligned", read_overlay_start)
    if read_overlay_start < 0 or read_overlay_end < 0:
        fail("could not isolate full-block read overlay handling")
    read_overlay_body = source[read_overlay_start:read_overlay_end]

    require(
        source,
        "constexpr size_t XFS_BUFFERED_WRITE_BATCH_MAX_BYTES = size_t{256} * 1024;",
        "XFS buffered writes must stay under the buffer-cache contiguous allocation cap",
    )
    require(
        source,
        "constexpr xfs_extlen_t XFS_WRITE_ALLOC_TRANSACTION_BLOCKS = 1024;",
        "large XFS hole writes must split metadata allocation transactions before transaction log headroom is exhausted",
    )
    require(
        source,
        "alloc_blocks = std::min(alloc_blocks, XFS_WRITE_ALLOC_TRANSACTION_BLOCKS);",
        "XFS hole allocation helper must enforce the metadata transaction block cap",
    )
    require(
        source,
        "auto xfs_write_alloc_min_blocks(xfs_extlen_t max_blocks, bool extent_pressure, bool sequential_append) -> xfs_extlen_t",
        "XFS write allocation must compute a contiguous allocation floor for streaming writes",
    )
    require(
        source,
        "return std::max<xfs_extlen_t>(1, std::min(max_blocks, XFS_STREAM_PREALLOC_BLOCKS));",
        "XFS streaming writes must prefer the stream preallocation run before falling back to tiny extents",
    )
    require(
        source,
        "if (xfs_bmap_lookup(ip, file_block - 1, &prev) != 0 || prev.is_hole || prev.startblock == NULLFSBLOCK)",
        "XFS sequential allocation hints must work after BMBT promotion",
    )
    require(
        source,
        "xfs_set_alloc_hint_from_fsb(ctx, NEXT_FSB, req);",
        "XFS sequential allocation hints must target the physical block after the previous logical block",
    )
    require(
        source,
        "constexpr size_t XFS_DIRTY_THROTTLE_INTERVAL_BYTES = size_t{4} * 1024 * 1024;",
        "dirty throttling must use the current bounded writeback interval",
    )
    require(
        source,
        "void xfs_stamp_inode_data_change(XfsInode* ip)",
        "XFS must provide a shared data-change timestamp helper",
    )
    require(
        source,
        "ip->mtime = NOW;\n    ip->ctime = NOW;\n    ip->dirty = true;",
        "XFS data changes must advance mtime/ctime and mark inode dirty",
    )
    require(
        write_body,
        "xfs_stamp_inode_data_change(ip);",
        "regular file writes must refresh timestamps even without size growth",
    )
    require(
        write_body,
        "bool& throttle_after_unlock",
        "locked XFS write path must report deferred dirty throttling to its caller",
    )
    require(
        write_body,
        "kick_dirty_buffer_cache_writeback(ctx->device);",
        "locked XFS write path may only kick dirty writeback while holding XFS locks",
    )
    require(
        write_body,
        "req.minlen = xfs_write_alloc_min_blocks(req.maxlen, EXTENT_PRESSURE, SEQUENTIAL_APPEND);",
        "regular file writes must ask for contiguous stream extents before falling back",
    )
    require(
        write_body,
        "if (ret != -ENOSPC || req.minlen == 1)",
        "regular file writes must only reduce allocation minlen on ENOSPC",
    )
    require(
        write_body,
        "req.minlen = NEXT_MINLEN;",
        "regular file writes must retry fragmented files with smaller allocation floors",
    )
    require(
        write_body,
        "throttle_after_unlock = true;",
        "locked XFS write path must defer blocking dirty throttling until locks are released",
    )
    require_absent(
        write_body,
        "throttle_dirty_buffer_cache(ctx->device)",
        "locked XFS write path must not park in dirty throttling while holding metadata/io locks",
    )
    require_order(
        write_body,
        [
            "ssize_t const MAPPED_FAST_RET = try_mapped_write_without_metadata_lock();",
            "if (MAPPED_FAST_RET != -EAGAIN)",
            "return finish_write(MAPPED_FAST_RET);",
            "XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());",
        ],
        "mapped writes must try the lockless path before acquiring the metadata lock for allocation or inode updates",
    )
    require_absent(
        source,
        "XfsMetadataUnlockedScope",
        "XFS writes must not drop and reacquire the mount metadata lock while inode io_lock is held",
    )
    if source.count("throttle_dirty_buffer_cache(xfd->mount->device);") < 2:
        fail("XFS write and append wrappers must run deferred dirty throttling after write locks are released")
    require_absent(write_body, "dev::block_write(", "regular file write extent path")
    require_absent(write_body, "discard_bdev_range(", "regular file write extent path")
    require_absent(write_body, "xfs_direct_write_full_blocks", "regular file write extent path")
    require_absent(write_body, "xfs_direct_write_zeroed_partial_block", "regular file write extent path")
    require_absent(write_body, "xfs_full_block_write_can_write_direct", "regular file write extent path")
    require(
        buffered_body,
        "BufHead* bp = bget_multi(ctx->device, DEV_BLOCK, DEV_COUNT)",
        "full-block regular file writes must stay buffered",
    )
    require(
        buffered_body,
        "std::memcpy(bp->data, src + current_src_offset, BATCH_BYTES)",
        "full-block regular file writes must copy into cache",
    )
    require(
        buffered_body,
        "bdirty(bp)",
        "full-block regular file writes must dirty cached buffers",
    )
    require(
        buffered_body,
        "BufHead* bp = fresh_allocation ? xfs_buf_get(ctx, current_disk_block) : xfs_buf_read_data(ctx, current_disk_block)",
        "fresh partial regular-file writes must avoid reading old data",
    )
    require(
        buffered_body,
        "xfs_zero_fresh_block_unwritten_ranges(bp->data, ctx->block_size, block_off, CHUNK)",
        "fresh partial regular-file writes must zero unwritten bytes",
    )
    require(
        read_overlay_body,
        "bread_multi(ctx->device, DEV_BLOCK, DEV_COUNT, BufferReadClass::FILE_DATA)",
        "full-block reads must populate and reuse cached data spans",
    )
    require(
        read_overlay_body,
        "std::memcpy(dst + total_read, bp->data, chunk)",
        "full-block reads must consume the authoritative cached span",
    )
    truncate_start = source.find("auto xfs_vfs_truncate(File* f")
    truncate_end = source.find("// ============================================================================\n// FileOperations vtable", truncate_start)
    if truncate_start < 0 or truncate_end < 0:
        fail("could not isolate xfs_vfs_truncate body")
    truncate_body = source[truncate_start:truncate_end]
    if truncate_body.count("xfs_stamp_inode_data_change(ip);") < 3:
        fail("all XFS truncate mutation branches must refresh mtime/ctime")
    print("XFS write buffering invariants hold")


if __name__ == "__main__":
    main()
