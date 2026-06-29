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

    read_overlay_start = source.find("// Full-block read. Large reads use direct I/O")
    read_overlay_end = source.find("// Partial or unaligned", read_overlay_start)
    if read_overlay_start < 0 or read_overlay_end < 0:
        fail("could not isolate full-block read overlay handling")
    read_overlay_body = source[read_overlay_start:read_overlay_end]

    require(
        source,
        "constexpr size_t XFS_BUFFERED_WRITE_BATCH_MAX_BYTES = size_t{2} * 1024 * 1024;",
        "XFS buffered writes must stay under the buffer-cache contiguous allocation cap",
    )
    require(
        source,
        "constexpr size_t XFS_DIRTY_THROTTLE_INTERVAL_BYTES = XFS_BUFFERED_WRITE_BATCH_MAX_BYTES;",
        "dirty throttling must run once per buffered write batch",
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
        "throttle_after_unlock = true;",
        "locked XFS write path must defer blocking dirty throttling until locks are released",
    )
    require_absent(
        write_body,
        "throttle_dirty_buffer_cache(ctx->device)",
        "locked XFS write path must not park in dirty throttling while holding metadata/io locks",
    )
    require_absent(
        write_body,
        "XfsMetadataGuard",
        "locked XFS write helper is shared by callers that already hold the mount metadata lock",
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
        "BufHead* bp = fresh_allocation ? xfs_buf_get(ctx, current_disk_block) : xfs_buf_read(ctx, current_disk_block)",
        "fresh partial regular-file writes must avoid reading old data",
    )
    require(
        buffered_body,
        "std::memset(bp->data, 0, ctx->block_size)",
        "fresh partial regular-file writes must zero unwritten bytes",
    )
    require(
        read_overlay_body,
        "copy_cached_bdev_range_if_complete(ctx->device, dev_block, dev_count, dst + total_read)",
        "full-block reads must reuse complete cached ranges",
    )
    require(
        read_overlay_body,
        "copy_dirty_bdev_range(ctx->device, dev_block, dev_count, direct_dst)",
        "direct reads must overlay dirty buffered writes",
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
