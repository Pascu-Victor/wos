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
    write_extent_start = source.find("auto write_extent_data = ")
    write_loop_end = source.find("#ifdef XFS_BENCH", write_extent_start)
    if write_extent_start < 0 or write_loop_end < 0:
        fail("could not isolate write_extent_data body")
    write_extent_body = source[write_extent_start:write_loop_end]

    read_overlap_start = source.find("bool const HAS_DIRTY_OVERLAP")
    read_overlap_end = source.find("} else {", read_overlap_start)
    if read_overlap_start < 0 or read_overlap_end < 0:
        fail("could not isolate dirty-overlap read handling")
    read_overlap_body = source[read_overlap_start:read_overlap_end]

    require_absent(source, "auto direct_full_block_write(", "regular file writes must not bypass dirty buffer cache")
    require_absent(write_extent_body, "dev::block_write(", "regular file write extent path")
    require_absent(write_extent_body, "discard_bdev_range(", "regular file write extent path")
    require(
        write_extent_body,
        "buffered_write(current_disk_block, 0, DIRECT_BYTES, current_src_offset)",
        "full-block regular file writes must stay buffered",
    )
    require(
        read_overlap_body,
        "bool const HAS_DIRTY_OVERLAP = has_dirty_bdev_range(ctx->device, DEV_BLOCK, DEV_COUNT)",
        "full-block reads must detect dirty buffered writes",
    )
    require(
        read_overlap_body,
        "BufHead* bp = xfs_buf_read(ctx, DISK_BLOCK + static_cast<xfs_fsblock_t>(i))",
        "dirty-overlap reads must use per-block cached buffers",
    )
    print("XFS write buffering invariants hold")


if __name__ == "__main__":
    main()
