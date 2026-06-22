#!/usr/bin/env python3
"""Source invariants for XFS block-directory growth."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_dir_growth_source_test: {message}")


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


def require_order(source: str, first: str, second: str, description: str) -> None:
    first_pos = source.find(first)
    second_pos = source.find(second)
    if first_pos < 0 or second_pos < 0 or first_pos >= second_pos:
        fail(f"bad order for {description}")


def main() -> None:
    source = DIR2.read_text()

    require(source, "auto dir2_block_to_leaf(XfsInode* dp, XfsTransaction* tp) -> int", "block-to-leaf converter")
    require(source, "data_hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_DATA_MAGIC)", "old block converted to data magic")
    require(source, "dir2_init_leaf_header(dp, leaf_disk, leaf_hdr, LEAF_CAPACITY, LEAF_CAPACITY - LEAF_COUNT)", "leaf stale capacity")
    require(source, "leaf_copy[i].hashval = Be32::from_cpu(UINT32_MAX)", "stale entries sorted after valid entries")
    require(source, "leaf_copy[i].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR)", "stale entry address")
    require(source, "xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log", "leaf block offset")
    require(source, "dir2_make_data_free(ctx, new_data, DATA_START, BLKSIZE, DATA_START, BLKSIZE - DATA_START)", "spare data block")
    require(source, "dp->size = 2ULL * static_cast<uint64_t>(ctx->dir_blk_size)", "data block count update")
    require(source, "dp->data_fork.extents.capacity = 1", "shortform extent capacity initialization")
    require(source, "auto dir2_leaf_ensure_stale_slot(XfsMountContext const* ctx, XfsDir3LeafHdr* hdr) -> int", "leaf slot growth")
    require(source, "auto dir2_leaf_alloc_data_block(XfsInode* dp, XfsTransaction* tp", "leaf data block allocation")
    require(source, "auto const NEW_DB = static_cast<xfs_dir2_db_t>(dir2_data_block_count(dp))", "append data block selection")
    require(source, "dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, NEW_DB), &disk_block)", "mapped data block allocation")
    require(source, "if (rc == -ENOSPC) {\n        rc = dir2_leaf_alloc_data_block", "leaf add ENOSPC data growth")
    require(source, "if (rc == -ENOSPC) {\n        update_leaf = false", "partial leaf index fallback")
    require(source, "if (update_leaf) {\n        dir2_leaf_reuse_stale_entry", "conditional leaf update")
    require(source, "goto linear_scan", "leaf lookup linear fallback")
    require(source, "if (rc == 0) {\n        auto* lep = dir2_leaf_entries(leaf_hdr)", "leaf removal tolerates unindexed entries")

    require_order(
        source,
        "rc = dir2_block_addname(dp, name, namelen, ino, ftype, tp);",
        "rc = dir2_block_to_leaf(dp, tp);",
        "block add ENOSPC conversion",
    )
    require_order(
        source,
        "rc = dir2_block_to_leaf(dp, tp);",
        "rc = dir2_leaf_node_addname(dp, name, namelen, ino, ftype, tp);",
        "leaf add retry",
    )


if __name__ == "__main__":
    main()
