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


def function_body(source: str, signature: str) -> str:
    start = source.find(signature)
    if start < 0:
        fail(f"missing function signature: {signature}")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"missing function body for: {signature}")

    depth = 1
    pos = brace + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function body for: {signature}")
    return source[brace + 1 : pos - 1]


def main() -> None:
    source = DIR2.read_text()
    block_lookup = function_body(source, "auto dir2_block_lookup")
    block_iterate = function_body(source, "auto dir2_block_iterate")
    leaf_lookup = function_body(source, "auto dir2_leaf_node_lookup")
    leaf_scan = function_body(source, "auto dir2_scan_data_block")
    leaf_find_data = function_body(source, "auto dir2_leaf_node_find_data_entry")
    leaf_find_free = function_body(source, "auto dir2_leaf_node_find_free_region")
    free_region = function_body(source, "auto dir2_find_free_region")
    sf_to_block = function_body(source, "auto dir2_sf_to_block")
    block_add = function_body(source, "auto dir2_block_addname")
    block_remove = function_body(source, "auto dir2_block_removename")
    leaf_add = function_body(source, "auto dir2_leaf_node_addname")

    require(
        source,
        "auto dir2_data_entry_at_if_valid(const XfsMountContext* ctx, const uint8_t* block, size_t offset, size_t data_end,",
        "bounded data-entry validator",
    )
    require(source, "tag->to_cpu() != static_cast<uint16_t>(offset)", "data entry tag validation")
    require(
        source,
        "auto dir2_data_unused_at_if_valid(const uint8_t* block, size_t offset, size_t data_end, uint16_t* free_len_out)",
        "bounded free-record validator",
    )
    require(source, "FREE_LEN < sizeof(XfsDir2DataUnused)", "free-record minimum length validation")
    require(source, "(FREE_LEN & (XFS_DIR2_DATA_ALIGN - 1)) != 0", "free-record alignment validation")
    require(source, "auto dir2_block_linear_lookup(const XfsMountContext* ctx, const uint8_t* block, size_t data_start, size_t data_end", "block linear lookup fallback")
    require(
        block_lookup,
        "int const FALLBACK = dir2_block_linear_lookup(ctx, block, DATA_START, DATA_END, name, namelen, entry);",
        "block lookup leaf-miss fallback",
    )
    require_order(
        block_lookup,
        "if (!found) {\n        int const FALLBACK = dir2_block_linear_lookup",
        "return FALLBACK;",
        "block lookup falls back before reporting leaf miss",
    )
    require(
        block_iterate,
        "if (!dir2_data_entry_at_if_valid(ctx, block, offset, DATA_END, &dep, &dep_size))",
        "block readdir bounded data-entry validation",
    )
    require(
        leaf_scan,
        "if (!dir2_data_entry_at_if_valid(ctx, block, offset, BLKSIZE, &dep, &dep_size))",
        "leaf/node readdir bounded data-entry validation",
    )
    require(
        leaf_lookup,
        "dir2_data_entry_at_if_valid(ctx, data_bh->data, OFF, ctx->dir_blk_size, &dep, nullptr)",
        "leaf-index lookup validates dataptr target",
    )
    require(
        leaf_lookup,
        "if (!dir2_data_entry_at_if_valid(ctx, block, offset, BLKSIZE, &dep, &dep_size))",
        "leaf linear lookup bounded data-entry validation",
    )
    require(
        leaf_find_data,
        "if (!dir2_data_entry_at_if_valid(ctx, block, off, DATA_END, &dep, &dep_size))",
        "leaf removal data search validates data entries",
    )
    require(
        free_region,
        "if (dir2_data_unused_at_if_valid(block, off, data_end, &candidate_free_len))",
        "generic free-space search validates free records",
    )
    require(
        leaf_find_free,
        "if (dir2_find_free_region(ctx, block, DATA_START, DATA_END, need_len, &free_off, &free_len))",
        "leaf free-space search uses validated free-region helper",
    )
    require(source, "auto dir2_block_to_leaf(XfsInode* dp, XfsTransaction* tp) -> int", "block-to-leaf converter")
    require(source, "data_hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_DATA_MAGIC)", "old block converted to data magic")
    require(source, "uint32_t const STALE_COUNT = btp->stale.to_cpu()", "block tail stale count preservation")
    require(source, "if (STALE_COUNT > LEAF_COUNT)", "block tail stale count validation")
    require(source, "dir2_init_leaf_header(dp, leaf_disk, leaf_hdr, LEAF_COUNT, STALE_COUNT)", "leaf count/stale preservation")
    require(source, "__builtin_memcpy(dir2_leaf_entries(leaf_hdr), leaf_copy, LEAF_BYTES)", "leaf entry preservation")
    require(source, "lep[LEAF_COUNT].hashval = Be32::from_cpu(UINT32_MAX)", "stale entries sorted after valid entries")
    require(source, "lep[LEAF_COUNT].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR)", "stale entry address")
    require(source, "xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log", "leaf block offset")
    require(source, "dir2_make_data_free(ctx, new_data, DATA_START, BLKSIZE, DATA_START, BLKSIZE - DATA_START)", "spare data block")
    require(source, "dp->size = 2ULL * static_cast<uint64_t>(ctx->dir_blk_size)", "data block count update")
    require(source, "dp->data_fork.extents.capacity = 1", "shortform extent capacity initialization")
    require(sf_to_block, "xfs_trans_log_buf_full(tp, bh);", "shortform conversion logs initialized block")
    require_order(
        sf_to_block,
        "__builtin_memcpy(&hdr3->hdr.crc, &CRC, sizeof(CRC));",
        "xfs_trans_log_buf_full(tp, bh);",
        "shortform conversion logs after final crc",
    )
    require_order(
        sf_to_block,
        "xfs_trans_log_buf_full(tp, bh);",
        "brelse(bh);",
        "shortform conversion logs before releasing buffer",
    )
    require(source, "auto dir2_leaf_ensure_stale_slot(XfsMountContext const* ctx, XfsDir3LeafHdr* hdr) -> int", "leaf slot growth")
    require(source, "auto dir2_leaf_preflight_index_slot(XfsMountContext const* ctx, const XfsDir3LeafHdr* hdr,", "leaf slot preflight")
    require(source, "auto dir2_count_stale_leaf_entries(const XfsDir2LeafEntry* entries, size_t count) -> size_t", "actual stale count")
    require(source, "if (ACTUAL_STALE != static_cast<size_t>(STALE_COUNT))", "leaf stale count validation")
    require(source, "auto dir2_leaf_alloc_data_block(XfsInode* dp, XfsTransaction* tp", "leaf data block allocation")
    require(source, "auto const NEW_DB = static_cast<xfs_dir2_db_t>(dir2_data_block_count(dp))", "append data block selection")
    require(source, "dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, NEW_DB), &disk_block)", "mapped data block allocation")
    require(source, "if (rc == -ENOSPC) {\n        rc = dir2_leaf_alloc_data_block", "leaf add ENOSPC data growth")
    require(leaf_add, "rc = dir2_leaf_preflight_index_slot(ctx, leaf_hdr);", "leaf add index preflight")
    require(leaf_add, "if (rc != 0) {\n        brelse(leaf_bh);\n        return rc;", "full or malformed leaf rejection")
    require(leaf_add, "rc = dir2_leaf_ensure_stale_slot(ctx, leaf_hdr);", "leaf add slot ensure")
    require(leaf_add, "rc = dir2_leaf_prepare_stale_insert(leaf_hdr, HASH, &stale_idx, &insert_pos);", "leaf add sorted slot preparation")
    require(leaf_add, "dir2_leaf_reuse_stale_entry(leaf_hdr, HASH, DATAPTR", "leaf add index update")
    require(source, "goto linear_scan", "leaf lookup linear fallback")
    require(source, "bool const LEAF_INDEX_FULL = leaf_count >= LEAF_CAPACITY", "full leaf lookup detection")
    require(source, "if (LEAF_INDEX_FULL) {\n                goto linear_scan;\n            }", "full leaf lookup data scan")
    require(source, "if (rc == 0) {\n        auto* lep = dir2_leaf_entries(leaf_hdr)", "leaf removal tolerates unindexed entries")
    require(
        block_add,
        "if (static_cast<size_t>(STALE_COUNT) != ACTUAL_STALE) {\n        brelse(bh);\n        return -EINVAL;\n    }",
        "block add stale count validation",
    )
    require(
        block_add,
        "if (STALE_COUNT == 0) {\n        size_t const NEW_LEAF_BYTES",
        "block add preflight for leaf-slot growth",
    )
    require(
        block_add,
        "if (found_offset + NEED_LEN > NEW_LEAF_START) {\n            brelse(bh);\n            return -ENOSPC;\n        }",
        "block add non-mutating ENOSPC return",
    )
    require(block_add, "uint16_t found_free_len = 0;", "block add stores validated free length")
    require(block_add, "uint16_t const OLD_FREE_LEN = found_free_len;", "block add reuses validated free length")
    require(
        block_remove,
        "if (OFF < DATA_START || !dir2_data_entry_at_if_valid(ctx, block, OFF, DATA_END, &dep, &dep_size))",
        "block remove validates leaf dataptr target",
    )
    require(
        block_remove,
        "if (dir2_data_unused_at_if_valid(block, off, DATA_END, &free_len))",
        "block remove validates free records while coalescing",
    )
    require_order(
        block_add,
        "if (found_offset + NEED_LEN > NEW_LEAF_START)",
        "auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);",
        "block add must prove leaf growth space before writing data entry",
    )
    require_order(
        block_add,
        "if (static_cast<size_t>(STALE_COUNT) != ACTUAL_STALE)",
        "auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);",
        "block add must validate stale slots before writing data entry",
    )
    require_order(
        leaf_add,
        "rc = dir2_leaf_preflight_index_slot(ctx, leaf_hdr);",
        "rc = dir2_leaf_node_find_free_region(dp, NEED_LEN, &data_bh, &db, &free_off, &free_len);",
        "leaf add must prove index capacity before data block growth",
    )
    require_order(
        leaf_add,
        "rc = dir2_leaf_prepare_stale_insert(leaf_hdr, HASH, &stale_idx, &insert_pos);",
        "dir2_write_data_entry(ctx, data_block, DATA_START, DATA_END, free_off, free_len, name, namelen, ino, ftype);",
        "leaf add must prepare index slot before writing data entry",
    )

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
