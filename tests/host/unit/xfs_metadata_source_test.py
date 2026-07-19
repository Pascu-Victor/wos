#!/usr/bin/env python3

"""Source invariants for XFS v5 metadata CRC finalization."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_BTREE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_btree.cpp"
XFS_ALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_alloc.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token!r}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token!r}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def test_btree_updates_refresh_crc_before_logging() -> None:
    source = XFS_BTREE.read_text()
    update_body = function_body(source, "xfs_btree_update")

    require_absent(update_body, "rec_offset", "btree record update must not log stale partial blocks")
    require_order(
        update_body,
        [
            "__builtin_memcpy(rec, &new_rec, Traits::REC_LEN)",
            "btree_update_crc<Traits>(cur->level_at(0).bp);",
            "xfs_trans_log_buf_full(tp, cur->level_at(0).bp);",
        ],
        "btree record update must refresh leaf CRC before logging",
    )
    require(
        update_body,
        "btree_update_crc<Traits>(cur->level_at(lev).bp);",
        "btree first-record update must refresh parent CRCs",
    )


def test_btree_insert_delete_refresh_crc_before_logging() -> None:
    source = XFS_BTREE.read_text()
    insert_body = function_body(source, "xfs_btree_insert")
    delete_body = function_body(source, "xfs_btree_delete")

    require_order(
        insert_body,
        [
            "cur->set_numrecs(0, NR + 1);",
            "btree_update_crc<Traits>(cur->level_at(0).bp);",
            "xfs_trans_log_buf_full(tp, cur->level_at(0).bp);",
        ],
        "btree non-split insert must refresh leaf CRC before logging",
    )
    require(
        insert_body,
        "btree_update_crc<Traits>(cur->level_at(lev).bp);",
        "btree first-record insert must refresh parent CRCs",
    )
    require_order(
        delete_body,
        [
            "cur->set_numrecs(0, NR - 1);",
            "btree_update_crc<Traits>(cur->level_at(0).bp);",
            "xfs_trans_log_buf_full(tp, cur->level_at(0).bp);",
        ],
        "btree non-empty delete must refresh leaf CRC before logging",
    )
    require(
        delete_body,
        "btree_update_crc<Traits>(cur->level_at(lev).bp);",
        "btree first-record delete must refresh parent CRCs",
    )


def test_btree_split_key_propagation_is_buffer_safe() -> None:
    source = XFS_BTREE.read_text()
    insert_body = function_body(source, "xfs_btree_insert")
    split_internal_body = function_body(source, "btree_split_internal")

    require(
        source,
        "auto btree_propagate_first_key(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int child_level, const typename Traits::Key& key)",
        "btree split first-key propagation helper",
    )
    require_order(
        insert_body,
        [
            "Key right_first_key;",
            "Traits::init_key_from_rec(&right_first_key, reinterpret_cast<const Rec*>(right_data + Traits::HDR_LEN));",
            "brelse(right_bp);",
            "return btree_insert_into_parent<Traits>(cur, tp, 1, right_first_key",
        ],
        "btree leaf split must capture promoted right key before releasing right buffer",
    )
    require_order(
        insert_body,
        [
            "if (insert_ptr == 1 && cur->nlevels > 1)",
            "Traits::init_key_from_rec(&left_first_key, reinterpret_cast<const Rec*>(left_data + Traits::HDR_LEN));",
            "btree_propagate_first_key<Traits>(cur, tp, 0, left_first_key);",
        ],
        "btree leaf split must propagate changed left first key",
    )
    require_order(
        split_internal_body,
        [
            "if (insert_pos == 1 && lev + 1 < cur->nlevels)",
            "__builtin_memcpy(&left_first_key, left_data + Traits::HDR_LEN, Traits::KEY_LEN);",
            "btree_propagate_first_key<Traits>(cur, tp, lev, left_first_key);",
        ],
        "btree internal split must propagate changed left first key",
    )


def test_agfl_slot_updates_refresh_sector_crc() -> None:
    source = XFS_ALLOC.read_text()
    helper_body = function_body(source, "update_agfl_crc")
    put_body = function_body(source, "xfs_alloc_put_freelist")

    require_order(
        helper_body,
        [
            "agfl->agfl_crc = Be32{0};",
            "util::crc32c_block_with_cksum(agfl, mount->sect_size, XFS_AGFL_CRC_OFF)",
            "__builtin_memcpy(&agfl->agfl_crc, &crc, sizeof(crc));",
        ],
        "AGFL CRC helper",
    )
    require_absent(put_body, "SLOT_OFF", "AGFL slot update must log the CRC-covered sector")
    require_order(
        put_body,
        [
            "agfl_bno[pag->agf_fllast] = Be32::from_cpu(bno);",
            "pag->agf_flcount++;",
            "update_agfl_crc(mount, agfl);",
            "xfs_trans_log_buf(tp, bh, static_cast<uint32_t>(agfl_off), static_cast<uint32_t>(mount->sect_size));",
            "log_agf_freelist(mount, tp, agno, bh)",
        ],
        "AGFL put must refresh AGFL CRC before logging AGFL and AGF",
    )


def test_free_space_allocators_validate_dual_indexes_before_delete() -> None:
    source = XFS_ALLOC.read_text()
    agfl_body = function_body(source, "agfl_refill")
    hint_body = function_body(source, "alloc_ag_by_hint")
    size_body = function_body(source, "alloc_ag_by_size")
    delete_body = function_body(source, "delete_free_extent_record")

    require(source, "auto same_free_extent(", "free-space tree identity helper")

    require_order(
        agfl_body,
        [
            "BNO_TARGET{.startblock = FOUND.startblock, .blockcount = FOUND.blockcount}",
            "XfsBtreeLookup::EQ",
            "same_free_extent(BNO_FOUND.startblock, BNO_FOUND.blockcount, FOUND.startblock, FOUND.blockcount)",
            "rc = xfs_btree_delete(&cur, tp);",
            "rc = xfs_btree_delete(&bno_cur, tp);",
        ],
        "AGFL refill must validate bnobt before deleting either free-space record",
    )

    require_order(
        hint_body,
        [
            "CNT_TARGET{.startblock = FOUND.startblock, .blockcount = FOUND.blockcount}",
            "XfsBtreeLookup::EQ",
            "same_free_extent(CNT_FOUND.startblock, CNT_FOUND.blockcount, FOUND.startblock, FOUND.blockcount)",
            "rc = xfs_btree_delete(&bno_cur, tp);",
            "rc = xfs_btree_delete(&cnt_cur, tp);",
        ],
        "hint allocation must validate cntbt before deleting either free-space record",
    )

    bnobt_probe = size_body[
        size_body.find("XfsBnobtTraits::IRec bno_target{};") : size_body.find("// Delete from cntbt")
    ]
    require_absent(
        bnobt_probe,
        "XfsBtreeLookup::GE",
        "size allocation must not use nearest-greater bnobt lookup for free-space deletion",
    )
    require_order(
        size_body,
        [
            "bno_target.blockcount = found.blockcount;",
            "XfsBtreeLookup::EQ",
            "same_free_extent(BNO_FOUND.startblock, BNO_FOUND.blockcount, found.startblock, found.blockcount)",
            "int delrc = xfs_btree_delete(&cur, tp);",
            "delrc = xfs_btree_delete(&bno_cur, tp);",
        ],
        "size allocation must validate exact bnobt extent before deleting either free-space record",
    )

    require_order(
        delete_body,
        [
            "XfsBtreeLookup::EQ",
            "XfsCntbtTraits::IRec const CNT_TARGET",
            "XfsBtreeLookup::EQ",
            "same_free_extent(CNT_REC.startblock, CNT_REC.blockcount, BNO_REC.startblock, BNO_REC.blockcount)",
            "rc = xfs_btree_delete(&bno_cur, tp);",
            "return xfs_btree_delete(&cnt_cur, tp);",
        ],
        "free-space coalescing must validate both indexes before deleting either record",
    )


def main() -> None:
    test_btree_updates_refresh_crc_before_logging()
    test_btree_insert_delete_refresh_crc_before_logging()
    test_btree_split_key_propagation_is_buffer_safe()
    test_agfl_slot_updates_refresh_sector_crc()
    test_free_space_allocators_validate_dual_indexes_before_delete()
    print("XFS metadata CRC source invariants hold")


if __name__ == "__main__":
    main()
