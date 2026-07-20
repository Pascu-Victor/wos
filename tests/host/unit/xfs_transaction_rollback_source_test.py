#!/usr/bin/env python3
"""Source invariants for pre-log XFS transaction rollback."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS = ROOT / "modules/kern/src/vfs/fs/xfs"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    start = -1
    search_from = 0
    while True:
        candidates = [source.find(f"\n{prefix} {name}(", search_from) for prefix in ("auto", "void", "int")]
        candidate = min((value for value in candidates if value >= 0), default=-1)
        if candidate < 0:
            break
        brace = source.find("{", candidate)
        semicolon = source.find(";", candidate)
        if brace >= 0 and (semicolon < 0 or brace < semicolon):
            start = candidate
            break
        search_from = candidate + 1
    if start < 0:
        fail(f"missing function {name}")
    open_brace = source.find("{", start)
    depth = 1
    pos = open_brace + 1
    while pos < len(source) and depth:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth:
        fail(f"unterminated function {name}")
    return source[open_brace + 1 : pos - 1]


def require_order(source: str, tokens: list[str], description: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{description}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    header = (XFS / "xfs_trans.hpp").read_text()
    trans = (XFS / "xfs_trans.cpp").read_text()
    btree = (XFS / "xfs_btree.cpp").read_text()
    alloc = (XFS / "xfs_alloc.cpp").read_text()
    bmap = (XFS / "xfs_bmap.cpp").read_text()
    ialloc = (XFS / "xfs_ialloc.cpp").read_text()
    inode = (XFS / "xfs_inode.cpp").read_text()
    dir2 = (XFS / "xfs_dir2.cpp").read_text()
    log = (XFS / "xfs_log.cpp").read_text()
    vfs = (XFS / "xfs_vfs.cpp").read_text()

    for token in [
        "struct XfsTransBufUndo",
        "struct XfsTransPerAgUndo",
        "struct XfsTransInodeUndo",
        "uint32_t nlink{};",
        "bool journal_held{};",
        "auto xfs_trans_capture_buf(XfsTransaction* tp, BufHead* bp) -> int;",
        "auto xfs_trans_capture_perag(XfsTransaction* tp, xfs_agnumber_t agno) -> int;",
        "auto xfs_trans_capture_inode(XfsTransaction* tp, XfsInode* ip) -> int;",
    ]:
        if token not in header:
            fail(f"transaction header missing undo contract {token!r}")

    restore = function_body(trans, "xfs_trans_restore_undo")
    for forbidden in ["new ", "xfs_buf_read(", "xfs_buf_get("]:
        if forbidden in restore:
            fail(f"cancel restoration must not allocate or issue I/O: {forbidden}")
    require_order(
        function_body(trans, "xfs_trans_cancel"),
        ["xfs_trans_restore_undo(tp);", "brelse(item.buf.bp);", "xfs_trans_release(tp);"],
        "cancel restores before releasing transaction buffers",
    )
    require_order(
        function_body(trans, "xfs_trans_capture_inode"),
        ["undo->nlink = ip->nlink;", "undo->dirty = ip->dirty;"],
        "inode capture snapshots link count",
    )
    if "undo->ip->nlink = undo->nlink;" not in restore:
        fail("inode cancellation must restore link count")
    require_order(
        function_body(trans, "xfs_trans_capture_buf"),
        ["bjournal_hold(bp);", "__builtin_memcpy(undo->before_image"],
        "buffer capture blocks writeback before copying or mutation",
    )
    require_order(
        function_body(trans, "xfs_trans_discard_undo"),
        ["bjournal_release(undo->bp);", "brelse(undo->bp);"],
        "undo disposal releases the precommit journal hold before its reference",
    )
    require_order(
        function_body(trans, "xfs_trans_commit"),
        ["tp->committed = true;", "xfs_trans_discard_undo(tp);", "xfs_trans_release(tp);"],
        "successful commit discards before-images",
    )
    require_order(
        function_body(trans, "xfs_trans_commit"),
        ["int const LOG_RC = xfs_log_write", "xfs_trans_cancel(tp);", "return LOG_RC;", "bdirty(item.buf.bp);"],
        "active-log failure cancels before metadata writeback",
    )
    require_order(
        function_body(log, "xfs_log_batch_flush_locked"),
        ["if (RC != 0)", "return RC;", "bdirty(item.buf.bp);"],
        "failed WAL flush retains holds and does not dirty metadata",
    )

    require_order(
        function_body(btree, "xfs_btree_update"),
        ["xfs_trans_capture_buf(tp, cur->level_at(0).bp)", "__builtin_memcpy(rec, &new_rec"],
        "btree update captures its leaf before overwrite",
    )
    require_order(
        function_body(btree, "xfs_btree_delete"),
        ["xfs_trans_capture_buf(tp, cur->level_at(0).bp)", "std::memmove(base"],
        "btree delete captures its leaf before compaction",
    )
    require_order(
        function_body(btree, "xfs_btree_insert"),
        ["xfs_trans_capture_buf(tp, cur->level_at(0).bp)", "std::memmove(base"],
        "btree insert captures its leaf before insertion",
    )

    require_order(
        function_body(alloc, "alloc_ag_by_size"),
        ["xfs_trans_capture_perag(tp, agno)", "agfl_refill(mount, tp, agno)"],
        "allocator snapshots per-AG state before refill/mutation",
    )
    require_order(
        function_body(alloc, "log_agf_free_space_roots"),
        ["xfs_trans_capture_buf(tp, agf_bh)", "agf->agf_freeblks"],
        "AGF logging captures the containing buffer before overwrite",
    )
    require_order(
        function_body(ialloc, "ialloc_ag"),
        ["xfs_trans_capture_perag(tp, agno)", "rec.free_mask &=", "xfs_btree_update(&cur, tp, rec)"],
        "inode allocation snapshots accounting before record mutation",
    )
    require_order(
        function_body(bmap, "xfs_bmap_add_extent"),
        ["xfs_trans_capture_inode(tp, ip)", "try_extents_fast_add(ip, tp, new_ext"],
        "bmap snapshots the inode fork before fast or rebuilding mutations",
    )
    require_order(
        function_body(inode, "xfs_inode_write"),
        ["xfs_trans_capture_buf(tp, bh)", "__builtin_memset(dip, 0, mount->inode_size)"],
        "inode write captures its disk buffer before serialization",
    )
    require_order(
        function_body(dir2, "xfs_dir_addname"),
        ["xfs_trans_capture_inode(tp, dp)", "switch (dp->data_fork.format)"],
        "directory add snapshots the inode before shortform or mapping mutation",
    )
    require_order(
        function_body(dir2, "dir2_block_addname"),
        ["xfs_trans_capture_buf(tp, bh)", "dep->inumber ="],
        "block-directory add snapshots the existing block before overwrite",
    )
    require_order(
        function_body(dir2, "dir2_block_to_leaf"),
        ["xfs_trans_capture_buf(tp, block_bh)", "dir2_alloc_mapped_dir_block"],
        "block-to-leaf conversion snapshots the existing data block before allocation",
    )
    require_order(
        function_body(dir2, "dir2_leaf1_to_node_addname"),
        ["xfs_trans_capture_buf(tp, root_bh)", "dir2_alloc_mapped_dir_block"],
        "leaf1-to-node conversion snapshots its old root before allocation",
    )
    require_order(
        function_body(dir2, "dir2_leaf_node_removename"),
        ["xfs_trans_capture_buf(tp, data_bh)", "lep[leaf_idx].address ="],
        "leaf/node remove snapshots data and index buffers before overwrite",
    )

    require_order(
        function_body(vfs, "xfs_link_path"),
        ["xfs_trans_capture_inode(tp, source_ip)", "source_ip->nlink++", "xfs_dir_addname(new_parent"],
        "hard link snapshots link count before namespace mutation",
    )
    require_order(
        function_body(vfs, "xfs_unlink_path"),
        [
            "target_ip = xfs_inode_read_known_allocated",
            "xfs_trans_capture_inode(tp, target_ip)",
            "xfs_dir_removename(parent_ip",
            "target_ip->nlink--",
        ],
        "unlink holds and snapshots its target before removing the name",
    )
    require_order(
        function_body(vfs, "xfs_rmdir_path"),
        ["xfs_trans_capture_inode(tp, dir_ip)", "xfs_dir_removename(parent_ip", "dir_ip->nlink = 0"],
        "rmdir snapshots the removed directory link count",
    )
    require_order(
        function_body(vfs, "xfs_rename_path"),
        [
            "displaced = xfs_inode_read_known_allocated",
            "xfs_trans_capture_inode(tp, displaced)",
            "xfs_dir_removename(new_parent",
            "displaced->nlink--",
            "xfs_dir_addname(new_parent",
            "xfs_dir_removename(old_parent",
        ],
        "overwrite rename snapshots the displaced inode before namespace mutation",
    )

    for function_name, add_token in [
        ("xfs_open_path", "xfs_dir_addname(create_parent_ip"),
        ("xfs_mkdir_path", "xfs_dir_addname(parent_ip"),
        ("xfs_symlink_path", "xfs_dir_addname(parent_ip"),
    ]:
        body = function_body(vfs, function_name)
        require_order(
            body,
            ["xfs_ialloc(ctx, tp", "xfs_ialloc_conflicts_with_cached_inode(ctx, NEW_INO)", add_token, "xfs_trans_commit(tp)"],
            f"{function_name} rejects an allocator/cache collision before publishing a name",
        )
    if "ip = xfs_inode_read_known_allocated(ctx, NEW_INO);" in function_body(vfs, "xfs_open_path"):
        fail("create must not reopen an existing cached inode after publishing a new name")


if __name__ == "__main__":
    main()
    print("XFS transaction rollback source invariants hold")
