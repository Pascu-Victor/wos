#!/usr/bin/env python3
"""Source invariants for XFS v3 level-1 node-directory growth."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp").read_text()
FORMAT = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_format.hpp").read_text()


def require(source: str, needle: str, label: str) -> None:
    if needle not in source:
        raise SystemExit(f"xfs_node_directory_source_test: missing {label}: {needle}")


require(FORMAT, "struct XfsDir3FreeHdr", "v3 free-space header")
require(FORMAT, "static_assert(sizeof(XfsDir3FreeHdr) == 64);", "free-space header size")
require(FORMAT, "XFS_DIR3_LEAFN_MAGIC = 0x3DFF", "upstream v3 leafn magic")
require(FORMAT, "XFS_DA3_NODE_MAGIC = 0x3EBE", "upstream v3 DA node magic")
require(DIR2, "auto dir2_leaf1_to_node_addname", "leaf1-to-node conversion")
require(DIR2, "dir2_init_node_header(dp, root_disk, root_hdr, 2);", "level-1 root initialization")
require(DIR2, "dir2_init_leafn_header", "leafn child initialization")
require(DIR2, "dir2_init_free_header", "XDF3 initialization")
require(DIR2, "auto dir2_find_free_leaf_block", "mapped-hole leaf allocation")
require(DIR2, "rc = dir2_find_free_leaf_block(dp, &new_leaf_fsbno);", "repeated child split allocation")
require(DIR2, "if (HASH > node_entries[i].hashval.to_cpu())", "equal-boundary remove scan")
require(DIR2, "if (node_entries[i].hashval.to_cpu() > HASH)", "remove stop after equal boundaries")
require(DIR2, "auto dir2_node_replacement_order_valid", "root-key order validator")
require(DIR2, "!dir2_node_replacement_order_valid(root_entries, ROOT_COUNT, child_index, LEFT_MAX, RIGHT_MAX)", "root-key order preflight")
require(DIR2, "if (LEAF_MAGIC == XFS_DA3_NODE_MAGIC)", "node-root lookup dispatch")
require(DIR2, "dir2_lookup_leafn_hash", "node child hash lookup")
require(DIR2, "auto xfs_selftest_node_directory_growth_layout", "6000-entry growth selftest")
