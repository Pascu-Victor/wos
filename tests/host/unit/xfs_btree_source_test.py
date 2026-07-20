#!/usr/bin/env python3
"""Source invariants for XFS btree split bookkeeping."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BTREE = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_btree.cpp"
ALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_alloc.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_btree_source_test: {message}")


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


def require_order(source: str, first: str, second: str, description: str) -> None:
    first_pos = source.find(first)
    second_pos = source.find(second)
    if first_pos < 0 or second_pos < 0 or first_pos >= second_pos:
        fail(f"bad order for {description}")


def main() -> None:
    source = BTREE.read_text()
    alloc_source = ALLOC.read_text()

    split_start = source.find("auto btree_split_internal(")
    parent_start = source.find("\n// Insert a new key/pointer pair", split_start)
    if split_start < 0 or parent_start < 0 or split_start >= parent_start:
        fail("could not isolate btree_split_internal")
    split = source[split_start:parent_start]

    left_init = "btree_set_numrecs_raw<Traits>(left_data, MID);"
    right_init = "btree_set_numrecs_raw<Traits>(right_data, RIGHT_NR);"
    insert_comment = "// Insert the new key/ptr into the correct half"
    promoted_copy = "__builtin_memcpy(&promoted_key, right_data + Traits::HDR_LEN, Traits::KEY_LEN);"
    parent_call = "btree_insert_into_parent<Traits>(cur, tp, lev + 1, promoted_key"

    require(split, left_init, "left split count initialization")
    require(split, right_init, "right split count initialization")
    require(split, "typename Traits::Key promoted_key;", "post-insert promoted key declaration")
    require(split, promoted_copy, "post-insert promoted key copy")
    require(source, "auto btree_node_ptr_off(uint32_t block_size, size_t n) -> size_t", "fixed node pointer offset helper")
    require(
        source,
        "btree_node_max_keys<Traits>(block_size)) * Traits::KEY_LEN",
        "pointer array starts after max key array",
    )
    require(
        source,
        "btree_write_ptr<Traits>(nr_data, cur->mount->block_size, 1, left_ptr);",
        "new root writes child pointer through fixed layout helper",
    )

    require_order(split, left_init, insert_comment, "left count before half insertion")
    require_order(split, right_init, insert_comment, "right count before half insertion")
    require_order(split, insert_comment, promoted_copy, "promoted key recomputed after insertion")
    require_order(split, promoted_copy, parent_call, "recomputed key propagated to parent")

    old_promote = "// Compute the promoted key (first key of right half, i.e. key at index mid)"
    if old_promote in split:
        fail("promoted key must not be captured before insertion")

    leaf_split = source[source.find("// Leaf is full - split it.") : source.find("// ============================================================================\n// B+tree delete")]
    if "cur->level_at(0).bp = right_bp;" in leaf_split:
        fail("leaf split must keep cursor on the left/root block until parent insertion")

    nonfull_parent = source[
        source.find("if (PARENT_NR < MAX_KEYS) {") : source.find("// Parent is full - split it")
    ]
    require(nonfull_parent, "uint8_t* ptr_base = p_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);", "fixed parent pointer base")
    if "new_ptr_base" in nonfull_parent or "old_ptr_base" in nonfull_parent:
        fail("parent insertion must not relocate pointer arrays based on current key count")

    require(split, "uint8_t* lp_base = left_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);", "fixed left split pointer base")
    require(split, "uint8_t* rp_base = right_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);", "fixed right split pointer base")
    require(split, "int const RIGHT_INSERT = insert_pos - MID;  // 1-based in right block", "right internal split insert index")
    if "insert_pos - MID - 1" in split:
        fail("internal split right-half insertion must not shift one slot too far left")
    if "lp_base_new" in split or "lp_base_old" in split or "rp_base_new" in split or "rp_base_old" in split:
        fail("internal split must not relocate pointer arrays based on current key count")

    require(leaf_split, "int const RIGHT_INSERT = insert_ptr - MID;  // 1-based in right block", "right leaf split insert index")
    if "insert_ptr - MID - 1" in leaf_split:
        fail("leaf split right-half insertion must not shift one slot too far left")

    require(
        source,
        "cur->mount->per_ag[cur->agno].agf_flcount < required_blocks",
        "whole split-chain AGFL preflight",
    )
    require(
        source,
        "if (cur->ptr_at(lev, pos) == left_ptr)",
        "parent insertion must locate the exact child that was split",
    )
    require(
        source,
        "cur->level_at(lev).ptr = left_pos;\n    int const INSERT_POS = left_pos + 1;",
        "parent insertion position derived from verified left child",
    )
    if "xfs_alloc_put_freelist(cur->mount, tp, cur->agno" in source:
        fail("btree deletion must not publish retired blocks before transaction-safe deferred retirement")
    require(
        source,
        "bool preserve_only_leaf = NR == 1 && cur->nlevels > 1;",
        "sole multi-level leaf preservation predicate",
    )
    require(
        source,
        "for (int lev = 1; preserve_only_leaf && lev < cur->nlevels; ++lev) {\n        preserve_only_leaf = cur->numrecs(lev) == 1;\n    }",
        "sole-child check through every internal level",
    )
    require(
        source,
        "if (preserve_only_leaf) {\n            // Every internal level has exactly one child",
        "empty sole leaf retention before parent removal",
    )

    alloc_extent_start = alloc_source.find("auto xfs_alloc_extent(")
    alloc_extent = alloc_source[alloc_extent_start:]
    if alloc_extent_start < 0:
        fail("could not isolate xfs_alloc_extent")
    require(
        alloc_extent,
        "if (HINT_RC != -ENOSPC) {\n                return HINT_RC;\n            }",
        "hinted allocation propagates non-space failures",
    )
    require(
        alloc_extent,
        "if (RC != -ENOSPC) {\n            return RC;\n        }",
        "preferred and fallback allocation propagate non-space failures",
    )

    by_size_start = alloc_source.find("auto alloc_ag_by_size(")
    by_size_end = alloc_source.find("\nauto log_agf_free_space_roots(", by_size_start)
    if by_size_start < 0 or by_size_end < 0 or by_size_start >= by_size_end:
        fail("could not isolate alloc_ag_by_size")
    by_size = alloc_source[by_size_start:by_size_end]
    require(
        by_size,
        "return rc == -ENOENT ? -ENOSPC : rc;",
        "empty cntbt lookup distinguishes no space from metadata failure",
    )
    require(
        by_size,
        "if (rc != -ENOENT) {\n                return rc;\n            }",
        "cntbt traversal propagates metadata failures",
    )


if __name__ == "__main__":
    main()
