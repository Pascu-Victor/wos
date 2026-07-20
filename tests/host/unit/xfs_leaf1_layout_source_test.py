#!/usr/bin/env python3
"""Source invariants for valid XFS leaf1 tail and bests layout."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_leaf1_layout_source_test: {message}")


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


def main() -> None:
    source = DIR2.read_text()

    require(source, "struct XfsDir2LeafTail", "leaf1 tail structure")
    require(source, "trailer_size = sizeof(XfsDir2LeafTail) + (bestcount * sizeof(Be16));", "capacity accounting for tail and bests")
    require(source, "bestcount != dir2_data_block_count(dp)", "leaf1 bestcount validation")
    require(source, "leaf_tail->bestcount = Be32::from_cpu(INITIAL_BESTCOUNT);", "conversion bestcount initialization")
    require(source, "bests[0] = Be16::from_cpu(OLD_DATA_BEST);", "first data-block bests entry")
    require(source, "bests[1] = Be16::from_cpu(NEW_DATA_BEST);", "second data-block bests entry")
    require(source, "rc = dir2_leaf_preflight_index_slot(ctx, leaf_hdr, bestcount + 1);", "bests-growth preflight")
    require(source, "rc = dir2_leaf_extend_bests(ctx, leaf_hdr", "bests extension")
    require(source, "rc = dir2_leaf_set_best(ctx, leaf_hdr, db", "bests update after insertion")
    if "update_leaf = false" in source:
        fail("leaf overflow must not create unindexed data entries")


if __name__ == "__main__":
    main()
