#!/usr/bin/env python3
"""Source invariants for fragmented XFS block-directory leaf growth."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp").read_text()


def fail(message: str) -> None:
    raise SystemExit(f"xfs_block_leaf_slot_source_test: {message}")


def require(needle: str, description: str) -> None:
    if needle not in SOURCE:
        fail(f"missing {description}: {needle}")


def require_order(first: str, second: str, description: str) -> None:
    first_pos = SOURCE.find(first)
    second_pos = SOURCE.find(second)
    if first_pos < 0 or second_pos < 0 or first_pos >= second_pos:
        fail(f"bad order for {description}")


require(
    "STALE_COUNT == 0 && (!found_tail_free || static_cast<size_t>(tail_free_len) < LEAF_SLOT_SIZE)",
    "trailing-free leaf-slot preflight",
)
require("if (STALE_COUNT == 0 && IS_TAIL_FREE)", "leaf slot exclusion from trailing data space")
require("current_data_end -= LEAF_SLOT_SIZE;", "leaf slot reservation")
require(
    "dir2_rebuild_data_bestfree(ctx, block, DATA_START, current_data_end);",
    "post-growth bestfree rebuild",
)
require_order(
    "current_data_end -= LEAF_SLOT_SIZE;",
    "auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);",
    "leaf reservation before data mutation",
)
require_order(
    "if (static_cast<size_t>(STALE_COUNT) != ACTUAL_STALE)",
    "auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);",
    "stale validation before data mutation",
)

print("XFS fragmented block-directory leaf-slot source invariants hold")
