#!/usr/bin/env python3
"""Source invariants for XFS inode allocation lookup."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
IALLOC = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_ialloc.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


def function_body(source: str, name: str) -> str:
    start = source.find(f"auto {name}(")
    if start < 0:
        fail(f"missing function {name}")
    open_brace = source.find("{", start)
    if open_brace < 0:
        fail(f"missing body for {name}")

    depth = 1
    pos = open_brace + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[open_brace + 1 : pos - 1]


def main() -> None:
    source = IALLOC.read_text()
    contains = function_body(source, "inobt_record_contains")
    free_mask = function_body(source, "inobt_free_mask_for_bit")
    first_free = function_body(source, "first_free_inode_bit")
    allocated = function_body(source, "xfs_inode_allocated")

    require(source, "constexpr uint32_t XFS_INODES_PER_HOLEMASK_BIT = 4;", "sparse inode holemask granularity")
    require(
        contains,
        "if (agino < rec.startino)",
        "containing-record lower-bound check",
    )
    require(
        contains,
        "if (BIT >= XFS_INODES_PER_CHUNK)",
        "containing-record upper-bound check",
    )
    require(
        contains,
        "return (rec.holemask & (static_cast<uint16_t>(1U) << HOLE_BIT)) == 0;",
        "sparse inode holemask exclusion",
    )
    require(
        free_mask,
        "return static_cast<uint64_t>(1) << bit;",
        "XFS inobt free mask bit order",
    )
    require(
        first_free,
        "if ((free_mask & inobt_free_mask_for_bit(bit)) != 0)",
        "first free inode scan uses XFS bit order",
    )
    require(
        source,
        "rec.free_mask &= ~inobt_free_mask_for_bit(static_cast<uint32_t>(BIT));",
        "allocation clears XFS-ordered free bit",
    )
    require(
        allocated,
        "target.startino = AGINO;",
        "allocation lookup targets actual AG inode",
    )
    require(
        allocated,
        "xfs_btree_lookup(&cur, pag->agi_root, pag->agi_level, target, XfsBtreeLookup::LE)",
        "allocation lookup finds containing inobt record",
    )
    require(
        allocated,
        "if (RC == -ENOENT)",
        "allocation lookup treats no containing record as free",
    )
    require(
        allocated,
        "if (!inobt_record_contains(REC, AGINO))",
        "allocation lookup verifies containing record",
    )
    require(
        allocated,
        "uint32_t const BIT = AGINO - REC.startino;",
        "free mask bit is relative to containing record",
    )
    require(
        allocated,
        "uint64_t const INODE_BIT = inobt_free_mask_for_bit(BIT);",
        "allocation state uses XFS-ordered free bit",
    )
    require(
        source,
        "rec.free_mask |= INODE_BIT;",
        "free path restores XFS-ordered free bit",
    )


if __name__ == "__main__":
    main()
    print("XFS inode allocation lookup invariants hold")
