#!/usr/bin/env python3
"""Source invariants for XFS shortform directory offsets."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_shortform_offset_source_test: {message}")


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


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
    repair = function_body(source, "auto sf_repair_offset_tags")
    next_offset = function_body(source, "auto sf_next_offset_tag")

    require(
        source,
        "return sizeof(XfsDir3DataHdr) + dir2_data_entsize(ctx, 1) + dir2_data_entsize(ctx, 2);",
        "first offset after the data header and dot entries",
    )
    require(repair, "if (stored < next_offset)", "overlapping offset repair")
    require(
        repair,
        "next_offset = stored + dir2_data_entsize(ctx, sfep->namelen);",
        "repair advancement by the hypothetical data-entry size",
    )
    require(next_offset, "if (STORED < next_offset)", "offset overlap rejection")
    require(
        next_offset,
        "next_offset = STORED + dir2_data_entsize(ctx, sfep->namelen);",
        "append advancement by the hypothetical data-entry size",
    )
    require(
        source,
        "auto xfs_selftest_shortform_offsets_match_data_layout() -> bool",
        "runtime selftest for shortform layout offsets",
    )


if __name__ == "__main__":
    main()
