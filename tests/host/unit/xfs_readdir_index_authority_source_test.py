#!/usr/bin/env python3
"""Source invariants for XFS leaf/node namespace authority."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp").read_text()
VFS = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()


def function_body(source: str, signature: str) -> str:
    start = source.index(signature)
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace : index + 1]
    raise AssertionError(f"unterminated function: {signature}")


membership = function_body(DIR2, "auto xfs_dir_entry_is_indexed(")
assert "dir2_extent_or_btree_lookup" in membership
assert "&indexed, false" in membership
assert "xfs_dir_lookup_authoritative" not in membership

visibility = function_body(VFS, "auto readdir_entry_visibility(")
assert "xfs_dentry_cache_lookup_parent" in visibility
assert "cached_result != -ENOENT" in visibility
assert "xfs_dir_entry_is_indexed" in visibility

single = function_body(VFS, "auto readdir_callback(")
batch = function_body(VFS, "auto readdir_batch_callback(")
for body in (single, batch):
    assert "readdir_entry_visibility" in body
    assert "VISIBILITY == 0" in body

print("XFS readdir index-authority source invariants passed")
