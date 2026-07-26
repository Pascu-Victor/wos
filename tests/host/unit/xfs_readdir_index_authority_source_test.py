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
assert "dir_entry_index_membership(dp, observed" in membership

index_membership = function_body(DIR2, "auto dir_entry_index_membership(")
assert "xfs_inode_stat(dp->mount, indexed->ino" in index_membership
assert "TARGET_STATUS == -ENOENT" in index_membership
assert "TARGET_STATUS != 0" in index_membership

visibility = function_body(VFS, "auto readdir_entry_visibility(")
assert "xfs_dentry_cache_lookup_parent" in visibility
assert "&may_have_removed_record" in visibility
assert "!may_have_removed_record && (!CACHED || cached_result != -ENOENT)" in visibility
assert "xfs_dir_entry_is_indexed" in visibility

remove = function_body(DIR2, "auto xfs_dir_removename(")
assert "xfs_dentry_cache_note_removed_name(dp, name, namelen)" in remove
assert remove.index("xfs_dentry_cache_note_removed_name") < remove.index("xfs_dentry_cache_store")

find_data = function_body(DIR2, "auto dir2_leaf_node_find_data_entry(")
assert "expected_ino == NULLFSINO || dep->inumber.to_cpu() == expected_ino" in find_data

leaf_remove = function_body(DIR2, "auto dir2_leaf_node_removename(")
assert "bool const INDEXED_ENTRY = rc == 0" in leaf_remove
assert "rc != 0 && rc != -ENOENT" in leaf_remove
assert "if (INDEXED_ENTRY)" in leaf_remove
assert "if (leaf_bh != nullptr)" in leaf_remove
assert "dir2_make_data_free" in leaf_remove

for caller in ("xfs_unlink_path", "xfs_rmdir_path", "xfs_rename_path"):
    assert "xfs_dir_removename(" in function_body(VFS, f"auto {caller}(")

single = function_body(VFS, "auto readdir_callback(")
batch = function_body(VFS, "auto readdir_batch_callback(")
for body in (single, batch):
    assert "readdir_entry_visibility" in body
    assert "VISIBILITY == 0" in body

print("XFS readdir index-authority source invariants passed")
