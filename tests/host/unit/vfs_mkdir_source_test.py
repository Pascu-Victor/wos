#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CORE = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
TMPFS = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
XFS = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()


def require(needle: str, source: str, message: str) -> None:
    if needle not in source:
        raise AssertionError(message)


require("return EXISTENCE_CACHED == 0 ? -EEXIST : -EAGAIN;", CORE, "cached mkdir lookup must preserve EEXIST")
require("vfs_stat_resolved_cache_or_impl(abs_path, false, true, false, &created_stat", CORE, "tmpfs mkdir must seed metadata after creation")
require("if (child != nullptr) {\n                    return -EEXIST;", TMPFS, "tmpfs mkdir must reject an existing final component")
require("if (child == nullptr) {\n                return -ENOENT;", TMPFS, "tmpfs mkdir must not create missing parents")
require("if (parent_ret == -ENOTDIR) {\n            return -ENOTDIR;", XFS, "XFS mkdir must preserve a non-directory parent error")

for forbidden in (
    "return IS_DIR ? -EEXIST : -ENOTDIR;",
    "return existing.ftype == XFS_DIR3_FT_DIR ? -EEXIST : -ENOTDIR;",
):
    if forbidden in XFS:
        raise AssertionError(f"XFS mkdir reports ENOTDIR for an existing final component: {forbidden}")

for forbidden in (
    "int const RESULT = (R == -EEXIST) ? 0 : R;",
    "mkdir -p calls mkdir on existing dirs; treat EEXIST as success",
):
    if forbidden in CORE:
        raise AssertionError(f"mkdir still masks EEXIST: {forbidden}")

print("vfs mkdir source tests: PASS")
