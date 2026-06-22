#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_MOUNT_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_mount.hpp"
XFS_VFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_vfs.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_mount_context_has_sleeping_metadata_mutex() -> None:
    header = XFS_MOUNT_HPP.read_text()
    for token in [
        "#include <platform/sys/mutex.hpp>",
        "mod::sys::Mutex metadata_lock;",
        "metadata transactions and AG btree/counter mutations",
    ]:
        if token not in header:
            fail(f"xfs_mount.hpp missing metadata lock invariant {token!r}")


def test_metadata_guard_raii_locks_mount_mutex() -> None:
    source = XFS_VFS_CPP.read_text()
    if "class XfsMetadataGuard" not in source:
        fail("xfs_vfs.cpp must define an RAII metadata guard")
    require_order(
        source,
        [
            "explicit XfsMetadataGuard(XfsMountContext* ctx, bool active = true)",
            "this->ctx->metadata_lock.lock();",
            "~XfsMetadataGuard()",
            "ctx->metadata_lock.unlock();",
        ],
        "XfsMetadataGuard lock/unlock",
    )


def test_metadata_lock_precedes_inode_locks() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = [
        ("xfs_vfs_close", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_vfs_write", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_vfs_truncate", "XfsMetadataGuard metadata_guard(ctx);", "MutexGuard guard(ip->io_lock)"),
        ("xfs_write_append", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_fsync", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_open_path", "XfsMetadataGuard metadata_guard(ctx, MUTATING_OPEN);", "MutexGuard guard(ip->io_lock)"),
    ]
    for name, guard, inode_lock in checks:
        require_order(function_body(source, name), [guard, inode_lock], f"{name} metadata/inode lock order")


def test_metadata_mutators_are_serialized() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = {
        "xfs_sync_mount": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_chmod_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_fchmod": "XfsMetadataGuard metadata_guard(xfd->mount);",
        "xfs_mkdir_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_rmdir_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_rename_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_unlink_path": "XfsMetadataGuard metadata_guard(ctx);",
    }
    for name, token in checks.items():
        if token not in function_body(source, name):
            fail(f"{name} must serialize XFS metadata mutations")


def test_allocator_helpers_do_not_take_mount_guard_directly() -> None:
    source = XFS_VFS_CPP.read_text()
    locked_writer = function_body(source, "xfs_vfs_write_locked")
    if "XfsMetadataGuard" in locked_writer:
        fail("xfs_vfs_write_locked is shared by already-guarded callers and must not re-lock metadata")


def main() -> None:
    test_mount_context_has_sleeping_metadata_mutex()
    test_metadata_guard_raii_locks_mount_mutex()
    test_metadata_lock_precedes_inode_locks()
    test_metadata_mutators_are_serialized()
    test_allocator_helpers_do_not_take_mount_guard_directly()
    print("XFS metadata lock source invariants hold")


if __name__ == "__main__":
    main()
