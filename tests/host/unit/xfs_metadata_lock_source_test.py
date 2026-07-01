#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_MOUNT_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_mount.hpp"
XFS_INODE_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_inode.hpp"
XFS_INODE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_inode.cpp"
XFS_VFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_vfs.cpp"
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
VFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "vfs.hpp"
PERF_EVENTS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "perf" / "perf_events.hpp"
PERF_EVENTS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "perf" / "perf_events.cpp"
PERF_WKI_CPP = ROOT / "modules" / "perf" / "src" / "wki.cpp"


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


def test_metadata_guard_records_wait_and_hold_time() -> None:
    xfs_source = XFS_VFS_CPP.read_text()
    perf_header = PERF_EVENTS_HPP.read_text()
    perf_source = PERF_EVENTS_CPP.read_text()
    perf_wki = PERF_WKI_CPP.read_text()

    for token in [
        "METADATA_LOCK_WAIT = 33",
        "METADATA_LOCK_HOLD = 34",
    ]:
        if token not in perf_header:
            fail(f"local_xfs perf enum must include metadata-lock op: {token}")

    for token in [
        'return "metadata_lock_wait";',
        'return "metadata_lock_hold";',
        "WkiPerfLocalXfsOp::METADATA_LOCK_HOLD) + 1",
    ]:
        if token not in perf_source:
            fail(f"local_xfs perf names/bucket sizing must include metadata-lock op: {token}")

    guard_body = xfs_source[xfs_source.find("class XfsMetadataGuard") : xfs_source.find("auto perf_xfs_started_us")]
    if "~XfsMetadataGuard() { unlock(); }" not in guard_body:
        fail("XfsMetadataGuard destructor must use unlock() so RAII lock holds are recorded")
    destructor_body = guard_body[guard_body.find("~XfsMetadataGuard") : guard_body.find("XfsMetadataGuard(const XfsMetadataGuard&)")]
    if "metadata_lock.unlock()" in destructor_body:
        fail("XfsMetadataGuard destructor must not bypass metadata-lock hold accounting")
    require_order(
        guard_body,
        [
            "metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT)",
            "this->ctx->metadata_lock.lock();",
            "metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT, WAIT_STARTED_US);",
            "hold_started_us = metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD);",
            "metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD, hold_started_us);",
            "ctx->metadata_lock.unlock();",
        ],
        "XfsMetadataGuard must record metadata-lock wait and hold time without changing lock order",
    )

    for token in [
        '"metadata_lock_wait"',
        '"metadata_lock_hold"',
    ]:
        if token not in perf_wki:
            fail(f"perf checkout-report focus counters must include metadata-lock op: {token}")


def test_metadata_lock_precedes_inode_locks() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = [
        ("xfs_vfs_close", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_vfs_write", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_vfs_truncate", "XfsMetadataGuard metadata_guard(ctx);", "MutexGuard guard(ip->io_lock)"),
        ("xfs_write_append", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
        ("xfs_fsync", "XfsMetadataGuard metadata_guard(xfd->mount);", "MutexGuard guard(xfd->inode->io_lock)"),
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
        "xfs_symlink_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_open_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_rmdir_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_rename_path": "XfsMetadataGuard metadata_guard(ctx);",
        "xfs_unlink_path": "XfsMetadataGuard metadata_guard(ctx);",
    }
    for name, token in checks.items():
        if token not in function_body(source, name):
            fail(f"{name} must serialize XFS metadata mutations")


def test_open_retries_stale_parent_path_cache_misses() -> None:
    source = XFS_VFS_CPP.read_text()
    find_parent_body = function_body(source, "xfs_find_parent_and_name")
    lookup_body = function_body(source, "xfs_lookup_with_cached_parent")
    open_body = function_body(source, "xfs_open_path")

    require_order(
        source,
        [
            "uint16_t* namelen_out, bool* cache_hit_out = nullptr, bool allow_parent_cache = true",
            "*cache_hit_out = false;",
            "allow_parent_cache && xfs_parent_path_cache_lookup_ino",
        ],
        "parent path cache helper must report whether the result came from cache",
    )
    require_order(
        find_parent_body,
        [
            "parent_ip = xfs_inode_read(ctx, cached_parent_ino);",
            "!xfs_inode_isdir(parent_ip) || parent_ip->nlink == 0",
            "parent_ip = nullptr;",
            "*cache_hit_out = true;",
        ],
        "parent path cache helper must reject stale non-directory or unlinked parents",
    )
    require_order(
        lookup_body,
        [
            "bool parent_cache_hit = false;",
            "&parent_cache_hit",
            "if (LOOKUP_RET == -ENOENT && parent_cache_hit)",
            "return -EAGAIN;",
        ],
        "non-create open must fall back to full path walk after a cached parent miss",
    )
    require_order(
        open_body,
        [
            "bool create_parent_cache_hit = false;",
            "&create_parent_cache_hit",
            "if (lookup_ret == -ENOENT && create_parent_cache_hit)",
            "xfs_inode_release(create_parent_ip);",
            "nullptr, false",
            "lookup_ret = xfs_dir_lookup(create_parent_ip, create_filename, create_filename_len, &existing);",
            "create_missing = true;",
        ],
        "O_CREAT must re-resolve a cached parent miss before creating a new entry",
    )


def test_xfs_open_seeds_fstat_snapshot_without_second_metadata_lock() -> None:
    xfs_source = XFS_VFS_CPP.read_text()
    vfs_source = VFS_CORE_CPP.read_text()
    vfs_header = VFS_HPP.read_text()

    if "void vfs_prefill_file_stat_snapshot(File* file, const Stat& statbuf);" not in vfs_header:
        fail("vfs.hpp must expose the backend stat snapshot prefill helper")

    open_body = function_body(xfs_source, "xfs_open_path")
    require_order(
        open_body,
        [
            "XfsMetadataGuard metadata_guard(ctx);",
            "fill_stat(ip, &opened_stat);",
            "ker::vfs::vfs_prefill_file_stat_snapshot(f, opened_stat);",
            "return f;",
        ],
        "xfs_open_path must seed the fstat snapshot while the metadata lock is still held",
    )

    refresh_body = function_body(vfs_source, "file_stat_snapshot_refresh")
    require_order(
        refresh_body,
        [
            "if (file_stat_snapshot_current(file))",
            "if (file_stat_snapshot_promote_prefilled_path(file))",
            "file_stat_snapshot_refresh_from_backend(file, &statbuf)",
        ],
        "VFS refresh must promote the open-time snapshot before calling backend fstat",
    )

    promote_body = function_body(vfs_source, "file_stat_snapshot_promote_prefilled_path")
    for token in [
        "file->stat_cache_path_len != 0",
        "metadata_path_invalidated_since(file->vfs_path, PATH_LEN, file->stat_cache_invalidation_generation)",
        "file->stat_cache_path_len = PATH_LEN;",
        "metadata_cache_note_observation_store();",
        "g_vfs_fstat_snapshot_stores.fetch_add(1, std::memory_order_relaxed);",
    ]:
        if token not in promote_body:
            fail(f"prefilled stat promotion must preserve invalidation invariants: {token}")


def test_allocator_helpers_do_not_take_mount_guard_directly() -> None:
    source = XFS_VFS_CPP.read_text()
    locked_writer = function_body(source, "xfs_vfs_write_locked")
    if "XfsMetadataGuard" in locked_writer:
        fail("xfs_vfs_write_locked is shared by already-guarded callers and must not re-lock metadata")


def test_write_does_not_reacquire_metadata_lock_while_holding_inode_lock() -> None:
    source = XFS_VFS_CPP.read_text()
    writer = function_body(source, "xfs_vfs_write_locked")

    if "XfsMetadataUnlockedScope" in source:
        fail("XFS write must not drop and reacquire metadata_lock while inode io_lock is held")
    if "metadata_guard.unlock()" in writer or "metadata_guard.lock()" in writer:
        fail("xfs_vfs_write_locked must keep a single metadata_lock acquisition order")
    require_order(
        writer,
        [
            "bool const WROTE = write_extent_data(DISK_BLOCK +",
            "if (!WROTE)",
            "bool const WROTE = write_extent_data(DISK_BLOCK, BLOCK_OFF, CHUNK, total_written, APPEND_BLOCK_HAS_NO_OLD_BYTES);",
        ],
        "write I/O must run without metadata-lock drop/reacquire scope",
    )


def test_inode_inactivation_is_serialized_by_metadata_lock() -> None:
    header = XFS_INODE_HPP.read_text()
    source = XFS_INODE_CPP.read_text()
    if "void xfs_inode_release_metadata_locked(XfsInode* ip);" not in header:
        fail("xfs_inode.hpp must expose a release API for callers already holding metadata_lock")

    release_impl = function_body(source, "release_inode_reference")
    require_order(
        release_impl,
        [
            "bool const NEEDS_INACTIVATION = (ip->nlink == 0 && !ip->inactivation_started);",
            "if (!metadata_locked && ip->mount != nullptr)",
            "ip->mount->metadata_lock.lock();",
            "ip->io_lock.lock();",
            "inactivation_rc = inactivate_unlinked_inode(ip);",
            "ip->io_lock.unlock();",
            "if (!metadata_locked && ip->mount != nullptr)",
            "ip->mount->metadata_lock.unlock();",
        ],
        "zero-link inode inactivation metadata lock",
    )
    require_order(
        source,
        [
            "void xfs_inode_release(XfsInode* ip) { release_inode_reference(ip, false); }",
            "void xfs_inode_release_metadata_locked(XfsInode* ip) { release_inode_reference(ip, true); }",
        ],
        "inode release wrappers",
    )


def test_zero_link_vfs_releases_use_metadata_locked_api() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = {
        "xfs_rmdir_path": ["dir_ip->nlink = 0;", "xfs_inode_release_metadata_locked(dir_ip);"],
        "xfs_rename_path": ["displaced->nlink--;", "xfs_inode_release_metadata_locked(displaced);"],
        "xfs_unlink_path": ["target_ip->nlink--;", "xfs_inode_release_metadata_locked(target_ip);"],
    }
    for name, tokens in checks.items():
        require_order(function_body(source, name), tokens, f"{name} zero-link inode release")


def main() -> None:
    test_mount_context_has_sleeping_metadata_mutex()
    test_metadata_guard_raii_locks_mount_mutex()
    test_metadata_guard_records_wait_and_hold_time()
    test_metadata_lock_precedes_inode_locks()
    test_metadata_mutators_are_serialized()
    test_open_retries_stale_parent_path_cache_misses()
    test_xfs_open_seeds_fstat_snapshot_without_second_metadata_lock()
    test_allocator_helpers_do_not_take_mount_guard_directly()
    test_write_does_not_reacquire_metadata_lock_while_holding_inode_lock()
    test_inode_inactivation_is_serialized_by_metadata_lock()
    test_zero_link_vfs_releases_use_metadata_locked_api()
    print("XFS metadata lock source invariants hold")


if __name__ == "__main__":
    main()
