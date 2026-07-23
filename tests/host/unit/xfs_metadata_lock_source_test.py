#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XFS_MOUNT_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_mount.hpp"
XFS_INODE_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_inode.hpp"
XFS_INODE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_inode.cpp"
XFS_DIR2_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_dir2.cpp"
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
    guard_body = source[source.find("class XfsMetadataGuard") : source.find("auto perf_xfs_started_us")]
    if not guard_body:
        fail("xfs_vfs.cpp must define an RAII metadata guard")
    require_order(
        guard_body,
        [
            "explicit XfsMetadataGuard(XfsMountContext* ctx, bool active = true, uint64_t callsite = 0)",
            "acquire();",
            "~XfsMetadataGuard() { unlock(); }",
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
        "WkiPerfLocalXfsOp::LOG_BLOCKS) + 1",
    ]:
        if token not in perf_source:
            fail(f"local_xfs perf names/bucket sizing must include metadata-lock op: {token}")

    guard_body = xfs_source[xfs_source.find("class XfsMetadataGuard") : xfs_source.find("auto perf_xfs_started_us")]
    if "~XfsMetadataGuard() { unlock(); }" not in guard_body:
        fail("XfsMetadataGuard destructor must use unlock() so RAII lock holds are recorded")
    destructor_body = guard_body[guard_body.find("~XfsMetadataGuard") : guard_body.find("XfsMetadataGuard(const XfsMetadataGuard&)")]
    if "metadata_lock.unlock()" in destructor_body:
        fail("XfsMetadataGuard destructor must not bypass metadata-lock hold accounting")
    acquire_body = function_body(guard_body, "acquire")
    require_order(
        acquire_body,
        [
            "metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT)",
            "if (!try_adaptive_lock())",
            "ctx->metadata_lock.lock();",
            "locked = true;",
            "metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT, WAIT_STARTED_US, callsite);",
            "hold_started_us = metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD);",
        ],
        "XfsMetadataGuard must record metadata-lock wait before starting hold accounting",
    )
    require_order(
        function_body(guard_body, "unlock"),
        [
            "metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD, hold_started_us, callsite);",
            "ctx->metadata_lock.unlock();",
        ],
        "XfsMetadataGuard must record hold time before releasing the metadata lock",
    )

    for token in [
        '"metadata_lock_wait"',
        '"metadata_lock_hold"',
    ]:
        if token not in perf_wki:
            fail(f"perf checkout-report focus counters must include metadata-lock op: {token}")


def test_inode_io_lock_precedes_metadata_lock() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = [
        ("xfs_vfs_close", "MutexGuard guard(xfd->inode->io_lock)", "XfsMetadataGuard metadata_guard(xfd->mount"),
        ("xfs_vfs_truncate", "MutexGuard guard(ip->io_lock)", "XfsMetadataGuard metadata_guard(ctx"),
        ("xfs_fsync", "MutexGuard guard(xfd->inode->io_lock)", "XfsMetadataGuard metadata_guard(xfd->mount"),
    ]
    for name, inode_lock, metadata_lock in checks:
        require_order(function_body(source, name), [inode_lock, metadata_lock], f"{name} inode/metadata lock order")

    for name in ["xfs_vfs_write", "xfs_write_append"]:
        require_order(
            function_body(source, name),
            ["MutexGuard guard(xfd->inode->io_lock)", "xfs_vfs_write_locked("],
            f"{name} must enter the shared writer with the inode I/O lock held",
        )


def test_metadata_mutators_are_serialized() -> None:
    source = XFS_VFS_CPP.read_text()
    checks = {
        "xfs_chmod_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_fchmod": "XfsMetadataGuard metadata_guard(xfd->mount",
        "xfs_mkdir_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_symlink_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_open_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_rmdir_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_rename_path": "XfsMetadataGuard metadata_guard(ctx",
        "xfs_unlink_path": "XfsMetadataGuard metadata_guard(ctx",
    }
    for name, token in checks.items():
        if token not in function_body(source, name):
            fail(f"{name} must serialize XFS metadata mutations")


def test_sync_flush_is_serialized_after_inode_writeback() -> None:
    source = XFS_VFS_CPP.read_text()
    require_order(
        function_body(source, "xfs_sync_mount"),
        [
            "xfs_icache_sync_dirty(ctx);",
            "XfsMetadataGuard metadata_guard(ctx",
            "xfs_log_flush(ctx);",
            "sync_blockdev(ctx->device);",
        ],
        "sync must serialize the WAL and block-device flush after inode writeback",
    )


def test_unlink_retries_false_enoent_without_namespace_caches() -> None:
    dir_source = XFS_DIR2_CPP.read_text()
    vfs_source = XFS_VFS_CPP.read_text()
    authoritative_body = function_body(dir_source, "xfs_dir_lookup_authoritative")
    if "xfs_dir_lookup_impl(dp, name, namelen, entry, false)" not in authoritative_body:
        fail("authoritative directory lookup must bypass the dentry cache")
    require_order(
        function_body(dir_source, "xfs_selftest_authoritative_lookup_repairs_stale_negative"),
        [
            'xfs_dentry_cache_store(&dir, "foo", 3, -ENOENT, nullptr)',
            'xfs_dir_lookup(&dir, "foo", 3, &entry) == -ENOENT',
            'xfs_dir_lookup_authoritative(&dir, "foo", 3, &entry) == 0',
            'xfs_dir_lookup(&dir, "foo", 3, &entry) == 0',
        ],
        "authoritative lookup selftest must reproduce and repair a stale negative dentry",
    )

    require_order(
        function_body(vfs_source, "xfs_unlink_path"),
        [
            "xfs_find_parent_and_name(fs_path",
            "if (rc == -ENOENT)",
            "nullptr, false, known_fs_path_len",
            "xfs_dir_lookup_authoritative(parent_ip, filename, filename_len, &de)",
            "if (rc == -ENOENT && !authoritative_lookup)",
            "xfs_inode_release(parent_ip);",
            "nullptr, false, known_fs_path_len",
            "xfs_dir_lookup_authoritative(parent_ip, filename, filename_len, &de)",
            "xfs_trans_alloc(ctx)",
        ],
        "unlink must retry cache-derived ENOENT authoritatively before starting a transaction",
    )

    walk_body = function_body(vfs_source, "walk_path")
    if "xfs_dir_lookup_authoritative(ip, comp_start, namelen, &de)" not in walk_body:
        fail("uncached parent walks must bypass component dentry-cache answers")


def test_xfs_namespace_cache_publication_is_ordered() -> None:
    source = VFS_CORE_CPP.read_text()
    for token in [
        "ker::mod::sys::Mutex g_xfs_namespace_publication_mutex;",
        "class XfsNamespacePublicationGuard",
        "g_xfs_namespace_publication_mutex.lock();",
        "g_xfs_namespace_publication_mutex.unlock();",
    ]:
        if token not in source:
            fail(f"VFS must define the XFS namespace publication guard: missing {token}")

    for name in [
        "vfs_open_resolved_for_task",
        "vfs_open_file_impl",
        "vfs_symlink_resolved_linkpath",
        "vfs_mkdir_resolved_path",
        "vfs_unlink_resolved_path",
        "vfs_rmdir_resolved_path",
        "vfs_rename_resolved_paths",
        "vfs_link_resolved_paths",
    ]:
        if "XfsNamespacePublicationGuard namespace_publication_guard" not in function_body(source, name):
            fail(f"{name} must order the XFS mutation with its VFS cache publication")

    require_order(
        function_body(source, "vfs_unlink_resolved_path"),
        [
            "XfsNamespacePublicationGuard namespace_publication_guard(mount);",
            "xfs_unlink_path(",
            "vfs_cache_notify_path_changed(resolved_path, nullptr);",
            "metadata_cache_store_missing_path_on_current_mount(",
        ],
        "XFS unlink and its negative cache publication must remain ordered",
    )


def test_reused_directory_inode_starts_with_fresh_dentry_generation() -> None:
    require_order(
        function_body(XFS_VFS_CPP.read_text(), "xfs_mkdir_path"),
        [
            "rc = xfs_trans_commit(tp);",
            "if (rc != 0)",
            "xfs_dentry_cache_invalidate_dir(new_inode);",
            "xfs_inode_cache_new(new_inode)",
        ],
        "a committed mkdir must discard dentry-cache state from an older inode incarnation",
    )


def test_known_absent_create_hint_requires_xfs_proof() -> None:
    core_source = VFS_CORE_CPP.read_text()
    open_body = function_body(XFS_VFS_CPP.read_text(), "xfs_open_path")
    require_order(
        function_body(core_source, "vfs_apply_xfs_known_absent_hint"),
        [
            "backend_flags &= ~ker::vfs::O_WOS_KNOWN_ABSENT;",
            "existence_cache_lookup_negative_mount(",
            "backend_flags |= ker::vfs::O_WOS_KNOWN_ABSENT;",
        ],
        "VFS must issue the private create hint only from a current negative existence observation",
    )
    require_order(
        open_body,
        [
            "bool const TRUSTED_KNOWN_ABSENT_CREATE",
            "bool const EXCLUSIVE_CREATE",
            "if (!EXCLUSIVE_CREATE)",
            "xfs_dir_name_filter_known_absent(",
            "else if (TRUSTED_KNOWN_ABSENT_CREATE)",
            "xfs_dentry_cache_lookup_parent(",
            "if (cached_lookup_ret == -ENOENT)",
            "if (!create_lookup_handled)",
            "xfs_dir_lookup(create_parent_ip, create_filename, create_filename_len, &existing)",
            "create_missing = true;",
        ],
        "XFS must bypass absence shortcuts and revalidate O_EXCL under its metadata lock",
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
            "XfsMetadataGuard metadata_guard(ctx",
            "fill_stat(ip, &opened_stat);",
            "metadata_guard.unlock();",
            "xfs_file_from_inode(",
        ],
        "xfs_open_path must snapshot inode state before releasing metadata serialization",
    )
    require_order(
        function_body(xfs_source, "xfs_file_from_inode"),
        ["ker::vfs::vfs_prefill_file_stat_snapshot(f, opened_stat);", "return f;"],
        "XFS File construction must publish the open-time stat snapshot",
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
        "file_stat_snapshot_path_current(file, PATH_LEN, CACHE_GENERATION)",
        "file->stat_cache_path_len = PATH_LEN;",
        "metadata_cache_note_observation_store();",
        "g_vfs_fstat_snapshot_stores.fetch_add(1, std::memory_order_relaxed);",
    ]:
        if token not in promote_body:
            fail(f"prefilled stat promotion must preserve invalidation invariants: {token}")


def test_writer_serializes_metadata_but_releases_it_for_data_io() -> None:
    source = XFS_VFS_CPP.read_text()
    writer = function_body(source, "xfs_vfs_write_locked")
    require_order(
        writer,
        [
            "try_mapped_write_without_metadata_lock();",
            "XfsMetadataGuard metadata_guard(ctx",
            "ret = xfs_trans_commit(tp);",
            "metadata_guard.unlock();",
            "bool const WROTE = write_extent_data(DISK_BLOCK +",
            "if (!WROTE)",
            "relock_if_more_metadata_needed();",
            "metadata_guard.unlock();",
            "bool const WROTE = write_extent_data(DISK_BLOCK, BLOCK_OFF, CHUNK, total_written, APPEND_BLOCK_HAS_NO_OLD_BYTES);",
        ],
        "write allocation metadata must commit while serialized and data copies must run after releasing the mount lock",
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
    test_inode_io_lock_precedes_metadata_lock()
    test_metadata_mutators_are_serialized()
    test_sync_flush_is_serialized_after_inode_writeback()
    test_xfs_namespace_cache_publication_is_ordered()
    test_reused_directory_inode_starts_with_fresh_dentry_generation()
    test_known_absent_create_hint_requires_xfs_proof()
    test_xfs_open_seeds_fstat_snapshot_without_second_metadata_lock()
    test_writer_serializes_metadata_but_releases_it_for_data_io()
    test_inode_inactivation_is_serialized_by_metadata_lock()
    test_zero_link_vfs_releases_use_metadata_locked_api()
    print("XFS metadata lock source invariants hold")


if __name__ == "__main__":
    main()
