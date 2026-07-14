#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MOUNT_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.hpp"
MOUNT_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.cpp"
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
REMOTE_VFS_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:static\s+)?(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_mount_lookup_returns_raii_refs() -> None:
    header = MOUNT_HPP.read_text()
    required = [
        "class MountRef",
        "std::atomic<uint32_t> refs{0}",
        "std::atomic<bool> retiring{false}",
        "auto find_mount_point(const char* path, size_t known_path_len = UNKNOWN_MOUNT_PATH_LEN) -> MountRef;",
        "auto get_mount_at(size_t index) -> MountRef;",
        "void put_mount_point(MountPoint* mount);",
    ]
    missing = [token for token in required if token not in header]
    if missing:
        fail("mount lifetime API is missing retained-ref tokens: " + ", ".join(missing))


def test_unmount_retires_removes_then_waits_before_destroy() -> None:
    source = MOUNT_CPP.read_text()
    body = function_body(source, "unmount_filesystem_impl")
    required = [
        "mp->retiring.store(true, std::memory_order_release);",
        "mounts.remove_at(i);",
        "mount_lock.unlock();",
        "wait_for_mount_refs_to_drain(removed_mount);",
        "destroy_mount(removed_mount);",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("unmount_filesystem() is missing retained teardown tokens: " + ", ".join(missing))
    require_order(body, "mp->retiring.store", "mounts.remove_at(i);", "mount retire/remove")
    remove_pos = body.find("mounts.remove_at(i);")
    unlock_after_remove = body.find("mount_lock.unlock();", remove_pos)
    wait_pos = body.find("wait_for_mount_refs_to_drain(removed_mount);")
    if remove_pos < 0 or unlock_after_remove < remove_pos or wait_pos < unlock_after_remove:
        fail("unmount must remove under mount_lock, unlock, then drain retained refs")
    require_order(body, "wait_for_mount_refs_to_drain(removed_mount);", "destroy_mount(removed_mount);", "wait before free")

    conditional_body = function_body(source, "unmount_filesystem_if_private_data")
    if "unmount_filesystem_impl(path, expected_private_data, true)" not in conditional_body:
        fail("conditional unmount must require an exact private_data owner match")
    if "mp->private_data == expected_private_data" not in body:
        fail("unmount implementation must compare the mount owner under mount_lock")

    owner_body = function_body(source, "unmount_filesystem_by_private_data")
    owner_required = [
        "MP->private_data != expected_private_data",
        "MP->retiring.store(true, std::memory_order_release)",
        "mounts.remove_at(i)",
        "mount_lock.unlock()",
        "wait_for_mount_refs_to_drain(owned_mount)",
        "destroy_mount(owned_mount)",
    ]
    missing = [token for token in owner_required if token not in owner_body]
    if missing:
        fail("owner-identity unmount is missing stable teardown tokens: " + ", ".join(missing))
    require_order(owner_body, "MP->private_data != expected_private_data", "mounts.remove_at(i)", "owner match before removal")
    require_order(owner_body, "mount_lock.unlock()", "wait_for_mount_refs_to_drain(owned_mount)", "owner wait outside mount lock")


def test_remote_mount_is_configured_atomically() -> None:
    mount_body = function_body(MOUNT_CPP.read_text(), "mount_filesystem")
    required = [
        "mount->private_data = initial_private_data",
        "mount->fops = initial_fops",
        "mount->fs_type == FSType::REMOTE",
        "std::strcmp(existing->path, mount->path) == 0",
        "mounts.push_back(mount)",
    ]
    missing = [token for token in required if token not in mount_body]
    if missing:
        fail("remote mount publication is missing atomic owner/path tokens: " + ", ".join(missing))
    require_order(mount_body, "mount->private_data = initial_private_data", "mounts.push_back(mount)", "owner before publication")
    require_order(mount_body, "std::strcmp(existing->path, mount->path) == 0", "mounts.push_back(mount)", "duplicate rejection")

    remote_mount = function_body(REMOTE_VFS_CPP.read_text(), "wki_remote_vfs_mount")
    if 'mount_filesystem(local_mount_path, "remote", nullptr, 0, nullptr, state, &g_remote_vfs_fops)' not in remote_mount:
        fail("remote VFS must publish its state/fops in the mount-table insertion transaction")
    if "configure_mount_point_exact" in remote_mount:
        fail("remote VFS must not configure a same-path mount in a second lookup")


def test_writable_block_mount_holds_lease_until_destroy() -> None:
    header = MOUNT_HPP.read_text()
    source = MOUNT_CPP.read_text()
    mount_body = function_body(source, "mount_filesystem")
    destroy_body = function_body(source, "destroy_mount")

    if "ker::dev::BlockWriterLease block_writer_lease{};" not in header:
        fail("MountPoint must own the local block-writer lease")
    required = [
        "bool const BLOCK_RW_FS",
        "mount->block_writer_lease.try_acquire(device, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT)",
        "destroy_mount(mount)",
        "mounts.push_back(mount)",
    ]
    missing = [token for token in required if token not in mount_body]
    if missing:
        fail("writable block mount lease lifecycle is missing: " + ", ".join(missing))
    require_order(mount_body, "mount->block_writer_lease.try_acquire", "fat32_init_device", "lease before FAT initialization")
    require_order(mount_body, "mount->block_writer_lease.try_acquire", "xfs_vfs_init_device", "lease before XFS initialization")
    require_order(mount_body, "mount->block_writer_lease.try_acquire", "mounts.push_back(mount)", "lease before mount publication")
    if "delete mount;" not in destroy_body:
        fail("destroy_mount() must destroy the MountPoint and its writer lease")
    if "block_writer_lease.release" in source:
        fail("local writer lease must release only through MountPoint destruction after ref drain")


def test_pivot_rewrites_are_transactional_and_gate_publication() -> None:
    source = MOUNT_CPP.read_text()
    mount_body = function_body(source, "mount_filesystem")
    wait_body = function_body(source, "wait_for_stable_mount_pivot_epoch")
    remap_body = function_body(source, "remap_mounts_for_pivot")
    rebase_body = function_body(source, "rebase_wki_mounts_for_new_root")
    required_source = [
        "std::atomic<uint64_t> mount_pivot_epoch{2}",
        "wait_for_stable_mount_pivot_epoch()",
        "release_pivot_replacements",
    ]
    missing = [token for token in required_source if token not in source]
    if missing:
        fail("pivot mount publication gate is missing: " + ", ".join(missing))
    wait_required = [
        "ker::mod::sched::can_query_current_task()",
        "ker::mod::sched::preemptible()",
        "ker::mod::sched::interrupts_enabled()",
        'asm volatile("pause" ::: "memory")',
    ]
    missing = [token for token in wait_required if token not in wait_body]
    if missing:
        fail("odd pivot epoch wait is unsafe outside yieldable task context: " + ", ".join(missing))
    if "mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH" not in mount_body:
        fail("mount_filesystem() must reject a path resolved in an older pivot epoch")
    if remap_body.count("mount_has_active_refs_locked") < 2 or "return -EBUSY;" not in remap_body:
        fail("remap_mounts_for_pivot() must fail instead of rewriting paths with active refs")
    required_remap = [
        "mount_pivot_epoch.store(ODD_EPOCH, std::memory_order_release)",
        "ker::util::SmallVec<PivotMountReplacement, 8> replacements",
        "mount_generation.load(std::memory_order_acquire) != TABLE_GENERATION",
        "replacements.push_back(REPLACEMENT)",
        "std::strcmp(replacements.at(i).path, replacements.at(j).path) == 0",
        "std::strcmp(replacements.at(i).path, existing->path) == 0",
        "delete[] mp->path",
        "mp->path = replacement.path",
    ]
    missing = [token for token in required_remap if token not in remap_body]
    if missing:
        fail("pivot remap transaction is missing: " + ", ".join(missing))
    require_order(remap_body, "ker::util::SmallVec<PivotMountReplacement, 8> replacements", "bump_mount_generation_locked()", "allocate before commit")
    require_order(remap_body, "mount_has_active_refs_locked(replacement.mount)", "delete[] mp->path", "final ref check before commit")
    collision_pos = remap_body.find("bool target_collision = false;")
    final_lock_pos = remap_body.find("mount_lock.lock();", collision_pos)
    if collision_pos < 0 or final_lock_pos < collision_pos:
        fail("pivot collision analysis must complete before taking the final commit lock")
    if "mount_lock.lock();" in remap_body[collision_pos:final_lock_pos]:
        fail("pivot collision analysis must not hold mount_lock")
    if "mount_pivot_epoch.store(ODD_EPOCH + 1, std::memory_order_release)" not in rebase_body:
        fail("final WKI rebase must reopen mount publication after task roots change")


def test_mount_table_readers_gate_on_stable_pivot_epoch() -> None:
    source = MOUNT_CPP.read_text()
    readers = [
        "mounted_block_device_overlaps",
        "find_mount_point",
        "mount_table_generation_snapshot",
        "configure_mount_point_exact",
        "get_mount_count",
        "get_mount_at",
        "get_mount_snapshot_at",
    ]
    for name in readers:
        body = function_body(source, name)
        required = [
            "wait_for_stable_mount_pivot_epoch()",
            "mount_pivot_epoch.load(std::memory_order_acquire)",
        ]
        missing = [token for token in required if token not in body]
        if missing:
            fail(f"{name} does not gate mount-table reads on a stable pivot epoch: " + ", ".join(missing))

    for name in ("mount_lookup_cache_get_retained", "mount_root_fallback_cache_get_retained"):
        body = function_body(source, name)
        if "mount_pivot_epoch.load(std::memory_order_acquire) == pivot_epoch" not in body:
            fail(f"{name} can retain a cached mount after pivot entered its odd epoch")


def test_pivot_prepares_wki_before_mount_remap() -> None:
    body = function_body(CORE_CPP.read_text(), "vfs_pivot_root")
    required = [
        "snapshot_bounded_path_string(new_root",
        "snapshot_bounded_path_string(put_old",
        "wki_remote_vfs_prepare_export_rebuild()",
        "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data())",
        "wki_remote_vfs_cancel_export_rebuild()",
        "rebase_wki_mounts_for_new_root(stable_new_root.data())",
        "wki_remote_vfs_rebuild_exports()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("pivot/WKI split-phase barrier is missing: " + ", ".join(missing))
    require_order(body, "snapshot_bounded_path_string(put_old", "wki_remote_vfs_prepare_export_rebuild()", "snapshot paths before prepare")
    require_order(body, "wki_remote_vfs_prepare_export_rebuild()", "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data())", "prepare before remap")
    require_order(body, "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data())", "rebase_wki_mounts_for_new_root(stable_new_root.data())", "remap before final rebase")
    require_order(body, "rebase_wki_mounts_for_new_root(stable_new_root.data())", "wki_remote_vfs_rebuild_exports()", "rebase before export rebuild")

    snapshot_body = function_body(CORE_CPP.read_text(), "snapshot_bounded_path_string")
    if "if (pos == 0)" not in snapshot_body or "return -EINVAL;" not in snapshot_body:
        fail("bounded pivot path snapshots must reject empty paths with EINVAL")


def test_vfs_umount_invalidates_only_exact_mount() -> None:
    body = function_body(CORE_CPP.read_text(), "vfs_umount")
    required = [
        "auto mount_ref = find_mount_point(resolved.data());",
        "std::strcmp(mount->path, resolved.data()) == 0",
        "stream_invalidate_mount_scope(mount->fs_type, stream_scope_key_for_mount(mount));",
        "return unmount_filesystem(target);",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("vfs_umount() must invalidate stream scope only for exact mount targets: " + ", ".join(missing))
    require_order(body, "std::strcmp(mount->path, resolved.data()) == 0", "stream_invalidate_mount_scope", "exact umount invalidation")


def test_vfs_mount_resolves_dev_source_symlinks() -> None:
    source = CORE_CPP.read_text()
    helper_body = function_body(source, "resolve_mount_source_path")
    helper_required = [
        "resolve_task_path_raw(source, abs_source.data(), abs_source.size())",
        "resolve_symlinks(abs_source.data(), resolved_source.data(), resolved_source.size(), true)",
        "strip_current_task_root_prefix(resolved_source.data(), out, outsize)",
    ]
    missing = [token for token in helper_required if token not in helper_body]
    if missing:
        fail("resolve_mount_source_path() must use normal VFS symlink resolution: " + ", ".join(missing))

    body = function_body(source, "vfs_mount")
    required = [
        "std::array<char, MAX_PATH_LEN> resolved_source{};",
        "int const SOURCE_RET = resolve_mount_source_path(source, resolved_source.data(), resolved_source.size());",
        "const char* block_source = resolved_source.data();",
        "ker::dev::block_device_find_by_name(block_source + 5)",
        "ker::vfs::devfs::devfs_resolve_block_device(block_source + 5)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("vfs_mount() must resolve /dev source symlinks before block lookup: " + ", ".join(missing))
    require_order(body, "wki://<hostname>/<export>", "resolve_mount_source_path", "WKI URI mount before dev source resolution")
    require_order(body, "if (IS_PARTUUID)", "resolve_mount_source_path", "PARTUUID before dev source resolution")
    require_order(body, "resolve_mount_source_path", "block_device_find_by_name(block_source + 5)", "source symlink before name lookup")
    require_order(body, "block_device_find_by_name(block_source + 5)", "devfs_resolve_block_device(block_source + 5)", "direct lookup before devfs walk")


def test_pipe_wake_preserves_prepark_wake_token() -> None:
    body = function_body(CORE_CPP.read_text(), "pipe_reschedule_waiters")
    if "EventWakeDeferredSwitch::CANCEL" in body:
        fail("pipe readiness wakes must preserve wakeup_pending; CANCEL can erase a wake that races before preemptible_syscall_park")
    if "ker::mod::sched::wake_task_from_event(waiter);" not in body:
        fail("pipe readiness wakes must use the default event wake path")


def test_no_raw_mount_lookup_assignments_outside_owner() -> None:
    raw_lookup = re.compile(
        r"(?:MountPoint(?:\s+const)?\s*\*|auto\s*\*)\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*"
        r"(?:ker::vfs::)?(?:find_mount_point|get_mount_at)\s*\("
    )
    offenders = []
    for path in (CORE_CPP, REMOTE_VFS_CPP, PROCFS_CPP):
        source = path.read_text()
        for match in raw_lookup.finditer(source):
            line = source.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(ROOT)}:{line}: {match.group(0)}")
    if offenders:
        fail("raw mount lookup assignments bypass MountRef lifetime: " + "; ".join(offenders))


def test_iteration_users_use_snapshots_when_possible() -> None:
    remote = function_body(REMOTE_VFS_CPP.read_text(), "wki_remote_vfs_auto_discover_internal")
    procfs = function_body(PROCFS_CPP.read_text(), "generate_mounts")
    for name, body in (("remote VFS auto-discovery", remote), ("/proc/mounts", procfs)):
        if "get_mount_snapshot_at" not in body:
            fail(f"{name} must iterate mount snapshots rather than raw mount pointers")
        if "get_mount_at" in body:
            fail(f"{name} must not use get_mount_at() for read-only mount metadata")


def main() -> None:
    test_mount_lookup_returns_raii_refs()
    test_unmount_retires_removes_then_waits_before_destroy()
    test_remote_mount_is_configured_atomically()
    test_writable_block_mount_holds_lease_until_destroy()
    test_pivot_rewrites_are_transactional_and_gate_publication()
    test_mount_table_readers_gate_on_stable_pivot_epoch()
    test_pivot_prepares_wki_before_mount_remap()
    test_vfs_umount_invalidates_only_exact_mount()
    test_vfs_mount_resolves_dev_source_symlinks()
    test_pipe_wake_preserves_prepark_wake_token()
    test_no_raw_mount_lookup_assignments_outside_owner()
    test_iteration_users_use_snapshots_when_possible()
    print("VFS mount lifetime source invariants hold")


if __name__ == "__main__":
    main()
