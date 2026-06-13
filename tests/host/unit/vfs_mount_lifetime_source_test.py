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
        "auto find_mount_point(const char* path) -> MountRef;",
        "auto get_mount_at(size_t index) -> MountRef;",
        "void put_mount_point(MountPoint* mount);",
    ]
    missing = [token for token in required if token not in header]
    if missing:
        fail("mount lifetime API is missing retained-ref tokens: " + ", ".join(missing))


def test_unmount_retires_removes_then_waits_before_destroy() -> None:
    body = function_body(MOUNT_CPP.read_text(), "unmount_filesystem")
    required = [
        "mp->retiring.store(true, std::memory_order_release);",
        "mounts.remove_at(i);",
        "mount_lock.unlock();",
        "wait_for_mount_refs_to_drain(mp);",
        "destroy_mount(mp);",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("unmount_filesystem() is missing retained teardown tokens: " + ", ".join(missing))
    require_order(body, "mp->retiring.store", "mounts.remove_at(i);", "mount retire/remove")
    require_order(body, "mounts.remove_at(i);", "mount_lock.unlock();", "remove before unlock")
    require_order(body, "mount_lock.unlock();", "wait_for_mount_refs_to_drain(mp);", "wait outside mount_lock")
    require_order(body, "wait_for_mount_refs_to_drain(mp);", "destroy_mount(mp);", "wait before free")


def test_path_rewrites_are_fenced_by_active_refs() -> None:
    source = MOUNT_CPP.read_text()
    replace_body = function_body(source, "replace_mount_path_locked")
    remap_body = function_body(source, "remap_mounts_for_pivot")
    rebase_body = function_body(source, "rebase_wki_mounts_for_new_root")
    if "mount_has_active_refs_locked(mount)" not in replace_body or "return -EBUSY;" not in replace_body:
        fail("replace_mount_path_locked() must reject active refs before freeing mount->path")
    if remap_body.count("mount_has_active_refs_locked") < 2 or "return -EBUSY;" not in remap_body:
        fail("remap_mounts_for_pivot() must fail instead of rewriting paths with active refs")
    if "mount_has_active_refs_locked(mp)" not in rebase_body:
        fail("rebase_wki_mounts_for_new_root() must skip mounts with active refs")


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
    test_path_rewrites_are_fenced_by_active_refs()
    test_vfs_umount_invalidates_only_exact_mount()
    test_vfs_mount_resolves_dev_source_symlinks()
    test_pipe_wake_preserves_prepark_wake_token()
    test_no_raw_mount_lookup_assignments_outside_owner()
    test_iteration_users_use_snapshots_when_possible()
    print("VFS mount lifetime source invariants hold")


if __name__ == "__main__":
    main()
