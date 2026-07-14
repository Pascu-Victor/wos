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


def block_body_after(source: str, header: str) -> str:
    header_pos = source.find(header)
    if header_pos < 0:
        fail(f"missing block header {header!r}")
    body_start = source.find("{", header_pos + len(header))
    if body_start < 0:
        fail(f"missing block body for {header!r}")

    depth = 1
    pos = body_start + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated block for {header!r}")
    return source[body_start + 1 : pos - 1]


def require_sequence(body: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = body.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def require_only_uninitialized_array(body: str, name: str, expected: str, context: str) -> None:
    declarations = re.findall(rf"\bstd::array<[^;\n]+>\s+{re.escape(name)}\b[^;\n]*;", body)
    if declarations != [expected]:
        fail(f"{context}: unexpected {name} declarations: {declarations!r}")
    forbidden = [
        rf"\b{re.escape(name)}\s*=\s*\{{\s*\}}\s*;",
        rf"\b{re.escape(name)}\.fill\s*\(",
        rf"\bmemset\s*\(\s*{re.escape(name)}\.data\s*\(",
    ]
    if any(re.search(pattern, body) for pattern in forbidden):
        fail(f"{context}: {name} must not be cleared after declaration")


def normalized_body(body: str) -> str:
    return " ".join(body.split())


def test_mount_path_scratch_is_fully_produced_before_use() -> None:
    source = MOUNT_CPP.read_text()
    copy_path = function_body(source, "copy_mount_path_string")
    make_absolute = function_body(source, "make_mount_path_absolute")
    canonicalize = function_body(source, "canonicalize_mount_path")
    apply_root = function_body(source, "apply_current_task_root_prefix")
    resolve = function_body(source, "resolve_mount_path")
    mount = function_body(source, "mount_filesystem")
    unmount = function_body(source, "unmount_filesystem_impl")

    components_decl = "std::array<const char*, MAX_MOUNT_COMPONENTS> components __attribute__((uninitialized));"
    result_decl = "std::array<char, MAX_MOUNT_PATH> result __attribute__((uninitialized));"
    logical_decl = "std::array<char, MAX_MOUNT_PATH> logical __attribute__((uninitialized));"
    resolved_decl = "std::array<char, MAX_MOUNT_PATH> resolved __attribute__((uninitialized));"

    require_only_uninitialized_array(canonicalize, "components", components_decl, "canonical component scratch")
    require_only_uninitialized_array(canonicalize, "result", result_decl, "canonical result scratch")
    require_only_uninitialized_array(resolve, "logical", logical_decl, "logical mount scratch")
    require_only_uninitialized_array(mount, "resolved", resolved_decl, "mount resolver output")
    require_only_uninitialized_array(unmount, "resolved", resolved_decl, "unmount resolver output")

    require_sequence(
        copy_path,
        [
            "size_t const PATH_LEN = std::strlen(path);",
            "if (PATH_LEN + 1 > outsize)",
            "std::memcpy(out, path, PATH_LEN + 1);",
            "return 0;",
        ],
        "bounded mount path copy",
    )
    require_sequence(
        make_absolute,
        [
            "size_t const PATH_LEN = std::strlen(path);",
            "if (path[0] == '/')",
            "return copy_mount_path_string(path, out, outsize);",
            "size_t const TOTAL = CWD_LEN + (NEED_SEP ? 1 : 0) + PATH_LEN + 1;",
            "if (TOTAL > outsize)",
            "std::memcpy(out, task->cwd.data(), CWD_LEN);",
            "std::memcpy(out + CWD_LEN + 1, path, PATH_LEN + 1);",
            "std::memcpy(out + CWD_LEN, path, PATH_LEN + 1);",
            "return 0;",
        ],
        "absolute mount path production",
    )
    need_sep_pos = make_absolute.find("if (NEED_SEP)")
    need_sep_else_pos = make_absolute.find("else", need_sep_pos)
    need_sep_body = block_body_after(make_absolute[need_sep_pos:], "if (NEED_SEP)")
    no_sep_body = block_body_after(make_absolute[need_sep_else_pos:], "else")
    if normalized_body(need_sep_body) != "out[CWD_LEN] = '/'; std::memcpy(out + CWD_LEN + 1, path, PATH_LEN + 1);":
        fail("relative mount paths must initialize their separator and NUL-terminated path")
    if normalized_body(no_sep_body) != "std::memcpy(out + CWD_LEN, path, PATH_LEN + 1);":
        fail("root-relative mount paths must initialize their complete NUL-terminated output")
    require_sequence(
        canonicalize,
        [
            components_decl,
            "size_t num_components = 0;",
            "if (num_components >= components.size())",
            "components[num_components++] = comp_start;",
            result_decl,
            "size_t pos = 0;",
            "result[pos++] = '/';",
            "for (size_t i = 0; i < num_components; ++i)",
            "std::strlen(components[i])",
            "std::memcpy(result.data() + pos, components[i], COMP_LEN);",
            "pos += COMP_LEN;",
            "result[pos] = '\\0';",
            "if (pos >= bufsize)",
            "std::memcpy(path, result.data(), pos + 1);",
            "return 0;",
        ],
        "canonical mount path prefix production",
    )
    component_loop = block_body_after(canonicalize, "for (size_t i = 0; i < num_components; ++i)")
    if component_loop.count("components[i]") != 2 or canonicalize.count("components[i]") != 2:
        fail("canonicalization must only read component slots inside the bounded component loop")
    if canonicalize.count("components[num_components++]") != 1:
        fail("canonicalization must admit each component through num_components")
    if canonicalize.count("result.data()") != 2 or canonicalize.count("result[") != 3:
        fail("canonicalization must only write the result prefix and copy its initialized length")

    require_sequence(
        apply_root,
        [
            "size_t const ROOT_LEN = std::strlen(task->root.data());",
            "size_t const PATH_LEN = std::strlen(path);",
            "if (ROOT_LEN + PATH_LEN + 1 > outsize)",
            "std::memmove(out + ROOT_LEN, out, PATH_LEN + 1);",
            "std::memcpy(out + ROOT_LEN, path, PATH_LEN + 1);",
            "std::memcpy(out, task->root.data(), ROOT_LEN);",
            "return 0;",
            "return copy_mount_path_string(path, out, outsize);",
        ],
        "task-root-prefixed mount path production",
    )
    in_place_pos = apply_root.find("if (out == path)")
    distinct_pos = apply_root.find("else", in_place_pos)
    in_place_body = block_body_after(apply_root[in_place_pos:], "if (out == path)")
    distinct_body = block_body_after(apply_root[distinct_pos:], "else")
    if normalized_body(in_place_body) != "std::memmove(out + ROOT_LEN, out, PATH_LEN + 1);":
        fail("in-place task-root prefixing must move the complete NUL-terminated path")
    if normalized_body(distinct_body) != "std::memcpy(out + ROOT_LEN, path, PATH_LEN + 1);":
        fail("distinct task-root prefixing must copy the complete NUL-terminated path")

    make_call = "int result = make_mount_path_absolute(path, logical.data(), logical.size());"
    canonical_call = "result = canonicalize_mount_path(logical.data(), logical.size());"
    apply_call = "return apply_current_task_root_prefix(logical.data(), out, outsize);"
    require_sequence(resolve, [logical_decl, make_call, "if (result < 0)", canonical_call, "if (result < 0)", apply_call], "mount resolution")
    first_gate = resolve.find("if (result < 0)", resolve.find(make_call) + len(make_call))
    second_gate = resolve.find("if (result < 0)", resolve.find(canonical_call) + len(canonical_call))
    if first_gate < 0 or second_gate < 0:
        fail("mount resolution must gate both fallible scratch producers")
    for label, gate_pos in (("absolute path", first_gate), ("canonical path", second_gate)):
        failure = block_body_after(resolve[gate_pos:], "if (result < 0)")
        if failure.strip() != "return result;":
            fail(f"{label} failure must return before consuming logical scratch")
    if resolve.count("logical.data()") != 3 or resolve.count("logical.size()") != 2:
        fail("logical mount scratch has an unexpected producer or consumer")

    for name, body, first_consumer in (
        ("mount_filesystem", mount, "std::strlen(resolved.data())"),
        ("unmount_filesystem_impl", unmount, "std::strcmp(resolved.data(), mp->path)"),
    ):
        producer = "int const PATH_RET = resolve_mount_path(path, resolved.data(), resolved.size());"
        gate = "if (PATH_RET < 0)"
        require_order(body, resolved_decl, producer, f"{name} resolver output")
        require_order(body, producer, gate, f"{name} resolver failure gate")
        require_order(body, gate, first_consumer, f"{name} consume only after success")
        before_gate = body[: body.find(gate)]
        if before_gate.count("resolved.data()") != 1 or before_gate.count("resolved.size()") != 1:
            fail(f"{name} must not inspect resolver output before its failure gate")
        failure = block_body_after(body, gate)
        if not failure.rstrip().endswith("return PATH_RET;") or failure.count("return PATH_RET;") != 1 or "resolved" in failure:
            fail(f"{name} must return without consuming resolver output on failure")


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
    test_mount_path_scratch_is_fully_produced_before_use()
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
