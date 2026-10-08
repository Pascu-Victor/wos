#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MOUNT_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.hpp"
MOUNT_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.cpp"
FILE_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "file.hpp"
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
    mount = function_body(source, "prepare_mount_filesystem")
    unmount = function_body(source, "unmount_filesystem_impl")

    components_decl = "std::array<const char*, MAX_MOUNT_COMPONENTS> components __attribute__((uninitialized));"
    result_decl = "std::array<char, MAX_MOUNT_PATH> result __attribute__((uninitialized));"
    logical_decl = "std::array<char, MAX_MOUNT_PATH> logical __attribute__((uninitialized));"
    resolved_decl = "std::array<char, MAX_MOUNT_PATH> resolved __attribute__((uninitialized));"

    require_only_uninitialized_array(canonicalize, "components", components_decl, "canonical component scratch")
    require_only_uninitialized_array(canonicalize, "result", result_decl, "canonical result scratch")
    require_only_uninitialized_array(resolve, "logical", logical_decl, "logical mount scratch")
    require_only_uninitialized_array(mount, "resolved", resolved_decl, "prepared mount resolver output")
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
        producer = ("int const PATH_RET = path_kind == MountPathKind::RETAINED_PHYSICAL"
                    if name == "mount_filesystem" else
                    "int const PATH_RET = scope.is_host() ? resolve_mount_path(path, resolved.data(), resolved.size())")
        gate = "if (PATH_RET < 0)"
        require_order(body, resolved_decl, producer, f"{name} resolver output")
        require_order(body, producer, gate, f"{name} resolver failure gate")
        require_order(body, gate, first_consumer, f"{name} consume only after success")
        before_gate = body[: body.find(gate)]
        producers = 2  # Physical copy and host-root resolution each fully produce output.
        if before_gate.count("resolved.data()") != producers or before_gate.count("resolved.size()") != producers:
            fail(f"{name} must not inspect resolver output before its failure gate")
        if name == "mount_filesystem":
            require_sequence(before_gate, [
                "if (path_kind == MountPathKind::RETAINED_PHYSICAL && path[0] != '/')",
                producer,
                "? copy_mount_path_string(path, resolved.data(), resolved.size())",
                ": resolve_mount_path(path, resolved.data(), resolved.size());",
            ], "physical copy or legacy resolution must fully produce mount scratch")
        failure = block_body_after(body, gate)
        if not failure.rstrip().endswith("return PATH_RET;") or failure.count("return PATH_RET;") != 1 or "resolved" in failure:
            fail(f"{name} must return without consuming resolver output on failure")


def test_mount_lookup_returns_raii_refs() -> None:
    header = normalized_body(MOUNT_HPP.read_text())
    required = [
        "class MountRef",
        "std::atomic<uint32_t> refs{0}",
        "std::atomic<uint32_t> open_files{0}",
        "std::atomic<bool> retiring{false}",
        "auto find_mount_point(const char* path, size_t known_path_len = UNKNOWN_MOUNT_PATH_LEN, const mount_ns::NamespaceRef& scope = {}) -> MountRef;",
        "auto get_mount_at(size_t index, const mount_ns::NamespaceRef& scope = {}) -> MountRef;",
        "void put_mount_point(MountPoint* mount);",
    ]
    missing = [token for token in required if token not in header]
    if missing:
        fail("mount lifetime API is missing retained-ref tokens: " + ", ".join(missing))


def test_open_files_pin_mount_through_backend_close() -> None:
    header = MOUNT_HPP.read_text()
    file_header = FILE_HPP.read_text()
    source = MOUNT_CPP.read_text()
    core = CORE_CPP.read_text()

    for token in ("auto retain_mount_for_open_file(MountPoint* mount) -> bool;", "void release_mount_from_open_file(MountPoint* mount);"):
        if token not in header:
            fail(f"missing mount/open-file ownership API: {token}")
    if "MountPoint* mount_owner = nullptr;" not in file_header:
        fail("File must retain its owning MountPoint independently of fd aliases")

    retain = function_body(source, "retain_mount_for_open_file")
    release = function_body(source, "release_mount_from_open_file")
    destroy = function_body(core, "vfs_destroy_file")
    require_sequence(
        retain,
        ["mount_lock.lock()", "!mount->retiring.load(std::memory_order_acquire)", "mount->open_files.fetch_add", "mount_lock.unlock()"],
        "open-file mount reservation",
    )
    require_sequence(
        release,
        ["mount_lock.lock()", "mount->open_files.load(std::memory_order_acquire)", "mount->open_files.store(OPEN_FILES - 1", "mount_lock.unlock()"],
        "open-file mount release",
    )
    require_sequence(destroy, ["f->fops->vfs_close(f)", "finish_file_close(f, CLOSE_MAY_CHANGE_METADATA, close_result, &owner)"],
                     "backend close before shared finalization")
    finish = function_body(core, "finish_file_close")
    require_sequence(finish, ["MountPoint* const MOUNT_OWNER = f->mount_owner", "f->mount_owner = nullptr",
                             "release_mount_from_open_file(MOUNT_OWNER)", "delete f"],
                     "shared finalization releases mount once before File destruction")
    if "return finish_file_close(file, close_may_change_metadata, close_result)" not in function_body(core, "vfs_finish_file_close"):
        fail("abandoned close must use the same post-backend finalizer")

    for name in ("vfs_open_resolved_for_task", "vfs_open_file_impl"):
        body = function_body(core, name)
        require_order(body, "MountOpenFileReservation mount_file_reservation{mount, OPEN_POLICY}", "switch (mount->fs_type)", f"{name} reserve before backend open")
        require_order(body, "switch (mount->fs_type)", "mount_file_reservation.attach(f)", f"{name} attach backend result")


def test_unmount_retires_in_table_then_drains_before_destroy() -> None:
    source = MOUNT_CPP.read_text()
    body = function_body(source, "unmount_filesystem_impl")
    required = [
        "mp->retiring.store(true, std::memory_order_release);",
        "mount_lock.unlock();",
        "complete_retired_mount_teardown(retiring_mount, false, &mount_count_after_remove)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("unmount_filesystem() is missing retained teardown tokens: " + ", ".join(missing))
    if "mounts.remove_at" in body:
        fail("explicit unmount must keep the retiring tombstone published while references and backend teardown drain")
    require_order(body, "mount_lock.unlock()", "complete_retired_mount_teardown", "retire before teardown outside mount lock")

    teardown = function_body(source, "complete_retired_mount_teardown")
    require_sequence(
        teardown,
        [
            "wait_for_mount_refs_to_drain(mount)",
            "mount->open_files.load(std::memory_order_acquire)",
            "destroy_mount_private_data(mount, false)",
            "mounts.remove_at(REMOVE_INDEX)",
            "destroy_mount_storage(mount)",
        ],
        "retiring mount teardown",
    )
    if "rollback_mount_retirement(mount)" not in teardown:
        fail("failed backend teardown must make the intact mount usable for retry")

    conditional_body = function_body(source, "unmount_filesystem_if_private_data")
    if "unmount_filesystem_impl(path, expected_private_data, true)" not in conditional_body:
        fail("conditional unmount must require an exact private_data owner match")
    if "mp->private_data == expected_private_data" not in body:
        fail("unmount implementation must compare the mount owner under mount_lock")

    owner_body = function_body(source, "unmount_filesystem_by_private_data")
    owner_required = [
        "MP->private_data != expected_private_data",
        "MP->retiring.store(true, std::memory_order_release)",
        "mount_lock.unlock()",
        "complete_retired_mount_teardown(owned_mount, false, &mount_count_after_remove)",
    ]
    missing = [token for token in owner_required if token not in owner_body]
    if missing:
        fail("owner-identity unmount is missing stable teardown tokens: " + ", ".join(missing))
    require_order(owner_body, "MP->private_data != expected_private_data", "MP->retiring.store", "owner match before retirement")
    require_order(owner_body, "mount_lock.unlock()", "complete_retired_mount_teardown", "owner teardown outside mount lock")


def test_mount_initialization_reserves_exact_path() -> None:
    source = MOUNT_CPP.read_text()
    prepare_body = function_body(source, "prepare_mount_filesystem")
    publish_body = function_body(source, "publish_prepared_mount_with_identity")
    required = [
        "std::array<MountInitialization, MAX_MOUNT_INITIALIZATIONS> mount_initializations{}",
        "mount_path_initializing_locked",
        "MountInitializationReservation reservation{}",
        "state->reservation.acquire(mount->path, PIVOT_EPOCH, scope)",
        "state->reservation.active_locked()",
        "state->reservation.commit_locked()",
    ]
    missing = [token for token in required if token not in source and token not in prepare_body and token not in publish_body]
    if missing:
        fail("same-path mount initialization reservation is missing: " + ", ".join(missing))
    require_order(prepare_body, "state->reservation.acquire", "fat32_init_device", "reserve before FAT initialization")
    require_order(prepare_body, "state->reservation.acquire", "xfs_vfs_init_device", "reserve before XFS recovery")
    require_order(publish_body, "mounts.push_back(mount)", "state->reservation.commit_locked()", "publish before reservation release")


def test_remote_mount_is_configured_atomically() -> None:
    source = MOUNT_CPP.read_text()
    prepare_body = function_body(source, "prepare_mount_filesystem")
    publish_body = function_body(source, "publish_prepared_mount_with_identity")
    prepare_required = ["mount->private_data = initial_private_data", "mount->fops = initial_fops"]
    publish_required = ["state->reservation.active_locked()", "mount_path_occupied_locked(mount->path, mount->namespace_owner)", "mounts.push_back(mount)"]
    missing = [token for token in prepare_required if token not in prepare_body]
    missing.extend(token for token in publish_required if token not in publish_body)
    if missing:
        fail("remote mount publication is missing atomic owner/path tokens: " + ", ".join(missing))
    require_order(prepare_body, "mount->private_data = initial_private_data", "mount->fops = initial_fops", "remote owner setup")
    require_order(publish_body, "mount_path_occupied_locked(mount->path, mount->namespace_owner)", "mounts.push_back(mount)", "duplicate rejection")

    remote_mount = function_body(REMOTE_VFS_CPP.read_text(), "mount_vfs_proxy_lane")
    if 'mount_filesystem(local_mount_path, "remote", nullptr, 0, nullptr, state, &g_remote_vfs_fops)' not in remote_mount:
        fail("remote VFS must publish its state/fops in the mount-table insertion transaction")
    if "configure_mount_point_exact" in remote_mount:
        fail("remote VFS must not configure a same-path mount in a second lookup")


def test_writable_block_mount_holds_lease_until_destroy() -> None:
    header = MOUNT_HPP.read_text()
    source = MOUNT_CPP.read_text()
    prepare_body = function_body(source, "prepare_mount_filesystem")
    publish_body = function_body(source, "publish_prepared_mount_with_identity")
    destroy_body = function_body(source, "destroy_mount")

    if "ker::dev::BlockWriterLease block_writer_lease{};" not in header:
        fail("MountPoint must own the local block-writer lease")
    required = [
        "bool const BLOCK_RW_FS",
        "mount->block_writer_lease.try_acquire(device, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT)",
        "destroy_mount(mount, true)",
        "mounts.push_back(mount)",
    ]
    missing = [token for token in required if token not in prepare_body and token not in publish_body]
    if missing:
        fail("writable block mount lease lifecycle is missing: " + ", ".join(missing))
    require_order(prepare_body, "mount->block_writer_lease.try_acquire", "fat32_init_device", "lease before FAT initialization")
    require_order(prepare_body, "mount->block_writer_lease.try_acquire", "xfs_vfs_init_device", "lease before XFS initialization")
    if source.find("mount->block_writer_lease.try_acquire") >= source.find("mounts.push_back(mount)"):
        fail("lease before mount publication: writer lease must precede mount-table insertion")
    if "destroy_mount_storage(mount)" not in destroy_body:
        fail("destroy_mount() must release MountPoint storage and its writer lease after backend teardown")
    if "block_writer_lease.release" in source:
        fail("local writer lease must release only through MountPoint destruction after ref drain")


def test_pivot_rewrites_are_transactional_and_gate_publication() -> None:
    source = MOUNT_CPP.read_text()
    prepare_body = function_body(source, "prepare_mount_filesystem")
    publish_body = function_body(source, "publish_prepared_mount_with_identity")
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
    pivot_check = "mount_pivot_epoch.load(std::memory_order_acquire) != PIVOT_EPOCH"
    publish_pivot_check = "mount_pivot_epoch.load(std::memory_order_acquire) != state->pivot_epoch"
    if pivot_check not in prepare_body or publish_pivot_check not in publish_body:
        fail("prepared mount publication must reject a path resolved in an older pivot epoch")
    if remap_body.count("mount_has_active_refs_locked") < 2 or "return -EBUSY;" not in remap_body:
        fail("remap_mounts_for_pivot() must fail instead of rewriting paths with active refs")
    required_remap = [
        "mount_pivot_epoch.store(ODD_EPOCH, std::memory_order_release)",
        "mp->retiring.load(std::memory_order_acquire)",
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
    require_order(remap_body, "mp->retiring.load(std::memory_order_acquire)", "path_is_under_root", "retirement fences every pivot path")
    require_order(remap_body, "ker::util::SmallVec<PivotMountReplacement, 8> replacements", "bump_mount_generation_locked()", "allocate before commit")
    require_order(remap_body, "mount_has_active_refs_locked(replacement.mount, cover_state)", "delete[] mp->path", "final ref check before commit")
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
        "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data(), &CREDENTIALS, &references)",
        "wki_remote_vfs_cancel_export_rebuild()",
        "rebase_wki_mounts_for_new_root(stable_new_root.data())",
        "wki_remote_vfs_rebuild_exports()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("pivot/WKI split-phase barrier is missing: " + ", ".join(missing))
    require_order(body, "snapshot_bounded_path_string(put_old", "wki_remote_vfs_prepare_export_rebuild()", "snapshot paths before prepare")
    require_order(body, "wki_remote_vfs_prepare_export_rebuild()", "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data(), &CREDENTIALS, &references)", "prepare before remap")
    require_order(body, "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data(), &CREDENTIALS, &references)", "rebase_wki_mounts_for_new_root(stable_new_root.data())", "remap before final rebase")
    require_order(body, "rebase_wki_mounts_for_new_root(stable_new_root.data())", "wki_remote_vfs_rebuild_exports()", "rebase before export rebuild")

    snapshot_body = function_body(CORE_CPP.read_text(), "snapshot_bounded_path_string")
    if "if (pos == 0)" not in snapshot_body or "return -EINVAL;" not in snapshot_body:
        fail("bounded pivot path snapshots must reject empty paths with EINVAL")


def test_vfs_umount_invalidates_only_exact_mount() -> None:
    body = function_body(CORE_CPP.read_text(), "vfs_umount")
    required = [
        "ker::mod::sys::MutexGuard publication_guard(g_vfs_namespace_publication_mutex);",
        "auto mount_ref = find_mount_point(resolved.data());",
        "std::strcmp(mount->path, resolved.data()) == 0",
        "expected_dev_id = mount->dev_id;",
        "stream_invalidate_mount_scope(mount->fs_type, stream_scope_key_for_mount(mount));",
        "unmount_filesystem_if_dev_id(stable_target.data(), expected_dev_id)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("vfs_umount() must invalidate stream scope only for exact mount targets: " + ", ".join(missing))
    require_order(body, "std::strcmp(mount->path, resolved.data()) == 0", "stream_invalidate_mount_scope", "exact umount invalidation")
    require_order(body, "expected_dev_id = mount->dev_id;", "unmount_filesystem_if_dev_id(stable_target.data(), expected_dev_id)", "stable unmount identity")
    if "return unmount_filesystem(target);" in body:
        fail("vfs_umount() must not retire a replacement mount by pathname alone")


def test_vfs_mount_resolves_dev_source_symlinks() -> None:
    source = CORE_CPP.read_text()
    body = function_body(source, "vfs_mount")
    normalized = " ".join(body.split())
    for token in ["LookupIntent::MOUNT_SOURCE", "LookupFollowPolicy::FOLLOW_FINAL, false, true, &source_lookup, 3, false, &CREDENTIALS",
                  "consume_mount_source(source_lookup, *authorization_task, CREDENTIALS, selected)",
                  "devfs::devfs_resolve_block_source(selected)"]:
        if token not in normalized:
            fail("mount must retain its admitted source: " + token)
    require_order(body, "vfs_acquire_lookup(authorization_task, AT_FDCWD, source", "consume_mount_source", "source acquisition")
    require_order(body, "consume_mount_source", "devfs_resolve_block_source", "source admission before resolution")
    require_order(body, "devfs_resolve_block_source", "probe_block_device_fstype", "selected provider before I/O")
    if "block_device_find_by_name" in body or "resolve_mount_source_path" in source:
        fail("mount must not recapture ambient source authority or reselect a global name")


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
    wrapper = function_body(PROCFS_CPP.read_text(), "procfs_read_mounts_as")
    if "generate_mounts_for_task(task, staged.data(), staged.size())" not in wrapper:
        fail("/proc/mounts must pass its retained caller to the staged root-aware generator")
    procfs = function_body(PROCFS_CPP.read_text(), "generate_mounts_for_task")
    for name, body in (("remote VFS auto-discovery", remote), ("/proc/mounts", procfs)):
        if "get_mount_snapshot_at" not in body:
            fail(f"{name} must iterate mount snapshots rather than raw mount pointers")
        if "get_mount_at" in body:
            fail(f"{name} must not use get_mount_at() for read-only mount metadata")


def test_mount_descendants_and_retiring_ancestors_share_publication_lock() -> None:
    source = MOUNT_CPP.read_text()
    descendants = function_body(source, "mount_path_has_descendants_locked")
    assert "for (auto* mount : mounts)" in descendants
    assert "mount_initializations" in descendants and "initialization.active" in descendants
    assert "retiring" not in descendants  # Retiring descendants still own their position.
    relation = function_body(source, "mount_path_is_descendant")
    assert "parent[0] == '/' && parent[1] == '\\0'" in relation and "child[LENGTH] == '/'" in relation
    for name, subject in (("unmount_filesystem_impl", "mp"), ("unmount_filesystem_by_private_data", "MP")):
        body = function_body(source, name)
        require_order(body, f"mount_path_has_descendants_locked({subject}->path, {subject}->namespace_owner)",
                      f"{subject}->retiring.store(true", name)
    teardown = function_body(source, "complete_retired_mount_teardown")
    require_order(teardown, "mount_path_has_descendants_locked(mount->path, mount->namespace_owner)", "destroy_mount_private_data", "shutdown parent guard")
    for name, after in (("prepare_mount_filesystem", "new (std::nothrow) PreparedMountState"),
                        ("publish_prepared_mount_with_identity", "mounts.push_back(mount)")):
        body = function_body(source, name)
        require_order(body, "mount_lock.lock()", "mount_path_has_retiring_ancestor_locked", name)
        require_order(body, "mount_path_has_retiring_ancestor_locked", after, name)
    reservation = function_body(source[source.index("class MountInitializationReservation"):], "acquire")
    require_order(reservation, "mount_path_has_retiring_ancestor_locked(path, scope)", "initialization.active = true", "reservation race")
    fixture = function_body(source, "mount_selftest_descendant_lifetime")
    for token in ("deny_parent()", "RETIRING_GENERATION", "raced.acquire", "complete_retired_mount_teardown(cleanup.parent",
                  "unmount_filesystem(CHILD)", "std::memcmp(read.data(), \"seed\", 4)"):
        assert token in fixture


def test_native_mount_cover_is_owned_and_pivot_retires_after_unlock() -> None:
    source = MOUNT_CPP.read_text()
    core = CORE_CPP.read_text()
    assert "File* covered_directory" in MOUNT_HPP.read_text()
    consume = function_body(core, "consume_mount_target")
    require_order(consume, "std::unique_ptr<File, LocalWalkFileRelease> covered", "MutexGuard publication_guard", "outer cover owner")
    require_order(consume, "validate_mount_target_lookup(target, nullptr)", "pin_directory_view_lookup(target, false, state.covered)", "admit before pin")
    require_order(consume, "pin_directory_view_lookup(target, false, state.covered)", "publish_prepared_mount(state.prepared, target.path(), state.covered.get())", "pin before publication")
    publish = function_body(source, "publish_prepared_mount_with_identity")
    require_order(publish, "covered_directory->refcount.load(std::memory_order_acquire) != 1", "mounts.push_back(mount)", "independent native pin")
    require_order(publish, "mounts.push_back(mount)", "vfs_retain_file(covered_directory)", "failed publication retains nothing")
    for token in ("covered_directory->directory_view", "covered_directory->vfs_path != nullptr", "FILE_ACCESS_SEARCH_ONLY"):
        assert token in publish
    capture = function_body(source, "capture_mount_covered_directory")
    require_order(capture, "mount_lock.lock()", "vfs_retain_file(mount->covered_directory)", "capture under table lock")
    assert "mount_pivot_epoch" in capture and "mount->retiring" in capture and "-ENOTSUP" in capture
    assert "vfs_put_file" not in capture
    busy = function_body(source, "mount_has_active_refs_locked")
    assert "pin->refcount.load(std::memory_order_acquire) == 1" in busy
    assert "mount->open_files.load(std::memory_order_acquire) != internal_pins" in busy
    pivot = function_body(source, "remap_mounts_for_pivot")
    require_order(pivot, "mount_has_active_refs_locked(replacement.mount, cover_state)", "mount->covered_directory = std::exchange(entry.candidate, nullptr)", "abort preserves cover")
    require_order(pivot, "mount->covered_directory = std::exchange(entry.candidate, nullptr)", "mp->path = replacement.path", "retire stale edges before remap")
    assert "reference_owner.state_ = cover_state" in pivot and "new (std::nothrow) PivotCoverEntry[MOUNT_COUNT]" in pivot
    destroy = function_body(source, "destroy_mount_storage")
    assert "vfs_put_file(mount->covered_directory)" in destroy and "mount_lock.lock" not in destroy


def test_mount_identity_exhaustion_precedes_publication() -> None:
    source = MOUNT_CPP.read_text()
    wrapper = function_body(source, "publish_prepared_mount")
    assert "publish_prepared_mount_with_identity(prepared, expected_physical_path, covered_directory, next_dev_id)" in wrapper
    body = function_body(source, "publish_prepared_mount_with_identity")
    require_order(body, "mount_lock.lock()", "if (next_identity == 0)", "cursor serialized with publication")
    require_order(body, "return -EOVERFLOW", "mounts.push_back(mount)", "exhaustion leaves table and cover unchanged")
    require_order(body, "mounts.push_back(mount)", "mount->dev_id = next_identity", "failed table insertion consumes no ID")
    assert "next_identity = next_identity == UINT32_MAX ? 0 : next_identity + 1;" in body
    assert "next_dev_id++" not in source


def main() -> None:
    test_mount_identity_exhaustion_precedes_publication()
    test_native_mount_cover_is_owned_and_pivot_retires_after_unlock()
    test_mount_descendants_and_retiring_ancestors_share_publication_lock()
    test_mount_path_scratch_is_fully_produced_before_use()
    test_mount_lookup_returns_raii_refs()
    test_open_files_pin_mount_through_backend_close()
    test_unmount_retires_in_table_then_drains_before_destroy()
    test_mount_initialization_reserves_exact_path()
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
