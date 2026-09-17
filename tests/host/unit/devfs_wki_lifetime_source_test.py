#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEVFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.cpp"
DEVFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.hpp"
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, function_name: str) -> str:
    marker = source.find(function_name)
    if marker < 0:
        fail(f"missing function {function_name}")
    start = source.find("{", marker)
    if start < 0:
        fail(f"missing body for {function_name}")
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    fail(f"unterminated body for {function_name}")
    return ""


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0 or second_index < 0 or first_index >= second_index:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_internal_raw_walk_path_callers_are_allowlisted() -> None:
    source = DEVFS_CPP.read_text()

    # walk_path is deliberately private. Keep every remaining raw use named
    # here so a new unpinned reader cannot quietly bypass devfs_acquire_path.
    expected_callers = {
        "auto devfs_acquire_path": 1,  # WKI-aware reader; lock + pin asserted below.
        "auto with_lookup_node": 1,  # Retained binding validation + commit under namespace lock.
        "auto devfs_open_path": 1,  # WKI-aware open; lock + file-owned pin.
        "auto devfs_create_directory": 1,  # Tree mutation helper.
        "auto devfs_create_symlink": 1,  # Tree mutation helper.
        "auto add_device_node_locked": 1,  # Public/peer writers hold the matching namespace guard.
        "auto devfs_remove_node": 1,  # Tree mutation helper.
        "void devfs_populate_partition_symlinks": 1,  # Boot/device population.
        "void devfs_populate_net_nodes": 2,  # Device population.
        "auto wki_ensure_dirs": 5,  # /dev/wki initialization/mutation path.
        "void devfs_nodes_init": 1,  # /dev/nodes mutation path.
        "void devfs_nodes_add_peer": 1,  # /dev/nodes mutation path.
        "void devfs_nodes_remove_peer": 2,  # /dev/nodes mutation path.
    }

    expected_total = 0
    for function_name, expected_count in expected_callers.items():
        actual_count = function_body(source, function_name).count("walk_path(")
        if actual_count != expected_count:
            fail(
                f"raw walk_path allowlist drift in {function_name}: "
                f"expected {expected_count}, found {actual_count}"
            )
        expected_total += expected_count

    # The sole additional occurrence is walk_path's private definition.
    if source.count("walk_path(") != expected_total + 1:
        fail("an internal raw walk_path caller is missing from the explicit allowlist")


def test_wki_path_readers_hold_raii_pins() -> None:
    header = DEVFS_HPP.read_text()
    source = DEVFS_CPP.read_text()
    core = CORE_CPP.read_text()

    require_tokens(
        header,
        [
            "class DevFSNodeRef",
            "DevFSNodeRef(const DevFSNodeRef&) = delete;",
            "~DevFSNodeRef();",
            "auto devfs_acquire_path(const char* path) -> DevFSNodeRef;",
        ],
        "WKI path lifetime API",
    )
    if "devfs_walk_path(" in header or "devfs_walk_path(" in core or "devfs_walk_path(" in source:
        fail("raw devfs path lookup must not escape without a WKI lifetime pin")

    acquire = function_body(source, "auto devfs_acquire_path")
    require_tokens(
        acquire,
        [
            "OptionalWkiLock const WKI_GUARD(WKI_PATH)",
            "DevFSNode* node = walk_path(path, false)",
            "node->wki_open_refs++",
            "return DevFSNodeRef(node, node != nullptr && WKI_PATH, node != nullptr && PTS_PATH, LOCAL_NODE)",
        ],
        "WKI path pin acquisition",
    )
    reset = function_body(source, "void DevFSNodeRef::reset")
    require_tokens(reset, ["if (owns_wki_node_ref_)", "wki_release_node_ref(node_)"], "WKI path pin release")

    if core.count("ker::vfs::devfs::devfs_acquire_path(fs_path)") != 6:
        fail("all six DEVFS component/stat/existence/metadata raw-walk call sites must retain a path reference")


def test_block_resolution_uses_only_a_locked_snapshot_after_unpin() -> None:
    source = DEVFS_CPP.read_text()
    body = function_body(source, "auto devfs_resolve_block_device")
    require_order(body, "devfs_capture_block_source", "retained.reset()", "capture before unpin")
    require_order(body, "retained.reset()", "devfs_resolve_block_source", "unpin before resolve")
    capture = function_body(source, "auto devfs_capture_block_source")
    require_order(capture, "with_lookup_node", "MetadataGuard const GUARD", "namespace before metadata")
    require_order(capture, "if (!is_wki_devfs_path(path))", "node->device->", "local provider avoids wrapper dereference")
    require_tokens(capture, ["block_device_is_registered(node->block_provider)", "candidate.local_provider = node->block_provider",
                             "candidate.resource_generation = ctx->resource_generation", "out = candidate"], "source identity capture")
    resolver = function_body(source, "auto devfs_resolve_block_source")
    if "node->" in resolver or "ctx->" in resolver:
        fail("resolved source must use only its copied snapshot")
    require_tokens(resolver, ["wki_resource_observation_snapshot", "wki_dev_proxy_attach_block"], "copied WKI identity")
    core = CORE_CPP.read_text()
    mount = function_body(core, "auto vfs_mount(")
    require_tokens(mount, ["LookupIntent::MOUNT_SOURCE", "&source_lookup, 3, false, &CREDENTIALS",
                          "consume_mount_source(source_lookup, *authorization_task, CREDENTIALS, selected)"], "admitted source")
    require_order(mount, "consume_mount_source", "devfs_resolve_block_source", "authorize before effects")
    if "block_device_find_by_name" in mount or "resolve_mount_source_path" in mount:
        fail("public mount must not reselect a global device name")
    consume = function_body(core, "auto consume_mount_source")
    require_order(consume, "authorize_mount_administration", "devfs_capture_block_source", "source policy before capture")
    require_tokens(consume, ["g_vfs_namespace_publication_mutex", "&task, &credentials"], "request-bound consumption")


def test_block_alias_removal_is_target_qualified() -> None:
    source = DEVFS_CPP.read_text()
    qualified_remove = function_body(source, "void wki_remove_named_symlink_if_target")
    require_tokens(
        qualified_remove,
        [
            "child->type != DevFSNodeType::SYMLINK",
            "std::string_view{child->symlink_target.data()} != expected_target",
            "wki_unlink_node(dir, child)",
        ],
        "target-qualified WKI alias removal",
    )

    aliases = function_body(source, "void wki_remove_wki_resource_aliases")
    require_tokens(
        aliases,
        [
            "wki_remove_named_symlink_if_distinct(",
            "expected_target",
            "wki_remove_named_symlink_if_target(dir, alias, expected_target)",
        ],
        "BLOCK remote alias ownership",
    )
    remove_all = function_body(source, "void wki_remove_resource_symlinks")
    require_tokens(
        remove_all,
        [
            "wki_build_resource_symlink_target(",
            "std::string_view const TARGET{target.data()}",
            "wki_remove_wki_resource_aliases(zone_sub, ctx, TARGET)",
        ],
        "resource target propagation",
    )


def test_root_children_share_a_bounded_guard() -> None:
    source = DEVFS_CPP.read_text()
    for name, node in [("auto find_child", "dir"), ("void remove_child", "parent"),
                       ("auto devfs_readdir", "dir_node")]:
        body = function_body(source, name)
        require_tokens(body, [f"RootChildrenGuard const ROOT_GUARD({node})"], name)
    commit = function_body(source, "auto try_add_root_child")
    require_order(commit, "RootChildrenGuard const GUARD", "root_node.children_count", "root commit lock")
    require_order(commit, "spare_capacity <= root_node.children_count", "std::swap", "spare revalidation")
    if "new " in commit or "delete" in commit or "vfs_debug_log(" in commit:
        fail("root publication must not allocate/free/log while IRQ locked")
    append = function_body(source, "void add_root_child")
    require_tokens(append, ["while (!try_add_root_child", "new DevFSNode*[required]", "delete[] spare"], "root growth retry")
    require_tokens(function_body(source, "void add_child"), ["if (parent == &root_node)", "add_root_child(child)"], "root writer routing")
    readdir = function_body(source, "auto devfs_readdir")
    if "child->device->" in readdir:
        fail("readdir must not dereference a borrowed Device")
    require_tokens(readdir, ["MetadataGuard const CHILD_GUARD", "child->is_block_device"], "directory type snapshot")


def test_local_metadata_pins_and_namespace_guards() -> None:
    source = DEVFS_CPP.read_text()
    header = DEVFS_HPP.read_text()
    require_tokens(header, ["std::atomic<uint32_t> local_refs{1}", "owns_local_node_ref_"], "local metadata ownership")
    for name in ["auto with_lookup_node", "auto devfs_acquire_path", "auto devfs_create_directory",
                 "auto devfs_create_symlink", "auto devfs_remove_node"]:
        body = function_body(source, name)
        require_order(body, "OptionalLocalLock const LOCAL_GUARD", "walk_path(", name)
    require_order(function_body(source, "auto devfs_add_device_node(const std::array"),
                  "OptionalLocalLock const LOCAL_GUARD", "add_device_node_locked(", "generic writer")
    acquire = function_body(source, "auto devfs_acquire_path")
    require_tokens(acquire, ["!WKI_PATH && !PTS_PATH", "node->local_refs.fetch_add"], "local pin acquisition")
    require_tokens(function_body(source, "void DevFSNodeRef::reset"),
                   ["if (owns_local_node_ref_)", "local_release_node_ref(node_)"], "local pin release")
    opened = function_body(source, "auto devfs_open_path")
    require_order(opened, "OptionalLocalLock const LOCAL_GUARD", "node->local_refs.fetch_add", "open selection pin")
    require_order(opened, "node->local_refs.fetch_add", "OPS->open_with_credentials", "pin before callback")
    require_tokens(opened, ["local_release_node_ref(node)"], "failed open pin release")
    require_tokens(function_body(source, "auto devfs_close"), ["local_release_node_ref(devfs_file->node)"], "file pin release")
    require_order(function_body(source, "auto devfs_readdir"),
                  "OptionalLocalLock const LOCAL_GUARD", "dir_node->children_count", "local directory read")
    remove = function_body(source, "auto devfs_remove_node")
    require_order(remove, "node->children_count != 0", "remove_child(", "nonempty namespace preservation")
    require_order(remove, "remove_child(", "local_release_node_ref(node)", "unlink before reference release")
    for name in ["auto is_nodes_devfs_path", "auto is_wki_devfs_path", "auto is_pts_devfs_path"]:
        require_tokens(function_body(source, name), ["while (*"], "leading separator classification")


def test_mount_directory_consumer_and_wki_writer_boundary() -> None:
    source = DEVFS_CPP.read_text()
    populate = function_body(source, "void devfs_populate_wki")
    require_order(populate, "WkiAutoLock const GUARD", "wki_ensure_dirs()", "bulk WKI initialization")
    require_tokens(populate, ["count = g_wki_total", "vfs_debug_log_hex(count)"], "locked WKI count snapshot")
    body = function_body(source, "auto devfs_consume_directory_lookup")
    require_order(body, "with_lookup_node(path, retained", "node->type != DevFSNodeType::DIRECTORY", "retained mount binding")
    require_order(body, "return -ENOTDIR", "return consume(context)", "directory before mount publication")
    for name, argument, mutation in [("auto devfs_create_directory", "path", "walk_path("),
                                     ("auto devfs_create_symlink", "path", "walk_path("),
                                     ("auto devfs_add_device_node(const std::array", "name.data()", "add_device_node_locked("),
                                     ("auto devfs_remove_node", "path", "walk_path(")]:
        writer = function_body(source, name)
        require_order(writer, f"if (is_wki_devfs_path({argument}))", mutation, "WKI dedicated writer boundary")
        prefix = writer[:writer.index("OptionalLocalLock")]
        require_tokens(prefix, ["return false;" if "remove" in name else "return nullptr;"], "WKI writer refusal")


def main() -> None:
    test_existing_block_reuse_checks_selected_observation()
    test_mount_directory_consumer_and_wki_writer_boundary()
    test_local_metadata_pins_and_namespace_guards()
    test_root_children_share_a_bounded_guard()
    test_internal_raw_walk_path_callers_are_allowlisted()
    test_wki_path_readers_hold_raii_pins()
    test_block_resolution_uses_only_a_locked_snapshot_after_unpin()
    test_block_alias_removal_is_target_qualified()
    print("devfs WKI lifetime source invariants hold")


def test_existing_block_reuse_checks_selected_observation() -> None:
    resolver = function_body(DEVFS_CPP.read_text(), "auto devfs_resolve_block_source")
    require_order(resolver, "wki_resource_observation_snapshot", "wki_dev_proxy_block_matches_observation", "live selected identity")
    require_tokens(" ".join(resolver.split()), ["wki_dev_proxy_block_matches_observation(existing, OWNER_NODE, RESOURCE_ID, GENERATION, incarnation)",
                             "? existing : nullptr"], "existing WKI identity")
    proxy = (ROOT / "modules/kern/src/net/wki/dev_proxy.cpp").read_text()
    predicate = function_body(proxy, "auto wki_dev_proxy_block_matches_observation")
    require_tokens(predicate, ["s_proxy_lock.lock()", "find_proxy_by_bdev(bdev)", "state->lock.lock()",
                              "state->ever_published", "!proxy_block_fenced(state)", "!state->epoch_reset_pending",
                              "!state->cleanup_in_progress", "!state->resume_pending", "!state->resume_in_progress",
                              "!state->detach_pending", "state->owner_node == owner_node", "state->resource_id == resource_id",
                              "state->resource_generation == resource_generation",
                              "wki_resource_incarnation_equal(state->binding_incarnation, owner_incarnation)",
                              "block_device_find_by_name(state->bdev.name.data()) == bdev",
                              "state->lock.unlock()", "s_proxy_lock.unlock()"], "published provider identity")
    for forbidden in ["wki_dev_proxy_attach_block(", "wki_send(", "block_read(", "block_write("]:
        if forbidden in predicate:
            fail("existing-provider comparison must not issue I/O or attach")
    attach = function_body(proxy, "auto wki_dev_proxy_attach_block")
    publication = attach.split("block_device_register(&state->bdev) == 0", 1)[1]
    require_order(publication, "s_proxy_lock.lock()", "state->ever_published = true", "publication flag writer serialization")


if __name__ == "__main__":
    main()
