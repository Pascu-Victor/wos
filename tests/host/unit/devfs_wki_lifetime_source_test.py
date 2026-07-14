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
        "auto devfs_open_path": 1,  # WKI-aware open; lock + file-owned pin.
        "auto devfs_create_directory": 1,  # Tree mutation helper.
        "auto devfs_create_symlink": 1,  # Tree mutation helper.
        "auto devfs_add_device_node(const std::array": 1,  # Tree mutation helper.
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
            "return DevFSNodeRef(node, node != nullptr && WKI_PATH)",
        ],
        "WKI path pin acquisition",
    )
    reset = function_body(source, "void DevFSNodeRef::reset")
    require_tokens(reset, ["if (owns_wki_node_ref_)", "wki_release_node_ref(node_)"], "WKI path pin release")

    if core.count("ker::vfs::devfs::devfs_acquire_path(fs_path)") != 5:
        fail("all five DEVFS stat/existence/metadata raw-walk call sites must retain a path reference")


def test_block_resolution_uses_only_a_locked_snapshot_after_unpin() -> None:
    body = function_body(DEVFS_CPP.read_text(), "auto devfs_resolve_block_device")
    require_tokens(
        body,
        [
            "auto node_ref = devfs_acquire_path(path)",
            "OptionalWkiLock const WKI_GUARD(WKI_PATH)",
            "node->wki_unlinked",
            "resource_generation = ctx->resource_generation",
            "node_ref.reset()",
            "wki_resource_observation_snapshot(",
            "resource_generation, &owner_incarnation",
            "wki_dev_proxy_attach_block(owner_node, resource_id, resource_generation, owner_incarnation,",
            "device_name.data())",
        ],
        "WKI block resolution snapshot",
    )
    require_order(body, "OptionalWkiLock const WKI_GUARD", "node_ref.reset()", "identity snapshot before unpin")
    require_order(body, "node_ref.reset()", "wki_dev_proxy_attach_block", "unpin before blocking attach")
    after_unpin = body.split("node_ref.reset();", 1)[1]
    if "node->" in after_unpin or "ctx->" in after_unpin:
        fail("block resolution must not dereference devfs backing objects after dropping the path pin")


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


def main() -> None:
    test_internal_raw_walk_path_callers_are_allowlisted()
    test_wki_path_readers_hold_raii_pins()
    test_block_resolution_uses_only_a_locked_snapshot_after_unpin()
    test_block_alias_removal_is_target_qualified()
    print("devfs WKI lifetime source invariants hold")


if __name__ == "__main__":
    main()
