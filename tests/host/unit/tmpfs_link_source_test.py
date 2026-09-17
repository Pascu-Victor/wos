#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TMPFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "tmpfs.hpp"
TMPFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "tmpfs.cpp"
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, pattern: str, context: str) -> str:
    match = re.search(pattern + r"\s*\{", source)
    if match is None:
        fail(f"missing function {context}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {context}")
    return source[match.end() : pos - 1]


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_tmpfs_nodes_have_canonical_hardlink_state() -> None:
    header = TMPFS_HPP.read_text()
    for token in [
        "std::atomic<uint32_t> link_count{1}",
        "TmpNode* hardlink_target = nullptr",
        "auto tmpfs_create_hardlink(TmpNode* parent, const char* name, TmpNode* target) -> TmpNode*;",
        "auto tmpfs_canonical_node(TmpNode* node) -> TmpNode*;",
        "auto tmpfs_canonical_node(const TmpNode* node) -> const TmpNode*;",
        "auto tmpfs_link_count(const TmpNode* node) -> uint32_t;",
        "void tmpfs_drop_detached_node(TmpNode* node);",
    ]:
        require(header, token, "tmpfs hard-link declarations")


def test_tmpfs_hardlink_aliases_share_canonical_target() -> None:
    source = TMPFS_CPP.read_text()
    create_link = function_body(
        source,
        r"auto\s+tmpfs_create_hardlink\(TmpNode\*\s+parent,\s*const\s+char\*\s+name,\s*TmpNode\*\s+target\)\s*->\s*TmpNode\*",
        "tmpfs_create_hardlink",
    )
    drop_node = function_body(source, r"void\s+tmpfs_drop_detached_node\(TmpNode\*\s+node\)", "tmpfs_drop_detached_node")

    require_order(
        create_link,
        [
            "TmpNode* canonical = tmpfs_canonical_node(target)",
            "canonical->type == TmpNodeType::DIRECTORY",
            "tmpfs_lookup(parent, name) != nullptr",
            "auto* node = new TmpNode",
            "node->type = canonical->type",
            "node->hardlink_target = canonical",
            "node->link_count.store(0, std::memory_order_relaxed)",
            "register_tmp_node(node)",
            "add_child(parent, node)",
            "canonical->link_count.fetch_add(1, std::memory_order_acq_rel)",
        ],
        "tmpfs hard-link alias creation",
    )

    require_order(
        drop_node,
        [
            "TmpNode* canonical = tmpfs_canonical_node(node)",
            "canonical->link_count.fetch_sub(1, std::memory_order_acq_rel)",
            "if (node != canonical)",
            "tmpfs_free_node(node)",
            "if (canonical != nullptr && last_link)",
            "canonical->unlinked = true",
            "canonical->open_count.load(std::memory_order_acquire) == 0",
            "tmpfs_free_node(canonical)",
        ],
        "tmpfs detached hard-link drop",
    )


def test_tmpfs_file_operations_use_canonical_node() -> None:
    source = TMPFS_CPP.read_text()
    open_path = function_body(source, r"auto\s+tmpfs_open_path\([^{}]*\)\s*->\s*ker::vfs::File\*", "tmpfs path")
    open_lookup = function_body(source, r"auto\s+tmpfs_open_lookup\([^{}]*\)\s*->\s*ker::vfs::File\*", "tmpfs lookup")
    read_body = function_body(source, r"auto\s+tmpfs_read\(ker::vfs::File\*\s+f,\s*void\*\s+buf,\s*size_t\s+count,\s*size_t\s+offset\)\s*->\s+ssize_t", "tmpfs_read")
    write_body = function_body(source, r"auto\s+tmpfs_write\(ker::vfs::File\*\s+f,\s*const\s+void\*\s+buf,\s*size_t\s+count,\s*size_t\s+offset\)\s*->\s+ssize_t", "tmpfs_write")
    close_body = function_body(source, r"auto\s+tmpfs_fops_close\(ker::vfs::File\*\s+f\)\s*->\s+int", "tmpfs_fops_close")
    truncate_body = function_body(source, r"auto\s+tmpfs_fops_truncate\(ker::vfs::File\*\s+f,\s*off_t\s+length\)\s*->\s+int", "tmpfs_fops_truncate")

    require_order(open_lookup, ["MutexGuard guard(tmpfs_lock)", "tmpfs_validate_lookup_locked(handle)",
                                "TmpNode* node = tmpfs_canonical_node(handle.target())", "authorize_file_open(credentials,",
                                "tmpfs_resize_locked(node, 0)", "node->open_count.fetch_add(",
                                "file->private_data = node", "file->is_directory = node->type == TmpNodeType::DIRECTORY"],
                  "tmpfs validates and authorizes its canonical retained node before truncate/open")
    require_order(open_path, ["tmpfs_acquire_lookup(credentials, root, path, &lookup)",
                              "tmpfs_open_lookup(credentials, lookup, flags, mode, REQUIRE_DIRECTORY, &result)"],
                  "tmpfs compatibility path shares retained canonical open")
    for body, context in [
        (read_body, "tmpfs read"),
        (write_body, "tmpfs write"),
        (close_body, "tmpfs close"),
        (truncate_body, "tmpfs truncate"),
    ]:
        require(body, "tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data))", context)


def test_vfs_link_uses_alias_not_copy() -> None:
    core = CORE_CPP.read_text()
    link_body = function_body(
        core,
        r"auto\s+vfs_link_resolved_paths\([^)]*\)\s*->\s+int",
        "vfs_link_resolved_paths",
    )
    tmpfs = TMPFS_CPP.read_text()
    stat_helper = function_body(tmpfs, r"void\s+tmpfs_fill_metadata_locked\([^)]*\)", "tmpfs metadata snapshot")
    unlink_body = function_body(core, r"auto\s+vfs_unlink_resolved_path\([^)]*\)\s*->\s+int", "vfs_unlink_resolved_path")

    require_order(
        link_body,
        [
            "TmpfsLookupHandle source{}, destination{}",
            "tmpfs_acquire_lookup(credentials, tmpfs_root_for_mount(old_mount), old_fs, &source)",
            "tmpfs_acquire_lookup(credentials, tmpfs_root_for_mount(new_mount), new_fs, &destination)",
            "tmpfs_link_lookup(credentials, source, destination, nullptr,",
            "if (ret != 0) return ret;",
            "metadata_cache_note_path_changed(\"/\", nullptr)",
            "vfs_cache_notify_path_changed(old_abs_path, new_abs_path)",
        ],
        "vfs_link tmpfs hard-link path",
    )
    require_absent(link_body, "tmpfs_copy_file_contents", "vfs_link must not copy file data")
    require_absent(link_body, "tmpfs_create_file(new_parent", "vfs_link must not create copied file")
    require_absent(link_body, "tmpfs_create_symlink(new_parent", "vfs_link must not create copied symlink")

    require_order(stat_helper, ["tmpfs_canonical_node(node)", ".inode = reinterpret_cast<uintptr_t>(canonical)",
                                ".size = canonical->size", ".link_count = tmpfs_link_count(canonical)"], "tmpfs canonical stat identity")
    require_order(unlink_body, ["TmpfsLookupHandle lookup", "tmpfs_acquire_lookup(",
                                "tmpfs_unlink_lookup(credentials, lookup, &hardlink_count_changed, mount->read_only)",
                                "if (RET == 0 && hardlink_count_changed)",
                                'metadata_cache_note_path_changed("/", nullptr)'], "retained tmpfs unlink and hardlink invalidation")
    require_absent(unlink_body, "tmpfs_walk_path", "unlink may not borrow a parent before locking")
    backend = function_body(tmpfs, r"auto\s+tmpfs_unlink_lookup\([^)]*\)\s*->\s+int", "tmpfs_unlink_lookup")
    require_order(backend, ["MutexGuard guard(tmpfs_lock)", "tmpfs_lookup_target_locked(handle, &entry, &canonical)",
                           "authorize_file_removal(credentials,", "metadata_identity_locked(*canonical)", "if (AUTH != 0) return AUTH",
                           "bool const LINK_COUNT_CHANGED = tmpfs_link_count(entry) > 1", "tmpfs_detach_child(handle.parent(), entry)",
                           "tmpfs_drop_detached_node(entry)", "*hardlink_count_changed = LINK_COUNT_CHANGED"],
                  "authorize canonical sticky ownership before detachment and output")



def main() -> None:
    test_tmpfs_nodes_have_canonical_hardlink_state()
    test_tmpfs_hardlink_aliases_share_canonical_target()
    test_tmpfs_file_operations_use_canonical_node()
    test_vfs_link_uses_alias_not_copy()
    print("tmpfs hard-link source invariants hold")


if __name__ == "__main__":
    main()
