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
    open_path = function_body(
        source,
        r"auto\s+tmpfs_open_path\(TmpNode\*\s+root,\s*const\s+char\*\s+path,\s*int\s+flags,\s*int\s+mode\)\s*->\s*ker::vfs::File\*",
        "tmpfs_open_path root",
    )
    read_body = function_body(source, r"auto\s+tmpfs_read\(ker::vfs::File\*\s+f,\s*void\*\s+buf,\s*size_t\s+count,\s*size_t\s+offset\)\s*->\s+ssize_t", "tmpfs_read")
    write_body = function_body(source, r"auto\s+tmpfs_write\(ker::vfs::File\*\s+f,\s*const\s+void\*\s+buf,\s*size_t\s+count,\s*size_t\s+offset\)\s*->\s+ssize_t", "tmpfs_write")
    close_body = function_body(source, r"auto\s+tmpfs_fops_close\(ker::vfs::File\*\s+f\)\s*->\s+int", "tmpfs_fops_close")
    truncate_body = function_body(source, r"auto\s+tmpfs_fops_truncate\(ker::vfs::File\*\s+f,\s*off_t\s+length\)\s*->\s+int", "tmpfs_fops_truncate")

    require_order(
        open_path,
        [
            "TmpNode* file_node = tmpfs_canonical_node(node)",
            "file_node->open_count.fetch_add(1, std::memory_order_relaxed)",
            "file_node->type == TmpNodeType::FILE",
            "tmpfs_resize_locked(file_node, 0)",
            "f->private_data = file_node",
            "f->is_directory = (file_node->type == TmpNodeType::DIRECTORY)",
        ],
        "tmpfs open canonical file node",
    )
    for body, context in [
        (read_body, "tmpfs read"),
        (write_body, "tmpfs write"),
        (close_body, "tmpfs close"),
        (truncate_body, "tmpfs truncate"),
    ]:
        require(body, "tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data))", context)


def test_vfs_link_uses_alias_not_copy() -> None:
    core = CORE_CPP.read_text()
    link_body = function_body(core, r"auto\s+vfs_link\(const\s+char\*\s+oldpath,\s*const\s+char\*\s+newpath\)\s*->\s+int", "vfs_link")
    stat_helper = function_body(core, r"void\s+fill_tmpfs_node_stat\(uint32_t\s+dev_id,\s*const\s+ker::vfs::tmpfs::TmpNode\*\s+node,\s*Stat\*\s+statbuf\)", "fill_tmpfs_node_stat")
    unlink_body = function_body(core, r"auto\s+vfs_unlink\(const\s+char\*\s+path\)\s*->\s+int", "vfs_unlink")

    require_order(
        link_body,
        [
            "src_node = ker::vfs::tmpfs::tmpfs_canonical_node(src_node)",
            "src_node->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY",
            "ker::vfs::tmpfs::tmpfs_lock_tree()",
            "ker::vfs::tmpfs::tmpfs_lookup(new_parent, new_name) != nullptr",
            "auto* link_node = ker::vfs::tmpfs::tmpfs_create_hardlink(new_parent, new_name, src_node)",
            "ker::vfs::tmpfs::tmpfs_unlock_tree()",
            "metadata_cache_note_path_changed(\"/\", nullptr)",
            "vfs_cache_notify_path_changed(old_buf.data(), new_buf.data())",
        ],
        "vfs_link tmpfs hard-link path",
    )
    require_absent(link_body, "tmpfs_copy_file_contents", "vfs_link must not copy file data")
    require_absent(link_body, "tmpfs_create_file(new_parent", "vfs_link must not create copied file")
    require_absent(link_body, "tmpfs_create_symlink(new_parent", "vfs_link must not create copied symlink")

    require_order(
        stat_helper,
        [
            "auto const* stat_node = ker::vfs::tmpfs::tmpfs_canonical_node(node)",
            "statbuf->st_ino = reinterpret_cast<ino_t>(stat_node)",
            "statbuf->st_nlink = ker::vfs::tmpfs::tmpfs_link_count(stat_node)",
            "statbuf->st_size = static_cast<off_t>(stat_node->size)",
        ],
        "tmpfs stat canonical inode",
    )
    require_order(
        unlink_body,
        [
            "bool const HARDLINK_COUNT_CHANGE = ker::vfs::tmpfs::tmpfs_link_count(child) > 1",
            "ker::vfs::tmpfs::tmpfs_detach_child(parent, child)",
            "ker::vfs::tmpfs::tmpfs_drop_detached_node(child)",
            "if (HARDLINK_COUNT_CHANGE)",
            "metadata_cache_note_path_changed(\"/\", nullptr)",
        ],
        "tmpfs unlink hard-link drop",
    )


def main() -> None:
    test_tmpfs_nodes_have_canonical_hardlink_state()
    test_tmpfs_hardlink_aliases_share_canonical_target()
    test_tmpfs_file_operations_use_canonical_node()
    test_vfs_link_uses_alias_not_copy()
    print("tmpfs hard-link source invariants hold")


if __name__ == "__main__":
    main()
