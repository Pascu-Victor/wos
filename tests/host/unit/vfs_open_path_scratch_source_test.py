#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def forbid(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


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


def require_only_uninitialized_array(body: str, name: str, expected: str, context: str) -> None:
    declarations = re.findall(rf"\bstd::array<[^;\n]+>\s+{re.escape(name)}\b[^;\n]*;", body)
    if declarations != [expected]:
        fail(f"{context}: unexpected {name} declarations: {declarations!r}")
    forbidden = [
        rf"\b{re.escape(name)}\s*=\s*\{{\s*\}}\s*;",
        rf"\b{re.escape(name)}\.fill\s*\(",
        rf"\b(?:std::)?memset\s*\(\s*{re.escape(name)}\.data\s*\(",
        rf"\bstd::fill\s*\(\s*{re.escape(name)}\.",
    ]
    if any(re.search(pattern, body) for pattern in forbidden):
        fail(f"{context}: {name} must not be cleared after declaration")


def test_dirfd_visible_scratch_is_initialized_by_root_strip() -> None:
    core = VFS_CORE_CPP.read_text()
    copy_path = function_body(core, "copy_path_string")
    strip_root = function_body(core, "strip_task_root_prefix")
    fast = function_body(core, "copy_common_local_dirfd_relative_path")
    slow = function_body(core, "resolve_dirfd_task_path_raw")
    selftest = function_body(core, "common_local_relative_resolver_fast_path_selftest_impl")

    declaration = "std::array<char, MAX_PATH_LEN> visible __attribute__((uninitialized));"
    require_only_uninitialized_array(fast, "visible", declaration, "fast dirfd visible scratch")
    require_only_uninitialized_array(slow, "visible", declaration, "fallback dirfd visible scratch")

    require_order(
        copy_path,
        [
            "size_t const LEN = known_src_len != UNKNOWN_PATH_LEN ? known_src_len : std::strlen(src)",
            "if (LEN + 1 > dst_size)",
            "std::memcpy(dst, src, LEN + 1)",
            "return 0",
        ],
        "bounded path string production",
    )
    if strip_root.count("return copy_path_string(") != 5:
        fail("task-root stripping must produce every successful output through copy_path_string")
    require_order(
        strip_root,
        [
            "const char* logical_path = path + ROOT_LEN",
            "if (*logical_path == '\\0')",
            'return copy_path_string("/", out, out_size)',
            "return copy_path_string(logical_path, out, out_size)",
        ],
        "exact-root visible path production",
    )

    require_order(
        fast,
        [
            "table_task->fd_table_lock.lock_irqsave()",
            declaration,
            "int const STRIP_RET = strip_task_root_prefix(task, base_file->vfs_path, visible.data(), visible.size(), nullptr)",
            "result = STRIP_RET < 0 ? STRIP_RET : copy_simple_relative_path_from_base(visible.data(), pathname, scan, out, outsize, out_len)",
            "table_task->fd_table_lock.unlock_irqrestore(IRQF)",
        ],
        "locked fast dirfd visible path production",
    )
    if fast.count("visible.data()") != 2 or fast.count("visible.size()") != 1 or "visible[" in fast:
        fail("fast dirfd visible scratch has an unexpected producer or consumer")

    require_order(
        slow,
        [
            declaration,
            "auto* file = vfs_get_file_retain(task, dirfd)",
            "int const STRIP_RET = strip_task_root_prefix(task, file->vfs_path, visible.data(), visible.size(), nullptr)",
            "vfs_put_file(file)",
            "if (STRIP_RET < 0)",
            "return STRIP_RET",
            "base = visible.data()",
            "std::strlen(base)",
        ],
        "fallback dirfd visible path production",
    )
    strip_failure = block_body_after(slow[slow.find("if (STRIP_RET < 0)") :], "if (STRIP_RET < 0)")
    if strip_failure.strip() != "return STRIP_RET;":
        fail("fallback dirfd resolution must return before consuming failed root-strip output")
    if slow.count("visible.data()") != 2 or slow.count("visible.size()") != 1 or "visible[" in slow:
        fail("fallback dirfd visible scratch has an unexpected producer or consumer")

    require_order(
        selftest,
        [
            "copy_path_string(DIR_PATH, rooted_task.root.data(), rooted_task.root.size())",
            "int const ROOTED_DIRFD = vfs_alloc_fd(&rooted_task, dir)",
            'ROOTED_DIRFD, ".", scan_path_text(".")',
            'FAST_ROOTED_DOT_PATH = "/tmp/ktest_common_local_relative_resolver/"',
            "resolved_len == std::strlen(FAST_ROOTED_DOT_PATH)",
            "resolved.at(resolved_len) == '\\0'",
            "std::strcmp(resolved.data(), FAST_ROOTED_DOT_PATH) == 0",
            'ROOTED_DIRFD, ".", resolved.data(), resolved.size(), false',
            'SLOW_ROOTED_DOT_PATH = "/tmp/ktest_common_local_relative_resolver/"',
            "resolved_len == std::strlen(SLOW_ROOTED_DOT_PATH)",
            "resolved.at(resolved_len) == '\\0'",
            "std::strcmp(resolved.data(), SLOW_ROOTED_DOT_PATH) == 0",
            "vfs_release_fd(&rooted_task, ROOTED_DIRFD)",
            "rooted_task.fd_table.empty()",
        ],
        "rooted real-dirfd exact-root KTEST coverage",
    )


def test_absolute_visible_scratch_is_initialized_by_dot_clean_producer() -> None:
    core = VFS_CORE_CPP.read_text()
    append = function_body(core, "append_dot_clean_path_components")
    pop = function_body(core, "pop_dot_clean_path_component")
    producer = function_body(core, "copy_dot_clean_visible_absolute_path")
    fast = function_body(core, "copy_common_local_visible_absolute_path_fast_path")
    selftest = function_body(core, "common_local_relative_resolver_fast_path_selftest_impl")

    declaration = "std::array<char, MAX_PATH_LEN> visible __attribute__((uninitialized));"
    require_only_uninitialized_array(fast, "visible", declaration, "absolute visible path scratch")
    require_order(
        producer,
        [
            "if (path == nullptr || out == nullptr || outsize < 2 || path[0] != '/')",
            "return RESOLVE_FAST_PATH_DECLINED",
            "out[0] = '/'",
            "out[1] = '\\0'",
            "size_t len = 1",
            "int const RET = append_dot_clean_path_components(path, scan, 1, out, &len, outsize)",
            "if (RET == 0 && out_len != nullptr)",
            "*out_len = len",
            "return RET",
        ],
        "dot-clean absolute path production",
    )
    require_order(
        append,
        [
            "size_t cursor = start_pos",
            "while (cursor < scan.path_len)",
            "if (COMPONENT_LEN == 2 && path[COMPONENT_START] == '.' && path[COMPONENT_START + 1] == '.')",
            "pop_dot_clean_path_component(out, out_len)",
            "size_t pos = *out_len",
            "std::memcpy(out + pos, path + COMPONENT_START, COMPONENT_LEN)",
            "pos += COMPONENT_LEN",
            "out[pos] = '\\0'",
            "*out_len = pos",
            "return 0",
        ],
        "dot-clean component append maintains a terminated initialized prefix",
    )
    require_order(
        pop,
        [
            "if (out == nullptr || out_len == nullptr || *out_len <= 1)",
            "out[0] = '/'",
            "out[1] = '\\0'",
            "*out_len = 1",
            "size_t pos = *out_len",
            "while (pos > 1 && out[pos - 1] == '/')",
            "while (pos > 1 && out[pos - 1] != '/')",
            "out[pos - 1] = '\\0'",
            "*out_len = pos - 1",
        ],
        "dot-clean parent traversal stays within the initialized prefix",
    )
    require_order(
        fast,
        [
            "if (scan.needs_canonicalize)",
            declaration,
            "size_t visible_len = UNKNOWN_PATH_LEN",
            "int const DOT_CLEAN_RET = copy_dot_clean_visible_absolute_path(path, scan, visible.data(), visible.size(), &visible_len)",
            "if (DOT_CLEAN_RET != 0)",
            "return DOT_CLEAN_RET",
            "if (!common_local_visible_path_is_noop(visible.data()))",
            "copy_task_visible_absolute_path_with_root(task, visible.data(), visible_len, out, outsize, RESOLVED_LEN_OUT)",
        ],
        "absolute visible path producer and consumer ordering",
    )
    producer_failure = block_body_after(fast[fast.find("if (DOT_CLEAN_RET != 0)") :], "if (DOT_CLEAN_RET != 0)")
    if producer_failure.strip() != "return DOT_CLEAN_RET;":
        fail("absolute visible path production must return before consuming failed output")
    if fast.count("visible.data()") != 3 or fast.count("visible.size()") != 1 or "visible[" in fast:
        fail("absolute visible path scratch has an unexpected producer or consumer")
    if len(re.findall(r"\bvisible\b", fast)) != 5:
        fail("absolute visible path scratch must only appear in its declaration, producer, and two consumers")
    require_order(
        selftest,
        [
            "resolved.fill('x')",
            'copy_common_local_visible_absolute_path_fast_path(&task, "/..", scan_path_text("/..")',
            "resolved_len == 1",
            "resolved.at(resolved_len) == '\\0'",
            'std::strcmp(resolved.data(), "/") == 0',
            'metadata_path_hash_raw("/", 1)',
        ],
        "canonicalize-to-root KTEST coverage",
    )
    poison = "resolved.fill('x');"
    root_call = 'ret = copy_common_local_visible_absolute_path_fast_path(&task, "/..", scan_path_text("/..")'
    poison_end = selftest.find(poison) + len(poison)
    root_call_pos = selftest.find(root_call, poison_end)
    if poison_end < len(poison) or root_call_pos < 0 or selftest[poison_end:root_call_pos].strip():
        fail("canonicalize-to-root destination poison must immediately precede the producer call")


def test_prefix_symlink_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()
    resolve_prefix = function_body(core, "resolve_prefix_symlink_once")
    splice = function_body(core, "splice_symlink_target")
    canonicalize = function_body(core, "canonicalize_path")
    copy_path = function_body(core, "copy_path_string")
    path_helper_selftest = function_body(core, "vfs_selftest_path_text_scan_matches_helpers")
    access_selftest = function_body(core, "vfs_selftest_faccessat_flags")

    linkbuf_declaration = "std::array<char, MAX_PATH_LEN> linkbuf __attribute__((uninitialized));"
    substituted_declaration = "std::array<char, MAX_PATH_LEN> substituted __attribute__((uninitialized));"
    require_only_uninitialized_array(resolve_prefix, "linkbuf", linkbuf_declaration, "prefix readlink scratch")
    require_only_uninitialized_array(resolve_prefix, "substituted", substituted_declaration, "prefix substitution scratch")

    require_order(
        resolve_prefix,
        [
            linkbuf_declaration,
            "path[end] = '\\0'",
            "readlink_resolved_on_mount(path, linkbuf.data(), linkbuf.size() - 1, current_path_mount, end)",
            "readlink_resolved(path, linkbuf.data(), linkbuf.size() - 1, end)",
            "path[end] = CH",
            "if (LINK_LEN > 0)",
            "if (static_cast<size_t>(LINK_LEN) >= linkbuf.size())",
            "return -ENAMETOOLONG",
            "linkbuf[LINK_LEN] = '\\0'",
            substituted_declaration,
            "int const SPLICE_RESULT = splice_symlink_target(path, end, linkbuf.data(), substituted.data(), substituted.size())",
            "if (SPLICE_RESULT < 0)",
            "return SPLICE_RESULT",
            "int const COPY_RESULT = copy_path_string(substituted.data(), path, bufsize)",
            "if (COPY_RESULT < 0)",
            "return COPY_RESULT",
            "if (linkbuf[0] == '/')",
        ],
        "prefix symlink scratch producer and consumer ordering",
    )
    positive = block_body_after(resolve_prefix, "if (LINK_LEN > 0)")
    length_failure = block_body_after(positive, "if (static_cast<size_t>(LINK_LEN) >= linkbuf.size())")
    splice_failure = block_body_after(positive[positive.find("if (SPLICE_RESULT < 0)") :], "if (SPLICE_RESULT < 0)")
    copy_failure = block_body_after(positive[positive.find("if (COPY_RESULT < 0)") :], "if (COPY_RESULT < 0)")
    if (
        length_failure.strip() != "return -ENAMETOOLONG;"
        or splice_failure.strip() != "return SPLICE_RESULT;"
        or copy_failure.strip() != "return COPY_RESULT;"
    ):
        fail("prefix symlink partial output must not be consumed after a producer failure")
    if (
        resolve_prefix.count("linkbuf.data()") != 3
        or resolve_prefix.count("linkbuf.size() - 1") != 2
        or resolve_prefix.count("linkbuf[") != 2
        or len(re.findall(r"\blinkbuf\b", resolve_prefix)) != 9
        or len(re.findall(r"\blinkbuf\b", positive)) != 4
    ):
        fail("prefix readlink scratch has an unexpected producer or consumer")
    if (
        resolve_prefix.count("substituted.data()") != 2
        or resolve_prefix.count("substituted.size()") != 1
        or "substituted[" in resolve_prefix
        or len(re.findall(r"\bsubstituted\b", resolve_prefix)) != 4
    ):
        fail("prefix substitution scratch has an unexpected producer or consumer")

    require_order(
        splice,
        [
            "size_t const REMAINDER_LEN = std::strlen(remainder)",
            "size_t const TARGET_LEN = std::strlen(target)",
            "if (target[0] == '/')",
            "std::memcpy(out, target, TARGET_LEN)",
            "std::memcpy(out, original_path, parent_len)",
            "std::memcpy(out + pos, target, TARGET_LEN)",
            "if (REMAINDER_LEN > 0)",
            "return canonicalize_path(out, out_size)",
        ],
        "symlink target splice construction",
    )
    if len(re.findall(r"\bout\b", splice)) != 11:
        fail("symlink target splice has an unexpected output-buffer producer or consumer")
    forbid(
        splice,
        ["memset", "bzero", "std::fill", "std::ranges::fill", "fill_n"],
        "symlink target splice output must not be bulk-cleared",
    )
    remainder_tail = splice[splice.find("if (REMAINDER_LEN > 0)") :]
    remainder_body = block_body_after(remainder_tail, "if (REMAINDER_LEN > 0)")
    else_pos = remainder_tail.find("} else {")
    if else_pos < 0:
        fail("symlink target splice is missing its empty-remainder branch")
    empty_remainder_body = block_body_after(remainder_tail[else_pos + 2 :], "else")
    require_order(
        remainder_body,
        [
            "if (pos == 0 || out[pos - 1] != '/')",
            "if (pos + REMAINDER_LEN + 1 > out_size)",
            "std::memcpy(out + pos, remainder, REMAINDER_LEN + 1)",
        ],
        "symlink splice remainder termination",
    )
    forbid(remainder_body, ["out[pos] = '\\0'"], "nonempty symlink remainder termination")
    require_order(
        empty_remainder_body,
        ["if (pos >= out_size)", "return -ENAMETOOLONG", "out[pos] = '\\0'"],
        "empty symlink remainder termination",
    )
    forbid(empty_remainder_body, ["REMAINDER_LEN + 1"], "empty symlink remainder production")
    returns = re.findall(r"\breturn\s+([^;]+);", splice)
    if (
        returns[-1:] != ["canonicalize_path(out, out_size)"]
        or returns.count("canonicalize_path(out, out_size)") != 1
        or any(result not in {"-EINVAL", "-ENAMETOOLONG", "canonicalize_path(out, out_size)"} for result in returns)
    ):
        fail("symlink target splice must not report success before producing and canonicalizing a complete string")
    require_order(
        canonicalize,
        [
            "for (; path_len < bufsize && path[path_len] != '\\0'; ++path_len)",
            "if (path_len == bufsize)",
            "if (!needs_rewrite)",
            "result[pos] = '\\0'",
            "std::memcpy(path, static_cast<const char*>(result), pos + 1)",
            "return 0",
        ],
        "canonicalized splice preserves a complete terminated string",
    )
    require_order(
        copy_path,
        [
            "size_t const LEN = known_src_len != UNKNOWN_PATH_LEN ? known_src_len : std::strlen(src)",
            "if (LEN + 1 > dst_size)",
            "std::memcpy(dst, src, LEN + 1)",
            "return 0",
        ],
        "successful substitution copy",
    )

    require_order(
        path_helper_selftest,
        [
            "spliced.fill('x')",
            'splice_symlink_target("/base/link/tail", sizeof("/base/link") - 1, "../target/./dir"',
            'RELATIVE_EXPECTED = "/target/dir/tail"',
            "spliced.at(std::strlen(RELATIVE_EXPECTED)) != '\\0'",
            "spliced.fill('x')",
            'splice_symlink_target("/base/link", sizeof("/base/link") - 1, "/"',
            "spliced.at(1) != '\\0'",
            "spliced.fill('x')",
            'splice_symlink_target("/base/link", sizeof("/base/link") - 1, "child/../leaf"',
            'RELATIVE_FINAL_EXPECTED = "/base/leaf"',
            "spliced.at(std::strlen(RELATIVE_FINAL_EXPECTED)) != '\\0'",
            "spliced.fill('x')",
            'splice_symlink_target("/base/link/tail", sizeof("/base/link") - 1, "/absolute/./dir"',
            'ABSOLUTE_REMAINDER_EXPECTED = "/absolute/dir/tail"',
            "spliced.at(std::strlen(ABSOLUTE_REMAINDER_EXPECTED)) != '\\0'",
        ],
        "poisoned symlink splice KTEST coverage",
    )
    poison = "spliced.fill('x');"
    splice_calls = [
        'int splice_ret = splice_symlink_target("/base/link/tail", sizeof("/base/link") - 1, "../target/./dir", spliced.data(), spliced.size());',
        'splice_ret = splice_symlink_target("/base/link", sizeof("/base/link") - 1, "/", spliced.data(), spliced.size());',
        'splice_ret = splice_symlink_target("/base/link", sizeof("/base/link") - 1, "child/../leaf", spliced.data(), spliced.size());',
        'splice_ret = splice_symlink_target("/base/link/tail", sizeof("/base/link") - 1, "/absolute/./dir", spliced.data(), spliced.size());',
    ]
    if path_helper_selftest.count("spliced.fill(") != len(splice_calls) or path_helper_selftest.count(poison) != len(splice_calls):
        fail("every symlink splice coverage case must preserve its poisoned destination")
    cursor = 0
    for splice_call in splice_calls:
        poison_pos = path_helper_selftest.find(poison, cursor)
        splice_pos = path_helper_selftest.find(splice_call, poison_pos + len(poison))
        if poison_pos < 0 or splice_pos < 0 or path_helper_selftest[poison_pos + len(poison) : splice_pos].strip():
            fail("symlink splice destination poison must immediately precede each producer call")
        cursor = splice_pos + len(splice_call)
    require_order(
        access_selftest,
        [
            "vfs_symlink(MISSING_TARGET, LINK_PATH)",
            "vfs_cache_notify_path_changed(LINK_PATH, nullptr)",
            "vfs_faccessat(&task, AT_FDCWD, LINK_PATH, 0, 0) == -ENOENT",
        ],
        "uncached positive tmpfs prefix-symlink KTEST coverage",
    )


def test_readdir_child_path_scratch_is_initialized_by_its_producer() -> None:
    core = VFS_CORE_CPP.read_text()
    builder = function_body(core, "build_readdir_child_path")
    seed = function_body(core, "vfs_seed_readdir_entry_cache_hints")
    selftest = function_body(core, "vfs_selftest_readdir_seeds_non_symlink_hints")

    declaration = "std::array<char, MAX_PATH_LEN> child_path __attribute__((uninitialized));"
    require_only_uninitialized_array(seed, "child_path", declaration, "readdir child-path scratch")
    require_order(
        seed,
        [
            "if (ENTRY_TYPE == DT_UNKNOWN || ENTRY_TYPE == DT_LNK)",
            declaration,
            "size_t child_path_len = 0",
            "if (!build_readdir_child_path(dir, entry, child_path, &child_path_len) ||",
            "!path_is_under_mount(mount, child_path.data(), child_path_len))",
            "return",
            "metadata_cache_prepare_path_observation(child_path.data()",
            "metadata_path_hash_raw(child_path.data(), child_path_len)",
        ],
        "readdir child-path producer and consumer ordering",
    )
    build_failure = block_body_after(seed, "if (!build_readdir_child_path(dir, entry, child_path, &child_path_len) ||")
    if build_failure.strip() != "return;":
        fail("failed readdir child-path production must return before consuming scratch")
    if len(re.findall(r"\bchild_path\b", seed)) != 13:
        fail("readdir child-path scratch has an unexpected producer or consumer")

    require_order(
        builder,
        [
            "size_t const NAME_LEN = dirent_name_length(entry)",
            "for (size_t i = 0; i < NAME_LEN; ++i)",
            "size_t parent_len = file_vfs_path_len(dir)",
            "size_t const SEP_LEN = (parent_len == 1 && dir->vfs_path[0] == '/') ? 0 : 1",
            "size_t const CHILD_LEN = parent_len + SEP_LEN + NAME_LEN",
            "if (CHILD_LEN == 0 || CHILD_LEN >= out.size())",
            "std::memcpy(out.data(), dir->vfs_path, parent_len)",
            "if (SEP_LEN != 0)",
            "out.at(pos++) = '/'",
            "std::memcpy(out.data() + pos, entry.d_name.data(), NAME_LEN)",
            "out.at(CHILD_LEN) = '\\0'",
            "*path_len_out = CHILD_LEN",
            "return true",
        ],
        "bounded readdir child-path construction",
    )
    first_output_write = builder.find("std::memcpy(out.data(), dir->vfs_path, parent_len)")
    if first_output_write < 0:
        fail("readdir child-path builder is missing its first output write")
    expected_success_tail = """std::memcpy(out.data(), dir->vfs_path, parent_len);
    size_t pos = parent_len;
    if (SEP_LEN != 0) {
        out.at(pos++) = '/';
    }
    std::memcpy(out.data() + pos, entry.d_name.data(), NAME_LEN);
    out.at(CHILD_LEN) = '\\0';
    *path_len_out = CHILD_LEN;
    return true;"""
    if builder[first_output_write:].strip() != expected_success_tail:
        fail("readdir child-path success must remain one complete straight-line production after its optional separator")
    returns = re.findall(r"\breturn\s+([^;]+);", builder)
    if returns != ["false", "false", "false", "false", "false", "true"]:
        fail("readdir child-path builder must reject all failures before writing output and have one final success exit")
    if len(re.findall(r"\bout\b", builder)) != 5:
        fail("readdir child-path builder has an unexpected success exit or output-buffer use")
    forbid(
        builder,
        ["memset", "bzero", "std::fill", "std::ranges::fill", "fill_n"],
        "readdir child-path output must not be bulk-cleared",
    )

    require_order(
        selftest,
        [
            "built_child_path.fill('x')",
            "BUILT_CHILD_PATH = build_readdir_child_path(dir, found, built_child_path, &built_child_path_len)",
            "built_child_path_len == CHILD_PATH_LEN",
            "built_child_path.at(CHILD_PATH_LEN) == '\\0'",
            "root_dir.vfs_path = \"/\"",
            "root_dir.vfs_path_len = 1",
            "built_child_path.fill('x')",
            "BUILT_ROOT_CHILD_PATH = build_readdir_child_path(&root_dir, root_entry, built_child_path, &built_child_path_len)",
            "built_child_path_len == 2",
            "built_child_path.at(2) == '\\0'",
            "UNCHANGED_PATH_LEN = MAX_PATH_LEN",
            "built_child_path.fill('x')",
            "BUILT_INVALID_CHILD_PATH = build_readdir_child_path(dir, invalid_entry, built_child_path, &built_child_path_len)",
            "built_child_path_len == UNCHANGED_PATH_LEN",
            "built_child_path.front() == 'x'",
            "built_child_path.back() == 'x'",
        ],
        "poisoned readdir child-path KTEST coverage",
    )
    poison = "built_child_path.fill('x');"
    coverage_cases = [
        (
            "bool const BUILT_CHILD_PATH = build_readdir_child_path(dir, found, built_child_path, &built_child_path_len);",
            "ok = ok && BUILT_CHILD_PATH && built_child_path_len == CHILD_PATH_LEN",
        ),
        (
            "bool const BUILT_ROOT_CHILD_PATH = build_readdir_child_path(&root_dir, root_entry, built_child_path, &built_child_path_len);",
            "ok = ok && BUILT_ROOT_CHILD_PATH && built_child_path_len == 2",
        ),
        (
            "bool const BUILT_INVALID_CHILD_PATH = build_readdir_child_path(dir, invalid_entry, built_child_path, &built_child_path_len);",
            "ok = ok && !BUILT_INVALID_CHILD_PATH && built_child_path_len == UNCHANGED_PATH_LEN",
        ),
    ]
    if (
        selftest.count("built_child_path.fill(") != len(coverage_cases)
        or selftest.count(poison) != len(coverage_cases)
        or len(re.findall(r"\bbuilt_child_path\b", selftest)) != 13
        or len(re.findall(r"\bbuilt_child_path_len\b", selftest)) != 9
    ):
        fail("every readdir child-path coverage case must preserve its poisoned destination")
    cursor = 0
    for builder_call, verification in coverage_cases:
        poison_pos = selftest.find(poison, cursor)
        call_pos = selftest.find(builder_call, poison_pos + len(poison))
        if poison_pos < 0 or call_pos < 0 or selftest[poison_pos + len(poison) : call_pos].strip():
            fail("readdir child-path destination poison must immediately precede each producer call")
        call_end = call_pos + len(builder_call)
        verification_pos = selftest.find(verification, call_end)
        if verification_pos < 0 or selftest[call_end:verification_pos].strip():
            fail("readdir child-path producer result must be verified before any destination mutation")
        cursor = verification_pos + len(verification)


def test_synthetic_readdir_visible_paths_are_initialized_by_root_strip() -> None:
    core = VFS_CORE_CPP.read_text()
    readdir = function_body(core, "vfs_read_dir_entries")
    strip_current = function_body(core, "strip_current_task_root_prefix")
    strip_task = function_body(core, "strip_task_root_prefix")
    copy_path = function_body(core, "copy_path_string")
    selftest = function_body(core, "vfs_selftest_readdir_seeds_non_symlink_hints")

    declarations = {
        "visible_dir_path": "char visible_dir_path[MAX_PATH_LEN] __attribute__((uninitialized));",
        "visible_mount_path": "char visible_mount_path[MAX_PATH_LEN] __attribute__((uninitialized));",
        "visible_mount_path2": "char visible_mount_path2[MAX_PATH_LEN] __attribute__((uninitialized));",
    }
    for name, declaration in declarations.items():
        array_declarations = re.findall(rf"\bchar\s+{name}\s*\[[^;\n]+;", readdir)
        if array_declarations != [declaration]:
            fail(f"synthetic readdir path: unexpected {name} declarations: {array_declarations!r}")
        forbid(
            readdir,
            [
                f"{name}[MAX_PATH_LEN] = {{}}",
                f"memset({name}",
                f"std::fill({name}",
                f"std::ranges::fill({name}",
            ],
            f"synthetic readdir redundant {name} initialization",
        )

    require_order(
        readdir,
        [
            declarations["visible_dir_path"],
            "strip_current_task_root_prefix(f->vfs_path, visible_dir_path, sizeof(visible_dir_path))",
            "break",
            "std::strcmp(visible_dir_path, \"/\")",
            "size_t const DIR_LEN = std::strlen(visible_dir_path)",
            declarations["visible_mount_path"],
            "strip_current_task_root_prefix(mount_snapshot.path, visible_mount_path, sizeof(visible_mount_path))",
            "continue",
            "size_t const MP_LEN = std::strlen(visible_mount_path)",
            declarations["visible_mount_path2"],
            "strip_current_task_root_prefix(mount_snapshot2.path, visible_mount_path2, sizeof(visible_mount_path2))",
            "continue",
            "size_t const MP2_LEN = std::strlen(visible_mount_path2)",
        ],
        "synthetic readdir visible-path producer and consumer ordering",
    )
    failure_guards = [
        (
            "if (strip_current_task_root_prefix(f->vfs_path, visible_dir_path, sizeof(visible_dir_path)) < 0)",
            "break;",
        ),
        (
            "if (strip_current_task_root_prefix(mount_snapshot.path, visible_mount_path, sizeof(visible_mount_path)) < 0)",
            "continue;",
        ),
        (
            "if (strip_current_task_root_prefix(mount_snapshot2.path, visible_mount_path2, sizeof(visible_mount_path2)) < 0)",
            "continue;",
        ),
    ]
    for header, expected_body in failure_guards:
        if block_body_after(readdir, header).strip() != expected_body:
            fail(f"synthetic readdir must not consume a failed visible-path production: {header}")
    expected_uses = {"visible_dir_path": 13, "visible_mount_path": 9, "visible_mount_path2": 9}
    for name, count in expected_uses.items():
        if len(re.findall(rf"\b{name}\b", readdir)) != count:
            fail(f"synthetic readdir path has an unexpected {name} producer or consumer")

    copy_returns = re.findall(r"\breturn\s+([^;]+);", copy_path)
    if copy_returns != ["-EINVAL", "-ENAMETOOLONG", "0"]:
        fail("bounded path copy must only succeed after copying the complete string")
    expected_copy_body = """if (src == nullptr || dst == nullptr || dst_size == 0) {
        return -EINVAL;
    }

    size_t const LEN = known_src_len != UNKNOWN_PATH_LEN ? known_src_len : std::strlen(src);
    if (LEN + 1 > dst_size) {
        return -ENAMETOOLONG;
    }

    std::memcpy(dst, src, LEN + 1);
    if (copied_len_out != nullptr) {
        *copied_len_out = LEN;
    }
    return 0;"""
    if copy_path.strip() != expected_copy_body:
        fail("bounded path copy must remain one complete straight-line string production")
    require_order(
        copy_path,
        [
            "size_t const LEN = known_src_len != UNKNOWN_PATH_LEN ? known_src_len : std::strlen(src)",
            "if (LEN + 1 > dst_size)",
            "std::memcpy(dst, src, LEN + 1)",
            "return 0",
        ],
        "synthetic readdir bounded path production",
    )
    forbid(copy_path, ["memset", "bzero", "std::fill", "std::ranges::fill", "fill_n"], "bounded path copy bulk clear")

    strip_task_returns = re.findall(r"\breturn\s+([^;]+);", strip_task)
    if strip_task_returns != [
        "-EINVAL",
        "copy_path_string(path, out, out_size)",
        "copy_path_string(path, out, out_size)",
        "copy_path_string(path, out, out_size)",
        'copy_path_string("/", out, out_size)',
        "copy_path_string(logical_path, out, out_size)",
    ]:
        fail("task-root stripping must produce every successful output through bounded path copy")
    if len(re.findall(r"\bout\b", strip_task)) != 6:
        fail("task-root stripping has an unexpected destination access")
    strip_current_returns = re.findall(r"\breturn\s+([^;]+);", strip_current)
    if strip_current_returns != [
        "-EINVAL",
        "copy_path_string(path, out, out_size)",
        "strip_task_root_prefix(task, path, out, out_size, nullptr)",
    ]:
        fail("current-task root stripping must produce every successful output through the proven producers")
    if len(re.findall(r"\bout\b", strip_current)) != 3:
        fail("current-task root stripping has an unexpected destination access")
    forbid(
        strip_task + strip_current,
        ["memset", "bzero", "std::fill", "std::ranges::fill", "fill_n"],
        "task-root stripping destinations must not be bulk-cleared",
    )

    require_order(
        selftest,
        [
            "visible_path.fill('x')",
            "VISIBLE_PATH_RET = strip_task_root_prefix(nullptr, DIR_PATH, visible_path.data(), visible_path.size(), nullptr)",
            "std::strcmp(visible_path.data(), DIR_PATH) == 0",
            "visible_path.at(std::strlen(DIR_PATH)) == '\\0'",
            'copy_path_string("/tmp", rooted_task.root.data(), rooted_task.root.size())',
            'ROOTED_VISIBLE_PATH = "/ktest_readdir_hints"',
            "visible_path.fill('x')",
            "ROOTED_VISIBLE_PATH_RET =",
            "strip_task_root_prefix(&rooted_task, DIR_PATH, visible_path.data(), visible_path.size(), nullptr)",
            "visible_path.at(std::strlen(ROOTED_VISIBLE_PATH)) == '\\0'",
        ],
        "poisoned synthetic readdir visible-path KTEST coverage",
    )
    poison = "visible_path.fill('x');"
    coverage_cases = [
        (
            "int const VISIBLE_PATH_RET = strip_task_root_prefix(nullptr, DIR_PATH, visible_path.data(), visible_path.size(), nullptr);",
            "ok = ok && VISIBLE_PATH_RET == 0",
        ),
        (
            "int const ROOTED_VISIBLE_PATH_RET = strip_task_root_prefix(&rooted_task, DIR_PATH, visible_path.data(), visible_path.size(), nullptr);",
            "ok = ok && ROOTED_VISIBLE_PATH_RET == 0",
        ),
    ]
    if (
        selftest.count("visible_path.fill(") != len(coverage_cases)
        or selftest.count(poison) != len(coverage_cases)
        or len(re.findall(r"\bvisible_path\b", selftest)) != 11
    ):
        fail("every synthetic readdir visible-path case must preserve its poisoned destination")
    cursor = 0
    for call, verification in coverage_cases:
        poison_pos = selftest.find(poison, cursor)
        call_pos = selftest.find(call, poison_pos + len(poison))
        if poison_pos < 0 or call_pos < 0 or selftest[poison_pos + len(poison) : call_pos].strip():
            fail("synthetic readdir visible-path poison must immediately precede each producer call")
        call_end = call_pos + len(call)
        verification_pos = selftest.find(verification, call_end)
        if verification_pos < 0 or selftest[call_end:verification_pos].strip():
            fail("synthetic readdir visible-path output must be verified before any destination mutation")
        cursor = verification_pos + len(verification)


def test_stat_trailing_slash_scratch_is_initialized_by_trim_producer() -> None:
    core = VFS_CORE_CPP.read_text()
    producer = function_body(core, "copy_trailing_slash_trimmed_path")
    fast_path = function_body(core, "vfs_stat_absolute_local_fast_path")
    selftest = function_body(core, "vfs_selftest_absolute_local_stat_fast_path_gate")

    declaration = "std::array<char, MAX_PATH_LEN> trimmed __attribute__((uninitialized));"
    require_only_uninitialized_array(fast_path, "trimmed", declaration, "trailing-slash stat scratch")
    require_order(
        fast_path,
        [
            "task_absolute_local_trailing_slash_direct_allowed(task, path, SCAN)",
            declaration,
            "if (!copy_trailing_slash_trimmed_path",
            "*result_out = -ENAMETOOLONG",
            "return true",
            "vfs_stat_resolved_cache_or_impl(trimmed.data()",
        ],
        "trailing-slash stat scratch producer and consumer ordering",
    )
    failure = block_body_after(
        fast_path[fast_path.find("if (!copy_trailing_slash_trimmed_path") :],
        "if (!copy_trailing_slash_trimmed_path",
    )
    if failure.strip() != "*result_out = -ENAMETOOLONG;\n            return true;":
        fail("trailing-slash stat fast path must return before consuming failed scratch output")
    if len(re.findall(r"\btrimmed\b", fast_path)) != 3:
        fail("trailing-slash stat scratch has an unexpected producer or consumer")

    require_order(
        producer,
        [
            "if (path == nullptr || scan.normalized_len == 0 || scan.normalized_len >= out.size())",
            "return false",
            "std::memcpy(out.data(), path, scan.normalized_len)",
            "out.at(scan.normalized_len) = '\\0'",
            "return true",
        ],
        "bounded trailing-slash path production",
    )
    if len(re.findall(r"\bout\b", producer)) != 3:
        fail("trailing-slash path producer destination flow changed; re-audit complete initialization")
    if producer.count("return false;") != 1 or producer.count("return true;") != 1 or len(re.findall(r"\breturn\b", producer)) != 2:
        fail("trailing-slash path producer must have no success exit before complete production")
    forbid(
        producer,
        [
            "out.fill(",
            "std::memset(out.data()",
            "memset(out.data()",
            "std::fill(out.",
            "std::ranges::fill(out.",
        ],
        "trailing-slash path producer redundant destination clearing",
    )

    poison = "trimmed.fill('x');"
    if selftest.count("\n    trimmed.fill('x');\n") != 1:
        fail("trailing-slash stat destination poison must be one unconditional complete statement")
    verification = (
        'if (!copy_trailing_slash_trimmed_path("/tmp/file/", trailing_scan, trimmed) || '
        'std::strcmp(trimmed.data(), "/tmp/file") != 0 ||\n'
        "        trimmed.at(trailing_scan.normalized_len) != '\\0') {\n"
        "        return false;\n"
        "    }"
    )
    poison_pos = selftest.find(poison)
    verification_pos = selftest.find(verification, poison_pos + len(poison))
    if poison_pos < 0 or verification_pos < 0 or selftest[poison_pos + len(poison) : verification_pos].strip():
        fail("trailing-slash stat poison must immediately precede exact content and terminator verification")
    if len(re.findall(r"\btrimmed\b", selftest)) != 5:
        fail("trailing-slash stat KTEST has unexpected scratch use")


def test_statat_scratch_is_initialized_by_dirfd_resolver() -> None:
    core = VFS_CORE_CPP.read_text()
    statat = function_body(core, "vfs_statat")
    resolver = function_body(core, "resolve_dirfd_task_path_raw")
    fast_resolver = function_body(core, "resolve_dirfd_task_path_raw_common_local_fast_path")
    selftest = function_body(core, "vfs_selftest_statat_root_cwd_relative_paths")

    declaration = "std::array<char, MAX_PATH_LEN> resolved __attribute__((uninitialized));"
    require_only_uninitialized_array(statat, "resolved", declaration, "statat resolved-path scratch")
    require_order(
        statat,
        [
            "vfs_stat_absolute_local_fast_path(task, pathname, FOLLOW_FINAL_SYMLINK, statbuf, &fast_result)",
            "return fast_result",
            declaration,
            "int const RESOLVE_RET = resolve_dirfd_task_path_raw(task, dirfd, pathname, resolved.data(), resolved.size(), true,",
            "if (RESOLVE_RET < 0)",
            "return RESOLVE_RET",
            "resolved_task_path_is_wki_entry(task, resolved.data())",
            "maybe_ensure_wki_host_root_mount_for_task(task, resolved.data())",
            "vfs_stat_resolved_cache_or_impl(resolved.data()",
        ],
        "statat resolved-path producer and consumer ordering",
    )
    failure = block_body_after(statat[statat.find("if (RESOLVE_RET < 0)") :], "if (RESOLVE_RET < 0)")
    if failure.strip() != "return RESOLVE_RET;":
        fail("statat must return before consuming failed resolved-path output")
    if len(re.findall(r"\bresolved\b", statat)) != 6:
        fail("statat resolved-path scratch has an unexpected producer or consumer")

    if len(re.findall(r"\bout\b", resolver)) != 16 or len(re.findall(r"\boutsize\b", resolver)) != 7:
        fail("dirfd resolver destination flow changed; re-audit for redundant whole-buffer clearing")
    resolver_clear_patterns = [
        r"\b(?:std::)?memset\s*\(\s*out\b",
        r"\bstd::(?:ranges::)?fill(?:_n)?\s*\(\s*out\b",
        r"\bout\s*\[[^]]+\]\s*=\s*(?:0|'\\0')",
    ]
    if any(re.search(pattern, resolver) for pattern in resolver_clear_patterns):
        fail("dirfd resolver must not clear its complete destination before bounded production")
    if len(re.findall(r"\bout\b", fast_resolver)) != 9 or len(re.findall(r"\boutsize\b", fast_resolver)) != 4:
        fail("fast dirfd resolver destination flow changed; re-audit for redundant whole-buffer clearing")
    if any(re.search(pattern, fast_resolver) for pattern in resolver_clear_patterns):
        fail("fast dirfd resolver must not clear its complete destination before bounded production")

    fast_producer_flows = {
        "copy_common_local_dirfd_relative_path": (4, 4),
        "copy_common_local_visible_absolute_path_fast_path": (5, 3),
        "copy_task_visible_absolute_path_with_root": (5, 3),
        "copy_simple_relative_path_from_base": (9, 4),
        "copy_dot_clean_visible_absolute_path": (4, 2),
        "append_dot_clean_path_components": (5, 4),
    }
    bulk_clear_patterns = resolver_clear_patterns[:2]
    for producer, (out_count, outsize_count) in fast_producer_flows.items():
        body = function_body(core, producer)
        if len(re.findall(r"\bout\b", body)) != out_count or len(re.findall(r"\boutsize\b", body)) != outsize_count:
            fail(f"{producer} destination flow changed; re-audit for redundant whole-buffer clearing")
        if any(re.search(pattern, body) for pattern in bulk_clear_patterns):
            fail(f"{producer} must not bulk-clear its destination before bounded production")

    require_order(
        selftest,
        [
            "resolved.fill('x')",
            "RESOLVE_RET = resolve_dirfd_task_path_raw(&task, AT_FDCWD, CANONICALIZED_RELATIVE, resolved.data(), resolved.size(), true,",
            "RESOLVE_RET == 0",
            "resolved_len == std::strlen(FILE_PATH)",
            "std::strcmp(resolved.data(), FILE_PATH) == 0",
            "resolved.at(resolved_len) == '\\0'",
            "vfs_statat(&task, AT_FDCWD, CANONICALIZED_RELATIVE",
        ],
        "poisoned statat resolver KTEST coverage",
    )
    poison = "resolved.fill('x');"
    if selftest.count("\n    resolved.fill('x');\n") != 1:
        fail("statat resolver destination poison must be one unconditional complete statement")
    call = "int const RESOLVE_RET = resolve_dirfd_task_path_raw(&task, AT_FDCWD, CANONICALIZED_RELATIVE, resolved.data(), resolved.size(), true,\n" \
        "                                                        &requires_directory, &resolved_len);"
    poison_pos = selftest.find(poison)
    call_pos = selftest.find(call, poison_pos + len(poison))
    if poison_pos < 0 or call_pos < 0 or selftest[poison_pos + len(poison) : call_pos].strip():
        fail("statat resolver destination poison must immediately precede the producer call")
    verification = (
        "bool ok = RESOLVE_RET == 0 && !requires_directory && resolved_len == std::strlen(FILE_PATH) &&\n"
        "              std::strcmp(resolved.data(), FILE_PATH) == 0 && resolved.at(resolved_len) == '\\0';"
    )
    call_end = call_pos + len(call)
    verification_pos = selftest.find(verification, call_end)
    if verification_pos < 0 or selftest[call_end:verification_pos].strip():
        fail("statat resolver output must be verified before any destination mutation")
    expected_selftest_uses = {
        "resolved": 6,
        "resolved_len": 4,
        "RESOLVE_RET": 2,
        "requires_directory": 3,
    }
    for name, count in expected_selftest_uses.items():
        if len(re.findall(rf"\b{re.escape(name)}\b", selftest)) != count:
            fail(f"statat resolver KTEST has unexpected {name} use")

    functional_checks = [
        "ok = ok && vfs_statat(&task, AT_FDCWD, SIMPLE_RELATIVE, 0, &st) == 0 && "
        "(st.st_mode & static_cast<mode_t>(S_IFMT)) == S_IFREG;",
        "ok = ok && vfs_statat(&task, AT_FDCWD, CANONICALIZED_RELATIVE, 0, &st) == 0 && "
        "(st.st_mode & static_cast<mode_t>(S_IFMT)) == S_IFREG;",
        "ok = ok && vfs_statat(&task, AT_FDCWD, SIMPLE_RELATIVE_TRAILING_SLASH, 0, &st) == -ENOTDIR;",
        "ok = ok && vfs_statat(&task, AT_FDCWD, \"tmp/\", 0, &st) == 0 && "
        "(st.st_mode & static_cast<mode_t>(S_IFMT)) == S_IFDIR;",
        "ok = (vfs_unlink(FILE_PATH) == 0) && ok;",
        "return ok;",
    ]
    require_order(selftest, [verification, *functional_checks], "sticky statat resolver KTEST result propagation")
    if len(re.findall(r"\bok\s*=", selftest)) != 6 or selftest.count("return ok;") != 1:
        fail("statat resolver KTEST result must remain sticky through cleanup and return")


def test_open_path_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()

    open_body = function_body(core, "vfs_open")
    require_order(
        open_body,
        [
            "std::array<char, MAX_PATH_LEN> raw_path __attribute__((uninitialized));",
            "if (path.size() >= MAX_PATH_LEN)",
            "std::memcpy(raw_path.data(), path.data(), path.size())",
            "raw_path[path.size()] = '\\0'",
            "path_requires_directory(raw_path.data(), path.size())",
            "std::array<char, MAX_PATH_LEN> path_buffer __attribute__((uninitialized));",
            "int const FAST_RET",
            "vfs_open_absolute_common_local_fast_path(task, raw_path.data(), path_buffer, &fast_path_requires_directory,",
            "if (FAST_RET == 0)",
            "vfs_open_resolved_for_task(task, raw_path.data(), path_buffer",
            "if (FAST_RET < 0)",
            "int const RESOLVE_RET",
            "resolve_dirfd_task_path_raw(task, AT_FDCWD, raw_path.data(), path_buffer.data(), path_buffer.size(), !OPEN_LOCAL,",
            "resolve_task_path_raw_impl(raw_path.data(), path_buffer.data(), MAX_PATH_LEN, !OPEN_LOCAL, &path_buffer_len,",
            "if (RESOLVE_RET < 0)",
            "return -ENOENT",
            "vfs_open_resolved_for_task(task, raw_path.data(), path_buffer",
        ],
        "vfs_open scratch producer and consumer ordering",
    )
    forbid(
        open_body,
        [
            "std::array<char, MAX_PATH_LEN> raw_path;",
            "std::array<char, MAX_PATH_LEN> raw_path{};",
            "std::array<char, MAX_PATH_LEN> path_buffer;",
            "std::array<char, MAX_PATH_LEN> path_buffer{};",
            "raw_path.fill(",
            "path_buffer.fill(",
            "std::memset(raw_path.data()",
            "std::memset(path_buffer.data()",
        ],
        "vfs_open redundant scratch initialization",
    )

    openat_body = function_body(core, "vfs_openat")
    require_order(
        openat_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved __attribute__((uninitialized));",
            "int const FAST_RET",
            "vfs_open_absolute_common_local_fast_path(task, pathname, resolved, &path_requires_directory,",
            "if (FAST_RET == 0)",
            "vfs_open_resolved_for_task(task, pathname, resolved",
            "if (FAST_RET < 0)",
            "int const RESOLVE_RET",
            "resolve_dirfd_task_path_raw(task, dirfd, pathname, resolved.data(), resolved.size(), !OPEN_LOCAL, &path_requires_directory,",
            "if (RESOLVE_RET < 0)",
            "return RESOLVE_RET",
            "vfs_open_resolved_for_task(task, pathname, resolved",
        ],
        "vfs_openat scratch producer and consumer ordering",
    )
    forbid(
        openat_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved;",
            "std::array<char, MAX_PATH_LEN> resolved{};",
            "resolved.fill(",
            "std::memset(resolved.data()",
        ],
        "vfs_openat redundant scratch initialization",
    )

    resolved_open_body = function_body(core, "vfs_open_resolved_for_task")
    require_order(
        resolved_open_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved __attribute__((uninitialized));",
            "int const RESOLVE_RET = resolve_symlinks(path_buffer.data(), resolved.data(), resolved.size(),",
            "if (RESOLVE_RET < 0)",
            "return RESOLVE_RET",
            "path_text_equal(path_buffer.data(), path_buffer_len, resolved.data(), resolved_len)",
        ],
        "open symlink scratch producer and consumer ordering",
    )
    forbid(
        resolved_open_body,
        [
            "std::array<char, MAX_PATH_LEN> resolved;",
            "std::array<char, MAX_PATH_LEN> resolved{};",
            "resolved.fill(",
            "std::memset(resolved.data()",
        ],
        "open symlink redundant scratch initialization",
    )

    resolve_symlinks_body = function_body(core, "resolve_symlinks")
    require_order(
        resolve_symlinks_body,
        [
            "if (known_path_len != UNKNOWN_PATH_LEN)",
            "std::memcpy(resolved_buf, path, known_path_len)",
            "while (path[path_len] != '\\0' && path_len < bufsize - 1)",
            "resolved_buf[path_len] = path[path_len]",
            "resolved_buf[path_len] = '\\0'",
            "if (apply_task_policy)",
        ],
        "resolve_symlinks initial string construction",
    )


if __name__ == "__main__":
    test_dirfd_visible_scratch_is_initialized_by_root_strip()
    test_absolute_visible_scratch_is_initialized_by_dot_clean_producer()
    test_prefix_symlink_scratch_is_initialized_by_its_producers()
    test_readdir_child_path_scratch_is_initialized_by_its_producer()
    test_synthetic_readdir_visible_paths_are_initialized_by_root_strip()
    test_stat_trailing_slash_scratch_is_initialized_by_trim_producer()
    test_statat_scratch_is_initialized_by_dirfd_resolver()
    test_open_path_scratch_is_initialized_by_its_producers()
    print("VFS open path scratch invariants hold")
