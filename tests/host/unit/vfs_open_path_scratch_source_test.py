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
    test_open_path_scratch_is_initialized_by_its_producers()
    print("VFS open path scratch invariants hold")
