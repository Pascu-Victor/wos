#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_VFS_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.cpp"
REMOTE_VFS_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
WIRE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wire.hpp"
DEV_SERVER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
VFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "vfs.hpp"
VFS_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "vfs_ktest.cpp"
WKI_DEV_PROXY_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_dev_proxy_ktest.cpp"
WKI_WIRE_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wire_ktest.cpp"
WKI_WIRE_HOST_TEST = ROOT / "tests" / "host" / "unit" / "wki_wire_test.cpp"


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


def block_body_after(source: str, marker: str) -> str:
    marker_pos = source.find(marker)
    if marker_pos < 0:
        fail(f"missing block marker {marker}")
    brace_pos = marker_pos + len(marker)
    while brace_pos < len(source) and source[brace_pos].isspace():
        brace_pos += 1
    if brace_pos >= len(source) or source[brace_pos] != "{":
        fail(f"missing block after {marker}")

    depth = 1
    pos = brace_pos + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated block after {marker}")
    return source[brace_pos + 1 : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def brace_depth_at(source: str, pos: int) -> int:
    depth = 0
    for char in source[:pos]:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
    return depth


def test_vfs_host_alias_rewrite_is_overlap_safe() -> None:
    core = VFS_CORE_CPP.read_text()
    build = function_body(core, "build_wki_host_path")
    require_order(
        build,
        [
            "if (TOTAL > out_size)",
            "size_t const HOST_END = WKI_PATH_PREFIX_LEN + HOST_LEN",
            "std::memmove(out + HOST_END + 1, trimmed_suffix, SUFFIX_LEN + 1)",
            'std::memcpy(out, "/wki/", WKI_PATH_PREFIX_LEN)',
            "std::memcpy(out + WKI_PATH_PREFIX_LEN, hostname, HOST_LEN)",
            "out[HOST_END] = '/'",
            "out[TOTAL - 1] = '\\0'",
        ],
        "overlap-safe WKI host path construction",
    )
    if "std::memcpy(out + pos, trimmed_suffix" in build:
        fail("WKI host path construction must relocate an aliased suffix before prefix writes")

    rewrite = function_body(core, "rewrite_wki_host_alias")
    require_order(
        rewrite,
        [
            "size_t const SUFFIX_LEN = std::strlen(suffix)",
            "std::memmove(current.data() + 1, suffix, SUFFIX_LEN + 1)",
            "current[0] = '/'",
        ],
        "overlap-safe local WKI host alias stripping",
    )
    if "std::memcpy(current.data() + 1, suffix" in rewrite:
        fail("WKI host alias stripping must not use memcpy on overlapping ranges")

    require_tokens(
        VFS_HPP.read_text(),
        ["auto vfs_selftest_wki_host_alias_overlap() -> bool;"],
        "WKI host alias overlap selftest declaration",
    )
    require_tokens(
        VFS_KTEST.read_text(),
        [
            "KTEST(VFS, WkiHostAliasOverlap)",
            "vfs_selftest_wki_host_alias_overlap()",
        ],
        "WKI host alias overlap KTEST",
    )


def test_vfs_route_scratch_is_initialized_by_its_producer() -> None:
    core = VFS_CORE_CPP.read_text()

    rewrite = function_body(core, "rewrite_wki_host_alias")
    require_order(
        rewrite,
        [
            "std::array<char, MAX_PATH_LEN> current __attribute__((uninitialized));",
            "copy_path_string(path, current.data(), current.size())",
            "if (copy_result < 0)",
            "std::array<char, MAX_PATH_LEN> self_prefix __attribute__((uninitialized));",
            "build_wki_host_path(ker::net::wki::g_wki.local_hostname.data(), \"\", self_prefix.data(), self_prefix.size())",
            "if (copy_result < 0)",
            "self_prefix_len = std::strlen(self_prefix.data())",
        ],
        "WKI alias scratch producer ordering",
    )
    require_tokens(
        rewrite,
        [
            "return copy_path_string(current.data(), out, out_size);",
            "self_prefix_len > WKI_PATH_PREFIX_LEN",
        ],
        "WKI alias scratch consumers",
    )

    route = function_body(core, "apply_task_vfs_route")
    require_order(
        route,
        [
            "std::array<char, MAX_PATH_LEN> logical_path __attribute__((uninitialized));",
            "strip_task_root_prefix(task, path, logical_path.data(), logical_path.size(), &had_root_prefix)",
            "if (LOGICAL_RESULT < 0)",
            "std::array<char, MAX_PATH_LEN> aliased __attribute__((uninitialized));",
            "rewrite_wki_host_alias(task, logical_path.data(), aliased.data(), aliased.size())",
            "if (alias_result < 0)",
            "choose_task_route(task, aliased.data())",
            "std::array<char, MAX_PATH_LEN> routed __attribute__((uninitialized));",
            "if (alias_result < 0)",
            "copy_path_string(routed.data(), out, out_size)",
        ],
        "task route scratch producer ordering",
    )
    require_tokens(
        route,
        [
            "alias_result = copy_path_string(aliased.data(), routed.data(), routed.size());",
            "alias_result = build_wki_host_path(submitter, aliased.data(), routed.data(), routed.size());",
        ],
        "task route scratch branch producers",
    )

    caller_specs = [
        (
            "normalize_task_path_inplace_for_task",
            "int const ROUTE_RESULT = apply_task_vfs_route",
            "if (ROUTE_RESULT < 0)",
            "copy_path_string(routed.data(), path, bufsize)",
        ),
        (
            "finish_canonical_task_path_raw",
            "int const ROUTE_RESULT = apply_task_vfs_route",
            "if (ROUTE_RESULT < 0)",
            "copy_path_string(routed.data(), out, outsize",
        ),
        (
            "resolve_dirfd_task_path_raw",
            "result = apply_task_vfs_route",
            "if (result < 0)",
            "copy_path_string(routed.data(), out, outsize",
        ),
    ]
    for function_name, producer, error_check, consumer in caller_specs:
        body = function_body(core, function_name)
        require_order(
            body,
            [
                "std::array<char, MAX_PATH_LEN> routed __attribute__((uninitialized));",
                producer,
                error_check,
                consumer,
            ],
            f"{function_name} route scratch producer ordering",
        )
        if "std::array<char, MAX_PATH_LEN> routed{};" in body:
            fail(f"{function_name} must not value-initialize fully produced route scratch")

    normalizer = function_body(core, "normalize_task_path_inplace_for_task")
    normalizer_producer = "int const ROUTE_RESULT = apply_task_vfs_route(task, path, routed.data(), routed.size());"
    normalizer_gate = "if (ROUTE_RESULT < 0) {\n        return ROUTE_RESULT;\n    }"
    normalizer_consumer = "return copy_path_string(routed.data(), path, bufsize);"
    normalizer_producer_pos = normalizer.find(normalizer_producer)
    normalizer_gate_pos = normalizer.find(normalizer_gate, normalizer_producer_pos + len(normalizer_producer))
    normalizer_consumer_pos = normalizer.find(normalizer_consumer, normalizer_gate_pos + len(normalizer_gate))
    if (
        normalizer.count(normalizer_producer) != 1
        or normalizer.count(normalizer_gate) != 1
        or normalizer.count(normalizer_consumer) != 1
        or normalizer_producer_pos < 0
        or normalizer_gate_pos < 0
        or normalizer_consumer_pos < 0
        or normalizer[normalizer_producer_pos + len(normalizer_producer) : normalizer_gate_pos].strip()
        or normalizer[normalizer_gate_pos + len(normalizer_gate) : normalizer_consumer_pos].strip()
    ):
        fail("task-route normalization must return producer failures before consuming routed scratch")

    for legacy in [
        "std::array<char, MAX_PATH_LEN> current{};",
        "std::array<char, MAX_PATH_LEN> self_prefix{};",
        "std::array<char, MAX_PATH_LEN> logical_path{};",
        "std::array<char, MAX_PATH_LEN> aliased{};",
    ]:
        if legacy in rewrite or legacy in route:
            fail(f"route scratch must not retain redundant value initialization: {legacy}")


def test_remote_chown_matches_xfs_noop_semantics() -> None:
    core = VFS_CORE_CPP.read_text()
    path_chown = function_body(core, "vfs_chown_resolved_path")
    file_chown = function_body(core, "vfs_fchown_for_task")

    require_order(
        path_chown,
        [
            "case FSType::FAT32:",
            "case FSType::XFS:",
            "case FSType::REMOTE:",
            "return 0;  // Accept silently",
        ],
        "remote path chown follows XFS ownership semantics",
    )
    require_order(
        file_chown,
        [
            "case FSType::FAT32:",
            "case FSType::XFS:",
            "case FSType::REMOTE:",
            "vfs_put_file(f);",
            "return 0;  // Accept silently",
        ],
        "remote file chown follows XFS ownership semantics",
    )


def test_wki_host_mount_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()
    mount = function_body(core, "ensure_wki_host_root_mount")

    for failure_gate in [
        "if (STRIP_RET < 0)",
        "if (host_len >= hostname.size())",
        "if (MOUNT_PATH_RET <= 0 || static_cast<size_t>(MOUNT_PATH_RET) >= mount_root.size())",
    ]:
        if block_body_after(mount, failure_gate).strip() != "return 0;":
            fail(f"{failure_gate} must return before consuming uninitialized scratch")

    require_order(
        mount,
        [
            "std::array<char, MAX_PATH_LEN> logical __attribute__((uninitialized));",
            "int const STRIP_RET =",
            "ROOT_IS_GLOBAL ? copy_path_string(path, logical.data(), logical.size())",
            "strip_task_root_prefix(task, path, logical.data(), logical.size(), nullptr)",
            "if (STRIP_RET < 0)",
            "std::strncmp(logical.data(), WKI_PREFIX.data(), WKI_PREFIX.size())",
            "const char* host_part = logical.data() + WKI_PREFIX.size()",
            "const char* host_end = host_part",
            "while (*host_end != '\\0' && *host_end != '/')",
            "if (host_end == host_part)",
            "std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname __attribute__((uninitialized));",
            "auto host_len = static_cast<size_t>(host_end - host_part)",
        ],
        "WKI host mount logical-path producer ordering",
    )
    require_order(
        mount,
        [
            "std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname __attribute__((uninitialized));",
            "if (host_len >= hostname.size())",
            "std::memcpy(hostname.data(), host_part, host_len)",
            "hostname[host_len] = '\\0'",
            "std::strcmp(hostname.data(), ker::net::wki::g_wki.local_hostname.data())",
            "std::snprintf(mount_root.data(), mount_root.size(), \"/wki/%s\", hostname.data())",
            "wki_peer_find_by_hostname(hostname.data())",
        ],
        "WKI host mount hostname producer ordering",
    )
    require_order(
        mount,
        [
            "std::array<char, MAX_PATH_LEN> mount_root __attribute__((uninitialized));",
            "int const MOUNT_PATH_RET =",
            "std::snprintf(mount_root.data(), mount_root.size(), \"/wki/%s\", hostname.data())",
            "if (MOUNT_PATH_RET <= 0 || static_cast<size_t>(MOUNT_PATH_RET) >= mount_root.size())",
            "std::array<char, MAX_PATH_LEN> resolved_mount_root __attribute__((uninitialized));",
            "resolve_mount_path(mount_root.data(), resolved_mount_root.data(), resolved_mount_root.size()) == 0",
            "vfs_mkdir(mount_root.data(), 0755)",
            "wki_remote_vfs_mount(NODE_ID, find_ctx.result.resource_id, mount_root.data(), find_ctx.result.generation)",
            "log::info(\"auto-mounted WKI host root %s for path %s\", mount_root.data(), logical.data())",
        ],
        "WKI host mount root-path producer ordering",
    )

    resolved_success = block_body_after(
        mount, "if (resolve_mount_path(mount_root.data(), resolved_mount_root.data(), resolved_mount_root.size()) == 0)"
    )
    require_order(
        resolved_success,
        [
            "find_mount_point(resolved_mount_root.data())",
            "std::strcmp(existing->path, resolved_mount_root.data())",
        ],
        "WKI host mount resolved-path success consumers",
    )
    require_tokens(
        mount,
        [
            "RootExportFindCtx find_ctx = {.node_id = NODE_ID};",
        ],
        "WKI host mount state initialization",
    )
    for legacy in [
        "std::array<char, MAX_PATH_LEN> logical{};",
        "std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname{};",
        "std::array<char, MAX_PATH_LEN> mount_root{};",
        "std::array<char, MAX_PATH_LEN> resolved_mount_root{};",
        "logical.fill(",
        "hostname.fill(",
        "mount_root.fill(",
        "resolved_mount_root.fill(",
        "std::memset(logical.data()",
        "std::memset(hostname.data()",
        "std::memset(mount_root.data()",
        "std::memset(resolved_mount_root.data()",
    ]:
        if legacy in mount:
            fail(f"WKI host mount scratch must not retain a redundant clear: {legacy}")


def test_vfs_rename_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()
    rename = function_body(core, "vfs_rename")

    require_tokens(
        rename,
        [
            "std::array<char, MAX_PATH_LEN> old_buf __attribute__((uninitialized));",
            "std::array<char, MAX_PATH_LEN> new_buf __attribute__((uninitialized));",
        ],
        "vfs_rename producer-owned scratch",
    )
    old_fast = block_body_after(rename, "if (task_absolute_local_path_fast_path_allowed(task, oldpath, &old_scan))")
    require_order(
        old_fast,
        [
            "copy_path_string(oldpath, old_buf.data(), old_buf.size(), old_scan.path_len, &old_buf_len)",
            "if (COPY_RET < 0)",
            "return COPY_RET",
            "old_buf_hash = old_scan.path_hash",
        ],
        "vfs_rename old fast producer",
    )
    old_slow = block_body_after(
        rename,
        "else if (resolve_task_path_raw_impl(oldpath, old_buf.data(), old_buf.size(), true, &old_buf_len, &old_buf_hash) < 0)",
    )
    if old_slow.strip() != "return -ENAMETOOLONG;":
        fail("vfs_rename must return when the old fallback producer fails")

    new_fast = block_body_after(rename, "if (task_absolute_local_path_fast_path_allowed(task, newpath, &new_scan))")
    require_order(
        new_fast,
        [
            "copy_path_string(newpath, new_buf.data(), new_buf.size(), new_scan.path_len, &new_buf_len)",
            "if (COPY_RET < 0)",
            "return COPY_RET",
            "new_buf_hash = new_scan.path_hash",
        ],
        "vfs_rename new fast producer",
    )
    new_slow = block_body_after(
        rename,
        "else if (resolve_task_path_raw_impl(newpath, new_buf.data(), new_buf.size(), true, &new_buf_len, &new_buf_hash) < 0)",
    )
    if new_slow.strip() != "return -ENAMETOOLONG;":
        fail("vfs_rename must return when the new fallback producer fails")
    require_order(
        rename,
        [
            "if (task_absolute_local_path_fast_path_allowed(task, oldpath, &old_scan))",
            "else if (resolve_task_path_raw_impl(oldpath, old_buf.data(), old_buf.size(), true, &old_buf_len, &old_buf_hash) < 0)",
            "if (task_absolute_local_path_fast_path_allowed(task, newpath, &new_scan))",
            "else if (resolve_task_path_raw_impl(newpath, new_buf.data(), new_buf.size(), true, &new_buf_len, &new_buf_hash) < 0)",
            "vfs_rename_resolved_paths(old_buf.data(), new_buf.data(), path_requires_directory(oldpath), path_requires_directory(newpath),",
            "old_buf_len, new_buf_len, old_buf_hash, new_buf_hash)",
        ],
        "vfs_rename producer-before-consumer ordering",
    )

    renameat = function_body(core, "vfs_renameat")
    require_tokens(
        renameat,
        [
            "std::array<char, MAX_PATH_LEN> old_resolved __attribute__((uninitialized));",
            "std::array<char, MAX_PATH_LEN> new_resolved __attribute__((uninitialized));",
        ],
        "vfs_renameat producer-owned scratch",
    )
    old_resolver = (
        "resolve_dirfd_task_path_raw_with_absolute_local_fast_path(task, olddirfd, oldpath, old_resolved.data(), old_resolved.size(), true,"
    )
    new_resolver = (
        "resolve_dirfd_task_path_raw_with_absolute_local_fast_path(task, newdirfd, newpath, new_resolved.data(), new_resolved.size(), true,"
    )
    old_start = renameat.find(old_resolver)
    new_start = renameat.find(new_resolver)
    if old_start < 0 or new_start <= old_start:
        fail("vfs_renameat must resolve old path before new path")
    require_order(
        renameat[old_start:new_start],
        [
            old_resolver,
            "&old_path_requires_directory, &old_resolved_len, &old_resolved_hash)",
            "if (result < 0)",
            "return result",
        ],
        "vfs_renameat old producer failure gate",
    )
    require_order(
        renameat[new_start:],
        [
            new_resolver,
            "&new_path_requires_directory, &new_resolved_len, &new_resolved_hash)",
            "if (result < 0)",
            "return result",
            "vfs_rename_resolved_paths(old_resolved.data(), new_resolved.data(), old_path_requires_directory, new_path_requires_directory,",
            "old_resolved_len, new_resolved_len, old_resolved_hash, new_resolved_hash)",
        ],
        "vfs_renameat new producer and consumer ordering",
    )

    for body, name, data_uses, total_uses in [
        (rename, "old_buf", 3, 6),
        (rename, "new_buf", 3, 6),
        (renameat, "old_resolved", 2, 4),
        (renameat, "new_resolved", 2, 4),
    ]:
        if body.count(f"{name}.data()") != data_uses:
            fail(f"{name} must have exactly {data_uses} producer/consumer data uses")
        if len(re.findall(rf"\b{re.escape(name)}\b", body)) != total_uses:
            fail(f"{name} has an unexpected use outside its producers and final consumer")

    for legacy in [
        "std::array<char, MAX_PATH_LEN> old_buf;",
        "std::array<char, MAX_PATH_LEN> new_buf;",
        "std::array<char, MAX_PATH_LEN> old_resolved;",
        "std::array<char, MAX_PATH_LEN> new_resolved;",
        "old_buf = {};",
        "new_buf = {};",
        "old_resolved = {};",
        "new_resolved = {};",
        "old_buf.fill(",
        "new_buf.fill(",
        "old_resolved.fill(",
        "new_resolved.fill(",
        "std::memset(old_buf.data()",
        "std::memset(new_buf.data()",
        "std::memset(old_resolved.data()",
        "std::memset(new_resolved.data()",
    ]:
        if legacy in rename or legacy in renameat:
            fail(f"VFS rename scratch must not retain a redundant clear: {legacy}")


def test_proxy_op_slot_waits_are_bounded() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    wki_header = WKI_HPP.read_text()
    wki_source = WKI_CPP.read_text()
    require_tokens(
        header,
        [
            "constexpr size_t VFS_PROXY_SLOT_WAITER_CAPACITY = 64;",
            "uint64_t op_generation = 0;",
            "uint64_t op_waiter_pid = 0;",
            "WkiWaitEntry* op_retiring_wait_entry = nullptr;",
            "uint64_t op_retiring_waiter_pid = 0;",
            "std::array<uint64_t, VFS_PROXY_SLOT_WAITER_CAPACITY> op_slot_waiter_pids = {};",
            "size_t op_slot_waiter_count = 0;",
            "void wki_remote_vfs_cleanup_for_task(uint64_t pid);",
        ],
        "remote VFS allocation-free proxy slot FIFO",
    )
    require_tokens(
        source,
        [
            "constexpr uint64_t VFS_PROXY_SLOT_WAIT_TIMEOUT_US = VFS_PROXY_OP_TIMEOUT_US;",
            "auto acquire_proxy_slot_locked(ProxyVfsState* state, uint64_t start_us, bool claim_untracked_send) -> int",
            "auto acquire_proxy_op_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int",
            "auto acquire_proxy_untracked_send_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int",
        ],
        "remote VFS proxy slot timeout scaffolding",
    )
    acquire_body = function_body(source, "acquire_proxy_slot_locked")
    require_order(
        acquire_body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->lock.lock()",
            "if (!state->active)",
            "return WKI_ERR_PEER_FENCED",
            "uint64_t const HEAD_PID = proxy_slot_waiter_head_locked(state)",
            "if (!proxy_op_slot_busy(state) && (HEAD_PID == 0 || CALLER_IS_HEAD))",
            "remove_proxy_slot_waiter_locked(state, CALLER.pid)",
            "state->op_untracked_send_pending.store(true, std::memory_order_release)",
            "return WKI_OK",
            "if (NOW_US >= DEADLINE_US)",
            "remove_proxy_slot_waiter_locked(state, CALLER.pid)",
            "return WKI_ERR_TIMEOUT",
            "enqueue_proxy_slot_waiter_locked(state, CALLER.pid)",
            "state->lock.unlock()",
            "park_proxy_slot_caller(CALLER, DEADLINE_US, REGISTERED)",
        ],
        "wake-driven FIFO proxy slot acquisition",
    )
    require_order(
        function_body(source, "park_proxy_slot_caller"),
        [
            "uint64_t const REMAINING_US = deadline_us - NOW_US",
            "wki_current_wait_must_drive_progress()",
            "wki_spin_yield()",
            "caller.type == ker::mod::sched::task::TaskType::PROCESS",
            "wki_current_process_wait_can_park()",
            "uint64_t const PARK_DEADLINE_US",
            'ker::mod::sched::preemptible_syscall_park("wki_vfs_slot", PARK_DEADLINE_US)',
            "if (!registered)",
            "std::min(REMAINING_US, VFS_PROXY_CONTENTION_SLEEP_US)",
            "ker::mod::sched::kern_sleep_us(REMAINING_US)",
        ],
        "proxy slot PROCESS/DAEMON park split",
    )
    require_tokens(
        wki_header,
        [
            "std::atomic<bool> retirement_pending{false};",
            "auto wki_current_wait_must_drive_progress() -> bool;",
            "auto wki_current_process_wait_can_park() -> bool;",
        ],
        "WKI progress-driving wait API",
    )
    park_body = function_body(source, "park_proxy_slot_caller")
    if "interrupts_enabled()" in park_body:
        fail("remote VFS PROCESS park must accept the IF-masked syscall entry state")
    require_order(
        function_body(wki_source, "wki_wait_cleanup_for_task"),
        [
            "s_wait_lock.unlock()",
            "wki_remote_vfs_cleanup_for_task(task->pid)",
        ],
        "task exit releases remote VFS operation ownership after WKI wait quiescence",
    )


def test_proxy_operations_fail_before_setup_when_slot_wait_times_out() -> None:
    source = REMOTE_VFS_CPP.read_text()
    send_body = function_body(source, "vfs_proxy_send_and_wait")
    untracked_body = function_body(source, "vfs_proxy_send_untracked")
    rdma_body = function_body(source, "vfs_proxy_write_rdma_and_wait")

    require_order(
        send_body,
        [
            "int const SLOT_RET = acquire_proxy_op_slot_locked(state, PROXY_WAIT_START)",
            "if (SLOT_RET != WKI_OK)",
            "return encode_proxy_wki_status(SLOT_RET)",
            "peek_channel_tx_seq16",
            "advance_proxy_op_generation_locked(state)",
            "state->op_wait_entry = &wait",
            "state->op_waiter_pid = perf_current_pid()",
        ],
        "vfs_proxy_send_and_wait slot timeout",
    )
    require_order(
        untracked_body,
        [
            "int const SLOT_RET = acquire_proxy_untracked_send_slot_locked(state, PROXY_WAIT_START)",
            "if (SLOT_RET != WKI_OK)",
            "return normalize_proxy_status_for_errno(encode_proxy_wki_status(SLOT_RET))",
            "peek_channel_tx_seq16",
        ],
        "vfs_proxy_send_untracked slot timeout",
    )
    require_order(
        rdma_body,
        [
            "int const SLOT_RET = acquire_proxy_op_slot_locked(state, PROXY_WAIT_START)",
            "if (SLOT_RET != WKI_OK)",
            "return normalize_proxy_status_for_errno(encode_proxy_wki_status(SLOT_RET))",
            "peek_channel_tx_seq16",
            "advance_proxy_op_generation_locked(state)",
            "state->op_wait_entry = &wait",
            "state->op_waiter_pid = perf_current_pid()",
        ],
        "vfs_proxy_write_rdma_and_wait slot timeout",
    )


def test_proxy_sends_retry_only_pre_enqueue_pressure() -> None:
    source = REMOTE_VFS_CPP.read_text()
    retryable_body = function_body(source, "vfs_proxy_send_status_is_retryable")
    tracked_body = function_body(source, "vfs_proxy_send_and_wait")
    untracked_body = function_body(source, "vfs_proxy_send_untracked")
    rdma_body = function_body(source, "vfs_proxy_write_rdma_and_wait")

    require_tokens(
        retryable_body,
        [
            "status == WKI_ERR_NO_CREDITS",
            "status == WKI_ERR_NO_MEM",
        ],
        "proxy retryable pre-enqueue statuses",
    )
    for terminal_status in [
        "WKI_ERR_NO_ROUTE",
        "WKI_ERR_PEER_FENCED",
        "WKI_ERR_TIMEOUT",
        "WKI_ERR_INVALID",
        "WKI_ERR_NOT_FOUND",
        "WKI_ERR_BUSY",
        "WKI_ERR_TX_FAILED",
    ]:
        if terminal_status in retryable_body:
            fail(f"proxy send must not retry terminal status {terminal_status}")

    require_order(
        tracked_body,
        [
            "state->op_pending.store(true, std::memory_order_release)",
            "state->lock.unlock()",
            "uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, wait_timeout_us)",
            "ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller()",
            "int send_ret = WKI_ERR_NO_CREDITS",
            "while (true)",
            "wki_send_on_channel_identity",
            "send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)",
            "wki_now_us() >= SEND_DEADLINE_US",
            "send_ret = WKI_ERR_TIMEOUT",
            "park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false)",
            "int const SEND_RET = send_ret",
            "cancel_proxy_op_wait(state, wait, OP_GENERATION, SEND_RET)",
        ],
        "bounded tracked proxy send retry",
    )

    require_order(
        untracked_body,
        [
            "acquire_proxy_untracked_send_slot_locked(state, PROXY_WAIT_START)",
            "state->lock.unlock()",
            "uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, VFS_PROXY_OP_TIMEOUT_US)",
            "ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller()",
            "int send_ret = WKI_ERR_NO_CREDITS",
            "while (true)",
            "wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ",
            "send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)",
            "wki_now_us() >= SEND_DEADLINE_US",
            "send_ret = WKI_ERR_TIMEOUT",
            "park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false)",
            "int const SEND_RET = send_ret",
            "state->lock.lock()",
            "state->op_untracked_send_pending.store(false, std::memory_order_release)",
            "unlock_proxy_slot_and_wake_next(state)",
        ],
        "bounded ordered untracked proxy send retry",
    )

    require_order(
        rdma_body,
        [
            "state->op_pending.store(true, std::memory_order_release)",
            "state->lock.unlock()",
            "bool const CONTROL_FIRST = transport_is_roce(state->rdma_transport)",
            "uint64_t const SEND_DEADLINE_US = wki_future_deadline_us(PROXY_WAIT_START, VFS_PROXY_OP_TIMEOUT_US)",
            "ProxySlotCaller const SEND_CALLER = current_proxy_slot_caller()",
            "int send_ret = WKI_ERR_NO_CREDITS",
            "while (true)",
            "wki_send_on_channel_identity",
            "send_ret == WKI_OK || !vfs_proxy_send_status_is_retryable(send_ret)",
            "wki_now_us() >= SEND_DEADLINE_US",
            "send_ret = WKI_ERR_TIMEOUT",
            "park_proxy_slot_caller(SEND_CALLER, SEND_DEADLINE_US, false)",
            "int const SEND_RET = send_ret",
            "if (CONTROL_FIRST)",
            "wki_roce_rdma_write_tagged",
        ],
        "bounded RDMA control send retry before tagged data",
    )


def test_proxy_request_envelopes_use_stack_storage() -> None:
    source = REMOTE_VFS_CPP.read_text()

    require_tokens(
        source,
        [
            "static_assert(sizeof(DevOpReqPayload) <= WKI_ETH_MAX_PAYLOAD)",
            "static_assert(WKI_ETH_MAX_PAYLOAD <= UINT16_MAX)",
            "auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, size_t req_data_len",
            "auto vfs_proxy_send_untracked(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, size_t req_data_len",
        ],
        "request-envelope wire-size assumptions",
    )

    for function_name in ["vfs_proxy_send_and_wait", "vfs_proxy_send_untracked"]:
        body = function_body(source, function_name)
        if function_name == "vfs_proxy_send_and_wait":
            envelope_tokens = [
                "constexpr size_t MAX_REQ_DATA_LEN = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload)",
                "req_data_len > MAX_REQ_DATA_LEN",
                "req_tail_len > MAX_REQ_DATA_LEN - req_data_len",
                "return -EMSGSIZE",
                "req_data_len > 0 && req_data == nullptr",
                "req_tail_len > 0 && req_tail == nullptr",
                "return -EINVAL",
                "auto const REQ_DATA_LEN = static_cast<uint16_t>(req_data_len + req_tail_len)",
                "size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + req_data_len",
                "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized))",
                "reinterpret_cast<DevOpReqPayload*>(req_buf.data())",
                "memcpy(req_buf.data() + sizeof(DevOpReqPayload), req_data, req_data_len)",
                "if (req_tail_len == 0)",
                "wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data()",
                "static_cast<uint16_t>(REQ_PREFIX_TOTAL)",
            ]
        else:
            envelope_tokens = [
                "req_data_len > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload)",
                "return -EMSGSIZE",
                "req_data_len > 0 && req_data == nullptr",
                "return -EINVAL",
                "auto const REQ_DATA_LEN = static_cast<uint16_t>(req_data_len)",
                "size_t const REQ_TOTAL = sizeof(DevOpReqPayload) + req_data_len",
                "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf",
                "reinterpret_cast<DevOpReqPayload*>(req_buf.data())",
                "memcpy(req_buf.data() + sizeof(DevOpReqPayload), req_data, req_data_len)",
                "wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data()",
                "static_cast<uint16_t>(REQ_TOTAL)",
            ]
        require_order(
            body,
            envelope_tokens,
            f"{function_name} stack-backed request envelope",
        )
        if re.search(r"\bnew\b|delete\s*\[\s*\]", body):
            fail(f"{function_name} retained heap-backed request-envelope storage")
        if re.search(
            r"std::array<uint8_t,\s*WKI_ETH_MAX_PAYLOAD>\s+req_buf\s*(?:\{\}|=\s*\{\})",
            body,
        ):
            fail(f"{function_name} zero-initializes its full request envelope")

    rdma_body = function_body(source, "vfs_proxy_write_rdma_and_wait")
    require_order(
        rdma_body,
        [
            "std::array<uint8_t, 16> ctrl{}",
            "std::array<uint8_t, sizeof(DevOpReqPayload) + 16> req_buf = {}",
            "reinterpret_cast<DevOpReqPayload*>(req_buf.data())",
            "memcpy(req_buf.data() + sizeof(DevOpReqPayload), ctrl.data(), ctrl.size())",
            "wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ, req_buf.data()",
            "static_cast<uint16_t>(req_buf.size())",
        ],
        "RDMA write exact stack-backed request envelope",
    )
    if re.search(r"\bnew\b|delete\s*\[\s*\]", rdma_body):
        fail("RDMA write retained heap-backed request-envelope storage")

    open_body = function_body(source, "wki_remote_vfs_open_path")
    require_tokens(
        source,
        [
            "constexpr size_t OPEN_REQ_INLINE_CAPACITY =",
            "OPEN_REQ_BASE_LEN + ker::vfs::MOUNT_PATH_MAX + OPEN_PREFETCH_REQ_LEN",
            "static_assert(OPEN_REQ_INLINE_CAPACITY <= WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload))",
        ],
        "ordinary remote open inline request capacity",
    )
    require_order(
        open_body,
        [
            "size_t const REQ_FIXED_LEN = OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0)",
            "REQ_FIXED_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload)",
            "PATH_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - REQ_FIXED_LEN",
            "return nullptr",
            "auto path_len = static_cast<uint16_t>(PATH_LEN)",
            "size_t const REQ_DATA_LEN = REQ_FIXED_LEN + PATH_LEN",
            "std::array<uint8_t, OPEN_REQ_INLINE_CAPACITY> inline_req_data",
            "std::unique_ptr<uint8_t[]> heap_req_data",
            "uint8_t* req_data = inline_req_data.data()",
            "if (REQ_DATA_LEN > inline_req_data.size())",
            "heap_req_data.reset(new (std::nothrow) uint8_t[REQ_DATA_LEN])",
            "if (heap_req_data == nullptr)",
            "return nullptr",
            "req_data = heap_req_data.get()",
            "vfs_proxy_send_and_wait(state, OP_VFS_OPEN, req_data, REQ_DATA_LEN",
        ],
        "open request validates its wire size before narrowing",
    )
    if "delete[] req_data" in open_body:
        fail("ordinary remote open retained unconditional heap request ownership")
    if re.search(r"inline_req_data\s*(?:\{\}|=\s*\{\})", open_body):
        fail("remote open zero-initializes its inline request storage")


def test_readdir_batch_buffers_use_bounded_stack_storage() -> None:
    source = REMOTE_VFS_CPP.read_text()
    require_tokens(
        source,
        [
            "constexpr uint32_t VFS_READDIR_BATCH_MAX_ENTRIES =",
            "constexpr size_t VFS_READDIR_BATCH_DATA_CAPACITY =",
            "constexpr size_t VFS_READDIR_BATCH_RESPONSE_CAPACITY =",
            "static_assert(VFS_READDIR_BATCH_RESPONSE_CAPACITY <= WKI_ETH_MAX_PAYLOAD)",
            "constexpr auto vfs_readdir_batch_payload_is_valid(uint16_t payload_len, uint32_t count) -> bool",
            "count > VFS_READDIR_BATCH_MAX_ENTRIES",
            "REQUIRED_LEN <= payload_len",
            "static_assert(vfs_readdir_batch_payload_is_valid(sizeof(uint32_t), 0))",
            "static_assert(!vfs_readdir_batch_payload_is_valid(UINT16_MAX, VFS_READDIR_BATCH_MAX_ENTRIES + 1))",
        ],
        "remote readdir wire-derived stack capacities",
    )

    client_body = function_body(source, "remote_vfs_readdir")
    require_order(
        client_body,
        [
            "std::array<uint8_t, VFS_READDIR_BATCH_DATA_CAPACITY> batch_buf __attribute__((uninitialized))",
            "memcpy(req_data.data() + 8, &VFS_READDIR_BATCH_MAX_ENTRIES",
            "vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR_BATCH",
            "batch_buf.data()",
            "static_cast<uint16_t>(batch_buf.size())",
            "memcpy(&count, batch_buf.data(), sizeof(uint32_t))",
            "vfs_readdir_batch_payload_is_valid(resp_len, count)",
            "for (uint32_t i = 0; i < count; i++)",
        ],
        "consumer readdir batch stack storage and length validation",
    )
    if re.search(r"\bnew\b|delete\s*\[\s*\]|\bmemset\s*\(", client_body):
        fail("consumer readdir batch retained heap allocation or full-buffer clearing")
    if re.search(r"\bbatch_buf\s*(?:\{\}|=\s*\{\})", client_body):
        fail("consumer readdir batch zero-initializes its full stack buffer")

    handler_body = function_body(source, "handle_vfs_op")
    batch_start = handler_body.find("case OP_VFS_READDIR_BATCH:")
    stat_start = handler_body.find("case OP_VFS_STAT:", batch_start)
    if batch_start < 0 or stat_start < 0:
        fail("remote VFS readdir-batch opcode case must remain present")
    server_case = handler_body[batch_start:stat_start]
    require_order(
        server_case,
        [
            "max_count > VFS_READDIR_BATCH_MAX_ENTRIES",
            "max_count = VFS_READDIR_BATCH_MAX_ENTRIES",
            "std::array<uint8_t, VFS_READDIR_BATCH_RESPONSE_CAPACITY> resp_buf __attribute__((uninitialized))",
            "uint8_t* entries_base = resp_buf.data()",
            "reinterpret_cast<DevOpRespPayload*>(resp_buf.data())",
            "send_buffered_resp(resp_buf.data(), send_len)",
        ],
        "server readdir batch bounded stack response",
    )
    if re.search(r"\bnew\b|delete\s*\[\s*\]|\bmemset\s*\(", server_case):
        fail("server readdir batch retained heap allocation or full-buffer clearing")
    if re.search(r"\bresp_buf\s*(?:\{\}|=\s*\{\})", server_case):
        fail("server readdir batch zero-initializes its full stack buffer")


def test_proxy_slot_release_paths_handoff_after_unlock() -> None:
    source = REMOTE_VFS_CPP.read_text()
    ktest = WKI_DEV_PROXY_KTEST.read_text()
    cancel_body = function_body(source, "cancel_proxy_op_wait")
    send_body = function_body(source, "vfs_proxy_send_and_wait")
    untracked_body = function_body(source, "vfs_proxy_send_untracked")
    rdma_body = function_body(source, "vfs_proxy_write_rdma_and_wait")
    deactivate_body = function_body(source, "deactivate_vfs_proxy_locked")
    response_body = function_body(source, "handle_vfs_op_resp")

    require_order(
        cancel_body,
        [
            "state->lock.lock()",
            "state->op_pending.load(std::memory_order_acquire)",
            "state->op_generation == op_generation",
            "clear_proxy_op_state_locked(state, result)",
            "next_pid = proxy_slot_handoff_candidate_locked(state)",
            "state->lock.unlock()",
            "wake_proxy_slot_waiter(next_pid)",
            "finish_or_quiesce_waiter(&wait, claimed, result)",
            "wait_for_waiter_retirement(&wait)",
        ],
        "tracked cancellation only clears its own generation and wakes unlocked",
    )
    if send_body.count("unlock_proxy_slot_and_wake_next(state)") < 3:
        fail("generic proxy RPC must hand off on both setup failures and successful teardown")
    if rdma_body.count("unlock_proxy_slot_and_wake_next(state)") < 2:
        fail("RDMA proxy write must hand off on setup failure and successful teardown")
    if untracked_body.count("unlock_proxy_slot_and_wake_next(state)") < 2:
        fail("untracked proxy send must hand off on sequence failure and send completion")
    require_order(
        send_body,
        ["consume_proxy_op_result_locked(state, OP_GENERATION)", "wait_for_waiter_retirement(&wait)"],
        "generic proxy RPC waits for teardown's final stack reference",
    )
    require_order(
        rdma_body,
        ["consume_proxy_op_result_locked(state, OP_GENERATION)", "wait_for_waiter_retirement(&wait)"],
        "RDMA proxy RPC waits for teardown's final stack reference",
    )

    require_order(
        deactivate_body,
        [
            "state->lock.lock()",
            "state->active = false",
            "teardown.op_wait_entry = state->op_wait_entry",
            "teardown.op_wait_entry->retirement_pending.store(true, std::memory_order_release)",
            "state->op_wait_entry = nullptr",
            "clear_proxy_op_state_locked(state, -1)",
            "teardown.op_slot_waiter_pids = state->op_slot_waiter_pids",
            "state->op_slot_waiter_pids.fill(0)",
            "state->op_slot_waiter_count = 0",
            "state->lock.unlock()",
        ],
        "proxy teardown rejects and detaches FIFO waiters under the state lock",
    )
    finish_teardown_body = function_body(source, "finish_proxy_teardown_op_waiter")
    require_order(
        finish_teardown_body,
        [
            "finish_or_quiesce_waiter(teardown.op_wait_entry, teardown.op_wait_claimed, result)",
            "teardown.state->lock.lock()",
            "teardown.state->op_retiring_wait_entry = nullptr",
            "teardown.state->op_retiring_waiter_pid = 0",
            "teardown.op_wait_entry->retirement_pending.store(false, std::memory_order_release)",
            "teardown.state->lock.unlock()",
        ],
        "teardown clears discovery marker before its final stack-waiter release",
    )
    wake_body = function_body(source, "wake_proxy_slot_waiter")
    require_tokens(wake_body, ["remove_one_proxy_slot_waiter(pid)"], "failed wake reaps FIFO state only")
    if "wki_remote_vfs_cleanup_for_task" in wake_body or "remove_one_proxy_task_reference" in wake_body:
        fail("failed wake must not touch active-operation task lifetime state")
    if "wake_proxy_slot_waiter" in response_body or "unlock_proxy_slot_and_wake_next" in response_body:
        fail("DEV_OP_RESP must leave proxy slot handoff to the result consumer")
    require_order(
        function_body(source, "claim_response_waiter_locked"),
        [
            "if (!wki_claim_op(waiter))",
            "return nullptr",
            "return waiter",
        ],
        "response claim retains the exact stack waiter through completion",
    )
    require_tokens(
        response_body,
        ["wait_entry = claim_response_waiter_locked(state->op_wait_entry)"],
        "DEV_OP_RESP retained waiter ownership",
    )
    if "state->op_wait_entry = nullptr" in response_body:
        fail("DEV_OP_RESP must retain the waiter for pre-registration task-exit cleanup")

    clear_body = function_body(source, "clear_proxy_op_state_locked")
    require_tokens(
        clear_body,
        ["state->op_wait_entry = nullptr", "state->op_waiter_pid = 0", "state->op_pending.store(false, std::memory_order_release)"],
        "tracked proxy operation state reset",
    )
    cleanup_ref_body = function_body(source, "cleanup_proxy_task_reference_locked")
    require_order(
        cleanup_ref_body,
        [
            "remove_proxy_slot_waiter_locked(state, pid)",
            "state->op_waiter_pid == pid",
            "cleanup.op_wait_entry = state->op_wait_entry",
            "state->op_wait_entry = nullptr",
            "cleanup.op_wait_claimed = wki_claim_op(cleanup.op_wait_entry)",
            "clear_proxy_op_state_locked(state, WKI_ERR_PEER_FENCED)",
            "else if (RELEASED_RETIRING_OP)",
            "cleanup.op_wait_entry = state->op_retiring_wait_entry",
            "cleanup.op_wait_retiring = true",
            "cleanup.removed = REMOVAL.removed || RELEASED_ACTIVE_OP || RELEASED_RETIRING_OP",
            "proxy_slot_handoff_candidate_locked(state)",
        ],
        "task exit releases active operation and hands off its FIFO slot",
    )
    if "state->op_retiring_wait_entry = nullptr" in cleanup_ref_body:
        fail("task exit must not steal teardown's retiring waiter marker")
    cleanup_body = function_body(source, "wki_remote_vfs_cleanup_for_task")
    require_order(
        cleanup_body,
        [
            "remove_one_proxy_task_reference(pid)",
            "if (!CLEANUP.removed)",
            "if (CLEANUP.op_wait_retiring)",
            "wait_for_waiter_retirement(CLEANUP.op_wait_entry)",
            "finish_or_quiesce_waiter(CLEANUP.op_wait_entry, CLEANUP.op_wait_claimed, WKI_ERR_PEER_FENCED)",
            "wake_proxy_slot_waiter(CLEANUP.next_pid)",
        ],
        "task-exit proxy cleanup and handoff",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteVfsProxySlot, WaitersRemainFifo)",
            "KTEST(WkiRemoteVfsProxySlot, StaleCancelPreservesSuccessor)",
            "KTEST(WkiRemoteVfsProxySlot, ResponseClaimRetainsWaiterSlot)",
            "KTEST(WkiRemoteVfsProxySlot, CompletedResponseCancelReleasesSlot)",
            "KTEST(WkiRemoteVfsProxySlot, TaskExitReleasesOwnedSlot)",
            "KTEST(WkiRemoteVfsProxySlot, TaskExitDiscoversRetiringSlot)",
            "KTEST(WkiRemoteVfsProxySlot, TeardownQuiescesRetiringSlot)",
            "KTEST(WkiRemoteVfsProxySlot, InactiveProxyRejectsAcquisition)",
        ],
        "remote VFS proxy-slot KTEST coverage",
    )


def test_shared_io_slot_waits_are_bounded() -> None:
    source = REMOTE_VFS_CPP.read_text()
    body = function_body(source, "proxy_acquire_shared_io_slot")

    require_tokens(
        body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->shared_io_in_use.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)",
            "return WKI_OK",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ProxySlotCaller const CALLER = current_proxy_slot_caller()",
            "park_proxy_slot_caller(CALLER, DEADLINE_US, false)",
        ],
        "shared RDMA I/O slot acquisition",
    )
    if "kern_sleep_us" in body:
        fail("shared RDMA I/O PROCESS contenders must use caller-aware parking")
    if "void proxy_acquire_shared_io_slot" in source:
        fail("shared RDMA I/O slot acquisition must return timeout status")
    require_tokens(
        source,
        [
            "explicit SharedIoSlotGuard(ProxyVfsState* state_ref, uint64_t start_us)",
            "OptionalSharedIoSlotGuard(ProxyVfsState* state_ref, bool enabled, uint64_t start_us)",
            "auto acquired() const -> bool",
            "auto result() const -> int",
        ],
        "shared RDMA I/O slot guards",
    )


def test_shared_io_callers_timeout_or_fallback() -> None:
    source = REMOTE_VFS_CPP.read_text()
    write_body = function_body(source, "vfs_proxy_write_rdma_and_wait")
    read_body = function_body(source, "remote_vfs_read")
    open_body = function_body(source, "wki_remote_vfs_open_path")

    require_order(
        write_body,
        [
            "chunk == 0 || chunk > VFS_RDMA_WRITE_SIZE",
            "return -EINVAL",
            "*written_out = 0",
            "SharedIoSlotGuard const SHARED_IO_GUARD(state, wki_now_us())",
            "if (!SHARED_IO_GUARD.acquired())",
            "return normalize_proxy_status_for_errno(encode_proxy_wki_status(SHARED_IO_GUARD.result()))",
            "acquire_proxy_op_slot_locked(state, PROXY_WAIT_START)",
        ],
        "RDMA write shared-slot timeout",
    )
    require_order(
        read_body,
        [
            "SharedIoSlotGuard const SHARED_IO_GUARD(ctx->proxy, wki_now_us())",
            "if (!SHARED_IO_GUARD.acquired())",
            "skip_read_rdma_this_call = true",
            "const bool RDMA_READ_ENABLED = !skip_read_rdma_this_call",
            "SharedIoSlotGuard const SHARED_IO_GUARD(ctx->proxy, wki_now_us())",
            "if (SHARED_IO_GUARD.acquired())",
            "int rdma_error = 0",
        ],
        "RDMA read shared-slot fallback",
    )
    require_order(
        open_body,
        [
            "bool send_open_prefetch = WANT_OPEN_PREFETCH && OPEN_PREFETCH_LEN > 0",
            "OptionalSharedIoSlotGuard const OPEN_PREFETCH_GUARD(state, send_open_prefetch, wki_now_us())",
            "if (send_open_prefetch && !OPEN_PREFETCH_GUARD.acquired())",
            "send_open_prefetch = false",
            "size_t const REQ_FIXED_LEN = OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0)",
            "if (send_open_prefetch)",
            "tagged_receive.rkey = state->rdma_bulk_rkey",
        ],
        "open prefetch shared-slot fallback",
    )


def test_message_fallback_readahead_targets_small_sequential_reads() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = REMOTE_VFS_HPP.read_text()
    read_body = function_body(source, "remote_vfs_read")

    require_order(
        read_body,
        [
            "std::array<uint8_t, VFS_DIRECT_READ_STACK_SIZE> direct_read_buf __attribute__((uninitialized))",
            "bool const SHOULD_READ_AHEAD = ALLOW_READ_CACHES && !POSITIONAL_READ && remaining < VFS_CACHE_SIZE",
            "if (SHOULD_READ_AHEAD && ctx->read_cache == nullptr)",
            "ctx->read_cache = new (std::nothrow) ReadAheadCache;",
            "bool const USING_CACHE = SHOULD_READ_AHEAD && ctx->read_cache != nullptr",
            "auto fetch_size = USING_CACHE ? static_cast<uint32_t>(VFS_CACHE_SIZE) : std::min(remaining, VFS_DIRECT_READ_STACK_SIZE)",
            "uint8_t* fetch_dest = direct_read_buf.data()",
            "if (USING_CACHE)",
            "fetch_dest = ctx->read_cache->data.data()",
            "uint16_t resp_len = 0",
            "vfs_proxy_read_with_retry",
            "if (STATUS != 0)",
            "uint16_t const BYTES_READ = resp_len",
            "if (BYTES_READ == 0)",
            "ctx->read_cache->cached_offset = cur_offset",
            "ctx->read_cache->cached_len = BYTES_READ",
            "auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(BYTES_READ), remaining))",
            "remaining -= to_copy",
        ],
        "message fallback small-read read-ahead",
    )
    require_tokens(
        read_body,
        [
            "memcpy(dest, ctx->read_cache->data.data(), to_copy)",
            "memcpy(dest, fetch_dest, BYTES_READ)",
        ],
        "message fallback bounded response consumers",
    )
    require_tokens(
        header,
        [
            "uint16_t cached_len = 0;",
            "std::array<uint8_t, VFS_CACHE_SIZE> data;",
        ],
        "read-ahead cache validity boundary",
    )
    if "std::array<uint8_t, VFS_DIRECT_READ_STACK_SIZE> direct_read_buf{};" in read_body:
        fail("message fallback must not clear response-owned direct-read scratch")
    if "new (std::nothrow) ReadAheadCache()" in read_body:
        fail("read-ahead allocation must use default initialization so cache data is not pre-cleared")
    if "std::array<uint8_t, VFS_CACHE_SIZE> data = {};" in header:
        fail("read-ahead cache data must remain response-initialized")

    send_body = function_body(source, "vfs_proxy_send_and_wait")
    require_order(
        send_body,
        [
            "state->op_resp_buf = resp_buf",
            "state->op_resp_max = resp_buf_max",
            "state->op_resp_len = 0",
            "consume_proxy_op_result_locked(state, OP_GENERATION)",
            "uint16_t const RESP_LEN = RESULT.response_len",
            "*resp_len_out = RESP_LEN",
        ],
        "message response length publication",
    )
    response_body = function_body(source, "handle_vfs_op_resp")
    require_order(
        response_body,
        [
            "if (sizeof(DevOpRespPayload) + RESP_DATA_LEN > payload_len)",
            "copy_len = (RESP_DATA_LEN > state->op_resp_max) ? state->op_resp_max : RESP_DATA_LEN",
            "memcpy(state->op_resp_buf, resp_data, copy_len)",
            "state->op_resp_len = copy_len",
        ],
        "message response prefix initialization",
    )
    if "remaining >= VFS_CACHE_SIZE" in read_body:
        fail("message fallback must not reserve read-ahead for already-full cache-sized reads")
    if "return (total_read > 0) ? total_read : -ENOMEM" in read_body:
        fail("optional message read-ahead allocation failure must use the direct stack path")


def test_server_open_reuses_the_open_file_stat_snapshot() -> None:
    source = REMOTE_VFS_CPP.read_text()
    handler_body = function_body(source, "handle_vfs_op")
    open_start = handler_body.find("case OP_VFS_OPEN:")
    read_start = handler_body.find("case OP_VFS_READ:", open_start)
    if open_start < 0 or read_start < 0:
        fail("remote VFS server open/read opcode cases must remain present")
    open_case = handler_body[open_start:read_start]

    require_order(
        open_case,
        [
            "std::array<char, 512> full_path __attribute__((uninitialized));",
            "std::array<char, 512> full_visible_path __attribute__((uninitialized));",
            "build_full_path(full_path.data(), full_path.size(), export_path",
            "build_full_path(full_visible_path.data(), full_visible_path.size(), export_name",
            "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
            "ker::vfs::vfs_open_file_resolved(full_path.data()",
        ],
        "remote VFS server open full-path production before resolved-open consumption",
    )
    direct_open_path_tokens = [
        "std::array<char, 512> full_path __attribute__((uninitialized));",
        "std::array<char, 512> full_visible_path __attribute__((uninitialized));",
        "build_full_path(full_path.data(), full_path.size(), export_path",
        "build_full_path(full_visible_path.data(), full_visible_path.size(), export_name",
        "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
        "ker::vfs::vfs_open_file_resolved(full_path.data()",
    ]
    for token in direct_open_path_tokens:
        token_pos = open_case.find(token)
        if token_pos < 0 or brace_depth_at(open_case, token_pos) != 1:
            fail("remote VFS server open full-path production and consumption must be unconditional case statements")
    require_order(
        open_case,
        [
            "ker::vfs::vfs_open_file_resolved(full_path.data()",
            "ker::vfs::vfs_fstat_file(file, &open_stat)",
            "open_resp.has_stat = 1",
            "open_resp.stat = open_stat",
            "if (prefetch_rkey != 0",
            "alloc_remote_fd(channel_identity, file)",
            "open_resp.fd = fd_id",
            "int const SEND_RET = wki_send_on_channel_identity(channel_identity",
            "if (SEND_RET != WKI_OK)",
            "RemoteVfsFd* rfd = find_remote_fd(channel_identity, fd_id)",
            "rfd->file == file",
            "orphan = rfd->file",
            "rfd->file = nullptr",
            "rfd->retiring = true",
            "rfd->active = false",
            "s_vfs_lock.unlock()",
            "ker::vfs::vfs_close_file(orphan)",
        ],
        "remote VFS server open metadata snapshot reuse",
    )
    if "vfs_stat_resolved(full_path.data(), &open_stat)" in open_case:
        fail("remote VFS server open must not repeat path resolution for metadata")
    published_file = open_case.find("alloc_remote_fd(channel_identity, file)")
    if re.search(r"\bfile->", open_case[published_file:]):
        fail("remote VFS server open must not dereference a file after publishing it to peer cleanup")


def test_server_backing_path_mutations_bypass_worker_task_root() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = VFS_HPP.read_text()
    core = VFS_CORE_CPP.read_text()
    wire = WIRE_HPP.read_text()
    handler_body = function_body(source, "handle_vfs_op")

    symlink_start = handler_body.find("case OP_VFS_SYMLINK:")
    unlink_start = handler_body.find("case OP_VFS_UNLINK:", symlink_start)
    rename_start = handler_body.find("case OP_VFS_RENAME:")
    chmod_start = handler_body.find("case OP_VFS_CHMOD:", rename_start)
    fsync_start = handler_body.find("case OP_VFS_FSYNC:", chmod_start)
    if min(symlink_start, unlink_start, rename_start, chmod_start, fsync_start) < 0:
        fail("remote VFS server mutation opcode cases must remain present")

    symlink_case = handler_body[symlink_start:unlink_start]
    require_order(
        symlink_case,
        [
            "build_full_path(full_link.data(), full_link.size(), export_path",
            "path_crosses_recursive_wki_boundary(full_link.data())",
            "ker::vfs::vfs_symlink_resolved(target_str.data(), full_link.data())",
        ],
        "server symlink uses its export backing path without worker-root resolution",
    )
    if "ker::vfs::vfs_symlink(target_str.data(), full_link.data())" in symlink_case:
        fail("server symlink must not re-resolve an export backing path against the worker root")

    rename_case = handler_body[rename_start:chmod_start]
    require_order(
        rename_case,
        [
            "build_full_path(old_full.data(), old_full.size(), export_path",
            "build_full_path(new_full.data(), new_full.size(), export_path",
            "path_crosses_recursive_wki_boundary(old_full.data())",
            "ker::vfs::vfs_rename_resolved(old_full.data(), new_full.data())",
        ],
        "server rename uses export backing paths without worker-root resolution",
    )
    if "ker::vfs::vfs_rename(old_full.data(), new_full.data())" in rename_case:
        fail("server rename must not re-resolve export backing paths against the worker root")

    chmod_case = handler_body[chmod_start:fsync_start]
    require_order(
        chmod_case,
        [
            "build_full_path(full_path.data(), full_path.size(), export_path",
            "build_full_path(full_visible_path.data(), full_visible_path.size(), export_name",
            "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
            "ker::vfs::vfs_chmod_resolved(full_path.data(), static_cast<int>(mode),",
            "(flags & WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK) != 0",
        ],
        "server chmod uses its export backing path without worker-root resolution",
    )
    if "ker::vfs::vfs_chmod(full_path.data(), static_cast<int>(mode)," in chmod_case:
        fail("server chmod must not re-resolve an export backing path against the worker root")

    require_tokens(
        header,
        [
            "auto vfs_symlink_resolved(const char* target, const char* linkpath) -> int;",
            "auto vfs_rename_resolved(const char* oldpath, const char* newpath) -> int;",
            "auto vfs_chmod_resolved(const char* path, int mode, bool follow_final_symlink) -> int;",
        ],
        "resolved VFS mutation API declarations",
    )
    require_tokens(
        function_body(core, "vfs_symlink_resolved"),
        ["return vfs_symlink_resolved_linkpath(target, linkpath);"],
        "resolved symlink backing-path implementation",
    )
    require_tokens(
        function_body(core, "vfs_rename_resolved"),
        ["vfs_rename_resolved_paths(oldpath, newpath, path_requires_directory(oldpath), path_requires_directory(newpath))"],
        "resolved rename backing-path implementation",
    )
    require_tokens(
        function_body(core, "vfs_chmod_resolved"),
        ["return vfs_chmod_resolved_path(path, mode, follow_final_symlink);"],
        "resolved chmod backing-path implementation",
    )
    require_tokens(
        wire,
        [
            "constexpr uint16_t OP_VFS_CHMOD = 0x040F;",
            "constexpr uint8_t WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK = 0x01;",
        ],
        "remote chmod wire contract",
    )
    require_order(
        function_body(core, "vfs_chmod_resolved_path"),
        [
            "case FSType::REMOTE:",
            "wki_remote_vfs_chmod(mount->private_data, fs_path, mode, follow_final_symlink)",
            "cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type)",
        ],
        "remote chmod dispatch preserves final-symlink and cache semantics",
    )


def test_remote_path_utimens_preserves_owner_time_and_routing() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = REMOTE_VFS_HPP.read_text()
    core = VFS_CORE_CPP.read_text()
    vfs_header = VFS_HPP.read_text()
    wire = WIRE_HPP.read_text()
    handler = function_body(source, "handle_vfs_op")
    utimens_start = handler.find("case OP_VFS_UTIMENS:")
    fsync_start = handler.find("case OP_VFS_FSYNC:", utimens_start)
    if min(utimens_start, fsync_start) < 0:
        fail("remote VFS path utimens opcode case must remain present")
    utimens_case = handler[utimens_start:fsync_start]

    require_tokens(
        wire,
        [
            "constexpr uint16_t OP_VFS_UTIMENS = 0x0415;",
            "constexpr uint8_t WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK = 0x01;",
            "constexpr uint8_t WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT = 0x02;",
            "struct VfsUtimensReqPrefix",
            "static_assert(sizeof(VfsUtimensReqPrefix) == 36);",
        ],
        "remote utimens additive wire contract",
    )
    require_tokens(
        header,
        ["auto wki_remote_vfs_utimens(void* mount_private_data, const char* fs_relative_path, const ker::vfs::Timespec* times,"],
        "remote utimens client declaration",
    )
    require_tokens(
        vfs_header,
        [
            "auto vfs_utimens_resolved_beneath(const char* confinement_root, const char* path, const Timespec* times,",
            "bool follow_final_symlink) -> int;",
        ],
        "confined resolved utimens owner declaration",
    )

    client = function_body(source, "wki_remote_vfs_utimens")
    require_order(
        client,
        [
            "auto* state = acquire_vfs_proxy_lane(anchor)",
            "ProxyLifecycleRefGuard lane_ref_guard(state)",
            "VfsUtimensReqPrefix prefix{}",
            "prefix.flags = follow_final_symlink ? WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK : 0",
            "if (times != nullptr)",
            "prefix.atime_sec = times[0].tv_sec",
            "prefix.mtime_nsec = times[1].tv_nsec",
            "prefix.flags |= WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT",
            "memcpy(req_stack.data(), &prefix, sizeof(prefix))",
            "memcpy(req_stack.data() + sizeof(prefix), fs_relative_path, PATH_LEN)",
            "vfs_proxy_send_and_wait(state, OP_VFS_UTIMENS",
        ],
        "remote utimens client preserves raw timestamp markers and lane lifetime",
    )

    require_order(
        utimens_case,
        [
            "data_len < sizeof(VfsUtimensReqPrefix)",
            "memcpy(&prefix, data, sizeof(prefix))",
            "(prefix.flags & ~KNOWN_FLAGS) != 0",
            "prefix.reserved != 0",
            "EXPECTED_DATA_LEN != data_len",
            "relative_wire_path_has_safe_components(PATH_DATA, prefix.path_len)",
            "full_path_fits(export_path)",
            "build_full_path(full_path.data(), full_path.size(), export_path",
            "build_full_path(full_visible_path.data(), full_visible_path.size(), export_name",
            "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
            "ker::vfs::Timespec{.tv_sec = prefix.atime_sec, .tv_nsec = prefix.atime_nsec}",
            "(prefix.flags & WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT) != 0 ? request_times.data() : nullptr",
            "ker::vfs::vfs_utimens_resolved_beneath(",
            "export_path, full_path.data(), TIMES",
            "(prefix.flags & WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK) != 0",
        ],
        "remote utimens server validates before resolved owner-side mutation",
    )
    if "ker::vfs::vfs_utimensat(" in utimens_case:
        fail("remote utimens server must not route an export backing path through worker task state")

    validation = function_body(source, "relative_wire_path_has_safe_components")
    require_tokens(
        validation,
        [
            "path[0] == '/'",
            "std::memchr(path, '\\0', path_len) != nullptr",
            "bool const DOT =",
            "bool const DOT_DOT =",
        ],
        "wire path validation rejects truncation, absolute paths, and traversal components",
    )

    owner = function_body(core, "vfs_utimens_resolved_beneath")
    require_order(
        owner,
        [
            "copy_path_string(confinement_root",
            "canonicalize_path(canonical_root.data()",
            "find_mount_point(canonical_root.data(), canonical_root_len)",
            "confinement_mount->fs_type == FSType::REMOTE",
            "copy_path_string(path",
            "canonicalize_path(canonical_path.data()",
            "path_prefix_matches(canonical_path.data(), canonical_root.data(), canonical_root_len)",
            ".reject_remote_mounts = true",
            ".reapply_task_root = false",
            "vfs_apply_utimens_to_resolved_path(canonical_path.data(), times, follow_final_symlink, canonical_path_len, false, &POLICY)",
        ],
        "owner canonicalizes and confines before local-only resolution",
    )
    if "resolve_mount_path(" in owner:
        fail("owner backing-path confinement must not reapply worker task root through resolve_mount_path")

    apply = function_body(core, "vfs_apply_utimens_to_resolved_path")
    require_order(
        apply,
        [
            "resolve_policy->reject_remote_mounts && REMOTE_MOUNT",
            "if (!REMOTE_MOUNT)",
            "bool const RESOLVE_FINAL_SYMLINK = follow_final_symlink && !skip_final_symlink_probe",
            "resolve_symlinks(path_buffer.data(), resolved_path.data()",
            "mount_ref = find_mount_point(path_buffer.data(), resolved_len)",
            "if (!allow_remote_backend && mount->fs_type == FSType::REMOTE)",
            "case FSType::REMOTE:",
            "wki_remote_vfs_utimens(mount->private_data, fs_path, times, follow_final_symlink)",
            "cache_notify_path_data_changed_impl(path_buffer.data(), mount->fs_type)",
            "cache_notify_path_data_changed_impl(requested_path.data(), REQUESTED_FS_TYPE, true)",
        ],
        "utimens resolves intermediates for both final-follow modes and invalidates target plus alias",
    )

    resolver = function_body(core, "resolve_symlinks")
    require_order(
        resolver,
        [
            "symlink_resolve_path_is_confined(resolved_buf, policy)",
            "find_mount_point(resolved_buf",
            "policy->reject_remote_mounts && mount != nullptr && mount->fs_type == FSType::REMOTE",
            "resolve_prefix_symlink_once(resolved_buf",
        ],
        "confined resolver rejects a final REMOTE mount before prefix probing",
    )
    prefix_resolver = function_body(core, "resolve_prefix_symlink_once")
    require_order(
        prefix_resolver,
        [
            "if (policy == nullptr)",
            "symlink_prefix_cache_lookup",
            "prefix_mount_ref = find_mount_point(path, end)",
            "policy->reject_remote_mounts && prefix_mount != nullptr && prefix_mount->fs_type == FSType::REMOTE",
            "readlink_resolved_on_mount(path",
            "splice_symlink_target(path",
            "policy == nullptr || policy->reapply_task_root",
            "symlink_resolve_path_is_confined(path, policy)",
        ],
        "confined prefix resolver bypasses cached skips and checks every mount before readlink",
    )

    require_tokens(
        header + WKI_DEV_PROXY_KTEST.read_text(),
        [
            "wki_remote_vfs_selftest_utimens_wire_path_validation()",
            "KTEST(WkiRemoteVfsUtimens, WirePathValidationRejectsEscapes)",
        ],
        "remote utimens malformed-path KTEST coverage",
    )

    perf_header = (ROOT / "modules" / "kern" / "src" / "platform" / "perf" / "perf_events.hpp").read_text()
    perf_source = (ROOT / "modules" / "kern" / "src" / "platform" / "perf" / "perf_events.cpp").read_text()
    require_tokens(
        source + perf_header + perf_source,
        [
            "WkiPerfVfsOp::UTIMENS",
            "WkiPerfVfsServerOp::UTIMENS",
            'return "utimens";',
        ],
        "remote utimens explicit perf classification",
    )


def test_server_message_read_uses_bounded_stack_response() -> None:
    source = REMOTE_VFS_CPP.read_text()
    handler_body = function_body(source, "handle_vfs_op")
    read_start = handler_body.find("case OP_VFS_READ:")
    write_start = handler_body.find("case OP_VFS_WRITE:", read_start)
    if read_start < 0 or write_start < 0:
        fail("remote VFS server read/write opcode cases must remain present")
    read_case = handler_body[read_start:write_start]

    require_order(
        read_case,
        [
            "len = std::min<uint32_t>(len, max_resp_data)",
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> resp_buf __attribute__((uninitialized))",
            "reinterpret_cast<DevOpRespPayload*>(resp_buf.data())",
            "uint8_t* read_buf = resp_buf.data() + sizeof(DevOpRespPayload)",
            "read_local_file_windowed(local_file, read_buf, len",
            "resp->op_id = OP_VFS_READ",
            "resp->status = 0",
            "resp->data_len = static_cast<uint16_t>(BYTES_READ)",
            "resp->status = static_cast<int16_t>(BYTES_READ)",
            "resp->data_len = 0",
            "resp->reserved = REQ_COOKIE",
            "uint16_t const SEND_LEN = (BYTES_READ > 0) ? static_cast<uint16_t>(sizeof(DevOpRespPayload) + BYTES_READ)",
            ": static_cast<uint16_t>(sizeof(DevOpRespPayload))",
            "wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(), SEND_LEN)",
        ],
        "server message-read bounded stack response",
    )
    if re.search(r"\bnew\b|delete\s*\[\s*\]", read_case) or "std::memset(read_buf" in read_case:
        fail("server message read retained heap allocation or redundant full-span clearing")
    if re.search(r"\bresp_buf\s*(?:\{\}|=\s*\{\})", read_case):
        fail("server message read zero-initializes unused response capacity")
    if "WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16" not in source:
        fail("server message-read stack response lacks a kernel-stack headroom bound")


def test_readlink_cache_invalidation_is_generation_based() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    ktest = WKI_DEV_PROXY_KTEST.read_text()

    require_tokens(
        header,
        [
            "uint32_t generation = 0;",
            "static_assert(sizeof(ReadlinkCacheEntry) == 1040);",
            "uint32_t readlink_cache_generation = 1;",
            "wki_remote_vfs_selftest_readlink_cache_generation_invalidation()",
        ],
        "generation-scoped readlink cache layout",
    )
    if "bool valid = false;" in header:
        fail("readlink cache entries must not retain per-row validity flags")

    reset_body = function_body(source, "reset_readlink_cache_entry")
    require_tokens(reset_body, ["entry.generation = 0"], "constant-time readlink entry reset")
    for forbidden in ["entry.status", "entry.target_len", "entry.cached_at_us", "entry.path", "entry.target"]:
        if forbidden in reset_body:
            fail(f"readlink entry reset still clears stale payload field: {forbidden}")

    invalidate_body = function_body(source, "invalidate_readlink_cache_locked")
    require_order(
        invalidate_body,
        [
            "uint32_t const NEXT_GENERATION = state->readlink_cache_generation + 1",
            "if (NEXT_GENERATION != 0)",
            "state->readlink_cache_generation = NEXT_GENERATION",
            "return",
            "for (auto& entry : state->readlink_cache)",
            "reset_readlink_cache_entry(entry)",
            "state->readlink_cache_generation = 1",
        ],
        "O(1) normal invalidation with bounded wrap reset",
    )

    lookup_body = function_body(source, "try_readlink_cache_lookup")
    require_order(
        lookup_body,
        [
            "state->lock.lock()",
            "uint32_t const CACHE_GENERATION = state->readlink_cache_generation",
            "*generation_out = CACHE_GENERATION",
            "entry.generation != CACHE_GENERATION",
            "entry.cached_at_us",
            "state->lock.unlock()",
        ],
        "readlink lookup generation gate",
    )

    cache_body = function_body(source, "cache_readlink_result")
    require_tokens(
        cache_body,
        [
            "expected_generation == 0",
            "entry.generation == CACHE_GENERATION",
            "entry.generation != CACHE_GENERATION",
        ],
        "readlink insertion generation selection",
    )
    require_order(
        cache_body,
        [
            "state->lock.lock()",
            "state->readlink_cache_generation != expected_generation",
            "state->lock.unlock()",
            "return",
            "uint32_t const CACHE_GENERATION = expected_generation",
        ],
        "late readlink result generation fence",
    )
    require_order(
        cache_body,
        [
            "reset_readlink_cache_entry(*selected)",
            "selected->status =",
            "selected->cached_at_us = NOW",
            "memcpy(selected->path.data()",
            "selected->target_len = target_len",
            "memcpy(selected->target.data()",
            "selected->generation = CACHE_GENERATION",
        ],
        "readlink entry generation publication",
    )

    readlink_body = function_body(source, "wki_remote_vfs_readlink_path")
    require_order(
        readlink_body,
        [
            "uint32_t cache_generation = 0",
            "try_readlink_cache_lookup(state, fs_relative_path, buf, bufsize, &cached_result, &cache_generation)",
            "vfs_proxy_read_with_retry(state, OP_VFS_READLINK",
            "cache_readlink_result(state, cache_generation, fs_relative_path, STATUS, nullptr, 0)",
            "cache_readlink_result(state, cache_generation, fs_relative_path, 0",
        ],
        "readlink miss generation carried across RPC",
    )

    require_tokens(
        source,
        [
            "state.readlink_cache_generation = UINT32_MAX",
            "entry.generation == 0",
            "cache_misses(POSITIVE_PATH, nullptr)",
            "cache_misses(WRAP_PATH, nullptr)",
            "lookup_status(NEGATIVE_PATH, -ENOENT)",
            "late_positive_generation",
            "cache_misses(LATE_POSITIVE_PATH, nullptr)",
            "late_negative_generation",
            "cache_misses(LATE_NEGATIVE_PATH, nullptr)",
        ],
        "readlink generation invalidation selftest",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteVfsReadlinkCache, GenerationInvalidationAndWrap)",
            "wki_remote_vfs_selftest_readlink_cache_generation_invalidation()",
        ],
        "readlink generation invalidation KTEST registration",
    )


def test_server_roce_push_reads_reuse_registered_staging() -> None:
    source = REMOTE_VFS_CPP.read_text()
    handler = function_body(source, "handle_vfs_op")
    rdma_start = handler.find("case OP_VFS_READ_RDMA:")
    bulk_start = handler.find("case OP_VFS_READ_BULK:", rdma_start)
    write_start = handler.find("case OP_VFS_WRITE_RDMA:", bulk_start)
    if min(rdma_start, bulk_start, write_start) < 0:
        fail("missing server RDMA/bulk read cases")

    cases = [
        (
            handler[rdma_start:bulk_start],
            "read_staging",
            "wki_dev_server_get_vfs_read_staging_buf(channel_identity)",
            "VFS_RDMA_BOUNCE_SIZE",
        ),
        (
            handler[bulk_start:write_start],
            "bulk_staging",
            "wki_dev_server_get_vfs_bulk_staging_buf(channel_identity)",
            "VFS_RDMA_ROCE_BULK_SIZE",
        ),
    ]
    for case, staging_name, getter, size_token in cases:
        require_tokens(
            case,
            [
                size_token,
                f"uint8_t* {staging_name} = {getter}",
                f"PULL_MODE && {staging_name} == nullptr",
                "uint8_t* allocated_read_buf = nullptr",
                f"uint8_t* read_buf = {staging_name}",
                "if (read_buf == nullptr)",
                "allocated_read_buf = new (std::nothrow) uint8_t[len]",
                "read_buf = allocated_read_buf",
                "read_local_file_windowed(local_file, read_buf, len",
                "if (!PULL_MODE && bytes_read > 0)",
                "wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf",
                "rdma_write(",
                "read_buf,",
                "delete[] allocated_read_buf",
            ],
            "server push read staging reuse",
        )
        if f"PULL_MODE ? {getter}" in case:
            fail("server push reads must fetch registered staging in both modes")
        if "delete[] read_buf" in case:
            fail("server push reads must never delete registered staging")
        require_order(
            case,
            [
                f"uint8_t* {staging_name} = {getter}",
                f"uint8_t* read_buf = {staging_name}",
                "read_local_file_windowed(local_file, read_buf, len",
                "if (!PULL_MODE && bytes_read > 0)",
                "wki_roce_rdma_write_tagged(hdr->src_node, consumer_rkey, 0, read_buf",
                "delete[] allocated_read_buf",
            ],
            "server push read staging lifetime",
        )


def test_remote_metadata_scratch_initializes_only_consumed_prefix() -> None:
    source = REMOTE_VFS_CPP.read_text()

    build_calls = list(re.finditer(r"build_full_path\((\w+)\.data\(\)", source))
    if len(build_calls) != 20:
        fail(f"expected 20 bounded build_full_path outputs, found {len(build_calls)}")
    for call in build_calls:
        name = call.group(1)
        declaration = f"std::array<char, 512> {name} __attribute__((uninitialized));"
        declaration_pos = source.rfind(declaration, 0, call.start())
        if declaration_pos < 0 or call.start() - declaration_pos > 768:
            fail(f"build_full_path output {name} must be a nearby explicitly uninitialized local")
    declaration_counts = {
        "std::array<char, 512> resolved_path __attribute__((uninitialized));": 1,
        "std::array<char, 512> full_path __attribute__((uninitialized));": 9,
        "std::array<char, 512> full_visible_path __attribute__((uninitialized));": 8,
        "std::array<char, 512> full_link __attribute__((uninitialized));": 1,
        "std::array<char, 512> old_full __attribute__((uninitialized));": 1,
        "std::array<char, 512> new_full __attribute__((uninitialized));": 1,
        "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));": 4,
        "std::array<uint8_t, 518> req_stack __attribute__((uninitialized));": 1,
        "std::array<uint8_t, 519> req_stack __attribute__((uninitialized));": 1,
        "std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));": 2,
        "std::array<uint8_t, 514> resp_buf __attribute__((uninitialized));": 1,
    }
    for declaration, expected_count in declaration_counts.items():
        actual_count = source.count(declaration)
        if actual_count != expected_count:
            fail(f"metadata scratch declaration count changed for {declaration}: expected {expected_count}, found {actual_count}")

    build = function_body(source, "build_full_path")
    require_order(build, ["size_t pos = 0", "out[pos] = '\\0'"], "full-path output is terminated after construction")
    require_tokens(
        build,
        [
            "if (EXPORT_LEN > 0 && EXPORT_LEN < out_size - 1)",
            "memcpy(out, export_path, EXPORT_LEN)",
            "if (pos + copy_len >= out_size)",
            "memcpy(out + pos, relative_path, copy_len)",
        ],
        "bounded full-path construction",
    )

    boundary_start = source.find("bool path_crosses_remote_mount(")
    boundary_end = source.find("bool path_crosses_remote_mount_direct(", boundary_start)
    if boundary_start < 0 or boundary_end < 0:
        fail("missing recursive-mount boundary helpers")
    boundary = source[boundary_start:boundary_end]
    require_tokens(
        boundary,
        [
            "std::array<char, 512> resolved_path __attribute__((uninitialized));",
            "const char* mount_path = path",
            "resolve_mount_path(path, resolved_path.data(), resolved_path.size()) == 0",
            "mount_path = resolved_path.data()",
        ],
        "recursive-mount path scratch",
    )
    require_order(boundary, ["resolve_mount_path(", "mount_path = resolved_path.data()"], "resolved path used only on success")
    require_order(boundary, ["mount_path = resolved_path.data()", "find_mount_point(mount_path)"], "resolved path built before lookup")

    path_request = [
        "memcpy(req_data, &path_len, sizeof(uint16_t))",
        "if (path_len > 0)",
        "memcpy(req_data + 2, fs_relative_path, path_len)",
    ]
    request_specs = {
        "remote_vfs_stat_on_proxy": (
            "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(2 + path_len)",
            path_request + ["vfs_proxy_send_and_wait(state, OP_VFS_STAT, req_data, req_data_len"],
        ),
        "wki_remote_vfs_mkdir": (
            "std::array<uint8_t, 518> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(6 + path_len)",
            [
                "memcpy(req_data, &u_mode, sizeof(uint32_t))",
                "memcpy(req_data + 4, &path_len, sizeof(uint16_t))",
                "if (path_len > 0)",
                "memcpy(req_data + 6, fs_relative_path, path_len)",
                "vfs_proxy_send_and_wait(state, OP_VFS_MKDIR, req_data, req_data_len",
            ],
        ),
        "wki_remote_vfs_chmod": (
            "std::array<uint8_t, 519> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(7 + path_len)",
            [
                "memcpy(req_data, &u_mode, sizeof(uint32_t))",
                "memcpy(req_data + 4, &FLAGS, sizeof(uint8_t))",
                "memcpy(req_data + 5, &path_len, sizeof(uint16_t))",
                "if (path_len > 0)",
                "memcpy(req_data + 7, fs_relative_path, path_len)",
                "vfs_proxy_send_and_wait(state, OP_VFS_CHMOD, req_data, req_data_len",
            ],
        ),
        "wki_remote_vfs_symlink": (
            "std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(4 + target_len + link_len)",
            [
                "memcpy(req_data, &target_len, sizeof(uint16_t))",
                "if (target_len > 0)",
                "memcpy(req_data + 2, target, target_len)",
                "memcpy(req_data + 2 + target_len, &link_len, sizeof(uint16_t))",
                "if (link_len > 0)",
                "memcpy(req_data + 4 + target_len, fs_relative_path, link_len)",
                "vfs_proxy_send_and_wait(state, OP_VFS_SYMLINK, req_data, req_data_len",
            ],
        ),
        "wki_remote_vfs_unlink": (
            "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(2 + path_len)",
            path_request + ["vfs_proxy_send_and_wait(state, OP_VFS_UNLINK, req_data, req_data_len"],
        ),
        "wki_remote_vfs_rmdir": (
            "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(2 + path_len)",
            path_request + ["vfs_proxy_send_and_wait(state, OP_VFS_RMDIR, req_data, req_data_len"],
        ),
        "wki_remote_vfs_rename": (
            "std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(4 + old_len + new_len)",
            [
                "memcpy(req_data, &old_len, sizeof(uint16_t))",
                "if (old_len > 0)",
                "memcpy(req_data + 2, old_fs_path, old_len)",
                "memcpy(req_data + 2 + old_len, &new_len, sizeof(uint16_t))",
                "if (new_len > 0)",
                "memcpy(req_data + 4 + old_len, new_fs_path, new_len)",
                "vfs_proxy_send_and_wait(state, OP_VFS_RENAME, req_data, req_data_len",
            ],
        ),
        "wki_remote_vfs_readlink_path": (
            "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));",
            "auto req_data_len = static_cast<uint16_t>(2 + path_len)",
            path_request + ["vfs_proxy_read_with_retry(state, OP_VFS_READLINK, req_data, req_data_len"],
        ),
    }
    for function_name, (declaration, length_assignment, encoding) in request_specs.items():
        body = function_body(source, function_name)
        require_tokens(
            body,
            [declaration, "req_data_len", "uint8_t* req_data = req_stack.data()", "memcpy(req_data"],
            f"{function_name} exact-prefix request scratch",
        )
        require_order(
            body,
            [declaration, length_assignment, "uint8_t* req_data = req_stack.data()"] + encoding,
            f"{function_name} exact-prefix request encoding",
        )
        if re.search(r"std::array<uint8_t,\s*(?:514|518|519|1028)>\s+req_stack\s*(?:\{\}|=\s*\{\})", body):
            fail(f"{function_name} must not initialize an unused request tail")

    readlink = function_body(source, "wki_remote_vfs_readlink_path")
    require_tokens(
        readlink,
        [
            "std::array<uint8_t, 514> resp_buf __attribute__((uninitialized));",
            "uint16_t resp_len = 0",
            "if (resp_len < 2)",
            "if (target_len == 0 || target_len + 2 > resp_len)",
        ],
        "readlink exact-response scratch",
    )
    require_order(readlink, ["vfs_proxy_read_with_retry", "if (resp_len < 2)"], "readlink response fill before header read")
    require_order(
        readlink,
        ["if (target_len == 0 || target_len + 2 > resp_len)", "memcpy(buf, resp_buf.data() + 2"],
        "readlink bounds before payload read",
    )

    require_tokens(
        source,
        [
            "std::array<char, 512> target_str{};",
            "ker::vfs::Stat kernel_buf{};",
        ],
        "metadata buffers whose initialized tails remain observable",
    )


def test_write_behind_storage_grows_in_allocator_shaped_classes() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    ktest = WKI_DEV_PROXY_KTEST.read_text()

    require_tokens(
        header,
        [
            "#include <memory>",
            "#include <platform/sys/mutex.hpp>",
            "uint32_t capacity = 0;",
            "std::unique_ptr<uint8_t[]> data{};",
            "ker::mod::sys::Mutex io_lock;",
            "std::atomic<bool> cache_invalidation_pending{false};",
            "wki_remote_vfs_selftest_write_behind_capacity_classes()",
            "wki_remote_vfs_selftest_write_behind_growth()",
        ],
        "adaptive remote VFS write-behind storage",
    )
    if "std::array<uint8_t, VFS_WRITE_BEHIND_SIZE> data" in header:
        fail("remote VFS write-behind must not embed and zero the maximum-size storage")

    require_tokens(
        source,
        [
            "constexpr std::array<uint32_t, 8> VFS_WRITE_BEHIND_CAPACITIES",
            "28U * 1024U",
            "60U * 1024U",
            "124U * 1024U",
            "252U * 1024U",
            "508U * 1024U",
            "1020U * 1024U",
            "2044U * 1024U",
            "4092U * 1024U",
            "static_assert(VFS_WRITE_BEHIND_MAX_CAPACITY < VFS_WRITE_BEHIND_SIZE)",
        ],
        "allocator-shaped remote VFS write-behind capacities",
    )
    capacity_body = function_body(source, "write_behind_capacity_for")
    require_order(
        capacity_body,
        [
            "for (uint32_t const CAPACITY : VFS_WRITE_BEHIND_CAPACITIES)",
            "if (required_bytes <= CAPACITY)",
            "return CAPACITY",
            "return VFS_WRITE_BEHIND_MAX_CAPACITY",
        ],
        "saturating remote VFS write-behind capacity selection",
    )

    install_body = function_body(source, "install_write_behind_storage")
    require_order(
        install_body,
        [
            "replacement == nullptr",
            "wb->pending_len > replacement_capacity",
            "memcpy(replacement.get(), wb->data.get(), wb->pending_len)",
            "wb->data = std::move(replacement)",
            "wb->capacity = replacement_capacity",
        ],
        "remote VFS write-behind growth commit",
    )
    reserve_body = function_body(source, "try_reserve_write_behind")
    require_order(
        reserve_body,
        [
            "write_behind_capacity_for(required_bytes)",
            "TARGET_CAPACITY <= wb->capacity",
            "new (std::nothrow) uint8_t[TARGET_CAPACITY]",
            "install_write_behind_storage(wb, std::move(replacement), TARGET_CAPACITY)",
        ],
        "remote VFS write-behind temporary growth",
    )

    write_body = function_body(source, "remote_vfs_write")
    require_order(
        write_body,
        [
            "bool allow_write_behind_growth = true",
            "bool const IS_SEQUENTIAL",
            "if (!IS_SEQUENTIAL)",
            "flush_write_behind(ctx)",
            "continue",
            "uint64_t const REQUIRED_CAPACITY",
            "!try_reserve_write_behind(wb, REQUIRED_CAPACITY)",
            "allow_write_behind_growth = false",
            "uint32_t const SPACE = wb->capacity - wb->pending_len",
            "memcpy(wb->data.get() + wb->pending_len, src, TO_BUFFER)",
            "total_written += TO_BUFFER",
            "if (wb->pending_len >= wb->capacity)",
            "flush_write_behind(ctx)",
            "return total_written",
        ],
        "adaptive remote VFS sequential write path",
    )
    if "wb->data.data()" in write_body:
        fail("adaptive remote VFS write path must use the owning storage pointer")
    if "remaining >= VFS_WRITE_BEHIND_SIZE" in write_body:
        fail("large remote VFS writes must continue through the RDMA-capable write-behind path")
    if "WRITTEN_BEFORE_BUFFER" in write_body:
        fail("remote VFS must report bytes already accepted into write-behind after a flush failure")

    for function_name in [
        "remote_vfs_read",
        "remote_vfs_write",
        "remote_vfs_truncate",
        "remote_vfs_fsync_file",
        "wki_remote_vfs_fstat",
    ]:
        require_order(
            function_body(source, function_name),
            [
                "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
                "consume_remote_file_cache_invalidation(ctx)",
            ],
            f"{function_name} adaptive write-behind serialization and deferred invalidation",
        )
    require_order(
        function_body(source, "remote_vfs_lseek"),
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "consume_remote_file_cache_invalidation(ctx)",
            "int const FLUSH_STATUS = flush_write_behind(ctx)",
            "if (FLUSH_STATUS != 0)",
            "return FLUSH_STATUS",
            "vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_SEEK_END",
        ],
        "remote VFS SEEK_END flush-before-operation serialization",
    )
    require_order(
        function_body(source, "remote_vfs_read"),
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "consume_remote_file_cache_invalidation(ctx)",
            "int const FLUSH_STATUS = flush_write_behind(ctx)",
            "if (FLUSH_STATUS != 0)",
            "return FLUSH_STATUS",
            "vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ_BULK",
        ],
        "remote VFS read flush-before-operation serialization",
    )
    require_order(
        function_body(source, "remote_vfs_truncate"),
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "consume_remote_file_cache_invalidation(ctx)",
            "int const FLUSH_STATUS = flush_write_behind(ctx)",
            "if (FLUSH_STATUS != 0)",
            "return FLUSH_STATUS",
            "vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_TRUNCATE",
        ],
        "remote VFS truncate flush-before-operation serialization",
    )
    require_order(
        function_body(source, "remote_vfs_fsync_file"),
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "consume_remote_file_cache_invalidation(ctx)",
            "int const FLUSH_STATUS = flush_write_behind(ctx)",
            "if (FLUSH_STATUS != 0)",
            "return FLUSH_STATUS",
            "vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_FSYNC",
        ],
        "remote VFS fsync flush-before-operation serialization",
    )
    consume_body = function_body(source, "consume_remote_file_cache_invalidation")
    require_order(
        consume_body,
        [
            "ctx->cache_invalidation_pending.exchange(false, std::memory_order_acq_rel)",
            "invalidate_remote_file_open_caches(ctx)",
        ],
        "task-context remote VFS cache invalidation consumption",
    )
    invalidate_body = function_body(source, "wki_remote_vfs_invalidate_open_file_caches")
    require_tokens(
        invalidate_body,
        ["ctx->cache_invalidation_pending.store(true, std::memory_order_release)"],
        "RX-safe remote VFS cache invalidation deferral",
    )
    if "MutexGuard" in invalidate_body or "invalidate_remote_file_open_caches(ctx)" in invalidate_body:
        fail("RX-reachable remote VFS cache invalidation must not block or free per-open storage")

    require_tokens(
        source,
        [
            "CapacityCase{1, 28U * 1024U}",
            "CapacityCase{16U * 1024U, 28U * 1024U}",
            "CapacityCase{28U * 1024U, 28U * 1024U}",
            "CapacityCase{(28U * 1024U) + 1U, 60U * 1024U}",
            "CapacityCase{60U * 1024U, 60U * 1024U}",
            "CapacityCase{(60U * 1024U) + 1U, 124U * 1024U}",
            "CapacityCase{64U * 1024U, 124U * 1024U}",
            "CapacityCase{124U * 1024U, 124U * 1024U}",
            "CapacityCase{(124U * 1024U) + 1U, 252U * 1024U}",
            "CapacityCase{128U * 1024U, 252U * 1024U}",
            "CapacityCase{252U * 1024U, 252U * 1024U}",
            "CapacityCase{(252U * 1024U) + 1U, 508U * 1024U}",
            "CapacityCase{256U * 1024U, 508U * 1024U}",
            "CapacityCase{508U * 1024U, 508U * 1024U}",
            "CapacityCase{(508U * 1024U) + 1U, 1020U * 1024U}",
            "CapacityCase{1020U * 1024U, 1020U * 1024U}",
            "CapacityCase{(1020U * 1024U) + 1U, 2044U * 1024U}",
            "CapacityCase{1U * 1024U * 1024U, 2044U * 1024U}",
            "CapacityCase{2044U * 1024U, 2044U * 1024U}",
            "CapacityCase{(2044U * 1024U) + 1U, 4092U * 1024U}",
            "CapacityCase{4092U * 1024U, 4092U * 1024U}",
            "CapacityCase{(4092U * 1024U) + 1U, 4092U * 1024U}",
            "CapacityCase{UINT64_MAX, 4092U * 1024U}",
        ],
        "remote VFS write-behind capacity selftest boundaries",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteVfsWriteBehind, CapacityClassesMatchAllocator)",
            "wki_remote_vfs_selftest_write_behind_capacity_classes()",
            "KTEST(WkiRemoteVfsWriteBehind, GrowthPreservesPendingData)",
            "wki_remote_vfs_selftest_write_behind_growth()",
        ],
        "remote VFS write-behind KTEST coverage",
    )


def test_message_write_flush_uses_split_send_and_retains_tail() -> None:
    source = REMOTE_VFS_CPP.read_text()
    flush_body = function_body(source, "flush_write_behind")
    require_order(
        flush_body,
        [
            "uint32_t const CHUNK = std::min(remaining, VFS_RDMA_WRITE_SIZE)",
            "vfs_proxy_write_rdma_and_wait(ctx->proxy, ctx->remote_fd, cur_offset, src, CHUNK, &written)",
            "constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12",
            "auto max_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD)",
            "uint32_t const CHUNK = (remaining > max_data) ? max_data : remaining",
            "std::array<uint8_t, 12> req_prefix{}",
            "memcpy(req_prefix.data(), &remote_fd, sizeof(int32_t))",
            "memcpy(req_prefix.data() + 4, &cur_offset, sizeof(int64_t))",
            "vfs_proxy_send_split_and_wait(ctx->proxy, OP_VFS_WRITE, req_prefix.data(), req_prefix.size(), src, CHUNK",
        ],
        "RDMA and message write-behind chunk bounds",
    )
    for forbidden in [
        "new (std::nothrow) uint8_t[req_data_len]",
        "delete[] req_data",
        "memcpy(req_data + 12, src, CHUNK)",
    ]:
        if forbidden in flush_body:
            fail(f"message write flush retained request allocation/copy: {forbidden}")
    require_order(
        flush_body,
        [
            "vfs_proxy_send_split_and_wait(",
            "if (STATUS != 0)",
            "keep_pending_tail(src, remaining, cur_offset)",
            "return STATUS",
            "written == 0 || written > CHUNK",
            "keep_pending_tail(src, remaining, cur_offset)",
            "return -EIO",
        ],
        "message-mode write-behind failure tail retention",
    )

    send_body = function_body(source, "vfs_proxy_send_and_wait")
    require_order(
        send_body,
        [
            "req_data_len > MAX_REQ_DATA_LEN",
            "req_tail_len > MAX_REQ_DATA_LEN - req_data_len",
            "req_tail_len > 0 && req_tail == nullptr",
            "static_cast<uint16_t>(req_data_len + req_tail_len)",
            "size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + req_data_len",
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized))",
            "memcpy(req_buf.data() + sizeof(DevOpReqPayload), req_data, req_data_len)",
            "if (req_tail_len == 0)",
            "wki_send_on_channel_identity(",
            "wki_send_on_channel_identity_split(",
        ],
        "generic proxy RPC split request assembly",
    )
    split_body = function_body(source, "vfs_proxy_send_split_and_wait")
    require_order(
        split_body,
        [
            "vfs_proxy_send_and_wait(",
            "req_prefix, req_prefix_len",
            "VFS_PROXY_OP_TIMEOUT_US",
            "req_tail, req_tail_len",
        ],
        "write-only split RPC wrapper",
    )
    require_tokens(
        flush_body,
        [
            "wb->data.get()",
            "wb->pending_len > wb->capacity",
            "remote_vfs_invalidate_cached_stat(ctx)",
            "written == 0 || written > CHUNK",
        ],
        "dynamic write-behind flush bounds",
    )
    if flush_body.count("written == 0 || written > CHUNK") != 2:
        fail("both RDMA and message write-behind flushes must reject oversized write responses")


def test_remote_open_closes_server_fd_on_local_allocation_failure() -> None:
    source = REMOTE_VFS_CPP.read_text()
    helper_body = function_body(source, "remote_vfs_close_remote_fd_best_effort")
    open_body = function_body(source, "wki_remote_vfs_open_path")

    require_order(
        helper_body,
        [
            "if (state == nullptr || remote_fd < 0)",
            "return",
            "vfs_proxy_send_and_wait(state, OP_VFS_CLOSE",
            "reinterpret_cast<const uint8_t*>(&remote_fd)",
            "sizeof(remote_fd)",
            "nullptr, 0",
        ],
        "remote open cleanup close helper",
    )
    require_order(
        open_body,
        [
            "auto* file = new (std::nothrow) ker::vfs::File{}",
            "if (file == nullptr)",
            "remote_vfs_close_remote_fd_best_effort(state, open_resp.fd)",
            "return nullptr",
            "auto* ctx = new (std::nothrow) RemoteFileContext{}",
        ],
        "remote open closes fd when File allocation fails",
    )
    require_order(
        open_body,
        [
            "auto* ctx = new (std::nothrow) RemoteFileContext{}",
            "if (ctx == nullptr)",
            "delete file",
            "remote_vfs_close_remote_fd_best_effort(state, open_resp.fd)",
            "return nullptr",
            "ctx->proxy = state",
        ],
        "remote open closes fd when context allocation fails",
    )


def test_remote_close_waits_only_for_writable_output_publication() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = REMOTE_VFS_HPP.read_text()
    ktest = WKI_DEV_PROXY_KTEST.read_text()
    wire = WIRE_HPP.read_text()
    close_body = function_body(source, "remote_vfs_close")
    policy_body = function_body(source, "remote_vfs_close_needs_completion")

    require_order(
        close_body,
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "flush_status = flush_write_behind(ctx)",
            "int32_t const REMOTE_FD = ctx->remote_fd",
            "bool const NEEDS_CLOSE_STATUS = remote_vfs_close_needs_completion(f->open_flags)",
            "if (NEEDS_CLOSE_STATUS)",
            "close_status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_CLOSE",
            "sizeof(REMOTE_FD), nullptr, 0)",
            "std::array<uint8_t, WKI_VFS_CLOSE_EXTENDED_DATA_LEN> close_req{}",
            "memcpy(close_req.data(), &REMOTE_FD, sizeof(REMOTE_FD))",
            "close_req.at(WKI_VFS_CLOSE_FLAGS_OFFSET) = WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE",
            "vfs_proxy_send_untracked(ctx->proxy, OP_VFS_CLOSE",
            "close_req.data(), close_req.size()",
            "delete ctx->write_buf",
            "delete ctx;",
            "release_vfs_proxy_open_ref(PROXY)",
            "return flush_status != 0 ? flush_status : close_status",
        ],
        "remote close publication ordering",
    )
    if close_body.count("ker::mod::sys::MutexGuard io_guard(ctx->io_lock)") != 2:
        fail("both inactive and normal remote close cleanup must serialize per-file storage")
    require_tokens(
        policy_body,
        [
            "int const ACCESS_MODE = open_flags & 3",
            "ACCESS_MODE != 0",
            "ker::vfs::O_CREAT | ker::vfs::O_TRUNC",
        ],
        "writable remote close response policy",
    )
    require_tokens(
        header + ktest,
        [
            "wki_remote_vfs_selftest_writable_close_wait_policy()",
            "KTEST(WkiRemoteVfsClose, WritableWaitsForOwnerPublication)",
        ],
        "writable remote close KTEST coverage",
    )

    require_tokens(
        wire,
        [
            "WKI_VFS_CLOSE_LEGACY_DATA_LEN = sizeof(int32_t)",
            "WKI_VFS_CLOSE_FLAGS_OFFSET = WKI_VFS_CLOSE_LEGACY_DATA_LEN",
            "WKI_VFS_CLOSE_EXTENDED_DATA_LEN = WKI_VFS_CLOSE_FLAGS_OFFSET + sizeof(uint8_t)",
            "WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE = 0x01",
            "wki_vfs_close_no_success_response_requested",
        ],
        "backward-compatible remote close request extension",
    )

    server_body = function_body(source, "handle_vfs_op")
    close_case_start = server_body.index("case OP_VFS_CLOSE:")
    close_case = server_body[close_case_start : server_body.index("case OP_VFS_READDIR:", close_case_start)]
    require_order(
        close_case,
        [
            "if (data_len < WKI_VFS_CLOSE_LEGACY_DATA_LEN)",
            "send_simple_resp(resp)",
            "memcpy(&fd_id, data, sizeof(int32_t))",
            "wki_vfs_close_no_success_response_requested(data, data_len)",
            "rfd->file = nullptr",
            "s_vfs_lock.unlock()",
            "close_file->fops->vfs_close(close_file)",
            "perf_record_vfs_server_end",
            "if (status != 0 || !NO_SUCCESS_RESPONSE)",
            "send_simple_resp(resp)",
        ],
        "owner close completes before suppressing only a successful opted-in response",
    )


def test_export_lookup_returns_locked_snapshot() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()

    require_tokens(
        header,
        ["auto wki_remote_vfs_find_export_snapshot(uint32_t resource_id, VfsExport* out) -> bool;"],
        "remote VFS export snapshot declaration",
    )
    if "auto wki_remote_vfs_find_export(uint32_t resource_id) -> VfsExport*" in header + source:
        fail("remote VFS export lookup must not return an unlocked pointer into g_vfs_exports")

    body = function_body(source, "wki_remote_vfs_find_export_snapshot")
    require_order(
        body,
        [
            "if (out == nullptr)",
            "return false",
            "s_vfs_lock.lock()",
            "for (const auto& exp : g_vfs_exports)",
            "if (exp.active && exp.resource_id == resource_id && exp.publication_revision == REVISION)",
            "*out = exp",
            "s_vfs_lock.unlock()",
            "return true",
            "s_vfs_lock.unlock()",
            "return false",
        ],
        "remote VFS export snapshot locking",
    )
    if "return &exp" in body:
        fail("remote VFS export snapshot helper must not return a pointer into g_vfs_exports")


def test_rdma_retry_cooldowns_are_saturating() -> None:
    source = REMOTE_VFS_CPP.read_text()
    body = function_body(source, "remote_vfs_rdma_note_transient_failure")

    require_order(
        body,
        [
            "uint32_t const FAILURES = failure_count.fetch_add(1, std::memory_order_acq_rel) + 1",
            "uint32_t const SHIFT = std::min<uint32_t>(FAILURES - 1, VFS_RDMA_TRANSIENT_COOLDOWN_SHIFT_MAX)",
            "uint64_t const COOLDOWN_US = std::min<uint64_t>(VFS_RDMA_TRANSIENT_COOLDOWN_BASE_US << SHIFT, VFS_RDMA_TRANSIENT_COOLDOWN_MAX_US)",
            "retry_after_us.store(wki_future_deadline_us(wki_now_us(), COOLDOWN_US), std::memory_order_release)",
            "return COOLDOWN_US",
        ],
        "RDMA transient failure cooldown",
    )
    if "retry_after_us.store(wki_now_us() + COOLDOWN_US" in body:
        fail("RDMA retry cooldown must not use wrapping deadline arithmetic")

    retry_ready_body = function_body(source, "remote_vfs_rdma_retry_ready")
    require_order(
        retry_ready_body,
        [
            "uint64_t const RETRY_AFTER_US = retry_after_us.load(std::memory_order_acquire)",
            "return RETRY_AFTER_US == 0 || now_us >= RETRY_AFTER_US",
        ],
        "RDMA retry gate",
    )


def test_vfs_attach_ack_requires_expected_cookie_before_completion() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    ktest = WKI_DEV_PROXY_KTEST.read_text()
    require_tokens(
        header,
        [
            "uint8_t attach_expected_cookie = 0;",
            "auto wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion() -> bool;",
        ],
        "remote VFS attach-cookie state",
    )
    require_tokens(
        source,
        [
            "uint8_t g_vfs_attach_next_cookie = 1;",
            "auto allocate_vfs_attach_cookie_locked(uint16_t owner_node, uint32_t resource_id,",
            "auto vfs_attach_ack_matches_pending_locked(ProxyVfsState const* state, const DevAttachAckPayload& ack, const uint8_t* payload",
            "state->attach_expected_cookie == 0",
            "wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)",
            "attach_req.attach_cookie = attach_cookie",
        ],
        "remote VFS attach-cookie scaffolding",
    )
    require_order(
        function_body(source, "mount_vfs_proxy_lane"),
        [
            "attach_cookie = allocate_vfs_attach_cookie_locked(owner_node, resource_id, owner_incarnation)",
            "state->attach_expected_cookie = attach_cookie",
            "attach_req.attach_cookie = attach_cookie",
            "wki_send_on_channel_identity(resource_channel_identity, MsgType::DEV_ATTACH_REQ",
        ],
        "remote VFS attach arms cookie before send",
    )
    require_order(
        function_body(source, "handle_vfs_attach_ack"),
        [
            "find_vfs_proxy_by_attach(hdr->src_node, ack->resource_id, ack->reserved)",
            "if (!vfs_attach_ack_matches_pending_locked(state, *ack, payload, payload_len))",
            "wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry)",
            "state->attach_status = ack->status",
            "state->attach_expected_cookie = 0",
            "state->attach_pending.store(false, std::memory_order_release)",
        ],
        "remote VFS attach ACK validates cookie before completion",
    )
    allocator = function_body(source, "allocate_vfs_attach_cookie_locked")
    require_tokens(
        allocator,
        [
            "attempt < UINT8_MAX",
            "proxy->owner_node != owner_node",
            "proxy->resource_id != resource_id",
            "proxy->binding_attach_cookie != cookie",
            "wki_resource_incarnation_equal(EXISTING_INCARNATION, owner_incarnation)",
            "return 0",
        ],
        "remote VFS attach-cookie wrap exclusion",
    )
    mount_body = function_body(source, "mount_vfs_proxy_lane")
    require_order(
        mount_body,
        [
            "attach_cookie = allocate_vfs_attach_cookie_locked(owner_node, resource_id, owner_incarnation)",
            "state->binding_attach_cookie = attach_cookie",
        ],
        "remote VFS cookie reservation publishes under the registry lock",
    )
    require_tokens(mount_body, ["if (attach_cookie == 0)", "return -EBUSY"], "remote VFS cookie exhaustion")
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteVfsAttachAck, CookieFencesStaleMountCompletion)",
            "wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion()",
        ],
        "remote VFS attach-cookie KTEST coverage",
    )


def test_remote_vfs_unmount_cancels_waiters_before_teardown() -> None:
    source = REMOTE_VFS_CPP.read_text()

    teardown_body = function_body(source, "deactivate_vfs_proxy_locked")
    require_order(
        teardown_body,
        [
            "teardown.state = state",
            "teardown.owner_node = state->owner_node",
            "teardown.assigned_channel_ref = state->assigned_channel_ref",
            "teardown.assigned_channel_generation = state->assigned_channel_generation",
            "teardown.binding_incarnation = vfs_detach_incarnation_snapshot_locked(state)",
            "state->lifecycle_refs++",
            "state->lock.lock()",
            "state->active = false",
            "if (state->op_pending.load(std::memory_order_acquire))",
            "teardown.op_wait_entry = state->op_wait_entry",
            "state->op_wait_entry = nullptr",
            "teardown.op_wait_claimed = wki_claim_op(teardown.op_wait_entry)",
            "clear_proxy_op_state_locked(state, -1)",
            "state->op_retiring_wait_entry = teardown.op_wait_entry",
            "state->op_retiring_waiter_pid = OP_WAITER_PID",
            "if (state->attach_pending.load(std::memory_order_acquire))",
            "teardown.had_attach_pending = true",
            "teardown.attach_wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry)",
            "clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY))",
            "teardown.op_slot_waiter_pids = state->op_slot_waiter_pids",
            "state->op_slot_waiter_count = 0",
            "state->destroy_when_idle = true",
            "state->lock.unlock()",
        ],
        "remote VFS proxy deactivation",
    )
    require_tokens(
        function_body(source, "find_vfs_proxy_by_mount"),
        ["p->lane_anchor", "p->mount_configured", "!p->destroy_when_idle", "!p->mount_released"],
        "path unmount lookup retains the physical mount anchor",
    )

    group_claim = function_body(source, "claim_vfs_proxy_group_unmount_locked")
    require_order(
        group_claim,
        [
            "mark_vfs_proxy_group_unavailable_locked(anchor)",
            "for (auto& proxy : g_vfs_proxies)",
            "state->mount_group_id != anchor->mount_group_id",
            "deactivate_vfs_proxy_locked(state, teardown, true)",
            "stage_vfs_detach_locked(state, teardown.owner_node, teardown.resource_id",
            "state->mount_released = true",
            "invalidate_all_dir_caches(state)",
        ],
        "remote VFS group unmount claim",
    )
    require_tokens(
        group_claim,
        [
            "group.count >= group.lanes.size()",
            "state->attach_pending.load(std::memory_order_acquire)",
            "teardown.had_attach_pending",
        ],
        "remote VFS group teardown includes every bounded lane",
    )
    require_tokens(source, ["struct PendingVfsProxyGroupTeardown"], "remote VFS fixed group teardown storage")

    lane_finish = function_body(source, "finish_vfs_proxy_lane_teardown")
    require_order(
        lane_finish,
        [
            "finish_proxy_teardown_op_waiter(teardown, -1)",
            "vfs_stream_cache_invalidate_remote_scope(teardown.state)",
            "wake_proxy_slot_waiters(teardown)",
            "finish_claimed_waiter(teardown.attach_wait_entry, -1)",
            "teardown.detach_staged",
            "wki_deferred_work_notify()",
            "wki_channel_close_generation(teardown.assigned_channel_ref",
        ],
        "remote VFS per-lane teardown order",
    )

    unmount_body = function_body(source, "finish_vfs_proxy_group_unmount")
    require_order(
        unmount_body,
        [
            "for (size_t i = 0; i < group.count; ++i)",
            "finish_vfs_proxy_lane_teardown(group.lanes.at(i), group.detach_remote.at(i))",
            "ker::vfs::unmount_filesystem_by_private_data(static_cast<const void*>(group.anchor))",
            "mark_vfs_proxy_mount_released_and_maybe_destroy(state)",
            "mark_vfs_proxy_mount_released_and_maybe_destroy(group.anchor)",
            "release_vfs_proxy_lifecycle_ref(state)",
            "release_vfs_proxy_lifecycle_ref(group.anchor)",
        ],
        "remote VFS group teardown unmounts exactly its anchor",
    )

    generation_claim = function_body(source, "claim_vfs_proxy_unmount_by_generation")
    require_tokens(
        generation_claim,
        [
            "state->lane_anchor",
            "state->mount_configured",
            "state->destroy_when_idle",
            "state->mount_released",
            "state->resource_generation != resource_generation",
        ],
        "generation-bound remote VFS teardown selection",
    )
    require_order(
        generation_claim,
        [
            "claim_vfs_proxy_group_unmount_locked(anchor, group)",
            "s_vfs_lock.unlock()",
        ],
        "generation-bound remote VFS unmount reserves its whole group before registry unlock",
    )


def test_vfs_detach_uses_exact_negotiated_incarnation_form() -> None:
    source = REMOTE_VFS_CPP.read_text()
    body = function_body(source, "send_vfs_detach")
    require_tokens(
        body,
        [
            "wki_dev_detach_payload_size(true)",
            "det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie",
            "wki_resource_incarnation_negotiated(owner_node, ResourceType::VFS)",
            "wki_resource_incarnation_valid(resource_incarnation)",
            "det_buf.data() + WKI_DEV_DETACH_INCARNATION_OFFSET",
            "wki_dev_detach_payload_size(WITH_INCARNATION)",
            "wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data(), DETACH_SIZE, tx_token_out)",
        ],
        "negotiated VFS detach suffix",
    )

    discard = function_body(source, "discard_failed_attached_proxy")
    require_tokens(
        discard,
        [
            "binding_incarnation = vfs_detach_incarnation_snapshot_locked(state)",
            "stage_vfs_detach_locked(state, owner_node, resource_id, attach_cookie, binding_incarnation, false)",
        ],
        "failed attached VFS detach binding snapshot",
    )
    snapshot = function_body(source, "vfs_detach_incarnation_snapshot_locked")
    require_tokens(
        snapshot,
        [
            "wki_resource_incarnation_valid(state->binding_incarnation)",
            "state->binding_incarnation",
            "state->attach_expected_incarnation",
        ],
        "epoch-marked in-progress attach retains its exact requested incarnation",
    )
    require_order(
        discard,
        [
            "stage_vfs_detach_locked(state, owner_node, resource_id, attach_cookie, binding_incarnation, false)",
            "state->active = false",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
        ],
        "failed attached VFS rollback reserves before inactive publication",
    )
    mount_body = function_body(source, "mount_vfs_proxy_lane")
    require_order(
        mount_body,
        [
            "create_vfs_proxy_state_locked(owner_node, resource_id, RESOURCE_GENERATION",
            "state->lanes.fill(nullptr)",
            "state->lanes_ready = ANCHOR_READY",
            'ker::vfs::mount_filesystem(local_mount_path, "remote"',
            "state->mount_configured = true",
            "wki_peer_lifecycle_acquire(final_peer)",
            "wki_resource_observation_is_live",
            "wki_remote_vfs_unmount_resource_generation",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "mount publication must stay pinned through exact resource-generation validation and rollback",
    )
    MOUNT_CALL = 'ker::vfs::mount_filesystem(local_mount_path, "remote"'
    if mount_body.find("state->lanes_ready = ANCHOR_READY") > mount_body.find(MOUNT_CALL):
        fail("remote VFS lane zero must be ready before its VFS row is published")
    if mount_body.find("state->mount_configured = true") < mount_body.find(MOUNT_CALL):
        fail("mount_configured must not expose an anchor before mount_filesystem returns")
    validation_start = mount_body.find("wki_peer_lifecycle_acquire(final_peer)")
    if mount_body.find("release_vfs_proxy_lifecycle_ref(state)", validation_start) < validation_start:
        fail("mount publication must not drop its construction pin before final peer/resource validation")

    cleanup_body = function_body(source, "wki_remote_vfs_cleanup_for_peer")
    require_order(
        cleanup_body,
        [
            "deactivate_vfs_proxy_locked(p, cleanup, false)",
            "invalidate_all_dir_caches(p)",
            "s_vfs_lock.unlock()",
            "finish_proxy_teardown_op_waiter(cleanup, -1)",
            "wake_proxy_slot_waiters(cleanup)",
            "finish_claimed_waiter(cleanup.attach_wait_entry, -1)",
            "wki_channel_close_generation(cleanup.assigned_channel_ref",
            "release_vfs_proxy_lifecycle_ref(cleanup.state)",
        ],
        "remote VFS peer cleanup teardown order",
    )


def test_remote_vfs_teardown_releases_rdma_state_when_idle() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()

    require_tokens(
        header,
        [
            "std::atomic<uint32_t> open_file_refs{0};",
            "uint32_t lifecycle_refs = 0;",
            "bool destroy_when_idle = false;",
            "bool mount_released = false;",
            "bool resources_releasing = false;",
            "bool resources_released = false;",
        ],
        "remote VFS proxy lifetime state",
    )

    release_body = function_body(source, "release_vfs_proxy_buffers")
    require_tokens(
        release_body,
        [
            "RDMA_TRANSPORT->rdma_unregister_region",
            "state->rdma_read_rkey, VFS_RDMA_BOUNCE_SIZE",
            "state->rdma_bulk_rkey, state->rdma_bulk_size",
            "state->rdma_transport = nullptr",
            "state->rdma_server_write_rkey = 0",
            "state->rdma_server_read_staging_rkey = 0",
            "state->rdma_server_bulk_staging_rkey = 0",
            "state->bulk_owner_fd = -1",
            "state->shared_io_in_use.store(false, std::memory_order_release)",
            "state->op_untracked_send_pending.store(false, std::memory_order_release)",
        ],
        "remote VFS RDMA resource release",
    )
    require_order(
        release_body,
        [
            "RDMA_TRANSPORT->rdma_unregister_region",
            "delete[] state->rdma_bounce_buf",
            "delete[] state->rdma_bulk_buf",
        ],
        "remote VFS unregisters local RDMA regions before freeing their buffers",
    )

    erase_body = function_body(source, "erase_destroyed_idle_vfs_proxy_locked")
    require_order(
        erase_body,
        [
            "!state->destroy_when_idle",
            "!state->mount_released",
            "state->resources_releasing",
            "!state->resources_released",
            "std::erase_if(g_vfs_proxies",
        ],
        "remote VFS proxy erase gate",
    )

    mark_body = function_body(source, "mark_vfs_proxy_mount_released_and_maybe_destroy")
    require_order(
        mark_body,
        [
            "s_vfs_lock.lock()",
            "state->mount_released = true",
            "s_vfs_lock.unlock()",
            "release_and_maybe_destroy_idle_vfs_proxy(state)",
        ],
        "remote VFS mount release gate",
    )


def test_remote_open_refs_delay_proxy_destroy_until_close() -> None:
    source = REMOTE_VFS_CPP.read_text()

    acquire_body = function_body(source, "acquire_vfs_proxy_open_ref")
    require_order(
        acquire_body,
        [
            "s_vfs_lock.lock()",
            "if (state->active && !state->destroy_when_idle && !state->resources_releasing && !state->resources_released)",
            "uint32_t const REFS = state->open_file_refs.load(std::memory_order_acquire)",
            "state->open_file_refs.store(REFS + 1, std::memory_order_release)",
            "s_vfs_lock.unlock()",
        ],
        "remote VFS proxy open ref acquire",
    )

    release_body = function_body(source, "release_vfs_proxy_open_ref")
    require_order(
        release_body,
        [
            "uint32_t const REFS = state->open_file_refs.load(std::memory_order_acquire)",
            "state->open_file_refs.store(REFS - 1, std::memory_order_release)",
            "release_resources = claim_idle_vfs_proxy_resource_release_locked(state)",
            "erase_destroyed_idle_vfs_proxy_locked(state)",
            "release_vfs_proxy_buffers(state)",
            "finish_idle_vfs_proxy_resource_release(state)",
        ],
        "remote VFS proxy open ref release",
    )

    idle_body = function_body(source, "proxy_is_idle_for_resource_release_locked")
    require_tokens(
        idle_body,
        ["!state->active", "!state->epoch_reset_pending", "state->lifecycle_refs == 0"],
        "epoch marker retains VFS proxy resources and storage",
    )

    open_body = function_body(source, "wki_remote_vfs_open_path")
    require_order(
        open_body,
        [
            "if (!acquire_vfs_proxy_open_ref(state))",
            "ProxyOpenRefGuard open_ref_guard(state)",
            "ctx->proxy = state",
            "open_ref_guard.disarm()",
            "return file",
        ],
        "remote VFS open ref transfer to file context",
    )

    close_body = function_body(source, "remote_vfs_close")
    require_tokens(
        close_body,
        [
            "ProxyVfsState* const PROXY = ctx->proxy;",
            "release_vfs_proxy_open_ref(PROXY);",
            "return flush_status != 0 ? flush_status : close_status;",
        ],
        "remote VFS close releases proxy open ref",
    )


def test_remote_vfs_channel_identity_survives_pool_slot_reuse() -> None:
    header = WKI_HPP.read_text()
    wki_source = WKI_CPP.read_text()
    source = REMOTE_VFS_CPP.read_text()

    require_tokens(
        header,
        [
            "struct WkiChannelIdentity",
            "WkiChannel* channel = nullptr;",
            "uint16_t peer_node_id = WKI_NODE_INVALID;",
            "uint16_t channel_id = 0;",
            "uint32_t generation = 0;",
            "WkiChannelIdentity* identity_out = nullptr",
            "wki_send_on_channel_identity(const WkiChannelIdentity& identity",
        ],
        "immutable WKI channel allocation identity",
    )
    alloc_body = function_body(wki_source, "channel_pool_alloc")
    require_order(
        alloc_body,
        [
            "ch->lock.lock()",
            "channel_init(ch, peer_node, chan_id, prio, credits)",
            "*identity_out = {",
            ".channel = ch",
            ".peer_node_id = ch->peer_node_id",
            ".channel_id = ch->channel_id",
            ".generation = ch->generation",
            "ch->lock.unlock()",
        ],
        "channel allocation token capture under the channel lock",
    )

    mount_body = function_body(source, "mount_vfs_proxy_lane")
    require_order(
        mount_body,
        [
            "WkiChannelIdentity reserved_channel_identity{}",
            "wki_channel_alloc(owner_node, PriorityClass::LATENCY, &reserved_channel_identity)",
            "attach_req.requested_channel = reserved_channel_identity.channel_id",
            "WkiChannelIdentity resource_channel_identity{}",
            "capture_peer_channel_identity(owner_node, WKI_CHAN_RESOURCE, &resource_channel_identity)",
            "wki_send_on_channel_identity(resource_channel_identity, MsgType::DEV_ATTACH_REQ",
            "uint16_t const ATTACH_CHANNEL = state->attach_channel",
            "if (WAIT_RC != 0)",
            "cancel_proxy_attach_wait(state, wait, WAIT_RC)",
                "send_or_defer_vfs_detach(state, owner_node, resource_id, attach_cookie, owner_incarnation)",
            "close_reserved_channel()",
            "wki_channel_reserve(owner_node, ATTACH_CHANNEL, PriorityClass::LATENCY, &reserved_channel_identity)",
            "reserved_channel_identity.channel->lock.lock()",
            "reserved_channel_identity.channel->generation == reserved_channel_identity.generation",
            "s_vfs_lock.lock()",
            "if (!state->epoch_reset_pending)",
            "state->assigned_channel_ref = reserved_channel_identity.channel",
            "state->assigned_channel_generation = reserved_channel_identity.generation",
            "state->active = true",
            "s_vfs_lock.unlock()",
            "reserved_channel_identity.channel->lock.unlock()",
            "wki_peer_lifecycle_acquire(final_peer)",
            "!final_peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)",
            "wki_channel_generation_is_live(CHANNEL_REF, owner_node, CHANNEL_ID, CHANNEL_GENERATION)",
            "wki_peer_lifecycle_release(final_peer)",
        ],
        "remote VFS channel validation and proxy publication",
    )
    if "wki_channel_close(" in mount_body:
        fail("remote VFS attach rollback must never close a reusable channel by raw pointer")

    discard_body = function_body(source, "discard_failed_attached_proxy")
    require_order(
        discard_body,
        [
            "state->active || state->epoch_reset_pending",
            "assigned_channel_ref = state->assigned_channel_ref",
            "assigned_channel_generation = state->assigned_channel_generation",
            "stage_vfs_detach_locked",
            "state->epoch_reset_pending = false",
            "state->active = false",
            "s_vfs_lock.unlock()",
            "wki_channel_close_generation(assigned_channel_ref, owner_node, assigned_channel, assigned_channel_generation)",
        ],
        "failed attached proxy exact-generation rollback",
    )
    if "wki_channel_close(" in discard_body:
        fail("failed attached proxy rollback must not close a reused channel generation")

    marker_body = function_body(source, "wki_remote_vfs_mark_epoch_reset")
    require_tokens(
        marker_body,
        ["state->attach_pending.load(std::memory_order_acquire)", "state->epoch_reset_pending = true"],
        "epoch marker must include in-progress attaches",
    )
    require_order(
        marker_body,
        [
            "s_vfs_lock.lock()",
            "state->lock.lock()",
            "state->active = false",
            "state->epoch_reset_pending = true",
            "state->lock.unlock()",
            "s_vfs_lock.unlock()",
        ],
        "bounded remote VFS epoch marker",
    )
    for forbidden in ["stage_vfs_detach_locked", "wki_deferred_work_notify", "std::make_unique", "new (std::nothrow)", "push_back"]:
        if forbidden in marker_body:
            fail(f"remote VFS RX marker must remain allocation/send-free: found {forbidden}")

    admission_body = function_body(source, "vfs_attach_blocked_by_retiring_binding_locked")
    require_tokens(
        admission_body,
        [
            "vfs_detach_pending_for_resource_locked(owner_node, resource_id)",
            "proxy->owner_node == owner_node",
            "proxy->resource_id == resource_id",
            "proxy->epoch_reset_pending",
        ],
        "remote VFS mount admission includes epoch-marker ownership",
    )
    public_mount_body = function_body(source, "wki_remote_vfs_mount")
    require_order(
        public_mount_body,
        [
            "s_vfs_lock.lock()",
            "vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)",
            "return -EAGAIN",
            "mount_group_id = allocate_vfs_mount_group_id_locked()",
            "return mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION, owner_incarnation",
        ],
        "remote VFS replacement mount cannot cross an epoch marker",
    )
    cleanup_body = function_body(source, "wki_remote_vfs_cleanup_for_peer")
    require_tokens(
        cleanup_body,
        ["(!p->active && !p->epoch_reset_pending)", "deactivate_vfs_proxy_locked(p, cleanup, false)"],
        "task-context epoch cleanup must consume pre-marked proxies",
    )
    require_order(
        cleanup_body,
        [
            "if (!owner_reboot_proven)",
            "stage_vfs_detach_locked",
            "if (owner_reboot_proven || p->detach_pending)",
            "p->epoch_reset_pending = false",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
        ],
        "task-context cleanup releases marker only after staging or reboot proof",
    )
    require_order(
        cleanup_body,
        [
            "for (auto& rfd : g_remote_fds)",
            "if (rfd.consumer_node != node_id)",
            "if (rfd.file != nullptr)",
            "files_to_close.push_back(rfd.file)",
            "rfd.file = nullptr",
            "rfd.retiring = true",
            "rfd.active = false",
            "std::erase_if(g_remote_fds",
            "[node_id]",
            "rfd.consumer_node == node_id && rfd.retiring && rfd.file == nullptr",
            "s_vfs_lock.unlock()",
            "for (auto* file : files_to_close)",
        ],
        "peer cleanup claims every exact-peer FD before erasing only claimed null rows",
    )
    if "return !rfd.active" in cleanup_body or "if (!rfd.active || rfd.consumer_node != node_id)" in cleanup_body:
        fail("peer cleanup must not globally erase inactive FD markers or skip retiring rows with live files")
    require_tokens(
        source,
        [
            "entry.fd_id == fd_id && entry.retiring && entry.file == nullptr",
            "rfd.consumer_node == NODE_ID && rfd.retiring && rfd.file == nullptr",
            "rfd.consumer_node == node_id && rfd.retiring && rfd.file == nullptr",
        ],
        "every remote-FD retirement erases only an exact claimed null row",
    )
    for unsafe in [
        "return !entry.active",
        "return !r.active",
        "return !rfd.active",
    ]:
        if unsafe in source:
            fail(f"remote VFS must not globally erase inactive FD ownership markers: found {unsafe}")

    send_identity_body = function_body(wki_source, "wki_send_on_channel_identity")
    require_tokens(
        send_identity_body,
        [
            "identity.peer_node_id",
            "identity.channel_id",
            "identity.channel",
            "identity.generation",
            "wki_send_impl",
        ],
        "generic exact-generation send",
    )

    for function_name in ["vfs_proxy_send_and_wait", "vfs_proxy_send_untracked", "vfs_proxy_write_rdma_and_wait"]:
        body = function_body(source, function_name)
        require_tokens(
            body,
            [
                "WkiChannelIdentity const CHANNEL_IDENTITY = proxy_channel_identity_locked(state)",
                "peek_channel_tx_seq16(CHANNEL_IDENTITY, &expected_seq)",
                "wki_send_on_channel_identity(CHANNEL_IDENTITY, MsgType::DEV_OP_REQ",
            ],
            f"{function_name} exact channel generation",
        )
        if "wki_send(state->owner_node, state->assigned_channel" in body:
            fail(f"{function_name} must not fall through to an ID-only replacement channel")

    server_body = function_body(source, "handle_vfs_op")
    require_tokens(
        server_body,
        [
            "wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP",
            "wki_dev_server_get_vfs_write_region(channel_identity)",
            "wki_dev_server_complete_vfs_write(channel_identity",
        ],
        "server VFS responses and binding lookups use exact generation",
    )
    if "wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP" in server_body:
        fail("server VFS worker must not reply on an ID-only replacement channel")


def test_server_bounded_metadata_responses_use_stack_storage() -> None:
    handler_body = function_body(REMOTE_VFS_CPP.read_text(), "handle_vfs_op")
    stat_start = handler_body.find("case OP_VFS_STAT:")
    mkdir_start = handler_body.find("case OP_VFS_MKDIR:", stat_start)
    readlink_start = handler_body.find("case OP_VFS_READLINK:", mkdir_start)
    symlink_start = handler_body.find("case OP_VFS_SYMLINK:", readlink_start)
    if min(stat_start, mkdir_start, readlink_start, symlink_start) < 0:
        fail("remote VFS bounded metadata opcode cases must remain present")

    stat_case = handler_body[stat_start:mkdir_start]
    require_order(
        stat_case,
        [
            "ker::vfs::Stat statbuf = {}",
            "int const RET = ker::vfs::vfs_stat_resolved(full_path.data(), &statbuf)",
            "std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::Stat)> resp_buf",
            "reinterpret_cast<DevOpRespPayload*>(resp_buf.data())",
            "memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &statbuf, sizeof(ker::vfs::Stat))",
            "send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(resp_buf.size()))",
        ],
        "fixed-size stat response storage",
    )

    readlink_case = handler_body[readlink_start:symlink_start]
    require_order(
        readlink_case,
        [
            "std::array<char, 512> target_buf{}",
            "vfs_readlink_resolved(full_path.data(), target_buf.data(), target_buf.size() - 1)",
            "std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint16_t) + 512> resp_buf",
            "reinterpret_cast<DevOpRespPayload*>(resp_buf.data())",
            "memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &tlen, sizeof(uint16_t))",
            "memcpy(resp_buf.data() + sizeof(DevOpRespPayload) + 2, target_buf.data(), TARGET_LEN)",
            "wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(), resp_total)",
        ],
        "bounded readlink response storage",
    )

    for name, case in [("stat", stat_case), ("readlink", readlink_case)]:
        if "new (std::nothrow)" in case or "delete[]" in case:
            fail(f"server {name} response retained heap storage")
        if re.search(r"resp_buf\s*(?:\{\}|=\s*\{\})", case):
            fail(f"server {name} response zero-initializes its full stack buffer")


def test_stale_fd_gc_drains_binding_users_before_file_close() -> None:
    body = function_body(REMOTE_VFS_CPP.read_text(), "wki_remote_vfs_gc_stale_fds")
    require_tokens(
        body,
        [
            "wki_peer_lifecycle_acquire(peer)",
            "wki_dev_server_detach_all_for_peer(NODE_ID)",
            "uint64_t const CHECK_NOW = wki_now_us()",
            "CHECK_NOW < rfd.last_activity_us",
            "NOW < rfd.last_activity_us",
            "Peer slots live in g_wki.peers and are never",
            "server FD",
            "files_to_close.push_back(rfd.file)",
            "rfd.file = nullptr",
            "file->fops->vfs_close(file)",
            "delete file",
            "wki_peer_lifecycle_release(peer)",
        ],
        "stale RemoteVfsFd GC ownership transfer",
    )
    require_order(
        body,
        [
            "wki_peer_lifecycle_acquire(peer)",
            "wki_dev_server_detach_all_for_peer(NODE_ID)",
            "uint64_t const CHECK_NOW = wki_now_us()",
            "files_to_close.push_back(rfd.file)",
            "file->fops->vfs_close(file)",
            "delete file",
            "wki_peer_lifecycle_release(peer)",
        ],
        "GC must drain deferred VFS binding refs before detaching and closing File",
    )
    if REMOTE_VFS_CPP.read_text().count("alloc_remote_fd(") != 2:
        fail("RemoteVfsFd creation must remain confined to the reliable server VFS OPEN path")
    require_tokens(
        WKI_HPP.read_text(),
        ["std::array<WkiPeer, WKI_MAX_PEERS> peers"],
        "RemoteVfsFd peer rows use the fixed-lifetime peer table",
    )


def test_server_fd_and_consumer_rx_use_exact_channel_identity() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    dev_server = DEV_SERVER_CPP.read_text()

    require_tokens(
        header,
        [
            "bool retiring = false;",
            "WkiChannelIdentity channel_identity{};",
            "void wki_remote_vfs_cleanup_server_fds_for_channel(const WkiChannelIdentity& channel_identity);",
            "void wki_remote_vfs_mark_server_fds_for_channel(const WkiChannelIdentity& channel_identity);",
            "void wki_remote_vfs_process_pending_server_fd_cleanup();",
            "const WkiChannelIdentity& channel_identity",
        ],
        "remote VFS exact channel lifetime state",
    )

    lookup = function_body(source, "find_remote_fd")
    require_tokens(
        lookup,
        [
            "rfd.active",
            "!rfd.retiring",
            "rfd.fd_id == fd_id",
            "vfs_channel_identity_matches(rfd.channel_identity, channel_identity)",
        ],
        "server FD exact-generation lookup",
    )
    allocate = function_body(source, "alloc_remote_fd")
    require_order(
        allocate,
        [
            "rfd.consumer_node = channel_identity.peer_node_id",
            "rfd.channel_identity = channel_identity",
            "rfd.fd_id = FD_ID",
            "g_remote_fds.push_back(rfd)",
        ],
        "server FD exact-generation publication",
    )

    mark_cleanup = function_body(source, "wki_remote_vfs_mark_server_fds_for_channel")
    require_order(
        mark_cleanup,
        [
            "s_vfs_lock.lock()",
            "vfs_channel_identity_matches(rfd.channel_identity, channel_identity)",
            "rfd.retiring = true",
            "rfd.active = false",
            "s_vfs_lock.unlock()",
            "wki_deferred_work_notify()",
        ],
        "exact binding FD retirement is allocation-free in reliable RX",
    )
    if "vfs_close_file" in mark_cleanup or "std::deque" in mark_cleanup:
        fail("reliable RX server-FD retirement must not allocate or close files")
    if "consumer_node == channel_identity.peer_node_id" in mark_cleanup:
        fail("ordinary binding detach must not close sibling channel generations for the same peer")

    drain_cleanup = function_body(source, "wki_remote_vfs_process_pending_server_fd_cleanup")
    require_order(
        drain_cleanup,
        [
            "std::array<ker::vfs::File*, CLOSE_BATCH> files_to_close{}",
            "s_vfs_lock.lock()",
            "rfd.retiring",
            "rfd.file = nullptr",
            "std::erase_if(g_remote_fds",
            "s_vfs_lock.unlock()",
            "ker::vfs::vfs_close_file(files_to_close.at(i))",
        ],
        "deferred exact binding FD close",
    )
    require_tokens(
        function_body(WKI_CPP.read_text(), "process_deferred_blocking_work"),
        ["wki_remote_vfs_process_pending_server_fd_cleanup()"],
        "WKI task-context server-FD cleanup drain",
    )

    server = function_body(source, "handle_vfs_op")
    require_tokens(
        server,
        [
            "vfs_channel_identity_matches_header(hdr, channel_identity)",
            "find_remote_fd(channel_identity, fd_id)",
            "wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP",
            "Legacy DEV_OP_REQ has no binding nonce",
        ],
        "server VFS exact-generation dispatch",
    )
    for handler in ["handle_vfs_op_resp", "handle_vfs_invalidate_notify"]:
        body = function_body(source, handler)
        require_tokens(
            body,
            [
                "vfs_channel_identity_matches_header(hdr, channel_identity)",
                "channel_identity",
            ],
            f"{handler} exact-generation RX dispatch",
        )
    invalidate = function_body(source, "handle_vfs_invalidate_notify")
    require_order(
        invalidate,
        [
            "find_vfs_proxy_by_channel(channel_identity)",
            "anchor->lanes",
            "member->lifecycle_refs++",
            "s_vfs_lock.unlock()",
            "release_vfs_proxy_lifecycle_ref(group.members",
        ],
        "invalidate notification pins the mount group across unlocked VFS work",
    )

    require_tokens(
        dev_server,
        [
            "wki_remote_vfs_cleanup_server_fds_for_channel(channel_identity)",
            "wki_remote_vfs_mark_server_fds_for_channel(item.channel_identity)",
        ],
        "ordinary and reconciliation binding teardown close exact server FDs",
    )


def test_invalidate_notify_retains_and_invalidates_complete_mount_group() -> None:
    body = function_body(REMOTE_VFS_CPP.read_text(), "handle_vfs_invalidate_notify")

    require_tokens(
        body,
        [
            "std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> members",
            "find_vfs_proxy_by_channel(channel_identity)",
            "anchor->lanes",
            "member->lifecycle_refs++",
            "group.members",
            "group.count++",
            "invalidate_all_dir_caches(member)",
            "state->lifecycle_refs++",
            "invalidate_all_dir_caches(state)",
        ],
        "whole-group invalidate retention",
    )

    unlock_pos = body.find("s_vfs_lock.unlock()")
    if unlock_pos < 0 or body.find("s_vfs_lock.unlock()", unlock_pos + 1) >= 0:
        fail("invalidate notify must have one explicit transition from locked retention to unlocked invalidation")
    locked = body[:unlock_pos]
    unlocked = body[unlock_pos + len("s_vfs_lock.unlock()") :]

    require_order(
        locked,
        [
            "s_vfs_lock.lock()",
            "find_vfs_proxy_by_channel(channel_identity)",
            "anchor->lanes",
            "member->lifecycle_refs++",
            "group.count++",
            "invalidate_all_dir_caches(member)",
        ],
        "mount-group retention under the VFS lock",
    )
    if "release_vfs_proxy_lifecycle_ref" in locked:
        fail("retained invalidate group must not be released while s_vfs_lock is held")
    for token in [
        "vfs_cache_notify_invalidate_path",
        "invalidate_readlink_cache_group",
        "vfs_stream_cache_invalidate_remote_scope",
    ]:
        if token in locked:
            fail(f"invalidate notify must perform {token} after dropping s_vfs_lock")
    if "s_vfs_lock.lock()" in unlocked:
        fail("invalidate notify must not reacquire s_vfs_lock around retained-group cache invalidation")

    require_order(
        unlocked,
        [
            "if (group.count == 0)",
            "vfs_cache_notify_invalidate_path",
            "invalidate_readlink_cache_group(group.members",
            "vfs_stream_cache_invalidate_remote_scope(group.members",
            "release_vfs_proxy_lifecycle_ref(group.members",
        ],
        "unlocked whole-group invalidation and release",
    )

    group_loops = []
    loop_pattern = re.compile(
        r"for\s*\(\s*size_t\s+(?P<index>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*0\s*;\s*"
        r"(?P=index)\s*<\s*group\.count\s*;\s*\+\+(?P=index)\s*\)\s*\{(?P<body>.*?)\}",
        re.DOTALL,
    )
    for match in loop_pattern.finditer(unlocked):
        group_loops.append((match.group("index"), match.group("body")))

    def loop_applies(call: str) -> bool:
        for index, loop_body in group_loops:
            member_access = rf"group\.members(?:\.at\(\s*{re.escape(index)}\s*\)|\[\s*{re.escape(index)}\s*\])"
            if call in loop_body and re.search(member_access, loop_body):
                return True
        return False

    if not loop_applies("vfs_stream_cache_invalidate_remote_scope"):
        fail("invalidate notify must clear the stream-cache scope of every retained group member")
    if not loop_applies("release_vfs_proxy_lifecycle_ref"):
        fail("invalidate notify must release exactly the retained group span after unlocked work")


def test_path_change_notifies_each_matching_export_once() -> None:
    body = function_body(REMOTE_VFS_CPP.read_text(), "wki_remote_vfs_notify_path_changed")
    require_order(
        body,
        [
            "for (const auto& exp : g_vfs_exports)",
            "trim_export_prefix(EXPORT_PATH, old_local_vfs_path)",
            "trim_export_prefix(EXPORT_PATH, new_local_vfs_path)",
            "if (OLD_REL_SRC == nullptr && NEW_REL_SRC == nullptr)",
            "send_notify(exp, old_rel.data(), new_rel.data())",
        ],
        "single-pass old/new export invalidation",
    )
    if body.count("for (const auto& exp : g_vfs_exports)") != 1:
        fail("path change must visit each export exactly once")
    if body.count("send_notify(exp, old_rel.data(), new_rel.data())") != 1:
        fail("path change must have one send site per matching export")


def test_export_rebuild_is_revisioned_and_backing_mount_exact() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    dev_server = DEV_SERVER_CPP.read_text()
    core = VFS_CORE_CPP.read_text()

    require_tokens(
        header,
        [
            "uint64_t publication_revision = 0;",
            "uint32_t backing_dev_id = 0;",
            "ker::vfs::FSType backing_fs_type",
            "auto wki_remote_vfs_export_snapshot_is_current(const VfsExport& expected) -> bool;",
            "auto wki_remote_vfs_prepare_export_rebuild() -> bool;",
            "void wki_remote_vfs_cancel_export_rebuild();",
        ],
        "revisioned VFS export identity",
    )

    preserve = function_body(source, "take_preserved_export_identity")
    require_tokens(
        preserve,
        [
            "it->name",
            "it->export_path",
            "export_backing_identity_matches(*it, backing)",
            ".resource_incarnation = it->resource_incarnation",
        ],
        "export token preservation requires the same backing mount",
    )
    backing_match = function_body(source, "export_backing_identity_matches")
    require_tokens(
        backing_match,
        [
            "backing.dev_id != 0",
            "export_entry.backing_dev_id == backing.dev_id",
            "export_entry.backing_fs_type == backing.fs_type",
        ],
        "same visible path on a replacement mount receives a new export token",
    )
    backing_snapshot = function_body(source, "snapshot_export_backing_identity")
    require_tokens(
        backing_snapshot,
        [
            "snapshot.dev_id == 0",
            "const char* const MOUNT_PATH = static_cast<const char*>(snapshot.path)",
            "export_path_belongs_to_mount(export_path, MOUNT_PATH)",
            "PATH_LEN > best_path_len",
            ".dev_id = snapshot.dev_id",
            ".fs_type = snapshot.fs_type",
        ],
        "explicit export captures the longest owning mount identity",
    )
    export_add = function_body(source, "wki_remote_vfs_export_add_internal")
    require_tokens(
        export_add,
        [
            "backing.dev_id == 0",
            "g_vfs_export_revision > UINT64_MAX - 2",
            "g_vfs_export_target_revision == 0",
        ],
        "export insertion rejects invalid backing and revision identities",
    )

    lookup = function_body(source, "wki_remote_vfs_find_export_snapshot")
    require_order(
        lookup,
        [
            "uint64_t const REVISION = g_vfs_export_revision",
            "if ((REVISION & 1U) != 0)",
            "return false",
            "exp.publication_revision == REVISION",
            "*out = exp",
        ],
        "attach snapshots reject an in-progress odd export table",
    )
    current = function_body(source, "wki_remote_vfs_export_snapshot_is_current")
    require_tokens(
        current,
        [
            "REVISION == expected.publication_revision",
            "exp.resource_incarnation == expected.resource_incarnation",
            "exp.backing_dev_id == expected.backing_dev_id",
            "exp.backing_fs_type == expected.backing_fs_type",
            "exp.export_path",
            "exp.name",
        ],
        "final attach publication validates the full export identity",
    )
    advertise = function_body(source, "advertise_exports_to_peer")
    require_tokens(
        advertise,
        [
            "if ((PUBLICATION_REVISION & 1U) != 0)",
            "g_vfs_export_revision != PUBLICATION_REVISION",
            "EXP.publication_revision != PUBLICATION_REVISION",
        ],
        "partial odd export tables are not advertised",
    )

    prepare = function_body(source, "wki_remote_vfs_prepare_export_rebuild")
    require_order(
        prepare,
        [
            "g_vfs_export_target_revision = TARGET_REVISION",
            "g_vfs_export_rebuild_prepared = true",
            "s_vfs_lock.unlock()",
            "wki_dev_server_begin_vfs_export_reconciliation(TARGET_REVISION)",
            "g_vfs_export_revision++",
        ],
        "pre-gate attaches drain against the stable table before the export revision becomes odd",
    )
    if "exp.publication_revision = TARGET_REVISION" in prepare:
        fail("failed VFS admission close must leave the old stable revision unchanged")
    reconcile = function_body(source, "reconcile_and_publish_vfs_exports")
    require_order(
        reconcile,
        [
            "g_vfs_export_rebuild_accepting_entries = false",
            "VfsExport const EXP =",
            "s_vfs_lock.unlock()",
            "wki_dev_server_reconcile_vfs_export(EXP.resource_id",
            "wki_dev_server_finish_vfs_export_reconciliation(TARGET_REVISION)",
            "g_vfs_export_revision = TARGET_REVISION",
            "s_vfs_lock.unlock()",
            "wki_dev_server_end_vfs_export_reconciliation(TARGET_REVISION)",
            "g_vfs_export_target_revision = 0",
            "g_vfs_export_rebuild_prepared = false",
        ],
        "binding reconciliation runs unlocked while the table stays odd until exact retirement finishes",
    )
    cancel = function_body(source, "wki_remote_vfs_cancel_export_rebuild")
    require_tokens(cancel, ["reconcile_and_publish_vfs_exports()"], "failed pivot reopens the unchanged exact table")

    rebuild = function_body(source, "wki_remote_vfs_rebuild_exports")
    require_order(
        rebuild,
        [
            "g_vfs_exports.clear()",
            "g_vfs_export_rebuild_accepting_entries = true",
            "wki_remote_vfs_auto_discover_internal(&stale_exports)",
            "reconcile_and_publish_vfs_exports()",
            "MsgType::RESOURCE_WITHDRAW",
            "wki_remote_vfs_advertise_exports()",
        ],
        "new export table and bindings publish before stale-token withdrawal/advertisement",
    )
    if "wki_dev_server_refresh_vfs_binding" in source:
        fail("export insertion must not mutate one binding before full-table reconciliation")

    pivot = function_body(core, "vfs_pivot_root")
    require_order(
        pivot,
        [
            "snapshot_bounded_path_string(new_root",
            "snapshot_bounded_path_string(put_old",
            "wki_remote_vfs_prepare_export_rebuild()",
            "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data())",
            "if (REMAP_RET != 0)",
            "wki_remote_vfs_cancel_export_rebuild()",
            "rebase_wki_mounts_for_new_root(stable_new_root.data())",
            "wki_remote_vfs_rebuild_exports()",
        ],
        "pivot gates/drains VFS before remap and cancels or publishes on every outcome",
    )

    require_tokens(
        dev_server,
        [
            "binding.vfs_export_dev_id = exp.backing_dev_id",
            "binding.vfs_export_publication_revision = exp.publication_revision",
            "if (wki_remote_vfs_export_snapshot_is_current(exp))",
            "provisional_binding->active = true",
        ],
        "provisional VFS binding activates only after final exact snapshot validation",
    )


def test_remote_vfs_mount_lanes_preserve_channel_and_lifetime_affinity() -> None:
    header = REMOTE_VFS_HPP.read_text()
    source = REMOTE_VFS_CPP.read_text()

    require_tokens(
        header,
        [
            "constexpr size_t VFS_PROXY_LANE_COUNT = 8;",
            "constexpr size_t VFS_PROXY_DEFAULT_LANE_COUNT = VFS_PROXY_LANE_COUNT;",
            "VFS_PROXY_DEFAULT_LANE_COUNT <= VFS_PROXY_LANE_COUNT",
            "constexpr size_t VFS_PROXY_RDMA_LANE_COUNT = VFS_PROXY_LANE_COUNT;",
            "static_assert(VFS_PROXY_RDMA_LANE_COUNT > 1 && VFS_PROXY_RDMA_LANE_COUNT <= VFS_PROXY_LANE_COUNT);",
            "uint64_t mount_group_id = 0;",
            "uint8_t lane_index = 0;",
            "bool lane_anchor = false;",
            "std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> lanes = {};",
            "uint8_t lane_count = 0;",
            "bool lanes_ready = false;",
            "uint64_t lane_selection_cursor = 0;",
        ],
        "bounded remote VFS lane metadata",
    )

    selector = function_body(source, "acquire_vfs_proxy_lane")
    rdma_matcher = function_body(source, "vfs_proxy_lane_has_requested_rdma")
    require_order(
        rdma_matcher,
        [
            "bool const HAS_READ_RDMA",
            "candidate->rdma_capable && !candidate->rdma_read_disabled.load(std::memory_order_acquire)",
            "remote_vfs_rdma_retry_ready(candidate->rdma_read_retry_after_us, now_us)",
            "candidate->bulk_rdma_capable && !candidate->bulk_rdma_disabled.load(std::memory_order_acquire)",
            "remote_vfs_rdma_retry_ready(candidate->bulk_rdma_retry_after_us, now_us)",
            "bool const HAS_WRITE_RDMA",
            "transport_supports_vfs_write_push_rdma(candidate->rdma_transport)",
            "candidate->rdma_server_write_rkey != 0",
            "(!require_read || HAS_READ_RDMA) && (!require_write || HAS_WRITE_RDMA)",
        ],
        "per-lane RDMA matching requires every requested data direction",
    )
    require_tokens(
        source,
        [
            "auto acquire_vfs_proxy_lane(ProxyVfsState* anchor, bool prefer_rdma_read_anchor = false,",
            "bool prefer_rdma_write_anchor = false)",
        ],
        "lane selector exposes optional data-RDMA preferences",
    )
    require_order(
        selector,
        [
            "s_vfs_lock.lock()",
            "anchor->lanes_ready",
            "anchor->lane_count > 0",
            "anchor->lane_count <= anchor->lanes.size()",
            "std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> live_lanes{}",
            "std::array<bool, VFS_PROXY_LANE_COUNT> live_lane_has_requested_rdma{}",
            "bool const PREFER_RDMA = prefer_rdma_read_anchor || prefer_rdma_write_anchor",
            "uint64_t const NOW_US = PREFER_RDMA ? wki_now_us() : 0",
            "for (size_t lane_index = 0; lane_index < anchor->lane_count; ++lane_index)",
            "auto* candidate = anchor->lanes.at(lane_index)",
            "bool const HAS_REQUESTED_RDMA",
            "vfs_proxy_lane_has_requested_rdma(candidate, prefer_rdma_read_anchor, prefer_rdma_write_anchor, NOW_US)",
            "live_lanes.at(live_lane_count) = candidate",
            "live_lane_has_requested_rdma.at(live_lane_count) = HAS_REQUESTED_RDMA",
            "auto const START = static_cast<size_t>(anchor->lane_selection_cursor++ % live_lane_count)",
            "candidate->op_pending.load(std::memory_order_acquire)",
            "candidate->op_untracked_send_pending.load(std::memory_order_acquire)",
            "uint32_t const LIFECYCLE_REFS = candidate->lifecycle_refs",
            "candidate->open_file_refs.load(std::memory_order_acquire)",
            "bool const LOWER_PRESSURE",
            "(selected_busy && !BUSY)",
            "LIFECYCLE_REFS < selected_lifecycle_refs",
            "OPEN_REFS < selected_open_refs",
            "HAS_REQUESTED_RDMA &&",
            "selected->lifecycle_refs++",
            "s_vfs_lock.unlock()",
        ],
        "lane selection prefers low-pressure bindings before RDMA capability",
    )
    if "(require_read && HAS_READ_RDMA) || (require_write && HAS_WRITE_RDMA)" in rdma_matcher:
        fail("O_RDWR lane selection must require both requested RDMA directions, not either direction")
    if "anchor->mount_configured" in selector:
        fail("lane selection must remain available while mount_filesystem is publishing the ready anchor")

    lane_mount = function_body(source, "mount_vfs_proxy_lane")
    lane_policy = function_body(source, "vfs_proxy_lane_count_for_mount")
    require_order(
        lane_policy,
        [
            'constexpr std::string_view AUTO_MOUNT_PREFIX = "/wki/"',
            'constexpr std::string_view TMP_EXPORT_SUFFIX = "/tmp"',
            "local_mount_path.starts_with(AUTO_MOUNT_PREFIX)",
            "local_mount_path.remove_prefix(AUTO_MOUNT_PREFIX.size())",
            "size_t const HOST_END = local_mount_path.find('/')",
            "HOST_END != std::string_view::npos && HOST_END > 0",
            "local_mount_path.substr(HOST_END) == TMP_EXPORT_SUFFIX",
            "return static_cast<uint8_t>(VFS_PROXY_LANE_COUNT)",
            "return static_cast<uint8_t>(VFS_PROXY_DEFAULT_LANE_COUNT)",
        ],
        "only temporary-data mounts opt into the bounded eight-lane capacity",
    )
    require_tokens(
        source,
        [
            'static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp") == VFS_PROXY_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp/work") == VFS_PROXY_DEFAULT_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/cache/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/mnt/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/wki/tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/wki//tmp") == VFS_PROXY_DEFAULT_LANE_COUNT);',
            'static_assert(vfs_proxy_lane_count_for_mount("/wki/wos-1/tmp/") == VFS_PROXY_DEFAULT_LANE_COUNT);',
        ],
        "lane policy admits only the dedicated automatic temporary-data mount shape",
    )
    require_order(
        lane_mount,
        [
            "VFS_PROXY_AUX_ATTACH_TIMEOUT_US",
            "if (!lane_anchor)",
            "anchor->lanes.at(lane_index) = state",
            "anchor->lane_count = static_cast<uint8_t>(lane_index + 1U)",
            "release_vfs_proxy_lifecycle_ref(state)",
            "state->lanes.fill(nullptr)",
            "state->lanes.at(0) = state",
            "state->lane_count = 1",
            "state->lanes_ready = ANCHOR_READY",
            'ker::vfs::mount_filesystem(local_mount_path, "remote"',
            "if (MOUNT_RET != 0)",
            "state->mount_configured = true",
            "uint8_t const TARGET_LANE_COUNT = vfs_proxy_lane_count_for_mount(local_mount_path)",
            "for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT; ++lane_index)",
            "mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION",
        ],
        "mount publishes a ready lane zero before auxiliary lanes are attached",
    )
    MOUNT_CALL = 'ker::vfs::mount_filesystem(local_mount_path, "remote"'
    ready_at = lane_mount.find("state->lanes_ready = ANCHOR_READY")
    mount_at = lane_mount.find(MOUNT_CALL)
    configured_at = lane_mount.find("state->mount_configured = true")
    if ready_at < 0 or mount_at < 0 or configured_at < 0:
        fail("remote VFS mount publication markers are missing")
    if mount_at < ready_at:
        fail("remote VFS mount row must not become visible before lane zero is selectable")
    if configured_at < mount_at:
        fail("mount_configured must commit only after mount_filesystem returns")
    if lane_mount.count(MOUNT_CALL) != 1:
        fail("remote VFS lane mount must have one physical VFS mount publication")

    aux_publish_marker = lane_mount.rfind("if (!lane_anchor)")
    if aux_publish_marker < 0:
        fail("auxiliary VFS lane publication branch is missing")
    aux_publish = block_body_after(lane_mount[aux_publish_marker:], "if (!lane_anchor)")
    require_tokens(
        aux_publish,
        [
            "s_vfs_lock.lock()",
            "anchor->mount_configured",
            "anchor->lane_count == lane_index",
            "anchor->lanes.at(lane_index) = state",
        ],
        "auxiliary lane publication remains serialized and contiguous after anchor publication",
    )
    require_order(
        lane_mount,
        [
            "int const PRE_AUX_VALIDATION = validate_mount_binding()",
            "uint8_t const TARGET_LANE_COUNT = vfs_proxy_lane_count_for_mount(local_mount_path)",
            "for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT; ++lane_index)",
            "if (LANE_RET == -ENOENT || LANE_RET == -ESTALE)",
            "wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION)",
            "release_vfs_proxy_lifecycle_ref(state)",
            "return LANE_RET",
            "int const POST_AUX_VALIDATION = validate_mount_binding()",
            "wki_remote_vfs_unmount_resource_generation(owner_node, resource_id, RESOURCE_GENERATION)",
        ],
        "definitive auxiliary rejection rolls back before final binding/resource revalidation",
    )
    post_aux = lane_mount[lane_mount.find("for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT;") :]
    require_order(
        post_aux,
        [
            "state->lanes_ready",
            "wki_remote_vfs_unmount_resource_generation",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "post-aux peer cleanup unmounts the exact configured group before dropping its construction pin",
    )

    for function_name in [
        "wki_remote_vfs_stat",
        "wki_remote_vfs_mkdir",
        "wki_remote_vfs_chmod",
        "wki_remote_vfs_utimens",
        "wki_remote_vfs_symlink",
        "wki_remote_vfs_unlink",
        "wki_remote_vfs_rmdir",
        "wki_remote_vfs_rename",
        "wki_remote_vfs_readlink_path",
    ]:
        body = function_body(source, function_name)
        require_order(
            body,
            ["auto* state = acquire_vfs_proxy_lane(anchor)", "ProxyLifecycleRefGuard lane_ref_guard(state)"],
            f"{function_name} selects and pins a mount lane",
        )

    open_body = function_body(source, "wki_remote_vfs_open_path")
    require_order(
        open_body,
        [
            "int const ACCESS_MODE = flags & OPEN_ACCMODE",
            "bool const NON_DIRECTORY_OPEN = (flags & ker::vfs::O_DIRECTORY) == 0",
            "bool const PREFER_RDMA_READ_ANCHOR =",
            "NON_DIRECTORY_OPEN && (ACCESS_MODE == OPEN_RDONLY || ACCESS_MODE == OPEN_RDWR)",
            "bool const PREFER_RDMA_WRITE_ANCHOR =",
            "NON_DIRECTORY_OPEN && (ACCESS_MODE == OPEN_WRONLY || ACCESS_MODE == OPEN_RDWR)",
            "auto* state = acquire_vfs_proxy_lane(anchor, PREFER_RDMA_READ_ANCHOR, PREFER_RDMA_WRITE_ANCHOR)",
            "ProxyLifecycleRefGuard lane_ref_guard(state)",
            "if (!acquire_vfs_proxy_open_ref(state))",
            "ctx->proxy = state",
        ],
        "data-oriented open selects a direction-capable RDMA lane and transfers its ownership to the file context",
    )
    if source.count("acquire_vfs_proxy_lane(anchor,") != 1:
        fail("only data-oriented open may request RDMA-anchor lane selection")
    fstat_body = function_body(source, "wki_remote_vfs_fstat")
    require_tokens(fstat_body, ["remote_vfs_stat_on_proxy(ctx->proxy"], "fstat keeps its FD lane")
    if "wki_remote_vfs_stat(mount->private_data" in fstat_body:
        fail("fstat must not reselect a lane for an existing remote FD")

    for function_name in [
        "remote_vfs_close",
        "remote_vfs_read",
        "remote_vfs_write",
        "remote_vfs_lseek",
        "remote_vfs_readdir",
        "remote_vfs_truncate",
        "remote_vfs_fsync_file",
    ]:
        body = function_body(source, function_name)
        if "acquire_vfs_proxy_lane(" in body:
            fail(f"{function_name} must use its RemoteFileContext lane directly")

    for function_name in [
        "wki_remote_vfs_mkdir",
        "wki_remote_vfs_symlink",
        "wki_remote_vfs_unlink",
        "wki_remote_vfs_rmdir",
        "wki_remote_vfs_rename",
    ]:
        require_tokens(
            function_body(source, function_name),
            ["invalidate_readlink_cache_group(state)"],
            f"{function_name} invalidates every lane's readlink cache",
        )
    require_tokens(
        function_body(source, "handle_vfs_invalidate_notify"),
        ["invalidate_readlink_cache_group(group.members"],
        "server invalidation clears sibling-lane readlink caches",
    )

    group_claim = function_body(source, "claim_vfs_proxy_group_unmount_locked")
    require_order(
        group_claim,
        [
            "mark_vfs_proxy_group_unavailable_locked(anchor)",
            "for (auto& proxy : g_vfs_proxies)",
            "deactivate_vfs_proxy_locked(state, teardown, true)",
        ],
        "group teardown unpublishes lanes before retiring them",
    )
    require_tokens(
        function_body(source, "mark_vfs_proxy_group_unavailable_locked"),
        ["proxy->lane_count = 0", "proxy->lanes_ready = false"],
        "unavailable lane group cannot be selected",
    )
    for function_name, marker in [
        ("wki_remote_vfs_mark_epoch_reset", "mark_vfs_proxy_group_unavailable_locked(state)"),
        ("wki_remote_vfs_cleanup_for_peer", "mark_vfs_proxy_group_unavailable_locked(p)"),
    ]:
        require_tokens(function_body(source, function_name), [marker], f"{function_name} unpublishes lane groups")


def test_capability_gated_vfs_data_lanes_bound_rdma_buffers() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = REMOTE_VFS_HPP.read_text()
    wire = WIRE_HPP.read_text()
    wki = WKI_CPP.read_text()
    wki_header = WKI_HPP.read_text()
    lane_mount = function_body(source, "mount_vfs_proxy_lane")
    capability = function_body(wki, "wki_peer_capability_negotiated")
    lane_selftest = function_body(source, "wki_remote_vfs_selftest_multi_rdma_lane_selection")

    require_tokens(
        wire,
        [
            "constexpr uint16_t WKI_CAP_VFS_MULTI_RDMA_LANES = 0x0008;",
            "constexpr uint8_t DEV_ATTACH_DISABLE_RDMA = 0x40;",
            "constexpr uint8_t DEV_ATTACH_VFS_AUX_LANE = 0x80;",
            "auto wki_vfs_proxy_attach_mode(",
            "auto wki_vfs_attach_lane_is_anchor(",
            "static_assert(sizeof(DevAttachReqPayload) == 12",
            "static_assert(sizeof(HelloPayload) == 96",
        ],
        "multi-lane VFS RDMA preserves the existing wire layouts",
    )
    require_order(
        function_body(wire, "wki_vfs_proxy_attach_mode"),
        [
            "static_cast<uint8_t>(AttachMode::PROXY)",
            "if (!lane_anchor)",
            "mode |= DEV_ATTACH_VFS_AUX_LANE",
            "if (!request_rdma)",
            "mode |= DEV_ATTACH_DISABLE_RDMA",
            "return mode",
        ],
        "consumer attach helper keeps auxiliary identity independent from RDMA request state",
    )
    require_order(
        function_body(wire, "wki_vfs_attach_lane_is_anchor"),
        [
            "if (explicit_aux_lane_negotiated)",
            "(mode & DEV_ATTACH_VFS_AUX_LANE) == 0",
            "(mode & DEV_ATTACH_DISABLE_RDMA) == 0",
        ],
        "server attach helper uses explicit identity only when negotiated and preserves legacy fallback",
    )
    require_tokens(
        header,
        [
            "constexpr size_t VFS_PROXY_LANE_COUNT = 8;",
            "constexpr size_t VFS_PROXY_DEFAULT_LANE_COUNT = VFS_PROXY_LANE_COUNT;",
            "constexpr size_t VFS_PROXY_RDMA_LANE_COUNT = VFS_PROXY_LANE_COUNT;",
            "VFS_PROXY_RDMA_LANE_COUNT <= VFS_PROXY_LANE_COUNT",
        ],
        "all bounded VFS lanes retain data-path capability",
    )
    require_tokens(
        wki_header,
        ["auto wki_peer_capability_negotiated(uint16_t peer_node, uint16_t capabilities) -> bool;"],
        "generic peer capability negotiation API",
    )
    require_tokens(
        capability,
        [
            "capabilities == 0",
            "(g_wki.capabilities & capabilities) != capabilities",
            "WkiPeer const* peer = wki_peer_find(peer_node)",
            "(peer->capabilities & capabilities) == capabilities",
        ],
        "capability negotiation requires every requested bit from both peers",
    )
    require_tokens(
        function_body(wki, "wki_init"),
        ["WKI_CAP_RESOURCE_INCARNATION | WKI_CAP_VFS_MULTI_RDMA_LANES | WKI_CAP_VFS_METADATA_BATCH"],
        "local HELLO capability advertisement",
    )
    require_order(
        lane_mount,
        [
            "wki_peer_capability_negotiated(owner_node, WKI_CAP_VFS_MULTI_RDMA_LANES)",
            "bool const RDMA_LANE = lane_anchor || (MULTI_RDMA_LANES && lane_index < VFS_PROXY_RDMA_LANE_COUNT)",
            "wki_vfs_proxy_attach_mode(lane_anchor, RDMA_LANE)",
            "WkiPeer const* peer = wki_peer_find(owner_node)",
            "if (RDMA_LANE && peer != nullptr",
        ],
        "only the negotiated bounded data-lane set requests and allocates VFS RDMA state",
    )
    if "if (lane_anchor && peer != nullptr" in lane_mount:
        fail("VFS RDMA allocation must not remain restricted to the notification anchor")

    require_tokens(
        header,
        ["auto wki_remote_vfs_selftest_multi_rdma_lane_selection() -> bool;"],
        "multi-lane selector selftest declaration",
    )
    require_order(
        lane_selftest,
        [
            "anchor.rdma_server_write_rkey = 1",
            "auxiliary.rdma_server_write_rkey = 0",
            "acquire_vfs_proxy_lane(&anchor, true, true)",
            "selected != &anchor",
            "anchor.rdma_server_write_rkey = 0",
            "auxiliary.rdma_server_write_rkey = 2",
            "selected != &auxiliary",
            "auxiliary.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release)",
            "selected != &anchor",
            "auxiliary.rdma_read_retry_after_us.store(0, std::memory_order_release)",
            "anchor.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release)",
            "selected != &auxiliary",
            "auxiliary.rdma_read_retry_after_us.store(UINT64_MAX, std::memory_order_release)",
            "selected == nullptr",
            "acquire_vfs_proxy_lane(&anchor)",
        ],
        "selector selftest covers O_RDWR direction matching, auxiliary selection, cooldown, and message fallback",
    )
    require_tokens(
        WKI_DEV_PROXY_KTEST.read_text(),
        [
            "KTEST(WkiRemoteVfsLanes, MultiRdmaSelectionRequiresRequestedDirections)",
            "wki_remote_vfs_selftest_multi_rdma_lane_selection()",
            "KTEST(WkiRemoteVfsLanes, RoundRobinUsesFullCapacity)",
            "wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity()",
            "KTEST(WkiRemoteVfsLanes, PressurePrecedesRdmaPreference)",
            "wki_remote_vfs_selftest_lane_pressure_precedes_rdma()",
        ],
        "multi-lane selector KTEST hook",
    )
    require_tokens(
        WKI_WIRE_KTEST.read_text(),
        [
            "KTEST(WkiWire, VfsMultiRdmaCapabilityAndAuxFlagPreserveLayouts)",
            "WKI_CAP_VFS_MULTI_RDMA_LANES",
            "DEV_ATTACH_VFS_AUX_LANE",
            "sizeof(HelloPayload)",
            "sizeof(DevAttachReqPayload)",
            "wki_vfs_proxy_attach_mode(true, true)",
            "wki_vfs_proxy_attach_mode(false, true)",
            "wki_vfs_proxy_attach_mode(false, false)",
            "wki_vfs_attach_lane_is_anchor(ANCHOR_RDMA, true)",
            "wki_vfs_attach_lane_is_anchor(AUX_RDMA, true)",
            "wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, false)",
        ],
        "wire helper compatibility matrix KTEST hook",
    )
    require_tokens(
        WKI_WIRE_HOST_TEST.read_text(),
        [
            "TEST(WkiWire, VfsMultiRdmaCapabilityAndAuxFlagPreserveLayouts)",
            "wki_vfs_proxy_attach_mode(true, true)",
            "wki_vfs_proxy_attach_mode(false, true)",
            "wki_vfs_proxy_attach_mode(false, false)",
            "wki_vfs_attach_lane_is_anchor(AUX_RDMA, true)",
            "wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, false)",
        ],
        "wire helper compatibility matrix host-unit hook",
    )


def test_metadata_batch_never_replays_after_a_send_attempt() -> None:
    source = REMOTE_VFS_CPP.read_text()
    header = REMOTE_VFS_HPP.read_text()
    batch = function_body(source, "wki_remote_vfs_metadata_batch")

    require_tokens(
        header,
        [
            "auto wki_remote_vfs_metadata_batch(",
            "EOPNOTSUPP is returned only before any request send",
        ],
        "metadata batch public no-replay contract",
    )
    require_order(
        batch,
        [
            "results[index].status = -EINPROGRESS",
            "relative_wire_path_has_safe_components",
            "item_size > MAX_REQUEST_DATA - mutation_request_len",
            "return -EOPNOTSUPP",
            "wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_METADATA_BATCH)",
            "return -EOPNOTSUPP",
            "acquire_vfs_proxy_lane(anchor)",
            "mutation_attempted = true",
            "*mutation_request_attempted = true",
            "vfs_proxy_send_and_wait(state, OP_VFS_METADATA_BATCH",
            "STATUS == -EOPNOTSUPP ? -EPROTO : STATUS",
        ],
        "metadata batch validates and negotiates before its first send and never exposes replayable EOPNOTSUPP afterwards",
    )
    acquire_pos = batch.find("acquire_vfs_proxy_lane(anchor)")
    if acquire_pos < 0 or batch.rfind("return -EOPNOTSUPP") >= acquire_pos:
        fail("metadata batch may expose EOPNOTSUPP only before lane acquisition and a possible send")
    require_tokens(
        batch,
        [
            "VFS_METADATA_BATCH_MAX_STAT_ITEMS",
            "response_len != EXPECTED_RESPONSE_LEN",
            "response_header.version != REQUEST_HEADER.version",
            "response_header.operation != REQUEST_HEADER.operation",
            "response_header.count != REQUEST_HEADER.count",
            "response_header.mode != REQUEST_HEADER.mode",
            "invalidate_readlink_cache_group(state)",
        ],
        "metadata batch bounded chunking, exact response framing, and ambiguous completion invalidation",
    )

    core_batch = function_body(VFS_CORE_CPP.read_text(), "vfs_metadata_batch")
    require_tokens(
        core_batch,
        [
            "mutation_request_attempted",
            "COMPLETION_AMBIGUOUS",
            "CREATE_MAY_HAVE_TAKEN_EFFECT",
            "vfs_cache_notify_path_changed",
        ],
        "metadata batch invalidates pathname caches after ambiguous or effectful create completion",
    )


def main() -> None:
    test_vfs_host_alias_rewrite_is_overlap_safe()
    test_vfs_route_scratch_is_initialized_by_its_producer()
    test_remote_chown_matches_xfs_noop_semantics()
    test_wki_host_mount_scratch_is_initialized_by_its_producers()
    test_vfs_rename_scratch_is_initialized_by_its_producers()
    test_proxy_op_slot_waits_are_bounded()
    test_proxy_operations_fail_before_setup_when_slot_wait_times_out()
    test_proxy_sends_retry_only_pre_enqueue_pressure()
    test_proxy_request_envelopes_use_stack_storage()
    test_readdir_batch_buffers_use_bounded_stack_storage()
    test_proxy_slot_release_paths_handoff_after_unlock()
    test_shared_io_slot_waits_are_bounded()
    test_shared_io_callers_timeout_or_fallback()
    test_message_fallback_readahead_targets_small_sequential_reads()
    test_server_open_reuses_the_open_file_stat_snapshot()
    test_server_backing_path_mutations_bypass_worker_task_root()
    test_remote_path_utimens_preserves_owner_time_and_routing()
    test_server_message_read_uses_bounded_stack_response()
    test_readlink_cache_invalidation_is_generation_based()
    test_server_roce_push_reads_reuse_registered_staging()
    test_remote_metadata_scratch_initializes_only_consumed_prefix()
    test_write_behind_storage_grows_in_allocator_shaped_classes()
    test_message_write_flush_uses_split_send_and_retains_tail()
    test_remote_open_closes_server_fd_on_local_allocation_failure()
    test_remote_close_waits_only_for_writable_output_publication()
    test_export_lookup_returns_locked_snapshot()
    test_rdma_retry_cooldowns_are_saturating()
    test_vfs_attach_ack_requires_expected_cookie_before_completion()
    test_vfs_detach_uses_exact_negotiated_incarnation_form()
    test_remote_vfs_unmount_cancels_waiters_before_teardown()
    test_remote_vfs_teardown_releases_rdma_state_when_idle()
    test_capability_gated_vfs_data_lanes_bound_rdma_buffers()
    test_metadata_batch_never_replays_after_a_send_attempt()
    test_remote_open_refs_delay_proxy_destroy_until_close()
    test_remote_vfs_channel_identity_survives_pool_slot_reuse()
    test_server_bounded_metadata_responses_use_stack_storage()
    test_stale_fd_gc_drains_binding_users_before_file_close()
    test_server_fd_and_consumer_rx_use_exact_channel_identity()
    test_path_change_notifies_each_matching_export_once()
    test_invalidate_notify_retains_and_invalidates_complete_mount_group()
    test_export_rebuild_is_revisioned_and_backing_mount_exact()
    test_remote_vfs_mount_lanes_preserve_channel_and_lifetime_affinity()
    print("WKI remote VFS source invariants hold")


if __name__ == "__main__":
    main()
