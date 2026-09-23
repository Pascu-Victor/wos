#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTABLE_CPP = ROOT / "modules/kern/src/net/wki/remotable.cpp"
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
    match = re.search(rf"\b(?:void|auto|bool)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def test_remote_chown_reports_missing_owner_operation() -> None:
    core = VFS_CORE_CPP.read_text()
    path_chown = function_body(core, "vfs_chown_resolved_path")
    file_chown = function_body(core, "vfs_fchown_file_as")
    require_order(path_chown, ["case FSType::REMOTE:", "return -EOPNOTSUPP;"],
                  "remote path chown must report missing wire operation")
    require_order(file_chown, ["case FSType::REMOTE:", "return -EOPNOTSUPP;"],
                  "remote fd chown reports missing wire operation without consuming its caller reference")
    if "xfs_chown_path(" not in path_chown or "xfs_fchown(" not in file_chown:
        fail("local XFS ownership must use implemented backend policy")


def test_wki_host_mount_scratch_is_initialized_by_its_producers() -> None:
    core = VFS_CORE_CPP.read_text()
    mount = function_body(core, "ensure_wki_host_root_mount")
    if "vfs_mkdir(mount_root.data()" in mount:
        fail("host-root attach must not recurse through mapped mkdir before mount publication")
    if "wki_remote_vfs_mount(NODE_ID, find_ctx.result.resource_id, mount_root.data(), find_ctx.result.generation)" not in mount:
        fail("host-root attach must directly publish the observed resource generation")

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
            "vfs_rename_resolved_paths(current_metadata_credentials(), old_buf.data(), new_buf.data(), path_requires_directory(oldpath),",
            "old_buf_len, new_buf_len, old_buf_hash, new_buf_hash)",
        ],
        "vfs_rename producer-before-consumer ordering",
    )

    renameat = function_body(core, "vfs_renameat")
    require_order(
        renameat,
        [
            "LookupHandle source{};",
            "LookupHandle destination{};",
            "vfs_acquire_lookup(task, olddirfd, oldpath, LookupIntent::RENAME_SOURCE, LookupFollowPolicy::NOFOLLOW_FINAL",
            "vfs_acquire_lookup(task, newdirfd, newpath, LookupIntent::RENAME_TARGET, LookupFollowPolicy::NOFOLLOW_FINAL",
            "if (source.mount() != destination.mount())",
            "return -EXDEV;",
            "if (source.mount()->fs_type == FSType::REMOTE)",
            "return consume_rename(source, destination, nullptr);",
            "ker::mod::sys::MutexGuard publication_guard(g_vfs_namespace_publication_mutex)",
            "consume_lookup_pair_locked(source, destination, true, consume_rename, nullptr, task, &REQUEST_CREDENTIALS)",
            "if (result != -EAGAIN || attempt == 3)",
        ],
        "vfs_renameat retained source/destination acquisition and bounded commit",
    )
    require_tokens(
        renameat,
        [
            "ker::vfs::tmpfs::tmpfs_rename_lookup(",
            "ker::vfs::xfs::xfs_rename_lookup(source.authorization().credentials, source_lookup, destination_lookup, &renamed,",
            "vfs_cache_notify_path_changed(source.path(), destination.path())",
            "vfs_rename_resolved_paths(source.authorization().credentials, source.path(), destination.path(),",
        ],
        "vfs_renameat retained backend and compatibility consumers",
    )

    for body, name, data_uses, total_uses in [(rename, "old_buf", 3, 6), (rename, "new_buf", 3, 6)]:
        if body.count(f"{name}.data()") != data_uses:
            fail(f"{name} must have exactly {data_uses} producer/consumer data uses")
        if len(re.findall(rf"\b{re.escape(name)}\b", body)) != total_uses:
            fail(f"{name} has an unexpected use outside its producers and final consumer")

    for legacy in [
        "std::array<char, MAX_PATH_LEN> old_buf;",
        "std::array<char, MAX_PATH_LEN> new_buf;",
        "old_buf = {};",
        "new_buf = {};",
        "old_buf.fill(",
        "new_buf.fill(",
        "std::memset(old_buf.data()",
        "std::memset(new_buf.data()",
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
            "try_slot_only ? try_acquire_proxy_op_slot_locked(state) : acquire_proxy_op_slot_locked(state, PROXY_WAIT_START)",
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
    send_body = function_body(WKI_CPP.read_text(), "wki_send_impl")
    require_order(
        send_body,
        [
            "return WKI_ERR_NO_ROUTE;",
            "net::pkt_alloc_tx()",
            "int const RET = route.send_pkt(pkt)",
            "ch->tx_seq++",
            "return WKI_OK;",
        ],
        "route admission failure must precede reliable sequence ownership",
    )
    published_send = send_body[send_body.index("int const RET = route.send_pkt(pkt)"):]
    if re.search(r"\breturn\s+(?!WKI_OK\s*;)", published_send):
        fail("a queued reliable frame must not return a retryable send failure")
    retryable_body = function_body(source, "vfs_proxy_send_status_is_retryable")
    tracked_body = function_body(source, "vfs_proxy_send_and_wait")
    untracked_body = function_body(source, "vfs_proxy_send_untracked")
    rdma_body = function_body(source, "vfs_proxy_write_rdma_and_wait")

    require_tokens(
        retryable_body,
        [
            "status == WKI_ERR_NO_CREDITS",
            "status == WKI_ERR_NO_MEM",
            "status == WKI_ERR_NO_ROUTE",
        ],
        "proxy retryable pre-enqueue statuses",
    )
    for terminal_status in [
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
            "send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT",
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
            "send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT",
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
            "send_ret = send_ret == WKI_ERR_NO_ROUTE ? WKI_ERR_NO_ROUTE : WKI_ERR_TIMEOUT",
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
                "size_t const MAX_REQ_DATA_LEN = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - WRAPPER",
                "req_data_len > MAX_REQ_DATA_LEN",
                "req_tail_len > MAX_REQ_DATA_LEN - req_data_len",
                "return -EMSGSIZE",
                "req_data_len > 0 && req_data == nullptr",
                "req_tail_len > 0 && req_tail == nullptr",
                "return -EINVAL",
                "auto const REQ_DATA_LEN = static_cast<uint16_t>(WRAPPER + req_data_len + req_tail_len)",
                "size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + WRAPPER + req_data_len",
                "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized))",
                "reinterpret_cast<DevOpReqPayload*>(req_buf.data())",
                "memcpy(req_buf.data() + sizeof(DevOpReqPayload) + WRAPPER, req_data, req_data_len)",
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
            "size_t const REQ_FIXED_LEN = RECORD_SIZE + OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0)",
            "REQ_FIXED_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload)",
            "PATH_LEN > WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload) - REQ_FIXED_LEN",
            "return nullptr",
            "auto path_len = static_cast<uint16_t>(PATH_LEN)",
            "size_t const REQ_DATA_LEN = REQ_FIXED_LEN + PATH_LEN",
            "std::array<uint8_t, OPEN_REQ_INLINE_CAPACITY> inline_req_data",
            "uint8_t* req_data = inline_req_data.data()",
            "if (REQ_DATA_LEN > inline_req_data.size())",
            "req_data = preparation.allocate_request(REQ_DATA_LEN)",
            "if (req_data == nullptr) return nullptr",
            "vfs_proxy_send_and_wait(state, OPEN_OPERATION, req_data, REQ_DATA_LEN",
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
            "size_t const REQ_FIXED_LEN = RECORD_SIZE + OPEN_REQ_BASE_LEN + (send_open_prefetch ? OPEN_PREFETCH_REQ_LEN : 0)",
            "if (send_open_prefetch)",
            "tagged_receive.rkey = state->rdma_bulk_rkey",
            "vfs_proxy_send_and_wait(state, OPEN_OPERATION",
            "wki_roce_region_tagged_write_complete(state->rdma_bulk_rkey, tagged_receive.cookie, open_resp.prefetched_bytes)",
        ],
        "open prefetch shared-slot fallback",
    )
    open_prefetch_completion = open_body[
        open_body.index("uint32_t valid_prefetched_bytes = 0") : open_body.index("// Required private storage was allocated before sending OPEN.")
    ]
    if "wki_roce_region_wait_tagged_write" in open_prefetch_completion or "VFS_PROXY_OP_TIMEOUT_US" in open_prefetch_completion:
        fail("post-response optional open prefetch must fall back causally without another timed wait")


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
            "EXPECTED_LEN != payload_len",
            "resp->op_id == OP_VFS_XATTR && RESP_DATA_LEN > state->op_resp_max",
            "state->op_status = -EMSGSIZE",
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
            "prepare_mapped_scalar_path(export_path, export_name",
            "if (PATH_RET != 0)",
            "reject_request(static_cast<int16_t>(PATH_RET))",
            "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
            "RemoteFdReservation reservation(channel_identity)",
            "if (reservation.result() < 0)",
            "ker::vfs::vfs_open_file_resolved_beneath_as(*worker, mapped_credentials, export_path, full_path.data()",
        ],
        "remote VFS server open full-path production before resolved-open consumption",
    )
    direct_open_path_tokens = [
        "std::array<char, 512> full_path __attribute__((uninitialized));",
        "std::array<char, 512> full_visible_path __attribute__((uninitialized));",
        "prepare_mapped_scalar_path(export_path, export_name",
        "if (PATH_RET != 0)",
        "path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())",
    ]
    for token in direct_open_path_tokens:
        token_pos = open_case.find(token)
        if token_pos < 0 or brace_depth_at(open_case, token_pos) != 1:
            fail("remote VFS server open full-path production and consumption must be unconditional case statements")
    require_order(
        open_case,
        [
            "ker::vfs::vfs_open_file_resolved_beneath_as(*worker, mapped_credentials, export_path, full_path.data()",
            "ker::vfs::vfs_fstat_file(file, &open_stat)",
            "open_resp.has_stat = 1",
            "std::memcpy(reinterpret_cast<uint8_t*>(&open_resp) + offsetof(OpenRespPayload, stat), &open_stat, sizeof(open_stat))",
            "if (prefetch_rkey != 0",
            "reservation.publish(file, 0, 0, 0, ordinary_token, ordinary_binding_id)",
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
            "release_server_file(orphan)",
        ],
        "remote VFS server open metadata snapshot reuse",
    )
    if "vfs_stat_resolved(full_path.data(), &open_stat)" in open_case:
        fail("remote VFS server open must not repeat path resolution for metadata")
    published_file = open_case.find("reservation.publish(file, 0, 0, 0, ordinary_token, ordinary_binding_id)")
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
    if "reject_request(-ENOTSUP)" not in symlink_case or "vfs_symlink_resolved_beneath(" in symlink_case:
        fail("legacy symlink must refuse callerless owner authority")

    rename_case = handler_body[rename_start:chmod_start]
    if "reject_request(-ENOTSUP)" not in rename_case or "vfs_rename_resolved_beneath(" in rename_case:
        fail("legacy rename must refuse callerless authority")

    chmod_case = handler_body[chmod_start:handler_body.index("case OP_VFS_UTIMENS:", chmod_start)]
    if "reject_request(-ENOTSUP)" not in chmod_case or "vfs_chmod_resolved_beneath(" in chmod_case:
        fail("legacy chmod must refuse ambient worker authorization")
    mapped = handler_body[handler_body.index("if (request.operation == VfsPathOperation::CHMOD)"):]
    require_order(mapped, ["wki_vfs_chmod_request_valid", "credential_record::decode", "map_vfs_mutation_identity",
                          "find_remote_fd(channel_identity, prefix.directory_fd)", "vfs_chmod_resolved_beneath_as(*task, mapped"],
                  "mapped chmod authorizes one snapshot before retained-directory and inode effects")

    require_tokens(
        header,
        [
            "auto vfs_symlink_resolved(const char* target, const char* linkpath) -> int;",
            "auto vfs_symlink_resolved_beneath(const char* confinement_root, const char* target, const char* linkpath) -> int;",
            "auto vfs_rename_resolved(const char* oldpath, const char* newpath) -> int;",
            "auto vfs_rename_resolved_beneath(const char* confinement_root, const char* oldpath, const char* newpath) -> int;",
            "auto vfs_chmod_resolved(const char* path, int mode, bool follow_final_symlink) -> int;",
            "auto vfs_chmod_resolved_beneath(const char* confinement_root, const char* path, int mode, bool follow_final_symlink) -> int;",
        ],
        "resolved VFS mutation API declarations",
    )
    require_tokens(
        function_body(core, "vfs_symlink_resolved"),
        ["return vfs_symlink_resolved_linkpath(current_metadata_credentials(), target, linkpath);"],
        "resolved symlink backing-path implementation",
    )
    require_tokens(
        function_body(core, "vfs_rename_resolved"),
        ["vfs_rename_resolved_paths(current_metadata_credentials(), oldpath, newpath, path_requires_directory(oldpath),"],
        "resolved rename backing-path implementation",
    )
    require_tokens(
        function_body(core, "vfs_chmod_resolved"),
        ["return vfs_chmod_resolved_path(current_metadata_credentials(), path, mode, follow_final_symlink);"],
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
            "wki_remote_vfs_chmod(mount->private_data, fs_path, mode, follow_final_symlink, CREDENTIALS)",
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

    client = " ".join(function_body(source, "wki_remote_vfs_utimens").split())
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "TIMES = times != nullptr", "TimestampUpdate::prepare",
        "wki_resource_observation_require_flags", "RESOURCE_FLAG_VFS_UTIMENS_IDENTITY", "credential_record::encode(REQUEST_CREDENTIALS)",
        "VfsPathUtimensPrefix const PREFIX", ".atime_sec = TIMES[0].tv_sec", ".mtime_nsec = TIMES[1].tv_nsec",
        "vfs_proxy_send_and_wait", "wki_vfs_path_response_valid"], "mapped timestamp request preserves snapshot and time intent")
    require_order(client, ["if (directory != nullptr)", "io_guard(context->io_lock)", "send(context->proxy, context->remote_fd)"],
        "timestamp directory operation keeps the retained exact lane")
    if "vfs_proxy_read_with_retry" in client or "OP_VFS_UTIMENS," in client or client.count("vfs_proxy_send_and_wait(") != 1:
        fail("timestamp mutation cannot use a read retry or legacy callerless fallback")
    if "reject_request(-ENOTSUP)" not in utimens_case or "vfs_utimens_resolved_beneath(" in utimens_case:
        fail("legacy timestamp operation must refuse before local mutation")
    mapped = handler[handler.index("if (request.operation == VfsPathOperation::UTIMENS)"):]
    mapped = mapped[:mapped.index("if (request.operation == VfsPathOperation::READLINK_AT")]
    require_order(mapped, ["wki_vfs_utimens_request_valid", "mapped_wire_path_status", "credential_record::decode",
        "map_vfs_mutation_identity", "find_remote_fd(channel_identity", "prepare_mapped_scalar_path", "vfs_utimens_resolved_beneath_as"],
        "owner timestamp handler maps and confines before committing")
    mapped_owner = function_body(core, "vfs_utimens_resolved_beneath_as")
    require_order(mapped_owner, ["REQUEST_CREDENTIALS = credentials", "resolve_utimens_times", "for (unsigned attempt",
        "acquire_confined_owner_lookup", "lookup_is_beneath_export", "consume_lookup_locked"],
        "owner retains one timestamp value and caller snapshot across precommit retries")
    if "current_metadata_credentials" in mapped_owner or "task.credentials()" in mapped_owner:
        fail("mapped timestamp owner cannot borrow ambient credentials")
    entry = " ".join(function_body(core, "vfs_utimensat").split())
    require_order(entry, ["REQUEST_CREDENTIALS = task->credentials()", "REQUEST_TIMES =", "times = REQUEST_TIMES.data()",
        "resolve_utimens_times", "for (unsigned attempt", "LookupIntent::CHANGE_TIMES"], "public timestamp request captures once")
    require_tokens(entry, ["handle.remote_relative_path()", "handle.dirfd_anchor()", "&REQUEST_CREDENTIALS",
        "if (lookup.mount()->fs_type == FSType::REMOTE) return consume_utimens(lookup, &context)"], "timestamp retained path and no remote replay")

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
            "vfs_apply_utimens_to_resolved_path(CREDENTIALS, canonical_path.data(), times, follow_final_symlink, canonical_path_len, false,",
            "&POLICY)",
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
            "if (!REMOTE_MOUNT && !lookup_symlinks_resolved)",
            "bool const RESOLVE_FINAL_SYMLINK = follow_final_symlink && !skip_final_symlink_probe",
            "resolve_symlinks(path_buffer.data(), resolved_path.data()",
            "mount_ref = find_mount_point(path_buffer.data(), resolved_len)",
            "if (!allow_remote_backend && mount->fs_type == FSType::REMOTE)",
            "case FSType::REMOTE:",
            "wki_remote_vfs_utimens(mount->private_data, fs_path, times, follow_final_symlink, CREDENTIALS)",
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
            "resolve_prefix_symlink_once(",
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
            "send_completed_vfs_read_reply(channel_identity, resp_buf.data(), SEND_LEN, binding_retiring)",
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
    require_order(readlink_body,
                  ["WKI_CAP_VFS_PATH_IDENTITY", "credential_record::encode", "vfs_proxy_read_with_retry(state, OP_VFS_PATH_AS",
                   "wki_vfs_path_response_valid", "std::memcpy(buf,"],
                  "mapped readlink validates authority and staged reply before publication")
    for token in ("try_readlink_cache_lookup", "cache_readlink_result", "readlink_unsupported"):
        if token in readlink_body:
            fail("mapped READLINK must bypass credentialless cache state")

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

    handler = function_body(source, "handle_vfs_op")
    if handler.count("prepare_legacy_scalar_path(") != 0 or handler.count("prepare_mapped_scalar_path(") != 10:
        fail("every scalar pathname output must use centralized checked preparation")
    if "build_full_path(" in handler:
        fail("scalar opcode cases must not bypass centralized component and dual-path admission")
    declaration_counts = {
        "std::array<char, 512> full_path __attribute__((uninitialized));": 2,
        "std::array<char, 512> full_visible_path __attribute__((uninitialized));": 1,
        "std::array<char, 512> full_link __attribute__((uninitialized));": 0,
        "std::array<char, 512> full_visible_link __attribute__((uninitialized));": 0,
        "std::array<char, 512> old_full __attribute__((uninitialized));": 0,
        "std::array<char, 512> new_full __attribute__((uninitialized));": 0,
        "std::array<uint8_t, 514> req_stack __attribute__((uninitialized));": 0,
        "std::array<uint8_t, 518> req_stack __attribute__((uninitialized));": 0,
        "std::array<uint8_t, 519> req_stack __attribute__((uninitialized));": 0,
        "std::array<uint8_t, 1028> req_stack __attribute__((uninitialized));": 0,
        "std::array<uint8_t, 514> resp_buf __attribute__((uninitialized));": 0,
    }
    for declaration, expected_count in declaration_counts.items():
        actual_count = source.count(declaration)
        if actual_count != expected_count:
            fail(f"metadata scratch declaration count changed for {declaration}: expected {expected_count}, found {actual_count}")

    build = function_body(source, "build_full_path")
    require_order(build, ["out[0] = '\\0'", "size_t const EXPORT_LEN", "out[pos] = '\\0'"], "full-path output is terminated after construction")
    require_tokens(
        build,
        [
            "if (EXPORT_LEN >= out_size)",
            "return -ENAMETOOLONG",
            "size_t remaining = out_size - EXPORT_LEN - 1",
            "if (rel_len > remaining)",
            "memcpy(out, export_path, EXPORT_LEN)",
            "memcpy(out + pos, relative_path, rel_len)",
        ],
        "bounded full-path construction",
    )
    prepare = function_body(source, "prepare_legacy_scalar_path")
    require_order(
        prepare,
        [
            "relative_wire_path_has_safe_components(relative_path, relative_path_len)",
            "build_full_path(full_path, full_path_size, export_path",
            "if (BACKING_RET != 0)",
            "return BACKING_RET",
            "build_full_path(full_visible_path, full_visible_path_size, export_name",
        ],
        "scalar admission validates components and both bounded path views",
    )

    boundary = function_body(source, "path_crosses_recursive_wki_boundary_direct")
    require_tokens(
        boundary,
        [
            "path_crosses_remote_mount_direct(backing_path)",
            "path_crosses_recursive_self_alias(visible_path)",
        ],
        "owner recursive-mount boundary uses already admitted backing and visible paths",
    )
    if "resolve_mount_path(" in boundary:
        fail("owner recursive-mount boundary must not re-resolve admitted paths through worker task state")

    readlink = function_body(source, "wki_remote_vfs_readlink_path")
    require_order(readlink,
                  ["std::array<uint8_t, DATA_OFFSET + 511> request", "credential_record::encode", "std::memcpy(request.data(), RECORD.data()",
                   "std::memcpy(request.data() + RECORD_SIZE, &HEADER", "std::memcpy(request.data() + DATA_OFFSET, fs_relative_path, PATH_LEN)",
                   "vfs_proxy_read_with_retry(state, OP_VFS_PATH_AS", "wki_vfs_path_response_valid"],
                  "mapped READLINK exact request encoding and staged response")
    require_tokens(readlink, ["DATA_OFFSET + PATH_LEN", "std::array<uint8_t, sizeof(VfsPathIdentityHeader) + 512> response"],
                   "bounded mapped READLINK scratch")


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

    require_order(
        function_body(source, "remote_vfs_write"),
        ["FileGrowth growth", "growth.bound_write(offset, count)", "remote_vfs_write_impl(f, buf, count, offset)"],
        "file-size admission must precede all remote write side effects",
    )
    require_order(
        function_body(source, "remote_vfs_truncate"),
        ["FileGrowth growth", "growth.allow_resize", "remote_vfs_truncate_impl(f, length)"],
        "file-size admission must precede remote truncate flushes and mutations",
    )
    write_body = function_body(source, "remote_vfs_write_impl")
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
        "remote_vfs_write_impl",
        "remote_vfs_truncate_impl",
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
        function_body(source, "remote_vfs_truncate_impl"),
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
            "static_cast<uint16_t>(WRAPPER + req_data_len + req_tail_len)",
            "size_t const REQ_PREFIX_TOTAL = sizeof(DevOpReqPayload) + WRAPPER + req_data_len",
            "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> req_buf __attribute__((uninitialized))",
            "memcpy(req_buf.data() + sizeof(DevOpReqPayload) + WRAPPER, req_data, req_data_len)",
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


def test_remote_open_prepares_storage_before_mutation_request() -> None:
    source = REMOTE_VFS_CPP.read_text()
    open_body = function_body(source, "wki_remote_vfs_open_path")
    if "remote_vfs_close_remote_fd_best_effort" in source:
        fail("ordinary OPEN cleanup must use its exact token, not a returned numeric FD")
    require_order(
        open_body,
        [
            "PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN)",
            "if (preparation.status() != 0) return nullptr",
            "lane_ref_guard.reset()",
            "auto* file = preparation.file()",
            "auto* ctx = preparation.context()",
            "req_data = preparation.allocate_request(REQ_DATA_LEN)",
            "if (req_data == nullptr) return nullptr",
            "vfs_proxy_send_and_wait(state, OPEN_OPERATION",
            "ctx->proxy = state",
            "return preparation.publish()",
        ],
        "required private storage and task-exit ownership precede a scalar OPEN request",
    )
    if "new (std::nothrow) ker::vfs::File" in open_body or "new (std::nothrow) RemoteFileContext" in open_body:
        fail("scalar OPEN must use its tracked preparation for required storage")
    if "remote_vfs_close_remote_fd_best_effort" in open_body:
        fail("scalar OPEN must not replace preflight allocation with a post-mutation close")
    drain = function_body(source, "drain")
    require_tokens(drain, ["delete retired.file", "delete retired.context", "delete[] retired.request",
                           "release_vfs_proxy_open_ref(retired.proxy)"], "private preparation owns all abandoned storage")


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
            "release_server_file(close_file)",
            "status = static_cast<int16_t>",
            "perf_record_vfs_server_end",
            "if (status != 0 || !NO_SUCCESS_RESPONSE)",
            "send_simple_resp(resp)",
        ],
        "owner close completes the generic VFS lifecycle before suppressing only a successful opted-in response",
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
            "wki_peer_lifecycle_acquire(final_peer)",
            "wki_resource_observation_is_live",
            "int const POST_AUX_VALIDATION = validate_mount_binding()",
            'ker::vfs::mount_filesystem(local_mount_path, "remote"',
            "state->mount_configured = true",
            "retire_constructed_vfs_mount_group(state, false)",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "private mount admission precedes publication; concurrent retirement preserves commit",
    )
    if "wki_remote_vfs_unmount_resource_generation" in mount_body:
        fail("failed candidate must not retire other mounts of the same resource")
    committed_tail = mount_body[mount_body.index("state->mount_configured = true"):]
    if re.findall(r"return\s+([^;]+);", committed_tail) != ["0"]:
        fail("successful mount publication must not return a later transport failure")
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
            "PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN)",
            "if (preparation.status() != 0) return nullptr",
            "lane_ref_guard.reset()",
            "ctx->proxy = state",
            "return preparation.publish()",
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
        [
            "vfs_proxy_requires_peer_cleanup(p, node_id)",
            "deactivate_vfs_proxy_locked(p, cleanup, false)",
        ],
        "task-context peer cleanup must consume active, pre-marked, and in-progress attach proxies",
    )
    cleanup_gate_body = function_body(source, "vfs_proxy_requires_peer_cleanup")
    require_tokens(
        cleanup_gate_body,
        [
            "state->owner_node == node_id",
            "state->active",
            "state->epoch_reset_pending",
            "state->attach_pending.load(std::memory_order_acquire)",
        ],
        "remote VFS peer cleanup admission includes pending attach waiters",
    )
    require_tokens(
        source,
        ["wki_remote_vfs_selftest_peer_cleanup_includes_pending_attach"],
        "pending remote VFS attach cleanup selftest",
    )
    require_tokens(
        WKI_DEV_PROXY_KTEST.read_text(),
        ["KTEST(WkiRemoteVfsPeerCleanup, PendingAttachWaitIsIncluded)"],
        "pending remote VFS attach cleanup KTEST registration",
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
            "for (auto& [fd_id, rfd] : g_remote_fds)",
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
            "g_remote_fds.find(fd_id)",
            "rfd.retiring && rfd.file == nullptr && vfs_channel_identity_matches",
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
            "ker::vfs::Stat statbuf{}",
            "ker::vfs::vfs_stat_resolved_beneath_as(",
            "std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::Stat)> response",
            "reinterpret_cast<DevOpRespPayload*>(response.data())",
            "memcpy(response.data() + sizeof(*header), &statbuf, sizeof(statbuf))",
            "send_buffered_resp(response.data(), static_cast<uint16_t>(response.size()))",
        ],
        "fixed-size stat response storage",
    )

    readlink_case = handler_body[readlink_start:symlink_start]
    require_order(readlink_case,
                  ["WKI_CAP_VFS_PATH_IDENTITY", "credential_record::decode", "map_vfs_read_identity", "vfs_readlink_resolved_beneath_as",
                   "std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(VfsPathIdentityHeader) + 512> response",
                   "std::memcpy(response.data() + sizeof(*header), &REPLY", "send_buffered_resp(response.data(),"],
                  "mapped READLINK bounded owner response")

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
            "release_server_file(file)",
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
            "release_server_file(file)",
            "wki_peer_lifecycle_release(peer)",
        ],
        "GC must drain deferred VFS binding refs before detaching and closing File",
    )
    if "alloc_remote_fd(" in REMOTE_VFS_CPP.read_text():
        fail("scalar OPEN must not allocate registry storage after the backend operation")
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
            "g_remote_fds.find(fd_id)",
            "rfd.active",
            "!rfd.retiring",
            "vfs_channel_identity_matches(rfd.channel_identity, channel_identity)",
        ],
        "server FD exact-generation lookup",
    )
    allocate = block_body_after(source, "explicit RemoteFdReservation(const WkiChannelIdentity& identity) : identity_(identity)")
    require_order(
        allocate,
        [
            "row.consumer_node = identity.peer_node_id",
            "row.channel_identity = identity",
            "prepared.emplace(0, row)",
            "s_vfs_lock.lock()",
            "node.mapped().fd_id = fd_",
            "g_remote_fds.insert(std::move(node))",
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

    async_close = function_body(dev_server, "wki_dev_server_admit_vfs_op_rx")
    require_order(
        async_close,
        [
            "wki_vfs_close_no_success_response_requested(req_data, req->data_len)",
            "try_queue_pre_admitted_async_close",
            "try_queue_pre_admitted_vfs_op",
        ],
        "one-way close fixed work admission precedes reliable ACK",
    )
    for outcome in ("WkiVfsOpRxAdmission::RETRY", "WkiVfsOpRxAdmission::DEFERRED"):
        if outcome not in async_close:
            fail(f"VFS RX admission is missing {outcome}")
    for unsafe in ("operator new", "new ", "vfs_close_file", "dbg::log"):
        if unsafe in async_close:
            fail(f"one-way close RX precommit contains unsafe operation {unsafe!r}")

    fixed_queue = function_body(dev_server, "try_queue_pre_admitted_async_close")
    require_order(
        fixed_queue,
        [
            "s_vfs_async_close_pool_lock.try_lock()",
            "shard->lock.try_lock()",
            "s_vfs_async_close_free = op->next",
            "op->fixed_async_close = true",
            "op->op_id = OP_VFS_CLOSE",
            "shard->pending.fetch_add",
            "shard->lock.unlock()",
            "s_vfs_async_close_pool_lock.unlock()",
        ],
        "fixed close work enters the exact VFS FIFO without allocation or blocking locks",
    )
    for unsafe in ("operator new", "new ", "vfs_close_file", "wake_task_from_event"):
        if unsafe in fixed_queue:
            fail(f"fixed close queue admission contains unsafe operation {unsafe!r}")

    rx = function_body(WKI_CPP.read_text(), "wki_rx")
    ordered_dispatch = function_body(WKI_CPP.read_text(), "reliable_dispatch_needs_serial_order")
    require_tokens(
        ordered_dispatch,
        [
            "ch->channel_id == WKI_CHAN_IPC_DATA",
            "channel_requires_existing_rx_reservation(ch->channel_id)",
        ],
        "ordinary dynamic resource channels publish handlers in reliable receive order",
    )
    require_order(
        " ".join(rx.split()),
        [
            "admit_vfs_op(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation, admitted_identity_policy, admitted_identity_capabilities)",
            "VFS_OP_ADMISSION == WkiVfsOpRxAdmission::RETRY",
            "ch->rx_seq++",
        ],
        "in-order async close commits before reliable RX sequence publication",
    )
    if rx.count("admit_vfs_op(") != 2:
        fail("both direct and reorder reliable RX paths must precommit every VFS request")

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
            "release_server_file(files_to_close.at(i))",
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
            "wki_remote_vfs_cleanup_server_fds_for_channel(item.channel_identity)",
        ],
        "ordinary and reconciliation binding teardown close exact server FDs",
    )
    channel_cleanup = function_body(source, "wki_remote_vfs_cleanup_server_fds_for_channel")
    require_order(
        channel_cleanup,
        [
            "g_vfs_xattr_stages.find(vfs_xattr_stage_key(channel_identity))",
            "g_vfs_xattr_stages.erase(STAGE_IT)",
            "release_vfs_xattr_stage(stage)",
            "wki_remote_vfs_process_pending_server_fd_cleanup()",
        ],
        "detach cycles release exact-generation xattr stages and retained files",
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
            "(PUBLICATION_REVISION & 1U) != 0",
            "g_vfs_withdrawals.pending(PEER_SLOT)",
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
            "g_vfs_withdrawals.append(exp.resource_id, INCARNATION, peers)",
            "reconcile_and_publish_vfs_exports()",
            "wki_remote_vfs_advertise_exports()",
        ],
        "reserved stale-token outbox publishes before replacement table and advertisements",
    )
    require_order(
        prepare,
        ["g_vfs_withdrawals.reserve(g_vfs_exports.size())", "g_vfs_export_target_revision = TARGET_REVISION",
         "wki_dev_server_begin_vfs_export_reconciliation(TARGET_REVISION)"],
        "withdrawal capacity is reserved before destructive pivot admission",
    )
    require_tokens(reconcile, ["g_vfs_withdrawals.cancel_reservation()"], "commit/cancel releases unused withdrawal capacity")
    publish = function_body(source, "wki_remote_vfs_publish_withdrawals")
    require_order(
        publish,
        ["g_vfs_withdrawals.claim(PEER_SLOT, attempt)", "s_vfs_lock.unlock()", "wki_reliable_tx_status(attempt.tx)",
         "wki_peer_lifecycle_acquire(peer)", "wki_resource_incarnation_negotiated(peer_node, ResourceType::VFS)",
         "wki_send_tracked(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_WITHDRAW", "wki_peer_lifecycle_release(peer)",
         "g_vfs_withdrawals.finish(PEER_SLOT, attempt, STATUS, result, replacement)"],
        "tracked withdrawal send occurs outside VFS mutex with stable peer wire shape",
    )
    if "wki_dev_server_refresh_vfs_binding" in source:
        fail("export insertion must not mutate one binding before full-table reconciliation")

    unmount = function_body(core, "vfs_umount")
    require_order(unmount, [
        "authorize_mount_administration(CREDENTIALS)",
        "snapshot_bounded_path_string(target",
        "expected_dev_id = mount->dev_id",
        "mount->fs_type != FSType::REMOTE",
        "if (expected_dev_id == 0) return -ENOENT",
        "wki_remote_vfs_prepare_export_rebuild()",
        "unmount_filesystem_if_dev_id(stable_target.data(), expected_dev_id)",
        "if (RESULT == 0)",
        "wki_remote_vfs_commit_export_unmount(expected_dev_id)",
        "wki_remote_vfs_cancel_export_rebuild()",
        "return RESULT",
    ], "native unmount drains before exact commit and preserves failed bindings")

    commit_unmount = function_body(source, "wki_remote_vfs_commit_export_unmount")
    require_order(commit_unmount, [
        "g_wki.peer_lock.unlock()", "s_vfs_lock.lock()",
        "it->backing_dev_id != backing_dev_id",
        "g_vfs_withdrawals.append(it->resource_id, INCARNATION, peers)",
        "g_vfs_exports.erase(it)", "reconcile_and_publish_vfs_exports()",
        "wki_remote_vfs_advertise_exports()",
    ], "native unmount retires exact backing exports before reopening admission")
    if "wki_remote_vfs_auto_discover_internal" in commit_unmount or "build_export_name" in commit_unmount:
        fail("native unmount must not rename unrelated exports through caller root")

    pivot = function_body(core, "vfs_pivot_root")
    require_order(
        pivot,
        [
            "snapshot_bounded_path_string(new_root",
            "snapshot_bounded_path_string(put_old",
            "wki_remote_vfs_prepare_export_rebuild()",
            "remap_mounts_for_pivot(stable_new_root.data(), stable_put_old.data(), &CREDENTIALS, &references)",
            "if (remap_result == 0)",
            "rebase_wki_mounts_for_new_root(stable_new_root.data())",
            "if (REMAP_RET != 0)",
            "wki_remote_vfs_cancel_export_rebuild()",
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
            "uint8_t const TARGET_LANE_COUNT = vfs_proxy_lane_count_for_mount(local_mount_path)",
            "for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT; ++lane_index)",
            "mount_vfs_proxy_lane(owner_node, resource_id, local_mount_path, RESOURCE_GENERATION",
            "int const POST_AUX_VALIDATION = validate_mount_binding()",
            'ker::vfs::mount_filesystem(local_mount_path, "remote"',
            "if (MOUNT_RET != 0)",
            "state->mount_configured = true",
        ],
        "mount publishes the prepared group only after auxiliary admission",
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
            "anchor->lanes_ready",
            "anchor->lane_count == lane_index",
            "anchor->lanes.at(lane_index) = state",
        ],
        "auxiliary lane publication remains serialized and contiguous in the private ready group",
    )
    require_order(
        lane_mount,
        [
            "int const PRE_AUX_VALIDATION = validate_mount_binding()",
            "uint8_t const TARGET_LANE_COUNT = vfs_proxy_lane_count_for_mount(local_mount_path)",
            "for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT; ++lane_index)",
            "if (LANE_RET == -ENOENT || LANE_RET == -ESTALE)",
            "retire_constructed_vfs_mount_group(state, true)",
            "release_vfs_proxy_lifecycle_ref(state)",
            "return LANE_RET",
            "int const POST_AUX_VALIDATION = validate_mount_binding()",
            "retire_constructed_vfs_mount_group(state, true)",
        ],
        "definitive auxiliary rejection rolls back before final binding/resource revalidation",
    )
    post_aux = lane_mount[lane_mount.find("for (uint8_t lane_index = 1; lane_index < TARGET_LANE_COUNT;") :]
    require_order(
        post_aux,
        [
            "state->lanes_ready",
            "retire_constructed_vfs_mount_group(state, true)",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "post-aux refusal retires only the exact private candidate before dropping its construction pin",
    )

    for function_name in [
        "wki_remote_vfs_stat",
        "wki_remote_vfs_mkdir",
        "wki_remote_vfs_chmod",
        "wki_remote_vfs_utimens",
        "wki_remote_vfs_symlink",
        "remote_vfs_remove_path",
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
            "PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN)",
            "if (preparation.status() != 0) return nullptr",
            "lane_ref_guard.reset()",
            "ctx->proxy = state",
        ],
        "data-oriented open selects a direction-capable RDMA lane and transfers its ownership to the file context",
    )
    if source.count("acquire_vfs_proxy_lane(anchor,") != 1:
        fail("only data-oriented open may request RDMA-anchor lane selection")
    fstat_body = function_body(source, "wki_remote_vfs_fstat")
    require_tokens(fstat_body, ["ctx->proxy, OP_VFS_FSTAT", "&ctx->remote_fd"], "fstat keeps its FD lane and descriptor")
    if "wki_remote_vfs_stat(mount->private_data" in fstat_body:
        fail("fstat must not reselect a lane for an existing remote FD")

    for function_name in [
        "remote_vfs_close",
        "remote_vfs_read",
        "remote_vfs_write_impl",
        "remote_vfs_lseek",
        "remote_vfs_readdir",
        "remote_vfs_truncate_impl",
        "remote_vfs_fsync_file",
    ]:
        body = function_body(source, function_name)
        if "acquire_vfs_proxy_lane(" in body:
            fail(f"{function_name} must use its RemoteFileContext lane directly")

    for function_name in [
        "wki_remote_vfs_mkdir",
        "wki_remote_vfs_symlink",
        "remote_vfs_remove_path",
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
    round_robin_selftest = function_body(source, "wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity")

    require_tokens(
        wire,
        [
            "constexpr uint16_t WKI_CAP_VFS_MULTI_RDMA_LANES = 0x0008;",
            "constexpr uint8_t DEV_ATTACH_DISABLE_RDMA = 0x40;",
            "constexpr uint8_t DEV_ATTACH_VFS_AUX_LANE = 0x80;",
            "auto wki_vfs_proxy_attach_mode(",
            "auto wki_vfs_attach_lane_is_anchor(",
            "static_assert(sizeof(DevAttachReqPayload) == 12",
            "static_assert(sizeof(HelloPayload) == 172",
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
        round_robin_selftest,
        [
            "std::unique_ptr<ProxyVfsState[]> lanes",
            "new (std::nothrow) ProxyVfsState[VFS_PROXY_LANE_COUNT]{}",
            "if (lanes == nullptr)",
            "auto& anchor = lanes[0]",
            "for (size_t index = 1; index < VFS_PROXY_LANE_COUNT; ++index)",
        ],
        "full-capacity selector selftest keeps its large lane fixtures off the kernel stack",
    )
    if "ProxyVfsState anchor{}" in round_robin_selftest or "std::array<ProxyVfsState" in round_robin_selftest:
        fail("full-capacity selector selftest must not restore its oversized automatic fixture")
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
            "mapped_wire_path_status",
            "item_size > MAX_REQUEST_DATA - mutation_request_len",
            "return -EOPNOTSUPP",
            "wki_peer_capability_negotiated(anchor->owner_node, WKI_CAP_VFS_METADATA_BATCH)",
            "return -EOPNOTSUPP",
            "acquire_vfs_proxy_lane(anchor)",
            "mutation_attempted = true",
            "*mutation_request_attempted = true",
            "vfs_proxy_send_and_wait(state, OP_VFS_METADATA_BATCH",
            "STATUS == -EOPNOTSUPP ? -EPROTO : normalize_proxy_status_for_errno(STATUS)",
        ],
        "metadata batch validates and negotiates before its first send and never exposes replayable EOPNOTSUPP afterwards",
    )
    attempt_pos = batch.find("mutation_attempted = true")
    if attempt_pos < 0 or batch.rfind("return -EOPNOTSUPP") >= attempt_pos:
        fail("metadata batch may expose EOPNOTSUPP only before a mutation attempt")
    require_order(batch, ["credential_record::encode(*credentials)", "REQUEST_PREFIX", "mutation_request_len = REQUEST_PREFIX",
        "IDENTITY = proxy_channel_identity_locked", "std::memcpy(request.data() + sizeof(VfsMetadataBatchHeader)",
        "std::memcpy(request.data(), &REQUEST_HEADER", "OP_VFS_FEATURES", "WKI_VFS_FEATURE_BATCH_MUTATION_IDENTITY",
        "mutation_attempted = true", "OP_VFS_METADATA_BATCH"], "mapped batch copies complete envelope before exact-binding query and one commit")
    if batch.count("false, &IDENTITY") != 2:
        fail("feature query and batch submission must both require captured channel identity")
    if "if (READ_ONLY) credential_record" in batch or "if (READ_ONLY) std::memcpy" in batch:
        fail("mutation batches must carry the same immutable record as read batches")
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


def test_remote_xattr_is_bounded_replay_safe_and_export_confined() -> None:
    wire = WIRE_HPP.read_text()
    remote = REMOTE_VFS_CPP.read_text()
    server = DEV_SERVER_CPP.read_text()
    wki = WKI_CPP.read_text()
    core = VFS_CORE_CPP.read_text()

    require_tokens(
        wire,
        [
            "WKI_CAP_VFS_XATTR = 0x0040",
            "OP_VFS_XATTR = 0x0417",
            "struct VfsXattrPhaseHeader",
            "uint64_t session_id",
            "uint64_t operation_id",
            "WKI_VFS_XATTR_NAME_MAX = 255",
            "WKI_VFS_XATTR_DATA_MAX = 65536",
            "BEGIN = 1",
            "DATA = 2",
            "COMMIT = 3",
            "ABORT = 4",
        ],
        "remote xattr additive wire contract",
    )
    require_tokens(
        wki,
        [
            "req->op_id == OP_VFS_XATTR",
            "g_wki.capabilities = WKI_CAP_RESOURCE_INCARNATION",
            "WKI_CAP_VFS_XATTR | WKI_CAP_RESOURCE_LIMITS | WKI_CAP_VFS_READ_IDENTITY | WKI_CAP_COMPUTE_CREDENTIALS |",
        ],
        "remote xattr fixed-pool RX admission and advertisement",
    )
    require_tokens(
        server,
        [
            "op_id == OP_VFS_XATTR",
            "wki_peer_capability_negotiated(op->hdr.src_node, WKI_CAP_VFS_XATTR)",
            "run_deferred_vfs_op",
        ],
        "remote xattr deferred-worker capability gate",
    )
    require_tokens(
        remote,
        [
            "VFS_XATTR_SERVER_STAGE_COUNT_MAX = 128",
            "VFS_XATTR_SERVER_STAGE_BYTES_MAX = 1024ULL * 1024ULL",
            "std::map<VfsXattrStageKey, VfsXattrServerStage*>",
            "retain_vfs_xattr_stage(stage)",
            "VfsXattrStageRefGuard const STAGE_GUARD",




            "remote_xattr_name_allowed",
            'USER_PREFIX = "user."',
            "vfs_xattr_data_is_exact_replay",
            "vfs_xattr_resolved_beneath_as(*worker, stage.credentials",
            "vfs_xattr_file_as(stage.credentials, stage.file",
            "incoming.operation_id <= old.identity.operation_id",
            "old.identity.session_id != incoming.session_id",
            "begin.flags == 0 || begin.flags == 1 || begin.flags == 2",
            "release_vfs_xattr_stage_data(*stage)",
            "chunk.chunk_len == 0",
            "chunk.chunk_len > WKI_VFS_XATTR_MAX_DATA_CHUNK",
            "static_cast<uint64_t>(measured) > stage->staged_bytes",
            "g_vfs_xattr_stages.size() < VFS_XATTR_SERVER_STAGE_COUNT_MAX",
            "invalidate_readlink_cache_group(state)",
            "vfs_cache_notify_path_changed(state->local_mount_path.data(), nullptr)",
            "vfs_cache_notify_file_changed",
        ],
        "remote xattr bounded staging, exact replay, namespace restriction, confinement, and invalidation",
    )
    owner = function_body(remote, "handle_vfs_xattr_op")
    require_order(owner, ["identity.version != WKI_VFS_XATTR_VERSION", "credential_record::decode(caller_record, supplied)",
                          "if (ADMISSION != 0)", "VfsXattrServerStage* prior_stage", "stage->credentials = mapped"],
                  "xattr version and explicit identity admission precede stage replacement")
    require_order(owner, ["capture_vfs_xattr_export(", "VfsXattrServerStage* prior_stage", "stage->export_root = captured_root",
                          "stage->export_name = captured_name"], "capture both export values before prior-stage cancellation")
    capture = function_body(remote, "capture_vfs_xattr_export")
    require_order(capture, ["strnlen(path", "strnlen(name", "return -ENAMETOOLONG", "root.fill", "std::memcpy"],
                  "bounded capture validates both names before either output changes")
    call = function_body(remote, "vfs_xattr_stage_call")
    require_tokens(call, ["vfs_xattr_file_as(stage.credentials", "vfs_xattr_resolved_beneath_as(*worker, stage.credentials",
                          "stage.require_directory", "stage.file"], "both xattr targets consume staged caller identity")
    require_tokens(call, ["stage.export_name[0] == '/' ? stage.export_name.data() : nullptr"],
                   "path xattr consumes captured namespace while named exports preserve defaults")
    native = function_body(remote, "selftest_xattr_staged_namespace")
    require_tokens(native, ["capture_vfs_xattr_export", "root.fill('x')", "visible.fill('y')", "vfs_xattr_stage_apply_mutation(stage, &worker)",
                           "vfs_xattr_stage_call(stage", "vfs::XattrOperation::LIST", "-EOPNOTSUPP", "-EROFS"],
                   "native stage fixture covers lifetime, mutation, read and unsupported/refusal paths")
    if "->credentials()" in call:
        fail("xattr owner dispatch cannot borrow worker credentials")
    replay = function_body(remote, "vfs_xattr_begin_is_exact_replay")
    require_tokens(replay, ["old.remote_fd == begin.remote_fd", "old.caller_record == caller_record",
                            "old.require_directory ==", "old.path == path", "old.name == name", "old.export_root == export_root", "old.export_name == export_name"],
                   "duplicate xattr BEGIN must identify the original request")
    stage = block_body_after(remote, "struct VfsXattrServerStage")
    require_tokens(
        stage,
        ["ker::vfs::vfs_put_file(file);"],
        "remote xattr stages release only their retained File reference",
    )
    if "vfs_close_file(file)" in stage:
        fail("remote xattr stage teardown must not destroy a File still owned by the remote-fd table")
    client = function_body(remote, "remote_vfs_xattr_on_proxy")
    require_order(
        client,
        [
            "wki_peer_capability_negotiated(state->owner_node, WKI_CAP_VFS_XATTR)",
            "state->xattr_transfer_lock.lock()",
            "invoke(OP_VFS_FEATURES",
            "features.features & WKI_VFS_FEATURE_XATTR_IDENTITY",
            "operation_id = ++state->xattr_next_operation_id",
            "invoke(OP_VFS_XATTR",
        ],
        "remote xattr rejects unsupported peers before identity allocation and transmission",
    )
    require_tokens(
        client,
        [
            "vfs_xattr_transfer_total(READ_OPERATION, size, TOTAL)",
            "false, &IDENTITY)",
            "credential_record::encode(REQUEST_CREDENTIALS)",
            "offset < TRANSFER_TOTAL",
            "normalize_proxy_status_for_errno(status)",
        ],
        "remote xattr size probes avoid DATA and transport failures expose only POSIX errno",
    )
    require_tokens(
        core,
        [
            "mount->fs_type == FSType::REMOTE && policy == nullptr",
            "wki_remote_vfs_setxattr(mount->private_data",
            "wki_remote_vfs_getxattr(mount->private_data",
            "wki_remote_vfs_listxattr(mount->private_data",
            "wki_remote_vfs_removexattr(mount->private_data",
            "wki_remote_vfs_fsetxattr(credentials, file",
            "wki_remote_vfs_fgetxattr(credentials, file",
            "wki_remote_vfs_flistxattr(credentials, file",
            "wki_remote_vfs_fremovexattr(credentials, file",
        ],
        "VFS core remote xattr path and retained File dispatch",
    )


def test_exec_view_preserves_owner_grant_and_complete_reply() -> None:
    source = REMOTE_VFS_CPP.read_text()
    owner = function_body(source, "pin_exec_view")
    if not (owner.index("find_remote_fd(identity, request.source_fd)") < owner.index("authorize_file_description_access") <
            owner.index("vfs_stabilize_exec_file") < owner.index("reservation.publish")):
        fail("remote executable views must pin the retained read grant before publication")
    if "open_path" in owner or "get_current_task" in owner:
        fail("remote executable pinning must not reopen a path or borrow worker credentials")
    client = function_body(source, "wki_remote_vfs_clone_exec_file")
    if not (client.index("wki_resource_observation_require_flags") < client.index("if (SUPPORT != 0) return SUPPORT;") <
            client.index("new (std::nothrow)") < client.index("allocate_identity()") <
            client.index("reserve_exec_cancellation(STATE, TOKEN)")):
        fail("unsupported executable views must fail before allocation, token, cancellation ownership or RPC")
    for field in ("STATE->owner_node", "ResourceType::VFS", "STATE->resource_id", "STATE->resource_generation",
                  "STATE->owner_incarnation", "RESOURCE_FLAG_VFS_EXEC_VIEW"):
        if field not in client[:client.index("if (SUPPORT != 0) return SUPPORT;")]:
            fail("executable support admission must check the retained source's complete resource identity")
    if "adv->flags = RESOURCE_FLAG_VFS_EXEC_VIEW | RESOURCE_FLAG_VFS_CHMOD_IDENTITY | RESOURCE_FLAG_VFS_UTIMENS_IDENTITY;" not in function_body(source, "advertise_exports_to_peer"):
        fail("VFS owners must advertise executable-view v1 support in the existing resource flags")
    if not (client.index("allocate_identity()") < client.index("vfs_proxy_send_and_wait") <
            client.index("wki_vfs_exec_view_response_valid") < client.index("*output = preparation.publish()")):
        fail("only a validated versioned owner reply may publish an executable view")
    if "cancel_remote_exec_view(CANCEL_SLOT)" not in client:
        fail("failed PIN must cancel with its known token rather than an untrusted reply FD")
    close = function_body(source, "remote_vfs_close")
    if not (close.index("f->exec_read_view != nullptr") < close.index("close_status = cancel_remote_exec_view") < close.index("else if (NEEDS_CLOSE_STATUS)")):
        fail("executable views require acknowledged owner retirement even though read-only")
    if not (client.index("reserve_exec_cancellation(STATE, TOKEN)") < client.index("vfs_proxy_send_and_wait")):
        fail("PIN requires durable cancellation storage before any owner side effect")
    if "context->exec_cancel_slot = CANCEL_SLOT" not in client:
        fail("a published execution view must own its reserved cancellation record")
    finish = function_body(source, "finish_exec_cancellation")
    if "ExecCancellationPhase::PENDING" not in finish or "release_vfs_proxy_open_ref(released)" not in finish:
        fail("failed cancellation must retain ownership, successful cancellation releases its independent proxy ref")
    cancel = function_body(source, "cancel_remote_exec_view")
    if "&record.identity" not in cancel or "finish_exec_cancellation" not in cancel:
        fail("every cancellation outcome must retain or release its exact original identity")
    core = WKI_CPP.read_text()
    if "wki_remote_vfs_process_pending_exec_cancellations" in function_body(core, "process_deferred_blocking_work"):
        fail("client cancellation waits cannot occupy the protocol-service worker")
    if "wki_remote_vfs_process_pending_exec_cancellations" not in function_body(core, "process_deferred_auto_attach_work"):
        fail("durable cancellation retry needs the separate client worker")
    reply = function_body(source, "handle_vfs_op_resp")
    if "wki_vfs_exec_view_reply_size_valid" not in reply or "INVALID_EXEC" not in reply:
        fail("RX must reject malformed PIN/CANCEL replies before generic response truncation")


def test_scalar_metadata_keeps_snapshot_and_binding_through_retries() -> None:
    core = VFS_CORE_CPP.read_text()
    for name, intent, consume in [("vfs_fchmodat", "CHANGE_MODE", "consume_chmod"), ("vfs_fchownat", "CHANGE_OWNER", "consume_chown_lookup"), ("vfs_utimensat", "CHANGE_TIMES", "consume_utimens")]:
        public = function_body(core, name)
        require_order(public, ["REQUEST_CREDENTIALS = task->credentials()", "for (unsigned attempt", "LookupIntent::" + intent, "&REQUEST_CREDENTIALS", "return " + consume + "(lookup, &context)", "publication_guard", "consume_lookup_locked"], "scalar metadata preserves one caller and never replays a remote result")
        if public.count("task->credentials()") != 1 or "vfs_consume_lookup(" in public: fail("metadata retries recapture ambient authority")
        require_tokens(public, ["consume_lookup_locked(lookup, false, " + consume + ", &context, task, &REQUEST_CREDENTIALS)"], "native commit must use explicit request identity")
    source = REMOTE_VFS_CPP.read_text()
    for name in ("wki_remote_vfs_chmod", "wki_remote_vfs_utimens"):
        client = function_body(source, name)
        require_order(client, ["REQUEST_CREDENTIALS = credentials", "proxy_channel_identity_locked(state)", "s_vfs_lock.unlock()", "wki_resource_observation_require_flags", "vfs_proxy_send_and_wait", "&IDENTITY"], "metadata support and send must retain one complete binding")
        if client.count("vfs_proxy_send_and_wait(") != 1 or "with_retry" in client: fail("scalar metadata must submit exactly once")


def test_mapped_chmod_retains_snapshot_and_exact_directory_lane() -> None:
    source = REMOTE_VFS_CPP.read_text()
    client = function_body(source, "wki_remote_vfs_chmod")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "wki_resource_observation_require_flags", "RESOURCE_FLAG_VFS_CHMOD_IDENTITY",
                           "credential_record::encode(REQUEST_CREDENTIALS)", "vfs_proxy_send_and_wait", "wki_vfs_path_response_valid"],
                  "chmod binds support and a snapshot before one mutation request")
    if "vfs_proxy_read_with_retry" in client or "OP_VFS_CHMOD," in client:
        fail("chmod must not retry a mutation as a read or use the legacy callerless request")
    require_order(client, ["if (directory != nullptr)", "io_guard(context->io_lock)", "send(context->proxy, context->remote_fd)"],
                  "directory chmod keeps the exact retained proxy lane")
    core = VFS_CORE_CPP.read_text()
    entry = function_body(core, "vfs_fchmodat")
    for token in ("LookupIntent::CHANGE_MODE", "handle.remote_relative_path()", "handle.dirfd_anchor()", "handle.authorization().credentials"):
        if token not in entry: fail("chmod lost mapped path/descriptor admission: " + token)
    owner = function_body(core, "vfs_chmod_resolved_beneath_as")
    require_order(owner, ["REQUEST_CREDENTIALS = credentials", "acquire_confined_owner_lookup", "lookup_is_beneath_export",
                          "consume_lookup_locked"], "owner chmod commits only the admitted confined lookup")
    if "current_metadata_credentials()" in owner or "task.credentials()" in owner:
        fail("mapped owner chmod cannot borrow serving credentials")


def test_mapped_removal_preserves_caller_and_never_replays() -> None:
    source = REMOTE_VFS_CPP.read_text()
    client = function_body(source, "remote_vfs_remove_path")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "wki_resource_observation_is_live",
        "s_vfs_lock.lock()", "IDENTITY = proxy_channel_identity_locked(state)", "s_vfs_lock.unlock()",
        "vfs_proxy_send_and_wait", "&IDENTITY", "credential_record::encode(REQUEST_CREDENTIALS)",
        "invoke(OP_VFS_FEATURES", "wki_vfs_features_response_valid", "WKI_VFS_FEATURE_REMOVE_IDENTITY",
        "invoke(OP_VFS_PATH_AS", "wki_vfs_path_response_valid", "invalidate_readlink_cache_group(state)"],
        "removal queries support and commits once on one exact live channel binding")
    require_tokens(client, ["std::array<uint8_t, OFFSET + 511> request{}", "VfsPathRemovePrefix const PREFIX",
        "io_guard(context->io_lock)", "send(context->proxy, context->remote_fd)", "ProxyLifecycleRefGuard lane_ref_guard(state)"],
        "removal retains bounded framing and exact directory or mount lane")
    if client.count("invoke(OP_VFS_PATH_AS") != 1 or "OP_VFS_UNLINK," in client or "vfs_proxy_read_with_retry" in client:
        fail("removal cannot retry an uncertain mutation or use callerless fallback")
    handler = function_body(source, "handle_vfs_op")
    mapped = handler[handler.index("if (request.operation == VfsPathOperation::REMOVE)"):]
    mapped = mapped[:mapped.index("if (request.operation == VfsPathOperation::UTIMENS)")]
    require_order(mapped, ["wki_vfs_remove_request_valid", "mapped_wire_path_status", "credential_record::decode",
        "map_vfs_mutation_identity", "find_remote_fd(channel_identity", "prepare_mapped_scalar_path",
        "vfs_unlink_resolved_beneath_as"], "removal owner maps identity before confined parent mutation")
    reply = function_body(source, "handle_vfs_op_resp")
    require_tokens(reply, ["INVALID_FEATURES", "!INVALID_RESPONSE", "sizeof(VfsFeaturesResponse)"],
        "feature reply framing must be checked before truncation and status publication")
    core = VFS_CORE_CPP.read_text()
    owner = function_body(core, "vfs_unlink_resolved_beneath_as")
    require_order(owner, ["REQUEST_CREDENTIALS = credentials", "acquire_confined_owner_lookup",
        "lookup_is_beneath_export", "consume_lookup_locked"], "removal uses one admitted owner snapshot")
    if "current_metadata_credentials()" in owner or "task.credentials()" in owner:
        fail("mapped removal cannot borrow worker credentials")
    entry = function_body(core, "vfs_unlinkat")
    require_order(entry, ["REQUEST_CREDENTIALS = task->credentials()", "for (unsigned attempt", "vfs_acquire_lookup"],
        "public removal holds one snapshot across precommit lookup retries")
    require_tokens(entry, ["handle.remote_relative_path()", "handle.dirfd_anchor()", "&REQUEST_CREDENTIALS",
        "if (lookup.mount()->fs_type == FSType::REMOTE) return consume_unlink(lookup, &context)"],
        "removal preserves raw components, retained directory and no remote replay")

    for name, expression in [("wki_remote_vfs_unlink", "false, credentials, require_directory, directory"),
                             ("wki_remote_vfs_rmdir", "true, credentials, true, directory")]:
        require_tokens(function_body(source, name), ["remote_vfs_remove_path", expression], "removal wrappers retain operation semantics")
    legacy = handler[handler.index("case OP_VFS_UNLINK:"):handler.index("case OP_VFS_RENAME:")]
    require_tokens(legacy, ["case OP_VFS_RMDIR:", "reject_request(-ENOTSUP)"], "callerless scalar removal is unsupported")
    if "vfs_unlink_resolved_beneath" in legacy:
        fail("legacy scalar removal must not commit")

def test_mapped_mkdir_queries_exact_binding_before_one_commit() -> None:
    source = REMOTE_VFS_CPP.read_text()
    client = function_body(source, "wki_remote_vfs_mkdir")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "wki_resource_observation_is_live",
        "s_vfs_lock.lock()", "IDENTITY = proxy_channel_identity_locked(state)", "s_vfs_lock.unlock()",
        "vfs_proxy_send_and_wait", "&IDENTITY", "credential_record::encode(REQUEST_CREDENTIALS)",
        "invoke(OP_VFS_FEATURES", "wki_vfs_features_response_valid", "WKI_VFS_FEATURE_MKDIR_IDENTITY",
        "invoke(OP_VFS_PATH_AS", "wki_vfs_path_response_valid", "invalidate_readlink_cache_group(state)"],
        "mkdir queries support and commits once on one exact live channel binding")
    require_tokens(client, ["std::array<uint8_t, OFFSET + 511> request{}", "VfsPathMkdirPrefix const PREFIX",
        "io_guard(context->io_lock)", "send(context->proxy, context->remote_fd)", "ProxyLifecycleRefGuard lane_ref_guard(state)"],
        "mkdir retains bounded framing and exact directory or mount lane")
    if client.count("invoke(OP_VFS_PATH_AS") != 1 or "OP_VFS_MKDIR," in client or "vfs_proxy_read_with_retry" in client:
        fail("mkdir cannot retry an uncertain mutation or use callerless fallback")
    handler = function_body(source, "handle_vfs_op")
    mapped = handler[handler.index("if (request.operation == VfsPathOperation::MKDIR)"):]
    mapped = mapped[:mapped.index("if (request.operation == VfsPathOperation::UTIMENS)")]
    require_order(mapped, ["wki_vfs_mkdir_request_valid", "mapped_wire_path_status", "credential_record::decode",
        "map_vfs_mutation_identity", "find_remote_fd(channel_identity", "prepare_mapped_scalar_path",
        "vfs_mkdir_resolved_beneath_as"], "mkdir owner maps identity before confined parent mutation")
    reply = function_body(source, "handle_vfs_op_resp")
    require_tokens(reply, ["INVALID_FEATURES", "!INVALID_RESPONSE", "sizeof(VfsFeaturesResponse)"],
        "feature reply framing must be checked before truncation and status publication")
    core = VFS_CORE_CPP.read_text()
    owner = function_body(core, "vfs_mkdir_resolved_beneath_as")
    require_order(owner, ["REQUEST_CREDENTIALS = credentials", "acquire_confined_owner_lookup",
        "lookup_is_beneath_export", "consume_lookup_locked"], "mkdir uses one admitted owner snapshot")
    if "current_metadata_credentials()" in owner or "task.credentials()" in owner:
        fail("mapped mkdir cannot borrow worker credentials")
    entry = function_body(core, "vfs_mkdirat")
    require_order(entry, ["REQUEST_CREDENTIALS = task->credentials()", "for (unsigned attempt", "vfs_acquire_lookup"],
        "public mkdir holds one snapshot across precommit lookup retries")
    require_tokens(entry, ["handle.remote_relative_path()", "handle.dirfd_anchor()", "&REQUEST_CREDENTIALS",
        "if (lookup.mount()->fs_type == FSType::REMOTE) return consume_mkdir(lookup, &context)"],
        "mkdir preserves raw components, retained directory and no remote replay")


def test_mapped_symlink_preserves_literal_target_and_one_identity() -> None:
    source = REMOTE_VFS_CPP.read_text()
    client = function_body(source, "wki_remote_vfs_symlink")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "wki_resource_observation_is_live",
        "IDENTITY = proxy_channel_identity_locked(state)", "&IDENTITY", "credential_record::encode(REQUEST_CREDENTIALS)",
        "std::memcpy(request.data() + OFFSET, target, TARGET_LENGTH)", "invoke(OP_VFS_FEATURES",
        "wki_vfs_features_response_valid", "WKI_VFS_FEATURE_SYMLINK_IDENTITY", "invoke(OP_VFS_PATH_AS",
        "wki_vfs_path_response_valid", "invalidate_readlink_cache_group(state)"],
        "symlink copies caller and literal bytes before support query on exact binding")
    require_tokens(client, ["TARGET_LENGTH > 511", "VfsPathSymlinkPrefix", "OFFSET + 2 * 511",
        "io_guard(context->io_lock)", "send(context->proxy, context->remote_fd)", "ProxyLifecycleRefGuard lane_ref_guard(state)"],
        "symlink bounds independent data/path and retains its exact directory/lane")
    if client.count("invoke(OP_VFS_PATH_AS") != 1 or "vfs_proxy_read_with_retry" in client or "OP_VFS_SYMLINK," in client:
        fail("symlink cannot replay a mutation or use callerless fallback")
    handler = function_body(source, "handle_vfs_op")
    mapped = handler[handler.index("if (request.operation == VfsPathOperation::SYMLINK)"):]
    mapped = mapped[:mapped.index("if (request.operation == VfsPathOperation::UTIMENS)")]
    require_order(mapped, ["wki_vfs_symlink_request_valid", "wire_symlink_target_is_representable", "mapped_wire_path_status",
        "credential_record::decode", "map_vfs_mutation_identity", "find_remote_fd(channel_identity",
        "prepare_mapped_scalar_path", "vfs_symlink_resolved_beneath_as"], "owner symlink maps before retained parent commit")
    core = VFS_CORE_CPP.read_text()
    entry = function_body(core, "vfs_symlinkat")
    require_order(entry, ["REQUEST_CREDENTIALS = task->credentials()", "for (unsigned attempt", "vfs_acquire_lookup"],
        "public symlink captures once across precommit retries")
    require_tokens(entry, ["handle.remote_relative_path()", "handle.dirfd_anchor()", "&REQUEST_CREDENTIALS",
        "if (lookup.mount()->fs_type == FSType::REMOTE) return consume_symlink(lookup, &context)"],
        "symlink keeps raw ancestor checks and cannot replay a remote result")
    owner = function_body(core, "vfs_symlink_resolved_beneath_as")
    require_order(owner, ["REQUEST_CREDENTIALS = credentials", "acquire_confined_owner_lookup", "lookup_is_beneath_export",
        "consume_lookup_locked"], "owner consumes only admitted confined identity")
    if "current_metadata_credentials()" in owner or "task.credentials()" in owner:
        fail("mapped symlink cannot borrow worker credentials")
    pending = function_body(REMOTABLE_CPP.read_text(), "wki_remotable_process_pending_mounts")
    if "vfs_mkdir(" in pending:
        fail("autoattach must not wait in mkdir while its own withdrawal is queued")


def test_mapped_rename_uses_exact_directory_bindings_and_one_commit():
    source = REMOTE_VFS_CPP.read_text()
    client = function_body(source, "wki_remote_vfs_rename")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "credential_record::encode", "std::memcpy(request.data() + OFFSET",
        "first_guard.emplace", "acquire_vfs_proxy_lane", "IDENTITY = proxy_channel_identity_locked", "OP_VFS_FEATURES",
        "WKI_VFS_FEATURE_RENAME_IDENTITY", "binding(source", "binding(destination", "// Exactly one namespace mutation",
        "invoke(state, IDENTITY, OP_VFS_PATH_AS", "invalidate_readlink_cache_group"], "rename copies input and queries exact directory bindings before one commit")
    require_tokens(client, ["second != first", "std::swap(first, second)", "proxy->mount_group_id != state->mount_group_id",
        "VfsPathOperation::DIRECTORY_BINDING", "reference.channel_id == identity.channel_id", "&identity"], "rename keeps lock order, mount group and binding identity")
    if "OP_VFS_RENAME," in client or "vfs_proxy_read_with_retry" in client:
        fail("rename must not fall back or replay an uncertain mutation")
    handler = function_body(source, "handle_vfs_op")
    mapped = handler[handler.index("if (request.operation == VfsPathOperation::RENAME)"):]
    mapped = mapped[:mapped.index("if (request.operation == VfsPathOperation::REMOVE)")]
    require_order(mapped, ["wki_vfs_rename_request_valid", "credential_record::decode", "map_vfs_mutation_identity",
        "wki_dev_server_retain_vfs_sibling", "s_vfs_lock.lock()", "find_remote_fd(identity", "s_vfs_lock.unlock()",
        "prepare(prefix.source", "prepare(prefix.destination", "vfs_rename_resolved_beneath_as"], "owner pins exact sibling bindings before confined rename")
    server = DEV_SERVER_CPP.read_text()
    sibling = function_body(server, "wki_dev_server_retain_vfs_sibling")
    require_order(sibling,["s_server_lock.lock_irqsave", "find_retained_binding_by_channel_identity", "vfs_sibling_binding_matches",
        "retain_binding_locked", "candidate.channel_identity", "s_server_lock.unlock_irqrestore"], "sibling acquisition pins a validated binding under one short lock")
    match = function_body(server,"vfs_sibling_binding_matches")
    require_tokens(match,["!candidate.epoch_reset_pending", "!candidate.detach_cleanup_pending", "candidate.retiring.load",
        "origin.consumer_node == candidate.consumer_node", "candidate.channel_identity.generation == generation", "origin.resource_id == candidate.resource_id",
        "wki_resource_incarnation_equal", "vfs_export_publication_revision", "vfs_export_dev_id", "std::strcmp"], "sibling authority includes peer, incarnation, export and lifecycle")


def test_remote_descriptor_metadata_maps_one_caller_and_never_replays() -> None:
    source = REMOTE_VFS_CPP.read_text()
    core = VFS_CORE_CPP.read_text()
    client = function_body(source, "remote_file_metadata_request")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "io_guard(context->io_lock)",
        "proxy_channel_identity_locked(state)", "invoke(OP_VFS_FEATURES", "WKI_VFS_FEATURE_FILE_METADATA_IDENTITY",
        "invoke(OP_VFS_PATH_AS", "wki_vfs_path_response_valid", "wki_remote_vfs_invalidate_open_file_caches"],
        "descriptor mutation must admit an exact feature binding before one mutation and invalidation")
    require_tokens(client, ["&IDENTITY", "file->fops != &g_remote_vfs_fops", "context->remote_fd <= 0",
        "VfsFileChmodRequest", "VfsFileChownRequest", "VfsFileUtimensRequest", "TimestampUpdate::prepare"],
        "descriptor metadata validates file identity and preserves bounded typed payloads")
    if client.count("invoke(OP_VFS_PATH_AS") != 1 or "acquire_vfs_proxy_lane" in client or "vfs_path" in client.replace("wki_vfs_path_response_valid", ""):
        fail("descriptor metadata must not replay, reselect a lane, or reconstruct a pathname")
    handler = function_body(source, "handle_vfs_op")
    owner = handler.split("if (wki_vfs_is_file_metadata(request.operation))", 1)[1].split("if (request.operation == VfsPathOperation::CHMOD)", 1)[0]
    require_order(owner, ["decode_file_metadata_request", "map_vfs_mutation_identity", "find_remote_fd(channel_identity, metadata.fd)",
        "vfs_retain_file", "s_vfs_lock.unlock()", "file->fs_type == ker::vfs::FSType::REMOTE", "file->exec_read_view != nullptr",
        "vfs_fchmod_file_as(file.get(), mapped", "vfs_fchown_file_as(file.get(), mapped", "vfs_futimens_file_as(file.get(), mapped"],
        "owner maps caller before retaining exact File and commits outside registry lock")
    if "get_current_task" in owner or "credentials()" in owner or "prepare_mapped_scalar_path" in owner:
        fail("owner file metadata cannot borrow worker identity or resolve a cached path")
    for operation, entry in [("chmod", "vfs_fchmod_for_task"), ("chown", "vfs_fchown_for_task"), ("utimens", "vfs_futimens")]:
        wrapper = function_body(core, entry)
        require_order(wrapper, ["task->credentials()", "vfs_get_file_retain", "wki_remote_vfs_f" + operation,
            "cache_notify_file_metadata_changed_impl", "vfs_put_file"], "public wrapper retains one caller and file through remote commit")
        if "publication_guard" in wrapper or wrapper.count("task->credentials()") != 1:
            fail("remote metadata wrapper cannot wait under publication or resnapshot the caller")


def test_path_chown_keeps_one_caller_and_exact_binding() -> None:
    core = VFS_CORE_CPP.read_text()
    source = REMOTE_VFS_CPP.read_text()
    public = function_body(core, "vfs_fchownat")
    require_order(public, ["REQUEST_CREDENTIALS = task->credentials()", "for (unsigned attempt", "LookupIntent::CHANGE_OWNER", "&REQUEST_CREDENTIALS", "consume_chown_lookup", "publication_guard", "consume_lookup_locked"], "chown request survives lookup retries with one Snapshot")
    if public.count("task->credentials()") != 1: fail("chown recaptures current caller")
    owner = function_body(core, "vfs_chown_resolved_beneath_as")
    require_order(owner, ["REQUEST_CREDENTIALS = credentials", "acquire_confined_owner_lookup", "LookupIntent::CHANGE_OWNER", "publication_guard", "lookup_is_beneath_export", "consume_lookup_locked"], "owner chown must confine and commit with supplied caller")
    consume = function_body(core, "consume_chown_lookup")
    require_tokens(consume, ["handle.remote_relative_path()", "handle.dirfd_anchor()", "handle.authorization().credentials", "handle.requires_directory()", "handle.mount_policy().read_only", "tmpfs_chown_lookup", "xfs_chown_lookup", "devfs_chown_lookup"], "chown retains ancestry, directory, mount and native policy")
    client = function_body(source, "wki_remote_vfs_chown")
    require_order(client, ["REQUEST_CREDENTIALS = credentials", "proxy_channel_identity_locked", "&IDENTITY", "invoke(OP_VFS_FEATURES", "WKI_VFS_FEATURE_CHOWN_IDENTITY", "invoke(OP_VFS_PATH_AS", "wki_vfs_path_response_valid"], "exact feature binding before one chown")
    if client.count("invoke(OP_VFS_PATH_AS") != 1 or "with_retry" in client: fail("chown cannot replay a mutation")
    handler = function_body(source, "handle_vfs_op")
    dispatch = handler.split("if (request.operation == VfsPathOperation::CHOWN)", 1)[1].split("if (request.operation == VfsPathOperation::MKDIR)", 1)[0]
    require_order(dispatch, ["wki_vfs_chown_request_valid", "credential_record::decode", "map_vfs_mutation_identity", "find_remote_fd(channel_identity", "vfs_retain_file", "s_vfs_lock.unlock()", "vfs_chown_resolved_beneath_as(*task, mapped"], "owner maps before retaining exact directory and native call")
    if "task->credentials()" in dispatch: fail("owner chown borrows worker authority")


def test_native_directory_grants_have_separate_close_only_staging():
    source = REMOTE_VFS_CPP.read_text()
    retain = function_body(source, "retain_directory_grant")
    require_order(retain, ["local = row->directory_grant_local_source", "s_vfs_lock.unlock()", "if (!local)",
                           "wki_dev_server_retain_vfs_grant_source", "directory_grant_source_current_locked",
                           "directory_grant_credentials != RECORD", "vfs_retain_file"],
                  "native and remote grant admission retain exact source authority")
    close = function_body(source, "close_native_directory_grant")
    require_tokens(close, ["NativeDirectoryGrantCleanup", "file->private_data = nullptr", "cancel_native_directory_grant"],
                   "close-only local staging must own cancellation")
    ops = source.split("g_native_directory_grant_cleanup_ops{", 1)[1].split("};", 1)[0]
    if ".vfs_close = close_native_directory_grant" not in ops or any(name in ops for name in ("vfs_read", "vfs_write", "vfs_readdir")):
        fail("native grant staging must not grant file data access")
    require_tokens(WKI_DEV_PROXY_KTEST.read_text(), ["NativeOwnerRetainsScopeAndCancelsWithoutPeerBinding",
                   "wki_remote_vfs_selftest_native_directory_grant()"], "native owner fixture must be registered")


def test_grant_rpc_exit_cleanup_transfers_exact_reserved_cancellation():
    source = REMOTE_VFS_CPP.read_text()
    guard = source.split("class PinPreparation {", 1)[1].split("auto vfs_proxy_send_untracked", 1)[0]
    publish = function_body(guard, "publish")
    require_order(publish, ["s_vfs_lock.lock()", "unlink_locked()", "std::exchange(file_, nullptr)",
                           "std::exchange(context_, nullptr)", "slot_ = VFS_EXEC_CANCEL_SLOT_NONE", "s_vfs_lock.unlock()"],
                  "successful grant transfers private File/context and outbox ownership atomically")
    cleanup = function_body(guard, "cleanup_for_task")
    require_order(cleanup, ["candidate->pid_ != pid", "candidate->detach_locked(true)", "s_vfs_lock.unlock()", "drain(retired)"],
                  "task cleanup detaches exact PID before releasing allocations")
    drain = function_body(guard, "drain")
    require_tokens(drain, ["delete retired.file", "delete retired.context", "release_vfs_proxy_open_ref", "wki_deferred_work_notify"],
                   "private preparation drains while durable cancellation keeps its proxy")
    if "s_vfs_lock.lock()" in drain or "vfs_proxy_send_and_wait" in drain:
        fail("private cleanup must not release under registry lock or wait for cancellation")
    require_tokens(WKI_DEV_PROXY_KTEST.read_text(), ["AbandonedPreparationQueuesCancellationAndReleasesPrivateState",
                   "wki_remote_vfs_selftest_directory_grant_preparation()"], "production lifetime fixture must run")


def test_directory_lane_and_executable_pin_ownership_do_not_depend_on_unwinding():
    source = REMOTE_VFS_CPP.read_text()
    directory = function_body(source, "wki_remote_vfs_pin_directory")
    require_order(directory, ["ProxyLifecycleRefGuard lane_guard", "PinPreparation preparation", "lane_guard.reset()",
                              "vfs_proxy_send_and_wait", "preparation.own_cancellation", "OP_VFS_DIRECTORY_REFERENCE",
                              "preparation.publish()"], "directory lane lifetime must transfer before either wait")
    execution = function_body(source, "wki_remote_vfs_clone_exec_file")
    require_order(execution, ["PinPurpose::EXECUTABLE", "exec_read_view.reset", "allocate_identity", "preparation.retain_proxy",
                              "preparation.own_cancellation", "vfs_proxy_send_and_wait", "wki_vfs_exec_view_response_valid",
                              "cancel_remote_exec_view(CANCEL_SLOT)", "return CANCELED != 0 ? CANCELED : result",
                              "preparation.publish()"], "exec preparation owns both PIN and cancellation waits without changing errno precedence")
    require_tokens(WKI_DEV_PROXY_KTEST.read_text(), ["DirectoryAndExecutableLifetimesKeepCancellationPurpose",
                   "wki_remote_vfs_selftest_pin_preparation_purpose()"], "shared purpose lifetime fixture must run")


def test_pin_close_continuation_is_registered_and_preserves_writable_close():
    source = REMOTE_VFS_CPP.read_text()
    close = function_body(source, "remote_vfs_close")
    require_order(close, ["immutable_pin_close(*f, *ctx)", "close_immutable_pin(f)", "flush_write_behind(ctx)",
                          "NEEDS_CLOSE_STATUS", "OP_VFS_CLOSE", "return flush_status != 0 ? flush_status : close_status"],
                  "specialized immutable close must preserve general writable completion and errno")
    backend = function_body(source, "finish_pin_close_backend")
    require_order(backend, ["clear_remote_file_open_prefetch", "delete context->read_cache", "delete context->write_buf",
                            "delete context;", "file->private_data = nullptr", "release_vfs_proxy_open_ref(proxy)"],
                  "backend cleanup retires allocations before releasing proxy")
    if "MutexGuard" in backend or "s_vfs_lock.lock()" in backend:
        fail("immutable backend release must run without a held per-file or registry lock")
    require_tokens(WKI_DEV_PROXY_KTEST.read_text(), ["AbandonedCloseReleasesFilePathMountAndExactCancellation",
                   "ker::net::wki::wki_remote_vfs_selftest_pin_close_lifetime()"], "actual pin close fixture must run")


def test_mapped_open_carries_captured_export_symlink_namespace() -> None:
    source = REMOTE_VFS_CPP.read_text()
    body = function_body(source, "handle_vfs_op")
    start = body.index("case OP_VFS_OPEN_AS:")
    end = body.index("case ", start + len("case OP_VFS_OPEN_AS:"))
    opened = body[start:end]
    assert "export_name[0] == '/' ? export_name : nullptr" in body
    for name in ("vfs_open_read_file_resolved_beneath_as", "vfs_open_file_resolved_beneath_as"):
        call = re.search(rf"{name}\([^;]+;", opened).group(0)
        assert "mapped_credentials" in call and "export_path" in call and "absolute_symlink_root" in call
    assert "worker->root" not in opened
    for name in ("vfs_stat_resolved_beneath_as", "vfs_readlink_resolved_beneath_as", "vfs_realpath_resolved_beneath_as"):
        calls = re.findall(rf"{name}\([^;]+;", body)
        assert len(calls) == 2
        assert all("mapped" in call and "export_path" in call and "absolute_symlink_root" in call for call in calls)
    for operation in ("mkdir", "symlink", "unlink", "rename", "chmod", "chown", "utimens"):
        calls = re.findall(r"vfs_" + operation + r"_resolved_beneath_as\((.*?)\);", body, re.S)
        assert len(calls) == 1
        assert "mapped" in calls[0] and "export_path" in calls[0] and "absolute_symlink_root" in calls[0]



def test_open_reply_admission_preserves_rejection_status() -> None:
    source = REMOTE_VFS_CPP.read_text()
    reply = function_body(source, "handle_vfs_op_resp")
    require_order(reply, ["EXPECTED_LEN != payload_len", "find_vfs_proxy_for_response_locked",
                         "scalar_open_response_valid", "if (INVALID_RESPONSE)",
                         "state->op_status = -EPROTO", "memcpy(state->op_resp_buf, resp_data, copy_len)",
                         "if (!INVALID_RESPONSE", "state->op_status = resp->status"],
                  "reply validation must precede copying and preserve all protocol errors")
    rejection = reply[reply.index("bool const INVALID_RESPONSE"):reply.index("if (INVALID_RESPONSE)")]
    require_tokens(rejection, ["INVALID_STAT", "INVALID_BATCH", "INVALID_PATH", "INVALID_EXEC",
                               "INVALID_FEATURES", "INVALID_DIRECTORY", "INVALID_GRANT", "INVALID_OPEN"],
                   "all reply rejection reasons share one status-publication gate")
    validator = function_body(source, "scalar_open_response_valid")
    require_order(validator, ["length > capacity", "length != OPEN_RESP_NO_STAT_LEN",
                             "std::memcpy(&reply, data, length)", "reply.fd < 0"],
                  "scalar reply validation bounds the copy and rejects invalid descriptors")


def test_ordinary_open_lifetime_owner_uses_ordered_exact_binding() -> None:
    source = REMOTE_VFS_CPP.read_text()
    owner = function_body(source, "handle_vfs_op")
    require_order(owner, ["op_id == OP_VFS_OPEN_LIFETIME", "wki_vfs_open_lifetime_request_valid",
                          "grant_binding == nullptr", "cancel_ordinary_open", "admit_ordinary_open_token",
                          "switch (effective_op)", "reservation.publish(file, 0, 0, 0, ordinary_token, ordinary_binding_id)",
                          "reservation.publish(opened, 0, 0, 0, ordinary_token, ordinary_binding_id)"],
                  "versioned ordinary lifetime wraps both existing open publication paths")
    cancellation = function_body(source, "cancel_ordinary_open")
    require_order(cancellation, ["s_vfs_lock.lock()", "ordinary_open_binding_matches_locked", "vfs_open_token_high_water = std::max",
                                "row.ordinary_open_token != request.token", "row.ordinary_open_binding_id != request.binding_id",
                                "vfs_channel_identity_matches(row.channel_identity, identity)",
                                "g_remote_fds.extract(it)", "s_vfs_lock.unlock()", "ker::vfs::vfs_put_file_result(file)"],
                  "exact token cancellation extracts before releasing outside the lock")
    admission = function_body(source, "admit_ordinary_open_token")
    require_order(admission, ["ordinary_open_binding_matches_locked", "request.token == 0", "request.token <= binding.vfs_open_token_high_water",
                             "status = -EALREADY", "binding.vfs_open_token_high_water = request.token"],
                  "consumed tokens cannot replay mutation")
    wki = WKI_CPP.read_text()
    dispatch = function_body(wki, "admit_vfs_op")
    require_order(dispatch, ["OP_VFS_OPEN_LIFETIME", "EXPECTED_LEN != payload_len",
                            "channel->rx_dispatch_seq != hdr->seq_num", "WkiVfsOpRxAdmission::RETRY",
                            "wki_dev_server_admit_vfs_op_rx"], "new operation enters ordered pre-ACK admission")
    server = DEV_SERVER_CPP.read_text()
    for name in ["is_vfs_op", "wki_dev_server_admit_vfs_op_rx"]:
        require_tokens(function_body(server, name), ["OP_VFS_OPEN_LIFETIME"], "new operation retains its exact worker binding")
    require_tokens(WKI_DEV_PROXY_KTEST.read_text(), ["OwnerTokenCancellationCannotRepeatOrReachSuccessor"],
                   "actual owner lifetime regression remains registered")


def test_ordinary_open_lifetime_client_ownership() -> None:
    source = REMOTE_VFS_CPP.read_text()
    sender = function_body(source, "vfs_proxy_send_and_wait")
    require_order(sender, ["acquire_proxy_op_slot_locked(state, PROXY_WAIT_START)", "take_ordinary_open_token_locked(state)",
                           "state->op_lifetime_inner = lifetime.inner_op", "state->lock.unlock()",
                           "ordinary->request.token = lifetime.token", "wki_send_on_channel_identity"],
                  "token allocation follows the operation gate and publication precedes transport")
    binding = function_body(source, "prepare_ordinary_open_binding")
    require_order(binding, ["WKI_CAP_RESOURCE_INCARNATION", "OP_VFS_FEATURES", "WKI_VFS_FEATURE_OPEN_LIFETIME",
                            "OP_VFS_OPEN_LIFETIME", "wki_vfs_open_binding_response_valid", "response.owner_boot_epoch != EPOCH",
                            "state->ordinary_open_binding_id = response.binding_id"], "feature and exact challenged owner binding")
    for function in ("wki_remote_vfs_open_path", "wki_remote_vfs_open_at"):
        body = function_body(source, function)
        require_order(body, ["preparation.prepare_ordinary_open()", "vfs_proxy_send_and_wait", "preparation.ordinary_slot()",
                             "preparation.publish()"], "ordinary open reserves durable cleanup before mutation")
    cleanup = function_body(source, "wki_remote_vfs_cleanup_for_task")
    require_order(cleanup, ["finish_or_quiesce_waiter", "SharedIoTaskOwner::cleanup_for_task(pid)",
                            "PinPreparation::cleanup_for_task(pid)", "OrdinaryCloseOwner::cleanup_for_task(pid)"],
                  "waiters and shared slots drain before File storage")
    close = function_body(source, "close_ordinary_file")
    require_order(close, ["OrdinaryCloseOwner owner(file)", "flush_write_behind(context)",
                          "cancel_remote_ordinary_open", "FLUSH_STATUS != 0 ? FLUSH_STATUS : close_status"],
                  "durable close ownership and flush errno precedence")
    if "io_lock" in close or "OP_VFS_CLOSE" in close:
        fail("ordinary close must use the exact token without a mutex spanning waits")
    finish = function_body(source, "finish_ordinary_cancellation")
    require_tokens(finish, ["status == 0 || retirement_proven", "ExecCancellationPhase::PENDING",
                            "release_vfs_proxy_open_ref(released)"], "unknown outcomes retain the independent proxy ref")
    require_tokens(source, ["VFS_OPEN_CANCEL_CAPACITY = 1024", "wki_remote_vfs_selftest_open_lifetime_client",
                            "g_shared_io_owners"], "separate bounded task-owned ordinary outbox")


def main() -> None:
    test_ordinary_open_lifetime_client_ownership()
    test_ordinary_open_lifetime_owner_uses_ordered_exact_binding()
    test_open_reply_admission_preserves_rejection_status()
    test_pin_close_continuation_is_registered_and_preserves_writable_close()
    test_directory_lane_and_executable_pin_ownership_do_not_depend_on_unwinding()
    test_grant_rpc_exit_cleanup_transfers_exact_reserved_cancellation()
    test_native_directory_grants_have_separate_close_only_staging()
    test_scalar_metadata_keeps_snapshot_and_binding_through_retries()
    test_path_chown_keeps_one_caller_and_exact_binding()
    test_remote_descriptor_metadata_maps_one_caller_and_never_replays()
    test_mapped_rename_uses_exact_directory_bindings_and_one_commit()
    test_mapped_removal_preserves_caller_and_never_replays()
    test_mapped_symlink_preserves_literal_target_and_one_identity()
    test_mapped_mkdir_queries_exact_binding_before_one_commit()
    test_vfs_host_alias_rewrite_is_overlap_safe()
    test_vfs_route_scratch_is_initialized_by_its_producer()
    test_remote_chown_reports_missing_owner_operation()
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
    test_remote_open_prepares_storage_before_mutation_request()
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
    test_remote_xattr_is_bounded_replay_safe_and_export_confined()
    test_exec_view_preserves_owner_grant_and_complete_reply()
    test_mapped_chmod_retains_snapshot_and_exact_directory_lane()
    test_mapped_open_carries_captured_export_symlink_namespace()
    print("WKI remote VFS source invariants hold")


if __name__ == "__main__":
    main()
