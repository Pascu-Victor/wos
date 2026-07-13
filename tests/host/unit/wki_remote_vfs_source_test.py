#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_VFS_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.cpp"
REMOTE_VFS_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.hpp"
WKI_DEV_PROXY_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_dev_proxy_ktest.cpp"


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


def require_proxy_slot_helper(name: str, body: str, claims_untracked_slot: bool) -> None:
    require_tokens(
        body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, VFS_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "while (true)",
            "state->lock.lock()",
            "if (!proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "if (!proxy_op_slot_busy(state))",
            "continue",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ker::mod::sched::kern_sleep_us(VFS_PROXY_CONTENTION_SLEEP_US)",
        ],
        name,
    )
    if claims_untracked_slot and "state->op_untracked_send_pending.store(true, std::memory_order_release)" not in body:
        fail(f"{name} must claim the untracked-send slot before returning success")
    if not claims_untracked_slot and "op_untracked_send_pending.store(true" in body:
        fail(f"{name} must not mark untracked sends pending")
    require_order(
        body,
        [
            "state->lock.lock()",
            "if (!proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
        ],
        name,
    )


def test_proxy_op_slot_waits_are_bounded() -> None:
    source = REMOTE_VFS_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "constexpr uint64_t VFS_PROXY_SLOT_WAIT_TIMEOUT_US = VFS_PROXY_OP_TIMEOUT_US;",
            "auto acquire_proxy_op_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int",
            "auto acquire_proxy_untracked_send_slot_locked(ProxyVfsState* state, uint64_t start_us) -> int",
        ],
        "remote VFS proxy slot timeout scaffolding",
    )
    require_proxy_slot_helper("acquire_proxy_op_slot_locked", function_body(source, "acquire_proxy_op_slot_locked"), False)
    require_proxy_slot_helper(
        "acquire_proxy_untracked_send_slot_locked",
        function_body(source, "acquire_proxy_untracked_send_slot_locked"),
        True,
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
            "delete[] req_buf",
            "return encode_proxy_wki_status(SLOT_RET)",
            "peek_channel_tx_seq16",
            "state->op_wait_entry = &wait",
        ],
        "vfs_proxy_send_and_wait slot timeout",
    )
    require_order(
        untracked_body,
        [
            "int const SLOT_RET = acquire_proxy_untracked_send_slot_locked(state, PROXY_WAIT_START)",
            "if (SLOT_RET != WKI_OK)",
            "delete[] req_buf",
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
            "state->op_wait_entry = &wait",
        ],
        "vfs_proxy_write_rdma_and_wait slot timeout",
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
            "ker::mod::sched::kern_sleep_us(VFS_PROXY_CONTENTION_SLEEP_US)",
        ],
        "shared RDMA I/O slot acquisition",
    )
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
    read_body = function_body(source, "remote_vfs_read")

    require_order(
        read_body,
        [
            "bool const SHOULD_READ_AHEAD = ALLOW_READ_CACHES && !POSITIONAL_READ && remaining < VFS_CACHE_SIZE",
            "if (SHOULD_READ_AHEAD && ctx->read_cache == nullptr)",
            "ctx->read_cache = new (std::nothrow) ReadAheadCache()",
            "bool const USING_CACHE = SHOULD_READ_AHEAD && ctx->read_cache != nullptr",
            "auto fetch_size = USING_CACHE ? static_cast<uint32_t>(VFS_CACHE_SIZE) : std::min(remaining, VFS_DIRECT_READ_STACK_SIZE)",
            "uint8_t* fetch_dest = direct_read_buf.data()",
            "if (USING_CACHE)",
            "fetch_dest = ctx->read_cache->data.data()",
            "ctx->read_cache->cached_offset = cur_offset",
            "ctx->read_cache->cached_len = BYTES_READ",
            "auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(BYTES_READ), remaining))",
            "remaining -= to_copy",
        ],
        "message fallback small-read read-ahead",
    )
    if "remaining >= VFS_CACHE_SIZE" in read_body:
        fail("message fallback must not reserve read-ahead for already-full cache-sized reads")
    if "return (total_read > 0) ? total_read : -ENOMEM" in read_body:
        fail("optional message read-ahead allocation failure must use the direct stack path")


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


def test_message_write_flush_retains_tail_on_request_allocation_failure() -> None:
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
            "auto req_data_len = static_cast<uint16_t>(12 + CHUNK)",
            "vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len",
        ],
        "RDMA and message write-behind chunk bounds",
    )
    require_order(
        flush_body,
        [
            "auto* req_data = new (std::nothrow) uint8_t[req_data_len]",
            "if (req_data == nullptr)",
            "keep_pending_tail(src, remaining, cur_offset)",
            "return -ENOMEM",
        ],
        "message-mode write-behind allocation failure",
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


def test_normal_remote_close_flushes_then_sends_without_response_wait() -> None:
    source = REMOTE_VFS_CPP.read_text()
    close_body = function_body(source, "remote_vfs_close")

    require_order(
        close_body,
        [
            "ker::mod::sys::MutexGuard io_guard(ctx->io_lock)",
            "flush_status = flush_write_behind(ctx)",
            "int32_t remote_fd = ctx->remote_fd",
            "vfs_proxy_send_untracked(ctx->proxy, OP_VFS_CLOSE",
            "delete ctx->write_buf",
            "delete ctx;",
            "release_vfs_proxy_open_ref(PROXY)",
            "return flush_status",
        ],
        "normal remote close ordering",
    )
    if close_body.count("ker::mod::sys::MutexGuard io_guard(ctx->io_lock)") != 2:
        fail("both inactive and normal remote close cleanup must serialize per-file storage")
    if "vfs_proxy_send_and_wait" in close_body:
        fail("normal remote close must not wait for the owner close response")
    if "NEEDS_CLOSE_STATUS" in close_body:
        fail("normal remote close must not branch on access mode for response waiting")


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
            "if (exp.active && exp.resource_id == resource_id)",
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
            "auto allocate_vfs_attach_cookie_locked() -> uint8_t",
            "auto vfs_attach_ack_matches_pending_locked(ProxyVfsState const* state, const DevAttachAckPayload& ack) -> bool",
            "state->attach_expected_cookie != 0",
            "wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)",
            "attach_req.attach_cookie = attach_cookie",
        ],
        "remote VFS attach-cookie scaffolding",
    )
    require_order(
        function_body(source, "wki_remote_vfs_mount"),
        [
            "attach_cookie = allocate_vfs_attach_cookie_locked()",
            "state->attach_expected_cookie = attach_cookie",
            "attach_req.attach_cookie = attach_cookie",
            "wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        ],
        "remote VFS attach arms cookie before send",
    )
    require_order(
        function_body(source, "handle_vfs_attach_ack"),
        [
            "if (!vfs_attach_ack_matches_pending_locked(state, *ack))",
            "wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry)",
            "state->attach_status = ack->status",
            "state->attach_expected_cookie = 0",
            "state->attach_pending.store(false, std::memory_order_release)",
        ],
        "remote VFS attach ACK validates cookie before completion",
    )
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
            "state->lock.lock()",
            "if (state->op_pending.load(std::memory_order_acquire))",
            "teardown.op_wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry)",
            "clear_proxy_op_state_locked(state, -1)",
            "if (state->attach_pending.load(std::memory_order_acquire))",
            "teardown.attach_wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry)",
            "clear_proxy_attach_state_locked(state, static_cast<uint8_t>(DevAttachStatus::BUSY))",
            "state->active = false",
            "state->destroy_when_idle = true",
            "state->lock.unlock()",
        ],
        "remote VFS proxy deactivation",
    )

    unmount_body = function_body(source, "wki_remote_vfs_unmount")
    require_order(
        unmount_body,
        [
            "deactivate_vfs_proxy_locked(state, teardown, true)",
            "invalidate_all_dir_caches(state)",
            "s_vfs_lock.unlock()",
            "finish_claimed_waiter(teardown.op_wait_entry, -1)",
            "finish_claimed_waiter(teardown.attach_wait_entry, -1)",
            "wki_send(teardown.owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH",
            "wki_channel_close(ch)",
            "ker::vfs::unmount_filesystem(local_mount_path)",
            "mark_vfs_proxy_mount_released_and_maybe_destroy(teardown.state)",
        ],
        "remote VFS unmount teardown order",
    )

    cleanup_body = function_body(source, "wki_remote_vfs_cleanup_for_peer")
    require_order(
        cleanup_body,
        [
            "deactivate_vfs_proxy_locked(p, cleanup, false)",
            "invalidate_all_dir_caches(p)",
            "finish_claimed_waiter(cleanup.op_wait_entry, -1)",
            "finish_claimed_waiter(cleanup.attach_wait_entry, -1)",
            "wki_channel_close(ch)",
            "release_and_maybe_destroy_idle_vfs_proxy(cleanup.state)",
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
            "return flush_status;",
        ],
        "remote VFS close releases proxy open ref",
    )


def main() -> None:
    test_proxy_op_slot_waits_are_bounded()
    test_proxy_operations_fail_before_setup_when_slot_wait_times_out()
    test_shared_io_slot_waits_are_bounded()
    test_shared_io_callers_timeout_or_fallback()
    test_message_fallback_readahead_targets_small_sequential_reads()
    test_write_behind_storage_grows_in_allocator_shaped_classes()
    test_message_write_flush_retains_tail_on_request_allocation_failure()
    test_remote_open_closes_server_fd_on_local_allocation_failure()
    test_normal_remote_close_flushes_then_sends_without_response_wait()
    test_export_lookup_returns_locked_snapshot()
    test_rdma_retry_cooldowns_are_saturating()
    test_vfs_attach_ack_requires_expected_cookie_before_completion()
    test_remote_vfs_unmount_cancels_waiters_before_teardown()
    test_remote_vfs_teardown_releases_rdma_state_when_idle()
    test_remote_open_refs_delay_proxy_destroy_until_close()
    print("WKI remote VFS source invariants hold")


if __name__ == "__main__":
    main()
