#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_VFS_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.cpp"
REMOTE_VFS_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
DEV_SERVER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
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
            "bool const SYSCALL_PARK_SAFE",
            "task->syscall_account_start_us != 0",
            "ker::mod::sched::interrupts_enabled()",
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
        ],
        "WKI progress-driving wait API",
    )
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
        require_order(
            body,
            [
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
            ],
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
        function_body(source, "wki_remote_vfs_mount"),
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
    mount_body = function_body(source, "wki_remote_vfs_mount")
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
        ["p->active || p->epoch_reset_pending", "p->mount_configured"],
        "path unmount lookup retains epoch-marked mount ownership",
    )

    claim_body = function_body(source, "claim_vfs_proxy_unmount_by_path")
    require_order(
        claim_body,
        [
            "deactivate_vfs_proxy_locked(state, teardown, true)",
            "stage_vfs_detach_locked(state, teardown.owner_node, teardown.resource_id",
            "invalidate_all_dir_caches(state)",
            "s_vfs_lock.unlock()",
        ],
        "remote VFS unmount claim",
    )

    unmount_body = function_body(source, "finish_vfs_proxy_unmount")
    require_order(
        unmount_body,
        [
            "finish_proxy_teardown_op_waiter(teardown, -1)",
            "vfs_stream_cache_invalidate_remote_scope(teardown.state)",
            "wake_proxy_slot_waiters(teardown)",
            "finish_claimed_waiter(teardown.attach_wait_entry, -1)",
            "teardown.detach_staged",
            "wki_deferred_work_notify()",
            "wki_channel_close_generation(teardown.assigned_channel_ref",
            "ker::vfs::unmount_filesystem_by_private_data",
            "mark_vfs_proxy_mount_released_and_maybe_destroy(teardown.state)",
            "release_vfs_proxy_lifecycle_ref(teardown.state)",
        ],
        "remote VFS unmount teardown order",
    )

    generation_claim = function_body(source, "claim_vfs_proxy_unmount_by_generation")
    require_tokens(
        generation_claim,
        [
            "proxy->mount_configured",
            "!proxy->destroy_when_idle",
            "!proxy->mount_released",
            "proxy->resource_generation == resource_generation",
        ],
        "generation-bound remote VFS teardown selection",
    )
    require_order(
        generation_claim,
        [
            "deactivate_vfs_proxy_locked(state, teardown, true)",
            "stage_vfs_detach_locked(state, teardown.owner_node, teardown.resource_id",
            "s_vfs_lock.unlock()",
        ],
        "generation-bound remote VFS unmount reserves before registry unlock",
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
    mount_body = function_body(source, "wki_remote_vfs_mount")
    require_order(
        mount_body,
        [
            "state->resource_generation = RESOURCE_GENERATION",
            "state->mount_configured = true",
            "wki_peer_lifecycle_acquire(final_peer)",
            "wki_resource_observation_is_live",
            "wki_remote_vfs_unmount_resource_generation",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "mount publication must stay pinned through exact resource-generation validation and rollback",
    )
    if mount_body.find("release_vfs_proxy_lifecycle_ref(state)") < mount_body.find("wki_peer_lifecycle_acquire(final_peer)"):
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
            "return flush_status;",
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

    mount_body = function_body(source, "wki_remote_vfs_mount")
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
    require_order(
        mount_body,
        [
            "s_vfs_lock.lock()",
            "vfs_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)",
            "return -EAGAIN",
            "g_vfs_proxies.push_back",
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
            "state->lifecycle_refs++",
            "s_vfs_lock.unlock()",
            "release_vfs_proxy_lifecycle_ref(state)",
        ],
        "invalidate notification pins proxy lifetime across unlocked VFS work",
    )

    require_tokens(
        dev_server,
        [
            "wki_remote_vfs_cleanup_server_fds_for_channel(channel_identity)",
            "wki_remote_vfs_mark_server_fds_for_channel(item.channel_identity)",
        ],
        "ordinary and reconciliation binding teardown close exact server FDs",
    )


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


def main() -> None:
    test_proxy_op_slot_waits_are_bounded()
    test_proxy_operations_fail_before_setup_when_slot_wait_times_out()
    test_proxy_request_envelopes_use_stack_storage()
    test_proxy_slot_release_paths_handoff_after_unlock()
    test_shared_io_slot_waits_are_bounded()
    test_shared_io_callers_timeout_or_fallback()
    test_message_fallback_readahead_targets_small_sequential_reads()
    test_server_open_reuses_the_open_file_stat_snapshot()
    test_write_behind_storage_grows_in_allocator_shaped_classes()
    test_message_write_flush_retains_tail_on_request_allocation_failure()
    test_remote_open_closes_server_fd_on_local_allocation_failure()
    test_normal_remote_close_flushes_then_sends_without_response_wait()
    test_export_lookup_returns_locked_snapshot()
    test_rdma_retry_cooldowns_are_saturating()
    test_vfs_attach_ack_requires_expected_cookie_before_completion()
    test_vfs_detach_uses_exact_negotiated_incarnation_form()
    test_remote_vfs_unmount_cancels_waiters_before_teardown()
    test_remote_vfs_teardown_releases_rdma_state_when_idle()
    test_remote_open_refs_delay_proxy_destroy_until_close()
    test_remote_vfs_channel_identity_survives_pool_slot_reuse()
    test_server_bounded_metadata_responses_use_stack_storage()
    test_stale_fd_gc_drains_binding_users_before_file_close()
    test_server_fd_and_consumer_rx_use_exact_channel_identity()
    test_export_rebuild_is_revisioned_and_backing_mount_exact()
    print("WKI remote VFS source invariants hold")


if __name__ == "__main__":
    main()
