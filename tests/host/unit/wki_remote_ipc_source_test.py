#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_IPC_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc.cpp"
REMOTE_IPC_SOCKET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc_socket.cpp"
REMOTE_IPC_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc.hpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"


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


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_poll_wake_drains_all_batches() -> None:
    body = function_body(REMOTE_IPC_CPP.read_text(), "wki_ipc_proxy_wake_poll_waiters")
    required = [
        "while (true)",
        "proxy_collect_waiters_locked(proxy->poll_waiters, pending_waiters, &pending_waiter_count)",
        "if (pending_waiter_count == 0)",
        "proxy_reschedule_waiters(pending_waiters, pending_waiter_count)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("poll waiter wake path must drain every fixed-size batch: " + ", ".join(missing))
    require_order(body, "proxy->lock.unlock_irqrestore(IRQF);", "proxy_reschedule_waiters", "poll wake lock release")


def test_proxy_poll_wake_cancels_deferred_switch_without_losing_wake_token() -> None:
    body = function_body(REMOTE_IPC_CPP.read_text(), "proxy_reschedule_waiters")
    required = [
        "ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL)",
        "waiter->release()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("proxy poll wakes must cancel deferred parking without losing the scheduler wake token: " + ", ".join(missing))


def test_pipe_pump_read_waiter_uses_event_block_fast_path() -> None:
    body = function_body(REMOTE_IPC_CPP.read_text(), "pipe_pump_read_ready")
    required = [
        "register_poll_read_waiter(file, &ready_now)",
        "if (ready_now)",
        "ker::mod::sched::kern_block()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("pipe pump read path must block on the registered poll waiter fast path: " + ", ".join(missing))
    if "kern_sleep_us(WKI_IPC_PIPE_READ_POLL_RECHECK_US)" in body:
        fail("pipe pump read fast path must not use timer polling after registering a poll waiter")


def test_inactive_proxy_poll_reports_terminal_readiness() -> None:
    pipe_body = function_body(REMOTE_IPC_CPP.read_text(), "proxy_pipe_poll_check")
    socket_body = function_body(REMOTE_IPC_SOCKET_CPP.read_text(), "proxy_socket_poll_check")

    pipe_required = [
        "!proxy->active.load(std::memory_order_acquire)",
        "ready |= dev::pty::POLLHUP",
        "ready |= dev::pty::POLLERR",
        "return ready",
    ]
    socket_required = [
        "!proxy->active.load(std::memory_order_acquire)",
        "return 0x0008 | 0x0010",
    ]
    missing = [token for token in pipe_required if token not in pipe_body]
    missing += [token for token in socket_required if token not in socket_body]
    if missing:
        fail("inactive IPC proxy poll paths must report terminal readiness: " + ", ".join(missing))


def test_epoll_close_releases_lookup_ref_after_detach() -> None:
    body = function_body(REMOTE_IPC_CPP.read_text(), "proxy_epoll_close")
    require_order(body, "proxy = find_proxy_by_endpoint_locked(HOME_NODE, resource_id);", "wki_ipc_detach_proxy_file(f, proxy);", "epoll lookup")
    detach_pos = body.find("wki_ipc_detach_proxy_file(f, proxy);")
    if detach_pos < 0 or "proxy_release(proxy);" not in body[detach_pos:]:
        fail("epoll close must release the lookup ref after detaching the proxy file")


def test_proxy_lookup_is_peer_scoped() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    lookup_body = function_body(remote_ipc, "find_proxy_by_endpoint_locked")
    if "find_proxy_by_endpoint_locked(uint16_t home_node, uint32_t resource_id)" not in remote_ipc:
        fail("IPC proxy lookup helper must accept home_node and resource_id")
    lookup_required = [
        "proxy->home_node == home_node",
        "proxy->resource_id == resource_id",
        "proxy->refcount.fetch_add(1, std::memory_order_acq_rel)",
    ]
    missing = [token for token in lookup_required if token not in lookup_body]
    if missing:
        fail("IPC proxy lookup must be keyed by home node and resource id: " + ", ".join(missing))
    if "find_proxy_by_resource_id" in remote_ipc:
        fail("IPC proxy lookup must not use resource_id-only helpers")

    path_required = {
        "should_defer_ipc_dev_op": "find_proxy_by_endpoint_locked(src_node, resource_id)",
        "proxy_epoll_close": "find_proxy_by_endpoint_locked(HOME_NODE, resource_id)",
        "wki_ipc_epoll_ctl_forward": "find_proxy_by_endpoint_locked(HOME_NODE, epf->resource_id)",
        "wki_ipc_handle_dev_op_resp": "find_proxy_by_endpoint_locked(src_node, poll_resource_id)",
    }
    for name, token in path_required.items():
        body = function_body(remote_ipc, name)
        if token not in body:
            fail(f"{name} must use peer-scoped IPC proxy lookup: {token}")

    dev_req_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    if dev_req_body.count("find_proxy_by_endpoint_locked(hdr->src_node, resource_id)") < 2:
        fail("IPC DEV_OP request data/close paths must use source-node-scoped proxy lookup")

    resp_body = function_body(remote_ipc, "wki_ipc_socket_handle_dev_op_resp")
    response_required = [
        "p->active.load(std::memory_order_acquire)",
        "p->home_node == src_node",
        "p->resource_id == resource_id",
    ]
    missing = [token for token in response_required if token not in resp_body]
    if missing:
        fail("IPC DEV_OP response lookup must be scoped to the response source: " + ", ".join(missing))


def test_export_pipe_write_uses_nonmutating_nonblocking_view() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    view_body = function_body(remote_ipc, "init_nonblocking_pipe_write_view")
    write_body = function_body(remote_ipc, "export_pipe_write_nonblocking")

    view_required = [
        "view.open_flags = source.open_flags | WKI_IPC_O_NONBLOCK",
        "view.private_data = source.private_data",
        "view.fops = source.fops",
        "view.pos = source.pos",
        "view.stream_cache_attachment = source.stream_cache_attachment",
        "view.cache_notify_attachment = source.cache_notify_attachment",
    ]
    missing = [token for token in view_required if token not in view_body]
    if missing:
        fail("nonblocking pipe write view must preserve backend-facing file fields: " + ", ".join(missing))

    write_required = [
        "ker::vfs::File pipe_write_view{}",
        "ker::vfs::File* write_file = file",
        "init_nonblocking_pipe_write_view(pipe_write_view, *file)",
        "write_file = &pipe_write_view",
        "IS_PIPE && len <= WKI_IPC_PIPE_DATA_MAX_CHUNK",
        "BURST ? WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS : 1",
        "export_pipe_write_bounded(write_file, data, len, MAX_CALLS)",
    ]
    missing = [token for token in write_required if token not in write_body]
    if missing:
        fail("export pipe writes must use a local nonblocking file view: " + ", ".join(missing))
    if "file->open_flags =" in write_body:
        fail("export pipe writes must not mutate shared File::open_flags")

    bounded_body = function_body(remote_ipc, "export_pipe_write_bounded")
    bounded_required = [
        "call < max_calls && written < len",
        "data + written",
        "len - written",
        "clamp_io_count",
        "if (result <= 0)",
        "written += static_cast<size_t>(result)",
        "written != 0 ? static_cast<ssize_t>(written) : result",
    ]
    missing = [token for token in bounded_required if token not in bounded_body]
    if missing:
        fail("export pipe write bursts must stay bounded and preserve positive progress: " + ", ".join(missing))
    if "WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS = 3" not in remote_ipc:
        fail("export pipe write burst must stay capped at one three-write wire-frame turn")


def test_proxy_pipe_write_reuses_bounded_stack_frame() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    body = function_body(remote_ipc, "proxy_pipe_write")
    required = [
        "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> msg __attribute__((uninitialized))",
        "reinterpret_cast<DevOpReqPayload*>(msg.data())",
        "std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t))",
        "std::memcpy(msg.data() + HEADER_SIZE, src + sent, TO_SEND)",
        "msg.data(),",
        "static_cast<uint16_t>(HEADER_SIZE + TO_SEND)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("proxy pipe writes must reuse one bounded stack frame with an exact transmitted prefix: " + ", ".join(missing))
    for forbidden in ["new (std::nothrow) uint8_t[MSG_SIZE]", "delete[] msg"]:
        if forbidden in body:
            fail(f"proxy pipe writes must not allocate their reusable wire frame: {forbidden}")
    require_order(body, "req->op_id =", "wki_send(", "proxy pipe frame operation")
    require_order(body, "&proxy->resource_id", "wki_send(", "proxy pipe frame resource")
    require_order(body, "req->data_len =", "wki_send(", "proxy pipe frame length")
    require_order(body, "std::memcpy(msg.data() + HEADER_SIZE", "wki_send(", "proxy pipe frame payload")
    for assertion in [
        "WKI_IPC_PIPE_DATA_HEADER_SIZE + WKI_IPC_PIPE_DATA_MAX_CHUNK == WKI_ETH_MAX_PAYLOAD",
        "WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16",
    ]:
        if assertion not in remote_ipc:
            fail(f"proxy pipe stack frame bound is missing {assertion!r}")


def test_pipe_fd_open_flags_preserve_nonblocking_access_mode() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()

    required = [
        "constexpr int WKI_IPC_FD_ACCESS_MASK = 0x0003",
        "constexpr int WKI_IPC_O_NONBLOCK = 04000",
        "constexpr int WKI_IPC_FD_OPEN_FLAG_MASK = WKI_IPC_FD_ACCESS_MASK | WKI_IPC_O_NONBLOCK",
        "ipc_fd_access_mode",
        "ipc_fd_export_open_flags",
        "ipc_fd_import_open_flags",
    ]
    missing = [token for token in required if token not in remote_ipc]
    if missing:
        fail("IPC fd flag export/import helpers are missing: " + ", ".join(missing))

    access_body = function_body(remote_ipc, "ipc_fd_access_mode")
    export_flags_body = function_body(remote_ipc, "ipc_fd_export_open_flags")
    import_flags_body = function_body(remote_ipc, "ipc_fd_import_open_flags")
    if "open_flags & WKI_IPC_FD_ACCESS_MASK" not in access_body:
        fail("IPC fd access helper must isolate only access-mode bits")
    if "open_flags & WKI_IPC_FD_OPEN_FLAG_MASK" not in export_flags_body:
        fail("IPC fd export helper must preserve allowed open flags")
    if "flags & WKI_IPC_FD_OPEN_FLAG_MASK" not in import_flags_body:
        fail("IPC fd import helper must reject unexported flag bits")

    export_body = function_body(remote_ipc, "wki_ipc_export_task_fds")
    export_required = [
        "uint16_t const ACCESS_MODE = ipc_fd_access_mode(file->open_flags)",
        "if (res_type == ResourceType::IPC_PIPE && ACCESS_MODE != 0)",
        "bool const NEEDS_DATA_PUMP = (res_type == ResourceType::IPC_PIPE && ACCESS_MODE == 0) || res_type == ResourceType::IPC_SOCKET",
        "entry.reserved1 = ipc_fd_export_open_flags(file->open_flags)",
        "setup_export_pipe_rdma(exp, target_node, entry)",
    ]
    missing = [token for token in export_required if token not in export_body]
    if missing:
        fail("IPC export must preserve O_NONBLOCK while classifying pipe ends by access mode: " + ", ".join(missing))
    if "file->open_flags == 0" in export_body or "file->open_flags != 0" in export_body:
        fail("IPC export must not classify pipe read/write ends by raw open_flags")

    attach_body = function_body(remote_ipc, "wki_ipc_attach_task_fds")
    attach_required = [
        "int const OPEN_FLAGS = ipc_fd_import_open_flags(entry.reserved1)",
        "uint16_t const ACCESS_MODE = ipc_fd_access_mode(OPEN_FLAGS)",
        "proxy_file->open_flags = OPEN_FLAGS",
        "if (ACCESS_MODE != 0 && entry.rdma_rkey != 0 && entry.rdma_offset != 0 && WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED)",
        "if (ACCESS_MODE == 0)",
    ]
    missing = [token for token in attach_required if token not in attach_body]
    if missing:
        fail("IPC attach must restore open flags but choose pipe proxy fops from access mode: " + ", ".join(missing))
    if "entry.reserved1 & WKI_IPC_FD_ACCESS_MASK" in attach_body:
        fail("IPC attach must not strip nonblocking flags with the access-only mask")
    if "proxy_file->open_flags = 0;" in attach_body:
        fail("IPC attach must not reset non-pipe proxy fds to read-only open_flags")
    pty_start = attach_body.find("} else if (proxy->res_type == ResourceType::IPC_PTY)")
    if pty_start < 0:
        fail("IPC attach must handle PTY proxies")
    pty_end = attach_body.find("} else {", pty_start)
    if pty_end < 0:
        fail("IPC attach PTY branch must precede the fallback branch")
    pty_attach = attach_body[pty_start:pty_end]
    pty_required = [
        "proxy_file->fops = &g_proxy_pty_fops",
        "proxy_file->open_flags = (OPEN_FLAGS & ~WKI_IPC_FD_ACCESS_MASK) | WKI_IPC_FD_RDWR",
    ]
    missing = [token for token in pty_required if token not in pty_attach]
    if missing:
        fail("PTY proxy fds must be bidirectional so remote terminal stdout can write: " + ", ".join(missing))

    status_body = function_body(remote_ipc, "ipc_pipe_nonblocking_send_status")
    status_required = [
        "case WKI_ERR_NO_CREDITS",
        "case WKI_ERR_BUSY",
        "case WKI_ERR_TX_FAILED",
        "return -EAGAIN",
        "case WKI_ERR_NO_MEM",
        "return -ENOMEM",
        "return -EIO",
    ]
    missing = [token for token in status_required if token not in status_body]
    if missing:
        fail("IPC nonblocking send status must map backpressure without sleeping: " + ", ".join(missing))

    acquire_body = function_body(remote_ipc, "acquire_proxy_pipe_rdma_writer")
    if "nonblocking || current_task_has_deliverable_signal()" not in acquire_body:
        fail("RDMA writer acquisition must return immediately for nonblocking pipe writes")

    if "auto proxy_pipe_write_rdma(ProxyIpcState* proxy, const uint8_t* src, size_t count, uint64_t callsite, bool nonblocking)" not in remote_ipc:
        fail("RDMA pipe write helper must receive nonblocking mode")
    rdma_body = function_body(remote_ipc, "proxy_pipe_write_rdma")
    rdma_required = [
        "acquire_proxy_pipe_rdma_writer(proxy, nonblocking)",
        "return nonblocking ? -EAGAIN : -EINTR",
        "if (nonblocking)",
        "return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EAGAIN)",
    ]
    missing = [token for token in rdma_required if token not in rdma_body]
    if missing:
        fail("RDMA pipe writes must not sleep when the proxy fd is nonblocking: " + ", ".join(missing))
    require_order(rdma_body, "if (nonblocking)", "ker::mod::sched::kern_sleep_us(SLEEP_US)", "RDMA nonblocking full-ring check")

    write_body = function_body(remote_ipc, "proxy_pipe_write")
    write_required = [
        "bool const NONBLOCKING = (f->open_flags & WKI_IPC_O_NONBLOCK) != 0",
        "proxy_pipe_write_rdma(proxy, static_cast<const uint8_t*>(buf), count, WOS_PERF_CALLSITE(), NONBLOCKING)",
        "if (NONBLOCKING)",
        "ipc_pipe_nonblocking_send_status(ret)",
    ]
    missing = [token for token in write_required if token not in write_body]
    if missing:
        fail("message-path pipe writes must honor nonblocking fds before retry sleeps: " + ", ".join(missing))
    require_order(write_body, "if (NONBLOCKING)", "pause_for_ipc_send_retry(ret, ATTEMPT)", "message nonblocking send check")


def test_write_only_pipe_proxy_omits_receive_ring() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    header = REMOTE_IPC_HPP.read_text()

    if "bool can_receive_data = false" not in header:
        fail("IPC proxy state must distinguish intentional non-receivers from ring allocation failure")

    capability_body = function_body(remote_ipc, "ipc_proxy_can_receive_data")
    capability_required = [
        "res_type == ResourceType::IPC_SOCKET",
        "res_type == ResourceType::IPC_PTY",
        "res_type == ResourceType::IPC_PIPE && access_mode == 0",
    ]
    missing = [token for token in capability_required if token not in capability_body]
    if missing:
        fail("IPC receive-ring eligibility must preserve pipe-read, socket, and PTY receivers: " + ", ".join(missing))

    attach_body = function_body(remote_ipc, "wki_ipc_attach_task_fds")
    attach_required = [
        "int const OPEN_FLAGS = ipc_fd_import_open_flags(entry.reserved1)",
        "uint16_t const ACCESS_MODE = ipc_fd_access_mode(OPEN_FLAGS)",
        "proxy->can_receive_data = ipc_proxy_can_receive_data(RES_TYPE, ACCESS_MODE)",
        "if (proxy->can_receive_data)",
        "new uint8_t[PIPE_RING_CAPACITY]",
        "else if (!proxy->can_receive_data)",
        "take_pending_pipe_delivery_locked(entry.home_node, entry.resource_id)",
        "free_pending_pipe_delivery(dropped_pending)",
    ]
    missing = [token for token in attach_required if token not in attach_body]
    if missing:
        fail("IPC attach must allocate only usable receive rings and discard unreachable early data: " + ", ".join(missing))
    require_order(attach_body, "uint16_t const ACCESS_MODE", "auto* proxy = new ProxyIpcState", "IPC proxy access classification")
    require_order(attach_body, "take_pending_pipe_delivery_locked", "free_pending_pipe_delivery(dropped_pending)", "pending data cleanup")
    if "std::fill_n(rb, PIPE_RING_CAPACITY" in attach_body:
        fail("IPC receive rings must not zero free capacity that remains unpublished")

    queue_body = function_body(remote_ipc, "queue_pending_pipe_data")
    queue_required = [
        "s_ipc_lock.lock_irqsave()",
        "proxy_endpoint_rejects_received_data_locked(home_node, resource_id)",
        "s_ipc_lock.unlock_irqrestore(IRQF)",
        "delete[] copy",
        "find_pending_pipe_delivery_locked(home_node, resource_id)",
    ]
    missing = [token for token in queue_required if token not in queue_body]
    if missing:
        fail("pending DATA insertion must atomically reject a published nonreceiver: " + ", ".join(missing))
    require_order(queue_body, "proxy_endpoint_rejects_received_data_locked", "find_pending_pipe_delivery_locked", "pending DATA attach race")

    close_body = function_body(remote_ipc, "mark_pending_pipe_write_closed")
    require_order(
        close_body,
        "proxy_endpoint_rejects_received_data_locked(home_node, resource_id)",
        "find_pending_pipe_delivery_locked(home_node, resource_id)",
        "pending CLOSE attach race",
    )

    defer_body = function_body(remote_ipc, "should_defer_ipc_dev_op")
    defer_required = [
        "has_receiving_proxy = proxy->can_receive_data",
        "exp->consumer_node == src_node",
        "if (has_proxy && !has_receiving_proxy && !has_matching_export)",
        "*drop_out = true",
        "return !has_receiving_proxy",
    ]
    missing = [token for token in defer_required if token not in defer_body]
    if missing:
        fail("IPC DATA admission must inline receivers, defer exports/pre-attach data, and drop invalid nonreceivers: " + ", ".join(missing))

    dispatch_body = function_body(remote_ipc, "handle_ipc_dev_op_req")
    dispatch_required = [
        "bool drop = false",
        "should_defer_ipc_dev_op(req.op_id, resource_id, hdr->src_node, &drop)",
        "if (drop)",
        "handle_ipc_dev_op_req_inline(hdr, payload, payload_len, nullptr)",
    ]
    missing = [token for token in dispatch_required if token not in dispatch_body]
    if missing:
        fail("IPC DEV_OP dispatch must apply the no-allocation DATA drop decision: " + ", ".join(missing))
    require_order(dispatch_body, "if (drop)", "handle_ipc_dev_op_req_inline", "invalid IPC DATA admission")

    data_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    data_required = [
        "if (proxy != nullptr && !proxy->can_receive_data)",
        "nonreceiving_proxy = proxy",
        "proxy = nullptr",
        "find_export_by_resource_id(resource_id)",
        "bool const HAD_NONRECEIVING_PROXY = nonreceiving_proxy != nullptr",
        "proxy_release(nonreceiving_proxy)",
        "if (HAD_NONRECEIVING_PROXY)",
        "pipe_trace.finish(-EBADF)",
    ]
    missing = [token for token in data_required if token not in data_body]
    if missing:
        fail("write-only IPC DATA must fall through to colliding exports, then reject without pending allocation: " + ", ".join(missing))
    if data_body.count("exp->consumer_node == hdr->src_node") < 2:
        fail("DATA/CLOSE_WRITE collision fallback must only select exports consumed by the source peer")
    require_order(data_body, "proxy = nullptr", "find_export_by_resource_id(resource_id)", "opposite-direction resource collision")
    no_export_start = data_body.find("if (!export_exists)")
    no_export_end = data_body.find("if (OP_DATA_LEN == 0)", no_export_start)
    no_export_body = data_body[no_export_start:no_export_end]
    require_order(no_export_body, "if (HAD_NONRECEIVING_PROXY)", "queue_pending_pipe_data(hdr->src_node", "write-only pending allocation guard")

    close_write_start = data_body.find("if (OP_ID == OP_PIPE_CLOSE_WRITE)")
    close_read_start = data_body.find("if (OP_ID == OP_PIPE_CLOSE_READ)", close_write_start)
    close_write_body = data_body[close_write_start:close_read_start]
    for token in [
        "if (proxy != nullptr && !proxy->can_receive_data)",
        "nonreceiving_proxy = proxy",
        "auto* exp = find_export_by_resource_id(resource_id)",
        "if (HAD_NONRECEIVING_PROXY)",
    ]:
        if token not in close_write_body:
            fail(f"pipe CLOSE_WRITE collision handling must contain {token!r}")


def test_pty_export_data_can_write_from_deferred_worker_without_backlog() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    handler_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    required = [
        "exp->res_type == ResourceType::IPC_PTY",
        "bool const HAS_BACKLOG = find_export_pipe_write_backlog_locked(exp) != nullptr",
        "export_pty_immediate = true",
        "ssize_t const WRITE_RET = export_pipe_write_nonblocking(export_file, op_data, OP_DATA_LEN)",
        "note_export_pipe_data_received_locked(exp, OP_DATA_LEN)",
        "queue_export_pipe_write_data(resource_id, op_data + written",
    ]
    missing = [token for token in required if token not in handler_body]
    if missing:
        fail("PTY export data should use immediate deferred-worker writes before falling back to ordered backlog: " + ", ".join(missing))
    require_order(handler_body, "bool const HAS_BACKLOG", "export_pty_immediate = true", "PTY immediate write ordering")
    require_order(
        handler_body,
        "export_pipe_write_nonblocking(export_file",
        "queue_export_pipe_write_data(resource_id, op_data + written",
        "PTY fallback ordering",
    )


def test_attach_fd_install_is_transactional() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    install_body = function_body(remote_ipc, "install_ipc_proxy_file")
    attach_body = function_body(remote_ipc, "wki_ipc_attach_task_fds")

    install_required = [
        "task->fd_table_lock.lock_irqsave()",
        "task->fd_table.lookup(fd)",
        "task->fd_table.insert(fd, proxy_file)",
        "IpcProxyFdInstallResult{.inserted = INSERTED, .existing = existing}",
    ]
    missing = [token for token in install_required if token not in install_body]
    if missing:
        fail("IPC attach fd install must be a locked replace operation: " + ", ".join(missing))

    attach_required = [
        "entry.local_fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE",
        "IpcProxyFdInstallResult const INSTALL = install_ipc_proxy_file(task, entry.local_fd, proxy_file)",
        "free_uninstalled_ipc_proxy_file(proxy_file, proxy)",
        "ker::vfs::vfs_put_file(INSTALL.existing)",
        "g_ipc_proxies.push_back(proxy)",
    ]
    missing = [token for token in attach_required if token not in attach_body]
    if missing:
        fail("IPC attach must cleanly handle proxy fd install failure: " + ", ".join(missing))
    if "task->fd_table.remove(entry.local_fd)" in attach_body:
        fail("IPC attach must not remove the old fd before the proxy insert succeeds")
    if "static_cast<void>(task->fd_table.insert(entry.local_fd, proxy_file))" in attach_body:
        fail("IPC attach must not ignore proxy fd insert failure")
    require_order(attach_body, "IpcProxyFdInstallResult const INSTALL", "ker::vfs::vfs_put_file(INSTALL.existing)", "attach replacement")
    require_order(attach_body, "IpcProxyFdInstallResult const INSTALL", "g_ipc_proxies.push_back(proxy)", "attach registration")


def test_dev_op_response_cookies_fence_stale_waiters() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    remote_socket = REMOTE_IPC_SOCKET_CPP.read_text()
    header = REMOTE_IPC_HPP.read_text()

    header_required = [
        "uint16_t pending_wait_cookie = 0",
        "uint16_t next_wait_cookie = 1",
        "wki_ipc_allocate_wait_cookie_locked",
        "wki_ipc_response_matches_pending",
    ]
    missing = [token for token in header_required if token not in header]
    if missing:
        fail("proxy control waiters must carry nonzero response cookies: " + ", ".join(missing))

    response_body = function_body(remote_ipc, "wki_ipc_socket_handle_dev_op_resp")
    if "wki_ipc_response_matches_pending(resp.op_id, resp.reserved, *target_proxy)" not in response_body:
        fail("DEV_OP_RESP handler must match both op id and echoed response cookie")
    if "target_proxy->pending_wait_op = 0" in response_body or "target_proxy->pending_wait_cookie = 0" in response_body:
        fail("DEV_OP_RESP handler must keep the busy slot occupied until the waiter consumes the response")
    if "wki_ipc_consume_pending_wait_response" not in remote_ipc:
        fail("proxy control waiters must consume and clear side-band responses through the shared helper")

    server_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    server_required = [
        "ipc_op_uses_response_cookie(OP_ID)",
        "std::memcpy(&request_cookie, op_data, WKI_IPC_OP_COOKIE_BYTES)",
        "resp.reserved = response_cookie",
        "send_ipc_dev_op_error_response(hdr, op_id, resource_id, -ENOMEM, REQUEST_COOKIE)",
        "send_ipc_dev_op_error_response(hdr, op_id, resource_id, -EAGAIN, REQUEST_COOKIE)",
        "resp->reserved = request_cookie",
        "ctl_resp.reserved = request_cookie",
        "wake_resp.reserved = request_cookie",
        "pty_resp.reserved = request_cookie",
    ]
    missing = [token for token in server_required if token not in remote_ipc]
    if missing:
        fail("home-side IPC control responses must echo request cookies: " + ", ".join(missing))

    socket_body = function_body(remote_socket, "send_socket_op_sync")
    pty_body = function_body(remote_ipc, "proxy_pty_ioctl")
    epoll_body = function_body(remote_ipc, "wki_ipc_epoll_ctl_forward")
    sender_required = [
        (socket_body, "RID_SIZE + WKI_IPC_OP_COOKIE_BYTES + extra_len", "socket request size"),
        (socket_body, "proxy->pending_wait_cookie = op_cookie", "socket pending cookie"),
        (socket_body, "std::memcpy(msg + sizeof(DevOpReqPayload) + RID_SIZE, &op_cookie, WKI_IPC_OP_COOKIE_BYTES)", "socket cookie write"),
        (
            socket_body,
            "wki_ipc_consume_pending_wait_response(proxy, &wait, op_id, op_cookie",
            "socket consume keeps busy slot until response is read",
        ),
        (pty_body, "WKI_IPC_OP_COOKIE_BYTES + EXTRA", "pty request size"),
        (pty_body, "proxy->pending_wait_cookie = op_cookie", "pty pending cookie"),
        (
            pty_body,
            "wki_ipc_consume_pending_wait_response(proxy, &wait, OP_PTY_IOCTL, op_cookie",
            "pty consume keeps busy slot until response is read",
        ),
        (epoll_body, "RID_SIZE + WKI_IPC_OP_COOKIE_BYTES + EXTRA_SIZE", "epoll request size"),
        (epoll_body, "proxy->pending_wait_cookie = op_cookie", "epoll pending cookie"),
        (
            epoll_body,
            "wki_ipc_consume_pending_wait_response(proxy, &wait, OP_EPOLL_CTL, op_cookie",
            "epoll consume keeps busy slot until response is read",
        ),
    ]
    for body, token, context in sender_required:
        if token not in body:
            fail(f"{context}: missing {token!r}")


def test_peer_cleanup_drains_deferred_dev_op_work() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()

    worker_struct_required = [
        "uint64_t cleanup_epoch = 0",
        "std::array<IpcPeerCleanupEpoch, WKI_MAX_PEERS> g_ipc_peer_cleanup_epochs",
        "begin_ipc_peer_dev_op_cleanup_locked",
        "end_ipc_peer_dev_op_cleanup_locked",
        "ipc_dev_op_work_is_fenced_locked",
    ]
    missing = [token for token in worker_struct_required if token not in remote_ipc]
    if missing:
        fail("deferred IPC DEV_OP work must carry peer cleanup epoch fencing: " + ", ".join(missing))

    enqueue_body = function_body(remote_ipc, "enqueue_ipc_dev_op_work")
    if "work->cleanup_epoch = ipc_peer_cleanup_epoch_locked(hdr->src_node)" not in enqueue_body:
        fail("deferred IPC DEV_OP enqueue must snapshot the source peer cleanup epoch")

    worker_body = function_body(remote_ipc, "ipc_dev_op_worker_thread_fn")
    require_order(
        worker_body,
        "bool const FENCED = ipc_dev_op_work_is_fenced_locked(work)",
        "handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len, &work)",
        "worker stale work check",
    )
    if "free_ipc_dev_op_work(work);" not in worker_body[worker_body.find("bool const FENCED") :]:
        fail("deferred IPC DEV_OP worker must free stale work without handling it")

    batch = remote_ipc[remote_ipc.find("struct IpcPeerCleanupBatch") : remote_ipc.find("auto export_needs_peer_cleanup_locked")]
    for snippet in [
        "std::array<IpcDevOpWork*, WKI_IPC_DEV_OP_CLEANUP_BATCH> detached_dev_ops",
        "size_t detached_dev_op_count = 0",
    ]:
        if snippet not in batch:
            fail(f"peer cleanup batch must carry detached DEV_OP work: {snippet}")

    collect_body = function_body(remote_ipc, "collect_ipc_peer_cleanup_batch_locked")
    for snippet in [
        "for (auto& queue : g_ipc_dev_op_queues)",
        "work->hdr.src_node != node_id",
        "batch.detached_dev_ops.at(batch.detached_dev_op_count++) = work",
        "it = queue.erase(it)",
    ]:
        if snippet not in collect_body:
            fail(f"peer cleanup must drain queued deferred DEV_OP work for the fenced peer: {snippet}")

    drain_body = function_body(remote_ipc, "drain_ipc_peer_cleanup_batch")
    if "std::span(batch.detached_dev_ops.data(), batch.detached_dev_op_count)" not in drain_body:
        fail("peer cleanup drain must iterate detached DEV_OP work")
    if "free_ipc_dev_op_work(work)" not in drain_body:
        fail("peer cleanup drain must free detached DEV_OP work")

    cleanup_body = function_body(remote_ipc, "wki_ipc_cleanup_for_peer")
    require_order(
        cleanup_body,
        "begin_ipc_peer_dev_op_cleanup_locked(node_id)",
        "collect_ipc_peer_cleanup_batch_locked(node_id, batch)",
        "cleanup begin epoch",
    )
    require_order(
        cleanup_body,
        "collect_ipc_peer_cleanup_batch_locked(node_id, batch)",
        "end_ipc_peer_dev_op_cleanup_locked(node_id)",
        "cleanup end epoch",
    )


def test_large_deferred_dev_op_payloads_are_coallocated() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    for snippet in [
        "bool payload_coallocated = false",
        "static_assert(sizeof(IpcDevOpWork) == 56)",
        "WKI_IPC_DEV_OP_COALLOC_MIN_PAYLOAD = 8192",
        "payload_len <= WKI_ETH_MAX_PAYLOAD",
    ]:
        if snippet not in remote_ipc:
            fail(f"large deferred IPC DEV_OP storage policy is missing {snippet!r}")

    alloc_body = function_body(remote_ipc, "alloc_ipc_dev_op_work")
    for snippet in [
        "payload_len >= WKI_IPC_DEV_OP_COALLOC_MIN_PAYLOAD",
        "sizeof(IpcDevOpWork) + (COALLOCATE_PAYLOAD ? payload_len : 0)",
        "::operator new(STORAGE_SIZE, std::nothrow)",
        "new (STORAGE) IpcDevOpWork{}",
        "work->payload = reinterpret_cast<uint8_t*>(work + 1)",
        "new (std::nothrow) uint8_t[payload_len]",
    ]:
        if snippet not in alloc_body:
            fail(f"deferred IPC DEV_OP allocator is missing {snippet!r}")

    release_body = function_body(remote_ipc, "free_ipc_dev_op_work")
    for snippet in [
        "if (!work->payload_coallocated)",
        "delete[] work->payload",
        "work->~IpcDevOpWork()",
        "::operator delete(work)",
    ]:
        if snippet not in release_body:
            fail(f"deferred IPC DEV_OP release is missing {snippet!r}")

    enqueue_body = function_body(remote_ipc, "enqueue_ipc_dev_op_work")
    if "alloc_ipc_dev_op_work(payload_len)" not in enqueue_body:
        fail("deferred IPC DEV_OP enqueue must use the shared storage allocator")
    if "new (std::nothrow) IpcDevOpWork" in enqueue_body or "new (std::nothrow) uint8_t[payload_len]" in enqueue_body:
        fail("deferred IPC DEV_OP enqueue must not allocate the descriptor and large payload separately")


def test_large_deferred_dev_op_payloads_transfer_to_export_backlog() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    for snippet in [
        "IpcDevOpWork* dev_op_owner = nullptr",
        "static_assert(sizeof(PendingPipeChunk) == 24)",
        "WKI_IPC_DEV_OP_TRANSFER_MIN_DATA",
        "len < WKI_IPC_DEV_OP_TRANSFER_MIN_DATA",
        "len <= work->payload_len - OFFSET",
        "queue_export_pipe_write_data(uint32_t resource_id, const uint8_t* data, uint16_t len, IpcDevOpWork** work_owner)",
    ]:
        if snippet not in remote_ipc:
            fail(f"large deferred IPC backlog ownership is missing {snippet!r}")

    release_body = function_body(remote_ipc, "release_pending_pipe_chunk")
    for snippet in [
        "auto* const OWNER = chunk.dev_op_owner",
        "const auto* const DATA = chunk.data",
        "chunk = {}",
        "free_ipc_dev_op_work(OWNER)",
        "delete[] DATA",
    ]:
        if snippet not in release_body:
            fail(f"pipe chunk release must handle both backing kinds: {snippet!r}")
    if "delete[] chunk.data" in remote_ipc:
        fail("pipe chunks must be released through the ownership-aware helper")
    for name in [
        "free_pending_pipe_delivery",
        "free_export_pipe_write_backlog",
        "export_pipe_write_flush_thread_loop",
        "drain_pending_pipe_data",
    ]:
        body = function_body(remote_ipc, name)
        if "release_pending_pipe_chunk(chunk)" not in body:
            fail(f"{name} must release chunks through the ownership-aware helper")

    publish_body = function_body(remote_ipc, "publish_export_pipe_write_chunk_locked")
    for snippet in [
        ".dev_op_owner = transfer_owner",
        "*work_owner = nullptr",
    ]:
        if snippet not in publish_body:
            fail(f"export backlog owner publication is missing {snippet!r}")
    require_order(publish_body, "backlog->chunks.push_back", "*work_owner = nullptr", "export backlog owner publication")

    queue_body = function_body(remote_ipc, "queue_export_pipe_write_data")
    for snippet in [
        "ipc_dev_op_work_can_back_pipe_chunk(work_owner != nullptr ? *work_owner : nullptr, data, len)",
        "if (TRANSFER_OWNER == nullptr)",
        "new (std::nothrow) uint8_t[len]",
        "publish_export_pipe_write_chunk_locked(backlog",
    ]:
        if snippet not in queue_body:
            fail(f"export backlog ownership transfer is missing {snippet!r}")
    publish_pos = queue_body.find("publish_export_pipe_write_chunk_locked(backlog")
    unlock_pos = queue_body.find("s_ipc_lock.unlock_irqrestore(IRQF)", publish_pos)
    if publish_pos < 0 or unlock_pos < publish_pos:
        fail("export backlog owner publication must complete before releasing s_ipc_lock")

    worker_body = function_body(remote_ipc, "ipc_dev_op_worker_thread_fn")
    worker_call = "handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len, &work)"
    worker_call_pos = worker_body.find(worker_call)
    if worker_call_pos < 0:
        fail("deferred IPC worker must offer its work item to the inline handler")
    if "work->" in worker_body[worker_call_pos + len(worker_call) :]:
        fail("deferred IPC worker must not dereference work after ownership can transfer")
    dispatch_body = function_body(remote_ipc, "handle_ipc_dev_op_req")
    if "handle_ipc_dev_op_req_inline(hdr, payload, payload_len, nullptr)" not in dispatch_body:
        fail("immediate IPC RX dispatch must not transfer non-owned payload storage")
    handler_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    queue_marker = "queue_export_pipe_write_data("
    queue_positions = []
    search_from = 0
    while True:
        position = handler_body.find(queue_marker, search_from)
        if position < 0:
            break
        queue_positions.append(position)
        search_from = position + len(queue_marker)
    if len(queue_positions) != 2:
        fail("both PTY-tail and normal export backlog paths must offer deferred work ownership")
    for position in queue_positions:
        line_end = handler_body.find("\n", position)
        return_pos = handler_body.find("return;", line_end)
        if line_end < 0 or return_pos < 0:
            fail("export backlog queue calls must return on their current control-flow branch")
        suffix = handler_body[line_end:return_pos]
        forbidden = [token for token in ["hdr->", "payload", "op_data"] if token in suffix]
        if forbidden:
            fail("handler dereferences transferred storage after queue publication: " + ", ".join(forbidden))


def test_futex_dev_op_preserves_broadcast_wake_count() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    for snippet in [
        "OP_ID == OP_FUTEX_WAKE",
        "op_data layout: [phys_addr:u64]",
        "futex_wake_by_phys(phys_addr, INT32_MAX)",
    ]:
        if snippet not in remote_ipc:
            fail(f"remote IPC futex wake bridge is missing broadcast-count snippet: {snippet}")
    if "futex_wake_by_phys(phys_addr));" in remote_ipc:
        fail("remote IPC futex wake bridge must pass an explicit wake count")


def test_ipc_send_retry_backpressure_sleeps_without_yield_livelock() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    retry_body = function_body(remote_ipc, "pause_for_ipc_send_retry")
    if "kern_yield" in retry_body:
        fail("IPC send retry must sleep on no-credit backpressure instead of keeping senders runnable")
    for snippet in [
        "ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(ret, attempt))",
        "uint64_t const BASE_US = (ret == WKI_ERR_NO_CREDITS) ? 1000 : 2000",
    ]:
        if snippet not in remote_ipc:
            fail(f"IPC send retry backoff is missing {snippet}")


def test_ipc_selftests_are_declared_and_registered() -> None:
    header = REMOTE_IPC_HPP.read_text()
    ktest = WKI_WAIT_KTEST.read_text()
    required = [
        "wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops",
        "wki_ipc_selftest_large_dev_op_work_coallocates_payload",
        "wki_ipc_selftest_large_dev_op_work_backs_pipe_chunk",
        "wki_ipc_selftest_poll_wake_drains_over_capacity",
        "wki_ipc_selftest_inactive_proxy_poll_is_terminal",
        "wki_ipc_selftest_epoll_close_releases_lookup_ref",
        "wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags",
        "wki_ipc_selftest_export_pipe_write_burst_is_bounded",
        "wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode",
        "wki_ipc_selftest_write_only_pipe_omits_receive_ring",
        "wki_ipc_selftest_attach_insert_failure_preserves_existing_fd",
        "wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion",
        "wki_ipc_selftest_dev_op_response_uses_home_node_identity",
    ]
    for token in required:
        if token not in header:
            fail(f"missing selftest declaration {token}")
        if token not in ktest:
            fail(f"missing KTEST coverage for {token}")


def main() -> None:
    test_poll_wake_drains_all_batches()
    test_proxy_poll_wake_cancels_deferred_switch_without_losing_wake_token()
    test_pipe_pump_read_waiter_uses_event_block_fast_path()
    test_inactive_proxy_poll_reports_terminal_readiness()
    test_epoll_close_releases_lookup_ref_after_detach()
    test_proxy_lookup_is_peer_scoped()
    test_export_pipe_write_uses_nonmutating_nonblocking_view()
    test_proxy_pipe_write_reuses_bounded_stack_frame()
    test_pipe_fd_open_flags_preserve_nonblocking_access_mode()
    test_write_only_pipe_proxy_omits_receive_ring()
    test_pty_export_data_can_write_from_deferred_worker_without_backlog()
    test_attach_fd_install_is_transactional()
    test_dev_op_response_cookies_fence_stale_waiters()
    test_peer_cleanup_drains_deferred_dev_op_work()
    test_large_deferred_dev_op_payloads_are_coallocated()
    test_large_deferred_dev_op_payloads_transfer_to_export_backlog()
    test_futex_dev_op_preserves_broadcast_wake_count()
    test_ipc_send_retry_backpressure_sleeps_without_yield_livelock()
    test_ipc_selftests_are_declared_and_registered()
    print("WKI remote IPC source invariants hold")


if __name__ == "__main__":
    main()
