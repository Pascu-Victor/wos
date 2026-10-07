#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_COMPUTE_CPP = ROOT / "modules/kern/src/net/wki/remote_compute.cpp"
REMOTE_IPC_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc.cpp"
REMOTE_IPC_SOCKET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc_socket.cpp"
REMOTE_IPC_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_ipc.hpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"
WKI_WIRE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wire.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"


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


def test_pipe_pump_reuses_worker_stack_frame() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    body = function_body(remote_ipc, "pipe_pump_thread_fn")
    required = [
        "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> msg __attribute__((uninitialized))",
        "file->fops->vfs_read(file, msg.data() + HEADER_SIZE, BUF_SIZE, 0)",
        "reinterpret_cast<DevOpReqPayload*>(msg.data())",
        "std::memcpy(msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t))",
        "wki_send_on_channel_identity(data_channel, MsgType::DEV_OP_REQ, msg.data()",
        "static_cast<uint16_t>(HEADER_SIZE + n)",
        "static_cast<uint16_t>(HEADER_SIZE)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("pipe pump must reuse one worker-private stack frame: " + ", ".join(missing))
    require_order(body, required[0], "for (;;)", "pipe pump frame lifetime")
    if body.count("wki_send_on_channel_identity(data_channel, MsgType::DEV_OP_REQ, msg.data()") != 2:
        fail("pipe pump must send exact DATA and EOF prefixes from its stack frame")
    for forbidden in ["new (std::nothrow) uint8_t[HEADER_SIZE + BUF_SIZE]", "delete[] msg", "IPC pump: malloc failed"]:
        if forbidden in body:
            fail(f"pipe pump must not allocate its reusable wire frame: {forbidden}")
    for assertion in [
        "WKI_IPC_PIPE_DATA_HEADER_SIZE + WKI_IPC_PIPE_DATA_MAX_CHUNK == WKI_ETH_MAX_PAYLOAD",
        "WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16",
    ]:
        if assertion not in remote_ipc:
            fail(f"pipe pump stack frame bound is missing {assertion!r}")


def test_stopped_pipe_pump_publishes_discard_fence() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    collect = function_body(remote_ipc, "collect_pipe_pump_retirement_locked")
    finish = function_body(remote_ipc, "finish_pipe_pump_retirement")
    pump = function_body(remote_ipc, "pipe_pump_thread_fn")

    for token in [
        "retirement.target = exp->consumer_node",
        "retirement.resource_id = exp->resource_id",
        "retirement.discard_pending = !exp->active",
        "release_pipe_pump_slot_locked(pump_arg, exp)",
        "compact_inactive_exports_locked()",
    ]:
        if token not in collect:
            fail(f"stopped pump retirement is missing {token!r}")
    require_order(collect, "retirement.discard_pending = !exp->active", "release_pipe_pump_slot_locked", "pump discard fence")
    for token in [
        "free_retired_exports(retirement.retired_exports)",
        "if (retirement.discard_pending)",
        "schedule_pending_pipe_discard(retirement.data_channel, retirement.resource_id)",
    ]:
        if token not in finish:
            fail(f"stopped pump finisher is missing {token!r}")
    if pump.count("collect_pipe_pump_retirement_locked(*arg, exp)") != 2 or pump.count("finish_pipe_pump_retirement(RETIREMENT)") != 3:
        fail("both pre-read and post-read pump exits must publish the same retirement fence")
    shutdown_collect = "collect_pipe_pump_retirement_locked(*arg, arg->exp.load(std::memory_order_acquire))"
    require_order(pump, "std::exchange(arg->worker, nullptr)", shutdown_collect, "shutdown pump publication removal")
    shutdown_prefix = pump[:pump.index("auto* exp = arg->exp.load")]
    require_order(shutdown_prefix, shutdown_collect, "s_ipc_lock.unlock_irqrestore(IRQF)", "shutdown retirement unlock")
    require_order(shutdown_prefix, "finish_pipe_pump_retirement(RETIREMENT)", "arg->stopped.store(true", "shutdown fence before stopped")

    flush = function_body(remote_ipc, "export_pipe_write_flush_thread_loop")
    for token in [
        "bool const DIRECT_DISCARD = pump_task == nullptr",
        "schedule_pending_pipe_discard(DATA_CHANNEL, resource_id)",
    ]:
        if token not in flush:
            fail(f"no-pump close retirement is missing {token!r}")
    require_order(flush, "s_ipc_lock.unlock_irqrestore(irqf)", "schedule_pending_pipe_discard(DATA_CHANNEL, resource_id)", "close discard fence")


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
    if "find_proxy_by_endpoint_locked(uint16_t home_node, uint32_t resource_id, bool receipt = false)" not in remote_ipc:
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
        "bool const IS_PROXY_PIPE = is_proxy_pipe_file(file)",
        "if (IS_PIPE || IS_PROXY_PIPE || ker::vfs::vfs_is_socket_file(file))",
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
        "std::memcpy(msg.data() + HEADER_SIZE, src + sent, to_send)",
        "msg.data(),",
        "static_cast<uint16_t>(HEADER_SIZE + to_send)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("proxy pipe writes must reuse one bounded stack frame with an exact transmitted prefix: " + ", ".join(missing))
    for forbidden in ["new (std::nothrow) uint8_t[MSG_SIZE]", "delete[] msg"]:
        if forbidden in body:
            fail(f"proxy pipe writes must not allocate their reusable wire frame: {forbidden}")
    require_order(body, "req->op_id =", "wki_send_accounted(", "proxy pipe frame operation")
    require_order(body, "&proxy->resource_id", "wki_send_accounted(", "proxy pipe frame resource")
    require_order(body, "req->data_len =", "wki_send_accounted(", "proxy pipe frame length")
    require_order(body, "std::memcpy(msg.data() + HEADER_SIZE", "wki_send_accounted(", "proxy pipe frame payload")
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
        "exp->pump_pending_accept = plan.pump_on_accept[fd_key]",
        "entry.reserved1 = ipc_fd_export_open_flags(file->open_flags)",
        "prepare_export_pipe_rdma(*exp, snapshot.prepared_backlogs[count], file)",
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
        "if (ACCESS_MODE != 0 && entry.rdma_rkey != 0 && entry.rdma_offset == WKI_IPC_PIPE_RDMA_CAPACITY)",
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
    rdma_required = ["wki_secure_rdma_remote_grant(proxy->data_channel, rdma_protocol::Service::IPC",
                     "wki_secure_rdma_read(proxy->data_channel, grant", "wki_secure_rdma_write(proxy->data_channel, grant",
                     "WkiSecureRdmaOutcome::NO_EFFECT", "WkiSecureRdmaOutcome::EXACT",
                     "pipe_secure_uncertain.store(true", "proxy->bytes_delivered.fetch_add(RESULT.bytes",
                     "if (nonblocking || current_task_has_deliverable_signal()) break"]
    missing = [token for token in rdma_required if token not in rdma_body]
    if missing: fail("secure IPC native receipt/authority missing: " + ", ".join(missing))
    if "kern_sleep" in rdma_body or "->rdma_" in rdma_body:
        fail("secure IPC frontend must not restore raw callbacks or full-ring sleeps")
    require_order(rdma_body, "wki_secure_rdma_write(", "bytes_delivered.fetch_add", "delivery only after joined native RESULT")

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


def test_pipe_affinity_ignores_write_only_shared_output_endpoints() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    helper = function_body(remote_ipc, "ipc_pipe_endpoint_requires_affinity")
    affinity = function_body(remote_ipc, "wki_ipc_find_pipe_affinity_node")

    if "ipc_fd_access_mode(open_flags) == 0" not in helper:
        fail("pipe affinity must be limited to readable pipe endpoints")
    if "!ipc_pipe_endpoint_requires_affinity(file->open_flags)" not in affinity:
        fail("pipe affinity selection must ignore shared write-only stdout/stderr endpoints")


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
        "new (std::nothrow) uint8_t[PIPE_RING_CAPACITY]",
        "else if (!proxy->can_receive_data)",
        "take_pending_pipe_delivery_locked(entry.home_node, entry.resource_id)",
        "free_pending_pipe_delivery(dropped_pending)",
    ]
    missing = [token for token in attach_required if token not in attach_body]
    if missing:
        fail("IPC attach must allocate only usable receive rings and discard unreachable early data: " + ", ".join(missing))
    require_order(attach_body, "uint16_t const ACCESS_MODE", "auto* proxy = new (std::nothrow) ProxyIpcState", "IPC proxy access classification")
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
        "bool const FLOW_CONTROLLED = op_id == OP_PIPE_DATA_FLOW",
        "has_receiving_proxy = proxy->can_receive_data",
        "exp->consumer_node == src_node",
        "if (!FLOW_CONTROLLED && has_proxy && !has_receiving_proxy && !has_matching_export)",
        "*drop_out = true",
        "return FLOW_CONTROLLED || !has_receiving_proxy",
    ]
    missing = [token for token in defer_required if token not in defer_body]
    if missing:
        fail("IPC DATA admission must inline receivers, defer exports/pre-attach data, and drop invalid nonreceivers: " + ", ".join(missing))

    admission_body = function_body(remote_ipc, "wki_ipc_admit_dev_op_rx")
    admission_required = [
        "WkiIpcDevOpRxAdmission::DISCARD",
        "if (req.op_id == OP_EPOLL_CTL)",
        "WkiIpcDevOpRxAdmission::INLINE",
        "try_enqueue_ipc_dev_op_work_rx",
        "WkiIpcDevOpRxAdmission::RETRY",
        "WkiIpcDevOpRxAdmission::DEFERRED",
    ]
    missing = [token for token in admission_required if token not in admission_body]
    if missing:
        fail("IPC reliable RX must reserve bounded deferred work before ACK: " + ", ".join(missing))

    dispatch_body = function_body(remote_ipc, "handle_ipc_dev_op_req")
    dispatch_required = [
        "if (req.op_id != OP_EPOLL_CTL)",
        "handle_ipc_dev_op_req_inline(hdr, payload, payload_len, nullptr, nullptr, response_channel)",
    ]
    missing = [token for token in dispatch_required if token not in dispatch_body]
    if missing:
        fail("IPC inline dispatch must be restricted to bounded epoll_ctl: " + ", ".join(missing))
    require_order(dispatch_body, "if (req.op_id != OP_EPOLL_CTL)", "handle_ipc_dev_op_req_inline", "bounded IPC inline dispatch")

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

    close_write_start = data_body.find("if (OP_ID == OP_PIPE_CLOSE_WRITE || OP_ID == OP_PIPE_CLOSE_WRITE_FLOW)")
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


def test_export_snapshot_pins_and_cloexec_are_safe() -> None:
    source = REMOTE_IPC_CPP.read_text()
    start = source.index("explicit IpcFdSnapshot(")
    end = source.index("~IpcFdSnapshot()", start)
    ctor = source[start:end]
    require_order(ctor, "fd_table_lock.lock_irqsave()", "source.get_fd_cloexec(", "snapshot flags under FD lock")
    require_order(ctor, "refcount.fetch_add(", "fd_table_lock.unlock_irqrestore(", "pin File before unlocking FD table")
    if "new " in ctor or "ipc_release_file_ref(" in ctor:
        fail("snapshot capture cannot allocate or release Files while holding FD lock")
    export = function_body(source, "wki_ipc_export_task_fds")
    if "task->fd_table.for_each" in export or "IpcFdSnapshot snapshot(*task)" not in export:
        fail("IPC export must traverse a retained non-CLOEXEC snapshot")
    handoff = function_body(source, "wki_ipc_commit_task_fd_handoff")
    require_order(handoff, "fd_usage_changed_locked(*task)", "fd_table_lock.unlock_irqrestore(",
                  "handoff FD observation remains within the removal transaction")
    compute = REMOTE_COMPUTE_CPP.read_text()
    cleanup = function_body(compute, "wki_remote_compute_cleanup_for_task")
    require_order(cleanup, "wki_ipc_cancel_fd_snapshots(*exiting_task)", "s_compute_lock.lock()",
                  "fatal snapshot cleanup before compute lock acquisition")


def test_export_registry_publication_cannot_allocate() -> None:
    source = REMOTE_IPC_CPP.read_text()
    begin = source.index("class IpcRegistry {")
    end = source.index("IpcExportRegistry g_ipc_exports", begin)
    registry = source[begin:end]
    if "new " in registry or "delete " in registry or "std::deque" in registry or "pump_next" in registry:
        fail("export membership must neither allocate/free nor share the pump retirement link")
    for token in ["registry_previous", "registry_next", "auto erase(", "auto begin()", "auto end()", "auto size()"]:
        if token not in registry:
            fail("intrusive registry contract missing: " + token)
    if "IpcRegistry<ProxyIpcState> g_ipc_proxies" not in source:
        fail("proxy publication after FD install cannot allocate registry storage")
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    begin = task.index("Task* Task::create_kernel_thread(")
    end = task.index("void Task::load_context", begin)
    worker = task[begin:end]
    require_order(worker, "new (std::nothrow) Task", "if (task == nullptr)", "check worker allocation")
    require_order(worker, "mm::phys::page_free(reinterpret_cast<void*>(stack_base))", "task->kthread_entry",
                  "unwind unowned kernel stack before dereferencing failed worker")


def test_submission_guard_covers_preparation_and_failure_transfer() -> None:
    source = REMOTE_IPC_CPP.read_text()
    begin = source.index("void WkiIpcSubmissionGuard::cancel()")
    end = source.index("void wki_ipc_cancel_submission_guards(", begin)
    cancel = source[begin:end]
    require_order(cancel, "entry = {}", "ipc_release_file_ref(file)", "clear ownership before reentrant File put")
    require_order(cancel, "export_cursor++", "wki_ipc_cleanup_exported_fds(&entry, 1, consumer, original_channels)", "drain one tracked export per cleanup")
    require_order(cancel, "wki_ipc_cleanup_exported_fds(", "disarm()", "remain linked through blocking cleanup")
    cleanup = function_body(source, "wki_ipc_cleanup_exported_fds")
    require_order(cleanup, "free_retired_exports(retired_exports)", "ipc_release_file_ref(file)", "free detached object before last File put")
    compute = REMOTE_COMPUTE_CPP.read_text()
    cleanup = function_body(compute, "wki_remote_compute_cleanup_for_task")
    require_order(cleanup, "wki_ipc_cancel_submission_guards(*exiting_task)", "wki_ipc_cancel_fd_snapshots(*exiting_task)", "cancel publication before dropping snapshot pins")
    from exec_source_test import function_body as multiline_body
    placement = multiline_body(compute, "wki_try_remote_spawn")
    require_order(placement, "WkiIpcSubmissionGuard ipc_guard(", "wki_ipc_export_task_fds(", "track before export publication")
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_body(compute, name)
        require_order(body, "WkiIpcSubmissionGuard ipc_guard(", "preparation->disarm()", "adopt before abandoning outer guard")
        require_order(body, "publish_submitted_task_locked(std::move(st), &publication_error)", "remember_submitted_ipc_handoff(*published", "durable row before pin transfer")
        require_order(body, "remember_submitted_ipc_handoff(*published", "ipc_guard.disarm()", "row owns pins before guard disarms")
        if body.count("restore_submitted_ipc_preparation_locked(*task_ptr") != 3:
            fail("send failure, timeout and rejection must return ownership to tracked guard")
    restore = function_body(compute, "restore_submitted_ipc_preparation_locked")
    require_order(restore, "handoff[i] = submitted.ipc_fd_handoff[i]", "submitted.handoff_pin_count = 0", "restore exact pins before emptying row")
    require_order(restore, "submitted.ipc_fd_count = 0", "guard.arm()", "one owner after restoration")


def test_deferred_rdma_write_pins_retirement() -> None:
    source = REMOTE_IPC_CPP.read_text()
    predicate = function_body(source, "export_can_retire_locked")
    if "exp->pipe_write_in_flight" not in predicate:
        fail("retirement must respect a deferred writer's ring pointer")
    begin = source.index("void export_pipe_write_flush_thread_loop(")
    end = source.index("[[noreturn]] void export_pipe_write_flush_thread_0()", begin)
    worker = source[begin:end]
    require_order(worker, "exp->pipe_write_in_flight = true", "ssize_t const WRITE_RET = export_pipe_write_nonblocking(", "pin before unlocked VFS write")
    require_order(worker, "ssize_t const WRITE_RET = export_pipe_write_nonblocking(", "finish_export_pipe_write_locked(backlog, WRITE_OWNER, file, &retired)", "release original owner only after VFS returns")
    finish = function_body(source, "finish_export_pipe_write_locked")
    require_order(finish, "owner->pipe_write_in_flight = false", "compact_inactive_exports_locked()", "writer completes deferred retirement")
    require_order(finish, "erase_export_pipe_write_backlog_locked(backlog)", "compact_inactive_exports_locked()", "detach backlog before final retirement")


def test_rdma_preparation_is_private_and_revoked_before_free() -> None:
    source = REMOTE_IPC_CPP.read_text()
    body = function_body(source, "prepare_export_pipe_rdma")
    for forbidden in ["g_ipc_exports", "g_export_pipe_write_backlogs", "queue_export_pipe_write_flush_locked", "wake_", "transport->"]:
        if forbidden in body: fail("private secure preparation cannot publish or queue: " + forbidden)
    require_order(body, "s_ipc_lock.unlock_irqrestore(IRQF)", "wki_secure_rdma_register_region(", "core outside metadata guard")
    for token in ["vfs_is_pipe_file(file)", "rdma_protocol::Service::IPC", "PERMIT_READ | rdma_protocol::PERMIT_WRITE",
                  "&exp, exp.pipe_secure_descriptor", "LIFETIME == UINT64_MAX"]:
        if token not in body: fail("typed private pipe preparation missing " + token)
    release = function_body(source, "release_export_pipe_rdma")
    require_order(release, "wki_secure_rdma_unregister_region(", "delete[] region", "close/drain before backing free")
    if "!= 0) return false" not in release: fail("failed reclaim must retain backing")
    retire = function_body(source, "free_retired_exports")
    if "g_secure_retired_exports = retired" not in retire: fail("registered context must remain on task retirement chain")
    drain = function_body(source, "service_secure_retired_exports")
    require_order(drain, "s_ipc_lock.unlock_irqrestore(IRQF)", "release_export_pipe_rdma(*retired)", "reclaim outside IPC guard")
    if "g_secure_retired_exports = retired" not in drain: fail("BUSY must retain/requeue context")
    export = function_body(source, "wki_ipc_export_task_fds")
    require_order(export, "prepare_export_pipe_rdma(", "g_ipc_exports.push_back(exp)", "finish private prep before publication")
    tail = export[export.index("g_ipc_exports.push_back(exp)"):]
    if "new " in tail or "reserve_capacity(" in tail or "rdma_register_region(" in tail:
        fail("publication cannot allocate or invoke preparation callbacks")
    tail = tail[tail.index("s_ipc_lock.unlock_irqrestore(irqf)"):]
    if "exp->" in tail or "backlog->" in tail: fail("published pointer cannot survive registry unlock")
    if "IpcRegistry<ExportPipeWriteBacklog> g_export_pipe_write_backlogs" not in source:
        fail("backlog publication cannot allocate registry storage")


def test_secure_pipe_native_prefix_is_owner_authoritative() -> None:
    source = REMOTE_IPC_CPP.read_text()
    for forbidden in ["transport->rdma_read", "transport->rdma_write", "transport->rdma_register_region", "transport->doorbell",
                      "WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED", "header->head", "header->tail"]:
        if forbidden in source: fail("raw pipe memory authority remains: " + forbidden)
    arm = function_body(source, "ipc_secure_arm")
    for token in ["ipc_channels_live", "pipe_secure_activated", "pipe_secure_epoch", "request.peer_node",
                  "memcmp(&request.grant", "intent.head == exp->pipe_bytes_received", "intent.credit == ipc_secure_credit_locked",
                  "pipe_secure_reserved = true", "file->refcount.fetch_add"]:
        if token not in arm: fail("native exact ARM policy missing " + token)
    effect = function_body(source, "ipc_secure_effect")
    require_order(effect, "s_ipc_lock.unlock_irqrestore(IRQF)", "export_pipe_write_nonblocking(", "native write outside guards")
    require_order(effect, "export_pipe_write_nonblocking(", "pipe_bytes_delivered += PREFIX", "actual VFS prefix before receipt")
    for token in ["++epoch->active_workers", "release_ipc_dev_op_worker_locked", "WRITTEN == -EAGAIN", "WkiSecureRdmaOutcome::UNCERTAIN"]:
        if token not in effect: fail("native effect drain/error policy missing " + token)
    if "static_cast<uint16_t>(request.length), false)" not in effect:
        fail("secure IPC effect cannot allocate/reclaim by growing native pipe")
    native_write = function_body(source, "export_pipe_write_nonblocking")
    if "IS_PIPE && grow_capacity" not in native_write:
        fail("ordinary native pipe growth must remain separately opt-in")
    writer = function_body(source, "proxy_pipe_write")
    require_order(writer, "acquire_proxy_pipe_rdma_writer(", "proxy_pipe_write_rdma(", "whole-call writer loan")
    if "ProxyPipeWriterLoan writer" not in writer: fail("ordinary zero-effect fallback must retain writer serialization")
    if "return false" not in function_body(source, "wki_ipc_doorbell_rx"): fail("raw doorbell must fail closed")


def test_export_admission_precedes_side_effects() -> None:
    source = REMOTE_IPC_CPP.read_text()
    plan = function_body(source, "plan_ipc_fd_export")
    for token in ["candidate.count == MAX_SUBMIT_IPC_FDS", "return -ENOTSUP", "fd >= 3", "FSType::DEVFS",
                  "vfs_is_epoll_file(file)"]:
        if token not in plan:
            fail("complete retained FD admission missing: " + token)
    if "new " in plan or "s_ipc_lock" in plan or "start_pipe_pump" in plan:
        fail("FD admission must not publish or allocate exports")
    export = function_body(source, "wki_ipc_export_task_fds")
    for effect in ["reserve_ipc_resource_ids("]:
        require_order(export, "if (ADMISSION != 0) return ADMISSION", effect, "all FD admission before side effects")
    require_order(export, "snapshot.prepare_exports(plan.count)", "reserve_ipc_resource_ids(", "prepare all heap objects before ID reservation/publication")
    require_order(export, "if (PREPARATION != 0) return PREPARATION", "g_ipc_exports.push_back(exp)", "allocation failure cannot leave registry entries")
    if "new " in export:
        fail("export publication must transfer already prepared objects")
    if "count < MAX_IPC_FDS" in export or "return count > 0" in export:
        fail("export cannot silently truncate or conflate no IPC with failure")
    placement = function_body(REMOTE_COMPUTE_CPP.read_text(), "wki_try_remote_spawn")
    if placement.count('"fd-export-admission"') != 2:
        fail("both first export and inline retry must preserve admission failure")


def test_pumps_activate_only_after_accepted_handoff() -> None:
    source = REMOTE_IPC_CPP.read_text()
    export = function_body(source, "wki_ipc_export_task_fds")
    if "start_pipe_pump(" in export or "enqueue_pipe_pump_locked(" in export:
        fail("unaccepted export must not consume source input")
    if "vfs_pipe_reserve_capacity(" in export:
        fail("submission preparation cannot grow a source pipe")
    write = function_body(source, "export_pipe_write_nonblocking")
    require_order(write, "return -EBADF", "vfs_pipe_reserve_capacity(", "invalid writes cannot grow pipe")
    require_order(write, "if (len == 0)", "vfs_pipe_reserve_capacity(", "zero writes cannot grow pipe")
    claim = function_body(source, "claim_accepted_pipe_pump_locked")
    for token in ["find_export_by_resource_id(resource_id)", "exp->consumer_node != consumer_node", "!exp->pump_pending_accept",
                  "exp->pump_pending_accept = false"]:
        if token not in claim:
            fail("accepted activation must be peer-scoped and idempotent: " + token)
    start = function_body(source, "start_pipe_pump")
    require_order(start, "s_ipc_lock.lock_irqsave()", "claim_accepted_pipe_pump_locked(", "claim under registry lock")
    after_creation = start[start.index("task = create_pipe_pump_worker("):]
    if "exp->" in after_creation:
        fail("activation cannot dereference an exporter retained across worker creation")
    require_order(after_creation, "s_ipc_lock.lock_irqsave()", "slot_arg->exp.load(", "resolve current owned slot under lock")
    from exec_source_test import function_body as multiline_body
    compute = REMOTE_COMPUTE_CPP.read_text()
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_body(compute, name)
        require_order(body, "task_rejection_errno(accept_status)", "wki_ipc_activate_task_fds(", "reject before activation")
        require_order(body, "handoff_cleanup.count = 0", "wki_ipc_activate_task_fds(", "consume temporary pins before blocking activation")
        require_order(body, "WkiIpcSubmissionGuard accepted_handoff(", "handoff_cleanup = take_submitted_ipc_handoff_locked(task_ptr)",
                      "accepted pins remain reachable from fatal cleanup before row ownership transfers")



def test_import_provenance_precedes_all_side_effects() -> None:
    source = REMOTE_IPC_CPP.read_text()
    validate = function_body(source, "wki_ipc_validate_task_fds")
    for token in ["count > MAX_SUBMIT_IPC_FDS", "source_node == WKI_NODE_INVALID", "source_node == WKI_NODE_BROADCAST",
                  "entry.home_node != source_node", "entry.resource_id == 0", "map[j].resource_id == entry.resource_id",
                  "map[j].local_fd == entry.local_fd"]:
        if token not in validate:
            fail("descriptor import validation missing: " + token)
    for token in ["new ", "s_ipc_lock", "fd_table.insert", "get_current_task", "wki_send"]:
        if token in validate:
            fail("descriptor validation cannot act on objects: " + token)
    receiver = function_body(REMOTE_COMPUTE_CPP.read_text(), "handle_task_submit_work")
    admission = "wki_ipc_validate_task_fds(entries, submit->ipc_fd_count, src_node)"
    require_order(receiver, "prepare_submitted_resources(", admission, "decode lengths before map read")
    for token in ["prepare_submitted_identity(", "ScopedSubmitVfsIdentity submit_vfs_identity", "load_elf_from_vfs_path"]:
        require_order(receiver, admission, token, "map provenance before construction side effects")
    if not re.search(r"wki_ipc_attach_task_fds\(new_task,\s*ipc_entries,\s*submit->ipc_fd_count,\s*src_node,\s*IPC_CONTROL_CHANNEL,\s*IPC_DATA_CHANNEL,\s*output_completion.get\(\)\)", receiver):
        fail("final import must use the same authenticated submitter")


def test_attach_fd_install_is_transactional() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    install_body = function_body(remote_ipc, "install_ipc_proxy_files")
    attach_body = function_body(remote_ipc, "wki_ipc_attach_task_fds")
    for token in ["task.fd_table_lock.lock_irqsave()", "task.fd_table.reserved(entry.fd)",
                  "task.fd_table.insert(entry.fd, entry.file)",
                  "task.fd_table.insert(imports[j].fd, imports[j].existing)",
                  "ker::vfs::fd_usage_changed_locked(task)"]:
        if token not in install_body:
            fail("IPC batch installation or rollback missing: " + token)
    if "new " in install_body or "vfs_put_file(" in install_body or "s_ipc_lock" in install_body:
        fail("IPC descriptor commit must not allocate, close files, or nest the IPC lock")
    for token in ["wki_ipc_validate_task_fds(map, count, source_node)",
                  "free_uninstalled_ipc_proxy_file(prepared[i].file, prepared[i].proxy)",
                  "if (!install_ipc_proxy_files(", "return abandon()",
                  "ker::vfs::vfs_put_file(prepared[i].existing)", "g_ipc_proxies.push_back(proxy)"]:
        if token not in attach_body:
            fail("IPC batch preparation/publication missing: " + token)
    require_order(attach_body, "wki_ipc_validate_task_fds(map, count, source_node)", "new (std::nothrow) ProxyIpcState",
                  "validate all inherited descriptors before allocation")
    require_order(attach_body, "install_ipc_proxy_files(", "g_ipc_proxies.push_back(proxy)", "batch before proxy registration")
    require_order(attach_body, "g_ipc_proxies.push_back(proxy)", "vfs_put_file(prepared[i].existing)",
                  "publish all proxy ownership before potentially blocking old-file closes")


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
    envelope = function_body(remote_ipc, "read_ipc_control_response")
    for token in ["channel != WKI_CHAN_RESOURCE", "payload == nullptr", "len < sizeof(response)",
                  "response.data_len >= sizeof(uint32_t)", "sizeof(response) + response.data_len <= len"]:
        if token not in envelope:
            fail("IPC control response must validate channel and bounded envelope: " + token)
    for name in ["wki_ipc_handle_dev_op_resp", "wki_ipc_socket_handle_dev_op_resp"]:
        body = function_body(remote_ipc, name)
        require_order(body, "read_ipc_control_response(channel, payload, len, resp)", "s_ipc_lock.lock_irqsave()",
                      "validate response before proxy lookup or completion effects")
    if "wki_ipc_response_matches_pending(resp.op_id, resp.reserved, *target_proxy)" not in response_body:
        fail("DEV_OP_RESP handler must match both op id and echoed response cookie")
    if "target_proxy->pending_wait_op = 0" in response_body or "target_proxy->pending_wait_cookie = 0" in response_body:
        fail("DEV_OP_RESP handler must keep the busy slot occupied until the waiter consumes the response")
    if "wki_ipc_retire_wait_cookie_locked(target_proxy, resp.op_id, resp.reserved)" not in response_body:
        fail("exact response must retire the unanswered identity even after waiter timeout")
    allocator = function_body(header, "wki_ipc_allocate_wait_cookie_locked")
    for token in ["proxy->unanswered_controls", "available == nullptr", "identity.cookie == cookie", ".op = op, .cookie = cookie"]:
        if token not in allocator:
            fail("cookie allocation must reserve bounded unanswered identities: " + token)
    cancellation = function_body(remote_ipc, "wki_ipc_cancel_pending_wait")
    if "if (!request_sent)" not in cancellation:
        fail("timeouts must preserve unanswered control identities")
    send = function_body(WKI_CPP.read_text(), "wki_send_impl")
    after_tx = send[send.index("route.send_pkt(pkt)"):]
    if re.findall(r"\breturn\s+([^;]+);", after_tx) != ["WKI_OK"]:
        fail("cookie release on send failure requires every accepted reliable send to return WKI_OK")
    if "wki_ipc_consume_pending_wait_response" not in remote_ipc:
        fail("proxy control waiters must consume and clear side-band responses through the shared helper")

    cookie_classes = function_body(remote_ipc, "ipc_op_uses_response_cookie")
    if "op_id != OP_SOCK_CLOSE" not in cookie_classes:
        fail("fire-and-forget socket close must not be rejected for omitting a response cookie")

    server_body = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    server_required = [
        "ipc_op_uses_response_cookie(OP_ID)",
        "std::memcpy(&request_cookie, op_data, WKI_IPC_OP_COOKIE_BYTES)",
        "resp->reserved = request_cookie",
        "unsupported_ipc_control_reply(OP_ID, resource_id, request_cookie)",
        "response.reserved = cookie",
        "pty_resp.reserved = request_cookie",
    ]
    missing = [token for token in server_required if token not in remote_ipc]
    if missing:
        fail("home-side IPC control responses must echo request cookies: " + ", ".join(missing))

    admission = function_body(remote_ipc, "wki_ipc_admit_dev_op_rx")
    if "WkiIpcDevOpRxAdmission::RETRY" not in admission:
        fail("IPC admission exhaustion must withhold ACK instead of synthesizing a stale-cookie error")

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
    for body in [socket_body, pty_body, epoll_body]:
        require_order(body, "if (op_cookie == 0)", "proxy->pending_wait = &wait", "reserve response identity before publishing waiter")
        if "return finish(-EAGAIN)" not in body or "wki_ipc_cancel_pending_wait(proxy, &wait, WKI_ERR_TX_FAILED, false)" not in body:
            fail("control sender must reject saturated identities and release definitive unsent requests")


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

    enqueue_body = function_body(remote_ipc, "try_enqueue_ipc_dev_op_work_rx")
    if "work->cleanup_epoch = PEER_EPOCH->epoch" not in enqueue_body:
        fail("deferred IPC DEV_OP enqueue must snapshot the source peer cleanup epoch")

    worker_body = function_body(remote_ipc, "ipc_dev_op_worker_thread_fn")
    require_order(
        worker_body,
        "claimed = claim_ipc_dev_op_worker_locked(work)",
        "handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len, &work, &retry, work->channel_identity)",
        "worker stale work check",
    )
    if "free_ipc_dev_op_work(work);" not in worker_body[worker_body.find("claimed = claim_ipc_dev_op_worker_locked(work)") :]:
        fail("deferred IPC DEV_OP worker must free stale work without handling it")
    for token in ["release_ipc_dev_op_worker_locked(SRC_NODE)", "ipc_dev_op_queue_push_front_locked", "kern_sleep_us"]:
        if token not in worker_body:
            fail(f"IPC worker must retain and retry accepted must-not-drop work: missing {token}")

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
        "queue.count--",
        "work->next = nullptr",
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
        "ipc_peer_active_workers_locked(node_id) == 0",
        "cleanup active-worker drain",
    )
    require_order(
        cleanup_body,
        "ipc_peer_active_workers_locked(node_id) == 0",
        "collect_ipc_peer_cleanup_batch_locked(node_id, batch)",
        "cleanup begin epoch",
    )
    require_order(
        cleanup_body,
        "collect_ipc_peer_cleanup_batch_locked(node_id, batch)",
        "end_ipc_peer_dev_op_cleanup_locked(node_id)",
        "cleanup end epoch",
    )
    pump_pending = cleanup_body.find("bool const STOPPED_PUMP_PENDING = batch.stopped_pump_count != 0")
    batch_drain = cleanup_body.find("drain_ipc_peer_cleanup_batch(batch)", pump_pending)
    pump_guard = cleanup_body.find("if (STOPPED_PUMP_PENDING)", batch_drain)
    pump_reschedule = cleanup_body.find("ker::mod::sched::kern_yield()", batch_drain)
    cleanup_end = cleanup_body.find("end_ipc_peer_dev_op_cleanup_locked(node_id)", pump_reschedule)
    if pump_pending < 0 or batch_drain < 0 or pump_guard < 0 or pump_reschedule < pump_guard or cleanup_end < 0:
        fail("peer cleanup must reschedule only stopped pipe pumps while cleanup admission remains fenced")


def test_ipc_dev_ops_are_reserved_before_reliable_ack() -> None:
    wki = WKI_CPP.read_text()
    remote_ipc = REMOTE_IPC_CPP.read_text()
    rx = function_body(wki, "wki_rx")
    admission = function_body(remote_ipc, "wki_ipc_admit_dev_op_rx")
    enqueue = function_body(remote_ipc, "try_enqueue_ipc_dev_op_work_rx")

    require_order(
        rx,
        "admit_ipc_dev_op(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation)",
        "IPC_DEV_OP_ADMISSION == WkiIpcDevOpRxAdmission::RETRY",
        "IPC direct admission result",
    )
    direct_admission = rx[rx.index("WkiIpcDevOpRxAdmission const IPC_DEV_OP_ADMISSION") :]
    require_order(
        direct_admission,
        "IPC_DEV_OP_ADMISSION == WkiIpcDevOpRxAdmission::RETRY",
        "ch->rx_seq++",
        "IPC RETRY must precede reliable sequence publication",
    )
    if rx.count("admit_ipc_dev_op(") != 2:
        fail("direct and reorder reliable RX paths must both pre-admit IPC DEV_OP work")
    for token in [
        "if (req.op_id == OP_EPOLL_CTL)",
        "WkiIpcDevOpRxAdmission::INLINE",
        "try_enqueue_ipc_dev_op_work_rx",
    ]:
        if token not in admission:
            fail(f"IPC epoll/deferred admission split is missing {token}")
    for token in [
        "s_ipc_lock.try_lock()",
        "s_ipc_dev_op_pool_lock.try_lock()",
        "work->channel_identity =",
        "IpcPeerCleanupEpoch* const PEER_EPOCH = ipc_peer_cleanup_slot_locked(hdr->src_node, true)",
        "work->cleanup_epoch = PEER_EPOCH->epoch",
        "std::memcpy(work->payload, payload, payload_len)",
        "ipc_dev_op_queue_push_back_locked(queue, work)",
    ]:
        if token not in enqueue:
            fail(f"IPC fixed pre-ACK reservation is missing {token}")
    for forbidden in ["operator new", "new (std::nothrow)", "lock_irqsave", "kern_", "vfs_", "rdma_"]:
        if forbidden in enqueue:
            fail(f"IPC RX reservation contains unsafe operation {forbidden!r}")


def test_large_deferred_dev_op_payloads_are_coallocated() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    for snippet in [
        "struct IpcDevOpStorage",
        "std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> payload",
        "bool payload_coallocated = false",
        "bool fixed_pool = false",
        "bool pipe_chunk_transferable = false",
        "WKI_IPC_DEV_OP_MAX_PENDING = 256",
        "WKI_IPC_DEV_OP_TRANSFER_RESERVE = WKI_IPC_DEV_OP_WORKER_COUNT * 2",
        "std::array<IpcDevOpStorage, WKI_IPC_DEV_OP_MAX_PENDING> g_ipc_dev_op_work",
        "size_t g_ipc_dev_op_free_count = 0",
    ]:
        if snippet not in remote_ipc:
            fail(f"bounded deferred IPC DEV_OP storage policy is missing {snippet!r}")

    alloc_body = function_body(remote_ipc, "take_ipc_dev_op_work_locked")
    for snippet in [
        "payload_len > WKI_ETH_MAX_PAYLOAD",
        "IpcDevOpWork* work = g_ipc_dev_op_free",
        "g_ipc_dev_op_free = work->next",
        "work->payload = storage->payload.data()",
        "work->payload_coallocated = true",
        "work->fixed_pool = true",
        "work->pipe_chunk_transferable = g_ipc_dev_op_free_count >= WKI_IPC_DEV_OP_TRANSFER_RESERVE",
    ]:
        if snippet not in alloc_body:
            fail(f"deferred IPC fixed allocator is missing {snippet!r}")
    for forbidden in ("operator new", "new (std::nothrow)", "lock_irqsave"):
        if forbidden in function_body(remote_ipc, "try_enqueue_ipc_dev_op_work_rx"):
            fail(f"IPC RX admission must be allocation-free and try-only: found {forbidden!r}")

    release_body = function_body(remote_ipc, "free_ipc_dev_op_work")
    for snippet in [
        "if (!work->fixed_pool)",
        "work->payload = storage->payload.data()",
        "s_ipc_dev_op_pool_lock.lock_irqsave()",
        "work->next = g_ipc_dev_op_free",
        "g_ipc_dev_op_free = work",
        "++g_ipc_dev_op_free_count",
    ]:
        if snippet not in release_body:
            fail(f"deferred IPC DEV_OP release is missing {snippet!r}")

    enqueue_body = function_body(remote_ipc, "try_enqueue_ipc_dev_op_work_rx")
    for snippet in ["s_ipc_lock.try_lock()", "s_ipc_dev_op_pool_lock.try_lock()", "take_ipc_dev_op_work_locked(payload_len, delivery_group)"]:
        if snippet not in enqueue_body:
            fail(f"deferred IPC RX enqueue is missing {snippet!r}")


def test_large_deferred_dev_op_payloads_transfer_to_export_backlog() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    for snippet in [
        "IpcDevOpWork* dev_op_owner = nullptr",
        "static_assert(sizeof(PendingPipeChunk) == 24)",
        "WKI_IPC_DEV_OP_TRANSFER_MIN_DATA",
        "len < WKI_IPC_DEV_OP_TRANSFER_MIN_DATA",
        "len <= work->payload_len - OFFSET",
        "!work->pipe_chunk_transferable",
        "queue_export_pipe_write_data(uint32_t resource_id, const uint8_t* data, uint16_t len, IpcDevOpWork** work_owner,",
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
        "proxy_pipe_read",
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
    worker_call = "handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len, &work, &retry, work->channel_identity)"
    worker_call_pos = worker_body.find(worker_call)
    if worker_call_pos < 0:
        fail("deferred IPC worker must offer its work item to the inline handler")
    if "work->" in worker_body[worker_call_pos + len(worker_call) :]:
        fail("deferred IPC worker must not dereference work after ownership can transfer")
    dispatch_body = function_body(remote_ipc, "handle_ipc_dev_op_req")
    if "handle_ipc_dev_op_req_inline(hdr, payload, payload_len, nullptr, nullptr, response_channel)" not in dispatch_body:
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
        condition_start = handler_body.rfind("if (!", 0, position)
        block_start = handler_body.find("{", position)
        if condition_start < 0 or block_start < 0:
            fail("export backlog ownership transfer must be guarded by a failed-queue branch")
        depth = 1
        block_end = block_start + 1
        while block_end < len(handler_body) and depth > 0:
            if handler_body[block_end] == "{":
                depth += 1
            elif handler_body[block_end] == "}":
                depth -= 1
            block_end += 1
        return_pos = handler_body.find("return;", block_end)
        if depth != 0 or return_pos < 0:
            fail("export backlog queue calls must return on their current control-flow branch")
        suffix = handler_body[block_end:return_pos]
        forbidden = [token for token in ["hdr->", "payload", "op_data"] if token in suffix]
        if forbidden:
            fail("handler dereferences transferred storage after queue publication: " + ", ".join(forbidden))


def test_legacy_ipc_controls_require_issued_objects() -> None:
    source = REMOTE_IPC_CPP.read_text()
    handler = function_body(source, "handle_ipc_dev_op_req_inline")
    start = handler.index("if (OP_ID == OP_EPOLL_CTL || OP_ID == OP_FUTEX_WAKE)")
    refusal = handler[start:handler.index("if (OP_ID == OP_PTY_IOCTL)", start)]
    for token in ["unsupported_ipc_control_reply(OP_ID, resource_id, request_cookie)", "send_response(REPLY.data()", "return;"]:
        if token not in refusal:
            fail("legacy control refusal must preserve completion identity: " + token)
    for token in ["futex_wake_by_phys(", "ker::vfs::epoll_ctl("]:
        if token in source:
            fail("legacy IPC must not invoke ambient local object callbacks: " + token)
    reply = function_body(source, "unsupported_ipc_control_reply")
    if "response.status = -ENOTSUP" not in reply:
        fail("ungranted legacy controls must report unsupported operation")
    for token in ["new ", "lock", "find_", "get_current_task"]:
        if token in reply:
            fail("legacy refusal must build only bounded reply bytes: " + token)


def test_export_resource_ids_cannot_wrap_or_partially_publish() -> None:
    source = REMOTE_IPC_CPP.read_text()
    reserve = function_body(source, "reserve_ipc_resource_ids")
    require_order(reserve, "next == 0", "first = next", "exhaustion before output mutation")
    require_order(reserve, "UINT32_MAX - next", "first = next", "range check before output mutation")
    if "LAST == UINT32_MAX ? 0 : LAST + 1" not in reserve:
        fail("resource ID exhaustion must stay permanent")
    export = function_body(source, "wki_ipc_export_task_fds")
    require_order(export, "reserve_ipc_resource_ids(", "if (!IDS_RESERVED) return -EOVERFLOW", "check full reservation")
    for token in ["prepare_export_pipe_rdma(", "g_ipc_exports.push_back(exp)"]:
        require_order(export, "if (!IDS_RESERVED) return -EOVERFLOW", token, "exhaustion before side effects")


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


def test_retired_exports_fence_and_discard_unattached_deliveries() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    wire = WKI_WIRE_HPP.read_text()
    required = [
        "constexpr uint16_t OP_PIPE_DISCARD_PENDING = 0x0707",
        "any pre-attachment DATA/EOF state that can no longer acquire a proxy owner",
    ]
    for snippet in required:
        if snippet not in wire:
            fail(f"ordered pending-delivery discard wire contract is missing {snippet!r}")

    cleanup = function_body(remote_ipc, "wki_ipc_cleanup_exported_fds")
    for snippet in [
        "direct_discards",
        "stop_pipe_pump_locked(exp)",
        "schedule_pending_pipe_discard(discard.channel, discard.resource_id)",
    ]:
        if snippet not in cleanup:
            fail(f"export cleanup is missing ordered pending-delivery retirement {snippet!r}")

    handler = function_body(remote_ipc, "handle_ipc_dev_op_req_inline")
    require_order(
        handler,
        "OP_ID == OP_PIPE_DISCARD_PENDING",
        "OP_ID == OP_PIPE_DATA || OP_ID == OP_PIPE_DATA_FLOW",
        "pending-delivery discard dispatch",
    )
    if "discard_pending_pipe_delivery(hdr->src_node, resource_id)" not in handler:
        fail("pending-delivery discard must release the exact peer/resource queue")

    close_begin = handler.find("if (OP_ID == OP_PIPE_CLOSE_READ)")
    close_end = handler.find("if (OP_ID >= OP_SOCK_ACCEPT", close_begin)
    if close_begin < 0 or close_end < 0:
        fail("pipe read-close retirement block is missing")
    close_read = handler[close_begin:close_end]
    for snippet in [
        "bool const DIRECT_DISCARD = pump_task == nullptr",
        "schedule_pending_pipe_discard(response_channel, resource_id)",
    ]:
        if snippet not in close_read:
            fail(f"pipe read-close retirement must fence late EOF state: {snippet!r}")


def test_socket_close_uses_reliable_proxy_close_queue() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    remote_socket = REMOTE_IPC_SOCKET_CPP.read_text()
    header = REMOTE_IPC_HPP.read_text()

    if "void wki_ipc_send_proxy_close(" not in header:
        fail("socket close must be able to share the bounded reliable proxy-close sender")

    shared_sender = function_body(remote_ipc, "wki_ipc_send_proxy_close")
    if "send_proxy_pipe_close(proxy, msg, msg_size, resource_id, op_id)" not in shared_sender:
        fail("shared proxy-close API must delegate to the copied-message retry queue")

    close_body = function_body(remote_socket, "proxy_socket_close")
    for snippet in [
        "req->op_id = OP_SOCK_CLOSE",
        "std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id",
        "wki_ipc_send_proxy_close(proxy, msg.data()",
        "wki_ipc_detach_proxy_file(f, proxy)",
    ]:
        if snippet not in close_body:
            fail(f"socket close is missing reliable terminal publication {snippet!r}")
    require_order(close_body, "wki_ipc_send_proxy_close", "wki_ipc_detach_proxy_file", "socket close publication")
    if "wki_send(" in close_body or "send_socket_op(" in close_body:
        fail("socket close must not bypass the copied-message retry queue")
    if "WKI_IPC_OP_COOKIE_BYTES" in close_body:
        fail("fire-and-forget socket close must retain its resource-only wire payload")


def test_remote_exec_fd_handoff_transfers_only_the_pinned_owner() -> None:
    remote_ipc = REMOTE_IPC_CPP.read_text()
    export_body = function_body(remote_ipc, "wki_ipc_export_task_fds")
    for snippet in [
        "file->refcount.fetch_add(1, std::memory_order_acq_rel)",
        "handoff_out[i] = WkiIpcTaskFdHandoff",
    ]:
        if snippet not in export_body:
            fail(f"IPC export must pin exact local fd identity before remote submit: {snippet!r}")

    commit_body = function_body(remote_ipc, "wki_ipc_commit_task_fd_handoff")
    for snippet in [
        "task->fd_table_lock.lock_irqsave()",
        "task->fd_table.lookup(entry.local_fd) == entry.file",
        "task->fd_table.remove(entry.local_fd)",
        "task->clear_fd_cloexec(entry.local_fd)",
        "ipc_release_file_ref(removed)",
        "ipc_release_file_ref(entry.file)",
    ]:
        if snippet not in commit_body:
            fail(f"accepted IPC fd handoff is missing exact-owner transfer: {snippet!r}")
    require_order(commit_body, "task->fd_table.remove(entry.local_fd)", "task->fd_table_lock.unlock_irqrestore(IRQF)", "fd removal publication")
    require_order(commit_body, "task->fd_table_lock.unlock_irqrestore(IRQF)", "ipc_release_file_ref(removed)", "fd owner release")
    if "s_ipc_lock" in commit_body:
        fail("accepted IPC fd handoff must not nest the fd-table and IPC locks")

    release_body = function_body(remote_ipc, "wki_ipc_release_task_fd_handoff")
    if "fd_table" in release_body or "ipc_release_file_ref(file)" not in release_body:
        fail("abandoned IPC fd handoff must drop only temporary identity pins")


def test_ipc_selftests_are_declared_and_registered() -> None:
    header = REMOTE_IPC_HPP.read_text()
    ktest = WKI_WAIT_KTEST.read_text()
    required = [
        "wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops",
        "wki_ipc_selftest_task_fd_handoff_transfers_exact_owner",
        "wki_ipc_selftest_large_dev_op_work_coallocates_payload",
        "wki_ipc_selftest_large_dev_op_work_backs_pipe_chunk",
        "wki_ipc_selftest_dev_op_work_pool_is_bounded",
        "wki_ipc_selftest_poll_wake_drains_over_capacity",
        "wki_ipc_selftest_inactive_proxy_poll_is_terminal",
        "wki_ipc_selftest_epoll_close_releases_lookup_ref",
        "wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags",
        "wki_ipc_selftest_export_pipe_write_burst_is_bounded",
        "wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode",
        "wki_ipc_selftest_pipe_affinity_uses_read_endpoints_only",
        "wki_ipc_selftest_write_only_pipe_omits_receive_ring",
        "wki_ipc_selftest_attach_insert_failure_preserves_existing_fd",
        "wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion",
        "wki_ipc_selftest_dev_op_response_uses_home_node_identity",
        "wki_ipc_selftest_discard_retires_unattached_delivery",
        "wki_ipc_selftest_stopped_pump_publishes_discard_fence",
    ]
    for token in required:
        if token not in header:
            fail(f"missing selftest declaration {token}")
        if token not in ktest:
            fail(f"missing KTEST coverage for {token}")


def test_output_receipts_follow_commit_and_survive_only_for_completion() -> None:
    source = REMOTE_IPC_CPP.read_text()
    send = function_body(WKI_CPP.read_text(), "wki_send_impl")
    require_order(send, "ch->retransmit_count++", "accepted_bytes->fetch_add", "account only recoverable frames")
    require_order(send, "ch->tx_seq++", "accepted_bytes->fetch_add", "counter follows sequence commit")
    counter = send.index("accepted_bytes->fetch_add")
    if "ch->lock.unlock()" in send[send.index("ch->tx_seq++"):counter]:
        fail("accepted prefix must be counted before releasing channel serialization")
    write = function_body(source, "proxy_pipe_write")
    if "wki_send_accounted(" not in write or "bytes_written.fetch_add" in write:
        fail("proxy writes must account at enqueue, not in return order")
    lookup = function_body(source, "find_proxy_by_endpoint_locked")
    if "receipt && proxy->completion_refs != 0" not in lookup:
        fail("closed proxies may be found only by retained receipt lookup")
    if source.count("find_proxy_by_endpoint_locked(hdr->src_node, resource_id, true)") != 1:
        fail("only the flow receipt handler may request closed-proxy lookup")
    detach = function_body(source, "wki_ipc_detach_proxy_file")
    if "if (proxy->completion_refs == 0) dropped_registry_ref = proxy_unregister_locked(proxy)" not in detach:
        fail("File close must preserve completion-owned receipt registry membership")
    refunds = function_body(source, "proxy_grant_message_write_credits")
    if "bytes_delivered" in refunds:
        fail("local credit refunds are not evidence of delivery")
    scan = function_body(REMOTE_COMPUTE_CPP.read_text(), "wki_remote_compute_check_completions")
    require_order(scan, "!rt.output_completion->ready()", "queue_pending_completion_locked(", "delivery before child completion")
    if "PeerLifecycleLease" in scan:
        fail("receipt progress must not wait behind the scanner's exclusive lifecycle lease")
    receipt = function_body(source, "complete_export_pipe_flow_receipt_locked")
    require_order(receipt, "if (!accepted) return false", "release_pending_pipe_chunk", "retain credit on failed enqueue")
    flush = function_body(source, "export_pipe_write_flush_thread_loop")
    require_order(flush, "bool const ACCEPTED = send_export_pipe_write_flow", "complete_export_pipe_flow_receipt_locked(backlog, ACCEPTED)",
                  "receipt acceptance before credit retirement")
    require_order(flush, "complete_export_pipe_flow_receipt_locked(backlog, ACCEPTED)", "retire_failed_export_pipe_write_locked",
                  "terminal error receipt before export retirement")
    if "chunk.offset == chunk.len && chunk.flow_credit == 0" not in flush:
        fail("completed flow chunks must remain owned until their receipt is enqueued")
    handler = function_body(source, "handle_ipc_dev_op_req_inline")
    if "if (!send_export_pipe_write_flow(CONTROL, resource_id, 0, -EPIPE)) retry_owned_work()" not in handler:
        fail("missing exports must retain deferred DATA until the terminal receipt is enqueued")


def test_pipe_eof_rechecks_data_under_registration_locks() -> None:
    body = function_body(REMOTE_IPC_CPP.read_text(), "proxy_pipe_read")
    serialized = body[body.index("for (;;) {"):]
    require_order(serialized, "s_ipc_lock.lock_irqsave()", "proxy->lock.lock_irqsave()", "read lock order")
    for token in ["if (HEAD != TAIL)", "pending != nullptr && !pending->chunks.empty()"]:
        require_order(serialized, token, "result = 0;", "EOF follows final data recheck")
    require_order(serialized, "s_ipc_lock.unlock_irqrestore", "release_pending_pipe_chunk(chunk)", "release outside locks")


def test_exported_controls_bind_the_issued_consumer() -> None:
    source = REMOTE_IPC_CPP.read_text()
    match = function_body(source, "ipc_export_matches_consumer")
    for token in ("consumer_node != WKI_NODE_INVALID", "consumer_node != WKI_NODE_BROADCAST", "exp->consumer_node == consumer_node", "exp->res_type == expected_type"):
        assert token in match
    pin = function_body(source, "ipc_acquire_export_file_locked")
    assert pin.index("ipc_export_matches_consumer(") < pin.index("refcount.fetch_add(")
    close = function_body(source, "mark_export_pipe_write_closed")
    assert close.index("ipc_export_matches_consumer(") < close.index("ensure_export_pipe_write_backlog_locked(")
    handler = function_body(source, "handle_ipc_dev_op_req_inline")
    calls = re.findall(r"ipc_acquire_export_file_locked\(([^)]*)\)", handler)
    assert len(calls) == 6 and all("hdr->src_node" in call for call in calls)
    assert sum("ResourceType::IPC_SOCKET" in call for call in calls) == 4
    assert sum("ResourceType::IPC_PTY" in call for call in calls) == 1
    read_close = handler[handler.index("if (OP_ID == OP_PIPE_CLOSE_READ)"):handler.index("if (OP_ID >= OP_SOCK_ACCEPT")]
    assert read_close.index("ipc_export_matches_consumer(") < read_close.index("stop_pipe_pump_locked(")
    assert "if (WRONG_CONSUMER) return;" in read_close
    pty_close = function_body(source, "mark_ipc_pty_write_closed_or_pending")
    assert "ipc_export_matches_consumer(exp, resource_id, peer_node, ResourceType::IPC_PTY)" in pty_close
    assert pty_close.index("if (denied) return true;") < pty_close.index("mark_pending_pipe_write_closed(")


def test_ipc_epoch_control_identities() -> None:
    source = REMOTE_IPC_CPP.read_text()
    peer = (ROOT / "modules/kern/src/net/wki/peer.cpp").read_text()
    for name in ["handle_hello", "handle_hello_ack"]:
        body = function_body(peer, name)
        require_order(body, "wki_ipc_mark_epoch_reset(peer_node)", "wki_channels_close_for_peer(peer_node)",
                      "retire IPC endpoints before connected epoch channel reset")
    drain = function_body(peer, "drain_pending_epoch_reset_cleanups")
    require_order(drain, "wki_remote_compute_cleanup_retired_for_peer", "wki_ipc_cleanup_for_peer", "compute before IPC drain")
    require_order(drain, "wki_ipc_cleanup_for_peer", "vfs_reset_rebind_pending.store(false", "IPC drain before admission reopens")
    mark = function_body(source, "wki_ipc_mark_epoch_reset")
    for token in ["begin_ipc_peer_dev_op_cleanup_locked(node_id)", "exp->active = false", "proxy->active.store(false",
                  "proxy->message_write_error.store(-EIO"]:
        if token not in mark:
            fail("IPC epoch marker missing " + token)
    for token in ["new ", "delete ", "kern_yield", "proxy_release", "ipc_release_file_ref", "wki_finish_claimed_op"]:
        if token in mark:
            fail("IPC HELLO marker must not allocate, free, block or wake: " + token)
    attach = function_body(source, "wki_ipc_attach_task_fds")
    require_order(attach, "wki_channel_generation_is_live(control_channel.channel", "new (std::nothrow) ProxyIpcState",
                  "validate admitted channel before import allocation")
    require_order(attach, "proxy->control_channel = control_channel", "g_ipc_proxies.push_back(proxy)", "retain immutable channel before publication")
    send = function_body(source, "wki_ipc_send_control")
    if "wki_send_on_channel_identity(identity, MsgType::DEV_OP_REQ" not in send or "wki_send(" in send:
        fail("control requests must not resolve a replacement numeric channel")
    handler = function_body(source, "handle_ipc_dev_op_req_inline")
    if "wki_send_on_channel_identity(response_channel, MsgType::DEV_OP_RESP" not in handler or "wki_send(" in handler:
        fail("deferred IPC replies must retain their RX allocation across callbacks")
    worker = function_body(source, "ipc_dev_op_worker_thread_fn")
    if "&work, &retry, work->channel_identity" not in worker:
        fail("worker must pass the admitted reply channel by value")
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    if "handle_ipc_dev_op_req(hdr, payload, payload_len, CHANNEL_IDENTITY)" not in server:
        fail("inline IPC refusal must retain the RX channel identity")


def test_data_and_queued_close_channel_retirement() -> None:
    source = REMOTE_IPC_CPP.read_text()
    capture = function_body(source, "wki_ipc_capture_data_channel")
    require_order(capture, "if (!control_live())", "capture_ipc_channel(", "admitted control before DATA capture")
    require_order(capture, "capture_ipc_channel(", "return control_live()", "control retirement after DATA capture")
    attach = function_body(source, "wki_ipc_attach_task_fds")
    for token in ["data_channel.peer_node_id != source_node", "data_channel.channel_id != WKI_CHAN_IPC_DATA",
                  "wki_channel_generation_is_live(data_channel.channel"]:
        require_order(attach, token, "new (std::nothrow) ProxyIpcState", "DATA token before import allocation")
    require_order(attach, "proxy->data_channel = data_channel", "g_ipc_proxies.push_back(proxy)", "immutable DATA before publication")
    write = function_body(source, "proxy_pipe_write")
    if "wki_send_accounted(proxy->data_channel," not in write or "if (ret == WKI_ERR_NOT_FOUND)" not in write:
        fail("pipe writes must retain DATA allocation and terminate retired-channel waits")
    accounted = function_body(WKI_CPP.read_text(), "wki_send_accounted")
    if not re.search(r"wki_send_impl\(identity.peer_node_id,\s*identity.channel_id,.*identity.channel,\s*identity.generation", accounted, re.S):
        fail("accounted send must carry exact generation into reliable commit")
    for name in ["send_proxy_pipe_close", "send_pending_proxy_pipe_close"]:
        body = function_body(source, name)
        if "wki_send_on_channel_identity(" not in body or "wki_send(" in body or "wki_channel_get(" in body:
            fail(name + " must use captured allocation without replacement lookup")
    queue = function_body(source, "enqueue_proxy_pipe_close_tx")
    require_order(queue, "pending->channel_identity = identity", "g_pending_proxy_pipe_closes.push_back", "queue owns copied channel identity")
    worker = function_body(source, "proxy_pipe_close_tx_thread_fn")
    if "send_pending_proxy_pipe_close(*pending)" not in worker or "wki_send(" in worker:
        fail("popped close work cannot select a replacement channel")
    socket = function_body(REMOTE_IPC_SOCKET_CPP.read_text(), "proxy_socket_write")
    if "wki_send_on_channel_identity(proxy->data_channel," not in socket or "wki_send(" in socket:
        fail("socket data must use the imported allocation")


def test_original_export_channels_survive_submission_and_cleanup() -> None:
    from exec_source_test import function_body as multiline_body
    source = REMOTE_IPC_CPP.read_text()
    compute = REMOTE_COMPUTE_CPP.read_text()
    export = function_body(source, "wki_ipc_export_task_fds")
    require_order(export, "ipc_channels_live(channels, target_node)", "snapshot.prepare_exports(", "export identity before allocation")
    require_order(export, "exp->channels = channels", "g_ipc_exports.push_back(exp)", "original pair before publication")
    require_order(export, "ipc_peer_cleanup_epoch_locked(target_node) != EXPORT_EPOCH", "g_ipc_exports.push_back(exp)", "epoch before publication")
    match = function_body(source, "wki_ipc_exports_match_submit")
    for token in ["ipc_channels_live(channels, node)", "ipc_channel_identity_equal(channels.control, submit)",
                  "exp->active", "exp->consumer_node == node", "ipc_channel_identity_equal(exp->channels.data, channels.data)"]:
        if token not in match: fail("prepared export matching missing " + token)
    for name in ["wki_task_submit_inline", "wki_task_submit_vfs_ref"]:
        body = multiline_body(compute, name)
        require_order(body, "wki_ipc_exports_match_submit(", "publish_submitted_task_locked(", "same session before submitted-task publication")
        require_order(body, "st.ipc_channels = ipc_guard.channels()", "publish_submitted_task_locked(", "durable pair before publication")
    cleanup = function_body(source, "wki_ipc_cleanup_exported_fds")
    for token in ["{entry.resource_id, channels.data}", "{entry.resource_id, exp->channels.data}",
                  "schedule_pending_pipe_discard(discard.channel, discard.resource_id)"]:
        if token not in cleanup: fail("cleanup lost original DATA identity: " + token)
    if "capture_ipc" in cleanup: fail("missing-export cleanup cannot capture a replacement")
    discard = function_body(source, "schedule_pending_pipe_discard")
    if "capture_ipc" in discard or "wki_channel_get" in discard:
        fail("discard scheduling must receive the original token")
    if "DATA_CHANNEL.channel_id != WKI_CHAN_IPC_DATA" not in discard:
        fail("legacy RESOURCE close cannot invent a DATA identity")
    pump = function_body(source, "pipe_pump_thread_fn")
    require_order(pump, "data_channel = exp->channels.data", "file->fops->vfs_read(", "retain original DATA before read")
    if pump.count("wki_send_on_channel_identity(data_channel,") != 2: fail("both pump DATA/EOF use retained identity")
    receipt = function_body(source, "send_export_pipe_write_flow")
    if "wki_send_on_channel_identity(control," not in receipt: fail("receipts require original RESOURCE")
    handler = function_body(source, "handle_ipc_dev_op_req_inline")
    require_order(handler, "capture_ipc_companion_channel(response_channel, WKI_CHAN_IPC_DATA, WKI_CHAN_RESOURCE)",
                  "send_export_pipe_write_flow(CONTROL,", "missing-export receipt bound to admitted DATA")
    companion = function_body(source, "capture_ipc_companion_channel")
    require_order(companion, "if (!live())", "capture_ipc_channel(", "admitted generation before companion")
    require_order(companion, "capture_ipc_channel(", "return live()", "same generation after companion")
    if re.search(r"\bwki_send\(", source): fail("IPC must not send through numeric replacement lookup")


def test_consumer_workers_retire_before_shutdown_drain() -> None:
    source = REMOTE_IPC_CPP.read_text()
    flush = function_body(source, "export_pipe_write_flush_thread_loop")
    require_order(flush, "hold_kernel_shutdown()", "kernel_threads_shutdown_requested()", "flush shutdown hold")
    require_order(flush, "std::exchange(g_export_pipe_write_flush_tasks.at(shard), nullptr)", "worker->release()", "flush publication retirement")
    require_order(flush, "worker->release()", "g_export_pipe_write_flush_stopped.at(shard).store(true", "flush stopped publication")
    pump = function_body(source, "pipe_pump_thread_fn")
    require_order(pump, "hold_kernel_shutdown()", "kernel_threads_shutdown_requested()", "pump shutdown hold")
    if "ker::vfs::vfs_is_socket_file(file) || ker::vfs::vfs_is_pipe_file(file)" not in pump:
        fail("native pipe readiness races must use the private nonblocking File view")
    require_order(pump, "init_nonblocking_pipe_write_view(view, *file)", "view.fops->vfs_read(&view", "nonblocking native pipe read")
    native_vfs = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    native_pipe_read = native_vfs[native_vfs.index("static auto pipe_read ="):native_vfs.index("static auto pipe_write =")]
    nonblocking = native_pipe_read[native_pipe_read.index("if (f->open_flags & O_NONBLOCK)"):native_pipe_read.index("auto* current_task =")]
    require_order(nonblocking, "st->lock.unlock_irqrestore(IRQF)", "return finish(-EAGAIN)", "native pipe returns without waiting")
    require_order(native_pipe_read, "return finish(-EAGAIN)", "pipe_register_waiter(st->readers_waiting", "native nonblocking branch before wait registration")
    assign = function_body(source, "assign_next_pipe_pump_locked")
    require_order(assign, "kernel_threads_shutdown_requested()", "pop_pipe_pump_queue_locked()", "no successor after global shutdown")
    wake = function_body(source, "wake_export_pipe_write_flush_worker")
    require_order(wake, "try_acquire_lifetime_ref()", "s_ipc_lock.unlock_irqrestore", "flush retained wake")
    require_order(wake, "s_ipc_lock.unlock_irqrestore", "kern_wake(task)", "flush wake outside guard")
    drain = function_body(source, "wki_ipc_secure_shutdown_drain")
    for token in ["g_ipc_shutdown_drain_owner.compare_exchange_strong", "!g_ipc_flush_workers_starting", "arg.starting", "arg.stopped.load", "g_export_pipe_write_flush_stopped.at(shard).load", "g_ipc_exports.size() == 0", "g_export_pipe_write_backlogs.size() == 0"]:
        if token not in drain:
            fail(f"consumer shutdown certificate missing {token!r}")
    require_order(drain, "if (!stopped)", "service_secure_retired_exports()", "stopped exclusive retirement owner")


def main() -> None:
    test_consumer_workers_retire_before_shutdown_drain()
    test_original_export_channels_survive_submission_and_cleanup()
    test_data_and_queued_close_channel_retirement()
    test_ipc_epoch_control_identities()
    test_exported_controls_bind_the_issued_consumer()
    test_pipe_eof_rechecks_data_under_registration_locks()
    test_output_receipts_follow_commit_and_survive_only_for_completion()
    test_poll_wake_drains_all_batches()
    test_proxy_poll_wake_cancels_deferred_switch_without_losing_wake_token()
    test_pipe_pump_read_waiter_uses_event_block_fast_path()
    test_pipe_pump_reuses_worker_stack_frame()
    test_stopped_pipe_pump_publishes_discard_fence()
    test_inactive_proxy_poll_reports_terminal_readiness()
    test_epoll_close_releases_lookup_ref_after_detach()
    test_proxy_lookup_is_peer_scoped()
    test_export_pipe_write_uses_nonmutating_nonblocking_view()
    test_proxy_pipe_write_reuses_bounded_stack_frame()
    test_pipe_fd_open_flags_preserve_nonblocking_access_mode()
    test_pipe_affinity_ignores_write_only_shared_output_endpoints()
    test_write_only_pipe_proxy_omits_receive_ring()
    test_pty_export_data_can_write_from_deferred_worker_without_backlog()
    test_export_snapshot_pins_and_cloexec_are_safe()
    test_export_registry_publication_cannot_allocate()
    test_rdma_preparation_is_private_and_revoked_before_free()
    test_secure_pipe_native_prefix_is_owner_authoritative()
    test_deferred_rdma_write_pins_retirement()
    test_submission_guard_covers_preparation_and_failure_transfer()
    test_export_admission_precedes_side_effects()
    test_pumps_activate_only_after_accepted_handoff()
    test_import_provenance_precedes_all_side_effects()
    test_attach_fd_install_is_transactional()
    test_dev_op_response_cookies_fence_stale_waiters()
    test_peer_cleanup_drains_deferred_dev_op_work()
    test_ipc_dev_ops_are_reserved_before_reliable_ack()
    test_large_deferred_dev_op_payloads_are_coallocated()
    test_large_deferred_dev_op_payloads_transfer_to_export_backlog()
    test_legacy_ipc_controls_require_issued_objects()
    test_export_resource_ids_cannot_wrap_or_partially_publish()
    test_ipc_send_retry_backpressure_sleeps_without_yield_livelock()
    test_retired_exports_fence_and_discard_unattached_deliveries()
    test_socket_close_uses_reliable_proxy_close_queue()
    test_remote_exec_fd_handoff_transfers_only_the_pinned_owner()
    test_ipc_selftests_are_declared_and_registered()
    print("WKI remote IPC source invariants hold")


if __name__ == "__main__":
    main()
