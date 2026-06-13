#include <bits/ssize_t.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <net/socket.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

#include "remote_ipc.hpp"

namespace ker::net::wki {

namespace {

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return task->has_interrupting_signal_pending();
}

void perf_record_ipc_event(uint8_t op, ker::mod::perf::WkiPerfPhase phase, uint16_t peer, uint16_t channel, uint32_t correlation,
                           int32_t status, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, phase, peer,
                                     channel, correlation, status, aux, callsite);
}

void perf_record_ipc_summary(uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us, uint64_t bytes = 0) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, peer, channel, status, latency_us, true, 0, bytes);
}

struct __attribute__((packed)) PeerAddrWire {
    uint16_t family;
    uint32_t addr;
    uint16_t port;
};

static_assert(sizeof(PeerAddrWire) == 8, "PeerAddrWire must match OP_SOCK_GETPEERNAME response");

auto socket_proxy_from_file(ker::vfs::File* file) -> ProxyIpcState* {
    if (file == nullptr || file->fops != &g_proxy_socket_fops || file->private_data == nullptr) {
        return nullptr;
    }
    return static_cast<ProxyIpcState*>(file->private_data);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Fire-and-forget stream op (no response needed, e.g. OP_SOCK_CLOSE).
auto send_socket_op(ProxyIpcState* proxy, uint16_t op_id) -> int {
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return -EBADF;
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return -EHOSTUNREACH;
    }

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::array<uint8_t, HEADER_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = op_id;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    int const TX = wki_send(proxy->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    return TX == WKI_OK ? 0 : -EIO;
}

// Synchronous control op — sends request with optional extra payload, waits for
// response, copies response data into resp_buf up to resp_max bytes.
// Returns 0 on success or a negative errno.
auto send_socket_op_sync(ProxyIpcState* proxy, uint16_t op_id, const void* extra, uint16_t extra_len, void* resp_buf, uint16_t resp_max)
    -> int;

auto send_socket_op_sync(ProxyIpcState* proxy, uint16_t op_id, const void* extra, uint16_t extra_len, void* resp_buf, uint16_t resp_max,
                         uint16_t* resp_len_out) -> int {
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    auto finish = [&](int rc, uint64_t bytes = 0) -> int {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::SOCK_CTRL), ker::mod::perf::WkiPerfPhase::END,
                              proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, rc, ELAPSED_US,
                              CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::SOCK_CTRL),
                                proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, rc, ELAPSED_US, bytes);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::SOCK_CTRL), ker::mod::perf::WkiPerfPhase::BEGIN,
                          proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, op_id, extra_len,
                          CALLSITE);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return finish(-EHOSTUNREACH);
    }

    // Build request: [DevOpReqPayload][resource_id:u32][cookie:u16][extra...]
    constexpr size_t RID_SIZE = sizeof(uint32_t);
    const size_t TOTAL_DATA = RID_SIZE + WKI_IPC_OP_COOKIE_BYTES + extra_len;
    const size_t MSG_SIZE = sizeof(DevOpReqPayload) + TOTAL_DATA;

    auto* msg = new (std::nothrow) uint8_t[MSG_SIZE];
    if (msg == nullptr) {
        return finish(-ENOMEM);
    }

    auto* req_hdr = reinterpret_cast<DevOpReqPayload*>(msg);
    req_hdr->op_id = op_id;
    req_hdr->data_len = static_cast<uint16_t>(TOTAL_DATA);
    // Register wait entry on the proxy (serialised by proxy->lock).
    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait_op != 0) {
        // Another in-flight control op — unlikely, return busy.
        proxy->lock.unlock_irqrestore(irqf);
        delete[] msg;
        return finish(-EBUSY);
    }
    op_cookie = wki_ipc_allocate_wait_cookie_locked(proxy);
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = op_id;
    proxy->pending_wait_cookie = op_cookie;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, RID_SIZE);
    std::memcpy(msg + sizeof(DevOpReqPayload) + RID_SIZE, &op_cookie, WKI_IPC_OP_COOKIE_BYTES);
    if (extra != nullptr && extra_len > 0) {
        std::memcpy(msg + sizeof(DevOpReqPayload) + RID_SIZE + WKI_IPC_OP_COOKIE_BYTES, extra, extra_len);
    }

    int const TX = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    delete[] msg;

    if (TX != WKI_OK) {
        wki_ipc_cancel_pending_wait(proxy, &wait, WKI_ERR_TX_FAILED);
        return finish(-EIO);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        wki_ipc_cancel_pending_wait(proxy, &wait, -ETIMEDOUT);
        return finish(-ETIMEDOUT);
    }

    uint16_t resp_len = 0;
    int const STATUS = wki_ipc_consume_pending_wait_response(proxy, &wait, op_id, op_cookie, resp_buf, resp_max, &resp_len);
    if (resp_len_out != nullptr) {
        *resp_len_out = resp_len;
    }
    return finish(STATUS, resp_len);
}

auto send_socket_op_sync(ProxyIpcState* proxy, uint16_t op_id, const void* extra, uint16_t extra_len, void* resp_buf, uint16_t resp_max)
    -> int {
    return send_socket_op_sync(proxy, op_id, extra, extra_len, resp_buf, resp_max, nullptr);
}

// ---------------------------------------------------------------------------
// File operations
// ---------------------------------------------------------------------------

auto proxy_socket_read(ker::vfs::File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_READ), ker::mod::perf::WkiPerfPhase::END,
                              proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, STATUS, ELAPSED_US,
                              CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_READ),
                                proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, STATUS, ELAPSED_US, bytes);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_READ), ker::mod::perf::WkiPerfPhase::BEGIN,
                          proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, 0, 0, CALLSITE);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (proxy->ring_buf == nullptr) {
        return finish(-EIO);
    }

    for (;;) {
        for (int spin = 0; spin < 2000; spin++) {
            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
            uint32_t const AVAIL = HEAD - TAIL;
            if (AVAIL > 0) {
                uint32_t const TO_READ = (count < AVAIL) ? static_cast<uint32_t>(count) : AVAIL;
                uint32_t const CAP = proxy->ring_capacity;
                uint32_t const RING_TAIL = TAIL % CAP;
                auto* dst = static_cast<uint8_t*>(buf);
                uint32_t const FIRST = CAP - RING_TAIL;
                if (FIRST >= TO_READ) {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, TO_READ);
                } else {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, FIRST);
                    std::memcpy(dst + FIRST, proxy->ring_buf, TO_READ - FIRST);
                }
                proxy->ring_tail.store(TAIL + TO_READ, std::memory_order_release);
                return finish(static_cast<ssize_t>(TO_READ), TO_READ);
            }
            if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
                return finish(0);
            }
            asm volatile("pause" ::: "memory");
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return finish(-ESRCH);
        }

        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
        if (HEAD != TAIL) {
            proxy->lock.unlock_irqrestore(IRQF);
            continue;
        }
        if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
            proxy->lock.unlock_irqrestore(IRQF);
            return finish(0);
        }
        if (current_task_has_deliverable_signal()) {
            proxy->lock.unlock_irqrestore(IRQF);
            return finish(-EINTR);
        }
        task->wait_channel = "wki_proxy_sock";
        proxy->blocked_reader.store(task, std::memory_order_release);
        proxy->lock.unlock_irqrestore(IRQF);
        ker::mod::sched::preemptible_syscall_park("wki_proxy_sock");
        if (current_task_has_deliverable_signal()) {
            auto* expected = task;
            proxy->blocked_reader.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel, std::memory_order_acquire);
            return finish(-EINTR);
        }
    }
}

auto proxy_socket_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE), ker::mod::perf::WkiPerfPhase::END,
                              proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_IPC_DATA, CORRELATION, STATUS, ELAPSED_US,
                              CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE),
                                proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_IPC_DATA, STATUS, ELAPSED_US, bytes);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE), ker::mod::perf::WkiPerfPhase::BEGIN,
                          proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_IPC_DATA, CORRELATION, 0, 0, CALLSITE);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }

    // Forward data to home node via OP_PIPE_DATA (home writes it into socket send path)
    constexpr size_t MAX_CHUNK = 8192;
    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    size_t const TO_SEND = (count < MAX_CHUNK) ? count : MAX_CHUNK;
    size_t const MSG_SIZE = HEADER_SIZE + TO_SEND;

    auto* msg = new (std::nothrow) uint8_t[MSG_SIZE];
    if (msg == nullptr) {
        return finish(-ENOMEM);
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PIPE_DATA;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + TO_SEND);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    std::memcpy(msg + HEADER_SIZE, buf, TO_SEND);

    int const RET = wki_send(proxy->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    delete[] msg;

    if (RET != WKI_OK) {
        return finish(-EIO);
    }
    return finish(static_cast<ssize_t>(TO_SEND), TO_SEND);
}

auto proxy_socket_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }
    (void)send_socket_op(proxy, OP_SOCK_CLOSE);
    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

auto proxy_socket_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }
    if (!proxy->active.load(std::memory_order_acquire)) {
        return 0x0008 | 0x0010;  // POLLERR | POLLHUP
    }

    int ready = 0;
    uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
    uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
    uint32_t const AVAIL = HEAD - TAIL;
    bool const CLOSED = proxy->write_closed.load(std::memory_order_acquire) != 0U;

    if ((events & 0x0001) != 0 && (AVAIL > 0 || CLOSED)) {
        ready |= 0x0001;  // POLLIN
    }
    if ((events & 0x0004) != 0) {
        ready |= 0x0004;  // POLLOUT always ready
    }
    return ready;
}

auto proxy_socket_poll_register_waiter(ker::vfs::File* f, uint64_t pid) -> bool {
    auto* proxy = socket_proxy_from_file(f);
    return wki_ipc_proxy_register_poll_waiter(proxy, pid);
}

}  // namespace

auto wki_ipc_is_socket_proxy_file(const ker::vfs::File* file) -> bool {
    return file != nullptr && file->fops == &g_proxy_socket_fops && file->private_data != nullptr;
}

auto wki_ipc_socket_shutdown(ker::vfs::File* file, int how) -> int {
    auto* proxy = socket_proxy_from_file(file);
    if (proxy == nullptr) {
        return -ENOTSOCK;
    }

    auto how32 = static_cast<int32_t>(how);
    return send_socket_op_sync(proxy, OP_SOCK_SHUTDOWN, &how32, sizeof(how32), nullptr, 0);
}

auto wki_ipc_socket_getpeername(ker::vfs::File* file, void* addr_out, size_t* addr_len) -> int {
    auto* proxy = socket_proxy_from_file(file);
    if (proxy == nullptr) {
        return -ENOTSOCK;
    }

    PeerAddrWire peer = {};
    uint16_t resp_len = 0;
    int const RC = send_socket_op_sync(proxy, OP_SOCK_GETPEERNAME, nullptr, 0, &peer, sizeof(peer), &resp_len);
    if (RC != 0) {
        return RC;
    }
    if (resp_len < sizeof(peer)) {
        return -EIO;
    }
    if (peer.family != 2) {
        return -EAFNOSUPPORT;
    }
    if (addr_out == nullptr) {
        return -EINVAL;
    }

    size_t max_len = ker::net::SOCKADDR_V4_LEN;
    if (addr_len != nullptr) {
        max_len = *addr_len;
    }
    if (!ker::net::socket_fill_sockaddr_v4(addr_out, max_len, addr_len, peer.addr, peer.port)) {
        return -EINVAL;
    }
    return 0;
}

auto wki_ipc_socket_getsockopt(ker::vfs::File* file, int level, int optname, void* optval, size_t* optlen) -> int {
    auto* proxy = socket_proxy_from_file(file);
    if (proxy == nullptr) {
        return -ENOTSOCK;
    }
    if (optlen == nullptr) {
        return -EINVAL;
    }

    std::array<int32_t, 2> req = {
        static_cast<int32_t>(level),
        static_cast<int32_t>(optname),
    };
    uint16_t resp_len = 0;
    uint16_t const RESP_CAP = *optlen < static_cast<size_t>(std::numeric_limits<uint16_t>::max()) ? static_cast<uint16_t>(*optlen)
                                                                                                  : std::numeric_limits<uint16_t>::max();
    int const RC =
        send_socket_op_sync(proxy, OP_SOCK_GETSOCKOPT, req.data(), static_cast<uint16_t>(sizeof(req)), optval, RESP_CAP, &resp_len);
    if (RC != 0) {
        return RC;
    }

    *optlen = resp_len;
    return 0;
}

auto wki_ipc_socket_setsockopt(ker::vfs::File* file, int level, int optname, const void* optval, size_t optlen) -> int {
    auto* proxy = socket_proxy_from_file(file);
    if (proxy == nullptr) {
        return -ENOTSOCK;
    }

    constexpr size_t ARG_HEADER_SIZE = 2 * sizeof(int32_t);
    if (optlen > static_cast<size_t>(std::numeric_limits<uint16_t>::max()) - ARG_HEADER_SIZE) {
        return -EINVAL;
    }

    auto const EXTRA_LEN = static_cast<uint16_t>(ARG_HEADER_SIZE + optlen);
    auto* extra = new (std::nothrow) uint8_t[EXTRA_LEN];
    if (extra == nullptr) {
        return -ENOMEM;
    }

    auto level32 = static_cast<int32_t>(level);
    auto optname32 = static_cast<int32_t>(optname);
    std::memcpy(extra, &level32, sizeof(level32));
    std::memcpy(extra + sizeof(level32), &optname32, sizeof(optname32));
    if (optval != nullptr && optlen > 0) {
        std::memcpy(extra + ARG_HEADER_SIZE, optval, optlen);
    }

    int const RC = send_socket_op_sync(proxy, OP_SOCK_SETSOCKOPT, extra, EXTRA_LEN, nullptr, 0);
    delete[] extra;
    return RC;
}

// ---------------------------------------------------------------------------
// Public: socket-specific DEV_OP_RESP handler
// NOTE: This function is intentionally NOT implemented here because it needs
// access to s_ipc_lock and g_ipc_proxies which live in remote_ipc.cpp.
// The implementation is in remote_ipc.cpp as wki_ipc_socket_handle_dev_op_resp().
// ---------------------------------------------------------------------------

ker::vfs::FileOperations g_proxy_socket_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_socket_close,
    .vfs_read = proxy_socket_read,
    .vfs_write = proxy_socket_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_socket_poll_check,
    .vfs_poll_register_waiter = proxy_socket_poll_register_waiter,
    .vfs_ioctl = nullptr,
};

}  // namespace ker::net::wki
