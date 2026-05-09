#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <net/endian.hpp>
#include <net/socket.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

#include "remote_ipc.hpp"

namespace ker::net::wki {

namespace {

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

// Fire-and-forget control op (no response needed, e.g. OP_SOCK_CLOSE).
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

    int tx = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    return tx == WKI_OK ? 0 : -EIO;
}

// Synchronous control op — sends request with optional extra payload, waits for
// response, copies response data into resp_buf up to resp_max bytes.
// Returns 0 on success or a negative errno.
auto send_socket_op_sync(ProxyIpcState* proxy, uint16_t op_id, const void* extra, uint16_t extra_len, void* resp_buf, uint16_t resp_max)
    -> int;

auto send_socket_op_sync(ProxyIpcState* proxy, uint16_t op_id, const void* extra, uint16_t extra_len, void* resp_buf, uint16_t resp_max,
                         uint16_t* resp_len_out) -> int {
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return -EBADF;
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return -EHOSTUNREACH;
    }

    // Build request: [DevOpReqPayload][resource_id:u32][extra...]
    constexpr size_t RID_SIZE = sizeof(uint32_t);
    const size_t total_data = RID_SIZE + extra_len;
    const size_t msg_size = sizeof(DevOpReqPayload) + total_data;

    auto* msg = new (std::nothrow) uint8_t[msg_size];
    if (msg == nullptr) return -ENOMEM;

    auto* req_hdr = reinterpret_cast<DevOpReqPayload*>(msg);
    req_hdr->op_id = op_id;
    req_hdr->data_len = static_cast<uint16_t>(total_data);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, RID_SIZE);
    if (extra != nullptr && extra_len > 0) {
        std::memcpy(msg + sizeof(DevOpReqPayload) + RID_SIZE, extra, extra_len);
    }

    // Register wait entry on the proxy (serialised by proxy->lock).
    WkiWaitEntry wait = {};
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait != nullptr) {
        // Another in-flight control op — unlikely, return busy.
        proxy->lock.unlock_irqrestore(irqf);
        delete[] msg;
        return -EBUSY;
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = op_id;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    int tx = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(msg_size));
    delete[] msg;

    if (tx != WKI_OK) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return -EIO;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (wait_rc != 0) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return -ETIMEDOUT;
    }

    irqf = proxy->lock.lock_irqsave();
    int status = proxy->pending_wait_status;
    uint16_t resp_len = proxy->pending_wait_resp_len;
    if (resp_buf != nullptr && resp_max > 0 && resp_len > 0) {
        uint16_t copy_len = resp_len < resp_max ? resp_len : resp_max;
        std::memcpy(resp_buf, proxy->pending_wait_resp, copy_len);
    }
    if (resp_len_out != nullptr) {
        *resp_len_out = resp_len;
    }
    proxy->pending_wait = nullptr;
    proxy->lock.unlock_irqrestore(irqf);

    return status;
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
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return -EBADF;
    if (proxy->ring_buf == nullptr) return -EIO;

    for (;;) {
        for (int spin = 0; spin < 2000; spin++) {
            uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
            uint32_t tail = proxy->ring_tail.load(std::memory_order_relaxed);
            uint32_t avail = head - tail;
            if (avail > 0) {
                uint32_t to_read = (count < avail) ? static_cast<uint32_t>(count) : avail;
                uint32_t cap = proxy->ring_capacity;
                uint32_t ring_tail = tail % cap;
                auto* dst = static_cast<uint8_t*>(buf);
                uint32_t first = cap - ring_tail;
                if (first >= to_read) {
                    std::memcpy(dst, proxy->ring_buf + ring_tail, to_read);
                } else {
                    std::memcpy(dst, proxy->ring_buf + ring_tail, first);
                    std::memcpy(dst + first, proxy->ring_buf, to_read - first);
                }
                proxy->ring_tail.store(tail + to_read, std::memory_order_release);
                return static_cast<ssize_t>(to_read);
            }
            if (proxy->write_closed.load(std::memory_order_acquire)) return 0;
            asm volatile("pause" ::: "memory");
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) return -ESRCH;

        uint64_t irqf = proxy->lock.lock_irqsave();
        uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t tail = proxy->ring_tail.load(std::memory_order_relaxed);
        if (head != tail) {
            proxy->lock.unlock_irqrestore(irqf);
            continue;
        }
        if (proxy->write_closed.load(std::memory_order_acquire)) {
            proxy->lock.unlock_irqrestore(irqf);
            return 0;
        }
        task->wait_channel = "wki_proxy_sock";
        task->deferred_task_switch = true;
        proxy->blocked_reader.store(task, std::memory_order_release);
        proxy->lock.unlock_irqrestore(irqf);
        return -512;  // ERESTARTSYS
    }
}

auto proxy_socket_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return -EBADF;

    // Forward data to home node via OP_PIPE_DATA (home writes it into socket send path)
    constexpr size_t MAX_CHUNK = 4096;
    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    size_t to_send = (count < MAX_CHUNK) ? count : MAX_CHUNK;
    size_t msg_size = HEADER_SIZE + to_send;

    auto* msg = new (std::nothrow) uint8_t[msg_size];
    if (msg == nullptr) return -ENOMEM;

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PIPE_DATA;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + to_send);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    std::memcpy(msg + HEADER_SIZE, buf, to_send);

    int ret = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(msg_size));
    delete[] msg;

    if (ret != WKI_OK) return -EIO;
    return static_cast<ssize_t>(to_send);
}

auto proxy_socket_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) return 0;
    (void)send_socket_op(proxy, OP_SOCK_CLOSE);
    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

auto proxy_socket_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return 0;

    int ready = 0;
    uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
    uint32_t tail = proxy->ring_tail.load(std::memory_order_acquire);
    uint32_t avail = head - tail;
    bool closed = proxy->write_closed.load(std::memory_order_acquire);

    if ((events & 0x0001) != 0 && (avail > 0 || closed)) ready |= 0x0001;  // POLLIN
    if ((events & 0x0004) != 0) ready |= 0x0004;                           // POLLOUT always ready
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

    int32_t how32 = static_cast<int32_t>(how);
    return send_socket_op_sync(proxy, OP_SOCK_SHUTDOWN, &how32, sizeof(how32), nullptr, 0);
}

auto wki_ipc_socket_getpeername(ker::vfs::File* file, void* addr_out, size_t* addr_len) -> int {
    auto* proxy = socket_proxy_from_file(file);
    if (proxy == nullptr) {
        return -ENOTSOCK;
    }

    PeerAddrWire peer = {};
    uint16_t resp_len = 0;
    int rc = send_socket_op_sync(proxy, OP_SOCK_GETPEERNAME, nullptr, 0, &peer, sizeof(peer), &resp_len);
    if (rc != 0) {
        return rc;
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
    uint16_t resp_cap = *optlen < static_cast<size_t>(std::numeric_limits<uint16_t>::max()) ? static_cast<uint16_t>(*optlen)
                                                                                            : std::numeric_limits<uint16_t>::max();
    int rc = send_socket_op_sync(proxy, OP_SOCK_GETSOCKOPT, req.data(), static_cast<uint16_t>(sizeof(req)), optval, resp_cap, &resp_len);
    if (rc != 0) {
        return rc;
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

    uint16_t extra_len = static_cast<uint16_t>(ARG_HEADER_SIZE + optlen);
    auto* extra = new (std::nothrow) uint8_t[extra_len];
    if (extra == nullptr) {
        return -ENOMEM;
    }

    int32_t level32 = static_cast<int32_t>(level);
    int32_t optname32 = static_cast<int32_t>(optname);
    std::memcpy(extra, &level32, sizeof(level32));
    std::memcpy(extra + sizeof(level32), &optname32, sizeof(optname32));
    if (optval != nullptr && optlen > 0) {
        std::memcpy(extra + ARG_HEADER_SIZE, optval, optlen);
    }

    int rc = send_socket_op_sync(proxy, OP_SOCK_SETSOCKOPT, extra, extra_len, nullptr, 0);
    delete[] extra;
    return rc;
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

void wki_ipc_socket_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    (void)src_node;
    (void)channel;
    if (payload == nullptr || len < sizeof(ker::net::wki::DevOpRespPayload)) {
        return;
    }

    // Socket control responses are intentionally a no-op for now.
    // Follow-up milestones will match responses to waiting socket proxy ops.
}
