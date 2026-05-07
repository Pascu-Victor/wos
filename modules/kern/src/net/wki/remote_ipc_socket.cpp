#include <cerrno>
#include <cstring>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

#include "remote_ipc.hpp"

namespace ker::net::wki {

// =============================================================================
// Socket-specific proxy fops
//
// TCP/UDP sockets use the existing lock-free RingBuffer rcvbuf.
// TcpCB stays on the home node — retransmission, congestion control, timers
// are irreducible home-node overhead.
//
// Send (remote -> home -> wire):
//   RDMA-write payload directly into home rcvbuf. Doorbell home. Home ISR
//   advances rcvbuf.write_pos and calls tcp_output().
//
// Recv (wire -> home -> remote):
//   NIC NAPI fills home rcvbuf. If owner_pid is remote, doorbell remote.
//   Remote task reads from local shadow or RDMA-reads from home rcvbuf.
//
// Accept: DEV_OP OP_SOCK_ACCEPT (control path, ~0.12ms acceptable)
// Connect/setsockopt/getsockname: DEV_OP (control path only)
// =============================================================================

namespace {

auto proxy_socket_read(ker::vfs::File* f, void* /*buf*/, size_t /*count*/, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active) return -EBADF;

    // Socket proxy not yet implemented — stub returns EIO
    return -EIO;
}

auto proxy_socket_write(ker::vfs::File* f, const void* /*buf*/, size_t /*count*/, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active) return -EBADF;

    // Socket proxy not yet implemented — stub returns EIO
    return -EIO;
}

auto proxy_socket_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) return 0;

    // Send close via DEV_OP
    if (proxy->home_node != WKI_NODE_INVALID) {
        constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
        uint8_t msg[HEADER_SIZE];
        auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
        req->op_id = OP_SOCK_CLOSE;
        req->data_len = sizeof(uint32_t);
        auto* res_id_ptr = reinterpret_cast<uint32_t*>(msg + sizeof(DevOpReqPayload));
        *res_id_ptr = proxy->resource_id;
        wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
    }

    proxy->active = false;
    if (proxy->ring_buf != nullptr) {
        delete[] proxy->ring_buf;
        proxy->ring_buf = nullptr;
    }
    delete proxy;
    f->private_data = nullptr;
    return 0;
}

}  // namespace

// Socket proxy fops are referenced from remote_ipc.cpp when attaching socket fds.
// They are not yet fully wired — the initial implementation focuses on pipes which
// are the source of the cat|wc-l crash.

}  // namespace ker::net::wki
