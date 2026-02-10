#include "tcp.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

// Global TCB list (shared with tcp_timer.cpp)
TcpCB* tcb_list_head = nullptr;
ker::mod::sys::Spinlock tcb_list_lock;

// Monotonic millisecond counter (shared with tcp_timer.cpp)
volatile uint64_t tcp_ms_counter = 0;

namespace {
// TCP binding table (for listening sockets)
struct TcpBinding {
    TcpCB* cb = nullptr;
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
};

TcpBinding tcp_bindings[MAX_TCP_BINDINGS] = {};
ker::mod::sys::Spinlock tcp_bind_lock;

// Ephemeral port counter
uint16_t tcp_ephemeral_port = 49152;

// Simple ISS generator (not cryptographically secure, adequate for a hobby OS)
uint32_t iss_counter = 0x12345678;
auto generate_iss() -> uint32_t {
    iss_counter += 64000;
    iss_counter ^= (iss_counter << 13);
    iss_counter ^= (iss_counter >> 17);
    iss_counter ^= (iss_counter << 5);
    return iss_counter;
}

// Allocate an ephemeral port
auto alloc_ephemeral_port() -> uint16_t { return tcp_ephemeral_port++; }

// TCP socket protocol operations
int tcp_bind(Socket* sock, const void* addr_raw, size_t addr_len) {
    if (addr_len < 8) {
        return -1;
    }

    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -ENOTCONN;
    }

    tcp_bind_lock.lock();

    // Check for existing binding
    if (!sock->reuse_port) {
        for (auto& b : tcp_bindings) {
            if (b.cb != nullptr && b.local_port == port && (b.local_ip == ip || b.local_ip == 0 || ip == 0)) {
                tcp_bind_lock.unlock();
                return -1;  // EADDRINUSE
            }
        }
    }

    // Find free slot
    TcpBinding* slot = nullptr;
    for (auto& b : tcp_bindings) {
        if (b.cb == nullptr) {
            slot = &b;
            break;
        }
    }
    if (slot == nullptr) {
        tcp_bind_lock.unlock();
        return -1;
    }

    slot->cb = cb;
    slot->local_ip = ip;
    slot->local_port = port;

    cb->local_ip = ip;
    cb->local_port = port;
    sock->local_v4.addr = ip;
    sock->local_v4.port = port;
    sock->state = SocketState::BOUND;
    cb->state = TcpState::CLOSED;

    tcp_bind_lock.unlock();
    return 0;
}

int tcp_listen(Socket* sock, int backlog) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -ENOTCONN;
    }

    cb->state = TcpState::LISTEN;
    sock->state = SocketState::LISTENING;
    sock->backlog = (backlog > 0 && backlog < 128) ? backlog : 128;

    return 0;
}

int tcp_accept(Socket* sock, Socket** new_sock_out, void* addr_out, size_t* addr_len) {
    // Check accept queue — if empty, defer task switch and return EAGAIN.
    // Userspace must retry the syscall after being woken by tcp_input.
    sock->lock.lock();
    if (sock->aq_count == 0) {
        sock->lock.unlock();
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            task->deferredTaskSwitch = true;
        }
        return -EAGAIN;
    }

    Socket* child = sock->accept_queue[sock->aq_head];
    sock->accept_queue[sock->aq_head] = nullptr;
    sock->aq_head = (sock->aq_head + 1) % SOCKET_ACCEPT_QUEUE;
    sock->aq_count--;
    sock->lock.unlock();

    // Fill in peer address if requested
    if (addr_out != nullptr && addr_len != nullptr && *addr_len >= 8) {
        auto* addr = static_cast<uint8_t*>(addr_out);
        // sockaddr_in: family(2) + port(2) + addr(4)
        *reinterpret_cast<uint16_t*>(addr) = 2;  // AF_INET
        *reinterpret_cast<uint16_t*>(addr + 2) = htons(child->remote_v4.port);
        *reinterpret_cast<uint32_t*>(addr + 4) = htonl(child->remote_v4.addr);
        *addr_len = 8;
    }

    *new_sock_out = child;
    return 0;
}

int tcp_connect(Socket* sock, const void* addr_raw, size_t addr_len) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -1;
    }

    // Already connected
    if (cb->state == TcpState::ESTABLISHED) {
        sock->state = SocketState::CONNECTED;
        return 0;
    }

    // Connect in progress
    if (cb->state == TcpState::SYN_SENT) {
        if (sock->nonblock) {
            return -EINPROGRESS;
        }
        // Blocking connect: wait until handshake completes or fails
        for (;;) {
            if (cb->state == TcpState::ESTABLISHED) {
                sock->state = SocketState::CONNECTED;
                return 0;
            }
            if (cb->state == TcpState::CLOSED) {
                return -ECONNREFUSED;
            }
            ker::mod::sched::kern_yield();
        }
    }

    // Connection failed (RST received while we were waiting)
    if (cb->state == TcpState::CLOSED && sock->state == SocketState::CONNECTING) {
        return -ECONNREFUSED;
    }

    // First connect call — initiate connection
    if (addr_len < 8) {
        return -1;
    }

    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    // Auto-bind if not already bound
    if (cb->local_port == 0) {
        cb->local_port = alloc_ephemeral_port();
        sock->local_v4.port = cb->local_port;
    }

    cb->remote_ip = ip;
    cb->remote_port = port;
    sock->remote_v4.addr = ip;
    sock->remote_v4.port = port;

    // Generate ISS and send SYN
    cb->iss = generate_iss();
    cb->snd_una = cb->iss;
    cb->snd_nxt = cb->iss + 1;
    cb->rcv_wnd = SOCKET_BUF_SIZE;
    cb->state = TcpState::SYN_SENT;
    sock->state = SocketState::CONNECTING;

    tcp_send_segment(cb, TCP_SYN, nullptr, 0);

    if (sock->nonblock) {
        return -EINPROGRESS;
    }

    // Blocking connect: wait for SYN-ACK / RST
    for (;;) {
        if (cb->state == TcpState::ESTABLISHED) {
            sock->state = SocketState::CONNECTED;
            return 0;
        }
        if (cb->state == TcpState::CLOSED) {
            return -ECONNREFUSED;
        }
        ker::mod::sched::kern_yield();
    }
}

auto tcp_send(Socket* sock, const void* buf, size_t len, int) -> ssize_t {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr || cb->state != TcpState::ESTABLISHED) {
        return -1;
    }

    const auto* data = static_cast<const uint8_t*>(buf);
    size_t sent = 0;

    while (sent < len) {
        cb->lock.lock();

        // Re-check state under lock (may have changed concurrently)
        if (cb->state != TcpState::ESTABLISHED) {
            cb->lock.unlock();
            return (sent > 0) ? static_cast<ssize_t>(sent) : -1;
        }

        // Send in MSS-sized chunks
        size_t chunk = len - sent;
        chunk = std::min<size_t>(chunk, cb->snd_mss);

        // Limit by send window
        uint32_t window = cb->snd_wnd;
        if (window == 0) {
            window = 1;  // Persist probe
        }
        uint32_t in_flight = cb->snd_nxt - cb->snd_una;
        if (in_flight >= window) {
            cb->lock.unlock();
            // Window full — if we already sent some data, return partial.
            // Otherwise defer and return EAGAIN so userspace retries.
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->deferredTaskSwitch = true;
            }
            return -EAGAIN;
        }
        uint32_t available = window - in_flight;
        if (chunk > available) {
            chunk = available;
        }

        bool ok = tcp_send_segment(cb, TCP_ACK | TCP_PSH, data + sent, chunk);
        cb->lock.unlock();

        if (!ok) {
            // Buffer exhaustion — return partial or EAGAIN
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->deferredTaskSwitch = true;
            }
            return -EAGAIN;
        }
        sent += chunk;
    }

    return static_cast<ssize_t>(sent);
}

auto tcp_recv(Socket* sock, void* buf, size_t len, int) -> ssize_t {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -1;
    }

    // If data available, read it immediately
    if (sock->rcvbuf.available() > 0) {
        ssize_t n = sock->rcvbuf.read(buf, len);
        if (n > 0) {
            // Update receive window and send window update if it
            // opened significantly (>= MSS).  This unblocks the
            // sender when it was stalled on a zero/small window.
            uint32_t old_wnd = cb->rcv_wnd;
            cb->rcv_wnd = sock->rcvbuf.free_space();
            if (cb->rcv_wnd >= cb->rcv_mss && old_wnd < cb->rcv_mss) {
                tcp_send_ack(cb);
            }
        }
        return n;
    }

    // No data — check connection state.
    // After we have sent our FIN (FIN_WAIT_*) and the peer has also
    // closed (TIME_WAIT, CLOSING), or the peer closed first
    // (CLOSE_WAIT), there will be no more data — return EOF.
    if (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED || cb->state == TcpState::TIME_WAIT ||
        cb->state == TcpState::CLOSING || cb->state == TcpState::LAST_ACK) {
        return 0;  // EOF
    }

    if (cb->state != TcpState::ESTABLISHED && cb->state != TcpState::FIN_WAIT_1 && cb->state != TcpState::FIN_WAIT_2) {
        return -EAGAIN;  // Not in a readable state yet
    }

    // Nonblocking sockets: report would-block immediately.
    if (sock->nonblock) {
        return -EAGAIN;
    }

    // Blocking sockets: sleep until data arrives or the peer closes.
    for (;;) {
        if (sock->rcvbuf.available() > 0) {
            ssize_t n = sock->rcvbuf.read(buf, len);
            if (n > 0) {
                uint32_t old_wnd = cb->rcv_wnd;
                cb->rcv_wnd = sock->rcvbuf.free_space();
                if (cb->rcv_wnd >= cb->rcv_mss && old_wnd < cb->rcv_mss) {
                    tcp_send_ack(cb);
                }
            }
            return n;
        }
        if (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED || cb->state == TcpState::TIME_WAIT ||
            cb->state == TcpState::CLOSING || cb->state == TcpState::LAST_ACK) {
            return 0;  // EOF while waiting
        }
        ker::mod::sched::kern_yield();
    }
}

auto tcp_sendto(Socket* sock, const void* buf, size_t len, int flags, const void*, size_t) -> ssize_t {
    return tcp_send(sock, buf, len, flags);
}

auto tcp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void*, size_t*) -> ssize_t { return tcp_recv(sock, buf, len, flags); }

void tcp_close_op(Socket* sock) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return;
    }

    cb->lock.lock();
    // Drain any outstanding retransmit entries — no further retransmissions
    // after close() regardless of state.
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
    }
    switch (cb->state) {
        case TcpState::CLOSED:
        case TcpState::LISTEN:
            cb->state = TcpState::CLOSED;
            break;

        case TcpState::SYN_SENT:
            cb->state = TcpState::CLOSED;
            break;

        case TcpState::ESTABLISHED:
            if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
                cb->state = TcpState::FIN_WAIT_1;
            } else {
                // Buffer exhaustion — force RST and close
                cb->state = TcpState::CLOSED;
            }
            break;

        case TcpState::CLOSE_WAIT:
            if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
                cb->state = TcpState::LAST_ACK;
            } else {
                cb->state = TcpState::CLOSED;
            }
            break;

        case TcpState::FIN_WAIT_1:
        case TcpState::FIN_WAIT_2:
        case TcpState::CLOSING:
            // FIN already sent by a prior shutdown(); nothing more to send.
            // The TCB will transition to TIME_WAIT when the peer ACKs/FINs.
            break;

        case TcpState::TIME_WAIT:
            // Already in TIME_WAIT; timer thread will free the TCB.
            break;

        default:
            break;
    }
    cb->lock.unlock();

    // Remove binding
    tcp_bind_lock.lock();
    for (auto& b : tcp_bindings) {
        if (b.cb == cb) {
            b.cb = nullptr;
            b.local_ip = 0;
            b.local_port = 0;
        }
    }
    tcp_bind_lock.unlock();

    // Detach the socket from the TCB.  For states where the TCP
    // connection is still winding down (FIN_WAIT_*, TIME_WAIT, etc.)
    // the TCB stays on the global list so the timer / input path can
    // finish the handshake, but the socket object is freed by the
    // caller.  For CLOSED, free the TCB right away.
    sock->proto_data = nullptr;
    cb->socket = nullptr;
    if (cb->state == TcpState::CLOSED) {
        tcp_free_cb(cb);
    }
}

int tcp_shutdown_op(Socket* sock, int how) {
    (void)how;
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -1;
    }

    cb->lock.lock();
    if (cb->state == TcpState::ESTABLISHED) {
        if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
            cb->state = TcpState::FIN_WAIT_1;
        }
    } else if (cb->state == TcpState::CLOSE_WAIT) {
        if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
            cb->state = TcpState::LAST_ACK;
        }
    }
    cb->lock.unlock();
    return 0;
}

int tcp_setsockopt_op(Socket* sock, int, int optname, const void* optval, size_t optlen) {
    if (optname == 2 && optlen >= sizeof(int)) {  // SO_REUSEADDR
        sock->reuse_addr = *static_cast<const int*>(optval) != 0;
    }
    if (optname == 15 && optlen >= sizeof(int)) {  // SO_REUSEPORT
        sock->reuse_port = *static_cast<const int*>(optval) != 0;
    }
    return 0;
}

int tcp_getsockopt_op(Socket*, int, int, void*, size_t*) { return 0; }

int tcp_poll_check_op(Socket* sock, int events) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    int ready = 0;

    if ((events & 1) != 0) {  // POLLIN
        if (sock->rcvbuf.available() > 0) {
            ready |= 1;
        }
        // Accept queue has connections
        if (cb != nullptr && cb->state == TcpState::LISTEN && sock->aq_count > 0) {
            ready |= 1;
        }
        // Connection closed (EOF readable)
        if (cb != nullptr && (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED)) {
            ready |= 1;
        }
    }
    if ((events & 4) != 0 && sock->sndbuf.free_space() > 0) {  // POLLOUT
        ready |= 4;
    }
    return ready;
}

SocketProtoOps tcp_ops = {
    .bind = tcp_bind,
    .listen = tcp_listen,
    .accept = tcp_accept,
    .connect = tcp_connect,
    .send = tcp_send,
    .recv = tcp_recv,
    .sendto = tcp_sendto,
    .recvfrom = tcp_recvfrom,
    .close = tcp_close_op,
    .shutdown = tcp_shutdown_op,
    .setsockopt = tcp_setsockopt_op,
    .getsockopt = tcp_getsockopt_op,
    .poll_check = tcp_poll_check_op,
};
}  // namespace

auto get_tcp_proto_ops() -> SocketProtoOps* { return &tcp_ops; }

auto tcp_now_ms() -> uint64_t { return tcp_ms_counter; }

auto tcp_alloc_cb() -> TcpCB* {
    auto* cb = static_cast<TcpCB*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(TcpCB)));
    if (cb == nullptr) {
        return nullptr;
    }

    cb->refcnt.store(1, std::memory_order_relaxed);

    tcb_list_lock.lock();
    cb->next = tcb_list_head;
    tcb_list_head = cb;
    tcb_list_lock.unlock();

    return cb;
}

void tcp_cb_acquire(TcpCB* cb) {
    if (cb == nullptr) {
        return;
    }
    cb->refcnt.fetch_add(1, std::memory_order_acq_rel);
}

namespace {
void tcp_cb_destroy(TcpCB* cb) {
    if (cb == nullptr) {
        return;
    }

    // Free retransmit queue
    auto* entry = cb->retransmit_head;
    while (entry != nullptr) {
        auto* next = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
        entry = next;
    }

    ker::mod::mm::dyn::kmalloc::free(cb);
}
}  // namespace

void tcp_cb_release(TcpCB* cb) {
    if (cb == nullptr) {
        return;
    }
    if (cb->refcnt.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        tcp_cb_destroy(cb);
    }
}

void tcp_free_cb(TcpCB* cb) {
    if (cb == nullptr) {
        return;
    }

    // Remove from global list
    bool removed = false;
    tcb_list_lock.lock();
    TcpCB** pp = &tcb_list_head;
    while (*pp != nullptr) {
        if (*pp == cb) {
            *pp = cb->next;
            removed = true;
            break;
        }
        pp = &(*pp)->next;
    }
    tcb_list_lock.unlock();

    // Drop the list's reference if we removed it
    if (removed) {
        tcp_cb_release(cb);
    }
}

auto tcp_find_cb(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> TcpCB* {
    tcb_list_lock.lock();
    for (auto* cb = tcb_list_head; cb != nullptr; cb = cb->next) {
        if (cb->local_port == local_port && cb->remote_port == remote_port && (cb->local_ip == local_ip || cb->local_ip == 0) &&
            cb->remote_ip == remote_ip) {
            tcp_cb_acquire(cb);
            tcb_list_lock.unlock();
            return cb;
        }
    }
    tcb_list_lock.unlock();
    return nullptr;
}

auto tcp_find_listener(uint32_t local_ip, uint16_t local_port) -> TcpCB* {
    tcb_list_lock.lock();
    for (auto* cb = tcb_list_head; cb != nullptr; cb = cb->next) {
        if (cb->state == TcpState::LISTEN && cb->local_port == local_port && (cb->local_ip == local_ip || cb->local_ip == 0)) {
            tcb_list_lock.unlock();
            return cb;
        }
    }
    tcb_list_lock.unlock();
    return nullptr;
}

}  // namespace ker::net::proto
