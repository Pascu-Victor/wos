#include "tcp.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/netif.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/route.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

// Per-bucket hash tables.
TcpHashBucket tcb_hash[TCB_HASH_SIZE] = {};
TcpHashBucket listener_hash[LISTENER_HASH_SIZE] = {};

volatile uint64_t tcp_ms_counter = 0;

namespace {
struct TcpBinding {
    TcpCB* cb = nullptr;
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
};

TcpBinding tcp_bindings[MAX_TCP_BINDINGS] = {};
ker::mod::sys::Spinlock tcp_bind_lock;

uint16_t tcp_ephemeral_port = 49152;

// Simple ISS generator.
uint32_t iss_counter = 0x12345678;
auto generate_iss() -> uint32_t {
    iss_counter += 64000;
    iss_counter ^= (iss_counter << 13);
    iss_counter ^= (iss_counter >> 17);
    iss_counter ^= (iss_counter << 5);
    return iss_counter;
}

auto alloc_ephemeral_port() -> uint16_t { return tcp_ephemeral_port++; }

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

    if (!sock->reuse_port) {
        for (auto& b : tcp_bindings) {
            if (b.cb != nullptr && b.local_port == port && (b.local_ip == ip || b.local_ip == 0 || ip == 0)) {
                tcp_bind_lock.unlock();
                return -1;
            }
        }
    }

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

    tcp_insert_listener(cb);

    return 0;
}

int tcp_accept(Socket* sock, Socket** new_sock_out, void* addr_out, size_t* addr_len) {
    // Empty queue: park caller and return EAGAIN.
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

    if (addr_out != nullptr && addr_len != nullptr && *addr_len >= 8) {
        auto* addr = static_cast<uint8_t*>(addr_out);
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
        // Blocking connect
        for (;;) {
            if (cb->state == TcpState::ESTABLISHED) {
                sock->state = SocketState::CONNECTED;
                return 0;
            }
            if (cb->state == TcpState::CLOSED) {
                return -ECONNREFUSED;
            }
            ker::mod::sched::kern_yield();
            ker::net::napi_poll_all_pending();
        }
    }

    // Connection failed
    if (cb->state == TcpState::CLOSED && sock->state == SocketState::CONNECTING) {
        return -ECONNREFUSED;
    }

    // First connect call
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

    // Resolve local IP from the outgoing interface if not explicitly bound.
    if (cb->local_ip == 0) {
        auto* route = ker::net::route_lookup(ip);
        if (route != nullptr && route->dev != nullptr) {
            auto* nif = ker::net::netif_get(route->dev);
            if (nif != nullptr && nif->ipv4_addr_count > 0) {
                cb->local_ip = nif->ipv4_addrs[0].addr;
                sock->local_v4.addr = cb->local_ip;
            }
        }
    }

    cb->remote_ip = ip;
    cb->remote_port = port;
    sock->remote_v4.addr = ip;
    sock->remote_v4.port = port;

    // Generate ISS and send SYN
    cb->iss = generate_iss();
    cb->snd_una = cb->iss;
    cb->snd_nxt = cb->iss + 1;
    cb->rcv_wnd = sock->rcvbuf.capacity;
    cb->rcv_wscale = tcp_wscale_for_buf(sock->rcvbuf.capacity);
    cb->state = TcpState::SYN_SENT;
    sock->state = SocketState::CONNECTING;

    // Insert into hash table now that endpoints are set
    tcp_insert_cb(cb);

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
        ker::net::napi_poll_all_pending();
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

        if (cb->state != TcpState::ESTABLISHED) {
            cb->lock.unlock();
            return (sent > 0) ? static_cast<ssize_t>(sent) : -1;
        }

        // Send in MSS-sized chunks
        size_t chunk = len - sent;
        if (cb->snd_mss > 0) {
            chunk = std::min<size_t>(chunk, cb->snd_mss);
        }

        if (chunk == 0) {
            cb->lock.unlock();
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            if (sock->nonblock) {
                auto* task = ker::mod::sched::get_current_task();
                if (task != nullptr) {
                    task->deferredTaskSwitch = true;
                }
                return -EAGAIN;
            }
            ker::mod::sched::kern_yield();
            continue;
        }

        // Limit by send window
        uint32_t window = cb->snd_wnd;
        if (window == 0) {
            window = 1;
        }
        uint32_t in_flight = cb->snd_nxt - cb->snd_una;
        if (in_flight >= window) {
            cb->lock.unlock();
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            if (sock->nonblock) {
                auto* task = ker::mod::sched::get_current_task();
                if (task != nullptr) {
                    task->deferredTaskSwitch = true;
                }
                return -EAGAIN;
            }
            // Poll NAPI inline so incoming ACKs are processed promptly.
            ker::mod::sched::kern_yield();
            ker::net::napi_poll_all_pending();
            continue;
        }
        uint32_t available = window - in_flight;
        chunk = std::min<size_t>(chunk, available);

        bool ok = tcp_send_segment(cb, TCP_ACK | TCP_PSH, data + sent, chunk);
        cb->lock.unlock();

        if (!ok) {
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            if (sock->nonblock) {
                auto* task = ker::mod::sched::get_current_task();
                if (task != nullptr) {
                    task->deferredTaskSwitch = true;
                }
                return -EAGAIN;
            }
            ker::mod::sched::kern_yield();
            continue;
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

    if (sock->rcvbuf.available() > 0) {
        ssize_t n = sock->rcvbuf.read(buf, len);
        if (n > 0) {
            uint32_t old_wnd = cb->rcv_wnd;
            cb->rcv_wnd = sock->rcvbuf.free_space();
            // Only send proactive update when recovering from zero-window.
            if (old_wnd == 0) {
                if (!tcp_send_ack(cb)) {
                    cb->ack_pending = true;
                    tcp_timer_arm(cb);
                }
            }
        }
        return n;
    }

    // EOF states.
    if (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED || cb->state == TcpState::TIME_WAIT ||
        cb->state == TcpState::CLOSING || cb->state == TcpState::LAST_ACK) {
        return 0;
    }

    if (cb->state != TcpState::ESTABLISHED && cb->state != TcpState::FIN_WAIT_1 && cb->state != TcpState::FIN_WAIT_2) {
        return -EAGAIN;
    }

    if (sock->nonblock) {
        return -EAGAIN;
    }

    for (;;) {
        if (sock->rcvbuf.available() > 0) {
            ssize_t n = sock->rcvbuf.read(buf, len);
            if (n > 0) {
                uint32_t old_wnd = cb->rcv_wnd;
                cb->rcv_wnd = sock->rcvbuf.free_space();
                if (old_wnd == 0) {
                    tcp_send_ack(cb);
                }
            }
            return n;
        }
        if (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED || cb->state == TcpState::TIME_WAIT ||
            cb->state == TcpState::CLOSING || cb->state == TcpState::LAST_ACK) {
            return 0;
        }
        ker::mod::sched::kern_yield();
        ker::net::napi_poll_all_pending();
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

    bool was_listener = (cb->state == TcpState::LISTEN);

    // Drain outstanding retransmits.
    size_t rtx_drained = 0;
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
        rtx_drained++;
    }
    cb->retransmit_tail = nullptr;
    if (rtx_drained > 0) {
        ker::mod::dbg::log("tcp_close: drained %zu rtx entries, pool_free=%zu", rtx_drained, ker::net::pkt_pool_free_count());
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
            break;

        case TcpState::TIME_WAIT:
            break;

        default:
            break;
    }
    cb->lock.unlock();

    if (was_listener) {
        tcp_remove_listener(cb);
    }

    tcp_bind_lock.lock();
    for (auto& b : tcp_bindings) {
        if (b.cb == cb) {
            b.cb = nullptr;
            b.local_ip = 0;
            b.local_port = 0;
        }
    }
    tcp_bind_lock.unlock();

    // Keep TCB alive for closing states; free immediately if CLOSED.
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
    if (optname == 8 && optlen >= sizeof(int)) {  // SO_RCVBUF
        socket_resize_rcvbuf(sock, static_cast<size_t>(*static_cast<const int*>(optval)));
    }
    return 0;
}

int tcp_getsockopt_op(Socket* sock, int, int optname, void* optval, size_t* optlen) {
    if (optname == 8 && optval != nullptr && optlen != nullptr && *optlen >= sizeof(int)) {  // SO_RCVBUF
        *static_cast<int*>(optval) = static_cast<int>(sock->rcvbuf.capacity);
        *optlen = sizeof(int);
    }
    return 0;
}

int tcp_poll_check_op(Socket* sock, int events) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    int ready = 0;

    if ((events & 1) != 0) {  // POLLIN
        if (sock->rcvbuf.available() > 0) {
            ready |= 1;
        }
        if (cb != nullptr && cb->state == TcpState::LISTEN && sock->aq_count > 0) {
            ready |= 1;
        }
        if (cb != nullptr && (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED)) {
            ready |= 1;
        }
    }
    if ((events & 4) != 0) {  // POLLOUT
        if (cb == nullptr || cb->state != TcpState::ESTABLISHED) {
            ready |= 4;
        } else {
            uint32_t in_flight = cb->snd_nxt - cb->snd_una;
            if (in_flight < cb->snd_wnd) {
                ready |= 4;
            }
        }
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

auto tcp_now_ms() -> uint64_t { return ker::mod::time::getUs() / 1000; }

auto tcp_alloc_cb() -> TcpCB* {
    auto* cb = new TcpCB();
    return cb;
}

void tcp_insert_cb(TcpCB* cb) {
    uint32_t idx = tcp_hash_4tuple(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash[idx];
    bucket.lock.lock();
    cb->hash_next = bucket.head;
    bucket.head = cb;
    bucket.lock.unlock();
}

void tcp_insert_listener(TcpCB* cb) {
    uint32_t idx = tcp_hash_listener(cb->local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash[idx];
    bucket.lock.lock();
    cb->hash_next = bucket.head;
    bucket.head = cb;
    bucket.lock.unlock();
}

void tcp_remove_listener(TcpCB* cb) {
    uint32_t idx = tcp_hash_listener(cb->local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash[idx];
    bucket.lock.lock();
    TcpCB** pp = &bucket.head;
    while (*pp != nullptr) {
        if (*pp == cb) {
            *pp = cb->hash_next;
            cb->hash_next = nullptr;
            break;
        }
        pp = &(*pp)->hash_next;
    }
    bucket.lock.unlock();
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

    auto* entry = cb->retransmit_head;
    while (entry != nullptr) {
        auto* next = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
        entry = next;
    }

    delete cb;
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

    tcp_timer_disarm(cb);

    uint32_t idx = tcp_hash_4tuple(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash[idx];
    bool removed = false;
    bucket.lock.lock();
    TcpCB** pp = &bucket.head;
    while (*pp != nullptr) {
        if (*pp == cb) {
            *pp = cb->hash_next;
            cb->hash_next = nullptr;
            removed = true;
            break;
        }
        pp = &(*pp)->hash_next;
    }
    bucket.lock.unlock();

    if (removed) {
        tcp_cb_release(cb);
    }
}

auto tcp_find_cb(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> TcpCB* {
    NET_TRACE_SPAN(SPAN_TCP_FIND_CB);
    uint32_t idx = tcp_hash_4tuple(local_ip, local_port, remote_ip, remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash[idx];
    bucket.lock.lock();
    for (auto* cb = bucket.head; cb != nullptr; cb = cb->hash_next) {
        if (cb->local_port == local_port && cb->remote_port == remote_port && (cb->local_ip == local_ip || cb->local_ip == 0) &&
            cb->remote_ip == remote_ip) {
            tcp_cb_acquire(cb);
            bucket.lock.unlock();
            return cb;
        }
    }
    bucket.lock.unlock();
    return nullptr;
}

auto tcp_find_listener(uint32_t local_ip, uint16_t local_port) -> TcpCB* {
    uint32_t idx = tcp_hash_listener(local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash[idx];
    bucket.lock.lock();
    for (auto* cb = bucket.head; cb != nullptr; cb = cb->hash_next) {
        if (cb->state == TcpState::LISTEN && cb->local_port == local_port && (cb->local_ip == local_ip || cb->local_ip == 0)) {
            bucket.lock.unlock();
            return cb;
        }
    }
    bucket.lock.unlock();
    return nullptr;
}

}  // namespace ker::net::proto
