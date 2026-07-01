#include "tcp.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/backlog.hpp>
#include <net/net_trace.hpp>
#include <net/netif.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/route.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

#include "net/socket.hpp"

namespace ker::net::proto {

// Per-bucket hash tables.
// NOLINTBEGIN(misc-use-internal-linkage)
std::array<TcpHashBucket, TCB_HASH_SIZE> tcb_hash = {};
std::array<TcpHashBucket, LISTENER_HASH_SIZE> listener_hash = {};
volatile uint64_t tcp_ms_counter = 0;
// NOLINTEND(misc-use-internal-linkage)

namespace {
using log = ker::mod::dbg::logger<"tcp">;

struct TcpBinding {
    TcpCB* cb = nullptr;
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
};

std::array<TcpBinding, MAX_TCP_BINDINGS> tcp_bindings = {};
ker::mod::sys::Spinlock tcp_bind_lock;

constexpr uint16_t TCP_EPHEMERAL_PORT_FIRST = 49152;
constexpr uint16_t TCP_EPHEMERAL_PORT_LAST = 65535;
constexpr uint32_t TCP_EPHEMERAL_PORT_COUNT =
    static_cast<uint32_t>(TCP_EPHEMERAL_PORT_LAST) - static_cast<uint32_t>(TCP_EPHEMERAL_PORT_FIRST) + 1U;

uint16_t tcp_ephemeral_port = TCP_EPHEMERAL_PORT_FIRST;
bool tcp_ephemeral_seeded = false;

constexpr int TCP_SHUT_RD = 0;
constexpr int TCP_SHUT_WR = 1;
constexpr int TCP_SHUT_RDWR = 2;
constexpr int POLLIN = 0x0001;
constexpr int POLLOUT = 0x0004;
constexpr int POLLERR = 0x0008;
constexpr int POLLHUP = 0x0010;
constexpr int POLLRDHUP = 0x2000;

void defer_socket_wait(Socket* sock) {
    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
        return;
    }

    if (sock != nullptr) {
        sock->owner_pid = current_task->pid;
    }
    current_task->set_wait_channel("tcp_wait");
}

std::atomic<uint32_t> iss_counter{0x12345678};
uint64_t iss_secret = 0;
bool iss_secret_seeded = false;

auto tcp_mix64(uint64_t seed) -> uint64_t {
    seed ^= seed >> 33U;
    seed *= 0xff51afd7ed558ccdULL;
    seed ^= seed >> 33U;
    seed *= 0xc4ceb9fe1a85ec53ULL;
    seed ^= seed >> 33U;
    return seed;
}

void seed_tcp_iss_secret_once() {
    if (iss_secret_seeded) {
        return;
    }
    uint64_t seed = ker::mod::time::get_us();
    seed ^= reinterpret_cast<uintptr_t>(&iss_counter) >> 4U;
    seed ^= reinterpret_cast<uintptr_t>(&tcb_hash) >> 7U;
    iss_secret = tcp_mix64(seed);
    iss_secret_seeded = true;
}

auto tcp_iss_tuple_hash(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> uint32_t {
    uint64_t tuple = (static_cast<uint64_t>(local_ip) << 32U) | remote_ip;
    tuple ^= (static_cast<uint64_t>(local_port) << 48U) | (static_cast<uint64_t>(remote_port) << 16U);
    return static_cast<uint32_t>(tcp_mix64(tuple ^ iss_secret));
}

auto tcp_read_eof_state(TcpState state) -> bool {
    switch (state) {
        case TcpState::CLOSE_WAIT:
        case TcpState::CLOSED:
        case TcpState::TIME_WAIT:
        case TcpState::CLOSING:
        case TcpState::LAST_ACK:
            return true;
        default:
            return false;
    }
}

auto tcp_hup_state(TcpState state) -> bool {
    switch (state) {
        case TcpState::CLOSED:
        case TcpState::TIME_WAIT:
        case TcpState::CLOSING:
        case TcpState::LAST_ACK:
            return true;
        default:
            return false;
    }
}

auto tcp_ephemeral_port_from_seed(uint64_t seed) -> uint16_t {
    seed ^= seed >> 33U;
    seed *= 0xff51afd7ed558ccdULL;
    seed ^= seed >> 33U;
    seed *= 0xc4ceb9fe1a85ec53ULL;
    seed ^= seed >> 33U;
    return static_cast<uint16_t>(TCP_EPHEMERAL_PORT_FIRST + (seed % TCP_EPHEMERAL_PORT_COUNT));
}

void seed_tcp_ephemeral_port_once() {
    if (tcp_ephemeral_seeded) {
        return;
    }
    uint64_t seed = ker::mod::time::get_us();
    seed ^= static_cast<uint64_t>(iss_counter.load(std::memory_order_relaxed)) << 17U;
    seed ^= reinterpret_cast<uintptr_t>(&tcp_ephemeral_port) >> 4U;
    tcp_ephemeral_port = tcp_ephemeral_port_from_seed(seed);
    tcp_ephemeral_seeded = true;
}

auto tcp_next_ephemeral_port(uint16_t port) -> uint16_t {
    return port == TCP_EPHEMERAL_PORT_LAST ? TCP_EPHEMERAL_PORT_FIRST : static_cast<uint16_t>(port + 1);
}

auto tcp_binding_conflicts_locked(uint32_t local_ip, uint16_t local_port) -> bool {
    return std::ranges::any_of(tcp_bindings, [local_ip, local_port](const TcpBinding& b) -> bool {
        return b.cb != nullptr && b.local_port == local_port && (b.local_ip == local_ip || b.local_ip == 0 || local_ip == 0);
    });
}

auto tcp_free_binding_slot_locked() -> TcpBinding* {
    for (auto& b : tcp_bindings) {
        if (b.cb == nullptr) {
            return &b;
        }
    }
    return nullptr;
}

auto reserve_ephemeral_port_for_connect(Socket* sock, TcpCB* cb, uint32_t local_ip) -> int {
    if (sock == nullptr || cb == nullptr) {
        return -EINVAL;
    }

    tcp_bind_lock.lock();
    seed_tcp_ephemeral_port_once();

    TcpBinding* slot = tcp_free_binding_slot_locked();
    if (slot == nullptr) {
        tcp_bind_lock.unlock();
        return -EADDRNOTAVAIL;
    }

    for (uint32_t attempts = 0; attempts < TCP_EPHEMERAL_PORT_COUNT; attempts++) {
        uint16_t const PORT = tcp_ephemeral_port;
        tcp_ephemeral_port = tcp_next_ephemeral_port(tcp_ephemeral_port);
        if (tcp_binding_conflicts_locked(local_ip, PORT)) {
            continue;
        }

        slot->cb = cb;
        slot->local_ip = local_ip;
        slot->local_port = PORT;
        cb->local_ip = local_ip;
        cb->local_port = PORT;
        sock->local_v4.addr = local_ip;
        sock->local_v4.port = PORT;
        tcp_bind_lock.unlock();
        return 0;
    }

    tcp_bind_lock.unlock();
    return -EADDRNOTAVAIL;
}

void maybe_send_recv_window_update(TcpCB* cb, Socket* sock) {
    if (cb == nullptr || sock == nullptr) {
        return;
    }

    PacketBuffer* ack_pkt = nullptr;
    uint32_t ack_local = 0;
    uint32_t ack_remote = 0;

    uint64_t const FLAGS = cb->lock.lock_irqsave();
    uint32_t const OLD_WND = cb->rcv_wnd;
    cb->rcv_wnd = tcp_receive_window_space(cb, sock);
    uint32_t const WND_GROWTH = cb->rcv_wnd > OLD_WND ? cb->rcv_wnd - OLD_WND : 0;
    uint32_t const UPDATE_THRESHOLD = std::max<uint32_t>(static_cast<uint32_t>(cb->rcv_mss) * 2U, cb->rcv_wnd / 4U);
    bool const SHOULD_UPDATE_WINDOW = cb->rcv_wnd > OLD_WND && (OLD_WND == 0 || OLD_WND < cb->rcv_mss || WND_GROWTH >= UPDATE_THRESHOLD);
    if (SHOULD_UPDATE_WINDOW) {
        ack_pkt = tcp_build_ack(cb, &ack_local, &ack_remote);
        if (ack_pkt == nullptr) {
            cb->ack_pending = true;
            tcp_timer_arm(cb);
        }
    }
    cb->lock.unlock_irqrestore(FLAGS);

    if (ack_pkt != nullptr && ipv4_tx(ack_pkt, ack_local, ack_remote, 6, 64) < 0) {
        uint64_t const RETRY_FLAGS = cb->lock.lock_irqsave();
        cb->ack_pending = true;
        tcp_timer_arm(cb);
        cb->lock.unlock_irqrestore(RETRY_FLAGS);
    }
}

int tcp_bind(Socket* sock, const void* addr_raw, size_t addr_len) {
    uint16_t port = 0;
    uint32_t ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &ip, &port)) {
        return -1;
    }

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
#ifdef TCP_DEBUG
    log::debug("tcp_accept: sock=%p aq_count=%zu owner_pid=%lu", static_cast<void*>(sock), sock->aq_count, sock->owner_pid);
#endif
    if (sock->aq_count == 0) {
        sock->lock.unlock();
        defer_socket_wait(sock);
#ifdef TCP_DEBUG
        log::debug("tcp_accept: queue empty, parking pid=%lu", sock->owner_pid);
#endif
        return -EAGAIN;
    }

    Socket* child = sock->aq_head;
    sock->aq_head = child->accept_next;
    child->accept_next = nullptr;
    if (sock->aq_head == nullptr) {
        sock->aq_tail = nullptr;
    }
    sock->aq_count--;
    sock->lock.unlock();

    if (addr_out != nullptr && addr_len != nullptr && *addr_len >= SOCKADDR_V4_MIN_LEN) {
        socket_fill_sockaddr_v4(addr_out, *addr_len, addr_len, child->remote_v4.addr, child->remote_v4.port);
    }
#ifdef TCP_DEBUG
    log::debug("tcp_accept: dequeued child=%p remote_port=%u", static_cast<void*>(child), child->remote_v4.port);
#endif
    *new_sock_out = child;
    return 0;
}

int tcp_connect(Socket* sock, const void* addr_raw, size_t addr_len, int flags) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -1;
    }
    bool const NONBLOCKING = socket_call_nonblock(sock, flags);

    // Already connected
    if (cb->state == TcpState::ESTABLISHED) {
        sock->state = SocketState::CONNECTED;
        return 0;
    }

    // Connect in progress
    if (cb->state == TcpState::SYN_SENT) {
        if (NONBLOCKING) {
            return -EINPROGRESS;
        }
        if (cb->state == TcpState::ESTABLISHED) {
            sock->state = SocketState::CONNECTED;
            return 0;
        }
        if (cb->state == TcpState::CLOSED) {
            return -ECONNREFUSED;
        }
        defer_socket_wait(sock);
        return -EINPROGRESS;
    }

    // Connection failed
    if (cb->state == TcpState::CLOSED && sock->state == SocketState::CONNECTING) {
        return -ECONNREFUSED;
    }

    // First connect call
    uint16_t port = 0;
    uint32_t ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &ip, &port)) {
        return -1;
    }

    // Resolve local IP from the outgoing interface if not explicitly bound.
    if (cb->local_ip == 0) {
        auto* route = ker::net::route_lookup(ip);
        if (route != nullptr && route->dev != nullptr) {
            auto* nif = ker::net::netif_get(route->dev);
            if (nif != nullptr && nif->ipv4_addr_count > 0) {
                cb->local_ip = nif->ipv4_addrs.front().addr;
                sock->local_v4.addr = cb->local_ip;
            }
        }
    }

    // Auto-bind if not already bound.  Reserve under tcp_bind_lock before the
    // TCB is published so concurrent connects cannot claim the same local port.
    if (cb->local_port == 0) {
        int const BIND_RET = reserve_ephemeral_port_for_connect(sock, cb, cb->local_ip);
        if (BIND_RET != 0) {
            return BIND_RET;
        }
    }

    cb->remote_ip = ip;
    cb->remote_port = port;
    sock->remote_v4.addr = ip;
    sock->remote_v4.port = port;

    // Generate ISS and send SYN
    cb->iss = tcp_generate_iss(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port);
    cb->snd_una = cb->iss;
    cb->snd_nxt = cb->iss + 1;
    cb->rcv_wnd = sock->rcvbuf.capacity;
    cb->rcv_wscale = tcp_wscale_for_buf(sock->rcvbuf.capacity);
    cb->state = TcpState::SYN_SENT;
    sock->state = SocketState::CONNECTING;

    // Insert into hash table now that endpoints are set
    tcp_insert_cb(cb);

    tcp_send_segment(cb, TCP_SYN, nullptr, 0);

    if (NONBLOCKING) {
        return -EINPROGRESS;
    }

    if (cb->state == TcpState::ESTABLISHED) {
        sock->state = SocketState::CONNECTED;
        return 0;
    }
    if (cb->state == TcpState::CLOSED) {
        return -ECONNREFUSED;
    }
    defer_socket_wait(sock);
    return -EINPROGRESS;
}

auto tcp_send(Socket* sock, const void* buf, size_t len, int flags) -> ssize_t {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -ENOTCONN;
    }
    bool const NONBLOCKING = socket_call_nonblock(sock, flags);
    if (cb->state == TcpState::SYN_SENT) {
        return -EINPROGRESS;
    }
    if (cb->state != TcpState::ESTABLISHED && cb->state != TcpState::CLOSE_WAIT) {
        return -ENOTCONN;
    }

    // Update owner_pid so wake_socket() wakes the correct task after fork.
    {
        auto* cur = ker::mod::sched::get_current_task();
        if (cur != nullptr) {
            sock->owner_pid = cur->pid;
        }
    }

    const auto* data = static_cast<const uint8_t*>(buf);
    size_t sent = 0;

    while (sent < len) {
        if (sent >= TCP_SEND_BURST_BYTES) {
            return static_cast<ssize_t>(sent);
        }

        cb->lock.lock();

        if (cb->state != TcpState::ESTABLISHED && cb->state != TcpState::CLOSE_WAIT) {
            auto const RESULT = cb->state == TcpState::SYN_SENT ? -EINPROGRESS : -ENOTCONN;
            cb->lock.unlock();
            return (sent > 0) ? static_cast<ssize_t>(sent) : RESULT;
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
            if (NONBLOCKING) {
                return -EAGAIN;
            }
            defer_socket_wait(sock);
            return -EAGAIN;
        }

        uint32_t const AVAILABLE = tcp_send_available_bytes(cb);
        if (AVAILABLE == 0) {
            cb->lock.unlock();
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            if (NONBLOCKING) {
                return -EAGAIN;
            }
            defer_socket_wait(sock);
            return -EAGAIN;
        }
        chunk = std::min<size_t>(chunk, TCP_SEND_BURST_BYTES - sent);
        chunk = std::min<size_t>(chunk, AVAILABLE);

        cb->lock.unlock();
        bool const OK = tcp_send_segment(cb, TCP_ACK | TCP_PSH, data + sent, chunk);

        if (!OK) {
            if (sent > 0) {
                return static_cast<ssize_t>(sent);
            }
            if (NONBLOCKING) {
                return -EAGAIN;
            }
            defer_socket_wait(sock);
            return -EAGAIN;
        }
        sent += chunk;
    }

    return static_cast<ssize_t>(sent);
}

auto tcp_recv(Socket* sock, void* buf, size_t len, int flags) -> ssize_t {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -1;
    }
    bool const NONBLOCKING = socket_call_nonblock(sock, flags);

#ifdef TCP_DEBUG
    auto current_pid = [&]() -> uint64_t {
        if (auto* current_task = ker::mod::sched::get_current_task(); current_task != nullptr) {
            return current_task->pid;
        }
        return sock->owner_pid;
    };
#endif

    ssize_t completed = 0;
    auto maybe_finish_recv = [&]() -> bool {
        if (sock->rcvbuf.available() > 0) {
            ssize_t const N = sock->rcvbuf.read(buf, len);
            if (N > 0) {
                maybe_send_recv_window_update(cb, sock);
            }
            completed = N;
            return true;
        }

        // EOF states.
        if (cb->state == TcpState::CLOSE_WAIT || cb->state == TcpState::CLOSED || cb->state == TcpState::TIME_WAIT ||
            cb->state == TcpState::CLOSING || cb->state == TcpState::LAST_ACK) {
#ifdef TCP_DEBUG
            log::trace("tcp_recv: pid=%lu eof state=%u len=%zu avail=%zu", current_pid(), static_cast<unsigned>(cb->state), len,
                       sock->rcvbuf.available());
#endif
            completed = 0;
            return true;
        }

        return false;
    };

    if (maybe_finish_recv()) {
        return completed;
    }

    if (cb->state != TcpState::ESTABLISHED && cb->state != TcpState::FIN_WAIT_1 && cb->state != TcpState::FIN_WAIT_2) {
        return -EAGAIN;
    }

    if (NONBLOCKING) {
        return -EAGAIN;
    }

    // Update owner_pid so wake_socket() wakes the correct task (handles fork: child
    // inherits the fd but owner_pid still points to the parent that accepted it).
    {
        auto* cur = ker::mod::sched::get_current_task();
        if (cur != nullptr) {
            sock->owner_pid = cur->pid;
        }
    }

    // Drain any already-pending RX work before committing to a blocking wait.
    ker::net::napi_poll_all_pending();
    ker::net::backlog_drain_all_pending_inline();
    if (maybe_finish_recv()) {
        return completed;
    }

    socket_defer_wait(sock, "tcp_wait");

    // Close the race where data arrives after the last readiness check.
    if (maybe_finish_recv()) {
        if (auto* current_task = ker::mod::sched::get_current_task(); current_task != nullptr) {
            current_task->clear_wait_channel();
        }
        return completed;
    }

    return -EAGAIN;
}

auto tcp_sendto(Socket* sock, const void* buf, size_t len, int flags, const void* /*unused*/, size_t /*unused*/) -> ssize_t {
    return tcp_send(sock, buf, len, flags);
}

auto tcp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* /*unused*/, size_t* /*unused*/) -> ssize_t {
    return tcp_recv(sock, buf, len, flags);
}

void tcp_close_op(Socket* sock) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return;
    }

    cb->lock.lock();

    bool const WAS_LISTENER = (cb->state == TcpState::LISTEN);
    bool send_fin = false;
    TcpState next_state = cb->state;

    // Drain outstanding retransmits.
    [[maybe_unused]] size_t rtx_drained = 0;
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        delete entry;
        rtx_drained++;
    }
    cb->retransmit_tail = nullptr;
#ifdef TCP_DEBUG
    if (rtx_drained > 0) {
        log::debug("tcp_close: drained %zu rtx entries, pool_free=%zu", rtx_drained, ker::net::pkt_pool_free_count());
    }
#endif
    switch (cb->state) {
        case TcpState::CLOSED:
        case TcpState::LISTEN:
        case TcpState::SYN_SENT:
            cb->state = TcpState::CLOSED;
            break;

        case TcpState::ESTABLISHED:
            send_fin = true;
            next_state = TcpState::FIN_WAIT_1;
            break;

        case TcpState::CLOSE_WAIT:
            send_fin = true;
            next_state = TcpState::LAST_ACK;
            break;

        default:
            break;
    }
    cb->lock.unlock();

    if (send_fin) {
        if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
            cb->lock.lock();
            cb->state = next_state;
            cb->lock.unlock();
        } else {
            cb->lock.lock();
            cb->state = TcpState::CLOSED;
            cb->lock.unlock();
        }
    }

    if (WAS_LISTENER) {
        uint64_t const SOCK_FLAGS = sock->lock.lock_irqsave();
        sock->state = SocketState::CLOSED;
        sock->lock.unlock_irqrestore(SOCK_FLAGS);

        tcp_remove_listener(cb);
        tcp_drain_accept_queue(sock);
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
    // Drop the owning socket ref; timer/hash/listener refs keep the TCB alive if needed.
    tcp_cb_release(cb);  // NOLINT(clang-analyzer-cplusplus.NewDelete)
}

int tcp_shutdown_op(Socket* sock, int how) {
    (void)how;
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    if (cb == nullptr) {
        return -ENOTCONN;
    }

    if (how == TCP_SHUT_RD) {
        return 0;
    }
    if (how != TCP_SHUT_WR && how != TCP_SHUT_RDWR) {
        return -EINVAL;
    }

    for (;;) {
        cb->lock.lock();
        if (cb->state == TcpState::ESTABLISHED) {
            cb->lock.unlock();
            if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
                cb->lock.lock();
                cb->state = TcpState::FIN_WAIT_1;
                cb->lock.unlock();
                return 0;
            }
        } else if (cb->state == TcpState::CLOSE_WAIT) {
            cb->lock.unlock();
            if (tcp_send_segment(cb, TCP_FIN | TCP_ACK, nullptr, 0)) {
                cb->lock.lock();
                cb->state = TcpState::LAST_ACK;
                cb->lock.unlock();
                return 0;
            }
        } else {
            cb->lock.unlock();
            return 0;
        }
        if (sock->nonblock) {
            return -EAGAIN;
        }

        ker::mod::sched::kern_yield();
        ker::net::napi_poll_all_pending();
        ker::net::backlog_drain_all_pending_inline();
    }
}

int tcp_setsockopt_op(Socket* sock, int /*unused*/, int optname, const void* optval, size_t optlen) {
    int optint = 0;
    if (optval != nullptr && optlen >= sizeof(optint)) {
        std::memcpy(&optint, optval, sizeof(optint));
    }

    if (optname == 2 && optlen >= sizeof(int)) {  // SO_REUSEADDR
        sock->reuse_addr = optint != 0;
    }
    if (optname == 15 && optlen >= sizeof(int)) {  // SO_REUSEPORT
        sock->reuse_port = optint != 0;
    }
    if (optname == 8 && optlen >= sizeof(int)) {  // SO_RCVBUF
        socket_resize_rcvbuf(sock, static_cast<size_t>(optint));
    }
    if (optname == 9 && optlen >= sizeof(int)) {  // SO_KEEPALIVE
        auto* cb = static_cast<TcpCB*>(sock->proto_data);
        if (cb != nullptr) {
            uint64_t const FLAGS = cb->lock.lock_irqsave();
            cb->keepalive_enabled = optint != 0;
            if (cb->keepalive_enabled && cb->state == TcpState::ESTABLISHED) {
                cb->keepalive_count = 0;
                cb->keepalive_deadline = tcp_deadline_after_ms(tcp_now_ms(), cb->keepalive_idle_ms);
                tcp_timer_arm(cb);
            } else {
                cb->keepalive_deadline = 0;
            }
            cb->lock.unlock_irqrestore(FLAGS);
        }
    }
    return 0;
}

int tcp_getsockopt_op(Socket* sock, int /*unused*/, int optname, void* optval, size_t* optlen) {
    if (optname == 4 && optval != nullptr && optlen != nullptr && *optlen >= sizeof(int)) {  // SO_ERROR
        int value = 0;
        auto* cb = static_cast<TcpCB*>(sock->proto_data);
        if (cb != nullptr) {
            uint64_t const FLAGS = cb->lock.lock_irqsave();
            if (cb->state == TcpState::SYN_SENT) {
                value = EINPROGRESS;
            } else if (cb->state == TcpState::CLOSED && sock->state == SocketState::CONNECTING) {
                value = ECONNREFUSED;
            }
            cb->lock.unlock_irqrestore(FLAGS);
        }
        std::memcpy(optval, &value, sizeof(value));
        *optlen = sizeof(int);
        return 0;
    }

    if (optname == 8 && optval != nullptr && optlen != nullptr && *optlen >= sizeof(int)) {  // SO_RCVBUF
        int value = static_cast<int>(sock->rcvbuf.capacity);
        std::memcpy(optval, &value, sizeof(value));
        *optlen = sizeof(int);
    }
    return 0;
}

int tcp_poll_check_op(Socket* sock, int events) {
    auto* cb = static_cast<TcpCB*>(sock->proto_data);
    int ready = 0;

    if (cb == nullptr) {
        return POLLERR | POLLHUP;
    }

    uint64_t const FLAGS = cb->lock.lock_irqsave();
    TcpState const STATE = cb->state;
    bool const READ_EOF = tcp_read_eof_state(STATE);
    bool const FULL_HUP = tcp_hup_state(STATE);
    bool const CONNECT_FAILED = STATE == TcpState::CLOSED && sock->state == SocketState::CONNECTING;

    if ((events & POLLIN) != 0) {
        if (sock->rcvbuf.available() > 0 || READ_EOF) {
            ready |= POLLIN;
        }
        if (STATE == TcpState::LISTEN && sock->aq_count > 0) {
            ready |= POLLIN;
        }
    }
    if ((events & POLLRDHUP) != 0 && READ_EOF) {
        ready |= POLLRDHUP;
    }
    if ((events & POLLOUT) != 0) {
        bool const CAN_SEND = STATE == TcpState::ESTABLISHED || STATE == TcpState::CLOSE_WAIT;
        bool const HAS_SEND_CREDIT = CAN_SEND && tcp_send_available_bytes(cb) > 0;
        if (HAS_SEND_CREDIT || CONNECT_FAILED) {
            ready |= POLLOUT;
        }
    }

    if (CONNECT_FAILED) {
        ready |= POLLERR;
    }
    if (FULL_HUP) {
        ready |= POLLHUP;
    }

    cb->lock.unlock_irqrestore(FLAGS);
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

auto tcp_now_ms() -> uint64_t { return ker::mod::time::get_us() / 1000; }

auto tcp_generate_iss(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> uint32_t {
    seed_tcp_iss_secret_once();
    auto const CLOCK = static_cast<uint32_t>(ker::mod::time::get_us() / 4U);
    uint32_t const SERIAL = iss_counter.fetch_add(64000U, std::memory_order_relaxed);
    return CLOCK + SERIAL + tcp_iss_tuple_hash(local_ip, local_port, remote_ip, remote_port);
}

auto tcp_alloc_cb() -> TcpCB* {
    auto* cb = new TcpCB();
    return cb;
}

void tcp_insert_cb(TcpCB* cb) {
    uint32_t const IDX = tcp_hash_4tuple(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash.at(IDX);
    bucket.lock.lock();
    // Hash membership owns a ref so lookups can safely retain under the bucket lock.
    tcp_cb_acquire(cb);
    cb->hash_next = bucket.head;
    bucket.head = cb;
    bucket.lock.unlock();
}

void tcp_insert_listener(TcpCB* cb) {
    uint32_t const IDX = tcp_hash_listener(cb->local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash.at(IDX);
    bucket.lock.lock();
    // Listener-table membership owns a ref independent of the owning socket.
    tcp_cb_acquire(cb);
    cb->hash_next = bucket.head;
    bucket.head = cb;
    bucket.lock.unlock();
}

void tcp_remove_listener(TcpCB* cb) {
    uint32_t const IDX = tcp_hash_listener(cb->local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash.at(IDX);
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

    auto* ooo = cb->ooo_head;
    while (ooo != nullptr) {
        auto* next = ooo->next;
        delete[] ooo->data;
        delete ooo;
        ooo = next;
    }

    auto* entry = cb->retransmit_head;
    while (entry != nullptr) {
        auto* next = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        delete entry;
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

void tcp_destroy_unaccepted_child(Socket* child) {
    if (child == nullptr) {
        return;
    }

    child->accept_next = nullptr;
    child->state = SocketState::CLOSED;

    auto* cb = static_cast<TcpCB*>(child->proto_data);
    if (cb != nullptr) {
        cb->lock.lock();
        cb->state = TcpState::CLOSED;
        cb->lock.unlock();

        tcp_free_cb(cb);
        child->proto_data = nullptr;
        cb->socket = nullptr;
        tcp_cb_release(cb);
    }

    socket_destroy(child);
}

void tcp_drain_accept_queue(Socket* listener) {
    if (listener == nullptr) {
        return;
    }

    for (;;) {
        uint64_t const FLAGS = listener->lock.lock_irqsave();
        Socket* child = listener->aq_head;
        if (child == nullptr) {
            listener->aq_tail = nullptr;
            listener->aq_count = 0;
            listener->lock.unlock_irqrestore(FLAGS);
            return;
        }

        listener->aq_head = child->accept_next;
        if (listener->aq_head == nullptr) {
            listener->aq_tail = nullptr;
        }
        if (listener->aq_count > 0) {
            listener->aq_count--;
        }
        child->accept_next = nullptr;
        listener->lock.unlock_irqrestore(FLAGS);

        tcp_destroy_unaccepted_child(child);
    }
}

void tcp_free_cb(TcpCB* cb) {
    if (cb == nullptr) {
        return;
    }

    tcp_timer_disarm(cb);

    uint32_t const IDX = tcp_hash_4tuple(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash.at(IDX);
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
    uint32_t const IDX = tcp_hash_4tuple(local_ip, local_port, remote_ip, remote_port) % TCB_HASH_SIZE;
    auto& bucket = tcb_hash.at(IDX);
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
    uint32_t const IDX = tcp_hash_listener(local_port) % LISTENER_HASH_SIZE;
    auto& bucket = listener_hash.at(IDX);
    bucket.lock.lock();
    for (auto* cb = bucket.head; cb != nullptr; cb = cb->hash_next) {
        if (cb->state == TcpState::LISTEN && cb->local_port == local_port && (cb->local_ip == local_ip || cb->local_ip == 0)) {
            tcp_cb_acquire(cb);
            bucket.lock.unlock();
            return cb;
        }
    }
    bucket.lock.unlock();
    return nullptr;
}

auto tcp_listener_snapshot(TcpListenerSnapshot* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    for (auto& bucket : listener_hash) {
        bucket.lock.lock();
        for (TcpCB const* cb = bucket.head; cb != nullptr && count < max; cb = cb->hash_next) {
            if (cb->state != TcpState::LISTEN) {
                continue;
            }

            auto& row = out[count++];
            row.local_ip = cb->local_ip;
            row.local_port = cb->local_port;
            row.state = static_cast<uint8_t>(cb->state);
            row.rcv_wnd = cb->rcv_wnd;
            row.refcount = cb->refcnt.load(std::memory_order_acquire);
            Socket const* sock = cb->socket;
            if (sock != nullptr) {
                row.owner_pid = sock->owner_pid;
                row.accept_queue = sock->aq_count;
                row.backlog = sock->backlog;
                row.rcvbuf_used = sock->rcvbuf.available();
                row.rcvbuf_capacity = sock->rcvbuf.capacity;
            }
        }
        bucket.lock.unlock();
        if (count >= max) {
            break;
        }
    }
    return count;
}

auto tcp_conn_snapshot(TcpConnSnapshot* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    for (auto& bucket : tcb_hash) {
        bucket.lock.lock();
        for (TcpCB const* cb = bucket.head; cb != nullptr && count < max; cb = cb->hash_next) {
            if (cb->state == TcpState::LISTEN) {
                continue;
            }

            auto& row = out[count++];
            row.local_ip = cb->local_ip;
            row.local_port = cb->local_port;
            row.remote_ip = cb->remote_ip;
            row.remote_port = cb->remote_port;
            row.state = static_cast<uint8_t>(cb->state);
            row.rcv_nxt = cb->rcv_nxt;
            row.rcv_wnd = cb->rcv_wnd;
            row.snd_una = cb->snd_una;
            row.snd_nxt = cb->snd_nxt;
            row.snd_wnd = cb->snd_wnd;
            row.ooo_bytes = cb->ooo_bytes.load(std::memory_order_acquire);
            row.refcount = cb->refcnt.load(std::memory_order_acquire);
            row.sack_permitted = cb->sack_permitted;
            Socket const* sock = cb->socket;
            if (sock != nullptr) {
                row.owner_pid = sock->owner_pid;
                row.rcvbuf_used = sock->rcvbuf.available();
                row.rcvbuf_capacity = sock->rcvbuf.capacity;
            }
        }
        bucket.lock.unlock();
        if (count >= max) {
            break;
        }
    }
    return count;
}

}  // namespace ker::net::proto
