#include "raw.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/packet.hpp>
#include <net/proto/icmp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/socket.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
struct RawRecvRecord {
    uint16_t packet_len;  // bytes following this record
    uint16_t reserved = 0;
    uint32_t src_ip;  // host byte order
} __attribute__((packed));
static_assert(sizeof(RawRecvRecord) == 8);

// Global list of raw sockets for packet delivery
constexpr size_t MAX_RAW_SOCKETS = 64;
std::array<Socket*, MAX_RAW_SOCKETS> raw_sockets = {};
ker::mod::sys::Spinlock raw_sockets_lock;

// Register a raw socket for receiving packets
void register_raw_socket(Socket* sock) {
    raw_sockets_lock.lock();
    for (auto& raw_socket : raw_sockets) {
        if (raw_socket == nullptr) {
            raw_socket = sock;
            break;
        }
    }
    raw_sockets_lock.unlock();
}

// Unregister a raw socket
void unregister_raw_socket(Socket* sock) {
    raw_sockets_lock.lock();
    for (auto& raw_socket : raw_sockets) {
        if (raw_socket == sock) {
            raw_socket = nullptr;
            break;
        }
    }
    raw_sockets_lock.unlock();
}

// Raw socket sendto - for ICMP and other raw IP protocols
auto raw_sendto(Socket* sock, const void* buf, size_t len, int, const void* addr_raw, size_t addr_len) -> ssize_t {
    if (buf == nullptr) {
        return -EINVAL;
    }

    uint16_t port = 0;
    uint32_t dst_ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &dst_ip, &port)) {
        return -EINVAL;
    }

    (void)port;  // Not used for raw sockets

    // Allocate packet buffer
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return -ENOBUFS;
    }

    // Copy ICMP data from userspace
    if (len > pkt->tailroom()) {
        pkt_free(pkt);
        return -EMSGSIZE;
    }

    std::memcpy(pkt->data, buf, len);
    pkt->len = len;

    // Send via IPv4 (which will add IP header and route)
#ifdef DEBUG_RAW
    ker::mod::dbg::log("raw_sendto: sending %zu bytes proto=%u to %u.%u.%u.%u\n", len, sock->protocol, (dst_ip >> 24) & 0xFF,
                       (dst_ip >> 16) & 0xFF, (dst_ip >> 8) & 0xFF, dst_ip & 0xFF);
#endif
    int result = ipv4_tx_auto(pkt, dst_ip, sock->protocol);

    if (result < 0) {
#ifdef DEBUG_RAW
        ker::mod::dbg::log("raw_sendto: ipv4_tx_auto failed with %d\n", result);
#endif
        pkt_free(pkt);
        return result;
    }

    return static_cast<ssize_t>(len);
}

// Raw socket recvfrom - receive ICMP and other raw IP packets
auto raw_recvfrom(Socket* sock, void* buf, size_t len, int, void* addr_out, size_t* addr_len) -> ssize_t {
    if (buf == nullptr) {
        return -EINVAL;
    }

    if (sock->rcvbuf.available() < sizeof(RawRecvRecord)) {
        if (!sock->nonblock) {
            socket_defer_wait(sock, "raw_wait");
        }
        return -EAGAIN;
    }

#ifdef DEBUG_RAW
    ker::mod::dbg::log("raw_recvfrom: pid=%zu has %zu bytes available\n", sock->owner_pid, sock->rcvbuf.available());
#endif

    RawRecvRecord record{};
    ssize_t hdr_n = sock->rcvbuf.read(&record, sizeof(record));
    if (hdr_n != static_cast<ssize_t>(sizeof(record))) {
        return -EIO;
    }

    size_t packet_len = record.packet_len;
    if (packet_len == 0 || packet_len > sock->rcvbuf.capacity) {
        return -EIO;
    }

    if (sock->rcvbuf.available() < packet_len) {
        return -EIO;
    }

    size_t to_copy = len < packet_len ? len : packet_len;
    ssize_t n = sock->rcvbuf.read(buf, to_copy);
    if (n != static_cast<ssize_t>(to_copy)) {
        return -EIO;
    }

    std::array<uint8_t, 256> discard{};
    size_t remaining = packet_len - to_copy;
    while (remaining > 0) {
        size_t chunk = remaining < discard.size() ? remaining : discard.size();
        ssize_t discarded = sock->rcvbuf.read(discard.data(), chunk);
        if (discarded <= 0) {
            return -EIO;
        }
        remaining -= static_cast<size_t>(discarded);
    }

    if (addr_out != nullptr && addr_len != nullptr) {
        std::memset(addr_out, 0, *addr_len);
        if (*addr_len >= SOCKADDR_V4_MIN_LEN) {
            socket_fill_sockaddr_v4(addr_out, *addr_len, nullptr, record.src_ip, 0);
        }
        *addr_len = SOCKADDR_V4_LEN;
    }

    return n;
}

auto raw_bind(Socket* sock, const void*, size_t) -> int {
    // Register this socket to receive packets
    register_raw_socket(sock);
    return 0;
}

auto raw_close(Socket* sock) -> void {
    // Unregister socket
    unregister_raw_socket(sock);
}
}  // namespace

// Deliver a packet to matching raw sockets
void raw_deliver(PacketBuffer* pkt, uint8_t protocol, uint32_t src_ip, uint32_t dst_ip, uint8_t ttl) {
    // For ICMP, try to match the ID field to route to correct socket
    uint16_t icmp_id = 0;
    bool has_icmp_id = false;

    if (protocol == 1 && pkt->len >= 8) {  // IPPROTO_ICMP
        // ICMP header: type(1) code(1) checksum(2) id(2) sequence(2)
        auto* icmp_hdr = pkt->data;
        uint8_t icmp_type = icmp_hdr[0];
        if (icmp_type == 0 || icmp_type == 8) {  // Echo reply or request
            icmp_id = (static_cast<uint16_t>(icmp_hdr[4]) << 8) | icmp_hdr[5];
            has_icmp_id = true;
#ifdef DEBUG_RAW
            ker::mod::dbg::log("raw_deliver: ICMP type=%u id=%u (from bytes: %02x %02x)\n", icmp_type, icmp_id, icmp_hdr[4], icmp_hdr[5]);
#endif
        }
    }

#ifdef DEBUG_RAW
    ker::mod::dbg::log("raw_deliver: proto=%u len=%zu has_icmp_id=%d icmp_id=%u\n", protocol, pkt->len, has_icmp_id, icmp_id);
#endif

    size_t payload_len = pkt->len;
    constexpr size_t prepend_len = sizeof(RawRecvRecord) + sizeof(IPv4Header);
    if (pkt->headroom() < prepend_len) {
        pkt_free(pkt);
        return;
    }

    auto* frame = pkt->push(prepend_len);
    auto* record = reinterpret_cast<RawRecvRecord*>(frame);
    auto* ip = reinterpret_cast<IPv4Header*>(frame + sizeof(RawRecvRecord));

    record->packet_len = static_cast<uint16_t>(sizeof(IPv4Header) + payload_len);
    record->reserved = 0;
    record->src_ip = src_ip;

    std::memset(ip, 0, sizeof(*ip));
    ip->ihl_version = (4 << 4) | 5;
    ip->total_len = htons(record->packet_len);
    ip->flags_fragoff = htons(0x4000);
    ip->ttl = ttl;
    ip->protocol = protocol;
    ip->src_addr = htonl(src_ip);
    ip->dst_addr = htonl(dst_ip);
    ip->checksum = checksum_compute(ip, sizeof(*ip));

    raw_sockets_lock.lock();
    std::array<Socket*, MAX_RAW_SOCKETS> sockets_to_wake = {};
    size_t wake_count = 0;

    for (auto* sock : raw_sockets) {
        if (sock != nullptr && sock->protocol == protocol) {
#ifdef DEBUG_RAW
            ker::mod::dbg::log("raw_deliver: checking socket owner_pid=%zu\n", sock->owner_pid);
#endif

            // If we have an ICMP ID, try to match it with the socket's owner PID
            if (has_icmp_id && sock->owner_pid != 0) {
                auto expected_id = static_cast<uint16_t>(sock->owner_pid);
                if (icmp_id != expected_id) {
#ifdef DEBUG_RAW
                    ker::mod::dbg::log("raw_deliver: SKIP socket: icmp_id=%u != expected=%u\n", icmp_id, expected_id);
#endif
                    continue;  // Not for this socket
                }
            }

            if (sock->rcvbuf.available() + pkt->len > sock->rcvbuf.capacity) {
#ifdef DEBUG_RAW
                ker::mod::dbg::log("raw_deliver: SKIP socket: not enough buffer space (available=%zu, pkt_len=%zu, capacity=%zu)\n",
                                   sock->rcvbuf.available(), pkt->len, sock->rcvbuf.capacity);
#endif
                continue;  // Not enough space in receive buffer
            }

            // Copy the record plus full IPv4 packet into the socket buffer.
            if (sock->rcvbuf.write(pkt->data, pkt->len) > 0 && wake_count < sockets_to_wake.size()) {
                sockets_to_wake[wake_count++] = sock;
            }

#ifdef DEBUG_RAW
            ker::mod::dbg::log("raw_deliver: delivered to socket, now used=%zu\n", sock->rcvbuf.used);
#endif
        }
    }

    raw_sockets_lock.unlock();
    for (size_t i = 0; i < wake_count; ++i) {
        socket_wake_waiters(sockets_to_wake[i]);
    }
    pkt_free(pkt);
}

namespace {

int raw_setsockopt(Socket* sock, int, int optname, const void* optval, size_t optlen) {
    int optint = 0;
    if (optval != nullptr && optlen >= sizeof(optint)) {
        std::memcpy(&optint, optval, sizeof(optint));
    }

    if (optname == 8 && optlen >= sizeof(int)) {  // SO_RCVBUF
        socket_resize_rcvbuf(sock, static_cast<size_t>(optint));
    }
    return 0;
}

int raw_getsockopt(Socket* sock, int, int optname, void* optval, size_t* optlen) {
    if (optname == 8 && optval != nullptr && optlen != nullptr && *optlen >= sizeof(int)) {  // SO_RCVBUF
        int value = static_cast<int>(sock->rcvbuf.capacity);
        std::memcpy(optval, &value, sizeof(value));
        *optlen = sizeof(int);
    }
    return 0;
}

SocketProtoOps raw_proto_ops = {
    .bind = raw_bind,
    .listen = nullptr,
    .accept = nullptr,
    .connect = nullptr,
    .send = nullptr,
    .recv = nullptr,
    .sendto = raw_sendto,
    .recvfrom = raw_recvfrom,
    .close = raw_close,
    .shutdown = nullptr,
    .setsockopt = raw_setsockopt,
    .getsockopt = raw_getsockopt,
    .poll_check = nullptr,
};

}  // namespace

auto get_raw_proto_ops() -> SocketProtoOps* { return &raw_proto_ops; }

}  // namespace ker::net::proto
