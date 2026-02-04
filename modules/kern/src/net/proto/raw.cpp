#include "raw.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <net/endian.hpp>
#include <net/packet.hpp>
#include <net/proto/icmp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/socket.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
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
    if (buf == nullptr || addr_raw == nullptr || addr_len < 16) {
        return -EINVAL;
    }

    // Parse destination address (sockaddr_in)
    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t dst_ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    (void)port;  // Not used for raw sockets

    // Allocate packet buffer
    auto* pkt = pkt_alloc();
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

    // Try to read from socket receive buffer
    sock->rcvbuf.lock.lock();

    if (sock->rcvbuf.used == 0) {
        sock->rcvbuf.lock.unlock();
        // No data available - would need to block or return EAGAIN
        // For now, just return would block
        return -EAGAIN;
    }

#ifdef DEBUG_RAW
    ker::mod::dbg::log("raw_recvfrom: pid=%zu has %zu bytes available\n", sock->owner_pid, sock->rcvbuf.used);
#endif

    // Read available data
    size_t to_read = (len < sock->rcvbuf.used) ? len : sock->rcvbuf.used;

    auto* dst = static_cast<uint8_t*>(buf);
    for (size_t i = 0; i < to_read; i++) {
        dst[i] = sock->rcvbuf.data[sock->rcvbuf.read_pos];
        sock->rcvbuf.read_pos = (sock->rcvbuf.read_pos + 1) % sock->rcvbuf.capacity;
    }
    sock->rcvbuf.used -= to_read;

    sock->rcvbuf.lock.unlock();

    // For raw sockets, we don't have the source address stored in the buffer
    // Would need more sophisticated buffering to include source address
    if (addr_out != nullptr && addr_len != nullptr) {
        // Zero out for now
        std::memset(addr_out, 0, *addr_len);
    }

    return static_cast<ssize_t>(to_read);
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
void raw_deliver(PacketBuffer* pkt, uint8_t protocol) {
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

    raw_sockets_lock.lock();

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

            // Copy packet data to socket receive buffer
            sock->rcvbuf.lock.lock();

            size_t space = sock->rcvbuf.capacity - sock->rcvbuf.used;
            size_t to_copy = (pkt->len < space) ? pkt->len : space;

#ifdef DEBUG_RAW
            ker::mod::dbg::log("raw_deliver: MATCH socket: copying %zu bytes (space=%zu used=%zu)\n", to_copy, space, sock->rcvbuf.used);
#endif

            for (size_t j = 0; j < to_copy; j++) {
                sock->rcvbuf.data[sock->rcvbuf.write_pos] = pkt->data[j];
                sock->rcvbuf.write_pos = (sock->rcvbuf.write_pos + 1) % sock->rcvbuf.capacity;
            }
            sock->rcvbuf.used += to_copy;

            sock->rcvbuf.lock.unlock();

#ifdef DEBUG_RAW
            ker::mod::dbg::log("raw_deliver: delivered to socket, now used=%zu\n", sock->rcvbuf.used);
#endif
        }
    }

    raw_sockets_lock.unlock();
    pkt_free(pkt);
}

namespace {

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
    .setsockopt = nullptr,
    .getsockopt = nullptr,
    .poll_check = nullptr,
};

}

auto get_raw_proto_ops() -> SocketProtoOps* { return &raw_proto_ops; }

}  // namespace ker::net::proto
