#include "udp.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
constexpr size_t MAX_UDP_SOCKETS = 128;

struct UdpBinding {
    Socket* sock = nullptr;
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
};

UdpBinding udp_bindings[MAX_UDP_SOCKETS] = {};
ker::mod::sys::Spinlock udp_lock;
uint16_t udp_ephemeral_port = 49152;

auto find_binding(uint32_t ip, uint16_t port) -> UdpBinding* {
    for (auto& b : udp_bindings) {
        if (b.sock == nullptr) {
            continue;
        }
        // Match exact IP+port, or INADDR_ANY (0) + port
        if ((b.local_ip == ip || b.local_ip == 0) && b.local_port == port) {
            return &b;
        }
    }
    return nullptr;
}

auto alloc_binding() -> UdpBinding* {
    for (auto& b : udp_bindings) {
        if (b.sock == nullptr) {
            return &b;
        }
    }
    return nullptr;
}

int udp_bind(Socket* sock, const void* addr_raw, size_t addr_len) {
    if (addr_len < 8) {
        return -1;
    }

    // sockaddr_in layout: family(2) + port(2) + addr(4)
    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    udp_lock.lock();

    // Check if already bound
    if (!sock->reuse_port) {
        auto* existing = find_binding(ip, port);
        if (existing != nullptr) {
            udp_lock.unlock();
            return -1;  // EADDRINUSE
        }
    }

    auto* binding = alloc_binding();
    if (binding == nullptr) {
        udp_lock.unlock();
        return -1;
    }

    binding->sock = sock;
    binding->local_ip = ip;
    binding->local_port = port;

    sock->local_v4.addr = ip;
    sock->local_v4.port = port;
    sock->state = SocketState::BOUND;

    udp_lock.unlock();
    return 0;
}

int udp_listen(Socket*, int) { return -1; }  // UDP doesn't listen
int udp_accept(Socket*, Socket**, void*, size_t*) { return -1; }
int udp_connect(Socket* sock, const void* addr_raw, size_t addr_len) {
    if (addr_len < 8) {
        return -1;
    }
    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    sock->remote_v4.addr = ip;
    sock->remote_v4.port = port;
    sock->state = SocketState::CONNECTED;

    // Auto-bind if not bound
    if (sock->local_v4.port == 0) {
        udp_lock.lock();
        auto* binding = alloc_binding();
        if (binding != nullptr) {
            binding->sock = sock;
            binding->local_ip = 0;
            binding->local_port = udp_ephemeral_port++;
            sock->local_v4.port = binding->local_port;
        }
        udp_lock.unlock();
    }

    return 0;
}

auto udp_send(Socket* sock, const void* buf, size_t len, int) -> ssize_t {
    if (sock->state != SocketState::CONNECTED) {
        return -1;
    }
    // Use connected destination
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return -1;
    }

    // Copy payload
    auto* payload = pkt->put(len);
    std::memcpy(payload, buf, len);

    // Prepend UDP header
    auto* udp = reinterpret_cast<UdpHeader*>(pkt->push(sizeof(UdpHeader)));
    udp->src_port = htons(sock->local_v4.port);
    udp->dst_port = htons(sock->remote_v4.port);
    udp->length = htons(static_cast<uint16_t>(sizeof(UdpHeader) + len));
    udp->checksum = 0;  // Optional for IPv4

    return ipv4_tx(pkt, sock->local_v4.addr, sock->remote_v4.addr, IPPROTO_UDP, 64) == 0 ? static_cast<ssize_t>(len)
                                                                                         : static_cast<ssize_t>(-1);
}

auto udp_recv(Socket* sock, void* buf, size_t len, int) -> ssize_t { return sock->rcvbuf.read(buf, len); }

auto udp_sendto(Socket* sock, const void* buf, size_t len, int, const void* addr_raw, size_t addr_len) -> ssize_t {
    if (addr_raw == nullptr || addr_len < 8) {
        return -1;
    }

    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port = ntohs(*reinterpret_cast<const uint16_t*>(addr + 2));
    uint32_t ip = ntohl(*reinterpret_cast<const uint32_t*>(addr + 4));

    // Auto-bind if not bound
    if (sock->local_v4.port == 0) {
        udp_lock.lock();
        auto* binding = alloc_binding();
        if (binding != nullptr) {
            binding->sock = sock;
            binding->local_ip = 0;
            binding->local_port = udp_ephemeral_port++;
            sock->local_v4.port = binding->local_port;
        }
        udp_lock.unlock();
    }

    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return -1;
    }

    auto* payload = pkt->put(len);
    std::memcpy(payload, buf, len);

    auto* udp = reinterpret_cast<UdpHeader*>(pkt->push(sizeof(UdpHeader)));
    udp->src_port = htons(sock->local_v4.port);
    udp->dst_port = htons(port);
    udp->length = htons(static_cast<uint16_t>(sizeof(UdpHeader) + len));
    udp->checksum = 0;

    uint32_t src = sock->local_v4.addr;
    return ipv4_tx(pkt, src, ip, IPPROTO_UDP, 64) == 0 ? static_cast<ssize_t>(len) : static_cast<ssize_t>(-1);
}

auto udp_recvfrom(Socket* sock, void* buf, size_t len, int, void* addr_raw, size_t* addr_len) -> ssize_t {
    // For now, return data without source address info
    // TODO: store per-datagram source address
    (void)addr_raw;
    (void)addr_len;
    return sock->rcvbuf.read(buf, len);
}

void udp_close(Socket* sock) {
    udp_lock.lock();
    for (auto& b : udp_bindings) {
        if (b.sock == sock) {
            b.sock = nullptr;
            b.local_ip = 0;
            b.local_port = 0;
        }
    }
    udp_lock.unlock();
    sock->state = SocketState::CLOSED;
}

int udp_shutdown(Socket*, int) { return 0; }
int udp_setsockopt(Socket* sock, int, int optname, const void* optval, size_t optlen) {
    if (optname == 2 && optlen >= sizeof(int)) {  // SO_REUSEADDR
        sock->reuse_addr = *static_cast<const int*>(optval) != 0;
    }
    if (optname == 15 && optlen >= sizeof(int)) {  // SO_REUSEPORT
        sock->reuse_port = *static_cast<const int*>(optval) != 0;
    }
    return 0;
}
int udp_getsockopt(Socket*, int, int, void*, size_t*) { return 0; }
int udp_poll_check(Socket* sock, int events) {
    int ready = 0;
    if ((events & 1) != 0 && sock->rcvbuf.available() > 0) {  // POLLIN
        ready |= 1;
    }
    if ((events & 4) != 0 && sock->sndbuf.free_space() > 0) {  // POLLOUT
        ready |= 4;
    }
    return ready;
}

SocketProtoOps udp_ops = {
    .bind = udp_bind,
    .listen = udp_listen,
    .accept = udp_accept,
    .connect = udp_connect,
    .send = udp_send,
    .recv = udp_recv,
    .sendto = udp_sendto,
    .recvfrom = udp_recvfrom,
    .close = udp_close,
    .shutdown = udp_shutdown,
    .setsockopt = udp_setsockopt,
    .getsockopt = udp_getsockopt,
    .poll_check = udp_poll_check,
};
}  // namespace

void udp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip) {
    (void)dev;

    if (pkt->len < sizeof(UdpHeader)) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<const UdpHeader*>(pkt->data);
    uint16_t dst_port = ntohs(hdr->dst_port);
    uint16_t payload_len = ntohs(hdr->length);

    if (payload_len < sizeof(UdpHeader) || payload_len > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Strip UDP header
    pkt->pull(sizeof(UdpHeader));
    size_t data_len = payload_len - sizeof(UdpHeader);
    pkt->len = data_len;

    udp_lock.lock();
    auto* binding = find_binding(dst_ip, dst_port);
    if (binding == nullptr) {
        // Try INADDR_ANY
        binding = find_binding(0, dst_port);
    }

    if (binding != nullptr && binding->sock != nullptr) {
        // Deliver to socket receive buffer
        binding->sock->rcvbuf.write(pkt->data, pkt->len);

        // Store sender info for recvfrom
        binding->sock->remote_v4.addr = src_ip;
        binding->sock->remote_v4.port = ntohs(reinterpret_cast<const UdpHeader*>(pkt->data - sizeof(UdpHeader))->src_port);
    }
    udp_lock.unlock();

    pkt_free(pkt);
}

auto get_udp_proto_ops() -> SocketProtoOps* { return &udp_ops; }

}  // namespace ker::net::proto
