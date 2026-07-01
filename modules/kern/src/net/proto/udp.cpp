#include "udp.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>

#include "net/netdevice.hpp"
#include "net/packet.hpp"
#include "net/socket.hpp"

namespace ker::net::proto {

namespace {
constexpr size_t MAX_UDP_SOCKETS = 128;
constexpr uint16_t UDP_EPHEMERAL_PORT_FIRST = 49152;
constexpr uint16_t UDP_EPHEMERAL_PORT_LAST = 65535;
constexpr auto UDP_IPV4_TTL = static_cast<uint8_t>(IPV4_DEFAULT_TTL);
constexpr int SO_REUSEADDR = 2;
constexpr int SO_RCVBUF = 8;
constexpr int SO_REUSEPORT = 15;
constexpr int SO_BINDTODEVICE = 25;
constexpr int SOL_SOCKET_LEVEL = 1;
constexpr int POLLIN = 0x001;
constexpr int POLLOUT = 0x004;
constexpr size_t UDP_MAX_PACKET_PAYLOAD =
    std::min(PKT_BUF_SIZE - PKT_HEADROOM, static_cast<size_t>(UINT16_MAX) - sizeof(IPv4Header) - sizeof(UdpHeader));

struct UdpBinding {
    Socket* sock = nullptr;
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
};

struct UdpRecvRecord {
    uint16_t payload_len = 0;
    uint16_t src_port = 0;
    uint32_t src_ip = 0;
} __attribute__((packed));
static_assert(sizeof(UdpRecvRecord) == 8);

std::array<UdpBinding, MAX_UDP_SOCKETS> udp_bindings{};
ker::mod::sys::Spinlock udp_lock;
uint16_t udp_ephemeral_port = UDP_EPHEMERAL_PORT_FIRST;

auto find_binding(uint32_t ip, uint16_t port) -> UdpBinding* {
    for (auto& binding : udp_bindings) {
        if (binding.sock == nullptr) {
            continue;
        }
        // Match exact IP+port, or INADDR_ANY (0) + port
        if ((binding.local_ip == ip || binding.local_ip == 0) && binding.local_port == port) {
            return &binding;
        }
    }
    return nullptr;
}

auto alloc_binding() -> UdpBinding* {
    for (auto& binding : udp_bindings) {
        if (binding.sock == nullptr) {
            return &binding;
        }
    }
    return nullptr;
}

auto next_udp_ephemeral_port() -> uint16_t {
    uint16_t const PORT = udp_ephemeral_port;
    udp_ephemeral_port =
        udp_ephemeral_port == UDP_EPHEMERAL_PORT_LAST ? UDP_EPHEMERAL_PORT_FIRST : static_cast<uint16_t>(udp_ephemeral_port + 1);
    return PORT;
}

auto alloc_ephemeral_port_locked(uint32_t ip) -> uint16_t {
    constexpr uint32_t PORT_COUNT = static_cast<uint32_t>(UDP_EPHEMERAL_PORT_LAST) - static_cast<uint32_t>(UDP_EPHEMERAL_PORT_FIRST) + 1U;
    for (uint32_t attempt = 0; attempt < PORT_COUNT; attempt++) {
        uint16_t const PORT = next_udp_ephemeral_port();
        if (find_binding(ip, PORT) == nullptr) {
            return PORT;
        }
    }
    return 0;
}

auto bind_udp_socket_locked(Socket* sock, uint32_t ip, uint16_t port) -> int {
    if (sock == nullptr) {
        return -1;
    }

    if (port == 0) {
        port = alloc_ephemeral_port_locked(ip);
        if (port == 0) {
            return -1;
        }
    }

    if (!sock->reuse_port && find_binding(ip, port) != nullptr) {
        return -1;
    }

    auto* binding = alloc_binding();
    if (binding == nullptr) {
        return -1;
    }

    binding->sock = sock;
    binding->local_ip = ip;
    binding->local_port = port;

    sock->local_v4.addr = ip;
    sock->local_v4.port = port;
    sock->state = SocketState::BOUND;
    return 0;
}

auto ensure_udp_bound_locked(Socket* sock) -> int {
    if (sock == nullptr) {
        return -1;
    }
    if (sock->local_v4.port != 0) {
        return 0;
    }
    return bind_udp_socket_locked(sock, sock->local_v4.addr, 0);
}

auto binding_accepts_dev(const UdpBinding* binding, NetDevice const* dev) -> bool {
    if (binding == nullptr || binding->sock == nullptr) {
        return false;
    }
    uint32_t const BOUND_IFINDEX = binding->sock->bound_ifindex;
    return BOUND_IFINDEX == 0 || (dev != nullptr && dev->ifindex == BOUND_IFINDEX);
}

auto netdev_find_by_ifindex(uint32_t ifindex) -> NetDevice* {
    if (ifindex == 0) {
        return nullptr;
    }

    size_t const COUNT = netdev_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = netdev_at(i);
        if (dev != nullptr && dev->ifindex == ifindex) {
            return dev;
        }
    }
    return nullptr;
}

auto first_ipv4_or_any(NetDevice* dev) -> IPv4Address {
    auto* nif = netif_get(dev);
    if (nif == nullptr || nif->ipv4_addr_count == 0) {
        return IPv4Address::any();
    }
    return nif->ipv4_addrs.front().addr;
}

auto udp_payload_too_large(size_t len) -> bool { return len > UDP_MAX_PACKET_PAYLOAD; }

auto udp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* addr_raw, size_t* addr_len) -> ssize_t;

int udp_bind(Socket* sock, const void* addr_raw, size_t addr_len) {
    uint16_t port = 0;
    uint32_t ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &ip, &port)) {
        return -1;
    }

    udp_lock.lock();
    int const RESULT = bind_udp_socket_locked(sock, ip, port);
    udp_lock.unlock();
    return RESULT;
}

int udp_listen(Socket* /*unused*/, int /*unused*/) { return -1; }  // UDP doesn't listen
int udp_accept(Socket* /*unused*/, Socket** /*unused*/, void* /*unused*/, size_t* /*unused*/) { return -1; }
int udp_connect(Socket* sock, const void* addr_raw, size_t addr_len, int /*unused*/) {
    uint16_t port = 0;
    uint32_t ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &ip, &port)) {
        return -1;
    }

    sock->remote_v4.addr = ip;
    sock->remote_v4.port = port;
    sock->state = SocketState::CONNECTED;

    // Auto-bind if not bound
    if (sock->local_v4.port == 0) {
        udp_lock.lock();
        int const BIND_RET = ensure_udp_bound_locked(sock);
        udp_lock.unlock();
        if (BIND_RET < 0) {
            return BIND_RET;
        }
        sock->state = SocketState::CONNECTED;
    }

    return 0;
}

auto udp_send(Socket* sock, const void* buf, size_t len, int /*unused*/) -> ssize_t {
    if (sock->state != SocketState::CONNECTED) {
        return -1;
    }
    if (udp_payload_too_large(len)) {
        return -EMSGSIZE;
    }
    // Use connected destination
    auto* pkt = pkt_alloc_tx();
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

    return ipv4_tx(pkt, sock->local_v4.addr, sock->remote_v4.addr, IPPROTO_UDP, UDP_IPV4_TTL) == 0 ? static_cast<ssize_t>(len)
                                                                                                   : static_cast<ssize_t>(-1);
}

auto udp_recv(Socket* sock, void* buf, size_t len, int flags) -> ssize_t { return udp_recvfrom(sock, buf, len, flags, nullptr, nullptr); }

auto udp_sendto(Socket* sock, const void* buf, size_t len, int /*unused*/, const void* addr_raw, size_t addr_len) -> ssize_t {
    uint16_t port = 0;
    uint32_t ip = 0;
    if (!socket_parse_sockaddr_v4(addr_raw, addr_len, &ip, &port)) {
        return -1;
    }
    if (udp_payload_too_large(len)) {
        return -EMSGSIZE;
    }

    // Auto-bind if not bound
    if (sock->local_v4.port == 0) {
        udp_lock.lock();
        int const BIND_RET = ensure_udp_bound_locked(sock);
        udp_lock.unlock();
        if (BIND_RET < 0) {
            return BIND_RET;
        }
    }

    auto* pkt = pkt_alloc_tx();
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

    uint32_t const SRC = sock->local_v4.addr;
    int tx_ret = -1;
    if (sock->bound_ifindex != 0) {
        auto* bound_dev = netdev_find_by_ifindex(sock->bound_ifindex);
        IPv4Address const BOUND_SRC = SRC == 0 ? first_ipv4_or_any(bound_dev) : IPv4Address(SRC);
        tx_ret = ipv4_tx_on_dev(pkt, bound_dev, BOUND_SRC, ip, IPPROTO_UDP, UDP_IPV4_TTL);
    } else if (SRC == 0) {
        tx_ret = ipv4_tx_auto(pkt, ip, IPPROTO_UDP);
    } else {
        tx_ret = ipv4_tx(pkt, SRC, ip, IPPROTO_UDP, UDP_IPV4_TTL);
    }
    return tx_ret == 0 ? static_cast<ssize_t>(len) : static_cast<ssize_t>(-1);
}

auto udp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* addr_raw, size_t* addr_len) -> ssize_t {
    if (sock->rcvbuf.available() < sizeof(UdpRecvRecord)) {
        if (!socket_call_nonblock(sock, flags)) {
            socket_defer_wait(sock, "udp_wait");
        }
        return -EAGAIN;
    }

    UdpRecvRecord record{};
    ssize_t const HDR_N = sock->rcvbuf.read(&record, sizeof(record));
    if (std::cmp_not_equal(HDR_N, sizeof(record))) {
        return -EIO;
    }

    size_t const PAYLOAD_LEN = record.payload_len;
    if (PAYLOAD_LEN > sock->rcvbuf.capacity) {
        return -EIO;
    }
    if (sock->rcvbuf.available() < PAYLOAD_LEN) {
        return -EIO;
    }

    size_t const TO_COPY = std::min(len, PAYLOAD_LEN);
    if (addr_raw != nullptr && addr_len != nullptr) {
        std::memset(addr_raw, 0, *addr_len);
        if (*addr_len >= SOCKADDR_V4_MIN_LEN) {
            socket_fill_sockaddr_v4(addr_raw, *addr_len, nullptr, record.src_ip, record.src_port);
        }
        *addr_len = SOCKADDR_V4_LEN;
    }

    sock->remote_v4.addr = record.src_ip;
    sock->remote_v4.port = record.src_port;

    ssize_t const N = sock->rcvbuf.read(buf, TO_COPY);
    if (std::cmp_not_equal(N, TO_COPY)) {
        return -EIO;
    }

    std::array<uint8_t, 256> discard{};
    size_t remaining = PAYLOAD_LEN - TO_COPY;
    while (remaining > 0) {
        size_t const CHUNK = std::min(remaining, discard.size());
        ssize_t const DISCARDED = sock->rcvbuf.read(discard.data(), CHUNK);
        if (DISCARDED <= 0) {
            return -EIO;
        }
        remaining -= static_cast<size_t>(DISCARDED);
    }

    return N;
}

void udp_close(Socket* sock) {
    udp_lock.lock();
    for (auto& binding : udp_bindings) {
        if (binding.sock == sock) {
            binding.sock = nullptr;
            binding.local_ip = 0;
            binding.local_port = 0;
        }
    }
    udp_lock.unlock();
    sock->state = SocketState::CLOSED;
}

int udp_shutdown(Socket* /*unused*/, int /*unused*/) { return 0; }
int udp_setsockopt(Socket* sock, int level, int optname, const void* optval, size_t optlen) {
    int optint = 0;
    if (optval != nullptr && optlen >= sizeof(optint)) {
        std::memcpy(&optint, optval, sizeof(optint));
    }

    if (optname == SO_REUSEADDR && optlen >= sizeof(int)) {
        sock->reuse_addr = optint != 0;
    }
    if (optname == SO_REUSEPORT && optlen >= sizeof(int)) {
        sock->reuse_port = optint != 0;
    }
    if (optname == SO_RCVBUF && optlen >= sizeof(int)) {
        socket_resize_rcvbuf(sock, static_cast<size_t>(optint));
    }
    if (level == SOL_SOCKET_LEVEL && optname == SO_BINDTODEVICE) {
        if (optval == nullptr || optlen == 0) {
            sock->bound_ifindex = 0;
            return 0;
        }

        std::array<char, NETDEV_NAME_LEN> ifname{};
        size_t const COPY_LEN = std::min(optlen, ifname.size() - 1);
        std::memcpy(ifname.data(), optval, COPY_LEN);
        if (ifname.front() == '\0') {
            sock->bound_ifindex = 0;
            return 0;
        }

        auto* dev = netdev_find_by_name(ifname.data());
        if (dev == nullptr) {
            return -1;
        }
        sock->bound_ifindex = dev->ifindex;
    }
    // SO_SNDBUF (7): UDP has no kernel send buffer - accept silently
    return 0;
}
int udp_getsockopt(Socket* sock, int /*unused*/, int optname, void* optval, size_t* optlen) {
    if (optname == SO_RCVBUF && optval != nullptr && optlen != nullptr && *optlen >= sizeof(int)) {
        int value = static_cast<int>(sock->rcvbuf.capacity);
        std::memcpy(optval, &value, sizeof(value));
        *optlen = sizeof(int);
    }
    return 0;
}
int udp_poll_check(Socket* sock, int events) {
    int ready = 0;
    if ((events & POLLIN) != 0 && sock->rcvbuf.available() >= sizeof(UdpRecvRecord)) {
        ready |= POLLIN;
    }
    if ((events & POLLOUT) != 0) {
        ready |= POLLOUT;
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

void udp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip) {
    (void)dev;

    if (pkt->len < sizeof(UdpHeader)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const UdpHeader*>(pkt->data);
    uint16_t const DST_PORT = ntohs(hdr->dst_port);
    uint16_t const SRC_PORT = ntohs(hdr->src_port);
    uint16_t const PAYLOAD_LEN = ntohs(hdr->length);

    if (PAYLOAD_LEN < sizeof(UdpHeader) || PAYLOAD_LEN > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Strip UDP header
    pkt->pull(sizeof(UdpHeader));
    size_t const DATA_LEN = PAYLOAD_LEN - sizeof(UdpHeader);
    pkt->len = DATA_LEN;

    Socket* wake_sock = nullptr;
    udp_lock.lock();
    auto* binding = find_binding(dst_ip, DST_PORT);
    if (!binding_accepts_dev(binding, dev)) {
        binding = nullptr;
    }
    if (binding == nullptr) {
        // Try INADDR_ANY
        binding = find_binding(0, DST_PORT);
        if (!binding_accepts_dev(binding, dev)) {
            binding = nullptr;
        }
    }

    if (binding != nullptr && binding->sock != nullptr) {
        auto* sock = binding->sock;
        if (sock->rcvbuf.free_space() >= sizeof(UdpRecvRecord) + DATA_LEN) {
            UdpRecvRecord record{};
            record.payload_len = static_cast<uint16_t>(DATA_LEN);
            record.src_port = SRC_PORT;
            record.src_ip = src_ip;

            ssize_t const WRITTEN = sock->rcvbuf.write_pair(&record, sizeof(record), pkt->data, DATA_LEN);
            if (std::cmp_equal(WRITTEN, sizeof(UdpRecvRecord) + DATA_LEN)) {
                wake_sock = sock;
            }
        }
    }
    udp_lock.unlock();

    if (wake_sock != nullptr) {
        socket_wake_waiters(wake_sock);
    }

    pkt_free(pkt);
}

auto get_udp_proto_ops() -> SocketProtoOps* { return &udp_ops; }

}  // namespace ker::net::proto
