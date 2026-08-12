#include "udp.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <net/route.hpp>
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
constexpr uint8_t UDP_IPV6_HOP_LIMIT = 64;
constexpr int SO_ERROR = 4;
constexpr int SO_REUSEADDR = 2;
constexpr int SO_RCVBUF = 8;
constexpr int SO_REUSEPORT = 15;
constexpr int SO_BINDTODEVICE = 25;
constexpr int SOL_SOCKET_LEVEL = 1;
constexpr int SOL_IPV6_LEVEL = 41;
constexpr int IPV6_V6ONLY = 26;
constexpr int IPV6_UNICAST_HOPS = 16;
constexpr int IPV6_MULTICAST_HOPS = 18;
constexpr int SHUT_RD = 0;
constexpr int SHUT_WR = 1;
constexpr int SHUT_RDWR = 2;
constexpr int POLLIN = 0x001;
constexpr int POLLOUT = 0x004;
constexpr int POLLERR = 0x008;
constexpr int POLLHUP = 0x010;
constexpr size_t UDP_MAX_PACKET_PAYLOAD = std::min(PKT_BUF_SIZE - PKT_HEADROOM, static_cast<size_t>(UINT16_MAX) - sizeof(UdpHeader));

struct UdpBinding {
    Socket* sock = nullptr;
};

struct UdpRecvRecord {
    uint16_t payload_len = 0;
    uint16_t reserved = 0;
    SocketEndpoint source{};
};
static_assert(sizeof(UdpRecvRecord) == 28);

std::array<UdpBinding, MAX_UDP_SOCKETS> udp_bindings{};
ker::mod::sys::Spinlock udp_lock;
uint16_t udp_ephemeral_port = UDP_EPHEMERAL_PORT_FIRST;

auto alloc_binding() -> UdpBinding* {
    for (auto& binding : udp_bindings) {
        if (binding.sock == nullptr) {
            return &binding;
        }
    }
    return nullptr;
}

auto binding_interfaces_overlap(const Socket* lhs, const Socket* rhs) -> bool {
    return lhs->bound_ifindex == 0 || rhs->bound_ifindex == 0 || lhs->bound_ifindex == rhs->bound_ifindex;
}

auto binding_conflicts_locked(const Socket* sock, const SocketEndpoint& local) -> bool {
    for (const auto& binding : udp_bindings) {
        const Socket* existing = binding.sock;
        if (existing == nullptr || !binding_interfaces_overlap(existing, sock)) {
            continue;
        }
        if (socket_endpoints_bind_conflict(existing->local, existing->ipv6_v6only, local, sock->ipv6_v6only) &&
            !(existing->reuse_port && sock->reuse_port)) {
            return true;
        }
    }
    return false;
}

auto next_udp_ephemeral_port() -> uint16_t {
    uint16_t const PORT = udp_ephemeral_port;
    udp_ephemeral_port =
        udp_ephemeral_port == UDP_EPHEMERAL_PORT_LAST ? UDP_EPHEMERAL_PORT_FIRST : static_cast<uint16_t>(udp_ephemeral_port + 1);
    return PORT;
}

auto alloc_ephemeral_port_locked(const Socket* sock, SocketEndpoint local) -> uint16_t {
    constexpr uint32_t PORT_COUNT = static_cast<uint32_t>(UDP_EPHEMERAL_PORT_LAST) - static_cast<uint32_t>(UDP_EPHEMERAL_PORT_FIRST) + 1U;
    for (uint32_t attempt = 0; attempt < PORT_COUNT; ++attempt) {
        local.port = next_udp_ephemeral_port();
        if (!binding_conflicts_locked(sock, local)) {
            return local.port;
        }
    }
    return 0;
}

auto bind_udp_socket_locked(Socket* sock, SocketEndpoint local) -> int {
    if (sock == nullptr || (local.family != SOCKADDR_V4_FAMILY && local.family != SOCKADDR_V6_FAMILY)) {
        return -EINVAL;
    }
    if (local.is_v4_mapped() && sock->ipv6_v6only) {
        return -EAFNOSUPPORT;
    }
    if (local.port == 0) {
        local.port = alloc_ephemeral_port_locked(sock, local);
        if (local.port == 0) {
            return -EADDRNOTAVAIL;
        }
    }
    if (binding_conflicts_locked(sock, local)) {
        return -EADDRINUSE;
    }
    auto* binding = alloc_binding();
    if (binding == nullptr) {
        return -ENOBUFS;
    }
    binding->sock = sock;
    sock->local = local;
    sock->state = SocketState::BOUND;
    return 0;
}

auto ensure_udp_bound_locked(Socket* sock) -> int {
    if (sock == nullptr) {
        return -EINVAL;
    }
    if (sock->local.port != 0) {
        return 0;
    }
    return bind_udp_socket_locked(sock, sock->local);
}

auto binding_accepts_dev(const UdpBinding& binding, const NetDevice* dev) -> bool {
    uint32_t const BOUND_IFINDEX = binding.sock->bound_ifindex;
    return BOUND_IFINDEX == 0 || (dev != nullptr && dev->ifindex == BOUND_IFINDEX);
}

auto source_matches_connected_peer(const Socket* sock, const SocketEndpoint& source) -> bool {
    if (sock->state != SocketState::CONNECTED || sock->remote.port == 0) {
        return true;
    }
    return sock->remote.port == source.port && socket_endpoint_address_equal(sock->remote, source);
}

auto find_binding_locked(const SocketEndpoint& destination, const SocketEndpoint& source, const NetDevice* dev) -> UdpBinding* {
    UdpBinding* wildcard = nullptr;
    for (auto& binding : udp_bindings) {
        if (binding.sock == nullptr || !binding_accepts_dev(binding, dev) ||
            !socket_endpoint_matches(binding.sock->local, destination, binding.sock->ipv6_v6only) ||
            !source_matches_connected_peer(binding.sock, source)) {
            continue;
        }
        if (!binding.sock->local.is_unspecified()) {
            return &binding;
        }
        if (wildcard == nullptr) {
            wildcard = &binding;
        }
    }
    return wildcard;
}

auto netdev_find_by_ifindex(uint32_t ifindex) -> NetDeviceRef {
    if (ifindex == 0) {
        return {};
    }
    size_t const COUNT = netdev_count();
    for (size_t i = 0; i < COUNT; ++i) {
        NetDeviceRef dev = netdev_at_ref(i);
        if (dev && dev->ifindex == ifindex) {
            return dev;
        }
    }
    return {};
}

auto first_ipv4_or_any(NetDevice* dev) -> IPv4Address {
    auto* nif = netif_find_by_dev(dev);
    if (nif == nullptr || nif->ipv4_addr_count == 0) {
        return IPv4Address::any();
    }
    return nif->ipv4_addrs.front().addr;
}

auto udp_payload_too_large(size_t len) -> bool { return len > UDP_MAX_PACKET_PAYLOAD; }

auto endpoint_for_v4_socket(const Socket* sock, IPv4Address address, uint16_t port) -> SocketEndpoint {
    return sock->domain == SOCKADDR_V6_FAMILY ? SocketEndpoint::mapped_ipv4(address, port) : SocketEndpoint::ipv4(address, port);
}

auto queue_datagram_locked(UdpBinding& binding, const SocketEndpoint& source, const uint8_t* data, size_t data_len) -> Socket* {
    Socket* sock = binding.sock;
    if (sock == nullptr || sock->read_shutdown || sock->rcvbuf.free_space() < sizeof(UdpRecvRecord) + data_len ||
        !socket_try_acquire(sock)) {
        return nullptr;
    }
    UdpRecvRecord record{.payload_len = static_cast<uint16_t>(data_len), .source = source};
    ssize_t const WRITTEN = sock->rcvbuf.write_pair(&record, sizeof(record), data, data_len);
    if (std::cmp_equal(WRITTEN, sizeof(UdpRecvRecord) + data_len)) {
        return sock;
    }
    socket_release(sock);
    return nullptr;
}

auto udp_transmit_v4(Socket* sock, const SocketEndpoint& destination, const void* buf, size_t len) -> ssize_t {
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return -ENOBUFS;
    }
    std::memcpy(pkt->put(len), buf, len);
    auto* udp = reinterpret_cast<UdpHeader*>(pkt->push(sizeof(UdpHeader)));
    udp->src_port = htons(sock->local.port);
    udp->dst_port = htons(destination.port);
    udp->length = htons(static_cast<uint16_t>(sizeof(UdpHeader) + len));
    udp->checksum = 0;

    IPv4Address const SRC = sock->local.ipv4_address();
    IPv4Address const DST = destination.ipv4_address();
    int tx_ret = -1;
    if (sock->bound_ifindex != 0) {
        NetDeviceRef bound_ref = netdev_find_by_ifindex(sock->bound_ifindex);
        auto* bound_dev = bound_ref.get();
        if (!bound_ref || !pkt_adopt_netdev_ref(pkt, std::move(bound_ref))) {
            pkt_free(pkt);
            return -ENODEV;
        }
        IPv4Address const BOUND_SRC = SRC.is_any() ? first_ipv4_or_any(bound_dev) : SRC;
        tx_ret = ipv4_tx_on_dev(pkt, bound_dev, BOUND_SRC, DST, IPPROTO_UDP, UDP_IPV4_TTL);
    } else if (SRC.is_any()) {
        tx_ret = ipv4_tx_auto(pkt, DST, IPPROTO_UDP);
    } else {
        tx_ret = ipv4_tx(pkt, SRC, DST, IPPROTO_UDP, UDP_IPV4_TTL);
    }
    return tx_ret == 0 ? static_cast<ssize_t>(len) : static_cast<ssize_t>(tx_ret);
}

auto udp_transmit_v6(Socket* sock, const SocketEndpoint& destination, const void* buf, size_t len) -> ssize_t {
    IPv6Address const DST = destination.ipv6_address();
    IPv6Address requested{};
    const IPv6Address* requested_ptr = nullptr;
    if (!sock->local.is_unspecified()) {
        requested = sock->local.ipv6_address();
        requested_ptr = &requested;
    }
    uint32_t const IFINDEX = sock->bound_ifindex != 0 ? sock->bound_ifindex : destination.scope_id;
    IPv6OutputRoute route{};
    int const ROUTE_RESULT = ipv6_route_resolve(DST, requested_ptr, IFINDEX, route);
    if (ROUTE_RESULT < 0) {
        return ROUTE_RESULT;
    }

    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return -ENOBUFS;
    }
    std::memcpy(pkt->put(len), buf, len);
    auto* udp = reinterpret_cast<UdpHeader*>(pkt->push(sizeof(UdpHeader)));
    udp->src_port = htons(sock->local.port);
    udp->dst_port = htons(destination.port);
    uint16_t const UDP_LEN = static_cast<uint16_t>(sizeof(UdpHeader) + len);
    udp->length = htons(UDP_LEN);
    udp->checksum = 0;
    udp->checksum = checksum_pseudo_ipv6(route.source, DST, IPV6_PROTO_UDP, UDP_LEN, pkt->data, pkt->len);
    if (udp->checksum == 0) {
        udp->checksum = 0xFFFF;
    }

    int const CONFIGURED_HOPS = DST.is_multicast() ? sock->ipv6_multicast_hops : sock->ipv6_unicast_hops;
    uint8_t const HOP_LIMIT = static_cast<uint8_t>(std::clamp(CONFIGURED_HOPS, 0, 255));
    int const RESULT = ipv6_tx_routed(pkt, std::move(route), DST, IPV6_PROTO_UDP, HOP_LIMIT);
    return RESULT == 0 ? static_cast<ssize_t>(len) : static_cast<ssize_t>(RESULT);
}

auto udp_send_preflight(Socket* sock, const void* buf, size_t len) -> int {
    if (sock == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    if (sock->write_shutdown) {
        return -EPIPE;
    }
    int const PENDING_ERROR = sock->pending_error.exchange(0, std::memory_order_acq_rel);
    if (PENDING_ERROR != 0) {
        return -PENDING_ERROR;
    }
    if (udp_payload_too_large(len)) {
        return -EMSGSIZE;
    }
    return 0;
}

auto udp_transmit_prechecked(Socket* sock, const SocketEndpoint& destination, const void* buf, size_t len) -> ssize_t {
    if (destination.is_v4_mapped()) {
        if (sock->ipv6_v6only) {
            return -EAFNOSUPPORT;
        }
        return udp_transmit_v4(sock, destination, buf, len);
    }
    if (destination.is_ipv4()) {
        return udp_transmit_v4(sock, destination, buf, len);
    }
    return udp_transmit_v6(sock, destination, buf, len);
}

auto udp_transmit(Socket* sock, const SocketEndpoint& destination, const void* buf, size_t len) -> ssize_t {
    int const PREFLIGHT_RESULT = udp_send_preflight(sock, buf, len);
    return PREFLIGHT_RESULT < 0 ? static_cast<ssize_t>(PREFLIGHT_RESULT) : udp_transmit_prechecked(sock, destination, buf, len);
}

auto udp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* addr_raw, size_t* addr_len) -> ssize_t;

int udp_bind(Socket* sock, const void* addr_raw, size_t addr_len) {
    SocketEndpoint local{};
    if (sock == nullptr || !socket_state_allows_explicit_bind(sock->state)) {
        return -EINVAL;
    }
    if (!socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &local)) {
        return -EINVAL;
    }
    int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, local, true);
    if (SCOPE_RESULT < 0) {
        return SCOPE_RESULT;
    }
    udp_lock.lock();
    int const RESULT = bind_udp_socket_locked(sock, local);
    udp_lock.unlock();
    return RESULT;
}

int udp_listen(Socket* /*unused*/, int /*unused*/) { return -EOPNOTSUPP; }
int udp_accept(Socket* /*unused*/, Socket** /*unused*/, void* /*unused*/, size_t* /*unused*/) { return -EOPNOTSUPP; }

int udp_connect(Socket* sock, const void* addr_raw, size_t addr_len, int /*unused*/) {
    SocketEndpoint remote{};
    if (sock == nullptr || !socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &remote) || remote.port == 0) {
        return -EINVAL;
    }
    if (remote.is_v4_mapped() && sock->ipv6_v6only) {
        return -EAFNOSUPPORT;
    }
    int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, remote, false);
    if (SCOPE_RESULT < 0) {
        return SCOPE_RESULT;
    }

    if (remote.is_v4_mapped() && sock->local.is_unspecified()) {
        auto* route = ker::net::route_lookup(remote.ipv4_address());
        NetDeviceRef route_ref = sock->bound_ifindex != 0 ? netdev_find_by_ifindex(sock->bound_ifindex)
                                                          : (route != nullptr ? netdev_try_retain(route->dev_identity) : NetDeviceRef{});
        if (!route_ref) {
            return -EHOSTUNREACH;
        }
        auto* nif = netif_find_by_dev(route_ref.get());
        if (nif == nullptr || nif->ipv4_addr_count == 0) {
            return -EADDRNOTAVAIL;
        }
        sock->local = SocketEndpoint::mapped_ipv4(nif->ipv4_addrs.front().addr, sock->local.port);
    }

    if (remote.is_ipv6() && !remote.is_v4_mapped()) {
        IPv6Address requested{};
        const IPv6Address* requested_ptr = nullptr;
        if (!sock->local.is_unspecified()) {
            requested = sock->local.ipv6_address();
            requested_ptr = &requested;
        }
        IPv6OutputRoute route{};
        uint32_t const IFINDEX = sock->bound_ifindex != 0 ? sock->bound_ifindex : remote.scope_id;
        int const ROUTE_RESULT = ipv6_route_resolve(remote.ipv6_address(), requested_ptr, IFINDEX, route);
        if (ROUTE_RESULT < 0) {
            return ROUTE_RESULT;
        }
        uint32_t const ROUTE_IFINDEX = route.device ? route.device->ifindex : 0;
        if (sock->local.is_unspecified()) {
            sock->local = SocketEndpoint::ipv6(route.source, sock->local.port, ROUTE_IFINDEX);
        }
        remote = SocketEndpoint::ipv6(remote.ipv6_address(), remote.port, ROUTE_IFINDEX);
    }

    udp_lock.lock();
    int const BIND_RESULT = ensure_udp_bound_locked(sock);
    if (BIND_RESULT == 0) {
        sock->remote = remote;
        sock->state = SocketState::CONNECTED;
    }
    udp_lock.unlock();
    return BIND_RESULT;
}

auto udp_send(Socket* sock, const void* buf, size_t len, int /*unused*/) -> ssize_t {
    if (sock == nullptr || sock->state != SocketState::CONNECTED) {
        return -ENOTCONN;
    }
    return udp_transmit(sock, sock->remote, buf, len);
}

auto udp_recv(Socket* sock, void* buf, size_t len, int flags) -> ssize_t { return udp_recvfrom(sock, buf, len, flags, nullptr, nullptr); }

auto udp_sendto(Socket* sock, const void* buf, size_t len, int /*unused*/, const void* addr_raw, size_t addr_len) -> ssize_t {
    SocketEndpoint destination{};
    if (sock == nullptr || !socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &destination) || destination.port == 0) {
        return -EINVAL;
    }
    int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, destination, false);
    if (SCOPE_RESULT < 0) {
        return SCOPE_RESULT;
    }
    int const PREFLIGHT_RESULT = udp_send_preflight(sock, buf, len);
    if (PREFLIGHT_RESULT < 0) {
        return PREFLIGHT_RESULT;
    }
    udp_lock.lock();
    int const BIND_RESULT = ensure_udp_bound_locked(sock);
    udp_lock.unlock();
    if (BIND_RESULT < 0) {
        return BIND_RESULT;
    }
    return udp_transmit_prechecked(sock, destination, buf, len);
}

auto udp_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* addr_raw, size_t* addr_len) -> ssize_t {
    if (sock == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    if (sock->read_shutdown) {
        return 0;
    }
    int const PENDING_ERROR = sock->pending_error.exchange(0, std::memory_order_acq_rel);
    if (PENDING_ERROR != 0) {
        return -PENDING_ERROR;
    }
    if (sock->rcvbuf.available() < sizeof(UdpRecvRecord)) {
        if (!socket_call_nonblock(sock, flags)) {
            if (!socket_defer_wait(sock, "udp_wait")) {
                return -ENOMEM;
            }
        }
        return -EAGAIN;
    }

    UdpRecvRecord record{};
    ssize_t const HDR_N = sock->rcvbuf.read(&record, sizeof(record));
    if (std::cmp_not_equal(HDR_N, sizeof(record))) {
        return -EIO;
    }
    size_t const PAYLOAD_LEN = record.payload_len;
    if (PAYLOAD_LEN > sock->rcvbuf.capacity || sock->rcvbuf.available() < PAYLOAD_LEN) {
        return -EIO;
    }
    if (addr_raw != nullptr && addr_len != nullptr) {
        size_t const CAPACITY = *addr_len;
        static_cast<void>(socket_fill_sockaddr_endpoint(addr_raw, CAPACITY, addr_len, record.source));
    }

    size_t const TO_COPY = std::min(len, PAYLOAD_LEN);
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
        }
    }
    udp_lock.unlock();
    sock->state = SocketState::CLOSED;
}

int udp_shutdown(Socket* sock, int how) {
    if (sock == nullptr || how < SHUT_RD || how > SHUT_RDWR) {
        return -EINVAL;
    }
    if (how == SHUT_RD || how == SHUT_RDWR) {
        sock->read_shutdown = true;
    }
    if (how == SHUT_WR || how == SHUT_RDWR) {
        sock->write_shutdown = true;
    }
    socket_wake_waiters(sock);
    return 0;
}

int udp_setsockopt(Socket* sock, int level, int optname, const void* optval, size_t optlen) {
    int optint = 0;
    if (optval != nullptr && optlen >= sizeof(optint)) {
        std::memcpy(&optint, optval, sizeof(optint));
    }
    if (level == SOL_IPV6_LEVEL && optname == IPV6_V6ONLY) {
        if (sock->domain != SOCKADDR_V6_FAMILY) {
            return -ENOPROTOOPT;
        }
        if (optval == nullptr || optlen < sizeof(int)) {
            return -EINVAL;
        }
        if (sock->state != SocketState::UNBOUND || sock->local.port != 0) {
            return -EINVAL;
        }
        sock->ipv6_v6only = optint != 0;
        return 0;
    }
    if (level == SOL_IPV6_LEVEL && (optname == IPV6_UNICAST_HOPS || optname == IPV6_MULTICAST_HOPS)) {
        if (sock->domain != SOCKADDR_V6_FAMILY || optval == nullptr || optlen < sizeof(int) || optint < -1 || optint > 255) {
            return -EINVAL;
        }
        int const EFFECTIVE = optint < 0 ? (optname == IPV6_UNICAST_HOPS ? UDP_IPV6_HOP_LIMIT : 1) : optint;
        if (optname == IPV6_UNICAST_HOPS) {
            sock->ipv6_unicast_hops = EFFECTIVE;
        } else {
            sock->ipv6_multicast_hops = EFFECTIVE;
        }
        return 0;
    }
    if (level == SOL_SOCKET_LEVEL && optname == SO_REUSEADDR && optlen >= sizeof(int)) {
        sock->reuse_addr = optint != 0;
        return 0;
    }
    if (level == SOL_SOCKET_LEVEL && optname == SO_REUSEPORT && optlen >= sizeof(int)) {
        sock->reuse_port = optint != 0;
        return 0;
    }
    if (level == SOL_SOCKET_LEVEL && optname == SO_RCVBUF && optlen >= sizeof(int)) {
        return socket_resize_rcvbuf(sock, static_cast<size_t>(optint));
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
        NetDeviceRef dev = netdev_find_by_name_ref(ifname.data());
        if (!dev) {
            return -ENODEV;
        }
        int const INTERFACE_RESULT = socket_validate_bound_interface(sock, dev->ifindex);
        if (INTERFACE_RESULT < 0) {
            return INTERFACE_RESULT;
        }
        sock->bound_ifindex = dev->ifindex;
        return 0;
    }
    return 0;
}

int udp_getsockopt(Socket* sock, int level, int optname, void* optval, size_t* optlen) {
    if (optval == nullptr || optlen == nullptr || *optlen < sizeof(int)) {
        return -EINVAL;
    }
    int value = 0;
    if (level == SOL_IPV6_LEVEL && optname == IPV6_V6ONLY && sock->domain == SOCKADDR_V6_FAMILY) {
        value = sock->ipv6_v6only ? 1 : 0;
    } else if (level == SOL_IPV6_LEVEL && optname == IPV6_UNICAST_HOPS && sock->domain == SOCKADDR_V6_FAMILY) {
        value = sock->ipv6_unicast_hops;
    } else if (level == SOL_IPV6_LEVEL && optname == IPV6_MULTICAST_HOPS && sock->domain == SOCKADDR_V6_FAMILY) {
        value = sock->ipv6_multicast_hops;
    } else if (level == SOL_SOCKET_LEVEL && optname == SO_RCVBUF) {
        value = static_cast<int>(sock->rcvbuf.capacity);
    } else if (level == SOL_SOCKET_LEVEL && optname == SO_ERROR) {
        value = sock->pending_error.exchange(0, std::memory_order_acq_rel);
    } else {
        return 0;
    }
    std::memcpy(optval, &value, sizeof(value));
    *optlen = sizeof(value);
    return 0;
}

int udp_poll_check(Socket* sock, int events) {
    int ready = 0;
    if ((events & POLLIN) != 0 && (sock->read_shutdown || sock->rcvbuf.available() >= sizeof(UdpRecvRecord))) {
        ready |= POLLIN;
    }
    if ((events & POLLOUT) != 0 && !sock->write_shutdown) {
        ready |= POLLOUT;
    }
    if (sock->pending_error.load(std::memory_order_acquire) != 0) {
        ready |= POLLERR;
    }
    if (sock->read_shutdown && sock->write_shutdown) {
        ready |= POLLHUP;
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
    if (pkt == nullptr || pkt->len < sizeof(UdpHeader)) {
        pkt_free(pkt);
        return;
    }
    const auto* hdr = reinterpret_cast<const UdpHeader*>(pkt->data);
    uint16_t const UDP_LEN = ntohs(hdr->length);
    if (UDP_LEN < sizeof(UdpHeader) || UDP_LEN > pkt->len) {
        pkt_free(pkt);
        return;
    }
    if (hdr->checksum != 0 && checksum_pseudo_ipv4(src_ip, dst_ip, IPPROTO_UDP, UDP_LEN, pkt->data, UDP_LEN) != 0) {
        pkt_free(pkt);
        return;
    }
    uint16_t const DST_PORT = ntohs(hdr->dst_port);
    uint16_t const SRC_PORT = ntohs(hdr->src_port);
    size_t const DATA_LEN = UDP_LEN - sizeof(UdpHeader);
    SocketEndpoint const DESTINATION = SocketEndpoint::ipv4(dst_ip, DST_PORT);
    SocketEndpoint const SOURCE = SocketEndpoint::ipv4(src_ip, SRC_PORT);

    Socket* wake_sock = nullptr;
    udp_lock.lock();
    auto* binding = find_binding_locked(DESTINATION, SOURCE, dev);
    if (binding != nullptr) {
        SocketEndpoint const DELIVERED_SOURCE = endpoint_for_v4_socket(binding->sock, src_ip, SRC_PORT);
        wake_sock = queue_datagram_locked(*binding, DELIVERED_SOURCE, pkt->data + sizeof(UdpHeader), DATA_LEN);
    }
    udp_lock.unlock();
    if (wake_sock != nullptr) {
        socket_wake_waiters(wake_sock);
        socket_release(wake_sock);
    }
    pkt_free(pkt);
}

auto udp_rx_v6(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) -> UdpRxV6Result {
    if (pkt == nullptr || pkt->len < sizeof(UdpHeader)) {
        pkt_free(pkt);
        return UdpRxV6Result::Consumed;
    }
    const auto* hdr = reinterpret_cast<const UdpHeader*>(pkt->data);
    uint16_t const UDP_LEN = ntohs(hdr->length);
    if (UDP_LEN < sizeof(UdpHeader) || UDP_LEN != pkt->len || hdr->checksum == 0 ||
        checksum_pseudo_ipv6(info.src, info.dst, IPV6_PROTO_UDP, UDP_LEN, pkt->data, UDP_LEN) != 0) {
        pkt_free(pkt);
        return UdpRxV6Result::Consumed;
    }
    uint16_t const DST_PORT = ntohs(hdr->dst_port);
    uint16_t const SRC_PORT = ntohs(hdr->src_port);
    SocketEndpoint const DESTINATION = SocketEndpoint::ipv6(info.dst, DST_PORT, dev != nullptr ? dev->ifindex : 0);
    SocketEndpoint const SOURCE = SocketEndpoint::ipv6(info.src, SRC_PORT, dev != nullptr ? dev->ifindex : 0);

    Socket* wake_sock = nullptr;
    bool matched = false;
    udp_lock.lock();
    auto* binding = find_binding_locked(DESTINATION, SOURCE, dev);
    if (binding != nullptr) {
        matched = true;
        wake_sock = queue_datagram_locked(*binding, SOURCE, pkt->data + sizeof(UdpHeader), UDP_LEN - sizeof(UdpHeader));
    }
    udp_lock.unlock();
    if (!matched) {
        return UdpRxV6Result::NoPort;
    }
    if (wake_sock != nullptr) {
        socket_wake_waiters(wake_sock);
        socket_release(wake_sock);
    }
    pkt_free(pkt);
    return UdpRxV6Result::Consumed;
}

void udp_error_v6(const IPv6Address& local_addr, uint16_t local_port, const IPv6Address& remote_addr, uint16_t remote_port, uint8_t type,
                  uint8_t code, uint32_t /*mtu*/, uint32_t scope_id) {
    int const ERROR = type == 2 ? EMSGSIZE : (type == 1 && code == 4 ? ECONNREFUSED : EHOSTUNREACH);
    SocketEndpoint const LOCAL = SocketEndpoint::ipv6(local_addr, local_port, scope_id);
    SocketEndpoint const REMOTE = SocketEndpoint::ipv6(remote_addr, remote_port, scope_id);
    std::array<Socket*, MAX_UDP_SOCKETS> wake{};
    size_t wake_count = 0;
    udp_lock.lock();
    for (const auto& binding : udp_bindings) {
        Socket* sock = binding.sock;
        if (sock == nullptr || sock->state != SocketState::CONNECTED || sock->local.port != LOCAL.port ||
            sock->remote.port != REMOTE.port || !socket_endpoint_address_equal(sock->local, LOCAL) ||
            !socket_endpoint_address_equal(sock->remote, REMOTE) || !socket_try_acquire(sock)) {
            continue;
        }
        sock->pending_error.store(ERROR, std::memory_order_release);
        if (wake_count < wake.size()) {
            wake.at(wake_count++) = sock;
        } else {
            socket_release(sock);
        }
    }
    udp_lock.unlock();
    for (size_t i = 0; i < wake_count; ++i) {
        socket_wake_waiters(wake.at(i));
        socket_release(wake.at(i));
    }
}

auto get_udp_proto_ops() -> SocketProtoOps* { return &udp_ops; }

}  // namespace ker::net::proto
