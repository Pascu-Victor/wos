#include "raw.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/proto/icmp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <net/socket.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>
#include <utility>

namespace ker::net::proto {

namespace {
struct RawRecvRecord {
    uint16_t packet_len{};
    uint16_t reserved{};
    SocketEndpoint source{};
};
static_assert(sizeof(RawRecvRecord) == 28);

constexpr size_t MAX_RAW_SOCKETS = 64;
constexpr size_t ICMP_ECHO_HEADER_LEN = 8;
constexpr uint8_t IPV4_VERSION = 4;
constexpr uint8_t IPV4_IHL_NO_OPTIONS = 5;
constexpr uint16_t IPV4_FLAG_DONT_FRAGMENT = 0x4000;
constexpr uint8_t IPV6_DEFAULT_HOP_LIMIT = 64;
constexpr int SO_RCVBUF = 8;
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
constexpr int POLLHUP = 0x010;

std::array<Socket*, MAX_RAW_SOCKETS> raw_sockets{};
ker::mod::sys::Spinlock raw_sockets_lock;

auto raw_netdev_find_by_ifindex(uint32_t ifindex) -> NetDeviceRef {
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

auto register_raw_socket(Socket* sock) -> int {
    raw_sockets_lock.lock();
    for (auto* registered : raw_sockets) {
        if (registered == sock) {
            raw_sockets_lock.unlock();
            return 0;
        }
    }
    for (auto& registered : raw_sockets) {
        if (registered == nullptr) {
            registered = sock;
            raw_sockets_lock.unlock();
            return 0;
        }
    }
    raw_sockets_lock.unlock();
    return -ENOBUFS;
}

void unregister_raw_socket(Socket* sock) {
    raw_sockets_lock.lock();
    for (auto& registered : raw_sockets) {
        if (registered == sock) {
            registered = nullptr;
        }
    }
    raw_sockets_lock.unlock();
}

auto raw_sendto_endpoint(Socket* sock, const void* buf, size_t len, const SocketEndpoint& destination) -> ssize_t {
    if (sock == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    if (sock->write_shutdown) {
        return -EPIPE;
    }
    if (len > PKT_BUF_SIZE - PKT_HEADROOM) {
        return -EMSGSIZE;
    }

    if (destination.is_v4_mapped() || destination.is_ipv4()) {
        if (destination.is_v4_mapped() && sock->ipv6_v6only) {
            return -EAFNOSUPPORT;
        }
        auto* pkt = pkt_alloc_tx();
        if (pkt == nullptr) {
            return -ENOBUFS;
        }
        std::memcpy(pkt->put(len), buf, len);
        int result = 0;
        if (sock->bound_ifindex != 0) {
            NetDeviceRef device = raw_netdev_find_by_ifindex(sock->bound_ifindex);
            auto* raw_device = device.get();
            if (!device || !pkt_adopt_netdev_ref(pkt, std::move(device))) {
                pkt_free(pkt);
                return -ENODEV;
            }
            auto* nif = netif_find_by_dev(raw_device);
            IPv4Address source = sock->local.ipv4_address();
            if (source.is_any()) {
                if (nif == nullptr || nif->ipv4_addr_count == 0) {
                    pkt_free(pkt);
                    return -EADDRNOTAVAIL;
                }
                source = nif->ipv4_addrs.front().addr;
            }
            result =
                ipv4_tx_on_dev(pkt, raw_device, source, destination.ipv4_address(), static_cast<uint8_t>(sock->protocol), IPV4_DEFAULT_TTL);
        } else {
            result = ipv4_tx_auto(pkt, destination.ipv4_address(), static_cast<uint8_t>(sock->protocol));
        }
        return result < 0 ? static_cast<ssize_t>(result) : static_cast<ssize_t>(len);
    }

    IPv6Address requested{};
    const IPv6Address* requested_ptr = nullptr;
    if (!sock->local.is_unspecified()) {
        requested = sock->local.ipv6_address();
        requested_ptr = &requested;
    }
    uint32_t const IFINDEX = sock->bound_ifindex != 0 ? sock->bound_ifindex : destination.scope_id;
    IPv6OutputRoute route{};
    int const ROUTE_RESULT = ipv6_route_resolve(destination.ipv6_address(), requested_ptr, IFINDEX, route);
    if (ROUTE_RESULT < 0) {
        return ROUTE_RESULT;
    }
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return -ENOBUFS;
    }
    std::memcpy(pkt->put(len), buf, len);
    if (sock->protocol == IPV6_PROTO_ICMPV6 && len >= 4) {
        uint16_t checksum = 0;
        std::memcpy(&checksum, pkt->data + 2, sizeof(checksum));
        if (checksum == 0) {
            checksum = checksum_pseudo_ipv6(route.source, destination.ipv6_address(), IPV6_PROTO_ICMPV6, static_cast<uint32_t>(len),
                                            pkt->data, pkt->len);
            std::memcpy(pkt->data + 2, &checksum, sizeof(checksum));
        }
    }
    int const CONFIGURED_HOPS = destination.ipv6_address().is_multicast() ? sock->ipv6_multicast_hops : sock->ipv6_unicast_hops;
    uint8_t const HOP_LIMIT = static_cast<uint8_t>(std::clamp(CONFIGURED_HOPS, 0, 255));
    int const RESULT = ipv6_tx_routed(pkt, std::move(route), destination.ipv6_address(), static_cast<uint8_t>(sock->protocol), HOP_LIMIT);
    return RESULT < 0 ? static_cast<ssize_t>(RESULT) : static_cast<ssize_t>(len);
}

auto raw_sendto(Socket* sock, const void* buf, size_t len, int /*flags*/, const void* addr_raw, size_t addr_len) -> ssize_t {
    SocketEndpoint destination{};
    if (sock == nullptr || !socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &destination)) {
        return -EINVAL;
    }
    int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, destination, false);
    if (SCOPE_RESULT < 0) {
        return SCOPE_RESULT;
    }
    return raw_sendto_endpoint(sock, buf, len, destination);
}

auto raw_send(Socket* sock, const void* buf, size_t len, int /*flags*/) -> ssize_t {
    if (sock == nullptr || sock->state != SocketState::CONNECTED) {
        return -ENOTCONN;
    }
    return raw_sendto_endpoint(sock, buf, len, sock->remote);
}

auto raw_recvfrom(Socket* sock, void* buf, size_t len, int flags, void* addr_out, size_t* addr_len) -> ssize_t {
    if (sock == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    if (sock->read_shutdown) {
        return 0;
    }
    if (sock->rcvbuf.available() < sizeof(RawRecvRecord)) {
        if (!socket_call_nonblock(sock, flags)) {
            if (!socket_defer_wait(sock, "raw_wait")) {
                return -ENOMEM;
            }
        }
        return -EAGAIN;
    }
    RawRecvRecord record{};
    if (std::cmp_not_equal(sock->rcvbuf.read(&record, sizeof(record)), sizeof(record))) {
        return -EIO;
    }
    size_t const PACKET_LEN = record.packet_len;
    if (PACKET_LEN == 0 || PACKET_LEN > sock->rcvbuf.capacity || sock->rcvbuf.available() < PACKET_LEN) {
        return -EIO;
    }
    if (addr_out != nullptr && addr_len != nullptr) {
        size_t const CAPACITY = *addr_len;
        static_cast<void>(socket_fill_sockaddr_endpoint(addr_out, CAPACITY, addr_len, record.source));
    }
    size_t const TO_COPY = std::min(len, PACKET_LEN);
    ssize_t const N = sock->rcvbuf.read(buf, TO_COPY);
    if (std::cmp_not_equal(N, TO_COPY)) {
        return -EIO;
    }
    std::array<uint8_t, 256> discard{};
    size_t remaining = PACKET_LEN - TO_COPY;
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

auto raw_recv(Socket* sock, void* buf, size_t len, int flags) -> ssize_t { return raw_recvfrom(sock, buf, len, flags, nullptr, nullptr); }

auto raw_bind(Socket* sock, const void* addr_raw, size_t addr_len) -> int {
    if (sock == nullptr) {
        return -EINVAL;
    }
    SocketEndpoint local = sock->local;
    if (addr_raw != nullptr) {
        if (!socket_state_allows_explicit_bind(sock->state)) {
            return -EINVAL;
        }
        if (!socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &local)) {
            return -EINVAL;
        }
        int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, local, true);
        if (SCOPE_RESULT < 0) {
            return SCOPE_RESULT;
        }
    }
    int const REGISTER_RESULT = register_raw_socket(sock);
    if (REGISTER_RESULT < 0) {
        return REGISTER_RESULT;
    }
    if (addr_raw != nullptr) {
        sock->local = local;
        sock->state = SocketState::BOUND;
    }
    return 0;
}

auto raw_connect(Socket* sock, const void* addr_raw, size_t addr_len, int /*flags*/) -> int {
    SocketEndpoint remote{};
    if (sock == nullptr || !socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &remote)) {
        return -EINVAL;
    }
    if (remote.is_v4_mapped() && sock->ipv6_v6only) {
        return -EAFNOSUPPORT;
    }
    int const SCOPE_RESULT = socket_prepare_endpoint_scope(sock, remote, false);
    if (SCOPE_RESULT < 0) {
        return SCOPE_RESULT;
    }
    if (remote.is_ipv6() && !remote.is_v4_mapped()) {
        IPv6Address requested{};
        const IPv6Address* requested_ptr = nullptr;
        if (!sock->local.is_unspecified()) {
            requested = sock->local.ipv6_address();
            requested_ptr = &requested;
        }
        uint32_t const IFINDEX = sock->bound_ifindex != 0 ? sock->bound_ifindex : remote.scope_id;
        IPv6OutputRoute route{};
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
    sock->remote = remote;
    sock->state = SocketState::CONNECTED;
    return 0;
}

auto raw_shutdown(Socket* sock, int how) -> int {
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

auto raw_close(Socket* sock) -> void {
    unregister_raw_socket(sock);
    sock->state = SocketState::CLOSED;
}

auto raw_setsockopt(Socket* sock, int level, int optname, const void* optval, size_t optlen) -> int {
    int value = 0;
    if (optval != nullptr && optlen >= sizeof(value)) {
        std::memcpy(&value, optval, sizeof(value));
    }
    if (level == SOL_IPV6_LEVEL && optname == IPV6_V6ONLY) {
        if (sock->domain != SOCKADDR_V6_FAMILY) {
            return -ENOPROTOOPT;
        }
        if (optval == nullptr || optlen < sizeof(value) || sock->state != SocketState::UNBOUND) {
            return -EINVAL;
        }
        sock->ipv6_v6only = value != 0;
        return 0;
    }
    if (level == SOL_IPV6_LEVEL && (optname == IPV6_UNICAST_HOPS || optname == IPV6_MULTICAST_HOPS)) {
        if (sock->domain != SOCKADDR_V6_FAMILY || optval == nullptr || optlen < sizeof(value) || value < -1 || value > 255) {
            return -EINVAL;
        }
        int const EFFECTIVE = value < 0 ? (optname == IPV6_UNICAST_HOPS ? IPV6_DEFAULT_HOP_LIMIT : 1) : value;
        if (optname == IPV6_UNICAST_HOPS) {
            sock->ipv6_unicast_hops = EFFECTIVE;
        } else {
            sock->ipv6_multicast_hops = EFFECTIVE;
        }
        return 0;
    }
    if (level == SOL_SOCKET_LEVEL && optname == SO_RCVBUF && optlen >= sizeof(value)) {
        return socket_resize_rcvbuf(sock, static_cast<size_t>(value));
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

auto raw_getsockopt(Socket* sock, int level, int optname, void* optval, size_t* optlen) -> int {
    if (optval == nullptr || optlen == nullptr || *optlen < sizeof(int)) {
        return -EINVAL;
    }
    int value = 0;
    if (level == SOL_IPV6_LEVEL && sock->domain == SOCKADDR_V6_FAMILY && optname == IPV6_V6ONLY) {
        value = sock->ipv6_v6only ? 1 : 0;
    } else if (level == SOL_IPV6_LEVEL && sock->domain == SOCKADDR_V6_FAMILY && optname == IPV6_UNICAST_HOPS) {
        value = sock->ipv6_unicast_hops;
    } else if (level == SOL_IPV6_LEVEL && sock->domain == SOCKADDR_V6_FAMILY && optname == IPV6_MULTICAST_HOPS) {
        value = sock->ipv6_multicast_hops;
    } else if (level == SOL_SOCKET_LEVEL && optname == SO_RCVBUF) {
        value = static_cast<int>(sock->rcvbuf.capacity);
    }
    std::memcpy(optval, &value, sizeof(value));
    *optlen = sizeof(value);
    return 0;
}

auto raw_poll_check(Socket* sock, int events) -> int {
    int ready = 0;
    if ((events & POLLIN) != 0 && (sock->read_shutdown || sock->rcvbuf.available() >= sizeof(RawRecvRecord))) {
        ready |= POLLIN;
    }
    if ((events & POLLOUT) != 0 && !sock->write_shutdown) {
        ready |= POLLOUT;
    }
    if (sock->read_shutdown && sock->write_shutdown) {
        ready |= POLLHUP;
    }
    return ready;
}

auto raw_icmp_id_matches(const Socket* sock, uint8_t protocol, const uint8_t* payload, size_t payload_len) -> bool {
    if ((protocol != IPPROTO_ICMP && protocol != IPV6_PROTO_ICMPV6) || payload_len < ICMP_ECHO_HEADER_LEN || sock->owner_pid == 0) {
        return true;
    }
    uint8_t const TYPE = payload[0];
    bool const ECHO = protocol == IPPROTO_ICMP ? (TYPE == ICMP_ECHO_REPLY || TYPE == ICMP_ECHO_REQUEST) : (TYPE == 128 || TYPE == 129);
    if (!ECHO) {
        return true;
    }
    uint16_t const ID = (static_cast<uint16_t>(payload[4]) << 8U) | payload[5];
    return ID == static_cast<uint16_t>(sock->owner_pid);
}

void deliver_raw_frame(PacketBuffer* pkt, const SocketEndpoint& source, const SocketEndpoint& destination, uint32_t ingress_ifindex,
                       uint8_t protocol, const uint8_t* match_payload, size_t match_len) {
    RawRecvRecord record{.packet_len = static_cast<uint16_t>(pkt->len), .source = source};
    std::array<Socket*, MAX_RAW_SOCKETS> sockets_to_wake{};
    size_t wake_count = 0;
    raw_sockets_lock.lock();
    for (auto* sock : raw_sockets) {
        if (sock == nullptr || sock->read_shutdown || !std::cmp_equal(sock->protocol, protocol) ||
            (sock->domain == SOCKADDR_V6_FAMILY) != source.is_ipv6() || !raw_icmp_id_matches(sock, protocol, match_payload, match_len) ||
            !raw_delivery_endpoint_matches(sock->local, sock->remote, sock->state == SocketState::CONNECTED, sock->bound_ifindex,
                                           ingress_ifindex, source, destination) ||
            sock->rcvbuf.free_space() < sizeof(record) + pkt->len || !socket_try_acquire(sock)) {
            continue;
        }
        ssize_t const WRITTEN = sock->rcvbuf.write_pair(&record, sizeof(record), pkt->data, pkt->len);
        if (std::cmp_equal(WRITTEN, sizeof(record) + pkt->len) && wake_count < sockets_to_wake.size()) {
            sockets_to_wake.at(wake_count++) = sock;
        } else {
            socket_release(sock);
        }
    }
    raw_sockets_lock.unlock();
    for (auto* sock : std::span<Socket*>{sockets_to_wake.data(), wake_count}) {
        socket_wake_waiters(sock);
        socket_release(sock);
    }
    pkt_free(pkt);
}

SocketProtoOps raw_proto_ops = {
    .bind = raw_bind,
    .listen = nullptr,
    .accept = nullptr,
    .connect = raw_connect,
    .send = raw_send,
    .recv = raw_recv,
    .sendto = raw_sendto,
    .recvfrom = raw_recvfrom,
    .close = raw_close,
    .shutdown = raw_shutdown,
    .setsockopt = raw_setsockopt,
    .getsockopt = raw_getsockopt,
    .poll_check = raw_poll_check,
};
}  // namespace

void raw_deliver(PacketBuffer* pkt, uint8_t protocol, IPv4Address src_ip, IPv4Address dst_ip, uint8_t ttl) {
    if (pkt == nullptr || pkt->len + sizeof(IPv4Header) > UINT16_MAX || pkt->headroom() < sizeof(IPv4Header)) {
        pkt_free(pkt);
        return;
    }
    const uint8_t* match_payload = pkt->data;
    size_t const match_len = pkt->len;
    uint32_t const INGRESS_IFINDEX = pkt->dev != nullptr ? pkt->dev->ifindex : 0;
    auto* ip = reinterpret_cast<IPv4Header*>(pkt->push(sizeof(IPv4Header)));
    *ip = IPv4Header{};
    ip->ihl_version = static_cast<uint8_t>((IPV4_VERSION << 4U) | IPV4_IHL_NO_OPTIONS);
    ip->total_len = htons(static_cast<uint16_t>(pkt->len));
    ip->flags_fragoff = htons(IPV4_FLAG_DONT_FRAGMENT);
    ip->ttl = ttl;
    ip->protocol = protocol;
    ip->src_addr = src_ip.to_network_order();
    ip->dst_addr = dst_ip.to_network_order();
    ip->checksum = checksum_compute(ip, sizeof(*ip));
    deliver_raw_frame(pkt, SocketEndpoint::ipv4(src_ip), SocketEndpoint::ipv4(dst_ip), INGRESS_IFINDEX, protocol, match_payload, match_len);
}

void raw_deliver_v6(PacketBuffer* pkt, const IPv6RxInfo& info, uint32_t scope_id) {
    if (pkt == nullptr || pkt->len > UINT16_MAX - sizeof(IPv6Header) || pkt->headroom() < sizeof(IPv6Header)) {
        pkt_free(pkt);
        return;
    }
    const uint8_t* match_payload = pkt->data;
    size_t const match_len = pkt->len;
    uint16_t const PAYLOAD_LEN = static_cast<uint16_t>(pkt->len);
    auto* ip = reinterpret_cast<IPv6Header*>(pkt->push(sizeof(IPv6Header)));
    *ip = IPv6Header{};
    ip->version_tc_flow = htonl(6U << 28U);
    ip->payload_length = htons(PAYLOAD_LEN);
    ip->next_header = info.next_header;
    ip->hop_limit = info.hop_limit;
    ip->src = info.src;
    ip->dst = info.dst;
    deliver_raw_frame(pkt, SocketEndpoint::ipv6(info.src, 0, scope_id), SocketEndpoint::ipv6(info.dst, 0, scope_id), scope_id,
                      info.next_header, match_payload, match_len);
}

auto get_raw_proto_ops() -> SocketProtoOps* { return &raw_proto_ops; }

}  // namespace ker::net::proto
