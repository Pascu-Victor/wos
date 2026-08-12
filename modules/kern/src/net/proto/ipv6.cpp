#include "ipv6.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ndp.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <net/route6.hpp>
#include <utility>

namespace ker::net::proto {

const std::array<uint8_t, 13> IPV6_SOLICITED_NODE_PREFIX = {0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF};
const IPv6Address IPV6_ALL_NODES_MULTICAST =
    IPv6Address::from_bytes({0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01});
const IPv6Address IPV6_ALL_ROUTERS_MULTICAST =
    IPv6Address::from_bytes({0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02});
const IPv6Address IPV6_UNSPECIFIED = IPv6Address::unspecified();
const std::array<uint8_t, 2> IPV6_LINK_LOCAL_PREFIX = {0xFE, 0x80};

namespace {
constexpr size_t IPV6_PAYLOAD_LENGTH_OFFSET = 4;
constexpr size_t IPV6_NEXT_HEADER_OFFSET = 6;
constexpr size_t IPV6_HOP_LIMIT_OFFSET = 7;
constexpr size_t IPV6_SOURCE_OFFSET = 8;
constexpr size_t IPV6_DESTINATION_OFFSET = 24;

auto extension_options_valid(const uint8_t* data, size_t length) -> bool {
    size_t offset = 2;
    while (offset < length) {
        uint8_t const TYPE = data[offset];
        if (TYPE == 0) {  // Pad1
            ++offset;
            continue;
        }
        if (offset + 2 > length) {
            return false;
        }
        size_t const OPTION_LENGTH = static_cast<size_t>(data[offset + 1]);
        if (OPTION_LENGTH > length - offset - 2) {
            return false;
        }
        // Unknown options whose high bits are not 00 require ICMP processing
        // that is deliberately unsupported in this bounded parser.
        if (TYPE != 1 && (TYPE & 0xC0U) != 0) {
            return false;
        }
        offset += OPTION_LENGTH + 2;
    }
    return offset == length;
}

auto is_our_address(NetDevice* dev, const IPv6Address& address) -> bool {
    if (dev == nullptr) {
        return false;
    }
    if (address == IPV6_ALL_NODES_MULTICAST || address == IPV6_ALL_ROUTERS_MULTICAST) {
        return true;
    }
    IPv6Addr owned{};
    if (netif_ipv6_find(dev, address, owned) && (owned.state == IPv6Addr::State::PREFERRED || owned.state == IPv6Addr::State::DEPRECATED)) {
        return true;
    }
    if (!address.is_link_local_multicast()) {
        return false;
    }
    std::array<IPv6Addr, MAX_ADDRS_PER_IF> addresses{};
    size_t const COUNT = netif_ipv6_snapshot(dev, addresses.data(), addresses.size());
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& candidate = addresses.at(i);
        if (candidate.state != IPv6Addr::State::DADFAILED && address == ipv6_make_solicited_node(candidate.addr)) {
            return true;
        }
    }
    return false;
}

auto write_ipv6_header(PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit) -> int {
    if (pkt == nullptr || pkt->len > UINT16_MAX || pkt->headroom() < IPV6_HLEN) {
        return -EMSGSIZE;
    }
    uint16_t const PAYLOAD_LENGTH = static_cast<uint16_t>(pkt->len);
    auto* header = reinterpret_cast<IPv6Header*>(pkt->push(IPV6_HLEN));
    std::memset(header, 0, sizeof(*header));
    header->version_tc_flow = htonl(uint32_t{6} << 28U);
    header->payload_length = htons(PAYLOAD_LENGTH);
    header->next_header = next_header;
    header->hop_limit = hop_limit;
    header->src = src;
    header->dst = dst;
    return 0;
}

auto deliver_local(PacketBuffer* pkt, const IPv6Address& dst) -> int {
    IPv6Addr address{};
    NetDeviceRef device{};
    if (!netif_ipv6_find_owner(dst, address, device)) {
        return -ENOENT;
    }
    if (pkt->retained_netdev.valid() && pkt->retained_netdev != device.identity()) {
        pkt_release_netdev(pkt);
    }
    if (!pkt_adopt_netdev_ref(pkt, std::move(device))) {
        return -ENODEV;
    }
    NetDevice* const DEV = pkt->dev;
    pkt->src_mac = DEV->mac;
    ipv6_rx(DEV, pkt);
    return 0;
}

auto emit_on_ref(PacketBuffer* pkt, NetDeviceRef&& device, const IPv6Address& source, const IPv6Address& next_hop) -> int {
    if (pkt == nullptr || !device) {
        pkt_free(pkt);
        return -ENODEV;
    }
    NetDeviceIdentity const IDENTITY = device.identity();
    if (pkt->retained_netdev.valid() && pkt->retained_netdev != IDENTITY) {
        pkt_release_netdev(pkt);
    }
    if (!pkt_adopt_netdev_ref(pkt, std::move(device))) {
        pkt_free(pkt);
        return -ENODEV;
    }
    NetDevice* const DEV = IDENTITY.device;
    if (next_hop.is_multicast()) {
        return eth_tx(DEV, pkt, ipv6_multicast_to_mac(next_hop), ETH_TYPE_IPV6);
    }
    MacAddress destination{};
    NdpResolveResult const RESULT = ndp_resolve(IDENTITY, next_hop, source, destination, pkt);
    if (RESULT == NdpResolveResult::RESOLVED) {
        return eth_tx(DEV, pkt, destination, ETH_TYPE_IPV6);
    }
    return RESULT == NdpResolveResult::QUEUED ? 0 : -EHOSTUNREACH;
}

auto device_for_ifindex(uint32_t ifindex) -> NetDeviceRef {
    size_t const COUNT = netdev_count();
    for (size_t i = 0; i < COUNT; ++i) {
        NetDeviceRef candidate = netdev_at_ref(i);
        if (candidate && candidate->ifindex == ifindex) {
            return candidate;
        }
    }
    return {};
}
}  // namespace

auto ipv6_make_link_local(const MacAddress& mac) -> IPv6Address {
    IPv6Address out{};
    out.bytes.at(0) = 0xFE;
    out.bytes.at(1) = 0x80;
    out.bytes.at(8) = static_cast<uint8_t>(mac.at(0) ^ 0x02U);
    out.bytes.at(9) = mac.at(1);
    out.bytes.at(10) = mac.at(2);
    out.bytes.at(11) = 0xFF;
    out.bytes.at(12) = 0xFE;
    out.bytes.at(13) = mac.at(3);
    out.bytes.at(14) = mac.at(4);
    out.bytes.at(15) = mac.at(5);
    return out;
}

auto ipv6_make_solicited_node(const IPv6Address& addr) -> IPv6Address {
    IPv6Address out{};
    std::ranges::copy(IPV6_SOLICITED_NODE_PREFIX, out.bytes.begin());
    out.bytes.at(13) = addr.bytes.at(13);
    out.bytes.at(14) = addr.bytes.at(14);
    out.bytes.at(15) = addr.bytes.at(15);
    return out;
}

auto ipv6_multicast_to_mac(const IPv6Address& ipv6_mcast) -> MacAddress {
    return MacAddress::from_bytes(
        {0x33, 0x33, ipv6_mcast.bytes.at(12), ipv6_mcast.bytes.at(13), ipv6_mcast.bytes.at(14), ipv6_mcast.bytes.at(15)});
}

namespace {
auto ipv6_parse_impl(const uint8_t* data, size_t length, IPv6RxInfo& out, bool allow_truncated_payload) -> IPv6ParseError {
    out = {};
    if (data == nullptr || length < IPV6_HLEN) {
        return IPv6ParseError::TRUNCATED;
    }
    if ((data[0] >> 4U) != 6) {
        return IPv6ParseError::BAD_VERSION;
    }
    uint16_t const PAYLOAD_LENGTH =
        static_cast<uint16_t>((static_cast<uint16_t>(data[IPV6_PAYLOAD_LENGTH_OFFSET]) << 8U) | data[IPV6_PAYLOAD_LENGTH_OFFSET + 1]);
    size_t const DECLARED_LENGTH = IPV6_HLEN + static_cast<size_t>(PAYLOAD_LENGTH);
    if (!allow_truncated_payload && length < DECLARED_LENGTH) {
        return IPv6ParseError::TRUNCATED;
    }
    size_t const LOGICAL_LENGTH = std::min(length, DECLARED_LENGTH);
    std::memcpy(out.src.data(), data + IPV6_SOURCE_OFFSET, out.src.size());
    std::memcpy(out.dst.data(), data + IPV6_DESTINATION_OFFSET, out.dst.size());
    out.hop_limit = data[IPV6_HOP_LIMIT_OFFSET];

    uint8_t next = data[IPV6_NEXT_HEADER_OFFSET];
    if (PAYLOAD_LENGTH == 0) {
        if (next != IPV6_PROTO_NO_NEXT) {
            return IPv6ParseError::BAD_PAYLOAD_LENGTH;  // Jumbograms are unsupported.
        }
        out.next_header = next;
        out.upper_layer_offset = IPV6_HLEN;
        out.upper_layer_length = 0;
        return IPv6ParseError::NONE;
    }
    size_t offset = IPV6_HLEN;
    size_t extension_count = 0;
    size_t extension_bytes = 0;
    bool saw_hop_by_hop = false;
    bool saw_destination = false;
    while (next == IPV6_PROTO_HOP_BY_HOP || next == IPV6_PROTO_DEST_OPTIONS) {
        if (++extension_count > IPV6_MAX_EXTENSION_HEADERS) {
            return IPv6ParseError::TOO_MANY_HEADERS;
        }
        if (next == IPV6_PROTO_HOP_BY_HOP) {
            if (saw_hop_by_hop || offset != IPV6_HLEN) {
                return IPv6ParseError::BAD_EXTENSION_ORDER;
            }
            saw_hop_by_hop = true;
        } else {
            if (saw_destination) {
                return IPv6ParseError::BAD_EXTENSION_ORDER;
            }
            saw_destination = true;
        }
        if (offset + 2 > LOGICAL_LENGTH) {
            return IPv6ParseError::TRUNCATED;
        }
        size_t const HEADER_LENGTH = (static_cast<size_t>(data[offset + 1]) + 1U) * 8U;
        if (HEADER_LENGTH > LOGICAL_LENGTH - offset) {
            return IPv6ParseError::TRUNCATED;
        }
        extension_bytes += HEADER_LENGTH;
        if (extension_bytes > IPV6_MAX_EXTENSION_BYTES) {
            return IPv6ParseError::TOO_MANY_EXTENSION_BYTES;
        }
        if (!extension_options_valid(data + offset, HEADER_LENGTH)) {
            return IPv6ParseError::UNSUPPORTED_EXTENSION;
        }
        next = data[offset];
        offset += HEADER_LENGTH;
    }

    switch (next) {
        case IPV6_PROTO_FRAGMENT:
            return IPv6ParseError::FRAGMENT_UNSUPPORTED;
        case IPV6_PROTO_ROUTING:
        case IPV6_PROTO_AH:
        case IPV6_PROTO_ESP:
            return IPv6ParseError::UNSUPPORTED_EXTENSION;
        case IPV6_PROTO_NO_NEXT:
            return IPv6ParseError::NO_NEXT_HEADER;
        case IPV6_PROTO_ICMPV6:
        case IPV6_PROTO_TCP:
        case IPV6_PROTO_UDP:
            break;
        default:
            return IPv6ParseError::UNSUPPORTED_PROTOCOL;
    }
    out.next_header = next;
    out.upper_layer_offset = offset;
    out.upper_layer_length = LOGICAL_LENGTH - offset;
    return IPv6ParseError::NONE;
}
}  // namespace

auto ipv6_parse(const uint8_t* data, size_t length, IPv6RxInfo& out) -> IPv6ParseError { return ipv6_parse_impl(data, length, out, false); }

auto ipv6_parse_quoted(const uint8_t* data, size_t length, IPv6RxInfo& out) -> IPv6ParseError {
    return ipv6_parse_impl(data, length, out, true);
}

void ipv6_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return;
    }
    IPv6RxInfo info{};
    if (ipv6_parse(pkt->data, pkt->len, info) != IPv6ParseError::NONE || !is_our_address(dev, info.dst)) {
        pkt_free(pkt);
        return;
    }
    // WOS does not expose transport multicast membership yet. The link-local
    // multicast groups admitted above are control-plane groups only; never
    // let TCP generate a response with a multicast source, and do not deliver
    // unsolicited UDP to them.
    if (info.dst.is_multicast() && info.next_header != IPV6_PROTO_ICMPV6) {
        pkt_free(pkt);
        return;
    }
    pkt->len = info.upper_layer_offset + info.upper_layer_length;
    pkt->pull(info.upper_layer_offset);
    switch (info.next_header) {
        case IPV6_PROTO_ICMPV6:
            icmpv6_rx(dev, pkt, info);
            return;
        case IPV6_PROTO_UDP:
            if (udp_rx_v6(dev, pkt, info) == UdpRxV6Result::NoPort) {
                icmpv6_send_port_unreachable(dev, pkt, info);
            }
            return;
        case IPV6_PROTO_TCP:
            tcp_rx_v6(dev, pkt, info);
            return;
        default:
            pkt_free(pkt);
            return;
    }
}

auto ipv6_route_resolve(const IPv6Address& dst, const IPv6Address* requested_src, uint32_t bound_ifindex, IPv6OutputRoute& out) -> int {
    if (dst.is_unspecified()) {
        return -EINVAL;
    }
    if (dst.is_multicast()) {
        if (bound_ifindex == 0) {
            return -EADDRNOTAVAIL;
        }
        NetDeviceRef device = device_for_ifindex(bound_ifindex);
        if (!device) {
            return -ENODEV;
        }
        IPv6Address source{};
        int const SOURCE_RESULT = netif_ipv6_select_source(device.get(), dst, requested_src, source);
        if (SOURCE_RESULT < 0) {
            return SOURCE_RESULT;
        }
        out.source = source;
        out.next_hop = dst;
        out.mtu = device->mtu;
        out.device = std::move(device);
        return 0;
    }

    IPv6RouteSnapshot route{};
    NetDeviceRef device{};
    int const LOOKUP_RESULT = route6_lookup(dst, bound_ifindex, route, device);
    if (LOOKUP_RESULT < 0) {
        return LOOKUP_RESULT;
    }
    IPv6Address source{};
    int const SOURCE_RESULT = netif_ipv6_select_source(device.get(), dst, requested_src, source);
    if (SOURCE_RESULT < 0) {
        return SOURCE_RESULT;
    }
    out.source = source;
    out.next_hop = (route.flags & IPV6_ROUTE_F_GATEWAY) != 0 ? route.gateway : dst;
    out.mtu = device->mtu;
    out.device = std::move(device);
    return 0;
}

auto ipv6_tx_routed(PacketBuffer* pkt, IPv6OutputRoute&& route, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit) -> int {
    if (pkt == nullptr || !route.device) {
        if (pkt != nullptr) {
            pkt_free(pkt);
        }
        return -ENODEV;
    }
    if (pkt->len + IPV6_HLEN > route.mtu || write_ipv6_header(pkt, route.source, dst, next_header, hop_limit) < 0) {
        pkt_free(pkt);
        return -EMSGSIZE;
    }
    if (deliver_local(pkt, dst) == 0) {
        return 0;
    }
    return emit_on_ref(pkt, std::move(route.device), route.source, route.next_hop);
}

auto ipv6_tx_on_dev(PacketBuffer* pkt, NetDevice* dev, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header,
                    uint8_t hop_limit) -> int {
    if (pkt == nullptr || dev == nullptr) {
        if (pkt != nullptr) {
            pkt_free(pkt);
        }
        return -ENODEV;
    }
    NetDeviceRef retained = netdev_retain_registered(dev);
    if (!retained) {
        pkt_free(pkt);
        return -ENODEV;
    }
    if (pkt->len + IPV6_HLEN > retained->mtu || write_ipv6_header(pkt, src, dst, next_header, hop_limit) < 0) {
        pkt_free(pkt);
        return -EMSGSIZE;
    }
    if (deliver_local(pkt, dst) == 0) {
        return 0;
    }
    return emit_on_ref(pkt, std::move(retained), src, dst);
}

void ipv6_tx(PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit, NetDevice* dev) {
    if (dev != nullptr) {
        (void)ipv6_tx_on_dev(pkt, dev, src, dst, next_header, hop_limit);
        return;
    }
    IPv6OutputRoute route{};
    if (ipv6_route_resolve(dst, &src, 0, route) < 0) {
        pkt_free(pkt);
        return;
    }
    (void)ipv6_tx_routed(pkt, std::move(route), dst, next_header, hop_limit);
}

}  // namespace ker::net::proto
