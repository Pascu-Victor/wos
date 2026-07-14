#include "ipv6.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ndp.hpp>

#include "net/netdevice.hpp"
#include "net/packet.hpp"

namespace ker::net::proto {

// Well-known IPv6 addresses
const std::array<uint8_t, 13> IPV6_SOLICITED_NODE_PREFIX = {0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF};

const IPv6Address IPV6_ALL_NODES_MULTICAST =
    IPv6Address::from_bytes({0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01});

const IPv6Address IPV6_UNSPECIFIED = IPv6Address::unspecified();

const std::array<uint8_t, 2> IPV6_LINK_LOCAL_PREFIX = {0xFE, 0x80};

auto ipv6_make_link_local(const MacAddress& mac) -> IPv6Address {
    // fe80::MAC[0]^02:MAC[1]:MAC[2]:ff:fe:MAC[3]:MAC[4]:MAC[5]
    IPv6Address out{};
    out.bytes.at(0) = 0xFE;
    out.bytes.at(1) = 0x80;
    // bytes 2-7 are zero (interface ID starts at byte 8)
    out.bytes.at(8) = mac.at(0) ^ 0x02;  // flip universal/local bit
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
    // ff02::1:ffXX:XXYY where XX:XXYY are last 3 bytes of addr
    IPv6Address out{};
    std::ranges::copy(IPV6_SOLICITED_NODE_PREFIX, out.bytes.begin());
    out.bytes.at(13) = addr.bytes.at(13);
    out.bytes.at(14) = addr.bytes.at(14);
    out.bytes.at(15) = addr.bytes.at(15);
    return out;
}

auto ipv6_multicast_to_mac(const IPv6Address& ipv6_mcast) -> MacAddress {
    // 33:33:XX:XX:XX:XX (last 4 bytes of IPv6 multicast)
    return MacAddress::from_bytes(
        {0x33, 0x33, ipv6_mcast.bytes.at(12), ipv6_mcast.bytes.at(13), ipv6_mcast.bytes.at(14), ipv6_mcast.bytes.at(15)});
}

namespace {
// Check if an IPv6 address is one of ours (unicast or multicast)
auto is_our_address(NetDevice* dev, const IPv6Address& addr) -> bool {
    // Check all-nodes multicast
    if (addr == IPV6_ALL_NODES_MULTICAST) {
        return true;
    }

    // Check if it matches any of our interface addresses
    auto* nif = netif_find_by_ipv6(addr);
    if (nif != nullptr && nif->dev == dev) {
        return true;
    }

    // Check solicited-node multicast for our link-local
    if (addr.is_link_local_multicast()) {
        // This is a multicast - check if it's our solicited-node
        auto* iface = netif_find_by_dev(dev);
        if (iface != nullptr) {
            for (size_t i = 0; i < iface->ipv6_addr_count; i++) {
                IPv6Address const SN = ipv6_make_solicited_node(iface->ipv6_addrs.at(i).addr);
                if (addr == SN) {
                    return true;
                }
            }
        }
    }

    return false;
}
}  // namespace

void ipv6_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (pkt->len < IPV6_HLEN) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const IPv6Header*>(pkt->data);

    // Verify version == 6
    uint8_t const VERSION = (ntohl(hdr->version_tc_flow) >> 28) & 0x0F;
    if (VERSION != 6) {
        pkt_free(pkt);
        return;
    }

    uint16_t const PAYLOAD_LEN = ntohs(hdr->payload_length);
    uint8_t const NEXT_HEADER = hdr->next_header;

    // Verify packet is long enough
    if (pkt->len < IPV6_HLEN + PAYLOAD_LEN) {
        pkt_free(pkt);
        return;
    }

    // Save src/dst for protocol handlers before pulling the IPv6 header.
    IPv6Address const SRC = hdr->src;
    IPv6Address const DST = hdr->dst;

    // Check if this packet is for us
    if (!is_our_address(dev, DST)) {
        pkt_free(pkt);
        return;
    }

    // Strip IPv6 header
    pkt->pull(IPV6_HLEN);

    switch (NEXT_HEADER) {
        case IPV6_PROTO_ICMPV6:
            icmpv6_rx(dev, pkt, SRC, DST);
            break;

        case IPV6_PROTO_TCP:
        case IPV6_PROTO_UDP:
        default:
            // TODO: UDP over IPv6
            pkt_free(pkt);
            break;
    }
}

void ipv6_tx(PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit, NetDevice* dev) {
    if (dev == nullptr || pkt == nullptr) {
        if (pkt != nullptr) {
            pkt_free(pkt);
        }
        return;
    }

    auto payload_len = static_cast<uint16_t>(pkt->len);

    // Prepend IPv6 header
    auto* hdr = reinterpret_cast<IPv6Header*>(pkt->push(IPV6_HLEN));

    // Version=6, Traffic Class=0, Flow Label=0
    constexpr uint32_t VERSION_TC_FLOW = (6 << 28) | 0;
    hdr->version_tc_flow = htonl(VERSION_TC_FLOW);
    hdr->payload_length = htons(payload_len);
    hdr->next_header = next_header;
    hdr->hop_limit = hop_limit;
    hdr->src = src;
    hdr->dst = dst;

    // Packets destined to one of our own interface addresses should be
    // reinjected locally instead of depending on NIC/NDP self-delivery.
    if (auto* local_nif = netif_find_by_ipv6(dst); local_nif != nullptr && local_nif->dev != nullptr) {
        pkt->dev = local_nif->dev;
        pkt->src_mac = local_nif->dev->mac;
        ipv6_rx(local_nif->dev, pkt);
        return;  // pkt ownership transferred to ipv6_rx()
    }

    // Determine destination MAC
    MacAddress dst_mac{};
    if (dst.is_multicast()) {
        // Multicast: derive MAC from IPv6 address
        dst_mac = ipv6_multicast_to_mac(dst);
    } else {
        // Unicast: use NDP neighbor cache
        if (!ndp_resolve(dev, dst, dst_mac, pkt)) {
            // Packet queued for resolution
            return;
        }
    }

    eth_tx(dev, pkt, dst_mac, ETH_TYPE_IPV6);
}

}  // namespace ker::net::proto
