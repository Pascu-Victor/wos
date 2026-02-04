#include "ipv6.hpp"

#include <cstring>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ndp.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

// Well-known IPv6 addresses
const std::array<uint8_t, 13> IPV6_SOLICITED_NODE_PREFIX = {0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF};

const std::array<uint8_t, 16> IPV6_ALL_NODES_MULTICAST = {0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

const std::array<uint8_t, 16> IPV6_UNSPECIFIED = {};

const std::array<uint8_t, 2> IPV6_LINK_LOCAL_PREFIX = {0xFE, 0x80};

void ipv6_make_link_local(std::array<uint8_t, 16>& out, const std::array<uint8_t, 6>& mac) {
    // fe80::MAC[0]^02:MAC[1]:MAC[2]:ff:fe:MAC[3]:MAC[4]:MAC[5]
    std::memset(out.data(), 0, 16);
    out[0] = 0xFE;
    out[1] = 0x80;
    // bytes 2-7 are zero (interface ID starts at byte 8)
    out[8] = mac[0] ^ 0x02;  // flip universal/local bit
    out[9] = mac[1];
    out[10] = mac[2];
    out[11] = 0xFF;
    out[12] = 0xFE;
    out[13] = mac[3];
    out[14] = mac[4];
    out[15] = mac[5];
}

void ipv6_make_solicited_node(std::array<uint8_t, 16>& out, const std::array<uint8_t, 16>& addr) {
    // ff02::1:ffXX:XXYY where XX:XXYY are last 3 bytes of addr
    std::memset(out.data(), 0, 16);
    std::memcpy(out.data(), IPV6_SOLICITED_NODE_PREFIX.data(), 13);
    out[13] = addr[13];
    out[14] = addr[14];
    out[15] = addr[15];
}

void ipv6_multicast_to_mac(std::array<uint8_t, 6>& out_mac, const std::array<uint8_t, 16>& ipv6_mcast) {
    // 33:33:XX:XX:XX:XX (last 4 bytes of IPv6 multicast)
    out_mac[0] = 0x33;
    out_mac[1] = 0x33;
    out_mac[2] = ipv6_mcast[12];
    out_mac[3] = ipv6_mcast[13];
    out_mac[4] = ipv6_mcast[14];
    out_mac[5] = ipv6_mcast[15];
}

namespace {
// Check if an IPv6 address is one of ours (unicast or multicast)
bool is_our_address(NetDevice* dev, const std::array<uint8_t, 16>& addr) {
    // Check all-nodes multicast
    if (std::memcmp(addr.data(), IPV6_ALL_NODES_MULTICAST.data(), 16) == 0) {
        return true;
    }

    // Check if it matches any of our interface addresses
    auto* nif = netif_find_by_ipv6(addr);
    if (nif != nullptr && nif->dev == dev) {
        return true;
    }

    // Check solicited-node multicast for our link-local
    if (addr[0] == 0xFF && addr[1] == 0x02) {
        // This is a multicast — check if it's our solicited-node
        auto* iface = netif_get(dev);
        if (iface != nullptr) {
            for (size_t i = 0; i < iface->ipv6_addr_count; i++) {
                std::array<uint8_t, 16> sn{};
                ipv6_make_solicited_node(sn, iface->ipv6_addrs[i].addr);
                if (std::memcmp(addr.data(), sn.data(), 16) == 0) {
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
    uint8_t version = (ntohl(hdr->version_tc_flow) >> 28) & 0x0F;
    if (version != 6) {
        pkt_free(pkt);
        return;
    }

    uint16_t payload_len = ntohs(hdr->payload_length);
    uint8_t next_header = hdr->next_header;

    // Verify packet is long enough
    if (pkt->len < IPV6_HLEN + payload_len) {
        pkt_free(pkt);
        return;
    }

    // Check if this packet is for us
    if (!is_our_address(dev, hdr->dst)) {
        pkt_free(pkt);
        return;
    }

    // Save src/dst for protocol handlers
    std::array<uint8_t, 16> src{};
    std::array<uint8_t, 16> dst{};
    std::memcpy(src.data(), hdr->src.data(), hdr->src.size());
    std::memcpy(dst.data(), hdr->dst.data(), hdr->dst.size());

    // Strip IPv6 header
    pkt->pull(IPV6_HLEN);

    switch (next_header) {
        case IPV6_PROTO_ICMPV6:
            icmpv6_rx(dev, pkt, src, dst);
            break;

        case IPV6_PROTO_TCP:
            // TODO: TCP over IPv6
            pkt_free(pkt);
            break;

        case IPV6_PROTO_UDP:
            // TODO: UDP over IPv6
            pkt_free(pkt);
            break;

        default:
            pkt_free(pkt);
            break;
    }
}

void ipv6_tx(PacketBuffer* pkt, const std::array<uint8_t, 16>& src, const std::array<uint8_t, 16>& dst, uint8_t next_header,
             uint8_t hop_limit, NetDevice* dev) {
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
    hdr->version_tc_flow = htonl(0x60000000);
    hdr->payload_length = htons(payload_len);
    hdr->next_header = next_header;
    hdr->hop_limit = hop_limit;
    std::memcpy(hdr->src.data(), src.data(), hdr->src.size());
    std::memcpy(hdr->dst.data(), dst.data(), hdr->dst.size());

    // Determine destination MAC
    std::array<uint8_t, 6> dst_mac{};
    if (dst[0] == 0xFF) {
        // Multicast: derive MAC from IPv6 address
        ipv6_multicast_to_mac(dst_mac, dst);
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
