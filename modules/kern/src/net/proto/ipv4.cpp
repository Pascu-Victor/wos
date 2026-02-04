#include "ipv4.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/arp.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmp.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <net/route.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

namespace {
uint16_t ip_id_counter = 0;
}

void ipv4_rx(NetDevice* dev, PacketBuffer* pkt) {
#ifdef DEBUG_IPV4
    ker::mod::dbg::log("ipv4_rx: received packet len=%zu on device %s\n", pkt->len, dev ? dev->name : "null");
#endif

    if (pkt->len < sizeof(IPv4Header)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const IPv4Header*>(pkt->data);

    // Validate version
    uint8_t version = (hdr->ihl_version >> 4) & 0xF;
    if (version != 4) {
        pkt_free(pkt);
        return;
    }

    // Validate header length
    uint8_t ihl = hdr->ihl_version & 0xF;
    size_t hdr_len = static_cast<size_t>(ihl) * 4;
    if (hdr_len < sizeof(IPv4Header) || hdr_len > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Validate total length
    uint16_t total_len = ntohs(hdr->total_len);
    if (total_len < hdr_len || total_len > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Verify header checksum
    uint16_t cksum = checksum_compute(hdr, hdr_len);
    if (cksum != 0) {
        pkt_free(pkt);
        return;
    }

    uint32_t dst = ntohl(hdr->dst_addr);
    uint32_t src = ntohl(hdr->src_addr);
    uint8_t proto = hdr->protocol;

    // Learn sender's MAC via ARP (dynamic learning from incoming packets)
    arp_learn(src, pkt->src_mac);

    // Check if this packet is for us
    bool for_us = false;

    // Check all local addresses
    auto* nif = netif_find_by_ipv4(dst);
    if (nif != nullptr) {
        for_us = true;
    }

    // Check for broadcast (limited)
    if (dst == 0xFFFFFFFF) {
        for_us = true;
    }

    // Check for loopback range
    if ((dst >> 24) == 127) {
        for_us = true;
    }

    if (!for_us) {
        // Not for us - could forward if we implement routing later
        pkt_free(pkt);
        return;
    }

    // Strip IP header, keep only payload
    pkt->pull(hdr_len);

    // Trim to actual payload length
    size_t payload_len = total_len - hdr_len;
    pkt->len = payload_len;

    switch (proto) {
        case IPPROTO_ICMP:
#ifdef DEBUG_IPV4
            ker::mod::dbg::log("ipv4_rx: ICMP packet from %u.%u.%u.%u to %u.%u.%u.%u\n", (src >> 24) & 0xFF, (src >> 16) & 0xFF,
                               (src >> 8) & 0xFF, src & 0xFF, (dst >> 24) & 0xFF, (dst >> 16) & 0xFF, (dst >> 8) & 0xFF, dst & 0xFF);
#endif
            icmp_rx(dev, pkt, src, dst);
            break;
        case IPPROTO_UDP:
            udp_rx(dev, pkt, src, dst);
            break;
        case IPPROTO_TCP:
            tcp_rx(dev, pkt, src, dst);
            break;
        default:
            pkt_free(pkt);
            break;
    }
}

auto ipv4_tx(PacketBuffer* pkt, uint32_t src, uint32_t dst, uint8_t proto, uint8_t ttl) -> int {
    // Prepend IPv4 header
    auto* hdr = reinterpret_cast<IPv4Header*>(pkt->push(sizeof(IPv4Header)));
    std::memset(hdr, 0, sizeof(IPv4Header));

    hdr->ihl_version = (4 << 4) | 5;  // IPv4, 5 dwords (no options)
    hdr->tos = 0;
    hdr->total_len = htons(static_cast<uint16_t>(pkt->len));
    hdr->id = htons(ip_id_counter++);
    hdr->flags_fragoff = htons(0x4000);  // Don't Fragment flag
    hdr->ttl = ttl;
    hdr->protocol = proto;
    hdr->checksum = 0;
    hdr->src_addr = htonl(src);
    hdr->dst_addr = htonl(dst);

    // Compute header checksum
    hdr->checksum = checksum_compute(hdr, sizeof(IPv4Header));

    // Route the packet
    auto* route = route_lookup(dst);
    NetDevice* out_dev = nullptr;

    if (route != nullptr && route->dev != nullptr) {
        out_dev = route->dev;
    } else if (dst == 0xFFFFFFFF) {
        // Broadcast fallback: no route needed, use first UP non-loopback device
        for (size_t i = 0; i < netdev_count(); i++) {
            auto* d = netdev_at(i);
            if (d != nullptr && d->state == 1 && std::strcmp(d->name.data(), "lo") != 0) {
                out_dev = d;
                break;
            }
        }
    }

    if (out_dev == nullptr) {
        pkt_free(pkt);
        return -1;
    }

    // Determine next hop (guard against null route)
    uint32_t next_hop = dst;
    if (route != nullptr && route->gateway != 0) {
        // Check if destination is on the same subnet as the route
        // If so, send directly to the destination (don't use gateway)
        uint32_t dst_net = dst & route->netmask;
        uint32_t route_net = route->dest & route->netmask;
        if (dst_net != route_net) {
            // Different subnet - use gateway as next hop
            next_hop = route->gateway;
        }
        // else: same subnet - keep next_hop = dst
    }

    // If the device is loopback, bypass ARP
    if (std::strcmp(out_dev->name.data(), "lo") == 0) {
#ifdef DEBUG_IPV4
        ker::mod::dbg::log("ipv4_tx: loopback device, calling start_xmit\n");
#endif
        return out_dev->ops->start_xmit(out_dev, pkt);
    }

    // Resolve MAC address via ARP
    std::array<uint8_t, 6> dst_mac{};
    int arp_result = arp_resolve(out_dev, next_hop, dst_mac, pkt);
    if (arp_result < 0) {
        // Packet queued in ARP pending list or dropped on timeout
        // Packet ownership transferred to ARP subsystem
        return 0;
    }

    return eth_tx(out_dev, pkt, dst_mac, ETH_TYPE_IPV4);
}

auto ipv4_tx_auto(PacketBuffer* pkt, uint32_t dst, uint8_t proto) -> int {
#ifdef DEBUG_IPV4
    ker::mod::dbg::log("ipv4_tx_auto: dst=%u.%u.%u.%u proto=%u\n", (dst >> 24) & 0xFF, (dst >> 16) & 0xFF, (dst >> 8) & 0xFF, dst & 0xFF,
                       proto);
#endif

    // Route to determine source address
    auto* route = route_lookup(dst);
    if (route == nullptr || route->dev == nullptr) {
        // Broadcast fallback: send via first UP non-loopback device with src=0.0.0.0
        if (dst == 0xFFFFFFFF) {
            return ipv4_tx(pkt, 0, dst, proto, 64);
        }
#ifdef DEBUG_IPV4
        ker::mod::dbg::log("ipv4_tx_auto: route lookup failed\n");
#endif
        pkt_free(pkt);
        return -1;
    }

#ifdef DEBUG_IPV4
    ker::mod::dbg::log("ipv4_tx_auto: route found, dev=%s\n", route->dev->name);
#endif

    // Get the first IPv4 address on the outgoing interface
    auto* nif = netif_get(route->dev);
    if (nif == nullptr || nif->ipv4_addr_count == 0) {
        // Broadcast fallback: no address configured yet (e.g. pre-DHCP)
        if (dst == 0xFFFFFFFF) {
            return ipv4_tx(pkt, 0, dst, proto, 64);
        }
#ifdef DEBUG_IPV4
        ker::mod::dbg::log("ipv4_tx_auto: no IPv4 address on interface\n");
#endif
        pkt_free(pkt);
        return -1;
    }

    return ipv4_tx(pkt, nif->ipv4_addrs[0].addr, dst, proto, 64);
}

}  // namespace ker::net::proto
