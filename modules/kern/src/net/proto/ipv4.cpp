#include "ipv4.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
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

#include "net/netdevice.hpp"
#include "net/packet.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"ipv4">;

namespace {
uint16_t ip_id_counter = 0;
}

void ipv4_rx(NetDevice* dev, PacketBuffer* pkt) {
#ifdef DEBUG_IPV4
    log::debug("ipv4_rx: received packet len=%zu on device %s", pkt->len, dev != nullptr ? dev->name.data() : "null");
#endif

    if (pkt->len < sizeof(IPv4Header)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const IPv4Header*>(pkt->data);

    // Validate version
    uint8_t const VERSION = (hdr->ihl_version >> 4) & 0xF;
    if (VERSION != 4) {
        pkt_free(pkt);
        return;
    }

    // Validate header length
    uint8_t const IHL = hdr->ihl_version & 0xF;
    size_t const HDR_LEN = static_cast<size_t>(IHL) * 4;
    if (HDR_LEN < sizeof(IPv4Header) || HDR_LEN > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Validate total length
    uint16_t const TOTAL_LEN = ntohs(hdr->total_len);
    if (TOTAL_LEN < HDR_LEN || TOTAL_LEN > pkt->len) {
        pkt_free(pkt);
        return;
    }

    // Verify header checksum
    uint16_t const CKSUM = checksum_compute(hdr, HDR_LEN);
    if (CKSUM != 0) {
        pkt_free(pkt);
        return;
    }

    IPv4Address const DST = IPv4Address::from_network_order(hdr->dst_addr);
    IPv4Address const SRC = IPv4Address::from_network_order(hdr->src_addr);
    uint8_t const PROTO = hdr->protocol;

    // Learn sender's MAC via ARP (dynamic learning from incoming packets)
    arp_learn(SRC, pkt->src_mac);

    // Check if this packet is for us
    bool for_us = false;

    // Check all local addresses
    auto* nif = netif_find_by_ipv4(DST);
    if (nif != nullptr) {
        for_us = true;
    }

    // Check for broadcast (limited)
    if (DST.is_broadcast()) {
        for_us = true;
    }

    // Check for loopback range
    if (DST.is_loopback()) {
        for_us = true;
    }

    if (!for_us) {
        // Not for us - could forward if we implement routing later
        pkt_free(pkt);
        return;
    }

    // Strip IP header, keep only payload
    pkt->pull(HDR_LEN);

    // Trim to actual payload length
    size_t const PAYLOAD_LEN = TOTAL_LEN - HDR_LEN;
    pkt->len = PAYLOAD_LEN;

    switch (PROTO) {
        case IPPROTO_ICMP:
#ifdef DEBUG_IPV4
            log::debug("ipv4_rx: ICMP packet from %u.%u.%u.%u to %u.%u.%u.%u", (SRC >> 24) & 0xFF, (SRC >> 16) & 0xFF, (SRC >> 8) & 0xFF,
                       SRC & 0xFF, (DST >> 24) & 0xFF, (DST >> 16) & 0xFF, (DST >> 8) & 0xFF, DST & 0xFF);
#endif
            icmp_rx(dev, pkt, SRC, DST, hdr->ttl);
            break;
        case IPPROTO_UDP:
            udp_rx(dev, pkt, SRC, DST);
            break;
        case IPPROTO_TCP:
            tcp_rx(dev, pkt, SRC, DST);
            break;
        default:
            pkt_free(pkt);
            break;
    }
}

auto ipv4_tx(PacketBuffer* pkt, IPv4Address src, IPv4Address dst, uint8_t proto, uint8_t ttl) -> int {
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
    hdr->src_addr = src.to_network_order();
    hdr->dst_addr = dst.to_network_order();

    // Compute header checksum
    hdr->checksum = checksum_compute(hdr, sizeof(IPv4Header));

    // Packets destined to one of our own interface addresses should be
    // reinjected locally instead of depending on NIC/ARP self-delivery.
    if (auto* local_nif = netif_find_by_ipv4(dst); local_nif != nullptr && local_nif->dev != nullptr) {
        pkt->dev = local_nif->dev;
        pkt->src_mac = local_nif->dev->mac;
        ipv4_rx(local_nif->dev, pkt);
        return 0;  // pkt ownership transferred to ipv4_rx()
    }

    // Route the packet
    auto* route = route_lookup(dst);
    NetDevice* out_dev = nullptr;

    if (route != nullptr && route->dev != nullptr) {
        out_dev = route->dev;
    } else if (dst.is_broadcast()) {
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

    // Determine next hop: if the matched route has a gateway, use it.
    // Direct routes (on-link) have gateway=0, so next_hop stays as dst.
    IPv4Address next_hop = dst;
    if (route != nullptr && !route->gateway.is_any()) {
        next_hop = route->gateway;
    }

    // If the device is loopback, bypass ARP
    if (std::strcmp(out_dev->name.data(), "lo") == 0) {
#ifdef DEBUG_IPV4
        log::debug("ipv4_tx: loopback device, calling start_xmit");
#endif
        return out_dev->ops->start_xmit(out_dev, pkt);
    }

    // Resolve MAC address via ARP
    MacAddress dst_mac{};
    int const ARP_RESULT = arp_resolve(out_dev, next_hop, dst_mac, pkt);
    if (ARP_RESULT < 0) {
        // Packet queued in ARP pending list or dropped on timeout
        // Packet ownership transferred to ARP subsystem
        return 0;
    }

    return eth_tx(out_dev, pkt, dst_mac, ETH_TYPE_IPV4);
}

auto ipv4_tx_auto(PacketBuffer* pkt, IPv4Address dst, uint8_t proto) -> int {
#ifdef DEBUG_IPV4
    log::debug("ipv4_tx_auto: dst=%u.%u.%u.%u proto=%u", (dst >> 24) & 0xFF, (dst >> 16) & 0xFF, (dst >> 8) & 0xFF, dst & 0xFF, proto);
#endif

    // Route to determine source address
    auto* route = route_lookup(dst);
    if (route == nullptr || route->dev == nullptr) {
        // Broadcast fallback: send via first UP non-loopback device with src=0.0.0.0
        if (dst.is_broadcast()) {
            return ipv4_tx(pkt, IPv4Address::any(), dst, proto, IPV4_DEFAULT_TTL);
        }
#ifdef DEBUG_IPV4
        log::debug("ipv4_tx_auto: route lookup failed");
#endif
        pkt_free(pkt);
        return -1;
    }

#ifdef DEBUG_IPV4
    log::debug("ipv4_tx_auto: route found, dev=%s", route->dev->name.data());
#endif

    // Get the first IPv4 address on the outgoing interface
    auto* nif = netif_get(route->dev);
    if (nif == nullptr || nif->ipv4_addr_count == 0) {
        // Broadcast fallback: no address configured yet (e.g. pre-DHCP)
        if (dst.is_broadcast()) {
            return ipv4_tx(pkt, IPv4Address::any(), dst, proto, IPV4_DEFAULT_TTL);
        }
#ifdef DEBUG_IPV4
        log::debug("ipv4_tx_auto: no IPv4 address on interface");
#endif
        pkt_free(pkt);
        return -1;
    }

    return ipv4_tx(pkt, nif->ipv4_addrs.front().addr, dst, proto, IPV4_DEFAULT_TTL);
}

}  // namespace ker::net::proto
