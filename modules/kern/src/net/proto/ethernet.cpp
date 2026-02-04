#include "ethernet.hpp"

#include <cstring>
#include <net/endian.hpp>
#include <net/proto/arp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <net/wki/transport_eth.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

const std::array<uint8_t, ETH_ALEN> ETH_BROADCAST = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

void eth_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (pkt->len < ETH_HLEN) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const EthernetHeader*>(pkt->data);

    // MAC filtering: accept only packets destined to us, broadcast, or multicast
    bool is_our_mac = std::memcmp(hdr->dst.data(), dev->mac.data(), ETH_ALEN) == 0;
    bool is_broadcast = std::memcmp(hdr->dst.data(), ETH_BROADCAST.data(), ETH_ALEN) == 0;
    bool is_multicast = (hdr->dst[0] & 0x01) != 0;  // LSB of first byte set = multicast

    if (!is_our_mac && !is_broadcast && !is_multicast) {
        pkt_free(pkt);
        return;
    }

    uint16_t ethertype = ntohs(hdr->ethertype);
    pkt->protocol = ethertype;

    // Preserve source MAC for reply use
    std::memcpy(pkt->src_mac.data(), hdr->src.data(), ETH_ALEN);

    // Strip ethernet header
    pkt->pull(ETH_HLEN);

    switch (ethertype) {
        case ETH_TYPE_ARP:
            arp_rx(dev, pkt);
            break;
        case ETH_TYPE_IPV4:
            ipv4_rx(dev, pkt);
            break;
        case ETH_TYPE_IPV6:
            ipv6_rx(dev, pkt);
            break;
        case ETH_TYPE_WKI:
            ker::net::wki::wki_eth_rx(dev, pkt);
            break;
        default:
            pkt_free(pkt);
            break;
    }
}

auto eth_tx(NetDevice* dev, PacketBuffer* pkt, const std::array<uint8_t, 6>& dst_mac, uint16_t ethertype) -> int {
    if (dev == nullptr || pkt == nullptr || dev->ops == nullptr || dev->ops->start_xmit == nullptr) {
        pkt_free(pkt);
        return -1;
    }

    // Prepend ethernet header
    auto* hdr = reinterpret_cast<EthernetHeader*>(pkt->push(ETH_HLEN));
    std::memcpy(hdr->dst.data(), dst_mac.data(), ETH_ALEN);
    std::memcpy(hdr->src.data(), dev->mac.data(), ETH_ALEN);
    hdr->ethertype = htons(ethertype);

    dev->tx_packets++;
    dev->tx_bytes += pkt->len;

    return dev->ops->start_xmit(dev, pkt);
}

}  // namespace ker::net::proto
