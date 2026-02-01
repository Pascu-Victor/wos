#include "ethernet.hpp"

#include <cstring>
#include <net/endian.hpp>
#include <net/proto/arp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

const uint8_t ETH_BROADCAST[ETH_ALEN] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

void eth_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (pkt->len < ETH_HLEN) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<const EthernetHeader*>(pkt->data);
    uint16_t ethertype = ntohs(hdr->ethertype);
    pkt->protocol = ethertype;

    // Preserve source MAC for reply use
    std::memcpy(pkt->src_mac, hdr->src, 6);

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
        default:
            pkt_free(pkt);
            break;
    }
}

auto eth_tx(NetDevice* dev, PacketBuffer* pkt, const uint8_t* dst_mac, uint16_t ethertype) -> int {
    if (dev == nullptr || pkt == nullptr || dev->ops == nullptr || dev->ops->start_xmit == nullptr) {
        pkt_free(pkt);
        return -1;
    }

    // Prepend ethernet header
    auto* hdr = reinterpret_cast<EthernetHeader*>(pkt->push(ETH_HLEN));
    std::memcpy(hdr->dst, dst_mac, ETH_ALEN);
    std::memcpy(hdr->src, dev->mac, ETH_ALEN);
    hdr->ethertype = htons(ethertype);

    dev->tx_packets++;
    dev->tx_bytes += pkt->len;

    return dev->ops->start_xmit(dev, pkt);
}

}  // namespace ker::net::proto
