#include "ethernet.hpp"

#include <cstdint>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/proto/arp.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/transport_roce.hpp>

#include "net/packet.hpp"

namespace ker::net::proto {

const MacAddress ETH_BROADCAST = MacAddress::broadcast();

void eth_rx(NetDevice* dev, PacketBuffer* pkt) {
    NET_TRACE_SPAN(SPAN_ETH_RX);
    if (pkt->len < ETH_HLEN) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const EthernetHeader*>(pkt->data);

    // MAC filtering: accept only packets destined to us, broadcast, or multicast
    MacAddress const DST = hdr->dst;
    MacAddress const SRC = hdr->src;

    bool const IS_OUR_MAC = DST == dev->mac;
    bool const IS_BROADCAST = DST.is_broadcast();
    bool const IS_MULTICAST = DST.is_multicast();

    if (!IS_OUR_MAC && !IS_BROADCAST && !IS_MULTICAST) {
        pkt_free(pkt);
        return;
    }

    uint16_t const ETHERTYPE = ntohs(hdr->ethertype);
    pkt->protocol = ETHERTYPE;

    // Preserve source MAC for reply use
    pkt->src_mac = SRC;

    // Strip ethernet header
    pkt->pull(ETH_HLEN);

    switch (ETHERTYPE) {
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
        case ETH_TYPE_WKI_ROCE:
            ker::net::wki::roce_rx(dev, pkt);
            break;
        default:
            pkt_free(pkt);
            break;
    }
}

auto eth_tx(NetDevice* dev, PacketBuffer* pkt, const MacAddress& dst_mac, uint16_t ethertype) -> int {
    if (dev == nullptr || pkt == nullptr || dev->ops == nullptr || dev->ops->start_xmit == nullptr) {
        pkt_free(pkt);
        return -1;
    }

    // Prepend ethernet header
    auto* hdr = reinterpret_cast<EthernetHeader*>(pkt->push(ETH_HLEN));
    hdr->dst = dst_mac;
    hdr->src = dev->mac;
    hdr->ethertype = htons(ethertype);

    dev->tx_packets++;
    dev->tx_bytes += pkt->len;

    return dev->ops->start_xmit(dev, pkt);
}

}  // namespace ker::net::proto
