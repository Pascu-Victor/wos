#include "icmpv6.hpp"

#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/proto/ipv6.hpp>
#include <net/proto/ndp.hpp>

#include "net/address.hpp"
#include "net/netdevice.hpp"
#include "net/packet.hpp"

namespace ker::net::proto {

namespace {
void handle_echo_request(NetDevice* dev, PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst) {
    // Build echo reply with same payload
    size_t const PAYLOAD_LEN = pkt->len;  // includes ICMPv6 header + echo header + data

    auto* reply = pkt_alloc_tx();
    if (reply == nullptr) {
        pkt_free(pkt);
        return;
    }

    // Copy the full ICMPv6 packet (header + payload)
    auto* out = reply->put(PAYLOAD_LEN);
    std::memcpy(out, pkt->data, PAYLOAD_LEN);

    // Change type to Echo Reply
    auto* icmp = reinterpret_cast<ICMPv6Header*>(reply->data);
    icmp->type = ICMPV6_ECHO_REPLY;
    icmp->code = 0;
    icmp->checksum = 0;

    // Compute ICMPv6 checksum (mandatory, uses IPv6 pseudo-header)
    // Reply: src=our dst, dst=their src
    const auto& reply_src = dst;
    const auto& reply_dst = src;
    icmp->checksum =
        checksum_pseudo_ipv6(reply_src, reply_dst, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(reply->len), reply->data, reply->len);

    pkt_free(pkt);

    // Send reply (swap src/dst)
    ipv6_tx(reply, reply_src, reply_dst, IPV6_PROTO_ICMPV6, 64, dev);
}
}  // namespace

void icmpv6_rx(NetDevice* dev, PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst) {
    if (pkt->len < sizeof(ICMPv6Header)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const ICMPv6Header*>(pkt->data);

    // Verify checksum
    uint16_t const STORED = hdr->checksum;
    if (STORED != 0) {
        uint16_t const COMPUTED = checksum_pseudo_ipv6(src, dst, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(pkt->len), pkt->data, pkt->len);
        if (COMPUTED != 0 && COMPUTED != 0xFFFF) {
            pkt_free(pkt);
            return;
        }
    }

    switch (hdr->type) {
        case ICMPV6_ECHO_REQUEST:
            handle_echo_request(dev, pkt, src, dst);
            break;

        case ICMPV6_NEIGHBOR_SOLICIT:
            ndp_handle_ns(dev, pkt, src, dst);
            break;

        case ICMPV6_NEIGHBOR_ADVERT:
            ndp_handle_na(dev, pkt, src, dst);
            break;

        case ICMPV6_ECHO_REPLY:
        case ICMPV6_ROUTER_SOLICIT:
        case ICMPV6_ROUTER_ADVERT:
        default:
            // Not handled yet
            pkt_free(pkt);
            break;
    }
}

}  // namespace ker::net::proto
