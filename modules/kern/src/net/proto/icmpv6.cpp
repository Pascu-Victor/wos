#include "icmpv6.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv6.hpp>
#include <net/proto/ndp.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

namespace {
void handle_echo_request(NetDevice* dev, PacketBuffer* pkt,
                         const uint8_t* src, const uint8_t* dst) {
    // Build echo reply with same payload
    size_t payload_len = pkt->len;  // includes ICMPv6 header + echo header + data

    auto* reply = pkt_alloc();
    if (reply == nullptr) {
        pkt_free(pkt);
        return;
    }

    // Copy the full ICMPv6 packet (header + payload)
    auto* out = reply->put(payload_len);
    std::memcpy(out, pkt->data, payload_len);

    // Change type to Echo Reply
    auto* icmp = reinterpret_cast<ICMPv6Header*>(reply->data);
    icmp->type = ICMPV6_ECHO_REPLY;
    icmp->code = 0;
    icmp->checksum = 0;

    // Compute ICMPv6 checksum (mandatory, uses IPv6 pseudo-header)
    // Reply: src=our dst, dst=their src
    icmp->checksum = checksum_pseudo_ipv6(dst, src, IPV6_PROTO_ICMPV6,
                                          static_cast<uint32_t>(reply->len),
                                          reply->data, reply->len);

    pkt_free(pkt);

    // Send reply (swap src/dst)
    ipv6_tx(reply, dst, src, IPV6_PROTO_ICMPV6, 64, dev);
}
}  // namespace

void icmpv6_rx(NetDevice* dev, PacketBuffer* pkt, const uint8_t* src, const uint8_t* dst) {
    if (pkt->len < sizeof(ICMPv6Header)) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<const ICMPv6Header*>(pkt->data);

    // Verify checksum
    uint16_t stored = hdr->checksum;
    if (stored != 0) {
        uint16_t computed = checksum_pseudo_ipv6(src, dst, IPV6_PROTO_ICMPV6,
                                                 static_cast<uint32_t>(pkt->len),
                                                 pkt->data, pkt->len);
        if (computed != 0 && computed != 0xFFFF) {
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
            // Not handled yet
            pkt_free(pkt);
            break;

        default:
            pkt_free(pkt);
            break;
    }
}

}  // namespace ker::net::proto
