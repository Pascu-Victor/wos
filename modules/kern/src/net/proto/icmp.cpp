#include "icmp.hpp"

#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/checksum.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/raw.hpp>
#include <platform/dbg/dbg.hpp>

#include "net/netdevice.hpp"
#include "net/packet.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"icmp">;

void icmp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip, uint8_t ttl) {
    (void)dev;

    if (pkt->len < sizeof(IcmpHeader)) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<IcmpHeader*>(pkt->data);

    // Verify checksum (over entire ICMP message including payload)
    uint16_t const CKSUM = checksum_compute(pkt->data, pkt->len);
    if (CKSUM != 0) {
        pkt_free(pkt);
        return;
    }

    switch (hdr->type) {
        case ICMP_ECHO_REQUEST: {
            // Respond with echo reply
            // Reuse the packet: swap src/dst, change type to reply
#ifdef DEBUG_ICMP
            log::debug("icmp_rx: got ECHO_REQUEST from %u.%u.%u.%u, sending reply", (src_ip >> 24) & 0xFF, (src_ip >> 16) & 0xFF,
                       (src_ip >> 8) & 0xFF, src_ip & 0xFF);
#endif
            hdr->type = ICMP_ECHO_REPLY;
            hdr->code = 0;
            hdr->checksum = 0;
            hdr->checksum = checksum_compute(pkt->data, pkt->len);

            // Send reply: dst_ip becomes our source, src_ip becomes dest
            IPv4Address const REPLY_SRC_IP = dst_ip;
            IPv4Address const REPLY_DST_IP = src_ip;
            ipv4_tx(pkt, REPLY_SRC_IP, REPLY_DST_IP, IPPROTO_ICMP, 64);
            return;  // pkt ownership transferred
        }

        case ICMP_ECHO_REPLY:
            // Deliver to raw sockets listening for ICMP
#ifdef DEBUG_ICMP
            log::debug("icmp_rx: got ECHO_REPLY, delivering to raw sockets");
#endif
            raw_deliver(pkt, IPPROTO_ICMP, src_ip, dst_ip, ttl);
            // Packet freed by raw_deliver
            return;

        case ICMP_DEST_UNREACHABLE:
            // Could notify relevant socket
#ifdef DEBUG_ICMP
            log::debug("icmp_rx: got DEST_UNREACHABLE, dropping packet");
#endif
            [[fallthrough]];

        default:
#ifdef DEBUG_ICMP
            if (hdr->type != ICMP_DEST_UNREACHABLE) {
                log::debug("icmp_rx: got unknown type %u, dropping packet", hdr->type);
            }
#endif
            pkt_free(pkt);
            break;
    }
}

}  // namespace ker::net::proto
