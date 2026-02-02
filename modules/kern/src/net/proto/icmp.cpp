#include "icmp.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/raw.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::proto {

void icmp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip) {
    (void)dev;

    if (pkt->len < sizeof(IcmpHeader)) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<IcmpHeader*>(pkt->data);

    // Verify checksum (over entire ICMP message including payload)
    uint16_t cksum = checksum_compute(pkt->data, pkt->len);
    if (cksum != 0) {
        pkt_free(pkt);
        return;
    }

    switch (hdr->type) {
        case ICMP_ECHO_REQUEST: {
            // Respond with echo reply
            // Reuse the packet: swap src/dst, change type to reply
#ifdef DEBUG_ICMP
            ker::mod::dbg::log("icmp_rx: got ECHO_REQUEST from %u.%u.%u.%u, sending reply\n", (src_ip >> 24) & 0xFF, (src_ip >> 16) & 0xFF,
                               (src_ip >> 8) & 0xFF, src_ip & 0xFF);
#endif
            hdr->type = ICMP_ECHO_REPLY;
            hdr->code = 0;
            hdr->checksum = 0;
            hdr->checksum = checksum_compute(pkt->data, pkt->len);

            // Send reply: dst_ip becomes our source, src_ip becomes dest
            ipv4_tx(pkt, dst_ip, src_ip, IPPROTO_ICMP, 64);
            return;  // pkt ownership transferred
        }

        case ICMP_ECHO_REPLY:
            // Deliver to raw sockets listening for ICMP
#ifdef DEBUG_ICMP
            ker::mod::dbg::log("icmp_rx: got ECHO_REPLY, delivering to raw sockets\n");
#endif
            raw_deliver(pkt, IPPROTO_ICMP);
            // Packet freed by raw_deliver
            return;

        case ICMP_DEST_UNREACHABLE:
            // Could notify relevant socket
#ifdef DEBUG_ICMP
            ker::mod::dbg::log("icmp_rx: got DEST_UNREACHABLE, dropping packet\n");
#endif
            pkt_free(pkt);
            break;

        default:
#ifdef DEBUG_ICMP
            ker::mod::dbg::log("icmp_rx: got unknown type %u, dropping packet\n", hdr->type);
#endif
            pkt_free(pkt);
            break;
    }
}

}  // namespace ker::net::proto
