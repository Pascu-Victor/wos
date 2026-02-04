#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

void tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    // Build TCP options for SYN segments (MSS option)
    uint8_t options[4] = {};
    size_t opts_len = 0;
    if ((flags & TCP_SYN) != 0) {
        options[0] = 2;  // MSS option kind
        options[1] = 4;  // MSS option length
        *reinterpret_cast<uint16_t*>(options + 2) = htons(cb->rcv_mss);
        opts_len = 4;
    }

    size_t hdr_len = sizeof(TcpHeader) + opts_len;
    size_t total = hdr_len + len;

    // Build packet: copy payload first
    auto* payload = pkt->put(total);
    if (len > 0 && data != nullptr) {
        std::memcpy(payload + hdr_len, data, len);
    }

    // Copy options
    if (opts_len > 0) {
        std::memcpy(payload + sizeof(TcpHeader), options, opts_len);
    }

    // Build TCP header
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_nxt - ((flags & TCP_SYN) ? 1 : 0));
    if ((flags & TCP_ACK) != 0) {
        hdr->ack = htonl(cb->rcv_nxt);
    } else {
        hdr->ack = 0;
    }
    hdr->data_offset = static_cast<uint8_t>((hdr_len / 4) << 4);
    hdr->flags = flags;
    hdr->window = htons(static_cast<uint16_t>(cb->rcv_wnd > 65535 ? 65535 : cb->rcv_wnd));
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;

    // Compute TCP checksum with pseudo-header
    hdr->checksum = pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len);

    // Update snd_nxt for data and SYN/FIN (which consume sequence space)
    if (len > 0) {
        cb->snd_nxt += static_cast<uint32_t>(len);
    }
    // SYN and FIN each consume one sequence number (already accounted for
    // by the caller incrementing snd_nxt after setting iss for SYN)

    // Add to retransmit queue if this carries data or SYN/FIN
    if (len > 0 || (flags & (TCP_SYN | TCP_FIN)) != 0) {
        // Clone packet for retransmit
        auto* rtx_pkt = pkt_alloc();
        if (rtx_pkt != nullptr) {
            std::memcpy(rtx_pkt->storage.data(), pkt->storage.data(), PKT_BUF_SIZE);
            rtx_pkt->data = rtx_pkt->storage.data() + (pkt->data - pkt->storage.data());
            rtx_pkt->len = pkt->len;

            auto* entry = static_cast<RetransmitEntry*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(RetransmitEntry)));
            if (entry != nullptr) {
                entry->pkt = rtx_pkt;
                entry->seq = ntohl(hdr->seq);
                entry->len = len + (((flags & TCP_SYN) != 0) ? 1 : 0) + (((flags & TCP_FIN) != 0) ? 1 : 0);
                entry->send_time_ms = tcp_now_ms();
                entry->retries = 0;
                entry->next = nullptr;

                // Append to retransmit queue
                if (cb->retransmit_head == nullptr) {
                    cb->retransmit_head = entry;
                    cb->retransmit_deadline = tcp_now_ms() + cb->rto_ms;
                } else {
                    RetransmitEntry* tail = cb->retransmit_head;
                    while (tail->next != nullptr) {
                        tail = tail->next;
                    }
                    tail->next = entry;
                }
            } else {
                pkt_free(rtx_pkt);
            }
        }
    }

    // Send via IP
    ipv4_tx(pkt, cb->local_ip, cb->remote_ip, 6, 64);  // proto=TCP(6), TTL=64
}

void tcp_send_rst(uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack, uint8_t extra_flags) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    auto* payload = pkt->put(sizeof(TcpHeader));
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(src_port);
    hdr->dst_port = htons(dst_port);
    hdr->seq = htonl(seq);
    hdr->ack = htonl(ack);
    hdr->data_offset = (sizeof(TcpHeader) / 4) << 4;
    hdr->flags = TCP_RST | extra_flags;
    hdr->window = 0;
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;

    hdr->checksum = pseudo_header_checksum(src_ip, dst_ip, 6, pkt->data, pkt->len);

    ipv4_tx(pkt, src_ip, dst_ip, 6, 64);
}

void tcp_send_ack(TcpCB* cb) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    auto* payload = pkt->put(sizeof(TcpHeader));
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_nxt);
    hdr->ack = htonl(cb->rcv_nxt);
    hdr->data_offset = (sizeof(TcpHeader) / 4) << 4;
    hdr->flags = TCP_ACK;
    hdr->window = htons(static_cast<uint16_t>(cb->rcv_wnd > 65535 ? 65535 : cb->rcv_wnd));
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;

    hdr->checksum = pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len);

    ipv4_tx(pkt, cb->local_ip, cb->remote_ip, 6, 64);
}

}  // namespace ker::net::proto
