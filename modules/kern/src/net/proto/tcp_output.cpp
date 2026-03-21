#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

bool tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len) {
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return false;
    }

    // SYN options: MSS + WSCALE.
    uint8_t options[8] = {};
    size_t opts_len = 0;
    if ((flags & TCP_SYN) != 0) {
        options[0] = 2;  // MSS kind
        options[1] = 4;  // MSS length
        *reinterpret_cast<uint16_t*>(options + 2) = htons(cb->rcv_mss);
        options[4] = 1;               // NOP
        options[5] = 3;               // WSCALE kind
        options[6] = 3;               // WSCALE length
        options[7] = cb->rcv_wscale;  // shift count
        opts_len = 8;
    }

    size_t hdr_len = sizeof(TcpHeader) + opts_len;
    size_t total = hdr_len + len;

    auto* payload = pkt->put(total);
    if (len > 0 && data != nullptr) {
        std::memcpy(payload + hdr_len, data, len);
    }

    if (opts_len > 0) {
        std::memcpy(payload + sizeof(TcpHeader), options, opts_len);
    }

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
    // SYN window is not scaled (RFC 1323).
    if ((flags & TCP_SYN) != 0 || !cb->ws_enabled) {
        hdr->window = htons(static_cast<uint16_t>(cb->rcv_wnd > 65535 ? 65535 : cb->rcv_wnd));
    } else {
        hdr->window = htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale));
    }
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;

    hdr->checksum = pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len);

    if ((flags & TCP_ACK) != 0 && len > 0) {
        cb->segs_pending_ack = 0;
        cb->delayed_ack_deadline = 0;
    }

    if (len > 0) {
        cb->snd_nxt += static_cast<uint32_t>(len);
    }
    // FIN consumes one sequence number.
    if ((flags & TCP_FIN) != 0) {
        cb->snd_nxt++;
    }

    if (len > 0 || (flags & (TCP_SYN | TCP_FIN)) != 0) {
        auto* rtx_pkt = pkt_alloc_tx();
        if (rtx_pkt == nullptr) {
            ker::mod::dbg::log("[net] RTX CLONE FAILED (pool_free=%zu) port=%u snd_nxt=%u", ker::net::pkt_pool_free_count(), cb->local_port,
                               cb->snd_nxt);
        }
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
                    cb->retransmit_tail = entry;
                    cb->retransmit_deadline = tcp_now_ms() + cb->rto_ms;
                    tcp_timer_arm(cb);
                } else {
                    cb->retransmit_tail->next = entry;
                    cb->retransmit_tail = entry;
                }
#ifdef TCP_DEBUG
                {
                    size_t depth = 0;
                    for (auto* e = cb->retransmit_head; e != nullptr; e = e->next) {
                        depth++;
                    }
                    if (depth == 64 || depth == 256 || depth == 512) {
                        ker::mod::dbg::log("[net] RTX QUEUE depth=%zu port=%u pool_free=%zu snd_wnd=%u", depth, cb->local_port,
                                           ker::net::pkt_pool_free_count(), cb->snd_wnd);
                    }
                }
#endif
            } else {
                pkt_free(rtx_pkt);
            }
        }
    }

    ipv4_tx(pkt, cb->local_ip, cb->remote_ip, 6, 64);
    return true;
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

// Build an ACK without sending; caller holds cb->lock.
auto tcp_build_ack(TcpCB* cb, uint32_t* out_local, uint32_t* out_remote) -> PacketBuffer* {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return nullptr;
    }

    auto* payload = pkt->put(sizeof(TcpHeader));
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_nxt);
    hdr->ack = htonl(cb->rcv_nxt);
    hdr->data_offset = (sizeof(TcpHeader) / 4) << 4;
    hdr->flags = TCP_ACK;
    hdr->window = cb->ws_enabled ? htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale))
                                 : htons(static_cast<uint16_t>(cb->rcv_wnd > 65535 ? 65535 : cb->rcv_wnd));
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;
    hdr->checksum = pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len);

    *out_local = cb->local_ip;
    *out_remote = cb->remote_ip;
    return pkt;
}

bool tcp_send_ack(TcpCB* cb) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return false;
    }

    auto* payload = pkt->put(sizeof(TcpHeader));
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_nxt);
    hdr->ack = htonl(cb->rcv_nxt);
    hdr->data_offset = (sizeof(TcpHeader) / 4) << 4;
    hdr->flags = TCP_ACK;
    hdr->window = cb->ws_enabled ? htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale))
                                 : htons(static_cast<uint16_t>(cb->rcv_wnd > 65535 ? 65535 : cb->rcv_wnd));
    hdr->checksum = 0;
    hdr->urgent_ptr = 0;

    hdr->checksum = pseudo_header_checksum(cb->local_ip, cb->remote_ip, 6, pkt->data, pkt->len);

    return ipv4_tx(pkt, cb->local_ip, cb->remote_ip, 6, 64) >= 0;
}

}  // namespace ker::net::proto
