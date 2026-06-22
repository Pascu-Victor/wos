#include <array>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"tcp">;

namespace {
constexpr size_t TCP_SYN_OPTIONS_LEN = 12;
constexpr size_t TCP_ACK_SACK_OPTIONS_LEN = 12;

auto write_sack_option_locked(const TcpCB* cb, uint8_t* options, size_t capacity) -> size_t {
    if (cb == nullptr || options == nullptr || capacity < TCP_ACK_SACK_OPTIONS_LEN || !cb->sack_permitted || cb->ooo_head == nullptr) {
        return 0;
    }

    const auto* seg = cb->ooo_head;
    uint32_t const LEFT_EDGE = htonl(seg->seq);
    uint32_t const RIGHT_EDGE = htonl(seg->seq + static_cast<uint32_t>(seg->len));

    options[0] = TCP_OPTION_NOP;
    options[1] = TCP_OPTION_NOP;
    options[2] = TCP_OPTION_SACK;
    options[3] = TCP_OPTION_SACK_1_BLOCK_LEN;
    std::memcpy(options + 4, &LEFT_EDGE, sizeof(LEFT_EDGE));
    std::memcpy(options + 8, &RIGHT_EDGE, sizeof(RIGHT_EDGE));
    return TCP_ACK_SACK_OPTIONS_LEN;
}
}  // namespace

bool tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len) {
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return false;
    }

    const size_t SEQ_LEN = len + (((flags & TCP_SYN) != 0) ? 1U : 0U) + (((flags & TCP_FIN) != 0) ? 1U : 0U);
    PacketBuffer* rtx_pkt = nullptr;
    RetransmitEntry* rtx_entry = nullptr;

    if (SEQ_LEN > 0) {
        rtx_pkt = pkt_alloc_tx();
        if (rtx_pkt == nullptr) {
            log::warn("RTX clone failed (pool_free=%zu)", ker::net::pkt_pool_free_count());
        } else {
            rtx_entry = new (std::nothrow) RetransmitEntry{};
            if (rtx_entry == nullptr) {
                pkt_free(rtx_pkt);
                rtx_pkt = nullptr;
            }
        }
        if (rtx_pkt == nullptr || rtx_entry == nullptr) {
            pkt_free(pkt);
            return false;
        }
    }

    uint32_t local_ip = 0;
    uint32_t remote_ip = 0;
    bool queued_for_retransmit = false;

    cb->lock.lock();

    local_ip = cb->local_ip;
    remote_ip = cb->remote_ip;

    const uint32_t SEQ = cb->snd_nxt - (((flags & TCP_SYN) != 0) ? 1U : 0U);

    // SYN options: MSS + WSCALE.
    std::array<uint8_t, TCP_SYN_OPTIONS_LEN> options{};
    size_t opts_len = 0;
    if ((flags & TCP_SYN) != 0) {
        options.at(0) = TCP_OPTION_MSS;
        options.at(1) = TCP_OPTION_MSS_LEN;
        uint16_t const MSS = htons(cb->rcv_mss);
        std::memcpy(options.data() + 2, &MSS, sizeof(MSS));
        options.at(4) = TCP_OPTION_SACK_PERMITTED;
        options.at(5) = TCP_OPTION_SACK_PERMITTED_LEN;
        options.at(6) = TCP_OPTION_NOP;
        options.at(7) = TCP_OPTION_WSCALE;
        options.at(8) = TCP_OPTION_WSCALE_LEN;
        options.at(9) = cb->rcv_wscale;
        options.at(10) = TCP_OPTION_EOL;
        options.at(11) = TCP_OPTION_EOL;
        opts_len = TCP_SYN_OPTIONS_LEN;
    }

    size_t const HDR_LEN = sizeof(TcpHeader) + opts_len;
    size_t const TOTAL = HDR_LEN + len;

    auto* payload = pkt->put(TOTAL);
    if (len > 0 && data != nullptr) {
        std::memcpy(payload + HDR_LEN, data, len);
    }

    if (opts_len > 0) {
        std::memcpy(payload + sizeof(TcpHeader), options.data(), opts_len);
    }

    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(SEQ);
    if ((flags & TCP_ACK) != 0) {
        hdr->ack = htonl(cb->rcv_nxt);
    } else {
        hdr->ack = 0;
    }
    hdr->data_offset = static_cast<uint8_t>((HDR_LEN / 4) << 4);
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

    if (rtx_pkt != nullptr) {
        std::memcpy(rtx_pkt->storage.data(), pkt->storage.data(), PKT_BUF_SIZE);
        rtx_pkt->data = rtx_pkt->storage.data() + (pkt->data - pkt->storage.data());
        rtx_pkt->len = pkt->len;
    }

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

    if (rtx_pkt != nullptr && rtx_entry != nullptr) {
        rtx_entry->pkt = rtx_pkt;
        rtx_entry->seq = SEQ;
        rtx_entry->len = SEQ_LEN;
        rtx_entry->send_time_ms = tcp_now_ms();
        rtx_entry->retries = 0;
        rtx_entry->next = nullptr;

        // Append to retransmit queue before transmit. An immediate peer ACK can
        // then race only after snd_nxt and RTX bookkeeping are coherent.
        if (cb->retransmit_head == nullptr) {
            cb->retransmit_head = rtx_entry;
            cb->retransmit_tail = rtx_entry;
            cb->retransmit_deadline = tcp_deadline_after_ms(tcp_now_ms(), cb->rto_ms);
            tcp_timer_arm(cb);
        } else {
            cb->retransmit_tail->next = rtx_entry;
            cb->retransmit_tail = rtx_entry;
        }
        queued_for_retransmit = true;
#ifdef TCP_DEBUG
        {
            size_t depth = 0;
            for (auto* e = cb->retransmit_head; e != nullptr; e = e->next) {
                depth++;
            }
            if (depth == 64 || depth == 256 || depth == 512) {
                log::debug("RTX queue depth=%zu port=%u pool_free=%zu snd_wnd=%u", depth, cb->local_port, ker::net::pkt_pool_free_count(),
                           cb->snd_wnd);
            }
        }
#endif
    }

    cb->lock.unlock();

    if (ipv4_tx(pkt, local_ip, remote_ip, 6, 64) < 0) {
        return queued_for_retransmit;
    }

    return true;
}

void tcp_send_rst(IPv4Address src_ip, IPv4Address dst_ip, uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack,
                  uint8_t extra_flags) {
    auto* pkt = pkt_alloc_tx();
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
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return nullptr;
    }

    std::array<uint8_t, TCP_ACK_SACK_OPTIONS_LEN> options{};
    size_t const OPTS_LEN = write_sack_option_locked(cb, options.data(), options.size());
    size_t const HDR_LEN = sizeof(TcpHeader) + OPTS_LEN;
    auto* payload = pkt->put(HDR_LEN);
    if (OPTS_LEN > 0) {
        std::memcpy(payload + sizeof(TcpHeader), options.data(), OPTS_LEN);
    }

    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_nxt);
    hdr->ack = htonl(cb->rcv_nxt);
    hdr->data_offset = static_cast<uint8_t>((HDR_LEN / 4) << 4);
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
    auto* pkt = pkt_alloc_tx();
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

// Build a keepalive probe: ACK with seq = snd_una - 1
auto tcp_build_keepalive_probe(TcpCB* cb, uint32_t* out_local, uint32_t* out_remote) -> PacketBuffer* {
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return nullptr;
    }

    auto* payload = pkt->put(sizeof(TcpHeader));
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    hdr->src_port = htons(cb->local_port);
    hdr->dst_port = htons(cb->remote_port);
    hdr->seq = htonl(cb->snd_una - 1);
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

}  // namespace ker::net::proto
