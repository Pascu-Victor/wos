#include <algorithm>
#include <cstring>
#include <net/wki/channel.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Standalone ACK
// -----------------------------------------------------------------------------

void wki_channel_send_ack(WkiChannel* ch) {
    if ((ch == nullptr) || !ch->active) {
        return;
    }

    WkiHeader ack = {};
    ack.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT | WKI_FLAG_PRIORITY);
    ack.msg_type = 0;  // pure ACK, no message
    ack.src_node = g_wki.my_node_id;
    ack.dst_node = ch->peer_node_id;
    ack.channel_id = ch->channel_id;
    ack.seq_num = 0;
    ack.ack_num = ch->rx_ack_pending;
    ack.payload_len = 0;
    ack.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
    ack.hop_ttl = WKI_DEFAULT_TTL;
    ack.checksum = 0;
    ack.reserved = 0;

    // Find transport and send
    WkiPeer* peer = wki_peer_find(ch->peer_node_id);
    if ((peer == nullptr) || (peer->transport == nullptr)) {
        return;
    }

    uint16_t next_hop = peer->is_direct ? peer->node_id : peer->next_hop;
    if (next_hop == WKI_NODE_INVALID) {
        return;
    }

    peer->transport->tx(peer->transport, next_hop, &ack, WKI_HEADER_SIZE);
    ch->ack_pending = false;
    ch->dup_ack_count = 0;
}

// -----------------------------------------------------------------------------
// RTT estimation (Jacobson/Karels)
// -----------------------------------------------------------------------------

void wki_channel_update_rtt(WkiChannel* ch, uint32_t sample_us) {
    if (ch->srtt_us == 0) {
        // First sample
        ch->srtt_us = sample_us;
        ch->rttvar_us = sample_us / 2;
    } else {
        int32_t err = static_cast<int32_t>(sample_us) - static_cast<int32_t>(ch->srtt_us);
        ch->srtt_us = static_cast<uint32_t>(static_cast<int32_t>(ch->srtt_us) + (err / 8));
        int32_t abs_err = err < 0 ? -err : err;
        ch->rttvar_us = static_cast<uint32_t>(static_cast<int32_t>(ch->rttvar_us) + ((abs_err - static_cast<int32_t>(ch->rttvar_us)) / 4));
    }

    ch->rto_us = ch->srtt_us + (4 * ch->rttvar_us);
    ch->rto_us = std::max(ch->rto_us, WKI_MIN_RTO_US);
    ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
}

// -----------------------------------------------------------------------------
// Retransmit oldest unACKed message
// -----------------------------------------------------------------------------

auto wki_channel_retransmit(WkiChannel* ch) -> int {
    if ((ch == nullptr) || (ch->retransmit_head == nullptr)) {
        return WKI_ERR_NOT_FOUND;
    }

    WkiRetransmitEntry* rt = ch->retransmit_head;

    WkiPeer* peer = wki_peer_find(ch->peer_node_id);
    if ((peer == nullptr) || (peer->transport == nullptr)) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t next_hop = peer->is_direct ? peer->node_id : peer->next_hop;
    if (next_hop == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    int ret = peer->transport->tx(peer->transport, next_hop, rt->data, rt->len);
    if (ret < 0) {
        return WKI_ERR_TX_FAILED;
    }

    rt->retries++;
    rt->send_time_us = wki_now_us();
    ch->retransmits++;

    // Exponential backoff
    ch->rto_us *= 2;
    ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);

    ch->retransmit_deadline = rt->send_time_us + ch->rto_us;

    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Reset channel
// -----------------------------------------------------------------------------

void wki_channel_reset(WkiChannel* ch) {
    if (ch == nullptr) {
        return;
    }

    // Free retransmit queue
    WkiRetransmitEntry* rt = ch->retransmit_head;
    while (rt != nullptr) {
        WkiRetransmitEntry* next = rt->next;
        ker::mod::mm::dyn::kmalloc::free(rt->data);
        ker::mod::mm::dyn::kmalloc::free(rt);
        rt = next;
    }

    // Free reorder buffer
    WkiReorderEntry* ro = ch->reorder_head;
    while (ro != nullptr) {
        WkiReorderEntry* next = ro->next;
        ker::mod::mm::dyn::kmalloc::free(ro->data);
        ker::mod::mm::dyn::kmalloc::free(ro);
        ro = next;
    }

    // Reset state
    ch->tx_seq = 0;
    ch->tx_ack = 0;
    ch->rx_seq = 0;
    ch->rx_ack_pending = 0;
    ch->ack_pending = false;
    ch->retransmit_head = nullptr;
    ch->retransmit_tail = nullptr;
    ch->retransmit_count = 0;
    ch->reorder_head = nullptr;
    ch->reorder_count = 0;
    ch->dup_ack_count = 0;
    ch->rto_us = WKI_INITIAL_RTO_US;
    ch->srtt_us = 0;
    ch->rttvar_us = 0;
    ch->retransmit_deadline = 0;
    ch->bytes_sent = 0;
    ch->bytes_received = 0;
    ch->retransmits = 0;

    // Restore default credits
    ch->tx_credits = wki_channel_default_credits(ch->channel_id);
    ch->rx_credits = ch->tx_credits;
}

// -----------------------------------------------------------------------------
// Default credits lookup
// -----------------------------------------------------------------------------

auto wki_channel_default_credits(uint16_t channel_id) -> uint16_t {
    switch (channel_id) {
        case WKI_CHAN_CONTROL:
            return WKI_CREDITS_CONTROL;
        case WKI_CHAN_ZONE_MGMT:
            return WKI_CREDITS_ZONE_MGMT;
        case WKI_CHAN_EVENT_BUS:
            return WKI_CREDITS_EVENT_BUS;
        case WKI_CHAN_RESOURCE:
            return WKI_CREDITS_RESOURCE;
        default:
            return WKI_CREDITS_DYNAMIC;
    }
}

}  // namespace ker::net::wki
