#include <algorithm>
#include <cstdint>
#include <cstring>
#include <net/wki/channel.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

namespace {

void reset_retransmit_entry_storage(WkiChannel* ch, WkiRetransmitEntry* rt) {
    if (rt == &ch->tx_rt_entry) {
        ch->tx_rt_entry_in_use = false;
        ch->tx_rt_entry = {};
        return;
    }

    delete[] rt->data;
    delete rt;
}

}  // namespace

// -----------------------------------------------------------------------------
// Standalone ACK
// -----------------------------------------------------------------------------

void wki_channel_send_ack(WkiChannel* ch) {
    if ((ch == nullptr) || !ch->active) {
        return;
    }

    WkiHeader ack = {};
    ack.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT | WKI_FLAG_PRIORITY);
    ack.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);  // pure ACK carrier
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
    WkiPeer const* peer = wki_peer_find(ch->peer_node_id);
    if ((peer == nullptr) || (peer->transport == nullptr)) {
        return;
    }

    uint16_t const NEXT_HOP = peer->is_direct ? peer->node_id : peer->next_hop;
    if (NEXT_HOP == WKI_NODE_INVALID) {
        return;
    }

    peer->transport->tx(peer->transport, NEXT_HOP, &ack, WKI_HEADER_SIZE);
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
        int32_t const ERR = static_cast<int32_t>(sample_us) - static_cast<int32_t>(ch->srtt_us);
        ch->srtt_us = static_cast<uint32_t>(static_cast<int32_t>(ch->srtt_us) + (ERR / 8));
        int32_t const ABS_ERR = ERR < 0 ? -ERR : ERR;
        ch->rttvar_us = static_cast<uint32_t>(static_cast<int32_t>(ch->rttvar_us) + ((ABS_ERR - static_cast<int32_t>(ch->rttvar_us)) / 4));
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

    WkiPeer const* peer = wki_peer_find(ch->peer_node_id);
    if ((peer == nullptr) || (peer->transport == nullptr)) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t const NEXT_HOP = peer->is_direct ? peer->node_id : peer->next_hop;
    if (NEXT_HOP == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    int const RET = peer->transport->tx(peer->transport, NEXT_HOP, rt->data, rt->len);
    if (RET < 0) {
        return WKI_ERR_TX_FAILED;
    }

    rt->retries++;
    rt->send_time_us = wki_now_us();
    ch->retransmits++;

    // Exponential backoff
    ch->rto_us *= 2;
    ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);

    ch->retransmit_deadline = wki_future_deadline_us(rt->send_time_us, ch->rto_us);

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
        reset_retransmit_entry_storage(ch, rt);
        rt = next;
    }

    // Free reorder buffer
    WkiReorderEntry const* ro = ch->reorder_head;
    while (ro != nullptr) {
        WkiReorderEntry const* next = ro->next;
        delete[] ro->data;
        delete ro;
        ro = next;
    }

    // Reset state
    ch->tx_seq = 0;
    ch->tx_ack = 0;
    ch->rx_seq = 0;
    ch->rx_baseline_initialized = false;
    ch->rx_dispatch_seq = 0;
    ch->rx_dispatch_waiters.fill(nullptr);
    ch->rx_ack_pending = WKI_ACK_NONE;
    ch->ack_pending = false;
    ch->ack_pending_since_us = 0;
    ch->retransmit_head = nullptr;
    ch->retransmit_tail = nullptr;
    ch->retransmit_count = 0;
    ch->reorder_head = nullptr;
    ch->reorder_count = 0;
    ch->last_dup_ack = 0;
    ch->dup_ack_count = 0;
    ch->rto_us = WKI_INITIAL_RTO_US;
    ch->srtt_us = 0;
    ch->rttvar_us = 0;
    ch->retransmit_deadline = 0;
    ch->bytes_sent = 0;
    ch->bytes_received = 0;
    ch->retransmits = 0;
    ch->perf_last_stall_report_us = 0;
    ch->perf_last_stall_status = 0;
    ch->tx_rt_entry = {};
    ch->tx_rt_entry_in_use = false;

    // Restore default credits
    ch->tx_credits = wki_channel_default_credits(ch->channel_id);
    ch->rx_credits = ch->tx_credits;
}

auto wki_channel_lookup_in_peer(WkiPeer* peer, uint16_t peer_node, uint16_t channel_id) -> WkiChannel* {
    if (peer == nullptr || channel_id >= WKI_MAX_CHANNELS) {
        return nullptr;
    }

    WkiChannel* ch = peer->channels.at(channel_id);
    if (ch == nullptr || !ch->active || ch->peer_node_id != peer_node || ch->channel_id != channel_id) {
        return nullptr;
    }

    return ch;
}

auto wki_channel_ack_next_within_sent_window(const WkiChannel* ch, uint32_t ack_next) -> bool {
    if (ch == nullptr) {
        return false;
    }
    return !seq_after(ack_next, ch->tx_seq);
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
        case WKI_CHAN_IPC_DATA:
            return WKI_CREDITS_IPC_DATA;
        default:
            return WKI_CREDITS_DYNAMIC;
    }
}

}  // namespace ker::net::wki
