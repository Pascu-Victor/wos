// Unit tests for WKI channel reliability structures and constants.
//
// Tests channel state defaults, credit configuration, retransmit queue
// structures, and RTT estimation constants.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <net/wki/channel.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

using namespace ker::net::wki;

namespace ker::net::wki {

WkiState g_wki;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto wki_peer_find(uint16_t /*node_id*/) -> WkiPeer* { return nullptr; }

auto wki_now_us() -> uint64_t { return 0; }

}  // namespace ker::net::wki

// =============================================================================
// Channel Default State
// =============================================================================

TEST(WkiChannel, DefaultState) {
    WkiChannel ch{};
    EXPECT_EQ(ch.channel_id, 0);
    EXPECT_EQ(ch.peer_node_id, WKI_NODE_INVALID);
    EXPECT_FALSE(ch.active);
    EXPECT_EQ(ch.generation, 0u);
    EXPECT_EQ(ch.tx_seq, 0u);
    EXPECT_EQ(ch.tx_ack, 0u);
    EXPECT_EQ(ch.rx_seq, 0u);
    EXPECT_FALSE(ch.ack_pending);
}

TEST(WkiChannel, RtoDefaults) {
    WkiChannel ch{};
    EXPECT_EQ(ch.rto_us, WKI_INITIAL_RTO_US);
    EXPECT_EQ(ch.srtt_us, 0u);
    EXPECT_EQ(ch.rttvar_us, 0u);
    EXPECT_EQ(ch.retransmit_head, nullptr);
    EXPECT_EQ(ch.retransmit_tail, nullptr);
    EXPECT_EQ(ch.retransmit_count, 0u);
}

TEST(WkiChannel, StatsInitZero) {
    WkiChannel ch{};
    EXPECT_EQ(ch.bytes_sent, 0u);
    EXPECT_EQ(ch.bytes_received, 0u);
    EXPECT_EQ(ch.retransmits, 0u);
}

TEST(WkiChannel, DupAckInitZero) {
    WkiChannel ch{};
    EXPECT_EQ(ch.last_dup_ack, 0u);
    EXPECT_EQ(ch.dup_ack_count, 0u);
}

TEST(WkiChannel, ResetClearsPostFenceReliabilityState) {
    WkiChannel ch{};
    ch.channel_id = WKI_CHAN_IPC_DATA;
    ch.peer_node_id = 0x1234;
    ch.priority = PriorityClass::THROUGHPUT;
    ch.active = true;
    ch.generation = 7;
    ch.tx_seq = 41;
    ch.tx_ack = 17;
    ch.rx_seq = 99;
    ch.rx_dispatch_seq = 97;
    ch.rx_ack_pending = 98;
    ch.ack_pending = true;
    ch.ack_pending_since_us = std::numeric_limits<uint64_t>::max();
    ch.tx_credits = 1;
    ch.rx_credits = 2;
    ch.rto_us = WKI_MAX_RTO_US;
    ch.srtt_us = 123;
    ch.rttvar_us = 456;
    ch.retransmit_deadline = std::numeric_limits<uint64_t>::max();
    ch.last_dup_ack = 96;
    ch.dup_ack_count = WKI_FAST_RETRANSMIT_THRESH;
    ch.bytes_sent = 111;
    ch.bytes_received = 222;
    ch.retransmits = 3;
    ch.perf_last_stall_report_us = 444;
    ch.perf_last_stall_status = 555;

    auto* rt = new WkiRetransmitEntry{};
    rt->data = new uint8_t[4]{};
    rt->len = 4;
    rt->seq = 41;
    ch.retransmit_head = rt;
    ch.retransmit_tail = rt;
    ch.retransmit_count = 1;

    auto* ro = new WkiReorderEntry{};
    ro->data = new uint8_t[2]{};
    ro->len = 2;
    ro->seq = 100;
    ch.reorder_head = ro;
    ch.reorder_count = 1;

    wki_channel_reset(&ch);

    EXPECT_TRUE(ch.active);
    EXPECT_EQ(ch.channel_id, WKI_CHAN_IPC_DATA);
    EXPECT_EQ(ch.peer_node_id, 0x1234);
    EXPECT_EQ(ch.priority, PriorityClass::THROUGHPUT);
    EXPECT_EQ(ch.generation, 7u);
    EXPECT_EQ(ch.tx_seq, 0u);
    EXPECT_EQ(ch.tx_ack, 0u);
    EXPECT_EQ(ch.rx_seq, 0u);
    EXPECT_EQ(ch.rx_dispatch_seq, 0u);
    EXPECT_EQ(ch.rx_ack_pending, 0u);
    EXPECT_FALSE(ch.ack_pending);
    EXPECT_EQ(ch.ack_pending_since_us, 0u);
    EXPECT_EQ(ch.tx_credits, WKI_CREDITS_IPC_DATA);
    EXPECT_EQ(ch.rx_credits, WKI_CREDITS_IPC_DATA);
    EXPECT_EQ(ch.retransmit_head, nullptr);
    EXPECT_EQ(ch.retransmit_tail, nullptr);
    EXPECT_EQ(ch.retransmit_count, 0u);
    EXPECT_EQ(ch.reorder_head, nullptr);
    EXPECT_EQ(ch.reorder_count, 0u);
    EXPECT_EQ(ch.last_dup_ack, 0u);
    EXPECT_EQ(ch.dup_ack_count, 0u);
    EXPECT_EQ(ch.rto_us, WKI_INITIAL_RTO_US);
    EXPECT_EQ(ch.srtt_us, 0u);
    EXPECT_EQ(ch.rttvar_us, 0u);
    EXPECT_EQ(ch.retransmit_deadline, 0u);
    EXPECT_EQ(ch.bytes_sent, 0u);
    EXPECT_EQ(ch.bytes_received, 0u);
    EXPECT_EQ(ch.retransmits, 0u);
    EXPECT_EQ(ch.perf_last_stall_report_us, 0u);
    EXPECT_EQ(ch.perf_last_stall_status, 0u);
    EXPECT_FALSE(ch.tx_rt_entry_in_use);
}

TEST(WkiChannel, ResetKeepsInlineRetransmitStorageOwnedByChannel) {
    WkiChannel ch{};
    ch.channel_id = WKI_CHAN_CONTROL;
    ch.peer_node_id = 0x5678;
    ch.active = true;
    ch.tx_rt_entry_in_use = true;
    ch.tx_rt_entry.data = ch.tx_rt_buf.data();
    ch.tx_rt_entry.len = 16;
    ch.tx_rt_entry.seq = 7;
    ch.retransmit_head = &ch.tx_rt_entry;
    ch.retransmit_tail = &ch.tx_rt_entry;
    ch.retransmit_count = 1;

    wki_channel_reset(&ch);

    EXPECT_TRUE(ch.active);
    EXPECT_EQ(ch.channel_id, WKI_CHAN_CONTROL);
    EXPECT_EQ(ch.peer_node_id, 0x5678);
    EXPECT_EQ(ch.retransmit_head, nullptr);
    EXPECT_EQ(ch.retransmit_tail, nullptr);
    EXPECT_EQ(ch.retransmit_count, 0u);
    EXPECT_FALSE(ch.tx_rt_entry_in_use);
    EXPECT_EQ(ch.tx_rt_entry.data, nullptr);
}

TEST(WkiChannel, LookupInPeerReturnsOnlyActiveMatchingSlot) {
    WkiPeer peer{};
    peer.node_id = 0x4321;
    WkiChannel ch{};
    constexpr uint16_t CHANNEL_ID = WKI_CHAN_RESOURCE;

    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, CHANNEL_ID), nullptr);

    peer.channels.at(CHANNEL_ID) = &ch;
    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, CHANNEL_ID), nullptr);

    ch.active = true;
    ch.peer_node_id = 0x9999;
    ch.channel_id = CHANNEL_ID;
    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, CHANNEL_ID), nullptr);

    ch.peer_node_id = peer.node_id;
    ch.channel_id = WKI_CHAN_EVENT_BUS;
    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, CHANNEL_ID), nullptr);

    ch.channel_id = CHANNEL_ID;
    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, CHANNEL_ID), &ch);
    EXPECT_EQ(wki_channel_lookup_in_peer(nullptr, peer.node_id, CHANNEL_ID), nullptr);
    EXPECT_EQ(wki_channel_lookup_in_peer(&peer, peer.node_id, WKI_MAX_CHANNELS), nullptr);
}

TEST(WkiChannel, AckNextMustNotAdvancePastTransmittedSeq) {
    WkiChannel ch{};
    ch.tx_seq = 1;
    ch.tx_ack = 0;

    EXPECT_TRUE(wki_channel_ack_next_within_sent_window(&ch, 0));
    EXPECT_TRUE(wki_channel_ack_next_within_sent_window(&ch, 1));
    EXPECT_FALSE(wki_channel_ack_next_within_sent_window(&ch, 2));
    EXPECT_FALSE(wki_channel_ack_next_within_sent_window(nullptr, 1));
}

TEST(WkiChannel, InlineRetransmitStorageRequiresCapacityAndIdleSlot) {
    WkiChannel ch{};

    EXPECT_TRUE(wki_channel_has_inline_retransmit_storage(&ch, WkiChannel::WKI_RT_INLINE_SIZE));
    EXPECT_FALSE(wki_channel_has_inline_retransmit_storage(&ch, WkiChannel::WKI_RT_INLINE_SIZE + 1));

    ch.tx_rt_entry_in_use = true;
    EXPECT_FALSE(wki_channel_has_inline_retransmit_storage(&ch, 1));
    EXPECT_FALSE(wki_channel_has_inline_retransmit_storage(nullptr, 1));
}

// =============================================================================
// Reliability Constants
// =============================================================================

TEST(WkiReliability, RtoRange) {
    EXPECT_LT(WKI_MIN_RTO_US, WKI_INITIAL_RTO_US);
    EXPECT_LT(WKI_INITIAL_RTO_US, WKI_MAX_RTO_US);
    EXPECT_EQ(WKI_MIN_RTO_US, 100u);
    EXPECT_EQ(WKI_MAX_RTO_US, 50000u);
}

TEST(WkiReliability, FastRetransmitThreshold) { EXPECT_EQ(WKI_FAST_RETRANSMIT_THRESH, 3); }

// =============================================================================
// Credit Configuration
// =============================================================================

TEST(WkiCredits, WellKnownChannels) {
    EXPECT_EQ(WKI_CREDITS_CONTROL, 64);
    EXPECT_EQ(WKI_CREDITS_ZONE_MGMT, 32);
    EXPECT_EQ(WKI_CREDITS_EVENT_BUS, 128);
    EXPECT_EQ(WKI_CREDITS_RESOURCE, 32);
    EXPECT_EQ(WKI_CREDITS_DYNAMIC, 256);
}

TEST(WkiCredits, ControlHasHighCredits) {
    // Control channel should have reasonable credits for protocol traffic
    EXPECT_GE(WKI_CREDITS_CONTROL, 32u);
}

// =============================================================================
// Peer State
// =============================================================================

TEST(WkiPeer, DefaultState) {
    WkiPeer peer{};
    EXPECT_EQ(peer.node_id, WKI_NODE_INVALID);
    EXPECT_EQ(peer.state, PeerState::UNKNOWN);
    EXPECT_EQ(peer.rtt_us, 0u);
    EXPECT_EQ(peer.missed_beats, 0);
    EXPECT_FALSE(peer.is_direct);
    EXPECT_EQ(peer.next_hop, WKI_NODE_INVALID);
}

TEST(WkiPeer, HeartbeatDefaults) {
    WkiPeer peer{};
    EXPECT_EQ(peer.heartbeat_interval_ms, WKI_DEFAULT_HEARTBEAT_INTERVAL_MS);
    EXPECT_EQ(peer.miss_threshold, WKI_DEFAULT_MISS_THRESHOLD);
}

TEST(WkiPeer, ChannelArraySize) {
    WkiPeer peer{};
    EXPECT_EQ(peer.channels.size(), WKI_MAX_CHANNELS);
    // All channels start null
    for (auto* ch : peer.channels) {
        EXPECT_EQ(ch, nullptr);
    }
}

// =============================================================================
// PeerState Enum
// =============================================================================

TEST(PeerState, AllStatesDistinct) {
    EXPECT_NE(static_cast<uint8_t>(PeerState::UNKNOWN), static_cast<uint8_t>(PeerState::HELLO_SENT));
    EXPECT_NE(static_cast<uint8_t>(PeerState::HELLO_SENT), static_cast<uint8_t>(PeerState::CONNECTED));
    EXPECT_NE(static_cast<uint8_t>(PeerState::CONNECTED), static_cast<uint8_t>(PeerState::FENCED));
    EXPECT_NE(static_cast<uint8_t>(PeerState::FENCED), static_cast<uint8_t>(PeerState::RECONNECTING));
}

// =============================================================================
// Error Codes
// =============================================================================

TEST(WkiErrors, AllDistinctAndNegative) {
    int codes[] = {WKI_ERR_NO_MEM,  WKI_ERR_NO_ROUTE,  WKI_ERR_PEER_FENCED, WKI_ERR_NO_CREDITS, WKI_ERR_TIMEOUT,
                   WKI_ERR_INVALID, WKI_ERR_NOT_FOUND, WKI_ERR_BUSY,        WKI_ERR_TX_FAILED};
    for (int c : codes) {
        EXPECT_LT(c, 0);
    }
    // All distinct
    for (size_t i = 0; i < 9; i++) {
        for (size_t j = i + 1; j < 9; j++) {
            EXPECT_NE(codes[i], codes[j]);
        }
    }
    EXPECT_EQ(WKI_OK, 0);
}

// =============================================================================
// Retransmit / Reorder Entry Structs
// =============================================================================

TEST(WkiRetransmit, EntryDefaults) {
    WkiRetransmitEntry e{};
    EXPECT_EQ(e.data, nullptr);
    EXPECT_EQ(e.len, 0);
    EXPECT_EQ(e.seq, 0u);
    EXPECT_EQ(e.retries, 0);
    EXPECT_EQ(e.next, nullptr);
}

TEST(WkiReorder, EntryDefaults) {
    WkiReorderEntry e{};
    EXPECT_EQ(e.data, nullptr);
    EXPECT_EQ(e.len, 0);
    EXPECT_EQ(e.next, nullptr);
}

// =============================================================================
// Heartbeat Configuration
// =============================================================================

TEST(WkiHeartbeat, IntervalRange) {
    EXPECT_LE(WKI_MIN_HEARTBEAT_INTERVAL_MS, WKI_DEFAULT_HEARTBEAT_INTERVAL_MS);
    EXPECT_LE(WKI_DEFAULT_HEARTBEAT_INTERVAL_MS, WKI_MAX_HEARTBEAT_INTERVAL_MS);
}

TEST(WkiHeartbeat, GracePeriod) {
    // Grace period should be at least as long as miss_threshold * heartbeat_interval
    EXPECT_GE(WKI_PEER_GRACE_PERIOD_MS, static_cast<uint32_t>(WKI_DEFAULT_MISS_THRESHOLD) * WKI_DEFAULT_HEARTBEAT_INTERVAL_MS);
}
