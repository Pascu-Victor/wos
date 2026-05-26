// Unit tests for WKI channel reliability structures and constants.
//
// Tests channel state defaults, credit configuration, retransmit queue
// structures, and RTT estimation constants.

#include <gtest/gtest.h>

#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

using namespace ker::net::wki;

// =============================================================================
// Channel Default State
// =============================================================================

TEST(WkiChannel, DefaultState) {
    WkiChannel ch{};
    EXPECT_EQ(ch.channel_id, 0);
    EXPECT_EQ(ch.peer_node_id, WKI_NODE_INVALID);
    EXPECT_FALSE(ch.active);
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
