#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <net/wki/peer_liveness.hpp>

using namespace ker::net::wki;

namespace {

constexpr uint64_t DEFAULT_TIMEOUT_US = static_cast<uint64_t>(WKI_DEFAULT_HEARTBEAT_INTERVAL_MS) * 1000 * WKI_DEFAULT_MISS_THRESHOLD;
constexpr uint64_t DEFAULT_CONFIRM_GRACE_US = static_cast<uint64_t>(WKI_PEER_FENCE_CONFIRM_GRACE_MS) * 1000;
constexpr uint64_t DEFAULT_STARTUP_GRACE_US = static_cast<uint64_t>(WKI_PEER_GRACE_PERIOD_MS) * 1000;
constexpr uint64_t DEFAULT_FENCE_CONFIRMATION_WINDOW_US = DEFAULT_CONFIRM_GRACE_US * WKI_PEER_FENCE_PROBE_ROUNDS;

void init_connected_direct_peer(WkiPeer& peer) {
    peer.node_id = 0x1234;
    peer.state = PeerState::CONNECTED;
    peer.is_direct = true;
    peer.connected_time = 1'000;
    peer.last_heartbeat = peer.connected_time;
    peer.last_rx_activity = peer.connected_time;
}

}  // namespace

TEST(WkiPeerLiveness, ObservedLivenessUsesLatestInboundSignal) {
    WkiPeer peer{};
    peer.last_heartbeat = 10;
    peer.last_rx_activity = 25;
    EXPECT_EQ(wki_peer_observed_liveness_us(&peer), 25u);

    peer.last_rx_activity = 5;
    EXPECT_EQ(wki_peer_observed_liveness_us(&peer), 10u);

    EXPECT_EQ(wki_peer_observed_liveness_us(nullptr), 0u);
}

TEST(WkiPeerLiveness, ConfirmationGraceIsAtLeastConfiguredFenceGrace) {
    WkiPeer peer{};
    EXPECT_EQ(wki_peer_startup_grace_us(), DEFAULT_STARTUP_GRACE_US);
    EXPECT_EQ(wki_peer_startup_grace_deadline_us(nullptr), 0u);
    EXPECT_FALSE(wki_peer_startup_grace_pending(nullptr, 0));
    EXPECT_EQ(wki_peer_interval_us(&peer), static_cast<uint64_t>(WKI_DEFAULT_HEARTBEAT_INTERVAL_MS) * 1000);
    EXPECT_EQ(wki_peer_interval_us(nullptr), 0u);
    EXPECT_EQ(wki_peer_confirm_grace_us(&peer), DEFAULT_CONFIRM_GRACE_US);

    peer.heartbeat_interval_ms = WKI_PEER_FENCE_CONFIRM_GRACE_MS + 1;
    EXPECT_EQ(wki_peer_confirm_grace_us(&peer), static_cast<uint64_t>(WKI_PEER_FENCE_CONFIRM_GRACE_MS + 1) * 1000);

    EXPECT_EQ(wki_peer_confirm_grace_us(nullptr), 0u);
}

TEST(WkiPeerLiveness, StartupGraceTreatsFutureConnectedTimeAsPending) {
    WkiPeer peer{};
    init_connected_direct_peer(peer);
    peer.connected_time = 50'000;
    peer.last_heartbeat = 1;
    peer.last_rx_activity = 1;

    uint64_t const GRACE_DEADLINE = peer.connected_time + DEFAULT_STARTUP_GRACE_US;
    EXPECT_EQ(wki_peer_startup_grace_deadline_us(&peer), GRACE_DEADLINE);
    EXPECT_TRUE(wki_peer_startup_grace_pending(&peer, 10));
    EXPECT_TRUE(wki_peer_startup_grace_pending(&peer, peer.connected_time));
    EXPECT_TRUE(wki_peer_startup_grace_pending(&peer, GRACE_DEADLINE - 1));
    EXPECT_FALSE(wki_peer_startup_grace_pending(&peer, GRACE_DEADLINE));
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, 10, false));
}

TEST(WkiPeerLiveness, TimeoutHelpersComposeIntervalMissThresholdAndConfirmGrace) {
    WkiPeer peer{};
    EXPECT_EQ(wki_peer_timeout_us(nullptr), 0u);
    EXPECT_EQ(wki_peer_timeout_with_confirm_grace_us(nullptr), 0u);
    EXPECT_EQ(wki_peer_fence_confirmation_window_us(nullptr), 0u);
    EXPECT_EQ(wki_peer_required_fence_elapsed_us(nullptr), 0u);
    EXPECT_EQ(wki_peer_timeout_us(&peer), DEFAULT_TIMEOUT_US);
    EXPECT_EQ(wki_peer_timeout_with_confirm_grace_us(&peer), DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US);
    EXPECT_EQ(wki_peer_fence_confirmation_window_us(&peer), DEFAULT_FENCE_CONFIRMATION_WINDOW_US);
    EXPECT_EQ(wki_peer_required_fence_elapsed_us(&peer), DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US);

    peer.heartbeat_interval_ms = 1'500;
    peer.miss_threshold = 3;
    EXPECT_EQ(wki_peer_interval_us(&peer), 1'500'000u);
    EXPECT_EQ(wki_peer_timeout_us(&peer), 4'500'000u);
    EXPECT_EQ(wki_peer_timeout_with_confirm_grace_us(&peer), 9'500'000u);
    EXPECT_EQ(wki_peer_required_fence_elapsed_us(&peer), 14'500'000u);
}

TEST(WkiPeerLiveness, TimeoutDeadlineIncludesProbeAndExplicitDeferWindows) {
    WkiPeer peer{};
    init_connected_direct_peer(peer);
    uint64_t const FIRST_DEADLINE = peer.last_heartbeat + DEFAULT_TIMEOUT_US;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), FIRST_DEADLINE);

    peer.missed_beats = 1;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), FIRST_DEADLINE + DEFAULT_CONFIRM_GRACE_US);

    peer.missed_beats = WKI_PEER_FENCE_PROBE_ROUNDS;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), FIRST_DEADLINE + DEFAULT_FENCE_CONFIRMATION_WINDOW_US);

    peer.fence_defer_until_us = FIRST_DEADLINE + DEFAULT_FENCE_CONFIRMATION_WINDOW_US + 42;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), peer.fence_defer_until_us);

    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.fence_defer_until_us + 100), peer.fence_defer_until_us + 101);
    EXPECT_EQ(wki_peer_timeout_deadline_us(nullptr, peer.last_heartbeat), std::numeric_limits<uint64_t>::max());
}

TEST(WkiPeerLiveness, TimeoutDeadlineSaturatesInsteadOfWrapping) {
    WkiPeer peer{};
    init_connected_direct_peer(peer);

    EXPECT_EQ(wki_saturating_add_us(std::numeric_limits<uint64_t>::max() - 1, 2), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(wki_saturating_mul_us(std::numeric_limits<uint64_t>::max() / 2 + 1, 2), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(wki_saturating_mul_us(0, std::numeric_limits<uint64_t>::max()), 0u);
    EXPECT_EQ(wki_saturating_mul_us(7, 6), 42u);
    EXPECT_EQ(wki_future_deadline_us(std::numeric_limits<uint64_t>::max(), 1), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(wki_next_or_immediate_deadline_us(10, std::numeric_limits<uint64_t>::max()), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(wki_next_or_immediate_deadline_us(20, 10), 20u);

    peer.last_heartbeat = std::numeric_limits<uint64_t>::max() - 1;
    peer.last_rx_activity = 0;
    peer.missed_beats = 1;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), std::numeric_limits<uint64_t>::max());

    peer.last_heartbeat = 0;
    peer.missed_beats = 0;
    EXPECT_EQ(wki_peer_timeout_deadline_us(&peer, std::numeric_limits<uint64_t>::max()), std::numeric_limits<uint64_t>::max());
}

TEST(WkiPeerLiveness, HeartbeatJitterArithmeticSaturatesInsteadOfWrapping) {
    constexpr uint64_t BASE_US = 1'000'000;
    constexpr uint64_t SPAN_US = BASE_US * WKI_HEARTBEAT_JITTER_PERCENT / 100;
    EXPECT_EQ(wki_heartbeat_jitter_span_us(BASE_US), SPAN_US);
    EXPECT_EQ(wki_heartbeat_jitter_range_us(BASE_US), SPAN_US * 2);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(BASE_US, 0), BASE_US - SPAN_US);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(BASE_US, SPAN_US), BASE_US);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(BASE_US, (SPAN_US * 2) - 1), BASE_US + SPAN_US - 1);

    uint64_t const HUGE_BASE = std::numeric_limits<uint64_t>::max();
    uint64_t const HUGE_SPAN = wki_heartbeat_jitter_span_us(HUGE_BASE);
    EXPECT_NE(HUGE_SPAN, 0u);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(HUGE_BASE, 0), HUGE_BASE - HUGE_SPAN);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(HUGE_BASE, HUGE_SPAN), HUGE_BASE);
    EXPECT_EQ(wki_heartbeat_interval_with_jitter_us(HUGE_BASE, std::numeric_limits<uint64_t>::max()), HUGE_BASE);
}

TEST(WkiPeerLiveness, LocalObservationRequiresDirectConnectedPeerToOutliveStartupAndProbeGrace) {
    WkiPeer peer{};
    init_connected_direct_peer(peer);

    uint64_t const BEFORE_STARTUP_GRACE = peer.connected_time + DEFAULT_STARTUP_GRACE_US - 1;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, BEFORE_STARTUP_GRACE, false));

    uint64_t const BEFORE_CONFIRM_GRACE = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US - 1;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, BEFORE_CONFIRM_GRACE, false));

    uint64_t const AT_FIRST_CONFIRM_GRACE = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, AT_FIRST_CONFIRM_GRACE, false));

    uint64_t const BEFORE_REQUIRED_WINDOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US - 1;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, BEFORE_REQUIRED_WINDOW, false));

    uint64_t const AT_REQUIRED_WINDOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US;
    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(&peer, AT_REQUIRED_WINDOW, false));
}

TEST(WkiPeerLiveness, LocalObservationDefersDuringTxPressureOrFenceDeferWindow) {
    WkiPeer peer{};
    init_connected_direct_peer(peer);
    uint64_t const CONFIRMED_NOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US;

    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, false));
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, true));

    peer.fence_defer_until_us = CONFIRMED_NOW + 1;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, false));

    peer.fence_defer_until_us = 0;
    peer.last_rx_activity = CONFIRMED_NOW;
    EXPECT_FALSE(wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, false));
}

TEST(WkiPeerLiveness, NonLiveLocalStatesDoNotBlockExternalFenceNotification) {
    WkiPeer peer{};
    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(nullptr, 0, false));
    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(&peer, 0, false));

    peer.state = PeerState::FENCED;
    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(&peer, 0, false));

    peer.state = PeerState::CONNECTED;
    peer.is_direct = false;
    EXPECT_TRUE(wki_peer_local_observation_confirms_fence(&peer, 0, false));
}
