#include <cstdint>
#include <limits>
#include <net/wki/peer.hpp>
#include <net/wki/peer_liveness.hpp>
#include <test/ktest.hpp>

namespace {

using namespace ker::net::wki;

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

KTEST(WkiPeerLiveness, ObservedLivenessUsesLatestInboundSignal) {
    ker::net::wki::WkiPeer peer{};
    peer.last_heartbeat = 10;
    peer.last_rx_activity = 25;
    KEXPECT_EQ(ker::net::wki::wki_peer_observed_liveness_us(&peer), 25U);

    peer.last_rx_activity = 5;
    KEXPECT_EQ(ker::net::wki::wki_peer_observed_liveness_us(&peer), 10U);

    KEXPECT_EQ(ker::net::wki::wki_peer_observed_liveness_us(nullptr), 0U);
}

KTEST(WkiPeerLiveness, ConfirmationGraceIsAtLeastConfiguredFenceGrace) {
    ker::net::wki::WkiPeer peer{};
    KEXPECT_EQ(ker::net::wki::wki_peer_startup_grace_us(), DEFAULT_STARTUP_GRACE_US);
    KEXPECT_EQ(ker::net::wki::wki_peer_startup_grace_deadline_us(nullptr), 0U);
    KEXPECT_FALSE(ker::net::wki::wki_peer_startup_grace_pending(nullptr, 0));
    KEXPECT_EQ(ker::net::wki::wki_peer_interval_us(&peer), static_cast<uint64_t>(ker::net::wki::WKI_DEFAULT_HEARTBEAT_INTERVAL_MS) * 1000);
    KEXPECT_EQ(ker::net::wki::wki_peer_interval_us(nullptr), 0U);
    KEXPECT_EQ(ker::net::wki::wki_peer_confirm_grace_us(&peer), DEFAULT_CONFIRM_GRACE_US);

    peer.heartbeat_interval_ms = ker::net::wki::WKI_PEER_FENCE_CONFIRM_GRACE_MS + 1;
    KEXPECT_EQ(ker::net::wki::wki_peer_confirm_grace_us(&peer),
               static_cast<uint64_t>(ker::net::wki::WKI_PEER_FENCE_CONFIRM_GRACE_MS + 1) * 1000);

    KEXPECT_EQ(ker::net::wki::wki_peer_confirm_grace_us(nullptr), 0U);
}

KTEST(WkiPeerLiveness, StartupGraceTreatsFutureConnectedTimeAsPending) {
    ker::net::wki::WkiPeer peer{};
    init_connected_direct_peer(peer);
    peer.connected_time = 50'000;
    peer.last_heartbeat = 1;
    peer.last_rx_activity = 1;

    uint64_t const GRACE_DEADLINE = peer.connected_time + DEFAULT_STARTUP_GRACE_US;
    KEXPECT_EQ(ker::net::wki::wki_peer_startup_grace_deadline_us(&peer), GRACE_DEADLINE);
    KEXPECT_TRUE(ker::net::wki::wki_peer_startup_grace_pending(&peer, 10));
    KEXPECT_TRUE(ker::net::wki::wki_peer_startup_grace_pending(&peer, peer.connected_time));
    KEXPECT_TRUE(ker::net::wki::wki_peer_startup_grace_pending(&peer, GRACE_DEADLINE - 1));
    KEXPECT_FALSE(ker::net::wki::wki_peer_startup_grace_pending(&peer, GRACE_DEADLINE));
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, 10, false));
}

KTEST(WkiPeerLiveness, TimeoutHelpersComposeIntervalMissThresholdAndConfirmGrace) {
    ker::net::wki::WkiPeer peer{};
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_us(nullptr), 0U);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_with_confirm_grace_us(nullptr), 0U);
    KEXPECT_EQ(ker::net::wki::wki_peer_fence_confirmation_window_us(nullptr), 0U);
    KEXPECT_EQ(ker::net::wki::wki_peer_required_fence_elapsed_us(nullptr), 0U);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_us(&peer), DEFAULT_TIMEOUT_US);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_with_confirm_grace_us(&peer), DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US);
    KEXPECT_EQ(ker::net::wki::wki_peer_fence_confirmation_window_us(&peer), DEFAULT_FENCE_CONFIRMATION_WINDOW_US);
    KEXPECT_EQ(ker::net::wki::wki_peer_required_fence_elapsed_us(&peer), DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US);

    peer.heartbeat_interval_ms = 1'500;
    peer.miss_threshold = 3;
    KEXPECT_EQ(ker::net::wki::wki_peer_interval_us(&peer), 1'500'000U);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_us(&peer), 4'500'000U);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_with_confirm_grace_us(&peer), 9'500'000U);
    KEXPECT_EQ(ker::net::wki::wki_peer_required_fence_elapsed_us(&peer), 14'500'000U);
}

KTEST(WkiPeerLiveness, TimeoutDeadlineIncludesProbeAndDeferWindows) {
    ker::net::wki::WkiPeer peer{};
    init_connected_direct_peer(peer);

    uint64_t const FIRST_DEADLINE = peer.last_heartbeat + DEFAULT_TIMEOUT_US;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), FIRST_DEADLINE);

    peer.missed_beats = 1;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), FIRST_DEADLINE + DEFAULT_CONFIRM_GRACE_US);

    peer.missed_beats = ker::net::wki::WKI_PEER_FENCE_PROBE_ROUNDS;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat),
               FIRST_DEADLINE + DEFAULT_FENCE_CONFIRMATION_WINDOW_US);

    peer.fence_defer_until_us = FIRST_DEADLINE + DEFAULT_FENCE_CONFIRMATION_WINDOW_US + 42;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), peer.fence_defer_until_us);

    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.fence_defer_until_us + 100), peer.fence_defer_until_us + 101);
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(nullptr, peer.last_heartbeat), std::numeric_limits<uint64_t>::max());
}

KTEST(WkiPeerLiveness, TimeoutDeadlineSaturatesInsteadOfWrapping) {
    ker::net::wki::WkiPeer peer{};
    init_connected_direct_peer(peer);

    KEXPECT_EQ(ker::net::wki::wki_saturating_add_us(std::numeric_limits<uint64_t>::max() - 1, 2), std::numeric_limits<uint64_t>::max());
    KEXPECT_EQ(ker::net::wki::wki_saturating_mul_us(std::numeric_limits<uint64_t>::max() / 2 + 1, 2), std::numeric_limits<uint64_t>::max());
    KEXPECT_EQ(ker::net::wki::wki_saturating_mul_us(0, std::numeric_limits<uint64_t>::max()), 0U);
    KEXPECT_EQ(ker::net::wki::wki_saturating_mul_us(7, 6), 42U);
    KEXPECT_EQ(ker::net::wki::wki_future_deadline_us(std::numeric_limits<uint64_t>::max(), 1), std::numeric_limits<uint64_t>::max());
    KEXPECT_EQ(ker::net::wki::wki_next_or_immediate_deadline_us(10, std::numeric_limits<uint64_t>::max()),
               std::numeric_limits<uint64_t>::max());
    KEXPECT_EQ(ker::net::wki::wki_next_or_immediate_deadline_us(20, 10), 20U);

    peer.last_heartbeat = std::numeric_limits<uint64_t>::max() - 1;
    peer.last_rx_activity = 0;
    peer.missed_beats = 1;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, peer.last_heartbeat), std::numeric_limits<uint64_t>::max());

    peer.last_heartbeat = 0;
    peer.missed_beats = 0;
    KEXPECT_EQ(ker::net::wki::wki_peer_timeout_deadline_us(&peer, std::numeric_limits<uint64_t>::max()),
               std::numeric_limits<uint64_t>::max());
}

KTEST(WkiPeerLiveness, LocalObservationRequiresStartupAndProbeGrace) {
    ker::net::wki::WkiPeer peer{};
    init_connected_direct_peer(peer);

    uint64_t const BEFORE_STARTUP_GRACE = peer.connected_time + DEFAULT_STARTUP_GRACE_US - 1;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, BEFORE_STARTUP_GRACE, false));

    uint64_t const BEFORE_CONFIRM_GRACE = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US - 1;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, BEFORE_CONFIRM_GRACE, false));

    uint64_t const AT_FIRST_CONFIRM_GRACE = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, AT_FIRST_CONFIRM_GRACE, false));

    uint64_t const BEFORE_REQUIRED_WINDOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US - 1;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, BEFORE_REQUIRED_WINDOW, false));

    uint64_t const AT_REQUIRED_WINDOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_FENCE_CONFIRMATION_WINDOW_US;
    KEXPECT_TRUE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, AT_REQUIRED_WINDOW, false));
}

KTEST(WkiPeerLiveness, LocalObservationDefersDuringTxPressureOrFreshActivity) {
    ker::net::wki::WkiPeer peer{};
    init_connected_direct_peer(peer);
    uint64_t const CONFIRMED_NOW = peer.last_heartbeat + DEFAULT_TIMEOUT_US + DEFAULT_CONFIRM_GRACE_US;

    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, true));

    peer.fence_defer_until_us = CONFIRMED_NOW + 1;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, false));

    peer.fence_defer_until_us = 0;
    peer.last_rx_activity = CONFIRMED_NOW;
    KEXPECT_FALSE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, CONFIRMED_NOW, false));
}

KTEST(WkiPeerLiveness, NonLiveLocalStatesDoNotBlockExternalFenceNotification) {
    ker::net::wki::WkiPeer peer{};
    KEXPECT_TRUE(ker::net::wki::wki_peer_local_observation_confirms_fence(nullptr, 0, false));
    KEXPECT_TRUE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, 0, false));

    peer.state = ker::net::wki::PeerState::FENCED;
    KEXPECT_TRUE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, 0, false));

    peer.state = ker::net::wki::PeerState::CONNECTED;
    peer.is_direct = false;
    KEXPECT_TRUE(ker::net::wki::wki_peer_local_observation_confirms_fence(&peer, 0, false));
}

KTEST(WkiPeerRxFence, ReliableRxAcceptsOnlyConnectedPeers) {
    KEXPECT_FALSE(ker::net::wki::wki_selftest_reliable_rx_peer_state_accepts(ker::net::wki::PeerState::UNKNOWN));
    KEXPECT_FALSE(ker::net::wki::wki_selftest_reliable_rx_peer_state_accepts(ker::net::wki::PeerState::HELLO_SENT));
    KEXPECT_TRUE(ker::net::wki::wki_selftest_reliable_rx_peer_state_accepts(ker::net::wki::PeerState::CONNECTED));
    KEXPECT_FALSE(ker::net::wki::wki_selftest_reliable_rx_peer_state_accepts(ker::net::wki::PeerState::FENCED));
    KEXPECT_FALSE(ker::net::wki::wki_selftest_reliable_rx_peer_state_accepts(ker::net::wki::PeerState::RECONNECTING));
}

KTEST(WkiPeerHandshake, HelloAckReconnectsFencedPeerState) { KEXPECT_TRUE(ker::net::wki::wki_peer_selftest_hello_ack_state_transition()); }

KTEST(WkiPeerHandshake, HelloEpochWordsAreIndependent) {
    KEXPECT_TRUE(ker::net::wki::wki_peer_selftest_hello_epoch_words_are_independent());
}

KTEST(WkiPeerHandshake, RemoteBootEpochDetectsRestart) {
    KEXPECT_TRUE(ker::net::wki::wki_peer_selftest_remote_boot_epoch_detects_restart());
}
