#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <net/wki/timer_math.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

inline auto wki_peer_observed_liveness_us(const WkiPeer* peer) -> uint64_t {
    if (peer == nullptr) {
        return 0;
    }
    return std::max(peer->last_heartbeat, peer->last_rx_activity);
}

constexpr auto wki_peer_startup_grace_us() -> uint64_t {
    return wki_saturating_mul_us(static_cast<uint64_t>(WKI_PEER_GRACE_PERIOD_MS), 1000);
}

inline auto wki_peer_startup_grace_deadline_us(const WkiPeer* peer) -> uint64_t {
    if (peer == nullptr) {
        return 0;
    }
    return wki_future_deadline_us(peer->connected_time, wki_peer_startup_grace_us());
}

inline auto wki_peer_startup_grace_pending(const WkiPeer* peer, uint64_t now_us) -> bool {
    if (peer == nullptr) {
        return false;
    }
    return now_us < wki_peer_startup_grace_deadline_us(peer);
}

inline auto wki_peer_interval_us(const WkiPeer* peer) -> uint64_t {
    if (peer == nullptr) {
        return 0;
    }
    return wki_saturating_mul_us(static_cast<uint64_t>(peer->heartbeat_interval_ms), 1000);
}

inline auto wki_peer_confirm_grace_us(const WkiPeer* peer) -> uint64_t {
    if (peer == nullptr) {
        return 0;
    }
    uint64_t const INTERVAL_US = wki_peer_interval_us(peer);
    uint64_t const CONFIRM_GRACE_US = wki_saturating_mul_us(static_cast<uint64_t>(WKI_PEER_FENCE_CONFIRM_GRACE_MS), 1000);
    return std::max<uint64_t>(INTERVAL_US, CONFIRM_GRACE_US);
}

inline auto wki_peer_timeout_us(const WkiPeer* peer) -> uint64_t {
    if (peer == nullptr) {
        return 0;
    }
    return wki_saturating_mul_us(wki_peer_interval_us(peer), peer->miss_threshold);
}

inline auto wki_peer_timeout_with_confirm_grace_us(const WkiPeer* peer) -> uint64_t {
    return wki_saturating_add_us(wki_peer_timeout_us(peer), wki_peer_confirm_grace_us(peer));
}

inline auto wki_peer_fence_confirmation_window_us(const WkiPeer* peer) -> uint64_t {
    return wki_saturating_mul_us(wki_peer_confirm_grace_us(peer), WKI_PEER_FENCE_PROBE_ROUNDS);
}

inline auto wki_peer_required_fence_elapsed_us(const WkiPeer* peer) -> uint64_t {
    return wki_saturating_add_us(wki_peer_timeout_us(peer), wki_peer_fence_confirmation_window_us(peer));
}

inline auto wki_peer_timeout_deadline_us(const WkiPeer* peer, uint64_t now_us) -> uint64_t {
    if (peer == nullptr) {
        return std::numeric_limits<uint64_t>::max();
    }

    uint64_t next_deadline = wki_future_deadline_us(wki_peer_observed_liveness_us(peer), wki_peer_timeout_us(peer));
    if (peer->missed_beats != 0) {
        uint64_t const PROBE_ROUNDS = std::min<uint64_t>(peer->missed_beats, WKI_PEER_FENCE_PROBE_ROUNDS);
        next_deadline = wki_future_deadline_us(next_deadline, wki_saturating_mul_us(wki_peer_confirm_grace_us(peer), PROBE_ROUNDS));
    }
    if (peer->fence_defer_until_us > now_us) {
        next_deadline = std::max(next_deadline, peer->fence_defer_until_us);
    }
    return next_deadline > now_us ? next_deadline : wki_future_deadline_us(now_us, 1);
}

inline auto wki_peer_local_observation_confirms_fence(const WkiPeer* peer, uint64_t now_us, bool recent_tx_pressure) -> bool {
    if (peer == nullptr) {
        return true;
    }
    if (peer->state == PeerState::FENCED) {
        return true;
    }
    if (peer->state != PeerState::CONNECTED || !peer->is_direct) {
        return true;
    }
    if (peer->fence_defer_until_us > now_us) {
        return false;
    }
    if (recent_tx_pressure) {
        return false;
    }
    if (wki_peer_startup_grace_pending(peer, now_us)) {
        return false;
    }

    uint64_t const LAST_SEEN = wki_peer_observed_liveness_us(peer);
    if (LAST_SEEN >= now_us) {
        return false;
    }

    uint64_t const ELAPSED = now_us - LAST_SEEN;
    return ELAPSED >= wki_peer_required_fence_elapsed_us(peer);
}

}  // namespace ker::net::wki
