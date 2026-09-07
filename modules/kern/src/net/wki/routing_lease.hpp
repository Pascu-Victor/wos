#pragma once

#include <array>
#include <cstdint>
#include <net/wki/routing_model.hpp>

namespace ker::net::wki::routing_lease {

struct Identity {
    uint32_t boot_epoch = 0;
    std::array<uint8_t, 32> nonce{};

    auto operator==(Identity const&) const -> bool = default;
};

struct State {
    Identity identity{};
    uint32_t session_generation = 0;
    bool bound = false;
    bool extended_required = false;

    // Never reset this counter on expiry, fence, reboot, or session rotation.
    // Exhaustion is terminal, so no delayed response can match a reused token.
    uint64_t token_counter = 0;
    uint64_t pending_token = 0;
    uint64_t request_started_us = 0;
    uint64_t reply_deadline_us = 0;
    uint64_t pending_lease_expires_us = 0;

    uint32_t sequence = 0;
    bool sequence_known = false;
    bool withdrawn = false;
    uint64_t validated_us = 0;
    uint64_t lease_expires_us = 0;
};

enum class Result : uint8_t { ACCEPTED, NO_PENDING, IDENTITY_MISMATCH, TOKEN_MISMATCH, EXPIRED, STALE, MALFORMED };

inline auto identity_valid(Identity const& identity) -> bool {
    if (identity.boot_epoch == 0) {
        return false;
    }
    for (auto byte : identity.nonce) {
        if (byte != 0) {
            return true;
        }
    }
    return false;
}

inline auto legacy_allowed(State const& state) -> bool { return !state.extended_required; }

inline void fence(State& state) {
    state.bound = false;
    state.pending_token = 0;
    state.lease_expires_us = 0;
}

// Only the completed authenticated origin handshake may call this function.
// A relay cannot authorize its own origin identity or capability downgrade.
// The identity is boot-wide; the local session generation is pair-specific.
inline auto bind_origin(State& state, Identity const& identity, uint32_t session_generation, bool capable) -> bool {
    if (!capable) {
        return legacy_allowed(state);
    }
    if (!identity_valid(identity) || session_generation == 0) {
        return false;
    }
    if (!state.bound || state.identity != identity || state.session_generation != session_generation) {
        fence(state);
        if (state.identity != identity) {
            state.sequence_known = false;
            state.withdrawn = false;
        }
        state.identity = identity;
        state.session_generation = session_generation;
        state.bound = true;
    }
    state.extended_required = true;
    return true;
}

inline auto pending(State const& state, uint64_t now_us) -> bool {
    return state.bound && state.pending_token != 0 && now_us >= state.request_started_us && now_us < state.reply_deadline_us &&
           now_us < state.pending_lease_expires_us;
}

// Returns the existing token while its challenge is pending (coalescing).
// Deadlines must fit exactly; overflowing lifetimes fail closed. The caller
// records whether the token changed to avoid retransmitting a coalesced query.
inline auto begin_request(State& state, uint64_t now_us, uint64_t reply_timeout_us, uint64_t lease_duration_us) -> uint64_t {
    if (!state.bound || reply_timeout_us == 0 || lease_duration_us == 0) {
        return 0;
    }
    if (pending(state, now_us)) {
        return state.pending_token;
    }
    if ((state.pending_token != 0 && now_us < state.request_started_us) || (state.lease_expires_us != 0 && now_us < state.validated_us)) {
        fence(state);
        return 0;
    }
    state.pending_token = 0;
    if (state.token_counter == UINT64_MAX || reply_timeout_us > UINT64_MAX - now_us || lease_duration_us > UINT64_MAX - now_us) {
        return 0;
    }
    state.pending_token = ++state.token_counter;
    state.request_started_us = now_us;
    state.reply_deadline_us = now_us + reply_timeout_us;
    state.pending_lease_expires_us = now_us + lease_duration_us;
    return state.pending_token;
}

// The caller verifies frame authentication, negotiated framing, exact payload
// length, canonical neighbors, and same-sequence topology equality BEFORE
// admission. An exact comparison is needed: a small noncryptographic hash is
// not sufficient. same_sequence_payload_matches explicitly carries that result.
inline auto accept_response(State& state, Identity const& identity, uint32_t session_generation, uint64_t token, uint32_t sequence,
                            bool withdrawn, uint16_t neighbor_count, bool same_sequence_payload_matches, uint64_t now_us) -> Result {
    if (!state.bound || state.identity != identity || state.session_generation != session_generation) {
        return Result::IDENTITY_MISMATCH;
    }
    if (state.pending_token == 0) {
        return Result::NO_PENDING;
    }
    if (token == 0 || state.pending_token != token) {
        return Result::TOKEN_MISMATCH;
    }
    if (!pending(state, now_us)) {
        state.pending_token = 0;
        return Result::EXPIRED;
    }
    if (neighbor_count > 32 || (withdrawn && neighbor_count != 0)) {
        return Result::MALFORMED;
    }
    if (state.sequence_known) {
        if (sequence == state.sequence) {
            if (withdrawn != state.withdrawn || !same_sequence_payload_matches) {
                return Result::MALFORMED;
            }
        } else if (!routing_model::sequence_newer(sequence, state.sequence)) {
            return Result::STALE;
        }
    }
    state.pending_token = 0;
    state.sequence = sequence;
    state.sequence_known = true;
    state.withdrawn = withdrawn;
    state.validated_us = now_us;
    state.lease_expires_us = state.pending_lease_expires_us;
    return Result::ACCEPTED;
}

inline auto usable(State const& state, uint64_t now_us) -> bool {
    return state.bound && state.extended_required && state.sequence_known && !state.withdrawn && now_us >= state.validated_us &&
           now_us < state.lease_expires_us;
}

}  // namespace ker::net::wki::routing_lease
