#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth.hpp>
#include <net/wki/auth_crypto.hpp>
#include <net/wki/wire.hpp>

namespace ker::net::wki::auth_protocol {

struct DerivedSession {
    std::array<uint8_t, WKI_AUTH_SESSION_ID_SIZE> session_id{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> low_to_high{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> high_to_low{};
};

enum class ReplayResult : uint8_t {
    ACCEPTED,
    DUPLICATE,
    TOO_OLD,
    INVALID,
};

struct ReplayWindow {
    uint64_t high = 0;
    uint64_t bitmap = 0;
};

auto derive_session(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& pairwise_key, uint16_t key_id, uint16_t local_node, uint16_t remote_node,
                    uint32_t local_boot_epoch, uint32_t remote_boot_epoch, uint32_t local_channel_epoch, uint32_t remote_channel_epoch,
                    uint16_t local_capabilities, uint16_t remote_capabilities, const std::array<uint8_t, WKI_AUTH_NONCE_SIZE>& local_nonce,
                    const std::array<uint8_t, WKI_AUTH_NONCE_SIZE>& remote_nonce, DerivedSession& output) -> bool;

auto frame_tag(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& key, const WkiHeader& header, const uint8_t* payload,
               const WkiAuthTrailer& trailer, bool handshake) -> auth_crypto::Digest;

auto verify_frame_tag(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& key, const WkiHeader& header, const uint8_t* payload,
                      const WkiAuthTrailer& trailer, bool handshake) -> bool;

auto replay_admit(ReplayWindow& window, uint64_t counter) -> ReplayResult;

// Returns zero after exhaustion and leaves next_counter terminal so a session
// can never wrap back to a previously authenticated counter.
auto reserve_tx_counter(uint64_t& next_counter) -> uint64_t;

}  // namespace ker::net::wki::auth_protocol
