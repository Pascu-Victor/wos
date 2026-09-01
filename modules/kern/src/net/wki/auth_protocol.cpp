#include "auth_protocol.hpp"

#include <algorithm>
#include <array>
#include <cstdint>

#include "net/wki/auth.hpp"
#include "net/wki/auth_crypto.hpp"
#include "net/wki/wire.hpp"

namespace ker::net::wki::auth_protocol {

namespace {

constexpr char FRAME_DOMAIN[] = "WOS-WKI-V3-FRAME";
constexpr char HANDSHAKE_DOMAIN[] = "WOS-WKI-V3-HANDSHAKE";
constexpr char LOW_TO_HIGH_INFO[] = "WOS-WKI-V3-LOW-TO-HIGH";
constexpr char HIGH_TO_LOW_INFO[] = "WOS-WKI-V3-HIGH-TO-LOW";
constexpr char SESSION_ID_INFO[] = "WOS-WKI-V3-SESSION-ID";

struct SessionSalt {
    std::array<uint8_t, 16> domain = {'W', 'O', 'S', '-', 'W', 'K', 'I', '-', 'V', '3', '-', 'S', 'A', 'L', 'T', 0};
    uint16_t low_node = 0;
    uint16_t high_node = 0;
    uint32_t low_boot_epoch = 0;
    uint32_t high_boot_epoch = 0;
    uint32_t low_channel_epoch = 0;
    uint32_t high_channel_epoch = 0;
    uint16_t low_capabilities = 0;
    uint16_t high_capabilities = 0;
    uint16_t key_id = 0;
    uint16_t version = WKI_VERSION;
    uint16_t auth_suite = WKI_AUTH_SUITE_HMAC_SHA256_HKDF_SHA256;
    uint16_t reserved = 0;
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> low_nonce{};
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> high_nonce{};
} __attribute__((packed));

}  // namespace

auto derive_session(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& pairwise_key, uint16_t key_id, uint16_t local_node, uint16_t remote_node,
                    uint32_t local_boot_epoch, uint32_t remote_boot_epoch, uint32_t local_channel_epoch, uint32_t remote_channel_epoch,
                    uint16_t local_capabilities, uint16_t remote_capabilities, const std::array<uint8_t, WKI_AUTH_NONCE_SIZE>& local_nonce,
                    const std::array<uint8_t, WKI_AUTH_NONCE_SIZE>& remote_nonce, DerivedSession& output) -> bool {
    bool const LOCAL_NONCE_ZERO = std::all_of(local_nonce.begin(), local_nonce.end(), [](uint8_t byte) { return byte == 0; });
    bool const REMOTE_NONCE_ZERO = std::all_of(remote_nonce.begin(), remote_nonce.end(), [](uint8_t byte) { return byte == 0; });
    if (key_id == 0 || local_node == WKI_NODE_INVALID || remote_node == WKI_NODE_INVALID || local_node == remote_node ||
        local_boot_epoch == 0 || remote_boot_epoch == 0 || local_channel_epoch == 0 || remote_channel_epoch == 0 || LOCAL_NONCE_ZERO ||
        REMOTE_NONCE_ZERO) {
        return false;
    }

    uint16_t const LOW_NODE = std::min(local_node, remote_node);
    uint16_t const HIGH_NODE = std::max(local_node, remote_node);
    SessionSalt salt{};
    salt.low_node = LOW_NODE;
    salt.high_node = HIGH_NODE;
    salt.low_boot_epoch = local_node == LOW_NODE ? local_boot_epoch : remote_boot_epoch;
    salt.high_boot_epoch = local_node == HIGH_NODE ? local_boot_epoch : remote_boot_epoch;
    salt.low_channel_epoch = local_node == LOW_NODE ? local_channel_epoch : remote_channel_epoch;
    salt.high_channel_epoch = local_node == HIGH_NODE ? local_channel_epoch : remote_channel_epoch;
    salt.low_capabilities = local_node == LOW_NODE ? local_capabilities : remote_capabilities;
    salt.high_capabilities = local_node == HIGH_NODE ? local_capabilities : remote_capabilities;
    salt.key_id = key_id;
    salt.low_nonce = local_node == LOW_NODE ? local_nonce : remote_nonce;
    salt.high_nonce = local_node == HIGH_NODE ? local_nonce : remote_nonce;

    auth_crypto::Digest prk = auth_crypto::hkdf_extract(&salt, sizeof(salt), pairwise_key.data(), pairwise_key.size());
    DerivedSession candidate{};
    bool const DERIVED = auth_crypto::hkdf_expand(prk, LOW_TO_HIGH_INFO, sizeof(LOW_TO_HIGH_INFO) - 1, candidate.low_to_high.data(),
                                                  candidate.low_to_high.size()) &&
                         auth_crypto::hkdf_expand(prk, HIGH_TO_LOW_INFO, sizeof(HIGH_TO_LOW_INFO) - 1, candidate.high_to_low.data(),
                                                  candidate.high_to_low.size()) &&
                         auth_crypto::hkdf_expand(prk, SESSION_ID_INFO, sizeof(SESSION_ID_INFO) - 1, candidate.session_id.data(),
                                                  candidate.session_id.size());
    auth_crypto::secure_erase(prk.data(), prk.size());
    if (!DERIVED) {
        auth_crypto::secure_erase(&candidate, sizeof(candidate));
        return false;
    }
    output = candidate;
    auth_crypto::secure_erase(&candidate, sizeof(candidate));
    return true;
}

auto frame_tag(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& key, const WkiHeader& header, const uint8_t* payload,
               const WkiAuthTrailer& trailer, bool handshake) -> auth_crypto::Digest {
    WkiHeader normalized = header;
    normalized.hop_ttl = 0;
    normalized.checksum = 0;

    auth_crypto::HmacSha256 hmac(key.data(), key.size());
    hmac.update(handshake ? HANDSHAKE_DOMAIN : FRAME_DOMAIN, handshake ? sizeof(HANDSHAKE_DOMAIN) - 1 : sizeof(FRAME_DOMAIN) - 1);
    hmac.update(&normalized, sizeof(normalized));
    hmac.update(payload, header.payload_len);
    hmac.update(trailer.session_id.data(), trailer.session_id.size());
    hmac.update(&trailer.counter, sizeof(trailer.counter));
    return hmac.finish();
}

auto verify_frame_tag(const std::array<uint8_t, WKI_AUTH_KEY_SIZE>& key, const WkiHeader& header, const uint8_t* payload,
                      const WkiAuthTrailer& trailer, bool handshake) -> bool {
    auth_crypto::Digest expected = frame_tag(key, header, payload, trailer, handshake);
    bool const VALID = auth_crypto::constant_time_equal(expected.data(), trailer.tag.data(), trailer.tag.size());
    auth_crypto::secure_erase(expected.data(), expected.size());
    return VALID;
}

auto replay_admit(ReplayWindow& window, uint64_t counter) -> ReplayResult {
    if (counter == 0) {
        return ReplayResult::INVALID;
    }
    if (counter > window.high) {
        uint64_t const SHIFT = counter - window.high;
        window.bitmap = SHIFT >= 64 ? 1 : (window.bitmap << SHIFT) | 1U;
        window.high = counter;
        return ReplayResult::ACCEPTED;
    }
    uint64_t const AGE = window.high - counter;
    if (AGE >= 64) {
        return ReplayResult::TOO_OLD;
    }
    uint64_t const BIT = uint64_t{1} << AGE;
    if ((window.bitmap & BIT) != 0) {
        return ReplayResult::DUPLICATE;
    }
    window.bitmap |= BIT;
    return ReplayResult::ACCEPTED;
}

auto reserve_tx_counter(uint64_t& next_counter) -> uint64_t {
    if (next_counter == 0 || next_counter == UINT64_MAX) {
        return 0;
    }
    return next_counter++;
}

}  // namespace ker::net::wki::auth_protocol
