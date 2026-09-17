#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth_crypto.hpp>
#include <net/wki/auth_protocol.hpp>
#include <test/ktest.hpp>
#include <vfs/stat.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"
#include "net/wki/wki.hpp"

namespace {

constexpr auto nybble(char value) -> uint8_t {
    return value >= '0' && value <= '9' ? static_cast<uint8_t>(value - '0') : static_cast<uint8_t>(value - 'a' + 10);
}

template <size_t Size>
constexpr auto from_hex(const char (&value)[(Size * 2) + 1]) -> std::array<uint8_t, Size> {
    std::array<uint8_t, Size> result{};
    for (size_t index = 0; index < Size; ++index) {
        result.at(index) = static_cast<uint8_t>((nybble(value[index * 2]) << 4U) | nybble(value[index * 2 + 1]));
    }
    return result;
}

}  // namespace

KTEST(WkiAuth, Sha256AndHmacKnownAnswers) {
    using namespace ker::net::wki;
    constexpr char MESSAGE[] = "abc";
    constexpr auto SHA_EXPECTED = from_hex<32>("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    KEXPECT_TRUE(auth_crypto::sha256(MESSAGE, sizeof(MESSAGE) - 1) == SHA_EXPECTED);

    std::array<uint8_t, 20> key{};
    key.fill(0x0b);
    constexpr char HMAC_MESSAGE[] = "Hi There";
    constexpr auto HMAC_EXPECTED = from_hex<32>("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    KEXPECT_TRUE(auth_crypto::hmac_sha256(key.data(), key.size(), HMAC_MESSAGE, sizeof(HMAC_MESSAGE) - 1) == HMAC_EXPECTED);
}

KTEST(WkiAuth, SessionAndReplayAreFreshAndBounded) {
    using namespace ker::net::wki;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> low_nonce{};
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> high_nonce{};
    key.fill(0x5a);
    low_nonce.fill(0x11);
    high_nonce.fill(0x22);

    auth_protocol::DerivedSession first{};
    auth_protocol::DerivedSession successor{};
    KEXPECT_TRUE(auth_protocol::derive_session(key, 9, 1, 2, 100, 200, 7, 8, 1, 2, low_nonce, high_nonce, first));
    low_nonce.at(0)++;
    KEXPECT_TRUE(auth_protocol::derive_session(key, 9, 1, 2, 101, 200, 7, 8, 1, 2, low_nonce, high_nonce, successor));
    KEXPECT_FALSE(first.session_id == successor.session_id);
    KEXPECT_FALSE(first.low_to_high == first.high_to_low);

    auth_protocol::ReplayWindow replay{};
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 4), auth_protocol::ReplayResult::ACCEPTED);
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 6), auth_protocol::ReplayResult::ACCEPTED);
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 5), auth_protocol::ReplayResult::ACCEPTED);
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 5), auth_protocol::ReplayResult::DUPLICATE);
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 80), auth_protocol::ReplayResult::ACCEPTED);
    KEXPECT_EQ(auth_protocol::replay_admit(replay, 4), auth_protocol::ReplayResult::TOO_OLD);
}

KTEST(WkiAuth, RetriedConfirmationPromotesOnlyVerifiedPendingSession) {
    using namespace ker::net::wki;
    for (uint64_t counter : {uint64_t{1}, uint64_t{81}}) {
        // A private peer exercises promotion without touching live peer state
        // or requiring a provisioned remote node in the single-node VM.
        WkiPeer peer{};
        auto& session = peer.auth_session;
        session.active = true;
        session.session_id.fill(0x11);
        session.pending = true;
        session.pending_session_id.fill(0x22);
        session.pending_tx_key.fill(0x33);
        session.pending_rx_key.fill(0x44);
        session.generation = 7;
        session.rx_counter_high = 14;
        session.rx_counter_bitmap = 3;
        WkiAuthTrailer verified{};
        verified.session_id = session.pending_session_id;

        KEXPECT_FALSE(wki_auth_confirm_peer_session(&peer, verified));
        KEXPECT_TRUE(session.pending);
        KEXPECT_EQ(session.generation, 7U);
        KEXPECT_EQ(session.rx_counter_high, 14U);
        KEXPECT_EQ(session.rx_counter_bitmap, 3U);

        verified.counter = counter;
        verified.session_id.at(0)++;
        KEXPECT_FALSE(wki_auth_confirm_peer_session(&peer, verified));
        KEXPECT_TRUE(session.pending);
        KEXPECT_EQ(session.generation, 7U);
        KEXPECT_EQ(session.rx_counter_high, 14U);
        KEXPECT_EQ(session.rx_counter_bitmap, 3U);

        verified.session_id = session.pending_session_id;
        KEXPECT_TRUE(wki_auth_confirm_peer_session(&peer, verified));
        KEXPECT_TRUE(session.active);
        KEXPECT_FALSE(session.pending);
        KEXPECT_TRUE(session.session_id == verified.session_id);
        KEXPECT_TRUE(session.retired_session_valid);
        KEXPECT_EQ(session.retired_session_id.at(0), 0x11U);
        KEXPECT_EQ(session.generation, 8U);
        KEXPECT_EQ(session.rx_counter_high, counter);
        KEXPECT_EQ(session.rx_counter_bitmap, 1U);
        KEXPECT_EQ(session.tx_key.at(0), 0x33U);
        KEXPECT_EQ(session.rx_key.at(0), 0x44U);
        KEXPECT_TRUE(session.pending_tx_key == (std::array<uint8_t, WKI_AUTH_KEY_SIZE>{}));
        KEXPECT_TRUE(session.pending_rx_key == (std::array<uint8_t, WKI_AUTH_KEY_SIZE>{}));
        KEXPECT_FALSE(wki_auth_confirm_peer_session(&peer, verified));
        KEXPECT_EQ(session.generation, 8U);

        auth_protocol::ReplayWindow replay{.high = session.rx_counter_high, .bitmap = session.rx_counter_bitmap};
        KEXPECT_EQ(auth_protocol::replay_admit(replay, counter), auth_protocol::ReplayResult::DUPLICATE);
        if (counter > WKI_AUTH_REPLAY_WINDOW) {
            KEXPECT_EQ(auth_protocol::replay_admit(replay, 1), auth_protocol::ReplayResult::TOO_OLD);
        }
        KEXPECT_EQ(auth_protocol::replay_admit(replay, counter + 1), auth_protocol::ReplayResult::ACCEPTED);
        wki_auth_retire_peer_session(&peer);
        KEXPECT_FALSE(session.active);
    }
}

KTEST(WkiAuth, SignalPolicyDenialsCompleteCorrelatedRequests) { KEXPECT_TRUE(ker::net::wki::wki_selftest_signal_policy_denials()); }

KTEST(WkiAuth, PolicyDenialPreservesFollowingDispatch) { KEXPECT_TRUE(ker::net::wki::wki_selftest_policy_denial_dispatch()); }
