#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth_protocol.hpp>
#include <vfs/stat.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/wire.hpp"

namespace protocol = ker::net::wki::auth_protocol;
using namespace ker::net::wki;

TEST(WkiAuthProtocol, SessionDerivationIsSymmetricDirectionalAndEpochBound) {
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
    for (size_t index = 0; index < key.size(); ++index) {
        key.at(index) = static_cast<uint8_t>(index);
    }
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> low_nonce{};
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> high_nonce{};
    low_nonce.fill(0x11);
    high_nonce.fill(0x22);

    protocol::DerivedSession low_view{};
    protocol::DerivedSession high_view{};
    ASSERT_TRUE(protocol::derive_session(key, 7, 1, 2, 0x11223344, 0x55667788, 3, 9, 0x12, 0x34, low_nonce, high_nonce, low_view));
    ASSERT_TRUE(protocol::derive_session(key, 7, 2, 1, 0x55667788, 0x11223344, 9, 3, 0x34, 0x12, high_nonce, low_nonce, high_view));
    EXPECT_EQ(low_view.session_id, high_view.session_id);
    EXPECT_EQ(low_view.low_to_high, high_view.low_to_high);
    EXPECT_EQ(low_view.high_to_low, high_view.high_to_low);
    EXPECT_NE(low_view.low_to_high, low_view.high_to_low);

    protocol::DerivedSession successor{};
    auto successor_nonce = low_nonce;
    successor_nonce.at(0)++;
    ASSERT_TRUE(protocol::derive_session(key, 7, 1, 2, 0x11223345, 0x55667788, 3, 9, 0x12, 0x34, successor_nonce, high_nonce, successor));
    EXPECT_NE(successor.session_id, low_view.session_id);
    EXPECT_NE(successor.low_to_high, low_view.low_to_high);

    protocol::DerivedSession key_boundary{};
    ASSERT_TRUE(protocol::derive_session(key, 8, 1, 2, 0x11223344, 0x55667788, 3, 9, 0x12, 0x34, low_nonce, high_nonce, key_boundary));
    EXPECT_NE(key_boundary.session_id, low_view.session_id);

    protocol::DerivedSession channel_epoch_boundary{};
    ASSERT_TRUE(
        protocol::derive_session(key, 7, 1, 2, 0x11223344, 0x55667788, 4, 9, 0x12, 0x34, low_nonce, high_nonce, channel_epoch_boundary));
    EXPECT_NE(channel_epoch_boundary.session_id, low_view.session_id);

    protocol::DerivedSession capability_boundary{};
    ASSERT_TRUE(
        protocol::derive_session(key, 7, 1, 2, 0x11223344, 0x55667788, 3, 9, 0x13, 0x34, low_nonce, high_nonce, capability_boundary));
    EXPECT_NE(capability_boundary.session_id, low_view.session_id);
}

TEST(WkiAuthProtocol, TagProtectsEndToEndFieldsButPermitsRouterMutation) {
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
    key.fill(0xA5);
    std::array<uint8_t, 5> payload = {1, 2, 3, 4, 5};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.channel_id = 17;
    header.seq_num = 9;
    header.ack_num = 8;
    header.payload_len = payload.size();
    header.credits = 4;
    header.hop_ttl = 16;
    header.src_port = 3;
    header.dst_port = 4;
    header.checksum = 0x12345678;
    header.reserved = 17;
    WkiAuthTrailer trailer{};
    trailer.session_id.fill(0x3C);
    trailer.counter = 99;

    auto const digest = protocol::frame_tag(key, header, payload.data(), trailer, false);
    std::copy_n(digest.begin(), trailer.tag.size(), trailer.tag.begin());
    EXPECT_TRUE(protocol::verify_frame_tag(key, header, payload.data(), trailer, false));

    WkiHeader routed = header;
    routed.hop_ttl = 3;
    routed.checksum = 0xDEADBEEF;
    EXPECT_TRUE(protocol::verify_frame_tag(key, routed, payload.data(), trailer, false));

    WkiHeader substituted = header;
    substituted.dst_node = 3;
    EXPECT_FALSE(protocol::verify_frame_tag(key, substituted, payload.data(), trailer, false));
    substituted = header;
    substituted.ack_num++;
    EXPECT_FALSE(protocol::verify_frame_tag(key, substituted, payload.data(), trailer, false));

    payload.at(2) ^= 0x80;
    EXPECT_FALSE(protocol::verify_frame_tag(key, header, payload.data(), trailer, false));
    payload.at(2) ^= 0x80;
    trailer.counter++;
    EXPECT_FALSE(protocol::verify_frame_tag(key, header, payload.data(), trailer, false));
}

TEST(WkiAuthProtocol, HandshakeDomainCannotBeReflectedAsData) {
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
    key.fill(0x19);
    std::array<uint8_t, 3> payload = {7, 8, 9};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_PRIORITY);
    header.msg_type = static_cast<uint8_t>(MsgType::HELLO);
    header.src_node = 1;
    header.dst_node = 2;
    header.payload_len = payload.size();
    WkiAuthTrailer trailer{};
    trailer.counter = 44;
    auto const digest = protocol::frame_tag(key, header, payload.data(), trailer, true);
    std::copy_n(digest.begin(), trailer.tag.size(), trailer.tag.begin());

    EXPECT_TRUE(protocol::verify_frame_tag(key, header, payload.data(), trailer, true));
    EXPECT_FALSE(protocol::verify_frame_tag(key, header, payload.data(), trailer, false));
}

TEST(WkiAuthProtocol, ReplayWindowAllowsBoundedReorderingAndRejectsReplay) {
    protocol::ReplayWindow window{};
    EXPECT_EQ(protocol::replay_admit(window, 10), protocol::ReplayResult::ACCEPTED);
    EXPECT_EQ(protocol::replay_admit(window, 12), protocol::ReplayResult::ACCEPTED);
    EXPECT_EQ(protocol::replay_admit(window, 11), protocol::ReplayResult::ACCEPTED);
    EXPECT_EQ(protocol::replay_admit(window, 11), protocol::ReplayResult::DUPLICATE);
    EXPECT_EQ(protocol::replay_admit(window, 0), protocol::ReplayResult::INVALID);
    EXPECT_EQ(protocol::replay_admit(window, 80), protocol::ReplayResult::ACCEPTED);
    EXPECT_EQ(protocol::replay_admit(window, 10), protocol::ReplayResult::TOO_OLD);
}

TEST(WkiAuthProtocol, TransmitCounterExhaustionIsTerminal) {
    uint64_t next = 1;
    EXPECT_EQ(protocol::reserve_tx_counter(next), 1U);
    EXPECT_EQ(next, 2U);

    next = UINT64_MAX - 1;
    EXPECT_EQ(protocol::reserve_tx_counter(next), UINT64_MAX - 1);
    EXPECT_EQ(next, UINT64_MAX);
    EXPECT_EQ(protocol::reserve_tx_counter(next), 0U);
    EXPECT_EQ(next, UINT64_MAX);

    next = 0;
    EXPECT_EQ(protocol::reserve_tx_counter(next), 0U);
    EXPECT_EQ(next, 0U);
}
