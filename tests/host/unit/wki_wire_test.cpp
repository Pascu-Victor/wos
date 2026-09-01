// Unit tests for WKI wire protocol definitions (wire.hpp).
// Tests header construction, field extraction, sequence arithmetic,
// and payload layout assertions.

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <net/wki/wire.hpp>
#include <vfs/stat.hpp>

using namespace ker::net::wki;

// ---------------------------------------------------------------------------
// Header size and layout
// ---------------------------------------------------------------------------

TEST(WkiWire, HeaderIs32Bytes) { EXPECT_EQ(sizeof(WkiHeader), 32u); }

TEST(WkiWire, HelloPayloadIs96Bytes) { EXPECT_EQ(sizeof(HelloPayload), 96u); }

TEST(WkiWire, HeartbeatPayloadIs16Bytes) { EXPECT_EQ(sizeof(HeartbeatPayload), 16u); }

TEST(WkiWire, PeerGoodbyePayloadIs8Bytes) { EXPECT_EQ(sizeof(PeerGoodbyePayload), 8u); }

TEST(WkiWire, NetIpv6CapabilityUsesExactAdditiveSuffix) {
    EXPECT_EQ(WKI_CAP_NET_IPV6_STATE, 0x0020u);
    EXPECT_EQ(sizeof(NetIpv6StateEntry), 20u);
    EXPECT_EQ(sizeof(NetIpv6StateSuffix), 168u);
    EXPECT_EQ(offsetof(NetIpv6StateSuffix, entries), 8u);

    NetIpv6StateSuffix suffix = wki_net_ipv6_state_empty();
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    suffix.count = WKI_NET_IPV6_STATE_MAX_ADDRS;
    for (size_t i = 0; i < suffix.count; ++i) {
        suffix.entries.at(i).address.at(0) = 0x20;
        suffix.entries.at(i).address.at(1) = 0x01;
        suffix.entries.at(i).address.at(15) = static_cast<uint8_t>(i + 1);
        suffix.entries.at(i).prefix_len = 64;
    }
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    suffix.count++;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.length--;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.reserved.at(1) = 1;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).prefix_len = 129;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).prefix_len = 64;
    suffix.entries.at(0).scope = 1;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.entries.at(7).address.at(15) = 1;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
}

TEST(WkiWire, NetIpv6CapabilityRejectsInvalidAddressScopePairs) {
    NetIpv6StateSuffix suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).prefix_len = 64;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));  // :: is not an interface address

    suffix.entries.at(0).address.at(0) = 0xFF;
    suffix.entries.at(0).address.at(1) = 0x02;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));

    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).scope = 253;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 0;
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));

    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(0) = 0xFE;
    suffix.entries.at(0).address.at(1) = 0x80;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 253;
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));

    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(15) = 1;
    suffix.entries.at(0).scope = 0;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 254;
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
}

TEST(WkiWire, NetIpv6CompatibilityRequiresExactNegotiatedLengths) {
    constexpr size_t NAME_LENGTH = 7;
    constexpr size_t ADVERT_LEGACY = sizeof(ResourceAdvertNetPayload) + NAME_LENGTH;
    constexpr size_t ACK_LEGACY = sizeof(DevAttachAckNetPayload);
    constexpr size_t NOTIFY_LEGACY = sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload);

    for (size_t legacy : {ADVERT_LEGACY, ACK_LEGACY, NOTIFY_LEGACY}) {
        EXPECT_TRUE(wki_net_ipv6_extended_length_valid(false, legacy, legacy));
        EXPECT_FALSE(wki_net_ipv6_extended_length_valid(false, legacy + sizeof(NetIpv6StateSuffix), legacy));
        EXPECT_FALSE(wki_net_ipv6_extended_length_valid(false, legacy + 1, legacy));
        EXPECT_FALSE(wki_net_ipv6_extended_length_valid(true, legacy, legacy));
        EXPECT_TRUE(wki_net_ipv6_extended_length_valid(true, legacy + sizeof(NetIpv6StateSuffix), legacy));
        EXPECT_FALSE(wki_net_ipv6_extended_length_valid(true, legacy + sizeof(NetIpv6StateSuffix) + 1, legacy));
    }
}

TEST(WkiWire, NetIpv6ReadinessRejectsTentativeAndDadFailedOnlyState) {
    NetIpv6StateSuffix suffix = wki_net_ipv6_state_empty();
    EXPECT_FALSE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.count = 1;
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).prefix_len = 64;
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_TENTATIVE;
    EXPECT_FALSE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED;
    EXPECT_FALSE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = 0;
    EXPECT_TRUE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DEPRECATED;
    EXPECT_TRUE(wki_net_ipv6_state_has_usable_address(suffix));
}

TEST(WkiWire, NetIpv6CapabilityRejectsUnknownAndContradictoryFlags) {
    NetIpv6StateSuffix suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).prefix_len = 64;

    suffix.entries.at(0).flags = 0x0001;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_TENTATIVE;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_TENTATIVE | WKI_NET_IPV6_ADDR_F_DEPRECATED;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_DEPRECATED;
    EXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DEPRECATED | WKI_NET_IPV6_ADDR_F_PERMANENT | WKI_NET_IPV6_ADDR_F_NOPREFIXROUTE;
    EXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    EXPECT_TRUE(wki_net_ipv6_state_has_usable_address(suffix));
}

TEST(WkiWire, VfsMultiRdmaCapabilityAndAuxFlagPreserveLayouts) {
    EXPECT_EQ(WKI_CAP_VFS_MULTI_RDMA_LANES, 0x0008u);
    EXPECT_EQ(DEV_ATTACH_VFS_AUX_LANE, 0x80u);
    EXPECT_EQ(DEV_ATTACH_VFS_AUX_LANE & (DEV_ATTACH_MODE_KIND_MASK | DEV_ATTACH_ACCESS_MASK | DEV_ATTACH_DISABLE_RDMA), 0);
    EXPECT_EQ(sizeof(HelloPayload), 96u);
    EXPECT_EQ(sizeof(DevAttachReqPayload), 12u);

    uint8_t const ANCHOR_RDMA = wki_vfs_proxy_attach_mode(true, true);
    uint8_t const AUX_RDMA = wki_vfs_proxy_attach_mode(false, true);
    uint8_t const AUX_MESSAGE = wki_vfs_proxy_attach_mode(false, false);
    EXPECT_EQ(ANCHOR_RDMA, static_cast<uint8_t>(AttachMode::PROXY));
    EXPECT_EQ(AUX_RDMA, static_cast<uint8_t>(static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_VFS_AUX_LANE));
    EXPECT_EQ(AUX_MESSAGE,
              static_cast<uint8_t>(static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_VFS_AUX_LANE | DEV_ATTACH_DISABLE_RDMA));

    EXPECT_TRUE(wki_vfs_attach_lane_is_anchor(ANCHOR_RDMA, true));
    EXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_RDMA, true));
    EXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, true));
    EXPECT_TRUE(wki_vfs_attach_lane_is_anchor(ANCHOR_RDMA, false));
    EXPECT_TRUE(wki_vfs_attach_lane_is_anchor(AUX_RDMA, false));
    EXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, false));
}

// ---------------------------------------------------------------------------
// Version/flags byte helpers
// ---------------------------------------------------------------------------

TEST(WkiWire, VersionFlagsRoundTrip) {
    for (uint8_t ver = 0; ver < 16; ver++) {
        for (uint8_t flags = 0; flags < 16; flags++) {
            uint8_t vf = wki_version_flags(ver, flags);
            EXPECT_EQ(wki_version(vf), ver);
            EXPECT_EQ(wki_flags(vf), flags);
        }
    }
}

TEST(WkiWire, CurrentVersionMatchesConstant) {
    uint8_t vf = wki_version_flags(WKI_VERSION, 0);
    EXPECT_EQ(wki_version(vf), WKI_VERSION);
    EXPECT_EQ(wki_flags(vf), 0);
}

TEST(WkiWire, FlagBitsAreDistinct) {
    EXPECT_EQ(WKI_FLAG_ACK_PRESENT & WKI_FLAG_PRIORITY, 0);
    EXPECT_EQ(WKI_FLAG_ACK_PRESENT & WKI_FLAG_FRAGMENT, 0);
    EXPECT_EQ(WKI_FLAG_PRIORITY & WKI_FLAG_FRAGMENT, 0);
}

// ---------------------------------------------------------------------------
// Sequence number arithmetic (RFC 1982 style)
// ---------------------------------------------------------------------------

TEST(WkiWire, SeqBeforeBasic) {
    EXPECT_TRUE(seq_before(0, 1));
    EXPECT_TRUE(seq_before(100, 200));
    EXPECT_FALSE(seq_before(200, 100));
    EXPECT_FALSE(seq_before(1, 1));
}

TEST(WkiWire, SeqAfterBasic) {
    EXPECT_TRUE(seq_after(1, 0));
    EXPECT_TRUE(seq_after(200, 100));
    EXPECT_FALSE(seq_after(100, 200));
    EXPECT_FALSE(seq_after(1, 1));
}

TEST(WkiWire, SeqArithmeticWraparound) {
    // Wraparound: UINT32_MAX is "before" 0 in sequence space
    EXPECT_TRUE(seq_before(UINT32_MAX, 0));
    EXPECT_TRUE(seq_after(0, UINT32_MAX));

    // Large gap near wraparound
    EXPECT_TRUE(seq_before(UINT32_MAX - 10, 5));
    EXPECT_TRUE(seq_after(5, UINT32_MAX - 10));
}

TEST(WkiWire, SeqBetween) {
    EXPECT_TRUE(seq_between(5, 3, 10));
    EXPECT_TRUE(seq_between(3, 3, 10));    // inclusive low
    EXPECT_FALSE(seq_between(10, 3, 10));  // exclusive high
    EXPECT_FALSE(seq_between(2, 3, 10));

    // Wraparound
    EXPECT_TRUE(seq_between(UINT32_MAX, UINT32_MAX - 5, 5));
    EXPECT_TRUE(seq_between(0, UINT32_MAX - 5, 5));
}

// ---------------------------------------------------------------------------
// Header construction and field access
// ---------------------------------------------------------------------------

TEST(WkiWire, HeaderFieldAccess) {
    WkiHeader hdr = {};
    hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
    hdr.msg_type = static_cast<uint8_t>(MsgType::HELLO);
    hdr.src_node = 0x0001;
    hdr.dst_node = 0x0002;
    hdr.channel_id = WKI_CHAN_CONTROL;
    hdr.seq_num = 42;
    hdr.ack_num = 0;
    hdr.payload_len = 96;
    hdr.credits = 64;
    hdr.hop_ttl = WKI_DEFAULT_TTL;
    hdr.src_port = 0;
    hdr.dst_port = 0;
    hdr.checksum = 0;
    hdr.reserved = 0;

    EXPECT_EQ(wki_version(hdr.version_flags), WKI_VERSION);
    EXPECT_EQ(wki_flags(hdr.version_flags) & WKI_FLAG_ACK_PRESENT, WKI_FLAG_ACK_PRESENT);
    EXPECT_EQ(hdr.msg_type, static_cast<uint8_t>(MsgType::HELLO));
    EXPECT_EQ(hdr.src_node, 0x0001);
    EXPECT_EQ(hdr.payload_len, 96);
}

// ---------------------------------------------------------------------------
// LSA payload layout and accessors
// ---------------------------------------------------------------------------

TEST(WkiWire, LsaPayloadNeighborAccess) {
    // Allocate a buffer large enough for header + 3 neighbors
    constexpr size_t n = 3;
    uint8_t buf[sizeof(LsaPayload) + n * sizeof(LsaNeighborEntry)] = {};

    auto* lsa = reinterpret_cast<LsaPayload*>(buf);
    lsa->origin_node = 0x0001;
    lsa->lsa_seq = 1;
    lsa->num_neighbors = n;
    lsa->rdma_zone_bitmap = 0;

    auto* nbrs = lsa_neighbors(lsa);
    for (uint16_t i = 0; i < n; i++) {
        nbrs[i].node_id = static_cast<uint16_t>(0x0010 + i);
        nbrs[i].link_cost = static_cast<uint16_t>(i + 1);
        nbrs[i].transport_mtu = 8954;
    }

    // Read back via const accessor
    const auto* clsa = reinterpret_cast<const LsaPayload*>(buf);
    const auto* cnbrs = lsa_neighbors(clsa);
    EXPECT_EQ(cnbrs[0].node_id, 0x0010);
    EXPECT_EQ(cnbrs[1].link_cost, 2);
    EXPECT_EQ(cnbrs[2].transport_mtu, 8954);

    EXPECT_EQ(lsa_total_size(clsa), sizeof(LsaPayload) + n * sizeof(LsaNeighborEntry));
}

// ---------------------------------------------------------------------------
// Hello payload magic and structure
// ---------------------------------------------------------------------------

TEST(WkiWire, HelloMagicValue) {
    HelloPayload hello = {};
    hello.magic = WKI_HELLO_MAGIC;
    EXPECT_EQ(hello.magic, 0x574B4900u);

    // Verify magic bytes spell "WKI\0"
    auto* bytes = reinterpret_cast<uint8_t*>(&hello.magic);
    // Packed struct, little-endian on x86
    EXPECT_EQ(bytes[0], 0x00);  // '\0'
    EXPECT_EQ(bytes[1], 0x49);  // 'I'
    EXPECT_EQ(bytes[2], 0x4B);  // 'K'
    EXPECT_EQ(bytes[3], 0x57);  // 'W'
}

// ---------------------------------------------------------------------------
// Well-known channel IDs and constants
// ---------------------------------------------------------------------------

TEST(WkiWire, ChannelIdsAreDistinct) {
    EXPECT_NE(WKI_CHAN_CONTROL, WKI_CHAN_ZONE_MGMT);
    EXPECT_NE(WKI_CHAN_CONTROL, WKI_CHAN_EVENT_BUS);
    EXPECT_NE(WKI_CHAN_CONTROL, WKI_CHAN_RESOURCE);
    EXPECT_LT(WKI_CHAN_RESOURCE, WKI_CHAN_DYNAMIC_BASE);
}

TEST(WkiWire, MsgTypeEnumValues) {
    // Spot-check a few message types to catch accidental renumbering
    EXPECT_EQ(static_cast<uint8_t>(MsgType::HELLO), 0x01);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::HEARTBEAT), 0x03);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::LSA), 0x05);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::PEER_GOODBYE), 0x0C);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::ZONE_CREATE_REQ), 0x20);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::EVENT_PUBLISH), 0x32);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::DEV_ATTACH_REQ), 0x40);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::TASK_SUBMIT), 0x50);
}

TEST(WkiWire, DevDetachMatchesBindingIdentity) {
    DevDetachPayload detach = {};
    detach.target_node = 0x1001;
    detach.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    detach.resource_id = 42;

    EXPECT_TRUE(wki_dev_detach_matches_binding(0x2002, ResourceType::VFS, 42, 0x2002, detach));
    EXPECT_FALSE(wki_dev_detach_matches_binding(0x2003, ResourceType::VFS, 42, 0x2002, detach));
    EXPECT_FALSE(wki_dev_detach_matches_binding(0x2002, ResourceType::BLOCK, 42, 0x2002, detach));
    EXPECT_FALSE(wki_dev_detach_matches_binding(0x2002, ResourceType::VFS, 43, 0x2002, detach));
}

TEST(WkiWire, DevDetachExactCookieAndIncarnationFormsMatchBinding) {
    uint8_t payload[wki_dev_detach_payload_size(true)] = {};
    auto* detach = reinterpret_cast<DevDetachPayload*>(payload);
    detach->target_node = 0x1001;
    detach->resource_type = static_cast<uint16_t>(ResourceType::VFS);
    detach->resource_id = 42;
    payload[WKI_DEV_DETACH_COOKIE_OFFSET] = 0x5C;
    ResourceIncarnationToken const BINDING_INCARNATION = {
        .owner_boot_epoch = 0x12345678,
        .resource_incarnation = 0x9ABCDEF0,
    };
    ResourceIncarnationToken const STALE_INCARNATION = {
        .owner_boot_epoch = BINDING_INCARNATION.owner_boot_epoch,
        .resource_incarnation = BINDING_INCARNATION.resource_incarnation - 1,
    };
    std::memcpy(payload + WKI_DEV_DETACH_INCARNATION_OFFSET, &BINDING_INCARNATION, sizeof(BINDING_INCARNATION));

    EXPECT_EQ(sizeof(DevDetachPayload), 8u);
    EXPECT_EQ(wki_dev_detach_payload_size(false), 9u);
    EXPECT_EQ(wki_dev_detach_payload_size(true), 17u);
    EXPECT_TRUE(wki_dev_detach_payload_size_matches(wki_dev_detach_payload_size(false), false));
    EXPECT_TRUE(wki_dev_detach_payload_size_matches(wki_dev_detach_payload_size(true), true));
    EXPECT_FALSE(wki_dev_detach_payload_size_matches(sizeof(DevDetachPayload), false));
    EXPECT_FALSE(wki_dev_detach_payload_size_matches(wki_dev_detach_payload_size(true), false));
    EXPECT_FALSE(wki_dev_detach_payload_size_matches(wki_dev_detach_payload_size(false), true));
    EXPECT_EQ(wki_dev_detach_cookie_from_payload(payload, wki_dev_detach_payload_size(false)), 0x5C);
    EXPECT_EQ(wki_dev_detach_cookie_from_payload(payload, sizeof(DevDetachPayload)), 0);
    EXPECT_TRUE(wki_dev_detach_cookie_matches_binding(0x5C, 0x5C));
    EXPECT_FALSE(wki_dev_detach_cookie_matches_binding(0x5C, 0));
    EXPECT_TRUE(wki_dev_detach_cookie_matches_binding(0, 0));
    EXPECT_FALSE(wki_dev_detach_cookie_matches_binding(0x5C, 0x5D));
    EXPECT_TRUE(wki_dev_detach_incarnation_matches_binding(BINDING_INCARNATION, BINDING_INCARNATION, true));
    EXPECT_FALSE(wki_dev_detach_incarnation_matches_binding(BINDING_INCARNATION, STALE_INCARNATION, true));
    EXPECT_FALSE(wki_dev_detach_incarnation_matches_binding(BINDING_INCARNATION, {}, true));
    EXPECT_TRUE(wki_dev_detach_incarnation_matches_binding(BINDING_INCARNATION, {}, false));
}

TEST(WkiWire, DevOpResponseMatchesExpectedIdentity) {
    DevOpRespPayload resp = {};
    resp.op_id = OP_BLOCK_READ;
    resp.reserved = 0x1234;

    EXPECT_TRUE(wki_dev_op_response_matches_expected(OP_BLOCK_READ, 0x1234, resp));
    EXPECT_FALSE(wki_dev_op_response_matches_expected(OP_BLOCK_WRITE, 0x1234, resp));
    EXPECT_FALSE(wki_dev_op_response_matches_expected(OP_BLOCK_READ, 0x1235, resp));

    resp.op_id = OP_NET_OPEN;
    resp.reserved = 0xCAFE;
    EXPECT_TRUE(wki_dev_op_response_matches_expected(OP_NET_OPEN, 0xCAFE, resp));
    EXPECT_FALSE(wki_dev_op_response_matches_expected(OP_NET_CLOSE, 0xCAFE, resp));
    EXPECT_FALSE(wki_dev_op_response_matches_expected(OP_NET_OPEN, 0xCAFF, resp));

    resp.op_id = OP_NET_GET_STATS;
    resp.reserved = 0xBEEF;
    EXPECT_TRUE(wki_dev_op_response_matches_expected(OP_NET_GET_STATS, 0xBEEF, resp));
    EXPECT_FALSE(wki_dev_op_response_matches_expected(OP_NET_GET_STATS, 0xBEEE, resp));
}

TEST(WkiWire, VfsCloseNoSuccessResponseExtensionIsLengthGated) {
    std::array<uint8_t, WKI_VFS_CLOSE_EXTENDED_DATA_LEN> request{};

    EXPECT_EQ(WKI_VFS_CLOSE_LEGACY_DATA_LEN, 4u);
    EXPECT_EQ(WKI_VFS_CLOSE_EXTENDED_DATA_LEN, 5u);
    EXPECT_FALSE(wki_vfs_close_no_success_response_requested(nullptr, 0));
    EXPECT_FALSE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(WKI_VFS_CLOSE_LEGACY_DATA_LEN)));
    EXPECT_FALSE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));

    request.at(WKI_VFS_CLOSE_FLAGS_OFFSET) = WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
    EXPECT_TRUE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
    EXPECT_FALSE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(WKI_VFS_CLOSE_LEGACY_DATA_LEN)));

    request.at(WKI_VFS_CLOSE_FLAGS_OFFSET) = 0x80;
    EXPECT_FALSE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
    request.at(WKI_VFS_CLOSE_FLAGS_OFFSET) |= WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
    EXPECT_TRUE(wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
}

TEST(WkiWire, VfsUtimensUsesAnAdditiveFixedPrefix) {
    EXPECT_EQ(OP_VFS_UTIMENS, 0x0415u);
    EXPECT_EQ(WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK, 0x01u);
    EXPECT_EQ(WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT, 0x02u);
    EXPECT_EQ(sizeof(VfsUtimensReqPrefix), 36u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, atime_sec), 0u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, atime_nsec), 8u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, mtime_sec), 16u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, mtime_nsec), 24u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, path_len), 32u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, flags), 34u);
    EXPECT_EQ(offsetof(VfsUtimensReqPrefix, reserved), 35u);
    EXPECT_EQ(sizeof(DevOpReqPayload), 4u);
    EXPECT_EQ(sizeof(DevOpRespPayload), 8u);
}

TEST(WkiWire, VfsMetadataBatchUsesAdditiveBoundedFraming) {
    EXPECT_EQ(WKI_CAP_VFS_METADATA_BATCH, 0x0010u);
    EXPECT_EQ(OP_VFS_METADATA_BATCH, 0x0416u);
    EXPECT_EQ(VFS_METADATA_BATCH_VERSION, 1u);
    EXPECT_EQ(VFS_METADATA_BATCH_MAX_ITEMS, 64u);
    EXPECT_EQ(VFS_METADATA_BATCH_MAX_STAT_ITEMS, 60u);
    EXPECT_EQ(VFS_METADATA_BATCH_MAX_PATH_LEN, 511u);
    EXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::CREATE_CLOSE), 1u);
    EXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::STAT_FOLLOW), 2u);
    EXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::UNLINK), 3u);
    EXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::RENAME), 4u);
    EXPECT_EQ(sizeof(VfsMetadataBatchHeader), 8u);
    EXPECT_EQ(offsetof(VfsMetadataBatchHeader, version), 0u);
    EXPECT_EQ(offsetof(VfsMetadataBatchHeader, operation), 2u);
    EXPECT_EQ(offsetof(VfsMetadataBatchHeader, count), 3u);
    EXPECT_EQ(offsetof(VfsMetadataBatchHeader, mode), 4u);

    constexpr size_t STAT_RECORD_SIZE = sizeof(int32_t) + sizeof(ker::vfs::Stat);
    EXPECT_LE(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + VFS_METADATA_BATCH_MAX_STAT_ITEMS * STAT_RECORD_SIZE,
              WKI_ETH_MAX_PAYLOAD);
    EXPECT_GT(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + (VFS_METADATA_BATCH_MAX_STAT_ITEMS + 1) * STAT_RECORD_SIZE,
              WKI_ETH_MAX_PAYLOAD);
    EXPECT_LE(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + VFS_METADATA_BATCH_MAX_ITEMS * sizeof(int32_t),
              WKI_ETH_MAX_PAYLOAD);
    EXPECT_EQ(sizeof(HelloPayload), 96u);
}

TEST(WkiWire, VfsXattrUsesVersionedBoundedReplayIdentity) {
    EXPECT_EQ(WKI_CAP_VFS_XATTR, 0x0040u);
    EXPECT_EQ(OP_VFS_XATTR, 0x0417u);
    EXPECT_EQ(WKI_VFS_XATTR_VERSION, 1u);
    EXPECT_EQ(WKI_VFS_XATTR_NAME_MAX, 255u);
    EXPECT_EQ(WKI_VFS_XATTR_DATA_MAX, 65536u);
    EXPECT_EQ(sizeof(VfsXattrPhaseHeader), 20u);
    EXPECT_EQ(sizeof(VfsXattrBeginPayload), 40u);
    EXPECT_EQ(sizeof(VfsXattrDataPayload), 28u);
    EXPECT_EQ(sizeof(VfsXattrResultPayload), 28u);
    EXPECT_EQ(offsetof(VfsXattrPhaseHeader, session_id), 0u);
    EXPECT_EQ(offsetof(VfsXattrPhaseHeader, operation_id), 8u);
    EXPECT_EQ(WKI_VFS_XATTR_MAX_DATA_CHUNK, WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(VfsXattrResultPayload));
    EXPECT_LT(WKI_VFS_XATTR_MAX_DATA_CHUNK, static_cast<size_t>(UINT16_MAX));

    VfsXattrDataPayload hostile_read = {};
    hostile_read.chunk_len = UINT16_MAX;
    EXPECT_GT(hostile_read.chunk_len, WKI_VFS_XATTR_MAX_DATA_CHUNK);

    VfsXattrBeginPayload begin = {};
    begin.header = {.session_id = 1,
                    .operation_id = 2,
                    .version = WKI_VFS_XATTR_VERSION,
                    .phase = VfsXattrPhase::BEGIN,
                    .operation = VfsXattrOperation::SET,
                    .target = VfsXattrTarget::PATH};
    begin.remote_fd = -1;
    begin.data_len = WKI_VFS_XATTR_DATA_MAX;
    begin.path_len = 1;
    begin.name_len = WKI_VFS_XATTR_NAME_MAX;
    begin.follow_final_symlink = 1;
    EXPECT_TRUE(wki_vfs_xattr_begin_valid(begin));
    begin.data_len++;
    EXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
    begin.data_len = 0;
    begin.header.operation_id = 0;
    EXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
}

TEST(WkiWire, DevAttachAckMatchesExpectedCookie) {
    DevAttachAckPayload ack = {};
    ack.resource_id = 55;
    ack.reserved = 0x7A;

    EXPECT_TRUE(wki_dev_attach_ack_matches_expected(0x7A, ack));
    EXPECT_FALSE(wki_dev_attach_ack_matches_expected(0x7B, ack));
}

TEST(WkiWire, NetNotifyHeaderMatchesExpectedCookieAndLength) {
    NetNotifyHeader notify = {};
    notify.magic = WKI_NET_NOTIFY_MAGIC;
    notify.attach_cookie = 0x2A;
    notify.data_len = 64;

    EXPECT_TRUE(wki_net_notify_header_matches_expected(0x2A, notify));
    EXPECT_FALSE(wki_net_notify_header_matches_expected(0x2B, notify));
    EXPECT_TRUE(wki_net_notify_payload_fits(sizeof(NetNotifyHeader) + 64, notify));
    EXPECT_FALSE(wki_net_notify_payload_fits(sizeof(NetNotifyHeader) + 63, notify));

    notify.magic = 0;
    EXPECT_FALSE(wki_net_notify_header_matches_expected(0x2A, notify));
}

// ---------------------------------------------------------------------------
// Header byte-level serialization (packed struct on wire)
// ---------------------------------------------------------------------------

TEST(WkiWire, HeaderSerializationRoundTrip) {
    WkiHeader orig = {};
    orig.version_flags = wki_version_flags(1, WKI_FLAG_ACK_PRESENT | WKI_FLAG_PRIORITY);
    orig.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    orig.src_node = 0xCAFE;
    orig.dst_node = 0xBEEF;
    orig.channel_id = 42;
    orig.seq_num = 0xDEADBEEF;
    orig.ack_num = 0x12345678;
    orig.payload_len = 1000;
    orig.credits = 255;
    orig.hop_ttl = 8;
    orig.src_port = 100;
    orig.dst_port = 200;
    orig.checksum = 0xAAAABBBB;
    orig.reserved = 0;

    // Serialize to bytes and back
    uint8_t wire[32];
    memcpy(wire, &orig, 32);

    WkiHeader copy;
    memcpy(&copy, wire, 32);

    EXPECT_EQ(copy.version_flags, orig.version_flags);
    EXPECT_EQ(copy.msg_type, orig.msg_type);
    EXPECT_EQ(copy.src_node, orig.src_node);
    EXPECT_EQ(copy.dst_node, orig.dst_node);
    EXPECT_EQ(copy.channel_id, orig.channel_id);
    EXPECT_EQ(copy.seq_num, orig.seq_num);
    EXPECT_EQ(copy.ack_num, orig.ack_num);
    EXPECT_EQ(copy.payload_len, orig.payload_len);
    EXPECT_EQ(copy.credits, orig.credits);
    EXPECT_EQ(copy.hop_ttl, orig.hop_ttl);
    EXPECT_EQ(copy.src_port, orig.src_port);
    EXPECT_EQ(copy.dst_port, orig.dst_port);
    EXPECT_EQ(copy.checksum, orig.checksum);
}
