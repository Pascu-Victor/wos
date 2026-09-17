#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/auth_policy.hpp>
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wire.hpp>
#include <test/ktest.hpp>
#include <vfs/stat.hpp>

KTEST(WkiIvshmem, CompleteAuthenticatedFramesSurviveRingWrap) { KEXPECT_TRUE(ker::net::wki::wki_ivshmem_selftest_complete_frames()); }

KTEST(WkiWire, NetIpv6CapabilityUsesExactAdditiveSuffix) {
    using namespace ker::net::wki;

    KEXPECT_EQ(WKI_CAP_NET_IPV6_STATE, static_cast<uint16_t>(0x0020));
    KEXPECT_EQ(sizeof(NetIpv6StateEntry), static_cast<size_t>(20));
    KEXPECT_EQ(sizeof(NetIpv6StateSuffix), static_cast<size_t>(168));
    KEXPECT_EQ(offsetof(NetIpv6StateSuffix, entries), static_cast<size_t>(8));

    NetIpv6StateSuffix suffix = wki_net_ipv6_state_empty();
    KEXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    suffix.count = static_cast<uint8_t>(WKI_NET_IPV6_STATE_MAX_ADDRS + 1);
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.version++;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).prefix_len = 129;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix = wki_net_ipv6_state_empty();
    suffix.entries.at(1).flags = 1;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));

    suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).prefix_len = 64;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).address.at(0) = 0xFF;
    suffix.entries.at(0).address.at(1) = 0x02;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).scope = 253;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 0;
    KEXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(0) = 0xFE;
    suffix.entries.at(0).address.at(1) = 0x80;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 253;
    KEXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).address = {};
    suffix.entries.at(0).address.at(15) = 1;
    suffix.entries.at(0).scope = 0;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).scope = 254;
    KEXPECT_TRUE(wki_net_ipv6_state_valid(suffix));

    constexpr size_t LEGACY_ACK = sizeof(DevAttachAckNetPayload);
    KEXPECT_TRUE(wki_net_ipv6_extended_length_valid(false, LEGACY_ACK, LEGACY_ACK));
    KEXPECT_FALSE(wki_net_ipv6_extended_length_valid(false, LEGACY_ACK + sizeof(NetIpv6StateSuffix), LEGACY_ACK));
    KEXPECT_TRUE(wki_net_ipv6_extended_length_valid(true, LEGACY_ACK + sizeof(NetIpv6StateSuffix), LEGACY_ACK));
    KEXPECT_FALSE(wki_net_ipv6_extended_length_valid(true, LEGACY_ACK + sizeof(NetIpv6StateSuffix) + 1, LEGACY_ACK));

    suffix = wki_net_ipv6_state_empty();
    suffix.count = 1;
    suffix.entries.at(0).address.at(0) = 0x20;
    suffix.entries.at(0).address.at(1) = 0x01;
    suffix.entries.at(0).prefix_len = 64;
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_TENTATIVE;
    KEXPECT_FALSE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED;
    KEXPECT_FALSE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = 0x0001;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_TENTATIVE;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_TENTATIVE | WKI_NET_IPV6_ADDR_F_DEPRECATED;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_DEPRECATED;
    KEXPECT_FALSE(wki_net_ipv6_state_valid(suffix));
    suffix.entries.at(0).flags = WKI_NET_IPV6_ADDR_F_DEPRECATED | WKI_NET_IPV6_ADDR_F_PERMANENT | WKI_NET_IPV6_ADDR_F_NOPREFIXROUTE;
    KEXPECT_TRUE(wki_net_ipv6_state_valid(suffix));
    KEXPECT_TRUE(wki_net_ipv6_state_has_usable_address(suffix));
    suffix.entries.at(0).flags = 0;
    KEXPECT_TRUE(wki_net_ipv6_state_has_usable_address(suffix));
}

KTEST(WkiWire, VfsMultiRdmaCapabilityAndAuxFlagPreserveLayouts) {
    using namespace ker::net::wki;

    KEXPECT_EQ(WKI_CAP_VFS_MULTI_RDMA_LANES, static_cast<uint16_t>(0x0008));
    KEXPECT_EQ(DEV_ATTACH_VFS_AUX_LANE, static_cast<uint8_t>(0x80));
    KEXPECT_EQ(
        static_cast<uint8_t>(DEV_ATTACH_VFS_AUX_LANE & (DEV_ATTACH_MODE_KIND_MASK | DEV_ATTACH_ACCESS_MASK | DEV_ATTACH_DISABLE_RDMA)),
        static_cast<uint8_t>(0));
    KEXPECT_EQ(sizeof(HelloPayload), static_cast<size_t>(172));
    KEXPECT_EQ(sizeof(DevAttachReqPayload), static_cast<size_t>(12));

    uint8_t const ANCHOR_RDMA = wki_vfs_proxy_attach_mode(true, true);
    uint8_t const AUX_RDMA = wki_vfs_proxy_attach_mode(false, true);
    uint8_t const AUX_MESSAGE = wki_vfs_proxy_attach_mode(false, false);
    KEXPECT_EQ(ANCHOR_RDMA, static_cast<uint8_t>(AttachMode::PROXY));
    KEXPECT_EQ(AUX_RDMA, static_cast<uint8_t>(static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_VFS_AUX_LANE));
    KEXPECT_EQ(AUX_MESSAGE,
               static_cast<uint8_t>(static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_VFS_AUX_LANE | DEV_ATTACH_DISABLE_RDMA));

    KEXPECT_TRUE(wki_vfs_attach_lane_is_anchor(ANCHOR_RDMA, true));
    KEXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_RDMA, true));
    KEXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, true));
    KEXPECT_TRUE(wki_vfs_attach_lane_is_anchor(ANCHOR_RDMA, false));
    KEXPECT_TRUE(wki_vfs_attach_lane_is_anchor(AUX_RDMA, false));
    KEXPECT_FALSE(wki_vfs_attach_lane_is_anchor(AUX_MESSAGE, false));
}

KTEST(WkiWire, DevDetachMatchesBindingIdentity) {
    ker::net::wki::DevDetachPayload detach = {};
    detach.target_node = 0x1001;
    detach.resource_type = static_cast<uint16_t>(ker::net::wki::ResourceType::NET);
    detach.resource_id = 99;

    KEXPECT_TRUE(ker::net::wki::wki_dev_detach_matches_binding(0x2002, ker::net::wki::ResourceType::NET, 99, 0x2002, detach));
    KEXPECT_FALSE(ker::net::wki::wki_dev_detach_matches_binding(0x2003, ker::net::wki::ResourceType::NET, 99, 0x2002, detach));
    KEXPECT_FALSE(ker::net::wki::wki_dev_detach_matches_binding(0x2002, ker::net::wki::ResourceType::VFS, 99, 0x2002, detach));
    KEXPECT_FALSE(ker::net::wki::wki_dev_detach_matches_binding(0x2002, ker::net::wki::ResourceType::NET, 100, 0x2002, detach));
}

KTEST(WkiWire, DevDetachOptionalCookieMatchesBinding) {
    uint8_t payload[sizeof(ker::net::wki::DevDetachPayload) + ker::net::wki::WKI_DEV_DETACH_COOKIE_BYTES] = {};
    auto* detach = reinterpret_cast<ker::net::wki::DevDetachPayload*>(payload);
    detach->target_node = 0x1001;
    detach->resource_type = static_cast<uint16_t>(ker::net::wki::ResourceType::NET);
    detach->resource_id = 42;
    payload[sizeof(ker::net::wki::DevDetachPayload)] = 0x5C;

    KEXPECT_EQ(ker::net::wki::wki_dev_detach_cookie_from_payload(payload, sizeof(payload)), 0x5C);
    KEXPECT_EQ(ker::net::wki::wki_dev_detach_cookie_from_payload(payload, sizeof(ker::net::wki::DevDetachPayload)), 0);
    KEXPECT_TRUE(ker::net::wki::wki_dev_detach_cookie_matches_binding(0x5C, 0x5C));
    KEXPECT_FALSE(ker::net::wki::wki_dev_detach_cookie_matches_binding(0x5C, 0));
    KEXPECT_TRUE(ker::net::wki::wki_dev_detach_cookie_matches_binding(0, 0));
    KEXPECT_FALSE(ker::net::wki::wki_dev_detach_cookie_matches_binding(0x5C, 0x5D));
}

KTEST(WkiWire, DevOpResponseMatchesExpectedIdentity) {
    ker::net::wki::DevOpRespPayload resp = {};
    resp.op_id = ker::net::wki::OP_BLOCK_FLUSH;
    resp.reserved = 0xBEEF;

    KEXPECT_TRUE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_BLOCK_FLUSH, 0xBEEF, resp));
    KEXPECT_FALSE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_BLOCK_READ, 0xBEEF, resp));
    KEXPECT_FALSE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_BLOCK_FLUSH, 0xBEEE, resp));

    resp.op_id = ker::net::wki::OP_NET_OPEN;
    resp.reserved = 0xCAFE;
    KEXPECT_TRUE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_NET_OPEN, 0xCAFE, resp));
    KEXPECT_FALSE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_NET_CLOSE, 0xCAFE, resp));
    KEXPECT_FALSE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_NET_OPEN, 0xCAFF, resp));

    resp.op_id = ker::net::wki::OP_NET_GET_STATS;
    resp.reserved = 0xBEEF;
    KEXPECT_TRUE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_NET_GET_STATS, 0xBEEF, resp));
    KEXPECT_FALSE(ker::net::wki::wki_dev_op_response_matches_expected(ker::net::wki::OP_NET_GET_STATS, 0xBEEE, resp));
}

KTEST(WkiWire, VfsCloseNoSuccessResponseExtensionIsLengthGated) {
    std::array<uint8_t, ker::net::wki::WKI_VFS_CLOSE_EXTENDED_DATA_LEN> request{};

    KEXPECT_EQ(ker::net::wki::WKI_VFS_CLOSE_LEGACY_DATA_LEN, 4);
    KEXPECT_EQ(ker::net::wki::WKI_VFS_CLOSE_EXTENDED_DATA_LEN, 5);
    KEXPECT_FALSE(ker::net::wki::wki_vfs_close_no_success_response_requested(nullptr, 0));
    KEXPECT_FALSE(ker::net::wki::wki_vfs_close_no_success_response_requested(
        request.data(), static_cast<uint16_t>(ker::net::wki::WKI_VFS_CLOSE_LEGACY_DATA_LEN)));
    KEXPECT_FALSE(ker::net::wki::wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));

    request.at(ker::net::wki::WKI_VFS_CLOSE_FLAGS_OFFSET) = ker::net::wki::WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
    KEXPECT_TRUE(ker::net::wki::wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
    KEXPECT_FALSE(ker::net::wki::wki_vfs_close_no_success_response_requested(
        request.data(), static_cast<uint16_t>(ker::net::wki::WKI_VFS_CLOSE_LEGACY_DATA_LEN)));

    request.at(ker::net::wki::WKI_VFS_CLOSE_FLAGS_OFFSET) = 0x80;
    KEXPECT_FALSE(ker::net::wki::wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
    request.at(ker::net::wki::WKI_VFS_CLOSE_FLAGS_OFFSET) |= ker::net::wki::WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE;
    KEXPECT_TRUE(ker::net::wki::wki_vfs_close_no_success_response_requested(request.data(), static_cast<uint16_t>(request.size())));
}

KTEST(WkiWire, VfsUtimensUsesAnAdditiveFixedPrefix) {
    using namespace ker::net::wki;

    KEXPECT_EQ(OP_VFS_UTIMENS, static_cast<uint16_t>(0x0415));
    KEXPECT_EQ(WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK, static_cast<uint8_t>(0x01));
    KEXPECT_EQ(WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT, static_cast<uint8_t>(0x02));
    KEXPECT_EQ(sizeof(VfsUtimensReqPrefix), static_cast<size_t>(36));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, atime_sec), static_cast<size_t>(0));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, atime_nsec), static_cast<size_t>(8));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, mtime_sec), static_cast<size_t>(16));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, mtime_nsec), static_cast<size_t>(24));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, path_len), static_cast<size_t>(32));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, flags), static_cast<size_t>(34));
    KEXPECT_EQ(offsetof(VfsUtimensReqPrefix, reserved), static_cast<size_t>(35));
    KEXPECT_EQ(sizeof(DevOpReqPayload), static_cast<size_t>(4));
    KEXPECT_EQ(sizeof(DevOpRespPayload), static_cast<size_t>(8));
}

KTEST(WkiWire, VfsMetadataBatchUsesAdditiveBoundedFraming) {
    using namespace ker::net::wki;

    KEXPECT_EQ(WKI_CAP_VFS_METADATA_BATCH, static_cast<uint16_t>(0x0010));
    KEXPECT_EQ(OP_VFS_METADATA_BATCH, static_cast<uint16_t>(0x0416));
    KEXPECT_EQ(VFS_METADATA_BATCH_VERSION, static_cast<uint16_t>(1));
    KEXPECT_EQ(VFS_METADATA_BATCH_MAX_ITEMS, static_cast<uint8_t>(64));
    KEXPECT_EQ(VFS_METADATA_BATCH_MAX_STAT_ITEMS, static_cast<uint8_t>(60));
    KEXPECT_EQ(VFS_METADATA_BATCH_MAX_PATH_LEN, static_cast<uint16_t>(511));
    KEXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::CREATE_CLOSE), static_cast<uint8_t>(1));
    KEXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::STAT_FOLLOW), static_cast<uint8_t>(2));
    KEXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::UNLINK), static_cast<uint8_t>(3));
    KEXPECT_EQ(static_cast<uint8_t>(VfsMetadataBatchOperation::RENAME), static_cast<uint8_t>(4));
    KEXPECT_EQ(sizeof(VfsMetadataBatchHeader), static_cast<size_t>(8));
    KEXPECT_EQ(offsetof(VfsMetadataBatchHeader, version), static_cast<size_t>(0));
    KEXPECT_EQ(offsetof(VfsMetadataBatchHeader, operation), static_cast<size_t>(2));
    KEXPECT_EQ(offsetof(VfsMetadataBatchHeader, count), static_cast<size_t>(3));
    KEXPECT_EQ(offsetof(VfsMetadataBatchHeader, mode), static_cast<size_t>(4));

    constexpr size_t STAT_RECORD_SIZE = sizeof(int32_t) + sizeof(ker::vfs::Stat);
    KEXPECT_TRUE(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + VFS_METADATA_BATCH_MAX_STAT_ITEMS * STAT_RECORD_SIZE <=
                 WKI_ETH_MAX_PAYLOAD);
    KEXPECT_TRUE(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + (VFS_METADATA_BATCH_MAX_STAT_ITEMS + 1) * STAT_RECORD_SIZE >
                 WKI_ETH_MAX_PAYLOAD);
    KEXPECT_TRUE(sizeof(DevOpRespPayload) + sizeof(VfsMetadataBatchHeader) + VFS_METADATA_BATCH_MAX_ITEMS * sizeof(int32_t) <=
                 WKI_ETH_MAX_PAYLOAD);
    KEXPECT_EQ(sizeof(HelloPayload), static_cast<size_t>(172));
}

KTEST(WkiWire, VfsXattrUsesVersionedBoundedReplayIdentity) {
    using namespace ker::net::wki;

    KEXPECT_EQ(WKI_CAP_VFS_XATTR, static_cast<uint16_t>(0x0040));
    KEXPECT_EQ(OP_VFS_XATTR, static_cast<uint16_t>(0x0417));
    KEXPECT_EQ(WKI_VFS_XATTR_VERSION, static_cast<uint8_t>(2));
    KEXPECT_EQ(WKI_VFS_XATTR_CALLER_SIZE, static_cast<size_t>(80));
    KEXPECT_EQ(WKI_VFS_FEATURE_XATTR_IDENTITY, static_cast<uint64_t>(32));
    KEXPECT_EQ(WKI_VFS_XATTR_NAME_MAX, static_cast<uint16_t>(255));
    KEXPECT_EQ(WKI_VFS_XATTR_DATA_MAX, static_cast<uint32_t>(65536));
    KEXPECT_EQ(sizeof(VfsXattrPhaseHeader), static_cast<size_t>(20));
    KEXPECT_EQ(sizeof(VfsXattrBeginPayload), static_cast<size_t>(40));
    KEXPECT_EQ(sizeof(VfsXattrDataPayload), static_cast<size_t>(28));
    KEXPECT_EQ(sizeof(VfsXattrResultPayload), static_cast<size_t>(28));
    KEXPECT_TRUE(WKI_VFS_XATTR_MAX_DATA_CHUNK < static_cast<size_t>(UINT16_MAX));

    VfsXattrDataPayload hostile_read = {};
    hostile_read.chunk_len = UINT16_MAX;
    KEXPECT_TRUE(hostile_read.chunk_len > WKI_VFS_XATTR_MAX_DATA_CHUNK);

    VfsXattrBeginPayload begin = {};
    begin.header = {.session_id = 7,
                    .operation_id = 9,
                    .version = WKI_VFS_XATTR_VERSION,
                    .phase = VfsXattrPhase::BEGIN,
                    .operation = VfsXattrOperation::GET,
                    .target = VfsXattrTarget::FD};
    begin.remote_fd = 3;
    begin.data_len = WKI_VFS_XATTR_DATA_MAX;
    begin.name_len = 1;
    KEXPECT_TRUE(wki_vfs_xattr_begin_valid(begin));
    begin.name_len = WKI_VFS_XATTR_NAME_MAX + 1;
    KEXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
    begin.name_len = 1;
    begin.require_directory = 1;
    KEXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
    begin.header.target = VfsXattrTarget::PATH;
    begin.path_len = 1;
    KEXPECT_TRUE(wki_vfs_xattr_begin_valid(begin));
    begin.path_len = 0;
    KEXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
    begin.remote_fd = -1;
    KEXPECT_TRUE(wki_vfs_xattr_begin_valid(begin));
    begin.header.version = 1;
    KEXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
    begin.header.version = WKI_VFS_XATTR_VERSION;
    begin.reserved[1] = 1;
    KEXPECT_FALSE(wki_vfs_xattr_begin_valid(begin));
}

KTEST(WkiWire, DevAttachAckMatchesExpectedCookie) {
    ker::net::wki::DevAttachAckPayload ack = {};
    ack.resource_id = 55;
    ack.reserved = 0x7A;

    KEXPECT_TRUE(ker::net::wki::wki_dev_attach_ack_matches_expected(0x7A, ack));
    KEXPECT_FALSE(ker::net::wki::wki_dev_attach_ack_matches_expected(0x7B, ack));
}

KTEST(WkiWire, NetNotifyHeaderMatchesExpectedCookieAndLength) {
    ker::net::wki::NetNotifyHeader notify = {};
    notify.magic = ker::net::wki::WKI_NET_NOTIFY_MAGIC;
    notify.attach_cookie = 0x2A;
    notify.data_len = 64;

    KEXPECT_TRUE(ker::net::wki::wki_net_notify_header_matches_expected(0x2A, notify));
    KEXPECT_FALSE(ker::net::wki::wki_net_notify_header_matches_expected(0x2B, notify));
    KEXPECT_TRUE(ker::net::wki::wki_net_notify_payload_fits(sizeof(ker::net::wki::NetNotifyHeader) + 64, notify));
    KEXPECT_FALSE(ker::net::wki::wki_net_notify_payload_fits(sizeof(ker::net::wki::NetNotifyHeader) + 63, notify));

    notify.magic = 0;
    KEXPECT_FALSE(ker::net::wki::wki_net_notify_header_matches_expected(0x2A, notify));
}

KTEST(WkiWireDirectoryGrant, ExactLayoutIdentityAndRequestVariants) {
    using namespace ker::net::wki;
    KEXPECT_EQ(sizeof(VfsDirectoryGrant), static_cast<size_t>(48));
    KEXPECT_EQ(sizeof(VfsDirectoryGrantRequest), static_cast<size_t>(88));
    KEXPECT_EQ(sizeof(VfsDirectoryGrantResponse), static_cast<size_t>(72));
    KEXPECT_EQ(WKI_VFS_FEATURE_DIRECTORY_GRANT & (WKI_VFS_FEATURE_DIRECTORY_GRANT - 1), 0ULL);
    KEXPECT_EQ(WKI_VFS_FEATURE_DIRECTORY_GRANT & (WKI_VFS_FEATURE_CHOWN_IDENTITY | WKI_VFS_FEATURE_FILE_METADATA_IDENTITY |
                                                  WKI_VFS_FEATURE_DIRECTORY_REFERENCE | WKI_VFS_FEATURE_REALPATH_AT),
               0ULL);
    VfsDirectoryGrant const GRANT{.version = 1,
                                  .policy = 3,
                                  .owner_node = 1,
                                  .source_node = 2,
                                  .target_node = 3,
                                  .resource_id = 4,
                                  .owner_incarnation = {5, 6},
                                  .source_boot_epoch = 7,
                                  .target_boot_epoch = 8,
                                  .owner_grant_id = 9,
                                  .submission_id = 10};
    KEXPECT_TRUE(wki_vfs_directory_grant_valid(GRANT));
    KEXPECT_FALSE(wki_vfs_directory_grant_valid({}));
    // Equality binds every wire byte, even fields that remain individually valid.
    for (size_t i = 0; i < sizeof(GRANT); ++i) {
        auto changed = GRANT;
        auto* bytes = reinterpret_cast<uint8_t*>(&changed);
        bytes[i] ^= 1;
        KEXPECT_FALSE(wki_vfs_directory_grant_equal(GRANT, changed));
    }
    for (unsigned field = 0; field < 14; ++field) {
        auto invalid = GRANT;
        switch (field) {
            case 0:
                invalid.version = 0;
                break;
            case 1:
                invalid.policy = 4;
                break;
            case 2:
                invalid.owner_node = 0;
                break;
            case 3:
                invalid.source_node = UINT16_MAX;
                break;
            case 4:
                invalid.target_node = 0;
                break;
            case 5:
                invalid.reserved = 1;
                break;
            case 6:
                invalid.resource_id = 0;
                break;
            case 7:
                invalid.owner_incarnation.owner_boot_epoch = 0;
                break;
            case 8:
                invalid.owner_incarnation.resource_incarnation = 0;
                break;
            case 9:
                invalid.source_boot_epoch = 0;
                break;
            case 10:
                invalid.target_boot_epoch = 0;
                break;
            case 11:
                invalid.owner_grant_id = 0;
                break;
            case 12:
                invalid.submission_id = 0;
                break;
            default:
                invalid.target_node = UINT16_MAX;
                break;
        }
        KEXPECT_FALSE(wki_vfs_directory_grant_valid(invalid));
    }
    constexpr size_t LENGTH = sizeof(VfsDirectoryGrantRequest) + WKI_VFS_DIRECTORY_CALLER_SIZE;
    VfsDirectoryGrantRequest const ISSUE{.policy = 3,
                                         .target_node = 3,
                                         .directory_fd = 42,
                                         .target_boot_epoch = 8,
                                         .source_pin_token = 11,
                                         .pin_token = 12,
                                         .submission_id = 10};
    VfsDirectoryGrantRequest const ADOPT{.operation = VfsDirectoryGrantOperation::ADOPT, .pin_token = 13, .grant = GRANT};
    KEXPECT_TRUE(wki_vfs_directory_grant_request_valid(ISSUE, LENGTH));
    KEXPECT_TRUE(wki_vfs_directory_grant_request_valid(ADOPT, LENGTH));
    for (auto request : {ISSUE, ADOPT}) {
        for (size_t length : {size_t{0}, LENGTH - 1, LENGTH + 1}) KEXPECT_FALSE(wki_vfs_directory_grant_request_valid(request, length));
        request.pin_token = 0;
        KEXPECT_FALSE(wki_vfs_directory_grant_request_valid(request, LENGTH));
    }
    for (unsigned field = 0; field < 10; ++field) {
        auto request = ISSUE;
        switch (field) {
            case 0:
                request.version = 0;
                break;
            case 1:
                request.operation = static_cast<VfsDirectoryGrantOperation>(0);
                break;
            case 2:
                request.policy = 4;
                break;
            case 3:
                request.target_node = 0;
                break;
            case 4:
                request.directory_fd = -1;
                break;
            case 5:
                request.target_boot_epoch = 0;
                break;
            case 6:
                request.source_pin_token = 0;
                break;
            case 7:
                request.source_pin_token = request.pin_token;
                break;
            case 8:
                request.submission_id = 0;
                break;
            default:
                request.grant = GRANT;
                break;
        }
        KEXPECT_FALSE(wki_vfs_directory_grant_request_valid(request, LENGTH));
    }
    for (unsigned field = 0; field < 7; ++field) {
        auto request = ADOPT;
        switch (field) {
            case 0:
                request.policy = 1;
                break;
            case 1:
                request.target_node = 3;
                break;
            case 2:
                request.directory_fd = 0;
                break;
            case 3:
                request.target_boot_epoch = 8;
                break;
            case 4:
                request.source_pin_token = 1;
                break;
            case 5:
                request.submission_id = 10;
                break;
            default:
                request.grant = {};
                break;
        }
        KEXPECT_FALSE(wki_vfs_directory_grant_request_valid(request, LENGTH));
    }
    constexpr std::array<uint8_t, 4> PATH{'/', 'd', 'i', 'r'};
    VfsDirectoryGrantResponse response{.directory = {.fd = 43, .token = 12, .path_length = 4}, .grant = GRANT};
    constexpr size_t RESPONSE_LENGTH = sizeof(response) + PATH.size();
    KEXPECT_TRUE(wki_vfs_directory_grant_response_valid(response, 12, PATH.data(), RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_response_valid(response, 13, PATH.data(), RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_response_valid(response, 12, PATH.data(), RESPONSE_LENGTH + 1));
    KEXPECT_FALSE(wki_vfs_directory_grant_response_valid(response, 12, nullptr, RESPONSE_LENGTH));
    response.directory.reserved3 = 1;
    KEXPECT_FALSE(wki_vfs_directory_grant_response_valid(response, 12, PATH.data(), RESPONSE_LENGTH));
    KEXPECT_TRUE(wki_vfs_directory_grant_reply_size_valid(0, RESPONSE_LENGTH, RESPONSE_LENGTH));
    KEXPECT_TRUE(wki_vfs_directory_grant_reply_size_valid(-1, 0, RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_reply_size_valid(-1, RESPONSE_LENGTH, RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_reply_size_valid(1, 0, RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_reply_size_valid(0, sizeof(response), RESPONSE_LENGTH));
    KEXPECT_FALSE(wki_vfs_directory_grant_reply_size_valid(0, RESPONSE_LENGTH, RESPONSE_LENGTH - 1));
    std::array<uint8_t, sizeof(DevOpReqPayload) + LENGTH> bytes{};
    DevOpReqPayload const HEADER{.op_id = OP_VFS_DIRECTORY_GRANT, .data_len = LENGTH};
    std::memcpy(bytes.data(), &HEADER, sizeof(HEADER));
    constexpr auto REQUIRED = wki_policy_bits(WkiPolicy::VFS_READ) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    for (auto const& request : {ISSUE, ADOPT}) {
        std::memcpy(bytes.data() + sizeof(HEADER), &request, sizeof(request));
        KEXPECT_EQ(auth_policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), REQUIRED);
        KEXPECT_EQ(auth_policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size() - 1), auth_policy::INVALID);
    }
    bytes[sizeof(HEADER)] = 0;
    KEXPECT_EQ(auth_policy::required_permissions(MsgType::DEV_OP_REQ, bytes.data(), bytes.size()), auth_policy::INVALID);
}
