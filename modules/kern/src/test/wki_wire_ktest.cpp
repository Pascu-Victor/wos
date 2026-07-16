#include <array>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <test/ktest.hpp>

KTEST(WkiWire, VfsMultiRdmaCapabilityAndAuxFlagPreserveLayouts) {
    using namespace ker::net::wki;

    KEXPECT_EQ(WKI_CAP_VFS_MULTI_RDMA_LANES, static_cast<uint16_t>(0x0008));
    KEXPECT_EQ(DEV_ATTACH_VFS_AUX_LANE, static_cast<uint8_t>(0x80));
    KEXPECT_EQ(
        static_cast<uint8_t>(DEV_ATTACH_VFS_AUX_LANE & (DEV_ATTACH_MODE_KIND_MASK | DEV_ATTACH_ACCESS_MASK | DEV_ATTACH_DISABLE_RDMA)),
        static_cast<uint8_t>(0));
    KEXPECT_EQ(sizeof(HelloPayload), static_cast<size_t>(96));
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
