#include <cstdint>
#include <net/wki/wire.hpp>
#include <test/ktest.hpp>

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
