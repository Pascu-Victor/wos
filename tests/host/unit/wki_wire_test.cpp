// Unit tests for WKI wire protocol definitions (wire.hpp).
// Tests header construction, field extraction, sequence arithmetic,
// and payload layout assertions.

#include <gtest/gtest.h>
#include <net/wki/wire.hpp>
#include <cstring>

using namespace ker::net::wki;

// ---------------------------------------------------------------------------
// Header size and layout
// ---------------------------------------------------------------------------

TEST(WkiWire, HeaderIs32Bytes) {
    EXPECT_EQ(sizeof(WkiHeader), 32u);
}

TEST(WkiWire, HelloPayloadIs96Bytes) {
    EXPECT_EQ(sizeof(HelloPayload), 96u);
}

TEST(WkiWire, HeartbeatPayloadIs16Bytes) {
    EXPECT_EQ(sizeof(HeartbeatPayload), 16u);
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

TEST(WkiWire, CurrentVersionIs1) {
    uint8_t vf = wki_version_flags(WKI_VERSION, 0);
    EXPECT_EQ(wki_version(vf), 1);
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
    EXPECT_TRUE(seq_between(3, 3, 10));   // inclusive low
    EXPECT_FALSE(seq_between(10, 3, 10)); // exclusive high
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
    EXPECT_EQ(bytes[0], 0x00); // '\0'
    EXPECT_EQ(bytes[1], 0x49); // 'I'
    EXPECT_EQ(bytes[2], 0x4B); // 'K'
    EXPECT_EQ(bytes[3], 0x57); // 'W'
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
    EXPECT_EQ(static_cast<uint8_t>(MsgType::ZONE_CREATE_REQ), 0x20);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::EVENT_PUBLISH), 0x32);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::DEV_ATTACH_REQ), 0x40);
    EXPECT_EQ(static_cast<uint8_t>(MsgType::TASK_SUBMIT), 0x50);
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
