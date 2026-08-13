#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <vfs/fs/xfs/xfs_log_codec.hpp>

using namespace ker::vfs::xfs;

TEST(XfsLogCodec, RoundTripsExplicitBigEndianBufferItem) {
    constexpr uint64_t LSN = 0x0000000700000010ULL;
    constexpr uint64_t TARGET_BLOCK = 0x0102030405060708ULL;
    constexpr uint32_t TARGET_BLOCKS = 3;
    constexpr uint32_t DATA_OFFSET = 17;
    constexpr std::array<uint8_t, 5> PAYLOAD{0x11, 0x22, 0x33, 0x44, 0x55};
    constexpr size_t BODY_BYTES = sizeof(WosLogBodyHeader) + sizeof(WosLogBufferItemHeader) + PAYLOAD.size();

    std::array<uint8_t, BODY_BYTES> body{};
    ASSERT_EQ(wos_log_body_begin(body.data(), body.size(), 0, 1, LSN, body.size()), 0);
    size_t offset = sizeof(WosLogBodyHeader);
    ASSERT_EQ(wos_log_body_append_buffer(body.data(), body.size(), &offset, TARGET_BLOCK, TARGET_BLOCKS, DATA_OFFSET, PAYLOAD.data(),
                                         PAYLOAD.size()),
              0);
    ASSERT_EQ(wos_log_body_finalize(body.data(), body.size(), offset), 0);

    EXPECT_EQ(body[0], 0x57);
    EXPECT_EQ(body[1], 0x4A);
    EXPECT_EQ(body[2], 0x4E);
    EXPECT_EQ(body[3], 0x4C);
    EXPECT_EQ(body[4], 0x00);
    EXPECT_EQ(body[5], 0x01);

    WosLogBodyCursor cursor{};
    ASSERT_EQ(wos_log_body_cursor_init(body.data(), body.size(), LSN, &cursor), 0);
    WosLogBufferItemView item{};
    ASSERT_EQ(wos_log_body_cursor_next(&cursor, &item), 0);
    EXPECT_EQ(item.target_block, TARGET_BLOCK);
    EXPECT_EQ(item.target_blocks, TARGET_BLOCKS);
    EXPECT_EQ(item.data_offset, DATA_OFFSET);
    ASSERT_EQ(item.data_bytes, PAYLOAD.size());
    EXPECT_EQ(std::memcmp(item.data, PAYLOAD.data(), PAYLOAD.size()), 0);
    EXPECT_EQ(wos_log_body_cursor_finish(&cursor), 0);
}

TEST(XfsLogCodec, RejectsIntegrityCountAndConsumptionMismatches) {
    constexpr uint64_t LSN = 0x0000000200000008ULL;
    constexpr std::array<uint8_t, 4> PAYLOAD{1, 2, 3, 4};
    constexpr size_t BODY_BYTES = sizeof(WosLogBodyHeader) + sizeof(WosLogBufferItemHeader) + PAYLOAD.size();
    std::array<uint8_t, BODY_BYTES> body{};

    ASSERT_EQ(wos_log_body_begin(body.data(), body.size(), 0, 2, LSN, body.size()), 0);
    size_t offset = sizeof(WosLogBodyHeader);
    ASSERT_EQ(wos_log_body_append_buffer(body.data(), body.size(), &offset, 40, 2, 8, PAYLOAD.data(), PAYLOAD.size()), 0);
    ASSERT_EQ(wos_log_body_finalize(body.data(), body.size(), offset), 0);

    WosLogBodyCursor cursor{};
    ASSERT_EQ(wos_log_body_cursor_init(body.data(), body.size(), LSN, &cursor), 0);
    WosLogBufferItemView item{};
    ASSERT_EQ(wos_log_body_cursor_next(&cursor, &item), 0);
    EXPECT_EQ(wos_log_body_cursor_next(&cursor, &item), -EUCLEAN);
    EXPECT_EQ(wos_log_body_cursor_finish(&cursor), -EUCLEAN);

    body.back() ^= 0x80;
    EXPECT_EQ(wos_log_body_cursor_init(body.data(), body.size(), LSN, &cursor), -EUCLEAN);
    EXPECT_EQ(wos_log_body_begin(body.data(), body.size(), WOS_XLOG_BODY_FLAG_CLEAN, 1, LSN, body.size()), -EINVAL);
}

TEST(XfsLogCodec, OuterChecksumCoversFixedPrefixAndExactPayload) {
    std::array<uint8_t, sizeof(WosLogBodyHeader)> body{};
    ASSERT_EQ(wos_log_body_begin(body.data(), body.size(), WOS_XLOG_BODY_FLAG_CLEAN, 0, 1ULL << 32, body.size()), 0);
    ASSERT_EQ(wos_log_body_finalize(body.data(), body.size(), body.size()), 0);

    XlogRecHeader header{};
    header.h_magicno = Be32::from_cpu(XLOG_HEADER_MAGIC_NUM);
    header.h_cycle = Be32::from_cpu(1);
    header.h_version = Be32::from_cpu(XLOG_VERSION_2);
    header.h_len = Be32::from_cpu(body.size());
    header.h_lsn = Be64::from_cpu(1ULL << 32);
    header.h_tail_lsn = Be64::from_cpu(1ULL << 32);
    header.h_fmt = Be32::from_cpu(XLOG_FMT_LINUX_LE);
    header.h_size = Be32::from_cpu(4096);

    uint32_t const BASE = wos_log_record_crc(header, body.data(), body.size());
    ASSERT_NE(BASE, 0U);
    header.h_crc = 0xDEADBEEF;
    EXPECT_EQ(wos_log_record_crc(header, body.data(), body.size()), BASE);
    header.h_pad0 = 1;
    EXPECT_NE(wos_log_record_crc(header, body.data(), body.size()), BASE);
    header.h_pad0 = 0;
    body.back() ^= 1;
    EXPECT_NE(wos_log_record_crc(header, body.data(), body.size()), BASE);
}
