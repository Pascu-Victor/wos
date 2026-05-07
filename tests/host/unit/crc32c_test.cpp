#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <util/crc32c.hpp>

// CRC32C iSCSI standard test vector: crc32c_compute("123456789", 9) == 0xE3069283
// Reference: https://reveng.sourceforge.io/crc-catalogue/all.htm (CRC-32/ISCSI)

TEST(Crc32c, EmptyInput) {
    // No data processed: CRC32C_SEED is ~0; crc32c_final(~0) = ~~0 = 0
    uint32_t result = ker::util::crc32c_compute(nullptr, 0);
    EXPECT_EQ(result, 0x00000000u);
}

TEST(Crc32c, KnownVector) {
    // iSCSI standard CRC32C test vector
    const char data[] = "123456789";
    uint32_t result = ker::util::crc32c_compute(static_cast<const void*>(data), 9);
    EXPECT_EQ(result, 0xE3069283u);
}

TEST(Crc32c, SingleByteZero) {
    // Known value: CRC32C of a single zero byte
    const uint8_t zero = 0x00;
    uint32_t result = ker::util::crc32c_compute(static_cast<const void*>(&zero), 1);
    // Consistency check: compute twice and verify same result
    uint32_t result2 = ker::util::crc32c_compute(static_cast<const void*>(&zero), 1);
    EXPECT_EQ(result, result2);
    // Verify it differs from the empty-input result
    EXPECT_NE(result, ker::util::crc32c_compute(nullptr, 0));
}

TEST(Crc32c, Streaming) {
    // crc32c in two halves must equal one-shot over "123456789"
    const char data[] = "123456789";
    uint32_t crc = ker::util::crc32c(ker::util::CRC32C_SEED,
                                     static_cast<const void*>(data), 4);
    crc = ker::util::crc32c(crc, static_cast<const void*>(data + 4), 5);
    uint32_t streamed = ker::util::crc32c_final(crc);

    uint32_t oneshot = ker::util::crc32c_compute(static_cast<const void*>(data), 9);
    EXPECT_EQ(streamed, oneshot);
}

TEST(Crc32c, AllZeros64) {
    uint8_t buf[64] = {};
    memset(static_cast<void*>(buf), 0, sizeof(buf));
    uint32_t r1 = ker::util::crc32c_compute(static_cast<const void*>(buf), 64);
    // Must equal itself (deterministic)
    uint32_t r2 = ker::util::crc32c_compute(static_cast<const void*>(buf), 64);
    EXPECT_EQ(r1, r2);
    // Must differ from empty-input CRC
    EXPECT_NE(r1, ker::util::crc32c_compute(nullptr, 0));
}

TEST(Crc32c, AllOnes64) {
    uint8_t buf[64];
    memset(static_cast<void*>(buf), 0xFF, sizeof(buf));
    uint32_t r1 = ker::util::crc32c_compute(static_cast<const void*>(buf), 64);
    uint32_t r2 = ker::util::crc32c_compute(static_cast<const void*>(buf), 64);
    EXPECT_EQ(r1, r2);

    // AllOnes64 must differ from AllZeros64
    uint8_t zeros[64] = {};
    uint32_t rz = ker::util::crc32c_compute(static_cast<const void*>(zeros), 64);
    EXPECT_NE(r1, rz);
}
