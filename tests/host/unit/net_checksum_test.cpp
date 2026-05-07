#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>

// One's complement checksum: checksum of correctly-checksummed data is 0x0000.
// checksum_compute({data, checksum}) == 0x0000

// Helper: verify that adding the returned checksum to the buffer makes it 0x0000
static bool is_valid_checksum(const void* data, size_t len) { return ker::net::checksum_compute(data, len) == 0x0000; }

// ---------------------------------------------------------------------------
// checksum_compute
// ---------------------------------------------------------------------------

TEST(Checksum, EmptyBuffer) {
    // Empty input: no bits to sum → result is 0xFFFF (all ones complement of 0)
    uint16_t result = ker::net::checksum_compute(nullptr, 0);
    EXPECT_EQ(result, 0xFFFFu);
}

TEST(Checksum, AllZeroWord) {
    uint16_t data = 0x0000;
    uint16_t result = ker::net::checksum_compute(&data, 2);
    EXPECT_EQ(result, 0xFFFFu);
}

TEST(Checksum, AllOnesWord) {
    uint16_t data = 0xFFFF;
    uint16_t result = ker::net::checksum_compute(&data, 2);
    EXPECT_EQ(result, 0x0000u);
}

TEST(Checksum, KnownVector4Bytes) {
    // Current kernel checksum helper consumes 16-bit words in little-endian byte order.
    // For {00 01 F2 03}: 0x0100 + 0x03F2 = 0x04F2; ~0x04F2 = 0xFB0D
    uint8_t data[] = {0x00, 0x01, 0xF2, 0x03};
    uint16_t result = ker::net::checksum_compute(data, sizeof(data));
    EXPECT_EQ(result, 0xFB0Du);
}

TEST(Checksum, OddLengthBuffer) {
    // "ABC" = 0x41 0x42 0x43
    // With little-endian 16-bit words and odd byte added as low byte:
    // Sum: 0x4241 + 0x43 = 0x4284; ~0x4284 = 0xBD7B
    uint8_t data[] = {0x41, 0x42, 0x43};
    uint16_t result = ker::net::checksum_compute(data, sizeof(data));
    EXPECT_EQ(result, 0xBD7Bu);
}

TEST(Checksum, SelfChecksum) {
    // Compute checksum, append it, verify whole block validates to 0x0000
    uint8_t data[6] = {0x01, 0x02, 0x03, 0x04, 0x00, 0x00};
    uint16_t cs = ker::net::checksum_compute(data, 4);
    memcpy(data + 4, &cs, 2);
    EXPECT_TRUE(is_valid_checksum(data, 6));
}

TEST(Checksum, IPv4HeaderVector) {
    // Minimal IPv4 header with checksum field zeroed, computed and verified.
    // Version=4, IHL=5, TOS=0, Total length=20, ID=0, Flags/Frag=0,
    // TTL=64, Proto=1 (ICMP), Checksum=0, Src=127.0.0.1, Dst=127.0.0.1
    uint8_t hdr[] = {
        0x45, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00,  // checksum = 0x0000
        0x7F, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01,
    };
    uint16_t cs = ker::net::checksum_compute(hdr, sizeof(hdr));
    memcpy(hdr + 10, &cs, 2);
    EXPECT_TRUE(is_valid_checksum(hdr, sizeof(hdr)));
}

// ---------------------------------------------------------------------------
// checksum_pseudo_ipv4 (TCP/UDP pseudo-header)
// ---------------------------------------------------------------------------

TEST(ChecksumPseudoIPv4, ZeroPayload) {
    // Any consistent checksum should not be 0 for non-trivial headers
    uint16_t result = ker::net::checksum_pseudo_ipv4(0x7F000001, 0x7F000001, 6, 0, nullptr, 0);
    // Just verify it produces a value (non-crash test)
    (void)result;
}

TEST(ChecksumPseudoIPv4, SelfValidating) {
    // Build a small TCP segment buffer, compute checksum, verify it validates.
    uint8_t segment[8] = {0x00, 0x50, 0x04, 0xD2, 0x00, 0x00, 0x00, 0x00};
    uint16_t cs = ker::net::checksum_pseudo_ipv4(0xC0A80001, 0xC0A80002, 6, 8, segment, 8);
    memcpy(segment + 6, &cs, 2);
    uint16_t verify = ker::net::checksum_pseudo_ipv4(0xC0A80001, 0xC0A80002, 6, 8, segment, 8);
    EXPECT_EQ(verify, 0x0000u);
}

// ---------------------------------------------------------------------------
// checksum_pseudo_ipv6
// ---------------------------------------------------------------------------

TEST(ChecksumPseudoIPv6, SelfValidating) {
    std::array<uint8_t, 16> src{}, dst{};
    src[15] = 1;  // ::1
    dst[15] = 2;  // ::2
    uint8_t segment[4] = {0x00, 0x50, 0x00, 0x00};
    uint16_t cs = ker::net::checksum_pseudo_ipv6(src, dst, 6, 4, segment, 4);
    memcpy(segment + 2, &cs, 2);
    uint16_t verify = ker::net::checksum_pseudo_ipv6(src, dst, 6, 4, segment, 4);
    EXPECT_EQ(verify, 0x0000u);
}
