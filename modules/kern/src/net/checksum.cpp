#include "checksum.hpp"

#include <net/endian.hpp>

namespace ker::net {

namespace {
auto fold_checksum(uint32_t sum) -> uint16_t {
    while (sum >> 16) {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    return static_cast<uint16_t>(~sum);
}
}  // namespace

auto checksum_compute(const void* data, size_t len) -> uint16_t {
    const auto* buf = static_cast<const uint8_t*>(data);
    uint32_t sum = 0;

    // Sum 16-bit words
    while (len > 1) {
        uint16_t word = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
        sum += word;
        buf += 2;
        len -= 2;
    }

    // Handle odd byte
    if (len == 1) {
        sum += buf[0];
    }

    return fold_checksum(sum);
}

auto checksum_pseudo_ipv4(uint32_t src, uint32_t dst, uint8_t proto, uint16_t len, const void* data, size_t data_len) -> uint16_t {
    uint32_t sum = 0;

    // Pseudo header: src(32) + dst(32) + zero(8) + proto(8) + length(16)
    // All in network byte order
    uint32_t src_n = htonl(src);
    uint32_t dst_n = htonl(dst);
    sum += (src_n >> 16) & 0xFFFF;
    sum += src_n & 0xFFFF;
    sum += (dst_n >> 16) & 0xFFFF;
    sum += dst_n & 0xFFFF;
    sum += htons(proto);
    sum += htons(len);

    // Add data
    const auto* buf = static_cast<const uint8_t*>(data);
    size_t remaining = data_len;
    while (remaining > 1) {
        uint16_t word = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
        sum += word;
        buf += 2;
        remaining -= 2;
    }
    if (remaining == 1) {
        sum += buf[0];
    }

    return fold_checksum(sum);
}

auto checksum_pseudo_ipv6(const uint8_t* src, const uint8_t* dst, uint8_t next_header,
                          uint32_t payload_len, const void* data, size_t data_len) -> uint16_t {
    uint32_t sum = 0;

    // IPv6 pseudo-header: src(128) + dst(128) + payload_len(32) + zero(24) + next_header(8)
    for (int i = 0; i < 16; i += 2) {
        sum += (static_cast<uint16_t>(src[i]) << 8) | src[i + 1];
    }
    for (int i = 0; i < 16; i += 2) {
        sum += (static_cast<uint16_t>(dst[i]) << 8) | dst[i + 1];
    }

    uint32_t len_n = htonl(payload_len);
    sum += (len_n >> 16) & 0xFFFF;
    sum += len_n & 0xFFFF;
    sum += htons(static_cast<uint16_t>(next_header));

    // Add data
    const auto* buf = static_cast<const uint8_t*>(data);
    size_t remaining = data_len;
    while (remaining > 1) {
        uint16_t word = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
        sum += word;
        buf += 2;
        remaining -= 2;
    }
    if (remaining == 1) {
        sum += buf[0];
    }

    return fold_checksum(sum);
}

}  // namespace ker::net
