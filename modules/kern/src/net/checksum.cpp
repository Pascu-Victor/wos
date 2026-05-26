#include "checksum.hpp"

#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <span>

namespace ker::net {

namespace {
auto add_little_endian_words(uint32_t sum, std::span<const uint8_t> bytes) -> uint32_t {
    uint16_t word = 0;
    bool have_low_byte = false;
    for (uint8_t const BYTE : bytes) {
        if (!have_low_byte) {
            word = BYTE;
            have_low_byte = true;
            continue;
        }

        sum += word | (static_cast<uint16_t>(BYTE) << 8U);
        have_low_byte = false;
    }

    if (have_low_byte) {
        sum += word;
    }
    return sum;
}

auto add_ipv6_address_words(uint32_t sum, const proto::IPv6Address& addr) -> uint32_t {
    for (size_t i = 0; i < proto::IPv6Address::SIZE_BYTES; i += 2) {
        sum += (static_cast<uint16_t>(addr.bytes.at(i)) << 8U) | addr.bytes.at(i + 1);
    }
    return sum;
}

auto fold_checksum(uint32_t sum) -> uint16_t {
    while ((sum >> 16) != 0U) {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    return static_cast<uint16_t>(~sum);
}
}  // namespace

auto checksum_compute(const void* data, size_t len) -> uint16_t {
    const auto* bytes = static_cast<const uint8_t*>(data);
    return fold_checksum(add_little_endian_words(0, std::span<const uint8_t>{bytes, len}));
}

auto checksum_pseudo_ipv4(proto::IPv4Address src, proto::IPv4Address dst, uint8_t proto, uint16_t len, const void* data, size_t data_len)
    -> uint16_t {
    uint32_t sum = 0;

    // Pseudo header: src(32) + dst(32) + zero(8) + proto(8) + length(16)
    // All in network byte order
    uint32_t const SRC_N = src.to_network_order();
    uint32_t const DST_N = dst.to_network_order();
    sum += (SRC_N >> 16) & 0xFFFF;
    sum += SRC_N & 0xFFFF;
    sum += (DST_N >> 16) & 0xFFFF;
    sum += DST_N & 0xFFFF;
    sum += htons(proto);
    sum += htons(len);

    const auto* bytes = static_cast<const uint8_t*>(data);
    sum = add_little_endian_words(sum, std::span<const uint8_t>{bytes, data_len});

    return fold_checksum(sum);
}

auto checksum_pseudo_ipv6(const proto::IPv6Address& src, const proto::IPv6Address& dst, uint8_t next_header, uint32_t payload_len,
                          const void* data, size_t data_len) -> uint16_t {
    uint32_t sum = 0;

    // IPv6 pseudo-header: src(128) + dst(128) + payload_len(32) + zero(24) + next_header(8)
    sum = add_ipv6_address_words(sum, src);
    sum = add_ipv6_address_words(sum, dst);

    uint32_t const LEN_N = htonl(payload_len);
    sum += (LEN_N >> 16) & 0xFFFF;
    sum += LEN_N & 0xFFFF;
    sum += htons(static_cast<uint16_t>(next_header));

    const auto* bytes = static_cast<const uint8_t*>(data);
    sum = add_little_endian_words(sum, std::span<const uint8_t>{bytes, data_len});

    return fold_checksum(sum);
}

}  // namespace ker::net
