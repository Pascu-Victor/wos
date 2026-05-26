#pragma once

#include <cstddef>
#include <cstdint>
#include <net/address.hpp>

namespace ker::net {

// Compute one's complement checksum over a buffer (used for IP, ICMP)
auto checksum_compute(const void* data, size_t len) -> uint16_t;

// Compute TCP/UDP pseudo-header checksum (IPv4)
// proto: IPPROTO_TCP (6) or IPPROTO_UDP (17)
auto checksum_pseudo_ipv4(proto::IPv4Address src, proto::IPv4Address dst, uint8_t proto, uint16_t len, const void* data, size_t data_len)
    -> uint16_t;

// Convenience: pseudo-header checksum where segment length == data length
// Used by TCP code where the packet buffer IS the full segment
inline auto pseudo_header_checksum(proto::IPv4Address src, proto::IPv4Address dst, uint8_t proto, const void* data, size_t data_len)
    -> uint16_t {
    return checksum_pseudo_ipv4(src, dst, proto, static_cast<uint16_t>(data_len), data, data_len);
}

// Compute TCP/UDP/ICMPv6 pseudo-header checksum (IPv6)
auto checksum_pseudo_ipv6(const proto::IPv6Address& src, const proto::IPv6Address& dst, uint8_t next_header, uint32_t payload_len,
                          const void* data, size_t data_len) -> uint16_t;

}  // namespace ker::net
