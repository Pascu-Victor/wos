#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv6.hpp>

namespace ker::net::proto {

// ICMPv6 header
struct ICMPv6Header {
    uint8_t type{};
    uint8_t code{};
    uint16_t checksum{};
} __attribute__((packed));
static_assert(sizeof(ICMPv6Header) == 4);

// ICMPv6 types
constexpr uint8_t ICMPV6_ECHO_REQUEST = 128;
constexpr uint8_t ICMPV6_ECHO_REPLY = 129;
constexpr uint8_t ICMPV6_DEST_UNREACH = 1;
constexpr uint8_t ICMPV6_PACKET_TOO_BIG = 2;
constexpr uint8_t ICMPV6_TIME_EXCEEDED = 3;
constexpr uint8_t ICMPV6_PARAM_PROBLEM = 4;

// NDP message types (ICMPv6 subtypes)
constexpr uint8_t ICMPV6_ROUTER_SOLICIT = 133;
constexpr uint8_t ICMPV6_ROUTER_ADVERT = 134;
constexpr uint8_t ICMPV6_NEIGHBOR_SOLICIT = 135;
constexpr uint8_t ICMPV6_NEIGHBOR_ADVERT = 136;

// ICMPv6 echo (ping) header (after the base ICMPv6 header)
struct ICMPv6Echo {
    uint16_t identifier{};
    uint16_t sequence{};
} __attribute__((packed));
static_assert(sizeof(ICMPv6Echo) == 4);

// RX: called from ipv6.cpp for next_header == 58
void icmpv6_rx(NetDevice* dev, PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst);

}  // namespace ker::net::proto
