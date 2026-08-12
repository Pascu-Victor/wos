#pragma once

#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv6.hpp>

namespace ker::net::proto {

struct ICMPv6Header {
    uint8_t type{};
    uint8_t code{};
    uint16_t checksum{};
} __attribute__((packed));
static_assert(sizeof(ICMPv6Header) == 4);

constexpr uint8_t ICMPV6_DEST_UNREACH = 1;
constexpr uint8_t ICMPV6_PACKET_TOO_BIG = 2;
constexpr uint8_t ICMPV6_TIME_EXCEEDED = 3;
constexpr uint8_t ICMPV6_PARAM_PROBLEM = 4;
constexpr uint8_t ICMPV6_ECHO_REQUEST = 128;
constexpr uint8_t ICMPV6_ECHO_REPLY = 129;
constexpr uint8_t ICMPV6_ROUTER_SOLICIT = 133;
constexpr uint8_t ICMPV6_ROUTER_ADVERT = 134;
constexpr uint8_t ICMPV6_NEIGHBOR_SOLICIT = 135;
constexpr uint8_t ICMPV6_NEIGHBOR_ADVERT = 136;

constexpr uint8_t ICMPV6_DEST_UNREACH_NO_ROUTE = 0;
constexpr uint8_t ICMPV6_DEST_UNREACH_PORT = 4;

struct ICMPv6Echo {
    uint16_t identifier{};
    uint16_t sequence{};
} __attribute__((packed));
static_assert(sizeof(ICMPv6Echo) == 4);

struct ICMPv6ErrorBody {
    uint32_t parameter{};
} __attribute__((packed));

struct ICMPv6RouterSolicitation {
    uint32_t reserved{};
} __attribute__((packed));

struct ICMPv6RouterAdvertisement {
    uint8_t current_hop_limit{};
    uint8_t flags{};
    uint16_t router_lifetime{};
    uint32_t reachable_time{};
    uint32_t retrans_timer{};
} __attribute__((packed));

enum class ICMPv6ValidationError : uint8_t {
    NONE,
    TRUNCATED,
    BAD_CHECKSUM,
    BAD_CODE,
    BAD_HOP_LIMIT,
    BAD_SOURCE,
    BAD_DESTINATION,
    BAD_OPTIONS,
    BAD_TYPE,
};

[[nodiscard]] auto icmpv6_validate(const PacketBuffer* pkt, const IPv6RxInfo& info) -> ICMPv6ValidationError;
void icmpv6_rx(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info);

// pkt currently starts at the UDP header after ipv6_rx pulled the complete
// bounded IPv6/extension prefix. This helper reconstructs a base IPv6 quote,
// consumes pkt exactly once, and rate-limits generated errors per interface.
void icmpv6_send_port_unreachable(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info);

}  // namespace ker::net::proto
