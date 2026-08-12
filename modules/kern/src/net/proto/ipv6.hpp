#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

// IPv6 header (40 bytes, fixed)
struct IPv6Header {
    uint32_t version_tc_flow{};  // 4-bit version, 8-bit traffic class, 20-bit flow label
    uint16_t payload_length{};   // network order
    uint8_t next_header{};       // protocol (same as IPv4 protocol field)
    uint8_t hop_limit{};
    IPv6Address src;
    IPv6Address dst;
} __attribute__((packed));

constexpr size_t IPV6_HLEN = 40;
static_assert(sizeof(IPv6Header) == IPV6_HLEN);

// IPv6 next-header / protocol numbers
constexpr uint8_t IPV6_PROTO_ICMPV6 = 58;
constexpr uint8_t IPV6_PROTO_TCP = 6;
constexpr uint8_t IPV6_PROTO_UDP = 17;
constexpr uint8_t IPV6_PROTO_HOP_BY_HOP = 0;
constexpr uint8_t IPV6_PROTO_ROUTING = 43;
constexpr uint8_t IPV6_PROTO_FRAGMENT = 44;
constexpr uint8_t IPV6_PROTO_ESP = 50;
constexpr uint8_t IPV6_PROTO_AH = 51;
constexpr uint8_t IPV6_PROTO_NO_NEXT = 59;
constexpr uint8_t IPV6_PROTO_DEST_OPTIONS = 60;

constexpr size_t IPV6_MAX_EXTENSION_HEADERS = 8;
constexpr size_t IPV6_MAX_EXTENSION_BYTES = 256;

enum class IPv6ParseError : uint8_t {
    NONE,
    TRUNCATED,
    BAD_VERSION,
    BAD_PAYLOAD_LENGTH,
    TOO_MANY_HEADERS,
    TOO_MANY_EXTENSION_BYTES,
    BAD_EXTENSION_ORDER,
    UNSUPPORTED_EXTENSION,
    FRAGMENT_UNSUPPORTED,
    NO_NEXT_HEADER,
    UNSUPPORTED_PROTOCOL,
};

struct IPv6RxInfo {
    IPv6Address src{};
    IPv6Address dst{};
    uint8_t next_header{};
    uint8_t hop_limit{};
    size_t upper_layer_offset{};
    size_t upper_layer_length{};
};

struct IPv6OutputRoute {
    IPv6Address source{};
    IPv6Address next_hop{};
    NetDeviceRef device{};
    uint32_t mtu{};
};

[[nodiscard]] auto ipv6_parse(const uint8_t* data, size_t length, IPv6RxInfo& out) -> IPv6ParseError;
// Parses a standards-permitted truncated ICMPv6 quote. The complete fixed and
// extension headers must be present, while upper-layer bytes may be shorter
// than the invoking packet's declared payload length.
[[nodiscard]] auto ipv6_parse_quoted(const uint8_t* data, size_t length, IPv6RxInfo& out) -> IPv6ParseError;
auto ipv6_route_resolve(const IPv6Address& dst, const IPv6Address* requested_src, uint32_t bound_ifindex, IPv6OutputRoute& out) -> int;
auto ipv6_tx_routed(PacketBuffer* pkt, IPv6OutputRoute&& route, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit) -> int;
auto ipv6_tx_on_dev(PacketBuffer* pkt, NetDevice* dev, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header,
                    uint8_t hop_limit) -> int;

// Solicited-node multicast prefix: ff02::1:ff00:0/104
// For NDP neighbor solicitation
extern const std::array<uint8_t, 13> IPV6_SOLICITED_NODE_PREFIX;

// All-nodes multicast: ff02::1
extern const IPv6Address IPV6_ALL_NODES_MULTICAST;
// All-routers link-local multicast: ff02::2.
extern const IPv6Address IPV6_ALL_ROUTERS_MULTICAST;

// Unspecified address: ::
extern const IPv6Address IPV6_UNSPECIFIED;

// Link-local prefix: fe80::/10
extern const std::array<uint8_t, 2> IPV6_LINK_LOCAL_PREFIX;

// RX: called from ethernet.cpp on ETH_TYPE_IPV6
void ipv6_rx(NetDevice* dev, PacketBuffer* pkt);

// TX: send an IPv6 packet
// next_header: protocol (TCP=6, UDP=17, ICMPv6=58)
void ipv6_tx(PacketBuffer* pkt, const IPv6Address& src, const IPv6Address& dst, uint8_t next_header, uint8_t hop_limit, NetDevice* dev);

// Generate link-local address from MAC (EUI-64)
auto ipv6_make_link_local(const MacAddress& mac) -> IPv6Address;

// Generate solicited-node multicast address from unicast address
auto ipv6_make_solicited_node(const IPv6Address& addr) -> IPv6Address;

// Convert IPv6 multicast address to Ethernet multicast MAC
auto ipv6_multicast_to_mac(const IPv6Address& ipv6_mcast) -> MacAddress;

}  // namespace ker::net::proto
