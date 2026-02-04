#pragma once

#include <array>
#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

// IPv6 header (40 bytes, fixed)
struct IPv6Header {
    uint32_t version_tc_flow;  // 4-bit version, 8-bit traffic class, 20-bit flow label
    uint16_t payload_length;   // network order
    uint8_t next_header;       // protocol (same as IPv4 protocol field)
    uint8_t hop_limit;
    std::array<uint8_t, 16> src;
    std::array<uint8_t, 16> dst;
} __attribute__((packed));

constexpr size_t IPV6_HLEN = 40;

// IPv6 next-header / protocol numbers
constexpr uint8_t IPV6_PROTO_ICMPV6 = 58;
constexpr uint8_t IPV6_PROTO_TCP = 6;
constexpr uint8_t IPV6_PROTO_UDP = 17;

// Solicited-node multicast prefix: ff02::1:ff00:0/104
// For NDP neighbor solicitation
extern const std::array<uint8_t, 13> IPV6_SOLICITED_NODE_PREFIX;

// All-nodes multicast: ff02::1
extern const std::array<uint8_t, 16> IPV6_ALL_NODES_MULTICAST;

// Unspecified address: ::
extern const std::array<uint8_t, 16> IPV6_UNSPECIFIED;

// Link-local prefix: fe80::/10
extern const std::array<uint8_t, 2> IPV6_LINK_LOCAL_PREFIX;

// RX: called from ethernet.cpp on ETH_TYPE_IPV6
void ipv6_rx(NetDevice* dev, PacketBuffer* pkt);

// TX: send an IPv6 packet
// next_header: protocol (TCP=6, UDP=17, ICMPv6=58)
void ipv6_tx(PacketBuffer* pkt, const std::array<uint8_t, 16>& src, const std::array<uint8_t, 16>& dst, uint8_t next_header,
             uint8_t hop_limit, NetDevice* dev);

// Generate link-local address from MAC (EUI-64)
void ipv6_make_link_local(std::array<uint8_t, 16>& out, const std::array<uint8_t, 6>& mac);

// Generate solicited-node multicast address from unicast address
void ipv6_make_solicited_node(std::array<uint8_t, 16>& out, const std::array<uint8_t, 16>& addr);

// Convert IPv6 multicast address to Ethernet multicast MAC
void ipv6_multicast_to_mac(std::array<uint8_t, 6>& out_mac, const std::array<uint8_t, 16>& ipv6_mcast);

}  // namespace ker::net::proto
