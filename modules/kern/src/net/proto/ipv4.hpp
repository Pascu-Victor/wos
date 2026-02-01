#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

constexpr uint8_t IPPROTO_ICMP = 1;
constexpr uint8_t IPPROTO_TCP = 6;
constexpr uint8_t IPPROTO_UDP = 17;

struct IPv4Header {
    uint8_t ihl_version;  // version (4 bits) | IHL (4 bits)
    uint8_t tos;
    uint16_t total_len;      // network order
    uint16_t id;             // network order
    uint16_t flags_fragoff;  // network order
    uint8_t ttl;
    uint8_t protocol;
    uint16_t checksum;  // network order
    uint32_t src_addr;  // network order
    uint32_t dst_addr;  // network order
} __attribute__((packed));

// RX entry point: validate and demux by protocol
void ipv4_rx(NetDevice* dev, PacketBuffer* pkt);

// TX: build IPv4 header, route, resolve ARP, send
auto ipv4_tx(PacketBuffer* pkt, uint32_t src, uint32_t dst, uint8_t proto, uint8_t ttl) -> int;

// Convenience: auto-select source address based on routing
auto ipv4_tx_auto(PacketBuffer* pkt, uint32_t dst, uint8_t proto) -> int;

}  // namespace ker::net::proto
