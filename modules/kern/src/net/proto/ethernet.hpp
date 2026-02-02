#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

constexpr uint16_t ETH_TYPE_IPV4 = 0x0800;
constexpr uint16_t ETH_TYPE_ARP = 0x0806;
constexpr uint16_t ETH_TYPE_IPV6 = 0x86DD;

constexpr size_t ETH_HLEN = 14;
constexpr size_t ETH_ALEN = 6;

struct EthernetHeader {
    uint8_t dst[ETH_ALEN];
    uint8_t src[ETH_ALEN];
    uint16_t ethertype;  // network byte order
} __attribute__((packed));

// RX entry point: demux by ethertype
void eth_rx(NetDevice* dev, PacketBuffer* pkt);

// TX: prepend ethernet header and transmit
auto eth_tx(NetDevice* dev, PacketBuffer* pkt, const uint8_t* dst_mac, uint16_t ethertype) -> int;

// Broadcast MAC address
extern const uint8_t ETH_BROADCAST[ETH_ALEN];

}  // namespace ker::net::proto
