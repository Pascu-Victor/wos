#pragma once

#include <array>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

constexpr uint16_t ETH_TYPE_IPV4 = 0x0800;
constexpr uint16_t ETH_TYPE_ARP = 0x0806;
constexpr uint16_t ETH_TYPE_IPV6 = 0x86DD;
constexpr uint16_t ETH_TYPE_WKI = 0x88B7;
constexpr uint16_t ETH_TYPE_WKI_ROCE = 0x88B8;

constexpr size_t ETH_HLEN = 14;
constexpr size_t ETH_ALEN = 6;

struct EthernetHeader {
    MacAddress dst;
    MacAddress src;
    uint16_t ethertype{};  // network byte order
} __attribute__((packed));
static_assert(sizeof(EthernetHeader) == ETH_HLEN);

// RX entry point: demux by ethertype
void eth_rx(NetDevice* dev, PacketBuffer* pkt);

// TX: prepend ethernet header and transmit
auto eth_tx(NetDevice* dev, PacketBuffer* pkt, const MacAddress& dst_mac, uint16_t ethertype) -> int;

// Broadcast MAC address
extern const MacAddress ETH_BROADCAST;

}  // namespace ker::net::proto
