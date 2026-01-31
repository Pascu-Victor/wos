#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

constexpr uint8_t ICMP_ECHO_REPLY = 0;
constexpr uint8_t ICMP_DEST_UNREACHABLE = 3;
constexpr uint8_t ICMP_ECHO_REQUEST = 8;

struct IcmpHeader {
    uint8_t type;
    uint8_t code;
    uint16_t checksum;   // network order
    uint16_t id;         // network order
    uint16_t sequence;   // network order
} __attribute__((packed));

void icmp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip);

}  // namespace ker::net::proto
