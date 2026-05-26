#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

struct UdpHeader {
    uint16_t src_port{};  // network order
    uint16_t dst_port{};  // network order
    uint16_t length{};    // network order (header + payload)
    uint16_t checksum{};  // network order
} __attribute__((packed));
static_assert(sizeof(UdpHeader) == 8);

void udp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip);

auto get_udp_proto_ops() -> SocketProtoOps*;

}  // namespace ker::net::proto
