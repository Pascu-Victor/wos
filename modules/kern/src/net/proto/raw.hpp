#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

// Get raw socket protocol operations
auto get_raw_proto_ops() -> SocketProtoOps*;

// Deliver a raw IP packet to matching raw sockets
void raw_deliver(PacketBuffer* pkt, uint8_t protocol, IPv4Address src_ip, IPv4Address dst_ip, uint8_t ttl);

}  // namespace ker::net::proto
