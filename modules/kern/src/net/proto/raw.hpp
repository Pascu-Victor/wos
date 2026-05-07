#pragma once

#include <cstdint>
#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

// Get raw socket protocol operations
auto get_raw_proto_ops() -> SocketProtoOps*;

// Deliver a raw IP packet to matching raw sockets
void raw_deliver(PacketBuffer* pkt, uint8_t protocol, uint32_t src_ip, uint32_t dst_ip, uint8_t ttl);

}  // namespace ker::net::proto
