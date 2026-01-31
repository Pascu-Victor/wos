#pragma once

#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

// Get raw socket protocol operations
auto get_raw_proto_ops() -> SocketProtoOps*;

// Deliver a raw IP packet to matching raw sockets
void raw_deliver(PacketBuffer* pkt, uint8_t protocol);

}  // namespace ker::net::proto
