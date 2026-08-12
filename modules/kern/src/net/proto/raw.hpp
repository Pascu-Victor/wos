#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

struct IPv6RxInfo;

[[nodiscard]] inline auto raw_delivery_endpoint_matches(const SocketEndpoint& local, const SocketEndpoint& remote, bool connected,
                                                        uint32_t bound_ifindex, uint32_t ingress_ifindex, const SocketEndpoint& source,
                                                        const SocketEndpoint& destination) -> bool {
    if (bound_ifindex != 0 && bound_ifindex != ingress_ifindex) {
        return false;
    }
    bool const MULTICAST_DESTINATION = destination.is_ipv6() && destination.ipv6_address().is_multicast();
    if (!local.is_unspecified() && !MULTICAST_DESTINATION && !socket_endpoint_address_equal(local, destination)) {
        return false;
    }
    return !connected || socket_endpoint_address_equal(remote, source);
}

// Get raw socket protocol operations
auto get_raw_proto_ops() -> SocketProtoOps*;

// Deliver a raw IP packet to matching raw sockets
void raw_deliver(PacketBuffer* pkt, uint8_t protocol, IPv4Address src_ip, IPv4Address dst_ip, uint8_t ttl);

// Consumes pkt exactly once. The queued raw payload contains a reconstructed
// base IPv6 header followed by the upper-layer bytes currently in pkt.
void raw_deliver_v6(PacketBuffer* pkt, const IPv6RxInfo& info, uint32_t scope_id);

}  // namespace ker::net::proto
