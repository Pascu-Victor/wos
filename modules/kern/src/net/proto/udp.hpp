#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>

namespace ker::net::proto {

struct IPv6RxInfo;

struct UdpHeader {
    uint16_t src_port{};  // network order
    uint16_t dst_port{};  // network order
    uint16_t length{};    // network order (header + payload)
    uint16_t checksum{};  // network order
} __attribute__((packed));
static_assert(sizeof(UdpHeader) == 8);

void udp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip);

enum class UdpRxV6Result : uint8_t {
    Consumed,
    NoPort,
};

// NoPort leaves pkt untouched and owned by the caller. Every other outcome is
// reported as Consumed and consumes pkt exactly once.
auto udp_rx_v6(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) -> UdpRxV6Result;

void udp_error_v6(const IPv6Address& local_addr, uint16_t local_port, const IPv6Address& remote_addr, uint16_t remote_port, uint8_t type,
                  uint8_t code, uint32_t mtu, uint32_t scope_id);

auto get_udp_proto_ops() -> SocketProtoOps*;

}  // namespace ker::net::proto
