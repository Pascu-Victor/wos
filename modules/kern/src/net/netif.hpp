#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/proto/ipv6.hpp>

namespace ker::net {

constexpr size_t MAX_ADDRS_PER_IF = 8;
constexpr size_t MAX_NET_INTERFACES = MAX_NET_DEVICES;

struct IPv4Addr {
    proto::IPv4Address addr;
    proto::IPv4Address netmask;
};

struct IPv6Addr {
    proto::IPv6Address addr;
    uint8_t prefix_len{};
};

struct NetInterface {
    NetDevice* dev = nullptr;
    std::array<IPv4Addr, MAX_ADDRS_PER_IF> ipv4_addrs = {};
    size_t ipv4_addr_count = 0;
    std::array<IPv6Addr, MAX_ADDRS_PER_IF> ipv6_addrs = {};
    size_t ipv6_addr_count = 0;
};

// Get or create interface config for a device. Returned interfaces remain at a
// permanent address after registry deletion and are never reused.
auto netif_get(NetDevice* dev) -> NetInterface*;
// Find existing interface config without allocating or publishing a new row.
// Packet/RX and read-only inspection paths must use this form.
auto netif_find_by_dev(NetDevice* dev) -> NetInterface*;
auto netif_del_for_dev(NetDevice* dev) -> bool;

// Add addresses
auto netif_add_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int;
auto netif_set_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask, bool replace) -> int;
auto netif_del_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int;
auto netif_add_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix) -> int;

// Lookup: find interface that owns a given IPv4 address
auto netif_find_by_ipv4(proto::IPv4Address addr) -> NetInterface*;

// Lookup: find interface that owns a given IPv6 address
auto netif_find_by_ipv6(const proto::IPv6Address& addr) -> NetInterface*;

}  // namespace ker::net
