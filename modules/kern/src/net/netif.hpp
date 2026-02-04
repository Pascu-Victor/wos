#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>

namespace ker::net {

constexpr size_t MAX_ADDRS_PER_IF = 8;
constexpr size_t MAX_NET_INTERFACES = MAX_NET_DEVICES;

struct IPv4Addr {
    uint32_t addr;
    uint32_t netmask;
};

struct IPv6Addr {
    std::array<uint8_t, 16> addr;
    uint8_t prefix_len;
};

struct NetInterface {
    NetDevice* dev = nullptr;
    std::array<IPv4Addr, MAX_ADDRS_PER_IF> ipv4_addrs = {};
    size_t ipv4_addr_count = 0;
    std::array<IPv6Addr, MAX_ADDRS_PER_IF> ipv6_addrs = {};
    size_t ipv6_addr_count = 0;
};

// Get or create interface config for a device
auto netif_get(NetDevice* dev) -> NetInterface*;

// Add addresses
auto netif_add_ipv4(NetDevice* dev, uint32_t addr, uint32_t mask) -> int;
auto netif_add_ipv6(NetDevice* dev, const std::array<uint8_t, 16>& addr, uint8_t prefix) -> int;

// Lookup: find interface that owns a given IPv4 address
auto netif_find_by_ipv4(uint32_t addr) -> NetInterface*;

// Lookup: find interface that owns a given IPv6 address
auto netif_find_by_ipv6(const std::array<uint8_t, 16>& addr) -> NetInterface*;

}  // namespace ker::net
