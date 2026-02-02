#pragma once

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
    uint8_t addr[16];
    uint8_t prefix_len;
};

struct NetInterface {
    NetDevice* dev = nullptr;
    IPv4Addr ipv4_addrs[MAX_ADDRS_PER_IF] = {};
    size_t ipv4_addr_count = 0;
    IPv6Addr ipv6_addrs[MAX_ADDRS_PER_IF] = {};
    size_t ipv6_addr_count = 0;
};

// Get or create interface config for a device
auto netif_get(NetDevice* dev) -> NetInterface*;

// Add addresses
auto netif_add_ipv4(NetDevice* dev, uint32_t addr, uint32_t mask) -> int;
auto netif_add_ipv6(NetDevice* dev, const uint8_t* addr, uint8_t prefix) -> int;

// Lookup: find interface that owns a given IPv4 address
auto netif_find_by_ipv4(uint32_t addr) -> NetInterface*;

// Lookup: find interface that owns a given IPv6 address
auto netif_find_by_ipv6(const uint8_t* addr) -> NetInterface*;

}  // namespace ker::net
