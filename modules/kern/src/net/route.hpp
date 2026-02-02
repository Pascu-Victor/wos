#pragma once

#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>

namespace ker::net {

constexpr size_t MAX_ROUTES = 32;

struct RouteEntry {
    uint32_t dest;
    uint32_t netmask;
    uint32_t gateway;
    uint32_t metric;
    NetDevice* dev;
    bool valid;
};

// Longest prefix match routing lookup
auto route_lookup(uint32_t dst) -> RouteEntry*;

// Add/remove routes
auto route_add(uint32_t dest, uint32_t netmask, uint32_t gateway, uint32_t metric, NetDevice* dev) -> int;
auto route_del(uint32_t dest, uint32_t netmask) -> int;

void route_init();

}  // namespace ker::net
