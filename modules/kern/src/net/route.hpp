#pragma once

#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>

namespace ker::net {

constexpr size_t MAX_ROUTES = 32;

struct RouteEntry {
    proto::IPv4Address dest;
    proto::IPv4Address netmask;
    proto::IPv4Address gateway;
    uint32_t metric{};
    NetDevice* dev{};
    bool valid{};
};

// Longest prefix match routing lookup
auto route_lookup(proto::IPv4Address dst) -> RouteEntry*;

// Add/remove routes
auto route_add(proto::IPv4Address dest, proto::IPv4Address netmask, proto::IPv4Address gateway, uint32_t metric, NetDevice* dev) -> int;
auto route_del(proto::IPv4Address dest, proto::IPv4Address netmask) -> int;

void route_init();

}  // namespace ker::net
