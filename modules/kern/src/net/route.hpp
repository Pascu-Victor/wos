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
    // Set before publication and immutable thereafter. Current membership is
    // internal to the route registry, not represented by this snapshot bit.
    bool valid{};
};

// Longest prefix match routing lookup. Returned entries are immutable snapshots
// whose addresses and fields remain stable after registry deletion.
auto route_lookup(proto::IPv4Address dst) -> RouteEntry*;

// Add/remove routes
auto route_add(proto::IPv4Address dest, proto::IPv4Address netmask, proto::IPv4Address gateway, uint32_t metric, NetDevice* dev) -> int;
auto route_del(proto::IPv4Address dest, proto::IPv4Address netmask) -> int;
auto route_del_for_dev(NetDevice* dev) -> size_t;

void route_init();

}  // namespace ker::net
