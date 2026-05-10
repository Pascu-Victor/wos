#include "route.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <platform/dbg/dbg.hpp>

#include "net/netdevice.hpp"

namespace ker::net {

using log = ker::mod::dbg::logger<"route">;

namespace {
std::array<RouteEntry, MAX_ROUTES> routes = {};
size_t route_count = 0;

// Count leading 1-bits in netmask (for prefix length comparison)
auto mask_prefix_len(proto::IPv4Address mask_addr) -> int {
    uint32_t mask = mask_addr.to_host_order();
    int n = 0;
    while ((mask & 0x80000000) != 0U) {
        n++;
        mask <<= 1;
    }
    return n;
}
}  // namespace

void route_init() {
    route_count = 0;
    for (auto& r : routes) {
        r.valid = false;
    }
}

auto route_lookup(proto::IPv4Address dst) -> RouteEntry* {
    RouteEntry* best = nullptr;
    int best_prefix = -1;

    for (size_t i = 0; i < route_count; i++) {
        auto& route = routes.at(i);
        if (!route.valid) {
            continue;
        }
        if ((dst.to_host_order() & route.netmask.to_host_order()) == (route.dest.to_host_order() & route.netmask.to_host_order())) {
            int const PREFIX = mask_prefix_len(route.netmask);
            if (PREFIX > best_prefix || (PREFIX == best_prefix && best != nullptr && route.metric < best->metric)) {
                best = &route;
                best_prefix = PREFIX;
            }
        }
    }

    return best;
}

auto route_add(proto::IPv4Address dest, proto::IPv4Address netmask, proto::IPv4Address gateway, uint32_t metric, NetDevice* dev) -> int {
    if (route_count >= MAX_ROUTES) {
        return -1;
    }

    // Find a free slot
    for (auto& r : routes) {
        if (!r.valid) {
            r.dest = dest;
            r.netmask = netmask;
            r.gateway = gateway;
            r.metric = metric;
            r.dev = dev;
            r.valid = true;
            route_count++;

#ifdef DEBUG_ROUTE
            log::debug("add %d.%d.%d.%d/%d.%d.%d.%d gw %d.%d.%d.%d dev %s", (dest >> 24) & 0xFF, (dest >> 16) & 0xFF, (dest >> 8) & 0xFF,
                       dest & 0xFF, (netmask >> 24) & 0xFF, (netmask >> 16) & 0xFF, (netmask >> 8) & 0xFF, netmask & 0xFF,
                       (gateway >> 24) & 0xFF, (gateway >> 16) & 0xFF, (gateway >> 8) & 0xFF, gateway & 0xFF,
                       dev != nullptr ? dev->name.data() : "none");
#endif
            return 0;
        }
    }
    return -1;
}

auto route_del(proto::IPv4Address dest, proto::IPv4Address netmask) -> int {
    for (auto& r : routes) {
        if (r.valid && r.dest == dest && r.netmask == netmask) {
            r.valid = false;
            route_count--;
            return 0;
        }
    }
    return -1;
}

}  // namespace ker::net
