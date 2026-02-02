#include "route.hpp"

#include <platform/dbg/dbg.hpp>

namespace ker::net {

namespace {
RouteEntry routes[MAX_ROUTES] = {};
size_t route_count = 0;

// Count leading 1-bits in netmask (for prefix length comparison)
auto mask_prefix_len(uint32_t mask) -> int {
    int n = 0;
    while (mask & 0x80000000) {
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

auto route_lookup(uint32_t dst) -> RouteEntry* {
    RouteEntry* best = nullptr;
    int best_prefix = -1;

    for (size_t i = 0; i < route_count; i++) {
        if (!routes[i].valid) {
            continue;
        }
        if ((dst & routes[i].netmask) == (routes[i].dest & routes[i].netmask)) {
            int prefix = mask_prefix_len(routes[i].netmask);
            if (prefix > best_prefix || (prefix == best_prefix && best != nullptr && routes[i].metric < best->metric)) {
                best = &routes[i];
                best_prefix = prefix;
            }
        }
    }

    return best;
}

auto route_add(uint32_t dest, uint32_t netmask, uint32_t gateway, uint32_t metric, NetDevice* dev) -> int {
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
            ker::mod::dbg::log("net: route add %d.%d.%d.%d/%d.%d.%d.%d gw %d.%d.%d.%d dev %s", (dest >> 24) & 0xFF, (dest >> 16) & 0xFF,
                               (dest >> 8) & 0xFF, dest & 0xFF, (netmask >> 24) & 0xFF, (netmask >> 16) & 0xFF, (netmask >> 8) & 0xFF,
                               netmask & 0xFF, (gateway >> 24) & 0xFF, (gateway >> 16) & 0xFF, (gateway >> 8) & 0xFF, gateway & 0xFF,
                               dev ? dev->name : "none");
#endif
            return 0;
        }
    }
    return -1;
}

auto route_del(uint32_t dest, uint32_t netmask) -> int {
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
