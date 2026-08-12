#include "route6.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

namespace {
struct RouteSlot {
    IPv6RouteSnapshot route{};
    uint16_t connected_references{};
    bool live{};
};

std::array<RouteSlot, MAX_IPV6_ROUTES> route_slots{};
mod::sys::Spinlock route6_lock;

auto route_key_matches(const IPv6RouteSnapshot& route, const IPv6RouteSpec& match) -> bool {
    return route.prefix == match.prefix.masked(match.prefix_len) && route.prefix_len == match.prefix_len &&
           route.gateway == match.gateway && route.dev_identity == match.dev_identity && route.flags == match.flags;
}

auto usable_route(const IPv6RouteSnapshot& route, const proto::IPv6Address& dst, uint32_t bound_ifindex, uint64_t now_ms) -> bool {
    if (route.expires_at_ms != UINT64_MAX && now_ms >= route.expires_at_ms) {
        return false;
    }
    if (bound_ifindex != 0 && route.ifindex != bound_ifindex) {
        return false;
    }
    return dst.matches_prefix(route.prefix, route.prefix_len);
}
}  // namespace

void route6_init() {
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    route_slots = {};
    route6_lock.unlock_irqrestore(FLAGS);
}

auto route6_add(const IPv6RouteSpec& spec) -> int {
    constexpr uint32_t PUBLIC_FLAGS = IPV6_ROUTE_F_GATEWAY | IPV6_ROUTE_F_AUTOCONF;
    if (spec.prefix_len > 128 || !spec.dev_identity.valid() || (spec.flags & ~PUBLIC_FLAGS) != 0 || spec.gateway.is_multicast() ||
        (((spec.flags & IPV6_ROUTE_F_GATEWAY) == 0) != spec.gateway.is_unspecified())) {
        return -EINVAL;
    }
    if (spec.expires_at_ms != UINT64_MAX && spec.expires_at_ms <= mod::time::get_ms()) {
        int const DELETE_RESULT = route6_del(spec);
        return DELETE_RESULT == -ENOENT ? 0 : DELETE_RESULT;
    }
    NetDeviceRef retained = netdev_try_retain(spec.dev_identity);
    if (!retained) {
        return -ENODEV;
    }

    IPv6RouteSnapshot const SNAPSHOT{.prefix = spec.prefix.masked(spec.prefix_len),
                                     .gateway = spec.gateway,
                                     .prefix_len = spec.prefix_len,
                                     .metric = spec.metric,
                                     .flags = spec.flags,
                                     .expires_at_ms = spec.expires_at_ms,
                                     .ifindex = retained->ifindex,
                                     .dev_identity = spec.dev_identity};

    // The registry lease is the outer lock. If publication wins this lease,
    // unregister cannot retire the identity until its later route6 teardown
    // has observed the new row. If retirement wins, membership is already
    // gone and publication is refused.
    NetDeviceRegistryLease const REGISTRATION;
    if (!REGISTRATION.contains(spec.dev_identity.device) ||
        spec.dev_identity.device->lifetime_generation.load(std::memory_order_acquire) != spec.dev_identity.generation) {
        return -ENODEV;
    }
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    RouteSlot* free_slot = nullptr;
    for (auto& slot : route_slots) {
        if (!slot.live) {
            if (free_slot == nullptr) {
                free_slot = &slot;
            }
            continue;
        }
        if (slot.route.prefix == SNAPSHOT.prefix && slot.route.prefix_len == SNAPSHOT.prefix_len &&
            slot.route.gateway == SNAPSHOT.gateway && slot.route.dev_identity == SNAPSHOT.dev_identity &&
            slot.route.flags == SNAPSHOT.flags) {
            slot.route = SNAPSHOT;
            route6_lock.unlock_irqrestore(FLAGS);
            return 0;
        }
    }
    if (free_slot == nullptr) {
        route6_lock.unlock_irqrestore(FLAGS);
        return -ENOSPC;
    }
    free_slot->route = SNAPSHOT;
    free_slot->live = true;
    route6_lock.unlock_irqrestore(FLAGS);
    return 0;
}

auto route6_del(const IPv6RouteSpec& match) -> int {
    if (match.prefix_len > 128 || (match.flags & IPV6_ROUTE_F_CONNECTED) != 0) {
        return -EINVAL;
    }
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto& slot : route_slots) {
        if (slot.live && route_key_matches(slot.route, match)) {
            slot = {};
            route6_lock.unlock_irqrestore(FLAGS);
            return 0;
        }
    }
    route6_lock.unlock_irqrestore(FLAGS);
    return -ENOENT;
}

auto route6_del_for_dev(NetDeviceIdentity identity) -> size_t {
    size_t removed = 0;
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto& slot : route_slots) {
        if (slot.live && slot.route.dev_identity == identity) {
            slot = {};
            ++removed;
        }
    }
    route6_lock.unlock_irqrestore(FLAGS);
    return removed;
}

auto route6_add_connected(NetDeviceIdentity identity, const proto::IPv6Address& address, uint8_t prefix_len) -> int {
    if (prefix_len > 128 || !identity.valid()) {
        return -EINVAL;
    }
    NetDeviceRef retained = netdev_try_retain(identity);
    if (!retained) {
        return -ENODEV;
    }
    IPv6RouteSnapshot const SNAPSHOT{.prefix = address.masked(prefix_len),
                                     .prefix_len = prefix_len,
                                     .metric = 0,
                                     .flags = IPV6_ROUTE_F_CONNECTED,
                                     .expires_at_ms = UINT64_MAX,
                                     .ifindex = retained->ifindex,
                                     .dev_identity = identity};

    NetDeviceRegistryLease const REGISTRATION;
    if (!REGISTRATION.contains(identity.device) ||
        identity.device->lifetime_generation.load(std::memory_order_acquire) != identity.generation) {
        return -ENODEV;
    }
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    RouteSlot* free_slot = nullptr;
    for (auto& slot : route_slots) {
        if (!slot.live) {
            if (free_slot == nullptr) {
                free_slot = &slot;
            }
            continue;
        }
        if (slot.route.prefix == SNAPSHOT.prefix && slot.route.prefix_len == SNAPSHOT.prefix_len && slot.route.dev_identity == identity &&
            slot.route.flags == IPV6_ROUTE_F_CONNECTED) {
            if (slot.connected_references == UINT16_MAX) {
                route6_lock.unlock_irqrestore(FLAGS);
                return -EOVERFLOW;
            }
            ++slot.connected_references;
            route6_lock.unlock_irqrestore(FLAGS);
            return 0;
        }
    }
    if (free_slot == nullptr) {
        route6_lock.unlock_irqrestore(FLAGS);
        return -ENOSPC;
    }
    free_slot->route = SNAPSHOT;
    free_slot->connected_references = 1;
    free_slot->live = true;
    route6_lock.unlock_irqrestore(FLAGS);
    return 0;
}

auto route6_del_connected(NetDeviceIdentity identity, const proto::IPv6Address& address, uint8_t prefix_len) -> int {
    if (prefix_len > 128) {
        return -EINVAL;
    }
    proto::IPv6Address const PREFIX = address.masked(prefix_len);
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto& slot : route_slots) {
        if (!slot.live || slot.route.prefix != PREFIX || slot.route.prefix_len != prefix_len || slot.route.dev_identity != identity ||
            slot.route.flags != IPV6_ROUTE_F_CONNECTED) {
            continue;
        }
        if (slot.connected_references > 1) {
            --slot.connected_references;
        } else {
            slot = {};
        }
        route6_lock.unlock_irqrestore(FLAGS);
        return 0;
    }
    route6_lock.unlock_irqrestore(FLAGS);
    return -ENOENT;
}

auto route6_snapshot(IPv6RouteSnapshot* out, size_t capacity) -> size_t {
    size_t total = 0;
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto const& slot : route_slots) {
        if (!slot.live) {
            continue;
        }
        if (out != nullptr && total < capacity) {
            out[total] = slot.route;
        }
        ++total;
    }
    route6_lock.unlock_irqrestore(FLAGS);
    return total;
}

void route6_expire(uint64_t now_ms) {
    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto& slot : route_slots) {
        if (slot.live && slot.route.expires_at_ms != UINT64_MAX && now_ms >= slot.route.expires_at_ms) {
            slot = {};
        }
    }
    route6_lock.unlock_irqrestore(FLAGS);
}

auto route6_lookup(const proto::IPv6Address& dst, uint32_t bound_ifindex, IPv6RouteSnapshot& out, NetDeviceRef& device) -> int {
    uint8_t best_prefix = 0;
    uint32_t best_metric = UINT32_MAX;
    bool found = false;
    bool ambiguous_link_local = false;
    bool scope_required = dst.is_link_local();
    uint32_t selected_ifindex = 0;
    uint64_t const NOW_MS = mod::time::get_ms();

    uint64_t const FLAGS = route6_lock.lock_irqsave();
    for (auto const& slot : route_slots) {
        if (!slot.live || !usable_route(slot.route, dst, bound_ifindex, NOW_MS)) {
            continue;
        }
        auto const& route = slot.route;
        bool const BETTER = !found || route.prefix_len > best_prefix || (route.prefix_len == best_prefix && route.metric < best_metric);
        if (BETTER) {
            best_prefix = route.prefix_len;
            best_metric = route.metric;
            selected_ifindex = route.ifindex;
            ambiguous_link_local = false;
            scope_required = dst.is_link_local() || route.gateway.is_link_local_unicast();
            found = true;
        } else if (route.prefix_len == best_prefix && route.metric == best_metric) {
            if (route.ifindex != selected_ifindex) {
                ambiguous_link_local = true;
            }
            scope_required = scope_required || route.gateway.is_link_local_unicast();
        }
    }
    if (!found || (scope_required && bound_ifindex == 0 && ambiguous_link_local)) {
        route6_lock.unlock_irqrestore(FLAGS);
        return found ? -EADDRNOTAVAIL : -ENETUNREACH;
    }

    // Retain while the route registry is locked. Device retirement removes
    // all of its rows under this same lock before waiting for retained users,
    // so a copied identity cannot become a dangling pointer between lookup
    // and retention.
    for (auto const& slot : route_slots) {
        if (!slot.live || !usable_route(slot.route, dst, bound_ifindex, NOW_MS) || slot.route.prefix_len != best_prefix ||
            slot.route.metric != best_metric) {
            continue;
        }
        NetDeviceRef retained = netdev_try_retain(slot.route.dev_identity);
        if (retained) {
            out = slot.route;
            device = static_cast<NetDeviceRef&&>(retained);
            route6_lock.unlock_irqrestore(FLAGS);
            return 0;
        }
    }
    route6_lock.unlock_irqrestore(FLAGS);
    return -ENODEV;
}

}  // namespace ker::net
