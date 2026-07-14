#include "route.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

#include "net/netdevice.hpp"

namespace ker::net {

using log = ker::mod::dbg::logger<"route">;

namespace {
struct RouteStorageEntry {
    RouteEntry route = {};
    RouteStorageEntry* next = nullptr;
};

// RouteEntry pointers escape route_lookup(). Keep each published entry at a
// permanent address. Readers scan a fixed atomic live index without locking;
// the writer lock serializes slot publication/removal and permanent storage.
RouteStorageEntry* route_storage = nullptr;
std::array<std::atomic<RouteEntry*>, MAX_ROUTES> live_routes = {};
std::atomic<size_t> route_scan_limit{0};
std::atomic<size_t> route_live_count{0};
mod::sys::Spinlock route_registry_lock;
static_assert(std::atomic<RouteEntry*>::is_always_lock_free, "route live slots must stay lock-free");
static_assert(std::atomic<size_t>::is_always_lock_free, "route scan bounds must stay lock-free");

// route_registry_lock must be held by the caller.
auto find_free_route_slot_locked(size_t scan_limit) -> size_t {
    for (size_t i = 0; i < scan_limit; i++) {
        if (live_routes.at(i).load(std::memory_order_relaxed) == nullptr) {
            return i;
        }
    }
    return scan_limit;
}

// route_registry_lock must be held by the caller.
void remove_live_route_at(size_t index) {
    live_routes.at(index).store(nullptr, std::memory_order_release);
    route_live_count.store(route_live_count.load(std::memory_order_relaxed) - 1, std::memory_order_release);

    size_t scan_limit = route_scan_limit.load(std::memory_order_relaxed);
    while (scan_limit > 0 && live_routes.at(scan_limit - 1).load(std::memory_order_relaxed) == nullptr) {
        scan_limit--;
    }
    route_scan_limit.store(scan_limit, std::memory_order_release);
}

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
    uint64_t const FLAGS = route_registry_lock.lock_irqsave();
    size_t const SCAN_LIMIT = route_scan_limit.load(std::memory_order_relaxed);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        live_routes.at(i).store(nullptr, std::memory_order_release);
    }
    route_scan_limit.store(0, std::memory_order_release);
    route_live_count.store(0, std::memory_order_release);
    route_registry_lock.unlock_irqrestore(FLAGS);
}

auto route_lookup(proto::IPv4Address dst) -> RouteEntry* {
    RouteEntry* best = nullptr;
    int best_prefix = -1;

    size_t const SCAN_LIMIT = route_scan_limit.load(std::memory_order_acquire);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto* route = live_routes.at(i).load(std::memory_order_acquire);
        if (route == nullptr) {
            continue;
        }
        if ((dst.to_host_order() & route->netmask.to_host_order()) == (route->dest.to_host_order() & route->netmask.to_host_order())) {
            int const PREFIX = mask_prefix_len(route->netmask);
            if (PREFIX > best_prefix || (PREFIX == best_prefix && best != nullptr && route->metric < best->metric)) {
                best = route;
                best_prefix = PREFIX;
            }
        }
    }

    return best;
}

auto route_add(proto::IPv4Address dest, proto::IPv4Address netmask, proto::IPv4Address gateway, uint32_t metric, NetDevice* dev) -> int {
    if (route_live_count.load(std::memory_order_acquire) >= MAX_ROUTES) {
        return -1;
    }

    // Allocation and initialization stay outside the IRQ-safe writer section.
    auto* storage = new (std::nothrow) RouteStorageEntry{};
    if (storage == nullptr) {
        return -1;
    }
    auto* route = &storage->route;
    route->dest = dest;
    route->netmask = netmask;
    route->gateway = gateway;
    route->metric = metric;
    route->dev = dev;
    route->valid = true;

    bool published = false;
    uint64_t const FLAGS = route_registry_lock.lock_irqsave();
    size_t const LIVE_COUNT = route_live_count.load(std::memory_order_relaxed);
    if (LIVE_COUNT < MAX_ROUTES) {
        size_t const SCAN_LIMIT = route_scan_limit.load(std::memory_order_relaxed);
        size_t const SLOT = find_free_route_slot_locked(SCAN_LIMIT);
        if (SLOT < MAX_ROUTES) {
            storage->next = route_storage;
            route_storage = storage;
            live_routes.at(SLOT).store(route, std::memory_order_release);
            if (SLOT == SCAN_LIMIT) {
                route_scan_limit.store(SCAN_LIMIT + 1, std::memory_order_release);
            }
            route_live_count.store(LIVE_COUNT + 1, std::memory_order_release);
            published = true;
        }
    }
    route_registry_lock.unlock_irqrestore(FLAGS);

    if (!published) {
        delete storage;
        return -1;
    }

#ifdef DEBUG_ROUTE
    log::debug("add %d.%d.%d.%d/%d.%d.%d.%d gw %d.%d.%d.%d dev %s", (dest >> 24) & 0xFF, (dest >> 16) & 0xFF, (dest >> 8) & 0xFF,
               dest & 0xFF, (netmask >> 24) & 0xFF, (netmask >> 16) & 0xFF, (netmask >> 8) & 0xFF, netmask & 0xFF, (gateway >> 24) & 0xFF,
               (gateway >> 16) & 0xFF, (gateway >> 8) & 0xFF, gateway & 0xFF, dev != nullptr ? dev->name.data() : "none");
#endif
    return 0;
}

auto route_del(proto::IPv4Address dest, proto::IPv4Address netmask) -> int {
    uint64_t const FLAGS = route_registry_lock.lock_irqsave();
    size_t const SCAN_LIMIT = route_scan_limit.load(std::memory_order_relaxed);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto const* route = live_routes.at(i).load(std::memory_order_relaxed);
        if (route != nullptr && route->dest == dest && route->netmask == netmask) {
            remove_live_route_at(i);
            route_registry_lock.unlock_irqrestore(FLAGS);
            return 0;
        }
    }
    route_registry_lock.unlock_irqrestore(FLAGS);
    return -1;
}

auto route_del_for_dev(NetDevice* dev) -> size_t {
    if (dev == nullptr) {
        return 0;
    }

    uint64_t const FLAGS = route_registry_lock.lock_irqsave();
    size_t const SCAN_LIMIT = route_scan_limit.load(std::memory_order_relaxed);
    size_t live_count = route_live_count.load(std::memory_order_relaxed);
    size_t removed = 0;
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto const* route = live_routes.at(i).load(std::memory_order_relaxed);
        if (route == nullptr || route->dev != dev) {
            continue;
        }

        // Only retire the live-index publication. Readers that already loaded
        // this permanent entry retain its immutable route snapshot.
        live_routes.at(i).store(nullptr, std::memory_order_release);
        live_count--;
        removed++;
    }

    size_t trimmed_scan_limit = SCAN_LIMIT;
    while (trimmed_scan_limit > 0 && live_routes.at(trimmed_scan_limit - 1).load(std::memory_order_relaxed) == nullptr) {
        trimmed_scan_limit--;
    }
    route_scan_limit.store(trimmed_scan_limit, std::memory_order_release);
    route_live_count.store(live_count, std::memory_order_release);
    route_registry_lock.unlock_irqrestore(FLAGS);
    return removed;
}

}  // namespace ker::net
