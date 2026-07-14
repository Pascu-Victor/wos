#include "netif.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"netif">;

namespace {
struct NetInterfaceStorageEntry {
    NetInterface interface = {};
    NetInterfaceStorageEntry* next = nullptr;
};

// NetInterface pointers escape every lookup. Keep each published interface at
// a permanent address. Readers scan a fixed atomic live index without locking;
// the writer lock serializes slot publication/removal and permanent storage.
// Address-list mutation remains governed by its pre-existing caller contract.
NetInterfaceStorageEntry* interface_storage = nullptr;
std::array<std::atomic<NetInterface*>, MAX_NET_INTERFACES> live_interfaces = {};
std::atomic<size_t> interface_scan_limit{0};
std::atomic<size_t> interface_live_count{0};
mod::sys::Spinlock interface_registry_lock;
static_assert(std::atomic<NetInterface*>::is_always_lock_free, "netif live slots must stay lock-free");
static_assert(std::atomic<size_t>::is_always_lock_free, "netif scan bounds must stay lock-free");

auto find_live_interface(NetDevice* dev, std::memory_order order) -> NetInterface* {
    size_t const SCAN_LIMIT = interface_scan_limit.load(order);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto* nif = live_interfaces.at(i).load(order);
        if (nif != nullptr && nif->dev == dev) {
            return nif;
        }
    }
    return nullptr;
}

// interface_registry_lock must be held by the caller.
auto find_free_interface_slot_locked(size_t scan_limit) -> size_t {
    for (size_t i = 0; i < scan_limit; i++) {
        if (live_interfaces.at(i).load(std::memory_order_relaxed) == nullptr) {
            return i;
        }
    }
    return scan_limit;
}

// interface_registry_lock must be held by the caller.
void trim_interface_scan_limit_locked() {
    size_t scan_limit = interface_scan_limit.load(std::memory_order_relaxed);
    while (scan_limit > 0 && live_interfaces.at(scan_limit - 1).load(std::memory_order_relaxed) == nullptr) {
        scan_limit--;
    }
    interface_scan_limit.store(scan_limit, std::memory_order_release);
}
}  // namespace

auto netif_find_by_dev(NetDevice* dev) -> NetInterface* {
    if (dev == nullptr) {
        return nullptr;
    }
    return find_live_interface(dev, std::memory_order_acquire);
}

auto netif_get(NetDevice* dev) -> NetInterface* {
    if (dev == nullptr) {
        return nullptr;
    }

    // Lock-free fast path. Deletion never mutates the permanent object.
    if (auto* existing = netif_find_by_dev(dev); existing != nullptr) {
        return existing;
    }

    // Teardown unregisters before removing the live interface. Once the fast
    // path misses that row, never recreate configuration for an old raw device
    // pointer held by an in-flight packet.
    if (!netdev_is_registered(dev)) {
        return nullptr;
    }

    if (interface_live_count.load(std::memory_order_acquire) >= MAX_NET_INTERFACES) {
        return nullptr;
    }

    // Allocation and initialization stay outside the IRQ-safe writer section.
    auto* storage = new (std::nothrow) NetInterfaceStorageEntry{};
    if (storage == nullptr) {
        return nullptr;
    }
    auto* nif = &storage->interface;
    nif->dev = dev;

    NetInterface* result = nullptr;
    bool published = false;
    {
        // Registration is the outer lock. If teardown wins first, publication
        // is refused; if publication wins, unregister cannot precede the
        // teardown's subsequent netif_del_for_dev().
        NetDeviceRegistryLease const REGISTRATION;
        if (REGISTRATION.contains(dev)) {
            uint64_t const FLAGS = interface_registry_lock.lock_irqsave();
            if (auto* existing = find_live_interface(dev, std::memory_order_relaxed); existing != nullptr) {
                result = existing;
            } else {
                size_t const LIVE_COUNT = interface_live_count.load(std::memory_order_relaxed);
                if (LIVE_COUNT < MAX_NET_INTERFACES) {
                    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_relaxed);
                    size_t const SLOT = find_free_interface_slot_locked(SCAN_LIMIT);
                    if (SLOT < MAX_NET_INTERFACES) {
                        storage->next = interface_storage;
                        interface_storage = storage;
                        live_interfaces.at(SLOT).store(nif, std::memory_order_release);
                        if (SLOT == SCAN_LIMIT) {
                            interface_scan_limit.store(SCAN_LIMIT + 1, std::memory_order_release);
                        }
                        interface_live_count.store(LIVE_COUNT + 1, std::memory_order_release);
                        result = nif;
                        published = true;
                    }
                }
            }
            interface_registry_lock.unlock_irqrestore(FLAGS);
        }
    }

    if (!published) {
        delete storage;
    }
    return result;
}

auto netif_del_for_dev(NetDevice* dev) -> bool {
    if (dev == nullptr) {
        return false;
    }

    uint64_t const FLAGS = interface_registry_lock.lock_irqsave();
    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_relaxed);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto* nif = live_interfaces.at(i).load(std::memory_order_relaxed);
        if (nif == nullptr || nif->dev != dev) {
            continue;
        }

        // Only retire the live-index publication. Readers that already loaded
        // this permanent interface retain its existing configuration snapshot.
        live_interfaces.at(i).store(nullptr, std::memory_order_release);
        interface_live_count.store(interface_live_count.load(std::memory_order_relaxed) - 1, std::memory_order_release);
        trim_interface_scan_limit_locked();
        interface_registry_lock.unlock_irqrestore(FLAGS);
        return true;
    }
    interface_registry_lock.unlock_irqrestore(FLAGS);
    return false;
}

auto netif_add_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    if (nif->ipv4_addr_count >= MAX_ADDRS_PER_IF) {
        return -1;
    }

    auto& slot = nif->ipv4_addrs.at(nif->ipv4_addr_count);
    slot.addr = addr;
    slot.netmask = mask;
    nif->ipv4_addr_count++;
    ker::net::wki::wki_dev_server_notify_net_changed(dev);
    ker::net::wki::wki_remotable_notify_net_changed(dev);

#ifdef DEBUG_NETIF
    log::debug("%s: added IPv4 %d.%d.%d.%d/%d.%d.%d.%d", dev->name.data(), (addr >> 24) & 0xFF, (addr >> 16) & 0xFF, (addr >> 8) & 0xFF,
               addr & 0xFF, (mask >> 24) & 0xFF, (mask >> 16) & 0xFF, (mask >> 8) & 0xFF, mask & 0xFF);
#endif

    return 0;
}

auto netif_set_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask, bool replace) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    for (size_t i = 0; i < nif->ipv4_addr_count; i++) {
        auto& slot = nif->ipv4_addrs.at(i);
        if (slot.addr == addr) {
            if (!replace && slot.netmask == mask) {
                return -EEXIST;
            }
            slot.netmask = mask;
            ker::net::wki::wki_dev_server_notify_net_changed(dev);
            ker::net::wki::wki_remotable_notify_net_changed(dev);
            return 0;
        }
    }

    return netif_add_ipv4(dev, addr, mask);
}

auto netif_del_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    for (size_t i = 0; i < nif->ipv4_addr_count; i++) {
        if (auto& slot = nif->ipv4_addrs.at(i); slot.addr != addr || slot.netmask != mask) {
            continue;
        }
        for (size_t j = i + 1; j < nif->ipv4_addr_count; j++) {
            nif->ipv4_addrs.at(j - 1) = nif->ipv4_addrs.at(j);
        }
        nif->ipv4_addr_count--;
        nif->ipv4_addrs.at(nif->ipv4_addr_count) = {};
        ker::net::wki::wki_dev_server_notify_net_changed(dev);
        ker::net::wki::wki_remotable_notify_net_changed(dev);
        return 0;
    }

    return -EADDRNOTAVAIL;
}

auto netif_add_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    if (nif->ipv6_addr_count >= MAX_ADDRS_PER_IF) {
        return -1;
    }

    auto& slot = nif->ipv6_addrs.at(nif->ipv6_addr_count);
    slot.addr = addr;
    slot.prefix_len = prefix;
    nif->ipv6_addr_count++;

#ifdef DEBUG_NETIF
    log::debug("%s: added IPv6 address (prefix_len=%d)", dev->name.data(), prefix);
#endif

    return 0;
}

auto netif_find_by_ipv4(proto::IPv4Address addr) -> NetInterface* {
    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_acquire);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto* nif = live_interfaces.at(i).load(std::memory_order_acquire);
        if (nif == nullptr) {
            continue;
        }
        for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
            if (nif->ipv4_addrs.at(j).addr == addr) {
                return nif;
            }
        }
    }
    return nullptr;
}

auto netif_find_by_ipv6(const proto::IPv6Address& addr) -> NetInterface* {
    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_acquire);
    for (size_t i = 0; i < SCAN_LIMIT; i++) {
        auto* nif = live_interfaces.at(i).load(std::memory_order_acquire);
        if (nif == nullptr) {
            continue;
        }
        for (size_t j = 0; j < nif->ipv6_addr_count; j++) {
            if (nif->ipv6_addrs.at(j).addr == addr) {
                return nif;
            }
        }
    }
    return nullptr;
}

}  // namespace ker::net
