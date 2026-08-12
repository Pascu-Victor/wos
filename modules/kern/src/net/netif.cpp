#include "netif.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/route6.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
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
            nif->dev_identity = {.device = dev, .generation = dev->lifetime_generation.load(std::memory_order_acquire)};
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
    return netif_set_ipv6(dev, addr, prefix, IPV6_ADDR_F_PERMANENT | IPV6_ADDR_F_NODAD, UINT64_MAX, UINT64_MAX, false);
}

namespace {
auto ipv6_state_flags(IPv6Addr::State state) -> uint32_t {
    switch (state) {
        case IPv6Addr::State::TENTATIVE:
            return IPV6_ADDR_F_TENTATIVE;
        case IPv6Addr::State::DEPRECATED:
            return IPV6_ADDR_F_DEPRECATED;
        case IPv6Addr::State::DADFAILED:
            return IPV6_ADDR_F_DADFAILED;
        case IPv6Addr::State::PREFERRED:
            return 0;
    }
    return 0;
}

void notify_ipv6_change(NetDevice* dev) {
    ker::net::wki::wki_dev_server_notify_net_changed(dev);
    ker::net::wki::wki_remotable_notify_net_changed(dev);
}

auto ipv6_source_scope_compatible(const proto::IPv6Address& src, const proto::IPv6Address& dst) -> bool {
    if (dst.is_loopback()) {
        return src.is_loopback();
    }
    if (dst.is_link_local()) {
        return src.is_link_local_unicast();
    }
    return !src.is_link_local_unicast() && !src.is_loopback();
}

auto ipv6_usable_state(uint64_t preferred_until_ms, uint64_t now_ms) -> IPv6Addr::State {
    return preferred_until_ms != UINT64_MAX && preferred_until_ms <= now_ms ? IPv6Addr::State::DEPRECATED : IPv6Addr::State::PREFERRED;
}
}  // namespace

auto netif_set_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix, uint32_t flags, uint64_t preferred_until_ms,
                    uint64_t valid_until_ms, bool replace) -> int {
    constexpr uint32_t CONFIG_FLAGS = IPV6_ADDR_F_PERMANENT | IPV6_ADDR_F_NOPREFIXROUTE | IPV6_ADDR_F_AUTOCONF | IPV6_ADDR_F_NODAD;
    if (dev == nullptr || prefix > 128 || addr.is_unspecified() || addr.is_multicast() || (flags & ~CONFIG_FLAGS) != 0) {
        return -EINVAL;
    }
    if (preferred_until_ms > valid_until_ms) {
        return -EINVAL;
    }
    uint64_t const NOW_MS = mod::time::get_ms();
    if (valid_until_ms != UINT64_MAX && valid_until_ms <= NOW_MS) {
        if (!replace) {
            return -EINVAL;
        }
        NetDeviceRef device = netdev_retain_registered(dev);
        if (!device) {
            return -ENODEV;
        }
        int const DELETE_RESULT = netif_del_ipv6(dev, addr, prefix);
        return DELETE_RESULT == -EADDRNOTAVAIL || DELETE_RESULT == -ENODEV ? 0 : DELETE_RESULT;
    }

    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -ENODEV;
    }
    NetDeviceRef retained_device = netdev_try_retain(nif->dev_identity);
    if (!retained_device) {
        return -ENODEV;
    }

    bool const SKIP_DAD = (flags & IPV6_ADDR_F_NODAD) != 0 || addr.is_loopback();
    IPv6Addr updated{.addr = addr,
                     .prefix_len = prefix,
                     .state = SKIP_DAD ? ipv6_usable_state(preferred_until_ms, NOW_MS) : IPv6Addr::State::TENTATIVE,
                     .flags = flags,
                     .preferred_until_ms = preferred_until_ms,
                     .valid_until_ms = valid_until_ms};

    bool replace_existing = false;
    IPv6Addr old{};
    size_t target_index = 0;
    uint64_t const LOCK_FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    for (size_t i = 0; i < nif->ipv6_addr_count; ++i) {
        auto const& slot = nif->ipv6_addrs.at(i);
        if (slot.addr != addr) {
            continue;
        }
        if (!replace) {
            nif->ipv6_addr_lock.unlock_irqrestore(LOCK_FLAGS);
            return -EEXIST;
        }
        old = slot;
        target_index = i;
        replace_existing = true;
        break;
    }

    if (!replace_existing && nif->ipv6_addr_count >= MAX_ADDRS_PER_IF) {
        nif->ipv6_addr_lock.unlock_irqrestore(LOCK_FLAGS);
        return -ENOSPC;
    }
    uint64_t const OBSERVED_GENERATION = nif->ipv6_addr_generation;
    nif->ipv6_addr_lock.unlock_irqrestore(LOCK_FLAGS);

    if (replace_existing && old.prefix_len == prefix) {
        bool const OLD_SKIPPED_DAD = (old.flags & IPV6_ADDR_F_NODAD) != 0 || addr.is_loopback();
        if (SKIP_DAD) {
            updated.state = ipv6_usable_state(preferred_until_ms, NOW_MS);
        } else if (!OLD_SKIPPED_DAD) {
            if (old.state == IPv6Addr::State::PREFERRED || old.state == IPv6Addr::State::DEPRECATED) {
                // An unchanged address that already completed DAD remains
                // usable across periodic RA/SLAAC lifetime refreshes.
                updated.state = ipv6_usable_state(preferred_until_ms, NOW_MS);
            } else {
                // Preserve an in-flight DAD deadline/probe count, or a proven
                // duplicate failure, instead of restarting each RA refresh.
                updated.state = old.state;
                updated.dad_deadline_ms = old.dad_deadline_ms;
                updated.dad_probes_sent = old.dad_probes_sent;
            }
        }
        // Removing NODAD from an address that never ran DAD intentionally
        // starts a fresh tentative cycle.
    }
    updated.flags &= ~(IPV6_ADDR_F_DADFAILED | IPV6_ADDR_F_DEPRECATED | IPV6_ADDR_F_TENTATIVE);
    updated.flags |= ipv6_state_flags(updated.state);

    bool const NEW_HAS_ROUTE = (updated.state == IPv6Addr::State::PREFERRED || updated.state == IPv6Addr::State::DEPRECATED) &&
                               (updated.flags & IPV6_ADDR_F_NOPREFIXROUTE) == 0;
    if (NEW_HAS_ROUTE) {
        int const ROUTE_RESULT = route6_add_connected(nif->dev_identity, updated.addr, updated.prefix_len);
        if (ROUTE_RESULT < 0) {
            return ROUTE_RESULT;
        }
    }

    uint64_t const COMMIT_FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    bool stale_transaction = nif->ipv6_addr_generation != OBSERVED_GENERATION;
    if (!stale_transaction && replace_existing) {
        stale_transaction = target_index >= nif->ipv6_addr_count || nif->ipv6_addrs.at(target_index).addr != old.addr ||
                            nif->ipv6_addrs.at(target_index).prefix_len != old.prefix_len;
    }
    if (!stale_transaction && !replace_existing) {
        stale_transaction = nif->ipv6_addr_count >= MAX_ADDRS_PER_IF;
    }
    if (stale_transaction) {
        nif->ipv6_addr_lock.unlock_irqrestore(COMMIT_FLAGS);
        if (NEW_HAS_ROUTE) {
            (void)route6_del_connected(nif->dev_identity, updated.addr, updated.prefix_len);
        }
        return -EAGAIN;
    }
    if (replace_existing) {
        nif->ipv6_addrs.at(target_index) = updated;
    } else {
        nif->ipv6_addrs.at(nif->ipv6_addr_count++) = updated;
    }
    ++nif->ipv6_addr_generation;
    nif->ipv6_addr_lock.unlock_irqrestore(COMMIT_FLAGS);

    bool const OLD_HAS_ROUTE = replace_existing && (old.state == IPv6Addr::State::PREFERRED || old.state == IPv6Addr::State::DEPRECATED) &&
                               (old.flags & IPV6_ADDR_F_NOPREFIXROUTE) == 0;
    if (OLD_HAS_ROUTE) {
        (void)route6_del_connected(nif->dev_identity, old.addr, old.prefix_len);
    }
    notify_ipv6_change(dev);

#ifdef DEBUG_NETIF
    log::debug("%s: added IPv6 address (prefix_len=%d)", dev->name.data(), prefix);
#endif

    return 0;
}

auto netif_del_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix) -> int {
    auto* nif = netif_find_by_dev(dev);
    if (nif == nullptr || prefix > 128) {
        return -ENODEV;
    }
    NetDeviceRef retained_device = netdev_try_retain(nif->dev_identity);
    if (!retained_device) {
        return -ENODEV;
    }

    IPv6Addr removed{};
    bool found = false;
    uint64_t const LOCK_FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    for (size_t i = 0; i < nif->ipv6_addr_count; ++i) {
        if (nif->ipv6_addrs.at(i).addr != addr || nif->ipv6_addrs.at(i).prefix_len != prefix) {
            continue;
        }
        removed = nif->ipv6_addrs.at(i);
        for (size_t j = i + 1; j < nif->ipv6_addr_count; ++j) {
            nif->ipv6_addrs.at(j - 1) = nif->ipv6_addrs.at(j);
        }
        --nif->ipv6_addr_count;
        nif->ipv6_addrs.at(nif->ipv6_addr_count) = {};
        ++nif->ipv6_addr_generation;
        found = true;
        break;
    }
    nif->ipv6_addr_lock.unlock_irqrestore(LOCK_FLAGS);
    if (!found) {
        return -EADDRNOTAVAIL;
    }
    if ((removed.state == IPv6Addr::State::PREFERRED || removed.state == IPv6Addr::State::DEPRECATED) &&
        (removed.flags & IPV6_ADDR_F_NOPREFIXROUTE) == 0) {
        (void)route6_del_connected(nif->dev_identity, removed.addr, removed.prefix_len);
    }
    notify_ipv6_change(dev);
    return 0;
}

auto netif_ipv6_snapshot(NetDevice* dev, IPv6Addr* out, size_t capacity) -> size_t {
    auto* nif = netif_find_by_dev(dev);
    if (nif == nullptr) {
        return 0;
    }
    uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    size_t const TOTAL = nif->ipv6_addr_count;
    if (out != nullptr) {
        size_t const COPY_COUNT = std::min(capacity, TOTAL);
        for (size_t i = 0; i < COPY_COUNT; ++i) {
            out[i] = nif->ipv6_addrs.at(i);
        }
    }
    nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    return TOTAL;
}

auto netif_ipv6_find(NetDevice* dev, const proto::IPv6Address& addr, IPv6Addr& out) -> bool {
    auto* nif = netif_find_by_dev(dev);
    if (nif == nullptr) {
        return false;
    }
    uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    for (size_t i = 0; i < nif->ipv6_addr_count; ++i) {
        if (nif->ipv6_addrs.at(i).addr == addr) {
            out = nif->ipv6_addrs.at(i);
            nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    return false;
}

auto netif_ipv6_find_owner(const proto::IPv6Address& addr, IPv6Addr& out, NetDeviceRef& device) -> bool {
    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_acquire);
    for (size_t i = 0; i < SCAN_LIMIT; ++i) {
        auto* nif = live_interfaces.at(i).load(std::memory_order_acquire);
        if (nif == nullptr) {
            continue;
        }
        NetDeviceRef retained = netdev_try_retain(nif->dev_identity);
        if (!retained) {
            continue;
        }
        uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
        for (size_t j = 0; j < nif->ipv6_addr_count; ++j) {
            auto const& candidate = nif->ipv6_addrs.at(j);
            if (candidate.addr == addr &&
                (candidate.state == IPv6Addr::State::PREFERRED || candidate.state == IPv6Addr::State::DEPRECATED)) {
                out = candidate;
                nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
                device = static_cast<NetDeviceRef&&>(retained);
                return true;
            }
        }
        nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    }
    return false;
}

auto netif_ipv6_select_source(NetDevice* dev, const proto::IPv6Address& dst, const proto::IPv6Address* requested, proto::IPv6Address& out)
    -> int {
    auto* nif = netif_find_by_dev(dev);
    if (nif == nullptr) {
        return -ENODEV;
    }

    bool found = false;
    bool best_deprecated = true;
    uint8_t best_common = 0;
    uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    for (size_t i = 0; i < nif->ipv6_addr_count; ++i) {
        auto const& candidate = nif->ipv6_addrs.at(i);
        if (candidate.state != IPv6Addr::State::PREFERRED && candidate.state != IPv6Addr::State::DEPRECATED) {
            continue;
        }
        if (requested != nullptr && !requested->is_unspecified() && candidate.addr != *requested) {
            continue;
        }
        if (!ipv6_source_scope_compatible(candidate.addr, dst)) {
            continue;
        }
        bool const DEPRECATED = candidate.state == IPv6Addr::State::DEPRECATED;
        uint8_t const COMMON = candidate.addr.common_prefix_length(dst);
        if (!found || (best_deprecated && !DEPRECATED) || (best_deprecated == DEPRECATED && COMMON > best_common)) {
            out = candidate.addr;
            found = true;
            best_deprecated = DEPRECATED;
            best_common = COMMON;
        }
    }
    nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    return found ? 0 : -EADDRNOTAVAIL;
}

auto netif_ipv6_dad_failed(NetDeviceIdentity identity, const proto::IPv6Address& addr) -> bool {
    auto* nif = netif_find_by_dev(identity.device);
    if (nif == nullptr || nif->dev_identity != identity) {
        return false;
    }
    NetDeviceRef retained_device = netdev_try_retain(identity);
    if (!retained_device) {
        return false;
    }
    bool changed = false;
    uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
    for (size_t i = 0; i < nif->ipv6_addr_count; ++i) {
        auto& slot = nif->ipv6_addrs.at(i);
        if (slot.addr == addr && slot.state == IPv6Addr::State::TENTATIVE) {
            slot.state = IPv6Addr::State::DADFAILED;
            slot.flags &= ~(IPV6_ADDR_F_TENTATIVE | IPV6_ADDR_F_DEPRECATED);
            slot.flags |= IPV6_ADDR_F_DADFAILED;
            slot.dad_deadline_ms = 0;
            slot.dad_probes_sent = 0;
            ++nif->ipv6_addr_generation;
            changed = true;
            break;
        }
    }
    nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    if (changed) {
        notify_ipv6_change(retained_device.get());
    }
    return changed;
}

auto netif_ipv6_timer_tick(uint64_t now_ms, IPv6DadProbe* probes, size_t capacity) -> size_t {
    struct PromotionAction {
        proto::IPv6Address address{};
        uint8_t prefix_len{};
    };
    size_t probe_count = 0;

    size_t const SCAN_LIMIT = interface_scan_limit.load(std::memory_order_acquire);
    for (size_t interface_index = 0; interface_index < SCAN_LIMIT; ++interface_index) {
        auto* nif = live_interfaces.at(interface_index).load(std::memory_order_acquire);
        if (nif == nullptr) {
            continue;
        }
        NetDeviceRef retained_device = netdev_try_retain(nif->dev_identity);
        if (!retained_device) {
            continue;
        }

        std::array<IPv6Addr, MAX_ADDRS_PER_IF> removals{};
        std::array<PromotionAction, MAX_ADDRS_PER_IF> promotions{};
        size_t removal_count = 0;
        size_t promotion_count = 0;
        bool changed = false;
        uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
        size_t address_index = 0;
        while (address_index < nif->ipv6_addr_count) {
            auto& slot = nif->ipv6_addrs.at(address_index);
            if (slot.valid_until_ms != UINT64_MAX && now_ms >= slot.valid_until_ms) {
                if ((slot.state == IPv6Addr::State::PREFERRED || slot.state == IPv6Addr::State::DEPRECATED) &&
                    (slot.flags & IPV6_ADDR_F_NOPREFIXROUTE) == 0 && removal_count < removals.size()) {
                    removals.at(removal_count++) = slot;
                }
                for (size_t j = address_index + 1; j < nif->ipv6_addr_count; ++j) {
                    nif->ipv6_addrs.at(j - 1) = nif->ipv6_addrs.at(j);
                }
                --nif->ipv6_addr_count;
                nif->ipv6_addrs.at(nif->ipv6_addr_count) = {};
                ++nif->ipv6_addr_generation;
                changed = true;
                continue;
            }
            if (slot.state == IPv6Addr::State::PREFERRED && slot.preferred_until_ms != UINT64_MAX && now_ms >= slot.preferred_until_ms) {
                slot.state = IPv6Addr::State::DEPRECATED;
                slot.flags |= IPV6_ADDR_F_DEPRECATED;
                ++nif->ipv6_addr_generation;
                changed = true;
            }
            if (slot.state == IPv6Addr::State::TENTATIVE) {
                if (slot.dad_probes_sent == 0 && probe_count < capacity && probes != nullptr) {
                    NetDeviceRef probe_device = netdev_try_retain(nif->dev_identity);
                    if (probe_device) {
                        auto& probe = probes[probe_count++];
                        probe.dev_identity = nif->dev_identity;
                        probe.device = static_cast<NetDeviceRef&&>(probe_device);
                        probe.target = slot.addr;
                        slot.dad_probes_sent = 1;
                        slot.dad_deadline_ms = now_ms + 1000;
                    }
                } else if (slot.dad_probes_sent != 0 && now_ms >= slot.dad_deadline_ms) {
                    if ((slot.flags & IPV6_ADDR_F_NOPREFIXROUTE) != 0) {
                        slot.state = ipv6_usable_state(slot.preferred_until_ms, now_ms);
                        slot.flags &= ~IPV6_ADDR_F_TENTATIVE;
                        if (slot.state == IPv6Addr::State::DEPRECATED) {
                            slot.flags |= IPV6_ADDR_F_DEPRECATED;
                        }
                        slot.dad_deadline_ms = 0;
                        slot.dad_probes_sent = 0;
                        ++nif->ipv6_addr_generation;
                        changed = true;
                    } else if (promotion_count < promotions.size()) {
                        promotions.at(promotion_count++) = {.address = slot.addr, .prefix_len = slot.prefix_len};
                        // Keep the address tentative until its connected route
                        // is successfully published outside this lock.
                        slot.dad_deadline_ms = now_ms + 100;
                    }
                }
            }
            ++address_index;
        }
        nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);

        for (size_t i = 0; i < removal_count; ++i) {
            auto const& address = removals.at(i);
            (void)route6_del_connected(nif->dev_identity, address.addr, address.prefix_len);
        }
        for (size_t i = 0; i < promotion_count; ++i) {
            auto const& action = promotions.at(i);
            if (route6_add_connected(nif->dev_identity, action.address, action.prefix_len) < 0) {
                continue;
            }

            bool committed = false;
            uint64_t const COMMIT_FLAGS = nif->ipv6_addr_lock.lock_irqsave();
            for (size_t j = 0; j < nif->ipv6_addr_count; ++j) {
                auto& slot = nif->ipv6_addrs.at(j);
                if (slot.addr != action.address || slot.prefix_len != action.prefix_len || slot.state != IPv6Addr::State::TENTATIVE ||
                    slot.dad_probes_sent == 0) {
                    continue;
                }
                slot.state = ipv6_usable_state(slot.preferred_until_ms, now_ms);
                slot.flags &= ~IPV6_ADDR_F_TENTATIVE;
                if (slot.state == IPv6Addr::State::DEPRECATED) {
                    slot.flags |= IPV6_ADDR_F_DEPRECATED;
                }
                slot.dad_deadline_ms = 0;
                slot.dad_probes_sent = 0;
                ++nif->ipv6_addr_generation;
                committed = true;
                break;
            }
            nif->ipv6_addr_lock.unlock_irqrestore(COMMIT_FLAGS);
            if (!committed) {
                (void)route6_del_connected(nif->dev_identity, action.address, action.prefix_len);
            } else {
                changed = true;
            }
        }
        if (changed) {
            notify_ipv6_change(retained_device.get());
        }
    }
    return probe_count;
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
        uint64_t const FLAGS = nif->ipv6_addr_lock.lock_irqsave();
        for (size_t j = 0; j < nif->ipv6_addr_count; j++) {
            auto const& candidate = nif->ipv6_addrs.at(j);
            if (candidate.addr == addr &&
                (candidate.state == IPv6Addr::State::PREFERRED || candidate.state == IPv6Addr::State::DEPRECATED)) {
                nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
                return nif;
            }
        }
        nif->ipv6_addr_lock.unlock_irqrestore(FLAGS);
    }
    return nullptr;
}

}  // namespace ker::net
