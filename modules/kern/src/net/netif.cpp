#include "netif.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"netif">;

namespace {
std::array<NetInterface, MAX_NET_INTERFACES> interfaces = {};
size_t interface_count = 0;
}  // namespace

auto netif_get(NetDevice* dev) -> NetInterface* {
    if (dev == nullptr) {
        return nullptr;
    }

    // Check if interface already exists for this device
    for (size_t i = 0; i < interface_count; i++) {
        auto& nif = interfaces.at(i);
        if (nif.dev == dev) {
            return &nif;
        }
    }

    // Create new interface
    if (interface_count >= MAX_NET_INTERFACES) {
        return nullptr;
    }

    auto* nif = &interfaces.at(interface_count);
    nif->dev = dev;
    interface_count++;

    return nif;
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
    for (size_t i = 0; i < interface_count; i++) {
        auto& nif = interfaces.at(i);
        for (size_t j = 0; j < nif.ipv4_addr_count; j++) {
            if (nif.ipv4_addrs.at(j).addr == addr) {
                return &nif;
            }
        }
    }
    return nullptr;
}

auto netif_find_by_ipv6(const proto::IPv6Address& addr) -> NetInterface* {
    for (size_t i = 0; i < interface_count; i++) {
        auto& nif = interfaces.at(i);
        for (size_t j = 0; j < nif.ipv6_addr_count; j++) {
            if (nif.ipv6_addrs.at(j).addr == addr) {
                return &nif;
            }
        }
    }
    return nullptr;
}

}  // namespace ker::net
