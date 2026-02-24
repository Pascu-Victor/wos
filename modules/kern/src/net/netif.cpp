#include "netif.hpp"

#include <array>
#include <cstring>
#include <platform/dbg/dbg.hpp>

namespace ker::net {

namespace {
NetInterface interfaces[MAX_NET_INTERFACES] = {};
size_t interface_count = 0;
}  // namespace

auto netif_get(NetDevice* dev) -> NetInterface* {
    if (dev == nullptr) {
        return nullptr;
    }

    // Check if interface already exists for this device
    for (size_t i = 0; i < interface_count; i++) {
        if (interfaces[i].dev == dev) {
            return &interfaces[i];
        }
    }

    // Create new interface
    if (interface_count >= MAX_NET_INTERFACES) {
        return nullptr;
    }

    auto* nif = &interfaces[interface_count];
    nif->dev = dev;
    interface_count++;

    return nif;
}

auto netif_add_ipv4(NetDevice* dev, uint32_t addr, uint32_t mask) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    if (nif->ipv4_addr_count >= MAX_ADDRS_PER_IF) {
        return -1;
    }

    nif->ipv4_addrs[nif->ipv4_addr_count].addr = addr;
    nif->ipv4_addrs[nif->ipv4_addr_count].netmask = mask;
    nif->ipv4_addr_count++;

#ifdef DEBUG_NETIF
    ker::mod::dbg::log("net: %s: added IPv4 %d.%d.%d.%d/%d.%d.%d.%d", dev->name, (addr >> 24) & 0xFF, (addr >> 16) & 0xFF,
                       (addr >> 8) & 0xFF, addr & 0xFF, (mask >> 24) & 0xFF, (mask >> 16) & 0xFF, (mask >> 8) & 0xFF, mask & 0xFF);
#endif

    return 0;
}

auto netif_add_ipv6(NetDevice* dev, const std::array<uint8_t, 16>& addr, uint8_t prefix) -> int {
    auto* nif = netif_get(dev);
    if (nif == nullptr) {
        return -1;
    }

    if (nif->ipv6_addr_count >= MAX_ADDRS_PER_IF) {
        return -1;
    }

    nif->ipv6_addrs[nif->ipv6_addr_count].addr = addr;
    nif->ipv6_addrs[nif->ipv6_addr_count].prefix_len = prefix;
    nif->ipv6_addr_count++;

#ifdef DEBUG_NETIF
    ker::mod::dbg::log("net: %s: added IPv6 address (prefix_len=%d)", dev->name, prefix);
#endif

    return 0;
}

auto netif_find_by_ipv4(uint32_t addr) -> NetInterface* {
    for (size_t i = 0; i < interface_count; i++) {
        for (size_t j = 0; j < interfaces[i].ipv4_addr_count; j++) {
            if (interfaces[i].ipv4_addrs[j].addr == addr) {
                return &interfaces[i];
            }
        }
    }
    return nullptr;
}

auto netif_find_by_ipv6(const std::array<uint8_t, 16>& addr) -> NetInterface* {
    for (size_t i = 0; i < interface_count; i++) {
        for (size_t j = 0; j < interfaces[i].ipv6_addr_count; j++) {
            if (interfaces[i].ipv6_addrs[j].addr == addr) {
                return &interfaces[i];
            }
        }
    }
    return nullptr;
}

}  // namespace ker::net
