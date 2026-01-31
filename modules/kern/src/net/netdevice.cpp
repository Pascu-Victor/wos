#include "netdevice.hpp"

#include <cstring>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net {

namespace {
NetDevice* devices[MAX_NET_DEVICES] = {};
size_t device_count = 0;
uint32_t next_ifindex = 1;
uint32_t next_eth_index = 0;
}  // namespace

auto netdev_register(NetDevice* dev) -> int {
    if (dev == nullptr || device_count >= MAX_NET_DEVICES) {
        return -1;
    }

    dev->ifindex = next_ifindex++;

    // Auto-name if the device name is empty
    if (dev->name[0] == '\0') {
        // Name format: "ethN"
        char buf[NETDEV_NAME_LEN] = "eth";
        uint32_t idx = next_eth_index++;
        // Simple integer-to-string for index
        if (idx < 10) {
            buf[3] = '0' + static_cast<char>(idx);
            buf[4] = '\0';
        } else {
            buf[3] = '0' + static_cast<char>(idx / 10);
            buf[4] = '0' + static_cast<char>(idx % 10);
            buf[5] = '\0';
        }
        std::memcpy(dev->name, buf, NETDEV_NAME_LEN);
    }

    devices[device_count] = dev;
    device_count++;

#ifdef DEBUG_NETDEV
    ker::mod::dbg::log("net: Registered device %s (ifindex=%d, MAC=%x:%x:%x:%x:%x:%x)", dev->name, dev->ifindex, dev->mac[0], dev->mac[1],
                       dev->mac[2], dev->mac[3], dev->mac[4], dev->mac[5]);
#endif

    return 0;
}

auto netdev_find_by_name(const char* name) -> NetDevice* {
    if (name == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < device_count; i++) {
        if (std::strcmp(devices[i]->name, name) == 0) {
            return devices[i];
        }
    }
    return nullptr;
}

auto netdev_count() -> size_t { return device_count; }

auto netdev_at(size_t i) -> NetDevice* {
    if (i >= device_count) {
        return nullptr;
    }
    return devices[i];
}

// Forward declaration - will be implemented in ethernet.cpp
namespace proto {
void eth_rx(NetDevice* dev, PacketBuffer* pkt);
}

void netdev_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (dev == nullptr || pkt == nullptr) {
        return;
    }

#ifdef DEBUG_NETDEV
    ker::mod::dbg::log("netdev_rx: received packet len=%zu on device %s\n", pkt->len, dev->name);
#endif

    pkt->dev = dev;
    dev->rx_packets++;
    dev->rx_bytes += pkt->len;

    // Loopback device sends raw IP packets (no Ethernet header)
    // Check if this is loopback by name
    if (dev->name[0] == 'l' && dev->name[1] == 'o' && dev->name[2] == '\0') {
#ifdef DEBUG_NETDEV
        ker::mod::dbg::log("netdev_rx: loopback device detected, bypassing Ethernet\n");
#endif
        // Determine protocol from IP version in first byte
        if (pkt->len > 0) {
            uint8_t version = (pkt->data[0] >> 4) & 0xF;
#ifdef DEBUG_NETDEV
            ker::mod::dbg::log("netdev_rx: IP version = %u\n", version);
#endif
            if (version == 4) {
                proto::ipv4_rx(dev, pkt);
                return;
            } else if (version == 6) {
                proto::ipv6_rx(dev, pkt);
                return;
            }
        }
        // Unknown or malformed
        pkt_free(pkt);
        return;
    }

    // Hand off to ethernet layer for demuxing
    proto::eth_rx(dev, pkt);
}

}  // namespace ker::net
