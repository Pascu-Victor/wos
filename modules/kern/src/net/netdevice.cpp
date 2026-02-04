#include "netdevice.hpp"

#include <array>
#include <cstring>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/dbg/dbg.hpp>
#include <string_view>

namespace ker::net {

namespace {
std::array<NetDevice*, MAX_NET_DEVICES> devices = {};
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
        std::array<char, NETDEV_NAME_LEN> buf = {};
        buf[0] = 'e';
        buf[1] = 't';
        buf[2] = 'h';
        uint32_t idx = next_eth_index++;
        // Simple integer-to-string for index
        if (idx < 10) {
            buf[3] = static_cast<char>('0' + static_cast<char>(idx));
            buf[4] = '\0';
        } else {
            buf[3] = static_cast<char>('0' + static_cast<char>(idx / 10));
            buf[4] = static_cast<char>('0' + static_cast<char>(idx % 10));
            buf[5] = '\0';
        }
        dev->name = buf;
    }

    devices[device_count] = dev;
    device_count++;

#ifdef DEBUG_NETDEV
    ker::mod::dbg::log("net: Registered device %s (ifindex=%d, MAC=%x:%x:%x:%x:%x:%x)", dev->name, dev->ifindex, dev->mac[0], dev->mac[1],
                       dev->mac[2], dev->mac[3], dev->mac[4], dev->mac[5]);
#endif

    return 0;
}

auto netdev_find_by_name(const std::string_view NAME) -> NetDevice* {
    if (NAME.empty()) {
        return nullptr;
    }
    for (size_t i = 0; i < device_count; i++) {
        if (std::string_view(devices[i]->name.data()) == NAME) {
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

    // D11: Forward packet to WKI remote consumers (if any are attached)
    if (dev->wki_rx_forward != nullptr) {
        dev->wki_rx_forward(dev, pkt);
        // Original packet still processed locally
    }

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
            }
            if (version == 6) {
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
