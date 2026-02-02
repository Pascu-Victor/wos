#pragma once

#include <cstddef>
#include <cstdint>
#include <net/packet.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

constexpr size_t MAX_NET_DEVICES = 16;
constexpr size_t NETDEV_NAME_LEN = 16;

struct NetDevice;

struct NetDeviceOps {
    int (*open)(NetDevice* dev);
    void (*close)(NetDevice* dev);
    int (*start_xmit)(NetDevice* dev, PacketBuffer* pkt);
    void (*set_mac)(NetDevice* dev, const uint8_t* mac);
};

struct NetDevice {
    char name[NETDEV_NAME_LEN];
    uint8_t mac[6];
    uint32_t mtu = 1500;
    uint8_t state = 0;  // 0=down, 1=up
    uint32_t ifindex = 0;
    NetDeviceOps const* ops = nullptr;
    void* private_data = nullptr;

    // Statistics
    uint64_t rx_packets = 0;
    uint64_t tx_packets = 0;
    uint64_t rx_bytes = 0;
    uint64_t tx_bytes = 0;
    uint64_t rx_dropped = 0;
    uint64_t tx_dropped = 0;
};

// Register a new network device (assigns ifindex, auto-names "ethN" if name is empty)
auto netdev_register(NetDevice* dev) -> int;

// Lookup
auto netdev_find_by_name(const char* name) -> NetDevice*;
auto netdev_count() -> size_t;
auto netdev_at(size_t i) -> NetDevice*;

// Called by drivers when a packet is received
void netdev_rx(NetDevice* dev, PacketBuffer* pkt);

}  // namespace ker::net
