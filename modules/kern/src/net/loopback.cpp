#include "loopback.hpp"

#include <array>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <platform/dbg/dbg.hpp>

#include "net/route.hpp"

namespace ker::net {

namespace {
NetDevice lo_dev;

int lo_open(NetDevice*) { return 0; }
void lo_close(NetDevice*) {}

int lo_xmit(NetDevice* dev, PacketBuffer* pkt) {
    // Loopback: feed the packet directly back into the RX path
#ifdef DEBUG_LOOPBACK
    ker::mod::dbg::log("lo_xmit: looping back packet len=%zu\n", pkt->len);
#endif
    dev->tx_packets++;
    dev->tx_bytes += pkt->len;
    netdev_rx(dev, pkt);
    return 0;
}

void lo_set_mac(NetDevice*, const uint8_t*) {}

NetDeviceOps lo_ops = {
    .open = lo_open,
    .close = lo_close,
    .start_xmit = lo_xmit,
    .set_mac = lo_set_mac,
};
}  // namespace

void loopback_init() {
    std::memset(&lo_dev, 0, sizeof(lo_dev));
    std::memcpy(lo_dev.name.data(), "lo", 3);
    std::memset(lo_dev.mac.data(), 0, 6);
    lo_dev.mtu = 65535;
    lo_dev.state = 1;  // always up
    lo_dev.ops = &lo_ops;

    netdev_register(&lo_dev);

    // Assign 127.0.0.1/8
    uint32_t lo_ip = (127 << 24) | 1;  // 127.0.0.1
    uint32_t lo_mask = (255 << 24);    // 255.0.0.0
    netif_add_ipv4(&lo_dev, lo_ip, lo_mask);

    // Add route for 127.0.0.0/8 via loopback
    uint32_t lo_net = (127 << 24);              // 127.0.0.0
    route_add(lo_net, lo_mask, 0, 0, &lo_dev);  // 0 gateway, 0 metric

    // Assign ::1/128
    std::array<uint8_t, 16> lo_ipv6 = {};
    lo_ipv6[15] = 1;
    netif_add_ipv6(&lo_dev, lo_ipv6, 128);

#ifdef DEBUG_LOOPBACK
    ker::mod::dbg::log("net: Loopback device initialized");
#endif
}

}  // namespace ker::net
