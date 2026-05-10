#include "loopback.hpp"

#include <cstddef>
#include <cstdint>
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>

#include "net/address.hpp"
#include "net/route.hpp"

namespace ker::net {

constexpr size_t ETHERNET_MTU_MAX = 65535;
constexpr uint8_t LO_IPV6_MASK = 128;

using log = ker::mod::dbg::logger<"loopback">;

namespace {
NetDevice lo_dev;

auto lo_open(NetDevice* /*unused*/) -> int { return 0; }
void lo_close(NetDevice* /*unused*/) {}

auto lo_xmit(NetDevice* dev, PacketBuffer* pkt) -> int {
    // Loopback packets must not recurse directly back into TCP RX because
    // loopback ACK generation can otherwise re-enter the same control-block path.
#ifdef DEBUG_LOOPBACK
    log::debug("lo_xmit: looping back packet len=%zu", pkt->len);
#endif
    dev->tx_packets++;
    dev->tx_bytes += pkt->len;

    if (backlog_ready()) {
        pkt->dev = dev;
        dev->rx_packets++;
        dev->rx_bytes += pkt->len;
        backlog_enqueue(ker::mod::cpu::current_cpu(), pkt);
        return 0;
    }

    netdev_rx(dev, pkt);
    return 0;
}

void lo_set_mac(NetDevice* /*unused*/, const uint8_t* /*unused*/) {}

NetDeviceOps lo_ops = {
    .open = lo_open,
    .close = lo_close,
    .start_xmit = lo_xmit,
    .set_mac = lo_set_mac,
    .set_queue_cpu = nullptr,
};
}  // namespace

void loopback_init() {
    lo_dev = {};
    lo_dev.name = {'l', 'o'};
    lo_dev.mac.fill(0);
    lo_dev.mtu = ETHERNET_MTU_MAX;
    lo_dev.state = 1;  // always up
    lo_dev.ops = &lo_ops;

    netdev_register(&lo_dev);

    // Assign 127.0.0.1/8
    uint32_t const LO_IP = (127 << 24) | 1;  // 127.0.0.1
    uint32_t const LO_MASK = (255 << 24);    // 255.0.0.0
    netif_add_ipv4(&lo_dev, LO_IP, LO_MASK);

    // Add route for 127.0.0.0/8 via loopback
    uint32_t const LO_NET = (127 << 24);        // 127.0.0.0
    route_add(LO_NET, LO_MASK, 0, 0, &lo_dev);  // 0 gateway, 0 metric

    // Assign ::1/128
    proto::IPv6Address const LO_IPV6 = proto::IPv6Address::loopback();
    netif_add_ipv6(&lo_dev, LO_IPV6, LO_IPV6_MASK);

#ifdef DEBUG_LOOPBACK
    log::debug("device initialized");
#endif
}

}  // namespace ker::net
