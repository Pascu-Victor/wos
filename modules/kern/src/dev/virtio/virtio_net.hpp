#pragma once

#include <cstdint>
#include <dev/pci.hpp>
#include <dev/virtio/virtio.hpp>
#include <net/netdevice.hpp>

namespace ker::dev::virtio {

struct VirtIONetDevice {
    ker::net::NetDevice netdev;   // Embedded, first member for casting
    ker::dev::pci::PCIDevice* pci;
    Virtqueue* rxq;               // Virtqueue 0 (receive)
    Virtqueue* txq;               // Virtqueue 1 (transmit)
    uint16_t io_base;             // BAR0 I/O port base
    uint8_t irq_vector;           // Allocated IRQ vector
    uint32_t negotiated_features;
};

// Probe for VirtIO-Net PCI devices and initialize them
auto virtio_net_init() -> int;

}  // namespace ker::dev::virtio
