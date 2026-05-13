#pragma once

#include <array>
#include <cstdint>
#include <dev/pci.hpp>
#include <dev/virtio/virtio.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>

namespace ker::dev::virtio {

constexpr uint8_t VIRTIO_NET_MAX_QUEUE_PAIRS = 8;

struct VirtIONetDevice;

struct VirtIONetQueuePair {
    VirtIONetDevice* dev{};
    Virtqueue* rxq{};
    Virtqueue* txq{};
    ker::net::NapiStruct napi{};
    uint8_t index{};
    uint8_t irq_vector{};
};

struct VirtIONetDevice {
    ker::net::NetDevice netdev;  // Embedded, first member for casting
    ker::dev::pci::PCIDevice* pci{};
    std::array<VirtIONetQueuePair, VIRTIO_NET_MAX_QUEUE_PAIRS> queue_pairs{};
    Virtqueue* ctrlq{};          // Control queue (activated when MQ is negotiated)
    uint16_t io_base{};          // BAR0 I/O port base (legacy path)
    uint8_t num_queue_pairs{1};  // Active queue pairs after MQ negotiation
    uint8_t configured_queue_pairs{1};
    uint8_t hdr_size{};  // sizeof virtio-net header: 10 (legacy) or 12 (modern/VERSION_1)
    uint32_t negotiated_features{};
    bool msix_enabled{};

    // Modern virtio (non-null when using virtio 1.0 MMIO interface)
    VirtioModernCfg* modern_cfg{};
    volatile uint8_t* notify_base{};
    uint32_t notify_off_multiplier{};
    volatile uint8_t* device_cfg_base{};

    // Serializes QUEUE_SELECT + MSI_QUEUE_VECTOR programming.
    ker::mod::sys::Spinlock irq_lock;
};

auto virtio_net_init() -> int;

}  // namespace ker::dev::virtio
