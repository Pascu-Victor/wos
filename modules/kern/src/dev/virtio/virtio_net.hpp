#pragma once

#include <cstdint>
#include <dev/pci.hpp>
#include <dev/virtio/virtio.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>

namespace ker::dev::virtio {

struct VirtIONetDevice {
    ker::net::NetDevice netdev;  // Embedded, first member for casting
    ker::dev::pci::PCIDevice* pci{};
    Virtqueue* rxq{};            // Virtqueue 0 (receive, pair 0)
    Virtqueue* txq{};            // Virtqueue 1 (transmit, pair 0)
    Virtqueue* rxq2{};           // Virtqueue 2 (receive, pair 1), nullptr if single-queue
    Virtqueue* txq2{};           // Virtqueue 3 (transmit, pair 1), nullptr if single-queue
    Virtqueue* ctrlq{};          // Control queue (activated when MQ is negotiated)
    uint16_t io_base{};          // BAR0 I/O port base (legacy path)
    uint8_t irq_vector{};        // Allocated IRQ vector (pair 0 / single-queue)
    uint8_t irq_vector2{};       // Allocated IRQ vector for pair 1 (0 if unused)
    uint8_t num_queue_pairs{1};  // 1 = single-queue, 2 = multi-queue
    uint8_t hdr_size{};          // sizeof virtio-net header: 10 (legacy) or 12 (modern/VERSION_1)
    uint32_t negotiated_features{};
    bool msix_enabled{};

    // Modern virtio (non-null when using virtio 1.0 MMIO interface)
    VirtioModernCfg* modern_cfg{};
    volatile uint8_t* notify_base{};
    uint32_t notify_off_multiplier{};
    volatile uint8_t* device_cfg_base{};

    // Serializes QUEUE_SELECT + MSI_QUEUE_VECTOR programming.
    ker::mod::sys::Spinlock irq_lock;

    ker::net::NapiStruct napi{};   // NAPI for queue pair 0
    ker::net::NapiStruct napi2{};  // NAPI for queue pair 1 (used when num_queue_pairs == 2)
};

auto virtio_net_init() -> int;

}  // namespace ker::dev::virtio
