#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/pci.hpp>
#include <dev/virtio/virtio.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>

namespace ker::dev::virtio {

constexpr uint8_t VIRTIO_NET_MAX_QUEUE_PAIRS = 8;
constexpr size_t VIRTIO_NET_DIAG_MAX_ROWS = 32;

struct VirtIONetDevice;

struct VirtqueueDiagSnapshot {
    uint16_t size = 0;
    uint16_t num_free = 0;
    uint16_t pending = 0;
    uint16_t avail_idx = 0;
    uint16_t used_idx = 0;
    uint16_t last_used_idx = 0;
    uint16_t mapped = 0;
};

struct VirtIONetDiagSnapshot {
    std::array<char, ker::net::NETDEV_NAME_LEN> name{};
    uint32_t ifindex = 0;
    uint32_t negotiated_features = 0;
    uint8_t pair = 0;
    uint8_t num_queue_pairs = 0;
    uint8_t configured_queue_pairs = 0;
    uint8_t hdr_size = 0;
    uint8_t irq_vector = 0;
    uint8_t napi_state = 0;
    bool msix_enabled = false;
    bool active = false;
    bool napi_has_work = false;
    uint64_t napi_worker_pid = 0;
    uint64_t napi_worker_cpu = 0;
    uint64_t napi_polls = 0;
    uint64_t napi_completes = 0;
    VirtqueueDiagSnapshot rx{};
    VirtqueueDiagSnapshot tx{};
};

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
    uint8_t hdr_size{};  // sizeof negotiated virtio-net header
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
auto virtio_net_diag_snapshot(VirtIONetDiagSnapshot* out, size_t max) -> size_t;

}  // namespace ker::dev::virtio
