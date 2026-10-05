#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/pci.hpp>
#include <dev/virtio/virtio.hpp>
#include <dev/virtio/virtio_lifecycle.hpp>
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
    ker::net::NapiDiagSnapshot napi_diag{};
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
    bool irq_owned{};  // Reserved or successfully registered by this device.
    uint8_t rx_empty_pending_rearms{};

    // Serializes the short IRQ/NAPI notification handoff.  Poll processing
    // itself runs without this lock.
    ker::mod::sys::Spinlock notification_lock;
};

struct VirtIONetDevice {
    ker::net::NetDevice netdev;  // Embedded, first member for casting
    ker::dev::pci::PCIDevice* pci{};
    std::array<VirtIONetQueuePair, VIRTIO_NET_MAX_QUEUE_PAIRS> queue_pairs{};
    managed::Region control_buffer;
    bool callbacks_published{};
    lifecycle::Admission admission;
    // 0 live, 1 retiring, 2 retired, 3 terminal failure. Never reprobe/reopen.
    std::atomic<uint8_t> retirement{0};
    VirtIONetDevice* failed_probe_next{};
    Virtqueue* ctrlq{};          // Control queue (activated when MQ is negotiated)
    uint16_t io_base{};          // BAR0 I/O port base (legacy path)
    uint8_t num_queue_pairs{1};  // Active queue pairs after MQ negotiation
    uint8_t configured_queue_pairs{1};
    uint8_t hdr_size{};  // sizeof negotiated virtio-net header
    uint32_t negotiated_features{};
    bool msix_enabled{};
    bool use_intx{};

    // Modern virtio (non-null when using virtio 1.0 MMIO interface)
    VirtioModernCfg* modern_cfg{};
    volatile uint8_t* notify_base{};
    uint32_t notify_off_multiplier{};
    volatile uint8_t* device_cfg_base{};
};

#ifdef WOS_SELFTEST
struct SelftestProbeResult {
    bool reject_platform_requested = false;
    bool platform_admission_closed = false;
    bool withhold_reset_proof = false;
    bool packets_requested = false;
    bool terminal_requested = false;
    bool terminal_published = false;
    bool terminal_late_rejected = false;
    bool terminal_irq_drained = false;
    bool terminal_repeat_stable = false;
    uint8_t packets_verified = 0;
    size_t packet_pool_before = 0, packet_pool_after = 0;
    bool found = false;
    bool prepared = false;
    bool bme_enabled = false;
    bool control_present = false;
    bool control_ack = false;
    bool reset_observed = false;
    bool bme_disabled = false;
    dma::Stats before{}, mapped{}, after{};
};
// Boot-only, before this NIC's IRQ/NAPI/netdev publication. Withheld proof is
// an injected retirement failure, not an emulated device reset failure.
auto virtio_selftest_probe_unwind() -> SelftestProbeResult;
#endif

auto virtio_net_init() -> int;
// Sleepable task or serialized pre-task BSP; never from an admitted callback.
// Retains closed device/NAPI objects for late raw pointers; retires DMA only.
auto virtio_net_shutdown() -> int;
auto virtio_net_diag_snapshot(VirtIONetDiagSnapshot* out, size_t max) -> size_t;

}  // namespace ker::dev::virtio
