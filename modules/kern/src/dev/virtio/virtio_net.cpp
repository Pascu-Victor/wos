#include "virtio_net.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/net_trace.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/wki/remotable.hpp>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>

#include "dev/pci.hpp"
#include "dev/virtio/virtio.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::dev::virtio {

namespace {
auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    (void)node_id;
    ker::mod::io::serial::write("virtio-net: remote attach\n");
    return 0;
}
void remotable_on_detach(uint16_t node_id) {
    (void)node_id;
    ker::mod::io::serial::write("virtio-net: remote detach\n");
}
void remotable_on_fault(uint16_t node_id) {
    (void)node_id;
    ker::mod::io::serial::write("virtio-net: remote fault\n");
}
const ker::net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

constexpr size_t MAX_VIRTIO_NET_DEVICES = 4;
std::array<VirtIONetDevice*, MAX_VIRTIO_NET_DEVICES> devices = {};
size_t device_count = 0;

// Handle both kernel static and HHDM mappings.
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    if (addr >= mod::mm::addr::get_hhdm_offset()) {
        uint64_t phys = ker::mod::mm::virt::translate(ker::mod::mm::virt::getKernelPagemap(), addr);
        if (phys == ker::mod::mm::virt::PADDR_INVALID) {
            ker::mod::dbg::log("virtio_net: virt_to_phys failed for kernel address 0x%lx", addr);
            hcf();
        }
        return phys;
    }
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(addr));
}

void fill_rx_queue_for(Virtqueue* rxq) {
    while (rxq->num_free > 0) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }
        pkt->data = pkt->storage.data();
        pkt->len = 0;

        uint64_t phys = virt_to_phys(pkt->storage.data());
        uint32_t buf_len = ker::net::PKT_BUF_SIZE;

        virtq_add_buf(rxq, phys, buf_len, VRING_DESC_F_WRITE, pkt);
    }
    virtq_kick(rxq);
}

auto process_rx_budget_for(VirtIONetDevice* dev, Virtqueue* rxq, int budget) -> int {
    int processed = 0;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while (processed < budget && (desc_idx = virtq_get_buf(rxq, &len)) != 0xFFFF) {
        auto* pkt = rxq->pkt_map[desc_idx];
        rxq->pkt_map[desc_idx] = nullptr;

        if (pkt == nullptr || len <= VIRTIO_NET_HDR_SIZE) {
            if (pkt != nullptr) {
                ker::net::pkt_free(pkt);
            }
            continue;
        }

        pkt->data = pkt->storage.data() + VIRTIO_NET_HDR_SIZE;
        pkt->len = len - VIRTIO_NET_HDR_SIZE;
        pkt->dev = &dev->netdev;

        ker::net::netdev_rx(&dev->netdev, pkt);
        processed++;
    }

    fill_rx_queue_for(rxq);

    return processed;
}

auto process_rx_budget(VirtIONetDevice* dev, int budget) -> int { return process_rx_budget_for(dev, dev->rxq, budget); }

// Collect completed TX buffers from a specific queue; caller frees after releasing lock.
auto collect_tx_for(Virtqueue* txq) -> ker::net::PacketBuffer* {
    ker::net::PacketBuffer* head = nullptr;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while ((desc_idx = virtq_get_buf(txq, &len)) != 0xFFFF) {
        auto* pkt = txq->pkt_map[desc_idx];
        txq->pkt_map[desc_idx] = nullptr;
        if (pkt != nullptr) {
            pkt->next = head;
            head = pkt;
        }
    }
    return head;
}

auto collect_tx(VirtIONetDevice* dev) -> ker::net::PacketBuffer* { return collect_tx_for(dev->txq); }

void free_pkt_list(ker::net::PacketBuffer* head) {
    while (head != nullptr) {
        auto* next = head->next;
        ker::net::pkt_free(head);
        head = next;
    }
}

// Per-queue-pair MSI-X vector control.  `pair` is 0 or 1; the MSI-X entry
// index matches the pair index so pair 0 -> entry 0, pair 1 -> entry 1.
void virtio_net_irq_disable_pair(VirtIONetDevice* dev, uint8_t pair) {
    if (!dev->msix_enabled) {
        return;
    }
    uint16_t rx_q = pair * 2;
    uint16_t tx_q = (pair * 2) + 1;
    uint64_t flags = dev->irq_lock.lock_irqsave();
    ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, rx_q);
    ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
    ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, tx_q);
    ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
    dev->irq_lock.unlock_irqrestore(flags);
}

void virtio_net_irq_enable_pair(VirtIONetDevice* dev, uint8_t pair) {
    if (!dev->msix_enabled) {
        return;
    }
    uint16_t rx_q = pair * 2;
    uint16_t tx_q = (pair * 2) + 1;
    uint16_t entry = pair;  // MSI-X entry index matches pair index
    uint64_t flags = dev->irq_lock.lock_irqsave();
    ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, rx_q);
    ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, entry);
    ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, tx_q);
    ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, entry);
    dev->irq_lock.unlock_irqrestore(flags);
}

int virtio_net_poll(ker::net::NapiStruct* napi, int budget) {
    NET_TRACE_SPAN(SPAN_NAPI_POLL);
    auto* dev = static_cast<VirtIONetDevice*>(napi->dev->private_data);
    int processed = 0;

    // Reclaim TX buffers early to return PacketBuffers to the pool.
    {
        NET_TRACE_SPAN(SPAN_TX_RECLAIM);
        uint64_t txflags = dev->txq->lock.lock_irqsave();
        auto* reclaimed = collect_tx(dev);
        dev->txq->lock.unlock_irqrestore(txflags);
        free_pkt_list(reclaimed);
    }

    {
        NET_TRACE_SPAN(SPAN_RX_BUDGET);
        processed = process_rx_budget(dev, budget);
    }

    if (processed < budget) {
        ker::net::napi_complete(napi);
        virtio_net_irq_enable_pair(dev, 0);

        // Handle missed notification races across IRQ re-enable.
        if (virtq_has_pending(dev->rxq)) {
            virtio_net_irq_disable_pair(dev, 0);
            ker::net::napi_schedule(napi);
        }
    }

    return processed;
}

void virtio_net_irq(uint8_t vector, void* private_data) {
    (void)vector;

    auto* dev = static_cast<VirtIONetDevice*>(private_data);
    if (dev == nullptr) {
        return;
    }

    if (!dev->msix_enabled) {
        uint8_t isr = ::inb(dev->io_base + VIRTIO_REG_ISR_STATUS);
        if (isr == 0) {
            return;
        }
    }

    // Always defer packet processing to NAPI worker context.
    virtio_net_irq_disable_pair(dev, 0);
    ker::net::napi_schedule(&dev->napi);
}

// IRQ handler for queue pair 1 MSI-X entry (steered to CPU 2).
void virtio_net_irq2(uint8_t vector, void* private_data) {
    (void)vector;
    auto* dev = static_cast<VirtIONetDevice*>(private_data);
    if (dev == nullptr) {
        return;
    }
    virtio_net_irq_disable_pair(dev, 1);
    ker::net::napi_schedule(&dev->napi2);
}

// NAPI poll for queue pair 1 (rxq2 / txq2).
auto virtio_net_poll2(ker::net::NapiStruct* napi, int budget) -> int {
    NET_TRACE_SPAN(SPAN_NAPI_POLL);
    auto* dev = static_cast<VirtIONetDevice*>(napi->dev->private_data);
    int processed = 0;

    {
        NET_TRACE_SPAN(SPAN_TX_RECLAIM);
        uint64_t txflags = dev->txq2->lock.lock_irqsave();
        auto* reclaimed = collect_tx_for(dev->txq2);
        dev->txq2->lock.unlock_irqrestore(txflags);
        free_pkt_list(reclaimed);
    }

    {
        NET_TRACE_SPAN(SPAN_RX_BUDGET);
        processed = process_rx_budget_for(dev, dev->rxq2, budget);
    }

    if (processed < budget) {
        ker::net::napi_complete(napi);
        virtio_net_irq_enable_pair(dev, 1);

        if (virtq_has_pending(dev->rxq2)) {
            virtio_net_irq_disable_pair(dev, 1);
            ker::net::napi_schedule(napi);
        }
    }

    return processed;
}

// Send VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET(2) over the control queue to activate
// queue pair 1.  Busy-polls the used ring for the ACK.  Must be called after
// VIRTIO_STATUS_DRIVER_OK is written to the device.  Returns true on success.
auto send_mq_ctrl_cmd(VirtIONetDevice* dev) -> bool {
    if (dev->ctrlq == nullptr) {
        return false;
    }

    // Allocate a DMA-accessible page: [class][cmd][pairs_lo][pairs_hi][ack]
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::phys::pageAlloc(4096));
    if (buf == nullptr) {
        return false;
    }
    buf[0] = VIRTIO_NET_CTRL_CLASS_MQ;
    buf[1] = VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET;
    buf[2] = 2;     // num_pairs = 2, little-endian low byte
    buf[3] = 0;     // num_pairs high byte
    buf[4] = 0xFF;  // ack placeholder - device writes VIRTIO_NET_OK (0) on success

    uint64_t phys = virt_to_phys(buf);

    // Chain three descriptors: [header(2B) | data(2B) | ack(1B,writable)]
    auto* ctrlq = dev->ctrlq;
    ctrlq->desc[0].addr = phys;
    ctrlq->desc[0].len = 2;
    ctrlq->desc[0].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[0].next = 1;
    ctrlq->desc[1].addr = phys + 2;
    ctrlq->desc[1].len = 2;
    ctrlq->desc[1].flags = VRING_DESC_F_NEXT;
    ctrlq->desc[1].next = 2;
    ctrlq->desc[2].addr = phys + 4;
    ctrlq->desc[2].len = 1;
    ctrlq->desc[2].flags = VRING_DESC_F_WRITE;
    ctrlq->desc[2].next = 0;

    ctrlq->avail->ring[0] = 0;  // descriptor chain head
    __atomic_thread_fence(__ATOMIC_RELEASE);
    ctrlq->avail->idx = 1;

    virtq_kick(ctrlq);
    constexpr int MAX_SPINS = 5000000;
    for (int spin = 0; spin < MAX_SPINS; spin++) {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        if (ctrlq->used->idx != 0) {
            return buf[4] == VIRTIO_NET_OK;  // buf leaked intentionally (init-time, 1 page)
        }
    }
    ker::mod::io::serial::write("virtio-net: ctrl-queue MQ ack timeout\n");
    return false;
}

auto virtio_net_open(ker::net::NetDevice* netdev) -> int {
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

auto virtio_net_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) -> int {
    NET_TRACE_SPAN(SPAN_START_XMIT);
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr || pkt == nullptr) {
        return -1;
    }

    // Prevent preemption while holding txq->lock.
    uint64_t txflags = dev->txq->lock.lock_irqsave();

    ker::net::PacketBuffer* reclaimed = nullptr;
    if (dev->txq->num_free == 0) {
        reclaimed = collect_tx(dev);
        if (dev->txq->num_free == 0) {
            dev->txq->lock.unlock_irqrestore(txflags);
            free_pkt_list(reclaimed);
            netdev->tx_dropped++;
            ker::mod::dbg::log("[net] TX RING FULL drop #%lu pool_free=%zu", static_cast<unsigned long>(netdev->tx_dropped),
                               ker::net::pkt_pool_free_count());
            ker::net::pkt_free(pkt);
            return -1;
        }
    }

    auto* hdr = reinterpret_cast<VirtIONetHeader*>(pkt->push(VIRTIO_NET_HDR_SIZE));
    std::memset(hdr, 0, VIRTIO_NET_HDR_SIZE);

    uint64_t phys = virt_to_phys(pkt->data);
    auto total_len = static_cast<uint32_t>(pkt->len);

    int ret = virtq_add_buf(dev->txq, phys, total_len, 0, pkt);
    if (ret < 0) {
        dev->txq->lock.unlock_irqrestore(txflags);
        free_pkt_list(reclaimed);
        netdev->tx_dropped++;
        ker::net::pkt_free(pkt);
        return -1;
    }

    virtq_kick(dev->txq);
    netdev->tx_packets++;
    netdev->tx_bytes += total_len - VIRTIO_NET_HDR_SIZE;

    dev->txq->lock.unlock_irqrestore(txflags);
    free_pkt_list(reclaimed);
    return 0;
}

auto virtio_net_set_queue_cpu(ker::net::NetDevice* net, uint32_t pair_idx, uint64_t cpu) -> int {
    auto* dev = static_cast<VirtIONetDevice*>(net->private_data);
    if (pair_idx >= dev->num_queue_pairs) {
        return -EINVAL;
    }
    if (pair_idx == 0) {
        ker::net::napi_set_worker_cpu(&dev->napi, cpu);
        if (dev->msix_enabled) {
            ker::dev::pci::pci_configure_msix_entry(dev->pci, 0, dev->irq_vector, cpu);
        }
    } else if (pair_idx == 1) {
        ker::net::napi_set_worker_cpu(&dev->napi2, cpu);
        if (dev->msix_enabled && dev->irq_vector2 != 0) {
            ker::dev::pci::pci_configure_msix_entry(dev->pci, 1, dev->irq_vector2, cpu);
        }
    }
    return 0;
}

void virtio_net_set_mac(ker::net::NetDevice* netdev, const uint8_t* mac) {
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr || mac == nullptr) {
        return;
    }

    if ((dev->negotiated_features & VIRTIO_NET_F_MAC) == 0) {
        return;
    }

    uint16_t mac_off = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    for (size_t i = 0; i < netdev->mac.size(); i++) {
        ::outb(dev->io_base + mac_off + static_cast<uint16_t>(i), mac[i]);
        netdev->mac[i] = mac[i];
    }
}

void write_status(uint16_t io_base, uint8_t status) { ::outb(io_base + VIRTIO_REG_DEVICE_STATUS, status); }

ker::net::NetDeviceOps virtio_net_ops = {
    .open = virtio_net_open,
    .close = virtio_net_close,
    .start_xmit = virtio_net_start_xmit,
    .set_mac = virtio_net_set_mac,
    .set_queue_cpu = virtio_net_set_queue_cpu,
};

auto init_device(ker::dev::pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_VIRTIO_NET_DEVICES) {
        return -1;
    }

    auto io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3U);
    if (io_base == 0) {
        ker::mod::io::serial::write("virtio-net: BAR0 is zero\n");
        return -1;
    }

    ker::dev::pci::pci_enable_bus_master(pci_dev);
    uint16_t cmd = ker::dev::pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND);
    cmd |= ker::dev::pci::PCI_COMMAND_IO_SPACE;
    ker::dev::pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND, cmd);

    write_status(io_base, 0);

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE);

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER);

    uint32_t dev_features = ::inl(io_base + VIRTIO_REG_DEVICE_FEATURES);
    uint64_t core_count = ker::mod::smt::get_core_count();

    // Negotiate features: MAC, status, and multi-queue (MQ requires CTRL_VQ).
    uint32_t our_features = 0;
    if ((dev_features & VIRTIO_NET_F_MAC) != 0) {
        our_features |= VIRTIO_NET_F_MAC;
    }
    if ((dev_features & VIRTIO_NET_F_STATUS) != 0) {
        our_features |= VIRTIO_NET_F_STATUS;
    }
    bool want_mq = (core_count >= 2) && ((dev_features & VIRTIO_NET_F_CTRL_VQ) != 0) && ((dev_features & VIRTIO_NET_F_MQ) != 0);
    if (want_mq) {
        our_features |= VIRTIO_NET_F_CTRL_VQ | VIRTIO_NET_F_MQ;
    }
    ::outl(io_base + VIRTIO_REG_GUEST_FEATURES, our_features);

    auto* dev = static_cast<VirtIONetDevice*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(VirtIONetDevice)));
    if (dev == nullptr) {
        write_status(io_base, VIRTIO_STATUS_FAILED);
        return -1;
    }
    dev->pci = pci_dev;
    dev->io_base = io_base;
    dev->negotiated_features = our_features;

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
    uint16_t rxq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (rxq_size == 0) {
        ker::mod::io::serial::write("virtio-net: RX queue size is 0\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    rxq_size = std::min(rxq_size, VIRTQ_MAX_SIZE);

    dev->rxq = virtq_alloc(rxq_size);
    if (dev->rxq == nullptr) {
        ker::mod::io::serial::write("virtio-net: failed to alloc RX queue\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    dev->rxq->io_base = io_base;
    dev->rxq->queue_index = 0;

    uint64_t rxq_phys = virt_to_phys(dev->rxq->desc);
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(rxq_phys / 4096));

    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
    uint16_t txq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (txq_size == 0) {
        ker::mod::io::serial::write("virtio-net: TX queue size is 0\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    txq_size = std::min(txq_size, VIRTQ_MAX_SIZE);

    dev->txq = virtq_alloc(txq_size);
    if (dev->txq == nullptr) {
        ker::mod::io::serial::write("virtio-net: failed to alloc TX queue\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    dev->txq->io_base = io_base;
    dev->txq->queue_index = 1;

    uint64_t txq_phys = virt_to_phys(dev->txq->desc);
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(txq_phys / 4096));

    // Queue pair 1 (queues 2/3) + control queue (queue 4) - set up only when MQ desired.
    if (want_mq) {
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 2);
        uint16_t rxq2_size = std::min(::inw(io_base + VIRTIO_REG_QUEUE_SIZE), VIRTQ_MAX_SIZE);
        if (rxq2_size == 0) {
            want_mq = false;
        } else {
            dev->rxq2 = virtq_alloc(rxq2_size);
            if (dev->rxq2 == nullptr) {
                want_mq = false;
            }
        }
        if (want_mq) {
            dev->rxq2->io_base = io_base;
            dev->rxq2->queue_index = 2;
            uint64_t rxq2_phys = virt_to_phys(dev->rxq2->desc);
            ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(rxq2_phys / 4096));
        }
    }
    if (want_mq) {
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 3);
        uint16_t txq2_size = std::min(::inw(io_base + VIRTIO_REG_QUEUE_SIZE), VIRTQ_MAX_SIZE);
        if (txq2_size == 0) {
            want_mq = false;
        } else {
            dev->txq2 = virtq_alloc(txq2_size);
            if (dev->txq2 == nullptr) {
                want_mq = false;
            }
        }
        if (want_mq) {
            dev->txq2->io_base = io_base;
            dev->txq2->queue_index = 3;
            uint64_t txq2_phys = virt_to_phys(dev->txq2->desc);
            ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(txq2_phys / 4096));
        }
    }
    if (want_mq) {
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 4);  // control queue
        uint16_t ctrlq_size = std::min(::inw(io_base + VIRTIO_REG_QUEUE_SIZE), (uint16_t)32);
        if (ctrlq_size == 0) {
            want_mq = false;
        } else {
            dev->ctrlq = virtq_alloc(ctrlq_size);
            if (dev->ctrlq == nullptr) {
                want_mq = false;
            }
        }
        if (want_mq) {
            dev->ctrlq->io_base = io_base;
            dev->ctrlq->queue_index = 4;
            uint64_t ctrlq_phys = virt_to_phys(dev->ctrlq->desc);
            ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(ctrlq_phys / 4096));
        }
    }

    // Try MSI-X first (needed for per-pair CPU steering), then MSI, then INTx.
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector == 0) {
        ker::mod::io::serial::write("virtio-net: no free IRQ vector\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    dev->irq_vector = vector;

    // CPU steering: pin NAPI workers to the LAST two CPUs
    uint64_t net_cpu0 = (core_count > 1) ? (core_count - 1) : 0U;
    uint64_t net_cpu1 = (core_count >= 2) ? (core_count - 2) : net_cpu0;

    // Allocate a second IRQ vector for queue pair 1 if MQ is desired.
    uint8_t vector2 = 0;
    if (want_mq) {
        vector2 = ker::mod::gates::allocateVector();
        if (vector2 == 0) {
            ker::mod::io::serial::write("virtio-net: no second IRQ vector, disabling MQ\n");
            want_mq = false;
        } else {
            dev->irq_vector2 = vector2;
        }
    }

    int msix_ret = ker::dev::pci::pci_enable_msix(pci_dev, vector, net_cpu0);
    if (msix_ret == 0) {
        dev->msix_enabled = true;

        // Config vector: disabled (we don't handle config-change interrupts).
        ::outw(io_base + VIRTIO_MSI_CONFIG_VECTOR, VIRTIO_MSI_NO_VECTOR);

        // Configure MSI-X entry 1 for pair 1 (pair 0 was configured by pci_enable_msix).
        if (want_mq) {
            if (ker::dev::pci::pci_configure_msix_entry(pci_dev, 1, vector2, net_cpu1) != 0) {
                ker::mod::io::serial::write("virtio-net: MSI-X entry 1 failed, disabling MQ\n");
                want_mq = false;
            }
        }
    }

    // Assign MSI-X entry indices to individual queues.
    if (dev->msix_enabled) {
        // Pair 0: queues 0 (RX) and 1 (TX) -> entry 0
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X RX0 vector rejected\n");
            dev->msix_enabled = false;
        }
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X TX0 vector rejected\n");
            dev->msix_enabled = false;
        }

        // Pair 1: queues 2 (RX) and 3 (TX) -> entry 1
        if (dev->msix_enabled && want_mq) {
            ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 2);
            ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 1);
            if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
                ker::mod::io::serial::write("virtio-net: MSI-X RX1 rejected, disabling MQ\n");
                want_mq = false;
            }
        }
        if (dev->msix_enabled && want_mq) {
            ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 3);
            ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 1);
            if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
                ker::mod::io::serial::write("virtio-net: MSI-X TX1 rejected, disabling MQ\n");
                want_mq = false;
            }
        }
        // Control queue: polled, no interrupt vector.
        if (dev->ctrlq != nullptr) {
            ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 4);
            ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
        }
    }

    if (!dev->msix_enabled) {
        // MQ requires per-pair MSI-X vectors; downgrade to single-queue.
        want_mq = false;
        int msi_ret = ker::dev::pci::pci_enable_msi(pci_dev, vector, net_cpu0);
        if (msi_ret != 0) {
            vector = pci_dev->interrupt_line + 32;
            dev->irq_vector = vector;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    dev->num_queue_pairs = (want_mq && dev->msix_enabled) ? 2 : 1;

    // MAC offset depends on MSI-X vector fields.
    uint16_t mac_off = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    if ((our_features & VIRTIO_NET_F_MAC) != 0) {
        for (int i = 0; i < 6; i++) {
            dev->netdev.mac[i] = ::inb(io_base + mac_off + i);
        }
    }

    ker::mod::gates::requestIrq(vector, virtio_net_irq, dev, "virtio-net");
    if (dev->num_queue_pairs == 2 && dev->irq_vector2 != 0) {
        ker::mod::gates::requestIrq(dev->irq_vector2, virtio_net_irq2, dev, "virtio-net-1");
    }

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);

    fill_rx_queue_for(dev->rxq);
    if (dev->num_queue_pairs == 2) {
        fill_rx_queue_for(dev->rxq2);
        // Activate pair 1 in QEMU via the control queue.
        if (!send_mq_ctrl_cmd(dev)) {
            ker::mod::io::serial::write("virtio-net: MQ activation failed, downgrading to single-queue\n");
            virtio_net_irq_disable_pair(dev, 1);
            dev->num_queue_pairs = 1;
        }
    }

    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.mtu = 9000;
    dev->netdev.state = 1;
    dev->netdev.private_data = dev;
    dev->netdev.name[0] = '\0';
    dev->netdev.remotable = &S_REMOTABLE_OPS;

    ker::net::netdev_register(&dev->netdev);

    ker::net::napi_init(&dev->napi, &dev->netdev, virtio_net_poll, 64);
    ker::net::napi_enable(&dev->napi, net_cpu0);
    if (dev->num_queue_pairs == 2) {
        ker::net::napi_init(&dev->napi2, &dev->netdev, virtio_net_poll2, 64);
        ker::net::napi_enable(&dev->napi2, net_cpu1);
    }

    devices[device_count++] = dev;

    ker::mod::io::serial::write("virtio-net: ");
    ker::mod::io::serial::write(dev->netdev.name.data());
    ker::mod::io::serial::write(" MAC=");
    for (int i = 0; i < 6; i++) {
        if (i > 0) {
            ker::mod::io::serial::write(":");
        }
        ker::mod::io::serial::writeHex(dev->netdev.mac[i]);
    }
    ker::mod::io::serial::write(" rxq=");
    ker::mod::io::serial::writeHex(rxq_size);
    ker::mod::io::serial::write(" txq=");
    ker::mod::io::serial::writeHex(txq_size);
    ker::mod::io::serial::write(" vec=");
    ker::mod::io::serial::writeHex(vector);
    ker::mod::io::serial::write(dev->msix_enabled ? " msix" : " legacy");
    ker::mod::io::serial::write(" pairs=");
    ker::mod::io::serial::writeHex(dev->num_queue_pairs);
    ker::mod::io::serial::write(" napi ready\n");

    return 0;
}
}  // namespace

auto virtio_net_init() -> int {
    int found = 0;

    size_t count = ker::dev::pci::pci_device_count();
    for (size_t i = 0; i < count; i++) {
        auto* dev = ker::dev::pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id != VIRTIO_VENDOR) {
            continue;
        }

        bool is_net = false;
        if (dev->device_id == VIRTIO_NET_LEGACY) {
            if (dev->class_code == ker::dev::pci::PCI_CLASS_NETWORK) {
                is_net = true;
            }
        } else if (dev->device_id == VIRTIO_NET_MODERN) {
            is_net = true;
        }

        if (!is_net) {
            continue;
        }

        ker::mod::io::serial::write("virtio-net: found device at PCI ");
        ker::mod::io::serial::writeHex(dev->bus);
        ker::mod::io::serial::write(":");
        ker::mod::io::serial::writeHex(dev->slot);
        ker::mod::io::serial::write(".");
        ker::mod::io::serial::writeHex(dev->function);
        ker::mod::io::serial::write("\n");

        if (init_device(dev) == 0) {
            found++;
        }
    }

    if (found == 0) {
        ker::mod::io::serial::write("virtio-net: no devices found\n");
    }

    return found;
}

}  // namespace ker::dev::virtio
