#include "virtio_net.hpp"

#include <algorithm>
#include <array>
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
const ker::net::wki::RemotableOps s_remotable_ops = {
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
    if (addr >= 0xffffffff80000000ULL) {
        uint64_t phys = ker::mod::mm::virt::translate(ker::mod::mm::virt::getKernelPagemap(), addr);
        if (phys == ker::mod::mm::virt::PADDR_INVALID) {
            ker::mod::dbg::log("virtio_net: virt_to_phys failed for kernel address 0x%lx", addr);
            hcf();
        }
        return phys;
    }
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(addr));
}

void fill_rx_queue(VirtIONetDevice* dev) {
    while (dev->rxq->num_free > 0) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }
        pkt->data = pkt->storage.data();
        pkt->len = 0;

        uint64_t phys = virt_to_phys(pkt->storage.data());
        uint32_t buf_len = ker::net::PKT_BUF_SIZE;

        virtq_add_buf(dev->rxq, phys, buf_len, VRING_DESC_F_WRITE, pkt);
    }
    virtq_kick(dev->rxq);
}

int process_rx_budget(VirtIONetDevice* dev, int budget) {
    int processed = 0;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while (processed < budget && (desc_idx = virtq_get_buf(dev->rxq, &len)) != 0xFFFF) {
        auto* pkt = dev->rxq->pkt_map[desc_idx];
        dev->rxq->pkt_map[desc_idx] = nullptr;

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

    fill_rx_queue(dev);

    return processed;
}

// Collect completed TX buffers; caller frees after releasing txq->lock.
auto collect_tx(VirtIONetDevice* dev) -> ker::net::PacketBuffer* {
    ker::net::PacketBuffer* head = nullptr;
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while ((desc_idx = virtq_get_buf(dev->txq, &len)) != 0xFFFF) {
        auto* pkt = dev->txq->pkt_map[desc_idx];
        dev->txq->pkt_map[desc_idx] = nullptr;
        if (pkt != nullptr) {
            pkt->next = head;
            head = pkt;
        }
    }
    return head;
}

static void free_pkt_list(ker::net::PacketBuffer* head) {
    while (head != nullptr) {
        auto* next = head->next;
        ker::net::pkt_free(head);
        head = next;
    }
}

void virtio_net_irq_disable(VirtIONetDevice* dev) {
    if (dev->msix_enabled) {
        // Serialize QUEUE_SELECT + MSI_QUEUE_VECTOR programming.
        uint64_t flags = dev->irq_lock.lock_irqsave();
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
        dev->irq_lock.unlock_irqrestore(flags);
    }
}

void virtio_net_irq_enable(VirtIONetDevice* dev) {
    if (dev->msix_enabled) {
        uint64_t flags = dev->irq_lock.lock_irqsave();
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        dev->irq_lock.unlock_irqrestore(flags);
    }
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
        virtio_net_irq_enable(dev);

        // Handle missed notification races across IRQ re-enable.
        if (virtq_has_pending(dev->rxq)) {
            virtio_net_irq_disable(dev);
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
    virtio_net_irq_disable(dev);
    ker::net::napi_schedule(&dev->napi);
}

int virtio_net_open(ker::net::NetDevice* netdev) {
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

int virtio_net_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) {
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

void virtio_net_set_mac(ker::net::NetDevice*, const uint8_t*) {}

ker::net::NetDeviceOps virtio_net_ops = {
    .open = virtio_net_open,
    .close = virtio_net_close,
    .start_xmit = virtio_net_start_xmit,
    .set_mac = virtio_net_set_mac,
};

void write_status(uint16_t io_base, uint8_t status) { ::outb(io_base + VIRTIO_REG_DEVICE_STATUS, status); }

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

    // Negotiate MAC + status features.
    uint32_t our_features = 0;
    if ((dev_features & VIRTIO_NET_F_MAC) != 0) {
        our_features |= VIRTIO_NET_F_MAC;
    }
    if ((dev_features & VIRTIO_NET_F_STATUS) != 0) {
        our_features |= VIRTIO_NET_F_STATUS;
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

    // Try MSI-X first, then MSI, then INTx.
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector == 0) {
        ker::mod::io::serial::write("virtio-net: no free IRQ vector\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    dev->irq_vector = vector;

    int msix_ret = ker::dev::pci::pci_enable_msix(pci_dev, vector);
    if (msix_ret == 0) {
        dev->msix_enabled = true;

        ::outw(io_base + VIRTIO_MSI_CONFIG_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_CONFIG_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X config vector rejected\n");
            dev->msix_enabled = false;
        }
    }

    if (dev->msix_enabled) {
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X RX vector rejected\n");
            dev->msix_enabled = false;
        }

        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X TX vector rejected\n");
            dev->msix_enabled = false;
        }
    }

    if (!dev->msix_enabled) {
        int msi_ret = ker::dev::pci::pci_enable_msi(pci_dev, vector);
        if (msi_ret != 0) {
            vector = pci_dev->interrupt_line + 32;
            dev->irq_vector = vector;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    // MAC offset depends on MSI-X vector fields.
    uint16_t mac_off = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    if ((our_features & VIRTIO_NET_F_MAC) != 0) {
        for (int i = 0; i < 6; i++) {
            dev->netdev.mac[i] = ::inb(io_base + mac_off + i);
        }
    }

    ker::mod::gates::requestIrq(vector, virtio_net_irq, dev, "virtio-net");

    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);

    fill_rx_queue(dev);

    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.mtu = 9000;
    dev->netdev.state = 1;
    dev->netdev.private_data = dev;
    dev->netdev.name[0] = '\0';
    dev->netdev.remotable = &s_remotable_ops;

    ker::net::netdev_register(&dev->netdev);

    ker::net::napi_init(&dev->napi, &dev->netdev, virtio_net_poll, 64);
    ker::net::napi_enable(&dev->napi);

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
