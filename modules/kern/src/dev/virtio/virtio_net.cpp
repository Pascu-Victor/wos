#include "virtio_net.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
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

// WKI remotable ops for VirtIO-Net devices
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

// Get physical address from kernel virtual address
// Handles both HHDM-mapped addresses and kernel static (.bss/.data) addresses
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    if (addr >= 0xffffffff80000000ULL) {
        // Kernel static mapping — requires page table walk
        return ker::mod::mm::virt::translate(ker::mod::mm::virt::getKernelPagemap(), addr);
    }
    // HHDM mapping — simple offset subtraction
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(addr));
}

// Fill RX virtqueue with packet buffers
void fill_rx_queue(VirtIONetDevice* dev) {
    while (dev->rxq->num_free > 0) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }
        // Reset data pointer to start of storage (we need space for virtio-net header + data)
        pkt->data = pkt->storage.data();
        pkt->len = 0;

        uint64_t phys = virt_to_phys(pkt->storage.data());
        // Buffer receives virtio-net header + ethernet frame
        uint32_t buf_len = ker::net::PKT_BUF_SIZE;

        virtq_add_buf(dev->rxq, phys, buf_len, VRING_DESC_F_WRITE, pkt);
    }
    virtq_kick(dev->rxq);
}

// Process received packets from the used ring (budget-limited for NAPI)
// Returns number of packets processed
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

        // Skip virtio-net header, set data to point past it
        pkt->data = pkt->storage.data() + VIRTIO_NET_HDR_SIZE;
        pkt->len = len - VIRTIO_NET_HDR_SIZE;
        pkt->dev = &dev->netdev;

        // Deliver to network stack
        ker::net::netdev_rx(&dev->netdev, pkt);
        processed++;
    }

    // Refill RX queue
    fill_rx_queue(dev);

    return processed;
}

// Process completed TX buffers
void process_tx(VirtIONetDevice* dev) {
    uint32_t len = 0;
    uint16_t desc_idx = 0;

    while ((desc_idx = virtq_get_buf(dev->txq, &len)) != 0xFFFF) {
        auto* pkt = dev->txq->pkt_map[desc_idx];
        dev->txq->pkt_map[desc_idx] = nullptr;
        if (pkt != nullptr) {
            ker::net::pkt_free(pkt);
        }
    }
}

// Disable device interrupts (for NAPI - called from IRQ handler)
void virtio_net_irq_disable(VirtIONetDevice* dev) {
    if (dev->msix_enabled) {
        // MSI-X: disable by setting vectors to NO_VECTOR
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, VIRTIO_MSI_NO_VECTOR);
    }
    // Legacy/MSI: no per-device interrupt disable, rely on NAPI state machine
}

// Enable device interrupts (for NAPI - called from poll when complete)
void virtio_net_irq_enable(VirtIONetDevice* dev) {
    if (dev->msix_enabled) {
        // MSI-X: re-enable by setting vectors back to 0
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        ::outw(dev->io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(dev->io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
    }
}

// NAPI poll function - called from worker thread context
int virtio_net_poll(ker::net::NapiStruct* napi, int budget) {
    auto* dev = static_cast<VirtIONetDevice*>(napi->dev->private_data);
    int processed = 0;

    // Process RX with regular lock (we're in thread context, not IRQ)
    dev->rxq->lock.lock();
    processed = process_rx_budget(dev, budget);
    dev->rxq->lock.unlock();

    // Process TX completions
    dev->txq->lock.lock();
    process_tx(dev);
    dev->txq->lock.unlock();

    // If we processed less than budget, we're done - re-enable IRQs
    if (processed < budget) {
        ker::net::napi_complete(napi);
        virtio_net_irq_enable(dev);
    }

    return processed;
}

// Minimal IRQ handler for virtio-net (NAPI model)
// Only acknowledges interrupt, disables further interrupts, and schedules poll
void virtio_net_irq(uint8_t vector, void* private_data) {
    (void)vector;

    auto* dev = static_cast<VirtIONetDevice*>(private_data);
    if (dev == nullptr) {
        return;
    }

    if (!dev->msix_enabled) {
        // Legacy/MSI: read ISR status to acknowledge and filter
        uint8_t isr = ::inb(dev->io_base + VIRTIO_REG_ISR_STATUS);
        if (isr == 0) {
            return;  // Not our interrupt
        }
    }

    // Disable device interrupts to prevent further IRQs until poll completes
    virtio_net_irq_disable(dev);

    // Schedule NAPI poll - all real work happens in worker thread
    // This is IRQ-safe: only uses atomics and rescheduleTaskForCpu
    ker::net::napi_schedule(&dev->napi);
}

// NetDevice operations for virtio-net
int virtio_net_open(ker::net::NetDevice* netdev) {
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

int virtio_net_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) {
    auto* dev = static_cast<VirtIONetDevice*>(netdev->private_data);
    if (dev == nullptr || pkt == nullptr) {
        return -1;
    }

    dev->txq->lock.lock();

    // Check for free descriptors
    if (dev->txq->num_free == 0) {
        // Try to reclaim completed TX buffers first
        process_tx(dev);
        if (dev->txq->num_free == 0) {
            dev->txq->lock.unlock();
            netdev->tx_dropped++;
            ker::net::pkt_free(pkt);
            return -1;
        }
    }

    // Prepend virtio-net header
    auto* hdr = reinterpret_cast<VirtIONetHeader*>(pkt->push(VIRTIO_NET_HDR_SIZE));
    std::memset(hdr, 0, VIRTIO_NET_HDR_SIZE);

    uint64_t phys = virt_to_phys(pkt->data);
    auto total_len = static_cast<uint32_t>(pkt->len);

    int ret = virtq_add_buf(dev->txq, phys, total_len, 0, pkt);
    if (ret < 0) {
        dev->txq->lock.unlock();
        netdev->tx_dropped++;
        ker::net::pkt_free(pkt);
        return -1;
    }

    virtq_kick(dev->txq);
    netdev->tx_packets++;
    netdev->tx_bytes += total_len - VIRTIO_NET_HDR_SIZE;

    dev->txq->lock.unlock();
    return 0;
}

void virtio_net_set_mac(ker::net::NetDevice*, const uint8_t*) {
    // MAC is read-only for virtio-net legacy
}

ker::net::NetDeviceOps virtio_net_ops = {
    .open = virtio_net_open,
    .close = virtio_net_close,
    .start_xmit = virtio_net_start_xmit,
    .set_mac = virtio_net_set_mac,
};

// Write device status byte
void write_status(uint16_t io_base, uint8_t status) { ::outb(io_base + VIRTIO_REG_DEVICE_STATUS, status); }

// Initialize a single virtio-net PCI device
auto init_device(ker::dev::pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_VIRTIO_NET_DEVICES) {
        return -1;
    }

    auto io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3U);
    if (io_base == 0) {
        ker::mod::io::serial::write("virtio-net: BAR0 is zero\n");
        return -1;
    }

    // Enable PCI bus mastering and I/O space
    ker::dev::pci::pci_enable_bus_master(pci_dev);
    uint16_t cmd = ker::dev::pci::pci_config_read16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND);
    cmd |= ker::dev::pci::PCI_COMMAND_IO_SPACE;
    ker::dev::pci::pci_config_write16(pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND, cmd);

    // === VirtIO device initialization sequence (legacy) ===

    // 1. Reset device
    write_status(io_base, 0);

    // 2. Acknowledge the device
    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE);

    // 3. Set DRIVER status
    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER);

    // 4. Read device features
    uint32_t dev_features = ::inl(io_base + VIRTIO_REG_DEVICE_FEATURES);

    // 5. Negotiate features (we want MAC, status; no checksum offload for simplicity)
    uint32_t our_features = 0;
    if ((dev_features & VIRTIO_NET_F_MAC) != 0) {
        our_features |= VIRTIO_NET_F_MAC;
    }
    if ((dev_features & VIRTIO_NET_F_STATUS) != 0) {
        our_features |= VIRTIO_NET_F_STATUS;
    }
    ::outl(io_base + VIRTIO_REG_GUEST_FEATURES, our_features);

    // 6. Allocate VirtIONetDevice
    auto* dev = static_cast<VirtIONetDevice*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(VirtIONetDevice)));
    if (dev == nullptr) {
        write_status(io_base, VIRTIO_STATUS_FAILED);
        return -1;
    }
    dev->pci = pci_dev;
    dev->io_base = io_base;
    dev->negotiated_features = our_features;

    // 7. Set up virtqueues
    // Select queue 0 (RX)
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

    // Tell device the physical address of the queue (page-aligned, divided by 4096)
    uint64_t rxq_phys = virt_to_phys(dev->rxq->desc);
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR, static_cast<uint32_t>(rxq_phys / 4096));

    // Select queue 1 (TX)
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

    // 8. Set up IRQ — try MSI-X first (virtio on QEMU only exposes MSI-X, not MSI)
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

        // Tell the device which MSI-X table entry to use for config changes
        ::outw(io_base + VIRTIO_MSI_CONFIG_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_CONFIG_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X config vector rejected\n");
            dev->msix_enabled = false;
        }
    }

    if (dev->msix_enabled) {
        // Set MSI-X vector for RX queue (queue 0)
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 0);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X RX vector rejected\n");
            dev->msix_enabled = false;
        }

        // Set MSI-X vector for TX queue (queue 1)
        ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
        ::outw(io_base + VIRTIO_MSI_QUEUE_VECTOR, 0);
        if (::inw(io_base + VIRTIO_MSI_QUEUE_VECTOR) == VIRTIO_MSI_NO_VECTOR) {
            ker::mod::io::serial::write("virtio-net: MSI-X TX vector rejected\n");
            dev->msix_enabled = false;
        }
    }

    if (!dev->msix_enabled) {
        // MSI-X not available or rejected — try MSI, then fall back to legacy INTx
        int msi_ret = ker::dev::pci::pci_enable_msi(pci_dev, vector);
        if (msi_ret != 0) {
            // Fall back to legacy interrupt
            vector = pci_dev->interrupt_line + 32;  // IRQ to vector mapping
            dev->irq_vector = vector;
            ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
        }
    }

    // 9. Read MAC address (offset shifts by 4 when MSI-X vector fields are present)
    uint16_t mac_off = dev->msix_enabled ? VIRTIO_NET_CFG_MAC_MSIX : VIRTIO_NET_CFG_MAC;
    if ((our_features & VIRTIO_NET_F_MAC) != 0) {
        for (int i = 0; i < 6; i++) {
            dev->netdev.mac[i] = ::inb(io_base + mac_off + i);
        }
    }

    ker::mod::gates::requestIrq(vector, virtio_net_irq, dev, "virtio-net");

    // 10. Set DRIVER_OK status
    write_status(io_base, VIRTIO_STATUS_ACKNOWLEDGE | VIRTIO_STATUS_DRIVER | VIRTIO_STATUS_DRIVER_OK);

    // 11. Fill RX queue with buffers
    fill_rx_queue(dev);

    // 12. Register as a network device
    dev->netdev.ops = &virtio_net_ops;
    dev->netdev.mtu = 1500;
    dev->netdev.state = 1;
    dev->netdev.private_data = dev;
    dev->netdev.name[0] = '\0';  // Auto-assign name
    dev->netdev.remotable = &s_remotable_ops;

    ker::net::netdev_register(&dev->netdev);

    // 13. Initialize and enable NAPI for deferred packet processing
    ker::net::napi_init(&dev->napi, &dev->netdev, virtio_net_poll, 64);
    ker::net::napi_enable(&dev->napi);

    devices[device_count++] = dev;

    // Log success
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

    // Scan all PCI devices for virtio-net
    size_t count = ker::dev::pci::pci_device_count();
    for (size_t i = 0; i < count; i++) {
        auto* dev = ker::dev::pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id != VIRTIO_VENDOR) {
            continue;
        }

        // Check for legacy virtio-net (device ID 0x1000 + subsystem for net)
        // or modern virtio-net (device ID 0x1041)
        bool is_net = false;
        if (dev->device_id == VIRTIO_NET_LEGACY) {
            // Legacy device: subsystem device ID determines type
            // For legacy, device_id 0x1000 with class=network is a net device
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
