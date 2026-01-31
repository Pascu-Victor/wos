#include "virtio_net.hpp"

#include <cstring>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <platform/acpi/ioapic/ioapic.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::virtio {

namespace {
constexpr size_t MAX_VIRTIO_NET_DEVICES = 4;
VirtIONetDevice* devices[MAX_VIRTIO_NET_DEVICES] = {};
size_t device_count = 0;

// Get physical address from kernel virtual address
// Handles both HHDM-mapped addresses and kernel static (.bss/.data) addresses
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    if (addr >= 0xffffffff80000000ULL) {
        // Kernel static mapping — requires page table walk
        return ker::mod::mm::virt::translate(
            ker::mod::mm::virt::getKernelPagemap(), addr);
    }
    // HHDM mapping — simple offset subtraction
    return reinterpret_cast<uint64_t>(
        ker::mod::mm::addr::getPhysPointer(addr));
}

// Fill RX virtqueue with packet buffers
void fill_rx_queue(VirtIONetDevice* dev) {
    while (dev->rxq->num_free > 0) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }
        // Reset data pointer to start of storage (we need space for virtio-net header + data)
        pkt->data = pkt->storage;
        pkt->len = 0;

        uint64_t phys = virt_to_phys(pkt->storage);
        // Buffer receives virtio-net header + ethernet frame
        uint32_t buf_len = ker::net::PKT_BUF_SIZE;

        virtq_add_buf(dev->rxq, phys, buf_len, VRING_DESC_F_WRITE, pkt);
    }
    virtq_kick(dev->rxq);
}

// Process received packets from the used ring
void process_rx(VirtIONetDevice* dev) {
    uint32_t len = 0;
    uint16_t desc_idx;

    while ((desc_idx = virtq_get_buf(dev->rxq, &len)) != 0xFFFF) {
        auto* pkt = dev->rxq->pkt_map[desc_idx];
        dev->rxq->pkt_map[desc_idx] = nullptr;

        if (pkt == nullptr || len <= VIRTIO_NET_HDR_SIZE) {
            if (pkt != nullptr) {
                ker::net::pkt_free(pkt);
            }
            continue;
        }

        // Skip virtio-net header, set data to point past it
        pkt->data = pkt->storage + VIRTIO_NET_HDR_SIZE;
        pkt->len = len - VIRTIO_NET_HDR_SIZE;
        pkt->dev = &dev->netdev;

        // Deliver to network stack
        ker::net::netdev_rx(&dev->netdev, pkt);
    }

    // Refill RX queue
    fill_rx_queue(dev);
}

// Process completed TX buffers
void process_tx(VirtIONetDevice* dev) {
    uint32_t len = 0;
    uint16_t desc_idx;

    while ((desc_idx = virtq_get_buf(dev->txq, &len)) != 0xFFFF) {
        auto* pkt = dev->txq->pkt_map[desc_idx];
        dev->txq->pkt_map[desc_idx] = nullptr;
        if (pkt != nullptr) {
            ker::net::pkt_free(pkt);
        }
    }
}

// IRQ handler for virtio-net
void virtio_net_irq(uint8_t, void* private_data) {
    auto* dev = static_cast<VirtIONetDevice*>(private_data);
    if (dev == nullptr) {
        return;
    }

    // Read ISR status to acknowledge interrupt
    uint8_t isr = ::inb(dev->io_base + VIRTIO_REG_ISR_STATUS);
    if (isr == 0) {
        return;  // Not our interrupt
    }

    // Bit 0: used buffer notification
    if ((isr & 1) != 0) {
        process_rx(dev);
        process_tx(dev);
    }

    // Bit 1: configuration change (ignored for now)
}

// NetDevice operations for virtio-net
int virtio_net_open(ker::net::NetDevice* netdev) {
    netdev->state = 1;
    return 0;
}

void virtio_net_close(ker::net::NetDevice* netdev) {
    netdev->state = 0;
}

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
    uint32_t total_len = static_cast<uint32_t>(pkt->len);

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
void write_status(uint16_t io_base, uint8_t status) {
    ::outb(io_base + VIRTIO_REG_DEVICE_STATUS, status);
}

// Initialize a single virtio-net PCI device
auto init_device(ker::dev::pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_VIRTIO_NET_DEVICES) {
        return -1;
    }

    uint16_t io_base = static_cast<uint16_t>(pci_dev->bar[0] & ~0x3u);
    if (io_base == 0) {
        ker::mod::io::serial::write("virtio-net: BAR0 is zero\n");
        return -1;
    }

    // Enable PCI bus mastering and I/O space
    ker::dev::pci::pci_enable_bus_master(pci_dev);
    uint16_t cmd = ker::dev::pci::pci_config_read16(
        pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND);
    cmd |= ker::dev::pci::PCI_COMMAND_IO_SPACE;
    ker::dev::pci::pci_config_write16(
        pci_dev->bus, pci_dev->slot, pci_dev->function, ker::dev::pci::PCI_COMMAND, cmd);

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
    auto* dev = static_cast<VirtIONetDevice*>(
        ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(VirtIONetDevice)));
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
    if (rxq_size > VIRTQ_MAX_SIZE) {
        rxq_size = VIRTQ_MAX_SIZE;
    }

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
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR,
                              static_cast<uint32_t>(rxq_phys / 4096));

    // Select queue 1 (TX)
    ::outw(io_base + VIRTIO_REG_QUEUE_SELECT, 1);
    uint16_t txq_size = ::inw(io_base + VIRTIO_REG_QUEUE_SIZE);
    if (txq_size == 0) {
        ker::mod::io::serial::write("virtio-net: TX queue size is 0\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    if (txq_size > VIRTQ_MAX_SIZE) {
        txq_size = VIRTQ_MAX_SIZE;
    }

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
    ::outl(io_base + VIRTIO_REG_QUEUE_ADDR,
                              static_cast<uint32_t>(txq_phys / 4096));

    // 8. Read MAC address
    if ((our_features & VIRTIO_NET_F_MAC) != 0) {
        for (int i = 0; i < 6; i++) {
            dev->netdev.mac[i] = ::inb(io_base + VIRTIO_NET_CFG_MAC + i);
        }
    }

    // 9. Set up IRQ
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector == 0) {
        ker::mod::io::serial::write("virtio-net: no free IRQ vector\n");
        write_status(io_base, VIRTIO_STATUS_FAILED);
        ker::mod::mm::dyn::kmalloc::free(dev);
        return -1;
    }
    dev->irq_vector = vector;

    // Try MSI first, fall back to legacy INTx
    int msi_ret = ker::dev::pci::pci_enable_msi(pci_dev, vector);
    if (msi_ret != 0) {
        // Fall back to legacy interrupt
        // The interrupt_line from PCI config is the legacy IRQ number (GSI)
        vector = pci_dev->interrupt_line + 32;  // IRQ to vector mapping
        dev->irq_vector = vector;
        // Configure the IOAPIC to deliver this GSI as the chosen vector
        ker::mod::ioapic::route_irq(pci_dev->interrupt_line, vector, 0);
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

    ker::net::netdev_register(&dev->netdev);

    devices[device_count++] = dev;

    // Log success
    ker::mod::io::serial::write("virtio-net: ");
    ker::mod::io::serial::write(dev->netdev.name);
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
    ker::mod::io::serial::write(" ready\n");

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
