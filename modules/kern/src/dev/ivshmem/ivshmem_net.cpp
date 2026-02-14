#include "ivshmem_net.hpp"

#include <array>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <net/packet.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>

namespace ker::dev::ivshmem {

// Forward declarations for NAPI
int ivshmem_poll(ker::net::NapiStruct* napi, int budget);

namespace {
constexpr size_t MAX_IVSHMEM_DEVICES = 2;
IvshmemNetDevice dev_pool[MAX_IVSHMEM_DEVICES] = {};
IvshmemNetDevice* devices[MAX_IVSHMEM_DEVICES] = {};
size_t device_count = 0;

// BAR0 register offsets (ivshmem-plain)
constexpr uint32_t IVSHMEM_REG_INTRMASK = 0x00;
constexpr uint32_t IVSHMEM_REG_INTRSTATUS = 0x04;
constexpr uint32_t IVSHMEM_REG_IVPOSITION = 0x08;
constexpr uint32_t IVSHMEM_REG_DOORBELL = 0x0C;

// -- IRQ enable/disable helpers --
void ivshmem_irq_disable(IvshmemNetDevice* dev) {
    // Mask all interrupts
    dev->regs[IVSHMEM_REG_INTRMASK / 4] = 0;
}

void ivshmem_irq_enable(IvshmemNetDevice* dev) {
    // Unmask all interrupts
    dev->regs[IVSHMEM_REG_INTRMASK / 4] = 0xFFFFFFFF;
}

// -- Ring buffer operations --

// Write a packet to the ring. Returns 0 on success, -1 if ring full.
int ring_write(RingBuffer* ring, const uint8_t* data, uint16_t len) {
    // Packet format: [len:u16][data][pad to 4 bytes]
    uint32_t pkt_size = 2 + len;
    uint32_t padded = (pkt_size + 3) & ~3u;

    uint32_t head = ring->head;
    uint32_t tail = ring->tail;
    uint32_t space;
    if (head >= tail) {
        space = ring->size - head + tail;
    } else {
        space = tail - head;
    }
    // Need space + 1 to distinguish full from empty
    if (padded + 1 > space) return -1;

    // Write length
    uint32_t pos = head;
    ring->data[pos % ring->size] = static_cast<uint8_t>(len & 0xFF);
    ring->data[(pos + 1) % ring->size] = static_cast<uint8_t>(len >> 8);
    pos += 2;

    // Write data
    for (uint16_t i = 0; i < len; i++) {
        ring->data[(pos + i) % ring->size] = data[i];
    }
    pos += len;

    // Pad
    while ((pos - head) & 3u) {
        ring->data[pos % ring->size] = 0;
        pos++;
    }

    // Update head (store with release semantics)
    asm volatile("" ::: "memory");
    ring->head = pos % ring->size;
    return 0;
}

// Read a packet from the ring. Returns packet length, 0 if empty.
uint16_t ring_read(RingBuffer* ring, uint8_t* buf, uint16_t buf_size) {
    uint32_t head = ring->head;
    uint32_t tail = ring->tail;
    asm volatile("" ::: "memory");

    if (head == tail) return 0;  // Empty

    // Read length
    uint16_t len = static_cast<uint16_t>(ring->data[tail % ring->size]) | (static_cast<uint16_t>(ring->data[(tail + 1) % ring->size]) << 8);

    uint32_t pkt_size = 2 + len;
    uint32_t padded = (pkt_size + 3) & ~3u;

    uint32_t pos = tail + 2;

    // Read data
    uint16_t copy_len = (len > buf_size) ? buf_size : len;
    for (uint16_t i = 0; i < copy_len; i++) {
        buf[i] = ring->data[(pos + i) % ring->size];
    }

    // Advance tail past padding
    asm volatile("" ::: "memory");
    ring->tail = (tail + padded) % ring->size;
    return copy_len;
}

// -- NetDevice operations --
int ivshmem_open(ker::net::NetDevice* netdev) {
    netdev->state = 1;
    return 0;
}

void ivshmem_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

int ivshmem_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) {
    auto* dev = static_cast<IvshmemNetDevice*>(netdev->private_data);
    if (dev == nullptr || !dev->active || pkt == nullptr) {
        if (pkt) ker::net::pkt_free(pkt);
        return -1;
    }

    dev->tx_lock.lock();
    int ret = ring_write(&dev->tx_ring, pkt->data, static_cast<uint16_t>(pkt->len));
    dev->tx_lock.unlock();

    if (ret == 0) {
        netdev->tx_packets++;
        netdev->tx_bytes += pkt->len;
        // Ring doorbell to notify peer
        dev->regs[IVSHMEM_REG_DOORBELL / 4] = (dev->my_vm_id == 0) ? 1 : 0;
    } else {
        netdev->tx_dropped++;
    }

    ker::net::pkt_free(pkt);
    return ret;
}

void ivshmem_set_mac(ker::net::NetDevice*, const uint8_t*) {}

ker::net::NetDeviceOps ivshmem_ops = {
    .open = ivshmem_open,
    .close = ivshmem_close,
    .start_xmit = ivshmem_start_xmit,
    .set_mac = ivshmem_set_mac,
};

// -- IRQ handler (minimal - just schedule NAPI) --
void ivshmem_irq(uint8_t, void* data) {
    auto* dev = static_cast<IvshmemNetDevice*>(data);
    if (dev == nullptr || !dev->active) return;

    // Acknowledge interrupt
    dev->regs[IVSHMEM_REG_INTRSTATUS / 4] = dev->regs[IVSHMEM_REG_INTRSTATUS / 4];

    // Disable device interrupts
    ivshmem_irq_disable(dev);

    // Schedule NAPI poll (atomic, IRQ-safe)
    ker::net::napi_schedule(&dev->napi);
}

// -- Device init --
auto init_device(pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_IVSHMEM_DEVICES) return -1;

    // Enable bus mastering + memory space
    pci::pci_enable_bus_master(pci_dev);
    pci::pci_enable_memory_space(pci_dev);

    // Map BAR0 (registers) into kernel page table
    auto* bar0_ptr = pci::pci_map_bar(pci_dev, 0);
    if (bar0_ptr == nullptr) {
        ker::mod::io::serial::write("ivshmem: BAR0 is zero\n");
        return -1;
    }
    auto* regs = reinterpret_cast<volatile uint32_t*>(bar0_ptr);

    // Map BAR2 (shared memory) into kernel page table
    auto* bar2_ptr = pci::pci_map_bar(pci_dev, 2);
    if (bar2_ptr == nullptr) {
        ker::mod::io::serial::write("ivshmem: BAR2 is zero\n");
        return -1;
    }
    auto* shmem = const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(bar2_ptr));

    // Determine VM position (0 or 1)
    uint32_t iv_pos = regs[IVSHMEM_REG_IVPOSITION / 4];

    // Use 16MB as default shared memory size
    constexpr size_t SHMEM_SIZE = 16 * 1024 * 1024;

    auto* idev = &dev_pool[device_count];
    std::memset(idev, 0, sizeof(IvshmemNetDevice));

    idev->pci = pci_dev;
    idev->regs = regs;
    idev->shmem = shmem;
    idev->shmem_size = SHMEM_SIZE;
    idev->my_vm_id = iv_pos;

    // Initialize or read shared memory header
    auto* hdr = reinterpret_cast<IvshmemHeader*>(shmem);

    if (hdr->magic != IVSHMEM_MAGIC) {
        // First VM: initialize header
        std::memset(hdr, 0, sizeof(IvshmemHeader));
        hdr->magic = IVSHMEM_MAGIC;
        hdr->version = IVSHMEM_VERSION;
        size_t half = (SHMEM_SIZE - RING_HEADER_SIZE) / 2;
        hdr->ring0_offset = RING_HEADER_SIZE;
        hdr->ring0_size = static_cast<uint32_t>(half);
        hdr->ring1_offset = static_cast<uint32_t>(RING_HEADER_SIZE + half);
        hdr->ring1_size = static_cast<uint32_t>(half);
        hdr->vm_id = 0;
        idev->my_vm_id = 0;
    } else {
        // Second VM: peer already set up
        hdr->peer_ready = 1;
        idev->my_vm_id = 1;
    }

    // Set up TX/RX rings based on VM ID
    if (idev->my_vm_id == 0) {
        // VM0 transmits on ring0, receives on ring1
        idev->tx_ring.data = shmem + hdr->ring0_offset + 8;  // skip head/tail
        idev->tx_ring.size = hdr->ring0_size - 8;
        idev->tx_ring.head = 0;
        idev->tx_ring.tail = 0;
        idev->rx_ring.data = shmem + hdr->ring1_offset + 8;
        idev->rx_ring.size = hdr->ring1_size - 8;
        idev->rx_ring.head = 0;
        idev->rx_ring.tail = 0;
        // Store head/tail in shared memory for cross-VM sync
        // Use first 8 bytes of each ring area: [head:u32][tail:u32]
    } else {
        // VM1 transmits on ring1, receives on ring0
        idev->tx_ring.data = shmem + hdr->ring1_offset + 8;
        idev->tx_ring.size = hdr->ring1_size - 8;
        idev->tx_ring.head = 0;
        idev->tx_ring.tail = 0;
        idev->rx_ring.data = shmem + hdr->ring0_offset + 8;
        idev->rx_ring.size = hdr->ring0_size - 8;
        idev->rx_ring.head = 0;
        idev->rx_ring.tail = 0;
    }

    // Set up IRQ
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector != 0) {
        idev->irq_vector = vector;
        int msi_ret = pci::pci_enable_msi(pci_dev, vector);
        if (msi_ret != 0) {
            vector = pci_dev->interrupt_line + 32;
            idev->irq_vector = vector;
        }
        ker::mod::gates::requestIrq(vector, ivshmem_irq, idev, "ivshmem-net");
        // Enable interrupts
        regs[IVSHMEM_REG_INTRMASK / 4] = 0xFFFFFFFF;
    }

    // Generate MAC: locally administered, based on VM position
    idev->netdev.mac[0] = 0x02;
    idev->netdev.mac[1] = 0x44;  // 'D' for DMA
    idev->netdev.mac[2] = 0x4D;  // 'M'
    idev->netdev.mac[3] = 0x41;  // 'A'
    idev->netdev.mac[4] = 0x00;
    idev->netdev.mac[5] = static_cast<uint8_t>(idev->my_vm_id);

    // Register NetDevice as "dmaN"
    std::array<char, 16> name = {};
    name[0] = 'd';
    name[1] = 'm';
    name[2] = 'a';
    name[3] = static_cast<char>(static_cast<uint8_t>('0') + device_count);
    std::memcpy(idev->netdev.name.data(), name.data(), 5);

    idev->netdev.ops = &ivshmem_ops;
    idev->netdev.mtu = 9000;  // jumbo frames supported
    idev->netdev.state = 1;
    idev->netdev.private_data = idev;
    idev->active = true;

    ker::net::netdev_register(&idev->netdev);

    // Initialize and enable NAPI for deferred packet processing
    ker::net::napi_init(&idev->napi, &idev->netdev, ivshmem_poll, 64);
    ker::net::napi_enable(&idev->napi);

    devices[device_count++] = idev;

    ker::mod::io::serial::write("ivshmem-net: ");
    ker::mod::io::serial::write(idev->netdev.name.data());
    ker::mod::io::serial::write(" vm_id=");
    ker::mod::io::serial::writeHex(idev->my_vm_id);
    ker::mod::io::serial::write(" shmem=");
    ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(shmem));
    ker::mod::io::serial::write(" ready\n");

    return 0;
}
}  // namespace

// -- NAPI poll function (runs in worker thread context) --
int ivshmem_poll(ker::net::NapiStruct* napi, int budget) {
    auto* dev = static_cast<IvshmemNetDevice*>(napi->dev->private_data);
    if (dev == nullptr || !dev->active) {
        ker::net::napi_complete(napi);
        return 0;
    }

    int processed = 0;
    uint8_t buf[2048];

    // Process RX packets up to budget
    while (processed < budget) {
        uint16_t len = ring_read(&dev->rx_ring, buf, sizeof(buf));
        if (len == 0) break;

        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) break;

        auto* dst = pkt->put(len);
        std::memcpy(dst, buf, len);
        pkt->dev = &dev->netdev;

        dev->netdev.rx_packets++;
        dev->netdev.rx_bytes += len;
        ker::net::netdev_rx(&dev->netdev, pkt);

        processed++;
    }

    // If we processed less than budget, we're done - re-enable interrupts
    if (processed < budget) {
        ker::net::napi_complete(napi);
        ivshmem_irq_enable(dev);
    }

    return processed;
}

auto ivshmem_net_is_claimed(pci::PCIDevice* dev) -> bool {
    for (size_t i = 0; i < device_count; i++) {
        if (devices[i] != nullptr && devices[i]->pci == dev) {
            return true;
        }
    }
    return false;
}

auto ivshmem_net_init() -> int {
    int found = 0;

    size_t count = pci::pci_device_count();
    for (size_t i = 0; i < count; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) continue;

        if (dev->vendor_id == IVSHMEM_VENDOR && dev->device_id == IVSHMEM_DEVICE) {
            ker::mod::io::serial::write("ivshmem: found device at PCI ");
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
    }

    if (found == 0) {
        ker::mod::io::serial::write("ivshmem: no devices found\n");
    }

    return found;
}

}  // namespace ker::dev::ivshmem
