#include "ivshmem_net.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <net/packet.hpp>
#include <new>  // IWYU pragma: keep
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <span>

#include "dev/pci.hpp"
#include "net/netdevice.hpp"
#include "net/netpoll.hpp"

namespace ker::dev::ivshmem {

using log = ker::mod::dbg::logger<"ivsh">;

namespace {
// Forward declarations for NAPI
auto ivshmem_poll(ker::net::NapiStruct* napi, int budget) -> int;

constexpr size_t MAX_IVSHMEM_DEVICES = 2;
std::array<IvshmemNetDevice, MAX_IVSHMEM_DEVICES> dev_pool = {};
std::array<IvshmemNetDevice*, MAX_IVSHMEM_DEVICES> devices = {};
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
auto ring_write(RingBuffer* ring, std::span<const uint8_t> data) -> int {
    // Packet format: [len:u16][data][pad to 4 bytes]
    auto const LEN = static_cast<uint16_t>(data.size());
    uint32_t const PKT_SIZE = 2 + LEN;
    uint32_t const PADDED = (PKT_SIZE + 3) & ~3U;

    uint32_t const HEAD = ring->head;
    uint32_t const TAIL = ring->tail;
    uint32_t space = 0;
    if (HEAD >= TAIL) {
        space = ring->size - HEAD + TAIL;
    } else {
        space = TAIL - HEAD;
    }
    // Need space + 1 to distinguish full from empty
    if (PADDED + 1 > space) {
        return -1;
    }

    // Write length
    uint32_t pos = HEAD;
    ring->data[pos % ring->size] = static_cast<uint8_t>(LEN & 0xFF);
    ring->data[(pos + 1) % ring->size] = static_cast<uint8_t>(LEN >> 8);
    pos += 2;

    // Write data
    for (uint16_t i = 0; i < LEN; i++) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): bounded by LEN from the span size.
        ring->data[(pos + i) % ring->size] = data[i];
    }
    pos += LEN;

    // Pad
    while (((pos - HEAD) & 3U) != 0U) {
        ring->data[pos % ring->size] = 0;
        pos++;
    }

    // Update head (store with release semantics)
    asm volatile("" ::: "memory");
    ring->head = pos % ring->size;
    return 0;
}

// Read a packet from the ring. Returns packet length, 0 if empty.
auto ring_read(RingBuffer* ring, std::span<uint8_t> buf) -> uint16_t {
    uint32_t const HEAD = ring->head;
    uint32_t const TAIL = ring->tail;
    asm volatile("" ::: "memory");

    if (HEAD == TAIL) {
        return 0;  // Empty
    }

    // Read length
    uint16_t const LEN =
        static_cast<uint16_t>(ring->data[TAIL % ring->size]) | (static_cast<uint16_t>(ring->data[(TAIL + 1) % ring->size]) << 8);

    uint32_t const PKT_SIZE = 2 + LEN;
    uint32_t const PADDED = (PKT_SIZE + 3) & ~3U;

    uint32_t const POS = TAIL + 2;

    // Read data
    uint16_t const COPY_LEN = (LEN > buf.size()) ? static_cast<uint16_t>(buf.size()) : LEN;
    for (uint16_t i = 0; i < COPY_LEN; i++) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): bounded by COPY_LEN from the span size.
        buf[i] = ring->data[(POS + i) % ring->size];
    }

    // Advance tail past padding
    asm volatile("" ::: "memory");
    ring->tail = (TAIL + PADDED) % ring->size;
    return COPY_LEN;
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
        if (pkt != nullptr) {
            ker::net::pkt_free(pkt);
        }
        return -1;
    }

    dev->tx_lock.lock();
    int const RET = ring_write(&dev->tx_ring, std::span(pkt->data, static_cast<uint16_t>(pkt->len)));
    dev->tx_lock.unlock();

    if (RET == 0) {
        netdev->tx_packets++;
        netdev->tx_bytes += pkt->len;
        // Ring doorbell to notify peer
        dev->regs[IVSHMEM_REG_DOORBELL / 4] = (dev->my_vm_id == 0) ? 1 : 0;
    } else {
        netdev->tx_dropped++;
    }

    ker::net::pkt_free(pkt);
    return RET;
}

void ivshmem_set_mac(ker::net::NetDevice* /*unused*/, const uint8_t* /*unused*/) {}

ker::net::NetDeviceOps ivshmem_ops = {
    .open = ivshmem_open,
    .close = ivshmem_close,
    .start_xmit = ivshmem_start_xmit,
    .set_mac = ivshmem_set_mac,
    .set_queue_cpu = nullptr,
};

// -- IRQ handler (minimal - just schedule NAPI) --
void ivshmem_irq(uint8_t /*unused*/, void* data) {
    auto* dev = static_cast<IvshmemNetDevice*>(data);
    if (dev == nullptr || !dev->active) {
        return;
    }

    // Acknowledge interrupt
    dev->regs[IVSHMEM_REG_INTRSTATUS / 4] = dev->regs[IVSHMEM_REG_INTRSTATUS / 4];

    // Disable device interrupts
    ivshmem_irq_disable(dev);

    // Schedule NAPI poll (atomic, IRQ-safe)
    ker::net::napi_schedule(&dev->napi);
}

// -- Device init --
auto init_device(pci::PCIDevice* pci_dev) -> int {
    if (device_count >= MAX_IVSHMEM_DEVICES) {
        return -1;
    }

    // Enable bus mastering + memory space
    pci::pci_enable_bus_master(pci_dev);
    pci::pci_enable_memory_space(pci_dev);

    // Map BAR0 (registers) into kernel page table
    auto* bar0_ptr = pci::pci_map_bar(pci_dev, 0);
    if (bar0_ptr == nullptr) {
        log::error("BAR0 is zero");
        return -1;
    }
    auto* regs = reinterpret_cast<volatile uint32_t*>(bar0_ptr);

    // Map BAR2 (shared memory) into kernel page table
    auto* bar2_ptr = pci::pci_map_bar(pci_dev, 2);
    if (bar2_ptr == nullptr) {
        log::error("BAR2 is zero");
        return -1;
    }
    auto* shmem = static_cast<uint8_t*>(bar2_ptr);

    // Determine VM position (0 or 1)
    uint32_t const IV_POS = regs[IVSHMEM_REG_IVPOSITION / 4];

    // Use 16MB as default shared memory size
    constexpr size_t SHMEM_SIZE = size_t{16} * 1024 * 1024;

    auto* idev = new (&dev_pool.at(device_count)) IvshmemNetDevice{};

    idev->pci = pci_dev;
    idev->regs = regs;
    idev->shmem = shmem;
    idev->shmem_size = SHMEM_SIZE;
    idev->my_vm_id = IV_POS;

    // Initialize or read shared memory header
    auto* hdr = reinterpret_cast<IvshmemHeader*>(shmem);

    if (hdr->magic != IVSHMEM_MAGIC) {
        // First VM: initialize header
        std::memset(hdr, 0, sizeof(IvshmemHeader));
        hdr->magic = IVSHMEM_MAGIC;
        hdr->version = IVSHMEM_VERSION;
        size_t const HALF = (SHMEM_SIZE - RING_HEADER_SIZE) / 2;
        hdr->ring0_offset = RING_HEADER_SIZE;
        hdr->ring0_size = static_cast<uint32_t>(HALF);
        hdr->ring1_offset = static_cast<uint32_t>(RING_HEADER_SIZE + HALF);
        hdr->ring1_size = static_cast<uint32_t>(HALF);
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
    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector != 0) {
        idev->irq_vector = vector;
        int const MSI_RET = pci::pci_enable_msi(pci_dev, vector);
        if (MSI_RET != 0) {
            vector = pci_dev->interrupt_line + 32;
            idev->irq_vector = vector;
        }
        ker::mod::gates::request_irq(vector, ivshmem_irq, idev, "ivshmem-net");
        // Enable interrupts
        regs[IVSHMEM_REG_INTRMASK / 4] = 0xFFFFFFFF;
    }

    // Generate MAC: locally administered, based on VM position
    idev->netdev.mac.at(0) = 0x02;
    idev->netdev.mac.at(1) = 0x44;  // 'D' for DMA
    idev->netdev.mac.at(2) = 0x4D;  // 'M'
    idev->netdev.mac.at(3) = 0x41;  // 'A'
    idev->netdev.mac.at(4) = 0x00;
    idev->netdev.mac.at(5) = static_cast<uint8_t>(idev->my_vm_id);

    // Register NetDevice as "dmaN"
    std::array<char, 16> name = {};
    name.at(0) = 'd';
    name.at(1) = 'm';
    name.at(2) = 'a';
    name.at(3) = static_cast<char>(static_cast<uint8_t>('0') + device_count);
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

    devices.at(device_count++) = idev;

    log::info("%s vm_id=%u shmem=0x%lx ready", idev->netdev.name.data(), idev->my_vm_id, reinterpret_cast<uint64_t>(shmem));

    return 0;
}

// -- NAPI poll function (runs in worker thread context) --
int ivshmem_poll(ker::net::NapiStruct* napi, int budget) {
    auto* dev = static_cast<IvshmemNetDevice*>(napi->dev->private_data);
    if (dev == nullptr || !dev->active) {
        ker::net::napi_complete(napi);
        return 0;
    }

    int processed = 0;
    std::array<uint8_t, 2048> buf{};

    // Process RX packets up to budget
    while (processed < budget) {
        uint16_t const LEN = ring_read(&dev->rx_ring, buf);
        if (LEN == 0) {
            break;
        }

        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            break;
        }

        auto* dst = pkt->put(LEN);
        std::memcpy(dst, buf.data(), LEN);
        pkt->dev = &dev->netdev;

        dev->netdev.rx_packets++;
        dev->netdev.rx_bytes += LEN;
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
}  // namespace

auto ivshmem_net_is_claimed(pci::PCIDevice* dev) -> bool {
    for (size_t i = 0; i < device_count; i++) {
        if (devices.at(i) != nullptr && devices.at(i)->pci == dev) {
            return true;
        }
    }
    return false;
}

auto ivshmem_net_init() -> int {
    int found = 0;

    size_t const COUNT = pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        if (dev->vendor_id == IVSHMEM_VENDOR && dev->device_id == IVSHMEM_DEVICE) {
            log::info("found device at PCI %02x:%02x.%x", dev->bus, dev->slot, dev->function);

            if (init_device(dev) == 0) {
                found++;
            }
        }
    }

    if (found == 0) {
        log::info("no devices found");
    }

    return found;
}

}  // namespace ker::dev::ivshmem
