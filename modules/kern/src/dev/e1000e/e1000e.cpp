#include "e1000e.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/wki/remotable.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

#include "dev/pci.hpp"
#include "util/hcf.hpp"

namespace ker::dev::e1000e {

using log = ker::mod::dbg::logger<"e1000e">;

namespace {

// WKI remotable ops for E1000E devices
auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    log::trace("remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { log::trace("remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { log::trace("remote fault for 0x%04x", node_id); }
const ker::net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

constexpr size_t MAX_E1000_DEVICES = 4;
std::array<E1000Device*, MAX_E1000_DEVICES> devices = {};
size_t device_count = 0;

// Supported PCI device IDs (vendor = 0x8086)
struct DeviceID {
    uint16_t id;
    const char* name;
};

constexpr std::array<DeviceID, 5> SUPPORTED_DEVICES = {{
    {.id = 0x100E, .name = "82540EM (e1000)"},
    {.id = 0x10D3, .name = "82574L (e1000e)"},
    {.id = 0x1539, .name = "I211-AT"},
    {.id = 0x153A, .name = "I217-LM"},
    {.id = 0x15B8, .name = "I219-V"},
}};

// -- MMIO access ---------------------------------------------------------
inline auto reg_read(E1000Device* dev, uint32_t offset) -> uint32_t { return dev->mmio[offset / 4]; }

inline void reg_write(E1000Device* dev, uint32_t offset, uint32_t value) { dev->mmio[offset / 4] = value; }

// -- Physical address conversion -----------------------------------------
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    auto hhdm_offset = ker::mod::mm::addr::get_hhdm_offset();

    // Check if address is in HHDM range (physical memory direct mapped)
    if (addr >= hhdm_offset) {
        // HHDM address: just subtract the offset
        return addr - hhdm_offset;
    }

    // Kernel virtual address (static data, BSS, etc.): use page table walk
    if (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL) {
        auto* kernel_pt = ker::mod::mm::virt::get_kernel_page_table();
        uint64_t const PHYS = ker::mod::mm::virt::translate(kernel_pt, addr);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            log::error("virt_to_phys failed for kernel address 0x%lx", addr);
            hcf();
        }
        return PHYS;
    }

    log::error("virt_to_phys called with unrecognized address 0x%lx", addr);
    hcf();
    __builtin_unreachable();
}

// -- EEPROM read ---------------------------------------------------------
auto eeprom_read(E1000Device* dev, uint8_t addr) -> uint16_t {
    // Write address and start bit
    reg_write(dev, REG_EERD, (static_cast<uint32_t>(addr) << 8) | EERD_START);

    // Poll for completion
    uint32_t val = 0;
    for (int i = 0; i < 10000; i++) {
        val = reg_read(dev, REG_EERD);
        if ((val & EERD_DONE) != 0) {
            return static_cast<uint16_t>(val >> 16);
        }
    }

    return 0;  // Timeout
}

// -- Read MAC address ----------------------------------------------------
void read_mac(E1000Device* dev) {
    // Try reading from RAL/RAH first (already programmed by EEPROM autoload)
    uint32_t const RAL = reg_read(dev, REG_RAL);
    uint32_t const RAH = reg_read(dev, REG_RAH);

    if (RAL != 0 || (RAH & 0xFFFF) != 0) {
        dev->netdev.mac.at(0) = static_cast<uint8_t>(RAL);
        dev->netdev.mac.at(1) = static_cast<uint8_t>(RAL >> 8);
        dev->netdev.mac.at(2) = static_cast<uint8_t>(RAL >> 16);
        dev->netdev.mac.at(3) = static_cast<uint8_t>(RAL >> 24);
        dev->netdev.mac.at(4) = static_cast<uint8_t>(RAH);
        dev->netdev.mac.at(5) = static_cast<uint8_t>(RAH >> 8);
        return;
    }

    // Fallback: read from EEPROM
    uint16_t const W0 = eeprom_read(dev, 0);
    uint16_t const W1 = eeprom_read(dev, 1);
    uint16_t const W2 = eeprom_read(dev, 2);

    dev->netdev.mac.at(0) = static_cast<uint8_t>(W0);
    dev->netdev.mac.at(1) = static_cast<uint8_t>(W0 >> 8);
    dev->netdev.mac.at(2) = static_cast<uint8_t>(W1);
    dev->netdev.mac.at(3) = static_cast<uint8_t>(W1 >> 8);
    dev->netdev.mac.at(4) = static_cast<uint8_t>(W2);
    dev->netdev.mac.at(5) = static_cast<uint8_t>(W2 >> 8);
}

// -- Initialize RX ring --------------------------------------------------
void init_rx(E1000Device* dev) {
    // Allocate descriptor ring (physically contiguous, 16-byte aligned)
    size_t const RING_SIZE = NUM_RX_DESC * sizeof(E1000RxDesc);
    auto* descs = static_cast<E1000RxDesc*>(ker::mod::mm::phys::page_alloc(RING_SIZE));
    std::memset(descs, 0, RING_SIZE);
    dev->rx_descs = descs;

    // Allocate packet buffers and fill descriptors
    for (size_t i = 0; i < NUM_RX_DESC; i++) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            log::warn("failed to allocate RX buffer %zu", i);
            break;
        }
        pkt->data = pkt->storage.data();
        pkt->len = 0;
        dev->rx_bufs.at(i) = pkt;
        descs[i].addr = virt_to_phys(pkt->storage.data());
        descs[i].status = 0;
    }

    // Program descriptor ring registers
    uint64_t const RING_PHYS = virt_to_phys(descs);
    reg_write(dev, REG_RDBAL, static_cast<uint32_t>(RING_PHYS));
    reg_write(dev, REG_RDBAH, static_cast<uint32_t>(RING_PHYS >> 32));
    reg_write(dev, REG_RDLEN, static_cast<uint32_t>(RING_SIZE));
    reg_write(dev, REG_RDH, 0);
    reg_write(dev, REG_RDT, NUM_RX_DESC - 1);

    dev->rx_tail = 0;

    // Enable receiver
    uint32_t const RCTL = RCTL_EN | RCTL_BAM | RCTL_BSIZE_2048 | RCTL_SECRC;
    reg_write(dev, REG_RCTL, RCTL);
}

// -- Initialize TX ring --------------------------------------------------
void init_tx(E1000Device* dev) {
    size_t const RING_SIZE = NUM_TX_DESC * sizeof(E1000TxDesc);
    auto* descs = static_cast<E1000TxDesc*>(ker::mod::mm::phys::page_alloc(RING_SIZE));
    std::memset(descs, 0, RING_SIZE);
    dev->tx_descs = descs;

    // Program descriptor ring registers
    uint64_t const RING_PHYS = virt_to_phys(descs);
    reg_write(dev, REG_TDBAL, static_cast<uint32_t>(RING_PHYS));
    reg_write(dev, REG_TDBAH, static_cast<uint32_t>(RING_PHYS >> 32));
    reg_write(dev, REG_TDLEN, static_cast<uint32_t>(RING_SIZE));
    reg_write(dev, REG_TDH, 0);
    reg_write(dev, REG_TDT, 0);

    dev->tx_tail = 0;

    // Set Inter-Packet Gap (recommended values from Intel datasheet)
    // IPGT=10, IPGR1=8, IPGR2=6
    reg_write(dev, REG_TIPG, (6U << 20) | (8U << 10) | 10U);

    // Enable transmitter
    uint32_t const TCTL = TCTL_EN | TCTL_PSP | (15U << TCTL_CT_SHIFT) |  // Collision Threshold
                          (64U << TCTL_COLD_SHIFT);                      // Collision Distance (full duplex)
    reg_write(dev, REG_TCTL, TCTL);
}

// -- Process received packets (budget-limited for NAPI) ------------------
int process_rx_budget(E1000Device* dev, int budget) {
    int processed = 0;

    while (processed < budget) {
        uint16_t const IDX = dev->rx_tail;
        auto* desc = &dev->rx_descs[IDX];

        if ((desc->status & RX_STATUS_DD) == 0) {
            break;  // No more completed descriptors
        }

        if ((desc->status & RX_STATUS_EOP) != 0 && desc->errors == 0) {
            auto* pkt = dev->rx_bufs.at(IDX);
            if (pkt != nullptr) {
                pkt->data = pkt->storage.data();
                pkt->len = desc->length;
                pkt->dev = &dev->netdev;

                // Hand off to network stack
                ker::net::netdev_rx(&dev->netdev, pkt);
                processed++;

                // Allocate replacement buffer
                auto* new_pkt = ker::net::pkt_alloc();
                if (new_pkt != nullptr) {
                    new_pkt->data = new_pkt->storage.data();
                    new_pkt->len = 0;
                    dev->rx_bufs.at(IDX) = new_pkt;
                    desc->addr = virt_to_phys(new_pkt->storage.data());
                } else {
                    // No buffer available - mark slot as empty
                    dev->rx_bufs.at(IDX) = nullptr;
                    desc->addr = 0;
                }
            }
        }

        desc->status = 0;
        reg_write(dev, REG_RDT, IDX);
        dev->rx_tail = (IDX + 1) % NUM_RX_DESC;
    }

    return processed;
}

// -- Process TX completions ----------------------------------------------
void process_tx(E1000Device* dev) {
    for (size_t i = 0; i < NUM_TX_DESC; i++) {
        auto* desc = &dev->tx_descs[i];
        if ((desc->status & TX_STATUS_DD) != 0 && dev->tx_bufs.at(i) != nullptr) {
            ker::net::pkt_free(dev->tx_bufs.at(i));
            dev->tx_bufs.at(i) = nullptr;
            desc->status = 0;
        }
    }
}

// -- NAPI poll function - called from worker thread context --------------
int e1000_poll(ker::net::NapiStruct* napi, int budget) {
    auto* dev = reinterpret_cast<E1000Device*>(napi->dev);
    int processed = 0;

    // TX reclamation is done lazily in start_xmit when the ring is full.
    // NAPI poll is the sole consumer of RX (mutual exclusion via NAPI CAS).
    processed = process_rx_budget(dev, budget);

    // If we processed less than budget, we're done - re-enable IRQs
    if (processed < budget) {
        ker::net::napi_complete(napi);
        // Re-enable interrupts
        reg_write(dev, REG_IMS, ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_LSC | ICR_TXDW);
    }

    return processed;
}

// -- Minimal IRQ handler (NAPI model) ------------------------------------
void e1000_irq_handler(uint8_t /*vector*/, void* private_data) {
    auto* dev = static_cast<E1000Device*>(private_data);
    if (dev == nullptr) {
        return;
    }

    // Read and acknowledge interrupt cause (auto-clears on read)
    uint32_t const ICR = reg_read(dev, REG_ICR);
    if (ICR == 0) {
        return;  // Not our interrupt
    }

    // Handle link status change immediately (doesn't need deferral)
    if ((ICR & ICR_LSC) != 0) {
        uint32_t const STATUS = reg_read(dev, REG_STATUS);
        if ((STATUS & 0x02) != 0) {
            log::info("link up");
        } else {
            log::info("link down");
        }
    }

    // For RX/TX, disable interrupts and schedule NAPI poll
    if ((ICR & (ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_TXDW | ICR_TXQE)) != 0) {
        // Disable interrupts until poll completes
        reg_write(dev, REG_IMC, 0xFFFFFFFF);

        // Schedule NAPI poll - all real work happens in worker thread
        ker::net::napi_schedule(&dev->napi);
    }
}

// -- NetDevice operations ------------------------------------------------
int e1000_open(ker::net::NetDevice* /*ndev*/) { return 0; }

void e1000_close(ker::net::NetDevice* /*ndev*/) {}

int e1000_start_xmit(ker::net::NetDevice* ndev, ker::net::PacketBuffer* pkt) {
    auto* dev = reinterpret_cast<E1000Device*>(ndev);

    // Use lock_irqsave to prevent preemption while holding the lock.
    // Without this, a DAEMON task holding the plain lock can be preempted,
    // and a non-preemptible PROCESS task on the same CPU will spin forever.
    uint64_t const TXFLAGS = dev->tx_lock.lock_irqsave();

    uint16_t const IDX = dev->tx_tail;
    auto* desc = &dev->tx_descs[IDX];

    // Check if descriptor is available
    if (dev->tx_bufs.at(IDX) != nullptr) {
        // TX ring full - try to reclaim
        process_tx(dev);
        if (dev->tx_bufs.at(IDX) != nullptr) {
            dev->tx_lock.unlock_irqrestore(TXFLAGS);
            ker::net::pkt_free(pkt);
            return -1;  // Drop packet
        }
    }

    // Set up descriptor
    desc->addr = virt_to_phys(pkt->data);
    desc->length = static_cast<uint16_t>(pkt->len);
    desc->cmd = TX_CMD_EOP | TX_CMD_IFCS | TX_CMD_RS;
    desc->status = 0;
    desc->cso = 0;
    desc->css = 0;
    desc->special = 0;

    dev->tx_bufs.at(IDX) = pkt;
    dev->tx_tail = (IDX + 1) % NUM_TX_DESC;

    // Notify hardware
    reg_write(dev, REG_TDT, dev->tx_tail);

    ndev->tx_packets++;
    ndev->tx_bytes += pkt->len;

    dev->tx_lock.unlock_irqrestore(TXFLAGS);
    return 0;
}

void e1000_set_mac(ker::net::NetDevice* /*ndev*/, const uint8_t* /*mac*/) {
    // Not implemented - MAC is read from hardware
}

ker::net::NetDeviceOps e1000_netdev_ops = {
    .open = e1000_open,
    .close = e1000_close,
    .start_xmit = e1000_start_xmit,
    .set_mac = e1000_set_mac,
    .set_queue_cpu = nullptr,
};

// -- Check if PCI device is supported ------------------------------------
auto find_device_name(uint16_t device_id) -> const char* {
    for (auto supported_device : SUPPORTED_DEVICES) {
        if (supported_device.id == device_id) {
            return supported_device.name;
        }
    }
    return nullptr;
}

// -- Initialize a single e1000e device -----------------------------------
void init_device(pci::PCIDevice* pci_dev, const char* name) {
    if (device_count >= MAX_E1000_DEVICES) {
        return;
    }

    auto* dev = new (std::nothrow) E1000Device{};
    if (dev == nullptr) {
        return;
    }

    dev->pci = pci_dev;

    // Enable bus mastering and memory space
    pci::pci_enable_bus_master(pci_dev);
    pci::pci_enable_memory_space(pci_dev);

    // Map BAR0 (MMIO) into kernel page table
    auto* bar0_ptr = pci::pci_map_bar(pci_dev, 0);
    if (bar0_ptr == nullptr) {
        log::error("BAR0 is 0, cannot map MMIO");
        delete dev;
        return;
    }

    dev->mmio = reinterpret_cast<volatile uint32_t*>(bar0_ptr);

    // Reset the device
    reg_write(dev, REG_CTRL, CTRL_RST);
    // Wait for reset to complete (read status until non-zero or timeout)
    for (int i = 0; i < 100000; i++) {
        if ((reg_read(dev, REG_CTRL) & CTRL_RST) == 0) {
            break;
        }
    }

    // Disable interrupts during setup
    reg_write(dev, REG_IMC, 0xFFFFFFFF);
    // Clear pending interrupts
    (void)reg_read(dev, REG_ICR);

    // Disable interrupt moderation/coalescing for low-latency VM-to-VM traffic.
    reg_write(dev, REG_ITR, 0);
    reg_write(dev, REG_RDTR, 0);
    reg_write(dev, REG_RADV, 0);
    reg_write(dev, REG_TIDV, 0);
    reg_write(dev, REG_TADV, 0);

    // Set link up
    uint32_t ctrl = reg_read(dev, REG_CTRL);
    ctrl |= CTRL_SLU;
    ctrl &= ~CTRL_PHY_RST;
    reg_write(dev, REG_CTRL, ctrl);

    // Read MAC address
    read_mac(dev);

    // Clear multicast table array
    for (int i = 0; i < 128; i++) {
        reg_write(dev, REG_MTA + static_cast<uint32_t>(i * 4), 0);
    }

    // Program MAC into RAL/RAH
    uint32_t const RAL = static_cast<uint32_t>(dev->netdev.mac.at(0)) | (static_cast<uint32_t>(dev->netdev.mac.at(1)) << 8) |
                         (static_cast<uint32_t>(dev->netdev.mac.at(2)) << 16) | (static_cast<uint32_t>(dev->netdev.mac.at(3)) << 24);
    uint32_t const RAH = static_cast<uint32_t>(dev->netdev.mac.at(4)) | (static_cast<uint32_t>(dev->netdev.mac.at(5)) << 8) | RAH_AV;
    reg_write(dev, REG_RAL, RAL);
    reg_write(dev, REG_RAH, RAH);

    // Initialize RX and TX rings
    init_rx(dev);
    init_tx(dev);

    // Set up interrupt
    uint8_t vector = ker::mod::gates::allocate_vector();
    if (vector == 0) {
        log::error("failed to allocate IRQ vector");
        delete dev;
        return;
    }

    dev->irq_vector = vector;

    // Try MSI first, fall back to legacy interrupt
    int const MSI_RESULT = pci::pci_enable_msi(pci_dev, vector);
    if (MSI_RESULT != 0) {
        // Use legacy IRQ
        log::warn("MSI not available, using legacy IRQ %d", pci_dev->interrupt_line);
        vector = pci_dev->interrupt_line + 32;  // IRQ line + ISA offset
        dev->irq_vector = vector;
    }

    ker::mod::gates::request_irq(vector, e1000_irq_handler, dev, "e1000e");

    // Set up NetDevice
    dev->netdev.mtu = 1500;
    dev->netdev.state = 1;  // UP
    dev->netdev.ops = &e1000_netdev_ops;
    dev->netdev.private_data = dev;
    dev->netdev.remotable = &S_REMOTABLE_OPS;

    // Register with network stack
    ker::net::netdev_register(&dev->netdev);

    // Initialize and enable NAPI for deferred packet processing
    ker::net::napi_init(&dev->napi, &dev->netdev, e1000_poll, 64);
    ker::net::napi_enable(&dev->napi);

    devices.at(device_count) = dev;
    device_count++;

    // Clear any pending interrupts before enabling them
    (void)reg_read(dev, REG_ICR);

    // Enable interrupts: RX timer, RX overrun, link status change, TX done
    reg_write(dev, REG_IMS, ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_LSC | ICR_TXDW);

    log::info("%s initialized, MAC=%02x:%02x:%02x:%02x:%02x:%02x, IRQ=%d (%s) napi", name, dev->netdev.mac.at(0), dev->netdev.mac.at(1),
              dev->netdev.mac.at(2), dev->netdev.mac.at(3), dev->netdev.mac.at(4), dev->netdev.mac.at(5), dev->irq_vector,
              MSI_RESULT == 0 ? "MSI" : "legacy");
}
}  // namespace

void e1000e_init() {
    // Scan all PCI devices for supported Intel NICs
    size_t const COUNT = pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }
        if (dev->vendor_id != 0x8086) {
            continue;
        }
        if (dev->class_code != pci::PCI_CLASS_NETWORK || dev->subclass_code != pci::PCI_SUBCLASS_ETHERNET) {
            continue;
        }
        const char* name = find_device_name(dev->device_id);
        if (name != nullptr) {
            log::info("found %s (device 0x%x) at %d:%d.%d", name, dev->device_id, dev->bus, dev->slot, dev->function);
            init_device(dev, name);
        }
    }

    if (device_count == 0) {
        log::info("no supported Intel NIC found");
    }
}

}  // namespace ker::dev::e1000e
