#include "e1000e.hpp"

#include <cstring>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::e1000e {

namespace {
constexpr size_t MAX_E1000_DEVICES = 4;
E1000Device* devices[MAX_E1000_DEVICES] = {};  // NOLINT
size_t device_count = 0;

// Supported PCI device IDs (vendor = 0x8086)
struct DeviceID {
    uint16_t id;
    const char* name;
};

constexpr DeviceID supported_devices[] = {
    {0x100E, "82540EM (e1000)"}, {0x10D3, "82574L (e1000e)"}, {0x1539, "I211-AT"}, {0x153A, "I217-LM"}, {0x15B8, "I219-V"},
};
constexpr size_t NUM_SUPPORTED = sizeof(supported_devices) / sizeof(supported_devices[0]);

// ── MMIO access ─────────────────────────────────────────────────────────
inline auto reg_read(E1000Device* dev, uint32_t offset) -> uint32_t { return dev->mmio[offset / 4]; }

inline void reg_write(E1000Device* dev, uint32_t offset, uint32_t value) { dev->mmio[offset / 4] = value; }

// ── Physical address conversion ─────────────────────────────────────────
auto virt_to_phys(void* vaddr) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(vaddr);
    auto hhdm_offset = ker::mod::mm::addr::getHHDMOffset();

    // Check if address is in HHDM range (physical memory direct mapped)
    if (addr >= hhdm_offset) {
        // HHDM address: just subtract the offset
        return addr - hhdm_offset;
    }

    // Kernel virtual address (static data, BSS, etc.): use page table walk
    if (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL) {
        auto* kernel_pt = ker::mod::mm::virt::getKernelPageTable();
        return ker::mod::mm::virt::translate(kernel_pt, addr);
    }

    ker::mod::dbg::log("e1000e: ERROR - invalid virtual address 0x%lx for DMA", addr);
    return 0;
}

// ── EEPROM read ─────────────────────────────────────────────────────────
auto eeprom_read(E1000Device* dev, uint8_t addr) -> uint16_t {
    // Write address and start bit
    reg_write(dev, REG_EERD, (static_cast<uint32_t>(addr) << 8) | EERD_START);

    // Poll for completion
    uint32_t val;
    for (int i = 0; i < 10000; i++) {
        val = reg_read(dev, REG_EERD);
        if ((val & EERD_DONE) != 0) {
            return static_cast<uint16_t>(val >> 16);
        }
    }

    return 0;  // Timeout
}

// ── Read MAC address ────────────────────────────────────────────────────
void read_mac(E1000Device* dev) {
    // Try reading from RAL/RAH first (already programmed by EEPROM autoload)
    uint32_t ral = reg_read(dev, REG_RAL);
    uint32_t rah = reg_read(dev, REG_RAH);

    if (ral != 0 || (rah & 0xFFFF) != 0) {
        dev->netdev.mac[0] = static_cast<uint8_t>(ral);
        dev->netdev.mac[1] = static_cast<uint8_t>(ral >> 8);
        dev->netdev.mac[2] = static_cast<uint8_t>(ral >> 16);
        dev->netdev.mac[3] = static_cast<uint8_t>(ral >> 24);
        dev->netdev.mac[4] = static_cast<uint8_t>(rah);
        dev->netdev.mac[5] = static_cast<uint8_t>(rah >> 8);
        return;
    }

    // Fallback: read from EEPROM
    uint16_t w0 = eeprom_read(dev, 0);
    uint16_t w1 = eeprom_read(dev, 1);
    uint16_t w2 = eeprom_read(dev, 2);

    dev->netdev.mac[0] = static_cast<uint8_t>(w0);
    dev->netdev.mac[1] = static_cast<uint8_t>(w0 >> 8);
    dev->netdev.mac[2] = static_cast<uint8_t>(w1);
    dev->netdev.mac[3] = static_cast<uint8_t>(w1 >> 8);
    dev->netdev.mac[4] = static_cast<uint8_t>(w2);
    dev->netdev.mac[5] = static_cast<uint8_t>(w2 >> 8);
}

// ── Initialize RX ring ──────────────────────────────────────────────────
void init_rx(E1000Device* dev) {
    // Allocate descriptor ring (physically contiguous, 16-byte aligned)
    size_t ring_size = NUM_RX_DESC * sizeof(E1000RxDesc);
    auto* descs = static_cast<E1000RxDesc*>(ker::mod::mm::phys::pageAlloc(ring_size));
    memset(descs, 0, ring_size);
    dev->rx_descs = descs;

    // Allocate packet buffers and fill descriptors
    for (size_t i = 0; i < NUM_RX_DESC; i++) {
        auto* pkt = ker::net::pkt_alloc();
        if (pkt == nullptr) {
            ker::mod::dbg::log("e1000e: Failed to allocate RX buffer %d", i);
            break;
        }
        pkt->data = pkt->storage;
        pkt->len = 0;
        dev->rx_bufs[i] = pkt;
        descs[i].addr = virt_to_phys(pkt->storage);
        descs[i].status = 0;
    }

    // Program descriptor ring registers
    uint64_t ring_phys = virt_to_phys(descs);
    reg_write(dev, REG_RDBAL, static_cast<uint32_t>(ring_phys));
    reg_write(dev, REG_RDBAH, static_cast<uint32_t>(ring_phys >> 32));
    reg_write(dev, REG_RDLEN, static_cast<uint32_t>(ring_size));
    reg_write(dev, REG_RDH, 0);
    reg_write(dev, REG_RDT, NUM_RX_DESC - 1);

    dev->rx_tail = 0;

    // Enable receiver
    uint32_t rctl = RCTL_EN | RCTL_BAM | RCTL_BSIZE_2048 | RCTL_SECRC;
    reg_write(dev, REG_RCTL, rctl);
}

// ── Initialize TX ring ──────────────────────────────────────────────────
void init_tx(E1000Device* dev) {
    size_t ring_size = NUM_TX_DESC * sizeof(E1000TxDesc);
    auto* descs = static_cast<E1000TxDesc*>(ker::mod::mm::phys::pageAlloc(ring_size));
    memset(descs, 0, ring_size);
    dev->tx_descs = descs;

    // Program descriptor ring registers
    uint64_t ring_phys = virt_to_phys(descs);
    reg_write(dev, REG_TDBAL, static_cast<uint32_t>(ring_phys));
    reg_write(dev, REG_TDBAH, static_cast<uint32_t>(ring_phys >> 32));
    reg_write(dev, REG_TDLEN, static_cast<uint32_t>(ring_size));
    reg_write(dev, REG_TDH, 0);
    reg_write(dev, REG_TDT, 0);

    dev->tx_tail = 0;

    // Set Inter-Packet Gap (recommended values from Intel datasheet)
    // IPGT=10, IPGR1=8, IPGR2=6
    reg_write(dev, REG_TIPG, (6u << 20) | (8u << 10) | 10u);

    // Enable transmitter
    uint32_t tctl = TCTL_EN | TCTL_PSP | (15u << TCTL_CT_SHIFT) |  // Collision Threshold
                    (64u << TCTL_COLD_SHIFT);                      // Collision Distance (full duplex)
    reg_write(dev, REG_TCTL, tctl);
}

// ── Process received packets ────────────────────────────────────────────
void process_rx(E1000Device* dev) {
    while (true) {
        uint16_t idx = dev->rx_tail;
        auto* desc = &dev->rx_descs[idx];

        if ((desc->status & RX_STATUS_DD) == 0) {
            break;  // No more completed descriptors
        }

        if ((desc->status & RX_STATUS_EOP) != 0 && desc->errors == 0) {
            auto* pkt = dev->rx_bufs[idx];
            if (pkt != nullptr) {
                pkt->data = pkt->storage;
                pkt->len = desc->length;
                pkt->dev = &dev->netdev;

                // Hand off to network stack
                ker::net::netdev_rx(&dev->netdev, pkt);

                // Allocate replacement buffer
                auto* new_pkt = ker::net::pkt_alloc();
                if (new_pkt != nullptr) {
                    new_pkt->data = new_pkt->storage;
                    new_pkt->len = 0;
                    dev->rx_bufs[idx] = new_pkt;
                    desc->addr = virt_to_phys(new_pkt->storage);
                } else {
                    // No buffer available — reuse old descriptor
                    dev->rx_bufs[idx] = nullptr;
                    desc->addr = 0;
                }
            }
        } else {
            // Error packet — just recycle the buffer
            dev->netdev.rx_bytes += 0;  // don't count errored packets
        }

        desc->status = 0;
        reg_write(dev, REG_RDT, idx);
        dev->rx_tail = (idx + 1) % NUM_RX_DESC;
    }
}

// ── Process TX completions ──────────────────────────────────────────────
void process_tx(E1000Device* dev) {
    for (size_t i = 0; i < NUM_TX_DESC; i++) {
        auto* desc = &dev->tx_descs[i];
        if ((desc->status & TX_STATUS_DD) != 0 && dev->tx_bufs[i] != nullptr) {
            ker::net::pkt_free(dev->tx_bufs[i]);
            dev->tx_bufs[i] = nullptr;
            desc->status = 0;
        }
    }
}

// ── IRQ handler ─────────────────────────────────────────────────────────
void e1000_irq_handler(uint8_t /*vector*/, void* private_data) {
    auto* dev = static_cast<E1000Device*>(private_data);
    if (dev == nullptr) {
        return;
    }

    // Read and acknowledge interrupt cause
    uint32_t icr = reg_read(dev, REG_ICR);

    if ((icr & (ICR_RXT0 | ICR_RXDMT0 | ICR_RXO)) != 0) {
        process_rx(dev);
    }

    if ((icr & (ICR_TXDW | ICR_TXQE)) != 0) {
        process_tx(dev);
    }

    if ((icr & ICR_LSC) != 0) {
        uint32_t status = reg_read(dev, REG_STATUS);
        if ((status & 0x02) != 0) {
            ker::mod::dbg::log("e1000e: Link up");
        } else {
            ker::mod::dbg::log("e1000e: Link down");
        }
    }
}

// ── NetDevice operations ────────────────────────────────────────────────
int e1000_open(ker::net::NetDevice* /*ndev*/) { return 0; }

void e1000_close(ker::net::NetDevice* /*ndev*/) {}

int e1000_start_xmit(ker::net::NetDevice* ndev, ker::net::PacketBuffer* pkt) {
    auto* dev = reinterpret_cast<E1000Device*>(ndev);

    dev->tx_lock.lock();

    uint16_t idx = dev->tx_tail;
    auto* desc = &dev->tx_descs[idx];

    // Check if descriptor is available
    if (dev->tx_bufs[idx] != nullptr) {
        // TX ring full — try to reclaim
        process_tx(dev);
        if (dev->tx_bufs[idx] != nullptr) {
            dev->tx_lock.unlock();
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

    dev->tx_bufs[idx] = pkt;
    dev->tx_tail = (idx + 1) % NUM_TX_DESC;

    // Notify hardware
    reg_write(dev, REG_TDT, dev->tx_tail);

    ndev->tx_packets++;
    ndev->tx_bytes += pkt->len;

    dev->tx_lock.unlock();
    return 0;
}

void e1000_set_mac(ker::net::NetDevice* /*ndev*/, const uint8_t* /*mac*/) {
    // Not implemented — MAC is read from hardware
}

ker::net::NetDeviceOps e1000_netdev_ops = {
    .open = e1000_open,
    .close = e1000_close,
    .start_xmit = e1000_start_xmit,
    .set_mac = e1000_set_mac,
};

// ── Check if PCI device is supported ────────────────────────────────────
auto find_device_name(uint16_t device_id) -> const char* {
    for (size_t i = 0; i < NUM_SUPPORTED; i++) {
        if (supported_devices[i].id == device_id) {
            return supported_devices[i].name;
        }
    }
    return nullptr;
}

// ── Initialize a single e1000e device ───────────────────────────────────
void init_device(pci::PCIDevice* pci_dev, const char* name) {
    if (device_count >= MAX_E1000_DEVICES) {
        return;
    }

    auto* dev = static_cast<E1000Device*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(E1000Device)));
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
        ker::mod::dbg::log("e1000e: BAR0 is 0, cannot map MMIO");
        ker::mod::mm::dyn::kmalloc::free(dev);
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
    uint32_t ral = static_cast<uint32_t>(dev->netdev.mac[0]) | (static_cast<uint32_t>(dev->netdev.mac[1]) << 8) |
                   (static_cast<uint32_t>(dev->netdev.mac[2]) << 16) | (static_cast<uint32_t>(dev->netdev.mac[3]) << 24);
    uint32_t rah = static_cast<uint32_t>(dev->netdev.mac[4]) | (static_cast<uint32_t>(dev->netdev.mac[5]) << 8) | RAH_AV;
    reg_write(dev, REG_RAL, ral);
    reg_write(dev, REG_RAH, rah);

    // Initialize RX and TX rings
    init_rx(dev);
    init_tx(dev);

    // Set up interrupt
    uint8_t vector = ker::mod::gates::allocateVector();
    if (vector == 0) {
        ker::mod::dbg::log("e1000e: Failed to allocate IRQ vector");
        ker::mod::mm::dyn::kmalloc::free(dev);
        return;
    }

    dev->irq_vector = vector;

    // Try MSI first, fall back to legacy interrupt
    int msi_result = pci::pci_enable_msi(pci_dev, vector);
    if (msi_result != 0) {
        // Use legacy IRQ
        ker::mod::dbg::log("e1000e: MSI not available, using legacy IRQ %d", pci_dev->interrupt_line);
        vector = pci_dev->interrupt_line + 32;  // IRQ line + ISA offset
        dev->irq_vector = vector;
    }

    ker::mod::gates::requestIrq(vector, e1000_irq_handler, dev, "e1000e");

    // Set up NetDevice
    dev->netdev.mtu = 1500;
    dev->netdev.state = 1;  // UP
    dev->netdev.ops = &e1000_netdev_ops;
    dev->netdev.private_data = dev;

    // Register with network stack
    ker::net::netdev_register(&dev->netdev);

    devices[device_count] = dev;
    device_count++;

    // Clear any pending interrupts before enabling them
    (void)reg_read(dev, REG_ICR);

    // Enable interrupts: RX timer, RX overrun, link status change, TX done
    reg_write(dev, REG_IMS, ICR_RXT0 | ICR_RXDMT0 | ICR_RXO | ICR_LSC | ICR_TXDW);

    ker::mod::dbg::log("e1000e: %s initialized, MAC=%02x:%02x:%02x:%02x:%02x:%02x, IRQ=%d (%s)", name, dev->netdev.mac[0],
                       dev->netdev.mac[1], dev->netdev.mac[2], dev->netdev.mac[3], dev->netdev.mac[4], dev->netdev.mac[5], dev->irq_vector,
                       msi_result == 0 ? "MSI" : "legacy");
}
}  // namespace

void e1000e_init() {
    // Scan all PCI devices for supported Intel NICs
    size_t count = pci::pci_device_count();
    for (size_t i = 0; i < count; i++) {
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
            ker::mod::dbg::log("e1000e: Found %s (device 0x%x) at %d:%d.%d", name, dev->device_id, dev->bus, dev->slot, dev->function);
            init_device(dev, name);
        }
    }

    if (device_count == 0) {
        ker::mod::dbg::log("e1000e: No supported Intel NIC found");
    }
}

}  // namespace ker::dev::e1000e
