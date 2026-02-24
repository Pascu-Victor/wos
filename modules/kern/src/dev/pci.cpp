#include "pci.hpp"

#include <cstdint>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::pci {

namespace {
constexpr int MAX_PCI_DEVICES = 64;
PCIDevice discovered_devices[MAX_PCI_DEVICES];
int device_count = 0;
bool enumerated = false;

// 32-bit port I/O using inline assembly
inline auto inl(uint16_t port) -> uint32_t {
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}

inline void outl(uint16_t port, uint32_t value) { asm volatile("outl %0, %1" : : "a"(value), "Nd"(port)); }

void scan_bus(uint8_t bus);

void scan_function(uint8_t bus, uint8_t slot, uint8_t func) {
    uint16_t vendor = pci_config_read16(bus, slot, func, PCI_VENDOR_ID);
    if (vendor == 0xFFFF) return;
    if (device_count >= MAX_PCI_DEVICES) return;

    auto* dev = &discovered_devices[device_count];
    dev->bus = bus;
    dev->slot = slot;
    dev->function = func;
    dev->vendor_id = vendor;
    dev->device_id = pci_config_read16(bus, slot, func, PCI_DEVICE_ID);

    // Read class/subclass/prog_if from the 32-bit register at offset 0x08
    uint32_t class_dword = pci_config_read32(bus, slot, func, PCI_CLASS_CODE);
    dev->class_code = (class_dword >> 24) & 0xFF;
    dev->subclass_code = (class_dword >> 16) & 0xFF;
    dev->prog_if = (class_dword >> 8) & 0xFF;

    dev->header_type = pci_config_read8(bus, slot, func, PCI_HEADER_TYPE);
    dev->interrupt_line = pci_config_read8(bus, slot, func, PCI_INTERRUPT_LINE);
    dev->interrupt_pin = pci_config_read8(bus, slot, func, PCI_INTERRUPT_PIN);

    // Read BARs for type 0 (normal) devices
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_NORMAL) {
        for (int i = 0; i < BAR_COUNT; i++) {
            dev->bar[i] = pci_config_read32(bus, slot, func, PCI_BAR0 + (i * 4));
        }
    } else {
        for (int i = 0; i < BAR_COUNT; i++) {
            dev->bar[i] = 0;
        }
    }

    device_count++;

    // If this is a PCI-to-PCI bridge, scan the secondary bus
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_BRIDGE) {
        uint8_t secondary_bus = pci_config_read8(bus, slot, func, PCI_SECONDARY_BUS);
        if (secondary_bus != 0) {
            scan_bus(secondary_bus);
        }
    }
}

void scan_slot(uint8_t bus, uint8_t slot) {
    uint16_t vendor = pci_config_read16(bus, slot, 0, PCI_VENDOR_ID);
    if (vendor == 0xFFFF) return;

    scan_function(bus, slot, 0);

    // Check for multi-function device
    uint8_t header_type = pci_config_read8(bus, slot, 0, PCI_HEADER_TYPE);
    if (header_type & PCI_HEADER_TYPE_MULTI_FUNC) {
        for (uint8_t func = 1; func < 8; func++) {
            if (pci_config_read16(bus, slot, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_function(bus, slot, func);
            }
        }
    }
}

void scan_bus(uint8_t bus) {
    for (uint8_t slot = 0; slot < 32; slot++) {
        scan_slot(bus, slot);
    }
}
}  // namespace

// Read 32-bit from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    uint32_t address = (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
                       (offset & 0xFC) | 0x80000000UL;

    outl(PCI_CONFIG_ADDRESS, address);
    return inl(PCI_CONFIG_DATA);
}

// Read 16-bit from PCI configuration space
auto pci_config_read16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint16_t {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    return (dword >> ((offset & 2) * 8)) & 0xFFFF;
}

// Read 8-bit from PCI configuration space
auto pci_config_read8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint8_t {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    return (dword >> ((offset & 3) * 8)) & 0xFF;
}

// Write 32-bit to PCI configuration space
void pci_config_write32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint32_t value) {
    uint32_t address = (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
                       (offset & 0xFC) | 0x80000000UL;

    outl(PCI_CONFIG_ADDRESS, address);
    outl(PCI_CONFIG_DATA, value);
}

// Write 16-bit to PCI configuration space
void pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value) {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    int shift = (offset & 2) * 8;
    dword = (dword & ~(0xFFFF << shift)) | (static_cast<uint32_t>(value) << shift);
    pci_config_write32(bus, slot, func, offset, dword);
}

// Write 8-bit to PCI configuration space
void pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value) {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    int shift = (offset & 3) * 8;
    dword = (dword & ~(0xFF << shift)) | (static_cast<uint32_t>(value) << shift);
    pci_config_write32(bus, slot, func, offset, dword);
}

// Enumerate all PCI devices across all buses
auto pci_enumerate_all() -> int {
    if (enumerated) return device_count;

    device_count = 0;

    // Check if host bridge is multi-function (multiple root buses)
    uint8_t header_type = pci_config_read8(0, 0, 0, PCI_HEADER_TYPE);
    if (header_type & PCI_HEADER_TYPE_MULTI_FUNC) {
        for (uint8_t func = 0; func < 8; func++) {
            if (pci_config_read16(0, 0, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_bus(func);
            }
        }
    } else {
        scan_bus(0);
    }

    enumerated = true;

    ker::mod::dbg::log("PCI: Found %d devices", device_count);
    for (int i = 0; i < device_count; i++) {
        auto* d = &discovered_devices[i];
        ker::mod::dbg::log("  PCI %d:%d.%d  vendor=%x device=%x class=%x:%x prog_if=%x", (int)d->bus, (int)d->slot, (int)d->function,
                           (int)d->vendor_id, (int)d->device_id, (int)d->class_code, (int)d->subclass_code, (int)d->prog_if);
    }

    return device_count;
}

auto pci_get_device(size_t idx) -> PCIDevice* {
    if (idx >= static_cast<size_t>(device_count)) return nullptr;
    return &discovered_devices[idx];
}

auto pci_device_count() -> size_t { return static_cast<size_t>(device_count); }

auto pci_find_by_class(uint8_t cls, uint8_t sub) -> PCIDevice* {
    if (!enumerated) pci_enumerate_all();
    for (int i = 0; i < device_count; i++) {
        if (discovered_devices[i].class_code == cls && discovered_devices[i].subclass_code == sub) {
            return &discovered_devices[i];
        }
    }
    return nullptr;
}

auto pci_find_by_vendor_device(uint16_t vendor, uint16_t device) -> PCIDevice* {
    if (!enumerated) pci_enumerate_all();
    for (int i = 0; i < device_count; i++) {
        if (discovered_devices[i].vendor_id == vendor && discovered_devices[i].device_id == device) {
            return &discovered_devices[i];
        }
    }
    return nullptr;
}

void pci_enable_bus_master(PCIDevice* dev) {
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_BUS_MASTER;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);
}

void pci_enable_memory_space(PCIDevice* dev) {
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_MEM_SPACE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);
}

auto pci_find_capability(PCIDevice* dev, uint8_t cap_id) -> uint8_t {
    // Check if capabilities list is supported (bit 4 of Status register)
    uint16_t status = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_STATUS);
    if (!(status & (1 << 4))) return 0;

    uint8_t offset = pci_config_read8(dev->bus, dev->slot, dev->function, PCI_CAP_PTR);
    offset &= 0xFC;  // Align to dword boundary

    int limit = 48;  // prevent infinite loops on malformed capability chains
    while (offset != 0 && limit-- > 0) {
        uint8_t id = pci_config_read8(dev->bus, dev->slot, dev->function, offset);
        if (id == cap_id) return offset;
        offset = pci_config_read8(dev->bus, dev->slot, dev->function, offset + 1);
        offset &= 0xFC;
    }
    return 0;
}

auto pci_enable_msi(PCIDevice* dev, uint8_t vector) -> int {
    uint8_t msi_off = pci_find_capability(dev, PCI_CAP_ID_MSI);
    if (msi_off == 0) return -1;

    // MSI capability layout:
    // +0: Cap ID (8) | Next (8) | Message Control (16)
    // +4: Message Address (lower 32)
    // +8: Message Address (upper 32) [if 64-bit capable]
    // then: Message Data (16)

    uint16_t msg_ctrl = pci_config_read16(dev->bus, dev->slot, dev->function, msi_off + 2);
    bool is_64bit = (msg_ctrl >> 7) & 1;

    // Message Address: 0xFEE00000 | (dest APIC ID << 12)
    // Use destination 0 (BSP) with physical destination mode
    uint32_t msg_addr = 0xFEE00000;
    pci_config_write32(dev->bus, dev->slot, dev->function, msi_off + 4, msg_addr);

    if (is_64bit) {
        pci_config_write32(dev->bus, dev->slot, dev->function, msi_off + 8, 0);
        pci_config_write16(dev->bus, dev->slot, dev->function, msi_off + 12, vector);
    } else {
        pci_config_write16(dev->bus, dev->slot, dev->function, msi_off + 8, vector);
    }

    // Enable MSI (bit 0), request 1 vector (bits 4-6 = 000)
    msg_ctrl = (msg_ctrl & ~(0x7 << 4)) | 1;
    pci_config_write16(dev->bus, dev->slot, dev->function, msi_off + 2, msg_ctrl);

    // Disable legacy INTx
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_INT_DISABLE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

    return 0;
}

auto pci_enable_msix(PCIDevice* dev, uint8_t vector) -> int {
    uint8_t msix_off = pci_find_capability(dev, PCI_CAP_ID_MSIX);
    if (msix_off == 0) return -1;

    // MSI-X capability layout:
    // +0: Cap ID (8) | Next (8) | Message Control (16)
    //     Message Control: bits 10:0 = table size - 1, bit 14 = function mask, bit 15 = enable
    // +4: Table Offset/BIR (32)
    //     bits 2:0 = BAR index, bits 31:3 = offset within BAR
    // +8: PBA Offset/BIR (32)

    uint16_t msg_ctrl = pci_config_read16(dev->bus, dev->slot, dev->function, msix_off + 2);

    uint32_t table_off_bir = pci_config_read32(dev->bus, dev->slot, dev->function, msix_off + 4);
    uint8_t table_bir = table_off_bir & 0x7;
    uint32_t table_offset = table_off_bir & ~0x7u;

    // Ensure memory space is enabled for the table BAR
    pci_enable_memory_space(dev);

    // Map the BAR containing the MSI-X table
    auto* bar_base = pci_map_bar(dev, table_bir);
    if (bar_base == nullptr) return -1;

    auto* table = reinterpret_cast<volatile uint32_t*>(
        reinterpret_cast<volatile uint8_t*>(bar_base) + table_offset);

    // Enable MSI-X with Function Mask set (mask all vectors while configuring)
    msg_ctrl |= (1u << 15) | (1u << 14);
    pci_config_write16(dev->bus, dev->slot, dev->function, msix_off + 2, msg_ctrl);

    // Configure entry 0 (each entry is 4 x 32-bit words = 16 bytes)
    table[0] = 0xFEE00000;  // Message Address Lower (BSP, physical destination mode)
    table[1] = 0;            // Message Address Upper
    table[2] = vector;       // Message Data (interrupt vector)
    table[3] = 0;            // Vector Control (0 = unmasked)

    // Clear Function Mask to enable delivery
    msg_ctrl &= ~(1u << 14);
    pci_config_write16(dev->bus, dev->slot, dev->function, msix_off + 2, msg_ctrl);

    // Disable legacy INTx
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_INT_DISABLE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

    return 0;
}

auto pci_get_bar_addr(PCIDevice* dev, int bar_idx) -> uint64_t {
    if (bar_idx < 0 || bar_idx >= BAR_COUNT) return 0;

    uint32_t bar_val = dev->bar[bar_idx];

    if (bar_val & 1) {
        // I/O space BAR
        return bar_val & ~0x3UL;
    }

    // Memory space BAR
    uint8_t type = (bar_val >> 1) & 0x3;
    if (type == 0x02 && bar_idx + 1 < BAR_COUNT) {
        // 64-bit BAR: combine with next BAR register
        uint64_t addr = bar_val & ~0xFUL;
        addr |= static_cast<uint64_t>(dev->bar[bar_idx + 1]) << 32;
        return addr;
    }

    // 32-bit BAR
    return bar_val & ~0xFUL;
}

auto pci_get_bar_size(PCIDevice* dev, int bar_idx) -> uint64_t {
    if (bar_idx < 0 || bar_idx >= BAR_COUNT) return 0;

    uint8_t reg = PCI_BAR0 + (bar_idx * 4);
    uint32_t original = pci_config_read32(dev->bus, dev->slot, dev->function, reg);

    // I/O BAR - no MMIO size needed
    if (original & 1) {
        return 0;
    }

    // Disable memory decoding before BAR sizing to prevent KVM from trying
    // to map intermediate bogus addresses (e.g. writing 0xFFFFFFFF to a
    // 64-bit BAR while the high half still holds the original value).
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND,
                       cmd & ~PCI_COMMAND_MEM_SPACE);

    // Write all 1s to determine size
    pci_config_write32(dev->bus, dev->slot, dev->function, reg, 0xFFFFFFFF);
    uint32_t readback = pci_config_read32(dev->bus, dev->slot, dev->function, reg);
    // Restore original value
    pci_config_write32(dev->bus, dev->slot, dev->function, reg, original);

    readback &= ~0xFU;  // mask type/prefetch bits
    if (readback == 0) {
        // Restore command register and return
        pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);
        return 0;
    }

    uint8_t type = (original >> 1) & 0x3;
    if (type == 0x02 && bar_idx + 1 < BAR_COUNT) {
        // 64-bit BAR: also size the upper 32 bits
        uint8_t reg_hi = PCI_BAR0 + ((bar_idx + 1) * 4);
        uint32_t orig_hi = pci_config_read32(dev->bus, dev->slot, dev->function, reg_hi);
        pci_config_write32(dev->bus, dev->slot, dev->function, reg_hi, 0xFFFFFFFF);
        uint32_t readback_hi = pci_config_read32(dev->bus, dev->slot, dev->function, reg_hi);
        pci_config_write32(dev->bus, dev->slot, dev->function, reg_hi, orig_hi);

        // Re-enable memory decoding now that BARs are restored
        pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

        uint64_t mask = (static_cast<uint64_t>(readback_hi) << 32) | readback;
        return (~mask) + 1;
    }

    // Re-enable memory decoding now that BAR is restored
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

    // 32-bit BAR
    return static_cast<uint64_t>(~readback + 1);
}

auto pci_map_bar(PCIDevice* dev, int bar_idx) -> volatile void* {
    uint64_t phys = pci_get_bar_addr(dev, bar_idx);
    if (phys == 0) return nullptr;

    // Don't map I/O space BARs
    if (dev->bar[bar_idx] & 1) return nullptr;

    uint64_t size = pci_get_bar_size(dev, bar_idx);
    if (size == 0) size = 0x1000;  // at least one page

    uint64_t phys_aligned = phys & ~0xFFFULL;
    uint64_t end = (phys + size + 0xFFF) & ~0xFFFULL;

    // Map the MMIO range into the kernel page table
    ker::mod::mm::virt::mapRangeToKernelPageTable(ker::mod::mm::virt::Range{phys_aligned, end}, ker::mod::mm::paging::pageTypes::KERNEL);

    return reinterpret_cast<volatile void*>(ker::mod::mm::addr::getVirtPointer(phys));
}

// Legacy wrapper: find AHCI controller using full enumeration
auto pci_find_ahci_controller() -> PCIDevice* {
    if (!enumerated) pci_enumerate_all();

    for (int i = 0; i < device_count; i++) {
        auto* dev = &discovered_devices[i];
        bool is_ahci = (dev->class_code == PCI_CLASS_STORAGE && dev->subclass_code == PCI_SUBCLASS_SATA) ||
                       (dev->vendor_id == 0x8086 && dev->device_id == 0x2922);
        if (is_ahci) {
            pci_log("pci: FOUND AHCI SATA CONTROLLER\n");
            return dev;
        }
    }
    pci_log("pci: No AHCI controller found\n");
    return nullptr;
}

}  // namespace ker::dev::pci
