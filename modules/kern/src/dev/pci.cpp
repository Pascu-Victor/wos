#include "pci.hpp"

#include <cstdint>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::dev::pci {

namespace {
// Static buffer for discovered PCI devices
// TODO: make dynamic
constexpr int MAX_PCI_DEVICES = 16;
PCIDevice discovered_devices[MAX_PCI_DEVICES];
int device_count = 0;

// 32-bit port I/O using inline assembly
inline auto inl(uint16_t port) -> uint32_t {
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}

inline void outl(uint16_t port, uint32_t value) { asm volatile("outl %0, %1" : : "a"(value), "Nd"(port)); }
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

// Enumerate PCI devices looking for AHCI (SATA) controller
auto pci_find_ahci_controller() -> PCIDevice* {
    ker::mod::io::serial::write("pci: Scanning for AHCI controller...\n");

    // Test PCI access FIRST, before allocating
    ker::mod::io::serial::write("pci: Testing port I/O - reading vendor at slot 0:0...\n");
    uint32_t test_address = (0 << 16) | (0 << 11) | (0 << 8) | 0 | 0x80000000UL;
    ker::mod::io::serial::write("pci: Address to write to 0xCF8: 0x");
    ker::mod::io::serial::writeHex(test_address);
    ker::mod::io::serial::write("\n");

    outl(0xCF8, test_address);
    uint32_t test_data = inl(0xCFC);
    ker::mod::io::serial::write("pci: Data read from 0xCFC: 0x");
    ker::mod::io::serial::writeHex(test_data);
    ker::mod::io::serial::write("\n");

    if (test_data == 0xFFFFFFFF) {
        ker::mod::io::serial::write("pci: Port I/O appears broken - got 0xFFFFFFFF (no device)\n");
    } else if (test_data == 0) {
        ker::mod::io::serial::write("pci: Port I/O returned 0 - device might not exist or port access not working\n");
    } else {
        ker::mod::io::serial::write("pci: Port I/O returned valid-looking data\n");
    }

    // No allocation needed - we have a static buffer
    device_count = 0;

    // Scan bus 0 (primary), all slots, function 0
    // In QEMU, AHCI is typically on bus 0, slot 1F, function 2
    // But we'll scan to be safe
    for (uint8_t slot = 0; slot < 32 && device_count < MAX_PCI_DEVICES; ++slot) {
        for (uint8_t func = 0; func < 8; ++func) {
            uint16_t vendor = pci_config_read16(0, slot, func, PCI_VENDOR_ID);

            // Check if device exists (vendor ID != 0xFFFF)
            if (vendor == 0xFFFF) {
                continue;
            }

            uint16_t device_id = pci_config_read16(0, slot, func, PCI_DEVICE_ID);
            uint8_t class_code = pci_config_read8(0, slot, func, PCI_CLASS_CODE);
            uint8_t subclass = pci_config_read8(0, slot, func, PCI_CLASS_CODE + 1);

            ker::mod::io::serial::write("pci: Found device at slot ");
            ker::mod::io::serial::writeHex(slot);
            ker::mod::io::serial::write(":0x");
            ker::mod::io::serial::writeHex(func);
            ker::mod::io::serial::write(" - vendor 0x");
            ker::mod::io::serial::writeHex(vendor);
            ker::mod::io::serial::write(", device 0x");
            ker::mod::io::serial::writeHex(device_id);
            ker::mod::io::serial::write(", class 0x");
            ker::mod::io::serial::writeHex(class_code);
            ker::mod::io::serial::write(", subclass 0x");
            ker::mod::io::serial::writeHex(subclass);
            ker::mod::io::serial::write("\n");

            // Check if this is a SATA AHCI controller
            // Class 0x01 = Storage, Subclass 0x06 = SATA
            // Also check for Intel AHCI device IDs (0x2922, 0x2923, etc) which may misreport class
            bool is_ahci = (class_code == PCI_CLASS_STORAGE && subclass == PCI_SUBCLASS_SATA) || (vendor == 0x8086 && device_id == 0x2922);

            if (is_ahci) {
                ker::mod::io::serial::write("pci: FOUND AHCI SATA CONTROLLER at slot ");
                ker::mod::io::serial::writeHex(slot);
                ker::mod::io::serial::write(":0x");
                ker::mod::io::serial::writeHex(func);
                ker::mod::io::serial::write("!\n");

                PCIDevice* dev = &discovered_devices[device_count];
                dev->bus = 0;
                dev->slot = slot;
                dev->function = func;
                dev->vendor_id = vendor;
                dev->device_id = device_id;
                dev->class_code = class_code;
                dev->subclass_code = subclass;

                // Read BARs
                for (int i = 0; i < BAR_COUNT; i++) {
                    dev->bar[i] = pci_config_read32(0, slot, func, PCI_BAR0 + (i * 4));
                    ker::mod::io::serial::write("pci:   BAR");
                    ker::mod::io::serial::writeHex(i);
                    ker::mod::io::serial::write(" = 0x");
                    ker::mod::io::serial::writeHex(dev->bar[i]);
                    ker::mod::io::serial::write("\n");
                }

                device_count++;
                return dev;  // Return first AHCI device found
            }
        }
    }
    ker::mod::io::serial::write("pci: No AHCI controller found\n");
    return nullptr;
}

}  // namespace ker::dev::pci
