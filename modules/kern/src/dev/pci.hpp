#pragma once

#include <cstdint>

namespace ker::dev::pci {

// PCI Configuration Space Access
// QEMU provides PCI via port I/O (0xCF8 and 0xCFC)

constexpr uint16_t PCI_CONFIG_ADDRESS = 0xCF8;
constexpr uint16_t PCI_CONFIG_DATA = 0xCFC;

// PCI Configuration Register Offsets
constexpr uint8_t PCI_VENDOR_ID = 0x00;
constexpr uint8_t PCI_DEVICE_ID = 0x02;
constexpr uint8_t PCI_COMMAND = 0x04;
constexpr uint8_t PCI_STATUS = 0x06;
constexpr uint8_t PCI_CLASS_CODE = 0x08;
constexpr uint8_t PCI_HEADER_TYPE = 0x0E;
constexpr uint8_t PCI_BAR0 = 0x10;
constexpr uint8_t PCI_BAR1 = 0x14;
constexpr uint8_t PCI_BAR2 = 0x18;
constexpr uint8_t PCI_BAR3 = 0x1C;
constexpr uint8_t PCI_BAR4 = 0x20;
constexpr uint8_t PCI_BAR5 = 0x24;

// PCI Class Codes
constexpr uint8_t PCI_CLASS_STORAGE = 0x01;

// PCI Subclass Codes (for Storage devices)
constexpr uint8_t PCI_SUBCLASS_SATA = 0x06;

constexpr int BAR_COUNT = 6;

// PCI Device structures
struct PCIDevice {
    uint8_t bus;
    uint8_t slot;
    uint8_t function;
    uint16_t vendor_id;
    uint16_t device_id;
    uint8_t class_code;
    uint8_t subclass_code;
    uint32_t bar[BAR_COUNT];  // Base Address Registers NOLINT
};

// Read from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t;
auto pci_config_read16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint16_t;
auto pci_config_read8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint8_t;

// Write to PCI configuration space
void pci_config_write32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint32_t value);
void pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value);
void pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value);

// Enumerate PCI devices and find SATA controllers
auto pci_find_ahci_controller() -> PCIDevice*;

}  // namespace ker::dev::pci
