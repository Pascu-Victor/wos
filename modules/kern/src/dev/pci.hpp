#pragma once

#include <cstddef>
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
constexpr uint8_t PCI_CLASS_CODE = 0x08;  // 32-bit register: rev | prog_if | subclass | class
constexpr uint8_t PCI_REVISION_ID = 0x08;
constexpr uint8_t PCI_PROG_IF = 0x09;
constexpr uint8_t PCI_SUBCLASS = 0x0A;
constexpr uint8_t PCI_CLASS = 0x0B;
constexpr uint8_t PCI_HEADER_TYPE = 0x0E;
constexpr uint8_t PCI_BAR0 = 0x10;
constexpr uint8_t PCI_BAR1 = 0x14;
constexpr uint8_t PCI_BAR2 = 0x18;
constexpr uint8_t PCI_BAR3 = 0x1C;
constexpr uint8_t PCI_BAR4 = 0x20;
constexpr uint8_t PCI_BAR5 = 0x24;
constexpr uint8_t PCI_CAP_PTR = 0x34;
constexpr uint8_t PCI_INTERRUPT_LINE = 0x3C;
constexpr uint8_t PCI_INTERRUPT_PIN = 0x3D;

// PCI Bridge registers (header type 1)
constexpr uint8_t PCI_SECONDARY_BUS = 0x19;

// PCI Command register bits
constexpr uint16_t PCI_COMMAND_IO_SPACE = 0x0001;
constexpr uint16_t PCI_COMMAND_MEM_SPACE = 0x0002;
constexpr uint16_t PCI_COMMAND_BUS_MASTER = 0x0004;
constexpr uint16_t PCI_COMMAND_INT_DISABLE = 0x0400;

// PCI Class Codes
constexpr uint8_t PCI_CLASS_STORAGE = 0x01;
constexpr uint8_t PCI_CLASS_NETWORK = 0x02;
constexpr uint8_t PCI_CLASS_DISPLAY = 0x03;
constexpr uint8_t PCI_CLASS_BRIDGE = 0x06;
constexpr uint8_t PCI_CLASS_SERIAL_BUS = 0x0C;

// PCI Subclass Codes
constexpr uint8_t PCI_SUBCLASS_SATA = 0x06;
constexpr uint8_t PCI_SUBCLASS_ETHERNET = 0x00;
constexpr uint8_t PCI_SUBCLASS_PCI_BRIDGE = 0x04;
constexpr uint8_t PCI_SUBCLASS_USB = 0x03;

// USB Programming Interface
constexpr uint8_t PCI_PROG_IF_XHCI = 0x30;

// PCI Capability IDs
constexpr uint8_t PCI_CAP_ID_MSI = 0x05;
constexpr uint8_t PCI_CAP_ID_MSIX = 0x11;

// Header types
constexpr uint8_t PCI_HEADER_TYPE_NORMAL = 0x00;
constexpr uint8_t PCI_HEADER_TYPE_BRIDGE = 0x01;
constexpr uint8_t PCI_HEADER_TYPE_MULTI_FUNC = 0x80;

constexpr int BAR_COUNT = 6;

// PCI Device structure
struct PCIDevice {
    uint8_t bus;
    uint8_t slot;
    uint8_t function;
    uint16_t vendor_id;
    uint16_t device_id;
    uint8_t class_code;
    uint8_t subclass_code;
    uint32_t bar[BAR_COUNT];  // Base Address Registers NOLINT
    uint8_t prog_if;
    uint8_t header_type;
    uint8_t interrupt_line;
    uint8_t interrupt_pin;
};

// Read from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t;
auto pci_config_read16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint16_t;
auto pci_config_read8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint8_t;

// Write to PCI configuration space
void pci_config_write32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint32_t value);
void pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value);
void pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value);

// Full PCI bus enumeration (all buses, slots, functions; follows bridges)
auto pci_enumerate_all() -> int;

// Device access
auto pci_get_device(size_t idx) -> PCIDevice*;
auto pci_device_count() -> size_t;

// Find devices by class/subclass or vendor/device
auto pci_find_by_class(uint8_t cls, uint8_t sub) -> PCIDevice*;
auto pci_find_by_vendor_device(uint16_t vendor, uint16_t device) -> PCIDevice*;

// Enable PCI features on a device
void pci_enable_bus_master(PCIDevice* dev);
void pci_enable_memory_space(PCIDevice* dev);

// PCI Capability support
auto pci_find_capability(PCIDevice* dev, uint8_t cap_id) -> uint8_t;  // config offset, 0 = not found
auto pci_enable_msi(PCIDevice* dev, uint8_t vector) -> int;           // 0 = success

// BAR address resolution (handles 64-bit BARs)
auto pci_get_bar_addr(PCIDevice* dev, int bar_idx) -> uint64_t;

// BAR size detection (standard PCI BAR sizing)
auto pci_get_bar_size(PCIDevice* dev, int bar_idx) -> uint64_t;

// Map a memory BAR into the kernel page table and return the virtual address.
// Returns nullptr for I/O BARs or invalid BARs.
auto pci_map_bar(PCIDevice* dev, int bar_idx) -> volatile void*;

// Legacy: find AHCI controller (wrapper around enumerate + find_by_class)
auto pci_find_ahci_controller() -> PCIDevice*;

// PCI Logging Control
inline void pci_log(const char* msg) {
#ifdef PCI_DEBUG
    ker::mod::io::serial::write(msg);
#else
    (void)msg;
#endif
}

inline void pci_log_hex(uint64_t value) {
#ifdef PCI_DEBUG
    ker::mod::io::serial::writeHex(value);
#else
    (void)value;
#endif
}

}  // namespace ker::dev::pci
