#include "pci.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>
#include <utility>

namespace ker::dev::pci {

using log = ker::mod::dbg::logger<"pci">;

namespace {
constexpr int MAX_PCI_DEVICES = 64;
std::array<PCIDevice, MAX_PCI_DEVICES> discovered_devices = {};
int device_count = 0;
bool enumerated = false;
constexpr int PCI_FUNCTION_COUNT = 8;
constexpr int PCI_SLOT_COUNT = 32;

// 32-bit port I/O using inline assembly
inline auto inl(uint16_t port) -> uint32_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}

inline void outl(uint16_t port, uint32_t value) { asm volatile("outl %0, %1" : : "a"(value), "Nd"(port)); }

void scan_bus(uint8_t bus);

void scan_function(uint8_t bus, uint8_t slot, uint8_t func) {
    uint16_t const VENDOR = pci_config_read16(bus, slot, func, PCI_VENDOR_ID);
    if (VENDOR == 0xFFFF) {
        return;
    }
    if (device_count >= MAX_PCI_DEVICES) {
        return;
    }

    auto* dev = &discovered_devices.at(static_cast<size_t>(device_count));
    dev->bus = bus;
    dev->slot = slot;
    dev->function = func;
    dev->vendor_id = VENDOR;
    dev->device_id = pci_config_read16(bus, slot, func, PCI_DEVICE_ID);

    // Read class/subclass/prog_if from the 32-bit register at offset 0x08
    uint32_t const CLASS_DWORD = pci_config_read32(bus, slot, func, PCI_CLASS_CODE);
    dev->class_code = (CLASS_DWORD >> 24) & 0xFF;
    dev->subclass_code = (CLASS_DWORD >> 16) & 0xFF;
    dev->prog_if = (CLASS_DWORD >> 8) & 0xFF;

    dev->header_type = pci_config_read8(bus, slot, func, PCI_HEADER_TYPE);
    dev->interrupt_line = pci_config_read8(bus, slot, func, PCI_INTERRUPT_LINE);
    dev->interrupt_pin = pci_config_read8(bus, slot, func, PCI_INTERRUPT_PIN);

    // Read BARs for type 0 (normal) devices
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_NORMAL) {
        for (size_t i = 0; i < BAR_COUNT; i++) {
            dev->bar.at(i) = pci_config_read32(bus, slot, func, static_cast<uint8_t>(PCI_BAR0 + (i * sizeof(uint32_t))));
        }
    } else {
        for (unsigned int& i : dev->bar) {
            i = 0;
        }
    }

    device_count++;

    // If this is a PCI-to-PCI bridge, scan the secondary bus
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_BRIDGE) {
        uint8_t const SECONDARY_BUS = pci_config_read8(bus, slot, func, PCI_SECONDARY_BUS);
        if (SECONDARY_BUS != 0) {
            scan_bus(SECONDARY_BUS);
        }
    }
}

void scan_slot(uint8_t bus, uint8_t slot) {
    uint16_t const VENDOR = pci_config_read16(bus, slot, 0, PCI_VENDOR_ID);
    if (VENDOR == 0xFFFF) {
        return;
    }

    scan_function(bus, slot, 0);

    // Check for multi-function device
    uint8_t const HEADER_TYPE = pci_config_read8(bus, slot, 0, PCI_HEADER_TYPE);
    if ((HEADER_TYPE & PCI_HEADER_TYPE_MULTI_FUNC) != 0) {
        for (uint8_t func = 1; func < PCI_FUNCTION_COUNT; func++) {
            if (pci_config_read16(bus, slot, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_function(bus, slot, func);
            }
        }
    }
}

void scan_bus(uint8_t bus) {
    for (uint8_t slot = 0; slot < PCI_SLOT_COUNT; slot++) {
        scan_slot(bus, slot);
    }
}
}  // namespace

// Read 32-bit from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    uint32_t const ADDRESS = (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
                             (offset & 0xFC) | 0x80000000UL;

    outl(PCI_CONFIG_ADDRESS, ADDRESS);
    return inl(PCI_CONFIG_DATA);
}

// Read 16-bit from PCI configuration space
auto pci_config_read16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint16_t {
    uint32_t const DWORD = pci_config_read32(bus, slot, func, offset);
    return (DWORD >> ((offset & 2) * 8)) & 0xFFFF;
}

// Read 8-bit from PCI configuration space
auto pci_config_read8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint8_t {
    uint32_t const DWORD = pci_config_read32(bus, slot, func, offset);
    return (DWORD >> ((offset & 3) * 8)) & 0xFF;
}

// Write 32-bit to PCI configuration space
auto pci_config_write32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint32_t value) -> void {
    uint32_t const ADDRESS = (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
                             (offset & 0xFC) | 0x80000000UL;

    outl(PCI_CONFIG_ADDRESS, ADDRESS);
    outl(PCI_CONFIG_DATA, value);
}

// Write 16-bit to PCI configuration space
auto pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value) -> void {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    int const SHIFT = (offset & 2) * 8;
    dword = (dword & ~(0xFFFF << SHIFT)) | (static_cast<uint32_t>(value) << SHIFT);
    pci_config_write32(bus, slot, func, offset, dword);
}

// Write 8-bit to PCI configuration space
auto pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value) -> void {
    uint32_t dword = pci_config_read32(bus, slot, func, offset);
    int const SHIFT = (offset & 3) * 8;
    dword = (dword & ~(0xFF << SHIFT)) | (static_cast<uint32_t>(value) << SHIFT);
    pci_config_write32(bus, slot, func, offset, dword);
}

// Enumerate all PCI devices across all buses
auto pci_enumerate_all() -> int {
    if (enumerated) {
        return device_count;
    }

    device_count = 0;

    // Check if host bridge is multi-function (multiple root buses)
    uint8_t const HEADER_TYPE = pci_config_read8(0, 0, 0, PCI_HEADER_TYPE);
    if ((HEADER_TYPE & PCI_HEADER_TYPE_MULTI_FUNC) != 0) {
        for (uint8_t func = 0; func < PCI_FUNCTION_COUNT; func++) {
            if (pci_config_read16(0, 0, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_bus(func);
            }
        }
    } else {
        scan_bus(0);
    }

    enumerated = true;

    log::info("found %d devices", device_count);
    for (int i = 0; i < device_count; i++) {
        auto* d = &discovered_devices.at(static_cast<size_t>(i));
        log::debug("%d:%d.%d vendor=%x device=%x class=%x:%x prog_if=%x", static_cast<int>(d->bus), static_cast<int>(d->slot),
                   static_cast<int>(d->function), static_cast<int>(d->vendor_id), static_cast<int>(d->device_id),
                   static_cast<int>(d->class_code), static_cast<int>(d->subclass_code), static_cast<int>(d->prog_if));
    }

    return device_count;
}

auto pci_get_device(size_t idx) -> PCIDevice* {
    if (std::cmp_greater_equal(idx, device_count)) {
        return nullptr;
    }
    return &discovered_devices.at(idx);
}

auto pci_device_count() -> size_t { return static_cast<size_t>(device_count); }

auto pci_find_by_class(uint8_t cls, uint8_t sub) -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }
    for (int i = 0; i < device_count; i++) {
        auto& dev = discovered_devices.at(static_cast<size_t>(i));
        if (dev.class_code == cls && dev.subclass_code == sub) {
            return &dev;
        }
    }
    return nullptr;
}

auto pci_find_by_vendor_device(uint16_t vendor, uint16_t device) -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }
    for (int i = 0; i < device_count; i++) {
        auto& dev = discovered_devices.at(static_cast<size_t>(i));
        if (dev.vendor_id == vendor && dev.device_id == device) {
            return &dev;
        }
    }
    return nullptr;
}

auto pci_enable_bus_master(PCIDevice* dev) -> void {
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_BUS_MASTER;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);
}

auto pci_enable_memory_space(PCIDevice* dev) -> void {
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_MEM_SPACE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);
}

auto pci_find_capability(PCIDevice* dev, uint8_t cap_id) -> uint8_t {
    // Check if capabilities list is supported (bit 4 of Status register)
    uint16_t const STATUS = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_STATUS);
    if ((STATUS & (1 << 4)) == 0) {
        return 0;
    }

    uint8_t offset = pci_config_read8(dev->bus, dev->slot, dev->function, PCI_CAP_PTR);
    offset &= 0xFC;  // Align to dword boundary

    int limit = 48;  // prevent infinite loops on malformed capability chains
    while (offset != 0 && limit-- > 0) {
        uint8_t const ID = pci_config_read8(dev->bus, dev->slot, dev->function, offset);
        if (ID == cap_id) {
            return offset;
        }
        offset = pci_config_read8(dev->bus, dev->slot, dev->function, offset + 1);
        offset &= 0xFC;
    }
    return 0;
}

auto pci_enable_msi(PCIDevice* dev, uint8_t vector, uint64_t target_cpu) -> int {
    uint8_t const MSI_OFF = pci_find_capability(dev, PCI_CAP_ID_MSI);
    if (MSI_OFF == 0) {
        return -1;
    }

    // MSI capability layout:
    // +0: Cap ID (8) | Next (8) | Message Control (16)
    // +4: Message Address (lower 32)
    // +8: Message Address (upper 32) [if 64-bit capable]
    // then: Message Data (16)

    uint16_t msg_ctrl = pci_config_read16(dev->bus, dev->slot, dev->function, MSI_OFF + 2);
    bool const IS_64BIT = ((msg_ctrl >> 7) & 1) != 0;

    // Message Address: 0xFEE00000 | (dest APIC ID << 12), physical destination mode.
    // target_cpu is a logical CPU index; translate to APIC ID for the hardware.
    uint32_t const APIC_ID = ker::mod::smt::get_apic_id_for_cpu(target_cpu);
    uint32_t const MSG_ADDR = 0xFEE00000U | (APIC_ID << 12);
    pci_config_write32(dev->bus, dev->slot, dev->function, MSI_OFF + 4, MSG_ADDR);

    if (IS_64BIT) {
        pci_config_write32(dev->bus, dev->slot, dev->function, MSI_OFF + 8, 0);
        pci_config_write16(dev->bus, dev->slot, dev->function, MSI_OFF + 12, vector);
    } else {
        pci_config_write16(dev->bus, dev->slot, dev->function, MSI_OFF + 8, vector);
    }

    // Enable MSI (bit 0), request 1 vector (bits 4-6 = 000)
    msg_ctrl = (msg_ctrl & ~(0x7 << 4)) | 1;
    pci_config_write16(dev->bus, dev->slot, dev->function, MSI_OFF + 2, msg_ctrl);

    // Disable legacy INTx
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_INT_DISABLE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

    return 0;
}

auto pci_enable_msix(PCIDevice* dev, uint8_t vector, uint64_t target_cpu) -> int {
    uint8_t const MSIX_OFF = pci_find_capability(dev, PCI_CAP_ID_MSIX);
    if (MSIX_OFF == 0) {
        return -1;
    }

    // MSI-X capability layout:
    // +0: Cap ID (8) | Next (8) | Message Control (16)
    //     Message Control: bits 10:0 = table size - 1, bit 14 = function mask, bit 15 = enable
    // +4: Table Offset/BIR (32)
    //     bits 2:0 = BAR index, bits 31:3 = offset within BAR
    // +8: PBA Offset/BIR (32)

    uint16_t msg_ctrl = pci_config_read16(dev->bus, dev->slot, dev->function, MSIX_OFF + 2);

    uint32_t const TABLE_OFF_BIR = pci_config_read32(dev->bus, dev->slot, dev->function, MSIX_OFF + 4);
    uint8_t const TABLE_BIR = TABLE_OFF_BIR & 0x7;
    uint32_t const TABLE_OFFSET = TABLE_OFF_BIR & ~0x7U;

    // Ensure memory space is enabled for the table BAR
    pci_enable_memory_space(dev);

    // Map the BAR containing the MSI-X table
    auto* bar_base = pci_map_bar(dev, TABLE_BIR);
    if (bar_base == nullptr) {
        return -1;
    }

    auto* table = reinterpret_cast<volatile uint32_t*>(reinterpret_cast<volatile uint8_t*>(bar_base) + TABLE_OFFSET);

    // Enable MSI-X with Function Mask set (mask all vectors while configuring)
    msg_ctrl |= (1U << 15) | (1U << 14);
    pci_config_write16(dev->bus, dev->slot, dev->function, MSIX_OFF + 2, msg_ctrl);

    // Configure entry 0 (each entry is 4 x 32-bit words = 16 bytes)
    // target_cpu is a logical CPU index; translate to APIC ID for physical destination mode.
    uint32_t const APIC_ID = ker::mod::smt::get_apic_id_for_cpu(target_cpu);
    table[0] = 0xFEE00000U | (APIC_ID << 12);  // Message Address Lower
    table[1] = 0;                              // Message Address Upper
    table[2] = vector;                         // Message Data (interrupt vector)
    table[3] = 0;                              // Vector Control (0 = unmasked)

    // Clear Function Mask to enable delivery
    msg_ctrl &= ~(1U << 14);
    pci_config_write16(dev->bus, dev->slot, dev->function, MSIX_OFF + 2, msg_ctrl);

    // Disable legacy INTx
    uint16_t cmd = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    cmd |= PCI_COMMAND_INT_DISABLE;
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, cmd);

    return 0;
}

auto pci_get_bar_addr(PCIDevice* dev, int bar_idx) -> uint64_t {
    if (bar_idx < 0 || std::cmp_greater_equal(bar_idx, BAR_COUNT)) {
        return 0;
    }

    auto const BAR_INDEX = static_cast<size_t>(bar_idx);
    uint32_t const BAR_VAL = dev->bar.at(BAR_INDEX);

    if ((BAR_VAL & 1) != 0U) {
        // I/O space BAR
        return BAR_VAL & ~0x3UL;
    }

    // Memory space BAR
    uint8_t const TYPE = (BAR_VAL >> 1) & 0x3;
    if (TYPE == 0x02 && (BAR_INDEX + 1) < BAR_COUNT) {
        // 64-bit BAR: combine with next BAR register
        uint64_t addr = BAR_VAL & ~0xFUL;
        addr |= static_cast<uint64_t>(dev->bar.at(BAR_INDEX + 1)) << 32;
        return addr;
    }

    // 32-bit BAR
    return BAR_VAL & ~0xFUL;
}

auto pci_get_bar_size(PCIDevice* dev, int bar_idx) -> uint64_t {
    if (bar_idx < 0 || std::cmp_greater_equal(bar_idx, BAR_COUNT)) {
        return 0;
    }

    auto const BAR_INDEX = static_cast<size_t>(bar_idx);
    auto const REG = static_cast<uint8_t>(PCI_BAR0 + (BAR_INDEX * sizeof(uint32_t)));
    uint32_t const ORIGINAL = pci_config_read32(dev->bus, dev->slot, dev->function, REG);

    // I/O BAR - no MMIO size needed
    if ((ORIGINAL & 1) != 0U) {
        return 0;
    }

    uint8_t const TYPE = (ORIGINAL >> 1) & 0x3;

    // Disable memory decoding before BAR sizing to prevent KVM from trying
    // to map intermediate bogus addresses (e.g. writing 0xFFFFFFFF to a
    // 64-bit BAR while the high half still holds the original value).
    uint16_t const CMD = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_COMMAND);
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, CMD & ~PCI_COMMAND_MEM_SPACE);

    if (TYPE == 0x02 && (BAR_INDEX + 1) < BAR_COUNT) {
        auto const REG_HI = static_cast<uint8_t>(PCI_BAR0 + ((BAR_INDEX + 1) * sizeof(uint32_t)));
        uint32_t const ORIGINAL_HI = pci_config_read32(dev->bus, dev->slot, dev->function, REG_HI);

        // For 64-bit BARs, both halves must be all-ones at the same time.
        pci_config_write32(dev->bus, dev->slot, dev->function, REG, 0xFFFFFFFFU);
        pci_config_write32(dev->bus, dev->slot, dev->function, REG_HI, 0xFFFFFFFFU);
        uint32_t const READBACK_LO = pci_config_read32(dev->bus, dev->slot, dev->function, REG) & ~0xFU;
        uint32_t const READBACK_HI = pci_config_read32(dev->bus, dev->slot, dev->function, REG_HI);

        pci_config_write32(dev->bus, dev->slot, dev->function, REG, ORIGINAL);
        pci_config_write32(dev->bus, dev->slot, dev->function, REG_HI, ORIGINAL_HI);
        pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, CMD);

        uint64_t const MASK = (static_cast<uint64_t>(READBACK_HI) << 32) | READBACK_LO;
        if (MASK == 0) {
            return 0;
        }
        return (~MASK) + 1;
    }

    pci_config_write32(dev->bus, dev->slot, dev->function, REG, 0xFFFFFFFFU);
    uint32_t const READBACK = pci_config_read32(dev->bus, dev->slot, dev->function, REG) & ~0xFU;
    pci_config_write32(dev->bus, dev->slot, dev->function, REG, ORIGINAL);
    pci_config_write16(dev->bus, dev->slot, dev->function, PCI_COMMAND, CMD);

    if (READBACK == 0) {
        return 0;
    }

    // Keep the complement arithmetic 32-bit. Widening before `~` turns a
    // 4 KiB BAR mask (0xfffff000) into a huge 64-bit size.
    return static_cast<uint64_t>(static_cast<uint32_t>(~READBACK + 1U));
}

auto pci_map_bar(PCIDevice* dev, int bar_idx) -> void* {
    if (bar_idx < 0 || std::cmp_greater_equal(bar_idx, BAR_COUNT)) {
        return nullptr;
    }

    uint64_t const PHYS = pci_get_bar_addr(dev, bar_idx);
    if (PHYS == 0) {
        return nullptr;
    }

    // Don't map I/O space BARs
    auto const BAR_INDEX = static_cast<size_t>(bar_idx);
    if ((dev->bar.at(BAR_INDEX) & 1) != 0U) {
        return nullptr;
    }

    uint64_t size = pci_get_bar_size(dev, bar_idx);
    if (size == 0) {
        size = 0x1000;  // at least one page
    }

    uint64_t const PHYS_ALIGNED = PHYS & ~0xFFFULL;
    uint64_t const END = (PHYS + size + 0xFFF) & ~0xFFFULL;

    // Map the MMIO range into the kernel page table (uncacheable for MMIO correctness)
    ker::mod::mm::virt::map_range_to_kernel_page_table(ker::mod::mm::virt::Range{.start = PHYS_ALIGNED, .end = END},
                                                       ker::mod::mm::paging::page_types::MMIO);

    return reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
}

// Legacy wrapper: find AHCI controller using full enumeration
auto pci_configure_msix_entry(PCIDevice* dev, uint32_t entry_idx, uint8_t vector, uint64_t target_cpu) -> int {
    uint8_t const MSIX_OFF = pci_find_capability(dev, PCI_CAP_ID_MSIX);
    if (MSIX_OFF == 0) {
        return -1;
    }

    // Validate entry_idx against the table size in the capability header.
    uint16_t const MSG_CTRL = pci_config_read16(dev->bus, dev->slot, dev->function, MSIX_OFF + 2);
    uint32_t const TABLE_SIZE = (MSG_CTRL & 0x07FFU) + 1U;
    if (entry_idx >= TABLE_SIZE) {
        return -1;
    }

    uint32_t const TABLE_OFF_BIR = pci_config_read32(dev->bus, dev->slot, dev->function, MSIX_OFF + 4);
    uint8_t const TABLE_BIR = TABLE_OFF_BIR & 0x7;
    uint32_t const TABLE_OFFSET = TABLE_OFF_BIR & ~0x7U;

    auto* bar_base = pci_map_bar(dev, TABLE_BIR);
    if (bar_base == nullptr) {
        return -1;
    }

    auto* table = reinterpret_cast<volatile uint32_t*>(reinterpret_cast<uint8_t*>(bar_base) + TABLE_OFFSET);

    // Each entry is 4 x 32-bit words = 16 bytes.
    volatile uint32_t* entry = table + (static_cast<size_t>(entry_idx) * 4);
    uint32_t const APIC_ID = ker::mod::smt::get_apic_id_for_cpu(target_cpu);
    entry[0] = 0xFEE00000U | (APIC_ID << 12);  // Message Address Lower
    entry[1] = 0;                              // Message Address Upper
    entry[2] = vector;                         // Message Data
    entry[3] = 0;                              // Vector Control (bit 0 = 0 -> unmasked)

    return 0;
}

auto pci_find_ahci_controller() -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }

    for (int i = 0; i < device_count; i++) {
        auto* dev = &discovered_devices.at(static_cast<size_t>(i));
        bool const IS_AHCI = (dev->class_code == PCI_CLASS_STORAGE && dev->subclass_code == PCI_SUBCLASS_SATA) ||
                             (dev->vendor_id == 0x8086 && dev->device_id == 0x2922);
        if (IS_AHCI) {
            pci_log("pci: FOUND AHCI SATA CONTROLLER\n");
            return dev;
        }
    }
    pci_log("pci: No AHCI controller found\n");
    return nullptr;
}

}  // namespace ker::dev::pci
