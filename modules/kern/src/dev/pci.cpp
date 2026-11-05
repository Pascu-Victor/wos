#include "pci.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/domains.hpp>
#include <dev/pci_bar_probe.hpp>
#include <dev/pci_extended.hpp>
#include <dev/pci_resource.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>

namespace ker::dev::pci {

using log = ker::mod::dbg::logger<"pci">;

namespace {
constexpr int MAX_PCI_DEVICES = 64;
std::array<PCIDevice, MAX_PCI_DEVICES> discovered_devices = {};
int device_count = 0;
bool enumerated = false;
bool enumeration_error = false;
std::array<bool, 256> visited_buses{};
mod::sys::Spinlock config_lock{};
struct ConfigGuard {
    uint64_t flags = config_lock.lock_irqsave();
    ~ConfigGuard() { config_lock.unlock_irqrestore(flags); }
    ConfigGuard() = default;
    ConfigGuard(const ConfigGuard&) = delete;
    auto operator=(const ConfigGuard&) -> ConfigGuard& = delete;
};
constexpr int PCI_FUNCTION_COUNT = 8;
constexpr int PCI_SLOT_COUNT = 32;

// 32-bit port I/O using inline assembly
inline auto inl(uint16_t port) -> uint32_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port) : "memory");
    return result;
}

inline void outl(uint16_t port, uint32_t value) { asm volatile("outl %0, %1" : : "a"(value), "Nd"(port) : "memory"); }
inline void outw(uint16_t port, uint16_t value) { asm volatile("outw %0, %1" : : "a"(value), "Nd"(port) : "memory"); }
inline void outb(uint16_t port, uint8_t value) { asm volatile("outb %0, %1" : : "a"(value), "Nd"(port) : "memory"); }

auto config_address(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    return (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
           (offset & 0xfcU) | 0x80000000U;
}

// Pure admission lookup is safe under config_lock. Quarantine publishes its
// closed flag before disabling each requester under this same lock.
auto bus_master_write_allowed(uint8_t bus, uint8_t slot, uint8_t func, uint32_t value) -> bool {
    if ((value & PCI_COMMAND_BUS_MASTER) == 0) return true;
    for (int i = 0; i < device_count; ++i) {
        auto const& device = discovered_devices[static_cast<size_t>(i)];
        if (device.bus == bus && device.slot == slot && device.function == func && iommu::bus_master_allowed(&device)) return true;
    }
    return false;
}
void authorize_bus_master_write(uint8_t bus, uint8_t slot, uint8_t func, uint32_t value) {
    if (bus_master_write_allowed(bus, slot, func, value)) return;
    mod::dbg::panic_handler("Raw PCI command write attempted bus mastering without a ready DMA domain");
    hcf();
}

void scan_bus(uint8_t bus, uint16_t parent = NO_PARENT);

void scan_function(uint8_t bus, uint8_t slot, uint8_t func, uint16_t parent) {
    uint16_t const VENDOR = pci_config_read16(bus, slot, func, PCI_VENDOR_ID);
    if (VENDOR == 0xFFFF) {
        return;
    }
    if (device_count >= MAX_PCI_DEVICES) {
        enumeration_error = true;
        return;
    }

    auto* dev = &discovered_devices.at(static_cast<size_t>(device_count));
    dev->bus = bus;
    dev->slot = slot;
    dev->function = func;
    dev->parent_index = parent;
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

    // Bridges have two BARs; their remaining registers describe bus/windows.
    auto const HEADER = dev->header_type & 0x7fU;
    size_t const BARS = HEADER == PCI_HEADER_TYPE_NORMAL ? BAR_COUNT : HEADER == PCI_HEADER_TYPE_BRIDGE ? 2 : 0;
    dev->bar.fill(0);
    if (BARS != 0) {
        for (size_t i = 0; i < BARS; i++) {
            dev->bar.at(i) = pci_config_read32(bus, slot, func, static_cast<uint8_t>(PCI_BAR0 + (i * sizeof(uint32_t))));
        }
    }

    device_count++;

    // If this is a PCI-to-PCI bridge, scan the secondary bus
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_BRIDGE) {
        uint32_t const BUSES = pci_config_read32(bus, slot, func, 0x18);
        auto const PRIMARY = static_cast<uint8_t>(BUSES);
        dev->secondary_bus = static_cast<uint8_t>(BUSES >> 8);
        dev->subordinate_bus = static_cast<uint8_t>(BUSES >> 16);
        if (dev->secondary_bus == 0 && dev->subordinate_bus == 0) {
            return;  // Unconfigured bridge: no downstream bus to enumerate.
        }
        if (PRIMARY != bus || dev->secondary_bus == 0 || dev->secondary_bus == bus || dev->subordinate_bus < dev->secondary_bus ||
            (parent != NO_PARENT && (dev->secondary_bus < discovered_devices[parent].secondary_bus ||
                                     dev->subordinate_bus > discovered_devices[parent].subordinate_bus))) {
            enumeration_error = true;
            return;
        }
        scan_bus(dev->secondary_bus, static_cast<uint16_t>(device_count - 1));
    }
}

void scan_slot(uint8_t bus, uint8_t slot, uint16_t parent) {
    uint16_t const VENDOR = pci_config_read16(bus, slot, 0, PCI_VENDOR_ID);
    if (VENDOR == 0xFFFF) {
        return;
    }

    scan_function(bus, slot, 0, parent);

    // Check for multi-function device
    uint8_t const HEADER_TYPE = pci_config_read8(bus, slot, 0, PCI_HEADER_TYPE);
    if ((HEADER_TYPE & PCI_HEADER_TYPE_MULTI_FUNC) != 0) {
        for (uint8_t func = 1; func < PCI_FUNCTION_COUNT; func++) {
            if (pci_config_read16(bus, slot, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_function(bus, slot, func, parent);
            }
        }
    }
}

void scan_bus(uint8_t bus, uint16_t parent) {
    if (visited_buses[bus]) {
        enumeration_error = true;
        return;
    }
    visited_buses[bus] = true;
    for (uint8_t slot = 0; slot < PCI_SLOT_COUNT; slot++) {
        scan_slot(bus, slot, parent);
    }
}
}  // namespace

// Read 32-bit from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    ConfigGuard const GUARD;
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
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
    if ((offset & 0xfcU) == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if ((offset & 0xfcU) == PCI_COMMAND && !bus_master_write_allowed(bus, slot, func, value))
        value = static_cast<uint32_t>(value & ~PCI_COMMAND_BUS_MASTER);
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outl(PCI_CONFIG_DATA, value);
}

// Write 16-bit to PCI configuration space
auto pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value) -> void {
    if ((offset & 0xfeU) == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if ((offset & 0xfeU) == PCI_COMMAND && !bus_master_write_allowed(bus, slot, func, value))
        value = static_cast<uint16_t>(value & ~PCI_COMMAND_BUS_MASTER);
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outw(static_cast<uint16_t>(PCI_CONFIG_DATA + (offset & 2U)), value);
}

// Write 8-bit to PCI configuration space
auto pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value) -> void {
    if (offset == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if (offset == PCI_COMMAND && !bus_master_write_allowed(bus, slot, func, value))
        value = static_cast<uint8_t>(value & ~PCI_COMMAND_BUS_MASTER);
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outb(static_cast<uint16_t>(PCI_CONFIG_DATA + (offset & 3U)), value);
}

// Enumerate all PCI devices across all buses
auto pci_enumerate_all() -> int {
    if (enumerated) {
        return device_count;
    }

    device_count = 0;
    enumeration_error = false;
    visited_buses.fill(false);

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

    log::info("found %d devices (topology complete=%u)", device_count, static_cast<unsigned>(!enumeration_error));
    for (int i = 0; i < device_count; i++) {
        auto* d = &discovered_devices.at(static_cast<size_t>(i));
        log::debug("%d:%d.%d vendor=%x device=%x class=%x:%x prog_if=%x", static_cast<int>(d->bus), static_cast<int>(d->slot),
                   static_cast<int>(d->function), static_cast<int>(d->vendor_id), static_cast<int>(d->device_id),
                   static_cast<int>(d->class_code), static_cast<int>(d->subclass_code), static_cast<int>(d->prog_if));
    }

    collect_extended_config();
    return device_count;
}

auto pci_get_device(size_t idx) -> PCIDevice* {
    if (std::cmp_greater_equal(idx, device_count)) {
        return nullptr;
    }
    return &discovered_devices.at(idx);
}

auto pci_device_count() -> size_t { return static_cast<size_t>(device_count); }

auto pci_enumeration_complete() -> bool { return enumerated && !enumeration_error; }

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
    if (!pci_set_bus_master(dev, true)) {
        mod::dbg::panic_handler("PCI bus mastering requested without a ready DMA domain");
        hcf();
    }
}

auto pci_set_bus_master(PCIDevice* dev, bool enable) -> bool {
    if (dev == nullptr || (enable && !iommu::bus_master_allowed(dev))) return false;
    ConfigGuard const GUARD;
    if (enable && !iommu::bus_master_allowed(dev)) return false;
    outl(PCI_CONFIG_ADDRESS, config_address(dev->bus, dev->slot, dev->function, PCI_COMMAND));
    auto command = static_cast<uint16_t>(inl(PCI_CONFIG_DATA));
    command = enable ? static_cast<uint16_t>(command | PCI_COMMAND_BUS_MASTER) : static_cast<uint16_t>(command & ~PCI_COMMAND_BUS_MASTER);
    outw(PCI_CONFIG_DATA, command);
    return (static_cast<uint16_t>(inl(PCI_CONFIG_DATA)) & PCI_COMMAND_BUS_MASTER) == (enable ? PCI_COMMAND_BUS_MASTER : 0);
}

auto pci_enable_memory_space(PCIDevice* dev) -> void {
    ConfigGuard const GUARD;
    outl(PCI_CONFIG_ADDRESS, config_address(dev->bus, dev->slot, dev->function, PCI_COMMAND));
    auto cmd = static_cast<uint16_t>(inl(PCI_CONFIG_DATA));
    cmd |= PCI_COMMAND_MEM_SPACE;
    outw(PCI_CONFIG_DATA, cmd);
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
    if (dev == nullptr || bar_idx < 0) return 0;
    return resource::decode_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx)).address;
}

auto pci_get_bar_size_checked(PCIDevice* dev, int bar_idx) -> std::optional<uint64_t> {
    if (dev == nullptr || bar_idx < 0) return std::nullopt;
    struct Reader {
        PCIDevice* device;
        auto read32(uint8_t offset) -> uint32_t { return pci_config_read32(device->bus, device->slot, device->function, offset); }
        auto read16(uint8_t offset) -> uint16_t { return pci_config_read16(device->bus, device->slot, device->function, offset); }
        void write32(uint8_t offset, uint32_t value) { pci_config_write32(device->bus, device->slot, device->function, offset, value); }
        void write16(uint8_t offset, uint16_t value) { pci_config_write16(device->bus, device->slot, device->function, offset, value); }
    } reader{dev};
    return resource::probe_memory_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx), reader);
}

auto pci_get_bar_size(PCIDevice* dev, int bar_idx) -> uint64_t { return pci_get_bar_size_checked(dev, bar_idx).value_or(0); }

auto pci_map_bar(PCIDevice* dev, int bar_idx) -> void* {
    if (dev == nullptr || bar_idx < 0) return nullptr;
    auto const BAR = resource::decode_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx));
    if (!BAR.memory() || BAR.address == 0) return nullptr;
    auto const SIZE = pci_get_bar_size_checked(dev, bar_idx);
    if (!SIZE || *SIZE == 0) return nullptr;
    mod::cpu::CpuidContext cpu{.function = static_cast<int>(0x80000000U)};
    mod::cpu::cpuid(&cpu);
    if (static_cast<uint32_t>(cpu.eax) < 0x80000008U) return nullptr;
    cpu.function = static_cast<int>(0x80000008U);
    mod::cpu::cpuid(&cpu);
    auto const RANGE = resource::mmio_range(BAR, *SIZE, mod::mm::addr::get_hhdm_offset(), cpu.eax & 255U);
    if (!RANGE) return nullptr;
    auto const* memory = mod::mm::boot_memory_map();
    if (memory == nullptr || memory->entries == nullptr ||
        !resource::mmio_memory_safe(*RANGE, memory->entry_count, [memory](size_t i) -> std::optional<resource::MemoryRegion> {
            auto const* entry = memory->entries[i];
            if (entry == nullptr) return std::nullopt;
            return resource::MemoryRegion{entry->base, entry->length, entry->type == LIMINE_MEMMAP_RESERVED};
        }))
        return nullptr;

    // Map the MMIO range into the kernel page table (uncacheable for MMIO correctness)
    ker::mod::mm::virt::map_range_to_kernel_page_table(ker::mod::mm::virt::Range{.start = RANGE->start, .end = RANGE->end},
                                                       ker::mod::mm::virt::kernel_mmio_page_flags());

    return reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(BAR.address));
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
