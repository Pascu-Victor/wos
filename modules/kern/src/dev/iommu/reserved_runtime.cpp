#include "reserved_runtime.hpp"

#include <dev/pci.hpp>
#include <dev/pci_ecam.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/asm/msr.hpp>
#include <platform/mm/mm.hpp>

namespace ker::dev::iommu::reserved {
namespace {
// Boot-only scratch storage, not kernel-stack arrays.
std::array<Memory, MAX_MEMORY_REGIONS> memory{};
std::array<Exclusion, MAX_EXCLUSIONS> exclusions{};
size_t used = 0;
auto exclude(uint64_t base, uint64_t bytes) -> bool {
    if (bytes == 0) return true;
    if (bytes > UINT64_MAX - base || used == exclusions.size()) return false;
    exclusions[used++] = {base, bytes};
    return true;
}
auto pci_exclusions(pci::PCIDevice* device) -> bool {
    auto read = [&](uint8_t offset) { return pci::pci_config_read32(device->bus, device->slot, device->function, offset); };
    auto write = [&](uint8_t offset, uint32_t value) {
        pci::pci_config_write32(device->bus, device->slot, device->function, offset, value);
    };
    uint16_t const COMMAND = pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND);
    if ((COMMAND & pci::PCI_COMMAND_BUS_MASTER) != 0) return false;
    bool const BRIDGE = (device->header_type & 0x7fU) == pci::PCI_HEADER_TYPE_BRIDGE;
    size_t const BARS = BRIDGE ? 2 : 6;
    for (size_t i = 0; i < BARS; ++i) {
        uint8_t const OFFSET = static_cast<uint8_t>(pci::PCI_BAR0 + i * 4);
        uint32_t const LOW = read(OFFSET);
        if ((LOW & 1U) != 0) continue;
        uint32_t const TYPE = (LOW >> 1U) & 3U;
        if (TYPE == 3 || (TYPE == 2 && i + 1 == BARS)) return false;
        uint32_t const HIGH = TYPE == 2 ? read(OFFSET + 4) : 0;
        uint64_t const ADDRESS = (uint64_t{HIGH} << 32U) | (LOW & ~uint32_t{15});
        uint64_t const SIZE = pci::pci_get_bar_size(device, static_cast<int>(i));
        if (read(OFFSET) != LOW || (TYPE == 2 && read(OFFSET + 4) != HIGH) ||
            pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) != COMMAND)
            return false;
        if (SIZE != 0 && ((SIZE & (SIZE - 1)) != 0 || (ADDRESS & (SIZE - 1)) != 0 || !exclude(ADDRESS, SIZE))) return false;
        if (TYPE == 2) ++i;
    }
    uint8_t const ROM_OFFSET = BRIDGE ? 0x38 : 0x30;
    uint32_t const ROM = read(ROM_OFFSET);
    pci::pci_config_write16(device->bus, device->slot, device->function, pci::PCI_COMMAND, COMMAND & ~pci::PCI_COMMAND_MEM_SPACE);
    write(ROM_OFFSET, 0xfffff800U);  // Decode stays disabled during sizing.
    uint32_t const MASK = read(ROM_OFFSET) & 0xfffff800U;
    write(ROM_OFFSET, ROM);
    pci::pci_config_write16(device->bus, device->slot, device->function, pci::PCI_COMMAND, COMMAND);
    if (read(ROM_OFFSET) != ROM || pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) != COMMAND)
        return false;
    uint64_t const ROM_SIZE = MASK == 0 ? 0 : static_cast<uint32_t>(~MASK + 1U);
    if (ROM_SIZE != 0 &&
        ((ROM_SIZE & (ROM_SIZE - 1)) != 0 || ((ROM & 0xfffff800U) & (ROM_SIZE - 1)) != 0 || !exclude(ROM & 0xfffff800U, ROM_SIZE)))
        return false;
    if (!BRIDGE) return true;
    uint32_t const WINDOW = read(0x20);
    uint64_t base = uint64_t{WINDOW & 0xfff0U} << 16U;
    uint64_t end = uint64_t{(WINDOW >> 16U) & 0xfff0U} << 16U | 0xfffffU;
    if (base <= end && !exclude(base, end - base + 1)) return false;
    uint32_t const PREFETCH = read(0x24);
    uint32_t const TYPE = PREFETCH & 15U;
    if (TYPE > 1 || ((PREFETCH >> 16U) & 15U) != TYPE) return false;
    base = uint64_t{PREFETCH & 0xfff0U} << 16U;
    end = uint64_t{(PREFETCH >> 16U) & 0xfff0U} << 16U | 0xfffffU;
    if (TYPE == 1) {
        base |= uint64_t{read(0x28)} << 32U;
        end |= uint64_t{read(0x2c)} << 32U;
    }
    return base > end || (end != UINT64_MAX && exclude(base, end - base + 1));
}
auto platform_exclusions(const Discovery& inventory) -> bool {
    // Legacy VGA/ROM and the x86 chipset/MSI/APIC window are never system RAM.
    if (!exclude(0xa0000, 0x60000) || !exclude(0xfec00000, 0x1400000)) return false;
    uint64_t apic = 0;
    cpu_get_msr(IA32_APIC_BASE, &apic);
    if (!exclude(apic & 0x000ffffffffff000ULL, 4096)) return false;
    auto const& apics = mod::acpi::madt::get_apic_info();
    if (apics.usable_ioapics > mod::acpi::madt::MADT_PARSED_ENTRY_CAPACITY || !exclude(apics.lapic_addr, 4096)) return false;
    for (size_t i = 0; i < apics.usable_ioapics; ++i)
        if (!exclude(apics.ioapics[i].io_apic_addr, 4096)) return false;
    auto const HPET = mod::acpi::find_table_bounded("HPET");
    if (!HPET.valid() || HPET.bytes.size() < 56 || HPET.bytes[40] != 0 ||
        !exclude(mod::acpi::bounded::detail::read(HPET.bytes, 44, 8), 4096))
        return false;
    auto const MCFG = mod::acpi::find_table_bounded("MCFG");
    if (!MCFG.valid()) return false;
    auto const WINDOWS = pci::ecam::parse(MCFG.bytes);
    if (!WINDOWS.valid()) return false;
    for (size_t i = 0; i < WINDOWS.count; ++i) {
        auto const& window = WINDOWS.windows[i];
        if (!exclude(window.start(), window.end() - window.start())) return false;
    }
    for (size_t i = 0; i < inventory.unit_count; ++i)
        if (!exclude(inventory.units[i].physical, inventory.units[i].register_bytes)) return false;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* device = pci::pci_get_device(i);
        if (device == nullptr || !pci_exclusions(device)) return false;
    }
    return true;
}
}  // namespace

auto prepare(const Discovery& inventory, const groups::Result& grouping, std::span<Mapping> output) -> Plan {
    if (inventory.ownership.region_count == 0) return {.error = Error::none};
    auto const* map = mod::mm::boot_memory_map();
    if (map == nullptr || map->entries == nullptr || map->entry_count == 0 || map->entry_count > memory.size())
        return {.error = Error::memory_map};
    for (size_t i = 0; i < map->entry_count; ++i) {
        auto const* entry = map->entries[i];
        if (entry == nullptr) return {.error = Error::memory_map};
        // NVS can contain live FACS/tables. It stays protected until exact
        // subrange ownership can be established, despite generic model support.
        memory[i] = {entry->base, entry->length, entry->type == LIMINE_MEMMAP_RESERVED ? Kind::firmware_reserved : Kind::protected_memory};
    }
    used = 0;
    if (!platform_exclusions(inventory)) return {.error = Error::mmio};
    std::array<uint8_t, MAX_UNITS> widths{};
    if (inventory.unit_count > widths.size()) return {.error = Error::limit};
    for (size_t i = 0; i < inventory.unit_count; ++i) {
        auto const& cap = inventory.units[i].capability;
        widths[i] = cap.physical_bits < cap.iova_bits ? cap.physical_bits : cap.iova_bits;
    }
    return plan(inventory.ownership, grouping, std::span<const uint8_t>{widths}.first(inventory.unit_count),
                std::span<const Memory>{memory}.first(map->entry_count), std::span<const Exclusion>{exclusions}.first(used), output);
}
}  // namespace ker::dev::iommu::reserved
