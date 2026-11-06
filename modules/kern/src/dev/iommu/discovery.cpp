#include "discovery.hpp"

#include <atomic>
#include <cstring>
#include <dev/pci.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>

namespace ker::dev::iommu {
namespace {
using log = mod::dbg::logger<"iommu">;
std::array<uint8_t, dmar::MAX_TABLE_SIZE> firmware_copy{};
static_assert(ivrs::MAX_TABLE_SIZE <= firmware_copy.size());
Discovery inventory{};
std::atomic<bool> published{false};

auto registers_safe(uint64_t physical, uint64_t bytes) -> bool {
    auto const* memory = mod::mm::boot_memory_map();
    uint64_t const HHDM = mod::mm::addr::get_hhdm_offset();
    if (memory == nullptr || memory->entries == nullptr || memory->entry_count > 4096 || bytes < 4096 || physical > UINT64_MAX - HHDM ||
        bytes > UINT64_MAX - HHDM - physical) {
        return false;
    }
    for (size_t i = 0; i < memory->entry_count; ++i) {
        auto const* entry = memory->entries[i];
        if (entry == nullptr || entry->length > UINT64_MAX - entry->base) {
            return false;
        }
        // Unlisted or firmware-reserved MMIO is allowed. Never change cache
        // attributes of RAM, ACPI tables, boot images, framebuffer or bad RAM.
        if (entry->type != LIMINE_MEMMAP_RESERVED && entry->length != 0 && physical < entry->base + entry->length &&
            entry->base < physical + bytes) {
            return false;
        }
    }
    return true;
}

void probe(Unit& unit) {
    if (!registers_safe(unit.physical, unit.register_bytes)) {
        unit.state = UnitState::unsafe_register_range;
        return;
    }
    // Capability discovery needs only the first page. Later command/fault
    // access must map the validated variable register banks explicitly.
    mod::mm::virt::map_range_to_kernel_page_table({unit.physical, unit.physical + 4096}, mod::mm::virt::kernel_mmio_page_flags());
    auto const* registers = reinterpret_cast<const volatile uint8_t*>(mod::mm::addr::get_virt_pointer(unit.physical));
    unit.version = *reinterpret_cast<const volatile uint32_t*>(registers);
    unit.cap = *reinterpret_cast<const volatile uint64_t*>(registers + 8);
    unit.ecap = *reinterpret_cast<const volatile uint64_t*>(registers + 16);
    unit.global_status = *reinterpret_cast<const volatile uint32_t*>(registers + 28);
    unit.capability = vtd::decode_capability(unit.version, unit.cap, unit.ecap, inventory.table.address_bits, unit.register_bytes);
    bool const FIRMWARE_ACTIVE = (unit.global_status & ((1U << 31U) | (1U << 26U) | (1U << 25U))) != 0;
    unit.state = FIRMWARE_ACTIVE           ? UnitState::firmware_active
                 : unit.capability.valid() ? UnitState::available
                                           : UnitState::capability_error;
}

void build_intel(std::span<const uint8_t> bytes) {
    auto const PARSED = dmar::parse(bytes);
    if (!PARSED.valid()) {
        inventory.table_error = PARSED.error;
        inventory.state = DiscoveryState::dmar_error;
        return;
    }
    if (PARSED.table.units > MAX_UNITS) {
        inventory.state = DiscoveryState::inventory_limit;
        return;
    }
    inventory.table = PARSED.table;
    auto const BYTES = inventory.table.bytes;
    for (size_t offset = dmar::HEADER_SIZE; offset < BYTES.size(); offset += dmar::detail::read(BYTES, offset + 2, 2)) {
        if (dmar::detail::read(BYTES, offset, 2) != 0) {
            break;
        }
        auto& unit = inventory.units[inventory.unit_count++];
        unit.physical = dmar::detail::read(BYTES, offset + 8, 8);
        unit.register_bytes = uint64_t{4096} << BYTES[offset + 5];
        unit.segment = static_cast<uint16_t>(dmar::detail::read(BYTES, offset + 6, 2));
        unit.include_all = (BYTES[offset + 4] & 1U) != 0;
        unit.table_offset = offset;
        probe(unit);
    }
    inventory.state = DiscoveryState::discovered;
    std::array<scopes::Device, scopes::MAX_DEVICES> devices{};
    size_t const COUNT = pci::pci_device_count();
    if (COUNT > devices.size()) {
        inventory.ownership.error = scopes::Error::limit;
        return;
    }
    for (size_t i = 0; i < COUNT; ++i) {
        auto const* dev = pci::pci_get_device(i);
        if (dev == nullptr) return;
        devices[i] = {.bus = dev->bus,
                      .slot = dev->slot,
                      .function = dev->function,
                      .header = static_cast<uint8_t>(dev->header_type & 0x7fU),
                      .parent = dev->parent_index,
                      .secondary = dev->secondary_bus,
                      .subordinate = dev->subordinate_bus};
    }
    inventory.ownership =
        scopes::qualify(inventory.table.bytes, std::span<const scopes::Device>{devices}.first(COUNT), pci::pci_enumeration_complete());
}

void build_inventory() {
    using FirmwareError = mod::acpi::bounded::Error;
    // Source selection precedes every vendor-specific MMIO access. Even a
    // malformed second table or an untrustworthy root prevents a unique choice.
    auto const DMAR = mod::acpi::find_table_bounded("DMAR");
    auto const IVRS = mod::acpi::find_table_bounded("IVRS");
    bool const HAS_DMAR = DMAR.error != FirmwareError::not_found;
    bool const HAS_IVRS = IVRS.error != FirmwareError::not_found;
    if (HAS_DMAR && HAS_IVRS) {
        inventory.vendor = FirmwareVendor::ambiguous;
        inventory.state = DiscoveryState::conflicting_firmware;
        if (!DMAR.valid())
            inventory.firmware_error = DMAR.error;
        else if (!IVRS.valid())
            inventory.firmware_error = IVRS.error;
        return;
    }
    if (!HAS_DMAR && !HAS_IVRS) {
        inventory.state = DiscoveryState::absent;
        return;
    }
    inventory.vendor = HAS_DMAR ? FirmwareVendor::intel : FirmwareVendor::amd;
    auto const& found = HAS_DMAR ? DMAR : IVRS;
    if (!found.valid()) {
        inventory.firmware_error = found.error;
        inventory.state = DiscoveryState::firmware_error;
        return;
    }
    if (found.bytes.size() > firmware_copy.size()) {
        if (HAS_DMAR) {
            inventory.table_error = dmar::Error::length;
            inventory.state = DiscoveryState::dmar_error;
        } else {
            inventory.ivrs_error = ivrs::Error::length;
            inventory.state = DiscoveryState::ivrs_error;
        }
        return;
    }
    // Validate the retained copy itself. Firmware mutation after the bounded
    // lookup must not turn a validated source into an unvalidated publication.
    if (!found.bytes.empty()) std::memcpy(firmware_copy.data(), found.bytes.data(), found.bytes.size());
    auto const BYTES = std::span<const uint8_t>{firmware_copy}.first(found.bytes.size());
    if (HAS_DMAR) {
        build_intel(BYTES);
        return;
    }
    auto const PARSED = ivrs::parse(BYTES);
    if (!PARSED.valid()) {
        inventory.ivrs_error = PARSED.error;
        inventory.state = DiscoveryState::ivrs_error;
        return;
    }
    inventory.amd_table = PARSED.table;
    inventory.state = DiscoveryState::ivrs_discovered_unsupported;
}
}  // namespace

void discover() {
    if (published.load(std::memory_order_acquire)) {
        return;
    }
    build_inventory();
    log::info(
        "discovery=%u firmware_error=%u table_error=%u units=%zu vendor=%u ivrs_error=%u; DMA isolation unavailable (unmanaged boot DMA)",
        static_cast<unsigned>(inventory.state), static_cast<unsigned>(inventory.firmware_error),
        static_cast<unsigned>(inventory.table_error), inventory.unit_count, static_cast<unsigned>(inventory.vendor),
        static_cast<unsigned>(inventory.ivrs_error));
    if (inventory.vendor == FirmwareVendor::amd) {
        log::info("IVRS hardware_blocks=%zu memory_blocks=%zu entries=%zu unsupported=%u; AMD-Vi backend unavailable, registers unprobed",
                  inventory.amd_table.hardware_blocks, inventory.amd_table.memory_blocks, inventory.amd_table.device_entries,
                  static_cast<unsigned>(inventory.amd_table.unsupported));
    }
    for (size_t i = 0; i < inventory.unit_count; ++i) {
        auto const& unit = inventory.units[i];
        log::info(
            "unit=%zu segment=%u base=0x%llx version=0x%x cap=0x%llx ecap=0x%llx status=0x%x state=%u capability_error=%u "
            "levels=%u iova_bits=%u domains=%u coherent=%u",
            i, static_cast<unsigned>(unit.segment), static_cast<unsigned long long>(unit.physical), unit.version,
            static_cast<unsigned long long>(unit.cap), static_cast<unsigned long long>(unit.ecap), unit.global_status,
            static_cast<unsigned>(unit.state), static_cast<unsigned>(unit.capability.error), static_cast<unsigned>(unit.capability.levels),
            static_cast<unsigned>(unit.capability.iova_bits), unit.capability.domains, static_cast<unsigned>(unit.capability.coherent));
    }
    log::info("scopes error=%u offset=%zu devices=%zu reserved_regions=%zu absent_scopes=%zu",
              static_cast<unsigned>(inventory.ownership.error), inventory.ownership.offset, inventory.ownership.device_count,
              inventory.ownership.region_count, inventory.ownership.absent_scopes);
    published.store(true, std::memory_order_release);
}

auto discovery() -> const Discovery* { return published.load(std::memory_order_acquire) ? &inventory : nullptr; }

}  // namespace ker::dev::iommu
