#include "pci_platform_apertures.hpp"

#include <dev/iommu/discovery.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <platform/asm/msr.hpp>

namespace ker::dev::pci::aperture {
void collect_platform(Platform& output, const Image& host, const Image& lpc) {
    output.valid = false;
    output.count = 0;
    mod::cpu::CpuidContext cpu{};
    cpu.function = static_cast<int>(0x80000000U);
    mod::cpu::cpuid(&cpu);
    if (static_cast<uint32_t>(cpu.eax) < 0x80000008U) return;
    cpu.function = static_cast<int>(0x80000008U);
    mod::cpu::cpuid(&cpu);
    output.physical_bits = static_cast<uint8_t>(cpu.eax & 255U);
    if (output.physical_bits < 32 || output.physical_bits > 52) return;
    uint64_t apic = 0;
    if (!mod::cpu::try_read_msr(IA32_APIC_BASE, apic)) return;
    uint64_t const APIC_BASE = apic & 0x000ffffffffff000ULL;
    if (APIC_BASE == 0 || (apic & (uint64_t{1} << 11)) == 0) return;
    auto const MCFG = mod::acpi::find_table_bounded("MCFG");
    auto const MADT = mod::acpi::find_table_bounded("APIC");
    auto const HPET = mod::acpi::find_table_bounded("HPET");
    auto const FADT = mod::acpi::find_table_bounded("FACP");
    if (!MCFG.valid() || !MADT.valid() || !HPET.valid() || !FADT.valid()) return;
    output.ecam = ecam::parse(MCFG.bytes);
    if (!output.ecam.valid() || output.ecam.count != 1 || output.ecam.windows[0].segment != 0 || output.ecam.windows[0].first_bus != 0)
        return;
    for (size_t i = 0; i < output.ecam.count; ++i) {
        auto const& window = output.ecam.windows[i];
        if (!output.add(window.start(), window.end() - window.start())) return;
    }
    if (!platform_detail::madt(output, MADT.bytes, APIC_BASE) || !platform_detail::hpet(output, HPET.bytes) ||
        !platform_detail::fadt(output, FADT.bytes) || !platform_detail::q35(output, host, lpc))
        return;
    auto const* inventory = iommu::discovery();
    if (inventory == nullptr || inventory->state == iommu::DiscoveryState::not_run ||
        inventory->state == iommu::DiscoveryState::conflicting_firmware || inventory->state == iommu::DiscoveryState::firmware_error ||
        inventory->state == iommu::DiscoveryState::dmar_error || inventory->state == iommu::DiscoveryState::ivrs_error ||
        inventory->state == iommu::DiscoveryState::inventory_limit)
        return;
    if (inventory->vendor == iommu::FirmwareVendor::intel) {
        if (inventory->state != iommu::DiscoveryState::discovered || inventory->unit_count == 0 ||
            inventory->unit_count > iommu::MAX_UNITS || inventory->table.unsupported ||
            inventory->table.bytes.size() < iommu::dmar::HEADER_SIZE)
            return;
        for (size_t i = 0; i < inventory->unit_count; ++i) {
            auto const& unit = inventory->units[i];
            if (unit.segment != 0 || unit.register_bytes == 0 || !output.add(unit.physical, unit.register_bytes)) return;
        }
        auto const bytes = inventory->table.bytes;
        for (size_t offset = iommu::dmar::HEADER_SIZE; offset < bytes.size();) {
            if (bytes.size() - offset < 4) return;
            size_t const length = iommu::dmar::detail::read(bytes, offset + 2, 2);
            if (length < 4 || length > bytes.size() - offset) return;
            uint16_t const type = static_cast<uint16_t>(iommu::dmar::detail::read(bytes, offset, 2));
            if (type == 1) {
                if (length < 24 || iommu::dmar::detail::read(bytes, offset + 6, 2) != 0) return;
                uint64_t const base = iommu::dmar::detail::read(bytes, offset + 8, 8);
                uint64_t const last = iommu::dmar::detail::read(bytes, offset + 16, 8);
                if (last < base || last == UINT64_MAX || !output.add(base, last - base + 1)) return;
            } else if (type != 0 && type != 2 && type != 3 && type != 4) {
                return;
            }
            offset += length;
        }
    } else if (inventory->vendor == iommu::FirmwareVendor::amd) {
        if (inventory->state != iommu::DiscoveryState::ivrs_discovered_unsupported ||
            inventory->amd_table.bytes.size() < iommu::ivrs::HEADER_SIZE || inventory->amd_table.hardware_blocks == 0)
            return;
        // AMD48882rev3.11's observed register-space profile: a conservative
        // 512-KiB bank covers both documented 16-KiB and PCSup formats. It does
        // not activate, read or claim unique units from IVHD alternatives.
        auto const bytes = inventory->amd_table.bytes;
        for (size_t offset = iommu::ivrs::HEADER_SIZE; offset < bytes.size();) {
            if (bytes.size() - offset < 4) return;
            size_t const length = iommu::ivrs::detail::read(bytes, offset + 2, 2);
            if (length < 4 || length > bytes.size() - offset) return;
            uint8_t const type = bytes[offset];
            if (type == 0x10 || type == 0x11 || type == 0x40) {
                if (length < 24 || iommu::ivrs::detail::read(bytes, offset + 16, 2) != 0 ||
                    !output.add(iommu::ivrs::detail::read(bytes, offset + 8, 8), 512 * 1024))
                    return;
            } else if (type != 0x20 && type != 0x21 && type != 0x22) {
                return;
            } else {
                if (length != 32 ||
                    !output.add(iommu::ivrs::detail::read(bytes, offset + 16, 8), iommu::ivrs::detail::read(bytes, offset + 24, 8)))
                    return;
            }
            offset += length;
        }
    } else if (inventory->vendor != iommu::FirmwareVendor::none || inventory->state != iommu::DiscoveryState::absent) {
        return;
    }
    output.valid = true;
}
}  // namespace ker::dev::pci::aperture
