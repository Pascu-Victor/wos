#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/dmar.hpp>
#include <dev/iommu/ivrs.hpp>
#include <dev/iommu/scopes.hpp>
#include <dev/iommu/vtd_capability.hpp>
#include <platform/acpi/bounded_tables.hpp>

namespace ker::dev::iommu {

constexpr size_t MAX_UNITS = 16;
enum class DiscoveryState : uint8_t {
    not_run,
    absent,
    firmware_error,
    dmar_error,
    inventory_limit,
    discovered,
    ivrs_error,
    ivrs_discovered_unsupported,
    conflicting_firmware,
};
enum class FirmwareVendor : uint8_t { none, intel, amd, ambiguous };
enum class UnitState : uint8_t { unprobed, unsafe_register_range, capability_error, firmware_active, available };

struct Unit {
    uint64_t physical = 0;
    uint64_t register_bytes = 0;
    uint16_t segment = 0;
    bool include_all = false;
    size_t table_offset = 0;
    uint32_t version = 0;
    uint32_t global_status = 0;
    uint64_t cap = 0;
    uint64_t ecap = 0;
    vtd::Capability capability{};
    UnitState state = UnitState::unprobed;
};

struct Discovery {
    DiscoveryState state = DiscoveryState::not_run;
    FirmwareVendor vendor = FirmwareVendor::none;
    mod::acpi::bounded::Error firmware_error = mod::acpi::bounded::Error::none;
    dmar::Error table_error = dmar::Error::none;
    ivrs::Error ivrs_error = ivrs::Error::none;
    // AMD observations never populate the Intel table, units or ownership.
    ivrs::Table amd_table{};
    dmar::Table table{};
    std::array<Unit, MAX_UNITS> units{};
    size_t unit_count = 0;
    scopes::Result ownership{.error = scopes::Error::incomplete};
};

// BSP boot-only initialization before AHCI. Retains a private copy of the
// selected DMAR or IVRS. Intel capability reads never change command state;
// AMD firmware is observed without mapping or probing its register banks.
void discover();
// Immutable after discover() publishes. Null before boot discovery completes.
[[nodiscard]] auto discovery() -> const Discovery*;

}  // namespace ker::dev::iommu
