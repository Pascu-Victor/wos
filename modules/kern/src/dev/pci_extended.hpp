#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/pci_ecam.hpp>
#include <span>

namespace ker::dev::pci {
struct PCIDevice;
enum class ExtendedState : uint8_t {
    unavailable,
    firmware_error,
    table_error,
    unsafe_window,
    missing_window,
    identity_error,
    capability_error,
    ready
};
struct ExtendedConfig {
    ExtendedState state = ExtendedState::unavailable;
    uint64_t physical = 0;
    ecam::Capabilities capabilities{};
};
struct ExtendedInventory {
    ExtendedState state = ExtendedState::unavailable;
    ecam::Error table_error = ecam::Error::none;
    size_t windows = 0, devices = 0, readable = 0;
};
// Serialized BSP boot only, after ACPI and complete PCI enumeration, before
// driver probes. Maps only validated function pages; changes no device control.
void collect_extended_config();
// Immutable retained boot snapshots; never perform MMIO from a caller's context.
[[nodiscard]] auto extended_inventory() -> ExtendedInventory;
[[nodiscard]] auto extended_config(const PCIDevice* device) -> const ExtendedConfig*;
// One BSP boot attempt, before domain publication, with every BME disabled.
// Returns freshly read capabilities after supported ACS controls are enabled.
// Does not mutate the immutable discovery snapshots. Failure forbids admission.
[[nodiscard]] auto enable_strict_acs(std::span<ecam::Capabilities> capabilities) -> bool;
}  // namespace ker::dev::pci
