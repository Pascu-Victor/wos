#pragma once

#include <platform/acpi/bounded_tables.hpp>
#include <platform/acpi/numa_topology.hpp>
#include <span>

namespace ker::mod::acpi::numa {

struct BootStatus {
    bounded::Error srat{bounded::Error::not_found};
    bounded::Error slit{bounded::Error::not_found};
    Result parser{};
    bool rejected_firmware{};
};

// BSP boot only, after MM/ACPI discovery and before starting other CPUs.
// Inputs are copied; no firmware or caller-owned span survives publication.
void initialize_boot_topology(std::span<const CpuInput> cpus);

// Immutable kernel-lifetime snapshots. Null until release publication.
[[nodiscard]] auto boot_topology() noexcept -> const Topology*;
[[nodiscard]] auto boot_status() noexcept -> const BootStatus*;

}  // namespace ker::mod::acpi::numa
