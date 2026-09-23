#pragma once
#include <dev/iommu/discovery.hpp>
#include <dev/iommu/reserved.hpp>

namespace ker::dev::iommu::reserved {
// BSP boot only, all PCI BME already cleared and no drivers probing. Uses
// reversible BAR sizing to construct exclusions, never changes reservations.
[[nodiscard]] auto prepare(const Discovery& inventory, const groups::Result& grouping, std::span<Mapping> output) -> Plan;
}  // namespace ker::dev::iommu::reserved
