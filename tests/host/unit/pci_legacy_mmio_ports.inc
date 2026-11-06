// Explicitly non-typed compatibility ports for the existing conventional PCI
// and DMA host tests. These fixtures cannot create an MMIO owner or qualify the
// new typed path; the separate actual-engine fixture links real MM instead.
#include <cassert>
#include <dev/dma/dma.hpp>
#include <dev/pci_extended.hpp>
#include <dev/pci_platform_apertures.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
#include <platform/mm/mmio_mapping_private.hpp>

#ifndef PCI_LEGACY_ACTUAL_EXTENDED
namespace ker::dev::pci {
auto extended_inventory() -> ExtendedInventory { return {}; }
auto extended_config(const PCIDevice*) -> const ExtendedConfig* { return nullptr; }
}  // namespace ker::dev::pci
#endif
#ifdef PCI_LEGACY_NO_DMA_ENGINE
namespace ker::dev::dma {
auto stats(const pci::PCIDevice*) -> Stats { __builtin_trap(); }
}  // namespace ker::dev::dma
#endif
namespace ker::dev::pci::aperture {
void collect_platform(Platform& output, const Image&, const Image&) { output.valid = false; }
}  // namespace ker::dev::pci::aperture
namespace ker::mod::mm::mmio {
auto enabled() noexcept -> bool { return false; }
Mapping::~Mapping() { assert(empty()); }
auto Mapping::operator=(Mapping&& other) noexcept -> Mapping& {
    assert(empty() && other.empty());
    return *this;
}
namespace detail {
void finish_boot_arena() noexcept {}
PreparedMapping::~PreparedMapping() { assert(empty()); }
auto prepare_from_pci(ker::dev::pci::detail::ColdFunctionClaim&&, const cpu_maintenance::detail::Controller&, PreparedMapping&) -> Status {
    __builtin_trap();
}
auto resume_prepared(PreparedMapping&, const cpu_maintenance::detail::Controller&) -> Status { __builtin_trap(); }
auto publish_from_pci(PreparedMapping&, const cpu_maintenance::detail::Controller&, Mapping&) -> Status { __builtin_trap(); }
}  // namespace detail
}  // namespace ker::mod::mm::mmio
namespace ker::mod::mm::cpu_maintenance::detail {
auto Controller::context_status() const noexcept -> Status { return Status::invalid_context; }
}  // namespace ker::mod::mm::cpu_maintenance::detail
