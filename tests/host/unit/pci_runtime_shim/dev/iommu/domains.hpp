#pragma once
namespace ker::dev::pci {
struct PCIDevice;
}
namespace ker::dev::iommu {
auto bus_master_allowed(const pci::PCIDevice*) -> bool;
}
