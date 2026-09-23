#include <dev/iommu/discovery.hpp>
#include <dev/iommu/scopes.hpp>
#include <dev/iommu/vtd_capability.hpp>
#include <dev/pci.hpp>
#include <test/ktest.hpp>

KTEST(IommuDiscovery, BootInventoryIsPublishedAndRetained) {
    namespace iommu = ker::dev::iommu;
    auto const* inventory = iommu::discovery();
    KREQUIRE_TRUE(inventory != nullptr);
    KEXPECT_NE(inventory->state, iommu::DiscoveryState::not_run);
    KEXPECT_TRUE(inventory->unit_count <= iommu::MAX_UNITS);
    if (inventory->state == iommu::DiscoveryState::discovered) {
        auto const PARSED = iommu::dmar::parse(inventory->table.bytes);
        KREQUIRE_TRUE(PARSED.valid());
        KEXPECT_EQ(PARSED.table.units, inventory->unit_count);
        for (size_t i = 0; i < inventory->unit_count; ++i) {
            auto const& unit = inventory->units[i];
            KEXPECT_NE(unit.state, iommu::UnitState::unprobed);
            if (unit.state == iommu::UnitState::available) {
                KEXPECT_TRUE(unit.capability.valid());
                KEXPECT_EQ(unit.global_status & ((1U << 31U) | (1U << 26U) | (1U << 25U)), 0U);
            }
        }
    } else {
        KEXPECT_EQ(inventory->unit_count, size_t{0});
        KEXPECT_TRUE(inventory->table.bytes.empty());
    }
}

KTEST(IommuDiscovery, CapabilityRejectsUnusableRegisterBanks) {
    namespace vtd = ker::dev::iommu::vtd;
    constexpr uint64_t CAP = (uint64_t{3} << 54) | (uint64_t{0x22} << 24) | (uint64_t{47} << 16) | (6U << 8) | 6U;
    auto const VALID = vtd::decode_capability(0x10, CAP, 0xf00, 46, 4096);
    KREQUIRE_TRUE(VALID.valid());
    KEXPECT_EQ(VALID.levels, uint8_t{4});
    KEXPECT_EQ(VALID.domains, uint32_t{65536});
    KEXPECT_EQ(vtd::decode_capability(0x10, CAP, 0x2200, 46, 4096).error, vtd::CapabilityError::register_range);
    KEXPECT_EQ(vtd::decode_capability(0x10, CAP, 0xf00, 46, 128).error, vtd::CapabilityError::register_range);
    KEXPECT_EQ(vtd::decode_capability(0x10, CAP & ~(uint64_t{1} << 54), 0xf00, 46, 4096).error, vtd::CapabilityError::draining);
}

KTEST(IommuDiscovery, ScopeOwnershipRequiresCompleteTopologyAndBoundedUnits) {
    namespace iommu = ker::dev::iommu;
    namespace pci = ker::dev::pci;
    auto const* inventory = iommu::discovery();
    KREQUIRE_TRUE(inventory != nullptr);
    auto const& owners = inventory->ownership;
    if (!owners.valid()) {
        KEXPECT_EQ(owners.device_count, size_t{0});
        KEXPECT_EQ(owners.region_count, size_t{0});
        return;
    }
    KEXPECT_TRUE(pci::pci_enumeration_complete());
    KEXPECT_EQ(owners.device_count, pci::pci_device_count());
    KEXPECT_TRUE(owners.region_count <= iommu::scopes::MAX_REGIONS);
    for (size_t i = 0; i < owners.device_count; ++i) {
        KEXPECT_TRUE(owners.devices[i].unit < inventory->unit_count);
        auto const* device = pci::pci_get_device(i);
        KREQUIRE_TRUE(device != nullptr);
        KEXPECT_TRUE(device->parent_index == pci::NO_PARENT || device->parent_index < i);
        if (owners.region_count < 64) KEXPECT_EQ(owners.devices[i].reserved >> owners.region_count, uint64_t{0});
    }
}
