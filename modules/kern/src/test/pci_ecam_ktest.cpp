#include <dev/pci.hpp>
#include <dev/pci_extended.hpp>
#include <platform/dbg/dbg.hpp>
#include <test/ktest.hpp>

KTEST(PciEcam, RetainedSnapshotMatchesLegacyIdentityAndCapabilityHeader) {
    namespace pci = ker::dev::pci;
    auto const INVENTORY = pci::extended_inventory();
    KEXPECT_EQ(INVENTORY.devices, pci::pci_device_count());
    KEXPECT_NULL(pci::extended_config(nullptr));
    pci::PCIDevice copied{};
    KEXPECT_NULL(pci::extended_config(&copied));
    if (INVENTORY.state != pci::ExtendedState::ready) {
        ker::mod::dbg::logger<"pci-ecam-test">::info("extended inventory unavailable state=%u", static_cast<unsigned>(INVENTORY.state));
        return;
    }
    size_t ready = 0;
    for (size_t i = 0; i < INVENTORY.devices; ++i) {
        auto const* device = pci::pci_get_device(i);
        auto const* snapshot = pci::extended_config(device);
        KREQUIRE_TRUE(snapshot != nullptr);
        if (snapshot->state != pci::ExtendedState::ready) continue;
        ++ready;
        KEXPECT_EQ(snapshot->physical & 4095U, uint64_t{0});
        KEXPECT_TRUE(snapshot->capabilities.valid());
        if (snapshot->capabilities.pcie != 0) {
            auto const OFFSET = static_cast<uint8_t>(snapshot->capabilities.pcie);
            KEXPECT_EQ(pci::pci_config_read8(device->bus, device->slot, device->function, OFFSET), uint8_t{0x10});
            auto const FLAGS = pci::pci_config_read16(device->bus, device->slot, device->function, static_cast<uint8_t>(OFFSET + 2));
            KEXPECT_EQ(FLAGS & 15U, static_cast<unsigned>(snapshot->capabilities.pcie_version));
            KEXPECT_EQ((FLAGS >> 4U) & 15U, static_cast<unsigned>(snapshot->capabilities.pcie_type));
        }
    }
    KEXPECT_EQ(ready, INVENTORY.readable);
    // Collection is idempotent and performs no further MMIO or state mutation.
    pci::collect_extended_config();
    KEXPECT_EQ(pci::extended_inventory().readable, ready);
}
