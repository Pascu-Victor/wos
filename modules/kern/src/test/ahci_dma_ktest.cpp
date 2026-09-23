#include <array>
#include <cerrno>
#include <cstddef>
#include <dev/ahci.hpp>
#include <dev/block_device.hpp>
#include <dev/dma/dma.hpp>
#include <dev/pci.hpp>
#include <platform/mm/phys.hpp>
#include <test/ktest.hpp>
#include <vfs/vfs.hpp>

namespace ahci = ker::dev::ahci;
namespace dma = ker::dev::dma;
namespace pci = ker::dev::pci;
namespace mm = ker::mod::mm;

KTEST_FINAL(AhciDma, ShutdownRetiresMappingsOrPinsUnprovenStop) {
    auto* controller = pci::pci_find_ahci_controller();
    if (controller == nullptr) {
        KEXPECT_EQ(ahci::ahci_shutdown(), 0);
        return;
    }
    auto* disk = ker::dev::block_device_find_by_name("sda");
    KREQUIRE_TRUE(disk != nullptr);
    KREQUIRE_TRUE(disk->read_blocks != nullptr && disk->flush != nullptr);
    std::array<std::byte, 512> sector{};
    // Bypass the buffer cache: this must issue and retire a real ATA command.
    KREQUIRE_EQ(disk->read_blocks(disk, 0, 1, sector.data()), 0);
    KREQUIRE_EQ(ker::vfs::vfs_shutdown_sync(), 0);
    auto const BEFORE = dma::stats(controller);
    KREQUIRE_TRUE(BEFORE.regions != 0 && BEFORE.backing_pages != 0);
    mm::phys::PhysicalBalanceSnapshot balance{};
    mm::phys::get_physical_balance_snapshot(balance);
    auto const OWNER_PAGES = balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::DMA_BUFFER)].pages;
    auto const FAULT = ahci::selftest_retirement_fault();
    int const EXPECTED = FAULT == ahci::SelftestRetirementFault::none ? 0 : -EIO;
    KEXPECT_EQ(ahci::ahci_shutdown(), EXPECTED);
    auto const AFTER = dma::stats(controller);
    if (FAULT == ahci::SelftestRetirementFault::stop) {
        KEXPECT_EQ(AFTER.regions, BEFORE.regions);
        KEXPECT_EQ(AFTER.backing_pages, BEFORE.backing_pages);
        KEXPECT_EQ(AFTER.quarantined, BEFORE.regions);
    } else {
        KEXPECT_EQ(AFTER.regions, size_t{0});
        KEXPECT_EQ(AFTER.backing_pages, size_t{0});
        KEXPECT_EQ(AFTER.device_owned, size_t{0});
    }
    mm::phys::get_physical_balance_snapshot(balance);
    auto const RELEASED = FAULT == ahci::SelftestRetirementFault::stop ? 0 : BEFORE.backing_pages;
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::DMA_BUFFER)].pages, OWNER_PAGES - RELEASED);
    KEXPECT_EQ(
        pci::pci_config_read16(controller->bus, controller->slot, controller->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER, 0);
    // Static providers may still be held by callers after teardown. They must
    // reject every subsequent command without touching released DMA storage.
    KEXPECT_EQ(disk->read_blocks(disk, 0, 1, sector.data()), -EIO);
    KEXPECT_EQ(disk->flush(disk), -EIO);
    KEXPECT_EQ(ahci::ahci_shutdown(), EXPECTED);
    ahci::ahci_controller_init();  // No resurrection or ring reallocation.
    KEXPECT_EQ(dma::stats(controller).regions, AFTER.regions);
    KEXPECT_EQ(dma::stats(controller).backing_pages, AFTER.backing_pages);
    KEXPECT_EQ(disk->read_blocks(disk, 0, 1, sector.data()), -EIO);
}
