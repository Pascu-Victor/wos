#include <atomic>
#include <dev/dma/dma.hpp>
#include <dev/ivshmem/ivshmem_net.hpp>
#include <dev/ivshmem/pci_resources.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <test/ktest.hpp>

namespace iv = ker::dev::ivshmem;
namespace pci = ker::dev::pci;
namespace {
iv::PciResources owner;
std::atomic<unsigned> interrupts{0};
void count_irq(uint8_t, void*) { interrupts.fetch_add(1, std::memory_order_relaxed); }
auto bus_master(pci::PCIDevice* device) -> bool {
    return (pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) != 0;
}
}  // namespace

KTEST_FINAL(IvshmemPci, UnpublishedProbeOwnsAndDrainsInterrupts) {
    if (!iv::pci_selftest_reserved()) {
        KEXPECT_TRUE(owner.device == nullptr);
        return;
    }
    pci::PCIDevice* device = nullptr;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* candidate = pci::pci_get_device(i);
        if (candidate != nullptr && candidate->vendor_id == iv::IVSHMEM_VENDOR && candidate->device_id == iv::IVSHMEM_DEVICE) {
            device = candidate;
            break;
        }
    }
    KREQUIRE_TRUE(device != nullptr);
    KREQUIRE_TRUE(!iv::ivshmem_net_is_claimed(device));
    auto const BEFORE = ker::dev::dma::stats(device);
    KEXPECT_EQ(BEFORE.regions, size_t{0});
    constexpr size_t MINIMUM_SHARED = 16 * 1024 * 1024;
    bool const UNDERSIZED = pci::pci_get_bar_size(device, 2) < MINIMUM_SHARED;
    bool const PREPARED = owner.prepare(device, MINIMUM_SHARED);
    KEXPECT_FALSE(bus_master(device));
    if (UNDERSIZED) {
        KEXPECT_FALSE(PREPARED);
        KEXPECT_FALSE(owner.irq_owned);
        KEXPECT_FALSE(owner.activate(count_irq, nullptr, "ivshmem-pci-test"));
        KEXPECT_TRUE(owner.stop_interrupts());
        ker::mod::dbg::log("ivshmem-pci: test rejected undersized BAR; BME=0 IRQ=0 DMA=0");
        return;
    }
    KREQUIRE_TRUE(PREPARED);
    KREQUIRE_TRUE(owner.activate(count_irq, nullptr, "ivshmem-pci-test"));
    bool const MSIX = owner.mode == iv::model::Mode::msix;
    KEXPECT_EQ(owner.irq_owned, MSIX);
    KEXPECT_EQ(bus_master(device), MSIX);
    unsigned delivered = 0;
    if (MSIX) {
        // KTEST parks APs before scheduler startup with a valid BSP GS/IDT.
        // Temporarily admit the real device IRQ, restoring the incoming IF.
        uint64_t flags = 0;
        asm volatile("pushfq; pop %0; sti" : "=r"(flags) : : "memory");
        auto const SELF = static_cast<uint16_t>(owner.regs[2]);
        owner.ring(SELF);
        uint64_t const START = ker::mod::time::get_us();
        while (interrupts.load(std::memory_order_relaxed) == 0 && ker::mod::time::get_us() - START < 1'000'000)
            asm volatile("pause" ::: "memory");
        delivered = interrupts.load(std::memory_order_relaxed);
        if ((flags & (1U << 9)) == 0) asm volatile("cli" ::: "memory");
        KEXPECT_TRUE(delivered > 0);
    }
    KEXPECT_TRUE(owner.stop_interrupts());
    KEXPECT_FALSE(owner.irq_owned);
    KEXPECT_FALSE(bus_master(device));
    KEXPECT_TRUE(owner.stop_interrupts());
    KEXPECT_FALSE(owner.activate(count_irq, nullptr, "ivshmem-pci-repeat"));
    owner.ring(0);
    KEXPECT_EQ(interrupts.load(std::memory_order_relaxed), delivered);
    auto const AFTER = ker::dev::dma::stats(device);
    KEXPECT_EQ(AFTER.regions, BEFORE.regions);
    KEXPECT_EQ(AFTER.backing_pages, BEFORE.backing_pages);
    ker::mod::dbg::log("ivshmem-pci: test mode=%s delivered=%u stopped=%u BME=%u IRQ=%u DMA=%zu", MSIX ? "msix" : "plain-poll", delivered,
                       !owner.irq_owned, bus_master(device), owner.irq_owned, AFTER.regions);
}
