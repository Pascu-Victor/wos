#include <array>
#include <dev/dma/dma.hpp>
#include <dev/pci.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>
#include <test/ktest.hpp>

namespace dma = ker::dev::dma;
namespace pci = ker::dev::pci;
namespace mm = ker::mod::mm;

KTEST(Dma, CoherentAllocationPublicationAndReleasePreservePhysicalOwnership) {
    auto const BEFORE = dma::stats();
    auto* device = pci::pci_get_device(0);
    KREQUIRE_TRUE(device != nullptr);
    auto result = dma::allocate_coherent(device, 5000, 65536);
    KREQUIRE_EQ(result.error, dma::Error::none);
    auto const A = result.allocation;
    KREQUIRE_TRUE(A.cpu != nullptr);
    KEXPECT_EQ(reinterpret_cast<uint64_t>(A.cpu) & 65535U, uint64_t{0});
    for (size_t i = 0; i < A.size; ++i) KEXPECT_EQ(A.cpu[i], std::byte{0});
    mm::phys::PhysicalBalanceSnapshot balance{};
    mm::phys::get_physical_balance_snapshot(balance);
    auto const ACTIVE = dma::stats();
    KEXPECT_EQ(ACTIVE.regions, BEFORE.regions + 1);
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::DMA_BUFFER)].pages, ACTIVE.backing_pages);
    auto tx = dma::prepare(A.handle);
    KREQUIRE_EQ(tx.error, dma::Error::none);
    KEXPECT_EQ(tx.submission.address & 65535U, uint64_t{0});
    KEXPECT_EQ(dma::release(A.handle), dma::Error::busy);
    KEXPECT_EQ(dma::complete(tx.submission, 0, dma::Retirement::command_completed), dma::Error::invalid_completion);
    // No device register/descriptor receives this test address. Retirement is
    // cancellation before hardware exposure, not a pretend DMA completion.
    KEXPECT_EQ(dma::complete(tx.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    KEXPECT_EQ(dma::release(A.handle), dma::Error::none);
    KEXPECT_EQ(dma::release(A.handle), dma::Error::invalid_handle);
    KEXPECT_EQ(dma::stats().regions, BEFORE.regions);
    KEXPECT_EQ(dma::stats().backing_pages, BEFORE.backing_pages);
    mm::phys::get_physical_balance_snapshot(balance);
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::DMA_BUFFER)].pages, BEFORE.backing_pages);
}

KTEST(Dma, StreamingBounceCopiesStayWithinBorrowedExtentAndReuseWithoutAllocation) {
    std::array<std::byte, 32> buffer{};
    buffer.fill(std::byte{0x11});
    auto result = dma::map_streaming(pci::pci_get_device(0), buffer, dma::Direction::bidirectional);
    KREQUIRE_EQ(result.error, dma::Error::none);
    KEXPECT_TRUE(result.allocation.cpu == nullptr);
    auto const HANDLE = result.allocation.handle;
    auto const PAGES = dma::stats().backing_pages;
    ker::mod::sys::Spinlock context;
    uint64_t const FLAGS = context.lock_irqsave();
    // Submission of preallocated storage remains usable with IRQs disabled.
    // KTEST has no current task: runtime allocation-context rejection is
    // exercised by the shared-manager host test, not inferred from boot state.
    auto tx = dma::prepare(HANDLE);
    context.unlock_irqrestore(FLAGS);
    KREQUIRE_EQ(tx.error, dma::Error::none);
    auto* bounce = reinterpret_cast<std::byte*>(mm::addr::get_virt_pointer(tx.submission.address));
    KEXPECT_TRUE(bounce != buffer.data());
    for (size_t i = 0; i < buffer.size(); ++i) {
        KEXPECT_EQ(bounce[i], std::byte{0x11});
        bounce[i] = std::byte{0x22};  // Test-only emulated device writes; no hardware submission.
    }
    uint64_t const COMPLETE_FLAGS = context.lock_irqsave();
    auto const COMPLETED = dma::complete(tx.submission, 7, dma::Retirement::never_submitted);
    context.unlock_irqrestore(COMPLETE_FLAGS);
    KEXPECT_EQ(COMPLETED, dma::Error::none);
    for (size_t i = 0; i < buffer.size(); ++i) KEXPECT_EQ(buffer[i], i < 7 ? std::byte{0x22} : std::byte{0x11});
    KEXPECT_EQ(dma::bind_streaming(HANDLE, buffer, dma::Direction::bidirectional), dma::Error::none);
    auto next = dma::prepare(HANDLE);
    KREQUIRE_EQ(next.error, dma::Error::none);
    KEXPECT_EQ(dma::complete(tx.submission, 0, dma::Retirement::never_submitted), dma::Error::invalid_completion);
    KEXPECT_EQ(dma::complete(next.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    KEXPECT_EQ(dma::stats().backing_pages, PAGES);
    KEXPECT_EQ(dma::release(HANDLE), dma::Error::none);
}

KTEST(Dma, InvalidMasksAndDevicesNeverLeaveOwnedBacking) {
    auto const BEFORE = dma::stats();
    pci::PCIDevice copied{};
    KEXPECT_EQ(dma::allocate_coherent(&copied, 4096).error, dma::Error::unavailable_device);
    KEXPECT_EQ(dma::allocate_coherent(pci::pci_get_device(0), 4096, 3).error, dma::Error::invalid_argument);
    KEXPECT_EQ(dma::allocate_coherent(pci::pci_get_device(0), SIZE_MAX).error, dma::Error::invalid_argument);
    KEXPECT_EQ(dma::allocate_coherent(pci::pci_get_device(0), 4096, 4096, 0xfff).error, dma::Error::address_limit);
    KEXPECT_EQ(dma::stats().regions, BEFORE.regions);
    KEXPECT_EQ(dma::stats().backing_pages, BEFORE.backing_pages);
}

KTEST(Dma, PreallocatedPoolBindsInAtomicContextAndConsumesEachBorrow) {
    auto const BEFORE = dma::stats();
    auto result = dma::allocate_streaming(pci::pci_get_device(0), 128, dma::Direction::bidirectional);
    KREQUIRE_EQ(result.error, dma::Error::none);
    auto const POOL = result.allocation;
    KREQUIRE_TRUE(POOL.cpu != nullptr);
    KEXPECT_EQ(dma::prepare(POOL.handle).error, dma::Error::invalid_argument);
    std::array<std::byte, 16> buffer{};
    buffer.fill(std::byte{0x73});
    ker::mod::sys::Spinlock context;
    uint64_t const FLAGS = context.lock_irqsave();
    auto const BOUND = dma::bind_streaming(POOL.handle, buffer, dma::Direction::to_device);
    auto tx = dma::prepare(POOL.handle);
    auto const COMPLETED = dma::complete(tx.submission, 0, dma::Retirement::never_submitted);
    context.unlock_irqrestore(FLAGS);
    KEXPECT_EQ(BOUND, dma::Error::none);
    KEXPECT_EQ(tx.error, dma::Error::none);
    KEXPECT_EQ(COMPLETED, dma::Error::none);
    for (size_t i = 0; i < buffer.size(); ++i) KEXPECT_EQ(POOL.cpu[i], std::byte{0x73});
    KEXPECT_EQ(dma::prepare(POOL.handle).error, dma::Error::invalid_argument);
    KEXPECT_EQ(dma::bind_streaming(POOL.handle, {POOL.cpu + 1, 8}, dma::Direction::from_device), dma::Error::invalid_argument);
    KEXPECT_EQ(dma::bind_streaming(POOL.handle, {POOL.cpu, 8}, dma::Direction::from_device), dma::Error::none);
    auto next = dma::prepare(POOL.handle);
    KREQUIRE_EQ(next.error, dma::Error::none);
    KEXPECT_EQ(dma::complete(next.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    KEXPECT_EQ(dma::release(POOL.handle), dma::Error::none);
    KEXPECT_EQ(dma::stats().regions, BEFORE.regions);
    KEXPECT_EQ(dma::stats().backing_pages, BEFORE.backing_pages);
}
