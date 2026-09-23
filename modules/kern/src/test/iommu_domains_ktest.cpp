#include <dev/dma/dma.hpp>
#include <dev/iommu/discovery.hpp>
#include <dev/iommu/domains.hpp>
#include <dev/iommu/vtd_activation.hpp>
#include <dev/pci.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <test/ktest.hpp>

KTEST(IommuDomains, PublishedDomainsMatchHardwareAndPhysicalPageAccounting) {
    namespace iommu = ker::dev::iommu;
    namespace pci = ker::dev::pci;
    namespace mm = ker::mod::mm;
    auto const STATS = iommu::domain_stats();
    KREQUIRE_TRUE(STATS.ready);
    KEXPECT_EQ(STATS.devices, pci::pci_device_count());
    mm::phys::PhysicalBalanceSnapshot balance{};
    mm::phys::get_physical_balance_snapshot(balance);
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::IOMMU_TABLE)].pages, STATS.table_pages);
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::IOMMU_TABLE)].objects, STATS.table_pages);
    auto const* inventory = iommu::discovery();
    KREQUIRE_TRUE(inventory != nullptr);
    for (size_t i = 0; i < STATS.devices; ++i) {
        auto* device = pci::pci_get_device(i);
        auto const DOMAIN = iommu::domain_info(device);
        if (STATS.strict) {
            KEXPECT_EQ(DOMAIN.mode, iommu::DomainMode::translated);
            KEXPECT_EQ(DOMAIN.group, i);
            KEXPECT_EQ(DOMAIN.members, uint16_t{1});
        }
        bool const PLATFORM_PENDING =
            DOMAIN.mode == iommu::DomainMode::translated && device->vendor_id == 0x1af4 && device->device_id != 0x1110;
        KEXPECT_EQ(iommu::bus_master_allowed(device), !PLATFORM_PENDING);
        if (PLATFORM_PENDING) {
            KEXPECT_FALSE(pci::pci_set_bus_master(device, true));
            // Raw command writers intentionally panic on an unauthorized
            // request; the fallible typed API is the live refusal test.
            KEXPECT_EQ(pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER,
                       uint16_t{0});
        }
        if (STATS.active_units == 0) {
            KEXPECT_EQ(DOMAIN.mode, iommu::DomainMode::software_passthrough);
            KEXPECT_EQ(STATS.table_pages, size_t{0});
        } else {
            KEXPECT_TRUE(DOMAIN.mode == iommu::DomainMode::hardware_passthrough || DOMAIN.mode == iommu::DomainMode::translated);
            KREQUIRE_TRUE(DOMAIN.unit < inventory->unit_count);
            KEXPECT_NE(DOMAIN.id, uint16_t{0});
            if (DOMAIN.mode == iommu::DomainMode::translated) {
                auto const STATE = iommu::unit_status(DOMAIN.unit);
                KEXPECT_FALSE(STATE.failed);
                KEXPECT_FALSE(STATE.inhibited);
                KEXPECT_EQ(STATE.irq_count, uint64_t{0});
                KEXPECT_NULL(iommu::fault_report(DOMAIN.unit));
            }
            auto const& unit = inventory->units[DOMAIN.unit];
            KEXPECT_TRUE(DOMAIN.id < unit.capability.domains);
            auto const* registers = reinterpret_cast<const volatile uint8_t*>(mm::addr::get_virt_pointer(unit.physical));
            KEXPECT_EQ(*reinterpret_cast<const volatile uint32_t*>(registers + iommu::vtd::GSTS) & iommu::vtd::TE, iommu::vtd::TE);
            KEXPECT_EQ(*reinterpret_cast<const volatile uint32_t*>(registers + 0x34) & 3U, 0U);  // No pending/overflow faults.
        }
    }
    pci::PCIDevice copied{};
    KEXPECT_FALSE(iommu::bus_master_allowed(&copied));
    KEXPECT_FALSE(pci::pci_set_bus_master(&copied, true));
    KEXPECT_FALSE(iommu::bus_master_allowed(nullptr));
    KEXPECT_FALSE(iommu::selftest_limit_table_allocations(0));  // Cannot reconfigure live allocation ownership.
}

namespace {
namespace iommu = ker::dev::iommu;
namespace pci = ker::dev::pci;
namespace mm = ker::mod::mm;
namespace dma = ker::dev::dma;
using log = ker::mod::dbg::logger<"iommu-test">;
// Independent CPU inspection of the actual active hardware root/context/PTEs.
// Called in the single-threaded boot test phase, with no concurrent mutations.
auto active_context_sid(const pci::PCIDevice* device, uint16_t sid) -> iommu::vtd::Entry {
    auto const DOMAIN = iommu::domain_info(device);
    auto const* inventory = iommu::discovery();
    if (DOMAIN.mode != iommu::DomainMode::translated || inventory == nullptr || DOMAIN.unit >= inventory->unit_count) return {};
    auto const& unit = inventory->units[DOMAIN.unit];
    auto* registers = reinterpret_cast<volatile uint8_t*>(mm::addr::get_virt_pointer(unit.physical));
    uint64_t const ROOT = *reinterpret_cast<volatile uint64_t*>(registers + iommu::vtd::RTADDR);
    if (ROOT == 0 || (ROOT & 4095U) != 0) return {};
    auto const* roots = reinterpret_cast<const iommu::vtd::Entry*>(mm::addr::get_virt_pointer(ROOT));
    auto const ENTRY = roots[sid >> 8U];
    if ((ENTRY.low & 4095U) != 1 || ENTRY.high != 0) return {};
    auto const* contexts = reinterpret_cast<const iommu::vtd::Entry*>(mm::addr::get_virt_pointer(ENTRY.low & ~uint64_t{4095}));
    return contexts[sid & 255U];
}
auto active_context(const pci::PCIDevice* device) -> iommu::vtd::Entry {
    return active_context_sid(device, static_cast<uint16_t>((device->bus << 8U) | (device->slot << 3U) | device->function));
}
auto leaf(const pci::PCIDevice* device, uint64_t address) -> uint64_t {
    auto const CONTEXT = active_context(device);
    if ((CONTEXT.low & 4095U) != 1) return UINT64_MAX;
    uint64_t table = CONTEXT.low & ~uint64_t{4095};
    for (unsigned level = static_cast<unsigned>((CONTEXT.high & 7U) + 2U); level > 0; --level) {
        auto const* entries = reinterpret_cast<const uint64_t*>(mm::addr::get_virt_pointer(table));
        uint64_t const ENTRY = entries[(address >> (12U + 9U * (level - 1U))) & 511U];
        if (level == 1 || ENTRY == 0) return ENTRY;
        if ((ENTRY & 4095U) != 3) return UINT64_MAX;
        table = ENTRY & ~uint64_t{4095};
    }
    return UINT64_MAX;
}
}  // namespace

KTEST(IommuDomains, ActiveTranslationMapsOnlyOwnedPagesWithOriginalPermissions) {
    auto hold = ker::mod::sched::hold_kernel_shutdown();
    auto* device = pci::pci_get_device(0);
    KREQUIRE_TRUE(device != nullptr);
    if (iommu::domain_info(device).mode != iommu::DomainMode::translated) {
        log::info("active translation test unavailable in passthrough boot");
        return;
    }
    auto const* grouping = iommu::group_inventory();
    KREQUIRE_TRUE(grouping != nullptr);
    KEXPECT_EQ(grouping->devices, pci::pci_device_count());
    for (size_t r = 0; r < grouping->requester_count; ++r) {
        auto const& requester = grouping->requesters[r];
        auto* owner = pci::pci_get_device(requester.owner);
        auto const EXPECTED = active_context(owner);
        auto const ALIAS = active_context_sid(owner, requester.sid);
        KEXPECT_EQ(ALIAS.low, EXPECTED.low);
        KEXPECT_EQ(ALIAS.high, EXPECTED.high);
    }
    auto const BASELINE = dma::stats();
    for (size_t index = 0; index < pci::pci_device_count(); ++index) {
        device = pci::pci_get_device(index);
        auto const DOMAIN = iommu::domain_info(device);
        auto const CONTEXT = active_context(device);
        KREQUIRE_EQ(CONTEXT.low & 4095U, uint64_t{1});
        KEXPECT_EQ(DOMAIN.group, grouping->owners[index]);
        for (size_t i = 0; i < pci::pci_device_count(); ++i) {
            auto const OTHER = active_context(pci::pci_get_device(i));
            bool const SHARED = grouping->owners[i] == DOMAIN.group;
            KEXPECT_EQ(CONTEXT.low == OTHER.low, SHARED);
            if (iommu::domain_info(pci::pci_get_device(i)).unit == DOMAIN.unit)
                KEXPECT_EQ((CONTEXT.high >> 8U) == (OTHER.high >> 8U), SHARED);
        }
        for (auto direction : {dma::Direction::to_device, dma::Direction::from_device, dma::Direction::bidirectional}) {
            auto result = dma::allocate_streaming(device, 5000, direction);
            KREQUIRE_EQ(result.error, dma::Error::none);
            auto const POOL = result.allocation;
            auto const ADDRESS = reinterpret_cast<uint64_t>(POOL.cpu) - mm::addr::get_hhdm_offset();
            uint64_t const PERMISSIONS = direction == dma::Direction::to_device ? 1 : direction == dma::Direction::from_device ? 2 : 3;
            KEXPECT_EQ(leaf(device, ADDRESS), ADDRESS | PERMISSIONS);
            KEXPECT_EQ(leaf(device, ADDRESS + 4096), (ADDRESS + 4096) | PERMISSIONS);
            std::array<uint64_t, 2> const NEIGHBORS{ADDRESS - 4096, ADDRESS + 8192};
            std::array<uint64_t, 2> neighbor_entries{};
            for (size_t n = 0; n < NEIGHBORS.size(); ++n) {
                uint64_t expected = 0;
                for (auto const& firmware : iommu::reserved_mappings())
                    if (firmware.group == DOMAIN.group && NEIGHBORS[n] >= firmware.base && NEIGHBORS[n] - firmware.base < firmware.bytes)
                        expected = NEIGHBORS[n] | uint64_t{3};
                for (size_t i = 0; i < pci::pci_device_count(); ++i) {
                    if (grouping->owners[i] != DOMAIN.group) continue;
                    auto const EXISTING = dma::mapping_info(pci::pci_get_device(i), NEIGHBORS[n]);
                    if (EXISTING.handle.registry == nullptr) continue;
                    KEXPECT_NE(EXISTING.handle.generation, POOL.handle.generation);
                    uint64_t const FLAGS = EXISTING.direction == dma::Direction::to_device     ? 1
                                           : EXISTING.direction == dma::Direction::from_device ? 2
                                                                                               : 3;
                    expected = (EXISTING.physical + NEIGHBORS[n] - EXISTING.address) | FLAGS;
                }
                neighbor_entries[n] = leaf(device, NEIGHBORS[n]);
                KEXPECT_EQ(neighbor_entries[n], expected);
            }
            for (size_t i = 0; i < pci::pci_device_count(); ++i) {
                auto* other = pci::pci_get_device(i);
                KEXPECT_EQ(leaf(other, ADDRESS), grouping->owners[i] == DOMAIN.group ? ADDRESS | PERMISSIONS : uint64_t{0});
                KEXPECT_EQ(dma::mapping_info(other, ADDRESS).handle.registry != nullptr, i == index);
            }
            // Never submitted to hardware; per-device allocation handles retain
            // exact original permissions even when the hardware root is shared.
            auto const BIND_DIRECTION = direction == dma::Direction::bidirectional ? dma::Direction::to_device : direction;
            KEXPECT_EQ(dma::bind_streaming(POOL.handle, {POOL.cpu, 16}, BIND_DIRECTION), dma::Error::none);
            auto const SUBMISSION = dma::prepare(POOL.handle);
            KREQUIRE_EQ(SUBMISSION.error, dma::Error::none);
            KEXPECT_EQ(dma::complete(SUBMISSION.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
            KEXPECT_EQ(dma::release(POOL.handle), dma::Error::none);
            for (size_t n = 0; n < NEIGHBORS.size(); ++n) KEXPECT_EQ(leaf(device, NEIGHBORS[n]), neighbor_entries[n]);
            for (size_t i = 0; i < pci::pci_device_count(); ++i) {
                KEXPECT_EQ(leaf(pci::pci_get_device(i), ADDRESS), uint64_t{0});
                KEXPECT_EQ(leaf(pci::pci_get_device(i), ADDRESS + 4096), uint64_t{0});
            }
        }
    }
    KEXPECT_EQ(dma::stats().backing_pages, BASELINE.backing_pages);
    KEXPECT_EQ(dma::stats().regions, BASELINE.regions);
    mm::phys::PhysicalBalanceSnapshot balance{};
    mm::phys::get_physical_balance_snapshot(balance);
    KEXPECT_EQ(balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::IOMMU_TABLE)].pages, iommu::domain_stats().table_pages);
    log::info("group roots and aliases exact; directional leaves, allocation ownership and neighbor boundaries verified; backing restored");
}

KTEST(IommuDomains, FirmwareMappingsAreScopedRetainedAndRejectDriverMutation) {
    auto hold = ker::mod::sched::hold_kernel_shutdown();
    auto const MAPPINGS = iommu::reserved_mappings();
    auto const BEFORE = dma::stats();
    uint64_t pages = 0;
    for (auto const& mapping : MAPPINGS) {
        auto* owner = pci::pci_get_device(mapping.group);
        KREQUIRE_TRUE(owner != nullptr);
        auto const DOMAIN = iommu::domain_info(owner);
        KEXPECT_EQ(DOMAIN.group, mapping.group);
        KEXPECT_EQ(DOMAIN.mode, iommu::DomainMode::translated);
        pages += mapping.bytes / 4096;
        for (uint64_t offset = 0; offset < mapping.bytes; offset += 4096) {
            uint64_t const ADDRESS = mapping.base + offset;
            for (size_t i = 0; i < pci::pci_device_count(); ++i) {
                auto* device = pci::pci_get_device(i);
                auto const GROUP = iommu::domain_info(device).group;
                bool const MAPPED = iommu::reserved::overlaps(MAPPINGS, GROUP, ADDRESS, 4096);
                KEXPECT_EQ(leaf(device, ADDRESS), MAPPED ? ADDRESS | uint64_t{3} : uint64_t{0});
                KEXPECT_NULL(dma::mapping_info(device, ADDRESS).handle.registry);
            }
        }
        KEXPECT_EQ(iommu::map(owner, mapping.base, 4096, iommu::vtd::Access::read).error, iommu::vtd::TableError::overlap);
        for (size_t i = 0; i < pci::pci_device_count(); ++i)
            KEXPECT_EQ(iommu::map(pci::pci_get_device(i), mapping.base, 4096, iommu::vtd::Access::read).error,
                       iommu::vtd::TableError::overlap);
        KEXPECT_FALSE(iommu::unmap(owner, mapping.base, mapping.base, 4096, iommu::vtd::Access::read_write));
        KEXPECT_EQ(leaf(owner, mapping.base), mapping.base | uint64_t{3});
        KEXPECT_FALSE(iommu::unit_status(DOMAIN.unit).failed);
    }
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto const DOMAIN = iommu::domain_info(pci::pci_get_device(i));
        uint64_t expected = 0;
        for (auto const& mapping : MAPPINGS)
            if (mapping.group == DOMAIN.group) expected += mapping.bytes / 4096;
        KEXPECT_EQ(DOMAIN.firmware_pages, expected);
    }
    KEXPECT_EQ(iommu::domain_stats().firmware_pages, pages);
    KEXPECT_EQ(dma::stats().regions, BEFORE.regions);
    KEXPECT_EQ(dma::stats().backing_pages, BEFORE.backing_pages);
    log::info("firmware identity mappings=%zu pages=%llu scoped RW; driver mutation refused; firmware backing never allocator-owned",
              MAPPINGS.size(), static_cast<unsigned long long>(pages));
}
