#include <array>
#include <dev/iommu/page_storage.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <test/ktest.hpp>

namespace {
namespace vtd = ker::dev::iommu::vtd;
namespace mm = ker::mod::mm;
auto owned_pages(mm::PhysicalPageOwner owner = mm::PhysicalPageOwner::IOMMU_TABLE) -> uint64_t {
    mm::phys::PhysicalBalanceSnapshot balance{};
    mm::phys::get_physical_balance_snapshot(balance);
    return balance.owners[static_cast<size_t>(owner)].pages;
}
}  // namespace

KTEST(VtdPageTable, UnpublishedRealTablePagesRestorePhysicalAccounting) {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    auto const BEFORE = owned_pages();
    std::array<vtd::TableRecord, 16> records{};
    vtd::PageStorage storage(records, true, 0);
    vtd::PageTable table(storage);
    KREQUIRE_EQ(table.initialize(52, 48, 4), vtd::TableError::none);
    auto* backing = mm::phys::page_alloc_may_fail(mm::PhysicalPageOwner::DMA_BUFFER, 16384, "iommu_table_test_data");
    if (backing == nullptr) {
        KEXPECT_TRUE(table.destroy_unpublished());
        KREQUIRE_TRUE(backing != nullptr);
    }
    uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(backing) - mm::addr::get_hhdm_offset();
    uint64_t const IOVA = (uint64_t{1} << 30) - 4096;
    KEXPECT_EQ(table.map(IOVA, PHYSICAL, 3 * 4096, vtd::Access::read_write).error, vtd::TableError::none);
    auto const SNAPSHOT = table.snapshot();
    KEXPECT_TRUE(SNAPSHOT.has_value());
    if (SNAPSHOT) {
        KEXPECT_EQ(SNAPSHOT->table_pages, uint64_t{6});
        KEXPECT_EQ(SNAPSHOT->owned_pages, uint64_t{3});
        KEXPECT_FALSE(SNAPSHOT->exposed);
        KEXPECT_FALSE(SNAPSHOT->quarantined);
        KEXPECT_EQ(owned_pages() - BEFORE, SNAPSHOT->table_pages);
        // Inspect real encoded leaves independently of PageTable's walk.
        for (uint64_t offset = 0; offset < 3 * 4096; offset += 4096) {
            uint64_t address = SNAPSHOT->root;
            for (uint8_t level = 4; level != 0; --level) {
                vtd::TranslationTable* current = nullptr;
                for (auto const& record : records)
                    if (record.page.cpu != nullptr && record.page.physical == address) current = record.page.cpu;
                KEXPECT_TRUE(current != nullptr);
                if (current == nullptr) break;
                auto const ENTRY = current->entries.at(((IOVA + offset) >> (12U + 9U * (level - 1U))) & 511U);
                KEXPECT_EQ(ENTRY & 3U, uint64_t{3});
                address = ENTRY & 0x000ffffffffff000;
                if (level == 1) KEXPECT_EQ(address, PHYSICAL + offset);
            }
        }
    }
    KEXPECT_FALSE(table.destroy_unpublished());
    KEXPECT_EQ(table.unmap(IOVA, PHYSICAL, 3 * 4096, vtd::Access::read_write).error, vtd::TableError::none);
    // No hardware context ever referenced this root; no IOTLB proof is claimed.
    KEXPECT_TRUE(table.destroy_unpublished());
    mm::phys::page_free(backing);
    KEXPECT_EQ(owned_pages(), BEFORE);
    for (auto const& record : records) KEXPECT_TRUE(record.page.cpu == nullptr);
}

KTEST(VtdPageTable, UnpublishedCapacityFailureExposesNoDataAndRestoresTables) {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    auto const BEFORE = owned_pages();
    for (size_t capacity = 1; capacity < 4; ++capacity) {
        std::array<vtd::TableRecord, 3> records{};
        vtd::PageStorage storage(std::span(records).first(capacity), true, 0);
        vtd::PageTable table(storage);
        KREQUIRE_EQ(table.initialize(52, 48, 4), vtd::TableError::none);
        auto const RESULT = table.map(4096, 0x800000, 4096, vtd::Access::read);
        KEXPECT_EQ(RESULT.error, vtd::TableError::allocation);
        KEXPECT_FALSE(RESULT.retain_data);
        auto const SNAPSHOT = table.snapshot();
        KEXPECT_TRUE(SNAPSHOT.has_value());
        if (SNAPSHOT) {
            KEXPECT_EQ(SNAPSHOT->table_pages, capacity);
            KEXPECT_EQ(SNAPSHOT->owned_pages, uint64_t{0});
            KEXPECT_EQ(owned_pages() - BEFORE, capacity);
        }
        KEXPECT_TRUE(table.destroy_unpublished());
        KEXPECT_EQ(owned_pages(), BEFORE);
    }
    std::array<vtd::TableRecord, 1> records{};
    vtd::PageStorage invalid_cache(records, false, 3);
    vtd::PageTable table(invalid_cache);
    KEXPECT_EQ(table.initialize(52, 48, 4), vtd::TableError::allocation);
    KEXPECT_TRUE(table.destroy_unpublished());
    KEXPECT_EQ(owned_pages(), BEFORE);
}

KTEST(VtdPageTable, UnpublishedCachePublicationUsesQualifiedClflush) {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    auto const BEFORE = owned_pages();
    ker::mod::cpu::CpuidContext cpu{.function = 1};
    ker::mod::cpu::cpuid(&cpu);
    bool const CLFLUSH = (static_cast<uint32_t>(cpu.edx) & (1U << 19U)) != 0;
    size_t const LINE = CLFLUSH ? ((static_cast<uint32_t>(cpu.ebx) >> 8U) & 255U) * 8U : 0;
    ker::mod::dbg::logger<"vtd-page-table">::info("unpublished cache publication clflush=%u line=%zu", CLFLUSH, LINE);
    std::array<vtd::TableRecord, 8> records{};
    vtd::PageStorage storage(records, false, LINE);
    vtd::PageTable table(storage);
    if (!CLFLUSH) {
        KEXPECT_EQ(table.initialize(52, 48, 4), vtd::TableError::allocation);
        KEXPECT_EQ(owned_pages(), BEFORE);
        return;
    }
    KREQUIRE_TRUE(LINE >= 8 && LINE <= 4096 && (LINE & (LINE - 1)) == 0);
    KREQUIRE_EQ(table.initialize(52, 48, 4), vtd::TableError::none);
    // Two leaf stores cross a cache-line boundary; no hardware context points
    // here. This checks the real CLFLUSH path, not noncoherent device behavior.
    uint64_t const IOVA = ((LINE / sizeof(uint64_t)) - 1) * 4096;
    KEXPECT_EQ(table.map(IOVA, 0x800000, 8192, vtd::Access::read).error, vtd::TableError::none);
    KEXPECT_EQ(table.unmap(IOVA, 0x800000, 8192, vtd::Access::read).error, vtd::TableError::none);
    KEXPECT_TRUE(table.destroy_unpublished());
    KEXPECT_EQ(owned_pages(), BEFORE);
}

KTEST(VtdPageTable, PreparationDiscardReleasesOnlyTablesAndPreservesReferencedBacking) {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    auto const BEFORE = owned_pages();
    auto const DATA_BEFORE = owned_pages(mm::PhysicalPageOwner::DMA_BUFFER);
    std::array<vtd::TableRecord, 5> records{};
    vtd::PageStorage storage(records, true, 0);
    vtd::PageTable table(storage);
    auto* backing =
        static_cast<uint64_t*>(mm::phys::page_alloc_may_fail(mm::PhysicalPageOwner::DMA_BUFFER, 4096, "iommu_discard_test_data"));
    KREQUIRE_TRUE(backing != nullptr);
    auto data = std::span<uint64_t>{backing, 4096 / sizeof(uint64_t)};
    for (size_t i = 0; i < data.size(); ++i) data[i] = 0x93a70b0000000000ULL + i;
    uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(backing) - mm::addr::get_hhdm_offset();
    KEXPECT_EQ(table.initialize(52, 48, 4), vtd::TableError::none);
    KEXPECT_EQ(table.map(4096, PHYSICAL, 4096, vtd::Access::read_write).error, vtd::TableError::none);
    KEXPECT_EQ(table.map(uint64_t{1} << 30, PHYSICAL, 4096, vtd::Access::read).error, vtd::TableError::allocation);
    KEXPECT_EQ(owned_pages() - BEFORE, uint64_t{5});
    KEXPECT_FALSE(table.destroy_unpublished());
    KEXPECT_TRUE(table.discard_unpublished());
    KEXPECT_EQ(owned_pages(), BEFORE);
    KEXPECT_EQ(owned_pages(mm::PhysicalPageOwner::DMA_BUFFER), DATA_BEFORE + 1);
    for (size_t i = 0; i < data.size(); ++i) KEXPECT_EQ(data[i], 0x93a70b0000000000ULL + i);
    for (auto const& record : records) KEXPECT_NULL(record.page.cpu);
    mm::phys::page_free(backing);
    KEXPECT_EQ(owned_pages(mm::PhysicalPageOwner::DMA_BUFFER), DATA_BEFORE);
}
