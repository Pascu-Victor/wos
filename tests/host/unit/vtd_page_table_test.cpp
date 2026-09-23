#include <gtest/gtest.h>

#include <dev/iommu/page_table.hpp>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <vector>

namespace {
namespace vtd = ker::dev::iommu::vtd;
using Error = vtd::TableError;
using Access = vtd::Access;
constexpr uint64_t PAGE = 4096;
struct Backend {
    struct Node {
        std::unique_ptr<vtd::TranslationTable> table;
        uint64_t physical, prefix;
        uint8_t level;
        bool published = false;
    };
    std::vector<Node> nodes;
    size_t budget = SIZE_MAX, invalidations = 0, publications = 0, releases = 0;
    uint64_t next = PAGE;
    bool invalidate_ok = true, reject_find = false, invalid_address = false;
    std::function<void()> during_allocate, during_invalidate;

    auto allocate(uint8_t level, uint64_t prefix) -> vtd::TablePage {
        if (during_allocate) during_allocate();
        if (budget == 0) return {};
        --budget;
        auto address = next;
        next += PAGE;
        if (invalid_address) address |= 1;
        auto table = std::make_unique<vtd::TranslationTable>();
        // The engine, not the allocator, must zero new table pages.
        table->entries.fill(UINT64_MAX);
        auto* cpu = table.get();
        nodes.push_back({std::move(table), address, prefix, level});
        return {cpu, address};
    }
    auto find(uint64_t physical, uint8_t level, uint64_t prefix) -> vtd::TranslationTable* {
        if (reject_find) return nullptr;
        for (auto& node : nodes)
            if (node.physical == physical && node.level == level && node.prefix == prefix) return node.table.get();
        return nullptr;
    }
    void publish(void* pointer, size_t bytes) {
        ++publications;
        if (bytes == sizeof(vtd::TranslationTable)) {
            for (auto& node : nodes) {
                if (node.table.get() != pointer) continue;
                for (auto entry : node.table->entries) EXPECT_EQ(entry, 0U);
                node.published = true;
                return;
            }
            FAIL() << "publication of foreign table";
        }
        ASSERT_EQ(bytes, sizeof(uint64_t));
        auto value = *static_cast<uint64_t*>(pointer);
        for (auto& node : nodes) {
            auto address = reinterpret_cast<uintptr_t>(pointer);
            auto base = reinterpret_cast<uintptr_t>(node.table.get());
            if (address < base || address - base >= PAGE) continue;
            ASSERT_TRUE(node.published);
            if (node.level > 1 && value != 0) {
                auto child = value & ~uint64_t{4095};
                bool ready = false;
                for (auto& candidate : nodes) ready |= candidate.physical == child && candidate.published;
                EXPECT_TRUE(ready) << "parent became visible before zero child";
            }
            return;
        }
        FAIL() << "publication outside owned table storage";
    }
    auto invalidate() -> bool {
        ++invalidations;
        if (during_invalidate) during_invalidate();
        return invalidate_ok;
    }
    void release_all() {
        releases += nodes.size();
        nodes.clear();
    }
    // Independent hardware-style walk: follows encoded addresses and intersects
    // permissions; deliberately does not call the engine's find/slot helpers.
    auto walk(uint64_t root, uint8_t levels, uint64_t iova, Access required) const -> std::optional<uint64_t> {
        uint64_t address = root;
        uint8_t permissions = 3;
        for (uint8_t level = levels; level > 0; --level) {
            const vtd::TranslationTable* table = nullptr;
            for (auto& node : nodes)
                if (node.physical == address) table = node.table.get();
            if (table == nullptr) return {};
            uint64_t entry = table->entries[(iova >> (12 + 9 * (level - 1))) & 511];
            if ((entry & ~uint64_t{0x000ffffffffff003}) != 0) return {};
            permissions &= static_cast<uint8_t>(entry & 3);
            if ((permissions & static_cast<uint8_t>(required)) != static_cast<uint8_t>(required)) return {};
            address = entry & 0x000ffffffffff000;
        }
        return address | (iova & 4095);
    }
};

TEST(VtdPageTable, ValidatesConfigurationAndWholeRangesBeforePublication) {
    Backend backend;
    vtd::PageTable table(backend);
    EXPECT_EQ(table.initialize(53, 48, 4), Error::invalid);
    EXPECT_EQ(table.initialize(48, 58, 5), Error::invalid);
    EXPECT_EQ(table.initialize(48, 48, 2), Error::invalid);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    EXPECT_EQ(table.initialize(48, 48, 4), Error::unavailable);
    auto publications = backend.publications;
    for (auto address : {uint64_t{1}, uint64_t{1} << 48, UINT64_MAX - 4095}) {
        EXPECT_EQ(table.map(address, PAGE, PAGE, Access::read).error, Error::invalid);
        EXPECT_EQ(table.map(PAGE, address, PAGE, Access::read).error, Error::invalid);
    }
    for (auto bytes : {uint64_t{0}, uint64_t{1}, uint64_t{64 * 1024 * 1024 + PAGE}, UINT64_MAX})
        EXPECT_EQ(table.map(PAGE, PAGE, bytes, Access::read).error, Error::invalid);
    EXPECT_EQ(table.map((uint64_t{1} << 48) - PAGE, PAGE, PAGE * 2, Access::read).error, Error::invalid);
    EXPECT_EQ(table.map(PAGE, (uint64_t{1} << 48) - PAGE, PAGE * 2, Access::read).error, Error::invalid);
    EXPECT_EQ(table.map(PAGE, PAGE, PAGE, static_cast<Access>(4)).error, Error::invalid);
    EXPECT_EQ(backend.publications, publications);
    EXPECT_EQ(backend.nodes.size(), 1U);
    EXPECT_TRUE(table.destroy_unpublished());
    EXPECT_EQ(backend.releases, 1U);
}

TEST(VtdPageTable, ExactPermissionsAndDomainSeparationAcrossEverySupportedLevel) {
    for (uint8_t levels : {3, 4, 5}) {
        for (auto permission : {Access::read, Access::write, Access::read_write}) {
            Backend a, b;
            vtd::PageTable first(a), second(b);
            ASSERT_EQ(first.initialize(48, 12 + 9 * levels, levels), Error::none);
            ASSERT_EQ(second.initialize(48, 12 + 9 * levels, levels), Error::none);
            auto root = first.snapshot()->root;
            uint64_t iova = (uint64_t{1} << (12 + 9 * (levels - 1))) - PAGE;
            ASSERT_EQ(first.map(iova, 0x234000, PAGE * 3, permission).error, Error::none);
            for (uint64_t offset : {uint64_t{0}, PAGE - 1, PAGE, 3 * PAGE - 1}) {
                EXPECT_EQ(a.walk(root, levels, iova + offset, permission), 0x234000 + offset);
                EXPECT_FALSE(b.walk(second.snapshot()->root, levels, iova + offset, permission));
                if (permission != Access::read_write)
                    EXPECT_FALSE(a.walk(root, levels, iova + offset, permission == Access::read ? Access::write : Access::read));
            }
            EXPECT_FALSE(a.walk(root, levels, iova - 1, Access::read));
            EXPECT_FALSE(a.walk(root, levels, iova + 3 * PAGE, Access::write));
            EXPECT_FALSE(first.destroy_unpublished());
            ASSERT_EQ(first.unmap(iova, 0x234000, PAGE * 3, permission).error, Error::none);
            EXPECT_EQ(first.snapshot()->owned_pages, 0U);
            EXPECT_EQ(a.invalidations, 0U);  // Root never exposed.
            EXPECT_TRUE(first.destroy_unpublished());
            EXPECT_TRUE(second.destroy_unpublished());
        }
    }
}

TEST(VtdPageTable, PreparationFailureNeverPublishesPartialDataAndBranchesStayOwned) {
    // Crossing a 1-GiB boundary needs five child tables at four levels.
    for (size_t budget = 0; budget <= 5; ++budget) {
        Backend backend;
        vtd::PageTable table(backend);
        ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
        ASSERT_TRUE(table.expose(*table.snapshot()));
        auto root = table.snapshot()->root;
        backend.budget = budget;
        uint64_t iova = (uint64_t{1} << 30) - PAGE;
        auto result = table.map(iova, 0x800000, PAGE * 3, Access::read_write);
        if (budget < 5) {
            EXPECT_EQ(result.error, Error::allocation);
            EXPECT_FALSE(result.retain_data);
            EXPECT_EQ(table.snapshot()->owned_pages, 0U);
            EXPECT_EQ(backend.invalidations, 0U);
            for (size_t offset = 0; offset < 3 * PAGE; offset += PAGE) EXPECT_FALSE(backend.walk(root, 4, iova + offset, Access::read));
            backend.budget = SIZE_MAX;
            EXPECT_EQ(table.map(iova, 0x800000, PAGE * 3, Access::read_write).error, Error::none);
        } else {
            EXPECT_EQ(result.error, Error::none);
        }
        EXPECT_EQ(table.snapshot()->table_pages, 6U);
        EXPECT_EQ(backend.nodes.size(), 6U);
        EXPECT_EQ(table.snapshot()->owned_pages, 3U);
        EXPECT_EQ(backend.invalidations, 1U);
        EXPECT_EQ(table.unmap(iova, 0x800000, PAGE * 3, Access::read_write).error, Error::none);
        EXPECT_FALSE(table.destroy_unpublished());
        EXPECT_EQ(backend.releases, 0U);  // Empty exposed hierarchy remains owned.
    }
}

TEST(VtdPageTable, OverlapAndWrongUnmapAreAtomicAcrossCompleteRange) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    ASSERT_TRUE(table.expose(*table.snapshot()));
    ASSERT_EQ(table.map(2 * PAGE, 0xa000, 2 * PAGE, Access::read).error, Error::none);
    auto publications = backend.publications;
    EXPECT_EQ(table.map(PAGE, 0x9000, 4 * PAGE, Access::read).error, Error::overlap);
    EXPECT_EQ(table.unmap(2 * PAGE, 0xb000, 2 * PAGE, Access::read).error, Error::mismatch);
    EXPECT_EQ(table.unmap(2 * PAGE, 0xa000, 2 * PAGE, Access::write).error, Error::mismatch);
    EXPECT_EQ(table.unmap(PAGE, 0x9000, 2 * PAGE, Access::read).error, Error::mismatch);
    EXPECT_EQ(backend.publications, publications);
    EXPECT_EQ(backend.invalidations, 1U);
    EXPECT_EQ(table.snapshot()->owned_pages, 2U);
    EXPECT_FALSE(backend.walk(table.snapshot()->root, 4, PAGE, Access::read));
    EXPECT_EQ(backend.walk(table.snapshot()->root, 4, 2 * PAGE, Access::read), 0xa000U);
}

TEST(VtdPageTable, InvalidationFailurePinsBothNewMappingsAndClearedMappings) {
    for (bool fail_map : {false, true}) {
        Backend backend;
        vtd::PageTable table(backend);
        ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
        ASSERT_TRUE(table.expose(*table.snapshot()));
        auto root = table.snapshot()->root;
        backend.invalidate_ok = !fail_map;
        auto result = table.map(PAGE, 0x40000, PAGE * 2, Access::write);
        if (!fail_map) {
            ASSERT_EQ(result.error, Error::none);
            backend.invalidate_ok = false;
            result = table.unmap(PAGE, 0x40000, PAGE * 2, Access::write);
        }
        EXPECT_EQ(result.error, Error::quarantined);
        EXPECT_TRUE(result.retain_data);
        EXPECT_TRUE(table.snapshot()->quarantined);
        EXPECT_EQ(table.snapshot()->owned_pages, 2U);
        EXPECT_EQ(backend.walk(root, 4, PAGE, Access::write).has_value(), fail_map);
        EXPECT_EQ(table.map(4 * PAGE, 0x60000, PAGE, Access::read).error, Error::quarantined);
        EXPECT_EQ(table.unmap(PAGE, 0x40000, PAGE * 2, Access::write).error, Error::quarantined);
        EXPECT_FALSE(table.expose(*table.snapshot()));
        EXPECT_FALSE(table.destroy_unpublished());
        EXPECT_EQ(backend.releases, 0U);
    }
}

TEST(VtdPageTable, PublicationPrecedesInvalidationAndMutationCannotReenter) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    ASSERT_TRUE(table.expose(*table.snapshot()));
    auto root = table.snapshot()->root;
    auto token = *table.snapshot();
    backend.during_allocate = [&] {
        EXPECT_EQ(table.map(PAGE, 0x8000, PAGE, Access::read).error, Error::busy);
        EXPECT_EQ(table.unmap(PAGE, 0x8000, PAGE, Access::read).error, Error::busy);
        EXPECT_FALSE(table.snapshot());
        EXPECT_FALSE(table.destroy_unpublished());
        EXPECT_FALSE(table.expose(token));
    };
    backend.during_invalidate = [&] {
        EXPECT_EQ(backend.walk(root, 4, PAGE, Access::read), 0x8000U);
        EXPECT_FALSE(table.snapshot());
    };
    ASSERT_EQ(table.map(PAGE, 0x8000, PAGE, Access::read).error, Error::none);
    backend.during_invalidate = [&] { EXPECT_FALSE(backend.walk(root, 4, PAGE, Access::read)); };
    ASSERT_EQ(table.unmap(PAGE, 0x8000, PAGE, Access::read).error, Error::none);
}

TEST(VtdPageTable, RejectsForeignWrongLevelWrongSubtreeAndReservedEntries) {
    for (unsigned corruption = 0; corruption < 5; ++corruption) {
        Backend backend;
        vtd::PageTable table(backend);
        ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
        ASSERT_EQ(table.map(PAGE, 0x8000, PAGE, Access::read).error, Error::none);
        auto root = table.snapshot()->root;
        auto* entries = &backend.nodes.front().table->entries;
        if (corruption == 0) entries->at(0) = 0x12345003;           // Foreign table.
        if (corruption == 1) entries->at(0) = root | 3;             // Wrong level/cycle.
        if (corruption == 2) backend.nodes[1].prefix = 1ULL << 39;  // Wrong subtree.
        if (corruption == 3) entries->at(0) |= 0x80;                // Unsupported huge-page bit.
        if (corruption == 4) entries->at(0) = 0x100;                // Nonzero absent encoding.
        auto result = table.map(PAGE * 2, 0x9000, PAGE, Access::read);
        EXPECT_EQ(result.error, Error::corrupt);
        EXPECT_TRUE(result.retain_data);
        EXPECT_TRUE(table.snapshot()->quarantined);
        EXPECT_EQ(backend.releases, 0U);
    }
    Backend backend;
    vtd::PageTable table(backend);
    backend.invalid_address = true;
    EXPECT_EQ(table.initialize(48, 48, 4), Error::corrupt);
    EXPECT_TRUE(table.snapshot()->quarantined);
    EXPECT_TRUE(table.destroy_unpublished());
    EXPECT_EQ(backend.releases, 1U);
}

TEST(VtdPageTable, RandomizedSparseMappingsMatchIndependentGoldenMap) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    ASSERT_TRUE(table.expose(*table.snapshot()));
    std::mt19937 random(0x20d4);
    std::map<uint64_t, std::pair<uint64_t, Access>> golden;
    auto root = table.snapshot()->root;
    for (size_t step = 0; step < 2000; ++step) {
        uint64_t iova = uint64_t(random() % 256) * PAGE;
        auto found = golden.find(iova);
        if (found == golden.end()) {
            uint64_t physical = (uint64_t(random() % 1024) + 1024) * PAGE;
            auto access = static_cast<Access>(random() % 3 + 1);
            ASSERT_EQ(table.map(iova, physical, PAGE, access).error, Error::none);
            golden[iova] = {physical, access};
        } else {
            ASSERT_EQ(table.unmap(iova, found->second.first, PAGE, found->second.second).error, Error::none);
            golden.erase(found);
        }
        for (uint64_t page = 0; page < 256; ++page) {
            auto expected = golden.find(page * PAGE);
            for (auto access : {Access::read, Access::write}) {
                auto actual = backend.walk(root, 4, page * PAGE + 37, access);
                if (expected == golden.end() || (static_cast<unsigned>(expected->second.second) & static_cast<unsigned>(access)) == 0)
                    ASSERT_FALSE(actual);
                else
                    ASSERT_EQ(actual, expected->second.first + 37);
            }
        }
        ASSERT_EQ(table.snapshot()->owned_pages, golden.size());
    }
}

TEST(VtdPageTable, FullMappingCapacityAndLastAddressPagesRemainExact) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    uint64_t const LAST = (uint64_t{1} << 48) - PAGE;
    ASSERT_EQ(table.map(LAST, LAST, PAGE, Access::read_write).error, Error::none);
    EXPECT_EQ(backend.walk(table.snapshot()->root, 4, LAST + PAGE - 1, Access::write), LAST + PAGE - 1);
    ASSERT_EQ(table.unmap(LAST, LAST, PAGE, Access::read_write).error, Error::none);
    constexpr uint64_t BYTES = 64 * 1024 * 1024;
    ASSERT_EQ(table.map(0, 0, BYTES, Access::read_write).error, Error::none);
    auto root = table.snapshot()->root;
    for (uint64_t address = 0; address < BYTES; address += PAGE)
        ASSERT_EQ(backend.walk(root, 4, address + PAGE - 1, Access::read), address + PAGE - 1);
    EXPECT_FALSE(backend.walk(root, 4, BYTES, Access::read));
    EXPECT_EQ(table.snapshot()->owned_pages, BYTES / PAGE);
    ASSERT_EQ(table.unmap(0, 0, BYTES, Access::read_write).error, Error::none);
    EXPECT_EQ(table.snapshot()->owned_pages, 0U);
    EXPECT_TRUE(table.destroy_unpublished());
    EXPECT_EQ(backend.releases, 38U);  // Last-address path and 64-MiB low path.
}

TEST(VtdPageTable, CompetingCpuCannotObserveOrMutateAnAllocatingTree) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    auto token = *table.snapshot();
    std::atomic<bool> entered{false}, release{false};
    backend.during_allocate = [&] {
        entered.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
    };
    vtd::TableResult result{};
    std::thread owner([&] { result = table.map(PAGE, 0x8000, PAGE, Access::read); });
    while (!entered.load(std::memory_order_acquire)) std::this_thread::yield();
    EXPECT_EQ(table.map(2 * PAGE, 0x9000, PAGE, Access::read).error, Error::busy);
    EXPECT_EQ(table.unmap(PAGE, 0x8000, PAGE, Access::read).error, Error::busy);
    EXPECT_FALSE(table.snapshot());
    EXPECT_FALSE(table.expose(token));
    EXPECT_FALSE(table.destroy_unpublished());
    EXPECT_FALSE(table.discard_unpublished());
    release.store(true, std::memory_order_release);
    owner.join();
    EXPECT_EQ(result.error, Error::none);
    EXPECT_EQ(table.snapshot()->owned_pages, 1U);
    EXPECT_EQ(table.unmap(PAGE, 0x8000, PAGE, Access::read).error, Error::none);
    EXPECT_TRUE(table.destroy_unpublished());
}

TEST(VtdPageTable, RootExposureRejectsStaleForeignAndExhaustedLifetimeTokens) {
    Backend a, b;
    vtd::PageTable<Backend, 2> table(a), other(b);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    ASSERT_EQ(other.initialize(48, 48, 4), Error::none);
    auto stale = *table.snapshot();
    auto foreign = *other.snapshot();
    ASSERT_EQ(stale.root, foreign.root);
    ASSERT_EQ(stale.generation, foreign.generation);
    EXPECT_FALSE(table.expose(foreign));
    ASSERT_TRUE(table.destroy_unpublished());
    a.next = stale.root;  // The allocator reuses the same physical root page.
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    auto current = *table.snapshot();
    ASSERT_EQ(current.root, stale.root);
    EXPECT_NE(current.generation, stale.generation);
    EXPECT_FALSE(table.expose(stale));
    auto wrong_address = current;
    wrong_address.root += PAGE;
    EXPECT_FALSE(table.expose(wrong_address));
    EXPECT_TRUE(table.expose(current));
    EXPECT_FALSE(table.destroy_unpublished());
    ASSERT_TRUE(other.destroy_unpublished());
    ASSERT_EQ(other.initialize(48, 48, 4), Error::none);
    ASSERT_TRUE(other.destroy_unpublished());
    EXPECT_EQ(other.initialize(48, 48, 4), Error::unavailable);
    EXPECT_TRUE(b.nodes.empty());
}
TEST(VtdPageTable, PreparationDiscardReleasesMappedAndPartialTreesWithoutInvalidation) {
    for (size_t budget : {4U, 5U}) {
        Backend backend;
        backend.budget = budget;
        vtd::PageTable table(backend);
        ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
        ASSERT_EQ(table.map(PAGE, 0x800000, PAGE, Access::read_write).error, Error::none);
        auto const TOKEN = *table.snapshot();
        EXPECT_EQ(table.map(uint64_t{1} << 30, 0x900000, PAGE, Access::read).error, Error::allocation);
        EXPECT_EQ(table.snapshot()->owned_pages, 1U);
        EXPECT_EQ(backend.nodes.size(), budget);
        EXPECT_FALSE(table.destroy_unpublished());
        EXPECT_TRUE(table.discard_unpublished());
        EXPECT_TRUE(backend.nodes.empty());
        EXPECT_EQ(backend.releases, budget);
        EXPECT_EQ(backend.invalidations, 0U);
        EXPECT_EQ(table.snapshot()->owned_pages, 0U);
        EXPECT_EQ(table.snapshot()->root, 0U);
        backend.budget = SIZE_MAX;
        backend.next = TOKEN.root;
        ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
        EXPECT_EQ(table.snapshot()->root, TOKEN.root);
        EXPECT_GT(table.snapshot()->generation, TOKEN.generation);
        EXPECT_FALSE(table.expose(TOKEN));
        EXPECT_TRUE(table.destroy_unpublished());
    }
}

TEST(VtdPageTable, PreparationDiscardNeverReleasesAnExposedTree) {
    Backend backend;
    vtd::PageTable table(backend);
    ASSERT_EQ(table.initialize(48, 48, 4), Error::none);
    ASSERT_EQ(table.map(PAGE, 0x800000, PAGE, Access::read_write).error, Error::none);
    ASSERT_TRUE(table.expose(*table.snapshot()));
    EXPECT_FALSE(table.discard_unpublished());
    ASSERT_EQ(table.unmap(PAGE, 0x800000, PAGE, Access::read_write).error, Error::none);
    EXPECT_EQ(table.snapshot()->owned_pages, 0U);
    EXPECT_FALSE(table.discard_unpublished());
    backend.invalidate_ok = false;
    EXPECT_EQ(table.map(PAGE, 0x800000, PAGE, Access::read_write).error, Error::quarantined);
    EXPECT_FALSE(table.discard_unpublished());
    EXPECT_EQ(backend.releases, 0U);
    EXPECT_EQ(backend.nodes.size(), 4U);
}
}  // namespace
