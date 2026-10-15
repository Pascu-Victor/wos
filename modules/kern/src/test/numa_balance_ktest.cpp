#include <array>
#include <cstdint>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_balance.hpp>
#include <test/ktest.hpp>

namespace {
namespace balance = ker::mod::mm::numa::balance;
namespace numa = ker::mod::mm::numa;
namespace topology = ker::mod::acpi::numa;
namespace mm = ker::mod::mm;
using log = ker::mod::dbg::logger<"numa-test">;

// The diagnostic is deliberately bounded to the 8/32 GiB KTEST fixtures.
constexpr uint64_t MAX_FRAMES = 8 * 1024 * 1024;
constexpr size_t OWNER = static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST);
constinit balance::Snapshot snapshot;
constinit balance::Workspace scratch;
constinit std::array<mm::phys::PhysicalOwnerSnapshot, topology::MAX_NODES> owner_baseline{};

auto collect() -> bool {
    uint64_t const BEGIN = ker::mod::time::get_us();
    auto const RESULT = balance::snapshot(snapshot, scratch, MAX_FRAMES);
    uint64_t const END = ker::mod::time::get_us();
    // The O(frames) all-allocator-lock interval is measured, never described as
    // a hot-path operation or a hard wall-clock guarantee.
    log::info("node balance complete=%u error=%u zone=%u paddr=0x%llx frames=%llu elapsed-us=%llu",
              static_cast<unsigned>(snapshot.complete), static_cast<unsigned>(RESULT.error), RESULT.zone_index,
              static_cast<unsigned long long>(RESULT.physical), static_cast<unsigned long long>(snapshot.scanned_frames),
              static_cast<unsigned long long>(END >= BEGIN ? END - BEGIN : 0));
    KEXPECT_TRUE(RESULT.valid());
    KEXPECT_TRUE(snapshot.complete);
    return RESULT.valid() && snapshot.complete;
}

void expect_identity() {
    auto const* published = topology::boot_topology();
    KREQUIRE_TRUE(published != nullptr);
    KEXPECT_EQ(snapshot.node_count, published->node_count);
    KEXPECT_EQ(snapshot.global.identity_mismatch_pages, 0U);
    KEXPECT_EQ(snapshot.global.managed_pages, snapshot.scanned.managed_pages);
    KEXPECT_EQ(snapshot.global.identity_pages, snapshot.global.managed_pages);
    for (size_t node = 0; node < snapshot.node_count; ++node) {
        auto const& row = snapshot.nodes[node];
        KEXPECT_EQ(row.managed_pages, published->nodes[node].usable_bytes / mm::paging::PAGE_SIZE);
        KEXPECT_EQ(row.managed_pages, row.free_pages + row.allocator_metadata_pages + row.zone_descriptor_pages + row.owner_pages);
        KEXPECT_TRUE(row.cached_free_pages <= row.free_pages);
        uint64_t kinds = 0;
        for (auto const pages : row.kind_pages) kinds += pages;
        // A descriptor has no allocator-side kind tag of its own.
        KEXPECT_EQ(kinds, row.managed_pages - row.zone_descriptor_pages);
        uint64_t owners = 0;
        for (auto const& owner : row.owners) owners += owner.pages;
        KEXPECT_EQ(owners, row.owner_pages);
        log::info(
            "node balance node=%zu managed=%llu free=%llu cached-free=%llu metadata=%llu descriptor=%llu owned=%llu selftest=%llu/%llu",
            node, static_cast<unsigned long long>(row.managed_pages), static_cast<unsigned long long>(row.free_pages),
            static_cast<unsigned long long>(row.cached_free_pages), static_cast<unsigned long long>(row.allocator_metadata_pages),
            static_cast<unsigned long long>(row.zone_descriptor_pages), static_cast<unsigned long long>(row.owner_pages),
            static_cast<unsigned long long>(row.owners[OWNER].pages), static_cast<unsigned long long>(row.owners[OWNER].objects));
    }
}
}  // namespace

KTEST(NumaBalance, FrameBudgetRefusesWithoutPartialScan) {
    auto const RESULT = balance::snapshot(snapshot, scratch, 0);
    KEXPECT_EQ(RESULT.error, balance::Error::FRAME_LIMIT);
    KEXPECT_FALSE(snapshot.complete);
    KEXPECT_EQ(snapshot.scanned_frames, 0U);
}

KTEST(NumaBalance, ActualManagedFramesAndNodeOwnerTotalsReconcile) {
    KREQUIRE_TRUE(collect());
    expect_identity();
}

KTEST(NumaBalance, PlacedIndependentHeadsCloseOnTheirPhysicalNode) {
    auto const* published = topology::boot_topology();
    KREQUIRE_TRUE(published != nullptr);
    KREQUIRE_TRUE(collect());
    for (size_t node = 0; node < snapshot.node_count; ++node) owner_baseline[node] = snapshot.nodes[node].owners[OWNER];
    for (numa::NodeId node = 0; node < published->node_count; ++node) {
        if (published->nodes[node].usable_bytes == 0) continue;
        {
            numa::AllocationContext const CONTEXT{
                .policy = {.nodes = uint64_t{1} << node, .mode = numa::Mode::BIND}, .local_node = node, .virtual_page_index = 0};
            auto allocation = numa::try_allocate_pages(CONTEXT, 512, 512 * mm::paging::PAGE_SIZE, mm::PhysicalPageOwner::SELFTEST);
            KREQUIRE_TRUE(allocation.span.data() != nullptr);
            KEXPECT_EQ(allocation.actual, node);
            KREQUIRE_TRUE(collect());
            expect_identity();
            for (size_t observed = 0; observed < snapshot.node_count; ++observed) {
                uint64_t const EXTRA = observed == node ? 512 : 0;
                KEXPECT_EQ(snapshot.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages + EXTRA);
                KEXPECT_EQ(snapshot.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects + EXTRA);
            }
        }
        KREQUIRE_TRUE(collect());
        expect_identity();
        for (size_t observed = 0; observed < snapshot.node_count; ++observed) {
            KEXPECT_EQ(snapshot.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages);
            KEXPECT_EQ(snapshot.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects);
        }
    }
    // Other owners may run on other CPUs; no cross-time free-page quiescence
    // is inferred from this SELFTEST-owner closure.
}
