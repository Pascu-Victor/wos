#ifdef WOS_SELFTEST

#include <array>
#include <cstdint>
#include <cstring>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_root_reserve.hpp>
#include <platform/mm/virt.hpp>
#include <test/ktest.hpp>

namespace {
namespace mm = ker::mod::mm;
namespace virt = mm::virt;
namespace phys = mm::phys;
namespace paging = mm::paging;
namespace numa = mm::numa;
namespace reserve = numa::root_reserve;
using log = ker::mod::dbg::logger<"root-reserve-test">;
constexpr uint64_t VA = 0x69000000ULL;

// Small fixed native stack objects are instrumented/addressable before the
// excluded factory and instrumented reserve snapshot consume their references.
auto snapshot() -> reserve::Snapshot {
    reserve::Snapshot out{};
    reserve::snapshot(out);
    return out;
}
auto control_snapshot() -> virt::UserRootControlStats {
    virt::UserRootControlStats out{};
    virt::get_user_root_control_stats_snapshot(out);
    return out;
}
auto pool_snapshot() -> virt::PageTablePoolStatsSnapshot {
    virt::PageTablePoolStatsSnapshot out{};
    virt::get_page_table_pool_stats_snapshot(out);
    return out;
}
auto first_available(const reserve::Snapshot& state) -> numa::NodeId {
    for (numa::NodeId node = 0; node < state.node_count; ++node)
        if (state.nodes[node].available != 0) return node;
    return numa::INVALID_NODE;
}
void expect_valid(const reserve::Snapshot& state) {
    KEXPECT_TRUE(state.published);
    KEXPECT_TRUE(state.healthy);
    KEXPECT_NE(state.node_count, 0U);
    uint32_t members = 0;
    for (numa::NodeId node = 0; node < state.node_count; ++node) {
        auto const& counts = state.nodes[node];
        KEXPECT_EQ(counts.capacity + counts.shortfall, reserve::PAGES_PER_NODE);
        KEXPECT_EQ(counts.available + counts.loaned + counts.transfers, counts.capacity);
        KEXPECT_EQ(counts.transfers, 0U);
        members += counts.capacity;
    }
    KEXPECT_EQ(members, state.members);
}
void expect_same_debt(const reserve::Snapshot& before, const reserve::Snapshot& after) {
    expect_valid(after);
    KEXPECT_EQ(after.members, before.members);
    KEXPECT_EQ(after.node_count, before.node_count);
    KEXPECT_TRUE(after.sequence >= before.sequence);
    for (numa::NodeId node = 0; node < before.node_count; ++node) {
        KEXPECT_EQ(after.nodes[node].capacity, before.nodes[node].capacity);
        KEXPECT_EQ(after.nodes[node].shortfall, before.nodes[node].shortfall);
        KEXPECT_EQ(after.nodes[node].available, before.nodes[node].available);
        KEXPECT_EQ(after.nodes[node].loaned, before.nodes[node].loaned);
        KEXPECT_EQ(after.nodes[node].transfers, before.nodes[node].transfers);
    }
}
void expect_same_pool(const virt::PageTablePoolStatsSnapshot& before, const virt::PageTablePoolStatsSnapshot& after) {
    KEXPECT_EQ(after.capacity, before.capacity);
    KEXPECT_EQ(after.cached_pages, before.cached_pages);
    KEXPECT_EQ(after.alloc_hits, before.alloc_hits);
    KEXPECT_EQ(after.alloc_misses, before.alloc_misses);
    KEXPECT_EQ(after.releases, before.releases);
    KEXPECT_EQ(after.rejects, before.rejects);
}
void expect_same_controls(const virt::UserRootControlStats& before, const virt::UserRootControlStats& after) {
    KEXPECT_EQ(after.active_roots, before.active_roots);
    KEXPECT_EQ(after.control_pages, before.control_pages);
    KEXPECT_TRUE(after.last_serial >= before.last_serial);
}
auto zeroed(paging::PageTable* root) -> bool {
    auto const* bytes = reinterpret_cast<const uint8_t*>(root);
    for (size_t i = 0; i < paging::PAGE_SIZE; ++i)
        if (bytes[i] != 0) return false;
    return true;
}
struct Root {
    paging::PageTable* value{};
    bool mapped{};
    Root() = default;
    Root(const Root&) = delete;
    auto operator=(const Root&) -> Root& = delete;
    ~Root() { reset(); }
    void reset() {
        if (value == nullptr) return;
        if (mapped) virt::destroy_user_space(value, 0, "root-reserve-test", "owned-private");
        virt::release_pagemap(value);
        value = nullptr;
        mapped = false;
    }
    auto take(numa::NodeId node, virt::SelftestRootFactoryOutput& output, bool fail_control = false) -> bool {
        virt::SelftestRootFactoryOptions const options{
            .force_ordinary_failure = true, .use_node_override = true, .node_override = node, .fail_needed_control_chunk = fail_control};
        value = virt::selftest_create_pagemap(options, output);
        return value != nullptr;
    }
    auto map_one() -> bool {
        auto* page = phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, paging::PAGE_SIZE, "root-reserve-test");
        if (page == nullptr) return false;
        std::memset(page, 0x63, paging::PAGE_SIZE);
        auto const address = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        virt::map_page(value, VA, address, paging::page_types::USER);
        mapped = true;
        return true;
    }
};
struct Roots {
    std::array<paging::PageTable*, 96> values{};
    ~Roots() {
        for (auto* root : values)
            if (root != nullptr) virt::release_pagemap(root);
    }
};
struct Walk {
    virt::DestroyUserSpaceBudgetState* value{};
    ~Walk() {
        if (value != nullptr) virt::destroy_user_space_budget_state_destroy(value);
    }
};
void expect_loan(paging::PageTable* root, numa::NodeId node, const virt::SelftestRootFactoryOutput& output) {
    KEXPECT_FALSE(output.ordinary_allocator_called);
    KEXPECT_TRUE(output.ordinary_failure_injected);
    KEXPECT_TRUE(output.override_used);
    KEXPECT_EQ(output.requested, node);
    KEXPECT_TRUE(output.reserve_attempted);
    KEXPECT_EQ(output.reserve_error, reserve::Error::NONE);
    KEXPECT_TRUE(output.control_registered);
    KEXPECT_EQ(phys::page_kind_get(root), mm::PageKind::PAGE_TABLE);
    KEXPECT_EQ(phys::page_ref_get(root), 1U);
    KEXPECT_TRUE(
        phys::page_table_owner_transition_if_exclusive(root, mm::PhysicalPageOwner::PAGE_TABLE, mm::PhysicalPageOwner::PAGE_TABLE));
    auto const address = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(root)));
    KEXPECT_EQ(numa::physical_node(address), node);
    KEXPECT_NE(virt::selftest_user_root_incarnation(root), 0U);
    KEXPECT_TRUE(zeroed(root));
}
}  // namespace

KTEST(NumaRootReserve, PublishedCapacityAndActualMembersReconcile) {
    auto const before = snapshot();
    expect_valid(before);
    for (numa::NodeId node = 0; node < before.node_count; ++node) {
        KREQUIRE_TRUE(before.nodes[node].available <= reserve::PAGES_PER_NODE);
        Roots held;
        for (uint32_t i = 0; i < before.nodes[node].available; ++i) {
            virt::SelftestRootFactoryOptions const options{
                .force_ordinary_failure = true, .use_node_override = true, .node_override = node};
            virt::SelftestRootFactoryOutput output{};
            held.values[i] = virt::selftest_create_pagemap(options, output);
            KREQUIRE_NE(held.values[i], nullptr);
            expect_loan(held.values[i], node, output);
            for (uint32_t j = 0; j < i; ++j) KEXPECT_NE(held.values[i], held.values[j]);
        }
        auto const empty = snapshot();
        KEXPECT_EQ(empty.nodes[node].available, 0U);
        Root refused;
        virt::SelftestRootFactoryOutput output{};
        KEXPECT_FALSE(refused.take(node, output));
        KEXPECT_EQ(output.reserve_error, reserve::Error::EMPTY);
        KEXPECT_FALSE(output.control_registered);
        for (numa::NodeId other = 0; other < before.node_count; ++other)
            if (other != node) {
                KEXPECT_EQ(empty.nodes[other].available, before.nodes[other].available);
                KEXPECT_EQ(empty.nodes[other].loaned, before.nodes[other].loaned);
            }
    }
    expect_same_debt(before, snapshot());
    log::info("actual reserve capacity=%u nodes=%u; NODE1 uses explicit request only, not scheduled remote execution", before.members,
              static_cast<unsigned>(before.node_count));
}

KTEST(NumaRootReserve, OrdinarySuccessPreservesReserveAndZeroedReuse) {
    auto const before = snapshot();
    Root ordinary;
    virt::SelftestRootFactoryOptions options{};
    virt::SelftestRootFactoryOutput output{};
    ordinary.value = virt::selftest_create_pagemap(options, output);
    KREQUIRE_NE(ordinary.value, nullptr);
    KEXPECT_TRUE(output.ordinary_allocator_called);
    KEXPECT_FALSE(output.ordinary_failure_injected);
    KEXPECT_FALSE(output.reserve_attempted);
    KEXPECT_TRUE(output.control_registered);
    ordinary.reset();
    expect_same_debt(before, snapshot());
    auto const node = first_available(before);
    if (node == numa::INVALID_NODE) {
        log::info("zeroed reserve reuse unexercised: actual capacity unavailable");
        return;
    }
    Root loan;
    KREQUIRE_TRUE(loan.take(node, output));
    auto* const old = loan.value;
    // Deliberately fake never-active entry, like the existing ordinary pool test.
    // It grants no live mapping and must be zeroed on return, not walked.
    loan.value->entries[7].frame = 0x12345;
    loan.value->entries[7].present = 1;
    loan.reset();
    KREQUIRE_TRUE(loan.take(node, output));
    KEXPECT_EQ(loan.value, old);
    expect_loan(loan.value, node, output);
}

KTEST(NumaRootReserve, ThousandRealFactoryRegistrationCyclesReturnDebt) {
    auto const initial = snapshot();
    auto const node = first_available(initial);
    if (node == numa::INVALID_NODE) {
        log::info("1000 real root cycles unexercised: actual capacity unavailable");
        return;
    }
    virt::SelftestRootFactoryOutput output{};
    {
        Root warm;
        KREQUIRE_TRUE(warm.take(node, output));
        expect_loan(warm.value, node, output);
    }
    auto const before = snapshot();
    auto const controls = control_snapshot();
    auto const pool = pool_snapshot();
    phys::PhysicalBalanceSnapshot physical_before{}, physical_after{};
    phys::get_physical_balance_snapshot(physical_before);
    uint64_t serial = controls.last_serial;
    for (uint32_t i = 0; i < 1000; ++i) {
        Root loan;
        KREQUIRE_TRUE(loan.take(node, output));
        expect_loan(loan.value, node, output);
        auto const current = virt::selftest_user_root_incarnation(loan.value);
        KEXPECT_TRUE(current > serial);
        serial = current;
    }
    auto const after = snapshot();
    expect_same_debt(before, after);
    expect_same_controls(controls, control_snapshot());
    expect_same_pool(pool, pool_snapshot());
    phys::get_physical_balance_snapshot(physical_after);
    for (auto owner :
         {mm::PhysicalPageOwner::PAGE_TABLE, mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE, mm::PhysicalPageOwner::USER_ROOT_CONTROL}) {
        auto const index = static_cast<size_t>(owner);
        KEXPECT_EQ(physical_after.owners[index].pages, physical_before.owners[index].pages);
        KEXPECT_EQ(physical_after.owners[index].objects, physical_before.owners[index].objects);
    }
    KEXPECT_EQ(physical_after.identity_mismatch_pages, 0U);
    log::info("1000 actual private root register/return cycles node=%u free_before=%llu free_after=%llu; not map/fork stress",
              static_cast<unsigned>(node), static_cast<unsigned long long>(physical_before.free_pages),
              static_cast<unsigned long long>(physical_after.free_pages));
}

KTEST(NumaRootReserve, InvalidOverrideCannotConsumeOrdinaryOrOtherReserve) {
    auto const before = snapshot();
    auto const pool = pool_snapshot();
    auto const controls = control_snapshot();
    Root invalid;
    virt::SelftestRootFactoryOutput output{};
    KEXPECT_FALSE(invalid.take(numa::INVALID_NODE, output));
    KEXPECT_EQ(output.reserve_error, reserve::Error::INVALID_NODE);
    KEXPECT_FALSE(output.ordinary_allocator_called);
    KEXPECT_FALSE(output.reserve_attempted);
    expect_same_debt(before, snapshot());
    expect_same_controls(controls, control_snapshot());
    expect_same_pool(pool, pool_snapshot());
}

KTEST(NumaRootReserve, ControlProvisioningFailureRequiresRealNeedChunk) {
    auto const initial = snapshot();
    auto const node = first_available(initial);
    if (node == numa::INVALID_NODE) {
        log::info("control rollback unexercised: reserve unavailable");
        return;
    }
    auto const start = control_snapshot();
    KREQUIRE_TRUE(start.control_pages <= UINT64_MAX / 96);
    auto const slots = start.control_pages * 96;
    KREQUIRE_TRUE(start.active_roots <= slots);
    auto const free = slots - start.active_roots;
    if (free > 96) {
        log::info("control rollback unexercised: actual free controls exceed bounded96 fillers");
        return;
    }
    Roots fillers;
    for (uint64_t i = 0; i < free; ++i) {
        virt::SelftestRootFactoryOptions const options{};
        virt::SelftestRootFactoryOutput output{};
        fillers.values[i] = virt::selftest_create_pagemap(options, output);
        KREQUIRE_NE(fillers.values[i], nullptr);
        if (output.reserve_attempted) {
            log::info("control rollback unexercised: ordinary filler needed emergency fallback");
            return;
        }
    }
    auto const before = snapshot();
    auto const controls = control_snapshot();
    auto const pool = pool_snapshot();
    Root failed;
    virt::SelftestRootFactoryOutput output{};
    auto const succeeded = failed.take(node, output, true);
    if (!output.control_failure_injected) {
        failed.reset();
        log::info("control rollback unconsumed: another actual admission supplied control capacity");
        return;
    }
    KEXPECT_FALSE(succeeded);
    KEXPECT_TRUE(output.need_chunk_seen);
    KEXPECT_FALSE(output.ordinary_allocator_called);
    KEXPECT_TRUE(output.reserve_attempted);
    KEXPECT_TRUE(output.rollback_returned);
    KEXPECT_FALSE(output.control_registered);
    expect_same_debt(before, snapshot());
    expect_same_controls(controls, control_snapshot());
    expect_same_pool(pool, pool_snapshot());
}

KTEST(NumaRootReserve, ActualRawAndBudgetDoneReturnBorrowedRoot) {
    auto const before = snapshot();
    auto const node = first_available(before);
    if (node == numa::INVALID_NODE) {
        log::info("raw/budget reserve return unexercised: actual capacity unavailable");
        return;
    }
    {
        Root raw;
        virt::SelftestRootFactoryOutput output{};
        KREQUIRE_TRUE(raw.take(node, output));
        KREQUIRE_TRUE(raw.map_one());
        raw.reset();
    }
    {
        Root budget;
        virt::SelftestRootFactoryOutput output{};
        KREQUIRE_TRUE(budget.take(node, output));
        KREQUIRE_TRUE(budget.map_one());
        Walk walk{virt::create_destroy_user_space_budget_state(budget.value)};
        KREQUIRE_NE(walk.value, nullptr);
        uint32_t rounds = 0;
        while (!virt::destroy_user_space_budgeted(walk.value, 64) && ++rounds < 10000) {
        }
        KREQUIRE_TRUE(rounds < 10000);
        KREQUIRE_TRUE(virt::finish_destroy_user_space_and_release_pagemap(walk.value));
        walk.value = nullptr;
        budget.value = nullptr;
        budget.mapped = false;
    }
    expect_same_debt(before, snapshot());
}

KTEST(NumaRootReserve, PartialWalkDisposalRetainsOriginalRootOwner) {
    auto const before = snapshot();
    auto const node = first_available(before);
    if (node == numa::INVALID_NODE) {
        log::info("partial reserve walk unexercised: actual capacity unavailable");
        return;
    }
    Root root;
    virt::SelftestRootFactoryOutput output{};
    KREQUIRE_TRUE(root.take(node, output));
    KREQUIRE_TRUE(root.map_one());
    auto const loaned = snapshot();
    auto const serial = virt::selftest_user_root_incarnation(root.value);
    {
        Walk walk{virt::create_destroy_user_space_budget_state(root.value)};
        KREQUIRE_NE(walk.value, nullptr);
        KEXPECT_FALSE(virt::destroy_user_space_budgeted(walk.value, 1));
    }
    expect_same_debt(loaned, snapshot());
    KEXPECT_EQ(virt::selftest_user_root_incarnation(root.value), serial);
    KEXPECT_EQ(phys::page_ref_get(root.value), 1U);
    root.reset();
    expect_same_debt(before, snapshot());
    log::info("partial/complete private-root closure only; remote delayed ACK and cross-CPU return unexercised in BSP KTEST");
}

#endif
