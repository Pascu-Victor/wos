#include <gtest/gtest.h>

#include "ancestor_publication_ports.inc"

namespace {
namespace virt = ker::mod::mm::virt;
namespace paging = ker::mod::mm::paging;
using ancestor_host::State;
constexpr size_t SLOT = 19, DESCENDANT = 37;
constexpr uint64_t RIVAL_FLAGS = paging::PAGE_PRESENT | paging::PAGE_NX;
constexpr uint64_t REQUEST = paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER;

void install_rival_from_allocator(State& s) {
    s.on_allocation = [&s] {
        s.rival = virt::advance_page_table(s.root, SLOT, RIVAL_FLAGS);
        s.descendant = virt::advance_page_table(s.rival, DESCENDANT, paging::PAGE_PRESENT | paging::PAGE_WRITE);
    };
}

TEST(AncestorPublication, OrdinaryAllocatorRivalIsPreservedAndLoserReturned) {
    State s(false, true);
    install_rival_from_allocator(s);
    bool promoted = false;
    auto* selected = virt::advance_page_table(s.root, SLOT, REQUEST, &promoted);
    std::printf("ANCESTOR_INTERLEAVING selected=%llu rival=%llu spare=%llu pool=%u free=%u\n",
                static_cast<unsigned long long>(s.physical(selected)), static_cast<unsigned long long>(s.physical(s.rival)),
                static_cast<unsigned long long>(s.physical(s.outer_spare)), s.pool_calls, s.free_calls);
    EXPECT_EQ(selected, s.rival) << "outer allocation must not overwrite the actual nested publisher";
    EXPECT_EQ(s.root->entries[SLOT].frame << paging::PAGE_SHIFT, s.physical(s.rival));
    EXPECT_EQ(s.rival->entries[DESCENDANT].frame << paging::PAGE_SHIFT, s.physical(s.descendant));
    EXPECT_TRUE(s.record(s.outer_spare).pooled) << "unused outer allocation returns to the real pool endpoint";
    EXPECT_EQ(s.pool_calls, 1U);
    EXPECT_EQ(s.free_calls, 0U);
    EXPECT_TRUE(promoted);
    EXPECT_EQ(s.flushes, 1U);
    EXPECT_EQ(s.allocations, 3U);
    EXPECT_EQ(s.topology_calls, 0U);
    EXPECT_EQ(s.policy_calls, 0U);
    EXPECT_EQ(s.violations, 0U);
    EXPECT_EQ(s.pin_depth, 0U);
    EXPECT_TRUE(s.irq);
}
TEST(AncestorPublication, RivalPublicationAndLoserReturnCoverBothModesAndIrqStates) {
    for (bool typed : {false, true})
        for (bool irq : {false, true})
            for (bool pooled : {false, true}) {
                SCOPED_TRACE(::testing::Message() << "typed=" << typed << " irq=" << irq << " pool=" << pooled);
                State s(typed, irq);
                s.pool_accept = pooled;
                install_rival_from_allocator(s);
                bool promoted = false;
                auto* selected = virt::advance_page_table(s.root, SLOT, REQUEST, &promoted);
                EXPECT_EQ(selected, s.rival);
                EXPECT_EQ(s.root->entries[SLOT].frame << paging::PAGE_SHIFT, s.physical(s.rival));
                EXPECT_EQ(s.rival->entries[DESCENDANT].frame << paging::PAGE_SHIFT, s.physical(s.descendant));
                EXPECT_EQ(s.record(s.outer_spare).pooled, pooled);
                EXPECT_EQ(s.record(s.outer_spare).freed, !pooled);
                EXPECT_EQ(s.pool_calls, 1U);
                EXPECT_EQ(s.free_calls, pooled ? 0U : 1U);
                EXPECT_EQ(s.allocations, 3U);
                EXPECT_TRUE(promoted);
                EXPECT_EQ(s.flushes, 1U);
                EXPECT_EQ(s.topology_calls, typed ? 3U : 0U);
                EXPECT_EQ(s.topology_while_untyped, 0U);
                EXPECT_EQ(s.topology_outside_pin, 0U);
                EXPECT_EQ(s.outside_child_reads, 0U);
                EXPECT_EQ(s.outside_publish_addresses, 0U);
                EXPECT_EQ(s.violations, 0U);
                EXPECT_EQ(s.pin_depth, 0U);
                EXPECT_EQ(s.irq, irq);
            }
}

TEST(AncestorPublication, ExistingAncestorMergesOnlyRequestedPermissions) {
    for (bool typed : {false, true})
        for (bool irq : {false, true})
            for (unsigned permissions = 0; permissions < 8; ++permissions) {
                SCOPED_TRACE(::testing::Message() << "typed=" << typed << " irq=" << irq << " permissions=" << permissions);
                State s(typed, irq);
                auto* child = s.make_table();
                auto entry = paging::create_page_table_entry(
                    s.physical(child), paging::PAGE_PRESENT | paging::PAGE_NX | paging::PAGE_PWT | paging::PAGE_PCD | paging::PAGE_COW);
                entry.accessed = entry.dirty = entry.global = 1;
                s.root->entries[SLOT] = entry;
                uint64_t const before = ancestor_host::entry_raw(entry);
                uint64_t const flags = paging::PAGE_PRESENT | ((permissions & 1) ? paging::PAGE_WRITE : 0) |
                                       ((permissions & 2) ? paging::PAGE_USER : 0) | ((permissions & 4) ? 0 : paging::PAGE_NX);
                uint64_t expected = before | (flags & (paging::PAGE_WRITE | paging::PAGE_USER));
                if ((flags & paging::PAGE_NX) == 0) expected &= ~paging::PAGE_NX;
                bool promoted = false;
                auto* selected = virt::advance_page_table(s.root, SLOT, flags, &promoted);
                EXPECT_EQ(selected, child);
                EXPECT_EQ(ancestor_host::entry_raw(s.root->entries[SLOT]), expected);
                EXPECT_EQ(promoted, expected != before);
                EXPECT_EQ(s.flushes, expected != before ? 1U : 0U);
                EXPECT_EQ(s.topology_calls, typed && expected != before ? 1U : 0U);
                EXPECT_EQ(s.allocations, 0U);
                EXPECT_EQ(s.pool_calls, 0U);
                EXPECT_EQ(s.free_calls, 0U);
                EXPECT_EQ(s.child_reads, 1U);
                EXPECT_EQ(s.outside_child_reads, 0U);
                EXPECT_EQ(s.violations, 0U);
                EXPECT_EQ(s.pin_depth, 0U);
                EXPECT_EQ(s.irq, irq);
            }
}

TEST(AncestorPublication, NoopAndNullPromotionPointerPreserveExistingChild) {
    for (bool typed : {false, true})
        for (bool irq : {false, true}) {
            State s(typed, irq);
            auto* child = s.make_table();
            s.root->entries[SLOT] = paging::create_page_table_entry(s.physical(child), REQUEST);
            auto const before = ancestor_host::entry_raw(s.root->entries[SLOT]);
            bool promoted = true;
            EXPECT_EQ(virt::advance_page_table(s.root, SLOT, REQUEST | paging::PAGE_NX, &promoted), child);
            EXPECT_TRUE(promoted);
            EXPECT_EQ(ancestor_host::entry_raw(s.root->entries[SLOT]), before);
            EXPECT_EQ(s.flushes, 0U);
            EXPECT_EQ(s.topology_calls, 0U);
            s.root->entries[SLOT] = paging::create_page_table_entry(s.physical(child), RIVAL_FLAGS);
            EXPECT_EQ(virt::advance_page_table(s.root, SLOT, REQUEST, nullptr), child);
            EXPECT_EQ(s.flushes, 1U);
            EXPECT_EQ(s.topology_calls, typed ? 1U : 0U);
            EXPECT_EQ(s.allocations, 0U);
            EXPECT_EQ(s.outside_child_reads, 0U);
            EXPECT_EQ(s.violations, 0U);
            EXPECT_EQ(s.irq, irq);
        }
}

TEST(AncestorPublication, SelectedChildObservationIsInsidePinInOrdinaryMode) {
    State s(false, true);
    auto* child = virt::advance_page_table(s.root, SLOT, RIVAL_FLAGS);
    EXPECT_EQ(s.allocations, 1U);
    EXPECT_EQ(s.publish_addresses, 1U);
    EXPECT_EQ(s.outside_publish_addresses, 0U);
    bool promoted = false;
    EXPECT_EQ(virt::advance_page_table(s.root, SLOT, RIVAL_FLAGS, &promoted), child);
    EXPECT_FALSE(promoted);
    EXPECT_EQ(s.allocations, 1U);
    EXPECT_EQ(s.child_reads, 1U);
    EXPECT_EQ(s.outside_child_reads, 0U);
    EXPECT_EQ(s.policy_calls, 0U);
    EXPECT_EQ(s.topology_calls, 0U);
    EXPECT_EQ(s.flushes, 0U);
    EXPECT_EQ(s.violations, 0U);
}

TEST(AncestorPublication, TypedHistoryTracksOnlyActualTypedEffects) {
    for (bool typed : {false, true}) {
        State s(typed, true);
        auto* child = virt::advance_page_table(s.root, SLOT, RIVAL_FLAGS);
        EXPECT_EQ(s.topology_calls, typed ? 1U : 0U);
        EXPECT_EQ(virt::advance_page_table(s.root, SLOT, RIVAL_FLAGS), child);
        EXPECT_EQ(s.topology_calls, typed ? 1U : 0U);
        EXPECT_EQ(virt::advance_page_table(s.root, SLOT, REQUEST), child);
        EXPECT_EQ(s.topology_calls, typed ? 2U : 0U);
        EXPECT_EQ(virt::advance_page_table(s.root, SLOT, REQUEST), child);
        EXPECT_EQ(s.topology_calls, typed ? 2U : 0U);
        EXPECT_EQ(s.topology_while_untyped, 0U);
        EXPECT_EQ(s.topology_outside_pin, 0U);
        EXPECT_EQ(s.flushes, 1U);
        EXPECT_EQ(s.allocations, 1U);
        EXPECT_EQ(s.violations, 0U);
        if (!typed) EXPECT_EQ(s.policy_calls, 0U);
    }
}

TEST(AncestorPublication, OomRestoresPinAndOriginalIrqBeforeTerminalHalt) {
    for (bool typed : {false, true})
        for (bool irq : {false, true}) {
            State s(typed, irq);
            s.fail_allocation = true;
            EXPECT_THROW((void)virt::advance_page_table(s.root, SLOT, REQUEST), ancestor_host::Halt);
            EXPECT_EQ(s.pin_depth, 0U);
            EXPECT_EQ(s.irq, irq);
            EXPECT_EQ(s.allocations, 1U);
            EXPECT_EQ(s.topology_calls, 0U);
            EXPECT_EQ(s.flushes, 0U);
            EXPECT_EQ(s.pool_calls, 0U);
            EXPECT_EQ(s.free_calls, 0U);
            EXPECT_EQ(s.violations, 0U);
        }
}

TEST(AncestorPublication, LargeAncestorRefusalRestoresPinAndIrqBeforePanic) {
    for (bool typed : {false, true})
        for (bool irq : {false, true}) {
            State s(typed, irq);
            auto* child = s.make_table();
            s.root->entries[SLOT] = paging::create_page_table_entry(s.physical(child), REQUEST);
            s.root->entries[SLOT].pagesize = 1;
            auto const before = ancestor_host::entry_raw(s.root->entries[SLOT]);
            EXPECT_THROW((void)virt::advance_page_table(s.root, SLOT, REQUEST), ancestor_host::Panic);
            EXPECT_EQ(ancestor_host::entry_raw(s.root->entries[SLOT]), before);
            EXPECT_EQ(s.pin_depth, 0U);
            EXPECT_EQ(s.irq, irq);
            EXPECT_EQ(s.allocations, 0U);
            EXPECT_EQ(s.topology_calls, 0U);
            EXPECT_EQ(s.flushes, 0U);
            EXPECT_EQ(s.violations, 0U);
        }
}
}  // namespace
