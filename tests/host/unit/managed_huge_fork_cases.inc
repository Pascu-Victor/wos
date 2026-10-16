// Existing C47 and C44 cases remain unchanged. These execute the new coordinator
// through real full-user-half surveys with explicit Host context/ref endpoints.
#include <cerrno>
namespace {
class ManagedFork : public ManagedRange {
   protected:
    mm::paging::PageTable* child{};
    void SetUp() override {
        ManagedRange::SetUp();
        child = table();
        fork_child = child;
        fork_eager = fork_mode = false;
        fork_exclusives = fork_reader_attempts = fork_cow_entries = fork_swap_clones = 0;
        fork_fail_table_at = SIZE_MAX;
        fork_table_attempts = 0;
        fork_events.clear();
    }
    void TearDown() override {
        fork_mode = false;
        fork_child = nullptr;
        ManagedRange::TearDown();
    }
    auto copy(const huge::RangeSplitSurvey& survey, huge::PreparedTables& tables, numa::AllocationContext* placement = nullptr,
              numa::AllocationResult* result = nullptr) -> int {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        HostIrqOff irq;
        fork_mode = true;
        auto error =
            virt::deep_copy_current_user_pagemap_prepared(actor, child, preparation, publication, survey, tables, placement, result);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_FALSE(host_range::exclusive);
        EXPECT_EQ(root_readers, 0U);
        fork_mode = false;
        return error;
    }
    auto full(huge::RangeSplitSurvey& survey, huge::PreparedTables& tables) -> huge::Error {
        return proof(0, uint64_t{1} << 47, survey, tables);
    }
    void ordinary(uint64_t va) {
        auto* cursor = runtime_root;
        for (int level = 4; level > 1; --level) {
            auto& entry = word(cursor, virt::index_of(va, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            cursor = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        auto pa = model.topology.memory[0].base + 4 * LARGE;
        auto* bytes = model.allocate(pa, 1, PAGE, Initialization::FULL_OVERWRITE);
        ASSERT_NE(bytes, nullptr);
        std::memset(bytes, 0x63, PAGE);
        word(cursor, virt::index_of(va, 1)) = pa | 7;
    }
};
TEST_F(ManagedFork, FullZeroProofCopiesWithOneExclusiveAndNoReader) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    ASSERT_TRUE(survey.complete());
    ASSERT_EQ(survey.count(), 0U);
    EXPECT_EQ(copy(survey, tables), 0);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(fork_cow_entries, 1U);
    EXPECT_EQ(fork_reader_attempts, 0U);
    EXPECT_EQ(actual_flushes, 1U);
}
TEST_F(ManagedFork, PartialAndUnpreparedZeroRefuseBeforeExclusiveOrChildWrites) {
    huge::RangeSplitSurvey empty, partial;
    huge::PreparedTables tables;
    EXPECT_EQ(copy(empty, tables), -EINVAL);
    ASSERT_EQ(proof(0, LARGE, partial, tables), huge::Error::None);
    EXPECT_EQ(copy(partial, tables), -EINVAL);
    EXPECT_EQ(fork_exclusives + fork_cow_entries + fork_table_attempts, 0U);
    EXPECT_EQ(word(child, 0), 0U);
}
TEST_F(ManagedFork, StaleSerialEpochAndSaturationRefuseBeforeCopier) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    ++runtime_serial;
    EXPECT_EQ(copy(survey, tables), -EAGAIN);
    --runtime_serial;
    ++virt::host_root_control.ps_geometry_epoch;
    EXPECT_EQ(copy(survey, tables), -EAGAIN);
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    EXPECT_EQ(copy(survey, tables), -EOVERFLOW);
    EXPECT_EQ(fork_cow_entries + fork_table_attempts, 0U);
    EXPECT_EQ(word(child, 0), 0U);
}
TEST_F(ManagedFork, ChangedCurrentContextReleasesLeaseAndRejectsBeforeOwners) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    actor.type = ker::mod::sched::task::TaskType::THREAD;
    EXPECT_EQ(copy(survey, tables), -EWOULDBLOCK);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    actor.usercopy_pagemap_closing = true;
    EXPECT_EQ(copy(survey, tables), -EAGAIN);
    actor.usercopy_pagemap_closing = false;
    EXPECT_EQ(fork_exclusives + fork_cow_entries, 0U);
}
TEST_F(ManagedFork, OccupiedChildAndInexactEagerPairRefuseWithoutMutation) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    word(child, 0) = mm::paging::PAGE_RESERVED;
    EXPECT_EQ(copy(survey, tables), -EINVAL);
    word(child, 0) = 0;
    numa::AllocationContext placement = context(numa::Mode::BIND, 2);
    numa::AllocationResult result;
    EXPECT_EQ(copy(survey, tables, &placement, &result), -EINVAL);
    fork_eager = true;
    EXPECT_EQ(copy(survey, tables, &placement, nullptr), -EINVAL);
    placement.policy = {0, numa::Mode::BIND};
    EXPECT_EQ(copy(survey, tables, &placement, &result), -EINVAL);
    EXPECT_EQ(fork_exclusives + fork_cow_entries, 0U);
}
TEST_F(ManagedFork, ManagedSplitAckAnd512TrackersPrecedeCowAndRefSharing) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    ASSERT_EQ(survey.count(), 1U);
    plan_at_flush = huge::detail::PreparationAccess::plan_storage(tables);
    ASSERT_EQ(copy(survey, tables), 0);
    plan_at_flush = nullptr;
    mapped[0] = false;
    cleanup[0].reset();
    tables.reset();
    EXPECT_EQ(tracked_pages, 512U);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(fork_reader_attempts, 0U);
    EXPECT_EQ(model.live_refs(), 1024U);
    auto ack = std::find(fork_events.begin(), fork_events.end(), "ACK");
    auto cow = std::find(fork_events.begin(), fork_events.end(), "COW");
    ASSERT_NE(ack, fork_events.end());
    ASSERT_NE(cow, fork_events.end());
    EXPECT_LT(ack, cow);
    // Teardown's explicit endpoint owns both PTE ref sets; relinquish the child
    // aliases without refdec here, then let the fixture release both actual refs.
    huge_check_free = false;
    for (auto& b : model.blocks)
        for (auto& ref : b.refs)
            if (ref == 2) {
                auto page = static_cast<size_t>(&ref - b.refs.data());
                EXPECT_EQ(mm::phys::page_ref_dec(static_cast<uint8_t*>(b.storage.get()) + page * PAGE), 1U);
            }
}
TEST_F(ManagedFork, CopierOomFlushesParentPrefixAndCallerOwnsChildRollback) {
    ordinary(TEST_VA);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    fork_fail_table_at = 2;
    EXPECT_EQ(copy(survey, tables), -ENOMEM);
    EXPECT_NE(word(child, virt::index_of(TEST_VA, 4)), 0U);
    EXPECT_EQ(actual_flushes, 1U);
    EXPECT_EQ(model.live_refs(), 1U);
    EXPECT_EQ(fork_exclusives, 1U);
}
TEST_F(ManagedFork, EagerCopiedPolicyAndFullOverwriteReachRealFactory) {
    ordinary(TEST_VA);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    fork_eager = true;
    numa::AllocationContext placement = context(numa::Mode::BIND, 2);
    numa::AllocationResult result;
    ASSERT_EQ(copy(survey, tables, &placement, &result), 0);
    EXPECT_TRUE(result.span.empty());
    EXPECT_EQ(placement.policy, (numa::Policy{2, numa::Mode::BIND}));
    EXPECT_EQ(placement.virtual_page_index, TEST_VA / PAGE);
    ASSERT_FALSE(model.calls.empty());
    EXPECT_EQ(model.calls.back().dispatch, Dispatch::RANGE);
    EXPECT_EQ(model.calls.back().initialization, Initialization::FULL_OVERWRITE);
    auto physical = slot_at(child, TEST_VA, 1) & virt::PTE_FRAME_MASK;
    EXPECT_EQ(physical, model.topology.memory[1].base);
    auto* bytes = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(physical));
    for (size_t i = 0; i < PAGE; ++i) ASSERT_EQ(bytes[i], 0x63);
}
TEST_F(ManagedFork, OneNodeLocalUsesLegacyHeldBranchWithoutReacquisition) {
    model.domains(1);
    ordinary(TEST_VA);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(full(survey, tables), huge::Error::None);
    fork_eager = true;
    numa::AllocationContext placement{};
    placement.local_node = 0;
    numa::AllocationResult result;
    ASSERT_EQ(copy(survey, tables, &placement, &result), 0);
    ASSERT_FALSE(model.calls.empty());
    EXPECT_EQ(model.calls.back().dispatch, Dispatch::RECLAIM_OVERWRITE);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_TRUE(result.span.empty());
}
TEST_F(ManagedFork, HeldWalkerRejectsDifferentOriginalOwnerBeforeWrites) {
    ordinary(TEST_VA);
    fork_mode = true;
    {
        virt::UserPagemapExclusiveScope exclusive(runtime_root);
        EXPECT_FALSE(virt::deep_copy_user_pagemap_cow_held(child, runtime_root, false, nullptr, nullptr, UINT64_MAX, exclusive, true));
    }
    fork_mode = false;
    EXPECT_EQ(fork_table_attempts, 0U);
}
}  // namespace
