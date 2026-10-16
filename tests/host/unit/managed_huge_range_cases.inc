// Actual range preparation/admission against explicit Host lifetime/IRQ ports.
// These ports do not execute a WOS task, scheduler, TLB shootdown or CPL3.
#include <functional>
#include <thread>
#include <type_traits>
namespace {
std::function<void()> range_yield_action;
struct HostIrqOff {
    bool old{host_range::interrupts};
    HostIrqOff() { host_range::interrupts = false; }
    ~HostIrqOff() { host_range::interrupts = old; }
};
class ManagedRange : public ManagedResolver {
   protected:
    ker::mod::sched::task::Task actor;
    void SetUp() override {
        ManagedResolver::SetUp();
        actor.pagemap = runtime_root;
        host_range::current = &actor;
        range_yield_action = {};
        host_range::on_yield = [] {
            if (range_yield_action) range_yield_action();
        };
    }
    void TearDown() override {
        host_range::on_yield = nullptr;
        range_yield_action = {};
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        host_range::reader_busy = false;
        host_range::current = nullptr;
        host_range::interrupts = true;
        ManagedResolver::TearDown();
    }
    auto proof(uint64_t first, uint64_t end, huge::RangeSplitSurvey& survey, huge::PreparedTables& tables) -> huge::Error {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        return huge::prepare_current_range_split(actor, preparation, first, end, survey, tables);
    }
    auto arm(const huge::RangeSplitSurvey& survey, huge::PreparedTables& tables, bool expect_allow = true) -> huge::Error {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        HostIrqOff irq;
        ker::syscall::vmem::SharedVmemPublicationTryGuard publication;
        huge::CurrentRangeSplitAdmission admission(actor, preparation, publication);
        EXPECT_TRUE(admission.acquired());
        EXPECT_FALSE(admission.allows(actor, survey.start(), survey.end()));
        auto error = admission.validate_split_and_arm(survey, tables);
        EXPECT_EQ(admission.allows(actor, survey.start(), survey.end()), expect_allow && error == huge::Error::None);
        EXPECT_FALSE(admission.allows(actor, survey.start() + PAGE, survey.end()));
        return error;
    }
    auto table() -> mm::paging::PageTable* {
        auto* p = static_cast<mm::paging::PageTable*>(std::aligned_alloc(PAGE, PAGE));
        if (!p) std::abort();
        std::memset(p, 0, PAGE);
        table_live.push_back(p);
        return p;
    }
    void ordinary_path(uint64_t va) {
        auto* p = runtime_root;
        for (int level = 4; level > 2; --level) {
            auto& entry = word(p, virt::index_of(va, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            p = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        // The chunk walker must not follow a PML2 non-PS frame into PML1.
        word(p, virt::index_of(va, 2)) = 0x4007;
    }
};
TEST_F(ManagedRange, FullEmptyLowerHalfHasIndependentZeroProofAndLexicalAdmission) {
    huge_core = {};  // Complete zero proof does not initialize or require managed caps.
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, uint64_t{1} << 47, survey, tables), huge::Error::None);
    EXPECT_TRUE(survey.complete());
    EXPECT_EQ(survey.count(), 0U);
    EXPECT_TRUE(tables.empty());
    EXPECT_EQ(huge::detail::PreparationAccess::plan_storage(tables), nullptr);
    EXPECT_EQ(table_attempt, 0U);
    EXPECT_GT(host_range::yields, 0U);
    EXPECT_EQ(host_range::blocking, 0U);
    EXPECT_EQ(arm(survey, tables), huge::Error::None);
    EXPECT_EQ(actual_flushes, 0U);
    huge::Snapshot unready{};
    huge_core.snapshot(unready);
    EXPECT_FALSE(unready.ready);
}
TEST_F(ManagedRange, InvalidGeometryAndUnpreparedZeroCannotArm) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    EXPECT_EQ(proof(1, LARGE, survey, tables), huge::Error::Invalid);
    EXPECT_EQ(proof(0, (uint64_t{1} << 47) + PAGE, survey, tables), huge::Error::Invalid);
    EXPECT_EQ(proof(LARGE, LARGE, survey, tables), huge::Error::Invalid);
    EXPECT_EQ(arm(survey, tables, false), huge::Error::Invalid);
    EXPECT_EQ(table_attempt, 0U);
}
TEST_F(ManagedRange, BusyPublicationIsImmediateWithoutTaskLeaseOrYield) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ker::syscall::vmem::SharedVmemPublicationTryGuard held;
    ASSERT_TRUE(held.acquired());
    EXPECT_EQ(proof(0, uint64_t{1} << 47, survey, tables), huge::Error::Again);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(host_range::yields, 0U);
    EXPECT_EQ(table_attempt, 0U);
    EXPECT_EQ(host_range::blocking, 0U);
}
TEST_F(ManagedRange, BusyReaderReleasesTheAcquiredTaskLeaseImmediately) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    host_range::reader_busy = true;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Again);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_EQ(host_range::yields, 0U);
    EXPECT_EQ(host_range::blocking, 0U);
}
TEST_F(ManagedRange, DenseFourKAndSparseHolesDoNotGainAnOldVirtualSpanCap) {
    for (uint64_t i = 0; i < 96; ++i) ordinary_path(TEST_VA + i * LARGE);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(PAGE, (uint64_t{1} << 47) - PAGE, survey, tables), huge::Error::None);
    EXPECT_EQ(survey.count(), 0U);
    EXPECT_GT(host_range::yields, 10U);
    EXPECT_EQ(table_attempt, 0U);
    EXPECT_EQ(arm(survey, tables), huge::Error::None);
}
TEST_F(ManagedRange, UnknownPsOneGigabyteAndMalformedUpperRefuseBeforeOwnership) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    word(runtime_root, 0) = 0x40000087;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Unsupported);
    word(runtime_root, 0) = 0;
    ordinary_path(0);
    auto saved = slot_at(runtime_root, 0, 3);
    slot_at(runtime_root, 0, 3) = 0x40000087;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Unsupported);
    slot_at(runtime_root, 0, 3) = saved;
    slot_at(runtime_root, 0, 2) = 0x200087;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Unsupported);
    slot_at(runtime_root, 0, 2) = 0;
    word(runtime_root, 0) = 512;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Unsupported);
    word(runtime_root, 0) = 0;
    EXPECT_EQ(table_attempt, 0U);
    EXPECT_TRUE(tables.empty());
}
TEST_F(ManagedRange, PresentSupervisorOnlyLeafStillBelongsToTheCompleteGeometry) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0, 3 | mm::paging::PAGE_NX), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(TEST_VA + PAGE, TEST_VA + 65 * LARGE, survey, tables), huge::Error::None);
    EXPECT_EQ(survey.count(), 1U);
    plan_at_flush = huge::detail::PreparationAccess::plan_storage(tables);
    EXPECT_EQ(arm(survey, tables), huge::Error::None);
    plan_at_flush = nullptr;
    mapped[0] = false;
    cleanup[0].reset();
    tables.reset();
    EXPECT_EQ(model.live_refs(), 512U);
    EXPECT_EQ(tracked_pages, 512U);
}
TEST_F(ManagedRange, EpochChangeBetweenChunksRestartsWholeProofAndStopsAfterThree) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    unsigned changes = 0;
    range_yield_action = [&] {
        ++virt::host_root_control.ps_geometry_epoch;
        ++changes;
    };
    EXPECT_EQ(proof(0, uint64_t{1} << 47, survey, tables), huge::Error::Again);
    EXPECT_EQ(changes, 3U);
    EXPECT_FALSE(survey.complete());
    EXPECT_TRUE(tables.empty());
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
}
TEST_F(ManagedRange, HideRevealAndZeroThenPublicationCannotReuseAnOldProof) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, uint64_t{1} << 47, survey, tables), huge::Error::None);
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    EXPECT_EQ(arm(survey, tables, false), huge::Error::Again);
    auto& pml4 = word(runtime_root, virt::index_of(TEST_VA, 4));
    auto old = pml4;
    pml4 = 0;
    virt::advance_ps_geometry_epoch_locked(runtime_root);
    pml4 = old;
    virt::advance_ps_geometry_epoch_locked(runtime_root);
    EXPECT_EQ(arm(survey, tables, false), huge::Error::Again);
}
TEST_F(ManagedRange, RootAbaAndSaturationNeverYieldRangeAuthority) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, LARGE, survey, tables), huge::Error::None);
    ++runtime_serial;
    EXPECT_EQ(arm(survey, tables, false), huge::Error::Again);
    --runtime_serial;
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    EXPECT_EQ(arm(survey, tables, false), huge::Error::GenerationExhausted);
    huge::RangeSplitSurvey fresh;
    EXPECT_EQ(proof(0, LARGE, fresh, tables), huge::Error::GenerationExhausted);
}
TEST_F(ManagedRange, ExactCleanupWorksAfterSecondPublicationAndStickySaturationButCannotArm) {
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(prepare(i), huge::Error::None);
        ASSERT_EQ(publish(i), huge::Error::None);
    }
    huge::RangeSplitSurvey actual_one_leaf;
    huge::PreparedTables genuine_split;
    ASSERT_EQ(proof(TEST_VA, TEST_VA + LARGE, actual_one_leaf, genuine_split), huge::Error::None);
    ASSERT_EQ(actual_one_leaf.count(), 1U);
    // Even a fresh valid one-leaf range proof cannot turn Cleanup into metadata authority.
    EXPECT_EQ(arm(actual_one_leaf, cleanup[0], false), huge::Error::Invalid);
    genuine_split.reset();
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    huge::RangeSplitSurvey survey;
    huge::PreparedTables empty;
    EXPECT_EQ(arm(survey, cleanup[0], false), huge::Error::Invalid);
    ASSERT_EQ(split(cleanup[0]), huge::Error::None);
    mapped[0] = false;
    ASSERT_EQ(split(cleanup[1]), huge::Error::None);
    mapped[1] = false;
    EXPECT_EQ(virt::host_root_control.ps_geometry_epoch, UINT64_MAX);
    EXPECT_EQ(tracked_pages, 1024U);
    EXPECT_EQ(model.live_refs(), 1024U);
}
TEST_F(ManagedRange, HeldOriginalExclusiveCommitsWithoutAcquiringAReader) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(TEST_VA, TEST_VA + LARGE, survey, tables), huge::Error::None);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        virt::UserPagemapExclusiveScope exclusive(runtime_root);
        ASSERT_TRUE(exclusive.owns(runtime_root));
        EXPECT_EQ(virt::commit_managed_split_plan_exclusive(runtime_root, survey, tables, exclusive), huge::Error::None);
        EXPECT_EQ(root_readers, 0U);
    }
    mapped[0] = false;
    cleanup[0].reset();
    tables.reset();
}
TEST_F(ManagedRange, CurrentContextMustBeTheActualActiveProcessBeforeAnyOwner) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    actor.type = ker::mod::sched::task::TaskType::THREAD;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Invalid);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    actor.exit_in_progress = true;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Invalid);
    actor.exit_in_progress = false;
    host_range::queryable = false;
    EXPECT_EQ(proof(0, LARGE, survey, tables), huge::Error::Invalid);
    host_range::queryable = true;
    EXPECT_EQ(table_attempt, 0U);
}
TEST_F(ManagedRange, RealTryGuardContentionNestedFailureAndUnlockOwnership) {
    auto attempts = host_range::tries, unlocks = host_range::unlocks;
    {
        ker::syscall::vmem::SharedVmemPublicationTryGuard first;
        ASSERT_TRUE(first.acquired());
        {
            ker::syscall::vmem::SharedVmemPublicationTryGuard second;
            EXPECT_FALSE(second.acquired());
        }
        EXPECT_EQ(publication_depth, 1U);
        EXPECT_EQ(host_range::unlocks, unlocks);
    }
    EXPECT_EQ(host_range::tries, attempts + 1);
    EXPECT_EQ(host_range::unlocks, unlocks + 1);
    ker::syscall::vmem::SharedVmemPublicationTryGuard again;
    EXPECT_TRUE(again.acquired());
}
TEST_F(ManagedRange, EveryTableOomPrefixReturnsActualOwnersAndCapacity) {
    for (size_t fail = 0; fail < 64; ++fail) {
        table_attempt = table_freed = 0;
        fail_table_at = fail;
        huge::PreparedTables plan;
        EXPECT_EQ(plan.prepare(64), huge::Error::Exhausted);
        EXPECT_TRUE(plan.empty());
        EXPECT_EQ(table_freed, fail);
        EXPECT_TRUE(table_live.empty());
        huge::Snapshot snap{};
        huge_core.snapshot(snap);
        EXPECT_EQ(snap.plans, 0U);
        EXPECT_EQ(snap.table_pages, 0U);
    }
    fail_table_at = SIZE_MAX;
}
TEST_F(ManagedRange, PreparationOomHasNoMappingOrProofSideEffect) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    auto before = slot_at(runtime_root, TEST_VA, 2);
    for (bool heap : {false, true}) {
        huge::RangeSplitSurvey survey;
        huge::PreparedTables tables;
        table_attempt = 0;
        fail_table_at = heap ? SIZE_MAX : 0;
        fail_plan_heap = heap;
        EXPECT_EQ(proof(TEST_VA, TEST_VA + LARGE, survey, tables), huge::Error::Exhausted);
        EXPECT_FALSE(survey.complete());
        EXPECT_TRUE(tables.empty());
        EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2), before);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    }
    fail_table_at = SIZE_MAX;
}
TEST_F(ManagedRange, IndependentFinalZeroPassDetectsPublicationAndRebuildsAllDescriptors) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    unsigned visits = 0;
    range_yield_action = [&] {
        if (++visits == 31) EXPECT_EQ(publish(0), huge::Error::None);
    };
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, uint64_t{1} << 47, survey, tables), huge::Error::None);
    EXPECT_GT(visits, 31U);
    EXPECT_EQ(survey.count(), 1U);
    EXPECT_EQ(arm(survey, tables), huge::Error::None);
    mapped[0] = false;
    cleanup[0].reset();
    tables.reset();
}
TEST_F(ManagedRange, AdmissionRequiresActualIfOffAndExactPublicationLifetime) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, LARGE, survey, tables), huge::Error::None);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationTryGuard publication;
    huge::CurrentRangeSplitAdmission wrong_if(actor, preparation, publication);
    EXPECT_FALSE(wrong_if.acquired());
    HostIrqOff irq;
    huge::CurrentRangeSplitAdmission admitted(actor, preparation, publication);
    ASSERT_TRUE(admitted.acquired());
    EXPECT_EQ(admitted.validate_split_and_arm(survey, tables), huge::Error::None);
    EXPECT_TRUE(admitted.allows(actor, 0, LARGE));
    host_range::interrupts = true;
    EXPECT_FALSE(admitted.allows(actor, 0, LARGE));
    host_range::interrupts = false;
    EXPECT_EQ(admitted.validate_split_and_arm(survey, tables), huge::Error::Invalid);
}
TEST_F(ManagedRange, RealTryGuardRepeatedAndFourThreadCasOwnersAllClose) {
    using Guard = ker::syscall::vmem::SharedVmemPublicationTryGuard;
    static_assert(!std::is_copy_constructible_v<Guard> && !std::is_move_constructible_v<Guard>);
    static_assert(!std::is_copy_assignable_v<Guard> && !std::is_move_assignable_v<Guard>);
    static_assert(std::is_nothrow_constructible_v<Guard> && std::is_nothrow_destructible_v<Guard>);
    auto const tries = host_range::tries, unlocks = host_range::unlocks;
    for (unsigned i = 0; i < 1000; ++i) {
        Guard owner;
        EXPECT_TRUE(owner.acquired());
    }
    EXPECT_EQ(host_range::tries, tries + 1000);
    EXPECT_EQ(host_range::unlocks, unlocks + 1000);
    EXPECT_FALSE(g_shared_vmem_publication_lock.is_locked());
    host_range::records = host_range::clears = host_range::active = 0;
    host_range::concurrent = true;
    std::atomic<unsigned> successes{};
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 4; ++t)
        threads.emplace_back([&] {
            for (unsigned i = 0; i < 1000; ++i) {
                Guard owner;
                if (owner.acquired()) {
                    EXPECT_EQ(host_range::active.load(), 1U);
                    std::this_thread::yield();
                    ++successes;
                }
            }
        });
    for (auto& thread : threads) thread.join();
    host_range::concurrent = false;
    EXPECT_GT(successes.load(), 0U);
    EXPECT_EQ(host_range::records.load(), successes.load());
    EXPECT_EQ(host_range::clears.load(), successes.load());
    EXPECT_EQ(host_range::active.load(), 0U);
    EXPECT_FALSE(g_shared_vmem_publication_lock.is_locked());
    EXPECT_EQ(host_range::blocking, 0U);
}
}  // namespace
