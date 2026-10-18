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
    auto proof(uint64_t first, uint64_t end, huge::RangeSplitSurvey& survey, huge::PreparedTables& tables,
               huge::RangePrepareWitness* witness = nullptr) -> huge::Error {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        return huge::prepare_current_range_split(actor, preparation, first, end, survey, tables, witness);
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

TEST_F(ManagedRange, BlockingBorrowUsesRealGuardAfterOrdinaryAcquireBeforeLegacyInterval) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(0, LARGE, survey, tables), huge::Error::None);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    auto const before = host_range::blocking;
    {
        ASSERT_TRUE(host_range::interrupts);
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        ASSERT_TRUE(publication.acquired());
        EXPECT_EQ(host_range::blocking, before + 1);
        {
            ker::mod::sys::SyscallPreparationScope::LegacyInterval final_interval(preparation);
            ASSERT_FALSE(host_range::interrupts);
            huge::CurrentRangeSplitAdmission admission(actor, preparation, publication);
            ASSERT_TRUE(admission.acquired());
            EXPECT_FALSE(admission.allows(actor, 0, LARGE));
            ASSERT_EQ(admission.validate_split_and_arm(survey, tables), huge::Error::None);
            EXPECT_TRUE(admission.allows(actor, 0, LARGE));
            EXPECT_FALSE(admission.allows(actor, PAGE, LARGE));
        }
        EXPECT_TRUE(host_range::interrupts);
        EXPECT_TRUE(publication.acquired());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers, 0U);
    }
    EXPECT_FALSE(g_shared_vmem_publication_lock.is_locked());
}
TEST_F(ManagedRange, BlockingBorrowRechecksActualContextAndRejectsUnpreparedProof) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    huge::CurrentRangeSplitAdmission wrong_if(actor, preparation, publication);
    EXPECT_FALSE(wrong_if.acquired());
    ker::mod::sys::SyscallPreparationScope::LegacyInterval final_interval(preparation);
    huge::CurrentRangeSplitAdmission admission(actor, preparation, publication);
    ASSERT_TRUE(admission.acquired());
    EXPECT_EQ(admission.validate_split_and_arm(survey, tables), huge::Error::Invalid);
    EXPECT_FALSE(admission.allows(actor, 0, LARGE));
    actor.scheduler_transition_active.store(true);
    huge::CurrentRangeSplitAdmission transition(actor, preparation, publication);
    EXPECT_FALSE(transition.acquired());
    actor.scheduler_transition_active.store(false);
    backend().preempt_depth = 1;
    huge::CurrentRangeSplitAdmission preempted(actor, preparation, publication);
    EXPECT_FALSE(preempted.acquired());
    backend().preempt_depth = 0;
}
TEST_F(ManagedRange, BlockingBorrowCompletesActualNonemptySplitBeforeAuthority) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    ASSERT_EQ(proof(TEST_VA, TEST_VA + LARGE, survey, tables), huge::Error::None);
    ASSERT_EQ(survey.count(), 1U);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        ker::mod::sys::SyscallPreparationScope::LegacyInterval final_interval(preparation);
        huge::CurrentRangeSplitAdmission admission(actor, preparation, publication);
        ASSERT_TRUE(admission.acquired());
        ASSERT_EQ(admission.validate_split_and_arm(survey, tables), huge::Error::None);
        EXPECT_TRUE(admission.allows(actor, TEST_VA, TEST_VA + LARGE));
        EXPECT_GT(actual_flushes, 0U);
    }
    mapped[0] = false;
    cleanup[0].reset();
    tables.reset();
}

TEST_F(ManagedRange, BlockingOwnerIdentityIsEmptyExactAndClearedBeforeTryOwner) {
    using Guard = ker::syscall::vmem::SharedVmemPublicationGuard;
    using ker::syscall::vmem::blocking_publication_owner_matches;
    using ker::syscall::vmem::g_shared_vmem_publication_owner;
    static_assert(std::is_empty_v<Guard> && sizeof(Guard) == 1);
    static_assert(!std::is_copy_constructible_v<Guard> && !std::is_move_constructible_v<Guard>);
    EXPECT_FALSE(blocking_publication_owner_matches(nullptr));
    EXPECT_EQ(g_shared_vmem_publication_owner.load(), nullptr);
    alignas(Guard) unsigned char opaque_storage[sizeof(Guard)]{};
    auto* const other_identity = reinterpret_cast<const Guard*>(opaque_storage);
    // Pointer comparison only: no method, dereference, or admission borrows this storage.
    EXPECT_FALSE(blocking_publication_owner_matches(other_identity));
    {
        Guard owner;
        EXPECT_TRUE(owner.acquired());
        EXPECT_EQ(g_shared_vmem_publication_owner.load(), &owner);
        EXPECT_FALSE(blocking_publication_owner_matches(nullptr));
        EXPECT_FALSE(blocking_publication_owner_matches(other_identity));
        ker::syscall::vmem::SharedVmemPublicationTryGuard refused;
        EXPECT_FALSE(refused.acquired());
        EXPECT_TRUE(owner.acquired());
    }
    EXPECT_EQ(g_shared_vmem_publication_owner.load(), nullptr);
    EXPECT_FALSE(blocking_publication_owner_matches(other_identity));
    {
        ker::syscall::vmem::SharedVmemPublicationTryGuard try_owner;
        EXPECT_TRUE(try_owner.acquired());
        EXPECT_EQ(g_shared_vmem_publication_owner.load(), nullptr);
        EXPECT_FALSE(blocking_publication_owner_matches(other_identity));
    }
    EXPECT_EQ(g_shared_vmem_publication_owner.load(), nullptr);
}

// Refusal-only observations of the actual preparation bodies above. Host
// controls select real original failures; they grant no runtime root authority.
TEST_F(ManagedRange, WitnessEntryFailureAndSuccessNeverReuseAnEarlierFailure) {
    huge::RangePrepareWitness witness{};
    witness.valid = true;
    witness.site = huge::RangePrepareSite::PublicationBusy;
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    actor.type = ker::mod::sched::task::TaskType::THREAD;
    EXPECT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::Invalid);
    EXPECT_TRUE(witness.valid);
    EXPECT_EQ(witness.error, huge::Error::Invalid);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::Context);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Entry);
    EXPECT_EQ(witness.attempt, 0U);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    EXPECT_EQ(proof(1, LARGE, survey, tables, &witness), huge::Error::Invalid);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::Input);
    ASSERT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::None);
    EXPECT_FALSE(witness.valid);
    EXPECT_EQ(witness.error, huge::Error::None);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::Unreported);
}
TEST_F(ManagedRange, WitnessSeparatesPublicationLeaseReaderAndRootRefusals) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    huge::RangePrepareWitness witness{};
    {
        ker::syscall::vmem::SharedVmemPublicationTryGuard held;
        ASSERT_TRUE(held.acquired());
        EXPECT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::Again);
        EXPECT_EQ(witness.site, huge::RangePrepareSite::PublicationBusy);
        EXPECT_EQ(publication_depth, 1U);  // Only the independently held outer owner.
    }
    actor.usercopy_pagemap_closing = true;
    EXPECT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::Again);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::TaskLease);
    actor.usercopy_pagemap_closing = false;
    host_range::reader_busy = true;
    EXPECT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::Again);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::StripedReader);
    host_range::reader_busy = false;
    huge_root_live = false;
    EXPECT_EQ(proof(0, LARGE, survey, tables, &witness), huge::Error::Stale);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::RootStale);
    huge_root_live = true;
    EXPECT_TRUE(witness.valid);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Count);
    EXPECT_EQ(witness.attempt, 1U);
    EXPECT_FALSE(witness.exhausted);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(root_readers + publication_depth + cow_depth + pin_depth, 0U);
    EXPECT_EQ(host_range::yields + host_range::blocking, 0U);
}
TEST_F(ManagedRange, WitnessEpochRetryKeepsOnlyTheLastActualFailureOrClearsOnSuccess) {
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    huge::RangePrepareWitness witness{};
    unsigned changes = 0;
    range_yield_action = [&] {
        ++virt::host_root_control.ps_geometry_epoch;
        ++changes;
    };
    EXPECT_EQ(proof(0, uint64_t{1} << 47, survey, tables, &witness), huge::Error::Again);
    EXPECT_EQ(changes, 3U);
    EXPECT_TRUE(witness.valid);
    EXPECT_EQ(witness.error, huge::Error::Again);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::EpochChanged);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Count);
    EXPECT_EQ(witness.attempt, 3U);
    EXPECT_TRUE(witness.epoch_changed);
    EXPECT_TRUE(witness.exhausted);
    bool once = false;
    range_yield_action = [&] {
        if (!once) {
            once = true;
            ++virt::host_root_control.ps_geometry_epoch;
        }
    };
    ASSERT_EQ(proof(0, uint64_t{1} << 47, survey, tables, &witness), huge::Error::None);
    EXPECT_TRUE(once);
    EXPECT_FALSE(witness.valid);
    EXPECT_FALSE(witness.exhausted);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::Unreported);
}
TEST_F(ManagedRange, WitnessPlanReserveAndAllocationDoNotChangeOwnersOrErrors) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::PlanHandle held[huge::MAX_PLANS]{};
    size_t held_count = 0;
    for (; held_count < huge::MAX_PLANS; ++held_count) {
        auto const error = huge::detail::reserve_plan(1, held[held_count]);
        if (error != huge::Error::None) {
            EXPECT_EQ(error, huge::Error::Capacity);
            break;
        }
    }
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    huge::RangePrepareWitness witness{};
    EXPECT_EQ(proof(TEST_VA, TEST_VA + LARGE, survey, tables, &witness), huge::Error::Capacity);
    EXPECT_TRUE(witness.valid);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::PlanReserve);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Tables);
    EXPECT_EQ(witness.count, 1U);
    EXPECT_EQ(witness.expected_count, 1U);
    for (size_t i = 0; i < held_count; ++i) EXPECT_EQ(huge::detail::release_plan(held[i]), huge::Error::None);
    auto const original = slot_at(runtime_root, TEST_VA, 2);
    for (bool heap : {false, true}) {
        table_attempt = 0;
        fail_table_at = heap ? SIZE_MAX : 0;
        fail_plan_heap = heap;
        EXPECT_EQ(proof(TEST_VA, TEST_VA + LARGE, survey, tables, &witness), huge::Error::Exhausted);
        EXPECT_EQ(witness.error, huge::Error::Exhausted);
        EXPECT_EQ(witness.site, huge::RangePrepareSite::PlanAllocation);
        EXPECT_EQ(witness.pass, huge::RangePreparePass::Tables);
        EXPECT_TRUE(tables.empty());
        EXPECT_FALSE(survey.complete());
        EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2), original);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    }
    fail_table_at = SIZE_MAX;
}
TEST_F(ManagedRange, WitnessFinalCountMismatchIsNotAnEpochOrPublicationClaim) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    huge::RangePrepareWitness witness{};
    auto const original = slot_at(runtime_root, TEST_VA, 2);
    auto const before = table_attempt;
    bool removed = false;
    // Deliberately corrupt only this finite Host image without its normal
    // epoch update, after Fill has counted its first leaf. Verify must notice.
    range_yield_action = [&] {
        if (!removed && table_attempt > before) {
            removed = true;
            slot_at(runtime_root, TEST_VA, 2) = 0;
        }
    };
    EXPECT_EQ(proof(TEST_VA, TEST_VA + 65 * LARGE, survey, tables, &witness), huge::Error::Again);
    slot_at(runtime_root, TEST_VA, 2) = original;
    EXPECT_TRUE(removed);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::CountMismatch);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Verify);
    EXPECT_EQ(witness.count, 0U);
    EXPECT_EQ(witness.expected_count, 1U);
    EXPECT_FALSE(witness.epoch_changed);
    EXPECT_FALSE(witness.exhausted);
    EXPECT_TRUE(tables.empty());
}
TEST_F(ManagedRange, WitnessDescriptorMismatchComesFromActualFinalSurvey) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables tables;
    huge::RangePrepareWitness witness{};
    bool altered = false;
    range_yield_action = [&] {
        auto* storage = huge::detail::PreparationAccess::plan_storage(tables);
        if (!altered && storage != nullptr && storage->descriptors[0].handle.generation != 0) {
            altered = true;
            storage->descriptors[0].semantic_word ^= mm::paging::PAGE_WRITE;
        }
    };
    EXPECT_EQ(proof(TEST_VA, TEST_VA + 65 * LARGE, survey, tables, &witness), huge::Error::Again);
    EXPECT_TRUE(altered);
    EXPECT_EQ(witness.site, huge::RangePrepareSite::DescriptorMismatch);
    EXPECT_EQ(witness.pass, huge::RangePreparePass::Verify);
    EXPECT_FALSE(witness.epoch_changed);
    EXPECT_FALSE(witness.exhausted);
    EXPECT_TRUE(tables.empty());
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
}
