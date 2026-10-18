// Finite current/context/root/ACK endpoints. Real Task lease/PUB/admission and
// old batch owner/mapper bodies execute; this is not scheduled WOS authority.
namespace host_current_map {
void entered() {
    if (!enabled) return;
    ++entries;
    ASSERT_NE(actor, nullptr);
    EXPECT_EQ(actor->usercopy_pagemap_accesses.load(), 1U);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_EQ(publication_depth, 1U);
}
void ack() {
    if (!enabled) return;
    ++acks;
    ASSERT_NE(actor, nullptr);
    EXPECT_EQ(actor->usercopy_pagemap_accesses.load(), 1U);
    EXPECT_EQ(root_readers + cow_depth + pin_depth, 0U);
    EXPECT_TRUE(host_range::exclusive);
    EXPECT_TRUE(host_range::interrupts);
}
}  // namespace host_current_map
namespace {
class ManagedCurrentMap : public ManagedBatch {
   protected:
    ker::mod::sched::task::Task actor;
    void SetUp() override {
        ManagedBatch::SetUp();
        if (HasFatalFailure()) return;
        actor.pagemap = runtime_root;
        host_range::current = &actor;
        host_current_map::actor = &actor;
        host_current_map::enabled = true;
        host_current_map::entries = host_current_map::acks = 0;
    }
    void TearDown() override {
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        // After a genuine omitted-release control has failed, repair only this
        // finite scalar for safe fixture teardown, never a passing assertion.
        actor.usercopy_pagemap_accesses.store(0);
        host_current_map::enabled = false;
        host_current_map::actor = nullptr;
        host_range::current = nullptr;
        host_range::queryable = host_range::interrupts = true;
        ManagedBatch::TearDown();
    }
    auto publish_current(huge::CurrentHugeMapAdmission& admission) -> huge::Error {
        host_batch::observed = storage();
        host_batch::observed_parents = huge::detail::HugeMapBatchAccess::parents(batch);
        return admission.publish(batch);
    }
    void clear_mapping_between_repetitions() {
        discard();
        huge_check_free = false;
        for (auto& block : model.blocks)
            if (block.storage)
                for (uint32_t i = 0; i < block.pages; ++i)
                    if (block.refs[i]) (void)mm::phys::page_ref_dec(static_cast<uint8_t*>(block.storage.get()) + uint64_t{i} * PAGE);
        huge_check_free = true;
        for (auto* table : table_live) std::free(table);
        table_live.clear();
        std::memset(runtime_root, 0, PAGE);
        model.blocks.clear();
    }
};
TEST_F(ManagedCurrentMap, LocalOnlyOneAndTwoLeafPublishRetainsLeaseThroughAck) {
    for (uint16_t count : {uint16_t{1}, uint16_t{2}}) {
        ASSERT_EQ(prepare_batch(count), huge::Error::None);
        {
            ker::mod::sys::SyscallPreparationScope preparation(actor);
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            {
                huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
                ASSERT_EQ(admission.status(), huge::Error::None);
                EXPECT_EQ(admission.root(), runtime_root);
                EXPECT_EQ(admission.key().incarnation, runtime_serial);
                EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
                EXPECT_EQ(root_readers, 0U);
                EXPECT_EQ(publish_current(admission), huge::Error::None);
                EXPECT_TRUE(storage()->published && storage()->acknowledged);
                EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
            }
            EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
            EXPECT_TRUE(publication.acquired());
        }
        EXPECT_EQ(publication_depth + root_readers, 0U);
        clear_mapping_between_repetitions();
    }
    EXPECT_EQ(host_current_map::entries, 2U);
    EXPECT_EQ(host_current_map::acks, 2U);
}
TEST_F(ManagedCurrentMap, AbsentForeignInactiveAndDaemonCurrentNeverAcquire) {
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    auto reject = [&] {
        {
            huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
            EXPECT_EQ(admission.status(), huge::Error::Invalid);
            EXPECT_EQ(admission.root(), nullptr);
            EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        }
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    };
    host_range::queryable = false;
    reject();
    host_range::queryable = true;
    host_range::current = nullptr;
    reject();
    ker::mod::sched::task::Task foreign;
    host_range::current = &foreign;
    reject();
    host_range::current = &actor;
    actor.state.store(ker::mod::sched::task::TaskState::EXITED);
    reject();
    actor.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    actor.type = ker::mod::sched::task::TaskType::DAEMON;
    reject();
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    actor.syscall_account_start_us = 0;
    reject();
    actor.syscall_account_start_us = 1;
    EXPECT_EQ(host_current_map::entries, 0U);
}
TEST_F(ManagedCurrentMap, UnreadyScopeExactPubIfAndPreemptionAreRequired) {
    host_range::queryable = false;
    ker::mod::sys::SyscallPreparationScope unready(actor);
    host_range::queryable = true;
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    {
        huge::CurrentHugeMapAdmission admission(actor, unready, publication);
        EXPECT_EQ(admission.status(), huge::Error::Invalid);
    }
    auto reject = [&] {
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        EXPECT_EQ(admission.status(), huge::Error::Invalid);
    };
    // Explicit finite exact-owner mismatch, using the actual acquired() body.
    ker::syscall::vmem::g_shared_vmem_publication_owner.store(nullptr);
    reject();
    ker::syscall::vmem::g_shared_vmem_publication_owner.store(&publication);
    {
        HostIrqOff off;
        reject();
    }
    ++model.preempt_depth;
    reject();
    --model.preempt_depth;
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
}
TEST_F(ManagedCurrentMap, ClosingAndSaturatedTaskCounterRefuseWithoutUnderflow) {
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    actor.usercopy_pagemap_closing.store(true);
    {
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        EXPECT_EQ(admission.status(), huge::Error::Invalid);
    }
    actor.usercopy_pagemap_closing.store(false);
    actor.usercopy_pagemap_accesses.store(UINT32_MAX);
    {
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        EXPECT_EQ(admission.status(), huge::Error::Again);
    }
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), UINT32_MAX);
    actor.usercopy_pagemap_accesses.store(0);
    EXPECT_EQ(root_readers, 0U);
}
TEST_F(ManagedCurrentMap, PartialKernelKeyAndNullRootFailuresReleaseExactlyOnce) {
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    auto partial = [&] {
        {
            huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
            EXPECT_EQ(admission.status(), huge::Error::Stale);
            EXPECT_EQ(admission.root(), nullptr);
            EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
        }
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    };
    virt::kernel_pagemap = runtime_root;
    partial();
    virt::kernel_pagemap = nullptr;
    huge_root_live = false;
    partial();
    huge_root_live = true;
    auto const serial = runtime_serial;
    runtime_serial = 0;
    partial();
    runtime_serial = serial;
    actor.pagemap = nullptr;
    {
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        EXPECT_EQ(admission.status(), huge::Error::Again);
    }
    actor.pagemap = runtime_root;
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(root_readers, 0U);
}
TEST_F(ManagedCurrentMap, PublishRechecksContextBeforeOriginalPublisher) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
    ASSERT_EQ(admission.status(), huge::Error::None);
    auto reject = [&] {
        EXPECT_EQ(admission.publish(batch), huge::Error::Invalid);
        EXPECT_EQ(host_current_map::entries, 0U);
        expect_unpublished();
    };
    host_range::current = nullptr;
    reject();
    host_range::current = &actor;
    actor.usercopy_pagemap_closing.store(true);
    reject();
    actor.usercopy_pagemap_closing.store(false);
    actor.scheduler_transition_active.store(true);
    reject();
    actor.scheduler_transition_active.store(false);
    actor.exit_in_progress.store(true);
    reject();
    actor.exit_in_progress.store(false);
    actor.syscall_account_start_us = 0;
    reject();
    actor.syscall_account_start_us = 1;
    ker::syscall::vmem::g_shared_vmem_publication_owner.store(nullptr);
    reject();
    ker::syscall::vmem::g_shared_vmem_publication_owner.store(&publication);
    {
        HostIrqOff off;
        reject();
    }
    ++model.preempt_depth;
    reject();
    --model.preempt_depth;
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
}
TEST_F(ManagedCurrentMap, IncarnationChangeRefusesBeforeOriginalPublisher) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
    ASSERT_EQ(admission.status(), huge::Error::None);
    ++runtime_serial;
    EXPECT_EQ(admission.publish(batch), huge::Error::Stale);
    EXPECT_EQ(host_current_map::entries, 0U);
    --runtime_serial;
    expect_unpublished();
}
TEST_F(ManagedCurrentMap, EmptyAndDifferentBatchKeysNeverReachOriginalPublisher) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    huge::PreparedHugeMapBatch empty;
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
    ASSERT_EQ(admission.status(), huge::Error::None);
    EXPECT_EQ(admission.publish(empty), huge::Error::Invalid);
    auto* saved = storage();
    auto const key = saved->root;
    ++saved->root.incarnation;
    EXPECT_EQ(admission.publish(batch), huge::Error::Invalid);
    saved->root = key;
    saved->root.address_bits += PAGE;
    EXPECT_EQ(admission.publish(batch), huge::Error::Invalid);
    saved->root = key;
    EXPECT_EQ(host_current_map::entries, 0U);
    expect_unpublished();
}
TEST_F(ManagedCurrentMap, LatePreflightRefusalReleasesLocalBeforeOwnedReset) {
    auto* occupied = table();
    word(occupied, 0) = 0x4007;
    path(TEST_VA + LARGE) = reinterpret_cast<uint64_t>(occupied) | 7;
    auto const original = path(TEST_VA + LARGE);
    ASSERT_EQ(prepare_batch(2), huge::Error::None);
    {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        ASSERT_EQ(admission.status(), huge::Error::None);
        EXPECT_NE(publish_current(admission), huge::Error::None);
        EXPECT_EQ(host_current_map::entries, 1U);
        expect_unpublished();
    }
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(path(TEST_VA + LARGE), original);
    EXPECT_EQ(model.live_refs(), 2U * huge::LEAF_PAGES);
    discard();
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(ManagedCurrentMap, DetachedOldTableSurvivesAckAndLocalReleaseBeforeReset) {
    auto* old = table();
    path(TEST_VA) = reinterpret_cast<uint64_t>(old) | 7;
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
        ASSERT_EQ(admission.status(), huge::Error::None);
        EXPECT_EQ(publish_current(admission), huge::Error::None);
        EXPECT_EQ(host_current_map::acks, 1U);
        EXPECT_EQ(host_batch::detached_returns, 0U);
    }
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    discard();
    EXPECT_EQ(host_batch::detached_returns, 1U);
}
TEST_F(ManagedCurrentMap, SeparateActualReaderEndsBeforeCurrentAdmissionPublish) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    huge::detail::CurrentRangeRootAccess reader;
    ASSERT_EQ(huge::detail::acquire_current_range_root(actor, reader), huge::Error::None);
    EXPECT_EQ(root_readers, 1U);
    huge::detail::release_current_range_root(reader);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
    ASSERT_EQ(admission.status(), huge::Error::None);
    EXPECT_EQ(publish_current(admission), huge::Error::None);
}
TEST_F(ManagedCurrentMap, CallerBookendDiesBeforeAdmissionBeforeBorrowedPub) {
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        {
            huge::CurrentHugeMapAdmission admission(actor, preparation, publication);
            ASSERT_EQ(admission.status(), huge::Error::None);
            struct Bookend {
                ker::mod::sched::task::Task& actor;
                const ker::syscall::vmem::SharedVmemPublicationGuard& publication;
                ~Bookend() {
                    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
                    EXPECT_TRUE(publication.acquired());
                }
            } observer{actor, publication};
        }
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_TRUE(publication.acquired());
    }
    EXPECT_EQ(publication_depth, 0U);
}
}  // namespace
