// Real308 bodies on finite Host authority/side-effect ports. No WOS execution.
namespace host_policy {
void ordinary() {
    EXPECT_EQ(publication_depth + cow_depth + pin_depth + root_readers + backend().preempt_depth, 0U);
    EXPECT_EQ(host_range::interrupts, expected_if);
    ASSERT_NE(host_range::current, nullptr);
    EXPECT_EQ(host_range::current->usercopy_pagemap_accesses.load(), 0U);
    EXPECT_NE(host_range::current->pending_mremap_ps_policies, nullptr);
    EXPECT_GT(ker::mod::mm::virt::host_root_control.ps_admission_suspensions, 0U);
}
}  // namespace host_policy
namespace {
namespace policy = ker::syscall::vmem;
using PolicyToken = policy::PendingMremapPsPolicy;
class ManagedMremapPolicy : public ManagedRange {
   protected:
    void SetUp() override {
        ManagedRange::SetUp();
        host_policy::frees = host_policy::allocations = host_policy::effects = host_policy::claims = 0;
        host_policy::fd_calls = host_policy::resource_calls = 0;
        host_policy::overlap = host_policy::hardware_current = false;
        host_policy::raw_current_endpoint = false;
        host_policy::independent_root = nullptr;
        virt::host_child_control = {23, 1, 0};  // Explicit finite identity/storage seed, not native registration.
        host_policy::expected_if = true;
        host_policy::unique_ranges = true;
        host_policy::free_error = 0;
        actor.pending_mremap_ps_policies = nullptr;
    }
    void TearDown() override {
        host_policy::raw_current_endpoint = host_policy::hardware_current = false;
        policy::cancel_mremap_ps_policies(actor);
        EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
        EXPECT_EQ(virt::host_root_control.ps_admission_suspensions, 0U);
        EXPECT_EQ(virt::host_child_control.ps_admission_suspensions, 0U);
        host_policy::independent_root = nullptr;
        virt::host_child_control = {23, 1, 0};
        host_policy::unique_ranges = false;
        fork_mode = false;
        fork_child = nullptr;
        ManagedRange::TearDown();
    }
    auto remap(uint64_t old_size = 2 * PAGE, uint64_t new_size = PAGE, uint64_t flags = 0) -> int64_t {
        return static_cast<int64_t>(policy::anon_mremap(TEST_VA, old_size, new_size, flags));
    }
    auto count() const -> uint64_t { return virt::host_root_control.ps_admission_suspensions; }
    void settled() {
        EXPECT_EQ(publication_depth + cow_depth + pin_depth + root_readers + backend().preempt_depth, 0U);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_TRUE(host_range::interrupts);
    }
    template <class F>
    auto access(F&& action) -> huge::Error {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        huge::detail::CurrentRangeRootAccess root{};
        huge::Error error;
        {
            ker::mod::sched::PreemptGuard preempt;
            policy::SharedVmemPublicationTryGuard publication;
            error = huge::detail::acquire_current_range_root(actor, root);
            if (error == huge::Error::None) error = action(preparation, publication, root);
            huge::detail::release_current_range_root(root);
        }
        return error;
    }
};

TEST_F(ManagedMremapPolicy, CurrentContextAndPartialAccessRefusalsReturnEveryOwner) {
    for (unsigned axis = 0; axis < 8; ++axis) {
        PolicyToken token;
        if (axis == 0) host_range::queryable = false;
        if (axis == 1) actor.type = ker::mod::sched::task::TaskType::DAEMON;
        if (axis == 2) actor.syscall_account_start_us = 0;
        if (axis == 3) actor.exit_in_progress = true;
        if (axis == 4) actor.usercopy_pagemap_closing = true;
        if (axis == 5) host_range::reader_busy = true;
        if (axis == 6) actor.scheduler_transition_active = true;
        if (axis == 7) model.preempt_depth = 1;
        EXPECT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), -EAGAIN);
        EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
        EXPECT_EQ(count(), 0U);
        host_range::queryable = true;
        actor.type = ker::mod::sched::task::TaskType::PROCESS;
        actor.syscall_account_start_us = 1;
        actor.exit_in_progress = false;
        actor.usercopy_pagemap_closing = false;
        host_range::reader_busy = false;
        actor.scheduler_transition_active = false;
        model.preempt_depth = 0;
        settled();
    }
    {
        policy::SharedVmemPublicationTryGuard held;
        ASSERT_TRUE(held.acquired());
        PolicyToken token;
        EXPECT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), -EAGAIN);
        EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
        EXPECT_EQ(count(), 0U);
    }
    settled();
}
TEST_F(ManagedMremapPolicy, ExistingErrorPriorityIdentityAndNoMoveSkipAcquisition) {
    host_range::current = nullptr;
    EXPECT_EQ(remap(), -EFAULT);
    host_range::current = &actor;
    auto* root = actor.pagemap;
    actor.pagemap = nullptr;
    EXPECT_EQ(remap(), -EFAULT);
    actor.pagemap = root;
    actor.native_file_mapping_retirement = reinterpret_cast<void*>(1);
    EXPECT_EQ(remap(), -EBUSY);
    actor.native_file_mapping_retirement = nullptr;
    EXPECT_EQ(static_cast<int64_t>(policy::anon_mremap(0, PAGE, PAGE, 0)), -EINVAL);
    EXPECT_EQ(remap(UINT64_MAX, PAGE), -EINVAL);
    host_policy::overlap = true;
    EXPECT_EQ(remap(), -EOPNOTSUPP);
    host_policy::overlap = false;
    host_range::reader_busy = true;
    EXPECT_EQ(remap(PAGE, PAGE), static_cast<int64_t>(TEST_VA));
    EXPECT_EQ(remap(PAGE, 2 * PAGE, 0), -ENOMEM);
    host_range::reader_busy = false;
    EXPECT_EQ(count(), 0U);
    EXPECT_EQ(host_policy::frees + host_policy::allocations, 0U);
    settled();
}
TEST_F(ManagedMremapPolicy, RealShrinkAndGrowthKeepPolicyThroughOrdinaryCallbacks) {
    EXPECT_EQ(remap(), static_cast<int64_t>(TEST_VA));
    EXPECT_EQ(host_policy::frees, 1U);
    EXPECT_EQ(count(), 0U);
    host_policy::free_error = -EACCES;
    EXPECT_EQ(remap(), -EACCES);
    host_policy::free_error = 0;
    EXPECT_EQ(remap(PAGE, 2 * PAGE, 1), -ENOMEM);
    EXPECT_EQ(host_policy::allocations, 1U);
    host_range::interrupts = false;
    host_policy::expected_if = false;
    EXPECT_EQ(remap(), static_cast<int64_t>(TEST_VA));
    EXPECT_FALSE(host_range::interrupts);
    host_range::interrupts = true;
    host_policy::expected_if = true;
    EXPECT_EQ(count(), 0U);
    settled();
}
TEST_F(ManagedMremapPolicy, ShrinkScansRetainedPrefixAndRemovedSuffix) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    EXPECT_EQ(remap(2 * LARGE, LARGE), -EOPNOTSUPP);  // PS lies in retained prefix.
    EXPECT_EQ(host_policy::frees, 0U);
    EXPECT_EQ(count(), 0U);
    EXPECT_EQ(static_cast<int64_t>(policy::anon_mremap(TEST_VA - PAGE, LARGE + PAGE, PAGE, 0)), -EOPNOTSUPP);
    EXPECT_EQ(host_policy::frees, 0U);
    settled();
}
TEST_F(ManagedMremapPolicy, UnknownPsAndUnsupportedHierarchyNeverAllocateOrFree) {
    for (int level : {4, 3, 2}) {
        if (level != 4) ordinary_path(TEST_VA);
        auto& raw = level == 4 ? word(runtime_root, virt::index_of(TEST_VA, 4)) : slot_at(runtime_root, TEST_VA, level);
        auto before = raw;
        raw = 0x200087;
        EXPECT_EQ(remap(PAGE, 2 * PAGE, 1), -EOPNOTSUPP);
        EXPECT_EQ(count(), 0U);
        raw = before;
    }
    EXPECT_EQ(host_policy::frees + host_policy::allocations, 0U);
    settled();
}
TEST_F(ManagedMremapPolicy, NestedListHeadMiddleTailAndLastRelease) {
    PolicyToken a, b, c;
    ASSERT_EQ(a.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    ASSERT_EQ(b.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    ASSERT_EQ(c.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(count(), 3U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, &c);
    b.cancel();
    EXPECT_EQ(count(), 2U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, &c);
    a.cancel();
    EXPECT_EQ(count(), 1U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, &c);
    c.cancel();
    c.cancel();
    EXPECT_EQ(count(), 0U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
    settled();
}
TEST_F(ManagedMremapPolicy, SeparateExecutorsShareCountWithoutSharingLists) {
    ker::mod::sched::task::Task peer;
    peer.pagemap = runtime_root;
    PolicyToken a, b;
    ASSERT_EQ(a.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    host_range::current = &peer;
    ASSERT_EQ(b.prepare_current(peer, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(count(), 2U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, &a);
    EXPECT_EQ(peer.pending_mremap_ps_policies, &b);
    ASSERT_FALSE(fork_mode);
    ASSERT_EQ(fork_child, nullptr);
    std::array<unsigned, 4> fork_before{fork_exclusives, fork_reader_attempts, fork_cow_entries, fork_swap_clones};
    auto events_before = fork_events;
    auto* separate_root = table();
    ASSERT_NE(separate_root, runtime_root);
    host_policy::independent_root = separate_root;
    virt::host_child_control = {23, 1, 0};
    ker::mod::sched::task::Task other;
    other.pagemap = separate_root;
    host_range::current = &other;
    PolicyToken independent;
    ASSERT_EQ(independent.prepare_current(other, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(count(), 2U);
    EXPECT_EQ(virt::host_child_control.ps_admission_suspensions, 1U);
    independent.cancel();
    EXPECT_EQ(virt::host_child_control.ps_admission_suspensions, 0U);
    host_policy::independent_root = nullptr;
    virt::host_child_control = {23, 1, 0};
    host_range::current = &peer;
    EXPECT_FALSE(fork_mode);
    EXPECT_EQ(fork_child, nullptr);
    EXPECT_EQ((std::array<unsigned, 4>{fork_exclusives, fork_reader_attempts, fork_cow_entries, fork_swap_clones}), fork_before);
    EXPECT_EQ(fork_events, events_before);
    b.cancel();
    EXPECT_EQ(peer.pending_mremap_ps_policies, nullptr);
    EXPECT_EQ(count(), 1U);
    host_range::current = &actor;
    a.cancel();
    EXPECT_EQ(count(), 0U);
    settled();
}
TEST_F(ManagedMremapPolicy, EmptyLinkedAcquisitionFailureAndDestructionAreIdempotent) {
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    {
        PolicyToken token;
        EXPECT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), -EAGAIN);
        EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
        EXPECT_EQ(count(), 0U);
        token.cancel();
    }
    virt::host_root_control.ps_geometry_epoch = 1;
    {
        PolicyToken token;
        ASSERT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
        EXPECT_EQ(count(), 1U);
    }
    EXPECT_EQ(count(), 0U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
    settled();
}
TEST_F(ManagedMremapPolicy, RetiredAndReusedSerialCannotConsumeNewCount) {
    PolicyToken old, current;
    ASSERT_EQ(old.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    // Root lookup/retirement is an explicit Host identity port here. Actual
    // retirement/reset/72-73 allocation bodies are tested by the futex fixture.
    ++runtime_serial;
    virt::host_root_control.ps_admission_suspensions = 0;
    ASSERT_EQ(current.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(count(), 1U);
    old.cancel();
    EXPECT_EQ(count(), 1U);
    EXPECT_EQ(actor.pending_mremap_ps_policies, &current);
    current.cancel();
    EXPECT_EQ(count(), 0U);
    ASSERT_EQ(old.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    huge_root_live = false;
    old.cancel();
    huge_root_live = true;
    virt::host_root_control.ps_admission_suspensions = 0;
    EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
    settled();
}
TEST_F(ManagedMremapPolicy, OverflowRefusesAndSameSerialUnderflowTraps) {
    virt::host_root_control.ps_admission_suspensions = UINT64_MAX;
    PolicyToken token;
    EXPECT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), -EAGAIN);
    EXPECT_EQ(count(), UINT64_MAX);
    EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
    virt::host_root_control.ps_admission_suspensions = UINT64_MAX - 1;
    ASSERT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(count(), UINT64_MAX);
    token.cancel();
    EXPECT_EQ(count(), UINT64_MAX - 1);
    virt::host_root_control.ps_admission_suspensions = 0;
    huge::RootKey invalid_owned{reinterpret_cast<uint64_t>(runtime_root), runtime_serial};
    EXPECT_DEATH(huge::detail::release_mremap_ps_policy(invalid_owned), "");
    settled();
}
TEST_F(ManagedMremapPolicy, FinalValidatorRejectsIncompleteStaleAndChangedSerial) {
    PolicyToken token;
    ASSERT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(access([&](auto& preparation, auto& publication, auto& root) {
                  huge::RangeSplitCursor cursor{};
                  cursor.start = TEST_VA;
                  cursor.end = TEST_VA + PAGE;
                  auto result = huge::detail::survey_range_chunk(root.root, cursor, huge::RangeSurveyMode::Count, nullptr);
                  if (result != huge::Error::None) return result;
                  huge::RootKey key{reinterpret_cast<uint64_t>(runtime_root), runtime_serial};
                  EXPECT_EQ(huge::detail::validate_mremap_ps_free(actor, preparation, publication, root, key, cursor), huge::Error::None);
                  cursor.complete = false;
                  EXPECT_EQ(huge::detail::validate_mremap_ps_free(actor, preparation, publication, root, key, cursor), huge::Error::Again);
                  cursor.complete = true;
                  ++cursor.stamp.epoch;
                  EXPECT_EQ(huge::detail::validate_mremap_ps_free(actor, preparation, publication, root, key, cursor), huge::Error::Again);
                  --cursor.stamp.epoch;
                  ++key.incarnation;
                  EXPECT_EQ(huge::detail::validate_mremap_ps_free(actor, preparation, publication, root, key, cursor), huge::Error::Again);
                  return huge::Error::None;
              }),
              huge::Error::None);
    token.cancel();
    settled();
}
TEST_F(ManagedMremapPolicy, PreparedSingleAndBatchPublishersRefuseBeforeTransfer) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    huge::PreparedHugeMapBatch batch;
    ASSERT_EQ(batch.prepare({reinterpret_cast<uint64_t>(runtime_root), runtime_serial}, TEST_VA + 4 * LARGE, LARGE, 7, context()),
              huge::Error::None);
    PolicyToken token;
    ASSERT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    auto refs = model.live_refs();
    auto root_word = word(runtime_root, virt::index_of(TEST_VA, 4));
    EXPECT_EQ(publish(0), huge::Error::Again);
    EXPECT_FALSE(data[0].empty());
    {
        policy::SharedVmemPublicationGuard publication;
        EXPECT_EQ(huge::detail::publish_prepared_huge_batch(runtime_root, publication, batch), huge::Error::Again);
    }
    auto* saved = huge::detail::HugeMapBatchAccess::storage(batch);
    ASSERT_NE(saved, nullptr);
    EXPECT_FALSE(saved->published);
    EXPECT_EQ(saved->edit_count + saved->role_count, 0U);
    EXPECT_EQ(model.live_refs(), refs);
    EXPECT_EQ(word(runtime_root, virt::index_of(TEST_VA, 4)), root_word);
    EXPECT_EQ(actual_flushes, 0U);
    token.cancel();
    ASSERT_EQ(publish(0), huge::Error::None);
    {
        policy::SharedVmemPublicationGuard publication;
        ASSERT_EQ(huge::detail::publish_prepared_huge_batch(runtime_root, publication, batch), huge::Error::None);
    }
    ASSERT_TRUE(saved->published);
    ASSERT_TRUE(saved->acknowledged);
    // Explicit finite mapping cleanup, identical to the existing batch port:
    // publication ACK already completed; these release its registry credit.
    EXPECT_EQ(huge_core.begin_split(saved->leaves[0].view.handle, saved->leaves[0].key), huge::Error::None);
    EXPECT_EQ(huge_core.complete_split_after_ack(saved->leaves[0].view.handle), huge::Error::None);
    batch.reset();
    settled();
}
TEST_F(ManagedMremapPolicy, RawNewPsTransitionsRetainEveryOriginalPremise) {
    host_policy::raw_current_endpoint = true;
    ordinary_path(TEST_VA);
    slot_at(runtime_root, TEST_VA, 2) = 0;
    ker::mod::sched::task::Task unpublished;
    unpublished.pid = 0;
    unpublished.pagemap = runtime_root;
    virt::host_root_control.ps_admission_suspensions = 1;  // Explicit never-current control seed, not a policy acquisition.
    for (int level : {2, 3}) {
        auto& raw = slot_at(runtime_root, TEST_VA, level);
        auto saved = raw;
        for (uint64_t before : {uint64_t{0}, uint64_t{128}, uint64_t{7}}) {
            raw = before;
            EXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(unpublished, runtime_root, TEST_VA, level, before, before | 0x87));
            EXPECT_EQ(raw, before);
        }
        raw = saved;
    }
    unpublished.pid = 1;
    EXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(unpublished, runtime_root, TEST_VA, 2, 0, 0x87));
    unpublished.pid = 0;
    unpublished.scheduler_published = true;
    EXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(unpublished, runtime_root, TEST_VA, 2, 0, 0x87));
    unpublished.scheduler_published = false;
    host_policy::hardware_current = true;
    EXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(unpublished, runtime_root, TEST_VA, 2, 0, 0x87));
    host_policy::raw_current_endpoint = host_policy::hardware_current = false;
    virt::host_root_control.ps_admission_suspensions = 0;
    settled();
}
TEST_F(ManagedMremapPolicy, FullSurveyYieldsWithOnlyNodeAndCopiedKey) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    {
        PolicyToken disjoint;
        ASSERT_EQ(disjoint.prepare_current(actor, PAGE, 2 * PAGE), 0);
        EXPECT_EQ(split(cleanup[0]), huge::Error::None);
        mapped[0] = false;
        EXPECT_EQ(count(), 1U);
    }
    PolicyToken token;
    unsigned visits = 0;
    range_yield_action = [&] {
        ++visits;
        settled();
        EXPECT_EQ(count(), 1U);
        EXPECT_EQ(actor.pending_mremap_ps_policies, &token);
    };
    ASSERT_EQ(token.prepare_current(actor, PAGE, (uint64_t{1} << 47) - PAGE), 0);
    EXPECT_GT(visits, 0U);
    token.cancel();
    settled();
}
TEST_F(ManagedMremapPolicy, ExitFrontiersDrainActualNodesBeforeResourceCleanup) {
    for (bool worker : {false, true}) {
        PolicyToken a, b;
        ASSERT_EQ(a.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
        ASSERT_EQ(b.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
        if (worker)
            host_policy::worker_exit_frontier(&actor);
        else
            host_policy::process_exit_frontier(&actor);
        EXPECT_EQ(count(), 0U);
        EXPECT_EQ(actor.pending_mremap_ps_policies, nullptr);
        a.cancel();
        b.cancel();
    }
    EXPECT_EQ(host_policy::fd_calls, 2U);
    EXPECT_EQ(host_policy::resource_calls, 2U);
    settled();
}
TEST_F(ManagedMremapPolicy, NativeCommitAndPendingClaimPrefixesRefuseBeforeEffects) {
    PolicyToken token;
    ASSERT_EQ(token.prepare_current(actor, TEST_VA, TEST_VA + PAGE), 0);
    ker::syscall::process::PreparedNativeExecImage image;
    EXPECT_EQ(ker::syscall::process::commit_native_exec_image(&actor, image), -EAGAIN);
    ker::syscall::process::service_pending_exec_handoff();
    EXPECT_EQ(host_policy::effects + host_policy::claims, 0U);
    EXPECT_EQ(count(), 1U);
    token.cancel();
    EXPECT_EQ(ker::syscall::process::commit_native_exec_image(&actor, image), 0);
    ker::syscall::process::service_pending_exec_handoff();
    EXPECT_EQ(host_policy::effects, 1U);
    EXPECT_EQ(host_policy::claims, 1U);
    settled();
}
}  // namespace
