// Actual split/staging/unmap composition on declared Host registry, frame and
// completed-flush endpoints. AS recount below tests lifetime order, not quotas.
namespace {
class ManagedAnonymousRemove : public ForkVmCoordinator {
   protected:
    using Task = ker::mod::sched::task::Task;
    using Row = ker::mod::sched::task::LazyVmemRange;
    Task sibling;
    ker::mod::sched::threading::Thread protected_layout{};
    unsigned leaves{};
    std::array<uint64_t, 2> physical{};
    void SetUp() override {
        ForkVmCoordinator::SetUp();
        fork_mode = false;
        sibling.pid = 3;
        sibling.pagemap = runtime_root;
        host_managed_remove::enabled = true;
        host_managed_remove::invoking = false;
        host_managed_remove::registry_depth = host_managed_remove::observations = 0;
        host_managed_remove::unmap_acks = host_managed_remove::unmap_drops = 0;
        host_managed_remove::refused_task = nullptr;
        host_managed_remove::registry = {};
        host_managed_remove::registry[0] = &actor;
        host_managed_remove::registry[1] = &sibling;
        host_managed_remove::count = 2;
        host_managed_remove::expected_unmaps.clear();
    }
    void TearDown() override {
        host_fork_vm::enabled = host_managed_remove::invoking = false;
        host_fork_vm::on_pub = host_fork_vm::on_legacy = nullptr;
        sibling.thread = nullptr;
        actor.thread = nullptr;
        ker::mod::sched::task::release_lazy_vmem_ranges(sibling);
        EXPECT_EQ(sibling.lifetime_refs, 1U);
        EXPECT_EQ(host_managed_remove::registry_depth, 0U);
        host_managed_remove::registry = {};
        host_managed_remove::count = 0;
        host_managed_remove::enabled = false;
        ForkVmCoordinator::TearDown();
    }
    void row(Task& task, uint64_t start, uint64_t end) {
        Row value{};
        value.start = start;
        value.end = end;
        value.kind = ker::mod::sched::task::LazyVmemKind::ANONYMOUS;
        value.prot = 3;
        value.flags = ker::abi::vmem::MAP_PRIVATE;
        ASSERT_TRUE(ker::mod::sched::task::try_retain_lazy_vmem_range(value));
        ASSERT_TRUE(task.lazy_vmem_ranges.push_back(value));
    }
    void rows(unsigned n = 1) {
        row(actor, TEST_VA, TEST_VA + n * LARGE);
        // Deliberately reversed and partitioned differently from the actor.
        row(sibling, TEST_VA + LARGE / 2, TEST_VA + n * LARGE);
        row(sibling, TEST_VA, TEST_VA + LARGE / 2);
    }
    void seed(unsigned n = 1) {
        leaves = n;
        auto const BASE = model.topology.memory[0].base;
        for (unsigned i = 0; i < n; ++i) {
            // The finite backend allocates at the supplied range start. Give
            // each live run a distinct PA; no buddy allocator is modeled.
            model.topology.memory[0].base = BASE + i * LARGE;
            ASSERT_EQ(prepare(i), huge::Error::None);
            ASSERT_EQ(publish(i), huge::Error::None);
            physical[i] = slot_at(runtime_root, TEST_VA + i * LARGE, 2) & virt::PTE_FRAME_MASK;
            for (unsigned page = 0; page < 512; ++page) std::memset(bytes[i] + page * PAGE, (page % 251) + 1, PAGE);
        }
        model.topology.memory[0].base = BASE;
        if (n == 2) ASSERT_NE(physical[0], physical[1]);
        rows(n);
    }
    auto invoke(uint64_t first, uint64_t end) -> int {
        auto& expected = host_managed_remove::expected_unmaps;
        expected.clear();
        for (auto va = first; va < end && va >= TEST_VA && va < TEST_VA + leaves * LARGE; va += PAGE) {
            auto offset = va - TEST_VA;
            expected.push_back({va, bytes[offset / LARGE] + offset % LARGE});
        }
        host_managed_remove::invoking = host_fork_vm::enabled = true;
        huge_check_free = false;
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        int status = vmem::detail::remove_current_anonymous_range(actor, preparation, first, end);
        host_managed_remove::invoking = host_fork_vm::enabled = false;
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers, 0U);
        EXPECT_FALSE(host_range::exclusive);
        for (unsigned i = 0; i < leaves && (status == 0 || status == -EAGAIN); ++i) {
            if ((slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128) == 0) {
                mapped[i] = false;
                cleanup[i].reset();
            }
        }
        return status;
    }
    auto covering(const Task& task, uint64_t va) -> unsigned {
        unsigned count = 0;
        for (auto const& entry : task.lazy_vmem_ranges) count += entry.start <= va && va < entry.end;
        return count;
    }
    void unchanged() {
        for (unsigned i = 0; i < leaves; ++i) {
            EXPECT_NE(slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128, 0U);
            EXPECT_EQ(covering(actor, TEST_VA + i * LARGE), 1U);
            EXPECT_EQ(covering(sibling, TEST_VA + i * LARGE), 1U);
        }
        EXPECT_EQ(model.live_refs(), uint64_t{leaves} * 512);
        EXPECT_EQ(host_managed_remove::unmap_acks + host_managed_remove::unmap_drops, 0U);
    }
    void removed(uint64_t first, uint64_t end, uint64_t retained = UINT64_MAX) {
        EXPECT_EQ(host_managed_remove::unmap_acks, (end - first) / PAGE);
        EXPECT_EQ(host_managed_remove::unmap_drops, (end - first) / PAGE);
        EXPECT_EQ(host_managed_remove::observations, 1U);
        for (unsigned i = 0; i < leaves; ++i) {
            auto block =
                std::find_if(model.blocks.begin(), model.blocks.end(), [&](auto const& value) { return value.physical == physical[i]; });
            ASSERT_NE(block, model.blocks.end());
            for (unsigned page = 0; page < 512; ++page) {
                uint64_t va = TEST_VA + i * LARGE + page * PAGE;
                bool gone = first <= va && va < end;
                EXPECT_EQ(covering(actor, va), gone ? 0U : 1U);
                EXPECT_EQ(covering(sibling, va), gone ? 0U : 1U);
                EXPECT_EQ(block->refs[page], (gone ? 0U : 1U) + (va == retained ? 1U : 0U));
                auto raw = slot_at(runtime_root, va, 1);
                if (gone) {
                    EXPECT_EQ(raw, 0U);
                } else {
                    EXPECT_EQ(raw & virt::PTE_FRAME_MASK, physical[i] + page * PAGE);
                    EXPECT_EQ(raw & 7, 7U);
                    EXPECT_EQ(bytes[i][page * PAGE], (page % 251) + 1);
                    EXPECT_EQ(bytes[i][(page + 1) * PAGE - 1], (page % 251) + 1);
                }
            }
        }
        huge::Snapshot snapshot{};
        huge_core.snapshot(snapshot);
        EXPECT_EQ(snapshot.charged_pages, 0U);
        EXPECT_EQ(snapshot.plans, 0U);
    }
};
TEST_F(ManagedAnonymousRemove, PartialRemovalPreservesUnrequestedFramesAndSiblingRows) {
    seed();
    ASSERT_EQ(invoke(TEST_VA + 13 * PAGE, TEST_VA + 14 * PAGE), 0);
    removed(TEST_VA + 13 * PAGE, TEST_VA + 14 * PAGE);
}
TEST_F(ManagedAnonymousRemove, PrefixAndSuffixUseHalfOpenPageBoundaries) {
    seed();
    actor.lazy_vmem_ranges[0].flags = 0;  // Actual private convention also accepts no MAP_SHARED bit.
    ASSERT_EQ(invoke(TEST_VA, TEST_VA + PAGE), 0);
    removed(TEST_VA, TEST_VA + PAGE);
}
TEST_F(ManagedAnonymousRemove, SuffixRemovalDoesNotConsumeThePreviousPage) {
    seed();
    ASSERT_EQ(invoke(TEST_VA + LARGE - PAGE, TEST_VA + LARGE), 0);
    removed(TEST_VA + LARGE - PAGE, TEST_VA + LARGE);
}
TEST_F(ManagedAnonymousRemove, CrossLeafRemovalConsumesBothCompleteDescriptors) {
    seed(2);
    ASSERT_EQ(invoke(TEST_VA + LARGE - PAGE, TEST_VA + LARGE + PAGE), 0);
    removed(TEST_VA + LARGE - PAGE, TEST_VA + LARGE + PAGE);
}
TEST_F(ManagedAnonymousRemove, WholeLeafReturnsEveryMappingReferenceAfterAcknowledgement) {
    seed();
    ASSERT_EQ(invoke(TEST_VA, TEST_VA + LARGE), 0);
    removed(TEST_VA, TEST_VA + LARGE);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(ManagedAnonymousRemove, RetainedFrameSurvivesRemovalAndOldPinIdentityCannotWrite) {
    seed();
    uint64_t const VA = TEST_VA + 17 * PAGE;
    virt::UserLeafResolution leaf{};
    ASSERT_TRUE(virt::resolve_user_leaf_locked(runtime_root, VA, true, leaf));
    auto* retained = bytes[0] + 17 * PAGE;
    ASSERT_TRUE(mm::phys::page_ref_try_inc(retained, nullptr));
    // Same finite identity port as ManagedResolver; this does not acquire a
    // native Task/root pin lease or establish a concurrent pinning window.
    virt::UserPagePin pin{};
    pin.pagemap = runtime_root;
    pin.user_page = VA;
    pin.physical_page = leaf.physical_4k;
    pin.hhdm_page = retained;
    pin.access_gate_index = 0;
    pin.require_writable = true;
    pin.root_incarnation = runtime_serial;
    ASSERT_EQ(invoke(VA, VA + PAGE), 0);
    removed(VA, VA + PAGE, VA);
    EXPECT_FALSE(virt::user_page_pin_commit_write(pin));
    EXPECT_EQ(retained[0], 18U);
    EXPECT_EQ(mm::phys::page_ref_dec(retained), 0U);
}
TEST_F(ManagedAnonymousRemove, SharedAndFileLookingAnonymousRowsRefuseBeforeWrites) {
    seed();
    actor.lazy_vmem_ranges[0].flags = ker::abi::vmem::MAP_SHARED;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    unchanged();
    actor.lazy_vmem_ranges[0].flags = ker::abi::vmem::MAP_PRIVATE;
    // Anonymous rows must not conceal an independently owned union member.
    // The fixture pointer is borrowed and reset before ordinary row teardown.
    actor.lazy_vmem_ranges[0].file = &file;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    actor.lazy_vmem_ranges[0].file = nullptr;
    unchanged();
}
TEST_F(ManagedAnonymousRemove, SiblingGapsAndDuplicateCoverageRefuseBeforeWrites) {
    seed();
    sibling.lazy_vmem_ranges[1].start += PAGE;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    sibling.lazy_vmem_ranges[1].start -= PAGE;
    auto duplicate = sibling.lazy_vmem_ranges[1];
    ASSERT_TRUE(sibling.lazy_vmem_ranges.push_back(duplicate));
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    sibling.lazy_vmem_ranges.pop_back();
    unchanged();
}
TEST_F(ManagedAnonymousRemove, IndependentLegacyAndTypedOwnersRefuseBeforeSplit) {
    seed();
    legacy(TEST_VA + PAGE);
    ker::mod::sched::task::release_lazy_vmem_ranges(actor);
    row(actor, TEST_VA, TEST_VA + LARGE);
    int const REFS = file.refcount.load();
    EXPECT_EQ(invoke(TEST_VA + PAGE, TEST_VA + 2 * PAGE), -EOPNOTSUPP);
    EXPECT_EQ(file.refcount.load(), REFS);
    unchanged();
}
TEST_F(ManagedAnonymousRemove, IndependentTypedIndexSnapshotReturnsAfterPublication) {
    seed();
    tmpfs();
    ker::mod::sched::task::release_lazy_vmem_ranges(actor);
    row(actor, TEST_VA, TEST_VA + LARGE);
    auto before = family->owner_counts();
    EXPECT_EQ(invoke(TEST_VA + 2 * PAGE, TEST_VA + 3 * PAGE), -EOPNOTSUPP);
    auto after = family->owner_counts();
    EXPECT_EQ(after.count(fm::FamilyOwner::LAZY), before.count(fm::FamilyOwner::LAZY));
    EXPECT_EQ(after.count(fm::FamilyOwner::PREPARED), before.count(fm::FamilyOwner::PREPARED));
    EXPECT_EQ(host_managed_remove::observations, 0U);
    unchanged();
}
TEST_F(ManagedAnonymousRemove, InitialSiblingLayoutRefusesBeforePteWrites) {
    seed();
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(invoke(TEST_VA + PAGE, TEST_VA + 2 * PAGE), -EINVAL);
    unchanged();
}
TEST_F(ManagedAnonymousRemove, EmptyAndOrdinaryFourKGeometryCannotUseThisProfile) {
    rows();
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    ordinary_path(TEST_VA);
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    EXPECT_EQ(host_managed_remove::observations + host_managed_remove::unmap_drops, 0U);
}
TEST_F(ManagedAnonymousRemove, UnknownManagedAddressAndOneGigabyteGeometryRefuse) {
    seed();
    auto& raw = slot_at(runtime_root, TEST_VA, 2);
    auto saved = raw;
    raw += LARGE;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    raw = saved;
    auto& upper = slot_at(runtime_root, TEST_VA, 3);
    auto saved_upper = upper;
    upper = 0x40000087;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EOPNOTSUPP);
    upper = saved_upper;
    unchanged();
}
TEST_F(ManagedAnonymousRemove, StaleRootAndGeometryEpochRefuseWithoutPartialRemoval) {
    seed();
    host_fork_vm::on_legacy = [] { ++runtime_serial; };
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EAGAIN);
    --runtime_serial;
    host_fork_vm::on_legacy = [] { ++virt::host_root_control.ps_geometry_epoch; };
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    unchanged();
}
TEST_F(ManagedAnonymousRemove, LateSiblingChangeLeavesEquivalentDemotionAndNoMetadataCommit) {
    seed();
    auto* changing = &sibling;
    host_fork_vm::on_legacy = +[] {
        auto* task = host_managed_remove::registry[1];
        task->lazy_vmem_ranges[0].prot = 1;
    };
    EXPECT_EQ(invoke(TEST_VA + PAGE, TEST_VA + 2 * PAGE), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    EXPECT_EQ(changing->lazy_vmem_ranges[0].prot, 1U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(model.live_refs(), 512U);
    EXPECT_EQ(covering(actor, TEST_VA + PAGE), 1U);
    EXPECT_EQ(covering(sibling, TEST_VA + PAGE), 1U);
    EXPECT_EQ(host_managed_remove::unmap_acks + host_managed_remove::unmap_drops, 0U);
    EXPECT_EQ(host_managed_remove::observations, 1U);
}
TEST_F(ManagedAnonymousRemove, RefusedCohortLifetimeLeavesEveryOwnedPrefixUntouched) {
    seed();
    host_managed_remove::refused_task = &sibling;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -EAGAIN);
    host_managed_remove::refused_task = nullptr;
    EXPECT_EQ(actor.lifetime_refs, 1U);
    EXPECT_EQ(sibling.lifetime_refs, 1U);
    unchanged();
}
TEST_F(ManagedAnonymousRemove, EveryMetadataAllocationPrefixAndTableDenialReturnsOrdinarily) {
    seed();
    bool completed = false;
    for (int prefix = 0; prefix < 16; ++prefix) {
        auto const TAKEN = host_fork_vm::metadata_allocations;
        host_fork_vm::fail_after = prefix;
        int status = invoke(TEST_VA + PAGE, TEST_VA + 2 * PAGE);
        host_fork_vm::fail_after = -1;
        if (status == 0) {
            completed = true;
            removed(TEST_VA + PAGE, TEST_VA + 2 * PAGE);
            break;
        }
        EXPECT_EQ(status, -ENOMEM);
        EXPECT_GT(host_fork_vm::metadata_allocations, TAKEN);
        for (auto const* allocation : host_fork_vm::metadata_live) EXPECT_EQ(allocation, nullptr);
        EXPECT_EQ(actor.lifetime_refs, 1U);
        EXPECT_EQ(sibling.lifetime_refs, 1U);
        unchanged();
    }
    EXPECT_TRUE(completed);
}
TEST_F(ManagedAnonymousRemove, TableDenialReturnsPreparedMetadataWithoutPteChanges) {
    seed();
    fail_table_at = table_attempt;
    EXPECT_EQ(invoke(TEST_VA, TEST_VA + PAGE), -ENOMEM);
    fail_table_at = SIZE_MAX;
    unchanged();
    EXPECT_EQ(actor.lifetime_refs, 1U);
    EXPECT_EQ(sibling.lifetime_refs, 1U);
}
}  // namespace
