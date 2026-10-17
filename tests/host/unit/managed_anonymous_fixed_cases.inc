// Actual private anonymous MAP_FIXED body and caller. Backend PA allocation,
// HHDM numeric view, AS decision and hardware publication remain finite ports.
namespace {
class ManagedAnonymousFixed : public ManagedMixedProtect {
   protected:
    void SetUp() override {
        ManagedMixedProtect::SetUp();
        model.domains(2);  // Exercise real range dispatch; no UMA plain-port projection.
        host_fixed::active = false;
        host_fixed::valid_hhdm = host_fixed::admit = host_fixed::zero_cow = true;
        host_fixed::lazy = false;
        host_fixed::fail_after = -1;
        host_fixed::allocation_calls = host_fixed::admissions = host_fixed::bulk_maps = host_fixed::clock_reads =
            host_fixed::rollback_acks = 0;
        host_fixed::replacement_frames.clear();
        host_fixed::new_mappings.clear();
        huge_check_free = false;
    }
    void TearDown() override {
        host_fixed::active = false;
        host_fixed::fail_after = -1;
        host_fixed::replacement_frames.clear();
        host_fixed::new_mappings.clear();
        ManagedMixedProtect::TearDown();
    }
    auto fixed(uint64_t first, uint64_t length, uint64_t prot = ker::abi::vmem::PROT_READ,
               uint64_t flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED) -> uint64_t {
        expected(first, first + ((length + PAGE - 1) & ~(PAGE - 1)));
        host_mixed::expected_pubs = host_fork_vm::pubs + 1;
        host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
        uint64_t result;
        {
            ker::mod::sys::SyscallPreparationScope preparation(actor);
            result = vmem::detail::replace_current_anonymous_fixed(actor, preparation, first, length, prot, flags);
        }
        host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
        host_fork_vm::fail_after = -1;
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        for (unsigned i = 0; i < leaves; ++i)
            if ((slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128) == 0) {
                mapped[i] = false;
                cleanup[i].reset();
            }
        return result;
    }
    auto frame(uint64_t va) -> uint8_t* { return reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(raw(va) & virt::PTE_FRAME_MASK)); }
    void replaced_read(uint64_t first, unsigned pages) {
        for (unsigned i = 0; i < pages; ++i) {
            auto const va = first + uint64_t{i} * PAGE;
            EXPECT_EQ(raw(va) & 7, 5U);
            EXPECT_EQ(metadata_protection(actor, va), uint64_t{ker::abi::vmem::PROT_READ});
            EXPECT_EQ(metadata_protection(sibling, va), uint64_t{ker::abi::vmem::PROT_READ});
            auto* bytes = frame(va);
            ASSERT_NE(bytes, nullptr);
            EXPECT_EQ(mm::phys::page_ref_get(bytes), 1U);
            for (unsigned j = 0; j < PAGE; ++j) EXPECT_EQ(bytes[j], 0U);
        }
    }
};

TEST_F(ManagedAnonymousFixed, PrivateReadUsesSamePublicationAfterReaderDestruction) {
    seed();
    auto const address = TEST_VA + 13 * PAGE;
    ASSERT_EQ(fixed(address, PAGE), address);
    replaced_read(address, 1);
    EXPECT_EQ(raw(TEST_VA) & virt::PTE_FRAME_MASK, physical[0]);
    EXPECT_EQ(bytes[0][0], 1U);
    EXPECT_EQ(raw(address + PAGE) & virt::PTE_FRAME_MASK, physical[0] + 14 * PAGE);
    EXPECT_EQ(host_managed_remove::unmap_acks, 1U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 1U);
    EXPECT_EQ(host_protect::policy_reads, 1U);
    EXPECT_EQ(host_fixed::admissions, 1U);
    EXPECT_EQ(model.live_refs(), 512U);
}
TEST_F(ManagedAnonymousFixed, CrossLeafReplacementDemotesAllBeforeUnmap) {
    seed(2);
    auto const first = TEST_VA + LARGE - PAGE;
    ASSERT_EQ(fixed(first, 2 * PAGE), first);
    replaced_read(first, 2);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA + LARGE, 2) & 128, 0U);
    EXPECT_NE(raw(first) & virt::PTE_FRAME_MASK, raw(first + PAGE) & virt::PTE_FRAME_MASK);
    EXPECT_EQ(host_managed_remove::unmap_drops, 2U);
    EXPECT_EQ(model.live_refs(), 1024U);
}
TEST_F(ManagedAnonymousFixed, MixedFileHoleAndNoneRetainOrdinaryReplacementSemantics) {
    mixed_seed();
    auto const first = TEST_VA + LARGE - PAGE;
    ASSERT_EQ(fixed(first, 6 * PAGE), first);
    replaced_read(first, 6);
    EXPECT_EQ(host_managed_remove::unmap_drops, 5U);
    EXPECT_EQ(host_mixed::backend_calls, 1U);
    EXPECT_EQ(file.refcount.load(), 1);
    EXPECT_TRUE(vmem::g_file_mmap_ranges.empty());
    EXPECT_EQ(model.live_refs(), 517U);
}
TEST_F(ManagedAnonymousFixed, ProtNoneReplacesOldFramesWithSharedLazyMetadata) {
    seed();
    ASSERT_EQ(fixed(TEST_VA + PAGE, 2 * PAGE, ker::abi::vmem::PROT_NONE), TEST_VA + PAGE);
    EXPECT_EQ(raw(TEST_VA + PAGE), 0U);
    EXPECT_EQ(raw(TEST_VA + 2 * PAGE), 0U);
    EXPECT_EQ(metadata_protection(actor, TEST_VA + PAGE), 0U);
    EXPECT_EQ(metadata_protection(sibling, TEST_VA + PAGE), 0U);
    EXPECT_EQ(host_protect::policy_reads + host_fixed::allocation_calls, 0U);
    EXPECT_EQ(model.live_refs(), 510U);
}
TEST_F(ManagedAnonymousFixed, NoReserveKeepsExistingBackingDecision) {
    seed();
    auto const flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED | ker::abi::vmem::MAP_NORESERVE;
    ASSERT_EQ(fixed(TEST_VA, PAGE, 3, flags), TEST_VA);
    EXPECT_EQ(raw(TEST_VA), 0U);
    EXPECT_EQ(metadata_protection(actor, TEST_VA), 3U);
    EXPECT_EQ(host_protect::policy_reads + host_fixed::allocation_calls, 0U);
}
TEST_F(ManagedAnonymousFixed, WritableLazyKeepsSiblingsAndDoesNotAllocateBacking) {
    seed();
    host_fixed::lazy = true;
    ASSERT_EQ(fixed(TEST_VA, PAGE, 3), TEST_VA);
    EXPECT_EQ(raw(TEST_VA), 0U);
    EXPECT_EQ(metadata_protection(actor, TEST_VA), 3U);
    EXPECT_EQ(metadata_protection(sibling, TEST_VA), 3U);
    EXPECT_EQ(host_protect::policy_reads + host_fixed::allocation_calls, 0U);
}
TEST_F(ManagedAnonymousFixed, WritablePrivateEagerUsesDistinctFinitePages) {
    seed();
    host_fixed::zero_cow = false;
    ASSERT_EQ(fixed(TEST_VA, 2 * PAGE, 3), TEST_VA);
    EXPECT_EQ(raw(TEST_VA) & 7, 7U);
    EXPECT_EQ(raw(TEST_VA + PAGE) & 7, 7U);
    EXPECT_NE(raw(TEST_VA) & virt::PTE_FRAME_MASK, raw(TEST_VA + PAGE) & virt::PTE_FRAME_MASK);
    EXPECT_EQ(frame(TEST_VA)[0], 0U);
    EXPECT_EQ(host_protect::policy_reads, 1U);
}
TEST_F(ManagedAnonymousFixed, PopulatedWritableKeepsExistingSharedZeroAdoption) {
    seed();
    host_fixed::lazy = true;
    host_protect::supplied_zero = model.allocate(0x68000000, 1, PAGE, Initialization::ZERO);
    auto const flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED | ker::abi::vmem::MAP_POPULATE;
    ASSERT_EQ(fixed(TEST_VA, 2 * PAGE, 3, flags), TEST_VA);
    EXPECT_EQ(raw(TEST_VA) & virt::PTE_FRAME_MASK, 0x68000000U);
    EXPECT_EQ(raw(TEST_VA + PAGE) & virt::PTE_FRAME_MASK, 0x68000000U);
    EXPECT_EQ(raw(TEST_VA) & mm::paging::PAGE_COW, mm::paging::PAGE_COW);
    EXPECT_EQ(mm::phys::page_ref_get(host_protect::supplied_zero), 3U);
    EXPECT_EQ(host_fixed::bulk_maps, 1U);
    EXPECT_EQ(host_protect::policy_reads + host_fixed::allocation_calls, 0U);
}
TEST_F(ManagedAnonymousFixed, AsRefusalKeepsManagedLeafAndMetadataExact) {
    seed();
    host_fixed::admit = false;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -ENOMEM);
    unchanged();
    EXPECT_EQ(host_fixed::admissions, 1U);
    EXPECT_EQ(host_protect::map_calls + host_fixed::allocation_calls, 0U);
}
TEST_F(ManagedAnonymousFixed, InitialLayoutRefusesBeforeAsAdmissionAndDemotion) {
    seed();
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -EINVAL);
    unchanged();
    EXPECT_EQ(host_fixed::admissions, 0U);
}
TEST_F(ManagedAnonymousFixed, NativeAndInvalidHhdmRefuseWithoutReplacement) {
    seed();
    actor.native_file_mapping_retirement.store(reinterpret_cast<void*>(1));
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -EBUSY);
    actor.native_file_mapping_retirement.store(nullptr);
    host_fixed::valid_hhdm = false;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -EFAULT);
    unchanged();
    EXPECT_EQ(host_fixed::admissions, 0U);
}
TEST_F(ManagedAnonymousFixed, FixedGeometryAndCopiedPolicyRejectBeforePublication) {
    seed();
    auto const before = host_fork_vm::pubs;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, 0)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA + 1, PAGE)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(fixed(0, PAGE)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(fixed(LARGE, PAGE)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE, 1, ker::abi::vmem::MAP_PRIVATE)), -EINVAL);
    host_protect::copied_policy = {0, numa::Mode::BIND};
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -EINVAL);
    EXPECT_EQ(host_fork_vm::pubs, before);
    unchanged();
}
TEST_F(ManagedAnonymousFixed, EagerFailureDropsOnlyNewPrefixAfterActualAck) {
    seed();
    host_fixed::fail_after = 1;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, 3 * PAGE)), -ENOMEM);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(raw(TEST_VA), 0U);
    EXPECT_EQ(raw(TEST_VA + PAGE), 0U);
    EXPECT_EQ(raw(TEST_VA + 2 * PAGE), 0U);
    EXPECT_EQ(host_fixed::rollback_acks, 1U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 3U);
    EXPECT_EQ(model.live_refs(), 509U);
    EXPECT_EQ(covering(actor, TEST_VA), 0U);
    EXPECT_EQ(covering(sibling, TEST_VA), 0U);
    for (auto const& block : model.blocks)
        if (std::find(host_fixed::replacement_frames.begin(), host_fixed::replacement_frames.end(), block.physical) !=
            host_fixed::replacement_frames.end())
            for (auto refs : block.refs) EXPECT_EQ(refs, 0U);
}
TEST_F(ManagedAnonymousFixed, TablePreparationFailureLeavesAllOldOwnersIntact) {
    seed();
    fail_table_at = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(fixed(TEST_VA, PAGE)), -ENOMEM);
    fail_table_at = SIZE_MAX;
    unchanged();
    EXPECT_EQ(host_fixed::admissions, 0U);
}
TEST_F(ManagedAnonymousFixed, CountZeroHighAddressAndOriginalFourArgDelegateRemainDistinct) {
    auto const va = uint64_t{0x200000000000} + 3 * PAGE;
    reserve(va);
    // Supply existing ancestor geometry; the finite map endpoint never creates it.
    *virt::leaf_entry(runtime_root, va) = {};
    ASSERT_EQ(fixed(va, PAGE), va);
    replaced_read(va, 1);
    EXPECT_GT(va, UINT32_MAX);
    auto const next = va + PAGE;
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = true;
    host_mixed::expected_pubs = host_fork_vm::pubs + 1;
    host_managed_remove::expected_unmaps.clear();
    // MAP_FIXED hint0 follows the original nonfixed reservation path.
    actor.mmap_next.store(next);
    EXPECT_EQ(vmem::anon_allocate(0, PAGE, ker::abi::vmem::PROT_NONE, ker::abi::vmem::MAP_FIXED), next);
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = false;
    EXPECT_EQ(metadata_protection(actor, next), 0U);
    EXPECT_EQ(metadata_protection(sibling, next), 0U);
}
TEST_F(ManagedAnonymousFixed, PreparedRecordRejectsDifferentGeometryAndReuse) {
    seed();
    expected(TEST_VA, TEST_VA + PAGE);
    host_mixed::expected_pubs = host_fork_vm::pubs + 1;
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
    {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        vmem::PreparedRangeDemotion prepared(actor, preparation);
        ASSERT_EQ(prepared.prepare(TEST_VA, TEST_VA + PAGE), 0);
        auto const flags = ker::abi::vmem::MAP_FIXED | ker::abi::vmem::MAP_PRIVATE;
        EXPECT_EQ(static_cast<int64_t>(vmem::anon_allocate(TEST_VA + PAGE, PAGE, 1, flags, &prepared)), -EINVAL);
        EXPECT_EQ(vmem::anon_allocate(TEST_VA, PAGE, 1, flags, &prepared), TEST_VA);
        EXPECT_EQ(static_cast<int64_t>(vmem::anon_allocate(TEST_VA, PAGE, 1, flags, &prepared)), -EINVAL);
    }
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
    mapped[0] = false;
    cleanup[0].reset();
    EXPECT_EQ(host_fixed::admissions, 1U);
    replaced_read(TEST_VA, 1);
}
}  // namespace
