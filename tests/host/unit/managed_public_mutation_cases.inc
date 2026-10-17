// Whole actual public dispatch/delegate/geometry bodies. Existing finite ports
// model AS/VFS/physical/scheduler/hardware decisions; no native result follows.
namespace {
class ManagedPublicMutation : public ManagedFileFixed {
   protected:
    enum class Call { Free, Protect, RawAnon, MapAnon, File };
    void TearDown() override {
        host_public::active = false;
        host_range::interrupts = true;
        ManagedFileFixed::TearDown();
    }
    auto request(Call kind, uint64_t start, uint64_t length, uint64_t prot = 3,
                 uint64_t flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, uint64_t offset = 0, unsigned publications = 2,
                 bool incoming_if = true) -> uint64_t {
        expected(start, start + ((length + PAGE - 1) & ~(PAGE - 1)));
        unsigned const pubs = host_fork_vm::pubs;
        host_mixed::expected_pubs = pubs + publications;
        host_fixed::active = kind == Call::RawAnon || kind == Call::MapAnon || kind == Call::File;
        host_protect::active = kind != Call::Free;
        host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
        host_file::active = host_file::in_operation = kind == Call::File;
        host_public::active = true;
        huge_check_free = false;
        host_range::interrupts = incoming_if;
        uint64_t result = 0;
        switch (kind) {
            case Call::Free:
                result = vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::ANON_FREE), start, length, 0, 0);
                break;
            case Call::Protect:
                result = vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::PROTECT), start, length, prot, 0);
                break;
            case Call::RawAnon:
                result = vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::ANON_ALLOCATE), start, length, prot, flags);
                break;
            case Call::MapAnon:
                result = vmem::sys_vmem_map(start, length, prot, flags | ker::abi::vmem::MAP_ANONYMOUS, UINT64_MAX, UINT64_MAX);
                break;
            case Call::File:
                result = vmem::sys_vmem_map(start, length, prot, flags, 7, offset);
                break;
        }
        EXPECT_EQ(host_range::interrupts, incoming_if);
        host_range::interrupts = true;
        host_public::active = false;
        host_file::active = host_file::in_operation = false;
        host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
        host_fork_vm::fail_after = -1;
        EXPECT_EQ(host_fork_vm::pubs, pubs + publications);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        EXPECT_EQ(publication_depth, 0U);
        EXPECT_TRUE(host_fork_vm::ordinary());
        if (static_cast<int64_t>(result) != -EOPNOTSUPP)
            for (unsigned i = 0; i < leaves; ++i)
                if ((slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128) == 0) {
                    mapped[i] = false;
                    cleanup[i].reset();
                }
        return result;
    }
};

TEST_F(ManagedPublicMutation, RawFreeDemotesBeforeUnmapUnderFinalPublication) {
    seed();
    ASSERT_EQ(request(Call::Free, TEST_VA + 13 * PAGE, PAGE, 3, ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, 0, 2, false), 0U);
    EXPECT_EQ(raw(TEST_VA + 13 * PAGE), 0U);
    EXPECT_EQ(raw(TEST_VA + 14 * PAGE) & virt::PTE_FRAME_MASK, physical[0] + 14 * PAGE);
    EXPECT_EQ(model.live_refs(), 511U);
    EXPECT_EQ(host_managed_remove::unmap_acks, 1U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 1U);
}
TEST_F(ManagedPublicMutation, PublicProtectDemotesBothLeavesAndPreservesOutsideFlags) {
    seed(2);
    ASSERT_EQ(request(Call::Protect, TEST_VA + LARGE - PAGE, 2 * PAGE, ker::abi::vmem::PROT_READ), 0U);
    EXPECT_EQ(raw(TEST_VA + LARGE - PAGE) & 2U, 0U);
    EXPECT_EQ(raw(TEST_VA + LARGE) & 2U, 0U);
    EXPECT_NE(raw(TEST_VA + LARGE + PAGE) & 2U, 0U);
    EXPECT_GT(host_protect::acks, 0U);
    EXPECT_EQ(model.live_refs(), 1024U);
}
TEST_F(ManagedPublicMutation, RawAnonymousFixedReplacesOnlyRequestedPage) {
    seed();
    auto const address = TEST_VA + 7 * PAGE;
    ASSERT_EQ(request(Call::RawAnon, address, PAGE, ker::abi::vmem::PROT_READ), address);
    EXPECT_NE(raw(address) & virt::PTE_FRAME_MASK, physical[0] + 7 * PAGE);
    EXPECT_EQ(raw(address + PAGE) & virt::PTE_FRAME_MASK, physical[0] + 8 * PAGE);
    EXPECT_EQ(host_fixed::admissions, 2U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 1U);
}
TEST_F(ManagedPublicMutation, AnonymousMapIgnoresFdAndUsesPublicFixedGeometry) {
    seed();
    auto const address = TEST_VA + PAGE;
    ASSERT_EQ(request(Call::MapAnon, address, PAGE, ker::abi::vmem::PROT_NONE), address);
    EXPECT_EQ(raw(address), 0U);
    EXPECT_EQ(covering(actor, address), 1U);
    EXPECT_EQ(host_file::fd_lookups + host_file::stats, 0U);
    EXPECT_EQ(model.live_refs(), 511U);
}
TEST_F(ManagedPublicMutation, FileFixedKeepsOneDescriptionAcrossPreparationAndFdReuse) {
    seed();
    host_file::reuse_fd = true;
    host_file::max_read = 257;
    auto const address = TEST_VA + 9 * PAGE;
    ASSERT_EQ(request(Call::File, address, PAGE, 3, ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, PAGE), address);
    contents(address, PAGE);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(host_file::outer_closes, 1U);
    EXPECT_EQ(host_file::fd_file, &replacement_file);
    EXPECT_EQ(source_file.refcount.load(), 0);
    EXPECT_EQ(source_file.pos, 17);
    EXPECT_EQ(host_fixed::admissions, 2U);
}
TEST_F(ManagedPublicMutation, FastFourKNeedsNoPreparedTablesOrSecondPublication) {
    auto const address = TEST_VA + 3 * LARGE;
    normal(address, 0x68000000);
    auto const attempts = table_attempt;
    fail_table_at = table_attempt;
    EXPECT_EQ(request(Call::Free, address, PAGE, 3, 0, 0, 1), 0U);
    fail_table_at = SIZE_MAX;
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(ManagedPublicMutation, LongCountIsAllocationFreeBeforeWriteRestrictionRefusal) {
    for (uint64_t i = 0; i < 20; ++i) {
        auto const address = TEST_VA + i * LARGE;
        auto* path = runtime_root;
        for (int level = 4; level > 1; --level) {
            auto& entry = word(path, virt::index_of(address, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            path = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
    }
    normal(TEST_VA + 19 * LARGE, 0x68001000, 0x8000000000000005ULL | mm::paging::PAGE_SHARED | mm::paging::PAGE_WRITE_RESTRICTED);
    auto const attempts = table_attempt;
    fail_table_at = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, TEST_VA, 20 * LARGE, 3, 0, 0, 4)), -EACCES);
    fail_table_at = SIZE_MAX;
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(host_protect::acks, 0U);
    EXPECT_EQ(model.live_refs(), 1U);
    EXPECT_GT(host_range::yields, 0U);
}
TEST_F(ManagedPublicMutation, LegacyFileAuthorizationPrecedesSplitAllocation) {
    mixed_seed();
    file.open_flags = 0;
    auto const attempts = table_attempt;
    fail_table_at = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, TEST_VA + LARGE - PAGE, 6 * PAGE, 3, 0, 0, 1)), -EACCES);
    fail_table_at = SIZE_MAX;
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 128U);
    EXPECT_EQ(host_protect::acks, 0U);
}
TEST_F(ManagedPublicMutation, UnknownPml4PsRefusesBeforeWriteWalker) {
    normal(TEST_VA, 0x68002000);
    auto& entry = word(runtime_root, virt::index_of(TEST_VA, 4));
    uint64_t const original = entry;
    entry = original | 128;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, TEST_VA, PAGE, 3, 0, 0, 1)), -EOPNOTSUPP);
    entry = original;
    EXPECT_EQ(raw(TEST_VA) & virt::PTE_FRAME_MASK, 0x68002000U);
    EXPECT_EQ(host_protect::acks, 0U);
}
TEST_F(ManagedPublicMutation, UnknownOneGigabyteAndParentSoftwareRefuseWithoutMutation) {
    normal(TEST_VA, 0x68003000);
    auto* pml3 = reinterpret_cast<mm::paging::PageTable*>(word(runtime_root, virt::index_of(TEST_VA, 4)) & virt::PTE_FRAME_MASK);
    auto& lower = word(pml3, virt::index_of(TEST_VA, 3));
    uint64_t const saved_lower = lower;
    lower |= 128;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, TEST_VA, PAGE, 3, 0, 0, 1)), -EOPNOTSUPP);
    lower = saved_lower;
    auto& upper = word(runtime_root, virt::index_of(TEST_VA, 4));
    uint64_t const saved_upper = upper;
    upper |= uint64_t{1} << 52;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, TEST_VA, PAGE, 3, 0, 0, 1)), -EOPNOTSUPP);
    upper = saved_upper;
    EXPECT_EQ(host_protect::acks, 0U);
}
TEST_F(ManagedPublicMutation, FinalGeometryRejectsEpochChangedDuringPreparation) {
    seed();
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    vmem::PublicRangeMutation mutation(actor, &preparation, TEST_VA, TEST_VA + PAGE);
    {
        vmem::SharedVmemPublicationGuard publication;
        auto const first = mutation.inspect(publication);
        ASSERT_EQ(first.stage, vmem::MutationStage::NeedDemotion);
    }
    ++runtime_serial;
    {
        vmem::SharedVmemPublicationGuard publication;
        auto const stale_root = mutation.inspect(publication);
        EXPECT_EQ(stale_root.stage, vmem::MutationStage::NeedGeometry);
    }
    --runtime_serial;
    virt::advance_ps_geometry_epoch_locked(runtime_root);
    ASSERT_EQ(mutation.prepare_next(vmem::MutationStage::NeedDemotion), 0);
    {
        vmem::SharedVmemPublicationGuard publication;
        auto const final = mutation.inspect(publication);
        EXPECT_EQ(final.stage, vmem::MutationStage::Complete);
        EXPECT_EQ(static_cast<int64_t>(final.result), -EAGAIN);
    }
    unchanged();
}
TEST_F(ManagedPublicMutation, SplitAllocationFailureKeepsOldDestinationIntact) {
    seed();
    fail_table_at = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Free, TEST_VA + PAGE, PAGE, 3, 0, 0, 1)), -ENOMEM);
    fail_table_at = SIZE_MAX;
    unchanged();
    EXPECT_EQ(host_managed_remove::unmap_acks + host_managed_remove::unmap_drops, 0U);
}
TEST_F(ManagedPublicMutation, OriginalTaskSizeFdAndLayoutRefusalsStayBeforePreparation) {
    seed();
    auto const attempts = table_attempt;
    host_range::current = nullptr;
    EXPECT_EQ(static_cast<int64_t>(vmem::sys_vmem_map(TEST_VA, PAGE, 3, 0, UINT64_MAX, 0)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::ANON_FREE), 0, 0, 0, 0)), -EFAULT);
    host_range::current = &actor;
    actor.native_file_mapping_retirement.store(reinterpret_cast<void*>(uintptr_t{1}));
    EXPECT_EQ(static_cast<int64_t>(vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::ANON_FREE), 0, 0, 0, 0)), -EBUSY);
    actor.native_file_mapping_retirement.store(nullptr);
    EXPECT_EQ(static_cast<int64_t>(request(Call::RawAnon, TEST_VA, 0, 3, 16, 0, 0)), -EINVAL);
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(static_cast<int64_t>(request(Call::Free, TEST_VA + PAGE, PAGE, 3, 0, 0, 1)), -EINVAL);
    EXPECT_EQ(table_attempt, attempts);
    unchanged();
}
TEST_F(ManagedPublicMutation, AddressAdmissionStillRefusesBeforeDestinationDemotion) {
    seed();
    host_fixed::admit = false;
    auto const attempts = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, TEST_VA + PAGE, PAGE, 3, 16, 0, 1)), -ENOMEM);
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(host_file::reads, 0U);
    unchanged();
}
TEST_F(ManagedPublicMutation, FixedHintZeroStaysNonfixedAndProtectZeroRetainsOldSemantics) {
    auto const address = request(Call::MapAnon, 0, PAGE, ker::abi::vmem::PROT_NONE, 16, 0, 1);
    EXPECT_GE(address, vmem::MMAP_START);
    EXPECT_EQ(covering(actor, address), 1U);
    EXPECT_EQ(request(Call::Protect, 0, PAGE, ker::abi::vmem::PROT_READ, 0, 0, 1), 0U);
    EXPECT_EQ(host_file::fd_lookups, 0U);
}
TEST_F(ManagedPublicMutation, OrdinaryFileReadFailureKeepsBenignDemotionAndRealRollback) {
    seed();
    host_file::read_error = -EIO;
    auto const address = TEST_VA + 5 * PAGE;
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, PAGE)), -EFAULT);
    EXPECT_EQ(raw(address), 0U);
    EXPECT_EQ(raw(address + PAGE) & virt::PTE_FRAME_MASK, physical[0] + 6 * PAGE);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(model.live_refs(), 511U);
    EXPECT_EQ(host_fixed::rollback_acks, 0U);  // Read refused before publishing its new page.
}
}  // namespace
