// Whole actual public dispatch/delegate/geometry bodies. Existing finite ports
// model AS/VFS/physical/scheduler/hardware decisions; no native result follows.
namespace {
class ManagedPublicMutation : public ManagedFileFixed {
   protected:
    enum class Call { Free, Protect, RawAnon, MapAnon, File };
    // New content oracles stop before the old raw()/frame() helper can follow
    // an absent page-table path. A missing mapping remains a test failure.
    void checked_contents(uint64_t address, uint64_t offset) {
        auto* leaf = virt::leaf_entry(runtime_root, address);
        ASSERT_NE(leaf, nullptr);
        ASSERT_NE(virt::pte_raw(*leaf) & mm::paging::PAGE_PRESENT, 0U);
        contents(address, offset);
    }
    auto observed_raw(uint64_t address) -> uint64_t {
        auto* leaf = virt::leaf_entry(runtime_root, address);
        return leaf ? virt::pte_raw(*leaf) : 0;
    }
    void SetUp() override {
        ManagedFileFixed::SetUp();
        host_public::actual_reader_admission = false;
        host_accounting::enabled = false;
        host_accounting::pte_reads = host_accounting::reader_cas = host_accounting::policy_locks = 0;
        host_accounting::on_reader_cas = nullptr;
        host_range::run_queues = true;
        host_range::wrong_return_task = host_range::ptrace_effect = false;
        virt::user_pagemap_access_gates[0].teardown.store(false);
        virt::user_pagemap_access_gates[0].readers.saturated = false;
        virt::user_pagemap_access_gates[0].readers.close_after_cas = nullptr;
    }
    void TearDown() override {
        host_public::actual_reader_admission = false;
        host_accounting::enabled = false;
        host_accounting::on_reader_cas = nullptr;
        host_range::run_queues = true;
        host_range::wrong_return_task = host_range::ptrace_effect = false;
        virt::user_pagemap_access_gates[0].teardown.store(false);
        virt::user_pagemap_access_gates[0].readers.saturated = false;
        virt::user_pagemap_access_gates[0].readers.close_after_cas = nullptr;
        range_yield_action = {};
        host_public::active = false;
        host_range::interrupts = true;
        ManagedFileFixed::TearDown();
    }
    void actual_accounting() {
        host_accounting::enabled = host_public::actual_reader_admission = true;
        host_accounting::pte_reads = host_accounting::reader_cas = host_accounting::policy_locks = 0;
        source_file.stat.st_size = 0x111000;
    }
    void owner_free_accounting_boundary() {
        EXPECT_EQ(publication_depth + root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(actor.lifetime_refs, 1U);
        EXPECT_FALSE(actor.resource_lock.held);
        EXPECT_TRUE(host_range::interrupts);
        EXPECT_EQ(host_file::reads + host_fixed::allocation_calls + host_managed_remove::unmap_drops, 0U);
    }
    auto request(Call kind, uint64_t start, uint64_t length, uint64_t prot = 3,
                 uint64_t flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, uint64_t offset = 0, unsigned publications = 2,
                 bool incoming_if = true) -> uint64_t {
        expected(start, start + ((length + PAGE - 1) & ~(PAGE - 1)));
        unsigned const pubs = host_fork_vm::pubs;
        host_mixed::expected_pubs = publications == UINT_MAX ? UINT_MAX : pubs + publications;
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
        if (publications != UINT_MAX) EXPECT_EQ(host_fork_vm::pubs, pubs + publications);
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
TEST_F(ManagedPublicMutation, EmptyAndInteriorProtectionHolesHaveNoEffects) {
    auto const address = TEST_VA + 4 * LARGE;
    for (uint64_t prot : {uint64_t{0}, uint64_t{1}, uint64_t{3}}) {
        EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, address, 3 * PAGE, prot, 0, 0, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(observed_raw(address), 0U);
    }
    normal(address, 0x6a000000);
    normal(address + 2 * PAGE, 0x6a002000);
    auto const first = observed_raw(address), last = observed_raw(address + 2 * PAGE);
    auto const attempts = table_attempt;
    for (uint64_t prot : {uint64_t{0}, uint64_t{1}, uint64_t{3}}) {
        EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, address, 3 * PAGE, prot, 0, 0, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, address - PAGE, 2 * PAGE, prot, 0, 0, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, address + 2 * PAGE, 2 * PAGE, prot, 0, 0, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(observed_raw(address), first);
        EXPECT_EQ(observed_raw(address + 2 * PAGE), last);
    }
    EXPECT_TRUE(actor.lazy_vmem_ranges.empty());
    EXPECT_TRUE(sibling.lazy_vmem_ranges.empty());
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(host_protect::acks, 0U);
    EXPECT_EQ(model.live_refs(), 2U);
}
TEST_F(ManagedPublicMutation, ResidentLazyNoneAndReservationUnionIsMapped) {
    auto const address = TEST_VA + 4 * LARGE;
    normal(address, 0x6a010000);
    normal(address + 3 * PAGE, 0x6a013000, mm::paging::PAGE_PRESENT | mm::paging::PAGE_NX);
    for (auto* subject : {&actor, &sibling}) {
        subject->shares_user_pagemap = true;
        row(*subject, address + PAGE, address + 3 * PAGE);
    }
    auto* reserved = virt::leaf_entry(runtime_root, address + 4 * PAGE);
    ASSERT_NE(reserved, nullptr);
    *reserved = virt::pte_from_raw(mm::paging::PAGE_RESERVED | mm::paging::PAGE_COW);
    EXPECT_EQ(request(Call::Protect, address, 5 * PAGE, 0, 0, 0, UINT_MAX), 0U);
    EXPECT_EQ(observed_raw(address) & mm::paging::PAGE_USER, 0U);
    EXPECT_EQ(observed_raw(address + 3 * PAGE) & mm::paging::PAGE_USER, 0U);
    EXPECT_EQ(observed_raw(address + 4 * PAGE) & mm::paging::PAGE_RESERVED, mm::paging::PAGE_RESERVED);
    ASSERT_EQ(actor.lazy_vmem_ranges.size(), 1U);
    ASSERT_EQ(sibling.lazy_vmem_ranges.size(), 1U);
    EXPECT_EQ(actor.lazy_vmem_ranges[0].prot, 0U);
    EXPECT_EQ(sibling.lazy_vmem_ranges[0].prot, 0U);
    EXPECT_EQ(model.live_refs(), 2U);
}
TEST_F(ManagedPublicMutation, GapBeforeLazySuffixPreservesRowsAndPte) {
    auto const address = TEST_VA + 4 * LARGE;
    normal(address, 0x6a020000);
    for (auto* subject : {&actor, &sibling}) {
        subject->shares_user_pagemap = true;
        row(*subject, address + 2 * PAGE, address + 4 * PAGE);
    }
    auto const before = observed_raw(address);
    auto const attempts = table_attempt;
    for (uint64_t prot : {uint64_t{0}, uint64_t{1}, uint64_t{3}}) {
        EXPECT_EQ(static_cast<int64_t>(request(Call::Protect, address, 4 * PAGE, prot, 0, 0, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(observed_raw(address), before);
        ASSERT_EQ(actor.lazy_vmem_ranges.size(), 1U);
        ASSERT_EQ(sibling.lazy_vmem_ranges.size(), 1U);
        EXPECT_EQ(actor.lazy_vmem_ranges[0].prot, 3U);
        EXPECT_EQ(sibling.lazy_vmem_ranges[0].prot, 3U);
    }
    EXPECT_EQ(table_attempt, attempts);
    EXPECT_EQ(host_protect::acks, 0U);
    EXPECT_EQ(model.live_refs(), 1U);
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
    // Low addresses retain the existing geometry semantics when logically
    // mapped; an empty address interval is covered by the ENOMEM cases.
    row(actor, 0, PAGE);
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

// Actual public file mmap plus actual shared CAS admission. Only filesystem,
// physical pages and the single colliding-stripe storage are finite Host ports.
// The foreign root is never dereferenced; its production hash alone selects
// the same stripe. A legal external owner retires only after the stated number
// of safe ordinary cooperations, deliberately longer than an arbitrary two yields.
TEST_F(ManagedPublicMutation, UncontendedThreePagePrivateFileUsesActualReaderProtocol) {
    auto const address = TEST_VA + 0x10e000;
    // This finite map_page endpoint consumes an existing ordinary PT path.
    normal(address - PAGE, 0x68000000);
    normal(address + 3 * PAGE, 0x68001000);
    host_public::actual_reader_admission = true;
    source_file.stat.st_size = 0x111000;
    ASSERT_EQ(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, 1), address);
    for (unsigned i = 0; i < 3; ++i) checked_contents(address + i * PAGE, 0x10e000 + i * PAGE);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(host_file::outer_closes, 0U);
    EXPECT_EQ(source_file.refcount.load(), 1);
    EXPECT_EQ(host_range::yields, 0U);
}
TEST_F(ManagedPublicMutation, ThreePagePrivateFileReaderSaturationIsTerminalNotForeignBusy) {
    auto const address = TEST_VA + 0x10e000;
    // This finite map_page endpoint consumes an existing ordinary PT path.
    normal(address - PAGE, 0x68000000);
    normal(address + 3 * PAGE, 0x68001000);
    host_public::actual_reader_admission = true;
    auto& gate = virt::user_pagemap_access_gates[0];
    source_file.stat.st_size = 0x111000;
    gate.readers.saturated = true;
    EXPECT_FALSE(gate.teardown.load());
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -EAGAIN);
    EXPECT_EQ(host_range::yields, 0U);
    EXPECT_EQ(host_file::reads, 0U);
    EXPECT_EQ(observed_raw(address), 0U);
    EXPECT_EQ(observed_raw(address + PAGE), 0U);
    EXPECT_EQ(observed_raw(address + 2 * PAGE), 0U);
}
TEST_F(ManagedPublicMutation, ThreePagePrivateFileForeignStripeNeedsActualOwnerProgress) {
    auto* foreign = reinterpret_cast<mm::paging::PageTable*>(reinterpret_cast<uintptr_t>(runtime_root) + (4096ULL << 12));
    ASSERT_NE(foreign, runtime_root);
    ASSERT_EQ(host_public_hash::user_pagemap_access_gate_index(foreign), host_public_hash::user_pagemap_access_gate_index(runtime_root));
    host_public::actual_reader_admission = true;
    source_file.stat.st_size = 0x111000;
    unsigned vector = 0;
    for (unsigned release_after : {3U, 7U, 17U}) {
        SCOPED_TRACE(release_after);
        auto const address = TEST_VA + 0x10e000 + vector++ * 8 * PAGE;
        normal(address - PAGE, 0x68000000 + vector * 2 * PAGE);
        normal(address + 3 * PAGE, 0x68001000 + vector * 2 * PAGE);
        auto const before_left = observed_raw(address - PAGE), before_right = observed_raw(address + 3 * PAGE);
        auto& gate = virt::user_pagemap_access_gates[0];
        gate.teardown.store(true);
        auto const yields_before = host_range::yields;
        unsigned cooperations = 0;
        range_yield_action = [&] {
            EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
            EXPECT_TRUE(host_range::interrupts);
            if (++cooperations == release_after) gate.teardown.store(false);
        };
        // Do not turn the bug into an accepted result: the original code must
        // fail this positive regression with -EAGAIN before external progress.
        auto const result = request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX);
        EXPECT_EQ(result, address);
        EXPECT_GE(cooperations, release_after);
        EXPECT_GE(host_range::yields - yields_before, release_after);
        if (result == address) {
            for (unsigned i = 0; i < 3; ++i) checked_contents(address + i * PAGE, 0x10e000 + i * PAGE);
        } else {
            EXPECT_EQ(static_cast<int64_t>(result), -EAGAIN);
            EXPECT_EQ(observed_raw(address) | observed_raw(address + PAGE) | observed_raw(address + 2 * PAGE), 0U);
        }
        EXPECT_EQ(observed_raw(address - PAGE), before_left);
        EXPECT_EQ(observed_raw(address + 3 * PAGE), before_right);
        // This is fixture cleanup after the observed result, not a fabricated
        // successful cooperation or production gate release by the reader.
        range_yield_action = {};
        gate.teardown.store(false);
    }
}
TEST_F(ManagedPublicMutation, ThreePagePrivateFileWaitRefusesClosingOrChangedContext) {
    auto const address = TEST_VA + 0x10e000;
    normal(address - PAGE, 0x69000000);
    normal(address + 3 * PAGE, 0x69001000);
    host_public::actual_reader_admission = true;
    source_file.stat.st_size = 0x111000;
    for (unsigned axis = 0; axis < 2; ++axis) {
        SCOPED_TRACE(axis);
        auto& gate = virt::user_pagemap_access_gates[0];
        gate.teardown.store(true);
        unsigned cooperations = 0;
        range_yield_action = [&] {
            EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
            EXPECT_TRUE(host_range::interrupts);
            ++cooperations;
            if (axis == 0)
                actor.usercopy_pagemap_closing.store(true);
            else
                actor.scheduler_transition_active.store(true);
        };
        auto const reads_before = host_file::reads;
        EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -EINVAL);
        EXPECT_EQ(cooperations, 1U);
        EXPECT_EQ(host_file::reads, reads_before);
        EXPECT_EQ(observed_raw(address) | observed_raw(address + PAGE) | observed_raw(address + 2 * PAGE), 0U);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        actor.usercopy_pagemap_closing.store(false);
        actor.scheduler_transition_active.store(false);
        range_yield_action = {};
        gate.teardown.store(false);
    }
}
TEST_F(ManagedPublicMutation, ThreePagePrivateFileRacedReaderRollsBackBeforeWait) {
    auto const address = TEST_VA + 0x10e000;
    normal(address - PAGE, 0x69000000);
    normal(address + 3 * PAGE, 0x69001000);
    host_public::actual_reader_admission = true;
    source_file.stat.st_size = 0x111000;
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.readers.close_after_cas = &gate.teardown;
    unsigned cooperations = 0;
    range_yield_action = [&] {
        // The real CAS succeeded, then observed teardown and rolled back.
        // A retained striped reader here would block its exclusive owner.
        EXPECT_EQ(root_readers + cow_depth + pin_depth + backend().preempt_depth, 0U);
        EXPECT_TRUE(host_range::interrupts);
        EXPECT_EQ(gate.readers.close_after_cas, nullptr);
        if (++cooperations == 7) gate.teardown.store(false);
    };
    auto const result = request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX);
    ASSERT_EQ(result, address);
    EXPECT_EQ(cooperations, 7U);
    for (unsigned i = 0; i < 3; ++i) checked_contents(address + i * PAGE, 0x10e000 + i * PAGE);
    EXPECT_EQ(root_readers, 0U);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    range_yield_action = {};
}

// Opt-in actual typed count, full/replaced PTE walk and resource::State policy.
// Existing one-collision atomics, Task owner/lock and physical endpoints remain
// explicit Host seams; no native progress or error-cause claim follows.
TEST_F(ManagedPublicMutation, AccountingInitialBusyNeverWalksOrMutatesBeforeOwnerFreeCooperation) {
    auto const address = TEST_VA + 0x10e000;
    normal(address - PAGE, 0x69000000);
    normal(address + 3 * PAGE, 0x69001000);
    auto& gate = virt::user_pagemap_access_gates[0];
    auto* foreign = reinterpret_cast<mm::paging::PageTable*>(reinterpret_cast<uintptr_t>(runtime_root) + (4096ULL << 12));
    ASSERT_NE(foreign, runtime_root);
    ASSERT_EQ(host_public_hash::user_pagemap_access_gate_index(foreign), host_public_hash::user_pagemap_access_gate_index(runtime_root));
    actual_accounting();
    gate.teardown.store(true);
    unsigned cooperations = 0;
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        EXPECT_EQ(host_accounting::pte_reads, 0U);
        EXPECT_EQ(host_accounting::policy_locks, 0U);
        EXPECT_EQ(observed_raw(address), 0U);
        if (++cooperations == 3) gate.teardown.store(false);
    };
    ASSERT_EQ(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX), address);
    EXPECT_EQ(cooperations, 3U);
    EXPECT_GT(host_accounting::pte_reads, 0U);
    for (unsigned i = 0; i < 3; ++i) checked_contents(address + i * PAGE, 0x10e000 + i * PAGE);
    range_yield_action = {};
}
TEST_F(ManagedPublicMutation, AccountingRacedReaderRollsBackBeforeOwnerFreeRetry) {
    auto const address = TEST_VA + 0x10e000;
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.readers.close_after_cas = &gate.teardown;
    unsigned cooperations = 0;
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        EXPECT_EQ(host_accounting::pte_reads, 0U);
        EXPECT_EQ(gate.readers.close_after_cas, nullptr);
        ++cooperations;
        gate.teardown.store(false);
    };
    ASSERT_EQ(request(Call::MapAnon, address, PAGE, ker::abi::vmem::PROT_NONE, 0x12, 0, UINT_MAX, false), address);
    EXPECT_EQ(cooperations, 1U);
    EXPECT_EQ(host_accounting::policy_locks, 1U);
    range_yield_action = {};
}
TEST_F(ManagedPublicMutation, AccountingReplacedBusyDiscardsTheEarlierWholeCount) {
    auto const address = TEST_VA + 0x10e000;
    normal(address, 0x69000000);
    auto const original = observed_raw(address);
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    host_accounting::on_reader_cas = [] {
        if (host_accounting::reader_cas == 2)
            virt::user_pagemap_access_gates[0].readers.close_after_cas = &virt::user_pagemap_access_gates[0].teardown;
    };
    constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
    actor.resources.entries[AS].limit.soft = 3 * PAGE;
    unsigned cooperations = 0;
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        EXPECT_GT(host_accounting::pte_reads, 0U);
        EXPECT_EQ(host_accounting::policy_locks, 0U);
        EXPECT_EQ(observed_raw(address), original);
        ++cooperations;
        actor.lazy_vmem_ranges.push_back({address + 16 * PAGE, address + 20 * PAGE});
        gate.teardown.store(false);
    };
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -ENOMEM);
    EXPECT_EQ(cooperations, 1U);
    EXPECT_EQ(host_accounting::policy_locks, 1U);
    EXPECT_EQ(actor.resources.entries[AS].denied, 1U);
    EXPECT_EQ(observed_raw(address), original);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(host_file::reads, 0U);
    host_accounting::on_reader_cas = nullptr;
    range_yield_action = {};
}
TEST_F(ManagedPublicMutation, AccountingRetryKeepsTheOriginallyCapturedFileAcrossFdReuse) {
    auto const address = TEST_VA + 0x10e000;
    normal(address - PAGE, 0x69000000);
    normal(address + 3 * PAGE, 0x69001000);
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.teardown.store(true);
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        EXPECT_EQ(host_file::fd_lookups, 1U);
        EXPECT_EQ(host_file::stats, 1U);
        ASSERT_EQ(host_file::fd_file, &source_file);
        host_file::fd_file = &replacement_file;
        ker::vfs::vfs_put_file(&source_file);
        gate.teardown.store(false);
    };
    ASSERT_EQ(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX), address);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(host_file::outer_closes, 1U);
    EXPECT_EQ(source_file.refcount.load(), 0);
    EXPECT_EQ(source_file.pos, 17);
    for (unsigned i = 0; i < 3; ++i) checked_contents(address + i * PAGE, 0x10e000 + i * PAGE);
    range_yield_action = {};
}
TEST_F(ManagedPublicMutation, AccountingPersistentBusyStopsAfterSixtyFourOwnerFreeYields) {
    auto const address = TEST_VA + 0x10e000;
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.teardown.store(true);
    unsigned cooperations = 0;
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        EXPECT_EQ(host_accounting::pte_reads, 0U);
        ++cooperations;
    };
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -EAGAIN);
    EXPECT_EQ(cooperations, 64U);
    EXPECT_EQ(host_fixed::admissions, 65U);
    EXPECT_EQ(host_accounting::policy_locks, 0U);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_EQ(observed_raw(address), 0U);
    gate.teardown.store(false);
    range_yield_action = {};
}
TEST_F(ManagedPublicMutation, AccountingBusyRefusesPtraceAndUnsafeReturnContexts) {
    auto const address = TEST_VA + 0x10e000;
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.teardown.store(true);
    for (unsigned axis = 0; axis < 10; ++axis) {
        SCOPED_TRACE(axis);
        actor.usercopy_pagemap_closing.store(axis == 0);
        actor.scheduler_transition_active.store(axis == 1);
        host_range::wrong_return_task = axis == 2;
        host_range::run_queues = axis != 3;
        host_range::ptrace_effect = axis == 4;
        actor.deferred_task_switch = axis == 5;
        actor.wants_block = axis == 6;
        actor.process_exit_requested.store(axis == 7);
        actor.has_exited = axis == 8;
        actor.voluntary_blocked = axis == 9;
        auto const yields = host_range::yields;
        EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -EWOULDBLOCK);
        EXPECT_EQ(host_range::yields, yields);
        EXPECT_EQ(host_accounting::pte_reads, 0U);
    }
    actor.usercopy_pagemap_closing.store(false);
    actor.scheduler_transition_active.store(false);
    actor.process_exit_requested.store(false);
    actor.deferred_task_switch = actor.wants_block = actor.has_exited = actor.voluntary_blocked = false;
    host_range::wrong_return_task = host_range::ptrace_effect = false;
    host_range::run_queues = true;
    gate.teardown.store(false);
    // These retained contexts are valid for try-only counting but cannot cross
    // the actual ordinary cooperation frontier. Use the real guards/Task lease
    // methods, not a synthetic count or a fabricated successful syscall.
    for (unsigned axis = 0; axis < 2; ++axis) {
        SCOPED_TRACE(axis);
        std::optional<ker::mod::sched::PreemptGuard> preempt;
        mm::paging::PageTable* retained = nullptr;
        if (axis == 0) preempt.emplace();
        if (axis == 1) ASSERT_TRUE(actor.try_acquire_usercopy_pagemap(retained));
        auto const yields = host_range::yields;
        {
            ker::mod::sys::SyscallPreparationScope preparation(actor);
            vmem::PublicRangeMutation mutation(actor, &preparation, address, address + 3 * PAGE);
            EXPECT_EQ(mutation.prepare_accounting(preparation), -EWOULDBLOCK);
        }
        EXPECT_EQ(host_range::yields, yields);
        if (axis == 1) actor.release_usercopy_pagemap();
        preempt.reset();
    }
}
TEST_F(ManagedPublicMutation, AccountingTrueLimitCapacityMalformedRootAndOwnerFailureAreTerminal) {
    auto const address = TEST_VA + 0x10e000;
    normal(address - PAGE, 0x69000000);
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
    for (unsigned axis = 0; axis < 4; ++axis) {
        SCOPED_TRACE(axis);
        actor.resources.entries[AS].limit.soft = axis == 0 ? 0 : UINT64_MAX;
        gate.readers.saturated = axis == 1;
        auto& top = word(runtime_root, virt::index_of(address, 4));
        auto const original = top;
        if (axis == 2) top |= 128;
        actor.resource_owner_available = axis != 3;
        auto const yields = host_range::yields;
        EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -ENOMEM);
        EXPECT_EQ(host_range::yields, yields);
        EXPECT_EQ(host_file::reads + host_fixed::allocation_calls, 0U);
        top = original;
        EXPECT_EQ(observed_raw(address), 0U);
        gate.readers.saturated = false;
        actor.resource_owner_available = true;
    }
}
TEST_F(ManagedPublicMutation, AccountingRootDisappearanceAfterCooperationRefusesWithoutMutation) {
    auto const address = TEST_VA + 0x10e000;
    actual_accounting();
    auto& gate = virt::user_pagemap_access_gates[0];
    gate.teardown.store(true);
    unsigned cooperations = 0;
    range_yield_action = [&] {
        owner_free_accounting_boundary();
        ++cooperations;
        actor.pagemap = nullptr;
        gate.teardown.store(false);
    };
    EXPECT_EQ(static_cast<int64_t>(request(Call::File, address, 3 * PAGE, 3, 0x12, 0x10e000, UINT_MAX)), -ENOMEM);
    actor.pagemap = runtime_root;
    EXPECT_EQ(cooperations, 1U);
    EXPECT_EQ(host_accounting::policy_locks, 0U);
    EXPECT_EQ(host_file::reads, 0U);
    EXPECT_EQ(observed_raw(address), 0U);
    range_yield_action = {};
}
