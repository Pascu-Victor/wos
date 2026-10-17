// Actual anon_free/split/lazy/registry bodies; hardware, backend, snapshot and
// AS endpoints are finite Host ports. No native TLB/VFS/usercopy result claimed.
namespace {
class ManagedMixedGeometry : public ManagedAnonymousRemove {
   protected:
    std::vector<std::pair<uint64_t, void*>> normal_pages;
    ker::vfs::FileOps operations{+[] {}};
    void SetUp() override {
        ManagedAnonymousRemove::SetUp();
        host_mixed::active = false;
        host_mixed::expected_pubs = host_mixed::backend_calls = host_mixed::snapshots_taken = host_mixed::typed_calls =
            host_mixed::final_closes = 0;
        host_mixed::backend_error = host_mixed::typed_result = 0;
        host_mixed::dirty = host_mixed::snapshot_matches = true;
        host_mixed::snapshots = {};
        host_mixed::written_offsets.clear();
        host_mixed::written_bytes.clear();
        file.fops = &operations;
        file.open_flags = 2;
        file.read_only = false;
    }
    void TearDown() override {
        host_mixed::active = false;
        host_fork_vm::fail_after = -1;
        host_mixed::snapshots = {};
        normal_pages.clear();
        ManagedAnonymousRemove::TearDown();
    }
    auto normal(uint64_t address, uint64_t physical, uint64_t flags = 0x8000000000000007ULL) -> uint8_t* {
        auto* page = static_cast<uint8_t*>(model.allocate(physical, 1, PAGE, Initialization::ZERO));
        if (!page) std::abort();
        std::memset(page, 0x5a, PAGE);
        auto* path = runtime_root;
        for (int level = 4; level > 1; --level) {
            auto& entry = word(path, virt::index_of(address, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            EXPECT_EQ(entry & 128, 0U);
            path = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        word(path, virt::index_of(address, 1)) = physical | flags;
        normal_pages.push_back({address, page});
        for (auto& given : host_mixed::snapshots)
            if (!given.page) {
                given = {address, page};
                return page;
            }
        std::abort();
    }
    void file_row(Task& task, uint64_t first, uint64_t last, uint64_t offset, bool shared = false) {
        Row value{};
        value.start = first;
        value.end = last;
        value.kind = ker::mod::sched::task::LazyVmemKind::FILE_BACKED;
        value.file = &file;
        value.file_offset = offset;
        value.prot = 3;
        value.flags = shared ? ker::abi::vmem::MAP_SHARED : ker::abi::vmem::MAP_PRIVATE;
        ASSERT_TRUE(ker::mod::sched::task::try_retain_lazy_vmem_range(value));
        ASSERT_TRUE(task.lazy_vmem_ranges.push_back(value));
    }
    void registry(uint64_t first, uint64_t length, uint64_t offset, bool writeback = true) {
        ASSERT_EQ(vmem::register_file_mmap_range(runtime_root, first, length, offset, &file, writeback), 0);
    }
    void expected(uint64_t first, uint64_t end) {
        auto& out = host_managed_remove::expected_unmaps;
        out.clear();
        for (uint64_t va = first; va < end; va += PAGE) {
            void* p = nullptr;
            for (unsigned i = 0; i < leaves; ++i)
                if (va >= TEST_VA + i * LARGE && va < TEST_VA + (i + 1) * LARGE)
                    p = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(bytes[i]) + va - TEST_VA - i * LARGE);
            for (auto const& entry : normal_pages)
                if (entry.first == va) p = entry.second;
            if (p && mm::phys::page_ref_get(p)) out.push_back({va, p});
        }
    }
    auto invoke_mixed(uint64_t first, uint64_t length) -> int {
        expected(first, first + ((length + PAGE - 1) & ~(PAGE - 1)));
        host_mixed::expected_pubs = host_fork_vm::pubs + 1;
        host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
        huge_check_free = false;
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        int status = vmem::detail::remove_current_mixed_range(actor, preparation, first, length);
        host_fork_vm::fail_after = -1;
        host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers, 0U);
        // Unsupported hierarchy inputs are restored by their test before any
        // walk. Successful/ordinary-failure demotions retire mandatory fixture
        // cleanup owners outside PUB rather than treating them as stale PS.
        if (status != -EOPNOTSUPP)
            for (unsigned i = 0; i < leaves; ++i)
                if ((slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128) == 0) {
                    mapped[i] = false;
                    cleanup[i].reset();
                }
        return status;
    }
    void mixed_seed() {
        seed();
        auto base = TEST_VA + LARGE;
        normal(base, 0x60000000);
        normal(base + 2 * PAGE, 0x60002000, 0x8000000000000001ULL);  // USER-off PROT_NONE.
        normal(base + 3 * PAGE, 0x60003000);
        normal(base + 4 * PAGE, 0x60004000, 0x8000000000000007ULL | mm::paging::PAGE_SHARED);
        file_row(actor, base + 3 * PAGE, base + 4 * PAGE, 1000);
        file_row(sibling, base + 3 * PAGE, base + 4 * PAGE, 1000);
        file_row(actor, base + 4 * PAGE, base + 5 * PAGE, 9000, true);
        file_row(sibling, base + 4 * PAGE, base + 5 * PAGE, 9000, true);
        registry(base + 4 * PAGE, PAGE - 17, 9000);
    }
    void no_ordinary_unmap() { EXPECT_EQ(host_managed_remove::unmap_acks + host_managed_remove::unmap_drops, 0U); }
    void equivalent_leaf(unsigned count = 1) {
        for (unsigned i = 0; i < count; ++i) EXPECT_EQ(slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128, 0U);
        EXPECT_EQ(model.live_refs(), uint64_t{count} * 512);
    }
};

TEST_F(ManagedMixedGeometry, MixedHugeFourKFileHoleAndNoneUsesOrdinaryContinuation) {
    mixed_seed();
    auto const base = TEST_VA + LARGE;
    ASSERT_EQ(invoke_mixed(base - PAGE, 6 * PAGE), 0);
    EXPECT_EQ(host_managed_remove::unmap_acks, 5U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 5U);
    EXPECT_EQ(model.live_refs(), 511U);
    EXPECT_EQ(file.refcount.load(), 1);
    EXPECT_TRUE(vmem::g_file_mmap_ranges.empty());
    EXPECT_EQ(host_mixed::written_offsets, (std::vector<uint64_t>{9000}));
    ASSERT_EQ(host_mixed::written_bytes.size(), PAGE - 17);
    for (auto byte : host_mixed::written_bytes) EXPECT_EQ(byte, 0x5a);
    EXPECT_EQ(covering(actor, base - PAGE), 0U);
    EXPECT_EQ(covering(sibling, base - PAGE), 0U);
    auto const raw = slot_at(runtime_root, TEST_VA, 1);
    EXPECT_EQ(raw & virt::PTE_FRAME_MASK, physical[0]);
    EXPECT_EQ(raw & 0x8000000000000007ULL, 0x8000000000000007ULL);
    EXPECT_EQ(bytes[0][0], 1);
}
TEST_F(ManagedMixedGeometry, CrossLeafDescriptorsAllDemoteBeforeOrdinaryUnmap) {
    seed(2);
    ASSERT_EQ(invoke_mixed(TEST_VA + LARGE - PAGE, 2 * PAGE), 0);
    EXPECT_EQ(model.live_refs(), 1022U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 2U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA + LARGE, 2) & 128, 0U);
}
TEST_F(ManagedMixedGeometry, SparseSiblingRowsDoNotImposePrivateAnonymousCoverage) {
    seed();
    ker::mod::sched::task::release_lazy_vmem_ranges(sibling);
    row(sibling, TEST_VA + 8 * PAGE, TEST_VA + 9 * PAGE);
    ASSERT_EQ(invoke_mixed(TEST_VA + PAGE, PAGE), 0);
    EXPECT_EQ(model.live_refs(), 511U);
    EXPECT_EQ(covering(sibling, TEST_VA + 8 * PAGE), 1U);
    EXPECT_EQ(covering(actor, TEST_VA + PAGE), 0U);
}
TEST_F(ManagedMixedGeometry, CompleteZeroSurveyAcceptsFourKAndHoles) {
    auto va = TEST_VA + 3 * LARGE;
    normal(va, 0x61000000);
    normal(va + 2 * PAGE, 0x61002000);
    ASSERT_EQ(invoke_mixed(va, 3 * PAGE), 0);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 2U);
}
TEST_F(ManagedMixedGeometry, RequestedByteLengthRoundsAndPreservesOutsidePage) {
    auto va = TEST_VA + 3 * LARGE;
    normal(va, 0x61100000);
    normal(va + PAGE, 0x61101000);
    auto* outside = normal(va + 2 * PAGE, 0x61102000);
    ASSERT_EQ(invoke_mixed(va, PAGE + 1), 0);
    EXPECT_EQ(model.live_refs(), 1U);
    EXPECT_EQ(outside[0], 0x5a);
    EXPECT_EQ(slot_at(runtime_root, va + 2 * PAGE, 1) & virt::PTE_FRAME_MASK, 0x61102000U);
}
TEST_F(ManagedMixedGeometry, UserOffManagedProtectionStillParticipatesInGeometry) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0, 0x8000000000000001ULL), huge::Error::None);
    leaves = 1;
    physical[0] = slot_at(runtime_root, TEST_VA, 2) & virt::PTE_FRAME_MASK;
    rows();
    ASSERT_EQ(invoke_mixed(TEST_VA + PAGE, PAGE), 0);
    EXPECT_EQ(model.live_refs(), 511U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 1) & 7, 1U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA + PAGE, 1), 0U);
}
TEST_F(ManagedMixedGeometry, InitialLayoutRefusesBeforeAnyDemotion) {
    seed();
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(invoke_mixed(TEST_VA + PAGE, PAGE), -EINVAL);
    unchanged();
}
TEST_F(ManagedMixedGeometry, UnknownOneGigabyteAndMalformedGeometryRefuseBeforeWrites) {
    seed();
    auto& raw = slot_at(runtime_root, TEST_VA, 2);
    auto saved = raw;
    raw += LARGE;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EOPNOTSUPP);
    raw = saved;
    auto& upper = slot_at(runtime_root, TEST_VA, 3);
    auto old = upper;
    upper = 0x40000087;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EOPNOTSUPP);
    upper = old;
    upper = 0x1000;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EOPNOTSUPP);
    upper = old;
    unchanged();
}
TEST_F(ManagedMixedGeometry, EveryPreparedTablePrefixAndHeapRefusesWithoutWrites) {
    seed(2);
    for (unsigned prefix = 0; prefix < 2; ++prefix) {
        fail_table_at = table_attempt + prefix;
        EXPECT_EQ(invoke_mixed(TEST_VA, 2 * LARGE), -ENOMEM);
        fail_table_at = SIZE_MAX;
        unchanged();
    }
    fail_plan_heap = true;
    EXPECT_EQ(invoke_mixed(TEST_VA, 2 * LARGE), -ENOMEM);
    unchanged();
}
TEST_F(ManagedMixedGeometry, StaleRootAndEpochRefuseWithoutSemanticChanges) {
    seed();
    host_fork_vm::on_legacy = [] { ++runtime_serial; };
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EAGAIN);
    --runtime_serial;
    host_fork_vm::on_legacy = [] { ++virt::host_root_control.ps_geometry_epoch; };
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    unchanged();
}
TEST_F(ManagedMixedGeometry, SaturationRefusesAtPreparationAndFinalAdmission) {
    seed();
    host_fork_vm::on_legacy = [] { virt::host_root_control.ps_geometry_epoch = UINT64_MAX; };
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EOVERFLOW);
    host_fork_vm::on_legacy = nullptr;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EOVERFLOW);
    unchanged();
}
TEST_F(ManagedMixedGeometry, LastDescriptorRefusalPrecedesFirstSplit) {
    seed(2);
    auto saved = slot_at(runtime_root, TEST_VA + LARGE, 2);
    host_fork_vm::on_legacy = [] { slot_at(runtime_root, TEST_VA + LARGE, 2) ^= LARGE; };
    EXPECT_EQ(invoke_mixed(TEST_VA, 2 * LARGE), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    slot_at(runtime_root, TEST_VA + LARGE, 2) = saved;
    unchanged();
}
TEST_F(ManagedMixedGeometry, ExactRecordActorRangeAndConsumedStateAreChecked) {
    seed();
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    vmem::PreparedRangeDemotion record(actor, preparation);
    ASSERT_EQ(record.prepare(TEST_VA, TEST_VA + PAGE), 0);
    EXPECT_EQ(record.prepare(TEST_VA, TEST_VA + PAGE), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(TEST_VA + PAGE, PAGE, &record)), -EINVAL);
    host_range::current = &sibling;
    EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(TEST_VA, PAGE, &record)), -EINVAL);
    host_range::current = &actor;
    expected(TEST_VA, TEST_VA + PAGE);
    host_mixed::expected_pubs = host_fork_vm::pubs + 1;
    host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
    huge_check_free = false;
    EXPECT_EQ(vmem::anon_free(TEST_VA, PAGE, &record), 0U);
    EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(TEST_VA, PAGE, &record)), -EINVAL);
    host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
    mapped[0] = false;
    cleanup[0].reset();
}
TEST_F(ManagedMixedGeometry, UnaccountedForeignAndNativeContextsRefuse) {
    seed();
    actor.syscall_account_start_us = 0;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EWOULDBLOCK);
    actor.syscall_account_start_us = 1;
    host_range::current = &sibling;
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EWOULDBLOCK);
    host_range::current = &actor;
    actor.native_file_mapping_retirement.store(reinterpret_cast<void*>(1));
    EXPECT_EQ(invoke_mixed(TEST_VA, PAGE), -EBUSY);
    actor.native_file_mapping_retirement.store(nullptr);
    unchanged();
}
TEST_F(ManagedMixedGeometry, BackendRunsAfterReaderDestructionUnderSamePublication) {
    mixed_seed();
    auto base = TEST_VA + LARGE;
    ASSERT_EQ(invoke_mixed(base - PAGE, 6 * PAGE), 0);
    EXPECT_EQ(host_mixed::backend_calls, 1U);
    EXPECT_EQ(host_mixed::snapshots_taken, 1U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 5U);
}
TEST_F(ManagedMixedGeometry, SyncFailureKeepsEquivalentDemotionAndOrdinaryOwners) {
    mixed_seed();
    auto base = TEST_VA + LARGE;
    int refs = file.refcount.load();
    host_mixed::backend_error = -EIO;
    EXPECT_EQ(invoke_mixed(base - PAGE, 6 * PAGE), -EIO);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(model.live_refs(), 516U);
    EXPECT_EQ(file.refcount.load(), refs);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    EXPECT_EQ(covering(actor, base - PAGE), 1U);
    no_ordinary_unmap();
}
TEST_F(ManagedMixedGeometry, RegistryMiddleSplitPreservesByteOffsetsAndFileReferences) {
    seed();
    auto base = TEST_VA + LARGE;
    for (unsigned i = 0; i < 3; ++i) normal(base + i * PAGE, 0x62000000 + i * PAGE);
    file_row(actor, base, base + 3 * PAGE, 1000, true);
    file_row(sibling, base, base + 3 * PAGE, 1000, true);
    registry(base, 3 * PAGE - 17, 1000, false);
    ASSERT_EQ(invoke_mixed(base + PAGE, PAGE), 0);
    ASSERT_EQ(vmem::g_file_mmap_ranges.size(), 2U);
    for (auto const& entry : vmem::g_file_mmap_ranges) {
        if (entry.start == base) {
            EXPECT_EQ(entry.length, PAGE);
            EXPECT_EQ(entry.file_offset, 1000U);
        } else {
            EXPECT_EQ(entry.start, base + 2 * PAGE);
            EXPECT_EQ(entry.length, PAGE - 17);
            EXPECT_EQ(entry.file_offset, 1000 + 2 * PAGE);
        }
    }
    EXPECT_EQ(file.refcount.load(), 7);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 128U);
    EXPECT_EQ(covering(actor, base + PAGE), 0U);
    EXPECT_EQ(covering(sibling, base + PAGE), 0U);
    EXPECT_EQ(model.live_refs(), 514U);
}
TEST_F(ManagedMixedGeometry, LazyRetainFailureLeavesBenignDemotionAndOldFailure) {
    seed();
    auto base = TEST_VA + LARGE;
    file_row(actor, base, base + PAGE, 1000);
    file_row(sibling, base, base + PAGE, 1000);
    int refs = file.refcount.load();
    file.refcount.store(INT_MAX);
    EXPECT_EQ(invoke_mixed(TEST_VA + PAGE, PAGE), -ENOMEM);
    EXPECT_EQ(file.refcount.load(), INT_MAX);
    file.refcount.store(refs);
    equivalent_leaf();
    EXPECT_EQ(covering(actor, TEST_VA + PAGE), 1U);
    no_ordinary_unmap();
}
TEST_F(ManagedMixedGeometry, OrdinaryAllocationFailureRetainsExistingSiblingPrefix) {
    seed();
    for (unsigned i = 0; i < 12; ++i) row(actor, TEST_VA + 4 * LARGE + i * 2 * PAGE, TEST_VA + 4 * LARGE + (i * 2 + 1) * PAGE);
    host_fork_vm::on_legacy = [] { host_fork_vm::fail_after = 0; };
    EXPECT_EQ(invoke_mixed(TEST_VA + PAGE, PAGE), -ENOMEM);
    host_fork_vm::on_legacy = nullptr;
    equivalent_leaf();
    EXPECT_EQ(covering(actor, TEST_VA + PAGE), 1U);
    EXPECT_EQ(covering(sibling, TEST_VA + PAGE), 0U);
    no_ordinary_unmap();
}
TEST_F(ManagedMixedGeometry, TypedDispatchOnlyUsesDeclaredWholeOrRefusalEndpoint) {
    tmpfs();
    host_mixed::typed_result = 0;
    EXPECT_EQ(invoke_mixed(TEST_VA + 2 * PAGE, PAGE), 0);
    host_mixed::typed_result = -EOPNOTSUPP;
    EXPECT_EQ(invoke_mixed(TEST_VA + 2 * PAGE, 2 * PAGE), -EOPNOTSUPP);
    EXPECT_EQ(host_mixed::typed_calls, 2U);
    EXPECT_EQ(host_mixed::backend_calls, 0U);
    no_ordinary_unmap();
    EXPECT_EQ(actor.lazy_vmem_ranges.size(), 2U);  // Endpoint does not claim binding mutation.
}
TEST_F(ManagedMixedGeometry, LegacyTwoArgumentEntryKeepsOrdinaryRemoval) {
    auto va = TEST_VA + 3 * LARGE;
    normal(va, 0x63000000);
    ker::vfs::File closing;
    closing.fops = &operations;
    ASSERT_EQ(vmem::register_file_mmap_range(runtime_root, va, PAGE, 0, &closing, false), 0);
    for (auto* subject : {&actor, &sibling}) {
        Row entry{};
        entry.start = va;
        entry.end = va + PAGE;
        entry.kind = ker::mod::sched::task::LazyVmemKind::FILE_BACKED;
        entry.file = &closing;
        ASSERT_TRUE(ker::mod::sched::task::try_retain_lazy_vmem_range(entry));
        ASSERT_TRUE(subject->lazy_vmem_ranges.push_back(entry));
    }
    ker::vfs::vfs_put_file(&closing);  // Only actual lazy/registry owners remain.
    expected(va, va + PAGE);
    host_mixed::expected_pubs = host_fork_vm::pubs + 1;
    host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
    huge_check_free = false;
    EXPECT_EQ(vmem::anon_free(va, PAGE), 0U);
    host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 1U);
    EXPECT_EQ(closing.refcount.load(), 0);
    EXPECT_EQ(host_mixed::final_closes, 1U);  // Declared File close endpoint.
}
TEST_F(ManagedMixedGeometry, FinalIfAndRootBusyRefuseWithoutLeakedReader) {
    seed();
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    vmem::PreparedRangeDemotion record(actor, preparation);
    ASSERT_EQ(record.prepare(TEST_VA, TEST_VA + PAGE), 0);
    {
        HostIrqOff irq;
        EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(TEST_VA, PAGE, &record)), -EWOULDBLOCK);
    }
    host_range::reader_busy = true;
    EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(TEST_VA, PAGE, &record)), -EAGAIN);
    host_range::reader_busy = false;
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    unchanged();
}
TEST_F(ManagedMixedGeometry, ReadonlyDescriptionFailsAfterBenignDemotionBeforeBackend) {
    mixed_seed();
    auto base = TEST_VA + LARGE;
    file.open_flags = 0;
    EXPECT_EQ(invoke_mixed(base - PAGE, 6 * PAGE), -EBADF);
    EXPECT_EQ(host_mixed::backend_calls, 0U);
    EXPECT_GT(host_mixed::snapshots_taken, 0U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    no_ordinary_unmap();
}
TEST_F(ManagedMixedGeometry, SnapshotRevalidationFailureKeepsOrdinaryFailureBoundary) {
    mixed_seed();
    auto base = TEST_VA + LARGE;
    host_mixed::snapshot_matches = false;
    EXPECT_EQ(invoke_mixed(base - PAGE, 6 * PAGE), -EFAULT);
    EXPECT_EQ(host_mixed::backend_calls, 0U);
    EXPECT_EQ(model.live_refs(), 516U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    no_ordinary_unmap();
}
}  // namespace
// BEGIN MIXED PROTECTION CASES
// Exact protection/lazy/File/swap/permission bodies run on declared copied
// policy, completed-flush, zero-cache and map-publication endpoints.
namespace {
class ManagedMixedProtect : public ManagedMixedGeometry {
   protected:
    void SetUp() override {
        ManagedMixedGeometry::SetUp();
        host_protect::active = host_protect::zero_refused = false;
        host_protect::acks = host_protect::policy_reads = host_protect::map_calls = host_protect::cr3_writes = 0;
        host_protect::copied_policy = {};
        host_protect::supplied_zero = nullptr;
    }
    void TearDown() override {
        host_protect::active = false;
        host_protect::supplied_zero = nullptr;
        ManagedMixedGeometry::TearDown();
    }
    auto invoke_protect(uint64_t start, uint64_t length, uint64_t prot) -> int {
        host_mixed::expected_pubs = host_fork_vm::pubs + 1;
        host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
        huge_check_free = false;
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        int const status = vmem::detail::protect_current_mixed_range(actor, preparation, start, length, prot);
        host_fork_vm::fail_after = -1;
        host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = false;
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers, 0U);
        for (unsigned i = 0; i < leaves; ++i)
            if ((slot_at(runtime_root, TEST_VA + i * LARGE, 2) & 128) == 0) {
                mapped[i] = false;
                cleanup[i].reset();
            }
        no_ordinary_unmap();
        EXPECT_EQ(host_mixed::backend_calls + host_mixed::snapshots_taken, 0U);
        return status;
    }
    auto raw(uint64_t va) -> uint64_t { return slot_at(runtime_root, va, 1); }
    auto metadata_protection(const Task& task, uint64_t va) -> uint64_t {
        for (auto const& value : task.lazy_vmem_ranges)
            if (value.start <= va && va < value.end) return value.prot;
        return UINT64_MAX;
    }
    void no_semantic_drop() {
        EXPECT_EQ(host_managed_remove::unmap_acks + host_managed_remove::unmap_drops, 0U);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    }
    void reserve(uint64_t va) {
        auto* cursor = runtime_root;
        for (int level = 4; level > 1; --level) {
            auto& entry = word(cursor, virt::index_of(va, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            ASSERT_EQ(entry & 128, 0U);
            cursor = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        word(cursor, virt::index_of(va, 1)) = mm::paging::PAGE_RESERVED;
    }
};

TEST_F(ManagedMixedProtect, MixedReadPreservesDataOutsideFlagsAndFileOwners) {
    mixed_seed();
    auto const base = TEST_VA + LARGE;
    auto const refs = model.live_refs();
    int const files = file.refcount.load();
    ASSERT_EQ(invoke_protect(base - PAGE, 6 * PAGE, ker::abi::vmem::PROT_READ), 0);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(model.live_refs(), refs);
    EXPECT_EQ(file.refcount.load(), files);
    ASSERT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges[0].file_offset, 9000U);
    EXPECT_EQ(raw(base - PAGE) & 7, 5U);
    EXPECT_EQ(raw(base) & 7, 5U);
    EXPECT_EQ(raw(base + PAGE), 0U);
    EXPECT_EQ(raw(base + 2 * PAGE) & 7, 5U);
    EXPECT_EQ(raw(TEST_VA) & 7, 7U);
    EXPECT_EQ(metadata_protection(actor, base - PAGE), ker::abi::vmem::PROT_READ);
    EXPECT_EQ(metadata_protection(sibling, base - PAGE), ker::abi::vmem::PROT_READ);
    EXPECT_EQ(metadata_protection(actor, TEST_VA), 3U);
    EXPECT_EQ(bytes[0][0], 1U);
    EXPECT_EQ(bytes[0][LARGE - PAGE], (511 % 251) + 1U);
    EXPECT_GT(host_protect::acks, 0U);
    EXPECT_EQ(host_protect::policy_reads, 1U);
}
TEST_F(ManagedMixedProtect, CrossLeafDescriptorsFinishBeforeProtection) {
    seed(2);
    ASSERT_EQ(invoke_protect(TEST_VA + LARGE - PAGE, 2 * PAGE, ker::abi::vmem::PROT_READ), 0);
    equivalent_leaf(2);
    EXPECT_EQ(raw(TEST_VA + LARGE - PAGE) & 7, 5U);
    EXPECT_EQ(raw(TEST_VA + LARGE) & 7, 5U);
    EXPECT_EQ(raw(TEST_VA + LARGE + PAGE) & 7, 7U);
    EXPECT_EQ(metadata_protection(sibling, TEST_VA + LARGE), ker::abi::vmem::PROT_READ);
}
TEST_F(ManagedMixedProtect, NoneRevokesUserAndWriteWithoutDroppingFrames) {
    seed();
    ASSERT_EQ(invoke_protect(TEST_VA + 2 * PAGE, PAGE, ker::abi::vmem::PROT_NONE), 0);
    EXPECT_EQ(raw(TEST_VA + 2 * PAGE) & 7, 1U);
    EXPECT_NE(raw(TEST_VA + 2 * PAGE) & mm::paging::PAGE_NX, 0U);
    EXPECT_EQ(raw(TEST_VA + PAGE) & 7, 7U);
    EXPECT_EQ(metadata_protection(actor, TEST_VA + 2 * PAGE), 0U);
    EXPECT_EQ(model.live_refs(), 512U);
    EXPECT_EQ(host_protect::policy_reads, 0U);
}
TEST_F(ManagedMixedProtect, WritableAliasedPageRemainsCowUntilPrivateFault) {
    seed();
    auto* page = bytes[0] + 5 * PAGE;
    mm::phys::page_ref_inc(page);
    ASSERT_EQ(invoke_protect(TEST_VA + 5 * PAGE, PAGE, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE), 0);
    EXPECT_EQ(raw(TEST_VA + 5 * PAGE) & mm::paging::PAGE_WRITE, 0U);
    EXPECT_NE(raw(TEST_VA + 5 * PAGE) & mm::paging::PAGE_COW, 0U);
    EXPECT_EQ(mm::phys::page_ref_get(page), 2U);
    EXPECT_TRUE(mm::phys::page_ref_dec(page));
}
TEST_F(ManagedMixedProtect, ReadRevokesOldCowAuthorization) {
    auto const va = TEST_VA + 3 * LARGE;
    normal(va, 0x65000000, 0x8000000000000005ULL | mm::paging::PAGE_COW);
    row(actor, va, va + PAGE);
    ASSERT_EQ(invoke_protect(va, PAGE, ker::abi::vmem::PROT_READ), 0);
    EXPECT_EQ(raw(va) & (mm::paging::PAGE_COW | mm::paging::PAGE_WRITE), 0U);
    EXPECT_EQ(model.live_refs(), 1U);
}
TEST_F(ManagedMixedProtect, ExecuteClearsNxOnTheRequestedLeafOnly) {
    seed();
    ASSERT_EQ(invoke_protect(TEST_VA + PAGE, PAGE, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_EXEC), 0);
    EXPECT_EQ(raw(TEST_VA + PAGE) & mm::paging::PAGE_NX, 0U);
    EXPECT_NE(raw(TEST_VA) & mm::paging::PAGE_NX, 0U);
    EXPECT_EQ(model.live_refs(), 512U);
}
TEST_F(ManagedMixedProtect, ReadonlySharedTailRefusesBeforeAnySplit) {
    seed();
    auto const tail = TEST_VA + LARGE;
    normal(tail, 0x65001000, 0x8000000000000005ULL | mm::paging::PAGE_SHARED | mm::paging::PAGE_WRITE_RESTRICTED);
    EXPECT_EQ(invoke_protect(tail - PAGE, 2 * PAGE, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE), -EACCES);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 128U);
    EXPECT_EQ(model.live_refs(), 513U);
    EXPECT_EQ(metadata_protection(actor, tail - PAGE), 3U);
    EXPECT_EQ(host_protect::acks, 0U);
}
TEST_F(ManagedMixedProtect, ReadonlyFileTailRefusesBeforeAnySplit) {
    mixed_seed();
    auto const base = TEST_VA + LARGE;
    file.open_flags = 0;
    EXPECT_EQ(invoke_protect(base - PAGE, 6 * PAGE, 3), -EACCES);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 128U);
    EXPECT_EQ(host_protect::acks, 0U);
    EXPECT_EQ(metadata_protection(actor, base - PAGE), 3U);
}
TEST_F(ManagedMixedProtect, InitialLayoutRefusesBeforeGeometryChanges) {
    seed();
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(invoke_protect(TEST_VA, PAGE, ker::abi::vmem::PROT_READ), -EINVAL);
    unchanged();
}
TEST_F(ManagedMixedProtect, InvalidCopiedPolicyRefusesBeforePublication) {
    seed();
    host_protect::copied_policy = {0, numa::Mode::BIND};
    unsigned const publications = host_fork_vm::pubs;
    EXPECT_EQ(invoke_protect(TEST_VA, PAGE, ker::abi::vmem::PROT_READ), -EINVAL);
    EXPECT_EQ(host_fork_vm::pubs, publications);
    unchanged();
}
TEST_F(ManagedMixedProtect, ZeroAndSubFourMiBKeepExistingProtectionCompatibility) {
    EXPECT_EQ(invoke_protect(0, PAGE, ker::abi::vmem::PROT_NONE), 0);
    EXPECT_EQ(invoke_protect(LARGE, PAGE, ker::abi::vmem::PROT_READ), 0);
    EXPECT_EQ(host_protect::map_calls + host_protect::acks, 0U);
    EXPECT_EQ(static_cast<int64_t>(vmem::anon_free(LARGE, PAGE)), -EINVAL);
}
TEST_F(ManagedMixedProtect, OrdinaryPublicArgumentErrorsKeepTheirPrecedence) {
    EXPECT_EQ(static_cast<int64_t>(vmem::protect_range(1, 0, 0)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(vmem::protect_range(TEST_VA, UINT64_MAX, 0)), -EINVAL);
    actor.native_file_mapping_retirement.store(reinterpret_cast<void*>(1));
    EXPECT_EQ(static_cast<int64_t>(vmem::protect_range(1, 0, 0)), -EBUSY);
    actor.native_file_mapping_retirement.store(nullptr);
    auto* saved = host_range::current;
    host_range::current = nullptr;
    EXPECT_EQ(static_cast<int64_t>(vmem::protect_range(1, 0, 0)), -EFAULT);
    host_range::current = saved;
}
TEST_F(ManagedMixedProtect, OrdinaryFourKAndHoleNeedNoManagedCoverage) {
    auto const va = TEST_VA + 3 * LARGE;
    normal(va, 0x65002000);
    normal(va + 2 * PAGE, 0x65003000);
    row(actor, va, va + 3 * PAGE);
    ASSERT_EQ(invoke_protect(va, 2 * PAGE + 1, ker::abi::vmem::PROT_READ), 0);
    EXPECT_EQ(raw(va) & 7, 5U);
    EXPECT_EQ(raw(va + PAGE), 0U);
    EXPECT_EQ(raw(va + 2 * PAGE) & 7, 5U);
    EXPECT_EQ(model.live_refs(), 2U);
}
TEST_F(ManagedMixedProtect, SwapMarkerUpdatesActualMetadataWithoutMaterializing) {
    auto const va = TEST_VA + 3 * LARGE;
    marker(va, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    ASSERT_EQ(invoke_protect(va, PAGE, ker::abi::vmem::PROT_READ), 0);
    auto* mapping = virt::anonymous_swap_mappings;
    ASSERT_NE(mapping, nullptr);
    EXPECT_EQ(mapping->original_raw & 7, 5U);
    EXPECT_EQ(raw(va), virt::anonymous_swap_marker_raw(mapping->original_raw));
    EXPECT_EQ(host_protect::map_calls, 0U);
}
TEST_F(ManagedMixedProtect, ReservedReadUsesActualFactoryAndAdoptsZeroedPage) {
    auto const va = TEST_VA + 3 * LARGE;
    reserve(va);
    row(actor, va, va + PAGE);
    ASSERT_EQ(invoke_protect(va, PAGE, ker::abi::vmem::PROT_READ), 0);
    EXPECT_EQ(raw(va) & 7, 5U);
    EXPECT_EQ(host_protect::map_calls, 1U);
    auto* page = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(raw(va) & virt::PTE_FRAME_MASK));
    ASSERT_NE(page, nullptr);
    for (unsigned i = 0; i < PAGE; ++i) EXPECT_EQ(page[i], 0U);
    EXPECT_EQ(mm::phys::page_ref_get(page), 1U);
}
TEST_F(ManagedMixedProtect, ReservedNoneAndZeroCacheRefusalPreserveOldPrefix) {
    auto const va = TEST_VA + 3 * LARGE;
    reserve(va);
    row(actor, va, va + PAGE);
    ASSERT_EQ(invoke_protect(va, PAGE, ker::abi::vmem::PROT_NONE), 0);
    EXPECT_EQ(raw(va), mm::paging::PAGE_RESERVED);
    host_protect::zero_refused = true;
    EXPECT_EQ(invoke_protect(va, PAGE, 3), -ENOMEM);
    host_protect::zero_refused = false;
    EXPECT_EQ(raw(va), mm::paging::PAGE_RESERVED);
    EXPECT_EQ(metadata_protection(actor, va), 3U);
    EXPECT_EQ(host_protect::map_calls, 0U);
}
TEST_F(ManagedMixedProtect, LazyAllocationFailureKeepsEquivalentDemotion) {
    seed();
    for (unsigned i = 0; i < 12; ++i) row(actor, TEST_VA + 4 * LARGE + i * 2 * PAGE, TEST_VA + 4 * LARGE + (i * 2 + 1) * PAGE);
    host_fork_vm::on_legacy = [] { host_fork_vm::fail_after = 0; };
    EXPECT_EQ(invoke_protect(TEST_VA + PAGE, PAGE, ker::abi::vmem::PROT_READ), -ENOMEM);
    host_fork_vm::on_legacy = nullptr;
    equivalent_leaf();
    EXPECT_EQ(raw(TEST_VA + PAGE) & 7, 7U);
    EXPECT_EQ(host_protect::acks, 0U);
    no_semantic_drop();
}
TEST_F(ManagedMixedProtect, TypedDispatchPreservesWholeBindingOnlyEndpoint) {
    tmpfs();
    host_mixed::typed_result = -EOPNOTSUPP;
    EXPECT_EQ(invoke_protect(TEST_VA + 2 * PAGE, 2 * PAGE, ker::abi::vmem::PROT_READ), -EOPNOTSUPP);
    EXPECT_EQ(host_mixed::typed_calls, 1U);
    EXPECT_EQ(host_protect::policy_reads + host_protect::acks, 0U);
    EXPECT_EQ(actor.lazy_vmem_ranges.size(), 2U);
}
}  // namespace
