// Actual public358/360 bodies; finite AS/context/physical/ACK endpoints only.
namespace {
class ManagedPublicHugeMap : public ManagedPublicMutation {
   protected:
    static constexpr uint64_t START = vmem::MMAP_START;
    static constexpr uint64_t FLAGS = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_ANONYMOUS | ker::abi::vmem::MAP_HUGETLB;
    void SetUp() override {
        ManagedFileFixed::SetUp();
        fork_mode = false;
        actor.mmap_next = sibling.mmap_next = 0;
        host_public_huge::active = host_public_huge::publishing = false;
        host_public_huge::retry_cohort = host_public_huge::occupy_first = host_public_huge::changed = false;
        host_public_huge::admissions = host_public_huge::observations = host_public_huge::acknowledgments = host_public_huge::entries = 0;
        host_public_huge::refuse_admission = -1;
        host_public_huge::published_count = 0;
        host_batch::enabled = host_batch::fail_heap = false;
        host_batch::allocation_index = 0;
        host_batch::fail_data_before = SIZE_MAX;
        host_batch::observed = nullptr;
        host_batch::observed_parents = nullptr;
        host_batch::acknowledgments = host_batch::detached_returns = 0;
        host_batch::returning_count = 0;
        host_protect::copied_policy = {1, numa::Mode::BIND};
        host_protect::policy_reads = 0;
        model.cpu_reads = 0;
    }
    auto call(uint64_t length = LARGE, bool map = false, uint64_t prot = 3, uint64_t flags = FLAGS, uint64_t hint = START,
              bool incoming_if = true) -> uint64_t {
        host_public_huge::requested = hint;
        host_public_huge::active = host_batch::enabled = true;
        host_public::active = host_fixed::active = host_protect::active = host_mixed::active = true;
        host_fork_vm::enabled = host_managed_remove::invoking = true;
        host_file::active = host_file::in_operation = false;
        huge_check_free = false;
        host_range::interrupts = incoming_if;
        auto result = map ? vmem::sys_vmem_map(hint, length, prot, flags, UINT64_MAX, UINT64_MAX)
                          : vmem::sys_vmem(static_cast<uint64_t>(ker::abi::vmem::ops::ANON_ALLOCATE), hint, length, prot, flags);
        EXPECT_EQ(host_range::interrupts, incoming_if);
        host_range::interrupts = true;
        host_public_huge::active = host_public::active = host_fixed::active = host_protect::active = host_mixed::active = false;
        host_fork_vm::enabled = host_managed_remove::invoking = false;
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers + cow_depth + pin_depth + model.preempt_depth, 0U);
        EXPECT_EQ(publication_depth, 0U);
        EXPECT_FALSE(host_range::exclusive);
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(host_batch::observed, nullptr);
        EXPECT_EQ(host_batch::observed_parents, nullptr);
        return result;
    }
    void mapped_at(uint64_t address, uint16_t count, uint64_t prot = 3, uint64_t flags = FLAGS) {
        auto const expected = vmem::prot_to_page_flags(prot);
        for (uint16_t n = 0; n < count; ++n) {
            auto const va = address + uint64_t{n} * LARGE;
            auto const entry = slot_at(runtime_root, va, 2);
            EXPECT_EQ(entry & (128 | 7 | uint64_t{1} << 63), expected | 128);
            auto const physical = entry & virt::PTE_FRAME_MASK;
            uint32_t page{};
            auto* block = model.containing(reinterpret_cast<uint64_t>(mm::addr::get_virt_pointer(physical)), page);
            ASSERT_NE(block, nullptr);
            EXPECT_EQ(page, 0U);
            EXPECT_EQ(block->pages, 512U);
            auto* data = static_cast<uint8_t*>(block->storage.get());
            EXPECT_TRUE(std::all_of(data, data + LARGE, [](uint8_t value) { return value == 0; }));
            for (auto* task : {&actor, &sibling}) {
                EXPECT_EQ(covering(*task, va), 1U);
                auto row = std::find_if(task->lazy_vmem_ranges.begin(), task->lazy_vmem_ranges.end(),
                                        [&](auto const& r) { return r.start <= va && va < r.end; });
                ASSERT_NE(row, task->lazy_vmem_ranges.end());
                EXPECT_EQ(row->kind, ker::mod::sched::task::LazyVmemKind::ANONYMOUS);
                EXPECT_EQ(row->file, nullptr);
                EXPECT_EQ(row->prot, prot);
                EXPECT_EQ(row->flags, flags);
            }
        }
        EXPECT_GE(actor.mmap_next.load(), address + uint64_t{count} * LARGE);
        EXPECT_GE(sibling.mmap_next.load(), address + uint64_t{count} * LARGE);
    }
    void empty() {
        huge::Snapshot snapshot{};
        huge_core.snapshot(snapshot);
        EXPECT_EQ(snapshot.charged_pages + snapshot.plans + snapshot.table_pages, 0U);
        EXPECT_EQ(model.live_refs(), 0U);
        EXPECT_EQ(host_public_huge::published_count, 0U);
        EXPECT_EQ(host_public_huge::acknowledgments, 0U);
    }
    void TearDown() override {
        // Preserve failure assertions, then drain every genuine ACK range,
        // including an unreported postwrite-mutant mapping with missing rows.
        host_public_huge::active = host_public_huge::publishing = false;
        host_fork_vm::on_pub = nullptr;
        host_fork_vm::fail_after = -1;
        fail_table_at = SIZE_MAX;
        host_batch::fail_heap = false;
        host_batch::fail_data_before = SIZE_MAX;
        host_range::interrupts = true;
        for (unsigned i = 0; i < host_public_huge::published_count; ++i) {
            auto const range = host_public_huge::published[i];
            ker::mod::sys::SyscallPreparationScope preparation(actor);
            vmem::PreparedRangeDemotion demotion(actor, preparation);
            ASSERT_EQ(demotion.prepare(range.start, range.start + range.length), 0);
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                EXPECT_EQ(vmem::demote_prepared_range_locked(actor, range.start, range.start + range.length, publication, demotion), 0);
            }
        }
        host_batch::enabled = false;
        huge::Snapshot snapshot{};
        huge_core.snapshot(snapshot);
        EXPECT_EQ(snapshot.mapped_pages + snapshot.splitting_pages, 0U);
        ManagedPublicMutation::TearDown();
    }
};
TEST_F(ManagedPublicHugeMap, RawAliasRealPsAndMirroredRows) {
    ASSERT_EQ(call(), START);
    mapped_at(START, 1);
    EXPECT_EQ(host_public_huge::entries, 1U);
    EXPECT_EQ(host_public_huge::admissions, 2U);
    EXPECT_EQ(host_public_huge::observations, 2U);
    EXPECT_EQ(host_protect::policy_reads, 1U);
    EXPECT_EQ(model.cpu_reads, 1U);
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].pages, 512U);
    EXPECT_EQ(model.calls[0].alignment, LARGE);
    EXPECT_EQ(model.calls[0].initialization, Initialization::ZERO);
    EXPECT_EQ(model.calls[0].dispatch, Dispatch::RANGE);
}
TEST_F(ManagedPublicHugeMap, MapAliasIgnoresFileInputs) {
    ASSERT_EQ(call(LARGE, true), START);
    mapped_at(START, 1);
    EXPECT_EQ(host_file::fd_lookups + host_file::stats + host_file::reads, 0U);
    EXPECT_EQ(host_public_huge::acknowledgments, 1U);
}
TEST_F(ManagedPublicHugeMap, ProfileOriginalLengthAndPrefixErrors) {
    for (auto length : {uint64_t{0}, uint64_t{1}, LARGE - 1, LARGE + PAGE}) EXPECT_EQ(static_cast<int64_t>(call(length)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(call(65 * LARGE)), -ENOMEM);
    EXPECT_EQ(static_cast<int64_t>(call(UINT64_MAX)), -EOVERFLOW);
    for (auto flags : {FLAGS | ker::abi::vmem::MAP_SHARED, FLAGS | ker::abi::vmem::MAP_FIXED, FLAGS | ker::abi::vmem::MAP_NORESERVE,
                       FLAGS | (uint64_t{1} << 20), FLAGS | (uint64_t{21} << 26), FLAGS & ~ker::abi::vmem::MAP_PRIVATE,
                       FLAGS & ~ker::abi::vmem::MAP_ANONYMOUS})
        EXPECT_EQ(static_cast<int64_t>(call(LARGE, false, 3, flags)), -EINVAL);
    EXPECT_EQ(static_cast<int64_t>(call(LARGE, false, 8)), -EINVAL);
    auto* root = actor.pagemap;
    actor.pagemap = nullptr;
    EXPECT_EQ(static_cast<int64_t>(call(0)), -EFAULT);
    actor.pagemap = root;
    actor.native_file_mapping_retirement.store(reinterpret_cast<void*>(1));
    EXPECT_EQ(static_cast<int64_t>(call(0)), -EBUSY);
    actor.native_file_mapping_retirement.store(nullptr);
    EXPECT_EQ(host_public_huge::admissions, 0U);
    EXPECT_EQ(host_protect::policy_reads, 0U);
    empty();
    model.fail_all = true;
    EXPECT_EQ(static_cast<int64_t>(call(64 * LARGE)), -ENOMEM);
    model.fail_all = false;
    EXPECT_EQ(host_public_huge::admissions, 1U);
    ASSERT_EQ(model.calls.size(), 1U);
    EXPECT_EQ(model.calls[0].pages, 512U);
    empty();
}
TEST_F(ManagedPublicHugeMap, TwoLeafZeroAndProtNone) {
    ASSERT_EQ(call(2 * LARGE, false, ker::abi::vmem::PROT_NONE, FLAGS | ker::abi::vmem::MAP_POPULATE), START);
    mapped_at(START, 2, 0, FLAGS | ker::abi::vmem::MAP_POPULATE);
    EXPECT_EQ(host_public_huge::acknowledgments, 1U);
    EXPECT_EQ(model.live_refs(), 1024U);
}
TEST_F(ManagedPublicHugeMap, InitialAndFinalAsDenial) {
    host_public_huge::refuse_admission = 0;
    EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
    empty();
    host_public_huge::admissions = 0;
    host_public_huge::refuse_admission = 1;
    EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
    EXPECT_EQ(host_public_huge::admissions, 2U);
    empty();
}
TEST_F(ManagedPublicHugeMap, OneCaptureAcrossCohortRetry) {
    host_protect::copied_policy = {0, numa::Mode::LOCAL};
    host_public_huge::retry_cohort = true;
    ASSERT_EQ(call(), START);
    mapped_at(START, 1);
    EXPECT_TRUE(host_public_huge::changed);
    EXPECT_EQ(host_public_huge::admissions, 4U);
    EXPECT_EQ(host_public_huge::entries, 1U);
    EXPECT_EQ(host_protect::policy_reads, 1U);
    EXPECT_EQ(model.cpu_reads, 1U);
    EXPECT_EQ(numa::physical_node(slot_at(runtime_root, START, 2) & virt::PTE_FRAME_MASK), 0U);
}
TEST_F(ManagedPublicHugeMap, BindAndInterleaveCapturedPlacement) {
    model.failed_ranges = {model.topology.memory[0].base};
    EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
    empty();
    model.failed_ranges.clear();
    host_protect::copied_policy = {3, numa::Mode::INTERLEAVE};
    ASSERT_EQ(call(2 * LARGE), START);
    mapped_at(START, 2);
    EXPECT_EQ(numa::physical_node(slot_at(runtime_root, START, 2) & virt::PTE_FRAME_MASK), 0U);
    EXPECT_EQ(numa::physical_node(slot_at(runtime_root, START + LARGE, 2) & virt::PTE_FRAME_MASK), 1U);
}
TEST_F(ManagedPublicHugeMap, MetadataPrepareAndStageFailures) {
    for (int prefix = 0; prefix < 3; ++prefix) {
        host_fork_vm::fail_after = prefix;
        EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
        host_fork_vm::fail_after = -1;
        empty();
    }
    for (unsigned i = 0; i < 8; ++i) {
        row(actor, TEST_VA + i * PAGE, TEST_VA + (i + 1) * PAGE);
        row(sibling, TEST_VA + i * PAGE, TEST_VA + (i + 1) * PAGE);
    }
    // Eight rows fit each real inline8 clone; the three owner arrays precede staged actor reserve.
    host_fork_vm::fail_after = 3;
    EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
    host_fork_vm::fail_after = -1;
    EXPECT_EQ(actor.lazy_vmem_ranges.size(), 8U);
    EXPECT_EQ(sibling.lazy_vmem_ranges.size(), 8U);
    empty();
}
TEST_F(ManagedPublicHugeMap, HeapAndEveryTablePrefixFailure) {
    host_batch::fail_heap = true;
    EXPECT_EQ(static_cast<int64_t>(call()), -ENOMEM);
    empty();
    for (size_t prefix = 0; prefix < 6; ++prefix) {
        fail_table_at = table_attempt + prefix;
        EXPECT_EQ(static_cast<int64_t>(call(2 * LARGE)), -ENOMEM);
        fail_table_at = SIZE_MAX;
        empty();
    }
}
TEST_F(ManagedPublicHugeMap, EveryDataLeafPrefixFailure) {
    for (size_t prefix = 0; prefix < 2; ++prefix) {
        host_batch::allocation_index = 0;
        host_batch::fail_data_before = prefix;
        EXPECT_EQ(static_cast<int64_t>(call(2 * LARGE)), -ENOMEM);
        empty();
    }
    host_batch::fail_data_before = SIZE_MAX;
}
TEST_F(ManagedPublicHugeMap, RootCohortStalenessAndThreeAttemptBound) {
    host_fork_vm::on_pub = +[] { ++runtime_serial; };
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    EXPECT_EQ(host_public_huge::admissions, 3U);
    empty();
    host_fork_vm::on_pub = nullptr;
    host_public_huge::admissions = 0;
    host_managed_remove::refused_task = &sibling;
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    EXPECT_EQ(host_public_huge::admissions, 3U);
    empty();
    host_managed_remove::refused_task = nullptr;
}
TEST_F(ManagedPublicHugeMap, InvalidGeometryAndLayoutOwners) {
    auto& entry = word(runtime_root, virt::index_of(START, 4));
    entry = 0xdead000 | 135;
    EXPECT_EQ(static_cast<int64_t>(call()), -EOPNOTSUPP);
    entry = 0;
    empty();
    // The declared AS grant forces this selector branch; no actual quota/invalid-tree errno is inferred.
    protected_layout.layout_base_virt = START;
    protected_layout.layout_size = PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    sibling.thread = nullptr;
    empty();
}
TEST_F(ManagedPublicHugeMap, LegacyAndTypedContradictions) {
    registry(START, PAGE, 0, false);
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    empty();
    vmem::unregister_file_mmap_range(runtime_root, START, PAGE);
    tmpfs();
    auto* b = fm::FileMappingBinding::prepare_unbound(*family);
    ASSERT_NE(b, nullptr);
    ASSERT_TRUE(b->configure_unpublished(runtime_root, runtime_serial, START, START + PAGE, 0));
    ASSERT_TRUE(virt::file_mapping_index.attach_locked(b, runtime_serial));
    ASSERT_EQ(b, nullptr);
    auto const before = family->owner_counts();
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    empty();
    EXPECT_EQ(family->owner_counts().count(fm::FamilyOwner::BINDING), before.count(fm::FamilyOwner::BINDING));
}
TEST_F(ManagedPublicHugeMap, FinalVacancyAndMremapSuspension) {
    virt::host_root_control.ps_admission_suspensions = 1;
    EXPECT_EQ(static_cast<int64_t>(call()), -EAGAIN);
    empty();
    virt::host_root_control.ps_admission_suspensions = 0;
    host_public_huge::occupy_first = true;
    host_public_huge::changed = false;
    ASSERT_EQ(call(), START + LARGE);
    mapped_at(START + LARGE, 1);
    EXPECT_EQ(host_public_huge::acknowledgments, 1U);
    EXPECT_EQ(covering(actor, START), 1U);
}
TEST_F(ManagedPublicHugeMap, AckBeforeRowsCursorAndSuccess) {
    ASSERT_EQ(call(2 * LARGE), START);
    mapped_at(START, 2);
    EXPECT_EQ(host_public_huge::acknowledgments, 1U);
    EXPECT_EQ(host_public_huge::entries, 1U);
    EXPECT_EQ(host_public_huge::published_count, 1U);
    EXPECT_EQ(host_batch::acknowledgments, 1U);
}
TEST_F(ManagedPublicHugeMap, ContextIncomingIfAndOutsideCleanup) {
    actor.syscall_account_start_us = 0;
    EXPECT_EQ(static_cast<int64_t>(call()), -EWOULDBLOCK);
    actor.syscall_account_start_us = 1;
    actor.type = ker::mod::sched::task::TaskType::DAEMON;
    EXPECT_EQ(static_cast<int64_t>(call()), -EWOULDBLOCK);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    ASSERT_EQ(call(LARGE, false, 3, FLAGS, START, false), START);
    mapped_at(START, 1);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load() + sibling.usercopy_pagemap_accesses.load(), 0U);
}
}  // namespace
