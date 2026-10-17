// Actual File allocation/read-exact/registry/lazy/demotion/rollback bodies.
// FD/stat/cache/AS decisions, frame allocation and hardware ACK are finite ports.
namespace {
class ManagedFileFixed : public ManagedAnonymousFixed {
   protected:
    ker::vfs::File source_file;
    ker::vfs::File replacement_file;
    void SetUp() override {
        ManagedAnonymousFixed::SetUp();
        source_file.fops = replacement_file.fops = &operations;
        host_file::active = host_file::in_operation = host_file::lazy = host_file::cache = host_file::reuse_fd = false;
        host_file::stat_error = host_file::read_error = 0;
        host_file::fail_read_after = -1;
        host_file::fd_lookups = host_file::stats = host_file::reads = host_file::outer_closes = host_file::typed_calls =
            host_file::cache_hits = host_file::batch_flushes = 0;
        host_file::max_read = PAGE;
        host_file::fd_file = host_file::expected_file = &source_file;
        host_file::replacement = &replacement_file;
        host_file::read_offsets.clear();
        host_file::cache_pages.clear();
    }
    void TearDown() override {
        host_file::active = host_file::in_operation = false;
        // Cache references are distinct from the real PTE references held by
        // the original physical fixture and are dropped before its teardown.
        for (auto* page : host_file::cache_pages)
            if (page) (void)mm::phys::page_ref_dec(page);
        host_file::cache_pages.clear();
        ManagedAnonymousFixed::TearDown();
    }
    void release_derived_descriptor_owners() override {
        // The source descriptor owner was consumed exactly once on FD reuse.
        // Real metadata teardown above has already returned all row/registry refs.
        EXPECT_EQ(source_file.refcount.load(), host_file::fd_file == &source_file ? 1 : 0);
        if (host_file::fd_file == &source_file) ker::vfs::vfs_put_file(&source_file);
        EXPECT_EQ(replacement_file.refcount.load(), 1);
        ker::vfs::vfs_put_file(&replacement_file);
        EXPECT_EQ(source_file.refcount.load(), 0);
        EXPECT_EQ(replacement_file.refcount.load(), 0);
        host_file::fd_file = host_file::expected_file = host_file::replacement = nullptr;
    }
    auto file_fixed(uint64_t first, uint64_t length, uint64_t prot = 3,
                    uint64_t flags = ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, uint64_t offset = 0) -> uint64_t {
        expected(first, first + ((length + PAGE - 1) & ~(PAGE - 1)));
        host_mixed::expected_pubs = host_fork_vm::pubs + 1;
        host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_managed_remove::invoking = true;
        host_file::active = host_file::in_operation = true;
        uint64_t result;
        {
            ker::mod::sys::SyscallPreparationScope preparation(actor);
            result = vmem::detail::replace_current_file_fixed(actor, preparation, first, length, prot, flags, 7, offset);
        }
        host_file::active = host_file::in_operation = false;
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
    void contents(uint64_t address, uint64_t offset, size_t count = PAGE) {
        auto* data = frame(address);
        ASSERT_NE(data, nullptr);
        for (size_t i = 0; i < count; ++i) EXPECT_EQ(data[i], static_cast<uint8_t>(0x31 + (offset + i) % 113));
    }
};

TEST_F(ManagedFileFixed, ReadAndFinalOuterCloseUseOrdinaryContinuousPublication) {
    seed();
    host_file::reuse_fd = true;
    host_file::max_read = 257;
    auto const address = TEST_VA + 13 * PAGE;
    ASSERT_EQ(file_fixed(address, PAGE, 3, ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, PAGE), address);
    contents(address, PAGE);
    EXPECT_EQ(host_file::fd_lookups, 1U);
    EXPECT_EQ(host_file::stats, 1U);
    EXPECT_GT(host_file::reads, 1U);
    EXPECT_EQ(host_file::outer_closes, 1U);
    EXPECT_EQ(host_file::fd_file, &replacement_file);
    EXPECT_EQ(source_file.refcount.load(), 0);
    EXPECT_EQ(source_file.pos, 17);
    EXPECT_EQ(raw(address + PAGE) & virt::PTE_FRAME_MASK, physical[0] + 14 * PAGE);
    EXPECT_EQ(model.live_refs(), 512U);
}
TEST_F(ManagedFileFixed, CrossLeafReadReplacesOnlyRequestedFrames) {
    seed(2);
    auto const address = TEST_VA + LARGE - PAGE;
    ASSERT_EQ(file_fixed(address, 2 * PAGE), address);
    contents(address, 0);
    contents(address + PAGE, PAGE);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 128, 0U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA + LARGE, 2) & 128, 0U);
    EXPECT_NE(raw(address) & virt::PTE_FRAME_MASK, raw(address + PAGE) & virt::PTE_FRAME_MASK);
    EXPECT_EQ(raw(TEST_VA) & virt::PTE_FRAME_MASK, physical[0]);
    EXPECT_EQ(host_managed_remove::unmap_acks, 2U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 2U);
    EXPECT_EQ(model.live_refs(), 1024U);
}
TEST_F(ManagedFileFixed, MixedFileHoleNoneAndSharedWritebackKeepOrdinarySemantics) {
    mixed_seed();
    auto const address = TEST_VA + LARGE - PAGE;
    ASSERT_EQ(file_fixed(address, 6 * PAGE), address);
    for (unsigned i = 0; i < 6; ++i) contents(address + uint64_t{i} * PAGE, uint64_t{i} * PAGE);
    EXPECT_EQ(host_managed_remove::unmap_drops, 5U);
    EXPECT_EQ(host_mixed::backend_calls, 1U);
    EXPECT_EQ(host_mixed::written_offsets, (std::vector<uint64_t>{9000}));
    EXPECT_EQ(host_mixed::written_bytes.size(), PAGE - 17);
    EXPECT_EQ(file.refcount.load(), 1);
    EXPECT_TRUE(vmem::g_file_mmap_ranges.empty());
    EXPECT_EQ(model.live_refs(), 517U);
}
TEST_F(ManagedFileFixed, PartialPositionalReadsAndEofTailKeepZerosAndDescriptorOffset) {
    seed();
    source_file.stat.st_size = PAGE + 33;
    host_file::max_read = 11;
    ASSERT_EQ(file_fixed(TEST_VA, 2 * PAGE), TEST_VA);
    contents(TEST_VA, 0);
    // The actual captured stat controls the final33 bytes of the second page.
    contents(TEST_VA + PAGE, PAGE, 33);
    for (size_t i = 33; i < PAGE; ++i) EXPECT_EQ(frame(TEST_VA + PAGE)[i], 0U);
    EXPECT_EQ(source_file.pos, 17);
    EXPECT_EQ(host_file::fd_lookups, 1U);
}
TEST_F(ManagedFileFixed, LazyFileCapturesRetainedIdentityIntoBothSiblingRows) {
    seed();
    host_file::lazy = true;
    auto const address = TEST_VA + 2 * PAGE;
    ASSERT_EQ(file_fixed(address, PAGE, 3, ker::abi::vmem::MAP_SHARED | ker::abi::vmem::MAP_FIXED, 3 * PAGE), address);
    EXPECT_EQ(raw(address), 0U);
    for (auto* subject : {&actor, &sibling}) {
        auto found = std::find_if(subject->lazy_vmem_ranges.begin(), subject->lazy_vmem_ranges.end(),
                                  [&](auto const& row) { return row.start == address && row.end == address + PAGE; });
        ASSERT_NE(found, subject->lazy_vmem_ranges.end());
        EXPECT_EQ(found->file, &source_file);
        EXPECT_EQ(found->file_offset, 3 * PAGE);
        EXPECT_EQ(found->file_ino, source_file.stat.st_ino);
        EXPECT_EQ(found->kind, ker::mod::sched::task::LazyVmemKind::FILE_BACKED);
    }
    EXPECT_EQ(source_file.refcount.load(), 4);  // descriptor, two rows, registry.
    ASSERT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges[0].file, &source_file);
    EXPECT_EQ(host_file::reads + host_fixed::allocation_calls, 0U);
    EXPECT_EQ(model.live_refs(), 511U);
}
TEST_F(ManagedFileFixed, CacheLoanUsesActualWrapperAndPreservesSeparateCacheOwner) {
    seed();
    host_file::cache = true;
    auto* cached = model.allocate(0x69000000, 1, PAGE, Initialization::ZERO);
    ASSERT_NE(cached, nullptr);
    std::memset(cached, 0x6d, PAGE);
    host_file::cache_pages.push_back(cached);
    ASSERT_EQ(file_fixed(TEST_VA, PAGE - 17, ker::abi::vmem::PROT_READ, ker::abi::vmem::MAP_SHARED | ker::abi::vmem::MAP_FIXED), TEST_VA);
    ASSERT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges[0].length, PAGE - 17);
    EXPECT_EQ(vmem::g_file_mmap_ranges[0].file, &source_file);
    EXPECT_EQ(source_file.refcount.load(), 2);  // descriptor and exact requested-byte registry.
    EXPECT_EQ(frame(TEST_VA), cached);
    EXPECT_EQ(mm::phys::page_ref_get(cached), 2U);
    EXPECT_EQ(host_file::cache_hits, 1U);
    EXPECT_EQ(host_file::batch_flushes, 1U);
    EXPECT_EQ(host_file::reads + host_fixed::allocation_calls, 0U);
}
TEST_F(ManagedFileFixed, CacheMissFallsBackToOriginalEagerBacking) {
    seed();
    host_file::cache = true;
    ASSERT_EQ(file_fixed(TEST_VA, PAGE, ker::abi::vmem::PROT_READ), TEST_VA);
    contents(TEST_VA, 0);
    EXPECT_EQ(host_file::cache_hits, 0U);
    EXPECT_EQ(host_file::batch_flushes, 1U);
    EXPECT_EQ(host_fixed::allocation_calls, 1U);
}
TEST_F(ManagedFileFixed, AsDenialKeepsOldLeafAndDoesNotReadBacking) {
    seed();
    host_fixed::admit = false;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -ENOMEM);
    unchanged();
    EXPECT_EQ(host_fixed::admissions, 1U);
    EXPECT_EQ(host_file::reads + host_fixed::allocation_calls, 0U);
    EXPECT_EQ(source_file.refcount.load(), 1);
}
TEST_F(ManagedFileFixed, DescriptionAndReadonlySharedDenialsPrecedePreparation) {
    seed();
    auto const pubs = host_fork_vm::pubs;
    source_file.open_flags = 1;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -EACCES);
    source_file.open_flags = 2;
    source_file.read_only = true;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE, 3, ker::abi::vmem::MAP_SHARED | ker::abi::vmem::MAP_FIXED)), -EACCES);
    EXPECT_EQ(host_fork_vm::pubs, pubs);
    EXPECT_EQ(host_file::reads + host_fixed::admissions, 0U);
    unchanged();
}
TEST_F(ManagedFileFixed, StatAndOffsetRefusalsPreserveOldOwnersAndDescription) {
    seed();
    auto const pubs = host_fork_vm::pubs;
    host_file::stat_error = -EIO;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -EINVAL);
    host_file::stat_error = 0;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE, 3, ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED,
                                              static_cast<uint64_t>(std::numeric_limits<off_t>::max()))),
              -EOVERFLOW);
    EXPECT_EQ(host_fork_vm::pubs, pubs);
    EXPECT_EQ(source_file.refcount.load(), 1);
    unchanged();
}
TEST_F(ManagedFileFixed, ReadFailureReturnsUnmappedPrefixAfterAckAndCandidateRef) {
    seed();
    host_file::fail_read_after = 1;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, 3 * PAGE)), -EFAULT);
    EXPECT_EQ(host_file::reads, 2U);
    EXPECT_EQ(host_fixed::rollback_acks, 1U);
    EXPECT_EQ(host_managed_remove::unmap_drops, 3U);
    EXPECT_EQ(model.live_refs(), 509U);
    EXPECT_EQ(raw(TEST_VA), 0U);
    EXPECT_EQ(raw(TEST_VA + PAGE), 0U);
    for (auto const& block : model.blocks)
        if (std::find(host_fixed::replacement_frames.begin(), host_fixed::replacement_frames.end(), block.physical) !=
            host_fixed::replacement_frames.end())
            for (auto refs : block.refs) EXPECT_EQ(refs, 0U);
}
TEST_F(ManagedFileFixed, AllocationFailureRollsBackOnlyIssuedPrefix) {
    seed();
    host_fixed::fail_after = 1;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, 3 * PAGE)), -ENOMEM);
    EXPECT_EQ(host_fixed::rollback_acks, 1U);
    EXPECT_EQ(model.live_refs(), 509U);
    EXPECT_EQ(raw(TEST_VA), 0U);
    EXPECT_EQ(raw(TEST_VA + PAGE), 0U);
    EXPECT_EQ(host_file::reads, 1U);
}
TEST_F(ManagedFileFixed, OldWritebackErrorWarnsAndContinuesWithoutPolicyChange) {
    mixed_seed();
    host_mixed::backend_error = -EIO;
    auto const address = TEST_VA + LARGE + 4 * PAGE;
    ASSERT_EQ(file_fixed(address, PAGE), address);
    contents(address, 0);
    EXPECT_EQ(host_mixed::backend_calls, 1U);
    EXPECT_EQ(file.refcount.load(), 3);  // untouched private rows plus original owner.
    EXPECT_TRUE(vmem::g_file_mmap_ranges.empty());
}
TEST_F(ManagedFileFixed, TypedTmpfsDispatchStaysBeforePreparationAndUnsupportedPort) {
    seed();
    source_file.fs_type = ker::vfs::FSType::TMPFS;
    source_file.fops = ker::vfs::tmpfs::get_tmpfs_fops();
    source_file.mount_owner = &source_file;
    auto const pubs = host_fork_vm::pubs;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE, 3, ker::abi::vmem::MAP_SHARED | ker::abi::vmem::MAP_FIXED)), -EOPNOTSUPP);
    EXPECT_EQ(host_file::typed_calls, 1U);
    EXPECT_EQ(host_fork_vm::pubs, pubs);
    unchanged();
}
TEST_F(ManagedFileFixed, PreparationAndLayoutRefuseBeforeOldDestinationRelease) {
    seed();
    fail_table_at = table_attempt;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -ENOMEM);
    fail_table_at = SIZE_MAX;
    actor.type = ker::mod::sched::task::TaskType::DAEMON;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -EWOULDBLOCK);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = 2 * PAGE;
    sibling.thread = &protected_layout;
    EXPECT_EQ(static_cast<int64_t>(file_fixed(TEST_VA, PAGE)), -EINVAL);
    EXPECT_EQ(host_fixed::admissions + host_file::reads, 0U);
    unchanged();
}
TEST_F(ManagedFileFixed, ZeroSurveyHighAddressAndLegacyNullDelegateAreDistinct) {
    auto const address = uint64_t{0x200000000000} + 3 * PAGE;
    reserve(address);
    *virt::leaf_entry(runtime_root, address) = {};
    ASSERT_EQ(file_fixed(address, PAGE), address);
    contents(address, 0);
    EXPECT_GT(address, UINT32_MAX);
    auto const next = address + PAGE;
    host_mixed::expected_pubs = host_fork_vm::pubs + 1;
    host_managed_remove::expected_unmaps.clear();
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_file::active = true;
    host_file::lazy = true;
    actor.mmap_next.store(next);
    // The public seven-argument delegate keeps original FIXED hint0 behavior.
    EXPECT_EQ(vmem::file_allocate(&actor, 0, PAGE, 3, ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_FIXED, 7, 0), next);
    host_fixed::active = host_protect::active = host_mixed::active = host_fork_vm::enabled = host_file::active = false;
    EXPECT_EQ(metadata_protection(actor, next), 3U);
    EXPECT_EQ(metadata_protection(sibling, next), 3U);
}
}  // namespace
