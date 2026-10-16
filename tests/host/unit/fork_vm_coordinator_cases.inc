// Actual ownership bodies and full copier are above. This ledger supplies only
// Task/current/physical/VFS/IRQ and deferred callback endpoints and fixture seeds.
namespace host_fork_vm {
int fail_after = -1;
unsigned metadata_allocations{}, metadata_returns{};
std::array<void*, 2048> metadata_live{};
auto take(std::size_t n) -> void* {
    if (enabled) {
        EXPECT_TRUE(ordinary());
        ++metadata_allocations;
        if (fail_after == 0) return nullptr;
        if (fail_after > 0) --fail_after;
    }
    auto* p = std::malloc(n ? n : 1);
    if (enabled && p) {
        for (auto& slot : metadata_live)
            if (!slot) {
                slot = p;
                return p;
            }
        std::abort();
    }
    return p;
}
void drop(void* p) {
    if (!p) return;
    for (auto& slot : metadata_live)
        if (slot == p) {
            EXPECT_TRUE(ordinary());
            slot = nullptr;
            ++metadata_returns;
            break;
        }
    std::free(p);
}
}  // namespace host_fork_vm
void* operator new(std::size_t n) {
    if (auto* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc{};
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    if (n == sizeof(huge::SplitPlanStorage)) {
        huge::Snapshot s{};
        huge_core.snapshot(s);
        if (s.plans == 0) heap_without_reservation = true;
        if (fail_plan_heap) {
            fail_plan_heap = false;
            return nullptr;
        }
    }
    return host_fork_vm::take(n);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept { return host_fork_vm::take(n); }
void operator delete(void* p) noexcept { host_fork_vm::drop(p); }
void operator delete[](void* p) noexcept { host_fork_vm::drop(p); }
void operator delete(void* p, std::size_t) noexcept { host_fork_vm::drop(p); }
void operator delete[](void* p, std::size_t) noexcept { host_fork_vm::drop(p); }
namespace {
namespace fm = mm::file_mapping;
namespace vmem = ker::syscall::vmem;
namespace shm = ker::syscall::shm;
class ForkVmCoordinator : public ManagedFork {
   protected:
    ker::mod::sched::task::Task child_task;
    ker::vfs::File file;
    fm::FileMappingLifetime* family{};
    void SetUp() override {
        ManagedFork::SetUp();
        child_task.pid = 2;
        child_task.pagemap = child;
        actor.pid = 1;
        host_fork_vm::registry_tries = host_fork_vm::registry_fail_try = 0;
        host_fork_vm::pubs = 0;
        host_fork_vm::on_pub = nullptr;
        host_fork_vm::on_pin = host_fork_vm::on_shm = host_fork_vm::on_legacy = nullptr;
        host_fork_vm::fail_after = -1;
        host_fork_vm::metadata_allocations = host_fork_vm::metadata_returns = 0;
        host_fork_vm::binding_reserves = host_fork_vm::binding_returns = 0;
        host_fork_vm::fail_binding_reserve = false;
        for (auto const& entry : host_fork_vm::metadata_live) ASSERT_EQ(entry, nullptr);
        ASSERT_TRUE(vmem::g_file_mmap_ranges.empty());
        ASSERT_EQ(virt::anonymous_swap_mappings, nullptr);
        fork_mode = true;
    }
    void rollback() {
        ASSERT_TRUE(host_fork_vm::ordinary());
        host_fork_vm::fail_after = -1;
        host_fork_vm::registry_fail_try = 0;
        // This is the explicit Host substitute for caller root teardown. It
        // purges actual metadata, and the existing physical fixture owns actual
        // PTE/data/table ref disposal after our INDEX TLB-return boundary.
        shm::shm_cleanup_for_task(&child_task);
        // These real inherited rows are not writeback ranges. Exercise the
        // actual complete unregister tail; backend writeback stays out of scope.
        ASSERT_EQ(vmem::unregister_file_mmap_range(child, 0, uint64_t{1} << 47), 0);
        ker::mod::sched::task::release_lazy_vmem_ranges(child_task);
        virt::purge_anonymous_swap_mappings(child);
        fm::FileMappingTlbRetirement retirement;
        auto flags = virt::user_mapping_pin_lock.lock_irqsave();
        ASSERT_TRUE(virt::file_mapping_index.detach_root_locked(child, virt::host_child_control.serial, retirement));
        virt::user_mapping_pin_lock.unlock_irqrestore(flags);
        // The physical fixture supplies this completed-ACK endpoint; no real
        // TLB/runtime result is claimed. It precedes actual TLB-role release.
        retirement.release_after_completed_tlb();
        fm::host_drain();
    }
    void TearDown() override {
        host_fork_vm::enabled = false;
        rollback();
        ker::mod::sched::task::release_lazy_vmem_ranges(actor);
        ASSERT_EQ(vmem::unregister_file_mmap_range(runtime_root, 0, uint64_t{1} << 47), 0);
        ker::util::SmallVec<vmem::FileMmapRange, 32> empty_registry_storage;
        vmem::g_file_mmap_ranges = std::move(empty_registry_storage);
        virt::purge_anonymous_swap_mappings(runtime_root);
        fm::FileMappingTlbRetirement retirement;
        ASSERT_TRUE(virt::file_mapping_index.detach_root_locked(runtime_root, runtime_serial, retirement));
        retirement.release_after_completed_tlb();
        if (family) {
            family->release(fm::FamilyOwner::PREPARED);
            family = nullptr;
        }
        fm::host_drain();
        for (auto& t : fm::host_tickets) ASSERT_EQ(t.run, nullptr);
        for (auto const& p : host_fork_vm::metadata_live) ASSERT_EQ(p, nullptr);
        ASSERT_EQ(actor.lifetime_refs, 1U);
        ASSERT_EQ(file.refcount.load(), 1);
        ASSERT_EQ(host_fork_vm::locks, 0U);
        for (auto& entry : shm::g_attachments) entry = {};
        for (auto& entry : shm::g_segments) entry = {};
        // Existing physical fixture owns all PTE references. Reduce child
        // aliases before its one-ref-only ordinary cleanup endpoint.
        huge_check_free = false;
        for (auto& block : model.blocks)
            for (size_t page = 0; page < block.refs.size(); ++page)
                while (block.refs[page] > 1) (void)mm::phys::page_ref_dec(static_cast<uint8_t*>(block.storage.get()) + page * PAGE);
        ManagedFork::TearDown();
    }
    auto run(numa::AllocationContext* placement = nullptr, numa::AllocationResult* output = nullptr) -> int {
        ker::mod::sys::SyscallPreparationScope preparation(actor);
        host_fork_vm::enabled = true;
        auto status = ker::syscall::process::detail::prepare_fork_vm_snapshot(actor, child_task, preparation, placement, output);
        host_fork_vm::enabled = false;
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers, 0U);
        EXPECT_FALSE(host_range::exclusive);
        return status;
    }
    void lazy(uint64_t va, ker::mod::sched::task::LazyVmemKind kind) {
        ker::mod::sched::task::LazyVmemRange row;
        row.start = va;
        row.end = va + PAGE;
        row.prot = 3;
        row.kind = kind;
        if (kind == ker::mod::sched::task::LazyVmemKind::FILE_BACKED) {
            ASSERT_TRUE(ker::vfs::vfs_try_retain_file(&file));
            row.file = &file;
        } else if (ker::mod::sched::task::lazy_vmem_has_lifetime(row)) {
            ASSERT_NE(family, nullptr);
            ASSERT_TRUE(family->try_acquire(fm::FamilyOwner::LAZY));
            row.lifetime = family;
        }
        ASSERT_TRUE(actor.lazy_vmem_ranges.push_back(row));
    }
    void legacy(uint64_t va = TEST_VA) {
        ASSERT_TRUE(ker::vfs::vfs_try_retain_file(&file));
        vmem::FileMmapRange row;
        row.pagemap = runtime_root;
        row.start = va;
        row.length = PAGE;
        row.file = &file;
        ASSERT_TRUE(vmem::g_file_mmap_ranges.push_back(row));
        lazy(va, ker::mod::sched::task::LazyVmemKind::FILE_BACKED);
    }
    void tmpfs() {
        family = new (std::nothrow) fm::FileMappingLifetime(fm::host_port);
        ASSERT_NE(family, nullptr);
        ASSERT_TRUE(ker::vfs::vfs_try_retain_file(&file));
        family->file_ = &file;
        family->backing_mode_ = fm::FileMappingBackingMode::TMPFS_DEMAND_WRITE_SHARED;
        family->ticket_ = fm::host_reserve(nullptr, fm::FileMappingLifetime::finish, family);
        for (unsigned i = 0; i < 2; ++i) {
            auto va = TEST_VA + PAGE * (i + 2);
            auto* binding = fm::FileMappingBinding::prepare_unbound(*family);
            ASSERT_NE(binding, nullptr);
            ASSERT_TRUE(binding->configure_unpublished(runtime_root, runtime_serial, va, va + PAGE, 0));
            ASSERT_TRUE(virt::file_mapping_index.attach_locked(binding, runtime_serial));
            ASSERT_EQ(binding, nullptr);
            lazy(va, ker::mod::sched::task::LazyVmemKind::TMPFS_WRITE_SHARED);
        }
    }
    void shared() {
        auto& seg = shm::g_segments[0];
        seg.active = true;
        seg.id = 7;
        seg.size = PAGE;
        seg.attach_count = 1;
        auto& row = shm::g_attachments[0];
        row.active = true;
        row.pid = actor.pid;
        row.shmid = 7;
        row.addr = TEST_VA + 8 * PAGE;
        row.size = PAGE;
    }
    void marker(uint64_t va, virt::AnonymousSwapRuntimeState state, bool object) {
        auto* cursor = runtime_root;
        for (int level = 4; level > 1; --level) {
            auto& entry = word(cursor, virt::index_of(va, level));
            if (!entry) entry = reinterpret_cast<uint64_t>(table()) | 7;
            cursor = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        auto* row = new virt::AnonymousSwapMapping{};
        row->pagemap = runtime_root;
        row->vaddr = va;
        row->state = state;
        row->original_raw = mm::paging::PAGE_USER | mm::paging::PAGE_WRITE;
        row->generation = 10 + va / PAGE;
        row->maintenance_valid = true;
        row->maintenance_placement.local_node = 0;
        row->error = state == virt::AnonymousSwapRuntimeState::FAILED ? -EIO : 0;
        if (object) {
            row->object = new virt::AnonymousSwapObject{};
            row->object->mapping_refs = 1;
        }
        word(cursor, virt::index_of(va, 1)) = virt::anonymous_swap_marker_raw(row->original_raw);
        virt::insert_anonymous_swap_mapping_locked(row);
        if (state == virt::AnonymousSwapRuntimeState::SWAPPED) ++virt::anonymous_swap_stats.swapped_pages;
    }
};
TEST_F(ForkVmCoordinator, CountZeroUsesWholeSurveyAndOneOriginalExclusive) {
    actor.entry = 41;
    actor.program_header_count = 7;
    actor.mmap_next = 0x78000000;
    EXPECT_EQ(run(), 0);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(fork_cow_entries, 1U);
    EXPECT_EQ(fork_reader_attempts, 0U);
    EXPECT_EQ(child_task.entry, 41U);
    EXPECT_EQ(child_task.program_header_count, 7U);
    EXPECT_EQ(child_task.mmap_next.load(), 0x78000000U);
    EXPECT_GT(host_fork_vm::pubs, 0U);
}
TEST_F(ForkVmCoordinator, MixedFileMetadataKeepDistinctOwners) {
    legacy();
    tmpfs();
    EXPECT_EQ(run(), 0);
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 3U);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 2U);
    size_t n = 0;
    ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, n));
    EXPECT_EQ(n, 2U);
    EXPECT_EQ(family->owner_counts().count(fm::FamilyOwner::LAZY), 4U);
    EXPECT_EQ(fork_exclusives, 1U);
}
TEST_F(ForkVmCoordinator, SpilledMixedMetadataMovesRealHeapAndReturnsOrdinarily) {
    for (unsigned i = 0; i < 40; ++i) legacy(TEST_VA + PAGE * (100 + i));
    tmpfs();
    ASSERT_TRUE(actor.lazy_vmem_ranges.is_spilled());
    ASSERT_TRUE(vmem::g_file_mmap_ranges.is_spilled());
    ASSERT_EQ(run(), 0);
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 42U);
    EXPECT_TRUE(child_task.lazy_vmem_ranges.is_spilled());
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 80U);
    size_t n = 0;
    ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, n));
    EXPECT_EQ(n, 2U);
    rollback();
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 40U);
    EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
}
TEST_F(ForkVmCoordinator, ShmWholeTableCommitAndLegacyRows) {
    legacy();
    shared();
    EXPECT_EQ(run(), 0);
    EXPECT_EQ(shm::g_segments[0].attach_count, 2U);
    unsigned n = 0;
    for (auto const& r : shm::g_attachments) n += r.active && r.pid == child_task.pid;
    EXPECT_EQ(n, 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 2U);
}
TEST_F(ForkVmCoordinator, SwappedAndBothFailedStatesTransferActualMarkers) {
    marker(TEST_VA, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    marker(TEST_VA + PAGE, virt::AnonymousSwapRuntimeState::FAILED, true);
    marker(TEST_VA + 2 * PAGE, virt::AnonymousSwapRuntimeState::FAILED, false);
    EXPECT_EQ(run(), 0);
    unsigned n = 0;
    for (auto* r = virt::anonymous_swap_mappings; r; r = r->next)
        if (r->pagemap == child) {
            ++n;
            EXPECT_EQ(virt::pte_raw(*virt::leaf_entry(child, r->vaddr)), virt::pte_raw(*virt::leaf_entry(runtime_root, r->vaddr)));
            EXPECT_EQ(r->pagein_allocation, virt::AnonymousPageinAllocation::PREPARED);
            if (r->object) EXPECT_EQ(r->object->mapping_refs, 2U);
        }
    EXPECT_EQ(n, 3U);
    EXPECT_EQ(fork_swap_clones, 0U);
}
TEST_F(ForkVmCoordinator, InvalidCurrentAndIfContextRefuseBeforeOwners) {
    host_range::queryable = false;
    EXPECT_EQ(run(), -EWOULDBLOCK);
    host_range::queryable = true;
    actor.syscall_account_start_us = 0;
    EXPECT_EQ(run(), -EWOULDBLOCK);
    actor.syscall_account_start_us = 1;
    actor.type = ker::mod::sched::task::TaskType::THREAD;
    EXPECT_EQ(run(), -EWOULDBLOCK);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    EXPECT_EQ(fork_exclusives, 0U);
    EXPECT_EQ(host_fork_vm::metadata_allocations, 0U);
}
TEST_F(ForkVmCoordinator, NativeSourceAndMixedBoundaryRemainRefused) {
    legacy();
    ker::mod::sched::task::LazyVmemRange row;
    row.kind = ker::mod::sched::task::LazyVmemKind::NATIVE_FILE_BACKED;
    ASSERT_TRUE(actor.lazy_vmem_ranges.push_back(row));
    EXPECT_EQ(run(), -EBUSY);
    actor.lazy_vmem_ranges.pop_back();
    vmem::NativeMappingEntry native;
    native.root = runtime_root;
    vmem::g_native_file_mmap_ranges = &native;
    EXPECT_EQ(run(), -EOPNOTSUPP);
    vmem::g_native_file_mmap_ranges = nullptr;
    EXPECT_EQ(fork_exclusives, 0U);
}
TEST_F(ForkVmCoordinator, HistoricallyActiveChildRefusedBeforeCopy) {
    child_task.has_run = true;
    EXPECT_EQ(run(), -EINVAL);
    child_task.has_run = false;
    EXPECT_EQ(fork_exclusives, 0U);
}
TEST_F(ForkVmCoordinator, AllPreparationAllocationsRefuseWithoutLeakedOwners) {
    legacy();
    tmpfs();
    shared();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    unsigned failed = 0;
    for (int fail = 0; fail < 12; ++fail) {
        host_fork_vm::fail_after = fail;
        int status = run();
        if (status == 0) {
            rollback();
            break;
        }
        EXPECT_EQ(status, -ENOMEM);
        ++failed;
        EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
        fm::host_drain();
        for (auto* p : host_fork_vm::metadata_live) EXPECT_EQ(p, nullptr);
    }
    host_fork_vm::fail_after = -1;
    EXPECT_GE(failed, 6U);
}
TEST_F(ForkVmCoordinator, FinalLegacyValidationBusyNeverConsumesChild) {
    legacy();
    host_fork_vm::registry_fail_try = 1;
    EXPECT_EQ(run(), -EAGAIN);
    EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
    EXPECT_EQ(fork_exclusives, 0U);
}
TEST_F(ForkVmCoordinator, LaterLegacyCommitRefusalRetainsEveryAdoptedPrefix) {
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    shared();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    host_fork_vm::registry_fail_try = 2;
    EXPECT_EQ(run(), -EAGAIN);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 3U);
    EXPECT_EQ(shm::g_segments[0].attach_count, 2U);
    size_t n = 0;
    ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, n));
    EXPECT_EQ(n, 2U);
    unsigned swaps = 0;
    for (auto* r = virt::anonymous_swap_mappings; r; r = r->next) swaps += r->pagemap == child;
    EXPECT_EQ(swaps, 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    rollback();
    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
}
TEST_F(ForkVmCoordinator, CopierOomLeavesLazyPrefixAndAcknowledgesParent) {
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    fork_fail_table_at = 2;
    EXPECT_EQ(run(), -ENOMEM);
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 3U);
    EXPECT_GE(actual_flushes, 1U);
    size_t n = 0;
    ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, n));
    EXPECT_EQ(n, 0U);
}
TEST_F(ForkVmCoordinator, ManagedSplitAckPrecedesEagerCowWithCallerStorage) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    fork_eager = true;
    numa::AllocationContext placement = context(numa::Mode::BIND, 2);
    numa::AllocationResult output;
    EXPECT_EQ(run(&placement, &output), 0);
    EXPECT_TRUE(output.span.empty());
    EXPECT_EQ(tracked_pages, 512U);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(fork_reader_attempts, 0U);
    mapped[0] = false;
    cleanup[0].reset();
    auto ack = std::find(fork_events.begin(), fork_events.end(), "ACK"), cow = std::find(fork_events.begin(), fork_events.end(), "COW");
    ASSERT_NE(ack, fork_events.end());
    ASSERT_NE(cow, fork_events.end());
    EXPECT_LT(ack, cow);
}
unsigned inject_at{}, inject_count{};
ker::mod::sched::task::Task* inject_child{};
TEST_F(ForkVmCoordinator, FinalTmpfsMatchesRefusesActualReaderWithoutExclusive) {
    tmpfs();
    ASSERT_EQ(run(), 0);
    unsigned const publications = host_fork_vm::pubs;
    rollback();
    inject_at = publications * 2;
    host_fork_vm::on_pub = +[] {
        if (host_fork_vm::pubs == inject_at) host_range::reader_busy = true;
    };
    EXPECT_EQ(run(), -EAGAIN);
    host_range::reader_busy = false;
    host_fork_vm::on_pub = nullptr;
    EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
    EXPECT_EQ(fork_exclusives, 1U);
}
TEST_F(ForkVmCoordinator, TmpfsSecondAttachFailureLeavesExactFirstIndexOwner) {
    tmpfs();
    inject_count = 0;
    host_fork_vm::on_pin = +[] {
        if (fork_cow_entries && !host_range::exclusive && ++inject_count == 2) virt::host_child_control.serial = 0;
    };
    EXPECT_EQ(run(), -EAGAIN);
    host_fork_vm::on_pin = nullptr;
    virt::host_child_control.serial = 23;
    size_t n = 0;
    ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, n));
    EXPECT_EQ(n, 1U);
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 2U);
    EXPECT_EQ(fork_exclusives, 1U);
}
TEST_F(ForkVmCoordinator, ShmFinalCapacityAndRefOverflowKeepTmpfsPrefixOnly) {
    tmpfs();
    shared();
    for (unsigned axis = 0; axis < 2; ++axis) {
        inject_count = 0;
        inject_at = axis;
        host_fork_vm::on_shm = +[] {
            if (++inject_count != 4) return;
            if (inject_at)
                shm::g_segments[0].attach_count = UINT64_MAX;
            else
                for (auto& r : shm::g_attachments)
                    if (!r.active) {
                        r.active = true;
                        r.pid = 99;
                    }
        };
        EXPECT_EQ(run(), axis ? -EOVERFLOW : -ENOSPC);
        host_fork_vm::on_shm = nullptr;
        for (auto& r : shm::g_attachments)
            if (r.active && r.pid == 99) r = {};
        if (axis) shm::g_segments[0].attach_count = 1;
        unsigned n = 0;
        for (auto const& r : shm::g_attachments) n += r.active && r.pid == child_task.pid;
        EXPECT_EQ(n, 0U);
        size_t bindings = 0;
        ASSERT_TRUE(virt::file_mapping_index.count_root_locked(child, 23, bindings));
        EXPECT_EQ(bindings, 2U);
        rollback();
    }
}
TEST_F(ForkVmCoordinator, LazyConsumeRefusalSkipsEveryLaterAdoption) {
    legacy();
    inject_child = &child_task;
    host_fork_vm::on_legacy = +[] {
        ker::mod::sched::task::LazyVmemRange row;
        row.start = TEST_VA + 30 * PAGE;
        row.end = row.start + PAGE;
        EXPECT_TRUE(inject_child->lazy_vmem_ranges.push_back_no_grow(row));
    };
    EXPECT_EQ(run(), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    inject_child = nullptr;
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 1U);
    EXPECT_EQ(fork_exclusives, 0U);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
}
TEST_F(ForkVmCoordinator, WholeGeometryUnsupportedAndExhaustedRemainExact) {
    auto* next = table();
    word(runtime_root, virt::index_of(TEST_VA, 4)) = reinterpret_cast<uint64_t>(next) | 7;
    word(next, virt::index_of(TEST_VA, 3)) = 0x87;
    EXPECT_EQ(run(), -EOPNOTSUPP);
    word(runtime_root, virt::index_of(TEST_VA, 4)) = 0;
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    EXPECT_EQ(run(), -EOVERFLOW);
    EXPECT_EQ(fork_exclusives, 0U);
    EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
}
TEST_F(ForkVmCoordinator, SwapFreshPreflightRejectsAfterLazyOwnershipWithoutChildPtes) {
    legacy();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    host_fork_vm::on_legacy = +[] { virt::anonymous_swap_mappings->pagein_queued = true; };
    EXPECT_EQ(run(), -EAGAIN);
    host_fork_vm::on_legacy = nullptr;
    virt::anonymous_swap_mappings->pagein_queued = false;
    EXPECT_EQ(child_task.lazy_vmem_ranges.size(), 1U);
    EXPECT_EQ(fork_cow_entries, 0U);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(word(child, 0), 0U);
}
TEST_F(ForkVmCoordinator, LateActorContextRefusesBeforeChildConsumption) {
    legacy();
    ASSERT_EQ(run(), 0);
    unsigned const publications = host_fork_vm::pubs;
    rollback();
    inject_at = publications * 2;
    host_fork_vm::on_pub = +[] {
        if (host_fork_vm::pubs == inject_at) host_range::current->syscall_account_start_us = 0;
    };
    EXPECT_EQ(run(), -EAGAIN);
    host_fork_vm::on_pub = nullptr;
    actor.syscall_account_start_us = 1;
    EXPECT_TRUE(child_task.lazy_vmem_ranges.empty());
    EXPECT_EQ(fork_exclusives, 1U);
}
}  // namespace
