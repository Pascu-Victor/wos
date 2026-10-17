// Actual SHM algorithms and PTE observers with finite namespace, AS and backend
// endpoints. These cases make no native credential/quota/SMP or publicPS claim.
namespace {
class ManagedShmOccupancy : public ManagedAnonymousRemove {
   protected:
    using Task = ker::mod::sched::task::Task;
    void* segment_backing{};
    static constexpr int SEGMENT = 41;
    void SetUp() override {
        ManagedAnonymousRemove::SetUp();
        // Ordinary SHM frames carry no managed huge-page quota. Their exact
        // refs and ACK-before-drop are checked by this profile's own endpoints.
        huge_check_free = false;
        host_shm::scope = {};
        host_shm::enabled = true;
        host_shm::missing_namespace = false;
        host_shm::allowed_access = host_shm::allowed_as = true;
        host_shm::access_calls = host_shm::as_calls = host_shm::maps = host_shm::acks = host_shm::drops = host_shm::new_tables = 0;
        host_shm::requested_access = 0;
        host_shm::on_admit = nullptr;
        host_shm::after_map = nullptr;
        host_shm::pending_drop = nullptr;
        host_shm::published.clear();
        for (auto const& entry : shm::g_attachments) ASSERT_FALSE(entry.active || entry.publishing);
        for (auto const& entry : shm::g_segments) ASSERT_FALSE(entry.active);
    }
    void TearDown() override {
        host_shm::on_admit = nullptr;
        host_shm::after_map = nullptr;
        actor.owner = nullptr;
        EXPECT_EQ(host_shm::pending_drop, nullptr);
        EXPECT_EQ(host_shm::acks, host_shm::drops);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(root_readers + cow_depth + pin_depth + publication_depth, 0U);
        for (auto const& entry : shm::g_attachments) EXPECT_FALSE(entry.active || entry.publishing);
        EXPECT_EQ(shm::g_segments[0].attach_count, 0U);
        if (shm::g_segments[0].active) shm::release_segment(shm::g_segments[0]);
        host_shm::enabled = false;
        ManagedAnonymousRemove::TearDown();
    }
    void segment(unsigned pages = 2) {
        ASSERT_FALSE(shm::g_segments[0].active);
        segment_backing = model.allocate(model.topology.memory[0].base + 16 * LARGE, pages, PAGE, Initialization::ZERO);
        ASSERT_NE(segment_backing, nullptr);
        shm::g_segments[0] = {.active = true,
                              .id = SEGMENT,
                              .key = 79,
                              .backing = segment_backing,
                              .size = pages * PAGE,
                              .page_count = pages,
                              .creator_pid = 1,
                              .owner_pid = 1,
                              .last_pid = 1,
                              .creator_pid_scope_generation = host_shm::scope.generation(),
                              .visible_creator_pid = 1,
                              .visible_last_pid = 1,
                              .ipc_namespace = host_shm::scope};
    }
    auto path(uint64_t va, int desired = 1) -> uint64_t& {
        auto* cursor = runtime_root;
        for (int level = 4; level > desired; --level) {
            auto& raw = word(cursor, virt::index_of(va, level));
            if (raw & 128) std::abort();
            if (!raw) {
                auto* next = table();
                raw = reinterpret_cast<uint64_t>(next) | 7;
            }
            cursor = reinterpret_cast<mm::paging::PageTable*>(raw & virt::PTE_FRAME_MASK);
        }
        return word(cursor, virt::index_of(va, desired));
    }
    auto attach(uint64_t address, int flags = 0) -> int64_t {
        auto result = static_cast<int64_t>(shm::shmat_impl(&actor, SEGMENT, address, flags));
        EXPECT_EQ(host_fork_vm::locks + publication_depth + root_readers + cow_depth + pin_depth, 0U);
        EXPECT_TRUE(host_range::interrupts);
        return result;
    }
    void no_attachment() {
        EXPECT_EQ(shm::g_segments[0].attach_count, 0U);
        for (auto const& entry : shm::g_attachments) EXPECT_FALSE(entry.active || entry.publishing);
    }
    void untouched(uint64_t refs, size_t allocations) {
        no_attachment();
        EXPECT_EQ(model.live_refs(), refs);
        EXPECT_EQ(model.calls.size(), allocations);
        EXPECT_EQ(host_shm::maps + host_shm::acks + host_shm::drops + host_shm::new_tables, 0U);
    }
};

TEST_F(ManagedShmOccupancy, EmptyFourKReservedLazyAndInitialLayoutCompatibility) {
    segment(1);
    EXPECT_TRUE(shm::range_is_free(&actor, TEST_VA, PAGE));
    ordinary(TEST_VA);
    EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA, PAGE));
    auto& leaf = path(TEST_VA);
    auto* mapped = reinterpret_cast<void*>(mm::addr::get_virt_pointer(leaf & virt::PTE_FRAME_MASK));
    leaf = mm::paging::PAGE_RESERVED;
    EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA, PAGE));
    EXPECT_EQ(attach(TEST_VA), -EINVAL);
    leaf = 0;
    EXPECT_EQ(mm::phys::page_ref_dec(mapped), 0U);
    row(actor, TEST_VA, TEST_VA + PAGE);
    EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA, PAGE));
    ker::mod::sched::task::release_lazy_vmem_ranges(actor);
    protected_layout.layout_base_virt = TEST_VA;
    protected_layout.layout_size = PAGE;
    actor.thread = &protected_layout;
    EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA, PAGE));
    actor.thread = nullptr;
    sibling.thread = &protected_layout;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA, PAGE));
    }
    sibling.thread = nullptr;
    EXPECT_TRUE(shm::range_is_free(&actor, TEST_VA, PAGE));
    no_attachment();
}

TEST_F(ManagedShmOccupancy, RegisteredTwoMegabyteFirstInteriorLastAndCrossBoundaryAreOccupied) {
    segment(1);
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    auto const raw = slot_at(runtime_root, TEST_VA, 2);
    auto const refs = model.live_refs();
    auto const allocations = model.calls.size();
    for (auto address : {TEST_VA, TEST_VA + 17 * PAGE, TEST_VA + LARGE - PAGE}) {
        EXPECT_FALSE(shm::range_is_free(&actor, address, PAGE));
        EXPECT_EQ(attach(address), -EINVAL);
        EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2), raw);
    }
    EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA - PAGE, 2 * PAGE));
    EXPECT_EQ(host_shm::as_calls, 0U);
    untouched(refs, allocations);
}

TEST_F(ManagedShmOccupancy, UnknownTwoMegabyteAndSupervisorLeavesRejectWithoutDereference) {
    segment(1);
    auto& destination = path(TEST_VA, 2);
    auto const refs = model.live_refs();
    auto const allocations = model.calls.size();
    for (uint64_t flags : {uint64_t{0x87}, uint64_t{0x81}}) {
        destination = 0x60000000 | flags;
        auto const raw = destination;
        EXPECT_FALSE(shm::range_is_free(&actor, TEST_VA + 4 * PAGE, PAGE));
        EXPECT_EQ(attach(TEST_VA + 4 * PAGE), -EINVAL);
        EXPECT_EQ(destination, raw);
        untouched(refs, allocations);
    }
    destination = 0;
}

TEST_F(ManagedShmOccupancy, OneGigabyteAndSupervisorLeavesRejectWithoutDereference) {
    segment(1);
    constexpr uint64_t address = uint64_t{3} << 30;
    auto& destination = path(address, 3);
    auto const refs = model.live_refs();
    auto const allocations = model.calls.size();
    for (uint64_t flags : {uint64_t{0x87}, uint64_t{0x81}}) {
        destination = 0x80000000 | flags;
        auto const raw = destination;
        for (auto offset : {uint64_t{0}, uint64_t{16} * PAGE, (uint64_t{1} << 30) - PAGE}) {
            EXPECT_FALSE(shm::range_is_free(&actor, address + offset, PAGE));
            EXPECT_EQ(attach(address + offset), -EINVAL);
        }
        EXPECT_EQ(destination, raw);
        untouched(refs, allocations);
    }
    destination = 0;
}

TEST_F(ManagedShmOccupancy, AutomaticSearchSkipsWholePsExtentAndHonorsFreeHint) {
    segment(1);
    uint64_t const start = shm::SHM_SEARCH_START;
    auto& destination = path(start, 2);
    destination = 0x60000087;
    EXPECT_EQ(shm::find_free_range(&actor, PAGE, start), start + LARGE);
    EXPECT_EQ(shm::find_free_range(&actor, PAGE, start + 3 * LARGE), start + 3 * LARGE);
    EXPECT_EQ(attach(0), static_cast<int64_t>(start + LARGE));
    EXPECT_EQ(destination, 0x60000087U);
    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
    EXPECT_EQ(mm::phys::page_ref_get(segment_backing), 2U);
    EXPECT_EQ(shm::shmdt_impl(&actor, start + LARGE), 0U);
    EXPECT_EQ(mm::phys::page_ref_get(segment_backing), 1U);
    EXPECT_EQ(host_shm::acks, 1U);
    no_attachment();
    destination = 0;
}

TEST_F(ManagedShmOccupancy, NamespaceAndPermissionFailuresPrecedePublishingAndAs) {
    segment(1);
    auto const refs = model.live_refs();
    auto const allocations = model.calls.size();
    host_shm::missing_namespace = true;
    EXPECT_EQ(attach(TEST_VA), -ESRCH);
    host_shm::missing_namespace = false;
    host_shm::scope.retired = true;
    EXPECT_EQ(attach(TEST_VA), -EIDRM);
    host_shm::scope.retired = false;
    host_shm::scope.token = 7;
    EXPECT_EQ(attach(TEST_VA), -EINVAL);
    host_shm::scope.token = 1;
    host_shm::allowed_access = false;
    EXPECT_EQ(attach(TEST_VA), -EACCES);
    EXPECT_EQ(host_shm::access_calls, 1U);
    EXPECT_EQ(host_shm::as_calls, 0U);
    EXPECT_EQ(host_shm::requested_access, 6U);
    untouched(refs, allocations);
}

TEST_F(ManagedShmOccupancy, AsDenialReturnsEveryPublishingOwnerBeforeMapping) {
    segment(1);
    host_shm::allowed_as = false;
    auto const refs = model.live_refs();
    auto const allocations = model.calls.size();
    EXPECT_EQ(attach(TEST_VA), -ENOMEM);
    EXPECT_EQ(host_shm::access_calls, 1U);
    EXPECT_EQ(host_shm::as_calls, 1U);
    untouched(refs, allocations);
}

TEST_F(ManagedShmOccupancy, SharedReadonlyAttachDetachKeepsExactRefsFlagsAndHistory) {
    segment(2);
    actor.owner = &sibling;
    for (int flags : {0, ker::abi::shm::SHM_RDONLY}) {
        EXPECT_EQ(attach(TEST_VA, flags), static_cast<int64_t>(TEST_VA));
        EXPECT_EQ(host_shm::requested_access, flags ? 4U : 6U);
        EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
        EXPECT_EQ(shm::g_segments[0].last_pid, sibling.pid);
        EXPECT_EQ(shm::g_segments[0].visible_last_pid, sibling.pid);
        auto const* attachment = shm::find_attachment(sibling.pid, TEST_VA);
        ASSERT_NE(attachment, nullptr);
        EXPECT_TRUE(attachment->active);
        EXPECT_FALSE(attachment->publishing);
        for (unsigned page = 0; page < 2; ++page) {
            auto const raw = path(TEST_VA + page * PAGE);
            EXPECT_NE(raw & mm::paging::PAGE_SHARED, 0U);
            EXPECT_EQ((raw & mm::paging::PAGE_WRITE) != 0, flags == 0);
            EXPECT_EQ((raw & mm::paging::PAGE_WRITE_RESTRICTED) != 0, flags != 0);
            EXPECT_EQ(mm::phys::page_ref_get(static_cast<uint8_t*>(segment_backing) + page * PAGE), 2U);
        }
        if (flags) shm::g_segments[0].marked_destroy = true;
        EXPECT_EQ(shm::shmdt_impl(&actor, TEST_VA), 0U);
        no_attachment();
        if (!flags) EXPECT_EQ(mm::phys::page_ref_get(segment_backing), 1U);
    }
    EXPECT_FALSE(shm::g_segments[0].active);
    EXPECT_EQ(host_shm::maps, 4U);
    EXPECT_EQ(host_shm::acks, 4U);
    EXPECT_EQ(host_shm::drops, 4U);
    actor.owner = nullptr;
}

TEST_F(ManagedShmOccupancy, FinalPsGuardRollsBackMappedPrefixBeforeAbandoning) {
    segment(2);
    uint64_t const first = TEST_VA + LARGE - PAGE;
    auto& next = path(first + PAGE, 2);
    host_shm::on_admit = [] { slot_at(runtime_root, TEST_VA + LARGE, 2) = 0x60000087; };
    auto const refs = model.live_refs();
    EXPECT_EQ(attach(first), -EINVAL);
    EXPECT_EQ(next, 0x60000087U);
    EXPECT_EQ(path(first), 0U);
    EXPECT_EQ(model.live_refs(), refs);
    no_attachment();
    EXPECT_EQ(host_shm::maps, 1U);
    EXPECT_EQ(host_shm::acks, 1U);
    EXPECT_EQ(host_shm::drops, 1U);
    next = 0;
}

TEST_F(ManagedShmOccupancy, FinalPsGuardPreservesReplacementPaAndMarkedSegmentLifetime) {
    segment(2);
    uint64_t const first = TEST_VA + LARGE - PAGE;
    auto& next = path(first + PAGE, 2);
    auto* replacement = model.allocate(model.topology.memory[0].base + 32 * LARGE, 1, PAGE, Initialization::ZERO);
    ASSERT_NE(replacement, nullptr);
    host_shm::on_admit = [] {
        slot_at(runtime_root, TEST_VA + LARGE, 2) = 0x60000087;
        shm::g_segments[0].marked_destroy = true;
    };
    host_shm::after_map = [](uint64_t address) {
        auto& old = slot_at(runtime_root, address, 1);
        auto* previous = reinterpret_cast<void*>(mm::addr::get_virt_pointer(old & virt::PTE_FRAME_MASK));
        EXPECT_EQ(mm::phys::page_ref_dec(previous), 1U);
        auto const pa = backend().topology.memory[0].base + 32 * LARGE;
        auto* page = reinterpret_cast<void*>(mm::addr::get_virt_pointer(pa));
        mm::phys::page_ref_inc(page);
        old = pa | 7;
    };
    EXPECT_EQ(attach(first), -EINVAL);
    EXPECT_EQ(next, 0x60000087U);
    EXPECT_EQ(path(first) & virt::PTE_FRAME_MASK, model.topology.memory[0].base + 32 * LARGE);
    EXPECT_EQ(mm::phys::page_ref_get(replacement), 2U);
    EXPECT_FALSE(shm::g_segments[0].active);
    no_attachment();
    EXPECT_EQ(host_shm::acks + host_shm::drops, 0U);
    path(first) = 0;
    EXPECT_EQ(mm::phys::page_ref_dec(replacement), 1U);
    EXPECT_EQ(mm::phys::page_ref_dec(replacement), 0U);
    next = 0;
}
}  // namespace
