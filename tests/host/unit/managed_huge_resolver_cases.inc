}  // namespace ker::mod::mm::virt
namespace {
namespace virt = mm::virt;
constexpr uint64_t TEST_VA = 0x72000000ULL;
auto word(mm::paging::PageTable* p, size_t i) -> uint64_t& { return reinterpret_cast<uint64_t*>(p)[i]; }
auto slot_at(mm::paging::PageTable* root, uint64_t va, int desired) -> uint64_t& {
    auto* table = root;
    for (int level = 4; level > desired; --level)
        table = reinterpret_cast<mm::paging::PageTable*>(
            mm::addr::get_virt_pointer(word(table, virt::index_of(va, level)) & virt::PTE_FRAME_MASK));
    return word(table, virt::index_of(va, desired));
}
class ManagedResolver : public HugeOwnership {
   protected:
    huge::PreparedSpan data[2];
    huge::PreparedTables parents[2], cleanup[2];
    uint8_t* bytes[2]{};
    bool mapped[2]{};
    void SetUp() override {
        HugeOwnership::SetUp();
        runtime_root = static_cast<mm::paging::PageTable*>(std::aligned_alloc(PAGE, PAGE));
        ASSERT_NE(runtime_root, nullptr);
        std::memset(runtime_root, 0, PAGE);
        runtime_serial = 19;
        publication_depth = cow_depth = pin_depth = root_readers = 0;
        actual_flushes = tracked_pages = 0;
        binding_matches = true;
        span_at_flush = nullptr;
        plan_at_flush = nullptr;
    }
    auto prepare(size_t i) -> huge::Error {
        huge::RootKey key{reinterpret_cast<uint64_t>(runtime_root), runtime_serial};
        auto e = data[i].prepare(key, TEST_VA + i * LARGE, context(numa::Mode::BIND, 1));
        if (e != huge::Error::None) return e;
        bytes[i] = static_cast<uint8_t*>(data[i].data());
        if ((e = parents[i].prepare(2)) != huge::Error::None) return e;
        return cleanup[i].prepare(1);
    }
    auto publish(size_t i, uint64_t flags = 0x8000000000000007ULL) -> huge::Error {
        span_at_flush = &data[i];
        auto e = virt::publish_managed_2m_prepared(runtime_root, TEST_VA + i * LARGE, flags, data[i], parents[i], cleanup[i]);
        span_at_flush = nullptr;
        mapped[i] = e == huge::Error::None;
        return e;
    }
    auto split(huge::PreparedTables& plan) -> huge::Error {
        plan_at_flush = huge::detail::PreparationAccess::plan_storage(plan);
        huge::Error e;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            e = virt::commit_managed_split_plan(runtime_root, plan, publication);
        }
        plan_at_flush = nullptr;
        if (e == huge::Error::None) plan.reset();
        return e;
    }
    void TearDown() override {
        for (size_t i = 0; i < 2; ++i) {
            if (mapped[i]) {
                ASSERT_EQ(split(cleanup[i]), huge::Error::None);
                mapped[i] = false;
            }
            data[i].reset();
            parents[i].reset();
            cleanup[i].reset();
        }
        // Explicit Host mapping teardown: the actual split transferred each
        // reference, so only this endpoint releases the 512 mapping owners.
        huge_check_free = false;
        for (auto& b : model.blocks)
            if (b.storage)
                for (uint32_t i = 0; i < b.pages; ++i)
                    if (b.refs[i]) {
                        auto* pointer = static_cast<uint8_t*>(b.storage.get()) + uint64_t{i} * PAGE;
                        EXPECT_EQ(mm::phys::page_ref_dec(pointer), 0U);
                    }
        // PTs adopted by the real publication/split are now mapping-owned.
        for (auto* table : table_live) std::free(table);
        table_live.clear();
        std::free(runtime_root);
        runtime_root = nullptr;
        EXPECT_EQ(publication_depth + cow_depth + pin_depth + root_readers, 0U);
        HugeOwnership::TearDown();
    }
};
TEST_F(ManagedResolver, ActualPublisherTransfersDataOnceAndCleanupRetainsChargeThroughAck) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    EXPECT_TRUE(data[0].empty());
    data[0].reset();
    EXPECT_EQ(model.live_refs(), 512U);
    EXPECT_TRUE(parents[0].empty());
    EXPECT_EQ(actual_flushes, 1U);
    auto* plan = huge::detail::PreparationAccess::plan_storage(cleanup[0]);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->descriptor_count, 1U);
    ASSERT_EQ(split(cleanup[0]), huge::Error::None);
    mapped[0] = false;
    EXPECT_EQ(tracked_pages, 512U);
    EXPECT_EQ(model.live_refs(), 512U);
    huge::Snapshot s{};
    huge_core.snapshot(s);
    EXPECT_EQ(s.charged_pages, 0U);
    EXPECT_EQ(s.plans, 0U);
}
TEST_F(ManagedResolver, RegisteredFirstLastAndAllAncestorPermissionRestrictions) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    virt::UserLeafResolution out{};
    ASSERT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, true, out));
    auto physical = out.physical_4k;
    EXPECT_EQ(out.kind, virt::UserLeafKind::ManagedPml2);
    ASSERT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA + LARGE - 1, true, out));
    EXPECT_EQ(out.physical_4k, physical + LARGE - PAGE);
    for (int level : {4, 3}) {
        auto& w = slot_at(runtime_root, TEST_VA, level);
        auto old = w;
        for (uint64_t bad : {old & ~4ULL, old & ~2ULL, old | 512ULL}) {
            w = bad;
            EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, true, out));
        }
        w = old;
    }
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, uint64_t{1} << 47, false, out));
}
TEST_F(ManagedResolver, UnknownTwoMegabyteOneGigabyteAndAbaNeverGrantProvenance) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    auto& leaf = slot_at(runtime_root, TEST_VA, 2);
    auto old = leaf;
    auto refs = model.live_refs();
    virt::UserLeafResolution out{};
    leaf = old + LARGE;
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
    leaf = old;
    ++runtime_serial;
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
    --runtime_serial;
    auto& upper = slot_at(runtime_root, TEST_VA, 3);
    auto old_upper = upper;
    upper = 0x40000087;
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
    upper = old_upper;
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA + LARGE, false, out));
    EXPECT_EQ(model.live_refs(), refs);
    leaf = old | 0x60;
    EXPECT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
}
TEST_F(ManagedResolver, ExactCurrentLeafDirtyCommitSurvivesPureSplitWithNoRefChange) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0), huge::Error::None);
    virt::UserLeafResolution out{};
    ASSERT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, true, out));
    virt::UserPagePin pin{};
    pin.pagemap = runtime_root;
    pin.user_page = TEST_VA;
    pin.physical_page = out.physical_4k;
    pin.hhdm_page = bytes[0];
    pin.access_gate_index = 0;
    pin.require_writable = true;
    pin.root_incarnation = runtime_serial;
    ASSERT_TRUE(virt::user_page_pin_commit_write(pin));
    EXPECT_NE(slot_at(runtime_root, TEST_VA, 2) & 64, 0U);
    ASSERT_EQ(split(cleanup[0]), huge::Error::None);
    mapped[0] = false;
    auto& pte = slot_at(runtime_root, TEST_VA, 1);
    pte &= ~64ULL;
    auto& pde = slot_at(runtime_root, TEST_VA, 2);
    pde &= ~64ULL;
    ASSERT_TRUE(virt::user_page_pin_commit_write(pin));
    EXPECT_NE(pte & 64, 0U);
    EXPECT_EQ(pde & 64, 0U);
    auto old = pte;
    binding_matches = false;
    EXPECT_FALSE(virt::user_page_pin_commit_write(pin));
    EXPECT_EQ(pte, old);
    binding_matches = true;
    ++pin.root_incarnation;
    EXPECT_FALSE(virt::user_page_pin_commit_write(pin));
    --pin.root_incarnation;
    pin.physical_page += PAGE;
    EXPECT_FALSE(virt::user_page_pin_commit_write(pin));
    EXPECT_EQ(model.live_refs(), 512U);
}
TEST_F(ManagedResolver, WholeTwoLeafPreflightRejectsLateConflictBeforeAnyStore) {
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(prepare(i), huge::Error::None);
        ASSERT_EQ(publish(i), huge::Error::None);
    }
    huge::PreparedTables plan;
    ASSERT_EQ(virt::prepare_managed_split_plan(runtime_root, TEST_VA, TEST_VA + 2 * LARGE, plan), huge::Error::None);
    auto* storage = huge::detail::PreparationAccess::plan_storage(plan);
    ASSERT_NE(storage, nullptr);
    storage->descriptors[1].semantic_word ^= 2;
    EXPECT_EQ(split(plan), huge::Error::Again);
    storage->descriptors[1].semantic_word ^= 2;
    auto saved_end = storage->end;
    storage->end = TEST_VA + 65 * LARGE;
    EXPECT_EQ(split(plan), huge::Error::Invalid);
    storage->end = saved_end;
    auto& second = slot_at(runtime_root, TEST_VA + LARGE, 2);
    auto old = second;
    second ^= 2;
    auto first = slot_at(runtime_root, TEST_VA, 2);
    EXPECT_EQ(split(plan), huge::Error::Again);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2), first);
    second = old;
    ASSERT_EQ(split(plan), huge::Error::None);
    mapped[0] = mapped[1] = false;
    EXPECT_EQ(model.live_refs(), 1024U);
    EXPECT_EQ(tracked_pages, 1024U);
}
TEST_F(ManagedResolver, OrdinaryFourKRetainsCowAndSharedWriteCeilingAndNx) {
    ASSERT_EQ(prepare(0), huge::Error::None);
    ASSERT_EQ(publish(0, 7 | mm::paging::PAGE_NX), huge::Error::None);
    ASSERT_EQ(split(cleanup[0]), huge::Error::None);
    mapped[0] = false;
    auto& pte = slot_at(runtime_root, TEST_VA, 1);
    auto old = pte;
    virt::UserLeafResolution out{};
    ASSERT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, true, out));
    EXPECT_EQ(out.kind, virt::UserLeafKind::Pml1);
    EXPECT_NE(out.raw & mm::paging::PAGE_NX, 0U);
    for (uint64_t bad : std::array<uint64_t, 3>{old & ~uint64_t{2}, old | mm::paging::PAGE_COW,
                                                old | mm::paging::PAGE_SHARED | mm::paging::PAGE_WRITE_RESTRICTED}) {
        pte = bad;
        EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, true, out));
        EXPECT_TRUE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
    }
    pte = old & ~4ULL;
    EXPECT_FALSE(virt::resolve_user_leaf_locked(runtime_root, TEST_VA, false, out));
    pte = old;
}
TEST_F(ManagedResolver, ActualAlignedSlotCasPreservesAdAndRejectsNonTableGeometry) {
    virt::MutableUserEntrySlot slot, bad;
    EXPECT_FALSE(
        virt::MutableUserEntrySlot::from(reinterpret_cast<mm::paging::PageTable*>(reinterpret_cast<uintptr_t>(runtime_root) + 1), 0, bad));
    EXPECT_FALSE(virt::MutableUserEntrySlot::from(runtime_root, 512, bad));
    ASSERT_TRUE(virt::MutableUserEntrySlot::from(runtime_root, 0, slot));
    EXPECT_FALSE(virt::MutableUserEntrySlot::from(runtime_root, 0, slot));
    word(runtime_root, 0) = 0x2000 | 0x67;
    ASSERT_TRUE(virt::managed_replace_ad_locked(slot, 0x2007, 0x4007));
    EXPECT_EQ(slot.load(), 0x4067U);
    EXPECT_FALSE(virt::managed_replace_ad_locked(slot, 0x2007, 0x6007));
    EXPECT_EQ(slot.load(), 0x4067U);
    slot.mark_dirty();
    word(runtime_root, 0) = 0;
}
}
