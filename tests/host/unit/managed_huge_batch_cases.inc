// Finite Host endpoint witnesses, not WOS scheduling/PMM/TLB qualification.
namespace host_batch {
ker::mod::mm::huge::detail::HugeMapBatchStorage* observed{};
ker::mod::mm::huge::SplitPlanStorage* observed_parents{};
unsigned acknowledgments{}, detached_returns{};
std::array<void*, huge::MAX_ROOT_LEAVES> returning{};
uint16_t returning_count{};
bool returning_after_ack{};
void release(void* pointer) {
    if (!enabled || pointer == nullptr) return;
    EXPECT_EQ(publication_depth + cow_depth + pin_depth + root_readers, 0U);
    EXPECT_FALSE(host_range::exclusive);
    if (observed)
        for (uint16_t i = 0; i < observed->detached_count; ++i)
            if (observed->detached[i] == pointer) {
                EXPECT_TRUE(observed->acknowledged);
                ++detached_returns;
            }
    // During actual reset the heap dies before unused-plan table returns.
    // Compare copied historical identities only; never dereference that heap.
    for (uint16_t i = 0; i < returning_count; ++i)
        if (returning[i] == pointer) {
            EXPECT_TRUE(returning_after_ack);
            ++detached_returns;
        }
}
void ack() {
    if (!enabled || observed == nullptr) return;
    EXPECT_TRUE(host_range::exclusive);
    EXPECT_EQ(root_readers + cow_depth + pin_depth, 0U);
    EXPECT_TRUE(observed->published);
    EXPECT_FALSE(observed->acknowledged);
    for (uint16_t i = 0; i < observed->count; ++i) {
        EXPECT_TRUE(observed->leaves[i].span.empty());
        uint32_t page{};
        auto* block = backend().containing(reinterpret_cast<uint64_t>(mm::addr::get_virt_pointer(observed->leaves[i].key.pa_2m)), page);
        ASSERT_NE(block, nullptr);
        EXPECT_EQ(block->refs.size(), huge::LEAF_PAGES);
        for (auto ref : block->refs) EXPECT_EQ(ref, 1U);
    }
    for (uint16_t i = 0; i < observed_parents->table_count; ++i)
        if (observed->adopted_mask & (uint64_t{1} << i)) EXPECT_EQ(observed_parents->tables[i], nullptr);
    for (uint16_t i = 0; i < observed->detached_count; ++i)
        EXPECT_NE(std::find(table_live.begin(), table_live.end(), observed->detached[i]), table_live.end());
    ++acknowledgments;
}
}  // namespace host_batch
namespace {
class ManagedBatch : public ManagedResolver {
   protected:
    huge::PreparedHugeMapBatch batch;
    void SetUp() override {
        ManagedResolver::SetUp();
        huge_core = {};
        ASSERT_EQ(huge_core.initialize(caps()), huge::Error::None);
        host_batch::enabled = true;
        host_batch::fail_heap = host_batch::heap_without_plans = false;
        host_batch::allocation_index = 0;
        host_batch::fail_data_before = SIZE_MAX;
        host_batch::observed = nullptr;
        host_batch::observed_parents = nullptr;
        host_batch::acknowledgments = host_batch::detached_returns = 0;
        host_batch::returning_count = 0;
        host_batch::returning_after_ack = false;
    }
    auto storage() -> huge::detail::HugeMapBatchStorage* { return huge::detail::HugeMapBatchAccess::storage(batch); }
    auto prepare_batch(uint16_t count, uint64_t first = TEST_VA, uint64_t flags = 0x8000000000000007ULL) -> huge::Error {
        return batch.prepare({reinterpret_cast<uint64_t>(runtime_root), runtime_serial}, first, uint64_t{count} * LARGE, flags,
                             context(numa::Mode::BIND, 1));
    }
    auto publish_batch() -> huge::Error {
        host_batch::observed = storage();
        host_batch::observed_parents = huge::detail::HugeMapBatchAccess::parents(batch);
        huge::Error error;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            error = huge::detail::publish_prepared_huge_batch(runtime_root, publication, batch);
        }
        return error;
    }
    auto table() -> mm::paging::PageTable* {
        auto* value = static_cast<mm::paging::PageTable*>(std::aligned_alloc(PAGE, PAGE));
        if (!value) std::abort();
        std::memset(value, 0, PAGE);
        table_live.push_back(value);
        return value;
    }
    auto path(uint64_t va) -> uint64_t& {
        auto* current = runtime_root;
        for (int level = 4; level > 2; --level) {
            auto& entry = word(current, virt::index_of(va, level));
            if (entry == 0) entry = reinterpret_cast<uint64_t>(table()) | 7;
            current = reinterpret_cast<mm::paging::PageTable*>(entry & virt::PTE_FRAME_MASK);
        }
        return word(current, virt::index_of(va, 2));
    }
    void expect_unpublished() {
        ASSERT_NE(storage(), nullptr);
        EXPECT_FALSE(storage()->published);
        EXPECT_EQ(storage()->detached_count, 0U);
        EXPECT_EQ(storage()->edit_count, 0U);
        EXPECT_EQ(storage()->role_count, 0U);
        huge::Snapshot snapshot{};
        huge_core.snapshot(snapshot);
        EXPECT_EQ(snapshot.mapped_pages, 0U);
        EXPECT_EQ(actual_flushes, 0U);
    }
    void discard() {
        auto* saved = storage();
        if (saved && saved->published) {
            // Explicit Host mapping teardown. Genuine mutants can omit a
            // transfer; repair only AFTER the ACK assertions rejected it so
            // cleanup remains safe and cannot manufacture a passing witness.
            for (uint16_t i = 0; i < saved->count; ++i) {
                auto& row = saved->leaves[i];
                if (!row.span.empty()) huge::detail::PreparationAccess::span_transferred_after_publish(row.span);
                EXPECT_EQ(huge_core.begin_split(row.view.handle, row.key), huge::Error::None);
                EXPECT_EQ(huge_core.complete_split_after_ack(row.view.handle), huge::Error::None);
            }
        }
        host_batch::returning_count = saved ? saved->detached_count : 0;
        host_batch::returning_after_ack = saved && saved->acknowledged;
        for (uint16_t i = 0; i < host_batch::returning_count; ++i) host_batch::returning[i] = saved->detached[i];
        host_batch::observed = nullptr;
        host_batch::observed_parents = nullptr;
        batch.reset();
        host_batch::returning_count = 0;
    }
    void TearDown() override {
        discard();
        EXPECT_FALSE(host_batch::heap_without_plans);
        host_batch::enabled = false;
        ManagedResolver::TearDown();
    }
};
TEST_F(ManagedBatch, OneTwoAnd64LeavesTransferEveryIndependentReference) {
    for (uint16_t count : {uint16_t{1}, uint16_t{2}, uint16_t{64}}) {
        ASSERT_EQ(prepare_batch(count), huge::Error::None);
        auto* saved = storage();
        ASSERT_EQ(publish_batch(), huge::Error::None);
        EXPECT_TRUE(saved->published);
        EXPECT_TRUE(saved->acknowledged);
        EXPECT_EQ(model.live_refs(), uint64_t{count} * 512);
        for (uint16_t i = 0; i < count; ++i) {
            EXPECT_TRUE(saved->leaves[i].span.empty());
            EXPECT_EQ(slot_at(runtime_root, TEST_VA + uint64_t{i} * LARGE, 2) & 128, 128U);
        }
        discard();
        // Exact endpoint teardown between independent real-owner repetitions.
        huge_check_free = false;
        for (auto& block : model.blocks)
            if (block.storage)
                for (uint32_t i = 0; i < block.pages; ++i)
                    if (block.refs[i]) (void)mm::phys::page_ref_dec(static_cast<uint8_t*>(block.storage.get()) + uint64_t{i} * PAGE);
        huge_check_free = true;
        for (auto* value : table_live) std::free(value);
        table_live.clear();
        std::memset(runtime_root, 0, PAGE);
        model.blocks.clear();
    }
    EXPECT_EQ(host_batch::acknowledgments, 3U);
}
TEST_F(ManagedBatch, EverySpanAllocationPrefixReturnsAllFramesBeforeCredits) {
    for (size_t position = 0; position < 64; ++position) {
        host_batch::allocation_index = 0;
        host_batch::fail_data_before = position;
        EXPECT_EQ(prepare_batch(64), huge::Error::Exhausted);
        EXPECT_TRUE(batch.empty());
        EXPECT_EQ(model.live_refs(), 0U);
        EXPECT_TRUE(table_live.empty());
        huge::Snapshot snapshot{};
        huge_core.snapshot(snapshot);
        EXPECT_EQ(snapshot.charged_pages + snapshot.table_pages + snapshot.plans, 0U);
        model.blocks.clear();
    }
    host_batch::fail_data_before = SIZE_MAX;
}
TEST_F(ManagedBatch, EveryCleanupAndParentTablePrefixReturnsActualOwners) {
    for (size_t position = 0; position < 68; ++position) {
        table_attempt = 0;
        fail_table_at = position;
        EXPECT_EQ(prepare_batch(64), huge::Error::Exhausted);
        EXPECT_TRUE(batch.empty());
        EXPECT_TRUE(table_live.empty());
        EXPECT_EQ(model.live_refs(), 0U);
    }
    fail_table_at = SIZE_MAX;
}
TEST_F(ManagedBatch, Invalid65AndOutstandingCapacityRefuseWithoutAllocating) {
    EXPECT_EQ(prepare_batch(65), huge::Error::Capacity);
    EXPECT_EQ(table_attempt, 0U);
    huge::PreparedTables reserved;
    ASSERT_EQ(reserved.prepare(64), huge::Error::None);
    EXPECT_EQ(prepare_batch(64), huge::Error::Capacity);
    EXPECT_EQ(table_live.size(), 64U);
    reserved.reset();
    EXPECT_EQ(batch.prepare({reinterpret_cast<uint64_t>(runtime_root), runtime_serial}, TEST_VA + PAGE, LARGE, 7, context()),
              huge::Error::Invalid);
}
TEST_F(ManagedBatch, BatchHeapFailureIsReservedAndBothPlansReturn) {
    host_batch::fail_heap = true;
    EXPECT_EQ(prepare_batch(2), huge::Error::Exhausted);
    EXPECT_FALSE(host_batch::fail_heap);
    EXPECT_TRUE(batch.empty());
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(table_live.empty());
    huge::Snapshot snapshot{};
    huge_core.snapshot(snapshot);
    EXPECT_EQ(snapshot.plans + snapshot.table_pages + snapshot.charged_pages, 0U);
}
TEST_F(ManagedBatch, LastDestinationConflictLeavesTheWholeBatchUnpublished) {
    auto& last = path(TEST_VA + LARGE);
    auto* occupied = table();
    word(occupied, 0) = 0x4007;
    last = reinterpret_cast<uint64_t>(occupied) | 7;
    auto const last_before = last;
    auto* upper = reinterpret_cast<mm::paging::PageTable*>(word(runtime_root, virt::index_of(TEST_VA, 4)) & virt::PTE_FRAME_MASK);
    auto const root_before = word(runtime_root, virt::index_of(TEST_VA, 4));
    auto const upper_before = word(upper, virt::index_of(TEST_VA, 3));
    ASSERT_EQ(prepare_batch(2), huge::Error::None);
    EXPECT_NE(publish_batch(), huge::Error::None);
    expect_unpublished();
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2), 0U);
    EXPECT_EQ(last, last_before);
    EXPECT_EQ(word(runtime_root, virt::index_of(TEST_VA, 4)), root_before);
    EXPECT_EQ(word(upper, virt::index_of(TEST_VA, 3)), upper_before);
}
TEST_F(ManagedBatch, FourParentsAcrossThe512GigabyteBoundary) {
    uint64_t const first = (uint64_t{1} << 39) - LARGE;
    ASSERT_EQ(prepare_batch(2, first), huge::Error::None);
    ASSERT_EQ(publish_batch(), huge::Error::None);
    EXPECT_EQ(storage()->adopted_mask, 15U);
    EXPECT_NE(slot_at(runtime_root, first, 2) & 128, 0U);
    EXPECT_NE(slot_at(runtime_root, first + LARGE, 2) & 128, 0U);
}
TEST_F(ManagedBatch, DetachedOldTablesSurviveAckAndReturnOnlyAfterAllOwners) {
    auto* old = table();
    path(TEST_VA) = reinterpret_cast<uint64_t>(old) | 0x67;
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    ASSERT_EQ(publish_batch(), huge::Error::None);
    EXPECT_EQ(storage()->detached_count, 1U);
    EXPECT_EQ(host_batch::detached_returns, 0U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 0x60, 0x60U);
    discard();
    EXPECT_EQ(host_batch::detached_returns, 1U);
    EXPECT_EQ(std::find(table_live.begin(), table_live.end(), old), table_live.end());
}
TEST_F(ManagedBatch, EveryNonzeroOldEmptyPositionRefusesBeforeTheFirstWrite) {
    auto* old = table();
    path(TEST_VA) = reinterpret_cast<uint64_t>(old) | 7;
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    for (size_t position = 0; position < 512; ++position) {
        word(old, position) = 0x200;
        EXPECT_NE(publish_batch(), huge::Error::None) << position;
        expect_unpublished();
        word(old, position) = 0;
    }
}
TEST_F(ManagedBatch, UnknownPsOneGigabyteAndSoftwareParentNeverDescend) {
    path(TEST_VA) = 0;
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    for (int level : {4, 3, 2}) {
        auto& entry = slot_at(runtime_root, TEST_VA, level);
        auto const saved = entry;
        entry = level == 2 ? 0x200087 : saved | 128;
        EXPECT_NE(publish_batch(), huge::Error::None);
        expect_unpublished();
        entry = saved | 512;
        EXPECT_NE(publish_batch(), huge::Error::None);
        expect_unpublished();
        entry = saved;
    }
}
TEST_F(ManagedBatch, RootAbaAndSaturatedEpochCannotPublishOrDetach) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    ++runtime_serial;
    EXPECT_EQ(publish_batch(), huge::Error::Stale);
    expect_unpublished();
    --runtime_serial;
    virt::host_root_control.ps_geometry_epoch = UINT64_MAX;
    EXPECT_EQ(publish_batch(), huge::Error::GenerationExhausted);
    expect_unpublished();
}
TEST_F(ManagedBatch, DuplicateOldEmptyOwnerAndPreparedAliasRefuse) {
    auto* old = table();
    path(TEST_VA) = path(TEST_VA + LARGE) = reinterpret_cast<uint64_t>(old) | 7;
    ASSERT_EQ(prepare_batch(2), huge::Error::None);
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    auto* prepared = huge::detail::HugeMapBatchAccess::parents(batch)->tables[0];
    path(TEST_VA) = reinterpret_cast<uint64_t>(prepared) | 7;
    path(TEST_VA + LARGE) = 0;
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    path(TEST_VA) = 0;
}
TEST_F(ManagedBatch, DistinctPathsCannotAliasTheSameAncestorOrPhysicalSlot) {
    uint64_t const first = (uint64_t{1} << 30) - LARGE;
    path(first) = 0;
    auto* upper = reinterpret_cast<mm::paging::PageTable*>(word(runtime_root, 0) & virt::PTE_FRAME_MASK);
    word(upper, 1) = word(upper, 0);
    ASSERT_EQ(prepare_batch(2, first), huge::Error::None);
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    word(upper, 1) = 0;
}
TEST_F(ManagedBatch, DuplicatePreparedRolesAndOldAncestorReuseRefuse) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    auto* parents = huge::detail::HugeMapBatchAccess::parents(batch);
    auto* cleanup = huge::detail::HugeMapBatchAccess::cleanup(batch);
    auto* saved = cleanup->tables[0];
    auto saved_pa = cleanup->table_physical[0];
    cleanup->tables[0] = parents->tables[0];
    cleanup->table_physical[0] = parents->table_physical[0];
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    cleanup->tables[0] = saved;
    cleanup->table_physical[0] = saved_pa;
    auto& destination = path(TEST_VA);
    uint64_t const ACTUAL_ANCESTOR = word(runtime_root, virt::index_of(TEST_VA, 4));
    ASSERT_NE(ACTUAL_ANCESTOR & virt::PTE_FRAME_MASK, 0U);
    destination = ACTUAL_ANCESTOR;
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    path(TEST_VA) = 0;
}
TEST_F(ManagedBatch, SharedAncestorsAndEffectivePermissionsPreserveAdAndProtNone) {
    path(TEST_VA) = path(TEST_VA + LARGE) = 0;
    for (int level : {4, 3}) slot_at(runtime_root, TEST_VA, level) |= 0x60 | mm::paging::PAGE_NX;
    ASSERT_EQ(prepare_batch(2, TEST_VA, 1 | mm::paging::PAGE_NX), huge::Error::None);
    ASSERT_EQ(publish_batch(), huge::Error::None);
    for (int level : {4, 3}) EXPECT_EQ(slot_at(runtime_root, TEST_VA, level) & 0x67, 0x67U);
    EXPECT_EQ(slot_at(runtime_root, TEST_VA, 2) & 7, 1U);
}
TEST_F(ManagedBatch, MoveAndRepeatedResetKeepOnlyOneRealOwner) {
    ASSERT_EQ(prepare_batch(2), huge::Error::None);
    huge::PreparedHugeMapBatch moved(std::move(batch));
    EXPECT_TRUE(batch.empty());
    batch = std::move(moved);
    EXPECT_TRUE(moved.empty());
    batch = std::move(batch);
    batch.reset();
    batch.reset();
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(table_live.empty());
}
TEST_F(ManagedBatch, DirtyPrivateParentRefusesBeforeAnyVisibleStore) {
    ASSERT_EQ(prepare_batch(1), huge::Error::None);
    auto* parents = huge::detail::HugeMapBatchAccess::parents(batch);
    word(parents->tables[3], 511) = 0x200;
    EXPECT_EQ(publish_batch(), huge::Error::Invalid);
    expect_unpublished();
    EXPECT_EQ(word(runtime_root, virt::index_of(TEST_VA, 4)), 0U);
    word(parents->tables[3], 511) = 0;
}
}  // namespace
