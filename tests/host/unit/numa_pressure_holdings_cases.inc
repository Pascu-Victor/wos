// Actual extracted code; only immutable canonical-view, physical/ref/address
// endpoints are Host seams. Window eligibility is not simulated free RAM.
class PressureBitmap : public NumaAllocation {
   protected:
    mm::PageAllocator allocators[3]{};
    void SetUp() override {
        NumaAllocation::SetUp();
        state = {};
        state.cleanup_valid = true;
        state.baseline.complete = true;
        state.baseline.error = mm::NodeCounterError::NONE;
        state.baseline.node_count = 1;
        state.baseline.allocator_count = 1;
        state.baseline.nodes[0].usable = MAX_HELD;
        allocators[0].base = model.topology.memory[0].base;
        allocators[0].total_pages = allocators[0].usable_pages = MAX_HELD;
        state.work.views[0] = {&allocators[0], allocators[0].base};
        ASSERT_TRUE(prepare_frame_geometry(model.topology));
    }
    void again() {
        state.frames = {};
        state.failure = nullptr;
    }
    static auto page() -> numa::AllocationResult { return numa::try_allocate_pages({{0, numa::Mode::LOCAL}, 0, 0}, 1, PAGE, OWNER); }
    static auto bulk() -> numa::AllocationResult { return numa::try_allocate_pages({{0, numa::Mode::LOCAL}, 0, 0}, 512, LARGE, OWNER); }
    static void sequence(Backend& m, std::initializer_list<unsigned> indices) { m.script.assign(indices); }
    static auto owned() -> uint64_t {
        uint64_t n = 0;
        for (auto w : state.frames.owned) n += __builtin_popcountll(w);
        return n;
    }
};
TEST_F(PressureBitmap, RealOwnedSpanBulkAndTailPrefixFullyReturns) {
    {
        Holdings h;
        for (unsigned i = 0; i < 2; ++i) {
            auto r = bulk();
            ASSERT_TRUE(h.take(r, 512));
            EXPECT_TRUE(r.span.empty());
        }
        for (unsigned i = 0; i < 7; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.held, 1031U);
        EXPECT_EQ(owned(), 1031U);
    }
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(state.released, 1031U);
    EXPECT_EQ(owned(), 0U);
    EXPECT_EQ(state.bulk_allocations, 0U);
    EXPECT_EQ(state.order0_allocations, 0U);
    EXPECT_TRUE(state.cleanup_valid);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(PressureBitmap, EarlyReturnFreesExactSuccessfulPrefix) {
    auto path = []() {
        Holdings h;
        for (unsigned i = 0; i < 3; ++i) {
            auto r = page();
            if (!h.take(r, 1)) return false;
        }
        return false;
    };
    EXPECT_FALSE(path());
    EXPECT_EQ(state.released, 3U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(owned(), 0U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, InvalidSpanShapeRetainsActualResultUntilItsDestructor) {
    {
        Holdings h;
        auto r = bulk();
        EXPECT_FALSE(h.take(r, 1));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(state.held, 0U);
        EXPECT_EQ(owned(), 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
    EXPECT_NE(state.failure, nullptr);
}
TEST_F(PressureBitmap, ForeignNodeRefusalRetainsWholeActualCandidate) {
    model.domains(2);
    {
        Holdings h;
        auto r = numa::try_allocate_pages(context(numa::Mode::BIND, 2, 0), 1, PAGE, OWNER);
        ASSERT_EQ(r.actual, 1);
        EXPECT_FALSE(h.take(r, 1));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(state.held, 0U);
        EXPECT_EQ(owned(), 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, ExplicitClearThenDestructorDoesNotDoubleRelease) {
    {
        Holdings h;
        auto r = bulk();
        ASSERT_TRUE(h.take(r, 512));
        h.clear();
        EXPECT_EQ(state.released, 512U);
        h.clear();
    }
    EXPECT_EQ(state.released, 512U);
    EXPECT_EQ(model.decremented.size(), 512U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(owned(), 0U);
}
TEST_F(PressureBitmap, ForwardAdjacentIndependentPagesKeepExactMembership) {
    sequence(model, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    {
        Holdings h;
        for (unsigned i = 0; i < 20; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.order0_allocations, 20U);
        EXPECT_EQ(owned(), 20U);
        EXPECT_EQ(state.tail_pages, 20U);
        EXPECT_EQ(state.held_highwater, 20U);
    }
    EXPECT_EQ(state.released, 20U);
    EXPECT_EQ(model.decremented.size(), 20U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, ReverseAdjacentIndependentPagesKeepExactMembership) {
    sequence(model, {5, 4, 3, 2, 1, 0});
    {
        Holdings h;
        for (unsigned i = 0; i < 6; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(owned(), 6U);
    }
    EXPECT_EQ(state.released, 6U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, DisjointAlternatingDirectionNeverClaimsHoles) {
    sequence(model, {0, 1, 10, 9, 20, 21, 30, 29});
    {
        Holdings h;
        for (unsigned i = 0; i < 8; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(owned(), 8U);
        EXPECT_EQ(state.tail_pages, 8U);
    }
    EXPECT_EQ(state.released, 8U);
    EXPECT_EQ(model.decremented.size(), 8U);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(PressureBitmap, MoreThanLegacyRecordsAcceptsAdjacentCandidate) {
    for (unsigned i = 0; i < 16; ++i) model.script.push_back(i * 3);
    model.script.push_back(46);
    {
        Holdings h;
        for (unsigned i = 0; i < 17; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.order0_allocations, 17U);
        EXPECT_EQ(owned(), 17U);
        EXPECT_FALSE(state.limit_captured);
    }
    EXPECT_EQ(state.released, 17U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, FormerNonadjacentRecordCapacityNowAcceptsWholePrefix) {
    for (unsigned i = 0; i < 300; ++i) model.script.push_back(i * 3);
    {
        Holdings h;
        for (unsigned i = 0; i < 300; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.held, 300U);
        EXPECT_EQ(owned(), 300U);
        EXPECT_FALSE(state.limit_captured);
    }
    EXPECT_EQ(state.released, 300U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, AddressArithmeticAndSupportedShapeRejectBeforeAnyTransfer) {
    uint64_t out = 123;
    for (auto physical : {state.frames.first - 1, UINT64_MAX - PAGE + 1, state.frames.end, state.frames.first + 1}) {
        EXPECT_FALSE(frame_span_index(physical, 1, out));
        EXPECT_EQ(out, 123U);
    }
    EXPECT_FALSE(frame_span_index(state.frames.first, 0, out));
    EXPECT_FALSE(frame_span_index(state.frames.first, 2, out));
    EXPECT_FALSE(frame_span_index(state.frames.end - PAGE, 512, out));
    {
        Holdings h;
        auto r = page();
        EXPECT_FALSE(h.take(r, 2));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(state.limit_reasons, 8U);
        EXPECT_EQ(state.held, 0U);
        EXPECT_EQ(owned(), 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
}
TEST_F(PressureBitmap, UsableBoundRefusalCapturesBeforeDestructorAndDoesNotAdopt) {
    sequence(model, {0, 1});
    state.baseline.nodes[0].usable = 1;
    {
        Holdings h;
        {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        {
            auto r = page();
            EXPECT_FALSE(h.take(r, 1));
            EXPECT_FALSE(r.span.empty());
            EXPECT_EQ(state.limit_reasons, 4U);
            EXPECT_EQ(state.limit_held, 1U);
            EXPECT_EQ(state.held, 1U);
            EXPECT_EQ(owned(), 1U);
        }
    }
    EXPECT_EQ(state.released, 1U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.limit_held, 1U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, MixedBulkAndLongTailReturnsEachIndependentReferenceOnce) {
    {
        Holdings h;
        auto r = bulk();
        ASSERT_TRUE(h.take(r, 512));
        for (unsigned i = 0; i < 300; ++i) model.script.push_back(i);
        for (unsigned i = 0; i < 300; ++i) {
            auto p = page();
            ASSERT_TRUE(h.take(p, 1));
        }
        EXPECT_EQ(owned(), 812U);
        h.clear();
        h.clear();
    }
    EXPECT_EQ(state.released, 812U);
    EXPECT_EQ(model.decremented.size(), 812U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(owned(), 0U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, GenuineExtraRefDuplicateRefusesBeforeTransfer) {
    {
        Holdings h;
        void* saved = nullptr;
        {
            auto r = page();
            saved = r.span.data();
            ASSERT_TRUE(h.take(r, 1));
        }
        model.alias = saved;
        {
            auto duplicate = page();
            ASSERT_EQ(model.live_refs(), 2U);
            EXPECT_FALSE(h.take(duplicate, 1));
            EXPECT_FALSE(duplicate.span.empty());
            EXPECT_EQ(state.held, 1U);
            EXPECT_EQ(owned(), 1U);
        }
        model.alias = nullptr;
        EXPECT_EQ(model.live_refs(), 1U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 1U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, Whole512RefusalDoesNotAdoptEligiblePrefix) {
    {
        Holdings h;
        auto r = bulk();
        state.frames.eligible[511 / 64] &= ~(uint64_t{1} << (511 % 64));
        EXPECT_FALSE(h.take(r, 512));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(owned(), 0U);
        EXPECT_EQ(state.held, 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, GenuinePartialOverlapLeavesAll512RefsWithCandidate) {
    {
        Holdings h;
        {
            auto r = bulk();
            model.alias = static_cast<uint8_t*>(r.span.data()) + 257 * PAGE;
            {
                auto alias = page();
                ASSERT_TRUE(h.take(alias, 1));
            }
            model.alias = nullptr;
            EXPECT_FALSE(h.take(r, 512));
            EXPECT_FALSE(r.span.empty());
            EXPECT_EQ(owned(), 1U);
            EXPECT_EQ(state.held, 1U);
        }
        EXPECT_EQ(model.live_refs(), 1U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 1U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, Batch127128129AndRemainderAreOnceOnly) {
    for (unsigned count : {127U, 128U, 129U, 257U}) {
        model.batches.clear();
        uint64_t before = state.released;
        {
            Holdings h;
            for (unsigned i = 0; i < count; ++i) {
                auto r = page();
                ASSERT_TRUE(h.take(r, 1));
            }
        }
        EXPECT_EQ(state.released - before, count);
        EXPECT_EQ(model.batches.size(), (count + 127) / 128);
        for (size_t i = 0; i < model.batches.size(); ++i)
            EXPECT_EQ(model.batches[i], i + 1 < model.batches.size() ? 128U : (count % 128 ? count % 128 : 128U));
        EXPECT_EQ(owned(), 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureBitmap, BadBatchStatsMarksFailureButDrainsAllHealthyRefsWithoutRetry) {
    model.bad_batch_stats = true;
    {
        Holdings h;
        for (unsigned i = 0; i < 300; ++i) {
            auto r = page();
            ASSERT_TRUE(h.take(r, 1));
        }
        h.clear();
        h.clear();
    }
    EXPECT_FALSE(state.cleanup_valid);
    EXPECT_EQ(owned(), 0U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(state.released, 300U);
    EXPECT_EQ(model.decremented.size(), 300U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(model.batches.size(), 3U);
}
TEST_F(PressureBitmap, MetadataHolesAndForeignNodesAreNeverEligible) {
    again();
    allocators[0].metadata_pages = 2;
    allocators[0].usable_pages = allocators[0].total_pages - 2;
    state.baseline.nodes[0].usable = allocators[0].usable_pages - 10;
    auto const BASE = allocators[0].base;
    model.topology.node_count = state.baseline.node_count = 2;
    model.topology.memory_range_count = 3;
    model.topology.memory[0] = {BASE, 12 * PAGE, 0, 0};
    model.topology.memory[1] = {BASE + 12 * PAGE, 10 * PAGE, 1, 0};
    model.topology.memory[2] = {BASE + 22 * PAGE, (MAX_HELD - 22) * PAGE, 0, 0};
    ASSERT_TRUE(prepare_frame_geometry(model.topology));
    EXPECT_EQ(state.frames.first, BASE + 2 * PAGE);
    EXPECT_EQ(state.frames.eligible_pages, MAX_HELD - 12);
    EXPECT_EQ(state.frames.eligible[10 / 64] & (uint64_t{1} << (10 % 64)), 0U);
    EXPECT_EQ(state.frames.eligible[20 / 64] & (uint64_t{1} << (20 % 64)), uint64_t{1} << 20);
}
TEST_F(PressureBitmap, HighAbsolutePaAndLastRelativeBitRemainExact) {
    again();
    auto const BASE = allocators[0].base;
    state.baseline.nodes[0].usable = 2;
    state.baseline.allocator_count = 2;
    allocators[0].total_pages = allocators[0].usable_pages = 1;
    allocators[1].base = BASE + (FRAME_BITS - 1) * PAGE;
    allocators[1].total_pages = allocators[1].usable_pages = 1;
    state.work.views[1] = {&allocators[1], allocators[1].base};
    ASSERT_TRUE(prepare_frame_geometry(model.topology));
    EXPECT_GT(state.frames.first, 8ULL * 1024 * 1024 * 1024);
    EXPECT_EQ(state.frames.end - state.frames.first, FRAME_BITS * PAGE);
    EXPECT_EQ(state.frames.eligible[FRAME_WORDS - 1], uint64_t{1} << 63);
    model.cursors[0] = (FRAME_BITS - 1) * PAGE;
    {
        Holdings h;
        auto r = page();
        ASSERT_TRUE(h.take(r, 1));
        EXPECT_EQ(owned(), 1U);
    }
    EXPECT_EQ(state.released, 1U);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(PressureBitmap, TooLargeWindowRefusesEntireGeometryBeforeAnyClaim) {
    again();
    auto const BASE = allocators[0].base;
    state.baseline.nodes[0].usable = 2;
    state.baseline.allocator_count = 2;
    allocators[0].total_pages = allocators[0].usable_pages = 1;
    allocators[1].base = BASE + FRAME_BITS * PAGE;
    allocators[1].total_pages = allocators[1].usable_pages = 1;
    state.work.views[1] = {&allocators[1], allocators[1].base};
    model.topology.memory[0].length = (FRAME_BITS + 1) * PAGE;
    EXPECT_FALSE(prepare_frame_geometry(model.topology));
    EXPECT_FALSE(state.frames.ready);
    for (auto w : state.frames.eligible) EXPECT_EQ(w, 0U);
    EXPECT_TRUE(model.calls.empty());
}
TEST_F(PressureBitmap, GeometryErrorsRefuseWithoutBorrowedPointersOrEligibility) {
    for (unsigned kind = 0; kind < 9; ++kind) {
        again();
        auto saved = state.baseline;
        auto ranges = model.topology;
        auto fields = allocators[0].metadata_pages;
        switch (kind) {
            case 0:
                state.baseline.complete = false;
                break;
            case 1:
                state.baseline.error = mm::NodeCounterError::METADATA;
                break;
            case 2:
                state.baseline.allocator_count = 0;
                break;
            case 3:
                state.baseline.allocator_count = mm::node_counters::MAX_ALLOCATORS + 1;
                break;
            case 4:
                allocators[0].metadata_pages = MAX_HELD + 1;
                break;
            case 5:
                state.baseline.nodes[0].usable--;
                break;
            case 6:
                model.topology.memory[0].length = PAGE;
                break;
            case 7:
                state.baseline.allocator_count = 2;
                state.work.views[1] = state.work.views[0];
                break;
            case 8:
                model.topology.memory[0].base++;
                break;
        }
        EXPECT_FALSE(prepare_frame_geometry(model.topology)) << kind;
        EXPECT_FALSE(state.frames.ready);
        for (auto w : state.frames.eligible) EXPECT_EQ(w, 0U);
        state.baseline = saved;
        model.topology = ranges;
        allocators[0].metadata_pages = fields;
    }
}
TEST_F(PressureBitmap, WordAnd512BoundariesAndFullPhysicalDenominator) {
    uint64_t output = 0;
    for (uint64_t i : std::array<uint64_t, 7>{0, 63, 64, 65, 511, 512, MAX_HELD - 1}) {
        ASSERT_TRUE(frame_span_index(state.frames.first + i * PAGE, 1, output));
        EXPECT_EQ(output, i);
    }
    EXPECT_EQ(state.frames.eligible_pages, MAX_HELD);
    uint64_t n = 0;
    for (auto w : state.frames.eligible) n += __builtin_popcountll(w);
    EXPECT_EQ(n, MAX_HELD);
}
