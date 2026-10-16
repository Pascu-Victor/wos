// Immutable pre37e actual helper and13 old representation vectors.
namespace {

namespace phys = mm::phys;
constexpr uint64_t MAX_HELD = 1024 * 1024;
constexpr size_t SPANS = 2048, TAIL = 16;  // Deliberate scaled Host capacity; kernel32768 proven separately.
struct SpanRecord {
    void* base{};
    uint32_t pages{};
};
struct HostState {
    SpanRecord spans[SPANS]{};
    SpanRecord tail[TAIL]{};
    void* batch[128]{};
    size_t spans_count{}, tail_count{};
    uint64_t held{}, released{}, assertions{}, tail_pages{}, held_highwater{}, bulk_highwater{}, tail_records_highwater{},
        tail_pages_highwater{};
    uint64_t limit_held{}, limit_bulk{}, limit_tail_records{}, limit_tail_pages{}, limit_next_pages{}, limit_buddy{}, limit_cached{},
        limit_usable{};
    uint32_t limit_reasons{};
    bool limit_captured{}, limit_sample_valid{};
    numa::NodeId selected{};
    phys::NodeCounterSnapshot baseline{}, sample{};
    phys::NodeCounterWorkspace work{};
    bool cleanup_valid{true};
    const char* failure{};
};
static HostState state;
auto check(bool condition, const char* text) -> bool {
    ++state.assertions;
    if (!condition && state.failure == nullptr) state.failure = text;
    return condition;
}
void capture_limit(uint32_t reasons, uint32_t next_pages) {
    if (state.limit_captured) return;
    state.limit_captured = true;
    state.limit_reasons = reasons;
    state.limit_next_pages = next_pages;
    state.limit_held = state.held;
    state.limit_bulk = state.spans_count;
    state.limit_tail_records = state.tail_count;
    state.limit_tail_pages = state.tail_pages;
    auto const STATUS = phys::snapshot_node_counters(state.sample, state.work);
    state.limit_sample_valid = STATUS == mm::NodeCounterError::NONE && state.sample.complete && state.selected < state.sample.node_count;
    if (state.limit_sample_valid) {
        auto const& row = state.sample.nodes[state.selected];
        state.limit_buddy = row.buddy_free;
        state.limit_cached = row.cached;
        state.limit_usable = row.usable;
    }
}
enum class TailJoin : uint8_t { NEW_RECORD, FORWARD, BACKWARD, INVALID };
auto tail_join(const SpanRecord& record, uintptr_t page) -> TailJoin {
    auto const BASE = reinterpret_cast<uintptr_t>(record.base);
    if (record.pages == 0 || record.pages >= MAX_HELD || BASE % PAGE != 0 || page % PAGE != 0 || page > UINTPTR_MAX - PAGE)
        return TailJoin::INVALID;
    uint64_t const BYTES = static_cast<uint64_t>(record.pages) * PAGE;
    if (BASE > UINTPTR_MAX - BYTES) return TailJoin::INVALID;
    if (page == BASE + BYTES) return TailJoin::FORWARD;
    if (page + PAGE == BASE) return TailJoin::BACKWARD;
    if (page >= BASE && page < BASE + BYTES) return TailJoin::INVALID;
    return TailJoin::NEW_RECORD;
}
struct Holdings {
    Holdings() = default;
    Holdings(const Holdings&) = delete;
    ~Holdings() { clear(); }
    void clear() {
        while (state.spans_count != 0) {
            auto& entry = state.spans[--state.spans_count];
            for (uint32_t first = 0; first < entry.pages; first += 128) {
                uint32_t const COUNT = entry.pages - first < 128 ? entry.pages - first : 128;
                for (uint32_t i = 0; i < COUNT; ++i)
                    state.batch[i] = static_cast<uint8_t*>(entry.base) + static_cast<uint64_t>(first + i) * PAGE;
                auto const FREED = phys::page_ref_dec_batch({state.batch, COUNT});
                state.cleanup_valid &= FREED.refs_decremented == COUNT && FREED.pages_freed == COUNT;
                state.held -= COUNT;
                state.released += COUNT;
            }
            entry = {};
        }
        while (state.tail_count != 0) {
            auto& entry = state.tail[--state.tail_count];
            for (uint32_t first = 0; first < entry.pages; first += 128) {
                uint32_t const COUNT = entry.pages - first < 128 ? entry.pages - first : 128;
                for (uint32_t i = 0; i < COUNT; ++i)
                    state.batch[i] =
                        reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(entry.base) + static_cast<uint64_t>(first + i) * PAGE);
                auto const FREED = phys::page_ref_dec_batch({state.batch, COUNT});
                state.cleanup_valid &= FREED.refs_decremented == COUNT && FREED.pages_freed == COUNT;
                state.held -= COUNT;
                state.tail_pages -= COUNT;
                state.released += COUNT;
            }
            entry = {};
        }
    }
    auto take(numa::AllocationResult& result, uint32_t pages) -> bool {
        if (!check((pages == 1 || pages == 512) && result.span.data() != nullptr && result.span.pages() == pages,
                   "actual complete supported span shape before transfer")) {
            capture_limit(8, pages);
            return false;
        }
        auto const ADDRESS = reinterpret_cast<uintptr_t>(result.span.data());
        if (!check(ADDRESS % PAGE == 0 && ADDRESS <= UINTPTR_MAX - static_cast<uint64_t>(pages) * PAGE,
                   "checked actual owned span address before transfer")) {
            capture_limit(16, pages);
            return false;
        }
        if (!check(state.held <= MAX_HELD - pages && pages <= state.baseline.nodes[state.selected].usable &&
                       state.held <= state.baseline.nodes[state.selected].usable - pages,
                   "record capacity holds the unchanged physical page bound")) {
            capture_limit(4, pages);
            return false;
        }
        TailJoin join = TailJoin::NEW_RECORD;
        if (pages == 512) {
            if (!check(state.spans_count < SPANS, "bulk record capacity before transfer")) {
                capture_limit(1, pages);
                return false;
            }
        } else {
            if (state.tail_count != 0) join = tail_join(state.tail[state.tail_count - 1], ADDRESS);
            if (!check(join != TailJoin::INVALID, "checked independent tail adjacency")) {
                capture_limit(16, pages);
                return false;
            }
            if (!check(join != TailJoin::NEW_RECORD || state.tail_count < TAIL, "tail record capacity before transfer")) {
                capture_limit(2, pages);
                return false;
            }
        }
        // All fallible representation checks precede release. A range consists
        // solely of the individually owned, exactly adjacent pages recorded.
        if (pages == 512) {
            auto& record = state.spans[state.spans_count++];
            record.base = result.span.release();
            record.pages = pages;
        } else if (join == TailJoin::FORWARD || join == TailJoin::BACKWARD) {
            auto& record = state.tail[state.tail_count - 1];
            void* const PAGE_ADDRESS = result.span.release();
            if (join == TailJoin::BACKWARD) record.base = PAGE_ADDRESS;
            ++record.pages;
        } else {
            auto& record = state.tail[state.tail_count++];
            record.base = result.span.release();
            record.pages = 1;
        }
        state.held += pages;
        if (pages == 1) ++state.tail_pages;
        if (state.held > state.held_highwater) state.held_highwater = state.held;
        if (state.spans_count > state.bulk_highwater) state.bulk_highwater = state.spans_count;
        if (state.tail_count > state.tail_records_highwater) state.tail_records_highwater = state.tail_count;
        if (state.tail_pages > state.tail_pages_highwater) state.tail_pages_highwater = state.tail_pages;
        return check(result.actual == state.selected, "recorded actual selected prefix bound");
    }
};
class PressureHoldings : public NumaAllocation {
   protected:
    void SetUp() override {
        NumaAllocation::SetUp();
        state = {};
        state.cleanup_valid = true;
        state.baseline.nodes[0].usable = 1024 * 1024;
    }
};
TEST_F(PressureHoldings, RealOwnedSpanBulkAndTailPrefixFullyReturns) {
    {
        Holdings h;
        for (unsigned i = 0; i < 2; ++i) {
            auto r = numa::try_allocate_pages(context(), 512, LARGE, OWNER, Initialization::FULL_OVERWRITE);
            ASSERT_TRUE(h.take(r, 512));
            EXPECT_TRUE(r.span.empty());
        }
        for (unsigned i = 0; i < 7; ++i) {
            auto r = numa::try_allocate_pages(context(), 1, PAGE, OWNER);
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.held, 1031U);
    }
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(state.released, 1031U);
    EXPECT_EQ(state.spans_count, 0U);
    EXPECT_EQ(state.tail_count, 0U);
    EXPECT_TRUE(state.cleanup_valid);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(PressureHoldings, EarlyReturnFreesExactSuccessfulPrefix) {
    auto path = []() {
        Holdings h;
        for (unsigned i = 0; i < 3; ++i) {
            auto r = numa::try_allocate_pages(context(), 1, PAGE, OWNER);
            if (!h.take(r, 1)) return false;
        }
        return false;
    };
    EXPECT_FALSE(path());
    EXPECT_EQ(state.released, 3U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, InvalidSpanShapeRetainsActualResultUntilItsDestructor) {
    {
        Holdings h;
        auto r = numa::try_allocate_pages(context(), 512, LARGE, OWNER);
        EXPECT_FALSE(h.take(r, 1));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(state.held, 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
    EXPECT_NE(state.failure, nullptr);
}
TEST_F(PressureHoldings, PostTransferNodeRefusalStillReturnsCommittedOwnership) {
    model.domains(2);
    {
        Holdings h;
        auto r = numa::try_allocate_pages(context(numa::Mode::BIND, 2, 0), 1, PAGE, OWNER);
        ASSERT_EQ(r.actual, 1);
        EXPECT_FALSE(h.take(r, 1));
        EXPECT_TRUE(r.span.empty());
        EXPECT_EQ(state.held, 1U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 1U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, ExplicitClearThenDestructorDoesNotDoubleRelease) {
    {
        Holdings h;
        auto r = numa::try_allocate_pages(context(), 512, LARGE, OWNER);
        ASSERT_TRUE(h.take(r, 512));
        h.clear();
        EXPECT_EQ(state.released, 512U);
        h.clear();
    }
    EXPECT_EQ(state.released, 512U);
    EXPECT_EQ(model.decremented.size(), 512U);
    EXPECT_EQ(model.live_refs(), 0U);
}
// Backend scripts only the returned Host addresses/ref ledger. The actual
// production factory constructs OwnedSpan; actual take/clear are extracted.
void sequence(Backend& model, std::initializer_list<unsigned> indices) { model.script.assign(indices.begin(), indices.end()); }
auto claim_page() -> numa::AllocationResult { return numa::try_allocate_pages({{0, numa::Mode::LOCAL}, 0, 0}, 1, PAGE, OWNER); }
TEST_F(PressureHoldings, ForwardAdjacentIndependentPagesUseOneRecordAndBatchReturn) {
    sequence(model, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    {
        Holdings h;
        for (unsigned i = 0; i < 20; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.tail_count, 1U);
        EXPECT_EQ(state.tail[0].pages, 20U);
        EXPECT_EQ(state.tail_pages, 20U);
        EXPECT_EQ(state.held_highwater, 20U);
    }
    EXPECT_EQ(state.released, 20U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(model.decremented.size(), 20U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, ReverseAdjacentIndependentPagesExtendExactBase) {
    sequence(model, {5, 4, 3, 2, 1, 0});
    {
        Holdings h;
        for (unsigned i = 0; i < 6; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.tail_count, 1U);
        EXPECT_EQ(state.tail[0].pages, 6U);
        EXPECT_EQ(state.tail[0].base, model.arena);
    }
    EXPECT_EQ(state.released, 6U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, DisjointRecordsAndAlternatingDirectionNeverClaimHoles) {
    sequence(model, {0, 1, 10, 9, 20, 21, 30, 29});
    {
        Holdings h;
        for (unsigned i = 0; i < 8; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        ASSERT_EQ(state.tail_count, 4U);
        for (unsigned i = 0; i < 4; ++i) EXPECT_EQ(state.tail[i].pages, 2U);
        EXPECT_EQ(state.tail_pages, 8U);
    }
    EXPECT_EQ(state.released, 8U);
    EXPECT_EQ(model.decremented.size(), 8U);
    EXPECT_EQ(model.live_refs(), 0U);
}
TEST_F(PressureHoldings, FullRecordLedgerStillAllowsAdjacentActualCandidate) {
    for (unsigned i = 0; i < TAIL; ++i) model.script.push_back(i * 3);
    model.script.push_back((TAIL - 1) * 3 + 1);
    {
        Holdings h;
        for (unsigned i = 0; i < TAIL + 1; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        EXPECT_EQ(state.tail_count, TAIL);
        EXPECT_EQ(state.tail[TAIL - 1].pages, 2U);
        EXPECT_EQ(state.tail_pages, TAIL + 1);
        EXPECT_FALSE(state.limit_captured);
    }
    EXPECT_EQ(state.released, TAIL + 1);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, FullNonadjacentRefusesBeforeTransferAndKeepsFailurePrefix) {
    for (unsigned i = 0; i < TAIL + 1; ++i) model.script.push_back(i * 3);
    {
        Holdings h;
        for (unsigned i = 0; i < TAIL; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        {
            auto refused = claim_page();
            EXPECT_FALSE(h.take(refused, 1));
            EXPECT_FALSE(refused.span.empty());
            EXPECT_EQ(model.live_refs(), TAIL + 1);
            EXPECT_EQ(state.held, TAIL);
            EXPECT_EQ(state.tail_count, TAIL);
            EXPECT_EQ(state.limit_reasons, 2U);
            EXPECT_EQ(state.limit_held, TAIL);
            EXPECT_EQ(state.limit_tail_records, TAIL);
            EXPECT_EQ(state.limit_tail_pages, TAIL);
            EXPECT_FALSE(state.limit_sample_valid);
        }
        EXPECT_EQ(model.live_refs(), TAIL);
    }
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(state.tail_pages, 0U);
    EXPECT_EQ(state.released, TAIL);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.limit_held, TAIL);
    EXPECT_EQ(state.tail_records_highwater, TAIL);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, AddressArithmeticAndSupportedShapeRejectBeforeAnyTransfer) {
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(UINTPTR_MAX - PAGE + 1), 1}, PAGE), TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE), 1}, UINTPTR_MAX - PAGE + 1), TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE + 1), 1}, PAGE), TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE), 0}, PAGE), TailJoin::INVALID);
    EXPECT_EQ(tail_join({reinterpret_cast<void*>(PAGE), 1}, PAGE), TailJoin::INVALID);
    {
        Holdings h;
        auto r = claim_page();
        EXPECT_FALSE(h.take(r, 2));
        EXPECT_FALSE(r.span.empty());
        EXPECT_EQ(state.limit_reasons, 8U);
        EXPECT_EQ(state.held, 0U);
    }
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.released, 0U);
}
TEST_F(PressureHoldings, UsableBoundRefusalCapturesBeforeDestructorAndDoesNotAdopt) {
    sequence(model, {0, 1});
    state.baseline.nodes[0].usable = 1;
    {
        Holdings h;
        {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        {
            auto r = claim_page();
            EXPECT_FALSE(h.take(r, 1));
            EXPECT_FALSE(r.span.empty());
            EXPECT_EQ(state.limit_reasons, 4U);
            EXPECT_EQ(state.limit_held, 1U);
            EXPECT_EQ(state.held, 1U);
        }
    }
    EXPECT_EQ(state.released, 1U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_EQ(state.limit_held, 1U);
    EXPECT_TRUE(state.cleanup_valid);
}
TEST_F(PressureHoldings, MixedBulkAndLongTailReturnEachIndependentReferenceOnce) {
    {
        Holdings h;
        auto bulk = numa::try_allocate_pages(context(), 512, LARGE, OWNER);
        ASSERT_TRUE(h.take(bulk, 512));
        for (unsigned i = 0; i < 300; ++i) model.script.push_back(i);
        for (unsigned i = 0; i < 300; ++i) {
            auto r = claim_page();
            ASSERT_TRUE(h.take(r, 1));
        }
        ASSERT_EQ(state.tail_count, 1U);
        EXPECT_EQ(state.tail[0].pages, 300U);
        EXPECT_EQ(state.tail_pages, 300U);
        h.clear();
        h.clear();
    }
    EXPECT_EQ(state.released, 812U);
    EXPECT_EQ(model.decremented.size(), 812U);
    EXPECT_EQ(state.held, 0U);
    EXPECT_EQ(state.tail_pages, 0U);
    EXPECT_EQ(model.live_refs(), 0U);
    EXPECT_TRUE(state.cleanup_valid);
}

}  // namespace
