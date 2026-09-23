#include <gtest/gtest.h>

#include <dev/iommu/vtd_model.hpp>

namespace vtd = ker::dev::iommu::vtd;

TEST(VtdModel, EncodesLegacyTablesWithNoReservedBitsOrFaultSuppression) {
    auto const ROOT = vtd::root_entry(0x12345000, 48);
    ASSERT_TRUE(ROOT);
    EXPECT_EQ(ROOT->low, 0x12345001U);
    EXPECT_EQ(ROOT->high, 0U);
    auto const CONTEXT = vtd::context_entry(0x67890000, 48, 0x1234, 4, 0x06, 65536);
    ASSERT_TRUE(CONTEXT);
    EXPECT_EQ(CONTEXT->low, 0x67890001U);
    EXPECT_EQ(CONTEXT->high, 0x123402U);
    EXPECT_EQ(vtd::page_entry(0xabcdef000, 48, vtd::Access::read), 0xabcdef001U);
    EXPECT_EQ(vtd::page_entry(0xabcdef000, 48, vtd::Access::write), 0xabcdef002U);
    EXPECT_EQ(vtd::table_entry(0xabcdef000, 48), 0xabcdef003U);
    EXPECT_EQ(vtd::GLOBAL_CONTEXT_COMMAND, 0xa000000000000000ULL);
    EXPECT_EQ(vtd::GLOBAL_IOTLB_COMMAND, 0x9003000000000000ULL);
}

TEST(VtdModel, RejectsTruncationMisalignmentUnsupportedWidthsAndDomainIds) {
    for (uint8_t bits : {0, 11, 53, 64, 255}) {
        EXPECT_FALSE(vtd::root_entry(0, bits));
        EXPECT_FALSE(vtd::page_entry(0, bits, vtd::Access::read));
    }
    for (uint64_t address : {uint64_t{1}, uint64_t{4095}, uint64_t{1} << 48, UINT64_MAX}) {
        EXPECT_FALSE(vtd::root_entry(address, 48));
        EXPECT_FALSE(vtd::page_entry(address, 48, vtd::Access::read_write));
        EXPECT_FALSE(vtd::context_entry(address, 48, 1, 4, 6, 16));
    }
    EXPECT_TRUE(vtd::root_entry((uint64_t{1} << 52) - 4096, 52));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 16, 4, 6, 16));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 0, 4, 6, 0));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 1, 4, 6, 65537));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 1, 4, 2, 16));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 1, 2, 0xff, 16));
    EXPECT_FALSE(vtd::context_entry(0x1000, 48, 1, 6, 0xff, 16));
    EXPECT_FALSE(vtd::page_entry(0x1000, 48, static_cast<vtd::Access>(0)));
    EXPECT_FALSE(vtd::page_entry(0x1000, 48, static_cast<vtd::Access>(4)));
}

TEST(VtdModel, WalkIndicesCoverEveryLevelAndRejectInvalidShifts) {
    uint64_t const ADDRESS =
        (uint64_t{1} << 48) | (uint64_t{2} << 39) | (uint64_t{3} << 30) | (uint64_t{4} << 21) | (uint64_t{5} << 12) | 4095;
    for (uint8_t level = 1; level <= 5; ++level) {
        EXPECT_EQ(vtd::page_index(ADDRESS, level), 6U - level);
    }
    EXPECT_FALSE(vtd::page_index(ADDRESS, 0));
    EXPECT_FALSE(vtd::page_index(ADDRESS, 6));
    EXPECT_FALSE(vtd::page_index(ADDRESS, 255));
}

TEST(VtdModel, RequiresContextThenIotlbCompletionForTheExactTicket) {
    vtd::Invalidation state;
    EXPECT_FALSE(state.completed(0));
    uint64_t const FIRST = state.begin();
    ASSERT_NE(FIRST, 0U);
    EXPECT_EQ(state.begin(), 0U);
    EXPECT_FALSE(state.issue_iotlb(FIRST));
    EXPECT_EQ(state.observe_iotlb(FIRST, uint64_t{1} << 57), vtd::Observation::stale);
    EXPECT_EQ(state.observe_context(FIRST, vtd::BUSY), vtd::Observation::busy);
    EXPECT_FALSE(state.completed(FIRST));
    EXPECT_EQ(state.observe_context(FIRST, uint64_t{1} << 59), vtd::Observation::advance);
    EXPECT_FALSE(state.completed(FIRST));
    EXPECT_TRUE(state.issue_iotlb(FIRST));
    EXPECT_FALSE(state.issue_iotlb(FIRST));
    EXPECT_EQ(state.observe_iotlb(FIRST, vtd::BUSY), vtd::Observation::busy);
    EXPECT_FALSE(state.completed(FIRST));
    EXPECT_EQ(state.observe_iotlb(FIRST, uint64_t{1} << 57), vtd::Observation::advance);
    EXPECT_TRUE(state.completed(FIRST));
    EXPECT_FALSE(state.timeout(FIRST));
    uint64_t const SECOND = state.begin();
    EXPECT_GT(SECOND, FIRST);
    EXPECT_FALSE(state.completed(FIRST));
    EXPECT_EQ(state.observe_context(FIRST, uint64_t{1} << 59), vtd::Observation::stale);
    EXPECT_EQ(state.observe_iotlb(FIRST, uint64_t{1} << 57), vtd::Observation::stale);
    EXPECT_FALSE(state.timeout(FIRST));
    EXPECT_EQ(state.phase(), vtd::InvalidationPhase::context);
}

TEST(VtdModel, RejectedOrNarrowerInvalidationsQuarantineInsteadOfReleasingFrames) {
    for (uint64_t actual : {0, 2, 3}) {
        vtd::Invalidation context;
        auto const TICKET = context.begin();
        EXPECT_EQ(context.observe_context(TICKET, actual << 59), vtd::Observation::failed);
        EXPECT_FALSE(context.completed(TICKET));
        EXPECT_EQ(context.begin(), 0U);
        EXPECT_FALSE(context.issue_iotlb(TICKET));
        EXPECT_EQ(context.observe_context(TICKET, uint64_t{1} << 59), vtd::Observation::stale);
        vtd::Invalidation iotlb;
        auto const OTHER = iotlb.begin();
        ASSERT_EQ(iotlb.observe_context(OTHER, uint64_t{1} << 59), vtd::Observation::advance);
        ASSERT_TRUE(iotlb.issue_iotlb(OTHER));
        EXPECT_EQ(iotlb.observe_iotlb(OTHER, actual << 57), vtd::Observation::failed);
        EXPECT_FALSE(iotlb.completed(OTHER));
        EXPECT_EQ(iotlb.begin(), 0U);
    }
}

TEST(VtdModel, TimeoutAtAnyPendingStageRejectsLateCompletionAndNewCommands) {
    for (unsigned stage = 0; stage < 3; ++stage) {
        vtd::Invalidation state;
        auto const TICKET = state.begin();
        if (stage >= 1) ASSERT_EQ(state.observe_context(TICKET, uint64_t{1} << 59), vtd::Observation::advance);
        if (stage >= 2) ASSERT_TRUE(state.issue_iotlb(TICKET));
        EXPECT_TRUE(state.timeout(TICKET));
        EXPECT_FALSE(state.completed(TICKET));
        EXPECT_EQ(state.phase(), vtd::InvalidationPhase::quarantined);
        EXPECT_EQ(state.observe_context(TICKET, uint64_t{1} << 59), vtd::Observation::stale);
        EXPECT_EQ(state.observe_iotlb(TICKET, uint64_t{1} << 57), vtd::Observation::stale);
        EXPECT_EQ(state.begin(), 0U);
    }
}
