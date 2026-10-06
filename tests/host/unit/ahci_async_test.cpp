#include <gtest/gtest.h>

#include <dev/ahci_async.hpp>

namespace rules = ker::dev::ahci::async_rules;
using ker::dev::BlockRequestToken;

TEST(AhciAsync, EightIndependentTagsCompleteOutOfOrderAndIgnoreQueueAdmissionCiClear) {
    rules::Port port;
    port.depth = rules::MAX_SLOTS;
    uint32_t active = 0;
    for (size_t i = 0; i < port.depth; ++i) {
        ASSERT_EQ(rules::reserve(port, {1, i + 1, static_cast<uint16_t>(i)}, 512, 100, true), i);
        ASSERT_TRUE(rules::publish(port, i, port.slots[i].generation));
        active |= uint32_t{1} << i;
    }
    EXPECT_EQ(rules::reserve(port, {1, 99, 9}, 512, 100, true), rules::MAX_SLOTS);
    for (size_t i = 0; i < port.depth; ++i) EXPECT_FALSE(rules::claim(port, i, port.slots[i].generation, port.epoch, 0, active));
    for (size_t i : {size_t{7}, size_t{2}, size_t{5}, size_t{0}, size_t{3}, size_t{6}, size_t{1}, size_t{4}}) {
        auto const generation = port.slots[i].generation;
        active &= ~(uint32_t{1} << i);
        ASSERT_TRUE(rules::claim(port, i, generation, port.epoch, 0, active));
        EXPECT_FALSE(rules::claim(port, i, generation, port.epoch, 0, active));
        EXPECT_EQ(rules::reserve(port, {1, 99, 9}, 512, 100, false), rules::MAX_SLOTS);
        ASSERT_TRUE(rules::release(port, i, generation));
    }
    EXPECT_EQ(rules::owned(port), 0U);
}

TEST(AhciAsync, FlushAndOrdinaryDmaAreExclusiveUntilCopyAndRetirementFinish) {
    rules::Port port;
    port.depth = rules::MAX_SLOTS;
    auto const index = rules::reserve(port, {1, 1, 0}, 0, 100, false);
    ASSERT_EQ(index, 0U);
    EXPECT_EQ(rules::reserve(port, {1, 2, 1}, 512, 100, true), rules::MAX_SLOTS);
    ASSERT_TRUE(rules::publish(port, index, 1));
    ASSERT_TRUE(rules::claim(port, index, 1, 1, 0, 0));
    EXPECT_EQ(rules::reserve(port, {1, 2, 1}, 512, 100, true), rules::MAX_SLOTS);
    ASSERT_TRUE(rules::release(port, index, 1));
    EXPECT_EQ(rules::reserve(port, {1, 2, 1}, 512, 100, true), 0U);
}

TEST(AhciAsync, CancellationDuringPreparationPreventsHardwarePublication) {
    rules::Port port;
    BlockRequestToken const token{1, 1, 0};
    ASSERT_EQ(rules::reserve(port, token, 512, 100, true), 0U);
    ASSERT_TRUE(rules::cancel(port, token, -ECANCELED));
    EXPECT_FALSE(rules::publish(port, 0, 1));
    EXPECT_TRUE(port.frozen);
    EXPECT_EQ(port.slots[0].cancel_reason, -ECANCELED);
    EXPECT_FALSE(rules::finish_reset(port, true));
    ASSERT_TRUE(rules::release(port, 0, 1));
    ASSERT_TRUE(rules::finish_reset(port, true));
    EXPECT_EQ(port.epoch, 2U);
}

TEST(AhciAsync, PreparingDeadlineCannotStrandRetainedOwnersOrClaimClearHardwareBits) {
    rules::Port port;
    BlockRequestToken const token{1, 1, 0};
    ASSERT_EQ(rules::reserve(port, token, 512, 100, true), 0U);
    EXPECT_FALSE(rules::claim(port, 0, 1, 1, 0, 0));
    EXPECT_FALSE(rules::timeout(port, 99));
    ASSERT_TRUE(rules::timeout(port, 100));
    EXPECT_EQ(port.slots[0].phase, rules::Phase::preparing);
    EXPECT_EQ(port.slots[0].cancel_reason, -ETIMEDOUT);
    EXPECT_FALSE(rules::publish(port, 0, 1));
    EXPECT_EQ(port.reset_reason, -ETIMEDOUT);
    ASSERT_TRUE(rules::release(port, 0, 1));
    ASSERT_TRUE(rules::finish_reset(port, true));
    EXPECT_EQ(rules::owned(port), 0U);
}

TEST(AhciAsync, TimeoutFreezesEverySlotBeforeRegisterClearingCanLookLikeSuccess) {
    rules::Port port;
    port.depth = rules::MAX_SLOTS;
    for (size_t i = 0; i < 8; ++i) {
        ASSERT_EQ(rules::reserve(port, {1, i + 1, static_cast<uint16_t>(i)}, 512, 100 + i, true), i);
        ASSERT_TRUE(rules::publish(port, i, 1));
    }
    EXPECT_FALSE(rules::timeout(port, 99));
    ASSERT_TRUE(rules::timeout(port, 100));
    EXPECT_EQ(port.reset_reason, -ETIMEDOUT);
    EXPECT_EQ(port.slots[0].cancel_reason, -ETIMEDOUT);
    for (size_t i = 0; i < 8; ++i) EXPECT_FALSE(rules::claim(port, i, 1, 1, 0, 0));
    EXPECT_FALSE(rules::finish_reset(port, true));
    for (size_t i = 0; i < 8; ++i) ASSERT_TRUE(rules::release(port, i, 1));
    EXPECT_TRUE(rules::finish_reset(port, true));
}

TEST(AhciAsync, OldGenerationAndResetEpochCannotRetireReusedTag) {
    rules::Port port;
    for (uint64_t epoch = 1; epoch < 1000; ++epoch) {
        ASSERT_EQ(rules::reserve(port, {epoch, epoch, 0}, 512, 0, true), 0U);
        auto const generation = port.slots[0].generation;
        ASSERT_TRUE(rules::publish(port, 0, generation));
        EXPECT_FALSE(rules::claim(port, 0, generation - 1, epoch, 0, 0));
        if (epoch > 1) EXPECT_FALSE(rules::claim(port, 0, generation, epoch - 1, 0, 0));
        rules::freeze(port);
        EXPECT_FALSE(rules::claim(port, 0, generation, epoch, 0, 0));
        ASSERT_TRUE(rules::release(port, 0, generation));
        ASSERT_TRUE(rules::finish_reset(port, true));
    }
}

TEST(AhciAsync, UnprovenStopAndGenerationExhaustionNeverReopenAdmission) {
    rules::Port port;
    rules::freeze(port);
    EXPECT_FALSE(rules::finish_reset(port, false));
    EXPECT_TRUE(port.offline);
    EXPECT_EQ(rules::reserve(port, {1, 1, 0}, 512, 0, true), rules::MAX_SLOTS);
    port = {};
    port.slots[0].generation = UINT64_MAX;
    EXPECT_EQ(rules::reserve(port, {1, 1, 0}, 512, 0, true), rules::MAX_SLOTS);
    EXPECT_TRUE(port.offline);
    port = {};
    port.epoch = UINT64_MAX;
    rules::freeze(port);
    EXPECT_FALSE(rules::finish_reset(port, true));
    EXPECT_TRUE(port.offline);
}
