#include <gtest/gtest.h>

#include <platform/rtc/wall_clock.hpp>

namespace rtc = ker::mod::rtc;

TEST(WallClock, CalendarValidation) {
    uint64_t epoch = 0;
    EXPECT_TRUE(rtc::calendar_epoch(1970, 1, 1, 0, 0, 0, epoch));
    EXPECT_EQ(epoch, 0U);
    EXPECT_TRUE(rtc::calendar_epoch(2000, 2, 29, 0, 0, 0, epoch));
    EXPECT_EQ(epoch, 951782400U);
    EXPECT_FALSE(rtc::calendar_epoch(2100, 2, 29, 0, 0, 0, epoch));
    EXPECT_FALSE(rtc::calendar_epoch(2026, 0, 1, 0, 0, 0, epoch));
    EXPECT_FALSE(rtc::calendar_epoch(2026, 13, 1, 0, 0, 0, epoch));
    EXPECT_FALSE(rtc::calendar_epoch(2026, 4, 31, 0, 0, 0, epoch));
    EXPECT_FALSE(rtc::calendar_epoch(2026, 4, 30, 24, 0, 0, epoch));
    EXPECT_FALSE(rtc::calendar_epoch(1969, 1, 1, 0, 0, 0, epoch));
}

TEST(WallClock, BoundedInitialStepAndRejectedCorrections) {
    rtc::WallClock clock{.anchor_epoch_ns = 1000000000000000ULL};
    EXPECT_FALSE(clock.adjust(100, INT64_MIN));
    EXPECT_FALSE(clock.adjust(100, INT64_MAX));
    EXPECT_FALSE(clock.adjust(100, rtc::MAX_CORRECTION_NS + 1));
    EXPECT_EQ(clock.epoch(100), 1000000000000100ULL);
    EXPECT_EQ(clock.rejected, 3U);
    EXPECT_TRUE(clock.adjust(100, -100));
    EXPECT_EQ(clock.epoch(100), 1000000000000000ULL);
    EXPECT_EQ(clock.steps, 1U);
}

TEST(WallClock, PositiveAndNegativeSlewContinuousAndBounded) {
    for (int64_t delta : {-1000000LL, 1000000LL}) {
        rtc::WallClock clock{.anchor_epoch_ns = 10000000000ULL};
        ASSERT_TRUE(clock.adjust(0, 0));
        ASSERT_TRUE(clock.adjust(1000000000, delta));
        EXPECT_EQ(clock.epoch(1000000000), 11000000000ULL);
        EXPECT_EQ(clock.applied(2000000000), delta / 2);
        EXPECT_EQ(clock.applied(3000000000), delta);
        uint64_t previous = clock.epoch(1000000000);
        for (uint64_t now = 1000000001; now < 1000100000; ++now) {
            auto const CURRENT = clock.epoch(now);
            EXPECT_GE(CURRENT, previous);
            EXPECT_LE(CURRENT - previous, 2U);
            previous = CURRENT;
        }
        uint64_t const BEFORE = clock.epoch(1500000000);
        ASSERT_TRUE(clock.adjust(1500000000, -delta));
        EXPECT_EQ(clock.epoch(1500000000), BEFORE);
        EXPECT_EQ(clock.applied(3500000000), -delta);
    }
}

TEST(WallClock, NegativeSlewCannotRegressAtEpochSaturation) {
    rtc::WallClock clock{.anchor_epoch_ns = UINT64_MAX - 1000};
    ASSERT_TRUE(clock.adjust(0, 0));
    ASSERT_TRUE(clock.adjust(0, -1000000));
    EXPECT_EQ(clock.epoch(999), UINT64_MAX - 1);
    EXPECT_EQ(clock.epoch(1000), UINT64_MAX);
    EXPECT_EQ(clock.epoch(2000), UINT64_MAX);
    EXPECT_EQ(clock.epoch(4000), UINT64_MAX);
    EXPECT_EQ(clock.epoch(UINT64_MAX), UINT64_MAX);
    ASSERT_TRUE(clock.adjust(4000, -rtc::MAX_CORRECTION_NS));
    EXPECT_EQ(clock.epoch(4000), UINT64_MAX);
    EXPECT_EQ(clock.epoch(10000000), UINT64_MAX);

    rtc::WallClock zero{};
    ASSERT_TRUE(zero.adjust(0, 0));
    ASSERT_TRUE(zero.adjust(0, -rtc::MAX_CORRECTION_NS));
    EXPECT_EQ(zero.epoch(1999), 1999U);
    EXPECT_EQ(zero.epoch(2000), 1999U);
    EXPECT_EQ(zero.epoch(2001), 2000U);
}
