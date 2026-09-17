#include <gtest/gtest.h>

#include <array>
#include <platform/ktime/clockevent.hpp>

using ker::mod::time::ClockeventCalibration;

TEST(Clockevent, MissingCalibrationNeverDisarms) {
    ClockeventCalibration missing;
    EXPECT_FALSE(missing.valid());
    EXPECT_EQ(missing.ticks_for_delay_us(0), 1U);
    EXPECT_EQ(missing.ticks_for_delay_us(UINT64_MAX), 1U);
    EXPECT_EQ(missing.resolution_ns(), 0U);
    EXPECT_EQ(missing.minimum_delay_us(), 0U);
    EXPECT_EQ(missing.maximum_delay_us(), 0U);
}

TEST(Clockevent, FractionalConversionRoundsUpAndReportsRange) {
    ClockeventCalibration const SCALE{3, 10};
    EXPECT_EQ(SCALE.ticks_for_delay_us(0), 1U);
    EXPECT_EQ(SCALE.ticks_for_delay_us(1), 1U);
    EXPECT_EQ(SCALE.ticks_for_delay_us(4), 2U);
    EXPECT_EQ(SCALE.ticks_for_delay_us(10), 3U);
    EXPECT_EQ(SCALE.ticks_for_delay_us(11), 4U);
    EXPECT_EQ(SCALE.resolution_ns(), 3334U);
    EXPECT_EQ(SCALE.minimum_delay_us(), 4U);
    EXPECT_EQ(SCALE.maximum_delay_us(), uint64_t{UINT32_MAX} * 10 / 3);
}

TEST(Clockevent, WideConversionsMatchIntegerReference) {
    constexpr std::array<uint64_t, 9> VALUES{0, 1, 3, 4000, 10000, UINT32_MAX, uint64_t{UINT32_MAX} + 1, UINT64_MAX / 2, UINT64_MAX};
    for (uint64_t ticks : VALUES) {
        for (uint64_t sample_us : VALUES) {
            ClockeventCalibration const SCALE{ticks, sample_us};
            uint32_t previous = 0;
            for (uint64_t delay : VALUES) {
                uint64_t expected = 1;
                if (ticks != 0 && sample_us != 0 && delay != 0) {
                    auto const PRODUCT = static_cast<unsigned __int128>(ticks) * delay;
                    auto const QUOTIENT = (PRODUCT + sample_us - 1) / sample_us;
                    expected = QUOTIENT > UINT32_MAX ? UINT32_MAX : static_cast<uint64_t>(QUOTIENT);
                    if (expected == 0) expected = 1;
                }
                auto const ACTUAL = SCALE.ticks_for_delay_us(delay);
                EXPECT_EQ(ACTUAL, expected);
                EXPECT_GE(ACTUAL, previous);
                previous = ACTUAL;
            }
        }
    }
}

TEST(Clockevent, CpuCalibrationsRemainIndependent) {
    ClockeventCalibration const FIRST{400000, 4000};
    ClockeventCalibration const SECOND{200000, 4000};
    EXPECT_EQ(FIRST.ticks_for_delay_us(4000), 400000U);
    EXPECT_EQ(SECOND.ticks_for_delay_us(4000), 200000U);
    EXPECT_EQ(FIRST.ticks_for_delay_us(10000), 1000000U);
    EXPECT_EQ(SECOND.ticks_for_delay_us(UINT64_MAX), UINT32_MAX);
}
