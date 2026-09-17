#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>
#include <random>

namespace arithmetic = ker::mod::time::arithmetic;

namespace {
auto oracle(uint64_t value, uint64_t multiplier, uint64_t divisor, bool up) -> uint64_t {
    if (divisor == 0) return UINT64_MAX;
    auto const PRODUCT = static_cast<unsigned __int128>(value) * multiplier;
    auto result = PRODUCT / divisor;
    if (up && PRODUCT % divisor != 0) ++result;
    return result > UINT64_MAX ? UINT64_MAX : static_cast<uint64_t>(result);
}
}  // namespace

TEST(TimeArithmetic, FullWidthConversionAgainstIndependentOracle) {
    constexpr std::array<uint64_t, 12> VALUES{
        0, 1, 2, 999, 1000, 1000000, 1000000000, 3000000000, UINT64_MAX / 2, UINT64_MAX / 2 + 1, UINT64_MAX - 1, UINT64_MAX};
    for (auto value : VALUES)
        for (auto multiplier : VALUES)
            for (auto divisor : VALUES)
                for (bool up : {false, true})
                    ASSERT_EQ(arithmetic::mul_div(value, multiplier, divisor, up), oracle(value, multiplier, divisor, up));
    std::mt19937_64 random(0x31A817);
    for (unsigned i = 0; i < 100000; ++i) {
        auto const VALUE = random();
        auto const MULTIPLIER = random();
        auto const DIVISOR = random();
        ASSERT_EQ(arithmetic::mul_div(VALUE, MULTIPLIER, DIVISOR), oracle(VALUE, MULTIPLIER, DIVISOR, false));
        ASSERT_EQ(arithmetic::mul_div(VALUE, MULTIPLIER, DIVISOR, true), oracle(VALUE, MULTIPLIER, DIVISOR, true));
    }
}

TEST(TimeArithmetic, ClockAndWaitRoundingAtLongUptimes) {
    // 100 MHz HPET after 30 days: old ticks*femtoseconds wraps.
    EXPECT_EQ(arithmetic::mul_div(259200000000000ULL, 10000000, 1000000000), 2592000000000ULL);
    EXPECT_EQ(arithmetic::mul_div(3000000000ULL * 86400 * 365, 1000000000, 3000000000ULL), 31536000000000000ULL);
    EXPECT_EQ(arithmetic::mul_div(1, 1000000000, 3000000000ULL), 0U);
    EXPECT_EQ(arithmetic::mul_div(1, 1000000000, 3000000000ULL, true), 1U);
    EXPECT_EQ(arithmetic::mul_div(UINT64_MAX, UINT64_MAX, UINT64_MAX, true), UINT64_MAX);
}

TEST(TimeArithmetic, SignedAdjustmentsCannotWrap) {
    EXPECT_EQ(arithmetic::add_signed(0, -1), 0U);
    EXPECT_EQ(arithmetic::add_signed(UINT64_MAX, 1), UINT64_MAX);
    EXPECT_EQ(arithmetic::add_signed(uint64_t{1} << 63, INT64_MIN), 0U);
    EXPECT_EQ(arithmetic::add_signed(UINT64_MAX, INT64_MIN), uint64_t{INT64_MAX});
    EXPECT_EQ(arithmetic::saturating_signed_add(INT64_MIN, -1), INT64_MIN);
    EXPECT_EQ(arithmetic::saturating_signed_add(INT64_MAX, 1), INT64_MAX);
    EXPECT_EQ(arithmetic::saturating_signed_add(INT64_MIN, INT64_MAX), -1);
}

TEST(TimeArithmetic, DeadlinesAndPeriodicPhase) {
    EXPECT_EQ(arithmetic::saturating_add(UINT64_MAX - 3, 4), UINT64_MAX);
    EXPECT_EQ(arithmetic::remaining(40, 30), 0U);
    EXPECT_EQ(arithmetic::remaining(30, 40), 10U);
    EXPECT_EQ(arithmetic::periodic_next(10, 7, 8), 10U);
    EXPECT_EQ(arithmetic::periodic_next(10, 7, 10), 17U);
    EXPECT_EQ(arithmetic::periodic_next(10, 7, 32), 38U);
    EXPECT_EQ(arithmetic::periodic_next(10, 0, 32), UINT64_MAX);
    EXPECT_EQ(arithmetic::periodic_next(UINT64_MAX - 10, 20, UINT64_MAX - 1), UINT64_MAX);
}
