#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <platform/mm/reclaim_policy.hpp>

namespace reclaim = ker::mod::mm::reclaim;

TEST(ReclaimPolicyArithmeticTest, SaturatesWithoutWrapping) {
    constexpr uint64_t MAX = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(reclaim::saturating_add(MAX - 2, 1), MAX - 1);
    EXPECT_EQ(reclaim::saturating_add(MAX - 2, 3), MAX);
    EXPECT_EQ(reclaim::saturating_mul(MAX / 4, 4), MAX - 3);
    EXPECT_EQ(reclaim::saturating_mul((MAX / 4) + 1, 4), MAX);
    EXPECT_EQ(reclaim::order_pages(0), 1U);
    EXPECT_EQ(reclaim::order_pages(12), 4096U);
    EXPECT_EQ(reclaim::order_pages(63), MAX);
}

TEST(ReclaimPolicyWatermarkTest, ScalesLegacyReserveByZoneAndOrder) {
    constexpr uint64_t MANAGED_PAGES = 1'048'576;
    auto const SMALL = reclaim::zone_watermarks(MANAGED_PAGES / 4, MANAGED_PAGES, 0);
    auto const LARGE = reclaim::zone_watermarks((MANAGED_PAGES * 3) / 4, MANAGED_PAGES, 0);
    EXPECT_LT(SMALL.low_pages, LARGE.low_pages);
    EXPECT_LE(SMALL.critical_pages, SMALL.low_pages);
    EXPECT_LE(SMALL.low_pages, SMALL.high_pages);
    EXPECT_LE(LARGE.critical_pages, LARGE.low_pages);
    EXPECT_LE(LARGE.low_pages, LARGE.high_pages);

    auto const HIGH_ORDER = reclaim::zone_watermarks(MANAGED_PAGES, MANAGED_PAGES, 10);
    EXPECT_GE(HIGH_ORDER.critical_pages, reclaim::order_pages(10) * 2);
    EXPECT_GE(HIGH_ORDER.low_pages, reclaim::order_pages(10) * 4);
    EXPECT_GE(HIGH_ORDER.high_pages, HIGH_ORDER.low_pages);
}

TEST(ReclaimPolicyPressureTest, AccountsForWatermarksAndContiguousOrder) {
    reclaim::Watermarks const WATERMARKS{
        .critical_pages = 16,
        .low_pages = 32,
        .high_pages = 48,
    };
    EXPECT_EQ(reclaim::classify_zone_pressure(64, 1024, 6, 4, WATERMARKS), reclaim::PressureLevel::NONE);
    EXPECT_EQ(reclaim::classify_zone_pressure(32, 1024, 6, 4, WATERMARKS), reclaim::PressureLevel::LOW);
    EXPECT_EQ(reclaim::classify_zone_pressure(16, 1024, 6, 4, WATERMARKS), reclaim::PressureLevel::CRITICAL);
    EXPECT_EQ(reclaim::classify_zone_pressure(64, 1024, 3, 4, WATERMARKS), reclaim::PressureLevel::CRITICAL);
    EXPECT_EQ(reclaim::classify_zone_pressure(64, 8, 6, 4, WATERMARKS), reclaim::PressureLevel::UNSATISFIABLE);
    EXPECT_EQ(reclaim::pressure_priority(reclaim::PressureLevel::LOW), reclaim::ReclaimPriority::LOW);
    EXPECT_EQ(reclaim::pressure_priority(reclaim::PressureLevel::CRITICAL), reclaim::ReclaimPriority::CRITICAL);
}

TEST(ReclaimPolicyBudgetTest, ConvertsOnlyReclaimedUnitsAndClampsBatches) {
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::PAGES, 7, 64), 7U);
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::PAGES, 128, 64), 64U);
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::BYTES, 2, 16'384), 8192U);
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::BYTES, 8, 16'384), 16'384U);
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::OBJECTS, 1, 37), 37U);
    EXPECT_EQ(reclaim::callback_budget(reclaim::ReclaimUnit::PAGES, 1, 0), 0U);
}

TEST(ReclaimPolicyBackoffTest, IsBoundedAndExponential) {
    EXPECT_EQ(reclaim::backoff_rounds(0), 1U);
    EXPECT_EQ(reclaim::backoff_rounds(1), 2U);
    EXPECT_EQ(reclaim::backoff_rounds(2), 4U);
    EXPECT_EQ(reclaim::backoff_rounds(6), 64U);
    EXPECT_EQ(reclaim::backoff_rounds(63), 64U);
}
