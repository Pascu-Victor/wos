#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <platform/ktime/clocksource_policy.hpp>
#include <thread>

namespace policy = ker::mod::time::clocksource;
using State = policy::State;

TEST(ClocksourcePolicy, RejectsUnqualifiedHardwareBeforeTscSelection) {
    EXPECT_FALSE(policy::valid_hpet(0, true));
    EXPECT_FALSE(policy::valid_hpet(100000001, true));
    EXPECT_FALSE(policy::valid_hpet(10000000, false));
    EXPECT_TRUE(policy::valid_hpet(100000000, true));
    EXPECT_EQ(policy::qualify(false, true, 3000000000, 3000000000, 1), State::missing_tsc);
    EXPECT_EQ(policy::qualify(true, false, 3000000000, 3000000000, 1), State::noninvariant_tsc);
    EXPECT_EQ(policy::qualify(true, true, 0, 0, 1), State::calibration_failed);
    EXPECT_EQ(policy::qualify(true, true, 3000000000, 3000000000, 2), State::smp_unqualified);
    EXPECT_EQ(policy::qualify(true, true, 3000000000, 3000000000, 0), State::smp_unqualified);
    EXPECT_EQ(policy::qualify(true, true, 3000000000, 3003000000, 1), State::tsc);
    EXPECT_EQ(policy::qualify(true, true, 3000000000, 3003000001, 1), State::calibration_failed);
    EXPECT_EQ(policy::qualify(true, true, 3000000000, UINT64_MAX, 1), State::calibration_failed);
}

TEST(ClocksourcePolicy, WatchdogBracketsSamplingDelayAndBoundsFallbackPlateau) {
    EXPECT_EQ(policy::deviation(150, 100, 200), 0U);
    EXPECT_EQ(policy::deviation(50, 100, 200), 50U);
    EXPECT_EQ(policy::deviation(250, 100, 200), 50U);
    EXPECT_EQ(policy::deviation(UINT64_MAX, 0, 1), UINT64_MAX - 1);
    // A VM exit between reads widens the reference interval without accusing
    // a correctly ticking TSC. A TSC step outside it is rejected before publish.
    EXPECT_EQ(policy::deviation(1000000000, 10, 2000000000), 0U);
    EXPECT_GT(policy::deviation(3000000000, 10, 2000000000), policy::MAX_DEVIATION_NS);
    std::atomic<uint64_t> floor{0};
    uint64_t const REFERENCE = 1000000;
    EXPECT_EQ(policy::publish_max(floor, REFERENCE + policy::MAX_DEVIATION_NS), 1100000U);
    EXPECT_EQ(policy::publish_max(floor, REFERENCE), 1100000U);
    EXPECT_EQ(policy::publish_max(floor, 1100001), 1100001U);
    EXPECT_EQ(policy::publish_max(floor, UINT64_MAX), UINT64_MAX);
    EXPECT_EQ(policy::publish_max(floor, 0), UINT64_MAX);
}

TEST(ClocksourcePolicy, ConcurrentAndMigratedReadersNeverRegress) {
    std::atomic<uint64_t> floor{0};
    std::atomic<bool> regressed{false};
    std::array<std::thread, 4> readers;
    for (unsigned cpu = 0; cpu < readers.size(); ++cpu) {
        readers[cpu] = std::thread([&, cpu] {
            uint64_t previous = 0;
            for (uint64_t i = 0; i < 100000; ++i) {
                // Deliberately emulate negative per-CPU offsets, delayed
                // samples, and a reset of the candidate after source fallback.
                uint64_t const CANDIDATE = i % 7 == 0 ? 0 : i * 4 + cpu;
                uint64_t const NOW = policy::publish_max(floor, CANDIDATE);
                if (NOW < previous) regressed.store(true);
                previous = NOW;
            }
        });
    }
    for (auto& reader : readers) reader.join();
    EXPECT_FALSE(regressed.load());
    EXPECT_EQ(policy::publish_max(floor, 0), 399999U);
}
