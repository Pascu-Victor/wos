#include <cerrno>
#include <platform/ktime/interval_timer_model.hpp>
#include <platform/ktime/process_timer.hpp>
#include <test/ktest.hpp>

namespace {
using namespace ker::mod::time::interval;
}

KTEST(IntervalTimer, ProcessClockDomainsAndAtomicOldValue) {
    ProcessTimers timers;
    Clocks now{100, 200, 300};
    KREQUIRE_EQ(timers.configure(Kind::REAL, {10, 5}, now), ConfigureResult::APPLIED);
    KREQUIRE_EQ(timers.configure(Kind::VIRTUAL, {20, 7}, now), ConfigureResult::APPLIED);
    KREQUIRE_EQ(timers.configure(Kind::PROF, {30, 9}, now), ConfigureResult::APPLIED);
    auto ready = timers.collect({110, 220, 330});
    KEXPECT_EQ(ready.count, 3U);
    uint64_t pending = 0;
    for (size_t i = 0; i < ready.count; ++i) pending |= timers.claim(ready.tokens[i]);
    KEXPECT_EQ(pending, (uint64_t{1} << 13) | (uint64_t{1} << 25) | (uint64_t{1} << 26));
    Value previous;
    KEXPECT_EQ(timers.configure(Kind::REAL, {0, 11}, {112, 220, 330}, &previous), ConfigureResult::APPLIED);
    KEXPECT_EQ(previous.remaining_us, 3U);
    KEXPECT_EQ(previous.interval_us, 5U);
}

KTEST(IntervalTimer, TokensCannotCrossOwnerReplacementOrPeriod) {
    ProcessTimers timers, other;
    KREQUIRE_EQ(timers.configure(Kind::REAL, {1, 1}, {}), ConfigureResult::APPLIED);
    auto first = timers.collect({1, 0, 0});
    KREQUIRE_EQ(first.count, 1U);
    KEXPECT_EQ(other.claim(first.tokens[0]), 0U);
    KEXPECT_EQ(timers.claim(first.tokens[0]), ProcessTimers::signal_mask(Kind::REAL));
    auto second = timers.collect({2, 0, 0});
    KREQUIRE_EQ(second.count, 1U);
    KEXPECT_EQ(timers.claim(first.tokens[0]), 0U);
    KEXPECT_TRUE(timers.cancel(Kind::REAL));
    KEXPECT_EQ(timers.claim(second.tokens[0]), 0U);
    timers.close();
    KEXPECT_EQ(timers.configure(Kind::REAL, {1, 1}, {}), ConfigureResult::CLOSED);
}

KTEST(IntervalTimer, OverflowAndVersionExhaustionNeverRearmForever) {
    ProcessTimers timers(UINT64_MAX - 1);
    KREQUIRE_EQ(timers.configure(Kind::PROF, {10, 10}, {0, 0, UINT64_MAX - 5}), ConfigureResult::APPLIED);
    KEXPECT_EQ(timers.configure(Kind::PROF, {1, 1}, {}), ConfigureResult::GENERATION_EXHAUSTED);
    auto ready = timers.collect({0, 0, UINT64_MAX});
    KREQUIRE_EQ(ready.count, 1U);
    KEXPECT_EQ(timers.claim(ready.tokens[0]), ProcessTimers::signal_mask(Kind::PROF));
    KEXPECT_EQ(timers.collect({0, 0, UINT64_MAX}).count, 0U);
    KEXPECT_TRUE(timers.cancel(Kind::PROF));
}

KTEST(IntervalTimer, ServiceRealAndProcessCpuLifetimes) { KEXPECT_TRUE(ker::mod::time::process_timer::selftest_service()); }
