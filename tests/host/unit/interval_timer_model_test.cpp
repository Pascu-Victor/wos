#include <gtest/gtest.h>

#include <cstdint>
#include <platform/ktime/interval_timer_model.hpp>

namespace timer = ker::mod::time::interval;
using timer::Clocks;
using timer::ConfigureResult;
using timer::Kind;
using timer::ProcessTimers;
using timer::Value;

TEST(IntervalTimerModel, DomainsAndAtomicReplacement) {
    ProcessTimers state;
    Clocks now{100, 200, 300};
    for (auto kind : {Kind::REAL, Kind::VIRTUAL, Kind::PROF}) {
        ASSERT_TRUE(state.configure(kind, {10, 7}, now) == ConfigureResult::APPLIED);
    }
    uint64_t next = 0;
    ASSERT_TRUE(state.next_deadline(Kind::REAL, next) && next == 110);
    ASSERT_TRUE(state.collect({110, 209, 309}, 0b110).count == 0);
    ASSERT_TRUE(state.next_deadline(Kind::REAL, next) && next == 110);
    ASSERT_TRUE(state.collect({109, 209, 309}).count == 0);
    auto due = state.collect({110, 210, 310});
    ASSERT_TRUE(due.count == 3);
    uint64_t mask = 0;
    for (size_t i = 0; i < due.count; ++i) mask |= state.claim(due.tokens[i]);
    ASSERT_TRUE(mask == ((uint64_t{1} << 13) | (uint64_t{1} << 25) | (uint64_t{1} << 26)));
    Value previous;
    ASSERT_TRUE(state.configure(Kind::VIRTUAL, {0, 99}, {115, 212, 318}, &previous) == ConfigureResult::APPLIED);
    ASSERT_TRUE(previous.remaining_us == 5 && previous.interval_us == 7);
    Value current;
    ASSERT_TRUE(state.read(Kind::VIRTUAL, {115, 212, 318}, current));
    ASSERT_TRUE(current.remaining_us == 0 && current.interval_us == 99);
    ASSERT_TRUE(state.configure(static_cast<Kind>(3), {}, {}) == ConfigureResult::INVALID_KIND);
    ASSERT_TRUE(!state.read(static_cast<Kind>(3), {}, current));
}

TEST(IntervalTimerModel, GenerationAndOwnerLifecycle) {
    ProcessTimers state, other;
    ASSERT_TRUE(state.configure(Kind::REAL, {10, 10}, {}) == ConfigureResult::APPLIED);
    auto first = state.collect({10, 0, 0});
    ASSERT_TRUE(first.count == 1);
    // Missed periods coalesce but deadline stays phase aligned.
    ASSERT_TRUE(state.collect({105, 0, 0}).count == 0);
    Value current;
    ASSERT_TRUE(state.read(Kind::REAL, {105, 0, 0}, current) && current.remaining_us == 5);
    ASSERT_TRUE(other.claim(first.tokens[0]) == 0);
    ASSERT_TRUE(state.claim(first.tokens[0]) == ProcessTimers::signal_mask(Kind::REAL));
    ASSERT_TRUE(state.claim(first.tokens[0]) == 0);
    auto second = state.collect({110, 0, 0});
    ASSERT_TRUE(second.count == 1);
    ASSERT_TRUE(state.claim(first.tokens[0]) == 0);  // Same configuration, later expiry: no ABA.
    ASSERT_TRUE(state.configure(Kind::REAL, {10, 0}, {110, 0, 0}) == ConfigureResult::APPLIED);
    ASSERT_TRUE(state.claim(second.tokens[0]) == 0);
    auto third = state.collect({120, 0, 0});
    ASSERT_TRUE(third.count == 1);
    ASSERT_TRUE(state.cancel(Kind::REAL));
    ASSERT_TRUE(state.claim(third.tokens[0]) == 0);
    ASSERT_TRUE(state.configure(Kind::PROF, {1, 1}, {}) == ConfigureResult::APPLIED);
    auto fourth = state.collect({0, 0, 1});
    state.close();
    state.close();
    ASSERT_TRUE(state.closed());
    ASSERT_TRUE(state.claim(fourth.tokens[0]) == 0);
    ASSERT_TRUE(state.collect({UINT64_MAX, UINT64_MAX, UINT64_MAX}).count == 0);
    ASSERT_TRUE(state.configure(Kind::REAL, {1, 1}, {}) == ConfigureResult::CLOSED);
}

TEST(IntervalTimerModel, SaturationAndGenerationExhaustion) {
    ProcessTimers state;
    ASSERT_TRUE(state.configure(Kind::REAL, {10, 10}, {UINT64_MAX - 5, 0, 0}) == ConfigureResult::APPLIED);
    ASSERT_TRUE(state.collect({UINT64_MAX - 1, 0, 0}).count == 0);
    auto due = state.collect({UINT64_MAX, 0, 0});
    ASSERT_TRUE(due.count == 1);
    ASSERT_TRUE(state.claim(due.tokens[0]) != 0);
    ASSERT_TRUE(state.collect({UINT64_MAX, 0, 0}).count == 0);
    Value value;
    ASSERT_TRUE(state.read(Kind::REAL, {UINT64_MAX, 0, 0}, value) && value.remaining_us == 0);
    ProcessTimers exhausted(UINT64_MAX - 1);
    ASSERT_TRUE(exhausted.configure(Kind::REAL, {1, 1}, {}) == ConfigureResult::APPLIED);
    Value untouched{123, 456};
    ASSERT_TRUE(exhausted.configure(Kind::REAL, {2, 2}, {}, &untouched) == ConfigureResult::GENERATION_EXHAUSTED);
    ASSERT_TRUE(untouched.remaining_us == 123 && untouched.interval_us == 456);
    auto token = exhausted.collect({1, 0, 0});
    ASSERT_TRUE(exhausted.cancel(Kind::REAL));
    ASSERT_TRUE(exhausted.claim(token.tokens[0]) == 0);
    ASSERT_TRUE(exhausted.configure(Kind::REAL, {2, 2}, {}) == ConfigureResult::GENERATION_EXHAUSTED);
    ASSERT_TRUE(exhausted.configure(Kind::REAL, {0, 99}, {}) == ConfigureResult::APPLIED);
    ASSERT_TRUE(exhausted.read(Kind::REAL, {}, value) && value.remaining_us == 0 && value.interval_us == 99);
    ProcessTimers sequence_exhausted(0, UINT64_MAX - 1);
    ASSERT_TRUE(sequence_exhausted.configure(Kind::REAL, {1, 1}, {}) == ConfigureResult::APPLIED);
    auto last = sequence_exhausted.collect({1, 0, 0});
    ASSERT_TRUE(last.count == 1 && sequence_exhausted.claim(last.tokens[0]) != 0);
    ASSERT_TRUE(sequence_exhausted.collect({2, 0, 0}).count == 0);
    ASSERT_TRUE(sequence_exhausted.configure(Kind::REAL, {1, 1}, {}) == ConfigureResult::GENERATION_EXHAUSTED);
}

TEST(IntervalTimerModel, ExhaustivePeriodicPhase) {
    for (uint64_t start = 0; start < 20; ++start) {
        for (uint64_t initial = 1; initial < 20; ++initial) {
            for (uint64_t interval = 1; interval < 20; ++interval) {
                ProcessTimers state;
                ASSERT_TRUE(state.configure(Kind::VIRTUAL, {initial, interval}, {0, start, 0}) == ConfigureResult::APPLIED);
                uint64_t deadline = start + initial;
                for (uint64_t now = start; now < 200; now += 7) {
                    bool const expected_due = now >= deadline;
                    while (deadline <= now) deadline += interval;
                    auto result = state.collect({0, now, 0});
                    ASSERT_TRUE(result.count == static_cast<size_t>(expected_due));
                    if (expected_due) ASSERT_TRUE(state.claim(result.tokens[0]) == ProcessTimers::signal_mask(Kind::VIRTUAL));
                    Value current;
                    ASSERT_TRUE(state.read(Kind::VIRTUAL, {0, now, 0}, current));
                    ASSERT_TRUE(current.remaining_us == deadline - now);
                }
            }
        }
    }
}
