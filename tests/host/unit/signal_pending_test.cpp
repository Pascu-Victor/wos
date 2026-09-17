#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <platform/sched/signal_pending.hpp>
#include <thread>
#include <utility>

using ker::mod::sched::SignalPendingClaim;
constexpr uint64_t USR1 = uint64_t{1} << 9;

TEST(SignalPending, FailedFrameLeavesOccurrenceInOriginalPool) {
    std::atomic<uint64_t> pending{USR1}, claimed{0};
    {
        auto first = SignalPendingClaim::acquire(pending, claimed, USR1);
        ASSERT_TRUE(first);
        EXPECT_EQ(pending.load(), USR1);
        EXPECT_FALSE(SignalPendingClaim::acquire(pending, claimed, USR1));
    }
    auto retry = SignalPendingClaim::acquire(pending, claimed, USR1);
    ASSERT_TRUE(retry);
    EXPECT_TRUE(retry.commit());
    EXPECT_EQ(pending.load(), 0U);
    EXPECT_EQ(claimed.load(), 0U);
}

TEST(SignalPending, CancellationDuringPreparationCannotBeRestored) {
    constexpr uint64_t STOP = uint64_t{1} << 19;
    constexpr uint64_t CONT = uint64_t{1} << 17;
    std::atomic<uint64_t> pending{STOP}, claimed{0};
    {
        auto stop = SignalPendingClaim::acquire(pending, claimed, STOP);
        ASSERT_TRUE(stop);
        pending.store(CONT);
    }
    EXPECT_EQ(pending.load(), CONT);
    auto continued = SignalPendingClaim::acquire(pending, claimed, CONT);
    ASSERT_TRUE(continued);
    pending.store(STOP);
    EXPECT_FALSE(continued.commit());
    EXPECT_EQ(pending.load(), STOP);
    EXPECT_EQ(claimed.load(), 0U);
}

TEST(SignalPending, NewOccurrenceAfterCommitSurvivesOldClaimDestruction) {
    std::atomic<uint64_t> pending{USR1}, claimed{0};
    {
        auto first = SignalPendingClaim::acquire(pending, claimed, USR1);
        ASSERT_TRUE(first.commit());
        pending.fetch_or(USR1);
    }
    auto next = SignalPendingClaim::acquire(pending, claimed, USR1);
    EXPECT_TRUE(next.commit());
}

TEST(SignalPending, MovingClaimDoesNotReleaseAnotherConsumersReservation) {
    std::atomic<uint64_t> pending{USR1}, claimed{0};
    auto first = SignalPendingClaim::acquire(pending, claimed, USR1);
    auto moved = std::move(first);
    EXPECT_FALSE(first);
    EXPECT_TRUE(moved);
    EXPECT_FALSE(SignalPendingClaim::acquire(pending, claimed, USR1));
    EXPECT_TRUE(moved.commit());
}

TEST(SignalPending, InvalidAndEmptyClaimsDoNotReserveBits) {
    std::atomic<uint64_t> pending{USR1}, claimed{0};
    EXPECT_FALSE(SignalPendingClaim::acquire(pending, claimed, 0));
    EXPECT_FALSE(SignalPendingClaim::acquire(pending, claimed, USR1 | 1));
    EXPECT_FALSE(SignalPendingClaim::acquire(pending, claimed, 1));
    EXPECT_EQ(claimed.load(), 0U);
    EXPECT_EQ(pending.load(), USR1);
}

TEST(SignalPending, CompetingRecipientsCommitEachRoundExactlyOnce) {
    constexpr int WORKERS = 8;
    constexpr int ROUNDS = 2000;
    std::atomic<uint64_t> pending{0}, claimed{0};
    std::atomic<int> deliveries{0};
    std::barrier phase{WORKERS + 1};
    std::array<std::thread, WORKERS> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            for (int round = 0; round < ROUNDS; ++round) {
                phase.arrive_and_wait();
                auto claim = SignalPendingClaim::acquire(pending, claimed, USR1);
                if (claim.commit()) deliveries.fetch_add(1);
                phase.arrive_and_wait();
            }
        });
    }
    for (int round = 0; round < ROUNDS; ++round) {
        pending.store(USR1);
        phase.arrive_and_wait();
        phase.arrive_and_wait();
        EXPECT_EQ(deliveries.load(), round + 1);
        EXPECT_EQ(pending.load(), 0U);
        EXPECT_EQ(claimed.load(), 0U);
    }
    for (auto& worker : workers) worker.join();
}

TEST(SignalPending, ProvenanceIncludesPoolAndSignalUntilCommitOrMove) {
    std::atomic<uint64_t> first{USR1}, second{USR1}, claimed{0};
    auto claim = SignalPendingClaim::acquire(first, claimed, USR1);
    EXPECT_TRUE(claim.is_for(first, USR1));
    EXPECT_FALSE(claim.is_for(second, USR1));
    EXPECT_FALSE(claim.is_for(first, 1));
    auto moved = std::move(claim);
    EXPECT_FALSE(claim.is_for(first, USR1));
    EXPECT_TRUE(moved.is_for(first, USR1));
    EXPECT_TRUE(moved.commit());
    EXPECT_FALSE(moved.is_for(first, USR1));
    EXPECT_EQ(second.load(), USR1);
    EXPECT_EQ(claimed.load(), 0U);
}
