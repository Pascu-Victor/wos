#include <gtest/gtest.h>

#include <barrier>
#include <deque>
#include <mutex>
#include <net/wki/job_state.hpp>
#include <set>
#include <thread>
#include <tuple>
#include <vector>

namespace jobs = ker::net::wki::jobs;
namespace {
constexpr jobs::Key KEY{
    .scope = {.owner = {.node = 7, .uid = 1000}, .submitter_incarnation = 11},
    .serial = 1,
    .receiver_incarnation = 13,
    .attempt = 1,
};
constexpr jobs::Digest DIGEST{19};
constexpr jobs::RetentionPolicy POLICY{.byte_limit = 1024, .proposal_ttl_us = 100, .result_ttl_us = 200};
auto serial(uint64_t value) -> jobs::Key {
    auto key = KEY;
    key.serial = value;
    return key;
}
}  // namespace

TEST(WkiJobState, InvalidAndRestartedIdentitiesNeverReserveStorage) {
    jobs::Registry<1, 1> registry(13, POLICY);
    for (unsigned field = 0; field < 7; ++field) {
        auto key = KEY;
        if (field == 0) key.scope.owner.node = 0;
        if (field == 1) key.scope.owner.node = UINT16_MAX;
        if (field == 2) key.scope.owner.uid = UINT32_MAX;
        if (field == 3) key.scope.submitter_incarnation = 0;
        if (field == 4) key.serial = 0;
        if (field == 5) key.receiver_incarnation = 0;
        if (field == 6) key.attempt = 0;
        EXPECT_EQ(registry.propose(key, DIGEST, 1, 0).admission, jobs::Admission::INVALID);
    }
    auto stale = KEY;
    ++stale.receiver_incarnation;
    EXPECT_EQ(registry.propose(stale, DIGEST, 1, 0).admission, jobs::Admission::LOST);
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::NEW);
    jobs::Registry<1, 1> uninitialized(0, POLICY);
    EXPECT_EQ(uninitialized.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::INVALID);
}

TEST(WkiJobState, ExactReplayCannotReplaceDigestAttemptOrCompletedState) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const first = registry.propose(KEY, DIGEST, 1, 0);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    auto* life = registry.lifecycle(first.handle);
    ASSERT_NE(life, nullptr);
    ASSERT_TRUE(life->accept());
    ASSERT_TRUE(life->commit());
    ASSERT_TRUE(life->start());
    ASSERT_TRUE(life->claim_publication());
    ASSERT_TRUE(life->publication_result(true));
    ASSERT_TRUE(life->complete());
    auto different = DIGEST;
    ++different.back();
    EXPECT_EQ(registry.propose(KEY, different, 1, 0).admission, jobs::Admission::CONFLICT);
    auto attempt = KEY;
    ++attempt.attempt;
    EXPECT_EQ(registry.propose(attempt, DIGEST, 1, 0).admission, jobs::Admission::CONFLICT);
    auto const replay = registry.propose(KEY, DIGEST, 1, 0);
    EXPECT_EQ(replay.admission, jobs::Admission::REPLAY);
    EXPECT_EQ(replay.handle, first.handle);
    EXPECT_EQ(registry.lifecycle(replay.handle)->phase(), jobs::Phase::TERMINAL);
    EXPECT_FALSE(life->claim_publication());
    EXPECT_FALSE(life->commit());
}

TEST(WkiJobState, DifferentStableOwnersCannotLookUpAnotherOwnersJob) {
    jobs::Registry<3, 3> registry(13, POLICY);
    ASSERT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::NEW);
    for (unsigned field = 0; field < 3; ++field) {
        auto other = KEY;
        if (field == 0) ++other.scope.owner.node;
        if (field == 1) ++other.scope.owner.uid;
        if (field == 2) ++other.scope.submitter_incarnation;
        EXPECT_EQ(registry.lookup(other).admission, jobs::Admission::UNKNOWN);
    }
}

TEST(WkiJobState, LostAcceptanceAndCommitRepliesCanBeRecoveredWithoutAnotherExecution) {
    jobs::Registry<1, 2> registry(13, POLICY);
    auto const proposed = registry.propose(KEY, DIGEST, 1, 0);
    ASSERT_EQ(proposed.admission, jobs::Admission::NEW);
    auto* life = registry.lifecycle(proposed.handle);
    ASSERT_TRUE(life->accept());
    life->disconnect();
    auto const status = registry.lookup(KEY);
    ASSERT_EQ(status.admission, jobs::Admission::REPLAY);
    EXPECT_EQ(registry.lifecycle(status.handle)->phase(), jobs::Phase::ACCEPTED);
    ASSERT_TRUE(life->commit());
    life->reattach();
    EXPECT_FALSE(life->commit());
    EXPECT_EQ(life->phase(), jobs::Phase::COMMITTED);
    ASSERT_TRUE(life->start());
    ASSERT_TRUE(life->claim_publication());
    ASSERT_TRUE(life->publication_result(true));
    life->disconnect();
    ASSERT_TRUE(life->complete());
    life->reattach();
    EXPECT_EQ(registry.lookup(KEY).handle, proposed.handle);
    EXPECT_EQ(life->phase(), jobs::Phase::TERMINAL);
    EXPECT_FALSE(life->start());
    EXPECT_FALSE(life->claim_publication());
}

TEST(WkiJobState, HigherSerialDoesNotRetireTheOlderLiveRecord) {
    jobs::Registry<1, 2> registry(13, POLICY);
    auto const older = registry.propose(KEY, DIGEST, 1, 0);
    ASSERT_TRUE(registry.lifecycle(older.handle)->accept());
    auto const newer = registry.propose(serial(10), DIGEST, 1, 0);
    ASSERT_EQ(newer.admission, jobs::Admission::NEW);
    ASSERT_TRUE(registry.expire(newer.handle));
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).handle, older.handle);
    EXPECT_EQ(registry.lifecycle(older.handle)->phase(), jobs::Phase::ACCEPTED);
    EXPECT_EQ(registry.propose(serial(9), DIGEST, 1, 0).admission, jobs::Admission::EXPIRED);
    ASSERT_TRUE(registry.expire(older.handle));
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::EXPIRED);
}

TEST(WkiJobState, CancellationBeforeConstructionDefeatsLateAcceptanceAndCommit) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const proposed = registry.propose(KEY, DIGEST, 1, 0);
    auto* life = registry.lifecycle(proposed.handle);
    ASSERT_TRUE(life->cancel());
    EXPECT_FALSE(life->accept());
    EXPECT_FALSE(life->commit());
    EXPECT_FALSE(life->start());
    EXPECT_FALSE(life->claim_publication());
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).handle, proposed.handle);
    EXPECT_EQ(life->phase(), jobs::Phase::CANCELED);
}

TEST(WkiJobState, ExpiredStorageKeepsReplayBarrierAndRejectsLateSlotCallbacks) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const first = registry.propose(KEY, DIGEST, 1, 0);
    ASSERT_TRUE(registry.retain(first.handle));
    EXPECT_FALSE(registry.expire(first.handle));
    ASSERT_TRUE(registry.release(first.handle));
    EXPECT_FALSE(registry.release(first.handle));
    auto* life = registry.lifecycle(first.handle);
    ASSERT_TRUE(life->accept());
    ASSERT_TRUE(life->commit());
    EXPECT_FALSE(registry.expire(first.handle));
    ASSERT_TRUE(life->start());
    EXPECT_FALSE(registry.expire(first.handle));
    ASSERT_TRUE(life->claim_publication());
    ASSERT_TRUE(life->publication_result(true));
    EXPECT_FALSE(registry.expire(first.handle));
    ASSERT_TRUE(life->complete());
    ASSERT_TRUE(registry.expire(first.handle));
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::EXPIRED);
    auto const successor = registry.propose(serial(2), DIGEST, 1, 0);
    ASSERT_EQ(successor.admission, jobs::Admission::NEW);
    EXPECT_EQ(successor.handle.index, first.handle.index);
    EXPECT_NE(successor.handle.generation, first.handle.generation);
    EXPECT_EQ(registry.lifecycle(first.handle), nullptr);
    EXPECT_FALSE(registry.retain(first.handle));
    EXPECT_FALSE(registry.expire(first.handle));
    EXPECT_EQ(registry.lifecycle(successor.handle)->phase(), jobs::Phase::PROPOSED);
}

TEST(WkiJobState, CapacityRefusalConsumesNoSerialOrScopeAndNeverEvictsWatermarks) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const first = registry.propose(KEY, DIGEST, 1, 0);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.propose(serial(10), DIGEST, 1, 0).admission, jobs::Admission::CAPACITY);
    ASSERT_TRUE(registry.expire(first.handle));
    auto other = KEY;
    ++other.scope.submitter_incarnation;
    EXPECT_EQ(registry.propose(other, DIGEST, 1, 0).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.lookup(other).admission, jobs::Admission::UNKNOWN);
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::EXPIRED);
    EXPECT_EQ(registry.propose(serial(2), DIGEST, 1, 0).admission, jobs::Admission::NEW);
}

TEST(WkiJobState, UnseenLowerSerialAndExhaustedGenerationsFailClosed) {
    jobs::Registry<1, 1> registry(13, POLICY, UINT64_MAX);
    auto const last = registry.propose(serial(10), DIGEST, 1, 0);
    ASSERT_EQ(last.admission, jobs::Admission::NEW);
    EXPECT_EQ(last.handle.generation, UINT64_MAX);
    EXPECT_EQ(registry.propose(serial(9), DIGEST, 1, 0).admission, jobs::Admission::EXPIRED);
    EXPECT_EQ(registry.propose(serial(10), DIGEST, 1, 0).admission, jobs::Admission::REPLAY);
    ASSERT_TRUE(registry.expire(last.handle));
    EXPECT_EQ(registry.propose(serial(11), DIGEST, 1, 0).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.lifecycle({0, 0}), nullptr);
    EXPECT_EQ(registry.lifecycle({1, UINT64_MAX}), nullptr);
    jobs::Registry<1, 1> successor(14, POLICY);
    EXPECT_EQ(successor.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::LOST);
    auto new_job = KEY;
    new_job.receiver_incarnation = 14;
    EXPECT_EQ(successor.propose(new_job, DIGEST, 1, 0).admission, jobs::Admission::NEW);
}

TEST(WkiJobState, CancellationAndNativeCompletionHaveOneRaceDefinedOutcome) {
    for (bool after_claim : {false, true}) {
        jobs::Lifecycle life;
        ASSERT_TRUE(life.accept());
        ASSERT_TRUE(life.commit());
        ASSERT_TRUE(life.start());
        if (after_claim) ASSERT_TRUE(life.claim_publication());
        ASSERT_TRUE(life.cancel());
        EXPECT_FALSE(life.cancel());
        EXPECT_EQ(life.phase(), jobs::Phase::STARTING);
        if (after_claim) {
            ASSERT_TRUE(life.publication_result(true));
            EXPECT_EQ(life.phase(), jobs::Phase::RUNNING);
            ASSERT_TRUE(life.complete());
        } else {
            EXPECT_FALSE(life.claim_publication());
            ASSERT_TRUE(life.abort_start());
        }
        EXPECT_EQ(life.phase(), jobs::Phase::CANCELED);
        EXPECT_FALSE(life.complete());
    }
    jobs::Lifecycle completed;
    ASSERT_TRUE(completed.accept());
    ASSERT_TRUE(completed.commit());
    ASSERT_TRUE(completed.start());
    ASSERT_TRUE(completed.claim_publication());
    ASSERT_TRUE(completed.publication_result(true));
    ASSERT_TRUE(completed.complete());
    EXPECT_FALSE(completed.cancel());
    EXPECT_EQ(completed.phase(), jobs::Phase::TERMINAL);
}

TEST(WkiJobState, EveryReachableEventTracePreservesSinglePublicationAndTerminalState) {
    struct Trace {
        jobs::Lifecycle life;
        unsigned publications{};
        unsigned cancellations{};
    };
    std::deque<Trace> pending(1);
    std::set<std::tuple<jobs::Phase, bool, bool, bool, bool, unsigned, unsigned>> visited;
    while (!pending.empty()) {
        auto const before = pending.front();
        pending.pop_front();
        auto const key =
            std::tuple(before.life.phase(), before.life.detached(), before.life.cancellation_requested(),
                       before.life.cancellation_claimed(), before.life.publication_claimed(), before.publications, before.cancellations);
        if (!visited.insert(key).second) continue;
        for (unsigned event = 0; event < 12; ++event) {
            auto next = before;
            bool changed = false;
            if (event == 0) changed = next.life.accept();
            if (event == 1) changed = next.life.commit();
            if (event == 2) changed = next.life.start();
            if (event == 3 && next.life.claim_publication()) ++next.publications;
            if (event == 4) changed = next.life.publication_result(true);
            if (event == 5) changed = next.life.publication_result(false);
            if (event == 6) changed = next.life.abort_start();
            if (event == 7) changed = next.life.cancel();
            if (event == 8) changed = next.life.complete();
            if (event == 9) changed = next.life.expire();
            if (event == 10) {
                if (next.life.detached())
                    next.life.reattach();
                else
                    next.life.disconnect();
            }
            if (event == 11 && next.life.claim_cancellation()) ++next.cancellations;
            ASSERT_LE(next.publications, 1U);
            ASSERT_LE(next.cancellations, 1U);
            if (before.life.terminal()) {
                EXPECT_TRUE(next.life.terminal());
                EXPECT_TRUE(next.life.phase() == before.life.phase() || next.life.phase() == jobs::Phase::EXPIRED);
            }
            if (event == 10) EXPECT_EQ(next.life.phase(), before.life.phase());
            if (!changed && event != 3 && event != 10 && event != 11) EXPECT_EQ(next.life.phase(), before.life.phase());
            pending.push_back(next);
        }
    }
    EXPECT_GT(visited.size(), 20U);
    EXPECT_LT(visited.size(), 128U);
}

TEST(WkiJobState, DeferredCancelRequestSurvivesPressureAndHasOnePublishedTaskClaim) {
    jobs::Lifecycle life;
    ASSERT_TRUE(life.accept());
    ASSERT_TRUE(life.commit());
    ASSERT_TRUE(life.start());
    ASSERT_TRUE(life.claim_publication());
    ASSERT_TRUE(life.cancel());
    EXPECT_FALSE(life.claim_cancellation());
    ASSERT_TRUE(life.publication_result(true));
    // A full deferred queue leaves the request recorded without claiming it.
    EXPECT_TRUE(life.cancellation_requested());
    EXPECT_FALSE(life.cancellation_claimed());
    life.disconnect();
    life.reattach();
    EXPECT_FALSE(life.cancel());
    ASSERT_TRUE(life.claim_cancellation());
    EXPECT_FALSE(life.claim_cancellation());
    ASSERT_TRUE(life.complete());
    EXPECT_EQ(life.phase(), jobs::Phase::CANCELED);
    EXPECT_FALSE(life.claim_cancellation());
}

TEST(WkiJobState, ConcurrentDuplicatesAndCancelPublicationRacesBalanceOwnedHolders) {
    for (unsigned round = 0; round < 128; ++round) {
        jobs::Registry<1, 1> registry(13, POLICY);
        std::mutex lock;
        jobs::Handle handle;
        unsigned reservations = 0;
        std::vector<std::thread> duplicates;
        for (unsigned thread = 0; thread < 4; ++thread) {
            duplicates.emplace_back([&] {
                std::scoped_lock guard(lock);
                auto const result = registry.propose(KEY, DIGEST, 1, 0);
                if (result.admission == jobs::Admission::NEW) {
                    ++reservations;
                    handle = result.handle;
                } else {
                    EXPECT_EQ(result.admission, jobs::Admission::REPLAY);
                }
            });
        }
        for (auto& duplicate : duplicates) {
            duplicate.join();
        }
        ASSERT_EQ(reservations, 1U);
        auto* life = registry.lifecycle(handle);
        ASSERT_TRUE(life->accept());
        ASSERT_TRUE(life->commit());
        ASSERT_TRUE(life->start());
        ASSERT_TRUE(registry.retain(handle));
        std::barrier ready(3);
        unsigned publications = 0;
        bool canceled = false;
        std::thread publish([&] {
            ready.arrive_and_wait();
            std::scoped_lock guard(lock);
            if (life->claim_publication()) {
                ++publications;
                EXPECT_TRUE(life->publication_result(true));
                EXPECT_TRUE(life->complete());
            } else {
                EXPECT_TRUE(life->abort_start());
            }
            EXPECT_TRUE(registry.release(handle));
        });
        std::thread cancel([&] {
            ready.arrive_and_wait();
            std::scoped_lock guard(lock);
            canceled = life->cancel();
        });
        ready.arrive_and_wait();
        publish.join();
        cancel.join();
        EXPECT_LE(publications, 1U);
        EXPECT_EQ(life->phase(), canceled ? jobs::Phase::CANCELED : jobs::Phase::TERMINAL);
        EXPECT_TRUE(registry.expire(handle));
        EXPECT_EQ(registry.lookup(KEY).admission, jobs::Admission::EXPIRED);
    }
}

TEST(WkiJobState, MissingRetentionLimitsAndZeroReservationFailClosed) {
    for (unsigned field = 0; field < 3; ++field) {
        auto policy = POLICY;
        if (field == 0) policy.byte_limit = 0;
        if (field == 1) policy.proposal_ttl_us = 0;
        if (field == 2) policy.result_ttl_us = 0;
        jobs::Registry<1, 1> registry(13, policy);
        EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::INVALID);
        EXPECT_EQ(registry.records_in_use(), 0U);
        EXPECT_EQ(registry.scopes_in_use(), 0U);
        EXPECT_EQ(registry.reserved_bytes(), 0U);
    }
    jobs::Registry<1, 1> registry(13, POLICY);
    EXPECT_EQ(registry.propose(KEY, DIGEST, 0, 0).admission, jobs::Admission::INVALID);
    EXPECT_EQ(registry.propose(KEY, DIGEST, 1, 0).admission, jobs::Admission::NEW);
}

TEST(WkiJobState, ByteQuotaRefusalBurnsNoScopeOrSerialAndReplayDoesNotRecharge) {
    jobs::Registry<2, 3> registry(13, {.byte_limit = 8, .proposal_ttl_us = 100, .result_ttl_us = 200});
    auto const first = registry.propose(KEY, DIGEST, 5, 10);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.propose(serial(2), DIGEST, 4, 10).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.lookup(serial(2)).admission, jobs::Admission::UNKNOWN);
    auto other = KEY;
    ++other.scope.submitter_incarnation;
    EXPECT_EQ(registry.propose(other, DIGEST, 4, 10).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.propose(KEY, DIGEST, UINT64_MAX, UINT64_MAX).handle, first.handle);
    EXPECT_EQ(registry.reserved_bytes(), 5U);
    EXPECT_EQ(registry.scopes_in_use(), 1U);
    auto const second = registry.propose(serial(2), DIGEST, 3, 10);
    ASSERT_EQ(second.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.reserved_bytes(), 8U);
    ASSERT_TRUE(registry.expire(first.handle));
    EXPECT_EQ(registry.reserved_bytes(), 3U);
    EXPECT_EQ(registry.propose(other, DIGEST, 5, 10).admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.records_in_use(), 2U);
    EXPECT_EQ(registry.scopes_in_use(), 2U);
    EXPECT_EQ(registry.reserved_bytes(), 8U);
    EXPECT_EQ(registry.lookup(KEY).admission, jobs::Admission::EXPIRED);
}

TEST(WkiJobState, AcceptedProposalExpiresFromOriginalReservationDespiteReplay) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const proposed = registry.propose(KEY, DIGEST, 8, 10);
    ASSERT_TRUE(registry.lifecycle(proposed.handle)->accept());
    EXPECT_FALSE(registry.mark_terminal(proposed.handle, 10));
    EXPECT_FALSE(registry.expire_due(proposed.handle, 109));
    EXPECT_EQ(registry.propose(KEY, DIGEST, 8, 1000).handle, proposed.handle);
    EXPECT_TRUE(registry.expire_due(proposed.handle, 110));
    EXPECT_EQ(registry.reserved_bytes(), 0U);
    EXPECT_EQ(registry.propose(KEY, DIGEST, 8, 1000).admission, jobs::Admission::EXPIRED);
    EXPECT_EQ(registry.scopes_in_use(), 1U);
}

TEST(WkiJobState, CommittedStartingAndRunningWorkCannotExpireForQuotaPressure) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const proposed = registry.propose(KEY, DIGEST, 8, 0);
    auto* life = registry.lifecycle(proposed.handle);
    ASSERT_TRUE(life->accept());
    ASSERT_TRUE(life->commit());
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    ASSERT_TRUE(life->start());
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    ASSERT_TRUE(life->claim_publication());
    ASSERT_TRUE(life->publication_result(true));
    life->disconnect();
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    EXPECT_EQ(registry.reserved_bytes(), 8U);
    ASSERT_TRUE(life->complete());
    ASSERT_TRUE(registry.mark_terminal(proposed.handle, 100));
    life->reattach();
    EXPECT_FALSE(registry.mark_terminal(proposed.handle, 600));
    EXPECT_FALSE(registry.expire_due(proposed.handle, 299));
    EXPECT_TRUE(registry.expire_due(proposed.handle, 300));
    EXPECT_EQ(registry.reserved_bytes(), 0U);
}

TEST(WkiJobState, TerminalTtlNeedsFirstStampAndZeroHoldersIncludingTimeZero) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const proposed = registry.propose(KEY, DIGEST, 8, 0);
    auto* life = registry.lifecycle(proposed.handle);
    ASSERT_TRUE(life->cancel());
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    ASSERT_TRUE(registry.mark_terminal(proposed.handle, 0));
    ASSERT_TRUE(registry.retain(proposed.handle));
    EXPECT_FALSE(registry.expire_due(proposed.handle, 200));
    life->disconnect();
    EXPECT_EQ(registry.propose(KEY, DIGEST, 8, UINT64_MAX).handle, proposed.handle);
    life->reattach();
    EXPECT_FALSE(registry.mark_terminal(proposed.handle, UINT64_MAX));
    ASSERT_TRUE(registry.release(proposed.handle));
    EXPECT_TRUE(registry.expire_due(proposed.handle, 200));
    EXPECT_EQ(registry.records_in_use(), 0U);
    EXPECT_EQ(registry.reserved_bytes(), 0U);
}

TEST(WkiJobState, TimeRollbackAndDeadlineOverflowNeverExpireEarly) {
    jobs::Registry<1, 1> registry(13, {.byte_limit = 8, .proposal_ttl_us = 20, .result_ttl_us = 30});
    auto const proposed = registry.propose(KEY, DIGEST, 8, UINT64_MAX - 10);
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    EXPECT_FALSE(registry.expire_due(proposed.handle, 0));
    ASSERT_TRUE(registry.lifecycle(proposed.handle)->cancel());
    ASSERT_TRUE(registry.mark_terminal(proposed.handle, UINT64_MAX - 10));
    EXPECT_FALSE(registry.expire_due(proposed.handle, UINT64_MAX));
    EXPECT_FALSE(registry.expire_due(proposed.handle, 5));
    EXPECT_EQ(registry.reserved_bytes(), 8U);
    EXPECT_TRUE(registry.expire(proposed.handle));
}

TEST(WkiJobState, MaximumByteBudgetDoesNotWrapDuringAdmissionOrReclamation) {
    jobs::Registry<1, 3> registry(13, {.byte_limit = UINT64_MAX, .proposal_ttl_us = 100, .result_ttl_us = 200});
    auto const first = registry.propose(KEY, DIGEST, UINT64_MAX - 1, 0);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.propose(serial(2), DIGEST, 2, 0).admission, jobs::Admission::CAPACITY);
    auto const second = registry.propose(serial(2), DIGEST, 1, 0);
    ASSERT_EQ(second.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.reserved_bytes(), UINT64_MAX);
    EXPECT_EQ(registry.propose(serial(3), DIGEST, 1, 0).admission, jobs::Admission::CAPACITY);
    ASSERT_TRUE(registry.expire(first.handle));
    EXPECT_EQ(registry.reserved_bytes(), 1U);
    ASSERT_TRUE(registry.expire(second.handle));
    EXPECT_EQ(registry.reserved_bytes(), 0U);
}

TEST(WkiJobState, LateTimerSnapshotCannotRetireAReusedSlot) {
    jobs::Registry<1, 1> registry(13, POLICY);
    auto const first = registry.propose(KEY, DIGEST, 8, 0);
    EXPECT_EQ(registry.handle_at(0), first.handle);
    EXPECT_EQ(registry.handle_at(1).generation, 0U);
    ASSERT_TRUE(registry.expire(first.handle));
    EXPECT_EQ(registry.handle_at(0).generation, 0U);
    auto const second = registry.propose(serial(2), DIGEST, 8, 200);
    ASSERT_EQ(second.admission, jobs::Admission::NEW);
    EXPECT_FALSE(registry.mark_terminal(first.handle, 200));
    EXPECT_FALSE(registry.expire_due(first.handle, UINT64_MAX));
    EXPECT_EQ(registry.handle_at(0), second.handle);
    EXPECT_EQ(registry.lifecycle(second.handle)->phase(), jobs::Phase::PROPOSED);
    EXPECT_EQ(registry.reserved_bytes(), 8U);
}

TEST(WkiJobState, RepeatedAcceptedCompletionsReleaseAllChargesAndKeepReplayBarrier) {
    jobs::Registry<1, 2> registry(13, POLICY);
    jobs::Handle prior{};
    for (uint64_t ordinal = 1; ordinal <= 2500; ++ordinal) {
        auto const proposed = registry.propose(serial(ordinal), DIGEST, 8, ordinal);
        ASSERT_EQ(proposed.admission, jobs::Admission::NEW);
        auto* life = registry.lifecycle(proposed.handle);
        ASSERT_TRUE(life->accept());
        ASSERT_TRUE(life->commit());
        ASSERT_TRUE(life->start());
        ASSERT_TRUE(life->claim_publication());
        ASSERT_TRUE(life->publication_result(true));
        ASSERT_TRUE(registry.retain(proposed.handle));
        ASSERT_TRUE(life->complete());
        ASSERT_TRUE(registry.mark_terminal(proposed.handle, ordinal));
        EXPECT_FALSE(registry.expire_due(proposed.handle, ordinal + 200));
        ASSERT_TRUE(registry.release(proposed.handle));
        EXPECT_EQ(registry.lifecycle(prior), nullptr);
        ASSERT_TRUE(registry.expire_due(proposed.handle, ordinal + 200));
        EXPECT_EQ(registry.lookup(serial(ordinal)).admission, jobs::Admission::EXPIRED);
        EXPECT_EQ(registry.records_in_use(), 0U);
        EXPECT_EQ(registry.reserved_bytes(), 0U);
        EXPECT_EQ(registry.scopes_in_use(), 1U);
        prior = proposed.handle;
    }
}
