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
auto serial(uint64_t value) -> jobs::Key {
    auto key = KEY;
    key.serial = value;
    return key;
}
}  // namespace

TEST(WkiJobState, InvalidAndRestartedIdentitiesNeverReserveStorage) {
    jobs::Registry<1, 1> registry(13);
    for (unsigned field = 0; field < 7; ++field) {
        auto key = KEY;
        if (field == 0) key.scope.owner.node = 0;
        if (field == 1) key.scope.owner.node = UINT16_MAX;
        if (field == 2) key.scope.owner.uid = UINT32_MAX;
        if (field == 3) key.scope.submitter_incarnation = 0;
        if (field == 4) key.serial = 0;
        if (field == 5) key.receiver_incarnation = 0;
        if (field == 6) key.attempt = 0;
        EXPECT_EQ(registry.propose(key, DIGEST).admission, jobs::Admission::INVALID);
    }
    auto stale = KEY;
    ++stale.receiver_incarnation;
    EXPECT_EQ(registry.propose(stale, DIGEST).admission, jobs::Admission::LOST);
    EXPECT_EQ(registry.propose(KEY, DIGEST).admission, jobs::Admission::NEW);
    jobs::Registry<1, 1> uninitialized(0);
    EXPECT_EQ(uninitialized.propose(KEY, DIGEST).admission, jobs::Admission::INVALID);
}

TEST(WkiJobState, ExactReplayCannotReplaceDigestAttemptOrCompletedState) {
    jobs::Registry<1, 1> registry(13);
    auto const first = registry.propose(KEY, DIGEST);
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
    EXPECT_EQ(registry.propose(KEY, different).admission, jobs::Admission::CONFLICT);
    auto attempt = KEY;
    ++attempt.attempt;
    EXPECT_EQ(registry.propose(attempt, DIGEST).admission, jobs::Admission::CONFLICT);
    auto const replay = registry.propose(KEY, DIGEST);
    EXPECT_EQ(replay.admission, jobs::Admission::REPLAY);
    EXPECT_EQ(replay.handle, first.handle);
    EXPECT_EQ(registry.lifecycle(replay.handle)->phase(), jobs::Phase::TERMINAL);
    EXPECT_FALSE(life->claim_publication());
    EXPECT_FALSE(life->commit());
}

TEST(WkiJobState, DifferentStableOwnersCannotLookUpAnotherOwnersJob) {
    jobs::Registry<3, 3> registry(13);
    ASSERT_EQ(registry.propose(KEY, DIGEST).admission, jobs::Admission::NEW);
    for (unsigned field = 0; field < 3; ++field) {
        auto other = KEY;
        if (field == 0) ++other.scope.owner.node;
        if (field == 1) ++other.scope.owner.uid;
        if (field == 2) ++other.scope.submitter_incarnation;
        EXPECT_EQ(registry.lookup(other).admission, jobs::Admission::UNKNOWN);
    }
}

TEST(WkiJobState, LostAcceptanceAndCommitRepliesCanBeRecoveredWithoutAnotherExecution) {
    jobs::Registry<1, 2> registry(13);
    auto const proposed = registry.propose(KEY, DIGEST);
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
    jobs::Registry<1, 2> registry(13);
    auto const older = registry.propose(KEY, DIGEST);
    ASSERT_TRUE(registry.lifecycle(older.handle)->accept());
    auto const newer = registry.propose(serial(10), DIGEST);
    ASSERT_EQ(newer.admission, jobs::Admission::NEW);
    ASSERT_TRUE(registry.expire(newer.handle));
    EXPECT_EQ(registry.propose(KEY, DIGEST).handle, older.handle);
    EXPECT_EQ(registry.lifecycle(older.handle)->phase(), jobs::Phase::ACCEPTED);
    EXPECT_EQ(registry.propose(serial(9), DIGEST).admission, jobs::Admission::EXPIRED);
    ASSERT_TRUE(registry.expire(older.handle));
    EXPECT_EQ(registry.propose(KEY, DIGEST).admission, jobs::Admission::EXPIRED);
}

TEST(WkiJobState, CancellationBeforeConstructionDefeatsLateAcceptanceAndCommit) {
    jobs::Registry<1, 1> registry(13);
    auto const proposed = registry.propose(KEY, DIGEST);
    auto* life = registry.lifecycle(proposed.handle);
    ASSERT_TRUE(life->cancel());
    EXPECT_FALSE(life->accept());
    EXPECT_FALSE(life->commit());
    EXPECT_FALSE(life->start());
    EXPECT_FALSE(life->claim_publication());
    EXPECT_EQ(registry.propose(KEY, DIGEST).handle, proposed.handle);
    EXPECT_EQ(life->phase(), jobs::Phase::CANCELED);
}

TEST(WkiJobState, ExpiredStorageKeepsReplayBarrierAndRejectsLateSlotCallbacks) {
    jobs::Registry<1, 1> registry(13);
    auto const first = registry.propose(KEY, DIGEST);
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
    EXPECT_EQ(registry.propose(KEY, DIGEST).admission, jobs::Admission::EXPIRED);
    auto const successor = registry.propose(serial(2), DIGEST);
    ASSERT_EQ(successor.admission, jobs::Admission::NEW);
    EXPECT_EQ(successor.handle.index, first.handle.index);
    EXPECT_NE(successor.handle.generation, first.handle.generation);
    EXPECT_EQ(registry.lifecycle(first.handle), nullptr);
    EXPECT_FALSE(registry.retain(first.handle));
    EXPECT_FALSE(registry.expire(first.handle));
    EXPECT_EQ(registry.lifecycle(successor.handle)->phase(), jobs::Phase::PROPOSED);
}

TEST(WkiJobState, CapacityRefusalConsumesNoSerialOrScopeAndNeverEvictsWatermarks) {
    jobs::Registry<1, 1> registry(13);
    auto const first = registry.propose(KEY, DIGEST);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    EXPECT_EQ(registry.propose(serial(10), DIGEST).admission, jobs::Admission::CAPACITY);
    ASSERT_TRUE(registry.expire(first.handle));
    auto other = KEY;
    ++other.scope.submitter_incarnation;
    EXPECT_EQ(registry.propose(other, DIGEST).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.lookup(other).admission, jobs::Admission::UNKNOWN);
    EXPECT_EQ(registry.propose(KEY, DIGEST).admission, jobs::Admission::EXPIRED);
    EXPECT_EQ(registry.propose(serial(2), DIGEST).admission, jobs::Admission::NEW);
}

TEST(WkiJobState, UnseenLowerSerialAndExhaustedGenerationsFailClosed) {
    jobs::Registry<1, 1> registry(13, UINT64_MAX);
    auto const last = registry.propose(serial(10), DIGEST);
    ASSERT_EQ(last.admission, jobs::Admission::NEW);
    EXPECT_EQ(last.handle.generation, UINT64_MAX);
    EXPECT_EQ(registry.propose(serial(9), DIGEST).admission, jobs::Admission::EXPIRED);
    EXPECT_EQ(registry.propose(serial(10), DIGEST).admission, jobs::Admission::REPLAY);
    ASSERT_TRUE(registry.expire(last.handle));
    EXPECT_EQ(registry.propose(serial(11), DIGEST).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(registry.lifecycle({0, 0}), nullptr);
    EXPECT_EQ(registry.lifecycle({1, UINT64_MAX}), nullptr);
    jobs::Registry<1, 1> successor(14);
    EXPECT_EQ(successor.propose(KEY, DIGEST).admission, jobs::Admission::LOST);
    auto new_job = KEY;
    new_job.receiver_incarnation = 14;
    EXPECT_EQ(successor.propose(new_job, DIGEST).admission, jobs::Admission::NEW);
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
    };
    std::deque<Trace> pending(1);
    std::set<std::tuple<jobs::Phase, bool, bool, bool, unsigned>> visited;
    while (!pending.empty()) {
        auto const before = pending.front();
        pending.pop_front();
        auto const key = std::tuple(before.life.phase(), before.life.detached(), before.life.cancellation_requested(),
                                    before.life.publication_claimed(), before.publications);
        if (!visited.insert(key).second) continue;
        for (unsigned event = 0; event < 11; ++event) {
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
            ASSERT_LE(next.publications, 1U);
            if (before.life.terminal()) {
                EXPECT_TRUE(next.life.terminal());
                EXPECT_TRUE(next.life.phase() == before.life.phase() || next.life.phase() == jobs::Phase::EXPIRED);
            }
            if (event == 10) EXPECT_EQ(next.life.phase(), before.life.phase());
            if (!changed && event != 3 && event != 10) EXPECT_EQ(next.life.phase(), before.life.phase());
            pending.push_back(next);
        }
    }
    EXPECT_GT(visited.size(), 20U);
    EXPECT_LT(visited.size(), 80U);
}

TEST(WkiJobState, ConcurrentDuplicatesAndCancelPublicationRacesBalanceOwnedHolders) {
    for (unsigned round = 0; round < 128; ++round) {
        jobs::Registry<1, 1> registry(13);
        std::mutex lock;
        jobs::Handle handle;
        unsigned reservations = 0;
        std::vector<std::thread> duplicates;
        for (unsigned thread = 0; thread < 4; ++thread) {
            duplicates.emplace_back([&] {
                std::scoped_lock guard(lock);
                auto const result = registry.propose(KEY, DIGEST);
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
