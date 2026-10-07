#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <cerrno>
#include <mutex>
#include <net/wki/job_control.hpp>
#include <thread>
#include <type_traits>
#include <vector>

namespace jobs = ker::net::wki::jobs;
static_assert(!std::is_same_v<jobs::Handle, jobs::SignalHandle>);

TEST(WkiJobControl, InvalidSerialSignumOrSignalPolicyNeverConsumesCapacity) {
    jobs::SignalReceipts<1> receipts(64);
    EXPECT_EQ(receipts.propose(0, 1).admission, jobs::Admission::INVALID);
    EXPECT_EQ(receipts.propose(1, 0).admission, jobs::Admission::INVALID);
    EXPECT_EQ(receipts.propose(1, 65).admission, jobs::Admission::INVALID);
    EXPECT_EQ(receipts.propose(1, 64).admission, jobs::Admission::NEW);
    jobs::SignalReceipts<1> uninitialized(0);
    EXPECT_EQ(uninitialized.propose(1, 1).admission, jobs::Admission::INVALID);
}

TEST(WkiJobControl, LostClaimAndResultRepliesReplayWithoutAnotherEffect) {
    jobs::SignalReceipts<1> receipts(64);
    auto const first = receipts.propose(1, 15);
    ASSERT_EQ(first.admission, jobs::Admission::NEW);
    EXPECT_FALSE(receipts.complete(first.handle, 0));
    ASSERT_TRUE(receipts.claim(first.handle));
    auto const duplicate = receipts.propose(1, 15);
    EXPECT_EQ(duplicate.admission, jobs::Admission::REPLAY);
    EXPECT_EQ(duplicate.handle, first.handle);
    EXPECT_FALSE(receipts.claim(duplicate.handle));
    jobs::SignalView view;
    ASSERT_TRUE(receipts.inspect(first.handle, view));
    EXPECT_EQ(view.phase, jobs::SignalPhase::CLAIMED);
    ASSERT_TRUE(receipts.complete(first.handle, -ESRCH));
    EXPECT_FALSE(receipts.complete(first.handle, 0));
    EXPECT_EQ(receipts.propose(1, 9).admission, jobs::Admission::CONFLICT);
    EXPECT_EQ(receipts.propose(1, 15).admission, jobs::Admission::REPLAY);
    ASSERT_TRUE(receipts.inspect(first.handle, view));
    EXPECT_EQ(view.phase, jobs::SignalPhase::DONE);
    EXPECT_EQ(view.status, -ESRCH);
    ASSERT_TRUE(receipts.retire(first.handle));
    EXPECT_EQ(receipts.propose(1, 15).admission, jobs::Admission::EXPIRED);
    EXPECT_FALSE(receipts.claim(first.handle));
}

TEST(WkiJobControl, CapacityRefusalAndUnfinishedReceiptRetirementCannotLoseEffectIdentity) {
    jobs::SignalReceipts<1> receipts(64);
    auto const first = receipts.propose(1, 15);
    EXPECT_FALSE(receipts.retire(first.handle));
    ASSERT_TRUE(receipts.claim(first.handle));
    EXPECT_FALSE(receipts.retire(first.handle));
    EXPECT_EQ(receipts.propose(10, 9).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(receipts.lookup(10).admission, jobs::Admission::UNKNOWN);
    ASSERT_TRUE(receipts.complete(first.handle, 0));
    ASSERT_TRUE(receipts.retire(first.handle));
    auto const second = receipts.propose(2, 9);
    ASSERT_EQ(second.admission, jobs::Admission::NEW);
    EXPECT_EQ(second.handle.index, first.handle.index);
    EXPECT_NE(second.handle.generation, first.handle.generation);
    EXPECT_FALSE(receipts.claim(first.handle));
    EXPECT_FALSE(receipts.complete(first.handle, -EPERM));
    EXPECT_FALSE(receipts.retire(first.handle));
    jobs::SignalView unchanged{1, jobs::SignalPhase::DONE, -EPERM};
    EXPECT_FALSE(receipts.inspect(first.handle, unchanged));
    EXPECT_EQ(unchanged.status, -EPERM);
    EXPECT_EQ(receipts.lookup(1).admission, jobs::Admission::EXPIRED);
}

TEST(WkiJobControl, SignalSlotGenerationsAndOutOfOrderSerialsNeverWrapOrRerun) {
    jobs::SignalReceipts<1> receipts(64, UINT64_MAX);
    auto const last = receipts.propose(10, 9);
    ASSERT_EQ(last.admission, jobs::Admission::NEW);
    EXPECT_EQ(last.handle.generation, UINT64_MAX);
    EXPECT_EQ(receipts.propose(9, 9).admission, jobs::Admission::EXPIRED);
    EXPECT_FALSE(receipts.claim({0, 0}));
    EXPECT_FALSE(receipts.claim({1, UINT64_MAX}));
    ASSERT_TRUE(receipts.claim(last.handle));
    EXPECT_FALSE(receipts.complete(last.handle, 1));
    ASSERT_TRUE(receipts.complete(last.handle, 0));
    ASSERT_TRUE(receipts.retire(last.handle));
    EXPECT_EQ(receipts.propose(11, 9).admission, jobs::Admission::CAPACITY);
    EXPECT_EQ(receipts.propose(10, 9).admission, jobs::Admission::EXPIRED);
}

TEST(WkiJobControl, WholeJobCancellationCannotBeBlockedByFullSignalReceipts) {
    jobs::SignalReceipts<1> receipts(64);
    auto const signal = receipts.propose(1, 15);
    ASSERT_TRUE(receipts.claim(signal.handle));
    jobs::Lifecycle life;
    ASSERT_TRUE(life.accept());
    ASSERT_TRUE(life.commit());
    ASSERT_TRUE(life.start());
    ASSERT_TRUE(life.claim_publication());
    ASSERT_TRUE(life.publication_result(true));
    EXPECT_EQ(receipts.propose(2, 9).admission, jobs::Admission::CAPACITY);
    ASSERT_TRUE(life.cancel());
    EXPECT_FALSE(life.cancel());
    EXPECT_EQ(life.phase(), jobs::Phase::RUNNING);
    ASSERT_TRUE(life.complete());
    EXPECT_EQ(life.phase(), jobs::Phase::CANCELED);
    ASSERT_TRUE(receipts.complete(signal.handle, -ESRCH));
    EXPECT_EQ(life.phase(), jobs::Phase::CANCELED);
}

TEST(WkiJobControl, ConcurrentDuplicatesClaimOneOwnedEffectAndBalanceOuterJobHolders) {
    for (unsigned round = 0; round < 64; ++round) {
        jobs::Registry<1, 1> registry(13, {.byte_limit = 16, .proposal_ttl_us = 100, .result_ttl_us = 200});
        jobs::Key const key{
            .scope = {.owner = {.node = 7, .uid = 1000}, .submitter_incarnation = 11},
            .serial = 1,
            .receiver_incarnation = 13,
            .attempt = 1,
        };
        auto const job = registry.propose(key, {}, 16, 0);
        ASSERT_EQ(job.admission, jobs::Admission::NEW);
        auto* life = registry.lifecycle(job.handle);
        ASSERT_TRUE(life->accept());
        ASSERT_TRUE(life->commit());
        ASSERT_TRUE(life->start());
        ASSERT_TRUE(life->claim_publication());
        ASSERT_TRUE(life->publication_result(true));
        jobs::SignalReceipts<1> receipts(64);
        std::mutex owner_lock;
        std::barrier ready(8);
        std::atomic<unsigned> effects{};
        unsigned proposals = 0;
        std::vector<std::thread> workers;
        for (unsigned worker = 0; worker < 7; ++worker) {
            workers.emplace_back([&] {
                ready.arrive_and_wait();
                jobs::SignalHandle signal;
                bool claimed = false;
                {
                    std::scoped_lock guard(owner_lock);
                    auto const result = receipts.propose(1, 15);
                    if (result.admission == jobs::Admission::NEW)
                        ++proposals;
                    else
                        EXPECT_EQ(result.admission, jobs::Admission::REPLAY);
                    // Retain before claim, mirroring the adapter's ownership
                    // obligation. This witness is not native Task delivery.
                    EXPECT_TRUE(registry.retain(job.handle));
                    signal = result.handle;
                    claimed = receipts.claim(signal);
                    if (!claimed) EXPECT_TRUE(registry.release(job.handle));
                }
                if (claimed) {
                    effects.fetch_add(1);
                    std::scoped_lock guard(owner_lock);
                    EXPECT_TRUE(receipts.complete(signal, 0));
                    EXPECT_TRUE(registry.release(job.handle));
                }
            });
        }
        ready.arrive_and_wait();
        for (auto& worker : workers) worker.join();
        EXPECT_EQ(proposals, 1U);
        EXPECT_EQ(effects.load(), 1U);
        auto const signal = receipts.lookup(1);
        jobs::SignalView view;
        ASSERT_TRUE(receipts.inspect(signal.handle, view));
        EXPECT_EQ(view.phase, jobs::SignalPhase::DONE);
        EXPECT_EQ(view.status, 0);
        ASSERT_TRUE(life->complete());
        ASSERT_TRUE(registry.mark_terminal(job.handle, 10));
        ASSERT_TRUE(registry.expire_due(job.handle, 210));
        EXPECT_EQ(registry.reserved_bytes(), 0U);
        EXPECT_EQ(registry.lookup(key).admission, jobs::Admission::EXPIRED);
    }
}

TEST(WkiJobControl, ConcurrentResultsCannotOverwriteTheFirstCheckedOutcome) {
    jobs::SignalReceipts<1> receipts(64);
    auto const signal = receipts.propose(1, 15);
    ASSERT_TRUE(receipts.claim(signal.handle));
    std::mutex owner_lock;
    std::barrier ready(5);
    std::vector<std::thread> finishers;
    unsigned completions = 0;
    int32_t winner = 1;
    for (auto const status : {0, -ESRCH, -EPERM, 1}) {
        finishers.emplace_back([&, status] {
            ready.arrive_and_wait();
            std::scoped_lock guard(owner_lock);
            if (receipts.complete(signal.handle, status)) {
                ++completions;
                winner = status;
            }
        });
    }
    ready.arrive_and_wait();
    for (auto& finisher : finishers) finisher.join();
    EXPECT_EQ(completions, 1U);
    EXPECT_LE(winner, 0);
    jobs::SignalView view;
    ASSERT_TRUE(receipts.inspect(signal.handle, view));
    EXPECT_EQ(view.status, winner);
    EXPECT_EQ(view.phase, jobs::SignalPhase::DONE);
}
