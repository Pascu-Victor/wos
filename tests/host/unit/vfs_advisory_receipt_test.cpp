#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <mutex>
#include <thread>
#include <vfs/advisory_receipt.hpp>

using namespace ker::vfs;
namespace {
constexpr RemoteAdvisoryCommand COMMAND{.owner = {.generation = 13, .boot_epoch = 17, .pid = 7, .peer = 19},
                                        .start = -3,
                                        .length = 7,
                                        .position = 11,
                                        .type = 1,
                                        .whence = 1,
                                        .wait = true};
constexpr RemoteAdvisoryPrepared RANGE{.dev = 23, .ino = 29, .start = 8, .end = 15};
}  // namespace

TEST(VfsAdvisoryReceipt, ImmutableIntentAndFirstNativeRangeSurvivePollAndReplay) {
    RemoteAdvisoryReceipt receipt;
    ASSERT_EQ(receipt.claim(31, COMMAND, RANGE), RemoteAdvisoryClaim::NEW);
    ASSERT_TRUE(receipt.mark_waiting(31));
    auto changed_range = RANGE;
    changed_range.start = 100;
    changed_range.end = 107;
    EXPECT_EQ(receipt.claim(31, COMMAND, changed_range), RemoteAdvisoryClaim::PENDING);
    EXPECT_EQ(receipt.prepared().start, 8U);
    for (unsigned field = 0; field < 13; ++field) {
        auto command = COMMAND;
        if (field == 0) ++command.owner.generation;
        if (field == 1) ++command.owner.boot_epoch;
        if (field == 2) ++command.owner.pid;
        if (field == 3) ++command.owner.peer;
        if (field == 4) command.kind = RemoteAdvisoryKind::OPEN_FILE;
        if (field == 5) ++command.start;
        if (field == 6) ++command.length;
        if (field == 7) ++command.position;
        if (field == 8) command.type = 0;
        if (field == 9) command.whence = 0;
        if (field == 10) command.wait = false;
        if (field == 11) {
            command.query = true;
            command.wait = false;
        }
        if (field == 12) command.scope_token = 41;
        EXPECT_EQ(receipt.probe(31, command), RemoteAdvisoryClaim::MISMATCH) << field;
    }
    EXPECT_EQ(receipt.probe(32, COMMAND), RemoteAdvisoryClaim::BUSY);
    EXPECT_EQ(receipt.probe(30, COMMAND), RemoteAdvisoryClaim::STALE);
    EXPECT_FALSE(receipt.complete(32, {}));
    ASSERT_TRUE(receipt.complete(31, {}));
    EXPECT_FALSE(receipt.complete(31, {.status = -EINTR}, true));
    EXPECT_FALSE(receipt.waiting());
    EXPECT_EQ(receipt.claim(31, COMMAND, changed_range), RemoteAdvisoryClaim::REPLAY);
    EXPECT_EQ(receipt.prepared().start, 8U);
}

TEST(VfsAdvisoryReceipt, CancelBeforeStartBurnsIdentityAndCannotCancelSuccessor) {
    RemoteAdvisoryReceipt receipt;
    ASSERT_TRUE(receipt.cancel_unstarted(31, COMMAND));
    EXPECT_EQ(receipt.claim(31, COMMAND, RANGE), RemoteAdvisoryClaim::REPLAY);
    RemoteAdvisoryReply reply{};
    ASSERT_TRUE(receipt.snapshot(31, reply));
    EXPECT_EQ(reply.state, RemoteAdvisoryState::CANCELLED);
    EXPECT_EQ(reply.outcome.status, -EINTR);
    ASSERT_EQ(receipt.claim(32, COMMAND, RANGE), RemoteAdvisoryClaim::NEW);
    EXPECT_FALSE(receipt.complete(31, {.status = -EINTR}, true));
    EXPECT_FALSE(receipt.cancel_unstarted(31, COMMAND));
    EXPECT_EQ(receipt.probe(31, COMMAND), RemoteAdvisoryClaim::STALE);
    EXPECT_TRUE(receipt.pending());
    ASSERT_TRUE(receipt.complete(32, {}));
    auto const SAVED = reply;
    EXPECT_FALSE(receipt.snapshot(31, reply));
    EXPECT_EQ(reply.operation, SAVED.operation);
    EXPECT_EQ(reply.outcome, SAVED.outcome);
}

TEST(VfsAdvisoryReceipt, InvalidCommandsExhaustionAndRetirementFailWithoutReuse) {
    RemoteAdvisoryReceipt receipt;
    auto invalid = COMMAND;
    invalid.owner.generation = 0;
    EXPECT_EQ(receipt.claim(31, invalid, RANGE), RemoteAdvisoryClaim::INVALID);
    EXPECT_EQ(receipt.claim(0, COMMAND, RANGE), RemoteAdvisoryClaim::INVALID);
    EXPECT_EQ(receipt.claim(31, COMMAND, {}), RemoteAdvisoryClaim::INVALID);
    ASSERT_EQ(receipt.claim(UINT64_MAX, COMMAND, RANGE), RemoteAdvisoryClaim::NEW);
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {.status = 1}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {}, true));
    ASSERT_TRUE(receipt.mark_waiting(UINT64_MAX));
    EXPECT_FALSE(receipt.mark_waiting(UINT64_MAX));
    EXPECT_TRUE(receipt.retire());
    EXPECT_FALSE(receipt.retire());
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {}));
    EXPECT_EQ(receipt.probe(UINT64_MAX, COMMAND), RemoteAdvisoryClaim::CLOSED);
    EXPECT_EQ(receipt.probe(1, COMMAND), RemoteAdvisoryClaim::CLOSED);
    RemoteAdvisoryReply reply{};
    ASSERT_TRUE(receipt.snapshot(UINT64_MAX, reply));
    EXPECT_EQ(reply.outcome.status, -ENOTCONN);
    EXPECT_FALSE(receipt.waiting());
}

TEST(VfsAdvisoryReceipt, ConcurrentGrantCancelAndRetireHaveOneTerminalTransition) {
    for (unsigned iteration = 0; iteration < 128; ++iteration) {
        RemoteAdvisoryReceipt receipt;
        ASSERT_EQ(receipt.claim(31, COMMAND, RANGE), RemoteAdvisoryClaim::NEW);
        ASSERT_TRUE(receipt.mark_waiting(31));
        std::mutex manager;
        std::barrier ready(4);
        unsigned grants = 0;
        unsigned cancels = 0;
        unsigned released_capacity = 0;
        std::thread grant([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(manager);
            if (receipt.complete(31, {})) {
                ++grants;
                ++released_capacity;
            }
        });
        std::thread cancel([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(manager);
            if (receipt.complete(31, {.status = -EINTR}, true)) {
                ++cancels;
                ++released_capacity;
            }
        });
        std::thread retire([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(manager);
            if (receipt.retire()) ++released_capacity;
        });
        ready.arrive_and_wait();
        grant.join();
        cancel.join();
        retire.join();
        EXPECT_EQ(released_capacity, 1U);
        EXPECT_LE(grants + cancels, 1U);
        RemoteAdvisoryReply reply{};
        ASSERT_TRUE(receipt.snapshot(31, reply));
        EXPECT_EQ(reply.outcome.status, grants ? 0 : cancels ? -EINTR : -ENOTCONN);
        EXPECT_FALSE(receipt.pending());
        EXPECT_FALSE(receipt.waiting());
    }
}
