#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <mutex>
#include <thread>
#include <vfs/advisory_client.hpp>
#include <vfs/coherency.hpp>

using namespace ker::vfs;
namespace client = ker::vfs::advisory_client;
namespace {
constexpr client::ScopeKey KEY{.object = 11, .process = 13, .boot = 17, .owner = 19};
constexpr RemoteAdvisoryReply GRANT{.operation = 23, .state = RemoteAdvisoryState::COMPLETE};
constexpr RemoteAdvisoryReply CANCEL{.operation = 23, .state = RemoteAdvisoryState::CANCELLED, .outcome = {.status = -EINTR}};
constexpr RemoteAdvisoryReply WAIT{.operation = 23, .state = RemoteAdvisoryState::PENDING, .outcome = {.status = -EAGAIN}};
}  // namespace

TEST(VfsAdvisoryClient, ShortGateAllowsHolderUnlockBetweenWaitingPollsAndRejectsOldRelease) {
    client::RpcGate gate;
    coherency::Counter tickets;
    auto const CHILD = tickets.take();
    ASSERT_TRUE(gate.claim(CHILD));
    EXPECT_FALSE(gate.claim(CHILD));
    auto const PARENT = tickets.take();
    EXPECT_FALSE(gate.claim(PARENT));
    ASSERT_TRUE(gate.release(CHILD));  // Child parks only after releasing its RPC gate.
    ASSERT_TRUE(gate.claim(PARENT));   // Parent unlock can make the child's wait progress.
    EXPECT_FALSE(gate.release(CHILD));
    EXPECT_EQ(gate.holder(), PARENT);
    ASSERT_TRUE(gate.release(PARENT));
    ASSERT_TRUE(gate.claim(tickets.take()));
    EXPECT_FALSE(gate.release(PARENT));
    EXPECT_FALSE(gate.claim(0));
}

TEST(VfsAdvisoryClient, LostRegistrationKeepsExactIdAndCloseSuppressesRecoveredGrant) {
    client::Scope scope;
    ASSERT_TRUE(scope.reserve(29, KEY, 7, 31));
    ASSERT_TRUE(scope.claim_rpc(29, 37));
    ASSERT_TRUE(scope.request_close(29));
    EXPECT_EQ(scope.registration(), 31U);
    EXPECT_FALSE(scope.grants_allowed(29));
    EXPECT_TRUE(scope.blocks_successor(KEY));
    EXPECT_FALSE(scope.registered(29, 32, 41, KEY.object, false));
    EXPECT_FALSE(scope.registered(29, 31, 41, KEY.object + 1, false));
    ASSERT_TRUE(scope.release_rpc(29, 37));
    ASSERT_TRUE(scope.claim_rpc(29, 43));  // Replay the same registration, not a new operation.
    ASSERT_TRUE(scope.registered(29, 31, 41, KEY.object, false));
    EXPECT_EQ(scope.phase(), client::ScopePhase::CLOSING);
    EXPECT_FALSE(scope.grants_allowed(29));
    ASSERT_TRUE(scope.closed(29, 41, KEY.object));
    EXPECT_FALSE(scope.blocks_successor(KEY));
    EXPECT_FALSE(scope.reclaimable(29));
    ASSERT_TRUE(scope.acknowledged(29, 41, KEY.object));
    EXPECT_FALSE(scope.reclaimable(29));  // The RPC stack still owns this record.
    ASSERT_TRUE(scope.release_rpc(29, 43));
    EXPECT_TRUE(scope.reclaimable(29));
}

TEST(VfsAdvisoryClient, ConfirmedCloseAllowsSuccessorWhileOldAckAndUsersRemainPinned) {
    client::Scope old, successor;
    ASSERT_TRUE(old.reserve(29, KEY, 7, 31));
    ASSERT_TRUE(old.registered(29, 31, 41, KEY.object, false));
    ASSERT_TRUE(old.retain(29, 41));
    ASSERT_TRUE(old.request_close(29));
    EXPECT_FALSE(old.retain(29, 41));
    ASSERT_TRUE(old.closed(29, 41, KEY.object));
    ASSERT_TRUE(successor.reserve(43, KEY, 7, 47));
    ASSERT_TRUE(successor.registered(43, 47, 53, KEY.object, false));
    EXPECT_TRUE(successor.grants_allowed(43));
    ASSERT_TRUE(old.acknowledged(29, 41, KEY.object));
    EXPECT_FALSE(old.reclaimable(29));
    EXPECT_FALSE(successor.closed(29, 41, KEY.object));
    EXPECT_FALSE(successor.release(29, 41));
    ASSERT_TRUE(old.release(29, 41));
    EXPECT_TRUE(old.reclaimable(29));
    EXPECT_TRUE(successor.grants_allowed(43));
    old = {};
    ASSERT_TRUE(old.reserve(59, KEY, 7, 61));
    EXPECT_FALSE(old.registered(29, 31, 41, KEY.object, false));
    EXPECT_FALSE(old.retire(29));
    EXPECT_FALSE(old.release_rpc(29, 43));
    EXPECT_EQ(old.phase(), client::ScopePhase::REGISTERING);
}

TEST(VfsAdvisoryClient, RegistrationAndCloseRaceNeverReopensTheScope) {
    for (unsigned i = 0; i != 128; ++i) {
        client::Scope scope;
        ASSERT_TRUE(scope.reserve(29, KEY, 7, 31));
        std::mutex registry;
        std::barrier ready(3);
        bool registered = false, closed = false;
        std::thread register_reply([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(registry);
            registered = scope.registered(29, 31, 41, KEY.object, false);
        });
        std::thread descriptor_close([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(registry);
            closed = scope.request_close(29);
        });
        ready.arrive_and_wait();
        register_reply.join();
        descriptor_close.join();
        EXPECT_TRUE(registered);
        EXPECT_TRUE(closed);
        EXPECT_EQ(scope.phase(), client::ScopePhase::CLOSING);
        EXPECT_FALSE(scope.grants_allowed(29));
        EXPECT_TRUE(scope.blocks_successor(KEY));
    }
}

TEST(VfsAdvisoryClient, CloseObserverPinsUnknownRegistrationThroughAckAndCannotDropSuccessor) {
    client::Scope scope;
    ASSERT_TRUE(scope.reserve(29, KEY, 7, 31));
    ASSERT_TRUE(scope.borrow(29));
    ASSERT_TRUE(scope.request_close(29));
    ASSERT_TRUE(scope.registered(29, 31, 41, KEY.object, false));
    ASSERT_TRUE(scope.closed(29, 41, KEY.object));
    ASSERT_TRUE(scope.acknowledged(29, 41, KEY.object));
    EXPECT_FALSE(scope.reclaimable(29));
    ASSERT_TRUE(scope.drop(29));
    EXPECT_TRUE(scope.reclaimable(29));
    scope = {};
    ASSERT_TRUE(scope.reserve(43, KEY, 7, 47));
    ASSERT_TRUE(scope.borrow(43));
    EXPECT_FALSE(scope.drop(29));
    ASSERT_TRUE(scope.retire(43));
    EXPECT_FALSE(scope.reclaimable(43));
    ASSERT_TRUE(scope.drop(43));
    EXPECT_TRUE(scope.reclaimable(43));
}

TEST(VfsAdvisoryClient, CancelGrantRaceKeepsTheOwnersSingleTerminalResultUntilAck) {
    for (auto const& terminal : {GRANT, CANCEL}) {
        client::Operation operation;
        ASSERT_TRUE(operation.reserve(29));
        ASSERT_TRUE(operation.started(29, 23));
        ASSERT_TRUE(operation.observe(29, WAIT));
        ASSERT_TRUE(operation.abandon(29));
        ASSERT_TRUE(operation.observe(29, terminal));
        EXPECT_EQ(operation.phase(), client::OperationPhase::ACK);
        EXPECT_EQ(operation.outcome().status, terminal.outcome.status);
        EXPECT_FALSE(operation.reclaimable(29));
        EXPECT_FALSE(operation.observe(29, terminal.state == GRANT.state ? CANCEL : GRANT));
        ASSERT_TRUE(operation.abandon(29));
        EXPECT_EQ(operation.outcome().status, terminal.outcome.status);
        ASSERT_TRUE(operation.acknowledged(29, 23));
        EXPECT_TRUE(operation.reclaimable(29));
        EXPECT_EQ(operation.outcome().status, terminal.outcome.status);
    }
}

TEST(VfsAdvisoryClient, AbandonmentBeforeStartNeedsNoCancelAndOldRepliesCannotReachReusedStorage) {
    client::Operation operation;
    ASSERT_TRUE(operation.reserve(29));
    ASSERT_TRUE(operation.abandon(29));
    EXPECT_TRUE(operation.reclaimable(29));
    EXPECT_EQ(operation.terminal(), RemoteAdvisoryState::EMPTY);
    EXPECT_FALSE(operation.started(29, 23));
    EXPECT_FALSE(operation.observe(29, GRANT));
    operation = {};
    ASSERT_TRUE(operation.reserve(31));
    ASSERT_TRUE(operation.started(31, 37));
    EXPECT_FALSE(operation.observe(29, GRANT));
    EXPECT_FALSE(operation.acknowledged(29, 23));
    EXPECT_FALSE(operation.retire(29));
    ASSERT_TRUE(operation.abandon(31));
    EXPECT_FALSE(operation.cancelled_without_receipt(31, 23));
    ASSERT_TRUE(operation.cancelled_without_receipt(31, 37));
    EXPECT_TRUE(operation.reclaimable(31));
    EXPECT_EQ(operation.outcome().status, -EINTR);
}

TEST(VfsAdvisoryClient, IdentityExhaustionAndMalformedTerminalCannotCreateGrantOrCleanupAuthority) {
    coherency::Counter serials(UINT64_MAX);
    client::Scope scope;
    ASSERT_TRUE(scope.reserve(serials.take(), KEY, 7, 31));
    EXPECT_FALSE(scope.reserve(serials.take(), KEY, 7, 37));
    EXPECT_FALSE(scope.request_close(0));
    EXPECT_FALSE(scope.reclaimable(0));
    EXPECT_FALSE(scope.retire(0));
    ASSERT_TRUE(scope.retire(UINT64_MAX));
    EXPECT_TRUE(scope.reclaimable(UINT64_MAX));
    scope = {};
    EXPECT_FALSE(scope.reserve(serials.take(), KEY, 7, 37));
    for (unsigned field = 0; field != 4; ++field) {
        auto key = KEY;
        if (field == 0) key.object = 0;
        if (field == 1) key.process = 0;
        if (field == 2) key.boot = 0;
        if (field == 3) key.owner = UINT16_MAX;
        EXPECT_FALSE(scope.reserve(29, key, 7, 31));
    }
    client::Operation operation;
    ASSERT_TRUE(operation.reserve(29));
    ASSERT_TRUE(operation.started(29, 23));
    auto wrong = CANCEL;
    wrong.outcome.status = 0;
    EXPECT_FALSE(operation.observe(29, wrong));
    wrong = GRANT;
    wrong.outcome.status = 1;
    EXPECT_FALSE(operation.observe(29, wrong));
    wrong = WAIT;
    wrong.outcome.status = 0;
    EXPECT_FALSE(operation.observe(29, wrong));
    EXPECT_FALSE(operation.acknowledged(29, 23));
    EXPECT_FALSE(operation.cancelled_without_receipt(29, 23));
    EXPECT_EQ(operation.phase(), client::OperationPhase::STARTED);
}

TEST(VfsAdvisoryClient, CancelAndTerminalDeliveryRaceCannotDiscardACommittedGrant) {
    for (unsigned i = 0; i != 128; ++i) {
        client::Operation operation;
        ASSERT_TRUE(operation.reserve(29));
        ASSERT_TRUE(operation.started(29, 23));
        std::mutex registry;
        std::barrier ready(3);
        bool observed = false, abandoned = false;
        std::thread response([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(registry);
            observed = operation.observe(29, GRANT);
        });
        std::thread signal([&] {
            ready.arrive_and_wait();
            std::lock_guard guard(registry);
            abandoned = operation.abandon(29);
        });
        ready.arrive_and_wait();
        response.join();
        signal.join();
        EXPECT_TRUE(observed);
        EXPECT_TRUE(abandoned);
        EXPECT_EQ(operation.phase(), client::OperationPhase::ACK);
        EXPECT_EQ(operation.outcome().status, 0);
        EXPECT_FALSE(operation.reclaimable(29));
    }
}
