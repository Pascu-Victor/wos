#include <gtest/gtest.h>

#include <barrier>
#include <mutex>
#include <thread>
#include <vfs/advisory_scope.hpp>
#include <vfs/coherency.hpp>

using namespace ker::vfs;
namespace {
constexpr RemoteAdvisoryScopeIdentity IDENTITY{
    .owner = {.generation = 13, .boot_epoch = 17, .pid = 7, .peer = 19}, .object = 23, .dev = 29, .ino = 31};
}

TEST(VfsAdvisoryScope, ClosedTokenCannotAuthorizeSuccessorEvenAfterStorageReuse) {
    coherency::Counter tokens;
    RemoteAdvisoryScope scope;
    auto const FIRST = tokens.take();
    ASSERT_TRUE(scope.initialize(FIRST, IDENTITY));
    EXPECT_TRUE(scope.matches(IDENTITY));
    ASSERT_EQ(scope.close(FIRST, IDENTITY.owner), RemoteAdvisoryScopeClaim::ACTIVE);
    EXPECT_FALSE(scope.matches(IDENTITY));
    EXPECT_EQ(scope.close(FIRST, IDENTITY.owner), RemoteAdvisoryScopeClaim::CLOSED);
    EXPECT_FALSE(scope.initialize(tokens.take(), IDENTITY));
    scope = {};
    auto const SUCCESSOR = tokens.take();
    ASSERT_TRUE(scope.initialize(SUCCESSOR, IDENTITY));
    EXPECT_NE(FIRST, SUCCESSOR);
    EXPECT_EQ(scope.close(FIRST, IDENTITY.owner), RemoteAdvisoryScopeClaim::STALE);
    EXPECT_TRUE(scope.matches(IDENTITY));
    EXPECT_EQ(scope.snapshot().state, RemoteAdvisoryScopeState::ACTIVE);
}

TEST(VfsAdvisoryScope, EveryOwnerAndCanonicalObjectFieldQualifiesAdmission) {
    RemoteAdvisoryScope scope;
    ASSERT_TRUE(scope.initialize(37, IDENTITY));
    for (unsigned field = 0; field < 7; ++field) {
        auto changed = IDENTITY;
        if (field == 0) ++changed.owner.generation;
        if (field == 1) ++changed.owner.boot_epoch;
        if (field == 2) ++changed.owner.pid;
        if (field == 3) ++changed.owner.peer;
        if (field == 4) ++changed.object;
        if (field == 5) ++changed.dev;
        if (field == 6) ++changed.ino;
        EXPECT_FALSE(scope.matches(changed)) << field;
        if (field < 4) EXPECT_EQ(scope.close(37, changed.owner), RemoteAdvisoryScopeClaim::MISMATCH);
    }
    EXPECT_TRUE(scope.matches(IDENTITY));
    EXPECT_EQ(scope.close(0, IDENTITY.owner), RemoteAdvisoryScopeClaim::INVALID);
    EXPECT_EQ(scope.close(37, {}), RemoteAdvisoryScopeClaim::INVALID);
}

TEST(VfsAdvisoryScope, ExhaustionAndInvalidRegistrationNeverCreateAuthority) {
    coherency::Counter tokens(UINT64_MAX);
    RemoteAdvisoryScope scope;
    ASSERT_TRUE(scope.initialize(tokens.take(), IDENTITY));
    ASSERT_EQ(scope.close(UINT64_MAX, IDENTITY.owner), RemoteAdvisoryScopeClaim::ACTIVE);
    scope = {};
    EXPECT_FALSE(scope.initialize(tokens.take(), IDENTITY));
    EXPECT_EQ(scope.snapshot().state, RemoteAdvisoryScopeState::EMPTY);
    for (unsigned field = 0; field < 3; ++field) {
        auto invalid = IDENTITY;
        if (field == 0) invalid.owner.generation = 0;
        if (field == 1) invalid.object = 0;
        if (field == 2) invalid.ino = 0;
        EXPECT_FALSE(scope.initialize(37, invalid));
    }
}

TEST(VfsAdvisoryScope, ConcurrentCloseAndRetirementHaveOneTerminalTransition) {
    for (unsigned iteration = 0; iteration < 128; ++iteration) {
        RemoteAdvisoryScope scope;
        ASSERT_TRUE(scope.initialize(37, IDENTITY));
        std::mutex manager;
        std::barrier ready(3);
        unsigned transitions = 0;
        auto close = [&] {
            ready.arrive_and_wait();
            std::lock_guard guard(manager);
            if (scope.close(37, IDENTITY.owner) == RemoteAdvisoryScopeClaim::ACTIVE) ++transitions;
        };
        std::thread client(close);
        std::thread retirement(close);
        ready.arrive_and_wait();
        client.join();
        retirement.join();
        EXPECT_EQ(transitions, 1U);
        EXPECT_EQ(scope.snapshot().state, RemoteAdvisoryScopeState::CLOSED);
        EXPECT_FALSE(scope.matches(IDENTITY));
    }
}
