#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <platform/sched/pid_namespace.hpp>
#include <thread>
#include <vector>

namespace pid_ns = ker::mod::sched::pid_ns;

TEST(PidNamespace, InitRetirementClosesExactScopeOnceAndPreservesAnAdmittedLease) {
    auto const BASELINE = pid_ns::live_namespace_count();
    pid_ns::NamespaceRef host, scope, sibling;
    EXPECT_FALSE(host.begin_retirement());
    EXPECT_FALSE(host.closed());
    EXPECT_FALSE(host.retiring());
    ASSERT_EQ(pid_ns::create_namespace(scope), 0);
    ASSERT_EQ(pid_ns::create_namespace(sibling), 0);
    pid_ns::OperationLease operation;
    ASSERT_EQ(operation.begin(scope), 0);
    EXPECT_TRUE(scope.begin_retirement());
    EXPECT_FALSE(scope.begin_retirement());
    EXPECT_TRUE(scope.closed());
    EXPECT_TRUE(scope.retiring());
    EXPECT_FALSE(sibling.closed());
    EXPECT_FALSE(sibling.retiring());
    EXPECT_FALSE(scope.operations_drained());
    uint32_t unchanged = 77;
    EXPECT_EQ(scope.allocate_id(unchanged), -EIDRM);
    EXPECT_EQ(unchanged, 77U);
    pid_ns::OperationLease late;
    EXPECT_EQ(late.begin(scope), -EIDRM);
    scope.reset();
    sibling.reset();
    EXPECT_EQ(pid_ns::live_namespace_count(), BASELINE + 1);
    EXPECT_TRUE(operation.scope()->retiring());
    operation.reset();
    EXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

TEST(PidNamespace, ConcurrentRetirementHasOneWinnerAndGatesEveryLaterPublication) {
    auto const BASELINE = pid_ns::live_namespace_count();
    pid_ns::NamespaceRef scope;
    ASSERT_EQ(pid_ns::create_namespace(scope), 0);
    std::atomic<bool> start{};
    std::atomic<unsigned> winners{};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            if (scope.begin_retirement()) winners.fetch_add(1, std::memory_order_relaxed);
            pid_ns::PublicationGuard gate(scope);
            EXPECT_FALSE(gate.open());
            EXPECT_TRUE(scope.retiring());
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(winners.load(), 1U);
    EXPECT_TRUE(scope.operations_drained());
    scope.reset();
    EXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

TEST(PidNamespace, RetiredSlotsRemainOwnedUntilDropAndReuseResetsAllState) {
    auto const BASELINE = pid_ns::live_namespace_count();
    std::array<pid_ns::NamespaceRef, pid_ns::MAX_NAMESPACES> scopes;
    auto const AVAILABLE = pid_ns::MAX_NAMESPACES - BASELINE;
    ASSERT_GT(AVAILABLE, 0U);
    for (size_t i = 0; i < AVAILABLE; ++i) {
        ASSERT_EQ(pid_ns::create_namespace(scopes[i]), 0);
        EXPECT_TRUE(scopes[i].begin_retirement());
    }
    pid_ns::NamespaceRef fresh;
    EXPECT_EQ(pid_ns::create_namespace(fresh), -ENOSPC);
    auto const OLD_GENERATION = scopes[0].generation();
    scopes[0].reset();
    ASSERT_EQ(pid_ns::create_namespace(fresh), 0);
    EXPECT_NE(fresh.generation(), OLD_GENERATION);
    EXPECT_FALSE(fresh.closed());
    EXPECT_FALSE(fresh.retiring());
    EXPECT_TRUE(fresh.operations_drained());
    uint32_t id = 99;
    EXPECT_EQ(fresh.allocate_id(id), 0);
    EXPECT_EQ(id, 1U);
    fresh.reset();
    for (auto& scope : scopes) scope.reset();
    EXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}
