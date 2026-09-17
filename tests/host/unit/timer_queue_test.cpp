#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <mutex>
#include <platform/ktime/timer_queue.hpp>
#include <random>
#include <set>
#include <thread>
#include <vector>

using namespace ker::mod::time;

namespace {
void count_callback(void* context) { ++*static_cast<unsigned*>(context); }
void noop(void*) {}
}  // namespace

TEST(TimerQueue, BoundedExpiryAndNoCallbackInExtraction) {
    TimerQueue queue;
    std::array<TimerNode, 10000> nodes;
    unsigned callbacks = 0;
    for (auto& node : nodes) ASSERT_TRUE(queue.arm_at(node, 20, count_callback, &callbacks));
    EXPECT_LE(queue.tree_height(), 20);
    std::array<TimerToken, 16> tokens{};
    EXPECT_EQ(queue.extract_due(19, tokens), 0U);
    EXPECT_EQ(queue.extract_due(20, {}), 0U);
    size_t total = 0;
    while (queue.size() != 0) {
        size_t const COUNT = queue.extract_due(20, tokens);
        ASSERT_GT(COUNT, 0U);
        EXPECT_LE(COUNT, 16U);
        EXPECT_EQ(callbacks, total);
        for (size_t i = 0; i < COUNT; ++i) {
            TimerInvocation invocation;
            ASSERT_TRUE(queue.claim(tokens[i], invocation));
            invocation.callback(invocation.context);
            EXPECT_TRUE(queue.finish(tokens[i], 20));
        }
        total += COUNT;
    }
    EXPECT_EQ(total, nodes.size());
    EXPECT_EQ(callbacks, nodes.size());
}

TEST(TimerQueue, StaleTokensCannotClaimCancelledOrRearmedGeneration) {
    TimerQueue queue;
    TimerNode node;
    std::array<TimerToken, 1> token{};
    ASSERT_TRUE(queue.arm_at(node, 0, noop));
    ASSERT_EQ(queue.extract_due(0, token), 1U);
    TimerToken const OLD = token[0];
    EXPECT_EQ(queue.cancel(node), TimerCancel::CANCELLED);
    TimerInvocation invocation;
    EXPECT_FALSE(queue.claim(OLD, invocation));
    ASSERT_TRUE(queue.arm_at(node, 0, noop));
    ASSERT_EQ(queue.extract_due(0, token), 1U);
    EXPECT_FALSE(queue.claim(OLD, invocation));
    ASSERT_TRUE(queue.claim(token[0], invocation));
    EXPECT_FALSE(queue.claim(token[0], invocation));
    EXPECT_FALSE(queue.finish(OLD, 0));
    EXPECT_TRUE(queue.finish(token[0], 0));
    EXPECT_FALSE(queue.finish(token[0], 0));
    EXPECT_EQ(queue.cancel(node), TimerCancel::INACTIVE);
}

TEST(TimerQueue, ReadyRearmAndWrongQueueAreExplicit) {
    TimerQueue queue;
    TimerQueue other;
    TimerNode node;
    std::array<TimerToken, 1> token{};
    ASSERT_TRUE(queue.arm_at(node, 1, noop));
    ASSERT_EQ(queue.extract_due(1, token), 1U);
    ASSERT_TRUE(queue.arm_at(node, 5, noop));
    TimerInvocation invocation;
    EXPECT_FALSE(queue.claim(token[0], invocation));
    EXPECT_FALSE(other.arm_at(node, 9, noop));
    EXPECT_EQ(other.cancel(node), TimerCancel::WRONG_QUEUE);
    EXPECT_FALSE(other.claim(token[0], invocation));
    EXPECT_EQ(queue.cancel(node), TimerCancel::CANCELLED);
    EXPECT_FALSE(other.arm_at(node, 9, noop));
    EXPECT_FALSE(queue.arm_at(node, 9, nullptr));
}

TEST(TimerQueue, RunningCancellationSuppressesPeriodicRearm) {
    TimerQueue queue;
    TimerNode node;
    std::array<TimerToken, 1> token{};
    ASSERT_TRUE(queue.arm_at(node, 10, noop, nullptr, 3));
    ASSERT_EQ(queue.extract_due(10, token), 1U);
    TimerInvocation invocation;
    ASSERT_TRUE(queue.claim(token[0], invocation));
    EXPECT_FALSE(queue.arm_at(node, 50, noop));
    EXPECT_EQ(queue.cancel(node), TimerCancel::RUNNING);
    EXPECT_TRUE(queue.finish(token[0], 100));
    EXPECT_EQ(queue.size(), 0U);
    EXPECT_EQ(node.state(), TimerState::IDLE);
}

TEST(TimerQueue, PeriodicExpiryPreservesPhaseAndTerminatesAtOverflow) {
    TimerQueue queue;
    TimerNode node;
    std::array<TimerToken, 1> token{};
    ASSERT_TRUE(queue.arm_at(node, 10, noop, nullptr, 7));
    ASSERT_EQ(queue.extract_due(100, token), 1U);
    TimerInvocation invocation;
    ASSERT_TRUE(queue.claim(token[0], invocation));
    ASSERT_TRUE(queue.finish(token[0], 100));
    EXPECT_EQ(node.deadline(), 101U);
    EXPECT_EQ(queue.size(), 1U);
    EXPECT_FALSE(queue.claim(token[0], invocation));
    ASSERT_TRUE(queue.arm_after(node, UINT64_MAX - 3, 9, noop, nullptr, 7));
    EXPECT_EQ(node.deadline(), UINT64_MAX);
    ASSERT_EQ(queue.extract_due(UINT64_MAX, token), 1U);
    ASSERT_TRUE(queue.claim(token[0], invocation));
    EXPECT_TRUE(queue.finish(token[0], UINT64_MAX));
    EXPECT_EQ(node.state(), TimerState::IDLE);
    EXPECT_EQ(queue.size(), 0U);
}

TEST(TimerQueue, RandomizedMutationsMatchOrderedReference) {
    constexpr size_t COUNT = 2048;
    TimerQueue queue;
    std::array<TimerNode, COUNT> nodes;
    std::set<std::pair<uint64_t, size_t>> reference;
    std::mt19937_64 random{0x571a7};
    std::array<TimerToken, 13> tokens{};
    for (unsigned operation = 0; operation < 100000; ++operation) {
        size_t const INDEX = random() % COUNT;
        auto& node = nodes[INDEX];
        if (random() % 3 != 0) {
            if (node.state() == TimerState::ARMED) reference.erase({node.deadline(), INDEX});
            uint64_t const DEADLINE = random() % 1000;
            ASSERT_TRUE(queue.arm_at(node, DEADLINE, noop));
            reference.insert({DEADLINE, INDEX});
        } else if (random() % 2 != 0) {
            if (node.state() == TimerState::ARMED) reference.erase({node.deadline(), INDEX});
            static_cast<void>(queue.cancel(node));
        } else {
            uint64_t const NOW = random() % 1000;
            size_t const EXTRACTED = queue.extract_due(NOW, tokens);
            for (size_t i = 0; i < EXTRACTED; ++i) {
                ASSERT_FALSE(reference.empty());
                auto const EXPECTED = *reference.begin();
                EXPECT_EQ(tokens[i].node, &nodes[EXPECTED.second]);
                EXPECT_LE(EXPECTED.first, NOW);
                reference.erase(reference.begin());
                TimerInvocation invocation;
                ASSERT_TRUE(queue.claim(tokens[i], invocation));
                ASSERT_TRUE(queue.finish(tokens[i], NOW));
            }
            if (EXTRACTED < tokens.size() && !reference.empty()) EXPECT_GT(reference.begin()->first, NOW);
        }
        EXPECT_EQ(queue.size(), reference.size());
        EXPECT_LE(queue.tree_height(), 16);
        uint64_t next = 0;
        EXPECT_EQ(queue.next_deadline(next), !reference.empty());
        if (!reference.empty()) EXPECT_EQ(next, reference.begin()->first);
    }
    for (auto& node : nodes) static_cast<void>(queue.cancel(node));
    EXPECT_EQ(queue.size(), 0U);
    EXPECT_EQ(queue.tree_height(), 0);
}

TEST(TimerQueue, ConcurrentCancelAndClaimHaveOneOwnerUnderCallerLock) {
    TimerQueue queue;
    std::array<TimerNode, 1024> nodes;
    std::array<TimerToken, 1024> tokens{};
    std::mutex lock;
    for (auto& node : nodes) ASSERT_TRUE(queue.arm_at(node, 0, noop, nullptr, 1));
    ASSERT_EQ(queue.extract_due(0, tokens), tokens.size());
    unsigned claimed = 0;
    unsigned cancelled = 0;
    std::thread worker([&] {
        for (auto token : tokens) {
            TimerInvocation invocation;
            {
                std::lock_guard guard(lock);
                if (!queue.claim(token, invocation)) continue;
                ++claimed;
            }
            invocation.callback(invocation.context);
            std::this_thread::yield();
            std::lock_guard guard(lock);
            EXPECT_TRUE(queue.finish(token, 0));
        }
    });
    std::thread canceller([&] {
        for (auto& node : nodes) {
            std::lock_guard guard(lock);
            bool const READY = node.state() == TimerState::READY;
            auto const RESULT = queue.cancel(node);
            if (READY) {
                EXPECT_EQ(RESULT, TimerCancel::CANCELLED);
                ++cancelled;
            }
        }
    });
    worker.join();
    canceller.join();
    EXPECT_EQ(claimed + cancelled, nodes.size());
    EXPECT_EQ(queue.size(), 0U);
}

TEST(TimerQueue, SynchronousQueuesTransferWithoutOutstandingTokens) {
    TimerQueue source{TimerDispatch::SYNCHRONOUS};
    TimerQueue destination{TimerDispatch::SYNCHRONOUS};
    TimerQueue deferred;
    TimerNode node;
    unsigned callbacks = 0;
    std::array<TimerToken, 1> tokens{};
    ASSERT_TRUE(source.arm_at(node, 5, count_callback, &callbacks));
    EXPECT_EQ(source.extract_due(5, tokens), 0U);
    EXPECT_FALSE(destination.arm_at(node, 5, noop));
    EXPECT_EQ(source.cancel(node), TimerCancel::CANCELLED);
    ASSERT_TRUE(destination.arm_at(node, 7, count_callback, &callbacks));
    TimerInvocation invocation;
    EXPECT_FALSE(destination.pop_due(6, invocation));
    ASSERT_TRUE(destination.pop_due(7, invocation));
    EXPECT_EQ(invocation.context, &callbacks);
    EXPECT_EQ(callbacks, 0U);
    EXPECT_EQ(node.state(), TimerState::IDLE);
    EXPECT_EQ(destination.size(), 0U);
    ASSERT_TRUE(source.arm_at(node, 9, count_callback, &callbacks));
    EXPECT_TRUE(source.pop_due(9, invocation));
    ASSERT_TRUE(deferred.arm_at(node, 11, noop));
    EXPECT_FALSE(deferred.pop_due(11, invocation));
    EXPECT_EQ(deferred.extract_due(11, tokens), 1U);
    EXPECT_EQ(deferred.cancel(node), TimerCancel::CANCELLED);
    // Deferred cancellation still retains association even though IDLE: its
    // stale token has not been consumed and cannot race a different queue lock.
    EXPECT_FALSE(source.arm_at(node, 13, noop));
    EXPECT_FALSE(deferred.claim(tokens[0], invocation));
}
