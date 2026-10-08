#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <mutex>
#include <platform/sched/ipc_namespace.hpp>
#include <thread>
#include <utility>
#include <vector>

namespace ipc = ker::mod::sched::ipc;

TEST(IpcNamespace, HostIsExplicitAndCannotBeClosed) {
    ipc::NamespaceRef host;
    EXPECT_TRUE(host.is_host());
    EXPECT_EQ(host.generation(), 0U);
    host.close();
    EXPECT_FALSE(host.closed());
    ipc::NamespaceRef other;
    ASSERT_EQ(ipc::create_namespace(other), 0);
    EXPECT_FALSE(other == host);
    ASSERT_EQ(host.retain(other), 0);
    EXPECT_TRUE(other.is_host());
    EXPECT_TRUE(other == host);
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, SiblingsAreDistinctAndExactOwnersPinIdentity) {
    {
        ipc::NamespaceRef first, second, retained;
        ASSERT_EQ(ipc::create_namespace(first), 0);
        ASSERT_EQ(ipc::create_namespace(second), 0);
        EXPECT_FALSE(first == second);
        EXPECT_NE(first.generation(), second.generation());
        ASSERT_EQ(first.retain(retained), 0);
        auto const GENERATION = first.generation();
        first.reset();
        EXPECT_EQ(retained.generation(), GENERATION);
        EXPECT_EQ(ipc::live_namespace_count(), 2U);
        retained.reset();
        EXPECT_EQ(ipc::live_namespace_count(), 1U);
    }
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, RetentionAndMoveSupportAliasingWithoutReleasingIdentity) {
    ipc::NamespaceRef owner;
    ASSERT_EQ(ipc::create_namespace(owner), 0);
    auto const GENERATION = owner.generation();
    ASSERT_EQ(owner.retain(owner), 0);
    EXPECT_EQ(owner.generation(), GENERATION);
    auto* alias = &owner;
    owner = std::move(*alias);
    EXPECT_EQ(owner.generation(), GENERATION);
    ipc::NamespaceRef moved(std::move(owner));
    EXPECT_TRUE(owner.is_host());
    EXPECT_EQ(moved.generation(), GENERATION);
    ipc::NamespaceRef destination;
    ASSERT_EQ(ipc::create_namespace(destination), 0);
    destination = std::move(moved);
    EXPECT_TRUE(moved.is_host());
    EXPECT_EQ(destination.generation(), GENERATION);
    EXPECT_EQ(ipc::live_namespace_count(), 1U);
    destination.reset();
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, ClosureFollowsAllOwnersAndRetainingCannotReopenIt) {
    ipc::NamespaceRef first, second, after_close;
    ASSERT_EQ(ipc::create_namespace(first), 0);
    ASSERT_EQ(first.retain(second), 0);
    second.close();
    EXPECT_TRUE(first.closed());
    ASSERT_EQ(first.retain(after_close), 0);
    EXPECT_TRUE(after_close.closed());
    first.reset();
    second.reset();
    EXPECT_TRUE(after_close.closed());
    after_close.reset();
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, AdmissionExhaustionPreservesOutputUntilFinalOwnerDrops) {
    std::array<ipc::NamespaceRef, ipc::MAX_NAMESPACES> owners;
    for (auto& owner : owners) ASSERT_EQ(ipc::create_namespace(owner), 0);
    auto const ORIGINAL = owners[0].generation();
    EXPECT_EQ(ipc::create_namespace(owners[0]), -ENOSPC);
    EXPECT_EQ(owners[0].generation(), ORIGINAL);
    ipc::NamespaceRef host;
    EXPECT_EQ(ipc::create_namespace(host), -ENOSPC);
    EXPECT_TRUE(host.is_host());
    ipc::NamespaceRef retained;
    ASSERT_EQ(owners.back().retain(retained), 0);
    auto const OLD = retained.generation();
    retained.close();
    owners.back().reset();
    EXPECT_EQ(ipc::create_namespace(host), -ENOSPC);
    retained.reset();
    ASSERT_EQ(ipc::create_namespace(host), 0);
    EXPECT_GT(host.generation(), OLD);
    EXPECT_FALSE(host.closed());
    host.reset();
    for (auto& owner : owners) owner.reset();
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, ThousandCreateCloseDrainCyclesDoNotRetainSlotsOrReuseGeneration) {
    uint64_t previous = 0;
    for (unsigned cycle = 0; cycle < 1000; ++cycle) {
        ipc::NamespaceRef owner, retained;
        ASSERT_EQ(ipc::create_namespace(owner), 0);
        EXPECT_GT(owner.generation(), previous);
        previous = owner.generation();
        ASSERT_EQ(owner.retain(retained), 0);
        owner.close();
        owner.reset();
        EXPECT_TRUE(retained.closed());
        EXPECT_EQ(ipc::live_namespace_count(), 1U);
        retained.reset();
        EXPECT_EQ(ipc::live_namespace_count(), 0U);
    }
}

TEST(IpcNamespace, ConcurrentRetentionKeepsExactOwnerAlive) {
    ipc::NamespaceRef owner;
    ASSERT_EQ(ipc::create_namespace(owner), 0);
    auto const GENERATION = owner.generation();
    std::atomic<bool> failed{};
    std::array<std::thread, 8> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            for (unsigned iteration = 0; iteration < 5000; ++iteration) {
                ipc::NamespaceRef held;
                if (owner.retain(held) != 0 || held.generation() != GENERATION || !(held == owner)) failed.store(true);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_FALSE(failed.load());
    EXPECT_EQ(owner.generation(), GENERATION);
    EXPECT_EQ(ipc::live_namespace_count(), 1U);
    owner.reset();
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}

TEST(IpcNamespace, ConcurrentCreateAndFinalReleaseNeverReusesLiveIdentity) {
    std::mutex lock;
    std::vector<uint64_t> generations;
    std::atomic<bool> failed{};
    std::array<std::thread, 8> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            for (unsigned iteration = 0; iteration < 1000; ++iteration) {
                ipc::NamespaceRef owner, held;
                if (ipc::create_namespace(owner) != 0 || owner.retain(held) != 0) {
                    failed.store(true);
                    return;
                }
                auto const GENERATION = owner.generation();
                owner.reset();
                if (held.generation() != GENERATION || held.closed()) failed.store(true);
                std::lock_guard guard(lock);
                generations.push_back(GENERATION);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_FALSE(failed.load());
    ASSERT_EQ(generations.size(), 8000U);
    std::ranges::sort(generations);
    EXPECT_EQ(std::adjacent_find(generations.begin(), generations.end()), generations.end());
    EXPECT_EQ(ipc::live_namespace_count(), 0U);
}
