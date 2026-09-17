#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <cassert>
#include <cstdio>
#include <dev/device.hpp>
#include <new>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/spinlock.hpp>
#include <thread>
#include <vector>

namespace {
std::atomic<unsigned> inserts{};
std::atomic<unsigned> removals{};
thread_local bool fail_nothrow_allocation = false;
}  // namespace

// Replace only the standard nothrow allocation entrypoint used by actual
// device.cpp. Successful allocations keep ordinary new/delete ownership.
auto operator new(std::size_t size, const std::nothrow_t&) noexcept -> void* {
    assert(ker::mod::sys::registry_test_lock_depth == 0);
    if (fail_nothrow_allocation) return nullptr;
    try {
        return ::operator new(size);
    } catch (...) {
        return nullptr;
    }
}

namespace ker::mod::perf {
void registry_accounting_observer(int operation, int64_t size) {
    EXPECT_GE(size, 0);
    // Exercise actual registry reentry. A callback under its nonrecursive
    // lock deadlocks and fails the bounded CTest timeout.
    static_cast<void>(ker::dev::dev_get_count());
    (operation == PERF_FLAG_CT_INSERT ? inserts : removals).fetch_add(1);
}
}  // namespace ker::mod::perf

using namespace ker::dev;

TEST(DeviceRegistry, PreservesOrderDuplicateKeysAndBorrowedStorage) {
    ASSERT_EQ(dev_get_count(), 0U);
    Device first{.major = 7, .minor = 8, .name = "shared"};
    Device second{.major = 7, .minor = 8, .name = "shared"};
    Device invalid{};
    EXPECT_EQ(dev_register(nullptr), -1);
    EXPECT_EQ(dev_register(&invalid), -1);
    EXPECT_EQ(dev_unregister(nullptr), -1);
    EXPECT_EQ(dev_unregister(&first), -1);
    EXPECT_EQ(dev_find_by_name(nullptr), nullptr);
    ASSERT_EQ(dev_register(&first), 0);
    ASSERT_EQ(dev_register(&second), 0);
    ASSERT_EQ(dev_register(&first), 0);
    EXPECT_EQ(dev_get_count(), 3U);
    EXPECT_EQ(dev_find(7, 8), &first);
    EXPECT_EQ(dev_find_by_name("shared"), &first);
    EXPECT_EQ(dev_get_at_index(0), &first);
    EXPECT_EQ(dev_get_at_index(1), &second);
    EXPECT_EQ(dev_get_at_index(2), &first);
    EXPECT_EQ(dev_get_at_index(3), nullptr);
    EXPECT_EQ(dev_unregister(&first), 0);
    EXPECT_EQ(dev_find_by_name("shared"), &second);
    EXPECT_EQ(dev_unregister(&second), 0);
    EXPECT_EQ(dev_find_by_name("shared"), &first);
    EXPECT_EQ(dev_unregister(&first), 0);
    EXPECT_EQ(dev_get_count(), 0U);
    EXPECT_EQ(dev_find(7, 8), nullptr);
    EXPECT_STREQ(first.name, "shared");
    // Empty-list tail state must permit a subsequent registration.
    EXPECT_EQ(dev_register(&second), 0);
    EXPECT_EQ(dev_get_at_index(0), &second);
    EXPECT_EQ(dev_unregister(&second), 0);
}

TEST(DeviceRegistry, ConcurrentPublicationSelectionAndRemoval) {
    ASSERT_EQ(dev_get_count(), 0U);
    constexpr size_t WORKERS = 8;
    constexpr size_t PER_WORKER = 48;
    constexpr unsigned ROUNDS = 24;
    std::array<Device, WORKERS * PER_WORKER> devices{};
    std::array<std::array<char, 32>, WORKERS * PER_WORKER> names{};
    for (size_t i = 0; i < devices.size(); ++i) {
        std::snprintf(names[i].data(), names[i].size(), "registry-%zu", i);
        devices[i].major = 200;
        devices[i].minor = static_cast<unsigned>(i);
        devices[i].name = names[i].data();
    }
    std::barrier start(static_cast<ptrdiff_t>(WORKERS));
    std::atomic<unsigned> failures{};
    unsigned const INSERTS = inserts.load();
    unsigned const REMOVALS = removals.load();
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < WORKERS; ++worker) {
        workers.emplace_back([&, worker] {
            start.arrive_and_wait();
            for (unsigned round = 0; round < ROUNDS; ++round) {
                for (size_t j = 0; j < PER_WORKER; ++j) {
                    auto& device = devices[worker * PER_WORKER + j];
                    if (dev_register(&device) != 0) ++failures;
                }
                for (size_t j = 0; j < PER_WORKER; ++j) {
                    auto& device = devices[worker * PER_WORKER + j];
                    if (dev_find(device.major, device.minor) != &device) ++failures;
                    if (dev_find_by_name(device.name) != &device) ++failures;
                    // Other workers may remove entries between count and at;
                    // a null result is allowed. All device storage stays alive.
                    auto count = dev_get_count();
                    if (count != 0) static_cast<void>(dev_get_at_index(count - 1));
                    if (dev_unregister(&device) != 0) ++failures;
                    if (dev_find_by_name(device.name) != nullptr) ++failures;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(failures.load(), 0U);
    EXPECT_EQ(dev_get_count(), 0U);
    EXPECT_EQ(inserts.load() - INSERTS, WORKERS * PER_WORKER * ROUNDS);
    EXPECT_EQ(removals.load() - REMOVALS, WORKERS * PER_WORKER * ROUNDS);
}

TEST(DeviceRegistry, AllocationFailurePublishesNothing) {
    ASSERT_EQ(dev_get_count(), 0U);
    Device existing{.name = "existing"};
    Device refused{.name = "refused"};
    ASSERT_EQ(dev_register(&existing), 0);
    unsigned const INSERTS = inserts.load();
    fail_nothrow_allocation = true;
    int const RESULT = dev_register(&refused);
    fail_nothrow_allocation = false;
    EXPECT_EQ(RESULT, -1);
    EXPECT_EQ(dev_get_count(), 1U);
    EXPECT_EQ(dev_get_at_index(0), &existing);
    EXPECT_EQ(dev_find_by_name("refused"), nullptr);
    EXPECT_EQ(inserts.load(), INSERTS);
    EXPECT_EQ(dev_unregister(&existing), 0);
    EXPECT_EQ(dev_register(&refused), 0);
    EXPECT_EQ(dev_unregister(&refused), 0);
}
