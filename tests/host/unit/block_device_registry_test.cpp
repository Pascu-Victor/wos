#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <functional>
#include <latch>
#include <new>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>
#include <thread>
#include <vector>

namespace {
using Subsystem = ker::mod::perf::PerfSubsystem;
thread_local int fail_after = -1;
thread_local std::function<void()> allocation_failure_hook;
thread_local std::function<void(Subsystem, int)> accounting_hook;
}  // namespace

auto operator new(std::size_t size, const std::nothrow_t&) noexcept -> void* {
    if (ker::mod::sys::registry_test_lock_depth != 0) std::abort();
    if (fail_after == 0) {
        fail_after = -1;
        auto hook = std::move(allocation_failure_hook);
        if (hook) hook();
        return nullptr;
    }
    if (fail_after > 0) --fail_after;
    try {
        return ::operator new(size);
    } catch (...) {
        return nullptr;
    }
}

// Only the hosted decimal adapter is substituted. Registry, provider I/O,
// partition construction and GPT GUID formatting use production code.
namespace ker::util::string {
auto u64toa(uint64_t value, std::span<char> out, int base) -> int {
    auto result = std::to_chars(out.data(), out.data() + out.size() - 1, value, base);
    if (result.ec != std::errc{}) std::abort();
    *result.ptr = '\0';
    return static_cast<int>(result.ptr - out.data());
}
}  // namespace ker::util::string

namespace ker::mod::perf {
void registry_accounting_observer(PerfSubsystem subsystem, int operation, int64_t size) {
    EXPECT_GE(size, 0);
    // Both real registry implementations must release their nonrecursive
    // guards before accounting. These calls also expose staged transitions.
    static_cast<void>(ker::dev::dev_get_count());
    static_cast<void>(ker::dev::block_device_count());
    if (accounting_hook) accounting_hook(subsystem, operation);
}
}  // namespace ker::mod::perf

using namespace ker::dev;

namespace {
auto make_device(const char* name) -> BlockDevice {
    BlockDevice device{};
    std::snprintf(device.name.data(), device.name.size(), "%s", name);
    device.block_size = 512;
    device.total_blocks = 1000;
    return device;
}
}  // namespace

TEST(BlockRegistry, PreservesOrderNumericAliasesAndPartuuidSelection) {
    ASSERT_EQ(block_device_count(), 0U);
    auto first = make_device("first");
    auto second = make_device("second");
    auto duplicate = make_device("first");
    second.is_partition = true;
    std::snprintf(second.partuuid_str.data(), second.partuuid_str.size(), "test-uuid");
    EXPECT_EQ(block_device_register(nullptr), -1);
    EXPECT_EQ(block_device_find_by_name(nullptr), nullptr);
    EXPECT_EQ(block_device_find_by_partuuid(nullptr), nullptr);
    ASSERT_EQ(block_device_register(&first), 0);
    ASSERT_EQ(block_device_register(&second), 0);
    EXPECT_EQ(block_device_register(&first), -1);
    EXPECT_EQ(block_device_register(&duplicate), -1);
    EXPECT_EQ(block_device_count(), 2U);
    EXPECT_EQ(dev_get_count(), 2U);
    EXPECT_EQ(block_device_at(0), &first);
    EXPECT_EQ(block_device_at(1), &second);
    EXPECT_EQ(block_device_at(2), nullptr);
    EXPECT_EQ(block_device_find(0, 0), &first);
    EXPECT_EQ(block_device_find_by_partuuid("test-uuid"), &second);
    EXPECT_EQ(block_device_unregister(&second), 0);
    EXPECT_EQ(block_device_at(0), &first);
    EXPECT_EQ(block_device_unregister(&first), 0);
    EXPECT_EQ(block_device_unregister(&first), -1);
    EXPECT_EQ(block_device_count(), 0U);
    EXPECT_EQ(dev_get_count(), 0U);
}

TEST(BlockRegistry, ReservesNamesWhilePreparingAndRetiring) {
    auto original = make_device("reserved");
    auto competitor = make_device("reserved");
    std::latch prepared(1), publish(1);
    int result = -1;
    std::thread registering([&] {
        accounting_hook = [&](Subsystem subsystem, int operation) {
            if (subsystem == Subsystem::DEVICE_REG && operation == ker::mod::perf::PERF_FLAG_CT_INSERT) {
                prepared.count_down();
                publish.wait();
            }
        };
        result = block_device_register(&original);
    });
    prepared.wait();
    EXPECT_FALSE(block_device_is_registered(&original));
    EXPECT_EQ(block_device_find_by_wrapper(dev_find_by_name("reserved")), nullptr);
    EXPECT_EQ(block_device_find_by_name("reserved"), nullptr);
    EXPECT_EQ(block_device_count(), 0U);
    EXPECT_EQ(block_device_register(&competitor), -1);
    EXPECT_EQ(block_device_register(&original), -1);
    EXPECT_EQ(block_device_unregister(&original), -1);
    publish.count_down();
    registering.join();
    ASSERT_EQ(result, 0);
    std::latch retiring(1), finish(1);
    std::thread removing([&] {
        accounting_hook = [&](Subsystem subsystem, int operation) {
            if (subsystem == Subsystem::DEVICE_REG && operation == ker::mod::perf::PERF_FLAG_CT_REMOVE) {
                retiring.count_down();
                finish.wait();
            }
        };
        result = block_device_unregister(&original);
    });
    retiring.wait();
    EXPECT_FALSE(block_device_is_registered(&original));
    EXPECT_EQ(block_device_find_by_name("reserved"), nullptr);
    EXPECT_EQ(block_device_count(), 0U);
    EXPECT_EQ(dev_get_count(), 0U);
    EXPECT_EQ(block_device_register(&competitor), -1);
    EXPECT_EQ(block_device_unregister(&original), -1);
    finish.count_down();
    removing.join();
    EXPECT_EQ(result, 0);
    EXPECT_EQ(block_device_register(&competitor), 0);
    EXPECT_EQ(block_device_unregister(&competitor), 0);
}

TEST(BlockRegistry, WrapperAllocationFailureRollsBackOnlyItsReservation) {
    auto refused = make_device("refused");
    auto unrelated = make_device("unrelated");
    fail_after = 0;
    EXPECT_EQ(block_device_register(&refused), -1);
    EXPECT_EQ(block_device_count(), 0U);
    // Fail Device registry allocation after reserving the block name. Complete
    // another registration during the failure to expose incorrect tail rollback.
    allocation_failure_hook = [&] { EXPECT_EQ(block_device_register(&unrelated), 0); };
    fail_after = 1;
    EXPECT_EQ(block_device_register(&refused), -1);
    EXPECT_EQ(block_device_count(), 1U);
    EXPECT_EQ(block_device_at(0), &unrelated);
    EXPECT_EQ(block_device_find_by_name("refused"), nullptr);
    EXPECT_EQ(dev_find_by_name("refused"), nullptr);
    EXPECT_EQ(dev_get_count(), 1U);
    EXPECT_EQ(block_device_register(&refused), 0);
    EXPECT_EQ(block_device_unregister(&refused), 0);
    EXPECT_EQ(block_device_unregister(&unrelated), 0);
}

TEST(BlockRegistry, ConcurrentPublicationSelectionAndRemoval) {
    constexpr size_t WORKERS = 8, PER_WORKER = 24;
    constexpr unsigned ROUNDS = 20;
    std::array<BlockDevice, WORKERS * PER_WORKER> devices{};
    for (size_t i = 0; i < devices.size(); ++i) {
        std::snprintf(devices[i].name.data(), devices[i].name.size(), "concurrent-%zu", i);
        devices[i].minor = static_cast<unsigned>(i);
    }
    std::atomic<unsigned> failures{};
    std::barrier phase(static_cast<ptrdiff_t>(WORKERS));
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < WORKERS; ++worker) {
        workers.emplace_back([&, worker] {
            for (unsigned round = 0; round < ROUNDS; ++round) {
                phase.arrive_and_wait();
                for (size_t j = 0; j < PER_WORKER; ++j) {
                    if (block_device_register(&devices[worker * PER_WORKER + j]) != 0) ++failures;
                }
                phase.arrive_and_wait();
                for (size_t j = 0; j < PER_WORKER; ++j) {
                    auto& device = devices[worker * PER_WORKER + j];
                    if (block_device_find_by_name(device.name.data()) != &device) ++failures;
                    if (block_device_find(0, device.minor) != &device) ++failures;
                    block_device_set_read_only(&device, true);
                    if (!block_device_is_read_only(&device)) ++failures;
                    if (block_device_unregister(&device) != 0) ++failures;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(failures.load(), 0U);
    EXPECT_EQ(block_device_count(), 0U);
    EXPECT_EQ(dev_get_count(), 0U);
}

TEST(BlockRegistry, ConcurrentSameNameHasOneWinner) {
    constexpr size_t WORKERS = 8;
    std::array<BlockDevice, WORKERS> devices{};
    for (auto& device : devices) device = make_device("same-name");
    std::barrier start(static_cast<ptrdiff_t>(WORKERS));
    std::atomic<unsigned> winners{};
    std::vector<std::thread> workers;
    for (auto& device : devices)
        workers.emplace_back([&device, &start, &winners] {
            start.arrive_and_wait();
            if (block_device_register(&device) == 0) ++winners;
        });
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(winners.load(), 1U);
    EXPECT_EQ(block_device_count(), 1U);
    EXPECT_EQ(dev_get_count(), 1U);
    EXPECT_EQ(block_device_unregister(block_device_find_by_name("same-name")), 0);
}

TEST(BlockRegistry, ReadOnlyAliasesAndIoCallbacksDoNotHoldRegistryLock) {
    auto parent = make_device("parent");
    auto alias = make_device("alias");
    alias.is_partition = true;
    alias.parent_disk = &parent;
    alias.partition_end_lba = 100;
    parent.read_blocks = [](BlockDevice* device, uint64_t, size_t, void*) {
        EXPECT_EQ(block_device_find_by_name(device->name.data()), device);
        return 0;
    };
    parent.write_blocks = [](BlockDevice* device, uint64_t, size_t, const void*) {
        EXPECT_EQ(block_device_find_by_name(device->name.data()), device);
        return 0;
    };
    ASSERT_EQ(block_device_register(&parent), 0);
    ASSERT_EQ(block_device_register(&alias), 0);
    block_device_set_read_only(&parent, true);
    EXPECT_TRUE(block_device_is_read_only(&alias));
    std::array<char, 512> buffer{};
    EXPECT_EQ(block_write(&parent, 0, 1, buffer.data()), -EROFS);
    block_device_set_read_only(&alias, false);
    EXPECT_FALSE(block_device_is_read_only(&parent));
    EXPECT_EQ(block_read(&parent, 0, 1, buffer.data()), 0);
    EXPECT_EQ(block_write(&parent, 0, 1, buffer.data()), 0);
    BlockWriterLease local, remote;
    EXPECT_TRUE(local.try_acquire(&alias, BlockWriterLeaseOwner::LOCAL_MOUNT));
    EXPECT_FALSE(remote.try_acquire(&parent, BlockWriterLeaseOwner::REMOTE_BINDING));
    local.release();
    EXPECT_TRUE(remote.try_acquire(&parent, BlockWriterLeaseOwner::REMOTE_BINDING));
    remote.release();
    EXPECT_EQ(block_device_unregister(&alias), 0);
    EXPECT_EQ(block_device_unregister(&parent), 0);
}

TEST(BlockRegistry, PartitionFactoryPropagatesRegistrationFailure) {
    auto parent = make_device("disk");
    auto collision = make_device("disk1");
    std::array<uint8_t, 16> uuid{};
    ASSERT_EQ(block_device_register(&parent), 0);
    ASSERT_EQ(block_device_register(&collision), 0);
    EXPECT_EQ(block_device_create_partition(&parent, 1, 10, uuid.data(), 0), nullptr);
    EXPECT_EQ(block_device_count(), 2U);
    EXPECT_EQ(block_device_unregister(&collision), 0);
    auto* partition = block_device_create_partition(&parent, 1, 10, uuid.data(), 0);
    ASSERT_NE(partition, nullptr);
    EXPECT_EQ(block_device_find_by_partuuid(partition->partuuid_str.data()), partition);
    EXPECT_EQ(block_device_unregister(partition), 0);
    delete partition;
    EXPECT_EQ(block_device_unregister(&parent), 0);
}

TEST(BlockRegistry, ProviderIdentityDoesNotReselectAName) {
    auto first = make_device("identity");
    auto replacement = make_device("identity");
    EXPECT_FALSE(block_device_is_registered(nullptr));
    EXPECT_EQ(block_device_find_by_wrapper(nullptr), nullptr);
    ASSERT_EQ(block_device_register(&first), 0);
    auto* wrapper = dev_find_by_name("identity");
    ASSERT_NE(wrapper, nullptr);
    EXPECT_EQ(block_device_find_by_wrapper(wrapper), &first);
    EXPECT_TRUE(block_device_is_registered(&first));
    Device unrelated{};
    EXPECT_EQ(block_device_find_by_wrapper(&unrelated), nullptr);
    ASSERT_EQ(block_device_unregister(&first), 0);
    EXPECT_FALSE(block_device_is_registered(&first));
    // Compare only the retired pointer; do not dereference reclaimed wrapper.
    EXPECT_EQ(block_device_find_by_wrapper(wrapper), nullptr);
    ASSERT_EQ(block_device_register(&replacement), 0);
    EXPECT_FALSE(block_device_is_registered(&first));
    EXPECT_TRUE(block_device_is_registered(&replacement));
    EXPECT_EQ(block_device_find_by_name("identity"), &replacement);
    EXPECT_EQ(block_device_unregister(&replacement), 0);
}
