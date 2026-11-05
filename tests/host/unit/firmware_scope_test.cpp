#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <dev/firmware/firmware.hpp>
#include <net/wki/auth_crypto.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <thread>
#include <type_traits>

namespace fw = ker::dev::firmware;
namespace sched = ker::mod::sched;
namespace {
constexpr std::array<uint8_t, 3> BYTES{'a', 'b', 'c'};
const std::array<fw::ArchiveEntry, 1> INDEX{{{"host/blob", BYTES}}};
std::atomic<fw::Error> source_error{fw::Error::none};
auto policy() -> fw::Policy { return {"host/blob", 3, 3, ker::net::wki::auth_crypto::sha256(BYTES.data(), BYTES.size()), 7}; }
auto stats() -> fw::Stats {
    fw::Stats result{};
    EXPECT_EQ(fw::snapshot(result), fw::Error::none);
    return result;
}
struct TaskContext {
    sched::task::Task owner;
    TaskContext() {
        firmware_host::ordinary = true;
        firmware_host::current = &owner;
    }
    ~TaskContext() { firmware_host::current = nullptr; }
};
static_assert(!std::is_move_constructible_v<fw::Scope> && !std::is_copy_constructible_v<fw::Scope>);
}  // namespace
namespace ker::dev::firmware {
auto source_status() -> Error { return source_error.load(); }
auto source_index() -> std::span<const ArchiveEntry> { return INDEX; }
}  // namespace ker::dev::firmware

TEST(FirmwareScope, UnsafeEntryNeverQueriesTaskOrLocksOrPublishes) {
    TaskContext context;
    auto const LOCKS = firmware_host::locks.load();
    auto const QUERIES = firmware_host::queries;
    firmware_host::ordinary = false;
    {
        fw::Scope scope;
        fw::View view{};
        EXPECT_EQ(scope.request(policy(), 7, view), fw::Error::context);
        EXPECT_EQ(scope.release(view), fw::Error::context);
        fw::Stats output{99, 98, 97, 96};
        EXPECT_EQ(fw::snapshot(output), fw::Error::context);
        EXPECT_EQ(output.objects, 99u);
    }
    EXPECT_EQ(firmware_host::queries, QUERIES);
    EXPECT_EQ(firmware_host::locks.load(), LOCKS);
    EXPECT_FALSE(context.owner.kernel_shutdown_ownership.held());
    firmware_host::ordinary = true;
}

TEST(FirmwareScope, ForeignStaleAndDuplicateViewsCannotReleaseOwners) {
    TaskContext context;
    fw::View old{};
    {
        fw::Scope first;
        fw::Scope second;
        fw::View other{};
        ASSERT_EQ(first.request(policy(), 7, old), fw::Error::none);
        ASSERT_EQ(second.request(policy(), 7, other), fw::Error::none);
        EXPECT_EQ(other.object, old.object);
        EXPECT_EQ(second.release(old), fw::Error::stale);
        EXPECT_EQ(stats().references, 2u);
        EXPECT_EQ(first.release(old), fw::Error::none);
        EXPECT_EQ(first.release(old), fw::Error::stale);
        fw::View again{};
        ASSERT_EQ(first.request(policy(), 7, again), fw::Error::none);
        EXPECT_EQ(first.release(old), fw::Error::stale);
        EXPECT_EQ(stats().references, 2u);
        EXPECT_TRUE(context.owner.kernel_shutdown_ownership.held());
    }
    EXPECT_FALSE(context.owner.kernel_shutdown_ownership.held());
    EXPECT_EQ(stats().objects, 0u);
    EXPECT_EQ(stats().references, 0u);
    fw::Scope next;
    EXPECT_EQ(next.release(old), fw::Error::stale);
}

TEST(FirmwareScope, WrongTaskCallsAreRejectedBeforeMutex) {
    TaskContext context;
    fw::Scope scope;
    fw::View view{};
    ASSERT_EQ(scope.request(policy(), 7, view), fw::Error::none);
    auto const LOCKS = firmware_host::locks.load();
    sched::task::Task other;
    firmware_host::current = &other;
    EXPECT_EQ(scope.release(view), fw::Error::context);
    EXPECT_EQ(scope.request(policy(), 7, view), fw::Error::context);
    EXPECT_EQ(firmware_host::locks.load(), LOCKS);
    firmware_host::current = &context.owner;
    EXPECT_EQ(scope.release(view), fw::Error::none);
}

TEST(FirmwareScope, MissingSourceAndFullScopeLeaveOutputAndReferencesBalanced) {
    TaskContext context;
    {
        fw::Scope scope;
        fw::View sentinel{{}, {3, 99}, 88, 77};
        source_error.store(fw::Error::unavailable);
        EXPECT_EQ(scope.request(policy(), 7, sentinel), fw::Error::unavailable);
        source_error.store(fw::Error::none);
        EXPECT_EQ(sentinel.object, (fw::Handle{3, 99}));
        EXPECT_EQ(sentinel.scope, 88u);
        EXPECT_EQ(sentinel.lease, 77u);
        fw::View view{};
        for (size_t i = 0; i < fw::SCOPE_LEASES; ++i) ASSERT_EQ(scope.request(policy(), 7, view), fw::Error::none);
        EXPECT_EQ(scope.request(policy(), 7, sentinel), fw::Error::scope_full);
        EXPECT_EQ(sentinel.object, (fw::Handle{3, 99}));
        EXPECT_EQ(stats().references, fw::SCOPE_LEASES);
    }
    EXPECT_EQ(stats().references, 0u);
}

TEST(FirmwareScope, ActualServiceSerializesHostThreadsAndRetainsAcrossWait) {
    TaskContext context;
    auto const BEFORE = stats();
    std::array<fw::View, 2> views{};
    std::barrier barrier(3);
    std::array<std::thread, 2> workers;
    for (size_t i = 0; i < workers.size(); ++i) {
        workers.at(i) = std::thread([&, i] {
            TaskContext worker;
            {
                fw::Scope scope;
                EXPECT_EQ(scope.request(policy(), 7, views.at(i)), fw::Error::none);
                EXPECT_TRUE(worker.owner.kernel_shutdown_ownership.held());
                barrier.arrive_and_wait();
                barrier.arrive_and_wait();
                EXPECT_TRUE(worker.owner.kernel_shutdown_ownership.held());
            }
            EXPECT_FALSE(worker.owner.kernel_shutdown_ownership.held());
        });
    }
    barrier.arrive_and_wait();
    auto const SHARED = stats();
    EXPECT_EQ(SHARED.objects, 1u);
    EXPECT_EQ(SHARED.references, 2u);
    EXPECT_EQ(SHARED.verifications, BEFORE.verifications + 1);
    EXPECT_EQ(SHARED.hits, BEFORE.hits + 1);
    EXPECT_EQ(views.at(0).object, views.at(1).object);
    EXPECT_EQ(views.at(0).bytes.data(), views.at(1).bytes.data());
    barrier.arrive_and_wait();
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(stats().objects, 0u);
    EXPECT_EQ(stats().references, 0u);
}

TEST(FirmwareScope, UnsafeOrForeignDestructionIsFatal) {
    EXPECT_DEATH(
        {
            TaskContext context;
            fw::Scope scope;
            firmware_host::ordinary = false;
        },
        "firmware Scope destroyed");
    EXPECT_DEATH(
        {
            TaskContext context;
            fw::Scope scope;
            sched::task::Task other;
            firmware_host::current = &other;
        },
        "firmware Scope destroyed");
}
