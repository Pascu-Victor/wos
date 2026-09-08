#include "platform/sched/resource_model.hpp"

#include <gtest/gtest.h>

#include <mutex>
#include <thread>
#include <vector>

#include "platform/sched/fd_table.hpp"
#include "platform/sched/service_resource_model.hpp"

namespace resource = ker::mod::sched::resource;
using resource::State;
constexpr unsigned AS = static_cast<unsigned>(resource::Id::AS);
constexpr unsigned NOFILE = static_cast<unsigned>(resource::Id::NOFILE);

TEST(ServiceResourceModel, CpuBudgetSurvivesRestartsAndStopsEveryConsumer) {
    ker::abi::service_resource::Configuration config{};
    for (auto& limit : config.limits) limit = {UINT64_MAX, UINT64_MAX};
    config.limits[0] = {1, 2};
    config.limits[5] = {256, 256};
    resource::ServiceState service;
    ASSERT_TRUE(service.configure(config));
    ASSERT_TRUE(service.admit_start());
    EXPECT_EQ(service.totals.charge_cpu(999999), resource::CpuAction::NONE);
    State exited_process;
    service.retire(exited_process.entries);
    ASSERT_TRUE(service.admit_start());
    EXPECT_EQ(service.totals.charge_cpu(1), resource::CpuAction::SOFT_SIGNAL);
    service.retire(exited_process.entries);
    ASSERT_TRUE(service.admit_start());
    EXPECT_EQ(service.totals.charge_cpu(1000000), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(service.totals.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(service.totals.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_FALSE(service.admit_start());
    EXPECT_EQ(service.totals.cpu_time_us, 2000002);
    EXPECT_EQ(service.totals.entries[0].denied, 3);
    EXPECT_EQ(service.totals.entries[0].reason, resource::Denial::HARD_LIMIT);
}

TEST(ServiceResourceModel, ConfigurationIsAtomicAndZeroHardBudgetCannotStart) {
    ker::abi::service_resource::Configuration config{};
    for (auto& limit : config.limits) limit = {UINT64_MAX, UINT64_MAX};
    config.limits[0] = {0, 0};
    config.limits[5] = {256, 257};
    resource::ServiceState service;
    EXPECT_FALSE(service.configure(config));
    EXPECT_EQ(service.totals.entries[0].limit.hard, UINT64_MAX);
    config.limits[5] = {256, 256};
    ASSERT_TRUE(service.configure(config));
    EXPECT_FALSE(service.admit_start());
    EXPECT_EQ(service.totals.cpu_time_us, 0);
}

TEST(ServiceResourceModel, RetiredAndLiveObservationsPreservePeaksAndSaturateDenials) {
    resource::ServiceState service;
    State process;
    process.entries[AS].current = 4096;
    process.entries[AS].peak = 8192;
    process.entries[AS].denied = UINT64_MAX - 1;
    process.entries[AS].reason = resource::Denial::SOFT_LIMIT;
    process.entries[0].denied = 100;
    service.retire(process.entries);
    EXPECT_EQ(service.totals.entries[AS].current, 0);
    EXPECT_EQ(service.totals.entries[AS].peak, 8192);
    EXPECT_EQ(service.totals.entries[0].denied, 0);
    resource::Usage snapshot = service.totals.entries[AS];
    resource::ServiceState::merge(snapshot, process.entries[AS], true);
    EXPECT_EQ(snapshot.current, 4096);
    EXPECT_EQ(snapshot.peak, 8192);
    EXPECT_EQ(snapshot.denied, UINT64_MAX);
    EXPECT_EQ(snapshot.reason, resource::Denial::SOFT_LIMIT);
    EXPECT_EQ(snapshot.limit.hard, UINT64_MAX);
}

TEST(ResourceModel, ProcessAdmissionDoesNotChargeAndChecksEveryBoundary) {
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    State state;
    for (uint64_t limit : std::array<uint64_t, 4>{0, 1, 17, UINT64_MAX}) {
        ASSERT_EQ(state.set(NPROC, {limit, UINT64_MAX}, true, 256), 0);
        if (limit != 0) EXPECT_TRUE(state.admit_process(limit - 1));
        EXPECT_FALSE(state.admit_process(limit));
        EXPECT_EQ(state.entries[NPROC].current, 0);
        EXPECT_EQ(state.entries[NPROC].peak, 0);
        EXPECT_EQ(state.entries[NPROC].reason, limit == UINT64_MAX ? resource::Denial::OVERFLOW : resource::Denial::SOFT_LIMIT);
    }
    EXPECT_EQ(state.entries[NPROC].denied, 4);
}

TEST(ResourceModel, BoundaryAndDenialHaveNoChargeSideEffects) {
    for (uint64_t limit : {0ULL, 1ULL, 127ULL}) {
        State state;
        ASSERT_EQ(state.set(AS, {limit, limit}, false, 256), 0);
        if (limit != 0) {
            ASSERT_TRUE(state.reserve(AS, limit - 1));
            ASSERT_TRUE(state.reserve(AS, 1));
        }
        EXPECT_TRUE(state.reserve(AS, 0));
        EXPECT_FALSE(state.reserve(AS, 1));
        EXPECT_EQ(state.entries[AS].current, limit);
        EXPECT_EQ(state.entries[AS].peak, limit);
        EXPECT_EQ(state.entries[AS].denied, 1);
        EXPECT_TRUE(state.release(AS, limit));
        EXPECT_FALSE(state.release(AS, 1));
        EXPECT_EQ(state.entries[AS].current, 0);
    }
}

TEST(ResourceModel, InfinityDoesNotPermitIntegerOverflow) {
    State state;
    ASSERT_TRUE(state.reserve(AS, UINT64_MAX));
    EXPECT_FALSE(state.reserve(AS, 1));
    EXPECT_EQ(state.entries[AS].current, UINT64_MAX);
    EXPECT_EQ(state.entries[AS].reason, resource::Denial::OVERFLOW);
    ASSERT_TRUE(state.release(AS, UINT64_MAX));
    EXPECT_TRUE(state.reserve(AS, 1));
}

TEST(ResourceModel, PrivilegedHardRaiseAndPhysicalDescriptorCeiling) {
    State state;
    EXPECT_EQ(state.set(AS, {10, 9}, true, 256), -EINVAL);
    ASSERT_EQ(state.set(AS, {8, 9}, false, 256), 0);
    EXPECT_EQ(state.set(AS, {8, 10}, false, 256), -EPERM);
    EXPECT_EQ(state.entries[AS].limit.hard, 9);
    EXPECT_EQ(state.set(AS, {10, 10}, true, 256), 0);
    EXPECT_EQ(state.set(NOFILE, {257, 257}, true, 256), -EPERM);
    EXPECT_EQ(state.set(16, {0, 0}, true, 256), -EINVAL);
}

TEST(ResourceModel, LoweringBelowUseDoesNotRevokeAndAllowsUnwind) {
    State state;
    ASSERT_TRUE(state.reserve(AS, 10));
    ASSERT_EQ(state.set(AS, {3, 10}, false, 256), 0);
    EXPECT_FALSE(state.reserve(AS, 1));
    EXPECT_TRUE(state.reserve(AS, 0));
    ASSERT_TRUE(state.release(AS, 8));
    EXPECT_TRUE(state.reserve(AS, 1));
    EXPECT_EQ(state.entries[AS].peak, 10);
}

TEST(ResourceModel, ForkSnapshotSeparatesPolicyAndUsage) {
    State parent;
    ASSERT_EQ(parent.set(AS, {5, 9}, false, 256), 0);
    ASSERT_TRUE(parent.reserve(AS, 5));
    EXPECT_FALSE(parent.reserve(AS, 1));
    State child;
    child.inherit_limits(parent);
    EXPECT_EQ(child.entries[AS].limit.soft, 5);
    EXPECT_EQ(child.entries[AS].limit.hard, 9);
    EXPECT_EQ(child.entries[AS].current, 0);
    EXPECT_EQ(child.entries[AS].denied, 0);
    ASSERT_EQ(parent.set(AS, {0, 0}, false, 256), 0);
    EXPECT_TRUE(child.reserve(AS, 5));
}

TEST(ResourceModel, SerializedConcurrentReservationsCannotOversubscribe) {
    State state;
    ASSERT_EQ(state.set(AS, {17, 17}, false, 256), 0);
    std::mutex lock;
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < 8; ++i) {
        workers.emplace_back([&] {
            for (unsigned j = 0; j < 1000; ++j) {
                bool reserved;
                {
                    std::lock_guard guard(lock);
                    reserved = state.reserve(AS, 7);
                    EXPECT_LE(state.entries[AS].current, 17);
                }
                std::this_thread::yield();
                if (reserved) {
                    std::lock_guard guard(lock);
                    EXPECT_TRUE(state.release(AS, 7));
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(state.entries[AS].current, 0);
    EXPECT_LE(state.entries[AS].peak, 17);
}

TEST(ResourceFdTable, ReservationsAreInvisibleAndCannotBeStolen) {
    ker::mod::sched::task::FixedFdTable<4> table;
    int owner = 0;
    int other_owner = 0;
    int file = 0;
    ASSERT_EQ(table.reserve(0, 2, &owner), 0);
    EXPECT_EQ(table.lookup(0), nullptr);
    EXPECT_EQ(table.size(), 0);
    EXPECT_EQ(table.reserved_count(), 1);
    EXPECT_FALSE(table.insert(0, &file));
    EXPECT_FALSE(table.cancel(0, &other_owner));
    EXPECT_FALSE(table.commit(0, &other_owner, &file));
    EXPECT_EQ(table.reserve(0, 2, &other_owner), 1);
    EXPECT_EQ(table.reserve(0, 2, &owner), UINT64_MAX);
    unsigned inherited = 0;
    table.for_each([&](uint64_t, void*) { ++inherited; });
    EXPECT_EQ(inherited, 0);
    ASSERT_TRUE(table.commit(0, &owner, &file));
    EXPECT_FALSE(table.commit(0, &owner, &file));
    EXPECT_EQ(table.lookup(0), &file);
    EXPECT_EQ(table.size(), 1);
    ASSERT_TRUE(table.cancel(1, &other_owner));
    EXPECT_FALSE(table.cancel(1, &other_owner));
    EXPECT_EQ(table.reserved_count(), 0);
    EXPECT_EQ(table.reserve(0, 2, &owner), 1);
}

TEST(ResourceFdTable, BoundsZeroAndOneAndGrandfatheredDescriptor) {
    ker::mod::sched::task::FixedFdTable<4> table;
    int owner = 0;
    int file = 0;
    EXPECT_EQ(table.reserve(0, 0, &owner), UINT64_MAX);
    EXPECT_EQ(table.reserve(1, 1, &owner), UINT64_MAX);
    EXPECT_EQ(table.reserve(0, 4, nullptr), UINT64_MAX);
    ASSERT_TRUE(table.insert(3, &file));
    EXPECT_EQ(table.reserve(0, 1, &owner), 0);
    EXPECT_EQ(table.reserve(0, 1, &owner), UINT64_MAX);
    EXPECT_EQ(table.lookup(3), &file);
    EXPECT_EQ(table.remove(3), &file);
    EXPECT_EQ(table.remove(3), nullptr);
    ASSERT_TRUE(table.cancel(0, &owner));
    EXPECT_EQ(table.reserve(4, UINT64_MAX, &owner), UINT64_MAX);
}

TEST(ResourceFileSize, EndpointBoundsAndOverflow) {
    using ker::mod::sched::resource::file_write_budget;
    EXPECT_EQ(file_write_budget(0, 1, 0), 0);
    EXPECT_EQ(file_write_budget(8, 0, 0), 0);
    EXPECT_EQ(file_write_budget(0, 7, 8), 7);
    EXPECT_EQ(file_write_budget(0, 8, 8), 8);
    EXPECT_EQ(file_write_budget(0, 9, 8), 8);
    EXPECT_EQ(file_write_budget(7, 2, 8), 1);
    EXPECT_EQ(file_write_budget(8, 1, 8), 0);
    EXPECT_EQ(file_write_budget(9, 1, 8), 0);
    EXPECT_EQ(file_write_budget(UINT64_MAX - 1, UINT64_MAX, UINT64_MAX), 1);
    EXPECT_EQ(file_write_budget(UINT64_MAX, 1, UINT64_MAX), 0);
}

TEST(ResourceFileSize, SerializedAppendCannotExceedEndpoint) {
    using ker::mod::sched::resource::file_write_budget;
    std::mutex inode_lock;
    uint64_t extent = 0;
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < 8; ++i) {
        workers.emplace_back([&] {
            for (unsigned j = 0; j < 100; ++j) {
                std::lock_guard guard(inode_lock);
                extent += file_write_budget(extent, 7, 1024);
                EXPECT_LE(extent, 1024);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(extent, 1024);
}

TEST(ResourceExtent, DeniedSizeDoesNotCommitAndTotalsCannotOverflow) {
    constexpr unsigned CORE = static_cast<unsigned>(resource::Id::CORE);
    State state;
    ASSERT_EQ(state.set(CORE, {8, UINT64_MAX}, true, 256), 0);
    EXPECT_TRUE(state.admit_extent(CORE, 7));
    EXPECT_TRUE(state.admit_extent(CORE, 8));
    EXPECT_FALSE(state.admit_extent(CORE, 9));
    EXPECT_EQ(state.entries[CORE].current, 0);
    state.commit_extent(CORE, 8);
    ASSERT_EQ(state.set(CORE, {0, UINT64_MAX}, true, 256), 0);
    EXPECT_FALSE(state.admit_extent(CORE, 1));
    EXPECT_EQ(state.entries[CORE].current, 8);
    EXPECT_TRUE(state.admit_extent(CORE, 0));
    uint64_t total = 7;
    EXPECT_TRUE(resource::add_extent(total, 3, 5));
    EXPECT_EQ(total, 22);
    EXPECT_FALSE(resource::add_extent(total, UINT64_MAX, 2));
    EXPECT_EQ(total, 22);
    EXPECT_FALSE(resource::add_extent(total, UINT64_MAX - 21, 1));
    EXPECT_EQ(total, 22);
    EXPECT_TRUE(resource::add_extent(total, UINT64_MAX - 22, 1));
    EXPECT_EQ(total, UINT64_MAX);
    EXPECT_TRUE(resource::add_extent(total, UINT64_MAX, 0));
    EXPECT_FALSE(resource::add_extent(total, 1, 1));
}

TEST(ResourceModel, StackBackingRoundsDownWithoutOverflow) {
    constexpr uint64_t PAGE = 4096;
    constexpr uint64_t CAPACITY = 64 * 1024 * 1024;
    EXPECT_EQ(resource::stack_backing_budget(0, CAPACITY, PAGE), 0);
    EXPECT_EQ(resource::stack_backing_budget(PAGE - 1, CAPACITY, PAGE), 0);
    EXPECT_EQ(resource::stack_backing_budget(PAGE, CAPACITY, PAGE), PAGE);
    EXPECT_EQ(resource::stack_backing_budget(PAGE + 1, CAPACITY, PAGE), PAGE);
    EXPECT_EQ(resource::stack_backing_budget(UINT64_MAX, CAPACITY, PAGE), CAPACITY);
    EXPECT_EQ(resource::stack_backing_budget(UINT64_MAX, UINT64_MAX, PAGE), UINT64_MAX - (PAGE - 1));
}

TEST(ResourceCpu, ZeroBoundaryAndSoftSignalCadence) {
    constexpr unsigned CPU = static_cast<unsigned>(resource::Id::CPU);
    State state;
    ASSERT_EQ(state.set(CPU, {0, 3}, true, 256), 0);
    EXPECT_EQ(state.charge_cpu(0), resource::CpuAction::NONE);
    EXPECT_EQ(state.charge_cpu(1), resource::CpuAction::SOFT_SIGNAL);
    EXPECT_EQ(state.charge_cpu(999998), resource::CpuAction::NONE);
    EXPECT_EQ(state.charge_cpu(1), resource::CpuAction::SOFT_SIGNAL);
    EXPECT_EQ(state.entries[CPU].current, 1);
    EXPECT_EQ(state.entries[CPU].denied, 2);
    EXPECT_EQ(state.charge_cpu(1000000), resource::CpuAction::SOFT_SIGNAL);
    EXPECT_EQ(state.charge_cpu(1000000), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(state.entries[CPU].reason, resource::Denial::HARD_LIMIT);
    EXPECT_EQ(state.entries[CPU].denied, 4);
    EXPECT_EQ(state.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(state.entries[CPU].denied, 4);
}

TEST(ResourceCpu, ExactFiniteBoundaryAndHardZero) {
    constexpr unsigned CPU = static_cast<unsigned>(resource::Id::CPU);
    State state;
    ASSERT_EQ(state.set(CPU, {1, 2}, true, 256), 0);
    EXPECT_EQ(state.charge_cpu(999999), resource::CpuAction::NONE);
    EXPECT_EQ(state.charge_cpu(1), resource::CpuAction::SOFT_SIGNAL);
    EXPECT_EQ(state.charge_cpu(999999), resource::CpuAction::NONE);
    EXPECT_EQ(state.charge_cpu(1), resource::CpuAction::HARD_KILL);
    State zero;
    ASSERT_EQ(zero.set(CPU, {0, 0}, true, 256), 0);
    EXPECT_EQ(zero.charge_cpu(1), resource::CpuAction::HARD_KILL);
}

TEST(ResourceCpu, ForkResetsConsumptionAndOverflowCannotWrap) {
    constexpr unsigned CPU = static_cast<unsigned>(resource::Id::CPU);
    State parent;
    EXPECT_EQ(parent.charge_cpu(1234567), resource::CpuAction::NONE);
    State child;
    child.inherit_limits(parent);
    EXPECT_EQ(child.cpu_time_us, 0);
    EXPECT_EQ(child.entries[CPU].current, 0);
    EXPECT_EQ(child.charge_cpu(UINT64_MAX), resource::CpuAction::NONE);
    EXPECT_EQ(child.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(child.cpu_time_us, UINT64_MAX);
    EXPECT_EQ(child.entries[CPU].reason, resource::Denial::OVERFLOW);
    EXPECT_EQ(parent.cpu_time_us, 1234567);
}

TEST(ResourceCpu, ConcurrentConsumersShareOneHardThreshold) {
    constexpr unsigned CPU = static_cast<unsigned>(resource::Id::CPU);
    State state;
    ASSERT_EQ(state.set(CPU, {1, 1}, true, 256), 0);
    std::mutex lock;
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < 8; ++i) {
        workers.emplace_back([&] {
            for (unsigned j = 0; j < 125; ++j) {
                std::lock_guard guard(lock);
                static_cast<void>(state.charge_cpu(1000));
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(state.cpu_time_us, 1000000);
    EXPECT_EQ(state.entries[CPU].current, 1);
    EXPECT_EQ(state.entries[CPU].denied, 1);
    EXPECT_EQ(state.entries[CPU].reason, resource::Denial::HARD_LIMIT);
}

TEST(ResourceAddressSpace, GrowthReplacementAndLowering) {
    constexpr unsigned AS = static_cast<unsigned>(resource::Id::AS);
    State state;
    ASSERT_EQ(state.set(AS, {8192, UINT64_MAX}, true, 256), 0);
    EXPECT_TRUE(state.admit_address_space(4096, 0, 4096));
    EXPECT_FALSE(state.admit_address_space(4096, 0, 4097));
    EXPECT_EQ(state.entries[AS].current, 0);  // admission alone owns no charge
    EXPECT_EQ(state.entries[AS].denied, 1);
    EXPECT_TRUE(state.admit_address_space(8192, 4096, 4096));
    EXPECT_FALSE(state.admit_address_space(8192, 4096, 8192));
    ASSERT_EQ(state.set(AS, {0, UINT64_MAX}, true, 256), 0);
    EXPECT_TRUE(state.admit_address_space(8192, 4096, 4096));
    EXPECT_TRUE(state.admit_address_space(8192, 4096, 0));
    EXPECT_FALSE(state.admit_address_space(8192, 0, 1));
    EXPECT_FALSE(state.admit_address_space(0, 0, 1));
}

TEST(ResourceAddressSpace, OverflowAndInvalidReplacementDoNotCharge) {
    constexpr unsigned AS = static_cast<unsigned>(resource::Id::AS);
    State state;
    EXPECT_TRUE(state.admit_address_space(0, 0, UINT64_MAX));
    EXPECT_FALSE(state.admit_address_space(UINT64_MAX, 0, 1));
    EXPECT_EQ(state.entries[AS].reason, resource::Denial::OVERFLOW);
    EXPECT_FALSE(state.admit_address_space(4096, 8192, 0));
    EXPECT_EQ(state.entries[AS].denied, 2);
    EXPECT_EQ(state.entries[AS].current, 0);
    EXPECT_EQ(state.entries[AS].peak, 0);
}

TEST(ServiceReportModel, CumulativeDeltasDuplicatesAndFinality) {
    resource::State totals;
    resource::ServiceReportState report;
    bool incomplete = false;
    resource::RemoteServiceObservation observation;
    observation.cpu_time_us = 100;
    observation.members = observation.live_processes = 1;
    observation.usage[6].current = 1024;
    observation.usage[6].peak = 4096;
    observation.usage[6].denied = 1;
    observation.usage[6].reason = resource::Denial::SOFT_LIMIT;
    EXPECT_EQ(report.apply(1, observation, false, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(report.apply(1, observation, false, totals, incomplete), resource::ServiceReportResult::DUPLICATE);
    EXPECT_EQ(totals.cpu_time_us, 100);
    EXPECT_EQ(totals.entries[AS].denied, 1);
    EXPECT_EQ(totals.entries[AS].current, 0);
    observation.cpu_time_us = 150;
    observation.usage[6].denied = 3;
    observation.usage[6].current = 2048;
    observation.usage[6].peak = 8192;
    EXPECT_EQ(report.apply(2, observation, false, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(totals.cpu_time_us, 150);
    EXPECT_EQ(totals.entries[AS].denied, 3);
    EXPECT_EQ(totals.entries[AS].peak, 8192);
    EXPECT_EQ(report.last.usage[6].current, 2048);
    EXPECT_EQ(report.apply(3, observation, true, totals, incomplete), resource::ServiceReportResult::INVALID);
    observation.members = observation.live_processes = 0;
    observation.usage[6].current = 0;
    EXPECT_EQ(report.apply(3, observation, true, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(report.apply(3, observation, true, totals, incomplete), resource::ServiceReportResult::DUPLICATE);
    EXPECT_EQ(report.apply(4, observation, false, totals, incomplete), resource::ServiceReportResult::INVALID);
    EXPECT_TRUE(report.finished);
    EXPECT_FALSE(incomplete);
}

TEST(ServiceReportModel, MalformedAndResetCountersLeaveDestinationUnchanged) {
    resource::State totals;
    resource::ServiceReportState report;
    bool incomplete = false;
    resource::RemoteServiceObservation observation;
    observation.cpu_time_us = 100;
    observation.usage[6].peak = 4096;
    observation.usage[6].denied = 2;
    ASSERT_EQ(report.apply(1, observation, false, totals, incomplete), resource::ServiceReportResult::APPLIED);
    auto reject = [&](resource::RemoteServiceObservation bad, bool final = false) {
        EXPECT_EQ(report.apply(2, bad, final, totals, incomplete), resource::ServiceReportResult::INVALID);
        EXPECT_EQ(report.sequence, 1);
        EXPECT_EQ(report.last.cpu_time_us, 100);
        EXPECT_EQ(totals.cpu_time_us, 100);
        EXPECT_EQ(totals.entries[AS].peak, 4096);
        EXPECT_EQ(totals.entries[AS].denied, 2);
        EXPECT_FALSE(incomplete);
    };
    auto bad = observation;
    bad.cpu_time_us--;
    reject(bad);
    bad = observation;
    bad.usage[6].denied--;
    reject(bad);
    bad = observation;
    bad.usage[6].peak--;
    reject(bad);
    bad = observation;
    bad.usage[6].current = 8192;
    reject(bad);
    bad = observation;
    bad.flags = 2;
    reject(bad);
    bad = observation;
    bad.usage[0].current = 1;
    reject(bad);
    bad = observation;
    bad.usage[6].reserved = 1;
    reject(bad);
    bad = observation;
    bad.pending_remote_children = 1;
    reject(bad, true);
}

TEST(ServiceReportModel, ConcurrentStreamsSaturateAndPreserveIncompleteHistory) {
    resource::State totals;
    totals.entries[AS].denied = UINT64_MAX - 1;
    resource::ServiceReportState first;
    resource::ServiceReportState second;
    bool incomplete = false;
    resource::RemoteServiceObservation observation;
    observation.cpu_time_us = 100;
    observation.usage[6].denied = 2;
    observation.flags = ker::abi::service_resource::ACCOUNTING_INCOMPLETE;
    EXPECT_EQ(first.apply(1, observation, true, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(second.apply(1, observation, false, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(totals.cpu_time_us, 200);
    EXPECT_EQ(totals.entries[AS].denied, UINT64_MAX);
    EXPECT_TRUE(incomplete);
    observation.flags = 0;
    EXPECT_EQ(second.apply(2, observation, true, totals, incomplete), resource::ServiceReportResult::INVALID);
    EXPECT_EQ(second.sequence, 1);
}
