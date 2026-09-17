#include <gtest/gtest.h>

#include <cstring>

#include "net/wki/resource_contract_model.hpp"
#include "net/wki/service_contract.hpp"

namespace resource = ker::mod::sched::resource;
namespace contract = ker::net::wki::resource_contract;
using ker::net::wki::TaskResourceSnapshot;
constexpr unsigned CPU = static_cast<unsigned>(resource::Id::CPU);
constexpr unsigned AS = static_cast<unsigned>(resource::Id::AS);
constexpr unsigned NOFILE = static_cast<unsigned>(resource::Id::NOFILE);

TEST(WkiResourceContract, RoundTripPreservesEveryLimitAndLifetimeObservation) {
    resource::State source;
    for (unsigned id = 0; id < source.entries.size(); ++id) {
        ASSERT_EQ(source.set(id, {id, id + 1}, true, 256), 0);
        source.commit_extent(id, id + 2);
        source.deny(id, resource::Denial::SOFT_LIMIT);
    }
    source.cpu_time_us = 4000000;
    source.cpu_system_time_us = 4000000;
    source.commit_extent(CPU, 4);
    source.cpu_last_soft_second = 3;
    source.cpu_hard_notified = true;
    auto const RECORD = contract::snapshot(source);
    resource::State receiver;
    ASSERT_TRUE(contract::restore(RECORD, receiver, 256));
    for (unsigned id = 0; id < source.entries.size(); ++id) {
        EXPECT_EQ(receiver.entries[id].limit.soft, source.entries[id].limit.soft);
        EXPECT_EQ(receiver.entries[id].limit.hard, source.entries[id].limit.hard);
        EXPECT_EQ(receiver.entries[id].current, id == CPU ? 4 : 0);
        EXPECT_EQ(receiver.entries[id].peak, source.entries[id].peak);
        EXPECT_EQ(receiver.entries[id].denied, 1);
        EXPECT_EQ(receiver.entries[id].reason, resource::Denial::SOFT_LIMIT);
    }
    EXPECT_EQ(receiver.cpu_time_us, 4000000);
    EXPECT_EQ(receiver.cpu_last_soft_second, 3);
    EXPECT_TRUE(receiver.cpu_hard_notified);
}

TEST(WkiResourceContract, RejectsUnknownAndMalformedRecordsWithoutMutation) {
    resource::State source;
    auto const GOOD = contract::snapshot(source);
    auto reject = [&](TaskResourceSnapshot bad) {
        resource::State destination;
        ASSERT_TRUE(destination.reserve(AS, 123));
        auto const BEFORE = contract::snapshot(destination);
        EXPECT_FALSE(contract::restore(bad, destination, 256));
        auto const AFTER = contract::snapshot(destination);
        EXPECT_EQ(std::memcmp(&BEFORE, &AFTER, sizeof(BEFORE)), 0);
    };
    auto bad = GOOD;
    bad.version++;
    reject(bad);
    bad = GOOD;
    bad.count--;
    reject(bad);
    bad = GOOD;
    bad.flags = 0x8000;
    reject(bad);
    bad = GOOD;
    bad.reserved = 1;
    reject(bad);
    bad = GOOD;
    bad.entries[AS].reserved = 1;
    reject(bad);
    bad = GOOD;
    bad.entries[AS].limit = {2, 1};
    reject(bad);
    bad = GOOD;
    bad.entries[AS].current = 1;
    reject(bad);
    bad = GOOD;
    bad.entries[AS].reason = static_cast<resource::Denial>(4);
    reject(bad);
    bad = GOOD;
    bad.entries[NOFILE].limit = {256, 257};
    reject(bad);
    bad = GOOD;
    bad.cpu_time_us = 1000000;
    reject(bad);
    bad = GOOD;
    bad.cpu_last_soft_second = 1;
    reject(bad);
}

TEST(WkiResourceContract, CpuSoftEpochAndHardExhaustionSurviveMigration) {
    resource::State source;
    ASSERT_EQ(source.set(CPU, {1, 2}, true, 256), 0);
    EXPECT_EQ(source.charge_cpu(1000000), resource::CpuAction::SOFT_SIGNAL);
    resource::State receiver;
    ASSERT_TRUE(contract::restore(contract::snapshot(source), receiver, 256));
    EXPECT_EQ(receiver.charge_cpu(999999), resource::CpuAction::NONE);
    EXPECT_EQ(receiver.charge_cpu(1), resource::CpuAction::HARD_KILL);
    resource::State third;
    ASSERT_TRUE(contract::restore(contract::snapshot(receiver), third, 256));
    EXPECT_EQ(third.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(third.entries[CPU].denied, 2);
    EXPECT_EQ(third.cpu_time_us, 2000001);
}

TEST(WkiResourceContract, InfinityAndLoweredLimitsPreserveGrandfatheredPeaks) {
    resource::State source;
    ASSERT_TRUE(source.reserve(AS, UINT64_MAX));
    ASSERT_EQ(source.set(AS, {0, UINT64_MAX}, true, 256), 0);
    resource::State receiver;
    ASSERT_TRUE(contract::restore(contract::snapshot(source), receiver, 256));
    EXPECT_EQ(receiver.entries[AS].current, 0);
    EXPECT_EQ(receiver.entries[AS].peak, UINT64_MAX);
    EXPECT_FALSE(receiver.reserve(AS, 1));
    EXPECT_TRUE(receiver.reserve(AS, 0));
    EXPECT_EQ(receiver.entries[AS].denied, 1);
}

TEST(WkiResourceContract, CompletionRejectsRegressingLifetimeCounters) {
    resource::State source;
    EXPECT_EQ(source.charge_cpu(100), resource::CpuAction::NONE);
    ASSERT_TRUE(source.reserve(AS, 4096));
    source.deny(AS, resource::Denial::SOFT_LIMIT);
    auto const BASELINE = contract::snapshot(source);
    resource::State receiver;
    ASSERT_TRUE(contract::restore(BASELINE, receiver, 256));
    EXPECT_EQ(receiver.charge_cpu(200), resource::CpuAction::NONE);
    auto const COMPLETE = contract::snapshot(receiver);
    EXPECT_TRUE(contract::valid_completion(BASELINE, COMPLETE, 256));
    auto bad = COMPLETE;
    bad.cpu_time_us = 99;
    EXPECT_FALSE(contract::valid_completion(BASELINE, bad, 256));
    bad = COMPLETE;
    bad.entries[AS].peak = 4095;
    EXPECT_FALSE(contract::valid_completion(BASELINE, bad, 256));
    bad = COMPLETE;
    bad.entries[AS].denied = 0;
    EXPECT_FALSE(contract::valid_completion(BASELINE, bad, 256));
}

TEST(WkiResourceContract, UnalignedPacketCopyHasStableWireOffsets) {
    resource::State source;
    auto const SENT = contract::snapshot(source);
    std::array<unsigned char, sizeof(SENT) + 1> packet{};
    std::memcpy(packet.data() + 1, &SENT, sizeof(SENT));
    TaskResourceSnapshot received;
    ASSERT_TRUE(contract::decode(packet.data() + 1, sizeof(received), received, 256));
    EXPECT_TRUE(contract::valid(received, 256));
    EXPECT_EQ(received.entries[NOFILE].limit.hard, 256);
}

TEST(WkiResourceContract, DecoderRejectsTruncatedExtendedAndNullPacketsAtomically) {
    resource::State source;
    auto const RECORD = contract::snapshot(source);
    auto destination = RECORD;
    destination.cpu_time_us = 99;
    for (size_t size : {size_t{0}, sizeof(RECORD) - 1, sizeof(RECORD) + 1}) {
        EXPECT_FALSE(contract::decode(&RECORD, size, destination, 256));
        EXPECT_EQ(destination.cpu_time_us, 99);
    }
    EXPECT_FALSE(contract::decode(nullptr, sizeof(RECORD), destination, 256));
    EXPECT_EQ(destination.cpu_time_us, 99);
    auto invalid = RECORD;
    invalid.version++;
    EXPECT_FALSE(contract::decode(&invalid, sizeof(invalid), destination, 256));
    EXPECT_EQ(destination.cpu_time_us, 99);
}

TEST(WkiResourceContract, CpuAdmissionRejectsZeroAndExactHardCeiling) {
    resource::State source;
    EXPECT_FALSE(contract::cpu_exhausted(contract::snapshot(source)));
    ASSERT_EQ(source.set(CPU, {0, 0}, true, 256), 0);
    EXPECT_TRUE(contract::cpu_exhausted(contract::snapshot(source)));
    ASSERT_EQ(source.set(CPU, {0, 1}, true, 256), 0);
    EXPECT_EQ(source.charge_cpu(999999), resource::CpuAction::SOFT_SIGNAL);
    EXPECT_FALSE(contract::cpu_exhausted(contract::snapshot(source)));
    EXPECT_EQ(source.charge_cpu(1), resource::CpuAction::HARD_KILL);
    EXPECT_TRUE(contract::cpu_exhausted(contract::snapshot(source)));
    // A legitimate privileged raise can permit execution again even though
    // the historical hard-notified observation is retained.
    ASSERT_EQ(source.set(CPU, {UINT64_MAX, UINT64_MAX}, true, 256), 0);
    EXPECT_FALSE(contract::cpu_exhausted(contract::snapshot(source)));
}

TEST(WkiResourceContract, CompletionAddsOnlyRemoteDeltasAndPreservesLocalCharges) {
    resource::State home;
    EXPECT_EQ(home.charge_cpu(100), resource::CpuAction::NONE);
    ASSERT_TRUE(home.reserve(AS, 4096));
    home.deny(AS, resource::Denial::SOFT_LIMIT);
    auto const BASELINE = contract::snapshot(home);
    resource::State remote;
    ASSERT_TRUE(contract::restore(BASELINE, remote, 256));
    EXPECT_EQ(remote.charge_cpu(300), resource::CpuAction::NONE);
    ASSERT_TRUE(remote.reserve(AS, 8192));
    remote.deny(AS, resource::Denial::SOFT_LIMIT);
    ASSERT_EQ(remote.set(NOFILE, {10, 10}, true, 256), 0);
    EXPECT_EQ(home.charge_cpu(200), resource::CpuAction::NONE);
    home.deny(AS, resource::Denial::OVERFLOW);
    uint64_t delta = 0;
    ASSERT_TRUE(contract::merge_completion(BASELINE, contract::snapshot(remote), home, 256, delta));
    EXPECT_EQ(delta, 300);
    EXPECT_EQ(home.cpu_time_us, 600);
    EXPECT_EQ(home.entries[AS].current, 4096);
    EXPECT_EQ(home.entries[AS].peak, 8192);
    EXPECT_EQ(home.entries[AS].denied, 3);
    EXPECT_EQ(home.entries[AS].reason, resource::Denial::SOFT_LIMIT);
    EXPECT_EQ(home.entries[NOFILE].limit.hard, 10);
}

TEST(WkiResourceContract, CompletionSaturatesCpuAndDenialsWithoutWrapping) {
    resource::State home;
    auto const BASELINE = contract::snapshot(home);
    resource::State remote;
    EXPECT_EQ(remote.charge_cpu(UINT64_MAX), resource::CpuAction::NONE);
    remote.entries[AS].denied = UINT64_MAX;
    remote.entries[AS].reason = resource::Denial::SOFT_LIMIT;
    EXPECT_EQ(home.charge_cpu(1), resource::CpuAction::NONE);
    home.deny(AS, resource::Denial::OVERFLOW);
    uint64_t delta = 0;
    ASSERT_TRUE(contract::merge_completion(BASELINE, contract::snapshot(remote), home, 256, delta));
    EXPECT_EQ(delta, UINT64_MAX);
    EXPECT_EQ(home.cpu_time_us, UINT64_MAX);
    EXPECT_EQ(home.entries[AS].denied, UINT64_MAX);
    EXPECT_EQ(home.entries[CPU].reason, resource::Denial::OVERFLOW);
    EXPECT_TRUE(home.cpu_hard_notified);
}

TEST(WkiResourceContract, InvalidCompletionDoesNotChangeHomeOrReturnedDelta) {
    resource::State home;
    EXPECT_EQ(home.charge_cpu(100), resource::CpuAction::NONE);
    auto const BASELINE = contract::snapshot(home);
    auto bad = BASELINE;
    bad.cpu_time_us = 99;
    uint64_t delta = 123;
    EXPECT_FALSE(contract::merge_completion(BASELINE, bad, home, 256, delta));
    auto const AFTER = contract::snapshot(home);
    EXPECT_EQ(std::memcmp(&BASELINE, &AFTER, sizeof(AFTER)), 0);
    EXPECT_EQ(delta, 123);
}

TEST(WkiResourceContract, StdioPresenceFlagsSurviveUnalignedDecodeWithoutChangingCpuHistory) {
    for (uint16_t mask = 0; mask < 8; ++mask) {
        resource::State source;
        source.cpu_hard_notified = true;
        auto record = contract::snapshot(source);
        record.flags |= mask << ker::net::wki::WKI_RESOURCE_STDIO_SHIFT;
        std::array<uint8_t, sizeof(record) + 1> bytes{};
        std::memcpy(bytes.data() + 1, &record, sizeof(record));
        TaskResourceSnapshot decoded;
        ASSERT_TRUE(contract::decode(bytes.data() + 1, sizeof(record), decoded, 256));
        EXPECT_EQ(decoded.flags, record.flags);
        resource::State restored;
        ASSERT_TRUE(contract::restore(decoded, restored, 256));
        EXPECT_TRUE(restored.cpu_hard_notified);
        auto completion = contract::snapshot(restored);
        EXPECT_TRUE(contract::valid_completion(decoded, completion, 256));
    }
}

TEST(WkiResourceContract, SplitCpuAndReapedDeltasSurviveNestedExecution) {
    resource::State home;
    EXPECT_EQ(home.charge_cpu(100, 40), resource::CpuAction::NONE);
    auto const BASELINE = contract::snapshot(home);
    resource::State remote;
    ASSERT_TRUE(contract::restore(BASELINE, remote, 256));
    EXPECT_EQ(remote.charge_cpu(200, 150), resource::CpuAction::NONE);
    auto const NESTED_BASELINE = contract::snapshot(remote);
    resource::State nested;
    ASSERT_TRUE(contract::restore(NESTED_BASELINE, nested, 256));
    EXPECT_EQ(nested.charge_cpu(300, 250), resource::CpuAction::NONE);
    auto nested_completion = contract::snapshot(nested);
    nested_completion.reaped_user_time_us = 17;
    nested_completion.reaped_system_time_us = 19;
    uint64_t delta = 0;
    ASSERT_TRUE(contract::merge_completion(NESTED_BASELINE, nested_completion, remote, 256, delta));
    EXPECT_EQ(delta, 300);
    auto completion = contract::snapshot(remote);
    completion.reaped_user_time_us = 11 + nested_completion.reaped_user_time_us;
    completion.reaped_system_time_us = 13 + nested_completion.reaped_system_time_us;
    EXPECT_EQ(home.charge_cpu(50, 20), resource::CpuAction::NONE);
    ASSERT_TRUE(contract::merge_completion(BASELINE, completion, home, 256, delta));
    EXPECT_EQ(delta, 500);
    EXPECT_EQ(home.cpu_time_us, 650);
    EXPECT_EQ(home.cpu_user_time_us, 460);
    EXPECT_EQ(home.cpu_system_time_us, 190);
    EXPECT_EQ(completion.reaped_user_time_us, 28);
    EXPECT_EQ(completion.reaped_system_time_us, 32);
    resource::State child;
    child.inherit_limits(home);
    EXPECT_EQ(child.cpu_time_us, 0);
    EXPECT_EQ(child.cpu_user_time_us, 0);
    EXPECT_EQ(child.cpu_system_time_us, 0);
    // Child deltas cannot be imported as a fresh submission baseline.
    auto const BEFORE = contract::snapshot(child);
    EXPECT_FALSE(contract::restore(completion, child, 256));
    auto const AFTER = contract::snapshot(child);
    EXPECT_EQ(std::memcmp(&BEFORE, &AFTER, sizeof(BEFORE)), 0);
}

TEST(WkiResourceContract, RejectsMalformedSplitAndRegressingModes) {
    resource::State home;
    EXPECT_EQ(home.charge_cpu(100, 40), resource::CpuAction::NONE);
    auto const BASELINE = contract::snapshot(home);
    auto bad = BASELINE;
    bad.cpu_system_time_us++;
    EXPECT_FALSE(contract::valid(bad, 256));
    bad = BASELINE;
    bad.cpu_user_time_us--;
    bad.cpu_system_time_us++;
    EXPECT_TRUE(contract::valid(bad, 256));
    uint64_t delta = 17;
    EXPECT_FALSE(contract::merge_completion(BASELINE, bad, home, 256, delta));
    EXPECT_EQ(delta, 17);
    EXPECT_EQ(home.cpu_user_time_us, 40);
    EXPECT_EQ(home.cpu_system_time_us, 60);
    bad = BASELINE;
    bad.reaped_user_time_us = 1;
    EXPECT_FALSE(contract::valid_completion(bad, bad, 256));
    bad = BASELINE;
    bad.version = 1;
    EXPECT_FALSE(contract::valid(bad, 256));
    EXPECT_FALSE(contract::decode(&BASELINE, 800, bad, 256));
}

TEST(WkiResourceContract, SplitCpuSaturatesWithoutWrap) {
    resource::State home;
    auto const BASELINE = contract::snapshot(home);
    resource::State remote;
    EXPECT_EQ(remote.charge_cpu(UINT64_MAX, UINT64_MAX - 1), resource::CpuAction::NONE);
    EXPECT_EQ(remote.charge_cpu(10, 5), resource::CpuAction::HARD_KILL);
    EXPECT_EQ(remote.cpu_user_time_us, UINT64_MAX);
    EXPECT_EQ(remote.cpu_system_time_us, 6);
    auto const COMPLETION = contract::snapshot(remote);
    EXPECT_TRUE(contract::valid(COMPLETION, 256));
    EXPECT_EQ(home.charge_cpu(10, 7), resource::CpuAction::NONE);
    uint64_t delta = 0;
    ASSERT_TRUE(contract::merge_completion(BASELINE, COMPLETION, home, 256, delta));
    EXPECT_EQ(home.cpu_time_us, UINT64_MAX);
    EXPECT_EQ(home.cpu_user_time_us, UINT64_MAX);
    EXPECT_EQ(home.cpu_system_time_us, 9);
    EXPECT_TRUE(contract::valid(contract::snapshot(home), 256));
}

TEST(WkiResourceContract, RejectionMergesOnlyPeaksAndDenialDeltas) {
    resource::State home;
    ASSERT_EQ(home.set(AS, {4096, 8192}, true, 256), 0);
    home.commit_extent(AS, 123);
    home.deny(AS, resource::Denial::SOFT_LIMIT);
    static_cast<void>(home.charge_cpu(100, 40));
    auto const BASELINE = contract::snapshot(home);
    resource::State receiver;
    ASSERT_TRUE(contract::restore(BASELINE, receiver, 256));
    ASSERT_TRUE(receiver.reserve(AS, 4096));
    ASSERT_FALSE(receiver.reserve(AS, 1));
    auto const REJECTION = contract::snapshot(receiver);
    // Local work and policy changes after submission remain authoritative.
    ASSERT_EQ(home.set(AS, {2048, 4096}, true, 256), 0);
    home.deny(AS, resource::Denial::OVERFLOW);
    static_cast<void>(home.charge_cpu(50, 10));
    ASSERT_TRUE(contract::merge_rejection(BASELINE, REJECTION, home, 256));
    EXPECT_EQ(home.entries[AS].limit.soft, 2048);
    EXPECT_EQ(home.entries[AS].limit.hard, 4096);
    EXPECT_EQ(home.entries[AS].current, 123);
    EXPECT_EQ(home.entries[AS].peak, 4096);
    EXPECT_EQ(home.entries[AS].denied, 3);
    EXPECT_EQ(home.entries[AS].reason, resource::Denial::SOFT_LIMIT);
    EXPECT_EQ(home.cpu_time_us, 150);
    EXPECT_EQ(home.cpu_user_time_us, 50);
    EXPECT_EQ(home.cpu_system_time_us, 100);
}

TEST(WkiResourceContract, RejectionRejectsPolicyExecutionAndCounterChangesAtomically) {
    resource::State home;
    home.deny(AS, resource::Denial::SOFT_LIMIT);
    auto const BASELINE = contract::snapshot(home);
    auto reject = [&](TaskResourceSnapshot bad) {
        auto const BEFORE = contract::snapshot(home);
        EXPECT_FALSE(contract::merge_rejection(BASELINE, bad, home, 256));
        auto const AFTER = contract::snapshot(home);
        EXPECT_EQ(std::memcmp(&BEFORE, &AFTER, sizeof(BEFORE)), 0);
    };
    auto bad = BASELINE;
    bad.entries[AS].limit.soft--;
    reject(bad);
    bad = BASELINE;
    bad.cpu_time_us = bad.cpu_system_time_us = 1;
    reject(bad);
    bad = BASELINE;
    bad.reaped_user_time_us = 1;
    reject(bad);
    bad = BASELINE;
    bad.flags |= ker::net::wki::WKI_RESOURCE_CPU_HARD_NOTIFIED;
    reject(bad);
    bad = BASELINE;
    bad.entries[AS].denied = 0;
    reject(bad);
    bad = BASELINE;
    bad.version++;
    reject(bad);
}

TEST(WkiResourceContract, RejectionDenialMergeSaturates) {
    resource::State home;
    auto const BASELINE = contract::snapshot(home);
    auto rejection = BASELINE;
    rejection.entries[NOFILE].denied = 9;
    rejection.entries[NOFILE].reason = resource::Denial::SOFT_LIMIT;
    home.entries[NOFILE].denied = UINT64_MAX - 2;
    ASSERT_TRUE(contract::merge_rejection(BASELINE, rejection, home, 256));
    EXPECT_EQ(home.entries[NOFILE].denied, UINT64_MAX);
}

TEST(WkiResourceContract, ServiceReportFlagDoesNotAlterProcessPolicy) {
    resource::State source;
    auto baseline = contract::snapshot(source);
    baseline.flags |= ker::net::wki::WKI_RESOURCE_SERVICE_REPORTS;
    resource::State receiver;
    ASSERT_TRUE(contract::restore(baseline, receiver, 256));
    auto const COMPLETION = contract::snapshot(receiver);
    EXPECT_TRUE(contract::valid_completion(baseline, COMPLETION, 256));
    EXPECT_EQ(COMPLETION.flags & ker::net::wki::WKI_RESOURCE_SERVICE_REPORTS, 0);
    EXPECT_EQ(receiver.cpu_time_us, source.cpu_time_us);
    for (unsigned id = 0; id < source.entries.size(); ++id) {
        EXPECT_EQ(receiver.entries[id].limit.soft, source.entries[id].limit.soft);
        EXPECT_EQ(receiver.entries[id].limit.hard, source.entries[id].limit.hard);
    }
}

TEST(WkiServiceContract, UnalignedExactWireAndFinalFlags) {
    namespace service = ker::net::wki::service_contract;
    resource::RemoteServiceObservation source{};
    source.cpu_time_us = 2300000;
    source.usage[0].current = 2;
    source.usage[0].peak = 2;
    source.flags = ker::abi::service_resource::ACCOUNTING_INCOMPLETE;
    auto report = service::encode(17, 4, source, true);
    EXPECT_EQ(sizeof(report), 384U);
    EXPECT_EQ(report.flags, ker::net::wki::WKI_SERVICE_REPORT_FINAL | ker::net::wki::WKI_SERVICE_REPORT_INCOMPLETE);
    std::array<uint8_t, sizeof(report) + 1> bytes{};
    std::memcpy(bytes.data() + 1, &report, sizeof(report));
    ker::net::wki::TaskServiceReport decoded{};
    resource::RemoteServiceObservation observation{};
    ASSERT_TRUE(service::decode(bytes.data() + 1, sizeof(report), decoded, observation));
    EXPECT_EQ(decoded.task_id, 17U);
    EXPECT_EQ(decoded.sequence, 4U);
    EXPECT_EQ(observation.cpu_time_us, 2300000U);
    EXPECT_EQ(observation.flags, source.flags);
    resource::ServiceReportState state{};
    resource::State totals{};
    bool incomplete = false;
    EXPECT_EQ(state.apply(decoded.sequence, observation, true, totals, incomplete), resource::ServiceReportResult::APPLIED);
    EXPECT_EQ(totals.cpu_time_us, 2300000U);
    EXPECT_TRUE(incomplete);
    EXPECT_FALSE(service::decode(nullptr, sizeof(report), decoded, observation));
    EXPECT_FALSE(service::decode(bytes.data() + 1, sizeof(report) - 1, decoded, observation));
    EXPECT_FALSE(service::decode(bytes.data(), sizeof(report) + 1, decoded, observation));
    for (unsigned invalid = 0; invalid < 4; ++invalid) {
        auto bad = report;
        if (invalid == 0) bad.flags |= 4;
        if (invalid == 1) bad.version++;
        if (invalid == 2) bad.task_id = 0;
        if (invalid == 3) bad.sequence = 0;
        std::memcpy(bytes.data() + 1, &bad, sizeof(bad));
        EXPECT_FALSE(service::decode(bytes.data() + 1, sizeof(bad), decoded, observation));
    }
}

TEST(WkiResourceContract, ImportedChildObservationsRequireExplicitSubmissionOutput) {
    resource::State source;
    auto record = contract::snapshot(source);
    record.reaped_user_time_us = 17;
    record.reaped_system_time_us = 19;
    resource::State receiver;
    receiver.commit_extent(AS, 123);
    auto const BEFORE = contract::snapshot(receiver);
    ker::net::wki::TaskChildCpuObservation observations{31, 37};
    EXPECT_FALSE(contract::restore(record, receiver, 256, &observations));
    EXPECT_EQ(observations.user_time_us, 31);
    record.flags |= ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
    EXPECT_FALSE(contract::restore(record, receiver, 256));
    auto const UNCHANGED = contract::snapshot(receiver);
    EXPECT_EQ(std::memcmp(&BEFORE, &UNCHANGED, sizeof(BEFORE)), 0);
    ASSERT_TRUE(contract::restore(record, receiver, 256, &observations));
    EXPECT_EQ(observations.user_time_us, 17);
    EXPECT_EQ(observations.system_time_us, 19);
    auto const RESTORED = contract::snapshot(receiver);
    EXPECT_EQ(RESTORED.reaped_user_time_us, 0);
    EXPECT_EQ(RESTORED.reaped_system_time_us, 0);
    EXPECT_EQ(RESTORED.flags & ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS, 0);
    auto invalid = record;
    invalid.reserved = 1;
    EXPECT_FALSE(contract::restore(invalid, receiver, 256, &observations));
    EXPECT_EQ(observations.user_time_us, 17);
    EXPECT_EQ(observations.system_time_us, 19);
}

TEST(WkiResourceContract, CompletionAndRejectionNeverReturnImportedObservations) {
    resource::State source;
    auto baseline = contract::snapshot(source);
    baseline.flags |= ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
    baseline.reaped_user_time_us = UINT64_MAX;
    baseline.reaped_system_time_us = 19;
    auto completion = contract::snapshot(source);
    EXPECT_TRUE(contract::valid_rejection(baseline, completion, 256));
    completion.reaped_user_time_us = 5;
    completion.reaped_system_time_us = 7;
    EXPECT_TRUE(contract::valid_completion(baseline, completion, 256));
    EXPECT_FALSE(contract::valid_rejection(baseline, completion, 256));
    completion.flags |= ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
    EXPECT_FALSE(contract::valid_completion(baseline, completion, 256));
    EXPECT_FALSE(contract::valid_rejection(baseline, completion, 256));
    baseline.flags &= ~ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
    completion.flags &= ~ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
    EXPECT_FALSE(contract::valid_completion(baseline, completion, 256));
}
