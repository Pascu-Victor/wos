#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <functional>
#include <platform/ktime/kvmclock_policy.hpp>
#include <thread>

namespace pv = ker::mod::time::kvmclock;
namespace policy = ker::mod::time::clocksource;

namespace {
struct Fake {
    pv::TimeInfo page{.version = 2, .tsc_timestamp = 100, .system_time = 1000000000, .multiplier = 0x80000000, .shift = 1, .flags = 1};
    uint64_t tsc{100};
    uint64_t hpet{1000000};
    uint64_t reference_span{};
    unsigned references{};
    unsigned counters{};
    unsigned allocations{};
    unsigned registrations{};
    bool features{true};
    bool oom{};
    bool corrupt_on_register{};
    std::function<void()> counter_hook{};
    std::function<void()> reference_hook{};

    auto available() -> bool { return features; }
    auto allocate() -> pv::TimeInfo* {
        ++allocations;
        return oom ? nullptr : &page;
    }
    void register_page(pv::TimeInfo& record) {
        ++registrations;
        if (corrupt_on_register) record.flags = 0;
    }
    auto counter() -> uint64_t {
        ++counters;
        if (counter_hook) {
            auto hook = std::move(counter_hook);
            hook();
        }
        return tsc;
    }
    auto reference() -> uint64_t {
        if (reference_hook) {
            auto hook = std::move(reference_hook);
            hook();
        }
        return hpet + ((references++ % 2) == 0 ? 0 : reference_span);
    }
    void advance(uint64_t ns) {
        tsc += ns;
        hpet += ns;
    }
};
}  // namespace

TEST(KvmClock, RequiresExactSignatureAndModernStableFeature) {
    EXPECT_TRUE(pv::supported(0x40000001, 0x4b4d564b, 0x564b4d56, 0x4d, pv::REQUIRED_FEATURES));
    EXPECT_TRUE(pv::supported(0, 0x4b4d564b, 0x564b4d56, 0x4d, pv::REQUIRED_FEATURES));
    EXPECT_FALSE(pv::supported(0x40000000, 0x4b4d564b, 0x564b4d56, 0x4d, pv::REQUIRED_FEATURES));
    EXPECT_FALSE(pv::supported(0x40000001, 0x4b4d564a, 0x564b4d56, 0x4d, pv::REQUIRED_FEATURES));
    EXPECT_FALSE(pv::supported(0x40000001, 0x4b4d564b, 0x564b4d56, 0x4d, 1U << 3));
    EXPECT_FALSE(pv::supported(0x40000001, 0x4b4d564b, 0x564b4d56, 0x4d, 1U << 24));
}

TEST(KvmClock, ConvertsSignedShiftsWithoutWrapOrTrapping) {
    uint64_t result = 0;
    EXPECT_TRUE(pv::convert(200, 100, 1000, 0x80000000, 1, 1, result));
    EXPECT_EQ(result, 1100U);
    EXPECT_TRUE(pv::convert(204, 100, 1000, 0x80000000, -1, 1, result));
    EXPECT_EQ(result, 1026U);
    EXPECT_TRUE(pv::convert(100, 100, UINT64_MAX, 0x80000000, 1, 1, result));
    EXPECT_EQ(result, UINT64_MAX);
    EXPECT_FALSE(pv::convert(101, 100, UINT64_MAX, 0x80000000, 1, 1, result));
    EXPECT_FALSE(pv::convert(99, 100, 0, 1, 0, 1, result));
    EXPECT_FALSE(pv::convert(UINT64_MAX, 0, 0, UINT32_MAX, 31, 1, result));
    EXPECT_FALSE(pv::convert(1, 0, 0, 1, 32, 1, result));
    EXPECT_FALSE(pv::convert(1, 0, 0, 1, -32, 1, result));
    EXPECT_FALSE(pv::convert(1, 0, 0, 0, 0, 1, result));
    EXPECT_FALSE(pv::convert(1, 0, 0, 1, 0, 2, result));
    EXPECT_TRUE(pv::convert(1, 0, 0, UINT32_MAX, 0, 3, result));
}

TEST(KvmClock, RejectsIncompleteOrUnstableSnapshotsWithoutUnboundedWait) {
    Fake fake;
    uint64_t result = 0;
    fake.page.version = 0;
    EXPECT_FALSE(pv::sample(fake.page, [&] { return fake.counter(); }, result));
    EXPECT_EQ(fake.counters, 0U);
    fake.page.version = 3;
    EXPECT_FALSE(pv::sample(fake.page, [&] { return fake.counter(); }, result));
    EXPECT_EQ(fake.counters, 0U);
    fake.page.version = 2;
    EXPECT_FALSE(pv::sample(
        fake.page,
        [&] {
            fake.page.version += 2;
            return fake.counter();
        },
        result));
    EXPECT_EQ(fake.counters, pv::SNAPSHOT_ATTEMPTS);
    EXPECT_TRUE(pv::sample(fake.page, [&] { return fake.counter(); }, result));
    EXPECT_EQ(result, 1000000000U);
    fake.page.flags = 0;
    EXPECT_FALSE(pv::sample(fake.page, [&] { return fake.counter(); }, result));
}

TEST(KvmClock, RetriesChangedVersionAndUsesEntireNewScale) {
    Fake fake;
    uint64_t result = 0;
    EXPECT_TRUE(pv::sample(
        fake.page,
        [&] {
            if (fake.counters == 0) {
                fake.page.version = 4;
                fake.page.system_time += 1000;
                fake.page.tsc_timestamp += 1000;
                fake.tsc += 1000;
            }
            return fake.counter();
        },
        result));
    EXPECT_EQ(fake.counters, 2U);
    EXPECT_EQ(result, 1000001000U);
}

TEST(KvmClock, StaysOffForMissingFeaturesOrUnsupportedCpuCount) {
    for (uint64_t cpus : {0ULL, 257ULL}) {
        pv::Runtime runtime;
        Fake fake;
        runtime.prepare(true, cpus);
        runtime.register_cpu(0, true, fake);
        EXPECT_EQ(runtime.mode(), pv::Mode::off);
        EXPECT_EQ(fake.allocations, 0U);
    }
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(false, 8);
    runtime.register_cpu(0, true, fake);
    EXPECT_EQ(runtime.mode(), pv::Mode::off);
    EXPECT_EQ(fake.registrations, 0U);
}

TEST(KvmClock, NeedsFullCpuQuorumAndIndependentPerCpuOffsets) {
    pv::Runtime runtime;
    Fake bsp;
    Fake ap;
    runtime.prepare(true, 2);
    runtime.register_cpu(0, true, bsp);
    EXPECT_EQ(runtime.mode(), pv::Mode::qualifying);
    EXPECT_EQ(runtime.registered(), 1U);
    uint64_t result = 0;
    EXPECT_FALSE(runtime.read(0, false, bsp, result));
    // Very different raw CPU origins are safe only because the host's private
    // conversion record supplies the matching offset, not because invariant
    // TSC proves synchronization.
    ap.page.tsc_timestamp = 90000000000;
    ap.tsc = ap.page.tsc_timestamp;
    ap.page.system_time += 2000;
    ap.hpet += 2000;
    runtime.register_cpu(1, false, ap);
    EXPECT_EQ(runtime.mode(), pv::Mode::active);
    EXPECT_EQ(runtime.registered(), 2U);
    EXPECT_EQ(runtime.expected(), 2U);
    EXPECT_TRUE(runtime.read(1, false, ap, result));
    EXPECT_EQ(result, 1002000U);
    runtime.register_cpu(1, false, ap);
    EXPECT_EQ(ap.allocations, 1U);
    EXPECT_EQ(runtime.registered(), 2U);
}

TEST(KvmClock, FailureCannotBeOverwrittenByLateRegistration) {
    pv::Runtime runtime;
    Fake bsp;
    Fake bad;
    Fake late;
    runtime.prepare(true, 3);
    runtime.register_cpu(0, true, bsp);
    bad.page.flags = 0;
    runtime.register_cpu(1, false, bad);
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    runtime.register_cpu(2, false, late);
    EXPECT_EQ(late.allocations, 0U);
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
}

TEST(KvmClock, BootFailureKeepsRegisteredMemoryAndNeverActivates) {
    for (unsigned fault = 0; fault < 6; ++fault) {
        pv::Runtime runtime;
        Fake fake;
        runtime.prepare(true, 1);
        if (fault == 0) fake.oom = true;
        if (fault == 1) fake.features = false;
        if (fault == 2) fake.corrupt_on_register = true;
        if (fault == 3) fake.page.version = 1;
        if (fault == 4) fake.reference_span = policy::MAX_DEVIATION_NS + 1;
        runtime.register_cpu(fault == 5 ? 1 : 0, true, fake);
        EXPECT_EQ(runtime.mode(), pv::Mode::failed) << fault;
        EXPECT_EQ(runtime.registered(), 0U) << fault;
        EXPECT_EQ(fake.registrations, fault >= 2 && fault <= 4 ? 1U : 0U) << fault;
    }
}

TEST(KvmClock, RejectsApEpochMismatchAndMissingBootstrap) {
    pv::Runtime runtime;
    Fake bsp;
    Fake ap;
    runtime.prepare(true, 2);
    runtime.register_cpu(0, true, bsp);
    ap.page.system_time += policy::MAX_DEVIATION_NS + 1;
    runtime.register_cpu(1, false, ap);
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    EXPECT_GT(runtime.deviation(), policy::MAX_DEVIATION_NS);
    pv::Runtime no_bsp;
    no_bsp.prepare(true, 2);
    no_bsp.register_cpu(1, false, ap);
    EXPECT_EQ(no_bsp.mode(), pv::Mode::failed);
}

TEST(KvmClock, FastReadsUseNoReferenceAndCheckpointUsesFreshBracket) {
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(true, 1);
    runtime.register_cpu(0, true, fake);
    EXPECT_EQ(fake.references, 2U);
    uint64_t result = 0;
    for (unsigned i = 0; i < 1000; ++i) {
        fake.advance(1);
        EXPECT_TRUE(runtime.read(0, false, fake, result));
    }
    EXPECT_EQ(fake.references, 2U);
    EXPECT_EQ(runtime.checks(), 0U);
    fake.advance(pv::WATCHDOG_INTERVAL_NS);
    EXPECT_TRUE(runtime.read(0, false, fake, result));
    EXPECT_EQ(fake.references, 4U);
    EXPECT_EQ(runtime.checks(), 1U);
    EXPECT_EQ(runtime.deviation(), 0U);
    EXPECT_TRUE(runtime.read(0, true, fake, result));
    EXPECT_EQ(fake.references, 6U);
    EXPECT_EQ(runtime.checks(), 2U);
}

TEST(KvmClock, ForcedIrqCheckDetectsFrozenClockIndependentlyOfPvTime) {
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(true, 1);
    runtime.register_cpu(0, true, fake);
    fake.hpet += 2 * policy::MAX_DEVIATION_NS;
    uint64_t result = 0;
    EXPECT_FALSE(runtime.read(0, true, fake, result));
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    EXPECT_EQ(runtime.checks(), 1U);
}

TEST(KvmClock, RejectsBackwardsUnstableAndDriftedSamplesBeforePublication) {
    for (unsigned fault = 0; fault < 5; ++fault) {
        pv::Runtime runtime;
        Fake fake;
        runtime.prepare(true, 1);
        runtime.register_cpu(0, true, fake);
        fake.advance(1000);
        uint64_t result = 0;
        ASSERT_TRUE(runtime.read(0, false, fake, result));
        if (fault == 0) fake.tsc = 99;
        if (fault == 1) fake.tsc = 100;
        if (fault == 2) fake.page.flags = 0;
        if (fault == 3) fake.page.version = 3;
        if (fault == 4) fake.tsc += pv::WATCHDOG_INTERVAL_NS;
        EXPECT_FALSE(runtime.read(0, false, fake, result)) << fault;
        EXPECT_EQ(runtime.mode(), pv::Mode::failed) << fault;
        fake.page.flags = 1;
        fake.page.version = 4;
        fake.tsc = 1000000000;
        EXPECT_FALSE(runtime.read(0, false, fake, result)) << fault;
    }
}

TEST(KvmClock, ConcurrentCpuReadersAndHpetFallbackNeverRegressGlobalClock) {
    pv::Runtime runtime;
    std::array<Fake, 4> cpus;
    runtime.prepare(true, cpus.size());
    for (unsigned cpu = 0; cpu < cpus.size(); ++cpu) runtime.register_cpu(cpu, cpu == 0, cpus.at(cpu));
    ASSERT_EQ(runtime.mode(), pv::Mode::active);
    std::atomic<uint64_t> floor{0};
    std::atomic<bool> valid{true};
    std::array<std::thread, 4> threads;
    for (unsigned cpu = 0; cpu < threads.size(); ++cpu) {
        threads.at(cpu) = std::thread([&, cpu] {
            uint64_t previous = 0;
            for (unsigned i = 0; i < 10000; ++i) {
                auto& fake = cpus.at(cpu);
                fake.advance(2000);
                uint64_t candidate = 0;
                if (!runtime.read(cpu, i % 100 == 0, fake, candidate)) valid.store(false);
                uint64_t const NOW = policy::publish_max(floor, candidate);
                if (NOW < previous) valid.store(false);
                previous = NOW;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_TRUE(valid.load());
    uint64_t const LAST = policy::publish_max(floor, 0);
    cpus.at(0).page.flags = 0;
    uint64_t rejected = 0;
    EXPECT_FALSE(runtime.read(0, false, cpus.at(0), rejected));
    EXPECT_EQ(policy::publish_max(floor, LAST - 1), LAST);
    EXPECT_EQ(policy::publish_max(floor, LAST + 1), LAST + 1);
}

TEST(KvmClock, AdjustedReferenceRateDoesNotRejectHealthyRawClockOrRebaseFastTime) {
    for (uint64_t ppm : {25ULL, 500ULL, 1000ULL}) {
        pv::Runtime runtime;
        Fake fake;
        runtime.prepare(true, 1);
        runtime.register_cpu(0, true, fake);
        uint64_t result = 0;
        for (unsigned interval = 0; interval < 500; ++interval) {
            fake.advance(pv::WATCHDOG_INTERVAL_NS);
            fake.hpet += pv::WATCHDOG_INTERVAL_NS * ppm / 1000000;
            ASSERT_TRUE(runtime.read(0, true, fake, result)) << ppm << " interval " << interval;
            EXPECT_EQ(result, 1000000 + (interval + 1ULL) * pv::WATCHDOG_INTERVAL_NS);
        }
        EXPECT_EQ(runtime.mode(), pv::Mode::active);
        EXPECT_EQ(runtime.checks(), 500U);
        // Freezing the current qualified offset maps HPET onto the healthy PV
        // epoch despite the now-large cumulative raw/adjusted difference.
        fake.page.flags = 0;
        EXPECT_FALSE(runtime.read(0, false, fake, result));
        EXPECT_EQ(runtime.fallback(fake.hpet), 5001000000U);
        fake.hpet += 1000;
        EXPECT_EQ(runtime.fallback(fake.hpet), 5001001000U);
        EXPECT_EQ(runtime.fallback(UINT64_MAX), UINT64_MAX - 5000 * ppm);
    }
}

TEST(KvmClock, PersistentBadRateAccumulatesEvidenceAcrossFrequentIrqs) {
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(true, 1);
    runtime.register_cpu(0, true, fake);
    uint64_t result = 0;
    bool rejected = false;
    for (unsigned interval = 0; interval < 20; ++interval) {
        fake.advance(pv::WATCHDOG_INTERVAL_NS);
        fake.hpet += pv::WATCHDOG_INTERVAL_NS * 5000 / 1000000;
        if (!runtime.read(0, true, fake, result)) {
            rejected = true;
            break;
        }
    }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    EXPECT_LE(runtime.checks(), 4U);
}

TEST(KvmClock, WideSamplingDelayNeverCreatesAnUnsafeFallbackOffset) {
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(true, 1);
    runtime.register_cpu(0, true, fake);
    fake.advance(pv::WATCHDOG_INTERVAL_NS);
    fake.hpet += 250;
    uint64_t result = 0;
    ASSERT_TRUE(runtime.read(0, true, fake, result));
    fake.reference_span = 1000000000;
    EXPECT_TRUE(runtime.read(0, true, fake, result));
    fake.page.flags = 0;
    EXPECT_FALSE(runtime.read(0, false, fake, result));
    EXPECT_EQ(runtime.fallback(fake.hpet), 11000000U);
    EXPECT_NE(runtime.fallback(fake.hpet), fake.hpet - 500000000);
}

TEST(KvmClock, TransitionOffsetExcludesSentinelAndDoesNotWrap) {
    int64_t offset = 0;
    EXPECT_TRUE(pv::transition_offset(120, 100, 110, offset));
    EXPECT_EQ(offset, 15);
    EXPECT_TRUE(pv::transition_offset(90, 100, 110, offset));
    EXPECT_EQ(offset, -15);
    EXPECT_FALSE(pv::transition_offset(0, UINT64_MAX, UINT64_MAX, offset));
    EXPECT_FALSE(pv::transition_offset(UINT64_MAX, 0, 0, offset));
    EXPECT_FALSE(pv::transition_offset(0, 2, 1, offset));
    EXPECT_EQ(pv::rate_tolerance(0), policy::MAX_DEVIATION_NS);
    EXPECT_EQ(pv::rate_tolerance(1000000000), 1100000U);
    EXPECT_GE(pv::rate_tolerance(UINT64_MAX), policy::MAX_DEVIATION_NS);
}

TEST(KvmClock, LateQualifiedCheckCannotChangeFrozenFallbackOffset) {
    pv::Runtime runtime;
    Fake bsp;
    Fake ap;
    runtime.prepare(true, 2);
    runtime.register_cpu(0, true, bsp);
    runtime.register_cpu(1, false, ap);
    bsp.advance(pv::WATCHDOG_INTERVAL_NS);
    bsp.hpet += 250;
    ap.page.flags = 0;
    bsp.counter_hook = [&] {
        uint64_t ignored = 0;
        EXPECT_FALSE(runtime.read(1, false, ap, ignored));
        EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    };
    uint64_t result = 0;
    EXPECT_FALSE(runtime.read(0, true, bsp, result));
    EXPECT_EQ(runtime.fallback(bsp.hpet), bsp.hpet);
    EXPECT_EQ(runtime.fallback(bsp.hpet + 1000), bsp.hpet + 1000);
}

TEST(KvmClock, NestedCheckpointWriterFallsBackWithoutWaitingOrTornAnchor) {
    pv::Runtime runtime;
    Fake fake;
    runtime.prepare(true, 1);
    runtime.register_cpu(0, true, fake);
    fake.advance(pv::RATE_WINDOW_NS);
    fake.reference_hook = [&] {
        uint64_t inner = 0;
        EXPECT_TRUE(runtime.read(0, true, fake, inner));
    };
    uint64_t outer = 0;
    EXPECT_FALSE(runtime.read(0, true, fake, outer));
    EXPECT_EQ(runtime.mode(), pv::Mode::failed);
    EXPECT_EQ(runtime.checks(), 2U);
    EXPECT_LE(fake.counters, 5U);
    EXPECT_EQ(runtime.fallback(fake.hpet), fake.hpet);
}
