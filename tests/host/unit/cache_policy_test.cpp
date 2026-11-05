#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <latch>
#include <platform/mm/cache_policy.hpp>
#include <thread>
#include <type_traits>

namespace {
namespace cache = ker::mod::mm::cache;
using cache::CpuPat;
using cache::detail::PatReports;
constexpr uint64_t LIMINE_PAT = 0x0000010500070406ULL;

static_assert(!std::is_copy_constructible_v<PatReports> && !std::is_move_constructible_v<PatReports>);

auto observed(uint64_t raw = LIMINE_PAT) -> std::optional<CpuPat> { return CpuPat{true, raw}; }

void expect_unready(const cache::PatQualification& value) {
    EXPECT_FALSE(value.wb_encoding);
    EXPECT_FALSE(value.uc_encoding);
    EXPECT_FALSE(value.wc_encoding);
}

TEST(CachePolicy, EmptyQueriesDoNotPublishOrChangeTheOutput) {
    PatReports reports;
    auto const value = reports.qualification();
    EXPECT_FALSE(value.configured);
    EXPECT_FALSE(value.complete);
    EXPECT_EQ(value.expected_mask | value.reported_mask | value.mismatch_mask, 0U);
    expect_unready(value);
    CpuPat out{true, 123};
    for (size_t index : {size_t{0}, size_t{63}, size_t{64}, SIZE_MAX}) {
        EXPECT_FALSE(reports.cpu_pat(index, out));
        EXPECT_TRUE(out.supported);
        EXPECT_EQ(out.raw, 123U);
    }
    ASSERT_TRUE(reports.configure(1));
    ASSERT_TRUE(reports.capture(0, [] { return observed(); }));
    EXPECT_TRUE(reports.qualification().complete);
}

TEST(CachePolicy, ConfigurationBoundsAndMasksAreCheckedAndFaultsStaySticky) {
    for (size_t count : {size_t{0}, size_t{65}, SIZE_MAX}) {
        PatReports reports;
        EXPECT_FALSE(reports.configure(count));
        EXPECT_FALSE(reports.qualification().configured);
        ASSERT_TRUE(reports.configure(1));
        ASSERT_TRUE(reports.capture(0, [] { return observed(); }));
        EXPECT_FALSE(reports.qualification().complete);
        expect_unready(reports.qualification());
    }
    for (size_t count : {size_t{1}, size_t{63}, size_t{64}}) {
        PatReports reports;
        ASSERT_TRUE(reports.configure(count));
        uint64_t const MASK = count == 64 ? UINT64_MAX : (uint64_t{1} << count) - 1;
        EXPECT_EQ(reports.qualification().expected_mask, MASK);
        for (size_t i = 0; i < count; ++i) ASSERT_TRUE(reports.capture(i, [] { return observed(); }));
        auto const value = reports.qualification();
        EXPECT_TRUE(value.complete);
        EXPECT_EQ(value.reported_mask, MASK);
        EXPECT_EQ(value.mismatch_mask, 0U);
        EXPECT_TRUE(value.wb_encoding && value.uc_encoding && value.wc_encoding);
    }
}

TEST(CachePolicy, SameConfigurationIsIdempotentAndChangedConfigurationPoisonsWithoutReplacingIt) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(1));
    ASSERT_TRUE(reports.configure(1));
    ASSERT_TRUE(reports.capture(0, [] { return observed(); }));
    EXPECT_TRUE(reports.qualification().complete);
    EXPECT_FALSE(reports.configure(2));
    EXPECT_TRUE(reports.configure(1));
    auto const value = reports.qualification();
    EXPECT_EQ(value.expected_mask, 1U);
    EXPECT_EQ(value.reported_mask, 1U);
    EXPECT_FALSE(value.complete);
    expect_unready(value);
}

TEST(CachePolicy, AllExpectedCpuReportsAreRequiredBeforeAnyEncodingIsReady) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(2));
    ASSERT_TRUE(reports.capture(1, [] { return observed(); }));
    auto value = reports.qualification();
    EXPECT_EQ(value.reported_mask, 2U);
    EXPECT_FALSE(value.complete);
    expect_unready(value);
    ASSERT_TRUE(reports.capture(0, [] { return observed(); }));
    value = reports.qualification();
    EXPECT_TRUE(value.complete);
    EXPECT_TRUE(value.wb_encoding && value.uc_encoding && value.wc_encoding);
}

TEST(CachePolicy, RequiredPatEntriesAreCheckedIndependentlyAndWcRequiresTheBaseline) {
    for (unsigned index : {0U, 3U, 5U}) {
        PatReports reports;
        ASSERT_TRUE(reports.configure(1));
        uint64_t const RAW = (LIMINE_PAT & ~(uint64_t{255} << (index * 8U))) | (uint64_t{7} << (index * 8U));
        ASSERT_TRUE(reports.capture(0, [=] { return observed(RAW); }));
        auto const value = reports.qualification();
        EXPECT_TRUE(value.complete);
        EXPECT_EQ(value.mismatch_mask, 1U);
        EXPECT_EQ(value.wb_encoding, index != 0);
        EXPECT_EQ(value.uc_encoding, index != 3);
        EXPECT_FALSE(value.wc_encoding);
    }
}

TEST(CachePolicy, UnusedPatEntriesNeedNotMatchTheBootloaderImage) {
    constexpr uint64_t REQUIRED_MASK = uint64_t{255} | (uint64_t{255} << 24) | (uint64_t{255} << 40);
    constexpr uint64_t RAW = LIMINE_PAT ^ ~REQUIRED_MASK;
    PatReports reports;
    ASSERT_TRUE(reports.configure(1));
    ASSERT_TRUE(reports.capture(0, [] { return observed(RAW); }));
    auto const value = reports.qualification();
    EXPECT_TRUE(value.complete);
    EXPECT_EQ(value.mismatch_mask, 0U);
    EXPECT_TRUE(value.wb_encoding && value.uc_encoding && value.wc_encoding);
    CpuPat out{};
    ASSERT_TRUE(reports.cpu_pat(0, out));
    EXPECT_EQ(out.raw, RAW);
}

TEST(CachePolicy, LegacyAndMixedMachinesRetainWbAndUcWithoutClaimingWc) {
    for (bool first_supported : {false, true}) {
        PatReports reports;
        ASSERT_TRUE(reports.configure(2));
        ASSERT_TRUE(reports.capture(0, [=]() -> std::optional<CpuPat> { return CpuPat{first_supported, LIMINE_PAT}; }));
        ASSERT_TRUE(reports.capture(1, []() -> std::optional<CpuPat> { return CpuPat{}; }));
        auto const value = reports.qualification();
        EXPECT_TRUE(value.complete);
        EXPECT_EQ(value.mismatch_mask, 0U);
        EXPECT_TRUE(value.wb_encoding && value.uc_encoding);
        EXPECT_FALSE(value.wc_encoding);
    }
}

TEST(CachePolicy, DuplicateCaptureNeverInvokesAnotherReaderOrReplacesTheObservation) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(1));
    unsigned calls = 0;
    auto read = [&]() {
        ++calls;
        return observed(calls == 1 ? LIMINE_PAT : 0);
    };
    ASSERT_TRUE(reports.capture(0, read));
    EXPECT_FALSE(reports.capture(0, read));
    EXPECT_FALSE(reports.capture(0, read));
    EXPECT_EQ(calls, 1U);
    CpuPat out{};
    ASSERT_TRUE(reports.cpu_pat(0, out));
    EXPECT_TRUE(out.supported);
    EXPECT_EQ(out.raw, LIMINE_PAT);
    auto const value = reports.qualification();
    EXPECT_EQ(value.reported_mask, 1U);
    EXPECT_EQ(value.mismatch_mask, 1U);
    EXPECT_FALSE(value.complete);
    expect_unready(value);
}

TEST(CachePolicy, InvalidCaptureIndicesAndUnconfiguredCaptureNeverInvokeReaders) {
    for (size_t index : {size_t{1}, size_t{64}, SIZE_MAX}) {
        PatReports reports;
        ASSERT_TRUE(reports.configure(1));
        unsigned calls = 0;
        EXPECT_FALSE(reports.capture(index, [&] {
            ++calls;
            return observed();
        }));
        EXPECT_EQ(calls, 0U);
        ASSERT_TRUE(reports.capture(0, [] { return observed(); }));
        EXPECT_FALSE(reports.qualification().complete);
        expect_unready(reports.qualification());
    }
    PatReports reports;
    EXPECT_FALSE(reports.capture(0, [] { return observed(); }));
    EXPECT_EQ(reports.qualification().reported_mask, 0U);
}

TEST(CachePolicy, FailedObservationCannotPublishOrRetry) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(1));
    unsigned calls = 0;
    EXPECT_FALSE(reports.capture(0, [&]() -> std::optional<CpuPat> {
        ++calls;
        return std::nullopt;
    }));
    EXPECT_FALSE(reports.capture(0, [&] {
        ++calls;
        return observed();
    }));
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(reports.qualification().reported_mask, 0U);
    EXPECT_EQ(reports.qualification().mismatch_mask, 1U);
    CpuPat out{true, 123};
    EXPECT_FALSE(reports.cpu_pat(0, out));
    EXPECT_EQ(out.raw, 123U);
    expect_unready(reports.qualification());
}

TEST(CachePolicy, CpuidGatesEveryPrivilegedPatRead) {
    struct CpuIo {
        uint32_t flags{};
        unsigned cpuid_reads{}, pat_reads{};
        auto features() -> uint32_t {
            ++cpuid_reads;
            return flags;
        }
        auto read_pat() -> uint64_t {
            ++pat_reads;
            return LIMINE_PAT;
        }
    };
    for (uint32_t flags :
         {0U, cache::detail::MSR_FEATURE, cache::detail::PAT_FEATURE, cache::detail::MSR_FEATURE | cache::detail::PAT_FEATURE}) {
        CpuIo io{flags};
        auto const value = cache::detail::observe_pat(io);
        EXPECT_EQ(io.cpuid_reads, 1U);
        bool const READABLE = flags == (cache::detail::MSR_FEATURE | cache::detail::PAT_FEATURE);
        EXPECT_EQ(io.pat_reads, READABLE ? 1U : 0U);
        if ((flags & cache::detail::PAT_FEATURE) == 0) {
            ASSERT_TRUE(value);
            EXPECT_FALSE(value->supported);
            EXPECT_EQ(value->raw, 0U);
        } else {
            EXPECT_EQ(value.has_value(), READABLE);
            if (value) EXPECT_EQ(value->raw, LIMINE_PAT);
        }
    }
}

TEST(CachePolicy, ConcurrentReadersCannotObservePartialCaptureAndDuplicatePoisonIsSticky) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(1));
    std::latch entered(1), finish(1);
    std::atomic<bool> captured{false};
    std::thread producer([&] {
        captured.store(reports.capture(0, [&] {
            entered.count_down();
            finish.wait();
            return observed();
        }));
    });
    entered.wait();
    CpuPat out{false, 123};
    EXPECT_FALSE(reports.cpu_pat(0, out));
    EXPECT_EQ(out.raw, 123U);
    EXPECT_EQ(reports.qualification().reported_mask, 0U);
    expect_unready(reports.qualification());
    unsigned duplicate_calls = 0;
    EXPECT_FALSE(reports.capture(0, [&] {
        ++duplicate_calls;
        return observed();
    }));
    finish.count_down();
    producer.join();
    EXPECT_TRUE(captured.load());
    EXPECT_EQ(duplicate_calls, 0U);
    ASSERT_TRUE(reports.cpu_pat(0, out));
    EXPECT_EQ(out.raw, LIMINE_PAT);
    EXPECT_FALSE(reports.qualification().complete);
    EXPECT_EQ(reports.qualification().mismatch_mask, 1U);
    expect_unready(reports.qualification());
}

TEST(CachePolicy, DistinctCpuProducersPublishTheirExactImmutableValuesConcurrently) {
    PatReports reports;
    ASSERT_TRUE(reports.configure(8));
    std::latch start(1);
    std::array<std::thread, 8> workers;
    std::atomic<unsigned> captured{0};
    for (size_t i = 0; i < workers.size(); ++i) {
        workers[i] = std::thread([&, i] {
            start.wait();
            if (reports.capture(i, [=] { return observed(LIMINE_PAT ^ (uint64_t{i} << 8)); })) captured.fetch_add(1);
        });
    }
    start.count_down();
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(captured.load(), 8U);
    EXPECT_TRUE(reports.qualification().complete);
    EXPECT_TRUE(reports.qualification().wc_encoding);
    EXPECT_EQ(reports.qualification().mismatch_mask, 0U);
    for (size_t i = 0; i < workers.size(); ++i) {
        CpuPat out{};
        ASSERT_TRUE(reports.cpu_pat(i, out));
        EXPECT_EQ(out.raw, LIMINE_PAT ^ (uint64_t{i} << 8));
    }
}
}  // namespace
