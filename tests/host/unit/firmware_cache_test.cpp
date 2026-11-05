#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <cstdint>
#include <dev/firmware/cache.hpp>
#include <latch>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>

namespace {
namespace fw = ker::dev::firmware;
constexpr std::array<uint8_t, 3> ABC{'a', 'b', 'c'};
constexpr fw::Digest ABC_DIGEST{0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
                                0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
constexpr fw::Digest EMPTY_DIGEST{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
                                  0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55};

auto policy(std::string_view name = "sample.bin") -> fw::Policy { return {name, ABC.size(), ABC.size(), ABC_DIGEST, 7}; }
auto index() -> std::array<fw::ArchiveEntry, 1> { return {{{"sample.bin", ABC}}}; }

void expect_error(const fw::CacheResult& result, fw::Error error) {
    EXPECT_EQ(result.error, error);
    EXPECT_FALSE(result.valid());
    EXPECT_EQ(result.handle, fw::Handle{});
    EXPECT_TRUE(result.bytes.empty());
    EXPECT_EQ(result.bytes.data(), nullptr);
}

static_assert(!std::is_copy_constructible_v<fw::Cache>);
static_assert(!std::is_move_constructible_v<fw::Cache>);

TEST(FirmwareCache, RequiresBindingAndNeverReplacesEvenAnEmptySuccessfulSource) {
    fw::Cache cache;
    expect_error(cache.acquire(policy(), 7), fw::Error::unavailable);
    EXPECT_EQ(cache.bind({}), fw::Error::none);
    auto const ENTRIES = index();
    EXPECT_EQ(cache.bind(ENTRIES), fw::Error::policy);
    expect_error(cache.acquire(policy(), 7), fw::Error::not_found);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
    EXPECT_EQ(cache.stats().verifications, 0U);
}

TEST(FirmwareCache, RefusesMalformedIndexesWithoutPublishingAPartialSource) {
    fw::Cache cache;
    std::array<fw::ArchiveEntry, 2> entries{{{"sample.bin", ABC}, {"../other", ABC}}};
    EXPECT_EQ(cache.bind(entries), fw::Error::path);
    expect_error(cache.acquire(policy(), 7), fw::Error::unavailable);
    entries[1].name = "sample.bin";
    EXPECT_EQ(cache.bind(entries), fw::Error::duplicate);
    std::array<fw::ArchiveEntry, fw::MAX_INDEX_ENTRIES + 1> too_many{};
    EXPECT_EQ(cache.bind(too_many), fw::Error::limit);
    auto const VALID = index();
    EXPECT_EQ(cache.bind(VALID), fw::Error::none);
    auto const FIRST = cache.acquire(policy(), 7);
    ASSERT_TRUE(FIRST.valid());
    EXPECT_EQ(FIRST.handle.generation, 1U);
    EXPECT_EQ(cache.bind({}), fw::Error::policy);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
}

TEST(FirmwareCache, VerifiesKnownAnswerAndSharesExactlyOneRetainedZeroCopyObject) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    auto const FIRST = cache.acquire(policy(), 7);
    auto const SECOND = cache.acquire(policy(), 7);
    ASSERT_TRUE(FIRST.valid());
    ASSERT_TRUE(SECOND.valid());
    EXPECT_EQ(FIRST.bytes.data(), ABC.data());
    EXPECT_EQ(FIRST.bytes.size(), ABC.size());
    EXPECT_EQ(SECOND.handle, FIRST.handle);
    EXPECT_EQ(SECOND.bytes.data(), FIRST.bytes.data());
    EXPECT_EQ(cache.stats().objects, 1U);
    EXPECT_EQ(cache.stats().references, 2U);
    EXPECT_EQ(cache.stats().verifications, 1U);
    EXPECT_EQ(cache.stats().hits, 1U);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
    EXPECT_EQ(cache.stats().objects, 1U);
    EXPECT_EQ(cache.stats().references, 1U);
    EXPECT_EQ(cache.release(SECOND.handle), fw::Error::none);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
    expect_error(cache.acquire(policy("absent"), 7), fw::Error::not_found);
    EXPECT_EQ(ABC, (std::array<uint8_t, 3>{'a', 'b', 'c'}));
}

TEST(FirmwareCache, CopiesLivePolicyKeysInsteadOfRetainingTheCallersNameOrDigest) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    std::string caller_name = "sample.bin";
    auto caller_policy = policy(caller_name);
    auto const FIRST = cache.acquire(caller_policy, 7);
    ASSERT_TRUE(FIRST.valid());
    caller_name[0] = 'x';
    caller_policy.sha256.fill(0);
    caller_policy.maximum = 4;
    caller_policy.revision = 8;
    auto const SECOND = cache.acquire(policy(), 7);
    ASSERT_TRUE(SECOND.valid());
    EXPECT_EQ(SECOND.handle, FIRST.handle);
    EXPECT_EQ(cache.stats().verifications, 1U);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
    EXPECT_EQ(cache.release(SECOND.handle), fw::Error::none);
}

TEST(FirmwareCache, RejectsInvalidPolicyShapesBeforeAnyLookupOrHash) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    for (std::string_view name : {"", "/sample.bin", "./sample.bin", "a//b", "a/../b", "a/./b", "a/"})
        expect_error(cache.acquire(policy(name), 7), fw::Error::policy);
    std::string oversized_name(fw::MAX_NAME_BYTES, 'a');
    expect_error(cache.acquire(policy(oversized_name), 7), fw::Error::policy);
    auto bad = policy();
    bad.maximum = bad.length - 1;
    expect_error(cache.acquire(bad, 7), fw::Error::length);
    bad.maximum = fw::MAX_BLOB_BYTES + 1ULL;
    expect_error(cache.acquire(bad, 7), fw::Error::length);
    bad.length = bad.maximum = UINT64_MAX;
    expect_error(cache.acquire(bad, 7), fw::Error::length);
    expect_error(cache.acquire(policy(), 8), fw::Error::revision);
    EXPECT_EQ(cache.stats().references, 0U);
    EXPECT_EQ(cache.stats().verifications, 0U);
}

TEST(FirmwareCache, MissingAndWrongLengthObjectsNeverPublishOrHash) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    expect_error(cache.acquire(policy("missing.bin"), 7), fw::Error::not_found);
    auto wrong_length = policy();
    wrong_length.length = 2;
    expect_error(cache.acquire(wrong_length, 7), fw::Error::length);
    wrong_length.length = wrong_length.maximum = 4;
    expect_error(cache.acquire(wrong_length, 7), fw::Error::length);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
    EXPECT_EQ(cache.stats().verifications, 0U);
}

TEST(FirmwareCache, WrongDigestUnwindsWithoutConsumingGenerationOrPublishingAHandle) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    auto wrong = policy();
    wrong.sha256[31] ^= 1;
    expect_error(cache.acquire(wrong, 7), fw::Error::digest);
    EXPECT_EQ(cache.stats().verifications, 1U);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
    auto const CORRECT = cache.acquire(policy(), 7);
    ASSERT_TRUE(CORRECT.valid());
    EXPECT_EQ(CORRECT.handle.generation, 1U);
    EXPECT_EQ(cache.stats().verifications, 2U);
    EXPECT_EQ(cache.release(CORRECT.handle), fw::Error::none);
}

TEST(FirmwareCache, EveryLiveSameNamePolicyConflictRefusesBeforeHashOrReferenceChanges) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    auto const FIRST = cache.acquire(policy(), 7);
    ASSERT_TRUE(FIRST.valid());
    std::array<fw::Policy, 4> conflicts{policy(), policy(), policy(), policy()};
    conflicts[0].length = 2;
    conflicts[1].maximum = 4;
    conflicts[2].sha256[0] ^= 1;
    conflicts[3].revision = 8;
    for (auto const& conflict : conflicts) expect_error(cache.acquire(conflict, conflict.revision), fw::Error::policy_conflict);
    expect_error(cache.acquire(policy(), 8), fw::Error::revision);
    EXPECT_EQ(cache.stats().verifications, 1U);
    EXPECT_EQ(cache.stats().references, 1U);
    EXPECT_EQ(cache.stats().hits, 0U);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
    auto const NEXT = cache.acquire(conflicts[3], 8);
    ASSERT_TRUE(NEXT.valid());
    EXPECT_GT(NEXT.handle.generation, FIRST.handle.generation);
    EXPECT_EQ(cache.release(NEXT.handle), fw::Error::none);
}

TEST(FirmwareCache, ExplicitZeroLengthZeroMaximumAndZeroRevisionHaveAKnownAnswer) {
    fw::Cache cache;
    std::array<fw::ArchiveEntry, 1> const ENTRIES{{{"empty", {}}}};
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    fw::Policy const EMPTY{"empty", 0, 0, EMPTY_DIGEST, 0};
    auto const LEASE = cache.acquire(EMPTY, 0);
    ASSERT_TRUE(LEASE.valid());
    EXPECT_TRUE(LEASE.bytes.empty());
    EXPECT_NE(LEASE.handle.generation, 0U);
    EXPECT_EQ(cache.stats().objects, 1U);
    EXPECT_EQ(cache.release(LEASE.handle), fw::Error::none);
    EXPECT_EQ(cache.stats().references, 0U);
}

TEST(FirmwareCache, ProductionReferenceLimitFailsWithoutWrappingAndEveryAdmittedReferenceReturns) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    fw::Handle handle{};
    for (uint32_t i = 0; i < fw::MAX_REFERENCES; ++i) {
        auto const NEXT = cache.acquire(policy(), 7);
        ASSERT_TRUE(NEXT.valid());
        if (i != 0) EXPECT_EQ(NEXT.handle, handle);
        handle = NEXT.handle;
    }
    expect_error(cache.acquire(policy(), 7), fw::Error::reference_limit);
    EXPECT_EQ(cache.stats().references, fw::MAX_REFERENCES);
    EXPECT_EQ(cache.stats().verifications, 1U);
    EXPECT_EQ(cache.stats().hits, fw::MAX_REFERENCES - 1);
    for (uint32_t i = 0; i < fw::MAX_REFERENCES; ++i) EXPECT_EQ(cache.release(handle), fw::Error::none);
    EXPECT_EQ(cache.stats().references, 0U);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.release(handle), fw::Error::stale);
}

TEST(FirmwareCache, FullPoolAndSlotReuseNeverAcceptAnOldGeneration) {
    fw::CacheEngine<2> cache;
    std::array<fw::ArchiveEntry, 3> const ENTRIES{{{"a", ABC}, {"b", ABC}, {"c", ABC}}};
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    auto const FIRST = cache.acquire(policy("a"), 7);
    auto const SECOND = cache.acquire(policy("b"), 7);
    ASSERT_TRUE(FIRST.valid());
    ASSERT_TRUE(SECOND.valid());
    expect_error(cache.acquire(policy("c"), 7), fw::Error::cache_full);
    EXPECT_EQ(cache.stats().verifications, 2U);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
    auto const THIRD = cache.acquire(policy("c"), 7);
    ASSERT_TRUE(THIRD.valid());
    EXPECT_EQ(THIRD.handle.slot, FIRST.handle.slot);
    EXPECT_GT(THIRD.handle.generation, SECOND.handle.generation);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::stale);
    EXPECT_EQ(cache.release({UINT16_MAX, THIRD.handle.generation}), fw::Error::stale);
    EXPECT_EQ(cache.release({THIRD.handle.slot, 0}), fw::Error::stale);
    EXPECT_EQ(cache.stats().references, 2U);
    EXPECT_EQ(cache.release(SECOND.handle), fw::Error::none);
    EXPECT_EQ(cache.release(THIRD.handle), fw::Error::none);
}

TEST(FirmwareCache, GenerationExhaustionCannotWrapButExistingObjectsRemainUsable) {
    fw::CacheEngine<2, 2, 2> cache;
    std::array<fw::ArchiveEntry, 2> const ENTRIES{{{"a", ABC}, {"b", ABC}}};
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    auto const FIRST = cache.acquire(policy("a"), 7);
    auto const SECOND = cache.acquire(policy("b"), 7);
    ASSERT_TRUE(FIRST.valid());
    ASSERT_TRUE(SECOND.valid());
    EXPECT_EQ(FIRST.handle.generation, 1U);
    EXPECT_EQ(SECOND.handle.generation, 2U);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::none);
    expect_error(cache.acquire(policy("a"), 7), fw::Error::generation_exhausted);
    auto const RETAINED = cache.acquire(policy("b"), 7);
    ASSERT_TRUE(RETAINED.valid());
    EXPECT_EQ(RETAINED.handle, SECOND.handle);
    expect_error(cache.acquire(policy("b"), 7), fw::Error::reference_limit);
    EXPECT_EQ(cache.stats().verifications, 2U);
    EXPECT_EQ(cache.release(SECOND.handle), fw::Error::none);
    EXPECT_EQ(cache.release(RETAINED.handle), fw::Error::none);
    expect_error(cache.acquire(policy("b"), 7), fw::Error::generation_exhausted);
    EXPECT_EQ(cache.release(FIRST.handle), fw::Error::stale);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
}

TEST(FirmwareCache, OneThousandAcquireLastReleaseCyclesReturnToTheFixedMetadataBaseline) {
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    fw::Handle previous{};
    for (uint64_t cycle = 1; cycle <= 1000; ++cycle) {
        auto const LEASE = cache.acquire(policy(), 7);
        ASSERT_TRUE(LEASE.valid());
        EXPECT_EQ(LEASE.handle.generation, cycle);
        EXPECT_EQ(LEASE.bytes.data(), ABC.data());
        EXPECT_EQ(cache.release(previous), fw::Error::stale);
        EXPECT_EQ(cache.release(LEASE.handle), fw::Error::none);
        EXPECT_EQ(cache.stats().objects, 0U);
        EXPECT_EQ(cache.stats().references, 0U);
        EXPECT_EQ(cache.stats().verifications, cycle);
        previous = LEASE.handle;
    }
    EXPECT_EQ(cache.stats().hits, 0U);
}

TEST(FirmwareCache, ExternallySerializedHostThreadsShareOneVerifiedObjectUntilEveryLeaseReturns) {
    constexpr size_t THREADS = 8;
    fw::Cache cache;
    auto const ENTRIES = index();
    ASSERT_EQ(cache.bind(ENTRIES), fw::Error::none);
    std::mutex lock;
    std::barrier ready(static_cast<ptrdiff_t>(THREADS + 1));
    std::latch release(1);
    std::array<fw::CacheResult, THREADS> leases{};
    std::array<fw::Error, THREADS> returned{};
    std::array<std::thread, THREADS> threads{};
    for (size_t i = 0; i < THREADS; ++i) {
        threads[i] = std::thread([&, i] {
            {
                std::lock_guard guard(lock);
                leases[i] = cache.acquire(policy(), 7);
            }
            ready.arrive_and_wait();
            release.wait();
            std::lock_guard guard(lock);
            returned[i] = cache.release(leases[i].handle);
        });
    }
    ready.arrive_and_wait();
    {
        std::lock_guard guard(lock);
        for (auto const& lease : leases) {
            EXPECT_TRUE(lease.valid());
            EXPECT_EQ(lease.handle, leases[0].handle);
            EXPECT_EQ(lease.bytes.data(), ABC.data());
        }
        EXPECT_EQ(cache.stats().objects, 1U);
        EXPECT_EQ(cache.stats().references, THREADS);
        EXPECT_EQ(cache.stats().verifications, 1U);
        EXPECT_EQ(cache.stats().hits, THREADS - 1);
    }
    release.count_down();
    for (auto& thread : threads) thread.join();
    for (auto error : returned) EXPECT_EQ(error, fw::Error::none);
    EXPECT_EQ(cache.stats().objects, 0U);
    EXPECT_EQ(cache.stats().references, 0U);
}

}  // namespace
