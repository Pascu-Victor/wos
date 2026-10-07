#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>
#include <vfs/append_receipt.hpp>
#include <vfs/coherency.hpp>
#include <vfs/native_coherency.hpp>

namespace coherence = ker::vfs::coherency;

namespace {

constexpr coherence::Stamp SENTINEL{91, 92, 94, 93};
constexpr coherence::Ticket TICKET_SENTINEL{101, coherence::Domain::ALL};

auto snapshot(const coherence::State& state) -> coherence::Stamp {
    coherence::Stamp output{};
    EXPECT_TRUE(state.snapshot(output));
    EXPECT_TRUE(coherence::valid_stamp(output));
    return output;
}

}  // namespace

TEST(VfsAppendReceipt, DuplicateReturnsExactPartialOutcomeWithoutAnotherEffect) {
    coherence::AppendReceipt receipt;
    coherence::AppendReceipt::Digest digest{};
    digest.front() = 27;
    coherence::AppendOutcome replay{-123, 0, 0};
    EXPECT_EQ(receipt.claim(5, 20, digest, replay), coherence::AppendClaim::NEW);
    EXPECT_EQ(replay.status, -123);
    EXPECT_EQ(receipt.claim(5, 20, digest, replay), coherence::AppendClaim::PENDING);
    coherence::AppendOutcome const PARTIAL{0, 7, 41};
    ASSERT_TRUE(receipt.complete(5, PARTIAL));
    EXPECT_FALSE(receipt.complete(5, PARTIAL));
    EXPECT_EQ(receipt.claim(5, 20, digest, replay), coherence::AppendClaim::REPLAY);
    EXPECT_EQ(replay, PARTIAL);
}

TEST(VfsAppendReceipt, PendingMismatchAndOldIdsNeverReplaceAcceptedOperation) {
    coherence::AppendReceipt receipt;
    coherence::AppendReceipt::Digest digest{}, wrong{};
    wrong.back() = 1;
    coherence::AppendOutcome replay{-321, 0, 0};
    EXPECT_EQ(receipt.claim(0, 2, digest, replay), coherence::AppendClaim::INVALID);
    ASSERT_EQ(receipt.claim(9, 2, digest, replay), coherence::AppendClaim::NEW);
    EXPECT_EQ(receipt.claim(9, 2, wrong, replay), coherence::AppendClaim::MISMATCH);
    EXPECT_EQ(receipt.claim(9, 1, digest, replay), coherence::AppendClaim::MISMATCH);
    EXPECT_EQ(receipt.claim(8, 2, digest, replay), coherence::AppendClaim::STALE);
    EXPECT_EQ(receipt.claim(10, 2, digest, replay), coherence::AppendClaim::PENDING);
    EXPECT_EQ(replay.status, -321);
    EXPECT_FALSE(receipt.complete(10, {0, 2, 0}));
    ASSERT_TRUE(receipt.complete(9, {-EIO, 0, 0}));
    EXPECT_EQ(receipt.claim(9, 2, digest, replay), coherence::AppendClaim::REPLAY);
    EXPECT_EQ(replay.status, -EIO);
    ASSERT_EQ(receipt.claim(10, 2, wrong, replay), coherence::AppendClaim::NEW);
    ASSERT_TRUE(receipt.complete(10, {0, 2, 18}));
    EXPECT_EQ(receipt.claim(9, 2, digest, replay), coherence::AppendClaim::STALE);
}

TEST(VfsAppendReceipt, CompletionRequiresExactIdAndValidCommittedPrefix) {
    coherence::AppendReceipt receipt;
    coherence::AppendReceipt::Digest digest{};
    coherence::AppendOutcome replay{};
    ASSERT_EQ(receipt.claim(UINT64_MAX, 7, digest, replay), coherence::AppendClaim::NEW);
    EXPECT_FALSE(receipt.complete(UINT64_MAX - 1, {0, 7, 0}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {1, 0, 0}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {0, 8, 0}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {-EIO, 1, 0}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {-EIO, 0, 1}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {0, 0, UINT64_MAX}));
    EXPECT_FALSE(receipt.complete(UINT64_MAX, {0, 7, static_cast<uint64_t>(INT64_MAX) - 6}));
    ASSERT_TRUE(receipt.complete(UINT64_MAX, {0, 7, static_cast<uint64_t>(INT64_MAX) - 7}));
    EXPECT_EQ(receipt.claim(UINT64_MAX, 7, digest, replay), coherence::AppendClaim::REPLAY);
    EXPECT_EQ(replay.offset + replay.bytes, static_cast<uint64_t>(INT64_MAX));
    EXPECT_EQ(receipt.claim(UINT64_MAX - 1, 7, digest, replay), coherence::AppendClaim::STALE);
}

TEST(VfsAppendReceipt, ConcurrentDuplicatesReserveOnlyOneEffectUnderOwnerSerialization) {
    coherence::AppendReceipt receipt;
    std::mutex serialization;
    std::atomic<unsigned> effects{0};
    std::atomic<unsigned> failures{0};
    coherence::AppendReceipt::Digest digest{};
    std::vector<std::thread> clients;
    for (size_t i = 0; i < 8; ++i) {
        clients.emplace_back([&] {
            coherence::AppendOutcome replay{};
            coherence::AppendClaim claim;
            {
                std::lock_guard guard(serialization);
                claim = receipt.claim(19, 8, digest, replay);
            }
            if (claim == coherence::AppendClaim::NEW) {
                effects.fetch_add(1);
                std::lock_guard guard(serialization);
                if (!receipt.complete(19, {0, 8, 99})) failures.fetch_add(1);
            } else if (claim != coherence::AppendClaim::PENDING &&
                       (claim != coherence::AppendClaim::REPLAY || replay != coherence::AppendOutcome{0, 8, 99})) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& client : clients) client.join();
    EXPECT_EQ(effects.load(), 1U);
    EXPECT_EQ(failures.load(), 0U);
    coherence::AppendOutcome replay{};
    EXPECT_EQ(receipt.claim(19, 8, digest, replay), coherence::AppendClaim::REPLAY);
    EXPECT_EQ(replay, (coherence::AppendOutcome{0, 8, 99}));
}

TEST(VfsCacheValidation, RacingFetchCannotRetagOldBytesWithNewRevision) {
    coherence::Counter counter;
    coherence::Object object(counter);
    coherence::Stamp before{}, after{};
    ASSERT_TRUE(object.snapshot(before));
    coherence::CacheValidation cache;
    ASSERT_TRUE(cache.publish(before, before, coherence::Domain::CONTENT));
    // The fetched value belongs to the earlier snapshot. A writer commits
    // before the second query; using after alone would incorrectly bless it.
    uint64_t const FETCHED_VALUE = 12;
    uint64_t owner_value = FETCHED_VALUE;
    {
        coherence::Mutation writer(object, coherence::Domain::CONTENT);
        ASSERT_TRUE(writer);
        owner_value = 33;
    }
    ASSERT_TRUE(object.snapshot(after));
    ASSERT_NE(owner_value, FETCHED_VALUE);
    EXPECT_FALSE(cache.publish(before, after, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.matches(before, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.matches(after, coherence::Domain::CONTENT));
    EXPECT_TRUE(cache.publish(after, after, coherence::Domain::CONTENT));
    EXPECT_TRUE(cache.matches(after, coherence::Domain::CONTENT));
}

TEST(VfsCacheValidation, ReplacementFailureAndExplicitInvalidationRevokeAllAuthority) {
    coherence::Stamp const FIRST{4, 5, 7, 6};
    coherence::Stamp const SUCCESSOR{8, 8, 8, 8};
    coherence::CacheValidation cache;
    EXPECT_FALSE(cache.matches(FIRST, coherence::Domain::CONTENT));
    ASSERT_TRUE(cache.publish(FIRST, FIRST, coherence::Domain::ALL));
    EXPECT_FALSE(cache.publish(FIRST, SUCCESSOR, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.matches(FIRST, coherence::Domain::ALL));
    ASSERT_TRUE(cache.publish(FIRST, FIRST, coherence::Domain::ALL));
    EXPECT_FALSE(cache.publish(FIRST, {}, coherence::Domain::ALL));
    EXPECT_FALSE(cache.matches(FIRST, coherence::Domain::ALL));
    ASSERT_TRUE(cache.publish(FIRST, FIRST, coherence::Domain::ALL));
    cache.invalidate();
    EXPECT_FALSE(cache.matches(FIRST, coherence::Domain::ALL));
}

TEST(VfsCacheValidation, RelevantDomainRatherThanClockControlsFreshness) {
    coherence::Stamp const CONTENT{4, 5, 7, 6};
    coherence::Stamp const METADATA_ONLY{4, 5, 11, 6};
    coherence::Stamp const DIRECTORY_ONLY{4, 5, 13, 13};
    coherence::CacheValidation cache;
    ASSERT_TRUE(cache.publish(CONTENT, METADATA_ONLY, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.matches(METADATA_ONLY, coherence::Domain::METADATA));
    EXPECT_FALSE(cache.matches(METADATA_ONLY, coherence::Domain::ALL));
    EXPECT_TRUE(cache.matches(DIRECTORY_ONLY, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.matches(DIRECTORY_ONLY, coherence::Domain::METADATA));
    EXPECT_FALSE(cache.matches(DIRECTORY_ONLY, coherence::Domain::DIRECTORY));
    EXPECT_FALSE(cache.publish(CONTENT, DIRECTORY_ONLY, coherence::Domain::DIRECTORY));
    EXPECT_FALSE(cache.matches(CONTENT, coherence::Domain::CONTENT));
    EXPECT_FALSE(cache.publish(CONTENT, CONTENT, coherence::Domain::NONE));
    EXPECT_FALSE(cache.matches(CONTENT, coherence::Domain::CONTENT));
}

TEST(VfsCacheValidation, RandomPublicationMatchesIndependentSampleOracle) {
    std::mt19937_64 random(0x26ca);
    coherence::CacheValidation cache;
    for (size_t i = 0; i < 10000; ++i) {
        coherence::Stamp const BEFORE{1, 1 + random() % 100, 400, 1 + random() % 100};
        coherence::Stamp AFTER = BEFORE;
        if ((random() & 1U) != 0) AFTER.content += 100;
        if ((random() & 1U) != 0) AFTER = {401, 401, 401, 401};
        bool const SAME_OBJECT_AND_DATA = BEFORE.object == AFTER.object && BEFORE.content == AFTER.content;
        EXPECT_EQ(cache.publish(BEFORE, AFTER, coherence::Domain::CONTENT), SAME_OBJECT_AND_DATA);
        EXPECT_EQ(cache.matches(AFTER, coherence::Domain::CONTENT), SAME_OBJECT_AND_DATA);
        if ((random() & 1U) != 0) {
            cache.invalidate();
            EXPECT_FALSE(cache.matches(AFTER, coherence::Domain::CONTENT));
        }
    }
}

TEST(VfsCoherency, UninitializedAndClosedStatePreserveCallerOutputs) {
    coherence::Counter counter;
    coherence::State state;
    auto output = SENTINEL;
    auto ticket = TICKET_SENTINEL;
    EXPECT_FALSE(state.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
    EXPECT_FALSE(state.prepare(coherence::Domain::CONTENT, ticket));
    EXPECT_EQ(ticket, TICKET_SENTINEL);
    EXPECT_TRUE(state.close_admission());
    EXPECT_FALSE(state.initialize(counter));
    EXPECT_FALSE(state.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
    EXPECT_EQ(counter.take(), 1U);
}

TEST(VfsCoherency, IdentityIsNonreusedAcrossObjectsAndReinitializationIsRejected) {
    coherence::Counter counter;
    coherence::State first, second;
    ASSERT_TRUE(first.initialize(counter));
    auto const BEFORE = snapshot(first);
    EXPECT_FALSE(first.initialize(counter));
    ASSERT_TRUE(second.initialize(counter));
    auto const OTHER = snapshot(second);
    EXPECT_NE(BEFORE.object, OTHER.object);
    EXPECT_GT(OTHER.object, BEFORE.object);
    EXPECT_FALSE(coherence::cache_matches(BEFORE, OTHER, coherence::Domain::ALL));
    EXPECT_EQ(counter.take(), OTHER.object + 1);
}

TEST(VfsCoherency, ContentCommitInvalidatesContentAndMetadataAfterPublication) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    auto const OLD = snapshot(state);
    coherence::Ticket ticket{};
    ASSERT_TRUE(state.prepare(coherence::Domain::CONTENT, ticket));
    EXPECT_EQ(ticket.domains, coherence::Domain::CONTENT | coherence::Domain::METADATA);
    auto output = SENTINEL;
    EXPECT_FALSE(state.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
    ASSERT_TRUE(state.publish(ticket));
    auto const CURRENT = snapshot(state);
    EXPECT_EQ(CURRENT.object, OLD.object);
    EXPECT_GT(CURRENT.content, OLD.content);
    EXPECT_EQ(CURRENT.metadata, CURRENT.content);
    EXPECT_EQ(CURRENT.directory, OLD.directory);
    EXPECT_FALSE(coherence::cache_matches(OLD, CURRENT, coherence::Domain::CONTENT));
    EXPECT_FALSE(coherence::cache_matches(OLD, CURRENT, coherence::Domain::METADATA));
    EXPECT_TRUE(coherence::cache_matches(OLD, CURRENT, coherence::Domain::DIRECTORY));
}

TEST(VfsCoherency, DirectoryAndMetadataDomainsInvalidateExactlyTheirCaches) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    auto const OLD = snapshot(state);
    coherence::Ticket ticket{};
    ASSERT_TRUE(state.prepare(coherence::Domain::DIRECTORY, ticket));
    ASSERT_TRUE(state.publish(ticket));
    auto const DIR = snapshot(state);
    EXPECT_GT(DIR.directory, OLD.directory);
    EXPECT_EQ(DIR.metadata, DIR.directory);
    EXPECT_TRUE(coherence::cache_matches(OLD, DIR, coherence::Domain::CONTENT));
    EXPECT_FALSE(coherence::cache_matches(OLD, DIR, coherence::Domain::DIRECTORY));
    ASSERT_TRUE(state.prepare(coherence::Domain::METADATA, ticket));
    ASSERT_TRUE(state.publish(ticket));
    auto const META = snapshot(state);
    EXPECT_GT(META.metadata, DIR.metadata);
    EXPECT_TRUE(coherence::cache_matches(DIR, META, coherence::Domain::CONTENT | coherence::Domain::DIRECTORY));
    EXPECT_FALSE(coherence::cache_matches(DIR, META, coherence::Domain::METADATA));
}

TEST(VfsCoherency, NoEffectCancelPreservesVersionsButBurnsSerial) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    auto const OLD = snapshot(state);
    coherence::Ticket first{}, second{};
    ASSERT_TRUE(state.prepare(coherence::Domain::ALL, first));
    EXPECT_FALSE(state.prepare(coherence::Domain::CONTENT, second));
    EXPECT_EQ(second, coherence::Ticket{});
    ASSERT_TRUE(state.cancel_no_effect(first));
    EXPECT_EQ(snapshot(state), OLD);
    ASSERT_TRUE(state.prepare(coherence::Domain::ALL, second));
    EXPECT_GT(second.serial, first.serial);
    EXPECT_FALSE(state.publish(first));
    EXPECT_FALSE(state.cancel_no_effect(first));
    EXPECT_FALSE(state.quiescent());
    ASSERT_TRUE(state.publish(second));
    EXPECT_EQ(snapshot(state), (coherence::Stamp{OLD.object, second.serial, second.serial, second.serial}));
}

TEST(VfsCoherency, InvalidAndCrossObjectTicketsCannotCompletePendingMutation) {
    coherence::Counter counter;
    coherence::State first, second;
    ASSERT_TRUE(first.initialize(counter));
    ASSERT_TRUE(second.initialize(counter));
    coherence::Ticket one{}, two{};
    ASSERT_TRUE(first.prepare(coherence::Domain::CONTENT, one));
    ASSERT_TRUE(second.prepare(coherence::Domain::CONTENT, two));
    EXPECT_FALSE(first.publish(two));
    EXPECT_FALSE(first.cancel_no_effect(two));
    EXPECT_FALSE(first.publish({}));
    auto forged = one;
    forged.domains = coherence::Domain::ALL;
    EXPECT_FALSE(first.publish(forged));
    EXPECT_FALSE(first.cancel_no_effect(forged));
    auto output = SENTINEL;
    EXPECT_FALSE(first.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
    ASSERT_TRUE(first.publish(one));
    EXPECT_FALSE(first.publish(one));
    ASSERT_TRUE(second.cancel_no_effect(two));
}

TEST(VfsCoherency, ClosingAdmissionRequiresPendingEffectDrainAndCannotReopen) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    coherence::Ticket ticket{};
    ASSERT_TRUE(state.prepare(coherence::Domain::CONTENT, ticket));
    EXPECT_FALSE(state.close_admission());
    auto other = TICKET_SENTINEL;
    EXPECT_FALSE(state.prepare(coherence::Domain::DIRECTORY, other));
    EXPECT_EQ(other, TICKET_SENTINEL);
    ASSERT_TRUE(state.publish(ticket));
    EXPECT_TRUE(state.quiescent());
    EXPECT_TRUE(state.close_admission());
    EXPECT_FALSE(state.initialize(counter));
    auto output = SENTINEL;
    EXPECT_FALSE(state.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
}

TEST(VfsCoherency, ExhaustionRejectsNewEffectsWithoutInvalidatingCommittedSnapshots) {
    coherence::Counter counter(UINT64_MAX - 1);
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    coherence::Ticket ticket{};
    ASSERT_TRUE(state.prepare(coherence::Domain::ALL, ticket));
    EXPECT_EQ(ticket.serial, UINT64_MAX);
    ASSERT_TRUE(state.publish(ticket));
    auto const COMMITTED = snapshot(state);
    EXPECT_EQ(COMMITTED.metadata, UINT64_MAX);
    auto other = TICKET_SENTINEL;
    EXPECT_FALSE(state.prepare(coherence::Domain::ALL, other));
    EXPECT_EQ(other, TICKET_SENTINEL);
    EXPECT_EQ(snapshot(state), COMMITTED);
    coherence::State successor;
    EXPECT_FALSE(successor.initialize(counter));
    for (unsigned attempt = 0; attempt < 16; ++attempt) EXPECT_EQ(counter.take(), 0U);
}

TEST(VfsCoherency, InvalidDomainAndStampNeverGrantCacheFreshness) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    auto const GOOD = snapshot(state);
    for (unsigned bits = 0; bits < 256; ++bits) {
        auto const DOMAIN = static_cast<coherence::Domain>(bits);
        if (bits >= 1 && bits <= 7) continue;
        auto ticket = TICKET_SENTINEL;
        EXPECT_FALSE(state.prepare(DOMAIN, ticket));
        EXPECT_EQ(ticket, TICKET_SENTINEL);
        EXPECT_FALSE(coherence::cache_matches(GOOD, GOOD, DOMAIN));
    }
    EXPECT_FALSE(coherence::cache_matches({}, {}, coherence::Domain::ALL));
    for (unsigned field = 0; field < 4; ++field) {
        auto bad = GOOD;
        switch (field) {
            case 0:
                bad.object = 0;
                break;
            case 1:
                bad.content = 0;
                break;
            case 2:
                bad.metadata = 0;
                break;
            default:
                bad.directory = 0;
                break;
        }
        EXPECT_FALSE(coherence::cache_matches(bad, GOOD, coherence::Domain::ALL));
        EXPECT_FALSE(coherence::cache_matches(GOOD, bad, coherence::Domain::ALL));
    }
    auto bad = GOOD;
    bad.content = GOOD.metadata + 1;
    EXPECT_FALSE(coherence::valid_stamp(bad));
    EXPECT_EQ(snapshot(state), GOOD);
}

TEST(VfsCoherency, ConcurrentCounterReservationsHaveNoReuseAndExhaustExactlyOnce) {
    constexpr unsigned THREADS = 8;
    constexpr unsigned EACH = 1024;
    std::array<std::vector<uint64_t>, THREADS> ids;
    std::array<std::thread, THREADS> workers;
    coherence::Counter counter;
    for (unsigned thread = 0; thread < THREADS; ++thread) {
        workers[thread] = std::thread([&, thread] {
            ids[thread].reserve(EACH);
            for (unsigned i = 0; i < EACH; ++i) ids[thread].push_back(counter.take());
        });
    }
    for (auto& worker : workers) worker.join();
    std::set<uint64_t> all;
    for (auto const& lane : ids) all.insert(lane.begin(), lane.end());
    EXPECT_EQ(all.size(), THREADS * EACH);
    EXPECT_EQ(*all.begin(), 1U);
    EXPECT_EQ(*all.rbegin(), THREADS * EACH);

    coherence::Counter last(UINT64_MAX - THREADS + 1);
    std::array<uint64_t, THREADS * 2> tail{};
    for (unsigned thread = 0; thread < THREADS; ++thread) {
        workers[thread] = std::thread([&, thread] {
            tail[thread * 2] = last.take();
            tail[thread * 2 + 1] = last.take();
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(std::count(tail.begin(), tail.end(), uint64_t{0}), THREADS);
    std::set<uint64_t> nonzero;
    for (auto id : tail)
        if (id != 0) nonzero.insert(id);
    EXPECT_EQ(nonzero.size(), THREADS);
    EXPECT_EQ(*nonzero.rbegin(), UINT64_MAX);
    EXPECT_EQ(last.take(), 0U);
}

TEST(VfsCoherency, RandomTransitionsAgreeWithIndependentPublicationOracle) {
    coherence::Counter counter;
    coherence::State state;
    ASSERT_TRUE(state.initialize(counter));
    uint64_t next = 2;
    coherence::Stamp reference{1, 1, 1, 1};
    std::mt19937_64 random(0x26c0ffee);
    for (unsigned iteration = 0; iteration < 10000; ++iteration) {
        unsigned const BITS = 1 + random() % 7;
        auto const DOMAIN = static_cast<coherence::Domain>(BITS);
        auto const BEFORE = reference;
        coherence::Ticket ticket{};
        ASSERT_TRUE(state.prepare(DOMAIN, ticket));
        ASSERT_EQ(ticket.serial, next++);
        bool const COMMIT = (random() & 3U) != 0;
        if (COMMIT) {
            ASSERT_TRUE(state.publish(ticket));
            if ((BITS & 1U) != 0) reference.content = ticket.serial;
            if ((BITS & 4U) != 0) reference.directory = ticket.serial;
            reference.metadata = ticket.serial;
        } else {
            ASSERT_TRUE(state.cancel_no_effect(ticket));
        }
        auto const CURRENT = snapshot(state);
        ASSERT_EQ(CURRENT, reference) << iteration;
        for (unsigned check = 1; check <= 7; ++check) {
            bool const EXPECTED = ((check & 1U) == 0 || BEFORE.content == reference.content) &&
                                  ((check & 2U) == 0 || BEFORE.metadata == reference.metadata) &&
                                  ((check & 4U) == 0 || BEFORE.directory == reference.directory);
            ASSERT_EQ(coherence::cache_matches(BEFORE, CURRENT, static_cast<coherence::Domain>(check)), EXPECTED) << iteration;
        }
        EXPECT_FALSE(state.publish(ticket));
        EXPECT_FALSE(state.cancel_no_effect(ticket));
    }
}

TEST(VfsNativeCoherency, OverlapAndReversedPublicationNeverRegressVersions) {
    coherence::Counter counter;
    coherence::Object object(counter);
    coherence::Stamp original{};
    ASSERT_TRUE(object.snapshot(original));
    coherence::Mutation content(object, coherence::Domain::CONTENT);
    ASSERT_EQ(content.status(), 0);
    coherence::Mutation metadata(object, coherence::Domain::METADATA);
    ASSERT_EQ(metadata.status(), 0);
    auto output = SENTINEL;
    EXPECT_FALSE(object.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
    metadata.publish();
    EXPECT_FALSE(object.snapshot(output));
    content.publish();
    ASSERT_TRUE(object.snapshot(output));
    EXPECT_EQ(output.object, original.object);
    EXPECT_EQ(output.content, original.content + 1);
    EXPECT_EQ(output.metadata, original.metadata + 2);
    EXPECT_EQ(output.directory, original.directory);
    content.publish();
    metadata.cancel_no_effect();
    EXPECT_TRUE(object.quiescent());
}

TEST(VfsNativeCoherency, NestedScopePublishesConservativelyAndCancelRequiresProof) {
    coherence::Counter counter;
    coherence::Object object(counter);
    coherence::Stamp before{}, after{};
    ASSERT_TRUE(object.snapshot(before));
    {
        coherence::Mutation outer(object, coherence::Domain::DIRECTORY);
        ASSERT_TRUE(outer);
        {
            coherence::Mutation inner(object, coherence::Domain::CONTENT);
            ASSERT_TRUE(inner);
        }
        EXPECT_FALSE(object.snapshot(after));
        outer.cancel_no_effect();
    }
    ASSERT_TRUE(object.snapshot(after));
    EXPECT_GT(after.content, before.content);
    EXPECT_EQ(after.metadata, after.content);
    EXPECT_EQ(after.directory, before.directory);
    before = after;
    {
        coherence::Mutation no_effect(object, coherence::Domain::ALL);
        ASSERT_TRUE(no_effect);
        no_effect.cancel_no_effect();
    }
    ASSERT_TRUE(object.snapshot(after));
    EXPECT_EQ(after, before);
}

TEST(VfsNativeCoherency, ClosureDrainsExistingGuardsAndRejectsSuccessors) {
    coherence::Counter counter;
    coherence::Object object(counter);
    coherence::Mutation accepted(object, coherence::Domain::CONTENT);
    ASSERT_TRUE(accepted);
    EXPECT_FALSE(object.close_admission());
    EXPECT_FALSE(object.quiescent());
    coherence::Mutation denied(object, coherence::Domain::CONTENT);
    EXPECT_EQ(denied.status(), -ESTALE);
    accepted.publish();
    EXPECT_TRUE(object.quiescent());
    EXPECT_TRUE(object.close_admission());
    auto output = SENTINEL;
    EXPECT_FALSE(object.snapshot(output));
    EXPECT_EQ(output, SENTINEL);
}

TEST(VfsNativeCoherency, ExhaustionAndInvalidDomainsCannotBeginEffects) {
    coherence::Counter counter(UINT64_MAX - 1);
    coherence::Object object(counter);
    {
        coherence::Mutation invalid(object, coherence::Domain::NONE);
        EXPECT_EQ(invalid.status(), -EINVAL);
        coherence::Mutation last(object, coherence::Domain::CONTENT);
        ASSERT_TRUE(last);
    }
    coherence::Mutation denied(object, coherence::Domain::ALL);
    EXPECT_EQ(denied.status(), -EOVERFLOW);
    coherence::Stamp output{};
    ASSERT_TRUE(object.snapshot(output));
    EXPECT_EQ(output.content, UINT64_MAX);
    EXPECT_EQ(output.metadata, UINT64_MAX);
    coherence::Object successor(counter);
    EXPECT_FALSE(successor.snapshot(output));
    EXPECT_EQ(output.content, UINT64_MAX);
}

TEST(VfsNativeCoherency, ConcurrentWritersAndReadersPublishOnlyCompleteStamps) {
    coherence::Counter counter;
    coherence::Object object(counter);
    std::array<std::thread, 5> workers;
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<unsigned> errors{0};
    constexpr unsigned EACH = 4096;
    for (unsigned lane = 0; lane < 4; ++lane) {
        workers[lane] = std::thread([&] {
            while (!start.load()) std::this_thread::yield();
            for (unsigned i = 0; i < EACH; ++i) {
                coherence::Mutation mutation(object, coherence::Domain::ALL);
                if (!mutation) ++errors;
                if ((i & 15U) == 0) std::this_thread::yield();
            }
        });
    }
    workers[4] = std::thread([&] {
        uint64_t previous = 0;
        while (!start.load()) std::this_thread::yield();
        while (!done.load()) {
            coherence::Stamp stamp{};
            if (object.snapshot(stamp)) {
                if (!coherence::valid_stamp(stamp) || stamp.content != stamp.metadata || stamp.content != stamp.directory ||
                    stamp.content < previous)
                    ++errors;
                previous = stamp.content;
            }
        }
    });
    start = true;
    for (unsigned lane = 0; lane < 4; ++lane) workers[lane].join();
    done = true;
    workers[4].join();
    EXPECT_EQ(errors.load(), 0U);
    coherence::Stamp final{};
    ASSERT_TRUE(object.snapshot(final));
    EXPECT_EQ(final.content, 1U + 4U * EACH);
    EXPECT_EQ(final.content, final.metadata);
    EXPECT_EQ(final.content, final.directory);
}

TEST(VfsNativeCoherency, AdmissionIsBoundedAndFailedNativeReservationRevokesCacheAuthority) {
    coherence::Counter counter;
    coherence::Object object(counter);
    std::vector<std::unique_ptr<coherence::Mutation>> guards;
    for (unsigned i = 0; i < 1024; ++i) {
        auto guard = std::make_unique<coherence::Mutation>(object, coherence::Domain::CONTENT);
        ASSERT_TRUE(*guard);
        guards.push_back(std::move(guard));
    }
    coherence::Mutation excess(object, coherence::Domain::CONTENT);
    EXPECT_EQ(excess.status(), -EAGAIN);
    guards.clear();
    coherence::Stamp available{};
    ASSERT_TRUE(object.snapshot(available));
    // One failed explicit request did not authorize or perform a native effect.
    EXPECT_EQ(available.content, 1025U);

    coherence::Counter exhausted(UINT64_MAX);
    coherence::Object native(exhausted);
    ASSERT_TRUE(native.snapshot(available));
    {
        // Native backend availability is preserved, but no old version remains
        // authority for a cache hit once unversioned effects could occur.
        coherence::Publication publication(native, coherence::Domain::CONTENT);
        EXPECT_FALSE(native.snapshot(available));
    }
    EXPECT_FALSE(native.snapshot(available));
    EXPECT_EQ(available.content, UINT64_MAX);
    EXPECT_TRUE(native.quiescent());
}
