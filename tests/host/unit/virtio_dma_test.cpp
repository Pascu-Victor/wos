#include <gtest/gtest.h>

#include <array>
#include <dev/virtio/virtio_dma.hpp>
#include <vector>

namespace dma = ker::dev::dma;
namespace managed = ker::dev::virtio::managed;
namespace {
struct Api {
    std::array<std::byte, 4096> bytes{};
    std::vector<int> events;
    dma::Error allocation_error = dma::Error::none;
    dma::Error prepare_error = dma::Error::none;
    dma::Error complete_error = dma::Error::none;
    dma::Error release_error = dma::Error::none;
    dma::Retirement proof{};
    dma::Handle handle{this, nullptr, 12, 3};
    auto allocate_coherent(const ker::dev::pci::PCIDevice*, size_t size, size_t align, uint64_t mask) -> dma::AllocationResult {
        events.push_back(1);
        EXPECT_EQ(align, 4096U);
        EXPECT_EQ(mask, managed::LEGACY_QUEUE_MASK);
        if (allocation_error != dma::Error::none) return {.error = allocation_error};
        return {.allocation = {handle, bytes.data(), size}};
    }
    auto prepare(dma::Handle h) -> dma::SubmissionResult {
        events.push_back(2);
        EXPECT_EQ(h.generation, handle.generation);
        if (prepare_error != dma::Error::none) return {.error = prepare_error};
        return {.submission = {h, 9, 0x10000, bytes.size()}};
    }
    auto complete(dma::Submission s, size_t size, dma::Retirement p) -> dma::Error {
        events.push_back(3);
        EXPECT_EQ(s.sequence, 9U);
        EXPECT_EQ(size, 0U);
        proof = p;
        return complete_error;
    }
    auto release(dma::Handle h) -> dma::Error {
        events.push_back(4);
        EXPECT_EQ(h.generation, handle.generation);
        return release_error;
    }
    auto quarantine(dma::Handle h) -> dma::Error {
        events.push_back(5);
        EXPECT_EQ(h.generation, handle.generation);
        return dma::Error::none;
    }
};
auto allocate(managed::Region& r, Api& api) -> bool { return r.allocate(api, nullptr, 4096, managed::LEGACY_QUEUE_MASK); }
}  // namespace

TEST(VirtioDma, EverySupportedRingFitsItsOwnedLayout) {
    for (uint16_t size = 1; size <= 256; size *= 2) {
        auto const l = managed::layout(size);
        EXPECT_EQ(l.available, 16U * size);
        EXPECT_GE(l.used, l.available + 2U * (3 + size));
        EXPECT_EQ(l.used % 4096, 0U);
        EXPECT_GE(l.bytes, l.used + 6U + 8U * size);
        EXPECT_EQ(l.bytes % 4096, 0U);
        EXPECT_LE(l.bytes, 12288U);
    }
    for (uint16_t size : {0, 3, 255, 257, 65535}) EXPECT_EQ(managed::layout(size).bytes, 0U);
    EXPECT_EQ(managed::LEGACY_QUEUE_MASK >> 12, UINT32_MAX);
    EXPECT_EQ((managed::LEGACY_QUEUE_MASK + 1) >> 12, uint64_t{1} << 32);
}
TEST(VirtioDma, UnpreparedAndUnpublishedUnwindNeverClaimHardwareRetirement) {
    Api api;
    managed::Region region;
    EXPECT_FALSE(region.expose());
    ASSERT_TRUE(allocate(region, api));
    EXPECT_FALSE(allocate(region, api));
    ASSERT_TRUE(region.retire(api, false));
    EXPECT_EQ(api.events, (std::vector<int>{1, 4}));
    EXPECT_EQ(region.allocation.cpu, nullptr);
    EXPECT_TRUE(region.retire(api, false));
}
TEST(VirtioDma, PreparedUnpublishedUnwindUsesNeverSubmittedProof) {
    Api api;
    managed::Region region;
    ASSERT_TRUE(allocate(region, api));
    ASSERT_TRUE(region.prepare(api));
    EXPECT_FALSE(region.prepare(api));
    ASSERT_TRUE(region.retire(api, false));
    EXPECT_EQ(api.proof, dma::Retirement::never_submitted);
    EXPECT_EQ(api.events, (std::vector<int>{1, 2, 3, 4}));
}
TEST(VirtioDma, ExposedRegionRequiresQuiescenceEvenAfterACommandAck) {
    Api api;
    managed::Region region;
    ASSERT_TRUE(allocate(region, api));
    ASSERT_TRUE(region.prepare(api));
    ASSERT_TRUE(region.expose());
    EXPECT_FALSE(region.expose());
    EXPECT_FALSE(region.retire(api, false));
    EXPECT_EQ(api.events, (std::vector<int>{1, 2, 5}));
    EXPECT_EQ(region.submission.sequence, 9U);
    EXPECT_NE(region.allocation.cpu, nullptr);
}
TEST(VirtioDma, ResetAndReaderDrainPermitCompletionBeforeRelease) {
    Api api;
    managed::Region region;
    ASSERT_TRUE(allocate(region, api));
    ASSERT_TRUE(region.prepare(api));
    ASSERT_TRUE(region.expose());
    ASSERT_TRUE(region.retire(api, true));
    EXPECT_EQ(api.proof, dma::Retirement::device_quiesced);
    EXPECT_EQ(api.events, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(region.allocation.handle.generation, 0U);
    EXPECT_EQ(region.submission.sequence, 0U);
    EXPECT_FALSE(region.exposed);
}
TEST(VirtioDma, AllocationAndPreparationFailureLeaveAuditableOwnership) {
    Api api;
    managed::Region region;
    api.allocation_error = dma::Error::exhausted;
    EXPECT_FALSE(allocate(region, api));
    EXPECT_TRUE(region.retire(api, false));
    api.allocation_error = dma::Error::none;
    ASSERT_TRUE(allocate(region, api));
    api.prepare_error = dma::Error::invalid_handle;
    EXPECT_FALSE(region.prepare(api));
    EXPECT_FALSE(region.expose());
    EXPECT_TRUE(region.retire(api, false));
    EXPECT_EQ(api.events, (std::vector<int>{1, 1, 2, 4}));
}
TEST(VirtioDma, FailedCompletionPinsAndFailedReleaseRetainsHandle) {
    for (bool completion : {false, true}) {
        Api api;
        managed::Region region;
        ASSERT_TRUE(allocate(region, api));
        ASSERT_TRUE(region.prepare(api));
        ASSERT_TRUE(region.expose());
        if (completion)
            api.complete_error = dma::Error::invalid_completion;
        else
            api.release_error = dma::Error::quarantined;
        EXPECT_FALSE(region.retire(api, true));
        EXPECT_EQ(region.allocation.handle.generation, 12U);
        EXPECT_EQ(api.events, (std::vector<int>{1, 2, 3, completion ? 5 : 4}));
    }
}
TEST(VirtioDma, ResetRequiresAcknowledgementAndIsBoundedEvenWithStalledClock) {
    struct Reset {
        size_t writes = 0, reads = 0, pauses = 0, ack_after = SIZE_MAX;
        bool timeout = false;
        void write_reset() { ++writes; }
        auto read_status() -> uint8_t { return ++reads >= ack_after ? 0 : 4; }
        auto expired() -> bool { return timeout; }
        void relax() { ++pauses; }
    } b;
    EXPECT_FALSE(managed::reset(b, 7));
    EXPECT_EQ(b.writes, 1U);
    EXPECT_EQ(b.reads, 7U);
    EXPECT_EQ(b.pauses, 7U);
    b = {};
    b.timeout = true;
    EXPECT_FALSE(managed::reset(b, 7));
    EXPECT_EQ(b.reads, 1U);
    b = {};
    b.ack_after = 3;
    EXPECT_TRUE(managed::reset(b, 7));
    EXPECT_EQ(b.reads, 3U);
    EXPECT_EQ(b.pauses, 2U);
}
