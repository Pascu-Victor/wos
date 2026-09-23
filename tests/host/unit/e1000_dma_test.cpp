#include <gtest/gtest.h>

#include <array>
#include <dev/e1000e/dma.hpp>
#include <vector>

namespace dma = ker::dev::dma;
namespace managed = ker::dev::e1000e::managed;
namespace {
struct Api {
    std::vector<int> events;
    dma::Error bind_error = dma::Error::none, prepare_error = dma::Error::none, complete_error = dma::Error::none;
    dma::Error release_error = dma::Error::none, quarantine_error = dma::Error::quarantined;
    dma::Retirement proof{};
    size_t copied = 0;
    uint64_t sequence = 0;
    auto bind_streaming(dma::Handle, std::span<std::byte>, dma::Direction) -> dma::Error {
        events.push_back(1);
        return bind_error;
    }
    auto prepare(dma::Handle h) -> dma::SubmissionResult {
        events.push_back(2);
        if (prepare_error != dma::Error::none) return {.error = prepare_error};
        return {.submission = {h, ++sequence, 0x123000, 2048}};
    }
    auto complete(dma::Submission s, size_t bytes, dma::Retirement p) -> dma::Error {
        EXPECT_EQ(s.sequence, sequence);
        copied = bytes;
        proof = p;
        events.push_back(3);
        return complete_error;
    }
    auto release(dma::Handle) -> dma::Error {
        events.push_back(4);
        return release_error;
    }
    auto quarantine(dma::Handle) -> dma::Error {
        events.push_back(5);
        return quarantine_error;
    }
};
void allocated(managed::Buffer& b) { b.allocation.handle.generation = 1; }
}  // namespace

TEST(E1000Dma, ReceiveRequiresCompleteSingleFrameAndBoundedLength) {
    for (unsigned status = 0; status < 256; ++status) {
        for (size_t bytes : {size_t{0}, size_t{1}, size_t{2048}, size_t{2049}, SIZE_MAX}) {
            EXPECT_EQ(managed::rx_valid(status, 0, bytes, false), (status & 3U) == 3U && bytes > 0 && bytes <= 2048);
            EXPECT_FALSE(managed::rx_valid(status, 1, bytes, false));
            EXPECT_FALSE(managed::rx_valid(status, 0, bytes, true));
        }
    }
}
TEST(E1000Dma, TransmitSpanCannotEscapePacketStorageOrNarrowLength) {
    EXPECT_TRUE(managed::tx_valid(0x1000, 10240, 0x1080, 1514));
    EXPECT_FALSE(managed::tx_valid(0x1000, 10240, 0xfff, 1));
    EXPECT_FALSE(managed::tx_valid(0x1000, 10240, 0x1000 + 10240, 1));
    EXPECT_FALSE(managed::tx_valid(0x1000, 10240, 0x1000, 0));
    EXPECT_FALSE(managed::tx_valid(0x1000, SIZE_MAX, 0x1000, 65536));
    EXPECT_FALSE(managed::tx_valid(0x1000, 10240, UINTPTR_MAX, 1));
}
TEST(E1000Dma, UnpublishedPreparedStorageUnwindsWithoutHardwareProof) {
    Api api;
    managed::Buffer b;
    allocated(b);
    EXPECT_FALSE(b.expose());
    ASSERT_TRUE(b.prepare(api));
    EXPECT_FALSE(b.prepare(api));
    ASSERT_TRUE(b.retire(api, false));
    EXPECT_EQ(api.proof, dma::Retirement::never_submitted);
    EXPECT_EQ(api.events, (std::vector<int>{2, 3, 4}));
    EXPECT_EQ(b.allocation.handle.generation, 0U);
    EXPECT_TRUE(b.retire(api, false));
    EXPECT_EQ(api.events.size(), 3U);
}
TEST(E1000Dma, BindAndPrepareFailuresNeverCreateADeviceAddress) {
    Api api;
    managed::Buffer b;
    allocated(b);
    std::array<std::byte, 64> bytes{};
    api.bind_error = dma::Error::busy;
    EXPECT_FALSE(b.bind(api, bytes, dma::Direction::from_device));
    EXPECT_EQ(api.events, (std::vector<int>{1}));
    api.bind_error = dma::Error::none;
    api.prepare_error = dma::Error::exhausted;
    EXPECT_FALSE(b.bind(api, bytes, dma::Direction::from_device));
    EXPECT_FALSE(b.expose());
    EXPECT_EQ(b.submission.sequence, 0U);
}
TEST(E1000Dma, CompletionConsumesSubmissionButNotHistoricalExposure) {
    Api api;
    managed::Buffer b;
    allocated(b);
    std::array<std::byte, 64> bytes{};
    ASSERT_TRUE(b.bind(api, bytes, dma::Direction::from_device));
    ASSERT_TRUE(b.expose());
    ASSERT_TRUE(b.complete(api, 64));
    EXPECT_EQ(api.copied, 64U);
    EXPECT_EQ(api.proof, dma::Retirement::command_completed);
    EXPECT_FALSE(b.complete(api, 64));
    EXPECT_TRUE(b.exposed);
    ASSERT_TRUE(b.retire(api, false));
    EXPECT_EQ(api.events.back(), 5);
    EXPECT_NE(b.allocation.handle.generation, 0U);
}
TEST(E1000Dma, QuiescedRetirementCompletesBeforeRelease) {
    Api api;
    managed::Buffer b;
    allocated(b);
    ASSERT_TRUE(b.prepare(api));
    ASSERT_TRUE(b.expose());
    ASSERT_TRUE(b.retire(api, true));
    EXPECT_EQ(api.proof, dma::Retirement::device_quiesced);
    EXPECT_EQ(api.events, (std::vector<int>{2, 3, 4}));
}
TEST(E1000Dma, CompletionFailureRetainsSubmissionAndPinsBeforeBorrowRelease) {
    Api api;
    managed::Buffer b;
    allocated(b);
    ASSERT_TRUE(b.prepare(api));
    ASSERT_TRUE(b.expose());
    api.complete_error = dma::Error::invalid_completion;
    EXPECT_FALSE(b.complete(api, 7));
    EXPECT_NE(b.submission.sequence, 0U);
    ASSERT_TRUE(b.retire(api, true));
    EXPECT_EQ(api.events, (std::vector<int>{2, 3, 3, 5}));
    EXPECT_NE(b.allocation.handle.generation, 0U);
}
TEST(E1000Dma, BusyQuarantineCannotDetachCpuOwner) {
    Api api;
    managed::Buffer b;
    allocated(b);
    ASSERT_TRUE(b.prepare(api));
    ASSERT_TRUE(b.expose());
    api.quarantine_error = dma::Error::busy;
    EXPECT_FALSE(b.retire(api, false));
    EXPECT_EQ(api.events, (std::vector<int>{2, 5}));
    EXPECT_NE(b.submission.sequence, 0U);
}
