#include <gtest/gtest.h>

#include <dev/usb/xhci_dma.hpp>
#include <dev/usb/xhci_terminal.hpp>
#include <vector>

namespace dma = ker::dev::dma;
namespace xd = ker::dev::usb::xhci_dma;
namespace {
struct Api {
    std::vector<int> calls;
    dma::Error preparation = dma::Error::none, completion = dma::Error::none, release_error = dma::Error::none;
    dma::Retirement proof{};
    auto prepare(dma::Handle h) -> dma::SubmissionResult {
        calls.push_back(1);
        return {.error = preparation, .submission = {h, 7, 0x123000, 4096}};
    }
    auto complete(dma::Submission s, size_t bytes, dma::Retirement p) -> dma::Error {
        EXPECT_EQ(s.sequence, 7U);
        EXPECT_EQ(bytes, 0U);
        proof = p;
        calls.push_back(2);
        return completion;
    }
    auto release(dma::Handle) -> dma::Error {
        calls.push_back(3);
        return release_error;
    }
    auto quarantine(dma::Handle) -> dma::Error {
        calls.push_back(4);
        return dma::Error::quarantined;
    }
};
void allocated(xd::Coherent& b) { b.allocation.handle.generation = 1; }
}  // namespace
TEST(XhciDma, EmptyRetirementDoesNothing) {
    Api a;
    xd::Coherent b;
    EXPECT_TRUE(b.retire(a, false));
    EXPECT_TRUE(a.calls.empty());
}
TEST(XhciDma, PreparedButUnpublishedNeedsNoHalt) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    EXPECT_TRUE(b.retire(a, false));
    EXPECT_EQ(a.proof, dma::Retirement::never_submitted);
    EXPECT_EQ(a.calls, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(b.allocation.handle.generation, 0U);
}
TEST(XhciDma, ExposedMemoryRequiresBothHardwareAndReaderProof) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    b.expose();
    EXPECT_FALSE(b.retire(a, false));
    EXPECT_EQ(a.calls, (std::vector<int>{1, 4}));
    EXPECT_EQ(b.allocation.handle.generation, 1U);
    EXPECT_EQ(b.submission.sequence, 7U);
}
TEST(XhciDma, HaltedAndDrainedReleasesExactlyOnce) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    b.expose();
    EXPECT_TRUE(b.retire(a, true));
    EXPECT_EQ(a.proof, dma::Retirement::device_quiesced);
    EXPECT_TRUE(b.retire(a, true));
    EXPECT_EQ(a.calls, (std::vector<int>{1, 2, 3}));
}
TEST(XhciDma, FailedPrepareUnwindsCpuOwnedStorage) {
    Api a;
    a.preparation = dma::Error::mapping_failed;
    xd::Coherent b;
    allocated(b);
    EXPECT_FALSE(b.prepare(a));
    EXPECT_EQ(b.submission.sequence, 0U);
    EXPECT_TRUE(b.retire(a, false));
    EXPECT_EQ(a.calls, (std::vector<int>{1, 3}));
}
TEST(XhciDma, FailedCompletionNeverReleases) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    b.expose();
    a.completion = dma::Error::busy;
    EXPECT_FALSE(b.retire(a, true));
    EXPECT_EQ(a.calls, (std::vector<int>{1, 2, 4}));
}
TEST(XhciDma, FailedInvalidationPinsAllocation) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    b.expose();
    a.release_error = dma::Error::quarantined;
    EXPECT_FALSE(b.retire(a, true));
    EXPECT_EQ(a.calls, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(b.allocation.handle.generation, 1U);
}

TEST(XhciDma, ScratchpadCountSeparatesHighLowAndRestoreFlag) {
    for (uint32_t count = 0; count < 1024; ++count) {
        uint32_t const fields = ((count >> 5U) << 21U) | ((count & 31U) << 27U);
        EXPECT_EQ(xd::scratchpad_count(fields), count);
        EXPECT_EQ(xd::scratchpad_count(fields | (1U << 26U) | 0x1fffffU), count);
    }
}

TEST(XhciDma, ReleasedStorageCanBeginANewUnpublishedLifetime) {
    Api a;
    xd::Coherent b;
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    b.expose();
    ASSERT_TRUE(b.retire(a, true));
    EXPECT_FALSE(b.exposed);
    allocated(b);
    ASSERT_TRUE(b.prepare(a));
    EXPECT_TRUE(b.retire(a, false));
    EXPECT_EQ(a.proof, dma::Retirement::never_submitted);
    EXPECT_EQ(a.calls, (std::vector<int>{1, 2, 3, 1, 2, 3}));
}

TEST(XhciDma, EndpointErrorsNeverGrantStopProof) {
    for (uint32_t state = 0; state < 8; ++state) {
        auto const action = xd::endpoint_stop_action(state | 0xfffffff8U);
        if (state == 0 || state == 3)
            EXPECT_EQ(action, xd::StopAction::none);
        else if (state == 1)
            EXPECT_EQ(action, xd::StopAction::command);
        else
            EXPECT_EQ(action, xd::StopAction::unproven);
    }
}

namespace {
struct StreamApi : Api {
    std::span<std::byte> bound{};
    dma::Direction direction{};
    dma::Error binding = dma::Error::none;
    size_t completed_bytes{};
    auto bind_streaming(dma::Handle, std::span<std::byte> view, dma::Direction d) -> dma::Error {
        bound = view;
        direction = d;
        return binding;
    }
    auto complete(dma::Submission, size_t bytes, dma::Retirement p) -> dma::Error {
        completed_bytes = bytes;
        proof = p;
        calls.push_back(2);
        return completion;
    }
};
void allocated(xd::Streaming& b, std::span<std::byte> storage) {
    b.allocation = {.handle = {.generation = 1}, .cpu = storage.data(), .size = storage.size()};
}
}  // namespace
TEST(XhciStreaming, OwnViewBindingSurvivesCallerReturnAndRequiresRetirement) {
    StreamApi api;
    std::array<std::byte, 4096> storage{};
    xd::Streaming buffer;
    allocated(buffer, storage);
    ASSERT_TRUE(buffer.prepare(api, dma::Direction::from_device));
    EXPECT_EQ(api.bound.data(), storage.data());
    EXPECT_EQ(api.bound.size(), storage.size());
    EXPECT_EQ(api.direction, dma::Direction::from_device);
    buffer.expose();
    EXPECT_FALSE(buffer.release(api));
    ASSERT_TRUE(buffer.complete(api, 17, dma::Retirement::command_completed));
    EXPECT_EQ(api.completed_bytes, 17U);
    EXPECT_EQ(api.proof, dma::Retirement::command_completed);
    EXPECT_TRUE(buffer.release(api));
    EXPECT_EQ(buffer.allocation.handle.generation, 0U);
}
TEST(XhciStreaming, NeverSubmittedAndStoppedHaveDifferentProofs) {
    for (bool exposed : {false, true}) {
        StreamApi api;
        std::array<std::byte, 4096> storage{};
        xd::Streaming buffer;
        allocated(buffer, storage);
        ASSERT_TRUE(buffer.prepare(api, dma::Direction::to_device));
        if (exposed) buffer.expose();
        EXPECT_TRUE(buffer.retire(api, exposed));
        EXPECT_EQ(api.proof, exposed ? dma::Retirement::device_quiesced : dma::Retirement::never_submitted);
    }
}
TEST(XhciStreaming, MissingStopInvalidLengthAndFalseUnpublishedProofPinStorage) {
    for (unsigned mode = 0; mode < 3; ++mode) {
        StreamApi api;
        std::array<std::byte, 4096> storage{};
        xd::Streaming buffer;
        allocated(buffer, storage);
        ASSERT_TRUE(buffer.prepare(api, dma::Direction::from_device));
        buffer.expose();
        EXPECT_FALSE(mode == 0 ? buffer.retire(api, false)
                               : buffer.complete(api, mode == 1 ? 4097 : 0,
                                                 mode == 1 ? dma::Retirement::command_completed : dma::Retirement::never_submitted));
        EXPECT_EQ(buffer.allocation.handle.generation, 1U);
        EXPECT_EQ(api.calls, (std::vector<int>{1, 4}));
    }
}
TEST(XhciStreaming, FailureNeverLosesOwnedHandle) {
    for (unsigned mode = 0; mode < 4; ++mode) {
        StreamApi api;
        std::array<std::byte, 4096> storage{};
        xd::Streaming buffer;
        allocated(buffer, storage);
        if (mode == 0) api.binding = dma::Error::invalid_argument;
        if (mode == 1) api.preparation = dma::Error::mapping_failed;
        if (mode < 2) {
            EXPECT_FALSE(buffer.prepare(api, dma::Direction::from_device));
            EXPECT_TRUE(buffer.retire(api, false));
        } else {
            ASSERT_TRUE(buffer.prepare(api, dma::Direction::from_device));
            buffer.expose();
            if (mode == 2) api.completion = dma::Error::busy;
            if (mode == 3) api.release_error = dma::Error::quarantined;
            EXPECT_FALSE(buffer.retire(api, true));
            EXPECT_EQ(buffer.allocation.handle.generation, 1U);
            EXPECT_EQ(api.calls.back(), 4);
        }
    }
}
TEST(XhciTransferPlan, ExhaustsEveryBoundaryOffsetAndFullSupportedLengths) {
    for (uint64_t low = 0; low < 65536; ++low) {
        for (size_t length : {size_t{0}, size_t{1}, size_t{4096}, size_t{65535}, size_t{65536}, xd::MAX_TRANSFER}) {
            auto const plan = xd::plan_transfer(0x100000 + low, length, 512);
            ASSERT_TRUE(plan.valid);
            ASSERT_LE(plan.count, 3U);
            size_t total = 0;
            for (size_t i = 0; i < plan.count; ++i) {
                auto const& segment = plan.segments.at(i);
                ASSERT_EQ(segment.address, 0x100000 + low + total);
                ASSERT_EQ(segment.offset, total);
                ASSERT_GT(segment.length, 0U);
                ASSERT_LE((segment.address & 65535U) + segment.length, 65536U);
                ASSERT_LE(segment.td_size, 31U);
                total += segment.length;
            }
            ASSERT_EQ(total, length);
        }
    }
}
TEST(XhciTransferPlan, CappedFirstTrbPreservesFixtureCoverageAndPacketAccounting) {
    for (uint64_t low = 0; low < 65536; ++low) {
        for (size_t bytes : {size_t{65}, size_t{1514}, size_t{1801}, size_t{2015}, size_t{9018}}) {
            for (size_t packet : {size_t{64}, size_t{512}, size_t{1024}}) {
                auto const plan = xd::plan_transfer(0x100000 + low, bytes, packet, packet);
                ASSERT_TRUE(plan.valid);
                ASSERT_LE(plan.count, 3U);
                ASSERT_LE(plan.segments.front().length, packet);
                if (bytes > packet) ASSERT_GE(plan.count, 2U);
                size_t covered = 0;
                for (size_t i = 0; i < plan.count; ++i) {
                    auto const& segment = plan.segments.at(i);
                    ASSERT_EQ(segment.address, 0x100000 + low + covered);
                    ASSERT_EQ(segment.offset, covered);
                    ASSERT_GT(segment.length, 0U);
                    ASSERT_LE((segment.address & 65535U) + segment.length, 65536U);
                    covered += segment.length;
                    auto const remaining = covered == bytes ? 0 : (bytes + packet - 1) / packet - covered / packet;
                    ASSERT_EQ(segment.td_size, std::min(remaining, size_t{31}));
                }
                ASSERT_EQ(covered, bytes);
            }
        }
    }
    EXPECT_FALSE(xd::plan_transfer(0x1000, 1, 512, 0).valid);
    // First cap plus two full boundary spans would need a fourth identity.
    auto const overflow = xd::plan_transfer(65534, xd::MAX_TRANSFER, 512, 1);
    EXPECT_FALSE(overflow.valid);
    EXPECT_EQ(overflow.count, 0U);
}
TEST(XhciTransferPlan, RejectsOverflowAndInvalidPacketAndLength) {
    EXPECT_FALSE(xd::plan_transfer(UINT64_MAX, 2, 512).valid);
    EXPECT_FALSE(xd::plan_transfer(0x1000, xd::MAX_TRANSFER + 1, 512).valid);
    EXPECT_FALSE(xd::plan_transfer(0x1000, 1, 0).valid);
    auto const plan = xd::plan_transfer(65535, 1025, 512);
    ASSERT_TRUE(plan.valid);
    ASSERT_EQ(plan.count, 2U);
    EXPECT_EQ(plan.segments[0].length, 1U);
    EXPECT_EQ(plan.segments[0].td_size, 3U);
    EXPECT_EQ(plan.segments[1].td_size, 0U);
}

namespace {
struct TerminalBackend {
    unsigned failures{};
    std::vector<unsigned> calls;
    auto step(unsigned id) -> bool {
        calls.push_back(id);
        return (failures & (1U << id)) == 0;
    }
    void close_lifecycle() { (void)step(0); }
    auto drain_lifecycle() -> bool { return step(1); }
    void close_io() { (void)step(2); }
    auto close_io_and_slots() -> bool { return step(3); }
    auto drain_io() -> bool { return step(4); }
    auto drain_classes() -> bool { return step(5); }
    auto mask_interrupts() -> bool { return step(6); }
    auto halt() -> bool { return step(7); }
    auto disable_bus_master() -> bool { return step(8); }
    void drain_interrupts() { (void)step(9); }
    void quarantine() { (void)step(10); }
    auto release() -> bool { return step(11); }
    auto called(unsigned id) const -> bool { return std::ranges::find(calls, id) != calls.end(); }
};
}  // namespace
TEST(XhciTerminal, RetirementRequiresEverySoftwareAndHardwareProof) {
    namespace terminal = ker::dev::usb::xhci_terminal;
    for (unsigned mask = 0; mask < 4096; ++mask) {
        TerminalBackend backend{mask, {}};
        auto const result = terminal::retire(backend);
        bool const lifecycle = (mask & (1U << 1)) == 0;
        bool const readers = (mask & (1U << 4)) == 0;
        bool const classes = (mask & ((1U << 3) | (1U << 5))) == 0;
        bool const hardware = (mask & ((1U << 6) | (1U << 7) | (1U << 8))) == 0;
        EXPECT_TRUE(backend.called(2));
        EXPECT_TRUE(backend.called(8));
        EXPECT_EQ(backend.called(11), lifecycle && readers && classes && hardware);
        EXPECT_EQ(backend.called(10), lifecycle && readers && classes && !hardware);
        EXPECT_EQ(result == terminal::Result::released, lifecycle && readers && classes && hardware && (mask & (1U << 11)) == 0);
        if (!lifecycle) EXPECT_EQ(backend.calls, (std::vector<unsigned>{0, 1, 2, 8}));
        EXPECT_EQ(backend.called(9), lifecycle && (mask & (1U << 6)) == 0);
    }
}
TEST(XhciTerminal, PublicationClosesBeforeCancellationAndHaltPrecedesRelease) {
    TerminalBackend backend;
    EXPECT_EQ(ker::dev::usb::xhci_terminal::retire(backend), ker::dev::usb::xhci_terminal::Result::released);
    EXPECT_EQ(backend.calls, (std::vector<unsigned>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11}));
}
