#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/usb/xhci_request.hpp>

namespace {

using ker::dev::usb::Trb;
using ker::dev::usb::TRB_CC_SUCCESS;
using ker::dev::usb::TRB_CMD_COMPLETION;
using ker::dev::usb::TRB_CYCLE;
using ker::dev::usb::TRB_TRANSFER_EVENT;
using namespace ker::dev::usb::xhci_request;

using RequestTable = FixedRequestTable<4, 4>;

constexpr auto command_event(uint64_t trb_phys, uint8_t slot_id) -> Trb {
    return {
        .param = trb_phys,
        .status = TRB_CC_SUCCESS << 24U,
        .control = TRB_CMD_COMPLETION | (static_cast<uint32_t>(slot_id) << EVENT_SLOT_SHIFT) | TRB_CYCLE,
    };
}

constexpr auto transfer_event(uint64_t trb_phys, uint8_t slot_id, uint8_t dci) -> Trb {
    return {
        .param = trb_phys,
        .status = TRB_CC_SUCCESS << 24U,
        .control = TRB_TRANSFER_EVENT | (static_cast<uint32_t>(slot_id) << EVENT_SLOT_SHIFT) |
                   (static_cast<uint32_t>(dci) << EVENT_DCI_SHIFT) | TRB_CYCLE,
    };
}

struct TimeoutSinkContext {
    std::array<RequestHandle, RequestTable::capacity()> handles{};
    size_t count{};
};

void timeout_sink(void* opaque, RequestHandle request) {
    auto* context = static_cast<TimeoutSinkContext*>(opaque);
    if (context == nullptr || context->count >= context->handles.size()) {
        return;
    }
    context->handles.at(context->count++) = request;
}

TEST(XhciRequestGeneration, ReleasedSlotRejectsEveryStaleHandleOperation) {
    FixedRequestTable<1, 1> requests{};
    std::array<uint64_t, 1> const FIRST_TRBS = {0x1000};
    RequestHandle const FIRST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = FIRST_TRBS,
    });
    ASSERT_TRUE(FIRST.valid());
    ASSERT_EQ(requests.complete_event(command_event(0x1000, 1)).disposition, CompletionDisposition::COMPLETED);
    ASSERT_TRUE(requests.release(FIRST));

    std::array<uint64_t, 1> const SECOND_TRBS = {0x2000};
    RequestHandle const SECOND = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = SECOND_TRBS,
        .deadline_us = 20,
    });
    ASSERT_TRUE(SECOND.valid());
    EXPECT_EQ(SECOND.index, FIRST.index);
    EXPECT_NE(SECOND.generation, FIRST.generation);

    RequestSnapshot snapshot{};
    EXPECT_FALSE(requests.snapshot(FIRST, snapshot));
    EXPECT_EQ(requests.try_timeout(FIRST, 20, -110), TimeoutDisposition::STALE_HANDLE);
    EXPECT_EQ(requests.cancel(FIRST, -125), TimeoutDisposition::STALE_HANDLE);
    EXPECT_FALSE(requests.mark_timed_out_retired(FIRST));
    EXPECT_FALSE(requests.release(FIRST));
    EXPECT_EQ(requests.complete_event(command_event(0x1000, 1)).disposition, CompletionDisposition::UNKNOWN);

    EXPECT_EQ(requests.complete_event(command_event(0x2000, 2)).disposition, CompletionDisposition::COMPLETED);
    EXPECT_TRUE(requests.release(SECOND));
}

TEST(XhciRequestCompletion, DuplicateCompletionCannotOverwritePublishedResult) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x3000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = TRBS,
    });
    ASSERT_TRUE(REQUEST.valid());

    Trb const EVENT = command_event(0x3000, 3);
    EXPECT_EQ(requests.complete_event(EVENT, -77).disposition, CompletionDisposition::COMPLETED);
    EXPECT_EQ(requests.complete_event(EVENT, -99).disposition, CompletionDisposition::DUPLICATE);

    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(REQUEST, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::COMPLETED);
    EXPECT_EQ(snapshot.result, -77);
    EXPECT_EQ(snapshot.event_slot_id, 3U);
    EXPECT_TRUE(requests.release(REQUEST));
}

TEST(XhciRequestTimeout, TimeoutQuarantinesDmaUntilRetirementBarrier) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x4000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 4,
        .dci = 5,
        .trb_phys = TRBS,
        .requested_length = 512,
        .deadline_us = 100,
    });
    ASSERT_TRUE(REQUEST.valid());

    EXPECT_EQ(requests.try_timeout(REQUEST, 99, -110), TimeoutDisposition::NOT_DUE);
    EXPECT_EQ(requests.try_timeout(REQUEST, 100, -110), TimeoutDisposition::TIMED_OUT);
    EXPECT_EQ(requests.complete_event(transfer_event(0x4000, 4, 5)).disposition, CompletionDisposition::LATE_AFTER_TIMEOUT);
    EXPECT_FALSE(requests.release(REQUEST));

    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(REQUEST, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
    EXPECT_EQ(snapshot.result, -110);
    EXPECT_TRUE(requests.mark_timed_out_retired(REQUEST));
    ASSERT_TRUE(requests.snapshot(REQUEST, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::RETIRED);
    EXPECT_EQ(snapshot.result, -110);
    EXPECT_TRUE(requests.release(REQUEST));
    EXPECT_FALSE(requests.snapshot(REQUEST, snapshot));
}

TEST(XhciRequestTimeout, CompletionWinsAgainstTimeoutAndCancellation) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x5000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = TRBS,
        .deadline_us = 10,
    });
    ASSERT_TRUE(REQUEST.valid());

    EXPECT_EQ(requests.complete_event(command_event(0x5000, 5)).disposition, CompletionDisposition::COMPLETED);
    EXPECT_EQ(requests.try_timeout(REQUEST, 10, -110), TimeoutDisposition::COMPLETION_WON);
    EXPECT_EQ(requests.cancel(REQUEST, -125), TimeoutDisposition::COMPLETION_WON);

    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(REQUEST, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::COMPLETED);
    EXPECT_EQ(snapshot.result, 0);
    EXPECT_TRUE(requests.release(REQUEST));
}

TEST(XhciRequestCancel, RepeatedCancelPreservesFirstResultAndQuarantine) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x6000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 6,
        .dci = 7,
        .trb_phys = TRBS,
    });
    ASSERT_TRUE(REQUEST.valid());

    EXPECT_EQ(requests.cancel(REQUEST, -125), TimeoutDisposition::TIMED_OUT);
    EXPECT_EQ(requests.cancel(REQUEST, -126), TimeoutDisposition::ALREADY_TIMED_OUT);
    EXPECT_EQ(requests.complete_event(transfer_event(0x6000, 6, 7)).disposition, CompletionDisposition::LATE_AFTER_TIMEOUT);

    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(REQUEST, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
    EXPECT_EQ(snapshot.result, -125);
    EXPECT_FALSE(requests.release(REQUEST));
    EXPECT_TRUE(requests.mark_timed_out_retired(REQUEST));
    EXPECT_TRUE(requests.release(REQUEST));
}

TEST(XhciRequestTimeout, FixedScanExaminesCapacityAndExpiresOnlyDueRequests) {
    RequestTable requests{};
    std::array<uint64_t, 1> const FIRST_TRBS = {0x7000};
    std::array<uint64_t, 1> const SECOND_TRBS = {0x8000};
    RequestHandle const FIRST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = FIRST_TRBS,
        .deadline_us = 10,
    });
    RequestHandle const SECOND = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = SECOND_TRBS,
        .deadline_us = 20,
    });
    ASSERT_TRUE(FIRST.valid());
    ASSERT_TRUE(SECOND.valid());

    TimeoutSinkContext sink{};
    auto const SCAN = requests.expire_due(15, -110, timeout_sink, &sink);
    EXPECT_EQ(SCAN.examined, RequestTable::capacity());
    EXPECT_EQ(SCAN.timed_out, 1U);
    ASSERT_EQ(sink.count, 1U);
    EXPECT_EQ(sink.handles.at(0), FIRST);

    RequestSnapshot first{};
    RequestSnapshot second{};
    ASSERT_TRUE(requests.snapshot(FIRST, first));
    ASSERT_TRUE(requests.snapshot(SECOND, second));
    EXPECT_EQ(first.state, RequestState::TIMED_OUT);
    EXPECT_EQ(second.state, RequestState::SUBMITTED);

    EXPECT_TRUE(requests.mark_timed_out_retired(FIRST));
    EXPECT_TRUE(requests.release(FIRST));
    EXPECT_EQ(requests.complete_event(command_event(0x8000, 8)).disposition, CompletionDisposition::COMPLETED);
    EXPECT_TRUE(requests.release(SECOND));
}

}  // namespace
