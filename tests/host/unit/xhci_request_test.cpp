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

namespace {
// Hide a result exactly as snapshot() does during its single-writer transition.
struct DeferredSnapshotTable {
    RequestTable table;
    bool hide_once = false;
    bool hide_timeout = false;
    auto snapshot(RequestHandle h, RequestSnapshot& s) -> bool {
        if (hide_once) {
            hide_once = false;
            return false;
        }
        return table.snapshot(h, s);
    }
    auto try_timeout(RequestHandle h, uint64_t now, int result) -> TimeoutDisposition {
        auto const disposition = table.try_timeout(h, now, result);
        if (hide_timeout && disposition == TimeoutDisposition::TIMED_OUT) hide_once = true;
        return disposition;
    }
};

TEST(XhciRequestWait, UnavailableSnapshotWaitsForRealCompletion) {
    DeferredSnapshotTable requests;
    std::array<uint64_t, 1> const trbs = {0x7000};
    auto const h = requests.table.reserve({.kind = RequestKind::COMMAND, .trb_phys = trbs, .deadline_us = 100});
    requests.hide_once = true;
    unsigned yields = 0;
    RequestSnapshot snapshot{};
    auto const result = await_result(
        requests, h, 100, snapshot, [] { return 1U; },
        [&] {
            ++yields;
            EXPECT_FALSE(requests.table.release(h));
            EXPECT_EQ(requests.table.complete_event(command_event(0x7000, 1)).disposition, CompletionDisposition::COMPLETED);
        },
        -110, -5);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(yields, 1U);
    EXPECT_EQ(snapshot.state, RequestState::COMPLETED);
    EXPECT_TRUE(requests.table.release(h));
}

TEST(XhciRequestWait, TimeoutPublicationMustFinishBeforeReturning) {
    DeferredSnapshotTable requests;
    std::array<uint64_t, 1> const trbs = {0x8000};
    auto const h = requests.table.reserve({.kind = RequestKind::COMMAND, .trb_phys = trbs, .deadline_us = 10});
    requests.hide_timeout = true;
    unsigned yields = 0;
    RequestSnapshot snapshot{};
    auto const result = await_result(requests, h, 10, snapshot, [] { return 10U; }, [&] { ++yields; }, -110, -5);
    EXPECT_EQ(result, -110);
    EXPECT_EQ(yields, 1U);
    EXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
    EXPECT_FALSE(requests.table.release(h));
    EXPECT_TRUE(requests.table.mark_timed_out_retired(h));
    EXPECT_TRUE(requests.table.release(h));
}

TEST(XhciRequestWait, StaleHandleReturnsUnprovenOwnership) {
    DeferredSnapshotTable requests;
    RequestSnapshot snapshot{};
    auto const result =
        await_result(requests, {}, 100, snapshot, [] { return 1U; }, [] { ADD_FAILURE() << "stale handle yielded"; }, -110, -5);
    EXPECT_EQ(result, -5);
    EXPECT_EQ(snapshot.result, -5);
    EXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
}
TEST(XhciRequestWait, ReleasedRecordCannotBecomeACompletedSnapshot) {
    DeferredSnapshotTable requests;
    std::array<uint64_t, 1> const trbs = {0x9000};
    auto const h = requests.table.reserve({.kind = RequestKind::COMMAND, .trb_phys = trbs, .deadline_us = 10});
    ASSERT_EQ(requests.table.complete_event(command_event(0x9000, 1)).disposition, CompletionDisposition::COMPLETED);
    ASSERT_TRUE(requests.table.release(h));
    RequestSnapshot snapshot{};
    auto const result =
        await_result(requests, h, 10, snapshot, [] { return 1U; }, [] { ADD_FAILURE() << "released handle yielded"; }, -110, -5);
    EXPECT_EQ(result, -5);
    EXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
}
}  // namespace

namespace {
TEST(XhciRequestTerminal, ControlShortDataDoesNotRetireBeforeStatus) {
    RequestTable requests;
    std::array<uint64_t, 3> const ids{0x1000, 0x1010, 0x1020};
    auto const h = requests.reserve({.kind = RequestKind::TRANSFER,
                                     .slot_id = 1,
                                     .dci = 1,
                                     .trb_phys = ids,
                                     .requested_length = 128,
                                     .deadline_us = 100,
                                     .terminal_trb = 0x1020});
    ASSERT_TRUE(h.valid());
    auto short_event = transfer_event(0x1010, 1, 1);
    short_event.status = (ker::dev::usb::TRB_CC_SHORT_PKT << 24U) | 7;
    EXPECT_EQ(requests.complete_event(short_event).disposition, CompletionDisposition::PROGRESS);
    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(h, snapshot));
    EXPECT_EQ(snapshot.state, RequestState::SUBMITTED);
    EXPECT_TRUE(snapshot.short_seen);
    EXPECT_EQ(snapshot.short_identity, 0x1010U);
    EXPECT_EQ(snapshot.short_residual, 7U);
    EXPECT_FALSE(requests.release(h));
    EXPECT_EQ(requests.complete_event(transfer_event(0x1020, 1, 1)).disposition, CompletionDisposition::COMPLETED);
    ASSERT_TRUE(requests.snapshot(h, snapshot));
    EXPECT_TRUE(snapshot.short_seen);
    EXPECT_FALSE(snapshot.event_uses_data);
    EXPECT_TRUE(requests.release(h));
}
TEST(XhciRequestTerminal, BulkOnlyEventDataRetiresAndOldCookieCannotMatch) {
    RequestTable requests;
    std::array<uint64_t, 2> const ids{0x2000, 0x2010};
    auto const h = requests.reserve({.kind = RequestKind::TRANSFER,
                                     .slot_id = 2,
                                     .dci = 3,
                                     .trb_phys = ids,
                                     .requested_length = 500,
                                     .deadline_us = 100,
                                     .event_data_valid = true,
                                     .event_data = 55,
                                     .terminal_event_data = true});
    ASSERT_TRUE(h.valid());
    EXPECT_EQ(requests.complete_event(transfer_event(0x2000, 2, 3)).disposition, CompletionDisposition::PROGRESS);
    auto event = transfer_event(54, 2, 3);
    event.control |= EVENT_DATA_FLAG;
    event.status = (ker::dev::usb::TRB_CC_SHORT_PKT << 24U) | 17;
    EXPECT_EQ(requests.complete_event(event).disposition, CompletionDisposition::UNKNOWN);
    event.param = 55;
    EXPECT_EQ(requests.complete_event(event).disposition, CompletionDisposition::COMPLETED);
    RequestSnapshot snapshot{};
    ASSERT_TRUE(requests.snapshot(h, snapshot));
    EXPECT_TRUE(snapshot.event_uses_data);
    EXPECT_EQ(snapshot.residual, 17U);
    EXPECT_EQ(snapshot.result, 0);
    EXPECT_TRUE(requests.release(h));
}
TEST(XhciRequestTerminal, IntermediateErrorPublishesFailureAndCancellationWinsAfterProgress) {
    for (bool cancel : {false, true}) {
        RequestTable requests;
        std::array<uint64_t, 2> const ids{0x3000, 0x3010};
        auto const h = requests.reserve(
            {.kind = RequestKind::TRANSFER, .slot_id = 3, .dci = 1, .trb_phys = ids, .deadline_us = 100, .terminal_trb = 0x3010});
        auto event = transfer_event(0x3000, 3, 1);
        if (cancel) {
            EXPECT_EQ(requests.complete_event(event).disposition, CompletionDisposition::PROGRESS);
            EXPECT_EQ(requests.cancel(h, -125), TimeoutDisposition::TIMED_OUT);
            EXPECT_EQ(requests.complete_event(transfer_event(0x3010, 3, 1)).disposition, CompletionDisposition::LATE_AFTER_TIMEOUT);
            EXPECT_FALSE(requests.release(h));
        } else {
            event.status = 6U << 24U;
            EXPECT_EQ(requests.complete_event(event).disposition, CompletionDisposition::COMPLETED);
            RequestSnapshot snapshot{};
            ASSERT_TRUE(requests.snapshot(h, snapshot));
            EXPECT_NE(snapshot.result, 0);
        }
    }
}
}  // namespace
