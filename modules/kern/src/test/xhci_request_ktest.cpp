#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/usb/xhci_request.hpp>
#include <test/ktest.hpp>

namespace {

using ker::dev::usb::Trb;
using ker::dev::usb::TRB_CC_SHORT_PKT;
using ker::dev::usb::TRB_CC_SUCCESS;
using ker::dev::usb::TRB_CMD_COMPLETION;
using ker::dev::usb::TRB_CYCLE;
using ker::dev::usb::TRB_TRANSFER_EVENT;
using namespace ker::dev::usb::xhci_request;

using RequestTable = FixedRequestTable<4, 4>;

constexpr auto command_event(uint64_t trb_phys, uint32_t completion_code, uint8_t slot_id, bool cycle = true) -> Trb {
    return {
        .param = trb_phys,
        .status = completion_code << 24U,
        .control = TRB_CMD_COMPLETION | (static_cast<uint32_t>(slot_id) << EVENT_SLOT_SHIFT) | (cycle ? TRB_CYCLE : 0),
    };
}

constexpr auto transfer_event(uint64_t identity, uint32_t completion_code, uint32_t residual, uint8_t slot_id, uint8_t dci,
                              bool cycle = true, bool event_data = false) -> Trb {
    return {
        .param = identity,
        .status = (completion_code << 24U) | (residual & EVENT_RESIDUAL_MASK),
        .control = TRB_TRANSFER_EVENT | (static_cast<uint32_t>(slot_id) << EVENT_SLOT_SHIFT) |
                   (static_cast<uint32_t>(dci) << EVENT_DCI_SHIFT) | (event_data ? EVENT_DATA_FLAG : 0) | (cycle ? TRB_CYCLE : 0),
    };
}

struct CompletionSinkContext {
    RequestTable* requests = nullptr;
    std::array<CompletionDisposition, 4> dispositions{};
    size_t count = 0;
};

void completion_sink(void* opaque, const Trb& event) {
    auto* context = static_cast<CompletionSinkContext*>(opaque);
    if (context == nullptr || context->requests == nullptr || context->count >= context->dispositions.size()) {
        return;
    }
    context->dispositions.at(context->count++) = context->requests->complete_event(event).disposition;
}

struct RecordingSinkContext {
    std::array<uint32_t, 8> types{};
    size_t count = 0;
};

void recording_sink(void* opaque, const Trb& event) {
    auto* context = static_cast<RecordingSinkContext*>(opaque);
    if (context == nullptr || context->count >= context->types.size()) {
        return;
    }
    context->types.at(context->count++) = event.control & ker::dev::usb::TRB_TYPE_MASK;
}

struct TimeoutSinkContext {
    RequestHandle last{};
    size_t count = 0;
};

void timeout_sink(void* opaque, RequestHandle request) {
    auto* context = static_cast<TimeoutSinkContext*>(opaque);
    if (context == nullptr) {
        return;
    }
    context->last = request;
    ++context->count;
}

}  // namespace

KTEST(XhciRequestCommand, ExactPointerCompletesOnlyCommandRequest) {
    RequestTable requests{};
    std::array<uint64_t, 1> const COMMAND_TRBS = {0x1000};
    std::array<uint64_t, 1> const TRANSFER_TRBS = {0x1000};
    RequestHandle const COMMAND = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = COMMAND_TRBS,
    });
    RequestHandle const TRANSFER = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 3,
        .dci = 5,
        .trb_phys = TRANSFER_TRBS,
        .requested_length = 64,
    });
    KREQUIRE_TRUE(COMMAND.valid());
    KREQUIRE_TRUE(TRANSFER.valid());

    auto const COMPLETION = requests.complete_event(command_event(0x1000, TRB_CC_SUCCESS, 9));
    KEXPECT_EQ(COMPLETION.disposition, CompletionDisposition::COMPLETED);
    KEXPECT_EQ(COMPLETION.request.index, COMMAND.index);
    KEXPECT_EQ(COMPLETION.request.generation, COMMAND.generation);

    RequestSnapshot command{};
    RequestSnapshot transfer{};
    KREQUIRE_TRUE(requests.snapshot(COMMAND, command));
    KREQUIRE_TRUE(requests.snapshot(TRANSFER, transfer));
    KEXPECT_EQ(command.state, RequestState::COMPLETED);
    KEXPECT_EQ(command.result, 0);
    KEXPECT_EQ(command.completion_code, TRB_CC_SUCCESS);
    KEXPECT_EQ(command.event_slot_id, 9U);
    KEXPECT_EQ(transfer.state, RequestState::SUBMITTED);
}

KTEST(XhciRequestTransfer, SlotDciAndAnyTdTrbAreExactIdentity) {
    RequestTable requests{};
    std::array<uint64_t, 3> const TD_TRBS = {0x2000, 0x2010, 0x2020};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 4,
        .dci = 7,
        .trb_phys = TD_TRBS,
        .requested_length = 512,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.complete_event(transfer_event(0x2010, 4, 17, 4, 6), -77).disposition, CompletionDisposition::UNKNOWN);
    KEXPECT_EQ(requests.complete_event(transfer_event(0x2010, 4, 17, 4, 7), -77).disposition, CompletionDisposition::COMPLETED);

    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.state, RequestState::COMPLETED);
    KEXPECT_EQ(snapshot.result, -77);
    KEXPECT_EQ(snapshot.completion_code, 4U);
    KEXPECT_EQ(snapshot.residual, 17U);
    KEXPECT_EQ(snapshot.event_slot_id, 4U);
    KEXPECT_EQ(snapshot.event_dci, 7U);
    KEXPECT_EQ(snapshot.event_parameter, 0x2010U);
}

KTEST(XhciRequestTransfer, ShortPacketPublishesResidualAsSuccess) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x3000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 2,
        .dci = 3,
        .trb_phys = TRBS,
        .requested_length = 128,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.complete_event(transfer_event(0x3000, TRB_CC_SHORT_PKT, 23, 2, 3)).disposition, CompletionDisposition::COMPLETED);
    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.result, 0);
    KEXPECT_EQ(snapshot.completion_code, TRB_CC_SHORT_PKT);
    KEXPECT_EQ(snapshot.residual, 23U);
}

KTEST(XhciRequestCompletion, DuplicateUnknownAndUnsupportedEventsAreRejected) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x4000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = TRBS,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    Trb const EVENT = command_event(0x4000, TRB_CC_SUCCESS, 1);
    KEXPECT_EQ(requests.complete_event(EVENT).disposition, CompletionDisposition::COMPLETED);
    KEXPECT_EQ(requests.complete_event(EVENT, -99).disposition, CompletionDisposition::DUPLICATE);
    KEXPECT_EQ(requests.complete_event(command_event(0x5000, TRB_CC_SUCCESS, 1)).disposition, CompletionDisposition::UNKNOWN);
    KEXPECT_EQ(requests.complete_event(Trb{}).disposition, CompletionDisposition::UNSUPPORTED_EVENT);

    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.result, 0);
}

KTEST(XhciRequestTimeout, TimeoutWinsAndLateCompletionCannotReleaseDmaRecord) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x6000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 6,
        .dci = 2,
        .trb_phys = TRBS,
        .requested_length = 1500,
        .deadline_us = 100,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.try_timeout(REQUEST, 99, -110), TimeoutDisposition::NOT_DUE);
    KEXPECT_EQ(requests.try_timeout(REQUEST, 100, -110), TimeoutDisposition::TIMED_OUT);
    KEXPECT_EQ(requests.complete_event(transfer_event(0x6000, TRB_CC_SUCCESS, 0, 6, 2)).disposition,
               CompletionDisposition::LATE_AFTER_TIMEOUT);

    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
    KEXPECT_EQ(snapshot.result, -110);
    KEXPECT_FALSE(requests.release(REQUEST));
    KEXPECT_TRUE(requests.mark_timed_out_retired(REQUEST));
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.state, RequestState::RETIRED);
    KEXPECT_EQ(snapshot.result, -110);
    KEXPECT_TRUE(requests.release(REQUEST));
    KEXPECT_FALSE(requests.snapshot(REQUEST, snapshot));
}

KTEST(XhciRequestTimeout, CompletionWinsAgainstLaterTimeout) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x7000};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = TRBS,
        .deadline_us = 10,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.complete_event(command_event(0x7000, TRB_CC_SUCCESS, 4)).disposition, CompletionDisposition::COMPLETED);
    KEXPECT_EQ(requests.try_timeout(REQUEST, 10, -110), TimeoutDisposition::COMPLETION_WON);
    KEXPECT_EQ(requests.cancel(REQUEST, -125), TimeoutDisposition::COMPLETION_WON);
    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.state, RequestState::COMPLETED);
    KEXPECT_EQ(snapshot.result, 0);
}

KTEST(XhciRequestCancel, ImmediateCancellationUsesTimeoutQuarantine) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x7800};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 5,
        .dci = 6,
        .trb_phys = TRBS,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.cancel(REQUEST, -125), TimeoutDisposition::TIMED_OUT);
    KEXPECT_EQ(requests.cancel(REQUEST, -126), TimeoutDisposition::ALREADY_TIMED_OUT);
    KEXPECT_EQ(requests.complete_event(transfer_event(0x7800, TRB_CC_SUCCESS, 0, 5, 6)).disposition,
               CompletionDisposition::LATE_AFTER_TIMEOUT);

    RequestSnapshot snapshot{};
    KREQUIRE_TRUE(requests.snapshot(REQUEST, snapshot));
    KEXPECT_EQ(snapshot.state, RequestState::TIMED_OUT);
    KEXPECT_EQ(snapshot.result, -125);
    KEXPECT_EQ(snapshot.deadline_us, 0U);
    KEXPECT_FALSE(requests.release(REQUEST));
    KEXPECT_TRUE(requests.mark_timed_out_retired(REQUEST));
    KEXPECT_TRUE(requests.release(REQUEST));
}

KTEST(XhciRequestCancel, DisconnectDuringTransferCannotReleaseBeforeBarrier) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0x7900};
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 5,
        .dci = 3,
        .trb_phys = TRBS,
        .requested_length = 1518,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.cancel(REQUEST, -125), TimeoutDisposition::TIMED_OUT);
    KEXPECT_FALSE(requests.release(REQUEST));
    KEXPECT_EQ(requests.complete_event(transfer_event(0x7900, TRB_CC_SUCCESS, 0, 5, 3)).disposition,
               CompletionDisposition::LATE_AFTER_TIMEOUT);
    KEXPECT_FALSE(requests.release(REQUEST));

    KREQUIRE_TRUE(requests.mark_timed_out_retired(REQUEST));
    KEXPECT_TRUE(requests.release(REQUEST));
}

KTEST(XhciRequestTimeout, FixedScanPublishesOnlyDueRequests) {
    RequestTable requests{};
    std::array<uint64_t, 1> const FIRST_TRBS = {0x8000};
    std::array<uint64_t, 1> const SECOND_TRBS = {0x9000};
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
    KREQUIRE_TRUE(FIRST.valid());
    KREQUIRE_TRUE(SECOND.valid());

    TimeoutSinkContext sink{};
    auto const SCAN = requests.expire_due(15, -110, timeout_sink, &sink);
    KEXPECT_EQ(SCAN.examined, RequestTable::capacity());
    KEXPECT_EQ(SCAN.timed_out, 1U);
    KEXPECT_EQ(sink.count, 1U);
    KEXPECT_EQ(sink.last.index, FIRST.index);
    KEXPECT_EQ(sink.last.generation, FIRST.generation);

    RequestSnapshot first{};
    RequestSnapshot second{};
    KREQUIRE_TRUE(requests.snapshot(FIRST, first));
    KREQUIRE_TRUE(requests.snapshot(SECOND, second));
    KEXPECT_EQ(first.state, RequestState::TIMED_OUT);
    KEXPECT_EQ(second.state, RequestState::SUBMITTED);
}

KTEST(XhciRequestTransfer, EventDataCookieIsMatchedExactly) {
    RequestTable requests{};
    std::array<uint64_t, 1> const TRBS = {0xA000};
    constexpr uint64_t COOKIE = 0x123456789ABCDEF0ULL;
    RequestHandle const REQUEST = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 8,
        .dci = 9,
        .trb_phys = TRBS,
        .event_data_valid = true,
        .event_data = COOKIE,
    });
    KREQUIRE_TRUE(REQUEST.valid());

    KEXPECT_EQ(requests.complete_event(transfer_event(COOKIE + 1, TRB_CC_SUCCESS, 0, 8, 9, true, true)).disposition,
               CompletionDisposition::UNKNOWN);
    KEXPECT_EQ(requests.complete_event(transfer_event(COOKIE, TRB_CC_SUCCESS, 0, 8, 9, true, true)).disposition,
               CompletionDisposition::COMPLETED);
}

KTEST(XhciRequestEventRing, FakeEventsCorrelateThroughBoundedDrain) {
    RequestTable requests{};
    std::array<uint64_t, 1> const COMMAND_TRBS = {0xB000};
    std::array<uint64_t, 1> const TRANSFER_TRBS = {0xC000};
    RequestHandle const COMMAND = requests.reserve({
        .kind = RequestKind::COMMAND,
        .trb_phys = COMMAND_TRBS,
    });
    RequestHandle const TRANSFER = requests.reserve({
        .kind = RequestKind::TRANSFER,
        .slot_id = 10,
        .dci = 11,
        .trb_phys = TRANSFER_TRBS,
    });
    KREQUIRE_TRUE(COMMAND.valid());
    KREQUIRE_TRUE(TRANSFER.valid());

    std::array<Trb, 4> ring{};
    ring.at(0) = command_event(0xB000, TRB_CC_SUCCESS, 12);
    ring.at(1) = transfer_event(0xC000, TRB_CC_SUCCESS, 0, 10, 11);
    ring.at(2).control = 0;
    EventRingCursor cursor{};
    CompletionSinkContext sink{.requests = &requests};
    auto const DRAIN = consume_event_ring(ring.data(), ring.size(), cursor, 2, completion_sink, &sink);

    KEXPECT_TRUE(DRAIN.valid);
    KEXPECT_EQ(DRAIN.consumed, 2U);
    KEXPECT_FALSE(DRAIN.budget_exhausted);
    KEXPECT_EQ(sink.count, 2U);
    KEXPECT_EQ(sink.dispositions.at(0), CompletionDisposition::COMPLETED);
    KEXPECT_EQ(sink.dispositions.at(1), CompletionDisposition::COMPLETED);
}

KTEST(XhciRequestEventRing, WrapTogglesCycleAndStopsOnOldEntry) {
    std::array<Trb, 4> ring{};
    for (auto& event : ring) {
        event.control = TRB_CYCLE;
    }
    ring.at(3) = command_event(0xD000, TRB_CC_SUCCESS, 1, true);
    ring.at(0) = transfer_event(0xE000, TRB_CC_SUCCESS, 0, 2, 3, false);
    // Entry 1 retains the old producer cycle and must stop the consumer.

    EventRingCursor cursor{.dequeue = 3, .cycle = true};
    RecordingSinkContext sink{};
    auto const DRAIN = consume_event_ring(ring.data(), ring.size(), cursor, ring.size(), recording_sink, &sink);

    KEXPECT_TRUE(DRAIN.valid);
    KEXPECT_EQ(DRAIN.consumed, 2U);
    KEXPECT_FALSE(DRAIN.more_ready);
    KEXPECT_FALSE(DRAIN.budget_exhausted);
    KEXPECT_EQ(cursor.dequeue, 1U);
    KEXPECT_FALSE(cursor.cycle);
    KEXPECT_EQ(sink.count, 2U);
    KEXPECT_EQ(sink.types.at(0), TRB_CMD_COMPLETION);
    KEXPECT_EQ(sink.types.at(1), TRB_TRANSFER_EVENT);
}

KTEST(XhciRequestEventRing, BudgetLeavesReadyEntriesForNextDrain) {
    std::array<Trb, 4> ring{};
    ring.at(0) = command_event(0xF000, TRB_CC_SUCCESS, 1);
    ring.at(1) = command_event(0xF010, TRB_CC_SUCCESS, 2);
    ring.at(2) = command_event(0xF020, TRB_CC_SUCCESS, 3);
    ring.at(3).control = 0;
    EventRingCursor cursor{};
    RecordingSinkContext sink{};

    auto const FIRST = consume_event_ring(ring.data(), ring.size(), cursor, 2, recording_sink, &sink);
    KEXPECT_EQ(FIRST.consumed, 2U);
    KEXPECT_TRUE(FIRST.more_ready);
    KEXPECT_TRUE(FIRST.budget_exhausted);
    KEXPECT_EQ(cursor.dequeue, 2U);

    auto const SECOND = consume_event_ring(ring.data(), ring.size(), cursor, 2, recording_sink, &sink);
    KEXPECT_EQ(SECOND.consumed, 1U);
    KEXPECT_FALSE(SECOND.more_ready);
    KEXPECT_FALSE(SECOND.budget_exhausted);
    KEXPECT_EQ(cursor.dequeue, 3U);
    KEXPECT_EQ(sink.count, 3U);
}
