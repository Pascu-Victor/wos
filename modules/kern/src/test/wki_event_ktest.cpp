#include <cstdint>
#include <limits>
#include <net/wki/event.hpp>
#include <test/ktest.hpp>

namespace {

using namespace ker::net::wki;

uint32_t g_reentrant_handler_calls = 0;

void reentrant_publish_handler(uint16_t /*origin_node*/, uint16_t event_class, uint16_t event_id, const void* /*data*/,
                               uint16_t /*data_len*/) {
    g_reentrant_handler_calls++;
    if (event_class == EVENT_CLASS_CUSTOM && event_id == 0x5001) {
        wki_event_publish(EVENT_CLASS_CUSTOM, 0x5002, nullptr, 0);
    }
}

}  // namespace

KTEST(WkiEventPayloadSize, PreservesLengthsThatFitFixedEventBuffers) {
    auto const empty = wki_event_payload_size(0);
    KEXPECT_EQ(empty.data_len, 0U);
    KEXPECT_EQ(empty.total_len, sizeof(EventPublishPayload));
    KEXPECT_FALSE(empty.truncated);

    auto const max_fit = wki_event_payload_size(WKI_EVENT_DATA_MAX);
    KEXPECT_EQ(max_fit.data_len, WKI_EVENT_DATA_MAX);
    KEXPECT_EQ(max_fit.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    KEXPECT_FALSE(max_fit.truncated);
}

KTEST(WkiEventPayloadSize, ClampsBeforeTotalLengthCanWrap) {
    auto const one_too_many = wki_event_payload_size(WKI_EVENT_DATA_MAX + 1U);
    KEXPECT_EQ(one_too_many.data_len, WKI_EVENT_DATA_MAX);
    KEXPECT_EQ(one_too_many.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    KEXPECT_TRUE(one_too_many.truncated);

    auto const huge = wki_event_payload_size(std::numeric_limits<uint16_t>::max());
    KEXPECT_EQ(huge.data_len, WKI_EVENT_DATA_MAX);
    KEXPECT_EQ(huge.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    KEXPECT_TRUE(huge.truncated);
}

KTEST(WkiEventDispatch, LocalHandlerCanReenterPublish) {
    wki_event_init();
    g_reentrant_handler_calls = 0;

    wki_event_register_handler(EVENT_CLASS_CUSTOM, EVENT_WILDCARD, reentrant_publish_handler);
    wki_event_publish(EVENT_CLASS_CUSTOM, 0x5001, nullptr, 0);
    wki_event_unregister_handler(reentrant_publish_handler);

    KEXPECT_EQ(g_reentrant_handler_calls, 2U);
}

KTEST(WkiEventAck, SingleAckRemovesOnlyOneMatchingPendingReliableEvent) {
    KEXPECT_TRUE(wki_event_selftest_ack_removes_single_matching_pending());
}

KTEST(WkiEventAckPerf, RequiresCorrelationAndEnabledRecording) {
    KEXPECT_FALSE(wki_event_ack_should_record_perf(0, false));
    KEXPECT_FALSE(wki_event_ack_should_record_perf(0, true));
    KEXPECT_FALSE(wki_event_ack_should_record_perf(42, false));
    KEXPECT_TRUE(wki_event_ack_should_record_perf(42, true));
}
