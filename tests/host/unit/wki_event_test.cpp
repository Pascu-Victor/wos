#include <gtest/gtest.h>

#include <limits>
#include <net/wki/event.hpp>

using namespace ker::net::wki;

TEST(WkiEventPayloadSize, PreservesLengthsThatFitFixedEventBuffers) {
    auto const empty = wki_event_payload_size(0);
    EXPECT_EQ(empty.data_len, 0u);
    EXPECT_EQ(empty.total_len, sizeof(EventPublishPayload));
    EXPECT_FALSE(empty.truncated);

    auto const max_fit = wki_event_payload_size(WKI_EVENT_DATA_MAX);
    EXPECT_EQ(max_fit.data_len, WKI_EVENT_DATA_MAX);
    EXPECT_EQ(max_fit.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    EXPECT_FALSE(max_fit.truncated);
}

TEST(WkiEventPayloadSize, ClampsBeforeTotalLengthCanWrap) {
    auto const one_too_many = wki_event_payload_size(WKI_EVENT_DATA_MAX + 1U);
    EXPECT_EQ(one_too_many.data_len, WKI_EVENT_DATA_MAX);
    EXPECT_EQ(one_too_many.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    EXPECT_TRUE(one_too_many.truncated);

    auto const huge = wki_event_payload_size(std::numeric_limits<uint16_t>::max());
    EXPECT_EQ(huge.data_len, WKI_EVENT_DATA_MAX);
    EXPECT_EQ(huge.total_len, sizeof(EventPublishPayload) + WKI_EVENT_DATA_MAX);
    EXPECT_TRUE(huge.truncated);
}

TEST(WkiEventFilterMatch, SupportsClassAndIdWildcards) {
    EXPECT_TRUE(wki_event_filter_matches(EVENT_CLASS_CUSTOM, 0x42, EVENT_CLASS_CUSTOM, 0x42));
    EXPECT_TRUE(wki_event_filter_matches(EVENT_WILDCARD, 0x42, EVENT_CLASS_SYSTEM, 0x42));
    EXPECT_TRUE(wki_event_filter_matches(EVENT_CLASS_CUSTOM, EVENT_WILDCARD, EVENT_CLASS_CUSTOM, 0x99));
    EXPECT_TRUE(wki_event_filter_matches(EVENT_WILDCARD, EVENT_WILDCARD, EVENT_CLASS_STORAGE, 0x77));

    EXPECT_FALSE(wki_event_filter_matches(EVENT_CLASS_CUSTOM, 0x42, EVENT_CLASS_SYSTEM, 0x42));
    EXPECT_FALSE(wki_event_filter_matches(EVENT_CLASS_CUSTOM, 0x42, EVENT_CLASS_CUSTOM, 0x43));
}

TEST(WkiEventAckMatch, RequiresSubscriberAndEventIdentity) {
    EventAckPayload ack = {};
    ack.event_class = EVENT_CLASS_CUSTOM;
    ack.event_id = 0x42;
    ack.origin_node = 0x1001;

    EXPECT_TRUE(wki_event_ack_matches(0x2002, 0x2002, EVENT_CLASS_CUSTOM, 0x42, 0x1001, ack));
    EXPECT_FALSE(wki_event_ack_matches(0x2003, 0x2002, EVENT_CLASS_CUSTOM, 0x42, 0x1001, ack));
    EXPECT_FALSE(wki_event_ack_matches(0x2002, 0x2002, EVENT_CLASS_SYSTEM, 0x42, 0x1001, ack));
    EXPECT_FALSE(wki_event_ack_matches(0x2002, 0x2002, EVENT_CLASS_CUSTOM, 0x43, 0x1001, ack));
    EXPECT_FALSE(wki_event_ack_matches(0x2002, 0x2002, EVENT_CLASS_CUSTOM, 0x42, 0x1002, ack));
}

TEST(WkiEventAckPerf, RequiresCorrelationAndEnabledRecording) {
    EXPECT_FALSE(wki_event_ack_should_record_perf(0, false));
    EXPECT_FALSE(wki_event_ack_should_record_perf(0, true));
    EXPECT_FALSE(wki_event_ack_should_record_perf(42, false));
    EXPECT_TRUE(wki_event_ack_should_record_perf(42, true));
}
