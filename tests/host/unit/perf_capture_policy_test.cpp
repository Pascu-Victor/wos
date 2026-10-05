#include <gtest/gtest.h>

#include <array>

#include <wos/telemetry.hpp>

#include "capture.hpp"
#include "perf_data.hpp"

namespace {
constexpr std::string_view STATUS =
    "perf_capture version=1 session=3 control_version=4 enabled=1 mask=256 cpus=8 written=10 consumed=0 overwritten=0 pending=10\n";
auto complete_receipt() -> perf::CaptureReceipt {
    perf::CaptureReceipt r;
    r.started = perf::parse_capture_status(STATUS);
    r.finished = r.started;
    r.finished->enabled = false;
    ++r.finished->control_version;
    r.finished->consumed = 10;
    r.finished->pending = 0;
    r.saved_events = 10;
    r.saved_bytes = 1000;
    r.stopped_ns = 100;
    r.requested_filter = "local_proc";
    return r;
}
TEST(PerfCapture, StrictStatusParser) {
    auto s = perf::parse_capture_status(STATUS);
    ASSERT_TRUE(s);
    EXPECT_EQ(s->mask, 256);
    EXPECT_EQ(s->pending, 10);
    for (auto text :
         {std::string(STATUS.substr(0, STATUS.size() - 1)), std::string(STATUS) + "x\n", std::string(STATUS) + std::string(STATUS)})
        EXPECT_FALSE(perf::parse_capture_status(text));
    for (auto replacement : {"enabled=2", "mask=65536", "cpus=0", "written=9", "consumed=11", "overwritten=1", "pending=9", "version=2",
                             "session=-1", "session=18446744073709551616", "session=3x"}) {
        std::string text(STATUS);
        std::string_view token(replacement);
        auto key = token.substr(0, token.find('=') + 1);
        auto pos = text.find(key);
        ASSERT_NE(pos, std::string::npos);
        auto end = text.find(' ', pos);
        if (end == std::string::npos) end = text.find('\n', pos);
        text.replace(pos, end - pos, replacement);
        EXPECT_FALSE(perf::parse_capture_status(text)) << replacement;
    }
    auto duplicate = std::string(STATUS);
    duplicate.insert(duplicate.size() - 1, " session=3");
    EXPECT_FALSE(perf::parse_capture_status(duplicate));
}
TEST(PerfCapture, EveryLossAndInterferencePrecludesCompleteness) {
    auto r = complete_receipt();
    ASSERT_TRUE(perf::capture_stream_complete(r));
    for (unsigned mode = 0; mode < 15; ++mode) {
        auto broken = r;
        switch (mode) {
            case 0:
                broken.started.reset();
                break;
            case 1:
                broken.finished.reset();
                break;
            case 2:
                broken.started->enabled = false;
                break;
            case 3:
                broken.finished->enabled = true;
                break;
            case 4:
                ++broken.finished->session;
                break;
            case 5:
                ++broken.finished->mask;
                break;
            case 6:
                ++broken.finished->cpus;
                break;
            case 7:
                ++broken.finished->control_version;
                break;
            case 8:
                broken.finished->overwritten = 1;
                break;
            case 9:
                broken.finished->pending = 1;
                break;
            case 10:
                --broken.saved_events;
                break;
            case 11:
                broken.read_failed = true;
                break;
            case 12:
                broken.write_failed = true;
                break;
            case 13:
                broken.control_failed = true;
                break;
            case 14:
                broken.limit_reached = true;
                break;
        }
        EXPECT_FALSE(perf::capture_stream_complete(broken)) << mode;
    }
    r.started->control_version = UINT64_MAX;
    r.finished->control_version = 0;
    EXPECT_FALSE(perf::capture_stream_complete(r));
}
TEST(PerfCapture, ReceiptEnvelopeAndLegacyContainerRoundtrip) {
    auto r = complete_receipt();
    std::string error;
    auto receipt = perf::serialize_capture_receipt(r, "vm0", error);
    ASSERT_TRUE(receipt) << error;
    auto parsed = wos::telemetry::parse(*receipt);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(wos::telemetry::validate_envelope(*parsed.value));
    EXPECT_NE(receipt->find("\"event_stream_complete\":true"), std::string::npos);
    EXPECT_NE(receipt->find("not_assessed"), std::string::npos);
    std::string legacy = "--- SECTION EVENTS ---\n--- END EVENTS ---\n";
    legacy += perf::SECTION_CAPTURE_STATUS;
    legacy += *receipt;
    legacy += '\n';
    legacy += perf::SECTION_CAPTURE_STATUS_END;
    auto body = perf::capture_receipt_from_snapshot(legacy, error);
    ASSERT_TRUE(body) << error;
    EXPECT_EQ(*body, *receipt);
    auto file = perf::perf_data::parse(legacy);
    ASSERT_TRUE(file.file) << file.error;
    std::string jsonl = *receipt + "\n";
    auto container = perf::perf_data::encode_v1(file.file->legacy_snapshot, jsonl, 1);
    ASSERT_TRUE(container.bytes) << container.error;
    auto restored = perf::perf_data::parse(*container.bytes);
    ASSERT_TRUE(restored.file) << restored.error;
    EXPECT_EQ(restored.file->typed_events_jsonl, jsonl);
    error.clear();
    EXPECT_FALSE(perf::capture_receipt_from_snapshot("--- SECTION CAPTURE_STATUS ---\n{}\n--- END CAPTURE_STATUS ---\n", error));
    EXPECT_FALSE(error.empty());
}

TEST(PerfCapture, ObserverBoundaryOwnershipAndOldParserBounds) {
    auto r = complete_receipt();
    r.started->session = r.finished->session = 0;  // zero is a valid session
    auto& t = r.run_observer.emplace();
    t.parent_pid = UINT64_MAX;
    t.child_pid = 0x100000009ULL;
    t.spans.reserve(perf::RUN_OBSERVER_SPAN_LIMIT);
    using P = perf::RunObserverPhase;
    constexpr std::array SCAN_PHASES{P::PROC_STATS, P::PROC_IMAGES, P::PROC_CMDLINE};
    perf::advance_run_observer(t, P::PARENT_SETUP, 0);
    for (unsigned i = 1; i < perf::RUN_OBSERVER_SPAN_LIMIT; ++i)
        perf::advance_run_observer(t, i + 1 == perf::RUN_OBSERVER_SPAN_LIMIT ? P::FINAL_DRAIN : SCAN_PHASES.at(i % SCAN_PHASES.size()), i,
                                   INT64_MAX);
    perf::advance_run_observer(t, std::nullopt, perf::RUN_OBSERVER_SPAN_LIMIT, INT64_MAX);
    ASSERT_TRUE(perf::run_observer_complete(t));
    ASSERT_EQ(t.spans.size(), perf::RUN_OBSERVER_SPAN_LIMIT);
    for (unsigned i = 0; i < t.spans.size(); ++i) {
        EXPECT_EQ(t.spans.at(i).begin_ns, i);
        EXPECT_EQ(t.spans.at(i).end_ns, i + 1);
        EXPECT_EQ(t.spans.at(i).result, INT64_MAX);
    }
    std::string error;
    auto receipt = perf::serialize_capture_receipt(r, "vm0", error);
    ASSERT_TRUE(receipt) << error;
    EXPECT_NE(receipt->find("18446744073709551615"), std::string::npos);
    EXPECT_NE(receipt->find("4294967305"), std::string::npos);
    EXPECT_NE(receipt->find("\"complete\":true"), std::string::npos);
    std::string legacy = "--- SECTION EVENTS ---\n--- END EVENTS ---\n";
    legacy += perf::SECTION_CAPTURE_STATUS;
    legacy += *receipt + "\n";
    legacy += perf::SECTION_CAPTURE_STATUS_END;
    // Uses the unchanged 256-node, 128-array, 4K-string, 16K-receipt limits.
    ASSERT_TRUE(perf::capture_receipt_from_snapshot(legacy, error)) << error;
    auto encoded = perf::perf_data::encode_v1(legacy, *receipt + "\n", 1);
    ASSERT_TRUE(encoded.bytes) << encoded.error;
    auto restored = perf::perf_data::parse(*encoded.bytes);
    ASSERT_TRUE(restored.file) << restored.error;
    EXPECT_EQ(restored.file->typed_event_records, 1);
    EXPECT_EQ(restored.file->typed_events_jsonl, *receipt + "\n");
    ++r.finished->session;
    auto reset = perf::serialize_capture_receipt(r, "vm0", error);
    ASSERT_TRUE(reset);
    EXPECT_NE(reset->find("\"complete\":false"), std::string::npos);
    EXPECT_NE(reset->find("\"event_stream_complete\":false"), std::string::npos);
}

TEST(PerfCapture, ObserverClockFailureClippingAndMalformedState) {
    using P = perf::RunObserverPhase;
    for (unsigned mode = 0; mode < 3; ++mode) {
        perf::RunObserverTrace t;
        t.parent_pid = 7;
        t.child_pid = 9;
        t.spans.reserve(perf::RUN_OBSERVER_SPAN_LIMIT);
        perf::advance_run_observer(t, P::PARENT_SETUP, 9);
        auto sample = mode == 0 ? std::optional<int64_t>{} : std::optional<int64_t>{mode == 1 ? -1 : 8};
        perf::advance_run_observer(t, P::FINAL_DRAIN, sample);
        perf::advance_run_observer(t, std::nullopt, 10);
        EXPECT_TRUE(t.clock_failed);
        EXPECT_FALSE(perf::run_observer_complete(t));
        EXPECT_TRUE(t.spans.empty());
    }
    auto r = complete_receipt();
    auto& t = r.run_observer.emplace();
    t.parent_pid = 7;
    t.child_pid = 9;
    t.spans.reserve(perf::RUN_OBSERVER_SPAN_LIMIT);
    auto const CAPACITY = t.spans.capacity();
    perf::advance_run_observer(t, P::PARENT_SETUP, 0);
    for (unsigned i = 1; i <= 100; ++i) perf::advance_run_observer(t, P::SLEEP, i);
    perf::advance_run_observer(t, std::nullopt, std::nullopt);
    EXPECT_EQ(t.spans.size(), perf::RUN_OBSERVER_SPAN_LIMIT);
    EXPECT_EQ(t.spans.capacity(), CAPACITY);
    EXPECT_EQ(t.dropped_spans, 101 - perf::RUN_OBSERVER_SPAN_LIMIT);
    EXPECT_FALSE(t.clock_failed);  // intentionally unsampled after capacity
    EXPECT_TRUE(t.finished);
    EXPECT_FALSE(perf::run_observer_complete(t));
    EXPECT_TRUE(perf::capture_stream_complete(r));  // independent completeness
    std::string error;
    auto receipt = perf::serialize_capture_receipt(r, "vm0", error);
    ASSERT_TRUE(receipt);
    EXPECT_NE(receipt->find("\"event_stream_complete\":true"), std::string::npos);
    EXPECT_NE(receipt->find("\"complete\":false"), std::string::npos);
    perf::advance_run_observer(t, P::PARENT_SETUP, 200);
    EXPECT_TRUE(t.invalid_state);
    perf::RunObserverTrace invalid;
    perf::advance_run_observer(invalid, static_cast<P>(255), 0);
    EXPECT_TRUE(invalid.invalid_state);
}
}  // namespace
