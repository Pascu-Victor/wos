#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <wos/telemetry.hpp>

#include "capture.hpp"

namespace perf {
namespace {
auto status_payload(const std::optional<CaptureStatus>& status) -> wos::telemetry::Value {
    using wos::telemetry::Value;
    if (!status.has_value()) {
        return nullptr;
    }
    const auto& s = *status;
    return Value::Object{
        {"session", Value::unsigned_integer(s.session)},
        {"control_version", Value::unsigned_integer(s.control_version)},
        {"cpus", Value::unsigned_integer(s.cpus)},
        {"mask", Value::unsigned_integer(s.mask)},
        {"enabled", s.enabled},
        {"written", Value::unsigned_integer(s.written)},
        {"consumed", Value::unsigned_integer(s.consumed)},
        {"overwritten", Value::unsigned_integer(s.overwritten)},
        {"pending", Value::unsigned_integer(s.pending)},
    };
}
}  // namespace

auto parse_capture_status(std::string_view text) -> std::optional<CaptureStatus> {
    constexpr std::array<std::string_view, 10> KEYS{
        "version", "session", "control_version", "enabled", "mask", "cpus", "written", "consumed", "overwritten", "pending",
    };
    std::array<uint64_t, 10> values{};
    uint16_t seen = 0;
    if (!text.starts_with("perf_capture ") || !text.ends_with('\n')) {
        return std::nullopt;
    }
    text.remove_prefix(std::string_view("perf_capture ").size());
    text.remove_suffix(1);
    while (!text.empty()) {
        auto space = text.find(' ');
        auto token = text.substr(0, space);
        auto equals = token.find('=');
        if (equals == std::string_view::npos) {
            return std::nullopt;
        }
        auto key = token.substr(0, equals);
        auto value = token.substr(equals + 1);
        size_t index = 0;
        while (index < values.size() && KEYS.at(index) != key) {
            ++index;
        }
        if (index == values.size() || value.empty() || value.front() < '0' || value.front() > '9' || (seen & (1U << index)) != 0) {
            return std::nullopt;
        }
        auto parsed = std::from_chars(value.data(), value.data() + value.size(), values.at(index));
        if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
            return std::nullopt;
        }
        seen |= static_cast<uint16_t>(1U << index);
        if (space == std::string_view::npos) {
            break;
        }
        text.remove_prefix(space + 1);
        if (text.empty()) {
            return std::nullopt;
        }
    }
    if (seen != 1023 || values.at(0) != 1 || values.at(3) > 1 || values.at(4) > UINT16_MAX || values.at(5) == 0) {
        return std::nullopt;
    }
    uint64_t const WRITTEN = values.at(6);
    uint64_t const CONSUMED = values.at(7);
    uint64_t const OVERWRITTEN = values.at(8);
    uint64_t const PENDING = values.at(9);
    // Checked subtraction also rejects wrapped or inconsistent kernel counters.
    if (CONSUMED > WRITTEN || OVERWRITTEN > WRITTEN - CONSUMED || PENDING != WRITTEN - CONSUMED - OVERWRITTEN) {
        return std::nullopt;
    }
    return CaptureStatus{
        .session = values.at(1),
        .control_version = values.at(2),
        .cpus = values.at(5),
        .written = WRITTEN,
        .consumed = CONSUMED,
        .overwritten = OVERWRITTEN,
        .pending = PENDING,
        .mask = static_cast<uint16_t>(values.at(4)),
        .enabled = values.at(3) != 0,
    };
}

auto capture_stream_complete(const CaptureReceipt& r) -> bool {
    if (!r.started.has_value() || !r.finished.has_value() || r.read_failed || r.write_failed || r.control_failed || r.limit_reached) {
        return false;
    }
    auto const& a = *r.started;
    auto const& b = *r.finished;
    return a.enabled && !b.enabled && a.session == b.session && a.mask == b.mask && a.cpus == b.cpus && a.control_version != UINT64_MAX &&
           b.control_version == a.control_version + 1 && b.overwritten == 0 && b.pending == 0 && b.written == b.consumed &&
           r.saved_events == b.consumed;
}

auto serialize_capture_receipt(const CaptureReceipt& r, std::string_view node_id, std::string& error) -> std::optional<std::string> {
    using wos::telemetry::Value;
    Value::Object identity;
    if (!node_id.empty()) {
        identity.emplace("node_id", node_id);
    }
    Value::Object payload{
        {"requested_filter", r.requested_filter},
        {"coverage", "enabled_filter_only"},
        {"semantic_span_completeness", "not_assessed"},
        {"event_stream_complete", capture_stream_complete(r)},
        {"started", status_payload(r.started)},
        {"finished", status_payload(r.finished)},
        {"saved_events", Value::unsigned_integer(r.saved_events)},
        {"saved_bytes", Value::unsigned_integer(r.saved_bytes)},
        {"read_failed", r.read_failed},
        {"write_failed", r.write_failed},
        {"control_failed", r.control_failed},
        {"limit_reached", r.limit_reached},
    };
    if (r.run_observer) {
        const auto& trace = *r.run_observer;
        Value::Array spans;
        for (const auto& span : trace.spans) {
            spans.emplace_back(Value::Array{
                Value(run_observer_phase_name(span.phase)),
                Value::unsigned_integer(span.begin_ns),
                Value::unsigned_integer(span.end_ns),
                span.result ? Value(std::to_string(*span.result)) : Value(nullptr),
            });
        }
        payload.emplace("run_observer", Value::Object{
                                            {"version", Value::unsigned_integer(1)},
                                            {"parent_pid", Value::unsigned_integer(trace.parent_pid)},
                                            {"child_pid", Value::unsigned_integer(trace.child_pid)},
                                            {"capture_session", r.started ? Value::unsigned_integer(r.started->session) : Value(nullptr)},
                                            {"clock_domain", "boot_monotonic"},
                                            {"unit", "ns"},
                                            {"quality", "syscall_sample"},
                                            {"complete", capture_stream_complete(r) && run_observer_complete(trace)},
                                            {"clock_failed", trace.clock_failed},
                                            {"invalid_state", trace.invalid_state},
                                            {"finished", trace.finished},
                                            {"dropped_spans", Value::unsigned_integer(trace.dropped_spans)},
                                            {"spans", std::move(spans)},
                                        });
    }
    auto envelope = wos::telemetry::make_envelope(
        "perf", 1, "perf.capture_receipt", std::move(identity),
        {{"domain", "boot_monotonic"}, {"value", Value::unsigned_integer(r.stopped_ns)}, {"unit", "ns"}, {"quality", "local"}}, {},
        std::move(payload));
    auto serialized = wos::telemetry::serialize(envelope);
    if (!serialized) {
        error = serialized.error.message;
        return std::nullopt;
    }
    return std::move(serialized.text);
}

auto capture_receipt_from_snapshot(std::string_view snapshot, std::string& error) -> std::optional<std::string_view> {
    auto start = snapshot.find(SECTION_CAPTURE_STATUS);
    if (start == std::string_view::npos) {
        return std::nullopt;
    }
    start += SECTION_CAPTURE_STATUS.size();
    auto end = snapshot.find(SECTION_CAPTURE_STATUS_END, start);
    if (end == std::string_view::npos) {
        error = "unterminated CAPTURE_STATUS section";
        return std::nullopt;
    }
    auto body = snapshot.substr(start, end - start);
    if (!body.ends_with('\n')) {
        error = "partial capture receipt";
        return std::nullopt;
    }
    body.remove_suffix(1);
    wos::telemetry::Limits limits{
        .max_input_bytes = 16384,
        .max_depth = 16,
        .max_nodes = 256,
        .max_object_members = 128,
        .max_array_elements = 128,
        .max_string_bytes = 4096,
    };
    auto parsed = wos::telemetry::parse(body, limits);
    wos::telemetry::Error validation_error;
    if (!parsed.value.has_value() || !wos::telemetry::validate_envelope(*parsed.value, &validation_error) ||
        wos::telemetry::object_string(*parsed.value, "source") != std::optional<std::string_view>{"perf"} ||
        wos::telemetry::object_string(*parsed.value, "kind") != std::optional<std::string_view>{"perf.capture_receipt"}) {
        error = "invalid perf capture receipt";
        return std::nullopt;
    }
    return body;
}
}  // namespace perf
