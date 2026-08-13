#include "perf_event_json.hpp"

#include <string>
#include <utility>
#include <wos/telemetry.hpp>

namespace perf {
namespace {

auto perf_event_kind(char type) -> std::string_view {
    switch (type) {
        case 'S':
            return "perf.sample";
        case 'X':
            return "perf.switch";
        case 'W':
            return "perf.wake";
        case 'B':
            return "perf.sleep";
        case 'C':
            return "perf.container_stat";
        case 'K':
            return "perf.wki";
        default:
            return {};
    }
}

void add_callsite(wos::telemetry::Value::Object& payload, const TypedEventRecord& event) {
    if (!event.callsite.empty()) {
        payload.emplace("callsite", event.callsite);
    }
}

}  // namespace

auto serialize_typed_perf_event(const TypedEventRecord& event, std::optional<int64_t> realtime_offset_ns, std::string_view node_id,
                                std::string& error) -> std::optional<std::string> {
    using wos::telemetry::Value;

    std::string_view const KIND = perf_event_kind(event.type);
    if (KIND.empty()) {
        error = "unsupported perf event type";
        return std::nullopt;
    }

    Value::Object identity{
        {"pid", Value::unsigned_integer(event.pid)},
        {"cpu", Value::number(wos::telemetry::decimal_u64_string(event.cpu))},
    };
    if (!node_id.empty()) {
        identity.emplace("node_id", node_id);
    }

    Value::Object clock{
        {"domain", "boot_monotonic"},
        {"value", Value::unsigned_integer(event.ts_ns)},
        {"unit", "ns"},
        {"quality", "local"},
    };
    if (realtime_offset_ns.has_value()) {
        clock.emplace("realtime_offset_ns", Value::signed_integer(*realtime_offset_ns));
    }

    Value::Object correlation;
    Value::Object payload{
        {"legacy_type", std::string(1, event.type)},
        {"flags", Value::unsigned_integer(event.flags)},
        {"thread_identity_quality", "unavailable_in_legacy_kperf"},
    };

    switch (event.type) {
        case 'S':
            payload.emplace("instruction_pointer", Value::unsigned_integer(event.data));
            payload.emplace("lag_v", Value::signed_integer(event.lag));
            break;
        case 'X':
            payload.emplace("previous_pid", Value::unsigned_integer(event.pid));
            payload.emplace("next_pid", Value::unsigned_integer(event.other_pid));
            payload.emplace("lag_v", Value::signed_integer(event.lag));
            payload.emplace("run_us", Value::unsigned_integer(event.aux));
            add_callsite(payload, event);
            break;
        case 'W':
        case 'B':
            payload.emplace("wake_at_us", Value::unsigned_integer(event.data));
            payload.emplace("duration_us", Value::unsigned_integer(event.aux));
            payload.emplace("wait_channel", event.wait_channel);
            add_callsite(payload, event);
            break;
        case 'C':
            payload.emplace("subsystem", event.subsystem);
            payload.emplace("element_count", Value::signed_integer(event.lag));
            payload.emplace("capacity", Value::unsigned_integer(event.aux));
            payload.emplace("instance_id", nullptr);
            payload.emplace("instance_id_quality", "unavailable_in_legacy_kperf");
            add_callsite(payload, event);
            break;
        case 'K':
            correlation.emplace("wki_id", Value::unsigned_integer(event.correlation));
            correlation.emplace("peer", Value::unsigned_integer(event.peer));
            correlation.emplace("channel", Value::unsigned_integer(event.channel));
            payload.emplace("scope", event.scope);
            payload.emplace("operation", event.operation);
            payload.emplace("phase", event.phase);
            payload.emplace("peer", Value::unsigned_integer(event.peer));
            payload.emplace("channel", Value::unsigned_integer(event.channel));
            payload.emplace("correlation", Value::unsigned_integer(event.correlation));
            payload.emplace("status", Value::signed_integer(event.status));
            payload.emplace("aux", Value::unsigned_integer(event.aux));
            add_callsite(payload, event);
            break;
        default:
            break;
    }

    auto envelope = wos::telemetry::make_envelope("perf", 1, std::string(KIND), std::move(identity), std::move(clock),
                                                  std::move(correlation), std::move(payload));
    wos::telemetry::Limits limits{
        .max_input_bytes = 256 * 1024,
        .max_depth = 16,
        .max_nodes = 256,
        .max_object_members = 128,
        .max_array_elements = 128,
        .max_string_bytes = 64 * 1024,
    };
    auto serialized = wos::telemetry::serialize(envelope, limits);
    if (!serialized) {
        error = serialized.error.message;
        return std::nullopt;
    }
    return std::move(*serialized.text);
}

}  // namespace perf
