#include "observer.hpp"

#include <array>

namespace perf {

auto run_observer_phase_name(RunObserverPhase phase) -> std::string_view {
    constexpr std::array<std::string_view, 17> NAMES{
        "parent_setup", "data_open",   "timebase_write", "initial_scan", "wait_probe",   "loop_control",
        "group_scan",   "sleep",       "ring_drain",     "final_reap",   "capture_stop", "final_stats",
        "final_report", "final_drain", "proc_stats",     "proc_images",  "proc_cmdline",
    };
    auto const INDEX = static_cast<std::size_t>(phase);
    return INDEX < NAMES.size() ? NAMES.at(INDEX) : std::string_view{};
}

void advance_run_observer(RunObserverTrace& trace, std::optional<RunObserverPhase> next, std::optional<int64_t> sample,
                          std::optional<int64_t> result) {
    if (trace.finished || (next && run_observer_phase_name(*next).empty())) {
        trace.invalid_state = true;
        return;
    }
    if (trace.spans.size() >= RUN_OBSERVER_SPAN_LIMIT) {
        // The caller intentionally stops sampling the clock at the bound.
        // Count omitted intervals, rather than inventing their timestamps.
        if (trace.active_phase && trace.dropped_spans != UINT64_MAX) ++trace.dropped_spans;
        trace.active_phase = next;
        trace.begin_ns.reset();
        trace.finished = !next;
        return;
    }
    std::optional<uint64_t> now;
    if (sample && *sample >= 0 && (!trace.begin_ns || static_cast<uint64_t>(*sample) >= *trace.begin_ns)) {
        now = static_cast<uint64_t>(*sample);
    } else {
        trace.clock_failed = true;
    }
    if (trace.active_phase && trace.begin_ns && now) {
        trace.spans.push_back({.phase = *trace.active_phase, .begin_ns = *trace.begin_ns, .end_ns = *now, .result = result});
    }
    trace.active_phase = next;
    trace.begin_ns = now;
    trace.finished = !next;
}

auto run_observer_complete(const RunObserverTrace& trace) -> bool {
    if (trace.parent_pid == 0 || trace.child_pid == 0 || trace.parent_pid == trace.child_pid || trace.clock_failed || trace.invalid_state ||
        !trace.finished || trace.active_phase || trace.dropped_spans != 0 || trace.spans.empty() ||
        trace.spans.size() > RUN_OBSERVER_SPAN_LIMIT || trace.spans.front().phase != RunObserverPhase::PARENT_SETUP ||
        trace.spans.back().phase != RunObserverPhase::FINAL_DRAIN) {
        return false;
    }
    std::optional<uint64_t> previous_end;
    for (const auto& span : trace.spans) {
        if (run_observer_phase_name(span.phase).empty() || span.begin_ns > span.end_ns || (previous_end && span.begin_ns != *previous_end))
            return false;
        previous_end = span.end_ns;
    }
    return true;
}

}  // namespace perf
