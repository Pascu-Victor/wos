#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace perf {

// Fits the existing receipt's 256-node parser even with a result on every
// span: <=32 tuples of five JSON values plus the receipt and observer fields.
inline constexpr std::size_t RUN_OBSERVER_SPAN_LIMIT = 32;

enum class RunObserverPhase : uint8_t {
    PARENT_SETUP,
    DATA_OPEN,
    TIMEBASE_WRITE,
    INITIAL_SCAN,
    WAIT_PROBE,
    LOOP_CONTROL,
    GROUP_SCAN,
    SLEEP,
    RING_DRAIN,
    FINAL_REAP,
    CAPTURE_STOP,
    FINAL_STATS,
    FINAL_REPORT,
    FINAL_DRAIN,
    PROC_STATS,
    PROC_IMAGES,
    PROC_CMDLINE,
};

struct RunObserverSpan {
    RunObserverPhase phase{};
    uint64_t begin_ns{}, end_ns{};
    std::optional<int64_t> result;
};

struct RunObserverTrace {
    uint64_t parent_pid{}, child_pid{}, dropped_spans{};
    std::vector<RunObserverSpan> spans;
    std::optional<RunObserverPhase> active_phase;
    std::optional<uint64_t> begin_ns;
    bool clock_failed{}, invalid_state{}, finished{};
};

[[nodiscard]] auto run_observer_phase_name(RunObserverPhase phase) -> std::string_view;
// A single sample closes the previous phase and starts the next. A missing
// next phase closes the trace. Result belongs to the phase being closed.
// Reserve RUN_OBSERVER_SPAN_LIMIT before entering the measured window.
void advance_run_observer(RunObserverTrace& trace, std::optional<RunObserverPhase> next, std::optional<int64_t> sample,
                          std::optional<int64_t> result = std::nullopt);
[[nodiscard]] auto run_observer_complete(const RunObserverTrace& trace) -> bool;

}  // namespace perf
