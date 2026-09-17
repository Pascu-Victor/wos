#pragma once

#include <cstdint>
#include <platform/ktime/clocksource_policy.hpp>

namespace ker::mod::tsc {

void init();
[[nodiscard]] uint64_t get_hz();
[[nodiscard]] uint64_t get_ns();
[[nodiscard]] uint64_t ticks_to_ns(uint64_t delta);
[[nodiscard]] uint64_t get_resolution_ns();

struct ClocksourceStatus {
    time::clocksource::State state;
    bool invariant_tsc;
    uint64_t frequency_hz;
    uint64_t resolution_ns;
    uint64_t conversion_multiplier;
    uint64_t conversion_divisor;
    uint64_t calibration_spread_ppm;
    uint64_t watchdog_deviation_ns;
};

[[nodiscard]] ClocksourceStatus get_status();
[[nodiscard]] const char* state_name(time::clocksource::State state);

}  // namespace ker::mod::tsc
