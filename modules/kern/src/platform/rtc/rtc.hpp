#pragma once

#include <cstdint>
#include <platform/rtc/wall_clock.hpp>

namespace ker::mod::rtc {

// Read the CMOS real-time clock and store the epoch offset.
// Must be called after tsc::init() so that the monotonic base is stable.
void init();

// Returns Unix timestamp seconds (wall clock).
// Derived from the CMOS reading taken at boot plus the elapsed monotonic time.
[[nodiscard]] uint64_t get_epoch_sec();

// Returns Unix timestamp in nanoseconds.
[[nodiscard]] uint64_t get_epoch_ns();

// Apply a signed NTP correction to the current wall clock.
auto adjust_offset_ns(int64_t delta_ns) -> bool;
[[nodiscard]] auto status() -> WallClock;

}  // namespace ker::mod::rtc
