#pragma once

#include <cstdint>

namespace ker::mod::rtc {

// Read the CMOS real-time clock and store the epoch offset.
// Must be called after tsc::init() so that the monotonic base is stable.
void init();

// Returns Unix timestamp seconds (wall clock).
// Derived from the CMOS reading taken at boot plus the elapsed monotonic time.
uint64_t getEpochSec();

// Returns Unix timestamp in nanoseconds.
uint64_t getEpochNs();

// Apply an NTP correction.  delta_sec is the signed difference
// (ntp_unix_sec - rtc_unix_sec) measured at sync time.
void setOffset(int64_t delta_sec);

}  // namespace ker::mod::rtc
