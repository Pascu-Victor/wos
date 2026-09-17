#pragma once

#include <array>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>

namespace ker::mod::rtc {

constexpr int64_t MAX_CORRECTION_NS = 86400LL * 1000000000;
constexpr uint64_t SLEW_DIVISOR = 2000;  // 500 ppm, in either direction.

constexpr auto leap_year(int year) -> bool { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

constexpr auto calendar_epoch(int year, int month, int day, int hour, int minute, int second, uint64_t& result) -> bool {
    constexpr std::array<int, 12> DAYS{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year < 1970 || year > 2199 || month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59)
        return false;
    if (day > DAYS[static_cast<unsigned>(month - 1)] + (month == 2 && leap_year(year) ? 1 : 0)) return false;
    uint64_t days = 0;
    for (int y = 1970; y < year; ++y) days += leap_year(y) ? 366 : 365;
    for (int m = 1; m < month; ++m) days += DAYS[static_cast<unsigned>(m - 1)] + (m == 2 && leap_year(year) ? 1 : 0);
    days += static_cast<uint64_t>(day - 1);
    result = days * 86400 + static_cast<uint64_t>(hour) * 3600 + static_cast<uint64_t>(minute) * 60 + static_cast<uint64_t>(second);
    return true;
}

// Caller serializes snapshots/updates. All operations have fixed work; reads
// interpolate the slew instead of applying discrete corrections on a tick.
struct WallClock {
    uint64_t anchor_mono_ns{};
    uint64_t anchor_epoch_ns{};
    int64_t correction_ns{};
    uint64_t steps{};
    uint64_t slews{};
    uint64_t rejected{};
    bool rtc_valid{};

    auto applied(uint64_t now) const -> int64_t {
        uint64_t const ELAPSED = time::arithmetic::remaining(anchor_mono_ns, now);
        uint64_t const BUDGET = ELAPSED / SLEW_DIVISOR;
        uint64_t const MAGNITUDE = correction_ns < 0 ? static_cast<uint64_t>(-correction_ns) : static_cast<uint64_t>(correction_ns);
        auto const AMOUNT = static_cast<int64_t>(BUDGET < MAGNITUDE ? BUDGET : MAGNITUDE);
        return correction_ns < 0 ? -AMOUNT : AMOUNT;
    }

    auto epoch(uint64_t now) const -> uint64_t {
        // Apply slew to elapsed time before adding the epoch anchor. Capping
        // anchor+elapsed first would subtract an increasing negative correction
        // from UINT64_MAX and make an otherwise saturated clock run backwards.
        // Negative applied slew is at most elapsed/2000, so it cannot underflow.
        uint64_t const ELAPSED = time::arithmetic::remaining(anchor_mono_ns, now);
        return time::arithmetic::saturating_add(anchor_epoch_ns, time::arithmetic::add_signed(ELAPSED, applied(now)));
    }

    auto adjust(uint64_t now, int64_t delta) -> bool {
        if (delta < -MAX_CORRECTION_NS || delta > MAX_CORRECTION_NS) {
            rejected = time::arithmetic::saturating_add(rejected, 1);
            return false;
        }
        uint64_t const CURRENT = epoch(now);
        anchor_mono_ns = now;
        anchor_epoch_ns = CURRENT;
        if (steps == 0) {
            anchor_epoch_ns = time::arithmetic::add_signed(CURRENT, delta);
            steps = 1;
            correction_ns = 0;
        } else {
            // NTP measures against the current wall clock: replace the pending
            // correction, rather than adding a stale unapplied correction.
            correction_ns = delta;
            slews = time::arithmetic::saturating_add(slews, 1);
        }
        return true;
    }
};

}  // namespace ker::mod::rtc
