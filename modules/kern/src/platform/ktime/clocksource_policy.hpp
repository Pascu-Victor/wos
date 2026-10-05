#pragma once

#include <atomic>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>

namespace ker::mod::time::clocksource {

enum class State : uint8_t {
    unavailable,
    tsc,
    missing_tsc,
    noninvariant_tsc,
    calibration_failed,
    smp_unqualified,
    tsc_backwards,
    tsc_drift,
    kvm_clock,
    kvm_clock_failed
};
inline constexpr uint64_t MAX_CALIBRATION_PPM = 1000;
inline constexpr uint64_t MAX_DEVIATION_NS = 100000;

constexpr auto valid_hpet(uint64_t period_fs, bool counter64) -> bool { return counter64 && period_fs != 0 && period_fs <= 100000000; }

inline auto spread_ppm(uint64_t minimum_hz, uint64_t maximum_hz) -> uint64_t {
    return maximum_hz < minimum_hz ? UINT64_MAX : arithmetic::mul_div(maximum_hz - minimum_hz, 1000000, minimum_hz, true);
}

inline auto qualify(bool has_tsc, bool invariant, uint64_t minimum_hz, uint64_t maximum_hz, uint64_t cpus) -> State {
    if (!has_tsc) return State::missing_tsc;
    if (!invariant) return State::noninvariant_tsc;
    if (minimum_hz < 1000000 || maximum_hz > 20000000000ULL || spread_ppm(minimum_hz, maximum_hz) > MAX_CALIBRATION_PPM)
        return State::calibration_failed;
    return cpus == 1 ? State::tsc : State::smp_unqualified;
}

constexpr auto deviation(uint64_t candidate, uint64_t before, uint64_t after) -> uint64_t {
    if (candidate < before) return before - candidate;
    return candidate > after ? candidate - after : 0;
}

// Linearization point is the successful CAS or the load that observes a newer
// time. Overlapping calls may return in either order; sequential calls cannot
// regress even if the caller migrates. No lock can be interrupted while held.
inline auto publish_max(std::atomic<uint64_t>& floor, uint64_t candidate) -> uint64_t {
    uint64_t previous = floor.load(std::memory_order_relaxed);
    while (previous < candidate && !floor.compare_exchange_weak(previous, candidate, std::memory_order_relaxed)) {
    }
    return previous > candidate ? previous : candidate;
}

static_assert(std::atomic<uint64_t>::is_always_lock_free);

}  // namespace ker::mod::time::clocksource
