#pragma once

#include <cstdint>
#include <platform/ktime/arithmetic.hpp>

namespace ker::syscall::time::math {
constexpr uint64_t NSEC_PER_SEC = 1000000000;
constexpr uint64_t REALTIME_RECHECK_NS = 1000000;

constexpr auto valid_clock(int clock) -> bool { return clock >= 0 && clock <= 3; }
constexpr auto sleep_clock(int clock) -> bool { return clock == 0 || clock == 1; }
constexpr auto to_ns(int64_t seconds, int64_t nanos, uint64_t& result) -> bool {
    if (seconds < 0 || nanos < 0 || nanos >= static_cast<int64_t>(NSEC_PER_SEC)) return false;
    auto const SEC = static_cast<uint64_t>(seconds);
    auto const NS = static_cast<uint64_t>(nanos);
    if (SEC > (UINT64_MAX - NS) / NSEC_PER_SEC) return false;
    result = SEC * NSEC_PER_SEC + NS;
    return true;
}
constexpr auto add(uint64_t base, uint64_t delta) -> uint64_t { return ker::mod::time::arithmetic::saturating_add(base, delta); }
constexpr auto ceil_us(uint64_t ns) -> uint64_t { return ns / 1000 + (ns % 1000 != 0); }
constexpr auto park_deadline_us(uint64_t mono_ns, uint64_t remaining_ns, bool realtime) -> uint64_t {
    auto const DELTA = realtime && remaining_ns > REALTIME_RECHECK_NS ? REALTIME_RECHECK_NS : remaining_ns;
    return ceil_us(add(mono_ns, DELTA));
}
}  // namespace ker::syscall::time::math
