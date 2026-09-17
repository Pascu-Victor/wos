#pragma once

#include <cstdint>

namespace ker::mod::time::arithmetic {

constexpr auto saturating_add(uint64_t lhs, uint64_t rhs) -> uint64_t { return rhs > UINT64_MAX - lhs ? UINT64_MAX : lhs + rhs; }

// Exact 64x64/64 conversion. The high-half guard makes divq non-trapping,
// including for a zero divisor. No floating point or compiler runtime helper
// is needed in early boot/IRQ callers. Zero divisors saturate as invalid scale.
inline auto mul_div(uint64_t value, uint64_t multiplier, uint64_t divisor, bool round_up = false) -> uint64_t {
    auto const PRODUCT = static_cast<unsigned __int128>(value) * multiplier;
    uint64_t lo = static_cast<uint64_t>(PRODUCT);
    uint64_t hi = static_cast<uint64_t>(PRODUCT >> 64);
    if (divisor == 0 || hi >= divisor) return UINT64_MAX;
    asm("divq %[divisor]" : "+a"(lo), "+d"(hi) : [divisor] "r"(divisor) : "cc");
    return round_up && hi != 0 ? saturating_add(lo, 1) : lo;
}

constexpr auto add_signed(uint64_t base, int64_t delta) -> uint64_t {
    if (delta >= 0) return saturating_add(base, static_cast<uint64_t>(delta));
    // Avoid negating INT64_MIN.
    uint64_t const MAGNITUDE = static_cast<uint64_t>(-(delta + 1)) + 1;
    return MAGNITUDE > base ? 0 : base - MAGNITUDE;
}

constexpr auto saturating_signed_add(int64_t lhs, int64_t rhs) -> int64_t {
    if (rhs > 0 && lhs > INT64_MAX - rhs) return INT64_MAX;
    if (rhs < 0 && lhs < INT64_MIN - rhs) return INT64_MIN;
    return lhs + rhs;
}

constexpr auto remaining(uint64_t now, uint64_t deadline) -> uint64_t { return deadline > now ? deadline - now : 0; }

// First phase-aligned deadline strictly after now. UINT64_MAX is a terminal
// saturation, not a deadline to rearm repeatedly after it has been reached.
constexpr auto periodic_next(uint64_t deadline, uint64_t period, uint64_t now) -> uint64_t {
    if (period == 0 || deadline == UINT64_MAX) return UINT64_MAX;
    if (now < deadline) return deadline;
    return saturating_add(now, period - ((now - deadline) % period));
}

}  // namespace ker::mod::time::arithmetic
