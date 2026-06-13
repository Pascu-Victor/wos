#pragma once

#include <cstdint>
#include <limits>

namespace ker::net::wki {

constexpr auto wki_saturating_add_us(uint64_t lhs, uint64_t rhs) -> uint64_t {
    if (std::numeric_limits<uint64_t>::max() - lhs < rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

constexpr auto wki_saturating_mul_us(uint64_t lhs, uint64_t rhs) -> uint64_t {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

constexpr auto wki_future_deadline_us(uint64_t now_us, uint64_t delay_us) -> uint64_t { return wki_saturating_add_us(now_us, delay_us); }

constexpr auto wki_next_or_immediate_deadline_us(uint64_t fire_at_us, uint64_t now_us) -> uint64_t {
    return fire_at_us > now_us ? fire_at_us : wki_future_deadline_us(now_us, 1);
}

}  // namespace ker::net::wki
