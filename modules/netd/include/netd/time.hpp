#pragma once

#include <cstdint>

namespace netd {

constexpr uint32_t USEC_PER_SEC = 1000000;

auto monotonic_now_us() -> uint64_t;
void sleep_until_us(uint64_t deadline_us);
void sleep_for_seconds(uint32_t seconds);

}  // namespace netd
