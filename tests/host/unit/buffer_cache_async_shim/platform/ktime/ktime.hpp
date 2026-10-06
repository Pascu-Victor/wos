#pragma once

// Cache target only: keep the real host clock unless a bounded admission
// timeout test supplies a deterministic task-local microsecond clock.
#include <time.h>

#include <cstdint>

namespace ker::mod::ktime {
namespace cache_test {
inline thread_local uint64_t (*clock_us)(){};
}
inline auto now_ns() -> uint64_t {
    if (cache_test::clock_us != nullptr) return cache_test::clock_us() * 1000;
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
}
inline auto now_us() -> uint64_t { return now_ns() / 1000; }
}  // namespace ker::mod::ktime

namespace ker::mod::time {
inline auto get_us() -> uint64_t { return ker::mod::ktime::now_us(); }
}  // namespace ker::mod::time
