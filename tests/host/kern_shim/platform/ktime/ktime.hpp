#pragma once

// Host shim: replaces kernel time source with clock_gettime.

#include <time.h>

#include <cstdint>

namespace ker::mod::ktime {

inline uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

inline uint64_t now_us() { return now_ns() / 1000; }

}  // namespace ker::mod::ktime

namespace ker::mod::time {

inline uint64_t get_us() { return ker::mod::ktime::now_us(); }

}  // namespace ker::mod::time
