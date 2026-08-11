#pragma once

#include <cstdint>

namespace ker::abi {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class sys_time_ops : uint64_t {
    GETTIMEOFDAY = 0,
    CLOCK_GETTIME = 1,
    NANOSLEEP = 2,
    TIMES = 3,
    SETITIMER = 4,
    GETITIMER = 5,
    gettimeofday = GETTIMEOFDAY,
    clock_gettime = CLOCK_GETTIME,
    nanosleep = NANOSLEEP,
    times = TIMES,
    setitimer = SETITIMER,
    getitimer = GETITIMER,
};
}  // namespace ker::abi
