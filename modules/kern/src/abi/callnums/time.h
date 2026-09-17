#pragma once

#include <stdint.h>

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
    CLOCK_GETRES = 6,
    CLOCK_NANOSLEEP = 7,
    SETITIMER_V1 = 8,
    gettimeofday = GETTIMEOFDAY,
    clock_gettime = CLOCK_GETTIME,
    nanosleep = NANOSLEEP,
    times = TIMES,
    setitimer = SETITIMER,
    getitimer = GETITIMER,
    clock_getres = CLOCK_GETRES,
    clock_nanosleep = CLOCK_NANOSLEEP,
    setitimer_v1 = SETITIMER_V1,
};

inline constexpr uint64_t TIME_SLEEP_VERSION = 1;
inline constexpr uint32_t TIME_SLEEP_ABSTIME = 1;
struct ClockSleepRequest {
    int32_t clock_id;
    uint32_t flags;
    int64_t seconds;
    int64_t nanoseconds;
    uint64_t remaining_address;
};
static_assert(sizeof(ClockSleepRequest) == 32);
inline constexpr uint64_t TIME_ITIMER_VERSION = 1;
struct SetItimerRequest {
    int32_t which;
    uint32_t reserved;
    uint64_t new_value_address;
    uint64_t old_value_address;
};
static_assert(sizeof(SetItimerRequest) == 24);
}  // namespace ker::abi
