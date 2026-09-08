#pragma once

#include <abi/callnums/time.h>

#include <abi/callnums.hpp>
#include <cstdint>

namespace ker::syscall::time {
auto sys_time_get(uint64_t op, uint64_t arg1, uint64_t arg2) -> uint64_t;
#ifdef WOS_SELFTEST
auto selftest_cpu_observation() -> bool;
#endif
}  // namespace ker::syscall::time
