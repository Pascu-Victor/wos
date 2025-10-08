#pragma once

#include <cstdint>

namespace ker::abi {
enum class sys_time_ops : uint64_t { gettimeofday = 0, clock_gettime = 1 };
}  // namespace ker::abi
