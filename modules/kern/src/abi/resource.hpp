#pragma once

#include <cstdint>

namespace ker::abi::resource {
inline constexpr uint64_t VERSION = 1;
inline constexpr uint64_t INFINITY_VALUE = UINT64_MAX;
inline constexpr unsigned COUNT = 16;
// Selectors match the existing WOS libc RLIMIT_* ABI.
enum class Id : uint32_t { CPU = 0, FSIZE = 1, DATA = 2, STACK = 3, CORE = 4, RSS = 5, NPROC = 6, NOFILE = 7, AS = 9 };

struct Limit {
    uint64_t soft = INFINITY_VALUE;
    uint64_t hard = INFINITY_VALUE;
};
static_assert(sizeof(Limit) == 16);

enum class Denial : uint32_t { NONE, SOFT_LIMIT, OVERFLOW, HARD_LIMIT };
struct Usage {
    Limit limit{};
    uint64_t current = 0;
    uint64_t peak = 0;
    uint64_t denied = 0;
    Denial reason = Denial::NONE;
    uint32_t reserved = 0;
};
static_assert(sizeof(Usage) == 48);
}  // namespace ker::abi::resource
