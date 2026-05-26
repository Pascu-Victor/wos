#pragma once
#include <cstdint>

namespace ker::abi::futex {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class futex_ops : uint64_t {
    FUTEX_WAIT,
    FUTEX_WAKE,
};
}  // namespace ker::abi::futex
