#pragma once
#include <cstdint>

namespace ker::abi::futex {
enum class futex_ops : uint64_t {
    FUTEX_WAIT,
    FUTEX_WAKE,
};
}  // namespace ker::abi::futex
