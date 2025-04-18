#pragma once
#include <std/cstdint.hpp>

namespace ker::abi::futex {
enum class futex_ops : uint64_t {
    futex_wait,
    futex_wake,
};
}  // namespace ker::abi::futex
