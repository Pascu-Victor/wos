#pragma once
#include <defines/defines.hpp>

namespace ker::abi::inter::futex {
enum class futex_ops : uint64_t {
    futex_wait,
    futex_wake,
};
}  // namespace ker::abi::inter::futex
