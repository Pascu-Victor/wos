#pragma once
#include <std/cstdint.hpp>

namespace ker::abi::multiproc {
enum class threadInfoOps : uint64_t {
    currentThreadId,
    nativeThreadCount,
};

}  // namespace ker::abi::multiproc
