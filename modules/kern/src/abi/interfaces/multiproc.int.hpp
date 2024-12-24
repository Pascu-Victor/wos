#pragma once
#include <defines/defines.hpp>

namespace ker::abi::inter::multiproc {
enum class threadInfoOps : uint64_t {
    currentThreadId,
    nativeThreadCount,
};

}  // namespace ker::abi::inter::multiproc
