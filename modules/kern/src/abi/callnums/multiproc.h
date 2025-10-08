#pragma once
#include <cstdint>

namespace ker::abi::multiproc {
enum class threadInfoOps : uint64_t {
    currentThreadId,
    nativeThreadCount,
};
enum class threadControlOps : uint64_t {
    setTCB,
};
}  // namespace ker::abi::multiproc
