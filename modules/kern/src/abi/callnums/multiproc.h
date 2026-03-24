#pragma once
#include <cstdint>

namespace ker::abi::multiproc {
enum class threadInfoOps : uint64_t {
    currentThreadId,
    nativeThreadCount,
    currentCpu,
};
enum class threadControlOps : uint64_t {
    setTCB = 0x100,  // Offset to avoid overlap with threadInfoOps
    yield = 0x101,
    threadCreate = 0x102,  // Create a new userspace thread in the same process
    threadExit = 0x103,    // Exit current thread without exiting the process
    setAffinity = 0x104,   // Set a thread's CPU affinity mask
    getAffinity = 0x105,   // Get a thread's CPU affinity mask
};
}  // namespace ker::abi::multiproc
