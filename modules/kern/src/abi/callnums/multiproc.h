#pragma once
#include <cstdint>

namespace ker::abi::multiproc {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class threadInfoOps : uint64_t {
    CURRENT_THREAD_ID,
    NATIVE_THREAD_COUNT,
    CURRENT_CPU,
};
// NOLINTNEXTLINE(performance-enum-size)
enum class threadControlOps : uint64_t {
    SET_TCB = 0x100,  // Offset to avoid overlap with threadInfoOps
    YIELD = 0x101,
    THREAD_CREATE = 0x102,  // Create a new userspace thread in the same process
    THREAD_EXIT = 0x103,    // Exit current thread without exiting the process
    SET_AFFINITY = 0x104,   // Set a thread's CPU affinity mask
    GET_AFFINITY = 0x105,   // Get a thread's CPU affinity mask
    CREATE_DOMAIN = 0x106,  // Create a named leaf CPU domain
    SET_DOMAIN = 0x107,     // Assign a task to a domain
    QUERY_DOMAIN = 0x108,   // Query domain info (cpu_mask + per-CPU loads)
};
}  // namespace ker::abi::multiproc
