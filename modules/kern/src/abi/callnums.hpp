#pragma once

// Keep this header freestanding: avoid pulling in any typedefs that may
// conflict with userspace headers. Use a builtin-width unsigned type for
// the underlying enum storage instead of a typedef like uint64_t.
#include <cstdint>
namespace ker::abi {
// Syscall selectors are passed through 64-bit registers; the width is ABI, not storage overhead.
// NOLINTNEXTLINE(performance-enum-size)
enum class callnums : uint64_t { SYS_LOG, FUTEX, THREADING, PROCESS, TIME, VFS, NET, VMEM, VMEM_MAP, DEBUG };
}  // namespace ker::abi
