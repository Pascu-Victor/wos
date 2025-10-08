#pragma once

#include <cstdint>

namespace ker::syscall::vfs {
// Operation codes will be defined in abi when implemented further.
uint64_t sys_vfs(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3);

}  // namespace ker::syscall::vfs
