#pragma once

#include <cstdint>

namespace ker::syscall::vmem {
// Virtual memory syscall handler
// op: operation code (anon_allocate, anon_free)
// a1: hint address (for allocate) or address to free
// a2: size in bytes
// a3: protection flags (PROT_READ | PROT_WRITE | PROT_EXEC)
// a4: mapping flags (MAP_PRIVATE | MAP_ANONYMOUS, etc.)
uint64_t sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4);

}  // namespace ker::syscall::vmem
