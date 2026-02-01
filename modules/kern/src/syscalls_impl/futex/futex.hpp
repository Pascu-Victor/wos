#pragma once

#include <abi/callnums/futex.h>

#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::futex {

// Main syscall dispatcher for futex operations
uint64_t sys_futex(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3);

// Wait on a futex address until woken or timeout
// Returns 0 on success (woken), -EAGAIN if value mismatch, -ETIMEDOUT on timeout
int64_t futex_wait(int* addr, int expected, const void* timeout);

// Wake threads waiting on a futex address
// Returns the number of threads woken
int64_t futex_wake(int* addr);

}  // namespace ker::syscall::futex
