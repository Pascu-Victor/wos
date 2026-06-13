#pragma once

#include <abi/callnums/futex.h>

#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::futex {

// Main syscall dispatcher for futex operations
uint64_t sys_futex(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3);

// Wait on a futex address until woken or timeout
// Returns 0 on success (woken), -EAGAIN if value mismatch, -ETIMEDOUT on timeout
int64_t futex_wait(const int* addr, int expected, const void* timeout);

// Wake threads waiting on a futex address
// Returns the number of threads woken
int64_t futex_wake(int* addr);

// Detach and free the current task's waiter if it exits while blocked in futex_wait().
void futex_wait_cleanup_for_task(ker::mod::sched::task::Task* task);

// Wake threads waiting on a futex physical address (no virtual→physical translation needed).
// Used by the WKI remote IPC subsystem to forward OP_FUTEX_WAKE from a remote node.
// Returns the number of threads woken, or -EINVAL if phys_addr == 0.
int64_t futex_wake_by_phys(uint64_t phys_addr);

#ifdef WOS_SELFTEST
auto futex_selftest_table_init_is_serialized() -> bool;
auto futex_selftest_addr_alignment_guard() -> bool;
auto futex_selftest_stale_wake_does_not_claim_waiter() -> bool;
#endif

}  // namespace ker::syscall::futex
