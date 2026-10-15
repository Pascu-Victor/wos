#pragma once

#include <abi/numa.h>

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::vmem {
// Syscall context, exact retained executing Task. Request/output transport may
// fault normally; only PAGE_NODES payload lookup is resident-only. No caller
// MM/publication/policy lock or outstanding payload pin may surround this call.
[[nodiscard]] auto query_numa_for_task(ker::mod::sched::task::Task& actor, uint64_t request_address, uint64_t bytes, uint64_t unused3,
                                       uint64_t unused4) -> uint64_t;
[[nodiscard]] auto set_memory_policy_for_task(ker::mod::sched::task::Task& actor, uint64_t request_address, uint64_t bytes,
                                              uint64_t unused3, uint64_t unused4) -> uint64_t;
}  // namespace ker::syscall::vmem
