#pragma once

#include <platform/sched/shutdown_ownership.hpp>

namespace ker::mod::sched {
auto can_query_current_task() -> bool;
auto hold_kernel_shutdown() -> KernelShutdownHold;
}  // namespace ker::mod::sched
