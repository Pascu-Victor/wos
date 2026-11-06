#pragma once
#include <cpu_maintenance_controller_context.hpp>
#include <cstdint>
#include <platform/sched/shutdown_ownership.hpp>
namespace ker::mod::sched {
namespace task {
struct Task;
}
[[nodiscard]] auto can_query_current_task() -> bool;
[[nodiscard]] auto get_current_task() -> task::Task*;
[[nodiscard]] auto has_run_queues() -> bool;
[[nodiscard]] auto preempt_count() -> unsigned;
[[nodiscard]] auto interrupts_enabled() -> bool;
[[nodiscard]] auto hold_kernel_shutdown() -> KernelShutdownHold;
}  // namespace ker::mod::sched
