#pragma once
#include <platform/sched/task.hpp>
namespace firmware_host {
inline thread_local bool ordinary = true;
inline thread_local ker::mod::sched::task::Task* current = nullptr;
inline thread_local unsigned queries = 0;
}  // namespace firmware_host
namespace ker::mod::sched {
inline auto get_current_task() -> task::Task* {
    ++firmware_host::queries;
    return firmware_host::current;
}
inline auto hold_kernel_shutdown() -> KernelShutdownHold {
    auto* owner = get_current_task();
    return KernelShutdownHold(owner == nullptr ? nullptr : &owner->kernel_shutdown_ownership);
}
}  // namespace ker::mod::sched
