#include "getppid.hpp"

#include <cstdint>
#include <platform/sched/scheduler.hpp>
#include <syscalls_impl/process/child_events.hpp>

#include "platform/sched/task.hpp"
namespace ker::syscall::process {
auto wos_proc_getppid() -> uint64_t {
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return 0;
    }
    return child_events::namespace_parent_pid(*current_task);
}
}  // namespace ker::syscall::process
