#include "getppid.hpp"

#include <cstdint>
#include <platform/sched/scheduler.hpp>

#include "platform/sched/task.hpp"
namespace ker::syscall::process {
auto wos_proc_getppid() -> uint64_t {
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return 0;
    }
    // Some user tasks, including WKI receiver-side remote tasks, are parentless
    // for lifecycle purposes. Expose them to userspace as init-adopted.
    const bool PARENTLESS_PROCESS = current_task->parent_pid == 0 && current_task->type == mod::sched::task::TaskType::PROCESS;
    if (PARENTLESS_PROCESS && current_task->pid != 1) {
        return 1;
    }
    return current_task->parent_pid;
}
}  // namespace ker::syscall::process
