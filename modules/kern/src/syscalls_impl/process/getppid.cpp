#include "getppid.hpp"

#include <platform/sched/scheduler.hpp>
namespace ker::syscall::process {
auto wos_proc_getppid() -> uint64_t {
    auto* currentTask = mod::sched::get_current_task();
    if (currentTask == nullptr) {
        return 0;
    }
    // Some user tasks, including WKI receiver-side remote tasks, are parentless
    // for lifecycle purposes. Expose them to userspace as init-adopted.
    const bool parentless_process = currentTask->parentPid == 0 && currentTask->type == mod::sched::task::TaskType::PROCESS;
    if (parentless_process && currentTask->pid != 1) {
        return 1;
    }
    return currentTask->parentPid;
}
}  // namespace ker::syscall::process
