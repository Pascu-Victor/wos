#include "getpid.hpp"

#include <platform/sched/scheduler.hpp>
namespace ker::syscall::process {
auto wos_proc_getpid() -> uint64_t {
    auto* currentTask = mod::sched::get_current_task();
    if (currentTask == nullptr) {
        return 0;
    }
    return currentTask->pid;
}
}  // namespace ker::syscall::process
