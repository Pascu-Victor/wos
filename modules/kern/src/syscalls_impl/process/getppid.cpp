#include "getppid.hpp"

#include <platform/sched/scheduler.hpp>
namespace ker::syscall::process {
auto wos_proc_getppid() -> uint64_t {
    auto* currentTask = mod::sched::get_current_task();
    if (currentTask == nullptr) {
        return 0;
    }
    return currentTask->parentPid;
}
}  // namespace ker::syscall::process
