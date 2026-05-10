#include "getpid.hpp"

#include <cstdint>
#include <platform/sched/scheduler.hpp>
namespace ker::syscall::process {
auto wos_proc_getpid() -> uint64_t {
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return 0;
    }
    return current_task->pid;
}
}  // namespace ker::syscall::process
