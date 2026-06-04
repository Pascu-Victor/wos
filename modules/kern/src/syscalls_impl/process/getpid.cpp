#include "getpid.hpp"

#include <cstdint>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
namespace ker::syscall::process {
auto wos_proc_getpid() -> uint64_t {
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return 0;
    }
    return mod::sched::task::process_pid(*current_task);
}
}  // namespace ker::syscall::process
