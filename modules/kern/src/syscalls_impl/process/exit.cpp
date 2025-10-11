#include "exit.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::syscall::process {

void wos_proc_exit(int status) {
    (void)status;  // TODO: Store exit status for parent to retrieve

    auto *currentTask = ker::mod::sched::getCurrentTask();

    // TODO: Perform cleanup of task resources, memory, file descriptors, etc.
    // Mark as used until cleanup is implemented
    (void)currentTask;

    // Remove this task from the runqueue
    ker::mod::sched::removeCurrentTask();

    // This will never return
    asm volatile("int $0x20");

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
