#include "exit.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::process {

void wos_proc_exit(int status) {
    (void)status;  // TODO: Store exit status for parent to retrieve

    auto* currentTask = ker::mod::sched::getCurrentTask();
    if (currentTask == nullptr) {
        return;
    }

    ker::mod::dbg::log("wos_proc_exit: Task PID %x exiting with status %d", currentTask->pid, status);

    // Cleanup task resources

    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (currentTask->fds[i] != nullptr) {
            ker::vfs::vfs_close(static_cast<int>(i));
        }
    }

    if (currentTask->elfBuffer != nullptr) {
        ker::mod::dbg::log("wos_proc_exit: Freeing ELF buffer of size %d", currentTask->elfBufferSize);
        delete[] currentTask->elfBuffer;
        currentTask->elfBuffer = nullptr;
        currentTask->elfBufferSize = 0;
    }

    // TODO: Free other resources:
    // - User stack memory
    // - Kernel stack memory
    // - Thread/TLS structures
    // - Page tables
    // - Signal handlers
    // TODO: Signal handling:
    // - Notify parent process (SIGCHLD)

    ker::mod::dbg::log("wos_proc_exit: Removing task from runqueue");

    // Remove this task from the runqueue
    ker::mod::sched::removeCurrentTask();

    // This will never return
    asm volatile("int $0x20");

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
