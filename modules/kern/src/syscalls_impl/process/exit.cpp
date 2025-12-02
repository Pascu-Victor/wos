#include "exit.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/context_switch.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::process {

void wos_proc_exit(int status) {
    auto* currentTask = ker::mod::sched::getCurrentTask();
    if (currentTask == nullptr) {
        return;
    }

#ifdef EXIT_DEBUG
    ker::mod::dbg::log("wos_proc_exit: Task PID %x exiting with status %d", currentTask->pid, status);
#endif

    // Store exit status for waiting processes
    currentTask->exitStatus = status;
    currentTask->hasExited = true;

    // Reschedule all tasks waiting for this process to exit
    for (uint64_t i = 0; i < currentTask->awaitee_on_exit_count; ++i) {
        uint64_t waitingPid = currentTask->awaitee_on_exit[i];
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Rescheduling waiting task PID %x", waitingPid);
#endif

        auto* waitingTask = ker::mod::sched::findTaskByPid(waitingPid);
        if (waitingTask != nullptr) {
            // Set the return value of waitpid (RAX = child PID that exited)
            waitingTask->context.regs.rax = currentTask->pid;

            // CRITICAL: Clear the deferred task switch flag so the task doesn't
            // immediately get moved back to wait queue on its next syscall
            waitingTask->deferredTaskSwitch = false;

            // Write exit status to the status pointer if provided
            if (waitingTask->waitStatusPhysAddr != 0) {
                // Get kernel-accessible pointer via HHDM offset
                auto* statusPtr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::getVirtPointer(waitingTask->waitStatusPhysAddr));
                *statusPtr = status;
#ifdef EXIT_DEBUG
                ker::mod::dbg::log("wos_proc_exit: Set exit status %d for waiting task PID %x", status, waitingPid);
#endif
            }

            // Reschedule the waiting task on its original CPU
            ker::mod::sched::rescheduleTaskForCpu(waitingTask->cpu, waitingTask);
#ifdef EXIT_DEBUG
            ker::mod::dbg::log("wos_proc_exit: Successfully rescheduled waiting task PID %x on CPU %d", waitingPid, waitingTask->cpu);
#endif
        } else {
#ifdef EXIT_DEBUG
            ker::mod::dbg::log("wos_proc_exit: Could not find waiting task PID %x", waitingPid);
#endif
        }
    }

    // Cleanup task resources

    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (currentTask->fds[i] != nullptr) {
            ker::vfs::vfs_close(static_cast<int>(i));
        }
    }

    if (currentTask->elfBuffer != nullptr) {
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Freeing ELF buffer of size %d", currentTask->elfBufferSize);
#endif
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

#ifdef EXIT_DEBUG
    ker::mod::dbg::log("wos_proc_exit: Removing task from runqueue");
#endif

    // This function will not return
    jump_to_next_task_no_save();

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
