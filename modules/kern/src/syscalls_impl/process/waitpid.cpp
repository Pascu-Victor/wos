#include "waitpid.hpp"

#include <platform/dbg/dbg.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>

namespace ker::syscall::process {

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    (void)options;  // TODO: Handle WNOHANG and other options

    auto* currentTask = ker::mod::sched::get_current_task();
    if (currentTask == nullptr) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Current task is null");
#endif
        return static_cast<uint64_t>(-1);
    }

    // Save current task's context
    currentTask->context.regs = gpr;

#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: PID %x waiting for PID %x", currentTask->pid, pid);
#endif

    // Find the task with the given PID
    auto* target_task = ker::mod::sched::find_task_by_pid(pid);
    if (target_task == nullptr) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Target task PID %x not found", pid);
#endif
        return static_cast<uint64_t>(-1);  // Task not found
    }

    // Check if the target task has already exited
    if (target_task->hasExited) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Target task PID %x has already exited with status %d", pid, targetTask->exitStatus);
#endif
        if (status != nullptr) {
            *status = target_task->exitStatus;
        }
        // Mark that the parent has retrieved the exit status (zombie can now be reaped)
        target_task->waitedOn = true;
        return pid;  // Return the PID of the exited process
    }

    // Add the current task's PID to the target task's awaitee list
    if (target_task->awaitee_on_exit_count >= ker::mod::sched::task::Task::MAX_AWAITEE_COUNT) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Awaitee list full for PID %x", pid);
#endif
        return static_cast<uint64_t>(-1);  // Awaitee list is full
    }

    target_task->awaitee_on_exit[target_task->awaitee_on_exit_count] = currentTask->pid;
    target_task->awaitee_on_exit_count++;
#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: Added PID %x to awaitee list of PID %x", currentTask->pid, pid);
#endif

    currentTask->waitingForPid = pid;
    // May be different pagemap so store physical address
    if (status != nullptr) {
        currentTask->waitStatusPhysAddr = ker::mod::mm::virt::translate(currentTask->pagemap, reinterpret_cast<uint64_t>(status));
    } else {
        currentTask->waitStatusPhysAddr = 0;
    }

    // Set the deferred task switch flag - the task will be moved to wait queue after syscall returns
    currentTask->deferredTaskSwitch = true;
#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: Setting deferred task switch for PID %x, flag at %p = %d", currentTask->pid,
                       &(currentTask->deferredTaskSwitch), currentTask->deferredTaskSwitch);
    ker::mod::dbg::log("wos_proc_waitpid: Task pointer = %p", currentTask);
#endif

    // Return normally - the syscall.asm will check the flag and perform the task switch
    // The return value will be overwritten when the awaited process exits to PID of the exited process
    return 0;
}

}  // namespace ker::syscall::process
