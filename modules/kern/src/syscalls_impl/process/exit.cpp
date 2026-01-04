#include "exit.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/threading.hpp>
#include <platform/sys/context_switch.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::process {

void wos_proc_exit(int status) {
    auto* currentTask = ker::mod::sched::getCurrentTask();
    if (currentTask == nullptr) {
        return;
    }

    // CRITICAL: Atomically transition to EXITING state.
    // This prevents other CPUs from scheduling this task while we're cleaning up.
    // If this fails, another CPU already started our exit (shouldn't happen).
    if (!currentTask->transitionState(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
        // Already exiting - this shouldn't happen but handle it gracefully
        for (;;) {
            asm volatile("hlt");
        }
    }

    // Memory barrier to ensure state change is visible to all CPUs
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

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

        // Use findTaskByPidSafe to get a refcounted reference - prevents use-after-free
        auto* waitingTask = ker::mod::sched::findTaskByPidSafe(waitingPid);
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

            // Release the reference we acquired from findTaskByPidSafe
            waitingTask->release();
        } else {
#ifdef EXIT_DEBUG
            ker::mod::dbg::log("wos_proc_exit: Could not find waiting task PID %x", waitingPid);
#endif
        }
    }

    // CLEANUP TASK RESOURCES

    // Close all open file descriptors
    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (currentTask->fds[i] != nullptr) {
            ker::vfs::vfs_close(static_cast<int>(i));
        }
    }

    // Free ELF buffer
    if (currentTask->elfBuffer != nullptr) {
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Freeing ELF buffer of size %d", currentTask->elfBufferSize);
#endif
        delete[] currentTask->elfBuffer;
        currentTask->elfBuffer = nullptr;
        currentTask->elfBufferSize = 0;
    }

    // CRITICAL: Do NOT modify or destroy pagemap/thread here!
    // Another CPU might be in switchTo() and about to:
    //   - Load our pagemap into CR3
    //   - Read thread->gsbase/fsbase
    //
    // Even calling destroyUserSpace() is unsafe because it MODIFIES the pagemap
    // contents (clears page table entries) while another CPU might be using it.
    //
    // ALL pagemap and thread cleanup is deferred to gcExpiredTasks() which runs
    // after the epoch grace period, ensuring no CPU is still using these resources.
    //
    // The downside is user pages remain allocated ~1 second longer, but this is
    // necessary for correctness in the face of concurrent scheduling.
    if (currentTask->pagemap != nullptr) {
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Deferring pagemap destruction for PID %x to GC", currentTask->pid);
#endif
        // Switch to kernel pagemap so we don't use our own pagemap anymore
        ker::mod::mm::virt::switchToKernelPagemap();
        // Pagemap destruction deferred to gcExpiredTasks()
    }

    // Thread destruction deferred to gcExpiredTasks()

    // NOTE: We CANNOT free the kernel stack here because we're still running on it!
    // The kernel stack will be freed later when the task is fully cleaned up
    // (after switching to a different task's kernel stack).
    // The syscallKernelStack and syscallScratchArea are left intact for now.
    // They will be cleaned up by jumpToNextTask when it moves the task to expiredTasks,
    // and eventually by a garbage collection mechanism.

    // TODO: Handle signal handlers cleanup

    // Transition to DEAD state and record death epoch for garbage collection.
    // The task will be reclaimed once all CPUs have passed through the grace period.
    currentTask->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
    currentTask->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);

#ifdef EXIT_DEBUG
    ker::mod::dbg::log("wos_proc_exit: Removing task from runqueue");
#endif

    // This function will not return - it switches to the next task
    // The current task is moved to expiredTasks list by jumpToNextTask
    jump_to_next_task_no_save();

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
