#include "exit.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
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

    // Free the entire user address space (user stack, mmap regions, code pages, etc.)
    if (currentTask->pagemap != nullptr) {
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Destroying user address space for PID %x", currentTask->pid);
#endif
        // Switch to kernel pagemap first to avoid using the pagemap we're about to free
        ker::mod::mm::virt::switchToKernelPagemap();

        // Free all user-space pages and page tables
        ker::mod::mm::virt::destroyUserSpace(currentTask->pagemap);

        // Free the PML4 page table itself
        ker::mod::mm::phys::pageFree(currentTask->pagemap);
        currentTask->pagemap = nullptr;
    }

    // Free thread resources (TLS, user stack)
    if (currentTask->thread != nullptr) {
        ker::mod::sched::threading::destroyThread(currentTask->thread);
        currentTask->thread = nullptr;
    }

    // Free kernel stack memory (allocated separately for syscall handling)
    if (currentTask->context.syscallKernelStack != 0) {
        // We need to free from the base (bottom) of the stack
        void* stackBase = (void*)(currentTask->context.syscallKernelStack - KERNEL_STACK_SIZE);
        ker::mod::mm::phys::pageFree(stackBase);
        currentTask->context.syscallKernelStack = 0;
    }

    // Free syscall scratch area (allocated with new uint8_t[256])
    if (currentTask->context.syscallScratchArea != 0) {
        delete[] reinterpret_cast<uint8_t*>(currentTask->context.syscallScratchArea);
        currentTask->context.syscallScratchArea = 0;
    }

    // TODO: Handle signal handlers cleanup

#ifdef EXIT_DEBUG
    ker::mod::dbg::log("wos_proc_exit: Removing task from runqueue");
#endif

    // This function will not return
    jump_to_next_task_no_save();

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
