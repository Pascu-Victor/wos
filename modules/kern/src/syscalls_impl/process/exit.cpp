#include "exit.hpp"
#include "waitpid.hpp"

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

// Fill ru_utime and ru_stime in the waiter's rusage struct from the exiting child's timing data.
static void fill_rusage_for_waiter(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) {
    if (waiter->waitRusagePhysAddr == 0) {
        return;
    }
    auto* ru = reinterpret_cast<KernRusage*>(ker::mod::mm::addr::getVirtPointer(waiter->waitRusagePhysAddr));
    ru->ru_utime_sec  = (int64_t)(child->user_time_us / 1000000ULL);
    ru->ru_utime_usec = (int64_t)(child->user_time_us % 1000000ULL);
    ru->ru_stime_sec  = (int64_t)(child->system_time_us / 1000000ULL);
    ru->ru_stime_usec = (int64_t)(child->system_time_us % 1000000ULL);
    waiter->waitRusagePhysAddr = 0;
}

void wos_proc_exit(int status) {
    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
        return;
    }

    // CRITICAL: Atomically transition to EXITING state.
    // This prevents other CPUs from scheduling this task while we're cleaning up.
    // If this fails, another CPU already started our exit (shouldn't happen).
    if (!current_task->transitionState(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
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

    // Store exit status in POSIX waitpid format
    current_task->exitStatus = (status & 0xff) << 8;
    current_task->hasExited = true;

    // Send SIGCHLD to parent process
    if (current_task->parentPid != 0) {
        auto* parent = ker::mod::sched::find_task_by_pid_safe(current_task->parentPid);
        if (parent != nullptr) {
            // Set SIGCHLD (signal 17) pending on parent
            parent->sigPending |= (1ULL << (17 - 1));

            // If parent is waiting for any child (pid==-1 / WAIT_ANY_CHILD),
            // set the return value so waitpid returns the exited child's PID.
            // Only safe when deferredTaskSwitch is false (parent is in wait queue,
            // its context is stable). If deferredTaskSwitch is still true,
            // the deferred_task_switch race check will handle it.
            static constexpr auto WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
            if (parent->waitingForPid == WAIT_ANY_CHILD && !parent->deferredTaskSwitch) {
                parent->context.regs.rax = current_task->pid;
                if (parent->waitStatusPhysAddr != 0) {
                    auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::getVirtPointer(parent->waitStatusPhysAddr));
                    *status_ptr = current_task->exitStatus;
                }
                fill_rusage_for_waiter(parent, current_task);
                parent->waitingForPid = 0;
                current_task->waitedOn = true;
                // Parent is in the wait queue (not deferredTaskSwitch, not voluntaryBlock) —
                // we must explicitly reschedule it so it actually wakes up.
                uint64_t cpu = ker::mod::sched::get_least_loaded_cpu();
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            } else if (parent->deferredTaskSwitch || parent->voluntaryBlock) {
                // Parent is blocked for another reason (or WAIT_ANY_CHILD with
                // deferredTaskSwitch still true — race check will handle RAX).
                // Wake it so it can handle the signal / race check.
                uint64_t cpu = ker::mod::sched::get_least_loaded_cpu();
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            }
            parent->release();
        }
    }

    // Reschedule all tasks waiting for this process to exit
    for (uint64_t i = 0; i < current_task->awaitee_on_exit_count; ++i) {
        uint64_t waiting_pid = current_task->awaitee_on_exit[i];
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Rescheduling waiting task PID %x", waitingPid);
#endif

        // Use findTaskByPidSafe to get a refcounted reference - prevents use-after-free
        auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(waiting_pid);
        if (waiting_task != nullptr) {
            // Only modify the waiting task's saved context when it's safely in waitQueue
            // (deferredTaskSwitch is false). When deferredTaskSwitch is true, the task is
            // still running on another CPU - writing to context.regs is a data race and the
            // values would be overwritten by deferredTaskSwitch's context save anyway.
            // In that case, deferredTaskSwitch() will detect hasExited==true and set rax
            // correctly before re-scheduling the task (see scheduler.cpp deferredTaskSwitch).
            if (!waiting_task->deferredTaskSwitch) {
                waiting_task->context.regs.rax = current_task->pid;

                if (waiting_task->waitStatusPhysAddr != 0) {
                    auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::getVirtPointer(waiting_task->waitStatusPhysAddr));
                    *status_ptr = status;
#ifdef EXIT_DEBUG
                    ker::mod::dbg::log("wos_proc_exit: Set exit status %d for waiting task PID %x", status, waitingPid);
#endif
                }
                fill_rusage_for_waiter(waiting_task, current_task);
            }

            // Mark that this waiter has consumed the exit status (zombie can now be reaped)
            // Note: If multiple processes wait for the same child (not typical but possible),
            // only the first one marks waitedOn. In Linux, only one waiter succeeds anyway.
            if (i == 0) {  // First waiter marks the process as waited-on
                current_task->waitedOn = true;
            }

            // Reschedule the waiting task on the least loaded CPU.
            // reschedule_task_for_cpu validates state and removes from all queues before
            // adding, preventing double-queuing even if the task is still currentTask.
            uint64_t target_cpu = ker::mod::sched::get_least_loaded_cpu();
            ker::mod::sched::reschedule_task_for_cpu(target_cpu, waiting_task);
#ifdef EXIT_DEBUG
            ker::mod::dbg::log("wos_proc_exit: Successfully rescheduled waiting task PID %x on CPU %d", waitingPid, waitingTask->cpu);
#endif

            // Release the reference we acquired from findTaskByPidSafe
            waiting_task->release();
        } else {
#ifdef EXIT_DEBUG
            ker::mod::dbg::log("wos_proc_exit: Could not find waiting task PID %x", waitingPid);
#endif
        }
    }

    // Reparent all children of this process to init (PID 1), so init can reap them.
    // This prevents orphaned zombies from accumulating forever.
    {
        uint32_t count = ker::mod::sched::get_active_task_count();
        for (uint32_t i = 0; i < count; i++) {
            auto* child = ker::mod::sched::get_active_task_at(i);
            if (child != nullptr && child->parentPid == current_task->pid && child != current_task) {
                child->parentPid = 1;  // Reparent to init
            }
        }
    }

    // CLEANUP TASK RESOURCES

    // Close all open file descriptors
    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (current_task->fds[i] != nullptr) {
            ker::vfs::vfs_close(static_cast<int>(i));
        }
    }

    // Free ELF buffer
    if (current_task->elfBuffer != nullptr) {
#ifdef EXIT_DEBUG
        ker::mod::dbg::log("wos_proc_exit: Freeing ELF buffer of size %d", currentTask->elfBufferSize);
#endif
        delete[] current_task->elfBuffer;
        current_task->elfBuffer = nullptr;
        current_task->elfBufferSize = 0;
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
    if (current_task->pagemap != nullptr) {
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
    current_task->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
    current_task->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);

#ifdef EXIT_DEBUG
    ker::mod::dbg::log("wos_proc_exit: Removing task from runqueue");
#endif

    // This function will not return - it switches to the next task
    // The current task is moved to expiredTasks list by jumpToNextTask
    jump_to_next_task_no_save();

    __builtin_unreachable();
}

}  // namespace ker::syscall::process
