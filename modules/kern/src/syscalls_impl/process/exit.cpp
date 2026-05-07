#include "exit.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
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

#include "platform/sched/task.hpp"
#include "platform/smt/smt.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "waitpid.hpp"

namespace ker::syscall::process {

namespace {
using log = ker::mod::dbg::logger<"pexit">;
}

// Fill ru_utime and ru_stime in the waiter's rusage struct from the exiting child's timing data.
static void fill_rusage_for_waiter(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) {
    if (waiter->waitRusageUserAddr == 0 || waiter->pagemap == nullptr) {
        waiter->waitRusageUserAddr = 0;
        waiter->waitRusagePhysAddr = 0;
        return;
    }
    uint64_t phys = ker::mod::mm::virt::translate(waiter->pagemap, waiter->waitRusageUserAddr);
    if (phys == ker::mod::mm::virt::PADDR_INVALID || phys == 0) {
        waiter->waitRusageUserAddr = 0;
        waiter->waitRusagePhysAddr = 0;
        return;
    }
    if (waiter->waitRusagePhysAddr != 0 && waiter->waitRusagePhysAddr != phys) {
        log::warn("waitpid-rusage drift: waiter=%lu child=%lu va=0x%llx old_phys=0x%llx new_phys=0x%llx rsp=0x%llx pagemap=%p", waiter->pid,
                  child != nullptr ? child->pid : 0, (unsigned long long)waiter->waitRusageUserAddr,
                  (unsigned long long)waiter->waitRusagePhysAddr, (unsigned long long)phys, (unsigned long long)waiter->context.frame.rsp,
                  static_cast<void*>(waiter->pagemap));
    }
    waiter->waitRusagePhysAddr = phys;
    auto* ru = reinterpret_cast<KernRusage*>(ker::mod::mm::addr::get_virt_pointer(phys));
    ru->ru_utime_sec = (int64_t)(child->user_time_us / 1000000ULL);
    ru->ru_utime_usec = (int64_t)(child->user_time_us % 1000000ULL);
    ru->ru_stime_sec = (int64_t)(child->system_time_us / 1000000ULL);
    ru->ru_stime_usec = (int64_t)(child->system_time_us % 1000000ULL);
    waiter->waitRusageUserAddr = 0;
    waiter->waitRusagePhysAddr = 0;
}

static void write_wait_status_for_waiter(ker::mod::sched::task::Task* waiter, int32_t status) {
    if (waiter->waitStatusUserAddr == 0 || waiter->pagemap == nullptr) {
        waiter->waitStatusUserAddr = 0;
        waiter->waitStatusPhysAddr = 0;
        return;
    }
    uint64_t phys = ker::mod::mm::virt::translate(waiter->pagemap, waiter->waitStatusUserAddr);
    if (phys == ker::mod::mm::virt::PADDR_INVALID || phys == 0) {
        waiter->waitStatusUserAddr = 0;
        waiter->waitStatusPhysAddr = 0;
        return;
    }
    if (waiter->waitStatusPhysAddr != 0 && waiter->waitStatusPhysAddr != phys) {
        log::warn("waitpid-status drift: waiter=%lu va=0x%llx old_phys=0x%llx new_phys=0x%llx rsp=0x%llx pagemap=%p status=0x%x",
                  waiter->pid, (unsigned long long)waiter->waitStatusUserAddr, (unsigned long long)waiter->waitStatusPhysAddr,
                  (unsigned long long)phys, (unsigned long long)waiter->context.frame.rsp, static_cast<void*>(waiter->pagemap),
                  (unsigned)status);
    }
    waiter->waitStatusPhysAddr = phys;
    auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::get_virt_pointer(phys));
    *status_ptr = status;
    waiter->waitStatusUserAddr = 0;
    waiter->waitStatusPhysAddr = 0;
}

static void validate_waiter_resume_for_exit(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child, const char* path) {
    if (waiter == nullptr || waiter->pagemap == nullptr) {
        return;
    }

    if (waiter->waitResumeRipUserAddr != 0) {
        uint64_t rip_phys = ker::mod::mm::virt::translate(waiter->pagemap, waiter->waitResumeRipUserAddr);
        if (rip_phys == ker::mod::mm::virt::PADDR_INVALID || rip_phys == 0 ||
            (waiter->waitResumeRipPhysAddr != 0 && waiter->waitResumeRipPhysAddr != rip_phys)) {
            log::warn(
                "waitpid-resume drift: waiter=%lu child=%lu path=%s rip_va=0x%llx old_phys=0x%llx new_phys=0x%llx rsp_va=0x%llx pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                (unsigned long long)waiter->waitResumeRipUserAddr, (unsigned long long)waiter->waitResumeRipPhysAddr,
                (unsigned long long)((rip_phys == ker::mod::mm::virt::PADDR_INVALID) ? 0 : rip_phys),
                (unsigned long long)waiter->waitResumeRspUserAddr, static_cast<void*>(waiter->pagemap));
        }
        waiter->waitResumeRipPhysAddr = (rip_phys != ker::mod::mm::virt::PADDR_INVALID) ? rip_phys : 0;
    }

    if (waiter->waitResumeRspUserAddr != 0) {
        uint64_t rsp_phys = ker::mod::mm::virt::translate(waiter->pagemap, waiter->waitResumeRspUserAddr);
        if (rsp_phys == ker::mod::mm::virt::PADDR_INVALID || rsp_phys == 0 ||
            (waiter->waitResumeRspPhysAddr != 0 && waiter->waitResumeRspPhysAddr != rsp_phys)) {
            log::warn(
                "waitpid-stack drift: waiter=%lu child=%lu path=%s rsp_va=0x%llx old_phys=0x%llx new_phys=0x%llx rip_va=0x%llx pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                (unsigned long long)waiter->waitResumeRspUserAddr, (unsigned long long)waiter->waitResumeRspPhysAddr,
                (unsigned long long)((rsp_phys == ker::mod::mm::virt::PADDR_INVALID) ? 0 : rsp_phys),
                (unsigned long long)waiter->waitResumeRipUserAddr, static_cast<void*>(waiter->pagemap));
        }
        waiter->waitResumeRspPhysAddr = (rsp_phys != ker::mod::mm::virt::PADDR_INVALID) ? rsp_phys : 0;
    }
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
#ifdef EXIT_DEBUG
    log::trace("wos_proc_exit: pid=%lu name=%s status=%d thread=%d owner=%lu pagemap=%p", current_task->pid,
               current_task->name != nullptr ? current_task->name : "?", status, current_task->isThread, current_task->ownerPid,
               static_cast<void*>(current_task->pagemap));
#endif
    // Send SIGCHLD to parent process.
    // Threads do not send SIGCHLD - their exit is handled via futex (pthread_join).
    if (!current_task->isThread && current_task->parentPid != 0) {
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
                validate_waiter_resume_for_exit(parent, current_task, "exit-any");
                write_wait_status_for_waiter(parent, current_task->exitStatus);
                fill_rusage_for_waiter(parent, current_task);
                parent->waitingForPid = 0;
                current_task->waitedOn = true;
                // Parent is in the wait queue (not deferredTaskSwitch, not voluntaryBlock) -
                // we must explicitly reschedule it so it actually wakes up.
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            } else if (parent->deferredTaskSwitch || parent->voluntaryBlock) {
                // Parent is blocked for another reason (or WAIT_ANY_CHILD with
                // deferredTaskSwitch still true - race check will handle RAX).
                // Wake it so it can handle the signal / race check.
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            }
            parent->release();
        }
    }

    // Reparent all children of this process to init (PID 1), so init can reap them.
    // Threads do not own children directly - skip reparenting for thread exits.
    if (!current_task->isThread) {
        uint32_t count = ker::mod::sched::get_active_task_count();
        for (uint32_t i = 0; i < count; i++) {
            auto* child = ker::mod::sched::get_active_task_at(i);
            if (child != nullptr && child->parentPid == current_task->pid && child != current_task) {
                child->parentPid = 1;  // Reparent to init
            }
        }
    }

    // Close all open file descriptors and free ELF buffer before waking waiters.
    // This ensures files written by the exiting process are fully committed to
    // the VFS before waitpid returns to the parent.
    if (!current_task->isThread) {
        // Snapshot descriptor numbers before closing. vfs_close() removes from
        // fd_table and may free radix-tree nodes, so mutating during for_each()
        // can invalidate the traversal.
        while (!current_task->fd_table.empty()) {
            uint64_t fds[ker::mod::sched::task::Task::FD_TABLE_SIZE]{};
            size_t fd_count = 0;
            current_task->fd_table.for_each([&](uint64_t key, void* /*val*/) -> void {
                if (fd_count < ker::mod::sched::task::Task::FD_TABLE_SIZE) {
                    fds[fd_count++] = key;
                }
            });
            if (fd_count == 0) {
                break;
            }
            for (size_t i = 0; i < fd_count; ++i) {
                ker::vfs::vfs_close(static_cast<int>(fds[i]));
            }
        }

        if (current_task->elfBuffer != nullptr) {
            if (current_task->isElfBufferShared) {
                ker::net::wki::wki_remote_compute_release_elf_buffer(current_task->elfBuffer);
            } else {
                delete[] current_task->elfBuffer;
            }
            current_task->elfBuffer = nullptr;
            current_task->elfBufferSize = 0;
        }
    }

    // Reschedule all tasks waiting for this process to exit.
    // This happens AFTER reparenting + FD cleanup so that any files written
    // by the exiting process are fully committed to the VFS before waitpid
    // returns to the waiter.
    uint64_t waiter_lock_flags = current_task->exitWaitersLock.lock_irqsave();
    const size_t waiter_count = current_task->awaitee_on_exit.size();
    uint64_t waiting_pids[16] = {};
    const size_t waiting_pids_cap = sizeof(waiting_pids) / sizeof(waiting_pids[0]);
    for (size_t i = 0; i < waiter_count && i < waiting_pids_cap; ++i) {
        waiting_pids[i] = current_task->awaitee_on_exit[i];
    }
    current_task->exitWaitersLock.unlock_irqrestore(waiter_lock_flags);

    for (size_t i = 0; i < waiter_count && i < waiting_pids_cap; ++i) {
        uint64_t waiting_pid = waiting_pids[i];
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
                validate_waiter_resume_for_exit(waiting_task, current_task, "exit-specific");
                write_wait_status_for_waiter(waiting_task, current_task->exitStatus);
#ifdef EXIT_DEBUG
                ker::mod::dbg::log("wos_proc_exit: Set exit status %d for waiting task PID %x", current_task->exitStatus, waitingPid);
#endif
                fill_rusage_for_waiter(waiting_task, current_task);
                waiting_task->waitingForPid = 0;
            }

            // Mark that this waiter has consumed the exit status (zombie can now be reaped)
            // Note: If multiple processes wait for the same child (not typical but possible),
            // only the first one marks waitedOn. In Linux, only one waiter succeeds anyway.
            if (i == 0) {  // First waiter marks the process as waited-on
                current_task->waitedOn = true;
            }

            // Reschedule the waiting task on its last-known CPU to avoid cross-CPU migration
            // latency and the risk of landing on a non-preemptible CPU.  Fall back to the
            // least-loaded CPU only if the stored cpu index is out of range.
            uint64_t target_cpu = waiting_task->cpu;
            if (target_cpu >= ker::mod::smt::get_core_count()) {
                target_cpu = ker::mod::sched::get_least_loaded_cpu();
            }
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
    //
    // For threads (isThread == true): the pagemap is shared with the owning process
    // and must NEVER be freed or even switched away from here; GC will skip it.
    if (!current_task->isThread && current_task->pagemap != nullptr) {
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

    // Unlink any WKI wait entries belonging to this task so the timer scan
    // doesn't dereference freed stack memory after this task's stack is reclaimed.
    ker::net::wki::wki_wait_cleanup_for_task(current_task);

    // Remove any futex waiter node still owned by this task. Otherwise a later
    // futex_wake() can target a DEAD task and keep stale 64-byte wait nodes alive.
    ker::syscall::futex::futex_wait_cleanup_for_task(current_task);

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
