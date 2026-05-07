#include "waitpid.hpp"

#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>

namespace ker::syscall::process {

// Fill POSIX struct rusage with the child's timing data (ru_utime and ru_stime only).
// rusage_phys_addr: physical address of the userspace rusage struct, 0 to skip.
static void fill_rusage(uint64_t rusage_phys_addr, ker::mod::sched::task::Task* child) {
    if (rusage_phys_addr == 0 || rusage_phys_addr == ker::mod::mm::virt::PADDR_INVALID) return;
    auto* ru = reinterpret_cast<KernRusage*>(ker::mod::mm::addr::get_virt_pointer(rusage_phys_addr));
    ru->ru_utime_sec = (int64_t)(child->user_time_us / 1000000ULL);
    ru->ru_utime_usec = (int64_t)(child->user_time_us % 1000000ULL);
    ru->ru_stime_sec = (int64_t)(child->system_time_us / 1000000ULL);
    ru->ru_stime_usec = (int64_t)(child->system_time_us % 1000000ULL);
}

// Sentinel value: waitingForPid == WAIT_ANY_CHILD means "wait for any child"
static constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);

// Scan for an exited child of the given parent task.
// Returns the exited child task pointer, or nullptr if none found.
static auto find_exited_child(ker::mod::sched::task::Task* parent) -> ker::mod::sched::task::Task* {
    // Iterate active task list for children
    uint32_t count = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < count; i++) {
        auto* t = ker::mod::sched::get_active_task_at(i);
        if (t != nullptr && t->parentPid == parent->pid && t->hasExited && !t->waitedOn) {
            return t;
        }
    }
    // Also check via PID table for recently-exited zombies (may not be in active list)
    // Zombies are removed from active list when they exit but remain in PID table until GC.
    // We need to scan children - use the PID of each active child isn't enough since exited
    // children are removed from active list. However, find_task_by_pid still works for zombies.
    // There's no direct child list, so we can't efficiently scan zombies.
    // For now, the SIGCHLD wakeup + retry approach handles this case.
    return nullptr;
}

static void clear_wait_resume_debug(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->waitResumeRipUserAddr = 0;
    task->waitResumeRipPhysAddr = 0;
    task->waitResumeRspUserAddr = 0;
    task->waitResumeRspPhysAddr = 0;
}

static void capture_wait_resume_debug(ker::mod::sched::task::Task* task, ker::mod::cpu::GPRegs& gpr) {
    if (task == nullptr || task->pagemap == nullptr) {
        clear_wait_resume_debug(task);
        return;
    }

    task->waitResumeRipUserAddr = gpr.rcx;
    uint64_t rip_phys = ker::mod::mm::virt::translate(task->pagemap, gpr.rcx);
    task->waitResumeRipPhysAddr = (rip_phys != ker::mod::mm::virt::PADDR_INVALID) ? rip_phys : 0;

    task->waitResumeRspUserAddr = task->context.frame.rsp;
    if (task->waitResumeRspUserAddr != 0) {
        uint64_t rsp_phys = ker::mod::mm::virt::translate(task->pagemap, task->waitResumeRspUserAddr);
        task->waitResumeRspPhysAddr = (rsp_phys != ker::mod::mm::virt::PADDR_INVALID) ? rsp_phys : 0;
    } else {
        task->waitResumeRspPhysAddr = 0;
    }
}

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    ker::mod::sched::EpochGuard epoch_guard;

    auto* currentTask = ker::mod::sched::get_current_task();
    if (currentTask == nullptr) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Current task is null");
#endif
        return static_cast<uint64_t>(-1);
    }

    // Save current task's context
    currentTask->context.regs = gpr;

    // --- Handle pid == -1 or pid == 0: wait for ANY child ---
    if (pid <= 0) {
        // Check for already-exited children
        auto* exited = find_exited_child(currentTask);
        if (exited != nullptr) {
            if (status != nullptr) {
                *status = exited->exitStatus;
            }
            if (rusage_vaddr != 0) {
                uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, rusage_vaddr);
                fill_rusage(phys, exited);
            }
            currentTask->waitStatusUserAddr = 0;
            currentTask->waitStatusPhysAddr = 0;
            currentTask->waitRusageUserAddr = 0;
            currentTask->waitRusagePhysAddr = 0;
            clear_wait_resume_debug(currentTask);
            exited->waitedOn = true;
            return exited->pid;
        }

        // WNOHANG: return 0 immediately if no exited child
        if (options & 1 /* WNOHANG */) {
            return 0;
        }

        // No exited child yet - block until SIGCHLD wakes us
        currentTask->waitingForPid = WAIT_ANY_CHILD;
        if (status != nullptr) {
            currentTask->waitStatusUserAddr = reinterpret_cast<uint64_t>(status);
            uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, reinterpret_cast<uint64_t>(status));
            currentTask->waitStatusPhysAddr = (phys != ker::mod::mm::virt::PADDR_INVALID) ? phys : 0;
        } else {
            currentTask->waitStatusUserAddr = 0;
            currentTask->waitStatusPhysAddr = 0;
        }
        if (rusage_vaddr != 0) {
            currentTask->waitRusageUserAddr = rusage_vaddr;
            uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, rusage_vaddr);
            currentTask->waitRusagePhysAddr = (phys != ker::mod::mm::virt::PADDR_INVALID) ? phys : 0;
        } else {
            currentTask->waitRusageUserAddr = 0;
            currentTask->waitRusagePhysAddr = 0;
        }
        capture_wait_resume_debug(currentTask, gpr);

        currentTask->wait_channel = "waitpid";
        currentTask->deferredTaskSwitch = true;
        return 0;
    }

    // --- Handle pid > 0: wait for specific child ---

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

    if (target_task->parentPid != currentTask->pid || target_task->waitedOn) {
        return static_cast<uint64_t>(-ECHILD);
    }

    // Check if the target task has already exited
    if (target_task->hasExited) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Target task PID %x has already exited with status %d", pid, target_task->exitStatus);
#endif
        if (status != nullptr) {
            *status = target_task->exitStatus;
        }
        if (rusage_vaddr != 0) {
            uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, rusage_vaddr);
            fill_rusage(phys, target_task);
        }
        currentTask->waitStatusUserAddr = 0;
        currentTask->waitStatusPhysAddr = 0;
        currentTask->waitRusageUserAddr = 0;
        currentTask->waitRusagePhysAddr = 0;
        clear_wait_resume_debug(currentTask);
        // Mark that the parent has retrieved the exit status (zombie can now be reaped)
        target_task->waitedOn = true;
        return pid;  // Return the PID of the exited process
    }

    // Prepare the current task's wait state before publishing ourselves on the child's
    // waiter list. Once the child can see our PID, it may wake us immediately.
    currentTask->waitingForPid = pid;
    if (status != nullptr) {
        currentTask->waitStatusUserAddr = reinterpret_cast<uint64_t>(status);
        uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, reinterpret_cast<uint64_t>(status));
        currentTask->waitStatusPhysAddr = (phys != ker::mod::mm::virt::PADDR_INVALID) ? phys : 0;
    } else {
        currentTask->waitStatusUserAddr = 0;
        currentTask->waitStatusPhysAddr = 0;
    }
    if (rusage_vaddr != 0) {
        currentTask->waitRusageUserAddr = rusage_vaddr;
        uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, rusage_vaddr);
        currentTask->waitRusagePhysAddr = (phys != ker::mod::mm::virt::PADDR_INVALID) ? phys : 0;
    } else {
        currentTask->waitRusageUserAddr = 0;
        currentTask->waitRusagePhysAddr = 0;
    }
    capture_wait_resume_debug(currentTask, gpr);
    currentTask->wait_channel = "waitpid";

    // Serialize with exit: recheck hasExited while holding the waiter-list lock so we
    // cannot miss a child that exits while we're registering as a waiter.
    uint64_t waiter_lock_flags = target_task->exitWaitersLock.lock_irqsave();
    if (target_task->hasExited) {
        target_task->exitWaitersLock.unlock_irqrestore(waiter_lock_flags);
        if (status != nullptr) {
            *status = target_task->exitStatus;
        }
        if (rusage_vaddr != 0) {
            uint64_t phys = ker::mod::mm::virt::translate(currentTask->pagemap, rusage_vaddr);
            fill_rusage(phys, target_task);
        }
        currentTask->waitingForPid = 0;
        currentTask->waitStatusUserAddr = 0;
        currentTask->waitStatusPhysAddr = 0;
        currentTask->waitRusageUserAddr = 0;
        currentTask->waitRusagePhysAddr = 0;
        clear_wait_resume_debug(currentTask);
        target_task->waitedOn = true;
        return pid;
    }

    // Add the current task's PID to the target task's awaitee list
    if (!target_task->awaitee_on_exit.push_back(currentTask->pid)) {
        target_task->exitWaitersLock.unlock_irqrestore(waiter_lock_flags);
        currentTask->waitingForPid = 0;
        currentTask->waitStatusUserAddr = 0;
        currentTask->waitStatusPhysAddr = 0;
        currentTask->waitRusageUserAddr = 0;
        currentTask->waitRusagePhysAddr = 0;
        clear_wait_resume_debug(currentTask);
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Awaitee list full for PID %x", pid);
#endif
        return static_cast<uint64_t>(-1);  // OOM
    }
    currentTask->deferredTaskSwitch = true;
    target_task->exitWaitersLock.unlock_irqrestore(waiter_lock_flags);
#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: Added PID %x to awaitee list of PID %x", currentTask->pid, pid);
#endif
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
