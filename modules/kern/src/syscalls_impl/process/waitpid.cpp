#include "waitpid.hpp"

#include <cerrno>
#include <cstdint>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "platform/asm/cpu.hpp"

namespace ker::syscall::process {

// Fill POSIX struct rusage with the child's timing data (ru_utime and ru_stime only).
// rusage_phys_addr: physical address of the userspace rusage struct, 0 to skip.
static void fill_rusage(uint64_t rusage_phys_addr, ker::mod::sched::task::Task* child) {
    if (rusage_phys_addr == 0 || rusage_phys_addr == ker::mod::mm::virt::PADDR_INVALID) {
        return;
    }
    auto* ru = reinterpret_cast<KernRusage*>(ker::mod::mm::addr::get_virt_pointer(rusage_phys_addr));
    ru->ru_utime_sec = static_cast<int64_t>(child->user_time_us / 1000000ULL);
    ru->ru_utime_usec = static_cast<int64_t>(child->user_time_us % 1000000ULL);
    ru->ru_stime_sec = static_cast<int64_t>(child->system_time_us / 1000000ULL);
    ru->ru_stime_usec = static_cast<int64_t>(child->system_time_us % 1000000ULL);
}

// Sentinel value: waitingForPid == WAIT_ANY_CHILD means "wait for any child"
static constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);

// Scan for an exited child of the given parent task.
// Returns the exited child task pointer, or nullptr if none found.
static auto find_exited_child(ker::mod::sched::task::Task* parent) -> ker::mod::sched::task::Task* {
    // Iterate active task list for children
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* t = ker::mod::sched::get_active_task_at(i);
        if (t != nullptr && t->parent_pid == parent->pid && t->has_exited && !t->waited_on) {
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
    task->wait_resume_rip_user_addr = 0;
    task->wait_resume_rip_phys_addr = 0;
    task->wait_resume_rsp_user_addr = 0;
    task->wait_resume_rsp_phys_addr = 0;
}

static auto current_syscall_user_rsp() -> uint64_t {
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t rsp = 0;
    asm volatile("movq %%gs:0x08, %0" : "=r"(rsp)::"memory");
    return rsp;
}

static void capture_wait_resume_debug(ker::mod::sched::task::Task* task, ker::mod::cpu::GPRegs& gpr) {
    if (task == nullptr || task->pagemap == nullptr) {
        clear_wait_resume_debug(task);
        return;
    }

    task->wait_resume_rip_user_addr = gpr.rcx;
    uint64_t const RIP_PHYS = ker::mod::mm::virt::translate(task->pagemap, gpr.rcx);
    task->wait_resume_rip_phys_addr = (RIP_PHYS != ker::mod::mm::virt::PADDR_INVALID) ? RIP_PHYS : 0;

    task->wait_resume_rsp_user_addr = current_syscall_user_rsp();
    if (task->wait_resume_rsp_user_addr != 0) {
        uint64_t const RSP_PHYS = ker::mod::mm::virt::translate(task->pagemap, task->wait_resume_rsp_user_addr);
        task->wait_resume_rsp_phys_addr = (RSP_PHYS != ker::mod::mm::virt::PADDR_INVALID) ? RSP_PHYS : 0;
    } else {
        task->wait_resume_rsp_phys_addr = 0;
    }
}

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    ker::mod::sched::EpochGuard const EPOCH_GUARD;

    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Current task is null");
#endif
        return static_cast<uint64_t>(-1);
    }

    // Save current task's context
    current_task->context.regs = gpr;

    // --- Handle pid == -1 or pid == 0: wait for ANY child ---
    if (pid <= 0) {
        // Check for already-exited children
        auto* exited = find_exited_child(current_task);
        if (exited != nullptr) {
            if (status != nullptr) {
                *status = exited->exit_status;
            }
            if (rusage_vaddr != 0) {
                uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, rusage_vaddr);
                fill_rusage(PHYS, exited);
            }
            current_task->wait_status_user_addr = 0;
            current_task->wait_status_phys_addr = 0;
            current_task->wait_rusage_user_addr = 0;
            current_task->wait_rusage_phys_addr = 0;
            clear_wait_resume_debug(current_task);
            exited->waited_on = true;
            return exited->pid;
        }

        // WNOHANG: return 0 immediately if no exited child
        if ((options & 1) != 0 /* WNOHANG */) {
            return 0;
        }

        // No exited child yet - block until SIGCHLD wakes us
        current_task->waiting_for_pid = WAIT_ANY_CHILD;
        if (status != nullptr) {
            current_task->wait_status_user_addr = reinterpret_cast<uint64_t>(status);
            uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, reinterpret_cast<uint64_t>(status));
            current_task->wait_status_phys_addr = (PHYS != ker::mod::mm::virt::PADDR_INVALID) ? PHYS : 0;
        } else {
            current_task->wait_status_user_addr = 0;
            current_task->wait_status_phys_addr = 0;
        }
        if (rusage_vaddr != 0) {
            current_task->wait_rusage_user_addr = rusage_vaddr;
            uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, rusage_vaddr);
            current_task->wait_rusage_phys_addr = (PHYS != ker::mod::mm::virt::PADDR_INVALID) ? PHYS : 0;
        } else {
            current_task->wait_rusage_user_addr = 0;
            current_task->wait_rusage_phys_addr = 0;
        }
        capture_wait_resume_debug(current_task, gpr);

        current_task->wait_channel = "waitpid";
        current_task->deferred_task_switch = true;
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

    if (target_task->parent_pid != current_task->pid || target_task->waited_on) {
        return static_cast<uint64_t>(-ECHILD);
    }

    // Check if the target task has already exited
    if (target_task->has_exited) {
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Target task PID %x has already exited with status %d", pid, target_task->exit_status);
#endif
        if (status != nullptr) {
            *status = target_task->exit_status;
        }
        if (rusage_vaddr != 0) {
            uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, rusage_vaddr);
            fill_rusage(PHYS, target_task);
        }
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
        // Mark that the parent has retrieved the exit status (zombie can now be reaped)
        target_task->waited_on = true;
        return pid;  // Return the PID of the exited process
    }

    // Prepare the current task's wait state before publishing ourselves on the child's
    // waiter list. Once the child can see our PID, it may wake us immediately.
    current_task->waiting_for_pid = pid;
    if (status != nullptr) {
        current_task->wait_status_user_addr = reinterpret_cast<uint64_t>(status);
        uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, reinterpret_cast<uint64_t>(status));
        current_task->wait_status_phys_addr = (PHYS != ker::mod::mm::virt::PADDR_INVALID) ? PHYS : 0;
    } else {
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
    }
    if (rusage_vaddr != 0) {
        current_task->wait_rusage_user_addr = rusage_vaddr;
        uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, rusage_vaddr);
        current_task->wait_rusage_phys_addr = (PHYS != ker::mod::mm::virt::PADDR_INVALID) ? PHYS : 0;
    } else {
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
    }
    capture_wait_resume_debug(current_task, gpr);
    current_task->wait_channel = "waitpid";

    // Serialize with exit: recheck hasExited while holding the waiter-list lock so we
    // cannot miss a child that exits while we're registering as a waiter.
    uint64_t const WAITER_LOCK_FLAGS = target_task->exit_waiters_lock.lock_irqsave();
    if (target_task->has_exited) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        if (status != nullptr) {
            *status = target_task->exit_status;
        }
        if (rusage_vaddr != 0) {
            uint64_t const PHYS = ker::mod::mm::virt::translate(current_task->pagemap, rusage_vaddr);
            fill_rusage(PHYS, target_task);
        }
        current_task->waiting_for_pid = 0;
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
        target_task->waited_on = true;
        return pid;
    }

    // Add the current task's PID to the target task's awaitee list
    if (!target_task->awaitee_on_exit.push_back(current_task->pid)) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        current_task->waiting_for_pid = 0;
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
#ifdef WAITPID_DEBUG
        ker::mod::dbg::log("wos_proc_waitpid: Awaitee list full for PID %x", pid);
#endif
        return static_cast<uint64_t>(-1);  // OOM
    }
    current_task->deferred_task_switch = true;
    target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: Added PID %x to awaitee list of PID %x", currentTask->pid, pid);
#endif
#ifdef WAITPID_DEBUG
    ker::mod::dbg::log("wos_proc_waitpid: Setting deferred task switch for PID %x, flag at %p = %d", currentTask->pid,
                       &(currentTask->deferred_task_switch), currentTask->deferred_task_switch);
    ker::mod::dbg::log("wos_proc_waitpid: Task pointer = %p", currentTask);
#endif

    // Return normally - the syscall.asm will check the flag and perform the task switch
    // The return value will be overwritten when the awaited process exits to PID of the exited process
    return 0;
}

}  // namespace ker::syscall::process
