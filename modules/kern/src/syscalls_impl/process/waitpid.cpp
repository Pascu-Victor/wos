#include "waitpid.hpp"

#include <abi/ptrace.hpp>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "platform/asm/cpu.hpp"

namespace ker::syscall::process {
namespace {
using log = ker::mod::dbg::logger<"waitpid">;
namespace sched_task = ker::mod::sched::task;

// Fill POSIX struct rusage with the child's timing data (ru_utime and ru_stime only).
// rusage_phys_addr: physical address of the userspace rusage struct, 0 to skip.
void fill_rusage(uint64_t rusage_phys_addr, ker::mod::sched::task::Task* child) {
    if (rusage_phys_addr == 0 || rusage_phys_addr == ker::mod::mm::virt::PADDR_INVALID) {
        return;
    }
    auto* ru = reinterpret_cast<KernRusage*>(ker::mod::mm::addr::get_virt_pointer(rusage_phys_addr));
    uint64_t const USER_TIME_US = sched_task::task_rusage_user_time_us(*child);
    uint64_t const SYSTEM_TIME_US = sched_task::task_rusage_system_time_us(*child);
    ru->ru_utime_sec = static_cast<int64_t>(USER_TIME_US / 1000000ULL);
    ru->ru_utime_usec = static_cast<int64_t>(USER_TIME_US % 1000000ULL);
    ru->ru_stime_sec = static_cast<int64_t>(SYSTEM_TIME_US / 1000000ULL);
    ru->ru_stime_usec = static_cast<int64_t>(SYSTEM_TIME_US % 1000000ULL);
}

// Sentinel value: waitingForPid == WAIT_ANY_CHILD means "wait for any child"
constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
constexpr int WOS_WNOHANG = 1;
constexpr int WOS_WSTOPPED = 2;
constexpr int STOP_STATUS_LOW = 0x7f;

void clear_wait_resume_debug(ker::mod::sched::task::Task* task);

auto is_waitable_exit(const ker::mod::sched::task::Task* task) -> bool {
    return task != nullptr && task->exit_notify_ready.load(std::memory_order_acquire) && task->has_exited;
}

auto is_unwaited_child(const ker::mod::sched::task::Task* parent, const ker::mod::sched::task::Task* child) -> bool {
    return parent != nullptr && child != nullptr && !child->is_thread && child->parent_pid == parent->pid &&
           !sched_task::task_waited_on(*child);
}

auto ptrace_stop_status(const ker::mod::sched::task::Task& task) -> int32_t {
    uint32_t signal = task.ptrace_stop_signal != 0 ? task.ptrace_stop_signal : 5;
    if ((task.ptrace_stop_reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER ||
         task.ptrace_stop_reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) &&
        (task.ptrace_options & 0x00000001U) != 0) {
        signal |= 0x80U;
    }
    return static_cast<int32_t>((signal << 8U) | STOP_STATUS_LOW);
}

auto is_ptrace_wait_target(const ker::mod::sched::task::Task& waiter, const ker::mod::sched::task::Task& target) -> bool {
    return target.ptrace_traced && target.ptrace_tracer_pid == waiter.pid;
}

auto consume_ptrace_stop_if_waitable(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* target, int32_t* status,
                                     int32_t options) -> bool {
    if (waiter == nullptr || target == nullptr || !is_ptrace_wait_target(*waiter, *target)) {
        return false;
    }
    if ((options & WOS_WSTOPPED) == 0 && !target->ptrace_stop_pending) {
        return false;
    }
    if (!target->ptrace_stopped || !target->ptrace_stop_pending) {
        return false;
    }
    if (status != nullptr) {
        *status = ptrace_stop_status(*target);
    }
    target->ptrace_stop_pending = false;
    waiter->waiting_for_pid = 0;
    waiter->wait_status_user_addr = 0;
    waiter->wait_status_phys_addr = 0;
    waiter->wait_rusage_user_addr = 0;
    waiter->wait_rusage_phys_addr = 0;
    clear_wait_resume_debug(waiter);
    return true;
}

// Scan for an exited child of the given parent task. Zombie children are kept
// in the active registry while the parent is alive, but Task::try_acquire()
// intentionally rejects DEAD/EXITING tasks. Use the registry pointer directly:
// a matching unwaited child cannot be reclaimed until this parent marks it
// waited_on.
auto claim_exited_child(ker::mod::sched::task::Task* parent) -> ker::mod::sched::task::Task* {
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* child = ker::mod::sched::get_active_task_at(i);
        if (is_unwaited_child(parent, child) && is_waitable_exit(child) && sched_task::task_try_mark_waited_on(*child)) {
            return child;
        }
    }

    return nullptr;
}

auto has_unwaited_child(ker::mod::sched::task::Task* parent) -> bool {
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* child = ker::mod::sched::get_active_task_at(i);
        if (is_unwaited_child(parent, child)) {
            return true;
        }
    }
    return false;
}

void clear_wait_resume_debug(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->wait_resume_rip_user_addr = 0;
    task->wait_resume_rip_phys_addr = 0;
    task->wait_resume_rsp_user_addr = 0;
    task->wait_resume_rsp_phys_addr = 0;
}

void clear_waitpid_publish_pending(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        task->waitpid_publish_pending.store(false, std::memory_order_release);
    }
}

auto current_syscall_user_rsp() -> uint64_t {
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t rsp = 0;
    asm volatile("movq %%gs:0x08, %0" : "=r"(rsp)::"memory");
    return rsp;
}

void capture_wait_resume_debug(ker::mod::sched::task::Task* task, ker::mod::cpu::GPRegs& gpr) {
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

}  // namespace

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    ker::mod::sched::EpochGuard const EPOCH_GUARD;

    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
#ifdef WAITPID_DEBUG
        log::debug("current task is null");
#endif
        return static_cast<uint64_t>(-1);
    }

    // Save current task's context
    current_task->context.regs = gpr;

    // --- Handle pid == -1 or pid == 0: wait for ANY child ---
    if (pid <= 0) {
        current_task->waitpid_publish_pending.store(true, std::memory_order_release);
        current_task->waiting_for_pid = WAIT_ANY_CHILD;

        // Check for already-exited children
        auto* exited = claim_exited_child(current_task);
        if (exited != nullptr) {
            clear_waitpid_publish_pending(current_task);
            current_task->waiting_for_pid = 0;
            sched_task::task_accumulate_waited_child_times(*current_task, *exited);
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
            return exited->pid;
        }

        // No direct child remains waitable.  POSIX waitpid(-1/0) must fail
        // with ECHILD here; otherwise shells can block forever after their
        // last pipeline child has already exited and been consumed.
        if (!has_unwaited_child(current_task)) {
            clear_waitpid_publish_pending(current_task);
            current_task->waiting_for_pid = 0;
            return static_cast<uint64_t>(-ECHILD);
        }

        // WNOHANG: return 0 immediately if no exited child
        if ((options & WOS_WNOHANG) != 0) {
            clear_waitpid_publish_pending(current_task);
            current_task->waiting_for_pid = 0;
            return 0;
        }

        // No exited child yet - block until SIGCHLD wakes us
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

        current_task->set_wait_channel("waitpid", sched_task::WaitChannelKind::WAITPID);
        current_task->deferred_task_switch = true;
        exited = claim_exited_child(current_task);
        if (exited != nullptr) {
            clear_waitpid_publish_pending(current_task);
            current_task->deferred_task_switch = false;
            sched_task::task_accumulate_waited_child_times(*current_task, *exited);
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
            current_task->waiting_for_pid = 0;
            return exited->pid;
        }
        return 0;
    }

    // --- Handle pid > 0: wait for specific child ---

#ifdef WAITPID_DEBUG
    log::debug("PID %x waiting for PID %x", current_task->pid, pid);
#endif

    // Use zombie-visible lookup. A matching unwaited child cannot be reclaimed
    // while its parent is alive and has not consumed the exit status.
    auto* target_task = ker::mod::sched::find_task_by_pid(pid);
    if (target_task == nullptr) {
#ifdef WAITPID_DEBUG
        log::debug("target task PID %x not found", pid);
#endif
        return static_cast<uint64_t>(-ECHILD);
    }

    bool const TRACE_WAIT = is_ptrace_wait_target(*current_task, *target_task);
    if ((!TRACE_WAIT && target_task->parent_pid != current_task->pid) || sched_task::task_waited_on(*target_task)) {
        return static_cast<uint64_t>(-ECHILD);
    }

    // Check if the target task has already exited
    if (is_waitable_exit(target_task)) {
#ifdef WAITPID_DEBUG
        log::debug("target task PID %x has already exited with status %d", pid, target_task->exit_status);
#endif
        if (!sched_task::task_try_mark_waited_on(*target_task)) {
            return static_cast<uint64_t>(-ECHILD);
        }
        sched_task::task_accumulate_waited_child_times(*current_task, *target_task);
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
        return pid;  // Return the PID of the exited process
    }

    if (consume_ptrace_stop_if_waitable(current_task, target_task, status, options)) {
        return pid;
    }

    if ((options & WOS_WNOHANG) != 0) {
        return 0;
    }

    // Prepare the current task's wait state before publishing ourselves on the child's
    // waiter list. Once the child can see our PID, it may wake us immediately.
    current_task->waitpid_publish_pending.store(true, std::memory_order_release);
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
    current_task->set_wait_channel("waitpid", sched_task::WaitChannelKind::WAITPID);

    if (TRACE_WAIT) {
        current_task->deferred_task_switch = true;
        if (is_waitable_exit(target_task)) {
            if (!sched_task::task_try_mark_waited_on(*target_task)) {
                clear_waitpid_publish_pending(current_task);
                current_task->deferred_task_switch = false;
                current_task->waiting_for_pid = 0;
                current_task->wait_status_user_addr = 0;
                current_task->wait_status_phys_addr = 0;
                current_task->wait_rusage_user_addr = 0;
                current_task->wait_rusage_phys_addr = 0;
                clear_wait_resume_debug(current_task);
                return static_cast<uint64_t>(-ECHILD);
            }
            clear_waitpid_publish_pending(current_task);
            current_task->deferred_task_switch = false;
            sched_task::task_accumulate_waited_child_times(*current_task, *target_task);
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
            return pid;
        }
        return 0;
    }

    // Serialize with exit: recheck hasExited while holding the waiter-list lock so we
    // cannot miss a child that exits while we're registering as a waiter.
    uint64_t const WAITER_LOCK_FLAGS = target_task->exit_waiters_lock.lock_irqsave();
    if (is_waitable_exit(target_task)) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        clear_waitpid_publish_pending(current_task);
        if (!sched_task::task_try_mark_waited_on(*target_task)) {
            current_task->waiting_for_pid = 0;
            current_task->wait_status_user_addr = 0;
            current_task->wait_status_phys_addr = 0;
            current_task->wait_rusage_user_addr = 0;
            current_task->wait_rusage_phys_addr = 0;
            clear_wait_resume_debug(current_task);
            return static_cast<uint64_t>(-ECHILD);
        }
        sched_task::task_accumulate_waited_child_times(*current_task, *target_task);
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
        return pid;
    }

    // Add the current task's PID to the target task's awaitee list
    if (!target_task->awaitee_on_exit.push_back(current_task->pid)) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        clear_waitpid_publish_pending(current_task);
        current_task->waiting_for_pid = 0;
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
#ifdef WAITPID_DEBUG
        log::debug("awaitee list full for PID %x", pid);
#endif
        return static_cast<uint64_t>(-1);  // OOM
    }
    current_task->deferred_task_switch = true;
    target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
#ifdef WAITPID_DEBUG
    log::debug("added PID %x to awaitee list of PID %x", current_task->pid, pid);
#endif
#ifdef WAITPID_DEBUG
    log::debug("setting deferred task switch for PID %x, flag at %p = %d", current_task->pid, &(current_task->deferred_task_switch),
               current_task->deferred_task_switch);
    log::debug("task pointer = %p", current_task);
#endif

    // Return normally - the syscall.asm will check the flag and perform the task switch
    // The return value will be overwritten when the awaited process exits to PID of the exited process
    return 0;
}

}  // namespace ker::syscall::process
