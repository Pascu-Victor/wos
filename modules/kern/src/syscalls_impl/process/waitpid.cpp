#include "waitpid.hpp"

#include <abi/ptrace.hpp>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>

#include "platform/asm/cpu.hpp"

namespace ker::syscall::process {
namespace {
using log = ker::mod::dbg::logger<"waitpid">;
namespace sched_task = ker::mod::sched::task;

void fill_rusage_value(KernRusage& ru, ker::mod::sched::task::Task* child) {
    if (child == nullptr) {
        return;
    }
    uint64_t const USER_TIME_US = sched_task::task_rusage_user_time_us(*child);
    uint64_t const SYSTEM_TIME_US = sched_task::task_rusage_system_time_us(*child);
    ru.ru_utime_sec = static_cast<int64_t>(USER_TIME_US / 1000000ULL);
    ru.ru_utime_usec = static_cast<int64_t>(USER_TIME_US % 1000000ULL);
    ru.ru_stime_sec = static_cast<int64_t>(SYSTEM_TIME_US / 1000000ULL);
    ru.ru_stime_usec = static_cast<int64_t>(SYSTEM_TIME_US % 1000000ULL);
}

auto write_rusage_to_user(ker::mod::sched::task::Task* waiter, uint64_t rusage_vaddr, ker::mod::sched::task::Task* child) -> bool {
    if (rusage_vaddr == 0) {
        return true;
    }
    if (waiter == nullptr || waiter->pagemap == nullptr || child == nullptr) {
        return false;
    }

    KernRusage ru{};
    fill_rusage_value(ru, child);
    return ker::mod::sys::usercopy::copy_value_to_task(*waiter, rusage_vaddr, ru);
}

auto write_status_to_user(ker::mod::sched::task::Task* waiter, int32_t* status, int32_t value) -> bool {
    if (status == nullptr) {
        return true;
    }
    if (waiter == nullptr || waiter->pagemap == nullptr) {
        return false;
    }
    return ker::mod::sys::usercopy::copy_value_to_task(*waiter, reinterpret_cast<uint64_t>(status), value);
}

auto waitpid_outputs_writable(ker::mod::sched::task::Task& task, const int32_t* status, uint64_t rusage_vaddr) -> bool {
    if (status != nullptr && !ker::mod::sys::usercopy::ensure_writable(task, reinterpret_cast<uint64_t>(status), sizeof(*status))) {
        return false;
    }
    if (rusage_vaddr != 0 && !ker::mod::sys::usercopy::ensure_writable(task, rusage_vaddr, sizeof(KernRusage))) {
        return false;
    }
    return true;
}

// Sentinel value: waitingForPid == WAIT_ANY_CHILD means "wait for any child".
// Process-group waits use a high-bit tagged selector; direct PID waits remain
// plain positive pid values so existing specific-child paths keep working.
constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
constexpr uint64_t WAIT_PROCESS_GROUP_SELECTOR = 1ULL << 63U;
constexpr uint64_t WAIT_PROCESS_GROUP_MASK = WAIT_PROCESS_GROUP_SELECTOR - 1U;
constexpr int WOS_WNOHANG = 1;
constexpr int WOS_WSTOPPED = 2;
constexpr int STOP_STATUS_LOW = 0x7f;

void clear_wait_resume_debug(ker::mod::sched::task::Task* task);
void clear_waitpid_publish_pending(ker::mod::sched::task::Task* task);
void clear_waitpid_syscall_state(ker::mod::sched::task::Task* task);

struct ClaimedExitedChild {
    ker::mod::sched::task::Task* task{nullptr};
    bool release_ref{false};
};

struct ParentWaitScan {
    ker::mod::sched::task::Task* parent{nullptr};
    uint64_t selector{WAIT_ANY_CHILD};
};

struct SpecificWaitScan {
    ker::mod::sched::task::Task* waiter{nullptr};
    uint64_t pid{0};
};

auto is_waitable_exit(const ker::mod::sched::task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    // exit_notify_ready is published after descriptor teardown but before later
    // address-space cleanup. A task that has reached the final DEAD state is
    // also waitable: preserving a waitpid block on a dead-listed direct child
    // can strand the parent forever.
    bool const EXIT_READY = task->exit_notify_ready.load(std::memory_order_acquire);
    auto const STATE = task->state.load(std::memory_order_acquire);
    return EXIT_READY || STATE == ker::mod::sched::task::TaskState::DEAD;
}

auto effective_process_group_id(const ker::mod::sched::task::Task& task) -> uint64_t {
    return task.pgid != 0 ? task.pgid : sched_task::process_pid(task);
}

auto wait_selector_for_process_group(uint64_t pgid) -> uint64_t {
    if (pgid == 0 || pgid >= WAIT_PROCESS_GROUP_MASK) {
        return 0;
    }
    return WAIT_PROCESS_GROUP_SELECTOR | (pgid & WAIT_PROCESS_GROUP_MASK);
}

auto wait_selector_is_process_group(uint64_t selector) -> bool {
    return selector != WAIT_ANY_CHILD && (selector & WAIT_PROCESS_GROUP_SELECTOR) != 0;
}

auto wait_selector_process_group(uint64_t selector) -> uint64_t { return selector & WAIT_PROCESS_GROUP_MASK; }

auto wait_selector_matches_child(uint64_t selector, const ker::mod::sched::task::Task* child) -> bool {
    if (child == nullptr) {
        return false;
    }
    if (selector == WAIT_ANY_CHILD) {
        return true;
    }
    if (wait_selector_is_process_group(selector)) {
        return effective_process_group_id(*child) == wait_selector_process_group(selector);
    }
    return child->pid == selector;
}

auto is_unwaited_child(const ker::mod::sched::task::Task* parent, const ker::mod::sched::task::Task* child, uint64_t selector) -> bool {
    return parent != nullptr && child != nullptr && !child->is_thread && child->parent_pid == parent->pid &&
           wait_selector_matches_child(selector, child) && !sched_task::task_waited_on(*child);
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

auto job_stop_status(const ker::mod::sched::task::Task& task) -> int32_t {
    uint32_t signal = task.jobctl_stop_signal != 0 ? task.jobctl_stop_signal : 19;
    return static_cast<int32_t>((signal << 8U) | STOP_STATUS_LOW);
}

auto is_ptrace_wait_target(const ker::mod::sched::task::Task& waiter, const ker::mod::sched::task::Task& target) -> bool {
    return target.ptrace_traced && target.ptrace_tracer_pid == waiter.pid;
}

auto is_unwaited_specific_wait_target(const ker::mod::sched::task::Task* waiter, const ker::mod::sched::task::Task* child, uint64_t pid)
    -> bool {
    if (waiter == nullptr || child == nullptr || child->is_thread || child->pid != pid || sched_task::task_waited_on(*child)) {
        return false;
    }
    return child->parent_pid == waiter->pid || is_ptrace_wait_target(*waiter, *child);
}

auto active_waitable_unwaited_child(ker::mod::sched::task::Task* child, void* raw_ctx) -> bool {
    auto* ctx = static_cast<ParentWaitScan*>(raw_ctx);
    return ctx != nullptr && is_unwaited_child(ctx->parent, child, ctx->selector) && is_waitable_exit(child);
}

auto active_unwaited_child(ker::mod::sched::task::Task* child, void* raw_ctx) -> bool {
    auto* ctx = static_cast<ParentWaitScan*>(raw_ctx);
    return ctx != nullptr && is_unwaited_child(ctx->parent, child, ctx->selector);
}

auto active_waitable_specific_child(ker::mod::sched::task::Task* child, void* raw_ctx) -> bool {
    auto* ctx = static_cast<SpecificWaitScan*>(raw_ctx);
    return ctx != nullptr && is_unwaited_specific_wait_target(ctx->waiter, child, ctx->pid) && is_waitable_exit(child);
}

auto active_stopped_child(ker::mod::sched::task::Task* child, void* raw_ctx) -> bool {
    auto* ctx = static_cast<ParentWaitScan*>(raw_ctx);
    if (ctx == nullptr || ctx->parent == nullptr || child == nullptr) {
        return false;
    }
    return child->parent_pid == ctx->parent->pid && !child->is_thread && wait_selector_matches_child(ctx->selector, child) &&
           !child->has_exited && child->jobctl_stopped.load(std::memory_order_acquire) &&
           child->jobctl_stop_pending.load(std::memory_order_acquire);
}

auto consume_job_stop_if_waitable(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* target, int32_t* status,
                                  int32_t options) -> bool {
    if (waiter == nullptr || target == nullptr || (options & WOS_WSTOPPED) == 0) {
        return false;
    }
    if (target->parent_pid != waiter->pid || target->is_thread || target->has_exited ||
        !target->jobctl_stopped.load(std::memory_order_acquire) || !target->jobctl_stop_pending.load(std::memory_order_acquire)) {
        return false;
    }
    if (!write_status_to_user(waiter, status, job_stop_status(*target))) {
        return false;
    }
    target->jobctl_stop_pending.store(false, std::memory_order_release);
    clear_waitpid_syscall_state(waiter);
    return true;
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
    if (!write_status_to_user(waiter, status, ptrace_stop_status(*target))) {
        return false;
    }
    target->ptrace_stop_pending = false;
    clear_waitpid_syscall_state(waiter);
    return true;
}

// Scan for an exited child of the given parent task. Zombie children are kept
// in the active registry while the parent is alive, but Task::try_acquire()
// intentionally rejects DEAD/EXITING tasks. Use the registry pointer directly:
// a matching unwaited child cannot be reclaimed until this parent marks it
// waited_on.
auto claim_exited_child(ker::mod::sched::task::Task* parent, uint64_t selector) -> ClaimedExitedChild {
    ParentWaitScan ctx{.parent = parent, .selector = selector};
    for (;;) {
        auto* child = ker::mod::sched::find_active_task_lifetime_ref_if(active_waitable_unwaited_child, &ctx);
        if (child == nullptr) {
            break;
        }
        if (sched_task::task_try_mark_waited_on(*child)) {
            return {.task = child, .release_ref = true};
        }
        child->release();
    }

    for (;;) {
        auto* child = ker::mod::sched::find_dead_task_lifetime_ref_if(active_waitable_unwaited_child, &ctx);
        if (child == nullptr) {
            break;
        }
        if (sched_task::task_try_mark_waited_on(*child)) {
            return {.task = child, .release_ref = true};
        }
        child->release();
    }

    return {};
}

auto claim_specific_exited_child(ker::mod::sched::task::Task* waiter, uint64_t pid) -> ClaimedExitedChild {
    if (waiter == nullptr || pid == 0) {
        return {};
    }

    SpecificWaitScan ctx{.waiter = waiter, .pid = pid};
    for (;;) {
        auto* child = ker::mod::sched::find_active_task_lifetime_ref_if(active_waitable_specific_child, &ctx);
        if (child == nullptr) {
            break;
        }
        if (sched_task::task_try_mark_waited_on(*child)) {
            return {.task = child, .release_ref = true};
        }
        child->release();
    }

    for (;;) {
        auto* child = ker::mod::sched::find_dead_task_lifetime_ref_if(active_waitable_specific_child, &ctx);
        if (child == nullptr) {
            break;
        }
        if (sched_task::task_try_mark_waited_on(*child)) {
            return {.task = child, .release_ref = true};
        }
        child->release();
    }

    return {};
}

void release_claimed_child(ClaimedExitedChild claimed) {
    if (claimed.release_ref && claimed.task != nullptr) {
        claimed.task->release();
    }
}

auto consume_claimed_exit(ker::mod::sched::task::Task* waiter, ClaimedExitedChild claimed, int32_t* status, uint64_t rusage_vaddr)
    -> uint64_t {
    auto* exited = claimed.task;
    if (waiter == nullptr || exited == nullptr) {
        release_claimed_child(claimed);
        return static_cast<uint64_t>(-ECHILD);
    }

    clear_waitpid_syscall_state(waiter);
    sched_task::task_accumulate_waited_child_times(*waiter, *exited);
    if (!write_status_to_user(waiter, status, exited->exit_status) || !write_rusage_to_user(waiter, rusage_vaddr, exited)) {
        release_claimed_child(claimed);
        return static_cast<uint64_t>(-EFAULT);
    }
    uint64_t const PID = exited->pid;
    release_claimed_child(claimed);
    return PID;
}

auto consume_stopped_child(ker::mod::sched::task::Task* parent, int32_t* status, int32_t options, uint64_t selector) -> uint64_t {
    if ((options & WOS_WSTOPPED) == 0) {
        return 0;
    }

    ParentWaitScan ctx{.parent = parent, .selector = selector};
    for (;;) {
        auto* child = ker::mod::sched::find_active_task_lifetime_ref_if(active_stopped_child, &ctx);
        if (child == nullptr) {
            break;
        }
        uint64_t const PID = child->pid;
        bool const CONSUMED = consume_job_stop_if_waitable(parent, child, status, options);
        child->release();
        if (CONSUMED) {
            return PID;
        }
    }

    return 0;
}

auto has_unwaited_child(ker::mod::sched::task::Task* parent, uint64_t selector) -> bool {
    ParentWaitScan ctx{.parent = parent, .selector = selector};
    auto* active_child = ker::mod::sched::find_active_task_lifetime_ref_if(active_unwaited_child, &ctx);
    if (active_child != nullptr) {
        active_child->release();
        return true;
    }
    auto* dead_child = ker::mod::sched::find_dead_task_lifetime_ref_if(active_unwaited_child, &ctx);
    if (dead_child != nullptr) {
        dead_child->release();
        return true;
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

void clear_waitpid_syscall_state(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    clear_waitpid_publish_pending(task);
    task->deferred_task_switch = false;
    sched_task::task_clear_waitpid_block_state(*task);
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

    if (!waitpid_outputs_writable(*current_task, status, rusage_vaddr)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    // Save current task's context
    current_task->context.regs = gpr;

    // --- Handle pid == -1, pid == 0, or pid < -1: wait for a child selector ---
    if (pid <= 0) {
        uint64_t const WAIT_SELECTOR = [&]() -> uint64_t {
            if (pid == 0) {
                return wait_selector_for_process_group(effective_process_group_id(*current_task));
            }
            if (pid == -1) {
                return WAIT_ANY_CHILD;
            }
            if (pid == std::numeric_limits<int64_t>::min()) {
                return 0;
            }
            return wait_selector_for_process_group(static_cast<uint64_t>(-pid));
        }();
        if (WAIT_SELECTOR == 0) {
            return static_cast<uint64_t>(-EINVAL);
        }

        if ((options & WOS_WNOHANG) != 0) {
            auto exited = claim_exited_child(current_task, WAIT_SELECTOR);
            if (exited.task != nullptr) {
                return consume_claimed_exit(current_task, exited, status, rusage_vaddr);
            }

            uint64_t const STOPPED_PID = consume_stopped_child(current_task, status, options, WAIT_SELECTOR);
            if (STOPPED_PID != 0) {
                return STOPPED_PID;
            }

            if (!has_unwaited_child(current_task, WAIT_SELECTOR)) {
                return static_cast<uint64_t>(-ECHILD);
            }
            return 0;
        }

        current_task->waitpid_completion_claimed.store(false, std::memory_order_release);
        current_task->waitpid_last_repair_us = 0;
        current_task->waitpid_publish_pending.store(true, std::memory_order_release);
        current_task->waiting_for_pid = WAIT_SELECTOR;
        current_task->wait_options = options;

        // Check for already-exited children
        auto exited = claim_exited_child(current_task, WAIT_SELECTOR);
        if (exited.task != nullptr) {
            return consume_claimed_exit(current_task, exited, status, rusage_vaddr);
        }

        uint64_t stopped_pid = consume_stopped_child(current_task, status, options, WAIT_SELECTOR);
        if (stopped_pid != 0) {
            clear_waitpid_syscall_state(current_task);
            return stopped_pid;
        }

        // No selected child remains waitable. POSIX waitpid selectors must fail
        // with ECHILD here; otherwise shells can block forever after their last
        // pipeline child has already exited and been consumed.
        if (!has_unwaited_child(current_task, WAIT_SELECTOR)) {
            clear_waitpid_syscall_state(current_task);
            return static_cast<uint64_t>(-ECHILD);
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
        exited = claim_exited_child(current_task, WAIT_SELECTOR);
        if (exited.task != nullptr) {
            return consume_claimed_exit(current_task, exited, status, rusage_vaddr);
        }
        stopped_pid = consume_stopped_child(current_task, status, options, WAIT_SELECTOR);
        if (stopped_pid != 0) {
            clear_waitpid_syscall_state(current_task);
            return stopped_pid;
        }
        if (!has_unwaited_child(current_task, WAIT_SELECTOR)) {
            clear_waitpid_syscall_state(current_task);
            return static_cast<uint64_t>(-ECHILD);
        }
        return 0;
    }

    // --- Handle pid > 0: wait for specific child ---

#ifdef WAITPID_DEBUG
    log::debug("PID %x waiting for PID %x", current_task->pid, pid);
#endif

    auto const TARGET_PID = static_cast<uint64_t>(pid);
    auto claimed_target = claim_specific_exited_child(current_task, TARGET_PID);
    if (claimed_target.task != nullptr) {
        return consume_claimed_exit(current_task, claimed_target, status, rusage_vaddr);
    }

    // Use zombie-visible lookup. A matching unwaited child cannot be reclaimed
    // while its parent is alive and has not consumed the exit status.
    auto* target_task = ker::mod::sched::find_task_by_pid(TARGET_PID);
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
        if (!write_status_to_user(current_task, status, target_task->exit_status) ||
            !write_rusage_to_user(current_task, rusage_vaddr, target_task)) {
            return static_cast<uint64_t>(-EFAULT);
        }
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
        return TARGET_PID;  // Return the PID of the exited process
    }

    if (consume_ptrace_stop_if_waitable(current_task, target_task, status, options)) {
        return TARGET_PID;
    }
    if (consume_job_stop_if_waitable(current_task, target_task, status, options)) {
        return TARGET_PID;
    }

    if ((options & WOS_WNOHANG) != 0) {
        return 0;
    }

    // Prepare the current task's wait state before publishing ourselves on the child's
    // waiter list. Once the child can see our PID, it may wake us immediately.
    current_task->waitpid_completion_claimed.store(false, std::memory_order_release);
    current_task->waitpid_last_repair_us = 0;
    current_task->waitpid_publish_pending.store(true, std::memory_order_release);
    current_task->waiting_for_pid = TARGET_PID;
    current_task->wait_options = options;
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
                current_task->wait_options = 0;
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
            if (!write_status_to_user(current_task, status, target_task->exit_status) ||
                !write_rusage_to_user(current_task, rusage_vaddr, target_task)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            current_task->waiting_for_pid = 0;
            current_task->wait_options = 0;
            current_task->wait_status_user_addr = 0;
            current_task->wait_status_phys_addr = 0;
            current_task->wait_rusage_user_addr = 0;
            current_task->wait_rusage_phys_addr = 0;
            clear_wait_resume_debug(current_task);
            return TARGET_PID;
        }
        if (consume_job_stop_if_waitable(current_task, target_task, status, options)) {
            clear_waitpid_publish_pending(current_task);
            current_task->deferred_task_switch = false;
            current_task->waiting_for_pid = 0;
            current_task->wait_options = 0;
            return TARGET_PID;
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
            current_task->wait_options = 0;
            current_task->wait_status_user_addr = 0;
            current_task->wait_status_phys_addr = 0;
            current_task->wait_rusage_user_addr = 0;
            current_task->wait_rusage_phys_addr = 0;
            clear_wait_resume_debug(current_task);
            return static_cast<uint64_t>(-ECHILD);
        }
        sched_task::task_accumulate_waited_child_times(*current_task, *target_task);
        if (!write_status_to_user(current_task, status, target_task->exit_status) ||
            !write_rusage_to_user(current_task, rusage_vaddr, target_task)) {
            return static_cast<uint64_t>(-EFAULT);
        }
        current_task->waiting_for_pid = 0;
        current_task->wait_options = 0;
        current_task->wait_status_user_addr = 0;
        current_task->wait_status_phys_addr = 0;
        current_task->wait_rusage_user_addr = 0;
        current_task->wait_rusage_phys_addr = 0;
        clear_wait_resume_debug(current_task);
        return TARGET_PID;
    }
    if (consume_job_stop_if_waitable(current_task, target_task, status, options)) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        clear_waitpid_publish_pending(current_task);
        current_task->waiting_for_pid = 0;
        current_task->wait_options = 0;
        return TARGET_PID;
    }

    // Add the current task's PID to the target task's awaitee list
    if (!target_task->awaitee_on_exit.push_back(current_task->pid)) {
        target_task->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
        clear_waitpid_publish_pending(current_task);
        current_task->waiting_for_pid = 0;
        current_task->wait_options = 0;
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
