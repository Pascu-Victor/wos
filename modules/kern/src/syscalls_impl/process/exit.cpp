#include "exit.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/debug/ptrace.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/signal.hpp>
#include <platform/sys/usercopy.hpp>
#include <util/hcf.hpp>
#include <vfs/vfs.hpp>

#include "platform/sched/task.hpp"
#include "platform/smt/smt.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "syscalls_impl/log/sys_log.hpp"
#include "syscalls_impl/multiproc/threadControl.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "waitpid.hpp"

namespace ker::syscall::process {

namespace {
using log = ker::mod::dbg::logger<"pexit">;
namespace sched_task = ker::mod::sched::task;

constexpr auto WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
constexpr uint64_t WAIT_PROCESS_GROUP_SELECTOR = 1ULL << 63U;
constexpr uint64_t WAIT_PROCESS_GROUP_MASK = WAIT_PROCESS_GROUP_SELECTOR - 1U;
constexpr uint64_t SIGCHLD_MASK = 1ULL << (17 - 1);
constexpr uint64_t SIGKILL_MASK = 1ULL << (9 - 1);
constexpr size_t EXIT_WAITER_NOTIFY_BATCH = 16;

using ExitWaiterBatch = std::array<uint64_t, EXIT_WAITER_NOTIFY_BATCH>;

auto clamp_perf_aux(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

void record_local_proc_event(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, ker::mod::perf::WkiPerfPhase phase,
                             uint32_t correlation, int32_t status, uint32_t aux, uint64_t callsite) {
    if (task == nullptr) {
        return;
    }

    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), task->pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), phase, 0, 0, correlation, status,
                                     aux, callsite);
}

// Fill ru_utime and ru_stime in the waiter's rusage struct from the exiting child's timing data.
auto fill_rusage_for_waiter(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) -> bool {
    if (waiter->wait_rusage_user_addr == 0 || waiter->pagemap == nullptr) {
        waiter->wait_rusage_user_addr = 0;
        waiter->wait_rusage_phys_addr = 0;
        return true;
    }

    KernRusage ru{};
    uint64_t const USER_TIME_US = ker::mod::sched::task::task_rusage_user_time_us(*child);
    uint64_t const SYSTEM_TIME_US = ker::mod::sched::task::task_rusage_system_time_us(*child);
    ru.ru_utime_sec = static_cast<int64_t>(USER_TIME_US / 1000000ULL);
    ru.ru_utime_usec = static_cast<int64_t>(USER_TIME_US % 1000000ULL);
    ru.ru_stime_sec = static_cast<int64_t>(SYSTEM_TIME_US / 1000000ULL);
    ru.ru_stime_usec = static_cast<int64_t>(SYSTEM_TIME_US % 1000000ULL);
    bool const OK = ker::mod::sys::usercopy::copy_value_to_task(*waiter, waiter->wait_rusage_user_addr, ru);
    waiter->wait_rusage_user_addr = 0;
    waiter->wait_rusage_phys_addr = 0;
    return OK;
}

auto write_wait_status_for_waiter(ker::mod::sched::task::Task* waiter, int32_t status) -> bool {
    if (waiter->wait_status_user_addr == 0 || waiter->pagemap == nullptr) {
        waiter->wait_status_user_addr = 0;
        waiter->wait_status_phys_addr = 0;
        return true;
    }
    bool const OK = ker::mod::sys::usercopy::copy_value_to_task(*waiter, waiter->wait_status_user_addr, status);
    waiter->wait_status_user_addr = 0;
    waiter->wait_status_phys_addr = 0;
    return OK;
}

void validate_waiter_resume_for_exit(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child, const char* path) {
    if (waiter == nullptr || waiter->pagemap == nullptr) {
        return;
    }

    if (waiter->wait_resume_rip_user_addr != 0) {
        uint64_t const RIP_PHYS = ker::mod::mm::virt::translate(waiter->pagemap, waiter->wait_resume_rip_user_addr);
        if (RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID || RIP_PHYS == 0 ||
            (waiter->wait_resume_rip_phys_addr != 0 && waiter->wait_resume_rip_phys_addr != RIP_PHYS)) {
            log::warn(
                "waitpid-resume drift: waiter=%lu child=%lu path=%s rip_va=0x%llx old_phys=0x%llx new_phys=0x%llx rsp_va=0x%llx pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                static_cast<unsigned long long>(waiter->wait_resume_rip_user_addr),
                static_cast<unsigned long long>(waiter->wait_resume_rip_phys_addr),
                static_cast<unsigned long long>((RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RIP_PHYS),
                static_cast<unsigned long long>(waiter->wait_resume_rsp_user_addr), static_cast<void*>(waiter->pagemap));
        }
        waiter->wait_resume_rip_phys_addr = (RIP_PHYS != ker::mod::mm::virt::PADDR_INVALID) ? RIP_PHYS : 0;
    }

    if (waiter->wait_resume_rsp_user_addr != 0) {
        uint64_t const RSP_PHYS = ker::mod::mm::virt::translate(waiter->pagemap, waiter->wait_resume_rsp_user_addr);
        if (RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID || RSP_PHYS == 0) {
            log::warn(
                "waitpid-stack unmapped: waiter=%lu child=%lu path=%s rsp_va=0x%llx old_phys=0x%llx new_phys=0x%llx rip_va=0x%llx "
                "pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                static_cast<unsigned long long>(waiter->wait_resume_rsp_user_addr),
                static_cast<unsigned long long>(waiter->wait_resume_rsp_phys_addr),
                static_cast<unsigned long long>((RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RSP_PHYS),
                static_cast<unsigned long long>(waiter->wait_resume_rip_user_addr), static_cast<void*>(waiter->pagemap));
        }
        waiter->wait_resume_rsp_phys_addr = (RSP_PHYS != ker::mod::mm::virt::PADDR_INVALID) ? RSP_PHYS : 0;
    }
}

void reschedule_on_task_cpu(ker::mod::sched::task::Task* task) {
    uint64_t cpu = task->cpu;
    if (cpu >= ker::mod::smt::get_core_count()) {
        cpu = ker::mod::sched::get_least_loaded_cpu();
    }
    ker::mod::sched::reschedule_task_for_cpu(cpu, task);
}

auto effective_process_group_id(const ker::mod::sched::task::Task& task) -> uint64_t {
    return task.pgid != 0 ? task.pgid : sched_task::process_pid(task);
}

auto wait_selector_is_process_group(uint64_t selector) -> bool {
    return selector != WAIT_ANY_CHILD && (selector & WAIT_PROCESS_GROUP_SELECTOR) != 0;
}

auto wait_selector_is_specific_pid(uint64_t selector) -> bool {
    return selector != 0 && selector != WAIT_ANY_CHILD && !wait_selector_is_process_group(selector);
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

auto waiter_matches_child(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) -> bool {
    if (waiter == nullptr || child == nullptr || child->is_thread || waiter->waiting_for_pid == 0) {
        return false;
    }
    if (child->parent_pid != waiter->pid && (!child->ptrace_traced || child->ptrace_tracer_pid != waiter->pid)) {
        return false;
    }
    return wait_selector_matches_child(waiter->waiting_for_pid, child);
}

auto waiter_is_blocked_on_different_waitpid_child(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) -> bool {
    return waiter != nullptr && child != nullptr && waiter->waiting_for_pid != 0 && waiter->waiting_for_pid != WAIT_ANY_CHILD &&
           !wait_selector_matches_child(waiter->waiting_for_pid, child);
}

auto waiter_context_can_be_completed(ker::mod::sched::task::Task* waiter) -> bool {
    if (waiter == nullptr || waiter->deferred_task_switch) {
        return false;
    }

    // Only a parked waitpid waiter has a stable saved syscall context that the
    // exit path can complete. While the parent is still in the deferred-switch
    // window, the scheduler owns the final recheck and result publication.
    return waiter->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING &&
           waiter->wait_channel_is(ker::mod::sched::task::WaitChannelKind::WAITPID) &&
           !waiter->waitpid_publish_pending.load(std::memory_order_acquire);
}

auto waiter_has_stranded_specific_wait(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child) -> bool {
    return waiter != nullptr && child != nullptr && wait_selector_is_specific_pid(waiter->waiting_for_pid) &&
           waiter->waiting_for_pid == child->pid && waiter->wait_channel_is(ker::mod::sched::task::WaitChannelKind::WAITPID);
}

auto task_can_be_interrupted_by_signal(ker::mod::sched::task::Task* task) -> bool {
    return task != nullptr && (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING || task->deferred_task_switch ||
                               task->is_voluntary_blocked());
}

auto drain_exit_waiters_for_notify(ker::mod::sched::task::Task* exiting, ExitWaiterBatch& waiting_pids) -> size_t {
    if (exiting == nullptr) {
        return 0;
    }

    size_t count = 0;
    uint64_t const WAITER_LOCK_FLAGS = exiting->exit_waiters_lock.lock_irqsave();
    while (count < waiting_pids.size() && !exiting->awaitee_on_exit.empty()) {
        size_t const INDEX = exiting->awaitee_on_exit.size() - 1;
        waiting_pids.at(count++) = exiting->awaitee_on_exit.at(INDEX);
        exiting->awaitee_on_exit.pop_back();
    }
    exiting->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
    return count;
}

auto complete_exit_wait(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* child, const char* path) -> bool {
    if (waiter == nullptr || child == nullptr) {
        return false;
    }
    if (!sched_task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (!waiter_matches_child(waiter, child) || !waiter_context_can_be_completed(waiter)) {
        sched_task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    if (!sched_task::task_try_mark_waited_on(*child)) {
        sched_task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    sched_task::task_accumulate_waited_child_times(*waiter, *child);
    waiter->context.regs.rax = child->pid;
    validate_waiter_resume_for_exit(waiter, child, path);
    bool const OUTPUT_OK = write_wait_status_for_waiter(waiter, child->exit_status) && fill_rusage_for_waiter(waiter, child);
    if (!OUTPUT_OK) {
        waiter->context.regs.rax = static_cast<uint64_t>(-EFAULT);
    }
    waiter->waitpid_publish_pending.store(false, std::memory_order_release);
    waiter->deferred_task_switch = false;
    waiter->set_voluntary_blocked(false);
    waiter->wants_block = false;
    waiter->wake_at_us = 0;
    sched_task::task_clear_waitpid_block_state(*waiter);
    return true;
}

void notify_parent_after_exit_ready(ker::mod::sched::task::Task* child) {
    if (child == nullptr || child->is_thread || child->parent_pid == 0) {
        return;
    }

    auto* parent = ker::mod::sched::find_task_by_pid_safe(child->parent_pid);
    if (parent == nullptr) {
        return;
    }

    parent->signal_add_pending_mask(SIGCHLD_MASK);

    bool wake_parent = false;
    bool signal_wake_parent = false;
    if (waiter_is_blocked_on_different_waitpid_child(parent, child)) {
        wake_parent = false;
    } else if (sched_task::task_waited_on(*child)) {
        wake_parent = waiter_has_stranded_specific_wait(parent, child);
    } else {
        if (waiter_matches_child(parent, child) && waiter_context_can_be_completed(parent)) {
            char const* path = "exit-specific-parent";
            if (parent->waiting_for_pid == WAIT_ANY_CHILD) {
                path = "exit-any";
            } else if (wait_selector_is_process_group(parent->waiting_for_pid)) {
                path = "exit-pgrp-parent";
            }
            wake_parent = complete_exit_wait(parent, child, path);
        } else if (waiter_matches_child(parent, child) && (parent->deferred_task_switch || parent->is_voluntary_blocked() ||
                                                           parent->wait_channel_is(ker::mod::sched::task::WaitChannelKind::WAITPID))) {
            // The deferred switch path will re-check waitability after it saves
            // the parent's syscall context.
            wake_parent = true;
        }
    }

    if (!wake_parent && task_can_be_interrupted_by_signal(parent) && parent->has_interrupting_signal_pending()) {
        signal_wake_parent = true;
    }

    if (wake_parent) {
        reschedule_on_task_cpu(parent);
    } else if (signal_wake_parent) {
        ker::mod::sched::wake_task_for_signal(parent);
    }
    parent->release();
}

void notify_tracer_after_exit_ready(ker::mod::sched::task::Task* child) {
    if (child == nullptr || child->is_thread || !child->ptrace_traced || child->ptrace_tracer_pid == 0) {
        return;
    }

    auto* tracer = ker::mod::sched::find_task_by_pid_safe(child->ptrace_tracer_pid);
    if (tracer == nullptr) {
        return;
    }

    bool wake_tracer = false;
    if (waiter_matches_child(tracer, child)) {
        bool const TRACER_IS_PARENT = child->parent_pid == tracer->pid;
        if (TRACER_IS_PARENT && !sched_task::task_waited_on(*child) && waiter_context_can_be_completed(tracer)) {
            wake_tracer = complete_exit_wait(tracer, child, "exit-ptrace");
        } else {
            wake_tracer = true;
        }
    }

    if (wake_tracer) {
        reschedule_on_task_cpu(tracer);
    }
    tracer->release();
}

void release_exiting_user_address_space(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::PROCESS || task->is_thread || task->pagemap == nullptr) {
        return;
    }
    if (ker::mod::sched::task_has_live_pagemap_sibling(task)) {
        return;
    }

    auto* pagemap = task->pagemap;

    // The task has already transitioned out of ACTIVE, so it will not return to
    // userspace. Switch this CPU away from the exiting address space, then make
    // the Task stop publishing the pagemap before destroying the user half.
    // Waitpid-visible zombie state stays in the Task; scheduler GC still reclaims
    // the thread object, kernel stack, and scratch area after the epoch guard.
    ker::mod::mm::virt::switch_to_kernel_pagemap();
    task->pagemap = nullptr;
    ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(pagemap);
    ker::mod::mm::virt::destroy_user_space(pagemap, task->pid, task->name, "process-exit");
    ker::mod::mm::virt::release_pagemap(pagemap);
}

void cleanup_signal_handlers_for_exit(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    task->signal_pending_store(0, std::memory_order_relaxed);
    task->signal_mask_store(0, std::memory_order_relaxed);
    task->sigsuspend_saved_mask = 0;
    task->sigaltstack_sp = 0;
    task->sigaltstack_size = 0;
    task->sigaltstack_flags = ker::mod::sys::signal::WOS_SS_DISABLE;
    task->sigsuspend_active = false;
    task->in_signal_handler = false;
    task->do_sigreturn = false;
    for (auto& handler : task->sig_handlers) {
        handler = {};
    }
}

auto task_alive_for_group_exit_request(ker::mod::sched::task::Task* task) -> bool {
    return task != nullptr && task->type == ker::mod::sched::task::TaskType::PROCESS &&
           task->state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE && !task->has_exited;
}

void publish_process_exit_request(ker::mod::sched::task::Task* task, int status, int wait_status) {
    if (!task_alive_for_group_exit_request(task)) {
        return;
    }

    task->requested_process_exit_status.store(status, std::memory_order_relaxed);
    task->requested_process_exit_wait_status.store(wait_status, std::memory_order_relaxed);
    task->process_exit_requested.store(true, std::memory_order_release);
    task->signal_add_pending_mask(SIGKILL_MASK);
    ker::mod::sched::wake_task_for_signal(task);
}

void request_thread_group_exit(ker::mod::sched::task::Task* initiator, int status, int wait_status) {
    if (initiator == nullptr) {
        return;
    }

    uint64_t const PROCESS_PID = sched_task::process_pid(*initiator);
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }

        if (task != initiator && sched_task::same_thread_group(*task, PROCESS_PID)) {
            publish_process_exit_request(task, status, wait_status);
        }
        task->release();
    }
}

}  // namespace

namespace {

[[noreturn]] void wos_proc_exit_with_wait_status(int status, int wait_status) {
    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
        log::error("process exit without current task: status=%d wait_status=%d", status, wait_status);
        hcf();
    }
    request_thread_group_exit(current_task, status, wait_status);
    if (current_task->is_thread) {
        ker::syscall::multiproc::wos_thread_exit_current();
    }

    uint32_t const EXIT_CORR = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const EXIT_STARTED_US = ker::mod::time::get_us();
    record_local_proc_event(current_task, ker::mod::perf::WkiPerfLocalProcOp::EXIT, ker::mod::perf::WkiPerfPhase::BEGIN, EXIT_CORR, status,
                            0, WOS_PERF_CALLSITE());

    // Mark this task as committed to exit, but keep it schedulable until after
    // descriptor teardown.  vfs_close() can synchronously commit dirty file
    // state and may park at scheduler-safe wait points; an EXITING task is not
    // requeued by normal wake paths, so moving to EXITING before that cleanup
    // can strand the child and preserve a parent's waitpid() forever.
    //
    // Do not set has_exited here: other subsystems use it as a zombie/dead
    // predicate.  exit_in_progress only suppresses re-entrant exit attempts
    // while cleanup is still allowed to block.
    if (current_task->exit_in_progress) {
        for (;;) {
            asm volatile("hlt");
        }
    }
    current_task->exit_in_progress = true;
    ker::syscall::log::sys_log_cleanup_for_task(current_task);

#ifdef EXIT_DEBUG
    log::debug("task PID %x exiting with status %d", current_task->pid, status);
#endif

    current_task->exit_status = wait_status;
#ifdef EXIT_DEBUG
    log::trace("wos_proc_exit: pid=%lu name=%s status=%d thread=%d owner=%lu pagemap=%p", current_task->pid,
               current_task->name != nullptr ? current_task->name : "?", status, current_task->is_thread, current_task->owner_pid,
               static_cast<void*>(current_task->pagemap));
#endif
    ker::mod::debug::ptrace::detach_tracees_for_tracer_exit(current_task->pid);

    // Reparent all children of this process to init (PID 1), so init can reap them.
    // Threads do not own children directly - skip reparenting for thread exits.
    if (!current_task->is_thread) {
        uint32_t const COUNT = ker::mod::sched::get_active_task_count();
        for (uint32_t i = 0; i < COUNT; i++) {
            auto* child = ker::mod::sched::get_active_task_at(i);
            if (child != nullptr && child->parent_pid == current_task->pid && child != current_task) {
                child->parent_pid = 1;  // Reparent to init
            }
        }
    }

    // Close all open file descriptors and free ELF buffer before waking waiters.
    // This ensures files written by the exiting process are fully committed to
    // the VFS before waitpid returns to the parent.
    if (!current_task->is_thread) {
        // vfs_close() removes from fd_table and may free radix-tree nodes, so
        // close one descriptor per traversal instead of mutating during for_each().
        while (!current_task->fd_table.empty()) {
            uint64_t fd = 0;
            bool found_fd = false;
            current_task->fd_table.for_each([&](uint64_t key, void* /*val*/) -> void {
                if (!found_fd) {
                    fd = key;
                    found_fd = true;
                }
            });
            if (!found_fd) {
                break;
            }
            if (ker::vfs::vfs_close(static_cast<int>(fd)) == -EBADF) {
                ker::vfs::vfs_release_fd(current_task, static_cast<int>(fd));
            }
        }

        if (current_task->elf_buffer != nullptr) {
            if (current_task->is_elf_buffer_shared) {
                ker::net::wki::wki_remote_compute_release_elf_buffer(current_task->elf_buffer);
            } else {
                delete[] current_task->elf_buffer;
            }
            current_task->elf_buffer = nullptr;
            current_task->elf_buffer_size = 0;
        }
    }

    // From this point forward the task must not block or return to userspace:
    // user address-space teardown, waiter notification, and final GC handoff all
    // assume it has left the ordinary ACTIVE scheduling domain.
    if (!current_task->transition_state(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
        for (;;) {
            asm volatile("hlt");
        }
    }

    // Memory barrier to ensure state change is visible to all CPUs.
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    ker::mod::sched::finish_syscall_accounting();
    current_task->has_exited = true;
    current_task->exit_notify_ready.store(true, std::memory_order_release);

    // Publish waitability after descriptor teardown but before address-space
    // reclamation. waitpid must not observe the child until files/pipes are
    // closed and status/accounting are stable, but it also must not depend on
    // later memory cleanup making progress under heavy build load.

    // Reschedule all tasks waiting for this process to exit.
    // This happens AFTER reparenting + FD cleanup so that any files written
    // by the exiting process are fully committed to the VFS before waitpid
    // returns to the waiter.
    for (;;) {
        ExitWaiterBatch waiting_pids{};
        size_t const WAITER_COUNT = drain_exit_waiters_for_notify(current_task, waiting_pids);
        if (WAITER_COUNT == 0) {
            break;
        }

        for (size_t i = 0; i < WAITER_COUNT; ++i) {
            uint64_t const WAITING_PID = waiting_pids.at(i);
#ifdef EXIT_DEBUG
            log::debug("rescheduling waiting task PID %x", WAITING_PID);
#endif

            // Use findTaskByPidSafe to get a refcounted reference - prevents use-after-free
            auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(WAITING_PID);
            if (waiting_task != nullptr) {
                // Only modify the waiting task's saved context when it's safely in waitQueue
                // (deferred_task_switch is false). When deferred_task_switch is true, the task is
                // still running on another CPU - writing to context.regs is a data race and the
                // values would be overwritten by deferred_task_switch's context save anyway.
                // In that case, deferred_task_switch() will detect hasExited==true and set rax
                // correctly before re-scheduling the task (see scheduler.cpp deferred_task_switch).
                if (waiter_context_can_be_completed(waiting_task) && waiter_matches_child(waiting_task, current_task) &&
                    complete_exit_wait(waiting_task, current_task, "exit-specific")) {
#ifdef EXIT_DEBUG
                    log::debug("set exit status %d for waiting task PID %x", current_task->exit_status, WAITING_PID);
#endif
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
                log::debug("successfully rescheduled waiting task PID %x on CPU %d", WAITING_PID, waiting_task->cpu);
#endif

                // Release the reference we acquired from findTaskByPidSafe
                waiting_task->release();
            } else {
#ifdef EXIT_DEBUG
                log::debug("could not find waiting task PID %x", WAITING_PID);
#endif
            }
        }
    }

    notify_tracer_after_exit_ready(current_task);
    notify_parent_after_exit_ready(current_task);

    // A waitable zombie keeps only status/accounting/PID metadata. It must not
    // pin the exiting process's user address space while waiting for the parent
    // to reap it, especially under fork/COW storms.
    release_exiting_user_address_space(current_task);

    // Thread destruction deferred to gcExpiredTasks()

    // NOTE: We CANNOT free the kernel stack here because we're still running on it!
    // The kernel stack will be freed later when the task is fully cleaned up
    // (after switching to a different task's kernel stack).
    // The syscall_kernel_stack and syscall_scratch_area are left intact for now.
    // They will be cleaned up by jumpToNextTask when it moves the task to expiredTasks,
    // and eventually by a garbage collection mechanism.

    // Unlink any WKI wait entries belonging to this task so the timer scan
    // doesn't dereference freed stack memory after this task's stack is reclaimed.
    ker::net::wki::wki_wait_cleanup_for_task(current_task);

    // Remove any futex waiter node still owned by this task. Otherwise a later
    // futex_wake() can target a DEAD task and keep stale 64-byte wait nodes alive.
    ker::syscall::futex::futex_wait_cleanup_for_task(current_task);

    ker::syscall::shm::shm_cleanup_for_task(current_task);

    cleanup_signal_handlers_for_exit(current_task);

    uint32_t const EXIT_US = clamp_perf_aux(ker::mod::time::get_us() - EXIT_STARTED_US);
    record_local_proc_event(current_task, ker::mod::perf::WkiPerfLocalProcOp::EXIT, ker::mod::perf::WkiPerfPhase::END, EXIT_CORR, 0,
                            EXIT_US, WOS_PERF_CALLSITE());
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PROC,
                                       static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalProcOp::EXIT), 0, 0, 0, EXIT_US, true, 0, 0);

    // Transition to DEAD state and record death epoch for garbage collection.
    // The task will be reclaimed once all CPUs have passed through the grace period.
    current_task->death_epoch.store(ker::mod::sched::EpochManager::current_epoch(), std::memory_order_release);
    current_task->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);

#ifdef EXIT_DEBUG
    log::debug("removing task from runqueue");
#endif

    // This function will not return - it switches to the next task
    // The current task is moved to expiredTasks list by jumpToNextTask
    jump_to_next_task_no_save();

    __builtin_unreachable();
}

}  // namespace

void exit_current_if_process_exit_requested() {
    auto* task = ker::mod::sched::get_current_task();
    if (!task_alive_for_group_exit_request(task) || !task->process_exit_requested.load(std::memory_order_acquire)) {
        return;
    }

    int const STATUS = task->requested_process_exit_status.load(std::memory_order_relaxed);
    int const WAIT_STATUS = task->requested_process_exit_wait_status.load(std::memory_order_relaxed);
    task->process_exit_requested.store(false, std::memory_order_release);

    if (task->is_thread) {
        ker::syscall::multiproc::wos_thread_exit_current();
    }

    wos_proc_exit_with_wait_status(STATUS, WAIT_STATUS);
}

[[noreturn]] void wos_proc_exit(int status) {
    // Store normal process exits in POSIX waitpid format.
    wos_proc_exit_with_wait_status(status, (status & 0xff) << 8);
}

[[noreturn]] void wos_proc_exit_signal(int signo) {
    // A shell turns WIFSIGNALED(status)/WTERMSIG(status) into "$? = 128 + signo".
    wos_proc_exit_with_wait_status(128 + signo, signo & 0x7f);
}

#ifdef WOS_SELFTEST
auto process_selftest_exit_waiter_notify_drains_over_batch() -> bool {
    ker::mod::sched::task::Task exiting{};
    constexpr size_t WAITER_COUNT = (EXIT_WAITER_NOTIFY_BATCH * 2) + 3;
    constexpr uint64_t FIRST_PID = 1000;
    std::array<bool, WAITER_COUNT> seen{};

    for (size_t i = 0; i < WAITER_COUNT; ++i) {
        if (!exiting.awaitee_on_exit.push_back(FIRST_PID + i)) {
            return false;
        }
    }

    size_t drained = 0;
    bool saw_full_batch = false;
    for (;;) {
        ExitWaiterBatch batch{};
        size_t const COUNT = drain_exit_waiters_for_notify(&exiting, batch);
        if (COUNT == 0) {
            break;
        }
        saw_full_batch = saw_full_batch || COUNT == EXIT_WAITER_NOTIFY_BATCH;

        for (size_t i = 0; i < COUNT; ++i) {
            uint64_t const PID = batch.at(i);
            if (PID < FIRST_PID || PID >= FIRST_PID + WAITER_COUNT) {
                return false;
            }
            size_t const INDEX = static_cast<size_t>(PID - FIRST_PID);
            if (seen.at(INDEX)) {
                return false;
            }
            seen.at(INDEX) = true;
            drained++;
        }
    }

    return saw_full_batch && drained == WAITER_COUNT && exiting.awaitee_on_exit.empty();
}
#endif

}  // namespace ker::syscall::process
