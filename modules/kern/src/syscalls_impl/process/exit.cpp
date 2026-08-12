#include "exit.hpp"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/signal.hpp>
#include <util/hcf.hpp>
#include <vfs/vfs.hpp>

#include "child_events.hpp"
#include "platform/sched/task.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "syscalls_impl/log/sys_log.hpp"
#include "syscalls_impl/multiproc/threadControl.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"

#ifdef WOS_SELFTEST
#include <array>
#endif

namespace ker::syscall::process {

namespace {
using log = ker::mod::dbg::logger<"pexit">;
namespace sched_task = ker::mod::sched::task;

constexpr uint64_t SIGKILL_MASK = 1ULL << (9 - 1);
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

void release_exiting_user_address_space(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::PROCESS || task->is_thread || task->pagemap == nullptr) {
        return;
    }
    if (ker::mod::sched::task_has_live_pagemap_sibling(task)) {
        return;
    }

    // The task has already transitioned out of ACTIVE, so it will not return to
    // userspace. Switch this CPU away from the exiting address space, then make
    // the Task stop publishing the pagemap before destroying the user half.
    // Waitpid-visible zombie state stays in the Task; scheduler GC still reclaims
    // the thread object, kernel stack, and scratch area after the epoch guard.
    ker::mod::mm::virt::switch_to_kernel_pagemap();
    auto* pagemap = task->detach_pagemap_after_usercopy_quiescence();
    if (pagemap == nullptr) {
        return;
    }
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

auto store_process_exit_request(ker::mod::sched::task::Task* task, int status, int wait_status) -> bool {
    if (!task_alive_for_group_exit_request(task)) {
        return false;
    }

    task->requested_process_exit_status.store(status, std::memory_order_relaxed);
    task->requested_process_exit_wait_status.store(wait_status, std::memory_order_relaxed);
    task->process_exit_requested.store(true, std::memory_order_release);
    return true;
}

auto copy_pending_process_exit_request(ker::mod::sched::task::Task* source, ker::mod::sched::task::Task* new_sibling) -> bool {
    if (source == nullptr || new_sibling == nullptr || !source->process_exit_requested.load(std::memory_order_acquire)) {
        return false;
    }

    int const STATUS = source->requested_process_exit_status.load(std::memory_order_relaxed);
    int const WAIT_STATUS = source->requested_process_exit_wait_status.load(std::memory_order_relaxed);
    return store_process_exit_request(new_sibling, STATUS, WAIT_STATUS);
}

auto publish_process_exit_request(ker::mod::sched::task::Task* task, int status, int wait_status) -> bool {
    if (!store_process_exit_request(task, status, wait_status)) {
        return false;
    }

    task->signal_add_pending_mask(SIGKILL_MASK);
    ker::mod::sched::wake_task_for_signal(task);
    // Preserve request-before-park races without a periodic scheduler scan.
    // The target rechecks process_exit_requested at its next safe boundary.
    ker::mod::sched::wake_task_from_event(task);
    return true;
}

struct ThreadGroupExitRequest {
    ker::mod::sched::task::Task* initiator;
    uint64_t process_pid;
};

#ifdef WOS_SELFTEST
ker::mod::sched::task::Task group_exit_selftest_initiator;      // NOLINT
ker::mod::sched::task::Task group_exit_selftest_candidate;      // NOLINT
ker::mod::sched::task::Task exit_inheritance_selftest_source;   // NOLINT
ker::mod::sched::task::Task exit_inheritance_selftest_sibling;  // NOLINT
#endif

auto thread_group_exit_request_candidate(ker::mod::sched::task::Task* task, void* context) -> bool {
    auto* request = static_cast<ThreadGroupExitRequest*>(context);
    return request != nullptr && task != request->initiator && task_alive_for_group_exit_request(task) &&
           sched_task::same_thread_group(*task, request->process_pid) && !task->process_exit_requested.load(std::memory_order_acquire);
}

void request_thread_group_exit(ker::mod::sched::task::Task* initiator, int status, int wait_status) {
    if (initiator == nullptr) {
        return;
    }

    ThreadGroupExitRequest request{
        .initiator = initiator,
        .process_pid = sched_task::process_pid(*initiator),
    };
    for (;;) {
        // A sibling may run its exit path immediately after being woken. The
        // active-task registry removes entries by swapping its final slot into
        // the vacated index, so an index walk can skip the moved sibling.
        // Select one unrequested sibling at a time under the registry lock and
        // retain it through request publication instead.
        auto* task = ker::mod::sched::find_active_task_lifetime_ref_if(thread_group_exit_request_candidate, static_cast<void*>(&request));
        if (task == nullptr) {
            break;
        }

        static_cast<void>(publish_process_exit_request(task, status, wait_status));
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
    // Transfer authoritative child/event ownership to init, cancel this
    // process's wait registrations, and detach its tracees while the task may
    // still safely perform registry lookups and scheduler wakes.
    child_events::prepare_process_exit(*current_task);

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
        if (current_task->exec_image_file != nullptr) {
            ker::vfs::vfs_put_file(current_task->exec_image_file);
            current_task->exec_image_file = nullptr;
            current_task->exec_image_size = 0;
        }
    }

    // Retire direct-submit pointers before destroying a child whose creator
    // was fatally interrupted inside remote placement. Full Task teardown can
    // close files and block, so all of this remains in the ACTIVE exit phase.
    ker::net::wki::wki_remote_compute_cleanup_for_task(current_task);
    static_cast<void>(sched_task::destroy_unpublished_process(sched_task::take_unpublished_process(current_task)));

    // Child teardown and ordinary descriptor close may themselves have used
    // WKI. Quiesce the complete task-owned wait/VFS surface before EXITING.
    ker::net::wki::wki_wait_cleanup_for_task(current_task);

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
    if (!child_events::publish_exit(*current_task)) {
        ker::mod::dbg::panic_handler("process exit: failed to publish child exit event");
        hcf();
    }
    ker::net::wki::wki_remote_compute_notify_task_exit_ready(current_task);

    // Publish waitability after descriptor teardown but before address-space
    // reclamation. waitpid must not observe the child until files/pipes are
    // closed and status/accounting are stable, but it also must not depend on
    // later memory cleanup making progress under heavy build load.

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

auto inherit_pending_process_exit_request(ker::mod::sched::task::Task* source, ker::mod::sched::task::Task* new_sibling) -> bool {
    if (!copy_pending_process_exit_request(source, new_sibling)) {
        return false;
    }

    new_sibling->signal_add_pending_mask(SIGKILL_MASK);
    ker::mod::sched::wake_task_for_signal(new_sibling);
    ker::mod::sched::wake_task_from_event(new_sibling);
    return true;
}

void exit_current_if_process_exit_requested() {
    auto* task = ker::mod::sched::get_current_task();
    if (!task_alive_for_group_exit_request(task) || task->unpublished_teardown_in_progress.load(std::memory_order_acquire) ||
        !task->process_exit_requested.load(std::memory_order_acquire)) {
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
auto process_selftest_group_exit_candidate_filter() -> bool {
    auto& initiator = group_exit_selftest_initiator;
    auto& candidate = group_exit_selftest_candidate;
    constexpr uint64_t PROCESS_PID = 700;

    initiator.type = sched_task::TaskType::PROCESS;
    initiator.pid = PROCESS_PID;
    initiator.owner_pid = 0;
    initiator.has_exited = false;
    initiator.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);

    candidate.type = sched_task::TaskType::PROCESS;
    candidate.pid = PROCESS_PID + 1;
    candidate.owner_pid = PROCESS_PID;
    candidate.has_exited = false;
    candidate.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);
    candidate.process_exit_requested.store(false, std::memory_order_relaxed);

    ThreadGroupExitRequest request{
        .initiator = &initiator,
        .process_pid = PROCESS_PID,
    };
    bool const MATCHES_LIVE_SIBLING = thread_group_exit_request_candidate(&candidate, &request);

    candidate.process_exit_requested.store(true, std::memory_order_relaxed);
    bool const EXCLUDES_REQUESTED = !thread_group_exit_request_candidate(&candidate, &request);
    candidate.process_exit_requested.store(false, std::memory_order_relaxed);

    candidate.owner_pid = PROCESS_PID + 2;
    bool const EXCLUDES_OTHER_GROUP = !thread_group_exit_request_candidate(&candidate, &request);
    candidate.owner_pid = PROCESS_PID;

    candidate.state.store(sched_task::TaskState::DEAD, std::memory_order_relaxed);
    bool const EXCLUDES_DEAD = !thread_group_exit_request_candidate(&candidate, &request);
    candidate.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);

    bool const EXCLUDES_INITIATOR = !thread_group_exit_request_candidate(&initiator, &request);
    return MATCHES_LIVE_SIBLING && EXCLUDES_REQUESTED && EXCLUDES_OTHER_GROUP && EXCLUDES_DEAD && EXCLUDES_INITIATOR;
}

auto process_selftest_pending_exit_request_inheritance() -> bool {
    auto& source = exit_inheritance_selftest_source;
    auto& sibling = exit_inheritance_selftest_sibling;
    constexpr int STATUS = 73;
    constexpr int WAIT_STATUS = STATUS << 8;

    source.type = sched_task::TaskType::PROCESS;
    source.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);
    source.requested_process_exit_status.store(STATUS, std::memory_order_relaxed);
    source.requested_process_exit_wait_status.store(WAIT_STATUS, std::memory_order_relaxed);
    source.process_exit_requested.store(true, std::memory_order_release);

    sibling.type = sched_task::TaskType::PROCESS;
    sibling.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);
    bool const COPIED = copy_pending_process_exit_request(&source, &sibling);
    bool const STATUS_MATCHES = sibling.requested_process_exit_status.load(std::memory_order_relaxed) == STATUS;
    bool const WAIT_STATUS_MATCHES = sibling.requested_process_exit_wait_status.load(std::memory_order_relaxed) == WAIT_STATUS;
    bool const REQUESTED = sibling.process_exit_requested.load(std::memory_order_acquire);

    sibling.state.store(sched_task::TaskState::DEAD, std::memory_order_relaxed);
    bool const REJECTS_DEAD = !copy_pending_process_exit_request(&source, &sibling);

    source.process_exit_requested.store(false, std::memory_order_release);
    sibling.state.store(sched_task::TaskState::ACTIVE, std::memory_order_relaxed);
    bool const REJECTS_UNREQUESTED_SOURCE = !copy_pending_process_exit_request(&source, &sibling);
    return COPIED && STATUS_MATCHES && WAIT_STATUS_MATCHES && REQUESTED && REJECTS_DEAD && REJECTS_UNREQUESTED_SOURCE;
}

#endif

}  // namespace ker::syscall::process
