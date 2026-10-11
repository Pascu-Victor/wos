#include "threadControl.hpp"

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <platform/debug/ptrace.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/signal.hpp>
#include <platform/sys/usercopy.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/vfs.hpp>

#include "abi/callnums/multiproc.h"
#include "platform/dbg/dbg.hpp"
#include "platform/smt/smt.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "syscalls_impl/log/sys_log.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"

namespace ker::syscall::multiproc {
namespace {
constexpr uint32_t SOFT_EXCLUSIVE_DAEMON_PENALTY = 7;
constexpr uint64_t MLIBC_TCB_TID_OFFSET = mod::sched::threading::MLIBC_TCB_TID_OFFSET;
static_assert(MLIBC_TCB_TID_OFFSET + sizeof(int) <= mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES);
std::atomic<uint64_t> next_thread_cpu{0};

auto reclaim_thread_stack_from_syscall(mod::sched::task::Task* parent) -> bool {
    // THREAD_CREATE enters with IF cleared by IA32_FMASK. The allocator's
    // generic reclaim predicate must keep rejecting that state in fault
    // handlers; this private helper is reached only from the syscall dispatcher.
    if (!mod::sched::has_run_queues() || !mod::sched::can_query_current_task() || parent == nullptr ||
        parent != mod::sched::get_current_task() || parent->type != mod::sched::task::TaskType::PROCESS ||
        parent->syscall_account_start_us == 0 || mod::sched::preempt_count() != 0 || mod::io::serial::is_panic_mode() ||
        parent->scheduler_transition_active.load(std::memory_order_acquire) || parent->deferred_task_switch || parent->wants_block ||
        parent->is_voluntary_blocked()) {
        return false;
    }

    // No publication/runqueue/pool lock or unpublished child is held here.
    // GC can park at its existing scheduler safe points and keeps all of its
    // epoch/current/refcount guards. Preserve the caller's IF across that work.
    bool const INTERRUPTS_WERE_ENABLED = mod::sched::interrupts_enabled();
    asm volatile("sti" ::: "memory");
    mod::sched::GcPressureDiagnostics diagnostics{};
    uint32_t const RECLAIMED = mod::sched::reclaim_memory_pressure(&diagnostics);
    // NOLINTNEXTLINE(bugprone-branch-clone)
    if (INTERRUPTS_WERE_ENABLED) {
        asm volatile("sti" ::: "memory");
    } else {
        asm volatile("cli" ::: "memory");
    }
    mod::dbg::logger<"thread">::info(
        "stack pressure: pid=%lx entry_if=%u reclaimed=%u ran=%u scanned=%lu epoch=%lu young=%lu current=%lu ref=%lu not_dead=%lu "
        "zombie_busy=%lu retained=%lu invalid=%lu time_limit=%u deferred=%u",
        parent->pid, INTERRUPTS_WERE_ENABLED ? 1U : 0U, RECLAIMED, diagnostics.ran ? 1U : 0U, diagnostics.scanned, diagnostics.epoch_skips,
        diagnostics.epoch_still_young, diagnostics.current_skips, diagnostics.refcount_skips, diagnostics.not_dead_skips,
        diagnostics.zombie_busy_skips, diagnostics.retained_zombie_skips, diagnostics.invalid_tasks,
        diagnostics.time_budget_exhausted ? 1U : 0U, diagnostics.deferred_pending ? 1U : 0U);
    return true;
}

auto online_cpu_mask() -> uint64_t {
    uint64_t const CPU_COUNT = mod::smt::get_core_count();
    if (CPU_COUNT == 0) {
        return 0;
    }
    if (CPU_COUNT >= 64) {
        return UINT64_MAX;
    }
    return (1ULL << CPU_COUNT) - 1ULL;
}

auto cpu_mask_bit(uint64_t cpu) -> uint64_t {
    if (cpu >= 64) {
        return 0;
    }
    return 1ULL << cpu;
}

void release_thread_fd_refs(mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    while (!task->fd_table.empty()) {
        uint64_t fd = 0;
        bool found_fd = false;
        task->fd_table.for_each([&](uint64_t key, void* val) {
            if (!found_fd && val != nullptr) {
                fd = key;
                found_fd = true;
            }
        });
        if (!found_fd) {
            break;
        }

        auto* file = static_cast<ker::vfs::File*>(nullptr);
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        file = static_cast<ker::vfs::File*>(task->fd_table.lookup(fd));
        if (file != nullptr) {
            task->fd_table.remove(fd);
            task->clear_fd_cloexec(static_cast<unsigned>(fd));
        }
        task->fd_table_lock.unlock_irqrestore(IRQF);

        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
    }
}

}  // namespace

[[noreturn]] void wos_thread_exit_current() {
    auto* task = mod::sched::get_current_task();
    if (task == nullptr) {
        jump_to_next_task_no_save();
        __builtin_unreachable();
    }
    if (!task->is_thread && task->type == mod::sched::task::TaskType::PROCESS) {
        ker::syscall::process::retire_main_thread();
    }
    if (task->exit_in_progress) {
        for (;;) {
            asm volatile("hlt");
        }
    }
    task->exit_in_progress = true;
    mod::debug::ptrace::cleanup_operations(*task);
    task->jobctl_membership.detach();
    ker::vfs::cancel_fd_reservations(*task);
    mod::sched::resource::cancel_process_reservations(*task);
    ker::syscall::log::sys_log_cleanup_for_task(task);
    release_thread_fd_refs(task);
    // A thread can create a process too. Detach WKI's raw subject pointer
    // before reclaiming a child whose creator was interrupted in submission.
    ker::net::wki::wki_remote_compute_cleanup_for_task(task);
    static_cast<void>(mod::sched::task::destroy_unpublished_process(mod::sched::task::take_unpublished_process(task)));
    // Each worker owns cloned lazy-file references. Retire them while ACTIVE
    // and before finish_cpu allows the process owner to publish group exit.
    task->quiesce_usercopy_pagemap();
    mod::sched::task::release_lazy_vmem_ranges(*task);
    // Retire any stack-backed waiters created by descriptor/child teardown
    // before the thread becomes DEAD and its kernel stack enters reclamation.
    ker::syscall::process::cleanup_exec_preparation(*task);
    ker::net::wki::wki_wait_cleanup_for_task(task);
    ker::syscall::futex::futex_wait_cleanup_for_task(task);
    ker::syscall::process::child_events::cancel_wait(*task);
    task->process_charge.release();
    mod::sched::finalize_current_task_cpu_accounting();
    task->resource_owner.finish_cpu();
    task->has_exited = true;
    task->exit_status = 0;
    task->exit_notify_ready.store(true, std::memory_order_release);
    task->death_epoch.store(mod::sched::EpochManager::current_epoch(), std::memory_order_release);
    task->state.store(mod::sched::task::TaskState::DEAD, std::memory_order_release);

    jump_to_next_task_no_save();
    __builtin_unreachable();
}

auto host_thread_control_for_subject(mod::sched::task::Task* subject, abi::multiproc::threadControlOps op, uint64_t arg1, uint64_t arg2,
                                     uint64_t arg3) -> uint64_t {
    int const SCOPE = mod::security::compartment::host_namespace_admission(subject);
    if (SCOPE != 0) return static_cast<uint64_t>(SCOPE);
    switch (op) {
        case abi::multiproc::threadControlOps::SET_AFFINITY: {
            auto const TID = arg1;
            auto const MASK = arg2;

            auto* task = mod::sched::find_task_by_pid_safe(TID);
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            uint64_t const VALID_MASK = online_cpu_mask();
            if (VALID_MASK == 0) {
                task->release();
                return static_cast<uint64_t>(-ENODEV);
            }
            if ((MASK & VALID_MASK) == 0 || (MASK & ~VALID_MASK) != 0) {
                task->release();
                return static_cast<uint64_t>(-EINVAL);
            }

            uint64_t const RUNNING_CPU = mod::sched::current_cpu_for_task(task);
            bool const IS_RUNNING = RUNNING_CPU != UINT64_MAX;
            uint64_t const OWNER_CPU = IS_RUNNING ? RUNNING_CPU : mod::sched::owner_cpu_for_task(task);
            bool const IS_WAITING = task->sched_queue == mod::sched::task::Task::sched_queue::WAITING;
            if (IS_RUNNING && (MASK & cpu_mask_bit(RUNNING_CPU)) == 0) {
                task->release();
                return static_cast<uint64_t>(-EBUSY);
            }
            if (IS_WAITING && OWNER_CPU != UINT64_MAX && (MASK & cpu_mask_bit(OWNER_CPU)) == 0) {
                task->release();
                return static_cast<uint64_t>(-EBUSY);
            }
            if (std::has_single_bit(MASK)) {
                // Single-CPU pin
                uint64_t const TARGET_CPU = std::countr_zero(MASK);
                if (!mod::sched::pin_task_to_cpu(task, TARGET_CPU)) {
                    task->release();
                    return static_cast<uint64_t>(-EBUSY);
                }
                task->store_domain_mask(MASK);
                task->store_domain_hard(false);
            } else if (MASK == VALID_MASK) {
                // Full mask: remove all affinity restrictions
                task->store_domain_mask(~0ULL);
                task->store_domain_hard(false);
                task->store_cpu_pinned(false);
            } else {
                // Multi-bit subset: set domain_mask, pick least-loaded CPU within mask
                uint64_t const TARGET_CPU = mod::sched::get_least_loaded_cpu_in_mask(MASK);
                if (!IS_RUNNING && !IS_WAITING && !mod::sched::migrate_task_to_cpu(task, TARGET_CPU)) {
                    task->release();
                    return static_cast<uint64_t>(-EBUSY);
                }
                task->store_domain_mask(MASK);
                task->store_domain_hard(false);
                task->store_cpu_pinned(false);
            }

            task->release();
            return 0;
        }

        case abi::multiproc::threadControlOps::GET_AFFINITY: {
            auto const TID = arg1;

            auto* task = mod::sched::find_task_by_pid_safe(TID);
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            uint64_t mask = online_cpu_mask();
            if (mask == 0) {
                task->release();
                return static_cast<uint64_t>(-ENODEV);
            }
            if (task->load_cpu_pinned()) {
                if (task->load_cpu() >= 64) {
                    task->release();
                    return static_cast<uint64_t>(-ERANGE);
                }
                mask = 1ULL << task->load_cpu();
            } else if (task->load_domain_mask() != ~0ULL) {
                mask = task->load_domain_mask() & online_cpu_mask();
            }

            task->release();
            return mask;
        }

        case abi::multiproc::threadControlOps::CREATE_DOMAIN: {
            // arg1 = ptr to struct { char name[32]; uint64_t cpu_mask; uint8_t soft_exclusive; uint8_t hard; }
            constexpr size_t REQUEST_SIZE = 42;
            std::array<uint8_t, REQUEST_SIZE> request{};
            auto* current_task = subject;
            if (current_task == nullptr || !mod::sys::usercopy::copy_from_task(*current_task, arg1, request.data(), request.size())) {
                return static_cast<uint64_t>(-EFAULT);
            }
            std::array<char, 32> name{};
            std::memcpy(name.data(), request.data(), name.size() - 1);
            name.at(31) = '\0';
            uint64_t cpu_mask = 0;
            std::memcpy(&cpu_mask, request.data() + 32, sizeof(cpu_mask));
            bool const SOFT_EXCLUSIVE = request.at(40) != 0;
            bool const HARD = request.at(41) != 0;
            uint64_t const VALID = online_cpu_mask();
            if ((cpu_mask & VALID) == 0) {
                return static_cast<uint64_t>(-EINVAL);
            }
            cpu_mask &= VALID;
            uint32_t const DOMAIN_ID = mod::smt::create_leaf_domain(name.data(), cpu_mask, SOFT_EXCLUSIVE, HARD);
            if (DOMAIN_ID == mod::smt::DOMAIN_ID_INVALID) {
                return static_cast<uint64_t>(-ENOMEM);
            }
            // Apply daemon_load_penalty to each CPU in the new domain if soft_exclusive
            if (SOFT_EXCLUSIVE) {
                uint64_t const CPU_COUNT = mod::smt::get_core_count();
                for (uint64_t cpu = 0; cpu < CPU_COUNT && cpu < 64; ++cpu) {
                    if ((cpu_mask & (1ULL << cpu)) != 0U) {
                        mod::sched::set_cpu_daemon_penalty(cpu, SOFT_EXCLUSIVE_DAEMON_PENALTY);
                    }
                }
            }
            // Phase 8: stamp domain_id on each CPU's RunQueue so work-stealing
            // checks can enforce hard-domain boundaries without a separate registry lookup.
            {
                uint64_t const CPU_COUNT = mod::smt::get_core_count();
                for (uint64_t cpu = 0; cpu < CPU_COUNT && cpu < 64; ++cpu) {
                    if ((cpu_mask & (1ULL << cpu)) != 0U) {
                        mod::sched::set_cpu_domain_id(cpu, DOMAIN_ID);
                    }
                }
            }
            return DOMAIN_ID;
        }

        case abi::multiproc::threadControlOps::SET_DOMAIN: {
            // arg1=tid, arg2=domain_id, arg3=hard (0=soft, 1=hard)
            auto const TID = arg1;
            auto const DOMAIN_ID = static_cast<uint32_t>(arg2);
            bool const HARD = arg3 != 0;
            auto* dom = mod::smt::get_cpu_domain(DOMAIN_ID);
            if (dom == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto* task = mod::sched::find_task_by_pid_safe(TID);
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            uint64_t const RUNNING_CPU = mod::sched::current_cpu_for_task(task);
            bool const IS_RUNNING = RUNNING_CPU != UINT64_MAX;
            uint64_t const OWNER_CPU = IS_RUNNING ? RUNNING_CPU : mod::sched::owner_cpu_for_task(task);
            bool const IS_WAITING = task->sched_queue == mod::sched::task::Task::sched_queue::WAITING;
            if (IS_RUNNING && (dom->cpu_mask & cpu_mask_bit(RUNNING_CPU)) == 0) {
                task->release();
                return static_cast<uint64_t>(-EBUSY);
            }
            if (IS_WAITING && OWNER_CPU != UINT64_MAX && (dom->cpu_mask & cpu_mask_bit(OWNER_CPU)) == 0) {
                task->release();
                return static_cast<uint64_t>(-EBUSY);
            }
            task->store_domain_id(DOMAIN_ID);
            task->store_domain_mask(dom->cpu_mask);
            task->store_domain_hard(HARD || dom->hard);
            task->store_cpu_pinned(false);
            uint64_t const TARGET = mod::sched::get_least_loaded_cpu_in_mask(dom->cpu_mask);
            if (!IS_RUNNING && !IS_WAITING) {
                mod::sched::reschedule_task_for_cpu(TARGET, task);
            }
            task->release();
            return 0;
        }

        case abi::multiproc::threadControlOps::QUERY_DOMAIN: {
            // arg1=domain_id, arg2=ptr to struct { uint64_t cpu_mask; uint32_t cpu_loads[64]; }
            auto const DOMAIN_ID = static_cast<uint32_t>(arg1);
            auto* dom = mod::smt::get_cpu_domain(DOMAIN_ID);
            if (dom == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }

            constexpr size_t OUTPUT_SIZE = sizeof(uint64_t) + (64 * sizeof(uint32_t));
            std::array<uint8_t, OUTPUT_SIZE> output{};
            std::memcpy(output.data(), &dom->cpu_mask, sizeof(dom->cpu_mask));
            uint64_t const CPU_COUNT = mod::smt::get_core_count();
            for (uint64_t cpu = 0; cpu < CPU_COUNT && cpu < 64; ++cpu) {
                uint32_t const LOAD = ((dom->cpu_mask & (1ULL << cpu)) != 0U) ? mod::sched::get_cpu_load(cpu) : 0;
                std::memcpy(output.data() + sizeof(uint64_t) + (cpu * sizeof(uint32_t)), &LOAD, sizeof(LOAD));
            }

            auto* current_task = subject;
            if (current_task == nullptr || !mod::sys::usercopy::copy_to_task(*current_task, arg2, output.data(), output.size())) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        default:
            return static_cast<uint64_t>(-EINVAL);
    }
}

auto thread_control(abi::multiproc::threadControlOps op, uint64_t arg1, uint64_t arg2, uint64_t arg3) -> uint64_t {
    switch (op) {
        case abi::multiproc::threadControlOps::SET_TCB: {
            auto* task = mod::sched::get_current_task();
            if (task == nullptr || task->thread == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (arg1 == 0 || !mod::sys::usercopy::ensure_writable(*task, arg1, mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES) ||
                !mod::sys::usercopy::copy_value_to_task(*task, arg1, arg1) ||
                !mod::sys::signal::sync_task_signal_mask_cache_at(task, arg1)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return mod::smt::set_tcb(arg1);
        }

        case abi::multiproc::threadControlOps::YIELD: {
            auto* task = mod::sched::get_current_task();
            if (task != nullptr) {
                task->set_wait_channel("sched_yield");
                task->yield_switch = true;
                task->deferred_task_switch = true;
            }
            return 0;
        }

        case abi::multiproc::threadControlOps::THREAD_CREATE: {
            // arg1 = tcb vaddr (mlibc Tcb*, becomes fsbase / FS register)
            // arg2 = prepared stack pointer (entry + user_arg pushed below it)
            // arg3 = virtual address of __mlibc_enter_thread in the process image
            // arg4 (tid_out) is passed via the a4 register (r8); not yet exposed here -
            // the TID is returned as the syscall return value and mlibc writes it to tid_out.
            auto* parent = mod::sched::get_current_task();
            auto tcb_va = arg1;
            auto user_sp = arg2;
            auto enter_va = arg3;
            if (parent == nullptr || tcb_va == 0) {
                return static_cast<uint64_t>(-EINVAL);
            }
            ker::syscall::process::exit_current_if_process_exit_requested();
            mod::sched::resource::ProcessReservation process_reservation(parent, parent->credentials().user().real);
            if (process_reservation.result() < 0) {
                return static_cast<uint64_t>(process_reservation.result());
            }
            uint64_t tid_user_addr = 0;
            if (__builtin_add_overflow(tcb_va, MLIBC_TCB_TID_OFFSET, &tid_user_addr) ||
                !mod::sys::usercopy::range_valid(tid_user_addr, sizeof(int)) ||
                !mod::sys::usercopy::ensure_writable(*parent, tcb_va, mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            std::array<uint64_t, 2> prepared_stack_words{};
            if (!mod::sys::usercopy::copy_from_task(*parent, user_sp, prepared_stack_words.data(), sizeof(prepared_stack_words))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            uint64_t thread_pid = 0;
            for (uint32_t attempt = 0; attempt < 2; ++attempt) {
                bool creation_failed = false;
                bool stack_exhausted = false;
                {
                    // Serialize the lazy-range clone through scheduler publication with
                    // shared-address-space mmap/munmap/mprotect metadata propagation.
                    // The new thread must either clone a completed update or be visible
                    // to the next update's active-task snapshot.
                    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                    auto* t = mod::sched::task::Task::create_user_thread(parent, tcb_va, user_sp, enter_va, prepared_stack_words.at(0),
                                                                         prepared_stack_words.at(1), &stack_exhausted);
                    if (t == nullptr) {
                        creation_failed = true;
                    } else {
                        uint64_t const CPU_COUNT = mod::smt::get_core_count();
                        if (CPU_COUNT == 0) {
                            mod::sched::task::destroy_unpublished_user_thread(t);
                            return static_cast<uint64_t>(-ENOMEM);
                        }
                        uint64_t const TARGET_CPU = next_thread_cpu.fetch_add(1, std::memory_order_relaxed) % CPU_COUNT;

                        uint64_t const NEW_TID = t->namespace_tid();
                        if (!mod::sched::threading::publish_tid_to_tcb(*parent, tcb_va, NEW_TID, false)) {
                            mod::sched::task::destroy_unpublished_user_thread(t);
                            return static_cast<uint64_t>(-EFAULT);
                        }
                        mod::sys::signal::sync_task_signal_mask_cache_mapped(t);

                        // Keep the subject alive through exit propagation even
                        // if it runs and retires immediately after posting.
                        bool const PUBLICATION_REF = t->try_acquire_lifetime_ref();
                        bool const POSTED =
                            PUBLICATION_REF && process_reservation.attach(*t) && mod::sched::post_task_for_cpu(TARGET_CPU, t);
                        if (!POSTED) {
                            if (PUBLICATION_REF) t->release();
                            (void)mod::sched::threading::publish_tid_to_tcb(*parent, tcb_va, 0, false);
                            mod::sched::task::destroy_unpublished_user_thread(t);
                            return static_cast<uint64_t>(-ENOMEM);
                        }

                        // Group exit can race with THREAD_CREATE after its active-registry
                        // scan has passed this creator but before the new sibling is
                        // published. Inherit the creator's request after publication so
                        // the sibling cannot escape the process-wide exit.
                        static_cast<void>(ker::syscall::process::inherit_pending_process_exit_request(parent, t));
                        thread_pid = NEW_TID;
                        t->release();
                    }
                }
                if (!creation_failed) {
                    break;
                }
                // Retired tasks can exhaust the fixed guarded-stack reserve
                // even with free general RAM. Reclaim once without holding the
                // shared-VM publication mutex: GC may need that mutex itself.
                // Epoch/current/refcount checks still fence every stack free.
                if (attempt != 0 || !stack_exhausted || !reclaim_thread_stack_from_syscall(parent)) {
                    return static_cast<uint64_t>(-ENOMEM);
                }
            }

            // wos_proc_exit() does not unwind C++ stack objects. Check only
            // after the publication guard has released its global mutex.
            ker::syscall::process::exit_current_if_process_exit_requested();

            // Return the same public TID published before the thread ran.
            return thread_pid;
        }

        case abi::multiproc::threadControlOps::THREAD_EXIT: {
            wos_thread_exit_current();
        }

        case abi::multiproc::threadControlOps::SET_AFFINITY:
        case abi::multiproc::threadControlOps::GET_AFFINITY:
        case abi::multiproc::threadControlOps::CREATE_DOMAIN:
        case abi::multiproc::threadControlOps::SET_DOMAIN:
        case abi::multiproc::threadControlOps::QUERY_DOMAIN:
            return host_thread_control_for_subject(mod::sched::get_current_task(), op, arg1, arg2, arg3);

        default:
            mod::dbg::error("Invalid op in syscall thread control");
            return static_cast<uint64_t>(-EINVAL);
    }
}
}  // namespace ker::syscall::multiproc
