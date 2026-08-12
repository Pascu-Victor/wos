#include "threadControl.hpp"

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/signal.hpp>
#include <platform/sys/usercopy.hpp>
#include <vfs/vfs.hpp>

#include "abi/callnums/multiproc.h"
#include "platform/dbg/dbg.hpp"
#include "platform/smt/smt.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "syscalls_impl/log/sys_log.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"

namespace ker::syscall::multiproc {
namespace {
constexpr uint32_t SOFT_EXCLUSIVE_DAEMON_PENALTY = 7;
constexpr uint64_t MLIBC_TCB_TID_OFFSET = 0x18;
static_assert(MLIBC_TCB_TID_OFFSET + sizeof(int) <= mod::sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES);
std::atomic<uint64_t> next_thread_cpu{0};

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

auto publish_thread_tid_to_tcb(mod::sched::task::Task* parent, uint64_t tcb_va, uint64_t tid) -> bool {
    if (parent == nullptr || tcb_va == 0) {
        return false;
    }

    uint64_t tid_user_addr = 0;
    if (__builtin_add_overflow(tcb_va, MLIBC_TCB_TID_OFFSET, &tid_user_addr) ||
        !mod::sys::usercopy::range_valid(tid_user_addr, sizeof(int))) {
        return false;
    }

    mod::sys::usercopy::StableUserPage tid_page{};
    // THREAD_CREATE preflights the complete TCB publication range before its
    // publication guard.  Do not fault or allocate while that mutex is held.
    if (!mod::sys::usercopy::pin_task_user_page(*parent, tid_user_addr, true, false, tid_page)) {
        return false;
    }

    auto* tid_ptr = static_cast<int*>(tid_page.kernel_address());
    __atomic_store_n(tid_ptr, static_cast<int>(tid), __ATOMIC_RELEASE);
    return tid_page.commit_write();
}
}  // namespace

[[noreturn]] void wos_thread_exit_current() {
    auto* task = mod::sched::get_current_task();
    if (task == nullptr) {
        jump_to_next_task_no_save();
        __builtin_unreachable();
    }
    if (task->exit_in_progress) {
        for (;;) {
            asm volatile("hlt");
        }
    }
    task->exit_in_progress = true;
    ker::syscall::log::sys_log_cleanup_for_task(task);
    release_thread_fd_refs(task);
    // A thread can create a process too. Detach WKI's raw subject pointer
    // before reclaiming a child whose creator was interrupted in submission.
    ker::net::wki::wki_remote_compute_cleanup_for_task(task);
    static_cast<void>(mod::sched::task::destroy_unpublished_process(mod::sched::task::take_unpublished_process(task)));
    // Retire any stack-backed waiters created by descriptor/child teardown
    // before the thread becomes DEAD and its kernel stack enters reclamation.
    ker::net::wki::wki_wait_cleanup_for_task(task);
    ker::syscall::futex::futex_wait_cleanup_for_task(task);
    ker::syscall::process::child_events::cancel_wait(*task);
    task->has_exited = true;
    task->exit_status = 0;
    task->exit_notify_ready.store(true, std::memory_order_release);
    task->death_epoch.store(mod::sched::EpochManager::current_epoch(), std::memory_order_release);
    task->state.store(mod::sched::task::TaskState::DEAD, std::memory_order_release);

    jump_to_next_task_no_save();
    __builtin_unreachable();
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
            {
                // Serialize the lazy-range clone through scheduler publication with
                // shared-address-space mmap/munmap/mprotect metadata propagation.
                // The new thread must either clone a completed update or be visible
                // to the next update's active-task snapshot.
                ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                auto* t = mod::sched::task::Task::create_user_thread(parent, tcb_va, user_sp, enter_va, prepared_stack_words.at(0),
                                                                     prepared_stack_words.at(1));
                if (t == nullptr) {
                    return static_cast<uint64_t>(-ENOMEM);
                }

                uint64_t const CPU_COUNT = mod::smt::get_core_count();
                if (CPU_COUNT == 0) {
                    mod::sched::task::destroy_unpublished_user_thread(t);
                    return static_cast<uint64_t>(-ENOMEM);
                }
                uint64_t const TARGET_CPU = next_thread_cpu.fetch_add(1, std::memory_order_relaxed) % CPU_COUNT;

                if (!publish_thread_tid_to_tcb(parent, tcb_va, t->pid)) {
                    mod::sched::task::destroy_unpublished_user_thread(t);
                    return static_cast<uint64_t>(-EFAULT);
                }
                mod::sys::signal::sync_task_signal_mask_cache_mapped(t);

                bool const POSTED = mod::sched::post_task_for_cpu(TARGET_CPU, t);
                if (!POSTED) {
                    (void)publish_thread_tid_to_tcb(parent, tcb_va, 0);
                    mod::sched::task::destroy_unpublished_user_thread(t);
                    return static_cast<uint64_t>(-ENOMEM);
                }

                // Group exit can race with THREAD_CREATE after its active-registry
                // scan has passed this creator but before the new sibling is
                // published. Inherit the creator's request after publication so
                // the sibling cannot escape the process-wide exit.
                static_cast<void>(ker::syscall::process::inherit_pending_process_exit_request(parent, t));
                thread_pid = t->pid;
            }

            // wos_proc_exit() does not unwind C++ stack objects. Check only
            // after the publication guard has released its global mutex.
            ker::syscall::process::exit_current_if_process_exit_requested();

            // Return the new thread's PID as the TID; mlibc stores this in tcb->tid
            return thread_pid;
        }

        case abi::multiproc::threadControlOps::THREAD_EXIT: {
            wos_thread_exit_current();
        }

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
                task->domain_mask = MASK;
                task->domain_hard = false;
            } else if (MASK == VALID_MASK) {
                // Full mask: remove all affinity restrictions
                task->domain_mask = ~0ULL;
                task->domain_hard = false;
                task->cpu_pinned = false;
            } else {
                // Multi-bit subset: set domain_mask, pick least-loaded CPU within mask
                uint64_t const TARGET_CPU = mod::sched::get_least_loaded_cpu_in_mask(MASK);
                if (!IS_RUNNING && !IS_WAITING && !mod::sched::migrate_task_to_cpu(task, TARGET_CPU)) {
                    task->release();
                    return static_cast<uint64_t>(-EBUSY);
                }
                task->domain_mask = MASK;
                task->domain_hard = false;
                task->cpu_pinned = false;
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
            if (task->cpu_pinned) {
                if (task->cpu >= 64) {
                    task->release();
                    return static_cast<uint64_t>(-ERANGE);
                }
                mask = 1ULL << task->cpu;
            } else if (task->domain_mask != ~0ULL) {
                mask = task->domain_mask & online_cpu_mask();
            }

            task->release();
            return mask;
        }

        case abi::multiproc::threadControlOps::CREATE_DOMAIN: {
            // arg1 = ptr to struct { char name[32]; uint64_t cpu_mask; uint8_t soft_exclusive; uint8_t hard; }
            constexpr size_t REQUEST_SIZE = 42;
            std::array<uint8_t, REQUEST_SIZE> request{};
            auto* current_task = mod::sched::get_current_task();
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
            task->domain_id = DOMAIN_ID;
            task->domain_mask = dom->cpu_mask;
            task->domain_hard = HARD || dom->hard;
            task->cpu_pinned = false;
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

            auto* current_task = mod::sched::get_current_task();
            if (current_task == nullptr || !mod::sys::usercopy::copy_to_task(*current_task, arg2, output.data(), output.size())) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        default:
            mod::dbg::error("Invalid op in syscall thread control");
            return static_cast<uint64_t>(-EINVAL);
    }
}
}  // namespace ker::syscall::multiproc
