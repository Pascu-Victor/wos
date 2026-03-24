#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>

#include "abi/callnums/multiproc.h"
#include "platform/dbg/dbg.hpp"
#include "platform/smt/smt.hpp"

namespace ker::syscall::multiproc {
namespace {
auto online_cpu_mask() -> uint64_t {
    uint64_t cpu_count = mod::smt::getCoreCount();
    if (cpu_count == 0) {
        return 0;
    }
    if (cpu_count >= 64) {
        return UINT64_MAX;
    }
    return (1ULL << cpu_count) - 1ULL;
}
}  // namespace

auto threadControl(abi::multiproc::threadControlOps op, void* arg1, void* arg2, void* arg3) -> uint64_t {
    switch (op) {
        case abi::multiproc::threadControlOps::setTCB: {
            void* tcb = arg1;
            return mod::smt::setTcb(tcb);
        }

        case abi::multiproc::threadControlOps::yield: {
            auto* task = mod::sched::get_current_task();
            if (task != nullptr) {
                task->yieldSwitch = true;
                task->deferredTaskSwitch = true;
            }
            return 0;
        }

        case abi::multiproc::threadControlOps::threadCreate: {
            // arg1 = tcb vaddr (mlibc Tcb*, becomes fsbase / FS register)
            // arg2 = prepared stack pointer (entry + user_arg pushed below it)
            // arg3 = virtual address of __mlibc_enter_thread in the process image
            // arg4 (tid_out) is passed via the a4 register (r8); not yet exposed here —
            // the TID is returned as the syscall return value and mlibc writes it to tid_out.
            auto* parent = mod::sched::get_current_task();
            auto tcb_va = (uint64_t)arg1;
            auto user_sp = (uint64_t)arg2;
            auto enter_va = (uint64_t)arg3;

            auto* t = mod::sched::task::Task::createUserThread(parent, tcb_va, user_sp, enter_va);
            if (t == nullptr) {
                return (uint64_t)-ENOMEM;
            }

            bool posted = false;
            uint64_t cpu_count = mod::smt::getCoreCount();
            if (cpu_count > 0) {
                static std::atomic<uint64_t> next_thread_cpu{0};
                uint64_t target_cpu = next_thread_cpu.fetch_add(1, std::memory_order_relaxed) % cpu_count;
                posted = mod::sched::post_task_for_cpu(target_cpu, t);
            }
            if (!posted) {
                posted = mod::sched::post_task_balanced(t);
            }
            if (!posted) {
                return (uint64_t)-ENOMEM;
            }

            // Return the new thread's PID as the TID; mlibc stores this in tcb->tid
            return t->pid;
        }

        case abi::multiproc::threadControlOps::threadExit: {
            // Exit the current thread without tearing down the process.
            // The pagemap, FDs, and ELF are shared — do NOT free them.
            auto* task = mod::sched::get_current_task();
            if (task == nullptr) {
                return 0;
            }
            task->hasExited = true;
            task->exitStatus = 0;
            // Record death epoch for GC grace period, then transition to DEAD.
            // Without this, threads stay EXITING forever and GC never reclaims them.
            task->deathEpoch.store(mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
            task->state.store(mod::sched::task::TaskState::DEAD, std::memory_order_release);
            // Wake anyone waiting on this thread (e.g. via awaitee list)
            for (uint64_t i = 0; i < task->awaitee_on_exit_count; i++) {
                auto* waiter = mod::sched::find_task_by_pid_safe(task->awaitee_on_exit[i]);
                if (waiter != nullptr) {
                    mod::sched::reschedule_task_for_cpu(waiter->cpu, waiter);
                    waiter->release();
                }
            }
            // Transfer to dead list and pick next task — never returns
            jump_to_next_task_no_save();
            __builtin_unreachable();
        }

        case abi::multiproc::threadControlOps::setAffinity: {
            uint64_t tid = (uint64_t)arg1;
            uint64_t mask = (uint64_t)arg2;

            auto* task = mod::sched::find_task_by_pid_safe(tid);
            if (task == nullptr) {
                return (uint64_t)-ESRCH;
            }

            uint64_t valid_mask = online_cpu_mask();
            if (valid_mask == 0) {
                task->release();
                return (uint64_t)-ENODEV;
            }
            if ((mask & valid_mask) == 0 || (mask & ~valid_mask) != 0) {
                task->release();
                return (uint64_t)-EINVAL;
            }

            if (std::has_single_bit(mask)) {
                uint64_t target_cpu = std::countr_zero(mask);
                task->cpu = target_cpu;
                task->cpu_pinned = true;
                if (task->schedQueue != mod::sched::task::Task::SchedQueue::WAITING) {
                    mod::sched::reschedule_task_for_cpu(target_cpu, task);
                }
            } else if (mask == valid_mask) {
                task->cpu_pinned = false;
            } else {
                task->release();
                return (uint64_t)-ENOTSUP;
            }

            task->release();
            return 0;
        }

        case abi::multiproc::threadControlOps::getAffinity: {
            uint64_t tid = (uint64_t)arg1;

            auto* task = mod::sched::find_task_by_pid_safe(tid);
            if (task == nullptr) {
                return (uint64_t)-ESRCH;
            }

            uint64_t mask = online_cpu_mask();
            if (mask == 0) {
                task->release();
                return (uint64_t)-ENODEV;
            }
            if (task->cpu_pinned) {
                if (task->cpu >= 64) {
                    task->release();
                    return (uint64_t)-ERANGE;
                }
                mask = 1ULL << task->cpu;
            }

            task->release();
            return mask;
        }

        default:
            mod::dbg::error("Invalid op in syscall thread control");
            return (uint64_t)-EINVAL;
    }
}
}  // namespace ker::syscall::multiproc
