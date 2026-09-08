#include "resource.hpp"

#include <abi/resource.hpp>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/resource_model.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

namespace ker::mod::sched::resource {
LifetimeOwner::~LifetimeOwner() {
    finish_cpu();
    if (m_owner != nullptr) {
        m_owner->resource_thread_owners.fetch_sub(1, std::memory_order_acq_rel);
        m_owner->release();
    }
}

auto LifetimeOwner::attach(task::Task* owner) -> bool {
    if (m_owner != nullptr || owner == nullptr || !owner->try_acquire()) {
        return false;
    }
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    if (owner->resource_cpu_admission_closed) {
        owner->resource_lock.unlock_irqrestore(FLAGS);
        owner->release();
        return false;
    }
    m_owner = owner;
    m_cpu_active = true;
    owner->resource_cpu_threads.fetch_add(1, std::memory_order_relaxed);
    owner->resource_thread_owners.fetch_add(1, std::memory_order_acq_rel);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return true;
}

void LifetimeOwner::finish_cpu() {
    if (m_cpu_active) {
        m_cpu_active = false;
        m_owner->resource_cpu_threads.fetch_sub(1, std::memory_order_acq_rel);
    }
}

auto close_thread_cpu_admission(task::Task& subject) -> bool {
    auto* owner = subject.is_thread ? subject.resource_owner.get() : &subject;
    if (owner == nullptr) return false;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    owner->resource_cpu_admission_closed = true;
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto bind_thread_owner(task::Task& thread, task::Task& parent) -> bool {
    Owner owner(&parent);
    return thread.is_thread && owner.get() != &thread && thread.owner_pid != 0 && owner.get() != nullptr &&
           owner.get()->pid == thread.owner_pid && thread.resource_owner.attach(owner.get());
}

void charge_cpu(task::Task* subject, uint64_t elapsed_us, uint64_t user_us) {
    if (subject == nullptr || subject->type != task::TaskType::PROCESS || elapsed_us == 0 ||
        subject->cpu_accounting_finalized.load(std::memory_order_acquire)) {
        return;
    }
    auto* process = subject->is_thread ? subject->resource_owner.get() : subject;
    if (process == nullptr) {
        // Unbound user threads must never be published. Fail closed if that
        // lifecycle invariant is violated, without looking up a task in IRQ.
        subject->pending_cpu_signals.fetch_or(uint64_t{1} << 8, std::memory_order_release);
        return;
    }
    uint64_t const FLAGS = process->resource_lock.lock_irqsave();
    auto const ACTION = process->resources.charge_cpu(elapsed_us, user_us);
    process->resource_lock.unlock_irqrestore(FLAGS);
    auto const SERVICE_ACTION = charge_service_cpu(*process, elapsed_us);
    if (ACTION == CpuAction::HARD_KILL || SERVICE_ACTION == CpuAction::HARD_KILL) {
        subject->pending_cpu_signals.fetch_or(uint64_t{1} << 8, std::memory_order_release);  // SIGKILL 9
    } else if (ACTION == CpuAction::SOFT_SIGNAL || SERVICE_ACTION == CpuAction::SOFT_SIGNAL) {
        subject->pending_cpu_signals.fetch_or(uint64_t{1} << 23, std::memory_order_release);  // SIGXCPU 24
    }
}

Owner::Owner(task::Task* subject) : m_task(subject) {
    if (subject != nullptr && subject->is_thread && subject->owner_pid != 0 && subject->owner_pid != subject->pid) {
        m_task = find_task_by_pid_safe(subject->owner_pid);
        m_retained = m_task != nullptr;
    }
}

Owner::~Owner() {
    if (m_retained) {
        m_task->release();
    }
}

auto copy_cpu_observation(task::Task& subject, CpuObservation& observation) -> bool {
    auto* process = subject.is_thread ? subject.resource_owner.get() : &subject;
    if (process == nullptr || process->type != task::TaskType::PROCESS) return false;
    CpuObservation result{};
    uint64_t const FLAGS = process->resource_lock.lock_irqsave();
    result.user_time_us = process->resources.cpu_user_time_us;
    result.system_time_us = process->resources.cpu_system_time_us;
    process->resource_lock.unlock_irqrestore(FLAGS);
    auto const REAPED = ker::syscall::process::child_events::read_reaped_cpu_times(*process);
    result.child_user_time_us = REAPED.user_time_us;
    result.child_system_time_us = REAPED.system_time_us;
    observation = result;
    return true;
}

auto inherit(task::Task& child, task::Task& parent) -> bool {
    Owner owner(&parent);
    if (owner.get() == nullptr) {
        return false;
    }
    auto& source = *owner.get();
    uint64_t const FLAGS = source.resource_lock.lock_irqsave();
    child.resources.inherit_limits(source.resources);
    source.resource_lock.unlock_irqrestore(FLAGS);
    inherit_service(child, source);
    return true;
}

auto syscall_limit(uint64_t resource, uint64_t address, uint64_t version, bool set, bool usage) -> uint64_t {
    if (resource >= abi::resource::COUNT || version != abi::resource::VERSION) {
        return static_cast<uint64_t>(-EINVAL);
    }
    auto* caller = get_current_task();
    if (caller == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    Limit value{};
    if (set && !sys::usercopy::copy_value_from_task(*caller, address, value)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    int result = 0;
    Usage snapshot{};
    {
        // Usercopy may fault or block. Keep the retained task reference out
        // of that region: fatal syscall exit does not unwind C++ destructors.
        bool const IS_AS = resource == static_cast<unsigned>(Id::AS);
        std::optional<ker::syscall::vmem::SharedVmemPublicationGuard> publication_guard;
        if (IS_AS) {
            publication_guard.emplace();
            if (usage && !observe_address_space(*caller)) {
                return static_cast<uint64_t>(-ESRCH);
            }
        }
        Owner owner(caller);
        auto* process = owner.get();
        if (process == nullptr) {
            return static_cast<uint64_t>(-ESRCH);
        }
        bool const IS_FD = resource == static_cast<unsigned>(Id::NOFILE);
        uint64_t const FD_FLAGS = IS_FD ? process->fd_table_lock.lock_irqsave() : 0;
        uint64_t const FLAGS = process->resource_lock.lock_irqsave();
        if (IS_FD) {
            auto& entry = process->resources.entries[resource];
            entry.current = process->fd_table.size() + process->fd_table.reserved_count();
            if (entry.current > entry.peak) {
                entry.peak = entry.current;
            }
        }
        if (set) {
            result = process->resources.set(static_cast<unsigned>(resource), value, caller->euid == 0, task::Task::FD_TABLE_SIZE);
        }
        snapshot = process->resources.entries[resource];
        process->resource_lock.unlock_irqrestore(FLAGS);
        if (IS_FD) {
            process->fd_table_lock.unlock_irqrestore(FD_FLAGS);
        }
        if (IS_AS && usage) {
            copy_address_space_observation(*caller, snapshot);
        }
        if (resource == static_cast<unsigned>(Id::NPROC) && usage) {
            copy_process_observation(*caller, snapshot);
        }
    }
    if (result != 0 || set) {
        return static_cast<uint64_t>(result);
    }
    bool const COPIED = usage ? sys::usercopy::copy_value_to_task(*caller, address, snapshot)
                              : sys::usercopy::copy_value_to_task(*caller, address, snapshot.limit);
    return COPIED ? 0 : static_cast<uint64_t>(-EFAULT);
}
}  // namespace ker::mod::sched::resource
