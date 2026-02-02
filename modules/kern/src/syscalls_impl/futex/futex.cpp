#include "futex.hpp"

#include <abi/callnums/futex.h>
#include <errno.h>

#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::syscall::futex {

// ============================================================================
// Futex wait queue implementation
// ============================================================================

// Maximum number of futex waiters system-wide
static constexpr size_t MAX_FUTEX_WAITERS = 4096;

// A waiter entry in the futex wait queue
struct FutexWaiter {
    uint64_t phys_addr;  // Physical address of the futex (for uniqueness across processes)
    uint64_t task_pid;   // PID of the waiting task
    uint64_t task_cpu;   // CPU the task was running on
    bool active;         // Whether this slot is in use
};

// Global futex wait table
static FutexWaiter futex_waiters[MAX_FUTEX_WAITERS];
static mod::sys::Spinlock futex_lock;
static bool futex_initialized = false;

static void initFutexTable() {
    if (futex_initialized) {
        return;
    }
    for (auto& futex_waiter : futex_waiters) {
        futex_waiter.active = false;
    }
    futex_initialized = true;
}

// ============================================================================
// Syscall dispatcher
// ============================================================================

uint64_t sys_futex(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3) {
    auto futexOp = static_cast<abi::futex::futex_ops>(op);

    switch (futexOp) {
        case abi::futex::futex_ops::futex_wait:
            return static_cast<uint64_t>(futex_wait(reinterpret_cast<int*>(a1), static_cast<int>(a2), reinterpret_cast<const void*>(a3)));

        case abi::futex::futex_ops::futex_wake:
            return static_cast<uint64_t>(futex_wake(reinterpret_cast<int*>(a1)));

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

// ============================================================================
// futex_wait - Block until woken or value changes
// ============================================================================

int64_t futex_wait(int* addr, int expected, const void* timeout) {
    (void)timeout;  // TODO: Implement timeout support

    initFutexTable();

    auto* current_task = mod::sched::getCurrentTask();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address for cross-process uniqueness
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    uint64_t phys_addr = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (phys_addr == 0) {
        return -EFAULT;  // Invalid address
    }

    // Read the current value at the address
    // The address is in user space, so we need to access it via physical mapping
    uint64_t phys_page = phys_addr & ~0xFFFULL;
    uint64_t offset = phys_addr & 0xFFF;

    // Map physical page to read the value (using HHDM - Higher Half Direct Map)
    int* kernel_addr = reinterpret_cast<int*>((uint64_t)mod::mm::addr::getVirtPointer(phys_page) + offset);

    // Atomically check the value and add to wait queue
    futex_lock.lock();

    // Check if value still matches expected
    int current_value = *kernel_addr;
    if (current_value != expected) {
        futex_lock.unlock();
        return -EAGAIN;  // Value changed, don't wait
    }

    // Find a free slot in the wait table
    size_t slot = MAX_FUTEX_WAITERS;
    for (size_t i = 0; i < MAX_FUTEX_WAITERS; i++) {
        if (!futex_waiters[i].active) {
            slot = i;
            break;
        }
    }

    if (slot == MAX_FUTEX_WAITERS) {
        futex_lock.unlock();
        mod::dbg::error("futex_wait: No free slots in wait table");
        return -ENOMEM;
    }

    // Register this task as waiting
    futex_waiters[slot].phys_addr = phys_addr;
    futex_waiters[slot].task_pid = current_task->pid;
    futex_waiters[slot].task_cpu = current_task->cpu;
    futex_waiters[slot].active = true;

    futex_lock.unlock();

    // Set deferred task switch to move task to wait queue
    current_task->deferredTaskSwitch = true;

    // Return 0 - the actual wake will happen when futex_wake is called
    // The task will be rescheduled at that point
    return 0;
}

// ============================================================================
// futex_wake - Wake one or more waiters
// ============================================================================

int64_t futex_wake(int* addr) {  // NOLINT
    initFutexTable();

    auto* current_task = mod::sched::getCurrentTask();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    uint64_t phys_addr = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (phys_addr == 0) {
        return -EFAULT;  // Invalid address
    }

    int woken_count = 0;

    futex_lock.lock();

    // Find and wake all waiters on this address
    for (size_t i = 0; i < MAX_FUTEX_WAITERS; i++) {
        if (futex_waiters[i].active && futex_waiters[i].phys_addr == phys_addr) {
            // Found a waiter - wake it up
            uint64_t waiter_pid = futex_waiters[i].task_pid;
            uint64_t waiter_cpu = futex_waiters[i].task_cpu;

            // Clear the slot first
            futex_waiters[i].active = false;

            // Find the task and reschedule it
            auto* waiter_task = mod::sched::findTaskByPid(waiter_pid);
            if (waiter_task != nullptr) {
                mod::sched::rescheduleTaskForCpu(waiter_cpu, waiter_task);
                woken_count++;
            }
        }
    }

    futex_lock.unlock();

    return woken_count;
}

}  // namespace ker::syscall::futex
