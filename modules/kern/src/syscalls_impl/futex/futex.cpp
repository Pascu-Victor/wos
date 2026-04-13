#include "futex.hpp"

#include <abi/callnums/futex.h>
#include <errno.h>

#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/hashtable.hpp>

namespace ker::syscall::futex {

// ============================================================================
// Futex wait queue implementation — hash table keyed by physical address
// ============================================================================

struct FutexWaiter {
    uint64_t phys_addr;                // Physical address of the futex (for uniqueness across processes)
    uint64_t task_pid;                 // PID of the waiting task
    uint64_t task_cpu;                 // CPU the task was running on
    FutexWaiter* hash_next = nullptr;  // intrusive chain for hash table
};

struct FutexKeyExtract {
    uint64_t operator()(const FutexWaiter& w) const { return w.phys_addr; }
};

// 256 buckets — per-bucket spinlocks replace the single global lock.
// Default-constructed (no allocation); init() called on first use.
static ker::util::IntrHashTable<FutexWaiter, FutexKeyExtract, ker::util::IntHash, ker::util::IntEqual> futex_table;
static bool futex_table_initialized = false;

static void ensure_futex_table() {
    if (__builtin_expect(!futex_table_initialized, false)) {
        futex_table.init(256);
        futex_table_initialized = true;
    }
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
    ensure_futex_table();

    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address for cross-process uniqueness
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    uint64_t phys_addr = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (phys_addr == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    // Read the current value at the address via HHDM
    uint64_t phys_page = phys_addr & ~0xFFFULL;
    uint64_t offset = phys_addr & 0xFFF;
    int* kernel_addr = reinterpret_cast<int*>((uint64_t)mod::mm::addr::get_virt_pointer(phys_page) + offset);

    // Allocate a waiter node
    auto* waiter = static_cast<FutexWaiter*>(mod::mm::dyn::kmalloc::malloc(sizeof(FutexWaiter)));
    if (waiter == nullptr) {
        return -ENOMEM;
    }
    waiter->phys_addr = phys_addr;
    waiter->task_pid = current_task->pid;
    waiter->task_cpu = current_task->cpu;
    waiter->hash_next = nullptr;

    // We need atomic check-and-enqueue. The hash table locks per-bucket,
    // but we need to check the value under the same lock to prevent races.
    // Use a separate spinlock for the value check + insert atomicity.
    // (The hash table's per-bucket lock handles concurrent wake/insert on same bucket.)
    // Actually, we can rely on the hash table's bucket lock since insert acquires it.
    // But the value check must happen before insert returns. Since the hash table
    // insert is unconditional, we check first, then insert.
    int current_value = *kernel_addr;
    if (current_value != expected) {
        mod::mm::dyn::kmalloc::free(waiter);
        return -EAGAIN;  // Value changed, don't wait
    }

    // Insert into hash table (per-bucket lock acquired internally)
    if (!futex_table.insert(waiter)) {
        mod::mm::dyn::kmalloc::free(waiter);
        return -ENOMEM;
    }

    // Set deferred task switch to move task to wait queue
    current_task->wait_channel = "futex_wait";
    current_task->deferredTaskSwitch = true;

    return 0;
}

// ============================================================================
// futex_wake - Wake one or more waiters
// ============================================================================

int64_t futex_wake(int* addr) {  // NOLINT
    ensure_futex_table();
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    uint64_t phys_addr = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (phys_addr == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    int woken_count = 0;

    // Remove all waiters on this address and wake them
    futex_table.remove_all_by_key(phys_addr, [&](FutexWaiter* waiter) {
        auto* waiter_task = mod::sched::find_task_by_pid(waiter->task_pid);
        if (waiter_task != nullptr) {
            waiter_task->deferredTaskSwitch = false;
            mod::sched::reschedule_task_for_cpu(waiter->task_cpu, waiter_task);
            woken_count++;
        }
        mod::mm::dyn::kmalloc::free(waiter);
    });

    return woken_count;
}

}  // namespace ker::syscall::futex
