#include "futex.hpp"

#include <abi/callnums/futex.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <util/hashtable.hpp>

namespace ker::syscall::futex {
namespace {
using log = ker::mod::dbg::logger<"futex">;

// ============================================================================
// Futex wait queue implementation — hash table keyed by physical address
// ============================================================================

struct FutexWaiter {
    uint64_t phys_addr{};              // Physical address of the futex (for uniqueness across processes)
    uint64_t task_pid{};               // PID of the waiting task
    uint64_t task_cpu{};               // CPU the task was running on
    FutexWaiter* hash_next = nullptr;  // intrusive chain for hash table
};

struct FutexKeyExtract {
    uint64_t operator()(const FutexWaiter& w) const { return w.phys_addr; }
};

// 256 buckets — per-bucket spinlocks replace the single global lock.
// Default-constructed (no allocation); init() called on first use.
ker::util::IntrHashTable<FutexWaiter, FutexKeyExtract, ker::util::IntHash, ker::util::IntEqual> futex_table;
bool futex_table_initialized = false;

void ensure_futex_table() {
    if (__builtin_expect(static_cast<long>(!futex_table_initialized), 0) != 0) {
        static_cast<void>(futex_table.init(256));
        futex_table_initialized = true;
    }
}

}  // namespace

// ============================================================================
// Syscall dispatcher
// ============================================================================

uint64_t sys_futex(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3) {
    auto futex_op = static_cast<abi::futex::futex_ops>(op);

    switch (futex_op) {
        case abi::futex::futex_ops::FUTEX_WAIT:
            return static_cast<uint64_t>(futex_wait(reinterpret_cast<int*>(a1), static_cast<int>(a2), reinterpret_cast<const void*>(a3)));

        case abi::futex::futex_ops::FUTEX_WAKE:
            return static_cast<uint64_t>(futex_wake(reinterpret_cast<int*>(a1)));

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

// ============================================================================
// futex_wait - Block until woken or value changes
// ============================================================================

int64_t futex_wait(const int* addr, int expected, const void* timeout) {
    (void)timeout;  // TODO: Implement timeout support
    ensure_futex_table();

    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address for cross-process uniqueness
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    uint64_t const PHYS_ADDR = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (PHYS_ADDR == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    // Read the current value at the address via HHDM
    uint64_t const PHYS_PAGE = PHYS_ADDR & ~0xFFFULL;
    uint64_t const OFFSET = PHYS_ADDR & 0xFFF;
    int const* kernel_addr = reinterpret_cast<int*>(reinterpret_cast<uint64_t>(mod::mm::addr::get_virt_pointer(PHYS_PAGE)) + OFFSET);

    // Allocate a waiter node
    auto* waiter = new (std::nothrow) FutexWaiter{};
    if (waiter == nullptr) {
        return -ENOMEM;
    }
    waiter->phys_addr = PHYS_ADDR;
    waiter->task_pid = current_task->pid;
    waiter->task_cpu = current_task->cpu;
    waiter->hash_next = nullptr;

    if (!futex_table.valid()) {
        delete waiter;
        return -ENOMEM;
    }

    void* previous_waiter = nullptr;
    bool const INSERTED = futex_table.insert_if(
        waiter,
        [kernel_addr, expected]() -> bool {
            int const CURRENT_VALUE = *kernel_addr;
            return CURRENT_VALUE == expected;
        },
        [current_task, waiter, &previous_waiter]() {
            current_task->wait_channel = "futex_wait";
            current_task->deferred_task_switch = true;
            previous_waiter = current_task->futex_waiter.exchange(waiter, std::memory_order_acq_rel);
        });
    if (!INSERTED) {
        delete waiter;
        return -EAGAIN;  // Value changed, don't wait
    }

    if (previous_waiter != nullptr) {
        log::warn("wait: PID %x replaced stale waiter %p with %p", current_task->pid, previous_waiter, waiter);
    }

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
    uint64_t const PHYS_ADDR = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (PHYS_ADDR == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    int woken_count = 0;

    // Remove all waiters on this address and wake them
    futex_table.remove_all_by_key(PHYS_ADDR, [&](FutexWaiter* waiter) {
        bool own_waiter = true;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            void* expected_waiter = waiter;
            if (!waiter_task->futex_waiter.compare_exchange_strong(expected_waiter, nullptr, std::memory_order_acq_rel,
                                                                   std::memory_order_acquire)) {
                own_waiter = false;
            }
            mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
            woken_count++;
            waiter_task->release();
        }
        if (waiter_task == nullptr || own_waiter) {
            delete waiter;
        }
    });

    return woken_count;
}

void futex_wait_cleanup_for_task(mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    auto* waiter = static_cast<FutexWaiter*>(task->futex_waiter.exchange(nullptr, std::memory_order_acq_rel));
    if (waiter == nullptr) {
        return;
    }

    futex_table.remove(waiter);
    delete waiter;
}

int64_t futex_wake_by_phys(uint64_t phys_addr) {
    if (phys_addr == 0) {
        return -EINVAL;
    }
    ensure_futex_table();

    int woken_count = 0;
    futex_table.remove_all_by_key(phys_addr, [&](FutexWaiter* waiter) {
        bool own_waiter = true;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            void* expected_waiter = waiter;
            if (!waiter_task->futex_waiter.compare_exchange_strong(expected_waiter, nullptr, std::memory_order_acq_rel,
                                                                   std::memory_order_acquire)) {
                own_waiter = false;
            }
            mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
            woken_count++;
            waiter_task->release();
        }
        if (waiter_task == nullptr || own_waiter) {
            delete waiter;
        }
    });
    return woken_count;
}

}  // namespace ker::syscall::futex
