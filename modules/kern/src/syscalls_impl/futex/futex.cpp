#include "futex.hpp"

#include <abi/callnums/futex.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>
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

constexpr long NSEC_PER_SEC = 1000000000L;
constexpr uint64_t USEC_PER_SEC = 1000000ULL;

// 256 buckets — per-bucket spinlocks replace the single global lock.
// Default-constructed (no allocation); init() called on first use.
ker::util::IntrHashTable<FutexWaiter, FutexKeyExtract, ker::util::IntHash, ker::util::IntEqual> futex_table;
ker::mod::sys::Spinlock futex_init_lock;
std::atomic<bool> futex_table_initialized{false};

[[nodiscard]] auto ensure_futex_table() -> bool {
    if (__builtin_expect(static_cast<long>(!futex_table_initialized.load(std::memory_order_acquire)), 0) == 0) {
        return true;
    }

    uint64_t const FLAGS = futex_init_lock.lock_irqsave();
    bool initialized = futex_table_initialized.load(std::memory_order_relaxed);
    if (!initialized) {
        initialized = futex_table.init(256);
        futex_table_initialized.store(initialized, std::memory_order_release);
    }
    futex_init_lock.unlock_irqrestore(FLAGS);
    return initialized;
}

auto relative_timeout_us(mod::sched::task::Task& task, const void* timeout, uint64_t& out_us) -> int64_t {
    out_us = 0;
    if (timeout == nullptr) {
        return 0;
    }

    timespec ts{};
    if (!mod::sys::usercopy::copy_value_from_task(task, reinterpret_cast<uint64_t>(timeout), ts)) {
        return -EFAULT;
    }
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -EINVAL;
    }

    auto const NSEC_US = (static_cast<uint64_t>(ts.tv_nsec) + 999ULL) / 1000ULL;
    auto const SEC = static_cast<uint64_t>(ts.tv_sec);
    if (SEC > (UINT64_MAX - NSEC_US) / USEC_PER_SEC) {
        return -EINVAL;
    }

    out_us = (SEC * USEC_PER_SEC) + NSEC_US;
    return 0;
}

auto deadline_from_now_us(uint64_t timeout_us) -> uint64_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    if (UINT64_MAX - NOW_US < timeout_us) {
        return UINT64_MAX;
    }
    return NOW_US + timeout_us;
}

auto futex_addr_is_aligned(const void* addr) -> bool { return (reinterpret_cast<uintptr_t>(addr) % alignof(int)) == 0; }

auto futex_wake_limit_from_count(int count, size_t& out_limit) -> int64_t {
    out_limit = 0;
    if (count < 0) {
        return -EINVAL;
    }
    out_limit = static_cast<size_t>(count);
    return 0;
}

auto claim_task_waiter(mod::sched::task::Task* task, FutexWaiter* waiter) -> bool {
    if (task == nullptr || waiter == nullptr) {
        return false;
    }

    void* expected_waiter = waiter;
    return task->futex_waiter.compare_exchange_strong(expected_waiter, nullptr, std::memory_order_acq_rel, std::memory_order_acquire);
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
            return static_cast<uint64_t>(futex_wake(reinterpret_cast<int*>(a1), static_cast<int>(a2)));

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

// ============================================================================
// futex_wait - Block until woken or value changes
// ============================================================================

int64_t futex_wait(const int* addr, int expected, const void* timeout) {
    if (!futex_addr_is_aligned(addr)) {
        return -EINVAL;
    }
    if (!ensure_futex_table()) {
        return -ENOMEM;
    }

    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address for cross-process uniqueness
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    if (!mod::sys::usercopy::range_valid(user_vaddr, sizeof(int))) {
        return -EFAULT;
    }
    uint64_t const PHYS_ADDR = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (PHYS_ADDR == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    // Read the current value at the address via HHDM
    uint64_t const PHYS_PAGE = PHYS_ADDR & ~0xFFFULL;
    uint64_t const OFFSET = PHYS_ADDR & 0xFFF;
    int const* kernel_addr = reinterpret_cast<int*>(reinterpret_cast<uint64_t>(mod::mm::addr::get_virt_pointer(PHYS_PAGE)) + OFFSET);

    uint64_t timeout_us = 0;
    int64_t const TIMEOUT_STATUS = relative_timeout_us(*current_task, timeout, timeout_us);
    if (TIMEOUT_STATUS != 0) {
        return TIMEOUT_STATUS;
    }

    if (timeout != nullptr && timeout_us == 0) {
        return *kernel_addr == expected ? -ETIMEDOUT : -EAGAIN;
    }

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
        [current_task, waiter, timeout, timeout_us, &previous_waiter]() {
            current_task->set_wait_channel("futex_wait", ker::mod::sched::task::WaitChannelKind::FUTEX);
            current_task->wake_at_us = timeout != nullptr ? deadline_from_now_us(timeout_us) : 0;
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

int64_t futex_wake(int* addr, int count) {  // NOLINT
    size_t wake_limit = 0;
    int64_t const COUNT_STATUS = futex_wake_limit_from_count(count, wake_limit);
    if (COUNT_STATUS != 0 || wake_limit == 0) {
        return COUNT_STATUS;
    }

    if (!futex_addr_is_aligned(addr)) {
        return -EINVAL;
    }

    if (!ensure_futex_table()) {
        return -ENOMEM;
    }
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    // Translate user virtual address to physical address
    auto user_vaddr = reinterpret_cast<uint64_t>(addr);
    if (!mod::sys::usercopy::range_valid(user_vaddr, sizeof(int))) {
        return -EFAULT;
    }
    uint64_t const PHYS_ADDR = mod::mm::virt::translate(current_task->pagemap, user_vaddr);
    if (PHYS_ADDR == ker::mod::mm::virt::PADDR_INVALID) {
        return -EFAULT;  // Invalid address
    }

    int woken_count = 0;

    futex_table.remove_by_key_limit(PHYS_ADDR, wake_limit, [&](FutexWaiter* waiter) -> bool {
        bool own_waiter = true;
        bool claimed_waiter = false;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            if (!claim_task_waiter(waiter_task, waiter)) {
                own_waiter = false;
            } else {
                mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
                claimed_waiter = true;
                woken_count++;
            }
            waiter_task->release();
        }
        if (waiter_task == nullptr || own_waiter) {
            delete waiter;
        }
        return claimed_waiter;
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

int64_t futex_wake_by_phys(uint64_t phys_addr, int count) {
    size_t wake_limit = 0;
    int64_t const COUNT_STATUS = futex_wake_limit_from_count(count, wake_limit);
    if (COUNT_STATUS != 0 || wake_limit == 0) {
        return COUNT_STATUS;
    }

    if (phys_addr == 0) {
        return -EINVAL;
    }

    if (!ensure_futex_table()) {
        return -ENOMEM;
    }

    int woken_count = 0;
    futex_table.remove_by_key_limit(phys_addr, wake_limit, [&](FutexWaiter* waiter) -> bool {
        bool own_waiter = true;
        bool claimed_waiter = false;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            if (!claim_task_waiter(waiter_task, waiter)) {
                own_waiter = false;
            } else {
                mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
                claimed_waiter = true;
                woken_count++;
            }
            waiter_task->release();
        }
        if (waiter_task == nullptr || own_waiter) {
            delete waiter;
        }
        return claimed_waiter;
    });
    return woken_count;
}

#ifdef WOS_SELFTEST
auto futex_selftest_table_init_is_serialized() -> bool {
    bool const FIRST = ensure_futex_table();
    bool const SECOND = ensure_futex_table();
    return FIRST && SECOND && futex_table_initialized.load(std::memory_order_acquire) && futex_table.valid();
}

auto futex_selftest_addr_alignment_guard() -> bool {
    return futex_addr_is_aligned(reinterpret_cast<const int*>(0x1000)) && !futex_addr_is_aligned(reinterpret_cast<const int*>(0x1001));
}

auto futex_selftest_stale_wake_does_not_claim_waiter() -> bool {
    mod::sched::task::Task task{};
    FutexWaiter waiter{};
    FutexWaiter stale{};

    task.futex_waiter.store(&waiter, std::memory_order_release);
    if (!claim_task_waiter(&task, &waiter)) {
        return false;
    }
    if (task.futex_waiter.load(std::memory_order_acquire) != nullptr) {
        return false;
    }

    task.futex_waiter.store(&stale, std::memory_order_release);
    if (claim_task_waiter(&task, &waiter)) {
        return false;
    }

    return task.futex_waiter.load(std::memory_order_acquire) == &stale;
}

auto futex_selftest_wake_count_limit() -> bool {
    size_t limit = 99;
    if (futex_wake_limit_from_count(-1, limit) != -EINVAL || limit != 0) {
        return false;
    }
    if (futex_wake_limit_from_count(0, limit) != 0 || limit != 0) {
        return false;
    }
    if (futex_wake_limit_from_count(3, limit) != 0 || limit != 3) {
        return false;
    }
    return true;
}
#endif

}  // namespace ker::syscall::futex
