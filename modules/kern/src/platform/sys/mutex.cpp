// Sleeping Mutex implementation.
//
// Fast path: atomic CAS on the `held_` flag — if uncontested, lock/unlock
// are a single atomic operation with no scheduler involvement.
//
// Slow path: when the lock is held, the caller increments `waiters_` and
// enters a kern_yield() loop.  kern_yield() gives up the CPU (sti;hlt)
// so the task sleeps until the next timer interrupt or IPI wakes it.
// On unlock, the holder simply clears `held_` — the next scheduler tick
// will wake sleeping waiters.
//
// This design is deliberately simple and matches the existing WOS blocking
// pattern used by networking and I/O code.  A more sophisticated futex-style
// wait queue can replace the yield loop later if metrics warrant it.

#include "mutex.hpp"

#include <platform/sched/scheduler.hpp>

namespace ker::mod::sys {

void Mutex::lock() {
    // Fast path — try to acquire immediately
    bool expected = false;
    if (held_.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        return;
    }

    // Slow path — contended
    waiters_.fetch_add(1, std::memory_order_relaxed);

    // Brief spin before yielding (avoids a full context switch for very short
    // critical sections).
    constexpr int SPIN_LIMIT = 64;
    for (int i = 0; i < SPIN_LIMIT; i++) {
        expected = false;
        if (held_.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            waiters_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }
        asm volatile("pause");
    }

    // Yield loop — surrender the CPU and retry after wakeup
    while (true) {
        sched::kern_yield();

        expected = false;
        if (held_.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            waiters_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }
    }
}

auto Mutex::try_lock() -> bool {
    bool expected = false;
    return held_.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed);
}

void Mutex::unlock() {
    held_.store(false, std::memory_order_release);

    // If there are waiters, the next timer tick / kern_yield() wakeup will
    // let them retry.  For latency-sensitive paths we could send an IPI here,
    // but that's not needed for the XFS use-case.
}

}  // namespace ker::mod::sys
