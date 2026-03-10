#pragma once

// Sleeping Mutex — blocks the caller when contended instead of spinning.
//
// Uses an atomic flag for the fast uncontended path and falls back to
// kern_yield() to sleep when the lock is held by another task.  A simple
// waiter counter ensures fairness under moderate contention.
//
// This is the mutex primitive needed by XFS which holds inode and AG locks
// across I/O operations that may sleep.
//
// Reference: Linux kernel/locking/mutex.c (simplified)

#include <atomic>
#include <cstdint>

namespace ker::mod::sys {

class Mutex {
   public:
    Mutex() = default;

    // Non-copyable, non-movable
    Mutex(const Mutex&) = delete;
    Mutex(Mutex&&) = delete;
    auto operator=(const Mutex&) -> Mutex& = delete;
    auto operator=(Mutex&&) -> Mutex& = delete;

    // Acquire the mutex.  If already held, the caller sleeps (yields to the
    // scheduler) until the mutex becomes available.
    void lock();

    // Try to acquire the mutex without blocking.
    // Returns true if acquired, false if already held.
    auto try_lock() -> bool;

    // Release the mutex.  Wakes one waiting task (implicitly via the next
    // scheduler tick or kern_yield() wakeup).
    void unlock();

    // Check whether the mutex is currently held (non-authoritative, racy).
    auto is_locked() const -> bool { return held_.load(std::memory_order_relaxed); }

   private:
    std::atomic<bool> held_{false};
    // Number of tasks waiting to acquire.  Used to decide whether to yield
    // aggressively or just retry.
    std::atomic<uint32_t> waiters_{0};
};

// RAII lock guard for Mutex (analogous to std::lock_guard).
class MutexGuard {
   public:
    explicit MutexGuard(Mutex& mtx) : mtx_(mtx) { mtx_.lock(); }
    ~MutexGuard() { mtx_.unlock(); }

    MutexGuard(const MutexGuard&) = delete;
    MutexGuard(MutexGuard&&) = delete;
    auto operator=(const MutexGuard&) -> MutexGuard& = delete;
    auto operator=(MutexGuard&&) -> MutexGuard& = delete;

   private:
    Mutex& mtx_;
};

}  // namespace ker::mod::sys
