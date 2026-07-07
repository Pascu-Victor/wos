#pragma once

// Sleeping Mutex - blocks the caller when contended instead of spinning.
//
// Uses an atomic flag for the fast uncontended path and a short timed park
// when the lock is held by another task.  A simple waiter counter feeds
// diagnostics; unlock is just a release, so no waiter lifetime is coupled to a
// stack-local queue node.
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

    // Release the mutex.  Waiters observe this on their next timed wake or
    // scheduler tick.
    void unlock();

    // Check whether the mutex is currently held (non-authoritative, racy).
    auto is_locked() const -> bool { return held.load(std::memory_order_relaxed); }

   private:
    struct SlowPathStats {
        uint32_t wait_count = 0;
        uint32_t park_count = 0;
        uint32_t fallback_count = 0;
        uint32_t fallback_no_query = 0;
        uint32_t fallback_no_task = 0;
        uint32_t fallback_preempt = 0;
    };

    void record_owner(uint64_t acquire_site);
    void clear_owner();
    void maybe_log_stall(uint64_t waiter_site, uint64_t wait_start_us, const SlowPathStats& stats);

    std::atomic<bool> held{false};
    // Number of tasks waiting to acquire.  Diagnostic only.
    std::atomic<uint32_t> waiters{0};
    std::atomic<uint64_t> owner_pid{0};
    std::atomic<const char*> owner_name{nullptr};
    std::atomic<uint64_t> owner_acquire_site{0};
    std::atomic<uint64_t> last_stall_log_us{0};
};

// RAII lock guard for Mutex (analogous to std::lock_guard).
class MutexGuard {
   public:
    explicit MutexGuard(Mutex& mtx) : mtx(mtx) { mtx.lock(); }
    ~MutexGuard() { mtx.unlock(); }

    MutexGuard(const MutexGuard&) = delete;
    MutexGuard(MutexGuard&&) = delete;
    auto operator=(const MutexGuard&) -> MutexGuard& = delete;
    auto operator=(MutexGuard&&) -> MutexGuard& = delete;

   private:
    Mutex& mtx;
};

}  // namespace ker::mod::sys
