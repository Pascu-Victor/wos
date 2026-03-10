// Read-Write Lock implementation.
//
// State is encoded in a single atomic uint32:
//   - bits [30:0]: active reader count
//   - bit 31: writer-active flag
//
// A separate `write_waiters_` counter implements writer-preference:
// when one or more writers are waiting, new readers block until all
// pending writers have been served.  This prevents writer starvation
// on read-heavy workloads (common in XFS where many concurrent reads
// hit the same AG/inode).
//
// Contended paths use kern_yield() to sleep, matching the WOS blocking
// convention (see mutex.cpp).

#include "rwlock.hpp"

#include <platform/sched/scheduler.hpp>

namespace ker::mod::sys {

// ---------------------------------------------------------------------------
// Readers
// ---------------------------------------------------------------------------

void RwLock::read_lock() {
    // Fast path: no writer active and no writers waiting → increment readers
    constexpr int SPIN_LIMIT = 64;

    while (true) {
        // Brief spin
        for (int i = 0; i < SPIN_LIMIT; i++) {
            uint32_t s = state_.load(std::memory_order_acquire);
            // Block if a writer is active OR writers are waiting (writer-preference)
            if ((s & WRITER_BIT) == 0 && write_waiters_.load(std::memory_order_acquire) == 0) {
                uint32_t desired = s + 1;
                if (state_.compare_exchange_weak(s, desired, std::memory_order_acquire, std::memory_order_relaxed)) {
                    return;
                }
            }
            asm volatile("pause");
        }
        sched::kern_yield();
    }
}

auto RwLock::read_try_lock() -> bool {
    uint32_t s = state_.load(std::memory_order_acquire);
    if ((s & WRITER_BIT) != 0 || write_waiters_.load(std::memory_order_acquire) != 0) {
        return false;
    }
    uint32_t desired = s + 1;
    return state_.compare_exchange_strong(s, desired, std::memory_order_acquire, std::memory_order_relaxed);
}

void RwLock::read_unlock() { state_.fetch_sub(1, std::memory_order_release); }

// ---------------------------------------------------------------------------
// Writers
// ---------------------------------------------------------------------------

void RwLock::write_lock() {
    // Signal readers that a writer is waiting → prevents new readers
    write_waiters_.fetch_add(1, std::memory_order_release);

    constexpr int SPIN_LIMIT = 64;

    while (true) {
        // Try to set WRITER_BIT when state == 0 (no readers, no other writer)
        for (int i = 0; i < SPIN_LIMIT; i++) {
            uint32_t expected = 0;
            if (state_.compare_exchange_weak(expected, WRITER_BIT, std::memory_order_acquire, std::memory_order_relaxed)) {
                write_waiters_.fetch_sub(1, std::memory_order_relaxed);
                return;
            }
            asm volatile("pause");
        }
        sched::kern_yield();
    }
}

auto RwLock::write_try_lock() -> bool {
    uint32_t expected = 0;
    if (state_.compare_exchange_strong(expected, WRITER_BIT, std::memory_order_acquire, std::memory_order_relaxed)) {
        return true;
    }
    return false;
}

void RwLock::write_unlock() { state_.fetch_and(~WRITER_BIT, std::memory_order_release); }

}  // namespace ker::mod::sys
