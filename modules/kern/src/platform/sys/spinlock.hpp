#pragma once

#include <atomic>
#include <cstdint>
#include <defines/defines.hpp>

#define SPINLOCK_DEBUG 0

#if SPINLOCK_DEBUG
#include <array>
#endif

namespace ker::mod::sys {

// Ticket spinlock — FIFO fair, prevents starvation.
// Each waiter atomically grabs a ticket; it spins only until its ticket
// is the "now serving" value.  The owner advances "now_serving" on unlock,
// guaranteeing that every waiter eventually wins in arrival order.
struct Spinlock {
    std::atomic<uint32_t> next_ticket{0};
    std::atomic<uint32_t> now_serving{0};

#if SPINLOCK_DEBUG
    // Debug: caller that successfully acquired the lock + its full stack trace.
    void* owner_caller{nullptr};
    static constexpr int SPINLOCK_STACK_DEPTH = 32;
    std::array<void*, SPINLOCK_STACK_DEPTH> owner_stack{};
#endif

    void lock();
    auto try_lock() -> bool;
    void unlock();

    // IRQ-safe variants: disable interrupts before acquiring, restore after releasing
    // Returns the previous interrupt state (RFLAGS) to be passed to unlock_irqrestore
    auto lock_irqsave() -> uint64_t;
    void unlock_irqrestore(uint64_t flags);

    Spinlock() = default;
};
}  // namespace ker::mod::sys
