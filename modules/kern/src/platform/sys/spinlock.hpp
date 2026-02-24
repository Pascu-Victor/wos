#pragma once

#include <atomic>
#include <cstdint>
#include <defines/defines.hpp>

namespace ker::mod::sys {
struct Spinlock {
    std::atomic<bool> locked;
    void lock();
    auto try_lock() -> bool;
    void unlock();

    // IRQ-safe variants: disable interrupts before acquiring, restore after releasing
    // Returns the previous interrupt state (RFLAGS) to be passed to unlock_irqrestore
    auto lock_irqsave() -> uint64_t;
    void unlock_irqrestore(uint64_t flags);

    Spinlock() : locked(false) {}
};
}  // namespace ker::mod::sys
