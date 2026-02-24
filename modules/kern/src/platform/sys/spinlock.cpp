#include "spinlock.hpp"

namespace ker::mod::sys {
void Spinlock::lock() {
    while (locked.exchange(true, std::memory_order_acquire)) {
        // Spin with pause hint to improve performance and reduce bus traffic
        while (locked.load(std::memory_order_relaxed)) {
            asm volatile("pause");
        }
    }
}

auto Spinlock::try_lock() -> bool {
    // Single attempt to acquire - returns true if acquired, false if already held
    return !locked.exchange(true, std::memory_order_acquire);
}

void Spinlock::unlock() { locked.store(false, std::memory_order_release); }

auto Spinlock::lock_irqsave() -> uint64_t {
    // Save current RFLAGS (contains interrupt flag)
    uint64_t flags = 0;
    asm volatile("pushfq; popq %0" : "=r"(flags));

    // Disable interrupts
    asm volatile("cli");

    // Now acquire the lock
    lock();

    return flags;
}

void Spinlock::unlock_irqrestore(uint64_t flags) {
    // Release the lock first
    unlock();

    // Restore interrupt state - only re-enable if they were enabled before
    if ((flags & 0x200) != 0) {  // IF flag is bit 9
        asm volatile("sti");
    }
}
}  // namespace ker::mod::sys
