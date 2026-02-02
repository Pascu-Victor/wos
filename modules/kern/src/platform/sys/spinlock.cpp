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

void Spinlock::unlock() { locked.store(false, std::memory_order_release); }
}  // namespace ker::mod::sys
