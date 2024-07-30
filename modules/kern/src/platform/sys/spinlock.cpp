#include "spinlock.hpp"

namespace ker::mod::sys {
    void Spinlock::lock() {
        while (__sync_lock_test_and_set(&locked, 1)) {
            asm volatile("pause");
        }
    }

    void Spinlock::unlock() {
        __sync_lock_release(&locked);
    }
}