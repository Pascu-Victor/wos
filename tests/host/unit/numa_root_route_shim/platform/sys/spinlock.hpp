#pragma once
#include <cstdint>
#include <mutex>
namespace ker::mod::sys {
inline thread_local uint32_t host_spinlocks_held{};
struct Spinlock {
    std::mutex mutex;
    void lock() {
        mutex.lock();
        ++host_spinlocks_held;
    }
    auto try_lock() -> bool {
        if (!mutex.try_lock()) return false;
        ++host_spinlocks_held;
        return true;
    }
    void unlock() {
        --host_spinlocks_held;
        mutex.unlock();
    }
    auto lock_irqsave() -> uint64_t {
        lock();
        return 0;
    }
    void unlock_irqrestore(uint64_t) { unlock(); }
};
}  // namespace ker::mod::sys
