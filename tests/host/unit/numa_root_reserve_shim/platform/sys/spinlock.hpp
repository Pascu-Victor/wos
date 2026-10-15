#pragma once
#include <cstdint>
#include <mutex>
namespace ker::mod::sys {
// Deliberately nonrecursive. Host bridge/zero seams reject calls under this
// lock, while the actual allocator retains its real WOS_HOST_TEST atomic lock.
inline thread_local uint32_t host_spinlocks_held{};
struct Spinlock {
    std::mutex mutex;
    auto lock_irqsave() -> uint64_t {
        mutex.lock();
        ++host_spinlocks_held;
        return 0;
    }
    void unlock_irqrestore(uint64_t) {
        --host_spinlocks_held;
        mutex.unlock();
    }
};
}  // namespace ker::mod::sys
