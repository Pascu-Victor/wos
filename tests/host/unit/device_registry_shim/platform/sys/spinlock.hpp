#pragma once

#include <cstdint>
#include <mutex>

namespace ker::mod::sys {
inline thread_local unsigned registry_test_lock_depth = 0;
// Deliberately nonrecursive: accounting reentry must happen after unlock.
struct Spinlock {
    auto lock_irqsave() -> uint64_t {
        mutex.lock();
        ++registry_test_lock_depth;
        return 0;
    }
    void unlock_irqrestore(uint64_t) {
        --registry_test_lock_depth;
        mutex.unlock();
    }

   private:
    std::mutex mutex;
};
}  // namespace ker::mod::sys
