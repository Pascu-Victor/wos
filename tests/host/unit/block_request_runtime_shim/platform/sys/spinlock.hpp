#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>

namespace ker::mod::sys {
inline thread_local size_t block_test_irqsave_acquisitions{};
inline thread_local size_t block_test_spinlock_depth{};
inline thread_local std::function<void()> block_test_irqsave_hook;
class Spinlock {
   public:
    void lock() {
        lock_.lock();
        ++block_test_spinlock_depth;
    }
    auto try_lock() -> bool {
        if (!lock_.try_lock()) return false;
        ++block_test_spinlock_depth;
        return true;
    }
    void unlock() {
        if (block_test_spinlock_depth == 0) __builtin_trap();
        --block_test_spinlock_depth;
        lock_.unlock();
    }
    auto lock_irqsave() -> uint64_t {
        if (block_test_irqsave_hook) block_test_irqsave_hook();
        lock();
        ++block_test_irqsave_acquisitions;
        return 0;
    }
    void unlock_irqrestore(uint64_t) { unlock(); }

   private:
    std::mutex lock_;  // Deliberately nonrecursive, to expose nested guards.
};
}  // namespace ker::mod::sys
