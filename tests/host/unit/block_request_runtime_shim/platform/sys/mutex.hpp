#pragma once
#include <cstddef>
#include <functional>
#include <mutex>
#include <utility>

namespace ker::mod::sys {
inline thread_local size_t block_test_service_try_lock_attempts{};
inline thread_local std::function<void()> block_test_service_unlock_hook;
class Mutex {
   public:
    void lock() { lock_.lock(); }
    auto try_lock() -> bool {
        ++block_test_service_try_lock_attempts;
        return lock_.try_lock();
    }
    void unlock() {
        auto hook = std::move(block_test_service_unlock_hook);
        if (hook) hook();
        lock_.unlock();
    }

   private:
    std::mutex lock_;
};
class MutexGuard {
   public:
    explicit MutexGuard(Mutex& lock) : lock_(lock) { lock_.lock(); }
    ~MutexGuard() { lock_.unlock(); }

   private:
    Mutex& lock_;
};
}  // namespace ker::mod::sys
