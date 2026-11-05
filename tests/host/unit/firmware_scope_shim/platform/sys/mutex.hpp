#pragma once
#include <atomic>
#include <mutex>
namespace firmware_host {
inline std::atomic<unsigned> locks{0};
}
namespace ker::mod::sys {
// Real nonrecursive host mutex; this test does not simulate kernel scheduling.
class Mutex {
    std::mutex mutex_;

   public:
    void lock() {
        ++firmware_host::locks;
        mutex_.lock();
    }
    void unlock() { mutex_.unlock(); }
};
class MutexGuard {
    Mutex& mutex_;

   public:
    explicit MutexGuard(Mutex& mutex) : mutex_(mutex) { mutex_.lock(); }
    ~MutexGuard() { mutex_.unlock(); }
};
}  // namespace ker::mod::sys
