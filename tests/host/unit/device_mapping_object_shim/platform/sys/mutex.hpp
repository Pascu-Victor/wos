#pragma once
#include <object_pages.hpp>

namespace ker::mod::sys {
class Mutex {
   public:
    void lock() {
        object_fixture::check(object_fixture::irq_depth == 0);
        mutex.lock();
        ++object_fixture::mutex_depth;
    }
    void unlock() {
        object_fixture::check(object_fixture::mutex_depth != 0);
        --object_fixture::mutex_depth;
        mutex.unlock();
    }

   private:
    std::mutex mutex;
};
class MutexGuard {
   public:
    explicit MutexGuard(Mutex& mutex) : mutex(mutex) { mutex.lock(); }
    ~MutexGuard() { mutex.unlock(); }

   private:
    Mutex& mutex;
};
}  // namespace ker::mod::sys
