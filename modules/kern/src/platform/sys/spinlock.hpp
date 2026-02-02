#pragma once

#include <atomic>
#include <defines/defines.hpp>

namespace ker::mod::sys {
struct Spinlock {
    std::atomic<bool> locked;
    void lock();
    void unlock();
    Spinlock() : locked(false) {}
};
}  // namespace ker::mod::sys
