#pragma once

#include <defines/defines.hpp>

namespace ker::mod::sys {
struct Spinlock {
    volatile bool locked;
    void lock();
    void unlock();
    Spinlock() : locked(false) {}
};
}  // namespace ker::mod::sys
