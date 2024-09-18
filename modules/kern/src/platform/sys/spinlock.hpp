#pragma once

#include <defines/defines.hpp>

namespace ker::mod::sys {
struct Spinlock {
    volatile bool locked;
    void lock();
    void unlock();
};
}  // namespace ker::mod::sys
