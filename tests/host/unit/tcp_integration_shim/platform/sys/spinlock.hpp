#pragma once

#include <cassert>
#include <cstdint>

// The TCP harness deliberately rejects recursion and checks every packet
// allocation/emission seam against this depth. It runs on one host thread.
inline unsigned tcp_test_lock_depth = 0;
inline void (*tcp_test_before_lock)(const void*) = nullptr;

namespace ker::mod::sys {
struct Spinlock {
    bool held = false;
    void lock() {
        if (tcp_test_before_lock != nullptr) {
            tcp_test_before_lock(this);
        }
        assert(!held);
        held = true;
        ++tcp_test_lock_depth;
    }
    auto try_lock() -> bool {
        if (held) {
            return false;
        }
        lock();
        return true;
    }
    void unlock() {
        assert(held && tcp_test_lock_depth != 0);
        held = false;
        --tcp_test_lock_depth;
    }
    auto lock_irqsave() -> uint64_t {
        lock();
        return 0;
    }
    void unlock_irqrestore(uint64_t) { unlock(); }
};
}  // namespace ker::mod::sys
