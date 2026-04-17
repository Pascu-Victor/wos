#pragma once

// Host shim: replaces kernel ticket spinlock with std::mutex.
// IRQ-safe variants are no-ops on the host (no interrupt masking).

#include <atomic>
#include <cstdint>
#include <mutex>

// Stub out the defines include that the real spinlock.hpp pulls in
#include <defines/defines.hpp>

namespace ker::mod::sys {

struct Spinlock {
    std::atomic<uint32_t> next_ticket{0};
    std::atomic<uint32_t> now_serving{0};

    void lock() { m_mtx.lock(); }
    auto try_lock() -> bool { return m_mtx.try_lock(); }
    void unlock() { m_mtx.unlock(); }

    // IRQ-safe variants: on host, just lock/unlock (no interrupt state)
    auto lock_irqsave() -> uint64_t {
        m_mtx.lock();
        return 0;
    }
    void unlock_irqrestore(uint64_t /*flags*/) { m_mtx.unlock(); }

    Spinlock() = default;

   private:
    std::recursive_mutex m_mtx;  // recursive to handle lock_irqsave after lock
};

}  // namespace ker::mod::sys
