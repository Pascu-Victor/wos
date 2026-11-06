#pragma once

#include <cstdint>
#include <mutex>

namespace ker::mod::sys {
// Finite per-object hardware lock port. Definitions live in the composed host
// ports TU and preserve nested IRQ restoration/lock ownership. In particular,
// a TX lock may enclose the independent DMA registry lock. No global depth==0
// shortcut replaces the production algorithms' actual locks or admission.
class Spinlock {
    std::mutex mutex_;

   public:
    Spinlock() = default;
    void lock();
    [[nodiscard]] auto try_lock() -> bool;
    void unlock();
    [[nodiscard]] auto lock_irqsave() -> uint64_t;
    void unlock_irqrestore(uint64_t flags);
};
}  // namespace ker::mod::sys
