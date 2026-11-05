#pragma once
#include <cassert>
#include <cstdint>
#include <mutex>
#include <test_backend.hpp>
namespace ker::mod::sys {
class Spinlock {
   public:
    auto lock_irqsave() -> uint64_t {
        assert(pci_runtime::lock_depth == 0);
        mutex_.lock();
        ++pci_runtime::lock_depth;
        ++pci_runtime::lock_acquisitions;
        return 0;
    }
    void unlock_irqrestore(uint64_t) {
        assert(pci_runtime::lock_depth == 1);
        --pci_runtime::lock_depth;
        mutex_.unlock();
    }

   private:
    std::mutex mutex_;
};
}  // namespace ker::mod::sys
