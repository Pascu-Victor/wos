#pragma once

#include <atomic>
#include <cstdint>

namespace ker::mod::sched {

// Stack-owned resources that cooperative DAEMON exit must not abandon.
// This does not disable preemption, interrupts, or voluntary scheduling.
class ShutdownOwnership {
    std::atomic<uint32_t> depth_{0};

   public:
    void acquire() { depth_.fetch_add(1, std::memory_order_acq_rel); }
    void release() { depth_.fetch_sub(1, std::memory_order_release); }
    [[nodiscard]] auto held() const -> bool { return depth_.load(std::memory_order_acquire) != 0; }
};

class KernelShutdownHold {
    ShutdownOwnership* ownership_;

   public:
    explicit KernelShutdownHold(ShutdownOwnership* ownership) : ownership_(ownership) {
        if (ownership_ != nullptr) ownership_->acquire();
    }
    ~KernelShutdownHold() {
        if (ownership_ != nullptr) ownership_->release();
    }
    KernelShutdownHold(const KernelShutdownHold&) = delete;
    auto operator=(const KernelShutdownHold&) -> KernelShutdownHold& = delete;
};

[[nodiscard]] constexpr auto cooperative_exit_deferred(uint32_t preempt_depth, bool interrupts_enabled, bool unpublished_process,
                                                       bool stack_owned_resources) -> bool {
    return preempt_depth != 0 || !interrupts_enabled || unpublished_process || stack_owned_resources;
}
}  // namespace ker::mod::sched
