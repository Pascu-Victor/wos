#pragma once

#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sys {

// Only enter before publication/root/IRQ guards. Syscall entry clears IF;
// fallible preparation may reclaim or perform backend I/O in ordinary context.
// Declare this before prepared owners so their cleanup also precedes IF restore.
class SyscallPreparationScope {
   public:
    // Legacy task construction owns exclusive MM gates across allocations and
    // entered with IF clear. Declare after prepared/deferred-cleanup owners,
    // before publication, so their cleanup runs after this interval restores IF.
    class LegacyInterval {
       public:
        explicit LegacyInterval(const SyscallPreparationScope& preparation)
            : restore_enabled_(preparation.ready() && sched::interrupts_enabled()) {
            if (restore_enabled_) asm volatile("cli" ::: "memory");
        }
        ~LegacyInterval() {
            if (restore_enabled_) asm volatile("sti" ::: "memory");
        }
        LegacyInterval(const LegacyInterval&) = delete;
        auto operator=(const LegacyInterval&) -> LegacyInterval& = delete;

       private:
        bool restore_enabled_{};
    };

    explicit SyscallPreparationScope(sched::task::Task& executor) {
        ready_ = sched::can_query_current_task() && sched::get_current_task() == &executor &&
                 executor.type == sched::task::TaskType::PROCESS && executor.syscall_account_start_us != 0 &&
                 executor.state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
                 !executor.scheduler_transition_active.load(std::memory_order_acquire) && sched::preempt_count() == 0;
        if (ready_ && !sched::interrupts_enabled()) {
            restore_disabled_ = true;
            asm volatile("sti" ::: "memory");
        }
    }
    ~SyscallPreparationScope() {
        if (restore_disabled_) asm volatile("cli" ::: "memory");
    }
    SyscallPreparationScope(const SyscallPreparationScope&) = delete;
    auto operator=(const SyscallPreparationScope&) -> SyscallPreparationScope& = delete;
    [[nodiscard]] auto ready() const noexcept -> bool { return ready_; }

   private:
    bool ready_{};
    bool restore_disabled_{};
};

}  // namespace ker::mod::sys
