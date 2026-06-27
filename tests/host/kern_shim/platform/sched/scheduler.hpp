#pragma once

// Host shim: enough scheduler surface for host-compiled kernel units.

#include <cstdint>
#include <platform/sched/task.hpp>

namespace ker::mod::sched {

struct RunQueueStats {
    uint32_t active_task_count = 0;
};

inline auto get_current_task() -> task::Task* { return nullptr; }
inline auto can_query_current_task() -> bool { return false; }
inline auto get_run_queue_stats(uint64_t) -> RunQueueStats { return {}; }
inline auto has_run_queues() -> bool { return false; }
inline auto wake_task_by_pid_from_event(uint64_t) -> bool { return false; }
[[nodiscard]] constexpr auto saturating_deadline_us(uint64_t base_us, uint64_t delta_us) -> uint64_t {
    if (UINT64_MAX - base_us < delta_us) {
        return UINT64_MAX;
    }
    return base_us + delta_us;
}
inline void kern_yield() {}
inline void kern_block() {}
inline void kern_wake(task::Task*) {}
inline void kern_sleep_us(uint64_t) {}
inline void preemptible_syscall_park(const char*, uint64_t = 0) {}
inline void preemptible_syscall_park(const char*, task::WaitChannelKind, uint64_t = 0) {}
inline void set_task_nice(task::Task*, int) {}

}  // namespace ker::mod::sched
