#pragma once

// Host shim: enough scheduler surface for host-compiled kernel units.

#include <cstdint>
#include <platform/sched/task.hpp>

namespace ker::mod::sched {

struct RunQueueStats {
    uint32_t active_task_count = 0;
};

inline auto get_current_task() -> task::Task* { return nullptr; }
inline auto get_run_queue_stats(uint64_t) -> RunQueueStats { return {}; }
inline void kern_yield() {}
inline void kern_block() {}
inline void kern_wake(task::Task*) {}
inline void kern_sleep_us(uint64_t) {}
inline void set_task_nice(task::Task*, int) {}

}  // namespace ker::mod::sched
