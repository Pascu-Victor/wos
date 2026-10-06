#pragma once
#include <cstdint>
#include <platform/sched/task.hpp>
namespace ker::mod::sched {
namespace xfs_read_test {
inline bool queryable{};
inline bool runqueues{};
inline bool shutdown{};
inline thread_local task::Task current{};
}  // namespace xfs_read_test
struct RunQueueStats {
    uint32_t active_task_count{};
};
inline auto get_current_task() -> task::Task* { return xfs_read_test::queryable ? &xfs_read_test::current : nullptr; }
inline auto can_query_current_task() -> bool { return xfs_read_test::queryable; }
inline auto get_run_queue_stats(uint64_t) -> RunQueueStats { return {}; }
inline auto has_run_queues() -> bool { return xfs_read_test::runqueues; }
inline auto kernel_threads_shutdown_requested() -> bool { return xfs_read_test::shutdown; }
inline auto wake_task_by_pid_from_event(uint64_t) -> bool { return false; }
inline constexpr auto saturating_deadline_us(uint64_t base, uint64_t delta) -> uint64_t {
    return UINT64_MAX - base < delta ? UINT64_MAX : base + delta;
}
inline void kern_yield() {}
inline void kern_block() {}
inline void kern_wake(task::Task*) {}
inline void kern_sleep_us(uint64_t) {}
inline void preemptible_syscall_park(const char*, uint64_t = 0) {}
inline void preemptible_syscall_park(const char*, task::WaitChannelKind, uint64_t = 0) {}
inline void set_task_nice(task::Task*, int) {}
}  // namespace ker::mod::sched
