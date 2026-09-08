#pragma once

#include <platform/sched/task.hpp>

namespace ker::mod::sched::resource {
// Retains a thread's process owner. No fallback to a stale per-thread copy.
class Owner {
   public:
    explicit Owner(task::Task* subject);
    ~Owner();
    Owner(const Owner&) = delete;
    auto operator=(const Owner&) -> Owner& = delete;
    auto get() const -> task::Task* { return m_task; }

   private:
    task::Task* m_task = nullptr;
    bool m_retained = false;
};

struct CpuObservation {
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    uint64_t child_user_time_us{};
    uint64_t child_system_time_us{};
};

// Process lifetime counters, including reclaimed threads and imported exec
// history. Caller retains subject; its immutable thread owner keeps the leader
// alive. No usercopy/callbacks, allocation or registry lookup. Must not be called
// under runqueue/WKI/resource/lifecycle locks.
[[nodiscard]] auto copy_cpu_observation(task::Task& subject, CpuObservation& observation) -> bool;

[[nodiscard]] auto inherit(task::Task& child, task::Task& parent) -> bool;
[[nodiscard]] auto close_thread_cpu_admission(task::Task& subject) -> bool;
[[nodiscard]] auto bind_thread_owner(task::Task& thread, task::Task& parent) -> bool;
// IRQ-safe: fixed state/lock and atomic signal enqueue only.
void charge_cpu(task::Task* subject, uint64_t elapsed_us, uint64_t user_us = 0);
auto syscall_limit(uint64_t resource, uint64_t address, uint64_t version, bool set, bool usage = false) -> uint64_t;
}  // namespace ker::mod::sched::resource
