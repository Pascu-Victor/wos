#pragma once

#include <platform/ktime/interval_timer_model.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::time::process_timer {
struct Status {
    uint64_t real_depth{};
    uint64_t wake_depth{};
    uint64_t expirations{};
    uint64_t max_late_us{};
    uint64_t work_budget{};
};
// No resource locks; fixed service snapshot. Lateness measures REAL worker delay.
auto status() -> Status;
// Initialize once after scheduler task publication is available.
[[nodiscard]] auto init() -> bool;
// Syscall context, no caller locks. Resolve the retained canonical process owner.
// Return zero or negative errno. Previous state is captured atomically with set.
auto set(sched::task::Task& subject, int which, interval::Value requested, interval::Value* previous = nullptr) -> int;
auto get(sched::task::Task& subject, int which, interval::Value& value) -> int;
// Caller holds canonical owner's resource_lock. Allocation-free and IRQ-safe.
// Fresh counters were already charged. Publish shared pending bits; return true
// when the caller should notify after releasing resource_lock.
auto charge_locked(sched::task::Task& owner) -> bool;
// May be called under runqueue locks, never under resource_lock. Enqueue only;
// recipient lookup and scheduler wake execute in the dedicated worker.
void notify(sched::task::Task& owner);
// Timer-IRQ safe point, outside runqueue locks. Delivers a wake requested by
// notify() without recursively acquiring the runqueue that performed charging.
void service_deferred_wake();
// Committed process exit only, outside locks. Cancels all armed/queued state.
void close(sched::task::Task& owner);
// Caller holds owner's resource_lock after excluding setters and CPU producers.
// Includes armed, shared-pending/claimed and queued/extracted lifetime work.
auto has_active_locked(const sched::task::Task& owner) -> bool;
#ifdef WOS_SELFTEST
auto selftest_service() -> bool;
#endif
}  // namespace ker::mod::time::process_timer
