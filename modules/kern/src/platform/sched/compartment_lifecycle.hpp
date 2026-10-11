#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::compartment {

inline constexpr size_t EXIT_REQUEST_BUDGET = 16;

// Start once after Host init creation and before user dispatch. Failure is an
// explicit bootstrap failure; no per-init-death worker allocation is needed.
[[nodiscard]] auto init() -> bool;
// Canonical or deferred proxy exit, after exit_in_progress and outside locks.
// Only exact private local PID1 closes views; host/mixed host views are skipped.
[[nodiscard]] auto begin_init_retirement(task::Task& subject) -> int;

struct Status {
    size_t pending{};
    uint64_t completed{};
    uint64_t exit_requests{};
    size_t request_budget{EXIT_REQUEST_BUDGET};
};
[[nodiscard]] auto status() -> Status;

#ifdef WOS_SELFTEST
// Boot tests have no executing worker. Share its actual dispatch and request
// stores; replace only unlocked delivery. Explicit fixture admission permits
// registry-only inert Tasks, never a fabricated runnable frame.
void selftest_dispatch_once(void (*deliver)(task::Task&, void*), void* context, bool include_unpublished_fixtures = false);
#endif

}  // namespace ker::mod::sched::compartment
