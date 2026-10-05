#pragma once

#include <platform/sched/migration_guard.hpp>

namespace ker::mod::sched {

// Coalesce local event notifications during one bounded kernel-worker operation.
// Targets are made runnable immediately; the outermost scope requests scheduling
// before its caller can park. Timer preemption and non-event rechecks remain live.
class LocalWakeBatch {
   public:
    LocalWakeBatch();
    ~LocalWakeBatch();

    LocalWakeBatch(const LocalWakeBatch&) = delete;
    LocalWakeBatch(LocalWakeBatch&&) = delete;
    auto operator=(const LocalWakeBatch&) -> LocalWakeBatch& = delete;
    auto operator=(LocalWakeBatch&&) -> LocalWakeBatch& = delete;

   private:
    MigrationGuard cpu_guard_;
    task::Task* task_{};
};

}  // namespace ker::mod::sched
