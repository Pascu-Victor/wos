#pragma once

#include <platform/sched/task.hpp>

namespace ker::mod::sched::resource {
// Caller keeps subject alive. Retains a thread's exact, immutable credential
// owner while ACTIVE; no registry lookup or stale per-thread fallback. A
// nonthread subject remains borrowed, including boot/retirement callers.
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
// Main-thread retirement: atomically exclude future bindings only when idle.
[[nodiscard]] auto close_idle_thread_cpu_admission(task::Task& owner) -> bool;
[[nodiscard]] auto bind_thread_owner(task::Task& thread, task::Task& parent) -> bool;
// The caller keeps the executor and its retained process owner alive. Acquire
// under the VM publication guard; release after commit cleanup/retirement.
// Failed image preparation must never retire siblings or permanently close
// thread admission. This token does not hold a lock between its operations.
class ExecAdmission {
   public:
    ExecAdmission() = default;
    ~ExecAdmission();
    ExecAdmission(const ExecAdmission&) = delete;
    auto operator=(const ExecAdmission&) -> ExecAdmission& = delete;
    [[nodiscard]] auto begin(task::Task& executor) -> bool;
    [[nodiscard]] auto owns(task::Task& executor) const -> bool;
    // Canonical owner only, outside VM/action/registry/resource locks. Failure
    // keeps the rendezvous owned until finish() releases it for old-image return.
    [[nodiscard]] auto quiesce(task::Task& executor, uint64_t timeout_us = 5'000'000) -> bool;
    [[nodiscard]] auto quiesced(task::Task& executor) const -> bool;
    void finish();

   private:
    task::Task* m_owner = nullptr;
};

// Only a current thread at a lock-free user-return boundary or the explicit
// worker exec handoff wait may hold this marker. It must leave before any
// old-image user return, process mutation or noreturn thread retirement.
class ExecSafeWait {
   public:
    explicit ExecSafeWait(task::Task& subject);
    ~ExecSafeWait();
    ExecSafeWait(const ExecSafeWait&) = delete;
    auto operator=(const ExecSafeWait&) -> ExecSafeWait& = delete;
    [[nodiscard]] auto active() const -> bool { return m_subject != nullptr; }
    // Check the current epoch and withdraw the marker in one resource-lock
    // scope, so rollback followed by another exec cannot reuse a leaving wait.
    [[nodiscard]] auto finish_if_released() -> bool;
    // Fatal/group-exit readiness may return with the marker still owned; the
    // caller must consume that exit before returning to old-image userspace.
    void wait_for_release();

   private:
    task::Task* m_subject = nullptr;
};
// Caller has established a safe live user-return boundary. Returns on rollback
// or fatal/group-exit readiness; the caller consumes exit before user return.
void wait_for_exec_quiescence(task::Task& subject);
// IRQ-safe: fixed state/lock and atomic signal enqueue only.
void charge_cpu(task::Task* subject, uint64_t elapsed_us, uint64_t user_us = 0);
auto syscall_limit(uint64_t resource, uint64_t address, uint64_t version, bool set, bool usage = false) -> uint64_t;
}  // namespace ker::mod::sched::resource
