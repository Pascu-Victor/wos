#pragma once

#include <platform/sched/job_control_model.hpp>

namespace ker::mod::sched {
class SignalPendingClaim;
}

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::job_control {

class Membership;
struct Control {
    Membership* members = nullptr;
    uint64_t reported_generation = 0;
    bool enabled = false;
    bool continue_pending = false;
};

enum class StopDelivery { LEGACY, MANAGED, CANCELED };
[[nodiscard]] auto generate(task::Task& subject, uint64_t mask) -> bool;
// With a claim, consume its pending bit and matching membership marker under
// the same resource lock as CONT generation, before any stop gate is published.
[[nodiscard]] auto begin_stop(task::Task& subject, int signal, SignalPendingClaim* claim = nullptr) -> StopDelivery;
[[nodiscard]] auto resume(task::Task& subject) -> std::optional<bool>;
[[nodiscard]] auto legacy_continue_requested(task::Task& subject) -> bool;
void complete_legacy_continue(task::Task& subject);
// Caller has established that a stopped task cannot execute userspace.
void acknowledge_stopped(task::Task& subject);
// Queueing/marking require owner.resource_lock; draining and wakes require no
// scheduler, lifecycle, resource or WKI lock. The work queue retains owners.
void schedule_update_locked(task::Task& owner);
void mark_wakes_locked(task::Task& owner);
void wake_members(task::Task& owner);
void drain();

// Task-owned membership. Declare after resource_owner and the owner's barrier
// so destruction detaches while both the retained owner and its state live.
// Bind/detach require exclusive publication/terminal-exit ownership of the
// subject and must be called outside scheduler/registry/lifecycle/WKI locks.
class Membership {
   public:
    Membership() = default;
    ~Membership();
    Membership(const Membership&) = delete;
    auto operator=(const Membership&) -> Membership& = delete;
    [[nodiscard]] auto bind(task::Task& subject) -> bool;
    // Failed publication restores any local admission gate changed by bind.
    void rollback_publication(task::Task& subject);
    void finish_publication(task::Task& subject);
    void detach();

   private:
    friend struct Access;
    task::Task* m_owner = nullptr;
    task::Task* m_subject = nullptr;
    Membership* m_previous = nullptr;
    Membership* m_next = nullptr;
    Member m_member;
    uint64_t m_previous_job_pending = 0;
    uint64_t m_default_stop_pending = 0;
    uint32_t m_previous_stop_signal = 0;
    bool m_previous_stopped = false;
    bool m_admission_changed = false;
    bool m_visible = false;
    bool m_wake_pending = false;
    bool m_controlled = false;
    bool m_canceled_stop = false;
    bool m_legacy_continue = false;
};

}  // namespace ker::mod::sched::job_control
