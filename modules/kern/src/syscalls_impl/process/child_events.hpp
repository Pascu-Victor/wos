#pragma once

#include <cstdint>
#include <platform/sched/task.hpp>

namespace ker::syscall::process::child_events {

namespace sched_task = ker::mod::sched::task;

constexpr uint64_t SELECT_ANY = UINT64_MAX;
constexpr uint64_t SELECT_PROCESS_GROUP = 1ULL << 63U;
constexpr uint64_t SELECT_PROCESS_GROUP_MASK = SELECT_PROCESS_GROUP - 1U;

constexpr int32_t OPTION_NOHANG = 1;
constexpr int32_t OPTION_STOPPED = 2;
constexpr int32_t OPTION_EXITED = 4;
constexpr int32_t OPTION_CONTINUED = 8;
constexpr int32_t OPTION_NOWAIT = 0x01000000;

struct ClaimedEvent {
    sched_task::ChildEvent* node{};
    uint64_t claim_cookie{};
    uint64_t subject_pid{};
    uint64_t process_group{};
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    int32_t status{};
    sched_task::ChildEventKind kind{sched_task::ChildEventKind::EXIT};
    sched_task::ChildEventAudience audience{sched_task::ChildEventAudience::PARENT};
};

struct Diagnostics {
    uint64_t registered_selector{};
    uint64_t owned_event_count{};
    uint64_t owned_waiter_count{};
    sched_task::ChildMembershipState membership{sched_task::ChildMembershipState::UNLINKED};
    bool claim_active{};
};

enum class ProbeResult : uint8_t {
    CLAIMED,
    WOULD_BLOCK,
    NO_CHILD,
};

// Returns the process leader with a lifetime reference owned by the caller.
[[nodiscard]] auto acquire_process_owner(sched_task::Task& task) -> sched_task::Task*;

[[nodiscard]] auto selector_for_process_group(uint64_t process_group) -> uint64_t;
[[nodiscard]] auto selector_for_current_process_group(sched_task::Task& process) -> uint64_t;
[[nodiscard]] auto selector_matches(uint64_t selector, uint64_t pid, uint64_t process_group) -> bool;
// Route runtime setpgid/setsid changes through the lifecycle lock so a wait
// selector and an immutable event snapshot observe one coherent group value.
void update_process_group(sched_task::Task& process, uint64_t process_group);

// Publication is a two-phase transaction: begin establishes membership before
// scheduler/WKI visibility; commit marks it live; abort removes the unpublished
// child without exposing a waitable event.
[[nodiscard]] auto begin_publication(sched_task::Task& creator, sched_task::Task& child) -> bool;
[[nodiscard]] auto commit_publication(sched_task::Task& child) -> bool;
void abort_publication(sched_task::Task& child);

// Process lifecycle producers.  The stop/continued calls return false when an
// older event of that class is still unconsumed; callers must leave the state
// transition pending and retry after the child is next scheduled.
[[nodiscard]] auto publish_exit(sched_task::Task& child) -> bool;
[[nodiscard]] auto publish_job_control_stop(sched_task::Task& child, uint32_t signal) -> bool;
[[nodiscard]] auto publish_continued(sched_task::Task& child) -> bool;
[[nodiscard]] auto publish_ptrace_stop(sched_task::Task& tracee, int32_t status) -> bool;
// Rich ptrace controls may acknowledge a stop without going through waitpid.
// This retires only an unclaimed event owned by the supplied tracer, preserving
// single-winner semantics with concurrent waitpid calls.
[[nodiscard]] auto acknowledge_ptrace_stop(sched_task::Task& tracee, sched_task::Task& tracer) -> bool;

// Explicit ptrace ownership, maintained independently of the scheduler PID
// registry so wait selectors never need a global scan.
[[nodiscard]] auto attach_tracer(sched_task::Task& tracee, sched_task::Task& tracer) -> bool;
void detach_tracer(sched_task::Task& tracee);

// Reparent children/events, cancel registrations owned by a dying process, and
// detach tracees.  Call before publishing the process's own exit event.
void prepare_process_exit(sched_task::Task& process);
void cancel_wait(sched_task::Task& waiter);

// Atomic event claim / waiter registration.  The event remains queued and
// retryable until commit_claim; usercopy must occur between these calls.
[[nodiscard]] auto claim_or_register(sched_task::Task& owner, sched_task::Task& waiter, uint64_t selector, int32_t options,
                                     bool register_if_empty, ClaimedEvent& claimed) -> ProbeResult;
void release_claim(sched_task::Task& waiter, const ClaimedEvent& claimed);
[[nodiscard]] auto commit_claim(sched_task::Task& owner, sched_task::Task& waiter, const ClaimedEvent& claimed, bool keep_event) -> bool;

// Scheduler GC may inspect only this explicit lifecycle state; it must not scan
// for a parent or infer waitability from PID relationships.
[[nodiscard]] auto is_waitable_zombie(const sched_task::Task& task) -> bool;
[[nodiscard]] auto diagnostics(sched_task::Task& task) -> Diagnostics;

#ifdef WOS_SELFTEST
[[nodiscard]] auto selftest_publish_and_registration_ordering() -> bool;
[[nodiscard]] auto selftest_claim_retry_nowait_and_single_winner() -> bool;
[[nodiscard]] auto selftest_waiter_handoff_and_subject_serialization() -> bool;
[[nodiscard]] auto selftest_group_change_wakes_invalidated_selector() -> bool;
[[nodiscard]] auto selftest_selectors_and_publication_race() -> bool;
[[nodiscard]] auto selftest_ptrace_exit_preserves_parent_event() -> bool;
[[nodiscard]] auto selftest_dual_observer_zombie_lifetime() -> bool;
[[nodiscard]] auto selftest_stop_continue_order_and_backpressure() -> bool;
[[nodiscard]] auto selftest_parent_event_transfer() -> bool;
[[nodiscard]] auto selftest_parent_exit_discards_without_reaper() -> bool;
#endif

}  // namespace ker::syscall::process::child_events
