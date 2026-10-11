#pragma once

#include <cstdint>
#include <optional>
#include <platform/sched/session.hpp>
#include <platform/sched/task.hpp>

namespace ker::syscall::process::child_events {

namespace sched_task = ker::mod::sched::task;
using Selector = sched_task::ChildWaitSelector;
using Claim = sched_task::ChildEventClaim;
using View = sched_task::ChildWaitView;

constexpr uint64_t SELECT_ANY = UINT64_MAX;
constexpr uint64_t SELECT_PROCESS_GROUP = 1ULL << 63U;
constexpr uint64_t SELECT_PROCESS_GROUP_MASK = SELECT_PROCESS_GROUP - 1U;

constexpr int32_t OPTION_NOHANG = 1;
constexpr int32_t OPTION_STOPPED = 2;
constexpr int32_t OPTION_EXITED = 4;
constexpr int32_t OPTION_CONTINUED = 8;
constexpr int32_t OPTION_NOWAIT = 0x01000000;

struct ClaimedEvent {
    // Borrowed identity only. Cancellation may retire its storage while the
    // value snapshot is copied out; never dereference this pointer directly.
    sched_task::ChildEvent* node{};
    uint64_t claim_cookie{};
    uint64_t subject_pid{};
    uint64_t process_group{};
    ker::mod::sched::session::Origin group_origin{};
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    int32_t status{};
    sched_task::ChildEventKind kind{sched_task::ChildEventKind::EXIT};
    sched_task::ChildEventAudience audience{sched_task::ChildEventAudience::PARENT};
    // Value snapshot for public copyout/return; physical fields remain intact.
    uint64_t visible_pid{};
};

struct ReapedCpuTimes {
    uint64_t user_time_us{};
    uint64_t system_time_us{};
};

// Serialized with wait-event consumption and remote completion. Callers must
// hold the Task lifetime, and no runqueue/WKI/resource lock.
[[nodiscard]] auto read_reaped_cpu_times(const sched_task::Task& process) -> ReapedCpuTimes;
// Source charges and imported observations are distinct; both contribute to reads.
void add_reaped_cpu_times(sched_task::Task& process, uint64_t user_us, uint64_t system_us);
void import_reaped_cpu_times(sched_task::Task& process, uint64_t user_us, uint64_t system_us);
// Deferred committed receipts cannot mutate an already published exit event.
[[nodiscard]] auto import_live_reaped_cpu_times(sched_task::Task& process, uint64_t user_us, uint64_t system_us) -> bool;

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
    CLOSED,
};

// Positive-PID reports use the child's PID namespace, never its process group
// origin. Duplicate reports from one namespace are idempotent. A caller must
// reject ambiguous selection with EOVERFLOW before claiming either child.
struct PidCandidates {
    bool found{};
    bool ambiguous{};
    ker::mod::sched::session::Origin origin{};

    constexpr void observe(ker::mod::sched::session::Origin candidate) {
        if (found && origin != candidate) ambiguous = true;
        if (!found) origin = candidate;
        found = true;
    }
    constexpr void merge(const PidCandidates& candidates) {
        if (candidates.found) observe(candidates.origin);
        ambiguous |= candidates.ambiguous;
    }
};

struct InspectionContext {
    ker::mod::sched::session::Origin namespace_origin{};
    ker::mod::sched::session::Origin caller_origin{};
    std::optional<ker::mod::sched::session::Origin> caller_group_origin{};
};

// The group generation also covers tracer queues containing native threads:
// their effective group comes from a process owner outside that queue's links.
struct QueueVersion {
    uint64_t queue{};
    uint64_t groups{};
    auto operator==(const QueueVersion&) const -> bool = default;
};

struct QueueInspection {
    bool potential{};
    bool ready{};
    QueueVersion version{};
    PidCandidates pids{};
    ker::mod::sched::session::GroupCandidates groups{};
};

enum class QueueVersionStatus : uint8_t { CURRENT, STALE, EXHAUSTED };
struct ConditionalProbe {
    QueueVersionStatus version{QueueVersionStatus::STALE};
    ProbeResult probe{ProbeResult::WOULD_BLOCK};
};

// Advisory value snapshots only: no claim/reservation, reference transfer,
// registration or wake. Retain owner, with no scheduler/compute/channel/resource
// spinlock held. Bounded inspection may hold peer lifecycle admission.
// Selectors passed to inspect_queue are already localized to this kernel.
// Candidates, readiness and version are one lifecycle-locked snapshot. A version
// is meaningful only for this retained owner; it is not a reusable PID token.
[[nodiscard]] auto inspect_queue(sched_task::Task& owner, Selector selector, int32_t options, const InspectionContext& context = {})
    -> QueueInspection;
[[nodiscard]] auto validate_queue_version(sched_task::Task& owner, QueueVersion expected) -> QueueVersionStatus;
[[nodiscard]] auto try_claim_if_current(sched_task::Task& owner, Claim& claim, Selector selector, int32_t options, QueueVersion expected,
                                        ClaimedEvent& claimed) -> ConditionalProbe;
// Supply this kernel's physical node/boot identity, and the initiating caller's
// physical local/own-group identities, consistently across every queried hop.
[[nodiscard]] auto inspect_pid_candidates(sched_task::Task& owner, uint64_t pid, ker::mod::sched::session::Origin namespace_origin)
    -> PidCandidates;
[[nodiscard]] auto inspect_group_candidates(sched_task::Task& owner, uint64_t process_group,
                                            ker::mod::sched::session::Origin namespace_origin,
                                            ker::mod::sched::session::Origin caller_origin,
                                            std::optional<ker::mod::sched::session::Origin> caller_group_origin)
    -> ker::mod::sched::session::GroupCandidates;

// Returns the process leader with a lifetime reference owned by the caller.
[[nodiscard]] auto acquire_process_owner(sched_task::Task& task) -> sched_task::Task*;
// Caller retains task; its canonical owner is retained by thread binding.
// Snapshot parent membership under lifecycle, without a numeric registry lookup.
[[nodiscard]] auto namespace_parent_pid(const sched_task::Task& task) -> uint64_t;
// Exact private canonical parent membership, retained live for TRACEME.
// Namespace init/unknown/foreign membership has no Host/numeric fallback.
[[nodiscard]] auto acquire_namespace_parent(sched_task::Task& task) -> sched_task::Task*;
// Public wait argument translation. Errors preserve out; private names never
// consult the global registry or remote group-origin resolution.
[[nodiscard]] auto public_wait_selector(sched_task::Task& owner, int64_t pid, Selector& out) -> int;

[[nodiscard]] auto selector_for_process_group(uint64_t process_group, ker::mod::sched::session::Origin origin = {}) -> Selector;
// Resolve numeric aliases only within the owner's child/tracer authority.
// A zero selector means no unique matching subject; it probes as NO_CHILD.
[[nodiscard]] auto selector_for_process_group(sched_task::Task& owner, uint64_t process_group) -> Selector;
[[nodiscard]] auto selector_for_current_process_group(sched_task::Task& process) -> Selector;
[[nodiscard]] auto selector_matches(Selector selector, uint64_t pid, uint64_t process_group, ker::mod::sched::session::Origin origin = {})
    -> bool;
// Route runtime setpgid/setsid changes through the lifecycle lock so a wait
// selector and an immutable event snapshot observe one coherent group value.
void update_process_group(sched_task::Task& process, uint64_t process_group);
// Closure serializes with the bounded store commit. False leaves all identity
// and queue-version fields unchanged; wakes remain outside the namespace gate.
auto update_job_identity(sched_task::Task& process, const ker::mod::sched::session::Identity& identity,
                         std::optional<ker::mod::sched::session::Names> names = std::nullopt) -> bool;

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
// Deferred process-wide controller work; caller holds no scheduler/WKI locks.
void advance_process_job_control(sched_task::Task& owner);
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
// detach tracees. Host adopts through physical init; private orphans use only
// their exact scope's live local init. Call before the process's own exit event.
void prepare_process_exit(sched_task::Task& process);
void cancel_wait(sched_task::Task& waiter);

// Nonblocking independent claims. Caller retains the owner Task and stable claim
// storage through commit/release/cancel; never destroy or reuse an active record.
// A busy record returns WOULD_BLOCK without changing its existing claim. No
// sleeping waiter is registered, and no authority is inferred from the executor.
[[nodiscard]] auto try_claim(sched_task::Task& owner, Claim& claim, Selector selector, int32_t options, ClaimedEvent& claimed,
                             View view = View::PHYSICAL) -> ProbeResult;
void cancel_claim(Claim& claim);
void release_claim(Claim& claim, const ClaimedEvent& claimed);
[[nodiscard]] auto commit_claim(sched_task::Task& owner, Claim& claim, const ClaimedEvent& claimed, bool keep_event) -> bool;

// Atomic event claim / waiter registration.  The event remains queued and
// retryable until commit_claim; usercopy must occur between these calls.
[[nodiscard]] auto claim_or_register(sched_task::Task& owner, sched_task::Task& waiter, Selector selector, int32_t options,
                                     bool register_if_empty, ClaimedEvent& claimed, View view = View::PHYSICAL) -> ProbeResult;
void release_claim(sched_task::Task& waiter, const ClaimedEvent& claimed);
[[nodiscard]] auto commit_claim(sched_task::Task& owner, sched_task::Task& waiter, const ClaimedEvent& claimed, bool keep_event) -> bool;

// Scheduler GC may inspect only this explicit lifecycle state; it must not scan
// for a parent or infer waitability from PID relationships.
[[nodiscard]] auto is_waitable_zombie(const sched_task::Task& task) -> bool;
// Public proc observations suppress trusted physical wait selectors.
[[nodiscard]] auto diagnostics(sched_task::Task& task, bool public_names = false) -> Diagnostics;

#ifdef WOS_SELFTEST
// Per-invocation unlocked cutovers: false after retained reaper selection; true
// before each wake. No global hook or production callback/layout change.
void selftest_prepare_exit_reaper_cutover(sched_task::Task& process, void (*hook)(sched_task::Task&, sched_task::Task*, bool, void*),
                                          void* context);
[[nodiscard]] auto selftest_native_tracee_queue_versions() -> bool;
[[nodiscard]] auto selftest_queue_versions() -> bool;
[[nodiscard]] auto selftest_queue_version_retirement() -> bool;
[[nodiscard]] auto selftest_queue_inspection() -> bool;
[[nodiscard]] auto selftest_queue_candidate_merging() -> bool;
[[nodiscard]] auto selftest_independent_claims() -> bool;
[[nodiscard]] auto selftest_independent_claim_retirement() -> bool;
[[nodiscard]] auto selftest_retired_claim_storage() -> bool;
[[nodiscard]] auto selftest_stale_claim_preserves_reissued_owner() -> bool;
[[nodiscard]] auto selftest_group_origin_selection() -> bool;
[[nodiscard]] auto selftest_group_origin_waiters_and_claims() -> bool;
[[nodiscard]] auto selftest_exit_includes_retired_thread_cpu() -> bool;
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
