#include "child_events.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <net/wki/wire.hpp>
#include <optional>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <syscalls_impl/process/process.hpp>

namespace ker::syscall::process::child_events {
namespace {

using ChildEvent = sched_task::ChildEvent;
using ChildEventAudience = sched_task::ChildEventAudience;
using ChildEventKind = sched_task::ChildEventKind;
using ChildMembershipState = sched_task::ChildMembershipState;
using ChildWaitRegistration = sched_task::ChildWaitRegistration;
using Task = sched_task::Task;

constexpr int32_t STOP_STATUS_LOW = 0x7f;
constexpr int32_t CONTINUED_STATUS = 0xffff;

// Lock order: task-registry lookup (reference only) -> lifecycle -> runqueue.
// No caller may hold a runqueue or WKI compute/channel/RX spinlock. A retained
// peer lifecycle admission may span bounded inspection/validation/claim/commit;
// those paths never send or wait on a peer. Wakes occur after lifecycle unlock.
ker::mod::sys::Spinlock g_lifecycle_lock;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_event_sequence{1};     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_claim_cookie{1};       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_waiter_generation{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Conservative invalidation for inherited groups, including native tracees.
// Lifecycle protected; never wraps. Unrelated group changes may cause a retry.
uint64_t g_group_version{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct WakeSelection {
    Task* signal_owner{};
    Task* waiter{};
    Task* subject{};
};

[[nodiscard]] auto take_lifetime_ref(Task* task) -> bool { return task != nullptr && task->try_acquire_lifetime_ref(); }

void drop_lifetime_ref(Task* task) {
    if (task != nullptr) {
        task->release();
    }
}

[[nodiscard]] auto effective_group_identity(const Task& task) -> ker::mod::sched::session::Identity {
    auto identity = task.job_identity();
    if (identity.pgid == 0) {
        identity.pgid = sched_task::process_pid(task);
        identity.group_origin = {};
    }
    return identity;
}

void advance_version_locked(uint64_t& version) {
    if (version != UINT64_MAX) ++version;
}

void advance_queue_version_locked(Task& owner) { advance_version_locked(owner.child_queue_version); }

[[nodiscard]] auto queue_version_status_locked(const Task& owner, QueueVersion expected) -> QueueVersionStatus {
    if (owner.child_queue_version == UINT64_MAX || g_group_version == UINT64_MAX) return QueueVersionStatus::EXHAUSTED;
    return expected.queue != 0 && expected.groups != 0 && expected == QueueVersion{owner.child_queue_version, g_group_version}
               ? QueueVersionStatus::CURRENT
               : QueueVersionStatus::STALE;
}

void child_list_insert_locked(Task& parent, Task& child) {
    advance_queue_version_locked(parent);
    child.child_sibling_prev = nullptr;
    child.child_sibling_next = parent.child_list_head;
    if (parent.child_list_head != nullptr) {
        parent.child_list_head->child_sibling_prev = &child;
    }
    parent.child_list_head = &child;
}

void child_list_remove_locked(Task& parent, Task& child) {
    advance_queue_version_locked(parent);
    if (child.child_sibling_prev != nullptr) {
        child.child_sibling_prev->child_sibling_next = child.child_sibling_next;
    } else if (parent.child_list_head == &child) {
        parent.child_list_head = child.child_sibling_next;
    }
    if (child.child_sibling_next != nullptr) {
        child.child_sibling_next->child_sibling_prev = child.child_sibling_prev;
    }
    child.child_sibling_prev = nullptr;
    child.child_sibling_next = nullptr;
}

void queue_insert_locked(Task& owner, ChildEvent& event) {
    advance_queue_version_locked(owner);
    ChildEvent* previous = nullptr;
    ChildEvent* cursor = owner.child_event_head;
    while (cursor != nullptr && cursor->sequence < event.sequence) {
        previous = cursor;
        cursor = cursor->next;
    }
    event.next = cursor;
    if (previous != nullptr) {
        previous->next = &event;
    } else {
        owner.child_event_head = &event;
    }
    if (cursor == nullptr) {
        owner.child_event_tail = &event;
    }
}

[[nodiscard]] auto has_queued_exit_event_locked(const Task& subject) -> bool {
    return (subject.child_parent_exit_event.queued && subject.child_parent_exit_event.subject == &subject) ||
           (subject.child_tracer_exit_event.queued && subject.child_tracer_exit_event.subject == &subject);
}

void update_unparented_membership_locked(Task& subject) {
    if (subject.child_parent != nullptr) {
        return;
    }
    subject.child_membership_state.store(
        has_queued_exit_event_locked(subject) ? ChildMembershipState::ZOMBIE : ChildMembershipState::UNLINKED, std::memory_order_release);
}

[[nodiscard]] auto subject_claimed_by_owner_locked(const ChildEvent& event) -> bool {
    if (event.subject == nullptr || event.queue_owner == nullptr) {
        return false;
    }
    ChildEvent* const PARENT_CLAIM = event.subject->child_parent_claimed_event;
    ChildEvent* const TRACER_CLAIM = event.subject->child_tracer_claimed_event;
    return (PARENT_CLAIM != nullptr && PARENT_CLAIM->queue_owner == event.queue_owner) ||
           (TRACER_CLAIM != nullptr && TRACER_CLAIM->queue_owner == event.queue_owner);
}

void publish_subject_claim_locked(ChildEvent& event) {
    if (event.subject == nullptr) {
        return;
    }
    ChildEvent*& slot = event.audience == ChildEventAudience::PARENT ? event.subject->child_parent_claimed_event
                                                                     : event.subject->child_tracer_claimed_event;
    slot = &event;
    if (event.queue_owner != nullptr) advance_queue_version_locked(*event.queue_owner);
}

[[nodiscard]] auto enqueue_event_locked(Task& owner, Task& subject, ChildEvent& event, ChildEventKind kind, ChildEventAudience audience,
                                        int32_t status) -> bool {
    if (event.queued) {
        return false;
    }
    if (!take_lifetime_ref(&owner)) {
        return false;
    }
    if (!take_lifetime_ref(&subject)) {
        owner.release();
        return false;
    }

    event.queue_owner = &owner;
    event.subject = &subject;
    event.claimed_by = nullptr;
    event.sequence = g_event_sequence.fetch_add(1, std::memory_order_relaxed);
    event.claim_cookie = 0;
    event.subject_pid = subject.pid;
    auto const GROUP = effective_group_identity(subject);
    event.process_group = GROUP.pgid;
    event.group_origin = GROUP.group_origin;
    event.user_time_us = sched_task::task_rusage_user_time_us(subject);
    event.system_time_us = sched_task::task_rusage_system_time_us(subject);
    if (kind == ChildEventKind::EXIT && !subject.is_thread && subject.type == sched_task::TaskType::PROCESS) {
        // Production exit has drained CPU contributors and frozen accounting.
        // Lifecycle -> resource is bounded; no resource holder enters lifecycle.
        uint64_t const CPU_FLAGS = subject.resource_lock.lock_irqsave();
        event.user_time_us = ker::mod::sched::resource::saturating_cpu_add(
            subject.resources.cpu_user_time_us,
            ker::mod::sched::resource::saturating_cpu_add(subject.child_user_time_us, subject.imported_child_user_time_us));
        event.system_time_us = ker::mod::sched::resource::saturating_cpu_add(
            subject.resources.cpu_system_time_us,
            ker::mod::sched::resource::saturating_cpu_add(subject.child_system_time_us, subject.imported_child_system_time_us));
        subject.resource_lock.unlock_irqrestore(CPU_FLAGS);
    }
    event.status = status;
    event.kind = kind;
    event.audience = audience;
    event.queued = true;
    queue_insert_locked(owner, event);
    return true;
}

void clear_event_claim_locked(ChildEvent& event) {
    Claim* const CLAIMANT = event.claimed_by;
    if (CLAIMANT != nullptr && event.queue_owner != nullptr) advance_queue_version_locked(*event.queue_owner);
    Task* const WAITER = CLAIMANT != nullptr ? CLAIMANT->waiter : nullptr;
    if (event.subject != nullptr) {
        ChildEvent*& slot = event.audience == ChildEventAudience::PARENT ? event.subject->child_parent_claimed_event
                                                                         : event.subject->child_tracer_claimed_event;
        if (slot == &event) {
            slot = nullptr;
        }
    }
    if (CLAIMANT != nullptr && CLAIMANT->event == &event) {
        CLAIMANT->event = nullptr;
    }
    event.claimed_by = nullptr;
    event.claim_cookie = 0;
    drop_lifetime_ref(WAITER);
}

void queue_unlink_locked(Task& owner, ChildEvent& event) {
    ChildEvent* previous = nullptr;
    ChildEvent* cursor = owner.child_event_head;
    while (cursor != nullptr && cursor != &event) {
        previous = cursor;
        cursor = cursor->next;
    }
    if (cursor == nullptr) {
        return;
    }
    if (previous != nullptr) {
        previous->next = event.next;
    } else {
        owner.child_event_head = event.next;
    }
    if (owner.child_event_tail == &event) {
        owner.child_event_tail = previous;
    }
    event.next = nullptr;
    advance_queue_version_locked(owner);
}

void remove_event_locked(ChildEvent& event) {
    if (!event.queued || event.queue_owner == nullptr) {
        return;
    }
    Task* const OWNER = event.queue_owner;
    Task* const SUBJECT = event.subject;
    queue_unlink_locked(*OWNER, event);
    clear_event_claim_locked(event);
    event.queue_owner = nullptr;
    event.subject = nullptr;
    event.queued = false;
    if (event.kind == ChildEventKind::EXIT && SUBJECT != nullptr) {
        // Parent and tracer exit events are independent immutable audiences.
        // Once parent membership is gone, keep the Task discoverable until the
        // final audience retires its event.
        update_unparented_membership_locked(*SUBJECT);
    }
    drop_lifetime_ref(SUBJECT);
    drop_lifetime_ref(OWNER);
}

void move_event_locked(ChildEvent& event, Task& new_owner) {
    if (!event.queued || event.queue_owner == nullptr || event.queue_owner == &new_owner) {
        return;
    }
    if (!take_lifetime_ref(&new_owner)) {
        remove_event_locked(event);
        return;
    }
    Task* const OLD_OWNER = event.queue_owner;
    queue_unlink_locked(*OLD_OWNER, event);
    clear_event_claim_locked(event);
    event.queue_owner = &new_owner;
    queue_insert_locked(new_owner, event);
    drop_lifetime_ref(OLD_OWNER);
}

void remove_subject_events_locked(Task& owner, Task& subject, ChildEventAudience audience, bool preserve_claimed) {
    ChildEvent* cursor = owner.child_event_head;
    while (cursor != nullptr) {
        ChildEvent* const NEXT = cursor->next;
        if (cursor->subject == &subject && cursor->audience == audience && (!preserve_claimed || cursor->claimed_by == nullptr)) {
            remove_event_locked(*cursor);
        }
        cursor = NEXT;
    }
}

[[nodiscard]] auto event_matches(const ChildEvent& event, Selector selector, int32_t options) -> bool {
    if (!selector_matches(selector, event.subject_pid, event.process_group, event.group_origin)) {
        return false;
    }
    switch (event.kind) {
        case ChildEventKind::EXIT:
        case ChildEventKind::PTRACE_STOP:
            return true;
        case ChildEventKind::JOB_CONTROL_STOP:
            return (options & OPTION_STOPPED) != 0;
        case ChildEventKind::CONTINUED:
            return (options & OPTION_CONTINUED) != 0;
    }
    return false;
}

[[nodiscard]] auto first_matching_event_locked(Task& owner, Selector selector, int32_t options) -> ChildEvent* {
    ChildEvent* event = owner.child_event_head;
    while (event != nullptr) {
        if (event->claimed_by == nullptr && event_matches(*event, selector, options)) {
            // Serialize every status for one subject, not merely one event
            // node.  Otherwise a waiter that ignores an older stop can claim
            // the later exit while a WUNTRACED waiter concurrently claims the
            // stop, allowing exit reaping to invalidate an in-flight usercopy.
            if (!subject_claimed_by_owner_locked(*event)) {
                return event;
            }
        }
        event = event->next;
    }
    return nullptr;
}

[[nodiscard]] auto has_potential_subject_locked(Task& owner, Selector selector) -> bool {
    // Claimed events remain potential children for competing waiters.  They
    // must observe WOULD_BLOCK, not ECHILD, until the single winner commits or
    // releases the claim.
    for (ChildEvent* event = owner.child_event_head; event != nullptr; event = event->next) {
        if (selector_matches(selector, event->subject_pid, event->process_group, event->group_origin)) {
            return true;
        }
    }
    for (Task* child = owner.child_list_head; child != nullptr; child = child->child_sibling_next) {
        auto const GROUP = effective_group_identity(*child);
        if (child->child_membership_state.load(std::memory_order_relaxed) != ChildMembershipState::UNLINKED &&
            selector_matches(selector, child->pid, GROUP.pgid, GROUP.group_origin)) {
            return true;
        }
    }
    for (Task* tracee = owner.child_tracee_head; tracee != nullptr; tracee = tracee->child_tracee_next) {
        auto const GROUP = effective_group_identity(*tracee);
        if (selector_matches(selector, tracee->pid, GROUP.pgid, GROUP.group_origin)) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] auto group_candidates_locked(Task& owner, uint64_t process_group, ker::mod::sched::session::Origin namespace_origin,
                                           ker::mod::sched::session::Origin caller_origin,
                                           std::optional<ker::mod::sched::session::Origin> caller_group_origin)
    -> ker::mod::sched::session::GroupCandidates {
    using namespace ker::mod::sched;
    session::GroupCandidates candidates;
    auto observe = [&](uint64_t pgid, session::Origin origin) {
        if (pgid == process_group) {
            candidates.observe(origin.local() ? namespace_origin : origin, caller_origin, caller_group_origin);
        }
    };
    // Include immutable queued groups, even when the live subject has moved.
    // A claimed event still names a potential child for competing waiters.
    for (auto* event = owner.child_event_head; event != nullptr; event = event->next) observe(event->process_group, event->group_origin);
    for (auto* child = owner.child_list_head; child != nullptr; child = child->child_sibling_next) {
        if (child->child_membership_state.load(std::memory_order_relaxed) == ChildMembershipState::UNLINKED) continue;
        auto const GROUP = effective_group_identity(*child);
        observe(GROUP.pgid, GROUP.group_origin);
    }
    for (auto* tracee = owner.child_tracee_head; tracee != nullptr; tracee = tracee->child_tracee_next) {
        auto const GROUP = effective_group_identity(*tracee);
        observe(GROUP.pgid, GROUP.group_origin);
    }
    return candidates;
}

void waiter_unlink_locked(ChildWaitRegistration& registration) {
    if (!registration.registered || registration.queue_owner == nullptr) {
        return;
    }
    Task* const OWNER = registration.queue_owner;
    Task* const WAITER = registration.waiter;
    ChildWaitRegistration* previous = nullptr;
    ChildWaitRegistration* cursor = OWNER->child_waiter_head;
    while (cursor != nullptr && cursor != &registration) {
        previous = cursor;
        cursor = cursor->next;
    }
    if (cursor != nullptr) {
        if (previous != nullptr) {
            previous->next = registration.next;
        } else {
            OWNER->child_waiter_head = registration.next;
        }
    }
    registration = {};
    drop_lifetime_ref(WAITER);
    drop_lifetime_ref(OWNER);
}

[[nodiscard]] auto waiter_register_locked(Task& owner, Task& waiter, Selector selector, int32_t options) -> bool {
    ChildWaitRegistration& registration = waiter.child_wait_registration;
    if (registration.registered && registration.queue_owner == &owner) {
        registration.selector = selector;
        registration.options = options;
        registration.notified = false;
        registration.generation = g_waiter_generation.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    waiter_unlink_locked(registration);
    if (!take_lifetime_ref(&owner)) {
        return false;
    }
    if (!take_lifetime_ref(&waiter)) {
        owner.release();
        return false;
    }
    registration.queue_owner = &owner;
    registration.waiter = &waiter;
    registration.next = owner.child_waiter_head;
    registration.selector = selector;
    registration.generation = g_waiter_generation.fetch_add(1, std::memory_order_relaxed);
    registration.options = options;
    registration.registered = true;
    registration.notified = false;
    owner.child_waiter_head = &registration;
    return true;
}

[[nodiscard]] auto select_waiter_locked(Task& owner) -> Task* {
    ChildWaitRegistration* registration = owner.child_waiter_head;
    while (registration != nullptr) {
        if (!registration->notified && registration->waiter != nullptr &&
            first_matching_event_locked(owner, registration->selector, registration->options) != nullptr) {
            registration->notified = true;
            if (take_lifetime_ref(registration->waiter)) {
                return registration->waiter;
            }
        }
        registration = registration->next;
    }
    return nullptr;
}

[[nodiscard]] auto select_no_child_waiter_locked(Task& owner) -> Task* {
    ChildWaitRegistration* registration = owner.child_waiter_head;
    while (registration != nullptr) {
        if (!registration->notified && registration->waiter != nullptr && !has_potential_subject_locked(owner, registration->selector)) {
            registration->notified = true;
            if (take_lifetime_ref(registration->waiter)) {
                return registration->waiter;
            }
        }
        registration = registration->next;
    }
    return nullptr;
}

[[nodiscard]] auto make_wake_selection_locked(Task& owner, bool notify_sigchld) -> WakeSelection {
    WakeSelection wake{};
    if (notify_sigchld && take_lifetime_ref(&owner)) {
        wake.signal_owner = &owner;
    }
    wake.waiter = select_waiter_locked(owner);
    return wake;
}

void deliver_wake(WakeSelection wake) {
    if (wake.signal_owner != nullptr) {
        signal_child_event(*wake.signal_owner, ker::net::wki::WKI_CHILD_SIGNAL_HOPS);
    }
    if (wake.waiter != nullptr && wake.waiter != wake.signal_owner) {
        ker::mod::sched::wake_task_from_event(wake.waiter);
    }
    if (wake.subject != nullptr && wake.subject != wake.signal_owner && wake.subject != wake.waiter) {
        // Consuming a stop/continue slot can unblock deferred publication.
        // Retry after releasing the lifecycle lock, before scheduler admission.
        if (wake.subject->jobctl_stopped.load(std::memory_order_acquire)) {
            ker::mod::sched::wake_task_for_signal(wake.subject);
        } else {
            ker::mod::sched::wake_task_from_event(wake.subject);
        }
    }
    drop_lifetime_ref(wake.subject);
    drop_lifetime_ref(wake.waiter);
    drop_lifetime_ref(wake.signal_owner);
}

void wake_no_child_waiters(Task& owner) {
    for (;;) {
        uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
        Task* const WAITER = select_no_child_waiter_locked(owner);
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        if (WAITER == nullptr) {
            return;
        }
        ker::mod::sched::wake_task_from_event(WAITER);
        WAITER->release();
    }
}

void tracee_list_insert_locked(Task& tracer, Task& tracee) {
    advance_queue_version_locked(tracer);
    tracee.child_tracee_prev = nullptr;
    tracee.child_tracee_next = tracer.child_tracee_head;
    if (tracer.child_tracee_head != nullptr) {
        tracer.child_tracee_head->child_tracee_prev = &tracee;
    }
    tracer.child_tracee_head = &tracee;
}

void tracee_list_remove_locked(Task& tracer, Task& tracee) {
    advance_queue_version_locked(tracer);
    if (tracee.child_tracee_prev != nullptr) {
        tracee.child_tracee_prev->child_tracee_next = tracee.child_tracee_next;
    } else if (tracer.child_tracee_head == &tracee) {
        tracer.child_tracee_head = tracee.child_tracee_next;
    }
    if (tracee.child_tracee_next != nullptr) {
        tracee.child_tracee_next->child_tracee_prev = tracee.child_tracee_prev;
    }
    tracee.child_tracee_prev = nullptr;
    tracee.child_tracee_next = nullptr;
}

void unlink_tracer_locked(Task& tracee, bool discard_events) {
    Task* const TRACER = tracee.child_tracer;
    if (TRACER == nullptr) {
        return;
    }
    tracee_list_remove_locked(*TRACER, tracee);
    tracee.child_tracer = nullptr;
    if (discard_events) {
        remove_subject_events_locked(*TRACER, tracee, ChildEventAudience::TRACER, false);
    }
    drop_lifetime_ref(&tracee);
    drop_lifetime_ref(TRACER);
}

[[nodiscard]] auto transfer_parent_events_locked(Task& old_parent, Task* new_parent, Task& child) -> bool {
    bool transferred = false;
    ChildEvent* event = old_parent.child_event_head;
    while (event != nullptr) {
        ChildEvent* const NEXT = event->next;
        if (event->subject == &child && event->audience == ChildEventAudience::PARENT) {
            if (new_parent != nullptr) {
                move_event_locked(*event, *new_parent);
                transferred = transferred || event->queue_owner == new_parent;
            } else {
                remove_event_locked(*event);
            }
        }
        event = NEXT;
    }
    return transferred;
}

}  // namespace

auto acquire_process_owner(Task& task) -> Task* {
    auto* owner = task.credential_owner();
    return take_lifetime_ref(owner) ? owner : nullptr;
}

auto namespace_parent_pid(const Task& task) -> uint64_t {
    auto const* owner = task.credential_owner();
    if (owner == nullptr) return 0;
    auto const* scope = owner->pid_namespace();
    if (scope == nullptr) return 0;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    uint64_t result = 0;
    if (scope->is_host()) {
        // Keep Host's parentless receiver/process init-adoption convention.
        result = owner->parent_pid;
        if (result == 0 && owner->type == sched_task::TaskType::PROCESS && owner->pid != 1) result = 1;
    } else if (owner->namespace_tid() != 1 && owner->child_parent != nullptr &&
               sched_task::same_pid_namespace(*owner, *owner->child_parent)) {
        result = owner->child_parent->namespace_tid();
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto selector_for_process_group(uint64_t process_group, ker::mod::sched::session::Origin origin) -> Selector {
    if (process_group == 0 || process_group >= SELECT_PROCESS_GROUP_MASK) {
        return 0;
    }
    return {SELECT_PROCESS_GROUP | process_group, origin};
}

auto selector_for_current_process_group(Task& process) -> Selector {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const GROUP = effective_group_identity(process);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return selector_for_process_group(GROUP.pgid, GROUP.group_origin);
}

auto selector_for_process_group(Task& owner, uint64_t process_group) -> Selector {
    using namespace ker::mod::sched;
    if (selector_for_process_group(process_group).value == 0) return {};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const JOB = effective_group_identity(owner);
    std::optional<session::Origin> const OWN = JOB.pgid == process_group ? std::optional{JOB.group_origin} : std::nullopt;
    auto const ORIGIN = group_candidates_locked(owner, process_group, {}, {}, OWN).resolve(OWN);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return ORIGIN ? selector_for_process_group(process_group, *ORIGIN) : Selector{};
}

auto inspect_queue(Task& owner, Selector selector, int32_t options, const InspectionContext& context) -> QueueInspection {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    QueueInspection result{.potential = has_potential_subject_locked(owner, selector),
                           .ready = first_matching_event_locked(owner, selector, options) != nullptr,
                           .version = {owner.child_queue_version, g_group_version}};
    if (selector.value > 0 && selector.value < SELECT_PROCESS_GROUP && result.potential) {
        result.pids.observe(context.namespace_origin);
    } else if (selector.value != SELECT_ANY && (selector.value & SELECT_PROCESS_GROUP) != 0) {
        uint64_t const GROUP = selector.value & SELECT_PROCESS_GROUP_MASK;
        if (selector_for_process_group(GROUP).value != 0)
            result.groups =
                group_candidates_locked(owner, GROUP, context.namespace_origin, context.caller_origin, context.caller_group_origin);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto validate_queue_version(Task& owner, QueueVersion expected) -> QueueVersionStatus {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const RESULT = queue_version_status_locked(owner, expected);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto inspect_pid_candidates(Task& owner, uint64_t pid, ker::mod::sched::session::Origin namespace_origin) -> PidCandidates {
    PidCandidates candidates;
    if (pid == 0 || pid >= SELECT_PROCESS_GROUP) return candidates;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (has_potential_subject_locked(owner, pid)) candidates.observe(namespace_origin);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return candidates;
}

auto inspect_group_candidates(Task& owner, uint64_t process_group, ker::mod::sched::session::Origin namespace_origin,
                              ker::mod::sched::session::Origin caller_origin,
                              std::optional<ker::mod::sched::session::Origin> caller_group_origin)
    -> ker::mod::sched::session::GroupCandidates {
    if (selector_for_process_group(process_group).value == 0) return {};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const RESULT = group_candidates_locked(owner, process_group, namespace_origin, caller_origin, caller_group_origin);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto read_reaped_cpu_times(const Task& process) -> ReapedCpuTimes {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ReapedCpuTimes const RESULT{
        ker::mod::sched::resource::saturating_cpu_add(process.child_user_time_us, process.imported_child_user_time_us),
        ker::mod::sched::resource::saturating_cpu_add(process.child_system_time_us, process.imported_child_system_time_us)};
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

void add_reaped_cpu_times(Task& process, uint64_t user_us, uint64_t system_us) {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    process.child_user_time_us = ker::mod::sched::resource::saturating_cpu_add(process.child_user_time_us, user_us);
    process.child_system_time_us = ker::mod::sched::resource::saturating_cpu_add(process.child_system_time_us, system_us);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
}

void import_reaped_cpu_times(Task& process, uint64_t user_us, uint64_t system_us) {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    process.imported_child_user_time_us = ker::mod::sched::resource::saturating_cpu_add(process.imported_child_user_time_us, user_us);
    process.imported_child_system_time_us = ker::mod::sched::resource::saturating_cpu_add(process.imported_child_system_time_us, system_us);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
}

auto import_live_reaped_cpu_times(Task& process, uint64_t user_us, uint64_t system_us) -> bool {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    bool const LIVE = !process.exit_notify_ready.load(std::memory_order_acquire);
    if (LIVE) {
        process.imported_child_user_time_us = ker::mod::sched::resource::saturating_cpu_add(process.imported_child_user_time_us, user_us);
        process.imported_child_system_time_us =
            ker::mod::sched::resource::saturating_cpu_add(process.imported_child_system_time_us, system_us);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return LIVE;
}

auto update_job_identity(Task& process, const ker::mod::sched::session::Identity& identity,
                         std::optional<ker::mod::sched::session::Names> names) -> bool {
    auto const* scope = process.pid_namespace();
    if (scope == nullptr) return false;
    Task* parent = nullptr;
    Task* tracer = nullptr;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const BEFORE = effective_group_identity(process);
    bool committed = false;
    {
        ker::mod::sched::pid_ns::PublicationGuard const ADMISSION(*scope);
        if (ADMISSION.open()) {
            process.publish_job_identity(identity, names);
            committed = true;
        }
    }
    if (!committed) {
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    auto const AFTER = effective_group_identity(process);
    if (BEFORE.pgid != AFTER.pgid || BEFORE.group_origin != AFTER.group_origin) {
        advance_version_locked(g_group_version);
        if (process.child_parent != nullptr) advance_queue_version_locked(*process.child_parent);
        if (process.child_tracer != nullptr && process.child_tracer != process.child_parent)
            advance_queue_version_locked(*process.child_tracer);
    }
    if (take_lifetime_ref(process.child_parent)) {
        parent = process.child_parent;
    }
    if (process.child_tracer != parent && take_lifetime_ref(process.child_tracer)) {
        tracer = process.child_tracer;
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    // A negative/zero wait selector can lose its final potential subject when
    // setpgid/setsid commits.  Wake it to return ECHILD; immutable events keep
    // their publication-time group and therefore remain selector-visible.
    if (parent != nullptr) {
        wake_no_child_waiters(*parent);
        parent->release();
    }
    if (tracer != nullptr) {
        wake_no_child_waiters(*tracer);
        tracer->release();
    }
    return true;
}

void update_process_group(Task& process, uint64_t process_group) {
    auto identity = process.job_identity();
    identity.pgid = process_group;
    update_job_identity(process, identity);
}

auto selector_matches(Selector selector, uint64_t pid, uint64_t process_group, ker::mod::sched::session::Origin origin) -> bool {
    if (selector.value == SELECT_ANY) {
        return true;
    }
    if ((selector.value & SELECT_PROCESS_GROUP) != 0) {
        return process_group == (selector.value & SELECT_PROCESS_GROUP_MASK) && origin == selector.group_origin;
    }
    return selector.value != 0 && pid == selector.value;
}

auto begin_publication(Task& creator, Task& child) -> bool {
    if (child.is_thread) {
        return true;
    }
    Task* const PARENT = acquire_process_owner(creator);
    if (PARENT == nullptr || !take_lifetime_ref(&child)) {
        drop_lifetime_ref(PARENT);
        return false;
    }

    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    bool const AVAILABLE =
        child.child_membership_state.load(std::memory_order_relaxed) == ChildMembershipState::UNLINKED && child.child_parent == nullptr;
    if (AVAILABLE) {
        child.child_publication_established = true;
        child.child_parent = PARENT;
        child.parent_pid = PARENT->pid;
        child_list_insert_locked(*PARENT, child);
        child.child_membership_state.store(ChildMembershipState::PUBLISHING, std::memory_order_release);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    if (!AVAILABLE) {
        child.release();
        PARENT->release();
    }
    // On success, PARENT's lookup reference and child's added reference become
    // the two membership references released by reap/abort/reparent-to-none.
    return AVAILABLE;
}

auto commit_publication(Task& child) -> bool {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const STATE = child.child_membership_state.load(std::memory_order_relaxed);
    bool const COMMITTED = child.is_thread || child.child_publication_established;
    if (child.child_publication_established && child.child_parent != nullptr && STATE == ChildMembershipState::PUBLISHING) {
        child.child_membership_state.store(ChildMembershipState::LIVE, std::memory_order_release);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return COMMITTED;
}

void abort_publication(Task& child) {
    Task* parent = nullptr;
    bool release_child = false;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (child.child_membership_state.load(std::memory_order_relaxed) == ChildMembershipState::PUBLISHING && child.child_parent != nullptr) {
        parent = child.child_parent;
        child_list_remove_locked(*parent, child);
        remove_subject_events_locked(*parent, child, ChildEventAudience::PARENT, false);
        child.child_parent = nullptr;
        child.parent_pid = 0;
        child.child_publication_established = false;
        child.child_membership_state.store(ChildMembershipState::UNLINKED, std::memory_order_release);
        release_child = true;
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    if (release_child) {
        // A selector-specific waiter may have been sleeping solely on this
        // unpublished membership.  Wake it before dropping the membership's
        // parent reference so it can return ECHILD.
        wake_no_child_waiters(*parent);
        child.release();
        parent->release();
    }
}

auto publish_exit(Task& child) -> bool {
    WakeSelection parent_wake{};
    WakeSelection tracer_wake{};
    bool parent_required = false;
    bool parent_published = true;
    bool tracer_required = false;
    bool tracer_published = true;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    Task* const PARENT = child.child_parent;
    auto const MEMBERSHIP = child.child_membership_state.load(std::memory_order_relaxed);
    if (PARENT != nullptr && MEMBERSHIP != ChildMembershipState::UNLINKED) {
        parent_required = true;
        bool const ALREADY_PUBLISHED = child.child_parent_exit_event.queued && child.child_parent_exit_event.queue_owner == PARENT &&
                                       child.child_parent_exit_event.subject == &child &&
                                       child.child_parent_exit_event.kind == ChildEventKind::EXIT &&
                                       child.child_parent_exit_event.audience == ChildEventAudience::PARENT;
        bool const NEWLY_PUBLISHED =
            !ALREADY_PUBLISHED && enqueue_event_locked(*PARENT, child, child.child_parent_exit_event, ChildEventKind::EXIT,
                                                       ChildEventAudience::PARENT, child.exit_status);
        parent_published = ALREADY_PUBLISHED || NEWLY_PUBLISHED;
        if (NEWLY_PUBLISHED) {
            parent_wake = make_wake_selection_locked(*PARENT, true);
        }
    }
    Task* const TRACER = child.child_tracer;
    if (TRACER != nullptr && TRACER != PARENT) {
        tracer_required = true;
        bool const ALREADY_PUBLISHED = child.child_tracer_exit_event.queued && child.child_tracer_exit_event.queue_owner == TRACER &&
                                       child.child_tracer_exit_event.subject == &child &&
                                       child.child_tracer_exit_event.kind == ChildEventKind::EXIT &&
                                       child.child_tracer_exit_event.audience == ChildEventAudience::TRACER;
        bool const NEWLY_PUBLISHED =
            !ALREADY_PUBLISHED && enqueue_event_locked(*TRACER, child, child.child_tracer_exit_event, ChildEventKind::EXIT,
                                                       ChildEventAudience::TRACER, child.exit_status);
        tracer_published = ALREADY_PUBLISHED || NEWLY_PUBLISHED;
        if (NEWLY_PUBLISHED) {
            tracer_wake = make_wake_selection_locked(*TRACER, true);
        }
        if (tracer_published) {
            unlink_tracer_locked(child, false);
        }
    } else if (TRACER != nullptr && parent_published) {
        unlink_tracer_locked(child, false);
    }
    bool const PUBLISHED = (!parent_required || parent_published) && (!tracer_required || tracer_published);
    if (PUBLISHED && (parent_required || tracer_required)) {
        child.child_membership_state.store(ChildMembershipState::ZOMBIE, std::memory_order_release);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(parent_wake);
    deliver_wake(tracer_wake);
    return PUBLISHED;
}

void advance_process_job_control(Task& owner) {
    WakeSelection stop_wake{}, continue_wake{};
    bool released = false;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    uint64_t const RESOURCE_FLAGS = owner.resource_lock.lock_irqsave();
    auto& control = owner.jobctl_control;
    auto& barrier = owner.jobctl_barrier;
    if (control.enabled && barrier.stopping() && !owner.exit_in_progress && !owner.has_exited &&
        !owner.process_exit_requested.load(std::memory_order_acquire)) {
        if (barrier.complete()) {
            uint64_t const GENERATION = barrier.generation();
            if (control.reported_generation != GENERATION) {
                auto const STATUS = static_cast<int32_t>((barrier.stop_signal() << 8U) | static_cast<uint32_t>(STOP_STATUS_LOW));
                bool const PUBLISHED = owner.child_parent == nullptr ||
                                       (!owner.child_continued_event.queued &&
                                        enqueue_event_locked(*owner.child_parent, owner, owner.child_job_stop_event,
                                                             ChildEventKind::JOB_CONTROL_STOP, ChildEventAudience::PARENT, STATUS));
                owner.jobctl_stop_publish_deferred.store(!PUBLISHED, std::memory_order_release);
                if (PUBLISHED) {
                    control.reported_generation = GENERATION;
                    owner.jobctl_stop_pending.store(true, std::memory_order_release);
                    if (owner.child_parent != nullptr) stop_wake = make_wake_selection_locked(*owner.child_parent, true);
                }
            }
            if (control.continue_pending && control.reported_generation == GENERATION) {
                bool const PUBLISHED = owner.child_parent == nullptr ||
                                       enqueue_event_locked(*owner.child_parent, owner, owner.child_continued_event,
                                                            ChildEventKind::CONTINUED, ChildEventAudience::PARENT, CONTINUED_STATUS);
                if (PUBLISHED) {
                    if (owner.child_parent != nullptr) continue_wake = make_wake_selection_locked(*owner.child_parent, true);
                    released = barrier.request_continue();
                }
            }
        } else if (control.continue_pending) {
            // CONT canceled a stop that never completed. Do not invent a
            // stopped event for a process whose workers were still executing.
            released = barrier.request_continue();
        }
        if (released) {
            control.continue_pending = false;
            owner.jobctl_stop_publish_deferred.store(false, std::memory_order_release);
            ker::mod::sched::job_control::mark_wakes_locked(owner);
        }
    }
    owner.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(stop_wake);
    deliver_wake(continue_wake);
}

auto publish_job_control_stop(Task& child, uint32_t signal) -> bool {
    WakeSelection wake{};
    bool published = true;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (child.child_parent != nullptr) {
        auto const STATUS = static_cast<int32_t>((signal << 8U) | static_cast<uint32_t>(STOP_STATUS_LOW));
        // A task may only enter another stopped epoch after the preceding
        // continued event has been consumed.  This bounds allocation-free
        // producer storage without dropping an observable transition and
        // guarantees that SIGCONT always has a free event node while stopped.
        published = !child.child_continued_event.queued &&
                    enqueue_event_locked(*child.child_parent, child, child.child_job_stop_event, ChildEventKind::JOB_CONTROL_STOP,
                                         ChildEventAudience::PARENT, STATUS);
        if (published) {
            wake = make_wake_selection_locked(*child.child_parent, true);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    return published;
}

auto publish_continued(Task& child) -> bool {
    WakeSelection wake{};
    bool published = true;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (child.child_parent != nullptr) {
        published = enqueue_event_locked(*child.child_parent, child, child.child_continued_event, ChildEventKind::CONTINUED,
                                         ChildEventAudience::PARENT, CONTINUED_STATUS);
        if (published) {
            wake = make_wake_selection_locked(*child.child_parent, true);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    return published;
}

auto publish_ptrace_stop(Task& tracee, int32_t status) -> bool {
    WakeSelection wake{};
    bool published = false;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (tracee.child_tracer != nullptr) {
        published = enqueue_event_locked(*tracee.child_tracer, tracee, tracee.child_ptrace_stop_event, ChildEventKind::PTRACE_STOP,
                                         ChildEventAudience::TRACER, status);
        if (published) {
            wake = make_wake_selection_locked(*tracee.child_tracer, true);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    return published;
}

auto acknowledge_ptrace_stop(Task& tracee, Task& tracer) -> bool {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ChildEvent& event = tracee.child_ptrace_stop_event;
    bool const ACKNOWLEDGED = event.queued && event.queue_owner == &tracer && event.subject == &tracee &&
                              event.kind == ChildEventKind::PTRACE_STOP && event.audience == ChildEventAudience::TRACER &&
                              event.claimed_by == nullptr;
    if (ACKNOWLEDGED) {
        remove_event_locked(event);
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return ACKNOWLEDGED;
}

auto attach_tracer(Task& tracee, Task& tracer) -> bool {
    if (&tracee == &tracer) {
        return false;
    }
    bool const TRACEE_REF = take_lifetime_ref(&tracee);
    bool const TRACER_REF = TRACEE_REF && take_lifetime_ref(&tracer);
    if (!TRACER_REF) {
        if (TRACEE_REF) {
            tracee.release();
        }
        return false;
    }
    Task* previous_tracer = nullptr;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (tracee.child_tracer == &tracer) {
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        tracee.release();
        tracer.release();
        return true;
    }
    if (take_lifetime_ref(tracee.child_tracer)) {
        previous_tracer = tracee.child_tracer;
    }
    unlink_tracer_locked(tracee, true);
    tracee.child_tracer = &tracer;
    tracee_list_insert_locked(tracer, tracee);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    if (previous_tracer != nullptr) {
        wake_no_child_waiters(*previous_tracer);
        previous_tracer->release();
    }
    return true;
}

void detach_tracer(Task& tracee) {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    Task* const TRACER = tracee.child_tracer;
    bool const TRACER_REF = take_lifetime_ref(TRACER);
    unlink_tracer_locked(tracee, true);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    if (TRACER_REF) {
        wake_no_child_waiters(*TRACER);
        TRACER->release();
    }
}

void prepare_process_exit(Task& process) {
    // A process may itself be blocked in waitpid when group exit wins.  Retire
    // its intrusive registration/claim before transferring the queue it was
    // observing.
    cancel_wait(process);

    Task* init = nullptr;
    if (process.pid != 1) {
        init = ker::mod::sched::find_task_by_pid_safe(1);
    }

    for (;;) {
        Task* child = nullptr;
        WakeSelection init_wake{};
        uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
        child = process.child_list_head;
        if (child == nullptr) {
            g_lifecycle_lock.unlock_irqrestore(FLAGS);
            break;
        }
        // Retain a temporary reference before making the child UNLINKED.  The
        // scheduler GC predicate can change immediately with that store.
        bool const CHILD_REF = take_lifetime_ref(child);
        bool const CAN_REPARENT = init != nullptr && init != child && take_lifetime_ref(init);
        child_list_remove_locked(process, *child);
        if (CAN_REPARENT) {
            child->child_parent = init;
            child->parent_pid = init->pid;
            child_list_insert_locked(*init, *child);
            bool const TRANSFERRED_EVENT = transfer_parent_events_locked(process, init, *child);
            process.release();
            if (TRANSFERRED_EVENT) {
                init_wake = make_wake_selection_locked(*init, true);
            }
        } else {
            static_cast<void>(transfer_parent_events_locked(process, nullptr, *child));
            child->child_parent = nullptr;
            child->parent_pid = 0;
            update_unparented_membership_locked(*child);
            child->release();
            process.release();
        }
        g_lifecycle_lock.unlock_irqrestore(FLAGS);

        deliver_wake(init_wake);
        if (CHILD_REF) {
            // Transfer the exact selected child reference. A remote child may
            // have changed even a previously disabled registration after exec.
            static_cast<void>(signal_parent_death(*child, ker::net::wki::WKI_PARENT_DEATH_HOPS));
        }
    }

    // A dying tracer relinquishes every tracee explicitly.  Wake each tracee
    // after dropping the lifecycle lock so a stopped task can resume.
    for (;;) {
        Task* tracee = nullptr;
        uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
        tracee = process.child_tracee_head;
        if (tracee == nullptr) {
            g_lifecycle_lock.unlock_irqrestore(FLAGS);
            break;
        }
        bool const TRACEE_REF = take_lifetime_ref(tracee);
        unlink_tracer_locked(*tracee, true);
        tracee->ptrace_traced = false;
        tracee->ptrace_stopped = false;
        tracee->ptrace_stop_pending = false;
        tracee->ptrace_tracer_pid = 0;
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        if (TRACEE_REF) {
            ker::mod::sched::wake_task_from_event(tracee);
            tracee->release();
        }
    }

    // Tracer-exit events unlink their tracee at publication time, so they may
    // remain after child/tracee topology has been drained.  No event may retain
    // a dying owner or a claimant indefinitely.
    for (;;) {
        Task* claimant = nullptr;
        Task* subject = nullptr;
        uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
        ChildEvent* const EVENT = process.child_event_head;
        if (EVENT == nullptr) {
            g_lifecycle_lock.unlock_irqrestore(FLAGS);
            break;
        }
        claimant = EVENT->claimed_by != nullptr ? EVENT->claimed_by->waiter : nullptr;
        if (!take_lifetime_ref(claimant)) {
            claimant = nullptr;
        }
        subject = EVENT->subject;
        if (!take_lifetime_ref(subject)) {
            subject = nullptr;
        }
        remove_event_locked(*EVENT);
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        if (claimant != nullptr) {
            ker::mod::sched::wake_task_from_event(claimant);
            claimant->release();
        }
        if (subject != nullptr && subject != claimant) {
            ker::mod::sched::wake_task_from_event(subject);
        }
        drop_lifetime_ref(subject);
    }

    // Registrations are intrusive, so cancellation can drain them one at a
    // time without an allocation or an unbounded stack snapshot.
    for (;;) {
        Task* waiter = nullptr;
        uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
        ChildWaitRegistration* const REGISTRATION = process.child_waiter_head;
        if (REGISTRATION == nullptr) {
            g_lifecycle_lock.unlock_irqrestore(FLAGS);
            break;
        }
        waiter = REGISTRATION->waiter;
        bool const WAITER_REF = take_lifetime_ref(waiter);
        waiter_unlink_locked(*REGISTRATION);
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        if (WAITER_REF) {
            ker::mod::sched::wake_task_from_event(waiter);
            waiter->release();
        }
    }

    drop_lifetime_ref(init);
}

void cancel_wait(Task& waiter) {
    WakeSelection wake{};
    Task* owner = nullptr;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    Task* const REGISTRATION_OWNER = waiter.child_wait_registration.queue_owner;
    if (take_lifetime_ref(REGISTRATION_OWNER)) {
        owner = REGISTRATION_OWNER;
    }
    waiter_unlink_locked(waiter.child_wait_registration);
    ChildEvent* const EVENT = waiter.child_claim.event;
    if (EVENT != nullptr && EVENT->queued && EVENT->claimed_by == &waiter.child_claim) {
        Task* const OWNER = EVENT->queue_owner;
        if (owner == nullptr && take_lifetime_ref(OWNER)) {
            owner = OWNER;
        }
        clear_event_claim_locked(*EVENT);
    }
    if (owner != nullptr) {
        wake = make_wake_selection_locked(*owner, false);
    }
    waiter.child_claim.event = nullptr;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    drop_lifetime_ref(owner);
}

namespace {
auto claim_locked(Task& owner, Claim& claim, Selector selector, int32_t options, ClaimedEvent& claimed) -> ProbeResult {
    claimed = {};
    if (claim.event != nullptr) {
        return ProbeResult::WOULD_BLOCK;
    }
    if (ChildEvent* const EVENT = first_matching_event_locked(owner, selector, options); EVENT != nullptr) {
        if (claim.waiter != nullptr && !take_lifetime_ref(claim.waiter)) {
            return ProbeResult::WOULD_BLOCK;
        }
        EVENT->claimed_by = &claim;
        EVENT->claim_cookie = g_claim_cookie.fetch_add(1, std::memory_order_relaxed);
        publish_subject_claim_locked(*EVENT);
        claim.event = EVENT;
        claimed.node = EVENT;
        claimed.claim_cookie = EVENT->claim_cookie;
        claimed.subject_pid = EVENT->subject_pid;
        claimed.process_group = EVENT->process_group;
        claimed.group_origin = EVENT->group_origin;
        claimed.user_time_us = EVENT->user_time_us;
        claimed.system_time_us = EVENT->system_time_us;
        claimed.status = EVENT->status;
        claimed.kind = EVENT->kind;
        claimed.audience = EVENT->audience;
        return ProbeResult::CLAIMED;
    }

    return has_potential_subject_locked(owner, selector) ? ProbeResult::WOULD_BLOCK : ProbeResult::NO_CHILD;
}
}  // namespace

auto try_claim_if_current(Task& owner, Claim& claim, Selector selector, int32_t options, QueueVersion expected, ClaimedEvent& claimed)
    -> ConditionalProbe {
    claimed = {};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ConditionalProbe result{.version = queue_version_status_locked(owner, expected)};
    if (result.version == QueueVersionStatus::CURRENT) result.probe = claim_locked(owner, claim, selector, options, claimed);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto try_claim(Task& owner, Claim& claim, Selector selector, int32_t options, ClaimedEvent& claimed) -> ProbeResult {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto const RESULT = claim_locked(owner, claim, selector, options, claimed);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto claim_or_register(Task& owner, Task& waiter, Selector selector, int32_t options, bool register_if_empty, ClaimedEvent& claimed)
    -> ProbeResult {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    auto result = claim_locked(owner, waiter.child_claim, selector, options, claimed);
    if (result != ProbeResult::WOULD_BLOCK) {
        waiter_unlink_locked(waiter.child_wait_registration);
    } else if (waiter.child_claim.event == nullptr && register_if_empty && !waiter_register_locked(owner, waiter, selector, options)) {
        result = ProbeResult::NO_CHILD;
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

void cancel_claim(Claim& claim) {
    WakeSelection wake{};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ChildEvent* const EVENT = claim.event;
    if (EVENT != nullptr && EVENT->queued && EVENT->claimed_by == &claim) {
        Task* const OWNER = EVENT->queue_owner;
        clear_event_claim_locked(*EVENT);
        if (OWNER != nullptr) {
            wake = make_wake_selection_locked(*OWNER, false);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
}

void release_claim(Claim& claim, const ClaimedEvent& claimed) {
    WakeSelection wake{};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    // The copied token may outlive event removal and Task reclamation. Only
    // the lock-protected claim link still carries queue-owned storage.
    ChildEvent* const EVENT = claim.event;
    if (EVENT != nullptr && EVENT == claimed.node && EVENT->queued && EVENT->claimed_by == &claim &&
        EVENT->claim_cookie == claimed.claim_cookie) {
        Task* const OWNER = EVENT->queue_owner;
        clear_event_claim_locked(*EVENT);
        if (OWNER != nullptr) {
            wake = make_wake_selection_locked(*OWNER, false);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
}

auto commit_claim(Task& owner, Claim& claim, const ClaimedEvent& claimed, bool keep_event) -> bool {
    WakeSelection wake{};
    bool reaped = false;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ChildEvent* const EVENT = claim.event;
    if (EVENT == nullptr || EVENT != claimed.node || !EVENT->queued || EVENT->queue_owner != &owner || EVENT->claimed_by != &claim ||
        EVENT->claim_cookie != claimed.claim_cookie) {
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    clear_event_claim_locked(*EVENT);
    if (!keep_event) {
        Task* const SUBJECT = EVENT->subject;
        ChildEventKind const KIND = EVENT->kind;
        bool const PARENT_EXIT = EVENT->kind == ChildEventKind::EXIT && EVENT->audience == ChildEventAudience::PARENT &&
                                 SUBJECT != nullptr && SUBJECT->child_parent == &owner;
        if (SUBJECT != nullptr &&
            (KIND == ChildEventKind::PTRACE_STOP || KIND == ChildEventKind::JOB_CONTROL_STOP || KIND == ChildEventKind::CONTINUED) &&
            take_lifetime_ref(SUBJECT)) {
            wake.subject = SUBJECT;
        }
        if (SUBJECT != nullptr && KIND == ChildEventKind::PTRACE_STOP) {
            SUBJECT->ptrace_stop_pending = false;
        } else if (SUBJECT != nullptr && KIND == ChildEventKind::JOB_CONTROL_STOP) {
            SUBJECT->jobctl_stop_pending.store(false, std::memory_order_release);
        }
        remove_event_locked(*EVENT);
        if (PARENT_EXIT) {
            // Exit reaping invalidates every older status for this parent.  A
            // same-subject claim normally serializes exit behind the older
            // event; clearing here is the final defensive lifetime fence.
            remove_subject_events_locked(owner, *SUBJECT, ChildEventAudience::PARENT, false);
            child_list_remove_locked(owner, *SUBJECT);
            SUBJECT->child_parent = nullptr;
            SUBJECT->parent_pid = 0;
            update_unparented_membership_locked(*SUBJECT);
            owner.child_user_time_us = ker::mod::sched::resource::saturating_cpu_add(owner.child_user_time_us, claimed.user_time_us);
            owner.child_system_time_us = ker::mod::sched::resource::saturating_cpu_add(owner.child_system_time_us, claimed.system_time_us);
            SUBJECT->release();
            owner.release();
            reaped = true;
        }
    }
    wake.waiter = select_waiter_locked(owner);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    if (!keep_event) {
        // Event consumption can remove the final selector-visible subject
        // (notably after an exit reap or tracer-exit acknowledgement).  Wake
        // every affected registered waiter so losers observe ECHILD instead
        // of sleeping forever behind the single winner.
        wake_no_child_waiters(owner);
    }
    if (reaped) {
        ker::mod::sched::request_gc();
    }
    return true;
}

void release_claim(Task& waiter, const ClaimedEvent& claimed) { release_claim(waiter.child_claim, claimed); }

auto commit_claim(Task& owner, Task& waiter, const ClaimedEvent& claimed, bool keep_event) -> bool {
    return commit_claim(owner, waiter.child_claim, claimed, keep_event);
}

auto is_waitable_zombie(const Task& task) -> bool {
    return task.child_membership_state.load(std::memory_order_acquire) == ChildMembershipState::ZOMBIE;
}

auto diagnostics(Task& task) -> Diagnostics {
    Diagnostics result{};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (task.child_wait_registration.registered) {
        result.registered_selector = task.child_wait_registration.selector.value;
    }
    for (ChildEvent* event = task.child_event_head; event != nullptr; event = event->next) {
        result.owned_event_count++;
    }
    for (ChildWaitRegistration* waiter = task.child_waiter_head; waiter != nullptr; waiter = waiter->next) {
        result.owned_waiter_count++;
    }
    result.membership = task.child_membership_state.load(std::memory_order_relaxed);
    result.claim_active = task.child_claim.event != nullptr;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

#ifdef WOS_SELFTEST
namespace {

void initialize_fixture(Task& task, uint64_t pid, uint64_t process_group = 0) {
    task.type = sched_task::TaskType::PROCESS;
    task.pid = pid;
    task.initialize_job_identity({.pgid = process_group != 0 ? process_group : pid});
    // Fixture tasks are deliberately invisible to scheduler wake publication.
    task.state.store(sched_task::TaskState::EXITING, std::memory_order_release);
}

[[nodiscard]] auto fixture_is_unlinked(const Task& task) -> bool {
    return task.child_parent == nullptr && task.child_membership_state.load(std::memory_order_acquire) == ChildMembershipState::UNLINKED;
}

[[nodiscard]] auto fixture_waiter_notified(Task& waiter) -> bool {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    bool const NOTIFIED = waiter.child_wait_registration.notified;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return NOTIFIED;
}

}  // namespace

auto selftest_native_tracee_queue_versions() -> bool {
    auto owner = std::make_unique<Task>();
    auto tracer = std::make_unique<Task>();
    initialize_fixture(*owner, 0x9020, 77);
    initialize_fixture(*tracer, 1);
    bool ok = true;
    {
        auto thread = std::make_unique<Task>();
        initialize_fixture(*thread, 0x9021);
        thread->is_thread = true;
        thread->owner_pid = owner->pid;
        owner->state.store(sched_task::TaskState::ACTIVE);
        ok &= ker::mod::sched::resource::bind_thread_owner(*thread, *owner);
        owner->state.store(sched_task::TaskState::EXITING);
        ok &= attach_tracer(*thread, *tracer);
        auto const BEFORE = inspect_queue(*tracer, selector_for_process_group(77), 0);
        ok &= BEFORE.potential && owner->child_tracer == nullptr;
        update_process_group(*owner, 88);
        auto const AFTER = inspect_queue(*tracer, selector_for_process_group(77), 0);
        ok &= !AFTER.potential && AFTER.version.queue == BEFORE.version.queue && AFTER.version.groups != BEFORE.version.groups;
        ok &= validate_queue_version(*tracer, BEFORE.version) == QueueVersionStatus::STALE;
        Claim claim{};
        ClaimedEvent event{};
        auto probe = try_claim_if_current(*tracer, claim, selector_for_process_group(77), 0, BEFORE.version, event);
        ok &= probe.version == QueueVersionStatus::STALE && claim.event == nullptr;
        ok &= publish_ptrace_stop(*thread, 0x137f);
        auto const READY = inspect_queue(*tracer, selector_for_process_group(88), 0);
        probe = try_claim_if_current(*tracer, claim, selector_for_process_group(88), 0, READY.version, event);
        ok &= probe.version == QueueVersionStatus::CURRENT && probe.probe == ProbeResult::CLAIMED && event.process_group == 88;
        update_process_group(*owner, 99);
        // An already held immutable claim remains valid through a later group
        // change; only discovery/new claims depend on the inspected version.
        ok &= commit_claim(*tracer, claim, event, false);
        ok &= !inspect_queue(*tracer, selector_for_process_group(88), 0).potential;
        cancel_claim(claim);
        detach_tracer(*thread);
        ok &= thread->ref_count.load() == 1;
    }
    ok &= owner->ref_count.load() == 1 && owner->resource_thread_owners.load() == 0 && tracer->ref_count.load() == 1;
    return ok;
}

auto selftest_queue_versions() -> bool {
    auto tasks = std::make_unique<Task[]>(3);
    auto& owner = tasks[0];
    auto& child = tasks[1];
    auto& provisional = tasks[2];
    for (size_t i = 0; i < 3; ++i) initialize_fixture(tasks[i], i == 0 ? 1 : 0x9000 + i, 77);
    auto snapshot = [&] { return inspect_queue(owner, child.pid, OPTION_STOPPED, {{4, 5}, {4, 5}, std::nullopt}); };
    auto changed = [&](QueueVersion version) { return validate_queue_version(owner, version) == QueueVersionStatus::STALE; };
    auto before = snapshot();
    bool ok = before.version.queue != 0 && before.version.groups != 0 && !before.potential && !before.ready && !before.pids.found;
    ok &= validate_queue_version(owner, {}) == QueueVersionStatus::STALE;
    Claim claim{};
    ClaimedEvent event{};
    auto probe = try_claim_if_current(owner, claim, child.pid, 0, before.version, event);
    ok &= probe.version == QueueVersionStatus::CURRENT && probe.probe == ProbeResult::NO_CHILD;
    ok &= snapshot().version == before.version;
    ok &= begin_publication(owner, child) && changed(before.version);
    before = snapshot();
    ok &= before.potential && !before.ready && before.pids.found && before.pids.origin == ker::mod::sched::session::Origin{4, 5};
    ok &= commit_publication(child) && snapshot().version == before.version;
    ok &= begin_publication(owner, provisional) && changed(before.version);
    before = snapshot();
    abort_publication(provisional);
    ok &= changed(before.version);
    before = snapshot();
    update_process_group(child, 88);
    update_process_group(child, 77);
    ok &= changed(before.version);  // Restored group is not a restored snapshot.
    before = snapshot();
    auto job = child.job_identity();
    job.exec_completed = true;
    update_job_identity(child, job);
    ok &= snapshot().version == before.version;  // No selector-visible change.
    ok &= publish_job_control_stop(child, 19) && changed(before.version);
    before = snapshot();
    auto groups = inspect_queue(owner, selector_for_process_group(77), OPTION_STOPPED, {{4, 5}, {4, 5}, std::nullopt});
    ok &= groups.version == before.version && groups.ready && groups.groups.flags == ker::mod::sched::session::GroupCandidates::LOCAL;
    probe = try_claim_if_current(owner, claim, child.pid, OPTION_STOPPED, before.version, event);
    ok &= probe.version == QueueVersionStatus::CURRENT && probe.probe == ProbeResult::CLAIMED && changed(before.version);
    auto* const NODE = claim.event;
    auto const COOKIE = event.claim_cookie;
    ClaimedEvent rejected{};
    auto const REFS = child.ref_count.load();
    probe = try_claim_if_current(owner, claim, child.pid, OPTION_STOPPED, before.version, rejected);
    ok &= probe.version == QueueVersionStatus::STALE && rejected.node == nullptr && claim.event == NODE && NODE->claim_cookie == COOKIE;
    ok &= child.ref_count.load() == REFS && owner.child_user_time_us == 0;
    auto busy = snapshot();
    probe = try_claim_if_current(owner, claim, child.pid, OPTION_STOPPED, busy.version, rejected);
    ok &= probe.version == QueueVersionStatus::CURRENT && probe.probe == ProbeResult::WOULD_BLOCK && snapshot().version == busy.version;
    release_claim(claim, event);
    ok &= changed(before.version) && snapshot().ready;
    probe = try_claim_if_current(owner, claim, child.pid, OPTION_STOPPED, before.version, rejected);
    ok &= probe.version == QueueVersionStatus::STALE && claim.event == nullptr;
    before = snapshot();
    probe = try_claim_if_current(owner, claim, child.pid, OPTION_STOPPED, before.version, event);
    ok &= probe.probe == ProbeResult::CLAIMED && commit_claim(owner, claim, event, true);
    ok &= changed(before.version) && snapshot().ready && owner.child_user_time_us == 0;
    before = snapshot();
    child.resources.cpu_user_time_us = 11;
    child.resources.cpu_system_time_us = 13;
    child.exit_status = 37 << 8;
    ok &= publish_exit(child) && changed(before.version);
    before = snapshot();
    probe = try_claim_if_current(owner, claim, child.pid, 0, before.version, event);
    ok &= probe.probe == ProbeResult::CLAIMED && commit_claim(owner, claim, event, false);
    ok &= changed(before.version) && !snapshot().potential && owner.child_user_time_us == 11 && owner.child_system_time_us == 13;
    cancel_claim(claim);
    prepare_process_exit(owner);
    for (size_t i = 0; i < 3; ++i) ok &= tasks[i].ref_count.load() == 1;
    return ok;
}

auto selftest_queue_version_retirement() -> bool {
    auto tasks = std::make_unique<Task[]>(4);
    auto& owner = tasks[0];
    auto& child = tasks[1];
    auto& tracer = tasks[2];
    auto& replacement = tasks[3];
    for (size_t i = 0; i < 4; ++i) initialize_fixture(tasks[i], i == 1 ? 0x9011 : 1, 77);
    auto version = [&](Task& target) { return inspect_queue(target, SELECT_ANY, OPTION_STOPPED).version; };
    auto changed = [&](Task& target, QueueVersion old) { return validate_queue_version(target, old) == QueueVersionStatus::STALE; };
    bool ok = begin_publication(owner, child) && commit_publication(child);
    auto parent_version = version(owner), tracer_version = version(tracer);
    ok &= attach_tracer(child, tracer) && changed(tracer, tracer_version) && version(owner) == parent_version;
    tracer_version = version(tracer);
    ok &= attach_tracer(child, tracer) && version(tracer) == tracer_version;
    update_process_group(child, 88);
    ok &= changed(owner, parent_version) && changed(tracer, tracer_version);
    parent_version = version(owner);
    tracer_version = version(tracer);
    ok &= publish_ptrace_stop(child, 0x137f) && changed(tracer, tracer_version) && version(owner) == parent_version;
    tracer_version = version(tracer);
    auto replacement_version = version(replacement);
    ok &= attach_tracer(child, replacement) && changed(tracer, tracer_version) && changed(replacement, replacement_version);
    ok &= !inspect_queue(tracer, SELECT_ANY, 0).potential;
    ok &= publish_exit(child);
    Claim parent_claim{}, tracer_claim{};
    ClaimedEvent parent_event{}, tracer_event{};
    auto probe = try_claim_if_current(owner, parent_claim, child.pid, 0, version(owner), parent_event);
    ok &= probe.probe == ProbeResult::CLAIMED;
    probe = try_claim_if_current(replacement, tracer_claim, child.pid, 0, version(replacement), tracer_event);
    ok &= probe.probe == ProbeResult::CLAIMED;
    parent_version = version(owner);
    replacement_version = version(replacement);
    prepare_process_exit(owner);  // Private PID1: discards the parent audience.
    ok &= changed(owner, parent_version) && parent_claim.event == nullptr;
    ok &= version(replacement) == replacement_version && tracer_claim.event == tracer_event.node;
    ok &= commit_claim(replacement, tracer_claim, tracer_event, false) && changed(replacement, replacement_version);
    cancel_claim(parent_claim);
    cancel_claim(tracer_claim);
    prepare_process_exit(tracer);
    prepare_process_exit(replacement);
    for (size_t i = 0; i < 4; ++i) ok &= tasks[i].ref_count.load() == 1;

    // Exhaustion never wraps or prevents ordinary local lifecycle cleanup.
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    owner.child_queue_version = UINT64_MAX - 1;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    ok &= begin_publication(owner, child) && commit_publication(child) && publish_exit(child);
    ok &= version(owner).queue == UINT64_MAX && validate_queue_version(owner, version(owner)) == QueueVersionStatus::EXHAUSTED;
    probe = try_claim_if_current(owner, parent_claim, child.pid, 0, {UINT64_MAX - 1, version(owner).groups}, parent_event);
    ok &= probe.version == QueueVersionStatus::EXHAUSTED && parent_claim.event == nullptr;
    ok &= try_claim(owner, parent_claim, child.pid, 0, parent_event) == ProbeResult::CLAIMED;
    ok &= commit_claim(owner, parent_claim, parent_event, false) && version(owner).queue == UINT64_MAX;
    cancel_claim(parent_claim);
    prepare_process_exit(owner);
    for (size_t i = 0; i < 4; ++i) ok &= tasks[i].ref_count.load() == 1;
    return ok;
}

auto selftest_queue_inspection() -> bool {
    auto tasks = std::make_unique<Task[]>(4);
    auto& owner = tasks[0];
    auto& child = tasks[1];
    auto& other_owner = tasks[2];
    auto& tracee = tasks[3];
    for (size_t i = 0; i < 4; ++i) initialize_fixture(tasks[i], i % 2 == 0 ? 1 : 0x8F00 + i, 77);
    tracee.initialize_job_identity({.pgid = 99});
    auto check = [&](Selector selector, int32_t options, bool potential, bool ready) {
        auto const RESULT = inspect_queue(owner, selector, options);
        return RESULT.potential == potential && RESULT.ready == ready;
    };
    bool ok = check(SELECT_ANY, 0, false, false);
    ok &= begin_publication(owner, child);
    ok &= check(child.pid, 0, true, false);  // Membership precedes publication.
    ok &= commit_publication(child);
    ok &= begin_publication(other_owner, tracee) && commit_publication(tracee);
    ok &= check(tracee.pid, 0, false, false);  // No authority from a numeric PID.
    ok &= attach_tracer(tracee, owner) && check(tracee.pid, 0, true, false);
    ok &= publish_ptrace_stop(tracee, 0x137f) && check(tracee.pid, 0, true, true);
    ok &= publish_job_control_stop(child, 19);
    ok &= check(child.pid, 0, true, false) && check(child.pid, OPTION_STOPPED, true, true);
    Claim claim{};
    ClaimedEvent event{};
    ok &= try_claim(owner, claim, child.pid, OPTION_STOPPED, event) == ProbeResult::CLAIMED;
    auto const COOKIE = event.claim_cookie;
    auto* const NODE = event.node;
    auto const OWNER_REFS = owner.ref_count.load();
    auto const CHILD_REFS = child.ref_count.load();
    ok &= check(child.pid, OPTION_STOPPED, true, false);
    ok &= claim.event == NODE && NODE->claim_cookie == COOKIE;
    ok &= owner.ref_count.load() == OWNER_REFS && child.ref_count.load() == CHILD_REFS;
    update_process_group(child, 88);
    // Both the old immutable claimed event and the new live group remain visible.
    ok &= check(selector_for_process_group(77), OPTION_STOPPED, true, false);
    auto const GROUPS = inspect_group_candidates(owner, 77, {5, 6}, {5, 6}, std::nullopt);
    ok &= GROUPS.flags == ker::mod::sched::session::GroupCandidates::LOCAL;
    child.resources.cpu_user_time_us = 5;
    child.resources.cpu_system_time_us = 3;
    child.exit_status = 37 << 8;
    ok &= publish_exit(child) && check(child.pid, 0, true, false);  // Subject serialization includes exit.
    release_claim(claim, event);
    ok &= check(child.pid, 0, true, true);
    ok &= try_claim(owner, claim, child.pid, 0, event) == ProbeResult::CLAIMED;
    ok &= commit_claim(owner, claim, event, true) && check(child.pid, 0, true, true);
    ok &= owner.child_user_time_us == 0 && owner.child_system_time_us == 0;
    ok &= try_claim(owner, claim, child.pid, 0, event) == ProbeResult::CLAIMED;
    ok &= commit_claim(owner, claim, event, false) && check(child.pid, 0, false, false);
    ok &= check(selector_for_process_group(77), OPTION_STOPPED, false, false);
    ok &= owner.child_user_time_us == 5 && owner.child_system_time_us == 3;
    detach_tracer(tracee);
    ok &= check(tracee.pid, 0, false, false);
    cancel_claim(claim);
    prepare_process_exit(owner);
    prepare_process_exit(other_owner);
    for (size_t i = 0; i < 4; ++i) ok &= tasks[i].ref_count.load() == 1;
    return ok;
}

auto selftest_queue_candidate_merging() -> bool {
    using ker::mod::sched::session::GroupCandidates;
    using ker::mod::sched::session::Origin;
    auto tasks = std::make_unique<Task[]>(6);
    Origin const A{7, 11}, B{8, 11}, C{7, 12};
    bool ok = true;
    for (size_t i = 0; i < 3; ++i) {
        // Separate synthetic namespaces deliberately share the same child PID.
        auto& owner = tasks[2 * i];
        auto& child = tasks[2 * i + 1];
        initialize_fixture(owner, 1, 55);
        initialize_fixture(child, 0x8F10, 55);
        child.exit_status = static_cast<int32_t>((37 + i) << 8);
        child.resources.cpu_user_time_us = i + 1;
        ok &= begin_publication(owner, child) && commit_publication(child);
    }
    constexpr uint64_t PID = 0x8F10;
    auto first = inspect_pid_candidates(tasks[0], PID, A);
    auto const SECOND = inspect_pid_candidates(tasks[2], PID, B);
    auto const THIRD = inspect_pid_candidates(tasks[4], PID, C);
    ok &= first.found && !first.ambiguous && first.origin == A;
    first.merge(first);
    first.merge(inspect_pid_candidates(tasks[0], PID, A));
    ok &= !first.ambiguous;  // Retries and repeated traversal are not new children.
    first.merge(SECOND);
    ok &= first.ambiguous;  // Live children count before either exit exists.
    auto reverse = SECOND;
    reverse.merge(inspect_pid_candidates(tasks[0], PID, A));
    ok &= reverse.ambiguous;
    auto reboot = inspect_pid_candidates(tasks[0], PID, A);
    reboot.merge(THIRD);
    ok &= reboot.ambiguous;  // Same node, distinct boot namespace.
    PidCandidates propagated;
    propagated.merge(first);
    ok &= propagated.found && propagated.ambiguous;
    for (uint64_t invalid : {uint64_t{0}, SELECT_PROCESS_GROUP, SELECT_ANY}) {
        ok &= !inspect_pid_candidates(tasks[0], invalid, A).found;
    }
    ok &= !inspect_pid_candidates(tasks[0], PID + 1, A).found;
    auto a = inspect_group_candidates(tasks[0], 55, A, A, B);
    auto b = inspect_group_candidates(tasks[2], 55, B, A, B);
    auto c = inspect_group_candidates(tasks[4], 55, C, A, B);
    ok &= a.flags == GroupCandidates::LOCAL && b.flags == GroupCandidates::OWN && c.flags == GroupCandidates::OTHER;
    b.merge(c);
    ok &= b.resolve(B) == B;
    b.merge(a);
    ok &= b.resolve(B) == Origin{};
    c.merge(a);
    c.merge(inspect_group_candidates(tasks[2], 55, B, A, B));
    ok &= c.resolve(B) == Origin{};
    auto foreign = inspect_group_candidates(tasks[2], 55, B, A, std::nullopt);
    foreign.merge(inspect_group_candidates(tasks[4], 55, C, A, std::nullopt));
    ok &= (foreign.flags & GroupCandidates::AMBIGUOUS) != 0 && !foreign.resolve(std::nullopt);
    // A foreign group origin is already physical and must not become C.
    update_job_identity(tasks[5], {.pgid = 55, .group_origin = B});
    auto const INHERITED = inspect_group_candidates(tasks[4], 55, C, A, B);
    ok &= INHERITED.flags == GroupCandidates::OWN;
    ok &= inspect_group_candidates(tasks[0], 0, A, A, B).flags == 0;
    ok &= inspect_group_candidates(tasks[0], SELECT_PROCESS_GROUP_MASK, A, A, B).flags == 0;
    for (size_t i = 0; i < 3; ++i) {
        auto& owner = tasks[2 * i];
        auto& child = tasks[2 * i + 1];
        ok &= publish_exit(child);
        auto const REFS = child.ref_count.load();
        auto const BEFORE = diagnostics(owner);
        auto collision = inspect_pid_candidates(owner, PID, i == 0 ? A : (i == 1 ? B : C));
        collision.merge(first);
        ok &= collision.ambiguous && diagnostics(owner).owned_event_count == BEFORE.owned_event_count;
        ok &= !child.child_parent_exit_event.claimed_by && child.ref_count.load() == REFS && owner.child_user_time_us == 0;
        // Ambiguous positive-PID discovery leaves any-child consumption usable.
        Claim claim{};
        ClaimedEvent event{};
        ok &= try_claim(owner, claim, SELECT_ANY, 0, event) == ProbeResult::CLAIMED;
        ok &= event.subject_pid == PID && event.status == static_cast<int32_t>((37 + i) << 8);
        ok &= commit_claim(owner, claim, event, false) && owner.child_user_time_us == i + 1;
        ok &= !inspect_pid_candidates(owner, PID, A).found;
        cancel_claim(claim);
        prepare_process_exit(owner);
    }
    for (size_t i = 0; i < 6; ++i) ok &= tasks[i].ref_count.load() == 1;
    return ok;
}

auto selftest_independent_claims() -> bool {
    auto tasks = std::make_unique<Task[]>(5);
    auto& owner = tasks[0];
    auto& other_owner = tasks[1];
    auto& first_child = tasks[2];
    auto& second_child = tasks[3];
    auto& local_waiter = tasks[4];
    initialize_fixture(owner, 0x8E00);
    initialize_fixture(other_owner, 0x8E01);
    initialize_fixture(first_child, 0x8E02);
    initialize_fixture(second_child, 0x8E03);
    initialize_fixture(local_waiter, 0x8E04);
    first_child.exit_status = 37 << 8;
    second_child.exit_status = 41 << 8;
    first_child.resources.cpu_user_time_us = 17;
    second_child.resources.cpu_user_time_us = 23;
    bool ok = begin_publication(owner, first_child) && commit_publication(first_child) && publish_exit(first_child);
    ok &= begin_publication(owner, second_child) && commit_publication(second_child) && publish_exit(second_child);
    Claim first{}, second{};
    ClaimedEvent a{}, b{}, none{};
    ok &= try_claim(owner, first, first_child.pid, 0, a) == ProbeResult::CLAIMED;
    // The same executor services another transaction without stealing a slot.
    ok &= try_claim(owner, second, second_child.pid, 0, b) == ProbeResult::CLAIMED;
    ok &= first.waiter == nullptr && second.waiter == nullptr && first.event == a.node && second.event == b.node;
    ok &= try_claim(owner, first, second_child.pid, 0, none) == ProbeResult::WOULD_BLOCK && first.event == a.node;
    release_claim(second, a);
    ok &= !commit_claim(other_owner, first, a, false) && !commit_claim(owner, second, a, false);
    ok &= second.event == b.node && first.event == a.node;
    ok &= claim_or_register(owner, local_waiter, first_child.pid, 0, true, none) == ProbeResult::WOULD_BLOCK;
    cancel_claim(first);
    ok &= first.event == nullptr && diagnostics(local_waiter).registered_selector == first_child.pid;
    ClaimedEvent local{};
    ok &= claim_or_register(owner, local_waiter, first_child.pid, 0, false, local) == ProbeResult::CLAIMED;
    ok &= local_waiter.ref_count.load() == 2 && local_waiter.child_claim.event == local.node;
    ok &= commit_claim(owner, local_waiter, local, true) && local_waiter.ref_count.load() == 1;
    ClaimedEvent reissued{};
    ok &= try_claim(owner, first, first_child.pid, 0, reissued) == ProbeResult::CLAIMED;
    release_claim(first, a);
    ok &= !commit_claim(owner, first, a, false) && first.event == reissued.node;
    ok &= commit_claim(owner, first, reissued, false) && !commit_claim(owner, first, reissued, false);
    ok &= commit_claim(owner, second, b, false) && !commit_claim(owner, second, b, true);
    cancel_claim(first);
    cancel_claim(second);
    cancel_wait(local_waiter);
    return ok && owner.child_user_time_us == 40 && owner.ref_count.load() == 1 && other_owner.ref_count.load() == 1 &&
           first_child.ref_count.load() == 1 && second_child.ref_count.load() == 1 && local_waiter.ref_count.load() == 1;
}

auto selftest_independent_claim_retirement() -> bool {
    bool ok = true;
    for (bool const TRACER : {false, true}) {
        Task owner{};
        initialize_fixture(owner, 1);  // Never reparent a fixture to live init.
        auto subject = std::make_unique<Task>();
        initialize_fixture(*subject, 0x8F01);
        subject->exit_status = 37 << 8;
        if (TRACER)
            ok &= attach_tracer(*subject, owner) && publish_ptrace_stop(*subject, (19 << 8) | STOP_STATUS_LOW);
        else
            ok &= begin_publication(owner, *subject) && commit_publication(*subject) && publish_exit(*subject);
        Claim claim{};
        ClaimedEvent old{};
        ok &= try_claim(owner, claim, subject->pid, OPTION_STOPPED, old) == ProbeResult::CLAIMED;
        if (TRACER)
            detach_tracer(*subject);
        else
            prepare_process_exit(owner);
        ok &= claim.event == nullptr && subject->ref_count.load() == 1;
        subject.reset();
        cancel_claim(claim);
        release_claim(claim, old);
        ok &= !commit_claim(owner, claim, old, false) && !commit_claim(owner, claim, old, true);
        ok &= owner.ref_count.load() == 1;
    }
    return ok;
}

auto selftest_retired_claim_storage() -> bool {
    bool ok = true;
    for (bool const TRACER : {false, true}) {
        Task owner{};
        Task waiter{};
        initialize_fixture(owner, 1);  // Parent cleanup cannot touch live init.
        initialize_fixture(waiter, 0x8C01);
        auto subject = std::make_unique<Task>();
        initialize_fixture(*subject, 0x8C02);
        subject->exit_status = 37 << 8;
        if (TRACER) {
            ok &= attach_tracer(*subject, owner) && publish_ptrace_stop(*subject, (19 << 8) | STOP_STATUS_LOW);
        } else {
            ok &= begin_publication(owner, *subject) && commit_publication(*subject) && publish_exit(*subject);
        }
        ClaimedEvent retired{};
        ok &= claim_or_register(owner, waiter, subject->pid, OPTION_STOPPED, false, retired) == ProbeResult::CLAIMED;
        auto const STATUS = retired.status;
        if (TRACER)
            detach_tracer(*subject);
        else
            prepare_process_exit(owner);
        ok &= waiter.child_claim.event == nullptr && subject->ref_count.load() == 1;
        subject.reset();  // Event storage is actually freed before both calls.
        release_claim(waiter, retired);
        ok &= !commit_claim(owner, waiter, retired, false) && !commit_claim(owner, waiter, retired, true);
        ok &= retired.status == STATUS && owner.ref_count.load() == 1 && waiter.ref_count.load() == 1;

        // A replacement may reuse the allocation. The stale token must neither
        // dereference retired storage nor disturb a newly owned claim.
        auto replacement = std::make_unique<Task>();
        initialize_fixture(*replacement, 0x8C03);
        ok &= attach_tracer(*replacement, owner) && publish_ptrace_stop(*replacement, (20 << 8) | STOP_STATUS_LOW);
        ClaimedEvent current{};
        ok &= claim_or_register(owner, waiter, replacement->pid, OPTION_STOPPED, false, current) == ProbeResult::CLAIMED;
        release_claim(waiter, retired);
        ok &= !commit_claim(owner, waiter, retired, false);
        ok &= waiter.child_claim.event == current.node && current.claim_cookie != retired.claim_cookie;
        ok &= commit_claim(owner, waiter, current, false);
        detach_tracer(*replacement);
        ok &= owner.ref_count.load() == 1 && waiter.ref_count.load() == 1 && replacement->ref_count.load() == 1;
    }
    return ok;
}

auto selftest_stale_claim_preserves_reissued_owner() -> bool {
    Task owner{};
    Task waiter{};
    Task subject{};
    initialize_fixture(owner, 0x8D00);
    initialize_fixture(waiter, 0x8D01);
    initialize_fixture(subject, 0x8D02);
    bool ok = attach_tracer(subject, owner) && publish_ptrace_stop(subject, (19 << 8) | STOP_STATUS_LOW);
    ClaimedEvent first{};
    ok &= claim_or_register(owner, waiter, subject.pid, OPTION_STOPPED, false, first) == ProbeResult::CLAIMED;
    cancel_wait(waiter);
    ClaimedEvent second{};
    ok &= claim_or_register(owner, waiter, subject.pid, OPTION_STOPPED, false, second) == ProbeResult::CLAIMED;
    ok &= second.node == first.node && second.claim_cookie != first.claim_cookie;
    release_claim(waiter, first);
    ok &= !commit_claim(owner, waiter, first, false) && !commit_claim(owner, waiter, first, true);
    ok &= waiter.child_claim.event == second.node && commit_claim(owner, waiter, second, true);
    ClaimedEvent final{};
    ok &= claim_or_register(owner, waiter, subject.pid, OPTION_STOPPED, false, final) == ProbeResult::CLAIMED;
    ok &= final.status == first.status && final.node == first.node && final.claim_cookie != second.claim_cookie;
    ok &= commit_claim(owner, waiter, final, false);
    release_claim(waiter, final);
    detach_tracer(subject);
    return ok && !diagnostics(waiter).claim_active && owner.ref_count.load() == 1 && waiter.ref_count.load() == 1 &&
           subject.ref_count.load() == 1;
}

auto selftest_group_origin_selection() -> bool {
    using ker::mod::sched::session::Origin;
    bool ok = true;
    for (Origin const OTHER : {Origin{8, 91}, Origin{7, 92}}) {
        auto tasks = std::make_unique<Task[]>(4);
        auto& parent = tasks[0];
        auto& inherited = tasks[1];
        auto& foreign = tasks[2];
        auto& local = tasks[3];
        Origin const OWN{7, 91};
        for (size_t i = 0; i < 4; ++i) initialize_fixture(tasks[i], 0x8A00 + i, 77);
        parent.initialize_job_identity({.pgid = 77, .group_origin = OWN});
        inherited.initialize_job_identity({.pgid = 77, .group_origin = OWN});
        foreign.initialize_job_identity({.pgid = 77, .group_origin = OTHER});
        for (size_t i = 1; i < 4; ++i) ok &= begin_publication(parent, tasks[i]);
        auto const CURRENT = selector_for_current_process_group(parent);
        ok &= CURRENT == selector_for_process_group(77, OWN);
        ok &= !selector_matches(CURRENT, foreign.pid, 77, OTHER) && !selector_matches(CURRENT, local.pid, 77, {});
        ok &= selector_matches(CURRENT, inherited.pid, 77, OWN);
        ok &= selector_matches(SELECT_ANY, foreign.pid, 77, OTHER) && selector_matches(foreign.pid, foreign.pid, 77, OTHER);
        ok &= selector_for_process_group(parent, 77) == selector_for_process_group(77);
        abort_publication(local);
        ok &= selector_for_process_group(parent, 77) == CURRENT;
        update_job_identity(parent, {.pgid = 88});
        ok &= selector_for_process_group(parent, 77).value == 0;
        ClaimedEvent none{};
        ok &= claim_or_register(parent, parent, selector_for_process_group(parent, 77), 0, false, none) == ProbeResult::NO_CHILD;
        abort_publication(foreign);
        ok &= selector_for_process_group(parent, 77) == CURRENT;
        abort_publication(inherited);
        ok &= selector_for_process_group(parent, 77).value == 0;
        for (size_t i = 0; i < 4; ++i) ok &= tasks[i].ref_count.load() == 1;
    }
    return ok;
}

auto selftest_group_origin_waiters_and_claims() -> bool {
    using ker::mod::sched::session::Origin;
    bool ok = true;
    for (Origin const OTHER : {Origin{8, 91}, Origin{7, 92}}) {
        auto tasks = std::make_unique<Task[]>(4);
        auto& parent = tasks[0];
        auto& waiter = tasks[1];
        auto& child = tasks[2];
        auto& foreign = tasks[3];
        for (size_t i = 0; i < 4; ++i) initialize_fixture(tasks[i], i == 0 ? 1 : 0x8B00 + i, 77);
        Origin const OWN{7, 91};
        parent.initialize_job_identity({.pgid = 77, .group_origin = OWN});
        child.initialize_job_identity({.pgid = 77, .group_origin = OWN});
        foreign.initialize_job_identity({.pgid = 77, .group_origin = OTHER});
        ok &= begin_publication(parent, child) && commit_publication(child);
        ok &= begin_publication(parent, foreign) && commit_publication(foreign);
        auto const SELECTOR = selector_for_current_process_group(parent);
        ClaimedEvent claim{};
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, true, claim) == ProbeResult::WOULD_BLOCK;
        ok &= publish_job_control_stop(foreign, 19) && !fixture_waiter_notified(waiter);
        update_job_identity(child, {.pgid = 77, .group_origin = OTHER});
        ok &= fixture_waiter_notified(waiter);
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, false, claim) == ProbeResult::NO_CHILD;
        cancel_wait(waiter);
        update_job_identity(child, {.pgid = 77, .group_origin = OWN});
        ok &= publish_job_control_stop(child, 19);
        update_job_identity(child, {.pgid = 77, .group_origin = OTHER});
        // Only the immutable old-origin event now names this group.
        ok &= selector_for_process_group(parent, 77) == SELECTOR;
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, false, claim) == ProbeResult::CLAIMED;
        ok &= claim.subject_pid == child.pid && claim.group_origin == OWN;
        ClaimedEvent competing{};
        ok &= claim_or_register(parent, parent, SELECTOR, OPTION_STOPPED, false, competing) == ProbeResult::WOULD_BLOCK;
        auto const COOKIE = claim.claim_cookie;
        auto* const NODE = claim.node;
        release_claim(waiter, claim);  // A failed usercopy does not consume the snapshot.
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, false, claim) == ProbeResult::CLAIMED;
        ok &= claim.node == NODE && claim.claim_cookie != COOKIE && claim.group_origin == OWN;
        ok &= commit_claim(parent, waiter, claim, true);
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, false, claim) == ProbeResult::CLAIMED;
        ok &= commit_claim(parent, waiter, claim, false);
        ok &= claim_or_register(parent, waiter, SELECTOR, OPTION_STOPPED, false, claim) == ProbeResult::NO_CHILD;
        // PID and any-child claims still access the remaining owned subjects.
        ok &= claim_or_register(parent, waiter, foreign.pid, OPTION_STOPPED, false, claim) == ProbeResult::CLAIMED;
        ok &= claim.group_origin == OTHER && commit_claim(parent, waiter, claim, false);
        child.exit_status = 37 << 8;
        ok &= publish_exit(child);
        ok &= claim_or_register(parent, waiter, SELECT_ANY, 0, false, claim) == ProbeResult::CLAIMED;
        ok &= claim.subject_pid == child.pid && claim.group_origin == OTHER && claim.status == (37 << 8);
        ok &= commit_claim(parent, waiter, claim, false) && !commit_claim(parent, waiter, claim, false);
        cancel_wait(waiter);
        cancel_wait(parent);
        prepare_process_exit(parent);  // Private PID1: never reparents into real init.
        for (size_t i = 0; i < 4; ++i) ok &= tasks[i].ref_count.load() == 1;
    }
    return ok;
}

auto selftest_exit_includes_retired_thread_cpu() -> bool {
    Task parent{};
    Task child{};
    initialize_fixture(parent, 0x8150);
    initialize_fixture(child, 0x8151);
    child.state.store(sched_task::TaskState::ACTIVE);
    static_cast<void>(child.resources.charge_cpu(100, 70));
    child.child_user_time_us = 13;
    child.child_system_time_us = 17;
    {
        Task thread{};
        thread.type = child.type;
        thread.is_thread = true;
        thread.owner_pid = child.pid;
        if (!ker::mod::sched::resource::bind_thread_owner(thread, child)) return false;
        ker::mod::sched::resource::charge_cpu(&thread, 200, 140);
    }
    child.state.store(sched_task::TaskState::EXITING);
    if (!begin_publication(parent, child) || !commit_publication(child) || !publish_exit(child)) return false;
    ClaimedEvent claimed{};
    bool const CLAIMED = claim_or_register(parent, parent, child.pid, 0, false, claimed) == ProbeResult::CLAIMED;
    bool const CORRECT =
        CLAIMED && claimed.user_time_us == 223 && claimed.system_time_us == 107 && child.resource_thread_owners.load() == 0;
    bool const REAPED = CLAIMED && commit_claim(parent, parent, claimed, false);
    bool const DUPLICATE_REJECTED = !commit_claim(parent, parent, claimed, false);
    return CORRECT && REAPED && DUPLICATE_REJECTED && parent.child_user_time_us == 223 && parent.child_system_time_us == 107;
}

auto selftest_publish_and_registration_ordering() -> bool {
    Task parent{};
    Task waiter{};
    Task child{};
    initialize_fixture(parent, 0x8100);
    initialize_fixture(waiter, 0x8101);
    initialize_fixture(child, 0x8102, 0x81A0);
    waiter.owner_pid = parent.pid;
    child.exit_status = 0x3400;
    child.user_time_us = 71;
    child.system_time_us = 29;
    static_cast<void>(child.resources.charge_cpu(100, 71));

    if (!begin_publication(parent, child) || !commit_publication(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent empty{};
    bool const REGISTERED = claim_or_register(parent, waiter, child.pid, 0, true, empty) == ProbeResult::WOULD_BLOCK;
    Diagnostics const WAITING = diagnostics(waiter);
    bool const WAIT_VISIBLE = WAITING.registered_selector == child.pid && diagnostics(parent).owned_waiter_count == 1;
    cancel_wait(waiter);  // Models an interrupting signal before publication.
    bool const CANCELLED = diagnostics(waiter).registered_selector == 0 && diagnostics(parent).owned_waiter_count == 0;
    bool const REREGISTERED = claim_or_register(parent, waiter, child.pid, 0, true, empty) == ProbeResult::WOULD_BLOCK;
    bool const PUBLISHED = publish_exit(child);

    ClaimedEvent claimed{};
    bool const CLAIMED = claim_or_register(parent, waiter, child.pid, 0, false, claimed) == ProbeResult::CLAIMED;
    bool const SNAPSHOT = CLAIMED && claimed.subject_pid == child.pid && claimed.process_group == child.job_identity().pgid &&
                          claimed.status == child.exit_status && claimed.user_time_us == child.user_time_us &&
                          claimed.system_time_us == child.system_time_us;
    bool const CONSUMED = CLAIMED && commit_claim(parent, waiter, claimed, false);
    return REGISTERED && WAIT_VISIBLE && CANCELLED && REREGISTERED && PUBLISHED && SNAPSHOT && CONSUMED && fixture_is_unlinked(child) &&
           parent.child_user_time_us == child.user_time_us && parent.child_system_time_us == child.system_time_us &&
           parent.ref_count.load(std::memory_order_acquire) == 1 && waiter.ref_count.load(std::memory_order_acquire) == 1 &&
           child.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_claim_retry_nowait_and_single_winner() -> bool {
    Task parent{};
    Task first_waiter{};
    Task second_waiter{};
    Task child{};
    initialize_fixture(parent, 0x8200);
    initialize_fixture(first_waiter, 0x8201);
    initialize_fixture(second_waiter, 0x8202);
    initialize_fixture(child, 0x8203);
    first_waiter.owner_pid = parent.pid;
    second_waiter.owner_pid = parent.pid;
    child.exit_status = 0x5600;

    if (!begin_publication(parent, child) || !commit_publication(child) || !publish_exit(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent first{};
    bool const FIRST_CLAIM = claim_or_register(parent, first_waiter, SELECT_ANY, 0, false, first) == ProbeResult::CLAIMED;
    ClaimedEvent competing{};
    bool const SINGLE_WINNER = claim_or_register(parent, second_waiter, SELECT_ANY, 0, false, competing) == ProbeResult::WOULD_BLOCK;

    uint64_t const FIRST_COOKIE = first.claim_cookie;
    release_claim(first_waiter, first);  // Models failed usercopy: event must remain retryable.
    ClaimedEvent retry{};
    bool const RETRIED = claim_or_register(parent, second_waiter, SELECT_ANY, 0, false, retry) == ProbeResult::CLAIMED &&
                         retry.node == first.node && retry.claim_cookie != FIRST_COOKIE;
    bool const NOWAIT = RETRIED && commit_claim(parent, second_waiter, retry, true) && !fixture_is_unlinked(child);

    ClaimedEvent final{};
    bool const RECLAIMED = claim_or_register(parent, first_waiter, SELECT_ANY, 0, false, final) == ProbeResult::CLAIMED &&
                           commit_claim(parent, first_waiter, final, false);
    return FIRST_CLAIM && SINGLE_WINNER && RETRIED && NOWAIT && RECLAIMED && fixture_is_unlinked(child) &&
           first_waiter.ref_count.load(std::memory_order_acquire) == 1 && second_waiter.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_waiter_handoff_and_subject_serialization() -> bool {
    Task parent{};
    Task first_waiter{};
    Task second_waiter{};
    Task child{};
    initialize_fixture(parent, 0x8280);
    initialize_fixture(first_waiter, 0x8281);
    initialize_fixture(second_waiter, 0x8282);
    initialize_fixture(child, 0x8283);
    first_waiter.owner_pid = parent.pid;
    second_waiter.owner_pid = parent.pid;
    child.exit_status = 0x5A00;

    if (!begin_publication(parent, child) || !commit_publication(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent empty{};
    bool const FIRST_REGISTERED = claim_or_register(parent, first_waiter, SELECT_ANY, 0, true, empty) == ProbeResult::WOULD_BLOCK;
    bool const SECOND_REGISTERED = claim_or_register(parent, second_waiter, SELECT_ANY, 0, true, empty) == ProbeResult::WOULD_BLOCK;
    bool const STOP_PUBLISHED = publish_job_control_stop(child, 19);
    bool const EXIT_PUBLISHED = publish_exit(child);
    bool const SECOND_SELECTED = fixture_waiter_notified(second_waiter) && !fixture_waiter_notified(first_waiter);

    // Model an interrupt arriving after the first eligible waiter was selected
    // but before it could claim.  Cancellation must hand the event to the next
    // waiter instead of leaving the queue stranded behind a notified bit.
    cancel_wait(second_waiter);
    bool const HANDED_OFF = fixture_waiter_notified(first_waiter) && diagnostics(second_waiter).registered_selector == 0;

    // The ordinary waiter ignores the older job-control stop and claims exit.
    // A WUNTRACED competitor must register behind that subject-wide claim,
    // rather than copying a stop that exit reaping is about to invalidate.
    ClaimedEvent exit_claim{};
    bool const EXIT_CLAIMED = claim_or_register(parent, first_waiter, SELECT_ANY, 0, false, exit_claim) == ProbeResult::CLAIMED &&
                              exit_claim.kind == ChildEventKind::EXIT;
    ClaimedEvent competing{};
    bool const SUBJECT_SERIALIZED =
        claim_or_register(parent, second_waiter, SELECT_ANY, OPTION_STOPPED, true, competing) == ProbeResult::WOULD_BLOCK &&
        !fixture_waiter_notified(second_waiter);

    bool const EXIT_COMMITTED = EXIT_CLAIMED && commit_claim(parent, first_waiter, exit_claim, false);
    bool const LOSER_WOKEN_FOR_NO_CHILD = fixture_waiter_notified(second_waiter);
    ClaimedEvent none{};
    bool const LOSER_OBSERVED_NO_CHILD =
        claim_or_register(parent, second_waiter, SELECT_ANY, OPTION_STOPPED, false, none) == ProbeResult::NO_CHILD;

    return FIRST_REGISTERED && SECOND_REGISTERED && STOP_PUBLISHED && EXIT_PUBLISHED && SECOND_SELECTED && HANDED_OFF &&
           SUBJECT_SERIALIZED && EXIT_COMMITTED && LOSER_WOKEN_FOR_NO_CHILD && LOSER_OBSERVED_NO_CHILD && fixture_is_unlinked(child) &&
           parent.ref_count.load(std::memory_order_acquire) == 1 && first_waiter.ref_count.load(std::memory_order_acquire) == 1 &&
           second_waiter.ref_count.load(std::memory_order_acquire) == 1 && child.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_group_change_wakes_invalidated_selector() -> bool {
    Task parent{};
    Task waiter{};
    Task child{};
    initialize_fixture(parent, 0x82A0);
    initialize_fixture(waiter, 0x82A1);
    initialize_fixture(child, 0x82A2, 0x82B0);
    waiter.owner_pid = parent.pid;

    if (!begin_publication(parent, child) || !commit_publication(child)) {
        abort_publication(child);
        return false;
    }

    auto const OLD_SELECTOR = selector_for_process_group(child.job_identity().pgid);
    ClaimedEvent empty{};
    bool const REGISTERED = claim_or_register(parent, waiter, OLD_SELECTOR, 0, true, empty) == ProbeResult::WOULD_BLOCK;
    update_process_group(child, 0x82C0);
    bool const SELECTOR_INVALIDATED = fixture_waiter_notified(waiter);
    bool const OBSERVED_NO_CHILD = claim_or_register(parent, waiter, OLD_SELECTOR, 0, false, empty) == ProbeResult::NO_CHILD;

    child.exit_status = 0;
    ClaimedEvent exited{};
    bool const REAPED = publish_exit(child) &&
                        claim_or_register(parent, parent, selector_for_process_group(0x82C0), 0, false, exited) == ProbeResult::CLAIMED &&
                        commit_claim(parent, parent, exited, false);
    return REGISTERED && SELECTOR_INVALIDATED && OBSERVED_NO_CHILD && REAPED && fixture_is_unlinked(child) &&
           parent.ref_count.load(std::memory_order_acquire) == 1 && waiter.ref_count.load(std::memory_order_acquire) == 1 &&
           child.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_selectors_and_publication_race() -> bool {
    Task parent{};
    Task child{};
    initialize_fixture(parent, 0x8300, 0x83A0);
    initialize_fixture(child, 0x8301, 0x83B0);
    child.exit_status = 0x7800;

    if (selector_for_current_process_group(parent) != selector_for_process_group(parent.job_identity().pgid) ||
        !begin_publication(parent, child)) {
        abort_publication(child);
        return false;
    }
    update_process_group(child, 0x83C0);
    child.scheduler_published.store(true, std::memory_order_release);
    if (!publish_exit(child)) {
        return false;
    }

    ClaimedEvent wrong{};
    bool const WRONG_GROUP_REJECTED =
        claim_or_register(parent, parent, selector_for_process_group(0x83B0), 0, false, wrong) == ProbeResult::NO_CHILD;
    ClaimedEvent matching{};
    bool const MATCHED_NEW_GROUP =
        claim_or_register(parent, parent, selector_for_process_group(0x83C0), 0, false, matching) == ProbeResult::CLAIMED &&
        matching.process_group == 0x83C0;
    bool const REAPED_BEFORE_CREATOR_COMMIT = MATCHED_NEW_GROUP && commit_claim(parent, parent, matching, false);
    bool const IDEMPOTENT_CREATOR_COMMIT = commit_publication(child);
    return WRONG_GROUP_REJECTED && REAPED_BEFORE_CREATOR_COMMIT && IDEMPOTENT_CREATOR_COMMIT && fixture_is_unlinked(child);
}

auto selftest_ptrace_exit_preserves_parent_event() -> bool {
    Task parent{};
    Task tracer{};
    Task child{};
    initialize_fixture(parent, 0x8400);
    initialize_fixture(tracer, 0x8401);
    initialize_fixture(child, 0x8402);
    child.exit_status = 0x9A00;
    child.user_time_us = 17;
    static_cast<void>(child.resources.charge_cpu(17, 17));

    if (!begin_publication(parent, child) || !commit_publication(child) || !attach_tracer(child, tracer) || !publish_exit(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent tracer_exit{};
    bool const TRACER_CLAIMED = claim_or_register(tracer, tracer, child.pid, 0, false, tracer_exit) == ProbeResult::CLAIMED &&
                                tracer_exit.audience == ChildEventAudience::TRACER;
    bool const TRACER_CONSUMED = TRACER_CLAIMED && commit_claim(tracer, tracer, tracer_exit, false);
    bool const PARENT_STILL_OWNS = child.child_parent == &parent &&
                                   child.child_membership_state.load(std::memory_order_acquire) == ChildMembershipState::ZOMBIE &&
                                   parent.child_user_time_us == 0;

    ClaimedEvent parent_exit{};
    bool const PARENT_REAPED = claim_or_register(parent, parent, child.pid, 0, false, parent_exit) == ProbeResult::CLAIMED &&
                               parent_exit.audience == ChildEventAudience::PARENT && commit_claim(parent, parent, parent_exit, false);
    return TRACER_CONSUMED && PARENT_STILL_OWNS && PARENT_REAPED && fixture_is_unlinked(child) &&
           parent.child_user_time_us == child.user_time_us;
}

auto selftest_dual_observer_zombie_lifetime() -> bool {
    Task parent{};
    Task tracer{};
    Task child{};
    initialize_fixture(parent, 0x8480);
    initialize_fixture(tracer, 0x8481);
    initialize_fixture(child, 0x8482);
    child.exit_status = 0xA500;

    if (!begin_publication(parent, child) || !commit_publication(child) || !attach_tracer(child, tracer) || !publish_exit(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent parent_exit{};
    bool const PARENT_REAPED = claim_or_register(parent, parent, child.pid, 0, false, parent_exit) == ProbeResult::CLAIMED &&
                               parent_exit.audience == ChildEventAudience::PARENT && commit_claim(parent, parent, parent_exit, false);
    bool const RETAINED_FOR_TRACER =
        PARENT_REAPED && child.child_parent == nullptr && is_waitable_zombie(child) && diagnostics(tracer).owned_event_count == 1;

    ClaimedEvent tracer_exit{};
    bool const TRACER_CONSUMED = RETAINED_FOR_TRACER &&
                                 claim_or_register(tracer, tracer, child.pid, 0, false, tracer_exit) == ProbeResult::CLAIMED &&
                                 tracer_exit.audience == ChildEventAudience::TRACER && commit_claim(tracer, tracer, tracer_exit, false);
    return TRACER_CONSUMED && fixture_is_unlinked(child) && parent.ref_count.load(std::memory_order_acquire) == 1 &&
           tracer.ref_count.load(std::memory_order_acquire) == 1 && child.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_stop_continue_order_and_backpressure() -> bool {
    Task parent{};
    Task child{};
    initialize_fixture(parent, 0x8500);
    initialize_fixture(child, 0x8501);
    if (!begin_publication(parent, child) || !commit_publication(child) || !publish_job_control_stop(child, 19) ||
        !publish_continued(child)) {
        abort_publication(child);
        return false;
    }

    ClaimedEvent continued{};
    bool const CONTINUED_CLAIMED =
        claim_or_register(parent, parent, child.pid, OPTION_CONTINUED, false, continued) == ProbeResult::CLAIMED &&
        continued.kind == ChildEventKind::CONTINUED && continued.status == CONTINUED_STATUS;
    bool const CONTINUED_CONSUMED = CONTINUED_CLAIMED && commit_claim(parent, parent, continued, false);
    bool const STOP_BACKPRESSURED = !publish_job_control_stop(child, 20);

    ClaimedEvent stopped{};
    bool const STOP_CLAIMED = claim_or_register(parent, parent, child.pid, OPTION_STOPPED, false, stopped) == ProbeResult::CLAIMED &&
                              stopped.kind == ChildEventKind::JOB_CONTROL_STOP && stopped.status == ((19 << 8) | STOP_STATUS_LOW);
    bool const STOP_CONSUMED = STOP_CLAIMED && commit_claim(parent, parent, stopped, false);
    bool const RETRIED_STOP = STOP_CONSUMED && publish_job_control_stop(child, 20);

    ClaimedEvent retried{};
    bool const RETRIED_CONSUMED = RETRIED_STOP &&
                                  claim_or_register(parent, parent, child.pid, OPTION_STOPPED, false, retried) == ProbeResult::CLAIMED &&
                                  retried.status == ((20 << 8) | STOP_STATUS_LOW) && commit_claim(parent, parent, retried, false);
    child.exit_status = 0;
    ClaimedEvent exited{};
    bool const EXIT_REAPED = publish_exit(child) &&
                             claim_or_register(parent, parent, child.pid, 0, false, exited) == ProbeResult::CLAIMED &&
                             commit_claim(parent, parent, exited, false);
    return CONTINUED_CONSUMED && STOP_BACKPRESSURED && STOP_CONSUMED && RETRIED_CONSUMED && EXIT_REAPED;
}

auto selftest_parent_event_transfer() -> bool {
    Task old_parent{};
    Task new_parent{};
    Task child{};
    initialize_fixture(old_parent, 0x8600);
    initialize_fixture(new_parent, 0x8601);
    initialize_fixture(child, 0x8602);
    if (!begin_publication(old_parent, child) || !commit_publication(child) || !publish_job_control_stop(child, 19) ||
        !take_lifetime_ref(&new_parent)) {
        abort_publication(child);
        return false;
    }

    auto const OLD_VERSION = inspect_queue(old_parent, SELECT_ANY, OPTION_STOPPED).version;
    auto const NEW_VERSION = inspect_queue(new_parent, SELECT_ANY, OPTION_STOPPED).version;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    child_list_remove_locked(old_parent, child);
    child.child_parent = &new_parent;
    child.parent_pid = new_parent.pid;
    child_list_insert_locked(new_parent, child);
    bool const TRANSFERRED = transfer_parent_events_locked(old_parent, &new_parent, child);
    old_parent.release();
    g_lifecycle_lock.unlock_irqrestore(FLAGS);

    ClaimedEvent moved{};
    bool const MOVED_CLAIMED = TRANSFERRED && diagnostics(old_parent).owned_event_count == 0 &&
                               claim_or_register(new_parent, new_parent, child.pid, OPTION_STOPPED, false, moved) == ProbeResult::CLAIMED;
    bool const MOVED_CONSUMED = MOVED_CLAIMED && commit_claim(new_parent, new_parent, moved, false);
    child.exit_status = 0;
    ClaimedEvent exited{};
    bool const REAPED = MOVED_CONSUMED && publish_exit(child) &&
                        claim_or_register(new_parent, new_parent, child.pid, 0, false, exited) == ProbeResult::CLAIMED &&
                        commit_claim(new_parent, new_parent, exited, false);
    return TRANSFERRED && MOVED_CONSUMED && REAPED && fixture_is_unlinked(child) &&
           validate_queue_version(old_parent, OLD_VERSION) == QueueVersionStatus::STALE &&
           validate_queue_version(new_parent, NEW_VERSION) == QueueVersionStatus::STALE &&
           old_parent.ref_count.load(std::memory_order_acquire) == 1 && new_parent.ref_count.load(std::memory_order_acquire) == 1;
}

auto selftest_parent_exit_discards_without_reaper() -> bool {
    Task parent{};
    Task child{};
    // PID 1 deliberately has no higher reaper, exercising the explicit
    // discard/unlink rule without touching the live KTEST init task.
    initialize_fixture(parent, 1);
    initialize_fixture(child, 0x8701);
    if (!begin_publication(parent, child) || !commit_publication(child) || !publish_job_control_stop(child, 19)) {
        abort_publication(child);
        return false;
    }
    prepare_process_exit(parent);
    return fixture_is_unlinked(child) && diagnostics(parent).owned_event_count == 0 && parent.child_list_head == nullptr &&
           parent.ref_count.load(std::memory_order_acquire) == 1 && child.ref_count.load(std::memory_order_acquire) == 1;
}
#endif

}  // namespace ker::syscall::process::child_events
