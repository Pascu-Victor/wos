#include "child_events.hpp"

#include <atomic>
#include <cstdint>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::syscall::process::child_events {
namespace {

using ChildEvent = sched_task::ChildEvent;
using ChildEventAudience = sched_task::ChildEventAudience;
using ChildEventKind = sched_task::ChildEventKind;
using ChildMembershipState = sched_task::ChildMembershipState;
using ChildWaitRegistration = sched_task::ChildWaitRegistration;
using Task = sched_task::Task;

constexpr uint32_t SIGCHLD_NUMBER = 17;
constexpr int32_t STOP_STATUS_LOW = 0x7f;
constexpr int32_t CONTINUED_STATUS = 0xffff;

// Lock order: task-registry lookup (reference only) -> lifecycle -> runqueue.
// No caller may enter this lock while holding a runqueue or WKI lock.  Wakes
// occur only after it is released.
ker::mod::sys::Spinlock g_lifecycle_lock;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_event_sequence{1};     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_claim_cookie{1};       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> g_waiter_generation{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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

[[nodiscard]] auto effective_process_group(const Task& task) -> uint64_t {
    uint64_t const PROCESS_GROUP = task.pgid;
    return PROCESS_GROUP != 0 ? PROCESS_GROUP : sched_task::process_pid(task);
}

void child_list_insert_locked(Task& parent, Task& child) {
    child.child_sibling_prev = nullptr;
    child.child_sibling_next = parent.child_list_head;
    if (parent.child_list_head != nullptr) {
        parent.child_list_head->child_sibling_prev = &child;
    }
    parent.child_list_head = &child;
}

void child_list_remove_locked(Task& parent, Task& child) {
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
    event.process_group = effective_process_group(subject);
    event.user_time_us = sched_task::task_rusage_user_time_us(subject);
    event.system_time_us = sched_task::task_rusage_system_time_us(subject);
    event.status = status;
    event.kind = kind;
    event.audience = audience;
    event.queued = true;
    queue_insert_locked(owner, event);
    return true;
}

void clear_event_claim_locked(ChildEvent& event) {
    Task* const CLAIMANT = event.claimed_by;
    if (event.subject != nullptr) {
        ChildEvent*& slot = event.audience == ChildEventAudience::PARENT ? event.subject->child_parent_claimed_event
                                                                         : event.subject->child_tracer_claimed_event;
        if (slot == &event) {
            slot = nullptr;
        }
    }
    if (CLAIMANT != nullptr && CLAIMANT->child_claimed_event == &event) {
        CLAIMANT->child_claimed_event = nullptr;
    }
    event.claimed_by = nullptr;
    event.claim_cookie = 0;
    drop_lifetime_ref(CLAIMANT);
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

[[nodiscard]] auto event_matches(const ChildEvent& event, uint64_t selector, int32_t options) -> bool {
    if (!selector_matches(selector, event.subject_pid, event.process_group)) {
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

[[nodiscard]] auto first_matching_event_locked(Task& owner, uint64_t selector, int32_t options) -> ChildEvent* {
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

[[nodiscard]] auto has_potential_subject_locked(Task& owner, uint64_t selector) -> bool {
    // Claimed events remain potential children for competing waiters.  They
    // must observe WOULD_BLOCK, not ECHILD, until the single winner commits or
    // releases the claim.
    for (ChildEvent* event = owner.child_event_head; event != nullptr; event = event->next) {
        if (selector_matches(selector, event->subject_pid, event->process_group)) {
            return true;
        }
    }
    for (Task* child = owner.child_list_head; child != nullptr; child = child->child_sibling_next) {
        if (child->child_membership_state.load(std::memory_order_relaxed) != ChildMembershipState::UNLINKED &&
            selector_matches(selector, child->pid, effective_process_group(*child))) {
            return true;
        }
    }
    for (Task* tracee = owner.child_tracee_head; tracee != nullptr; tracee = tracee->child_tracee_next) {
        if (selector_matches(selector, tracee->pid, effective_process_group(*tracee))) {
            return true;
        }
    }
    return false;
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

[[nodiscard]] auto waiter_register_locked(Task& owner, Task& waiter, uint64_t selector, int32_t options) -> bool {
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
        wake.signal_owner->signal_add_pending_mask(1ULL << (SIGCHLD_NUMBER - 1U));
        ker::mod::sched::wake_task_from_event(wake.signal_owner);
    }
    if (wake.waiter != nullptr && wake.waiter != wake.signal_owner) {
        ker::mod::sched::wake_task_from_event(wake.waiter);
    }
    if (wake.subject != nullptr && wake.subject != wake.signal_owner && wake.subject != wake.waiter) {
        ker::mod::sched::wake_task_from_event(wake.subject);
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
    tracee.child_tracee_prev = nullptr;
    tracee.child_tracee_next = tracer.child_tracee_head;
    if (tracer.child_tracee_head != nullptr) {
        tracer.child_tracee_head->child_tracee_prev = &tracee;
    }
    tracer.child_tracee_head = &tracee;
}

void tracee_list_remove_locked(Task& tracer, Task& tracee) {
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
    uint64_t const OWNER_PID = sched_task::process_pid(task);
    if (OWNER_PID == task.pid) {
        return task.try_acquire_lifetime_ref() ? &task : nullptr;
    }
    return ker::mod::sched::find_task_by_pid_safe(OWNER_PID);
}

auto selector_for_process_group(uint64_t process_group) -> uint64_t {
    if (process_group == 0 || process_group >= SELECT_PROCESS_GROUP_MASK) {
        return 0;
    }
    return SELECT_PROCESS_GROUP | process_group;
}

auto selector_for_current_process_group(Task& process) -> uint64_t {
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    uint64_t const PROCESS_GROUP = effective_process_group(process);
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return selector_for_process_group(PROCESS_GROUP);
}

void update_process_group(Task& process, uint64_t process_group) {
    Task* parent = nullptr;
    Task* tracer = nullptr;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    process.pgid = process_group;
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
}

auto selector_matches(uint64_t selector, uint64_t pid, uint64_t process_group) -> bool {
    if (selector == SELECT_ANY) {
        return true;
    }
    if ((selector & SELECT_PROCESS_GROUP) != 0) {
        return process_group == (selector & SELECT_PROCESS_GROUP_MASK);
    }
    return pid == selector;
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
        uint32_t parent_death_signal = 0;
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
        parent_death_signal = child->parent_death_signal;
        g_lifecycle_lock.unlock_irqrestore(FLAGS);

        deliver_wake(init_wake);
        if (CHILD_REF) {
            if (parent_death_signal > 0 && parent_death_signal <= 64) {
                child->signal_add_pending_mask(1ULL << (parent_death_signal - 1U));
                ker::mod::sched::wake_task_from_event(child);
            }
            child->release();
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
        claimant = EVENT->claimed_by;
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
    ChildEvent* const EVENT = waiter.child_claimed_event;
    if (EVENT != nullptr && EVENT->queued && EVENT->claimed_by == &waiter) {
        Task* const OWNER = EVENT->queue_owner;
        if (owner == nullptr && take_lifetime_ref(OWNER)) {
            owner = OWNER;
        }
        clear_event_claim_locked(*EVENT);
    }
    if (owner != nullptr) {
        wake = make_wake_selection_locked(*owner, false);
    }
    waiter.child_claimed_event = nullptr;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
    drop_lifetime_ref(owner);
}

auto claim_or_register(Task& owner, Task& waiter, uint64_t selector, int32_t options, bool register_if_empty, ClaimedEvent& claimed)
    -> ProbeResult {
    claimed = {};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (ChildEvent* const EVENT = first_matching_event_locked(owner, selector, options); EVENT != nullptr) {
        if (!take_lifetime_ref(&waiter)) {
            g_lifecycle_lock.unlock_irqrestore(FLAGS);
            return ProbeResult::WOULD_BLOCK;
        }
        waiter_unlink_locked(waiter.child_wait_registration);
        EVENT->claimed_by = &waiter;
        EVENT->claim_cookie = g_claim_cookie.fetch_add(1, std::memory_order_relaxed);
        publish_subject_claim_locked(*EVENT);
        waiter.child_claimed_event = EVENT;
        claimed.node = EVENT;
        claimed.claim_cookie = EVENT->claim_cookie;
        claimed.subject_pid = EVENT->subject_pid;
        claimed.process_group = EVENT->process_group;
        claimed.user_time_us = EVENT->user_time_us;
        claimed.system_time_us = EVENT->system_time_us;
        claimed.status = EVENT->status;
        claimed.kind = EVENT->kind;
        claimed.audience = EVENT->audience;
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        return ProbeResult::CLAIMED;
    }

    if (!has_potential_subject_locked(owner, selector)) {
        waiter_unlink_locked(waiter.child_wait_registration);
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        return ProbeResult::NO_CHILD;
    }
    if (register_if_empty && !waiter_register_locked(owner, waiter, selector, options)) {
        g_lifecycle_lock.unlock_irqrestore(FLAGS);
        return ProbeResult::NO_CHILD;
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return ProbeResult::WOULD_BLOCK;
}

void release_claim(Task& waiter, const ClaimedEvent& claimed) {
    WakeSelection wake{};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ChildEvent* const EVENT = claimed.node;
    if (EVENT != nullptr && EVENT->queued && EVENT->claimed_by == &waiter && EVENT->claim_cookie == claimed.claim_cookie) {
        Task* const OWNER = EVENT->queue_owner;
        clear_event_claim_locked(*EVENT);
        if (OWNER != nullptr) {
            wake = make_wake_selection_locked(*OWNER, false);
        }
    }
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    deliver_wake(wake);
}

auto commit_claim(Task& owner, Task& waiter, const ClaimedEvent& claimed, bool keep_event) -> bool {
    WakeSelection wake{};
    bool reaped = false;
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    ChildEvent* const EVENT = claimed.node;
    if (EVENT == nullptr || !EVENT->queued || EVENT->queue_owner != &owner || EVENT->claimed_by != &waiter ||
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
            owner.child_user_time_us += claimed.user_time_us;
            owner.child_system_time_us += claimed.system_time_us;
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

auto is_waitable_zombie(const Task& task) -> bool {
    return task.child_membership_state.load(std::memory_order_acquire) == ChildMembershipState::ZOMBIE;
}

auto diagnostics(Task& task) -> Diagnostics {
    Diagnostics result{};
    uint64_t const FLAGS = g_lifecycle_lock.lock_irqsave();
    if (task.child_wait_registration.registered) {
        result.registered_selector = task.child_wait_registration.selector;
    }
    for (ChildEvent* event = task.child_event_head; event != nullptr; event = event->next) {
        result.owned_event_count++;
    }
    for (ChildWaitRegistration* waiter = task.child_waiter_head; waiter != nullptr; waiter = waiter->next) {
        result.owned_waiter_count++;
    }
    result.membership = task.child_membership_state.load(std::memory_order_relaxed);
    result.claim_active = task.child_claimed_event != nullptr;
    g_lifecycle_lock.unlock_irqrestore(FLAGS);
    return result;
}

#ifdef WOS_SELFTEST
namespace {

void initialize_fixture(Task& task, uint64_t pid, uint64_t process_group = 0) {
    task.type = sched_task::TaskType::PROCESS;
    task.pid = pid;
    task.pgid = process_group != 0 ? process_group : pid;
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
    bool const SNAPSHOT = CLAIMED && claimed.subject_pid == child.pid && claimed.process_group == child.pgid &&
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

    uint64_t const OLD_SELECTOR = selector_for_process_group(child.pgid);
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

    if (selector_for_current_process_group(parent) != selector_for_process_group(parent.pgid) || !begin_publication(parent, child)) {
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
