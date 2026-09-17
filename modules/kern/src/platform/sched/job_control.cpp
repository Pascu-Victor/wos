#include "job_control.hpp"

#include <atomic>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/signal_pending.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <syscalls_impl/process/child_events.hpp>

namespace ker::mod::sched::job_control {
namespace {
constexpr uint64_t STOP = uint64_t{1} << 18;
constexpr uint64_t CONT = uint64_t{1} << 17;
constexpr uint64_t KILL = uint64_t{1} << 8;
constexpr uint64_t DEFAULT_STOPS = (uint64_t{1} << 19) | (uint64_t{1} << 20) | (uint64_t{1} << 21);
mod::sys::Spinlock work_lock;
task::Task* work_head = nullptr;
task::Task* work_tail = nullptr;

void clear_local_stop(task::Task& subject) {
    subject.jobctl_stopped.store(false, std::memory_order_release);
    subject.jobctl_stop_publish_deferred.store(false, std::memory_order_release);
    subject.set_voluntary_blocked(false);
    subject.wants_block = false;
    subject.wake_at_us = 0;
    subject.clear_wait_channel();
}
}  // namespace

struct Access {
    static auto bound(task::Task& subject, task::Task* owner) -> bool { return subject.jobctl_membership.m_owner == owner; }

    static void unlink(Membership& member) {
        auto* owner = member.m_owner;
        if (member.m_previous != nullptr)
            member.m_previous->m_next = member.m_next;
        else
            owner->jobctl_control.members = member.m_next;
        if (member.m_next != nullptr) member.m_next->m_previous = member.m_previous;
        member.m_previous = member.m_next = nullptr;
        member.m_subject = nullptr;
        member.m_visible = member.m_wake_pending = false;
        member.m_controlled = member.m_canceled_stop = member.m_legacy_continue = false;
        member.m_default_stop_pending = 0;
        static_cast<void>(owner->jobctl_barrier.detach(member.m_member));
        if (owner->jobctl_barrier.complete()) schedule_update_locked(*owner);
        member.m_owner = nullptr;
    }

    static void mark_wakes(task::Task& owner) {
        for (auto* member = owner.jobctl_control.members; member != nullptr; member = member->m_next) member->m_wake_pending = true;
    }

    static auto take_wake(task::Task& owner) -> task::Task* {
        for (auto* member = owner.jobctl_control.members; member != nullptr; member = member->m_next) {
            if (!member->m_visible || !member->m_wake_pending) continue;
            member->m_wake_pending = false;
            if (member->m_subject->try_acquire()) return member->m_subject;
        }
        return nullptr;
    }

    static auto has_wakes(task::Task& owner) -> bool {
        for (auto* member = owner.jobctl_control.members; member != nullptr; member = member->m_next) {
            if (member->m_visible && member->m_wake_pending) return true;
        }
        return false;
    }

    static void generate_members(task::Task& owner, task::Task& recipient, uint64_t mask) {
        for (auto* member = owner.jobctl_control.members; member != nullptr; member = member->m_next) {
            auto& subject = *member->m_subject;
            subject.signal_generate_pending_mask_local(mask, mask == STOP || &subject == &recipient);
            if (mask == STOP) {
                member->m_controlled = true;
                member->m_canceled_stop = false;
                member->m_legacy_continue = false;
            } else {
                member->m_canceled_stop = true;
                member->m_default_stop_pending = 0;
                if (!member->m_controlled && subject.jobctl_stopped.load(std::memory_order_acquire)) member->m_legacy_continue = true;
            }
            member->m_wake_pending = true;
        }
    }

    static void generate_default_stop(task::Task& owner, task::Task& recipient, uint64_t mask) {
        // Generation cancels opposing pending CONT but does not stop any task:
        // a blocked, caught or ignored signal has no default stop action yet.
        for (auto* member = owner.jobctl_control.members; member != nullptr; member = member->m_next) {
            auto& subject = *member->m_subject;
            subject.signal_generate_pending_mask_local(mask, &subject == &recipient);
            if (&subject == &recipient) member->m_default_stop_pending |= mask;
        }
    }

    static auto consume_default_stop(task::Task& subject, uint64_t mask) -> bool {
        auto& pending = subject.jobctl_membership.m_default_stop_pending;
        bool const PRESENT = (pending & mask) != 0;
        pending &= ~mask;
        return PRESENT;
    }

    static auto begin(task::Task& subject, task::Task& owner) -> StopDelivery {
        auto& member = subject.jobctl_membership;
        if (!bound(subject, &owner)) return StopDelivery::LEGACY;
        if (!owner.jobctl_control.enabled) return StopDelivery::LEGACY;
        if (member.m_canceled_stop || !owner.jobctl_barrier.stopping()) return StopDelivery::CANCELED;
        member.m_controlled = true;
        // Publish the local gate under the same lock as CONT generation and
        // release. Delivery must not reassert it after this lock is dropped.
        subject.jobctl_stop_signal = owner.jobctl_barrier.stop_signal();
        subject.jobctl_stop_publish_deferred.store(false, std::memory_order_release);
        subject.set_wait_channel("job_stop", task::WaitChannelKind::GENERIC);
        subject.set_voluntary_blocked(true);
        subject.jobctl_stopped.store(true, std::memory_order_release);
        return StopDelivery::MANAGED;
    }

    static auto resume_member(task::Task& subject, task::Task& owner) -> std::optional<bool> {
        auto& member = subject.jobctl_membership;
        if (!bound(subject, &owner) || !member.m_controlled) return std::nullopt;
        if (!subject.jobctl_stopped.load(std::memory_order_acquire)) {
            if (!owner.jobctl_barrier.stopping()) {
                static_cast<void>(owner.jobctl_barrier.release(member.m_member));
                member.m_controlled = false;
            }
            return true;
        }
        if ((subject.signal_pending_bits() & KILL) != 0) {
            member.m_controlled = false;
            clear_local_stop(subject);
            return true;
        }
        if (owner.jobctl_barrier.stopping()) {
            if (owner.jobctl_barrier.complete() || owner.jobctl_control.continue_pending) schedule_update_locked(owner);
            return false;
        }
        static_cast<void>(owner.jobctl_barrier.release(member.m_member));
        member.m_controlled = false;
        clear_local_stop(subject);
        return true;
    }

    static auto legacy_continue(task::Task& subject, task::Task& owner, bool consume) -> bool {
        auto& member = subject.jobctl_membership;
        if (!bound(subject, &owner)) return false;
        bool const PENDING = member.m_legacy_continue;
        if (consume) member.m_legacy_continue = false;
        return PENDING;
    }

    static void acknowledge(task::Task& subject, task::Task& owner) {
        auto& member = subject.jobctl_membership;
        if (!bound(subject, &owner) || !member.m_controlled || !subject.jobctl_stopped.load(std::memory_order_acquire) ||
            !owner.jobctl_barrier.stopping())
            return;
        static_cast<void>(owner.jobctl_barrier.acknowledge(member.m_member, {&owner.jobctl_barrier, owner.jobctl_barrier.generation()}));
        if (owner.jobctl_barrier.complete()) schedule_update_locked(owner);
    }
};

void schedule_update_locked(task::Task& owner) {
    if (!owner.scheduler_published.load(std::memory_order_acquire)) return;
    uint64_t const FLAGS = work_lock.lock_irqsave();
    if (!owner.jobctl_work_queued && owner.try_acquire_lifetime_ref()) {
        owner.jobctl_work_queued = true;
        owner.jobctl_work_next = nullptr;
        if (work_tail != nullptr)
            work_tail->jobctl_work_next = &owner;
        else
            work_head = &owner;
        work_tail = &owner;
    }
    work_lock.unlock_irqrestore(FLAGS);
}

void mark_wakes_locked(task::Task& owner) { Access::mark_wakes(owner); }

void wake_members(task::Task& owner) {
    for (unsigned i = 0; i < 32; ++i) {
        uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
        auto* subject = Access::take_wake(owner);
        owner.resource_lock.unlock_irqrestore(FLAGS);
        if (subject == nullptr) return;
        wake_task_for_signal(subject);
        subject->release();
    }
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    if (Access::has_wakes(owner)) schedule_update_locked(owner);
    owner.resource_lock.unlock_irqrestore(FLAGS);
}

void drain() {
    // Bound IRQ/return work. Owners requeued by a concurrent transition retain
    // their own reference and are safe to process in a subsequent pass.
    for (unsigned i = 0; i < 8; ++i) {
        uint64_t const FLAGS = work_lock.lock_irqsave();
        auto* owner = work_head;
        if (owner != nullptr) {
            work_head = owner->jobctl_work_next;
            if (work_head == nullptr) work_tail = nullptr;
            owner->jobctl_work_next = nullptr;
            owner->jobctl_work_queued = false;
        }
        work_lock.unlock_irqrestore(FLAGS);
        if (owner == nullptr) return;
        ker::syscall::process::child_events::advance_process_job_control(*owner);
        wake_members(*owner);
        owner->release();
    }
    uint64_t const FLAGS = work_lock.lock_irqsave();
    bool const REMAINING = work_head != nullptr;
    work_lock.unlock_irqrestore(FLAGS);
    if (REMAINING) request_local_timer_recheck();
}

auto generate(task::Task& subject, uint64_t mask) -> bool {
    bool const DEFAULT_STOP = (mask & DEFAULT_STOPS) != 0 && (mask & (mask - 1)) == 0;
    if (mask != STOP && mask != CONT && !DEFAULT_STOP) return false;
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return false;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    if (!Access::bound(subject, owner)) {
        owner->resource_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    if (DEFAULT_STOP) {
        Access::generate_default_stop(*owner, subject, mask);
        owner->resource_lock.unlock_irqrestore(FLAGS);
        return true;
    }
    if (mask == STOP && !owner->jobctl_barrier.request_stop(19)) {
        owner->resource_lock.unlock_irqrestore(FLAGS);
        mod::dbg::panic_handler("process job-control generation exhausted");
        hcf();
    }
    owner->jobctl_control.enabled = true;
    owner->jobctl_control.continue_pending = mask == CONT && owner->jobctl_barrier.stopping();
    Access::generate_members(*owner, subject, mask);
    if (owner->jobctl_barrier.stopping()) schedule_update_locked(*owner);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    wake_members(*owner);
    return true;
}

auto begin_stop(task::Task& subject, int signal, SignalPendingClaim* claim) -> StopDelivery {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) {
        return claim != nullptr ? StopDelivery::CANCELED : StopDelivery::LEGACY;
    }
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    auto* pending_subject = &subject;
    if (claim != nullptr) {
        uint64_t const MASK = signal >= 1 && signal <= 64 ? uint64_t{1} << (signal - 1) : 0;
        if (!subject.signal_claim_matches(*claim, MASK)) {
            if (subject.retired_signal_owner() != owner || !owner->signal_claim_matches(*claim, MASK)) {
                owner->resource_lock.unlock_irqrestore(FLAGS);
                return StopDelivery::CANCELED;
            }
            pending_subject = owner;
        }
        // CONT cancellation and stop generation hold this same lock. Consume
        // the occurrence and its actual source's marker in one transaction.
        if (!claim->commit()) {
            owner->resource_lock.unlock_irqrestore(FLAGS);
            return StopDelivery::CANCELED;
        }
    }
    if (!Access::bound(subject, owner)) {
        owner->resource_lock.unlock_irqrestore(FLAGS);
        return StopDelivery::LEGACY;
    }
    bool const DEFAULT_STOP = signal >= 20 && signal <= 22;
    if (DEFAULT_STOP) {
        if (!Access::consume_default_stop(*pending_subject, uint64_t{1} << (signal - 1))) {
            owner->resource_lock.unlock_irqrestore(FLAGS);
            return StopDelivery::CANCELED;
        }
        if (!owner->jobctl_barrier.request_stop(static_cast<uint32_t>(signal))) {
            owner->resource_lock.unlock_irqrestore(FLAGS);
            mod::dbg::panic_handler("process job-control generation exhausted");
            hcf();
        }
        owner->jobctl_control.enabled = true;
        owner->jobctl_control.continue_pending = false;
        Access::generate_members(*owner, subject, STOP);
        schedule_update_locked(*owner);
    }
    auto const RESULT = Access::begin(subject, *owner);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    if (DEFAULT_STOP) wake_members(*owner);
    return RESULT;
}

auto resume(task::Task& subject) -> std::optional<bool> {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return std::nullopt;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    auto const RESULT = Access::resume_member(subject, *owner);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto legacy_continue_requested(task::Task& subject) -> bool {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return false;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    bool const RESULT = Access::legacy_continue(subject, *owner, false);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

void complete_legacy_continue(task::Task& subject) {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    static_cast<void>(Access::legacy_continue(subject, *owner, true));
    owner->resource_lock.unlock_irqrestore(FLAGS);
}

void acknowledge_stopped(task::Task& subject) {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    Access::acknowledge(subject, *owner);
    owner->resource_lock.unlock_irqrestore(FLAGS);
}

Membership::~Membership() { detach(); }

auto Membership::bind(task::Task& subject) -> bool {
    if (m_owner != nullptr || &subject.jobctl_membership != this || subject.type != task::TaskType::PROCESS) return false;
    auto* owner = subject.credential_owner();
    if (owner == nullptr || owner->type != task::TaskType::PROCESS || owner->is_thread || task::process_pid(subject) != owner->pid)
        return false;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    bool const ATTACHED = owner->jobctl_barrier.attach(m_member);
    if (ATTACHED) {
        m_owner = owner;
        m_subject = &subject;
        m_previous_job_pending = subject.signal_pending_bits();
        m_default_stop_pending = m_previous_job_pending & DEFAULT_STOPS;
        m_next = owner->jobctl_control.members;
        if (m_next != nullptr) m_next->m_previous = this;
        owner->jobctl_control.members = this;
        if (!owner->jobctl_barrier.release(m_member)) {
            m_previous_stop_signal = subject.jobctl_stop_signal;
            m_previous_stopped = subject.jobctl_stopped.load(std::memory_order_acquire);
            m_admission_changed = true;
            m_controlled = true;
            m_wake_pending = true;
            subject.jobctl_stop_signal = owner->jobctl_barrier.stop_signal();
            subject.jobctl_stopped.store(true, std::memory_order_release);
        }
    }
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return ATTACHED;
}

void Membership::finish_publication(task::Task& subject) {
    auto* owner = subject.credential_owner();
    if (owner == nullptr) return;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    if (m_owner == owner && &subject.jobctl_membership == this) {
        m_visible = true;
        if (owner->jobctl_barrier.complete()) schedule_update_locked(*owner);
    }
    owner->resource_lock.unlock_irqrestore(FLAGS);
    wake_members(*owner);
}

void Membership::rollback_publication(task::Task& subject) {
    auto* owner = m_owner;
    if (owner == nullptr || &subject.jobctl_membership != this) return;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    subject.signal_restore_job_control_pending(m_previous_job_pending);
    if (m_admission_changed) {
        subject.jobctl_stop_signal = m_previous_stop_signal;
        subject.jobctl_stopped.store(m_previous_stopped, std::memory_order_release);
    }
    Access::unlink(*this);
    m_admission_changed = false;
    owner->resource_lock.unlock_irqrestore(FLAGS);
}

void Membership::detach() {
    auto* owner = m_owner;
    if (owner == nullptr) return;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    Access::unlink(*this);
    m_admission_changed = false;
    owner->resource_lock.unlock_irqrestore(FLAGS);
}

}  // namespace ker::mod::sched::job_control
