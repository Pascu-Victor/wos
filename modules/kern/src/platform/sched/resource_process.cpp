#include "resource_process.hpp"

#include <cerrno>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sched::resource {
namespace {
constexpr unsigned NPROC = static_cast<unsigned>(Id::NPROC);
sys::Spinlock population_lock;
ProcessCharge* population = nullptr;

auto count_locked(uint32_t uid) -> uint64_t {
    uint64_t count = 0;
    for (auto* node = population; node != nullptr; node = node->next) {
        if (node->uid == uid) ++count;
    }
    return count;
}

void observe_locked(uint32_t uid, uint64_t count) {
    for (auto* node = population; node != nullptr; node = node->next) {
        if (node->uid != uid || node->subject == nullptr) continue;
        auto& subject = *node->subject;
        uint64_t const FLAGS = subject.resource_lock.lock_irqsave();
        subject.resources.commit_extent(NPROC, count);
        subject.resource_lock.unlock_irqrestore(FLAGS);
    }
}

void unlink_locked(ProcessCharge& charge) {
    if (charge.previous != nullptr)
        charge.previous->next = charge.next;
    else
        population = charge.next;
    if (charge.next != nullptr) charge.next->previous = charge.previous;
    charge.previous = nullptr;
    charge.next = nullptr;
    charge.linked = false;
    charge.subject = nullptr;
}

auto admit_locked(task::Task* policy, uint64_t current) -> bool {
    if (policy == nullptr) return true;
    uint64_t const FLAGS = policy->resource_lock.lock_irqsave();
    bool const ADMITTED = policy->resources.admit_process(current);
    policy->resource_lock.unlock_irqrestore(FLAGS);
    return ADMITTED;
}
}  // namespace

ProcessCharge::~ProcessCharge() { release(); }

void ProcessCharge::release() {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    if (linked) {
        unlink_locked(*this);
        observe_locked(uid, count_locked(uid));
    }
    population_lock.unlock_irqrestore(FLAGS);
}

ProcessReservation::ProcessReservation(task::Task* policy, uint32_t uid) {
    Owner owner(policy);
    if (policy != nullptr && owner.get() == nullptr) {
        m_result = -ESRCH;
        return;
    }
    reserve(owner.get(), nullptr, uid, policy);
}

ProcessReservation::ProcessReservation(State& private_policy, uint32_t uid) { reserve(nullptr, &private_policy, uid, nullptr); }

void ProcessReservation::reserve(task::Task* policy, State* private_policy, uint32_t uid, task::Task* executor_fallback) {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    uint64_t const CURRENT = count_locked(uid);
    bool const ADMITTED = private_policy != nullptr ? private_policy->admit_process(CURRENT) : admit_locked(policy, CURRENT);
    if (!ADMITTED) {
        m_result = -EAGAIN;
    } else {
        m_charge.uid = uid;
        m_charge.next = population;
        if (population != nullptr) population->previous = &m_charge;
        population = &m_charge;
        m_charge.linked = true;
        m_executor = can_query_current_task() ? get_current_task() : executor_fallback;
        if (m_executor != nullptr) {
            m_next = m_executor->pending_process_reservations;
            if (m_next != nullptr) m_next->m_previous = this;
            m_executor->pending_process_reservations = this;
        }
        observe_locked(uid, CURRENT + 1);
    }
    population_lock.unlock_irqrestore(FLAGS);
}

ProcessReservation::~ProcessReservation() { cancel(); }

void ProcessReservation::unlink_executor_locked() {
    if (m_executor == nullptr) return;
    if (m_previous != nullptr)
        m_previous->m_next = m_next;
    else
        m_executor->pending_process_reservations = m_next;
    if (m_next != nullptr) m_next->m_previous = m_previous;
    m_executor = nullptr;
    m_previous = nullptr;
    m_next = nullptr;
}

void ProcessReservation::cancel() {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    if (m_charge.linked) {
        unlink_locked(m_charge);
        unlink_executor_locked();
        m_result = -ECANCELED;
        observe_locked(m_charge.uid, count_locked(m_charge.uid));
    }
    population_lock.unlock_irqrestore(FLAGS);
}

auto ProcessReservation::attach(task::Task& subject) -> bool {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    auto& charge = subject.process_charge;
    bool const VALID = m_charge.linked && !charge.linked && subject.uid == m_charge.uid;
    if (VALID) {
        charge.uid = m_charge.uid;
        charge.subject = &subject;
        charge.previous = m_charge.previous;
        charge.next = m_charge.next;
        charge.linked = true;
        if (charge.previous != nullptr)
            charge.previous->next = &charge;
        else
            population = &charge;
        if (charge.next != nullptr) charge.next->previous = &charge;
        m_charge.linked = false;
        m_charge.previous = nullptr;
        m_charge.next = nullptr;
        unlink_executor_locked();
        observe_locked(charge.uid, count_locked(charge.uid));
    }
    population_lock.unlock_irqrestore(FLAGS);
    return VALID;
}

void cancel_process_reservations(task::Task& executor) {
    while (executor.pending_process_reservations != nullptr) executor.pending_process_reservations->cancel();
}

auto change_real_uid(task::Task& subject, uint32_t uid) -> int {
    Owner owner(&subject);
    if (owner.get() == nullptr) return -ESRCH;
    uint64_t const FLAGS = population_lock.lock_irqsave();
    auto& charge = subject.process_charge;
    int result = 0;
    if (!charge.linked) {
        result = -ESRCH;
    } else if (charge.uid != uid) {
        uint64_t const DESTINATION = count_locked(uid);
        if (!admit_locked(owner.get(), DESTINATION)) {
            result = -EAGAIN;
        } else {
            uint32_t const OLD_UID = charge.uid;
            charge.uid = uid;
            subject.uid = uid;
            observe_locked(OLD_UID, count_locked(OLD_UID));
            observe_locked(uid, DESTINATION + 1);
        }
    }
    population_lock.unlock_irqrestore(FLAGS);
    return result;
}

void copy_process_observation(task::Task& subject, abi::resource::Usage& usage) {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    uint64_t const RESOURCE_FLAGS = subject.resource_lock.lock_irqsave();
    subject.resources.commit_extent(NPROC, count_locked(subject.uid));
    usage.current = subject.resources.entries[NPROC].current;
    usage.peak = subject.resources.entries[NPROC].peak;
    subject.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    population_lock.unlock_irqrestore(FLAGS);
}
}  // namespace ker::mod::sched::resource
