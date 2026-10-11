#include "resource_process.hpp"

#include <cerrno>
#include <platform/sched/native_creation_policy.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/process/exit.hpp>

namespace ker::mod::sched::resource {
struct CredentialAccess {
    static auto store(task::Task& subject) -> security::CredentialStore& { return subject.m_credentials; }
};

auto capture_native_creator_fields(task::Task& source, const security::compartment::Request& request, const State* service_limits,
                                   NativeCreatorPolicy& out) -> int {
    if (source.is_thread || source.credential_owner() != &source || source.type != task::TaskType::PROCESS) return -EINVAL;
    if (source.exit_in_progress.load(std::memory_order_acquire) || source.has_exited ||
        source.state.load(std::memory_order_acquire) != task::TaskState::ACTIVE)
        return -ECANCELED;
    auto const* pids = source.pid_namespace();
    if (pids == nullptr) return -ESRCH;
    if (pids->closed()) return -EIDRM;
    NativeCreatorPolicy candidate{};
    int result = 0;
    {
        // UID publication uses credentials before resource while holding the
        // population lock. Never reserve population or acquire service here.
        security::CredentialStore::Transaction transaction(CredentialAccess::store(source));
        uint64_t const FLAGS = source.resource_lock.lock_irqsave();
        candidate.observed = transaction.observation();
        candidate.parent.credentials = candidate.observed.credentials;
        candidate.parent.network = pids->network();
        candidate.creation_mask = source.creation_mask();
        for (size_t i = 0; i < candidate.parent.limits.size(); ++i) {
            auto limit = source.resources.entries[i].limit;
            if (service_limits != nullptr) {
                auto const SERVICE = service_limits->entries[i].limit;
                if (SERVICE.soft < limit.soft) limit.soft = SERVICE.soft;
                if (SERVICE.hard < limit.hard) limit.hard = SERVICE.hard;
            }
            candidate.parent.limits[i] = limit;
        }
        result = security::compartment::prepare(candidate.parent, request, task::Task::FD_TABLE_SIZE, candidate.admitted);
        source.resource_lock.unlock_irqrestore(FLAGS);
    }
    if (result == 0) out = candidate;
    return result;
}

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
// population_lock is held; the caller also holds the process credential lock.
// Every linked thread counts toward NPROC and must move with its process.
auto transfer_uid_locked(task::Task& process, uint32_t old_uid, uint32_t new_uid, bool unpublished) -> int {
    if (old_uid == new_uid) return 0;
    uint64_t moving = 0;
    for (auto* node = population; node != nullptr; node = node->next) {
        if (node->subject != nullptr && node->subject->credential_owner() == &process) {
            if (node->uid != old_uid) return -EINVAL;
            ++moving;
        }
    }
    if (moving == 0) return unpublished ? 0 : -ESRCH;
    uint64_t const DESTINATION = count_locked(new_uid);
    uint64_t const FLAGS = process.resource_lock.lock_irqsave();
    bool const ADMITTED = moving <= UINT64_MAX - DESTINATION && process.resources.admit_extent(NPROC, DESTINATION + moving);
    process.resource_lock.unlock_irqrestore(FLAGS);
    if (!ADMITTED) return -EAGAIN;
    for (auto* node = population; node != nullptr; node = node->next) {
        if (node->subject != nullptr && node->subject->credential_owner() == &process) node->uid = new_uid;
    }
    return 0;
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
    bool const VALID = m_charge.linked && !charge.linked && subject.credentials().user().real == m_charge.uid;
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

auto change_credentials(task::Task& subject, const security::Request& request) -> int {
    auto* process = subject.credential_owner();
    if (process == nullptr) return -ESRCH;
    if (ker::syscall::process::request_replaced_exec_thread_retirement(subject)) return -ECANCELED;
    uint64_t const FLAGS = population_lock.lock_irqsave();
    int result = 0;
    {
        security::CredentialStore::Transaction transaction(CredentialAccess::store(*process));
        security::Snapshot next;
        result = transaction.current().prepare(request, next);
        uint32_t const OLD_UID = transaction.current().user().real;
        uint32_t const NEW_UID = next.user().real;
        if (result == 0 && !transaction.can_publish(next)) result = -EOVERFLOW;
        if (result == 0) result = transfer_uid_locked(*process, OLD_UID, NEW_UID, false);
        if (result == 0) {
            result = transaction.publish(next);
            if (OLD_UID != NEW_UID) {
                observe_locked(OLD_UID, count_locked(OLD_UID));
                observe_locked(NEW_UID, count_locked(NEW_UID));
            }
        }
    }
    population_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto initialize_credentials(task::Task& subject, const security::Snapshot& value) -> int {
    // An exclusively owned construction object has no concurrent publisher.
    // Never use this entrypoint to mutate a running task or its shared owner.
    if (subject.is_thread || subject.has_run || subject.sched_queue != task::Task::sched_queue::NONE ||
        subject.child_membership_state.load(std::memory_order_acquire) != task::ChildMembershipState::UNLINKED)
        return -EBUSY;
    uint64_t const FLAGS = population_lock.lock_irqsave();
    int result = 0;
    {
        security::CredentialStore::Transaction transaction(CredentialAccess::store(subject));
        uint32_t const OLD_UID = transaction.current().user().real;
        uint32_t const NEW_UID = value.user().real;
        if (!transaction.can_publish(value)) result = -EOVERFLOW;
        if (result == 0) result = transfer_uid_locked(subject, OLD_UID, NEW_UID, true);
        if (result == 0) {
            result = transaction.publish(value);
            if (OLD_UID != NEW_UID) {
                observe_locked(OLD_UID, count_locked(OLD_UID));
                observe_locked(NEW_UID, count_locked(NEW_UID));
            }
        }
    }
    population_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto initialize_real_uid(task::Task& subject, uint32_t uid) -> int {
    if (uid == security::UNCHANGED_ID) return -EINVAL;
    auto value = security::Snapshot::bootstrap_root();
    int const RESULT = value.prepare({.change = security::Change::SET_RESUID, .real = uid}, value);
    return RESULT == 0 ? initialize_credentials(subject, value) : RESULT;
}

auto prepare_exec_credentials(task::Task& subject, const security::ExecIdentity& image, security::Snapshot& before,
                              security::Snapshot& after, const security::Snapshot* admitted) -> int {
    auto* process = subject.credential_owner();
    if (process == nullptr) return -ESRCH;
    security::CredentialStore::Transaction transaction(CredentialAccess::store(*process));
    before = transaction.current();
    if (admitted != nullptr && before != *admitted) return -EAGAIN;
    int const RESULT = before.prepare_exec(image, after);
    return RESULT != 0 ? RESULT : (transaction.can_publish(after) ? 0 : -EOVERFLOW);
}

auto publish_exec_credentials(task::Task& subject, const security::Snapshot& before, const security::Snapshot& after) -> int {
    auto* process = subject.credential_owner();
    if (process == nullptr) return -ESRCH;
    security::CredentialStore::Transaction transaction(CredentialAccess::store(*process));
    if (transaction.current() != before) return -EAGAIN;
    if (before.user().real != after.user().real) return -EINVAL;
    return transaction.publish(after);
}

void copy_process_observation(task::Task& subject, abi::resource::Usage& usage) {
    uint64_t const FLAGS = population_lock.lock_irqsave();
    uint32_t const UID = subject.credentials().user().real;
    uint64_t const RESOURCE_FLAGS = subject.resource_lock.lock_irqsave();
    subject.resources.commit_extent(NPROC, count_locked(UID));
    usage.current = subject.resources.entries[NPROC].current;
    usage.peak = subject.resources.entries[NPROC].peak;
    subject.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    population_lock.unlock_irqrestore(FLAGS);
}
}  // namespace ker::mod::sched::resource
