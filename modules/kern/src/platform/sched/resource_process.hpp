#pragma once

#include <abi/resource.hpp>
#include <cstdint>
#include <platform/security/credentials.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
struct State;
struct CredentialAccess;
// Internal intrusive population entry. Mutated only under the population lock.
// A Task member releases unpublished construction failures independently of GC.
struct ProcessCharge {
    ProcessCharge() = default;
    ~ProcessCharge();
    ProcessCharge(const ProcessCharge&) = delete;
    auto operator=(const ProcessCharge&) -> ProcessCharge& = delete;
    void release();

    task::Task* subject = nullptr;
    ProcessCharge* previous = nullptr;
    ProcessCharge* next = nullptr;
    uint32_t uid = 0;
    bool linked = false;
};

class ProcessReservation {
   public:
    // A null policy is reserved for trusted boot/receiver seeding. It still
    // counts against the node's UID population; user creation supplies policy.
    ProcessReservation(task::Task* policy, uint32_t uid);
    // Receiver-only admission from a private, validated snapshot before a
    // Task or kernel stack exists. Records denial directly in private_policy.
    ProcessReservation(State& private_policy, uint32_t uid);
    ~ProcessReservation();
    ProcessReservation(const ProcessReservation&) = delete;
    auto operator=(const ProcessReservation&) -> ProcessReservation& = delete;
    auto result() const -> int { return m_result; }
    [[nodiscard]] auto attach(task::Task& subject) -> bool;
    void cancel();

   private:
    void reserve(task::Task* policy, State* private_policy, uint32_t uid, task::Task* executor_fallback);
    void unlink_executor_locked();
    ProcessCharge m_charge;
    task::Task* m_executor = nullptr;
    ProcessReservation* m_previous = nullptr;
    ProcessReservation* m_next = nullptr;
    int m_result = 0;
};

void cancel_process_reservations(task::Task& executor);
// Caller serializes live changes with SharedVmemPublicationGuard. Moves all
// process/thread population charges and publishes the full snapshot atomically.
// Published callers hold SharedVmemPublicationGuard through this operation.
// Replaced-image workers queue retirement and return internal ECANCELED without
// changing process state; their safe user-return path consumes the exit request.
auto change_credentials(task::Task& subject, const security::Request& request) -> int;
// Only for exclusively owned, unpublished non-thread tasks. Also validates
// any already-attached UID charge before replacing the complete snapshot.
auto initialize_credentials(task::Task& subject, const security::Snapshot& value) -> int;
auto initialize_real_uid(task::Task& subject, uint32_t uid) -> int;
// Under the VM publication guard: prepare before image commit, then publish
// after successful symbol/image staging. Exec never changes the real UID.
auto prepare_exec_credentials(task::Task& subject, const security::ExecIdentity& image, security::Snapshot& before,
                              security::Snapshot& after, const security::Snapshot* admitted = nullptr) -> int;
auto publish_exec_credentials(task::Task& subject, const security::Snapshot& before, const security::Snapshot& after) -> int;
void copy_process_observation(task::Task& subject, abi::resource::Usage& usage);
}  // namespace ker::mod::sched::resource
