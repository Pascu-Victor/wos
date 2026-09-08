#pragma once

#include <abi/resource.hpp>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
struct State;
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
// On success changes the real UID as part of the charge transfer. Effective
// and saved IDs remain the credential syscall's responsibility.
auto change_real_uid(task::Task& subject, uint32_t uid) -> int;
void copy_process_observation(task::Task& subject, abi::resource::Usage& usage);
}  // namespace ker::mod::sched::resource
