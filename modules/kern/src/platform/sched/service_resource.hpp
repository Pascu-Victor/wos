#pragma once

#include <abi/service_resource.hpp>
#include <atomic>
#include <platform/sched/service_resource_model.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
class RemoteServiceAccounting;
class ServiceReportTarget;
struct ServiceMember {
    ServiceMember() = default;
    ~ServiceMember();
    ServiceMember(const ServiceMember&) = delete;
    auto operator=(const ServiceMember&) -> ServiceMember& = delete;
    task::Task* subject = nullptr;
    ServiceMember* previous = nullptr;
    ServiceMember* next = nullptr;
    uint64_t slot = abi::service_resource::NO_SELECTION;
    RemoteServiceAccounting* remote = nullptr;
    std::array<uint64_t, abi::service_resource::RESOURCE_COUNT> denied_baseline{};
    bool executing_remotely = false;  // service lock; physical charges unchanged
};

// Serializes a process resource merge with all service observations. The caller
// must hold the Task lifetime and release this guard before lifecycle/network
// operations. Exclude only after a validated merge succeeds.
class ServiceResourceMerge {
   public:
    explicit ServiceResourceMerge(task::Task& process);
    ~ServiceResourceMerge();
    ServiceResourceMerge(const ServiceResourceMerge&) = delete;
    auto operator=(const ServiceResourceMerge&) -> ServiceResourceMerge& = delete;
    void exclude_reported_denials();

   private:
    task::Task& process;
    uint64_t service_flags;
    uint64_t resource_flags;
    std::array<uint64_t, abi::service_resource::RESOURCE_COUNT> before_denied{};
};

void mark_service_remote_execution(task::Task& process);
// Read-only admission, before exporting descriptors or publishing a stream.
[[nodiscard]] auto service_remote_admission(task::Task& process) -> int;

// Execution-site observations outlive the submitted root process. The caller
// owns create()'s initial reference; every attached process holds another.
// This is accounting only, not a distributed finite CPU budget enforcement API.
class RemoteServiceAccounting {
   public:
    [[nodiscard]] static auto create() -> RemoteServiceAccounting*;
    void retain();
    void release();
    [[nodiscard]] auto attach(task::Task& process, const State* imported_history = nullptr) -> bool;
    [[nodiscard]] auto snapshot() -> RemoteServiceObservation;
    RemoteServiceAccounting(const RemoteServiceAccounting&) = delete;
    auto operator=(const RemoteServiceAccounting&) -> RemoteServiceAccounting& = delete;

   private:
    RemoteServiceAccounting() = default;
    ~RemoteServiceAccounting() = default;
    auto attach_locked(task::Task& process, const State* imported_history = nullptr) -> bool;
    friend struct ServiceMember;
    friend class ServiceReportTarget;
    friend void inherit_service(task::Task& child, task::Task& source);
    friend auto charge_service_cpu(task::Task& process, uint64_t elapsed_us) -> CpuAction;
    std::atomic<uint64_t> references{1};
    State totals{};
    ServiceMember* members = nullptr;
    ServiceReportTarget* reports = nullptr;
    bool incomplete = false;
};

// Movable, allocation-free destination owned by a submission's report stream.
// The stream retains a service destination, never the submitted root Task.
class ServiceReportTarget {
   public:
    ServiceReportTarget() = default;
    ~ServiceReportTarget();
    ServiceReportTarget(const ServiceReportTarget&) = delete;
    auto operator=(const ServiceReportTarget&) -> ServiceReportTarget& = delete;
    ServiceReportTarget(ServiceReportTarget&& other) noexcept;
    auto operator=(ServiceReportTarget&& other) noexcept -> ServiceReportTarget&;
    // 1 binds an affiliated process, 0 means no affiliation, negative errno
    // refuses unsupported policy or reuse. Call before remote side effects.
    [[nodiscard]] auto bind(task::Task& process) -> int;
    [[nodiscard]] auto apply(uint64_t sequence, const RemoteServiceObservation& observation, bool final) -> ServiceReportResult;
    // Pass false only when no stream executed (e.g. confirmed admission reject).
    // Once a report was applied, only a final report proves completeness.
    void cancel(bool incomplete = true);

   private:
    friend class RemoteServiceAccounting;
    friend auto syscall_service_read(uint64_t slot, uint64_t address, uint64_t version) -> uint64_t;
    auto list_head_locked() -> ServiceReportTarget*&;
    void move_from_locked(ServiceReportTarget& other);
    void add_current_locked(RemoteServiceObservation& observation) const;
    static void observe_reports_locked(ServiceReportTarget* reports, RemoteServiceObservation& observation);
    ServiceReportTarget* previous = nullptr;
    ServiceReportTarget* next = nullptr;
    RemoteServiceAccounting* remote = nullptr;
    uint64_t slot = abi::service_resource::NO_SELECTION;
    ServiceReportState state{};
};

// Source is the retained process policy owner. Membership is immutable after
// construction, and the fixed service slot outlives every affiliated task.
void inherit_service(task::Task& child, task::Task& source);
auto charge_service_cpu(task::Task& process, uint64_t elapsed_us) -> CpuAction;
auto syscall_service_configure(uint64_t address) -> uint64_t;
auto syscall_service_select(uint64_t slot) -> uint64_t;
auto syscall_service_read(uint64_t slot, uint64_t address, uint64_t version) -> uint64_t;
#ifdef WOS_SELFTEST
auto selftest_service_management_authority() -> bool;
auto selftest_service_lifecycle() -> bool;
auto selftest_remote_service_accounting_lifecycle() -> bool;
auto selftest_service_report_targets() -> bool;
auto selftest_service_observation_merge() -> bool;
#endif
}  // namespace ker::mod::sched::resource
