#pragma once

#ifdef WOS_SELFTEST
#include <array>
#include <cstdint>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <test/compartment_lifecycle_runtime.hpp>
#include <test/lifecycle_owner_census.hpp>

namespace ker::test::lifecycle60 {
inline constexpr size_t CENSUS_CPUS = 8;

enum class CensusReason : uint8_t {
    NONE,
    INVALID_PERMISSION,
    WRONG_BOUNDARY,
    WRONG_ACTOR,
    COUNTER_INVALID,
    OWNER_CHANGED,
    SCALAR_CHANGED,
    TASK_DEBT,
    PRIVATE_DEBT,
    DRIVER_CHILD_DEBT,
    DRIVER_FD_DEBT,
    PROVIDER_DEBT,
    EXECUTOR_UNAVAILABLE,
    EXECUTOR_ERROR,
    HOST_BASELINE_MISSING,
    DRIVER_BASELINE_MISSING,
    BASELINE_MISMATCH,
    SUMMARY_MISMATCH,
    REPORT_EXHAUSTED
};

struct CensusCpuScalars {
    uint64_t runnable_heap{}, dead{}, waiting{}, timers{};
    uint64_t sampled_dead{}, first_dead_pid{}, first_dead_references{};
    auto operator==(const CensusCpuScalars&) const -> bool = default;
};

// All values, never object owners. PID/FD identities are comparisons only.
struct CensusScalars {
    uint64_t actor_pid{}, actor_references{}, actor_thread_owners{}, actor_cpu_threads{};
    bool actor_shares_root{};
    uint64_t core_count{}, active_registry{}, scoped_pid_registry{}, active_threads{};
    uint64_t pid_namespaces{}, mount_namespaces{}, ipc_namespaces{};
    uint64_t compartment_pending{}, compartment_completed{}, compartment_exit_requests{};
    bool native_retirements_pending{};
    uint64_t deferred_queued{}, deferred_completed{}, deferred_depth{};
    std::array<CensusCpuScalars, CENSUS_CPUS> cpu{};
    mod::mm::file_mapping::LifetimeRetirementSnapshot executor{};
    // Relaxed physical/cache telemetry: deliberately excluded from equality.
    mod::mm::phys::AllocStatsSnapshot physical{};
};

struct CensusGateReport {
    uint64_t calls{}, accepts{}, waits{}, refuses{};
    Boundary boundary{Boundary::BEFORE_DRIVER};
    GateResult result{GateResult::WAIT};
    CensusReason reason{CensusReason::NONE};
    uint64_t index{}, cookie{};
    bool owner_pair_stable{}, scalar_pair_stable{};
    bool host_baseline_valid{}, provisional_driver_valid{}, driver_baseline_valid{};
    CensusScalars before{}, after{};
    // Baseline scalar copies explain actor/registry/thread debts without
    // retaining another observer reference. Owner baselines remain private.
    CensusScalars host_baseline{}, driver_baseline{};
};

// Same permanent controller only, after its lease resets and no gate executes.
// No cross-CPU publication/synchronization is implied. Borrowed immutable value
// views; the FD identities in OwnerCensus must NEVER be dereferenced.
[[nodiscard]] auto census_gate_report() noexcept -> const CensusGateReport&;
[[nodiscard]] auto census_owner_report() noexcept -> const OwnerCensus&;
}  // namespace ker::test::lifecycle60
#endif
