#pragma once

#include <platform/sched/preemption_diagnostics.hpp>

namespace ker::mod::sched {

// Scalar facts keep cross-CPU placement policy independently testable. Kernel
// callers derive these while holding (or before acquiring) the runqueue lock
// that proves the task's source owner.
struct CrossCpuMigrationEligibilityInput {
    task::SavedFrameClass frame_class{task::SavedFrameClass::INVALID};
    bool source_owner_valid{};
    bool active{};
    bool resources_valid{};
    bool frame_valid{};
    bool migration_disabled{};
    bool preempt_disabled{};
    bool return_transition{};
    bool cpu_pin_allows_target{true};
    bool domain_allows_target{true};
    bool wki_owned{};
};

struct CrossCpuMigrationDecision {
    bool can_migrate{};
    MigrationRejectionReason reason{MigrationRejectionReason::INVALID_FRAME};
};

[[nodiscard]] constexpr auto evaluate_cross_cpu_migration(const CrossCpuMigrationEligibilityInput& input) -> CrossCpuMigrationDecision {
    if (!input.source_owner_valid) {
        return {.reason = MigrationRejectionReason::INVALID_OWNER};
    }
    if (!input.active || !input.resources_valid) {
        return {.reason = MigrationRejectionReason::NOT_RUNNABLE};
    }
    if (input.migration_disabled) {
        return {.reason = MigrationRejectionReason::MIGRATION_DISABLED};
    }
    if (input.preempt_disabled) {
        return {.reason = MigrationRejectionReason::PREEMPT_DISABLED};
    }
    if (input.return_transition) {
        return {.reason = MigrationRejectionReason::RETURN_TRANSITION};
    }
    if (!input.cpu_pin_allows_target) {
        return {.reason = MigrationRejectionReason::CPU_PINNED};
    }
    if (!input.domain_allows_target) {
        return {.reason = MigrationRejectionReason::DOMAIN_RESTRICTED};
    }
    if (input.wki_owned) {
        return {.reason = MigrationRejectionReason::WKI_OWNED};
    }

    switch (input.frame_class) {
        case task::SavedFrameClass::USER_RETURN:
        case task::SavedFrameClass::VOLUNTARY_PARKED_KERNEL:
        case task::SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL:
        case task::SavedFrameClass::DAEMON_KERNEL:
            break;
        case task::SavedFrameClass::INVALID:
            return {.reason = MigrationRejectionReason::INVALID_FRAME};
    }
    if (!input.frame_valid) {
        return {.reason = MigrationRejectionReason::INVALID_FRAME};
    }
    return {
        .can_migrate = true,
        .reason = MigrationRejectionReason::NONE,
    };
}

}  // namespace ker::mod::sched
