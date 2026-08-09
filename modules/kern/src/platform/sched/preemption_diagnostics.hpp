#pragma once

#include <cstdint>
#include <platform/sched/preemption_policy.hpp>

namespace ker::mod::sched {

constexpr uint16_t SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN = UINT16_MAX;

enum class MigrationRejectionReason : uint8_t {
    NONE,
    MIGRATION_DISABLED,
    INVALID_FRAME,
    PREEMPT_DISABLED,
    RETURN_TRANSITION,
    CPU_PINNED,
    DOMAIN_RESTRICTED,
    WKI_OWNED,
    NOT_RUNNABLE,
    INVALID_OWNER,
};

[[nodiscard]] constexpr auto migration_rejection_reason_name(MigrationRejectionReason reason) -> const char* {
    switch (reason) {
        case MigrationRejectionReason::NONE:
            return "none";
        case MigrationRejectionReason::MIGRATION_DISABLED:
            return "migration-disabled";
        case MigrationRejectionReason::INVALID_FRAME:
            return "invalid-frame";
        case MigrationRejectionReason::PREEMPT_DISABLED:
            return "preempt-disabled";
        case MigrationRejectionReason::RETURN_TRANSITION:
            return "return-transition";
        case MigrationRejectionReason::CPU_PINNED:
            return "cpu-pinned";
        case MigrationRejectionReason::DOMAIN_RESTRICTED:
            return "domain-restricted";
        case MigrationRejectionReason::WKI_OWNED:
            return "wki-owned";
        case MigrationRejectionReason::NOT_RUNNABLE:
            return "not-runnable";
        case MigrationRejectionReason::INVALID_OWNER:
            return "invalid-owner";
    }
    return "invalid-frame";
}

struct PreemptionDiagnostic {
    task::SavedFrameClass frame_class{task::SavedFrameClass::INVALID};
    KernelPreemptionBlockReason reason{KernelPreemptionBlockReason::INVALID_FRAME};
    uint16_t source_cpu{SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN};
    uint16_t target_cpu{SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN};
};

struct MigrationDiagnostic {
    task::SavedFrameClass frame_class{task::SavedFrameClass::INVALID};
    MigrationRejectionReason reason{MigrationRejectionReason::NONE};
    uint16_t source_cpu{SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN};
    uint16_t target_cpu{SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN};
};

[[nodiscard]] constexpr auto scheduler_diagnostic_cpu(uint64_t cpu) -> uint16_t {
    return cpu < SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN ? static_cast<uint16_t>(cpu) : SCHEDULER_DIAGNOSTIC_CPU_UNKNOWN;
}

[[nodiscard]] constexpr auto encode_preemption_diagnostic(PreemptionDiagnostic diagnostic) -> uint64_t {
    return static_cast<uint64_t>(diagnostic.frame_class) | (static_cast<uint64_t>(diagnostic.reason) << 8U) |
           (static_cast<uint64_t>(diagnostic.source_cpu) << 16U) | (static_cast<uint64_t>(diagnostic.target_cpu) << 32U);
}

[[nodiscard]] constexpr auto decode_preemption_diagnostic(uint64_t encoded) -> PreemptionDiagnostic {
    return {
        .frame_class = static_cast<task::SavedFrameClass>(encoded & 0xffU),
        .reason = static_cast<KernelPreemptionBlockReason>((encoded >> 8U) & 0xffU),
        .source_cpu = static_cast<uint16_t>((encoded >> 16U) & 0xffffU),
        .target_cpu = static_cast<uint16_t>((encoded >> 32U) & 0xffffU),
    };
}

[[nodiscard]] constexpr auto encode_migration_diagnostic(MigrationDiagnostic diagnostic) -> uint64_t {
    return static_cast<uint64_t>(diagnostic.frame_class) | (static_cast<uint64_t>(diagnostic.reason) << 8U) |
           (static_cast<uint64_t>(diagnostic.source_cpu) << 16U) | (static_cast<uint64_t>(diagnostic.target_cpu) << 32U);
}

[[nodiscard]] constexpr auto decode_migration_diagnostic(uint64_t encoded) -> MigrationDiagnostic {
    return {
        .frame_class = static_cast<task::SavedFrameClass>(encoded & 0xffU),
        .reason = static_cast<MigrationRejectionReason>((encoded >> 8U) & 0xffU),
        .source_cpu = static_cast<uint16_t>((encoded >> 16U) & 0xffffU),
        .target_cpu = static_cast<uint16_t>((encoded >> 32U) & 0xffffU),
    };
}

}  // namespace ker::mod::sched
