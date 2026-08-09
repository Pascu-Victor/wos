#pragma once

#include <cstdint>
#include <platform/sched/frame_class.hpp>

namespace ker::mod::sched {

struct KernelPreemptionBootPolicyInput {
    bool default_enabled{};
    bool force_on{};
    bool force_off{};
};

// Explicit off wins if a malformed command line supplies both overrides. This
// makes the emergency rollback deterministic at every default setting.
[[nodiscard]] constexpr auto resolve_kernel_preemption_boot_policy(KernelPreemptionBootPolicyInput input) -> bool {
    if (input.force_off) {
        return false;
    }
    if (input.force_on) {
        return true;
    }
    return input.default_enabled;
}

enum class KernelPreemptionBlockReason : uint8_t {
    NONE,
    NOT_KERNEL_FRAME,
    INVALID_FRAME,
    POLICY_DISABLED,
    MIGRATION_UNSTABLE,
    PREEMPT_DISABLED,
    RETURN_TRANSITION,
};

struct KernelPreemptionEligibilityInput {
    task::SavedFrameClass frame_class{task::SavedFrameClass::INVALID};
    bool ordinary_process_kernel_enabled{};
    bool same_cpu_migration_guarded{};
    uint32_t preempt_disable_depth{};
    bool scheduler_transition_active{};
    bool deferred_task_switch{};
    bool wants_block{};
    bool waitpid_publish_pending{};
};

struct KernelPreemptionEligibility {
    bool kernel_frame{};
    bool restorable{};
    bool can_switch{};
    bool record_pending{};
    KernelPreemptionBlockReason reason{KernelPreemptionBlockReason::INVALID_FRAME};
};

[[nodiscard]] constexpr auto evaluate_kernel_preemption(KernelPreemptionEligibilityInput input) -> KernelPreemptionEligibility {
    if (input.frame_class == task::SavedFrameClass::USER_RETURN) {
        if (input.preempt_disable_depth != 0) {
            return {
                .kernel_frame = false,
                .restorable = true,
                .can_switch = false,
                .record_pending = true,
                .reason = KernelPreemptionBlockReason::PREEMPT_DISABLED,
            };
        }

        bool const RETURN_TRANSITION =
            input.scheduler_transition_active || input.deferred_task_switch || input.wants_block || input.waitpid_publish_pending;
        if (RETURN_TRANSITION) {
            return {
                .kernel_frame = false,
                .restorable = true,
                .can_switch = false,
                .record_pending = true,
                .reason = KernelPreemptionBlockReason::RETURN_TRANSITION,
            };
        }

        return {
            .kernel_frame = false,
            .restorable = true,
            .can_switch = false,
            .record_pending = false,
            .reason = KernelPreemptionBlockReason::NOT_KERNEL_FRAME,
        };
    }

    auto const RESTORE_POLICY = task::saved_frame_restore_policy(input.frame_class);
    if (RESTORE_POLICY.kind != task::SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET) {
        return {};
    }

    bool const ORDINARY_PROCESS_KERNEL = input.frame_class == task::SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL;
    if (ORDINARY_PROCESS_KERNEL && !input.ordinary_process_kernel_enabled) {
        return {
            .kernel_frame = true,
            .restorable = true,
            .can_switch = false,
            .record_pending = false,
            .reason = KernelPreemptionBlockReason::POLICY_DISABLED,
        };
    }

    if (ORDINARY_PROCESS_KERNEL && !input.same_cpu_migration_guarded) {
        return {
            .kernel_frame = true,
            .restorable = true,
            .can_switch = false,
            .record_pending = false,
            .reason = KernelPreemptionBlockReason::MIGRATION_UNSTABLE,
        };
    }

    if (input.preempt_disable_depth != 0) {
        return {
            .kernel_frame = true,
            .restorable = true,
            .can_switch = false,
            .record_pending = true,
            .reason = KernelPreemptionBlockReason::PREEMPT_DISABLED,
        };
    }

    bool const ORDINARY_RETURN_TRANSITION = ORDINARY_PROCESS_KERNEL && (input.deferred_task_switch || input.wants_block);
    bool const RETURN_TRANSITION = input.scheduler_transition_active || input.waitpid_publish_pending || ORDINARY_RETURN_TRANSITION;
    if (RETURN_TRANSITION) {
        return {
            .kernel_frame = true,
            .restorable = true,
            .can_switch = false,
            .record_pending = true,
            .reason = KernelPreemptionBlockReason::RETURN_TRANSITION,
        };
    }

    return {
        .kernel_frame = true,
        .restorable = true,
        .can_switch = true,
        .record_pending = false,
        .reason = KernelPreemptionBlockReason::NONE,
    };
}

[[nodiscard]] constexpr auto kernel_preemption_block_reason_name(KernelPreemptionBlockReason reason) -> const char* {
    switch (reason) {
        case KernelPreemptionBlockReason::NONE:
            return "none";
        case KernelPreemptionBlockReason::NOT_KERNEL_FRAME:
            return "not-kernel-frame";
        case KernelPreemptionBlockReason::INVALID_FRAME:
            return "invalid-frame";
        case KernelPreemptionBlockReason::POLICY_DISABLED:
            return "policy-disabled";
        case KernelPreemptionBlockReason::MIGRATION_UNSTABLE:
            return "migration-unstable";
        case KernelPreemptionBlockReason::PREEMPT_DISABLED:
            return "preempt-disabled";
        case KernelPreemptionBlockReason::RETURN_TRANSITION:
            return "return-transition";
    }
    return "invalid-frame";
}

enum class PreemptGuardTransitionError : uint8_t {
    NONE,
    UNDERFLOW,
    OVERFLOW,
};

struct PreemptGuardTransition {
    uint32_t depth{};
    bool outermost{};
    PreemptGuardTransitionError error{PreemptGuardTransitionError::NONE};
};

[[nodiscard]] constexpr auto preempt_disable_transition(uint32_t depth) -> PreemptGuardTransition {
    if (depth == UINT32_MAX) {
        return {.depth = depth, .error = PreemptGuardTransitionError::OVERFLOW};
    }
    return {.depth = depth + 1U, .outermost = depth == 0};
}

[[nodiscard]] constexpr auto preempt_enable_transition(uint32_t depth) -> PreemptGuardTransition {
    if (depth == 0) {
        return {.error = PreemptGuardTransitionError::UNDERFLOW};
    }
    return {.depth = depth - 1U, .outermost = depth == 1U};
}

enum class PreemptPendingAction : uint8_t {
    NONE,
    PRESERVE,
    SERVICE,
};

[[nodiscard]] constexpr auto preempt_pending_action(uint32_t depth, bool pending, bool scheduler_transition_active)
    -> PreemptPendingAction {
    if (!pending || depth != 0) {
        return PreemptPendingAction::NONE;
    }
    return scheduler_transition_active ? PreemptPendingAction::PRESERVE : PreemptPendingAction::SERVICE;
}

// The architectural return tail is the final ownership boundary. A no-op
// commit is harmless while preemption is disabled, but changing current_task
// would strand a protected region on a different task/stack.
[[nodiscard]] constexpr auto handoff_commit_allowed(uint32_t outgoing_preempt_depth, bool ownership_changes) -> bool {
    return !ownership_changes || outgoing_preempt_depth == 0;
}

// Different tasks must never publish ownership of the same kernel stack.  A
// zero outgoing stack is permitted for the initial boot handoff; validity of
// each nonzero stack is checked by the context-switch boundary itself.
[[nodiscard]] constexpr auto handoff_stack_collision(uint64_t outgoing_stack_top, uint64_t incoming_stack_top, bool ownership_changes)
    -> bool {
    return ownership_changes && outgoing_stack_top != 0 && outgoing_stack_top == incoming_stack_top;
}

}  // namespace ker::mod::sched
