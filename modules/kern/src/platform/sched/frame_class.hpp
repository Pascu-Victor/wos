#pragma once

#include <cstdint>

namespace ker::mod::sched::task {

// Durable meaning of the register/frame image stored in Task::context.  Keep
// this independent of the mutable scheduler flags that happened to be set when
// the image was captured.
enum class SavedFrameClass : uint8_t {
    INVALID,
    USER_RETURN,
    VOLUNTARY_PARKED_KERNEL,
    TIMER_PREEMPTED_PROCESS_KERNEL,
    DAEMON_KERNEL,
};

enum class SavedFrameOwner : uint8_t {
    INVALID,
    PROCESS,
    DAEMON,
    IDLE,
};

enum class SavedFrameOrigin : uint8_t {
    INVALID,
    SYNTHETIC_USER_RETURN,
    INTERRUPT,
    DAEMON_START,
};

enum class SavedFrameSelectors : uint8_t {
    INVALID,
    USER,
    KERNEL,
};

// The durable frame class, rather than selectors inspected ad hoc by each
// consumer, determines which architectural return contract is legal.
enum class SavedFrameRestoreKind : uint8_t {
    REJECT,
    USER_IRET,
    SAME_CPL_KERNEL_IRET,
};

struct SavedFrameRestorePolicy {
    SavedFrameRestoreKind kind{SavedFrameRestoreKind::REJECT};
    bool validate_as_user{};
    bool deliver_signals{};
    bool restore_user_fpu{};
};

// Idle has no durable continuation: the scheduler re-enters its dedicated
// stack and loop directly. A timer can nevertheless interrupt between any two
// instructions in that loop, so validate the saved RIP against the complete
// assembly-owned half-open range rather than only its entry address.
[[nodiscard]] constexpr auto idle_loop_resume_ip_is_valid(uint64_t rip, uint64_t loop_begin, uint64_t loop_end) -> bool {
    return loop_begin < loop_end && rip >= loop_begin && rip < loop_end;
}

// Scalar facts let host tests exercise the classification table without
// depending on the kernel Task type or address-space implementation.  Kernel
// callers are responsible for deriving these facts from the live frame while
// they still own its producer context.
struct SavedFrameClassificationInput {
    SavedFrameOwner owner{SavedFrameOwner::INVALID};
    SavedFrameOrigin origin{SavedFrameOrigin::INVALID};
    SavedFrameSelectors selectors{SavedFrameSelectors::INVALID};
    bool instruction_pointer_valid{};
    bool stack_pointer_valid{};
    bool flags_valid{};
    bool voluntary_process{};
    bool timer_interrupt{};
};

[[nodiscard]] constexpr auto classify_saved_frame(const SavedFrameClassificationInput& input) -> SavedFrameClass {
    if (!input.instruction_pointer_valid || !input.stack_pointer_valid || !input.flags_valid) {
        return SavedFrameClass::INVALID;
    }

    if (input.selectors == SavedFrameSelectors::USER) {
        if (input.owner == SavedFrameOwner::PROCESS &&
            (input.origin == SavedFrameOrigin::SYNTHETIC_USER_RETURN || input.origin == SavedFrameOrigin::INTERRUPT)) {
            return SavedFrameClass::USER_RETURN;
        }
        return SavedFrameClass::INVALID;
    }

    if (input.selectors != SavedFrameSelectors::KERNEL) {
        return SavedFrameClass::INVALID;
    }

    if (input.owner == SavedFrameOwner::DAEMON) {
        if (input.origin == SavedFrameOrigin::DAEMON_START || (input.origin == SavedFrameOrigin::INTERRUPT && input.timer_interrupt)) {
            return SavedFrameClass::DAEMON_KERNEL;
        }
        return SavedFrameClass::INVALID;
    }

    if (input.owner == SavedFrameOwner::PROCESS && input.origin == SavedFrameOrigin::INTERRUPT && input.timer_interrupt) {
        return input.voluntary_process ? SavedFrameClass::VOLUNTARY_PARKED_KERNEL : SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL;
    }

    return SavedFrameClass::INVALID;
}

[[nodiscard]] constexpr auto saved_frame_class_is_user_return(SavedFrameClass frame_class) -> bool {
    return frame_class == SavedFrameClass::USER_RETURN;
}

[[nodiscard]] constexpr auto saved_frame_class_is_kernel(SavedFrameClass frame_class) -> bool {
    return frame_class == SavedFrameClass::VOLUNTARY_PARKED_KERNEL || frame_class == SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL ||
           frame_class == SavedFrameClass::DAEMON_KERNEL;
}

// A timer that keeps running the interrupted task returns through the live IRQ
// frame, which can legitimately differ from the task's last durable context.
// A handoff instead returns through the incoming task's saved frame. Keep that
// provenance choice explicit at the final validation boundary.
[[nodiscard]] constexpr auto select_timer_return_frame_class(bool returning_interrupted_task, SavedFrameClass interrupted_frame_class,
                                                             SavedFrameClass durable_frame_class) -> SavedFrameClass {
    return returning_interrupted_task ? interrupted_frame_class : durable_frame_class;
}

// The timer return tail consumes a one-shot class that was validated against
// the live IRQ frame.  Other user-return paths continue to rely on the durable
// Task::context class.  Keeping this selection explicit prevents a live user
// return from being rejected merely because the task's last durable snapshot
// is a parked kernel frame, while a token for another task is ignored.
[[nodiscard]] constexpr auto select_user_fpu_restore_frame_class(bool timer_return_matches_task, SavedFrameClass timer_return_frame_class,
                                                                 SavedFrameClass durable_frame_class) -> SavedFrameClass {
    return timer_return_matches_task ? timer_return_frame_class : durable_frame_class;
}

[[nodiscard]] constexpr auto saved_frame_restore_policy(SavedFrameClass frame_class) -> SavedFrameRestorePolicy {
    switch (frame_class) {
        case SavedFrameClass::USER_RETURN:
            return {
                .kind = SavedFrameRestoreKind::USER_IRET,
                .validate_as_user = true,
                .deliver_signals = true,
                .restore_user_fpu = true,
            };
        case SavedFrameClass::VOLUNTARY_PARKED_KERNEL:
        case SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL:
        case SavedFrameClass::DAEMON_KERNEL:
            return {
                .kind = SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET,
                .validate_as_user = false,
                .deliver_signals = false,
                .restore_user_fpu = false,
            };
        case SavedFrameClass::INVALID:
            return {};
    }
    return {};
}

[[nodiscard]] constexpr auto saved_frame_restore_kind_name(SavedFrameRestoreKind kind) -> const char* {
    switch (kind) {
        case SavedFrameRestoreKind::USER_IRET:
            return "user-iret";
        case SavedFrameRestoreKind::SAME_CPL_KERNEL_IRET:
            return "same-cpl-kernel-iret";
        case SavedFrameRestoreKind::REJECT:
            return "reject";
    }
    return "reject";
}

[[nodiscard]] constexpr auto saved_frame_class_name(SavedFrameClass frame_class) -> const char* {
    switch (frame_class) {
        case SavedFrameClass::USER_RETURN:
            return "user-return";
        case SavedFrameClass::VOLUNTARY_PARKED_KERNEL:
            return "voluntary-parked-kernel";
        case SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL:
            return "timer-preempted-process-kernel";
        case SavedFrameClass::DAEMON_KERNEL:
            return "daemon-kernel";
        case SavedFrameClass::INVALID:
            return "invalid";
    }
    return "invalid";
}

}  // namespace ker::mod::sched::task
