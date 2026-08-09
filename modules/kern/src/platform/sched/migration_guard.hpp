#pragma once

#include <cstdint>

namespace ker::mod::sched {

namespace task {
struct Task;
}  // namespace task

constexpr uint32_t MIGRATION_CPU_INVALID = UINT32_MAX;

struct MigrationGuardState {
    uint32_t depth{};
    uint32_t owner_cpu{MIGRATION_CPU_INVALID};
};

enum class MigrationGuardTransitionError : uint8_t {
    NONE,
    INVALID_CPU,
    DEPTH_OVERFLOW,
    ENABLE_WITHOUT_DISABLE,
    WRONG_CPU,
};

struct MigrationGuardTransition {
    MigrationGuardState state{};
    MigrationGuardTransitionError error{MigrationGuardTransitionError::NONE};
    bool outermost{};
};

[[nodiscard]] constexpr auto encode_migration_guard_state(MigrationGuardState state) -> uint64_t {
    return (static_cast<uint64_t>(state.owner_cpu) << 32U) | state.depth;
}

[[nodiscard]] constexpr auto decode_migration_guard_state(uint64_t encoded) -> MigrationGuardState {
    return MigrationGuardState{
        .depth = static_cast<uint32_t>(encoded),
        .owner_cpu = static_cast<uint32_t>(encoded >> 32U),
    };
}

[[nodiscard]] constexpr auto migration_guard_disabled(MigrationGuardState state) -> bool { return state.depth != 0; }

[[nodiscard]] constexpr auto migration_disable_transition(MigrationGuardState current, uint64_t cpu_no) -> MigrationGuardTransition {
    if (cpu_no >= MIGRATION_CPU_INVALID) {
        return {.state = current, .error = MigrationGuardTransitionError::INVALID_CPU};
    }
    if (current.depth == UINT32_MAX) {
        return {.state = current, .error = MigrationGuardTransitionError::DEPTH_OVERFLOW};
    }
    if (current.depth != 0 && current.owner_cpu != cpu_no) {
        return {.state = current, .error = MigrationGuardTransitionError::WRONG_CPU};
    }

    bool const OUTERMOST = current.depth == 0;
    return {
        .state =
            MigrationGuardState{
                .depth = current.depth + 1,
                .owner_cpu = OUTERMOST ? static_cast<uint32_t>(cpu_no) : current.owner_cpu,
            },
        .error = MigrationGuardTransitionError::NONE,
        .outermost = OUTERMOST,
    };
}

[[nodiscard]] constexpr auto migration_enable_transition(MigrationGuardState current, uint64_t cpu_no) -> MigrationGuardTransition {
    if (current.depth == 0) {
        return {.state = current, .error = MigrationGuardTransitionError::ENABLE_WITHOUT_DISABLE};
    }
    if (cpu_no >= MIGRATION_CPU_INVALID) {
        return {.state = current, .error = MigrationGuardTransitionError::INVALID_CPU};
    }
    if (current.owner_cpu != cpu_no) {
        return {.state = current, .error = MigrationGuardTransitionError::WRONG_CPU};
    }

    bool const OUTERMOST = current.depth == 1;
    return {
        .state =
            MigrationGuardState{
                .depth = current.depth - 1,
                .owner_cpu = OUTERMOST ? MIGRATION_CPU_INVALID : current.owner_cpu,
            },
        .error = MigrationGuardTransitionError::NONE,
        .outermost = OUTERMOST,
    };
}

// Return the only legal target for a placement request. An invalid result for
// a disabled task is a hard rejection: callers must not fall back to another
// CPU and silently violate the guard.
[[nodiscard]] constexpr auto migration_guard_target_cpu(MigrationGuardState state, uint64_t requested_cpu, uint64_t core_count)
    -> uint64_t {
    if (!migration_guard_disabled(state)) {
        return requested_cpu;
    }
    if (state.owner_cpu >= core_count) {
        return UINT64_MAX;
    }
    return state.owner_cpu;
}

[[nodiscard]] auto migration_disable_token_at(uint64_t caller) -> task::Task*;
void migration_disable_at(uint64_t caller);
void migration_disable();
void migration_enable_token_at(task::Task* task, uint64_t caller);
void migration_enable_at(uint64_t caller);
void migration_enable();
[[nodiscard]] auto migration_count() -> uint32_t;
[[nodiscard]] auto migration_disabled() -> bool;
void assert_current_cpu_stable();

class MigrationGuard {
   public:
    MigrationGuard();
    ~MigrationGuard();

    void release();

    MigrationGuard(const MigrationGuard&) = delete;
    MigrationGuard(MigrationGuard&&) = delete;
    auto operator=(const MigrationGuard&) -> MigrationGuard& = delete;
    auto operator=(MigrationGuard&&) -> MigrationGuard& = delete;

   private:
    task::Task* task_{};
};

}  // namespace ker::mod::sched
