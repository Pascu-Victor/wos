#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "service_manifest.h"

namespace wos::init {

constexpr size_t SUPERVISOR_ACTION_CAPACITY = (MAX_SERVICES * 4) + 4;
constexpr size_t SUPERVISOR_HISTORY_CAPACITY = 128;
constexpr uint64_t SUPERVISOR_NO_DEADLINE = UINT64_MAX;

enum class ServiceState : uint8_t {
    WAITING = 0,
    STARTING = 1,
    RUNNING = 2,
    READY = 3,
    STOPPING = 4,
    EXITED = 5,
    FAILED = 6,
    BACKOFF = 7,
    DISABLED = 8,
};

enum class EnablementResult : uint8_t {
    CONDITION_NOT_MET = 0,
    CONDITION_MET = 1,
};

enum class SupervisorActionKind : uint8_t {
    SPAWN = 0,
    START_IPV4_PROBE = 1,
    STOP_IPV4_PROBE = 2,
    SEND_TERM = 3,
    SEND_KILL = 4,
    CLOSE_OUTPUT = 5,
    NON_JOURNAL_SHUTDOWN_COMPLETE = 6,
    SHUTDOWN_COMPLETE = 7,
};

enum class SupervisorEventKind : uint8_t {
    SPAWN_SUCCEEDED = 0,
    SPAWN_FAILED = 1,
    EXEC_SUCCEEDED = 2,
    EXEC_FAILED = 3,
    PROBE_PENDING = 4,
    PROBE_READY = 5,
    PROBE_ERROR = 6,
    EXITED = 7,
    QUIESCED = 8,
};

enum class SupervisorExitKind : uint8_t {
    NONE = 0,
    EXIT_CODE = 1,
    SIGNAL = 2,
};

enum class SupervisorControl : uint8_t {
    START = 0,
    STOP = 1,
    RESTART = 2,
};

enum class SupervisorShutdownPhase : uint8_t {
    NONE = 0,
    SERVICES = 1,
    WAITING_FOR_JOURNAL = 2,
    JOURNAL = 3,
    COMPLETE = 4,
};

enum class SupervisorStopReason : uint8_t {
    NONE = 0,
    CONTROL_STOP = 1,
    CONTROL_RESTART = 2,
    DEPENDENCY_LOSS = 3,
    START_FAILURE = 4,
    READINESS_FAILURE = 5,
    SHUTDOWN = 6,
};

enum class TransitionReason : uint8_t {
    INITIAL_ENABLED = 0,
    INITIAL_DISABLED = 1,
    INITIAL_SKIPPED_SUCCESS = 2,
    DEPENDENCIES_READY = 3,
    SPAWN_FAILED = 4,
    EXEC_SUCCEEDED = 5,
    EXEC_FAILED = 6,
    READINESS_READY = 7,
    READINESS_LOST = 8,
    READINESS_TIMEOUT = 9,
    PROCESS_EXIT_SUCCESS = 10,
    PROCESS_EXIT_FAILURE = 11,
    STOP_REQUESTED = 12,
    STOPPED = 13,
    DEPENDENCY_LOST = 14,
    RESTART_BACKOFF = 15,
    BACKOFF_EXPIRED = 16,
    RESTART_BUDGET_EXHAUSTED = 17,
    STABLE_RUN_RESET = 18,
    CONTROL_START = 19,
    CONTROL_RESTART = 20,
    SHUTDOWN = 21,
    START_TIMEOUT = 22,
    STOP_TIMEOUT = 23,
    DRAIN_TIMEOUT = 24,
    QUIESCED = 25,
};

enum class SupervisorResult : uint8_t {
    OK = 0,
    INVALID_MODEL = 1,
    INVALID_SERVICE = 2,
    INVALID_STATE = 3,
    INVALID_ENABLEMENT_COUNT = 4,
    STALE_GENERATION = 5,
    TIME_REGRESSION = 6,
    ACTION_OVERFLOW = 7,
};

struct SupervisorAction {
    SupervisorActionKind kind{SupervisorActionKind::SPAWN};
    uint8_t service{INVALID_SERVICE_ID};
    uint32_t generation{};
    uint64_t deadline_ms{SUPERVISOR_NO_DEADLINE};
};

struct SupervisorActionList {
    std::array<SupervisorAction, SUPERVISOR_ACTION_CAPACITY> entries{};
    uint8_t count{};
    bool overflowed{};

    void clear();
    [[nodiscard]] auto push(const SupervisorAction& action) -> bool;
    [[nodiscard]] auto size() const -> size_t;
    [[nodiscard]] auto operator[](size_t index) const -> const SupervisorAction&;
};

struct SupervisorEvent {
    SupervisorEventKind kind{SupervisorEventKind::SPAWN_SUCCEEDED};
    uint8_t service{INVALID_SERVICE_ID};
    uint32_t generation{};
    SupervisorExitKind exit_kind{SupervisorExitKind::NONE};
    int64_t pid{-1};
    int64_t pgid{-1};
    int32_t detail{};
};

struct SupervisorTransition {
    uint64_t sequence{};
    uint64_t now_ms{};
    uint32_t generation{};
    int32_t detail{};
    uint8_t service{INVALID_SERVICE_ID};
    ServiceState from{ServiceState::DISABLED};
    ServiceState to{ServiceState::DISABLED};
    TransitionReason reason{TransitionReason::INITIAL_DISABLED};
};

struct ServiceLifecycle {
    ServiceState state{ServiceState::DISABLED};
    SupervisorStopReason stop_reason{SupervisorStopReason::NONE};
    SupervisorExitKind last_exit_kind{SupervisorExitKind::NONE};
    uint32_t generation{};
    uint32_t failures_in_window{};
    uint32_t backoff_exponent{};
    int64_t pid{-1};
    int64_t pgid{-1};
    int32_t last_detail{};
    uint64_t spawn_attempts{};
    uint64_t state_since_ms{};
    uint64_t state_deadline_ms{SUPERVISOR_NO_DEADLINE};
    uint64_t drain_deadline_ms{SUPERVISOR_NO_DEADLINE};
    uint64_t restart_deadline_ms{SUPERVISOR_NO_DEADLINE};
    uint64_t restart_window_started_ms{SUPERVISOR_NO_DEADLINE};
    uint64_t ready_since_ms{SUPERVISOR_NO_DEADLINE};
    bool condition_met{};
    bool desired_running{};
    bool process_present{};
    bool spawn_result_pending{};
    bool exec_result_pending{};
    bool probe_active{};
    bool completion_success{};
    bool quiesced{true};
    bool resume_after_quiesce{};
    bool restart_scheduled{};
    bool stable_reset_done{};
    bool term_sent{};
    bool kill_sent{};
    bool drain_escalated{};
    bool abandoned{};
    bool manual_enable_override{};
};

struct SupervisorModel {
    const ServiceManifest* manifest{};
    const ServiceTopology* topology{};
    std::array<ServiceLifecycle, MAX_SERVICES> services{};
    std::array<SupervisorTransition, SUPERVISOR_HISTORY_CAPACITY> history{};
    uint64_t now_ms{};
    uint64_t next_history_sequence{1};
    uint32_t dropped_history{};
    uint16_t history_start{};
    uint16_t history_count{};
    uint8_t service_count{};
    SupervisorShutdownPhase shutdown_phase{SupervisorShutdownPhase::NONE};
    bool initialized{};
    bool non_journal_completion_emitted{};
    bool shutdown_completion_emitted{};
};

auto initialize_supervisor(SupervisorModel& model, const ServiceManifest& manifest, const ServiceTopology& topology,
                           std::span<const EnablementResult> enablement, uint64_t now_ms) -> SupervisorResult;
auto supervisor_advance(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult;
auto supervisor_handle_event(SupervisorModel& model, uint64_t now_ms, const SupervisorEvent& event, SupervisorActionList& actions)
    -> SupervisorResult;
auto supervisor_control(SupervisorModel& model, uint64_t now_ms, uint8_t service, SupervisorControl control, SupervisorActionList& actions)
    -> SupervisorResult;
auto supervisor_begin_shutdown(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult;
auto supervisor_begin_journal_shutdown(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult;

[[nodiscard]] auto supervisor_dependencies_satisfied(const SupervisorModel& model, uint8_t service) -> bool;
[[nodiscard]] auto supervisor_service(const SupervisorModel& model, uint8_t service) -> const ServiceLifecycle*;
[[nodiscard]] auto supervisor_history_size(const SupervisorModel& model) -> size_t;
[[nodiscard]] auto supervisor_history_at(const SupervisorModel& model, size_t oldest_first_index) -> const SupervisorTransition*;
[[nodiscard]] auto supervisor_shutdown_complete(const SupervisorModel& model) -> bool;

}  // namespace wos::init
