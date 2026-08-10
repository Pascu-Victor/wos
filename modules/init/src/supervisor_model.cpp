#include "supervisor_model.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

#include "service_manifest.h"

namespace wos::init {

namespace {

[[nodiscard]] auto valid_model(const SupervisorModel& model) -> bool {
    return model.initialized && model.manifest != nullptr && model.topology != nullptr && model.service_count <= MAX_SERVICES &&
           model.manifest->service_count == model.service_count && model.topology->service_count == model.service_count;
}

[[nodiscard]] auto deadline_after(uint64_t now_ms, uint32_t delay_ms) -> uint64_t {
    constexpr uint64_t MAX_REAL_DEADLINE = std::numeric_limits<uint64_t>::max() - 1;
    if (now_ms >= MAX_REAL_DEADLINE || static_cast<uint64_t>(delay_ms) > MAX_REAL_DEADLINE - now_ms) {
        return MAX_REAL_DEADLINE;
    }
    return now_ms + delay_ms;
}

[[nodiscard]] auto optional_deadline_after(uint64_t now_ms, uint32_t delay_ms) -> uint64_t {
    return delay_ms == 0 ? SUPERVISOR_NO_DEADLINE : deadline_after(now_ms, delay_ms);
}

[[nodiscard]] auto deadline_reached(uint64_t now_ms, uint64_t deadline_ms) -> bool {
    return deadline_ms != SUPERVISOR_NO_DEADLINE && now_ms >= deadline_ms;
}

[[nodiscard]] auto elapsed_at_least(uint64_t now_ms, uint64_t since_ms, uint32_t duration_ms) -> bool {
    return since_ms != SUPERVISOR_NO_DEADLINE && now_ms >= since_ms && now_ms - since_ms >= duration_ms;
}

void record_transition(SupervisorModel& model, uint8_t service, ServiceState from, ServiceState to, TransitionReason reason,
                       int32_t detail) {
    size_t index = 0;
    if (model.history_count < SUPERVISOR_HISTORY_CAPACITY) {
        index = (static_cast<size_t>(model.history_start) + model.history_count) % SUPERVISOR_HISTORY_CAPACITY;
        model.history_count++;
    } else {
        index = model.history_start;
        model.history_start = static_cast<uint16_t>((model.history_start + 1) % SUPERVISOR_HISTORY_CAPACITY);
        if (model.dropped_history != std::numeric_limits<uint32_t>::max()) {
            model.dropped_history++;
        }
    }

    auto& entry = model.history.at(index);
    entry = SupervisorTransition{
        .sequence = model.next_history_sequence,
        .now_ms = model.now_ms,
        .generation = service < model.service_count ? model.services.at(service).generation : 0,
        .detail = detail,
        .service = service,
        .from = from,
        .to = to,
        .reason = reason,
    };
    if (model.next_history_sequence != std::numeric_limits<uint64_t>::max()) {
        model.next_history_sequence++;
    }
}

void transition_service(SupervisorModel& model, uint8_t service, ServiceState state, TransitionReason reason, int32_t detail = 0) {
    auto& lifecycle = model.services.at(service);
    ServiceState const FROM = lifecycle.state;
    lifecycle.state = state;
    if (FROM != state) {
        lifecycle.state_since_ms = model.now_ms;
    }
    record_transition(model, service, FROM, state, reason, detail);
}

[[nodiscard]] auto emit_action(SupervisorActionList& actions, SupervisorActionKind kind, uint8_t service, uint32_t generation,
                               uint64_t deadline_ms = SUPERVISOR_NO_DEADLINE) -> bool {
    return actions.push(SupervisorAction{
        .kind = kind,
        .service = service,
        .generation = generation,
        .deadline_ms = deadline_ms,
    });
}

[[nodiscard]] auto finish_result(const SupervisorActionList& actions) -> SupervisorResult {
    return actions.overflowed ? SupervisorResult::ACTION_OVERFLOW : SupervisorResult::OK;
}

[[nodiscard]] auto prepare_call(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult {
    actions.clear();
    if (!valid_model(model)) {
        return SupervisorResult::INVALID_MODEL;
    }
    if (now_ms < model.now_ms) {
        return SupervisorResult::TIME_REGRESSION;
    }
    model.now_ms = now_ms;
    return SupervisorResult::OK;
}

[[nodiscard]] auto exit_succeeded(const ServiceLifecycle& lifecycle) -> bool {
    return lifecycle.last_exit_kind == SupervisorExitKind::EXIT_CODE && lifecycle.last_detail == 0;
}

[[nodiscard]] auto service_satisfies_dependencies(const SupervisorModel& model, uint8_t service) -> bool {
    if (service >= model.service_count) {
        return false;
    }
    auto const& lifecycle = model.services.at(service);
    if (lifecycle.state == ServiceState::READY) {
        return true;
    }
    auto const& spec = model.manifest->services.at(service);
    return spec.type == ServiceType::ONESHOT && lifecycle.state == ServiceState::EXITED && lifecycle.completion_success &&
           lifecycle.quiesced;
}

[[nodiscard]] auto dependencies_satisfied(const SupervisorModel& model, uint8_t service) -> bool {
    if (service >= model.service_count) {
        return false;
    }
    uint16_t dependencies = model.manifest->services.at(service).dependency_mask;
    while (dependencies != 0) {
        uint8_t dependency = 0;
        while ((dependencies & static_cast<uint16_t>(1U << dependency)) == 0) {
            dependency++;
        }
        dependencies &= static_cast<uint16_t>(~static_cast<uint16_t>(1U << dependency));
        if (dependency >= model.service_count || !service_satisfies_dependencies(model, dependency)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] auto shutdown_settled(const ServiceLifecycle& lifecycle) -> bool {
    if (lifecycle.state == ServiceState::DISABLED) {
        return true;
    }
    if (lifecycle.state == ServiceState::FAILED && lifecycle.abandoned) {
        return true;
    }
    return (lifecycle.state == ServiceState::EXITED || lifecycle.state == ServiceState::FAILED) && lifecycle.quiesced &&
           !lifecycle.process_present && !lifecycle.spawn_result_pending && !lifecycle.exec_result_pending;
}

void reset_restart_tracking(ServiceLifecycle& lifecycle) {
    lifecycle.failures_in_window = 0;
    lifecycle.backoff_exponent = 0;
    lifecycle.restart_window_started_ms = SUPERVISOR_NO_DEADLINE;
    lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
    lifecycle.restart_scheduled = false;
    lifecycle.stable_reset_done = false;
}

[[nodiscard]] auto restart_allowed(const ServiceSpec& spec, bool failed) -> bool {
    switch (spec.restart) {
        case RestartPolicy::NEVER:
            return false;
        case RestartPolicy::ON_FAILURE:
            return failed;
        case RestartPolicy::ALWAYS:
            return true;
    }
    return false;
}

[[nodiscard]] auto backoff_delay(const ServiceSpec& spec, uint32_t exponent) -> uint32_t {
    uint64_t delay = spec.backoff_initial_ms;
    uint64_t const CAP = spec.backoff_max_ms;
    if (delay >= CAP) {
        return static_cast<uint32_t>(CAP);
    }
    for (uint32_t shift = 0; shift < exponent && delay < CAP; shift++) {
        delay = delay > CAP - delay ? CAP : delay + delay;
    }
    return static_cast<uint32_t>(delay < CAP ? delay : CAP);
}

void schedule_restart(SupervisorModel& model, uint8_t service, bool failed, int32_t detail) {
    auto& lifecycle = model.services.at(service);
    auto const& spec = model.manifest->services.at(service);
    lifecycle.stop_reason = SupervisorStopReason::NONE;
    lifecycle.resume_after_quiesce = false;
    lifecycle.last_detail = detail;

    if (lifecycle.abandoned || model.shutdown_phase != SupervisorShutdownPhase::NONE || !lifecycle.desired_running ||
        !restart_allowed(spec, failed)) {
        lifecycle.desired_running = false;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        transition_service(model, service, failed ? ServiceState::FAILED : ServiceState::EXITED,
                           failed ? TransitionReason::PROCESS_EXIT_FAILURE : TransitionReason::PROCESS_EXIT_SUCCESS, detail);
        return;
    }

    if (lifecycle.restart_window_started_ms == SUPERVISOR_NO_DEADLINE ||
        elapsed_at_least(model.now_ms, lifecycle.restart_window_started_ms, spec.restart_window_ms)) {
        lifecycle.restart_window_started_ms = model.now_ms;
        lifecycle.failures_in_window = 0;
    }
    if (spec.restart_budget == 0 || lifecycle.failures_in_window >= spec.restart_budget) {
        lifecycle.desired_running = false;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        transition_service(model, service, ServiceState::FAILED, TransitionReason::RESTART_BUDGET_EXHAUSTED, detail);
        return;
    }

    lifecycle.failures_in_window++;
    uint32_t const DELAY = backoff_delay(spec, lifecycle.backoff_exponent);
    if (lifecycle.backoff_exponent != std::numeric_limits<uint32_t>::max()) {
        lifecycle.backoff_exponent++;
    }
    lifecycle.restart_deadline_ms = deadline_after(model.now_ms, DELAY);
    lifecycle.restart_scheduled = true;
    transition_service(model, service, ServiceState::BACKOFF, TransitionReason::RESTART_BACKOFF, detail);
}

void finalize_quiesced_stop(SupervisorModel& model, uint8_t service) {
    auto& lifecycle = model.services.at(service);
    auto const& spec = model.manifest->services.at(service);
    SupervisorStopReason const STOP_REASON = lifecycle.stop_reason;
    lifecycle.state_deadline_ms = SUPERVISOR_NO_DEADLINE;
    lifecycle.drain_deadline_ms = SUPERVISOR_NO_DEADLINE;
    lifecycle.term_sent = false;
    lifecycle.kill_sent = false;
    lifecycle.drain_escalated = false;
    lifecycle.stop_reason = SupervisorStopReason::NONE;

    if (STOP_REASON == SupervisorStopReason::CONTROL_RESTART || STOP_REASON == SupervisorStopReason::DEPENDENCY_LOSS) {
        lifecycle.resume_after_quiesce = false;
        lifecycle.completion_success = false;
        if (lifecycle.desired_running && model.shutdown_phase == SupervisorShutdownPhase::NONE) {
            transition_service(model, service, ServiceState::WAITING,
                               STOP_REASON == SupervisorStopReason::CONTROL_RESTART ? TransitionReason::CONTROL_RESTART
                                                                                    : TransitionReason::DEPENDENCY_LOST);
        } else {
            transition_service(model, service, ServiceState::EXITED, TransitionReason::STOPPED);
        }
        return;
    }

    if (STOP_REASON == SupervisorStopReason::CONTROL_STOP || STOP_REASON == SupervisorStopReason::SHUTDOWN) {
        lifecycle.desired_running = false;
        lifecycle.resume_after_quiesce = false;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        lifecycle.completion_success = false;
        transition_service(model, service, ServiceState::EXITED,
                           STOP_REASON == SupervisorStopReason::SHUTDOWN ? TransitionReason::SHUTDOWN : TransitionReason::STOPPED);
        return;
    }

    bool const SUCCESS = exit_succeeded(lifecycle);
    if (spec.type == ServiceType::ONESHOT && spec.readiness == ReadinessKind::PROCESS_EXIT_SUCCESS && SUCCESS &&
        STOP_REASON == SupervisorStopReason::READINESS_FAILURE) {
        lifecycle.desired_running = false;
        lifecycle.completion_success = true;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        transition_service(model, service, ServiceState::EXITED, TransitionReason::PROCESS_EXIT_SUCCESS);
        return;
    }

    bool const FAILED = STOP_REASON == SupervisorStopReason::START_FAILURE || lifecycle.last_exit_kind == SupervisorExitKind::NONE ||
                        lifecycle.last_exit_kind == SupervisorExitKind::SIGNAL || !SUCCESS;
    lifecycle.completion_success = false;
    schedule_restart(model, service, FAILED, lifecycle.last_detail);
}

void stop_probe(SupervisorModel& model, uint8_t service, SupervisorActionList& actions) {
    auto& lifecycle = model.services.at(service);
    if (!lifecycle.probe_active) {
        return;
    }
    (void)emit_action(actions, SupervisorActionKind::STOP_IPV4_PROBE, service, lifecycle.generation);
    lifecycle.probe_active = false;
}

void begin_stop(SupervisorModel& model, uint8_t service, SupervisorStopReason reason, TransitionReason transition_reason,
                SupervisorActionList& actions) {
    auto& lifecycle = model.services.at(service);
    bool const ENTERING_STOP = lifecycle.state != ServiceState::STOPPING;
    if (reason == SupervisorStopReason::CONTROL_STOP || reason == SupervisorStopReason::SHUTDOWN) {
        lifecycle.desired_running = false;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
    } else if (reason == SupervisorStopReason::CONTROL_RESTART || reason == SupervisorStopReason::DEPENDENCY_LOSS) {
        lifecycle.desired_running = true;
        lifecycle.resume_after_quiesce = true;
    }

    if (lifecycle.stop_reason == SupervisorStopReason::NONE || reason == SupervisorStopReason::SHUTDOWN ||
        reason == SupervisorStopReason::CONTROL_STOP || reason == SupervisorStopReason::CONTROL_RESTART) {
        lifecycle.stop_reason = reason;
    }
    stop_probe(model, service, actions);
    if (ENTERING_STOP) {
        transition_service(model, service, ServiceState::STOPPING, transition_reason, lifecycle.last_detail);
    }

    if (lifecycle.quiesced) {
        finalize_quiesced_stop(model, service);
        return;
    }
    if (!lifecycle.term_sent && !lifecycle.spawn_result_pending && lifecycle.pid > 0 && lifecycle.pgid > 0) {
        uint64_t const DEADLINE = deadline_after(model.now_ms, model.manifest->services.at(service).stop_term_ms);
        (void)emit_action(actions, SupervisorActionKind::SEND_TERM, service, lifecycle.generation, DEADLINE);
        lifecycle.term_sent = true;
        lifecycle.kill_sent = false;
        lifecycle.state_deadline_ms = DEADLINE;
    } else if (!lifecycle.term_sent && !lifecycle.kill_sent && !lifecycle.drain_escalated && !lifecycle.abandoned &&
               (ENTERING_STOP || lifecycle.state_deadline_ms == SUPERVISOR_NO_DEADLINE)) {
        lifecycle.state_deadline_ms = deadline_after(model.now_ms, model.manifest->services.at(service).stop_term_ms);
    }
}

[[nodiscard]] auto cascade_dependency_loss(SupervisorModel& model, SupervisorActionList& actions) -> bool {
    bool changed = false;
    for (size_t order_index = 0; order_index < model.service_count; order_index++) {
        uint8_t const SERVICE = model.topology->stop_order.at(order_index);
        if (SERVICE >= model.service_count) {
            continue;
        }
        auto& lifecycle = model.services.at(SERVICE);
        bool const ACTIVE =
            lifecycle.state == ServiceState::STARTING || lifecycle.state == ServiceState::RUNNING || lifecycle.state == ServiceState::READY;
        if (!ACTIVE || !lifecycle.desired_running || dependencies_satisfied(model, SERVICE)) {
            continue;
        }
        begin_stop(model, SERVICE, SupervisorStopReason::DEPENDENCY_LOSS, TransitionReason::DEPENDENCY_LOST, actions);
        changed = true;
    }
    return changed;
}

void process_service_deadlines(SupervisorModel& model, SupervisorActionList& actions) {
    for (uint8_t service = 0; service < model.service_count; service++) {
        auto& lifecycle = model.services.at(service);
        auto const& spec = model.manifest->services.at(service);

        if (lifecycle.state == ServiceState::READY && !lifecycle.stable_reset_done &&
            elapsed_at_least(model.now_ms, lifecycle.ready_since_ms, spec.stable_run_ms)) {
            if (lifecycle.failures_in_window != 0 || lifecycle.backoff_exponent != 0) {
                record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::STABLE_RUN_RESET, 0);
            }
            reset_restart_tracking(lifecycle);
            lifecycle.stable_reset_done = true;
        }

        if (lifecycle.state == ServiceState::BACKOFF && lifecycle.quiesced &&
            deadline_reached(model.now_ms, lifecycle.restart_deadline_ms)) {
            lifecycle.restart_scheduled = false;
            lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
            transition_service(model, service, ServiceState::WAITING, TransitionReason::BACKOFF_EXPIRED);
        }

        if (lifecycle.state == ServiceState::STARTING && deadline_reached(model.now_ms, lifecycle.state_deadline_ms)) {
            lifecycle.last_exit_kind = SupervisorExitKind::NONE;
            lifecycle.last_detail = ETIMEDOUT;
            record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::START_TIMEOUT, ETIMEDOUT);
            begin_stop(model, service, SupervisorStopReason::START_FAILURE, TransitionReason::START_TIMEOUT, actions);
            continue;
        }

        if (lifecycle.state == ServiceState::RUNNING && deadline_reached(model.now_ms, lifecycle.state_deadline_ms)) {
            lifecycle.last_exit_kind = SupervisorExitKind::NONE;
            lifecycle.last_detail = ETIMEDOUT;
            record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::READINESS_TIMEOUT, ETIMEDOUT);
            begin_stop(model, service, SupervisorStopReason::READINESS_FAILURE, TransitionReason::READINESS_TIMEOUT, actions);
            continue;
        }

        if (lifecycle.state == ServiceState::STOPPING && deadline_reached(model.now_ms, lifecycle.state_deadline_ms)) {
            if (lifecycle.pid <= 0 || lifecycle.pgid <= 0) {
                if (!lifecycle.kill_sent) {
                    lifecycle.kill_sent = true;
                    lifecycle.state_deadline_ms = deadline_after(model.now_ms, spec.stop_kill_ms);
                } else if (!lifecycle.drain_escalated) {
                    record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::STOP_TIMEOUT, ETIMEDOUT);
                    (void)emit_action(actions, SupervisorActionKind::CLOSE_OUTPUT, service, lifecycle.generation);
                    lifecycle.drain_escalated = true;
                    lifecycle.state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                    lifecycle.drain_deadline_ms = deadline_after(model.now_ms, spec.drain_timeout_ms);
                }
            } else if (!lifecycle.kill_sent) {
                uint64_t const DEADLINE = deadline_after(model.now_ms, spec.stop_kill_ms);
                (void)emit_action(actions, SupervisorActionKind::SEND_KILL, service, lifecycle.generation, DEADLINE);
                lifecycle.kill_sent = true;
                lifecycle.state_deadline_ms = DEADLINE;
            } else if (!lifecycle.drain_escalated) {
                record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::STOP_TIMEOUT, ETIMEDOUT);
                (void)emit_action(actions, SupervisorActionKind::CLOSE_OUTPUT, service, lifecycle.generation);
                lifecycle.drain_escalated = true;
                lifecycle.state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle.drain_deadline_ms = deadline_after(model.now_ms, spec.drain_timeout_ms);
            }
        }

        if (!lifecycle.quiesced && lifecycle.drain_escalated && deadline_reached(model.now_ms, lifecycle.drain_deadline_ms)) {
            lifecycle.drain_deadline_ms = SUPERVISOR_NO_DEADLINE;
            lifecycle.desired_running = false;
            lifecycle.restart_scheduled = false;
            lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
            lifecycle.abandoned = true;
            transition_service(model, service, ServiceState::FAILED, TransitionReason::DRAIN_TIMEOUT, ETIMEDOUT);
        }
    }
}

[[nodiscard]] auto can_stop_shutdown_service(const SupervisorModel& model, uint8_t service) -> bool {
    uint16_t dependents = model.manifest->services.at(service).dependent_mask;
    while (dependents != 0) {
        uint8_t dependent = 0;
        while ((dependents & static_cast<uint16_t>(1U << dependent)) == 0) {
            dependent++;
        }
        dependents &= static_cast<uint16_t>(~static_cast<uint16_t>(1U << dependent));
        if (dependent < model.service_count && dependent != model.topology->journal_service &&
            !shutdown_settled(model.services.at(dependent))) {
            return false;
        }
    }
    return true;
}

void settle_unstarted_for_shutdown(SupervisorModel& model, uint8_t service) {
    auto& lifecycle = model.services.at(service);
    lifecycle.desired_running = false;
    lifecycle.restart_scheduled = false;
    lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
    lifecycle.completion_success = false;
    transition_service(model, service, ServiceState::EXITED, TransitionReason::SHUTDOWN);
}

void process_shutdown(SupervisorModel& model, SupervisorActionList& actions) {
    if (model.shutdown_phase == SupervisorShutdownPhase::SERVICES) {
        for (size_t order_index = 0; order_index < model.service_count; order_index++) {
            uint8_t const SERVICE = model.topology->stop_order.at(order_index);
            if (SERVICE >= model.service_count || SERVICE == model.topology->journal_service ||
                shutdown_settled(model.services.at(SERVICE)) || !can_stop_shutdown_service(model, SERVICE)) {
                continue;
            }
            auto& lifecycle = model.services.at(SERVICE);
            if ((lifecycle.state == ServiceState::WAITING || lifecycle.state == ServiceState::BACKOFF) && lifecycle.quiesced) {
                settle_unstarted_for_shutdown(model, SERVICE);
            } else {
                begin_stop(model, SERVICE, SupervisorStopReason::SHUTDOWN, TransitionReason::SHUTDOWN, actions);
            }
        }

        bool all_settled = true;
        for (uint8_t service = 0; service < model.service_count; service++) {
            if (service != model.topology->journal_service && !shutdown_settled(model.services.at(service))) {
                all_settled = false;
                break;
            }
        }
        if (all_settled) {
            model.shutdown_phase = SupervisorShutdownPhase::WAITING_FOR_JOURNAL;
            if (!model.non_journal_completion_emitted) {
                (void)emit_action(actions, SupervisorActionKind::NON_JOURNAL_SHUTDOWN_COMPLETE, INVALID_SERVICE_ID, 0);
                model.non_journal_completion_emitted = true;
            }
        }
    }

    if (model.shutdown_phase != SupervisorShutdownPhase::JOURNAL) {
        return;
    }

    uint8_t const JOURNAL = model.topology->journal_service;
    if (JOURNAL != INVALID_SERVICE_ID && JOURNAL < model.service_count && !shutdown_settled(model.services.at(JOURNAL))) {
        auto& lifecycle = model.services.at(JOURNAL);
        if ((lifecycle.state == ServiceState::WAITING || lifecycle.state == ServiceState::BACKOFF) && lifecycle.quiesced) {
            settle_unstarted_for_shutdown(model, JOURNAL);
        } else {
            begin_stop(model, JOURNAL, SupervisorStopReason::SHUTDOWN, TransitionReason::SHUTDOWN, actions);
        }
    }

    if (JOURNAL == INVALID_SERVICE_ID || JOURNAL >= model.service_count || shutdown_settled(model.services.at(JOURNAL))) {
        model.shutdown_phase = SupervisorShutdownPhase::COMPLETE;
        if (!model.shutdown_completion_emitted) {
            (void)emit_action(actions, SupervisorActionKind::SHUTDOWN_COMPLETE, INVALID_SERVICE_ID, 0);
            model.shutdown_completion_emitted = true;
        }
    }
}

void issue_spawns(SupervisorModel& model, SupervisorActionList& actions) {
    if (model.shutdown_phase != SupervisorShutdownPhase::NONE) {
        return;
    }
    for (size_t order_index = 0; order_index < model.service_count; order_index++) {
        uint8_t const SERVICE = model.topology->start_order.at(order_index);
        if (SERVICE >= model.service_count) {
            continue;
        }
        auto& lifecycle = model.services.at(SERVICE);
        if (lifecycle.state != ServiceState::WAITING || (!lifecycle.condition_met && !lifecycle.manual_enable_override) ||
            !lifecycle.desired_running || !lifecycle.quiesced || !dependencies_satisfied(model, SERVICE)) {
            continue;
        }
        if (lifecycle.generation == std::numeric_limits<uint32_t>::max()) {
            lifecycle.desired_running = false;
            lifecycle.last_detail = EOVERFLOW;
            transition_service(model, SERVICE, ServiceState::FAILED, TransitionReason::SPAWN_FAILED, EOVERFLOW);
            continue;
        }

        lifecycle.generation++;
        if (lifecycle.spawn_attempts != std::numeric_limits<uint64_t>::max()) {
            lifecycle.spawn_attempts++;
        }
        lifecycle.pid = -1;
        lifecycle.pgid = -1;
        lifecycle.process_present = false;
        lifecycle.spawn_result_pending = true;
        lifecycle.exec_result_pending = false;
        lifecycle.probe_active = false;
        lifecycle.completion_success = false;
        lifecycle.quiesced = false;
        lifecycle.resume_after_quiesce = false;
        lifecycle.stable_reset_done = false;
        lifecycle.term_sent = false;
        lifecycle.kill_sent = false;
        lifecycle.drain_escalated = false;
        lifecycle.abandoned = false;
        lifecycle.stop_reason = SupervisorStopReason::NONE;
        lifecycle.last_exit_kind = SupervisorExitKind::NONE;
        lifecycle.last_detail = 0;
        lifecycle.ready_since_ms = SUPERVISOR_NO_DEADLINE;
        lifecycle.drain_deadline_ms = SUPERVISOR_NO_DEADLINE;
        lifecycle.state_deadline_ms = optional_deadline_after(model.now_ms, model.manifest->services.at(SERVICE).readiness_timeout_ms);
        transition_service(model, SERVICE, ServiceState::STARTING, TransitionReason::DEPENDENCIES_READY);
        (void)emit_action(actions, SupervisorActionKind::SPAWN, SERVICE, lifecycle.generation, lifecycle.state_deadline_ms);
    }
}

void reconcile(SupervisorModel& model, SupervisorActionList& actions) {
    process_service_deadlines(model, actions);
    for (uint8_t pass = 0; pass < model.service_count; pass++) {
        if (!cascade_dependency_loss(model, actions)) {
            break;
        }
    }
    process_shutdown(model, actions);
    issue_spawns(model, actions);
    process_shutdown(model, actions);
}

[[nodiscard]] auto event_service(SupervisorModel& model, const SupervisorEvent& event, ServiceLifecycle*& lifecycle) -> SupervisorResult {
    if (event.service >= model.service_count) {
        return SupervisorResult::INVALID_SERVICE;
    }
    lifecycle = &model.services.at(event.service);
    if (event.generation == 0 || event.generation != lifecycle->generation) {
        return SupervisorResult::STALE_GENERATION;
    }
    return SupervisorResult::OK;
}

void handle_process_exit(SupervisorModel& model, const SupervisorEvent& event, SupervisorActionList& actions) {
    auto& lifecycle = model.services.at(event.service);
    ServiceState const PREVIOUS_STATE = lifecycle.state;
    lifecycle.process_present = false;
    lifecycle.spawn_result_pending = false;
    lifecycle.exec_result_pending = false;
    lifecycle.last_exit_kind = event.exit_kind;
    lifecycle.last_detail = event.detail;

    if (lifecycle.abandoned) {
        if (lifecycle.pgid > 0) {
            (void)emit_action(actions, SupervisorActionKind::SEND_KILL, event.service, lifecycle.generation,
                              deadline_after(model.now_ms, model.manifest->services.at(event.service).drain_timeout_ms));
        }
        return;
    }

    if (PREVIOUS_STATE == ServiceState::STOPPING || PREVIOUS_STATE == ServiceState::FAILED) {
        SupervisorStopReason const REASON =
            lifecycle.stop_reason == SupervisorStopReason::NONE ? SupervisorStopReason::START_FAILURE : lifecycle.stop_reason;
        begin_stop(model, event.service, REASON, TransitionReason::STOP_REQUESTED, actions);
        return;
    }
    if (PREVIOUS_STATE == ServiceState::STARTING) {
        begin_stop(model, event.service, SupervisorStopReason::START_FAILURE, TransitionReason::PROCESS_EXIT_FAILURE, actions);
        return;
    }
    begin_stop(model, event.service, SupervisorStopReason::READINESS_FAILURE,
               exit_succeeded(lifecycle) ? TransitionReason::PROCESS_EXIT_SUCCESS : TransitionReason::PROCESS_EXIT_FAILURE, actions);
}

}  // namespace

void SupervisorActionList::clear() {
    count = 0;
    overflowed = false;
}

auto SupervisorActionList::push(const SupervisorAction& action) -> bool {
    if (count >= entries.size()) {
        overflowed = true;
        return false;
    }
    entries.at(count) = action;
    count++;
    return true;
}

auto SupervisorActionList::size() const -> size_t { return count; }

auto SupervisorActionList::operator[](size_t index) const -> const SupervisorAction& { return entries.at(index); }

auto initialize_supervisor(SupervisorModel& model, const ServiceManifest& manifest, const ServiceTopology& topology,
                           std::span<const EnablementResult> enablement, uint64_t now_ms) -> SupervisorResult {
    model = SupervisorModel{};
    if (manifest.service_count > MAX_SERVICES || topology.service_count != manifest.service_count) {
        return SupervisorResult::INVALID_MODEL;
    }
    if (enablement.size() != manifest.service_count) {
        return SupervisorResult::INVALID_ENABLEMENT_COUNT;
    }

    model.manifest = &manifest;
    model.topology = &topology;
    model.service_count = manifest.service_count;
    model.now_ms = now_ms;
    model.initialized = true;

    for (uint8_t service = 0; service < model.service_count; service++) {
        auto& lifecycle = model.services.at(service);
        auto const& spec = manifest.services.at(service);
        lifecycle.state_since_ms = now_ms;
        lifecycle.condition_met = enablement[service] == EnablementResult::CONDITION_MET;
        if (lifecycle.condition_met) {
            lifecycle.desired_running = true;
            transition_service(model, service, ServiceState::WAITING, TransitionReason::INITIAL_ENABLED);
        } else if (spec.enablement == EnablementKind::PATH_MISSING && spec.type == ServiceType::ONESHOT) {
            lifecycle.completion_success = true;
            lifecycle.desired_running = false;
            transition_service(model, service, ServiceState::EXITED, TransitionReason::INITIAL_SKIPPED_SUCCESS);
        } else {
            lifecycle.desired_running = false;
            transition_service(model, service, ServiceState::DISABLED, TransitionReason::INITIAL_DISABLED);
        }
    }
    return SupervisorResult::OK;
}

auto supervisor_advance(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult {
    SupervisorResult const PREPARED = prepare_call(model, now_ms, actions);
    if (PREPARED != SupervisorResult::OK) {
        return PREPARED;
    }
    reconcile(model, actions);
    return finish_result(actions);
}

auto supervisor_handle_event(SupervisorModel& model, uint64_t now_ms, const SupervisorEvent& event, SupervisorActionList& actions)
    -> SupervisorResult {
    SupervisorResult const PREPARED = prepare_call(model, now_ms, actions);
    if (PREPARED != SupervisorResult::OK) {
        return PREPARED;
    }
    ServiceLifecycle* lifecycle = nullptr;
    SupervisorResult const SERVICE_RESULT = event_service(model, event, lifecycle);
    if (SERVICE_RESULT != SupervisorResult::OK) {
        return SERVICE_RESULT;
    }

    switch (event.kind) {
        case SupervisorEventKind::SPAWN_SUCCEEDED:
            if (!lifecycle->spawn_result_pending || event.pid <= 0 || event.pgid != event.pid) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->spawn_result_pending = false;
            lifecycle->exec_result_pending = true;
            lifecycle->process_present = true;
            lifecycle->pid = event.pid;
            lifecycle->pgid = event.pgid;
            if (lifecycle->abandoned) {
                (void)emit_action(actions, SupervisorActionKind::SEND_KILL, event.service, lifecycle->generation,
                                  deadline_after(now_ms, model.manifest->services.at(event.service).drain_timeout_ms));
                break;
            }
            if (lifecycle->state == ServiceState::STOPPING) {
                lifecycle->term_sent = false;
                lifecycle->kill_sent = false;
                lifecycle->drain_escalated = false;
                lifecycle->state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->drain_deadline_ms = SUPERVISOR_NO_DEADLINE;
                begin_stop(model, event.service, lifecycle->stop_reason, TransitionReason::STOP_REQUESTED, actions);
            }
            break;

        case SupervisorEventKind::SPAWN_FAILED:
            if (!lifecycle->spawn_result_pending) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->spawn_result_pending = false;
            lifecycle->process_present = false;
            lifecycle->quiesced = true;
            lifecycle->pid = -1;
            lifecycle->pgid = -1;
            lifecycle->last_exit_kind = SupervisorExitKind::NONE;
            lifecycle->last_detail = event.detail;
            if (lifecycle->state == ServiceState::STOPPING) {
                finalize_quiesced_stop(model, event.service);
            } else {
                transition_service(model, event.service, ServiceState::FAILED, TransitionReason::SPAWN_FAILED, event.detail);
                schedule_restart(model, event.service, true, event.detail);
            }
            break;

        case SupervisorEventKind::EXEC_SUCCEEDED:
            if (!lifecycle->exec_result_pending) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->exec_result_pending = false;
            if (lifecycle->state == ServiceState::STOPPING) {
                break;
            }
            if (lifecycle->state != ServiceState::STARTING) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->state_deadline_ms = optional_deadline_after(now_ms, model.manifest->services.at(event.service).readiness_timeout_ms);
            switch (model.manifest->services.at(event.service).readiness) {
                case ReadinessKind::IMMEDIATE:
                    lifecycle->ready_since_ms = now_ms;
                    lifecycle->state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                    transition_service(model, event.service, ServiceState::READY, TransitionReason::EXEC_SUCCEEDED);
                    break;
                case ReadinessKind::IPV4:
                    transition_service(model, event.service, ServiceState::RUNNING, TransitionReason::EXEC_SUCCEEDED);
                    lifecycle->probe_active = true;
                    (void)emit_action(actions, SupervisorActionKind::START_IPV4_PROBE, event.service, event.generation,
                                      lifecycle->state_deadline_ms);
                    break;
                case ReadinessKind::PROCESS_EXIT_SUCCESS:
                    transition_service(model, event.service, ServiceState::RUNNING, TransitionReason::EXEC_SUCCEEDED);
                    break;
            }
            break;

        case SupervisorEventKind::EXEC_FAILED:
            if (!lifecycle->exec_result_pending) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->exec_result_pending = false;
            lifecycle->last_exit_kind = SupervisorExitKind::NONE;
            lifecycle->last_detail = event.detail;
            if (lifecycle->state != ServiceState::STOPPING) {
                transition_service(model, event.service, ServiceState::FAILED, TransitionReason::EXEC_FAILED, event.detail);
                begin_stop(model, event.service, SupervisorStopReason::START_FAILURE, TransitionReason::EXEC_FAILED, actions);
            }
            break;

        case SupervisorEventKind::PROBE_PENDING:
            if (!lifecycle->probe_active || (lifecycle->state != ServiceState::RUNNING && lifecycle->state != ServiceState::READY)) {
                return SupervisorResult::INVALID_STATE;
            }
            if (lifecycle->state == ServiceState::READY) {
                lifecycle->ready_since_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->stable_reset_done = false;
                lifecycle->state_deadline_ms =
                    optional_deadline_after(now_ms, model.manifest->services.at(event.service).readiness_timeout_ms);
                transition_service(model, event.service, ServiceState::RUNNING, TransitionReason::READINESS_LOST, event.detail);
            }
            break;

        case SupervisorEventKind::PROBE_READY:
            if (!lifecycle->probe_active || (lifecycle->state != ServiceState::RUNNING && lifecycle->state != ServiceState::READY)) {
                return SupervisorResult::INVALID_STATE;
            }
            if (lifecycle->state == ServiceState::RUNNING) {
                lifecycle->ready_since_ms = now_ms;
                lifecycle->state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->stable_reset_done = false;
                transition_service(model, event.service, ServiceState::READY, TransitionReason::READINESS_READY, event.detail);
            }
            break;

        case SupervisorEventKind::PROBE_ERROR:
            if (!lifecycle->probe_active || (lifecycle->state != ServiceState::RUNNING && lifecycle->state != ServiceState::READY)) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->last_detail = event.detail;
            if (lifecycle->state == ServiceState::READY) {
                lifecycle->ready_since_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->stable_reset_done = false;
                lifecycle->state_deadline_ms =
                    optional_deadline_after(now_ms, model.manifest->services.at(event.service).readiness_timeout_ms);
                transition_service(model, event.service, ServiceState::RUNNING, TransitionReason::READINESS_LOST, event.detail);
            }
            break;

        case SupervisorEventKind::EXITED:
            if (!lifecycle->process_present ||
                (event.exit_kind != SupervisorExitKind::EXIT_CODE && event.exit_kind != SupervisorExitKind::SIGNAL)) {
                return SupervisorResult::INVALID_STATE;
            }
            handle_process_exit(model, event, actions);
            break;

        case SupervisorEventKind::QUIESCED:
            if (lifecycle->quiesced || lifecycle->process_present || lifecycle->spawn_result_pending || lifecycle->exec_result_pending) {
                return SupervisorResult::INVALID_STATE;
            }
            lifecycle->quiesced = true;
            lifecycle->process_present = false;
            lifecycle->spawn_result_pending = false;
            lifecycle->exec_result_pending = false;
            stop_probe(model, event.service, actions);
            lifecycle->pid = -1;
            lifecycle->pgid = -1;
            record_transition(model, event.service, lifecycle->state, lifecycle->state, TransitionReason::QUIESCED, event.detail);
            if (lifecycle->abandoned) {
                lifecycle->state_deadline_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->drain_deadline_ms = SUPERVISOR_NO_DEADLINE;
                lifecycle->stop_reason = SupervisorStopReason::NONE;
                lifecycle->term_sent = false;
                lifecycle->kill_sent = false;
                lifecycle->drain_escalated = false;
                lifecycle->abandoned = false;
            } else if (lifecycle->state == ServiceState::STOPPING || lifecycle->state == ServiceState::FAILED) {
                finalize_quiesced_stop(model, event.service);
            }
            break;
    }

    reconcile(model, actions);
    return finish_result(actions);
}

auto supervisor_control(SupervisorModel& model, uint64_t now_ms, uint8_t service, SupervisorControl control, SupervisorActionList& actions)
    -> SupervisorResult {
    SupervisorResult const PREPARED = prepare_call(model, now_ms, actions);
    if (PREPARED != SupervisorResult::OK) {
        return PREPARED;
    }
    if (service >= model.service_count) {
        return SupervisorResult::INVALID_SERVICE;
    }
    if (model.shutdown_phase != SupervisorShutdownPhase::NONE) {
        return SupervisorResult::INVALID_STATE;
    }
    auto& lifecycle = model.services.at(service);

    if (control == SupervisorControl::STOP) {
        lifecycle.desired_running = false;
        lifecycle.restart_scheduled = false;
        lifecycle.restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        lifecycle.completion_success = false;
        if (lifecycle.state == ServiceState::WAITING || lifecycle.state == ServiceState::BACKOFF) {
            transition_service(model, service, ServiceState::EXITED, TransitionReason::STOP_REQUESTED);
        } else if (lifecycle.state == ServiceState::STARTING || lifecycle.state == ServiceState::RUNNING ||
                   lifecycle.state == ServiceState::READY || lifecycle.state == ServiceState::STOPPING || !lifecycle.quiesced) {
            begin_stop(model, service, SupervisorStopReason::CONTROL_STOP, TransitionReason::STOP_REQUESTED, actions);
        } else if (lifecycle.state != ServiceState::DISABLED) {
            record_transition(model, service, lifecycle.state, lifecycle.state, TransitionReason::STOP_REQUESTED, 0);
        }
    } else {
        auto const& spec = model.manifest->services.at(service);
        bool const MANUAL_DISABLED_START =
            control == SupervisorControl::START && spec.enablement == EnablementKind::DISABLED && lifecycle.state == ServiceState::DISABLED;
        bool const MANUAL_ENABLEMENT = lifecycle.manual_enable_override || MANUAL_DISABLED_START;
        if (lifecycle.abandoned || (!lifecycle.condition_met && !MANUAL_ENABLEMENT) ||
            (lifecycle.state == ServiceState::DISABLED && !MANUAL_DISABLED_START)) {
            return SupervisorResult::INVALID_STATE;
        }
        if (MANUAL_DISABLED_START) {
            lifecycle.manual_enable_override = true;
        }
        lifecycle.desired_running = true;
        lifecycle.completion_success = false;
        reset_restart_tracking(lifecycle);
        bool const RESTART_ACTIVE = control == SupervisorControl::RESTART &&
                                    (lifecycle.state == ServiceState::STARTING || lifecycle.state == ServiceState::RUNNING ||
                                     lifecycle.state == ServiceState::READY || !lifecycle.quiesced);
        if (lifecycle.state == ServiceState::STOPPING || RESTART_ACTIVE) {
            begin_stop(model, service, SupervisorStopReason::CONTROL_RESTART, TransitionReason::CONTROL_RESTART, actions);
        } else if (lifecycle.state == ServiceState::DISABLED || lifecycle.state == ServiceState::EXITED ||
                   lifecycle.state == ServiceState::FAILED || lifecycle.state == ServiceState::BACKOFF) {
            transition_service(model, service, ServiceState::WAITING,
                               control == SupervisorControl::RESTART ? TransitionReason::CONTROL_RESTART : TransitionReason::CONTROL_START);
        } else {
            record_transition(model, service, lifecycle.state, lifecycle.state,
                              control == SupervisorControl::RESTART ? TransitionReason::CONTROL_RESTART : TransitionReason::CONTROL_START,
                              0);
        }
    }

    reconcile(model, actions);
    return finish_result(actions);
}

auto supervisor_begin_shutdown(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult {
    SupervisorResult const PREPARED = prepare_call(model, now_ms, actions);
    if (PREPARED != SupervisorResult::OK) {
        return PREPARED;
    }
    if (model.shutdown_phase != SupervisorShutdownPhase::NONE) {
        return SupervisorResult::INVALID_STATE;
    }
    model.shutdown_phase = SupervisorShutdownPhase::SERVICES;
    for (uint8_t service = 0; service < model.service_count; service++) {
        if (service != model.topology->journal_service) {
            model.services.at(service).desired_running = false;
            model.services.at(service).restart_scheduled = false;
            model.services.at(service).restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
        }
    }
    reconcile(model, actions);
    return finish_result(actions);
}

auto supervisor_begin_journal_shutdown(SupervisorModel& model, uint64_t now_ms, SupervisorActionList& actions) -> SupervisorResult {
    SupervisorResult const PREPARED = prepare_call(model, now_ms, actions);
    if (PREPARED != SupervisorResult::OK) {
        return PREPARED;
    }
    if (model.shutdown_phase != SupervisorShutdownPhase::WAITING_FOR_JOURNAL) {
        return SupervisorResult::INVALID_STATE;
    }
    model.shutdown_phase = SupervisorShutdownPhase::JOURNAL;
    uint8_t const JOURNAL = model.topology->journal_service;
    if (JOURNAL != INVALID_SERVICE_ID && JOURNAL < model.service_count) {
        model.services.at(JOURNAL).desired_running = false;
        model.services.at(JOURNAL).restart_scheduled = false;
        model.services.at(JOURNAL).restart_deadline_ms = SUPERVISOR_NO_DEADLINE;
    }
    reconcile(model, actions);
    return finish_result(actions);
}

auto supervisor_dependencies_satisfied(const SupervisorModel& model, uint8_t service) -> bool {
    return valid_model(model) && dependencies_satisfied(model, service);
}

auto supervisor_service(const SupervisorModel& model, uint8_t service) -> const ServiceLifecycle* {
    if (!valid_model(model) || service >= model.service_count) {
        return nullptr;
    }
    return &model.services.at(service);
}

auto supervisor_history_size(const SupervisorModel& model) -> size_t { return valid_model(model) ? model.history_count : 0; }

auto supervisor_history_at(const SupervisorModel& model, size_t oldest_first_index) -> const SupervisorTransition* {
    if (!valid_model(model) || oldest_first_index >= model.history_count) {
        return nullptr;
    }
    size_t const INDEX = (static_cast<size_t>(model.history_start) + oldest_first_index) % SUPERVISOR_HISTORY_CAPACITY;
    return &model.history.at(INDEX);
}

auto supervisor_shutdown_complete(const SupervisorModel& model) -> bool {
    return valid_model(model) && model.shutdown_phase == SupervisorShutdownPhase::COMPLETE;
}

}  // namespace wos::init
