#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::model {

// This model is deliberately independent of Task, RunQueue, APIC, and HPET.
// It is shared by host tests and freestanding KTEST, uses no dynamic storage,
// and validates the complete state after every modeled transition.
constexpr uint8_t CPU_COUNT = 2;
constexpr uint8_t TASK_COUNT = 3;
constexpr uint8_t INVALID_TASK = UINT8_MAX;
constexpr uint8_t INVALID_CPU = UINT8_MAX;
constexpr uint32_t MAX_REPLAY_STEPS = 16;
constexpr uint32_t MAX_EXPLORATION_STATES = 4096;
constexpr uint32_t MAX_EXPLORATION_TRACES = 1024;
constexpr uint32_t EXPLORATION_DEPTH = 6;
constexpr uint32_t MAX_PREEMPT_DEPTH = 64;
constexpr uint64_t DEFAULT_REPLAY_SEED = 0x574f535343484544ULL;

enum class StableMembership : uint8_t { DETACHED, RUNNABLE, WAITING, DEAD, RECLAIMED };
enum class Lifecycle : uint8_t { ACTIVE, EXITING, DEAD, RECLAIMED };
enum class TimerDecision : uint8_t { NONE, WAKE_DEADLINE, DEFER_PREEMPT, REQUEST_RESCHEDULE };

enum class EventKind : uint8_t {
    ADVANCE_TIME,
    ARM_TIMER,
    DELIVER_TIMER,
    DELIVER_IPI,
    WAKE,
    PARK,
    REQUEST_RESCHEDULE,
    APPLY_RESCHEDULE,
    RESERVE_HANDOFF,
    COMMIT_HANDOFF,
    MIGRATE,
    SET_PIN,
    SET_DOMAIN,
    PREEMPT_DISABLE,
    PREEMPT_ENABLE,
    EXIT_BEGIN,
    ENQUEUE_DEAD,
    ADVANCE_EPOCH,
    DROP_REFERENCE,
    RECLAIM,
};

enum class Rejection : uint8_t {
    NONE,
    INVALID_EVENT,
    WAKE_BEFORE_PARK,
    DUPLICATE_RESCHEDULE,
    PINNED,
    DOMAIN,
    TIMER_NOT_DUE,
    NO_PENDING_IPI,
    PREEMPT_UNDERFLOW,
    EPOCH_NOT_SAFE,
    REFERENCES_REMAIN,
};

enum class Invariant : uint64_t {
    NONE = 0,
    QUEUE_UNIQUENESS = 1ULL << 0,
    OWNER_CPU = 1ULL << 1,
    CURRENT_HANDOFF_EXCLUSIVITY = 1ULL << 2,
    HEAP_INDEX = 1ULL << 3,
    PUBLICATION = 1ULL << 4,
    WAKE_TOKEN = 1ULL << 5,
    TASK_STATE = 1ULL << 6,
    PREEMPT_DEPTH = 1ULL << 7,
    LIFETIME_EPOCH = 1ULL << 8,
    TIMER_IPI = 1ULL << 9,
};

[[nodiscard]] constexpr auto invariant_bit(Invariant invariant) -> uint64_t { return static_cast<uint64_t>(invariant); }

constexpr uint64_t REQUIRED_INVARIANTS = invariant_bit(Invariant::QUEUE_UNIQUENESS) | invariant_bit(Invariant::OWNER_CPU) |
                                         invariant_bit(Invariant::CURRENT_HANDOFF_EXCLUSIVITY) | invariant_bit(Invariant::HEAP_INDEX) |
                                         invariant_bit(Invariant::PUBLICATION) | invariant_bit(Invariant::WAKE_TOKEN) |
                                         invariant_bit(Invariant::TASK_STATE) | invariant_bit(Invariant::PREEMPT_DEPTH) |
                                         invariant_bit(Invariant::LIFETIME_EPOCH) | invariant_bit(Invariant::TIMER_IPI);

enum class Scenario : uint8_t {
    EVENT_BEFORE_PARK,
    EVENT_AFTER_PARK,
    DUPLICATE_RESCHEDULE,
    HANDOFF_WAKE,
    LOCAL_MIGRATION,
    TWO_CPU_MIGRATION,
    PIN_REJECTION,
    DOMAIN_REJECTION,
    PREEMPT_PENDING,
    EXIT_GC,
};

struct Event {
    EventKind kind{EventKind::ADVANCE_TIME};
    uint8_t task{INVALID_TASK};
    uint8_t cpu{INVALID_CPU};
    uint8_t target_cpu{INVALID_CPU};
    uint64_t value{};
};

struct ReplayStep {
    Event event{};
    uint64_t now{};
    uint64_t violations{};
    Rejection rejection{Rejection::NONE};
};

struct ModelTask {
    StableMembership membership{StableMembership::DETACHED};
    Lifecycle lifecycle{Lifecycle::ACTIVE};
    uint8_t owner_cpu{};
    int8_t heap_index{-1};
    bool published{};
    bool wake_pending{};
    bool reschedule_leader{};
    bool reschedule_pending{};
    uint8_t requested_cpu{INVALID_CPU};
    uint32_t preempt_depth{};
    bool preempt_pending{};
    bool pinned{};
    uint8_t pinned_cpu{INVALID_CPU};
    uint8_t domain_mask{(1U << CPU_COUNT) - 1U};
    uint64_t death_epoch{};
    uint32_t references{1};
};

struct ModelCpu {
    std::array<uint8_t, TASK_COUNT> heap{INVALID_TASK, INVALID_TASK, INVALID_TASK};
    uint8_t heap_size{};
    uint8_t current{INVALID_TASK};
    uint8_t handoff{INVALID_TASK};
    uint64_t timer_deadline{};
    bool timer_pending{};
    TimerDecision timer_decision{TimerDecision::NONE};
    bool ipi_pending{};
    uint32_t ipis_sent{};
    uint32_t ipis_delivered{};
};

struct EventResult {
    bool applied{};
    Rejection rejection{Rejection::NONE};
    uint64_t violations{};
};

struct ScenarioResult {
    bool passed{};
    uint64_t violations{};
    uint32_t steps{};
    uint32_t rejected{};
    uint32_t ipis_sent{};
    uint32_t ipis_delivered{};
};

struct ExplorationResult {
    bool passed{};
    uint32_t states{};
    uint32_t traces{};
    uint32_t max_depth{};
    uint64_t replay_seed{};
    uint64_t violations{};
};

struct Model {
    std::array<ModelTask, TASK_COUNT> tasks{};
    std::array<ModelCpu, CPU_COUNT> cpus{};
    std::array<ReplayStep, MAX_REPLAY_STEPS> replay{};
    uint64_t now{};
    uint64_t epoch{};
    uint64_t violations{};
    uint32_t steps{};
    uint32_t rejected{};
    uint32_t replay_count{};
    bool replay_truncated{};
};

[[nodiscard]] constexpr auto valid_task(uint8_t task) -> bool { return task < TASK_COUNT; }
[[nodiscard]] constexpr auto valid_cpu(uint8_t cpu) -> bool { return cpu < CPU_COUNT; }

constexpr void clear_heap(ModelCpu& cpu) {
    cpu.heap_size = 0;
    for (auto& entry : cpu.heap) {
        entry = INVALID_TASK;
    }
}

[[nodiscard]] constexpr auto heap_contains(Model const& model, uint8_t cpu, uint8_t task) -> bool {
    if (!valid_cpu(cpu) || !valid_task(task)) {
        return false;
    }
    auto const& queue = model.cpus[cpu];
    for (uint8_t index = 0; index < queue.heap_size && index < TASK_COUNT; ++index) {
        if (queue.heap[index] == task) {
            return true;
        }
    }
    return false;
}

constexpr void detach_runnable(Model& model, uint8_t task) {
    if (!valid_task(task)) {
        return;
    }
    auto& state = model.tasks[task];
    if (state.membership != StableMembership::RUNNABLE || !valid_cpu(state.owner_cpu)) {
        state.heap_index = -1;
        return;
    }
    auto& queue = model.cpus[state.owner_cpu];
    uint8_t index = TASK_COUNT;
    for (uint8_t candidate = 0; candidate < queue.heap_size && candidate < TASK_COUNT; ++candidate) {
        if (queue.heap[candidate] == task) {
            index = candidate;
            break;
        }
    }
    if (index < queue.heap_size) {
        for (uint8_t candidate = index; candidate + 1 < queue.heap_size; ++candidate) {
            queue.heap[candidate] = queue.heap[candidate + 1];
            if (valid_task(queue.heap[candidate])) {
                model.tasks[queue.heap[candidate]].heap_index = static_cast<int8_t>(candidate);
            }
        }
        --queue.heap_size;
        queue.heap[queue.heap_size] = INVALID_TASK;
    }
    state.heap_index = -1;
    state.membership = StableMembership::DETACHED;
}

[[nodiscard]] constexpr auto attach_runnable(Model& model, uint8_t task, uint8_t cpu) -> bool {
    if (!valid_task(task) || !valid_cpu(cpu)) {
        return false;
    }
    auto& state = model.tasks[task];
    if (state.membership == StableMembership::RUNNABLE) {
        detach_runnable(model, task);
    }
    auto& queue = model.cpus[cpu];
    if (queue.heap_size >= TASK_COUNT) {
        return false;
    }
    state.owner_cpu = cpu;
    state.membership = StableMembership::RUNNABLE;
    state.heap_index = static_cast<int8_t>(queue.heap_size);
    state.published = true;
    queue.heap[queue.heap_size++] = task;
    return true;
}

[[nodiscard]] constexpr auto overlay_cpu(Model const& model, uint8_t task) -> uint8_t {
    for (uint8_t cpu = 0; cpu < CPU_COUNT; ++cpu) {
        if (model.cpus[cpu].current == task || model.cpus[cpu].handoff == task) {
            return cpu;
        }
    }
    return INVALID_CPU;
}

[[nodiscard]] constexpr auto validate(Model const& model) -> uint64_t {
    uint64_t violations = 0;
    std::array<uint8_t, TASK_COUNT> heap_occurrences{};
    std::array<uint8_t, TASK_COUNT> reservation_cpus{};
    for (auto& cpu : reservation_cpus) {
        cpu = INVALID_CPU;
    }

    for (uint8_t cpu = 0; cpu < CPU_COUNT; ++cpu) {
        auto const& queue = model.cpus[cpu];
        if (queue.heap_size > TASK_COUNT) {
            violations |= invariant_bit(Invariant::QUEUE_UNIQUENESS);
        }
        for (uint8_t index = 0; index < queue.heap_size && index < TASK_COUNT; ++index) {
            uint8_t const task = queue.heap[index];
            if (!valid_task(task)) {
                violations |= invariant_bit(Invariant::QUEUE_UNIQUENESS);
                continue;
            }
            ++heap_occurrences[task];
            auto const& state = model.tasks[task];
            if (state.membership != StableMembership::RUNNABLE || state.owner_cpu != cpu) {
                violations |= invariant_bit(Invariant::OWNER_CPU) | invariant_bit(Invariant::QUEUE_UNIQUENESS);
            }
            if (state.heap_index != static_cast<int8_t>(index)) {
                violations |= invariant_bit(Invariant::HEAP_INDEX);
            }
            if (!state.published) {
                violations |= invariant_bit(Invariant::PUBLICATION);
            }
            if (state.lifecycle != Lifecycle::ACTIVE) {
                violations |= invariant_bit(Invariant::TASK_STATE);
            }
        }

        for (uint8_t reserved : {queue.current, queue.handoff}) {
            if (reserved == INVALID_TASK) {
                continue;
            }
            if (!valid_task(reserved)) {
                violations |= invariant_bit(Invariant::CURRENT_HANDOFF_EXCLUSIVITY);
                continue;
            }
            auto const& state = model.tasks[reserved];
            if (state.owner_cpu != cpu) {
                violations |= invariant_bit(Invariant::OWNER_CPU);
            }
            if (reservation_cpus[reserved] != INVALID_CPU && reservation_cpus[reserved] != cpu) {
                violations |= invariant_bit(Invariant::CURRENT_HANDOFF_EXCLUSIVITY);
            }
            reservation_cpus[reserved] = cpu;
            if (reserved == queue.handoff && state.lifecycle != Lifecycle::ACTIVE) {
                violations |= invariant_bit(Invariant::TASK_STATE);
            }
        }
        if (queue.ipis_delivered > queue.ipis_sent || (queue.ipi_pending && queue.ipis_sent == queue.ipis_delivered)) {
            violations |= invariant_bit(Invariant::TIMER_IPI);
        }
        if (queue.timer_pending && queue.timer_decision == TimerDecision::NONE) {
            violations |= invariant_bit(Invariant::TIMER_IPI);
        }
    }

    for (uint8_t task = 0; task < TASK_COUNT; ++task) {
        auto const& state = model.tasks[task];
        if ((state.membership == StableMembership::RUNNABLE) != (heap_occurrences[task] == 1)) {
            violations |= invariant_bit(Invariant::QUEUE_UNIQUENESS);
        }
        if (heap_occurrences[task] > 1) {
            violations |= invariant_bit(Invariant::QUEUE_UNIQUENESS);
        }
        if (state.membership != StableMembership::RUNNABLE && state.heap_index != -1) {
            violations |= invariant_bit(Invariant::HEAP_INDEX);
        }
        if (state.membership != StableMembership::DETACHED && state.membership != StableMembership::RECLAIMED && !state.published) {
            violations |= invariant_bit(Invariant::PUBLICATION);
        }
        if (state.wake_pending && state.lifecycle != Lifecycle::ACTIVE) {
            violations |= invariant_bit(Invariant::WAKE_TOKEN);
        }
        if (state.preempt_depth > MAX_PREEMPT_DEPTH || (state.preempt_pending && state.preempt_depth == 0)) {
            violations |= invariant_bit(Invariant::PREEMPT_DEPTH);
        }
        if (state.pinned && (!valid_cpu(state.pinned_cpu) || state.owner_cpu != state.pinned_cpu)) {
            violations |= invariant_bit(Invariant::OWNER_CPU);
        }
        if (valid_cpu(state.owner_cpu) && (state.domain_mask & (1U << state.owner_cpu)) == 0) {
            violations |= invariant_bit(Invariant::OWNER_CPU);
        }

        switch (state.membership) {
            case StableMembership::RUNNABLE:
            case StableMembership::WAITING:
                if (state.lifecycle != Lifecycle::ACTIVE) {
                    violations |= invariant_bit(Invariant::TASK_STATE);
                }
                break;
            case StableMembership::DEAD:
                if (state.lifecycle != Lifecycle::DEAD || state.death_epoch > model.epoch || state.references == 0) {
                    violations |= invariant_bit(Invariant::LIFETIME_EPOCH);
                }
                break;
            case StableMembership::RECLAIMED:
                if (state.lifecycle != Lifecycle::RECLAIMED || state.references != 0 || state.published) {
                    violations |= invariant_bit(Invariant::LIFETIME_EPOCH);
                }
                break;
            case StableMembership::DETACHED:
                if (state.lifecycle == Lifecycle::RECLAIMED || state.lifecycle == Lifecycle::DEAD) {
                    violations |= invariant_bit(Invariant::TASK_STATE);
                }
                break;
        }
        // EXITING may be detached after the assembly-delimited handoff has
        // stopped executing its stack and before the dead-list transaction.
    }
    return violations;
}

constexpr void record_step(Model& model, Event const& event, Rejection rejection, uint64_t violations) {
    if (model.replay_count < MAX_REPLAY_STEPS) {
        model.replay[model.replay_count++] = {.event = event, .now = model.now, .violations = violations, .rejection = rejection};
    } else {
        model.replay_truncated = true;
    }
}

constexpr void send_ipi(Model& model, uint8_t cpu) {
    if (!valid_cpu(cpu)) {
        return;
    }
    auto& target = model.cpus[cpu];
    if (!target.ipi_pending) {
        target.ipi_pending = true;
        ++target.ipis_sent;
    }
}

[[nodiscard]] constexpr auto reject(Model& model, Event const& event, Rejection rejection) -> EventResult {
    ++model.steps;
    ++model.rejected;
    uint64_t const violations = validate(model);
    model.violations |= violations;
    record_step(model, event, rejection, violations);
    return {.applied = false, .rejection = rejection, .violations = violations};
}

[[nodiscard]] constexpr auto finish(Model& model, Event const& event) -> EventResult {
    ++model.steps;
    uint64_t const violations = validate(model);
    model.violations |= violations;
    record_step(model, event, Rejection::NONE, violations);
    return {.applied = true, .rejection = Rejection::NONE, .violations = violations};
}

[[nodiscard]] constexpr auto apply(Model& model, Event const& event) -> EventResult {
    if ((event.task != INVALID_TASK && !valid_task(event.task)) || (event.cpu != INVALID_CPU && !valid_cpu(event.cpu)) ||
        (event.target_cpu != INVALID_CPU && !valid_cpu(event.target_cpu))) {
        return reject(model, event, Rejection::INVALID_EVENT);
    }

    switch (event.kind) {
        case EventKind::ADVANCE_TIME:
            if (event.value < model.now) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            model.now = event.value;
            break;
        case EventKind::ARM_TIMER: {
            if (!valid_cpu(event.cpu) || event.value < model.now) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& cpu = model.cpus[event.cpu];
            cpu.timer_deadline = event.value;
            cpu.timer_pending = true;
            cpu.timer_decision = TimerDecision::WAKE_DEADLINE;
            break;
        }
        case EventKind::DELIVER_TIMER: {
            if (!valid_cpu(event.cpu)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& cpu = model.cpus[event.cpu];
            if (!cpu.timer_pending || (cpu.timer_deadline != 0 && cpu.timer_deadline > model.now)) {
                return reject(model, event, Rejection::TIMER_NOT_DUE);
            }
            cpu.timer_pending = false;
            cpu.timer_deadline = 0;
            uint8_t const current = cpu.current;
            if (valid_task(current) && model.tasks[current].preempt_depth != 0 && cpu.heap_size > 1) {
                model.tasks[current].preempt_pending = true;
                cpu.timer_decision = TimerDecision::DEFER_PREEMPT;
            } else {
                cpu.timer_decision = TimerDecision::REQUEST_RESCHEDULE;
            }
            break;
        }
        case EventKind::DELIVER_IPI: {
            if (!valid_cpu(event.cpu) || !model.cpus[event.cpu].ipi_pending) {
                return reject(model, event, Rejection::NO_PENDING_IPI);
            }
            auto& cpu = model.cpus[event.cpu];
            cpu.ipi_pending = false;
            ++cpu.ipis_delivered;
            cpu.timer_pending = true;
            cpu.timer_deadline = model.now;
            cpu.timer_decision = TimerDecision::REQUEST_RESCHEDULE;
            break;
        }
        case EventKind::WAKE: {
            if (!valid_task(event.task) || model.tasks[event.task].lifecycle != Lifecycle::ACTIVE) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            uint8_t const reserved_cpu = overlay_cpu(model, event.task);
            if (task.membership == StableMembership::WAITING && valid_cpu(reserved_cpu) &&
                model.cpus[reserved_cpu].handoff != INVALID_TASK) {
                task.wake_pending = true;
            } else if (task.membership == StableMembership::WAITING) {
                if (!attach_runnable(model, event.task, task.owner_cpu)) {
                    return reject(model, event, Rejection::INVALID_EVENT);
                }
                task.wake_pending = false;
            } else {
                task.wake_pending = true;
            }
            break;
        }
        case EventKind::PARK: {
            if (!valid_task(event.task) || model.tasks[event.task].lifecycle != Lifecycle::ACTIVE) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            if (task.wake_pending) {
                task.wake_pending = false;
                return reject(model, event, Rejection::WAKE_BEFORE_PARK);
            }
            if (task.membership == StableMembership::RUNNABLE) {
                detach_runnable(model, event.task);
            }
            if (task.membership != StableMembership::DETACHED) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            task.membership = StableMembership::WAITING;
            task.heap_index = -1;
            break;
        }
        case EventKind::REQUEST_RESCHEDULE: {
            if (!valid_task(event.task) || !valid_cpu(event.target_cpu) || model.tasks[event.task].lifecycle != Lifecycle::ACTIVE) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            task.reschedule_pending = true;
            task.requested_cpu = event.target_cpu;
            if (task.reschedule_leader) {
                return reject(model, event, Rejection::DUPLICATE_RESCHEDULE);
            }
            task.reschedule_leader = true;
            break;
        }
        case EventKind::APPLY_RESCHEDULE: {
            if (!valid_task(event.task)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            if (!task.reschedule_leader || !task.reschedule_pending || !valid_cpu(task.requested_cpu)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            uint8_t const target = task.requested_cpu;
            if (task.pinned && task.pinned_cpu != target) {
                task.reschedule_leader = false;
                task.reschedule_pending = false;
                task.requested_cpu = INVALID_CPU;
                return reject(model, event, Rejection::PINNED);
            }
            if ((task.domain_mask & (1U << target)) == 0) {
                task.reschedule_leader = false;
                task.reschedule_pending = false;
                task.requested_cpu = INVALID_CPU;
                return reject(model, event, Rejection::DOMAIN);
            }
            uint8_t const source = task.owner_cpu;
            if (source != target) {
                if (!attach_runnable(model, event.task, target)) {
                    return reject(model, event, Rejection::INVALID_EVENT);
                }
                send_ipi(model, target);
            }
            task.reschedule_leader = false;
            task.reschedule_pending = false;
            task.requested_cpu = INVALID_CPU;
            break;
        }
        case EventKind::RESERVE_HANDOFF: {
            if (!valid_cpu(event.cpu) || !valid_task(event.task)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            auto& cpu = model.cpus[event.cpu];
            if (task.lifecycle != Lifecycle::ACTIVE || task.owner_cpu != event.cpu || cpu.handoff != INVALID_TASK) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            cpu.handoff = event.task;
            break;
        }
        case EventKind::COMMIT_HANDOFF: {
            if (!valid_cpu(event.cpu) || !valid_task(model.cpus[event.cpu].handoff)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& cpu = model.cpus[event.cpu];
            uint8_t const outgoing = cpu.current;
            cpu.current = cpu.handoff;
            cpu.handoff = INVALID_TASK;
            if (valid_task(outgoing)) {
                auto& task = model.tasks[outgoing];
                if (task.membership == StableMembership::WAITING && task.wake_pending) {
                    task.wake_pending = false;
                    if (!attach_runnable(model, outgoing, task.owner_cpu)) {
                        return reject(model, event, Rejection::INVALID_EVENT);
                    }
                }
            }
            break;
        }
        case EventKind::MIGRATE: {
            if (!valid_task(event.task) || !valid_cpu(event.target_cpu)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            if (task.lifecycle != Lifecycle::ACTIVE || task.membership != StableMembership::RUNNABLE) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            if (task.pinned && task.pinned_cpu != event.target_cpu) {
                return reject(model, event, Rejection::PINNED);
            }
            if ((task.domain_mask & (1U << event.target_cpu)) == 0) {
                return reject(model, event, Rejection::DOMAIN);
            }
            uint8_t const source = task.owner_cpu;
            if (source != event.target_cpu) {
                if (!attach_runnable(model, event.task, event.target_cpu)) {
                    return reject(model, event, Rejection::INVALID_EVENT);
                }
                send_ipi(model, event.target_cpu);
            }
            break;
        }
        case EventKind::SET_PIN: {
            if (!valid_task(event.task) || !valid_cpu(event.target_cpu)) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            task.pinned = true;
            task.pinned_cpu = event.target_cpu;
            if (task.owner_cpu != event.target_cpu) {
                return reject(model, event, Rejection::PINNED);
            }
            break;
        }
        case EventKind::SET_DOMAIN:
            if (!valid_task(event.task) || event.value == 0 || (event.value & ~((1U << CPU_COUNT) - 1U)) != 0) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            if ((event.value & (1U << model.tasks[event.task].owner_cpu)) == 0) {
                return reject(model, event, Rejection::DOMAIN);
            }
            model.tasks[event.task].domain_mask = static_cast<uint8_t>(event.value);
            break;
        case EventKind::PREEMPT_DISABLE:
            if (!valid_task(event.task) || model.tasks[event.task].preempt_depth >= MAX_PREEMPT_DEPTH) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            ++model.tasks[event.task].preempt_depth;
            break;
        case EventKind::PREEMPT_ENABLE: {
            if (!valid_task(event.task) || model.tasks[event.task].preempt_depth == 0) {
                return reject(model, event, Rejection::PREEMPT_UNDERFLOW);
            }
            auto& task = model.tasks[event.task];
            --task.preempt_depth;
            if (task.preempt_depth == 0 && task.preempt_pending) {
                task.preempt_pending = false;
                auto& owner = model.cpus[task.owner_cpu];
                owner.timer_pending = true;
                owner.timer_deadline = model.now;
                owner.timer_decision = TimerDecision::REQUEST_RESCHEDULE;
            }
            break;
        }
        case EventKind::EXIT_BEGIN: {
            if (!valid_task(event.task) || model.tasks[event.task].lifecycle != Lifecycle::ACTIVE) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            if (task.membership == StableMembership::RUNNABLE) {
                detach_runnable(model, event.task);
            } else if (task.membership == StableMembership::WAITING) {
                task.membership = StableMembership::DETACHED;
            }
            task.lifecycle = Lifecycle::EXITING;
            task.wake_pending = false;
            task.reschedule_leader = false;
            task.reschedule_pending = false;
            task.requested_cpu = INVALID_CPU;
            break;
        }
        case EventKind::ENQUEUE_DEAD: {
            if (!valid_task(event.task) || model.tasks[event.task].lifecycle != Lifecycle::EXITING ||
                overlay_cpu(model, event.task) != INVALID_CPU) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            task.lifecycle = Lifecycle::DEAD;
            task.membership = StableMembership::DEAD;
            task.death_epoch = model.epoch;
            task.heap_index = -1;
            break;
        }
        case EventKind::ADVANCE_EPOCH:
            model.epoch += event.value == 0 ? 1 : event.value;
            break;
        case EventKind::DROP_REFERENCE:
            if (!valid_task(event.task) || model.tasks[event.task].references <= 1) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            --model.tasks[event.task].references;
            break;
        case EventKind::RECLAIM: {
            if (!valid_task(event.task) || model.tasks[event.task].membership != StableMembership::DEAD) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            auto& task = model.tasks[event.task];
            if (model.epoch <= task.death_epoch) {
                return reject(model, event, Rejection::EPOCH_NOT_SAFE);
            }
            if (task.references != 1) {
                return reject(model, event, Rejection::REFERENCES_REMAIN);
            }
            if (overlay_cpu(model, event.task) != INVALID_CPU) {
                return reject(model, event, Rejection::INVALID_EVENT);
            }
            task.lifecycle = Lifecycle::RECLAIMED;
            task.membership = StableMembership::RECLAIMED;
            task.references = 0;
            task.published = false;
            break;
        }
    }
    return finish(model, event);
}

[[nodiscard]] constexpr auto base_model() -> Model {
    Model model{};
    for (auto& cpu : model.cpus) {
        clear_heap(cpu);
    }
    for (uint8_t task = 0; task < TASK_COUNT; ++task) {
        model.tasks[task].owner_cpu = task == 2 ? 1 : 0;
        model.tasks[task].domain_mask = (1U << CPU_COUNT) - 1U;
        model.tasks[task].references = 1;
        static_cast<void>(attach_runnable(model, task, model.tasks[task].owner_cpu));
    }
    model.cpus[0].current = 0;
    model.cpus[1].current = 2;
    return model;
}

[[nodiscard]] constexpr auto total_ipis_sent(Model const& model) -> uint32_t {
    uint32_t total = 0;
    for (auto const& cpu : model.cpus) {
        total += cpu.ipis_sent;
    }
    return total;
}

[[nodiscard]] constexpr auto total_ipis_delivered(Model const& model) -> uint32_t {
    uint32_t total = 0;
    for (auto const& cpu : model.cpus) {
        total += cpu.ipis_delivered;
    }
    return total;
}

[[nodiscard]] constexpr auto scenario_result(Model const& model, bool expected) -> ScenarioResult {
    return {
        .passed = expected && model.violations == 0 && validate(model) == 0,
        .violations = model.violations | validate(model),
        .steps = model.steps,
        .rejected = model.rejected,
        .ipis_sent = total_ipis_sent(model),
        .ipis_delivered = total_ipis_delivered(model),
    };
}

[[nodiscard]] constexpr auto run_scenario(Scenario scenario) -> ScenarioResult {
    Model model = base_model();
    switch (scenario) {
        case Scenario::EVENT_BEFORE_PARK:
            static_cast<void>(apply(model, {.kind = EventKind::WAKE, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::PARK, .task = 0}));
            return scenario_result(
                model, model.tasks[0].membership == StableMembership::RUNNABLE && !model.tasks[0].wake_pending && model.rejected == 1);
        case Scenario::EVENT_AFTER_PARK:
            static_cast<void>(apply(model, {.kind = EventKind::PARK, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::WAKE, .task = 0}));
            return scenario_result(model, model.tasks[0].membership == StableMembership::RUNNABLE && !model.tasks[0].wake_pending);
        case Scenario::DUPLICATE_RESCHEDULE:
            static_cast<void>(apply(model, {.kind = EventKind::REQUEST_RESCHEDULE, .task = 1, .target_cpu = 1}));
            static_cast<void>(apply(model, {.kind = EventKind::REQUEST_RESCHEDULE, .task = 1, .target_cpu = 1}));
            static_cast<void>(apply(model, {.kind = EventKind::APPLY_RESCHEDULE, .task = 1}));
            static_cast<void>(apply(model, {.kind = EventKind::DELIVER_IPI, .cpu = 1}));
            return scenario_result(model, model.tasks[1].owner_cpu == 1 && model.rejected == 1 && model.cpus[1].ipis_delivered == 1);
        case Scenario::HANDOFF_WAKE:
            static_cast<void>(apply(model, {.kind = EventKind::PARK, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::RESERVE_HANDOFF, .task = 1, .cpu = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::WAKE, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::COMMIT_HANDOFF, .cpu = 0}));
            return scenario_result(model, model.cpus[0].current == 1 && model.cpus[0].handoff == INVALID_TASK &&
                                              model.tasks[0].membership == StableMembership::RUNNABLE && !model.tasks[0].wake_pending);
        case Scenario::LOCAL_MIGRATION:
            static_cast<void>(apply(model, {.kind = EventKind::MIGRATE, .task = 1, .target_cpu = 0}));
            return scenario_result(model, model.tasks[1].owner_cpu == 0 && total_ipis_sent(model) == 0);
        case Scenario::TWO_CPU_MIGRATION:
            static_cast<void>(apply(model, {.kind = EventKind::MIGRATE, .task = 1, .target_cpu = 1}));
            static_cast<void>(apply(model, {.kind = EventKind::DELIVER_IPI, .cpu = 1}));
            return scenario_result(model, model.tasks[1].owner_cpu == 1 && model.cpus[1].ipis_delivered == 1);
        case Scenario::PIN_REJECTION:
            static_cast<void>(apply(model, {.kind = EventKind::SET_PIN, .task = 1, .target_cpu = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::MIGRATE, .task = 1, .target_cpu = 1}));
            return scenario_result(model, model.tasks[1].owner_cpu == 0 && model.rejected == 1);
        case Scenario::DOMAIN_REJECTION:
            static_cast<void>(apply(model, {.kind = EventKind::SET_DOMAIN, .task = 1, .value = 1U << 0}));
            static_cast<void>(apply(model, {.kind = EventKind::MIGRATE, .task = 1, .target_cpu = 1}));
            return scenario_result(model, model.tasks[1].owner_cpu == 0 && model.rejected == 1);
        case Scenario::PREEMPT_PENDING:
            static_cast<void>(apply(model, {.kind = EventKind::PREEMPT_DISABLE, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::ARM_TIMER, .cpu = 0, .value = 10}));
            static_cast<void>(apply(model, {.kind = EventKind::ADVANCE_TIME, .value = 10}));
            static_cast<void>(apply(model, {.kind = EventKind::DELIVER_TIMER, .cpu = 0}));
            if (!model.tasks[0].preempt_pending || model.cpus[0].timer_decision != TimerDecision::DEFER_PREEMPT) {
                return scenario_result(model, false);
            }
            static_cast<void>(apply(model, {.kind = EventKind::PREEMPT_ENABLE, .task = 0}));
            return scenario_result(model, !model.tasks[0].preempt_pending && model.tasks[0].preempt_depth == 0 &&
                                              model.cpus[0].timer_pending &&
                                              model.cpus[0].timer_decision == TimerDecision::REQUEST_RESCHEDULE);
        case Scenario::EXIT_GC:
            model.tasks[0].references = 2;
            static_cast<void>(apply(model, {.kind = EventKind::EXIT_BEGIN, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::RESERVE_HANDOFF, .task = 1, .cpu = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::COMMIT_HANDOFF, .cpu = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::ENQUEUE_DEAD, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::RECLAIM, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::ADVANCE_EPOCH, .value = 1}));
            static_cast<void>(apply(model, {.kind = EventKind::RECLAIM, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::DROP_REFERENCE, .task = 0}));
            static_cast<void>(apply(model, {.kind = EventKind::RECLAIM, .task = 0}));
            return scenario_result(model, model.tasks[0].lifecycle == Lifecycle::RECLAIMED &&
                                              model.tasks[0].membership == StableMembership::RECLAIMED && model.rejected == 2);
    }
    return scenario_result(model, false);
}

[[nodiscard]] constexpr auto exploration_event(uint32_t selector) -> Event {
    switch (selector & 3U) {
        case 0:
            return {.kind = EventKind::WAKE, .task = 0};
        case 1:
            return {.kind = EventKind::PARK, .task = 0};
        case 2:
            return {.kind = EventKind::REQUEST_RESCHEDULE, .task = 1, .target_cpu = 1};
        default:
            return {.kind = EventKind::APPLY_RESCHEDULE, .task = 1};
    }
}

[[nodiscard]] constexpr auto explore_bounded(uint64_t seed = DEFAULT_REPLAY_SEED) -> ExplorationResult {
    ExplorationResult result{.passed = true, .max_depth = EXPLORATION_DEPTH, .replay_seed = seed};
    uint64_t stream = seed;
    for (uint32_t trace = 0; trace < MAX_EXPLORATION_TRACES; ++trace) {
        if (result.states + EXPLORATION_DEPTH > MAX_EXPLORATION_STATES) {
            break;
        }
        Model model = base_model();
        uint32_t digits = trace;
        for (uint32_t depth = 0; depth < EXPLORATION_DEPTH; ++depth) {
            stream = stream * 6364136223846793005ULL + 1442695040888963407ULL;
            uint32_t const selector = (digits & 3U) ^ static_cast<uint32_t>((stream >> 62U) & 3U);
            digits >>= 2U;
            static_cast<void>(apply(model, exploration_event(selector)));
            ++result.states;
            if (model.violations != 0) {
                result.passed = false;
                result.violations = model.violations;
                result.replay_seed = seed ^ (static_cast<uint64_t>(trace) << 32U) ^ depth;
                return result;
            }
        }
        ++result.traces;
    }
    result.passed = result.passed && result.states <= MAX_EXPLORATION_STATES && result.traces <= MAX_EXPLORATION_TRACES &&
                    result.max_depth == EXPLORATION_DEPTH;
    return result;
}

[[nodiscard]] constexpr auto negative_invariant_coverage() -> uint64_t {
    uint64_t detected = 0;
    {
        Model model = base_model();
        model.cpus[1].heap[model.cpus[1].heap_size++] = 0;
        detected |= validate(model) & invariant_bit(Invariant::QUEUE_UNIQUENESS);
    }
    {
        Model model = base_model();
        model.tasks[1].owner_cpu = 1;
        detected |= validate(model) & invariant_bit(Invariant::OWNER_CPU);
    }
    {
        Model model = base_model();
        model.cpus[1].handoff = 0;
        detected |= validate(model) & invariant_bit(Invariant::CURRENT_HANDOFF_EXCLUSIVITY);
    }
    {
        Model model = base_model();
        model.tasks[1].heap_index = 2;
        detected |= validate(model) & invariant_bit(Invariant::HEAP_INDEX);
    }
    {
        Model model = base_model();
        model.tasks[1].published = false;
        detected |= validate(model) & invariant_bit(Invariant::PUBLICATION);
    }
    {
        Model model = base_model();
        model.tasks[1].lifecycle = Lifecycle::DEAD;
        model.tasks[1].wake_pending = true;
        detected |= validate(model) & invariant_bit(Invariant::WAKE_TOKEN);
        detected |= validate(model) & invariant_bit(Invariant::TASK_STATE);
    }
    {
        Model model = base_model();
        model.tasks[1].preempt_pending = true;
        detected |= validate(model) & invariant_bit(Invariant::PREEMPT_DEPTH);
    }
    {
        Model model = base_model();
        detach_runnable(model, 1);
        model.tasks[1].membership = StableMembership::DEAD;
        model.tasks[1].lifecycle = Lifecycle::DEAD;
        model.tasks[1].death_epoch = model.epoch + 1;
        detected |= validate(model) & invariant_bit(Invariant::LIFETIME_EPOCH);
    }
    {
        Model model = base_model();
        model.cpus[0].ipi_pending = true;
        detected |= validate(model) & invariant_bit(Invariant::TIMER_IPI);
    }
    return detected;
}

[[nodiscard]] constexpr auto negative_invariant_detection() -> bool {
    return (negative_invariant_coverage() & REQUIRED_INVARIANTS) == REQUIRED_INVARIANTS;
}

static_assert(run_scenario(Scenario::EVENT_BEFORE_PARK).passed);
static_assert(run_scenario(Scenario::EVENT_AFTER_PARK).passed);
static_assert(run_scenario(Scenario::DUPLICATE_RESCHEDULE).passed);
static_assert(run_scenario(Scenario::HANDOFF_WAKE).passed);
static_assert(run_scenario(Scenario::LOCAL_MIGRATION).passed);
static_assert(run_scenario(Scenario::TWO_CPU_MIGRATION).passed);
static_assert(run_scenario(Scenario::PIN_REJECTION).passed);
static_assert(run_scenario(Scenario::DOMAIN_REJECTION).passed);
static_assert(run_scenario(Scenario::PREEMPT_PENDING).passed);
static_assert(run_scenario(Scenario::EXIT_GC).passed);
static_assert(negative_invariant_detection());

}  // namespace ker::mod::sched::model
