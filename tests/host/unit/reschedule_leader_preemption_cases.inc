// Host transports for actual leadership/wrapper/PreemptGuard source. The timer
// decision uses the maintained policy; ptrace parking is an explicit endpoint,
// not an implementation of the native timer, APIC, wait list or context switch.
#include <platform/sched/preemption_policy.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string_view>
#include <vector>

namespace ker::mod::sched::task {
struct Task;
}
namespace fixture {
using ker::mod::sched::task::Task;
enum class Point { ATOMIC_BEFORE, ATOMIC_AFTER, DISPATCH, COMMIT, PUBLICATION_UNLOCK, NOTIFY, RELEASE };
enum class AtomicKind { REQUEST, PENDING, LEADER };
Task* current{};
Task* subject{};
unsigned baseline_depth{}, checks{}, timer_decisions{}, blocked_decisions{}, allowed_decisions{};
unsigned dispatches{}, commits{}, notifications{}, publication{}, publication_locks{}, publication_unlocks{};
unsigned lost_requests{}, request_budget{}, injection_depth{}, expected_core_count{4};
bool queue_present{true}, interrupts_enabled{true}, publication_may_sleep{}, place_runnable{true};
bool inactive{}, inject_request{}, atomic_release_request{};
int place_status{};
Point request_point{Point::DISPATCH};
uint64_t now{100};
std::vector<uint64_t> dispatched_cpus;
void boundary(Point point);
void atomic_boundary(AtomicKind kind, Task* task, bool before, bool changed, bool old, bool value);
void require(bool condition, const char* message) {
    ++checks;
    if (!condition) {
        std::fprintf(stderr, "leadership invariant: %s\n", message);
        std::abort();
    }
}
template <typename T>
struct Atomic {
    std::atomic<T> actual{};
    AtomicKind kind{};
    Task* task{};
    explicit Atomic(AtomicKind k, Task* t) : kind(k), task(t) {}
    T load(std::memory_order order = std::memory_order_seq_cst) const { return actual.load(order); }
    void store(T value, std::memory_order order = std::memory_order_seq_cst) {
        atomic_boundary(kind, task, true, false, false, bool(value));
        T old = actual.load(std::memory_order_relaxed);
        actual.store(value, order);
        atomic_boundary(kind, task, false, true, bool(old), bool(value));
    }
    T exchange(T value, std::memory_order order = std::memory_order_seq_cst) {
        atomic_boundary(kind, task, true, false, false, bool(value));
        T old = actual.exchange(value, order);
        atomic_boundary(kind, task, false, true, bool(old), bool(value));
        return old;
    }
    bool compare_exchange_strong(T& expected, T desired, std::memory_order success, std::memory_order failure) {
        atomic_boundary(kind, task, true, false, false, bool(desired));
        T old = expected;
        bool changed = actual.compare_exchange_strong(expected, desired, success, failure);
        atomic_boundary(kind, task, false, changed, bool(old), bool(desired));
        return changed;
    }
};
}
namespace ker::mod::sched {
namespace task {
enum class TaskType { PROCESS, DAEMON, IDLE };
struct Task {
    TaskType type{TaskType::PROCESS};
    fixture::Atomic<uint64_t> reschedule_requested_cpu{fixture::AtomicKind::REQUEST, this};
    fixture::Atomic<bool> reschedule_pending{fixture::AtomicKind::PENDING, this};
    fixture::Atomic<bool> reschedule_in_progress{fixture::AtomicKind::LEADER, this};
    Task* leader_executor{};  // Host-only observation of the real atomic election.
    uint32_t preempt_disable_depth{};
    uint64_t preempt_disable_owner{}, preempt_disable_start_us{}, preempt_disable_max_us{};
    bool preempt_pending{}, deferred_task_switch{}, ptrace_stopped{true};
    std::atomic<bool> scheduler_transition_active{false};
};
}
namespace time {
uint64_t get_us() { return ++fixture::now; }
}
namespace smt {
uint64_t get_core_count() { return fixture::expected_core_count; }
}
namespace cpu {
uint64_t current_cpu() { return 0; }
}
struct EventWakeDiagnostics {
    uint64_t admission_us{}, reschedule_us{};
    unsigned passes{};
    bool leader{};
};
enum class TaskPlacementKind : uint8_t { AFFINITY, DOMAIN, MIGRATE, PIN };
struct TaskPlacementRequest {
    TaskPlacementKind kind{TaskPlacementKind::MIGRATE};
    uint64_t target_cpu{1}, mask{};
};
struct TaskPlacementResult {
    int status{-1};
    bool runnable{};
    uint64_t target_cpu{};
};
struct Queues {};
Queues queues;
Queues* run_queues = &queues;
task::Task* current_task_for_preempt() { return fixture::current; }
void hcf() { fixture::require(false, "unexpected fatal guard transition"); }
void preempt_disable();
void preempt_enable();
void preempt_disable_at(uint64_t);
void preempt_enable_at(uint64_t);
void request_local_reschedule();
// ACTUAL_PREEMPT_INSERTION

struct MigrationGuard {
    // CPU identity is fixed in this fixture. The real migration guard remains
    // independently preemptible; it must not substitute for the real guard.
};
uint64_t event_wake_clock(EventWakeDiagnostics const* diagnostics) { return diagnostics ? time::get_us() : 0; }
bool task_placement_subject_is_live(task::Task* task) { return task && !fixture::inactive; }
TaskPlacementResult commit_task_placement_request(task::Task*, uint64_t, TaskPlacementRequest const& request) {
    fixture::require(fixture::subject->type != task::TaskType::PROCESS || fixture::publication == 1,
                     "PROCESS commit owns the publication mutex");
    ++fixture::commits;
    fixture::boundary(fixture::Point::COMMIT);
    return {fixture::place_status, fixture::place_runnable, request.target_cpu};
}
void reschedule_task_for_cpu_once(uint64_t cpu, task::Task* task, EventWakeDiagnostics*) {
    fixture::require(task == fixture::subject, "dispatch keeps the original subject");
    fixture::require(fixture::publication == 0, "no publication mutex survives into the drain");
    ++fixture::dispatches;
    fixture::dispatched_cpus.push_back(cpu);
    fixture::boundary(fixture::Point::DISPATCH);
    // Represents the actual _once notification after its queue lock is gone.
    request_local_reschedule();
}
void request_local_reschedule() {
    ++fixture::notifications;
    fixture::boundary(fixture::Point::NOTIFY);
}
void wake_cpu(uint64_t) { request_local_reschedule(); }
}
namespace ker::syscall::vmem {
struct SharedVmemPublicationGuard {
    SharedVmemPublicationGuard() {
        fixture::require(fixture::publication == 0, "no recursive publication lock");
        fixture::require(fixture::current->preempt_disable_depth == fixture::baseline_depth,
                         "sleepable publication acquisition precedes new preemption protection");
        fixture::publication = 1;
        ++fixture::publication_locks;
    }
    ~SharedVmemPublicationGuard() {
        fixture::require(fixture::publication == 1, "publication unlock is balanced");
        fixture::publication = 0;
        ++fixture::publication_unlocks;
        fixture::boundary(fixture::Point::PUBLICATION_UNLOCK);
    }
};
}

// ACTUAL_LEADERSHIP_INSERTION

namespace fixture {
using namespace ker::mod::sched;
using Wake = void (*)(uint64_t, task::Task*, EventWakeDiagnostics*);
using Place = TaskPlacementResult (*)(task::Task*, TaskPlacementRequest const&);
struct Profile { const char* name; Wake wake; Place place; };
Profile profiles[] = {
// PROFILE_INSERTION
};
Profile const* profile{};
Task follower;

void timer() {
    if (!current) return;  // Actual early-boot no-current guard is a no-op.
    require(interrupts_enabled, "preemption protection preserves interrupt delivery");
    ++timer_decisions;
    auto decision = evaluate_kernel_preemption({
        .frame_class = task::SavedFrameClass::TIMER_PREEMPTED_PROCESS_KERNEL,
        .ordinary_process_kernel_enabled = true,
        .same_cpu_migration_guarded = true,
        .preempt_disable_depth = current->preempt_disable_depth,
        .scheduler_transition_active = current->scheduler_transition_active.load(),
        .deferred_task_switch = current->deferred_task_switch,
        .wants_block = false,
    });
    if (decision.record_pending) current->preempt_pending = true;
    if (decision.can_switch && current->ptrace_stopped) {
        ++allowed_decisions;
        require(!subject || subject == current || subject->leader_executor != current,
                "timer must not park the executor while it owns foreign reschedule leadership");
    } else {
        ++blocked_decisions;
    }
}

void losing_request() {
    require(subject->reschedule_in_progress.load(), "a genuine competing leader exists");
    auto* executor = current;
    unsigned original = follower.preempt_disable_depth;
    current = &follower;
    EventWakeDiagnostics diagnostics;
    profile->wake(3, subject, &diagnostics);
    require(!diagnostics.leader, "concurrent producer genuinely loses the atomic election");
    require(follower.preempt_disable_depth == original, "losing producer balances its own guard");
    require(subject->reschedule_pending.load(), "losing producer leaves real pending work");
    current = executor;
    ++lost_requests;
}

void boundary(Point point) {
    timer();
    if (!injection_depth && inject_request && request_budget && point == request_point &&
        subject->reschedule_in_progress.load()) {
        --request_budget;
        ++injection_depth;
        losing_request();
        --injection_depth;
    }
}

void atomic_boundary(AtomicKind kind, Task* task, bool before, bool changed, bool old, bool value) {
    if (kind == AtomicKind::LEADER && !before && changed) {
        if (value && !old) task->leader_executor = current;
        if (!value) task->leader_executor = nullptr;
    }
    boundary(before ? Point::ATOMIC_BEFORE : Point::ATOMIC_AFTER);
    // Race specifically after the old leader's pending=false load, but before
    // its in_progress=false RMW. The real drain must reacquire and drain it.
    if (kind == AtomicKind::LEADER && before && !value && atomic_release_request && !injection_depth) {
        atomic_release_request = false;
        ++injection_depth;
        losing_request();
        --injection_depth;
    }
    if (kind == AtomicKind::LEADER && !before && changed && !value) boundary(Point::RELEASE);
}

void reset(Task& executor, Task& target, unsigned depth) {
    require(!target.reschedule_in_progress.load() && !target.reschedule_pending.load(), "previous case drained leadership");
    current = &executor;
    subject = &target;
    baseline_depth = depth;
    executor.preempt_disable_depth = depth;
    executor.preempt_pending = false;
    executor.ptrace_stopped = true;
    target.type = task::TaskType::PROCESS;
    follower.ptrace_stopped = false;
    require(follower.preempt_disable_depth == 0, "follower starts balanced");
    publication = publication_locks = publication_unlocks = 0;
    dispatches = commits = notifications = lost_requests = 0;
    inactive = inject_request = atomic_release_request = false;
    request_budget = 0;
    place_status = 0;
    place_runnable = true;
    expected_core_count = 4;
    run_queues = &queues;
    dispatched_cpus.clear();
}

void finished(Task& executor, Task& target, unsigned depth) {
    require(executor.preempt_disable_depth == depth, "executor restores exact nested preemption depth");
    require(!target.reschedule_in_progress.load() && !target.reschedule_pending.load() && !target.leader_executor,
            "all genuine pending requests drain before protection ends");
    require(publication == 0 && publication_locks == publication_unlocks, "publication ownership closes before return");
    require(interrupts_enabled, "interrupt state is unchanged");
    if (depth == 0) {
        require(!executor.preempt_pending, "guard release services the deferred timer");
        auto old = allowed_decisions;
        timer();
        require(allowed_decisions == old + 1, "ptrace parking is allowed after leadership is released");
    } else {
        require(executor.preempt_pending, "outer guard retains the pending timer request");
    }
}

void cases() {
    Task executor, target;
    for (unsigned depth : {0U, 2U}) {
        for (Point point : {Point::ATOMIC_AFTER, Point::DISPATCH, Point::NOTIFY, Point::RELEASE}) {
            reset(executor, target, depth);
            inject_request = true;
            request_point = point;
            request_budget = point == Point::RELEASE ? 0 : 4;
            atomic_release_request = point == Point::RELEASE;
            EventWakeDiagnostics diagnostics;
            profile->wake(2, &target, &diagnostics);
            require(diagnostics.leader && diagnostics.passes == dispatches, "actual public wake drives the actual drain");
            require(dispatches >= 2 && dispatched_cpus.back() == 3 && lost_requests != 0,
                    "real losing-producer requests are dispatched, including release races");
            finished(executor, target, depth);
        }
        for (auto type : {task::TaskType::PROCESS, task::TaskType::DAEMON}) {
            for (Point point : {Point::COMMIT, Point::PUBLICATION_UNLOCK, Point::NOTIFY}) {
                reset(executor, target, depth);
                target.type = type;
                inject_request = type == task::TaskType::PROCESS || point != Point::PUBLICATION_UNLOCK;
                request_point = point;
                request_budget = 1;
                auto result = profile->place(&target, {});
                require(result.status == 0 && result.runnable && commits == 1, "actual placement result survives protection");
                require(publication_locks == (type == task::TaskType::PROCESS ? 1U : 0U), "DAEMON never acquires VM publication");
                require(dispatches == (inject_request ? 1U : 0U), "placement drains only genuine coalesced wakes");
                finished(executor, target, depth);
            }
        }
        // Existing leader: both public paths must lose without touching it.
        for (bool placement : {false, true}) {
            reset(executor, target, depth);
            target.reschedule_in_progress.actual.store(true);
            target.leader_executor = &follower;
            if (placement) require(profile->place(&target, {}).status == -1 && commits == 0, "failed claim makes no placement effects");
            else profile->wake(2, &target, nullptr);
            require(executor.preempt_disable_depth == depth && target.leader_executor == &follower,
                    "failed election balances only the current executor's protection");
            target.reschedule_pending.actual.store(false);
            target.reschedule_in_progress.actual.store(false);
            target.leader_executor = nullptr;
        }
        for (bool runnable : {false, true}) {
            reset(executor, target, depth);
            place_runnable = runnable;
            place_status = runnable ? -7 : 0;
            auto result = profile->place(&target, {});
            require(result.status == place_status && result.runnable == runnable && !dispatches &&
                        notifications == (depth == 0 ? 1U : 0U),
                    "failed/nonrunnable placement only services the guard's deferred timer");
            finished(executor, target, depth);
        }
    }
    reset(executor, target, 0);
    require(profile->place(nullptr, {}).status == -1, "null placement refuses before protection");
    profile->wake(0, nullptr, nullptr);
    require(executor.preempt_disable_depth == 0, "null wake refuses before protection");
    inactive = true;
    require(profile->place(&target, {}).status == -1, "inactive placement refuses before protection");
    inactive = false;
    expected_core_count = 0;
    require(profile->place(&target, {}).status == -1, "zero-core placement refuses before protection");
    expected_core_count = 4;
    TaskPlacementRequest invalid{TaskPlacementKind::PIN, 4, 0};
    require(profile->place(&target, invalid).status == -1, "invalid PIN target refuses before protection");
    run_queues = nullptr;
    require(profile->place(&target, {}).status == -1, "missing RQ refuses placement");
    profile->wake(0, &target, nullptr);
    require(executor.preempt_disable_depth == 0, "all early refusals preserve depth");
    run_queues = &queues;
    current = nullptr;
    subject = &target;
    profile->wake(1, &target, nullptr);
    require(!target.reschedule_in_progress.load() && !target.reschedule_pending.load(), "early-boot no-current wake still drains");
    current = &executor;
    require(blocked_decisions && allowed_decisions && timer_decisions, "both policy outcomes were actually exercised");
}
}

int main(int argc, char** argv) {
    fixture::require(argc == 2, "select exactly one compiled source profile");
    for (auto const& profile : fixture::profiles) {
        if (std::string_view(argv[1]) == profile.name) fixture::profile = &profile;
    }
    fixture::require(fixture::profile != nullptr, "known compiled source profile");
    fixture::cases();
    std::printf("actual leadership/guard policy: %u checks, %u injected timers PASS\n", fixture::checks, fixture::timer_decisions);
}
