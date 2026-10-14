#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <platform/sched/task.hpp>
#include <test/ktest.hpp>
#include <type_traits>

namespace {

using Task = ker::mod::sched::task::Task;
using TaskState = ker::mod::sched::task::TaskState;
using TaskType = ker::mod::sched::task::TaskType;
using ChildMembershipState = ker::mod::sched::task::ChildMembershipState;

static_assert(std::is_same_v<decltype(Task::cpu), uint64_t>);
static_assert(std::is_same_v<decltype(Task::domain_mask), uint64_t>);
static_assert(std::is_same_v<decltype(Task::domain_id), uint32_t>);
static_assert(std::is_same_v<decltype(Task::cpu_pinned), bool>);
static_assert(std::is_same_v<decltype(Task::domain_hard), bool>);
static_assert(std::is_same_v<decltype(Task::deferred_task_switch), bool>);
static_assert(std::is_same_v<decltype(&Task::load_cpu), uint64_t (Task::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Task::store_cpu), void (Task::*)(uint64_t) noexcept>);
static_assert(std::is_same_v<decltype(&Task::load_domain_mask), uint64_t (Task::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Task::store_domain_mask), void (Task::*)(uint64_t) noexcept>);
static_assert(std::is_same_v<decltype(&Task::load_domain_id), uint32_t (Task::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Task::store_domain_id), void (Task::*)(uint32_t) noexcept>);
static_assert(std::is_same_v<decltype(&Task::load_cpu_pinned), bool (Task::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Task::store_cpu_pinned), void (Task::*)(bool) noexcept>);
static_assert(std::is_same_v<decltype(&Task::load_domain_hard), bool (Task::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Task::store_domain_hard), void (Task::*)(bool) noexcept>);

struct PlacementValues {
    uint64_t cpu = 0;
    uint64_t mask = UINT64_MAX;
    uint32_t id = 0;
    bool pinned = false;
    bool hard = false;
};

// Every Task here is exclusively heap-owned and never published. The ordinary
// member reads provide an independent storage oracle after each short-lived
// atomic_ref has ended; they are not an access pattern for live shared Tasks.
void expect_placement(Task& task, const Task& view, const PlacementValues& expected) {
    KEXPECT_EQ(task.load_cpu(), expected.cpu);
    KEXPECT_EQ(task.load_domain_mask(), expected.mask);
    KEXPECT_EQ(task.load_domain_id(), expected.id);
    KEXPECT_EQ(task.load_cpu_pinned(), expected.pinned);
    KEXPECT_EQ(task.load_domain_hard(), expected.hard);
    KEXPECT_EQ(view.load_cpu(), expected.cpu);
    KEXPECT_EQ(view.load_domain_mask(), expected.mask);
    KEXPECT_EQ(view.load_domain_id(), expected.id);
    KEXPECT_EQ(view.load_cpu_pinned(), expected.pinned);
    KEXPECT_EQ(view.load_domain_hard(), expected.hard);
    KEXPECT_EQ(task.cpu, expected.cpu);
    KEXPECT_EQ(task.domain_mask, expected.mask);
    KEXPECT_EQ(task.domain_id, expected.id);
    KEXPECT_EQ(task.cpu_pinned, expected.pinned);
    KEXPECT_EQ(task.domain_hard, expected.hard);
}

template <typename T>
auto member_offset(const Task& task, const T& member) -> size_t {
    const auto object_address = reinterpret_cast<uintptr_t>(std::addressof(task));
    const auto member_address = reinterpret_cast<uintptr_t>(std::addressof(member));
    return static_cast<size_t>(member_address - object_address);
}

template <typename T>
void expect_member_alignment(const T& member) {
    const auto address = reinterpret_cast<uintptr_t>(std::addressof(member));
    KEXPECT_EQ(address % std::atomic_ref<T>::required_alignment, uintptr_t{0});
}

// These fields include actual queue/lifetime state and the adjacent bytes after
// domain_id and the two placement booleans. No references or memberships are
// created, and the snapshots contain no owning resources.
struct PreservedState {
    TaskState state;
    TaskType type;
    ChildMembershipState child_membership;
    decltype(Task::sched_queue) queue;
    uint64_t pid;
    uint64_t parent_pid;
    uint64_t death_epoch;
    uint64_t requested_cpu;
    uint32_t references;
    uint32_t registry_index;
    uint32_t target_flags;
    uint32_t weight;
    uint32_t slice;
    uint32_t slice_used;
    int32_t heap_index;
    int64_t vruntime;
    int64_t vdeadline;
    int8_t nice;
    Task* next;
    Task* previous;
    Task* owned_process;
    const void* list_owner;
    const void* pagemap;
    const void* thread;
    bool published;
    bool exit_in_progress;
    bool has_exited;
    bool has_run;
    bool deferred;
    bool scheduler_transition;
    bool reschedule_pending;
    bool reschedule_in_progress;
    bool gc_queued;
};

auto preserved_state(const Task& task) -> PreservedState {
    return {
        .state = task.state.load(std::memory_order_relaxed),
        .type = task.type,
        .child_membership = task.child_membership_state.load(std::memory_order_relaxed),
        .queue = task.sched_queue,
        .pid = task.pid,
        .parent_pid = task.parent_pid,
        .death_epoch = task.death_epoch.load(std::memory_order_relaxed),
        .requested_cpu = task.reschedule_requested_cpu.load(std::memory_order_relaxed),
        .references = task.ref_count.load(std::memory_order_relaxed),
        .registry_index = task.active_registry_index,
        .target_flags = task.wki_target_flags,
        .weight = task.sched_weight,
        .slice = task.slice_ns,
        .slice_used = task.slice_used_ns,
        .heap_index = task.heap_index,
        .vruntime = task.vruntime,
        .vdeadline = task.vdeadline,
        .nice = task.sched_nice,
        .next = task.sched_next,
        .previous = task.sched_previous,
        .owned_process = task.owned_unpublished_process.load(std::memory_order_relaxed),
        .list_owner = task.sched_list_owner.load(std::memory_order_relaxed),
        .pagemap = task.pagemap,
        .thread = task.thread,
        .published = task.scheduler_published.load(std::memory_order_relaxed),
        .exit_in_progress = task.exit_in_progress.load(std::memory_order_relaxed),
        .has_exited = task.has_exited,
        .has_run = task.has_run,
        .deferred = task.deferred_task_switch,
        .scheduler_transition = task.scheduler_transition_active.load(std::memory_order_relaxed),
        .reschedule_pending = task.reschedule_pending.load(std::memory_order_relaxed),
        .reschedule_in_progress = task.reschedule_in_progress.load(std::memory_order_relaxed),
        .gc_queued = task.gc_queued.load(std::memory_order_relaxed),
    };
}

void expect_preserved(const Task& task, const PreservedState& expected) {
    const auto observed = preserved_state(task);
    KEXPECT_EQ(observed.state, expected.state);
    KEXPECT_EQ(observed.type, expected.type);
    KEXPECT_EQ(observed.child_membership, expected.child_membership);
    KEXPECT_EQ(observed.queue, expected.queue);
    KEXPECT_EQ(observed.pid, expected.pid);
    KEXPECT_EQ(observed.parent_pid, expected.parent_pid);
    KEXPECT_EQ(observed.death_epoch, expected.death_epoch);
    KEXPECT_EQ(observed.requested_cpu, expected.requested_cpu);
    KEXPECT_EQ(observed.references, expected.references);
    KEXPECT_EQ(observed.registry_index, expected.registry_index);
    KEXPECT_EQ(observed.target_flags, expected.target_flags);
    KEXPECT_EQ(observed.weight, expected.weight);
    KEXPECT_EQ(observed.slice, expected.slice);
    KEXPECT_EQ(observed.slice_used, expected.slice_used);
    KEXPECT_EQ(observed.heap_index, expected.heap_index);
    KEXPECT_EQ(observed.vruntime, expected.vruntime);
    KEXPECT_EQ(observed.vdeadline, expected.vdeadline);
    KEXPECT_EQ(observed.nice, expected.nice);
    KEXPECT_EQ(observed.next, expected.next);
    KEXPECT_EQ(observed.previous, expected.previous);
    KEXPECT_EQ(observed.owned_process, expected.owned_process);
    KEXPECT_EQ(observed.list_owner, expected.list_owner);
    KEXPECT_EQ(observed.pagemap, expected.pagemap);
    KEXPECT_EQ(observed.thread, expected.thread);
    KEXPECT_EQ(observed.published, expected.published);
    KEXPECT_EQ(observed.exit_in_progress, expected.exit_in_progress);
    KEXPECT_EQ(observed.has_exited, expected.has_exited);
    KEXPECT_EQ(observed.has_run, expected.has_run);
    KEXPECT_EQ(observed.deferred, expected.deferred);
    KEXPECT_EQ(observed.scheduler_transition, expected.scheduler_transition);
    KEXPECT_EQ(observed.reschedule_pending, expected.reschedule_pending);
    KEXPECT_EQ(observed.reschedule_in_progress, expected.reschedule_in_progress);
    KEXPECT_EQ(observed.gc_queued, expected.gc_queued);
}

}  // namespace

KTEST(TaskPlacementScalar, DefaultsAndExactLayout) {
    const auto task = std::unique_ptr<Task>{new (std::nothrow) Task{}};
    KEXPECT_TRUE(task != nullptr);
    if (task == nullptr) return;
    const Task& view = *task;

    // Keep the original scalar/assembly offsets after appending the native
    // retirement pointer. Task is not standard-layout: inspect the live
    // object's bytes instead of adding offsetof extension warnings.
    KEXPECT_EQ(sizeof(Task), size_t{16216});
    KEXPECT_EQ(alignof(Task), size_t{8});
    KEXPECT_EQ(member_offset(view, view.cpu), size_t{48});
    KEXPECT_EQ(member_offset(view, view.domain_mask), size_t{56});
    KEXPECT_EQ(member_offset(view, view.domain_id), size_t{8000});
    KEXPECT_EQ(member_offset(view, view.cpu_pinned), size_t{11860});
    KEXPECT_EQ(member_offset(view, view.domain_hard), size_t{11861});
    KEXPECT_EQ(member_offset(view, view.deferred_task_switch), size_t{12015});
    KEXPECT_EQ(member_offset(view, view.native_file_mapping_retirement), size_t{16208});
    KEXPECT_EQ(sizeof(view.native_file_mapping_retirement), size_t{8});
    KEXPECT_TRUE(view.native_file_mapping_retirement.load(std::memory_order_relaxed) == nullptr);
    KEXPECT_EQ(sizeof(view.cpu), size_t{8});
    KEXPECT_EQ(sizeof(view.domain_mask), size_t{8});
    KEXPECT_EQ(sizeof(view.domain_id), size_t{4});
    KEXPECT_EQ(sizeof(view.cpu_pinned), size_t{1});
    KEXPECT_EQ(sizeof(view.domain_hard), size_t{1});
    KEXPECT_EQ(sizeof(view.deferred_task_switch), size_t{1});
    expect_member_alignment(view.cpu);
    expect_member_alignment(view.domain_mask);
    expect_member_alignment(view.domain_id);
    expect_member_alignment(view.cpu_pinned);
    expect_member_alignment(view.domain_hard);
    expect_placement(*task, view, {});
    KEXPECT_EQ(task->state.load(std::memory_order_relaxed), TaskState::ACTIVE);
    KEXPECT_EQ(task->type, TaskType::DAEMON);
    KEXPECT_EQ(task->pid, uint64_t{0});
    KEXPECT_EQ(task->ref_count.load(std::memory_order_relaxed), uint32_t{1});
    KEXPECT_FALSE(task->scheduler_published.load(std::memory_order_relaxed));
    KEXPECT_EQ(task->child_membership_state.load(std::memory_order_relaxed), ChildMembershipState::UNLINKED);
    KEXPECT_EQ(task->sched_queue, decltype(Task::sched_queue)::NONE);
    KEXPECT_NULL(task->pagemap);
    KEXPECT_NULL(task->thread);
    KEXPECT_NULL(task->owned_unpublished_process.load(std::memory_order_relaxed));
}

KTEST(TaskPlacementScalar, MutableAndConstRoundTrips) {
    const auto task = std::unique_ptr<Task>{new (std::nothrow) Task{}};
    KEXPECT_TRUE(task != nullptr);
    if (task == nullptr) return;
    const Task& view = *task;
    PlacementValues expected;
    const auto original = preserved_state(view);
    expect_placement(*task, view, expected);

    for (auto value : std::array<uint64_t, 4>{0, 63, 64, UINT64_MAX}) {
        task->store_cpu(value);
        expected.cpu = value;
        expect_placement(*task, view, expected);
    }
    for (auto value : std::array<uint64_t, 3>{0, uint64_t{1} << 63, UINT64_MAX}) {
        task->store_domain_mask(value);
        expected.mask = value;
        expect_placement(*task, view, expected);
    }
    for (auto value : std::array<uint32_t, 2>{0, UINT32_MAX}) {
        task->store_domain_id(value);
        expected.id = value;
        expect_placement(*task, view, expected);
    }
    for (auto value : std::array<bool, 3>{true, false, true}) {
        task->store_cpu_pinned(value);
        expected.pinned = value;
        expect_placement(*task, view, expected);
    }
    for (auto value : std::array<bool, 3>{true, false, true}) {
        task->store_domain_hard(value);
        expected.hard = value;
        expect_placement(*task, view, expected);
    }
    expect_preserved(view, original);
}

KTEST(TaskPlacementScalar, IndependentScalarWritesPreserveOtherFields) {
    const auto task = std::unique_ptr<Task>{new (std::nothrow) Task{}};
    KEXPECT_TRUE(task != nullptr);
    if (task == nullptr) return;
    const Task& view = *task;
    PlacementValues expected{64, (uint64_t{1} << 63) | 1, 0x6a5a4a3aU, true, false};
    task->store_cpu(expected.cpu);
    task->store_domain_mask(expected.mask);
    task->store_domain_id(expected.id);
    task->store_cpu_pinned(expected.pinned);
    task->store_domain_hard(expected.hard);

    // Owned metadata canaries require no scheduler publication or reference.
    task->wki_target_flags = 0xa55a0000U;
    task->has_run = true;
    task->deferred_task_switch = true;
    task->vruntime = -9;
    task->vdeadline = 19;
    const auto original = preserved_state(view);
    expect_placement(*task, view, expected);

    for (auto value : std::array<uint64_t, 4>{UINT64_MAX, 0, 63, 64}) {
        task->store_cpu(value);
        expected.cpu = value;
        expect_placement(*task, view, expected);
        expect_preserved(view, original);
    }
    for (auto value : std::array<uint64_t, 3>{UINT64_MAX, 0, uint64_t{1} << 63}) {
        task->store_domain_mask(value);
        expected.mask = value;
        expect_placement(*task, view, expected);
        expect_preserved(view, original);
    }
    for (auto value : std::array<uint32_t, 2>{UINT32_MAX, 0}) {
        task->store_domain_id(value);
        expected.id = value;
        expect_placement(*task, view, expected);
        expect_preserved(view, original);
    }
    for (const auto& values : std::array<std::array<bool, 2>, 4>{{{false, true}, {true, false}, {true, true}, {false, false}}}) {
        task->store_cpu_pinned(values[0]);
        expected.pinned = values[0];
        expect_placement(*task, view, expected);
        expect_preserved(view, original);
        task->store_domain_hard(values[1]);
        expected.hard = values[1];
        expect_placement(*task, view, expected);
        expect_preserved(view, original);
    }

    task->wki_target_flags = 0;
    task->has_run = false;
    task->deferred_task_switch = false;
    task->vruntime = 0;
    task->vdeadline = 0;
}
