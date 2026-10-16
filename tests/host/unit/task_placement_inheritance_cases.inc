// Production policies/accessors/RunHeap::contains are extracted by the driver.
// CPU queue transport, Task lifetime storage and final publishers are endpoints;
// genuine kernel RunQueues/Tasks are separately exercised by native cases.
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <platform/sched/run_heap.hpp>
#include <thread>
#include <utility>
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON, IDLE };
enum class TaskState { ACTIVE, DEAD, EXITING };
struct Task {
    uint64_t cpu{}, domain_mask{UINT64_MAX}, pid{1};
    uint32_t domain_id{}, active_registry_index{UINT32_MAX};
    bool cpu_pinned{}, domain_hard{}, has_run{}, has_exited{};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> exit_in_progress{}, gc_queued{}, scheduler_published{};
    enum class sched_queue { NONE, RUNNABLE, WAITING, DEAD_GC };
    enum sched_queue sched_queue { sched_queue::NONE };
    int32_t heap_index{-1};
    Task* sched_previous{};
    Task* sched_next{};
    std::atomic<IntrusiveTaskList*> sched_list_owner{};
    TaskType type{TaskType::PROCESS};
    // INJECT_ACCESSORS
};
}  // namespace ker::mod::sched::task
namespace ker::mod::sched {
// INJECT_SNAPSHOT
// INJECT_OWNERSHIP
struct RunQueue {
    RunHeap runnable_heap{};
    IntrusiveTaskList wait_list{};
    task::Task* current_task{};
    task::Task* handoff_task{};
};
// INJECT_HEAP_ENTRY
// INJECT_CONTAINS
thread_local unsigned lock_depth{};
std::mutex transport_lock;
struct Queues {
    std::array<RunQueue, 64> rows{};
    uint64_t lock_calls{};
    template <class F>
    void with_lock_void(uint64_t cpu, F&& f) {
        std::lock_guard guard(transport_lock);
        assert(cpu < rows.size() && !lock_depth);
        ++lock_depth;
        ++lock_calls;
        f(&rows[cpu]);
        --lock_depth;
    }
};
std::unique_ptr<Queues> storage(new Queues);
Queues* run_queues = storage.get();
uint64_t cores = 4;
namespace smt {
uint64_t get_core_count() { return cores; }
}  // namespace smt
unsigned balanced_calls{}, explicit_calls{};
uint64_t posted_cpu{UINT64_MAX};
bool publication_ok = true;
bool post_task_for_cpu(uint64_t cpu, task::Task*) {
    assert(!lock_depth && cpu < cores);
    ++explicit_calls;
    posted_cpu = cpu;
    return publication_ok;
}
bool post_task_balanced(task::Task*) {
    assert(!lock_depth);
    ++balanced_calls;
    return publication_ok;
}
// INJECT_FUNCTIONS
}  // namespace ker::mod::sched
using namespace ker::mod::sched;
unsigned checks{};
#define CHECK(expression)     \
    do {                      \
        ++checks;             \
        assert((expression)); \
    } while (false)
void publish_private(task::Task& t, uint64_t cpu) {
    std::lock_guard guard(transport_lock);
    auto& rq = storage->rows[cpu];
    rq.runnable_heap.entries[0] = &t;
    rq.runnable_heap.size = 1;
    t.heap_index = 0;
    t.sched_queue = task::Task::sched_queue::RUNNABLE;
    t.scheduler_published = true;
    t.store_cpu(cpu);
}
void detach_private(task::Task& t, uint64_t cpu) {
    std::lock_guard guard(transport_lock);
    auto& rq = storage->rows[cpu];
    rq.runnable_heap.entries[0] = nullptr;
    rq.runnable_heap.size = 0;
    t.heap_index = -1;
    t.sched_queue = task::Task::sched_queue::NONE;
    t.scheduler_published = false;
}
int main() {
    task::Task source;
    publish_private(source, 1);
    source.store_cpu(3);  // actual membership is deliberately elsewhere
    source.store_domain_mask(0xA);
    source.store_domain_id(79);
    source.store_domain_hard(true);
    TaskPlacementSnapshot out{.owner_cpu = 17, .domain_mask = 0xCAFE, .domain_id = 91, .cpu_pinned = true};
    CHECK(capture_task_placement_snapshot(&source, out) == 0);
    CHECK(out == TaskPlacementSnapshot({1, 0xA, 79, false, true}));
    CHECK(source.load_cpu() == 3);  // observer never repairs its hint
    source.store_cpu_pinned(true);
    source.store_domain_mask(4);  // real PIN can preserve incompatible raw mask
    CHECK(capture_task_placement_snapshot(&source, out) == 0);
    CHECK(out.owner_cpu == 1 && out.domain_mask == 4 && out.cpu_pinned && out.domain_hard);
    TaskPlacementSnapshot const HELD = out;
    source.store_domain_mask(0);
    CHECK(capture_task_placement_snapshot(&source, out) == -EINVAL && out == HELD);
    source.store_domain_mask(0x20);
    CHECK(capture_task_placement_snapshot(&source, out) == -EINVAL && out == HELD);
    source.store_domain_mask(0xA);
    source.heap_index = -1;
    CHECK(capture_task_placement_snapshot(&source, out) == -EBUSY && out == HELD);
    source.heap_index = 0;
    source.exit_in_progress = true;
    CHECK(capture_task_placement_snapshot(&source, out) == -EBUSY && out == HELD);
    source.exit_in_progress = false;
    CHECK(capture_task_placement_snapshot(nullptr, out) == -ESRCH && out == HELD);
    cores = 0;
    CHECK(capture_task_placement_snapshot(&source, out) == -ENODEV && out == HELD);
    cores = 65;
    CHECK(capture_task_placement_snapshot(&source, out) == -ERANGE && out == HELD);
    cores = 4;
    run_queues = nullptr;
    CHECK(capture_task_placement_snapshot(&source, out) == -EBUSY && out == HELD);
    run_queues = storage.get();
    detach_private(source, 1);
    uint64_t const BEFORE = storage->lock_calls;
    CHECK(capture_task_placement_snapshot(&source, out) == -EBUSY && out == HELD);
    CHECK(storage->lock_calls - BEFORE <= 2 * (cores + 1));
    // Reserved and waiting membership use the actual production predicates.
    storage->rows[2].current_task = &source;
    source.store_cpu(0);
    source.store_domain_mask(4);
    source.store_cpu_pinned(false);
    CHECK(capture_task_placement_snapshot(&source, out) == 0 && out.owner_cpu == 2);
    storage->rows[2].current_task = nullptr;
    storage->rows[2].wait_list.head = &source;
    source.sched_list_owner = &storage->rows[2].wait_list;
    source.sched_queue = task::Task::sched_queue::WAITING;
    CHECK(capture_task_placement_snapshot(&source, out) == 0 && out.owner_cpu == 2);
    storage->rows[2].wait_list.head = nullptr;
    source.sched_list_owner = nullptr;
    source.sched_queue = task::Task::sched_queue::NONE;
    // New child storage is exclusively owned and never a queue/global member.
    task::Task child;
    TaskPlacementSnapshot const HARD{1, 0xA, 73, false, true};
    CHECK(initialize_inherited_task_placement(&child, HARD) == 0);
    CHECK(child.load_cpu() == 1 && child.load_domain_mask() == 0xA && child.load_domain_id() == 73 && !child.load_cpu_pinned() &&
          child.load_domain_hard());
    constexpr std::array<uint64_t, 4> EXPECTED{1, 1, 3, 3};
    std::array<unsigned, 4> counts{};
    for (uint64_t seed = 0; seed < 1000; ++seed) {
        CHECK(post_task_round_robin_inherited(&child, HARD, seed));
        CHECK(posted_cpu == EXPECTED[seed % 4]);
        ++counts[posted_cpu];
    }
    CHECK((counts == std::array<unsigned, 4>({0, 500, 0, 500})));
    CHECK(post_task_round_robin_inherited(&child, HARD, UINT64_MAX) && posted_cpu == 3);
    CHECK(post_task_balanced_inherited(&child, HARD) && balanced_calls == 1);
    TaskPlacementSnapshot const UMA{2, UINT64_MAX, 0, false, false};
    CHECK(initialize_inherited_task_placement(&child, UMA) == 0);
    for (uint64_t seed = 0; seed < 128; ++seed) CHECK(post_task_round_robin_inherited(&child, UMA, seed) && posted_cpu == seed % cores);
    cores = 64;
    TaskPlacementSnapshot const PIN{63, 1, 99, true, true};
    CHECK(initialize_inherited_task_placement(&child, PIN) == 0);
    unsigned const BALANCED_BEFORE = balanced_calls;
    CHECK(post_task_balanced_inherited(&child, PIN) && posted_cpu == 63 && balanced_calls == BALANCED_BEFORE);
    CHECK(post_task_round_robin_inherited(&child, PIN, 0) && posted_cpu == 63);
    CHECK(post_task_round_robin_inherited(&child, PIN, UINT64_MAX) && posted_cpu == 63);
    auto const ORIGINAL_CPU = child.load_cpu();
    auto invalid = PIN;
    invalid.domain_mask = 0;
    CHECK(initialize_inherited_task_placement(&child, invalid) == -EINVAL && child.load_cpu() == ORIGINAL_CPU);
    child.scheduler_published = true;
    CHECK(initialize_inherited_task_placement(&child, PIN) == -EBUSY);
    CHECK(!post_task_balanced_inherited(&child, PIN));
    child.scheduler_published = false;
    child.store_domain_id(100);
    CHECK(!post_task_round_robin_inherited(&child, PIN, 0));
    child.store_domain_id(99);
    publication_ok = false;
    CHECK(!post_task_balanced_inherited(&child, PIN));
    publication_ok = true;
    // Complete tuple consistency during real Host-thread writes serialized by
    // the explicit queue-transport seam. This is not kernel SMP qualification.
    cores = 4;
    source.store_cpu_pinned(false);
    source.store_domain_mask(0xA);
    source.store_domain_id(73);
    source.store_domain_hard(true);
    publish_private(source, 1);
    std::atomic<bool> stop{};
    std::thread writer([&] {
        while (!stop.load())
            run_queues->with_lock_void(1, [&](RunQueue*) {
                bool const change = source.load_domain_id() == 73;
                source.store_domain_mask(change ? 2 : 0xA);
                source.store_domain_id(change ? 91 : 73);
                source.store_cpu_pinned(change);
                source.store_domain_hard(!change);
            });
    });
    for (unsigned n = 0; n < 4000; ++n) {
        CHECK(capture_task_placement_snapshot(&source, out) == 0);
        CHECK(out == TaskPlacementSnapshot({1, 2, 91, true, false}) || out == TaskPlacementSnapshot({1, 0xA, 73, false, true}));
    }
    stop = true;
    writer.join();
    detach_private(source, 1);
    std::printf("PASS actual placement capture/initializer/publisher adapters: %u checks\n", checks);
}
