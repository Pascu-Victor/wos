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
#include <cstdlib>
#include <memory>
#include <mutex>
#include <platform/sched/run_heap.hpp>
#include <thread>
#include <utility>
// Credential, NUMA and input storage below are explicit caller endpoints. They
// exercise actual entry/copy/cleanup/post slices, not a whole syscall or root.
namespace ker::mod::security {
struct Snapshot {
    unsigned marker{};
};
}  // namespace ker::mod::security
namespace ker::mod::mm::numa {
struct Policy {
    unsigned marker{};
};
struct AllocationContext {
    Policy policy{};
};
}  // namespace ker::mod::mm::numa
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
    unsigned credential_marker{11}, memory_marker{17};
    auto credentials() const -> ker::mod::security::Snapshot { return {credential_marker}; }
    auto memory_policy_snapshot() const -> ker::mod::mm::numa::Policy { return {memory_marker}; }
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
task::Task* current_actor{};
auto get_current_task() -> task::Task* { return current_actor; }
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
namespace ker::mod::power {
bool shutdown{};
auto shutdown_in_progress() -> bool { return shutdown; }
}  // namespace ker::mod::power
namespace ker::mod::security::compartment {
int scope_status{};
auto host_namespace_admission(ker::mod::sched::task::Task* actor) -> int { return actor == nullptr ? -ESRCH : scope_status; }
}  // namespace ker::mod::security::compartment
namespace ker::mod::dbg {
void log(const char*, ...) {}
[[noreturn]] void panic_handler(const char*) { std::abort(); }
}  // namespace ker::mod::dbg
namespace ker::mod::cpu {
auto current_cpu() -> uint64_t { return 2; }
}  // namespace ker::mod::cpu
namespace ker::abi::process {
struct SpawnOptions {};
}  // namespace ker::abi::process
namespace ker::syscall::process {
using Task = ker::mod::sched::task::Task;
using ker::mod::sched::TaskPlacementSnapshot;
struct ExecArgumentsSnapshot {
    std::array<char, 16> path{};
    const char** argv{};
    const char** envp{};
};
struct SpawnOptionsSnapshot {
    ker::abi::process::SpawnOptions options;
};
unsigned argument_calls{}, memory_calls{}, options_calls{}, implementation_calls{}, native_core_calls{}, cleanup_calls{}, abort_calls{},
    priority_calls{};
int argument_error{}, options_error{};
bool mutate_after_capture{};
TaskPlacementSnapshot received_placement{};
unsigned received_credentials{}, received_memory{};
bool received_options{};
int received_depth{};
auto capture_exec_allocation_context(ker::mod::mm::numa::Policy policy, ker::mod::mm::numa::AllocationContext& output) -> int {
    ++memory_calls;
    output.policy = policy;
    return 0;
}
auto snapshot_exec_arguments(Task& actor, uint64_t, uint64_t, uint64_t, ExecArgumentsSnapshot&) -> int {
    ++argument_calls;
    if (mutate_after_capture) {
        actor.store_domain_mask(4);
        actor.store_domain_id(999);
    }
    return argument_error;
}
auto snapshot_spawn_options(Task&, uint64_t, SpawnOptionsSnapshot&) -> int {
    ++options_calls;
    return options_error;
}
auto wos_proc_exec_impl(const char*, const char* const*, const char* const*, const ker::abi::process::SpawnOptions* options, int depth,
                        const ker::mod::security::Snapshot& credentials, ker::mod::mm::numa::Policy memory_policy,
                        const ker::mod::mm::numa::AllocationContext& allocation_context, const TaskPlacementSnapshot& placement)
    -> uint64_t {
    ++implementation_calls;
    received_placement = placement;
    received_credentials = credentials.marker;
    received_memory = memory_policy.marker;
    assert(allocation_context.policy.marker == received_memory);
    received_options = options != nullptr;
    received_depth = depth;
    return 73;
}
auto create_native_compartment_for_subject(Task*, uint64_t, const TaskPlacementSnapshot& placement) -> int64_t {
    ++native_core_calls;
    received_placement = placement;
    return 79;
}
// The parser/loader is an endpoint: invoke the actual production continuation
// with stable credential/argument owners and a deliberately changed local tuple.
// This proves the copied forwarding expression, not shebang parsing or IO.
auto invoke_shebang_continuation(TaskPlacementSnapshot inherited_placement, const ker::mod::security::Snapshot& PARENT_CREDENTIALS,
                                 ker::mod::mm::numa::Policy memory_policy, ker::mod::mm::numa::AllocationContext allocation_context,
                                 const ker::abi::process::SpawnOptions* spawn_options) -> uint64_t {
    auto continuation =  // INJECT_SHEBANG_CONTINUATION
        ;
    inherited_placement.owner_cpu = UINT64_MAX;
    inherited_placement.domain_mask = 0;
    return continuation("/bin/interpreter", nullptr, nullptr, 3);
}
// Actual public wrappers, before any fabricated IO or GS/root execution.
// INJECT_CREATION_WRAPPERS
// Only the two actual constructor fields and non-selftest constructor are
// represented here. Other production preparation owners are not modelled.
struct NativeCompartmentPreparation {
    ker::mod::sched::task::Task& executor;
    // INJECT_NATIVE_PLACEMENT_MEMBER
    // INJECT_NATIVE_PLACEMENT_CONSTRUCTOR
};
auto initialize_native_creation_priority(NativeCompartmentPreparation&, Task&) -> int {
    ++priority_calls;
    return 0;
}
auto abort_native_compartment_preparation(Task&, Task*) -> bool {
    ++abort_calls;
    return true;
}
[[noreturn]] void hcf() { std::abort(); }
auto initialize_legacy(Task* new_task, const TaskPlacementSnapshot& inherited_placement) -> uint64_t {
    namespace sched = ker::mod::sched;
    auto cleanup_unpublished_task = [] { ++cleanup_calls; };
    // INJECT_LEGACY_INITIALIZER
    return 0;
}
auto publish_legacy(Task* new_task, const TaskPlacementSnapshot& inherited_placement) -> uint64_t {
    namespace sched = ker::mod::sched;
    namespace dbg = ker::mod::dbg;
    auto cleanup_unpublished_task = [] { ++cleanup_calls; };
    // INJECT_LEGACY_PUBLICATION
    return 73;
}
auto initialize_native(Task* child, NativeCompartmentPreparation& root) -> int {
    int error = 0;
    // INJECT_NATIVE_INITIALIZER
    return error;
}
auto complete_native(Task& executor, Task* child, int error) -> Task* {
    namespace mod = ker::mod;
    // INJECT_NATIVE_FAILURE_TAIL
    return child;
}
enum class NativeConstructionFault { PUBLICATION_INVALID_CPU };
bool invalid_publication_cpu{};
auto exec_selftest_take_native_construction_fault(NativeConstructionFault, int) -> bool {
    return std::exchange(invalid_publication_cpu, false);
}
struct PublicationRoot {
    TaskPlacementSnapshot inherited_placement;
    int fault_token{};
};
auto publish_native(Task* child, PublicationRoot* root) -> int {
    int result = 0;
#define WOS_SELFTEST 1
    // INJECT_NATIVE_PUBLICATION
#undef WOS_SELFTEST
    return result;
}
}  // namespace ker::syscall::process
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
void creation_callers() {
    namespace creation = ker::syscall::process;
    task::Task actor, canonical;
    cores = 4;
    actor.store_domain_mask(0xA);
    actor.store_domain_id(73);
    actor.store_domain_hard(true);
    canonical.credential_marker = 99;
    canonical.store_domain_mask(8);
    publish_private(actor, 1);
    publish_private(canonical, 3);
    current_actor = &actor;
    TaskPlacementSnapshot const HELD{1, 0xA, 73, false, true};
    auto reset = [&] {
        actor.store_cpu(1);
        actor.store_domain_mask(0xA);
        actor.store_domain_id(73);
        actor.store_domain_hard(true);
        creation::argument_calls = creation::memory_calls = creation::options_calls = creation::implementation_calls =
            creation::native_core_calls = 0;
        creation::argument_error = creation::options_error = 0;
        creation::mutate_after_capture = false;
    };
    for (unsigned kind = 0; kind < 3; ++kind) {
        reset();
        creation::mutate_after_capture = true;
        auto result = kind == 0 ? creation::wos_proc_exec(1, 2, 3) : creation::wos_proc_spawn(1, 2, 3, kind == 2 ? 4 : 0);
        CHECK(result == 73 && creation::implementation_calls == 1);
        CHECK(creation::received_placement == HELD && creation::received_credentials == 11 && creation::received_memory == 17);
        CHECK(creation::argument_calls == 1 && creation::memory_calls == 1 && creation::options_calls == (kind == 2));
        CHECK(creation::received_options == (kind == 2));
        CHECK(actor.load_domain_mask() == 4 && creation::received_placement.domain_mask == 0xA);
    }
    reset();
    CHECK(creation::create_native_compartment_for_subject(&actor, 1) == 79 && creation::native_core_calls == 1);
    CHECK(creation::received_placement == HELD);
    for (unsigned scenario = 0; scenario < 4; ++scenario) {
        reset();
        int expected = -EINVAL;
        if (scenario == 0) {
            detach_private(actor, 1);
            expected = -EBUSY;
        } else if (scenario == 1) {
            cores = 0;
            expected = -ENODEV;
        } else if (scenario == 2) {
            cores = 65;
            expected = -ERANGE;
        } else
            actor.store_domain_mask(0);
        CHECK(creation::wos_proc_exec(1, 2, 3) == static_cast<uint64_t>(expected));
        CHECK(creation::wos_proc_spawn(1, 2, 3, 4) == static_cast<uint64_t>(expected));
        CHECK(creation::create_native_compartment_for_subject(&actor, 1) == expected);
        CHECK(creation::argument_calls == 0 && creation::memory_calls == 0 && creation::options_calls == 0 &&
              creation::implementation_calls == 0 && creation::native_core_calls == 0);
        if (scenario == 0) publish_private(actor, 1);
        cores = 4;
    }
    reset();
    creation::argument_error = -EFAULT;
    CHECK(creation::wos_proc_exec(1, 2, 3) == static_cast<uint64_t>(-EFAULT) && creation::implementation_calls == 0);
    reset();
    creation::options_error = -EBADF;
    CHECK(creation::wos_proc_spawn(1, 2, 3, 4) == static_cast<uint64_t>(-EBADF) && creation::implementation_calls == 0);
    ker::mod::security::compartment::scope_status = -EPERM;
    CHECK(creation::create_native_compartment_for_subject(&actor, 1) == -EPERM);
    ker::mod::security::compartment::scope_status = 0;
    CHECK(creation::create_native_compartment_for_subject(nullptr, 1) == -ESRCH);
    reset();
    ker::mod::power::shutdown = true;
    CHECK(creation::wos_proc_exec(1, 2, 3) == static_cast<uint64_t>(-ESHUTDOWN));
    CHECK(creation::wos_proc_spawn(1, 2, 3, 4) == static_cast<uint64_t>(-ESHUTDOWN));
    CHECK(creation::create_native_compartment_for_subject(&actor, 1) == -ESHUTDOWN);
    CHECK(creation::argument_calls == 0 && creation::implementation_calls == 0 && creation::native_core_calls == 0);
    ker::mod::power::shutdown = false;
    detach_private(actor, 1);
    detach_private(canonical, 3);
    current_actor = nullptr;

    ker::mod::security::Snapshot const CREDENTIALS{119};
    ker::mod::mm::numa::Policy const MEMORY{127};
    ker::mod::mm::numa::AllocationContext const CONTEXT{MEMORY};
    ker::abi::process::SpawnOptions const OPTIONS{};
    unsigned const IMPLEMENTATIONS = creation::implementation_calls;
    CHECK(creation::invoke_shebang_continuation(HELD, CREDENTIALS, MEMORY, CONTEXT, &OPTIONS) == 73);
    CHECK(creation::implementation_calls == IMPLEMENTATIONS + 1 && creation::received_placement == HELD);
    CHECK(creation::received_credentials == 119 && creation::received_memory == 127 && creation::received_options &&
          creation::received_depth == 3);

    task::Task child;
    creation::cleanup_calls = creation::abort_calls = creation::priority_calls = 0;
    CHECK(creation::initialize_legacy(&child, HELD) == 0 && creation::cleanup_calls == 0);
    auto changed = HELD;
    changed.domain_mask = 0;
    CHECK(creation::initialize_legacy(&child, changed) == static_cast<uint64_t>(-EINVAL) && creation::cleanup_calls == 1);
    auto copy = HELD;
    creation::NativeCompartmentPreparation root(actor, copy);
    copy.domain_mask = 0;
    CHECK(root.inherited_placement == HELD);
    CHECK(creation::initialize_native(&child, root) == 0 && creation::priority_calls == 1);
    creation::NativeCompartmentPreparation invalid_root(actor, copy);
    int const REFUSAL = creation::initialize_native(&child, invalid_root);
    CHECK(REFUSAL == -EINVAL && creation::priority_calls == 1);
    CHECK(creation::complete_native(actor, &child, REFUSAL) == nullptr && creation::abort_calls == 1);
    CHECK(creation::complete_native(actor, &child, 0) == &child && creation::abort_calls == 1);
    CHECK(creation::initialize_native(&child, root) == 0);
    creation::PublicationRoot publication{HELD};
    unsigned const BALANCED = balanced_calls;
    CHECK(creation::publish_native(&child, &publication) == 0 && posted_cpu == 1 && balanced_calls == BALANCED);
    creation::invalid_publication_cpu = true;
    unsigned const EXPLICIT = explicit_calls;
    CHECK(creation::publish_native(&child, &publication) == -EAGAIN && explicit_calls == EXPLICIT);
    CHECK(publication.inherited_placement == HELD);
    child.store_domain_id(74);
    CHECK(creation::publish_native(&child, &publication) == -EAGAIN && explicit_calls == EXPLICIT);
    CHECK(creation::publish_legacy(&child, HELD) == 0 && creation::cleanup_calls == 2);
    child.store_domain_id(73);
    publication_ok = false;
    CHECK(creation::publish_native(&child, &publication) == -EAGAIN);
    publication_ok = true;
    cores = 64;
    TaskPlacementSnapshot const PIN{63, 1, 99, true, true};
    CHECK(creation::initialize_legacy(&child, PIN) == 0);
    unsigned const BALANCED_PIN = balanced_calls;
    CHECK(creation::publish_legacy(&child, PIN) == 73 && posted_cpu == 63 && balanced_calls == BALANCED_PIN);
    cores = 4;
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
    creation_callers();
    std::printf("PASS actual placement capture/initializer/publisher adapters: %u checks\n", checks);
}
