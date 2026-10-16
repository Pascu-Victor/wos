namespace host_fork_alloc {
void settle_early();
}
// The caller and cleanup slices are compiled separately without alteration.
// These are explicit Host FD/Thread/stack/root-physical endpoints, not native
// Task construction, resource accounting, page-table teardown, GS or publication.
namespace ker::mod::cpu {
// The actual PerCpu layout/default member is retained; GS guard acquisition is
// an explicit Host endpoint because these cleanup vectors never install GS.
__attribute__((no_stack_protector)) auto stack_protector_guard() -> uint64_t {
    host_fork_alloc::guard();
    return 0x48b4;
}
}  // namespace ker::mod::cpu
namespace host_fork_public {
using Task = ker::mod::sched::task::Task;
using Root = ker::mod::mm::paging::PageTable;
bool active{}, file_seen{}, destroyed{}, released{};
Task* subject{};
void *name_token{}, *stack_token{}, *thread_token{}, *percpu_token{};
unsigned fd_refs{}, aborts{}, successes{};
std::vector<std::string> events;
void note_delete(void* p) {
    if (!active || !p) return;
    if (p == thread_token) {
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(fd_refs, 0U);
        events.push_back("thread");
        thread_token = nullptr;
    } else if (p == percpu_token) {
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_EQ(thread_token, nullptr);
        events.push_back("percpu");
        percpu_token = nullptr;
    } else if (p == name_token) {
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_TRUE(released);
        events.push_back("name");
        name_token = nullptr;
    } else if (p == subject) {
        EXPECT_TRUE(host_fork_vm::ordinary());
        EXPECT_TRUE(released);
        EXPECT_EQ(name_token, nullptr);
        EXPECT_EQ(stack_token, nullptr);
        EXPECT_EQ(thread_token, nullptr);
        EXPECT_EQ(percpu_token, nullptr);
        events.push_back("task");
        subject = nullptr;
    }
}
bool free_stack(void* p) {
    if (!active || p != stack_token) return false;
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_TRUE(released);
    EXPECT_EQ(name_token, nullptr);
    events.push_back("stack");
    stack_token = nullptr;
    std::free(p);
    return true;
}
void lazy_lock(unsigned tag) {
    if (active && file_seen && tag == 2) {
        EXPECT_TRUE(host_fork_vm::ordinary());
        events.push_back("lazy");
    }
}
void file_sync(Root* root) {
    EXPECT_TRUE(active);
    EXPECT_TRUE(host_fork_vm::ordinary());
    ASSERT_NE(subject, nullptr);
    EXPECT_EQ(root, subject->pagemap);
    // The actual SHM body must detach its metadata before File writeback.
    for (auto const& row : ker::syscall::shm::g_attachments) EXPECT_FALSE(row.active && row.pid == subject->pid);
    EXPECT_EQ(thread_token, nullptr);
    EXPECT_EQ(percpu_token, nullptr);
    EXPECT_EQ(fd_refs, 0U);
    file_seen = true;
    events.push_back("file");
}
void destroy_root(Root* root) {
    namespace virt = ker::mod::mm::virt;
    namespace fm = ker::mod::mm::file_mapping;
    EXPECT_TRUE(active);
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_TRUE(file_seen);
    ASSERT_NE(subject, nullptr);
    EXPECT_EQ(root, subject->pagemap);
    EXPECT_TRUE(subject->lazy_vmem_ranges.empty());
    for (auto const& row : ker::syscall::vmem::g_file_mmap_ranges) EXPECT_NE(row.pagemap, root);
    events.push_back("root");
    // Preserve the accepted actual swap/INDEX cleanup and completed-ACK port.
    // The maintained physical fixture disposes PTE/data/table refs afterward.
    virt::purge_anonymous_swap_mappings(root);
    fm::FileMappingTlbRetirement retirement;
    auto const flags = virt::user_mapping_pin_lock.lock_irqsave();
    EXPECT_TRUE(virt::file_mapping_index.detach_root_locked(root, virt::host_child_control.serial, retirement));
    virt::user_mapping_pin_lock.unlock_irqrestore(flags);
    retirement.release_after_completed_tlb();
    fm::host_drain();
    destroyed = true;
}
void release_root(Root* root) {
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_TRUE(destroyed);
    ASSERT_NE(subject, nullptr);
    EXPECT_EQ(root, subject->pagemap);
    for (auto* row = ker::mod::mm::virt::anonymous_swap_mappings; row; row = row->next) EXPECT_NE(row->pagemap, root);
    events.push_back("root-release");
    released = true;
}
void release_fds(Task* task) {
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_EQ(task, subject);
    events.push_back("fds");
    fd_refs = 0;
}
void abort_child(Task& task) {
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_EQ(&task, subject);
    EXPECT_EQ(task.lifetime_refs, 1U);
    events.push_back("abort");
    ++aborts;
}
uint64_t finish(int64_t status, uint64_t bytes, const char*) {
    EXPECT_TRUE(host_fork_vm::ordinary());
    EXPECT_EQ(bytes, 0U);
    EXPECT_EQ(subject, nullptr);
    events.push_back("finish");
    return static_cast<uint64_t>(status);
}
void vm_success(Task* task) {
    EXPECT_EQ(task, subject);
    EXPECT_FALSE(host_range::interrupts);
    ++successes;
}
}  // namespace host_fork_public

namespace ker::syscall::process {
auto host_public_vm_stage(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, bool, ker::mod::mm::numa::AllocationContext&,
                          ker::mod::mm::numa::AllocationResult&, uint64_t) -> uint64_t;
using HostCleanup = uint64_t (*)(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool);
auto host_public_cleanup_thread(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool) -> uint64_t;
auto host_public_cleanup_tcb(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool) -> uint64_t;
auto host_public_cleanup_fd(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool) -> uint64_t;
auto host_public_cleanup_publication(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool) -> uint64_t;
auto host_public_cleanup_scheduler(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, bool) -> uint64_t;
}  // namespace ker::syscall::process
namespace {
using FailurePoint = ker::syscall::process::detail::ForkVmFailurePoint;
TEST(PublicForkError, CompletePhaseStatusTable) {
    constexpr int statuses[] = {0,       -EBUSY,  -ESRCH, -EAGAIN,    -ENOMEM, -EOVERFLOW, -EOPNOTSUPP, -EINVAL,
                                -ENOSPC, -EFAULT, -EIO,   -ESHUTDOWN, -999,    1,          INT_MAX,     INT_MIN};
    constexpr int pass[] = {0,       -EBUSY,  -ESRCH, -EAGAIN,    -ENOMEM, -EOVERFLOW, -EOPNOTSUPP, -EINVAL,
                            -ENOSPC, -EFAULT, -EIO,   -ESHUTDOWN, -999,    -EINVAL,    -EINVAL,     INT_MIN};
    constexpr int boolean[] = {0,       -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM,
                               -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM, -ENOMEM};
    constexpr int legacy[] = {0,       -EBUSY,  -ESRCH, -EAGAIN,    -ENOMEM, -ENOMEM, -ENOMEM, -EINVAL,
                              -ENOSPC, -EFAULT, -EIO,   -ESHUTDOWN, -999,    -EINVAL, -EINVAL, INT_MIN};
    constexpr int swap[] = {0,       -EBUSY,  -ESRCH, -EAGAIN,    -ENOMEM, -ENOMEM, -EOPNOTSUPP, -EINVAL,
                            -ENOSPC, -EFAULT, -EIO,   -ESHUTDOWN, -999,    -EINVAL, -EINVAL,     INT_MIN};
    constexpr int fresh[] = {0,       -EINVAL, -EINVAL, -EAGAIN, -EINVAL, -EINVAL, -EINVAL, -EINVAL,
                             -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL};
    constexpr int geometry[] = {0,       -EINVAL, -EINVAL, -EAGAIN, -ENOMEM, -EOVERFLOW, -EOPNOTSUPP, -EINVAL,
                                -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL,    -EINVAL,     -EINVAL};
    constexpr int invalid[] = {0,       -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL,
                               -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL, -EINVAL};
    struct Row {
        FailurePoint point;
        const int* expected;
    };
    Row const rows[] = {{FailurePoint::Context, pass},
                        {FailurePoint::LazyPrepare, pass},
                        {FailurePoint::TmpfsPrepare, pass},
                        {FailurePoint::LegacyPrepare, legacy},
                        {FailurePoint::SwapPrepare, swap},
                        {FailurePoint::ShmPrepare, boolean},
                        {FailurePoint::GeometryPrepare, geometry},
                        {FailurePoint::FinalLazyTmpfsMatch, fresh},
                        {FailurePoint::LegacyMatch, fresh},
                        {FailurePoint::ShmMatch, boolean},
                        {FailurePoint::LazyConsume, boolean},
                        {FailurePoint::Copy, boolean},
                        {FailurePoint::TmpfsAttach, boolean},
                        {FailurePoint::ShmCommit, boolean},
                        {FailurePoint::LegacyCommit, boolean},
                        {FailurePoint::Complete, invalid},
                        {static_cast<FailurePoint>(16), invalid},
                        {static_cast<FailurePoint>(255), invalid}};
    for (auto const& row : rows)
        for (size_t i = 0; i < std::size(statuses); ++i)
            EXPECT_EQ(ker::syscall::process::detail::public_fork_vm_error(row.point, statuses[i]), row.expected[i]);
    for (unsigned i = 0; i < 256; ++i) EXPECT_EQ(ker::syscall::process::detail::public_fork_vm_error(static_cast<FailurePoint>(i), 0), 0);
}

class PublicForkWiring : public ForkVmCoordinator {
   protected:
    using Task = ker::mod::sched::task::Task;
    void SetUp() override {
        ForkVmCoordinator::SetUp();
        host_fork_public::active = host_fork_public::file_seen = host_fork_public::destroyed = host_fork_public::released = false;
        host_fork_public::subject = nullptr;
        host_fork_public::name_token = host_fork_public::stack_token = host_fork_public::thread_token = host_fork_public::percpu_token =
            nullptr;
        host_fork_public::fd_refs = host_fork_public::aborts = host_fork_public::successes = 0;
        host_fork_public::events.clear();
    }
    auto make_child() -> Task* {
        auto* task = new Task;
        task->pid = 2;
        task->pagemap = child;
        task->name = new char[8]{};
        task->lazy_vmem_lock.tag = 2;
        host_fork_public::subject = task;
        host_fork_public::name_token = task->name;
        host_fork_public::stack_token = std::malloc(64);
        host_fork_public::file_seen = host_fork_public::destroyed = host_fork_public::released = false;
        host_fork_public::active = true;
        return task;
    }
    auto caller(bool eager = false) -> int64_t {
        auto* task = make_child();
        auto placement = context(numa::Mode::LOCAL, 0);
        numa::AllocationResult output;
        host_range::interrupts = false;
        host_fork_vm::enabled = true;
        auto const result = ker::syscall::process::host_public_vm_stage(&actor, task, eager, placement, output,
                                                                        reinterpret_cast<uint64_t>(host_fork_public::stack_token));
        host_fork_vm::enabled = false;
        EXPECT_FALSE(host_range::interrupts);
        host_range::interrupts = true;
        EXPECT_TRUE(output.span.empty());
        return static_cast<int64_t>(result);
    }
    void expect_vm_rollback() {
        EXPECT_EQ(host_fork_public::subject, nullptr);
        EXPECT_EQ(host_fork_public::successes, 0U);
        EXPECT_TRUE(host_fork_public::destroyed);
        EXPECT_TRUE(host_fork_public::released);
        EXPECT_EQ(host_fork_public::events,
                  (std::vector<std::string>{"file", "lazy", "root", "root-release", "name", "stack", "task", "finish"}));
    }
    void later(unsigned stage) {
        using namespace ker::syscall::process;
        ASSERT_EQ(caller(), 0);
        auto* task = host_fork_public::subject;
        ASSERT_NE(task, nullptr);
        if (stage != 0) {
            task->thread = new host_fork_public::Thread;
            host_fork_public::thread_token = task->thread;
        }
        if (stage >= 2) {
            auto* scratch = new ker::mod::cpu::PerCpu;
            task->context.syscall_scratch_area = reinterpret_cast<uint64_t>(scratch);
            host_fork_public::percpu_token = scratch;
            host_fork_public::fd_refs = 3;
        }
        bool const publication_ref = stage == 4;
        if (publication_ref) task->lifetime_refs = 2;
        HostCleanup const functions[] = {host_public_cleanup_thread, host_public_cleanup_tcb, host_public_cleanup_fd,
                                         host_public_cleanup_publication, host_public_cleanup_scheduler};
        host_range::interrupts = false;
        auto result = functions[stage](&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), publication_ref);
        EXPECT_EQ(static_cast<int64_t>(result), stage == 1 ? -EFAULT : -ENOMEM);
        EXPECT_FALSE(host_range::interrupts);
        host_range::interrupts = true;
        EXPECT_EQ(host_fork_public::subject, nullptr);
        EXPECT_EQ(host_fork_public::successes, 1U);
        EXPECT_EQ(host_fork_public::fd_refs, 0U);
        EXPECT_EQ(host_fork_public::aborts, stage >= 3 ? 1U : 0U);
        auto expected = std::vector<std::string>{};
        if (stage >= 3) expected.push_back("abort");
        if (stage >= 2) expected.push_back("fds");
        if (stage != 0) expected.push_back("thread");
        if (stage >= 2) expected.push_back("percpu");
        for (auto const& event : {"file", "lazy", "root", "root-release", "name", "stack", "task", "finish"}) expected.push_back(event);
        EXPECT_EQ(host_fork_public::events, expected);
    }
    void TearDown() override {
        host_fork_alloc::settle_early();
        // Settle a deliberately bad control's still-owned Host child via the
        // actual ordinary Thread-failure cleanup slice; never clear ownership.
        if (host_fork_public::subject) {
            host_range::interrupts = false;
            (void)ker::syscall::process::host_public_cleanup_thread(&actor, host_fork_public::subject,
                                                                    reinterpret_cast<uint64_t>(host_fork_public::stack_token), false);
        }
        host_fork_public::active = false;
        host_range::interrupts = true;
        ForkVmCoordinator::TearDown();
    }
};
TEST_F(PublicForkWiring, EmptyLocalSuccessRestoresEntryIfBeforeFrontier) { later(0); }
TEST_F(PublicForkWiring, ThreadFailureCleansOrdinarily) {
    legacy();
    tmpfs();
    shared();
    later(0);
}
TEST_F(PublicForkWiring, TcbFailureCleansOrdinarily) {
    legacy();
    tmpfs();
    shared();
    later(1);
}
TEST_F(PublicForkWiring, FdFailureCleansOrdinarily) {
    legacy();
    tmpfs();
    shared();
    later(2);
}
TEST_F(PublicForkWiring, ChildPublicationFailureCleansOrdinarily) {
    legacy();
    tmpfs();
    shared();
    later(3);
}
TEST_F(PublicForkWiring, SchedulerFailureReleasesOnlyAcquiredPublicationRef) {
    legacy();
    tmpfs();
    shared();
    later(4);
}
TEST_F(PublicForkWiring, NativeLazyAndGlobalRegistryKeepDifferentPublicErrors) {
    ker::mod::sched::task::LazyVmemRange row;
    row.kind = ker::mod::sched::task::LazyVmemKind::NATIVE_FILE_BACKED;
    ASSERT_TRUE(actor.lazy_vmem_ranges.push_back(row));
    EXPECT_EQ(caller(), -EBUSY);
    expect_vm_rollback();
    actor.lazy_vmem_ranges.pop_back();
    host_fork_public::active = false;
    host_fork_public::events.clear();
    vmem::NativeMappingEntry native;
    native.root = runtime_root;
    vmem::g_native_file_mmap_ranges = &native;
    EXPECT_EQ(caller(), -ENOMEM);
    vmem::g_native_file_mmap_ranges = nullptr;
    expect_vm_rollback();
    EXPECT_EQ(fork_exclusives, 0U);
}
TEST_F(PublicForkWiring, LateMixedRefusalRunsEveryAdoptedPrefixCleanup) {
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    shared();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    host_fork_vm::registry_fail_try = 2;
    EXPECT_EQ(caller(), -ENOMEM);
    expect_vm_rollback();
    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
    EXPECT_EQ(vmem::g_file_mmap_ranges.size(), 1U);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_GE(actual_flushes, 1U);
}
TEST_F(PublicForkWiring, CopierOomRollsBackLazyPrefixAfterParentAck) {
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    fork_fail_table_at = 2;
    EXPECT_EQ(caller(), -ENOMEM);
    expect_vm_rollback();
    EXPECT_GE(actual_flushes, 1U);
}
TEST_F(PublicForkWiring, EagerCallerStorageReachesRealPreparedCopier) {
    ordinary(TEST_VA);
    fork_eager = true;
    EXPECT_EQ(caller(true), 0);
    EXPECT_EQ(fork_exclusives, 1U);
    EXPECT_EQ(fork_reader_attempts, 0U);
}
TEST_F(PublicForkWiring, GeometryUnsupportedKeepsExplicitPublicError) {
    auto* next = table();
    word(runtime_root, virt::index_of(TEST_VA, 4)) = reinterpret_cast<uint64_t>(next) | 7;
    word(next, virt::index_of(TEST_VA, 3)) = 0x87;
    EXPECT_EQ(caller(), -EOPNOTSUPP);
    expect_vm_rollback();
    word(runtime_root, virt::index_of(TEST_VA, 4)) = 0;
}
TEST_F(PublicForkWiring, FailurePointIdentifiesPrivateRefusalAndSuccess) {
    FailurePoint point = FailurePoint::Complete;
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    actor.syscall_account_start_us = 0;
    EXPECT_EQ(ker::syscall::process::detail::prepare_fork_vm_snapshot(actor, child_task, preparation, nullptr, nullptr, &point),
              -EWOULDBLOCK);
    EXPECT_EQ(point, FailurePoint::Context);
    actor.syscall_account_start_us = 1;
    EXPECT_EQ(ker::syscall::process::detail::prepare_fork_vm_snapshot(actor, child_task, preparation, nullptr, nullptr, &point), 0);
    EXPECT_EQ(point, FailurePoint::Complete);
}
}  // namespace

// Fork allocation executes real new-expression/refusal slices. Task layout remains
// the accepted Host port; actual native Task providers are target evidence.
namespace host_fork_alloc {
using Task = ker::mod::sched::task::Task;
enum class Denial { None, Task, Name, Thread, PerCpu };
bool active{}, early{}, used{};
Denial denial{};
unsigned initialized{}, guards{};
Task* early_task{};
void* early_stack{};
std::vector<std::string> events;
auto initialized_pid() -> uint64_t {
    if (active) ++initialized;
    return 1;
}
void guard() {
    if (active) ++guards;
}
bool refuse(std::size_t size, bool array) {
    if (!active || used) return false;
    bool deny = (denial == Denial::Task && !array && size == sizeof(Task)) || (denial == Denial::Name && array && size == 22) ||
                (denial == Denial::Thread && !array && size == sizeof(ker::mod::sched::threading::Thread)) ||
                (denial == Denial::PerCpu && !array && size == sizeof(ker::mod::cpu::PerCpu));
    if (deny) {
        used = true;
        events.push_back("denied");
    }
    return deny;
}
[[gnu::noinline]] void* null_result() { return nullptr; }
void note_delete(void* p) {
    if (!early || !p) return;
    if (p == early_task) {
        events.push_back("task");
        early_task = nullptr;
    }
}
bool free_stack(void* p) {
    if (!early || p != early_stack || !p) return false;
    events.push_back("stack");
    early_stack = nullptr;
    std::free(p);
    return true;
}
auto finish(int64_t status, uint64_t bytes, const char* stage) -> uint64_t {
    if (!early) return host_fork_public::finish(status, bytes, stage);
    EXPECT_EQ(early_stack, nullptr);
    EXPECT_EQ(early_task, nullptr);
    events.push_back("finish");
    EXPECT_FALSE(host_range::interrupts);
    return static_cast<uint64_t>(status);
}
void adopt_task(Task* t) { early_task = t; }
void adopt_thread(Task* t) { host_fork_public::thread_token = t->thread; }
void adopt_percpu(void* p) { host_fork_public::percpu_token = p; }
void start(Denial d, bool is_early = false) {
    active = true;
    early = is_early;
    denial = d;
    used = false;
    initialized = guards = 0;
    events.clear();
}
void settle_early() {
    active = false;
    if (early_task) {
        delete[] early_task->name;
        delete early_task;
    }
    if (early_stack) {
        std::free(early_stack);
        early_stack = nullptr;
    }
    early = false;
    denial = Denial::None;
    used = false;
}
}  // namespace host_fork_alloc
namespace ker::syscall::process {
auto host_allocation_task(ker::mod::sched::task::Task*, uint64_t, uint64_t) -> uint64_t;
auto host_allocation_name(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, uint64_t) -> uint64_t;
auto host_allocation_thread(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, uint64_t) -> uint64_t;
auto host_allocation_percpu(ker::mod::sched::task::Task*, ker::mod::sched::task::Task*, uint64_t, uint64_t) -> uint64_t;
}  // namespace ker::syscall::process
namespace {
TEST_F(PublicForkWiring, AllocationTaskDenialSkipsInitialization) {
    using namespace host_fork_alloc;
    start(Denial::Task, true);
    early_stack = std::malloc(64);
    host_range::interrupts = false;
    auto result = ker::syscall::process::host_allocation_task(&actor, reinterpret_cast<uint64_t>(early_stack), 123456);
    active = false;
    host_range::interrupts = true;
    EXPECT_EQ(static_cast<int64_t>(result), -ENOMEM);
    EXPECT_TRUE(used);
    EXPECT_EQ(initialized, 0U);
    EXPECT_EQ(guards, 0U);
    EXPECT_EQ(events, (std::vector<std::string>{"denied", "stack", "finish"}));
}
TEST_F(PublicForkWiring, AllocationNameDenialDropsOnlyEarlyPrefix) {
    using namespace host_fork_alloc;
    actor.name = const_cast<char*>("fork-allocation-name!");
    early_task = new Task;
    early_stack = std::malloc(64);
    start(Denial::Name, true);
    host_range::interrupts = false;
    auto result = ker::syscall::process::host_allocation_name(&actor, early_task, reinterpret_cast<uint64_t>(early_stack), 123456);
    active = false;
    host_range::interrupts = true;
    EXPECT_EQ(static_cast<int64_t>(result), -ENOMEM);
    EXPECT_TRUE(used);
    EXPECT_EQ(guards, 0U);
    EXPECT_EQ(initialized, 0U);
    EXPECT_EQ(events, (std::vector<std::string>{"denied", "stack", "task", "finish"}));
    EXPECT_EQ(fork_exclusives, 0U);
    EXPECT_EQ(host_fork_public::successes, 0U);
}
TEST_F(PublicForkWiring, AllocationThreadDenialDropsMixedVmPrefix) {
    using namespace host_fork_alloc;
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    shared();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    ASSERT_EQ(caller(), 0);
    ker::mod::sched::threading::Thread parent_thread{};
    actor.thread = &parent_thread;
    auto* task = host_fork_public::subject;
    start(Denial::Thread);
    host_range::interrupts = false;
    auto result =
        ker::syscall::process::host_allocation_thread(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456);
    active = false;
    host_range::interrupts = true;
    actor.thread = nullptr;
    EXPECT_EQ(static_cast<int64_t>(result), -ENOMEM);
    EXPECT_TRUE(used);
    EXPECT_EQ(guards, 0U);
    EXPECT_EQ(host_fork_public::events,
              (std::vector<std::string>{"file", "lazy", "root", "root-release", "name", "stack", "task", "finish"}));
    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
    EXPECT_GE(actual_flushes, 1U);
}
TEST_F(PublicForkWiring, AllocationPerCpuDenialDropsMixedVmAndThreadPrefix) {
    using namespace host_fork_alloc;
    using namespace ker::syscall::process;
    ordinary(TEST_VA);
    legacy();
    tmpfs();
    shared();
    marker(TEST_VA + 20 * PAGE, virt::AnonymousSwapRuntimeState::SWAPPED, true);
    ASSERT_EQ(caller(), 0);
    ker::mod::sched::threading::Thread parent_thread{};
    actor.thread = &parent_thread;
    auto* task = host_fork_public::subject;
    ASSERT_EQ(host_allocation_thread(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456), 0U);
    start(Denial::PerCpu);
    host_range::interrupts = false;
    auto result = host_allocation_percpu(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456);
    active = false;
    host_range::interrupts = true;
    actor.thread = nullptr;
    EXPECT_EQ(static_cast<int64_t>(result), -ENOMEM);
    EXPECT_TRUE(used);
    EXPECT_EQ(guards, 0U);
    EXPECT_EQ(host_fork_public::events,
              (std::vector<std::string>{"thread", "file", "lazy", "root", "root-release", "name", "stack", "task", "finish"}));
    EXPECT_EQ(shm::g_segments[0].attach_count, 1U);
    EXPECT_GE(actual_flushes, 1U);
}
TEST_F(PublicForkWiring, AllocationPerCpuDenialWithoutThreadOwnsNoExtraPrefix) {
    using namespace host_fork_alloc;
    ASSERT_EQ(caller(), 0);
    auto* task = host_fork_public::subject;
    start(Denial::PerCpu);
    host_range::interrupts = false;
    auto result =
        ker::syscall::process::host_allocation_percpu(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456);
    active = false;
    host_range::interrupts = true;
    EXPECT_EQ(static_cast<int64_t>(result), -ENOMEM);
    EXPECT_TRUE(used);
    EXPECT_EQ(guards, 0U);
    EXPECT_EQ(host_fork_public::events,
              (std::vector<std::string>{"file", "lazy", "root", "root-release", "name", "stack", "task", "finish"}));
}
TEST_F(PublicForkWiring, AllocationSuccessInitializesAndAdoptsActualMetadata) {
    using namespace host_fork_alloc;
    using namespace ker::syscall::process;
    start(Denial::None, true);
    early_stack = std::malloc(64);
    ASSERT_EQ(host_allocation_task(&actor, reinterpret_cast<uint64_t>(early_stack), 123456), 0U);
    EXPECT_EQ(initialized, 1U);
    actor.name = const_cast<char*>("fork-allocation-name!");
    ASSERT_EQ(host_allocation_name(&actor, early_task, reinterpret_cast<uint64_t>(early_stack), 123456), 0U);
    EXPECT_STREQ(early_task->name, actor.name);
    settle_early();
    ASSERT_EQ(caller(), 0);
    auto* task = host_fork_public::subject;
    ker::mod::sched::threading::Thread parent_thread{};
    parent_thread.fsbase = 0x123456;
    parent_thread.tls_phys_ptr = 99;
    parent_thread.stack_phys_ptr = 100;
    actor.thread = &parent_thread;
    start(Denial::None);
    ASSERT_EQ(host_allocation_thread(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456), 0U);
    EXPECT_EQ(task->thread->fsbase, parent_thread.fsbase);
    EXPECT_EQ(task->thread->tls_phys_ptr, 0U);
    EXPECT_EQ(task->thread->stack_phys_ptr, 0U);
    auto const old_cpu_depth = backend().expected_cpu_depth;
    backend().expected_cpu_depth = backend().preempt_depth;
    ASSERT_EQ(host_allocation_percpu(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), 123456), 0U);
    backend().expected_cpu_depth = old_cpu_depth;
    EXPECT_EQ(guards, 1U);
    auto* scratch = reinterpret_cast<ker::mod::cpu::PerCpu*>(task->context.syscall_scratch_area);
    EXPECT_EQ(scratch->syscall_stack, 123456U);
    EXPECT_EQ(scratch->cpu_id, backend().current_cpu);
    EXPECT_EQ(scratch->stack_guard, 0x48b4U);
    active = false;
    host_range::interrupts = false;
    (void)host_public_cleanup_fd(&actor, task, reinterpret_cast<uint64_t>(host_fork_public::stack_token), false);
    host_range::interrupts = true;
    actor.thread = nullptr;
    EXPECT_EQ(host_fork_public::subject, nullptr);
    EXPECT_EQ(host_fork_public::percpu_token, nullptr);
}
}  // namespace
