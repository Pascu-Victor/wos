#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dev/pty.hpp>
#include <initializer_list>
#include <limits>
#include <memory>
#include <platform/debug/ptrace.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/session.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/process.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/filesystem_context.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/lookup.hpp>
#include <vfs/mount.hpp>

namespace abi = ker::abi;
namespace ipc = ker::mod::sched::ipc;
namespace resource = ker::mod::sched::resource;
using ker::mod::sched::task::Task;

namespace pid_ns = ker::mod::sched::pid_ns;
namespace sched = ker::mod::sched;
namespace session = ker::mod::sched::session;
namespace child_events = ker::syscall::process::child_events;

KTEST(CompartmentPid, RetainedPoolAndMonotonicIdsDrainWithoutRetargeting) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        std::array<pid_ns::NamespaceRef, pid_ns::MAX_NAMESPACES> scopes;
        auto const AVAILABLE = pid_ns::MAX_NAMESPACES - BASELINE;
        KREQUIRE_NE(AVAILABLE, size_t{0});
        for (size_t i = 0; i < AVAILABLE; ++i) KREQUIRE_EQ(pid_ns::create_namespace(scopes[i]), 0);
        pid_ns::NamespaceRef output;
        KREQUIRE_EQ(scopes[0].retain(output), 0);
        auto const ORIGINAL = output.generation();
        KEXPECT_EQ(pid_ns::create_namespace(output), -ENOSPC);
        KEXPECT_EQ(output.generation(), ORIGINAL);
        uint32_t id = 99;
        pid_ns::NamespaceRef host;
        KEXPECT_EQ(host.allocate_id(id), -EINVAL);
        KEXPECT_EQ(id, uint32_t{99});
        KEXPECT_EQ(output.allocate_id(id), 0);
        KEXPECT_EQ(id, uint32_t{1});
        KEXPECT_EQ(scopes[0].allocate_id(id), 0);
        KEXPECT_EQ(id, uint32_t{2});
        output.close();
        KEXPECT_TRUE(scopes[0].closed());
        KEXPECT_EQ(output.allocate_id(id), -EIDRM);
        KEXPECT_EQ(id, uint32_t{2});
        KEXPECT_EQ(output.retain(output), 0);
        KEXPECT_EQ(output.generation(), ORIGINAL);
        auto moved = std::move(output);
        KEXPECT_TRUE(output.is_host());
        KEXPECT_TRUE(moved.closed());
        KEXPECT_EQ(moved.generation(), ORIGINAL);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    uint64_t previous = 0;
    // Native owner/index foundation stress, not the required public
    // create/exec/kill/destroy service lifecycle campaign.
    for (size_t i = 0; i < 1000; ++i) {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        KEXPECT_TRUE(scope.generation() > previous);
        previous = scope.generation();
        uint32_t id = 0;
        KEXPECT_EQ(scope.allocate_id(id), 0);
        KEXPECT_EQ(id, uint32_t{1});
        scope.close();
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPid, CanonicalChildAndThreadIdsPreserveOwnerAdmission) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        auto owner = std::make_unique<Task>();
        owner->pid = sched::task::get_next_pid();
        KREQUIRE_TRUE(owner->initialize_pid_namespace(scope));
        KEXPECT_EQ(owner->namespace_tid(), uint64_t{1});
        auto thread = std::make_unique<Task>();
        thread->pid = sched::task::get_next_pid();
        thread->is_thread = true;
        thread->owner_pid = owner->pid;
        KEXPECT_NULL(thread->pid_namespace());
        KEXPECT_EQ(thread->namespace_tid(), uint64_t{0});
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *owner));
        KEXPECT_TRUE(thread->pid_namespace() == owner->pid_namespace());
        KEXPECT_EQ(thread->namespace_tid(), uint64_t{2});
        KEXPECT_FALSE(resource::bind_thread_owner(*thread, *owner));
        KEXPECT_EQ(thread->namespace_tid(), uint64_t{2});
        KEXPECT_EQ(owner->resource_thread_owners.load(), uint64_t{1});
        auto child = std::make_unique<Task>();
        KREQUIRE_TRUE(resource::inherit(*child, *thread));
        KEXPECT_TRUE(*child->pid_namespace() == scope);
        KEXPECT_EQ(child->namespace_tid(), uint64_t{3});
        KEXPECT_FALSE(child->initialize_pid_namespace(scope));
        // Force a late PID-installation failure after resource attachment;
        // immutable owner/ref/CPU contribution must all unwind immediately.
        auto rejected = std::make_unique<Task>();
        KREQUIRE_TRUE(rejected->initialize_pid_namespace(scope));
        rejected->is_thread = true;
        rejected->owner_pid = owner->pid;
        auto const REFERENCES = owner->ref_count.load();
        KEXPECT_FALSE(resource::bind_thread_owner(*rejected, *owner));
        KEXPECT_NULL(rejected->resource_owner.get());
        KEXPECT_EQ(owner->ref_count.load(), REFERENCES);
        KEXPECT_EQ(owner->resource_cpu_threads.load(), uint64_t{1});
        scope.close();
        auto late = std::make_unique<Task>();
        late->is_thread = true;
        late->owner_pid = owner->pid;
        KEXPECT_FALSE(resource::bind_thread_owner(*late, *owner));
        KEXPECT_NULL(late->resource_owner.get());
        auto denied = std::make_unique<Task>();
        KEXPECT_FALSE(resource::inherit(*denied, *thread));
        KEXPECT_TRUE(denied->pid_namespace()->is_host());
        // Exact retained metadata remains observable for teardown.
        KEXPECT_TRUE(thread->pid_namespace()->closed());
        KEXPECT_EQ(thread->namespace_tid(), uint64_t{2});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    auto host = std::make_unique<Task>();
    auto child = std::make_unique<Task>();
    KREQUIRE_TRUE(resource::inherit(*child, *host));
    child->pid = sched::task::get_next_pid();
    KEXPECT_TRUE(child->pid_namespace()->is_host());
    KEXPECT_EQ(child->namespace_tid(), child->pid);
    host->pid = sched::task::get_next_pid();
    {
        auto retired = std::make_unique<Task>();
        retired->is_thread = true;
        retired->pid = sched::task::get_next_pid();
        retired->owner_pid = host->pid;
        retired->state.store(sched::task::TaskState::EXITING);
        KREQUIRE_TRUE(resource::bind_thread_owner(*retired, *host));
        KEXPECT_TRUE(retired->pid_namespace()->is_host());
        KEXPECT_EQ(retired->namespace_tid(), retired->pid);
        KEXPECT_FALSE(resource::bind_thread_owner(*retired, *host));
    }
    KEXPECT_EQ(host->ref_count.load(), uint32_t{1});
    KEXPECT_EQ(host->resource_cpu_threads.load(), uint64_t{0});
}

namespace {
struct RegistryTask {
    std::unique_ptr<Task> task = std::make_unique<Task>();
    bool registered{};
    RegistryTask() {
        task->pid = sched::task::get_next_pid();
        task->type = sched::task::TaskType::PROCESS;
    }
    ~RegistryTask() {
        if (registered) sched::scheduler_selftest_unregister_unpublished_task(*task);
        KEXPECT_EQ(task->active_registry_index, Task::ACTIVE_REGISTRY_INDEX_INVALID);
        KEXPECT_EQ(task->ref_count.load(), uint32_t{1});
    }
    auto publish() -> bool {
        if (registered) return false;
        registered = sched::scheduler_selftest_register_unpublished_task(*task);
        return registered;
    }
    void retire() {
        if (!registered) return;
        sched::scheduler_selftest_unregister_unpublished_task(*task);
        registered = false;
    }
    auto retire_if_unreferenced() -> bool {
        if (!registered || !sched::scheduler_selftest_withdraw_unpublished_gc_task(*task)) return false;
        registered = false;
        return true;
    }
};
struct PublicationRollback {
    Task& child;
    ~PublicationRollback() { child_events::abort_publication(child); }
};
struct WaitMembership {
    Task& parent;
    Task& child;
    ~WaitMembership() {
        child_events::cancel_wait(parent);
        child_events::cancel_wait(child);
        child_events::detach_tracer(child);
        child_events::abort_publication(child);
        if (child.child_parent == &parent) {
            KEXPECT_TRUE(child_events::publish_exit(child));
            child_events::Claim claim;
            child_events::ClaimedEvent event;
            if (child_events::try_claim(parent, claim, child.pid, 0, event) == child_events::ProbeResult::CLAIMED)
                KEXPECT_TRUE(child_events::commit_claim(parent, claim, event, false));
            child_events::cancel_claim(claim);
        }
        KEXPECT_NULL(child.child_parent);
        KEXPECT_NULL(child.child_tracer);
        KEXPECT_EQ(child.child_membership_state.load(), sched::task::ChildMembershipState::UNLINKED);
    }
    auto publish() -> bool { return child_events::begin_publication(parent, child) && child_events::commit_publication(child); }
};
struct WaitClaim {
    child_events::Claim value;
    ~WaitClaim() { child_events::cancel_claim(value); }
};
struct WaitRegistration {
    Task& waiter;
    ~WaitRegistration() { child_events::cancel_wait(waiter); }
};
void make_wait_fixtures_inert(std::initializer_list<Task*> tasks) {
    for (auto* task : tasks) task->state.store(sched::task::TaskState::EXITING);
}
void expect_visible(const Task& requester, uint64_t id, Task* expected, bool lifetime = false) {
    auto* found =
        lifetime ? sched::find_task_by_visible_pid_lifetime_ref(requester, id) : sched::find_task_by_visible_pid_safe(requester, id);
    KEXPECT_TRUE(found == expected);
    if (found != nullptr) found->release();
}
}  // namespace

namespace {
void hide_signal_fixtures(std::initializer_list<Task*> tasks) {
    for (auto* task : tasks) task->hide_group_signals_until_publication();
}
auto private_signal(Task& requester, int64_t id, int signum) -> int {
    return ker::syscall::process::signal_in_namespace(requester, id, signum);
}
}  // namespace

KTEST(CompartmentSignal, PositiveLocalNamesExcludeSiblingHostAndPhysicalAliases) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const ACTIVE = sched::get_active_task_count();
    auto const INDEXED = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef a, b;
        KREQUIRE_EQ(pid_ns::create_namespace(a), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(b), 0);
        RegistryTask caller, target, sibling, foreign, host, thread;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(a));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(a));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(b));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(b));
        thread.task->is_thread = true;
        thread.task->owner_pid = target.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *target.task));
        hide_signal_fixtures(
            {caller.task.get(), target.task.get(), sibling.task.get(), foreign.task.get(), host.task.get(), thread.task.get()});
        for (auto* fixture : {&caller, &target, &sibling, &foreign, &host, &thread}) KREQUIRE_TRUE(fixture->publish());
        KEXPECT_EQ(target.task->namespace_tid(), foreign.task->namespace_tid());
        KEXPECT_EQ(private_signal(*caller.task, 2, 0), 0);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{0});
        KEXPECT_EQ(private_signal(*caller.task, 2, 10), 0);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{1} << 9);
        KEXPECT_EQ(private_signal(*caller.task, static_cast<int64_t>(thread.task->namespace_tid()), 12), 0);
        KEXPECT_EQ(thread.task->signal_pending_bits(), uint64_t{1} << 11);
        for (auto* other : {foreign.task.get(), sibling.task.get(), host.task.get()}) {
            KEXPECT_EQ(private_signal(*caller.task, static_cast<int64_t>(other->pid), 10), -ESRCH);
            KEXPECT_EQ(other->signal_pending_bits(), uint64_t{0});
        }
        KEXPECT_EQ(private_signal(*caller.task, static_cast<int64_t>(target.task->pid), 10), -ESRCH);
        KEXPECT_EQ(private_signal(*caller.task, 2, -1), -EINVAL);
        KEXPECT_EQ(private_signal(*caller.task, 2, 65), -EINVAL);
        KEXPECT_EQ(private_signal(*caller.task, std::numeric_limits<int64_t>::min(), 10), -ESRCH);
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
        KEXPECT_TRUE(thread.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), INDEXED);
}

KTEST(CompartmentSignal, DurableLocalGroupsSurviveLeaderDestructionWithoutForeignDelivery) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef a, b;
        KREQUIRE_EQ(pid_ns::create_namespace(a), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(b), 0);
        RegistryTask caller, member, foreign, host, thread;
        auto leader = std::make_unique<RegistryTask>();
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(a));
        KREQUIRE_TRUE(leader->task->initialize_pid_namespace(a));
        auto const CALLER_JOB = caller.task->job_identity_snapshot();
        leader->task->initialize_job_identity(CALLER_JOB.internal, CALLER_JOB.visible);
        KREQUIRE_EQ(session::create(*leader->task), int64_t{2});
        auto const GROUP = leader->task->job_identity_snapshot();
        KREQUIRE_TRUE(member.task->initialize_pid_namespace(a));
        member.task->initialize_job_identity(GROUP.internal, GROUP.visible);
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(b));
        foreign.task->initialize_job_identity(GROUP.internal, GROUP.visible);
        host.task->initialize_job_identity(GROUP.internal);
        thread.task->is_thread = true;
        thread.task->owner_pid = member.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *member.task));
        hide_signal_fixtures(
            {caller.task.get(), member.task.get(), foreign.task.get(), host.task.get(), thread.task.get(), leader->task.get()});
        for (auto* fixture : {&caller, &member, &foreign, &host, &thread, leader.get()}) KREQUIRE_TRUE(fixture->publish());
        leader.reset();
        KEXPECT_EQ(private_signal(*caller.task, -2, 10), 0);
        KEXPECT_EQ(member.task->signal_pending_bits(), uint64_t{1} << 9);
        KEXPECT_EQ(thread.task->signal_pending_bits(), uint64_t{1} << 9);
        KEXPECT_EQ(foreign.task->signal_pending_bits(), uint64_t{0});
        KEXPECT_EQ(host.task->signal_pending_bits(), uint64_t{0});
        member.task->signal_clear_pending_mask(~uint64_t{0});
        thread.task->signal_clear_pending_mask(~uint64_t{0});
        KEXPECT_EQ(private_signal(*caller.task, -2, 19), 0);
        KEXPECT_EQ(member.task->signal_pending_bits(), uint64_t{1} << 18);
        KEXPECT_EQ(thread.task->signal_pending_bits(), uint64_t{0});
        for (int64_t id : {int64_t{0}, int64_t{-1}}) {
            caller.task->signal_clear_pending_mask(~uint64_t{0});
            KEXPECT_EQ(private_signal(*caller.task, id, 12), 0);
            KEXPECT_EQ(caller.task->signal_pending_bits(), uint64_t{1} << 11);
            KEXPECT_EQ(thread.task->signal_pending_bits(), uint64_t{0});
        }
        KEXPECT_EQ(private_signal(*caller.task, -9999, 10), -ESRCH);
        KEXPECT_TRUE(member.task->local_signal_admissions_drained());
        KEXPECT_TRUE(thread.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentSignal, CredentialsSavedIdsAndContinueUseCurrentExactAdmission) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask caller, target;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(scope));
        auto const JOB = caller.task->job_identity_snapshot();
        target.task->initialize_job_identity(JOB.internal, JOB.visible);
        ker::mod::security::Snapshot user, other, saved;
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {100, 100, 100}, .group = {100, 100, 100}}, user), 0);
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {200, 200, 200}, .group = {200, 200, 200}}, other), 0);
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {200, 200, 100}, .group = {200, 200, 200}}, saved), 0);
        KREQUIRE_EQ(resource::initialize_credentials(*caller.task, user), 0);
        KREQUIRE_EQ(resource::initialize_credentials(*target.task, other), 0);
        hide_signal_fixtures({caller.task.get(), target.task.get()});
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(target.publish());
        KEXPECT_EQ(private_signal(*caller.task, 2, 0), -EPERM);
        KEXPECT_EQ(private_signal(*caller.task, 2, 12), -EPERM);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{0});
        KEXPECT_EQ(private_signal(*caller.task, 2, 18), 0);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{1} << 17);
        target.task->signal_clear_pending_mask(~uint64_t{0});
        auto changed = JOB.internal;
        changed.session_id = target.task->pid;
        KREQUIRE_TRUE(child_events::update_job_identity(*target.task, changed, session::Names{2, 1}));
        KEXPECT_EQ(private_signal(*caller.task, 2, 18), -EPERM);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{0});
        // These are exclusively owned, unscheduled construction fixtures.
        KREQUIRE_EQ(resource::initialize_credentials(*target.task, saved), 0);
        KEXPECT_EQ(private_signal(*caller.task, 2, 12), 0);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{1} << 11);
        KEXPECT_TRUE(target.task->credentials() == saved);
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentSignal, ClosedUnboundAndRemoteAdmissionNeverChangePending) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask caller, target;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(scope));
        hide_signal_fixtures({caller.task.get(), target.task.get()});
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(target.publish());
        auto unbound = std::make_unique<Task>();
        unbound->is_thread = true;
        KEXPECT_EQ(private_signal(*unbound, 2, 10), -ESRCH);
        // A retained owner alone does not publish the thread's local identity.
        KREQUIRE_TRUE(unbound->resource_owner.attach(caller.task.get()));
        KEXPECT_EQ(unbound->namespace_tid(), uint64_t{0});
        KEXPECT_EQ(private_signal(*unbound, 2, 10), -ESRCH);
        target.task->set_wki_proxy_task_id(42);
        KEXPECT_EQ(private_signal(*caller.task, 2, 0), -ENOTSUP);
        KEXPECT_EQ(private_signal(*caller.task, 2, 10), -ENOTSUP);
        target.task->set_wki_proxy_task_id(0);
        KEXPECT_EQ(private_signal(*caller.task, 2, 0), 0);
        target.task->publish_accepted_signal_route(43, 123);
        KEXPECT_EQ(private_signal(*caller.task, 2, 10), -ENOTSUP);
        KEXPECT_EQ(private_signal(*caller.task, -2, 10), -ENOTSUP);
        target.task->state.store(sched::task::TaskState::EXITING);
        KEXPECT_EQ(private_signal(*caller.task, 2, 10), -ESRCH);
        target.task->state.store(sched::task::TaskState::ACTIVE);
        scope.close();
        KEXPECT_EQ(private_signal(*caller.task, 2, 10), -EIDRM);
        KEXPECT_EQ(private_signal(*caller.task, -2, 10), -EIDRM);
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{0});
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentSignal, RetiredThreadImagesCannotReceiveLocalSignals) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask caller, owner, thread;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(scope));
        thread.task->is_thread = true;
        thread.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *owner.task));
        hide_signal_fixtures({caller.task.get(), owner.task.get(), thread.task.get()});
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(owner.publish());
        KREQUIRE_TRUE(thread.publish());
        auto first = std::make_unique<ker::mod::mm::paging::PageTable>();
        auto retired = std::make_unique<ker::mod::mm::paging::PageTable>();
        owner.task->pagemap = first.get();
        thread.task->pagemap = retired.get();
        KEXPECT_EQ(private_signal(*caller.task, 3, 10), -ESRCH);
        KEXPECT_EQ(thread.task->signal_pending_bits(), uint64_t{0});
        thread.task->pagemap = first.get();
        KEXPECT_EQ(private_signal(*caller.task, 3, 0), 0);
        thread.task->pagemap = nullptr;
        owner.task->pagemap = nullptr;
        KEXPECT_TRUE(thread.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentSignal, LargeNativeCohortDrainsRegistryReferences) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const ACTIVE = sched::get_active_task_count();
    auto const INDEXED = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask caller;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(scope));
        caller.task->hide_group_signals_until_publication();
        KREQUIRE_TRUE(caller.publish());
        auto const JOB = caller.task->job_identity_snapshot();
        std::array<RegistryTask, 40> members;
        for (auto& member : members) {
            KREQUIRE_TRUE(member.task->initialize_pid_namespace(scope));
            member.task->initialize_job_identity(JOB.internal, JOB.visible);
            member.task->hide_group_signals_until_publication();
            KREQUIRE_TRUE(member.publish());
        }
        KEXPECT_EQ(private_signal(*caller.task, 0, 10), 0);
        KEXPECT_EQ(caller.task->signal_pending_bits(), uint64_t{1} << 9);
        for (auto const& member : members) {
            KEXPECT_EQ(member.task->signal_pending_bits(), uint64_t{1} << 9);
            KEXPECT_EQ(member.task->ref_count.load(), uint32_t{1});
            KEXPECT_TRUE(member.task->local_signal_admissions_drained());
            KEXPECT_FALSE(member.task->group_signal_visible());
        }
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), INDEXED);
}

KTEST(CompartmentSignal, ClosureRouteAndLocalGroupChangesLinearizeWithAdmission) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_namespace_signal_interleaving());
    KEXPECT_TRUE(ker::syscall::process::process_selftest_signal_snapshot_ownership());
}

namespace {
namespace ptrace = ker::mod::debug::ptrace;
using PtraceRequest = abi::ptrace::request;
auto scoped_ptrace(Task& tracer, PtraceRequest request, uint64_t id, uint64_t data = 0) -> int64_t {
    return static_cast<int64_t>(ptrace::ptrace_as(tracer, request, id, 0, data));
}
// Membership cleanup must never make these unscheduled Tasks runnable, even
// when a REQUIRE returns before the last assertion.
struct InertOnExit {
    std::initializer_list<Task*> tasks;
    ~InertOnExit() { make_wait_fixtures_inert(tasks); }
};
struct PtraceUnlink {
    Task& target;
    ~PtraceUnlink() { child_events::detach_tracer(target); }
};
}  // namespace

KTEST(CompartmentPtrace, LocalSelectorsPreserveCredentialsAndHostInspection) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask caller, target, sibling, foreign, host;
        PtraceUnlink const TRACE_CLEANUP{*target.task};
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(target.publish());
        KREQUIRE_TRUE(sibling.publish());
        KREQUIRE_TRUE(foreign.publish());
        KREQUIRE_TRUE(host.publish());
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, target.task->pid), -int64_t{ESRCH});
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, foreign.task->pid), -int64_t{ESRCH});
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, host.task->pid), -int64_t{ESRCH});
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, 1), -int64_t{EPERM});
        KREQUIRE_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, 2), 0);
        KEXPECT_TRUE(target.task->child_tracer == caller.task.get());
        KEXPECT_EQ(target.task->ptrace_tracer_pid, caller.task->pid);
        KEXPECT_FALSE(foreign.task->ptrace_traced);
        KREQUIRE_EQ(scoped_ptrace(*caller.task, PtraceRequest::SETOPTIONS, 2, 0x41), 0);
        KEXPECT_EQ(target.task->ptrace_options, uint64_t{0x41});
        KREQUIRE_EQ(scoped_ptrace(*caller.task, PtraceRequest::DETACH, 2), 0);
        KEXPECT_NULL(target.task->child_tracer);
        ker::mod::security::Snapshot user, other;
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {100, 100, 100}, .group = {100, 100, 100}}, user), 0);
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {200, 200, 200}, .group = {200, 200, 200}}, other), 0);
        KREQUIRE_EQ(resource::initialize_credentials(*caller.task, user), 0);
        KREQUIRE_EQ(resource::initialize_credentials(*target.task, other), 0);
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::SEIZE, 2), -int64_t{EPERM});
        KEXPECT_FALSE(target.task->ptrace_traced);
        KREQUIRE_EQ(scoped_ptrace(*host.task, PtraceRequest::SEIZE, target.task->pid), 0);
        KEXPECT_EQ(target.task->ptrace_tracer_pid, host.task->pid);
        KREQUIRE_EQ(scoped_ptrace(*host.task, PtraceRequest::DETACH, target.task->pid), 0);
        // Both positive self lookups own and release their registry reference.
        KEXPECT_EQ(scoped_ptrace(*host.task, PtraceRequest::GET_REMOTE_INFO, host.task->pid), -int64_t{EFAULT});
        KEXPECT_EQ(scoped_ptrace(*caller.task, PtraceRequest::GET_REMOTE_INFO, 1), -int64_t{EFAULT});
        KEXPECT_EQ(caller.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(target.task->ref_count.load(), uint32_t{1});
        KEXPECT_TRUE(first.operations_drained());
        KEXPECT_TRUE(second.operations_drained());
        KEXPECT_NULL(caller.task->pending_ptrace_operations);
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPtrace, TracemeUsesCanonicalRetainedParent) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope, other;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(other), 0);
        RegistryTask parent, child, foreign, host, thread, init;
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(resource::inherit(*child.task, *parent.task));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(other));
        thread.task->is_thread = true;
        thread.task->owner_pid = child.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *child.task));
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(other));
        WaitMembership membership{*parent.task, *child.task};
        WaitMembership supervisor{*host.task, *foreign.task};
        InertOnExit const INERT{
            {parent.task.get(), child.task.get(), foreign.task.get(), host.task.get(), thread.task.get(), init.task.get()}};
        PtraceUnlink const CHILD_TRACE{*child.task}, THREAD_TRACE{*thread.task};
        KREQUIRE_TRUE(membership.publish());
        KREQUIRE_TRUE(supervisor.publish());
        child.task->parent_pid = foreign.task->pid;
        thread.task->parent_pid = host.task->pid;
        KREQUIRE_EQ(scoped_ptrace(*child.task, PtraceRequest::TRACEME, 0), 0);
        KEXPECT_TRUE(child.task->child_tracer == parent.task.get());
        KEXPECT_EQ(child.task->ptrace_tracer_pid, parent.task->pid);
        child_events::detach_tracer(*child.task);
        child.task->ptrace_traced = false;
        child.task->ptrace_tracer_pid = 0;
        KREQUIRE_EQ(scoped_ptrace(*thread.task, PtraceRequest::TRACEME, 0), 0);
        KEXPECT_TRUE(thread.task->child_tracer == parent.task.get());
        KEXPECT_EQ(thread.task->ptrace_tracer_pid, parent.task->pid);
        child_events::detach_tracer(*thread.task);
        thread.task->ptrace_traced = false;
        thread.task->ptrace_tracer_pid = 0;
        KEXPECT_EQ(scoped_ptrace(*foreign.task, PtraceRequest::TRACEME, 0), -int64_t{ESRCH});
        KEXPECT_EQ(scoped_ptrace(*init.task, PtraceRequest::TRACEME, 0), -int64_t{ESRCH});
        KEXPECT_FALSE(foreign.task->ptrace_traced);
        KEXPECT_TRUE(scope.operations_drained());
        KEXPECT_TRUE(other.operations_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPtrace, ClosedRemoteAndRetiredImagesFailBeforeEffects) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask caller, target, thread;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(scope));
        thread.task->is_thread = true;
        thread.task->owner_pid = target.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *target.task));
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(target.publish());
        KREQUIRE_TRUE(thread.publish());
        auto const REQUESTS = std::array{PtraceRequest::SEIZE,      PtraceRequest::SETOPTIONS,      PtraceRequest::KILL,
                                         PtraceRequest::CONT,       PtraceRequest::PEEKDATA,        PtraceRequest::POKEDATA,
                                         PtraceRequest::GETREGSET,  PtraceRequest::SETREGSET,       PtraceRequest::LIST_THREADS,
                                         PtraceRequest::GET_IMAGES, PtraceRequest::GET_REMOTE_INFO, PtraceRequest::SYSCALL_WAIT};
        target.task->set_wki_proxy_task_id(42);
        for (auto request : REQUESTS) KEXPECT_EQ(scoped_ptrace(*caller.task, request, 2), -int64_t{ENOTSUP});
        target.task->set_wki_proxy_task_id(0);
        auto unbound = std::make_unique<Task>();
        unbound->is_thread = true;
        KEXPECT_EQ(scoped_ptrace(*unbound, PtraceRequest::SEIZE, 2), -int64_t{ESRCH});
        KREQUIRE_TRUE(unbound->resource_owner.attach(caller.task.get()));
        KEXPECT_EQ(scoped_ptrace(*unbound, PtraceRequest::SEIZE, 2), -int64_t{ESRCH});
        thread.task->pagemap = reinterpret_cast<decltype(thread.task->pagemap)>(uintptr_t{1});
        // Restore before any assertion can return and before Task destruction.
        auto const STALE = scoped_ptrace(*caller.task, PtraceRequest::SEIZE, 3);
        thread.task->pagemap = nullptr;
        KEXPECT_EQ(STALE, -int64_t{ESRCH});
        target.task->publish_accepted_signal_route(43, 123);
        for (auto request : REQUESTS) KEXPECT_EQ(scoped_ptrace(*caller.task, request, 2), -int64_t{ENOTSUP});
        scope.close();
        for (auto request : REQUESTS) KEXPECT_EQ(scoped_ptrace(*caller.task, request, 2), -int64_t{EIDRM});
        KEXPECT_FALSE(target.task->ptrace_traced);
        KEXPECT_NULL(target.task->child_tracer);
        KEXPECT_EQ(target.task->ptrace_options, uint64_t{0});
        KEXPECT_EQ(target.task->signal_pending_bits(), uint64_t{0});
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
        KEXPECT_TRUE(scope.operations_drained());
        KEXPECT_NULL(caller.task->pending_ptrace_operations);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPtrace, ClosureAndRoutesLinearizeWithEffectAdmission) {
    KEXPECT_TRUE(ptrace::ptrace_selftest_namespace_effect_lifetime());
}
KTEST(CompartmentPtrace, ClaimsAndThreadObservationsUseLocalNames) { KEXPECT_TRUE(ptrace::ptrace_selftest_namespace_observations()); }
KTEST(CompartmentPtrace, RegisterAndImageEffectsRecheckScopeAndCredentials) {
    KEXPECT_TRUE(ptrace::ptrace_selftest_namespace_register_images());
}
KTEST(CompartmentPtrace, FatalAndOrdinaryOwnershipDrainExactlyOnce) { KEXPECT_TRUE(ptrace::ptrace_selftest_operation_cleanup()); }

KTEST(CompartmentWait, SameNumbersAndForeignMembershipNeverAuthorizePublicWait) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef a, b;
        KREQUIRE_EQ(pid_ns::create_namespace(a), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(b), 0);
        RegistryTask parent, sibling, child, foreign;
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(a));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(b));
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(a));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(b));
        KEXPECT_EQ(child.task->namespace_tid(), foreign.task->namespace_tid());
        make_wait_fixtures_inert({parent.task.get(), sibling.task.get(), child.task.get(), foreign.task.get()});
        WaitMembership own{*parent.task, *child.task}, other{*parent.task, *foreign.task};
        KREQUIRE_TRUE(own.publish());
        KREQUIRE_TRUE(other.publish());
        child.task->exit_status = 0x2300;
        foreign.task->exit_status = 0x4500;
        KREQUIRE_TRUE(child_events::publish_exit(*foreign.task));
        KREQUIRE_TRUE(child_events::publish_exit(*child.task));
        WaitClaim claim;
        child_events::ClaimedEvent event;
        using child_events::ProbeResult;
        using child_events::View;
        KEXPECT_EQ(child_events::try_claim(*sibling.task, claim.value, 2, 0, event, View::PUBLIC), ProbeResult::NO_CHILD);
        KEXPECT_EQ(child_events::try_claim(*parent.task, claim.value, foreign.task->pid, 0, event, View::PUBLIC), ProbeResult::NO_CHILD);
        KREQUIRE_EQ(child_events::try_claim(*parent.task, claim.value, 2, 0, event, View::PUBLIC), ProbeResult::CLAIMED);
        KEXPECT_EQ(event.subject_pid, child.task->pid);
        KEXPECT_EQ(event.visible_pid, uint64_t{2});
        KEXPECT_EQ(event.status, int32_t{0x2300});
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, claim.value, event, false));
        KEXPECT_EQ(child_events::try_claim(*parent.task, claim.value, child_events::SELECT_ANY, 0, event, View::PUBLIC),
                   ProbeResult::NO_CHILD);
        // Trusted physical consumers retain their existing contract.
        KREQUIRE_EQ(child_events::try_claim(*parent.task, claim.value, foreign.task->pid, 0, event), ProbeResult::CLAIMED);
        KEXPECT_EQ(event.visible_pid, foreign.task->pid);
        KEXPECT_EQ(event.status, int32_t{0x4500});
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, claim.value, event, false));
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWait, FrozenEventGroupsSurviveMutationAndLeaderDestruction) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask parent, child;
        auto leader = std::make_unique<RegistryTask>();
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(leader->task->initialize_pid_namespace(scope));
        auto const INHERITED = parent.task->job_identity_snapshot();
        leader->task->initialize_job_identity(INHERITED.internal, INHERITED.visible);
        KREQUIRE_EQ(session::create(*leader->task), int64_t{2});
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(scope));
        auto const JOB = leader->task->job_identity_snapshot();
        child.task->initialize_job_identity(JOB.internal, JOB.visible);
        uint64_t const LEADER_PID = leader->task->pid;
        make_wait_fixtures_inert({parent.task.get(), child.task.get(), leader->task.get()});
        WaitMembership membership{*parent.task, *child.task};
        KREQUIRE_TRUE(membership.publish());
        KREQUIRE_TRUE(child_events::publish_job_control_stop(*child.task, 19));
        auto job = JOB.internal;
        job.pgid = child.task->pid;
        KREQUIRE_TRUE(child_events::update_job_identity(*child.task, job, session::Names{2, 3}));
        leader.reset();
        child_events::Selector old_group;
        KREQUIRE_EQ(child_events::public_wait_selector(*parent.task, -2, old_group), 0);
        WaitClaim claim;
        child_events::ClaimedEvent stopped, continued, exited;
        using child_events::ProbeResult;
        using child_events::View;
        KREQUIRE_EQ(child_events::try_claim(*parent.task, claim.value, old_group, child_events::OPTION_STOPPED, stopped, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KEXPECT_EQ(stopped.process_group, LEADER_PID);
        KEXPECT_EQ(stopped.visible_pid, uint64_t{3});
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, claim.value, stopped, false));
        KREQUIRE_TRUE(child_events::publish_continued(*child.task));
        job.pgid = LEADER_PID;
        KREQUIRE_TRUE(child_events::update_job_identity(*child.task, job, session::Names{2, 2}));
        child_events::Selector moved_group;
        KREQUIRE_EQ(child_events::public_wait_selector(*parent.task, -3, moved_group), 0);
        KREQUIRE_EQ(
            child_events::try_claim(*parent.task, claim.value, moved_group, child_events::OPTION_CONTINUED, continued, View::PUBLIC),
            ProbeResult::CLAIMED);
        KEXPECT_EQ(continued.process_group, child.task->pid);
        KEXPECT_EQ(continued.status, int32_t{0xffff});
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, claim.value, continued, false));
        KREQUIRE_TRUE(child_events::publish_exit(*child.task));
        job.pgid = child.task->pid;
        KREQUIRE_TRUE(child_events::update_job_identity(*child.task, job, session::Names{2, 3}));
        KREQUIRE_EQ(child_events::try_claim(*parent.task, claim.value, old_group, 0, exited, View::PUBLIC), ProbeResult::CLAIMED);
        KEXPECT_EQ(exited.process_group, LEADER_PID);
        KEXPECT_EQ(exited.visible_pid, uint64_t{3});
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, claim.value, exited, false));
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWait, PublicClaimsPreserveNowaitCompetitionAndReissuedTokens) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask parent, child;
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(scope));
        make_wait_fixtures_inert({parent.task.get(), child.task.get()});
        WaitMembership membership{*parent.task, *child.task};
        KREQUIRE_TRUE(membership.publish());
        WaitClaim first, second;
        child_events::ClaimedEvent old, competing, retry;
        using child_events::ProbeResult;
        using child_events::View;
        KREQUIRE_TRUE(child_events::publish_job_control_stop(*child.task, 19));
        KREQUIRE_EQ(child_events::try_claim(*parent.task, first.value, 2, child_events::OPTION_STOPPED, old, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KEXPECT_EQ(child_events::try_claim(*parent.task, second.value, 2, child_events::OPTION_STOPPED, competing, View::PUBLIC),
                   ProbeResult::WOULD_BLOCK);
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, first.value, old, true));
        KREQUIRE_EQ(child_events::try_claim(*parent.task, second.value, 2, child_events::OPTION_STOPPED, retry, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KEXPECT_EQ(retry.visible_pid, old.visible_pid);
        KEXPECT_NE(retry.claim_cookie, old.claim_cookie);
        child_events::release_claim(second.value, retry);  // Failed copyout retry.
        KREQUIRE_EQ(child_events::try_claim(*parent.task, first.value, 2, child_events::OPTION_STOPPED, retry, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, first.value, retry, false));
        KREQUIRE_TRUE(child_events::publish_job_control_stop(*child.task, 20));
        KREQUIRE_EQ(child_events::try_claim(*parent.task, first.value, 2, child_events::OPTION_STOPPED, retry, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KEXPECT_TRUE(old.node == retry.node);
        child_events::release_claim(first.value, old);
        KEXPECT_TRUE(first.value.event == retry.node);
        KEXPECT_FALSE(child_events::commit_claim(*parent.task, first.value, old, false));
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, first.value, retry, false));
        KEXPECT_EQ(old.visible_pid, uint64_t{2});  // Copied values outlive node reuse.
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWait, RegistrationUsesPublicViewForEventsAndGroupInvalidation) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask parent, child, waiter, group_waiter;
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(scope));
        make_wait_fixtures_inert({parent.task.get(), child.task.get(), waiter.task.get(), group_waiter.task.get()});
        WaitMembership membership{*parent.task, *child.task};
        KREQUIRE_TRUE(membership.publish());
        WaitRegistration registration{*waiter.task}, group_registration{*group_waiter.task};
        child_events::ClaimedEvent event;
        using child_events::ProbeResult;
        using child_events::View;
        auto const GROUP = child_events::selector_for_process_group(2);
        KREQUIRE_EQ(child_events::claim_or_register(*parent.task, *waiter.task, 2, child_events::OPTION_STOPPED, true, event, View::PUBLIC),
                    ProbeResult::WOULD_BLOCK);
        KEXPECT_FALSE(waiter.task->child_wait_registration.notified);
        KREQUIRE_TRUE(child_events::publish_job_control_stop(*child.task, 19));
        KEXPECT_TRUE(waiter.task->child_wait_registration.notified);
        KREQUIRE_EQ(
            child_events::claim_or_register(*parent.task, *waiter.task, 2, child_events::OPTION_STOPPED, false, event, View::PUBLIC),
            ProbeResult::CLAIMED);
        KREQUIRE_TRUE(child_events::commit_claim(*parent.task, *waiter.task, event, false));
        KREQUIRE_EQ(child_events::claim_or_register(*parent.task, *group_waiter.task, GROUP, 0, true, event, View::PUBLIC),
                    ProbeResult::WOULD_BLOCK);
        auto job = child.task->job_identity_snapshot();
        auto const BEFORE = child_events::inspect_queue(*parent.task, child.task->pid, 0).version;
        // Only the local name changes. Versioning and no-child wake must still
        // observe the mutation, while physical WKI selectors stay identical.
        job.visible.pgid = 7;
        KREQUIRE_TRUE(child_events::update_job_identity(*child.task, job.internal, job.visible));
        KEXPECT_TRUE(group_waiter.task->child_wait_registration.notified);
        KEXPECT_EQ(child_events::validate_queue_version(*parent.task, BEFORE), child_events::QueueVersionStatus::STALE);
        KEXPECT_EQ(child_events::claim_or_register(*parent.task, *group_waiter.task, GROUP, 0, false, event, View::PUBLIC),
                   ProbeResult::NO_CHILD);
        KEXPECT_EQ(group_waiter.task->state.load(), sched::task::TaskState::EXITING);
        scope.close();
        KEXPECT_EQ(child_events::claim_or_register(*parent.task, *waiter.task, 2, 0, true, event, View::PUBLIC), ProbeResult::CLOSED);
        KEXPECT_FALSE(waiter.task->child_wait_registration.registered);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWait, HostSupervisionAndClosedAdmissionPreserveExistingClaims) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask host, init, child;
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(scope));
        make_wait_fixtures_inert({host.task.get(), init.task.get(), child.task.get()});
        WaitMembership host_membership{*host.task, *init.task}, child_membership{*init.task, *child.task};
        KREQUIRE_TRUE(host_membership.publish());
        KREQUIRE_TRUE(child_membership.publish());
        KEXPECT_EQ(child_events::namespace_parent_pid(*init.task), uint64_t{0});
        KREQUIRE_TRUE(child_events::publish_exit(*child.task));
        KREQUIRE_TRUE(child_events::publish_exit(*init.task));
        WaitClaim host_claim, claim, rejected;
        child_events::ClaimedEvent host_event, event, empty;
        using child_events::ProbeResult;
        using child_events::View;
        KREQUIRE_EQ(child_events::try_claim(*host.task, host_claim.value, init.task->pid, 0, host_event, View::PUBLIC),
                    ProbeResult::CLAIMED);
        KEXPECT_EQ(host_event.visible_pid, init.task->pid);
        KREQUIRE_EQ(child_events::try_claim(*init.task, claim.value, 2, 0, event, View::PUBLIC), ProbeResult::CLAIMED);
        scope.close();
        child_events::Selector preserved{123};
        KEXPECT_EQ(child_events::public_wait_selector(*init.task, -1, preserved), -EIDRM);
        KEXPECT_EQ(preserved.value, uint64_t{123});
        KEXPECT_EQ(child_events::try_claim(*init.task, rejected.value, 2, 0, empty, View::PUBLIC), ProbeResult::CLOSED);
        KEXPECT_NULL(empty.node);
        KREQUIRE_TRUE(child_events::commit_claim(*init.task, claim.value, event, false));
        KREQUIRE_TRUE(child_events::commit_claim(*host.task, host_claim.value, host_event, false));
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWait, TracedThreadsAndPublicSelectorsUseExactRetainedScope) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope, other;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(other), 0);
        RegistryTask tracer, owner, thread, foreign, unbound, host;
        KREQUIRE_TRUE(tracer.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(other));
        thread.task->is_thread = true;
        thread.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *owner.task));
        unbound.task->is_thread = true;
        make_wait_fixtures_inert(
            {tracer.task.get(), owner.task.get(), thread.task.get(), foreign.task.get(), unbound.task.get(), host.task.get()});
        WaitMembership cleanup{*tracer.task, *thread.task};
        KREQUIRE_TRUE(child_events::attach_tracer(*thread.task, *tracer.task));
        KREQUIRE_TRUE(child_events::publish_ptrace_stop(*thread.task, 0x137f));
        WaitClaim claim;
        child_events::ClaimedEvent event;
        using child_events::ProbeResult;
        using child_events::View;
        KEXPECT_EQ(child_events::try_claim(*foreign.task, claim.value, 3, 0, event, View::PUBLIC), ProbeResult::NO_CHILD);
        KREQUIRE_EQ(child_events::try_claim(*tracer.task, claim.value, 3, 0, event, View::PUBLIC), ProbeResult::CLAIMED);
        KEXPECT_EQ(event.subject_pid, thread.task->pid);
        KEXPECT_EQ(event.visible_pid, uint64_t{3});
        KREQUIRE_TRUE(child_events::commit_claim(*tracer.task, claim.value, event, false));
        child_events::Selector selector{123};
        KEXPECT_EQ(child_events::public_wait_selector(*unbound.task, -1, selector), -ESRCH);
        KEXPECT_EQ(selector.value, uint64_t{123});
        KEXPECT_EQ(child_events::public_wait_selector(*tracer.task, std::numeric_limits<int64_t>::min(), selector), -EINVAL);
        KEXPECT_EQ(selector.value, uint64_t{123});
        KREQUIRE_EQ(child_events::public_wait_selector(*tracer.task, 0, selector), 0);
        KEXPECT_EQ(selector.value, child_events::selector_for_process_group(1).value);
        KREQUIRE_EQ(child_events::public_wait_selector(*host.task, 0, selector), 0);
        KEXPECT_EQ(selector.value, child_events::selector_for_process_group(host.task->pid).value);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIdentity, CanonicalSelfAndParentNamesUseRetainedMembership) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask host, init, creator, child, sibling, grandchild;
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(scope));
        creator.task->is_thread = true;
        creator.task->owner_pid = init.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*creator.task, *init.task));
        KREQUIRE_TRUE(resource::inherit(*child.task, *creator.task));
        sibling.task->is_thread = true;
        sibling.task->owner_pid = child.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*sibling.task, *child.task));
        KREQUIRE_TRUE(resource::inherit(*grandchild.task, *sibling.task));
        PublicationRollback const INIT_MEMBERSHIP{*init.task}, CHILD_MEMBERSHIP{*child.task}, GRANDCHILD_MEMBERSHIP{*grandchild.task};
        KREQUIRE_TRUE(child_events::begin_publication(*host.task, *init.task));
        KREQUIRE_TRUE(child_events::begin_publication(*creator.task, *child.task));
        KREQUIRE_TRUE(child_events::begin_publication(*sibling.task, *grandchild.task));
        creator.task->parent_pid = 0x123456;
        sibling.task->parent_pid = host.task->pid;
        KEXPECT_EQ(sched::task::namespace_process_pid(*init.task), uint64_t{1});
        KEXPECT_EQ(sched::task::namespace_process_pid(*creator.task), uint64_t{1});
        KEXPECT_EQ(creator.task->namespace_tid(), uint64_t{2});
        KEXPECT_EQ(sched::task::namespace_process_pid(*sibling.task), uint64_t{3});
        KEXPECT_EQ(sibling.task->namespace_tid(), uint64_t{4});
        KEXPECT_EQ(child_events::namespace_parent_pid(*init.task), uint64_t{0});
        KEXPECT_EQ(child_events::namespace_parent_pid(*creator.task), uint64_t{0});
        KEXPECT_EQ(child_events::namespace_parent_pid(*child.task), uint64_t{1});
        KEXPECT_EQ(child_events::namespace_parent_pid(*sibling.task), uint64_t{1});
        KEXPECT_EQ(child_events::namespace_parent_pid(*grandchild.task), uint64_t{3});
        auto* owner = child_events::acquire_process_owner(*sibling.task);
        KREQUIRE_TRUE(owner == child.task.get());
        owner->release();
        // No physical registry entries are needed to observe these retained
        // process relationships, even after admission closes.
        scope.close();
        KEXPECT_EQ(sched::task::namespace_process_pid(*sibling.task), uint64_t{3});
        KEXPECT_EQ(child_events::namespace_parent_pid(*sibling.task), uint64_t{1});
        KEXPECT_EQ(child_events::namespace_parent_pid(*grandchild.task), uint64_t{3});
        KEXPECT_EQ(sched::task::process_pid(*sibling.task), child.task->pid);
        KEXPECT_EQ(init.task->parent_pid, host.task->pid);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIdentity, ForeignMissingAndReplacedParentsNeverUseNumericAliases) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask init, foreign, child, replacement, thread, host, host_thread, unbound;
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(resource::inherit(*child.task, *init.task));
        KREQUIRE_TRUE(resource::inherit(*replacement.task, *init.task));
        thread.task->is_thread = true;
        thread.task->owner_pid = child.task->pid;
        thread.task->parent_pid = init.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *child.task));
        PublicationRollback const CHILD_MEMBERSHIP{*child.task};
        KREQUIRE_TRUE(child_events::begin_publication(*foreign.task, *child.task));
        KEXPECT_EQ(child_events::namespace_parent_pid(*thread.task), uint64_t{0});
        child_events::abort_publication(*child.task);
        child.task->parent_pid = foreign.task->pid;
        KEXPECT_EQ(child_events::namespace_parent_pid(*child.task), uint64_t{0});
        KREQUIRE_TRUE(child_events::begin_publication(*replacement.task, *child.task));
        KEXPECT_EQ(child_events::namespace_parent_pid(*thread.task), uint64_t{3});
        KEXPECT_EQ(thread.task->parent_pid, init.task->pid);
        host_thread.task->is_thread = true;
        host_thread.task->owner_pid = host.task->pid;
        host_thread.task->parent_pid = 0xdead;
        KREQUIRE_TRUE(resource::bind_thread_owner(*host_thread.task, *host.task));
        KEXPECT_EQ(child_events::namespace_parent_pid(*host_thread.task), uint64_t{1});
        host.task->parent_pid = foreign.task->pid;
        KEXPECT_EQ(child_events::namespace_parent_pid(*host_thread.task), foreign.task->pid);
        KEXPECT_EQ(sched::task::namespace_process_pid(*host_thread.task), host.task->pid);
        KEXPECT_EQ(host_thread.task->namespace_tid(), host_thread.task->pid);
        unbound.task->is_thread = true;
        unbound.task->owner_pid = host.task->pid;
        KEXPECT_EQ(sched::task::namespace_process_pid(*unbound.task), uint64_t{0});
        KEXPECT_EQ(unbound.task->namespace_tid(), uint64_t{0});
        KEXPECT_EQ(child_events::namespace_parent_pid(*unbound.task), uint64_t{0});
        KEXPECT_NULL(child_events::acquire_process_owner(*unbound.task));
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

namespace {
namespace mm = ker::mod::mm;
namespace usercopy = ker::mod::sys::usercopy;
namespace threading = sched::threading;
struct UserTask {
    std::unique_ptr<Task> task = std::make_unique<Task>();
    threading::Thread cache_thread{};
    threading::Thread* created_thread{};
    UserTask() {
        task->pid = sched::task::get_next_pid();
        task->type = sched::task::TaskType::PROCESS;
        task->pagemap = mm::virt::create_pagemap();
    }
    ~UserTask() {
        if (created_thread != nullptr) threading::destroy_thread(created_thread);
        task->thread = nullptr;
        if (auto* pagemap = task->detach_pagemap_after_usercopy_quiescence(); pagemap != nullptr) {
            mm::virt::destroy_user_space(pagemap, task->pid, "compartment_ktest", "identity-fixture");
            mm::virt::release_pagemap(pagemap, task->pid);
        }
        KEXPECT_EQ(task->ref_count.load(), uint32_t{1});
    }
    auto map_zero(uint64_t address) -> bool {
        auto* page = mm::phys::page_alloc(mm::PhysicalPageOwner::SELFTEST, mm::paging::PAGE_SIZE, "compartment_tcb");
        if (page == nullptr) return false;
        std::memset(page, 0, mm::paging::PAGE_SIZE);
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        mm::virt::map_page(task->pagemap, address, PHYSICAL, mm::paging::page_types::USER);
        return mm::virt::translate(task->pagemap, address) == PHYSICAL;
    }
    auto tid(uint64_t tcb) -> int {
        int result = -1;
        KEXPECT_TRUE(usercopy::copy_value_from_task(*task, tcb + threading::MLIBC_TCB_TID_OFFSET, result));
        return result;
    }
};
}  // namespace

KTEST(CompartmentIdentity, ForkTcbPublicationBreaksCowWithoutChangingParent) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        constexpr uint64_t TCB = 0x48000000;
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        UserTask parent, child;
        KREQUIRE_NE(parent.task->pagemap, nullptr);
        KREQUIRE_NE(child.task->pagemap, nullptr);
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(resource::inherit(*child.task, *parent.task));
        KREQUIRE_TRUE(parent.map_zero(TCB));
        KREQUIRE_TRUE(threading::publish_tid_to_tcb(*parent.task, TCB, parent.task->namespace_tid(), true));
        uint32_t const DID_EXIT = 0x3a5b6c7d;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*parent.task, TCB + 0x1c, DID_EXIT));
        KREQUIRE_TRUE(mm::virt::deep_copy_user_pagemap_cow(parent.task->pagemap, child.task->pagemap));
        KEXPECT_FALSE(threading::publish_tid_to_tcb(*child.task, TCB, child.task->namespace_tid(), false));
        KEXPECT_EQ(parent.tid(TCB), 1);
        KEXPECT_EQ(child.tid(TCB), 1);
        KREQUIRE_TRUE(threading::publish_tid_to_tcb(*child.task, TCB, child.task->namespace_tid(), true));
        KEXPECT_EQ(parent.tid(TCB), 1);
        KEXPECT_EQ(child.tid(TCB), 2);
        KEXPECT_NE(mm::virt::translate(parent.task->pagemap, TCB), mm::virt::translate(child.task->pagemap, TCB));
        uint32_t child_exit = 0;
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*child.task, TCB + 0x1c, child_exit));
        KEXPECT_EQ(child_exit, DID_EXIT);
        KEXPECT_FALSE(threading::publish_tid_to_tcb(*child.task, TCB + 1, 99, true));
        KEXPECT_FALSE(threading::publish_tid_to_tcb(*child.task, UINT64_MAX, 99, true));
        KEXPECT_EQ(child.tid(TCB), 2);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIdentity, SharedCloneTcbRefusesVirtualAndPhysicalParentAliases) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        constexpr uint64_t PARENT_TCB = 0x49000000, CHILD_TCB = 0x49001000, ALIAS_TCB = 0x49002000;
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        UserTask parent;
        KREQUIRE_NE(parent.task->pagemap, nullptr);
        KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(parent.map_zero(PARENT_TCB));
        KREQUIRE_TRUE(parent.map_zero(CHILD_TCB));
        parent.cache_thread.fsbase = PARENT_TCB;
        parent.task->thread = &parent.cache_thread;
        KREQUIRE_TRUE(threading::publish_tid_to_tcb(*parent.task, PARENT_TCB, 1, true));
        auto const PHYSICAL = mm::virt::translate(parent.task->pagemap, PARENT_TCB);
        mm::phys::page_ref_inc(reinterpret_cast<void*>(mm::addr::get_virt_pointer(PHYSICAL)));
        mm::virt::map_page(parent.task->pagemap, ALIAS_TCB, PHYSICAL, mm::paging::page_types::USER);
        {
            ker::syscall::vmem::SharedVmemPublicationGuard const PUBLICATION;
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, 0), -ENOTSUP);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, PARENT_TCB), -ENOTSUP);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, PARENT_TCB + 4), -ENOTSUP);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, ALIAS_TCB), -ENOTSUP);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, CHILD_TCB + 1), -EFAULT);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, CHILD_TCB + 2 * mm::paging::PAGE_SIZE), -EFAULT);
            KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, CHILD_TCB), 0);
            KREQUIRE_TRUE(threading::publish_tid_to_tcb(*parent.task, CHILD_TCB, 2, false));
        }
        KEXPECT_EQ(parent.tid(PARENT_TCB), 1);
        KEXPECT_EQ(parent.tid(ALIAS_TCB), 1);
        KEXPECT_EQ(parent.tid(CHILD_TCB), 2);
        scope.close();
        KEXPECT_EQ(threading::clone_tcb_admission(*parent.task, CHILD_TCB), -EIDRM);
        UserTask host;
        KEXPECT_EQ(threading::clone_tcb_admission(*host.task, 0), 0);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIdentity, NativeImageTcbKeepsCanonicalProcessName) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const THREADS = threading::get_active_thread_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        UserTask owner;
        KREQUIRE_NE(owner.task->pagemap, nullptr);
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(scope));
        RegistryTask worker;
        worker.task->is_thread = true;
        worker.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*worker.task, *owner.task));
        ker::loader::elf::TlsModule const TLS{};
        owner.created_thread = threading::create_thread(32 * 1024, 0, owner.task->pagemap, sched::task::namespace_process_pid(*worker.task),
                                                        TLS, owner.task.get());
        KREQUIRE_NE(owner.created_thread, nullptr);
        owner.task->thread = owner.created_thread;
        KEXPECT_EQ(owner.tid(owner.created_thread->fsbase), 1);
        KEXPECT_EQ(worker.task->namespace_tid(), uint64_t{2});
        KEXPECT_EQ(threading::get_active_thread_count(), THREADS + 1);
    }
    KEXPECT_EQ(threading::get_active_thread_count(), THREADS);
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPtrace, PublicAdapterCopiesMappedMemoryRegistersAndLocalOutputs) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        constexpr uint64_t IO = 0x49000000, BUFFER = IO + 0x400, TARGET = 0x4a000000;
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        UserTask tracer, target;
        KREQUIRE_NE(tracer.task->pagemap, nullptr);
        KREQUIRE_NE(target.task->pagemap, nullptr);
        KREQUIRE_TRUE(tracer.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(tracer.map_zero(IO));
        KREQUIRE_TRUE(target.map_zero(TARGET));
        struct Registration {
            Task& task;
            bool registered{};
            ~Registration() {
                if (registered) sched::scheduler_selftest_unregister_unpublished_task(task);
            }
        } registration{*target.task};
        registration.registered = sched::scheduler_selftest_register_unpublished_task(*target.task);
        KREQUIRE_TRUE(registration.registered);
        struct TraceCleanup {
            Task& target;
            ~TraceCleanup() {
                child_events::detach_tracer(target);
                target.ptrace_traced = false;
                target.ptrace_stopped = false;
            }
        } trace_cleanup{*target.task};
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::SEIZE, 2), 0);
        uint64_t const FIRST = 0x1234567890abcdef;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*target.task, TARGET, FIRST));
        abi::ptrace::MemIo io{.address = TARGET, .buffer = reinterpret_cast<void*>(BUFFER), .size = sizeof(FIRST), .transferred = 99};
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, IO, io));
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::READ_MEM, 2, IO), 0);
        uint64_t read = 0;
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, read));
        KEXPECT_EQ(read, FIRST);
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, IO, io));
        KEXPECT_EQ(io.transferred, sizeof(FIRST));
        uint64_t const SECOND = 0xdeadbeefcafef00d;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, BUFFER, SECOND));
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::WRITE_MEM, 2, IO), 0);
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*target.task, TARGET, read));
        KEXPECT_EQ(read, SECOND);
        KEXPECT_EQ(static_cast<int64_t>(ptrace::ptrace_as(*tracer.task, PtraceRequest::PEEKDATA, 2, TARGET, BUFFER)), int64_t{0});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, read));
        KEXPECT_EQ(read, SECOND);
        KEXPECT_EQ(static_cast<int64_t>(ptrace::ptrace_as(*tracer.task, PtraceRequest::POKEDATA, 2, TARGET, FIRST)), int64_t{0});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*target.task, TARGET, read));
        KEXPECT_EQ(read, FIRST);
        // An unmapped second target page keeps the existing valid-prefix ABI.
        uint64_t const LAST = TARGET + mm::paging::PAGE_SIZE - sizeof(FIRST);
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*target.task, LAST, FIRST));
        io.address = LAST;
        io.size = 2 * sizeof(FIRST);
        io.transferred = 99;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, IO, io));
        KEXPECT_EQ(scoped_ptrace(*tracer.task, PtraceRequest::READ_MEM, 2, IO), -int64_t{EFAULT});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, IO, io));
        KEXPECT_EQ(io.transferred, sizeof(FIRST));
        target.task->ptrace_stopped = true;
        target.task->context.frame.rip = TARGET;
        target.task->context.frame.rsp = TARGET + 0x800;
        target.task->context.frame.cs = ker::mod::desc::gdt::GDT_USER_CS;
        target.task->context.frame.ss = ker::mod::desc::gdt::GDT_USER_DS;
        target.task->context.frame.flags = 0x202;
        target.task->context.regs.rax = 123;
        abi::ptrace::RegsetIo regset{.kind = abi::ptrace::regset::X86_64_GPR,
                                     .buffer = reinterpret_cast<void*>(BUFFER),
                                     .size = sizeof(abi::ptrace::X86_64GprState)};
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, IO, regset));
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::GETREGSET, 2, IO), 0);
        abi::ptrace::X86_64GprState regs{};
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, regs));
        KEXPECT_EQ(regs.rax, uint64_t{123});
        regs.rax = 456;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, BUFFER, regs));
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::SETREGSET, 2, IO), 0);
        KEXPECT_EQ(target.task->context.regs.rax, uint64_t{456});
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::GET_REMOTE_INFO, 2, BUFFER), 0);
        abi::ptrace::RemoteInfo remote{};
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, remote));
        KEXPECT_EQ(remote.proxy_pid, uint64_t{2});
        KEXPECT_EQ(remote.remote_pid, uint64_t{0});
        abi::ptrace::ThreadList list{.tids = reinterpret_cast<uint64_t*>(BUFFER), .capacity = 8, .count = 99};
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*tracer.task, IO, list));
        KREQUIRE_EQ(scoped_ptrace(*tracer.task, PtraceRequest::LIST_THREADS, 2, IO), 0);
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, IO, list));
        KEXPECT_EQ(list.count, size_t{1});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, read));
        KEXPECT_EQ(read, uint64_t{2});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, remote));
        auto const BEFORE = remote;
        scope.close();
        KEXPECT_EQ(scoped_ptrace(*tracer.task, PtraceRequest::GET_REMOTE_INFO, 2, BUFFER), -int64_t{EIDRM});
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*tracer.task, BUFFER, remote));
        KEXPECT_EQ(std::memcmp(&remote, &BEFORE, sizeof(remote)), 0);
        KEXPECT_TRUE(scope.operations_drained());
        KEXPECT_TRUE(target.task->local_signal_admissions_drained());
        KEXPECT_EQ(target.task->usercopy_pagemap_accesses.load(), uint32_t{0});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentJob, DurableNamesSurviveLeaderRetirementAndCanonicalThreadQueries) {
    auto const NAMESPACES = pid_ns::live_namespace_count();
    auto const REGISTRY = sched::get_scoped_pid_task_count();
    auto const ACTIVE = sched::get_active_task_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask init, member, peer, thread, host;
        auto leader = std::make_unique<RegistryTask>();
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(resource::inherit(*leader->task, *init.task));
        auto inherited = init.task->job_identity_snapshot();
        leader->task->initialize_job_identity(inherited.internal, inherited.visible);
        KEXPECT_EQ(session::create(*init.task), int64_t{-EPERM});
        KREQUIRE_EQ(session::create(*leader->task), int64_t{2});
        uint64_t const LEADER_PID = leader->task->pid;
        for (auto* child : {member.task.get(), peer.task.get()}) {
            KREQUIRE_TRUE(resource::inherit(*child, *leader->task));
            inherited = leader->task->job_identity_snapshot();
            child->initialize_job_identity(inherited.internal, inherited.visible);
        }
        KREQUIRE_TRUE(member.publish());
        KREQUIRE_TRUE(peer.publish());
        KREQUIRE_TRUE(leader->publish());
        // Actual withdrawal and Task destruction, not a leader lookup or a
        // copied stand-alone identity model. Members retain value-only names.
        leader.reset();
        KEXPECT_NULL(sched::find_task_by_visible_pid_safe(*member.task, 2));
        session::Names names{999, 998};
        KREQUIRE_EQ(session::query(*member.task, 0, names), 0);
        KEXPECT_EQ(names.session_id, uint64_t{2});
        KEXPECT_EQ(names.pgid, uint64_t{2});
        KEXPECT_EQ(member.task->job_identity().session_id, LEADER_PID);
        KEXPECT_EQ(member.task->job_identity().pgid, LEADER_PID);
        KREQUIRE_EQ(session::set_group(*member.task, 0, 0), 0);
        KEXPECT_EQ(member.task->job_identity_snapshot().visible.pgid, member.task->namespace_tid());
        // Resolve a group through a remaining member after its leader is gone.
        KREQUIRE_EQ(session::set_group(*member.task, 0, 2), 0);
        KEXPECT_EQ(member.task->job_identity().pgid, LEADER_PID);
        thread.task->is_thread = true;
        thread.task->owner_pid = member.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *member.task));
        KREQUIRE_TRUE(thread.publish());
        auto const REFERENCES = member.task->ref_count.load();
        KREQUIRE_EQ(session::query(*peer.task, static_cast<int64_t>(thread.task->namespace_tid()), names), 0);
        KEXPECT_EQ(names.session_id, uint64_t{2});
        KEXPECT_EQ(names.pgid, uint64_t{2});
        KEXPECT_EQ(member.task->ref_count.load(), REFERENCES);
        KREQUIRE_EQ(session::query(*host.task, static_cast<int64_t>(member.task->pid), names), 0);
        KEXPECT_EQ(names.session_id, LEADER_PID);
        KEXPECT_EQ(names.pgid, LEADER_PID);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), NAMESPACES);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
}

KTEST(CompartmentJob, LocalGroupAdmissionExcludesSiblingAndHostAliases) {
    auto const NAMESPACES = pid_ns::live_namespace_count();
    auto const REGISTRY = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask init, sibling, own, foreign, candidate, host;
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(resource::inherit(*own.task, *init.task));
        KREQUIRE_TRUE(resource::inherit(*foreign.task, *sibling.task));
        auto const JOB = init.task->job_identity_snapshot();
        own.task->initialize_job_identity(JOB.internal, JOB.visible);
        auto const OTHER_JOB = sibling.task->job_identity_snapshot();
        foreign.task->initialize_job_identity(OTHER_JOB.internal, OTHER_JOB.visible);
        KREQUIRE_EQ(session::prepare_spawn_group(*foreign.task, 0), 0);
        KREQUIRE_TRUE(init.publish());
        KREQUIRE_TRUE(sibling.publish());
        KREQUIRE_TRUE(own.publish());
        KREQUIRE_TRUE(foreign.publish());
        session::Names names;
        KREQUIRE_EQ(session::query(*init.task, 2, names), 0);
        KEXPECT_EQ(names.pgid, uint64_t{1});
        KREQUIRE_EQ(session::query(*sibling.task, 2, names), 0);
        KEXPECT_EQ(names.pgid, uint64_t{2});
        // A foreign live local-2 group cannot satisfy this namespace's alias.
        KREQUIRE_TRUE(resource::inherit(*candidate.task, *init.task));
        candidate.task->initialize_job_identity(JOB.internal, JOB.visible);
        KEXPECT_EQ(session::prepare_spawn_group(*candidate.task, 2), -EPERM);
        KEXPECT_EQ(candidate.task->job_identity_snapshot().visible.pgid, uint64_t{1});
        PublicationRollback const MEMBERSHIP{*own.task};
        KREQUIRE_TRUE(child_events::begin_publication(*init.task, *own.task));
        KREQUIRE_EQ(session::set_group(*init.task, 2, 0), 0);
        KEXPECT_EQ(own.task->job_identity_snapshot().visible.pgid, uint64_t{2});
        KEXPECT_EQ(own.task->job_identity().pgid, own.task->pid);
        KREQUIRE_EQ(session::prepare_spawn_group(*candidate.task, 2), 0);
        KEXPECT_EQ(candidate.task->job_identity().pgid, own.task->pid);
        KEXPECT_EQ(candidate.task->job_identity_snapshot().visible.pgid, uint64_t{2});
        KEXPECT_EQ(session::set_group(*init.task, static_cast<int64_t>(foreign.task->pid), 0), -ESRCH);
        KEXPECT_EQ(session::set_group(*host.task, static_cast<int64_t>(own.task->pid), 0), -EPERM);
        own.task->mark_exec_completed();
        KEXPECT_EQ(session::set_group(*init.task, 2, 0), -EACCES);
        KEXPECT_EQ(foreign.task->job_identity().pgid, foreign.task->pid);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), NAMESPACES);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY);
}

KTEST(CompartmentJob, ClosedImportedAndInvalidViewsPreserveOutputsAndState) {
    auto const NAMESPACES = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask init, child, unbound;
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(resource::inherit(*child.task, *init.task));
        auto const JOB = init.task->job_identity_snapshot();
        child.task->initialize_job_identity(JOB.internal, JOB.visible);
        KREQUIRE_TRUE(child.publish());
        session::Names names{111, 222};
        auto unchanged = [&]() {
            KEXPECT_EQ(names.session_id, uint64_t{111});
            KEXPECT_EQ(names.pgid, uint64_t{222});
        };
        KEXPECT_EQ(session::query(*init.task, -1, names), -ESRCH);
        unchanged();
        KEXPECT_EQ(session::query(*init.task, static_cast<int64_t>(child.task->pid), names), -ESRCH);
        unchanged();
        unbound.task->is_thread = true;
        unbound.task->owner_pid = init.task->pid;
        KEXPECT_EQ(session::query(*unbound.task, 0, names), -ESRCH);
        unchanged();
        auto imported = JOB.internal;
        imported.origin = {.node = 17, .boot = 31};
        child.task->initialize_job_identity(imported, JOB.visible);
        KEXPECT_EQ(child.task->job_identity_snapshot().visible.session_id, uint64_t{0});
        KEXPECT_EQ(session::query(*child.task, 0, names), -ESRCH);
        unchanged();
        child.task->initialize_job_identity(JOB.internal, JOB.visible);
        auto const VERSION = child.task->child_queue_version;
        scope.close();
        KEXPECT_FALSE(
            child_events::update_job_identity(*child.task, {.session_id = child.task->pid, .pgid = child.task->pid}, session::Names{2, 2}));
        KEXPECT_EQ(child.task->child_queue_version, VERSION);
        KEXPECT_EQ(child.task->job_identity().session_id, JOB.internal.session_id);
        KEXPECT_EQ(child.task->job_identity_snapshot().visible.session_id, uint64_t{1});
        KEXPECT_EQ(session::query(*child.task, 0, names), -EIDRM);
        unchanged();
        KEXPECT_EQ(session::create(*child.task), int64_t{-EIDRM});
        KEXPECT_EQ(session::set_group(*child.task, 0, 0), -EIDRM);
        KEXPECT_EQ(session::prepare_spawn_group(*child.task, 0), -EIDRM);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), NAMESPACES);
}

KTEST(CompartmentJob, TerminalForegroundKeepsExactNameAfterGroupLeaderRetirement) {
    namespace pty = ker::dev::pty;
    auto const NAMESPACES = pid_ns::live_namespace_count();
    auto const REGISTRY = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef scope, other;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(other), 0);
        auto release_file = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
        std::unique_ptr<ker::vfs::File, decltype(release_file)> master(
            ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), "/dev/ptmx", 2, 0), release_file);
        KREQUIRE_TRUE(master != nullptr);
        int number = -1;
        KREQUIRE_EQ(ker::vfs::devfs::devfs_ioctl(master.get(), pty::TIOCGPTN, reinterpret_cast<unsigned long>(&number)), 0);
        auto release_pair = [](pty::PtyPair* pair) { pty::pty_put(pair); };
        std::unique_ptr<pty::PtyPair, decltype(release_pair)> pair(pty::pty_get(number), release_pair);
        KREQUIRE_TRUE(pair != nullptr);
        RegistryTask init, member, foreign, host;
        auto leader = std::make_unique<RegistryTask>();
        KREQUIRE_TRUE(init.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(other));
        KREQUIRE_TRUE(resource::inherit(*leader->task, *init.task));
        auto const JOB = init.task->job_identity_snapshot();
        leader->task->initialize_job_identity(JOB.internal, JOB.visible);
        KREQUIRE_EQ(session::prepare_spawn_group(*leader->task, 0), 0);
        KREQUIRE_TRUE(resource::inherit(*member.task, *leader->task));
        auto const GROUP = leader->task->job_identity_snapshot();
        member.task->initialize_job_identity(GROUP.internal, GROUP.visible);
        KREQUIRE_TRUE(leader->publish());
        KREQUIRE_TRUE(member.publish());
        KREQUIRE_EQ(session::claim_terminal(*init.task, *pair), 0);
        KREQUIRE_EQ(session::set_foreground(*init.task, *pair, 2), 0);
        uint64_t const PHYSICAL_GROUP = leader->task->pid;
        KEXPECT_EQ(pair->foreground_pgrp, PHYSICAL_GROUP);
        KEXPECT_EQ(pair->pid_namespace_generation, scope.generation());
        leader.reset();
        uint64_t output = 77;
        KREQUIRE_EQ(session::read_foreground(*init.task, *pair, output), 0);
        KEXPECT_EQ(output, uint64_t{2});
        KREQUIRE_EQ(session::read_foreground(*host.task, *pair, output), 0);
        KEXPECT_EQ(output, PHYSICAL_GROUP);
        output = 77;
        KEXPECT_EQ(session::read_foreground(*foreign.task, *pair, output), -ENOTTY);
        KEXPECT_EQ(output, uint64_t{77});
        KEXPECT_EQ(session::set_foreground(*init.task, *pair, static_cast<int64_t>(PHYSICAL_GROUP)), -EPERM);
        KEXPECT_EQ(pair->foreground_pgrp, PHYSICAL_GROUP);
        scope.close();
        KEXPECT_EQ(session::read_foreground(*init.task, *pair, output), -EIDRM);
        KEXPECT_EQ(output, uint64_t{77});
        KEXPECT_EQ(session::set_foreground(*init.task, *pair, 2), -EIDRM);
        // Withdraw fixtures before terminal revocation can signal real tasks.
        member.retire();
        KREQUIRE_EQ(session::detach_terminal(*init.task, *pair), 0);
        KEXPECT_EQ(pair->pid_namespace_generation, uint64_t{0});
        KEXPECT_EQ(pair->namespace_foreground_pgrp, uint64_t{0});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), NAMESPACES);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY);
}

KTEST(CompartmentPid, ProductionRegistrySeparatesSameNumberAndHostTargets) {
    auto const BASELINE = sched::get_scoped_pid_task_count();
    auto const ACTIVE = sched::get_active_task_count();
    auto const NAMESPACES = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask a, b, ac, bc, host, thread;
        host.task->pid += uint64_t{1} << 62;
        KREQUIRE_TRUE(a.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(b.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(resource::inherit(*ac.task, *a.task));
        KREQUIRE_TRUE(resource::inherit(*bc.task, *b.task));
        thread.task->is_thread = true;
        thread.task->owner_pid = a.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *a.task));
        KREQUIRE_TRUE(a.publish());
        KREQUIRE_TRUE(b.publish());
        KREQUIRE_TRUE(ac.publish());
        KREQUIRE_TRUE(bc.publish());
        KREQUIRE_TRUE(host.publish());
        KREQUIRE_TRUE(thread.publish());
        KEXPECT_EQ(a.task->namespace_tid(), b.task->namespace_tid());
        KEXPECT_EQ(ac.task->namespace_tid(), bc.task->namespace_tid());
        expect_visible(*a.task, 1, a.task.get());
        expect_visible(*b.task, 1, b.task.get());
        expect_visible(*a.task, 2, ac.task.get());
        expect_visible(*b.task, 2, bc.task.get());
        expect_visible(*a.task, host.task->pid, nullptr);
        expect_visible(*b.task, host.task->pid, nullptr, true);
        expect_visible(*host.task, ac.task->pid, ac.task.get());
        expect_visible(*host.task, bc.task->pid, bc.task.get());
        expect_visible(*a.task, 0, nullptr);
        expect_visible(*a.task, thread.task->namespace_tid(), thread.task.get());
        expect_visible(*thread.task, 1, a.task.get());
        expect_visible(*thread.task, 2, ac.task.get());
        expect_visible(*b.task, thread.task->namespace_tid(), nullptr);
        auto unbound = std::make_unique<Task>();
        unbound->is_thread = true;
        expect_visible(*unbound, host.task->pid, nullptr);
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE + 5);
        KEXPECT_EQ(sched::get_active_task_count(), ACTIVE + 6);
        KEXPECT_FALSE(a.task->scheduler_published.load());
        ac.task->state.store(sched::task::TaskState::EXITING);
        expect_visible(*a.task, 2, nullptr);
        expect_visible(*a.task, 2, ac.task.get(), true);
        ac.task->state.store(sched::task::TaskState::DEAD);
        expect_visible(*a.task, 2, nullptr);
        expect_visible(*a.task, 2, ac.task.get(), true);
        expect_visible(*host.task, ac.task->pid, ac.task.get(), true);
        auto const OLD_ID = ac.task->namespace_tid();
        ac.retire();
        expect_visible(*a.task, OLD_ID, nullptr, true);
        RegistryTask replacement;
        KREQUIRE_TRUE(resource::inherit(*replacement.task, *a.task));
        KEXPECT_TRUE(replacement.task->namespace_tid() > OLD_ID);
        KREQUIRE_TRUE(replacement.publish());
        expect_visible(*a.task, OLD_ID, nullptr);
        expect_visible(*a.task, replacement.task->namespace_tid(), replacement.task.get());
        first.close();
        expect_visible(*a.task, 1, nullptr);
        expect_visible(*a.task, replacement.task->namespace_tid(), nullptr, true);
        expect_visible(*b.task, 2, bc.task.get());
        expect_visible(*host.task, a.task->pid, a.task.get());
    }
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
    KEXPECT_EQ(pid_ns::live_namespace_count(), NAMESPACES);
}

KTEST(CompartmentPid, ClosedAndCollidingPublicationHaveNoPartialRegistryWrites) {
    auto const BASELINE = sched::get_scoped_pid_task_count();
    auto const ACTIVE = sched::get_active_task_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask first, collision, closed, unpublished;
        KREQUIRE_TRUE(first.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(collision.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(closed.task->initialize_pid_namespace(scope));
        unpublished.task->scheduler_published.store(true);
        KEXPECT_FALSE(unpublished.task->initialize_pid_namespace(scope));
        unpublished.task->scheduler_published.store(false);
        KEXPECT_TRUE(unpublished.task->pid_namespace()->is_host());
        KREQUIRE_TRUE(first.publish());
        collision.task->pid = first.task->pid;
        KEXPECT_FALSE(collision.publish());
        expect_visible(*first.task, collision.task->namespace_tid(), nullptr);
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE + 1);
        KEXPECT_EQ(sched::get_active_task_count(), ACTIVE + 1);
        // A failed admission/removal must never remove a foreign collision.
        sched::scheduler_selftest_unregister_unpublished_task(*collision.task);
        expect_visible(*first.task, first.task->namespace_tid(), first.task.get());
        scope.close();
        KEXPECT_FALSE(closed.publish());
        auto* global = sched::find_task_by_pid_safe(closed.task->pid);
        KEXPECT_NULL(global);
        if (global != nullptr) global->release();
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE + 1);
        KEXPECT_EQ(sched::get_active_task_count(), ACTIVE + 1);
        first.retire();
    }
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
}

KTEST(CompartmentPid, ProductionProbeChainSurvivesHeadAndMiddleRemoval) {
    auto const BASELINE = sched::get_scoped_pid_task_count();
    auto const ACTIVE = sched::get_active_task_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask first, middle, last, host;
        KREQUIRE_TRUE(first.task->initialize_pid_namespace(scope));
        auto const BUCKET = sched::scheduler_selftest_scoped_pid_bucket(scope, first.task->namespace_tid());
        uint32_t allocated = 1;
        // Find actual production hash collisions, reserving skipped IDs via
        // the production allocator. No forged Task local IDs or model table.
        auto install_collision = [&](Task& target) {
            constexpr uint32_t SEARCH_BOUND = 1U << 20;
            while (allocated < SEARCH_BOUND) {
                auto const NEXT = allocated + 1;
                if (sched::scheduler_selftest_scoped_pid_bucket(scope, NEXT) == BUCKET) {
                    if (!target.initialize_pid_namespace(scope)) return false;
                    allocated = NEXT;
                    return target.namespace_tid() == NEXT;
                }
                if (scope.allocate_id(allocated) != 0) return false;
            }
            return false;
        };
        KREQUIRE_TRUE(install_collision(*middle.task));
        KREQUIRE_TRUE(install_collision(*last.task));
        KREQUIRE_TRUE(first.publish());
        KREQUIRE_TRUE(middle.publish());
        KREQUIRE_TRUE(last.publish());
        first.retire();
        expect_visible(*last.task, first.task->namespace_tid(), nullptr);
        expect_visible(*last.task, middle.task->namespace_tid(), middle.task.get());
        expect_visible(*last.task, last.task->namespace_tid(), last.task.get());
        // Retired physical ID may be reused in this fixture, but its old
        // scoped name must stay absent; Host-only replacement adds no row.
        host.task->pid = first.task->pid;
        KREQUIRE_TRUE(host.publish());
        expect_visible(*last.task, first.task->namespace_tid(), nullptr, true);
        expect_visible(*host.task, first.task->pid, host.task.get());
        middle.retire();
        expect_visible(*last.task, middle.task->namespace_tid(), nullptr);
        expect_visible(*last.task, last.task->namespace_tid(), last.task.get());
        last.retire();
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
    }
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
}

KTEST(CompartmentPid, ZombiePinsSerializeWithProductionGcWithdrawal) {
    auto const BASELINE = sched::get_scoped_pid_task_count();
    auto const ACTIVE = sched::get_active_task_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask zombie, host;
        KREQUIRE_TRUE(zombie.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(zombie.publish());
        KREQUIRE_TRUE(host.publish());
        KEXPECT_FALSE(zombie.retire_if_unreferenced());  // ACTIVE cannot enter GC.
        zombie.task->state.store(sched::task::TaskState::DEAD);
        auto* scoped_pin = sched::find_task_by_pid_namespace_lifetime_ref(scope, zombie.task->namespace_tid());
        auto* host_pin = sched::find_task_by_visible_pid_lifetime_ref(*host.task, zombie.task->pid);
        auto* active_pin =
            sched::find_active_task_lifetime_ref_if([](Task* target, void* expected) { return target == expected; }, zombie.task.get());
        KEXPECT_TRUE(scoped_pin == zombie.task.get());
        KEXPECT_TRUE(host_pin == zombie.task.get());
        KEXPECT_TRUE(active_pin == zombie.task.get());
        KEXPECT_EQ(zombie.task->ref_count.load(), uint32_t{4});
        KEXPECT_FALSE(zombie.retire_if_unreferenced());
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE + 1);
        KEXPECT_EQ(sched::get_active_task_count(), ACTIVE + 2);
        if (scoped_pin != nullptr) scoped_pin->release();
        KEXPECT_FALSE(zombie.retire_if_unreferenced());
        if (host_pin != nullptr) host_pin->release();
        KEXPECT_FALSE(zombie.retire_if_unreferenced());
        if (active_pin != nullptr) active_pin->release();
        KEXPECT_EQ(zombie.task->ref_count.load(), uint32_t{1});
        KREQUIRE_TRUE(zombie.retire_if_unreferenced());
        expect_visible(*zombie.task, zombie.task->namespace_tid(), nullptr, true);
        expect_visible(*host.task, zombie.task->pid, nullptr, true);
        KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
        KEXPECT_EQ(sched::get_active_task_count(), ACTIVE + 1);
        host.task->state.store(sched::task::TaskState::DEAD);
        host_pin = sched::find_task_by_pid_namespace_lifetime_ref(*host.task->pid_namespace(), host.task->pid);
        KEXPECT_TRUE(host_pin == host.task.get());
        KEXPECT_FALSE(host.retire_if_unreferenced());
        if (host_pin != nullptr) host_pin->release();
        KREQUIRE_TRUE(host.retire_if_unreferenced());
        expect_visible(*host.task, host.task->pid, nullptr, true);
    }
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
}

KTEST(CompartmentIpc, ChildAndThreadUseExactProcessNamespace) {
    auto const BASELINE = ipc::live_namespace_count();
    {
        ipc::NamespaceRef scope;
        KREQUIRE_EQ(ipc::create_namespace(scope), 0);
        auto owner = std::make_unique<Task>();
        owner->pid = ker::mod::sched::task::get_next_pid();
        KREQUIRE_TRUE(owner->initialize_ipc_namespace(scope));
        auto thread = std::make_unique<Task>();
        thread->is_thread = true;
        thread->owner_pid = owner->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *owner));
        KEXPECT_TRUE(thread->ipc_namespace() == owner->ipc_namespace());
        KEXPECT_FALSE(thread->initialize_ipc_namespace(scope));
        auto child = std::make_unique<Task>();
        KREQUIRE_TRUE(resource::inherit(*child, *thread));
        KEXPECT_TRUE(*child->ipc_namespace() == scope);
        KEXPECT_FALSE(child->initialize_ipc_namespace(scope));
        scope.reset();
        KEXPECT_FALSE(child->ipc_namespace()->is_host());
        thread.reset();
        owner.reset();
        KEXPECT_EQ(ipc::live_namespace_count(), BASELINE + 1);
    }
    KEXPECT_EQ(ipc::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIpc, PublishedAndClosedInstallationFailsWithoutHostFallback) {
    ipc::NamespaceRef scope;
    KREQUIRE_EQ(ipc::create_namespace(scope), 0);
    auto task = std::make_unique<Task>();
    task->scheduler_published.store(true);
    KEXPECT_FALSE(task->initialize_ipc_namespace(scope));
    KEXPECT_TRUE(task->ipc_namespace()->is_host());
    task->scheduler_published.store(false);
    scope.close();
    KEXPECT_FALSE(task->initialize_ipc_namespace(scope));
    KEXPECT_TRUE(task->ipc_namespace()->is_host());
    auto thread = std::make_unique<Task>();
    thread->is_thread = true;
    KEXPECT_NULL(thread->ipc_namespace());
    auto child = std::make_unique<Task>();
    KEXPECT_FALSE(resource::inherit(*child, *thread));
}

KTEST(CompartmentIpc, SysvNamesAndGuessedIdsAreScopedBeforeEffects) { KEXPECT_TRUE(ker::syscall::shm::shm_selftest_namespace_isolation()); }

KTEST(CompartmentIpc, RetirementPreservesInheritedAttachmentsAndDrainsBacking) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_namespace_retirement());
}

namespace {
namespace vfs = ker::vfs;
namespace mount_ns = vfs::mount_ns;
namespace tmpfs = vfs::tmpfs;

struct PrivateMounts {
    mount_ns::NamespaceRef first, second;
    vfs::PreparedMount pending_first{}, pending_second{};
    ~PrivateMounts() {
        vfs::finish_prepared_mount(&pending_first);
        vfs::finish_prepared_mount(&pending_second);
        if (!first.is_host()) KEXPECT_EQ(vfs::retire_mount_namespace(first), 0);
        if (!second.is_host()) KEXPECT_EQ(vfs::retire_mount_namespace(second), 0);
    }
};
struct PutFile {
    void operator()(vfs::File* file) const { vfs::vfs_put_file(file); }
};
using NativeFile = std::unique_ptr<vfs::File, PutFile>;

auto native_root(vfs::MountPoint* mount) -> tmpfs::TmpNode* {
    return tmpfs::mount_root(static_cast<tmpfs::TmpfsMount*>(mount->private_data));
}
// Direct native backend fixture. Use real File/mount ownership, without
// claiming that Task pathwalk or public compartment creation is integrated.
auto native_file(vfs::MountPoint* mount, const char* path) -> NativeFile {
    auto* file = tmpfs::tmpfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), native_root(mount), path, vfs::O_CREAT | 2, 0600);
    if (file != nullptr) {
        if (!vfs::retain_mount_for_open_file(mount)) {
            vfs::vfs_put_file(file);
            return {};
        }
        file->mount_owner = mount;
        file->mount_dev_id = mount->dev_id;
        file->mount_generation = vfs::mount_table_generation_snapshot();
    }
    return NativeFile(file);
}
auto private_mount(const mount_ns::NamespaceRef& scope, const char* path) -> int {
    return vfs::mount_filesystem(path, "tmpfs", nullptr, 0, "size=1M", nullptr, nullptr, scope);
}
auto scoped_mount(const mount_ns::NamespaceRef& scope, const char* path) -> vfs::MountRef {
    return vfs::find_mount_point(path, vfs::UNKNOWN_MOUNT_PATH_LEN, scope);
}
// Keep exact installed FD capabilities bounded and close them on every test
// return before the Task and its namespace/mount fixtures are destroyed.
struct NativeTask {
    std::unique_ptr<Task> task = std::make_unique<Task>();
    std::array<int, 16> descriptors{};
    size_t used{};
    NativeTask() { task->pid = ker::mod::sched::task::get_next_pid(); }
    ~NativeTask() {
        while (used != 0) KEXPECT_EQ(vfs::vfs_close_for_task(task.get(), descriptors[--used]), 0);
        KEXPECT_EQ(task->fd_table.size(), 0U);
        KEXPECT_EQ(task->fd_table.reserved_count(), 0U);
    }
    auto track(int fd) -> int {
        if (fd >= 0) {
            if (used == descriptors.size()) {
                static_cast<void>(vfs::vfs_close_for_task(task.get(), fd));
                return -EMFILE;
            }
            descriptors[used++] = fd;
        }
        return fd;
    }
    auto open(const char* path, int flags = vfs::O_CREAT | 2) -> int {
        return track(vfs::vfs_openat(task.get(), vfs::AT_FDCWD, path, flags, 0600));
    }
    auto close(int fd) -> int {
        for (size_t i = 0; i < used; ++i) {
            if (descriptors[i] != fd) continue;
            int const RESULT = vfs::vfs_close_for_task(task.get(), fd);
            if (RESULT == 0) descriptors[i] = descriptors[--used];
            return RESULT;
        }
        return -EBADF;
    }
};
auto task_lookup(Task& task, const char* path, vfs::LookupHandle& out, int dirfd = vfs::AT_FDCWD) -> int {
    return vfs::vfs_acquire_lookup(&task, dirfd, path, vfs::LookupIntent::STAT, vfs::LookupFollowPolicy::FOLLOW_FINAL, false, true, &out);
}
struct LookupObservation {
    unsigned calls{};
    vfs::Stat stat{};
};
struct DirectoryListing {
    std::array<std::array<char, 64>, 16> names{};
    std::array<uint8_t, 16> types{};
    size_t count{};
    auto type(const char* name) const -> int {
        for (size_t i = 0; i < count; ++i)
            if (std::strcmp(names[i].data(), name) == 0) return types[i];
        return -ENOENT;
    }
};
auto read_directory(NativeTask& subject, int fd, DirectoryListing& output) -> int {
    NativeFile file(vfs::vfs_get_file_retain(subject.task.get(), fd));
    if (!file) return -EBADF;
    file->pos = 0;
    file->dir_fs_count = static_cast<size_t>(-1);
    std::array<uint8_t, 2048> packed{};
    auto const SIZE = vfs::vfs_read_dir_entries(fd, packed.data(), packed.size(), subject.task.get());
    if (SIZE < 0) return static_cast<int>(SIZE);
    DirectoryListing staged;
    for (size_t offset = 0; offset < static_cast<size_t>(SIZE);) {
        if (static_cast<size_t>(SIZE) - offset < vfs::DIRENT_MIN_RECLEN) return -EIO;
        uint16_t length = 0;
        std::memcpy(&length, packed.data() + offset + offsetof(vfs::DirEntry, d_reclen), sizeof(length));
        if (length < vfs::DIRENT_MIN_RECLEN || length > static_cast<size_t>(SIZE) - offset || length % vfs::DIRENT_RECORD_ALIGNMENT != 0 ||
            staged.count == staged.names.size())
            return -EIO;
        auto const* name = reinterpret_cast<const char*>(packed.data() + offset + vfs::DIRENT_HEADER_SIZE);
        size_t n = 0;
        while (n < length - vfs::DIRENT_HEADER_SIZE && name[n] != '\0') ++n;
        if (n == length - vfs::DIRENT_HEADER_SIZE || n >= staged.names[0].size()) return -EIO;
        std::memcpy(staged.names[staged.count].data(), name, n + 1);
        staged.types[staged.count++] = packed[offset + offsetof(vfs::DirEntry, d_type)];
        offset += length;
    }
    output = staged;
    return 0;
}
auto observe_lookup(const vfs::LookupHandle& handle, void* opaque) -> int {
    auto& out = *static_cast<LookupObservation*>(opaque);
    ++out.calls;
    return tmpfs::tmpfs_stat_lookup(*static_cast<tmpfs::TmpfsLookupHandle*>(handle.backend_parent()), false, handle.mount()->dev_id,
                                    &out.stat);
}
}  // namespace

KTEST(CompartmentListing, PackedNamesUseFileScopeAndKeepInheritedCapabilities) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        NativeTask first, second;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/a_real", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mkdirat(second.task.get(), vfs::AT_FDCWD, "/b_real", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/wki", 0755), 0);
        // Trusted synthetic rows exercise augmentation independently of native
        // entries. They do not claim usable mount crossing without covered proof.
        KREQUIRE_EQ(private_mount(views.first, "/a_synthetic/leaf"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/b_synthetic/leaf"), 0);
        KREQUIRE_EQ(private_mount(views.first, "/wki/local"), 0);
        int const A = first.open("/", vfs::O_DIRECTORY), B = second.open("/", vfs::O_DIRECTORY);
        int const WKI = first.open("/wki", vfs::O_DIRECTORY);
        KREQUIRE_TRUE(A >= 0 && B >= 0 && WKI >= 0);
        DirectoryListing a, b, wki;
        KREQUIRE_EQ(read_directory(first, A, a), 0);
        KREQUIRE_EQ(read_directory(second, B, b), 0);
        KEXPECT_EQ(a.type("a_real"), vfs::DT_DIR);
        KEXPECT_EQ(a.type("a_synthetic"), vfs::DT_DIR);
        KEXPECT_EQ(a.type("b_synthetic"), -ENOENT);
        KEXPECT_EQ(a.type("b_real"), -ENOENT);
        KEXPECT_EQ(b.type("b_synthetic"), vfs::DT_DIR);
        KEXPECT_EQ(b.type("a_synthetic"), -ENOENT);
        KREQUIRE_EQ(read_directory(first, WKI, wki), 0);
        KEXPECT_EQ(wki.type("local"), vfs::DT_DIR);
        KEXPECT_EQ(wki.type("host"), -ENOENT);
        NativeFile capability(vfs::vfs_get_file_retain(first.task.get(), A));
        KREQUIRE_TRUE(capability);
        vfs::FdReservation reservation(second.task.get());
        int const INHERITED = reservation.install(capability.get());
        KREQUIRE_TRUE(INHERITED >= 0);
        static_cast<void>(capability.release());
        KREQUIRE_EQ(second.track(INHERITED), INHERITED);
        DirectoryListing inherited;
        KREQUIRE_EQ(read_directory(second, INHERITED, inherited), 0);
        KEXPECT_EQ(inherited.type("a_synthetic"), vfs::DT_DIR);
        KEXPECT_EQ(inherited.type("b_synthetic"), -ENOENT);
        vfs::Stat stat{};
        KEXPECT_EQ(vfs::vfs_statat(second.task.get(), INHERITED, "a_real", 0, &stat), -EXDEV);
        views.first.close();
        KREQUIRE_EQ(read_directory(second, INHERITED, inherited), 0);
        KEXPECT_EQ(inherited.type("a_real"), vfs::DT_DIR);
        KEXPECT_EQ(inherited.type("a_synthetic"), -ENOENT);
        KEXPECT_EQ(inherited.type("b_synthetic"), -ENOENT);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentListing, RenamedDirectoryCannotAugmentFromReplacementText) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    NativeTask subject;
    KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
    KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/original", 0755), 0);
    int const RETAINED = subject.open("/original", vfs::O_DIRECTORY);
    KREQUIRE_TRUE(RETAINED >= 0);
    KREQUIRE_EQ(vfs::vfs_renameat(subject.task.get(), vfs::AT_FDCWD, "/original", vfs::AT_FDCWD, "/moved"), 0);
    KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/original", 0755), 0);
    KREQUIRE_EQ(private_mount(views.first, "/original/stale/leaf"), 0);
    KREQUIRE_EQ(private_mount(views.first, "/moved/current/leaf"), 0);
    int const REPLACEMENT = subject.open("/original", vfs::O_DIRECTORY);
    KREQUIRE_TRUE(REPLACEMENT >= 0);
    DirectoryListing held, replacement;
    KREQUIRE_EQ(read_directory(subject, RETAINED, held), 0);
    KEXPECT_EQ(held.type("current"), vfs::DT_DIR);
    KEXPECT_EQ(held.type("stale"), -ENOENT);
    KREQUIRE_EQ(read_directory(subject, REPLACEMENT, replacement), 0);
    KEXPECT_EQ(replacement.type("stale"), vfs::DT_DIR);
    KEXPECT_EQ(replacement.type("current"), -ENOENT);
}

KTEST(CompartmentListing, ProcfsMountReadSeekUseSubjectScopeAndPreserveFailedOutput) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        for (auto* path : {"/", "/visible", "/visible/inside", "/hidden"}) KREQUIRE_EQ(private_mount(views.first, path), 0);
        for (auto* path : {"/", "/b_foreign"}) KREQUIRE_EQ(private_mount(views.second, path), 0);
        NativeTask first, second;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        std::memcpy(first.task->root.data(), "/visible", 9);
        first.task->root_len = 8;
        auto thread = std::make_unique<Task>();
        thread->is_thread = true;
        thread->owner_pid = first.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *first.task));
        std::memcpy(thread->root.data(), "/obsolete", 10);
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        vfs::procfs::ProcFileData data{};
        data.node.type = vfs::procfs::ProcNodeType::MOUNTS_FILE;
        vfs::File file{};
        file.private_data = &data;
        file.pos = 37;
        std::array<char, 2048> a{}, b{}, via_thread{};
        auto const A = vfs::procfs::procfs_read_mounts_as(first.task.get(), ROOT, &file, a.data(), a.size(), 0);
        auto const B = vfs::procfs::procfs_read_mounts_as(second.task.get(), ROOT, &file, b.data(), b.size(), 0);
        auto const THREAD = vfs::procfs::procfs_read_mounts_as(thread.get(), ROOT, &file, via_thread.data(), via_thread.size(), 0);
        KREQUIRE_TRUE(A > 0 && B > 0 && THREAD == A);
        KEXPECT_TRUE(a == via_thread);
        KEXPECT_TRUE(std::strstr(a.data(), "tmpfs / tmpfs") != nullptr);
        KEXPECT_TRUE(std::strstr(a.data(), "tmpfs /inside tmpfs") != nullptr);
        KEXPECT_NULL(std::strstr(a.data(), "hidden"));
        KEXPECT_NULL(std::strstr(a.data(), "b_foreign"));
        KEXPECT_TRUE(std::strstr(b.data(), "b_foreign") != nullptr);
        KEXPECT_NULL(std::strstr(b.data(), "inside"));
        KEXPECT_EQ(vfs::procfs::procfs_lseek_mounts_as(first.task.get(), ROOT, &file, 0, 2), A);
        vfs::FilesystemRootMetadata captured;
        KREQUIRE_EQ(vfs::vfs_capture_root_metadata(first.task.get(), captured), 0);
        KEXPECT_TRUE(captured.mount_scope() == views.first);
        struct RestoreHook {
            decltype(vfs::procfs::mounts_snapshot_test_hook) previous = vfs::procfs::mounts_snapshot_test_hook;
            ~RestoreHook() { vfs::procfs::mounts_snapshot_test_hook = previous; }
        } restore;
        vfs::procfs::mounts_snapshot_test_hook = [](Task* task) { task->mount_namespace()->close(); };
        a.fill('S');
        auto const SENTINEL = a;
        KEXPECT_EQ(vfs::procfs::procfs_read_mounts_as(first.task.get(), ROOT, &file, a.data(), a.size(), 0), ssize_t{-EAGAIN});
        KEXPECT_TRUE(a == SENTINEL);
        KEXPECT_FALSE(vfs::vfs_root_metadata_current(first.task.get(), captured));
        KEXPECT_EQ(vfs::vfs_capture_root_metadata(first.task.get(), captured), -EIDRM);
        KEXPECT_TRUE(captured.mount_scope() == views.first);
        KEXPECT_TRUE(std::strcmp(captured.path(), "/visible") == 0);
        auto const POSITION = file.pos;
        KEXPECT_EQ(vfs::procfs::procfs_lseek_mounts_as(first.task.get(), ROOT, &file, 0, 2), off_t{-EIDRM});
        KEXPECT_EQ(file.pos, POSITION);
        KEXPECT_EQ(vfs::procfs::procfs_read_mounts_as(thread.get(), ROOT, &file, a.data(), a.size(), 0), ssize_t{-EIDRM});
        KEXPECT_TRUE(a == SENTINEL);
        vfs::procfs::mounts_snapshot_test_hook = restore.previous;
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentAdmin, NativeMountUnmountUsesSubjectAndExactIdentity) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        NativeTask first, second;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        for (auto* task : {first.task.get(), second.task.get()}) {
            KREQUIRE_EQ(vfs::vfs_mkdirat(task, vfs::AT_FDCWD, "/service", 0755), 0);
            KREQUIRE_EQ(vfs::vfs_mount_for_task(task, nullptr, "/service", "tmpfs", 0, "size=1M"), 0);
        }
        uint32_t first_id = 0, second_id = 0;
        {
            auto a = scoped_mount(views.first, "/service"), b = scoped_mount(views.second, "/service");
            KREQUIRE_TRUE(a && b && a.get() != b.get());
            first_id = a->dev_id;
            second_id = b->dev_id;
            KEXPECT_TRUE(first_id != second_id);
            KEXPECT_TRUE(a->private_data != b->private_data);
        }
        int const FD = first.open("/service/data");
        KREQUIRE_TRUE(FD >= 0);
        NativeFile inherited(vfs::vfs_get_file_retain(first.task.get(), FD));
        KREQUIRE_TRUE(inherited);
        KREQUIRE_EQ(first.close(FD), 0);
        char const VALUE = 'v';
        KREQUIRE_EQ(tmpfs::tmpfs_write(inherited.get(), &VALUE, 1, 0), 1);
        KEXPECT_EQ(vfs::vfs_umount_for_task(first.task.get(), "/service"), -EBUSY);
        char value = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(inherited.get(), &value, 1, 0), 1);
        KEXPECT_EQ(value, VALUE);
        inherited.reset();
        KREQUIRE_EQ(vfs::vfs_umount_for_task(first.task.get(), "/service"), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(first.task.get(), nullptr, "/service", "tmpfs", 0, "size=1M"), 0);
        KEXPECT_EQ(vfs::unmount_filesystem_if_dev_id("/service", first_id, views.first, vfs::MountPathKind::RETAINED_PHYSICAL), -ENOENT);
        {
            auto a = scoped_mount(views.first, "/service"), b = scoped_mount(views.second, "/service");
            KREQUIRE_TRUE(a && b);
            KEXPECT_TRUE(a->dev_id != first_id);
            KEXPECT_EQ(b->dev_id, second_id);
        }
        KREQUIRE_EQ(vfs::vfs_umount_for_task(first.task.get(), "/service"), 0);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
        KEXPECT_EQ(vfs::get_mount_count(views.second), 2U);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentAdmin, UnsupportedClosedAndUnauthorizedFailBeforeMountEffects) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        NativeTask subject;
        KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
        KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
        for (auto* source : {"wki://foreign/private", "PARTUUID=foreign", "/dev/foreign", ""})
            KEXPECT_EQ(vfs::vfs_mount_for_task(subject.task.get(), source, "/service", "tmpfs"), -ENOTSUP);
        for (auto* type : {"xfs", "fat32", "remote", "devfs", ""})
            KEXPECT_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/service", type), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/service", nullptr), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/absent", "tmpfs"), -ENOENT);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
        NativeTask denied;
        KREQUIRE_TRUE(denied.task->initialize_mount_namespace(views.first));
        KREQUIRE_EQ(resource::initialize_credentials(*denied.task, ker::mod::security::Snapshot{}), 0);
        KEXPECT_EQ(vfs::vfs_mount_for_task(denied.task.get(), nullptr, "/service", "tmpfs"), -EPERM);
        KEXPECT_EQ(vfs::vfs_umount_for_task(denied.task.get(), "/service"), -EPERM);
        KEXPECT_EQ(vfs::vfs_pivot_root_for_task(denied.task.get(), "/service", "/service/old"), -EPERM);
        views.first.close();
        KEXPECT_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/service", "tmpfs"), -EIDRM);
        KEXPECT_EQ(vfs::vfs_umount_for_task(subject.task.get(), "/"), -EIDRM);
        KEXPECT_EQ(vfs::vfs_pivot_root_for_task(subject.task.get(), "/service", "/service/old"), -EIDRM);
        KEXPECT_EQ(vfs::unmount_filesystem_if_dev_id("/", 1, views.first, vfs::MountPathKind::RETAINED_PHYSICAL), -EIDRM);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentAdmin, NativePivotTwicePreservesSiblingAndHostViews) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        NativeTask first, second;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/new", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(first.task.get(), nullptr, "/new", "tmpfs", 0, "size=1M"), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(first.task.get(), nullptr, "/service", "tmpfs", 0, "size=1M"), 0);
        for (auto* path : {"/new/old", "/new/service", "/new/next"})
            KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, path, 0755), 0);
        for (auto* path : {"/rootdata", "/new/data", "/service/data"}) {
            int const FD = first.open(path);
            KREQUIRE_TRUE(FD >= 0);
            KREQUIRE_EQ(first.close(FD), 0);
        }
        uint32_t root_id = 0, new_id = 0, service_id = 0;
        {
            auto root = scoped_mount(views.first, "/"), next = scoped_mount(views.first, "/new"),
                 service = scoped_mount(views.first, "/service");
            KREQUIRE_TRUE(root && next && service);
            root_id = root->dev_id;
            new_id = next->dev_id;
            service_id = service->dev_id;
        }
        KEXPECT_EQ(vfs::vfs_pivot_root_for_task(first.task.get(), "/new", "/service"), -EXDEV);
        KEXPECT_EQ(vfs::vfs_pivot_root_for_task(first.task.get(), "/new/next", "/new/old"), -EINVAL);
        int const HELD = first.open("/rootdata", 0);
        KREQUIRE_TRUE(HELD >= 0);
        KEXPECT_EQ(vfs::vfs_pivot_root_for_task(first.task.get(), "/new", "/new/old"), -EBUSY);
        KREQUIRE_EQ(first.close(HELD), 0);
        // A thread uses its retained process filesystem owner; it does not
        // acquire a worker's host view or publish a new Task.
        auto thread = std::make_unique<Task>();
        thread->is_thread = true;
        thread->owner_pid = first.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *first.task));
        KREQUIRE_EQ(vfs::vfs_pivot_root_for_task(thread.get(), "/new", "/new/old"), 0);
        KEXPECT_TRUE(std::strcmp(first.task->root.data(), "/new") == 0);
        KEXPECT_TRUE(std::strcmp(second.task->root.data(), "/") == 0);
        vfs::Stat stat{};
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/data", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, new_id);
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/old/rootdata", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, root_id);
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/service/data", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, service_id);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(first.task.get(), nullptr, "/next", "tmpfs", 0, "size=1M"), 0);
        for (auto* path : {"/next/old2", "/next/old", "/next/service"})
            KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, path, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_pivot_root_for_task(first.task.get(), "/next", "/next/old2"), 0);
        KEXPECT_TRUE(std::strcmp(first.task->root.data(), "/new/next") == 0);
        KEXPECT_TRUE(std::strcmp(second.task->root.data(), "/") == 0);
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/old2/data", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, new_id);
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/old/rootdata", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, root_id);
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/service/data", 0, &stat), 0);
        KEXPECT_EQ(stat.st_dev, service_id);
        KREQUIRE_EQ(vfs::vfs_umount_for_task(first.task.get(), "/service"), 0);
        KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/service/data", 0, &stat), -ENOENT);
        KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/data", 0, &stat), -ENOENT);
        KEXPECT_EQ(vfs::get_mount_count(views.second), 1U);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
        KEXPECT_FALSE(vfs::mount_pivot_pending_for_test());
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentPath, ChildThreadAndUnpublishedOwnership) {
    auto const BASELINE = mount_ns::live_namespace_count();
    {
        mount_ns::NamespaceRef scope;
        KREQUIRE_EQ(mount_ns::create_namespace(scope), 0);
        auto owner = std::make_unique<Task>();
        owner->pid = ker::mod::sched::task::get_next_pid();
        KREQUIRE_TRUE(owner->initialize_mount_namespace(scope));
        auto thread = std::make_unique<Task>();
        thread->is_thread = true;
        thread->owner_pid = owner->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *owner));
        KEXPECT_EQ(thread->mount_namespace(), owner->mount_namespace());
        KEXPECT_FALSE(thread->initialize_mount_namespace(scope));
        auto child = std::make_unique<Task>();
        KREQUIRE_TRUE(resource::inherit(*child, *thread));
        KEXPECT_TRUE(*child->mount_namespace() == scope);
        KEXPECT_FALSE(child->initialize_mount_namespace(scope));
        auto published = std::make_unique<Task>();
        published->scheduler_published.store(true);
        KEXPECT_FALSE(published->initialize_mount_namespace(scope));
        KEXPECT_TRUE(published->mount_namespace()->is_host());
        auto unbound = std::make_unique<Task>();
        unbound->is_thread = true;
        KEXPECT_NULL(unbound->mount_namespace());
        vfs::LookupHandle absent;
        KEXPECT_EQ(task_lookup(*unbound, "/", absent), -ESRCH);
        KEXPECT_NULL(absent.mount());
        scope.close();
        auto closed_child = std::make_unique<Task>();
        KEXPECT_FALSE(closed_child->initialize_mount_namespace(scope));
        KEXPECT_FALSE(resource::inherit(*closed_child, *owner));
        KEXPECT_TRUE(closed_child->mount_namespace()->is_host());
        scope.reset();
        thread.reset();
        owner.reset();
        KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE + 1);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPath, NativeNamesSymlinksRenameAndMovedConsumption) {
    auto const BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        NativeTask first, second;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mkdirat(second.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
        int const A = first.open("/service/data"), B = second.open("/service/data");
        KREQUIRE_TRUE(A >= 0 && B >= 0);
        NativeFile a(vfs::vfs_get_file_retain(first.task.get(), A));
        NativeFile b(vfs::vfs_get_file_retain(second.task.get(), B));
        KREQUIRE_TRUE(a && b);
        KEXPECT_TRUE(a->mount_owner->namespace_owner == views.first);
        KEXPECT_TRUE(b->mount_owner->namespace_owner == views.second);
        char const FIRST = 'a', SECOND = 'b';
        KREQUIRE_EQ(tmpfs::tmpfs_write(a.get(), &FIRST, 1, 0), 1);
        KREQUIRE_EQ(tmpfs::tmpfs_write(b.get(), &SECOND, 1, 0), 1);
        vfs::Stat sa{}, sb{};
        KREQUIRE_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/service/data", 0, &sa), 0);
        KREQUIRE_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/service/data", 0, &sb), 0);
        KEXPECT_NE(sa.st_dev, sb.st_dev);
        KREQUIRE_EQ(vfs::vfs_symlinkat(first.task.get(), "/service/data", vfs::AT_FDCWD, "/absolute"), 0);
        KREQUIRE_EQ(vfs::vfs_symlinkat(first.task.get(), "service/../service/data", vfs::AT_FDCWD, "/relative"), 0);
        for (auto* path : {"/../../service/data", "/absolute", "/relative", "service/./data"}) {
            vfs::LookupHandle original;
            KREQUIRE_EQ(task_lookup(*first.task, path, original), 0);
            KEXPECT_TRUE(original.mount()->namespace_owner == views.first);
            KEXPECT_EQ(original.authorization().mount_namespace_generation, views.first.generation());
            vfs::LookupHandle moved(std::move(original));
            KEXPECT_NULL(original.mount());
            LookupObservation observation;
            KEXPECT_EQ(vfs::vfs_consume_lookup(moved, false, observe_lookup, &observation), 0);
            KEXPECT_EQ(observation.calls, 1U);
            KEXPECT_EQ(observation.stat.st_dev, sa.st_dev);
            KEXPECT_EQ(observation.stat.st_ino, sa.st_ino);
        }
        auto worker = std::make_unique<Task>();
        worker->is_thread = true;
        worker->owner_pid = first.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*worker, *first.task));
        vfs::LookupHandle stale;
        KREQUIRE_EQ(task_lookup(*worker, "/service/data", stale), 0);
        KREQUIRE_EQ(vfs::vfs_renameat(first.task.get(), vfs::AT_FDCWD, "/service/data", vfs::AT_FDCWD, "/service/renamed"), 0);
        LookupObservation skipped;
        KEXPECT_EQ(vfs::vfs_consume_lookup(stale, false, observe_lookup, &skipped), -EAGAIN);
        KEXPECT_EQ(skipped.calls, 0U);
        stale.reset();
        KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/service/data", 0, &sa), -ENOENT);
        KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/service/data", 0, &sb), 0);
        NativeFile read;
        vfs::File* raw = nullptr;
        KREQUIRE_EQ(vfs::vfs_open_file_as(*worker, worker->credentials(), "/service/renamed", 0, &raw), 0);
        read.reset(raw);
        char byte = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(read.get(), &byte, 1, 0), 1);
        KEXPECT_EQ(byte, FIRST);
        KEXPECT_EQ(tmpfs::tmpfs_read(b.get(), &byte, 1, 0), 1);
        KEXPECT_EQ(byte, SECOND);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentPath, ForeignDirfdRefusesNamesButKeepsFileCapability) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    NativeTask first, second;
    KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
    KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
    KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdirat(second.task.get(), vfs::AT_FDCWD, "/service", 0755), 0);
    KREQUIRE_TRUE(second.open("/service/data") >= 0);
    int const OWN = first.open("/service", vfs::O_DIRECTORY), FOREIGN = second.open("/service", vfs::O_DIRECTORY);
    KREQUIRE_TRUE(OWN >= 0 && FOREIGN >= 0);
    NativeFile inherited(vfs::vfs_get_file_retain(second.task.get(), FOREIGN));
    KREQUIRE_TRUE(inherited);
    vfs::FdReservation descriptor(first.task.get());
    int const FD = descriptor.install(inherited.get());
    KREQUIRE_TRUE(FD >= 0);
    static_cast<void>(inherited.release());
    KREQUIRE_EQ(first.track(FD), FD);
    vfs::LookupHandle absent;
    KEXPECT_EQ(task_lookup(*first.task, "data", absent, FD), -EXDEV);
    KEXPECT_NULL(absent.mount());
    vfs::Stat stat{};
    KEXPECT_EQ(vfs::vfs_statat(first.task.get(), FD, "data", 0, &stat), -EXDEV);
    KEXPECT_EQ(vfs::vfs_mkdirat(first.task.get(), FD, "escape", 0755), -EXDEV);
    KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/service/escape", 0, &stat), -ENOENT);
    KEXPECT_EQ(vfs::vfs_statat(first.task.get(), OWN, ".", 0, &stat), 0);
    NativeFile explicit_file(vfs::vfs_get_file_retain(first.task.get(), FD));
    KREQUIRE_TRUE(explicit_file);
    KEXPECT_EQ(vfs::vfs_fstat_file(explicit_file.get(), &stat), 0);
    KEXPECT_TRUE(explicit_file->mount_owner->namespace_owner == views.second);
}

KTEST(CompartmentPath, ClosedAndUnsupportedLookupNeverFallsBackToHost) {
    auto const HOST_COUNT = vfs::get_mount_count();
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    NativeTask subject;
    KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
    KREQUIRE_TRUE(subject.open("/data") >= 0);
    KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/wki", 0755), 0);
    vfs::LookupHandle ordinary;
    KEXPECT_EQ(task_lookup(*subject.task, "/wki", ordinary), 0);
    ordinary.reset();
    vfs::LookupHandle compatibility;
    KEXPECT_EQ(vfs::vfs_acquire_lookup(subject.task.get(), vfs::AT_FDCWD, "/data", vfs::LookupIntent::STAT,
                                       vfs::LookupFollowPolicy::FOLLOW_FINAL, false, false, &compatibility, 0, true),
               -ENOTSUP);
    KEXPECT_NULL(compatibility.mount());
    std::snprintf(subject.task->wki_submitter_hostname.data(), subject.task->wki_submitter_hostname.size(), "goal39-foreign-submitter");
    KEXPECT_EQ(task_lookup(*subject.task, "/data", compatibility), -ENOTSUP);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    subject.task->wki_submitter_hostname.fill('\0');
    vfs::LookupHandle pinned;
    KREQUIRE_EQ(task_lookup(*subject.task, "/data", pinned), 0);
    KEXPECT_EQ(vfs::retire_mount_namespace(views.first), -EBUSY);
    LookupObservation skipped;
    KEXPECT_EQ(vfs::vfs_consume_lookup(pinned, false, observe_lookup, &skipped), -EIDRM);
    KEXPECT_EQ(skipped.calls, 0U);
    KEXPECT_EQ(task_lookup(*subject.task, "/data", compatibility), -EIDRM);
    KEXPECT_NULL(compatibility.mount());
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentPath, NativeCoveredCrossingAndMissingEdgeRefusal) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    NativeTask first, second;
    KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
    KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
    KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/child", 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdirat(second.task.get(), vfs::AT_FDCWD, "/child", 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdirat(first.task.get(), vfs::AT_FDCWD, "/uncovered", 0755), 0);
    KREQUIRE_TRUE(first.open("/data") >= 0);
    auto root = scoped_mount(views.first, "/");
    KREQUIRE_TRUE(root);
    auto const CREDENTIALS = first.task->credentials();
    tmpfs::TmpfsLookupHandle backend;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(root.get()), "/child", &backend), 0);
    NativeFile cover(tmpfs::tmpfs_pin_directory_lookup(CREDENTIALS, backend));
    KREQUIRE_TRUE(cover);
    KREQUIRE_TRUE(vfs::retain_mount_for_open_file(root.get()));
    cover->mount_owner = root.get();
    cover->mount_dev_id = root->dev_id;
    cover->mount_generation = vfs::mount_table_generation_snapshot();
    KREQUIRE_EQ(vfs::prepare_mount_filesystem("/child", "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &views.pending_first,
                                              vfs::MountPathKind::RETAINED_PHYSICAL, views.first),
                0);
    KREQUIRE_EQ(vfs::publish_prepared_mount(&views.pending_first, "/child", cover.get()), 0);
    vfs::finish_prepared_mount(&views.pending_first);
    cover.reset();
    KREQUIRE_EQ(private_mount(views.first, "/uncovered"), 0);
    vfs::LookupHandle missing;
    KEXPECT_EQ(task_lookup(*first.task, "/uncovered", missing), -EXDEV);
    KEXPECT_NULL(missing.mount());
    int const CHILD_FD = first.open("/child/data");
    KREQUIRE_EQ(CHILD_FD < 0 ? CHILD_FD : 0, 0);
    auto child = scoped_mount(views.first, "/child");
    KREQUIRE_TRUE(child);
    vfs::Stat stat{};
    KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/child/data", 0, &stat), 0);
    KEXPECT_EQ(stat.st_dev, child->dev_id);
    KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/child/../data", 0, &stat), 0);
    KEXPECT_EQ(stat.st_dev, root->dev_id);
    auto sibling = scoped_mount(views.second, "/");
    KREQUIRE_TRUE(sibling);
    KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/child", 0, &stat), 0);
    KEXPECT_EQ(stat.st_dev, sibling->dev_id);
    KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/child/data", 0, &stat), -ENOENT);
    // The backend lookup's root pin must drain before namespace retirement.
    backend = {};
}

KTEST(CompartmentMount, SamePathsHaveDistinctBackingAndCacheIdentity) {
    auto const NS_BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    auto host_root = vfs::find_mount_point("/");
    KREQUIRE_TRUE(host_root);
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        KREQUIRE_EQ(private_mount(views.first, "/service"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/service"), 0);
        auto first = scoped_mount(views.first, "/service/data");
        auto second = scoped_mount(views.second, "/service/data");
        KREQUIRE_TRUE(first && second);
        KEXPECT_NE(first->dev_id, second->dev_id);
        KEXPECT_NE(native_root(first.get()), native_root(second.get()));
        auto first_root = scoped_mount(views.first, "/");
        auto second_root = scoped_mount(views.second, "/");
        KREQUIRE_TRUE(first_root && second_root);
        KEXPECT_NE(native_root(first_root.get()), tmpfs::get_root_node());
        KEXPECT_NE(native_root(second_root.get()), tmpfs::get_root_node());
        KEXPECT_NE(native_root(first_root.get()), native_root(second_root.get()));
        auto file_a = native_file(first.get(), "/data");
        auto file_b = native_file(second.get(), "/data");
        KREQUIRE_TRUE(file_a && file_b);
        char const A = 'a', B = 'b';
        KEXPECT_EQ(tmpfs::tmpfs_write(file_a.get(), &A, 1, 0), 1);
        KEXPECT_EQ(tmpfs::tmpfs_write(file_b.get(), &B, 1, 0), 1);
        vfs::mount_lookup_cache_reset_for_test();
        for (size_t iteration = 0; iteration < 32; ++iteration) {
            auto a = scoped_mount(views.first, "/service/data");
            auto b = scoped_mount(views.second, "/service/data");
            auto a_root = scoped_mount(views.first, "/unmounted/data");
            auto b_root = scoped_mount(views.second, "/unmounted/data");
            KEXPECT_EQ(a.get(), first.get());
            KEXPECT_EQ(b.get(), second.get());
            KEXPECT_EQ(a_root.get(), first_root.get());
            KEXPECT_EQ(b_root.get(), second_root.get());
            KEXPECT_EQ(vfs::find_mount_point("/unmounted/data").get(), host_root.get());
        }
        KEXPECT_TRUE(vfs::mount_lookup_cache_hits_for_test() > 0);
        char observed = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(file_a.get(), &observed, 1, 0), 1);
        KEXPECT_EQ(observed, A);
        KEXPECT_EQ(tmpfs::tmpfs_read(file_b.get(), &observed, 1, 0), 1);
        KEXPECT_EQ(observed, B);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 2U);
        KEXPECT_EQ(vfs::get_mount_count(views.second), 2U);
        for (size_t index = 0; index < HOST_COUNT; ++index) {
            vfs::MountSnapshot snapshot{};
            KREQUIRE_TRUE(vfs::get_mount_snapshot_at(index, &snapshot));
            KEXPECT_NE(snapshot.dev_id, first->dev_id);
            KEXPECT_NE(snapshot.dev_id, second->dev_id);
            KEXPECT_NE(snapshot.dev_id, first_root->dev_id);
            KEXPECT_NE(snapshot.dev_id, second_root->dev_id);
        }
        for (size_t index = 0; index < 2; ++index) {
            auto selected = vfs::get_mount_at(index, views.first);
            KREQUIRE_TRUE(selected);
            KEXPECT_TRUE(selected->namespace_owner == views.first);
        }
        KEXPECT_FALSE(vfs::get_mount_at(2, views.first));
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), NS_BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentMount, ReservationsClosureAndUnsupportedViewsFailBeforePublication) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    auto prepare = [](const mount_ns::NamespaceRef& scope, const char* type, vfs::PreparedMount& output) {
        return vfs::prepare_mount_filesystem("/same", type, nullptr, 0, nullptr, nullptr, nullptr, &output,
                                             vfs::MountPathKind::RETAINED_PHYSICAL, scope);
    };
    KREQUIRE_EQ(prepare(views.first, "tmpfs", views.pending_first), 0);
    KREQUIRE_EQ(prepare(views.second, "tmpfs", views.pending_second), 0);
    vfs::PreparedMount duplicate{};
    KEXPECT_EQ(prepare(views.first, "tmpfs", duplicate), -EBUSY);
    KEXPECT_NULL(duplicate.state);
    // Pending initialization holds the namespace; retirement closes admission
    // but preserves the prepared owner until its caller finishes rollback.
    KEXPECT_EQ(vfs::retire_mount_namespace(views.first), -EBUSY);
    KEXPECT_TRUE(views.first.closed());
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first), -EIDRM);
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_second), 0);
    KEXPECT_EQ(vfs::get_mount_count(views.first), 0U);
    KEXPECT_EQ(vfs::get_mount_count(views.second), 1U);
    vfs::finish_prepared_mount(&views.pending_first);
    KEXPECT_EQ(vfs::retire_mount_namespace(views.first), 0);
    KEXPECT_EQ(prepare(views.first, "tmpfs", duplicate), -EIDRM);
    for (auto* type : {"devfs", "remote", "fat32", "xfs"}) {
        KEXPECT_EQ(prepare(views.second, type, duplicate), -ENOTSUP);
        KEXPECT_NULL(duplicate.state);
    }
    mount_ns::NamespaceRef host;
    KEXPECT_EQ(vfs::retire_mount_namespace(host), -EINVAL);
    KEXPECT_FALSE(host.closed());
}

KTEST(CompartmentMount, ForeignDescendantsDoNotBlockOwnUnmount) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/same"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/same"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/same/child"), 0);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.first), 0);
    KEXPECT_FALSE(scoped_mount(views.first, "/same"));
    KEXPECT_EQ(vfs::get_mount_count(views.second), 2U);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.second), -EBUSY);
    KEXPECT_EQ(vfs::unmount_filesystem("/same/child", views.second), 0);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.second), 0);
}

KTEST(CompartmentMount, PivotRebasesOnlyExactViewAndOwnerReopensEpoch) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.first, "/new"), 0);
    KREQUIRE_EQ(private_mount(views.first, "/cache"), 0);
    // Same destination in a sibling must not collide with this pivot.
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/new/old"), 0);
    auto const HOST_COUNT = vfs::get_mount_count();
    auto host = vfs::find_mount_point("/");
    auto sibling = scoped_mount(views.second, "/new/old");
    KREQUIRE_TRUE(host && sibling);
    auto const SIBLING_ID = sibling->dev_id;
    uint32_t old_id = 0, new_id = 0, cache_id = 0;
    {
        auto old = scoped_mount(views.first, "/");
        auto new_root = scoped_mount(views.first, "/new");
        auto cache = scoped_mount(views.first, "/cache");
        KREQUIRE_TRUE(old && new_root && cache);
        old_id = old->dev_id;
        new_id = new_root->dev_id;
        cache_id = cache->dev_id;
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(new_root.get()), "/old", 0755), 0);
        KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(new_root.get()), "/cache", 0755), 0);
        // Exact owned lookup pins still enforce the existing pivot busy rule.
        KEXPECT_EQ(vfs::remap_mounts_for_pivot("/new", "/new/old", nullptr, nullptr, views.first), -EBUSY);
        KEXPECT_FALSE(vfs::mount_pivot_pending_for_test());
    }
    {
        vfs::PivotMountReferences references;
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(vfs::remap_mounts_for_pivot("/new", "/new/old", &ROOT, &references, views.first), 0);
        // Reopen before any assertion which could return; no lookup may enter
        // a deliberately pending pivot epoch.
        bool const PENDING = vfs::mount_pivot_pending_for_test();
        vfs::rebase_wki_mounts_for_new_root("/new", views.second);
        bool const FOREIGN_REJECTED = vfs::mount_pivot_pending_for_test();
        vfs::rebase_wki_mounts_for_new_root("/new", views.first);
        KEXPECT_TRUE(PENDING && FOREIGN_REJECTED);
        KEXPECT_FALSE(vfs::mount_pivot_pending_for_test());
    }
    auto old = scoped_mount(views.first, "/new/old");
    auto new_root = scoped_mount(views.first, "/new");
    auto cache = scoped_mount(views.first, "/new/cache");
    KREQUIRE_TRUE(old && new_root && cache);
    KEXPECT_EQ(old->dev_id, old_id);
    KEXPECT_EQ(new_root->dev_id, new_id);
    KEXPECT_EQ(cache->dev_id, cache_id);
    KEXPECT_TRUE(old->covered_directory && old->covered_directory->mount_owner == new_root.get());
    KEXPECT_TRUE(cache->covered_directory && cache->covered_directory->mount_owner == new_root.get());
    KEXPECT_EQ(scoped_mount(views.second, "/new/old")->dev_id, SIBLING_ID);
    KEXPECT_EQ(vfs::find_mount_point("/").get(), host.get());
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentMount, RetirementKeepsExplicitFileCapabilityUntilDrain) {
    auto const BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(private_mount(views.first, "/same"), 0);
        auto mount = scoped_mount(views.first, "/same");
        KREQUIRE_TRUE(mount);
        auto file = native_file(mount.get(), "/data");
        KREQUIRE_TRUE(file);
        auto const ID = mount->dev_id;
        mount.reset();
        KEXPECT_EQ(vfs::retire_mount_namespace(views.first), -EBUSY);
        KEXPECT_TRUE(views.first.closed());
        KEXPECT_FALSE(scoped_mount(views.first, "/same/data"));
        KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
        char const VALUE = 'c';
        KEXPECT_EQ(tmpfs::tmpfs_write(file.get(), &VALUE, 1, 0), 1);
        char actual = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(file.get(), &actual, 1, 0), 1);
        KEXPECT_EQ(actual, VALUE);
        KEXPECT_EQ(file->mount_dev_id, ID);
        file.reset();
        KEXPECT_EQ(vfs::retire_mount_namespace(views.first), 0);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 0U);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentMount, ForeignCoveredDirectoryCannotPublishMountEdge) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    auto first = scoped_mount(views.first, "/");
    auto second = scoped_mount(views.second, "/");
    KREQUIRE_TRUE(first && second);
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(first.get()), "/child", 0755), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(second.get()), "/child", 0755), 0);
    auto pin_cover = [&](vfs::MountPoint* mount) -> NativeFile {
        tmpfs::TmpfsLookupHandle lookup;
        if (tmpfs::tmpfs_acquire_lookup(ROOT, native_root(mount), "/child", &lookup) != 0) return {};
        NativeFile file(tmpfs::tmpfs_pin_directory_lookup(ROOT, lookup));
        if (!file || !vfs::retain_mount_for_open_file(mount)) return {};
        file->mount_owner = mount;
        file->mount_dev_id = mount->dev_id;
        file->mount_generation = vfs::mount_table_generation_snapshot();
        return file;
    };
    auto foreign = pin_cover(second.get());
    auto own = pin_cover(first.get());
    KREQUIRE_TRUE(foreign && own);
    KREQUIRE_EQ(vfs::prepare_mount_filesystem("/child", "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &views.pending_first,
                                              vfs::MountPathKind::RETAINED_PHYSICAL, views.first),
                0);
    auto const GENERATION = vfs::mount_table_generation_snapshot();
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first, "/child", foreign.get()), -EINVAL);
    KEXPECT_EQ(vfs::mount_table_generation_snapshot(), GENERATION);
    KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first, "/child", own.get()), 0);
    auto child = scoped_mount(views.first, "/child");
    KREQUIRE_TRUE(child);
    KEXPECT_EQ(child->covered_directory, own.get());
    KEXPECT_EQ(child->covered_directory->mount_owner, first.get());
    KEXPECT_EQ(vfs::get_mount_count(views.second), 1U);
}

KTEST(CompartmentMount, PrivateMountLimitRollsBackPreparedBacking) {
    auto const BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        for (size_t index = 0; index < mount_ns::MAX_PRIVATE_MOUNTS; ++index) {
            std::array<char, 32> path{};
            std::snprintf(path.data(), path.size(), "/mount%zu", index);
            KREQUIRE_EQ(private_mount(views.first, path.data()), 0);
        }
        auto const GENERATION = vfs::mount_table_generation_snapshot();
        KEXPECT_EQ(private_mount(views.first, "/exhausted"), -ENOSPC);
        KEXPECT_EQ(vfs::get_mount_count(views.first), mount_ns::MAX_PRIVATE_MOUNTS);
        KEXPECT_EQ(vfs::mount_table_generation_snapshot(), GENERATION);
        KEXPECT_FALSE(scoped_mount(views.first, "/exhausted"));
        // Rollback released the path reservation; removing one exact row lets
        // a fresh preparation publish the formerly rejected path.
        KEXPECT_EQ(vfs::unmount_filesystem("/mount0", views.first), 0);
        KEXPECT_EQ(private_mount(views.first, "/exhausted"), 0);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

namespace {
namespace procfs = vfs::procfs;

auto scoped_proc(Task& requester, const char* path, int* result = nullptr) -> NativeFile {
    auto* file = procfs::procfs_lookup_path_as(path, sched::task::process_pid(requester), 0, &requester, result);
    // Metadata lookup deliberately leaves operations unset; own the production
    // backend close path on every test return, including failed REQUIREs.
    if (file != nullptr) file->fops = procfs::get_procfs_fops();
    return NativeFile(file);
}
auto host_proc(uint64_t requester, const char* path) -> NativeFile {
    auto* file = procfs::procfs_lookup_path_as(path, requester);
    if (file != nullptr) file->fops = procfs::get_procfs_fops();
    return NativeFile(file);
}
auto proc_data(const NativeFile& file) -> const procfs::ProcFileData& {
    return *static_cast<const procfs::ProcFileData*>(file->private_data);
}
auto proc_text(const NativeFile& file, std::array<char, 8192>& output) -> ssize_t {
    output.fill('\0');
    return file->fops->vfs_read(file.get(), output.data(), output.size() - 1, 0);
}
auto status_row(const char* content, const char* key, uint64_t value) -> bool {
    std::array<char, 96> expected{};
    std::snprintf(expected.data(), expected.size(), "\n%s:\t%llu\n", key, static_cast<unsigned long long>(value));
    return std::strstr(content, expected.data()) != nullptr;
}
}  // namespace

KTEST(CompartmentProcfs, BindingCloningAndRetirementKeepExactViewAndTarget) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const ACTIVE = sched::get_active_task_count();
    auto const INDEXED = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask caller, target, sibling, foreign;
        KREQUIRE_TRUE(caller.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(second));
        KREQUIRE_TRUE(caller.publish());
        KREQUIRE_TRUE(target.publish());
        KREQUIRE_TRUE(sibling.publish());
        KREQUIRE_TRUE(foreign.publish());
        auto local = scoped_proc(*caller.task, "2/status");
        auto other = scoped_proc(*sibling.task, "2/status");
        KREQUIRE_TRUE(local && other);
        KEXPECT_TRUE(proc_data(local).target == target.task.get());
        KEXPECT_TRUE(proc_data(other).target == foreign.task.get());
        KEXPECT_TRUE(proc_data(local).pid_view == first);
        KEXPECT_EQ(proc_data(local).node.pid, target.task->pid);
        std::array<char, 64> path{};
        std::snprintf(path.data(), path.size(), "%llu/status", static_cast<unsigned long long>(target.task->pid));
        int result = 0;
        KEXPECT_FALSE(scoped_proc(*caller.task, path.data(), &result));
        KEXPECT_EQ(result, -ENOENT);
        auto host = host_proc(caller.task->pid, path.data());
        KREQUIRE_TRUE(host);
        KEXPECT_TRUE(proc_data(host).pid_view.is_host());
        KEXPECT_TRUE(proc_data(host).target == target.task.get());
        uint64_t generation = UINT64_MAX;
        NativeFile clone(procfs::procfs_open_lookup_as(ker::mod::security::Snapshot::bootstrap_root(), caller.task->pid, local.get(), 0,
                                                       false, &result, &generation));
        KREQUIRE_TRUE(clone);
        KEXPECT_EQ(result, 0);
        KEXPECT_TRUE(proc_data(clone).pid_view == first);
        KEXPECT_TRUE(proc_data(clone).target == target.task.get());
        KEXPECT_NULL(proc_data(clone).content);
        target.retire();
        RegistryTask replacement;
        KREQUIRE_TRUE(replacement.task->initialize_pid_namespace(first));
        KEXPECT_EQ(replacement.task->namespace_tid(), uint64_t{3});
        KREQUIRE_TRUE(replacement.publish());
        KEXPECT_FALSE(scoped_proc(*caller.task, "2/status", &result));
        KEXPECT_EQ(result, -ENOENT);
        KEXPECT_TRUE(proc_data(local).target == target.task.get());
        KEXPECT_TRUE(proc_data(clone).target == target.task.get());
        vfs::Stat stat{};
        KEXPECT_EQ(procfs::procfs_fill_stat(local.get(), &stat, 0), 0);
        KEXPECT_TRUE(first.operations_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_active_task_count(), ACTIVE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), INDEXED);
}

KTEST(CompartmentProcfs, DenseRootAndThreadListingsExcludeSiblingAndHostNames) {
    pid_ns::NamespaceRef first, second;
    KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
    KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
    RegistryTask caller, child, thread, foreign, host;
    KREQUIRE_TRUE(caller.task->initialize_pid_namespace(first));
    KREQUIRE_TRUE(child.task->initialize_pid_namespace(first));
    thread.task->is_thread = true;
    thread.task->owner_pid = caller.task->pid;
    KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *caller.task));
    KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(second));
    for (auto* fixture : {&caller, &child, &thread, &foreign, &host}) KREQUIRE_TRUE(fixture->publish());
    auto root = scoped_proc(*caller.task, "");
    auto tasks = scoped_proc(*caller.task, "1/task");
    KREQUIRE_TRUE(root && tasks);
    constexpr std::array<const char*, 10> FIXED{".",       "..",   "self",    "mounts",  "uptime",
                                                "version", "stat", "loadavg", "meminfo", "timekeeping"};
    vfs::DirEntry entry{};
    for (size_t i = 0; i < FIXED.size(); ++i) {
        KREQUIRE_EQ(root->fops->vfs_readdir(root.get(), &entry, i), 0);
        KEXPECT_EQ(std::strcmp(entry.d_name.data(), FIXED[i]), 0);
        KEXPECT_EQ(entry.d_off, uint64_t{i + 1});
    }
    bool saw_first = false, saw_child = false;
    for (size_t i = 10; i < 12; ++i) {
        KREQUIRE_EQ(root->fops->vfs_readdir(root.get(), &entry, i), 0);
        KEXPECT_EQ(entry.d_off, uint64_t{i + 1});
        saw_first |= std::strcmp(entry.d_name.data(), "1") == 0;
        saw_child |= std::strcmp(entry.d_name.data(), "2") == 0;
        KEXPECT_EQ(entry.d_type, vfs::DT_DIR);
    }
    KEXPECT_TRUE(saw_first && saw_child);
    entry.d_ino = UINT64_MAX;
    KEXPECT_EQ(root->fops->vfs_readdir(root.get(), &entry, 12), -ENOENT);
    KEXPECT_EQ(entry.d_ino, UINT64_MAX);
    bool saw_owner = false, saw_thread = false;
    for (size_t i = 2; i < 4; ++i) {
        KREQUIRE_EQ(tasks->fops->vfs_readdir(tasks.get(), &entry, i), 0);
        saw_owner |= std::strcmp(entry.d_name.data(), "1") == 0;
        saw_thread |= std::strcmp(entry.d_name.data(), "3") == 0;
    }
    KEXPECT_TRUE(saw_owner && saw_thread);
    KEXPECT_EQ(tasks->fops->vfs_readdir(tasks.get(), &entry, 4), -ENOENT);
    int result = 0;
    KEXPECT_FALSE(scoped_proc(*caller.task, "2/task/3/status", &result));
    KEXPECT_EQ(result, -ENOENT);
    auto selected = scoped_proc(*caller.task, "1/task/3/status");
    KREQUIRE_TRUE(selected);
    KEXPECT_TRUE(proc_data(selected).target == thread.task.get());
    KEXPECT_EQ(proc_data(selected).node.pid, thread.task->pid);
}

KTEST(CompartmentProcfs, PublicIdentityRowsAndWaitSelectorsUseLocalNames) {
    pid_ns::NamespaceRef scope;
    KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
    RegistryTask parent, child, thread;
    KREQUIRE_TRUE(parent.task->initialize_pid_namespace(scope));
    KREQUIRE_TRUE(child.task->initialize_pid_namespace(scope));
    auto const JOB = parent.task->job_identity_snapshot();
    child.task->initialize_job_identity(JOB.internal, JOB.visible);
    thread.task->is_thread = true;
    thread.task->owner_pid = child.task->pid;
    KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *child.task));
    for (auto* fixture : {&parent, &child, &thread}) KREQUIRE_TRUE(fixture->publish());
    WaitMembership membership{*parent.task, *child.task};
    InertOnExit const INERT{{parent.task.get(), child.task.get(), thread.task.get()}};
    KREQUIRE_TRUE(membership.publish());
    auto local = scoped_proc(*parent.task, "2/status");
    auto tid = scoped_proc(*parent.task, "2/task/3/status");
    KREQUIRE_TRUE(local && tid);
    std::array<char, 8192> output{};
    KREQUIRE_TRUE(proc_text(local, output) > 0);
    for (auto* row : {"Pid", "Tgid"}) KEXPECT_TRUE(status_row(output.data(), row, 2));
    for (auto* row : {"PPid", "PGID", "SID"}) KEXPECT_TRUE(status_row(output.data(), row, 1));
    KREQUIRE_TRUE(proc_text(tid, output) > 0);
    KEXPECT_TRUE(status_row(output.data(), "Pid", 3));
    KEXPECT_TRUE(status_row(output.data(), "Tgid", 2));
    std::array<char, 64> path{};
    std::snprintf(path.data(), path.size(), "%llu/status", static_cast<unsigned long long>(child.task->pid));
    auto host = host_proc(parent.task->pid, path.data());
    KREQUIRE_TRUE(host);
    KREQUIRE_TRUE(proc_text(host, output) > 0);
    KEXPECT_TRUE(status_row(output.data(), "Pid", child.task->pid));
    KEXPECT_TRUE(status_row(output.data(), "PPid", parent.task->pid));
    WaitRegistration registration{*parent.task};
    child_events::ClaimedEvent event;
    KREQUIRE_EQ(child_events::claim_or_register(*parent.task, *parent.task, child.task->pid, 0, true, event),
                child_events::ProbeResult::WOULD_BLOCK);
    auto physical_wait = scoped_proc(*parent.task, "1/status");
    KREQUIRE_TRUE(physical_wait);
    KREQUIRE_TRUE(proc_text(physical_wait, output) > 0);
    KEXPECT_TRUE(status_row(output.data(), "WaitingForPid", 0));
    child_events::cancel_wait(*parent.task);
    KREQUIRE_EQ(child_events::claim_or_register(*parent.task, *parent.task, 2, 0, true, event, child_events::View::PUBLIC),
                child_events::ProbeResult::WOULD_BLOCK);
    auto public_wait = scoped_proc(*parent.task, "1/status");
    KREQUIRE_TRUE(public_wait);
    KREQUIRE_TRUE(proc_text(public_wait, output) > 0);
    KEXPECT_TRUE(status_row(output.data(), "WaitingForPid", 2));
}

KTEST(CompartmentProcfs, DynamicSelfRequiresTypedReaderOfCapturedView) {
    pid_ns::NamespaceRef first, second;
    KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
    KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
    RegistryTask opener, reader, sibling;
    KREQUIRE_TRUE(opener.task->initialize_pid_namespace(first));
    KREQUIRE_TRUE(reader.task->initialize_pid_namespace(first));
    KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(second));
    auto link = scoped_proc(*opener.task, "self");
    KREQUIRE_TRUE(link);
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    std::array<char, 64> output{};
    uint64_t generation = UINT64_MAX;
    KEXPECT_EQ(
        procfs::procfs_readlink_as(link.get(), output.data(), output.size(), ROOT, reader.task->pid, &generation, false, reader.task.get()),
        ssize_t{7});
    KEXPECT_EQ(std::strcmp(output.data(), "/proc/2"), 0);
    KEXPECT_EQ(generation, uint64_t{0});
    output.fill('x');
    generation = UINT64_MAX;
    KEXPECT_EQ(procfs::procfs_readlink_as(link.get(), output.data(), output.size(), ROOT, sibling.task->pid, &generation, false,
                                          sibling.task.get()),
               -ssize_t{EXDEV});
    KEXPECT_EQ(output[0], 'x');
    KEXPECT_EQ(generation, UINT64_MAX);
    KEXPECT_EQ(procfs::procfs_readlink_as(link.get(), output.data(), output.size(), ROOT, reader.task->pid, &generation), -ssize_t{ESRCH});
    KEXPECT_EQ(output[0], 'x');
    auto inherited_host = host_proc(0xdead, "self");
    KREQUIRE_TRUE(inherited_host);
    KEXPECT_TRUE(proc_data(inherited_host).pid_view.is_host());
    output.fill('\0');
    std::array<char, 64> expected{};
    std::snprintf(expected.data(), expected.size(), "/proc/%llu", static_cast<unsigned long long>(reader.task->pid));
    KEXPECT_TRUE(procfs::procfs_readlink_as(inherited_host.get(), output.data(), output.size(), ROOT, reader.task->pid, &generation, false,
                                            reader.task.get()) > 0);
    KEXPECT_EQ(std::strcmp(output.data(), expected.data()), 0);
}

KTEST(CompartmentProcfs, ClosedViewRejectsAllObservationsWithoutPublishingOutputs) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask owner;
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(scope));
        auto root = scoped_proc(*owner.task, "");
        auto status = scoped_proc(*owner.task, "self/status");
        auto self = scoped_proc(*owner.task, "self");
        KREQUIRE_TRUE(root && status && self);
        scope.close();
        status->pos = 37;
        std::array<char, 64> output{};
        output.fill('x');
        KEXPECT_EQ(status->fops->vfs_read(status.get(), output.data(), output.size(), 0), -ssize_t{EIDRM});
        KEXPECT_EQ(output[0], 'x');
        KEXPECT_EQ(status->fops->vfs_lseek(status.get(), 0, 0), -off_t{EIDRM});
        KEXPECT_EQ(status->pos, off_t{37});
        vfs::DirEntry entry{};
        entry.d_ino = UINT64_MAX;
        KEXPECT_EQ(root->fops->vfs_readdir(root.get(), &entry, 0), -EIDRM);
        KEXPECT_EQ(entry.d_ino, UINT64_MAX);
        vfs::Stat stat{};
        stat.st_ino = 73;
        uint64_t generation = UINT64_MAX;
        KEXPECT_EQ(procfs::procfs_fill_stat(status.get(), &stat, 0, &generation), -EIDRM);
        KEXPECT_EQ(stat.st_ino, uint64_t{73});
        KEXPECT_EQ(generation, UINT64_MAX);
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        KEXPECT_EQ(procfs::procfs_readlink_as(self.get(), output.data(), output.size(), ROOT, owner.task->pid, &generation, false,
                                              owner.task.get()),
                   -ssize_t{EIDRM});
        KEXPECT_EQ(output[0], 'x');
        KEXPECT_EQ(generation, UINT64_MAX);
        int result = 0;
        NativeFile rejected(procfs::procfs_open_lookup_as(ROOT, owner.task->pid, status.get(), 0, false, &result, &generation));
        KEXPECT_FALSE(rejected);
        KEXPECT_EQ(result, -EIDRM);
        KEXPECT_EQ(generation, UINT64_MAX);
        KEXPECT_EQ(procfs::procfs_revalidate_open_result(status.get(), 0), -EIDRM);
        KEXPECT_EQ(procfs::procfs_revalidate_metadata_result(status.get(), 0), -EIDRM);
        KEXPECT_FALSE(scoped_proc(*owner.task, "", &result));
        KEXPECT_EQ(result, -EIDRM);
        KEXPECT_TRUE(scope.operations_drained());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentProcfs, UnsupportedGlobalsAndPlacementFailBeforeContentOrControlEffects) {
    pid_ns::NamespaceRef scope;
    KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
    RegistryTask owner;
    KREQUIRE_TRUE(owner.task->initialize_pid_namespace(scope));
    int result = 0;
    for (auto* path : {"kperf",       "kperfctl",       "kwkistat",     "kcpustat",
                       "kcpustate",   "kcontstat",      "kipcstat",     "kperfstatus",
                       "wki",         "wki/peers",      "wki/auth",     "wki/napi",
                       "wki/netdiag", "wki/pipes",      "wki/chaos",    "wki/chaos_workload",
                       "memacc",      "memacc/summary", "memacc/procs", "memacc/track/kmalloc_debug",
                       "blockq"}) {
        KEXPECT_FALSE(scoped_proc(*owner.task, path, &result));
        KEXPECT_EQ(result, -ENOTSUP);
        auto host = host_proc(owner.task->pid, path);
        KREQUIRE_TRUE(host);
        KEXPECT_TRUE(proc_data(host).pid_view.is_host());
    }
    procfs::ProcFileData denied{};
    KREQUIRE_EQ(scope.retain(denied.pid_view), 0);
    denied.node.type = procfs::ProcNodeType::KPERFCTL_FILE;
    vfs::File control{};
    control.private_data = &denied;
    control.pos = 73;
    KEXPECT_EQ(procfs::get_procfs_fops()->vfs_write(&control, "enable", 6, 0), -ssize_t{ENOTSUP});
    KEXPECT_EQ(control.pos, off_t{73});
    KEXPECT_NULL(denied.content);
    auto local = scoped_proc(*owner.task, "self/wki_remote_pid");
    KREQUIRE_TRUE(local);
    std::array<char, 8192> output{};
    KREQUIRE_TRUE(proc_text(local, output) > 0);
    KEXPECT_EQ(std::strcmp(output.data(), "0\n"), 0);
    auto cached = scoped_proc(*owner.task, "self/status");
    KREQUIRE_TRUE(cached);
    KREQUIRE_TRUE(proc_text(cached, output) > 0);
    owner.task->set_wki_remote_pid(9191);
    output.fill('x');
    KEXPECT_EQ(cached->fops->vfs_read(cached.get(), output.data(), output.size(), 0), -ssize_t{ENOTSUP});
    KEXPECT_EQ(output[0], 'x');
    KEXPECT_FALSE(scoped_proc(*owner.task, "self/status", &result));
    KEXPECT_EQ(result, -ENOTSUP);
    owner.task->set_wki_remote_pid(0);
    owner.task->set_wki_proxy_task_id(17);
    KEXPECT_FALSE(scoped_proc(*owner.task, "self/status", &result));
    KEXPECT_EQ(result, -ENOTSUP);
    owner.task->set_wki_proxy_task_id(0);
    KEXPECT_TRUE(scope.operations_drained());
}

KTEST(CompartmentProcfs, NativePrivateMountWalksAndDescriptorReopensKeepOwnedCapabilities) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    auto const HOST_MOUNTS = vfs::get_mount_count();
    {
        PrivateMounts views;
        pid_ns::NamespaceRef first_pid, second_pid;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(first_pid), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second_pid), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        NativeTask first, second;
        first.task->type = sched::task::TaskType::PROCESS;
        second.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(first.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(second.task->initialize_mount_namespace(views.second));
        KREQUIRE_TRUE(first.task->initialize_pid_namespace(first_pid));
        KREQUIRE_TRUE(second.task->initialize_pid_namespace(second_pid));
        for (auto* fixture : {&first, &second}) {
            KREQUIRE_EQ(vfs::vfs_mkdirat(fixture->task.get(), vfs::AT_FDCWD, "/proc", 0755), 0);
            KREQUIRE_EQ(vfs::vfs_mount_for_task(fixture->task.get(), nullptr, "/proc", "procfs"), 0);
        }
        {
            auto proc = scoped_mount(views.first, "/proc");
            KREQUIRE_TRUE(proc);
            KEXPECT_EQ(proc->fs_type, vfs::FSType::PROCFS);
            KEXPECT_TRUE(proc->read_only);
            KEXPECT_NULL(proc->device);
        }
        int const DATA = first.open("/data");
        int const FOREIGN = second.open("/sibling-only");
        KREQUIRE_TRUE(DATA >= 0 && FOREIGN >= 0);
        NativeFile original(vfs::vfs_get_file_retain(first.task.get(), DATA));
        KREQUIRE_TRUE(original);
        char const VALUE = 'q';
        KREQUIRE_EQ(tmpfs::tmpfs_write(original.get(), &VALUE, 1, 0), ssize_t{1});
        std::array<char, 64> path{};
        std::snprintf(path.data(), path.size(), "/proc/self/fd/%d", DATA);
        int const DUPLICATE = first.open(path.data(), 0);
        KREQUIRE_EQ(DUPLICATE < 0 ? DUPLICATE : 0, 0);
        NativeFile duplicate(vfs::vfs_get_file_retain(first.task.get(), DUPLICATE));
        KREQUIRE_TRUE(duplicate);
        KEXPECT_TRUE(duplicate.get() == original.get());
        int const STATUS = first.open("/proc/self/status", 0);
        KREQUIRE_TRUE(STATUS >= 0);
        NativeFile status(vfs::vfs_get_file_retain(first.task.get(), STATUS));
        KREQUIRE_TRUE(status);
        KEXPECT_TRUE(proc_data(status).pid_view == first_pid);
        KEXPECT_TRUE(proc_data(status).target == first.task.get());
        std::array<char, 8192> output{};
        KREQUIRE_TRUE(proc_text(status, output) > 0);
        KEXPECT_TRUE(status_row(output.data(), "Pid", 1));
        vfs::Stat stat{};
        KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/proc/self/status", 0, &stat), 0);
        std::array<char, 64> link{};
        KEXPECT_EQ(vfs::vfs_readlinkat(first.task.get(), vfs::AT_FDCWD, "/proc/self", link.data(), link.size()), ssize_t{7});
        KEXPECT_EQ(std::strcmp(link.data(), "/proc/1"), 0);
        int const THROUGH_ROOT = first.open("/proc/self/root/data", 0);
        KREQUIRE_TRUE(THROUGH_ROOT >= 0);
        KEXPECT_EQ(first.open("/proc/self/root/sibling-only", 0), -ENOENT);
        int const PARENT_DATA = first.open("/proc/../data", 0);
        KREQUIRE_TRUE(PARENT_DATA >= 0);
        KEXPECT_EQ(first.open("/proc/kperfctl", 0), -ENOTSUP);
        int const ROOT_FD = first.open("/proc", vfs::O_DIRECTORY);
        KREQUIRE_TRUE(ROOT_FD >= 0);
        NativeFile inherited_root(vfs::vfs_get_file_retain(first.task.get(), ROOT_FD));
        KREQUIRE_TRUE(inherited_root);
        vfs::FdReservation imported(second.task.get());
        KREQUIRE_TRUE(imported.result() >= 0);
        int const IMPORTED = imported.install(inherited_root.get());
        KREQUIRE_TRUE(IMPORTED >= 0);
        inherited_root.release();
        second.track(IMPORTED);
        KEXPECT_EQ(vfs::vfs_openat(second.task.get(), IMPORTED, "1/status", 0, 0), -EXDEV);
        first_pid.close();
        stat.st_ino = 73;
        KEXPECT_EQ(vfs::vfs_statat(first.task.get(), vfs::AT_FDCWD, "/proc/self/status", 0, &stat), -EIDRM);
        KEXPECT_EQ(stat.st_ino, uint64_t{73});
        char value = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(duplicate.get(), &value, 1, 0), ssize_t{1});
        KEXPECT_EQ(value, VALUE);
        KEXPECT_EQ(vfs::vfs_statat(second.task.get(), vfs::AT_FDCWD, "/proc/self/status", 0, &stat), 0);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_MOUNTS);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_MOUNTS);
}

KTEST(CompartmentProcfs, ScopedCensusAndRootFileOwnNamespaceUntilFinalClose) {
    auto const BASELINE = pid_ns::live_namespace_count();
    NativeFile retained;
    {
        pid_ns::NamespaceRef first, second;
        KREQUIRE_EQ(pid_ns::create_namespace(first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(second), 0);
        RegistryTask owner, child, foreign;
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(child.task->initialize_pid_namespace(first));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(second));
        for (auto* fixture : {&owner, &child, &foreign}) KREQUIRE_TRUE(fixture->publish());
        auto load = scoped_proc(*owner.task, "loadavg");
        KREQUIRE_TRUE(load);
        std::array<char, 8192> output{};
        KREQUIRE_TRUE(proc_text(load, output) > 0);
        KEXPECT_TRUE(std::strstr(output.data(), "0/2 2\n") != nullptr);
        retained = scoped_proc(*owner.task, "");
        KREQUIRE_TRUE(retained);
        KEXPECT_NULL(proc_data(retained).target);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE + 1);
    vfs::DirEntry entry{};
    KEXPECT_EQ(retained->fops->vfs_readdir(retained.get(), &entry, 0), 0);
    KEXPECT_EQ(std::strcmp(entry.d_name.data(), "."), 0);
    retained.reset();
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentProcfs, RenamedCoverAndReplacementTextCannotRebindMountedView) {
    PrivateMounts views;
    pid_ns::NamespaceRef scope;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    NativeTask subject;
    subject.task->type = sched::task::TaskType::PROCESS;
    KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
    KREQUIRE_TRUE(subject.task->initialize_pid_namespace(scope));
    KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/proc", 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/proc", "procfs"), 0);
    vfs::LookupHandle retained;
    KREQUIRE_EQ(task_lookup(*subject.task, "/proc/self/status", retained), 0);
    auto* const ORIGINAL = retained.backend_parent();
    auto root = scoped_mount(views.first, "/");
    KREQUIRE_TRUE(root);
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    {
        // Exercise an actual backend rename underneath an already published
        // cover. A matching new pathname must not stand in for the old inode.
        tmpfs::TmpfsLookupHandle source, destination;
        KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, native_root(root.get()), "/proc", &source), 0);
        KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, native_root(root.get()), "/moved", &destination), 0);
        KREQUIRE_EQ(tmpfs::tmpfs_rename_lookup(ROOT, source, destination, true, false), 0);
    }
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(root.get()), "/proc", 0755), 0);
    vfs::LookupHandle rejected;
    KEXPECT_EQ(task_lookup(*subject.task, "/proc/self/status", rejected), -EAGAIN);
    KEXPECT_NULL(rejected.backend_parent());
    KEXPECT_TRUE(retained.backend_parent() == ORIGINAL);
    KEXPECT_EQ(vfs::get_mount_count(views.first), 2U);
}
