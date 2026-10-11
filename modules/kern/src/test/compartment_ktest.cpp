#include <abi/callnums/net.h>
#include <abi/callnums/power.h>
#include <abi/callnums/vfs.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dev/pty.hpp>
#include <initializer_list>
#include <limits>
#include <memory>
#include <net/socket.hpp>
#include <net/wki/remote_compute.hpp>
#include <net/wki/resource_contract_model.hpp>
#include <platform/debug/ptrace.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/compartment_lifecycle.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/session.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/security/compartment_wki.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/net/sys_net.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <syscalls_impl/process/process.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <syscalls_impl/vfs/sys_vfs.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>
#include <util/hostname.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/filesystem_context.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/lookup.hpp>
#include <vfs/mount.hpp>

namespace abi = ker::abi;
namespace net_syscall = ker::syscall::net;
using ker::mod::security::compartment::Network;
namespace ipc = ker::mod::sched::ipc;
namespace resource = ker::mod::sched::resource;
using ker::mod::sched::task::Task;

namespace pid_ns = ker::mod::sched::pid_ns;
namespace sched = ker::mod::sched;
namespace session = ker::mod::sched::session;
namespace child_events = ker::syscall::process::child_events;
namespace lifecycle = ker::mod::sched::compartment;

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

namespace {
struct ReparentFixture {
    pid_ns::NamespaceRef scope, foreign;
    RegistryTask init, sibling, parent, child, tracer;
    WaitClaim old_claim, tracer_claim, reap_claim;
    bool initialized{};
    ~ReparentFixture() {
        child_events::cancel_claim(old_claim.value);
        child_events::cancel_claim(tracer_claim.value);
        child_events::cancel_claim(reap_claim.value);
        // Actual ownership may have moved to init. Make every fixture inert,
        // then drain through the real exit preparation, never raw link edits.
        for (auto* task : {init.task.get(), sibling.task.get(), parent.task.get(), child.task.get(), tracer.task.get()})
            if (task->state.load() != sched::task::TaskState::DEAD) task->state.store(sched::task::TaskState::EXITING);
        if (initialized) {
            child_events::prepare_process_exit(*parent.task);
            child_events::prepare_process_exit(*init.task);
            child_events::prepare_process_exit(*sibling.task);
            child_events::prepare_process_exit(*tracer.task);
        }
        KEXPECT_NULL(child.task->child_parent);
        KEXPECT_NULL(child.task->child_tracer);
        KEXPECT_EQ(child.task->child_membership_state.load(), sched::task::ChildMembershipState::UNLINKED);
    }
    auto prepare(bool foreign_child = false, bool publish_init = true) -> bool {
        if (pid_ns::create_namespace(scope) != 0 || pid_ns::create_namespace(foreign) != 0 || !init.task->initialize_pid_namespace(scope) ||
            !sibling.task->initialize_pid_namespace(foreign) || !parent.task->initialize_pid_namespace(scope) ||
            !child.task->initialize_pid_namespace(foreign_child ? foreign : scope) ||
            !tracer.task->initialize_pid_namespace(foreign_child ? foreign : scope))
            return false;
        initialized = true;
        make_wait_fixtures_inert({parent.task.get(), child.task.get(), tracer.task.get()});
        if ((publish_init && !init.publish()) || !sibling.publish()) return false;
        auto job = parent.task->job_identity();
        job.session_id = parent.task->pid;
        job.pgid = parent.task->pid;
        child.task->initialize_job_identity(job, session::Names{2, 2});
        child.task->exit_status = 0x3100;
        resource::charge_cpu(child.task.get(), 36, 17);
        return child_events::begin_publication(*parent.task, *child.task) && child_events::commit_publication(*child.task);
    }
};

enum class ReaperCutover { NONE, CLOSE, EXIT, REQUEST, NOTIFY, DEAD };
struct ReparentCutover {
    ReparentFixture& fixture;
    ReaperCutover action{ReaperCutover::NONE};
    Task* selected{};
    size_t deliveries{};
    static void run(Task& source, Task* init, bool before_delivery, void* context) {
        auto& self = *static_cast<ReparentCutover*>(context);
        KEXPECT_TRUE(&source == self.fixture.parent.task.get());
        if (before_delivery) {
            ++self.deliveries;
            // Commit inspected ACTIVE. Exit can win after that commit; this
            // also prevents an unscheduled fixture from becoming runnable.
            if (init != nullptr && init->state.load() == sched::task::TaskState::ACTIVE) init->state.store(sched::task::TaskState::EXITING);
            return;
        }
        self.selected = init;
        if (init == nullptr) return;
        KEXPECT_TRUE(init == self.fixture.init.task.get());
        KEXPECT_TRUE(init->ref_count.load() >= 2);
        switch (self.action) {
            case ReaperCutover::NONE:
                break;
            case ReaperCutover::CLOSE:
                self.fixture.scope.close();
                break;
            case ReaperCutover::EXIT:
                init->exit_in_progress.store(true);
                break;
            case ReaperCutover::REQUEST:
                init->process_exit_requested.store(true);
                break;
            case ReaperCutover::NOTIFY:
                init->exit_notify_ready.store(true);
                break;
            case ReaperCutover::DEAD:
                init->state.store(sched::task::TaskState::DEAD);
                break;
        }
    }
    void execute() { child_events::selftest_prepare_exit_reaper_cutover(*fixture.parent.task, run, this); }
};
}  // namespace

KTEST(CompartmentLifecycle, OrphansAdoptExactLocalInitAndFrozenEvents) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const REGISTRY_BASELINE = sched::get_scoped_pid_task_count();
    for (bool const ZOMBIE : {false, true}) {
        ReparentFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        KEXPECT_NE(fixture.init.task->pid, uint64_t{1});
        KEXPECT_EQ(fixture.init.task->namespace_tid(), uint64_t{1});
        KEXPECT_EQ(fixture.sibling.task->namespace_tid(), uint64_t{1});
        child_events::ClaimedEvent old_event;
        if (ZOMBIE) {
            KREQUIRE_TRUE(child_events::publish_exit(*fixture.child.task));
            KREQUIRE_EQ(child_events::try_claim(*fixture.parent.task, fixture.old_claim.value, 3, 0, old_event, child_events::View::PUBLIC),
                        child_events::ProbeResult::CLAIMED);
            auto job = fixture.child.task->job_identity();
            job.pgid = fixture.child.task->pid;
            KREQUIRE_TRUE(child_events::update_job_identity(*fixture.child.task, job, session::Names{2, 3}));
        }
        ReparentCutover cutover{fixture};
        cutover.execute();
        KEXPECT_TRUE(cutover.selected == fixture.init.task.get());
        KEXPECT_EQ(cutover.deliveries, size_t{1});
        KEXPECT_TRUE(fixture.child.task->child_parent == fixture.init.task.get());
        KEXPECT_EQ(fixture.child.task->parent_pid, fixture.init.task->pid);
        KEXPECT_EQ(child_events::namespace_parent_pid(*fixture.child.task), uint64_t{1});
        KEXPECT_NULL(fixture.parent.task->child_list_head);
        KEXPECT_NULL(fixture.sibling.task->child_list_head);
        KEXPECT_EQ(fixture.parent.task->ref_count.load(), uint32_t{1});
        KEXPECT_NULL(fixture.old_claim.value.event);
        if (ZOMBIE) KEXPECT_TRUE(!child_events::commit_claim(*fixture.parent.task, fixture.old_claim.value, old_event, false));
        if (!ZOMBIE) KREQUIRE_TRUE(child_events::publish_exit(*fixture.child.task));
        child_events::ClaimedEvent event;
        child_events::Selector const SELECTOR =
            ZOMBIE ? child_events::Selector{child_events::SELECT_PROCESS_GROUP | 2} : child_events::Selector{3};
        KREQUIRE_EQ(child_events::try_claim(*fixture.init.task, fixture.reap_claim.value, SELECTOR, 0, event, child_events::View::PUBLIC),
                    child_events::ProbeResult::CLAIMED);
        KEXPECT_EQ(event.visible_pid, uint64_t{3});
        KEXPECT_EQ(event.subject_pid, fixture.child.task->pid);
        KEXPECT_EQ(event.status, int32_t{0x3100});
        KEXPECT_EQ(event.user_time_us, uint64_t{17});
        KEXPECT_EQ(event.system_time_us, uint64_t{19});
        KEXPECT_EQ(event.process_group, fixture.parent.task->pid);
        KREQUIRE_TRUE(child_events::commit_claim(*fixture.init.task, fixture.reap_claim.value, event, false));
        KEXPECT_EQ(fixture.init.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(fixture.child.task->ref_count.load(), uint32_t{1});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY_BASELINE);
}

KTEST(CompartmentLifecycle, MissingClosedDyingAndForeignReapersNeverAdoptIntoHost) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const REGISTRY_BASELINE = sched::get_scoped_pid_task_count();
    for (auto const ACTION : {ReaperCutover::NONE, ReaperCutover::CLOSE, ReaperCutover::EXIT, ReaperCutover::REQUEST, ReaperCutover::NOTIFY,
                              ReaperCutover::DEAD}) {
        ReparentFixture fixture;
        KREQUIRE_TRUE(fixture.prepare(false, ACTION != ReaperCutover::NONE));
        KREQUIRE_TRUE(child_events::publish_exit(*fixture.child.task));
        ReparentCutover cutover{fixture, ACTION};
        cutover.execute();
        KEXPECT_TRUE(cutover.selected == (ACTION == ReaperCutover::NONE ? nullptr : fixture.init.task.get()));
        KEXPECT_NULL(fixture.child.task->child_parent);
        KEXPECT_NULL(fixture.init.task->child_list_head);
        KEXPECT_NULL(fixture.sibling.task->child_list_head);
        KEXPECT_EQ(child_events::namespace_parent_pid(*fixture.child.task), uint64_t{0});
        KEXPECT_EQ(fixture.child.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(fixture.init.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(fixture.parent.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(child_events::diagnostics(*fixture.parent.task).owned_event_count, size_t{0});
        KEXPECT_TRUE(fixture.scope.operations_drained());
    }
    {
        ReparentFixture fixture;
        KREQUIRE_TRUE(fixture.prepare(true));
        ReparentCutover cutover{fixture};
        cutover.execute();
        KEXPECT_TRUE(cutover.selected == fixture.init.task.get());
        KEXPECT_NULL(fixture.child.task->child_parent);
        KEXPECT_NULL(fixture.init.task->child_list_head);
        KEXPECT_NULL(fixture.sibling.task->child_list_head);
        KEXPECT_EQ(fixture.child.task->ref_count.load(), uint32_t{1});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY_BASELINE);
}

KTEST(CompartmentLifecycle, PrivateInitDiscardsParentAudienceAndPreservesHostAndTracerClaims) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        RegistryTask host;
        host.task->state.store(sched::task::TaskState::EXITING);
        ReparentFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        WaitMembership supervision{*host.task, *fixture.init.task};
        KREQUIRE_TRUE(supervision.publish());
        KREQUIRE_TRUE(child_events::attach_tracer(*fixture.child.task, *fixture.tracer.task));
        KREQUIRE_TRUE(child_events::publish_exit(*fixture.child.task));
        child_events::ClaimedEvent traced;
        KREQUIRE_EQ(child_events::try_claim(*fixture.tracer.task, fixture.tracer_claim.value, 3, 0, traced, child_events::View::PUBLIC),
                    child_events::ProbeResult::CLAIMED);
        ReparentCutover cutover{fixture};
        cutover.execute();
        KEXPECT_TRUE(fixture.child.task->child_parent == fixture.init.task.get());
        child_events::prepare_process_exit(*fixture.init.task);
        KEXPECT_NULL(fixture.child.task->child_parent);
        KEXPECT_TRUE(child_events::is_waitable_zombie(*fixture.child.task));
        KEXPECT_TRUE(fixture.tracer_claim.value.event == traced.node);
        KEXPECT_TRUE(fixture.init.task->child_parent == host.task.get());
        KEXPECT_EQ(child_events::namespace_parent_pid(*fixture.init.task), uint64_t{0});
        KEXPECT_EQ(child_events::diagnostics(*fixture.init.task).owned_event_count, size_t{0});
        KREQUIRE_TRUE(child_events::commit_claim(*fixture.tracer.task, fixture.tracer_claim.value, traced, false));
        KEXPECT_EQ(fixture.child.task->ref_count.load(), uint32_t{1});
        KREQUIRE_TRUE(child_events::publish_exit(*fixture.init.task));
        WaitClaim claim;
        child_events::ClaimedEvent event;
        KREQUIRE_EQ(child_events::try_claim(*host.task, claim.value, fixture.init.task->pid, 0, event, child_events::View::PUBLIC),
                    child_events::ProbeResult::CLAIMED);
        KEXPECT_EQ(event.visible_pid, fixture.init.task->pid);
        KREQUIRE_TRUE(child_events::commit_claim(*host.task, claim.value, event, false));
        KEXPECT_EQ(host.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(fixture.init.task->ref_count.load(), uint32_t{1});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

namespace {
struct RetirementFixture {
    pid_ns::NamespaceRef scope;
    RegistryTask init;
    size_t baseline{lifecycle::status().pending};
    ~RetirementFixture() {
        init.retire();
        for (size_t i = 0; i < pid_ns::MAX_NAMESPACES && lifecycle::status().pending > baseline; ++i)
            lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, baseline);
    }
    auto prepare() -> bool { return pid_ns::create_namespace(scope) == 0 && init.task->initialize_pid_namespace(scope); }
    auto begin() -> int {
        init.task->exit_in_progress.store(true, std::memory_order_release);
        return lifecycle::begin_init_retirement(*init.task);
    }
};
struct ExitDelivery {
    size_t count{};
    static void observe(Task& target, void* opaque) {
        auto& self = *static_cast<ExitDelivery*>(opaque);
        ++self.count;
        KEXPECT_TRUE(target.ref_count.load() >= 2);
        KEXPECT_TRUE(target.process_exit_requested.load(std::memory_order_acquire));
        KEXPECT_EQ(target.requested_process_exit_status.load(), 137);
        KEXPECT_EQ(target.requested_process_exit_wait_status.load(), 9);
        KEXPECT_TRUE((target.signal_pending_bits() & (uint64_t{1} << 8)) != 0);
        KEXPECT_FALSE(target.scheduler_published.load());
    }
};
struct RetirementDrain {
    size_t baseline{lifecycle::status().pending};
    ~RetirementDrain() {
        for (size_t i = 0; i < pid_ns::MAX_NAMESPACES && lifecycle::status().pending > baseline; ++i)
            lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, baseline);
    }
};
}  // namespace

KTEST(CompartmentLifecycle, InitClosureRequestsBoundedExactCohortsAndWaitsForOperations) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const REGISTRY = sched::get_scoped_pid_task_count();
    auto const STATUS = lifecycle::status();
    {
        RetirementFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        KREQUIRE_TRUE(fixture.init.publish());
        constexpr size_t VICTIMS = lifecycle::EXIT_REQUEST_BUDGET * 2 + 5;
        std::array<RegistryTask, VICTIMS> victims;
        for (size_t i = 0; i < victims.size(); ++i) {
            if (i == victims.size() - 1) {
                victims[i].task->is_thread = true;
                victims[i].task->owner_pid = victims[0].task->pid;
                KREQUIRE_TRUE(resource::bind_thread_owner(*victims[i].task, *victims[0].task));
            } else
                KREQUIRE_TRUE(victims[i].task->initialize_pid_namespace(fixture.scope));
            KREQUIRE_TRUE(victims[i].publish());
        }
        victims[5].task->exit_in_progress.store(true);
        victims[8].task->requested_process_exit_status.store(72);
        victims[8].task->requested_process_exit_wait_status.store(0x4800);
        victims[8].task->process_exit_requested.store(true);
        pid_ns::NamespaceRef foreign_scope;
        KREQUIRE_EQ(pid_ns::create_namespace(foreign_scope), 0);
        RegistryTask foreign, host;
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(foreign_scope));
        KREQUIRE_TRUE(foreign.publish());
        KREQUIRE_TRUE(host.publish());
        pid_ns::OperationLease operation;
        KREQUIRE_EQ(operation.begin(fixture.scope), 0);
        KREQUIRE_EQ(fixture.begin(), 0);
        KEXPECT_TRUE(fixture.scope.closed());
        KEXPECT_TRUE(fixture.scope.retiring());
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending + 1);
        KEXPECT_EQ(fixture.begin(), 0);
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending + 1);
        KEXPECT_TRUE(fixture.init.task->mount_namespace()->is_host());
        KEXPECT_FALSE(fixture.init.task->mount_namespace()->closed());
        KEXPECT_FALSE(fixture.init.task->ipc_namespace()->closed());
        KEXPECT_FALSE(foreign_scope.closed());
        ExitDelivery delivered;
        // Production excludes registry-only construction but must retain the
        // drain record while those visible constructors have not cleaned up.
        lifecycle::selftest_dispatch_once(ExitDelivery::observe, &delivered);
        KEXPECT_EQ(delivered.count, size_t{0});
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending + 1);
        fixture.init.retire();
        lifecycle::selftest_dispatch_once(ExitDelivery::observe, &delivered, true);
        KEXPECT_EQ(delivered.count, lifecycle::EXIT_REQUEST_BUDGET);
        lifecycle::selftest_dispatch_once(ExitDelivery::observe, &delivered, true);
        KEXPECT_EQ(delivered.count, lifecycle::EXIT_REQUEST_BUDGET * 2);
        lifecycle::selftest_dispatch_once(ExitDelivery::observe, &delivered, true);
        KEXPECT_EQ(delivered.count, VICTIMS - 2);
        KEXPECT_FALSE(host.task->process_exit_requested.load());
        KEXPECT_FALSE(foreign.task->process_exit_requested.load());
        KEXPECT_FALSE(victims[5].task->process_exit_requested.load());
        KEXPECT_EQ(victims[8].task->requested_process_exit_status.load(), 72);
        KEXPECT_EQ(victims[8].task->requested_process_exit_wait_status.load(), 0x4800);
        KEXPECT_TRUE(victims.back().task->ptrace_termination_pending());
        for (auto& victim : victims) victim.retire();
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending + 1);
        operation.reset();
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending);
        KEXPECT_EQ(lifecycle::status().exit_requests, STATUS.exit_requests + VICTIMS - 2);
        KEXPECT_EQ(fixture.begin(), 0);
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    KEXPECT_EQ(sched::get_scoped_pid_task_count(), REGISTRY);
}

KTEST(CompartmentLifecycle, ClosedLateMembershipCannotPublishAndExistingConstructionAborts) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        RetirementFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        RegistryTask preparing, late;
        KREQUIRE_TRUE(preparing.task->initialize_pid_namespace(fixture.scope));
        KREQUIRE_TRUE(late.task->initialize_pid_namespace(fixture.scope));
        PublicationRollback rollback{*preparing.task};
        KREQUIRE_TRUE(child_events::begin_publication(*fixture.init.task, *preparing.task));
        KREQUIRE_EQ(fixture.begin(), 0);
        KEXPECT_FALSE(preparing.publish());
        KEXPECT_FALSE(child_events::begin_publication(*fixture.init.task, *late.task));
        KEXPECT_NULL(late.task->child_parent);
        KEXPECT_EQ(late.task->ref_count.load(), uint32_t{1});
        child_events::abort_publication(*preparing.task);
        KEXPECT_NULL(fixture.init.task->child_list_head);
        KEXPECT_EQ(preparing.task->ref_count.load(), uint32_t{1});
        KEXPECT_EQ(fixture.init.task->ref_count.load(), uint32_t{1});
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, fixture.baseline);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentLifecycle, EveryPidPoolSlotHasRetirementCapacityAndReusableCleanState) {
    auto const BASELINE = pid_ns::live_namespace_count();
    auto const STATUS = lifecycle::status();
    {
        RetirementDrain drain;
        std::array<pid_ns::NamespaceRef, pid_ns::MAX_NAMESPACES> scopes;
        std::array<std::unique_ptr<Task>, pid_ns::MAX_NAMESPACES> inits;
        auto const AVAILABLE = pid_ns::MAX_NAMESPACES - BASELINE;
        for (size_t i = 0; i < AVAILABLE; ++i) {
            KREQUIRE_EQ(pid_ns::create_namespace(scopes[i]), 0);
            inits[i] = std::make_unique<Task>();
            inits[i]->pid = sched::task::get_next_pid();
            inits[i]->type = sched::task::TaskType::PROCESS;
            KREQUIRE_TRUE(inits[i]->initialize_pid_namespace(scopes[i]));
            inits[i]->exit_in_progress.store(true);
            KREQUIRE_EQ(lifecycle::begin_init_retirement(*inits[i]), 0);
        }
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending + AVAILABLE);
        for (size_t i = 0; i < AVAILABLE; ++i) lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, STATUS.pending);
        KEXPECT_EQ(lifecycle::status().completed, STATUS.completed + AVAILABLE);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    {
        RetirementFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        KEXPECT_FALSE(fixture.scope.retiring());
        KEXPECT_FALSE(fixture.scope.closed());
        KEXPECT_EQ(fixture.init.task->namespace_tid(), uint64_t{1});
        KREQUIRE_EQ(fixture.begin(), 0);
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
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
    KREQUIRE_EQ(task_lookup(*subject.task, "/data", compatibility), 0);
    KEXPECT_TRUE(compatibility.mount()->namespace_owner == views.first);
    compatibility.reset();
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

KTEST(CompartmentLifecycle, DeferredOwnedMountAndIpcDrainPreservesRetainedFileCapability) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    {
        PrivateMounts views;
        ipc::NamespaceRef names;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(ipc::create_namespace(names), 0);
        RetirementFixture fixture;
        KREQUIRE_TRUE(fixture.prepare());
        KREQUIRE_TRUE(fixture.init.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(fixture.init.task->initialize_ipc_namespace(names));
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.first, "/same"), 0);
        auto mount = scoped_mount(views.first, "/same");
        KREQUIRE_TRUE(mount);
        auto file = native_file(mount.get(), "/data");
        KREQUIRE_TRUE(file);
        auto const ID = file->mount_dev_id;
        mount.reset();
        KREQUIRE_EQ(fixture.begin(), 0);
        KEXPECT_TRUE(names.closed());
        KEXPECT_TRUE(views.first.closed());
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(lifecycle::status().pending, fixture.baseline + 1);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 2U);
        char const VALUE = 'r';
        KEXPECT_EQ(tmpfs::tmpfs_write(file.get(), &VALUE, 1, 0), 1);
        char actual = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(file.get(), &actual, 1, 0), 1);
        KEXPECT_EQ(actual, VALUE);
        KEXPECT_EQ(file->mount_dev_id, ID);
        file.reset();
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
        KEXPECT_EQ(lifecycle::status().pending, fixture.baseline + 1);
        lifecycle::selftest_dispatch_once(nullptr, nullptr);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 0U);
        KEXPECT_EQ(lifecycle::status().pending, fixture.baseline);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
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
        ssize_t{1});
    KEXPECT_EQ(std::strcmp(output.data(), "2"), 0);
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
        KEXPECT_EQ(vfs::vfs_readlinkat(first.task.get(), vfs::AT_FDCWD, "/proc/self", link.data(), link.size()), ssize_t{1});
        KEXPECT_EQ(std::strcmp(link.data(), "1"), 0);
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

namespace {
// Construct only an unpublished fixture from the production native pin/view/
// context factories. The prepared identities, not root/cwd strings, authorize it.
auto pin_procfs_fixture_directory(Task& task, vfs::MountPoint* mount, const char* path) -> NativeFile {
    auto const CREDENTIALS = task.credentials();
    tmpfs::TmpfsLookupHandle lookup;
    if (tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(mount), path, &lookup) != 0) return {};
    NativeFile file(tmpfs::tmpfs_pin_directory_lookup(CREDENTIALS, lookup));
    if (!file || !vfs::retain_mount_for_open_file(mount)) return {};
    file->mount_owner = mount;
    file->mount_dev_id = mount->dev_id;
    file->mount_generation = vfs::mount_table_generation_snapshot();
    return file;
}
auto prepare_procfs_fixture_root(Task& task, vfs::MountPoint* mount, const char* root, const char* cwd, vfs::MountPolicy policy = {})
    -> int {
    if (task.scheduler_published.load() || task.root_anchor || task.cwd_anchor || task.mount_namespace() == nullptr ||
        *task.mount_namespace() != mount->namespace_owner)
        return -EINVAL;
    auto const CREDENTIALS = task.credentials();
    auto root_pin = pin_procfs_fixture_directory(task, mount, root);
    auto cwd_pin = pin_procfs_fixture_directory(task, mount, cwd);
    if (!root_pin || !cwd_pin) return -ENOMEM;
    vfs::DirectoryViewRef view;
    vfs::PreparedFilesystemContext prepared;
    int result = vfs::vfs_prepare_directory_view(CREDENTIALS, root_pin.get(), cwd_pin.get(), policy, view);
    if (result == 0) result = vfs::vfs_prepare_filesystem_context(CREDENTIALS, view, prepared);
    if (result != 0) return result;
    auto const& filesystem = prepared.filesystem();
    task.root_anchor = filesystem.root_anchor;
    task.cwd_anchor = filesystem.cwd_anchor;
    task.root = filesystem.root;
    task.cwd = filesystem.cwd;
    task.root_len = filesystem.root_len;
    task.cwd_len = filesystem.cwd_len;
    return 0;
}
auto observe_procfs_lookup(const vfs::LookupHandle& handle, void* opaque) -> int {
    ++*static_cast<unsigned*>(opaque);
    return procfs::procfs_revalidate_lookup_result(static_cast<const vfs::File*>(handle.backend_parent()));
}
}  // namespace

KTEST(CompartmentProcfs, OwnedNativeRootsKeepProcfsNamesPolicyAndDirectoryBoundaries) {
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
            for (auto* path : {"/service", "/service/cwd", "/service/proc", "/service/nested", "/outside"})
                KREQUIRE_EQ(vfs::vfs_mkdirat(fixture->task.get(), vfs::AT_FDCWD, path, 0755), 0);
            KREQUIRE_EQ(vfs::vfs_mount_for_task(fixture->task.get(), nullptr, "/service/proc", "procfs"), 0);
            KREQUIRE_EQ(vfs::vfs_mount_for_task(fixture->task.get(), nullptr, "/service/nested", "tmpfs", 0, "size=1M"), 0);
            KREQUIRE_EQ(vfs::vfs_mkdirat(fixture->task.get(), vfs::AT_FDCWD, "/service/nested/proc", 0755), 0);
            KREQUIRE_EQ(vfs::vfs_mount_for_task(fixture->task.get(), nullptr, "/service/nested/proc", "procfs"), 0);
            int const DATA = fixture->open("/service/data"), OUTSIDE = fixture->open("/outside/secret");
            KREQUIRE_TRUE(DATA >= 0 && OUTSIDE >= 0);
            KREQUIRE_EQ(fixture->close(DATA), 0);
            KREQUIRE_EQ(fixture->close(OUTSIDE), 0);
            KREQUIRE_EQ(vfs::vfs_symlinkat(fixture->task.get(), "/proc/self/status", vfs::AT_FDCWD, "/service/status"), 0);
            auto mount = scoped_mount(*fixture->task->mount_namespace(), "/");
            KREQUIRE_TRUE(mount);
            KREQUIRE_EQ(
                prepare_procfs_fixture_root(*fixture->task, mount.get(), "/service", "/service/cwd", {.read_only = true, .nosuid = true}),
                0);
            KEXPECT_TRUE(fixture->task->root_anchor && fixture->task->cwd_anchor);
            KEXPECT_EQ(std::strcmp(fixture->task->root.data(), "/service"), 0);
            KEXPECT_EQ(std::strcmp(fixture->task->cwd.data(), "/cwd"), 0);
        }
        for (auto* fixture : {&first, &second}) {
            for (auto* path : {"/proc/self/status", "../proc/self/status", "/status", "/nested/proc/self/status"}) {
                int const FD = fixture->open(path, 0);
                KREQUIRE_TRUE(FD >= 0);
                NativeFile file(vfs::vfs_get_file_retain(fixture->task.get(), FD));
                KREQUIRE_TRUE(file);
                KEXPECT_TRUE(proc_data(file).target == fixture->task.get());
                KEXPECT_TRUE(proc_data(file).pid_view == *fixture->task->pid_namespace());
                KEXPECT_TRUE(file->mount_policy.read_only && file->mount_policy.nosuid);
                KEXPECT_FALSE(file->directory_view);
                std::array<char, 8192> output{};
                KREQUIRE_TRUE(proc_text(file, output) > 0);
                KEXPECT_TRUE(status_row(output.data(), "Pid", 1));
                KREQUIRE_EQ(fixture->close(FD), 0);
            }
            vfs::Stat stat{};
            KEXPECT_EQ(vfs::vfs_statat(fixture->task.get(), vfs::AT_FDCWD, "/proc/self/status", 0, &stat), 0);
            KEXPECT_EQ(vfs::vfs_statat(fixture->task.get(), vfs::AT_FDCWD, "/proc", 0, &stat), 0);
            std::array<char, 64> link{};
            KEXPECT_EQ(vfs::vfs_readlinkat(fixture->task.get(), vfs::AT_FDCWD, "/proc/self", link.data(), link.size()), ssize_t{1});
            KEXPECT_EQ(std::strcmp(link.data(), "1"), 0);
            int const PARENT = fixture->open("/proc/../../data", 0);
            KREQUIRE_TRUE(PARENT >= 0);
            KREQUIRE_EQ(fixture->close(PARENT), 0);
            KEXPECT_EQ(fixture->open("/proc/../../outside/secret", 0), -ENOENT);
            KEXPECT_EQ(fixture->open("/proc/kperfctl", 0), -ENOTSUP);
            KEXPECT_EQ(fixture->open("/new", vfs::O_CREAT | 2), -EROFS);
            auto const FDS = fixture->task->fd_table.size();
            int const PROC_DIR = fixture->open("/proc", vfs::O_DIRECTORY);
            KREQUIRE_TRUE(PROC_DIR >= 0);
            auto* directory = vfs::vfs_get_file(fixture->task.get(), PROC_DIR);
            KREQUIRE_TRUE(directory != nullptr && directory->directory_view);
            KEXPECT_TRUE(directory->directory_view.policy().read_only && directory->directory_view.policy().nosuid);
            KREQUIRE_EQ(fixture->close(PROC_DIR), 0);
            int const THROUGH_ROOT = fixture->open("/proc/self/root/data", 0);
            KREQUIRE_TRUE(THROUGH_ROOT >= 0);
            KREQUIRE_EQ(fixture->close(THROUGH_ROOT), 0);
            KEXPECT_EQ(fixture->task->fd_table.size(), FDS);
            KEXPECT_EQ(fixture->task->fd_table.reserved_count(), 0U);
        }
        int const ROOT_FD = first.open("/", vfs::O_DIRECTORY);
        KREQUIRE_TRUE(ROOT_FD >= 0);
        int const STATUS = first.track(vfs::vfs_openat(first.task.get(), ROOT_FD, "proc/self/status", 0, 0));
        KREQUIRE_TRUE(STATUS >= 0);
        NativeFile status(vfs::vfs_get_file_retain(first.task.get(), STATUS));
        KREQUIRE_TRUE(status);
        KEXPECT_TRUE(proc_data(status).pid_view == first_pid);
        KEXPECT_TRUE(status->mount_policy.read_only && status->mount_policy.nosuid);
        NativeFile root(vfs::vfs_get_file_retain(first.task.get(), ROOT_FD));
        KREQUIRE_TRUE(root && root->directory_view);
        vfs::FdReservation imported(second.task.get());
        int const INHERITED = imported.install(root.get());
        KREQUIRE_TRUE(INHERITED >= 0);
        root.release();
        second.track(INHERITED);
        KEXPECT_EQ(vfs::vfs_openat(second.task.get(), INHERITED, "proc/self/status", 0, 0), -EXDEV);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_MOUNTS);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_MOUNTS);
}

KTEST(CompartmentProcfs, OwnedRootCoverMutationAndClosureRejectBeforePublication) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        pid_ns::NamespaceRef pid_view;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(pid_view), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        NativeTask subject;
        subject.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pid_view));
        for (auto* path : {"/service", "/service/proc", "/outside"})
            KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, path, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/service/proc", "procfs"), 0);
        auto mount = scoped_mount(views.first, "/");
        KREQUIRE_TRUE(mount);
        KREQUIRE_EQ(prepare_procfs_fixture_root(*subject.task, mount.get(), "/service", "/service"), 0);
        vfs::LookupHandle selected;
        KREQUIRE_EQ(task_lookup(*subject.task, "/proc/self/status", selected), 0);
        KREQUIRE_TRUE(selected.directory_view() && selected.mount_membership());
        auto const* ORIGINAL = static_cast<const vfs::File*>(selected.backend_parent());
        KREQUIRE_TRUE(ORIGINAL && ORIGINAL->fs_type == vfs::FSType::PROCFS);
        unsigned calls = 0;
        KEXPECT_EQ(vfs::vfs_consume_lookup(selected, false, observe_procfs_lookup, &calls), 0);
        KEXPECT_EQ(calls, 1U);
        int const STATUS = subject.open("/proc/self/status", 0);
        KREQUIRE_TRUE(STATUS >= 0);
        NativeFile retained(vfs::vfs_get_file_retain(subject.task.get(), STATUS));
        KREQUIRE_TRUE(retained);
        auto const ROOT = subject.task->credentials();
        {
            tmpfs::TmpfsLookupHandle source, destination;
            KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, native_root(mount.get()), "/service/proc", &source), 0);
            KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, native_root(mount.get()), "/outside/hidden", &destination), 0);
            KREQUIRE_EQ(tmpfs::tmpfs_rename_lookup(ROOT, source, destination, true, false), 0);
        }
        KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(mount.get()), "/service/proc", 0755), 0);
        calls = 0;
        KEXPECT_EQ(vfs::vfs_consume_lookup(selected, false, observe_procfs_lookup, &calls), -EAGAIN);
        KEXPECT_EQ(calls, 0U);
        KEXPECT_TRUE(selected.backend_parent() == ORIGINAL);
        vfs::Stat stat{};
        stat.st_ino = 79;
        KEXPECT_EQ(vfs::vfs_statat(subject.task.get(), vfs::AT_FDCWD, "/proc/self/status", 0, &stat), -EAGAIN);
        KEXPECT_EQ(stat.st_ino, uint64_t{79});
        auto const FDS = subject.task->fd_table.size();
        KEXPECT_EQ(subject.open("/proc/self/status", 0), -EAGAIN);
        KEXPECT_EQ(subject.task->fd_table.size(), FDS);
        std::array<char, 8192> output{};
        KREQUIRE_TRUE(proc_text(retained, output) > 0);
        pid_view.close();
        calls = 0;
        KEXPECT_EQ(vfs::vfs_consume_lookup(selected, false, observe_procfs_lookup, &calls), -EIDRM);
        KEXPECT_EQ(calls, 0U);
        output[0] = '!';
        KEXPECT_EQ(retained->fops->vfs_read(retained.get(), output.data(), output.size() - 1, 0), ssize_t{-EIDRM});
        KEXPECT_EQ(output[0], '!');
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentProcfs, ScopedMagicLinksIntersectNativeRootsAndKeepRestrictions) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        pid_ns::NamespaceRef pid_view;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(pid_view), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        NativeTask subject;
        subject.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pid_view));
        for (auto* path : {"/service", "/service/proc", "/service/narrow", "/service/narrow/proc", "/service/outside"})
            KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, path, 0755), 0);
        for (auto* path : {"/service/proc", "/service/narrow/proc"})
            KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, path, "procfs"), 0);
        int const SEED = subject.open("/service/narrow/leaf"), SECRET = subject.open("/service/outside/secret");
        KREQUIRE_TRUE(SEED >= 0 && SECRET >= 0);
        NativeFile seed(vfs::vfs_get_file_retain(subject.task.get(), SEED));
        KREQUIRE_TRUE(seed);
        char const VALUE = 'q';
        KREQUIRE_EQ(tmpfs::tmpfs_write(seed.get(), &VALUE, 1, 0), ssize_t{1});
        KREQUIRE_EQ(subject.close(SEED), 0);
        KREQUIRE_EQ(subject.close(SECRET), 0);
        KREQUIRE_EQ(vfs::vfs_symlinkat(subject.task.get(), "/narrow/leaf", vfs::AT_FDCWD, "/service/narrow/absolute"), 0);
        KREQUIRE_EQ(vfs::vfs_symlinkat(subject.task.get(), "/outside/secret", vfs::AT_FDCWD, "/service/narrow/escape"), 0);
        auto mount = scoped_mount(views.first, "/");
        KREQUIRE_TRUE(mount);
        KREQUIRE_EQ(prepare_procfs_fixture_root(*subject.task, mount.get(), "/service", "/service/narrow"), 0);
        auto narrow_pin = pin_procfs_fixture_directory(*subject.task, mount.get(), "/service/narrow");
        KREQUIRE_TRUE(narrow_pin);
        vfs::DirectoryViewRef narrow;
        KREQUIRE_EQ(vfs::vfs_prepare_directory_view(subject.task->credentials(), narrow_pin.get(), narrow_pin.get(),
                                                    {.read_only = true, .nosuid = true}, narrow),
                    0);
        int const DIR = subject.open("/narrow", vfs::O_DIRECTORY);
        KREQUIRE_TRUE(DIR >= 0);
        NativeFile directory(vfs::vfs_get_file_retain(subject.task.get(), DIR));
        KREQUIRE_TRUE(directory && directory->directory_view);
        KREQUIRE_TRUE(directory->private_data == narrow_pin->private_data);
        KREQUIRE_TRUE(!subject.task->scheduler_published.load());
        // Prepare this exclusively owned unpublished fixture's inherited FD
        // from the native view factory. The explicit view path walker serves
        // Host exports; it intentionally refuses private mount ownership.
        directory->mount_policy = directory->mount_policy.restricted_by(narrow.policy());
        directory->directory_view = std::move(narrow);
        vfs::Stat expected{};
        KREQUIRE_EQ(vfs::vfs_fstat_file(seed.get(), &expected), 0);
        for (auto* path : {"proc/1/cwd/leaf", "proc/1/cwd/absolute", "proc/1/cwd/proc/1/cwd/leaf"}) {
            int const FD = subject.track(vfs::vfs_openat(subject.task.get(), DIR, path, 0, 0));
            KREQUIRE_TRUE(FD >= 0);
            NativeFile selected(vfs::vfs_get_file_retain(subject.task.get(), FD));
            KREQUIRE_TRUE(selected);
            vfs::Stat actual{};
            KREQUIRE_EQ(vfs::vfs_fstat_file(selected.get(), &actual), 0);
            KEXPECT_EQ(actual.st_ino, expected.st_ino);
            KEXPECT_TRUE(selected->mount_policy.read_only && selected->mount_policy.nosuid);
            KREQUIRE_EQ(subject.close(FD), 0);
        }
        std::array<char, 64> magic{};
        std::snprintf(magic.data(), magic.size(), "/proc/1/fd/%d/absolute", DIR);
        int const THROUGH_FD = subject.open(magic.data(), 0);
        KREQUIRE_TRUE(THROUGH_FD >= 0);
        NativeFile through(vfs::vfs_get_file_retain(subject.task.get(), THROUGH_FD));
        KREQUIRE_TRUE(through);
        KEXPECT_TRUE(through->mount_policy.read_only && through->mount_policy.nosuid);
        KREQUIRE_EQ(subject.close(THROUGH_FD), 0);
        auto const FDS = subject.task->fd_table.size();
        for (auto* path : {"proc/1/root/narrow/leaf", "proc/1/cwd/escape"})
            KEXPECT_EQ(vfs::vfs_openat(subject.task.get(), DIR, path, 0, 0), -EPERM);
        KEXPECT_EQ(vfs::vfs_openat(subject.task.get(), DIR, "proc/1/cwd/leaf", vfs::O_TRUNC | 2, 0), -EROFS);
        KEXPECT_EQ(vfs::vfs_openat(subject.task.get(), DIR, "proc/1/cwd/new", vfs::O_CREAT | 2, 0600), -EROFS);
        char actual = '\0';
        KEXPECT_EQ(tmpfs::tmpfs_read(seed.get(), &actual, 1, 0), ssize_t{1});
        KEXPECT_EQ(actual, VALUE);
        KEXPECT_EQ(subject.task->fd_table.size(), FDS);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentProcfs, RepeatedMagicLinksRetainEveryOriginAndDrainAtDepthLimit) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        pid_ns::NamespaceRef pid_view;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(pid_view), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        NativeTask subject;
        subject.task->type = sched::task::TaskType::PROCESS;
        RegistryTask target;
        for (auto* task : {subject.task.get(), target.task.get()}) {
            KREQUIRE_TRUE(task->initialize_mount_namespace(views.first));
            KREQUIRE_TRUE(task->initialize_pid_namespace(pid_view));
        }
        for (auto* path : {"/service", "/service/proc"}) KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, path, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/service/proc", "procfs"), 0);
        int const SEED = subject.open("/service/data");
        KREQUIRE_TRUE(SEED >= 0);
        KREQUIRE_EQ(subject.close(SEED), 0);
        auto mount = scoped_mount(views.first, "/");
        KREQUIRE_TRUE(mount);
        for (auto* task : {subject.task.get(), target.task.get()})
            KREQUIRE_EQ(prepare_procfs_fixture_root(*task, mount.get(), "/service", "/service"), 0);
        KREQUIRE_TRUE(target.publish());
        auto const SUBJECT_REFS = subject.task->ref_count.load();
        auto const TARGET_REFS = target.task->ref_count.load();
        vfs::LookupHandle captured;
        KREQUIRE_EQ(task_lookup(*subject.task, "/proc/1/root/proc/2/root/data", captured), 0);
        KEXPECT_TRUE(captured.procfs_view_origin() && captured.procfs_view_origins());
        vfs::LookupHandle moved(std::move(captured));
        KEXPECT_NULL(captured.procfs_view_origin());
        KEXPECT_NULL(captured.procfs_view_origins());
        LookupObservation observation;
        KEXPECT_EQ(vfs::vfs_consume_lookup(moved, false, observe_lookup, &observation), 0);
        KEXPECT_EQ(observation.calls, 1U);
        auto const ORIGINAL = target.task->credentials();
        ker::mod::security::Snapshot changed;
        KREQUIRE_EQ(ker::mod::security::Snapshot::restore_fields({.user = {100, 100, 100}, .group = {100, 100, 100}}, changed), 0);
        // Exclusively owned, unscheduled construction fixtures exercise the
        // real immutable credential cutover without editing a live task.
        KREQUIRE_EQ(resource::initialize_credentials(*target.task, changed), 0);
        observation = {};
        KEXPECT_EQ(vfs::vfs_consume_lookup(moved, false, observe_lookup, &observation), -EAGAIN);
        KEXPECT_EQ(observation.calls, 0U);
        moved.reset();
        KEXPECT_EQ(subject.task->ref_count.load(), SUBJECT_REFS);
        KEXPECT_EQ(target.task->ref_count.load(), TARGET_REFS);
        KREQUIRE_EQ(resource::initialize_credentials(*target.task, ORIGINAL), 0);
        KREQUIRE_EQ(task_lookup(*subject.task, "/proc/1/root/proc/2/root/data", moved), 0);
        target.task->state.store(sched::task::TaskState::EXITING);
        KEXPECT_EQ(vfs::vfs_consume_lookup(moved, false, observe_lookup, &observation), -ESRCH);
        KEXPECT_EQ(observation.calls, 0U);
        moved.reset();
        target.task->state.store(sched::task::TaskState::ACTIVE);
        std::array<char, 256> path{};
        for (unsigned depth = 1; depth <= 9; ++depth) {
            std::strcat(path.data(), "/proc/1/root");
            std::array<char, 256> selected = path;
            std::strcat(selected.data(), "/data");
            vfs::LookupHandle lookup;
            KEXPECT_EQ(task_lookup(*subject.task, selected.data(), lookup), depth <= 8 ? 0 : -ELOOP);
            if (depth <= 8) {
                observation = {};
                KEXPECT_EQ(vfs::vfs_consume_lookup(lookup, false, observe_lookup, &observation), 0);
                KEXPECT_EQ(observation.calls, 1U);
                KEXPECT_TRUE(depth == 1 ? lookup.procfs_view_origins() == nullptr : lookup.procfs_view_origins() != nullptr);
            } else {
                KEXPECT_NULL(lookup.backend_parent());
                KEXPECT_NULL(lookup.procfs_view_origins());
            }
            lookup.reset();
            KEXPECT_EQ(subject.task->ref_count.load(), SUBJECT_REFS);
            KEXPECT_EQ(target.task->ref_count.load(), TARGET_REFS);
        }
        KREQUIRE_EQ(task_lookup(*subject.task, "/proc/1/root/proc/2/root/data", moved), 0);
        pid_view.close();
        observation = {};
        KEXPECT_EQ(vfs::vfs_consume_lookup(moved, false, observe_lookup, &observation), -EIDRM);
        KEXPECT_EQ(observation.calls, 0U);
        moved.reset();
        KEXPECT_EQ(subject.task->ref_count.load(), SUBJECT_REFS);
        KEXPECT_EQ(target.task->ref_count.load(), TARGET_REFS);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

namespace {
auto proc_component(Task& caller, const NativeFile& directory, const char* name, int* result = nullptr) -> NativeFile {
    auto* file =
        procfs::procfs_lookup_component_as(caller.credentials(), sched::task::process_pid(caller), directory.get(), name, &caller, result);
    if (file != nullptr) file->fops = procfs::get_procfs_fops();
    return NativeFile(file);
}
struct ProcFixtureFd {
    Task& task;
    int fd = -1;
    ~ProcFixtureFd() {
        if (fd >= 0) KEXPECT_EQ(vfs::vfs_close_for_task(&task, fd), 0);
    }
};
}  // namespace

KTEST(CompartmentProcfs, DirectoryDescriptorsKeepTargetAfterRegistryWithdrawal) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        pid_ns::NamespaceRef pid_view, foreign_pid;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(pid_view), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(foreign_pid), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        RegistryTask target;
        NativeTask subject, foreign;
        subject.task->type = foreign.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pid_view));
        KREQUIRE_TRUE(target.task->initialize_pid_namespace(pid_view));
        KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(foreign_pid));
        for (auto* task : {subject.task.get(), target.task.get(), foreign.task.get()})
            KREQUIRE_TRUE(task->initialize_mount_namespace(views.first));
        KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/proc", 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/proc", "procfs"), 0);
        ProcFixtureFd target_fd{*target.task};
        target_fd.fd = vfs::vfs_openat(target.task.get(), vfs::AT_FDCWD, "/target-data", vfs::O_CREAT | 2, 0600);
        KREQUIRE_TRUE(target_fd.fd >= 0);
        NativeFile original(vfs::vfs_get_file_retain(target.task.get(), target_fd.fd));
        KREQUIRE_TRUE(original);
        KREQUIRE_TRUE(target.publish());
        int const DIR = subject.open("/proc/2", vfs::O_DIRECTORY);
        KREQUIRE_TRUE(DIR >= 0);
        target.retire();
        KEXPECT_EQ(subject.open("/proc/2/status", 0), -ENOENT);
        int const STATUS = subject.track(vfs::vfs_openat(subject.task.get(), DIR, "status", 0, 0));
        KREQUIRE_TRUE(STATUS >= 0);
        NativeFile status(vfs::vfs_get_file_retain(subject.task.get(), STATUS));
        KREQUIRE_TRUE(status);
        KEXPECT_TRUE(proc_data(status).target == target.task.get());
        std::array<char, 8192> output{};
        KREQUIRE_TRUE(proc_text(status, output) > 0);
        KEXPECT_TRUE(status_row(output.data(), "Pid", 2));
        int const FOLDER = subject.track(vfs::vfs_openat(subject.task.get(), DIR, "fd", vfs::O_DIRECTORY, 0));
        KREQUIRE_TRUE(FOLDER >= 0);
        std::array<char, 32> number{};
        std::snprintf(number.data(), number.size(), "%d", target_fd.fd);
        int const CAP = subject.track(vfs::vfs_openat(subject.task.get(), FOLDER, number.data(), 0, 0));
        KREQUIRE_TRUE(CAP >= 0);
        NativeFile duplicate(vfs::vfs_get_file_retain(subject.task.get(), CAP));
        KREQUIRE_TRUE(duplicate);
        KEXPECT_TRUE(duplicate.get() == original.get());
        int const PARENT = subject.track(vfs::vfs_openat(subject.task.get(), FOLDER, "../status", 0, 0));
        KREQUIRE_TRUE(PARENT >= 0);
        NativeFile directory(vfs::vfs_get_file_retain(subject.task.get(), DIR));
        KREQUIRE_TRUE(directory);
        vfs::FdReservation import(foreign.task.get());
        int const FOREIGN = foreign.track(import.install(directory.get()));
        KREQUIRE_TRUE(FOREIGN >= 0);
        directory.release();
        auto const FDS = foreign.task->fd_table.size();
        KEXPECT_EQ(vfs::vfs_openat(foreign.task.get(), FOREIGN, "status", 0, 0), -EXDEV);
        KEXPECT_EQ(foreign.task->fd_table.size(), FDS);
        target.task->state.store(sched::task::TaskState::EXITING);
        vfs::Stat unchanged{};
        unchanged.st_ino = 73;
        KEXPECT_EQ(vfs::vfs_statat(subject.task.get(), DIR, "status", 0, &unchanged), -ESRCH);
        KEXPECT_EQ(unchanged.st_ino, uint64_t{73});
        target.task->state.store(sched::task::TaskState::ACTIVE);
        pid_view.close();
        KEXPECT_EQ(vfs::vfs_openat(subject.task.get(), DIR, "status", 0, 0), -EIDRM);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentProcfs, RetainedThreadDirectoryParentsAndCanonicalNamesStayExact) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        RegistryTask group, thread, sibling;
        KREQUIRE_TRUE(group.task->initialize_pid_namespace(scope));
        thread.task->is_thread = true;
        thread.task->owner_pid = group.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *group.task));
        KREQUIRE_TRUE(sibling.task->initialize_pid_namespace(scope));
        KREQUIRE_TRUE(thread.publish());
        KREQUIRE_TRUE(sibling.publish());
        auto tasks = scoped_proc(*group.task, "1/task");
        KREQUIRE_TRUE(tasks);
        int error = 0;
        KEXPECT_FALSE(proc_component(*group.task, tasks, "3", &error));
        KEXPECT_EQ(error, -ENOENT);
        auto tid = proc_component(*group.task, tasks, "2", &error);
        KREQUIRE_TRUE(tid);
        KEXPECT_TRUE(proc_data(tid).target == thread.task.get());
        thread.retire();
        auto status = proc_component(*group.task, tid, "status");
        auto fds = proc_component(*group.task, tid, "fd");
        KREQUIRE_TRUE(status && fds);
        KEXPECT_TRUE(proc_data(status).target == thread.task.get() && proc_data(status).node.thread_view);
        std::array<char, 64> path{};
        KREQUIRE_EQ(procfs::procfs_directory_path(tid.get(), path.data(), path.size()), 0);
        KEXPECT_EQ(std::strcmp(path.data(), "/1/task/2"), 0);
        KREQUIRE_EQ(procfs::procfs_directory_path(fds.get(), path.data(), path.size()), 0);
        KEXPECT_EQ(std::strcmp(path.data(), "/1/task/2/fd"), 0);
        auto parent = proc_component(*group.task, fds, "..");
        KREQUIRE_TRUE(parent);
        KEXPECT_TRUE(proc_data(parent).target == thread.task.get());
        KEXPECT_EQ(proc_data(parent).node.type, procfs::ProcNodeType::TASK_TID_DIR);
        auto owner = proc_component(*group.task, parent, "..");
        KREQUIRE_TRUE(owner);
        KEXPECT_TRUE(proc_data(owner).target == group.task.get());
        KEXPECT_EQ(proc_data(owner).node.type, procfs::ProcNodeType::TASK_DIR);
        path.fill('x');
        KEXPECT_EQ(procfs::procfs_directory_path(tid.get(), path.data(), 2), -ERANGE);
        KEXPECT_EQ(path[0], 'x');
        KEXPECT_FALSE(proc_component(*group.task, tid, "status/extra", &error));
        KEXPECT_EQ(error, -EINVAL);
        std::array<char, 257> too_long{};
        too_long.fill('x');
        too_long.back() = '\0';
        KEXPECT_FALSE(proc_component(*group.task, tid, too_long.data(), &error));
        KEXPECT_EQ(error, -ENAMETOOLONG);
        scope.close();
        KEXPECT_EQ(procfs::procfs_directory_path(tid.get(), path.data(), path.size()), -EIDRM);
        KEXPECT_EQ(path[0], 'x');
        KEXPECT_FALSE(proc_component(*group.task, tid, "status", &error));
        KEXPECT_EQ(error, -EIDRM);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
}

KTEST(CompartmentProcfs, UnscopedDirectoryCoverMutationInvalidatesConsumption) {
    PrivateMounts views;
    pid_ns::NamespaceRef pid_view;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(pid_ns::create_namespace(pid_view), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    NativeTask subject;
    subject.task->type = sched::task::TaskType::PROCESS;
    KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
    KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pid_view));
    KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/proc", 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mount_for_task(subject.task.get(), nullptr, "/proc", "procfs"), 0);
    int const DIR = subject.open("/proc/1", vfs::O_DIRECTORY);
    KREQUIRE_TRUE(DIR >= 0);
    vfs::LookupHandle selected;
    KREQUIRE_EQ(task_lookup(*subject.task, "status", selected, DIR), 0);
    KEXPECT_FALSE(selected.directory_view());
    KEXPECT_TRUE(selected.mount_membership() != nullptr);
    unsigned calls = 0;
    KEXPECT_EQ(vfs::vfs_consume_lookup(selected, false, observe_procfs_lookup, &calls), 0);
    KEXPECT_EQ(calls, 1U);
    auto mount = scoped_mount(views.first, "/");
    KREQUIRE_TRUE(mount);
    auto const CREDENTIALS = subject.task->credentials();
    tmpfs::TmpfsLookupHandle source, destination;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(mount.get()), "/proc", &source), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(mount.get()), "/moved", &destination), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_rename_lookup(CREDENTIALS, source, destination, true, false), 0);
    calls = 0;
    KEXPECT_EQ(vfs::vfs_consume_lookup(selected, false, observe_procfs_lookup, &calls), -EAGAIN);
    KEXPECT_EQ(calls, 0U);
    KEXPECT_EQ(vfs::vfs_openat(subject.task.get(), DIR, "status", 0, 0), -EAGAIN);
    KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
}

namespace {
struct SelfOwnershipFixture {
    PrivateMounts mounts;
    pid_ns::NamespaceRef pids;
    NativeTask parent;
    auto prepare() -> int {
        parent.task->type = sched::task::TaskType::PROCESS;
        int result = mount_ns::create_namespace(mounts.first);
        if (result == 0) result = pid_ns::create_namespace(pids);
        if (result == 0) result = private_mount(mounts.first, "/");
        if (result != 0) return result;
        if (!parent.task->initialize_mount_namespace(mounts.first) || !parent.task->initialize_pid_namespace(pids)) return -ESRCH;
        result = vfs::vfs_mkdirat(parent.task.get(), vfs::AT_FDCWD, "/proc", 0755);
        if (result == 0) result = vfs::vfs_mount_for_task(parent.task.get(), nullptr, "/proc", "procfs");
        auto mount = scoped_mount(mounts.first, "/");
        return result != 0 ? result : prepare_procfs_fixture_root(*parent.task, mount.get(), "/", "/");
    }
};
struct UnpublishedFixture {
    Task& owner;
    Task* task = new Task{};
    bool claimed{};
    explicit UnpublishedFixture(Task& source) : owner(source) {
        if (task == nullptr) return;
        task->pid = sched::task::get_next_pid();
        task->type = sched::task::TaskType::PROCESS;
        if (!resource::inherit(*task, owner) || resource::initialize_credentials(*task, owner.credentials()) != 0) return;
        Task::FilesystemContext filesystem;
        if (!owner.capture_filesystem_context(filesystem)) return;
        task->root = filesystem.root;
        task->cwd = filesystem.cwd;
        task->root_len = filesystem.root_len;
        task->cwd_len = filesystem.cwd_len;
        task->root_anchor = std::move(filesystem.root_anchor);
        task->cwd_anchor = std::move(filesystem.cwd_anchor);
        claimed = sched::task::claim_unpublished_process(&owner, task);
    }
    ~UnpublishedFixture() {
        if (task == nullptr) return;
        if (claimed && sched::task::destroy_owned_unpublished_process(&owner, task)) return;
        // Fixture-only recovery. All outside aliases are scoped after this
        // owner and have already drained; never delete a shared candidate.
        task->root_anchor = {};
        task->cwd_anchor = {};
        sched::task::release_lazy_vmem_ranges(*task);
        for (unsigned fd = 0; fd < Task::FD_TABLE_SIZE; ++fd)
            if (task->fd_table.lookup(fd) != nullptr) KEXPECT_EQ(vfs::vfs_close_for_task(task, fd), 0);
        if (task->exec_image_file != nullptr) {
            vfs::vfs_put_file(task->exec_image_file);
            task->exec_image_file = nullptr;
        }
        if (claimed) {
            KEXPECT_TRUE(sched::task::destroy_owned_unpublished_process(&owner, task));
        } else {
            KEXPECT_EQ(task->ref_count.load(), uint32_t{1});
            if (task->ref_count.load() == 1) delete task;
        }
    }
    auto open() -> int { return vfs::vfs_openat(task, vfs::AT_FDCWD, "/proc/self/status", 0, 0); }
    auto destroy() -> bool {
        if (!sched::task::destroy_owned_unpublished_process(&owner, task)) return false;
        task = nullptr;
        claimed = false;
        return true;
    }
};
struct LazyOwnershipCutover {
    static inline LazyOwnershipCutover* active{};
    Task& task;
    NativeFile transferred;
    explicit LazyOwnershipCutover(Task& source) : task(source) {
        active = this;
        vfs::unpublished_self_refs_test_hook = inject;
    }
    ~LazyOwnershipCutover() {
        vfs::unpublished_self_refs_test_hook = nullptr;
        active = nullptr;
    }
    static void inject(Task& target) {
        if (active == nullptr || &active->task != &target) return;
        uint64_t const IRQF = target.lazy_vmem_lock.lock_irqsave();
        auto* file = target.lazy_vmem_ranges.empty() ? nullptr : target.lazy_vmem_ranges[0].file;
        target.lazy_vmem_ranges.clear();
        target.lazy_vmem_lock.unlock_irqrestore(IRQF);
        active->transferred.reset(file);
    }
};
}  // namespace

KTEST(CompartmentOwnership, DistinctSelfFilesAndDuplicateOwnersDrainWhenClosed) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        SelfOwnershipFixture fixture;
        KREQUIRE_EQ(fixture.prepare(), 0);
        UnpublishedFixture child(*fixture.parent.task);
        KREQUIRE_TRUE(child.claimed);
        int const FIRST = child.open();
        KREQUIRE_TRUE(FIRST >= 0);
        KREQUIRE_EQ(vfs::vfs_dup2_for_task(child.task, FIRST, 17), 17);
        KREQUIRE_TRUE(child.open() >= 0);
        auto* file = vfs::vfs_get_file(child.task, FIRST);
        KREQUIRE_TRUE(file != nullptr);
        vfs::vfs_retain_file(file);
        child.task->exec_image_file = file;
        vfs::vfs_retain_file(file);
        child.task->lazy_vmem_ranges.push_back({.kind = sched::task::LazyVmemKind::FILE_BACKED, .file = file});
        KEXPECT_EQ(child.task->ref_count.load(), uint32_t{4});
        uint32_t count = 91;
        KEXPECT_EQ(vfs::vfs_count_exclusive_unpublished_self_refs(*child.task, count), 0);
        KEXPECT_EQ(count, uint32_t{2});
        KEXPECT_EQ(child.task->ref_count.load(), uint32_t{4});
        fixture.pids.close();
        KEXPECT_TRUE(child.destroy());
        KEXPECT_TRUE(fixture.parent.task->owned_unpublished_process.load() == nullptr);
        KEXPECT_EQ(fixture.parent.task->ref_count.load(), uint32_t{1});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentOwnership, ExternalTaskFileAndReservationAliasesRefuseWithoutEffects) {
    SelfOwnershipFixture fixture;
    KREQUIRE_EQ(fixture.prepare(), 0);
    UnpublishedFixture child(*fixture.parent.task);
    KREQUIRE_TRUE(child.claimed);
    int const FD = child.open();
    KREQUIRE_TRUE(FD >= 0);
    NativeFile alias(vfs::vfs_get_file_retain(child.task, FD));
    KREQUIRE_TRUE(alias);
    uint32_t count = 91;
    KEXPECT_EQ(vfs::vfs_count_exclusive_unpublished_self_refs(*child.task, count), -EBUSY);
    KEXPECT_EQ(count, uint32_t{91});
    KEXPECT_FALSE(child.destroy());
    KEXPECT_TRUE(fixture.parent.task->owned_unpublished_process.load() == child.task);
    KEXPECT_TRUE(vfs::vfs_get_file(child.task, FD) == alias.get());
    KEXPECT_EQ(child.task->fd_table.size(), 1U);
    KEXPECT_EQ(child.task->ref_count.load(), uint32_t{3});
    alias.reset();
    KREQUIRE_TRUE(child.task->try_acquire());
    KEXPECT_FALSE(child.destroy());
    KEXPECT_EQ(child.task->fd_table.size(), 1U);
    child.task->release();
    {
        vfs::FdReservation reservation(child.task);
        KREQUIRE_TRUE(reservation.result() >= 0);
        KEXPECT_FALSE(child.destroy());
        KEXPECT_EQ(child.task->fd_table.size(), 1U);
        KEXPECT_EQ(child.task->fd_table.reserved_count(), 1U);
        KEXPECT_FALSE(fixture.parent.task->unpublished_teardown_in_progress.load());
    }
    KEXPECT_TRUE(child.destroy());
}

KTEST(CompartmentOwnership, TransferredRefusalConsumesOnlyOwnerSlotThenDrains) {
    SelfOwnershipFixture fixture;
    KREQUIRE_EQ(fixture.prepare(), 0);
    UnpublishedFixture child(*fixture.parent.task);
    KREQUIRE_TRUE(child.claimed);
    int const FD = child.open();
    KREQUIRE_TRUE(FD >= 0);
    NativeFile alias(vfs::vfs_get_file_retain(child.task, FD));
    KREQUIRE_TRUE(alias);
    auto* transferred = sched::task::take_unpublished_process(fixture.parent.task.get());
    KREQUIRE_TRUE(transferred == child.task);
    child.claimed = false;
    KEXPECT_FALSE(sched::task::destroy_unpublished_process(transferred));
    KEXPECT_EQ(child.task->ref_count.load(), uint32_t{2});
    KEXPECT_EQ(child.task->fd_table.size(), 1U);
    KEXPECT_TRUE(fixture.parent.task->owned_unpublished_process.load() == nullptr);
    alias.reset();
    child.claimed = sched::task::claim_unpublished_process(fixture.parent.task.get(), child.task);
    KREQUIRE_TRUE(child.claimed);
    fixture.pids.close();
    transferred = sched::task::take_unpublished_process(fixture.parent.task.get());
    child.claimed = false;
    bool const DESTROYED = sched::task::destroy_unpublished_process(transferred);
    KEXPECT_TRUE(DESTROYED);
    if (DESTROYED) child.task = nullptr;
}

KTEST(CompartmentOwnership, LazyOwnerTransferCannotMasqueradeAsInternalReference) {
    SelfOwnershipFixture fixture;
    KREQUIRE_EQ(fixture.prepare(), 0);
    UnpublishedFixture child(*fixture.parent.task);
    KREQUIRE_TRUE(child.claimed);
    int const FD = child.open();
    KREQUIRE_TRUE(FD >= 0);
    NativeFile file(vfs::vfs_get_file_retain(child.task, FD));
    KREQUIRE_TRUE(file);
    KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, FD), 0);
    child.task->lazy_vmem_ranges.push_back({.kind = sched::task::LazyVmemKind::FILE_BACKED, .file = file.get()});
    file.release();
    {
        LazyOwnershipCutover cutover(*child.task);
        uint32_t count = 91;
        KEXPECT_EQ(vfs::vfs_count_exclusive_unpublished_self_refs(*child.task, count), -EBUSY);
        KEXPECT_EQ(count, uint32_t{91});
        KEXPECT_TRUE(cutover.transferred);
        KEXPECT_TRUE(child.task->lazy_vmem_ranges.empty());
        KEXPECT_EQ(child.task->ref_count.load(), uint32_t{3});
        KEXPECT_TRUE(fixture.parent.task->owned_unpublished_process.load() == child.task);
    }
    KEXPECT_EQ(child.task->ref_count.load(), uint32_t{2});
    KEXPECT_TRUE(child.destroy());
}

KTEST(CompartmentOwnership, NativeSpawnActionFailureDrainsSelfDescriptorsAndPopulation) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        SelfOwnershipFixture fixture;
        KREQUIRE_EQ(fixture.prepare(), 0);
        KEXPECT_TRUE(ker::syscall::process::exec_selftest_spawn_procfs_failure_drains_child(*fixture.parent.task));
        KEXPECT_EQ(fixture.parent.task->ref_count.load(), uint32_t{1});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentProcfs, OwnedDirectoryCwdUsesRetainedNamesAndNativeParentBoundary) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        SelfOwnershipFixture fixture;
        KREQUIRE_EQ(fixture.prepare(), 0);
        KREQUIRE_TRUE(fixture.parent.open("/seed") >= 0);
        UnpublishedFixture child(*fixture.parent.task);
        KREQUIRE_TRUE(child.claimed);
        int const DIR = vfs::vfs_openat(child.task, vfs::AT_FDCWD, "/proc/self", vfs::O_DIRECTORY, 0);
        KREQUIRE_TRUE(DIR >= 0);
        auto* file = vfs::vfs_get_file(child.task, DIR);
        KREQUIRE_TRUE(file != nullptr && file->directory_view);
        KEXPECT_EQ(file->directory_view.root()->fs_type, vfs::FSType::TMPFS);
        KEXPECT_EQ(file->directory_view.directory()->fs_type, vfs::FSType::PROCFS);
        KEXPECT_EQ(file->directory_view.directory()->open_flags & 3, vfs::FILE_ACCESS_SEARCH_ONLY);
        vfs::DirectoryViewRef synthetic_root;
        KEXPECT_EQ(vfs::vfs_prepare_directory_view(child.task->credentials(), file->directory_view.directory(),
                                                   file->directory_view.directory(), {}, synthetic_root),
                   -ENOTSUP);
        KEXPECT_FALSE(synthetic_root);
        auto const REFS = child.task->ref_count.load();
        auto const FDS = child.task->fd_table.size();
        KEXPECT_EQ(vfs::vfs_openat(child.task, vfs::AT_FDCWD, "/proc/self", vfs::FILE_ACCESS_SEARCH_ONLY | vfs::O_DIRECTORY, 0), -EINVAL);
        KEXPECT_EQ(child.task->ref_count.load(), REFS);
        KEXPECT_EQ(child.task->fd_table.size(), FDS);
        KREQUIRE_EQ(vfs::vfs_fchdir(child.task, DIR), 0);
        std::array<char, 96> expected{}, path{};
        std::snprintf(expected.data(), expected.size(), "/proc/%llu",
                      static_cast<unsigned long long>(sched::task::namespace_process_pid(*child.task)));
        auto const CREDENTIALS = child.task->credentials();
        KREQUIRE_EQ(vfs::vfs_getcwd_for_task(child.task, path.data(), path.size(), nullptr, &CREDENTIALS), 0);
        KEXPECT_EQ(std::strcmp(path.data(), expected.data()), 0);
        for (int base : {vfs::AT_FDCWD, DIR}) {
            int const STATUS = vfs::vfs_openat(child.task, base, "status", 0, 0);
            KREQUIRE_TRUE(STATUS >= 0);
            auto* status = vfs::vfs_get_file(child.task, STATUS);
            KREQUIRE_TRUE(status != nullptr);
            KEXPECT_TRUE(static_cast<procfs::ProcFileData*>(status->private_data)->target == child.task);
            KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, STATUS), 0);
        }
        std::array<char, 96> fd_link{};
        std::snprintf(fd_link.data(), fd_link.size(), "/proc/self/fd/%d/status", DIR);
        for (auto* name : std::array<const char*, 2>{"/proc/self/cwd/status", fd_link.data()}) {
            int const STATUS = vfs::vfs_openat(child.task, vfs::AT_FDCWD, name, 0, 0);
            KREQUIRE_TRUE(STATUS >= 0);
            auto* status = vfs::vfs_get_file(child.task, STATUS);
            KREQUIRE_TRUE(status != nullptr);
            KEXPECT_TRUE(static_cast<procfs::ProcFileData*>(status->private_data)->target == child.task);
            KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, STATUS), 0);
        }
        int const PROC_ROOT = vfs::vfs_openat(child.task, DIR, "..", vfs::O_DIRECTORY, 0);
        KREQUIRE_TRUE(PROC_ROOT >= 0);
        KREQUIRE_EQ(vfs::vfs_fchdir(child.task, PROC_ROOT), 0);
        int const SEED = vfs::vfs_openat(child.task, vfs::AT_FDCWD, "../../seed", 0, 0);
        KREQUIRE_TRUE(SEED >= 0);
        KEXPECT_EQ(vfs::vfs_get_file(child.task, SEED)->fs_type, vfs::FSType::TMPFS);
        KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, SEED), 0);
        KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, PROC_ROOT), 0);
        KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, DIR), 0);
        KEXPECT_TRUE(child.destroy());
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentOwnership, SyntheticSelfCwdRejectsExternalAliasesThenDrainsClosedView) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        SelfOwnershipFixture fixture;
        KREQUIRE_EQ(fixture.prepare(), 0);
        UnpublishedFixture child(*fixture.parent.task);
        KREQUIRE_TRUE(child.claimed);
        int const DIR = vfs::vfs_openat(child.task, vfs::AT_FDCWD, "/proc/self", vfs::O_DIRECTORY, 0);
        KREQUIRE_TRUE(DIR >= 0);
        KREQUIRE_EQ(vfs::vfs_fchdir(child.task, DIR), 0);
        KREQUIRE_EQ(vfs::vfs_close_for_task(child.task, DIR), 0);
        KEXPECT_EQ(child.task->ref_count.load(), uint32_t{3});
        KEXPECT_TRUE(vfs::vfs_selftest_unpublished_procfs_cwd_aliases(*child.task));
        fixture.pids.close();
        std::array<char, 96> unchanged{};
        unchanged.fill('x');
        size_t length = 91;
        auto const CREDENTIALS = child.task->credentials();
        KEXPECT_EQ(vfs::vfs_getcwd_for_task(child.task, unchanged.data(), unchanged.size(), &length, &CREDENTIALS), -EIDRM);
        KEXPECT_EQ(unchanged[0], 'x');
        KEXPECT_EQ(length, size_t{91});
        KEXPECT_EQ(vfs::vfs_openat(child.task, vfs::AT_FDCWD, "status", 0, 0), -EIDRM);
        KEXPECT_EQ(child.task->fd_table.reserved_count(), 0U);
        KEXPECT_TRUE(child.destroy());
        KEXPECT_TRUE(fixture.parent.task->owned_unpublished_process.load() == nullptr);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentProcfs, OwnedSyntheticCwdAndDirfdRefuseRenamedReplacedCover) {
    SelfOwnershipFixture fixture;
    KREQUIRE_EQ(fixture.prepare(), 0);
    UnpublishedFixture child(*fixture.parent.task);
    KREQUIRE_TRUE(child.claimed);
    int const DIR = vfs::vfs_openat(child.task, vfs::AT_FDCWD, "/proc/self", vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(DIR >= 0);
    KREQUIRE_EQ(vfs::vfs_fchdir(child.task, DIR), 0);
    auto const CREDENTIALS = child.task->credentials();
    auto root = scoped_mount(fixture.mounts.first, "/");
    KREQUIRE_TRUE(root);
    tmpfs::TmpfsLookupHandle old, moved;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(root.get()), "/proc", &old), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(CREDENTIALS, native_root(root.get()), "/moved-proc", &moved), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_rename_lookup(CREDENTIALS, old, moved, true, false), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(CREDENTIALS, native_root(root.get()), "/proc", 0755), 0);
    auto* const ANCHOR = child.task->cwd_anchor.get();
    auto const TEXT = child.task->cwd;
    std::array<char, 96> unchanged{};
    unchanged.fill('x');
    size_t length = 91;
    KEXPECT_EQ(vfs::vfs_getcwd_for_task(child.task, unchanged.data(), unchanged.size(), &length, &CREDENTIALS), -EAGAIN);
    KEXPECT_EQ(unchanged[0], 'x');
    KEXPECT_EQ(length, size_t{91});
    KEXPECT_EQ(vfs::vfs_fchdir(child.task, DIR), -EAGAIN);
    KEXPECT_EQ(vfs::vfs_openat(child.task, DIR, "status", 0, 0), -EAGAIN);
    KEXPECT_TRUE(child.task->cwd_anchor.get() == ANCHOR && child.task->cwd == TEXT);
    KEXPECT_EQ(child.task->fd_table.size(), 1U);
    KEXPECT_EQ(child.task->fd_table.reserved_count(), 0U);
    KEXPECT_TRUE(child.destroy());
}

KTEST(CompartmentProcfs, SameMountForeignPidDirectoryCannotPublishCwdOrNames) {
    SelfOwnershipFixture fixture;
    KREQUIRE_EQ(fixture.prepare(), 0);
    UnpublishedFixture child(*fixture.parent.task);
    KREQUIRE_TRUE(child.claimed);
    int const DIR = vfs::vfs_openat(child.task, vfs::AT_FDCWD, "/proc/self", vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(DIR >= 0);
    pid_ns::NamespaceRef other;
    KREQUIRE_EQ(pid_ns::create_namespace(other), 0);
    NativeTask foreign;
    foreign.task->type = sched::task::TaskType::PROCESS;
    KREQUIRE_TRUE(foreign.task->initialize_mount_namespace(fixture.mounts.first));
    KREQUIRE_TRUE(foreign.task->initialize_pid_namespace(other));
    auto root = scoped_mount(fixture.mounts.first, "/");
    KREQUIRE_TRUE(root);
    KREQUIRE_EQ(prepare_procfs_fixture_root(*foreign.task, root.get(), "/", "/"), 0);
    NativeFile capability(vfs::vfs_get_file_retain(child.task, DIR));
    KREQUIRE_TRUE(capability);
    vfs::FdReservation reservation(foreign.task.get());
    int const INHERITED = reservation.install(capability.get());
    KREQUIRE_TRUE(INHERITED >= 0);
    capability.release();
    foreign.track(INHERITED);
    auto* const ANCHOR = foreign.task->cwd_anchor.get();
    auto const TEXT = foreign.task->cwd;
    KEXPECT_EQ(vfs::vfs_fchdir(foreign.task.get(), INHERITED), -EXDEV);
    KEXPECT_EQ(vfs::vfs_openat(foreign.task.get(), INHERITED, "status", 0, 0), -EXDEV);
    vfs::DirectoryViewRef output;
    KEXPECT_EQ(vfs::vfs_directory_view_from_file_for_task(*foreign.task, foreign.task->credentials(),
                                                          vfs::vfs_get_file(foreign.task.get(), INHERITED), output),
               -EXDEV);
    KEXPECT_FALSE(output);
    KEXPECT_TRUE(foreign.task->cwd_anchor.get() == ANCHOR && foreign.task->cwd == TEXT);
    KEXPECT_EQ(foreign.task->fd_table.size(), 1U);
    KEXPECT_EQ(foreign.task->fd_table.reserved_count(), 0U);
    KREQUIRE_EQ(foreign.close(INHERITED), 0);
    KEXPECT_TRUE(child.destroy());
}

namespace {
auto native_net(Task& subject, abi::net::ops op, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0)
    -> int {
    auto const RESULT = static_cast<int64_t>(net_syscall::net_selftest_call_as(subject, static_cast<uint64_t>(op), a1, a2, a3, a4, a5));
    // These cases return bounded FD/count/errno values. KTEST compares the
    // complete input representation, so check the range before matching its
    // int-width errno oracle; never silently truncate the syscall result.
    if (RESULT < std::numeric_limits<int>::min() || RESULT > std::numeric_limits<int>::max()) {
        KEXPECT_TRUE(false);
        return -EOVERFLOW;
    }
    return static_cast<int>(RESULT);
}
constexpr std::array AMBIENT_NET_CONTROLS{
    abi::net::ops::IOCTL_NET,          abi::net::ops::SET_DEV_CPU_AFFINITY, abi::net::ops::NETCTL_IF_LIST,
    abi::net::ops::NETCTL_ADDR_LIST,   abi::net::ops::NETCTL_ADDR_SET,      abi::net::ops::NETCTL_ADDR_DEL,
    abi::net::ops::NETCTL_ADDR_SET_V2, abi::net::ops::NETCTL_ROUTE_LIST,    abi::net::ops::NETCTL_ROUTE_SET,
    abi::net::ops::NETCTL_ROUTE_DEL,   abi::net::ops::NETCTL_LINK_SET};
}  // namespace

KTEST(CompartmentNetwork, DeniedNativeSocketsAndGlobalControlsRefuseBeforeEffects) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        NativeTask subject;
        subject.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(scope));
        KEXPECT_EQ(scope.network(), Network::DENIED);
        KEXPECT_EQ(ker::mod::security::authorize_network_administration(subject.task->credentials()), 0);
        constexpr auto NOFILE = static_cast<unsigned>(abi::resource::Id::NOFILE);
        auto& usage = subject.task->resources.entries[NOFILE];
        usage.limit = {.soft = 0, .hard = 0};
        auto const BEFORE = usage;
        auto const REFERENCES = subject.task->ref_count.load();
        for (int domain : {2, 10}) {
            for (int type : {1, 2, 3}) {
                KEXPECT_EQ(native_net(*subject.task, abi::net::ops::SOCKET, domain, type | ker::net::SOCK_NONBLOCK,
                                      type == 3 ? (domain == 10 ? 58 : 1) : 0),
                           -EPERM);
            }
        }
        for (auto op : AMBIENT_NET_CONTROLS) {
            KEXPECT_EQ(native_net(*subject.task, op, UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX), -EPERM);
        }
        KEXPECT_EQ(subject.task->fd_table.size(), 0U);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
        KEXPECT_EQ(usage.current, BEFORE.current);
        KEXPECT_EQ(usage.peak, BEFORE.peak);
        KEXPECT_EQ(usage.denied, BEFORE.denied);
        KEXPECT_EQ(usage.reason, BEFORE.reason);
        KEXPECT_NULL(subject.task->pending_fd_reservations);
        KEXPECT_EQ(subject.task->ref_count.load(), REFERENCES);
        {
            RegistryTask child, thread, unbound;
            KREQUIRE_TRUE(resource::inherit(*child.task, *subject.task));
            thread.task->is_thread = true;
            thread.task->owner_pid = subject.task->pid;
            KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *subject.task));
            for (auto* inherited : {child.task.get(), thread.task.get()}) {
                KEXPECT_TRUE(*inherited->pid_namespace() == scope);
                KEXPECT_EQ(native_net(*inherited, abi::net::ops::SOCKET, 2, 2, 0), -EPERM);
                KEXPECT_EQ(native_net(*inherited, abi::net::ops::NETCTL_IF_LIST, UINT64_MAX, UINT64_MAX), -EPERM);
            }
            unbound.task->is_thread = true;
            KEXPECT_NULL(unbound.task->pid_namespace());
            KEXPECT_EQ(native_net(*unbound.task, abi::net::ops::SOCKET, 2, 2, 0), -ESRCH);
            KEXPECT_EQ(native_net(*unbound.task, abi::net::ops::NETCTL_IF_LIST, UINT64_MAX, UINT64_MAX), -ESRCH);
        }
        scope.close();
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::SOCKET, 10, 2, 0), -EIDRM);
        for (auto op : AMBIENT_NET_CONTROLS) KEXPECT_EQ(native_net(*subject.task, op, UINT64_MAX, UINT64_MAX), -EIDRM);
        KEXPECT_EQ(usage.denied, BEFORE.denied);
        KEXPECT_EQ(subject.task->ref_count.load(), REFERENCES);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentNetwork, ExplicitSharedGrantCreatesRealSocketsAndKeepsRawCredentials) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef host, scope, denied, output;
        KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, scope), 0);
        KREQUIRE_EQ(pid_ns::create_namespace(scope, Network::DENIED, denied), 0);
        KREQUIRE_EQ(scope.retain(output), 0);
        auto const ORIGINAL = output.generation();
        KEXPECT_EQ(pid_ns::create_namespace(denied, Network::SHARED_HOST, output), -EPERM);
        KEXPECT_EQ(output.generation(), ORIGINAL);
        NativeTask shared, ordinary;
        shared.task->type = sched::task::TaskType::PROCESS;
        ordinary.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(shared.task->initialize_pid_namespace(scope));
        for (int domain : {2, 10}) {
            for (int type : {1, 2, 3}) {
                int const FD = shared.track(static_cast<int>(native_net(
                    *shared.task, abi::net::ops::SOCKET, domain, type | ker::net::SOCK_NONBLOCK, type == 3 ? (domain == 10 ? 58 : 1) : 0)));
                KREQUIRE_TRUE(FD >= 0);
                NativeFile file(vfs::vfs_get_file_retain(shared.task.get(), FD));
                KREQUIRE_NE(file.get(), nullptr);
                KEXPECT_EQ(file->fs_type, vfs::FSType::SOCKET);
                auto* socket = static_cast<ker::net::Socket*>(file->private_data);
                KREQUIRE_NE(socket, nullptr);
                KEXPECT_EQ(socket->domain, domain);
                KEXPECT_EQ(socket->type, type);
                KEXPECT_EQ(socket->owner_pid, shared.task->pid);
                KEXPECT_TRUE(socket->nonblock);
                KEXPECT_TRUE(socket->rcvbuf.capacity > 0);
            }
        }
        int const HOST_FD = ordinary.track(static_cast<int>(native_net(*ordinary.task, abi::net::ops::SOCKET, 2, 2, 0)));
        KREQUIRE_TRUE(HOST_FD >= 0);
        KEXPECT_EQ(native_net(*ordinary.task, abi::net::ops::SOCKET, 1, 2, 0), -EAFNOSUPPORT);
        KEXPECT_EQ(native_net(*ordinary.task, abi::net::ops::SOCKET, 2, 1, 17), -EPROTONOSUPPORT);
        KREQUIRE_EQ(resource::initialize_credentials(*shared.task, ker::mod::security::Snapshot{}), 0);
        KEXPECT_EQ(ker::mod::security::authorize_network_administration(shared.task->credentials()), -EPERM);
        auto const FDS = shared.task->fd_table.size();
        KEXPECT_EQ(native_net(*shared.task, abi::net::ops::SOCKET, 2, 3, 1), -EPERM);
        KEXPECT_EQ(shared.task->fd_table.size(), FDS);
        KREQUIRE_TRUE(shared.track(static_cast<int>(native_net(*shared.task, abi::net::ops::SOCKET, 10, 2, 0))) >= 0);
        scope.close();
        KEXPECT_EQ(scope.network(), Network::SHARED_HOST);
        KEXPECT_EQ(native_net(*shared.task, abi::net::ops::SOCKET, 2, 2, 0), -EIDRM);
        KEXPECT_EQ(pid_ns::create_namespace(scope, Network::DENIED, output), -EIDRM);
        KEXPECT_EQ(output.generation(), ORIGINAL);
        KEXPECT_EQ(shared.task->fd_table.reserved_count(), 0U);
        KEXPECT_NULL(shared.task->pending_fd_reservations);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentNetwork, InheritedSocketAndMappedOutputsSurviveScopeClosure) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef scope;
        KREQUIRE_EQ(pid_ns::create_namespace(scope), 0);
        NativeTask host;
        host.task->type = sched::task::TaskType::PROCESS;
        int const HOST_FD = host.track(static_cast<int>(native_net(*host.task, abi::net::ops::SOCKET, 10, 2 | ker::net::SOCK_NONBLOCK, 0)));
        KREQUIRE_TRUE(HOST_FD >= 0);
        UserTask subject;
        KREQUIRE_NE(subject.task->pagemap, nullptr);
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(scope));
        constexpr uint64_t OUTPUT = 0x640000;
        constexpr uint64_t LENGTH = OUTPUT + 64;
        KREQUIRE_TRUE(subject.map_zero(OUTPUT));
        NativeFile file(vfs::vfs_get_file_retain(host.task.get(), HOST_FD));
        KREQUIRE_NE(file.get(), nullptr);
        ProcFixtureFd inherited{*subject.task};
        {
            vfs::FdReservation reservation(subject.task.get());
            KREQUIRE_TRUE(reservation.result() >= 0);
            inherited.fd = reservation.install(file.get());
            KREQUIRE_TRUE(inherited.fd >= 0);
            file.release();
        }
        auto const SENTINEL = uint64_t{0x123456789abcdef0};
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, OUTPUT, SENTINEL));
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, LENGTH, SENTINEL));
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::NETCTL_IF_LIST, OUTPUT, LENGTH), -EPERM);
        uint64_t unchanged = 0;
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, OUTPUT, unchanged));
        KEXPECT_EQ(unchanged, SENTINEL);
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, LENGTH, unchanged));
        KEXPECT_EQ(unchanged, SENTINEL);
        scope.close();
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::NETCTL_IF_LIST, OUTPUT, LENGTH), -EIDRM);
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::SOCKET, 10, 2, 0), -EIDRM);
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::RECV, inherited.fd, 0, 0, ker::net::SOCKET_MSG_DONTWAIT), 0);
        size_t capacity = sizeof(int);
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, LENGTH, capacity));
        KEXPECT_EQ(native_net(*subject.task, abi::net::ops::GETSOCKOPT, inherited.fd, 1, 8, OUTPUT, LENGTH), 0);
        int buffer_size = 0;
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, OUTPUT, buffer_size));
        KEXPECT_EQ(buffer_size, static_cast<int>(ker::net::UDP_RCVBUF_SIZE));
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, LENGTH, capacity));
        KEXPECT_EQ(capacity, sizeof(buffer_size));
        KEXPECT_EQ(subject.task->fd_table.size(), 1U);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
        KEXPECT_EQ(subject.task->usercopy_pagemap_accesses.load(), uint32_t{0});
        KEXPECT_EQ(host.task->fd_table.size(), 1U);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

namespace {
namespace wki = ker::net::wki;
auto check_private_remote_entrypoints(Task& subject, int expected) -> void {
    constexpr std::array<uint8_t, 1> BINARY{0x7f};
    auto const RESOURCES = wki::resource_contract::snapshot(subject.resources);
    auto const REFERENCES = subject.ref_count.load();
    auto const FD_COUNT = subject.fd_table.size();
    wki::WkiIpcPerfSnapshot before{}, after{};
    wki::wki_ipc_get_perf_snapshot(before);
    int error = 99;
    KEXPECT_EQ(wki::wki_task_submit_inline(wki::WKI_NODE_INVALID, BINARY.data(), BINARY.size(), nullptr, nullptr, "/", &subject, nullptr, 0,
                                           nullptr, UINT8_MAX, &error),
               uint32_t{0});
    KEXPECT_EQ(error, expected);
    error = 99;
    KEXPECT_EQ(wki::wki_task_submit_vfs_ref(wki::WKI_NODE_INVALID, "/bin/private-fixture", nullptr, nullptr, "/", &subject, nullptr, 0,
                                            nullptr, UINT8_MAX, &error),
               uint32_t{0});
    KEXPECT_EQ(error, expected);
    std::array<wki::WkiIpcFdEntry, 16> map{};
    std::memset(map.data(), 0x5a, sizeof(map));
    auto const MAP = map;
    std::array<wki::WkiIpcTaskFdHandoff, 16> handoff{};
    handoff[0].local_fd = 43;
    uint16_t count = 99;
    uint8_t mask = 99;
    KEXPECT_EQ(wki::wki_ipc_export_task_fds(&subject, wki::WKI_NODE_INVALID, {}, map.data(), &count, handoff.data(), &mask), expected);
    KEXPECT_EQ(count, uint16_t{0});
    KEXPECT_EQ(mask, uint8_t{0});
    KEXPECT_EQ(std::memcmp(map.data(), MAP.data(), sizeof(map)), 0);
    KEXPECT_EQ(handoff[0].local_fd, uint16_t{43});
    KEXPECT_NULL(handoff[0].file);
    KEXPECT_EQ(subject.fd_table.size(), FD_COUNT);
    KEXPECT_EQ(subject.fd_table.reserved_count(), 0U);
    KEXPECT_EQ(subject.ref_count.load(), REFERENCES);
    KEXPECT_NULL(subject.pending_wki_filesystems);
    KEXPECT_NULL(subject.pending_wki_fd_snapshots);
    auto const CURRENT = wki::resource_contract::snapshot(subject.resources);
    KEXPECT_EQ(std::memcmp(&RESOURCES, &CURRENT, sizeof(RESOURCES)), 0);
    wki::wki_ipc_get_perf_snapshot(after);
    KEXPECT_EQ(after.exports, before.exports);
    KEXPECT_EQ(after.proxies, before.proxies);
    KEXPECT_EQ(after.pump_tasks, before.pump_tasks);
    KEXPECT_EQ(after.pending_chunks, before.pending_chunks);
    KEXPECT_EQ(after.pending_bytes, before.pending_bytes);
    KEXPECT_EQ(after.approx_alloc_bytes, before.approx_alloc_bytes);
}
auto observe_private_placement_prepare(Task&, void* context) -> int {
    *static_cast<bool*>(context) = true;
    return 0;
}
}  // namespace

KTEST(CompartmentWki, PrivateViewsRejectActualComputeAndFdExportBeforeEffects) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    auto const MOUNT_BASELINE = vfs::mount_ns::live_namespace_count();
    for (unsigned variant = 1; variant < 8; ++variant) {
        pid_ns::NamespaceRef host, pids;
        ipc::NamespaceRef names;
        vfs::mount_ns::NamespaceRef mounts;
        NativeTask subject;
        subject.task->type = sched::task::TaskType::PROCESS;
        if ((variant & 1U) != 0) {
            // An explicit shared network grant is still a private process view.
            KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, pids), 0);
            KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
        }
        if ((variant & 2U) != 0) {
            KREQUIRE_EQ(ipc::create_namespace(names), 0);
            KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
        }
        if ((variant & 4U) != 0) {
            KREQUIRE_EQ(vfs::mount_ns::create_namespace(mounts), 0);
            KREQUIRE_TRUE(subject.task->initialize_mount_namespace(mounts));
        }
        KEXPECT_EQ(ker::mod::security::compartment::remote_namespace_admission(subject.task.get()), -ENOTSUP);
        check_private_remote_entrypoints(*subject.task, -ENOTSUP);
        bool prepared = false;
        int error = 99;
        uint32_t deferred = 99;
        wki::WkiRemoteSpawnSpec spec{
            .error_out = &error, .prepare = observe_private_placement_prepare, .context = &prepared, .deferred_proxy_id = &deferred};
        for (uint32_t flags : {uint32_t{0}, Task::WKI_TARGET_FLAG_LOCAL, Task::WKI_TARGET_FLAG_REMOTE, Task::WKI_TARGET_FLAG_BALANCED}) {
            subject.task->wki_target_flags = flags;
            auto const EXPECTED =
                flags == 0 || flags == Task::WKI_TARGET_FLAG_LOCAL ? wki::WkiRemoteSpawnResult::LOCAL : wki::WkiRemoteSpawnResult::FAILED;
            KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), EXPECTED);
            KEXPECT_EQ(error, EXPECTED == wki::WkiRemoteSpawnResult::LOCAL ? 0 : -ENOTSUP);
            KEXPECT_EQ(deferred, uint32_t{0});
            KEXPECT_FALSE(prepared);
            KEXPECT_FALSE(subject.task->wki_skip_legacy_placement);
        }
        subject.task->wki_target_flags = 0;
        std::strcpy(subject.task->wki_target_hostname.data(), "explicit.invalid");
        KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -ENOTSUP);
        KEXPECT_FALSE(prepared);
        KEXPECT_FALSE(subject.task->wki_skip_legacy_placement);
        subject.task->wki_target_hostname.front() = '\0';
        wki::WkiRemoteExecCandidate candidate{.path = "/bin/candidate", .target = "candidate.invalid"};
        spec.exec_candidate = &candidate;
        KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -ENOTSUP);
        candidate.target_flags = Task::WKI_TARGET_FLAG_LOCAL;
        KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), wki::WkiRemoteSpawnResult::LOCAL);
        KEXPECT_EQ(error, 0);
        KEXPECT_FALSE(prepared);
        KEXPECT_FALSE(subject.task->wki_skip_legacy_placement);
        candidate.target = nullptr;
        KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -EINVAL);
        spec.exec_candidate = nullptr;
        pids.close();
        names.close();
        mounts.close();
        check_private_remote_entrypoints(*subject.task, -EIDRM);
        KEXPECT_EQ(wki::wki_try_remote_spawn(subject.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -EIDRM);
        KEXPECT_FALSE(prepared);
        KEXPECT_FALSE(subject.task->wki_skip_legacy_placement);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
    KEXPECT_EQ(vfs::mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentWki, CanonicalClosedOwnersAndExistingHandoffDrainExactly) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef pids;
        KREQUIRE_EQ(pid_ns::create_namespace(pids), 0);
        NativeTask subject, thread, unbound, host;
        subject.task->type = sched::task::TaskType::PROCESS;
        KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
        thread.task->is_thread = true;
        thread.task->owner_pid = subject.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *subject.task));
        check_private_remote_entrypoints(*thread.task, -ENOTSUP);
        unbound.task->is_thread = true;
        check_private_remote_entrypoints(*unbound.task, -ESRCH);
        KEXPECT_EQ(ker::mod::security::compartment::remote_namespace_admission(host.task.get()), 0);
        KEXPECT_EQ(ker::mod::security::compartment::remote_namespace_admission(nullptr), -ESRCH);
        std::array<int, 2> descriptors{-1, -1};
        KREQUIRE_EQ(vfs::vfs_selftest_create_pipe(*subject.task, descriptors.data()), 0);
        for (int fd : descriptors) KREQUIRE_EQ(subject.track(fd), fd);
        auto* reader = static_cast<vfs::File*>(subject.task->fd_table.lookup(descriptors[0]));
        auto* writer = static_cast<vfs::File*>(subject.task->fd_table.lookup(descriptors[1]));
        auto const REFERENCES = reader->refcount.load();
        constexpr std::array<uint8_t, 2> PAYLOAD{0x31, 0x32};
        KREQUIRE_EQ(writer->fops->vfs_write(writer, PAYLOAD.data(), PAYLOAD.size(), 0), ssize_t{2});
        for (bool inline_binary : {false, true}) {
            std::array<wki::WkiIpcFdEntry, 1> map{};
            std::array<wki::WkiIpcTaskFdHandoff, 1> handoff{};
            handoff[0] = {.local_fd = static_cast<uint16_t>(descriptors[0]),
                          .file = vfs::vfs_get_file_retain(subject.task.get(), descriptors[0])};
            KREQUIRE_TRUE(handoff[0].file == reader);
            uint16_t count = 1;
            // Existing ownership is transferred into direct-submit cleanup.
            // Invalid target has no export, so cancellation cannot transmit.
            wki::WkiIpcSubmissionGuard preparation(subject.task.get(), map.data(), handoff.data(), count, wki::WKI_NODE_INVALID);
            int error = 99;
            constexpr std::array<uint8_t, 1> BINARY{0x7f};
            uint32_t const ID =
                inline_binary
                    ? wki::wki_task_submit_inline(wki::WKI_NODE_INVALID, BINARY.data(), BINARY.size(), nullptr, nullptr, "/",
                                                  subject.task.get(), map.data(), count, handoff.data(), UINT8_MAX, &error, &preparation)
                    : wki::wki_task_submit_vfs_ref(wki::WKI_NODE_INVALID, "/bin/fixture", nullptr, nullptr, "/", subject.task.get(),
                                                   map.data(), count, handoff.data(), UINT8_MAX, &error, &preparation);
            KEXPECT_EQ(ID, uint32_t{0});
            KEXPECT_EQ(error, -ENOTSUP);
            KEXPECT_NULL(handoff[0].file);
            KEXPECT_EQ(reader->refcount.load(), REFERENCES);
            KEXPECT_TRUE(subject.task->fd_table.lookup(descriptors[0]) == reader);
        }
        std::array<uint8_t, 2> received{};
        KEXPECT_EQ(reader->fops->vfs_read(reader, received.data(), received.size(), 0), ssize_t{2});
        KEXPECT_TRUE(received == PAYLOAD);
        pids.close();
        check_private_remote_entrypoints(*thread.task, -EIDRM);
        int error = 99;
        wki::WkiRemoteSpawnSpec spec{.error_out = &error};
        KEXPECT_EQ(wki::wki_try_remote_spawn(thread.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -EIDRM);
        KEXPECT_EQ(wki::wki_try_remote_spawn(unbound.task.get(), spec), wki::WkiRemoteSpawnResult::FAILED);
        KEXPECT_EQ(error, -ESRCH);
        KEXPECT_EQ(wki::wki_try_remote_spawn(host.task.get(), spec), wki::WkiRemoteSpawnResult::LOCAL);
        KEXPECT_EQ(error, 0);
        for (int fd : descriptors) KREQUIRE_EQ(subject.close(fd), 0);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
}

namespace {
auto set_native_target(Task* subject, uint64_t hostname, size_t length, uint32_t flags) -> int {
    return static_cast<int>(static_cast<int64_t>(ker::syscall::process::set_wki_target_for_subject(subject, hostname, length, flags)));
}
auto get_native_target(Task* subject, uint64_t hostname = 0, size_t capacity = 0, uint64_t flags = 0) -> int {
    return static_cast<int>(static_cast<int64_t>(ker::syscall::process::get_wki_target_for_subject(subject, hostname, capacity, flags)));
}
}  // namespace

KTEST(CompartmentWki, TargetConfigurationRefusesUnsupportedModesBeforeUsercopy) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    auto const MOUNT_BASELINE = vfs::mount_ns::live_namespace_count();
    for (unsigned variant = 1; variant < 8; ++variant) {
        pid_ns::NamespaceRef host, pids;
        ipc::NamespaceRef names;
        vfs::mount_ns::NamespaceRef mounts;
        NativeTask subject;
        if ((variant & 1U) != 0) {
            KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, pids), 0);
            KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
        }
        if ((variant & 2U) != 0) {
            KREQUIRE_EQ(ipc::create_namespace(names), 0);
            KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
        }
        if ((variant & 4U) != 0) {
            KREQUIRE_EQ(vfs::mount_ns::create_namespace(mounts), 0);
            KREQUIRE_TRUE(subject.task->initialize_mount_namespace(mounts));
        }
        std::strcpy(subject.task->wki_target_hostname.data(), "inherited.fixture");
        subject.task->wki_target_flags = Task::WKI_TARGET_FLAG_LOCAL;
        auto const HOSTNAME = subject.task->wki_target_hostname;
        auto const REFERENCES = subject.task->ref_count.load();
        auto const RESOURCES = wki::resource_contract::snapshot(subject.task->resources);
        for (uint32_t flags :
             {Task::WKI_TARGET_FLAG_REMOTE, Task::WKI_TARGET_FLAG_BALANCED, Task::WKI_TARGET_FLAG_REMOTE | Task::WKI_TARGET_FLAG_STRICT,
              Task::WKI_TARGET_FLAG_BALANCED | Task::WKI_TARGET_FLAG_STRICT}) {
            KEXPECT_EQ(set_native_target(subject.task.get(), 0, 0, flags), -ENOTSUP);
            KEXPECT_TRUE(subject.task->wki_target_hostname == HOSTNAME);
            KEXPECT_EQ(subject.task->wki_target_flags, Task::WKI_TARGET_FLAG_LOCAL);
        }
        KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, 1, 0), -ENOTSUP);
        KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, SIZE_MAX, Task::WKI_TARGET_FLAG_STRICT), -ENOTSUP);
        KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, 1, Task::WKI_TARGET_FLAG_REMOTE), -ENOTSUP);
        KEXPECT_EQ(set_native_target(subject.task.get(), 0, 0, uint32_t{1} << 31), -EINVAL);
        KEXPECT_EQ(set_native_target(subject.task.get(), 0, 0, Task::WKI_TARGET_FLAG_REMOTE | Task::WKI_TARGET_FLAG_BALANCED), -EINVAL);
        KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, 1, Task::WKI_TARGET_FLAG_LOCAL), -EINVAL);
        KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, 1, Task::WKI_TARGET_FLAG_BALANCED), -EINVAL);
        KEXPECT_TRUE(subject.task->wki_target_hostname == HOSTNAME);
        KEXPECT_EQ(subject.task->wki_target_flags, Task::WKI_TARGET_FLAG_LOCAL);
        KEXPECT_EQ(subject.task->ref_count.load(), REFERENCES);
        KEXPECT_EQ(subject.task->fd_table.size(), 0U);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
        KEXPECT_EQ(subject.task->usercopy_pagemap_accesses.load(), uint32_t{0});
        auto const CURRENT = wki::resource_contract::snapshot(subject.task->resources);
        KEXPECT_EQ(std::memcmp(&RESOURCES, &CURRENT, sizeof(RESOURCES)), 0);
        pids.close();
        names.close();
        mounts.close();
        KEXPECT_EQ(set_native_target(subject.task.get(), 0, 0, Task::WKI_TARGET_FLAG_LOCAL), -EIDRM);
        KEXPECT_EQ(get_native_target(subject.task.get(), UINT64_MAX, SIZE_MAX, UINT64_MAX), -EIDRM);
        KEXPECT_TRUE(subject.task->wki_target_hostname == HOSTNAME);
        KEXPECT_EQ(subject.task->wki_target_flags, Task::WKI_TARGET_FLAG_LOCAL);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
    KEXPECT_EQ(vfs::mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentWki, MappedLocalPolicyAndClosedOutputsPreserveSubject) {
    auto const BASELINE = ipc::live_namespace_count();
    {
        ipc::NamespaceRef names;
        KREQUIRE_EQ(ipc::create_namespace(names), 0);
        UserTask subject;
        KREQUIRE_NE(subject.task->pagemap, nullptr);
        KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
        constexpr uint64_t OUTPUT = 0x650000;
        constexpr uint64_t FLAGS_OUTPUT = OUTPUT + 128;
        KREQUIRE_TRUE(subject.map_zero(OUTPUT));
        for (uint32_t modifiers = 0; modifiers < 8; ++modifiers) {
            uint32_t const FLAGS = ((modifiers & 1U) != 0 ? Task::WKI_TARGET_FLAG_STRICT : 0U) |
                                   ((modifiers & 2U) != 0 ? Task::WKI_TARGET_FLAG_NOINHERIT : 0U) |
                                   ((modifiers & 4U) != 0 ? Task::WKI_TARGET_FLAG_ONESHOT : 0U);
            for (uint32_t placement : {uint32_t{0}, Task::WKI_TARGET_FLAG_LOCAL}) {
                // A zero length keeps the existing clear semantics even with
                // an unusable hostname pointer. No source copy is necessary.
                KEXPECT_EQ(set_native_target(subject.task.get(), UINT64_MAX, 0, FLAGS | placement), 0);
                KEXPECT_EQ(get_native_target(subject.task.get(), OUTPUT, 1, FLAGS_OUTPUT), 0);
                char name = 99;
                uint32_t observed = UINT32_MAX;
                KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, OUTPUT, name));
                KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, FLAGS_OUTPUT, observed));
                KEXPECT_EQ(name, char{0});
                KEXPECT_EQ(observed, FLAGS | placement);
            }
        }
        auto const HOSTNAME = subject.task->wki_target_hostname;
        auto const FLAGS = subject.task->wki_target_flags;
        uint64_t const SENTINEL = 0x123456789abcdef0;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, OUTPUT, SENTINEL));
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, FLAGS_OUTPUT, SENTINEL));
        names.close();
        KEXPECT_EQ(get_native_target(subject.task.get(), OUTPUT, 64, FLAGS_OUTPUT), -EIDRM);
        KEXPECT_EQ(set_native_target(subject.task.get(), 0, 0, 0), -EIDRM);
        KEXPECT_TRUE(subject.task->wki_target_hostname == HOSTNAME);
        KEXPECT_EQ(subject.task->wki_target_flags, FLAGS);
        uint64_t observed = 0;
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, OUTPUT, observed));
        KEXPECT_EQ(observed, SENTINEL);
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, FLAGS_OUTPUT, observed));
        KEXPECT_EQ(observed, SENTINEL);
        KEXPECT_EQ(subject.task->usercopy_pagemap_accesses.load(), uint32_t{0});
    }
    KEXPECT_EQ(ipc::live_namespace_count(), BASELINE);
}

KTEST(CompartmentWki, CanonicalTargetAdmissionAndHostCopiesKeepExistingOwnership) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef pids;
        KREQUIRE_EQ(pid_ns::create_namespace(pids), 0);
        NativeTask owner, thread, unbound;
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(pids));
        thread.task->is_thread = true;
        thread.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *owner.task));
        unbound.task->is_thread = true;
        unbound.task->owner_pid = owner.task->pid;
        auto const OWNER_NAME = owner.task->wki_target_hostname;
        auto const OWNER_FLAGS = owner.task->wki_target_flags;
        KEXPECT_EQ(set_native_target(thread.task.get(), UINT64_MAX, 1, 0), -ENOTSUP);
        KEXPECT_EQ(set_native_target(thread.task.get(), 0, 0, Task::WKI_TARGET_FLAG_LOCAL), 0);
        KEXPECT_EQ(get_native_target(thread.task.get()), 0);
        KEXPECT_EQ(thread.task->wki_target_flags, Task::WKI_TARGET_FLAG_LOCAL);
        KEXPECT_TRUE(owner.task->wki_target_hostname == OWNER_NAME);
        KEXPECT_EQ(owner.task->wki_target_flags, OWNER_FLAGS);
        KEXPECT_EQ(set_native_target(unbound.task.get(), 0, 0, 0), -ESRCH);
        KEXPECT_EQ(get_native_target(unbound.task.get(), UINT64_MAX, SIZE_MAX, UINT64_MAX), -ESRCH);
        KEXPECT_EQ(set_native_target(nullptr, 0, 0, 0), -ESRCH);
        KEXPECT_EQ(get_native_target(nullptr), -ESRCH);
        pids.close();
        KEXPECT_EQ(set_native_target(thread.task.get(), 0, 0, 0), -EIDRM);
        KEXPECT_EQ(get_native_target(thread.task.get()), -EIDRM);
        KEXPECT_EQ(thread.task->wki_target_flags, Task::WKI_TARGET_FLAG_LOCAL);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    UserTask host;
    KREQUIRE_NE(host.task->pagemap, nullptr);
    constexpr uint64_t SOURCE = 0x660000;
    constexpr uint64_t OUTPUT = SOURCE + 128;
    constexpr uint64_t FLAGS_OUTPUT = SOURCE + 256;
    constexpr auto NAME = std::to_array("host.fixture");
    KREQUIRE_TRUE(host.map_zero(SOURCE));
    KREQUIRE_TRUE(usercopy::copy_to_task(*host.task, SOURCE, NAME.data(), NAME.size()));
    uint32_t const FLAGS = Task::WKI_TARGET_FLAG_REMOTE | Task::WKI_TARGET_FLAG_STRICT | Task::WKI_TARGET_FLAG_ONESHOT;
    KEXPECT_EQ(set_native_target(host.task.get(), SOURCE, NAME.size() - 1, FLAGS), 0);
    KEXPECT_EQ(get_native_target(host.task.get(), OUTPUT, NAME.size(), FLAGS_OUTPUT), static_cast<int>(NAME.size() - 1));
    std::array<char, NAME.size()> observed{};
    uint32_t observed_flags = 0;
    KREQUIRE_TRUE(usercopy::copy_from_task(*host.task, OUTPUT, observed.data(), observed.size()));
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, FLAGS_OUTPUT, observed_flags));
    KEXPECT_TRUE(observed == NAME);
    KEXPECT_EQ(observed_flags, FLAGS);
    auto const HOSTNAME = host.task->wki_target_hostname;
    KEXPECT_EQ(set_native_target(host.task.get(), UINT64_MAX, 1, 0), -EFAULT);
    KEXPECT_EQ(set_native_target(host.task.get(), UINT64_MAX, host.task->wki_target_hostname.size(), 0), -ENAMETOOLONG);
    KEXPECT_TRUE(host.task->wki_target_hostname == HOSTNAME);
    KEXPECT_EQ(host.task->wki_target_flags, FLAGS);
    uint64_t const SENTINEL = 0x76543210fedcba98;
    KREQUIRE_TRUE(usercopy::copy_value_to_task(*host.task, OUTPUT, SENTINEL));
    KREQUIRE_TRUE(usercopy::copy_value_to_task(*host.task, FLAGS_OUTPUT, SENTINEL));
    KEXPECT_EQ(get_native_target(host.task.get(), OUTPUT, 1, FLAGS_OUTPUT), -ENAMETOOLONG);
    KEXPECT_EQ(get_native_target(host.task.get(), OUTPUT, NAME.size(), UINT64_MAX), -EFAULT);
    uint64_t unchanged = 0;
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, OUTPUT, unchanged));
    KEXPECT_EQ(unchanged, SENTINEL);
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, FLAGS_OUTPUT, unchanged));
    KEXPECT_EQ(unchanged, SENTINEL);
    KEXPECT_EQ(set_native_target(host.task.get(), 0, SIZE_MAX, Task::WKI_TARGET_FLAG_BALANCED), 0);
    KEXPECT_EQ(host.task->wki_target_hostname.front(), char{0});
    KEXPECT_EQ(host.task->wki_target_flags, Task::WKI_TARGET_FLAG_BALANCED);
    KEXPECT_EQ(host.task->usercopy_pagemap_accesses.load(), uint32_t{0});
}

namespace {
using VfsRuleOp = abi::vfs::ops;
using VfsRoute = sched::task::WkiVfsRoute;
auto native_rule_control(Task* subject, VfsRuleOp op, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0) -> int64_t {
    return ker::syscall::vfs::wki_rules_for_subject(subject, static_cast<uint64_t>(op), a1, a2, a3, a4);
}
auto inherit_remote_rule(Task& subject) -> bool {
    sched::task::WkiVfsRule rule{};
    std::strcpy(rule.prefix.data(), "/");
    rule.prefix_len = 1;
    rule.route = static_cast<uint8_t>(VfsRoute::HOST);
    std::strcpy(subject.wki_submitter_hostname.data(), "foreign.fixture");
    return subject.wki_vfs_rules.push_back(rule);
}
struct HostRoutingMount {
    static constexpr const char* PATH = "/__goal39_routing25";
    bool mounted{};
    auto install() -> int {
        int const RESULT = private_mount({}, PATH);
        mounted = RESULT == 0;
        return RESULT;
    }
    ~HostRoutingMount() {
        if (mounted) KEXPECT_EQ(vfs::unmount_filesystem(PATH), 0);
    }
};
}  // namespace

KTEST(CompartmentRouting, PrivateRuleControlsRefuseCopiesAndKeepInheritedFields) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    for (unsigned variant = 1; variant < 8; ++variant) {
        pid_ns::NamespaceRef host, pids;
        ipc::NamespaceRef names;
        mount_ns::NamespaceRef mounts;
        UserTask subject;
        KREQUIRE_NE(subject.task->pagemap, nullptr);
        constexpr uint64_t ADDRESS = 0x670000;
        constexpr uint64_t ROUTE_OUTPUT = ADDRESS + 128;
        KREQUIRE_TRUE(subject.map_zero(ADDRESS));
        constexpr auto PREFIX = std::to_array("/mapped.fixture");
        KREQUIRE_TRUE(usercopy::copy_to_task(*subject.task, ADDRESS, PREFIX.data(), PREFIX.size()));
        if ((variant & 1U) != 0) {
            KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, pids), 0);
            KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
        }
        if ((variant & 2U) != 0) {
            KREQUIRE_EQ(ipc::create_namespace(names), 0);
            KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
        }
        if ((variant & 4U) != 0) {
            KREQUIRE_EQ(mount_ns::create_namespace(mounts), 0);
            KREQUIRE_TRUE(subject.task->initialize_mount_namespace(mounts));
        }
        KREQUIRE_TRUE(inherit_remote_rule(*subject.task));
        auto const RULE = subject.task->wki_vfs_rules[0];
        auto const REFERENCES = subject.task->ref_count.load();
        auto const RESOURCES = wki::resource_contract::snapshot(subject.task->resources);
        uint32_t const SENTINEL = 0x76543210;
        KREQUIRE_TRUE(usercopy::copy_value_to_task(*subject.task, ROUTE_OUTPUT, SENTINEL));
        KEXPECT_EQ(native_rule_control(subject.task.get(), VfsRuleOp::WKI_RULE_ADD, ADDRESS, 1), int64_t{-ENOTSUP});
        KEXPECT_EQ(native_rule_control(subject.task.get(), VfsRuleOp::WKI_RULE_ADD, UINT64_MAX, UINT64_MAX), int64_t{-ENOTSUP});
        for (auto op : {VfsRuleOp::WKI_RULE_GET, VfsRuleOp::WKI_RULE_GET_DEFAULT}) {
            KEXPECT_EQ(native_rule_control(subject.task.get(), op, 0, ADDRESS, PREFIX.size(), ROUTE_OUTPUT), int64_t{-ENOTSUP});
            KEXPECT_EQ(native_rule_control(subject.task.get(), op, UINT64_MAX, UINT64_MAX, SIZE_MAX, UINT64_MAX), int64_t{-ENOTSUP});
        }
        KEXPECT_EQ(native_rule_control(subject.task.get(), VfsRuleOp::WKI_RULE_CLEAR), int64_t{-ENOTSUP});
        std::array<char, PREFIX.size()> observed{};
        uint32_t route = 0;
        KREQUIRE_TRUE(usercopy::copy_from_task(*subject.task, ADDRESS, observed.data(), observed.size()));
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*subject.task, ROUTE_OUTPUT, route));
        KEXPECT_TRUE(observed == PREFIX);
        KEXPECT_EQ(route, SENTINEL);
        KEXPECT_EQ(vfs::vfs_wki_rule_add_for_task(subject.task.get(), reinterpret_cast<const char*>(UINTPTR_MAX), 1), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_wki_rule_get_for_task(subject.task.get(), 0, observed.data(), observed.size(), &route), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_wki_default_rule_get_for_task(subject.task.get(), 0, observed.data(), observed.size(), &route), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_wki_rule_clear_for_task(subject.task.get()), -ENOTSUP);
        KEXPECT_EQ(vfs::vfs_wki_effective_route_for_path(subject.task.get(), "/usr/bin/fixture", &route), 0);
        KEXPECT_EQ(route, static_cast<uint32_t>(VfsRoute::LOCAL));
        KEXPECT_EQ(subject.task->wki_vfs_rules.size(), size_t{1});
        KEXPECT_EQ(std::memcmp(&RULE, &subject.task->wki_vfs_rules[0], sizeof(RULE)), 0);
        KEXPECT_EQ(subject.task->ref_count.load(), REFERENCES);
        auto const CURRENT = wki::resource_contract::snapshot(subject.task->resources);
        KEXPECT_EQ(std::memcmp(&RESOURCES, &CURRENT, sizeof(CURRENT)), 0);
        KEXPECT_EQ(subject.task->usercopy_pagemap_accesses.load(), uint32_t{0});
        pids.close();
        names.close();
        mounts.close();
        for (auto op : {VfsRuleOp::WKI_RULE_ADD, VfsRuleOp::WKI_RULE_GET, VfsRuleOp::WKI_RULE_GET_DEFAULT, VfsRuleOp::WKI_RULE_CLEAR})
            KEXPECT_EQ(native_rule_control(subject.task.get(), op, UINT64_MAX, UINT64_MAX, SIZE_MAX, UINT64_MAX), int64_t{-EIDRM});
        route = SENTINEL;
        KEXPECT_EQ(vfs::vfs_wki_effective_route_for_path(subject.task.get(), "/", &route), -EIDRM);
        KEXPECT_EQ(route, SENTINEL);
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentRouting, CanonicalSubjectsAndMappedHostRulesKeepOriginalErrors) {
    auto const BASELINE = pid_ns::live_namespace_count();
    {
        pid_ns::NamespaceRef pids;
        KREQUIRE_EQ(pid_ns::create_namespace(pids), 0);
        NativeTask owner, thread, unbound;
        KREQUIRE_TRUE(owner.task->initialize_pid_namespace(pids));
        thread.task->is_thread = true;
        thread.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *owner.task));
        unbound.task->is_thread = true;
        unbound.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(inherit_remote_rule(*thread.task));
        for (auto op : {VfsRuleOp::WKI_RULE_ADD, VfsRuleOp::WKI_RULE_GET, VfsRuleOp::WKI_RULE_GET_DEFAULT, VfsRuleOp::WKI_RULE_CLEAR}) {
            KEXPECT_EQ(native_rule_control(thread.task.get(), op, UINT64_MAX, UINT64_MAX, SIZE_MAX, UINT64_MAX), int64_t{-ENOTSUP});
            KEXPECT_EQ(native_rule_control(unbound.task.get(), op), int64_t{-ESRCH});
            KEXPECT_EQ(native_rule_control(nullptr, op), int64_t{-ESRCH});
        }
        KEXPECT_EQ(thread.task->wki_vfs_rules.size(), size_t{1});
        KEXPECT_EQ(owner.task->wki_vfs_rules.size(), size_t{0});
        uint32_t route = 17;
        KEXPECT_EQ(vfs::vfs_wki_effective_route_for_path(unbound.task.get(), "/", &route), -ESRCH);
        KEXPECT_EQ(route, uint32_t{17});
        pids.close();
        KEXPECT_EQ(native_rule_control(thread.task.get(), VfsRuleOp::WKI_RULE_CLEAR), int64_t{-EIDRM});
    }
    KEXPECT_EQ(pid_ns::live_namespace_count(), BASELINE);
    UserTask host;
    KREQUIRE_NE(host.task->pagemap, nullptr);
    constexpr uint64_t INPUT = 0x680000, OUTPUT = INPUT + 128, ROUTE_OUTPUT = INPUT + 256;
    KREQUIRE_TRUE(host.map_zero(INPUT));
    constexpr auto PREFIX = std::to_array("a/../b//");
    KREQUIRE_TRUE(usercopy::copy_to_task(*host.task, INPUT, PREFIX.data(), PREFIX.size()));
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_ADD, INPUT, 1), int64_t{0});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET, 0, OUTPUT, 32, ROUTE_OUTPUT), int64_t{2});
    std::array<char, 3> observed{};
    uint32_t route = 0;
    KREQUIRE_TRUE(usercopy::copy_from_task(*host.task, OUTPUT, observed.data(), observed.size()));
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, ROUTE_OUTPUT, route));
    KEXPECT_EQ(std::strcmp(observed.data(), "/b"), 0);
    KEXPECT_EQ(route, uint32_t{1});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_ADD, INPUT, 0), int64_t{0});
    KEXPECT_EQ(host.task->wki_vfs_rules.size(), size_t{1});
    KEXPECT_EQ(host.task->wki_vfs_rules[0].route, uint8_t{0});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_ADD, 0, 0), int64_t{-EFAULT});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_ADD, UINT64_MAX, 3), int64_t{-EFAULT});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_ADD, INPUT, 3), int64_t{-EINVAL});
    uint64_t const SENTINEL = 0x654321abcdef9876;
    KREQUIRE_TRUE(usercopy::copy_value_to_task(*host.task, OUTPUT, SENTINEL));
    KREQUIRE_TRUE(usercopy::copy_value_to_task(*host.task, ROUTE_OUTPUT, SENTINEL));
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET, 0, OUTPUT, 2, ROUTE_OUTPUT), int64_t{-ERANGE});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET, 0, OUTPUT, 32, UINT64_MAX), int64_t{-EFAULT});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET, UINT64_MAX, UINT64_MAX, 32, 0), int64_t{-EFAULT});
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET, UINT64_MAX), int64_t{-ENOENT});
    uint64_t unchanged = 0;
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, OUTPUT, unchanged));
    KEXPECT_EQ(unchanged, SENTINEL);
    KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, ROUTE_OUTPUT, unchanged));
    KEXPECT_EQ(unchanged, SENTINEL);
    std::array<char, Task::CWD_MAX> defaults{};
    uint32_t default_route = 0;
    int const DEFAULT = vfs::vfs_wki_default_rule_get(0, defaults.data(), defaults.size(), &default_route);
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_GET_DEFAULT, 0, OUTPUT, defaults.size(), ROUTE_OUTPUT),
               static_cast<int64_t>(DEFAULT));
    if (DEFAULT >= 0) {
        std::array<char, Task::CWD_MAX> output{};
        KREQUIRE_TRUE(usercopy::copy_from_task(*host.task, OUTPUT, output.data(), static_cast<size_t>(DEFAULT) + 1));
        KREQUIRE_TRUE(usercopy::copy_value_from_task(*host.task, ROUTE_OUTPUT, route));
        KEXPECT_EQ(std::strcmp(output.data(), defaults.data()), 0);
        KEXPECT_EQ(route, default_route);
    }
    KEXPECT_EQ(native_rule_control(host.task.get(), VfsRuleOp::WKI_RULE_CLEAR), int64_t{0});
    KEXPECT_EQ(host.task->wki_vfs_rules.size(), size_t{0});
    KEXPECT_EQ(native_rule_control(nullptr, VfsRuleOp::OPEN), int64_t{-EINVAL});
    KEXPECT_EQ(host.task->usercopy_pagemap_accesses.load(), uint32_t{0});
}

KTEST(CompartmentRouting, PrivateNativeNamesIgnoreRemoteFieldsAndKeepFileCapabilities) {
    auto const HOST_COUNT = vfs::get_mount_count();
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    auto const MOUNT_BASELINE = mount_ns::live_namespace_count();
    {
        HostRoutingMount host_mount;
        KREQUIRE_EQ(host_mount.install(), 0);
        for (unsigned variant = 1; variant < 8; ++variant) {
            PrivateMounts views;
            pid_ns::NamespaceRef host, pids;
            ipc::NamespaceRef names;
            NativeTask subject;
            if ((variant & 1U) != 0) {
                KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, pids), 0);
                KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
            }
            if ((variant & 2U) != 0) {
                KREQUIRE_EQ(ipc::create_namespace(names), 0);
                KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
            }
            if ((variant & 4U) != 0) {
                KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
                KREQUIRE_EQ(private_mount(views.first, "/"), 0);
                KREQUIRE_TRUE(subject.task->initialize_mount_namespace(views.first));
            }
            auto mount = scoped_mount(views.first, (variant & 4U) != 0 ? "/" : HostRoutingMount::PATH);
            KREQUIRE_TRUE(mount);
            const char* path = (variant & 4U) != 0 ? "/data" : "/__goal39_routing25/data";
            int const CREATED = subject.open(path);
            KREQUIRE_TRUE(CREATED >= 0);
            NativeFile backing(vfs::vfs_get_file_retain(subject.task.get(), CREATED));
            KREQUIRE_TRUE(backing);
            constexpr auto PAYLOAD = std::to_array("retained");
            KREQUIRE_EQ(vfs::vfs_write_file(backing.get(), PAYLOAD.data(), PAYLOAD.size()), static_cast<ssize_t>(PAYLOAD.size()));
            KREQUIRE_TRUE(inherit_remote_rule(*subject.task));
            int const FD = subject.open(path, 0);
            KREQUIRE_TRUE(FD >= 0);
            NativeFile retained(vfs::vfs_get_file_retain(subject.task.get(), FD));
            KREQUIRE_TRUE(retained);
            KEXPECT_TRUE(retained->mount_owner == mount.get());
            vfs::LookupHandle lookup;
            KREQUIRE_EQ(task_lookup(*subject.task, path, lookup), 0);
            KEXPECT_TRUE(lookup.mount() == mount.get());
            lookup.reset();
            // A retained remote directory is an FD capability, not permission
            // to create child name requests in an unsupported private scope.
            NativeFile remote(new vfs::File{});
            remote->refcount.store(1);
            remote->fs_type = vfs::FSType::REMOTE;
            remote->is_directory = true;
            auto* remote_file = remote.get();
            vfs::FdReservation reservation(subject.task.get());
            int const REMOTE_FD = reservation.install(remote.get());
            KREQUIRE_TRUE(REMOTE_FD >= 0);
            static_cast<void>(remote.release());
            KREQUIRE_EQ(subject.track(REMOTE_FD), REMOTE_FD);
            int const REMOTE_ERROR = (variant & 4U) != 0 ? -EXDEV : -ENOTSUP;
            KEXPECT_EQ(task_lookup(*subject.task, "child", lookup, REMOTE_FD), REMOTE_ERROR);
            KEXPECT_NULL(lookup.mount());
            KEXPECT_EQ(remote_file->refcount.load(), 1);
            KEXPECT_EQ(vfs::vfs_acquire_lookup(subject.task.get(), vfs::AT_FDCWD, "/wki/foreign.fixture/child", vfs::LookupIntent::STAT,
                                               vfs::LookupFollowPolicy::FOLLOW_FINAL, false, true, &lookup, 0, true),
                       -ENOTSUP);
            if ((variant & 4U) != 0) {
                KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/wki", 0755), 0);
                KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/wki/host", 0755), 0);
                KREQUIRE_EQ(vfs::vfs_mkdirat(subject.task.get(), vfs::AT_FDCWD, "/wki/host/local", 0755), 0);
                KREQUIRE_EQ(task_lookup(*subject.task, "/wki/host/local", lookup), 0);
                KEXPECT_TRUE(lookup.mount() == mount.get());
                lookup.reset();
            }
            pids.close();
            names.close();
            views.first.close();
            vfs::Stat stat{};
            KEXPECT_EQ(vfs::vfs_statat(subject.task.get(), vfs::AT_FDCWD, path, 0, &stat), -EIDRM);
            std::array<char, PAYLOAD.size()> observed{};
            KEXPECT_EQ(retained->fops->vfs_read(retained.get(), observed.data(), observed.size(), 0), static_cast<ssize_t>(PAYLOAD.size()));
            KEXPECT_TRUE(observed == PAYLOAD);
            KEXPECT_EQ(vfs::vfs_fstat_file(retained.get(), &stat), 0);
        }
    }
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
    KEXPECT_EQ(mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

namespace {
namespace power = ker::mod::power;
namespace hostname = ker::util::hostname;
namespace security = ker::mod::security;
auto set_native_hostname(Task* subject, uint64_t address, size_t length) -> int {
    return static_cast<int>(static_cast<int64_t>(ker::syscall::process::set_hostname_for_subject(subject, address, length)));
}
void expect_host_controls_refused(Task* subject, int error) {
    auto const NAME = hostname::snapshot();
    auto const PHASE = power::shutdown_phase();
    KEXPECT_EQ(set_native_hostname(subject, UINT64_MAX, 1), error);
    KEXPECT_EQ(set_native_hostname(subject, 0, 0), error);
    KEXPECT_EQ(power::selftest_prepare_shutdown_for_subject(subject), error);
    for (uint64_t command : {abi::power::RB_AUTOBOOT, abi::power::RB_POWER_OFF, abi::power::RB_HALT_SYSTEM, abi::power::RB_ENABLE_CAD,
                             abi::power::RB_DISABLE_CAD}) {
        KEXPECT_EQ(power::selftest_begin_reboot_for_subject(subject, command), error);
        KEXPECT_EQ(power::shutdown_phase(), PHASE);
    }
    // Command decoding keeps its original precedence over subject admission.
    KEXPECT_EQ(power::selftest_begin_reboot_for_subject(subject, UINT64_MAX), -EINVAL);
    KEXPECT_TRUE(hostname::snapshot() == NAME);
    KEXPECT_EQ(power::shutdown_phase(), PHASE);
}
}  // namespace

KTEST(CompartmentControl, PrivateRootAndSharedNetworkCannotChangeHostState) {
    auto const PID_BASELINE = pid_ns::live_namespace_count();
    auto const IPC_BASELINE = ipc::live_namespace_count();
    auto const MOUNT_BASELINE = vfs::mount_ns::live_namespace_count();
    auto const NAME = hostname::snapshot();
    auto const PHASE = power::shutdown_phase();
    for (unsigned variant = 1; variant < 8; ++variant) {
        pid_ns::NamespaceRef host, pids;
        ipc::NamespaceRef names;
        vfs::mount_ns::NamespaceRef mounts;
        NativeTask subject;
        if ((variant & 1U) != 0) {
            KREQUIRE_EQ(pid_ns::create_namespace(host, Network::SHARED_HOST, pids), 0);
            KREQUIRE_TRUE(subject.task->initialize_pid_namespace(pids));
        }
        if ((variant & 2U) != 0) {
            KREQUIRE_EQ(ipc::create_namespace(names), 0);
            KREQUIRE_TRUE(subject.task->initialize_ipc_namespace(names));
        }
        if ((variant & 4U) != 0) {
            KREQUIRE_EQ(vfs::mount_ns::create_namespace(mounts), 0);
            KREQUIRE_TRUE(subject.task->initialize_mount_namespace(mounts));
        }
        auto const CREDENTIALS = subject.task->credentials();
        KREQUIRE_TRUE(CREDENTIALS == security::Snapshot::bootstrap_root());
        auto const REFERENCES = subject.task->ref_count.load();
        auto const RESOURCES = wki::resource_contract::snapshot(subject.task->resources);
        KEXPECT_EQ(security::compartment::host_namespace_admission(subject.task.get()), -EPERM);
        expect_host_controls_refused(subject.task.get(), -EPERM);
        pids.close();
        names.close();
        mounts.close();
        KEXPECT_EQ(security::compartment::host_namespace_admission(subject.task.get()), -EIDRM);
        expect_host_controls_refused(subject.task.get(), -EIDRM);
        KEXPECT_TRUE(subject.task->credentials() == CREDENTIALS);
        KEXPECT_EQ(subject.task->ref_count.load(), REFERENCES);
        KEXPECT_EQ(subject.task->fd_table.size(), 0U);
        KEXPECT_EQ(subject.task->fd_table.reserved_count(), 0U);
        KEXPECT_EQ(subject.task->usercopy_pagemap_accesses.load(), uint32_t{0});
        auto const CURRENT = wki::resource_contract::snapshot(subject.task->resources);
        KEXPECT_EQ(std::memcmp(&RESOURCES, &CURRENT, sizeof(RESOURCES)), 0);
    }
    KEXPECT_TRUE(hostname::snapshot() == NAME);
    KEXPECT_EQ(power::shutdown_phase(), PHASE);
    KEXPECT_EQ(pid_ns::live_namespace_count(), PID_BASELINE);
    KEXPECT_EQ(ipc::live_namespace_count(), IPC_BASELINE);
    KEXPECT_EQ(vfs::mount_ns::live_namespace_count(), MOUNT_BASELINE);
}

KTEST(CompartmentControl, MappedNamesCanonicalThreadsAndClosedOwnersRefuseEffects) {
    auto const BASELINE = ipc::live_namespace_count();
    {
        ipc::NamespaceRef names;
        KREQUIRE_EQ(ipc::create_namespace(names), 0);
        UserTask owner;
        NativeTask thread, unbound;
        KREQUIRE_NE(owner.task->pagemap, nullptr);
        KREQUIRE_TRUE(owner.task->initialize_ipc_namespace(names));
        thread.task->is_thread = true;
        thread.task->owner_pid = owner.task->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread.task, *owner.task));
        unbound.task->is_thread = true;
        unbound.task->owner_pid = owner.task->pid;
        constexpr uint64_t SOURCE = 0x670000;
        constexpr auto INPUT = std::to_array("private-escape");
        KREQUIRE_TRUE(owner.map_zero(SOURCE));
        KREQUIRE_TRUE(usercopy::copy_to_task(*owner.task, SOURCE, INPUT.data(), INPUT.size()));
        auto const NAME = hostname::snapshot();
        auto const PHASE = power::shutdown_phase();
        auto const REFERENCES = owner.task->ref_count.load();
        KREQUIRE_TRUE(thread.task->credentials() == security::Snapshot::bootstrap_root());
        KEXPECT_EQ(set_native_hostname(owner.task.get(), SOURCE, INPUT.size() - 1), -EPERM);
        KEXPECT_EQ(set_native_hostname(thread.task.get(), SOURCE, INPUT.size() - 1), -EPERM);
        expect_host_controls_refused(thread.task.get(), -EPERM);
        expect_host_controls_refused(unbound.task.get(), -ESRCH);
        expect_host_controls_refused(nullptr, -ESRCH);
        names.close();
        KEXPECT_EQ(set_native_hostname(owner.task.get(), SOURCE, INPUT.size() - 1), -EIDRM);
        KEXPECT_EQ(set_native_hostname(thread.task.get(), SOURCE, INPUT.size() - 1), -EIDRM);
        expect_host_controls_refused(thread.task.get(), -EIDRM);
        std::array<char, INPUT.size()> observed{};
        KREQUIRE_TRUE(usercopy::copy_from_task(*owner.task, SOURCE, observed.data(), observed.size()));
        KEXPECT_TRUE(observed == INPUT);
        KEXPECT_TRUE(hostname::snapshot() == NAME);
        KEXPECT_EQ(power::shutdown_phase(), PHASE);
        KEXPECT_EQ(owner.task->ref_count.load(), REFERENCES);
        KEXPECT_EQ(owner.task->usercopy_pagemap_accesses.load(), uint32_t{0});
        KEXPECT_EQ(thread.task->usercopy_pagemap_accesses.load(), uint32_t{0});
    }
    KEXPECT_EQ(ipc::live_namespace_count(), BASELINE);
}

KTEST(CompartmentControl, HostValidationAndDroppedAuthorityKeepExistingBehavior) {
    UserTask host;
    KREQUIRE_NE(host.task->pagemap, nullptr);
    KREQUIRE_TRUE(host.task->credentials() == security::Snapshot::bootstrap_root());
    KEXPECT_EQ(security::compartment::host_namespace_admission(host.task.get()), 0);
    constexpr uint64_t SOURCE = 0x680000;
    constexpr auto INVALID_NAME = std::to_array("invalid.name");
    KREQUIRE_TRUE(host.map_zero(SOURCE));
    KREQUIRE_TRUE(usercopy::copy_to_task(*host.task, SOURCE, INVALID_NAME.data(), INVALID_NAME.size()));
    auto const NAME = hostname::snapshot();
    auto const PHASE = power::shutdown_phase();
    KEXPECT_EQ(set_native_hostname(host.task.get(), SOURCE, INVALID_NAME.size() - 1), -EINVAL);
    KEXPECT_EQ(set_native_hostname(host.task.get(), UINT64_MAX, 1), -EFAULT);
    KEXPECT_EQ(set_native_hostname(host.task.get(), 0, 0), -EFAULT);
    KEXPECT_EQ(set_native_hostname(host.task.get(), SOURCE, 0), -EINVAL);
    KEXPECT_EQ(set_native_hostname(host.task.get(), SOURCE, hostname::HOSTNAME_MAX), -EINVAL);
    KEXPECT_EQ(power::selftest_begin_reboot_for_subject(host.task.get(), UINT64_MAX), -EINVAL);
    KEXPECT_EQ(power::begin_reboot_command(UINT64_MAX), -EINVAL);
    KEXPECT_EQ(power::selftest_begin_reboot_for_subject(host.task.get(), abi::power::RB_ENABLE_CAD), 0);
    KEXPECT_EQ(power::selftest_begin_reboot_for_subject(host.task.get(), abi::power::RB_DISABLE_CAD), 0);
    security::Snapshot restricted;
    uint64_t const CEILING = security::ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(security::Authority::HOSTNAME)) &
                             ~(uint64_t{1} << static_cast<unsigned>(security::Authority::POWER));
    KREQUIRE_EQ(host.task->credentials().prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = CEILING}, restricted),
                0);
    KREQUIRE_EQ(resource::initialize_credentials(*host.task, restricted), 0);
    KEXPECT_EQ(host.task->credentials().user().effective, uint32_t{0});
    expect_host_controls_refused(host.task.get(), -EPERM);
    KREQUIRE_EQ(resource::initialize_credentials(*host.task, security::Snapshot{}), 0);
    expect_host_controls_refused(host.task.get(), -EPERM);
    KEXPECT_TRUE(hostname::snapshot() == NAME);
    KEXPECT_EQ(power::shutdown_phase(), PHASE);
    KEXPECT_EQ(host.task->usercopy_pagemap_accesses.load(), uint32_t{0});
}
