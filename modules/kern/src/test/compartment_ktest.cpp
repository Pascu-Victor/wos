#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credentials.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <test/ktest.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/filesystem_context.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/lookup.hpp>
#include <vfs/mount.hpp>

namespace ipc = ker::mod::sched::ipc;
namespace resource = ker::mod::sched::resource;
using ker::mod::sched::task::Task;

namespace pid_ns = ker::mod::sched::pid_ns;
namespace sched = ker::mod::sched;

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
void expect_visible(const Task& requester, uint64_t id, Task* expected, bool lifetime = false) {
    auto* found =
        lifetime ? sched::find_task_by_visible_pid_lifetime_ref(requester, id) : sched::find_task_by_visible_pid_safe(requester, id);
    KEXPECT_TRUE(found == expected);
    if (found != nullptr) found->release();
}
}  // namespace

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
        for (auto* type : {"xfs", "fat32", "remote", "devfs", "procfs", ""})
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
    for (auto* type : {"devfs", "procfs", "remote", "fat32", "xfs"}) {
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
