#include <memory>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <test/ktest.hpp>

namespace ipc = ker::mod::sched::ipc;
namespace resource = ker::mod::sched::resource;
using ker::mod::sched::task::Task;

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
