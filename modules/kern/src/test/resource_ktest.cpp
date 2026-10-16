#ifdef WOS_SELFTEST
#include <test/numa_lifecycle_mm_debt.hpp>
#endif
#include <abi/resource.hpp>
#include <cerrno>
#include <net/wki/remote_compute.hpp>
#include <platform/dbg/coredump.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/file_growth.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/time/time.hpp>
#include <test/ktest.hpp>
#include <vfs/advisory.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace {
constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
using ker::mod::sched::task::Task;
}  // namespace

KTEST(Resource, PersistentServiceLifecycleAndThreadCpu) { KEXPECT_TRUE(ker::mod::sched::resource::selftest_service_lifecycle()); }
KTEST(Resource, NativeCreationKeepsSelectedServiceAndNonEscalatingLimits) {
    KEXPECT_TRUE(ker::mod::sched::resource::selftest_native_creator_service_policy());
}
KTEST(Resource, ServiceManagementRequiresRoleAndRetainedCredentialAuthority) {
    KEXPECT_TRUE(ker::mod::sched::resource::selftest_service_management_authority());
}
KTEST(Resource, RemoteServiceAccountingOutlivesRootProcess) {
    KEXPECT_TRUE(ker::mod::sched::resource::selftest_remote_service_accounting_lifecycle());
}
KTEST(Resource, ServiceReportsRetainDestinationAndMergeOnce) { KEXPECT_TRUE(ker::mod::sched::resource::selftest_service_report_targets()); }
KTEST(Resource, ServiceObservationMergePreservesLocalDenials) {
    KEXPECT_TRUE(ker::mod::sched::resource::selftest_service_observation_merge());
}
KTEST(Resource, RemotePolicyIsValidatedBeforeFetch) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_resource_context_before_fetch());
}

KTEST(Resource, ReceiverPopulationAdmissionBeforeTaskConstruction) {
    namespace resource = ker::mod::sched::resource;
    constexpr uint32_t UID = 0xffffffe0;
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    resource::State policy;
    KEXPECT_EQ(policy.set(NPROC, {0, UINT64_MAX}, true, 256), 0);
    {
        resource::ProcessReservation denied(policy, UID);
        KEXPECT_EQ(denied.result(), -EAGAIN);
        KEXPECT_EQ(policy.entries[NPROC].denied, 1);
        KEXPECT_EQ(policy.entries[NPROC].current, 0);
    }
    KEXPECT_EQ(policy.set(NPROC, {1, UINT64_MAX}, true, 256), 0);
    {
        resource::ProcessReservation first(policy, UID);
        KEXPECT_EQ(first.result(), 0);
        resource::ProcessReservation denied(policy, UID);
        KEXPECT_EQ(denied.result(), -EAGAIN);
        Task child{};
        KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(child, UID), 0);
        child.resources = policy;
        KEXPECT_TRUE(first.attach(child));
        KEXPECT_EQ(child.resources.entries[NPROC].current, 1);
        child.process_charge.release();
        KEXPECT_FALSE(child.process_charge.linked);
        ker::abi::resource::Usage observation{};
        resource::copy_process_observation(child, observation);
        KEXPECT_EQ(observation.current, 0);
        KEXPECT_EQ(observation.peak, 1);
    }
    {
        resource::ProcessReservation cancelled(policy, UID);
        KEXPECT_EQ(cancelled.result(), 0);
        cancelled.cancel();
        KEXPECT_EQ(cancelled.result(), -ECANCELED);
        resource::ProcessReservation reused(policy, UID);
        KEXPECT_EQ(reused.result(), 0);
    }
}

KTEST(Resource, DescriptorReservationLifecycle) {
    Task task{};
    ker::vfs::File file{};
    KEXPECT_EQ(task.resources.set(NOFILE, {1, 256}, true, 256), 0);
    {
        ker::vfs::FdReservation first(&task);
        KEXPECT_EQ(first.result(), 0);
        KEXPECT_EQ(task.ref_count.load(), 2);
        KEXPECT_EQ(task.fd_table.size(), 0);
        KEXPECT_EQ(task.fd_table.reserved_count(), 1);
        KEXPECT_NULL(task.fd_table.lookup(0));
        ker::vfs::FdReservation denied(&task);
        KEXPECT_EQ(denied.result(), -EMFILE);
        KEXPECT_EQ(task.resources.entries[NOFILE].denied, 1);
        // Accepted reservations survive a subsequent lower limit.
        KEXPECT_EQ(task.resources.set(NOFILE, {0, 256}, true, 256), 0);
        KEXPECT_EQ(first.install(&file, true), 0);
        KEXPECT_EQ(task.ref_count.load(), 1);
        KEXPECT_TRUE(task.get_fd_cloexec(0));
        KEXPECT_EQ(task.resources.entries[NOFILE].current, 1);
    }
    KEXPECT_EQ(ker::vfs::vfs_release_fd(&task, 0), 0);
    KEXPECT_EQ(task.resources.entries[NOFILE].current, 0);
    KEXPECT_EQ(task.resources.entries[NOFILE].peak, 1);
    KEXPECT_EQ(task.fd_table.reserved_count(), 0);
    KEXPECT_NULL(task.pending_fd_reservations);
}

KTEST(Resource, FatalExitCancelsPendingDescriptors) {
    // KTEST runs before process scheduling, so reservations register on target.
    Task task{};
    ker::vfs::FdReservation first(&task);
    ker::vfs::FdReservation second(&task);
    KEXPECT_EQ(first.result(), 0);
    KEXPECT_EQ(second.result(), 1);
    KEXPECT_EQ(task.ref_count.load(), 3);
    ker::vfs::cancel_fd_reservations(task);
    KEXPECT_EQ(task.ref_count.load(), 1);
    KEXPECT_EQ(task.fd_table.reserved_count(), 0);
    KEXPECT_EQ(task.resources.entries[NOFILE].current, 0);
    KEXPECT_EQ(first.result(), -ECANCELED);
    KEXPECT_EQ(second.result(), -ECANCELED);
    KEXPECT_NULL(task.pending_fd_reservations);
}

KTEST(Resource, DescriptorPairPublishesTogether) {
    Task task{};
    ker::vfs::File first_file{};
    ker::vfs::File second_file{};
    KEXPECT_EQ(task.resources.set(NOFILE, {2, 256}, true, 256), 0);
    ker::vfs::FdReservation first(&task);
    ker::vfs::FdReservation second(&task);
    KEXPECT_EQ(task.fd_table.size(), 0);
    KEXPECT_EQ(task.fd_table.reserved_count(), 2);
    KEXPECT_EQ(first.install_pair(second, &first_file, &second_file, true), 0);
    KEXPECT_EQ(task.fd_table.size(), 2);
    KEXPECT_EQ(task.fd_table.reserved_count(), 0);
    KEXPECT_EQ(task.ref_count.load(), 1);
    KEXPECT_TRUE(task.get_fd_cloexec(0) && task.get_fd_cloexec(1));
    KEXPECT_EQ(ker::vfs::vfs_release_fd(&task, 0), 0);
    KEXPECT_EQ(ker::vfs::vfs_release_fd(&task, 1), 0);
    KEXPECT_EQ(task.resources.entries[NOFILE].current, 0);
}

KTEST(Resource, ForkSnapshotIsCoherentAndIndependent) {
    Task parent{};
    Task child{};
    KEXPECT_EQ(parent.resources.set(NOFILE, {7, 11}, true, 256), 0);
    KEXPECT_TRUE(ker::mod::sched::resource::inherit(child, parent));
    KEXPECT_EQ(parent.resources.set(NOFILE, {1, 2}, true, 256), 0);
    KEXPECT_EQ(child.resources.entries[NOFILE].limit.soft, 7);
    KEXPECT_EQ(child.resources.entries[NOFILE].limit.hard, 11);
    KEXPECT_EQ(child.resources.entries[NOFILE].current, 0);
    KEXPECT_EQ(parent.ref_count.load(), 1);
}

KTEST(Resource, DeniedOpenCannotCreateOrTruncate) {
    constexpr auto PATH = "/tmp/ktest_resource_denied_open";
    static_cast<void>(ker::vfs::vfs_mkdir("/tmp", 0755));
    static_cast<void>(ker::vfs::vfs_unlink(PATH));
    Task task{};
    KEXPECT_EQ(task.resources.set(NOFILE, {0, 256}, true, 256), 0);
    KEXPECT_EQ(ker::vfs::vfs_openat(&task, ker::vfs::AT_FDCWD, PATH, ker::vfs::O_CREAT | 1, 0600), -EMFILE);
    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), -ENOENT);
    auto* file = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0600);
    KREQUIRE_TRUE(file != nullptr);
    constexpr char CONTENT[] = "preserve";
    KEXPECT_EQ(ker::vfs::tmpfs::tmpfs_write(file, CONTENT, sizeof(CONTENT), 0), sizeof(CONTENT));
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
    KEXPECT_EQ(ker::vfs::vfs_openat(&task, ker::vfs::AT_FDCWD, PATH, ker::vfs::O_TRUNC | 1, 0600), -EMFILE);
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);
    KEXPECT_EQ(st.st_size, sizeof(CONTENT));
    KEXPECT_EQ(task.fd_table.size(), 0);
    KEXPECT_EQ(task.fd_table.reserved_count(), 0);
    KEXPECT_EQ(task.resources.entries[NOFILE].current, 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
}

KTEST(Resource, FileGrowthSnapshotAndDenialTelemetry) {
    constexpr unsigned FSIZE = static_cast<unsigned>(ker::abi::resource::Id::FSIZE);
    Task task{};
    KEXPECT_EQ(task.resources.set(FSIZE, {8, 16}, true, 256), 0);
    {
        ker::mod::sched::resource::FileGrowth growth(&task);
        KEXPECT_EQ(task.resources.set(FSIZE, {0, 16}, true, 256), 0);
        size_t count = 4;
        KEXPECT_TRUE(growth.bound_write(6, count));
        KEXPECT_EQ(count, 2);
        growth.committed(8);
    }
    KEXPECT_EQ(task.resources.entries[FSIZE].current, 8);
    KEXPECT_EQ(task.resources.entries[FSIZE].peak, 8);
    KEXPECT_EQ(task.resources.entries[FSIZE].denied, 1);
    KEXPECT_EQ(task.signal_pending_bits(), 0);
    {
        ker::mod::sched::resource::FileGrowth growth(&task);
        size_t count = 1;
        KEXPECT_FALSE(growth.bound_write(0, count));
        KEXPECT_EQ(count, 0);
    }
    KEXPECT_EQ(task.resources.entries[FSIZE].denied, 2);
    KEXPECT_EQ(task.signal_pending_bits(), uint64_t{1} << 24);
    KEXPECT_EQ(task.ref_count.load(), 1);
    task.signal_pending_store(0);
    {
        ker::mod::sched::resource::FileGrowth growth(&task);
        size_t count = 0;
        KEXPECT_TRUE(growth.bound_write(UINT64_MAX, count));
        KEXPECT_TRUE(growth.allow_resize(0));
        growth.committed(0);
    }
    KEXPECT_EQ(task.resources.entries[FSIZE].current, 0);
    KEXPECT_EQ(task.resources.entries[FSIZE].peak, 8);
    KEXPECT_EQ(task.resources.entries[FSIZE].denied, 2);
    KEXPECT_EQ(task.signal_pending_bits(), 0);
}

KTEST(Resource, CoreDumpAdmissionAndOwnerLifetime) { KEXPECT_TRUE(ker::mod::dbg::coredump::selftest_budget_admission()); }

KTEST(Resource, OwnerRetainsCanonicalIdentityWithoutPidSelection) {
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::TaskState;
    Task process{};
    process.pid = 424260;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const INITIAL_REFS = process.ref_count.load();
    {
        resource::Owner absent(nullptr);
        KEXPECT_TRUE(absent.get() == nullptr);
        Task thread{};
        thread.is_thread = true;
        thread.pid = process.pid + 1;
        // Numeric metadata alone cannot supply policy to an unbound thread.
        for (uint64_t owner_pid : {uint64_t{0}, thread.pid, process.pid}) {
            thread.owner_pid = owner_pid;
            resource::Owner unbound(&thread);
            KEXPECT_TRUE(unbound.get() == nullptr);
        }
        KREQUIRE_TRUE(resource::bind_thread_owner(thread, process));
        KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS + 1);
        // Both objects are private/unregistered. Path/PID bookkeeping cannot
        // replace the immutable process binding once that binding exists.
        for (uint64_t owner_pid : {uint64_t{0}, thread.pid, process.pid, process.pid + 2}) {
            thread.owner_pid = owner_pid;
            {
                resource::Owner owner(&thread);
                KEXPECT_TRUE(owner.get() == &process);
                KEXPECT_TRUE(owner.get() == thread.credential_owner());
                KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS + 2);
            }
            KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS + 1);
        }
        for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
            process.state.store(state);
            resource::Owner retired(&thread);
            KEXPECT_TRUE(retired.get() == nullptr);
            KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS + 1);
            // Existing nonthread borrow semantics also serve teardown callers.
            resource::Owner direct(&process);
            KEXPECT_TRUE(direct.get() == &process);
        }
        process.state.store(TaskState::ACTIVE);
    }
    KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS);
    KEXPECT_EQ(process.resource_thread_owners.load(), 0);
}

KTEST(Resource, UnpublishedThreadUsesCanonicalLimitsForInheritanceAndReservations) {
    namespace resource = ker::mod::sched::resource;
    constexpr unsigned FSIZE = static_cast<unsigned>(resource::Id::FSIZE);
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    constexpr uint32_t UID = 0xffffff70;
    Task process{};
    process.pid = 424270;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    KREQUIRE_EQ(resource::initialize_real_uid(process, UID), 0);
    KREQUIRE_EQ(process.resources.set(FSIZE, {8, 16}, true, 256), 0);
    KREQUIRE_EQ(process.resources.set(NOFILE, {1, 2}, true, 256), 0);
    KREQUIRE_EQ(process.resources.set(NPROC, {0, 2}, true, 256), 0);
    auto const INITIAL_REFS = process.ref_count.load();
    {
        Task thread{};
        thread.pid = process.pid + 1;
        thread.owner_pid = process.pid;
        thread.is_thread = true;
        thread.type = process.type;
        KREQUIRE_TRUE(resource::bind_thread_owner(thread, process));
        KREQUIRE_EQ(thread.resources.set(FSIZE, {1024, 2048}, true, 256), 0);
        Task child{};
        KREQUIRE_TRUE(resource::inherit(child, thread));
        KEXPECT_EQ(child.resources.entries[FSIZE].limit.soft, 8);
        KEXPECT_EQ(child.resources.entries[NOFILE].limit.soft, 1);
        Task grandchild{};
        grandchild.pid = process.pid + 2;
        grandchild.is_thread = true;
        grandchild.owner_pid = process.pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(grandchild, thread));
        KEXPECT_TRUE(grandchild.credential_owner() == &process);
        {
            ker::vfs::FdReservation first(&thread);
            KEXPECT_EQ(first.result(), 0);
            KEXPECT_EQ(process.fd_table.reserved_count(), 1);
            KEXPECT_EQ(thread.fd_table.reserved_count(), 0);
            ker::vfs::FdReservation second(&grandchild);
            KEXPECT_EQ(second.result(), -EMFILE);
            KEXPECT_EQ(process.fd_table.reserved_count(), 1);
        }
        KEXPECT_EQ(process.fd_table.reserved_count(), 0);
        {
            resource::ProcessReservation denied(&thread, UID);
            KEXPECT_EQ(denied.result(), -EAGAIN);
        }
        KREQUIRE_EQ(process.resources.set(NPROC, {1, 2}, true, 256), 0);
        {
            resource::ProcessReservation first(&thread, UID);
            KEXPECT_EQ(first.result(), 0);
            resource::ProcessReservation second(&grandchild, UID);
            KEXPECT_EQ(second.result(), -EAGAIN);
        }
        {
            resource::FileGrowth growth(&thread);
            size_t count = 4;
            KEXPECT_TRUE(growth.bound_write(6, count));
            KEXPECT_EQ(count, 2);
            growth.committed(8);
        }
        KEXPECT_EQ(process.resources.entries[FSIZE].current, 8);
        KEXPECT_EQ(process.resources.entries[FSIZE].denied, 1);
        KEXPECT_EQ(thread.resources.entries[FSIZE].current, 0);
        KEXPECT_EQ(thread.resources.entries[FSIZE].denied, 0);
        {
            resource::FileGrowth growth(&grandchild);
            KEXPECT_FALSE(growth.allow_resize(9));
        }
        KEXPECT_EQ(process.resources.entries[FSIZE].denied, 2);
        KEXPECT_TRUE((grandchild.signal_pending_bits() & (uint64_t{1} << 24)) != 0);
        Task unbound{};
        unbound.is_thread = true;
        ker::vfs::FdReservation missing_fd(&unbound);
        resource::ProcessReservation missing_process(&unbound, UID);
        resource::FileGrowth missing_growth(&unbound);
        KEXPECT_EQ(missing_fd.result(), -ESRCH);
        KEXPECT_EQ(missing_process.result(), -ESRCH);
        KEXPECT_FALSE(resource::inherit(child, unbound));
        KEXPECT_FALSE(missing_growth.allow_resize(1));
    }
    KEXPECT_EQ(process.ref_count.load(), INITIAL_REFS);
    KEXPECT_EQ(process.fd_table.reserved_count(), 0);
    KEXPECT_EQ(process.resource_thread_owners.load(), 0);
}

KTEST(Resource, StackAdmissionAndAllocationRollback) {
    namespace threading = ker::mod::sched::threading;
    namespace virt = ker::mod::mm::virt;
    constexpr unsigned STACK = static_cast<unsigned>(ker::abi::resource::Id::STACK);
    constexpr uint64_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
    constexpr uint64_t BASE = 0x18000000;
    constexpr uint64_t TOP = BASE + 16 * PAGE;
    auto* pagemap = virt::create_pagemap();
    KREQUIRE_NE(pagemap, nullptr);
    virt::reserve_page_range(pagemap, BASE, 16);
    Task task{};
    ker::mod::mm::numa::AllocationContext allocation_context{};
    KREQUIRE_EQ(ker::mod::mm::numa::capture_allocation_context({}, allocation_context), ker::mod::mm::numa::AllocationError::NONE);
    threading::Thread thread{};
    thread.stack_base_virt = BASE;
    thread.stack_size = TOP - BASE;
    thread.layout_base_virt = BASE - PAGE;
    thread.layout_size = 20 * PAGE;
    thread.stack_lowest_backed = TOP;
    task.pagemap = pagemap;
    task.thread = &thread;
    KEXPECT_EQ(task.resources.set(STACK, {2 * PAGE, UINT64_MAX}, true, 256), 0);
    KEXPECT_FALSE(threading::ensure_stack_backing(&thread, pagemap, TOP - 3 * PAGE, TOP, &task, allocation_context));
    KEXPECT_EQ(task.resources.entries[STACK].denied, 1);
    KEXPECT_EQ(task.resources.entries[STACK].current, 0);
    KEXPECT_EQ(thread.stack_lowest_backed, TOP);
    for (uint64_t addr = BASE; addr < TOP; addr += PAGE) {
        KEXPECT_EQ(virt::translate(pagemap, addr), virt::PADDR_INVALID);
    }
    // Deterministic second-page allocation failure must return the first page
    // without publishing any PTE or altering policy observations.
    KEXPECT_FALSE(threading::selftest_back_stack_pages(pagemap, TOP - 2 * PAGE, TOP, 1));
    KEXPECT_EQ(virt::translate(pagemap, TOP - 2 * PAGE), virt::PADDR_INVALID);
    KEXPECT_EQ(virt::translate(pagemap, TOP - PAGE), virt::PADDR_INVALID);
    KEXPECT_EQ(task.resources.entries[STACK].current, 0);
    KEXPECT_TRUE(threading::ensure_stack_backing(&thread, pagemap, TOP - 2 * PAGE, TOP, &task, allocation_context));
    KEXPECT_EQ(task.resources.entries[STACK].current, 2 * PAGE);
    auto const EXISTING = virt::translate(pagemap, TOP - 2 * PAGE);
    KEXPECT_NE(EXISTING, virt::PADDR_INVALID);
    KEXPECT_EQ(task.resources.set(STACK, {0, UINT64_MAX}, true, 256), 0);
    KEXPECT_TRUE(threading::ensure_stack_backing(&thread, pagemap, TOP - 2 * PAGE, TOP, &task, allocation_context));
    KEXPECT_EQ(virt::translate(pagemap, TOP - 2 * PAGE), EXISTING);
    KEXPECT_FALSE(threading::handle_lazy_stack_fault(&thread, pagemap, TOP - 3 * PAGE, TOP - 3 * PAGE, &task));
    KEXPECT_EQ(task.resources.set(STACK, {3 * PAGE, UINT64_MAX}, true, 256), 0);
    KEXPECT_TRUE(threading::handle_lazy_stack_fault(&thread, pagemap, TOP - 3 * PAGE, TOP - 3 * PAGE, &task));
    KEXPECT_EQ(task.resources.entries[STACK].current, 3 * PAGE);
    KEXPECT_EQ(task.resources.entries[STACK].peak, 3 * PAGE);
    KEXPECT_EQ(virt::translate(pagemap, TOP - 4 * PAGE), virt::PADDR_INVALID);
    thread.layout_base_virt = BASE - 2 * PAGE;
    KEXPECT_FALSE(threading::has_automatic_stack(&thread));
    KEXPECT_FALSE(threading::handle_lazy_stack_fault(&thread, pagemap, BASE, BASE, &task));
    task.thread = nullptr;
    task.pagemap = nullptr;
    virt::destroy_user_space(pagemap, 0, "resource_stack", "test-complete");
    virt::release_pagemap(pagemap);
}

KTEST(Resource, CpuThreadOwnerLifetimeAndSignalAccounting) {
    namespace resource = ker::mod::sched::resource;
    constexpr unsigned CPU = static_cast<unsigned>(ker::abi::resource::Id::CPU);
    Task process{};
    process.pid = 42;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    KEXPECT_EQ(process.resources.set(CPU, {1, 2}, true, 256), 0);
    {
        Task first{};
        Task second{};
        first.type = second.type = process.type;
        first.is_thread = second.is_thread = true;
        first.owner_pid = second.owner_pid = process.pid;
        KEXPECT_TRUE(resource::bind_thread_owner(first, process));
        KEXPECT_TRUE(resource::bind_thread_owner(second, process));
        KEXPECT_EQ(process.resource_thread_owners.load(), 2);
        KEXPECT_EQ(process.ref_count.load(), 3);
        KEXPECT_FALSE(resource::bind_thread_owner(first, process));
        KEXPECT_EQ(process.resource_thread_owners.load(), 2);
        KEXPECT_EQ(process.ref_count.load(), 3);
        resource::charge_cpu(&first, 600000, 500000);
        resource::charge_cpu(&second, 400000, 100000);
        KEXPECT_EQ(process.resources.cpu_time_us, 1000000);
        KEXPECT_EQ(process.resources.cpu_user_time_us, 600000);
        KEXPECT_EQ(process.resources.cpu_system_time_us, 400000);
        KEXPECT_EQ(process.resources.entries[CPU].current, 1);
        KEXPECT_EQ(first.signal_pending_bits(), 0);
        KEXPECT_EQ(second.pending_cpu_signals.load(), uint64_t{1} << 23);
        KEXPECT_EQ(second.signal_pending_bits(), 0);
        // Even after owner registry eligibility ends, the pointer stays alive
        // until both thread objects are reclaimed. No lookup is needed in IRQ.
        process.state.store(ker::mod::sched::task::TaskState::EXITING);
        resource::charge_cpu(&first, 1000000);
        KEXPECT_EQ(first.pending_cpu_signals.load(), uint64_t{1} << 8);
        KEXPECT_EQ(first.signal_pending_bits(), 0);
        KEXPECT_EQ(process.resources.entries[CPU].reason, ker::abi::resource::Denial::HARD_LIMIT);
    }
    KEXPECT_EQ(process.ref_count.load(), 1);
    KEXPECT_EQ(process.resource_thread_owners.load(), 0);
    Task unbound{};
    unbound.type = ker::mod::sched::task::TaskType::PROCESS;
    unbound.is_thread = true;
    resource::charge_cpu(&unbound, 1);
    KEXPECT_EQ(unbound.pending_cpu_signals.load(), uint64_t{1} << 8);
    Task daemon{};
    KEXPECT_EQ(daemon.resources.set(CPU, {0, 0}, true, 256), 0);
    resource::charge_cpu(&daemon, 1000000);
    KEXPECT_EQ(daemon.signal_pending_bits(), 0);
    KEXPECT_EQ(daemon.resources.cpu_time_us, 0);
}

KTEST(Resource, AddressSpaceCountsReservationsAndSparseRanges) {
    namespace virt = ker::mod::mm::virt;
    namespace paging = ker::mod::mm::paging;
    auto* root = virt::create_pagemap();
    KEXPECT_TRUE(root != nullptr);
    if (root == nullptr) return;
    uint64_t bytes = 0;
    constexpr uint64_t LIMIT = 0x0000800000000000ULL;
    KEXPECT_TRUE(virt::count_user_virtual_bytes(root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 0);
    constexpr uint64_t LOW = 0x200000;
    constexpr uint64_t HIGH = 0x700000000000;
    virt::reserve_page_range(root, LOW, 3);
    virt::reserve_page_range(root, HIGH, 2);
    KEXPECT_TRUE(virt::count_user_virtual_bytes(root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 5 * paging::PAGE_SIZE);
    KEXPECT_TRUE(virt::count_user_virtual_bytes(root, LOW + paging::PAGE_SIZE, HIGH + paging::PAGE_SIZE, bytes));
    KEXPECT_EQ(bytes, 3 * paging::PAGE_SIZE);
    virt::unmap_page(root, LOW + paging::PAGE_SIZE);
    KEXPECT_TRUE(virt::count_user_virtual_bytes(root, LOW, LOW + 3 * paging::PAGE_SIZE, bytes));
    KEXPECT_EQ(bytes, 2 * paging::PAGE_SIZE);
    KEXPECT_FALSE(virt::count_user_virtual_bytes(root, LOW + 1, HIGH, bytes));
    KEXPECT_FALSE(virt::count_user_virtual_bytes(root, HIGH, LOW, bytes));
    KEXPECT_FALSE(virt::count_user_virtual_bytes(root, 0, LIMIT + paging::PAGE_SIZE, bytes));
    virt::destroy_user_space(root, 0, "resource-as-test", "selftest");
    virt::release_pagemap(root);
}

KTEST(Resource, AddressSpaceUnionsLazyMetadataAndProtectedLeaves) {
    namespace virt = ker::mod::mm::virt;
    namespace paging = ker::mod::mm::paging;
    namespace resource = ker::mod::sched::resource;
    constexpr uint64_t PAGE = paging::PAGE_SIZE;
    constexpr uint64_t BASE = 0x200000;
    constexpr uint64_t LIMIT = 0x0000800000000000ULL;
    auto* root = virt::create_pagemap();
    KREQUIRE_NE(root, nullptr);
    Task task{};
    task.pagemap = root;
    // Unsorted, adjacent metadata ranges with PTE overlap and a sparse gap.
#ifdef WOS_SELFTEST
    ::ker::test::numa_lifecycle::mm_debt::begin_change();
    bool const LAZY_ROW_ADDED_0 = task.lazy_vmem_ranges.push_back({.start = BASE + 8 * PAGE, .end = BASE + 10 * PAGE});
    if (LAZY_ROW_ADDED_0) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
    ::ker::test::numa_lifecycle::mm_debt::end_change();
    KREQUIRE_TRUE(LAZY_ROW_ADDED_0);
#else
    KREQUIRE_TRUE(task.lazy_vmem_ranges.push_back({.start = BASE + 8 * PAGE, .end = BASE + 10 * PAGE}));
#endif
#ifdef WOS_SELFTEST
    ::ker::test::numa_lifecycle::mm_debt::begin_change();
    bool const LAZY_ROW_ADDED_1 = task.lazy_vmem_ranges.push_back({.start = BASE, .end = BASE + 2 * PAGE});
    if (LAZY_ROW_ADDED_1) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
    ::ker::test::numa_lifecycle::mm_debt::end_change();
    KREQUIRE_TRUE(LAZY_ROW_ADDED_1);
#else
    KREQUIRE_TRUE(task.lazy_vmem_ranges.push_back({.start = BASE, .end = BASE + 2 * PAGE}));
#endif
#ifdef WOS_SELFTEST
    ::ker::test::numa_lifecycle::mm_debt::begin_change();
    bool const LAZY_ROW_ADDED_2 = task.lazy_vmem_ranges.push_back({.start = BASE + 2 * PAGE, .end = BASE + 3 * PAGE});
    if (LAZY_ROW_ADDED_2) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
    ::ker::test::numa_lifecycle::mm_debt::end_change();
    KREQUIRE_TRUE(LAZY_ROW_ADDED_2);
#else
    KREQUIRE_TRUE(task.lazy_vmem_ranges.push_back({.start = BASE + 2 * PAGE, .end = BASE + 3 * PAGE}));
#endif
    virt::reserve_page_range(root, BASE + PAGE, 1);
    virt::reserve_page_range(root, BASE + 5 * PAGE, 1);
    uint64_t bytes = 0;
    KEXPECT_TRUE(resource::count_address_space(task, root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 6 * PAGE);
    KEXPECT_TRUE(resource::count_address_space(task, root, BASE + PAGE, BASE + 9 * PAGE, bytes));
    KEXPECT_EQ(bytes, 4 * PAGE);
    // A resident supervisor-only leaf is reversible PROT_NONE and still
    // occupies VA. Materializing an overlapping lazy page adds no charge.
    auto* page = ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::SELFTEST, PAGE, "resource-as");
    KREQUIRE_NE(page, nullptr);
    auto const PHYS = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    virt::map_page(root, BASE + PAGE, PHYS, paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_NX);
    KEXPECT_TRUE(resource::count_address_space(task, root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 6 * PAGE);
    ker::mod::sched::task::release_lazy_vmem_ranges(task);
    KEXPECT_TRUE(resource::count_address_space(task, root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 2 * PAGE);
    // A different, staged exec root must ignore the subject's old metadata.
#ifdef WOS_SELFTEST
    ::ker::test::numa_lifecycle::mm_debt::begin_change();
    bool const LAZY_ROW_ADDED_3 = task.lazy_vmem_ranges.push_back({.start = BASE, .end = BASE + 10 * PAGE});
    if (LAZY_ROW_ADDED_3) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
    ::ker::test::numa_lifecycle::mm_debt::end_change();
    KREQUIRE_TRUE(LAZY_ROW_ADDED_3);
#else
    KREQUIRE_TRUE(task.lazy_vmem_ranges.push_back({.start = BASE, .end = BASE + 10 * PAGE}));
#endif
    task.pagemap = nullptr;
    KEXPECT_TRUE(resource::count_address_space(task, root, 0, LIMIT, bytes));
    KEXPECT_EQ(bytes, 2 * PAGE);
    ker::mod::sched::task::release_lazy_vmem_ranges(task);
    virt::destroy_user_space(root, 0, "resource-as-union", "selftest");
    virt::release_pagemap(root);
}

KTEST(Resource, ProcessReservationLifecycleAndFatalCancellation) {
    namespace resource = ker::mod::sched::resource;
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    constexpr uint32_t UID = 0xfffffff0;
    Task parent{};
    KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(parent, UID), 0);
    resource::ProcessReservation seed(nullptr, UID);
    KREQUIRE_TRUE(seed.attach(parent));
    KEXPECT_EQ(parent.resources.entries[NPROC].current, 1);
    KEXPECT_EQ(parent.resources.set(NPROC, {2, UINT64_MAX}, true, 256), 0);
    {
        resource::ProcessReservation first(&parent, UID);
        resource::ProcessReservation denied(&parent, UID);
        KEXPECT_EQ(first.result(), 0);
        KEXPECT_EQ(denied.result(), -EAGAIN);
        KEXPECT_EQ(parent.resources.entries[NPROC].current, 2);
        KEXPECT_EQ(parent.resources.entries[NPROC].denied, 1);
        KEXPECT_EQ(parent.resources.set(NPROC, {0, UINT64_MAX}, true, 256), 0);
        Task child{};
        KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(child, UID), 0);
        KREQUIRE_TRUE(first.attach(child));
        KEXPECT_FALSE(first.attach(child));
        KEXPECT_EQ(child.resources.entries[NPROC].current, 2);
        child.process_charge.release();
        child.process_charge.release();
        KEXPECT_EQ(parent.resources.entries[NPROC].current, 1);
        KEXPECT_EQ(parent.resources.entries[NPROC].peak, 2);
    }
    KEXPECT_EQ(parent.resources.set(NPROC, {3, UINT64_MAX}, true, 256), 0);
    resource::ProcessReservation first(&parent, UID);
    resource::ProcessReservation second(&parent, UID);
    KEXPECT_EQ(parent.resources.entries[NPROC].current, 3);
    resource::cancel_process_reservations(parent);
    KEXPECT_EQ(first.result(), -ECANCELED);
    KEXPECT_EQ(second.result(), -ECANCELED);
    KEXPECT_NULL(parent.pending_process_reservations);
    KEXPECT_EQ(parent.resources.entries[NPROC].current, 1);
    KEXPECT_EQ(parent.resources.entries[NPROC].peak, 3);
    KEXPECT_EQ(parent.ref_count.load(), 1);
}

KTEST(Resource, ProcessChargeUnpublishedDestructionAndUidTransfer) {
    namespace resource = ker::mod::sched::resource;
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    constexpr uint32_t SOURCE = 0xfffffff0;
    constexpr uint32_t DESTINATION = 0xfffffff1;
    Task parent{};
    KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(parent, SOURCE), 0);
    Task peer{};
    KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(peer, DESTINATION), 0);
    resource::ProcessReservation seed(nullptr, SOURCE);
    resource::ProcessReservation peer_seed(nullptr, DESTINATION);
    KREQUIRE_TRUE(seed.attach(parent));
    KREQUIRE_TRUE(peer_seed.attach(peer));
    {
        Task unpublished{};
        KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(unpublished, SOURCE), 0);
        resource::ProcessReservation pending(&parent, SOURCE);
        KREQUIRE_TRUE(pending.attach(unpublished));
        KEXPECT_EQ(parent.resources.entries[NPROC].current, 2);
    }
    KEXPECT_EQ(parent.resources.entries[NPROC].current, 1);
    KEXPECT_EQ(parent.resources.set(NPROC, {1, UINT64_MAX}, true, 256), 0);
    KEXPECT_EQ(resource::change_credentials(parent, {.change = ker::mod::security::Change::SET_RESUID, .real = DESTINATION}), -EAGAIN);
    KEXPECT_EQ(parent.credentials().user().real, SOURCE);
    KEXPECT_EQ(parent.process_charge.uid, SOURCE);
    KEXPECT_EQ(peer.resources.entries[NPROC].current, 1);
    KEXPECT_EQ(parent.resources.entries[NPROC].denied, 1);
    KEXPECT_EQ(parent.resources.set(NPROC, {2, UINT64_MAX}, true, 256), 0);
    KEXPECT_EQ(resource::change_credentials(parent, {.change = ker::mod::security::Change::SET_RESUID, .real = DESTINATION}), 0);
    KEXPECT_EQ(parent.credentials().user().real, DESTINATION);
    KEXPECT_EQ(peer.resources.entries[NPROC].current, 2);
    KEXPECT_EQ(parent.resources.set(NPROC, {0, UINT64_MAX}, true, 256), 0);
    KEXPECT_EQ(resource::change_credentials(parent, {.change = ker::mod::security::Change::SET_RESUID, .real = DESTINATION}), 0);
    parent.process_charge.release();
    KEXPECT_EQ(peer.resources.entries[NPROC].current, 1);
}

KTEST(ResourceCpuObservation, TimeSyscallUnitsAndThreadScope) { KEXPECT_TRUE(ker::syscall::time::selftest_cpu_observation()); }

KTEST(ResourceCpuObservation, ProcfsRetainsLifetimeWithoutRegistry) { KEXPECT_TRUE(ker::vfs::procfs::selftest_cpu_observation()); }

KTEST(ResourceCpuObservation, RetiredSiblingAndImportedHistory) {
    namespace resource = ker::mod::sched::resource;
    namespace children = ker::syscall::process::child_events;
    Task process{};
    process.pid = 0x7B43;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    // Simulate the resource State imported before remote image construction.
    resource::State imported;
    static_cast<void>(imported.charge_cpu(100, 70));
    process.resources = imported;
    process.user_time_us = 1;
    process.system_time_us = 2;
    children::add_reaped_cpu_times(process, 17, 19);
    resource::CpuObservation observed{};
    {
        Task thread{};
        thread.type = process.type;
        thread.is_thread = true;
        thread.owner_pid = process.pid;
        KEXPECT_TRUE(resource::bind_thread_owner(thread, process));
        static_cast<void>(thread.resources.charge_cpu(999));
        resource::charge_cpu(&thread, 200, 150);
        // Owner need not remain eligible for PID lookup.
        process.state.store(ker::mod::sched::task::TaskState::EXITING);
        KEXPECT_TRUE(resource::copy_cpu_observation(thread, observed));
        KEXPECT_EQ(observed.user_time_us, 220);
        KEXPECT_EQ(observed.system_time_us, 80);
        KEXPECT_EQ(observed.child_user_time_us, 17);
        KEXPECT_EQ(observed.child_system_time_us, 19);
    }
    KEXPECT_EQ(process.resource_thread_owners.load(), 0);
    KEXPECT_TRUE(resource::copy_cpu_observation(process, observed));
    KEXPECT_EQ(observed.user_time_us, 220);
    KEXPECT_EQ(observed.system_time_us, 80);
    children::add_reaped_cpu_times(process, UINT64_MAX, UINT64_MAX);
    KEXPECT_TRUE(resource::copy_cpu_observation(process, observed));
    KEXPECT_EQ(observed.child_user_time_us, UINT64_MAX);
    KEXPECT_EQ(observed.child_system_time_us, UINT64_MAX);
    Task unbound{};
    unbound.type = process.type;
    unbound.is_thread = true;
    KEXPECT_FALSE(resource::copy_cpu_observation(unbound, observed));
    KEXPECT_EQ(observed.user_time_us, 220);
    KEXPECT_EQ(observed.child_system_time_us, UINT64_MAX);
}

KTEST(ResourceCpuObservation, MainRetirementClosesOnlyIdleAdmission) {
    namespace resource = ker::mod::sched::resource;
    Task process{};
    process.pid = 0x7B52;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    Task worker{};
    worker.type = process.type;
    worker.is_thread = true;
    worker.owner_pid = process.pid;
    KEXPECT_TRUE(resource::bind_thread_owner(worker, process));
    KEXPECT_FALSE(resource::close_idle_thread_cpu_admission(process));
    KEXPECT_FALSE(process.resource_cpu_admission_closed);
    worker.resource_owner.finish_cpu();
    KEXPECT_EQ(process.resource_thread_owners.load(), 1);
    KEXPECT_TRUE(resource::close_idle_thread_cpu_admission(process));
    KEXPECT_TRUE(process.resource_cpu_admission_closed);
    Task late{};
    late.type = process.type;
    late.is_thread = true;
    late.owner_pid = process.pid;
    KEXPECT_FALSE(resource::bind_thread_owner(late, process));
    KEXPECT_EQ(process.resource_cpu_threads.load(), 0);
}

KTEST(ResourceExecAdmission, QuiescenceCountsOnlySafeLiveCpuContributorsAndRollsBack) {
    namespace resource = ker::mod::sched::resource;
    Task process{};
    process.pid = 0x7B54;
    Task worker{}, observer{};
    worker.is_thread = true;
    observer.is_thread = true;
    worker.owner_pid = process.pid;
    observer.owner_pid = process.pid;
    KEXPECT_TRUE(resource::bind_thread_owner(worker, process));
    KEXPECT_TRUE(resource::bind_thread_owner(observer, process));
    resource::ExecAdmission admission;
    KEXPECT_TRUE(admission.begin(process));
    KEXPECT_FALSE(admission.quiesce(worker, 0));
    {
        resource::ExecSafeWait waiting_worker(worker);
        KEXPECT_TRUE(waiting_worker.active());
        KEXPECT_FALSE(admission.quiesce(process, 0));
        auto const FIRST = process.resource_exec_quiescence_epoch.load();
        KEXPECT_NE(FIRST, 0);
        KEXPECT_TRUE(observer.has_interrupting_signal_pending());
        KEXPECT_EQ(process.resource_exec_quiescent_threads, 1);
        {
            resource::ExecSafeWait waiting_observer(observer);
            resource::ExecSafeWait duplicate(observer);
            KEXPECT_TRUE(waiting_observer.active());
            KEXPECT_FALSE(duplicate.active());
            KEXPECT_TRUE(admission.quiesced(process));
            KEXPECT_EQ(process.resource_exec_quiescent_threads, 2);
            // A previous wait may have observed rollback, but a new exec can
            // start before it withdraws. It must acknowledge the new epoch.
            admission.finish();
            KEXPECT_TRUE(admission.begin(process));
            KEXPECT_TRUE(admission.quiesce(process, 0));
            KEXPECT_FALSE(waiting_observer.finish_if_released());
            KEXPECT_TRUE(waiting_observer.active());
            KEXPECT_EQ(process.resource_exec_quiescent_threads, 2);
            admission.finish();
            KEXPECT_TRUE(waiting_observer.finish_if_released());
            KEXPECT_FALSE(waiting_observer.active());
            KEXPECT_EQ(process.resource_exec_quiescent_threads, 1);
        }
        KEXPECT_FALSE(admission.quiesced(process));
        admission.finish();
        KEXPECT_FALSE(observer.exec_quiescence_requested());
        KEXPECT_FALSE(admission.quiesced(process));
        KEXPECT_TRUE(admission.begin(process));
        KEXPECT_FALSE(admission.quiesce(process, 0));
        KEXPECT_TRUE(process.resource_exec_quiescence_epoch.load() > FIRST);
        admission.finish();
    }
    KEXPECT_EQ(process.resource_exec_quiescent_threads, 0);
    observer.resource_owner.finish_cpu();
    resource::ExecSafeWait retired_observer(observer);
    KEXPECT_FALSE(retired_observer.active());
    KEXPECT_EQ(process.resource_thread_owners.load(), 2);
    KEXPECT_EQ(process.resource_cpu_threads.load(), 1);
    {
        resource::ExecSafeWait waiting_worker(worker);
        KEXPECT_TRUE(admission.begin(process));
        KEXPECT_TRUE(admission.quiesce(process, 0));
        KEXPECT_TRUE(admission.quiesced(process));
        admission.finish();
    }
    KEXPECT_EQ(process.resource_exec_quiescent_threads, 0);
    KEXPECT_FALSE(process.resource_exec_admission_closed);
}

KTEST(ResourceExecAdmission, ExcludesNewThreadsAndCompetingCommitsWithoutReopeningExit) {
    namespace resource = ker::mod::sched::resource;
    Task process{};
    process.pid = 0x7B53;
    Task worker{};
    worker.is_thread = true;
    worker.owner_pid = process.pid;
    resource::ExecAdmission unbound;
    KEXPECT_FALSE(unbound.begin(worker));
    KEXPECT_TRUE(resource::bind_thread_owner(worker, process));
    {
        resource::ExecAdmission first;
        KEXPECT_TRUE(first.begin(worker));
        KEXPECT_FALSE(first.begin(process));
        resource::ExecAdmission competing;
        KEXPECT_FALSE(competing.begin(process));
        Task late{};
        late.is_thread = true;
        late.owner_pid = process.pid;
        KEXPECT_FALSE(resource::bind_thread_owner(late, process));
        KEXPECT_EQ(process.resource_cpu_threads.load(), 1);
        worker.resource_owner.finish_cpu();
        KEXPECT_EQ(process.resource_cpu_threads.load(), 0);
        KEXPECT_EQ(process.resource_thread_owners.load(), 1);
    }
    KEXPECT_FALSE(process.resource_exec_admission_closed);
    {
        Task fresh{};
        fresh.is_thread = true;
        fresh.owner_pid = process.pid;
        KEXPECT_TRUE(resource::bind_thread_owner(fresh, process));
    }
    resource::ExecAdmission final;
    KEXPECT_TRUE(final.begin(process));
    KEXPECT_TRUE(resource::close_thread_cpu_admission(process));
    final.finish();
    final.finish();
    KEXPECT_TRUE(process.resource_cpu_admission_closed);
    KEXPECT_FALSE(process.resource_exec_admission_closed);
    KEXPECT_FALSE(final.begin(process));
    Task denied{};
    denied.is_thread = true;
    denied.owner_pid = process.pid;
    KEXPECT_FALSE(resource::bind_thread_owner(denied, process));
}

KTEST(ResourceCpuObservation, ExitContributorsRetireBeforeMetadata) {
    namespace resource = ker::mod::sched::resource;
    Task process{};
    process.pid = 0x7B51;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    {
        Task first{};
        Task second{};
        first.is_thread = second.is_thread = true;
        first.type = second.type = process.type;
        first.owner_pid = second.owner_pid = process.pid;
        KEXPECT_TRUE(resource::bind_thread_owner(first, process));
        KEXPECT_TRUE(resource::bind_thread_owner(second, process));
        KEXPECT_EQ(process.resource_cpu_threads.load(), 2);
        KEXPECT_TRUE(resource::close_thread_cpu_admission(first));
        Task late{};
        late.is_thread = true;
        late.type = process.type;
        late.owner_pid = process.pid;
        KEXPECT_FALSE(resource::bind_thread_owner(late, process));
        KEXPECT_EQ(process.ref_count.load(), 3);
        first.resource_owner.finish_cpu();
        first.resource_owner.finish_cpu();
        KEXPECT_EQ(process.resource_cpu_threads.load(), 1);
        KEXPECT_EQ(process.resource_thread_owners.load(), 2);
        // The second contribution unwinds through its destructor, as with
        // failed publication. The first must not decrement a second time.
    }
    KEXPECT_EQ(process.resource_cpu_threads.load(), 0);
    KEXPECT_EQ(process.resource_thread_owners.load(), 0);
    KEXPECT_EQ(process.ref_count.load(), 1);
}

KTEST(ResourceIdentity, RemoteAdvisoryGenerationUsesRetainedOwnerAndRejectsPidReuse) {
    namespace resource = ker::mod::sched::resource;
    Task first{};
    first.pid = 0x7B54;
    first.type = ker::mod::sched::task::TaskType::PROCESS;
    Task reused{};
    reused.pid = first.pid;
    reused.type = first.type;
    KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(first, false), uint64_t{0});
    uint64_t const GENERATION = ker::vfs::vfs_advisory_process_generation(first);
    KREQUIRE_NE(GENERATION, uint64_t{0});
    KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(first), GENERATION);
    KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(first, false), GENERATION);
    KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(reused, false), uint64_t{0});
    uint64_t const REUSED = ker::vfs::vfs_advisory_process_generation(reused);
    KEXPECT_NE(REUSED, uint64_t{0});
    KEXPECT_NE(REUSED, GENERATION);
    {
        Task thread{};
        thread.pid = first.pid + 1;
        thread.type = first.type;
        thread.is_thread = true;
        thread.owner_pid = first.pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(thread, first));
        KEXPECT_EQ(thread.advisory_process_generation.load(), uint64_t{0});
        KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(thread, false), GENERATION);
        KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(thread), GENERATION);
        // Its owner remains authoritative after normal PID lookup admission closes.
        first.state.store(ker::mod::sched::task::TaskState::EXITING);
        KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(thread), GENERATION);
        KEXPECT_EQ(first.resource_thread_owners.load(), uint32_t{1});
    }
    KEXPECT_EQ(first.resource_thread_owners.load(), uint32_t{0});
    Task unbound{};
    unbound.is_thread = true;
    unbound.owner_pid = reused.pid;
    KEXPECT_EQ(ker::vfs::vfs_advisory_process_generation(unbound), uint64_t{0});
    KEXPECT_EQ(unbound.advisory_process_generation.load(), uint64_t{0});
}
