#pragma once

#ifdef WOS_SELFTEST
#include <platform/mm/lifecycle_census.hpp>
#include <platform/mm/lifecycle_storage.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/resource_process.hpp>
#include <platform/sched/service_resource.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <test/compartment_lifecycle_runtime.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/mount.hpp>

namespace ker::test::lifecycle60 {

// Fixed controller storage: allocate baseline/A/B before the measured baseline,
// not three large FD256 arrays on a small kernel stack. Contains no owner refs.
struct OwnerCensus {
    mod::mm::lifecycle_storage::Snapshot storage_before{}, storage_after{};
    vfs::SelftestMountCensus mounts{};
    mod::sched::resource::SelftestServiceCensus services{};
    mod::sched::resource::SelftestPopulationCensus population{};
    syscall::process::child_events::SelftestLifecycleCensus children{};
    syscall::process::child_events::SelftestOwnerLifecycleCensus driver_children{};
    syscall::shm::SelftestShmCensus ipc{};
    mod::mm::virt::SelftestRootCensus roots{};
    vfs::SelftestFdCensus descriptors{};
    uint64_t executor_cookie{};
    bool complete{};
};

[[nodiscard]] inline auto storage_quiescent(const mod::mm::lifecycle_storage::Snapshot& value) noexcept -> bool {
    return value.valid && value.task.allocating == 0 && value.file.allocating == 0 && value.family.allocating == 0 &&
           value.binding.allocating == 0;
}

[[nodiscard]] inline auto same_storage_debt(const mod::mm::lifecycle_storage::Snapshot& a,
                                            const mod::mm::lifecycle_storage::Snapshot& b) noexcept -> bool {
    return storage_quiescent(a) && storage_quiescent(b) && a.task.live_blocks == b.task.live_blocks &&
           a.file.live_blocks == b.file.live_blocks && a.family.live_blocks == b.family.live_blocks &&
           a.binding.live_blocks == b.binding.live_blocks;
}

[[nodiscard]] inline auto complete(const OwnerCensus& value) noexcept -> bool {
    using mod::mm::lifecycle_census::quiescent;
    return value.complete && storage_quiescent(value.storage_before) && storage_quiescent(value.storage_after) &&
           value.storage_before.sequence == value.storage_after.sequence && same_storage_debt(value.storage_before, value.storage_after) &&
           value.mounts.complete && quiescent(value.mounts.stamp) && value.services.complete && quiescent(value.services.stamp) &&
           value.population.complete && quiescent(value.population.stamp) && value.children.complete && quiescent(value.children.stamp) &&
           value.driver_children.complete && quiescent(value.driver_children.stamp) &&
           value.children.stamp == value.driver_children.stamp && value.ipc.complete && quiescent(value.ipc.stamp) &&
           value.roots.complete && quiescent(value.roots.stamp) && value.descriptors.complete &&
           value.descriptors.executor_cookie == value.executor_cookie && value.population.executor_cookie == value.executor_cookie;
}

// No I/O, usercopy, callback, yield, allocation or object-owner retain. The lease
// remains live around BOTH complete calls. Never substitute Task::ref_count.
[[nodiscard]] inline auto collect_into(const CensusLease& lease, OwnerCensus& out) noexcept -> bool {
    out.complete = false;
    auto* driver = lease.subject();
    if (!lease || driver == nullptr) return false;
    out.executor_cookie = lease.cookie();
    out.storage_before = mod::mm::lifecycle_storage::snapshot();
    out.mounts = vfs::selftest_mount_census();
    out.services = mod::sched::resource::selftest_service_census();
    out.population = mod::sched::resource::selftest_population_census(lease);
    out.children = syscall::process::child_events::selftest_lifecycle_census();
    out.driver_children = syscall::process::child_events::selftest_owner_lifecycle_census(*driver);
    out.ipc = syscall::shm::selftest_shm_census();
    out.roots = mod::mm::virt::selftest_root_census();
    out.descriptors = vfs::selftest_fd_census_parked(lease);
    out.storage_after = mod::mm::lifecycle_storage::snapshot();
    out.complete = true;
    if (!complete(out)) {
        out.complete = false;
        return false;
    }
    return true;
}

// This is only a covered-owner bracket. Controller acceptance additionally
// requires the real attempt/deletion debts and scheduler, namespace, physical,
// native-retirement and actual callback RETURNED/executor drains. The complete
// controller gate remains undefined until connected; this function grants no ACK.
[[nodiscard]] inline auto unchanged_complete(const OwnerCensus& a, const OwnerCensus& b) noexcept -> bool {
    using mod::mm::lifecycle_census::unchanged;
    if (!complete(a) || !complete(b) || a.executor_cookie != b.executor_cookie || a.storage_before.sequence != b.storage_after.sequence ||
        !same_storage_debt(a.storage_before, b.storage_after))
        return false;
    return unchanged(a.mounts.stamp, b.mounts.stamp) && unchanged(a.services.stamp, b.services.stamp) &&
           unchanged(a.population.stamp, b.population.stamp) && unchanged(a.children.stamp, b.children.stamp) &&
           unchanged(a.driver_children.stamp, b.driver_children.stamp) && unchanged(a.ipc.stamp, b.ipc.stamp) &&
           unchanged(a.roots.stamp, b.roots.stamp) && a.mounts == b.mounts && a.services == b.services && a.population == b.population &&
           a.children == b.children && a.driver_children == b.driver_children && a.ipc == b.ipc &&
           a.roots.roots.active_roots == b.roots.roots.active_roots && a.roots.roots.control_pages == b.roots.roots.control_pages &&
           a.roots.roots.last_serial == b.roots.roots.last_serial && a.roots.indexed_bindings == b.roots.indexed_bindings &&
           a.descriptors == b.descriptors;
}

// Baseline comparison intentionally discards versions/cookies/generations.
// Every baseline/current operand must FIRST pass its own full stable bracket.
[[nodiscard]] inline auto same_baseline_debt(const OwnerCensus& a, const OwnerCensus& b) noexcept -> bool {
    if (!complete(a) || !complete(b) || !same_storage_debt(a.storage_after, b.storage_after)) return false;
    auto mounts_a = a.mounts, mounts_b = b.mounts;
    mounts_a.stamp = mounts_b.stamp = {};
    mounts_a.generation = mounts_b.generation = 0;
    mounts_a.pivot_epoch = mounts_b.pivot_epoch = 0;
    auto services_a = a.services, services_b = b.services;
    services_a.stamp = services_b.stamp = {};
    auto population_a = a.population, population_b = b.population;
    population_a.stamp = population_b.stamp = {};
    population_a.executor_cookie = population_b.executor_cookie = 0;
    auto children_a = a.children, children_b = b.children;
    children_a.stamp = children_b.stamp = {};
    auto driver_a = a.driver_children, driver_b = b.driver_children;
    driver_a.stamp = driver_b.stamp = {};
    auto ipc_a = a.ipc, ipc_b = b.ipc;
    ipc_a.stamp = ipc_b.stamp = {};
    if (mounts_a != mounts_b || services_a != services_b || population_a != population_b || children_a != children_b ||
        driver_a != driver_b || ipc_a != ipc_b || a.roots.roots.active_roots != b.roots.roots.active_roots ||
        a.roots.roots.control_pages != b.roots.roots.control_pages || a.roots.indexed_bindings != b.roots.indexed_bindings ||
        a.descriptors.installed != b.descriptors.installed || a.descriptors.reserved != b.descriptors.reserved ||
        a.descriptors.distinct_files != b.descriptors.distinct_files || a.descriptors.executor_pending != b.descriptors.executor_pending ||
        a.descriptors.recovery_presence != b.descriptors.recovery_presence)
        return false;
    for (size_t i = 0; i < a.descriptors.slots.size(); ++i)
        if (a.descriptors.slots[i] != b.descriptors.slots[i]) return false;
    return true;
}

// These are the mechanically represented private and transient debts. They do
// NOT cover unenumerated WKI/signal queues or claim a complete kernel census.
[[nodiscard]] inline auto no_private_or_transient_debt(const OwnerCensus& value) noexcept -> bool {
    return complete(value) && value.mounts.initializing_private == 0 && value.mounts.published_private == 0 &&
           value.mounts.retiring_private == 0 && value.mounts.preparation_storage == 0 && value.mounts.preparing_operations == 0 &&
           value.services.remote_creations == 0 && value.population.reservations == 0 && value.population.executor_pending == 0 &&
           value.ipc.private_segments == 0 && value.ipc.private_pages == 0 && value.ipc.publishing_attachments == 0 &&
           value.driver_children.children == 0 && value.driver_children.tracees == 0 && value.driver_children.events == 0 &&
           value.driver_children.claimed_events == 0 && value.driver_children.waiters == 0 &&
           !value.driver_children.own_wait_registration && !value.driver_children.own_claim && value.descriptors.reserved == 0 &&
           value.descriptors.executor_pending == 0 && value.descriptors.recovery_presence == 0;
}

}  // namespace ker::test::lifecycle60
#endif
