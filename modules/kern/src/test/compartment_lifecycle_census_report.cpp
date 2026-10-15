#include <test/compartment_lifecycle_census_report.hpp>

#ifdef WOS_SELFTEST
#include <cstdint>
#include <limits>
#include <platform/dbg/dbg.hpp>
#include <test/compartment_lifecycle_census.hpp>

namespace ker::test::lifecycle60 {
namespace {
using log = mod::dbg::logger<"lifecycle-census">;
auto number(uint64_t value) noexcept -> unsigned long long { return static_cast<unsigned long long>(value); }
auto bit(bool value) noexcept -> unsigned { return static_cast<unsigned>(value); }
}  // namespace

void publish_census_report() noexcept {
    auto const& report = census_gate_report();
    auto const& owner = census_owner_report();
    auto const& scalar = report.after;
    auto const& executor = scalar.executor;
    auto const& storage = owner.storage_after;
    bool const OWNER_PAIR_VALID = report.owner_pair_stable && report.scalar_pair_stable && complete(owner);
    uint64_t dead = 0;
    bool dead_overflow = false;
    for (auto const& cpu : scalar.cpu) {
        if (cpu.dead > std::numeric_limits<uint64_t>::max() - dead) {
            dead_overflow = true;
            dead = std::numeric_limits<uint64_t>::max();
        } else if (!dead_overflow)
            dead += cpu.dead;
    }
    uint64_t present = 0, refs_one = 0, multiplicity_one = 0, cloexec = 0;
    for (auto const& slot : owner.descriptors.slots) {
        if (slot.file_identity == 0) continue;
        ++present;
        if (slot.file_references == 1) ++refs_one;
        if (slot.multiplicity == 1) ++multiplicity_one;
        if (slot.cloexec) ++cloexec;
    }
    log::info(
        "census-meta boundary=%u index=%llu cookie=%llu result=%u reason=%u calls=%llu accepted=%llu waits=%llu refused=%llu pair=%u/%u "
        "baselines=%u/%u/%u owner-pair-valid=%u",
        static_cast<unsigned>(report.boundary), number(report.index), number(report.cookie), static_cast<unsigned>(report.result),
        static_cast<unsigned>(report.reason), number(report.calls), number(report.accepts), number(report.waits), number(report.refuses),
        bit(report.owner_pair_stable), bit(report.scalar_pair_stable), bit(report.host_baseline_valid),
        bit(report.provisional_driver_valid), bit(report.driver_baseline_valid), bit(OWNER_PAIR_VALID));
    log::info(
        "census-actors pid=%llu refs-A/B=%llu/%llu baseline-refs-host/driver=%llu/%llu thread-owners/cpu=%llu/%llu shares=%u registry=%llu "
        "scoped=%llu threads=%llu ns-pid/mount/ipc=%llu/%llu/%llu",
        number(scalar.actor_pid), number(report.before.actor_references), number(scalar.actor_references),
        number(report.host_baseline.actor_references), number(report.driver_baseline.actor_references), number(scalar.actor_thread_owners),
        number(scalar.actor_cpu_threads), bit(scalar.actor_shares_root), number(scalar.active_registry), number(scalar.scoped_pid_registry),
        number(scalar.active_threads), number(scalar.pid_namespaces), number(scalar.mount_namespaces), number(scalar.ipc_namespaces));
    log::info(
        "census-providers dead=%llu overflow=%u deferred-depth/queued/completed=%llu/%llu/%llu native=%u compartment=%llu executor-open=%u "
        "outstanding/reserved/ready/running=%u/%u/%u/%u DONE=%u observers=%llu close/protocol-errors=%llu/%llu",
        number(dead), bit(dead_overflow), number(scalar.deferred_depth), number(scalar.deferred_queued), number(scalar.deferred_completed),
        bit(scalar.native_retirements_pending), number(scalar.compartment_pending),
        bit(executor.initialized && executor.available && executor.admission_open && !executor.stop_requested && !executor.loop_finished),
        static_cast<unsigned>(executor.outstanding), static_cast<unsigned>(executor.reserved), static_cast<unsigned>(executor.ready),
        static_cast<unsigned>(executor.running), static_cast<unsigned>(executor.completed_storage_slots), number(executor.observer_handles),
        number(executor.close_errors), number(executor.protocol_errors));
    log::info(
        "census-storage valid=%u sequence-begin/end=%llu/%llu task-live/alloc=%llu/%llu file-live/alloc=%llu/%llu "
        "family-live/alloc=%llu/%llu binding-live/alloc=%llu/%llu",
        bit(storage.valid), number(owner.storage_before.sequence), number(storage.sequence), number(storage.task.live_blocks),
        number(storage.task.allocating), number(storage.file.live_blocks), number(storage.file.allocating),
        number(storage.family.live_blocks), number(storage.family.allocating), number(storage.binding.live_blocks),
        number(storage.binding.allocating));
    log::info(
        "census-mount-root-ipc mounts-live/retiring/private/init-private=%llu/%llu/%llu/%llu prep-storage/ops=%llu/%llu "
        "refs/open/native/storage=%llu/%llu/%llu/%llu roots/control/index=%llu/%llu/%llu "
        "ipc-private-segments/pages/publishing=%llu/%llu/%llu",
        number(owner.mounts.published), number(owner.mounts.retiring), number(owner.mounts.published_private),
        number(owner.mounts.initializing_private), number(owner.mounts.preparation_storage), number(owner.mounts.preparing_operations),
        number(owner.mounts.refs), number(owner.mounts.open_files), number(owner.mounts.native_backing_pins),
        number(owner.mounts.mount_storage), number(owner.roots.roots.active_roots), number(owner.roots.roots.control_pages),
        number(owner.roots.indexed_bindings), number(owner.ipc.private_segments), number(owner.ipc.private_pages),
        number(owner.ipc.publishing_attachments));
    log::info(
        "census-owners remote-objects/refs/members/reports/create=%llu/%llu/%llu/%llu/%llu "
        "population-reserved/attached/uid100=%llu/%llu/%llu service-members/reports=%llu/%llu "
        "child-links/events/waits/claims=%llu/%llu/%llu/%llu",
        number(owner.services.remote_objects), number(owner.services.remote_references), number(owner.services.remote_members),
        number(owner.services.remote_reports), number(owner.services.remote_creations), number(owner.population.reservations),
        number(owner.population.attached_charges), number(owner.population.uid100_attached), number(owner.services.local_members),
        number(owner.services.local_reports), number(owner.children.child_links), number(owner.children.queued_events),
        number(owner.children.wait_registrations), number(owner.children.event_claims));
    log::info(
        "census-fd installed/reserved/distinct=%llu/%llu/%llu slot-present/ref1/multiplicity1/cloexec=%llu/%llu/%llu/%llu "
        "executor-pending/recovery=%llu/%llu subject-children/tracees/events/claims/waits=%llu/%llu/%llu/%llu/%llu",
        number(owner.descriptors.installed), number(owner.descriptors.reserved), number(owner.descriptors.distinct_files), number(present),
        number(refs_one), number(multiplicity_one), number(cloexec), number(owner.descriptors.executor_pending),
        number(owner.descriptors.recovery_presence), number(owner.driver_children.children), number(owner.driver_children.tracees),
        number(owner.driver_children.events), number(owner.driver_children.claimed_events), number(owner.driver_children.waiters));
    log::info(
        "census-allocator-cache-telemetry live-bytes/pages=%llu/%llu free-pages=%llu total-allocated/freed-pages=%llu/%llu "
        "alloc/free-ops=%llu/%llu not-object-debt",
        number(scalar.physical.live_allocated_bytes), number(scalar.physical.live_allocated_pages),
        number(scalar.physical.current_free_pages), number(scalar.physical.total_allocated_pages),
        number(scalar.physical.total_freed_pages), number(scalar.physical.alloc_count), number(scalar.physical.free_count));
}
}  // namespace ker::test::lifecycle60
#endif
