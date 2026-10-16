#ifdef WOS_SELFTEST
#include <atomic>
#include <cerrno>
#include <cstring>
#include <limits>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/numa_balance.hpp>
#include <platform/sched/compartment_lifecycle.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/compartment_lifecycle_census.hpp>
#include <test/numa_lifecycle_census.hpp>
#include <type_traits>
#include <vfs/buffer_cache.hpp>
#include <vfs/mount_namespace.hpp>

namespace ker::test::numa_lifecycle {
namespace sched = mod::sched;
namespace virt = mod::mm::virt;
namespace balance = mod::mm::numa::balance;
namespace lifetime = mod::mm::file_mapping;
namespace usercopy = mod::sys::usercopy;
using Task = sched::task::Task;
using lifecycle60::CensusScalars;
using lifecycle60::OwnerCensus;
inline constexpr size_t CENSUS_CPUS = lifecycle60::CENSUS_CPUS;

// This fast check never acquires a regular root-owner lock inside a census.
// Exact registered root serial and credentials are checked outside both owners.
auto CurrentCensusScope::executing_here() const noexcept -> bool {
    return actor_ != nullptr && cookie_ != 0 && root_incarnation_ != 0 && sched::can_query_current_task() &&
           sched::get_current_task() == actor_ && actor_->type == sched::task::TaskType::PROCESS && !actor_->is_thread &&
           actor_->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE && actor_->syscall_account_start_us != 0 &&
           !actor_->scheduler_transition_active.load(std::memory_order_acquire) && sched::preempt_count() == 0;
}
namespace {
constinit std::atomic<bool> boot_enabled{};
constexpr uint64_t DEADLINE_US = 900'000'000;
constexpr uint64_t SESSION_BUSY = 1, SESSION_POISONED = 2, SESSION_PREPARED = 4, SESSION_BASELINE = 8, SESSION_ENDED = 16;
struct Session {
    uint64_t actor{}, pid{}, root_serial{}, cookie{}, nonce{}, deadline{}, calls{}, waits{}, phase{}, flags{};
};
static_assert(sizeof(Session) == 80);
mod::sys::Spinlock session_lock;
Session session{};
struct PlainStorage {
    OwnerCensus owner_a{}, owner_b{}, owner_baseline{};
    CensusScalars scalar_a{}, scalar_b{}, scalar_baseline{};
    NativeDebtSnapshot native_a{}, native_b{};
    RegistrySnapshot registry_a{}, registry_b{}, registry_baseline{};
    MmDebtSnapshot mm_a{}, mm_b{}, mm_baseline{};
    balance::Snapshot physical_a{}, physical_b{}, physical_baseline{};
    balance::Workspace physical_work{};
    Report report{};
};
static_assert(std::is_trivially_copyable_v<PlainStorage>);
static_assert(sizeof(PlainStorage) <= 2 * 1024 * 1024);
PlainStorage plain{};

// Instrumented representation writes BEFORE provider locks and before BEGIN.
// No atomic, session, lock, counter, epoch or live provider object is reset.
void prefault_plain() noexcept {
    auto* bytes = reinterpret_cast<volatile unsigned char*>(&plain);
    for (size_t i = 0; i < sizeof(plain); ++i) bytes[i] = 0;
}

auto failure(int error) noexcept -> uint64_t { return static_cast<uint64_t>(-error); }

auto admitted(Task& actor, uint64_t& root_serial) noexcept -> bool {
    if (!boot_enabled.load(std::memory_order_acquire) || !sched::can_query_current_task() || sched::get_current_task() != &actor ||
        actor.pid == 0 || actor.type != sched::task::TaskType::PROCESS || actor.is_thread || actor.credential_owner() != &actor ||
        actor.state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE || actor.syscall_account_start_us == 0 ||
        sched::preempt_count() != 0 || !sched::interrupts_enabled() || actor.pagemap == nullptr ||
        actor.scheduler_transition_active.load(std::memory_order_acquire) || actor.deferred_task_switch || actor.wants_block ||
        actor.shares_user_pagemap.load(std::memory_order_acquire) || actor.resource_thread_owners.load(std::memory_order_acquire) != 0 ||
        actor.resource_cpu_threads.load(std::memory_order_acquire) != 0 || actor.wki_proxy_task ||
        actor.pending_wki_fd_snapshots != nullptr || actor.pending_wki_submissions != nullptr ||
        actor.pending_wki_submit_buffers != nullptr || actor.pending_wki_filesystems != nullptr ||
        actor.pending_signal_targets != nullptr || actor.owned_unpublished_process.load(std::memory_order_acquire) != nullptr ||
        actor.unpublished_teardown_in_progress.load(std::memory_order_acquire) ||
        actor.owned_exec_preparation.load(std::memory_order_acquire) != nullptr ||
        actor.owned_exec_arguments.load(std::memory_order_acquire) != nullptr ||
        actor.exec_preparation_teardown_in_progress.load(std::memory_order_acquire) ||
        actor.pending_exec_handoff.load(std::memory_order_acquire) != nullptr ||
        actor.active_exec_handoff.load(std::memory_order_acquire) != nullptr ||
        actor.native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr ||
        actor.exit_in_progress.load(std::memory_order_acquire) || actor.process_exit_requested.load(std::memory_order_acquire))
        return false;
    auto const credentials = actor.credentials();
    auto const U = credentials.user(), G = credentials.group();
    if (U.real != 0 || U.effective != 0 || U.saved != 0 || G.real != 0 || G.effective != 0 || G.saved != 0 ||
        !credentials.allows(mod::security::Authority::KERNEL_DEBUG))
        return false;
    auto const flags = actor.resource_lock.lock_irqsave();
    bool const resources = !actor.resource_cpu_admission_closed && !actor.resource_exec_admission_closed &&
                           actor.resource_exec_quiescence_epoch.load(std::memory_order_acquire) == 0 &&
                           actor.resource_exec_quiescent_threads == 0 && !actor.resource_exec_safe_wait;
    actor.resource_lock.unlock_irqrestore(flags);
    if (!resources || mod::smt::get_core_count() != CENSUS_CPUS || sched::selftest_task_context_ready_mask() != 0xff) return false;
    root_serial = 0;
    return virt::capture_user_root_incarnation(actor.pagemap, root_serial) && root_serial != 0;
}

auto collect_owners(const CurrentCensusScope& scope, OwnerCensus& out) noexcept -> bool {
    out.complete = false;
    if (!scope.executing_here()) return false;
    out.executor_cookie = scope.cookie();
    out.storage_before = mod::mm::lifecycle_storage::snapshot();
    out.mounts = vfs::selftest_mount_census();
    out.services = sched::resource::selftest_service_census();
    sched::resource::selftest_population_census_current(scope, out.population);
    out.children = syscall::process::child_events::selftest_lifecycle_census();
    out.driver_children = syscall::process::child_events::selftest_owner_lifecycle_census(scope.actor());
    out.ipc = syscall::shm::selftest_shm_census();
    out.roots = virt::selftest_root_census();
    vfs::selftest_fd_census_current(scope, out.descriptors);
    out.storage_after = mod::mm::lifecycle_storage::snapshot();
    out.complete = true;
    if (!lifecycle60::complete(out)) {
        out.complete = false;
        return false;
    }
    return true;
}

void collect_scalars(const CurrentCensusScope& scope, CensusScalars& out) noexcept {
    auto& actor = scope.actor();
    out.actor_pid = actor.pid;
    out.actor_references = actor.ref_count.load(std::memory_order_acquire);
    out.actor_thread_owners = actor.resource_thread_owners.load(std::memory_order_acquire);
    out.actor_cpu_threads = actor.resource_cpu_threads.load(std::memory_order_acquire);
    out.actor_shares_root = actor.shares_user_pagemap.load(std::memory_order_acquire);
    out.core_count = mod::smt::get_core_count();
    out.active_registry = sched::get_active_task_count();
    out.scoped_pid_registry = sched::get_scoped_pid_task_count();
    out.active_threads = sched::threading::get_active_thread_count();
    out.pid_namespaces = sched::pid_ns::live_namespace_count();
    out.mount_namespaces = vfs::mount_ns::live_namespace_count();
    out.ipc_namespaces = sched::ipc::live_namespace_count();
    auto const compartment = sched::compartment::status();
    out.compartment_pending = compartment.pending;
    out.compartment_completed = compartment.completed;
    out.compartment_exit_requests = compartment.exit_requests;
    // Avoid another regular native OwnerLock acquisition: the new protected
    // observer snapshots below already supply this exact outstanding scalar.
    // The old controller and its pending() reader remain unchanged.
    out.native_retirements_pending = false;
    auto const trace = sched::get_scheduler_trace_stats(0);
    out.deferred_queued = trace.gc_deferred_queued;
    out.deferred_completed = trace.gc_deferred_completed;
    out.deferred_depth = trace.gc_deferred_depth;
    // Constant bound; no linked-list walk when a dead list is empty. One
    // explicit prefix sample corroborates zero, not the total dead population.
    for (size_t cpu = 0; cpu < CENSUS_CPUS; ++cpu) {
        auto& row = out.cpu[cpu];
        row = {};
        if (cpu >= out.core_count) continue;
        auto const queue = sched::get_run_queue_stats(cpu);
        row.runnable_heap = queue.active_task_count;
        row.dead = queue.expired_task_count;
        row.waiting = queue.wait_queue_count;
        row.timers = queue.timer_queue_depth;
        uint64_t pid = 0;
        uint32_t references = 0;
        row.sampled_dead = sched::get_expired_task_refcounts(cpu, &pid, &references, 1);
        row.first_dead_pid = pid;
        row.first_dead_references = references;
    }
    out.executor = lifetime::LifetimeRetirementExecutor::snapshot();
    mod::mm::phys::get_alloc_stats_snapshot(out.physical);
}

auto same_executor(const lifetime::LifetimeRetirementSnapshot& a, const lifetime::LifetimeRetirementSnapshot& b) noexcept -> bool {
    return a.initialized == b.initialized && a.available == b.available && a.admission_open == b.admission_open &&
           a.stop_requested == b.stop_requested && a.loop_finished == b.loop_finished && a.outstanding == b.outstanding &&
           a.reserved == b.reserved && a.ready == b.ready && a.running == b.running && a.completed == b.completed &&
           a.cancelled == b.cancelled && a.close_errors == b.close_errors && a.protocol_errors == b.protocol_errors &&
           a.first_close_result == b.first_close_result && a.completed_storage_slots == b.completed_storage_slots &&
           a.observer_handles == b.observer_handles && a.exhausted_slots == b.exhausted_slots;
}

auto same_scalars(const CensusScalars& a, const CensusScalars& b) noexcept -> bool {
    // Run/wait/timer population is descriptive, not a total active-Task count.
    // It may fluctuate across the two samples; dead prefixes must agree and
    // be zero below. Physical pool activity is also deliberately descriptive.
    for (size_t i = 0; i < CENSUS_CPUS; ++i)
        if (a.cpu[i].dead != b.cpu[i].dead || a.cpu[i].sampled_dead != b.cpu[i].sampled_dead ||
            a.cpu[i].first_dead_pid != b.cpu[i].first_dead_pid || a.cpu[i].first_dead_references != b.cpu[i].first_dead_references)
            return false;
    return a.actor_pid == b.actor_pid && a.actor_references == b.actor_references && a.actor_thread_owners == b.actor_thread_owners &&
           a.actor_cpu_threads == b.actor_cpu_threads && a.actor_shares_root == b.actor_shares_root && a.core_count == b.core_count &&
           a.active_registry == b.active_registry && a.scoped_pid_registry == b.scoped_pid_registry &&
           a.active_threads == b.active_threads && a.pid_namespaces == b.pid_namespaces && a.mount_namespaces == b.mount_namespaces &&
           a.ipc_namespaces == b.ipc_namespaces && a.compartment_pending == b.compartment_pending &&
           a.compartment_completed == b.compartment_completed && a.compartment_exit_requests == b.compartment_exit_requests &&
           a.native_retirements_pending == b.native_retirements_pending && a.deferred_queued == b.deferred_queued &&
           a.deferred_completed == b.deferred_completed && a.deferred_depth == b.deferred_depth && same_executor(a.executor, b.executor);
}

auto no_shared_private_debt(const OwnerCensus& value) noexcept -> bool {
    return complete(value) && value.mounts.initializing_private == 0 && value.mounts.published_private == 0 &&
           value.mounts.retiring_private == 0 && value.mounts.preparation_storage == 0 && value.mounts.preparing_operations == 0 &&
           value.services.remote_creations == 0 && value.population.reservations == 0 && value.population.executor_pending == 0 &&
           value.ipc.private_segments == 0 && value.ipc.private_pages == 0 && value.ipc.publishing_attachments == 0 &&
           value.descriptors.reserved == 0 && value.descriptors.executor_pending == 0 && value.descriptors.recovery_presence == 0;
}

auto no_driver_child_debt(const OwnerCensus& value) noexcept -> bool {
    auto const& child = value.driver_children;
    // A driver has its legitimate parent link throughout; do not zero that.
    return child.children == 0 && child.publishing_children == 0 && child.live_children == 0 && child.zombie_children == 0 &&
           child.tracees == 0 && child.events == 0 && child.claimed_events == 0 && child.waiters == 0 && child.notified_waiters == 0 &&
           !child.tracer_link && !child.own_wait_registration && !child.own_claim;
}

auto providers_settled(const CensusScalars& value) noexcept -> bool {
    if (value.core_count != CENSUS_CPUS || value.pid_namespaces != 0 || value.mount_namespaces != 0 || value.ipc_namespaces != 0 ||
        value.compartment_pending != 0 || value.native_retirements_pending || value.deferred_depth != 0 ||
        value.deferred_queued != value.deferred_completed)
        return false;
    for (auto const& cpu : value.cpu)
        if (cpu.dead != 0 || cpu.sampled_dead != 0) return false;
    return true;
}

auto same_scalar_baseline(const CensusScalars& a, const CensusScalars& b) noexcept -> bool {
    // Both operands independently passed their complete drained pair. Do not
    // compare monotonic operation totals, run/wait placement, or physical pools.
    return a.actor_pid == b.actor_pid && a.actor_references == b.actor_references && a.actor_thread_owners == b.actor_thread_owners &&
           a.actor_cpu_threads == b.actor_cpu_threads && a.actor_shares_root == b.actor_shares_root && a.core_count == b.core_count &&
           a.active_registry == b.active_registry && a.scoped_pid_registry == b.scoped_pid_registry && a.active_threads == b.active_threads;
}

auto native_settled(const NativeDebtSnapshot& value) noexcept -> bool {
    return value.flags == (NATIVE_COMPLETE | NATIVE_ARITHMETIC_VALID) && value.constructions == 0 && value.queue_outstanding == 0 &&
           value.return_tails == 0 && mod::mm::lifecycle_census::quiescent(value.stamp);
}
auto registry_complete(const RegistrySnapshot& value) noexcept -> bool {
    return value.flags == REGISTRY_COMPLETE && value.row_count <= REGISTRY_ROW_LIMIT && value.legacy_rows <= value.row_count &&
           value.native_rows == value.row_count - value.legacy_rows && mod::mm::lifecycle_census::quiescent(value.stamp);
}
auto same_registry_rows(const RegistrySnapshot& a, const RegistrySnapshot& b) noexcept -> bool {
    if (!registry_complete(a) || !registry_complete(b) || a.legacy_rows != b.legacy_rows || a.native_rows != b.native_rows ||
        a.row_count != b.row_count)
        return false;
    for (size_t i = 0; i < a.row_count; ++i)
        if (a.rows[i] != b.rows[i]) return false;
    return true;
}
auto mm_complete(const MmDebtSnapshot& value) noexcept -> bool {
    return value.valid == 1 && value.inflight == 0 && value.version_begin == value.version_end;
}
auto same_mm_debt(const MmDebtSnapshot& a, const MmDebtSnapshot& b) noexcept -> bool {
    // All23 contiguous uint64 fields, no padding or pointer reinterpretation.
    return mm_complete(a) && mm_complete(b) &&
           std::memcmp(reinterpret_cast<const unsigned char*>(&a) + 32, reinterpret_cast<const unsigned char*>(&b) + 32, sizeof(a) - 32) ==
               0;
}
auto mm_settled(const MmDebtSnapshot& m, const RegistrySnapshot& registry) noexcept -> bool {
    return mm_complete(m) && m.access_readers == 0 && m.access_exclusives == 0 && m.user_page_pins == 0 && m.stable_handles == 0 &&
           m.retained_frame_refs == 0 && m.task_usercopy_accesses == 0 && m.pin_returns == 0 && m.lazy_returns == 0 && m.swap_queued == 0 &&
           m.swap_pageins == 0 && m.swap_pageouts == 0 && m.swap_waiters == 0 && m.swap_returns == 0 && m.registry_returns == 0 &&
           m.legacy_file_registry_rows == registry.legacy_rows && m.native_file_registry_rows == registry.native_rows;
}
auto same_physical_counts(const balance::Counts& a, const balance::Counts& b) noexcept -> bool {
    if (a.managed_pages != b.managed_pages || a.free_pages != b.free_pages || a.cached_free_pages != b.cached_free_pages ||
        a.allocator_metadata_pages != b.allocator_metadata_pages || a.zone_descriptor_pages != b.zone_descriptor_pages ||
        a.owner_pages != b.owner_pages || a.kind_pages != b.kind_pages)
        return false;
    for (size_t i = 0; i < OWNER_COUNT; ++i)
        if (a.owners[i].pages != b.owners[i].pages || a.owners[i].objects != b.owners[i].objects) return false;
    return true;
}
auto same_physical(const balance::Snapshot& a, const balance::Snapshot& b) noexcept -> bool {
    if (!a.complete || !b.complete || !a.result.valid() || !b.result.valid() || a.provenance != b.provenance || a.node_count == 0 ||
        a.node_count > MAX_NODES || a.node_count != b.node_count || a.zone_count != b.zone_count || a.scanned_frames != b.scanned_frames ||
        !same_physical_counts(a.scanned, b.scanned))
        return false;
    auto const& x = a.global;
    auto const& y = b.global;
    if (x.managed_pages != y.managed_pages || x.free_pages != y.free_pages || x.allocator_metadata_pages != y.allocator_metadata_pages ||
        x.zone_descriptor_pages != y.zone_descriptor_pages || x.per_cpu_page_cache_reserve_pages != y.per_cpu_page_cache_reserve_pages ||
        x.per_cpu_page_cache_reserve_objects != y.per_cpu_page_cache_reserve_objects || x.owner_pages != y.owner_pages ||
        x.identity_pages != y.identity_pages || x.identity_mismatch_pages != 0 || y.identity_mismatch_pages != 0 ||
        x.untracked_unreclaimable_pages != 0 || y.untracked_unreclaimable_pages != 0 || x.zone_count != y.zone_count)
        return false;
    for (size_t i = 0; i < OWNER_COUNT; ++i)
        if (x.owners[i].pages != y.owners[i].pages || x.owners[i].objects != y.owners[i].objects) return false;
    for (size_t i = 0; i < mod::mm::phys::PHYSICAL_RESERVE_KIND_COUNT; ++i)
        if (x.firmware_reserves[i].pages != y.firmware_reserves[i].pages ||
            x.firmware_reserves[i].objects != y.firmware_reserves[i].objects)
            return false;
    for (size_t i = 0; i < a.node_count; ++i)
        if (!same_physical_counts(a.nodes[i], b.nodes[i])) return false;
    return true;
}
auto providers_ready(const CensusScalars& s, const OwnerCensus& owners) noexcept -> bool {
    auto const& e = s.executor;
    return providers_settled(s) && s.actor_references != 0 && s.actor_references != UINT32_MAX && s.actor_thread_owners == 0 &&
           s.actor_cpu_threads == 0 && !s.actor_shares_root && e.initialized && e.available && e.admission_open && !e.stop_requested &&
           !e.loop_finished && e.close_errors == 0 && e.protocol_errors == 0 && e.first_close_result == 0 && e.exhausted_slots == 0 &&
           e.outstanding == 0 && e.reserved == 0 && e.ready == 0 && e.running == 0 && e.completed_storage_slots == 0 &&
           e.observer_handles == 0 && no_shared_private_debt(owners) && no_driver_child_debt(owners);
}
void physical_row(PhysicalRow& out, uint64_t node, const balance::Counts& in) noexcept {
    out.node = node;
    out.managed_pages = in.managed_pages;
    out.metadata_pages = in.allocator_metadata_pages;
    out.descriptor_pages = in.zone_descriptor_pages;
    out.free_inclusive = in.free_pages;
    out.cached_free = in.cached_free_pages;
    for (size_t i = 0; i < OWNER_COUNT; ++i) out.owners[i] = {in.owners[i].pages, in.owners[i].objects};
}
void export_values() noexcept {
    auto& out = plain.report;
    auto const& o = plain.owner_b;
    auto const& s = plain.scalar_b;
    out.mm = plain.mm_b;
    auto& t = out.totals;
    t.task_blocks = o.storage_after.task.live_blocks;
    t.file_blocks = o.storage_after.file.live_blocks;
    t.family_blocks = o.storage_after.family.live_blocks;
    t.binding_blocks = o.storage_after.binding.live_blocks;
    t.active_roots = o.roots.roots.active_roots;
    t.root_control_pages = o.roots.roots.control_pages;
    t.indexed_bindings = o.roots.indexed_bindings;
    t.active_tasks = s.active_registry;
    t.scoped_pids = s.scoped_pid_registry;
    t.active_threads = s.active_threads;
    t.pid_namespaces = s.pid_namespaces;
    t.mount_namespaces = s.mount_namespaces;
    t.ipc_namespaces = s.ipc_namespaces;
    t.mount_preparations = o.mounts.preparation_storage;
    t.service_members = o.services.local_members + o.services.remote_members;
    t.unfinished_service_reports = o.services.unfinished_local_reports;
    t.population_attached = o.population.attached_charges;
    t.child_links = o.children.child_links;
    t.tracee_links = o.children.tracee_links;
    t.child_events = o.children.queued_events;
    t.wait_registrations = o.children.wait_registrations;
    t.event_claims = o.children.event_claims;
    t.installed_fds = o.descriptors.installed;
    t.distinct_files = o.descriptors.distinct_files;
    t.reserved_fds = o.descriptors.reserved;
    t.pending_fds = o.descriptors.executor_pending;
    t.recovery_presence = o.descriptors.recovery_presence;
    t.deferred_gc_depth = s.deferred_depth;
    t.retirement_outstanding = s.executor.outstanding;
    t.retirement_done_storage = s.executor.completed_storage_slots;
    t.deferred_vmap_pages = virt::kernel_vmap_pending_free_pages();
    if (plain.physical_b.complete && plain.physical_b.result.valid() && plain.physical_b.node_count <= MAX_NODES) {
        out.header.node_count = plain.physical_b.node_count;
        physical_row(out.physical[0], UINT64_MAX, plain.physical_b.scanned);
        for (size_t i = 0; i < plain.physical_b.node_count; ++i) physical_row(out.physical[i + 1], i, plain.physical_b.nodes[i]);
    }
}
void request_progress() noexcept {
    // No scope/owner/pin/session lock survives here. Do not close providers.
    sched::request_gc();
    (void)virt::drain_kernel_vmap_frees_bounded(128);
}
}  // namespace

class CensusHandler final {
   public:
    static auto run(Task& actor, const Request& request) noexcept -> uint64_t {
        uint64_t serial = 0;
        if (!admitted(actor, serial)) return failure(EPERM);
        auto const now = mod::time::get_us();
        Session local{};
        auto flags = session_lock.lock_irqsave();
        if ((session.flags & SESSION_BUSY) != 0) {
            session_lock.unlock_irqrestore(flags);
            return failure(EBUSY);
        }
        if (session.cookie == 0) {
            if (request.phase != static_cast<uint32_t>(Phase::PREPARE) || request.cookie != 0 || now > UINT64_MAX - DEADLINE_US) {
                session_lock.unlock_irqrestore(flags);
                return failure(EINVAL);
            }
            session = {reinterpret_cast<uint64_t>(&actor), actor.pid, serial, 1, request.nonce, now + DEADLINE_US, 0, 0, 0, 0};
        } else if (session.actor != reinterpret_cast<uint64_t>(&actor) || session.pid != actor.pid) {
            session_lock.unlock_irqrestore(flags);
            return failure(EPERM);
        }
        if (session.calls == UINT64_MAX)
            session.flags |= SESSION_POISONED;
        else
            ++session.calls;
        session.flags |= SESSION_BUSY;
        local = session;
        session_lock.unlock_irqrestore(flags);

        // First PREPARE addressability setup is outside every observation.
        if (local.calls == 1) prefault_plain();
        auto& h = plain.report.header;
        // Only report storage is reset now; baseline storage is never reset.
        std::memset(&plain.report, 0, sizeof(plain.report));
        h.version = VERSION;
        h.bytes = sizeof(Report);
        h.phase = request.phase;
        h.cookie = local.cookie;
        h.nonce = local.nonce;
        h.required_coverage = KERNEL_COVERAGE_REQUIRED;
        h.capture_us = now;
        h.absolute_deadline_us = local.deadline;
        h.calls = local.calls;
        h.waits = local.waits;
        h.root_incarnation = serial;
        Outcome result = Outcome::WAIT;
        Reason reason = Reason::NONE;
        bool collected = false;
        if ((local.flags & SESSION_POISONED) != 0) {
            result = Outcome::REFUSE;
            reason = Reason::COUNTER_INVALID;
        } else if (serial != local.root_serial) {
            result = Outcome::REFUSE;
            reason = Reason::ROOT_CHANGED;
        } else if (request.nonce != local.nonce || (request.cookie != local.cookie && !(local.calls == 1 && request.cookie == 0))) {
            result = Outcome::REFUSE;
            reason = Reason::COOKIE;
        } else if (now >= local.deadline) {
            result = Outcome::REFUSE;
            reason = Reason::DEADLINE;
        } else if ((local.flags & SESSION_ENDED) != 0 ||
                   (request.phase == static_cast<uint32_t>(Phase::PREPARE) && (local.flags & SESSION_BASELINE) != 0) ||
                   (request.phase == static_cast<uint32_t>(Phase::BEGIN) &&
                    ((local.flags & SESSION_PREPARED) == 0 || (local.flags & SESSION_BASELINE) != 0)) ||
                   (request.phase == static_cast<uint32_t>(Phase::END) && (local.flags & SESSION_BASELINE) == 0)) {
            result = Outcome::REFUSE;
            reason = Reason::COOKIE;
        } else {
            auto const ntp = lifecycle60::ntp_boot_settlement();
            auto const writeback = vfs::selftest_prepare_dirty_writeback_worker();
            if (ntp == lifecycle60::GateResult::REFUSE || writeback == vfs::SelftestWritebackPreparation::FAILED) {
                result = Outcome::REFUSE;
                reason = Reason::PROVIDER_DEBT;
            } else if (ntp != lifecycle60::GateResult::ACCEPT || writeback != vfs::SelftestWritebackPreparation::READY) {
                reason = Reason::PROVIDER_DEBT;
            } else {
                collected = true;
                result = collect(actor, local, reason);
            }
        }
        if (mod::time::get_us() >= local.deadline && result != Outcome::REFUSE) {
            result = Outcome::REFUSE;
            reason = Reason::DEADLINE;
        }
        if (result == Outcome::ACCEPT && request.phase == static_cast<uint32_t>(Phase::BEGIN)) {
            plain.owner_baseline = plain.owner_b;
            plain.scalar_baseline = plain.scalar_b;
            plain.mm_baseline = plain.mm_b;
            plain.registry_baseline = plain.registry_b;
            plain.physical_baseline = plain.physical_b;
        }
        if (result == Outcome::ACCEPT && request.phase == static_cast<uint32_t>(Phase::END) &&
            (!lifecycle60::same_baseline_debt(plain.owner_baseline, plain.owner_b) ||
             !same_scalar_baseline(plain.scalar_baseline, plain.scalar_b) || !same_mm_debt(plain.mm_baseline, plain.mm_b) ||
             !same_registry_rows(plain.registry_baseline, plain.registry_b) || !same_physical(plain.physical_baseline, plain.physical_b))) {
            result = Outcome::WAIT;
            reason = Reason::BASELINE_DEBT;
        }
        if (mod::time::get_us() >= local.deadline && result != Outcome::REFUSE) {
            result = Outcome::REFUSE;
            reason = Reason::DEADLINE;
        }
        if (collected) export_values();
        h.outcome = static_cast<uint64_t>(result);
        h.reason = static_cast<uint64_t>(reason);
        if (result == Outcome::ACCEPT) h.coverage = KERNEL_COVERAGE_REQUIRED;
        if (result == Outcome::WAIT) {
            if (local.waits == UINT64_MAX) {
                result = Outcome::REFUSE;
                h.outcome = static_cast<uint64_t>(result);
                h.reason = static_cast<uint64_t>(Reason::COUNTER_INVALID);
            } else
                h.waits = ++local.waits;
        }
        // Even WAIT exports a full record. Never fault in/COW output transport.
        bool const exported = usercopy::copy_to_task_mapped(actor, request.output_address, &plain.report, sizeof(plain.report));
        uint64_t after_serial = 0;
        bool const identity_held = admitted(actor, after_serial) && after_serial == local.root_serial;
        flags = session_lock.lock_irqsave();
        session.waits = local.waits;
        session.phase = request.phase;
        session.flags &= ~SESSION_BUSY;
        if (!exported || !identity_held || result == Outcome::REFUSE)
            session.flags |= SESSION_POISONED;
        else if (result == Outcome::ACCEPT) {
            if (request.phase == static_cast<uint32_t>(Phase::PREPARE)) session.flags |= SESSION_PREPARED;
            if (request.phase == static_cast<uint32_t>(Phase::BEGIN)) session.flags |= SESSION_BASELINE;
            if (request.phase == static_cast<uint32_t>(Phase::END)) session.flags |= SESSION_ENDED;
        }
        session_lock.unlock_irqrestore(flags);
        if (result == Outcome::WAIT && exported && identity_held) request_progress();
        if (!identity_held) return failure(EPERM);
        return exported ? 0 : failure(EFAULT);
    }

   private:
    static auto collect(Task& actor, const Session& s, Reason& reason) noexcept -> Outcome {
        // No retained roots, locks, frame pins or usercopy scopes. The typed
        // permission authorizes only this actual current accounted executor.
        CurrentCensusScope scope{actor, s.cookie, s.root_serial};
        bool const owner_a = collect_owners(scope, plain.owner_a);
        collect_scalars(scope, plain.scalar_a);
        sched::task::selftest_native_debt_snapshot(plain.native_a);
        syscall::vmem::selftest_file_registry_snapshot(plain.registry_a);
        mm_debt::snapshot(plain.mm_a);
        auto const physical_a = balance::snapshot(plain.physical_a, plain.physical_work, MAX_FRAMES);
        auto const physical_b = balance::snapshot(plain.physical_b, plain.physical_work, MAX_FRAMES);
        mm_debt::snapshot(plain.mm_b);
        syscall::vmem::selftest_file_registry_snapshot(plain.registry_b);
        sched::task::selftest_native_debt_snapshot(plain.native_b);
        collect_scalars(scope, plain.scalar_b);
        bool const owner_b = collect_owners(scope, plain.owner_b);
        plain.scalar_a.native_retirements_pending = plain.native_a.queue_outstanding != 0;
        plain.scalar_b.native_retirements_pending = plain.native_b.queue_outstanding != 0;
        uint64_t serial = 0;
        if (!scope.executing_here() || !admitted(actor, serial) || serial != s.root_serial) {
            reason = Reason::ROOT_CHANGED;
            return Outcome::REFUSE;
        }
        if (!plain.owner_a.storage_before.valid || !plain.owner_a.storage_after.valid || !plain.owner_b.storage_before.valid ||
            !plain.owner_b.storage_after.valid || (plain.native_a.flags & NATIVE_ARITHMETIC_VALID) == 0 ||
            (plain.native_b.flags & NATIVE_ARITHMETIC_VALID) == 0 ||
            (plain.registry_a.flags & (REGISTRY_OVERFLOW | REGISTRY_INVALID)) != 0 ||
            (plain.registry_b.flags & (REGISTRY_OVERFLOW | REGISTRY_INVALID)) != 0) {
            reason = Reason::COUNTER_INVALID;
            return Outcome::REFUSE;
        }
        if (!physical_a.valid() || !physical_b.valid()) {
            reason = Reason::PHYSICAL;
            return Outcome::REFUSE;
        }
        if (!owner_a || !owner_b || !lifecycle60::unchanged_complete(plain.owner_a, plain.owner_b)) {
            reason = Reason::OWNER_CHURN;
            return Outcome::WAIT;
        }
        if (!same_scalars(plain.scalar_a, plain.scalar_b) || !native_settled(plain.native_a) || !native_settled(plain.native_b) ||
            !mod::mm::lifecycle_census::unchanged(plain.native_a.stamp, plain.native_b.stamp) ||
            !same_registry_rows(plain.registry_a, plain.registry_b) ||
            !mod::mm::lifecycle_census::unchanged(plain.registry_a.stamp, plain.registry_b.stamp) ||
            !same_mm_debt(plain.mm_a, plain.mm_b) || plain.mm_a.version_begin != plain.mm_b.version_end) {
            reason = Reason::MM_CHURN;
            return Outcome::WAIT;
        }
        if (!providers_ready(plain.scalar_a, plain.owner_a) || !providers_ready(plain.scalar_b, plain.owner_b) ||
            !mm_settled(plain.mm_a, plain.registry_a) || !mm_settled(plain.mm_b, plain.registry_b) ||
            virt::kernel_vmap_pending_free_pages() != 0) {
            reason = Reason::PROVIDER_DEBT;
            return Outcome::WAIT;
        }
        if (!same_physical(plain.physical_a, plain.physical_b)) {
            reason = Reason::PHYSICAL;
            return Outcome::WAIT;
        }
        reason = Reason::NONE;
        return Outcome::ACCEPT;
    }
};

void enable_current_census_boot() noexcept { boot_enabled.store(true, std::memory_order_release); }

auto dispatch_current_census(uint64_t address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t {
    if (!boot_enabled.load(std::memory_order_acquire)) return failure(ENOTSUP);
    if (!sched::can_query_current_task()) return failure(EPERM);
    auto* const actor = sched::get_current_task();
    if (actor == nullptr) return failure(EPERM);
    mod::sys::SyscallPreparationScope preparation{*actor};
    if (!preparation.ready()) return failure(EPERM);
    if (bytes != sizeof(Request) || unused3 != 0 || unused4 != 0) return failure(EINVAL);
    Request request{};
    if (!usercopy::copy_from_task_mapped(*actor, address, &request, sizeof(request))) return failure(EFAULT);
    if (request.version != VERSION || request.bytes != sizeof(Request) || request.phase > static_cast<uint32_t>(Phase::END) ||
        request.flags != 0 || request.nonce == 0 || request.reserved[0] != 0 || request.reserved[1] != 0 ||
        request.output_bytes != sizeof(Report))
        return failure(EINVAL);
    if (!usercopy::range_valid(request.output_address, sizeof(Report))) return failure(EFAULT);
    return CensusHandler::run(*actor, request);
}
static_assert(OWNER_COUNT == mod::mm::PHYSICAL_PAGE_OWNER_COUNT);
static_assert(MAX_NODES == mod::acpi::numa::MAX_NODES);
}  // namespace ker::test::numa_lifecycle
#endif
