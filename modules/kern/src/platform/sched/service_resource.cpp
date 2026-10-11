#include "service_resource.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <new>
#include <platform/sched/native_creation_policy.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_process.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/service_resource_model.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>

namespace ker::mod::sched::resource {
namespace {
namespace service_abi = abi::service_resource;
struct ServiceSlot {
    ServiceState state{};
    std::array<char, service_abi::NAME_CAPACITY> name{};
    ServiceMember* members = nullptr;
    ServiceReportTarget* reports = nullptr;
    bool incomplete = false;
    bool configured = false;
};
std::array<ServiceSlot, service_abi::MAX_SERVICES> services{};
sys::Spinlock service_lock;

auto is_init(task::Task* subject) -> bool { return subject != nullptr && subject->pid == 1 && !subject->is_thread; }

auto service_management_status(task::Task* caller) -> int {
    if (caller == nullptr) return -EPERM;
    int const SCOPE = security::compartment::host_namespace_admission(caller);
    if (SCOPE != 0) return SCOPE;
    auto const CREDENTIALS = caller->credentials();
    return security::authorize_service_resource_management(CREDENTIALS, is_init(caller));
}

auto validate_configuration(service_abi::Configuration& config) -> bool {
    if (config.size != sizeof(config) || config.version != service_abi::VERSION || config.slot >= services.size() || config.flags != 0 ||
        config.reserved[0] != 0 || config.reserved[1] != 0 || config.name[0] == '\0')
        return false;
    for (unsigned i = 0; i < service_abi::NAME_CAPACITY; ++i) {
        char const C = config.name[i];
        if (C == '\0') {
            for (++i; i < service_abi::NAME_CAPACITY; ++i) config.name[i] = '\0';
            return true;
        }
        if (!((C >= 'a' && C <= 'z') || (C >= 'A' && C <= 'Z') || (C >= '0' && C <= '9') || C == '_' || C == '-')) return false;
    }
    return false;
}

// Caller holds the process resource lock. Imported history belongs to the
// origin service already; only admission/execution on this node is new.
auto observed_member_usage(const ServiceMember& member, unsigned index) -> Usage {
    auto usage = member.subject->resources.entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[index])];
    uint64_t const BASELINE = member.denied_baseline[index];
    usage.denied = usage.denied >= BASELINE ? usage.denied - BASELINE : 0;
    if (usage.denied == 0) usage.reason = Denial::NONE;
    return usage;
}
}  // namespace

ServiceResourceMerge::ServiceResourceMerge(task::Task& process)
    : process(process), service_flags(service_lock.lock_irqsave()), resource_flags(process.resource_lock.lock_irqsave()) {
    for (unsigned i = 0; i < before_denied.size(); ++i)
        before_denied[i] = process.resources.entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[i])].denied;
}

ServiceResourceMerge::~ServiceResourceMerge() {
    process.resource_lock.unlock_irqrestore(resource_flags);
    service_lock.unlock_irqrestore(service_flags);
}

void ServiceResourceMerge::exclude_reported_denials() {
    auto& member = process.service_member;
    if (member.slot == service_abi::NO_SELECTION && member.remote == nullptr) return;
    for (unsigned i = 0; i < before_denied.size(); ++i) {
        uint64_t const AFTER = process.resources.entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[i])].denied;
        uint64_t const ADDED = AFTER >= before_denied[i] ? AFTER - before_denied[i] : 0;
        member.denied_baseline[i] = saturating_cpu_add(member.denied_baseline[i], ADDED);
        before_denied[i] = AFTER;
    }
}

void mark_service_remote_execution(task::Task& process) {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    process.service_member.executing_remotely = true;
    service_lock.unlock_irqrestore(FLAGS);
}

ServiceMember::~ServiceMember() {
    if (remote != nullptr) {
        auto* accounting = remote;
        uint64_t const FLAGS = service_lock.lock_irqsave();
        uint64_t const RESOURCE_FLAGS = subject->resource_lock.lock_irqsave();
        for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
            auto const ID = service_abi::RESOURCE_IDS[i];
            if (ID != Id::CPU)
                ServiceState::merge(accounting->totals.entries[static_cast<unsigned>(ID)], observed_member_usage(*this, i), false);
        }
        subject->resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
        if (previous != nullptr)
            previous->next = next;
        else
            accounting->members = next;
        if (next != nullptr) next->previous = previous;
        service_lock.unlock_irqrestore(FLAGS);
        accounting->release();
        return;
    }
    if (slot == service_abi::NO_SELECTION) return;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto& service = services.at(slot);
    uint64_t const RESOURCE_FLAGS = subject->resource_lock.lock_irqsave();
    for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
        auto const ID = service_abi::RESOURCE_IDS[i];
        if (ID != Id::CPU)
            ServiceState::merge(service.state.totals.entries[static_cast<unsigned>(ID)], observed_member_usage(*this, i), false);
    }
    subject->resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    if (previous != nullptr)
        previous->next = next;
    else
        service.members = next;
    if (next != nullptr) next->previous = previous;
    service_lock.unlock_irqrestore(FLAGS);
}

auto RemoteServiceAccounting::create() -> RemoteServiceAccounting* { return new (std::nothrow) RemoteServiceAccounting(); }

void RemoteServiceAccounting::retain() { references.fetch_add(1, std::memory_order_relaxed); }

void RemoteServiceAccounting::release() {
    if (references.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
}

auto RemoteServiceAccounting::attach_locked(task::Task& process, const State* imported_history) -> bool {
    auto& member = process.service_member;
    uint64_t const FLAGS = process.resource_lock.lock_irqsave();
    if (imported_history != nullptr) {
        for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
            auto const ID = static_cast<unsigned>(service_abi::RESOURCE_IDS[i]);
            if (imported_history->entries[ID].denied > process.resources.entries[ID].denied) {
                process.resource_lock.unlock_irqrestore(FLAGS);
                return false;
            }
        }
    }
    for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i)
        member.denied_baseline[i] = (imported_history != nullptr ? *imported_history : process.resources)
                                        .entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[i])]
                                        .denied;
    process.resource_lock.unlock_irqrestore(FLAGS);
    member.subject = &process;
    member.remote = this;
    member.next = members;
    if (members != nullptr) members->previous = &member;
    members = &member;
    retain();
    return true;
}

auto RemoteServiceAccounting::attach(task::Task& process, const State* imported_history) -> bool {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    bool const ADMITTED = process.type == task::TaskType::PROCESS && !process.is_thread &&
                          process.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
                          !process.scheduler_published.load(std::memory_order_acquire) && process.service_member.subject == nullptr &&
                          process.service_member.slot == service_abi::NO_SELECTION && process.service_member.remote == nullptr;
    bool const ATTACHED = ADMITTED && attach_locked(process, imported_history);
    service_lock.unlock_irqrestore(FLAGS);
    return ATTACHED;
}

auto RemoteServiceAccounting::snapshot() -> RemoteServiceObservation {
    RemoteServiceObservation observation{};
    uint64_t const FLAGS = service_lock.lock_irqsave();
    observation.cpu_time_us = totals.cpu_time_us;
    observation.flags = incomplete ? service_abi::ACCOUNTING_INCOMPLETE : 0;
    for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i)
        observation.usage[i] = totals.entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[i])];
    for (auto* member = members; member != nullptr; member = member->next) {
        auto& process = *member->subject;
        // Exit readiness publishes stable local CPU or proxy completion
        // accounting. Retained zombie/GC metadata cannot delay a final report;
        // its historical counters remain visible until the destructor folds
        // them into totals. Nested remote streams have their own lifetime gate.
        if (!process.exit_notify_ready.load(std::memory_order_acquire) ||
            process.resource_cpu_threads.load(std::memory_order_acquire) != 0 ||
            process.resource_deferred_observations.load(std::memory_order_acquire) != 0)
            ++observation.members;
        bool const LIVE = process.scheduler_published.load(std::memory_order_acquire) && !member->executing_remotely &&
                          process.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE;
        if (LIVE) ++observation.live_processes;
        uint64_t const RESOURCE_FLAGS = process.resource_lock.lock_irqsave();
        for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
            if (service_abi::RESOURCE_IDS[i] != Id::CPU) ServiceState::merge(observation.usage[i], observed_member_usage(*member, i), LIVE);
        }
        process.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    }
    ServiceReportTarget::observe_reports_locked(reports, observation);
    service_lock.unlock_irqrestore(FLAGS);
    return observation;
}

ServiceReportTarget::~ServiceReportTarget() { cancel(); }

auto ServiceReportTarget::list_head_locked() -> ServiceReportTarget*& {
    return remote != nullptr ? remote->reports : services.at(slot).reports;
}

void ServiceReportTarget::move_from_locked(ServiceReportTarget& other) {
    previous = other.previous;
    next = other.next;
    remote = other.remote;
    slot = other.slot;
    state = other.state;
    if (remote != nullptr || slot != service_abi::NO_SELECTION) {
        if (previous != nullptr)
            previous->next = this;
        else
            list_head_locked() = this;
        if (next != nullptr) next->previous = this;
    }
    other.previous = nullptr;
    other.next = nullptr;
    other.remote = nullptr;
    other.slot = service_abi::NO_SELECTION;
    other.state = {};
}

ServiceReportTarget::ServiceReportTarget(ServiceReportTarget&& other) noexcept {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    move_from_locked(other);
    service_lock.unlock_irqrestore(FLAGS);
}

auto ServiceReportTarget::operator=(ServiceReportTarget&& other) noexcept -> ServiceReportTarget& {
    if (this == &other) return *this;
    cancel();
    uint64_t const FLAGS = service_lock.lock_irqsave();
    move_from_locked(other);
    service_lock.unlock_irqrestore(FLAGS);
    return *this;
}

auto service_remote_admission(task::Task& process) -> int {
    auto* owner = process.is_thread ? process.resource_owner.get() : &process;
    if (owner == nullptr) return -ESRCH;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int result = 0;
    if (owner->service_member.remote != nullptr)
        result = 1;
    else if (owner->service_member.slot != service_abi::NO_SELECTION) {
        auto const LIMIT = services.at(owner->service_member.slot).state.totals.entries[static_cast<unsigned>(Id::CPU)].limit;
        result = LIMIT.soft == INFINITY_VALUE && LIMIT.hard == INFINITY_VALUE ? 1 : -ENOTSUP;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto ServiceReportTarget::bind(task::Task& process) -> int {
    auto* owner = process.is_thread ? process.resource_owner.get() : &process;
    if (owner == nullptr) return -ESRCH;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int result = 0;
    if (remote != nullptr || slot != service_abi::NO_SELECTION || state.sequence != 0) {
        result = -EBUSY;
    } else if (owner->service_member.remote != nullptr) {
        remote = owner->service_member.remote;
        remote->retain();
        result = 1;
    } else if (owner->service_member.slot != service_abi::NO_SELECTION) {
        auto const SLOT = owner->service_member.slot;
        auto const LIMIT = services.at(SLOT).state.totals.entries[static_cast<unsigned>(Id::CPU)].limit;
        if (LIMIT.soft != INFINITY_VALUE || LIMIT.hard != INFINITY_VALUE) {
            result = -ENOTSUP;
        } else {
            slot = SLOT;
            result = 1;
        }
    }
    if (result == 1) {
        auto*& head = list_head_locked();
        next = head;
        if (head != nullptr) head->previous = this;
        head = this;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto ServiceReportTarget::apply(uint64_t sequence, const RemoteServiceObservation& observation, bool final) -> ServiceReportResult {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    ServiceReportResult result = ServiceReportResult::INVALID;
    if (remote != nullptr) {
        result = state.apply(sequence, observation, final, remote->totals, remote->incomplete);
    } else if (slot != service_abi::NO_SELECTION) {
        auto& service = services.at(slot);
        result = state.apply(sequence, observation, final, service.state.totals, service.incomplete);
    }
    service_lock.unlock_irqrestore(FLAGS);
    return result;
}

void ServiceReportTarget::cancel(bool incomplete) {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto* retained = remote;
    if (remote != nullptr || slot != service_abi::NO_SELECTION) {
        if ((incomplete || state.sequence != 0) && !state.finished) {
            if (remote != nullptr)
                remote->incomplete = true;
            else
                services.at(slot).incomplete = true;
        }
        if (previous != nullptr)
            previous->next = next;
        else
            list_head_locked() = next;
        if (next != nullptr) next->previous = previous;
    }
    previous = nullptr;
    next = nullptr;
    remote = nullptr;
    slot = service_abi::NO_SELECTION;
    service_lock.unlock_irqrestore(FLAGS);
    if (retained != nullptr) retained->release();
}

void ServiceReportTarget::add_current_locked(RemoteServiceObservation& observation) const {
    if (state.finished) return;
    ++observation.pending_remote_children;
    observation.live_processes = saturating_cpu_add(observation.live_processes, state.last.live_processes);
    for (unsigned i = 1; i < observation.usage.size(); ++i) {
        if (state.last.usage[i].current > observation.usage[i].current) observation.usage[i].current = state.last.usage[i].current;
    }
}

void ServiceReportTarget::observe_reports_locked(ServiceReportTarget* reports, RemoteServiceObservation& observation) {
    for (auto* report = reports; report != nullptr; report = report->next) report->add_current_locked(observation);
}

namespace {
auto native_creator_scope_status(task::Task& source) -> int {
    if (source.is_thread || source.credential_owner() != &source || source.type != task::TaskType::PROCESS) return -EINVAL;
    int const SCOPE = security::compartment::host_namespace_admission(&source);
    return SCOPE == -EPERM ? -ENOTSUP : SCOPE;
}

auto capture_native_creator_locked(task::Task& source, const security::compartment::Request& request, NativeCreatorPolicy& out) -> int {
    if (source.service_member.remote != nullptr || source.service_member.executing_remotely || source.wki_proxy_task) return -ENOTSUP;
    bool const INIT = is_init(&source);
    uint64_t const SLOT = INIT ? source.selected_child_service : source.service_member.slot;
    State const* service_limits = nullptr;
    if (SLOT != service_abi::NO_SELECTION) {
        if (SLOT >= services.size() || !services.at(SLOT).configured) return -EINVAL;
        if (!INIT && source.service_member.subject != &source) return -EINVAL;
        auto& service = services.at(SLOT);
        if (!service.state.admit_start()) return -EDQUOT;
        if (INIT) service_limits = &service.state.totals;
    }
    NativeCreatorPolicy candidate{};
    int const RESULT = capture_native_creator_fields(source, request, service_limits, candidate);
    if (RESULT == 0) {
        candidate.service_slot = SLOT;
        out = candidate;
    }
    return RESULT;
}

auto revalidate_native_creator_locked(task::Task& source, const NativeCreatorPolicy& policy) -> int {
    uint64_t const SLOT = is_init(&source) ? source.selected_child_service : source.service_member.slot;
    if (SLOT != policy.service_slot) return -EAGAIN;
    security::compartment::Request request{
        .authority_ceiling = policy.admitted.credentials.authority_ceiling(),
        .service_privileges = policy.admitted.credentials.service_privileges(),
        .limits = policy.admitted.limits,
        .network = policy.admitted.network,
        .dumpable = policy.admitted.credentials.dumpable(),
    };
    NativeCreatorPolicy renewed{};
    int const RESULT = capture_native_creator_locked(source, request, renewed);
    if (RESULT != 0) return RESULT == -EPERM ? -EAGAIN : RESULT;
    bool limits_match = true;
    for (size_t i = 0; i < policy.admitted.limits.size(); ++i) {
        auto const BEFORE = policy.admitted.limits[i];
        auto const AFTER = renewed.admitted.limits[i];
        limits_match &= BEFORE.soft == AFTER.soft && BEFORE.hard == AFTER.hard;
        auto const OLD_PARENT = policy.parent.limits[i];
        auto const NEW_PARENT = renewed.parent.limits[i];
        limits_match &= OLD_PARENT.soft == NEW_PARENT.soft && OLD_PARENT.hard == NEW_PARENT.hard;
    }
    if (policy.observed.generation != renewed.observed.generation || policy.observed.credentials != renewed.observed.credentials ||
        policy.service_slot != renewed.service_slot || policy.admitted.credentials != renewed.admitted.credentials || !limits_match ||
        policy.admitted.network != renewed.admitted.network || policy.parent.network != renewed.parent.network ||
        policy.creation_mask != renewed.creation_mask)
        return -EAGAIN;
    return 0;
}
}  // namespace

auto capture_native_creator_policy(task::Task& source, const security::compartment::Request& request, NativeCreatorPolicy& out) -> int {
    if (int const STATUS = native_creator_scope_status(source); STATUS != 0) return STATUS;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int const RESULT = capture_native_creator_locked(source, request, out);
    service_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto revalidate_native_creator_policy(task::Task& source, const NativeCreatorPolicy& policy) -> int {
    if (int const STATUS = native_creator_scope_status(source); STATUS != 0) return STATUS;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int const RESULT = revalidate_native_creator_locked(source, policy);
    service_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto attach_native_creation_service(task::Task& child, task::Task& source, const NativeCreatorPolicy& policy) -> int {
    if (int const STATUS = native_creator_scope_status(source); STATUS != 0) return STATUS;
    if (child.is_thread || child.type != task::TaskType::PROCESS || child.thread != nullptr || child.has_run ||
        child.scheduler_published.load(std::memory_order_acquire) || child.service_member.subject != nullptr ||
        child.service_member.slot != service_abi::NO_SELECTION || child.service_member.remote != nullptr)
        return -EBUSY;
    auto const* pid = child.pid_namespace();
    auto const* mounts = child.mount_namespace();
    auto const* ipc = child.ipc_namespace();
    if (pid == nullptr || mounts == nullptr || ipc == nullptr) return -ESRCH;
    if (pid->closed() || mounts->closed() || ipc->closed()) return -EIDRM;
    if (pid->is_host() || mounts->is_host() || ipc->is_host()) return -EINVAL;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int result = revalidate_native_creator_locked(source, policy);
    if (result == 0 && child.credentials() != policy.admitted.credentials) result = -EPERM;
    if (result == 0) {
        uint64_t const RESOURCE_FLAGS = child.resource_lock.lock_irqsave();
        for (size_t i = 0; i < policy.admitted.limits.size(); ++i) {
            auto const CHILD = child.resources.entries[i].limit;
            auto const ADMITTED = policy.admitted.limits[i];
            if (CHILD.soft != ADMITTED.soft || CHILD.hard != ADMITTED.hard) result = -EPERM;
        }
        child.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    }
    if (result == 0 && policy.service_slot != service_abi::NO_SELECTION) {
        auto& service = services.at(policy.service_slot);
        auto& member = child.service_member;
        member.subject = &child;
        member.slot = policy.service_slot;
        member.next = service.members;
        if (service.members != nullptr) service.members->previous = &member;
        service.members = &member;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return result;
}

void inherit_service(task::Task& child, task::Task& source) {
    // Threads charge through their retained process owner and must not create
    // a second entry for the same process's peaks or denial counters.
    if (child.is_thread) return;
    if (source.service_member.remote != nullptr) {
        uint64_t const FLAGS = service_lock.lock_irqsave();
        static_cast<void>(source.service_member.remote->attach_locked(child));
        service_lock.unlock_irqrestore(FLAGS);
        return;
    }
    uint64_t const SLOT = is_init(&source) ? source.selected_child_service : source.service_member.slot;
    if (SLOT == service_abi::NO_SELECTION) return;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto& service = services.at(SLOT);
    if (is_init(&source)) {
        uint64_t const RESOURCE_FLAGS = child.resource_lock.lock_irqsave();
        child.resources.inherit_limits(service.state.totals);
        child.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    }
    auto& member = child.service_member;
    member.subject = &child;
    member.slot = SLOT;
    member.next = service.members;
    if (service.members != nullptr) service.members->previous = &member;
    service.members = &member;
    service_lock.unlock_irqrestore(FLAGS);
}

auto charge_service_cpu(task::Task& process, uint64_t elapsed_us) -> CpuAction {
    if (auto* remote = process.service_member.remote; remote != nullptr) {
        uint64_t const FLAGS = service_lock.lock_irqsave();
        // The domain records work actually charged to this execution site.
        // Its CPU limit remains infinite; origin aggregate admission is a
        // separate protocol contract, never a copied per-process allowance.
        static_cast<void>(remote->totals.charge_cpu(elapsed_us));
        service_lock.unlock_irqrestore(FLAGS);
        return CpuAction::NONE;
    }
    uint64_t const SLOT = process.service_member.slot;
    if (SLOT == service_abi::NO_SELECTION) return CpuAction::NONE;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto const ACTION = services.at(SLOT).state.totals.charge_cpu(elapsed_us);
    service_lock.unlock_irqrestore(FLAGS);
    return ACTION;
}

namespace {
auto configure_service_for(task::Task* caller, uint64_t address) -> uint64_t {
    int const AUTHORIZATION = service_management_status(caller);
    if (AUTHORIZATION != 0) return static_cast<uint64_t>(AUTHORIZATION);
    service_abi::Configuration config{};
    if (!sys::usercopy::copy_value_from_task(*caller, address, config)) return static_cast<uint64_t>(-EFAULT);
    ServiceState candidate{};
    if (!validate_configuration(config) || !candidate.configure(config)) return static_cast<uint64_t>(-EINVAL);
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto& service = services.at(config.slot);
    int result = 0;
    if (service.configured) {
        result = -EEXIST;
    } else {
        service.state = candidate;
        for (unsigned i = 0; i < service_abi::NAME_CAPACITY; ++i) service.name[i] = config.name[i];
        service.configured = true;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return static_cast<uint64_t>(result);
}

auto select_service_for(task::Task* caller, uint64_t slot) -> uint64_t {
    int const SCOPE = service_management_status(caller);
    if (SCOPE != 0) return static_cast<uint64_t>(SCOPE);
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    int const AUTHORIZATION = service_management_status(caller);
    if (AUTHORIZATION != 0) return static_cast<uint64_t>(AUTHORIZATION);
    uint64_t const FLAGS = service_lock.lock_irqsave();
    int result = 0;
    // A failed selection also clears any previous pending selection.
    caller->selected_child_service = service_abi::NO_SELECTION;
    if (slot != service_abi::NO_SELECTION) {
        if (slot >= services.size() || !services.at(slot).configured)
            result = -EINVAL;
        else if (!services.at(slot).state.admit_start())
            result = -EDQUOT;
        else
            caller->selected_child_service = slot;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return static_cast<uint64_t>(result);
}
}  // namespace

auto service_configure_for_subject(task::Task* caller, uint64_t address) -> uint64_t { return configure_service_for(caller, address); }
auto service_select_for_subject(task::Task* caller, uint64_t slot) -> uint64_t { return select_service_for(caller, slot); }

auto syscall_service_configure(uint64_t address) -> uint64_t { return service_configure_for_subject(get_current_task(), address); }

auto syscall_service_select(uint64_t slot) -> uint64_t { return service_select_for_subject(get_current_task(), slot); }

auto service_read_for_subject(task::Task* caller, uint64_t slot, uint64_t address, uint64_t version) -> uint64_t {
    if (caller == nullptr) return static_cast<uint64_t>(-ESRCH);
    int const SCOPE = security::compartment::host_namespace_admission(caller);
    if (SCOPE != 0) return static_cast<uint64_t>(SCOPE);
    if (!caller->credentials().allows(ker::mod::security::Authority::RESOURCE_LIMIT)) return static_cast<uint64_t>(-EACCES);
    if (slot >= services.size() || version != service_abi::VERSION) return static_cast<uint64_t>(-EINVAL);
    service_abi::Snapshot snapshot{};
    snapshot.size = sizeof(snapshot);
    snapshot.version = service_abi::VERSION;
    snapshot.slot = static_cast<uint32_t>(slot);
    uint64_t const FLAGS = service_lock.lock_irqsave();
    auto& service = services.at(slot);
    if (!service.configured) {
        service_lock.unlock_irqrestore(FLAGS);
        return static_cast<uint64_t>(-ENOENT);
    }
    for (unsigned i = 0; i < service_abi::NAME_CAPACITY; ++i) snapshot.name[i] = service.name[i];
    snapshot.cpu_time_us = service.state.totals.cpu_time_us;
    snapshot.flags = service.incomplete ? service_abi::ACCOUNTING_INCOMPLETE : 0;
    for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
        snapshot.usage[i] = service.state.totals.entries[static_cast<unsigned>(service_abi::RESOURCE_IDS[i])];
    }
    for (auto* member = service.members; member != nullptr; member = member->next) {
        auto& process = *member->subject;
        bool const LIVE = process.scheduler_published.load(std::memory_order_acquire) && !member->executing_remotely &&
                          process.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE;
        if (LIVE) ++snapshot.live_processes;
        uint64_t const RESOURCE_FLAGS = process.resource_lock.lock_irqsave();
        for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
            auto const ID = service_abi::RESOURCE_IDS[i];
            if (ID != Id::CPU) ServiceState::merge(snapshot.usage[i], observed_member_usage(*member, i), LIVE);
        }
        process.resource_lock.unlock_irqrestore(RESOURCE_FLAGS);
    }
    RemoteServiceObservation remote_observation{};
    ServiceReportTarget::observe_reports_locked(service.reports, remote_observation);
    snapshot.live_processes = saturating_cpu_add(snapshot.live_processes, remote_observation.live_processes);
    for (unsigned i = 1; i < service_abi::RESOURCE_COUNT; ++i) {
        if (remote_observation.usage[i].current > snapshot.usage[i].current)
            snapshot.usage[i].current = remote_observation.usage[i].current;
    }
    service_lock.unlock_irqrestore(FLAGS);
    return sys::usercopy::copy_value_to_task(*caller, address, snapshot) ? 0 : static_cast<uint64_t>(-EFAULT);
}

auto syscall_service_read(uint64_t slot, uint64_t address, uint64_t version) -> uint64_t {
    return service_read_for_subject(get_current_task(), slot, address, version);
}

#ifdef WOS_SELFTEST
auto selftest_service_management_authority() -> bool {
    using namespace security;
    constexpr uint64_t DENIED = static_cast<uint64_t>(-EPERM);
    if (configure_service_for(nullptr, UINT64_MAX) != DENIED || select_service_for(nullptr, 0) != DENIED) return false;
    auto const ROOT = Snapshot::bootstrap_root();
    std::array<Snapshot, 4> credentials{ROOT, ROOT, ROOT, ROOT};
    if (ROOT.prepare({.change = Change::SET_EUID, .effective = 1000}, credentials.at(1)) != 0 ||
        ROOT.prepare({.change = Change::DROP_AUTHORITY,
                      .authority_ceiling = ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(Authority::INIT_CONTROL))},
                     credentials.at(2)) != 0 ||
        ROOT.prepare({.change = Change::DROP_AUTHORITY,
                      .authority_ceiling = ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(Authority::RESOURCE_LIMIT))},
                     credentials.at(3)) != 0)
        return false;
    for (auto const& value : credentials) {
        for (uint64_t pid : {uint64_t{1}, uint64_t{2}}) {
            task::Task caller{};
            caller.pid = pid;
            if (initialize_credentials(caller, value) != 0) return false;
            caller.selected_child_service = 7;
            bool const ALLOWED = pid == 1 && value == ROOT;
            if (configure_service_for(&caller, UINT64_MAX) != static_cast<uint64_t>(ALLOWED ? -EFAULT : -EPERM) ||
                caller.selected_child_service != 7)
                return false;
            if (select_service_for(&caller, service_abi::NO_SELECTION) != (ALLOWED ? 0 : DENIED) ||
                caller.selected_child_service != (ALLOWED ? service_abi::NO_SELECTION : 7))
                return false;
        }
    }
    task::Task owner{};
    task::Task thread{};
    thread.pid = 1;
    thread.is_thread = true;
    thread.selected_child_service = 7;
    constexpr uint64_t UNBOUND = static_cast<uint64_t>(-ESRCH);
    if (configure_service_for(&thread, UINT64_MAX) != UNBOUND || select_service_for(&thread, service_abi::NO_SELECTION) != UNBOUND ||
        thread.selected_child_service != 7)
        return false;
    owner.pid = 1;
    thread.owner_pid = owner.pid;
    if (!bind_thread_owner(thread, owner)) return false;
    return configure_service_for(&thread, UINT64_MAX) == DENIED && select_service_for(&thread, service_abi::NO_SELECTION) == DENIED &&
           thread.selected_child_service == 7;
}

auto selftest_service_observation_merge() -> bool {
    constexpr unsigned AS = static_cast<unsigned>(Id::AS);
    constexpr unsigned AS_ROW = service_abi::RESOURCE_COUNT - 1;
    auto* accounting = RemoteServiceAccounting::create();
    if (accounting == nullptr) return false;
    bool ok = true;
    {
        task::Task process{};
        process.type = task::TaskType::PROCESS;
        process.resources.entries[AS].denied = 5;
        process.resources.entries[AS].reason = Denial::SOFT_LIMIT;
        process.resources.commit_extent(AS, 4096);
        State history{};
        history.entries[AS].denied = 6;
        ok &= !accounting->attach(process, &history) && accounting->snapshot().members == 0 && process.service_member.subject == nullptr &&
              process.service_member.remote == nullptr;
        history.entries[AS].denied = 3;
        ok &= accounting->attach(process, &history);
        process.scheduler_published.store(true);
        auto observed = accounting->snapshot();
        ok &= observed.usage[AS_ROW].denied == 2 && observed.live_processes == 1 && observed.usage[AS_ROW].current == 4096;
        {
            ServiceResourceMerge guard(process);
            process.resources.entries[AS].denied += 4;
            guard.exclude_reported_denials();
            guard.exclude_reported_denials();
        }
        observed = accounting->snapshot();
        ok &= observed.usage[AS_ROW].denied == 2 && process.resources.entries[AS].denied == 9;
        mark_service_remote_execution(process);
        observed = accounting->snapshot();
        ok &= observed.live_processes == 0 && observed.usage[AS_ROW].current == 0 && process.resources.entries[AS].current == 4096;
    }
    ok &= accounting->snapshot().usage[AS_ROW].denied == 2 && accounting->snapshot().members == 0;
    accounting->release();
    accounting = RemoteServiceAccounting::create();
    if (accounting == nullptr) return false;
    {
        task::Task process{};
        process.type = task::TaskType::PROCESS;
        process.resources.entries[AS].denied = UINT64_MAX - 1;
        State history{};
        ok &= accounting->attach(process, &history);
        {
            ServiceResourceMerge guard(process);
            process.resources.entries[AS].denied = UINT64_MAX;
            guard.exclude_reported_denials();
        }
        ok &= accounting->snapshot().usage[AS_ROW].denied == UINT64_MAX - 1;
    }
    ok &= accounting->snapshot().usage[AS_ROW].denied == UINT64_MAX - 1;
    accounting->release();
    return ok;
}

auto selftest_service_report_targets() -> bool {
    constexpr unsigned AS = static_cast<unsigned>(Id::AS);
    constexpr unsigned AS_ROW = service_abi::RESOURCE_COUNT - 1;
    auto* accounting = RemoteServiceAccounting::create();
    if (accounting == nullptr) return false;
    bool ok = true;
    {
        ServiceReportTarget first;
        ServiceReportTarget second;
        {
            task::Task root{};
            root.type = task::TaskType::PROCESS;
            ok &= accounting->attach(root);
            ok &= first.bind(root) == 1 && first.bind(root) == -EBUSY && second.bind(root) == 1;
        }
        auto observation = accounting->snapshot();
        ok &= observation.members == 0 && observation.pending_remote_children == 2;
        RemoteServiceObservation report{};
        report.cpu_time_us = 100;
        report.members = report.live_processes = 1;
        report.usage[AS_ROW].current = 1024;
        report.usage[AS_ROW].peak = 4096;
        report.usage[AS_ROW].denied = 1;
        report.usage[AS_ROW].reason = Denial::SOFT_LIMIT;
        ok &= first.apply(1, report, false) == ServiceReportResult::APPLIED;
        ok &= first.apply(1, report, false) == ServiceReportResult::DUPLICATE;
        report.cpu_time_us = 200;
        report.usage[AS_ROW].current = 2048;
        ok &= second.apply(1, report, false) == ServiceReportResult::APPLIED;
        // Both middle/tail and head list links must follow moves, while each
        // destination keeps precisely one reference to the enclosing object.
        ServiceReportTarget moved(std::move(first));
        ServiceReportTarget assigned;
        assigned = std::move(second);
        observation = accounting->snapshot();
        ok &= observation.pending_remote_children == 2 && observation.cpu_time_us == 300 && observation.live_processes == 2 &&
              observation.usage[AS_ROW].current == 2048 && observation.usage[AS_ROW].denied == 2;
        report.members = report.live_processes = 0;
        report.usage[AS_ROW].current = 0;
        ok &= assigned.apply(2, report, true) == ServiceReportResult::APPLIED;
        assigned.cancel();
        observation = accounting->snapshot();
        ok &= observation.pending_remote_children == 1 && observation.live_processes == 1 && observation.usage[AS_ROW].current == 1024 &&
              observation.flags == 0;
        moved.cancel();
        observation = accounting->snapshot();
        ok &= observation.pending_remote_children == 0 && observation.live_processes == 0 && observation.usage[AS_ROW].current == 0 &&
              observation.cpu_time_us == 300 && observation.usage[AS_ROW].denied == 2 &&
              observation.flags == service_abi::ACCOUNTING_INCOMPLETE;
    }
    accounting->release();

    constexpr uint64_t SLOT = service_abi::MAX_SERVICES - 1;
    auto& service = services.at(SLOT);
    if (service.configured || service.members != nullptr || service.reports != nullptr) return false;
    service.configured = true;
    {
        task::Task init{};
        init.pid = 1;
        init.selected_child_service = SLOT;
        task::Task root{};
        root.type = task::TaskType::PROCESS;
        ok &= inherit(root, init);
        ServiceReportTarget target;
        auto& cpu_limit = service.state.totals.entries[static_cast<unsigned>(Id::CPU)].limit;
        cpu_limit = {1, INFINITY_VALUE};
        ok &= target.bind(root) == -ENOTSUP && service.reports == nullptr && !service.incomplete;
        cpu_limit = {1, 2};
        ok &= target.bind(root) == -ENOTSUP && service.reports == nullptr;
        cpu_limit = {INFINITY_VALUE, INFINITY_VALUE};
        ok &= target.bind(root) == 1;
        RemoteServiceObservation report{};
        report.cpu_time_us = 123;
        report.usage[AS_ROW].peak = 8192;
        report.usage[AS_ROW].denied = 3;
        ok &= target.apply(1, report, true) == ServiceReportResult::APPLIED;
        {
            ServiceResourceMerge guard(root);
            root.resources.entries[AS].denied = 3;
            guard.exclude_reported_denials();
        }
        ok &= service.state.totals.cpu_time_us == 123 && service.state.totals.entries[AS].denied == 3;
        target.cancel();
        ServiceReportTarget rejected;
        ok &= rejected.bind(root) == 1;
        rejected.cancel(false);
        ok &= service.reports == nullptr && !service.incomplete;
    }
    ok &= service.members == nullptr && service.reports == nullptr && service.state.totals.cpu_time_us == 123 &&
          service.state.totals.entries[AS].denied == 3;
    service = {};
    return ok;
}

auto selftest_remote_service_accounting_lifecycle() -> bool {
    auto* accounting = RemoteServiceAccounting::create();
    if (accounting == nullptr) return false;
    constexpr unsigned AS = static_cast<unsigned>(Id::AS);
    constexpr unsigned AS_ROW = service_abi::RESOURCE_COUNT - 1;
    bool ok = true;
    {
        task::Task descendant{};
        descendant.pid = 0x22A2;
        descendant.type = task::TaskType::PROCESS;
        {
            task::Task root{};
            root.pid = 0x22A1;
            root.type = task::TaskType::PROCESS;
            root.resources.commit_extent(AS, 4096);
            root.resources.deny(AS, Denial::SOFT_LIMIT);
            root.resources.deny(AS, Denial::SOFT_LIMIT);
            static_cast<void>(root.resources.charge_cpu(700));
            if (!accounting->attach(root)) {
                accounting->release();
                return false;
            }
            ok &= !accounting->attach(root);
            root.scheduler_published.store(true);
            ok &= inherit(descendant, root);
            descendant.scheduler_published.store(true);
            descendant.resources.commit_extent(AS, 8192);
            descendant.resources.deny(AS, Denial::SOFT_LIMIT);
            root.resources.deny(AS, Denial::HARD_LIMIT);
            charge_cpu(&root, 100);
            charge_cpu(&descendant, 200);
            {
                task::Task worker{};
                worker.is_thread = true;
                worker.owner_pid = root.pid;
                worker.type = task::TaskType::PROCESS;
                ok &= !accounting->attach(worker) && bind_thread_owner(worker, root);
                charge_cpu(&worker, 50);
                task::Task grandchild{};
                grandchild.type = task::TaskType::PROCESS;
                ok &= inherit(grandchild, descendant);
                grandchild.resources.deny(AS, Denial::SOFT_LIMIT);
                grandchild.resources.deny(AS, Denial::SOFT_LIMIT);
                charge_cpu(&grandchild, 25);
                auto const OBSERVED = accounting->snapshot();
                ok &= OBSERVED.members == 3 && OBSERVED.live_processes == 2 && OBSERVED.cpu_time_us == 375 &&
                      OBSERVED.usage[AS_ROW].denied == 4 && OBSERVED.usage[AS_ROW].current == 8192;
            }
        }
        // Root metadata is gone. Its descendant can keep executing and fork
        // under the same independently retained observation lifetime.
        charge_cpu(&descendant, 75);
        auto const OBSERVED = accounting->snapshot();
        ok &= OBSERVED.members == 1 && OBSERVED.live_processes == 1 && OBSERVED.cpu_time_us == 450 && OBSERVED.usage[AS_ROW].denied == 4 &&
              OBSERVED.usage[AS_ROW].peak == 8192;
        task::Task already_published{};
        already_published.type = task::TaskType::PROCESS;
        already_published.scheduler_published.store(true);
        ok &= !accounting->attach(already_published) && accounting->snapshot().members == 1;
    }
    auto const FINAL = accounting->snapshot();
    ok &= FINAL.members == 0 && FINAL.live_processes == 0 && FINAL.cpu_time_us == 450 && FINAL.usage[AS_ROW].denied == 4 &&
          FINAL.usage[AS_ROW].peak == 8192 && FINAL.usage[AS_ROW].current == 0;
    accounting->release();

    // Dropping the monitor first must leave members' execution and inheritance
    // safe. The final process destructor releases the last accounting ref.
    auto* detached = RemoteServiceAccounting::create();
    if (detached == nullptr) return false;
    task::Task root{};
    root.type = task::TaskType::PROCESS;
    bool const ATTACHED = detached->attach(root);
    detached->release();
    if (!ATTACHED) return false;
    task::Task child{};
    child.type = task::TaskType::PROCESS;
    ok &= inherit(child, root);
    charge_cpu(&child, 11);
    ok &= root.service_member.remote->snapshot().members == 2 && root.service_member.remote->snapshot().cpu_time_us == 11;
    return ok;
}

auto selftest_service_lifecycle() -> bool {
    constexpr uint64_t SLOT = service_abi::MAX_SERVICES - 1;
    auto& service = services.at(SLOT);
    // KTEST invokes this before init or process scheduling. Never replace an
    // initialized slot even in a diagnostic kernel.
    if (service.configured || service.members != nullptr) return false;
    service_abi::Configuration config{};
    for (auto& limit : config.limits) limit = {INFINITY_VALUE, INFINITY_VALUE};
    config.limits[0] = {1, 2};
    config.limits[5] = {3, 5};
    if (!service.state.configure(config)) return false;
    service.configured = true;
    bool ok = true;
    {
        task::Task init{};
        init.pid = 1;
        init.selected_child_service = SLOT;
        task::Task child{};
        child.pid = 1000;
        child.type = task::TaskType::PROCESS;
        ok = inherit(child, init) && ok;
        init.selected_child_service = service_abi::NO_SELECTION;
        constexpr unsigned NOFILE = static_cast<unsigned>(Id::NOFILE);
        constexpr unsigned AS = static_cast<unsigned>(Id::AS);
        ok = child.service_member.slot == SLOT && child.resources.entries[NOFILE].limit.soft == 3 && ok;
        ok = child.resources.set(NOFILE, {2, 5}, true, 256) == 0 && ok;
        child.resources.commit_extent(AS, 4096);
        child.resources.deny(AS, Denial::SOFT_LIMIT);
        {
            task::Task descendant{};
            descendant.pid = 1001;
            descendant.type = task::TaskType::PROCESS;
            ok = inherit(descendant, child) && ok;
            ok = descendant.service_member.slot == SLOT && descendant.resources.entries[NOFILE].limit.soft == 2 && ok;
            task::Task worker{};
            worker.is_thread = true;
            worker.owner_pid = child.pid;
            worker.type = task::TaskType::PROCESS;
            ok = bind_thread_owner(worker, child) && ok;
            charge_cpu(&worker, 500000);
            charge_cpu(&child, 500000);
            charge_cpu(&descendant, 1000000);
            charge_cpu(&child, 1);
            ok = child.resources.cpu_time_us == 1000001 && descendant.resources.cpu_time_us == 1000000 &&
                 service.state.totals.cpu_time_us == 2000001 && (descendant.pending_cpu_signals.load() & (uint64_t{1} << 8)) != 0 &&
                 (child.pending_cpu_signals.load() & (uint64_t{1} << 8)) != 0 && !service.state.admit_start() && ok;
        }
        ok = service.members == &child.service_member && child.service_member.next == nullptr && child.ref_count.load() == 1 && ok;
    }
    ok = service.members == nullptr && service.state.totals.cpu_time_us == 2000001 &&
         service.state.totals.entries[static_cast<unsigned>(Id::AS)].peak == 4096 &&
         service.state.totals.entries[static_cast<unsigned>(Id::AS)].denied == 1 && ok;
    service = {};
    return ok;
}

auto selftest_native_creator_service_policy() -> bool {
    // KTEST runs before normal init. Exercise the actual global membership and
    // aggregate gate, returning the untouched slot after every return path.
    constexpr uint64_t SLOT = service_abi::MAX_SERVICES - 3;
    auto& service = services.at(SLOT);
    if (service.configured || service.members != nullptr || service.reports != nullptr) return false;
    struct ScopedSlot {
        ServiceSlot& service;
        ~ScopedSlot() { service = {}; }
    } reset{service};
    service_abi::Configuration config{};
    for (auto& limit : config.limits) limit = {INFINITY_VALUE, INFINITY_VALUE};
    config.limits[0] = {1, 2};
    config.limits[5] = {3, 5};
    if (!service.state.configure(config)) return false;
    service.configured = true;
    task::Task init{};
    init.pid = 1;
    init.type = task::TaskType::PROCESS;
    init.selected_child_service = SLOT;
    security::compartment::Request request;
    for (size_t i = 0; i < request.limits.size(); ++i) request.limits[i] = init.resources.entries[i].limit;
    for (unsigned i = 0; i < service_abi::RESOURCE_COUNT; ++i) {
        auto const ID = static_cast<unsigned>(service_abi::RESOURCE_IDS[i]);
        auto const LIMIT = service.state.totals.entries[ID].limit;
        request.limits[ID].soft = std::min(request.limits[ID].soft, LIMIT.soft);
        request.limits[ID].hard = std::min(request.limits[ID].hard, LIMIT.hard);
    }
    constexpr unsigned NOFILE = static_cast<unsigned>(Id::NOFILE);
    request.limits[NOFILE] = {1, 2};
    NativeCreatorPolicy policy;
    if (capture_native_creator_policy(init, request, policy) != 0 || policy.service_slot != SLOT) return false;
    bool ok = true;
    {
        pid_ns::NamespaceRef pids;
        ipc::NamespaceRef ipc_scope;
        vfs::mount_ns::NamespaceRef mounts;
        if (pid_ns::create_namespace(pids) != 0 || ipc::create_namespace(ipc_scope) != 0 || vfs::mount_ns::create_namespace(mounts) != 0)
            return false;
        task::Task child{};
        child.pid = task::get_next_pid();
        child.type = task::TaskType::PROCESS;
        for (size_t i = 0; i < policy.admitted.limits.size(); ++i) child.resources.entries[i].limit = policy.admitted.limits[i];
        if (initialize_credentials(child, policy.admitted.credentials) != 0 || !child.initialize_pid_namespace(pids) ||
            !child.initialize_ipc_namespace(ipc_scope) || !child.initialize_mount_namespace(mounts))
            return false;
        if (attach_native_creation_service(child, init, policy) != 0) return false;
        ok = child.service_member.slot == SLOT && service.members == &child.service_member && child.service_member.subject == &child &&
             child.resources.entries[NOFILE].limit.soft == 1 && child.resources.entries[NOFILE].limit.hard == 2 && ok;
        init.selected_child_service = service_abi::NO_SELECTION;
        ok = revalidate_native_creator_policy(init, policy) == -EAGAIN && ok;
        init.selected_child_service = SLOT;
        ok = charge_service_cpu(child, 2'000'001) == CpuAction::HARD_KILL && ok;
        ok = revalidate_native_creator_policy(init, policy) == -EDQUOT && ok;
        ok = child.resources.entries[NOFILE].limit.soft == 1 && ok;
    }
    return service.members == nullptr && service.state.totals.cpu_time_us == 2'000'001 && ok;
}
#endif
}  // namespace ker::mod::sched::resource
