#ifdef WOS_SELFTEST
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/service_resource.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/process/init_control.hpp>
#include <test/compartment_lifecycle_runtime.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>

namespace ker::test::lifecycle60 {
namespace {
namespace sched = ker::mod::sched;
namespace task = sched::task;
using log = ker::mod::dbg::logger<"lifecycle60">;
// A finite diagnostic ceiling, not a measured qualification duration. Pilot
// timing must establish an admitted deadline before any actual campaign.
constexpr uint64_t LIMIT_US = 3'600'000'000;
constexpr uint64_t EIGHT_CPUS = 0xff;
enum class Park : uint8_t { NONE, EVIDENCE, WANT_ACK, ACKED, RELEASE };
struct Debt {
    void* address{};  // comparison identity only, never a Task lease
    uint64_t deletion_cookie{};
    bool deleting{};
};
struct Record {
    AttemptView value{};
    std::atomic<bool> consumed{false};
    std::array<Debt, 2> debts{};
    bool armed{};
    bool complete{};
};
struct NtpBoot {
    void* address{};  // comparison only, cleared BEFORE free
    uint64_t cookie{};
    bool init_seen{}, post_known{}, posted{}, periodic{}, exiting{}, deleting{}, returned{}, invalid{};
};
struct State {
    bool enabled{};
    bool started{};
    bool failed{};
    bool image_admitted{};
    bool host_baseline{};
    bool driver_confirmed{};
    bool final_accepted{};
    bool init_reaped_driver{};
    bool evidence_inflight{};
    bool ack{};
    Task* init{};  // checked lifetime refs, released outside lock
    Task* driver{};
    Task* park_actor{};
    Task* controller{};  // actual current daemon, scheduler owns it
    uint64_t driver_pid{};
    uint64_t driver_service_slot{UINT64_MAX};
    ker::syscall::process::SelftestLifecycleInitBoundary init_boundary{};
    uint64_t image_epoch{};
    uint64_t next_cookie{};
    uint64_t next_ntp_cookie{};
    NtpBoot ntp{};
    uint64_t next_deletion_cookie{};
    uint64_t park_generation{};
    uint64_t readers{};
    uint64_t deadline{};
    size_t next{};
    Park park{Park::NONE};
    Boundary boundary{Boundary::BEFORE_CREATE};
};
constinit ker::mod::sys::Spinlock state_lock{};
constinit State state{};
constinit std::array<Record, ATTEMPTS> records{};
class Guard {
   public:
    Guard() : flags_(state_lock.lock_irqsave()) {}
    ~Guard() { state_lock.unlock_irqrestore(flags_); }
    Guard(const Guard&) = delete;
    auto operator=(const Guard&) -> Guard& = delete;

   private:
    uint64_t flags_;
};
constexpr std::array<Fault, FAULTS> stages{Fault::TASK,
                                           Fault::NAME,
                                           Fault::PAGEMAP,
                                           Fault::SCRATCH,
                                           Fault::BUFFER,
                                           Fault::RECOVERY_AFTER_READ,
                                           Fault::ARGUMENT_POINTERS,
                                           Fault::ARGUMENT_ARENA,
                                           Fault::INTERPRETER_BUFFER,
                                           Fault::STACK_ADDRESSES,
                                           Fault::RECOVERY_INTERPRETER_READ,
                                           Fault::RECOVERY_STACK_BUILT,
                                           Fault::CREATION_STATE,
                                           Fault::CREATION_RESERVATION,
                                           Fault::CREATION_ROOT,
                                           Fault::CREATION_IMPORT_BUFFER,
                                           Fault::CREATION_IMPORT_READ,
                                           Fault::CREATION_DESCRIPTORS,
                                           Fault::CREATION_RECOVERY_IMPORT,
                                           Fault::CREATION_RECOVERY_BUILT,
                                           Fault::PUBLICATION_BEFORE_POST,
                                           Fault::PUBLICATION_INVALID_CPU,
                                           Fault::USER_INPUT_STATE,
                                           Fault::USER_ARGUMENT_ARENA};
static_assert(ATTEMPTS == 1032 && WARMUPS + FAULTS == 32);

auto expected_error(Fault fault) noexcept -> int64_t {
    switch (fault) {
        case Fault::NONE:
            return 0;
        case Fault::RECOVERY_AFTER_READ:
        case Fault::RECOVERY_INTERPRETER_READ:
        case Fault::RECOVERY_STACK_BUILT:
        case Fault::CREATION_RECOVERY_IMPORT:
        case Fault::CREATION_RECOVERY_BUILT:
            return -ECANCELED;
        case Fault::CREATION_IMPORT_READ:
            return -EIO;
        case Fault::PUBLICATION_BEFORE_POST:
        case Fault::PUBLICATION_INVALID_CPU:
            return -EAGAIN;
        default:
            return -ENOMEM;
    }
}
auto issue(uint64_t& last) noexcept -> uint64_t {
    if (last == UINT64_MAX) {
        state.failed = true;
        return 0;
    }
    return ++last;
}
auto debt_empty(const Record& record) noexcept -> bool {
    for (auto const& debt : record.debts)
        if (debt.address != nullptr || debt.deleting) return false;
    return true;
}
auto view_locked(size_t index) noexcept -> AttemptView {
    auto out = records.at(index).value;
    out.consumed = records.at(index).consumed.load(std::memory_order_acquire);
    out.task_debt_empty = debt_empty(records.at(index));
    return out;
}
auto current_actor(Task& actor) noexcept -> bool {
    return sched::can_query_current_task() && sched::get_current_task() == &actor && actor.type == task::TaskType::PROCESS &&
           !actor.is_thread && actor.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
           !actor.scheduler_transition_active.load(std::memory_order_acquire) && sched::preempt_count() == 0;
}
auto retain_checked(Task& subject) noexcept -> bool {
    auto count = subject.ref_count.load(std::memory_order_acquire);
    while (count != 0 && count != UINT32_MAX)
        if (subject.ref_count.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel, std::memory_order_acquire)) return true;
    return false;
}
auto fatal_pending(Task& actor) noexcept -> bool {
    constexpr uint64_t KILL = uint64_t{1} << 8;
    return actor.process_exit_requested.load(std::memory_order_acquire) || actor.exit_in_progress.load(std::memory_order_acquire) ||
           (actor.signal_pending_bits() & KILL) != 0 || (actor.pending_cpu_signals.load(std::memory_order_acquire) & KILL) != 0 ||
           (actor.signal_deliverable_bits() & ~(uint64_t{1} << 16)) != 0 ||
           actor.pending_exec_handoff.load(std::memory_order_acquire) != nullptr ||
           actor.resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0;
}
// Only the executing driver calls this. Its kernel syscall frame remains
// owned while parked; existing signal handoff refuses fatal exit from KERNEL
// frames. No usercopy, VFS, process-exit gate or subsystem lock in this loop.
auto park_executor(Task& actor, Boundary boundary) noexcept -> bool {
    if (!current_actor(actor)) {
        Guard guard;
        state.failed = true;
        return false;
    }
    ker::mod::sys::SyscallPreparationScope preparation(actor);
    if (!preparation.ready()) {
        Guard guard;
        state.failed = true;
        return false;
    }
    uint64_t generation = 0;
    {
        Guard guard;
        bool const INIT = boundary == Boundary::BEFORE_DRIVER || boundary == Boundary::AFTER_DRIVER;
        bool const OWNER = INIT ? state.init == &actor : state.driver == &actor;
        if (state.failed || !OWNER || (!INIT && state.next >= ATTEMPTS) || state.park != Park::NONE || state.readers != 0 ||
            state.evidence_inflight) {
            state.failed = true;
            return false;
        }
        generation = issue(state.park_generation);
        if (generation == 0) return false;
        state.boundary = boundary;
        state.park_actor = &actor;
        state.park = Park::EVIDENCE;
    }
    for (;;) {
        bool const FATAL = fatal_pending(actor);
        uint64_t const NOW = ker::mod::time::get_us();
        bool released = false;
        bool passed = false;
        {
            Guard guard;
            if (generation != state.park_generation || state.park_actor != &actor) state.failed = true;
            if (FATAL || (state.deadline != 0 && NOW >= state.deadline)) state.failed = true;
            if (state.failed) {
                // Withdraw permission first. Already-issued leases remain real
                // owners until reset; evidence I/O also finishes before resume.
                state.ack = false;
                if (state.readers == 0 && !state.evidence_inflight) {
                    state.park = Park::NONE;
                    state.park_actor = nullptr;
                    released = true;
                }
            } else if (state.park == Park::WANT_ACK) {
                state.ack = true;
                state.park = Park::ACKED;
            } else if (state.park == Park::RELEASE && state.readers == 0 && !state.evidence_inflight) {
                state.ack = false;
                state.park = Park::NONE;
                state.park_actor = nullptr;
                released = passed = true;
            }
        }
        if (released) return passed;
        sched::kern_yield();
    }
}

auto summary_locked() noexcept -> CampaignSummary {
    CampaignSummary out{};
    out.last_cookie = state.next_cookie;
    out.original_init_reaped_driver = state.init_reaped_driver;
    out.all_attempt_task_debts_empty = true;
    for (size_t i = 0; i < records.size(); ++i) {
        auto const& record = records.at(i);
        if (!debt_empty(record)) out.all_attempt_task_debts_empty = false;
        if (!record.complete) continue;
        if (record.value.warmup)
            ++out.warmups;
        else if (record.value.fault != Fault::NONE)
            ++out.faults;
        else
            ++out.successes;
        if (record.consumed.load(std::memory_order_acquire)) ++out.consumed_faults;
        if (record.value.wait_consumed && record.value.wait_status == 9) ++out.actual_wait9;
    }
    return out;
}
void release_subject_refs(bool keep_init = false) noexcept {
    Task* init = nullptr;
    Task* driver = nullptr;
    {
        Guard guard;
        if (state.ack || state.readers != 0 || state.evidence_inflight || state.park != Park::NONE) {
            state.failed = true;
            return;
        }
        if (!keep_init) init = std::exchange(state.init, nullptr);
        driver = std::exchange(state.driver, nullptr);
    }
    if (driver != nullptr) driver->release();
    if (init != nullptr) init->release();
}
auto controller_loop() -> bool {
    {
        Guard guard;
        state.controller = sched::get_current_task();
        state.deadline = sched::saturating_deadline_us(ker::mod::time::get_us(), LIMIT_US);
    }
    log::info("LIFECYCLE60 BEGIN warmups=8 faults=24 successes=1000");
    for (;;) {
        uint64_t const NOW = ker::mod::time::get_us();
        bool failed = false;
        bool finished = false;
        bool sample_evidence = false;
        bool sample_census = false;
        Task* actor = nullptr;
        uint64_t epoch = 0;
        size_t index = 0;
        Boundary boundary{};
        AttemptView view{};
        {
            Guard guard;
            if (NOW >= state.deadline || sched::kernel_threads_shutdown_requested() || state.ntp.invalid) state.failed = true;
            failed = state.failed;
            if (!failed && state.final_accepted && state.park == Park::NONE && state.readers == 0) finished = true;
            if (!failed && state.park_actor != nullptr) {
                actor = state.park_actor;
                epoch = state.image_epoch;
                index = state.next;
                boundary = state.boundary;
                if (index < ATTEMPTS) view = view_locked(index);
                if ((boundary == Boundary::BEFORE_DRIVER || boundary == Boundary::AFTER_DRIVER || state.driver_confirmed) &&
                    state.park == Park::EVIDENCE && !state.evidence_inflight && !state.ack && state.readers == 0) {
                    sample_evidence = true;
                } else if (state.park == Park::ACKED && state.ack)
                    sample_census = true;
            }
        }
        if (failed) {
            bool drained = false;
            {
                Guard guard;
                drained = !state.ack && state.readers == 0 && !state.evidence_inflight && state.park == Park::NONE;
            }
            if (!drained) {
                sched::kern_yield();
                continue;
            }
            // Do not stop workers or power off behind live attempt/recovery
            // debt. Fail evidence is permanent; future exact host stop owns it.
            CampaignSummary result;
            {
                Guard guard;
                result = summary_locked();
            }
            publish_campaign_result(result, false);
            release_subject_refs();
            return false;
        }
        if (finished) {
            CampaignSummary result;
            {
                Guard guard;
                result = summary_locked();
            }
            release_subject_refs();
            publish_campaign_result(result, true);
            return true;
        }
        if (sample_evidence) {
            // All eight real task contexts must be ready, not merely one BSP
            // or an AP startup count. Refusal does not run any backend gate.
            if (ker::mod::smt::get_core_count() != 8 || sched::selftest_task_context_ready_mask() != EIGHT_CPUS) {
                {
                    Guard guard;
                    state.evidence_inflight = false;
                    if (ker::mod::smt::get_core_count() != 8) state.failed = true;
                }
                sched::kern_yield();
                continue;
            }
            auto evidence_lease = acquire_evidence_lease();
            if (!evidence_lease) {
                sched::kern_yield();
                continue;
            }
            // Backend phase has no ACK and no issued census permission. Its
            // real typed execution hold survives fatal/timeout until return.
            GateResult evidence = GateResult::REFUSE;
            if (boundary == Boundary::BEFORE_DRIVER) {
                auto const SETTLED = ntp_boot_settlement();
                evidence = SETTLED;
                if (SETTLED == GateResult::ACCEPT) {
                    auto const WORKER = ker::vfs::selftest_prepare_dirty_writeback_worker();
                    if (WORKER == ker::vfs::SelftestWritebackPreparation::READY)
                        evidence = validate_prefork_evidence(evidence_lease);
                    else
                        evidence = WORKER == ker::vfs::SelftestWritebackPreparation::WAIT ? GateResult::WAIT : GateResult::REFUSE;
                }
            } else if (boundary == Boundary::AFTER_DRIVER) {
                Task* driver = nullptr;
                CampaignSummary result;
                {
                    Guard guard;
                    driver = std::exchange(state.driver, nullptr);
                    result = summary_locked();
                }
                if (driver != nullptr) driver->release();
                bool const SHAPE = result.warmups == WARMUPS && result.faults == FAULTS && result.successes == SUCCESSES &&
                                   result.consumed_faults == FAULTS && result.actual_wait9 == WARMUPS + SUCCESSES &&
                                   result.all_attempt_task_debts_empty && result.original_init_reaped_driver;
                if (SHAPE) evidence = validate_final_evidence(evidence_lease, result);
            } else {
                GateResult const ADMISSION = admit_driver_image_and_sharing(evidence_lease, epoch);
                evidence = ADMISSION == GateResult::ACCEPT ? validate_attempt_evidence(evidence_lease, view, boundary) : ADMISSION;
            }
            evidence_lease.reset();
            if (boundary == Boundary::BEFORE_DRIVER && evidence == GateResult::WAIT) sched::request_gc();
            Guard guard;
            if (state.park_actor != actor || state.next != index || state.image_epoch != epoch || state.park != Park::EVIDENCE)
                state.failed = true;
            if (evidence == GateResult::REFUSE) state.failed = true;
            if (!state.failed && evidence == GateResult::ACCEPT) {
                if (boundary != Boundary::BEFORE_DRIVER && boundary != Boundary::AFTER_DRIVER) state.image_admitted = true;
                state.park = Park::WANT_ACK;
            }
        } else if (sample_census) {
            auto lease = acquire_census_lease();
            if (lease) {
                // Reader APIs receive the unforgeable live permission. Full
                // A/B owner versions must be stable, never baseline-equal.
                GateResult CENSUS = GateResult::REFUSE;
                if (boundary == Boundary::BEFORE_DRIVER)
                    CENSUS = validate_host_baseline(lease);
                else if (boundary == Boundary::AFTER_DRIVER) {
                    CampaignSummary result;
                    {
                        Guard guard;
                        result = summary_locked();
                    }
                    CENSUS = validate_final_census(lease, result);
                } else
                    CENSUS = validate_quiescent_census(lease, view, boundary, boundary == Boundary::BEFORE_CREATE && index == WARMUPS);
                lease.reset();
                // Progress is requested only after the observation permission ends.
                if (CENSUS == GateResult::WAIT) sched::request_gc();
                Guard guard;
                if (state.next != index || state.park_actor != actor || state.image_epoch != epoch) state.failed = true;
                if (CENSUS == GateResult::REFUSE) state.failed = true;
                if (!state.failed && CENSUS == GateResult::ACCEPT && state.park == Park::ACKED && state.readers == 0) {
                    if (boundary == Boundary::BEFORE_DRIVER)
                        state.host_baseline = true;
                    else if (boundary == Boundary::AFTER_DRIVER)
                        state.final_accepted = true;
                    else {
                        auto& record = records.at(index);
                        auto const ACTUAL = view_locked(index);
                        if (!ACTUAL.task_debt_empty)
                            state.failed = true;
                        else if (boundary == Boundary::BEFORE_CREATE)
                            record.armed = true;
                        else {
                            bool const EXPECTED = boundary == Boundary::AFTER_FAILURE
                                                      ? ACTUAL.create_returned && ACTUAL.fault != Fault::NONE && ACTUAL.consumed &&
                                                            ACTUAL.create_result == ACTUAL.expected_error
                                                      : ACTUAL.fault == Fault::NONE && !ACTUAL.consumed && ACTUAL.create_returned &&
                                                            ACTUAL.create_result > 0 && ACTUAL.wait_consumed && ACTUAL.wait_status == 9 &&
                                                            ACTUAL.visible_pid == static_cast<uint64_t>(ACTUAL.create_result);
                            if (!EXPECTED || record.complete)
                                state.failed = true;
                            else {
                                record.complete = true;
                                ++state.next;
                            }
                        }
                    }
                    if (!state.failed) {
                        state.ack = false;
                        state.park = Park::RELEASE;
                    }
                }
            }
        }
        sched::kern_yield();
    }
}
void controller_entry() {
    bool passed = false;
    {
        // Voluntary backend waits must not let generic DAEMON stop abandon
        // execution leases/driver ACKs on this kernel stack.
        auto const HOLD = sched::hold_kernel_shutdown();
        passed = controller_loop();
    }
    if (passed) ker::mod::power::selftest_shutdown();
    sched::exit_current_kernel_thread();
}
}  // namespace

CensusLease::~CensusLease() { reset(); }
CensusLease::CensusLease(CensusLease&& other) noexcept
    : subject_(std::exchange(other.subject_, nullptr)),
      cookie_(std::exchange(other.cookie_, 0)),
      generation_(std::exchange(other.generation_, 0)) {}
auto CensusLease::operator=(CensusLease&& other) noexcept -> CensusLease& {
    if (this != &other) {
        reset();
        subject_ = std::exchange(other.subject_, nullptr);
        cookie_ = std::exchange(other.cookie_, 0);
        generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
}
void CensusLease::reset() noexcept {
    if (subject_ == nullptr) return;
    Guard guard;
    if (subject_ != state.park_actor || generation_ != state.park_generation || state.readers == 0)
        state.failed = true;
    else
        --state.readers;
    subject_ = nullptr;
    cookie_ = generation_ = 0;
}
auto acquire_census_lease() noexcept -> CensusLease {
    CensusLease out;
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    Guard guard;
    if (current == nullptr || current != state.controller || state.failed || state.park_actor == nullptr || !state.ack ||
        state.park != Park::ACKED || state.evidence_inflight)
        return out;
    if (state.readers == UINT64_MAX) {
        state.failed = true;
        return out;
    }
    ++state.readers;
    out.subject_ = state.park_actor;
    out.cookie_ = state.boundary == Boundary::BEFORE_DRIVER || state.boundary == Boundary::AFTER_DRIVER
                      ? state.park_generation
                      : records.at(state.next).value.cookie;
    out.generation_ = state.park_generation;
    return out;
}
EvidenceLease::~EvidenceLease() { reset(); }
EvidenceLease::EvidenceLease(EvidenceLease&& other) noexcept
    : subject_(std::exchange(other.subject_, nullptr)), generation_(std::exchange(other.generation_, 0)) {}
auto EvidenceLease::operator=(EvidenceLease&& other) noexcept -> EvidenceLease& {
    if (this != &other) {
        reset();
        subject_ = std::exchange(other.subject_, nullptr);
        generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
}
void EvidenceLease::reset() noexcept {
    if (subject_ == nullptr) return;
    Guard guard;
    if (subject_ != state.park_actor || generation_ != state.park_generation || !state.evidence_inflight)
        state.failed = true;
    else
        state.evidence_inflight = false;
    subject_ = nullptr;
    generation_ = 0;
}
auto acquire_evidence_lease() noexcept -> EvidenceLease {
    EvidenceLease out;
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    Guard guard;
    if (current == nullptr || current != state.controller || state.failed || state.park_actor == nullptr || state.ack ||
        state.park != Park::EVIDENCE || state.evidence_inflight || state.readers != 0)
        return out;
    state.evidence_inflight = true;
    out.subject_ = state.park_actor;
    out.generation_ = state.park_generation;
    return out;
}
void enable_boot_capture(bool enabled) noexcept {
    Guard guard;
    if (state.enabled || state.started || state.init != nullptr) {
        state.failed = true;
        return;
    }
    state.enabled = enabled;
}
void original_init_posted(Task& subject) noexcept {
    {
        Guard guard;
        if (!state.enabled) return;
    }
    if (subject.pid != 1 || subject.type != task::TaskType::PROCESS || subject.is_thread ||
        ker::mod::security::compartment::host_namespace_admission(&subject) != 0 || !retain_checked(subject)) {
        Guard guard;
        state.failed = true;
        return;
    }
    bool kept = false;
    {
        Guard guard;
        if (state.init == nullptr && !state.failed) {
            state.init = &subject;
            kept = true;
        } else
            state.failed = true;
    }
    if (!kept) subject.release();
}
void before_selected_service_fork(Task& parent) noexcept {
    {
        Guard guard;
        if (!state.enabled || &parent != state.init) return;
    }
    if (!sched::resource::selftest_exact_selected_service(parent, SERVICE_NAME)) return;
    // This is a post-pipe validation only. The genuine baseline was captured
    // by the earlier read-only init query, before ANY service pipe existed.
    Guard guard;
    if (!state.host_baseline || state.driver != nullptr || state.driver_service_slot != parent.selected_child_service ||
        state.init_boundary.generation != 1)
        state.failed = true;
}
void service_resource_read_completed(Task& actor, uint64_t slot, uint64_t result) noexcept {
    bool after = false;
    {
        Guard guard;
        if (!state.enabled || state.failed || &actor != state.init || result != 0 || state.park != Park::NONE) return;
        if (!state.host_baseline && state.driver == nullptr && state.next == 0)
            after = false;
        else if (state.host_baseline && state.driver_confirmed && state.init_reaped_driver && state.next == ATTEMPTS &&
                 !state.final_accepted)
            after = true;
        else
            return;  // In particular, a live-service READ never parks init.
    }
    if (!current_actor(actor) || !sched::resource::selftest_exact_unselected_service_slot(actor, slot, SERVICE_NAME)) return;
    ker::syscall::process::SelftestLifecycleInitBoundary observed{};
    if (!ker::syscall::process::selftest_lifecycle_init_boundary(slot, after, observed)) return;
    {
        Guard guard;
        if (state.failed || &actor != state.init || state.park != Park::NONE) return;
        if (!after) {
            if (state.host_baseline || state.driver != nullptr || state.next != 0) {
                state.failed = true;
                return;
            }
            state.driver_service_slot = slot;
            state.init_boundary = observed;
        } else if (!state.init_reaped_driver || state.next != ATTEMPTS || state.driver_service_slot != slot ||
                   observed.generation != state.init_boundary.generation || observed.journal_pid != state.init_boundary.journal_pid ||
                   observed.journal_generation != state.init_boundary.journal_generation ||
                   observed.status_sequence <= state.init_boundary.status_sequence) {
            state.failed = true;
            return;
        }
    }
    static_cast<void>(park_executor(actor, after ? Boundary::AFTER_DRIVER : Boundary::BEFORE_DRIVER));
}
void fork_will_publish(Task& parent, Task& child) noexcept {
    {
        Guard guard;
        if (!state.enabled) return;
        if (&parent == state.driver) {
            state.failed = true;
            return;
        }
        if (&parent != state.init) return;
    }
    if (!sched::resource::selftest_exact_service_child(parent, child, SERVICE_NAME)) return;
    if (child.type != task::TaskType::PROCESS || child.is_thread || !retain_checked(child)) {
        Guard guard;
        state.failed = true;
        return;
    }
    bool kept = false;
    {
        Guard guard;
        if (!state.failed && state.host_baseline && state.driver == nullptr && &parent == state.init && state.park == Park::NONE) {
            state.driver = &child;
            state.driver_pid = child.pid;
            state.driver_confirmed = false;
            kept = true;
        } else
            state.failed = true;
    }
    if (!kept) child.release();
}
void fork_publication_failed(Task& parent, Task& child) noexcept {
    Task* release = nullptr;
    {
        Guard guard;
        if (!state.enabled || &parent != state.init || &child != state.driver) return;
        state.failed = true;
        release = std::exchange(state.driver, nullptr);
        state.driver_pid = 0;
        state.driver_confirmed = false;
    }
    if (release != nullptr) release->release();
}
void successful_fork(Task& parent, Task& child) noexcept {
    Guard guard;
    if (!state.enabled || &parent != state.init || &child != state.driver) return;
    if (state.driver_confirmed)
        state.failed = true;
    else
        state.driver_confirmed = true;
}
void image_committed(Task& subject, bool remote) noexcept {
    Guard guard;
    if (!state.enabled || &subject != state.driver) return;
    if (remote || state.next != 0 || records.front().value.cookie != 0 || state.park != Park::NONE || state.image_admitted)
        state.failed = true;
    if (issue(state.image_epoch) == 0) state.failed = true;
}
void create_returned(Token token, int64_t result) noexcept {
    if (token.actor == nullptr || token.cookie == 0) return;
    Task* actor = nullptr;
    bool park = false;
    {
        Guard guard;
        if (state.failed || token.actor != state.driver || state.next >= ATTEMPTS) return;
        auto& record = records.at(state.next);
        if (record.value.cookie != token.cookie || !record.armed || record.value.create_returned) {
            state.failed = true;
            return;
        }
        record.armed = false;
        record.value.create_returned = true;
        record.value.create_result = result;
        actor = state.driver;
        park = result < 0;
        if ((record.value.fault == Fault::NONE && result <= 0) ||
            (record.value.fault != Fault::NONE && result != record.value.expected_error))
            state.failed = true;
    }
    if (park) static_cast<void>(park_executor(*actor, Boundary::AFTER_FAILURE));
}
void consumed_wait(Task& waiter, uint64_t subject, uint64_t visible, int32_t status, bool parent_exit, bool nowait) noexcept {
    bool park = false;
    {
        Guard guard;
        if (!state.enabled || nowait || !parent_exit) return;
        if (&waiter == state.init && subject == state.driver_pid && state.driver_pid != 0) {
            if (state.next != ATTEMPTS || status != 0 || state.init_reaped_driver)
                state.failed = true;
            else
                state.init_reaped_driver = true;
        } else {
            if (&waiter != state.driver || state.failed || state.next >= ATTEMPTS) return;
            auto& record = records.at(state.next);
            if (!record.value.create_returned || record.value.create_result <= 0 || record.value.wait_consumed ||
                visible != static_cast<uint64_t>(record.value.create_result) || status != 9 || record.value.fault != Fault::NONE) {
                state.failed = true;
                return;
            }
            record.value.subject_pid = subject;
            record.value.visible_pid = visible;
            record.value.wait_status = status;
            record.value.wait_consumed = true;
            park = true;
        }
    }
    // Init still owns supervisor pipes here. Record actual WAIT0 only;
    // its later committed/quiescent read query owns the final park.
    if (park) static_cast<void>(park_executor(waiter, Boundary::AFTER_WAIT));
}
void driver_exit_begin(Task& actor) noexcept {
    bool unsafe = false;
    {
        Guard guard;
        if (!state.enabled || &actor != state.driver) return;
        unsafe = state.ack || state.readers != 0 || state.evidence_inflight || state.park != Park::NONE;
        if (unsafe || state.next != ATTEMPTS) state.failed = true;
    }
    // This can only fire if a future exit path bypasses the execution-owned
    // kernel park protocol. Never destroy FDs behind a live census reader.
    if (unsafe) ker::mod::dbg::panic_handler("lifecycle60: driver cleanup behind execution-owned reader");
}
void allocated_attempt_task(Token token, Task* subject) noexcept {
    if (subject == nullptr || token.actor == nullptr || token.cookie == 0) return;
    Guard guard;
    if (state.failed || token.actor != state.driver || state.next >= ATTEMPTS) return;
    auto& record = records.at(state.next);
    if (record.value.cookie != token.cookie || !record.armed) {
        state.failed = true;
        return;
    }
    for (auto& debt : record.debts) {
        if (debt.address != nullptr || debt.deleting) continue;
        uint64_t const COOKIE = issue(state.next_deletion_cookie);
        if (COOKIE == 0) return;
        debt = {.address = subject, .deletion_cookie = COOKIE};
        return;
    }
    state.failed = true;
}
void ntp_boot_initializing() noexcept {
    Guard guard;
    // Recording is independent of state.enabled: phase ordering cannot erase it.
    if (state.ntp.init_seen) {
        state.ntp.invalid = true;
        return;
    }
    state.ntp.init_seen = true;
}
void ntp_boot_will_post(Task& subject) noexcept {
    Guard guard;
    auto& boot = state.ntp;
    if (!boot.init_seen || boot.address != nullptr || boot.cookie != 0 || boot.post_known) {
        boot.invalid = true;
        return;
    }
    boot.cookie = issue(state.next_ntp_cookie);
    if (boot.cookie == 0) {
        boot.invalid = true;
        return;
    }
    boot.address = &subject;
}
void ntp_boot_posted(bool posted) noexcept {
    Guard guard;
    auto& boot = state.ntp;
    if (!boot.init_seen || boot.cookie == 0 || boot.post_known) {
        boot.invalid = true;
        return;
    }
    // Worker may already be periodic/exiting/deleted: never overwrite that progress.
    boot.post_known = true;
    boot.posted = posted;
    if (!posted) boot.invalid = true;  // Immediate honest refusal, not an infinite WAIT.
}
void ntp_boot_creation_failed() noexcept {
    Guard guard;
    auto& boot = state.ntp;
    if (!boot.init_seen || boot.cookie != 0 || boot.address != nullptr) boot.invalid = true;
    boot.post_known = true;
    boot.invalid = true;
}
void ntp_boot_periodic() noexcept {
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    Guard guard;
    auto& boot = state.ntp;
    if (current == nullptr || boot.address != current || boot.cookie == 0 || boot.exiting || boot.deleting || boot.returned)
        boot.invalid = true;
    else
        boot.periodic = true;
}
void ntp_boot_exiting() noexcept {
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    Guard guard;
    auto& boot = state.ntp;
    if (current == nullptr || boot.address != current || boot.cookie == 0 || boot.periodic || boot.deleting || boot.returned)
        boot.invalid = true;
    else
        boot.exiting = true;
}
auto ntp_boot_settlement() noexcept -> GateResult {
    Guard guard;
    auto const& boot = state.ntp;
    if (boot.invalid) return GateResult::REFUSE;
    if (!boot.init_seen || !boot.post_known) return GateResult::WAIT;
    if (!boot.posted || boot.cookie == 0) return GateResult::REFUSE;
    if (boot.periodic && !boot.exiting && !boot.deleting && !boot.returned && boot.address != nullptr) return GateResult::ACCEPT;
    if (boot.exiting && boot.returned && !boot.deleting && boot.address == nullptr) return GateResult::ACCEPT;
    return GateResult::WAIT;
}
auto before_task_deallocation(void* address) noexcept -> TaskDeletionClaim {
    if (address == nullptr) return {};
    Guard guard;
    auto& boot = state.ntp;
    if (boot.address == address) {
        if (boot.cookie == 0 || boot.deleting || boot.returned) {
            boot.invalid = true;
            return {};
        }
        if (!boot.exiting && !(boot.post_known && !boot.posted)) boot.invalid = true;
        boot.deleting = true;
        boot.address = nullptr;  // No saved-address lookup after allocator return.
        return {.cookie = boot.cookie, .deletion_cookie = boot.cookie, .ntp_boot = true};
    }
    for (size_t i = 0; i < records.size(); ++i)
        for (auto& debt : records.at(i).debts) {
            if (debt.address != address) continue;
            if (debt.deleting) {
                state.failed = true;
                return {};
            }
            debt.deleting = true;
            debt.address = nullptr;  // no freed-address ABA lookup after allocator return
            return {.attempt = i, .cookie = records.at(i).value.cookie, .deletion_cookie = debt.deletion_cookie};
        }
    return {};
}
void after_task_deallocation(TaskDeletionClaim claim) noexcept {
    if (claim.deletion_cookie == 0) return;
    Guard guard;
    if (claim.ntp_boot) {
        auto& boot = state.ntp;
        if (claim.cookie != boot.cookie || claim.deletion_cookie != boot.cookie || !boot.deleting || boot.returned ||
            boot.address != nullptr)
            boot.invalid = true;
        else {
            boot.deleting = false;
            boot.returned = true;
        }
        return;
    }
    if (claim.attempt >= records.size()) {
        state.failed = true;
        return;
    }
    auto& record = records.at(claim.attempt);
    if (record.value.cookie != claim.cookie) {
        state.failed = true;
        return;
    }
    for (auto& debt : record.debts)
        if (debt.deletion_cookie == claim.deletion_cookie && debt.deleting && debt.address == nullptr) {
            debt.address = nullptr;
            debt.deleting = false;
            return;
        }
    state.failed = true;
}
auto start_scheduled_controller() -> bool {
    {
        Guard guard;
        if (!state.enabled || state.started || state.init == nullptr || state.failed) return false;
        state.started = true;
    }
    auto* worker = Task::create_kernel_thread("compartment_lifecycle60", controller_entry);
    if (worker == nullptr) return false;
    if (!sched::post_task_balanced(worker)) {
        task::destroy_unpublished_user_thread(worker);
        return false;
    }
    return true;
}
}  // namespace ker::test::lifecycle60

namespace ker::syscall::process {
auto exec_selftest_begin_native_construction_attempt(ker::mod::sched::task::Task& actor) noexcept -> NativeConstructionToken {
    namespace ctl = ker::test::lifecycle60;
    bool const CURRENT = ctl::current_actor(actor);
    {
        ctl::Guard guard;
        if (!ctl::state.enabled || &actor != ctl::state.driver) return {};
        if (ctl::state.failed || ctl::state.next >= ctl::ATTEMPTS || !CURRENT || ctl::state.park != ctl::Park::NONE) {
            ctl::state.failed = true;
            return {};
        }
        auto& record = ctl::records.at(ctl::state.next);
        if (record.value.cookie != 0) {
            ctl::state.failed = true;
            return {};
        }
        uint64_t const COOKIE = ctl::issue(ctl::state.next_cookie);
        if (COOKIE == 0) return {};
        NativeConstructionFault const FAULT = ctl::state.next >= ctl::WARMUPS && ctl::state.next < ctl::WARMUPS + ctl::FAULTS
                                                  ? ctl::stages.at(ctl::state.next - ctl::WARMUPS)
                                                  : NativeConstructionFault::NONE;
        record.value = {.index = ctl::state.next,
                        .cookie = COOKIE,
                        .fault = FAULT,
                        .expected_error = ctl::expected_error(FAULT),
                        .warmup = ctl::state.next < ctl::WARMUPS};
    }
    if (!ctl::park_executor(actor, ctl::Boundary::BEFORE_CREATE)) return {};
    ctl::Guard guard;
    if (ctl::state.failed || ctl::state.next >= ctl::ATTEMPTS) return {};
    auto const& record = ctl::records.at(ctl::state.next);
    return record.armed ? NativeConstructionToken{.actor = &actor, .cookie = record.value.cookie} : NativeConstructionToken{};
}
auto exec_selftest_consume_scoped_native_construction_fault(NativeConstructionToken token, NativeConstructionFault fault) noexcept -> bool {
    if (token.actor == nullptr || token.cookie == 0 || fault == NativeConstructionFault::NONE) return false;
    namespace ctl = ker::test::lifecycle60;
    ctl::Guard guard;
    if (ctl::state.failed || token.actor != ctl::state.driver || ctl::state.next >= ctl::ATTEMPTS) return false;
    auto& record = ctl::records.at(ctl::state.next);
    if (!record.armed || record.value.cookie != token.cookie || record.value.fault != fault) return false;
    bool expected = false;
    return record.consumed.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}
}  // namespace ker::syscall::process
#endif
