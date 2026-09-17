#include <cstddef>
#include <cstdint>
#include <memory>
#include <platform/sched/resource.hpp>
#include <platform/sys/mutex.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/process.hpp>
#include <test/ktest.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/sys/signal.hpp"

KTEST(SignalAbi, FrameAddressPreservesRedZone) {
    constexpr uint64_t USER_RSP = 0x7ffefffee548ULL;
    constexpr uint64_t FRAME_ADDR = ker::mod::sys::signal::signal_frame_address(USER_RSP);
    constexpr uint64_t RED_ZONE_LOW = USER_RSP - ker::mod::sys::signal::USER_RED_ZONE_SIZE;
    constexpr uint64_t FRAME_END = FRAME_ADDR + sizeof(ker::mod::sys::signal::SignalFrame);

    KEXPECT_EQ(FRAME_ADDR & 0xFULL, 8ULL);
    KEXPECT_TRUE(FRAME_END <= RED_ZONE_LOW);
}

KTEST(SignalAbi, FrameAddressMatchesSysvCallEntryAlignment) {
    constexpr uint64_t USER_RSP = 0x700000000000ULL;
    constexpr uint64_t FRAME_ADDR = ker::mod::sys::signal::signal_frame_address(USER_RSP);

    KEXPECT_EQ((FRAME_ADDR + 8) & 0xFULL, 0ULL);
}

KTEST(SyscallAbi, PerCpuEntryScratchOffset) {
    KEXPECT_EQ(offsetof(ker::mod::cpu::PerCpu, syscall_entry_tmp), static_cast<size_t>(0x38));
    KEXPECT_EQ(sizeof(ker::mod::cpu::PerCpu), static_cast<size_t>(64));
}

#ifdef WOS_SELFTEST
KTEST(SignalPending, RecipientSnapshotCapacityAndFatalCleanup) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_signal_snapshot_ownership());
}
KTEST(SignalPending, CurrentReceiverPagesBoundGrowthAndPublicationOverlap) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_signal_receiver_pages());
}
KTEST(SignalPending, GroupMembershipAndCurrentCredentialsShareAdmission) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_group_signal_authority());
}

namespace {
ker::mod::sched::task::Task pending_signal_test_task;
}

KTEST(SignalPending, GenerationCancelsOpposingJobControl) {
    auto& task = pending_signal_test_task;
    constexpr uint64_t CONT = uint64_t{1} << (18 - 1);
    constexpr uint64_t USR1 = uint64_t{1} << (10 - 1);
    constexpr uint64_t KILL = uint64_t{1} << (9 - 1);
    for (unsigned signo = 19; signo <= 22; ++signo) {
        uint64_t const STOP = uint64_t{1} << (signo - 1);
        task.signal_pending_store(CONT | USR1 | KILL);
        task.signal_generate_pending_mask(STOP);
        KEXPECT_EQ(task.signal_pending_bits(), STOP | USR1 | KILL);
        task.signal_generate_pending_mask(CONT);
        KEXPECT_EQ(task.signal_pending_bits(), CONT | USR1 | KILL);
    }
    task.signal_pending_store(0);
}

KTEST(SignalPending, RetiredOwnerOccurrenceSurvivesRecipientAbandonment) {
    using Task = ker::mod::sched::task::Task;
    auto owner = std::make_unique<Task>();
    auto first = std::make_unique<Task>();
    auto second = std::make_unique<Task>();
    owner->type = ker::mod::sched::task::TaskType::PROCESS;
    owner->pid = 0x7B70;
    first->type = second->type = owner->type;
    first->is_thread = second->is_thread = true;
    first->owner_pid = second->owner_pid = owner->pid;
    KEXPECT_TRUE(ker::mod::sched::resource::bind_thread_owner(*first, *owner));
    KEXPECT_TRUE(ker::mod::sched::resource::bind_thread_owner(*second, *owner));
    constexpr uint64_t USR1 = uint64_t{1} << 9;
    constexpr uint64_t KILL_STOP = (uint64_t{1} << 8) | (uint64_t{1} << 18);
    owner->signal_add_pending_mask(USR1 | KILL_STOP);
    KEXPECT_EQ(first->signal_delivery_pending_bits(), 0U);
    owner->main_thread_retired.store(true, std::memory_order_release);
    KEXPECT_EQ(first->signal_delivery_pending_bits(), USR1);
    {
        auto preparation = first->claim_signal_for_delivery(USR1);
        KEXPECT_TRUE(static_cast<bool>(preparation));
        KEXPECT_EQ(second->signal_deliverable_bits(), 0U);
        KEXPECT_EQ(owner->signal_pending_bits(), USR1 | KILL_STOP);
    }
    first.reset();
    KEXPECT_EQ(second->signal_deliverable_bits(), USR1);
    KEXPECT_TRUE(second->consume_signal_for_delivery(USR1));
    KEXPECT_EQ(owner->signal_pending_bits(), KILL_STOP);
    KEXPECT_FALSE(second->consume_signal_for_delivery(USR1));
    owner->signal_pending_store(0);
}

KTEST(SignalPending, ProcessTimerSignalCanReachWorkerBeforeLeaderRetirement) {
    using Task = ker::mod::sched::task::Task;
    auto owner = std::make_unique<Task>();
    auto worker = std::make_unique<Task>();
    auto sibling = std::make_unique<Task>();
    owner->type = worker->type = sibling->type = ker::mod::sched::task::TaskType::PROCESS;
    owner->pid = 0x7B79;
    worker->is_thread = sibling->is_thread = true;
    worker->owner_pid = sibling->owner_pid = owner->pid;
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(*worker, *owner));
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(*sibling, *owner));
    constexpr uint64_t ALARM = uint64_t{1} << 13;
    owner->signal_mask_store(ALARM);
    owner->timer_sig_pending.store(ALARM);
    KEXPECT_EQ(owner->signal_deliverable_bits(), 0U);
    KEXPECT_EQ(worker->signal_delivery_pending_bits(), ALARM);
    {
        auto preparation = worker->claim_signal_for_delivery(ALARM);
        KEXPECT_TRUE(static_cast<bool>(preparation));
        KEXPECT_EQ(sibling->signal_deliverable_bits(), 0U);
    }
    worker.reset();
    KEXPECT_EQ(sibling->signal_deliverable_bits(), ALARM);
    KEXPECT_TRUE(sibling->consume_signal_for_delivery(ALARM));
    KEXPECT_EQ(owner->timer_sig_pending.load(), 0U);
    KEXPECT_FALSE(owner->consume_signal_for_delivery(ALARM));
    owner->main_thread_retired.store(true);
    owner->timer_sig_pending.store(ALARM);
    KEXPECT_TRUE(sibling->consume_signal_for_delivery(ALARM));
}

KTEST(SignalPending, LocalAndSharedOccurrencesRemainSeparate) {
    using Task = ker::mod::sched::task::Task;
    auto owner = std::make_unique<Task>();
    auto worker = std::make_unique<Task>();
    owner->type = worker->type = ker::mod::sched::task::TaskType::PROCESS;
    owner->pid = 0x7B71;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_TRUE(ker::mod::sched::resource::bind_thread_owner(*worker, *owner));
    owner->main_thread_retired.store(true, std::memory_order_release);
    constexpr uint64_t USR1 = uint64_t{1} << 9;
    owner->signal_add_pending_mask(USR1);
    worker->signal_add_pending_mask(USR1);
    worker->signal_mask_store(USR1);
    KEXPECT_EQ(worker->signal_delivery_pending_bits(), USR1);
    KEXPECT_EQ(worker->signal_deliverable_bits(), 0U);
    worker->signal_mask_store(0);
    KEXPECT_TRUE(worker->consume_signal_for_delivery(USR1));
    KEXPECT_EQ(worker->signal_pending_bits(), 0U);
    KEXPECT_EQ(owner->signal_pending_bits(), USR1);
    KEXPECT_TRUE(worker->consume_signal_for_delivery(USR1));
    KEXPECT_FALSE(worker->consume_signal_for_delivery(USR1));
}

KTEST(SignalDisposition, ExistingWorkerUsesProcessStoreAndStableValues) {
    using Task = ker::mod::sched::task::Task;
    auto owner = std::make_unique<Task>();
    auto worker = std::make_unique<Task>();
    owner->type = worker->type = ker::mod::sched::task::TaskType::PROCESS;
    owner->pid = 0x7B72;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_TRUE(ker::mod::sched::resource::bind_thread_owner(*worker, *owner));
    constexpr unsigned INDEX = 9;
    constexpr Task::SigHandler FIRST{.handler = 0x1000, .flags = 2, .restorer = 0x3000, .mask = 4};
    constexpr Task::SigHandler SECOND{.handler = 0x5000, .flags = 6, .restorer = 0x7000, .mask = 8};
    {
        ker::mod::sys::MutexGuard guard(owner->signal_action_mutex());
        owner->publish_signal_handler(INDEX, FIRST);
    }
    auto const BEFORE = worker->signal_handler_snapshot(INDEX);
    {
        ker::mod::sys::MutexGuard guard(worker->signal_action_mutex());
        worker->publish_signal_handler(INDEX, SECOND);
    }
    auto const AFTER = owner->signal_handler_snapshot(INDEX);
    KEXPECT_EQ(BEFORE.handler, FIRST.handler);
    KEXPECT_EQ(BEFORE.flags, FIRST.flags);
    KEXPECT_EQ(BEFORE.restorer, FIRST.restorer);
    KEXPECT_EQ(BEFORE.mask, FIRST.mask);
    KEXPECT_EQ(AFTER.handler, SECOND.handler);
    KEXPECT_EQ(AFTER.flags, SECOND.flags);
    KEXPECT_EQ(AFTER.restorer, SECOND.restorer);
    KEXPECT_EQ(AFTER.mask, SECOND.mask);
    owner->main_thread_retired.store(true);
    KEXPECT_EQ(worker->signal_handler_snapshot(INDEX).handler, SECOND.handler);
    worker.reset();
    KEXPECT_EQ(owner->signal_handler_snapshot(INDEX).handler, SECOND.handler);
}

KTEST(SignalDisposition, ForkSnapshotAndResetDoNotMutateAnotherProcess) {
    using Task = ker::mod::sched::task::Task;
    auto parent = std::make_unique<Task>();
    auto child = std::make_unique<Task>();
    constexpr unsigned INDEX = 9;
    {
        ker::mod::sys::MutexGuard guard(parent->signal_action_mutex());
        parent->publish_signal_handler(INDEX, {.handler = 0x1000, .flags = 2, .restorer = 0x3000, .mask = 4});
    }
    child->initialize_signal_handlers(parent->signal_handlers_snapshot());
    {
        ker::mod::sys::MutexGuard guard(parent->signal_action_mutex());
        parent->reset_signal_state_for_exec();
    }
    KEXPECT_EQ(parent->signal_handler_snapshot(INDEX).handler, 0U);
    auto const INHERITED = child->signal_handler_snapshot(INDEX);
    KEXPECT_EQ(INHERITED.handler, 0x1000U);
    KEXPECT_EQ(INHERITED.flags, 2U);
    KEXPECT_EQ(INHERITED.restorer, 0x3000U);
    KEXPECT_EQ(INHERITED.mask, 4U);
}

KTEST(SignalDisposition, ExecPreservesIgnoredPendingAndMaskButDropsOldImageState) {
    using Task = ker::mod::sched::task::Task;
    auto task = std::make_unique<Task>();
    constexpr uint64_t PENDING = uint64_t{1} << 11;
    constexpr uint64_t MASK = PENDING | (uint64_t{1} << 14);
    Task::SignalHandlerTable handlers{};
    for (unsigned index = 0; index < Task::MAX_SIGNALS; ++index) {
        handlers[index] = {
            .handler = index % 3 == 0 ? 1U : (index % 3 == 1 ? 0U : 0x1000U), .flags = 0x08000000, .restorer = 0x2000, .mask = MASK};
    }
    task->initialize_signal_handlers(handlers);
    task->signal_pending_store(PENDING);
    task->signal_mask_store(MASK);
    task->in_signal_handler = true;
    task->do_sigreturn = true;
    task->sigsuspend_active = true;
    task->sigsuspend_saved_mask = 0xF;
    task->sigaltstack_sp = 0x100000;
    task->sigaltstack_size = 65536;
    task->sigaltstack_flags = 0;
    {
        ker::mod::sys::MutexGuard guard(task->signal_action_mutex());
        task->reset_signal_state_for_exec();
    }
    for (unsigned index = 0; index < Task::MAX_SIGNALS; ++index) {
        auto const HANDLER = task->signal_handler_snapshot(index);
        KEXPECT_EQ(HANDLER.handler, index % 3 == 0 ? 1U : 0U);
        KEXPECT_EQ(HANDLER.flags, 0U);
        KEXPECT_EQ(HANDLER.restorer, 0U);
        KEXPECT_EQ(HANDLER.mask, 0U);
    }
    KEXPECT_EQ(task->signal_pending_bits(), PENDING);
    KEXPECT_EQ(task->signal_mask_bits(), MASK);
    KEXPECT_FALSE(task->in_signal_handler);
    KEXPECT_FALSE(task->do_sigreturn);
    KEXPECT_FALSE(task->sigsuspend_active);
    KEXPECT_EQ(task->sigsuspend_saved_mask, 0U);
    KEXPECT_EQ(task->sigaltstack_sp, 0U);
    KEXPECT_EQ(task->sigaltstack_size, 0U);
    KEXPECT_EQ(task->sigaltstack_flags, 2U);
}

KTEST(SignalPending, PublicationRetryPreservesNewContinue) {
    auto& task = pending_signal_test_task;
    constexpr uint64_t CONT = uint64_t{1} << (18 - 1);
    constexpr uint64_t STOP = uint64_t{1} << (19 - 1);
    task.signal_pending_store(0);
    task.signal_generate_pending_mask(STOP);
    task.signal_generate_pending_mask(CONT);
    task.signal_add_pending_mask(STOP);
    KEXPECT_EQ(task.signal_pending_bits(), STOP | CONT);
    task.signal_pending_store(0);
}

KTEST(JobControlMembership, DestructionDetachesBeforeRetainedOwnerRelease) {
    using namespace ker::mod::sched;
    auto owner = std::make_unique<task::Task>();
    auto worker = std::make_unique<task::Task>();
    owner->pid = 410001;
    owner->type = task::TaskType::PROCESS;
    KEXPECT_FALSE(worker->jobctl_membership.bind(*owner));
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    worker->pid = 410002;
    worker->type = task::TaskType::PROCESS;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_TRUE(resource::bind_thread_owner(*worker, *owner));
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    KEXPECT_FALSE(worker->jobctl_membership.bind(*worker));
    KEXPECT_EQ(owner->jobctl_barrier.members(), 2U);
    KEXPECT_EQ(owner->jobctl_barrier.quiescent_members(), 0U);
    KEXPECT_EQ(owner->ref_count.load(), 2U);
    owner->state.store(task::TaskState::EXITING);
    worker.reset();
    KEXPECT_EQ(owner->jobctl_barrier.members(), 1U);
    KEXPECT_EQ(owner->resource_thread_owners.load(), 0U);
    KEXPECT_EQ(owner->ref_count.load(), 1U);
    owner->jobctl_membership.detach();
    owner->jobctl_membership.detach();
    KEXPECT_EQ(owner->jobctl_barrier.members(), 0U);
}

KTEST(JobControlMembership, NewThreadJoinsActiveStopWithoutExecutionPermission) {
    using namespace ker::mod::sched;
    auto owner = std::make_unique<task::Task>();
    auto worker = std::make_unique<task::Task>();
    owner->pid = 410011;
    owner->type = task::TaskType::PROCESS;
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    worker->pid = 410012;
    worker->type = task::TaskType::PROCESS;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_TRUE(resource::bind_thread_owner(*worker, *owner));
    // These unpublished objects are exclusively owned by this test.
    KEXPECT_TRUE(owner->jobctl_barrier.request_stop(19).has_value());
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    KEXPECT_TRUE(worker->jobctl_stopped.load());
    KEXPECT_EQ(worker->jobctl_stop_signal, 19U);
    KEXPECT_EQ(owner->jobctl_barrier.members(), 2U);
    KEXPECT_EQ(owner->jobctl_barrier.quiescent_members(), 1U);
    KEXPECT_FALSE(owner->jobctl_barrier.complete());
    worker.reset();
    KEXPECT_EQ(owner->jobctl_barrier.members(), 1U);
    KEXPECT_EQ(owner->jobctl_barrier.quiescent_members(), 0U);
}

KTEST(JobControlMembership, UnboundThreadRejectsAndPublicationRollbackAllowsRetry) {
    using namespace ker::mod::sched;
    auto subject = std::make_unique<task::Task>();
    subject->pid = 410021;
    subject->type = task::TaskType::PROCESS;
    subject->is_thread = true;
    subject->owner_pid = 410020;
    KEXPECT_FALSE(subject->jobctl_membership.bind(*subject));
    KEXPECT_EQ(subject->jobctl_barrier.members(), 0U);
    subject->is_thread = false;
    subject->owner_pid = 0;
    KEXPECT_TRUE(subject->jobctl_membership.bind(*subject));
    KEXPECT_EQ(subject->jobctl_barrier.members(), 1U);
    subject->jobctl_membership.rollback_publication(*subject);
    KEXPECT_EQ(subject->jobctl_barrier.members(), 0U);
    KEXPECT_TRUE(subject->jobctl_membership.bind(*subject));
    KEXPECT_EQ(subject->jobctl_barrier.members(), 1U);
    KEXPECT_EQ(subject->ref_count.load(), 1U);
}

KTEST(JobControlMembership, FailedStoppedAdmissionRestoresGateBeforeRetry) {
    using namespace ker::mod::sched;
    auto owner = std::make_unique<task::Task>();
    auto worker = std::make_unique<task::Task>();
    owner->pid = 410031;
    owner->type = task::TaskType::PROCESS;
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    worker->pid = 410032;
    worker->type = task::TaskType::PROCESS;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_TRUE(resource::bind_thread_owner(*worker, *owner));
    KEXPECT_TRUE(owner->jobctl_barrier.request_stop(19).has_value());
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    KEXPECT_TRUE(worker->jobctl_stopped.load());
    worker->jobctl_membership.rollback_publication(*worker);
    KEXPECT_FALSE(worker->jobctl_stopped.load());
    KEXPECT_EQ(worker->jobctl_stop_signal, 0U);
    KEXPECT_EQ(owner->jobctl_barrier.members(), 1U);
    KEXPECT_EQ(owner->jobctl_barrier.quiescent_members(), 0U);
    // Rollback preserves an earlier local stop rather than always clearing it.
    worker->jobctl_stopped.store(true);
    worker->jobctl_stop_signal = 20;
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    KEXPECT_EQ(worker->jobctl_stop_signal, 19U);
    worker->jobctl_membership.rollback_publication(*worker);
    KEXPECT_TRUE(worker->jobctl_stopped.load());
    KEXPECT_EQ(worker->jobctl_stop_signal, 20U);
    worker->jobctl_stopped.store(false);
    worker->jobctl_stop_signal = 0;
    KEXPECT_TRUE(owner->jobctl_barrier.request_continue());
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    KEXPECT_FALSE(worker->jobctl_stopped.load());
    KEXPECT_EQ(owner->jobctl_barrier.members(), 2U);
    KEXPECT_EQ(owner->jobctl_barrier.quiescent_members(), 0U);
}

namespace {
namespace jc = ker::mod::sched::job_control;
namespace ce = ker::syscall::process::child_events;
constexpr uint64_t CONTROLLER_STOP = uint64_t{1} << 18;
constexpr uint64_t CONTROLLER_CONT = uint64_t{1} << 17;
struct ControllerFixture {
    using Task = ker::mod::sched::task::Task;
    std::unique_ptr<Task> parent = std::make_unique<Task>();
    std::unique_ptr<Task> owner = std::make_unique<Task>();
    std::unique_ptr<Task> worker = std::make_unique<Task>();
    bool initialized = false;
    ControllerFixture() {
        using namespace ker::mod::sched;
        parent->type = owner->type = worker->type = task::TaskType::PROCESS;
        parent->pid = 410041;
        owner->pid = 410042;
        owner->initialize_job_identity({.pgid = owner->pid});
        worker->pid = 410043;
        worker->is_thread = true;
        worker->owner_pid = owner->pid;
        initialized = owner->jobctl_membership.bind(*owner) && resource::bind_thread_owner(*worker, *owner) &&
                      worker->jobctl_membership.bind(*worker);
        // Private fixtures have no scheduler visibility. EXITING prevents the
        // production child-event wake path from admitting them to a runqueue.
        parent->state.store(task::TaskState::EXITING);
        owner->state.store(task::TaskState::EXITING);
        worker->state.store(task::TaskState::EXITING);
        initialized = initialized && ce::begin_publication(*parent, *owner);
    }
    ~ControllerFixture() { ce::abort_publication(*owner); }
    void stop(Task& subject) {
        KEXPECT_TRUE(jc::begin_stop(subject, 19) == jc::StopDelivery::MANAGED);
        KEXPECT_TRUE(subject.jobctl_stopped.load());
        jc::acknowledge_stopped(subject);
    }
    auto consume(int options, int expected_status) -> bool {
        ce::ClaimedEvent event{};
        if (ce::claim_or_register(*parent, *parent, owner->pid, options, false, event) != ce::ProbeResult::CLAIMED) return false;
        bool const MATCH = event.status == expected_status;
        return ce::commit_claim(*parent, *parent, event, false) && MATCH;
    }
};
}  // namespace

KTEST(JobControlController, AllMembersBeforeStopAndSingleContinueRecipient) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    KEXPECT_EQ(f.owner->signal_pending_bits(), CONTROLLER_STOP);
    KEXPECT_EQ(f.worker->signal_pending_bits(), CONTROLLER_STOP);
    f.stop(*f.owner);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
    KEXPECT_FALSE(f.owner->jobctl_barrier.complete());
    f.stop(*f.worker);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(f.owner->child_job_stop_event.queued);
    KEXPECT_TRUE(f.owner->jobctl_barrier.complete());
    KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, (19 << 8) | 0x7f));
    f.worker->signal_generate_pending_mask(CONTROLLER_CONT);
    KEXPECT_EQ(f.owner->signal_pending_bits(), 0ULL);
    KEXPECT_EQ(f.worker->signal_pending_bits(), CONTROLLER_CONT);
    KEXPECT_TRUE(f.owner->jobctl_barrier.stopping());
    KEXPECT_FALSE(jc::resume(*f.owner).value_or(true));
    ce::advance_process_job_control(*f.owner);
    KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
    KEXPECT_TRUE(f.consume(ce::OPTION_CONTINUED, 0xffff));
    KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    KEXPECT_FALSE(f.owner->jobctl_stopped.load());
    KEXPECT_FALSE(f.worker->jobctl_stopped.load());
    KEXPECT_EQ(f.owner->jobctl_barrier.quiescent_members(), 0U);
}

KTEST(JobControlController, ContinueCancelsIncompleteStopWithoutFalseEvent) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    f.stop(*f.owner);
    f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
    KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
    KEXPECT_FALSE(f.owner->child_continued_event.queued);
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 19) == jc::StopDelivery::CANCELED);
    KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    KEXPECT_FALSE(f.owner->jobctl_stopped.load());
    KEXPECT_EQ(f.owner->jobctl_barrier.quiescent_members(), 0U);
}

KTEST(JobControlController, ContinuedEventBackpressureAndKillEscape) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    f.stop(*f.owner);
    f.stop(*f.worker);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, (19 << 8) | 0x7f));
    f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    KEXPECT_TRUE(f.owner->child_continued_event.queued);
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    f.stop(*f.owner);
    f.stop(*f.worker);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
    KEXPECT_TRUE(f.owner->jobctl_stop_publish_deferred.load());
    f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(f.owner->jobctl_barrier.stopping());
    KEXPECT_FALSE(jc::resume(*f.worker).value_or(true));
    KEXPECT_TRUE(f.consume(ce::OPTION_CONTINUED, 0xffff));
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(f.owner->child_job_stop_event.queued);
    KEXPECT_TRUE(f.owner->child_continued_event.queued);
    KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
    KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, (19 << 8) | 0x7f));
    KEXPECT_TRUE(f.consume(ce::OPTION_CONTINUED, 0xffff));
    KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    f.stop(*f.owner);
    f.stop(*f.worker);
    f.worker->signal_generate_pending_mask(uint64_t{1} << 8);
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    KEXPECT_FALSE(f.worker->jobctl_stopped.load());
    KEXPECT_TRUE(f.owner->jobctl_barrier.stopping());
    KEXPECT_FALSE(f.owner->child_continued_event.queued);
}

KTEST(JobControlController, FailedPublicationRestoresOnlyJobControlBits) {
    using namespace ker::mod::sched;
    auto owner = std::make_unique<task::Task>();
    auto worker = std::make_unique<task::Task>();
    owner->pid = 410051;
    owner->type = worker->type = task::TaskType::PROCESS;
    worker->pid = 410052;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    constexpr uint64_t USR1 = uint64_t{1} << 9;
    constexpr uint64_t KILL = uint64_t{1} << 8;
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    KEXPECT_TRUE(resource::bind_thread_owner(*worker, *owner));
    worker->signal_pending_store(CONTROLLER_CONT | USR1);
    KEXPECT_TRUE(worker->jobctl_membership.bind(*worker));
    owner->signal_generate_pending_mask(CONTROLLER_STOP);
    worker->signal_generate_pending_mask(KILL);
    KEXPECT_EQ(worker->signal_pending_bits(), CONTROLLER_STOP | USR1 | KILL);
    worker->jobctl_membership.rollback_publication(*worker);
    KEXPECT_EQ(worker->signal_pending_bits(), CONTROLLER_CONT | USR1 | KILL);
    KEXPECT_EQ(owner->jobctl_barrier.members(), 1U);
    worker.reset();
    owner->signal_generate_pending_mask(CONTROLLER_CONT);
    KEXPECT_EQ(owner->signal_pending_bits(), CONTROLLER_CONT);
    KEXPECT_EQ(owner->ref_count.load(), 1U);
}

KTEST(JobControlController, ContinueBetweenStopGateAndAcknowledgment) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->signal_generate_pending_mask(CONTROLLER_STOP);
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 19) == jc::StopDelivery::MANAGED);
    KEXPECT_TRUE(f.worker->jobctl_stopped.load());
    KEXPECT_FALSE(f.owner->jobctl_barrier.complete());
    f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    jc::acknowledge_stopped(*f.worker);
    KEXPECT_FALSE(f.worker->jobctl_stopped.load());
    KEXPECT_FALSE(f.worker->is_voluntary_blocked());
    KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
    KEXPECT_FALSE(f.owner->child_continued_event.queued);
    KEXPECT_EQ(f.owner->jobctl_barrier.quiescent_members(), 0U);
}

KTEST(JobControlController, CatchableGenerationDefersStopAndContinueCancelsIt) {
    for (unsigned signal = 20; signal <= 22; ++signal) {
        ControllerFixture f;
        KEXPECT_TRUE(f.initialized);
        uint64_t const MASK = uint64_t{1} << (signal - 1);
        f.owner->signal_mask_store(MASK);
        f.owner->signal_generate_pending_mask(MASK);
        KEXPECT_EQ(f.owner->signal_pending_bits(), MASK);
        KEXPECT_EQ(f.owner->signal_deliverable_bits(), 0ULL);
        KEXPECT_EQ(f.worker->signal_pending_bits(), 0ULL);
        KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
        KEXPECT_FALSE(f.owner->jobctl_stopped.load());
        f.worker->signal_generate_pending_mask(CONTROLLER_CONT);
        f.owner->signal_mask_store(0);
        KEXPECT_EQ(f.owner->signal_pending_bits(), 0ULL);
        KEXPECT_TRUE(jc::begin_stop(*f.owner, static_cast<int>(signal)) == jc::StopDelivery::CANCELED);
        KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
        KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
    }
}

KTEST(JobControlController, DefaultActionStopsEveryMemberWithOriginalReason) {
    for (unsigned signal = 20; signal <= 22; ++signal) {
        ControllerFixture f;
        KEXPECT_TRUE(f.initialized);
        uint64_t const MASK = uint64_t{1} << (signal - 1);
        f.owner->signal_generate_pending_mask(MASK);
        f.owner->signal_clear_pending_mask(MASK);
        KEXPECT_TRUE(jc::begin_stop(*f.owner, static_cast<int>(signal)) == jc::StopDelivery::MANAGED);
        KEXPECT_EQ(f.owner->jobctl_stop_signal, signal);
        KEXPECT_EQ(f.worker->signal_pending_bits(), CONTROLLER_STOP);
        jc::acknowledge_stopped(*f.owner);
        ce::advance_process_job_control(*f.owner);
        KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
        f.stop(*f.worker);
        KEXPECT_EQ(f.worker->jobctl_stop_signal, signal);
        ce::advance_process_job_control(*f.owner);
        KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, static_cast<int>((signal << 8) | 0x7f)));
        f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
        ce::advance_process_job_control(*f.owner);
        KEXPECT_TRUE(f.consume(ce::OPTION_CONTINUED, 0xffff));
        KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
        KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    }
}

KTEST(JobControlController, NewUncatchableStopDoesNotReviveCanceledDefaultAction) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->signal_generate_pending_mask(uint64_t{1} << 19);
    f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
    f.worker->signal_generate_pending_mask(CONTROLLER_STOP);
    KEXPECT_TRUE(jc::begin_stop(*f.owner, 20) == jc::StopDelivery::CANCELED);
    KEXPECT_TRUE(f.owner->jobctl_barrier.stopping());
    KEXPECT_EQ(f.owner->jobctl_barrier.stop_signal(), 19U);
    f.stop(*f.owner);
    f.stop(*f.worker);
    ce::advance_process_job_control(*f.owner);
    KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, (19 << 8) | 0x7f));
}

KTEST(JobControlController, RetiredOwnerDefaultStopUsesClaimedMembership) {
    for (unsigned signal = 20; signal <= 22; ++signal) {
        ControllerFixture f;
        KEXPECT_TRUE(f.initialized);
        f.owner->main_thread_retired.store(true);
        uint64_t const MASK = uint64_t{1} << (signal - 1);
        f.owner->signal_generate_pending_mask(MASK);
        auto claim = f.worker->claim_signal_for_delivery(MASK);
        KEXPECT_TRUE(f.owner->signal_claim_matches(claim, MASK));
        KEXPECT_TRUE(jc::begin_stop(*f.worker, static_cast<int>(signal), &claim) == jc::StopDelivery::MANAGED);
        KEXPECT_FALSE(static_cast<bool>(claim));
        KEXPECT_EQ(f.owner->signal_pending_bits() & MASK, 0U);
        KEXPECT_EQ(f.owner->jobctl_barrier.stop_signal(), signal);
        KEXPECT_TRUE(f.worker->jobctl_stopped.load());
        f.stop(*f.owner);
        jc::acknowledge_stopped(*f.worker);
        ce::advance_process_job_control(*f.owner);
        KEXPECT_TRUE(f.consume(ce::OPTION_STOPPED, static_cast<int>((signal << 8) | 0x7f)));
        f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
        ce::advance_process_job_control(*f.owner);
        KEXPECT_TRUE(jc::resume(*f.owner).value_or(false));
        KEXPECT_TRUE(jc::resume(*f.worker).value_or(false));
    }
}

KTEST(JobControlController, ContinueCancelsReservedRetiredOwnerStop) {
    for (unsigned signal = 20; signal <= 22; ++signal) {
        ControllerFixture f;
        KEXPECT_TRUE(f.initialized);
        f.owner->main_thread_retired.store(true);
        uint64_t const MASK = uint64_t{1} << (signal - 1);
        f.owner->signal_generate_pending_mask(MASK);
        auto claim = f.worker->claim_signal_for_delivery(MASK);
        KEXPECT_TRUE(static_cast<bool>(claim));
        f.owner->signal_generate_pending_mask(CONTROLLER_CONT);
        KEXPECT_TRUE(jc::begin_stop(*f.worker, static_cast<int>(signal), &claim) == jc::StopDelivery::CANCELED);
        KEXPECT_FALSE(static_cast<bool>(claim));
        KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
        KEXPECT_FALSE(f.worker->jobctl_stopped.load());
        KEXPECT_FALSE(f.owner->child_job_stop_event.queued);
        KEXPECT_EQ(f.owner->signal_pending_bits() & MASK, 0U);
        KEXPECT_EQ(f.owner->sig_pending_claimed.load(), 0U);
    }
}

KTEST(JobControlController, LocalAndSharedStopMarkersRemainSeparate) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    f.owner->main_thread_retired.store(true);
    constexpr uint64_t MASK = uint64_t{1} << 19;
    f.owner->signal_generate_pending_mask(MASK);
    f.worker->signal_generate_pending_mask(MASK);
    auto local = f.worker->claim_signal_for_delivery(MASK);
    KEXPECT_TRUE(f.worker->signal_claim_matches(local, MASK));
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 20, &local) == jc::StopDelivery::MANAGED);
    KEXPECT_EQ(f.worker->signal_pending_bits() & MASK, 0U);
    KEXPECT_EQ(f.owner->signal_pending_bits() & MASK, MASK);
    auto shared = f.worker->claim_signal_for_delivery(MASK);
    KEXPECT_TRUE(f.owner->signal_claim_matches(shared, MASK));
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 20, &shared) == jc::StopDelivery::MANAGED);
    KEXPECT_EQ(f.owner->signal_pending_bits() & MASK, 0U);
    KEXPECT_FALSE(static_cast<bool>(shared));
}

KTEST(JobControlController, ForeignOrWrongSignalClaimCannotPublishStop) {
    ControllerFixture f;
    KEXPECT_TRUE(f.initialized);
    auto foreign = std::make_unique<ControllerFixture::Task>();
    constexpr uint64_t MASK = uint64_t{1} << 19;
    foreign->signal_add_pending_mask(MASK);
    auto claim = foreign->claim_signal_for_delivery(MASK);
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 20, &claim) == jc::StopDelivery::CANCELED);
    KEXPECT_EQ(foreign->signal_pending_bits(), MASK);
    KEXPECT_TRUE(static_cast<bool>(claim));
    f.worker->signal_generate_pending_mask(MASK);
    auto local = f.worker->claim_signal_for_delivery(MASK);
    KEXPECT_TRUE(jc::begin_stop(*f.worker, 21, &local) == jc::StopDelivery::CANCELED);
    KEXPECT_EQ(f.worker->signal_pending_bits(), MASK);
    KEXPECT_FALSE(f.owner->jobctl_barrier.stopping());
    KEXPECT_FALSE(f.worker->jobctl_stopped.load());
}

KTEST(JobControlController, PublicationRetryPreservesEarlierCatchableStop) {
    using namespace ker::mod::sched;
    auto owner = std::make_unique<task::Task>();
    owner->pid = 410061;
    owner->type = task::TaskType::PROCESS;
    constexpr uint64_t TSTP = uint64_t{1} << 19;
    owner->signal_generate_pending_mask(TSTP);
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    owner->signal_generate_pending_mask(CONTROLLER_CONT);
    owner->jobctl_membership.rollback_publication(*owner);
    KEXPECT_EQ(owner->signal_pending_bits(), TSTP);
    KEXPECT_TRUE(owner->jobctl_membership.bind(*owner));
    owner->signal_clear_pending_mask(TSTP);
    KEXPECT_TRUE(jc::begin_stop(*owner, 20) == jc::StopDelivery::MANAGED);
    KEXPECT_EQ(owner->jobctl_barrier.stop_signal(), 20U);
    KEXPECT_TRUE(owner->jobctl_stopped.load());
}
#endif

KTEST(SignalPending, TerminalMembershipAndUidIndependentGrantShareAdmission) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_terminal_signal_authority());
}

KTEST(SignalPending, SourceTerminalAdmissionDrainsBeforeExecCapture) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_terminal_source_cutover());
}

KTEST(SignalPending, ParentDeathUsesCurrentRegistrationAndDrainsBeforeExecCapture) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_parent_death_cutover());
}

KTEST(SignalPending, ChildEventsDrainLocalAdmissionBeforeExecCapture) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_child_signal_cutover());
}
