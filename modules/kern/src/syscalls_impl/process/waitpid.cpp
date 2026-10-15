#include "waitpid.hpp"

#ifdef WOS_SELFTEST
#include <test/compartment_lifecycle_runtime.hpp>
#endif

#include <cerrno>
#include <cstdint>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <optional>
#ifdef WOS_SELFTEST
#include <memory>
#endif
#include <platform/asm/cpu.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>

#include "child_events.hpp"

namespace ker::syscall::process {
namespace {

namespace sched_task = ker::mod::sched::task;
namespace events = ker::syscall::process::child_events;

[[nodiscard]] auto outputs_writable(sched_task::Task& waiter, const int32_t* status, uint64_t rusage_vaddr) -> bool {
    if (status != nullptr && !ker::mod::sys::usercopy::ensure_writable(waiter, reinterpret_cast<uint64_t>(status), sizeof(*status))) {
        return false;
    }
    return rusage_vaddr == 0 || ker::mod::sys::usercopy::ensure_writable(waiter, rusage_vaddr, sizeof(KernRusage));
}

[[nodiscard]] auto copy_outputs(sched_task::Task& waiter, int32_t* status, uint64_t rusage_vaddr, const events::ClaimedEvent& claimed)
    -> bool {
    if (status != nullptr && !ker::mod::sys::usercopy::copy_value_to_task(waiter, reinterpret_cast<uint64_t>(status), claimed.status)) {
        return false;
    }
    if (rusage_vaddr == 0) {
        return true;
    }

    KernRusage rusage{};
    rusage.ru_utime_sec = static_cast<int64_t>(claimed.user_time_us / 1000000ULL);
    rusage.ru_utime_usec = static_cast<int64_t>(claimed.user_time_us % 1000000ULL);
    rusage.ru_stime_sec = static_cast<int64_t>(claimed.system_time_us / 1000000ULL);
    rusage.ru_stime_usec = static_cast<int64_t>(claimed.system_time_us % 1000000ULL);
    return ker::mod::sys::usercopy::copy_value_to_task(waiter, rusage_vaddr, rusage);
}

// Fatal handoff waits for syscall unwinding. These markers cannot become a
// userspace handler/restart, so the continuation may outlive this caller even
// after COMMIT. Do not consume them: the validated return path owns teardown.
[[nodiscard]] auto remote_wait_interrupted(sched_task::Task& waiter, sched_task::Task& owner, bool committing) -> bool {
    constexpr uint64_t KILL_MASK = uint64_t{1} << 8;
    if ((waiter.signal_pending_bits() & KILL_MASK) != 0 || (waiter.pending_cpu_signals.load(std::memory_order_acquire) & KILL_MASK) != 0 ||
        waiter.process_exit_requested.load(std::memory_order_acquire))
        return true;
    return !committing &&
           (waiter.has_interrupting_signal_pending() || owner.resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0);
}

// nullopt selects the existing local wait path. Remote continuations retain
// their own owner/waiter pins and contain values only at the syscall boundary.
[[nodiscard]] auto wait_remote(int64_t pid, sched_task::Task& owner, sched_task::Task& waiter, int32_t* status, int32_t options,
                               uint64_t rusage_vaddr) -> std::optional<uint64_t> {
    // Private public IDs cannot enter the physical distributed-wait protocol.
    // Fail closed through local admission even for an unbound owner.
    auto const* scope = owner.pid_namespace();
    if (scope == nullptr || !scope->is_host()) return std::nullopt;
    using namespace ker::net::wki;
    uint64_t handle = 0;
    bool committing = false;
    uint64_t admission_deadline = ker::mod::sched::saturating_deadline_us(ker::mod::time::get_us(), WKI_OP_TIMEOUT_US);
    for (;;) {
        auto result = handle == 0 ? wki_child_wait_begin(owner, waiter, pid, options, handle) : wki_child_wait_poll(waiter, handle);
        if (result.state == WkiChildWaitState::LOCAL) return std::nullopt;
        if (result.state == WkiChildWaitState::CLAIMED) {
            events::ClaimedEvent const VALUE{.subject_pid = result.pid,
                                             .user_time_us = result.user_time_us,
                                             .system_time_us = result.system_time_us,
                                             .status = result.status};
            if (!copy_outputs(waiter, status, rusage_vaddr, VALUE)) {
                wki_child_wait_close(waiter, handle);
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!wki_child_wait_commit(waiter, handle)) {
                wki_child_wait_close(waiter, handle);
                return static_cast<uint64_t>(-EINTR);
            }
            committing = true;
        } else if (result.state == WkiChildWaitState::COMPLETE) {
            wki_child_wait_close(waiter, handle);
            return result.pid;
        } else if (result.state == WkiChildWaitState::NO_CHILD) {
            wki_child_wait_close(waiter, handle);
            return static_cast<uint64_t>(-ECHILD);
        } else if (result.state == WkiChildWaitState::EMPTY || result.state == WkiChildWaitState::ERROR) {
            wki_child_wait_close(waiter, handle);
            handle = 0;
            if (result.state == WkiChildWaitState::EMPTY) {
                if ((options & events::OPTION_NOHANG) != 0) return 0;
                admission_deadline = ker::mod::sched::saturating_deadline_us(ker::mod::time::get_us(), WKI_OP_TIMEOUT_US);
            } else if (committing || result.error != -EAGAIN)
                return static_cast<uint64_t>(result.error);
        }
        if (remote_wait_interrupted(waiter, owner, committing)) {
            wki_child_wait_close(waiter, handle);
            return static_cast<uint64_t>(-EINTR);
        }
        uint64_t const NOW = ker::mod::time::get_us();
        if (handle == 0 && NOW >= admission_deadline) return static_cast<uint64_t>(-ETIMEDOUT);
        ker::mod::sched::preemptible_syscall_park("waitpid", sched_task::WaitChannelKind::WAITPID,
                                                  ker::mod::sched::saturating_deadline_us(NOW, handle == 0 ? 10000 : 1000));
    }
}

}  // namespace

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    (void)gpr;
    auto* waiter = ker::mod::sched::get_current_task();
    if (waiter == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    // A span follows this syscall continuation across park/resume. Its
    // successful point names the physical child, with the same correlation as
    // the waiting parent's begin/end; no borrowed child pointer is retained.
    namespace perf = ker::mod::perf;
    constexpr auto OP = static_cast<uint8_t>(perf::WkiPerfLocalProcOp::WAITPID);
    bool const TRACE = perf::is_wki_scope_recording_enabled(perf::WkiPerfScope::LOCAL_PROC, OP);
    uint32_t const CORRELATION = TRACE ? perf::next_wki_trace_correlation() : 0;
    uint64_t const STARTED_US = TRACE ? ker::mod::time::get_us() : 0;
    if (TRACE) {
        perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), waiter->pid, perf::WkiPerfScope::LOCAL_PROC, OP,
                               perf::WkiPerfPhase::BEGIN, 0, 0, CORRELATION, 0, static_cast<uint32_t>(options), WOS_PERF_CALLSITE());
    }
    auto finish_wait = [&](uint64_t result, uint64_t physical_subject = 0) -> uint64_t {
        if (TRACE) {
            auto const SIGNED_RESULT = static_cast<int64_t>(result);
            int32_t const STATUS = SIGNED_RESULT < 0 ? static_cast<int32_t>(SIGNED_RESULT) : 0;
            uint64_t const NOW_US = ker::mod::time::get_us();
            uint64_t const ELAPSED_US = NOW_US >= STARTED_US ? NOW_US - STARTED_US : 0;
            uint32_t const DURATION_US = ELAPSED_US > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(ELAPSED_US);
            auto const CPU = static_cast<uint32_t>(ker::mod::cpu::current_cpu());
            if (SIGNED_RESULT > 0) {
                perf::record_wki_event(CPU, physical_subject != 0 ? physical_subject : result, perf::WkiPerfScope::LOCAL_PROC, OP,
                                       perf::WkiPerfPhase::POINT, 0, 0, CORRELATION, 0, 0, WOS_PERF_CALLSITE());
            }
            perf::record_wki_event(CPU, waiter->pid, perf::WkiPerfScope::LOCAL_PROC, OP, perf::WkiPerfPhase::END, 0, 0, CORRELATION, STATUS,
                                   DURATION_US, WOS_PERF_CALLSITE());
            perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, OP, 0, 0, STATUS, DURATION_US, true, 0, 0);
        }
        return result;
    };

    if (!outputs_writable(*waiter, status, rusage_vaddr)) {
        return finish_wait(static_cast<uint64_t>(-EFAULT));
    }

    sched_task::Task* const OWNER = events::acquire_process_owner(*waiter);
    if (OWNER == nullptr) {
        return finish_wait(static_cast<uint64_t>(-ECHILD));
    }
    if (auto const REMOTE = wait_remote(pid, *OWNER, *waiter, status, options, rusage_vaddr)) {
        OWNER->release();
        return finish_wait(*REMOTE);
    }
    events::Selector selector{};
    int const SELECTOR_ERROR = events::public_wait_selector(*OWNER, pid, selector);
    if (SELECTOR_ERROR != 0) {
        OWNER->release();
        return finish_wait(static_cast<uint64_t>(SELECTOR_ERROR));
    }

    bool const NOHANG = (options & events::OPTION_NOHANG) != 0;
    bool const NOWAIT = (options & events::OPTION_NOWAIT) != 0;
    for (;;) {
        events::ClaimedEvent claimed{};
        auto const PROBE = events::claim_or_register(*OWNER, *waiter, selector, options, !NOHANG, claimed, events::View::PUBLIC);
        if (PROBE == events::ProbeResult::CLAIMED) {
            if (!copy_outputs(*waiter, status, rusage_vaddr, claimed)) {
                events::release_claim(*waiter, claimed);
                events::cancel_wait(*waiter);
                OWNER->release();
                return finish_wait(static_cast<uint64_t>(-EFAULT));
            }
            if (!events::commit_claim(*OWNER, *waiter, claimed, NOWAIT)) {
                events::cancel_wait(*waiter);
                OWNER->release();
                return finish_wait(static_cast<uint64_t>(-EINTR));
            }
            events::cancel_wait(*waiter);
            OWNER->release();
#ifdef WOS_SELFTEST
            // claim was consumed; node is borrowed and never dereferenced.
            ker::test::lifecycle60::consumed_wait(
                *waiter, claimed.subject_pid, claimed.visible_pid, claimed.status,
                claimed.kind == sched_task::ChildEventKind::EXIT && claimed.audience == sched_task::ChildEventAudience::PARENT, NOWAIT);
#endif
            return finish_wait(claimed.visible_pid, claimed.subject_pid);
        }
        if (PROBE == events::ProbeResult::NO_CHILD || PROBE == events::ProbeResult::CLOSED) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return finish_wait(static_cast<uint64_t>(PROBE == events::ProbeResult::CLOSED ? -EIDRM : -ECHILD));
        }
        if (NOHANG) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return finish_wait(0);
        }
        if (waiter->has_interrupting_signal_pending()) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return finish_wait(static_cast<uint64_t>(-EINTR));
        }

        ker::mod::sched::preemptible_syscall_park("waitpid", sched_task::WaitChannelKind::WAITPID);
    }
}

#ifdef WOS_SELFTEST
auto selftest_remote_wait_commit_interruption() -> bool {
    auto task = std::make_unique<sched_task::Task>();
    task->type = sched_task::TaskType::PROCESS;
    task->state.store(sched_task::TaskState::ACTIVE);
    auto const CREDENTIALS = task->credentials();
    auto interrupted = [&](bool committing) { return remote_wait_interrupted(*task, *task, committing); };
    bool ok = !interrupted(false) && !interrupted(true);
    constexpr uint64_t USER = uint64_t{1} << 9, KILL = uint64_t{1} << 8, SOFT_CPU = uint64_t{1} << 23;
    task->signal_pending_store(USER);
    ok &= interrupted(false) && !interrupted(true) && task->signal_pending_bits() == USER;
    task->signal_mask_store(USER);
    ok &= !interrupted(false) && !interrupted(true);
    task->in_signal_handler = true;
    task->signal_pending_store(USER | KILL);
    ok &= interrupted(true) && task->signal_pending_bits() == (USER | KILL);
    task->signal_pending_store(0);
    task->signal_mask_store(0);
    task->in_signal_handler = false;
    task->pending_cpu_signals.store(SOFT_CPU);
    ok &= !interrupted(true) && task->pending_cpu_signals.load() == SOFT_CPU;
    task->pending_cpu_signals.store(SOFT_CPU | KILL);
    ok &= interrupted(true) && task->pending_cpu_signals.load() == (SOFT_CPU | KILL);
    task->pending_cpu_signals.store(0);
    task->process_exit_requested.store(true);
    ok &= interrupted(true) && task->process_exit_requested.load();
    task->is_thread = true;
    ok &= interrupted(true) && task->process_exit_requested.load();
    task->is_thread = false;
    task->process_exit_requested.store(false);
    task->resource_exec_quiescence_epoch.store(1);
    ok &= interrupted(false) && !interrupted(true);
    task->resource_exec_quiescence_epoch.store(0);
    return ok && !interrupted(true) && task->credentials() == CREDENTIALS && task->ref_count.load() == 1;
}
#endif

}  // namespace ker::syscall::process
