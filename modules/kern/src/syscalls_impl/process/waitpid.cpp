#include "waitpid.hpp"

#include <cerrno>
#include <cstdint>
#include <limits>
#include <net/wki/remote_compute.hpp>
#include <net/wki/wki.hpp>
#include <optional>
#ifdef WOS_SELFTEST
#include <memory>
#endif
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>

#include "child_events.hpp"

namespace ker::syscall::process {
namespace {

namespace sched_task = ker::mod::sched::task;
namespace events = ker::syscall::process::child_events;

[[nodiscard]] auto wait_selector(int64_t pid, sched_task::Task& owner, events::Selector& selector) -> bool {
    if (pid > 0) {
        selector = static_cast<uint64_t>(pid);
        return true;
    }
    if (pid == -1) {
        selector = events::SELECT_ANY;
        return true;
    }
    if (pid == 0) {
        selector = events::selector_for_current_process_group(owner);
        return selector.value != 0;
    }
    if (pid == std::numeric_limits<int64_t>::min()) {
        return false;
    }
    uint64_t const GROUP = static_cast<uint64_t>(-pid);
    if (events::selector_for_process_group(GROUP).value == 0) return false;
    selector = events::selector_for_process_group(owner, GROUP);
    return true;
}

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
    using namespace ker::net::wki;
    uint64_t handle = 0;
    bool committing = false;
    uint64_t admission_deadline = ker::mod::time::get_us() + WKI_OP_TIMEOUT_US;
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
                admission_deadline = ker::mod::time::get_us() + WKI_OP_TIMEOUT_US;
            } else if (committing || result.error != -EAGAIN)
                return static_cast<uint64_t>(result.error);
        }
        if (remote_wait_interrupted(waiter, owner, committing)) {
            wki_child_wait_close(waiter, handle);
            return static_cast<uint64_t>(-EINTR);
        }
        uint64_t const NOW = ker::mod::time::get_us();
        if (handle == 0 && NOW >= admission_deadline) return static_cast<uint64_t>(-ETIMEDOUT);
        ker::mod::sched::preemptible_syscall_park("waitpid", sched_task::WaitChannelKind::WAITPID, NOW + (handle == 0 ? 10000 : 1000));
    }
}

}  // namespace

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    (void)gpr;
    auto* waiter = ker::mod::sched::get_current_task();
    if (waiter == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (!outputs_writable(*waiter, status, rusage_vaddr)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    sched_task::Task* const OWNER = events::acquire_process_owner(*waiter);
    if (OWNER == nullptr) {
        return static_cast<uint64_t>(-ECHILD);
    }
    if (auto const REMOTE = wait_remote(pid, *OWNER, *waiter, status, options, rusage_vaddr)) {
        OWNER->release();
        return *REMOTE;
    }
    events::Selector selector{};
    if (!wait_selector(pid, *OWNER, selector)) {
        OWNER->release();
        return static_cast<uint64_t>(-EINVAL);
    }

    bool const NOHANG = (options & events::OPTION_NOHANG) != 0;
    bool const NOWAIT = (options & events::OPTION_NOWAIT) != 0;
    for (;;) {
        events::ClaimedEvent claimed{};
        auto const PROBE = events::claim_or_register(*OWNER, *waiter, selector, options, !NOHANG, claimed);
        if (PROBE == events::ProbeResult::CLAIMED) {
            if (!copy_outputs(*waiter, status, rusage_vaddr, claimed)) {
                events::release_claim(*waiter, claimed);
                events::cancel_wait(*waiter);
                OWNER->release();
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!events::commit_claim(*OWNER, *waiter, claimed, NOWAIT)) {
                events::cancel_wait(*waiter);
                OWNER->release();
                return static_cast<uint64_t>(-EINTR);
            }
            events::cancel_wait(*waiter);
            OWNER->release();
            return claimed.subject_pid;
        }
        if (PROBE == events::ProbeResult::NO_CHILD) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return static_cast<uint64_t>(-ECHILD);
        }
        if (NOHANG) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return 0;
        }
        if (waiter->has_interrupting_signal_pending()) {
            events::cancel_wait(*waiter);
            OWNER->release();
            return static_cast<uint64_t>(-EINTR);
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
