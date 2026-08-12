#include "waitpid.hpp"

#include <cerrno>
#include <cstdint>
#include <limits>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>

#include "child_events.hpp"

namespace ker::syscall::process {
namespace {

namespace sched_task = ker::mod::sched::task;
namespace events = ker::syscall::process::child_events;

[[nodiscard]] auto wait_selector(int64_t pid, sched_task::Task& owner, uint64_t& selector) -> bool {
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
        return selector != 0;
    }
    if (pid == std::numeric_limits<int64_t>::min()) {
        return false;
    }
    selector = events::selector_for_process_group(static_cast<uint64_t>(-pid));
    return selector != 0;
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
    uint64_t selector = 0;
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

}  // namespace ker::syscall::process
