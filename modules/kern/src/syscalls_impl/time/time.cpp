#include "time.hpp"

#include <bits/timeval.h>
#include <sys/times.h>

#include <cerrno>
#include <cstdint>
#include <ctime>
#include <platform/ktime/ktime.hpp>
#include <platform/ktime/process_timer.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <platform/tsc/tsc.hpp>

#include "abi/callnums/time.h"
#include "platform/dbg/dbg.hpp"
#include "time_math.hpp"

namespace ker::syscall::time {

namespace {

using log = ker::mod::dbg::logger<"time">;

struct Itimerval {
    struct timeval it_interval;
    struct timeval it_value;
};

constexpr uint64_t USEC_PER_SEC = 1000000ULL;

// CLK_TCK for times() return values - must match userspace sysconf(_SC_CLK_TCK)
constexpr uint64_t WOS_CLK_TCK = 100;

// Convert microseconds to clock ticks (CLK_TCK = 100, so 1 tick = 10000 us)
auto us_to_ticks(uint64_t us) -> uint64_t { return us / (1000000 / WOS_CLK_TCK); }

auto cpu_us_to_timespec(uint64_t cpu_us) -> timespec {
    return timespec{.tv_sec = static_cast<long>(cpu_us / USEC_PER_SEC), .tv_nsec = static_cast<long>((cpu_us % USEC_PER_SEC) * 1000ULL)};
}

auto task_cpu_time_us(const ker::mod::sched::task::Task& task) -> uint64_t {
    return ker::mod::sched::resource::saturating_cpu_add(task.user_time_us, task.system_time_us);
}

auto process_cpu_times(ker::mod::sched::task::Task& task, tms& result) -> bool {
    ker::mod::sched::resource::CpuObservation observed{};
    if (!ker::mod::sched::resource::copy_cpu_observation(task, observed)) return false;
    result.tms_utime = static_cast<clock_t>(us_to_ticks(observed.user_time_us));
    result.tms_stime = static_cast<clock_t>(us_to_ticks(observed.system_time_us));
    result.tms_cutime = static_cast<clock_t>(us_to_ticks(observed.child_user_time_us));
    result.tms_cstime = static_cast<clock_t>(us_to_ticks(observed.child_system_time_us));
    return true;
}

auto relative_timeval_to_us(const struct timeval& tv, uint64_t& out_us) -> bool {
    out_us = 0;
    if (tv.tv_sec < 0 || tv.tv_usec < 0) {
        return false;
    }

    auto const USEC = static_cast<uint64_t>(tv.tv_usec);
    if (USEC >= USEC_PER_SEC) {
        return false;
    }

    auto const SEC = static_cast<uint64_t>(tv.tv_sec);
    if (SEC > (UINT64_MAX - USEC) / USEC_PER_SEC) {
        return false;
    }

    out_us = (SEC * USEC_PER_SEC) + USEC;
    return true;
}

auto to_itimerval(ker::mod::time::interval::Value value) -> Itimerval {
    return {.it_interval = {.tv_sec = static_cast<long>(value.interval_us / USEC_PER_SEC),
                            .tv_usec = static_cast<long>(value.interval_us % USEC_PER_SEC)},
            .it_value = {.tv_sec = static_cast<long>(value.remaining_us / USEC_PER_SEC),
                         .tv_usec = static_cast<long>(value.remaining_us % USEC_PER_SEC)}};
}

auto set_itimer(ker::mod::sched::task::Task& task, int which, uint64_t new_address, uint64_t old_address) -> uint64_t {
    if (which < 0 || which > 2) return static_cast<uint64_t>(-EINVAL);
    Itimerval new_value{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(task, new_address, new_value)) return static_cast<uint64_t>(-EFAULT);
    uint64_t new_val_us = 0;
    uint64_t new_interval_us = 0;
    if (!relative_timeval_to_us(new_value.it_value, new_val_us) || !relative_timeval_to_us(new_value.it_interval, new_interval_us)) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (old_address != 0 && !ker::mod::sys::usercopy::ensure_writable(task, old_address, sizeof(Itimerval))) {
        return static_cast<uint64_t>(-EFAULT);
    }
    ker::mod::time::interval::Value previous{};
    int const RESULT = ker::mod::time::process_timer::set(task, which, {new_val_us, new_interval_us}, &previous);
    if (RESULT != 0) return static_cast<uint64_t>(RESULT);
    if (old_address != 0 && !ker::mod::sys::usercopy::copy_value_to_task(task, old_address, to_itimerval(previous))) {
        return static_cast<uint64_t>(-EFAULT);
    }
    return 0;
}

auto sleep_until(ker::mod::sched::task::Task& task, int clock_id, uint32_t flags, uint64_t requested_ns, uint64_t remaining_address)
    -> uint64_t {
    bool const ABSOLUTE = (flags & ker::abi::TIME_SLEEP_ABSTIME) != 0;
    bool const REALTIME = ABSOLUTE && clock_id == 0;
    uint64_t const DEADLINE = ABSOLUTE ? requested_ns : math::add(ker::mod::time::get_monotonic_ns(), requested_ns);
    for (;;) {
        uint64_t const MONO_NS = ker::mod::time::get_monotonic_ns();
        uint64_t const NOW_NS = REALTIME ? ker::mod::rtc::get_epoch_ns() : MONO_NS;
        if (NOW_NS >= DEADLINE) return 0;
        uint64_t const REMAINING_NS = DEADLINE - NOW_NS;
        if (task.has_interrupting_signal_pending()) {
            if (!ABSOLUTE && remaining_address != 0) {
                timespec const REMAINING{.tv_sec = static_cast<long>(REMAINING_NS / math::NSEC_PER_SEC),
                                         .tv_nsec = static_cast<long>(REMAINING_NS % math::NSEC_PER_SEC)};
                if (!ker::mod::sys::usercopy::copy_value_to_task(task, remaining_address, REMAINING)) {
                    return static_cast<uint64_t>(-EFAULT);
                }
            }
            return static_cast<uint64_t>(-EINTR);
        }
        ker::mod::sched::preemptible_syscall_park("nanosleep", math::park_deadline_us(MONO_NS, REMAINING_NS, REALTIME));
    }
}

}  // namespace

auto sys_time_get(uint64_t op, uint64_t arg1, uint64_t arg2) -> uint64_t {
    // op 0 => gettimeofday: arg1 is struct timeval*
    // op 1 => clock_gettime: arg1 is struct timespec*
    // op 2 => nanosleep: arg1 is const struct timespec* (requested), arg2 is struct timespec* (remaining, may be null)
    // op 3 => times: arg1 is struct tms*, arg2 is clock_t* (return value)

    switch (static_cast<ker::abi::sys_time_ops>(op)) {
        case ker::abi::sys_time_ops::GETTIMEOFDAY: {
            // CLOCK_REALTIME: RTC wall-clock epoch + TSC monotonic offset
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || arg1 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            uint64_t const EPOCH_NS = ker::mod::rtc::get_epoch_ns();
            timeval tv{};
            tv.tv_sec = static_cast<long>(EPOCH_NS / 1000000000ULL);
            tv.tv_usec = static_cast<long>((EPOCH_NS % 1000000000ULL) / 1000ULL);
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg1, tv) ? 0 : static_cast<uint64_t>(-EFAULT);
        }

        case ker::abi::sys_time_ops::CLOCK_GETTIME: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || arg1 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            timespec ts{};
            int const CLOCK_ID = static_cast<int>(arg2);  // 0=CLOCK_REALTIME, 1=CLOCK_MONOTONIC, 3=CLOCK_THREAD_CPUTIME_ID
            if (CLOCK_ID == 0) {
                // CLOCK_REALTIME: RTC wall-clock epoch (includes NTP offset)
                uint64_t const EPOCH_NS = ker::mod::rtc::get_epoch_ns();
                ts.tv_sec = static_cast<long>(EPOCH_NS / 1000000000ULL);
                ts.tv_nsec = static_cast<long>(EPOCH_NS % 1000000000ULL);
            } else if (CLOCK_ID == 2) {
                ker::mod::sched::resource::CpuObservation observed{};
                if (!ker::mod::sched::resource::copy_cpu_observation(*task, observed)) return static_cast<uint64_t>(-ESRCH);
                ts = cpu_us_to_timespec(ker::mod::sched::resource::saturating_cpu_add(observed.user_time_us, observed.system_time_us));
            } else if (CLOCK_ID == 3) {
                // Thread clock remains this task's own scheduled time.
                ts = cpu_us_to_timespec(task_cpu_time_us(*task));
            } else if (CLOCK_ID == 1) {
                // CLOCK_MONOTONIC uses the same clock as scheduler deadlines.
                uint64_t const mono_ns = ker::mod::time::get_monotonic_ns();
                ts.tv_sec = static_cast<long>(mono_ns / 1000000000ULL);
                ts.tv_nsec = static_cast<long>(mono_ns % 1000000000ULL);
            } else {
                return static_cast<uint64_t>(-EINVAL);
            }
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg1, ts) ? 0 : static_cast<uint64_t>(-EFAULT);
        }

        case ker::abi::sys_time_ops::NANOSLEEP: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || arg1 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            timespec req{};
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, arg1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            uint64_t sleep_ns = 0;
            if (!math::to_ns(req.tv_sec, req.tv_nsec, sleep_ns)) return static_cast<uint64_t>(-EINVAL);
            if (arg2 != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, arg2, sizeof(timespec))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return sleep_until(*task, 1, 0, sleep_ns, arg2);
        }

        case ker::abi::sys_time_ops::CLOCK_GETRES: {
            int const CLOCK_ID = static_cast<int>(arg2);
            if (!math::valid_clock(CLOCK_ID)) return static_cast<uint64_t>(-EINVAL);
            if (arg1 == 0) return 0;
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return static_cast<uint64_t>(-EFAULT);
            uint64_t const RESOLUTION_NS = CLOCK_ID >= 2 ? 1000 : ker::mod::tsc::get_resolution_ns();
            timespec const RESOLUTION{.tv_sec = static_cast<long>(RESOLUTION_NS / math::NSEC_PER_SEC),
                                      .tv_nsec = static_cast<long>(RESOLUTION_NS % math::NSEC_PER_SEC)};
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg1, RESOLUTION) ? 0 : static_cast<uint64_t>(-EFAULT);
        }

        case ker::abi::sys_time_ops::CLOCK_NANOSLEEP: {
            if (arg2 != ker::abi::TIME_SLEEP_VERSION) return static_cast<uint64_t>(-EINVAL);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || arg1 == 0) return static_cast<uint64_t>(-EFAULT);
            ker::abi::ClockSleepRequest request{};
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, arg1, request)) return static_cast<uint64_t>(-EFAULT);
            if (!math::sleep_clock(request.clock_id) || (request.flags & ~ker::abi::TIME_SLEEP_ABSTIME) != 0) {
                return static_cast<uint64_t>(-EINVAL);
            }
            uint64_t requested_ns = 0;
            if (!math::to_ns(request.seconds, request.nanoseconds, requested_ns)) return static_cast<uint64_t>(-EINVAL);
            if ((request.flags & ker::abi::TIME_SLEEP_ABSTIME) == 0 && request.remaining_address != 0 &&
                !ker::mod::sys::usercopy::ensure_writable(*task, request.remaining_address, sizeof(timespec))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return sleep_until(*task, request.clock_id, request.flags, requested_ns, request.remaining_address);
        }

        case ker::abi::sys_time_ops::TIMES: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-1);
            }

            if ((arg1 != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, arg1, sizeof(tms))) ||
                (arg2 != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, arg2, sizeof(clock_t)))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            tms process_times{};
            if (!process_cpu_times(*task, process_times)) return static_cast<uint64_t>(-ESRCH);
            auto const ELAPSED = static_cast<clock_t>(us_to_ticks(ker::mod::time::get_us()));
            if ((arg1 != 0 && !ker::mod::sys::usercopy::copy_value_to_task(*task, arg1, process_times)) ||
                (arg2 != 0 && !ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, ELAPSED))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        case ker::abi::sys_time_ops::SETITIMER: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return static_cast<uint64_t>(-ESRCH);
            return set_itimer(*task, static_cast<int>(arg1), arg2, 0);
        }

        case ker::abi::sys_time_ops::SETITIMER_V1: {
            if (arg2 != ker::abi::TIME_ITIMER_VERSION) return static_cast<uint64_t>(-EINVAL);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return static_cast<uint64_t>(-ESRCH);
            ker::abi::SetItimerRequest request{};
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, arg1, request)) return static_cast<uint64_t>(-EFAULT);
            if (request.reserved != 0) return static_cast<uint64_t>(-EINVAL);
            return set_itimer(*task, request.which, request.new_value_address, request.old_value_address);
        }

        case ker::abi::sys_time_ops::GETITIMER: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return static_cast<uint64_t>(-ESRCH);
            ker::mod::time::interval::Value current{};
            int const RESULT = ker::mod::time::process_timer::get(*task, static_cast<int>(arg1), current);
            if (RESULT != 0) return static_cast<uint64_t>(RESULT);
            Itimerval const current_value = to_itimerval(current);
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, current_value) ? 0 : static_cast<uint64_t>(-EFAULT);
        }

        default:
            log::error("invalid op %llu", static_cast<unsigned long long>(op));
            return static_cast<uint64_t>(-ENOSYS);
    }
}

#ifdef WOS_SELFTEST
auto selftest_cpu_observation() -> bool {
    namespace resource = ker::mod::sched::resource;
    ker::mod::sched::task::Task process{};
    process.pid = 0x7B41;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    static_cast<void>(process.resources.charge_cpu(100000, 70000));
    process.child_user_time_us = 90000;
    process.child_system_time_us = 110000;
    ker::mod::sched::task::Task thread{};
    thread.type = process.type;
    thread.is_thread = true;
    thread.owner_pid = process.pid;
    thread.user_time_us = 11;
    thread.system_time_us = 13;
    if (!resource::bind_thread_owner(thread, process)) return false;
    tms observed{};
    bool ok = process_cpu_times(thread, observed) && observed.tms_utime == 7 && observed.tms_stime == 3 && observed.tms_cutime == 9 &&
              observed.tms_cstime == 11 && task_cpu_time_us(thread) == 24;
    auto const MAXIMUM = cpu_us_to_timespec(UINT64_MAX);
    ok &= MAXIMUM.tv_sec == static_cast<long>(UINT64_MAX / USEC_PER_SEC) &&
          MAXIMUM.tv_nsec == static_cast<long>((UINT64_MAX % USEC_PER_SEC) * 1000ULL);
    auto const ORDINARY = cpu_us_to_timespec(1234567);
    ok &= ORDINARY.tv_sec == 1 && ORDINARY.tv_nsec == 234567000;
    thread.user_time_us = UINT64_MAX;
    ok &= task_cpu_time_us(thread) == UINT64_MAX;
    return ok;
}
#endif

}  // namespace ker::syscall::time
