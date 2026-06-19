#include "time.hpp"

#include <bits/timeval.h>
#include <sys/times.h>

#include <cerrno>
#include <cstdint>
#include <ctime>
#include <platform/ktime/ktime.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/tsc/tsc.hpp>

#include "abi/callnums/time.h"
#include "platform/dbg/dbg.hpp"

namespace ker::syscall::time {

namespace {

using log = ker::mod::dbg::logger<"time">;

struct Itimerval {
    struct timeval it_interval;
    struct timeval it_value;
};

constexpr int ITIMER_REAL = 0;
constexpr long NSEC_PER_SEC = 1000000000L;
constexpr uint64_t USEC_PER_SEC = 1000000ULL;

// CLK_TCK for times() return values - must match userspace sysconf(_SC_CLK_TCK)
constexpr uint64_t WOS_CLK_TCK = 100;

// Convert microseconds to clock ticks (CLK_TCK = 100, so 1 tick = 10000 us)
auto us_to_ticks(uint64_t us) -> uint64_t { return us / (1000000 / WOS_CLK_TCK); }

auto task_cpu_time_ns(ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr) {
        return 0;
    }
    return (task->user_time_us + task->system_time_us) * 1000ULL;
}

auto process_cpu_time_ns(ker::mod::sched::task::Task* current) -> uint64_t {
    if (current == nullptr) {
        return 0;
    }

    uint64_t const PROCESS_PID = ker::mod::sched::task::process_pid(*current);
    uint64_t total_ns = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (ker::mod::sched::task::same_thread_group(*task, PROCESS_PID)) {
            total_ns += task_cpu_time_ns(task);
        }
        task->release();
    }
    return total_ns;
}

auto relative_timespec_to_us(const struct timespec& ts, uint64_t& out_us) -> bool {
    out_us = 0;
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return false;
    }

    auto const NSEC_US = (static_cast<uint64_t>(ts.tv_nsec) + 999ULL) / 1000ULL;
    auto const SEC = static_cast<uint64_t>(ts.tv_sec);
    if (SEC > (UINT64_MAX - NSEC_US) / USEC_PER_SEC) {
        return false;
    }

    out_us = (SEC * USEC_PER_SEC) + NSEC_US;
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

auto deadline_from_now_us(uint64_t sleep_us) -> uint64_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    if (UINT64_MAX - NOW_US < sleep_us) {
        return UINT64_MAX;
    }
    return NOW_US + sleep_us;
}

}  // namespace

uint64_t sys_time_get(uint64_t op, void* arg1, void* arg2) {
    // op 0 => gettimeofday: arg1 is struct timeval*
    // op 1 => clock_gettime: arg1 is struct timespec*
    // op 2 => nanosleep: arg1 is const struct timespec* (requested), arg2 is struct timespec* (remaining, may be null)
    // op 3 => times: arg1 is struct tms*, arg2 is clock_t* (return value)

    switch (static_cast<ker::abi::sys_time_ops>(op)) {
        case ker::abi::sys_time_ops::GETTIMEOFDAY: {
            // CLOCK_REALTIME: RTC wall-clock epoch + TSC monotonic offset
            if (arg1 == nullptr) {
                return static_cast<uint64_t>(-1);
            }
            uint64_t const EPOCH_NS = ker::mod::rtc::get_epoch_ns();
            auto* tv = reinterpret_cast<timeval*>(arg1);
            tv->tv_sec = static_cast<long>(EPOCH_NS / 1000000000ULL);
            tv->tv_usec = static_cast<long>((EPOCH_NS % 1000000000ULL) / 1000ULL);
            return 0;
        }

        case ker::abi::sys_time_ops::CLOCK_GETTIME: {
            if (arg1 == nullptr) {
                return static_cast<uint64_t>(-1);
            }
            auto* ts = reinterpret_cast<struct timespec*>(arg1);
            int const CLOCK_ID =
                static_cast<int>(reinterpret_cast<uint64_t>(arg2));  // 0=CLOCK_REALTIME, 1=CLOCK_MONOTONIC, 3=CLOCK_THREAD_CPUTIME_ID
            if (CLOCK_ID == 0) {
                // CLOCK_REALTIME: RTC wall-clock epoch (includes NTP offset)
                uint64_t const EPOCH_NS = ker::mod::rtc::get_epoch_ns();
                ts->tv_sec = static_cast<long>(EPOCH_NS / 1000000000ULL);
                ts->tv_nsec = static_cast<long>(EPOCH_NS % 1000000000ULL);
            } else if (CLOCK_ID == 2) {
                auto* task = ker::mod::sched::get_current_task();
                uint64_t const CPU_NS = process_cpu_time_ns(task);
                ts->tv_sec = static_cast<long>(CPU_NS / 1000000000ULL);
                ts->tv_nsec = static_cast<long>(CPU_NS % 1000000000ULL);
            } else if (CLOCK_ID == 3) {
                // CLOCK_THREAD_CPUTIME_ID: kernel-tracked on-CPU time for this task.
                // user_time_us + system_time_us are accumulated by the scheduler's
                // timer tick handler (process_tasks) each time this task is current.
                auto* task = ker::mod::sched::get_current_task();
                uint64_t const CPU_NS = task_cpu_time_ns(task);
                ts->tv_sec = static_cast<long>(CPU_NS / 1000000000ULL);
                ts->tv_nsec = static_cast<long>(CPU_NS % 1000000000ULL);
            } else {
                // CLOCK_MONOTONIC (and any other id): TSC nanoseconds since boot
                uint64_t mono_ns = 0;
                if (ker::mod::tsc::get_hz() != 0) {
                    mono_ns = ker::mod::tsc::get_ns();
                } else {
                    mono_ns = ker::mod::time::get_us() * 1000ULL;
                }
                ts->tv_sec = static_cast<long>(mono_ns / 1000000000ULL);
                ts->tv_nsec = static_cast<long>(mono_ns % 1000000000ULL);
            }
            return 0;
        }

        case ker::abi::sys_time_ops::NANOSLEEP: {
            if (arg1 == nullptr) {
                return static_cast<uint64_t>(-1);
            }
            const auto* req = reinterpret_cast<const struct timespec*>(arg1);
            uint64_t sleep_us = 0;
            if (!relative_timespec_to_us(*req, sleep_us)) {
                return static_cast<uint64_t>(-EINVAL);
            }
            if (sleep_us > 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task != nullptr) {
                    // Set wake deadline and use deferred_task_switch to properly block
                    // (move to wait list). The timer tick wakeup scan will reschedule us
                    // once wake_at_us is reached.
                    task->wake_at_us = deadline_from_now_us(sleep_us);
                    task->set_wait_channel("nanosleep");
                    task->deferred_task_switch = true;
                    // Return 0 now - syscall exit path sees deferred_task_switch=true,
                    // moves task to wait list, switches to next task.
                } else {
                    // Pre-scheduler fallback: spin-wait
                    uint64_t const START = ker::mod::time::get_us();
                    while (ker::mod::time::get_us() - START < sleep_us) {
                    }
                }
            }
            // Set remaining to zero
            if (arg2 != nullptr) {
                auto* rem = reinterpret_cast<struct timespec*>(arg2);
                rem->tv_sec = 0;
                rem->tv_nsec = 0;
            }
            return 0;
        }

        case ker::abi::sys_time_ops::TIMES: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-1);
            }

            if (arg1 != nullptr) {
                auto* tms = reinterpret_cast<struct tms*>(arg1);
                tms->tms_utime = static_cast<long>(us_to_ticks(task->user_time_us));
                tms->tms_stime = static_cast<long>(us_to_ticks(task->system_time_us));
                tms->tms_cutime = static_cast<long>(us_to_ticks(task->child_user_time_us));
                tms->tms_cstime = static_cast<long>(us_to_ticks(task->child_system_time_us));
            }

            // Return value: elapsed real time in ticks since an arbitrary epoch (system boot)
            if (arg2 != nullptr) {
                auto* out = reinterpret_cast<long*>(arg2);
                *out = static_cast<long>(us_to_ticks(ker::mod::time::get_us()));
            }
            return 0;
        }

        case ker::abi::sys_time_ops::SETITIMER: {
            // arg1 = which (as uintptr_t), arg2 = const itimerval* new_value
            // old_value is not passed through this 2-arg path; mlibc reads it
            // via getitimer before calling setitimer if it needs the old value.
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            int const WHICH = static_cast<int>(reinterpret_cast<uintptr_t>(arg1));
            if (WHICH != ITIMER_REAL) {
                return static_cast<uint64_t>(-EINVAL);
            }

            const auto* nv = reinterpret_cast<const Itimerval*>(arg2);
            if (nv == nullptr) {
                return static_cast<uint64_t>(-EFAULT);
            }

            uint64_t new_val_us = 0;
            uint64_t new_interval_us = 0;
            if (!relative_timeval_to_us(nv->it_value, new_val_us) || !relative_timeval_to_us(nv->it_interval, new_interval_us)) {
                return static_cast<uint64_t>(-EINVAL);
            }

            if (new_val_us == 0) {
                task->itimer_real_expire_us = 0;
                task->itimer_real_interval_us = 0;
            } else {
                task->itimer_real_expire_us = deadline_from_now_us(new_val_us);
                task->itimer_real_interval_us = new_interval_us;
                ker::mod::sched::request_local_timer_recheck();
            }
            return 0;
        }

        case ker::abi::sys_time_ops::GETITIMER: {
            // arg1 = which (as uintptr_t), arg2 = itimerval* curr_value
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            int const WHICH = static_cast<int>(reinterpret_cast<uintptr_t>(arg1));
            if (WHICH != ITIMER_REAL) {
                return static_cast<uint64_t>(-EINVAL);
            }

            auto* cv = reinterpret_cast<Itimerval*>(arg2);
            if (cv == nullptr) {
                return static_cast<uint64_t>(-EFAULT);
            }

            uint64_t const NOW_US = ker::mod::time::get_us();
            uint64_t remain_us = 0;
            if (task->itimer_real_expire_us != 0 && task->itimer_real_expire_us > NOW_US) {
                remain_us = task->itimer_real_expire_us - NOW_US;
            }

            cv->it_value.tv_sec = static_cast<long>(remain_us / 1000000ULL);
            cv->it_value.tv_usec = static_cast<long>(remain_us % 1000000ULL);
            cv->it_interval.tv_sec = static_cast<long>(task->itimer_real_interval_us / 1000000ULL);
            cv->it_interval.tv_usec = static_cast<long>(task->itimer_real_interval_us % 1000000ULL);
            return 0;
        }

        default:
            log::error("invalid op %llu", static_cast<unsigned long long>(op));
            return static_cast<uint64_t>(-1);
    }
}

}  // namespace ker::syscall::time
