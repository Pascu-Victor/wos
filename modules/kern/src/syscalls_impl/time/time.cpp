#include "time.hpp"

#include <sys/times.h>

#include <platform/ktime/ktime.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/tsc/tsc.hpp>

#include "bits/posix/timeval.h"

// CLK_TCK for times() return values — must match userspace sysconf(_SC_CLK_TCK)
static constexpr uint64_t WOS_CLK_TCK = 100;

namespace ker::syscall::time {

// Convert microseconds to clock ticks (CLK_TCK = 100, so 1 tick = 10000 us)
static inline uint64_t us_to_ticks(uint64_t us) { return us / (1000000 / WOS_CLK_TCK); }

uint64_t sys_time_get(uint64_t op, void* arg1, void* arg2) {
    // op 0 => gettimeofday: arg1 is struct timeval*
    // op 1 => clock_gettime: arg1 is struct timespec*
    // op 2 => nanosleep: arg1 is const struct timespec* (requested), arg2 is struct timespec* (remaining, may be null)
    // op 3 => times: arg1 is struct tms*, arg2 is clock_t* (return value)

    switch ((ker::abi::sys_time_ops)op) {
        case ker::abi::sys_time_ops::gettimeofday: {
            // CLOCK_REALTIME: RTC wall-clock epoch + TSC monotonic offset
            if (arg1 == nullptr) {
                return (uint64_t)-1;
            }
            uint64_t epoch_ns = ker::mod::rtc::getEpochNs();
            auto* tv = reinterpret_cast<timeval*>(arg1);
            tv->tv_sec = (long)(epoch_ns / 1000000000ULL);
            tv->tv_usec = (long)((epoch_ns % 1000000000ULL) / 1000ULL);
            return 0;
        }

        case ker::abi::sys_time_ops::clock_gettime: {
            if (arg1 == nullptr) {
                return (uint64_t)-1;
            }
            auto* ts = reinterpret_cast<struct timespec*>(arg1);
            int clock_id = static_cast<int>(reinterpret_cast<uint64_t>(arg2));  // 0=CLOCK_REALTIME, 1=CLOCK_MONOTONIC
            if (clock_id == 0) {
                // CLOCK_REALTIME: RTC wall-clock epoch (includes NTP offset)
                uint64_t epoch_ns = ker::mod::rtc::getEpochNs();
                ts->tv_sec = (long)(epoch_ns / 1000000000ULL);
                ts->tv_nsec = (long)(epoch_ns % 1000000000ULL);
            } else {
                // CLOCK_MONOTONIC (and any other id): TSC nanoseconds since boot
                uint64_t mono_ns = 0;
                if (ker::mod::tsc::getHz() != 0) {
                    mono_ns = ker::mod::tsc::getNs();
                } else {
                    mono_ns = ker::mod::time::getUs() * 1000ULL;
                }
                ts->tv_sec = (long)(mono_ns / 1000000000ULL);
                ts->tv_nsec = (long)(mono_ns % 1000000000ULL);
            }
            return 0;
        }

        case ker::abi::sys_time_ops::nanosleep: {
            if (arg1 == nullptr) {
                return (uint64_t)-1;
            }
            const auto* req = reinterpret_cast<const struct timespec*>(arg1);
            uint64_t sleep_us = ((uint64_t)req->tv_sec * 1000000ULL) + ((uint64_t)req->tv_nsec / 1000ULL);
            uint64_t start = ker::mod::time::getUs();
            // Yield-based sleep loop
            while (ker::mod::time::getUs() - start < sleep_us) {
                ker::mod::sched::kern_yield();
            }
            // Set remaining to zero
            if (arg2 != nullptr) {
                auto* rem = reinterpret_cast<struct timespec*>(arg2);
                rem->tv_sec = 0;
                rem->tv_nsec = 0;
            }
            return 0;
        }

        case ker::abi::sys_time_ops::times: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return (uint64_t)-1;
            }

            if (arg1 != nullptr) {
                auto* tms = reinterpret_cast<struct tms*>(arg1);
                tms->tms_utime = (long)us_to_ticks(task->user_time_us);
                tms->tms_stime = (long)us_to_ticks(task->system_time_us);
                tms->tms_cutime = 0;  // TODO: accumulate children's times on waitpid
                tms->tms_cstime = 0;
            }

            // Return value: elapsed real time in ticks since an arbitrary epoch (system boot)
            if (arg2 != nullptr) {
                auto* out = (long*)arg2;
                *out = (long)us_to_ticks(ker::mod::time::getUs());
            }
            return 0;
        }

        default:
            ker::mod::dbg::error("Invalid op in syscall time");
            return (uint64_t)-1;
    }
}

}  // namespace ker::syscall::time
