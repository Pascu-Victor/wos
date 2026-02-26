#include "time.hpp"

#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>

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

    // fallback: use HPET microseconds
    uint64_t us = ker::mod::time::getUs();
    uint64_t sec = us / 1000000;
    uint64_t usec = us % 1000000;

    switch ((ker::abi::sys_time_ops)op) {
        case ker::abi::sys_time_ops::gettimeofday: {
            if (!arg1) return (uint64_t)-1;
            struct timeval {
                long tv_sec;
                long tv_usec;
            };
            struct timeval* tv = (struct timeval*)arg1;
            tv->tv_sec = (long)sec;
            tv->tv_usec = (long)usec;
            return 0;
        }

        case ker::abi::sys_time_ops::clock_gettime: {
            if (!arg1) return (uint64_t)-1;
            struct timespec {
                long tv_sec;
                long tv_nsec;
            };
            struct timespec* ts = (struct timespec*)arg1;
            ts->tv_sec = (long)sec;
            ts->tv_nsec = (long)(usec * 1000);
            return 0;
        }

        case ker::abi::sys_time_ops::nanosleep: {
            if (!arg1) return (uint64_t)-1;
            struct timespec {
                long tv_sec;
                long tv_nsec;
            };
            auto* req = (const struct timespec*)arg1;
            uint64_t sleep_us = (uint64_t)req->tv_sec * 1000000ULL + (uint64_t)req->tv_nsec / 1000ULL;
            uint64_t start = ker::mod::time::getUs();
            // Yield-based sleep loop
            while (ker::mod::time::getUs() - start < sleep_us) {
                ker::mod::sched::kern_yield();
            }
            // Set remaining to zero
            if (arg2 != nullptr) {
                auto* rem = (struct timespec*)arg2;
                rem->tv_sec = 0;
                rem->tv_nsec = 0;
            }
            return 0;
        }

        case ker::abi::sys_time_ops::times: {
            // struct tms layout (matches POSIX):
            //   clock_t tms_utime;   // user CPU time
            //   clock_t tms_stime;   // system CPU time
            //   clock_t tms_cutime;  // user CPU time of children
            //   clock_t tms_cstime;  // system CPU time of children
            struct tms_data {
                long tms_utime;
                long tms_stime;
                long tms_cutime;
                long tms_cstime;
            };

            auto* task = ker::mod::sched::get_current_task();
            if (!task) return (uint64_t)-1;

            if (arg1) {
                auto* tms = (tms_data*)arg1;
                tms->tms_utime = (long)us_to_ticks(task->user_time_us);
                tms->tms_stime = (long)us_to_ticks(task->system_time_us);
                tms->tms_cutime = 0;  // TODO: accumulate children's times on waitpid
                tms->tms_cstime = 0;
            }

            // Return value: elapsed real time in ticks since an arbitrary epoch (system boot)
            if (arg2) {
                auto* out = (long*)arg2;
                *out = (long)us_to_ticks(us);
            }
            return 0;
        }

        default:
            ker::mod::dbg::error("Invalid op in syscall time");
            return (uint64_t)-1;
    }
}

}  // namespace ker::syscall::time
