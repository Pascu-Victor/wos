#include "time.hpp"

#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::syscall::time {

uint64_t sys_time_get(uint64_t op, void* arg1, void* arg2) {
    (void)arg2;
    // op 0 => gettimeofday: arg1 is struct timeval*
    // op 1 => clock_gettime: arg1 is struct timespec*
    // op 2 => nanosleep: arg1 is const struct timespec* (requested), arg2 is struct timespec* (remaining, may be null)

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

        default:
            ker::mod::dbg::error("Invalid op in syscall time");
            return (uint64_t)-1;
    }
}

}  // namespace ker::syscall::time
