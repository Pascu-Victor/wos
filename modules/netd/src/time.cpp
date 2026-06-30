#include "netd/time.hpp"

#include <time.h>  // NOLINT(modernize-deprecated-headers): sysroot exposes POSIX time APIs here.

#include <cerrno>
#include <ctime>

namespace netd {

auto monotonic_now_us() -> uint64_t {
    struct timespec now{};
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (static_cast<uint64_t>(now.tv_sec) * USEC_PER_SEC) + static_cast<uint64_t>(now.tv_nsec / 1000);
}

void sleep_until_us(uint64_t deadline_us) {
    for (;;) {
        uint64_t const NOW_US = monotonic_now_us();
        if (NOW_US >= deadline_us) {
            return;
        }

        uint64_t const REMAINING_US = deadline_us - NOW_US;
        struct timespec ts{};
        ts.tv_sec = static_cast<time_t>(REMAINING_US / USEC_PER_SEC);
        ts.tv_nsec = static_cast<long>((REMAINING_US % USEC_PER_SEC) * 1000);

        if (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
            continue;
        }
    }
}

void sleep_for_seconds(uint32_t seconds) { sleep_until_us(monotonic_now_us() + (static_cast<uint64_t>(seconds) * USEC_PER_SEC)); }

}  // namespace netd
