#include "httpd/time.hpp"

#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/process.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdint>

#include "httpd/config.hpp"

namespace httpd {
namespace {

auto monotonic_now_ms() -> int64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }

    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -1;
    }

    int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC;
    auto const SEC = static_cast<int64_t>(ts.tv_sec);
    if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC) {
        return INT64_MAX;
    }

    return (SEC * MSEC_PER_SEC) + NSEC_MS;
}

auto child_wait_timed_out(int64_t deadline_ms, uint64_t waited_us, int timeout_ms) -> bool {
    if (deadline_ms >= 0) {
        int64_t const NOW_MS = monotonic_now_ms();
        if (NOW_MS >= 0) {
            return NOW_MS >= deadline_ms;
        }
    }
    return waited_us >= static_cast<uint64_t>(timeout_ms) * USEC_PER_MSEC;
}

void reap_child_after_timeout(int64_t pid) {
    if (pid <= 0) {
        return;
    }

    (void)ker::process::kill(pid, SIGKILL);
    for (int retry = 0; retry < MOUNT_CHILD_REAP_RETRIES; ++retry) {
        int32_t reap_status = 0;
        int64_t const REAPED = ker::process::waitpid(pid, &reap_status, WNOHANG, nullptr);
        if (REAPED == pid || (REAPED < 0 && REAPED != -EINTR)) {
            return;
        }
        usleep(CHILD_WAIT_POLL_US);
    }
}

}  // namespace

auto deadline_after_ms(int timeout_ms) -> int64_t {
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return -1;
    }
    if (timeout_ms <= 0) {
        return NOW_MS;
    }

    auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms);
    if (INT64_MAX - NOW_MS < TIMEOUT_MS) {
        return INT64_MAX;
    }
    return NOW_MS + TIMEOUT_MS;
}

auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int {
    if (deadline_ms < 0) {
        return fallback_timeout_ms;
    }
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return fallback_timeout_ms;
    }
    if (deadline_ms <= NOW_MS) {
        errno = ETIMEDOUT;
        return 0;
    }
    int64_t const REMAINING_MS = deadline_ms - NOW_MS;
    return REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS);
}

auto wait_for_child_timeout(int64_t pid, int32_t* status, int timeout_ms) -> bool {
    if (pid <= 0) {
        if (status != nullptr) {
            *status = -EINVAL;
        }
        return false;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    uint64_t waited_us = 0;

    for (;;) {
        int64_t const WAITED = ker::process::waitpid(pid, status, WNOHANG, nullptr);
        if (WAITED == pid) {
            return true;
        }
        if (WAITED < 0 && WAITED != -EINTR) {
            if (status != nullptr) {
                *status = static_cast<int32_t>(WAITED);
            }
            return false;
        }
        if (child_wait_timed_out(DEADLINE_MS, waited_us, timeout_ms)) {
            break;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    reap_child_after_timeout(pid);
    if (status != nullptr) {
        *status = -ETIMEDOUT;
    }
    return false;
}

}  // namespace httpd
