#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static volatile sig_atomic_t alarms;
static void alarm_handler(int signo) {
    (void)signo;
    ++alarms;
}

#define CHECK(condition)                                                                             \
    do {                                                                                             \
        if (!(condition)) {                                                                          \
            fprintf(stderr, "time ABI failure line %d: %s errno=%d\n", __LINE__, #condition, errno); \
            return 1;                                                                                \
        }                                                                                            \
    } while (0)

static uint64_t ns(struct timespec value) { return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec; }

static struct timespec plus_ms(struct timespec value, long ms) {
    value.tv_nsec += ms * 1000000;
    value.tv_sec += value.tv_nsec / 1000000000;
    value.tv_nsec %= 1000000000;
    return value;
}

int main(void) {
    struct timespec value, before, after, remaining;
    struct timespec request = {0, 2000000};
    CHECK(clock_gettime((clockid_t)-1, &value) == -1 && errno == EINVAL);
    CHECK(clock_getres((clockid_t)-1, NULL) == -1 && errno == EINVAL);
    for (int clock = 0; clock < 4; ++clock) {
        CHECK(clock_getres((clockid_t)clock, NULL) == 0);
        CHECK(clock_getres((clockid_t)clock, &value) == 0);
        CHECK(value.tv_sec >= 0 && value.tv_nsec >= 0 && value.tv_nsec < 1000000000 && ns(value) > 0);
    }
    errno = EDOM;
    CHECK(clock_nanosleep((clockid_t)-1, 0, &request, NULL) == EINVAL && errno == EDOM);
    CHECK(clock_nanosleep(CLOCK_MONOTONIC, 2, &request, NULL) == EINVAL && errno == EDOM);
    request.tv_nsec = 1000000000;
    CHECK(clock_nanosleep(CLOCK_MONOTONIC, 0, &request, NULL) == EINVAL && errno == EDOM);
    request.tv_nsec = 2000000;
    CHECK(clock_gettime(CLOCK_MONOTONIC, &before) == 0);
    CHECK(nanosleep(&request, NULL) == 0);
    CHECK(clock_gettime(CLOCK_MONOTONIC, &after) == 0 && ns(after) - ns(before) >= ns(request));
    for (int clock = 0; clock < 2; ++clock) {
        CHECK(clock_gettime((clockid_t)clock, &before) == 0);
        request = plus_ms(before, 2);
        remaining = (struct timespec){123, 456};
        CHECK(clock_nanosleep((clockid_t)clock, TIMER_ABSTIME, &request, &remaining) == 0);
        CHECK(remaining.tv_sec == 123 && remaining.tv_nsec == 456);
        CHECK(clock_gettime((clockid_t)clock, &after) == 0 && ns(after) >= ns(request));
    }
    struct sigaction action = {0};
    action.sa_handler = alarm_handler;
    action.sa_flags = SA_RESTART; /* Sleep must still report interruption. */
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGALRM, &action, NULL) == 0);
    for (int native = 0; native < 2; ++native) {
        struct itimerval timer = {{0, 0}, {0, 10000}};
        CHECK(setitimer(ITIMER_REAL, &timer, NULL) == 0);
        request = (struct timespec){1, 0};
        remaining = (struct timespec){0, 0};
        errno = EDOM;
        if (native) {
            CHECK(clock_nanosleep(CLOCK_MONOTONIC, 0, &request, &remaining) == EINTR && errno == EDOM);
        } else {
            CHECK(nanosleep(&request, &remaining) == -1 && errno == EINTR);
        }
        CHECK(ns(remaining) > 0 && ns(remaining) <= ns(request));
    }
    CHECK(alarms == 2);
    puts("PASS time ABI: clocks, resolution, absolute sleeps, EINTR and remaining time");
    return 0;
}
