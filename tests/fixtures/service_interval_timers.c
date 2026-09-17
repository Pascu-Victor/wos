#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t caught[3];
static int failures;
static unsigned checks;
static pthread_mutex_t thread_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t thread_condition = PTHREAD_COND_INITIALIZER;
static int thread_ready, thread_go;

static void handler(int signal) {
    if (signal == SIGALRM) ++caught[0];
    if (signal == SIGVTALRM) ++caught[1];
    if (signal == SIGPROF) ++caught[2];
}
static void check(int condition, const char* label) {
    ++checks;
    if (!condition) {
        ++failures;
        fprintf(stderr, "FAIL %s errno=%d\n", label, errno);
    }
}
static uint64_t now_ns(void) {
    struct timespec time;
    if (clock_gettime(CLOCK_MONOTONIC, &time)) return 0;
    return (uint64_t)time.tv_sec * 1000000000ULL + (uint64_t)time.tv_nsec;
}
static int arm(int kind, long us, long interval, struct itimerval* old) {
    struct itimerval value = {{interval / 1000000, interval % 1000000}, {us / 1000000, us % 1000000}};
    return setitimer(kind, &value, old);
}
static long remaining(const struct itimerval* value) { return value->it_value.tv_sec * 1000000 + value->it_value.tv_usec; }
static void sleep_ms(unsigned ms) {
    struct timespec request = {(time_t)(ms / 1000), (long)(ms % 1000) * 1000000};
    while (nanosleep(&request, &request) && errno == EINTR) {
    }
}
static int wait_signal(int kind, unsigned maximum_ms, int work) {
    uint64_t deadline = now_ns() + (uint64_t)maximum_ms * 1000000;
    volatile uint64_t calculation = 1;
    while (!caught[kind] && now_ns() < deadline) {
        if (work) {
            for (unsigned i = 0; i < 32768; ++i) calculation = calculation * 6364136223846793005ULL + 1;
        } else {
            sleep_ms(1);
        }
    }
    return caught[kind] != 0;
}
static void* shared_setter(void* unused) {
    (void)unused;
    return (void*)(intptr_t)(arm(ITIMER_VIRTUAL, 2000000, 500000, NULL) != 0);
}
static void* unmasked_receiver(void* unused) {
    (void)unused;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    if (pthread_sigmask(SIG_UNBLOCK, &set, NULL)) return (void*)1;
    pthread_mutex_lock(&thread_lock);
    thread_ready = 1;
    pthread_cond_signal(&thread_condition);
    while (!thread_go) pthread_cond_wait(&thread_condition, &thread_lock);
    pthread_mutex_unlock(&thread_lock);
    return (void*)(intptr_t)!wait_signal(0, 2000, 0);
}
static void* retired_main_receiver(void* unused) {
    (void)unused;
    if (arm(ITIMER_REAL, 20000, 0, NULL) || !wait_signal(0, 3000, 0)) _exit(1);
    _exit(0);
}

static int wait_child(pid_t child) {
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int argc, char** argv) {
    if (argc == 2 && !strcmp(argv[1], "exec-check")) {
        for (int kind = 0; kind < 3; ++kind) {
            struct itimerval value;
            if (getitimer(kind, &value) || remaining(&value) <= 0 || arm(kind, 0, 0, NULL)) return 1;
        }
        return 0;
    }
    pid_t parent = getpid();
    pid_t watchdog = fork();
    if (watchdog == 0) {
        sleep_ms(120000);
        if (getppid() == parent) kill(parent, SIGKILL);
        _exit(0);
    }
    if (watchdog < 0) return 2;
    struct sigaction action = {0};
    action.sa_handler = handler;
    action.sa_flags = SA_RESTART;
    sigemptyset(&action.sa_mask);
    check(sigaction(SIGALRM, &action, NULL) == 0 && sigaction(SIGVTALRM, &action, NULL) == 0 && sigaction(SIGPROF, &action, NULL) == 0,
          "install timer handlers");
    for (int kind = 0; kind < 3; ++kind) check(arm(kind, 0, 0, NULL) == 0, "initial disarm");
    struct itimerval invalid = {{0, 0}, {0, 1000000}}, value, old;
    check(setitimer(ITIMER_REAL, &invalid, NULL) == -1 && errno == EINVAL, "reject malformed timeval");
    check(getitimer(3, &value) == -1 && errno == EINVAL, "reject unknown timer");

    check(arm(ITIMER_REAL, 10000, 0, NULL) == 0, "arm real");
    struct timespec request = {1, 0}, remainder = {0, 0};
    check(nanosleep(&request, &remainder) == -1 && errno == EINTR && remainder.tv_nsec > 0,
          "real signal interrupts SA_RESTART nanosleep with remaining");
    check(caught[0] == 1, "one real expiration");
    for (int kind = 1; kind < 3; ++kind) {
        check(arm(kind, 10000, 0, NULL) == 0, "arm CPU timer");
        check(wait_signal(kind, 3000, 1), "CPU timer fires under work");
        check(arm(kind, 0, 0, NULL) == 0, "disarm CPU timer");
    }

    pthread_t thread;
    void* result = NULL;
    int created = pthread_create(&thread, NULL, shared_setter, NULL) == 0;
    check(created, "create shared setter");
    if (created) check(pthread_join(thread, &result) == 0 && !result, "join shared setter");
    check(getitimer(ITIMER_VIRTUAL, &value) == 0 && remaining(&value) > 1000000 && value.it_interval.tv_usec == 500000,
          "thread setter updates canonical process timer after thread exit");
    check(arm(ITIMER_VIRTUAL, 0, 123, &old) == 0 && remaining(&old) > 1000000 && old.it_interval.tv_usec == 500000,
          "atomic old timer snapshot");
    check(getitimer(ITIMER_VIRTUAL, &value) == 0 && remaining(&value) == 0 && value.it_interval.tv_usec == 123,
          "disarm preserves configured interval");

    sigset_t blocked;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGALRM);
    check(pthread_sigmask(SIG_BLOCK, &blocked, NULL) == 0, "block owner alarm");
    caught[0] = 0;
    created = pthread_create(&thread, NULL, unmasked_receiver, NULL) == 0;
    check(created, "create eligible receiver");
    if (created) {
        pthread_mutex_lock(&thread_lock);
        while (!thread_ready) pthread_cond_wait(&thread_condition, &thread_lock);
        check(arm(ITIMER_REAL, 20000, 0, NULL) == 0, "arm process-directed alarm");
        thread_go = 1;
        pthread_cond_broadcast(&thread_condition);
        pthread_mutex_unlock(&thread_lock);
        check(pthread_join(thread, &result) == 0 && !result && caught[0] == 1, "deliver once to unmasked sibling");
    }
    check(pthread_sigmask(SIG_UNBLOCK, &blocked, NULL) == 0, "restore owner mask");

    check(pthread_sigmask(SIG_BLOCK, &blocked, NULL) == 0, "block alarm for ignore transition");
    caught[0] = 0;
    check(arm(ITIMER_REAL, 10000, 0, NULL) == 0, "arm blocked pending alarm");
    sleep_ms(50);
    sigset_t pending;
    check(sigpending(&pending) == 0 && sigismember(&pending, SIGALRM) == 1, "blocked alarm becomes pending");
    struct sigaction ignored = action;
    ignored.sa_handler = SIG_IGN;
    check(sigaction(SIGALRM, &ignored, NULL) == 0, "ignore already pending alarm");
    check(sigpending(&pending) == 0 && sigismember(&pending, SIGALRM) == 0, "ignore discards pending alarm");
    check(sigaction(SIGALRM, &action, NULL) == 0, "restore handler after pending discard");
    check(pthread_sigmask(SIG_UNBLOCK, &blocked, NULL) == 0, "unblock discarded alarm");
    sleep_ms(10);
    check(caught[0] == 0, "discarded alarm is not delivered after restore");

    check(pthread_sigmask(SIG_BLOCK, &blocked, NULL) == 0, "block alarm before ignored expiry");
    check(sigaction(SIGALRM, &ignored, NULL) == 0, "ignore before arming alarm");
    check(arm(ITIMER_REAL, 10000, 0, NULL) == 0, "arm ignored alarm");
    sleep_ms(50);
    check(sigpending(&pending) == 0 && sigismember(&pending, SIGALRM) == 0, "ignored expiry is never pending");
    check(getitimer(ITIMER_REAL, &value) == 0 && remaining(&value) == 0, "ignored one-shot still expires");
    check(sigaction(SIGALRM, &action, NULL) == 0, "restore handler after ignored expiry");
    check(pthread_sigmask(SIG_UNBLOCK, &blocked, NULL) == 0, "unblock after ignored expiry");
    sleep_ms(10);
    check(caught[0] == 0, "ignored expiry is not delivered after restore");

    for (int kind = 0; kind < 3; ++kind) check(arm(kind, 5000000, 0, NULL) == 0, "arm before fork");
    pid_t child = fork();
    if (child == 0) {
        for (int kind = 0; kind < 3; ++kind)
            if (getitimer(kind, &value) || remaining(&value) != 0) _exit(1);
        _exit(0);
    }
    check(child > 0 && wait_child(child), "fork child starts disarmed");
    for (int kind = 0; kind < 3; ++kind) check(arm(kind, 0, 0, &old) == 0 && remaining(&old) > 0, "parent timers survive fork");
    child = fork();
    if (child == 0) {
        for (int kind = 0; kind < 3; ++kind)
            if (arm(kind, 5000000, 0, NULL)) _exit(2);
        execl(argv[0], argv[0], "exec-check", (char*)NULL);
        _exit(3);
    }
    check(child > 0 && wait_child(child), "local exec preserves all process timers");
    child = fork();
    if (child == 0) {
        caught[0] = 0;
        pthread_t receiver;
        if (pthread_create(&receiver, NULL, retired_main_receiver, NULL)) _exit(2);
        pthread_exit(NULL);
    }
    check(child > 0 && wait_child(child), "worker timer survives main pthread_exit");
    for (unsigned cycle = 0; cycle < 32; ++cycle) {
        child = fork();
        if (child == 0) {
            for (int kind = 0; kind < 3; ++kind) {
                for (unsigned repeat = 0; repeat < 4; ++repeat) {
                    if (arm(kind, 1000, 1000, NULL) || arm(kind, 0, 0, NULL)) _exit(1);
                }
                if (arm(kind, 1000000, 1000000, NULL)) _exit(2);
            }
            _exit(0); /* Active periodic timer lifetime must end with the process. */
        }
        check(child > 0 && wait_child(child), "rapid arm cancel and armed process exit");
    }
    for (int kind = 0; kind < 3; ++kind) check(arm(kind, 0, 0, NULL) == 0, "final disarm");
    kill(watchdog, SIGTERM);
    int watchdog_status = 0;
    while (waitpid(watchdog, &watchdog_status, 0) < 0 && errno == EINTR) {
    }
    printf("%s interval timers checks=%u failures=%d\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures != 0;
}
