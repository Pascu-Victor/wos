// Build/run inside a disposable WOS VM:
// cc -std=c23 -O2 service_signal_handler.c -o /tmp/service-signal-handler
// /tmp/service-signal-handler
#include <errno.h>
#include <grp.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int notification_fd;
static volatile sig_atomic_t handler_mode;
static volatile sig_atomic_t full_handler_mask;
static uint64_t cycles_per_ms;

static int64_t now_ms(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) _exit(4);
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static void active_handler(int signo) {
    (void)signo;
    const int64_t end = now_ms() + 2500;
    sigset_t active;
    if (sigprocmask(SIG_BLOCK, NULL, &active) != 0) _exit(5);
    bool valid_mask = sigismember(&active, SIGKILL) == 0 && sigismember(&active, SIGSTOP) == 0 &&
                      (!full_handler_mask || sigismember(&active, SIGUSR2) == 1);
    if (write(notification_fd, valid_mask ? "H" : "M", 1) != 1) _exit(5);
    // Remain inside the actual C signal frame. CPython's Python-level handlers
    // run after its C trampoline returns and cannot reproduce this condition.
    if (handler_mode == 2) {
        uint64_t deadline = __builtin_ia32_rdtsc() + cycles_per_ms * 2500;
        while (__builtin_ia32_rdtsc() < deadline) __asm__ volatile("pause");
        return;
    }
    while (now_ms() < end) {
        if (handler_mode == 1) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};
            (void)nanosleep(&delay, NULL);
        }
    }
}

static bool run_case(unsigned mode, bool full_mask) {
    int ready[2];
    if (pipe(ready) != 0) return false;
    pid_t child = fork();
    if (child == 0) {
        close(ready[0]);
        notification_fd = ready[1];
        handler_mode = (sig_atomic_t)mode;
        full_handler_mask = full_mask;
        if (setgroups(0, NULL) != 0 || setgid(100) != 0 || setuid(100) != 0) _exit(2);
        struct sigaction action = {0};
        action.sa_handler = active_handler;
        if (full_mask)
            sigfillset(&action.sa_mask);
        else
            sigemptyset(&action.sa_mask);
        if (sigaction(SIGUSR1, &action, NULL) != 0 || raise(SIGUSR1) != 0) _exit(3);
        _exit(6);
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        return false;
    }
    char entered = 0;
    ssize_t count = read(ready[0], &entered, 1);
    close(ready[0]);
    struct timespec entered_delay = {.tv_sec = 0, .tv_nsec = 20000000};
    nanosleep(&entered_delay, NULL);
    bool sent = count == 1 && kill(child, SIGKILL) == 0;
    int status = 0;
    int64_t start = now_ms();
    pid_t result = 0;
    // The handler has a finite fallback deadline, so even a failing baseline
    // can retire its child without resetting the VM or leaving a busy process.
    while (result == 0 && now_ms() - start < 5000) {
        result = waitpid(child, &status, WNOHANG);
        if (result == 0) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};
            nanosleep(&delay, NULL);
        }
    }
    int64_t elapsed = now_ms() - start;
    bool ok = sent && entered == 'H' && result == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL && elapsed < 1000;
    printf("handler=%s full_mask=%u effective_mask=%c child=%ld status=%d elapsed_ms=%ld result=%s\n",
           mode == 2   ? "userspace-loop"
           : mode == 1 ? "sleeping"
                       : "syscall-loop",
           (unsigned)full_mask, entered, (long)child, status, (long)elapsed, ok ? "PASS" : "FAIL");
    fflush(stdout);
    return ok;
}

__attribute__((noinline)) static void edit_return_mask(int signo) {
    // WOS SignalFrame is176 bytes: pretcode/signo/saved_mask precede registers.
    // The handler's native frame pointer identifies its incoming return slot.
    volatile uint64_t* frame = (volatile uint64_t*)((char*)__builtin_frame_address(0) + sizeof(uintptr_t));
    if (frame[1] != (uint64_t)signo) _exit(8);
    frame[2] = (UINT64_C(1) << (SIGKILL - 1)) | (UINT64_C(1) << (SIGSTOP - 1)) | (UINT64_C(1) << (SIGUSR2 - 1));
}

static bool return_mask_case(void) {
    pid_t child = fork();
    if (child == 0) {
        if (setgroups(0, NULL) != 0 || setgid(100) != 0 || setuid(100) != 0) _exit(2);
        struct sigaction action = {0};
        action.sa_handler = edit_return_mask;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGUSR1, &action, NULL) != 0 || raise(SIGUSR1) != 0) _exit(3);
        sigset_t active;
        if (sigprocmask(SIG_BLOCK, NULL, &active) != 0) _exit(4);
        bool ok = sigismember(&active, SIGKILL) == 0 && sigismember(&active, SIGSTOP) == 0 && sigismember(&active, SIGUSR2) == 1;
        printf("sigreturn mask: kill=%d stop=%d usr2=%d result=%s\n", sigismember(&active, SIGKILL), sigismember(&active, SIGSTOP),
               sigismember(&active, SIGUSR2), ok ? "PASS" : "FAIL");
        fflush(stdout);
        _exit(ok ? 0 : 7);
    }
    int status = 0;
    return child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(void) {
    if (geteuid() != 0) return 2;
    int64_t start_ms = now_ms();
    uint64_t start_cycles = __builtin_ia32_rdtsc();
    struct timespec calibration = {.tv_sec = 0, .tv_nsec = 20000000};
    nanosleep(&calibration, NULL);
    int64_t duration = now_ms() - start_ms;
    if (duration <= 0) return 2;
    cycles_per_ms = (__builtin_ia32_rdtsc() - start_cycles) / (uint64_t)duration;
    unsigned failed = 0;
    for (unsigned repeat = 0; repeat < 3; ++repeat) {
        for (unsigned mode = 0; mode < 3; ++mode)
            for (unsigned full_mask = 0; full_mask < 2; ++full_mask)
                if (!run_case(mode, full_mask != 0)) ++failed;
    }
    if (!return_mask_case()) ++failed;
    printf("active-handler SIGKILL/mask cases=19 failed=%u\n", failed);
    return failed != 0;
}
