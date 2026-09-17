// Build and run inside a disposable WOS VM as root:
// cc -std=c23 -O2 service_signal_stop.c -o /tmp/service-signal-stop
// /tmp/service-signal-stop
#include <grp.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int notification_fd;
static unsigned handler_mode;
static volatile unsigned long* progress;
static uint64_t cycles_per_ms;
static int progress_shmid;

static int64_t now_ms(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) _exit(4);
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static void delay_ms(long value) {
    struct timespec delay = {.tv_sec = value / 1000, .tv_nsec = (value % 1000) * 1000000};
    nanosleep(&delay, NULL);
}

static void active_handler(int signo) {
    (void)signo;
    uint64_t deadline = __builtin_ia32_rdtsc() + cycles_per_ms * 4000;
    if (write(notification_fd, "H", 1) != 1) _exit(5);
    // Shared progress also observes the pure userspace loop, without inserting
    // a syscall into that mode. The finite fallback bounds failing baselines.
    while (__builtin_ia32_rdtsc() < deadline) {
        *progress = *progress + 1;
        if (handler_mode == 0) (void)now_ms();
        if (handler_mode == 1) delay_ms(10);
        __asm__ volatile("pause");
    }
}

static pid_t wait_bounded(pid_t child, int* status, int options, int64_t limit) {
    int64_t end = now_ms() + limit;
    pid_t result;
    do {
        result = waitpid(child, status, options | WNOHANG);
        if (result != 0) return result;
        delay_ms(5);
    } while (now_ms() < end);
    return 0;
}

static bool stop_child(pid_t child) {
    int status = 0;
    return kill(child, SIGSTOP) == 0 && wait_bounded(child, &status, WUNTRACED, 750) == child && WIFSTOPPED(status) &&
           WSTOPSIG(status) == SIGSTOP;
}

static bool run_case(unsigned mode, bool full_mask, bool backpressure) {
    int ready[2];
    if (pipe(ready) != 0) return false;
    *progress = 0;
    pid_t child = fork();
    if (child == 0) {
        close(ready[0]);
        notification_fd = ready[1];
        handler_mode = mode;
        if (shmdt((void*)progress) != 0) _exit(2);
        progress = shmat(progress_shmid, NULL, 0);
        if (progress == (void*)-1) _exit(2);
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
    struct pollfd notification = {.fd = ready[0], .events = POLLIN};
    bool ready_ok =
        poll(&notification, 1, 1000) > 0 && (notification.revents & POLLIN) != 0 && read(ready[0], &entered, 1) == 1 && entered == 'H';
    close(ready[0]);
    delay_ms(20);
    bool running = *progress != 0;
    bool stopped = ready_ok && running && stop_child(child);
    bool quiet = false, continued = false, resumed = false, stopped_again = false, quiet_again = false;
    if (stopped) {
        delay_ms(20);
        unsigned long before = *progress;
        // A benign pending signal wakes the task but must not resume it.
        kill(child, SIGUSR1);
        delay_ms(100);
        quiet = *progress == before;
        int status = 0;
        continued = kill(child, SIGCONT) == 0;
        if (!backpressure) continued = continued && wait_bounded(child, &status, WCONTINUED, 750) == child && WIFCONTINUED(status);
        int64_t end = now_ms() + 750;
        while (*progress == before && now_ms() < end) delay_ms(5);
        resumed = *progress != before;
        if (continued && resumed) {
            // Keeping the continued event queued fills the next stop's event
            // slot. Quiescence and KILL must still work under that backpressure.
            stopped_again = backpressure ? kill(child, SIGSTOP) == 0 : stop_child(child);
            delay_ms(20);
            before = *progress;
            delay_ms(100);
            quiet_again = *progress == before;
        }
    }
    int status = 0;
    // KILL must work directly from a stopped handler, with no preceding CONT.
    bool sent_kill = kill(child, SIGKILL) == 0;
    pid_t reaped = wait_bounded(child, &status, 0, 5000);
    bool killed = sent_kill && reaped == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    bool ok = running && stopped && quiet && continued && resumed && stopped_again && quiet_again && killed;
    printf(
        "handler=%u full_mask=%u backpressure=%u child=%ld status=%d ready=%u running=%u stop=%u quiet=%u cont=%u resumed=%u stop2=%u "
        "quiet2=%u kill=%u result=%s\n",
        mode, (unsigned)full_mask, (unsigned)backpressure, (long)child, status, (unsigned)ready_ok, (unsigned)running, (unsigned)stopped,
        (unsigned)quiet, (unsigned)continued, (unsigned)resumed, (unsigned)stopped_again, (unsigned)quiet_again, (unsigned)killed,
        ok ? "PASS" : "FAIL");
    fflush(stdout);
    return ok;
}

int main(void) {
    if (geteuid() != 0) return 2;
    progress_shmid = shmget(IPC_PRIVATE, 4096, 0600);
    if (progress_shmid < 0) return 2;
    progress = shmat(progress_shmid, NULL, 0);
    if (progress == (void*)-1) {
        shmctl(progress_shmid, IPC_RMID, NULL);
        return 2;
    }
    int64_t start = now_ms();
    uint64_t cycles = __builtin_ia32_rdtsc();
    delay_ms(20);
    int64_t elapsed = now_ms() - start;
    if (elapsed <= 0) return 2;
    cycles_per_ms = (__builtin_ia32_rdtsc() - cycles) / (uint64_t)elapsed;
    unsigned failed = 0;
    for (unsigned repeat = 0; repeat < 3; ++repeat)
        for (unsigned mode = 0; mode < 3; ++mode)
            for (unsigned full_mask = 0; full_mask < 2; ++full_mask)
                if (!run_case(mode, full_mask != 0, false)) ++failed;
    for (unsigned mode = 0; mode < 3; ++mode)
        for (unsigned full_mask = 0; full_mask < 2; ++full_mask)
            if (!run_case(mode, full_mask != 0, true)) ++failed;
    shmdt((void*)progress);
    shmctl(progress_shmid, IPC_RMID, NULL);
    printf("active-handler STOP/CONT cases=24 failed=%u\n", failed);
    return failed != 0;
}
