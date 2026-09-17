// Compile/run as root in a disposable WOS VM:
// cc -std=c23 -O2 -pthread service_process_stop.c -o /tmp/service-process-stop
// Optional argument: stop signal number 19..22 (default SIGSTOP).
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile unsigned long* progress;
static unsigned worker_mode;
static int shmid;
static int stop_signal = SIGSTOP;

static int64_t now_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now)) _exit(2);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static void delay_ms(long value) {
    struct timespec delay = {.tv_sec = value / 1000, .tv_nsec = value % 1000 * 1000000};
    nanosleep(&delay, NULL);
}

static void* worker(void* unused) {
    (void)unused;
    int64_t end = now_ms() + 10000;
    while (now_ms() < end) {
        *progress = *progress + 1;
        if (worker_mode) delay_ms(1);
    }
    return NULL;
}

static pid_t wait_bounded(pid_t child, int* status, int options) {
    int64_t end = now_ms() + 1500;
    do {
        pid_t result = waitpid(child, status, options | WNOHANG);
        if (result) return result;
        delay_ms(5);
    } while (now_ms() < end);
    return 0;
}

static bool run_case(unsigned mode, bool stop_group, bool continue_group) {
    int ready[2];
    if (pipe(ready)) return false;
    *progress = 0;
    pid_t child = fork();
    if (child == 0) {
        close(ready[0]);
        if (shmdt((void*)progress)) _exit(2);
        progress = shmat(shmid, NULL, 0);
        if (progress == (void*)-1 || setpgid(0, 0) || setgroups(0, NULL) || setgid(100) || setuid(100)) _exit(2);
        worker_mode = mode;
        pthread_t thread;
        if (pthread_create(&thread, NULL, worker, NULL)) _exit(3);
        if (write(ready[1], "R", 1) != 1) _exit(4);
        close(ready[1]);
        pthread_join(thread, NULL);
        _exit(6);
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        return false;
    }
    struct pollfd entry = {.fd = ready[0], .events = POLLIN};
    char value;
    bool entered = poll(&entry, 1, 1000) > 0 && (entry.revents & POLLIN) && read(ready[0], &value, 1) == 1;
    close(ready[0]);
    delay_ms(20);
    bool running = *progress != 0;
    int status = 0;
    bool stopped = entered && kill(stop_group ? -child : child, stop_signal) == 0 && wait_bounded(child, &status, WUNTRACED) == child &&
                   WIFSTOPPED(status) && WSTOPSIG(status) == stop_signal;
    delay_ms(20);
    unsigned long before = *progress;
    delay_ms(100);
    unsigned long after = *progress;
    bool quiet = stopped && before == after;
    bool continued = stopped && kill(continue_group ? -child : child, SIGCONT) == 0 && wait_bounded(child, &status, WCONTINUED) == child &&
                     WIFCONTINUED(status);
    int64_t resume_end = now_ms() + 1000;
    while (continued && *progress == after && now_ms() < resume_end) delay_ms(5);
    bool resumed = continued && *progress != after;
    bool sent = kill(child, SIGKILL) == 0;
    bool killed = wait_bounded(child, &status, 0) == child && sent && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    bool ok = running && stopped && quiet && continued && resumed && killed;
    printf(
        "worker=%u stop_group=%u cont_group=%u child=%ld running=%u stop=%u before=%lu after=%lu quiet=%u cont=%u resumed=%u "
        "kill=%u result=%s\n",
        mode, (unsigned)stop_group, (unsigned)continue_group, (long)child, (unsigned)running, (unsigned)stopped, before, after,
        (unsigned)quiet, (unsigned)continued, (unsigned)resumed, (unsigned)killed, ok ? "PASS" : "FAIL");
    fflush(stdout);
    if (!killed) _exit(3);
    return ok;
}

int main(int argc, char** argv) {
    if (argc > 2) return 2;
    if (argc == 2) {
        char* end;
        long value = strtol(argv[1], &end, 10);
        if (*end || value < SIGSTOP || value > SIGTTOU) return 2;
        stop_signal = (int)value;
    }
    if (geteuid() != 0) return 2;
    shmid = shmget(IPC_PRIVATE, 4096, 0600);
    if (shmid < 0) return 2;
    progress = shmat(shmid, NULL, 0);
    if (progress == (void*)-1) return 2;
    unsigned failed = 0;
    for (unsigned repeat = 0; repeat < 3; ++repeat)
        for (unsigned mode = 0; mode < 2; ++mode)
            for (unsigned stop_group = 0; stop_group < 2; ++stop_group)
                for (unsigned continue_group = 0; continue_group < 2; ++continue_group)
                    if (!run_case(mode, stop_group != 0, continue_group != 0)) ++failed;
    shmdt((void*)progress);
    shmctl(shmid, IPC_RMID, NULL);
    printf("process STOP/CONT signal=%d cases=24 failed=%u\n", stop_signal, failed);
    return failed != 0;
}
