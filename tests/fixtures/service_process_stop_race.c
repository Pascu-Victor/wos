// Compile/run as root in a disposable WOS VM:
// cc -std=c23 -O2 -pthread service_process_stop_race.c -o /tmp/process-stop-race
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

struct shared_state {
    unsigned long progress[3];
    unsigned continued;
};
static struct shared_state* shared;
static int shmid;
static int stop_signal = SIGSTOP;

static int64_t now_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now)) _exit(2);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static void delay_ms(long ms) {
    struct timespec delay = {.tv_sec = ms / 1000, .tv_nsec = ms % 1000 * 1000000};
    nanosleep(&delay, NULL);
}
static unsigned long progress(unsigned slot) { return __atomic_load_n(&shared->progress[slot], __ATOMIC_SEQ_CST); }
static unsigned continued(void) { return __atomic_load_n(&shared->continued, __ATOMIC_SEQ_CST); }
static void handler(int signo) {
    if (signo == SIGCONT) __atomic_fetch_add(&shared->continued, 1, __ATOMIC_SEQ_CST);
}
static void* pure_worker(void* unused) {
    (void)unused;
    for (;;) __atomic_fetch_add(&shared->progress[2], 1, __ATOMIC_SEQ_CST);
    return NULL;
}
static void* short_worker(void* unused) {
    (void)unused;
    for (unsigned i = 0; i < 1000; ++i) __atomic_fetch_add(&shared->progress[1], 1, __ATOMIC_SEQ_CST);
    return NULL;
}
static pid_t wait_bounded(pid_t child, int* status, int options) {
    int64_t end = now_ms() + 2000;
    do {
        pid_t result = waitpid(child, status, options | WNOHANG);
        if (result) return result;
        delay_ms(2);
    } while (now_ms() < end);
    return 0;
}
static bool run_case(bool churn, unsigned workers) {
    *shared = (struct shared_state){0};
    int ready[2];
    if (pipe(ready)) return false;
    pid_t child = fork();
    if (!child) {
        close(ready[0]);
        if (shmdt(shared)) _exit(2);
        shared = shmat(shmid, NULL, 0);
        if (shared == (void*)-1 || setpgid(0, 0) || setgroups(0, NULL) || setgid(100) || setuid(100)) _exit(2);
        struct sigaction action = {.sa_handler = handler};
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGCONT, &action, NULL) || sigaction(SIGUSR1, &action, NULL)) _exit(2);
        for (unsigned i = 0; i < workers; ++i) {
            pthread_t permanent;
            if (pthread_create(&permanent, NULL, pure_worker, NULL)) _exit(3);
        }
        if (write(ready[1], "R", 1) != 1) _exit(4);
        close(ready[1]);
        for (;;) {
            if (churn) {
                pthread_t temporary;
                if (pthread_create(&temporary, NULL, short_worker, NULL) || pthread_join(temporary, NULL)) _exit(5);
            } else {
                delay_ms(1);
            }
            __atomic_fetch_add(&shared->progress[0], 1, __ATOMIC_SEQ_CST);
        }
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        return false;
    }
    struct pollfd entry = {.fd = ready[0], .events = POLLIN};
    char value;
    bool ok = poll(&entry, 1, 2000) > 0 && (entry.revents & POLLIN) && read(ready[0], &value, 1) == 1;
    close(ready[0]);
    delay_ms(20);
    ok = ok && progress(0) && progress(2) && (!churn || progress(1));
    unsigned cycles = 0;
    int status = 0;
    for (; ok && cycles < 32; ++cycles) {
        bool stopped = kill(cycles & 1 ? -child : child, stop_signal) == 0 && wait_bounded(child, &status, WUNTRACED) == child &&
                       WIFSTOPPED(status) && WSTOPSIG(status) == stop_signal;
        unsigned long before[3] = {progress(0), progress(1), progress(2)};
        // No settling delay after waitpid: the STOP event itself promises all
        // members are quiescent, including a newly published thread.
        bool nudged = kill(child, SIGUSR1) == 0;
        delay_ms(20);
        bool quiet = before[0] == progress(0) && before[1] == progress(1) && before[2] == progress(2);
        bool cont = stopped && kill(cycles & 2 ? -child : child, SIGCONT) == 0 && wait_bounded(child, &status, WCONTINUED) == child &&
                    WIFCONTINUED(status);
        int64_t end = now_ms() + 2000;
        while (cont && (progress(0) == before[0] || progress(2) == before[2] || continued() < cycles + 1) && now_ms() < end) delay_ms(2);
        bool resumed = progress(0) != before[0] && progress(2) != before[2];
        bool single = continued() == cycles + 1;
        ok = stopped && nudged && quiet && cont && resumed && single;
        printf("workers=%u churn=%u cycle=%u stop=%u quiet=%u cont=%u resumed=%u handlers=%u single=%u result=%s\n", workers,
               (unsigned)churn, cycles, (unsigned)stopped, (unsigned)quiet, (unsigned)cont, (unsigned)resumed, continued(),
               (unsigned)single, ok ? "PASS" : "FAIL");
        fflush(stdout);
    }
    bool killed =
        kill(child, SIGKILL) == 0 && wait_bounded(child, &status, 0) == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    printf("workers=%u churn=%u cycles=%u kill=%u result=%s\n", workers, (unsigned)churn, cycles, (unsigned)killed,
           ok && killed ? "PASS" : "FAIL");
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
    if (geteuid()) return 2;
    shmid = shmget(IPC_PRIVATE, 4096, 0600);
    if (shmid < 0) return 2;
    shared = shmat(shmid, NULL, 0);
    if (shared == (void*)-1) return 2;
    bool plain = run_case(false, 1);
    bool churn = run_case(true, 1);
    // Exceed the controller's 32-member wake batch with 40 workers + leader.
    bool dense = run_case(false, 40);
    shmdt(shared);
    shmctl(shmid, IPC_RMID, NULL);
    printf("process STOP/CONT race signal=%d cases=3 failed=%u\n", stop_signal, (unsigned)!plain + (unsigned)!churn + (unsigned)!dense);
    return !plain || !churn || !dense;
}
