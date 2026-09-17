// Compile/run as root in a disposable WOS VM:
// cc -std=c23 -O2 -pthread service_job_control_disposition.c -o /tmp/job-disposition
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

struct state {
    unsigned progress;
    unsigned handlers;
    unsigned unblock;
};
static struct state* shared;
static int shmid;
static int64_t now_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now)) _exit(2);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static void delay_ms(long ms) {
    int64_t end = now_ms() + ms;
    for (;;) {
        int64_t remaining = end - now_ms();
        if (remaining <= 0) return;
        // Signal/event wakes may return before the requested interval. Measure
        // the full observation window rather than assuming one sleep did so.
        struct timespec delay = {.tv_sec = remaining / 1000, .tv_nsec = remaining % 1000 * 1000000};
        nanosleep(&delay, NULL);
    }
}
static unsigned read_value(unsigned* value) { return __atomic_load_n(value, __ATOMIC_SEQ_CST); }
static void caught(int signo) {
    (void)signo;
    __atomic_fetch_add(&shared->handlers, 1, __ATOMIC_SEQ_CST);
}
static void* worker(void* unused) {
    (void)unused;
    for (;;) {
        __atomic_fetch_add(&shared->progress, 1, __ATOMIC_SEQ_CST);
        delay_ms(1);
    }
    return NULL;
}
static bool run_case(int signo, unsigned disposition, bool group) {
    *shared = (struct state){0};
    int ready[2];
    if (pipe(ready)) return false;
    pid_t child = fork();
    if (!child) {
        close(ready[0]);
        if (shmdt(shared)) _exit(2);
        shared = shmat(shmid, NULL, 0);
        if (shared == (void*)-1 || setpgid(0, 0) || setgroups(0, NULL) || setgid(100) || setuid(100)) _exit(2);
        struct sigaction action = {.sa_handler = disposition == 0 ? caught : disposition == 1 ? SIG_IGN : SIG_DFL};
        sigemptyset(&action.sa_mask);
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, signo);
        if (sigaction(signo, &action, NULL) || (disposition == 2 && sigprocmask(SIG_BLOCK, &mask, NULL))) _exit(2);
        pthread_t thread;
        if (pthread_create(&thread, NULL, worker, NULL)) _exit(3);
        if (write(ready[1], "R", 1) != 1) _exit(4);
        close(ready[1]);
        while (!read_value(&shared->unblock)) delay_ms(1);
        if (disposition == 2 && sigprocmask(SIG_UNBLOCK, &mask, NULL)) _exit(2);
        for (;;) delay_ms(1);
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        return false;
    }
    struct pollfd entry = {.fd = ready[0], .events = POLLIN};
    char value;
    bool entered = poll(&entry, 1, 2000) > 0 && (entry.revents & POLLIN) && read(ready[0], &value, 1) == 1;
    close(ready[0]);
    bool sent = entered && kill(group ? -child : child, signo) == 0;
    unsigned before = read_value(&shared->progress);
    delay_ms(100);
    int status = 0;
    bool running = read_value(&shared->progress) != before;
    bool no_stop = waitpid(child, &status, WNOHANG | WUNTRACED) == 0;
    unsigned handlers = read_value(&shared->handlers);
    bool delivery = handlers == (disposition == 0 ? 1U : 0U);
    // For the blocked case, CONT must discard the pending stop before unblock.
    bool cont = kill(group ? -child : child, SIGCONT) == 0;
    __atomic_store_n(&shared->unblock, 1, __ATOMIC_SEQ_CST);
    before = read_value(&shared->progress);
    delay_ms(100);
    bool canceled = read_value(&shared->progress) != before && waitpid(child, &status, WNOHANG | WUNTRACED) == 0;
    bool kill_sent = kill(child, SIGKILL) == 0;
    pid_t reaped = 0;
    int64_t end = now_ms() + 2000;
    while (!reaped && now_ms() < end) {
        reaped = waitpid(child, &status, WNOHANG);
        if (!reaped) delay_ms(2);
    }
    bool killed = kill_sent && reaped == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    bool ok = sent && running && no_stop && delivery && cont && canceled && killed;
    printf("signal=%d disposition=%u group=%u running=%u no_stop=%u handlers=%u canceled=%u kill=%u result=%s\n", signo, disposition,
           (unsigned)group, (unsigned)running, (unsigned)no_stop, handlers, (unsigned)canceled, (unsigned)killed, ok ? "PASS" : "FAIL");
    fflush(stdout);
    if (!killed) _exit(3);
    return ok;
}
static pthread_t retired_main;
static int retired_ready_fd, retired_signal;
static void* retired_worker(void* unused) {
    (void)unused;
    if (pthread_join(retired_main, NULL)) _exit(2);
    if (write(retired_ready_fd, "R", 1) != 1) _exit(2);
    close(retired_ready_fd);
    bool unblocked = false;
    for (;;) {
        __atomic_fetch_add(&shared->progress, 1, __ATOMIC_SEQ_CST);
        if (!unblocked && read_value(&shared->unblock)) {
            sigset_t mask;
            sigemptyset(&mask);
            sigaddset(&mask, retired_signal);
            if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL)) _exit(2);
            unblocked = true;
        }
        delay_ms(1);
    }
}
static bool await_stop(pid_t child, int signo) {
    int status = 0;
    int64_t end = now_ms() + 2000;
    while (now_ms() < end) {
        pid_t result = waitpid(child, &status, WNOHANG | WUNTRACED);
        if (result) return result == child && WIFSTOPPED(status) && WSTOPSIG(status) == signo;
        delay_ms(2);
    }
    return false;
}
// WOS /proc/PID/stat exposes the kernel wait-channel name. Joining the
// userspace TCB alone can precede THREAD_EXIT; wait for the retired executor.
static bool await_retired_owner(pid_t child) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%ld/stat", (long)child);
    int64_t end = now_ms() + 2000;
    while (now_ms() < end) {
        FILE* file = fopen(path, "r");
        char data[1024] = {0};
        bool retired = file && fgets(data, sizeof(data), file) && strstr(data, " thread_exit ");
        if (file) fclose(file);
        if (retired) return true;
        delay_ms(2);
    }
    return false;
}
static bool run_retired_case(int signo, unsigned disposition, bool group, unsigned uid) {
    *shared = (struct state){0};
    int ready[2];
    if (pipe(ready)) return false;
    pid_t child = fork();
    if (!child) {
        close(ready[0]);
        if (shmdt(shared)) _exit(2);
        shared = shmat(shmid, NULL, 0);
        if (shared == (void*)-1 || setpgid(0, 0) || setgroups(0, NULL) || setgid(uid) || setuid(uid)) _exit(2);
        struct sigaction action = {.sa_handler = disposition == 0 ? caught : disposition == 1 ? SIG_IGN : SIG_DFL};
        sigemptyset(&action.sa_mask);
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, signo);
        if (sigaction(signo, &action, NULL) || pthread_sigmask(disposition == 2 || disposition == 4 ? SIG_BLOCK : SIG_UNBLOCK, &mask, NULL))
            _exit(2);
        retired_main = pthread_self();
        retired_ready_fd = ready[1];
        retired_signal = signo;
        pthread_t thread;
        if (pthread_create(&thread, NULL, retired_worker, NULL)) _exit(3);
        pthread_exit(NULL);
    }
    close(ready[1]);
    if (child < 0) {
        close(ready[0]);
        return false;
    }
    struct pollfd entry = {.fd = ready[0], .events = POLLIN};
    char value = 0;
    bool entered = poll(&entry, 1, 2000) > 0 && read(ready[0], &value, 1) == 1 && value == 'R';
    close(ready[0]);
    pid_t target = group ? -child : child;
    bool retired = entered && await_retired_owner(child);
    bool ok = retired && kill(target, signo) == 0;
    int status = 0;
    bool stopped = false;
    if (ok && disposition != 3) {
        unsigned before = read_value(&shared->progress);
        delay_ms(100);
        ok = read_value(&shared->progress) != before && waitpid(child, &status, WNOHANG | WUNTRACED) == 0 &&
             read_value(&shared->handlers) == (disposition == 0 ? 1U : 0U);
    }
    if (ok && disposition == 4) __atomic_store_n(&shared->unblock, 1, __ATOMIC_SEQ_CST);
    if (ok && disposition >= 3) {
        stopped = await_stop(child, signo);
        ok = stopped;
        if (ok) {
            unsigned before = read_value(&shared->progress);
            delay_ms(50);
            ok = read_value(&shared->progress) == before;
        }
    }
    // CONT must cancel a blocked stop before unmasking; accepted default stops
    // must release both the retired main executor and the surviving worker.
    bool continued = kill(target, SIGCONT) == 0;
    __atomic_store_n(&shared->unblock, 1, __ATOMIC_SEQ_CST);
    unsigned before = read_value(&shared->progress);
    delay_ms(100);
    bool running = read_value(&shared->progress) != before && waitpid(child, &status, WNOHANG | WUNTRACED) == 0;
    bool kill_sent = kill(child, SIGKILL) == 0;
    pid_t reaped = 0;
    int64_t end = now_ms() + 2000;
    while (!reaped && now_ms() < end) {
        reaped = waitpid(child, &status, WNOHANG);
        if (!reaped) delay_ms(2);
    }
    bool killed = kill_sent && reaped == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    ok = ok && continued && running && killed;
    printf("retired uid=%u signal=%d disposition=%u group=%u retired=%u stopped=%u resumed=%u kill=%u result=%s\n", uid, signo, disposition,
           (unsigned)group, (unsigned)retired, (unsigned)stopped, (unsigned)running, (unsigned)killed, ok ? "PASS" : "FAIL");
    fflush(stdout);
    if (!killed) _exit(3);
    return ok;
}
int main(int argc, char** argv) {
    bool retired = argc == 3 && !strcmp(argv[1], "--retired");
    unsigned uid = retired ? (unsigned)atoi(argv[2]) : 100;
    if ((argc != 1 && !retired) || (uid != 0 && uid != 100 && uid != 104)) return 2;
    if (geteuid()) return 2;
    shmid = shmget(IPC_PRIVATE, 4096, 0600);
    if (shmid < 0) return 2;
    shared = shmat(shmid, NULL, 0);
    if (shared == (void*)-1) return 2;
    unsigned failed = 0;
    for (int signo = SIGTSTP; signo <= SIGTTOU; ++signo)
        for (unsigned disposition = 0; disposition < (retired ? 5U : 3U); ++disposition)
            for (unsigned group = 0; group < 2; ++group)
                if (!(retired ? run_retired_case(signo, disposition, group != 0, uid) : run_case(signo, disposition, group != 0))) ++failed;
    shmdt(shared);
    shmctl(shmid, IPC_RMID, NULL);
    printf("job-control disposition cases=%u failed=%u\n", retired ? 30U : 18U, failed);
    return failed != 0;
}
