// Run in a disposable WOS VM as root:
// cc -std=c23 -O2 service_pipe_signal.c -o /tmp/service-pipe-signal
// /tmp/service-pipe-signal
#include <grp.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int64_t now_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) _exit(2);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static void delay_ms(long delay) {
    struct timespec value = {.tv_sec = delay / 1000, .tv_nsec = delay % 1000 * 1000000};
    nanosleep(&value, NULL);
}

static bool read_status(pid_t child, char* buffer, size_t size) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%ld/status", (long)child);
    FILE* file = fopen(path, "r");
    if (!file) return false;
    size_t count = fread(buffer, 1, size - 1, file);
    buffer[count] = 0;
    fclose(file);
    return true;
}

static bool await_pipe_read(pid_t child) {
    int64_t end = now_ms() + 1000;
    char status[2048];
    do {
        if (read_status(child, status, sizeof(status)) && strstr(status, "\nWchan:\tpipe_read\n")) return true;
        delay_ms(5);
    } while (now_ms() < end);
    return false;
}

static pid_t await_exit(pid_t child, int* status, int64_t timeout) {
    int64_t end = now_ms() + timeout;
    do {
        pid_t result = waitpid(child, status, WNOHANG);
        if (result != 0) return result;
        delay_ms(5);
    } while (now_ms() < end);
    return 0;
}

static bool run_case(unsigned mode, unsigned uid, bool child_event, unsigned event_delay) {
    int data[2], ready[2], gate[2];
    if (pipe(data) || pipe(ready) || pipe(gate)) return false;
    pid_t child = fork();
    if (child == 0) {
        close(data[1]);
        close(ready[0]);
        close(gate[1]);
        if (setgroups(0, NULL) || setgid(uid) || setuid(uid)) _exit(2);
        pid_t grandchild = -1;
        if (child_event) {
            grandchild = fork();
            if (grandchild < 0) _exit(3);
            if (grandchild == 0) {
                close(data[0]);
                close(ready[1]);
                char value;
                (void)read(gate[0], &value, 1);
                _exit(0);
            }
        }
        close(gate[0]);
        if (write(ready[1], "R", 1) != 1) _exit(4);
        close(ready[1]);
        char value = 0;
        ssize_t count = read(data[0], &value, 1);
        close(data[0]);
        if (grandchild > 0) {
            int status;
            if (waitpid(grandchild, &status, 0) != grandchild) _exit(5);
        }
        _exit((mode == 2 && count == 1 && value == 'X') || (mode == 3 && count == 0) ? 0 : 6);
    }
    close(data[0]);
    close(ready[1]);
    close(gate[0]);
    if (child < 0) {
        close(data[1]);
        close(ready[0]);
        close(gate[1]);
        return false;
    }
    struct pollfd notification = {.fd = ready[0], .events = POLLIN};
    char value;
    bool entered = poll(&notification, 1, 1000) > 0 && (notification.revents & POLLIN) && read(ready[0], &value, 1) == 1;
    close(ready[0]);
    bool parked = entered && await_pipe_read(child);
    // Closing the gate produces SIGCHLD only after the target entered read.
    close(gate[1]);
    if (child_event) delay_ms(event_delay);
    bool action = false;
    int64_t start = now_ms();
    if (mode < 2)
        action = kill(child, mode == 0 ? SIGKILL : SIGTERM) == 0;
    else if (mode == 2)
        action = write(data[1], "X", 1) == 1;
    else {
        action = close(data[1]) == 0;
        data[1] = -1;
    }
    int status = 0;
    pid_t reaped = await_exit(child, &status, 1500);
    int64_t elapsed = now_ms() - start;
    bool exit_ok = mode < 2 ? WIFSIGNALED(status) && WTERMSIG(status) == (mode == 0 ? SIGKILL : SIGTERM)
                            : WIFEXITED(status) && WEXITSTATUS(status) == 0;
    bool ok = parked && action && reaped == child && exit_ok;
    printf("mode=%u uid=%u child_event=%u child=%ld parked=%u status=%d reaped=%ld elapsed_ms=%ld result=%s\n", mode, uid,
           (unsigned)child_event, (long)child, (unsigned)parked, status, (long)reaped, (long)elapsed, ok ? "PASS" : "FAIL");
    if (!ok) {
        char snapshot[2048];
        if (read_status(child, snapshot, sizeof(snapshot))) printf("FAILED CHILD\n%s", snapshot);
    }
    fflush(stdout);
    // Release the data wait even on failure; then attempt bounded KILL cleanup.
    if (data[1] >= 0) close(data[1]);
    if (reaped == 0) {
        kill(child, SIGKILL);
        if (await_exit(child, &status, 1500) != child) {
            printf("UNREAPED child=%ld; stop further fixture cases\n", (long)child);
            fflush(stdout);
            _exit(3);
        }
    }
    return ok;
}

int main(int argc, char** argv) {
    if (geteuid() != 0) return 2;
    signal(SIGPIPE, SIG_IGN);
    unsigned long repeats = 3;
    if (argc > 2) return 2;
    if (argc == 2) {
        char* end;
        repeats = strtoul(argv[1], &end, 10);
        if (*end || repeats == 0 || repeats > 100) return 2;
    }
    unsigned failed = 0;
    for (unsigned repeat = 0; repeat < repeats; ++repeat)
        for (unsigned mode = 0; mode < 4; ++mode)
            for (unsigned restricted = 0; restricted < 2; ++restricted)
                for (unsigned event = 0; event < 2; ++event)
                    if (!run_case(mode, restricted ? 100 : 0, event != 0, repeat % 3 == 2 ? 20 : repeat % 3)) ++failed;
    printf("pipe wake cases=%lu failed=%u\n", repeats * 16, failed);
    return failed != 0;
}
