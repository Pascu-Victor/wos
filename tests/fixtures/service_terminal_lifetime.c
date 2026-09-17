/* Disposable WOS VM: only owned children/descendants and private PTYs. */
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <pty.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>

static const char* executable;
static volatile sig_atomic_t received_signals;
static void notice(int signal) { received_signals |= signal == SIGHUP ? 1 : 2; }
#define CHECK(x)                                                                              \
    do {                                                                                      \
        if (!(x)) {                                                                           \
            fprintf(stderr, "terminal lifetime line %d: %s errno=%d\n", __LINE__, #x, errno); \
            return 1;                                                                         \
        }                                                                                     \
    } while (0)
struct message {
    int kind, result;
    pid_t member;
};
static int receive(int fd, void* data, size_t size) {
    char* p = data;
    while (size) {
        struct pollfd event = {.fd = fd, .events = POLLIN};
        int ready = poll(&event, 1, 6000);
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) return 1;
        ssize_t n = read(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return 1;
        p += n;
        size -= n;
    }
    return 0;
}
static pid_t bounded_wait(pid_t child, int* status, int flags) {
    for (int i = 0; i < 6000; i++) {
        pid_t result = waitpid(child, status, flags | WNOHANG);
        if (result != 0 && !(result < 0 && errno == EINTR)) return result;
        usleep(1000);
    }
    return 0;
}
static int reap(pid_t child) {
    int status = 0;
    pid_t result = bounded_wait(child, &status, 0);
    if (!result) {
        kill(child, SIGKILL);
        (void)bounded_wait(child, &status, 0);
        return 1;
    }
    return result != child || !WIFEXITED(status) || WEXITSTATUS(status);
}
static int member(int slave, int ready, int report) {
    struct sigaction action = {0};
    action.sa_handler = notice;
    sigemptyset(&action.sa_mask);
    CHECK(!sigaction(SIGHUP, &action, NULL) && !sigaction(SIGCONT, &action, NULL));
    CHECK(!setpgid(0, 0));
    CHECK(write(ready, "r", 1) == 1);
    close(ready);
    CHECK(!raise(SIGSTOP));
    for (int i = 0; i < 5000 && received_signals != 3; i++) usleep(1000);
    errno = 0;
    int alias = open("/dev/tty", O_RDWR);
    int okay = received_signals == 3 && alias == -1 && errno == ENXIO;
    if (alias >= 0) close(alias);
    struct message done = {.kind = 2, .result = !okay, .member = getpid()};
    CHECK(write(report, &done, sizeof(done)) == sizeof(done));
    close(slave);
    close(report);
    return !okay;
}
static struct {
    pthread_t main_thread;
    int command, report;
    pid_t process, foreground;
    uid_t uid;
    gid_t gid;
} worker_state;

static void* terminal_worker(void* unused) {
    (void)unused;
    int failed = pthread_join(worker_state.main_thread, NULL) != 0;
    int alias = open("/dev/tty", O_RDWR);
    failed |= alias < 0 || getpid() != worker_state.process || getsid(0) != worker_state.process || getuid() != worker_state.uid ||
              geteuid() != worker_state.uid || getgid() != worker_state.gid || getegid() != worker_state.gid;
    if (alias >= 0) {
        failed |= tcgetpgrp(alias) != worker_state.foreground;
        close(alias);
    }
    struct message ready = {.kind = 3, .result = failed, .member = worker_state.foreground};
    if (write(worker_state.report, &ready, sizeof(ready)) != sizeof(ready)) _exit(2);
    char token = 0;
    if (failed || receive(worker_state.command, &token, 1) || token != 'X') _exit(3);
    // Returning retires the final worker; the retained process owner must revoke its terminal.
    return NULL;
}

static int leader(int master, int slave, int command, int report, int mode) {
    close(master);
    CHECK(setsid() == getpid());
    CHECK(!ioctl(slave, TIOCSCTTY, 0));
    int ready[2];
    CHECK(!pipe(ready));
    pid_t child = fork();
    CHECK(child >= 0);
    if (!child) {
        close(ready[0]);
        close(command);
        _exit(member(slave, ready[1], report));
    }
    close(ready[1]);
    char token = 0;
    if (receive(ready[0], &token, 1) || tcsetpgrp(slave, child)) {
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    close(ready[0]);
    int stopped = 0;
    pid_t observed = bounded_wait(child, &stopped, WUNTRACED);
    if (observed != child || !WIFSTOPPED(stopped)) {
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    struct message started = {.kind = 1, .result = 0, .member = child};
    if (write(report, &started, sizeof(started)) != sizeof(started)) {
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    if (mode == 3) {
        worker_state.main_thread = pthread_self();
        worker_state.command = command;
        worker_state.report = report;
        worker_state.process = getpid();
        worker_state.foreground = child;
        worker_state.uid = getuid();
        worker_state.gid = getgid();
        pthread_t worker;
        if (pthread_create(&worker, NULL, terminal_worker, NULL)) {
            kill(child, SIGKILL);
            reap(child);
            return 1;
        }
        pthread_exit(NULL);
    }
    if (receive(command, &token, 1)) {
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    if (token == 'X') _exit(0);  // Deliberately exercise leader exit with a surviving descendant.
    int result = reap(child);
    close(command);
    close(report);
    close(slave);
    return result;
}
static int finish_hangup(int report, int command, pid_t child, int slave, int master) {
    struct message done = {0};
    int result = receive(report, &done, sizeof(done)) || done.kind != 2 || done.result;
    if (!result && master >= 0) {
        // A different session leader can acquire the still-open terminal after exit.
        result = ioctl(slave, TIOCSCTTY, 0) != 0;
        int alias = open("/dev/tty", O_RDWR);
        result |= alias < 0;
        if (alias >= 0) close(alias);
    }
    if (master < 0) {
        if (write(command, "Q", 1) != 1) result = 1;
    }
    if (result) kill(child, SIGKILL);
    result |= reap(child);
    close(report);
    close(command);
    close(slave);
    if (master >= 0) close(master);
    return result;
}
static int lifetime_case(int mode) {
    CHECK(setsid() == getpid());
    int master, slave, command[2], report[2];
    CHECK(!openpty(&master, &slave, NULL, NULL, NULL));
    CHECK(!pipe(command) && !pipe(report));
    pid_t child = fork();
    CHECK(child >= 0);
    if (!child) {
        close(command[1]);
        close(report[0]);
        _exit(leader(master, slave, command[0], report[1], mode));
    }
    close(command[0]);
    close(report[1]);
    struct message ready = {0};
    if (receive(report[0], &ready, sizeof(ready)) || ready.kind != 1) {
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    if (mode == 3) {
        struct message worker = {0};
        int status = 0;
        if (receive(report[0], &worker, sizeof(worker)) || worker.kind != 3 || worker.result || worker.member != ready.member ||
            waitpid(child, &status, WNOHANG) != 0) {
            kill(ready.member, SIGKILL);
            kill(child, SIGKILL);
            reap(child);
            return 1;
        }
        struct pollfd event = {.fd = report[0], .events = POLLIN};
        if (poll(&event, 1, 30) != 0) {
            kill(ready.member, SIGKILL);
            kill(child, SIGKILL);
            reap(child);
            return 1;
        }
    }
    if (mode == 0 || mode == 3) {
        CHECK(write(command[1], "X", 1) == 1);
        return finish_hangup(report[0], command[1], child, slave, master);
    }
    int duplicate = dup(master);
    CHECK(duplicate >= 0);
    close(master);
    struct pollfd event = {.fd = report[0], .events = POLLIN};
    CHECK(poll(&event, 1, 30) == 0);  // Closing one descriptor must not hang up a live master File.
    if (mode == 2) {
        CHECK(!fcntl(duplicate, F_SETFD, FD_CLOEXEC));
        char r[24], c[24], p[24], s[24];
        snprintf(r, sizeof(r), "%d", report[0]);
        snprintf(c, sizeof(c), "%d", command[1]);
        snprintf(p, sizeof(p), "%ld", (long)child);
        snprintf(s, sizeof(s), "%d", slave);
        execl(executable, executable, "--after-exec", r, c, p, s, (char*)NULL);
        kill(child, SIGKILL);
        reap(child);
        return 1;
    }
    close(duplicate);
    return finish_hangup(report[0], command[1], child, slave, -1);
}
int main(int argc, char** argv) {
    executable = argv[0];
    CHECK(signal(SIGHUP, SIG_IGN) != SIG_ERR);
    if (argc == 6 && !strcmp(argv[1], "--after-exec"))
        return finish_hangup(atoi(argv[2]), atoi(argv[3]), (pid_t)atol(argv[4]), atoi(argv[5]), -1);
    if (argc == 2 && !strcmp(argv[1], "--drop")) CHECK(!setgroups(0, NULL) && !setgid(104) && !setuid(104));
    const char* labels[] = {"leader exit revokes surviving member and permits a new session claim",
                            "last master close resumes and hangs up the stopped foreground; dup keeps it alive",
                            "CLOEXEC last-master teardown revokes without publication-lock recursion",
                            "worker keeps terminal after main pthread_exit; final worker exit revokes and permits a new claim"};
    for (int mode = 0; mode < 4; mode++) {
        pid_t child = fork();
        CHECK(child >= 0);
        if (!child) _exit(lifetime_case(mode));
        CHECK(!reap(child));
        printf("PASS %s\n", labels[mode]);
        fflush(stdout);
    }
    return 0;
}
