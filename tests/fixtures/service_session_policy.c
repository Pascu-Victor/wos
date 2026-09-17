/* Disposable WOS VM: all processes, terminals and signal targets are owned.
 * For --drop, set TMPDIR to a private directory owned by UID/GID104 before
 * launch; the production /tmp may intentionally deny creation by that user.
 */
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <pty.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>

// WOS exports this ioctl in dev/pty.hpp; current libc headers omit the name.
enum { WOS_TIOCNOTTY = 0x5422 };

#define CHECK(x)                                                                                 \
    do {                                                                                         \
        if (!(x)) {                                                                              \
            fprintf(stderr, "session check line %d failed: %s errno=%d\n", __LINE__, #x, errno); \
            return 1;                                                                            \
        }                                                                                        \
    } while (0)
static const char* executable;
static pid_t children[8];
static unsigned child_count;
static volatile sig_atomic_t hangup_signals;
static void on_hangup(int signal) { hangup_signals |= signal == SIGHUP ? 1 : 2; }
static pid_t child_fork(void) {
    pid_t pid = fork();
    if (!pid)
        child_count = 0;
    else if (pid > 0 && child_count < 8)
        children[child_count++] = pid;
    return pid;
}
static int reap(pid_t pid) {
    int status;
    pid_t waited;
    do {
        waited = waitpid(pid, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited != pid) return 1;
    for (unsigned i = 0; i < child_count; i++)
        if (children[i] == pid) children[i] = 0;
    while (child_count && !children[child_count - 1]) child_count--;
    return !WIFEXITED(status) || WEXITSTATUS(status);
}
static void cleanup_children(void) {
    for (unsigned i = 0; i < child_count; i++)
        if (children[i] > 0) {
            kill(children[i], SIGKILL);
            waitpid(children[i], NULL, 0);
        }
}
static int receive(int fd, void* buffer, size_t length) {
    char* cursor = buffer;
    while (length) {
        struct pollfd p = {.fd = fd, .events = POLLIN};
        int ready = poll(&p, 1, 5000);
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) return 1;
        ssize_t n = read(fd, cursor, length);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return 1;
        cursor += n;
        length -= (size_t)n;
    }
    return 0;
}
static int reply(int fd, int64_t value) { return write(fd, &value, sizeof(value)) != sizeof(value); }
static int rpc(int request, int response, char command, int64_t expected) {
    int64_t value;
    return write(request, &command, 1) != 1 || receive(response, &value, sizeof(value)) || value != expected;
}
static int alias_valid(int slave) {
    int expected = -1, observed = -1;
    int alias = open("/dev/tty", O_RDWR);
    if (alias < 0) return 0;
    int okay = !ioctl(slave, TIOCGPTN, &expected) && !ioctl(alias, TIOCGPTN, &observed) && expected == observed;
    close(alias);
    return okay;
}
static int child_loop(int request, int response, int slave) {
    if (reply(response, 0)) return 1;
    for (;;) {
        char command;
        CHECK(!receive(request, &command, 1));
        int64_t value = 0;
        if (command == 'G') {
            if (setpgid(0, 0)) value = errno;
        } else if (command == 'P')
            value = getpgrp();
        else if (command == 'C') {
            errno = 0;
            value = ioctl(slave, TIOCSCTTY, 0) < 0 ? errno : 0;
        } else if (command == 'N') {
            CHECK(!ioctl(slave, WOS_TIOCNOTTY, 0));
            errno = 0;
            int fd = open("/dev/tty", O_RDWR);
            CHECK(fd == -1 && errno == ENXIO);
        } else if (command == 'E') {
            char in[24], out[24], tty[24];
            snprintf(in, sizeof(in), "%d", request);
            snprintf(out, sizeof(out), "%d", response);
            snprintf(tty, sizeof(tty), "%d", slave);
            execl(executable, executable, "--child", in, out, tty, (char*)NULL);
            return 1;
        }
        CHECK(!reply(response, value));
        if (command == 'Q') return 0;
    }
}
static int group_and_exec(void) {
    CHECK(setsid() == getpid());
    int master, slave, request[2], response[2];
    CHECK(!openpty(&master, &slave, NULL, NULL, NULL));
    CHECK(!ioctl(slave, TIOCSCTTY, 0));
    CHECK(!pipe(request) && !pipe(response));
    pid_t child = child_fork();
    CHECK(child >= 0);
    if (!child) {
        close(request[1]);
        close(response[0]);
        _exit(child_loop(request[0], response[1], slave));
    }
    close(request[0]);
    close(response[1]);
    int64_t ready = -1;
    CHECK(!receive(response[0], &ready, sizeof(ready)) && !ready);
    CHECK(!rpc(request[1], response[0], 'C', EPERM));
    CHECK(!rpc(request[1], response[0], 'G', 0));
    CHECK(getpgid(child) == child);
    errno = 0;
    CHECK(setpgid(child, 0x7fffffff) == -1 && errno == EPERM);
    CHECK(getpgid(child) == child);
    CHECK(!setpgid(child, getpgrp()));
    CHECK(getpgid(child) == getpgrp());
    CHECK(!rpc(request[1], response[0], 'N', 0));
    CHECK(alias_valid(slave));
    CHECK(!rpc(request[1], response[0], 'E', 0));
    errno = 0;
    CHECK(setpgid(child, child) == -1 && errno == EACCES);
    CHECK(getpgid(child) == getpgrp());
    CHECK(!rpc(request[1], response[0], 'Q', 0));
    CHECK(!reap(child));
    errno = 0;
    CHECK(setpgid(0, 0) == -1 && errno == EPERM);
    errno = 0;
    CHECK(setsid() == -1 && errno == EPERM);
    struct sigaction action = {0};
    action.sa_handler = on_hangup;
    sigemptyset(&action.sa_mask);
    CHECK(!sigaction(SIGHUP, &action, NULL) && !sigaction(SIGCONT, &action, NULL));
    CHECK(!ioctl(slave, WOS_TIOCNOTTY, 0));
    for (int i = 0; i < 100 && hangup_signals != 3; i++) usleep(1000);
    CHECK(hangup_signals == 3);
    errno = 0;
    CHECK(open("/dev/tty", O_RDWR) == -1 && errno == ENXIO);
    CHECK(tcgetpgrp(slave) == 0);
    CHECK(!ioctl(slave, TIOCSCTTY, 0) && alias_valid(slave));
    close(master);
    close(slave);
    close(request[1]);
    close(response[0]);
    return 0;
}
static int foreign_session(void) {
    CHECK(setsid() == getpid());
    int master, slave, request[2], response[2];
    CHECK(!openpty(&master, &slave, NULL, NULL, NULL) && !ioctl(slave, TIOCSCTTY, 0));
    CHECK(!pipe(request) && !pipe(response));
    pid_t child = child_fork();
    CHECK(child >= 0);
    if (!child) {
        close(request[1]);
        close(response[0]);
        CHECK(setsid() == getpid());
        errno = 0;
        CHECK(ioctl(slave, TIOCSCTTY, 0) == -1 && errno == EPERM);
        errno = 0;
        CHECK(ioctl(slave, TIOCSCTTY, 1) == -1 && errno == EPERM);
        errno = 0;
        CHECK(tcsetpgrp(slave, getpgrp()) == -1 && errno == ENOTTY);
        errno = 0;
        CHECK(ioctl(slave, WOS_TIOCNOTTY, 0) == -1 && errno == ENOTTY);
        CHECK(!reply(response[1], 0));
        char quit;
        CHECK(!receive(request[0], &quit, 1));
        _exit(0);
    }
    close(request[0]);
    close(response[1]);
    int64_t ready = -1;
    CHECK(!receive(response[0], &ready, sizeof(ready)) && !ready);
    pid_t original = tcgetpgrp(slave);
    errno = 0;
    CHECK(tcsetpgrp(slave, child) == -1 && errno == EPERM);
    CHECK(tcgetpgrp(slave) == original && alias_valid(slave));
    errno = 0;
    CHECK(tcsetpgrp(slave, -1) == -1 && errno == EINVAL);
    CHECK(tcgetpgrp(slave) == original);
    CHECK(write(request[1], "q", 1) == 1 && !reap(child));
    close(master);
    close(slave);
    close(request[1]);
    close(response[0]);
    return 0;
}
struct contender {
    int slave;
    int sid_ok;
    int claim_ok;
    int error;
};
static int start;
static void* contend(void* opaque) {
    struct contender* c = opaque;
    while (!__atomic_load_n(&start, __ATOMIC_ACQUIRE)) usleep(100);
    errno = 0;
    pid_t sid = setsid();
    c->sid_ok = sid == getpid();
    if (!c->sid_ok && (sid != -1 || errno != EPERM)) c->error = 1;
    errno = 0;
    int rc = ioctl(c->slave, TIOCSCTTY, 0);
    c->claim_ok = rc == 0;
    if (!c->claim_ok && (rc != -1 || errno != EPERM)) c->error = 2;
    return NULL;
}
static int thread_race(void) {
    int master[2], slave[2];
    pthread_t threads[2];
    struct contender c[2] = {0};
    for (int i = 0; i < 2; i++) {
        CHECK(!openpty(&master[i], &slave[i], NULL, NULL, NULL));
        c[i].slave = slave[i];
        CHECK(!pthread_create(&threads[i], NULL, contend, &c[i]));
    }
    __atomic_store_n(&start, 1, __ATOMIC_RELEASE);
    for (int i = 0; i < 2; i++) CHECK(!pthread_join(threads[i], NULL));
    CHECK(!c[0].error && !c[1].error && c[0].sid_ok + c[1].sid_ok == 1 && c[0].claim_ok + c[1].claim_ok == 1);
    CHECK(getsid(0) == getpid() && getpgrp() == getpid());
    int winner = c[0].claim_ok ? 0 : 1;
    CHECK(alias_valid(slave[winner]));
    CHECK(tcgetpgrp(slave[winner]) == getpid() && tcgetpgrp(slave[1 - winner]) == 0);
    CHECK(!tcsetpgrp(slave[winner], getpgrp()));
    for (int i = 0; i < 2; i++) {
        close(master[i]);
        close(slave[i]);
    }
    return 0;
}
static int signal_session_boundary(void) {
    pid_t parent = getppid();
    CHECK(parent > 1 && getsid(parent) == getsid(0));
    CHECK(!setgroups(0, NULL) && !setgid(104) && !setuid(104));
    errno = 0;
    CHECK(kill(parent, 0) == -1 && errno == EPERM);
    CHECK(!kill(parent, SIGCONT));
    CHECK(setsid() == getpid());
    errno = 0;
    CHECK(kill(parent, SIGCONT) == -1 && errno == EPERM);
    return 0;
}

static int spawn_group_denial(void) {
    char path[512];
    const char* directory = getenv("TMPDIR");
    int length = snprintf(path, sizeof(path), "%s/goal19-spawn-group-%ld", directory ? directory : "/tmp", (long)getpid());
    CHECK(length > 0 && (size_t)length < sizeof(path));
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    CHECK(fd >= 0 && write(fd, "unchanged", 9) == 9);
    close(fd);
    posix_spawnattr_t attributes;
    posix_spawn_file_actions_t actions;
    CHECK(!posix_spawnattr_init(&attributes) && !posix_spawn_file_actions_init(&actions));
    CHECK(!posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP));
    CHECK(!posix_spawnattr_setpgroup(&attributes, 0x7fffffff));
    CHECK(!posix_spawn_file_actions_addopen(&actions, 1, path, O_WRONLY | O_TRUNC, 0600));
    char* arguments[] = {(char*)executable, "--exit", NULL};
    char* environment[] = {"PATH=/usr/bin:/bin", NULL};
    pid_t child = -77;
    char content[10] = {0};
    for (int fallback = 0; fallback < 2; fallback++) {
        for (int negative = 0; negative < 2; negative++) {
            CHECK(!posix_spawnattr_setpgroup(&attributes, negative ? -1 : 0x7fffffff));
            child = -77;
            int error = posix_spawn(&child, executable, fallback ? &actions : NULL, &attributes, arguments, environment);
            int expected = negative ? EINVAL : EPERM;
            if (error != expected)
                fprintf(stderr, "spawn fallback=%d negative=%d returned %d child=%ld\n", fallback, negative, error, (long)child);
            CHECK(error == expected && child == -77);
            fd = open(path, O_RDONLY);
            CHECK(fd >= 0 && read(fd, content, sizeof(content)) == 9 && !memcmp(content, "unchanged", 9));
            close(fd);
        }
    }
    CHECK(!posix_spawnattr_setpgroup(&attributes, 0));
    CHECK(!posix_spawn(&child, executable, &actions, &attributes, arguments, environment));
    children[child_count++] = child;
    CHECK(!reap(child));
    fd = open(path, O_RDONLY);
    CHECK(fd >= 0 && read(fd, content, sizeof(content)) == 0);
    close(fd);
    CHECK(!unlink(path));
    posix_spawn_file_actions_destroy(&actions);
    posix_spawnattr_destroy(&attributes);
    return 0;
}

static int run_case(int (*test)(void), const char* label) {
    pid_t pid = child_fork();
    CHECK(pid >= 0);
    if (!pid) {
        // Most cases close their owned master; dedicated lifetime tests check HUP.
        CHECK(signal(SIGHUP, SIG_IGN) != SIG_ERR);
        int result = test();
        cleanup_children();
        _exit(result);
    }
    CHECK(!reap(pid));
    printf("PASS %s\n", label);
    fflush(stdout);
    return 0;
}
int main(int argc, char** argv) {
    executable = argv[0];
    if (argc == 2 && !strcmp(argv[1], "--exit")) return 0;
    if (argc == 5 && !strcmp(argv[1], "--child")) return child_loop(atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
    if (argc == 2 && !strcmp(argv[1], "--drop")) CHECK(!setgroups(0, NULL) && !setgid(104) && !setuid(104));
    int result = run_case(spawn_group_denial, "spawn group denial precedes truncation and preserves output PID");
    if (!result && geteuid() == 0) result = run_case(signal_session_boundary, "same-session SIGCONT exception and foreign-session denial");
    if (!result) result = run_case(group_and_exec, "group/session/nonleader detach/exec admission and leader hangup");
    if (!result) result = run_case(foreign_session, "foreign session and privileged-steal attempts preserve terminal state");
    for (int i = 0; !result && i < 16; i++)
        result = run_case(thread_race, "concurrent thread setsid and competing terminal claims share one winner");
    cleanup_children();
    return result;
}
