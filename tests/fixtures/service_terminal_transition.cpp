// Ordinary terminal/job-control tests. Private PTYs and owned descendants only.
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <pty.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
volatile sig_atomic_t observed;
void notice(int) { observed = observed + 1; }
struct Report {
    pid_t pid, sid, pgid;
    int count;
    uid_t uid;
};
auto transfer(int fd, void* data, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(data);
    while (size != 0) {
        pollfd event{.fd = fd, .events = static_cast<short>(writing ? POLLOUT : POLLIN)};
        int ready = poll(&event, 1, 5000);
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) return false;
        ssize_t n = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= static_cast<size_t>(n);
    }
    return true;
}
auto report() -> bool {
    Report value{getpid(), getsid(0), getpgrp(), observed, getuid()};
    return transfer(1, &value, sizeof(value), true);
}
auto target(const char* mode) -> int {
    alarm(20);
    struct sigaction action{};
    action.sa_handler = notice;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, nullptr) || sigaction(SIGHUP, &action, nullptr) || !report()) return 2;
    char gate = 0;
    if (!transfer(0, &gate, 1, false) || gate != 'A') return 3;
    for (int i = 0; i < 3000 && observed != 1; ++i) usleep(1000);
    if (observed != 1 || !report()) return 4;
    if (!strcmp(mode, "group") && setpgid(0, 0)) return 5;
    if (!strcmp(mode, "session") && setsid() < 0) return 6;
    if (!strcmp(mode, "uid") && (setgroups(0, nullptr) || setgid(200) || setuid(200))) return 9;
    if (!report() || !transfer(0, &gate, 1, false) || gate != 'B') return 7;
    // The first event established an actual working terminal delivery path.
    // Bound the second observation; report the count rather than hiding failure.
    for (int i = 0; i < 300; ++i) usleep(1000);
    return report() ? 0 : 8;
}
auto reap(pid_t pid, int& status) -> bool {
    for (int i = 0; i < 6000; ++i) {
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return true;
        if (result < 0 && errno != EINTR) return false;
        usleep(1000);
    }
    return false;
}
auto controller(bool remote, bool drop, bool hangup, const char* mode, const char* self, const char* peer, const char* remote_self)
    -> bool {
    alarm(25);
    if (signal(SIGINT, SIG_IGN) == SIG_ERR || signal(SIGHUP, SIG_IGN) == SIG_ERR || setsid() < 0) return false;
    if (drop && (setgroups(0, nullptr) || setgid(104) || setuid(104))) return false;
    int master = -1, slave = -1;
    std::array<int, 2> replies{}, commands{};
    if (openpty(&master, &slave, nullptr, nullptr, nullptr) || ioctl(slave, TIOCSCTTY, 0) || pipe2(replies.data(), O_CLOEXEC) ||
        pipe2(commands.data(), O_CLOEXEC))
        return false;
    pid_t child = fork();
    if (child == 0) {
        close(master);
        close(slave);
        if (dup2(replies[1], 1) < 0 || dup2(commands[0], 0) < 0) _exit(2);
        if (remote)
            execl("/usr/bin/on", "on", peer, remote_self, "--target", mode, nullptr);
        else
            execl(self, self, "--target", mode, nullptr);
        _exit(3);
    }
    close(replies[1]);
    close(commands[0]);
    if (child < 0) return false;
    Report initial{}, first{}, changed{}, final{};
    bool ready = transfer(replies[0], &initial, sizeof(initial), false) && initial.sid == getsid(0) && initial.pgid == getpgrp();
    char intr = 3, gate = 'A';
    bool positive = ready && transfer(master, &intr, 1, true) && transfer(commands[1], &gate, 1, true) &&
                    transfer(replies[0], &first, sizeof(first), false) && first.count == 1;
    bool transition = positive && transfer(replies[0], &changed, sizeof(changed), false);
    if (transition) {
        if (!strcmp(mode, "session"))
            transition = changed.sid == changed.pid && changed.pgid == changed.pid;
        else if (!strcmp(mode, "group"))
            transition = changed.sid == initial.sid && changed.pgid == changed.pid;
        else
            transition = changed.sid == initial.sid && changed.pgid == initial.pgid;
    }
    if (!strcmp(mode, "uid")) transition = transition && changed.uid == 200;
    gate = 'B';
    bool sent = false;
    if (transition) {
        if (hangup) {
            sent = close(master) == 0;
            master = -1;
        } else
            sent = transfer(master, &intr, 1, true);
    }
    bool received = transition && sent && transfer(commands[1], &gate, 1, true) && transfer(replies[0], &final, sizeof(final), false);
    close(commands[1]);
    close(replies[0]);
    int status = 0;
    if (!received) kill(child, SIGKILL);
    bool reaped = reap(child, status);
    if (!reaped) {
        kill(child, SIGKILL);
        reaped = reap(child, status);
    }
    close(slave);
    if (master >= 0) close(master);
    int expected = (!strcmp(mode, "inherit") || !strcmp(mode, "uid")) ? 2 : 1;
    bool pass = received && final.count == expected && reaped && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf(
        "%s placement=%s uid=%u mode=%s event=%s target_uid=%u source_pid=%ld target_pid=%ld before=%ld/%ld after=%ld/%ld "
        "positive=%d transition=%d observed=%d expected=%d status=%d reaped=%d\n",
        pass ? "PASS" : "FAIL", remote ? "remote" : "local", getuid(), mode, hangup ? "hangup" : "intr", changed.uid,
        static_cast<long>(child), static_cast<long>(initial.pid), static_cast<long>(initial.sid), static_cast<long>(initial.pgid),
        static_cast<long>(changed.sid), static_cast<long>(changed.pgid), positive, transition, final.count, expected, status, reaped);
    fflush(stdout);
    return pass;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && !strcmp(argv[1], "--target")) return target(argv[2]);
    if (argc != 3) return 2;
    int failures = 0;
    for (bool hangup : {false, true}) {
        for (bool remote : {false, true}) {
            for (bool drop : {false, true}) {
                for (const char* mode : {"inherit", "group", "session", "uid"}) {
                    if (drop && !strcmp(mode, "uid")) continue;
                    pid_t child = fork();
                    if (!child) _exit(controller(remote, drop, hangup, mode, argv[0], argv[1], argv[2]) ? 0 : 1);
                    if (child < 0) return 3;
                    int status = 0;
                    pid_t waited;
                    do {
                        waited = waitpid(child, &status, 0);
                    } while (waited < 0 && errno == EINTR);
                    bool reaped = waited == child;
                    printf("controller reaped=%d status=%d\n", reaped, status);
                    fflush(stdout);
                    if (!reaped || !WIFEXITED(status) || WEXITSTATUS(status)) ++failures;
                }
            }
        }
    }
    return failures ? 1 : 0;
}
