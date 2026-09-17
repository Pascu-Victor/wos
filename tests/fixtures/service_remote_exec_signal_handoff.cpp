#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
constexpr char CONTROL[] = "/proc/wki/chaos_workload";
struct Report {
    int error{}, pending{}, delivered{};
    pid_t pid{}, pgid{};
    uid_t real{}, effective{}, saved{};
    std::array<char, 64> host{};
};
volatile sig_atomic_t delivered;
char* replacement;
char** replacement_args;
auto now_ms() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t)) _exit(90);
    return static_cast<uint64_t>(t.tv_sec) * 1000 + t.tv_nsec / 1000000;
}
auto pathname(const char* directory, const char* leaf) -> std::array<char, 1200> {
    std::array<char, 1200> value{};
    int n = snprintf(value.data(), value.size(), "%s/%s", directory, leaf);
    if (n < 0 || static_cast<size_t>(n) >= value.size()) _exit(91);
    return value;
}
auto store(const char* directory, const char* leaf, const void* value, size_t size) -> bool {
    auto name = pathname(directory, leaf);
    int fd = open(name.data(), O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd < 0) return false;
    bool ok = write(fd, value, size) == static_cast<ssize_t>(size);
    return close(fd) == 0 && ok;
}
auto load(const char* directory, const char* leaf, void* value, size_t size, unsigned timeout) -> bool {
    auto name = pathname(directory, leaf);
    auto deadline = now_ms() + timeout;
    do {
        int fd = open(name.data(), O_RDONLY);
        if (fd >= 0) {
            bool ok = read(fd, value, size) == static_cast<ssize_t>(size);
            close(fd);
            if (ok) return true;
        }
        usleep(1000);
    } while (now_ms() < deadline);
    return false;
}
auto hold(bool enabled, bool committed = false) -> bool {
    const char* command = !enabled    ? "compute-publish hold=0"
                          : committed ? "compute-publish hold=1 stage=commit"
                                      : "compute-publish hold=1";
    int fd = open(CONTROL, O_WRONLY);
    if (fd < 0) return false;
    bool ok = write(fd, command, strlen(command)) == static_cast<ssize_t>(strlen(command));
    return close(fd) == 0 && ok;
}
auto controller() -> int {
    alarm(180);
    if (!hold(false)) return 2;
    puts("CONTROLLER_READY");
    bool committed = false;
    for (;;) {
        int command = getchar();
        if (command == '\n') continue;
        if (command == EOF || command == 'Q') {
            bool ok = hold(false);
            puts("CONTROLLER_DONE");
            return ok ? 0 : 3;
        }
        if (command == 'H' || command == 'C' || command == 'R') {
            committed = command == 'C';
            if (!hold(command != 'R', committed)) return 4;
            puts(command != 'R' ? "HOLD_ENABLED" : "HOLD_RELEASED");
        } else if (command == 'W') {
            auto deadline = now_ms() + 2000;
            bool ready = false;
            do {
                std::array<char, 2048> text{};
                int fd = open(CONTROL, O_RDONLY);
                if (fd < 0) return 5;
                auto size = read(fd, text.data(), text.size() - 1);
                close(fd);
                if (size <= 0) return 6;
                const char* waiters = strstr(text.data(), "compute_waiters=");
                ready = strstr(text.data(), "enabled=1 ") && strstr(text.data(), "compute_hold=1 ") && waiters &&
                        strtoul(waiters + strlen("compute_waiters="), nullptr, 10) > 0 &&
                        strstr(text.data(), committed ? "compute_stage=2 " : "compute_stage=1 ");
                if (ready) break;
                usleep(1000);
            } while (now_ms() < deadline);
            puts(ready ? "WAITER_CONFIRMED" : "WAITER_TIMEOUT");
        } else
            return 7;
    }
}
void handler(int) { delivered = 1; }
auto report(int error = 0) -> Report {
    Report value{.error = error, .pid = getpid(), .pgid = getpgrp()};
    if (getresuid(&value.real, &value.effective, &value.saved) || gethostname(value.host.data(), value.host.size())) _exit(92);
    return value;
}
auto payload(const char* directory) -> int {
    alarm(20);
    auto value = report();
    if (!store(directory, "ready", &value, sizeof(value))) return 3;
    char inspect{};
    if (!load(directory, "inspect", &inspect, 1, 15000) || inspect != 'D') return 4;
    sigset_t pending{}, release{};
    if (sigpending(&pending)) return 5;
    value.pending = sigismember(&pending, SIGUSR2);
    struct sigaction action{};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    sigemptyset(&release);
    sigaddset(&release, SIGUSR2);
    if (sigaction(SIGUSR2, &action, nullptr) || sigprocmask(SIG_UNBLOCK, &release, nullptr)) return 6;
    auto deadline = now_ms() + 1000;
    while (value.pending == 1 && !delivered && now_ms() < deadline) usleep(1000);
    value.delivered = delivered;
    if (!store(directory, "result", &value, sizeof(value))) return 7;
    return value.pending == 1 && value.delivered == 1 ? 0 : 1;
}
[[noreturn]] void replace() {
    execv(replacement, replacement_args);
    auto value = report(errno);
    store(replacement_args[2], "result", &value, sizeof(value));
    _exit(8);
}
void* worker(void*) { replace(); }
auto run_case(uid_t uid, bool remote, bool worker_exec, char* local_image, char* remote_image, char* directory, char* peer, char* owner,
              bool group) -> int {
    if (setgroups(0, nullptr) || setgid(uid) || setuid(uid)) return 2;
    alarm(35);
    int started[2], prepared[2];
    if (pipe(started) || pipe(prepared)) return 3;
    std::array<char, 1200> destination{};
    snprintf(destination.data(), destination.size(), remote ? "/wki/%s%s" : "%s%s", remote ? owner : "", directory);
    char flag[] = "--payload";
    replacement = remote ? remote_image : local_image;
    char* args[] = {replacement, flag, destination.data(), nullptr};
    replacement_args = args;
    pid_t child = fork();
    if (child == 0) {
        if (group && setpgid(0, 0)) _exit(13);
        close(started[1]);
        close(prepared[0]);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        sigset_t blocked{};
        sigemptyset(&blocked);
        sigaddset(&blocked, SIGUSR2);
        if (sigprocmask(SIG_SETMASK, &blocked, nullptr)) _exit(9);
        if (remote ? ker::process::setwkitarget(peer, strlen(peer), ker::process::WKI_TARGET_FLAG_STRICT)
                   : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL))
            _exit(10);
        char token = 'Y';
        if (write(prepared[1], &token, 1) != 1 || read(started[0], &token, 1) != 1 || token != 'G') _exit(11);
        close(prepared[1]);
        close(started[0]);
        if (!worker_exec) replace();
        pthread_t thread;
        if (pthread_create(&thread, nullptr, worker, nullptr)) _exit(12);
        for (;;) usleep(1000);
    }
    close(started[0]);
    close(prepared[1]);
    if (child < 0) return 4;
    char token{};
    if (read(prepared[0], &token, 1) != 1 || token != 'Y') return 5;
    close(prepared[0]);
    printf("SOURCE_READY pid=%ld uid=%u group=%d\n", static_cast<long>(child), uid, group);
    for (;;) {
        int command = getchar();
        if (command == '\n') continue;
        if (command == 'S' || command == 'Q') {
            errno = 0;
            int result = kill(group ? -child : child, command == 'S' ? SIGUSR2 : 0);
            printf("%s result=%d errno=%d\n", command == 'S' ? "SIGNAL" : "QUERY", result, errno);
        } else if (command == 'G') {
            token = 'G';
            if (write(started[1], &token, 1) != 1) return 6;
            close(started[1]);
            puts("SOURCE_STARTED");
        } else if (command == 'P') {
            Report value{};
            bool ready = load(directory, "ready", &value, sizeof(value), 10000);
            auto deadline = now_ms() + 2000;
            bool accepted = !remote;
            while (ready && !accepted && now_ms() < deadline) {
                std::array<char, 64> name{};
                std::array<char, 1024> channel{};
                snprintf(name.data(), name.size(), "/proc/%ld/stat", static_cast<long>(child));
                int fd = open(name.data(), O_RDONLY);
                if (fd >= 0) {
                    auto count = read(fd, channel.data(), channel.size() - 1);
                    close(fd);
                    accepted = count > 0 && strstr(channel.data(), "wki_execve_proxy");
                }
                if (!accepted) usleep(1000);
            }
            printf("PAYLOAD_READY ready=%d accepted=%d receiver=%ld\n", ready, accepted, static_cast<long>(value.pid));
        } else if (command == 'D' || command == EOF) {
            token = 'D';
            bool released = store(directory, "inspect", &token, 1);
            int status = 0;
            auto deadline = now_ms() + 10000;
            bool reaped = false;
            do {
                auto got = waitpid(child, &status, WNOHANG);
                if (got == child) {
                    reaped = true;
                    break;
                }
                if (got < 0 && errno != EINTR) break;
                usleep(1000);
            } while (now_ms() < deadline);
            if (!reaped) {
                kill(child, SIGKILL);
                reaped = waitpid(child, &status, 0) == child;
            }
            Report value{};
            bool found = reaped && load(directory, "result", &value, sizeof(value), 1000);
            bool identity = found && value.real == uid && value.effective == uid && value.saved == uid &&
                            strcmp(value.host.data(), remote ? peer : owner) == 0 && (!group || value.pgid == child);
            bool pass = released && found && identity && value.error == 0 && value.pending == 1 && value.delivered == 1 &&
                        WIFEXITED(status) && WEXITSTATUS(status) == 0;
            printf(
                "%s source=%ld receiver=%ld host=%s uid=%u/%u/%u identity=%d exec_error=%d pending=%d delivered=%d status=%d found=%d "
                "pgid=%ld reaped=%d\n",
                pass ? "PASS" : "FAIL", static_cast<long>(child), static_cast<long>(value.pid), value.host.data(), value.real,
                value.effective, value.saved, identity, value.error, value.pending, value.delivered, status, found,
                static_cast<long>(value.pgid), reaped);
            return reaped && found ? (pass ? 0 : 1) : 2;
        } else
            return 7;
    }
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);
    if (argc == 2 && strcmp(argv[1], "--controller") == 0) return controller();
    if (argc == 3 && strcmp(argv[1], "--payload") == 0) return payload(argv[2]);
    if (argc != 8 && !(argc == 9 && strcmp(argv[8], "--group") == 0)) return 2;
    return run_case(static_cast<uid_t>(strtoul(argv[1], nullptr, 10)), strcmp(argv[2], "remote") == 0, strcmp(argv[3], "worker") == 0,
                    argv[0], argv[4], argv[5], argv[6], argv[7], argc == 9);
}
