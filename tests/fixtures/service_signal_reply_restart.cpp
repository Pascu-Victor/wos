// Ordinary owned-process signal request interrupted by receiver VM restart.
#include <grp.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
struct Ready {
    pid_t pid{};
    uid_t uid{};
};
struct Reply {
    int result{}, error{};
    unsigned long long elapsed_ms{};
};
auto transfer(int fd, void* ptr, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(ptr);
    while (size) {
        ssize_t count = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        bytes += count;
        size -= count;
    }
    return true;
}
auto drop(uid_t uid) -> bool { return !uid || (!setgroups(0, nullptr) && !setgid(uid) && !setuid(uid)); }
auto now() -> unsigned long long {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return static_cast<unsigned long long>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}
auto reap(pid_t pid, int& status) -> pid_t {
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && !strcmp(argv[1], "--receiver")) {
        uid_t const uid = strtoul(argv[2], nullptr, 10);
        if (!drop(uid)) return 2;
        sigset_t mask{};
        sigemptyset(&mask);
        sigaddset(&mask, SIGUSR1);
        if (sigprocmask(SIG_BLOCK, &mask, nullptr)) return 3;
        Ready ready{getpid(), geteuid()};
        int const fd = atoi(argv[3]);
        if (!transfer(fd, &ready, sizeof(ready), true) || close(fd)) return 4;
        alarm(90);
        for (;;) pause();
    }
    if (argc != 6 || geteuid()) return 5;
    uid_t const caller = strtoul(argv[3], nullptr, 10), target = strtoul(argv[4], nullptr, 10);
    int const signum = atoi(argv[5]);
    if (signum != 0 && signum != SIGUSR1) return 6;
    std::array<int, 2> ready_pipe{}, result_pipe{};
    if (pipe(ready_pipe.data()) || pipe(result_pipe.data())) return 7;
    pid_t const child = fork();
    if (child == 0) {
        close(ready_pipe[0]);
        close(result_pipe[0]);
        close(result_pipe[1]);
        std::array<char, 24> uid{}, fd{};
        snprintf(uid.data(), uid.size(), "%u", target);
        snprintf(fd.data(), fd.size(), "%d", ready_pipe[1]);
        execl("/usr/bin/on", "on", argv[1], argv[2], "--receiver", uid.data(), fd.data(), nullptr);
        _exit(8);
    }
    close(ready_pipe[1]);
    Ready ready{};
    if (child <= 0 || !transfer(ready_pipe[0], &ready, sizeof(ready), false) || ready.uid != target || ready.pid <= 0) return 9;
    close(ready_pipe[0]);
    printf("READY child=%ld receiver=%ld caller=%u target=%u signum=%d\n", child, ready.pid, caller, target, signum);
    fflush(stdout);
    char gate{};
    if (!transfer(0, &gate, 1, false) || gate != 'G') return 10;
    pid_t const sender = fork();
    if (sender == 0) {
        close(result_pipe[0]);
        if (!drop(caller)) _exit(11);
        auto const started = now();
        errno = 0;
        Reply result{kill(child, signum), errno};
        result.elapsed_ms = now() - started;
        _exit(transfer(result_pipe[1], &result, sizeof(result), true) ? 0 : 12);
    }
    close(result_pipe[1]);
    Reply reply{};
    bool const got = sender > 0 && transfer(result_pipe[0], &reply, sizeof(reply), false);
    close(result_pipe[0]);
    int sender_status = -1, status = -1;
    bool const sender_reaped = sender > 0 && reap(sender, sender_status) == sender && sender_status == 0;
    auto const deadline = now() + 60000;
    pid_t waited;
    while ((waited = waitpid(child, &status, WNOHANG)) == 0 && now() < deadline) usleep(1000);
    bool const timely = waited == child;
    if (!timely) {
        kill(child, SIGKILL);
        waited = reap(child, status);
    }
    bool const transport_error = reply.error == EHOSTDOWN || (reply.error == ETIMEDOUT && reply.elapsed_ms >= 14000);
    bool const ok = got && sender_reaped && reply.result == -1 && transport_error && reply.elapsed_ms < 20000 && timely &&
                    waited == child && status == (255 << 8) && geteuid() == 0;
    printf("%s child=%ld receiver=%ld sender=%ld result=%d errno=%d elapsed_ms=%llu sender_reaped=%d timely=%d status=%d\n",
           ok ? "PASS" : "FAIL", child, ready.pid, sender, reply.result, reply.error, reply.elapsed_ms, sender_reaped, timely, status);
    return ok ? 0 : 13;
}
