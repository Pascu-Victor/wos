// Ordinary termination of an owned parent with a remote child-wait commit.
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
volatile sig_atomic_t exit_requested = 0;
void request_exit(int) { exit_requested = 1; }
auto now_ms() -> unsigned long long {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(27);
    return static_cast<unsigned long long>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}
auto marker(const char* path, const char* text) -> bool {
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) return false;
    bool const ok = write(fd, text, strlen(text)) == static_cast<ssize_t>(strlen(text));
    return close(fd) == 0 && ok;
}
void* wait_worker(void* argument) {
    int status = -1;
    pid_t result;
    do {
        result = waitpid(*static_cast<pid_t*>(argument), &status, 0);
    } while (result < 0 && errno == EINTR);
    printf("UNEXPECTED_WORKER_RETURN result=%ld status=%d errno=%d\n", result, status, errno);
    _exit(24);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && (!strcmp(argv[1], "--kill") || !strcmp(argv[1], "--exit"))) {
        unsigned const uid = strtoul(argv[3], nullptr, 10);
        if (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) return 20;
        errno = 0;
        int const result = kill(strtol(argv[2], nullptr, 10), !strcmp(argv[1], "--exit") ? SIGUSR1 : SIGKILL);
        printf("KILL uid=%u target=%s result=%d errno=%d\n", geteuid(), argv[2], result, errno);
        return result ? 21 : 0;
    }
    if (argc == 3 && !strcmp(argv[1], "--after-exec")) {
        std::array<char, 256> ready{}, release{}, done{};
        snprintf(ready.data(), ready.size(), "%s-%u/ready", argv[0], geteuid());
        snprintf(release.data(), release.size(), "%s-%u/release", argv[0], geteuid());
        snprintf(done.data(), done.size(), "%s-%u/done", argv[0], geteuid());
        std::array<char, 128> report{};
        snprintf(report.data(), report.size(), "READY pid=%ld uid=%u\n", getpid(), geteuid());
        if (!marker(ready.data(), report.data())) return 28;
        auto const deadline = now_ms() + 15000;
        while (access(release.data(), F_OK)) {
            if (now_ms() >= deadline) return 29;
            usleep(1000);
        }
        tms before{}, after{};
        bool ok = times(&before) != static_cast<clock_t>(-1);
        for (int i = 0; i < 3; ++i) {
            int status = -1;
            errno = 0;
            ok &= waitpid(strtol(argv[2], nullptr, 10), &status, WNOHANG) == -1 && errno == ECHILD;
        }
        ok &= times(&after) != static_cast<clock_t>(-1) && before.tms_cutime == after.tms_cutime && before.tms_cstime == after.tms_cstime;
        snprintf(report.data(), report.size(), "%s pid=%ld uid=%u child_ticks=%ld stable=1\n", ok ? "PASS" : "FAIL", getpid(), geteuid(),
                 after.tms_cutime + after.tms_cstime);
        if (!marker(done.data(), report.data())) return 30;
        return ok ? 23 : 31;
    }
    if (argc == 3 && (!strcmp(argv[1], "--receiver-worker") || !strcmp(argv[1], "--receiver-exec"))) {
        struct sigaction action{};
        action.sa_handler = request_exit;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGUSR1, &action, nullptr)) return 25;
        pid_t child = strtol(argv[2], nullptr, 10);
        pthread_t thread;
        if (pthread_create(&thread, nullptr, wait_worker, &child)) return 26;
        while (!exit_requested) usleep(1000);
        if (!strcmp(argv[1], "--receiver-exec")) {
            std::array<char, 256> local{};
            snprintf(local.data(), local.size(), "/tmp/%s", strrchr(argv[0], '/') + 1);
            execl(local.data(), local.data(), "--after-exec", argv[2], nullptr);
            _exit(32);
        }
        _exit(23);
    }
    if (argc == 3 && !strcmp(argv[1], "--receiver")) {
        int status = -1;
        pid_t result;
        do {
            result = waitpid(strtol(argv[2], nullptr, 10), &status, 0);
        } while (result < 0 && errno == EINTR);
        printf("UNEXPECTED_WAIT_RETURN pid=%ld result=%ld status=%d errno=%d\n", getpid(), result, status, errno);
        return 22;
    }
    bool const EXEC = argc == 5 && !strcmp(argv[4], "--worker-exec");
    bool const RESET = argc == 5 && !strcmp(argv[4], "--receiver-reset");
    bool const WORKER = EXEC || (argc == 5 && !strcmp(argv[4], "--worker-exit"));
    if (argc != 4 && !WORKER && !RESET) return 2;
    unsigned const uid = strtoul(argv[1], nullptr, 10);
    std::array<int, 2> report{};
    if (pipe(report.data())) return 3;
    pid_t const parent = fork();
    if (parent == 0) {
        close(report[0]);
        if (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) _exit(4);
        pid_t child = fork();
        if (child == 0) {
            if (EXEC) {
                auto const deadline = now_ms() + 150;
                while (now_ms() < deadline) asm volatile("" ::: "memory");
            }
            _exit(37);
        }
        if (child <= 0 || write(report[1], &child, sizeof(child)) != sizeof(child)) _exit(5);
        close(report[1]);
        std::array<char, 32> id{};
        snprintf(id.data(), id.size(), "%ld", child);
        execl("/usr/bin/on", "on", argv[2], argv[3],
              EXEC     ? "--receiver-exec"
              : WORKER ? "--receiver-worker"
                       : "--receiver",
              id.data(), nullptr);
        _exit(6);
    }
    close(report[1]);
    pid_t child = 0;
    if (parent <= 0 || read(report[0], &child, sizeof(child)) != sizeof(child)) return 7;
    close(report[0]);
    printf("CREATED uid=%u parent_source=%ld child_source=%ld\n", uid, parent, child);
    fflush(stdout);
    int status = -1;
    pid_t waited;
    do {
        waited = waitpid(parent, &status, 0);
    } while (waited < 0 && errno == EINTR);
    int const EXPECTED = RESET ? (255 << 8) : WORKER ? (23 << 8) : SIGKILL;
    bool const ok = waited == parent && status == EXPECTED;
    printf("%s uid=%u parent_source=%ld child_source=%ld waited=%ld status=%d\n", ok ? "PASS" : "FAIL", uid, parent, child, waited, status);
    return ok ? 0 : 8;
}
