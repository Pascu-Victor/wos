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
struct Record {
    pid_t pid{}, pgid{};
    uid_t real{}, effective{}, saved{};
    int delivered{}, error{}, root_delivered{};
    std::array<char, 64> host{};
};
volatile sig_atomic_t signals;
volatile sig_atomic_t root_signals;
bool mapping_case;
bool foreign_home_case;
char* third_peer;
char** replacement_args;
const char* replacement_target;

auto now_ms() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return static_cast<uint64_t>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}
auto path(const char* directory, unsigned stage, const char* leaf) -> std::array<char, 1200> {
    std::array<char, 1200> result{};
    int n = snprintf(result.data(), result.size(), "%s/%u-%s", directory, stage, leaf);
    if (n < 0 || static_cast<size_t>(n) >= result.size()) _exit(91);
    return result;
}
auto store(const char* directory, unsigned stage, const char* leaf, const void* data, size_t size) -> bool {
    auto name = path(directory, stage, leaf);
    int fd = open(name.data(), O_CREAT | O_EXCL | O_WRONLY, 0666);
    if (fd < 0) return false;
    bool ok = write(fd, data, size) == static_cast<ssize_t>(size);
    return close(fd) == 0 && ok;
}
auto load(const char* directory, unsigned stage, const char* leaf, void* data, size_t size, unsigned timeout) -> bool {
    auto name = path(directory, stage, leaf);
    auto deadline = now_ms() + timeout;
    do {
        int fd = open(name.data(), O_RDONLY);
        if (fd >= 0) {
            bool ok = read(fd, data, size) == static_cast<ssize_t>(size);
            close(fd);
            if (ok) return true;
        }
        usleep(1000);
    } while (now_ms() < deadline);
    return false;
}
auto identity(int error = 0) -> Record {
    Record result{.pid = getpid(), .pgid = getpgrp(), .delivered = signals, .error = error, .root_delivered = root_signals};
    if (getresuid(&result.real, &result.effective, &result.saved) || gethostname(result.host.data(), result.host.size())) _exit(92);
    return result;
}
void handler(int) { signals = signals + 1; }
void root_handler(int) { root_signals = root_signals + 1; }
struct SendResult {
    int query{}, query_errno{}, signal{}, signal_errno{};
};
auto nonroot_signal(pid_t group, SendResult& result) -> bool {
    int pipefd[2];
    if (pipe(pipefd)) return false;
    pid_t sender = fork();
    if (sender == 0) {
        close(pipefd[0]);
        alarm(15);
        if (setgroups(0, nullptr) || setgid(100) || setuid(100)) _exit(2);
        errno = 0;
        result.query = kill(-group, 0);
        result.query_errno = errno;
        errno = 0;
        result.signal = kill(-group, SIGUSR2);
        result.signal_errno = errno;
        _exit(write(pipefd[1], &result, sizeof(result)) == sizeof(result) ? 0 : 3);
    }
    close(pipefd[1]);
    bool received = sender > 0 && read(pipefd[0], &result, sizeof(result)) == sizeof(result);
    close(pipefd[0]);
    if (sender < 0) return false;
    int status{};
    pid_t waited;
    do {
        waited = waitpid(sender, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return received && waited == sender && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
auto root_query_on_peer(char* image, char* peer, pid_t target) -> bool {
    pid_t probe = fork();
    if (probe == 0) {
        alarm(15);
        std::array<char, 32> pid{};
        snprintf(pid.data(), pid.size(), "%ld", static_cast<long>(target));
        if (ker::process::setwkitarget(peer, strlen(peer), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(2);
        execl(image, image, "--query-local", pid.data(), peer, nullptr);
        _exit(3);
    }
    if (probe < 0) return false;
    int status{};
    pid_t waited;
    do {
        waited = waitpid(probe, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return waited == probe && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
[[noreturn]] void replace() {
    if (ker::process::setwkitarget(replacement_target, strlen(replacement_target), ker::process::WKI_TARGET_FLAG_STRICT) == 0)
        execv(replacement_args[0], replacement_args);
    auto failure = identity(errno);
    store(replacement_args[2], static_cast<unsigned>(atoi(replacement_args[3])), "failure", &failure, sizeof(failure));
    _exit(7);
}
void* worker(void*) { replace(); }
[[noreturn]] void advance(char* image, char* directory, unsigned next, char* peer, char* owner, char* worker_mode) {
    std::array<char, 16> stage{};
    snprintf(stage.data(), stage.size(), "%u", next);
    char flag[] = "--stage";
    char mapping[] = "--mapping";
    char foreign[] = "--foreign-home";
    char third[] = "--third";
    char* args[] = {
        image,      flag,   directory,   stage.data(),
        peer,       owner,  worker_mode, foreign_home_case ? foreign : (mapping_case ? mapping : (third_peer ? third : nullptr)),
        third_peer, nullptr};
    replacement_args = args;
    replacement_target = next == 2 ? (third_peer ? third_peer : owner) : peer;
    if (strcmp(worker_mode, "worker") != 0) replace();
    pthread_t thread{};
    if (pthread_create(&thread, nullptr, worker, nullptr)) _exit(93);
    for (;;) usleep(1000);
}
auto stage_main(char** argv) -> int {
    alarm(35);
    unsigned const STAGE = static_cast<unsigned>(atoi(argv[3]));
    if (STAGE < 1 || STAGE > 3) return 2;
    if (foreign_home_case && STAGE == 1) {
        auto parent = identity();
        if (!store(argv[2], 1, "observed", &parent, sizeof(parent))) return 4;
        pid_t child = fork();
        if (child == 0) {
            auto branch = identity();
            if (!store(argv[2], 1, "ready", &branch, sizeof(branch))) _exit(4);
            advance(argv[0], argv[2], 2, argv[4], argv[5], argv[6]);
        }
        if (child < 0) return 9;
        int status{};
        pid_t waited;
        do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
        return waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 23 ? 23 : 10;
    }
    if (foreign_home_case && (STAGE != 2 || setpgid(0, 0))) return 11;
    struct sigaction action{};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGUSR2, &action, nullptr)) return 3;
    if (mapping_case) {
        action.sa_handler = root_handler;
        if (sigaction(SIGUSR1, &action, nullptr)) return 3;
    }
    auto ready = identity();
    if (!store(argv[2], STAGE, "ready", &ready, sizeof(ready))) return 4;
    auto deadline = now_ms() + 10000;
    while (!signals && now_ms() < deadline) usleep(1000);
    // Observe late sequential duplicates before advancing to another image.
    // Standard-signal coalescing still limits what this count alone proves.
    usleep(100000);
    auto observed = identity();
    if (!store(argv[2], STAGE, "observed", &observed, sizeof(observed))) return 5;
    char token{};
    if (!load(argv[2], STAGE, "advance", &token, 1, 10000) || token != 'G') return 6;
    if (signals != 1) return 8;
    if (STAGE == 3 || foreign_home_case) return 23;
    advance(argv[0], argv[2], STAGE + 1, argv[4], argv[5], argv[6]);
}
auto run_foreign_case(char** argv) -> int {
    if (getuid() != 0 || geteuid() != 0 || strcmp(argv[1], "100") != 0) return 2;
    alarm(90);
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        if (setgroups(0, nullptr) || setgid(100) || setuid(100) || setpgid(0, 0)) _exit(95);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        advance(argv[3], argv[2], 1, argv[4], argv[5], argv[6]);
    }
    if (CHILD < 0) return 3;
    Record ready{}, observed{}, branch{}, parent{};
    bool const READY = load(argv[2], 2, "ready", &ready, sizeof(ready), 10000);
    bool const BRANCH = load(argv[2], 1, "ready", &branch, sizeof(branch), 1);
    bool const PARENT = load(argv[2], 1, "observed", &parent, sizeof(parent), 1);
    bool const IDENTITY = READY && BRANCH && PARENT && ready.pid > 0 && ready.pgid == ready.pid && ready.real == 100 &&
                          ready.effective == 100 && ready.saved == 100 && strcmp(ready.host.data(), argv[5]) == 0 && branch.pid > 0 &&
                          branch.pid != parent.pid && branch.pgid == CHILD && branch.real == 100 && branch.effective == 100 &&
                          branch.saved == 100 && strcmp(branch.host.data(), argv[4]) == 0 && parent.pgid == CHILD && parent.real == 100 &&
                          parent.effective == 100 && parent.saved == 100 && strcmp(parent.host.data(), argv[4]) == 0;
    errno = 0;
    int const QUERY = IDENTITY ? kill(-ready.pgid, 0) : -1, QUERY_ERRNO = errno;
    errno = 0;
    int const SENT = IDENTITY ? kill(-ready.pgid, SIGUSR2) : -1, SIGNAL_ERRNO = errno;
    bool const OBSERVED = load(argv[2], 2, "observed", &observed, sizeof(observed), 12000);
    bool ok = IDENTITY && QUERY == 0 && SENT == 0 && OBSERVED && observed.pid == ready.pid && observed.pgid == ready.pgid &&
              observed.real == 100 && observed.effective == 100 && observed.saved == 100 && ready.delivered == 0 &&
              observed.delivered == 1 && strcmp(observed.host.data(), argv[5]) == 0;
    char const GO = 'G';
    ok = store(argv[2], 2, "advance", &GO, 1) && ok;
    printf(
        "%s foreign source=%ld parent=%ld branch=%ld receiver=%ld group=%ld query=%d errno=%d signal=%d errno=%d observed=%d identity=%d\n",
        ok ? "PASS" : "FAIL", static_cast<long>(CHILD), static_cast<long>(parent.pid), static_cast<long>(branch.pid),
        static_cast<long>(ready.pid), static_cast<long>(ready.pgid), QUERY, QUERY_ERRNO, SENT, SIGNAL_ERRNO, observed.delivered, IDENTITY);
    int status{};
    pid_t waited;
    do {
        waited = waitpid(CHILD, &status, 0);
    } while (waited < 0 && errno == EINTR);
    bool const REAPED = waited == CHILD;
    bool const NORMAL = REAPED && WIFEXITED(status) && WEXITSTATUS(status) == 23;
    printf("%s complete source=%ld status=%d normal=%d reaped=%d\n", ok && NORMAL ? "PASS" : "FAIL", static_cast<long>(CHILD), status,
           NORMAL, REAPED);
    return ok && NORMAL ? 0 : 1;
}
auto run_case(char** argv) -> int {
    uid_t const UID = static_cast<uid_t>(strtoul(argv[1], nullptr, 10));
    if (mapping_case) {
        if (UID != 100 || getuid() != 0 || geteuid() != 0) return 2;
    } else if (setgroups(0, nullptr) || setgid(UID) || setuid(UID))
        return 2;
    alarm(90);
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        if (mapping_case && (setgroups(0, nullptr) || setgid(UID) || setuid(UID))) _exit(95);
        if (setpgid(0, 0)) _exit(94);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        advance(argv[3], argv[2], 1, argv[4], argv[5], argv[6]);
    }
    if (CHILD < 0) return 3;
    printf("SOURCE pid=%ld uid=%u executor=%s\n", static_cast<long>(CHILD), UID, argv[6]);
    bool ok = true;
    for (unsigned stage = 1; stage <= 3; ++stage) {
        Record ready{}, observed{}, failure{};
        bool const READY = load(argv[2], stage, "ready", &ready, sizeof(ready), 10000);
        if (!READY) {
            bool const FAILED = load(argv[2], stage, "failure", &failure, sizeof(failure), 1);
            printf("FAIL stage=%u ready=0 error_record=%d exec_error=%d receiver=%ld host=%s\n", stage, FAILED, failure.error,
                   static_cast<long>(failure.pid), failure.host.data());
            ok = false;
            break;
        }
        SendResult sent{}, root{};
        bool sender_reaped = true;
        bool direct_query_ok = true;
        if (mapping_case) {
            errno = 0;
            root.query = kill(-CHILD, 0);
            root.query_errno = errno;
            errno = 0;
            root.signal = kill(-CHILD, SIGUSR1);
            root.signal_errno = errno;
            if (stage == 2) {
                if (third_peer) {
                    direct_query_ok = root_query_on_peer(argv[3], third_peer, ready.pid);
                    printf("%s receiver-control host=%s receiver=%ld root_query_and_reap=%d\n", direct_query_ok ? "PASS" : "FAIL",
                           third_peer, static_cast<long>(ready.pid), direct_query_ok);
                } else {
                    errno = 0;
                    int const DIRECT = kill(ready.pid, 0), DIRECT_ERRNO = errno;
                    direct_query_ok = DIRECT == 0;
                    printf("%s local-control receiver=%ld root_query=%d errno=%d\n", direct_query_ok ? "PASS" : "FAIL",
                           static_cast<long>(ready.pid), DIRECT, DIRECT_ERRNO);
                }
            }
            sender_reaped = nonroot_signal(CHILD, sent);
        } else {
            errno = 0;
            sent.query = kill(-CHILD, 0);
            sent.query_errno = errno;
            errno = 0;
            sent.signal = kill(-CHILD, SIGUSR2);
            sent.signal_errno = errno;
        }
        int const QUERY = sent.query, QUERY_ERRNO = sent.query_errno;
        int const SENT = sent.signal, SIGNAL_ERRNO = sent.signal_errno;
        printf("ADMISSION stage=%u receiver=%ld host=%s at_ms=%llu query=%d signal=%d\n", stage, static_cast<long>(ready.pid),
               ready.host.data(), static_cast<unsigned long long>(now_ms()), QUERY, SENT);
        bool const OBSERVED = load(argv[2], stage, "observed", &observed, sizeof(observed), 12000);
        bool const IDENTITY = ready.pgid == CHILD && ready.real == UID && ready.effective == UID && ready.saved == UID &&
                              strcmp(ready.host.data(), stage == 2 ? (third_peer ? third_peer : argv[5]) : argv[4]) == 0 &&
                              observed.pid == ready.pid && observed.pgid == ready.pgid && observed.real == UID &&
                              observed.effective == UID && observed.saved == UID && strcmp(observed.host.data(), ready.host.data()) == 0;
        bool policy_ok = true;
        if (mapping_case) {
            int const EXPECTED = stage == 2 ? 1 : 0;
            bool const CALLS = EXPECTED
                                   ? root.query == 0 && root.signal == 0
                                   : root.query == -1 && root.query_errno == EACCES && root.signal == -1 && root.signal_errno == EACCES;
            policy_ok =
                CALLS && OBSERVED && ready.root_delivered == 0 && observed.root_delivered == EXPECTED && sender_reaped && direct_query_ok;
            printf("%s policy stage=%u root_query=%d errno=%d root_signal=%d errno=%d root_observed=%d expected=%d sender_reaped=%d\n",
                   policy_ok ? "PASS" : "FAIL", stage, root.query, root.query_errno, root.signal, root.signal_errno,
                   observed.root_delivered, EXPECTED, sender_reaped);
        }
        bool const PASS = OBSERVED && IDENTITY && QUERY == 0 && SENT == 0 && ready.delivered == 0 && observed.delivered == 1 && policy_ok;
        printf(
            "%s stage=%u receiver=%ld host=%s pgid=%ld uid=%u/%u/%u query=%d query_errno=%d signal=%d signal_errno=%d observed=%d "
            "delivered=%d identity=%d\n",
            PASS ? "PASS" : "FAIL", stage, static_cast<long>(ready.pid), ready.host.data(), static_cast<long>(ready.pgid), ready.real,
            ready.effective, ready.saved, QUERY, QUERY_ERRNO, SENT, SIGNAL_ERRNO, OBSERVED, observed.delivered, IDENTITY);
        char const GO = 'G';
        bool const ADVANCED = store(argv[2], stage, "advance", &GO, 1);
        ok = ADVANCED && PASS && ok;
        // Collect the next policy observation only while owned child execution
        // is sound. A failed assertion remains in the aggregate exit status.
        bool const CAN_CONTINUE = third_peer && ADVANCED && OBSERVED && IDENTITY && QUERY == 0 && SENT == 0 && ready.delivered == 0 &&
                                  observed.delivered == 1 && sender_reaped && direct_query_ok;
        if (!ok && !CAN_CONTINUE) break;
    }
    int status = 0;
    bool reaped = false;
    auto reap = [&](unsigned timeout) {
        auto deadline = now_ms() + timeout;
        do {
            auto result = waitpid(CHILD, &status, WNOHANG);
            if (result == CHILD) return true;
            if (result < 0 && errno != EINTR) return false;
            usleep(1000);
        } while (now_ms() < deadline);
        return false;
    };
    reaped = reap(10000);
    bool const NORMAL = reaped;
    if (!reaped) {
        kill(CHILD, SIGKILL);
        reaped = reap(20000);
    }
    bool const PASS = ok && NORMAL && WIFEXITED(status) && WEXITSTATUS(status) == 23;
    printf("%s complete source=%ld status=%d normal=%d reaped=%d\n", PASS ? "PASS" : "FAIL", static_cast<long>(CHILD), status, NORMAL,
           reaped);
    return PASS ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);
    if (argc == 4 && strcmp(argv[1], "--query-local") == 0) {
        char* end{};
        unsigned long pid = strtoul(argv[2], &end, 10);
        auto caller = identity();
        bool const VALID = pid > 0 && *end == '\0' && caller.real == 0 && caller.effective == 0 && caller.saved == 0 &&
                           strcmp(caller.host.data(), argv[3]) == 0;
        errno = 0;
        int const RESULT = VALID ? kill(static_cast<pid_t>(pid), 0) : -1, ERROR = errno;
        printf("RECEIVER-QUERY host=%s caller=%ld target=%lu uid=%u/%u/%u result=%d errno=%d\n", caller.host.data(),
               static_cast<long>(caller.pid), pid, caller.real, caller.effective, caller.saved, RESULT, ERROR);
        return VALID && RESULT == 0 ? 0 : 1;
    }
    bool const RETIREMENT = argc == 4 && strcmp(argv[1], "--peer-retired") == 0;
    bool const SYNC = argc == 3 && strcmp(argv[1], "--peer-sync") == 0;
    if (RETIREMENT || SYNC) {
        if (setgroups(0, nullptr) || setgid(100) || setuid(100)) return 2;
        alarm(20);
        if (ker::process::setwkitarget(argv[2], strlen(argv[2]), ker::process::WKI_TARGET_FLAG_STRICT)) return 3;
        if (SYNC)
            execl("/usr/bin/sync", "/usr/bin/sync", nullptr);
        else {
            char* end{};
            unsigned long pid = strtoul(argv[3], &end, 10);
            if (pid == 0 || *end != '\0') return 4;
            std::array<char, 160> command{};
            snprintf(command.data(), command.size(), "test -d /proc/$$ || exit 3; test ! -d /proc/%lu || exit 4", pid);
            execl("/usr/bin/sh", "/usr/bin/sh", "-c", command.data(), nullptr);
        }
        return 5;
    }
    if (argc < 7 || argc > 9) return 2;
    mapping_case = argc >= 8 && strcmp(argv[7], "--mapping") == 0;
    foreign_home_case = argc == 8 && strcmp(argv[7], "--foreign-home") == 0;
    if (argc == 8 && !mapping_case && !foreign_home_case) return 2;
    if (argc == 9) {
        if ((!mapping_case && strcmp(argv[7], "--third") != 0) || argv[8][0] == '\0' || strcmp(argv[8], argv[4]) == 0 ||
            strcmp(argv[8], argv[5]) == 0)
            return 2;
        third_peer = argv[8];
    }
    return strcmp(argv[1], "--stage") == 0 ? stage_main(argv) : (foreign_home_case ? run_foreign_case(argv) : run_case(argv));
}
