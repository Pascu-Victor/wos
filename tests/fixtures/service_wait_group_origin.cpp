// Ordinary owned-child waits with equal local and imported group numbers.
#include <grp.h>
#include <poll.h>
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
auto now() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t)) _exit(90);
    return static_cast<uint64_t>(t.tv_sec) * 1000 + t.tv_nsec / 1000000;
}
auto transfer(int fd, void* data, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(data);
    auto const deadline = now() + 5000;
    while (size && now() < deadline) {
        pollfd event{.fd = fd, .events = static_cast<short>(writing ? POLLOUT : POLLIN)};
        int ready = poll(&event, 1, 50);
        if (ready < 0 && errno == EINTR) continue;
        if (ready < 0) return false;
        if (!ready) continue;
        ssize_t n = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= n;
    }
    return size == 0;
}
auto reap(pid_t pid, int& status) -> bool {
    auto const deadline = now() + 5000;
    do {
        pid_t const result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return true;
        if (result < 0 && errno != EINTR) return false;
        usleep(1000);
    } while (now() < deadline);
    return false;
}
struct Result {
    pid_t parent{}, group{}, foreign{}, local{}, zero{}, negative{}, final{};
    uid_t uid{};
    int zero_error{}, negative_status{-1}, final_status{-1}, setup{}, cleaned{};
};
auto receiver() -> int {
    alarm(15);
    Result result{.parent = getpid(), .group = getpgrp(), .uid = geteuid()};
    pid_t const group = getpgrp();
    std::array<int, 2> foreign_gate{}, local_gate{}, reports{};
    if (pipe(foreign_gate.data()) || pipe(local_gate.data()) || pipe(reports.data())) return 30;
    result.foreign = fork();
    if (result.foreign == 0) {
        char token{};
        _exit(transfer(foreign_gate[0], &token, 1, false) && token == 'X' ? 37 : 31);
    }
    if (result.foreign <= 0 || group <= result.foreign || group - result.foreign > 128) return 32;
    // Advance only this guest's normal PID allocator, reaping each owned pad.
    // The final child creates a local group whose number equals the parent's
    // imported source group; setsid distinguishes their provenance.
    for (unsigned i = 0; i < 128; ++i) {
        pid_t const child = fork();
        if (child == 0) {
            if (getpid() == group) {
                int ready = setsid() == getpid() ? 1 : 0;
                if (!transfer(reports[1], &ready, sizeof(ready), true)) _exit(33);
                char token{};
                _exit(transfer(local_gate[0], &token, 1, false) && token == 'X' ? 41 : 34);
            }
            _exit(0);
        }
        if (child == group) {
            result.local = child;
            break;
        }
        int status = -1;
        if (child <= 0 || !reap(child, status) || status != 0 || child > group) return 35;
    }
    int ready = 0;
    result.setup = result.local == group && transfer(reports[0], &ready, sizeof(ready), false) && ready == 1;
    char token = 'X';
    if (!result.setup || !transfer(local_gate[1], &token, 1, true)) return 36;
    // Observe the local child's exit without consuming it. This removes timing
    // from the subsequent nonblocking own-group selection.
    int observed = -1;
    auto const deadline = now() + 5000;
    pid_t stopped = 0;
    do {
        stopped = waitpid(result.local, &observed, WNOHANG | 0x01000000);
        if (stopped != 0 && !(stopped < 0 && errno == EINTR)) break;
        usleep(1000);
    } while (now() < deadline);
    result.setup &= stopped == result.local && observed == (41 << 8);
    int ignored = -1;
    errno = 0;
    result.zero = waitpid(0, &ignored, WNOHANG);
    result.zero_error = result.zero < 0 ? errno : 0;
    result.negative = waitpid(-group, &result.negative_status, WNOHANG);
    if (!transfer(foreign_gate[1], &token, 1, true)) return 37;
    auto const final_deadline = now() + 5000;
    do {
        result.final = waitpid(0, &result.final_status, WNOHANG);
        if (result.final != 0 && !(result.final < 0 && errno == EINTR)) break;
        usleep(1000);
    } while (now() < final_deadline);
    // Exact-PID recovery is only cleanup evidence, never a passing assertion.
    int status = -1;
    if (result.zero != result.local && result.negative != result.local) static_cast<void>(reap(result.local, status));
    if (result.final != result.foreign) static_cast<void>(reap(result.foreign, status));
    errno = 0;
    result.cleaned = waitpid(-1, &status, WNOHANG) == -1 && errno == ECHILD;
    return transfer(1, &result, sizeof(result), true) ? 0 : 38;
}
auto run_case(unsigned uid, const char* peer, const char* remote_self) -> bool {
    std::array<int, 2> reports{};
    if (pipe(reports.data())) return false;
    // A normal remote probe lets us choose a reachable source group number.
    pid_t const probe = fork();
    if (probe == 0) {
        if (dup2(reports[1], 1) < 0) _exit(40);
        close(reports[0]);
        close(reports[1]);
        execl("/usr/bin/on", "on", peer, remote_self, "--probe", nullptr);
        _exit(41);
    }
    pid_t remote_pid = 0;
    int probe_status = -1;
    if (!transfer(reports[0], &remote_pid, sizeof(remote_pid), false) || !reap(probe, probe_status) || probe_status) return false;
    pid_t parent = 0;
    for (unsigned i = 0; i < 128; ++i) {
        parent = fork();
        if (parent == 0) {
            if (getpid() <= remote_pid + 16) _exit(0);
            if (setsid() != getpid() || (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid)))) _exit(42);
            if (dup2(reports[1], 1) < 0) _exit(43);
            close(reports[0]);
            close(reports[1]);
            execl("/usr/bin/on", "on", peer, remote_self, "--receiver", nullptr);
            _exit(44);
        }
        if (parent > remote_pid + 16) break;
        int status = -1;
        if (parent <= 0 || !reap(parent, status) || status) return false;
    }
    close(reports[1]);
    Result result{};
    bool const reported = transfer(reports[0], &result, sizeof(result), false);
    close(reports[0]);
    int status = -1;
    bool const reaped = reap(parent, status);
    bool const ok = reported && result.group == parent && result.uid == uid && result.setup && result.cleaned && reaped && status == 0 &&
                    result.zero == 0 && result.negative == result.local && result.negative_status == (41 << 8) &&
                    result.final == result.foreign && result.final_status == (37 << 8);
    printf(
        "%s uid=%u receiver_uid=%u parent_source=%ld parent_receiver=%ld pgid=%ld local=%ld foreign=%ld zero=%ld zero_errno=%d "
        "negative=%ld "
        "negative_status=%d final=%ld final_status=%d setup=%d cleaned=%d parent_reaped=%d parent_status=%d\n",
        ok ? "PASS" : "FAIL", uid, result.uid, parent, result.parent, result.group, result.local, result.foreign, result.zero,
        result.zero_error, result.negative, result.negative_status, result.final, result.final_status, result.setup, result.cleaned, reaped,
        status);
    return ok;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 2 && !strcmp(argv[1], "--probe")) {
        pid_t pid = getpid();
        return transfer(1, &pid, sizeof(pid), true) ? 0 : 20;
    }
    if (argc == 2 && !strcmp(argv[1], "--receiver")) return receiver();
    if (argc != 3) return 2;
    bool ok = true;
    for (unsigned uid : {0U, 104U}) ok &= run_case(uid, argv[1], argv[2]);
    return ok ? 0 : 1;
}
