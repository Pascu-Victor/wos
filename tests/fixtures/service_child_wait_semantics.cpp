// Ordinary owned-child wait semantics across parent exec and PID namespaces.
#include <grp.h>
#include <poll.h>
#include <signal.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
constexpr int NOWAIT = 0x01000000;
auto now() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return static_cast<uint64_t>(value.tv_sec) * 1000 + value.tv_nsec / 1000000;
}
auto transfer(int fd, void* value, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(value);
    auto const deadline = now() + 15000;
    while (size && now() < deadline) {
        pollfd event{.fd = fd, .events = static_cast<short>(writing ? POLLOUT : POLLIN)};
        int const ready = poll(&event, 1, 50);
        if (ready < 0 && errno == EINTR) continue;
        if (ready < 0) return false;
        if (!ready) continue;
        ssize_t const count = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        bytes += count;
        size -= count;
    }
    return size == 0;
}
struct Wait {
    pid_t pid{};
    int status{-1}, error{};
};
auto wait_ready(pid_t selector, int options = 0) -> Wait {
    auto const deadline = now() + 5000;
    Wait result{};
    do {
        errno = 0;
        result.pid = waitpid(selector, &result.status, options | WNOHANG);
        result.error = result.pid < 0 ? errno : 0;
        if (result.pid > 0 || (result.pid < 0 && result.error != EINTR)) return result;
        usleep(1000);
    } while (now() < deadline);
    result.error = ETIMEDOUT;
    return result;
}
auto usage() -> tms {
    tms value{};
    if (times(&value) == static_cast<clock_t>(-1)) _exit(91);
    return value;
}
auto same_usage(const tms& first, const tms& second) -> bool {
    return first.tms_cutime == second.tms_cutime && first.tms_cstime == second.tms_cstime;
}
struct Result {
    pid_t receiver{}, child{}, group{};
    uid_t uid{};
    unsigned checks{}, failed{};
    int collision_error{}, group_before{}, fault_error{}, local_status{-1}, remote_status{-1}, cleaned{};
    tms before{}, peek{}, after{};
};
auto receiver(pid_t source_child, int source_gate) -> int {
    alarm(30);
    Result result{.receiver = getpid(), .child = source_child, .group = getpgrp(), .uid = geteuid()};
    auto check = [&](bool value) {
        if (!value) result.failed |= 1U << result.checks;
        ++result.checks;
    };
    std::array<int, 2> gate{}, ready{};
    if (pipe(gate.data()) || pipe(ready.data())) return 30;
    pid_t local = 0;
    for (unsigned i = 0; i < 128; ++i) {
        local = fork();
        if (local == 0) {
            if (getpid() != source_child) _exit(0);
            close(1);
            close(source_gate);
            close(gate[1]);
            close(ready[0]);
            int prepared = setsid() == getpid();
            if (!transfer(ready[1], &prepared, sizeof(prepared), true)) _exit(31);
            close(ready[1]);
            char token{};
            _exit(prepared && transfer(gate[0], &token, 1, false) && token == 'X' ? 41 : 32);
        }
        if (local == source_child) break;
        if (local <= 0 || local > source_child) return 33;
        auto const PAD = wait_ready(local);
        if (PAD.pid != local || PAD.status != 0) return 34;
    }
    close(gate[0]);
    close(ready[1]);
    int prepared = 0;
    if (local != source_child || !transfer(ready[0], &prepared, sizeof(prepared), false) || !prepared) return 35;
    close(ready[0]);
    int status = -1;
    errno = 0;
    pid_t const COLLISION = waitpid(source_child, &status, WNOHANG);
    result.collision_error = errno;
    check(COLLISION == -1 && errno == EOVERFLOW && status == -1);
    errno = 0;
    result.group_before = waitpid(0, &status, WNOHANG);
    check(result.group_before == 0 && status == 0);  // libc waitpid copies its zero temporary on WNOHANG
    char release = 'X';
    if (!transfer(gate[1], &release, 1, true) || !transfer(source_gate, &release, 1, true)) return 36;
    close(gate[1]);
    close(source_gate);
    auto const LOCAL_PEEK = wait_ready(-local, NOWAIT);
    check(LOCAL_PEEK.pid == local && LOCAL_PEEK.status == (41 << 8));
    errno = 0;
    status = -1;
    check(waitpid(source_child, &status, WNOHANG | NOWAIT) == -1 && errno == EOVERFLOW && status == -1);
    auto const FIRST = wait_ready(-1);
    result.local_status = FIRST.status;
    check(FIRST.pid == local && FIRST.status == (41 << 8));
    result.before = usage();
    auto const OWN_GROUP = wait_ready(0, NOWAIT);
    check(OWN_GROUP.pid == source_child && OWN_GROUP.status == (37 << 8));
    auto const NUMERIC_GROUP = wait_ready(-result.group, NOWAIT);
    check(NUMERIC_GROUP.pid == source_child && NUMERIC_GROUP.status == (37 << 8));
    auto const EXACT = wait_ready(source_child, NOWAIT);
    check(EXACT.pid == source_child && EXACT.status == (37 << 8));
    result.peek = usage();
    check(same_usage(result.before, result.peek));
    errno = 0;
    auto* const BAD_STATUS = reinterpret_cast<int*>(static_cast<uintptr_t>(1));
    // wait4 passes the caller status directly; waitpid uses a libc temporary.
    pid_t const BAD = wait4(source_child, BAD_STATUS, WNOHANG, nullptr);
    result.fault_error = errno;
    check(BAD == -1 && result.fault_error == EFAULT && same_usage(result.peek, usage()));
    auto const SECOND = wait_ready(-1);
    result.remote_status = SECOND.status;
    check(SECOND.pid == source_child && SECOND.status == (37 << 8));
    result.after = usage();
    check(result.after.tms_cutime + result.after.tms_cstime > result.peek.tms_cutime + result.peek.tms_cstime);
    errno = 0;
    result.cleaned = waitpid(-1, &status, WNOHANG) == -1 && errno == ECHILD;
    check(result.cleaned && same_usage(result.after, usage()));
    return transfer(1, &result, sizeof(result), true) ? 0 : 37;
}
auto run_case(unsigned uid, const char* peer, const char* remote_self) -> bool {
    alarm(45);
    std::array<int, 2> reports{};
    if (pipe(reports.data())) return false;
    pid_t const probe = fork();
    if (probe == 0) {
        if (dup2(reports[1], 1) < 0) _exit(40);
        close(reports[0]);
        close(reports[1]);
        execl("/usr/bin/on", "on", peer, remote_self, "--probe", nullptr);
        _exit(41);
    }
    pid_t remote_pid = 0;
    if (probe <= 0 || !transfer(reports[0], &remote_pid, sizeof(remote_pid), false)) return false;
    auto const PROBE = wait_ready(probe);
    if (PROBE.pid != probe || PROBE.status) return false;
    pid_t parent = 0;
    for (unsigned i = 0; i < 128; ++i) {
        parent = fork();
        if (parent == 0) {
            if (getpid() <= remote_pid + 24) _exit(0);
            if (setsid() != getpid() || (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid)))) _exit(42);
            if (dup2(reports[1], 1) < 0) _exit(43);
            close(reports[0]);
            close(reports[1]);
            std::array<int, 2> gate{};
            if (pipe(gate.data())) _exit(44);
            pid_t const child = fork();
            if (child == 0) {
                close(1);
                close(gate[1]);
                char token{};
                if (!transfer(gate[0], &token, 1, false) || token != 'X') _exit(45);
                auto const deadline = now() + 150;
                uint64_t value = 1;
                while (now() < deadline) {
                    for (unsigned j = 0; j < 10000; ++j) value = value * 1664525 + 1013904223;
                    asm volatile("" : "+r"(value));
                }
                _exit(37);
            }
            close(gate[0]);
            pid_t created = child;
            if (child <= 0 || !transfer(1, &created, sizeof(created), true)) _exit(46);
            std::array<char, 32> id{}, fd{};
            snprintf(id.data(), id.size(), "%ld", child);
            snprintf(fd.data(), fd.size(), "%d", gate[1]);
            execl("/usr/bin/on", "on", peer, remote_self, "--receiver", id.data(), fd.data(), nullptr);
            _exit(47);
        }
        if (parent > remote_pid + 24) break;
        auto const PAD = wait_ready(parent);
        if (parent <= 0 || PAD.pid != parent || PAD.status) return false;
    }
    close(reports[1]);
    pid_t child = 0;
    bool const CREATED = transfer(reports[0], &child, sizeof(child), false);
    Result result{};
    bool const REPORTED = CREATED && transfer(reports[0], &result, sizeof(result), false);
    close(reports[0]);
    auto const PARENT = wait_ready(parent);
    bool const OK = CREATED && REPORTED && result.uid == uid && result.child == child && result.group == parent && result.checks == 13 &&
                    result.failed == 0 && PARENT.pid == parent && PARENT.status == 0;
    printf(
        "%s uid=%u receiver_uid=%u parent_source=%ld parent_receiver=%ld child_source=%ld child_receiver=%ld pgid=%ld "
        "checks=%u failed=%u collision_errno=%d group_before=%d fault_errno=%d local_status=%d remote_status=%d "
        "cpu_before=%lld cpu_peek=%lld cpu_after=%lld cleaned=%d reported=%d parent_reaped=%d parent_status=%d\n",
        OK ? "PASS" : "FAIL", uid, result.uid, parent, result.receiver, child, result.child, result.group, result.checks, result.failed,
        result.collision_error, result.group_before, result.fault_error, result.local_status, result.remote_status,
        static_cast<long long>(result.before.tms_cutime + result.before.tms_cstime),
        static_cast<long long>(result.peek.tms_cutime + result.peek.tms_cstime),
        static_cast<long long>(result.after.tms_cutime + result.after.tms_cstime), result.cleaned, REPORTED, PARENT.pid == parent,
        PARENT.status);
    fflush(stdout);
    return OK;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc == 2 && !strcmp(argv[1], "--probe")) {
        pid_t pid = getpid();
        return transfer(1, &pid, sizeof(pid), true) ? 0 : 20;
    }
    if (argc == 4 && !strcmp(argv[1], "--receiver")) return receiver(strtol(argv[2], nullptr, 10), atoi(argv[3]));
    if (argc != 3) return 2;
    bool ok = true;
    for (unsigned uid : {0U, 104U}) ok &= run_case(uid, argv[1], argv[2]);
    return ok ? 0 : 1;
}
