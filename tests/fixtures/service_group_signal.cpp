#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
volatile sig_atomic_t observed = 0;
void observe(int) { observed = 1; }

auto transfer(int fd, void* buffer, size_t length, bool writing) -> bool {
    auto* bytes = static_cast<char*>(buffer);
    while (length) {
        ssize_t const N = writing ? write(fd, bytes, length) : read(fd, bytes, length);
        if (N < 0 && errno == EINTR) continue;
        if (N <= 0) return false;
        bytes += N;
        length -= static_cast<size_t>(N);
    }
    return true;
}

auto reap(pid_t pid, int& status) -> bool {
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == pid;
}

struct Identity {
    pid_t pid, session, before_group, group;
    uid_t real, effective, saved;
    pid_t monitor = 0;
};

auto target(const char* transition, pid_t group) -> int {
    alarm(30);
    struct sigaction action{};
    action.sa_handler = observe;
    sigemptyset(&action.sa_mask);
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    if (sigaction(SIGUSR1, &action, nullptr) || sigprocmask(SIG_BLOCK, &mask, nullptr) || setgroups(0, nullptr) || setgid(100) ||
        setuid(100))
        return 2;
    Identity identity{};
    if (std::strcmp(transition, "orphan") == 0) {
        pid_t const MONITOR = fork();
        if (MONITOR < 0) return 9;
        if (MONITOR > 0) return 0;  // The submitted root completes before kill.
        alarm(30);
        identity.monitor = getpid();
        std::array<int, 2> ready{};
        if (pipe2(ready.data(), O_CLOEXEC)) return 9;
        pid_t const MEMBER = fork();
        if (MEMBER < 0) return 9;
        if (MEMBER > 0) {
            close(ready[0]);
            unsigned char moved = setpgid(0, 0) == 0 ? 1 : 0;
            bool const RELEASED = transfer(ready[1], &moved, 1, true);
            close(ready[1]);
            int status = -1;
            int result = reap(MEMBER, status) && status == 0 && moved && RELEASED ? 1 : 0;
            // The supervisor is outside the signaled group and explicitly
            // reaps its member before reporting completion to the source.
            return transfer(1, &result, sizeof(result), true) ? 0 : 9;
        }
        close(ready[1]);
        unsigned char moved = 0;
        bool const READY = transfer(ready[0], &moved, 1, false);
        close(ready[0]);
        if (!READY || !moved) return 9;
        alarm(30);
    }
    identity.before_group = getpgrp();
    if (std::strcmp(transition, "leave") == 0 && setpgid(0, 0)) return 3;
    if (std::strcmp(transition, "join") == 0 && setpgid(0, group)) return 3;
    identity.pid = getpid();
    identity.session = getsid(0);
    identity.group = getpgrp();
    if (getresuid(&identity.real, &identity.effective, &identity.saved) || !transfer(1, &identity, sizeof(identity), true)) return 4;
    unsigned char expected = 0;
    if (!transfer(0, &expected, 1, false)) return 5;
    // The caller's synchronous kill has finished. Wait only for a signal that
    // should have been sent; missing delivery is reported, not hidden by exit.
    for (unsigned i = 0; expected && i < 1000; ++i) {
        sigset_t pending{};
        if (sigpending(&pending)) return 6;
        if (sigismember(&pending, SIGUSR1) == 1) break;
        usleep(1000);
    }
    if (sigprocmask(SIG_UNBLOCK, &mask, nullptr)) return 7;
    // Cross another WOS userspace-return boundary before observing the handler.
    usleep(1000);
    int result = observed;
    return transfer(1, &result, sizeof(result), true) ? 0 : 8;
}

struct Child {
    pid_t pid = -1;
    int report = -1, gate = -1;
    Identity identity{};
    bool ready = false, reaped = false;
    int observed = -1, status = -1;
};

auto start(bool remote, const char* transition, pid_t group, const char* local_helper, const char* remote_helper, const char* peer)
    -> Child {
    Child child;
    std::array<int, 2> report{}, gate{};
    if (pipe2(report.data(), O_CLOEXEC)) return child;
    if (pipe2(gate.data(), O_CLOEXEC)) {
        close(report[0]);
        close(report[1]);
        return child;
    }
    std::array<char, 24> group_text{};
    snprintf(group_text.data(), group_text.size(), "%ld", static_cast<long>(group));
    child.pid = fork();
    if (child.pid == 0) {
        if (dup2(report[1], 1) < 0 || dup2(gate[0], 0) < 0) _exit(2);
        if (std::strcmp(transition, "join") == 0 && setpgid(0, 0)) _exit(3);
        if (remote)
            execl("/usr/bin/on", "on", peer, remote_helper, "--target", transition, group_text.data(), nullptr);
        else
            execl(local_helper, local_helper, "--target", transition, group_text.data(), nullptr);
        _exit(4);
    }
    close(report[1]);
    close(gate[0]);
    child.report = report[0];
    child.gate = gate[1];
    if (child.pid > 0) child.ready = transfer(child.report, &child.identity, sizeof(child.identity), false);
    auto const& ID = child.identity;
    bool const JOIN = std::strcmp(transition, "join") == 0;
    bool const LEAVE = std::strcmp(transition, "leave") == 0;
    child.ready = child.ready && ID.real == 100 && ID.effective == 100 && ID.saved == 100 && ID.session == getsid(0) &&
                  ID.before_group == (JOIN ? child.pid : group) && ID.group == (LEAVE ? ID.pid : group);
    return child;
}

void finish(Child& child, bool expected) {
    unsigned char gate = expected ? 1 : 0;
    bool const RELEASED = transfer(child.gate, &gate, 1, true);
    close(child.gate);
    bool const RECEIVED = transfer(child.report, &child.observed, sizeof(child.observed), false);
    int member_reaped = 1;
    bool const MEMBER_REAPED =
        child.identity.monitor == 0 || (transfer(child.report, &member_reaped, sizeof(member_reaped), false) && member_reaped == 1);
    close(child.report);
    if (child.pid > 0 && !child.reaped) child.reaped = reap(child.pid, child.status);
    child.ready =
        child.ready && RELEASED && RECEIVED && MEMBER_REAPED && child.reaped && WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0;
}

auto send_group(pid_t group, std::array<int, 4>& results) -> bool {
    bool caller_ok = false;
    std::array<int, 2> report{};
    if (pipe2(report.data(), O_CLOEXEC) == 0) {
        pid_t const CALLER = fork();
        if (CALLER == 0) {
            close(report[0]);
            if (setgroups(0, nullptr) || setgid(100) || setuid(100)) _exit(2);
            errno = 0;
            results[0] = kill(-group, 0);
            results[1] = errno;
            errno = 0;
            results[2] = kill(-group, SIGUSR1);
            results[3] = errno;
            _exit(transfer(report[1], results.data(), sizeof(results), true) ? 0 : 3);
        }
        close(report[1]);
        caller_ok = CALLER > 0 && transfer(report[0], results.data(), sizeof(results), false);
        int status = 0;
        if (CALLER > 0) caller_ok = reap(CALLER, status) && status == 0 && caller_ok;
        close(report[0]);
    }
    return caller_ok;
}

auto run_case(bool remote, bool joining, const char* local_helper, const char* remote_helper, const char* peer) -> int {
    if (setsid() != getpid()) return 3;
    pid_t const GROUP = getpgrp();
    // The retained member makes the requested group exist on the receiver
    // before the changing target attempts to join it.
    auto retained = start(remote, "stay", GROUP, local_helper, remote_helper, peer);
    auto changed = start(remote, joining ? "join" : "leave", GROUP, local_helper, remote_helper, peer);
    std::array<int, 4> results{-2, 0, -2, 0};
    bool const CALLER_OK = retained.ready && changed.ready && send_group(GROUP, results);
    finish(retained, true);
    finish(changed, joining);
    bool const PASS = retained.ready && changed.ready && CALLER_OK && results[0] == 0 && results[2] == 0 && retained.observed == 1 &&
                      changed.observed == (joining ? 1 : 0);
    printf(
        "%s placement=%s transition=%s group=%ld changed_proxy=%ld changed_pid=%ld before_group=%ld current_group=%ld "
        "zero=%d/%d signal=%d/%d retained_observed=%d changed_observed=%d ready=%d caller=%d status=%d/%d reaped=%d\n",
        PASS ? "PASS" : "FAIL", remote ? "remote" : "local", joining ? "join" : "leave", static_cast<long>(GROUP),
        static_cast<long>(changed.pid), static_cast<long>(changed.identity.pid), static_cast<long>(changed.identity.before_group),
        static_cast<long>(changed.identity.group), results[0], results[1], results[2], results[3], retained.observed, changed.observed,
        retained.ready && changed.ready, CALLER_OK, retained.status, changed.status, retained.reaped && changed.reaped);
    fflush(stdout);
    return PASS ? 0 : 1;
}

auto run_wide(const char* helper) -> int {
    if (setsid() != getpid()) return 3;
    pid_t const GROUP = getpgrp();
    // Exceed the kernel's inline recipient capacity with simultaneously live
    // members, each reporting its handler and exact normal child completion.
    std::array<Child, 40> children{};
    bool ready = true;
    for (auto& child : children) {
        child = start(false, "stay", GROUP, helper, nullptr, nullptr);
        ready = child.ready && ready;
    }
    std::array<int, 4> results{-2, 0, -2, 0};
    bool const CALLER_OK = ready && send_group(GROUP, results);
    unsigned delivered = 0, reaped = 0;
    for (auto& child : children) {
        finish(child, true);
        ready = child.ready && ready;
        delivered += child.observed == 1 ? 1 : 0;
        reaped += child.reaped ? 1 : 0;
    }
    bool const PASS = ready && CALLER_OK && results[0] == 0 && results[2] == 0 && delivered == children.size();
    printf("%s placement=local transition=wide members=%zu delivered=%u completed=%u ready=%d caller=%d reaped=%d\n",
           PASS ? "PASS" : "FAIL", children.size(), delivered, reaped, ready, CALLER_OK, reaped == children.size());
    return PASS ? 0 : 1;
}

auto run_orphan(bool remote, const char* local_helper, const char* remote_helper, const char* peer) -> int {
    if (setsid() != getpid()) return 3;
    pid_t const GROUP = getpgrp();
    auto child = start(remote, "orphan", GROUP, local_helper, remote_helper, peer);
    child.reaped = child.pid > 0 && reap(child.pid, child.status);
    std::array<int, 4> results{-2, 0, -2, 0};
    bool const CALLER_OK = child.ready && child.reaped && child.status == 0 && send_group(GROUP, results);
    finish(child, true);
    bool gone = false;
    if (child.identity.monitor > 0) {
        std::array<char, 24> pid{};
        snprintf(pid.data(), pid.size(), "%ld", static_cast<long>(child.identity.monitor));
        pid_t const PROBE = fork();
        if (PROBE == 0) {
            if (remote)
                execl("/usr/bin/on", "on", peer, remote_helper, "--gone", pid.data(), nullptr);
            else
                execl(local_helper, local_helper, "--gone", pid.data(), nullptr);
            _exit(3);
        }
        int status = -1;
        gone = PROBE > 0 && reap(PROBE, status) && status == 0;
    }
    bool const PASS = child.ready && CALLER_OK && results[0] == 0 && results[2] == 0 && child.observed == 1 && gone;
    printf("%s placement=%s transition=orphan root=%ld member=%ld monitor=%ld observed=%d ready=%d caller=%d monitor_gone=%d reaped=%d\n",
           PASS ? "PASS" : "FAIL", remote ? "remote" : "local", static_cast<long>(child.pid), static_cast<long>(child.identity.pid),
           static_cast<long>(child.identity.monitor), child.observed, child.ready, CALLER_OK, gone, child.reaped && child.ready && gone);
    return PASS ? 0 : 1;
}

struct SenderReport {
    std::array<int, 4> results{-2, 0, -2, 0};
    pid_t pid = 0, session = 0, group = 0;
    uid_t real = 0, effective = 0, saved = 0;
};

auto foreign_sender(pid_t group) -> int {
    alarm(30);
    if (setsid() != getpid() || setgroups(0, nullptr) || setgid(100) || setuid(100)) return 3;
    SenderReport report;
    report.pid = getpid();
    report.session = getsid(0);
    report.group = getpgrp();
    if (getresuid(&report.real, &report.effective, &report.saved) || report.group == group) return 3;
    errno = 0;
    report.results[0] = kill(-group, 0);
    report.results[1] = errno;
    errno = 0;
    report.results[2] = kill(-group, SIGUSR1);
    report.results[3] = errno;
    return transfer(1, &report, sizeof(report), true) ? 0 : 3;
}

auto run_query(const char* local_helper, const char* remote_helper, const char* peer) -> int {
    if (setsid() != getpid()) return 3;
    pid_t const GROUP = getpgrp();
    auto child = start(false, "stay", GROUP, local_helper, remote_helper, peer);
    std::array<int, 2> output{};
    if (pipe2(output.data(), O_CLOEXEC)) return 3;
    std::array<char, 24> group{};
    snprintf(group.data(), group.size(), "%ld", static_cast<long>(GROUP));
    pid_t const CALLER = fork();
    if (CALLER == 0) {
        if (dup2(output[1], 1) < 0) _exit(3);
        execl("/usr/bin/on", "on", peer, remote_helper, "--sender", group.data(), nullptr);
        _exit(3);
    }
    close(output[1]);
    SenderReport report;
    bool const RECEIVED = CALLER > 0 && transfer(output[0], &report, sizeof(report), false);
    close(output[0]);
    int status = -1;
    bool const REAPED = CALLER > 0 && reap(CALLER, status);
    finish(child, true);
    bool const READY = RECEIVED && report.real == 100 && report.effective == 100 && report.saved == 100 && report.pid == report.session &&
                       report.pid == report.group && report.group != GROUP;
    bool const PASS =
        READY && REAPED && status == 0 && child.ready && child.observed == 1 && report.results[0] == 0 && report.results[2] == 0;
    printf(
        "%s placement=remote-caller transition=foreign-query group=%ld caller_pid=%ld caller_session=%ld caller_group=%ld "
        "zero=%d/%d signal=%d/%d observed=%d ready=%d status=%d reaped=%d\n",
        PASS ? "PASS" : "FAIL", static_cast<long>(GROUP), static_cast<long>(report.pid), static_cast<long>(report.session),
        static_cast<long>(report.group), report.results[0], report.results[1], report.results[2], report.results[3], child.observed,
        READY && child.ready, status, REAPED && child.reaped);
    return PASS ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::strcmp(argv[1], "--target") == 0) return target(argv[2], static_cast<pid_t>(std::strtol(argv[3], nullptr, 10)));
    if (argc == 3 && std::strcmp(argv[1], "--sender") == 0) return foreign_sender(static_cast<pid_t>(std::strtol(argv[2], nullptr, 10)));
    if (argc == 3 && std::strcmp(argv[1], "--gone") == 0) {
        auto const PID = static_cast<pid_t>(std::strtol(argv[2], nullptr, 10));
        if (PID <= 0) return 3;
        for (unsigned i = 0; i < 1000; ++i) {
            if (kill(PID, 0) == -1 && errno == ESRCH) return 0;
            usleep(1000);
        }
        return 1;
    }
    bool const WIDE = argc == 2 && std::strcmp(argv[1], "--wide") == 0;
    bool const QUERY = argc == 4 && std::strcmp(argv[1], "--query") == 0;
    if (argc != 3 && !WIDE && !QUERY) return 3;
    alarm(120);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    if (QUERY) {
        pid_t const CHILD = fork();
        if (CHILD == 0) return run_query(argv[0], argv[3], argv[2]);
        int status = -1;
        return CHILD > 0 && reap(CHILD, status) && status == 0 ? 0 : 1;
    }
    if (WIDE) {
        pid_t const CHILD = fork();
        if (CHILD == 0) return run_wide(argv[0]);
        int status = -1;
        return CHILD > 0 && reap(CHILD, status) && status == 0 ? 0 : 1;
    }
    unsigned passed = 0;
    for (bool remote : {false, true}) {
        for (bool joining : {false, true}) {
            pid_t const CHILD = fork();
            if (CHILD < 0) return 3;
            if (CHILD == 0) return run_case(remote, joining, argv[0], argv[2], argv[1]);
            int status = -1;
            if (reap(CHILD, status) && status == 0) ++passed;
        }
        pid_t const CHILD = fork();
        if (CHILD < 0) return 3;
        if (CHILD == 0) return run_orphan(remote, argv[0], argv[2], argv[1]);
        int status = -1;
        if (reap(CHILD, status) && status == 0) ++passed;
    }
    printf("group signal membership: %u/6 cases passed\n", passed);
    return passed == 6 ? 0 : 1;
}
