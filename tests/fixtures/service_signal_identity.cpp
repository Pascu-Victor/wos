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
volatile sig_atomic_t observed_continue = 0;
void observe(int number) {
    if (number == SIGUSR1)
        observed = 1;
    else
        observed_continue = 1;
}

struct Identity {
    uid_t real, effective, saved;
    uid_t before_real, before_effective, before_saved;
    pid_t pid, session;
};
struct Results {
    int zero, zero_errno, signal, signal_errno, continued, continue_errno;
};

auto transfer(int fd, void* buffer, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(buffer);
    while (size != 0) {
        ssize_t n = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= static_cast<size_t>(n);
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

auto target(uid_t uid, bool detach) -> int {
    alarm(30);
    struct sigaction action{};
    action.sa_handler = observe;
    sigemptyset(&action.sa_mask);
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    sigaddset(&mask, SIGCONT);
    Identity identity{};
    if (getresuid(&identity.before_real, &identity.before_effective, &identity.before_saved)) return 2;
    if (sigaction(SIGUSR1, &action, nullptr) || sigaction(SIGCONT, &action, nullptr) || sigprocmask(SIG_BLOCK, &mask, nullptr) ||
        seteuid(0) || setgroups(0, nullptr) || setgid(uid) || setuid(uid))
        return 2;
    if (detach && setsid() < 0) return 8;
    identity.pid = getpid();
    identity.session = getsid(0);
    if (getresuid(&identity.real, &identity.effective, &identity.saved) || !transfer(1, &identity, sizeof(identity), true)) return 3;
    unsigned char gate = 0;
    errno = 0;
    bool const GATE_READ = transfer(0, &gate, 1, false);
    if (!GATE_READ || (gate & ~3U) != 0x40U) {
        dprintf(2, "signal identity gate: read=%d value=%u errno=%d\n", GATE_READ, gate, errno);
        return 4;
    }
    if ((gate & 3U) != 0) {
        bool pending = false;
        for (unsigned i = 0; i < 3000; ++i) {
            sigset_t signals{};
            if (sigpending(&signals)) return 7;
            if (((gate & 1U) == 0 || sigismember(&signals, SIGUSR1) == 1) && ((gate & 2U) == 0 || sigismember(&signals, SIGCONT) == 1)) {
                pending = true;
                break;
            }
            usleep(1000);
        }
        if (!pending) return 7;
    }
    if (sigprocmask(SIG_UNBLOCK, &mask, nullptr)) return 5;
    // WOS dispatches one handler at a userspace return boundary. Observe
    // completion across further boundaries before recording both handlers.
    for (unsigned i = 0; i < 3000 && (((gate & 1U) != 0 && !observed) || ((gate & 2U) != 0 && !observed_continue)); ++i) usleep(1000);
    std::array<int, 2> counts{observed, observed_continue};
    return transfer(1, counts.data(), sizeof(counts), true) ? 0 : 6;
}

auto run_case(bool remote, uid_t initial_effective, uid_t final_uid, const char* local_helper, const char* remote_helper, const char* peer,
              bool detach = false) -> bool {
    std::array<int, 2> report{}, gate{};
    if (pipe2(report.data(), O_CLOEXEC) || pipe2(gate.data(), O_CLOEXEC)) return false;
    std::array<char, 16> uid_text{};
    snprintf(uid_text.data(), uid_text.size(), "%u", final_uid);
    pid_t child = fork();
    if (child == 0) {
        if (dup2(report[1], 1) < 0 || dup2(gate[0], 0) < 0 || seteuid(initial_effective)) _exit(2);
        if (remote)
            execl("/usr/bin/on", "on", peer, remote_helper, "--target", uid_text.data(), detach ? "detach" : "inherit", nullptr);
        else
            execl(local_helper, local_helper, "--target", uid_text.data(), detach ? "detach" : "inherit", nullptr);
        _exit(3);
    }
    close(report[1]);
    close(gate[0]);
    if (child < 0) {
        close(report[0]);
        close(gate[1]);
        return false;
    }
    Identity identity{};
    bool ready = transfer(report[0], &identity, sizeof(identity), false) && identity.real == final_uid && identity.effective == final_uid &&
                 identity.saved == final_uid && identity.before_real == 0 && identity.before_effective == initial_effective &&
                 identity.before_saved == initial_effective;
    ready = ready && identity.session == (detach ? identity.pid : getsid(0));
    Results results{-2, 0, -2, 0, -2, 0};
    bool caller_ok = false;
    if (ready) {
        std::array<int, 2> result_pipe{};
        if (pipe2(result_pipe.data(), O_CLOEXEC) == 0) {
            pid_t caller = fork();
            if (caller == 0) {
                close(result_pipe[0]);
                close(report[0]);
                close(gate[1]);
                if (setgroups(0, nullptr) || setgid(100) || setuid(100)) _exit(2);
                errno = 0;
                results.zero = kill(child, 0);
                results.zero_errno = errno;
                errno = 0;
                results.signal = kill(child, SIGUSR1);
                results.signal_errno = errno;
                errno = 0;
                results.continued = kill(child, SIGCONT);
                results.continue_errno = errno;
                _exit(transfer(result_pipe[1], &results, sizeof(results), true) ? 0 : 3);
            }
            close(result_pipe[1]);
            caller_ok = caller > 0 && transfer(result_pipe[0], &results, sizeof(results), false);
            int caller_status = 0;
            if (caller > 0)
                caller_ok = reap(caller, caller_status) && WIFEXITED(caller_status) && WEXITSTATUS(caller_status) == 0 && caller_ok;
            close(result_pipe[0]);
        }
    }
    unsigned char release = 0x40U | (results.signal == 0 ? 1U : 0U) | (results.continued == 0 ? 2U : 0U);
    bool released = transfer(gate[1], &release, 1, true);
    close(gate[1]);
    std::array<int, 2> counts{-1, -1};
    bool received = transfer(report[0], counts.data(), sizeof(counts), false);
    close(report[0]);
    int target_status = 0;
    bool reaped = reap(child, target_status);
    bool clean = reaped && WIFEXITED(target_status) && WEXITSTATUS(target_status) == 0;
    bool const ALLOWED = final_uid == 100;
    bool const PERMISSION =
        ALLOWED ? results.zero == 0 && results.signal == 0
                : results.zero == -1 && results.zero_errno == EPERM && results.signal == -1 && results.signal_errno == EPERM;
    bool const CONT_ALLOWED = ALLOWED || !detach;
    bool const CONT_PERMISSION = CONT_ALLOWED ? results.continued == 0 : results.continued == -1 && results.continue_errno == EPERM;
    bool const PASS = ready && caller_ok && released && received && clean && PERMISSION && CONT_PERMISSION &&
                      counts[0] == (ALLOWED ? 1 : 0) && counts[1] == (CONT_ALLOWED ? 1 : 0);
    printf(
        "%s placement=%s initial_effective=%u target_uid=%u proxy_pid=%ld target_pid=%ld zero=%d/%d signal=%d/%d observed=%d "
        "detach=%d cont=%d/%d cont_observed=%d "
        "ready=%d caller=%d released=%d received=%d target_status=%d reaped=%d\n",
        PASS ? "PASS" : "FAIL", remote ? "remote" : "local", initial_effective, final_uid, static_cast<long>(child),
        static_cast<long>(identity.pid), results.zero, results.zero_errno, results.signal, results.signal_errno, counts[0], detach,
        results.continued, results.continue_errno, counts[1], ready, caller_ok, released, received, target_status, reaped);
    fflush(stdout);
    return PASS;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::strcmp(argv[1], "--target") == 0)
        return target(static_cast<uid_t>(std::strtoul(argv[2], nullptr, 10)), std::strcmp(argv[3], "detach") == 0);
    if (argc != 3) return 2;
    alarm(180);
    // Failed setup must still release/reap the exact child; ignore a broken
    // release pipe so its error can be reported by the owning case.
    signal(SIGPIPE, SIG_IGN);
    unsigned passed = 0;
    for (bool remote : {false, true}) {
        passed += run_case(remote, 0, 0, argv[0], argv[2], argv[1]);
        passed += run_case(remote, 0, 100, argv[0], argv[2], argv[1]);
        passed += run_case(remote, 100, 200, argv[0], argv[2], argv[1]);
        passed += run_case(remote, 100, 100, argv[0], argv[2], argv[1]);
        passed += run_case(remote, 0, 200, argv[0], argv[2], argv[1], true);
        passed += run_case(remote, 100, 200, argv[0], argv[2], argv[1], true);
    }
    printf("signal identity: %u/12 cases passed\n", passed);
    return passed == 12 ? 0 : 1;
}
