// Parent-death registration and delivery across local/remote exec. Owned descendants only.
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
volatile sig_atomic_t observed, observed_signal;
void notice(int signal) {
    observed_signal = signal;
    observed = observed + 1;
}
struct Report {
    pid_t pid{}, parent{};
    uid_t uid{};
    int registration{}, count{}, stage{}, delivered{};
    std::array<char, 64> hostname{};
};
auto now() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t)) _exit(30);
    return static_cast<uint64_t>(t.tv_sec) * 1000 + t.tv_nsec / 1000000;
}
auto transfer(int fd, void* value, size_t bytes, bool writing) -> bool {
    auto* cursor = static_cast<char*>(value);
    auto const deadline = now() + 4000;
    while (bytes && now() < deadline) {
        pollfd event{.fd = fd, .events = static_cast<short>(writing ? POLLOUT : POLLIN)};
        int ready = poll(&event, 1, 50);
        if (ready < 0 && errno == EINTR) continue;
        if (ready < 0) return false;
        if (!ready) continue;
        ssize_t n = writing ? write(fd, cursor, bytes) : read(fd, cursor, bytes);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        cursor += n;
        bytes -= n;
    }
    return bytes == 0;
}
auto gone(pid_t pid) -> bool {
    if (pid <= 0) return false;
    std::array<char, 64> path{};
    snprintf(path.data(), path.size(), "/proc/%ld/stat", static_cast<long>(pid));
    auto const deadline = now() + 4000;
    do {
        struct stat st{};
        if (stat(path.data(), &st) == -1 && errno == ENOENT) return true;
        usleep(1000);
    } while (now() < deadline);
    return false;
}
auto reap(pid_t pid, int& status) -> bool {
    if (pid <= 0) return false;
    auto const deadline = now() + 5000;
    do {
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return true;
        if (result < 0 && errno != EINTR) return false;
        usleep(1000);
    } while (now() < deadline);
    return false;
}
auto target(int mode) -> int {
    bool const enabled = mode != 0 && mode != 2;
    alarm(10);
    struct sigaction action{};
    action.sa_handler = notice;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGUSR1, &action, nullptr) || sigaction(SIGUSR2, &action, nullptr)) return 2;
    if (mode >= 2 && ker::process::prctl(1, mode == 2 ? 0 : (mode == 4 ? SIGUSR2 : SIGUSR1), 0, 0, 0)) return 5;
    Report report{.pid = getpid(), .parent = getppid(), .uid = getuid(), .stage = 1};
    if (gethostname(report.hostname.data(), report.hostname.size()) ||
        ker::process::prctl(2, reinterpret_cast<uint64_t>(&report.registration), 0, 0, 0) != 0 ||
        !transfer(1, &report, sizeof(report), true))
        return 3;
    auto const deadline = now() + (enabled ? 1500 : 300);
    while (now() < deadline && (!enabled || observed == 0)) usleep(1000);
    report.count = observed;
    report.delivered = observed_signal;
    report.stage = 2;
    return transfer(1, &report, sizeof(report), true) ? 0 : 4;
}
auto run_case(unsigned uid, int placement, int mode, const char* self, const char* peer, const char* remote_self) -> bool {
    bool const remote = placement != 0;
    bool const enabled = mode == 1 || mode == 2 || mode == 4;
    int const expected = mode == 0 || mode == 2 ? 0 : (mode == 4 ? SIGUSR2 : SIGUSR1);
    std::array<char, 8> mode_text{};
    snprintf(mode_text.data(), mode_text.size(), "%d", mode);
    alarm(20);
    std::array<int, 2> identities{}, gate{}, reports{};
    if (pipe(identities.data()) || pipe(gate.data()) || pipe(reports.data())) return false;
    pid_t parent = fork();
    if (parent == 0) {
        close(identities[0]);
        close(gate[1]);
        close(reports[0]);
        if (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) _exit(5);
        pid_t child = fork();
        if (child == 0) {
            close(identities[1]);
            close(gate[0]);
            if (dup2(reports[1], 1) < 0) _exit(6);
            close(reports[1]);
            if (ker::process::prctl(1, enabled ? SIGUSR1 : 0, 0, 0, 0)) _exit(7);
            if (placement == 2)
                execl("/usr/bin/on", "on", peer, remote_self, "--hop", mode_text.data(), self, nullptr);
            else if (remote)
                execl("/usr/bin/on", "on", peer, remote_self, "--target", mode_text.data(), nullptr);
            else
                execl(self, self, "--target", mode_text.data(), nullptr);
            _exit(8);
        }
        close(reports[1]);
        if (!transfer(identities[1], &child, sizeof(child), true)) _exit(9);
        close(identities[1]);
        char release = 0;
        bool const released = transfer(gate[0], &release, 1, false) && release == 'X';
        close(gate[0]);
        _exit(child > 0 && released ? 0 : 10);
    }
    close(identities[1]);
    close(gate[0]);
    close(reports[1]);
    pid_t source_pid = -1;
    bool const identified = parent > 0 && transfer(identities[0], &source_pid, sizeof(source_pid), false) && source_pid > 0;
    close(identities[0]);
    Report initial{}, final{}, middle{};
    bool middle_ready = placement != 2 || (transfer(reports[0], &middle, sizeof(middle), false) && middle.stage == 3 &&
                                           !strcmp(middle.hostname.data(), peer));
    bool const ready = identified && middle_ready && transfer(reports[0], &initial, sizeof(initial), false) && initial.stage == 1 &&
                       initial.uid == uid && !strcmp(initial.hostname.data(), placement == 1 ? peer : "wos-0");
    char release = 'X';
    bool const released = transfer(gate[1], &release, 1, true);
    close(gate[1]);
    int status = -1;
    bool const reaped = reap(parent, status);
    bool const finished = ready && transfer(reports[0], &final, sizeof(final), false) && final.stage == 2 && final.pid == initial.pid;
    close(reports[0]);
    bool const absent = identified && gone(source_pid);
    bool const pass = ready && released && reaped && status == 0 && finished && absent && initial.registration == expected &&
                      final.count == (expected ? 1 : 0) && final.delivered == expected;
    printf(
        "%s uid=%u placement=%s enabled=%d source_pid=%ld target_pid=%ld target_node=%s registered=%d observed=%d "
        "ready=%d parent_reaped=%d parent_status=%d target_finished=%d source_absent=%d mode=%d delivered=%d hop_pid=%ld\n",
        pass ? "PASS" : "FAIL", uid, placement == 2 ? "roundtrip" : (remote ? "remote" : "local"), enabled, static_cast<long>(source_pid),
        static_cast<long>(initial.pid), ready ? initial.hostname.data() : "unknown", initial.registration, final.count, ready, reaped,
        status, finished, absent, mode, final.delivered, static_cast<long>(middle.pid));
    fflush(stdout);
    return pass;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 3 && !strcmp(argv[1], "--target")) return target(atoi(argv[2]));
    if (argc == 3 && !strcmp(argv[1], "--gone")) return gone(static_cast<pid_t>(strtol(argv[2], nullptr, 10))) ? 0 : 1;
    if (argc == 4 && !strcmp(argv[1], "--hop")) {
        Report middle{.pid = getpid(), .stage = 3};
        if (gethostname(middle.hostname.data(), middle.hostname.size()) || !transfer(1, &middle, sizeof(middle), true)) return 6;
        execl("/usr/bin/on", "on", "wos-0", argv[3], "--target", argv[2], nullptr);
        return 7;
    }
    bool const extended = argc == 4 && !strcmp(argv[3], "--extended");
    if ((!extended && argc != 3) || geteuid() != 0) return 2;
    // Preserve error handling if a setup failure closes the private release pipe.
    signal(SIGPIPE, SIG_IGN);
    unsigned failures = 0;
    for (unsigned uid : {0U, 104U})
        for (int placement = 0; placement < (extended ? 3 : 2); ++placement)
            for (int mode = 0; mode < (extended ? 5 : 2); ++mode)
                if (!run_case(uid, placement, mode, argv[0], argv[1], argv[2])) ++failures;
    return failures ? 1 : 0;
}
