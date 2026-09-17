// Observe child-event authority across parent exec using only owned families.
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <signal.h>
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
volatile sig_atomic_t notices;
void notify(int) { notices = notices + 1; }
struct Report {
    int stage{}, error{}, status{-1}, count{};
    pid_t pid{}, child{}, waited{};
    uid_t uid{};
    std::array<char, 64> host{};
};
auto now() -> uint64_t {
    timespec t{};
    if (clock_gettime(CLOCK_MONOTONIC, &t)) _exit(90);
    return static_cast<uint64_t>(t.tv_sec) * 1000 + t.tv_nsec / 1000000;
}
auto transfer(int fd, void* data, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<char*>(data);
    auto const deadline = now() + 4000;
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
auto report(int stage) -> Report {
    Report value{.stage = stage, .pid = getpid(), .uid = getuid()};
    if (gethostname(value.host.data(), value.host.size())) _exit(91);
    return value;
}
auto child() -> int {
    alarm(10);
    auto ready = report(2);
    if (!transfer(1, &ready, sizeof(ready), true)) return 20;
    char gate{};
    if (!transfer(0, &gate, 1, false) || gate != 'X') return 21;
    auto leaving = report(4);
    if (!transfer(1, &leaving, sizeof(leaving), true)) return 22;
    return 37;
}
auto receiver(pid_t child_pid) -> int {
    alarm(10);
    struct sigaction action{};
    action.sa_handler = notify;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGCHLD, &action, nullptr)) return 23;
    auto ready = report(3);
    ready.child = child_pid;
    errno = 0;
    ready.waited = waitpid(child_pid, &ready.status, WNOHANG);
    ready.error = errno;
    if (!transfer(1, &ready, sizeof(ready), true)) return 24;
    auto final = report(4);
    final.child = child_pid;
    auto const deadline = now() + 2000;
    do {
        errno = 0;
        final.waited = waitpid(child_pid, &final.status, WNOHANG);
        final.error = errno;
        if (final.waited == child_pid) break;
        usleep(1000);
    } while (now() < deadline);
    // Handler delivery may follow successful wait consumption on this return path.
    auto const signal_deadline = now() + 200;
    while (!notices && now() < signal_deadline) usleep(1000);
    final.count = notices;
    return transfer(1, &final, sizeof(final), true) ? 0 : 25;
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
auto run_case(unsigned uid, int mode, const char* self, const char* peer, const char* remote_self) -> bool {
    alarm(20);
    std::array<int, 2> gate{}, parent_reports{}, child_reports{};
    if (pipe(gate.data()) || pipe(parent_reports.data()) || pipe(child_reports.data())) return false;
    auto close_all = [&] {
        for (auto pipe : {gate, parent_reports, child_reports})
            for (int fd : pipe) close(fd);
    };
    pid_t const parent = fork();
    if (parent == 0) {
        if (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) _exit(26);
        pid_t const target = fork();
        if (target == 0) {
            if (dup2(gate[0], 0) < 0 || dup2(child_reports[1], 1) < 0) _exit(27);
            close_all();
            if (mode == 1)
                execl("/usr/bin/on", "on", peer, remote_self, "--child", nullptr);
            else
                execl(self, self, "--child", nullptr);
            _exit(28);
        }
        if (dup2(parent_reports[1], 1) < 0) _exit(29);
        close_all();
        auto created = report(1);
        created.child = target;
        if (!transfer(1, &created, sizeof(created), true)) _exit(30);
        std::array<char, 32> id{};
        snprintf(id.data(), id.size(), "%ld", static_cast<long>(target));
        if (mode == 3)
            execl("/usr/bin/on", "on", peer, remote_self, "--hop", id.data(), self, nullptr);
        else if (mode == 2)
            execl("/usr/bin/on", "on", peer, remote_self, "--receiver", id.data(), nullptr);
        else
            execl(self, self, "--receiver", id.data(), nullptr);
        _exit(31);
    }
    close(gate[0]);
    close(parent_reports[1]);
    close(child_reports[1]);
    Report created{}, child_ready{}, ready{}, middle{}, final{}, child_leaving{};
    bool const identified =
        parent > 0 && transfer(parent_reports[0], &created, sizeof(created), false) && created.stage == 1 && created.child > 0;
    bool const target_ready = transfer(child_reports[0], &child_ready, sizeof(child_ready), false) && child_ready.stage == 2;
    bool const hopped = mode != 3 || (transfer(parent_reports[0], &middle, sizeof(middle), false) && middle.stage == 5);
    bool const receiver_ready = transfer(parent_reports[0], &ready, sizeof(ready), false) && ready.stage == 3 && ready.uid == uid &&
                                !strcmp(ready.host.data(), mode == 2 ? peer : "wos-0");
    char release = 'X';
    bool const released = transfer(gate[1], &release, 1, true);
    close(gate[1]);
    bool const child_exiting = transfer(child_reports[0], &child_leaving, sizeof(child_leaving), false) && child_leaving.stage == 4 &&
                               child_leaving.pid == child_ready.pid;
    close(child_reports[0]);
    bool const finished = transfer(parent_reports[0], &final, sizeof(final), false) && final.stage == 4 && final.pid == ready.pid;
    close(parent_reports[0]);
    int parent_status = -1;
    bool const reaped = parent > 0 && reap(parent, parent_status);
    bool const setup =
        identified && target_ready && hopped && receiver_ready && released && child_exiting && finished && reaped && parent_status == 0;
    bool const pass =
        setup && ready.waited == 0 && ready.error == 0 && final.waited == created.child && final.status == (37 << 8) && final.count == 1;
    printf(
        "%s uid=%u mode=%d parent_source=%ld parent_receiver=%ld parent_node=%s hop_pid=%ld child_source=%ld child_receiver=%ld "
        "child_node=%s before_wait=%ld before_errno=%d final_wait=%ld final_errno=%d child_status=%d sigchld=%d setup=%d "
        "parent_reaped=%d parent_status=%d child_exiting=%d\n",
        pass ? "PASS" : "FAIL", uid, mode, static_cast<long>(parent), static_cast<long>(ready.pid), ready.host.data(),
        static_cast<long>(middle.pid), static_cast<long>(created.child), static_cast<long>(child_ready.pid), child_ready.host.data(),
        static_cast<long>(ready.waited), ready.error, static_cast<long>(final.waited), final.error, final.status, final.count, setup,
        reaped, parent_status, child_exiting);
    fflush(stdout);
    return pass;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && !strcmp(argv[1], "--child")) return child();
    if (argc == 3 && !strcmp(argv[1], "--receiver")) return receiver(static_cast<pid_t>(strtol(argv[2], nullptr, 10)));
    if (argc == 4 && !strcmp(argv[1], "--hop")) {
        auto middle = report(5);
        if (!transfer(1, &middle, sizeof(middle), true)) return 32;
        execl("/usr/bin/on", "on", "wos-0", argv[3], "--receiver", argv[2], nullptr);
        return 33;
    }
    if (argc != 3 || geteuid() != 0) return 2;
    signal(SIGPIPE, SIG_IGN);
    unsigned failures = 0;
    for (unsigned uid : {0U, 104U})
        for (int mode = 0; mode < 4; ++mode)
            if (!run_case(uid, mode, argv[0], argv[1], argv[2])) ++failures;
    return failures ? 1 : 0;
}
