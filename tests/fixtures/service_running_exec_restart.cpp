#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

namespace {
using Control = ker::abi::process::CredentialControl;
auto control(Control op, unsigned value = 0) -> int64_t { return ker::process::prctl(static_cast<int>(op), value, 0, 0, 0); }
auto now_ms() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return value.tv_sec * 1000ULL + value.tv_nsec / 1000000;
}
struct Identity {
    uid_t uid[3]{};
    gid_t gid[3]{}, groups[8]{};
    int count = -1;
    int64_t nnp = -1, ceiling = -1, grants = -1;
    bool capture() {
        count = getgroups(8, groups);
        nnp = control(Control::GET_NO_NEW_PRIVILEGES);
        ceiling = control(Control::GET_AUTHORITY_CEILING);
        grants = control(Control::GET_SERVICE_PRIVILEGES);
        return getresuid(&uid[0], &uid[1], &uid[2]) == 0 && getresgid(&gid[0], &gid[1], &gid[2]) == 0 && count >= 0 && nnp >= 0 &&
               ceiling >= 0 && grants >= 0;
    }
    bool operator==(const Identity& other) const {
        return !memcmp(uid, other.uid, sizeof(uid)) && !memcmp(gid, other.gid, sizeof(gid)) &&
               !memcmp(groups, other.groups, sizeof(groups)) && count == other.count && nnp == other.nnp && ceiling == other.ceiling &&
               grants == other.grants;
    }
};
struct Ready {
    pid_t pid{};
    int valid{};
};
struct Attempt {
    const char* target;
    char** args;
};
void* execute(void* ptr) {
    auto& attempt = *static_cast<Attempt*>(ptr);
    if (ker::process::setwkitarget(attempt.target, strlen(attempt.target), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(91);
    execv(attempt.args[0], attempt.args);
    dprintf(STDERR_FILENO, "EXEC_REFUSED errno=%d\n", errno);
    _exit(92);
}
auto prepare(const char* profile) -> bool {
    bool root = !strcmp(profile, "root"), service = !strcmp(profile, "service");
    gid_t groups[]{3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007};
    return setgroups(8, groups) == 0 && control(Control::SET_NO_NEW_PRIVILEGES, 1) == 0 &&
           control(Control::SET_SERVICE_PRIVILEGES, root ? 0 : 3) == 0 && (root || (setgid(2000) == 0 && setuid(1000) == 0)) &&
           control(Control::DROP_AUTHORITY_CEILING, service ? ker::abi::process::service_privilege_ceiling(3) : 0) == 0 &&
           ker::process::prctl(4, 1, 0, 0, 0) == 0;
}
auto receiver(char** argv) -> int {
    bool root = !strcmp(argv[2], "root"), service = !strcmp(argv[2], "service");
    Identity got{};
    char hostname[64]{};
    bool valid = got.capture() && gethostname(hostname, sizeof(hostname)) == 0 && !strcmp(hostname, argv[3]) && got.count == 8 &&
                 got.nnp == 1 && got.ceiling == (service ? ker::abi::process::service_privilege_ceiling(3) : 0) &&
                 got.grants == (root ? 256 : 259);
    for (int i = 0; i < 3; ++i) valid &= got.uid[i] == (root ? 0U : 1000U) && got.gid[i] == (root ? 0U : 2000U);
    for (int i = 0; i < 8; ++i) valid &= got.groups[i] == 3000U + i;
    valid &= control(Control::DROP_AUTHORITY_CEILING, ker::abi::process::CREDENTIAL_AUTHORITY_ALL) == -EPERM;
    valid &= control(Control::SET_SERVICE_PRIVILEGES, 0) == 0 && control(Control::SET_SERVICE_PRIVILEGES, 3) == -EPERM;
    valid &= control(Control::SET_NO_NEW_PRIVILEGES, 0) == -EINVAL;
    if (!root) {
        errno = 0;
        valid &= setuid(0) == -1 && errno == EPERM;
    }
    Identity after{};
    got.grants = 256;
    valid &= after.capture() && after == got;
    Ready record{getpid(), valid};
    int fd = atoi(argv[4]);
    if (write(fd, &record, sizeof(record)) != sizeof(record) || close(fd)) return 93;
    if (!valid) return 94;
    if (!strcmp(argv[5], "control")) return 23;
    // A real reboot must end this receiver; a timeout must never look like success.
    sleep(120);
    return 95;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 6 && !strcmp(argv[1], "--receiver")) return receiver(argv);
    if (argc != 6 || geteuid() != 0) return 2;
    // target, profile, worker, payload, restart|control
    if (strcmp(argv[2], "root") && strcmp(argv[2], "inert") && strcmp(argv[2], "service")) return 2;
    bool const RESTART = !strcmp(argv[5], "restart");
    if (!RESTART && strcmp(argv[5], "control")) return 2;
    Identity before{};
    int pipefd[2]{};
    if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) || !before.capture() || pipe(pipefd)) return 2;
    pid_t child = fork();
    if (child == 0) {
        close(pipefd[0]);
        if (!prepare(argv[2])) _exit(96);
        char fd[24]{};
        snprintf(fd, sizeof(fd), "%d", pipefd[1]);
        char receiver_arg[] = "--receiver";
        char* args[]{argv[4], receiver_arg, argv[2], argv[1], fd, argv[5], nullptr};
        Attempt attempt{argv[1], args};
        if (atoi(argv[3])) {
            pthread_t worker{};
            if (pthread_create(&worker, nullptr, execute, &attempt)) _exit(97);
            for (;;) pause();
        }
        execute(&attempt);
    }
    close(pipefd[1]);
    if (child < 0 || fcntl(pipefd[0], F_SETFL, O_NONBLOCK)) return 2;
    Ready ready{};
    size_t used = 0;
    uint64_t deadline = now_ms() + 90000;
    while (used < sizeof(ready) && now_ms() < deadline) {
        ssize_t n = read(pipefd[0], reinterpret_cast<char*>(&ready) + used, sizeof(ready) - used);
        if (n > 0)
            used += n;
        else if (n == 0 || (errno != EAGAIN && errno != EINTR))
            break;
        else
            usleep(1000);
    }
    close(pipefd[0]);
    bool admitted = used == sizeof(ready) && ready.valid == 1 && ready.pid > 0;
    printf("ADMITTED child=%ld receiver=%ld exact=%d\n", static_cast<long>(child), static_cast<long>(ready.pid), admitted);
    int status = 0;
    pid_t reaped = 0;
    deadline = now_ms() + (admitted ? 90000 : 1000);
    while ((reaped = waitpid(child, &status, WNOHANG)) == 0 && now_ms() < deadline) usleep(1000);
    bool timely = reaped == child;
    if (!timely) {
        kill(child, SIGKILL);
        deadline = now_ms() + 5000;
        while ((reaped = waitpid(child, &status, WNOHANG)) == 0 && now_ms() < deadline) usleep(1000);
    }
    Identity after{};
    bool unchanged = after.capture() && after == before;
    bool status_ok = WIFEXITED(status) && WEXITSTATUS(status) == (RESTART ? 255 : 23);
    errno = 0;
    bool consumed = reaped == child && waitpid(child, &status, WNOHANG) == -1 && errno == ECHILD;
    bool passed = admitted && timely && status_ok && unchanged && consumed;
    printf("RESULT passed=%d child=%ld timely=%d reaped=%d status_ok=%d parent_unchanged=%d consumed=%d\n", passed,
           static_cast<long>(child), timely, reaped == child, status_ok, unchanged, consumed);
    return passed ? 0 : 1;
}
