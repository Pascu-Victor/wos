// Ordinary journal publication and authorization checks in an owned WOS VM.
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

namespace {
auto profile(unsigned mode, const char* path) -> int {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 0 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 2;
    if (mode == 1 || mode == 2) {
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 2 ? 1 : 0, 0, 0, 0) ||
            setgid(mode == 2 ? 101 : 1000) || setuid(mode == 2 ? 101 : 1000))
            return 2;
    }
    uid_t r, e, s;
    gid_t gr, ge, gs;
    if (getresuid(&r, &e, &s) || getresgid(&gr, &ge, &gs)) return 2;
    auto const CEILING = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto const GRANTS = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    errno = 0;
    int reader = open(path, O_RDONLY | O_NONBLOCK);
    bool const ALLOWED = mode >= 2;
    bool okay = ALLOWED ? reader >= 0 : reader == -1 && errno == EACCES;
    if (reader >= 0) {
        std::array<char, 608> record{};
        okay &= read(reader, record.data(), record.size()) == static_cast<ssize_t>(record.size());
        close(reader);
    }
    int const WRITER = open(path, O_WRONLY | O_NONBLOCK);
    okay &= WRITER >= 0;
    if (WRITER >= 0) close(WRITER);
    uid_t ar, ae, as;
    gid_t agr, age, ags;
    okay &= !getresuid(&ar, &ae, &as) && !getresgid(&agr, &age, &ags) && r == ar && e == ae && s == as && gr == agr && ge == age &&
            gs == ags && getgroups(0, nullptr) == 0 && CEILING == ker::process::prctl(0x574f5301, 0, 0, 0, 0) &&
            GRANTS == ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    printf("%s profile=%u alias-read-policy public-write credentials-preserved\n", okay ? "PASS" : "FAIL", mode);
    fflush(stdout);
    return okay ? 0 : 1;
}
}  // namespace

auto main() -> int {
    alarm(30);
    if (geteuid() != 0) return 2;
    std::array<char, 128> path{}, message{};
    snprintf(path.data(), path.size(), "/dev/journal/modules/g19-pub-%ld", static_cast<long>(getpid()));
    snprintf(message.data(), message.size(), "[g19-pub-%ld] publication check", static_cast<long>(getpid()));
    struct stat initial{};
    errno = 0;
    if (stat(path.data(), &initial) == 0 || errno != ENOENT) return 3;
    int const JOURNAL = open("/dev/journal", O_WRONLY);
    if (JOURNAL < 0) return 4;
    struct Cleanup {
        int journal;
        const char* path;
        bool mode_changed{};
        ~Cleanup() {
            if (mode_changed) chmod(path, 0666);
            close(journal);
        }
    } cleanup{JOURNAL, path.data()};
    size_t const LENGTH = strlen(message.data());
    if (write(JOURNAL, message.data(), LENGTH) != static_cast<ssize_t>(LENGTH)) return 5;
    unsigned polls = 0;
    while (stat(path.data(), &initial) != 0 && polls++ < 250) {
        if (errno != ENOENT) return 6;
        usleep(20000);
    }
    if (!S_ISCHR(initial.st_mode) || initial.st_uid != 0 || initial.st_gid != 0 || (initial.st_mode & 07777) != 0666) return 7;
    for (unsigned mode = 0; mode < 4; ++mode) {
        pid_t child = fork();
        if (!child) _exit(profile(mode, path.data()));
        if (child < 0) return 8;
        int status{};
        pid_t observed{};
        for (unsigned i = 0; i < 5000 && observed == 0; ++i) {
            observed = waitpid(child, &status, WNOHANG);
            if (observed < 0 && errno == EINTR) observed = 0;
            if (!observed) usleep(1000);
        }
        if (observed != child) {
            kill(child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
            }
            return 9;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status)) return 10;
    }
    if (chmod(path.data(), 0600) != 0) return 11;
    cleanup.mode_changed = true;
    if (write(JOURNAL, message.data(), LENGTH) != static_cast<ssize_t>(LENGTH)) return 12;
    usleep(250000);
    struct stat after{};
    if (stat(path.data(), &after) != 0 || after.st_ino != initial.st_ino || (after.st_mode & 07777) != 0600 ||
        after.st_uid != initial.st_uid || after.st_gid != initial.st_gid)
        return 13;
    printf("PASS publication polls=%u identity-stable metadata-not-reset\n", polls);
    return 0;
}
