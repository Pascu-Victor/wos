// Ordinary WOS authorization checks in owned children of a disposable VM.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
int profile(unsigned mode) {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 1 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 2;
    if (mode >= 2) {
        unsigned const id = mode == 5 ? 104 : 98 + mode;
        unsigned const grants = mode == 3 ? 1 : mode == 4 ? 2 : 0;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, grants, 0, 0, 0) || setgid(id) || setuid(id)) return 2;
    }
    uid_t real, effective, saved;
    gid_t greal, geffective, gsaved;
    if (getresuid(&real, &effective, &saved) || getresgid(&greal, &geffective, &gsaved)) return 2;
    auto const ceiling = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto const grants = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    unsigned failures = 0;
    auto check = [&](bool okay, const char* label) {
        printf("%s profile=%u %s\n", okay ? "PASS" : "FAIL", mode, label);
        failures += !okay;
    };
    // Selector 1 enables interrupts; no fixture path invokes the disable selector.
    auto const debug = static_cast<int64_t>(syscall(ker::abi::callnums::debug, 1));
    check(debug == (mode == 0 ? 0 : -EPERM), "DEBUG authority");
    errno = 0;
    int fd = open("/dev/journal", O_RDONLY | O_NONBLOCK);
    check(mode == 0 || mode == 3 ? fd >= 0 : fd == -1 && errno == EACCES, "journal read admission");
    if (fd >= 0) close(fd);
    fd = open("/dev/journal", O_WRONLY | O_NONBLOCK);
    check(fd >= 0, "journal public write admission");
    if (fd >= 0) close(fd);
    uid_t after_real, after_effective, after_saved;
    gid_t after_greal, after_geffective, after_gsaved;
    check(!getresuid(&after_real, &after_effective, &after_saved) && !getresgid(&after_greal, &after_geffective, &after_gsaved) &&
              real == after_real && effective == after_effective && saved == after_saved && greal == after_greal &&
              geffective == after_geffective && gsaved == after_gsaved && getgroups(0, nullptr) == 0 &&
              ceiling == ker::process::prctl(0x574f5301, 0, 0, 0, 0) && grants == ker::process::prctl(0x574f5304, 0, 0, 0, 0),
          "credentials preserved");
    fflush(stdout);
    return failures != 0;
}
}  // namespace

int main() {
    if (geteuid() != 0) return 2;
    for (unsigned mode = 0; mode < 6; ++mode) {
        pid_t child = fork();
        if (!child) _exit(profile(mode));
        if (child < 0) return 2;
        int status = 0;
        pid_t observed = 0;
        for (unsigned i = 0; i < 6000 && observed == 0; ++i) {
            observed = waitpid(child, &status, WNOHANG);
            if (observed < 0 && errno == EINTR) observed = 0;
            if (!observed) usleep(1000);
        }
        if (observed != child || !WIFEXITED(status) || WEXITSTATUS(status)) {
            if (!observed) kill(child, SIGKILL);
            return 1;
        }
    }
    return 0;
}
