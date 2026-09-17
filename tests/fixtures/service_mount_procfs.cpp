// Functional mount checks on each owned child's procfs directory.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
int profile(unsigned mode) {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 1 && (ker::process::prctl(38, 1, 0, 0, 0) || setgid(104) || setuid(104))) return 2;
    if (mode >= 2 && ker::process::prctl(0x574f5302, mode == 3 ? 1UL << 10 : 0, 0, 0, 0)) return 2;
    bool const allowed = mode == 0 || mode == 3;
    uid_t real, effective, saved;
    gid_t greal, geffective, gsaved;
    if (getresuid(&real, &effective, &saved) || getresgid(&greal, &geffective, &gsaved)) return 2;
    auto const ceiling = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto const grants = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    auto const nnp = ker::process::prctl(39, 0, 0, 0, 0);
    char target[64], marker[96];
    snprintf(target, sizeof(target), "/proc/%d", getpid());
    snprintf(marker, sizeof(marker), "%s/goal19-mount-marker", target);
    struct stat before{}, after{};
    if (stat(target, &before) || !S_ISDIR(before.st_mode)) return 2;
    unsigned failures = 0;
    auto check = [&](bool okay, const char* label) {
        printf("%s profile=%u %s\n", okay ? "PASS" : "FAIL", mode, label);
        failures += !okay;
    };
    errno = 0;
    int result = mount(nullptr, "/proc/self/status", "tmpfs", MS_NOSUID, nullptr);
    check(result == -1 && errno == (allowed ? ENOTDIR : EPERM), "non-directory admission");
    if (result == 0) {
        char status_target[80];
        snprintf(status_target, sizeof(status_target), "%s/status", target);
        if (umount(status_target)) return 1;
    }
    errno = 0;
    result = mount(nullptr, "/proc/self", "tmpfs", MS_NOSUID, nullptr);
    check(allowed ? result == 0 : result == -1 && errno == EPERM, "directory admission");
    if (result == 0) {
        int fd = open("/proc/self/goal19-mount-marker", O_CREAT | O_EXCL | O_RDWR, 0600);
        bool io = fd >= 0 && write(fd, "bound", 5) == 5;
        if (fd >= 0) close(fd);
        fd = open(marker, O_RDONLY);
        char value[8]{};
        io = fd >= 0 && read(fd, value, sizeof(value)) == 5 && !memcmp(value, "bound", 5) && io;
        struct statvfs path_policy{}, file_policy{};
        bool const policy = statvfs(target, &path_policy) == 0 && fd >= 0 && fstatvfs(fd, &file_policy) == 0 &&
                            (path_policy.f_flag & ST_NOSUID) && (file_policy.f_flag & ST_NOSUID);
        if (fd >= 0) close(fd);
        check(io, "self and numeric aliases select one mount");
        check(policy, "retained nosuid policy");
        bool const removed = unlink(marker) == 0;
        int const unmounted = umount(target);
        check(removed && unmounted == 0 && stat(target, &after) == 0 && after.st_dev == before.st_dev && after.st_mode == before.st_mode,
              "mount cleanup restores procfs");
    } else {
        check(
            stat(target, &after) == 0 && after.st_dev == before.st_dev && after.st_ino == before.st_ino && after.st_mode == before.st_mode,
            "denial preserves target");
    }
    uid_t ar, ae, as;
    gid_t gr, ge, gs;
    check(!getresuid(&ar, &ae, &as) && !getresgid(&gr, &ge, &gs) && ar == real && ae == effective && as == saved && gr == greal &&
              ge == geffective && gs == gsaved && getgroups(0, nullptr) == 0 && ceiling == ker::process::prctl(0x574f5301, 0, 0, 0, 0) &&
              grants == ker::process::prctl(0x574f5304, 0, 0, 0, 0) && nnp == ker::process::prctl(39, 0, 0, 0, 0),
          "credentials preserved");
    fflush(stdout);
    return failures != 0;
}
}  // namespace

int main() {
    if (geteuid() != 0) return 2;
    for (unsigned mode = 0; mode < 4; ++mode) {
        pid_t const child = fork();
        if (!child) _exit(profile(mode));
        if (child < 0) return 2;
        int status = 0;
        pid_t observed = 0;
        for (unsigned i = 0; i < 6000 && observed == 0; ++i) {
            observed = waitpid(child, &status, WNOHANG);
            if (observed < 0 && errno == EINTR) observed = 0;
            if (!observed) usleep(1000);
        }
        if (!observed) {
            kill(child, SIGKILL);
            waitpid(child, &status, 0);
        }
        // Remove only this owned child's exact mount if an earlier check failed.
        char target[64];
        snprintf(target, sizeof(target), "/proc/%d", child);
        errno = 0;
        int const cleanup = umount(target);
        if (observed != child || !WIFEXITED(status) || WEXITSTATUS(status) || cleanup != -1 || errno != ENOENT) return 1;
    }
    puts("PASS: 20 native procfs mount checks; cleanup completed");
    return 0;
}
