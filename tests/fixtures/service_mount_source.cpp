// Ordinary mount-source functional checks in a disposable WOS guest.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
constexpr const char* target = "/tmp/goal19-source-target";
int wait_child(pid_t child) {
    if (child < 0) return 2;
    int status = 0;
    pid_t observed;
    do {
        observed = waitpid(child, &status, 0);
    } while (observed < 0 && errno == EINTR);
    return observed == child && WIFEXITED(status) ? WEXITSTATUS(status) : 2;
}
int profile(unsigned mode, const char* source) {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 1 && (ker::process::prctl(38, 1, 0, 0, 0) || setgid(104) || setuid(104))) return 2;
    if (mode >= 2 && ker::process::prctl(0x574f5302, mode == 3 ? 1UL << 10 : 0, 0, 0, 0)) return 2;
    bool const allowed = mode == 0 || mode == 3;
    uid_t r, e, s;
    gid_t gr, ge, gs;
    if (getresuid(&r, &e, &s) || getresgid(&gr, &ge, &gs)) return 2;
    auto ceiling = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto grants = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    auto nnp = ker::process::prctl(39, 0, 0, 0, 0);
    unsigned failures = 0;
    auto check = [&](bool okay, const char* label) {
        printf("%s profile=%u %s\n", okay ? "PASS" : "FAIL", mode, label);
        failures += !okay;
    };
    auto refused = [&](const char* path, int expected, const char* label) {
        errno = 0;
        int result = mount(path, target, "fat32", MS_RDONLY | MS_NOSUID, nullptr);
        check(result == -1 && errno == (allowed ? expected : EPERM), label);
        if (!result && umount(target)) ++failures;
    };
    refused("/dev/null", ENOTBLK, "non-block source");
    refused("/dev/goal19-source-does-not-exist", ENOENT, "missing source");
    char trailing[512];
    snprintf(trailing, sizeof(trailing), "%s/", source);
    refused(trailing, ENOTDIR, "block source requires directory");
    struct stat before{}, after{};
    if (stat(target, &before)) return 2;
    errno = 0;
    int result = mount(source, target, "fat32", MS_RDONLY | MS_NOSUID, nullptr);
    check(allowed ? result == 0 : result == -1 && errno == EPERM, "selected boot source");
    if (!result) {
        char path[256];
        snprintf(path, sizeof(path), "%s/limine/limine.conf", target);
        int fd = open(path, O_RDONLY);
        char byte = 0;
        bool io = fd >= 0 && read(fd, &byte, 1) == 1;
        int io_error = errno;
        struct statvfs policy{};
        int policy_result = statvfs(target, &policy);
        bool flags = !policy_result && (policy.f_flag & ST_NOSUID) && (policy.f_flag & ST_RDONLY);
        struct statvfs retained_policy{};
        check(fd >= 0 && !fstatvfs(fd, &retained_policy) && (retained_policy.f_flag & ST_RDONLY) && (retained_policy.f_flag & ST_NOSUID),
              "retained file readonly policy");
        if (fd >= 0) close(fd);
        errno = 0;
        int write_fd = open(path, O_WRONLY);
        check(write_fd == -1 && errno == EROFS, "readonly write-open refused");
        if (write_fd >= 0) close(write_fd);
        int cleanup = umount(target);
        if (!io || !flags || cleanup)
            printf("DETAIL fd=%d io=%d io_errno=%d statvfs=%d flags=%lu cleanup=%d errno=%d\n", fd, io, io_error, policy_result,
                   policy.f_flag, cleanup, errno);
        check(io && flags && !cleanup, "read-only boot content and cleanup");
    } else {
        check(!stat(target, &after) && before.st_ino == after.st_ino && before.st_dev == after.st_dev && before.st_mode == after.st_mode,
              "denial leaves target unchanged");
    }
    uid_t ar, ae, as;
    gid_t agr, age, ags;
    check(!getresuid(&ar, &ae, &as) && !getresgid(&agr, &age, &ags) && r == ar && e == ae && s == as && gr == agr && ge == age &&
              gs == ags && getgroups(0, nullptr) == 0 && ceiling == ker::process::prctl(0x574f5301, 0, 0, 0, 0) &&
              grants == ker::process::prctl(0x574f5304, 0, 0, 0, 0) && nnp == ker::process::prctl(39, 0, 0, 0, 0),
          "credentials preserved");
    fflush(stdout);
    return failures != 0;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc != 2 || geteuid()) return 2;
    alarm(60);
    char canonical[512];
    if (!realpath(argv[1], canonical) || strncmp(canonical, "/dev/", 5) || strchr(canonical + 5, '/')) return 2;
    if (mkdir(target, 0777) || chmod(target, 0777)) return 2;
    int result = 0;
    for (unsigned mode = 0; mode < 4; ++mode) {
        pid_t child = fork();
        if (!child) _exit(profile(mode, argv[1]));
        result |= wait_child(child);
        errno = 0;
        int cleanup = umount(target);
        if (cleanup != -1 || errno != ENOENT) result = 1;
    }
    if (rmdir(target)) result = 1;
    if (!result)
        puts("PASS: 28 native mount source checks; cleanup completed");
    else
        puts("Native mount source checks completed with failures; fixture paths cleaned");
    return result;
}
