// Ordinary readonly metadata functional checks in a disposable WOS guest.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
constexpr const char* TARGET = "/tmp/goal19-readonly-metadata";
int profile(unsigned mode, const char* fs, int fd) {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 1 && (ker::process::prctl(38, 1, 0, 0, 0) || setgid(104) || setuid(104))) return 2;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 2;
    uid_t r, e, s;
    gid_t gr, ge, gs;
    if (getresuid(&r, &e, &s) || getresgid(&gr, &ge, &gs)) return 2;
    auto ceiling = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto grants = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    auto nnp = ker::process::prctl(39, 0, 0, 0, 0);
    unsigned failures = 0;
    auto check = [&](bool okay, const char* label) {
        printf("%s profile=%u fs=%s %s\n", okay ? "PASS" : "FAIL", mode, fs, label);
        failures += !okay;
    };
    auto refused = [&](int result, int expected, const char* label) {
        int const error = errno;
        check(result == -1 && error == expected, label);
        if (result != -1 || error != expected) printf("DETAIL result=%d errno=%d expected=%d\n", result, error, expected);
    };
    struct stat before{}, after{};
    if (fstat(fd, &before)) return 2;
    const struct timespec times[2] = {{123, 456}, {789, 123}};
    const struct timespec omit[2] = {{0, UTIME_OMIT}, {0, UTIME_OMIT}};
    const struct timespec invalid[2] = {{0, 1000000000}, {0, 0}};
    int const denied = mode == 1 ? EPERM : EROFS;
    refused(chmod(TARGET, 0777), denied, "pathname chmod");
    refused(fchmodat(fd, ".", 0777, 0), denied, "dirfd chmod");
    refused(utimensat(AT_FDCWD, TARGET, times, 0), denied, "pathname explicit times");
    refused(utimensat(fd, ".", times, 0), denied, "dirfd explicit times");
    refused(utimensat(fd, ".", nullptr, 0), mode == 1 && !(before.st_mode & 0002) ? EACCES : EROFS, "dirfd now");
    check(!utimensat(AT_FDCWD, TARGET, omit, 0), "pathname all-omit noop");
    check(!utimensat(fd, ".", omit, 0), "dirfd all-omit noop");
    refused(utimensat(fd, ".", invalid, 0), EINVAL, "invalid timestamp ordering");
    refused(fchmodat(fd, "goal19-no-such-file", 0777, 0), ENOENT, "missing chmod ordering");
    refused(utimensat(fd, "goal19-no-such-file", times, 0), ENOENT, "missing timestamp ordering");
    refused(fchmod(fd, 0777), EROFS, "descriptor chmod");
    refused(futimens(fd, times), EROFS, "descriptor timestamps");
    check(!futimens(fd, omit), "descriptor all-omit noop");
    check(!fstat(fd, &after) && before.st_ino == after.st_ino && before.st_mode == after.st_mode && before.st_uid == after.st_uid &&
              before.st_gid == after.st_gid && before.st_atim.tv_sec == after.st_atim.tv_sec &&
              before.st_atim.tv_nsec == after.st_atim.tv_nsec && before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
              before.st_mtim.tv_nsec == after.st_mtim.tv_nsec && before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
              before.st_ctim.tv_nsec == after.st_ctim.tv_nsec,
          "metadata unchanged");
    struct statvfs policy{};
    check(!fstatvfs(fd, &policy) && (policy.f_flag & ST_RDONLY), "retained readonly policy");
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

int main() {
    if (geteuid()) return 2;
    alarm(60);
    if (mkdir(TARGET, 0755)) return 2;
    int result = 0;
    for (const char* fs : {"tmpfs", "devfs"}) {
        if (mount(nullptr, TARGET, fs, MS_RDONLY, nullptr)) {
            result = 2;
            break;
        }
        int fd = open(TARGET, O_RDONLY | O_DIRECTORY);
        if (fd < 0) {
            result = 2;
        } else {
            for (unsigned mode = 0; mode < 3; ++mode) {
                pid_t child = fork();
                if (!child) _exit(profile(mode, fs, fd));
                int status = 0;
                pid_t observed = -1;
                if (child > 0) {
                    do {
                        observed = waitpid(child, &status, 0);
                    } while (observed < 0 && errno == EINTR);
                }
                if (observed != child || child < 0 || !WIFEXITED(status) || WEXITSTATUS(status)) result = 1;
            }
            if (close(fd)) result = 1;
        }
        if (umount(TARGET)) {
            result = 1;
            break;
        }
    }
    if (rmdir(TARGET)) result = 1;
    puts(result ? "Native readonly metadata checks failed" : "PASS: 96 native readonly metadata checks; cleanup completed");
    return result;
}
