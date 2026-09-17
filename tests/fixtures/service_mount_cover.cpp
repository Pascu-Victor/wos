// Ordinary mount-cover and retained-directory checks on disposable WOS nodes.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
unsigned failures;
void check(bool okay, const char* phase, const char* operation) {
    printf("%s phase=%s uid=%u op=%s\n", okay ? "PASS" : "FAIL", phase, geteuid(), operation);
    failures += !okay;
}
void observe(const char* phase, const char* path, int directory, bool covered) {
    struct stat st{};
    int rc = stat(path, &st);
    check(rc == 0 && (st.st_mode & 0777) == (covered ? 0644 : 0000), phase, "stat");
    check(access(path, R_OK) == 0, phase, "access");
    char resolved[4096]{};
    check(realpath(path, resolved) != nullptr && strcmp(resolved, path) == 0, phase, "realpath");
    for (int retained = 0; retained < 2; ++retained) {
        errno = 0;
        int fd = retained ? openat(directory, "marker", O_RDONLY) : open(path, O_RDONLY);
        int error = errno;
        char marker[32]{};
        ssize_t count = fd >= 0 ? read(fd, marker, sizeof(marker) - 1) : -1;
        if (fd >= 0) close(fd);
        bool denied = retained && geteuid() != 0;
        const char* expected = covered && !retained ? "REMOTE" : "LOCAL";
        check(denied ? fd == -1 && error == EACCES : fd >= 0 && count == (ssize_t)strlen(expected) && strcmp(marker, expected) == 0, phase,
              retained ? "retained-openat" : "pathname-open");
    }
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc != 4 || geteuid() != 0) return 2;
    char child[512]{}, marker[512]{}, fresh[512]{};
    if (snprintf(child, sizeof(child), "%s/%s", argv[1], argv[2]) >= (int)sizeof(child) ||
        snprintf(marker, sizeof(marker), "%s/marker", child) >= (int)sizeof(marker) ||
        snprintf(fresh, sizeof(fresh), "%s-new", child) >= (int)sizeof(fresh))
        return 2;
    if (mkdir(argv[1], 0755) || mkdir(child, 0755) || mount(nullptr, child, "tmpfs", 0, nullptr)) {
        printf("SETUP errno=%d\n", errno);
        return 3;
    }
    int fd = open(marker, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0 || write(fd, "LOCAL", 5) != 5 || close(fd) || chmod(marker, 0000)) return 4;
    int directory = open(child, O_RDONLY | O_DIRECTORY);
    if (directory < 0) return 5;
    observe("before", marker, directory, false);
    errno = 0;
    int mounted = mount(argv[3], argv[1], "remote", 0, nullptr);
    printf("MOUNT parent rc=%d errno=%d\n", mounted, errno);
    check(mounted == 0, "cover", "mount-parent");
    if (mounted == 0) {
        observe("covered", marker, directory, true);
        pid_t pid = fork();
        if (pid == 0) {
            failures = 0;
            if (setgroups(0, nullptr) || setgid(1002) || setuid(1002)) _exit(6);
            observe("covered-user", marker, directory, true);
            _exit(failures ? 1 : 0);
        }
        int status = 0;
        if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) ++failures;
        errno = 0;
        int denied = mount(nullptr, child, "tmpfs", 0, nullptr);
        printf("MOUNT child rc=%d errno=%d\n", denied, errno);
        check(denied == -1 && errno == EBUSY, "denial", "occupied-child");
        errno = 0;
        denied = mount(nullptr, fresh, "tmpfs", 0, nullptr);
        printf("MOUNT fresh rc=%d errno=%d\n", denied, errno);
        check(denied == -1 && errno == EXDEV, "denial", "new-child-on-remote");
        errno = 0;
        denied = umount(argv[1]);
        check(denied == -1 && errno == EBUSY, "denial", "parent-before-child");
        observe("after-denials", marker, directory, true);
    }
    if (close(directory)) return 7;
    int child_result = umount(child);
    int parent_result = mounted == 0 ? umount(argv[1]) : 0;
    printf("CLEAN child=%d parent=%d errno=%d\n", child_result, parent_result, errno);
    if (child_result || parent_result) return 8;
    int child_dir = rmdir(child), parent_dir = rmdir(argv[1]);
    printf("MOUNT_COVER failures=%u cleanup=%d\n", failures, child_dir == 0 && parent_dir == 0);
    return failures || child_dir || parent_dir ? 1 : 0;
}
