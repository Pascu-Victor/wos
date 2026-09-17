// Functional current-caller xattr DAC checks on disposable native XFS files.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <initializer_list>

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    unsigned checks = 0, failed = 0;
    char path[512];
    snprintf(path, sizeof(path), "%s/object", argv[1]);
    int fd = open(path, O_CREAT | O_EXCL | O_RDONLY, 0644);
    if (fd < 0 || chown(path, 1001, 2002)) return 90;
    for (unsigned profile = 0; profile < 5; ++profile) {
        unsigned uid = profile == 0 ? 0 : profile == 1 ? 1001 : 1002;
        for (unsigned mode : {0600U, 0060U, 0006U, 0000U, 0444U, 0222U}) {
            if (chmod(path, mode)) return 91;
            unsigned shift = profile == 1 ? 6 : profile == 2 || profile == 3 ? 3 : 0;
            unsigned permission = (mode >> shift) & 7U;
            for (bool descriptor : {false, true}) {
                for (unsigned op = 0; op < 4; ++op) {
                    if (setxattr(path, "user.matrix", "seed", 4, 0)) return 92;
                    struct stat before{}, after{};
                    if (stat(path, &before)) return 93;
                    bool write = op == 0 || op == 3;
                    bool allowed = profile == 0 || (permission & (write ? 2U : 4U));
                    pid_t child = fork();
                    if (child == 0) {
                        gid_t group = 2002;
                        if (setgroups(profile == 3 ? 1 : 0, profile == 3 ? &group : nullptr) || setgid(profile == 2 ? 2002 : 2001) ||
                            setuid(uid))
                            _exit(94);
                        char out[32];
                        memset(out, '!', sizeof(out));
                        errno = 0;
                        ssize_t result = -1;
                        if (op == 0)
                            result = descriptor ? fsetxattr(fd, "user.matrix", "next", 4, XATTR_REPLACE)
                                                : setxattr(path, "user.matrix", "next", 4, XATTR_REPLACE);
                        if (op == 1)
                            result = descriptor ? fgetxattr(fd, "user.matrix", out, sizeof(out))
                                                : getxattr(path, "user.matrix", out, sizeof(out));
                        if (op == 2) result = descriptor ? flistxattr(fd, out, sizeof(out)) : listxattr(path, out, sizeof(out));
                        if (op == 3) result = descriptor ? fremovexattr(fd, "user.matrix") : removexattr(path, "user.matrix");
                        ssize_t expected = !allowed ? -1 : op == 1 ? 4 : op == 2 ? 12 : 0;
                        bool ok = result == expected && (allowed || errno == EACCES);
                        if (!allowed)
                            for (char c : out) ok = c == '!' && ok;
                        if (allowed && op == 1) ok = memcmp(out, "seed", 4) == 0 && out[4] == '!' && ok;
                        if (allowed && op == 2) ok = memcmp(out, "user.matrix", 12) == 0 && out[12] == '!' && ok;
                        if (!ok)
                            printf("operation profile=%u mode=%o fd=%d op=%u result=%ld errno=%d\n", profile, mode, descriptor, op,
                                   long(result), errno);
                        _exit(ok ? 0 : 1);
                    }
                    int status = 0;
                    bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                    ok = stat(path, &after) == 0 && after.st_ino == before.st_ino && after.st_size == before.st_size &&
                         after.st_mode == before.st_mode && after.st_uid == before.st_uid && after.st_gid == before.st_gid && ok;
                    if (!allowed)
                        ok = after.st_ctim.tv_sec == before.st_ctim.tv_sec && after.st_ctim.tv_nsec == before.st_ctim.tv_nsec &&
                             after.st_mtim.tv_sec == before.st_mtim.tv_sec && after.st_mtim.tv_nsec == before.st_mtim.tv_nsec && ok;
                    char value[4];
                    errno = 0;
                    ssize_t n = getxattr(path, "user.matrix", value, sizeof(value));
                    if (allowed && op == 3)
                        ok = n == -1 && errno == ENODATA && ok;
                    else
                        ok = n == 4 && memcmp(value, allowed && op == 0 ? "next" : "seed", 4) == 0 && ok;
                    if (!ok) {
                        printf("FAIL check=%u profile=%u mode=%o fd=%d op=%u\n", checks, profile, mode, descriptor, op);
                        ++failed;
                    }
                    ++checks;
                }
            }
        }
    }
    if (chmod(path, 0666) || setxattr(path, "user.matrix", "seed", 4, 0)) return 96;
    char trailing[513];
    snprintf(trailing, sizeof(trailing), "%s/", path);
    for (unsigned op = 0; op < 4; ++op) {
        char out[32];
        memset(out, '!', sizeof(out));
        errno = 0;
        ssize_t result = op == 0   ? setxattr(trailing, "user.matrix", "next", 4, XATTR_REPLACE)
                         : op == 1 ? getxattr(trailing, "user.matrix", out, sizeof(out))
                         : op == 2 ? listxattr(trailing, out, sizeof(out))
                                   : removexattr(trailing, "user.matrix");
        bool ok = result == -1 && errno == ENOTDIR;
        for (char c : out) ok = c == '!' && ok;
        char value[4];
        ok = getxattr(path, "user.matrix", value, sizeof(value)) == 4 && memcmp(value, "seed", 4) == 0 && ok;
        if (!ok) {
            printf("FAIL trailing-directory op=%u result=%ld\n", op, long(result));
            ++failed;
        }
        ++checks;
    }
    close(fd);
    if (unlink(path)) return 95;
    printf("%s xattr checks=%u failed=%u\n", failed ? "FAIL" : "PASS", checks, failed);
    return failed ? 1 : 0;
}
