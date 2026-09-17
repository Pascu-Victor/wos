// Functional mapped xattr checks on disposable owner XFS files.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <initializer_list>

namespace {
unsigned extra_checks = 0, extra_failed = 0;
void expect(bool ok, const char* what) {
    ++extra_checks;
    if (!ok) {
        ++extra_failed;
        printf("FAIL %s errno=%d\n", what, errno);
    }
}
bool create_file(const char* path) {
    int fd = open(path, O_CREAT | O_EXCL | O_RDONLY, 0666);
    return fd >= 0 && close(fd) == 0;
}
ssize_t operate(unsigned op, const char* path, int fd, char* out, bool nofollow = false) {
    if (op == 0)
        return fd >= 0    ? fsetxattr(fd, "user.matrix", "next", 4, XATTR_REPLACE)
               : nofollow ? lsetxattr(path, "user.matrix", "next", 4, XATTR_REPLACE)
                          : setxattr(path, "user.matrix", "next", 4, XATTR_REPLACE);
    if (op == 1)
        return fd >= 0    ? fgetxattr(fd, "user.matrix", out, 32)
               : nofollow ? lgetxattr(path, "user.matrix", out, 32)
                          : getxattr(path, "user.matrix", out, 32);
    if (op == 2) return fd >= 0 ? flistxattr(fd, out, 32) : nofollow ? llistxattr(path, out, 32) : listxattr(path, out, 32);
    return fd >= 0 ? fremovexattr(fd, "user.matrix") : nofollow ? lremovexattr(path, "user.matrix") : removexattr(path, "user.matrix");
}
int extra(const char* mode, const char* base) {
    char pub[512], priv[512], link[512], raw[512], anchor[512], moved[512], file[512], renamed[512];
    snprintf(pub, sizeof(pub), "%s/public", base);
    snprintf(priv, sizeof(priv), "%s/private", base);
    snprintf(link, sizeof(link), "%s/link", base);
    snprintf(raw, sizeof(raw), "%s/private/../public", base);
    snprintf(anchor, sizeof(anchor), "%s/anchor", base);
    snprintf(moved, sizeof(moved), "%s/moved", base);
    snprintf(file, sizeof(file), "%s/anchor/file", base);
    snprintf(renamed, sizeof(renamed), "%s/moved/file", base);
    if (!strcmp(mode, "extra-setup")) {
        bool ok = create_file(pub) && mkdir(priv, 0710) == 0 && chown(priv, 0, 2002) == 0 && symlink(raw, link) == 0 &&
                  mkdir(anchor, 0755) == 0 && create_file(file) && setxattr(pub, "user.matrix", "seed", 4, 0) == 0 &&
                  setxattr(pub, "trusted.hidden", "hidden", 6, 0) == 0;
        printf("%s extra-setup\n", ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }
    if (!strcmp(mode, "cleanup")) {
        bool ok = true;
        char object[512];
        snprintf(object, sizeof(object), "%s/object", base);
        for (const char* path : {object, pub, link, file, renamed})
            if (unlink(path) && errno != ENOENT) ok = false;
        for (const char* path : std::initializer_list<const char*>{anchor, moved, priv, base})
            if (rmdir(path) && errno != ENOENT) ok = false;
        printf("%s cleanup\n", ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }
    if (!strcmp(mode, "verify-unsupported")) {
        char out[8]{};
        bool ok = getxattr(pub, "user.matrix", out, sizeof(out)) == 4 && memcmp(out, "seed", 4) == 0;
        printf("%s verify-unsupported\n", ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }
    int fd = open(pub, O_RDONLY);
    if (fd < 0) return 80;
    if (!strcmp(mode, "unsupported")) {
        for (bool descriptor : {false, true})
            for (unsigned op = 0; op < 4; ++op) {
                char out[32];
                memset(out, '!', sizeof(out));
                errno = 0;
                ssize_t result = operate(op, pub, descriptor ? fd : -1, out);
                bool ok = result == -1 && errno == EOPNOTSUPP;
                for (char c : out) ok &= c == '!';
                expect(ok, "unsupported owner");
            }
    } else {
        // Search permission must apply to each original component, including before '..'.
        for (bool grouped : {false, true})
            for (const char* path : {raw, link})
                for (unsigned op = 0; op < 4; ++op) {
                    if (fsetxattr(fd, "user.matrix", "seed", 4, 0)) return 81;
                    pid_t child = fork();
                    if (child == 0) {
                        gid_t group = 2002;
                        if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1002)) _exit(2);
                        char out[32];
                        memset(out, '!', sizeof(out));
                        errno = 0;
                        ssize_t result = operate(op, path, -1, out);
                        bool ok = grouped ? result == (op == 1 ? 4 : op == 2 ? 12 : 0) : result == -1 && errno == EACCES;
                        if (!grouped)
                            for (char c : out) ok &= c == '!';
                        _exit(ok ? 0 : 1);
                    }
                    int status = 0;
                    bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                    char out[8]{};
                    errno = 0;
                    ssize_t n = fgetxattr(fd, "user.matrix", out, sizeof(out));
                    ok &=
                        grouped && op == 3 ? n == -1 && errno == ENODATA : n == 4 && !memcmp(out, grouped && op == 0 ? "next" : "seed", 4);
                    expect(ok, "raw search and caller group");
                }
        if (fsetxattr(fd, "user.matrix", "seed", 4, 0)) return 82;
        // The remote namespace exposes user.* only, even to root.
        for (bool descriptor : {false, true}) {
            char out[64]{};
            expect(
                (descriptor ? flistxattr(fd, out, sizeof(out)) : listxattr(pub, out, sizeof(out))) == 12 && !memcmp(out, "user.matrix", 12),
                "filtered list");
            errno = 0;
            expect((descriptor ? fgetxattr(fd, "trusted.hidden", out, sizeof(out)) : getxattr(pub, "trusted.hidden", out, sizeof(out))) ==
                           -1 &&
                       errno == EOPNOTSUPP,
                   "namespace boundary");
            expect((descriptor ? fgetxattr(fd, "user.matrix", nullptr, 0) : getxattr(pub, "user.matrix", nullptr, 0)) == 4, "size probe");
            memset(out, '!', sizeof(out));
            errno = 0;
            ssize_t n = descriptor ? fgetxattr(fd, "user.matrix", out, 3) : getxattr(pub, "user.matrix", out, 3);
            bool ok = n == -1 && errno == ERANGE;
            for (char c : out) ok &= c == '!';
            expect(ok, "short buffer unchanged");
            errno = 0;
            expect((descriptor ? fsetxattr(fd, "user.matrix", "bad", 3, XATTR_CREATE)
                               : setxattr(pub, "user.matrix", "bad", 3, XATTR_CREATE)) == -1 &&
                       errno == EEXIST,
                   "create existing");
            errno = 0;
            expect((descriptor ? fsetxattr(fd, "user.missing", "bad", 3, XATTR_REPLACE)
                               : setxattr(pub, "user.missing", "bad", 3, XATTR_REPLACE)) == -1 &&
                       errno == ENODATA,
                   "replace missing");
        }
        static unsigned char large[65536], output[65536];
        for (size_t i = 0; i < sizeof(large); ++i) large[i] = static_cast<unsigned char>(i * 37 + 11);
        for (bool descriptor : {false, true}) {
            expect((descriptor ? fsetxattr(fd, "user.large", large, sizeof(large), 0)
                               : setxattr(link, "user.large", large, sizeof(large), 0)) == 0,
                   "64K set");
            memset(output, 0, sizeof(output));
            expect((descriptor ? fgetxattr(fd, "user.large", output, sizeof(output))
                               : getxattr(link, "user.large", output, sizeof(output))) == sizeof(output) &&
                       !memcmp(large, output, sizeof(output)),
                   "64K get");
            expect((descriptor ? fsetxattr(fd, "user.large", nullptr, 0, XATTR_REPLACE)
                               : setxattr(link, "user.large", nullptr, 0, XATTR_REPLACE)) == 0,
                   "empty set");
            expect((descriptor ? fgetxattr(fd, "user.large", nullptr, 0) : getxattr(link, "user.large", nullptr, 0)) == 0, "empty get");
            expect((descriptor ? fremovexattr(fd, "user.large") : removexattr(link, "user.large")) == 0, "large remove");
        }
        // WOS forbids user.* mutation on symlinks; nofollow must not touch the target.
        errno = 0;
        expect(lsetxattr(link, "user.matrix", "link", 4, 0) == -1 && errno == EPERM, "nofollow set");
        char out[32]{};
        errno = 0;
        expect(lgetxattr(link, "user.matrix", out, sizeof(out)) == -1 && errno == ENODATA, "nofollow get");
        expect(llistxattr(link, out, sizeof(out)) == 0, "nofollow list");
        errno = 0;
        expect(lremovexattr(link, "user.matrix") == -1 && errno == EPERM, "nofollow remove");
        // Pin descriptions, then rename and reuse their former pathnames.
        int dir = open(anchor, O_RDONLY | O_DIRECTORY), held = open(file, O_RDONLY);
        if (dir < 0 || held < 0 || fsetxattr(held, "user.matrix", "seed", 4, 0) || rename(anchor, moved) || mkdir(anchor, 0755) ||
            !create_file(file) || setxattr(file, "user.matrix", "fake", 4, 0))
            return 83;
        // Retain the known remote-cwd regression as a separately runnable case.
        if (!strcmp(mode, "cwd-regression"))
            for (unsigned op = 0; op < 4; ++op) {
                if (fsetxattr(held, "user.matrix", "seed", 4, 0)) return 84;
                pid_t child = fork();
                if (child == 0) {
                    if (fchdir(dir) || setgroups(0, nullptr) || setgid(2001) || setuid(1002)) {
                        printf("anchor setup errno=%d\n", errno);
                        _exit(2);
                    }
                    char bytes[32]{};
                    ssize_t n = operate(op, "file", -1, bytes);
                    printf("anchor op=%u result=%ld errno=%d value=%.4s\n", op, long(n), errno, bytes);
                    _exit(n == (op == 1 ? 4 : op == 2 ? 12 : 0) && (op != 1 || !memcmp(bytes, "seed", 4)) ? 0 : 1);
                }
                int status = 0;
                bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                ok &= getxattr(file, "user.matrix", out, sizeof(out)) == 4 && !memcmp(out, "fake", 4);
                errno = 0;
                ssize_t n = fgetxattr(held, "user.matrix", out, sizeof(out));
                ok &= op == 3 ? n == -1 && errno == ENODATA : n == 4 && !memcmp(out, op == 0 ? "next" : "seed", 4);
                expect(ok, "retained directory after rename and reuse");
            }
        if (unlink(renamed)) return 85;
        expect(fsetxattr(held, "user.matrix", "held", 4, 0) == 0, "unlinked file set");
        expect(fgetxattr(held, "user.matrix", out, sizeof(out)) == 4 && !memcmp(out, "held", 4), "unlinked file get");
        expect(flistxattr(held, out, sizeof(out)) == 12, "unlinked file list");
        expect(fremovexattr(held, "user.matrix") == 0, "unlinked file remove");
        close(held);
        close(dir);
    }
    close(fd);
    printf("%s %s checks=%u failed=%u\n", extra_failed ? "FAIL" : "PASS", mode, extra_checks, extra_failed);
    return extra_failed ? 1 : 0;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    if (strcmp(argv[1], "setup") && strcmp(argv[1], "check")) {
        if (!strcmp(argv[1], "extra-setup") || !strcmp(argv[1], "cleanup") || !strcmp(argv[1], "verify-unsupported"))
            if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 79;
        return extra(argv[1], argv[2]);
    }
    unsigned checks = 0, failed = 0;
    char path[512];
    snprintf(path, sizeof(path), "%s/object", argv[2]);
    if (!strcmp(argv[1], "setup")) {
        if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 79;
        if (mkdir(argv[2], 0755)) return 80;
        int created = open(path, O_CREAT | O_EXCL | O_RDONLY, 0644);
        if (created < 0 || chown(path, 1001, 2002)) return 81;
        close(created);
        puts("PASS setup");
        return 0;
    }
    if (strcmp(argv[1], "check")) return 2;
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 90;
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
