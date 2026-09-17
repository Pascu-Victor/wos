// Functional two-parent rename permissions and retained-directory checks in disposable WOS VMs.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>
namespace {
char base[512];
constexpr unsigned CASES = 1536;
void path(char* out, const char* suffix) { snprintf(out, 512, "%s/%s", base, suffix); }
void object(char* out, const char* parent, char leaf, unsigned index) { snprintf(out, 512, "%s/%s/%c%u", base, parent, leaf, index); }
unsigned kind_for(unsigned i) { return (i / 16) % 3; }
bool replaced(unsigned i) { return (i / 8) % 2; }
bool create(const char* name, unsigned kind, bool destination) {
    bool ok = false;
    if (kind == 1)
        ok = mkdir(name, 0700) == 0;
    else if (kind == 2)
        ok = symlink(destination ? "literal-destination" : "literal-source", name) == 0;
    else {
        int fd = open(name, O_CREAT | O_EXCL | O_WRONLY, 0600);
        ok = fd >= 0 && write(fd, destination ? "old" : "new", 3) == 3;
        if (fd >= 0) close(fd);
    }
    return fchownat(AT_FDCWD, name, destination ? 1003 : 1002, 2001, AT_SYMLINK_NOFOLLOW) == 0 && ok;
}
int setup() {
    char left[512], right[512], priv[512], name[512];
    path(left, "left-anchor");
    path(right, "right-anchor");
    path(priv, "private");
    bool ok = mkdir(base, 0755) == 0 && mkdir(left, 0770) == 0 && mkdir(right, 0770) == 0 && chown(left, 1001, 2002) == 0 &&
              chown(right, 1001, 2002) == 0 && chmod(left, 0770) == 0 && chmod(right, 0770) == 0 && mkdir(priv, 0710) == 0 &&
              chown(priv, 0, 2000) == 0 && chmod(priv, 0710) == 0;
    for (unsigned i = 0; i < CASES && ok; ++i) {
        object(name, "left-anchor", 'a', i);
        ok = create(name, kind_for(i), false);
        if (replaced(i)) {
            object(name, i % 8 == 7 ? "left-anchor" : "right-anchor", 'b', i);
            ok = create(name, kind_for(i), true) && ok;
        }
        if (!ok) printf("setup index=%u errno=%d\n", i, errno);
    }
    printf("%s setup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int mutate() {
    char old[512], now[512], decoy[512];
    bool ok = true;
    for (auto side : {"left", "right"}) {
        snprintf(old, 512, "%s/%s-anchor", base, side);
        snprintf(now, 512, "%s/%s", base, side);
        snprintf(decoy, 512, "%s/%s-anchor/decoy", base, side);
        ok = rename(old, now) == 0 && mkdir(old, 0755) == 0 && create(decoy, 0, true) && ok;
    }
    printf("%s mutate\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int cleanup() {
    bool ok = true;
    char name[512];
    for (auto side : {"left", "right", "left-anchor", "right-anchor"}) {
        for (unsigned i = 0; i < CASES; ++i)
            for (char leaf : {'a', 'b'}) {
                object(name, side, leaf, i);
                int r = kind_for(i) == 1 ? rmdir(name) : unlink(name);
                if (r && errno != ENOENT) ok = false;
            }
        snprintf(name, 512, "%s/%s/decoy", base, side);
        if (unlink(name) && errno != ENOENT) ok = false;
        path(name, side);
        if (rmdir(name) && errno != ENOENT) ok = false;
    }
    path(name, "private");
    if (rmdir(name) && errno != ENOENT) ok = false;
    if (rmdir(base) && errno != ENOENT) ok = false;
    printf("%s cleanup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
bool same_time(timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; }
bool unchanged(const struct stat& a, const struct stat& b) {
    return a.st_ino == b.st_ino && a.st_nlink == b.st_nlink && a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_gid == b.st_gid &&
           same_time(a.st_ctim, b.st_ctim) && same_time(a.st_mtim, b.st_mtim);
}
int check(const char* ready, const char* gate) {
    char left[512], right[512], src[512], dst[512], oldleaf[64], newleaf[64], raw[512], decoy[512];
    path(left, "left-anchor");
    path(right, "right-anchor");
    int lfd = open(left, O_RDONLY | O_DIRECTORY), rfd = open(right, O_RDONLY | O_DIRECTORY);
    if (lfd < 0 || rfd < 0) return 90;
    int f = open(ready, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (f < 0) return 91;
    close(f);
    bool go = false;
    for (unsigned i = 0; i < 10000; ++i) {
        f = open(gate, O_RDONLY);
        if (f >= 0) {
            close(f);
            go = true;
            break;
        }
        usleep(1000);
    }
    if (!go) return 92;
    path(left, "left");
    path(right, "right");
    unsigned cases = 0, failed = 0;
    for (unsigned uid : {0U, 1001U, 1002U, 1003U})
        for (unsigned grouped = 0; grouped < 2; ++grouped)
            for (unsigned sticky = 0; sticky < 4; ++sticky)
                for (unsigned kind = 0; kind < 3; ++kind)
                    for (unsigned replace = 0; replace < 2; ++replace)
                        for (unsigned method = 0; method < 8; ++method) {
                            object(src, "left", 'a', cases);
                            object(dst, method == 7 ? "left" : "right", 'b', cases);
                            snprintf(raw, 512, "%s/private/../left/a%u", base, cases);
                            snprintf(oldleaf, sizeof(oldleaf), method == 2 ? "./a%u" : method == 6 ? "a%u/" : "a%u", cases);
                            snprintf(newleaf, sizeof(newleaf), method == 2 ? "./b%u" : "b%u", cases);
                            if (chmod(left, (sticky & 1) ? 01770 : 0770) || chmod(right, (sticky & 2) ? 01770 : 0770)) return 93;
                            struct stat a{}, b{}, pa{}, pb{}, aa{}, bb{}, paa{}, pbb{};
                            if (lstat(src, &a) || stat(left, &pa) || stat(right, &pb) || (replace && lstat(dst, &b))) return 94;
                            bool search = uid == 0 || uid == 1001 || grouped;
                            if (method == 3 && uid != 0 && !grouped) search = false;
                            unsigned dest_sticky = method == 7 ? (sticky & 1) : (sticky & 2);
                            int expected = !search                                                            ? EACCES
                                           : (sticky & 1) && uid != 0 && uid != 1001 && uid != 1002           ? EPERM
                                           : replace && dest_sticky && uid != 0 && uid != 1001 && uid != 1003 ? EPERM
                                           : kind == 1 && method != 7 && uid != 0 && uid != 1002              ? EACCES
                                           : method == 6 && kind != 1                                         ? ENOTDIR
                                                                                                              : 0;
                            int moved_fd = kind == 0 ? open(src, O_RDONLY) : -1,
                                displaced_fd = kind == 0 && replace ? open(dst, O_RDONLY) : -1;
                            if (kind == 0 && (moved_fd < 0 || (replace && displaced_fd < 0))) return 95;
                            pid_t child = fork();
                            if (child == 0) {
                                gid_t groups[] = {2000, 2002};
                                if (setgroups(grouped ? 2 : 0, grouped ? groups : nullptr) || setgid(2001) || setuid(uid)) _exit(96);
                                errno = 0;
                                int r;
                                if (method == 0)
                                    r = rename(src, dst);
                                else if (method == 3)
                                    r = rename(raw, dst);
                                else if (method == 4)
                                    r = renameat(AT_FDCWD, src, rfd, newleaf);
                                else if (method == 5)
                                    r = renameat(lfd, oldleaf, AT_FDCWD, dst);
                                else
                                    r = renameat(lfd, oldleaf, method == 7 ? lfd : rfd, newleaf);
                                int e = errno;
                                bool valid = expected ? r == -1 && e == expected : r == 0;
                                if (!valid && failed < 16)
                                    printf(
                                        "operation uid=%u group=%u sticky=%u kind=%u replace=%u method=%u result=%d errno=%d expected=%d\n",
                                        uid, grouped, sticky, kind, replace, method, r, e, expected);
                                _exit(valid ? 0 : 1);
                            }
                            int status = 0;
                            bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                            ok = stat(left, &paa) == 0 && stat(right, &pbb) == 0 && ok;
                            if (expected) {
                                ok = lstat(src, &aa) == 0 && unchanged(a, aa) && unchanged(pa, paa) && unchanged(pb, pbb) && ok;
                                if (replace)
                                    ok = lstat(dst, &bb) == 0 && unchanged(b, bb) && ok;
                                else {
                                    errno = 0;
                                    ok = lstat(dst, &bb) == -1 && errno == ENOENT && ok;
                                }
                            } else {
                                errno = 0;
                                ok = lstat(src, &aa) == -1 && errno == ENOENT && lstat(dst, &bb) == 0 && bb.st_ino == a.st_ino &&
                                     bb.st_uid == a.st_uid && bb.st_gid == a.st_gid && ok;
                                if (kind == 2) {
                                    char target[64]{};
                                    ok = readlink(dst, target, sizeof(target)) == 14 && memcmp(target, "literal-source", 14) == 0 && ok;
                                }
                            }
                            for (auto fd : {moved_fd, displaced_fd})
                                if (fd >= 0) {
                                    char text[3]{};
                                    ok = read(fd, text, 3) == 3 && memcmp(text, fd == moved_fd ? "new" : "old", 3) == 0 && ok;
                                    close(fd);
                                }
                            for (auto side : {"left-anchor", "right-anchor"}) {
                                snprintf(decoy, 512, "%s/%s/decoy", base, side);
                                struct stat other{};
                                ok = lstat(decoy, &other) == 0 && other.st_size == 3 && ok;
                                object(decoy, side, 'a', cases);
                                errno = 0;
                                ok = lstat(decoy, &other) == -1 && errno == ENOENT && ok;
                                object(decoy, side, 'b', cases);
                                errno = 0;
                                ok = lstat(decoy, &other) == -1 && errno == ENOENT && ok;
                            }
                            if (!ok) {
                                if (failed < 16) printf("FAIL case=%u status=%d\n", cases, status);
                                ++failed;
                            }
                            ++cases;
                        }
    close(lfd);
    close(rfd);
    printf("%s rename checks=%u failed=%u\n", failed ? "FAIL" : "PASS", cases, failed);
    return failed ? 1 : 0;
}
int unsupported() {
    char src[512], dst[512], left[512], right[512];
    object(src, "left-anchor", 'a', 0);
    object(dst, "right-anchor", 'b', 0);
    path(left, "left-anchor");
    path(right, "right-anchor");
    struct stat a{}, pa{}, pb{}, aa{}, paa{}, pbb{}, missing{};
    if (lstat(src, &a) || stat(left, &pa) || stat(right, &pb)) return 90;
    errno = 0;
    int r = rename(src, dst), e = errno;
    bool ok = r == -1 && e == ENOTSUP && lstat(src, &aa) == 0 && unchanged(a, aa) && stat(left, &paa) == 0 && stat(right, &pbb) == 0 &&
              unchanged(pa, paa) && unchanged(pb, pbb);
    errno = 0;
    ok = lstat(dst, &missing) == -1 && errno == ENOENT && ok;
    printf("%s unsupported rename=%d errno=%d unchanged=%d\n", ok ? "PASS" : "FAIL", r, e, ok);
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc < 3 || strlen(argv[2]) >= sizeof(base)) return 2;
    strcpy(base, argv[2]);
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    if (strcmp(argv[1], "check") && strcmp(argv[1], "unsupported") &&
        ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
        return 3;
    if (!strcmp(argv[1], "setup")) return setup();
    if (!strcmp(argv[1], "mutate")) return mutate();
    if (!strcmp(argv[1], "cleanup")) return cleanup();
    if (!strcmp(argv[1], "unsupported")) return unsupported();
    if (!strcmp(argv[1], "check") && argc >= 5) return check(argv[3], argv[4]);
    return 2;
}
