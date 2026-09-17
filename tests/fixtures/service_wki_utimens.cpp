// Functional timestamp ownership checks in a disposable WOS cluster.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
char base[512];
void path(char* out, const char* suffix) { snprintf(out, 512, "%s/%s", base, suffix); }
bool make_file(const char* suffix) {
    char name[512];
    path(name, suffix);
    int fd = open(name, O_CREAT | O_EXCL | O_WRONLY, 0664);
    if (fd < 0) return false;
    return close(fd) == 0 && chown(name, 1001, 2002) == 0 && chmod(name, 0664) == 0;
}
int setup() {
    char anchor[512], priv[512], link[512];
    path(anchor, "anchor");
    path(priv, "private");
    path(link, "anchor/link");
    bool ok =
        mkdir(base, 0755) == 0 && mkdir(anchor, 0755) == 0 && mkdir(priv, 0710) == 0 && chown(priv, 0, 2000) == 0 && chmod(priv, 0710) == 0;
    ok = make_file("anchor/file") && ok;
    ok = symlink("file", link) == 0 && lchown(link, 1001, 2002) == 0 && ok;
    printf("%s setup errno=%d\n", ok ? "PASS" : "FAIL", ok ? 0 : errno);
    return ok ? 0 : 1;
}
int mutate() {
    char old[512], moved[512];
    path(old, "anchor");
    path(moved, "moved");
    bool ok = rename(old, moved) == 0 && mkdir(old, 0755) == 0 && make_file("anchor/file");
    printf("%s mutate\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int cleanup() {
    bool ok = true;
    char name[512];
    const char* files[] = {"anchor/link", "anchor/file", "moved/link", "moved/file"};
    for (auto suffix : files) {
        path(name, suffix);
        if (unlink(name) && errno != ENOENT) ok = false;
    }
    const char* dirs[] = {"anchor", "moved", "private"};
    for (auto suffix : dirs) {
        path(name, suffix);
        if (rmdir(name) && errno != ENOENT) ok = false;
    }
    if (rmdir(base) && errno != ENOENT) ok = false;
    printf("%s cleanup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int check(const char* ready, const char* gate) {
    char anchor[512], target[512], link[512], decoy[512], raw[512], trailing[512];
    path(anchor, "anchor");
    path(target, "moved/file");
    path(link, "moved/link");
    path(decoy, "anchor/file");
    path(raw, "private/../moved/file");
    path(trailing, "moved/file/");
    int dir = open(anchor, O_RDONLY | O_DIRECTORY);
    if (dir < 0) return 90;
    int fd = open(ready, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd < 0) return 91;
    close(fd);
    bool signaled = false;
    for (unsigned i = 0; i < 10000; ++i) {
        fd = open(gate, O_RDONLY);
        if (fd >= 0) {
            close(fd);
            signaled = true;
            break;
        }
        usleep(1000);
    }
    if (!signaled) return 92;
    unsigned identities[] = {0, 1001, 1002};
    unsigned cases = 0, failed = 0;
    for (auto uid : identities)
        for (unsigned grouped = 0; grouped < 2; ++grouped)
            for (unsigned method = 0; method < 8; ++method) {
                for (unsigned intent = 0; intent < 6; ++intent) {
                    bool const NOFOLLOW = method == 7;
                    const char* observed = NOFOLLOW ? link : target;
                    if (chmod(target, 0664) || fchmodat(AT_FDCWD, link, 0777, AT_SYMLINK_NOFOLLOW)) return 93;
                    timespec const baseline[2] = {{1600000000, 101}, {1600000010, 202}};
                    if (utimensat(AT_FDCWD, target, baseline, 0) || utimensat(AT_FDCWD, link, baseline, AT_SYMLINK_NOFOLLOW)) return 96;
                    struct stat before{}, after{}, other{}, target_after{};
                    if (lstat(observed, &before) || stat(decoy, &other) || ((other.st_mode & 07777) != 0664)) return 94;
                    pid_t child = fork();
                    if (child == 0) {
                        gid_t groups[] = {2000, 2002};
                        if (setgroups(grouped ? 2 : 0, grouped ? groups : nullptr) || setgid(2001) || setuid(uid)) _exit(95);
                        timespec times[2] = {{1700000000, 303}, {1700000010, 404}};
                        if (intent == 2) times[0].tv_nsec = times[1].tv_nsec = UTIME_NOW;
                        if (intent == 3) {
                            times[0].tv_nsec = UTIME_NOW;
                            times[1].tv_nsec = UTIME_OMIT;
                        }
                        if (intent == 4) times[0].tv_nsec = times[1].tv_nsec = UTIME_OMIT;
                        if (intent == 5) times[0].tv_nsec = 1000000000;
                        const timespec* requested = intent == 1 ? nullptr : times;
                        errno = 0;
                        int result;
                        switch (method) {
                            case 0:
                                result = utimensat(AT_FDCWD, target, requested, 0);
                                break;
                            case 1:
                                result = utimensat(dir, "file", requested, 0);
                                break;
                            case 2:
                                result = utimensat(dir, "./file", requested, 0);
                                break;
                            case 3:
                                result = utimensat(AT_FDCWD, raw, requested, 0);
                                break;
                            case 4:
                                result = utimensat(AT_FDCWD, trailing, requested, 0);
                                break;
                            case 5:
                                result = utimensat(dir, "link", requested, 0);
                                break;
                            case 6:
                                result = utimensat(dir, "missing", requested, 0);
                                break;
                            default:
                                result = utimensat(dir, "link", requested, AT_SYMLINK_NOFOLLOW);
                                break;
                        }
                        int error = errno;
                        bool const NOW = intent == 1 || intent == 2;
                        int expected = intent == 5                              ? EINVAL
                                       : method == 4                            ? ENOTDIR
                                       : method == 6                            ? ENOENT
                                       : method == 3 && uid != 0 && !grouped    ? EACCES
                                       : intent == 4 || uid == 0 || uid == 1001 ? 0
                                       : NOW && (grouped || NOFOLLOW)           ? 0
                                       : NOW                                    ? EACCES
                                                                                : EPERM;
                        bool ok = expected ? result == -1 && error == expected : result == 0;
                        if (!ok)
                            printf("operation mismatch uid=%u grouped=%u method=%u intent=%u result=%d errno=%d expected=%d\n", uid,
                                   grouped, method, intent, result, error, expected);
                        _exit(ok ? 0 : 1);
                    }
                    int status = 0;
                    bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                    bool const NOW = intent == 1 || intent == 2;
                    bool denied = intent == 5 || method == 4 || method == 6 || (method == 3 && uid != 0 && !grouped) ||
                                  (uid == 1002 && intent != 4 && !(NOW && (grouped || NOFOLLOW)));
                    bool unchanged = denied || intent == 4;
                    auto same_time = [](timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; };
                    ok = lstat(observed, &after) == 0 && after.st_mode == before.st_mode && after.st_ino == before.st_ino &&
                         after.st_uid == before.st_uid && after.st_gid == before.st_gid && ok;
                    if (unchanged)
                        ok = same_time(after.st_atim, before.st_atim) && same_time(after.st_mtim, before.st_mtim) &&
                             same_time(after.st_ctim, before.st_ctim) && ok;
                    else if (intent == 0)
                        ok = same_time(after.st_atim, {1700000000, 303}) && same_time(after.st_mtim, {1700000010, 404}) && ok;
                    else {
                        ok = !same_time(after.st_atim, before.st_atim) && after.st_atim.tv_nsec >= 0 &&
                             after.st_atim.tv_nsec < 1000000000 && ok;
                        ok = (intent == 3 ? same_time(after.st_mtim, before.st_mtim) : same_time(after.st_atim, after.st_mtim)) && ok;
                    }
                    ok = stat(decoy, &other) == 0 && (other.st_mode & 07777) == 0664 && ok;
                    if (NOFOLLOW)
                        ok = stat(target, &target_after) == 0 && same_time(target_after.st_atim, baseline[0]) &&
                             same_time(target_after.st_mtim, baseline[1]) && ok;
                    printf("%s uid=%u grouped=%u method=%u intent=%u denied=%d\n", ok ? "PASS" : "FAIL", uid, grouped, method, intent,
                           denied);
                    ++cases;
                    failed += !ok;
                }
            }
    close(dir);
    printf("%s utimens checks=%u\n", failed ? "FAIL" : "PASS", cases);
    return failed ? 1 : 0;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc < 3 || strlen(argv[2]) >= sizeof(base)) return 2;
    strcpy(base, argv[2]);
    // Provision on the selected owner; remote compute may otherwise forward
    // persistent root paths home while /tmp already has a local rule.
    if (strcmp(argv[1], "check") && ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 3;
    if (!strcmp(argv[1], "setup")) return setup();
    if (!strcmp(argv[1], "mutate")) return mutate();
    if (!strcmp(argv[1], "cleanup")) return cleanup();
    if (!strcmp(argv[1], "check") && argc == 5) return check(argv[3], argv[4]);
    return 2;
}
