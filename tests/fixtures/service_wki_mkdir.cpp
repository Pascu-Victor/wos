// Functional parent-permission and directory ownership checks in disposable WOS VMs.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
char base[512];
void path(char* out, const char* suffix) { snprintf(out, 512, "%s/%s", base, suffix); }
int setup() {
    char anchor[512], priv[512], link[512], existing[512], absolute[512];
    path(absolute, "moved");
    path(anchor, "anchor");
    path(priv, "private");
    path(link, "link");
    path(existing, "anchor/exists-link");
    bool ok = mkdir(base, 0755) == 0 && mkdir(anchor, 0770) == 0 && chown(anchor, 1001, 2002) == 0 && chmod(anchor, 0770) == 0 &&
              mkdir(priv, 0710) == 0 && chown(priv, 0, 2000) == 0 && chmod(priv, 0710) == 0 && symlink(absolute, link) == 0 &&
              symlink("never-created", existing) == 0;
    printf("%s setup errno=%d\n", ok ? "PASS" : "FAIL", ok ? 0 : errno);
    return ok ? 0 : 1;
}
int mutate() {
    char anchor[512], moved[512];
    path(anchor, "anchor");
    path(moved, "moved");
    bool ok = rename(anchor, moved) == 0 && mkdir(anchor, 0755) == 0;
    printf("%s mutate\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int cleanup() {
    bool ok = true;
    char name[512];
    const char* children[] = {"anchor/child", "moved/child", "unsupported"};
    for (auto suffix : children) {
        path(name, suffix);
        if (rmdir(name) && errno != ENOENT) ok = false;
    }
    const char* links[] = {"anchor/exists-link", "moved/exists-link", "link"};
    for (auto suffix : links) {
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
bool same_time(timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; }
int check(const char* ready, const char* gate) {
    char anchor[512], moved[512], target[512], decoy[512], raw[512], link[512];
    path(anchor, "anchor");
    path(moved, "moved");
    path(target, "moved/child");
    path(decoy, "anchor/child");
    path(raw, "private/../moved/child");
    path(link, "link/child");
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
    unsigned ids[] = {0, 1001, 1002};
    unsigned cases = 0, failed = 0;
    for (unsigned uid : ids)
        for (unsigned grouped = 0; grouped < 2; ++grouped)
            for (unsigned sgid = 0; sgid < 2; ++sgid)
                for (unsigned masked = 0; masked < 2; ++masked)
                    for (unsigned method = 0; method < 8; ++method) {
                        if (chmod(moved, sgid ? 02770 : 0770)) return 93;
                        struct stat before{}, after{}, created{}, other{};
                        if (stat(moved, &before)) return 94;
                        bool permitted = uid == 0 || uid == 1001 || grouped;
                        if (method == 3 && uid != 0 && !grouped) permitted = false;
                        int expected = !permitted ? EACCES : method == 6 ? EEXIST : method == 7 ? ENOENT : 0;
                        pid_t child = fork();
                        if (child == 0) {
                            gid_t groups[] = {2000, 2002};
                            if (setgroups(grouped ? 2 : 0, grouped ? groups : nullptr) || setgid(2001) || setuid(uid)) _exit(95);
                            umask(masked ? 0027 : 0);
                            errno = 0;
                            int r;
                            switch (method) {
                                case 0:
                                    r = mkdir(target, 0777);
                                    break;
                                case 1:
                                    r = mkdirat(dir, "child", 0777);
                                    break;
                                case 2:
                                    r = mkdirat(dir, "./child", 0777);
                                    break;
                                case 3:
                                    r = mkdir(raw, 0777);
                                    break;
                                case 4:
                                    r = mkdirat(dir, "child/", 0777);
                                    break;
                                case 5:
                                    r = mkdir(link, 0777);
                                    break;
                                case 6:
                                    r = mkdirat(dir, "exists-link", 0777);
                                    break;
                                default:
                                    r = mkdirat(dir, "missing-parent/child", 0777);
                                    break;
                            }
                            int e = errno;
                            bool ok = expected ? r == -1 && e == expected : r == 0;
                            if (!ok)
                                printf("operation uid=%u grouped=%u sgid=%u masked=%u method=%u result=%d errno=%d expected=%d\n", uid,
                                       grouped, sgid, masked, method, r, e, expected);
                            _exit(ok ? 0 : 1);
                        }
                        int status = 0;
                        bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                        ok = stat(moved, &after) == 0 && after.st_mode == before.st_mode && after.st_uid == before.st_uid &&
                             after.st_gid == before.st_gid && after.st_ino == before.st_ino && ok;
                        if (expected == 0) {
                            mode_t mode = (masked ? 0750 : 0777) | (sgid ? 02000 : 0);
                            ok = stat(target, &created) == 0 && S_ISDIR(created.st_mode) && created.st_uid == uid &&
                                 created.st_gid == (sgid ? 2002U : 2001U) && (created.st_mode & 07777) == mode && ok;
                        } else {
                            errno = 0;
                            ok = stat(target, &created) == -1 && errno == ENOENT && same_time(after.st_mtim, before.st_mtim) &&
                                 same_time(after.st_ctim, before.st_ctim) && after.st_nlink == before.st_nlink && ok;
                        }
                        errno = 0;
                        ok = stat(decoy, &other) == -1 && errno == ENOENT && ok;
                        if (expected == 0 && rmdir(target)) ok = false;
                        printf("%s uid=%u grouped=%u sgid=%u masked=%u method=%u expected=%d\n", ok ? "PASS" : "FAIL", uid, grouped, sgid,
                               masked, method, expected);
                        ++cases;
                        failed += !ok;
                    }
    close(dir);
    printf("%s mkdir checks=%u\n", failed ? "FAIL" : "PASS", cases);
    return failed ? 1 : 0;
}
int unsupported() {
    char name[512];
    path(name, "unsupported");
    struct stat before{}, after{}, child{};
    if (stat(base, &before)) return 3;
    errno = 0;
    int r = mkdir(name, 0777);
    int e = errno;
    bool ok = r == -1 && (e == ENOTCONN || e == ENOTSUP);
    errno = 0;
    ok = stat(name, &child) == -1 && errno == ENOENT && stat(base, &after) == 0 && same_time(before.st_mtim, after.st_mtim) &&
         same_time(before.st_ctim, after.st_ctim) && before.st_nlink == after.st_nlink && ok;
    printf("%s unsupported mkdir=%d errno=%d parent-unchanged=%d\n", ok ? "PASS" : "FAIL", r, e, ok);
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc < 3 || strlen(argv[2]) >= sizeof(base)) return 2;
    strcpy(base, argv[2]);
    if (strcmp(argv[1], "check") && strcmp(argv[1], "unsupported") &&
        ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
        return 3;
    if (!strcmp(argv[1], "setup")) return setup();
    if (!strcmp(argv[1], "mutate")) return mutate();
    if (!strcmp(argv[1], "cleanup")) return cleanup();
    if (!strcmp(argv[1], "check") && argc == 5) return check(argv[3], argv[4]);
    if (!strcmp(argv[1], "unsupported")) return unsupported();
    return 2;
}
