// Functional removal permissions and directory descriptor checks in disposable WOS VMs.
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

#include <initializer_list>

namespace {
char base[512];
constexpr unsigned CASES = 512;
void path(char* out, const char* suffix) { snprintf(out, 512, "%s/%s", base, suffix); }
void object(char* out, const char* parent, unsigned index) { snprintf(out, 512, "%s/%s/c%u", base, parent, index); }
unsigned kind_for(unsigned index) { return (index / 8) % 4; }
bool make_object(const char* name, unsigned kind) {
    bool ok = false;
    if (kind == 1 || kind == 3)
        ok = mkdir(name, 0700) == 0;
    else if (kind == 2)
        ok = symlink("literal-not-followed", name) == 0;
    else {
        int fd = open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
        ok = fd >= 0 && write(fd, "kept", 4) == 4;
        if (fd >= 0) close(fd);
    }
    if (kind == 3) {
        char child[512];
        snprintf(child, sizeof(child), "%s/member", name);
        int fd = open(child, O_CREAT | O_EXCL | O_WRONLY, 0600);
        ok = fd >= 0 && ok;
        if (fd >= 0) close(fd);
    }
    return fchownat(AT_FDCWD, name, 1002, 2001, AT_SYMLINK_NOFOLLOW) == 0 && ok;
}
int setup() {
    char anchor[512], priv[512], link[512], name[512];
    path(anchor, "anchor");
    path(priv, "private");
    path(link, "link");
    bool ok = mkdir(base, 0755) == 0 && mkdir(anchor, 0770) == 0 && chown(anchor, 1001, 2002) == 0 && chmod(anchor, 0770) == 0 &&
              mkdir(priv, 0710) == 0 && chown(priv, 0, 2000) == 0 && chmod(priv, 0710) == 0 && symlink("moved", link) == 0;
    for (unsigned i = 0; i < CASES && ok; ++i) {
        object(name, "anchor", i);
        ok = make_object(name, kind_for(i));
        if (!ok) printf("setup object=%u kind=%u errno=%d\n", i, kind_for(i), errno);
    }
    printf("%s setup errno=%d\n", ok ? "PASS" : "FAIL", ok ? 0 : errno);
    return ok ? 0 : 1;
}
int mutate() {
    char anchor[512], moved[512], decoy[512];
    path(anchor, "anchor");
    path(moved, "moved");
    path(decoy, "anchor/decoy");
    bool ok = rename(anchor, moved) == 0 && mkdir(anchor, 0755) == 0 && make_object(decoy, 0);
    printf("%s mutate\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int cleanup() {
    bool ok = true;
    char name[512], child[512];
    for (auto parent : {"anchor", "moved"}) {
        for (unsigned i = 0; i < CASES; ++i) {
            object(name, parent, i);
            if (kind_for(i) == 3) {
                snprintf(child, sizeof(child), "%s/member", name);
                if (unlink(child) && errno != ENOENT) ok = false;
            }
            int r = kind_for(i) == 1 || kind_for(i) == 3 ? rmdir(name) : unlink(name);
            if (r && errno != ENOENT) ok = false;
        }
    }
    for (auto suffix : {"anchor/decoy", "link"}) {
        path(name, suffix);
        if (unlink(name) && errno != ENOENT) ok = false;
    }
    for (auto suffix : {"anchor", "moved", "private"}) {
        path(name, suffix);
        if (rmdir(name) && errno != ENOENT) ok = false;
    }
    if (rmdir(base) && errno != ENOENT) ok = false;
    printf("%s cleanup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
bool same_time(timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; }
int check(const char* ready, const char* gate) {
    char anchor[512], moved[512], target[512], raw[512], link[512], name[64], decoy[512];
    path(anchor, "anchor");
    path(moved, "moved");
    path(decoy, "anchor/decoy");
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
    struct stat decoy_before{};
    if (lstat(decoy, &decoy_before)) return 93;
    unsigned cases = 0, failed = 0;
    for (unsigned uid : {0U, 1001U, 1002U, 1003U})
        for (unsigned grouped = 0; grouped < 2; ++grouped)
            for (unsigned sticky = 0; sticky < 2; ++sticky)
                for (unsigned kind = 0; kind < 4; ++kind)
                    for (unsigned method = 0; method < 8; ++method) {
                        object(target, "moved", cases);
                        snprintf(raw, sizeof(raw), "%s/private/../moved/c%u", base, cases);
                        snprintf(link, sizeof(link), "%s/link/c%u", base, cases);
                        snprintf(name, sizeof(name),
                                 method == 2   ? "./c%u"
                                 : method == 4 ? "c%u/"
                                 : method == 7 ? "absent%u"
                                               : "c%u",
                                 cases);
                        if (chmod(moved, sticky ? 01770 : 0770)) return 94;
                        struct stat before{}, after{}, object_before{}, remaining{}, other{};
                        if (stat(moved, &before) || lstat(target, &object_before)) return 95;
                        bool search = uid == 0 || uid == 1001 || grouped;
                        if (method == 3 && uid != 0 && !grouped) search = false;
                        bool directory = kind == 1 || kind == 3;
                        int expected = !search                                            ? EACCES
                                       : method == 7                                      ? ENOENT
                                       : method == 4 && !directory                        ? ENOTDIR
                                       : method == 6                                      ? (directory ? EISDIR : ENOTDIR)
                                       : sticky && uid != 0 && uid != 1001 && uid != 1002 ? EPERM
                                       : kind == 3                                        ? ENOTEMPTY
                                                                                          : 0;
                        int retained = kind == 0 ? open(target, O_RDONLY) : -1;
                        if (kind == 0 && retained < 0) return 96;
                        pid_t child = fork();
                        if (child == 0) {
                            gid_t groups[] = {2000, 2002};
                            if (setgroups(grouped ? 2 : 0, grouped ? groups : nullptr) || setgid(2001) || setuid(uid)) _exit(97);
                            int flags = (method == 6 ? !directory : directory) ? AT_REMOVEDIR : 0;
                            errno = 0;
                            int r = method == 0   ? unlinkat(AT_FDCWD, target, flags)
                                    : method == 3 ? unlinkat(AT_FDCWD, raw, flags)
                                    : method == 5 ? unlinkat(AT_FDCWD, link, flags)
                                                  : unlinkat(dir, name, flags);
                            int e = errno;
                            bool ok = expected ? r == -1 && e == expected : r == 0;
                            if (!ok && failed < 16)
                                printf("operation uid=%u grouped=%u sticky=%u kind=%u method=%u result=%d errno=%d expected=%d\n", uid,
                                       grouped, sticky, kind, method, r, e, expected);
                            _exit(ok ? 0 : 1);
                        }
                        int status = 0;
                        bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
                        ok = stat(moved, &after) == 0 && after.st_ino == before.st_ino && after.st_mode == before.st_mode &&
                             after.st_uid == before.st_uid && after.st_gid == before.st_gid && ok;
                        if (expected == 0)
                            ok = lstat(target, &remaining) == -1 && errno == ENOENT && ok;
                        else
                            ok = lstat(target, &remaining) == 0 && remaining.st_ino == object_before.st_ino &&
                                 remaining.st_nlink == object_before.st_nlink && same_time(remaining.st_ctim, object_before.st_ctim) &&
                                 before.st_nlink == after.st_nlink && same_time(before.st_ctim, after.st_ctim) &&
                                 same_time(before.st_mtim, after.st_mtim) && ok;
                        if (retained >= 0) {
                            char data[4]{};
                            ok = read(retained, data, sizeof(data)) == 4 && memcmp(data, "kept", 4) == 0 && ok;
                            close(retained);
                        }
                        ok = lstat(decoy, &other) == 0 && other.st_ino == decoy_before.st_ino &&
                             same_time(other.st_ctim, decoy_before.st_ctim) && other.st_nlink == decoy_before.st_nlink && ok;
                        if (!ok) {
                            if (failed < 16)
                                printf("FAIL case=%u uid=%u group=%u sticky=%u kind=%u method=%u status=%d\n", cases, uid, grouped, sticky,
                                       kind, method, status);
                            ++failed;
                        }
                        ++cases;
                    }
    close(dir);
    printf("%s remove checks=%u failed=%u\n", failed ? "FAIL" : "PASS", cases, failed);
    return failed ? 1 : 0;
}
int unsupported() {
    char parent[512], name[512];
    path(parent, "anchor");
    struct stat before{}, after{}, object_before{}, remaining{};
    bool ok = stat(parent, &before) == 0;
    for (unsigned index : {0U, 8U}) {
        object(name, "anchor", index);
        ok = lstat(name, &object_before) == 0 && ok;
        errno = 0;
        int r = index == 0 ? unlink(name) : rmdir(name);
        int e = errno;
        bool valid = r == -1 && e == ENOTSUP && lstat(name, &remaining) == 0 && remaining.st_ino == object_before.st_ino &&
                     remaining.st_nlink == object_before.st_nlink && same_time(remaining.st_ctim, object_before.st_ctim) &&
                     stat(parent, &after) == 0 && after.st_nlink == before.st_nlink && same_time(after.st_ctim, before.st_ctim) &&
                     same_time(after.st_mtim, before.st_mtim);
        printf("%s unsupported directory=%d result=%d errno=%d unchanged=%d\n", valid ? "PASS" : "FAIL", index != 0, r, e, valid);
        ok = valid && ok;
    }
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc < 3 || strlen(argv[2]) >= sizeof(base)) return 2;
    strcpy(base, argv[2]);
    umask(0);
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (strcmp(argv[1], "check") && strcmp(argv[1], "unsupported") &&
        ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
        return 3;
    if (!strcmp(argv[1], "unsupported")) return unsupported();
    if (!strcmp(argv[1], "setup")) return setup();
    if (!strcmp(argv[1], "mutate")) return mutate();
    if (!strcmp(argv[1], "cleanup")) return cleanup();
    if (!strcmp(argv[1], "check") && argc >= 5) return check(argv[3], argv[4]);
    return 2;
}
