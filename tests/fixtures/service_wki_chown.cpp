// Functional authorization and retained-inode metadata checks on disposable WOS files.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
unsigned checks = 0, failures = 0;
void expect(bool ok, const char* label) {
    ++checks;
    if (!ok) {
        ++failures;
        printf("FAIL %s errno=%d\n", label, errno);
    }
}
bool error_is(int result, int error, const char* label) {
    if (result == -1 && errno == error) return true;
    printf("FAIL child %s result=%d errno=%d expected=%d\n", label, result, errno, error);
    return false;
}
bool unchanged(const struct stat& a, const struct stat& b) {
    return a.st_ino == b.st_ino && a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_gid == b.st_gid && a.st_size == b.st_size &&
           a.st_nlink == b.st_nlink && a.st_atim.tv_sec == b.st_atim.tv_sec && a.st_atim.tv_nsec == b.st_atim.tv_nsec &&
           a.st_mtim.tv_sec == b.st_mtim.tv_sec && a.st_mtim.tv_nsec == b.st_mtim.tv_nsec && a.st_ctim.tv_sec == b.st_ctim.tv_sec &&
           a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
}
bool create(const char* path, const char* data) {
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0640);
    if (fd < 0) return false;
    bool ok = write(fd, data, 4) == 4;
    return close(fd) == 0 && ok;
}
template <class Function>
bool as_user(uid_t uid, gid_t gid, bool supplementary, Function operation) {
    fflush(stdout);
    pid_t child = fork();
    if (child == 0) {
        gid_t group = 2002;
        using Control = ker::abi::process::CredentialControl;
        bool ready = setgroups(supplementary ? 1 : 0, supplementary ? &group : nullptr) == 0 && setgid(gid) == 0 && setuid(uid) == 0 &&
                     ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
        bool ok = ready && operation();
        fflush(stdout);
        _exit(ok ? 0 : 1);
    }
    int status = 0;
    return child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    const char* operation = argv[1];
    const char* base = argv[2];
    char anchor[512], moved[512], path[512], link[512], private_dir[512], denied_path[512];
    snprintf(anchor, sizeof(anchor), "%s/dir", base);
    snprintf(moved, sizeof(moved), "%s/moved", base);
    snprintf(path, sizeof(path), "%s/dir/file", base);
    snprintf(link, sizeof(link), "%s/dir/link", base);
    snprintf(private_dir, sizeof(private_dir), "%s/private", base);
    snprintf(denied_path, sizeof(denied_path), "%s/private/../dir/file", base);
    if (!strcmp(operation, "setup") || !strcmp(operation, "cleanup")) {
        if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 3;
        bool ok = true;
        if (!strcmp(operation, "setup")) {
            ok = mkdir(base, 0755) == 0 && mkdir(anchor, 0755) == 0 && mkdir(private_dir, 0710) == 0 && chown(private_dir, 0, 2000) == 0 &&
                 chmod(private_dir, 0710) == 0 && create(path, "seed") && symlink("file", link) == 0;
        } else {
            char name[512];
            for (auto suffix : {"dir/file", "dir/link", "moved/file", "moved/link"}) {
                snprintf(name, sizeof(name), "%s/%s", base, suffix);
                if (unlink(name) && errno != ENOENT) ok = false;
            }
            for (const char* dir :
                 {static_cast<const char*>(anchor), static_cast<const char*>(moved), static_cast<const char*>(private_dir), base})
                if (rmdir(dir) && errno != ENOENT) ok = false;
        }
        printf("%s %s\n", ok ? "PASS" : "FAIL", operation);
        return ok ? 0 : 1;
    }
    bool unsupported = !strcmp(operation, "unsupported");
    if (!unsupported && strcmp(operation, "check")) return 2;
    int directory = open(anchor, O_RDONLY | O_DIRECTORY), file = open(path, O_RDONLY);
    expect(directory >= 0 && file >= 0, "retain directory and original inode");
    if (directory < 0 || file < 0) return 1;
    if (unsupported) {
        struct stat before{}, after{};
        expect(fstat(file, &before) == 0, "old owner stat");
        expect(error_is(chown(path, 1001, 2002), EOPNOTSUPP, "old owner path"), "unsupported scalar chown");
        expect(error_is(fchownat(directory, "file", 1001, 2002, 0), EOPNOTSUPP, "old owner dirfd"), "unsupported retained-directory chown");
        expect(error_is(lchown(link, 1001, 2002), EOPNOTSUPP, "old owner symlink"), "unsupported nofollow");
        expect(fstat(file, &after) == 0 && unchanged(before, after), "unsupported chown leaves inode unchanged");
    } else {
        int replacement = -1;
        struct stat decoy_before{};
        for (unsigned stage = 0; stage < 2; ++stage) {
            if (stage) {
                expect(rename(anchor, moved) == 0, "rename retained parent");
                expect(mkdir(anchor, 0755) == 0 && create(path, "fake"), "reuse directory pathname");
                replacement = open(path, O_RDONLY);
                expect(replacement >= 0 && fstat(replacement, &decoy_before) == 0, "snapshot decoy inode");
            }
            expect(fchmod(file, 06755) == 0, "seed set-ID executable");
            expect((stage ? fchownat(directory, "file", 1001, 2002, 0) : chown(path, 1001, 2002)) == 0, "mapped root ownership");
            struct stat before{}, after{};
            expect(fstat(file, &before) == 0 && before.st_uid == 1001 && before.st_gid == 2002 && !(before.st_mode & 06000),
                   "fresh ownership and set-ID clearing");
            for (uid_t uid : {1003U, 0U}) {
                expect(as_user(uid, 3003, false,
                               [&] {
                                   return error_is(fchownat(directory, "file", 0, 0, 0), EPERM, "current nonowner") &&
                                          error_is(fchownat(directory, "file", -1, -1, 0), EPERM, "nonowner sentinels");
                               }),
                       "nonowner and zero-ceiling root denied");
                expect(fstat(file, &after) == 0 && unchanged(before, after), "denial preserves full metadata");
            }
            expect(as_user(1001, 2001, false,
                           [&] {
                               return error_is(fchownat(directory, "file", 1003, -1, 0), EPERM, "owner cannot assign UID") &&
                                      error_is(fchownat(directory, "file", -1, 2003, 0), EPERM, "owner foreign group") &&
                                      fchownat(directory, "file", -1, 2001, 0) == 0;
                           }),
                   "owner effective group and sentinel");
            expect(as_user(1001, 2001, true, [&] { return fchownat(directory, "file", -1, 2002, 0) == 0; }), "owner supplementary group");
            expect(fstat(file, &before) == 0 && before.st_uid == 1001 && before.st_gid == 2002, "owner group result");
            expect(fchownat(directory, "link", 1003, 3003, AT_SYMLINK_NOFOLLOW) == 0, "nofollow changes link only");
            expect(fstatat(directory, "link", &after, AT_SYMLINK_NOFOLLOW) == 0 && after.st_uid == 1003 && after.st_gid == 3003,
                   "symlink ownership result");
            expect(fstat(file, &after) == 0 && unchanged(before, after), "nofollow preserves target");
            expect(fchownat(directory, "link", 1001, 2001, 0) == 0, "follow changes target");
            expect(fstat(file, &before) == 0 && before.st_uid == 1001 && before.st_gid == 2001, "follow target result");
            expect(error_is(fchownat(directory, "file/", 0, 0, 0), ENOTDIR, "trailing slash"), "directory requirement");
            expect(error_is(fchownat(directory, "missing", 0, 0, 0), ENOENT, "missing file"), "missing target");
            expect(as_user(1001, 2001, false, [&] { return error_is(chown(denied_path, 1001, 2001), EACCES, "raw ancestor"); }),
                   "raw dotdot preserves ancestor search");
            expect(fstat(file, &after) == 0 && unchanged(before, after), "path denials preserve target");
            if (stage)
                expect(fstat(replacement, &after) == 0 && unchanged(decoy_before, after) && after.st_ino != before.st_ino,
                       "directory reference never selects decoy");
        }
        if (replacement >= 0) expect(close(replacement) == 0, "close decoy");
    }
    expect(error_is(fchownat(directory, "file", 0, 0, 0x40000000), EINVAL, "invalid flags"), "reject flags before effects");
    expect(close(file) == 0 && close(directory) == 0, "close references");
    printf("%s metadata checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures ? 1 : 0;
}
