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
    const char* mode = argv[1];
    const char* base = argv[2];
    char path[512], moved[512];
    snprintf(path, sizeof(path), "%s/original", base);
    snprintf(moved, sizeof(moved), "%s/moved", base);
    if (!strcmp(mode, "setup") || !strcmp(mode, "cleanup")) {
        if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 3;
        bool ok = true;
        if (!strcmp(mode, "setup"))
            ok = mkdir(base, 0755) == 0 && create(path, "seed");
        else {
            for (auto p : {path, moved})
                if (unlink(p) && errno != ENOENT) ok = false;
            if (rmdir(base) && errno != ENOENT) ok = false;
        }
        printf("%s %s\n", ok ? "PASS" : "FAIL", mode);
        return ok ? 0 : 1;
    }
    bool unsupported = !strcmp(mode, "unsupported");
    if (!unsupported && strcmp(mode, "check")) return 2;
    int fd = open(path, O_RDONLY);
    expect(fd >= 0, "open retained read-only file");
    if (fd < 0) return 1;
    const timespec explicit_times[2]{{123, 456}, {789, 123}};
    const timespec omit[2]{{0, UTIME_OMIT}, {0, UTIME_OMIT}};
    const timespec mixed[2]{{0, UTIME_NOW}, {0, UTIME_OMIT}};
    const timespec invalid[2]{{123, 1000000000}, {789, 123}};
    if (unsupported) {
        struct stat before{}, after{};
        expect(fstat(fd, &before) == 0, "old owner stat");
        expect(error_is(fchmod(fd, 0777), EOPNOTSUPP, "old owner chmod"), "unsupported chmod");
        expect(error_is(fchown(fd, 1001, 2002), EOPNOTSUPP, "old owner chown"), "unsupported chown");
        expect(error_is(futimens(fd, explicit_times), EOPNOTSUPP, "old owner times"), "unsupported times");
        expect(error_is(futimens(fd, invalid), EINVAL, "invalid times"), "input validation before feature query");
        expect(fstat(fd, &after) == 0 && unchanged(before, after), "unsupported operations preserve metadata");
        char bytes[4]{};
        expect(pread(fd, bytes, 4, 0) == 4 && !memcmp(bytes, "seed", 4), "unsupported operations preserve content grant");
    } else {
        int replacement = -1;
        struct stat replacement_before{};
        for (unsigned stage = 0; stage < 3; ++stage) {
            if (stage == 1) {
                expect(rename(path, moved) == 0, "rename retained inode");
                expect(create(path, "fake"), "reuse original pathname");
                replacement = open(path, O_RDONLY);
                expect(replacement >= 0 && fstat(replacement, &replacement_before) == 0, "retain replacement inode");
            }
            if (stage == 2) expect(unlink(moved) == 0, "unlink retained inode");
            expect(fchown(fd, 1001, 2002) == 0, "mapped root sets owner");
            expect(fchmod(fd, 0640) == 0, "mapped root sets mode");
            expect(futimens(fd, explicit_times) == 0, "mapped root sets explicit times");
            struct stat before{}, after{};
            expect(fstat(fd, &before) == 0, "fresh descriptor metadata");
            expect(before.st_uid == 1001 && before.st_gid == 2002 && (before.st_mode & 07777) == 0640 && before.st_atim.tv_sec == 123 &&
                       before.st_mtim.tv_nsec == 123,
                   "successful mutations invalidate cached stat");
            for (uid_t uid : {1003U, 0U}) {
                expect(as_user(uid, 3003, false,
                               [&] {
                                   bool ok = error_is(fchmod(fd, 0777), EPERM, "nonowner chmod");
                                   ok = error_is(fchown(fd, 0, 0), EPERM, "nonowner chown") && ok;
                                   ok = error_is(futimens(fd, explicit_times), EPERM, "nonowner explicit times") && ok;
                                   ok = error_is(futimens(fd, nullptr), EACCES, "nonowner now") && ok;
                                   ok = error_is(futimens(fd, mixed), EPERM, "nonowner mixed times") && ok;
                                   ok = futimens(fd, omit) == 0 && ok;
                                   return ok;
                               }),
                       "current nonowner or restricted root is denied");
                expect(fstat(fd, &after) == 0 && unchanged(before, after), "denials and OMIT preserve all metadata");
            }
            expect(error_is(futimens(fd, invalid), EINVAL, "malformed timestamp"), "invalid nanoseconds");
            expect(fstat(fd, &after) == 0 && unchanged(before, after), "malformed times preserve metadata");
            expect(as_user(1001, 2001, false, [&] { return fchmod(fd, 02750) == 0; }), "owner mode on read-only descriptor");
            expect(fstat(fd, &after) == 0 && (after.st_mode & 07777) == 0750, "unheld group clears setgid");
            expect(as_user(1001, 2001, true, [&] { return fchmod(fd, 02750) == 0; }), "supplementary group mode");
            expect(fstat(fd, &after) == 0 && (after.st_mode & 07777) == 02750, "held group preserves setgid");
            expect(as_user(1001, 2001, false,
                           [&] { return error_is(fchown(fd, -1, 2003), EPERM, "foreign group") && fchown(fd, -1, 2001) == 0; }),
                   "owner group transition policy");
            expect(as_user(1001, 2001, true, [&] { return fchown(fd, -1, 2002) == 0; }), "supplementary group chown");
            expect(fstat(fd, &after) == 0 && after.st_uid == 1001 && after.st_gid == 2002 && (after.st_mode & 06000) == 0,
                   "chown sentinels and set-ID clearing");
            expect(as_user(1001, 2001, false, [&] { return fchmod(fd, 0000) == 0 && futimens(fd, explicit_times) == 0; }),
                   "owner timestamps need no data permission");
            expect(fstat(fd, &after) == 0 && after.st_atim.tv_sec == 123 && after.st_atim.tv_nsec == 456 && after.st_mtim.tv_sec == 789 &&
                       after.st_mtim.tv_nsec == 123,
                   "owner explicit nanoseconds preserved");
            expect(fchmod(fd, 0666) == 0, "allow ordinary write policy");
            expect(as_user(1003, 3003, false,
                           [&] {
                               bool ok = futimens(fd, nullptr) == 0;
                               ok = error_is(futimens(fd, explicit_times), EPERM, "write grant cannot set explicit times") && ok;
                               ok = error_is(futimens(fd, mixed), EPERM, "write grant cannot set mixed times") && ok;
                               return ok;
                           }),
                   "NOW differs from explicit or mixed intent");
            if (stage != 0)
                expect(fstat(replacement, &after) == 0 && unchanged(replacement_before, after) && after.st_ino != before.st_ino,
                       "retained mutations never select reused pathname");
        }
        if (replacement >= 0) expect(close(replacement) == 0, "close replacement");
        char bytes[4]{};
        expect(pread(fd, bytes, 4, 0) == 4 && !memcmp(bytes, "seed", 4), "unlinked descriptor retains data grant");
    }
    expect(close(fd) == 0, "close retained file");
    expect(error_is(fchmod(-1, 0777), EBADF, "bad local fd"), "invalid local descriptor");
    printf("%s metadata checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures ? 1 : 0;
}
