// Ordinary read-only client mount checks against a writable WOS owner export.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
int mount_proxies(const char* target, bool require_live) {
    FILE* diagnostics = fopen("/proc/wki/netdiag", "r");
    if (!diagnostics) return -1;
    char line[4096], suffix[512];
    snprintf(suffix, sizeof(suffix), " mount=%s\n", target);
    int count = 0;
    bool valid = true;
    while (fgets(line, sizeof(line), diagnostics)) {
        if (strstr(line, "wki_vfs_proxy_truncated")) valid = false;
        if (strncmp(line, "wki_vfs_proxy ", 14) || !strstr(line, suffix)) continue;
        ++count;
        if (require_live && (!strstr(line, " active=1 ") || !strstr(line, " destroy_when_idle=0 ") || !strstr(line, " mount_released=0 ")))
            valid = false;
    }
    if (ferror(diagnostics)) valid = false;
    fclose(diagnostics);
    return valid ? count : -1;
}

bool wait_mount_retired(const char* target) {
    // Owner detach acknowledgements are deferred. Require complete registry
    // retirement, with a bounded wait, before reusing the same mount path.
    for (unsigned attempt = 0; attempt < 50; ++attempt) {
        int count = mount_proxies(target, false);
        if (count <= 0) return count == 0;
        usleep(100000);
    }
    return false;
}

bool same(const struct stat& a, const struct stat& b) {
    return a.st_ino == b.st_ino && a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_gid == b.st_gid && a.st_nlink == b.st_nlink &&
           a.st_size == b.st_size && a.st_atim.tv_sec == b.st_atim.tv_sec && a.st_atim.tv_nsec == b.st_atim.tv_nsec &&
           a.st_mtim.tv_sec == b.st_mtim.tv_sec && a.st_mtim.tv_nsec == b.st_mtim.tv_nsec && a.st_ctim.tv_sec == b.st_ctim.tv_sec &&
           a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
}
int profile(unsigned mode, const char* path, const char* missing, int directory, int file) {
    if (setgroups(0, nullptr)) return 2;
    if (mode == 1 && (ker::process::prctl(38, 1, 0, 0, 0) || setgid(104) || setuid(104))) return 2;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 2;
    uid_t r, e, s;
    gid_t gr, ge, gs;
    if (getresuid(&r, &e, &s) || getresgid(&gr, &ge, &gs)) return 2;
    auto ceiling = ker::process::prctl(0x574f5301, 0, 0, 0, 0);
    auto grants = ker::process::prctl(0x574f5304, 0, 0, 0, 0);
    auto nnp = ker::process::prctl(39, 0, 0, 0, 0);
    unsigned checks = 0, failed = 0;
    auto check = [&](bool okay, const char* label) {
        ++checks;
        failed += !okay;
        printf("%s profile=%u %s errno=%d\n", okay ? "PASS" : "FAIL", mode, label, errno);
    };
    auto denied = [&](int result, int error, const char* label) { check(result == -1 && errno == error, label); };
    // Exercise inherited grants after replacing a directory FD with a file
    // alias and closing the originals. The parent retains independent handles
    // for the writable-owner control and mount retirement checks.
    struct stat original{};
    if (fstat(file, &original)) return 2;
    int const alias = dup(file), directory_alias = dup(directory), replacement = dup(directory);
    if (alias < 0 || directory_alias < 0 || replacement < 0 || dup2(alias, replacement) != replacement || close(alias) || close(file) ||
        close(directory))
        return 2;
    file = replacement;
    directory = directory_alias;
    struct stat copied{};
    struct statvfs copied_policy{};
    char contents[4]{};
    check(!fstat(file, &copied) && copied.st_dev == original.st_dev && copied.st_ino == original.st_ino &&
              !fstatvfs(file, &copied_policy) && ((copied_policy.f_flag & (ST_RDONLY | ST_NOSUID)) == (ST_RDONLY | ST_NOSUID)) &&
              pread(file, contents, sizeof(contents), 0) == 4 && !memcmp(contents, "seed", 4),
          "dup2 File retains identity, readonly/nosuid policy and content after originals close");
    check(!fstatvfs(directory, &copied_policy) && ((copied_policy.f_flag & (ST_RDONLY | ST_NOSUID)) == (ST_RDONLY | ST_NOSUID)),
          "dup directory retains readonly/nosuid policy");
    const timespec times[2]{{123, 456}, {789, 123}}, omit[2]{{0, UTIME_OMIT}, {0, UTIME_OMIT}}, invalid[2]{{0, -1}, {0, 0}};
    struct stat before{}, after{};
    if (fstat(file, &before)) return 2;
    denied(chmod(path, 0777), EROFS, "path chmod");
    denied(chown(path, 104, 104), EROFS, "path chown");
    denied(utimensat(AT_FDCWD, path, times, 0), EROFS, "path timestamps");
    denied(fchmodat(directory, "original", 0777, 0), EROFS, "dirfd chmod");
    denied(fchownat(directory, "original", 104, 104, 0), EROFS, "dirfd chown");
    denied(utimensat(directory, "original", times, 0), EROFS, "dirfd timestamps");
    denied(utimensat(directory, "original", nullptr, 0), EROFS, "dirfd NOW");
    denied(utimensat(directory, "original", invalid, 0), EINVAL, "timestamp validation");
    check(!utimensat(AT_FDCWD, path, omit, 0), "path OMIT owner validation");
    check(!utimensat(directory, "original", omit, 0), "dirfd OMIT owner validation");
    denied(mkdir(missing, 0700), EROFS, "path mkdir");
    denied(mkdirat(directory, "missing", 0700), EROFS, "dirfd mkdir");
    denied(symlink("original", missing), EROFS, "path symlink");
    denied(symlinkat("original", directory, "missing"), EROFS, "dirfd symlink");
    denied(unlink(path), EROFS, "path unlink");
    denied(unlinkat(directory, "original", 0), EROFS, "dirfd unlink");
    denied(unlinkat(directory, "empty", AT_REMOVEDIR), EROFS, "dirfd rmdir");
    denied(rename(path, missing), EROFS, "path rename");
    denied(renameat(directory, "original", directory, "missing"), EROFS, "dirfd rename");
    denied(rename(path, path), EROFS, "same-name readonly rename");
    denied(fchmod(file, 0777), EROFS, "File chmod");
    denied(fchown(file, 104, 104), EROFS, "File chown");
    denied(futimens(file, times), EROFS, "File timestamps");
    check(!futimens(file, omit), "File OMIT owner validation");
    denied(chmod(missing, 0777), EROFS, "client policy before owner existence");
    namespace abi = ker::abi::vfs;
    for (auto operation :
         {abi::metadata_batch_operation::CREATE_CLOSE, abi::metadata_batch_operation::UNLINK, abi::metadata_batch_operation::RENAME}) {
        bool rename_op = operation == abi::metadata_batch_operation::RENAME;
        abi::metadata_batch_entry entries[2]{{path, rename_op ? missing : nullptr}, {missing, rename_op ? path : nullptr}};
        abi::metadata_batch_result results[2]{};
        abi::metadata_batch_header header{.version = abi::METADATA_BATCH_VERSION, .operation = operation, .count = 2, .mode = 0};
        check(abi::metadata_batch(&header, entries, results) == -EROFS && results[0].status == -EROFS && results[1].status == -EROFS,
              "whole mutation batch refused");
    }
    struct statvfs policy{};
    check(!fstatvfs(file, &policy) && (policy.f_flag & ST_RDONLY), "File readonly mount flag");
    check(!fstat(file, &after) && same(before, after), "owner metadata unchanged");
    uid_t ar, ae, as;
    gid_t agr, age, ags;
    check(!getresuid(&ar, &ae, &as) && !getresgid(&agr, &age, &ags) && ar == r && ae == e && as == s && agr == gr && age == ge &&
              ags == gs && getgroups(0, nullptr) == 0 && ceiling == ker::process::prctl(0x574f5301, 0, 0, 0, 0) &&
              grants == ker::process::prctl(0x574f5304, 0, 0, 0, 0) && nnp == ker::process::prctl(39, 0, 0, 0, 0),
          "complete credentials unchanged");
    printf("PROFILE %u checks=%u failed=%u\n", mode, checks, failed);
    fflush(stdout);
    return failed != 0;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc != 2 || geteuid()) return 2;
    char source[256], target[256], base[256], rw[512], ro[512], missing[512], owner_file[512], owner_empty[512], directory_path[512];
    snprintf(source, sizeof(source), "wki://%s//tmp", argv[1]);
    snprintf(target, sizeof(target), "/tmp/goal19-ro-view-%d", getpid());
    snprintf(base, sizeof(base), "goal19-ro-owner-%d", getpid());
    snprintf(rw, sizeof(rw), "/wki/%s/tmp/%s", argv[1], base);
    snprintf(owner_file, sizeof(owner_file), "%s/original", rw);
    snprintf(owner_empty, sizeof(owner_empty), "%s/empty", rw);
    snprintf(directory_path, sizeof(directory_path), "%s/%s", target, base);
    snprintf(ro, sizeof(ro), "%s/original", directory_path);
    snprintf(missing, sizeof(missing), "%s/missing", directory_path);
    int result = 0, directory = -1, file = -1, writer = -1;
    bool mounted = false, owner_created = false, target_created = false;
    auto setup = [&] {
        if (mkdir(rw, 0755)) return false;
        owner_created = true;
        writer = open(owner_file, O_CREAT | O_EXCL | O_RDWR, 0644);
        // The new read check uses a separate description and mount. Publish
        // buffered remote writes before opening that independent reader.
        if (writer < 0 || write(writer, "seed", 4) != 4 || fsync(writer) || mkdir(owner_empty, 0755)) return false;
        if (mkdir(target, 0755)) return false;
        target_created = true;
        if (mount(source, target, "remote", MS_RDONLY | MS_NOSUID, nullptr)) return false;
        mounted = true;
        directory = open(directory_path, O_RDONLY | O_DIRECTORY);
        file = open(ro, O_RDONLY);
        return directory >= 0 && file >= 0;
    };
    if (!setup()) {
        printf("FAIL setup errno=%d\n", errno);
        result = 2;
    }
    if (!result) {
        int const proxies_before = mount_proxies(target, true);
        int const unmount_result = umount(target);
        int const unmount_error = errno;
        bool const busy = unmount_result == -1 && unmount_error == EBUSY;
        if (unmount_result == 0) mounted = false;
        char contents[4]{};
        int const proxies_after = mount_proxies(target, true);
        auto const read_result = pread(file, contents, sizeof(contents), 0);
        bool const preserved =
            busy && proxies_before > 0 && proxies_after == proxies_before && read_result == 4 && !memcmp(contents, "seed", 4);
        printf("%s busy unmount preserves live proxy group and readable File\n", preserved ? "PASS" : "FAIL");
        printf("BUSY result=%d errno=%d proxies_before=%d proxies_after=%d read=%ld\n", unmount_result, unmount_error, proxies_before,
               proxies_after, static_cast<long>(read_result));
        if (!preserved) result = 1;
        for (unsigned mode = 0; mode < 3; ++mode) {
            fflush(stdout);
            pid_t child = fork();
            if (!child) _exit(profile(mode, ro, missing, directory, file));
            int status = 0;
            if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) result = 1;
        }
        // The owner's ordinary writable view retains authority after all denials.
        if (fchmod(writer, 0600) || fchmod(writer, 0644)) {
            puts("FAIL writable owner control");
            result = 1;
        } else
            puts("PASS writable owner control");
        struct stat absent{};
        char owner_missing[512];
        snprintf(owner_missing, sizeof(owner_missing), "%s/missing", rw);
        if (lstat(owner_missing, &absent) != -1 || errno != ENOENT) {
            puts("FAIL owner namespace changed");
            result = 1;
        }
    }
    if (file >= 0 && close(file)) result = 1;
    if (directory >= 0 && close(directory)) result = 1;
    if (mounted) {
        if (umount(target))
            result = 1;
        else
            mounted = false;
        bool const retired = !mounted && wait_mount_retired(target);
        printf("%s successful manual unmount retires every proxy lane\n", retired ? "PASS" : "FAIL");
        if (!retired) result = 1;
    }
    // Reuse the pathname with newly allocated groups and verify retirement on
    // each cycle. This catches a stale active group left by manual unmount.
    for (unsigned cycle = 0; cycle < 3 && !result; ++cycle) {
        if (mount(source, target, "remote", MS_RDONLY | MS_NOSUID, nullptr)) {
            result = 1;
            break;
        }
        mounted = true;
        bool const live = mount_proxies(target, true) > 0;
        if (umount(target))
            result = 1;
        else
            mounted = false;
        bool const retired = live && !mounted && wait_mount_retired(target);
        printf("%s replacement mount cycle=%u retires exact proxy group\n", retired ? "PASS" : "FAIL", cycle);
        if (!retired) result = 1;
    }
    if (mounted && umount(target)) result = 1;
    if (target_created && rmdir(target)) result = 1;
    if (writer >= 0 && close(writer)) result = 1;
    if (owner_created) {
        if (unlink(owner_file) && errno != ENOENT) result = 1;
        if (rmdir(owner_empty) && errno != ENOENT) result = 1;
        if (rmdir(rw)) result = 1;
    }
    printf("%s remote readonly fixture; cleanup completed\n", result ? "FAIL" : "PASS");
    return result;
}
