#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
bool contents(int fd, const char* expected) {
    char bytes[16]{};
    struct stat st{};
    size_t n = strlen(expected);
    return fd >= 0 && fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size == static_cast<off_t>(n) && lseek(fd, 0, SEEK_SET) == 0 &&
           read(fd, bytes, sizeof(bytes)) == static_cast<ssize_t>(n) && memcmp(bytes, expected, n) == 0;
}
bool path_contents(const char* path, const char* expected) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;
    bool ok = contents(fd, expected);
    return close(fd) == 0 && ok;
}
bool absent(const char* path) {
    struct stat st{};
    return lstat(path, &st) == -1 && errno == ENOENT;
}
bool provision(const char* base, const char* action, bool off) {
    char hidden[512], anchor[512], renamed[512], private_path[512], public_path[512], link[512], public_link[512], work[512];
    snprintf(hidden, sizeof(hidden), "%s/hidden", base);
    snprintf(anchor, sizeof(anchor), "%s/anchor", hidden);
    snprintf(renamed, sizeof(renamed), "%s/renamed", hidden);
    if (strcmp(action, "mutate") == 0) return rename(anchor, renamed) == 0 && chmod(hidden, 0700) == 0;
    bool remove = strcmp(action, "cleanup") == 0, verify = strcmp(action, "verify") == 0;
    if (remove || verify) {
        struct stat st{};
        if (stat(renamed, &st) == 0) memcpy(anchor, renamed, strlen(renamed) + 1);
    }
    snprintf(private_path, sizeof(private_path), "%s/private", anchor);
    snprintf(public_path, sizeof(public_path), "%s/public", anchor);
    snprintf(link, sizeof(link), "%s/link", anchor);
    snprintf(public_link, sizeof(public_link), "%s/public-link", anchor);
    snprintf(work, sizeof(work), "%s/work", anchor);
    if (remove || verify) {
        bool ok = !verify || path_contents(public_path, "public");
        char path[512];
        for (unsigned phase = 0; phase < 2; ++phase)
            for (unsigned group = 0; group < 2; ++group) {
                for (const char* name : {"new", "zero", "limit"}) {
                    snprintf(path, sizeof(path), "%s/%s-%u-%u", work, name, phase, group);
                    if (remove)
                        ok &= unlink(path) == 0 || errno == ENOENT;
                    else if (off || strcmp(name, "limit") == 0)
                        ok &= absent(path);
                    else {
                        ok &= path_contents(path, strcmp(name, "new") == 0 ? "again" : "secret");
                        struct stat st{};
                        ok &= stat(path, &st) == 0 && st.st_uid == 1000 && st.st_gid == 2001 &&
                              (st.st_mode & 0777) == (strcmp(name, "new") == 0 ? 0600 : 0);
                    }
                }
            }
        for (const char* name : {"absent", "private/missing"}) {
            snprintf(path, sizeof(path), "%s/%s", anchor, name);
            ok &= remove ? (unlink(path) == 0 || errno == ENOENT) : absent(path);
        }
        if (verify) return ok;
        ok &= rmdir(work) == 0 || errno == ENOENT;
        ok &= unlink(link) == 0 || errno == ENOENT;
        ok &= unlink(public_link) == 0 || errno == ENOENT;
        ok &= unlink(public_path) == 0 || errno == ENOENT;
        ok &= rmdir(private_path) == 0 || errno == ENOENT;
        ok &= rmdir(anchor) == 0 || errno == ENOENT;
        ok &= rmdir(hidden) == 0 || errno == ENOENT;
        ok &= rmdir(base) == 0 || errno == ENOENT;
        return ok;
    }
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(hidden, 0755) || chmod(hidden, 0755) || mkdir(anchor, 0755) ||
        chmod(anchor, 0755) || mkdir(private_path, 0710) || chown(private_path, 0, 2000) || chmod(private_path, 0710) ||
        mkdir(work, 0777) || chmod(work, 0777))
        return false;
    int fd = open(public_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) return false;
    bool ok = write(fd, "public", 6) == 6;
    ok &= close(fd) == 0;
    return ok && chmod(public_path, 0644) == 0 && symlink("private/../public", link) == 0 && symlink("public", public_link) == 0;
}
int next_fd() {
    int fd = open("/dev/null", O_RDONLY);
    return fd >= 0 && close(fd) == 0 ? fd : -1;
}

bool check(int anchor, int observer, bool grouped, bool off, unsigned phase) {
    struct Case {
        const char* path;
        int flags;
        int ungrouped;
        int grouped;
        bool directory = false;
    };
    const Case cases[] = {
        {"public", O_RDONLY, 0, 0},
        {"private/../public", O_RDONLY, EACCES, 0},
        {"private/missing", O_RDONLY, EACCES, ENOENT},
        {"link", O_RDONLY, EACCES, 0},
        {"public-link", O_RDONLY, 0, 0},
        {"public", O_WRONLY, EACCES, EACCES},
        {"public", O_RDWR, EACCES, EACCES},
        {"public", O_RDONLY | O_TRUNC, EACCES, EACCES},
        {"public", O_WRONLY | O_TRUNC, EACCES, EACCES},
        {"private/../public", O_RDONLY | O_TRUNC, EACCES, EACCES},
        {"public", O_CREAT | O_EXCL | O_WRONLY, EEXIST, EEXIST},
        {"private/../public", O_CREAT | O_EXCL | O_WRONLY, EACCES, EEXIST},
        {"public-link", O_NOFOLLOW | O_RDONLY, ELOOP, ELOOP},
        {"link", O_NOFOLLOW | O_RDONLY, ELOOP, ELOOP},
        {"private/../public-link", O_NOFOLLOW | O_RDONLY, EACCES, ELOOP},
        {".", O_DIRECTORY | O_RDONLY, 0, 0, true},
        {"private", O_DIRECTORY | O_RDONLY, EACCES, EACCES},
        {"public", O_DIRECTORY, ENOTDIR, ENOTDIR},
        {"public/", O_RDONLY, ENOTDIR, ENOTDIR},
        {"private/../public/", O_RDONLY, EACCES, ENOTDIR},
        {"absent", O_RDONLY, ENOENT, ENOENT},
        {"private/missing", O_CREAT | O_WRONLY, EACCES, EACCES},
        {"absent", O_CREAT | O_WRONLY, EACCES, EACCES},
        {"public-link", O_CREAT | O_EXCL | O_WRONLY, EEXIST, EEXIST},
        {"link", O_CREAT | O_EXCL | O_WRONLY, EEXIST, EEXIST},
        {"private/../absent", O_CREAT | O_WRONLY, EACCES, EACCES},
        {"public", O_RDONLY | O_CLOEXEC, 0, 0},
        {"public", O_NOFOLLOW | O_RDONLY, 0, 0},
        {"private/../.", O_DIRECTORY | O_RDONLY, EACCES, 0, true},
        {"public-link/", O_RDONLY, ENOTDIR, ENOTDIR},
        {"public", 3, EINVAL, EINVAL},
        {"absent", O_CREAT | O_DIRECTORY, EINVAL, EINVAL},
    };
    unsigned checks = 0, passed = 0;
    auto expect = [&](bool ok, const char* name) {
        checks++;
        passed += ok;
        if (!ok) printf("FAIL phase=%u grouped=%d check=%s errno=%d\n", phase, grouped, name, errno);
    };
    int const BASE_FD = next_fd();
    for (const auto& item : cases) {
        int expected = grouped ? item.grouped : item.ungrouped;
        if (off && expected != EINVAL) expected = ENOTSUP;
        errno = 0;
        int fd = openat(anchor, item.path, item.flags, 0600), error = errno;
        bool ok = expected ? fd == -1 && error == expected : fd >= 0;
        if (fd >= 0) {
            if (!expected) {
                ok &= (fcntl(fd, F_GETFD) & FD_CLOEXEC) == ((item.flags & O_CLOEXEC) ? FD_CLOEXEC : 0);
                if (item.directory) {
                    struct stat st{};
                    ok &= fstat(fd, &st) == 0 && S_ISDIR(st.st_mode);
                    int nested = openat(fd, "public", O_RDONLY);
                    ok &= contents(nested, "public");
                    if (nested >= 0) ok &= close(nested) == 0;
                } else
                    ok &= contents(fd, "public");
            }
            ok &= close(fd) == 0;
        }
        ok &= BASE_FD >= 0 && next_fd() == BASE_FD;
        if (!ok) printf("flags=%x actual=%d errno=%d expected=%d\n", item.flags, fd, error, expected);
        expect(ok, item.path);
    }
    char created[128], zero[128], limit[128];
    snprintf(created, sizeof(created), "work/new-%u-%u", phase, grouped);
    snprintf(zero, sizeof(zero), "work/zero-%u-%u", phase, grouped);
    snprintf(limit, sizeof(limit), "work/limit-%u-%u", phase, grouped);
    auto mutation = [&](const char* path, int flags, int mode, const char* value, bool truncate) {
        errno = 0;
        int fd = openat(anchor, path, flags, mode), error = errno;
        bool ok = off ? fd == -1 && error == ENOTSUP : fd >= 0;
        if (fd >= 0) {
            if (!off) {
                struct stat st{};
                ok &= fstat(fd, &st) == 0 && st.st_uid == 1000 && st.st_gid == 2001;
                if (truncate) ok &= st.st_size == 0;
                if (value) ok &= write(fd, value, strlen(value)) == static_cast<ssize_t>(strlen(value));
            }
            ok &= close(fd) == 0;
        }
        expect(ok && next_fd() == BASE_FD, path);
    };
    mutation(created, O_CREAT | O_EXCL | O_RDWR, 0600, "changed", false);
    mutation(created, O_RDONLY | O_TRUNC, 0, nullptr, true);
    mutation(created, O_WRONLY, 0, "again", false);
    mutation(zero, O_CREAT | O_EXCL | O_WRONLY, 0, "secret", false);
    errno = 0;
    int denied = openat(anchor, zero, O_RDONLY);
    bool zero_denied = denied == -1 && errno == (off ? ENOTSUP : EACCES);
    if (denied >= 0) close(denied);
    expect(zero_denied && next_fd() == BASE_FD, "mode-zero reopen");
    int witness = off ? dup(observer) : openat(anchor, created, O_RDONLY);
    expect(witness >= 0, "capacity witness");
    struct rlimit old{}, reduced{};
    bool got_limit = getrlimit(RLIMIT_NOFILE, &old) == 0;
    reduced = old;
    int available = next_fd();
    reduced.rlim_cur = available >= 0 ? static_cast<rlim_t>(available) : 0;
    bool limited = got_limit && available >= 0 && setrlimit(RLIMIT_NOFILE, &reduced) == 0;
    expect(limited, "set FD limit");
    for (bool truncation : {false, true}) {
        errno = 0;
        int fd = limited ? openat(anchor, truncation ? (off ? "public" : created) : limit,
                                  truncation ? O_RDONLY | O_TRUNC : O_CREAT | O_WRONLY, 0600)
                         : -1;
        bool ok = limited && fd == -1 && errno == EMFILE;
        if (fd >= 0) close(fd);
        expect(ok, truncation ? "FD limit before truncate" : "FD limit before create");
    }
    expect(got_limit && setrlimit(RLIMIT_NOFILE, &old) == 0, "restore FD limit");
    expect(contents(witness, off ? "public" : "again"), "FD limit preserves content");
    if (witness >= 0) close(witness);
    expect(next_fd() == BASE_FD, "no leaked FD slots");
    expect(contents(observer, "public"), "denied truncations preserve public data");
    printf("profile phase=%u grouped=%d passed=%u total=%u\n", phase, grouped, passed, checks);
    return passed == checks;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc < 3 || geteuid() != 0) return 2;
    if (strcmp(argv[1], "check")) {
        if (argc != 4 || (strcmp(argv[3], "on") && strcmp(argv[3], "off")) ||
            ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
            return 2;
        bool ok = provision(argv[2], argv[1], strcmp(argv[3], "off") == 0);
        printf("%s %s\n", ok ? "PASS" : "FAIL", argv[1]);
        return ok ? 0 : 1;
    }
    if (argc != 6 || (strcmp(argv[5], "on") && strcmp(argv[5], "off"))) return 2;
    int fd = open(argv[2], O_RDONLY | O_DIRECTORY);
    char path[512];
    snprintf(path, sizeof(path), "%s/public", argv[2]);
    int observer = open(path, O_RDONLY);
    if (fd < 0 || observer < 0) {
        printf("FAIL initial grants errno=%d\n", errno);
        return 2;
    }
    unsigned successful = 0;
    for (unsigned phase = 0; phase < 2; ++phase) {
        if (phase == 1) {
            int ready = open(argv[3], O_CREAT | O_EXCL | O_WRONLY, 0600);
            if (ready < 0 || close(ready)) {
                close(observer);
                close(fd);
                return 2;
            }
            unsigned attempts = 0;
            while (access(argv[4], F_OK) != 0 && attempts++ < 600) usleep(100000);
            if (attempts >= 600) {
                close(observer);
                close(fd);
                return 2;
            }
        }
        for (bool grouped : {false, true}) {
            pid_t child = fork();
            if (child == 0) {
                gid_t group = 2000;
                if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1000)) _exit(2);
                using Control = ker::abi::process::CredentialControl;
                if (ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
                    ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
                    _exit(2);
                bool ok = check(fd, observer, grouped, strcmp(argv[5], "off") == 0, phase);
                fflush(stdout);
                _exit(ok ? 0 : 1);
            }
            int status = 0;
            successful += child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        }
    }
    bool ok = close(observer) == 0;
    ok &= close(fd) == 0 && successful == 4;
    printf("%s remote OPEN_AT profiles=4\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
