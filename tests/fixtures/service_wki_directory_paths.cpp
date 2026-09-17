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
bool provision(const char* base, const char* action) {
    char hidden[512], anchor[512], renamed[512], private_path[512], public_path[512], link[512], public_link[512];
    snprintf(hidden, sizeof(hidden), "%s/hidden", base);
    snprintf(anchor, sizeof(anchor), "%s/anchor", hidden);
    snprintf(renamed, sizeof(renamed), "%s/renamed", hidden);
    if (strcmp(action, "mutate") == 0) return rename(anchor, renamed) == 0 && chmod(hidden, 0700) == 0;
    bool remove = strcmp(action, "cleanup") == 0;
    if (remove) {
        struct stat st{};
        if (stat(renamed, &st) == 0) memcpy(anchor, renamed, strlen(renamed) + 1);
    }
    snprintf(private_path, sizeof(private_path), "%s/private", anchor);
    snprintf(public_path, sizeof(public_path), "%s/public", anchor);
    snprintf(link, sizeof(link), "%s/link", anchor);
    snprintf(public_link, sizeof(public_link), "%s/public-link", anchor);
    if (remove) {
        bool ok = unlink(link) == 0 || errno == ENOENT;
        ok &= unlink(public_link) == 0 || errno == ENOENT;
        ok &= unlink(public_path) == 0 || errno == ENOENT;
        ok &= rmdir(private_path) == 0 || errno == ENOENT;
        ok &= rmdir(anchor) == 0 || errno == ENOENT;
        ok &= rmdir(hidden) == 0 || errno == ENOENT;
        ok &= rmdir(base) == 0 || errno == ENOENT;
        return ok;
    }
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(hidden, 0755) || chmod(hidden, 0755) || mkdir(anchor, 0755) ||
        chmod(anchor, 0755) || mkdir(private_path, 0710) || chown(private_path, 0, 2000) || chmod(private_path, 0710))
        return false;
    int fd = open(public_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) return false;
    bool ok = write(fd, "public", 6) == 6;
    ok &= close(fd) == 0;
    return ok && chmod(public_path, 0644) == 0 && symlink("private/../public", link) == 0 && symlink("public", public_link) == 0;
}

unsigned check(int fd, bool grouped, bool off, unsigned phase) {
    const char* paths[] = {"public", "private/../public", "private/missing", "link", "public-link", "public/", "private/../public/", "."};
    unsigned passed = 0;
    for (unsigned p = 0; p < 8; ++p)
        for (unsigned op = 0; op < 7; ++op) {
            bool text = op == 0 && (p == 3 || p == 4);
            bool symlink = op == 2 && (p == 3 || p == 4);
            bool private_search = p == 1 || p == 2 || p == 6 || (p == 3 && !text && !symlink);
            int expected = off                              ? ENOTSUP
                           : private_search && !grouped     ? EACCES
                           : p == 2                         ? ENOENT
                           : p == 5 || p == 6               ? ENOTDIR
                           : op == 0 && !text               ? EINVAL
                           : op == 5 || (op == 6 && p != 7) ? EACCES
                                                            : 0;
            struct stat st, before;
            memset(&st, 0x5A, sizeof(st));
            memcpy(&before, &st, sizeof(st));
            char bytes[32];
            memset(bytes, 'Z', sizeof(bytes));
            errno = 0;
            long result = op == 0  ? readlinkat(fd, paths[p], bytes, sizeof(bytes))
                          : op < 3 ? fstatat(fd, paths[p], &st, op == 2 ? AT_SYMLINK_NOFOLLOW : 0)
                                   : faccessat(fd, paths[p],
                                               op == 3   ? F_OK
                                               : op == 4 ? R_OK
                                               : op == 5 ? W_OK
                                                         : X_OK,
                                               0);
            int error = errno;
            bool untouched = true;
            if (op == 0 && expected)
                for (char c : bytes) untouched &= c == 'Z';
            if ((op == 1 || op == 2) && expected) untouched = memcmp(&st, &before, sizeof(st)) == 0;
            bool ok = expected ? result == -1 && error == expected && untouched : result >= 0;
            if (!expected && text) {
                const char* target = p == 3 ? "private/../public" : "public";
                size_t length = strlen(target);
                ok &= result == static_cast<long>(length) && memcmp(bytes, target, length) == 0;
                for (size_t i = length; i < sizeof(bytes); ++i) ok &= bytes[i] == 'Z';
            }
            if (!expected && (op == 1 || op == 2)) {
                ok &= symlink ? S_ISLNK(st.st_mode) : p == 7 ? S_ISDIR(st.st_mode) : S_ISREG(st.st_mode);
                if (p != 7) ok &= st.st_size == (symlink && p == 3 ? 17 : 6);
            }
            passed += ok;
            printf("%s phase=%u grouped=%d path=%s op=%u result=%ld errno=%d expected=%d untouched=%d\n", ok ? "PASS" : "FAIL", phase,
                   grouped, paths[p], op, result, error, expected, untouched);
        }
    return passed;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc < 3 || geteuid() != 0) return 2;
    if (strcmp(argv[1], "setup") == 0 || strcmp(argv[1], "mutate") == 0 || strcmp(argv[1], "cleanup") == 0) {
        if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 2;
        bool ok = provision(argv[2], argv[1]);
        printf("%s %s\n", ok ? "PASS" : "FAIL", argv[1]);
        return ok ? 0 : 1;
    }
    if (argc != 6 || strcmp(argv[1], "check") || (strcmp(argv[5], "on") && strcmp(argv[5], "off"))) return 2;
    int fd = open(argv[2], O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        printf("FAIL anchor open errno=%d\n", errno);
        return 2;
    }
    unsigned successful = 0;
    for (unsigned phase = 0; phase < 2; ++phase) {
        if (phase == 1) {
            int ready = open(argv[3], O_CREAT | O_EXCL | O_WRONLY, 0600);
            if (ready < 0 || close(ready)) {
                close(fd);
                return 2;
            }
            unsigned attempts = 0;
            while (access(argv[4], F_OK) != 0 && attempts++ < 600) usleep(100000);
            if (attempts >= 600) {
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
                unsigned passed = check(fd, grouped, strcmp(argv[5], "off") == 0, phase);
                printf("profile phase=%u grouped=%d passed=%u total=56\n", phase, grouped, passed);
                fflush(stdout);
                _exit(passed == 56 ? 0 : 1);
            }
            int status = 0;
            successful += child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        }
    }
    bool ok = close(fd) == 0 && successful == 4;
    printf("%s remote directory paths checks=224\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
