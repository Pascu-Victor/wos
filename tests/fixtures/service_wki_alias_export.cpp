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
bool seed(const char* path, const char* data, bool user_owned) {
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd < 0) return false;
    bool ok = write(fd, data, strlen(data)) == static_cast<ssize_t>(strlen(data));
    ok &= close(fd) == 0;
    if (user_owned) ok &= chown(path, 1000, 2001) == 0;
    return chmod(path, 0644) == 0 && ok;
}
bool owner(const char* operation, const char* base, const char* outside) {
    char private_path[512], public_path[512], link[512], escape[512], file[512], created[512], target[512];
    snprintf(private_path, sizeof(private_path), "%s/private", base);
    snprintf(public_path, sizeof(public_path), "%s/public", base);
    snprintf(link, sizeof(link), "%s/link", base);
    snprintf(escape, sizeof(escape), "%s/escape", base);
    snprintf(file, sizeof(file), "%s/file", outside);
    snprintf(created, sizeof(created), "%s/new", outside);
    if (!strcmp(operation, "cleanup")) {
        bool ok = true;
        for (const char* path : {created, file, escape, link, public_path}) ok &= unlink(path) == 0 || errno == ENOENT;
        for (const char* path : std::initializer_list<const char*>{private_path, base, outside}) ok &= rmdir(path) == 0 || errno == ENOENT;
        return ok;
    }
    if (!strcmp(operation, "verify")) {
        struct stat st;
        bool ok = lstat(created, &st) == -1 && errno == ENOENT;
        int fd = open(file, O_RDONLY);
        if (fd < 0) return false;
        char data[8]{};
        ssize_t count = read(fd, data, sizeof(data));
        ok &= count == 4 && memcmp(data, "KEEP", 4) == 0;
        return close(fd) == 0 && ok;
    }
    if (strcmp(operation, "setup")) return false;
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(private_path, 0710) || chown(private_path, 0, 2000) || chmod(private_path, 0710) ||
        mkdir(outside, 0777) || chmod(outside, 0777))
        return false;
    if (!seed(public_path, "OK", false) || !seed(file, "KEEP", true)) return false;
    snprintf(target, sizeof(target), "../../%s", outside + 1);
    return symlink("public", link) == 0 && symlink(target, escape) == 0;
}
int invoke(unsigned operation, const char* path, bool final_link, bool& preserved) {
    errno = 0;
    if (operation < 2) {
        struct stat st;
        memset(&st, 0xA5, sizeof(st));
        int result = operation ? lstat(path, &st) : stat(path, &st);
        int error = result == 0 ? 0 : errno;
        if (error)
            for (size_t i = 0; i < sizeof(st); ++i) preserved &= reinterpret_cast<const unsigned char*>(&st)[i] == 0xA5;
        return error;
    }
    if (operation == 2) return access(path, F_OK) == 0 ? 0 : errno;
    if (operation == 3) {
        char data[32];
        memset(data, 'Z', sizeof(data));
        ssize_t result = readlink(path, data, sizeof(data));
        int error = result >= 0 ? 0 : errno;
        if (error)
            for (char b : data) preserved &= b == 'Z';
        else {
            preserved &= final_link && result == 6 && memcmp(data, "public", 6) == 0;
            for (size_t i = 6; i < sizeof(data); ++i) preserved &= data[i] == 'Z';
        }
        return error;
    }
    int fd = open(path, O_RDONLY);
    int error = fd >= 0 ? 0 : errno;
    if (fd >= 0) preserved &= close(fd) == 0;
    return error;
}
bool check(const char* remote, const char* alias, const char* outside, bool grouped) {
    char escaped_file[512];
    snprintf(escaped_file, sizeof(escaped_file), "../../%s/file", outside + 1);
    struct Case {
        const char* tail;
        int ungrouped;
        int group;
        bool link;
    };
    const Case cases[] = {{"public", 0, 0, false},
                          {"private/../public", EACCES, 0, false},
                          {"private/.", EACCES, 0, false},
                          {"private/missing", EACCES, ENOENT, false},
                          {"public/", ENOTDIR, ENOTDIR, false},
                          {"escape/..", EPERM, EPERM, false},
                          {escaped_file, EPERM, EPERM, false},
                          {"../..", EPERM, EPERM, false},
                          {"link", 0, 0, true},
                          {"missing", ENOENT, ENOENT, false}};
    unsigned total = 0, passed = 0;
    for (const char* prefix : {remote, alias}) {
        for (const auto& c : cases) {
            char path[512];
            snprintf(path, sizeof(path), "%s/%s", prefix, c.tail);
            for (unsigned op = 0; op < 5; ++op) {
                int expected = grouped ? c.group : c.ungrouped;
                if (!expected && op == 3 && !c.link) expected = EINVAL;
                // Group execute permits traversal; opening the directory for
                // reading still requires its separate read permission.
                if (op == 4 && strcmp(c.tail, "private/.") == 0) expected = EACCES;
                bool preserved = true;
                int error = invoke(op, path, c.link, preserved);
                bool ok = error == expected && preserved;
                ++total;
                passed += ok;
                if (!ok)
                    printf("FAIL grouped=%d alias=%d op=%u tail=%s errno=%d expected=%d preserved=%d\n", grouped, prefix == alias, op,
                           c.tail, error, expected, preserved);
            }
        }
        for (bool create : {false, true}) {
            char path[512];
            snprintf(path, sizeof(path), "%s/../../%s/%s", prefix, outside + 1, create ? "new" : "file");
            int fd = open(path, O_WRONLY | (create ? O_CREAT | O_EXCL : O_TRUNC), 0600);
            int error = fd >= 0 ? 0 : errno;
            if (fd >= 0) close(fd);
            bool ok = error == EPERM;
            ++total;
            passed += ok;
            if (!ok)
                printf("FAIL grouped=%d alias=%d mutation=%s errno=%d expected=%d\n", grouped, prefix == alias,
                       create ? "create" : "truncate", error, EPERM);
        }
    }
    printf("profile grouped=%d passed=%u total=%u\n", grouped, passed, total);
    return passed == total;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc < 4 || geteuid() != 0) return 2;
    if (strcmp(argv[1], "check")) {
        if (argc != 4 || ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 2;
        bool ok = owner(argv[1], argv[2], argv[3]);
        printf("%s %s\n", ok ? "PASS" : "FAIL", argv[1]);
        return ok ? 0 : 1;
    }
    if (argc != 5) return 2;
    unsigned passed = 0;
    for (bool grouped : {false, true}) {
        pid_t pid = fork();
        if (pid == 0) {
            gid_t group = 2000;
            if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1000)) _exit(2);
            using Control = ker::abi::process::CredentialControl;
            if (ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
                ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
                _exit(2);
            bool ok = check(argv[2], argv[3], argv[4], grouped);
            fflush(stdout);
            _exit(ok ? 0 : 1);
        }
        int status = 0;
        if (pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0) ++passed;
    }
    printf("%s alias export checks=208\n", passed == 2 ? "PASS" : "FAIL");
    return passed == 2 ? 0 : 1;
}
