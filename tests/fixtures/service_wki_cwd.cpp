// Functional cwd identity and permission checks in a disposable WOS cluster.
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
unsigned checks = 0, failures = 0;
void expect(bool ok, const char* label) {
    ++checks;
    if (!ok) {
        ++failures;
        printf("FAIL %s errno=%d\n", label, errno);
    }
}
bool wait_ok(pid_t child) {
    int status = 0;
    return child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
bool contents(const char* path, const char* expected) {
    char bytes[8]{};
    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;
    ssize_t n = read(fd, bytes, sizeof(bytes));
    return close(fd) == 0 && n == 4 && memcmp(bytes, expected, 4) == 0;
}
bool create(const char* path, const char* value) {
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) return false;
    return write(fd, value, 4) == 4 && close(fd) == 0;
}
bool canonical_is(const char* path, const char* expected) {
    char canonical[512]{};
    int result = ker::abi::vfs::realpath(path, canonical, sizeof(canonical));
    if (result != 0) printf("realpath path=%s result=%d\n", path, result);
    return result == 0 && strcmp(canonical, expected) == 0;
}
bool name_is(const char* expected) {
    char name[512]{};
    return getcwd(name, sizeof(name)) != nullptr && strcmp(name, expected) == 0;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    const char* mode = argv[1];
    const char* base = argv[2];
    if (!strcmp(mode, "exec-check")) {
        bool ok = name_is(base) && contents("file", "seed");
        printf("%s exec-check\n", ok ? "PASS" : "FAIL");
        return ok ? 0 : 1;
    }
    char original[512], moved[512], file[512], moved_file[512], alias[512];
    snprintf(alias, sizeof(alias), "%s/alias", base);
    snprintf(original, sizeof(original), "%s/original", base);
    snprintf(moved, sizeof(moved), "%s/moved", base);
    snprintf(file, sizeof(file), "%s/original/file", base);
    snprintf(moved_file, sizeof(moved_file), "%s/moved/file", base);
    if (!strcmp(mode, "setup") || !strcmp(mode, "cleanup")) {
        if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 3;
        bool ok = true;
        if (!strcmp(mode, "setup")) {
            ok = mkdir(base, 0755) == 0 && mkdir(original, 0755) == 0 && chown(original, 0, 2002) == 0 && create(file, "seed") &&
                 symlink(original, alias) == 0;
        } else {
            for (const char* path : {alias, file, moved_file})
                if (unlink(path) && errno != ENOENT) ok = false;
            for (const char* path : std::initializer_list<const char*>{original, moved, base})
                if (rmdir(path) && errno != ENOENT) ok = false;
        }
        printf("%s %s\n", ok ? "PASS" : "FAIL", mode);
        return ok ? 0 : 1;
    }
    if (!strcmp(mode, "unsupported-realpath")) {
        expect(chdir(original) == 0, "old owner supports directory pin");
        char output[512];
        memset(output, '!', sizeof(output));
        int result = ker::abi::vfs::realpath("file", output, sizeof(output));
        bool unchanged = true;
        for (char byte : output) unchanged &= byte == '!';
        expect(result == -EOPNOTSUPP && unchanged, "old owner refuses retained realpath without output");
        expect(name_is(original), "realpath refusal preserves cwd");
        expect(contents("file", "seed"), "retained relative open remains valid");
        expect(chdir("/") == 0, "old owner cwd retirement");
        expect(contents(file, "seed"), "old owner contents unchanged");
        printf("%s unsupported-realpath checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
        return failures ? 1 : 0;
    }
    if (!strcmp(mode, "unsupported")) {
        char before[512]{};
        if (getcwd(before, sizeof(before)) == nullptr) return 10;
        int directory = open(original, O_RDONLY | O_DIRECTORY);
        if (directory < 0) return 11;
        errno = 0;
        expect(chdir(original) == -1 && errno == EOPNOTSUPP, "old owner chdir refused");
        expect(name_is(before), "chdir refusal preserves cwd");
        errno = 0;
        expect(fchdir(directory) == -1 && errno == EOPNOTSUPP, "old owner fchdir refused");
        expect(name_is(before), "fchdir refusal preserves cwd");
        expect(contents(file, "seed"), "old owner contents unchanged");
        expect(close(directory) == 0, "old owner source close");
        printf("%s unsupported checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
        return failures ? 1 : 0;
    }
    bool const REALPATH = !strcmp(mode, "realpath-check");
    bool const ALIASES = !strcmp(mode, "alias-check");
    if (strcmp(mode, "check") && !REALPATH && !ALIASES) return 2;
    if (ALIASES) {
        expect(chdir(alias) == 0, "mapped absolute alias chdir");
        expect(name_is(original), "mapped alias retains canonical directory");
        pid_t child = fork();
        if (child == 0) {
            if (setgroups(0, nullptr) || setgid(2001) || setuid(1002)) _exit(13);
            _exit(chdir(alias) == 0 && name_is(original) && contents("file", "seed") ? 0 : 1);
        }
        expect(wait_ok(child), "unprivileged mapped alias PIN");
        expect(chdir("/") == 0, "release mapped alias cwd");
    }
    int directory = open(original, O_RDONLY | O_DIRECTORY);
    if (directory < 0) return 4;
    expect(chdir(original) == 0, "remote chdir");
    expect(name_is(original), "initial cwd name");
    expect(rename(original, moved) == 0, "rename current directory");
    expect(mkdir(original, 0755) == 0 && create(file, "fake"), "reuse old name");
    expect(name_is(moved), "cwd follows retained directory rename");
    expect(contents("file", "seed"), "relative open retains directory");
    if (REALPATH) {
        expect(canonical_is("file", moved_file), "relative realpath retained cwd");
        expect(contents("/proc/self/cwd/file", "seed"), "follow proc cwd link");
    }
    char proc[512]{};
    ssize_t n = readlink("/proc/self/cwd", proc, sizeof(proc) - 1);
    expect(n == static_cast<ssize_t>(strlen(moved)) && !strcmp(proc, moved), "proc cwd current name");
    char short_name[2] = {'!', '!'};
    errno = 0;
    expect(getcwd(short_name, sizeof(short_name)) == nullptr && errno == ERANGE && short_name[0] == '!' && short_name[1] == '!',
           "short cwd output unchanged");
    pid_t child = fork();
    if (child == 0) _exit(name_is(moved) && contents("file", "seed") ? 0 : 1);
    expect(wait_ok(child), "fork inherits cwd reference");
    child = fork();
    if (child == 0) {
        char* const arguments[] = {argv[0], const_cast<char*>("exec-check"), moved, nullptr};
        execv(argv[0], arguments);
        _exit(5);
    }
    expect(wait_ok(child), "exec inherits cwd reference");
    expect(chmod(moved, 0111) == 0, "execute-only directory mode");
    for (bool descriptor : {false, true}) {
        child = fork();
        if (child == 0) {
            if (setgroups(0, nullptr) || setgid(2001) || setuid(1002)) _exit(6);
            int result = descriptor ? fchdir(directory) : chdir(moved);
            if (result || close(directory)) _exit(7);
            _exit(name_is(moved) && contents("file", "seed") ? 0 : 1);
        }
        expect(wait_ok(child), descriptor ? "search-only fchdir and source fd close" : "search-only chdir");
    }
    expect(chmod(moved, 0000) == 0, "revoke search");
    child = fork();
    if (child == 0) {
        if (setgroups(0, nullptr) || setgid(2001) || setuid(1002)) _exit(8);
        bool ok = name_is(moved);
        errno = 0;
        int denied = open("file", O_RDONLY);
        ok &= denied == -1 && errno == EACCES;
        errno = 0;
        ok &= fchdir(directory) == -1 && errno == EACCES;
        _exit(ok ? 0 : 1);
    }
    expect(wait_ok(child), "current caller search denial with retained name");
    // Ownership was provisioned on the owner; remote chown is a separate pending surface.
    expect(chmod(moved, 0010) == 0, "group-only search");
    for (bool grouped : {false, true}) {
        child = fork();
        if (child == 0) {
            gid_t group = 2002;
            if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1002)) _exit(9);
            errno = 0;
            int result = fchdir(directory);
            _exit(grouped ? (result == 0 && contents("file", "seed") ? 0 : 1) : (result == -1 && errno == EACCES ? 0 : 1));
        }
        expect(wait_ok(child), grouped ? "supplementary group search" : "missing group search denied");
    }
    expect(chmod(moved, 0755) == 0, "restore search");
    if (REALPATH) {
        expect(chmod(base, 0000) == 0, "remove ancestor search after cwd admission");
        child = fork();
        if (child == 0) {
            if (setgroups(0, nullptr) || setgid(2001) || setuid(1002)) _exit(12);
            bool ok = canonical_is("file", moved_file) && canonical_is(".", moved) && name_is(moved);
            char small[2] = {'!', '!'};
            ok &= ker::abi::vfs::realpath("file", small, sizeof(small)) == -ERANGE && small[0] == '!' && small[1] == '!';
            _exit(ok ? 0 : 1);
        }
        expect(wait_ok(child), "realpath uses retained cwd without ancestor reauthorization");
        expect(chmod(base, 0755) == 0, "restore ancestor search");
    }
    expect(fchdir(directory) == 0 && close(directory) == 0, "independent cwd pin");
    expect(contents("file", "seed") && name_is(moved), "cwd survives source close");
    expect(unlink(moved_file) == 0 && rmdir(moved) == 0, "remove held cwd");
    char gone[512];
    memset(gone, '!', sizeof(gone));
    errno = 0;
    expect(getcwd(gone, sizeof(gone)) == nullptr && errno == ENOENT && gone[0] == '!' && gone[511] == '!', "removed cwd name failure");
    expect(chdir("/") == 0, "release remote cwd into native root");
    expect(contents(file, "fake"), "replacement unchanged");
    printf("%s check checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures ? 1 : 0;
}
