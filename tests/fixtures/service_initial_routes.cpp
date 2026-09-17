#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
bool set_rule(const char* prefix, bool host) {
    return ker::abi::vfs::wki_rule_add_vfs(prefix, host ? ker::abi::vfs::WKI_VFS_ROUTE_HOST : ker::abi::vfs::WKI_VFS_ROUTE_LOCAL) == 0;
}

bool provision(const char* base, bool remove) {
    char private_path[512], public_path[512];
    snprintf(private_path, sizeof(private_path), "%s/private", base);
    snprintf(public_path, sizeof(public_path), "%s/public", base);
    if (remove) {
        bool ok = unlink(public_path) == 0 || errno == ENOENT;
        ok &= rmdir(private_path) == 0 || errno == ENOENT;
        ok &= rmdir(base) == 0 || errno == ENOENT;
        return ok;
    }
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(private_path, 0710) || chown(private_path, 0, 2000) || chmod(private_path, 0710))
        return false;
    int fd = open(public_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) return false;
    bool ok = write(fd, "public", 6) == 6;
    ok &= close(fd) == 0;
    return chmod(public_path, 0644) == 0 && ok;
}
}  // namespace

int main(int argc, char** argv) {
    utsname node{};
    if (argc != 5 || geteuid() != 0 || uname(&node) || (strcmp(node.nodename, "wos-0") && strcmp(node.nodename, "wos-3"))) return 2;
    if (strcmp(argv[4], "on") && strcmp(argv[4], "off")) return 2;
    if (strcmp(argv[1], "setup") == 0 || strcmp(argv[1], "cleanup") == 0) {
        if (!set_rule("/", false)) return 2;
        bool remove = strcmp(argv[1], "cleanup") == 0;
        bool ok = provision(argv[2], remove);
        ok &= provision(argv[3], remove);
        printf("%s %s node=%s\n", ok ? "PASS" : "FAIL", argv[1], node.nodename);
        return ok ? 0 : 1;
    }
    if (strcmp(argv[1], "check")) return 2;
    bool const FOREIGN = strcmp(node.nodename, "wos-3") == 0;
    bool const CAPOFF = strcmp(argv[4], "off") == 0;
    const char* other = FOREIGN ? "wos-0" : "wos-3";
    if (!set_rule(argv[2], true) || !set_rule(argv[3], false) || !set_rule("/wki", false)) return 2;
    char paths[7][512];
    snprintf(paths[0], sizeof(paths[0]), "/wki/%s%s/private/../public", other, argv[2]);
    snprintf(paths[1], sizeof(paths[1]), "/wki/host%s/private/../public", argv[2]);
    snprintf(paths[2], sizeof(paths[2]), "%s/private/../public", argv[2]);
    snprintf(paths[3], sizeof(paths[3]), "/wki/%s%s/private/../public", node.nodename, argv[3]);
    snprintf(paths[4], sizeof(paths[4]), "%s/private/../public", argv[3]);
    snprintf(paths[5], sizeof(paths[5]), "/wki/%s/../", other);
    snprintf(paths[6], sizeof(paths[6]), "//wki//%s/%s//private/../public", node.nodename, argv[3]);
    auto remote = [&](unsigned path) { return path == 0 || path == 5 || (FOREIGN && (path == 1 || path == 2)); };
    for (unsigned path = 0; path < 7; ++path) {
        struct stat st{};
        errno = 0;
        int result = stat(paths[path], &st);
        int expected = CAPOFF && remote(path) ? ENOTSUP : path == 5 ? EPERM : 0;
        if (expected == 0 ? result != 0 : result != -1 || errno != expected) {
            printf("FAIL warm path=%u result=%d errno=%d expected=%d\n", path, result, errno, expected);
            return 1;
        }
    }
    unsigned successful_children = 0;
    for (bool grouped : {false, true}) {
        pid_t child = fork();
        if (child == 0) {
            gid_t group = 2000;
            if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1000)) _exit(2);
            using Control = ker::abi::process::CredentialControl;
            if (ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
                ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
                _exit(2);
            unsigned passed = 0;
            for (unsigned path = 0; path < 7; ++path) {
                for (unsigned op = 0; op < 5; ++op) {
                    int expected = CAPOFF && remote(path) ? ENOTSUP : path == 5 ? EPERM : !grouped ? EACCES : op == 0 ? EINVAL : 0;
                    char bytes[32];
                    memset(bytes, 'Z', sizeof(bytes));
                    struct stat st, before;
                    memset(&st, 0x5A, sizeof(st));
                    memcpy(&before, &st, sizeof(st));
                    errno = 0;
                    long result = -1;
                    if (op == 0) result = readlink(paths[path], bytes, sizeof(bytes));
                    if (op == 1) result = stat(paths[path], &st);
                    if (op == 2) result = lstat(paths[path], &st);
                    if (op == 3) result = access(paths[path], F_OK);
                    if (op == 4) result = open(paths[path], O_RDONLY | O_NOCTTY);
                    int error = errno;
                    bool untouched = true;
                    if (op == 0)
                        for (char c : bytes) untouched &= c == 'Z';
                    if ((op == 1 || op == 2) && expected != 0) untouched = memcmp(&st, &before, sizeof(st)) == 0;
                    bool ok = expected == 0 ? result >= 0 : result == -1 && error == expected && untouched;
                    if ((op == 1 || op == 2) && expected == 0) ok &= S_ISREG(st.st_mode) && st.st_size == 6;
                    if (op == 4 && result >= 0) {
                        if (expected == 0)
                            ok &= read(static_cast<int>(result), bytes, sizeof(bytes)) == 6 && memcmp(bytes, "public", 6) == 0;
                        ok &= close(static_cast<int>(result)) == 0;
                    }
                    passed += ok;
                    printf("%s node=%s grouped=%d path=%u op=%u errno=%d expected=%d untouched=%d\n", ok ? "PASS" : "FAIL", node.nodename,
                           grouped, path, op, error, expected, untouched);
                }
            }
            printf("profile=%s passed=%u total=35\n", grouped ? "grouped" : "ungrouped", passed);
            fflush(stdout);
            _exit(passed == 35 ? 0 : 1);
        }
        int status = 0;
        successful_children += child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
    printf("%s initial routes node=%s checks=70\n", successful_children == 2 ? "PASS" : "FAIL", node.nodename);
    return successful_children == 2 ? 0 : 1;
}
