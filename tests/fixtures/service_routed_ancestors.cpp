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
int main(int argc, char** argv) {
    struct utsname node{};
    if ((argc != 3 && argc != 4) || uname(&node) || strcmp(node.nodename, "wos-3") || geteuid() != 0) return 2;
    if (argc == 4 && strcmp(argv[3], "dot") && strcmp(argv[3], "capoff")) return 2;
    int const HOST_ERROR = argc == 3 ? 0 : strcmp(argv[3], "dot") == 0 ? EACCES : ENOTSUP;
    char pub[512], priv[512], link[512], dot[512], missing[512], host[512], hidden_host[512], self[512], self_target[512];
    snprintf(pub, sizeof(pub), "%s/public", argv[1]);
    snprintf(priv, sizeof(priv), "%s/private", argv[1]);
    snprintf(link, sizeof(link), "%s/private/link", argv[1]);
    snprintf(dot, sizeof(dot), "%s/private/../public", argv[1]);
    snprintf(missing, sizeof(missing), "%s/private/missing", argv[1]);
    snprintf(host, sizeof(host), "%s/host", argv[1]);
    snprintf(hidden_host, sizeof(hidden_host), "%s/private/host", argv[1]);
    snprintf(self, sizeof(self), "%s/self", argv[1]);
    snprintf(self_target, sizeof(self_target), "/wki/wos-3%s/public", argv[1]);
    if (ker::abi::vfs::wki_rule_add_vfs("/tmp", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 2;
    if (mkdir(argv[1], 0777) || chmod(argv[1], 0777) || mkdir(priv, 0700) || chmod(priv, 0700)) return 2;
    int fd = open(pub, O_CREAT | O_EXCL | O_WRONLY, 0666);
    if (fd < 0 || write(fd, "public", 6) != 6 || close(fd) || chmod(pub, 0666) || symlink("../public", link)) return 2;
    if (symlink(argv[2], host) || symlink(argv[2], hidden_host) || symlink(self_target, self)) return 2;
    // Warm the privileged final target and link before the child loses authority.
    char warm[32];
    readlink(link, warm, sizeof(warm));
    struct stat warmed{};
    int const WARM_RESULT = stat(hidden_host, &warmed);
    if (HOST_ERROR == ENOTSUP ? WARM_RESULT != -1 || errno != ENOTSUP : WARM_RESULT != 0) return 2;
    pid_t child = fork();
    if (child == 0) {
        if (setgroups(0, nullptr) || setgid(2000) || setuid(1000)) _exit(2);
        using Control = ker::abi::process::CredentialControl;
        if (ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
            ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
            _exit(2);
        unsigned passed = 0, total = 0;
        for (const char* path : {link, dot, missing, hidden_host}) {
            for (unsigned op = 0; op < 4; ++op) {
                char bytes[32];
                memset(bytes, 'Z', sizeof(bytes));
                struct stat st, old;
                memset(&st, 0x5A, sizeof(st));
                memcpy(&old, &st, sizeof(st));
                errno = 0;
                long result = 0;
                if (op == 0) result = readlink(path, bytes, sizeof(bytes));
                if (op == 1) result = lstat(path, &st);
                if (op == 2) result = access(path, F_OK);
                if (op == 3) result = open(path, O_RDONLY | O_NOCTTY);
                int error = errno;
                bool untouched = true;
                if (op == 0)
                    for (char c : bytes) untouched &= c == 'Z';
                if (op == 1) untouched = memcmp(&st, &old, sizeof(st)) == 0;
                if (op == 3 && result >= 0) close(static_cast<int>(result));
                bool ok = result == -1 && error == EACCES && untouched;
                passed += ok;
                ++total;
                printf("%s op=%u path=%s result=%ld errno=%d untouched=%d\n", ok ? "PASS" : "FAIL", op, path, result, error, untouched);
            }
        }
        for (unsigned transition = 0; transition < 2; ++transition) {
            const char* path = transition == 0 ? host : self;
            const char* target = transition == 0 ? argv[2] : self_target;
            const char* content = transition == 0 ? "origin" : "public";
            for (unsigned op = 0; op < 4; ++op) {
                char bytes[512];
                memset(bytes, 'Z', sizeof(bytes));
                struct stat st, original;
                memset(&st, 0x5A, sizeof(st));
                memcpy(&original, &st, sizeof(st));
                errno = 0;
                bool ok = false;
                int const EXPECTED = transition == 0 ? HOST_ERROR : 0;
                if (op == 0) {
                    ssize_t n = readlink(path, bytes, sizeof(bytes));
                    ok = n == static_cast<ssize_t>(strlen(target)) && memcmp(bytes, target, strlen(target)) == 0 &&
                         bytes[strlen(target)] == 'Z';
                }
                if (op == 1) {
                    int result = stat(path, &st);
                    ok = EXPECTED == 0 ? result == 0 && S_ISREG(st.st_mode) && st.st_size == 6
                                       : result == -1 && errno == EXPECTED && memcmp(&st, &original, sizeof(st)) == 0;
                }
                if (op == 2) {
                    int result = access(path, F_OK);
                    ok = EXPECTED == 0 ? result == 0 : result == -1 && errno == EXPECTED;
                }
                if (op == 3) {
                    int file = open(path, O_RDONLY);
                    if (file >= 0) {
                        ok = EXPECTED == 0 && read(file, bytes, sizeof(bytes)) == 6 && memcmp(bytes, content, 6) == 0;
                        ok &= close(file) == 0;
                    } else
                        ok = EXPECTED != 0 && errno == EXPECTED;
                }
                passed += ok;
                ++total;
                printf("%s transition=%u op=%u errno=%d expected=%d\n", ok ? "PASS" : "FAIL", transition, op, errno,
                       op == 0 ? 0 : EXPECTED);
            }
        }
        if (HOST_ERROR != 0) {
            // An intermediate link hands the remaining READLINK operation to
            // the owner, unlike reading the receiver's final link as text.
            char followed[512], bytes[32];
            snprintf(followed, sizeof(followed), "%s/.", host);
            memset(bytes, 'Z', sizeof(bytes));
            errno = 0;
            ssize_t result = readlink(followed, bytes, sizeof(bytes));
            int error = errno;
            bool untouched = true;
            for (char c : bytes) untouched &= c == 'Z';
            bool ok = result == -1 && error == HOST_ERROR && untouched;
            passed += ok;
            ++total;
            printf("%s mapped-component-readlink errno=%d expected=%d untouched=%d\n", ok ? "PASS" : "FAIL", error, HOST_ERROR, untouched);
        }
        printf("receiver=%s passed=%u total=%u\n", node.nodename, passed, total);
        fflush(stdout);
        _exit(passed == total ? 0 : 1);
    }
    int status = 0;
    bool waited = child > 0 && waitpid(child, &status, 0) == child;
    bool clean = true;
    for (const char* path : {link, hidden_host, host, self, pub}) clean &= unlink(path) == 0;
    clean &= rmdir(priv) == 0;
    clean &= rmdir(argv[1]) == 0;
    printf("cleanup=%d\n", clean);
    return clean && waited && WIFEXITED(status) ? WEXITSTATUS(status) : 2;
}
