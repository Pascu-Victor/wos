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
// WOS's PR_SET_DUMPABLE selector; the two credential controls have named ABI values.
constexpr int SET_DUMPABLE = 4;
constexpr const char* REMOTE = "/wki/wos-3/tmp";
bool profile(unsigned kind) {
    if (kind == 0) return true;
    if (setgroups(0, nullptr) || setgid(2001) || setuid(kind == 3 ? 0 : 999 + kind)) return false;
    using Control = ker::abi::process::CredentialControl;
    return ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
bool wait_ok(pid_t pid) {
    int status = 0;
    return pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
struct Matrix {
    unsigned total = 0;
    unsigned passed = 0;
    void check(const char* relative, const char* expected, int expected_error) {
        char absolute[512];
        snprintf(absolute, sizeof(absolute), "/proc/%s", relative);
        size_t length = strlen(expected);
        for (size_t capacity : {size_t{0}, size_t{1}, length ? length : size_t{11}, length ? length + 1 : size_t{12}, size_t{1024}}) {
            char output[1024];
            memset(output, 'Z', sizeof(output));
            int result = ker::abi::vfs::realpath(absolute, output, capacity);
            int wanted = capacity == 0 ? EINVAL : expected_error ? expected_error : capacity <= length ? ERANGE : 0;
            bool ok = result == -wanted;
            if (!wanted) ok &= memcmp(output, expected, length + 1) == 0;
            for (size_t i = wanted ? 0 : length + 1; i < sizeof(output); ++i) ok &= output[i] == 'Z';
            ++total;
            passed += ok;
            if (!ok && total - passed <= 8)
                printf("FAIL path=%s cap=%zu result=%d expected=%d text=%.*s\n", relative, capacity, result, -wanted,
                       result == 0 ? static_cast<int>(length) : 0, output);
        }
    }
    void private_links(const char* pid, const char* base, const char* executable, int localfd, int remotefd, int error) {
        char path[512], local[512], local_tail[64], remote_tail[64];
        snprintf(local, sizeof(local), "%s/file", base);
        // REALPATH returns canonical task-visible text after following each link.
        snprintf(local_tail, sizeof(local_tail), "fd/%d", localfd);
        snprintf(remote_tail, sizeof(remote_tail), "fd/%d", remotefd);
        const char* tails[] = {"root", "cwd", "exe", local_tail, remote_tail};
        const char* values[] = {"/", base, executable, local, REMOTE};
        for (unsigned i = 0; i < 5; ++i) {
            snprintf(path, sizeof(path), "%s/%s", pid, tails[i]);
            check(path, values[i], error);
        }
    }
};
int observer(unsigned kind, int phase, pid_t target, const char* base, const char* exe, int localfd, int remotefd) {
    if (chdir(base) || !profile(kind)) return 2;
    int procfd = open("/proc", O_RDONLY | O_DIRECTORY);
    if (procfd < 0) return 2;
    Matrix matrix{};
    char numeric[64], self[64];
    snprintf(numeric, sizeof(numeric), "%ld", static_cast<long>(target ? target : getpid()));
    if (!target) {
        const char* aliases[] = {"self", numeric};
        for (const char* pid : aliases) {
            snprintf(self, sizeof(self), "/proc/%ld", static_cast<long>(getpid()));
            matrix.check(pid, self, 0);
            char path[256], canonical[256];
            for (const char* tail : {"status", "cmdline"}) {
                snprintf(path, sizeof(path), "%s/%s", pid, tail);
                snprintf(canonical, sizeof(canonical), "%s/%s", self, tail);
                matrix.check(path, canonical, 0);
            }
            matrix.private_links(pid, base, exe, localfd, remotefd, 0);
        }
        matrix.check(".", "/proc", 0);
        matrix.check("version", "/proc/version", 0);
        matrix.check("./version", "/proc/version", 0);
        matrix.check("version/", "", ENOTDIR);
        matrix.check("version/..", "", ENOTDIR);
        matrix.check("goal19-no-such-proc-entry", "", ENOENT);
        matrix.check("wki/auth", "/proc/wki/auth", 0);
        matrix.check("kperf", "/proc/kperf", 0);
        matrix.check("kperfctl", "/proc/kperfctl", 0);
    } else {
        bool allowed = kind == 0 || (kind == 1 && phase == 1);
        matrix.private_links(numeric, base, exe, localfd, remotefd, allowed ? 0 : EACCES);
        char path[256], canonical[256];
        snprintf(path, sizeof(path), "%s/cmdline", numeric);
        snprintf(canonical, sizeof(canonical), "/proc/%s/cmdline", numeric);
        // Resolving this public filename does not grant access to its contents.
        matrix.check(path, canonical, 0);
    }
    bool ok = close(procfd) == 0 && matrix.passed == matrix.total;
    printf("%s profile=%u phase=%d target=%ld passed=%u total=%u\n", ok ? "PASS" : "FAIL", kind, phase, static_cast<long>(target),
           matrix.passed, matrix.total);
    fflush(stdout);
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc != 1 || geteuid() != 0) return 2;
    char base[256], file[256];
    snprintf(base, sizeof(base), "/tmp/goal19-proc-realpath-%ld", static_cast<long>(getpid()));
    snprintf(file, sizeof(file), "%s/file", base);
    if (mkdir(base, 0755) || chmod(base, 0755)) return 2;
    int localfd = open(file, O_CREAT | O_EXCL | O_RDONLY, 0644);
    int remotefd = open(REMOTE, O_RDONLY | O_DIRECTORY);
    if (localfd < 0 || remotefd < 0) return 2;
    unsigned bad = 0;
    for (unsigned kind = 0; kind < 4; ++kind) {
        pid_t child = fork();
        if (child == 0) _exit(observer(kind, -1, 0, base, argv[0], localfd, remotefd));
        bad += !wait_ok(child);
    }
    int commands[2], replies[2];
    if (pipe(commands) || pipe(replies)) return 2;
    pid_t target = fork();
    if (target == 0) {
        close(commands[1]);
        close(replies[0]);
        if (chdir(base) || !profile(1)) _exit(2);
        char phase;
        while (read(commands[0], &phase, 1) == 1 && phase != 'q') {
            if (ker::process::prctl(SET_DUMPABLE, phase == '1', 0, 0, 0) || write(replies[1], &phase, 1) != 1) _exit(2);
        }
        close(commands[0]);
        close(replies[1]);
        _exit(0);
    }
    close(commands[0]);
    close(replies[1]);
    if (target > 0) {
        // Acknowledgments establish that each new operation sees the changed target policy.
        // The integrated kernel test separately exercises staged-result generation invalidation.
        for (int phase = 0; phase < 3; ++phase) {
            char command = phase == 1 ? '1' : '0', reply = 0;
            if (write(commands[1], &command, 1) != 1 || read(replies[0], &reply, 1) != 1 || reply != command) {
                ++bad;
                break;
            }
            for (unsigned kind = 0; kind < 4; ++kind) {
                pid_t child = fork();
                if (child == 0) _exit(observer(kind, phase, target, base, argv[0], localfd, remotefd));
                bad += !wait_ok(child);
            }
        }
        char quit = 'q';
        bad += write(commands[1], &quit, 1) != 1;
        bad += !wait_ok(target);
    } else {
        ++bad;
    }
    bool cleanup = close(commands[1]) == 0 && close(replies[0]) == 0;
    cleanup &= close(localfd) == 0;
    cleanup &= close(remotefd) == 0;
    cleanup &= unlink(file) == 0;
    cleanup &= rmdir(base) == 0;
    bad += !cleanup;
    printf("%s: 860 native PROCFS REALPATH checks; cleanup=%d\n", bad ? "FAIL" : "PASS", cleanup);
    return bad ? 1 : 0;
}
