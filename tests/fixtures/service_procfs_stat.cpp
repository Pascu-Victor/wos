#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
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
    int procfd;
    unsigned kind;
    unsigned total = 0;
    unsigned passed = 0;
    void record(bool ok, const char* path, bool at, int mode, int result, int error, int wanted) {
        ++total;
        passed += ok;
        if (!ok && total - passed <= 8)
            printf("FAIL path=%s at=%d mode=%d result=%d errno=%d expected=%d\n", path, at, mode, result, error, wanted);
    }
    void check(const char* relative, mode_t expected_mode, uid_t uid, gid_t gid, int expected_error = 0, bool nofollow = false) {
        char absolute[512];
        snprintf(absolute, sizeof(absolute), "/proc/%s", relative);
        int flags = nofollow ? AT_SYMLINK_NOFOLLOW : 0;
        for (bool at : {false, true}) {
            struct stat output, before;
            memset(&output, 0xA5, sizeof(output));
            memcpy(&before, &output, sizeof(output));
            errno = 0;
            int result = at ? fstatat(procfd, relative, &output, flags) : nofollow ? lstat(absolute, &output) : stat(absolute, &output);
            int error = result < 0 ? errno : 0;
            bool ok = error == expected_error && (expected_error ? result == -1 : result == 0);
            ok &= expected_error ? memcmp(&output, &before, sizeof(output)) == 0
                                 : output.st_mode == expected_mode && output.st_uid == uid && output.st_gid == gid;
            if (!ok && !expected_error && total - passed < 8)
                printf("metadata mode=%o uid=%u gid=%u wanted=%o/%u/%u\n", output.st_mode, output.st_uid, output.st_gid, expected_mode, uid,
                       gid);
            record(ok, relative, at, -2, result, error, expected_error);
            for (int mode : {0, 1, 2, 4, 6, 8, -1}) {
                int wanted = expected_error;
                if (!wanted) {
                    unsigned bits = (getuid() == uid)   ? ((expected_mode >> 6) & 7)
                                    : (getgid() == gid) ? ((expected_mode >> 3) & 7)
                                                        : (expected_mode & 7);
                    // Existing WOS DAC masks access arguments to their low three
                    // bits. This migration preserves that behavior and errno.
                    unsigned requested = static_cast<unsigned>(mode) & 7U;
                    if (kind == 0)
                        wanted = (requested & 1U) && !(expected_mode & 0111) ? EACCES : 0;
                    else
                        wanted = (bits & requested) == requested ? 0 : EACCES;
                }
                errno = 0;
                result = faccessat(at ? procfd : AT_FDCWD, at ? relative : absolute, mode, flags);
                error = result < 0 ? errno : 0;
                record(error == wanted && result == (wanted ? -1 : 0), relative, at, mode, result, error, wanted);
            }
        }
    }
    void process(const char* pid, uid_t uid, gid_t gid) {
        check(pid, S_IFDIR | 0555, uid, gid);
        char path[256];
        for (const char* tail : {"status", "stat", "limits", "cmdline", "maps"}) {
            snprintf(path, sizeof(path), "%s/%s", pid, tail);
            check(path, S_IFREG | 0444, uid, gid);
        }
        snprintf(path, sizeof(path), "%s/root", pid);
        check(path, S_IFLNK | 0777, uid, gid, 0, true);
    }
};
int observer(unsigned kind, int phase, pid_t target) {
    if (!profile(kind)) return 2;
    int procfd = open("/proc", O_RDONLY | O_DIRECTORY);
    if (procfd < 0) return 2;
    Matrix matrix{procfd, kind};
    if (target == 0) {
        matrix.check(".", S_IFDIR | 0555, 0, 0);
        matrix.check("version", S_IFREG | 0444, 0, 0);
        matrix.check("uptime", S_IFREG | 0444, 0, 0);
        matrix.check("kperf", S_IFREG | 0400, 0, 0);
        matrix.check("kperfctl", S_IFREG | 0600, 0, 0);
        matrix.check("version/", 0, 0, 0, ENOTDIR);
        matrix.check("goal19-no-such-metadata", 0, 0, 0, ENOENT);
        matrix.process("self", getuid(), getgid());
    } else {
        char numeric[64];
        snprintf(numeric, sizeof(numeric), "%ld", static_cast<long>(target));
        matrix.process(numeric, 1000, 2001);
    }
    bool ok = close(procfd) == 0 && matrix.passed == matrix.total;
    printf("%s profile=%u phase=%d passed=%u total=%u\n", ok ? "PASS" : "FAIL", kind, phase, matrix.passed, matrix.total);
    fflush(stdout);
    return ok ? 0 : 1;
}
bool pinned_stat(int fd) {
    struct stat output;
    return fstat(fd, &output) == 0 && output.st_uid == 1000 && output.st_gid == 2001 && output.st_mode == (S_IFREG | 0444);
}
}  // namespace
int main() {
    if (geteuid() != 0) return 2;
    unsigned bad = 0;
    for (unsigned kind = 0; kind < 4; ++kind) {
        pid_t child = fork();
        if (child == 0) _exit(observer(kind, -1, 0));
        bad += !wait_ok(child);
    }
    int commands[2], replies[2];
    if (pipe(commands) || pipe(replies)) return 2;
    pid_t target = fork();
    if (target == 0) {
        close(commands[1]);
        close(replies[0]);
        if (!profile(1)) _exit(2);
        char phase;
        while (read(commands[0], &phase, 1) == 1 && phase != 'q') {
            if (ker::process::prctl(4, phase == '1', 0, 0, 0) || write(replies[1], &phase, 1) != 1) _exit(2);
        }
        close(commands[0]);
        close(replies[1]);
        _exit(0);
    }
    close(commands[0]);
    close(replies[1]);
    int retained = -1;
    if (target > 0) {
        for (int phase = 0; phase < 3; ++phase) {
            char command = phase == 1 ? '1' : '0', reply = 0;
            if (write(commands[1], &command, 1) != 1 || read(replies[0], &reply, 1) != 1 || reply != command) {
                ++bad;
                break;
            }
            if (phase == 0) {
                char path[128];
                snprintf(path, sizeof(path), "/proc/%ld/status", static_cast<long>(target));
                retained = open(path, O_RDONLY);
            }
            bad += !pinned_stat(retained);
            for (unsigned kind = 0; kind < 4; ++kind) {
                pid_t child = fork();
                if (child == 0) _exit(observer(kind, phase, target));
                bad += !wait_ok(child);
            }
        }
        char quit = 'q';
        bad += write(commands[1], &quit, 1) != 1;
        bad += !wait_ok(target);
        bad += !pinned_stat(retained);
    } else
        ++bad;
    bool cleanup = close(commands[1]) == 0 && close(replies[0]) == 0;
    cleanup &= close(retained) == 0;
    bad += !cleanup;
    printf("%s: 2244 native PROCFS metadata checks; cleanup=%d\n", bad ? "FAIL" : "PASS", cleanup);
    return bad ? 1 : 0;
}
