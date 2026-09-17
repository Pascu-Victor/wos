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
constexpr int FLAGS[] = {0, O_NOFOLLOW, O_CREAT, O_CREAT | O_EXCL, O_WRONLY, O_TRUNC, O_DIRECTORY};
struct Matrix {
    int procfd;
    int first_free;
    unsigned total = 0;
    unsigned passed = 0;
    void check(const char* relative, int flags, int wanted, bool directory, int source_fd = -1) {
        char absolute[512];
        snprintf(absolute, sizeof(absolute), "/proc/%s", relative);
        for (bool at : {false, true})
            for (bool cloexec : {false, true}) {
                int requested = flags | (cloexec ? O_CLOEXEC : 0);
                errno = 0;
                int fd = at ? openat(procfd, relative, requested, 0600) : open(absolute, requested, 0600);
                int error = fd < 0 ? errno : 0;
                bool ok = error == wanted && (wanted ? fd == -1 : fd >= 0);
                if (fd >= 0) {
                    struct stat st{};
                    ok &= fstat(fd, &st) == 0 && static_cast<bool>(S_ISDIR(st.st_mode)) == directory;
                    ok &= fcntl(fd, F_GETFD) == (cloexec ? FD_CLOEXEC : 0);
                    int expected_mode = source_fd >= 0 ? fcntl(source_fd, F_GETFL) & O_ACCMODE : flags & O_ACCMODE;
                    ok &= (fcntl(fd, F_GETFL) & O_ACCMODE) == expected_mode;
                    if (source_fd >= 0 && !directory) {
                        // FD-link open duplicates the actual File: its grant and
                        // offset are shared even when different open flags were requested.
                        char data[4]{};
                        ok &= lseek(fd, 1, SEEK_SET) == 1 && lseek(source_fd, 0, SEEK_CUR) == 1;
                        ok &= pread(fd, data, sizeof(data), 0) == 4 && memcmp(data, "KEEP", 4) == 0;
                        ok &= lseek(fd, 0, SEEK_SET) == 0;
                    }
                    ok &= close(fd) == 0;
                }
                int available = dup(procfd);
                ok &= available == first_free;
                if (available >= 0) ok &= close(available) == 0;
                ++total;
                passed += ok;
                if (!ok)
                    printf("FAIL path=%s at=%d cloexec=%d flags=%x fd=%d errno=%d expected=%d nextfd=%d expectedfd=%d\n", relative, at,
                           cloexec, flags, fd, error, wanted, available, first_free);
            }
    }
    void metadata(const char* path, bool directory, bool allowed, bool writable, bool gated_read = false) {
        for (int flags : FLAGS) {
            int error = (flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL)          ? EEXIST
                        : (flags & O_DIRECTORY) && !directory                       ? ENOTDIR
                        : directory && (flags & (O_WRONLY | O_TRUNC))               ? EISDIR
                        : !allowed && gated_read && (flags & O_ACCMODE) != O_WRONLY ? EACCES
                        : (flags & (O_WRONLY | O_TRUNC)) && !writable               ? EPERM
                        : allowed                                                   ? 0
                                                                                    : EACCES;
            check(path, flags, error, directory);
        }
    }
    void process(const char* pid, bool allowed, int localfd, int remotefd) {
        char path[512];
        for (const char* tail : {"cmdline", "maps", "images", "fd"}) {
            snprintf(path, sizeof(path), "%s/%s", pid, tail);
            metadata(path, !strcmp(tail, "fd"), allowed, false);
        }
        for (const char* tail : {"root", "cwd", "exe"}) {
            snprintf(path, sizeof(path), "%s/%s", pid, tail);
            check(path, O_NOFOLLOW, ELOOP, false);
        }
        for (int source : {localfd, remotefd}) {
            snprintf(path, sizeof(path), "%s/fd/%d", pid, source);
            check(path, O_NOFOLLOW, ELOOP, false);
            bool directory = source == remotefd;
            for (int flags : FLAGS) {
                int error = (flags & O_NOFOLLOW)                                 ? ELOOP
                            : !allowed                                           ? EACCES
                            : (flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL) ? EEXIST
                            : (flags & O_DIRECTORY) && !directory                ? ENOTDIR
                                                                                 : 0;
                check(path, flags, error, directory, source);
            }
        }
    }
};
int observer(unsigned kind, int phase, pid_t target, const char* base, int localfd, int remotefd) {
    if (chdir(base) || !profile(kind)) return 2;
    int procfd = open("/proc", O_RDONLY | O_DIRECTORY);
    if (procfd < 0) return 2;
    int first_free = dup(procfd);
    if (first_free < 0 || close(first_free)) return 2;
    Matrix matrix{procfd, first_free};
    char numeric[64];
    snprintf(numeric, sizeof(numeric), "%ld", static_cast<long>(target ? target : getpid()));
    if (!target) {
        matrix.check("self", O_NOFOLLOW, ELOOP, false);
        matrix.process("self", true, localfd, remotefd);
        matrix.process(numeric, true, localfd, remotefd);
        matrix.metadata("version", false, true, false);
        matrix.metadata("wki", true, true, false);
        matrix.metadata("wki/auth", false, true, false);
        matrix.metadata("kperf", false, kind == 0, false, true);
        matrix.metadata("kperfctl", false, kind == 0, true);
        matrix.metadata("wki/chaos", false, kind == 0, true);
        for (int flags : FLAGS) matrix.check("goal19-open-missing", flags, ENOENT, false);
    } else {
        matrix.process(numeric, kind == 0 || (kind == 1 && phase == 1), localfd, remotefd);
    }
    bool ok = close(procfd) == 0 && matrix.passed == matrix.total;
    printf("%s profile=%u phase=%d target=%ld passed=%u total=%u\n", ok ? "PASS" : "FAIL", kind, phase, static_cast<long>(target),
           matrix.passed, matrix.total);
    fflush(stdout);
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char**) {
    if (argc != 1 || geteuid() != 0) return 2;
    char base[256], file[256];
    snprintf(base, sizeof(base), "/tmp/goal19-proc-open-%ld", static_cast<long>(getpid()));
    snprintf(file, sizeof(file), "%s/file", base);
    if (mkdir(base, 0755) || chmod(base, 0755)) return 2;
    int localfd = open(file, O_CREAT | O_EXCL | O_RDWR, 0644);
    int remotefd = open(REMOTE, O_RDONLY | O_DIRECTORY);
    if (localfd < 0 || remotefd < 0 || write(localfd, "KEEP", 4) != 4 || lseek(localfd, 0, SEEK_SET) != 0) return 2;
    unsigned bad = 0;
    for (unsigned kind = 0; kind < 4; ++kind) {
        pid_t child = fork();
        if (child == 0) _exit(observer(kind, -1, 0, base, localfd, remotefd));
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
                if (child == 0) _exit(observer(kind, phase, target, base, localfd, remotefd));
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
    char preserved[4]{};
    cleanup &= pread(localfd, preserved, 4, 0) == 4 && memcmp(preserved, "KEEP", 4) == 0;
    cleanup &= close(localfd) == 0;
    cleanup &= close(remotefd) == 0;
    cleanup &= unlink(file) == 0;
    cleanup &= rmdir(base) == 0;
    bad += !cleanup;
    printf("%s: 4560 native PROCFS open checks; cleanup=%d\n", bad ? "FAIL" : "PASS", cleanup);
    return bad ? 1 : 0;
}
